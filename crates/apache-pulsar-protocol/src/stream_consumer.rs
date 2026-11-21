// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The Stream consumer, sans IO (docs/6-scalable-topics.md §5).
//!
//! A [`StreamConsumer`] holds a Stream consumer's registration with the topic's controller, and the
//! [`SegmentConsumer`]s of the segments the controller assigns it. The driver feeds it the outcome of the
//! registration and the assignments the controller pushes, what happens to each segment's consumer, and the passing
//! of time; the application takes its messages and acknowledges them cumulatively. It hands back [`Action`]s.
//!
//! It follows the Java client (`ScalableConsumerClient` and `ScalableStreamConsumer` over v4 `ConsumerImpl`s), except
//! where noted:
//!
//! - **The assignment session** with the controller is an [`AssignmentSession`] of a STREAM consumer, under the
//!   consumer's name. An assignment that repeats a segment unchanged leaves it alone.
//! - **Attaching** (*key-shared* §4). A segment the consumer owns whole is subscribed Exclusive; one the controller
//!   shares between members, `Key_Shared` by entry bucket with the segment's boundaries. Its consumer is named
//!   `<name>-seg-<id>`. One on a legacy segment, whose topic is a `persistent://` one, carries the V5-managed
//!   property (PIP-475), as the Queue consumer's do, and as in Java.
//! - **The initial position** applies to the segments of the layout the consumer subscribed on. A segment created
//!   after that is subscribed at the earliest position, as the Queue consumer's are. That matters only when the
//!   consumer's registration lapsed while the segment was created: while a member is registered, the broker creates
//!   a split's or a merge's children with the subscription, at their start. Java subscribes every segment at the
//!   initial position.
//! - **Letting go.** A segment that leaves the assignment, or whose mode flips between Exclusive and shared, gives
//!   the application nothing more, and its consumer closes once the application has acknowledged everything it was
//!   given from it (*key-shared* §4): the messages still in go to the next owner, in order. A flipped segment is
//!   subscribed again, in its new mode, only then. The consumer never acknowledges anything itself.
//! - **Retries.** A segment whose consumer gives up, because a subscribe failed for good, is subscribed again after a
//!   backoff shared by every segment, from 100 ms doubling to 30 s and reset once every assigned segment is
//!   subscribed; or at once, when an assignment comes. While another member still holds a segment the controller
//!   moved here, the broker refuses it with `ConsumerBusy` or `ConsumerAssignError`: the subscribe waits those out,
//!   up to the operation timeout, as Java's does, and fails on any other refusal.
//! - **Receiving.** The segments with messages in take turns, one message each, and each segment's consumer buffers
//!   up to the receiver queue size. A message carries its position vector: the last message given to the application
//!   from each segment the consumer holds, its own included (*client-behavior* §6).
//! - **Acknowledging.** A cumulative ack acknowledges, on every segment of the message's vector the consumer still
//!   holds, everything up to the vector's position there: cumulatively on a segment it owns whole, and one message at
//!   a time on a shared one ([`SegmentConsumer::acknowledge_cumulative`]). The segments it no longer holds are
//!   skipped, as in Java.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::time::{Duration, Instant};

use crate::assignment::{self, Assignment, AssignmentSession, Outcome};
use crate::backoff::{Backoff, Jitter};
use crate::consumer::{self, ConsumerOptions, SegmentConsumer};
use crate::entry::ReceivedMessage;
use crate::frame::PayloadSection;
use crate::message_id::{MessageId, SegmentVector};
use crate::producer::{RegisterError, V5_MANAGED_PROPERTY};
use crate::proto::command_subscribe::{InitialPosition, SubType};
use crate::proto::{
    BaseCommand, CommandMessage, IntRange, ScalableConsumerAssignment, ScalableConsumerType, ServerError,
};

/// Java's `ScalableStreamConsumer` subscribes again to the segments that failed from 100 ms up to 30 s.
const RETRY_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// What the Stream consumer asks of its driver. A segment's consumer asks what a [`SegmentConsumer`] does
/// ([`consumer::Action`]), with its segment.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// The assignment session's [`assignment::Action::Register`], with [`StreamConsumer::register_command`]; the loss
    /// of its connection is reported with `on_session_lost`.
    Register { generation: u64 },
    /// The assignment session's [`assignment::Action::Unregister`].
    Unregister { generation: u64 },
    /// Look up `topic`, connect, register a consumer id, and send [`StreamConsumer::subscribe_command`]. Report with
    /// `on_subscribed` or `on_subscribe_failed`.
    Connect { segment: u64, generation: u64, topic: String },
    /// Send a command that has no response on the connection of the segment's consumer.
    Send { segment: u64, command: Box<BaseCommand> },
    /// Close the segment's consumer on the broker with `CommandCloseConsumer`, then report with `on_closed`.
    CloseConsumer { segment: u64, generation: u64 },
    /// How the subscribe went: registered, and every assigned segment subscribed, or not. Not sent if the consumer is
    /// closed before.
    Subscribed(Result<(), RegisterError>),
    /// The consumer is closed: [`StreamConsumer::close`] is complete, or the subscribe failed and closed it.
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Subscribing,
    Open,
    Closing,
    Closed,
}

/// The consumer of one segment.
#[derive(Debug)]
struct Slot {
    consumer: SegmentConsumer,
    /// The bucket boundaries it subscribed with: none when Exclusive.
    ranges: Vec<(i32, i32)>,
    /// Its first subscribe succeeded.
    subscribed: bool,
    /// It is being let go of: drained, then closed.
    released: bool,
    /// It was asked to close.
    closing: bool,
    /// It sent [`consumer::Action::Closed`].
    closed: bool,
    /// Why its first subscribe failed.
    refused: Option<RegisterError>,
}

/// See the module documentation.
#[derive(Debug)]
pub struct StreamConsumer {
    options: ConsumerOptions,
    topic: String,
    /// The segments of the layout it subscribed on, which subscribe at the initial position.
    initial: BTreeSet<u64>,
    state: State,
    session: AssignmentSession,
    assignment: Assignment,
    /// The segments' consumers, until they are finished ([`SegmentConsumer::is_finished`]).
    slots: BTreeMap<u64, Slot>,
    /// The segments whose consumers gave up, until they are subscribed again.
    failed: BTreeSet<u64>,
    /// When to subscribe again to them. Set while there are some.
    retry_at: Option<Instant>,
    backoff: Backoff,
    jitter: Jitter,
    /// Until when the subscribe waits out the refusals of a rebalance in progress.
    deadline: Instant,
    /// The last message given to the application from each segment it holds.
    positions: SegmentVector,
    /// The segment that delivered the last message.
    turn: Option<u64>,
    actions: VecDeque<Action>,
}

impl StreamConsumer {
    /// A Stream consumer of `topic`, the name the broker resolved, registering now as `name`. The segments of
    /// `initial`, those of the layout it subscribes on, start at the options' initial position. Poll
    /// [`StreamConsumer::poll_action`] for [`Action::Subscribed`].
    pub fn new(options: ConsumerOptions, topic: String, name: String, initial: BTreeSet<u64>, now: Instant) -> Self {
        let deadline = now + options.operation_timeout;
        let session = AssignmentSession::new(
            ScalableConsumerType::STREAM,
            topic.clone(),
            options.subscription.clone(),
            name,
            now,
        );
        let mut consumer = StreamConsumer {
            options,
            topic,
            initial,
            state: State::Subscribing,
            session,
            assignment: Assignment::new(),
            slots: BTreeMap::new(),
            failed: BTreeSet::new(),
            retry_at: None,
            backoff: Backoff::new(RETRY_BACKOFF.0, RETRY_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            deadline,
            positions: SegmentVector::new(),
            turn: None,
            actions: VecDeque::new(),
        };
        consumer.pass_session_actions();
        consumer
    }

    /// The topic, as the broker resolved it.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The name it registers under.
    pub fn name(&self) -> &str {
        self.session.name()
    }

    /// The segments it consumes: those with a consumer it isn't letting go of.
    pub fn segments(&self) -> impl Iterator<Item = u64> + '_ {
        self.slots.iter().filter(|(_, slot)| !slot.released).map(|(id, _)| *id)
    }

    /// Whether it is closed, or closing: [`StreamConsumer::close`] was called, or the subscribe failed.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closing | State::Closed)
    }

    /// The `CommandScalableTopicSubscribe` to register with, as `consumer_id`. The driver sets its request id.
    pub fn register_command(&self, consumer_id: u64) -> BaseCommand {
        self.session.register_command(consumer_id)
    }

    /// The `CommandSubscribe` to register `segment`'s consumer with, as `consumer_id`. The driver sets its request
    /// id.
    pub fn subscribe_command(&self, segment: u64, consumer_id: u64) -> Option<BaseCommand> {
        Some(self.slots.get(&segment)?.consumer.subscribe_command(consumer_id))
    }

    /// The controller registered the consumer, for the registration of `generation`, with its assignment.
    pub fn on_registered(&mut self, generation: u64, assignment: &ScalableConsumerAssignment, now: Instant) {
        let outcome = self.session.on_registered(generation, assignment);
        self.take(outcome, now);
    }

    /// The registration of `generation` failed. The first one fails the subscribe; the others are retried.
    pub fn on_register_failed(&mut self, generation: u64, error: RegisterError, now: Instant) {
        let outcome = self.session.on_register_failed(generation, error, now);
        self.take(outcome, now);
    }

    /// The controller pushed an assignment to the registration of `generation`.
    pub fn on_assignment(&mut self, generation: u64, assignment: &ScalableConsumerAssignment, now: Instant) {
        let outcome = self.session.on_assignment(generation, assignment);
        self.take(outcome, now);
    }

    /// The connection of the registration of `generation` closed: register again, after the backoff.
    pub fn on_session_lost(&mut self, generation: u64, now: Instant) {
        self.session.on_lost(generation, now);
    }

    /// The unsubscribe of the registration of `generation` was answered, or failed.
    pub fn on_unregistered(&mut self, generation: u64) {
        self.session.on_unregistered(generation);
        self.settle();
    }

    /// The broker registered `segment`'s consumer of `generation` as `consumer_id`.
    pub fn on_subscribed(&mut self, segment: u64, generation: u64, consumer_id: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribed(generation, consumer_id);
        self.sync(segment, now);
        self.fill(now);
        self.settle();
    }

    /// Subscribing `segment`'s consumer of `generation` failed.
    pub fn on_subscribe_failed(&mut self, segment: u64, generation: u64, error: RegisterError, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribe_failed(generation, error, now);
        self.sync(segment, now);
        self.fill(now);
        self.settle();
    }

    /// An entry the broker pushed for `segment`'s consumer of `generation`.
    pub fn on_message(
        &mut self,
        segment: u64,
        generation: u64,
        message: &CommandMessage,
        section: &PayloadSection,
        now: Instant,
    ) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_message(generation, message, section);
        self.sync(segment, now);
    }

    /// `segment`'s consumer of `generation` lost its connection, or the broker closed it.
    pub fn on_disconnected(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_disconnected(generation, now);
        self.sync(segment, now);
        self.fill(now);
        self.settle();
    }

    /// Our `CommandCloseConsumer` for `segment`'s consumer of `generation` was answered, or its connection closed.
    pub fn on_closed(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_closed(generation);
        self.sync(segment, now);
        self.fill(now);
        self.settle();
    }

    /// The next message for the application, if one is in, with its position vector. The segments with messages take
    /// turns.
    pub fn next_message(&mut self, now: Instant) -> Option<ReceivedMessage> {
        let after = self.turn.map_or(Bound::Unbounded, Bound::Excluded);
        let segment = self
            .slots
            .range((after, Bound::Unbounded))
            .chain(&self.slots)
            .find(|(_, slot)| slot.consumer.available() > 0)
            .map(|(id, _)| *id)?;
        self.turn = Some(segment);
        let mut message = self.slots.get_mut(&segment)?.consumer.next_message()?;
        self.sync(segment, now);
        self.positions.insert(segment as i64, message.id.entry);
        message.id.positions = self.positions.clone();
        Some(message)
    }

    /// How many messages are in, waiting for the application.
    pub fn available(&self) -> usize {
        self.slots.values().map(|slot| slot.consumer.available()).sum()
    }

    /// Acknowledges every message up to `id`, on every segment of its position vector that the consumer still holds.
    pub fn acknowledge_cumulative(&mut self, id: &MessageId, now: Instant) {
        for (&segment, position) in &id.positions {
            let segment = segment as u64;
            let Some(slot) = self.slots.get_mut(&segment) else { continue };
            slot.consumer.acknowledge_cumulative(position, now);
            self.sync(segment, now);
        }
        self.fill(now);
        self.settle();
    }

    /// Closes every segment's consumer: their acks go out first, and the messages not taken yet are dropped, since
    /// the broker delivers them again. Leaves the controller's group. [`Action::Closed`] follows once all are closed
    /// and the controller answered. Can be called again.
    pub fn close(&mut self, now: Instant) {
        if self.is_closed() {
            return;
        }
        self.state = State::Closing;
        self.retry_at = None;
        self.session.close();
        self.pass_session_actions();
        let ids: Vec<u64> = self.slots.keys().copied().collect();
        for id in ids {
            let slot = self.slots.get_mut(&id).expect("a segment's consumer");
            slot.released = true;
            slot.closing = true;
            slot.consumer.close();
            self.sync(id, now);
        }
        self.settle();
    }

    /// When the consumer next has something to do: a segment consumer's, registering again, or subscribing again to
    /// the segments whose consumers gave up.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let slots = self.slots.values().filter_map(|slot| slot.consumer.poll_timeout());
        slots.chain(self.session.poll_timeout()).chain(self.retry_at).min()
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        let ids: Vec<u64> = self.slots.keys().copied().collect();
        for id in ids {
            let Some(slot) = self.slots.get_mut(&id) else { continue };
            slot.consumer.handle_timeout(now);
            self.sync(id, now);
        }
        self.session.handle_timeout(now);
        self.pass_session_actions();
        if self.retry_at.is_some_and(|at| at <= now) {
            self.reconcile(now);
        }
        self.fill(now);
        self.settle();
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    /// Does what the session's report means, and passes on what the session asks.
    fn take(&mut self, outcome: Outcome, now: Instant) {
        match outcome {
            Outcome::Nothing => {}
            Outcome::Apply(assignment) => {
                self.assignment = assignment;
                self.reconcile(now);
            }
            Outcome::Failed(error) => self.fail(error, now),
        }
        self.pass_session_actions();
        self.settle();
    }

    fn pass_session_actions(&mut self) {
        while let Some(action) = self.session.poll_action() {
            self.actions.push_back(match action {
                assignment::Action::Register { generation } => Action::Register { generation },
                assignment::Action::Unregister { generation } => Action::Unregister { generation },
            });
        }
    }

    /// Lets go of the segments that left the assignment or changed mode, and subscribes to every assigned segment
    /// without a consumer, those that gave up included.
    fn reconcile(&mut self, now: Instant) {
        let leaving: Vec<u64> = self
            .slots
            .iter()
            .filter(|(id, slot)| {
                !slot.released && self.assignment.get(id).is_none_or(|a| a.bucket_ranges != slot.ranges)
            })
            .map(|(id, _)| *id)
            .collect();
        for id in leaving {
            let slot = self.slots.get_mut(&id).expect("a segment's consumer");
            slot.released = true;
            slot.consumer.pause();
            self.sync(id, now);
        }
        self.failed.clear();
        self.retry_at = None;
        self.fill(now);
    }

    /// Subscribes to the assigned segments without a consumer, but those waiting for a retry.
    fn fill(&mut self, now: Instant) {
        if self.is_closed() {
            return;
        }
        let missing: Vec<u64> = self
            .assignment
            .keys()
            .filter(|id| !self.slots.contains_key(id) && !self.failed.contains(id))
            .copied()
            .collect();
        for id in missing {
            self.attach(id, now);
        }
    }

    /// Subscribes to an assigned segment, in the mode its assignment gives.
    fn attach(&mut self, id: u64, now: Instant) {
        let assigned = &self.assignment[&id];
        let mut options = self.options.clone();
        options.consumer_name = Some(format!("{}-seg-{id}", self.session.name()));
        if assigned.bucket_ranges.is_empty() {
            options.sub_type = SubType::Exclusive;
        } else {
            options.sub_type = SubType::Key_Shared;
            options.bucket_ranges = assigned
                .bucket_ranges
                .iter()
                .map(|&(start, end)| IntRange { start, end, ..Default::default() })
                .collect();
        }
        if !self.initial.contains(&id) {
            options.initial_position = InitialPosition::Earliest;
        }
        if assigned.topic.starts_with("persistent://") {
            options.properties.push((V5_MANAGED_PROPERTY.0.into(), V5_MANAGED_PROPERTY.1.into()));
        }
        let ranges = assigned.bucket_ranges.clone();
        let consumer = SegmentConsumer::new(options, id as i64, assigned.topic.clone(), now);
        let slot =
            Slot { consumer, ranges, subscribed: false, released: false, closing: false, closed: false, refused: None };
        self.slots.insert(id, slot);
        self.sync(id, now);
    }

    /// Passes on what `segment`'s consumer asks, closes it once it is let go of and settled, and forgets it once it
    /// is finished.
    fn sync(&mut self, segment: u64, now: Instant) {
        while let Some(slot) = self.slots.get_mut(&segment) {
            if let Some(action) = slot.consumer.poll_action() {
                self.route(segment, action, now);
            } else if slot.released && !slot.closing && slot.consumer.is_settled() {
                // The application acknowledged everything it was given from the segment.
                slot.closing = true;
                slot.consumer.close();
            } else {
                if slot.consumer.is_finished() {
                    self.slots.remove(&segment);
                    self.positions.remove(&(segment as i64));
                }
                break;
            }
        }
    }

    /// Handles an action of `segment`'s consumer.
    fn route(&mut self, segment: u64, action: consumer::Action, now: Instant) {
        let slot = self.slots.get_mut(&segment).expect("the segment of the action");
        match action {
            consumer::Action::Subscribed(Ok(())) => {
                slot.subscribed = true;
                if self.all_subscribed() {
                    self.backoff.reset();
                }
            }
            consumer::Action::Subscribed(Err(error)) => slot.refused = Some(error),
            consumer::Action::Closed => {
                slot.closed = true;
                if !slot.released {
                    let refused = slot.refused.take();
                    self.gave_up(segment, refused, now);
                }
            }
            consumer::Action::Connect { generation } => {
                let topic = slot.consumer.topic().to_string();
                self.actions.push_back(Action::Connect { segment, generation, topic });
            }
            consumer::Action::Send(command) => self.actions.push_back(Action::Send { segment, command }),
            consumer::Action::CloseConsumer { generation } => {
                self.actions.push_back(Action::CloseConsumer { segment, generation });
            }
        }
    }

    /// A segment's consumer gave up. While subscribing, only the refusals of a rebalance in progress are waited out,
    /// until the deadline; the others fail the subscribe. Otherwise it is subscribed again after the backoff.
    fn gave_up(&mut self, segment: u64, refused: Option<RegisterError>, now: Instant) {
        if self.state == State::Subscribing
            && let Some(error) = refused.filter(|error| !rebalancing(error) || now >= self.deadline)
        {
            return self.fail(error, now);
        }
        self.failed.insert(segment);
        if self.retry_at.is_none() {
            self.retry_at = Some(now + self.backoff.next_delay(self.jitter.next()));
        }
    }

    /// Whether every assigned segment is subscribed, in its mode.
    fn all_subscribed(&self) -> bool {
        self.assignment.keys().all(|id| self.slots.get(id).is_some_and(|slot| slot.subscribed && !slot.released))
    }

    /// The subscribe failed: say so, and close.
    fn fail(&mut self, error: RegisterError, now: Instant) {
        self.actions.push_back(Action::Subscribed(Err(error)));
        self.close(now);
    }

    /// Completes the subscribe once registered with every assigned segment subscribed, and the close once every
    /// segment's consumer is closed and the controller let the consumer go.
    fn settle(&mut self) {
        if self.state == State::Subscribing
            && self.session.is_registered()
            && self.retry_at.is_none()
            && self.all_subscribed()
        {
            self.state = State::Open;
            self.actions.push_back(Action::Subscribed(Ok(())));
        }
        if self.state == State::Closing && self.session.is_done() && self.slots.values().all(|slot| slot.closed) {
            self.state = State::Closed;
            self.actions.push_back(Action::Closed);
        }
    }
}

/// Whether the broker refused a segment's subscribe because another member still holds it: a rebalance in progress.
fn rebalancing(error: &RegisterError) -> bool {
    matches!(error, RegisterError::Server { error: ServerError::ConsumerBusy | ServerError::ConsumerAssignError, .. })
}
