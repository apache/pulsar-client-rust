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

//! The Queue consumer, sans IO (docs/6-scalable-topics.md §4).
//!
//! A [`QueueConsumer`] holds the [`SegmentConsumer`]s of a Queue consumer: one on every segment of the topic's
//! layout, active or sealed, all on the same Shared subscription. The driver feeds it the layouts, what happens to
//! each segment's consumer (the outcome of its subscribe, the entries and commands the broker pushes for it) and the
//! passing of time; the application takes its messages and acknowledges them. It hands back [`Action`]s: those of
//! each segment's consumer, with its segment, and how the subscribe and the close went.
//!
//! It follows the Java client (`ScalableQueueConsumer` over v4 `ConsumerImpl`s), except where noted:
//!
//! - **Every segment.** It subscribes to every segment of the layout and keeps that set in step with each new
//!   layout: it subscribes to the segments a layout adds and lets go of those it drops. A segment's consumer is
//!   named `<name>-seg-<id>` when the application named the consumer, and one on a legacy segment carries the
//!   V5-managed property (PIP-475).
//! - **The initial position** applies to the segments of the first layout. A segment that a later layout adds is
//!   subscribed from its start: it is the child of a split or a merge, created after the subscription, and the
//!   broker creates the children with the subscriptions registered on the scalable topic, which a Queue consumer's
//!   subscribe doesn't register. Java subscribes it at the initial position, so a consumer at `Latest`, the default,
//!   misses what the children stored before it subscribed. That narrows a hole only the broker can close: until the
//!   consumer subscribes, nothing keeps a child's ledgers from being trimmed, and a subscription with no consumer
//!   during the split is created on the children at the initial position when one comes back.
//! - **The subscribe is all or nothing**, as in Java: [`Action::Subscribed`] comes once every segment is subscribed,
//!   those of layouts that came meanwhile included. If one can't be, the subscribe fails and every segment's
//!   consumer closes.
//! - **Retries.** A segment whose consumer gives up, because a subscribe failed for good, is subscribed again after a
//!   backoff shared by every segment, from 100 ms doubling to 30 s and reset once all are subscribed; or at once,
//!   when a new layout comes.
//! - **Receiving.** The segments with messages in take turns, one message each, so a busy segment can't starve the
//!   others. Each segment's consumer buffers up to the receiver queue size, as Java's do.
//! - **Acknowledging.** An ack or negative ack goes to the consumer of the segment that the message id names. One
//!   for a segment it let go of, or never had, does nothing, as in Java.
//! - **Detach once drained.** A segment's consumer closes once the segment is drained for it: the broker said the
//!   segment's topic ended, and everything it delivered is acknowledged ([`SegmentConsumer::is_drained`]). Java
//!   means to, but that code never runs: its segment consumers stay until the segment leaves the layout.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::backoff::{Backoff, Jitter};
use crate::consumer::{self, ConsumerOptions, SegmentConsumer};
use crate::entry::ReceivedMessage;
use crate::frame::PayloadSection;
use crate::layout::Layout;
use crate::message_id::MessageId;
use crate::producer::{RegisterError, V5_MANAGED_PROPERTY};
use crate::proto::command_subscribe::{InitialPosition, SubType};
use crate::proto::{BaseCommand, CommandMessage};

/// Java's `ScalableQueueConsumer` subscribes again to the segments that failed from 100 ms up to 30 s.
const RETRY_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// What the Queue consumer asks of its driver. A segment's consumer asks what a [`SegmentConsumer`] does
/// ([`consumer::Action`]), with its segment.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Look up `topic`, connect, register a consumer id, and send [`QueueConsumer::subscribe_command`]. Report with
    /// `on_subscribed` or `on_subscribe_failed`.
    Connect { segment: u64, generation: u64, topic: String },
    /// Send a command that has no response on the connection of the segment's consumer.
    Send { segment: u64, command: Box<BaseCommand> },
    /// Close the segment's consumer on the broker with `CommandCloseConsumer`, then report with `on_closed`.
    CloseConsumer { segment: u64, generation: u64 },
    /// How the subscribe went: every segment is subscribed, or one couldn't be. Not sent if the consumer is closed
    /// before.
    Subscribed(Result<(), RegisterError>),
    /// The consumer is closed: [`QueueConsumer::close`] is complete, or the subscribe failed and closed it.
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
    /// Its first subscribe succeeded.
    subscribed: bool,
    /// It is being let go: the segment is drained or left the layout, or the Queue consumer is closing.
    released: bool,
    /// It sent [`consumer::Action::Closed`].
    closed: bool,
}

/// See the module documentation.
#[derive(Debug)]
pub struct QueueConsumer {
    options: ConsumerOptions,
    layout: Rc<Layout>,
    state: State,
    /// The segments' consumers, until they are finished ([`SegmentConsumer::is_finished`]): one let go of while
    /// subscribing still closes its subscription once the subscribe completes.
    slots: BTreeMap<u64, Slot>,
    /// The segments of the first layout, which subscribe at the initial position.
    initial: BTreeSet<u64>,
    /// The segments let go of once drained, while the layout has them.
    drained: BTreeSet<u64>,
    /// While subscribing, the segments not subscribed yet.
    awaiting: BTreeSet<u64>,
    /// When to subscribe again to the segments whose consumers gave up. Set while there is one.
    retry_at: Option<Instant>,
    backoff: Backoff,
    jitter: Jitter,
    /// The segment that delivered the last message.
    turn: Option<u64>,
    actions: VecDeque<Action>,
}

impl QueueConsumer {
    /// A Queue consumer on the topic's first layout, subscribing to every segment now. Poll
    /// [`QueueConsumer::poll_action`] for [`Action::Subscribed`].
    pub fn new(options: ConsumerOptions, layout: Rc<Layout>, now: Instant) -> QueueConsumer {
        let initial = layout.segments().map(|s| s.id).collect();
        let mut queue = QueueConsumer {
            options,
            layout,
            state: State::Subscribing,
            slots: BTreeMap::new(),
            initial,
            drained: BTreeSet::new(),
            awaiting: BTreeSet::new(),
            retry_at: None,
            backoff: Backoff::new(RETRY_BACKOFF.0, RETRY_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            turn: None,
            actions: VecDeque::new(),
        };
        queue.reconcile(now);
        queue.settle();
        queue
    }

    /// The topic, as the broker resolved it.
    pub fn topic(&self) -> &str {
        self.layout.topic()
    }

    /// The segments it consumes: those with a consumer it hasn't let go of.
    pub fn segments(&self) -> impl Iterator<Item = u64> + '_ {
        self.slots.iter().filter(|(_, slot)| !slot.released).map(|(id, _)| *id)
    }

    /// Whether it is closed, or closing: [`QueueConsumer::close`] was called, or the subscribe failed.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closing | State::Closed)
    }

    /// The `CommandSubscribe` to register `segment`'s consumer with, as `consumer_id`. The driver sets its request
    /// id.
    pub fn subscribe_command(&self, segment: u64, consumer_id: u64) -> Option<BaseCommand> {
        Some(self.slots.get(&segment)?.consumer.subscribe_command(consumer_id))
    }

    /// Applies a newer layout: subscribes to the segments it adds, lets go of those it drops, and subscribes again,
    /// at once, to those whose consumers gave up.
    pub fn on_layout(&mut self, layout: Rc<Layout>, now: Instant) {
        if layout.epoch() <= self.layout.epoch() || self.is_closed() {
            return;
        }
        self.layout = layout;
        let layout = self.layout.clone();
        let present = |id: &u64| layout.segment(*id).is_some();
        self.initial.retain(present);
        self.drained.retain(present);
        self.awaiting.retain(present);
        let dropped: Vec<u64> = self.segments().filter(|id| !present(id)).collect();
        for id in dropped {
            self.release(id);
            self.sync(id);
        }
        self.reconcile(now);
        self.settle();
    }

    /// The broker registered `segment`'s consumer of `generation` as `consumer_id`.
    pub fn on_subscribed(&mut self, segment: u64, generation: u64, consumer_id: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribed(generation, consumer_id);
        self.sync(segment);
        self.settle();
    }

    /// Subscribing `segment`'s consumer of `generation` failed.
    pub fn on_subscribe_failed(&mut self, segment: u64, generation: u64, error: RegisterError, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribe_failed(generation, error, now);
        if self.sync(segment) {
            self.schedule_retry(now);
        }
        self.settle();
    }

    /// An entry the broker pushed for `segment`'s consumer of `generation`.
    pub fn on_message(&mut self, segment: u64, generation: u64, message: &CommandMessage, section: &PayloadSection) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_message(generation, message, section);
        self.sync(segment);
    }

    /// The broker says `segment`'s topic ended for the subscription.
    pub fn on_reached_end_of_topic(&mut self, segment: u64, generation: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_reached_end_of_topic(generation);
        self.sync(segment);
        self.settle();
    }

    /// `segment`'s consumer of `generation` lost its connection, or the broker closed it.
    pub fn on_disconnected(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_disconnected(generation, now);
        self.sync(segment);
        self.settle();
    }

    /// Our `CommandCloseConsumer` for `segment`'s consumer of `generation` was answered, or its connection closed.
    pub fn on_closed(&mut self, segment: u64, generation: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_closed(generation);
        self.sync(segment);
        self.settle();
    }

    /// The next message for the application, if one is in. The segments with messages take turns.
    pub fn next_message(&mut self) -> Option<ReceivedMessage> {
        let after = self.turn.map_or(Bound::Unbounded, Bound::Excluded);
        let segment = self
            .slots
            .range((after, Bound::Unbounded))
            .chain(&self.slots)
            .find(|(_, slot)| slot.consumer.available() > 0)
            .map(|(id, _)| *id)?;
        self.turn = Some(segment);
        let message = self.slots.get_mut(&segment)?.consumer.next_message();
        self.sync(segment);
        message
    }

    /// How many messages are in, waiting for the application.
    pub fn available(&self) -> usize {
        self.slots.values().map(|slot| slot.consumer.available()).sum()
    }

    /// Acknowledges a message. One of a segment it let go of, or never had, is ignored.
    pub fn acknowledge(&mut self, id: &MessageId, now: Instant) {
        let segment = id.segment_id as u64;
        let Some(slot) = self.slots.get_mut(&segment).filter(|slot| !slot.released) else { return };
        slot.consumer.acknowledge(&id.entry, now);
        self.sync(segment);
    }

    /// Asks for a message again after the negative ack delay. One of a segment it let go of, or never had, is
    /// ignored.
    pub fn negative_acknowledge(&mut self, id: &MessageId, now: Instant) {
        let segment = id.segment_id as u64;
        let Some(slot) = self.slots.get_mut(&segment).filter(|slot| !slot.released) else { return };
        slot.consumer.negative_acknowledge(&id.entry, now);
        self.sync(segment);
    }

    /// Closes every segment's consumer: their acks go out first, and the messages not taken yet are dropped, since
    /// the broker delivers them again. [`Action::Closed`] follows once all are closed. Can be called again.
    pub fn close(&mut self) {
        if self.is_closed() {
            return;
        }
        self.state = State::Closing;
        self.retry_at = None;
        let ids: Vec<u64> = self.segments().collect();
        for id in ids {
            self.release(id);
            self.sync(id);
        }
        self.settle();
    }

    /// When the consumer next has something to do: a segment consumer's, or subscribing again to the segments whose
    /// consumers gave up.
    pub fn poll_timeout(&self) -> Option<Instant> {
        self.slots.values().filter_map(|slot| slot.consumer.poll_timeout()).chain(self.retry_at).min()
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        let ids: Vec<u64> = self.slots.keys().copied().collect();
        for id in ids {
            let Some(slot) = self.slots.get_mut(&id) else { continue };
            slot.consumer.handle_timeout(now);
            self.sync(id);
        }
        if self.retry_at.is_some_and(|at| at <= now) {
            self.reconcile(now);
        }
        self.settle();
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    /// Subscribes to every segment of the layout it has no consumer for, but the drained ones.
    fn reconcile(&mut self, now: Instant) {
        let missing: Vec<u64> = self
            .layout
            .segments()
            .map(|s| s.id)
            .filter(|id| !self.slots.contains_key(id) && !self.drained.contains(id))
            .collect();
        for id in missing {
            self.attach(id, now);
        }
        // Every segment it wants has a consumer again.
        self.retry_at = None;
    }

    /// Subscribes to a segment of the layout.
    fn attach(&mut self, id: u64, now: Instant) {
        let segment = self.layout.segment(id).expect("a segment of the layout");
        let mut options = self.options.clone();
        options.sub_type = SubType::Shared;
        options.consumer_name = self.options.consumer_name.as_ref().map(|name| format!("{name}-seg-{id}"));
        if !self.initial.contains(&id) {
            options.initial_position = InitialPosition::Earliest;
        }
        if segment.is_legacy() {
            options.properties.push((V5_MANAGED_PROPERTY.0.into(), V5_MANAGED_PROPERTY.1.into()));
        }
        let consumer = SegmentConsumer::new(options, id as i64, segment.topic.clone(), now);
        self.slots.insert(id, Slot { consumer, subscribed: false, released: false, closed: false });
        if self.state == State::Subscribing {
            self.awaiting.insert(id);
        }
        self.sync(id);
    }

    /// Lets go of a segment's consumer, which isn't yet: closes it.
    fn release(&mut self, segment: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.released = true;
        slot.consumer.close();
    }

    /// Passes on what `segment`'s consumer asks, lets go of it once drained, and forgets it once finished. Returns
    /// whether it gave up: a subscribe failed for good.
    fn sync(&mut self, segment: u64) -> bool {
        let mut gave_up = false;
        while let Some(slot) = self.slots.get_mut(&segment) {
            if let Some(action) = slot.consumer.poll_action() {
                gave_up |= self.route(segment, action);
            } else if !slot.released && slot.consumer.is_drained() {
                // The segment's topic ended, and everything it delivered is acknowledged.
                self.drained.insert(segment);
                self.release(segment);
            } else {
                if slot.consumer.is_finished() {
                    self.slots.remove(&segment);
                }
                break;
            }
        }
        gave_up
    }

    /// Handles an action of `segment`'s consumer. Returns whether it gave up.
    fn route(&mut self, segment: u64, action: consumer::Action) -> bool {
        match action {
            consumer::Action::Connect { generation } => {
                let topic = self.slots[&segment].consumer.topic().to_string();
                self.actions.push_back(Action::Connect { segment, generation, topic });
            }
            consumer::Action::Send(command) => self.actions.push_back(Action::Send { segment, command }),
            consumer::Action::CloseConsumer { generation } => {
                self.actions.push_back(Action::CloseConsumer { segment, generation });
            }
            consumer::Action::Subscribed(Ok(())) => {
                self.slots.get_mut(&segment).expect("the segment of the action").subscribed = true;
                self.awaiting.remove(&segment);
                if self.all_subscribed() {
                    self.backoff.reset();
                }
            }
            consumer::Action::Subscribed(Err(error)) => {
                // Once subscribed, the `Closed` that follows subscribes to it again.
                if self.state == State::Subscribing {
                    self.actions.push_back(Action::Subscribed(Err(error)));
                    self.close();
                }
            }
            consumer::Action::Closed => {
                let slot = self.slots.get_mut(&segment).expect("the segment of the action");
                slot.closed = true;
                return !slot.released;
            }
        }
        false
    }

    /// Whether every segment it wants is subscribed.
    fn all_subscribed(&self) -> bool {
        let mut wanted = self.layout.segments().filter(|s| !self.drained.contains(&s.id));
        wanted.all(|s| self.slots.get(&s.id).is_some_and(|slot| slot.subscribed))
    }

    /// A segment's consumer gave up: subscribe to it again after the backoff, with any other that gives up meanwhile.
    fn schedule_retry(&mut self, now: Instant) {
        if self.retry_at.is_none() {
            self.retry_at = Some(now + self.backoff.next_delay(self.jitter.next()));
        }
    }

    /// Completes the subscribe once every segment is subscribed, and the close once every segment's consumer is
    /// closed.
    fn settle(&mut self) {
        if self.state == State::Subscribing && self.awaiting.is_empty() && self.retry_at.is_none() {
            self.state = State::Open;
            self.actions.push_back(Action::Subscribed(Ok(())));
        }
        if self.state == State::Closing && self.slots.values().all(|slot| slot.closed) {
            self.state = State::Closed;
            self.actions.push_back(Action::Closed);
        }
    }
}
