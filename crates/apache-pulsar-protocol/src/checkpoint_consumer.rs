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

//! The Checkpoint consumer, sans IO (docs/6-scalable-topics.md §6).
//!
//! A [`CheckpointConsumer`] reads the segments of a topic, each with a reader ([`SegmentConsumer::reader`]), and keeps
//! where the application is in each: a [`Checkpoint`] to restore it from. Nothing is kept on the broker. On its own,
//! it reads every segment of the topic's layout, active or sealed; as a member of a group, the segments the topic's
//! controller assigns it. The driver feeds it the layouts, or the outcome of its registration with the controller and
//! the assignments the controller pushes; what happens to each reader (the outcome of its subscribe, the entries and
//! commands the broker pushes for it); the segments' last message ids it asks for; and the passing of time. The
//! application takes its messages, and the driver tells it which ones reached the application. It hands back
//! [`Action`]s: those of each reader, with its segment, the registration's, a last message id to ask for, and how the
//! create and the close went.
//!
//! It follows the Java client (`ScalableCheckpointConsumer` over v4 readers), except where noted:
//!
//! - **Every segment.** A reader on every segment of the layout, in step with each new layout. A segment that a
//!   layout drops leaves the checkpoint, and its reader is let go of. A reader on a legacy segment carries the
//!   V5-managed property (PIP-475), as the Queue consumer's do; Java's readers don't.
//! - **Where each segment starts.** After the position that the start checkpoint names for it, or at its earliest,
//!   for a segment the checkpoint doesn't name: one that a split or a merge made after the checkpoint, which holds
//!   only newer messages. A consumer at the latest position starts at the end of each segment of its first layout
//!   (Java's `CheckpointV5.latestOf`), so the segments of later layouts are read from their start.
//! - **The latest position is resolved** before anything is read: a reader at latest asks for its segment's last
//!   message id once subscribed, and reads after it. A checkpoint taken before its first message names that message,
//!   and a restore reads what the consumer would have read.
//! - **Parents first.** A segment's messages are delivered only once every parent still in the layout is read to its
//!   end, or gone: the parents hold the older messages of the segment's keys. Its reader subscribes at once, and
//!   buffers meanwhile.
//! - **The end of a sealed segment.** Once the layout says a segment is sealed, the consumer asks for its last message
//!   id, which no longer changes: the segment is read to its end once its reader took that message, as Java's probe
//!   finds. The broker would say so itself only to a reader that acknowledges, which these don't. The reader is then
//!   let go of, and the segment's position stays in the checkpoint, so that a restore doesn't read it again.
//! - **A segment that is gone**, its topic deleted while the layout still has it (by the controller's garbage
//!   collection, past retention), is skipped: its subscribe fails with `TopicNotFound`. It leaves the checkpoint and
//!   holds no child back.
//! - **The create is all or nothing**, as in Java: [`Action::Subscribed`] comes once every reader is subscribed and
//!   knows where it starts, those of layouts that came meanwhile included. One that can't subscribe fails it.
//! - **Retries.** A reader that gives up later, because a subscribe failed for good, gets a new one after a backoff
//!   shared by every segment, from 100 ms doubling to 30 s, or at once when a new layout comes. It starts where the
//!   last one stopped. A last message id the broker fails to give is asked again after a second.
//! - **Checkpoints** name, for each segment, the last message that reached the application, or where the segment
//!   starts. The driver says which messages reached it ([`CheckpointConsumer::deliver`]), and which came back from a
//!   receive that went away ([`CheckpointConsumer::rewind`]). A reader resumes after the last message taken from it.
//!   A message carries the attachment of its segment it came from: one taken before its segment left a group
//!   member's assignment and came back moves no checkpoint, as in Java.
//! - **Groups.** A member of a group registers with the topic's controller as a CHECKPOINT consumer, under its name
//!   ([`AssignmentSession`]), and reads the segments the controller assigns it, each whole; a refused registration
//!   fails the create, which completes once the member is registered and every segment of its assignment read.
//!   Closing leaves the group. The controller orders a group's segments in no way, since no cursor shows how far the
//!   group read, and an assignment has no lineage: a member reads each of its segments as it comes, as Java's does,
//!   and asks for no sealed segment's end. A key's messages keep their order within a segment, not across a split or
//!   a merge. A segment that leaves the assignment leaves the checkpoint, and its reader is let go of; one that comes
//!   back gets a new reader once the old one is closed, at the consumer's start, as in Java. A member at the latest
//!   position starts each segment it gets at its end then: the assignment doesn't tell a segment created after the
//!   member started from one moved to it.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::assignment::{self, Assignment, AssignmentSession, Outcome};
use crate::backoff::{Backoff, Jitter};
use crate::checkpoint::Checkpoint;
use crate::consumer::{self, ConsumerOptions, SegmentConsumer, reached};
use crate::entry::ReceivedMessage;
use crate::frame::PayloadSection;
use crate::layout::Layout;
use crate::message_id::{EntryId, SegmentVector};
use crate::producer::{RegisterError, V5_MANAGED_PROPERTY};
use crate::proto::command_subscribe::{InitialPosition, SubType};
use crate::proto::{BaseCommand, CommandMessage, ScalableConsumerAssignment, ScalableConsumerType, ServerError};

/// Java's `ScalableCheckpointConsumer` attaches the readers that failed again from 100 ms up to 30 s.
const RETRY_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// How long to wait before asking again for a last message id the broker failed to give, as Java's probe does.
const ASK_AGAIN: Duration = Duration::from_secs(1);

/// What the Checkpoint consumer asks of its driver. A reader asks what a [`SegmentConsumer`] does
/// ([`consumer::Action`]), with its segment.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// The assignment session's [`assignment::Action::Register`]: a member of a group registers with the topic's
    /// controller, with [`CheckpointConsumer::register_command`]. The loss of its connection is reported with
    /// `on_session_lost`.
    Register { generation: u64 },
    /// The assignment session's [`assignment::Action::Unregister`].
    Unregister { generation: u64 },
    /// Look up `topic`, connect, register a consumer id, and send [`CheckpointConsumer::subscribe_command`]. Report
    /// with `on_subscribed` or `on_subscribe_failed`.
    Connect { segment: u64, generation: u64, topic: String },
    /// Send a command that has no response on the connection of the segment's reader.
    Send { segment: u64, command: Box<BaseCommand> },
    /// Close the segment's reader on the broker with `CommandCloseConsumer`, then report with `on_closed`.
    CloseConsumer { segment: u64, generation: u64 },
    /// Ask for the segment's last message id, with `CommandGetLastMessageId` on its reader's subscription of
    /// `generation`. Report with `on_last_message_id` or `on_last_message_id_failed`.
    GetLastMessageId { segment: u64, generation: u64 },
    /// How the create went: every reader is subscribed, or one couldn't be. Not sent if the consumer is closed before.
    Subscribed(Result<(), RegisterError>),
    /// The consumer is closed: [`CheckpointConsumer::close`] is complete, or the create failed and closed it.
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Subscribing,
    Open,
    Closing,
    Closed,
}

/// Where the segments it reads come from.
#[derive(Debug)]
enum Source {
    /// Every segment of the topic's layout.
    Layout(Rc<Layout>),
    /// The segments the topic's controller assigns to the consumer, a member of a group.
    Group(Box<Group>),
}

#[derive(Debug)]
struct Group {
    /// The topic, as the broker resolved it.
    topic: String,
    session: AssignmentSession,
    assignment: Assignment,
}

impl Source {
    fn session(&self) -> Option<&AssignmentSession> {
        match self {
            Source::Layout(_) => None,
            Source::Group(group) => Some(&group.session),
        }
    }
}

/// Where a segment it reads is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Without a reader: a new segment, which gets one as soon as no reader of a previous assignment is left.
    Waiting,
    /// Its reader gave up: it gets a new one after the backoff, or at once with a new layout or assignment.
    Retrying,
    /// Its reader is subscribing, or subscribed.
    Reading,
    /// Read to its end. Its reader is let go of.
    Drained,
    /// Its topic is gone.
    Gone,
}

/// A segment it reads.
#[derive(Debug)]
struct Track {
    /// Which of its attachments to the segment this is: a segment that leaves and comes back gets a new one.
    attachment: u64,
    phase: Phase,
    /// The last message that reached the application, or where the segment starts.
    checkpoint: EntryId,
    /// Where its next reader starts: after the last message taken from it, or where the segment starts.
    read: EntryId,
    sealed: bool,
    /// The segment's last message, asked once it was sealed.
    end: Option<EntryId>,
}

/// A segment's reader.
#[derive(Debug)]
struct Slot {
    consumer: SegmentConsumer,
    /// Its first subscribe succeeded.
    subscribed: bool,
    /// It is being let go of: its segment is drained or left the layout or the assignment, or the consumer is closing.
    released: bool,
    /// It sent [`consumer::Action::Closed`].
    closed: bool,
    /// A last message id asked on its subscription of this generation, and whether the segment was sealed when it was
    /// asked. A reader is forgotten only once that is answered, so that no later reader of its segment takes the
    /// answer for its own.
    asking: Option<(u64, bool)>,
    /// When to ask again, after the broker failed to answer.
    ask_at: Option<Instant>,
}

/// See the module documentation.
#[derive(Debug)]
pub struct CheckpointConsumer {
    options: ConsumerOptions,
    source: Source,
    state: State,
    /// Where each segment starts: where the start names, or `otherwise` for a segment it doesn't name.
    start: SegmentVector,
    otherwise: EntryId,
    /// The segments it reads.
    tracks: BTreeMap<u64, Track>,
    /// The readers, until they are finished ([`SegmentConsumer::is_finished`]).
    slots: BTreeMap<u64, Slot>,
    /// While creating, the segments whose readers aren't subscribed yet.
    awaiting: BTreeSet<u64>,
    /// When to attach again the readers that gave up. Set while there is one.
    retry_at: Option<Instant>,
    backoff: Backoff,
    jitter: Jitter,
    /// The segment that delivered the last message.
    turn: Option<u64>,
    /// The last attachment given to a segment.
    attachments: u64,
    actions: VecDeque<Action>,
}

impl CheckpointConsumer {
    /// A Checkpoint consumer on the topic's first layout, starting at `start`, attaching a reader to every segment
    /// now. The options' subscription names the readers' subscriptions, which are Exclusive and not durable. Poll
    /// [`CheckpointConsumer::poll_action`] for [`Action::Subscribed`].
    pub fn new(options: ConsumerOptions, start: Checkpoint, layout: Rc<Layout>, now: Instant) -> CheckpointConsumer {
        let start = match start {
            Checkpoint::Earliest => SegmentVector::new(),
            Checkpoint::Latest => layout.segments().map(|s| (s.id as i64, EntryId::LATEST)).collect(),
            Checkpoint::Positions(positions) => positions,
        };
        let mut consumer = CheckpointConsumer::with(options, Source::Layout(layout), start, EntryId::EARLIEST, now);
        consumer.reconcile(now);
        consumer.settle();
        consumer
    }

    /// A Checkpoint consumer of `topic`, the name the broker resolved, as the member `name` of `group`: it registers
    /// now with the topic's controller, and reads the segments it assigns, starting at `start`. Its readers are named
    /// `<name>-seg-<id>`, and their subscriptions after the options'. Poll [`CheckpointConsumer::poll_action`] for
    /// [`Action::Subscribed`].
    pub fn grouped(
        options: ConsumerOptions,
        start: Checkpoint,
        topic: String,
        group: String,
        name: String,
        now: Instant,
    ) -> CheckpointConsumer {
        let (start, otherwise) = match start {
            Checkpoint::Earliest => (SegmentVector::new(), EntryId::EARLIEST),
            Checkpoint::Latest => (SegmentVector::new(), EntryId::LATEST),
            Checkpoint::Positions(positions) => (positions, EntryId::EARLIEST),
        };
        let session = AssignmentSession::new(ScalableConsumerType::CHECKPOINT, topic.clone(), group, name.clone(), now);
        let options = ConsumerOptions { consumer_name: Some(name), ..options };
        let source = Source::Group(Box::new(Group { topic, session, assignment: Assignment::new() }));
        let mut consumer = CheckpointConsumer::with(options, source, start, otherwise, now);
        consumer.pass_session_actions();
        consumer
    }

    fn with(
        options: ConsumerOptions,
        source: Source,
        start: SegmentVector,
        otherwise: EntryId,
        now: Instant,
    ) -> CheckpointConsumer {
        CheckpointConsumer {
            options,
            source,
            state: State::Subscribing,
            start,
            otherwise,
            tracks: BTreeMap::new(),
            slots: BTreeMap::new(),
            awaiting: BTreeSet::new(),
            retry_at: None,
            backoff: Backoff::new(RETRY_BACKOFF.0, RETRY_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            turn: None,
            attachments: 0,
            actions: VecDeque::new(),
        }
    }

    /// The topic, as the broker resolved it.
    pub fn topic(&self) -> &str {
        match &self.source {
            Source::Layout(layout) => layout.topic(),
            Source::Group(group) => &group.topic,
        }
    }

    /// The segments it reads: those with a reader it hasn't let go of.
    pub fn segments(&self) -> impl Iterator<Item = u64> + '_ {
        self.slots.iter().filter(|(_, slot)| !slot.released).map(|(id, _)| *id)
    }

    /// Whether it is closed, or closing: [`CheckpointConsumer::close`] was called, or the create failed.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closing | State::Closed)
    }

    /// Whether it is a member of a group, which follows the assignments of the topic's controller, not its layouts.
    pub fn is_grouped(&self) -> bool {
        matches!(self.source, Source::Group(_))
    }

    /// The `CommandScalableTopicSubscribe` a member of a group registers with, as `consumer_id`; none for a consumer
    /// on its own. The driver sets its request id.
    pub fn register_command(&self, consumer_id: u64) -> Option<BaseCommand> {
        Some(self.source.session()?.register_command(consumer_id))
    }

    /// The `CommandSubscribe` to register `segment`'s reader with, as `consumer_id`. The driver sets its request id.
    pub fn subscribe_command(&self, segment: u64, consumer_id: u64) -> Option<BaseCommand> {
        Some(self.slots.get(&segment)?.consumer.subscribe_command(consumer_id))
    }

    /// Applies a newer layout: attaches readers to the segments it adds, lets go of those it drops, asks for the end of
    /// those it seals, and attaches again, at once, the readers that gave up. A member of a group has no use for it.
    pub fn on_layout(&mut self, layout: Rc<Layout>, now: Instant) {
        let Source::Layout(current) = &self.source else { return };
        if layout.epoch() <= current.epoch() || self.is_closed() {
            return;
        }
        self.source = Source::Layout(layout.clone());
        self.drop_left();
        let sealed: Vec<u64> = self
            .tracks
            .iter_mut()
            .filter(|(id, track)| !track.sealed && !layout.is_active(**id))
            .map(|(id, track)| {
                track.sealed = true;
                *id
            })
            .collect();
        for id in sealed {
            self.ask(id);
        }
        self.reconcile(now);
        self.settle();
    }

    /// The controller registered the member, for the registration of `generation`, with its assignment.
    pub fn on_registered(&mut self, generation: u64, assignment: &ScalableConsumerAssignment, now: Instant) {
        let Source::Group(group) = &mut self.source else { return };
        let outcome = group.session.on_registered(generation, assignment);
        self.take(outcome, now);
    }

    /// The registration of `generation` failed. The first one fails the create; the others are retried.
    pub fn on_register_failed(&mut self, generation: u64, error: RegisterError, now: Instant) {
        let Source::Group(group) = &mut self.source else { return };
        let outcome = group.session.on_register_failed(generation, error, now);
        self.take(outcome, now);
    }

    /// The controller pushed an assignment to the registration of `generation`.
    pub fn on_assignment(&mut self, generation: u64, assignment: &ScalableConsumerAssignment, now: Instant) {
        let Source::Group(group) = &mut self.source else { return };
        let outcome = group.session.on_assignment(generation, assignment);
        self.take(outcome, now);
    }

    /// The connection of the registration of `generation` closed: register again, after the backoff. The readers go
    /// on meanwhile.
    pub fn on_session_lost(&mut self, generation: u64, now: Instant) {
        let Source::Group(group) = &mut self.source else { return };
        group.session.on_lost(generation, now);
    }

    /// The unsubscribe of the registration of `generation` was answered, or failed.
    pub fn on_unregistered(&mut self, generation: u64) {
        let Source::Group(group) = &mut self.source else { return };
        group.session.on_unregistered(generation);
        self.settle();
    }

    /// The broker registered `segment`'s reader of `generation` as `consumer_id`.
    pub fn on_subscribed(&mut self, segment: u64, generation: u64, consumer_id: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribed(generation, consumer_id);
        self.ask(segment);
        self.sync(segment);
        self.settle();
    }

    /// Subscribing `segment`'s reader of `generation` failed.
    pub fn on_subscribe_failed(&mut self, segment: u64, generation: u64, error: RegisterError, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_subscribe_failed(generation, error, now);
        if self.sync(segment) {
            self.schedule_retry(now);
        }
        self.fill(now);
        self.settle();
    }

    /// An entry the broker pushed for `segment`'s reader of `generation`.
    pub fn on_message(&mut self, segment: u64, generation: u64, message: &CommandMessage, section: &PayloadSection) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_message(generation, message, section);
        self.sync(segment);
        self.settle();
    }

    /// The broker says `segment`'s topic ended for its reader.
    pub fn on_reached_end_of_topic(&mut self, segment: u64, generation: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_reached_end_of_topic(generation);
        self.settle();
    }

    /// `segment`'s reader of `generation` lost its connection, or the broker closed it.
    pub fn on_disconnected(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_disconnected(generation, now);
        // The answer to a last message id asked on it won't come.
        slot.asking = slot.asking.filter(|(asked, _)| *asked != generation);
        self.sync(segment);
        self.fill(now);
        self.settle();
    }

    /// Our `CommandCloseConsumer` for `segment`'s reader of `generation` was answered, or its connection closed.
    pub fn on_closed(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_closed(generation);
        self.sync(segment);
        self.fill(now);
        self.settle();
    }

    /// The broker's answer to the last message id asked for `segment` on its reader's subscription of `generation`.
    pub fn on_last_message_id(&mut self, segment: u64, generation: u64, id: EntryId, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        let Some((_, sealed)) = slot.asking.filter(|(asked, _)| *asked == generation) else { return };
        slot.asking = None;
        // A reader let go of only waited for the answer.
        if !slot.released {
            let track = self.tracks.get_mut(&segment).expect("the segment of a reader");
            if sealed {
                track.end = Some(id);
            }
            if slot.consumer.position() == Some(EntryId::LATEST) {
                slot.consumer.resolve(generation, id);
                // Nothing was read from the segment yet: the checkpoint names where it starts.
                track.checkpoint = id;
            }
        }
        // Sealed after it asked: the end is still to ask.
        self.ask(segment);
        self.sync(segment);
        self.fill(now);
        self.settle();
    }

    /// The broker failed to give the last message id asked for `segment` on its reader's subscription of
    /// `generation`: it is asked again in a second.
    pub fn on_last_message_id_failed(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        if slot.asking.is_some_and(|(asked, _)| asked == generation) {
            slot.asking = None;
            slot.ask_at = (!slot.released).then_some(now + ASK_AGAIN);
        }
        self.sync(segment);
        self.fill(now);
        self.settle();
    }

    /// The next message for the application, if one is in. The segments with messages take turns; a segment whose
    /// parents aren't read to their end has none, nor has one it lets go of.
    pub fn next_message(&mut self) -> Option<ReceivedMessage> {
        let after = self.turn.map_or(Bound::Unbounded, Bound::Excluded);
        let segment = self
            .slots
            .range((after, Bound::Unbounded))
            .chain(&self.slots)
            .find(|(id, slot)| slot.consumer.available() > 0 && self.deliverable(**id, slot))
            .map(|(id, _)| *id)?;
        self.turn = Some(segment);
        let mut message = self.slots.get_mut(&segment)?.consumer.next_message()?;
        message.attachment = self.tracks[&segment].attachment;
        self.sync(segment);
        // The segment may be read to its end now, and its children free to go.
        self.settle();
        Some(message)
    }

    /// How many messages are in, ready for the application.
    pub fn available(&self) -> usize {
        let deliverable = self.slots.iter().filter(|(id, slot)| self.deliverable(**id, slot));
        deliverable.map(|(_, slot)| slot.consumer.available()).sum()
    }

    /// A message reached the application: the checkpoint now names it for its segment. Returns what it named before,
    /// to [`rewind`](CheckpointConsumer::rewind) to if the message comes back; `None` for a message of a segment it no
    /// longer reads, or of an earlier attachment to it.
    pub fn deliver(&mut self, message: &ReceivedMessage) -> Option<EntryId> {
        let track = self.track(message.id.segment_id, message.attachment)?;
        Some(std::mem::replace(&mut track.checkpoint, message.id.entry))
    }

    /// Messages of `segment` came back from a receive that went away, the first of them after `position`, all of the
    /// segment's `attachment`: the checkpoint names `position` again, until they reach the application.
    pub fn rewind(&mut self, segment: i64, attachment: u64, position: EntryId) {
        if let Some(track) = self.track(segment, attachment) {
            track.checkpoint = position;
        }
    }

    /// The segment's track, if it is still of `attachment`.
    fn track(&mut self, segment: i64, attachment: u64) -> Option<&mut Track> {
        self.tracks.get_mut(&(segment as u64)).filter(|track| track.attachment == attachment)
    }

    /// Where the application is: for each segment, the last message that reached it, or where the segment starts.
    pub fn checkpoint(&self) -> Checkpoint {
        let positions = self.tracks.iter().filter(|(_, track)| track.phase != Phase::Gone);
        Checkpoint::Positions(positions.map(|(id, track)| (*id as i64, track.checkpoint)).collect())
    }

    /// Closes every reader, and leaves the group. The messages not taken yet are dropped. [`Action::Closed`] follows
    /// once all are closed, and the controller answered. Can be called again.
    pub fn close(&mut self) {
        if self.is_closed() {
            return;
        }
        self.state = State::Closing;
        self.retry_at = None;
        if let Source::Group(group) = &mut self.source {
            group.session.close();
        }
        self.pass_session_actions();
        let ids: Vec<u64> = self.segments().collect();
        for id in ids {
            self.release(id);
            self.sync(id);
        }
        self.settle();
    }

    /// When the consumer next has something to do: a reader's, asking again for a last message id, registering again,
    /// or attaching again the readers that gave up.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let readers = self.slots.values().flat_map(|slot| slot.consumer.poll_timeout().into_iter().chain(slot.ask_at));
        let session = self.source.session().and_then(AssignmentSession::poll_timeout);
        readers.chain(session).chain(self.retry_at).min()
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        let ids: Vec<u64> = self.slots.keys().copied().collect();
        for id in &ids {
            let Some(slot) = self.slots.get_mut(id) else { continue };
            slot.consumer.handle_timeout(now);
            self.sync(*id);
        }
        for id in ids {
            let Some(slot) = self.slots.get_mut(&id).filter(|slot| slot.ask_at.is_some_and(|at| at <= now)) else {
                continue;
            };
            slot.ask_at = None;
            self.ask(id);
        }
        if let Source::Group(group) = &mut self.source {
            group.session.handle_timeout(now);
        }
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
                let Source::Group(group) = &mut self.source else { unreachable!("only a member is assigned") };
                group.assignment = assignment;
                self.drop_left();
                self.reconcile(now);
            }
            Outcome::Failed(error) => {
                self.actions.push_back(Action::Subscribed(Err(error)));
                self.close();
            }
        }
        self.pass_session_actions();
        self.settle();
    }

    fn pass_session_actions(&mut self) {
        let Source::Group(group) = &mut self.source else { return };
        while let Some(action) = group.session.poll_action() {
            self.actions.push_back(match action {
                assignment::Action::Register { generation } => Action::Register { generation },
                assignment::Action::Unregister { generation } => Action::Unregister { generation },
            });
        }
    }

    /// Whether the segment is one it reads: of its layout, or of its assignment.
    fn has(&self, segment: u64) -> bool {
        match &self.source {
            Source::Layout(layout) => layout.segment(segment).is_some(),
            Source::Group(group) => group.assignment.contains_key(&segment),
        }
    }

    /// Lets go of the segments that left the layout or the assignment: they leave the checkpoint, and their readers
    /// are let go of.
    fn drop_left(&mut self) {
        let left: Vec<u64> = self.tracks.keys().copied().filter(|id| !self.has(*id)).collect();
        for id in left {
            self.tracks.remove(&id);
            self.awaiting.remove(&id);
            self.release(id);
            self.sync(id);
        }
    }

    /// Follows the segments of the layout or of the assignment it doesn't yet, and attaches a reader to each one
    /// without one, those whose readers gave up included.
    fn reconcile(&mut self, now: Instant) {
        let segments: Vec<(u64, bool)> = match &self.source {
            Source::Layout(layout) => layout.segments().map(|segment| (segment.id, !segment.active)).collect(),
            // A member doesn't need to know which are sealed: no child waits for them.
            Source::Group(group) => group.assignment.keys().map(|id| (*id, false)).collect(),
        };
        for (id, sealed) in segments {
            let start = self.start.get(&(id as i64)).copied().unwrap_or(self.otherwise);
            let attachments = &mut self.attachments;
            let track = self.tracks.entry(id).or_insert_with(|| Track {
                attachment: {
                    *attachments += 1;
                    *attachments
                },
                phase: Phase::Waiting,
                checkpoint: start,
                read: start,
                sealed,
                end: None,
            });
            if track.phase == Phase::Retrying {
                track.phase = Phase::Waiting;
            }
        }
        // Every segment that wants a reader gets one again.
        self.retry_at = None;
        self.fill(now);
    }

    /// Attaches a reader to each segment waiting for one, but those whose reader of a previous assignment is still
    /// there: the broker would refuse a second reader on the subscription.
    fn fill(&mut self, now: Instant) {
        if self.is_closed() {
            return;
        }
        let waiting =
            self.tracks.iter().filter(|(id, track)| track.phase == Phase::Waiting && !self.slots.contains_key(id));
        let waiting: Vec<u64> = waiting.map(|(id, _)| *id).collect();
        for id in waiting {
            self.attach(id, now);
        }
    }

    /// Attaches a reader to a segment it reads, where its last one stopped.
    fn attach(&mut self, id: u64, now: Instant) {
        let (topic, legacy) = match &self.source {
            Source::Layout(layout) => {
                let segment = layout.segment(id).expect("a segment of the layout");
                (segment.topic.clone(), segment.is_legacy())
            }
            // A legacy segment's topic is the `persistent://` one it wraps.
            Source::Group(group) => {
                let topic = group.assignment[&id].topic.clone();
                let legacy = topic.starts_with("persistent://");
                (topic, legacy)
            }
        };
        let track = self.tracks.get_mut(&id).expect("a segment it reads");
        let mut options = self.options.clone();
        options.sub_type = SubType::Exclusive;
        // A reader at the latest position starts at the end of its segment.
        options.initial_position = InitialPosition::Latest;
        options.consumer_name = self.options.consumer_name.as_ref().map(|name| format!("{name}-seg-{id}"));
        if legacy {
            options.properties.push((V5_MANAGED_PROPERTY.0.into(), V5_MANAGED_PROPERTY.1.into()));
        }
        let consumer = SegmentConsumer::reader(options, id as i64, topic, track.read, now);
        track.phase = Phase::Reading;
        let slot = Slot { consumer, subscribed: false, released: false, closed: false, asking: None, ask_at: None };
        self.slots.insert(id, slot);
        if self.state == State::Subscribing {
            self.awaiting.insert(id);
        }
        self.sync(id);
    }

    /// Lets go of a segment's reader, which isn't yet: closes it.
    fn release(&mut self, segment: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.released = true;
        slot.ask_at = None;
        slot.consumer.close();
    }

    /// Asks for the segment's last message id, if its reader is subscribed and needs it: to know where the latest
    /// position is, or where a sealed segment ends.
    fn ask(&mut self, segment: u64) {
        let (Some(track), Some(slot)) = (self.tracks.get(&segment), self.slots.get_mut(&segment)) else { return };
        let Some(generation) = slot.consumer.subscription().filter(|_| !slot.released) else { return };
        let resolving = slot.consumer.position() == Some(EntryId::LATEST);
        let ending = track.sealed && track.end.is_none();
        if (resolving || ending) && slot.asking.is_none() && slot.ask_at.is_none() {
            slot.asking = Some((generation, track.sealed));
            self.actions.push_back(Action::GetLastMessageId { segment, generation });
        }
    }

    /// Passes on what `segment`'s reader asks, and forgets it once finished. Returns whether it gave up and is to be
    /// attached again after the backoff.
    fn sync(&mut self, segment: u64) -> bool {
        let mut gave_up = false;
        while let Some(slot) = self.slots.get_mut(&segment) {
            if let Some(action) = slot.consumer.poll_action() {
                gave_up |= self.route(segment, action);
                continue;
            }
            if slot.consumer.is_finished() && slot.asking.is_none() {
                let slot = self.slots.remove(&segment).expect("the segment's reader");
                // The next reader starts where this one stopped. One let go of leaves its segment, or ends it.
                if !slot.released
                    && let (Some(track), Some(position)) = (self.tracks.get_mut(&segment), slot.consumer.position())
                {
                    track.read = position;
                }
            }
            break;
        }
        gave_up
    }

    /// Handles an action of `segment`'s reader. Returns whether it gave up and is to be attached again.
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
                self.slots.get_mut(&segment).expect("the segment's reader").subscribed = true;
                self.awaiting.remove(&segment);
                if self.all_subscribed() {
                    self.backoff.reset();
                }
            }
            // The `Closed` that follows says why it gave up.
            consumer::Action::Subscribed(Err(_)) => {}
            consumer::Action::Closed => {
                let slot = self.slots.get_mut(&segment).expect("the segment's reader");
                slot.closed = true;
                if !slot.released {
                    return self.gave_up(segment);
                }
            }
        }
        false
    }

    /// `segment`'s reader gave up: a subscribe failed for good. A segment whose topic is gone is skipped; another
    /// fails the create if it never subscribed while creating, and waits for a new reader otherwise. Returns whether
    /// it waits.
    fn gave_up(&mut self, segment: u64) -> bool {
        let slot = &self.slots[&segment];
        let track = self.tracks.get_mut(&segment).expect("a segment it reads");
        match slot.consumer.failure() {
            Some(RegisterError::Server { error: ServerError::TopicNotFound, .. }) => {
                track.phase = Phase::Gone;
                self.awaiting.remove(&segment);
                false
            }
            Some(error) if !slot.subscribed && self.state == State::Subscribing => {
                self.actions.push_back(Action::Subscribed(Err(error.clone())));
                self.close();
                false
            }
            _ => {
                track.phase = Phase::Retrying;
                true
            }
        }
    }

    /// Whether the messages of a segment's reader may go to the application: it isn't let go of, and every parent the
    /// segment has in the layout is read to its end, or gone. A member of a group knows no parents.
    fn deliverable(&self, segment: u64, slot: &Slot) -> bool {
        let Source::Layout(layout) = &self.source else { return !slot.released };
        let Some(segment) = layout.segment(segment).filter(|_| !slot.released) else { return false };
        segment.parents.iter().all(|parent| {
            self.tracks.get(parent).is_none_or(|track| matches!(track.phase, Phase::Drained | Phase::Gone))
        })
    }

    /// Whether every segment it reads is subscribed.
    fn all_subscribed(&self) -> bool {
        let mut reading = self.tracks.iter().filter(|(_, track)| track.phase == Phase::Reading);
        reading.all(|(id, _)| self.slots.get(id).is_some_and(|slot| slot.subscribed))
    }

    /// A reader gave up: attach a new one after the backoff, with any other that gives up meanwhile.
    fn schedule_retry(&mut self, now: Instant) {
        if self.retry_at.is_none() {
            self.retry_at = Some(now + self.backoff.next_delay(self.jitter.next()));
        }
    }

    /// Lets go of the readers of the segments read to their end, which may free their children's in turn; completes
    /// the create once registered with every reader subscribed, and the close once every reader is closed and the
    /// group left.
    fn settle(&mut self) {
        loop {
            // Once subscribed, a reader at the latest position knows where it is.
            let ended: Vec<u64> = self
                .slots
                .iter()
                .filter(|(id, slot)| slot.subscribed && self.deliverable(**id, slot) && self.read_to_end(**id, slot))
                .map(|(id, _)| *id)
                .collect();
            if ended.is_empty() {
                break;
            }
            for id in ended {
                self.tracks.get_mut(&id).expect("a segment it reads").phase = Phase::Drained;
                self.release(id);
                self.sync(id);
            }
        }
        let registered = self.source.session().is_none_or(AssignmentSession::is_registered);
        let waiting = self.tracks.values().any(|track| track.phase == Phase::Waiting);
        if self.state == State::Subscribing
            && registered
            && !waiting
            && self.awaiting.is_empty()
            && self.retry_at.is_none()
        {
            self.state = State::Open;
            self.actions.push_back(Action::Subscribed(Ok(())));
        }
        let left = self.source.session().is_none_or(AssignmentSession::is_done);
        if self.state == State::Closing && left && self.slots.values().all(|slot| slot.closed) {
            self.state = State::Closed;
            self.actions.push_back(Action::Closed);
        }
    }

    /// Whether a segment's reader took its last message: it reached the end its sealed segment has, or the broker
    /// said the topic ended where it started.
    fn read_to_end(&self, segment: u64, slot: &Slot) -> bool {
        let end = self.tracks.get(&segment).and_then(|track| track.end);
        let at_end = |end: EntryId| slot.consumer.position().is_some_and(|position| reached(&position, &end));
        slot.consumer.is_drained() || end.is_some_and(at_end)
    }
}
