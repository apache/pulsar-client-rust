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

//! The Checkpoint consumer, sans IO (docs/6-scalable-topics.md §6.1).
//!
//! A [`CheckpointConsumer`] reads every segment of a topic's layout, active or sealed, each with a reader
//! ([`SegmentConsumer::reader`]), and keeps where the application is in each: a [`Checkpoint`] to restore it from.
//! Nothing is kept on the broker. The driver feeds it the layouts, what happens to each reader (the outcome of its
//! subscribe, the entries and commands the broker pushes for it), the segments' last message ids it asks for, and
//! the passing of time. The application takes its messages, and the driver tells it which ones reached the
//! application. It hands back [`Action`]s: those of each reader, with its segment, a last message id to ask for, and
//! how the create and the close went.
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

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::ops::Bound;
use std::rc::Rc;
use std::time::{Duration, Instant};

use crate::backoff::{Backoff, Jitter};
use crate::checkpoint::Checkpoint;
use crate::consumer::{self, ConsumerOptions, SegmentConsumer, reached};
use crate::entry::ReceivedMessage;
use crate::frame::PayloadSection;
use crate::layout::Layout;
use crate::message_id::{EntryId, MessageId, SegmentVector};
use crate::producer::{RegisterError, V5_MANAGED_PROPERTY};
use crate::proto::command_subscribe::{InitialPosition, SubType};
use crate::proto::{BaseCommand, CommandMessage, ServerError};

/// Java's `ScalableCheckpointConsumer` attaches the readers that failed again from 100 ms up to 30 s.
const RETRY_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// How long to wait before asking again for a last message id the broker failed to give, as Java's probe does.
const ASK_AGAIN: Duration = Duration::from_secs(1);

/// What the Checkpoint consumer asks of its driver. A reader asks what a [`SegmentConsumer`] does
/// ([`consumer::Action`]), with its segment.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
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

/// Where a segment of the layout is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Phase {
    /// Without a reader: a new segment, or one whose reader gave up.
    Waiting,
    /// Its reader is subscribing, or subscribed.
    Reading,
    /// Read to its end. Its reader is let go of.
    Drained,
    /// Its topic is gone.
    Gone,
}

/// A segment of the layout.
#[derive(Debug)]
struct Track {
    phase: Phase,
    /// The last message that reached the application, or where the segment starts.
    checkpoint: EntryId,
    /// Where its next reader starts: after the last message taken from it, or where the segment starts.
    read: EntryId,
    sealed: bool,
    /// The segment's last message, asked once it was sealed.
    end: Option<EntryId>,
    /// A last message id asked on the reader's subscription of this generation, and whether the segment was sealed
    /// when it was asked.
    asking: Option<(u64, bool)>,
    /// When to ask again, after the broker failed to answer.
    ask_at: Option<Instant>,
}

/// A segment's reader.
#[derive(Debug)]
struct Slot {
    consumer: SegmentConsumer,
    /// Its first subscribe succeeded.
    subscribed: bool,
    /// It is being let go of: its segment is drained or left the layout, or the consumer is closing.
    released: bool,
    /// It sent [`consumer::Action::Closed`].
    closed: bool,
}

/// See the module documentation.
#[derive(Debug)]
pub struct CheckpointConsumer {
    options: ConsumerOptions,
    layout: Rc<Layout>,
    state: State,
    /// Where each segment starts; one it doesn't name starts at the earliest position.
    start: SegmentVector,
    /// The segments of the layout.
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
        let mut consumer = CheckpointConsumer {
            options,
            layout,
            state: State::Subscribing,
            start,
            tracks: BTreeMap::new(),
            slots: BTreeMap::new(),
            awaiting: BTreeSet::new(),
            retry_at: None,
            backoff: Backoff::new(RETRY_BACKOFF.0, RETRY_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            turn: None,
            actions: VecDeque::new(),
        };
        consumer.reconcile(now);
        consumer.settle();
        consumer
    }

    /// The topic, as the broker resolved it.
    pub fn topic(&self) -> &str {
        self.layout.topic()
    }

    /// The segments it reads: those with a reader it hasn't let go of.
    pub fn segments(&self) -> impl Iterator<Item = u64> + '_ {
        self.slots.iter().filter(|(_, slot)| !slot.released).map(|(id, _)| *id)
    }

    /// Whether it is closed, or closing: [`CheckpointConsumer::close`] was called, or the create failed.
    pub fn is_closed(&self) -> bool {
        matches!(self.state, State::Closing | State::Closed)
    }

    /// The `CommandSubscribe` to register `segment`'s reader with, as `consumer_id`. The driver sets its request id.
    pub fn subscribe_command(&self, segment: u64, consumer_id: u64) -> Option<BaseCommand> {
        Some(self.slots.get(&segment)?.consumer.subscribe_command(consumer_id))
    }

    /// Applies a newer layout: attaches readers to the segments it adds, lets go of those it drops, asks for the end of
    /// those it seals, and attaches again, at once, the readers that gave up.
    pub fn on_layout(&mut self, layout: Rc<Layout>, now: Instant) {
        if layout.epoch() <= self.layout.epoch() || self.is_closed() {
            return;
        }
        self.layout = layout;
        let layout = self.layout.clone();
        self.awaiting.retain(|id| layout.segment(*id).is_some());
        let dropped: Vec<u64> = self.tracks.keys().copied().filter(|id| layout.segment(*id).is_none()).collect();
        for id in dropped {
            self.tracks.remove(&id);
            self.release(id);
            self.sync(id);
        }
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
        if let Some(track) = self.tracks.get_mut(&segment) {
            track.asking = track.asking.filter(|(asked, _)| *asked != generation);
        }
        self.sync(segment);
        self.settle();
    }

    /// Our `CommandCloseConsumer` for `segment`'s reader of `generation` was answered, or its connection closed.
    pub fn on_closed(&mut self, segment: u64, generation: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.consumer.on_closed(generation);
        self.sync(segment);
        self.settle();
    }

    /// The broker's answer to the last message id asked for `segment` on its reader's subscription of `generation`.
    pub fn on_last_message_id(&mut self, segment: u64, generation: u64, id: EntryId) {
        let Some(track) = self.tracks.get_mut(&segment) else { return };
        let Some((_, sealed)) = track.asking.filter(|(asked, _)| *asked == generation) else { return };
        track.asking = None;
        if sealed {
            track.end = Some(id);
        }
        if let Some(slot) = self.slots.get_mut(&segment)
            && slot.consumer.position() == Some(EntryId::LATEST)
        {
            slot.consumer.resolve(generation, id);
            // Nothing was read from the segment yet: the checkpoint names where it starts.
            track.checkpoint = id;
        }
        // Sealed after it asked: the end is still to ask.
        self.ask(segment);
        self.sync(segment);
        self.settle();
    }

    /// The broker failed to give the last message id asked for `segment` on its reader's subscription of
    /// `generation`: it is asked again in a second.
    pub fn on_last_message_id_failed(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(track) = self.tracks.get_mut(&segment) else { return };
        if track.asking.is_some_and(|(asked, _)| asked == generation) {
            track.asking = None;
            track.ask_at = Some(now + ASK_AGAIN);
        }
    }

    /// The next message for the application, if one is in. The segments with messages take turns; a segment whose
    /// parents aren't read to their end has none.
    pub fn next_message(&mut self) -> Option<ReceivedMessage> {
        let after = self.turn.map_or(Bound::Unbounded, Bound::Excluded);
        let segment = self
            .slots
            .range((after, Bound::Unbounded))
            .chain(&self.slots)
            .find(|(id, slot)| slot.consumer.available() > 0 && self.deliverable(**id))
            .map(|(id, _)| *id)?;
        self.turn = Some(segment);
        let message = self.slots.get_mut(&segment)?.consumer.next_message();
        self.sync(segment);
        // The segment may be read to its end now, and its children free to go.
        self.settle();
        message
    }

    /// How many messages are in, ready for the application.
    pub fn available(&self) -> usize {
        self.slots.iter().filter(|(id, _)| self.deliverable(**id)).map(|(_, slot)| slot.consumer.available()).sum()
    }

    /// A message reached the application: the checkpoint now names it for its segment. Returns what it named before,
    /// to [`rewind`](CheckpointConsumer::rewind) to if the message comes back; `None` for a segment it no longer
    /// reads.
    pub fn deliver(&mut self, id: &MessageId) -> Option<EntryId> {
        let track = self.tracks.get_mut(&(id.segment_id as u64))?;
        Some(std::mem::replace(&mut track.checkpoint, id.entry))
    }

    /// Messages of `segment` came back from a receive that went away, the first of them after `position`: the
    /// checkpoint names `position` again, until they reach the application.
    pub fn rewind(&mut self, segment: i64, position: EntryId) {
        if let Some(track) = self.tracks.get_mut(&(segment as u64)) {
            track.checkpoint = position;
        }
    }

    /// Where the application is: for each segment, the last message that reached it, or where the segment starts.
    pub fn checkpoint(&self) -> Checkpoint {
        let positions = self.tracks.iter().filter(|(_, track)| track.phase != Phase::Gone);
        Checkpoint::Positions(positions.map(|(id, track)| (*id as i64, track.checkpoint)).collect())
    }

    /// Closes every reader. The messages not taken yet are dropped. [`Action::Closed`] follows once all are closed.
    /// Can be called again.
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

    /// When the consumer next has something to do: a reader's, asking again for a last message id, or attaching
    /// again the readers that gave up.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let readers = self.slots.values().filter_map(|slot| slot.consumer.poll_timeout());
        let asks = self.tracks.values().filter_map(|track| track.ask_at);
        readers.chain(asks).chain(self.retry_at).min()
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        let ids: Vec<u64> = self.slots.keys().copied().collect();
        for id in ids {
            let Some(slot) = self.slots.get_mut(&id) else { continue };
            slot.consumer.handle_timeout(now);
            self.sync(id);
        }
        let due: Vec<u64> = self
            .tracks
            .iter()
            .filter(|(_, track)| track.ask_at.is_some_and(|at| at <= now))
            .map(|(id, _)| *id)
            .collect();
        for id in due {
            self.tracks.get_mut(&id).expect("a segment of the layout").ask_at = None;
            self.ask(id);
        }
        if self.retry_at.is_some_and(|at| at <= now) {
            self.reconcile(now);
        }
        self.settle();
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    /// Follows the segments of the layout it doesn't yet, and attaches a reader to each one waiting for one.
    fn reconcile(&mut self, now: Instant) {
        let layout = self.layout.clone();
        for segment in layout.segments() {
            let start = self.start.get(&(segment.id as i64)).copied().unwrap_or(EntryId::EARLIEST);
            let track = self.tracks.entry(segment.id).or_insert_with(|| Track {
                phase: Phase::Waiting,
                checkpoint: start,
                read: start,
                sealed: !segment.active,
                end: None,
                asking: None,
                ask_at: None,
            });
            if track.phase == Phase::Waiting {
                self.attach(segment.id, now);
            }
        }
        // Every segment that wants a reader has one again.
        self.retry_at = None;
    }

    /// Attaches a reader to a segment of the layout, where its last one stopped.
    fn attach(&mut self, id: u64, now: Instant) {
        let segment = self.layout.segment(id).expect("a segment of the layout");
        let track = self.tracks.get_mut(&id).expect("a segment of the layout");
        let mut options = self.options.clone();
        options.sub_type = SubType::Exclusive;
        // A reader at the latest position starts at the end of its segment.
        options.initial_position = InitialPosition::Latest;
        options.consumer_name = self.options.consumer_name.as_ref().map(|name| format!("{name}-seg-{id}"));
        if segment.is_legacy() {
            options.properties.push((V5_MANAGED_PROPERTY.0.into(), V5_MANAGED_PROPERTY.1.into()));
        }
        let consumer = SegmentConsumer::reader(options, id as i64, segment.topic.clone(), track.read, now);
        track.phase = Phase::Reading;
        track.asking = None;
        track.ask_at = None;
        self.slots.insert(id, Slot { consumer, subscribed: false, released: false, closed: false });
        if self.state == State::Subscribing {
            self.awaiting.insert(id);
        }
        self.sync(id);
    }

    /// Lets go of a segment's reader, which isn't yet: closes it.
    fn release(&mut self, segment: u64) {
        let Some(slot) = self.slots.get_mut(&segment) else { return };
        slot.released = true;
        slot.consumer.close();
    }

    /// Asks for the segment's last message id, if its reader is subscribed and needs it: to know where the latest
    /// position is, or where a sealed segment ends.
    fn ask(&mut self, segment: u64) {
        let (Some(track), Some(slot)) = (self.tracks.get_mut(&segment), self.slots.get(&segment)) else { return };
        let Some(generation) = slot.consumer.subscription().filter(|_| !slot.released) else { return };
        let resolving = slot.consumer.position() == Some(EntryId::LATEST);
        let ending = track.sealed && track.end.is_none();
        if (resolving || ending) && track.asking.is_none() && track.ask_at.is_none() {
            track.asking = Some((generation, track.sealed));
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
            if slot.consumer.is_finished() {
                let slot = self.slots.remove(&segment).expect("the segment's reader");
                // The next reader starts where this one stopped.
                if let (Some(track), Some(position)) = (self.tracks.get_mut(&segment), slot.consumer.position()) {
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
        let track = self.tracks.get_mut(&segment).expect("a segment of the layout");
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
                track.phase = Phase::Waiting;
                true
            }
        }
    }

    /// Whether the segment's messages may go to the application: every parent it has in the layout is read to its
    /// end, or gone.
    fn deliverable(&self, segment: u64) -> bool {
        let Some(segment) = self.layout.segment(segment) else { return false };
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
    /// the create once every reader is subscribed, and the close once every reader is closed.
    fn settle(&mut self) {
        loop {
            // Once subscribed, a reader at the latest position knows where it is.
            let ended: Vec<u64> = self
                .slots
                .iter()
                .filter(|(id, slot)| {
                    slot.subscribed && !slot.released && self.deliverable(**id) && self.read_to_end(**id, slot)
                })
                .map(|(id, _)| *id)
                .collect();
            if ended.is_empty() {
                break;
            }
            for id in ended {
                self.tracks.get_mut(&id).expect("a segment of the layout").phase = Phase::Drained;
                self.release(id);
                self.sync(id);
            }
        }
        if self.state == State::Subscribing && self.awaiting.is_empty() && self.retry_at.is_none() {
            self.state = State::Open;
            self.actions.push_back(Action::Subscribed(Ok(())));
        }
        if self.state == State::Closing && self.slots.values().all(|slot| slot.closed) {
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
