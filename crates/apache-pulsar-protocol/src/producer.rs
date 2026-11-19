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

//! The scalable producer, sans IO (docs/5-segment-engine.md §2, docs/6-scalable-topics.md §3).
//!
//! One [`Producer`] holds every per-segment producer of a scalable producer: its registration, sequence ids and
//! queue of messages. The driver feeds it the application's sends, the layouts of the topic, the outcome of each
//! segment's registration, what the brokers push, and the passing of time; it hands back [`Action`]s: connect a
//! segment, write a frame, close a segment's producer, finish a send.
//!
//! It follows the Java client (`ScalableTopicProducer` over v4 `ProducerImpl`s):
//!
//! - **Routing** is the layout's (*client-behavior* §2). A segment's producer is created on the first message
//!   routed to it, or for every active segment under an exclusive access mode, so that a conflict fails `create`.
//! - **Each segment has its own producer**, named `<name>-seg-<id>` when the application named the producer, with
//!   its own sequence ids. A message gets its sequence id when it is first written, and keeps it, and its bytes,
//!   when it is written again after a reconnect.
//! - **Every entry is a batch of one message**, stamped with the key's entry-bucket hash (*key-shared* §3).
//! - **A segment that is gone** (its topic terminated by a split, a merge or a migration) retires: its messages
//!   that the broker didn't store are parked, and new messages whose keys it owned are held. Once the layout no
//!   longer has the segment and none of its messages is in flight, the parked messages are routed again in call
//!   order, then the held ones. That keeps per-key order across a layout change (*client-behavior* §3–§4). A message
//!   is parked at most ten times; a segment still in the layout gets ten checks, `min(100 ms × n, 500 ms)` apart,
//!   before its parked messages fail.
//! - **Close** waits for what the brokers already have (a broker answers `CommandCloseProducer` once its pending
//!   writes are done) and fails, with [`SendError::AlreadyClosed`], the messages that are still waiting.

use std::collections::{BTreeMap, VecDeque};
use std::rc::Rc;
use std::time::{Duration, Instant};

use buffa::Message;
use bytes::{BufMut, Bytes, BytesMut};

use crate::backoff::Backoff;
use crate::hash::KeyHash;
use crate::layout::{Layout, LayoutError};
use crate::message_id::{EntryId, MessageId};
use crate::proto::base_command::Type;
use crate::proto::{
    BaseCommand, CommandProducer, CommandProducerSuccess, CommandSend, CommandSendError, CommandSendReceipt, KeyValue,
    MessageMetadata, ProducerAccessMode, Schema, ServerError, SingleMessageMetadata,
};

/// How many times a message is parked because its segment is gone before its send fails (Java's
/// `SEND_RETRY_MAX_ATTEMPTS`).
pub const MAX_SEND_ATTEMPTS: u32 = 10;

/// The property that marks a producer on a legacy segment as managed by a V5 client (PIP-475).
pub const V5_MANAGED_PROPERTY: (&str, &str) = ("__pulsar.v5.managed", "true");

const RECONNECT_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(60));

/// The producer's configuration.
#[derive(Debug, Clone)]
pub struct ProducerOptions {
    /// The name the application gave the producer. Segment producers are named `<name>-seg-<id>`; without a
    /// name, each broker picks one.
    pub name: Option<String>,
    pub access_mode: ProducerAccessMode,
    /// How long a message may wait for its receipt once it is on a segment's producer; `None` for no limit.
    pub send_timeout: Option<Duration>,
    /// The sequence id before the first message's: the first is this plus one.
    pub initial_sequence_id: Option<i64>,
    /// The producer's properties, sent on registration.
    pub properties: Vec<(String, String)>,
    /// The schema declared on registration; `None` for raw bytes.
    pub schema: Option<Schema>,
    /// How long the first registration of a segment's producer may keep failing with errors worth retrying.
    pub operation_timeout: Duration,
}

/// A message to publish.
#[derive(Debug, Clone, Default)]
pub struct OutgoingMessage {
    pub key: Option<String>,
    pub payload: Bytes,
    pub properties: Vec<(String, String)>,
    /// Milliseconds since the epoch.
    pub event_time: Option<u64>,
    /// A sequence id set by the application, instead of the segment producer's next one.
    pub sequence_id: Option<u64>,
    /// When the message may be delivered, in milliseconds since the epoch.
    pub deliver_at: Option<i64>,
    /// The clusters to replicate to, instead of the namespace's.
    pub replicate_to: Option<Vec<String>>,
}

/// Identifies one send, in call order.
pub type SendId = u64;

/// Why a send, a registration or the producer failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum SendError {
    #[error("the producer is closed")]
    AlreadyClosed,
    #[error("the message was not stored within the send timeout")]
    Timeout,
    #[error("{0}")]
    Routing(#[from] LayoutError),
    #[error("the message is {size} bytes, more than the broker's limit of {limit}")]
    TooLarge { size: usize, limit: usize },
    #[error("broker error {error:?}: {message}")]
    Server { error: ServerError, message: String },
    /// Registering a segment's producer kept failing with errors worth retrying.
    #[error("{0}")]
    Unavailable(String),
}

/// Why registering a segment's producer failed, as the driver saw it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum RegisterError {
    /// The broker refused it.
    Server { error: ServerError, message: String },
    /// No connection, or no answer: worth retrying.
    Unavailable(String),
}

/// What the driver must do.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Register a producer for `segment`: look up `topic`, connect, allocate a producer id, then send
    /// [`Producer::register_command`]. Answer with [`Producer::on_registered`] or [`Producer::on_register_failed`].
    Connect { segment: u64, generation: u64, topic: String },
    /// Write a `CommandSend` on the segment's connection. Boxed: a `BaseCommand` holds every command inline.
    Send { segment: u64, command: Box<BaseCommand>, metadata: Box<MessageMetadata>, payload: Bytes },
    /// Close the segment's producer with `CommandCloseProducer`, then call [`Producer::on_producer_closed`].
    CloseProducer { segment: u64, generation: u64 },
    /// The broker and the segment's producer disagree on what was sent: close the connection, so that the
    /// producer registers again and writes its messages again.
    Disconnect { segment: u64, generation: u64 },
    /// A send is over.
    Done { send: SendId, result: Result<MessageId, SendError> },
    /// The producer is created, or failed to be. Under an exclusive access mode this waits for every active segment.
    Created(Result<(), SendError>),
    /// [`Producer::close`] is complete.
    Closed,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    /// Not registered, and not trying; `retry_at` when waiting for the backoff.
    Idle {
        retry_at: Option<Instant>,
    },
    Connecting {
        generation: u64,
    },
    Ready {
        generation: u64,
        producer_id: u64,
    },
    /// Our `CommandCloseProducer` is out.
    Closing {
        generation: u64,
    },
    /// The segment's topic was terminated: nothing more goes to it.
    Gone,
    Closed,
}

/// A message on a segment's queue.
#[derive(Debug)]
struct Queued {
    send: SendId,
    /// Set when the message is first written: its sequence id, metadata and payload, written again unchanged
    /// after a reconnect.
    frame: Option<(u64, MessageMetadata, Bytes)>,
    /// Whether it was written on the current registration, so a receipt or an error may come for it.
    sent: bool,
    deadline: Option<Instant>,
}

#[derive(Debug)]
struct Segment {
    topic: String,
    legacy: bool,
    link: Link,
    /// Registration attempts so far: `CommandProducer.epoch`.
    attempts: u64,
    topic_epoch: Option<u64>,
    /// The name the broker confirmed.
    name: Option<String>,
    schema_version: Option<Vec<u8>>,
    max_message_size: usize,
    next_sequence: u64,
    last_published: i64,
    registered_once: bool,
    /// When the first registration stops retrying.
    first_deadline: Instant,
    backoff: Backoff,
    queue: VecDeque<Queued>,
    /// The segment is gone, or the layout dropped it with messages on it: see the module documentation.
    retiring: bool,
    parked: Vec<SendId>,
    /// The next retirement check of a segment that is gone while the layout still has it: `(check, at)`.
    retirement_check: Option<(u32, Instant)>,
    /// The layout dropped it.
    dropped: bool,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Place {
    Nowhere,
    Queued(u64),
    Parked(u64),
    Held,
}

#[derive(Debug)]
struct Pending {
    message: OutgoingMessage,
    /// Milliseconds since the epoch, at the call.
    publish_time: u64,
    place: Place,
    attempts: u32,
    last_error: Option<SendError>,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    Creating,
    Open,
    Closing,
    Closed,
}

/// See the module documentation.
#[derive(Debug)]
pub struct Producer {
    options: ProducerOptions,
    layout: Rc<Layout>,
    state: State,
    segments: BTreeMap<u64, Segment>,
    sends: BTreeMap<SendId, Pending>,
    /// New sends held while segments retire, in call order.
    held: VecDeque<SendId>,
    /// The retiring segments since the hold started, with the layout each was active in, or `None` when unknown.
    hold_scopes: Vec<(u64, Option<Rc<Layout>>)>,
    holding: bool,
    /// Segments whose producers must all register before `create` completes.
    awaiting_create: Vec<u64>,
    actions: VecDeque<Action>,
    next_send: SendId,
    next_generation: u64,
    round_robin: u64,
    random: u64,
    /// How many times a send was parked or held, for tests and metrics.
    waited: u64,
}

impl Producer {
    /// A producer on the topic's first layout. Poll [`Producer::poll_action`] for [`Action::Created`].
    pub fn new(options: ProducerOptions, layout: Rc<Layout>, now: Instant) -> Producer {
        let mut producer = Producer {
            options,
            layout,
            state: State::Creating,
            segments: BTreeMap::new(),
            sends: BTreeMap::new(),
            held: VecDeque::new(),
            hold_scopes: Vec::new(),
            holding: false,
            awaiting_create: Vec::new(),
            actions: VecDeque::new(),
            next_send: 0,
            next_generation: 0,
            round_robin: 0,
            random: now.elapsed().as_nanos() as u64 | 1,
            waited: 0,
        };
        if producer.exclusive() {
            let active: Vec<u64> = producer.layout.active().map(|s| s.id).collect();
            for &id in &active {
                producer.segment(id, now);
            }
            producer.awaiting_create = active;
        } else {
            producer.state = State::Open;
            producer.actions.push_back(Action::Created(Ok(())));
        }
        producer
    }

    /// The topic, as the broker resolved it.
    pub fn topic(&self) -> &str {
        self.layout.topic()
    }

    /// The highest sequence id the brokers confirmed, over the segments' producers, or the initial one.
    pub fn last_sequence_id(&self) -> i64 {
        let initial = self.options.initial_sequence_id.unwrap_or(-1);
        self.segments.values().map(|s| s.last_published).fold(initial, i64::max)
    }

    /// Sends not finished yet.
    pub fn outstanding(&self) -> usize {
        self.sends.len()
    }

    /// How many times a send had to wait behind a segment that went away: parked or held.
    pub fn sends_parked_or_held(&self) -> u64 {
        self.waited
    }

    /// The oldest send not finished yet.
    pub fn oldest_outstanding(&self) -> Option<SendId> {
        self.sends.keys().next().copied()
    }

    /// Queues a message. `publish_time` is the wall clock, in milliseconds since the epoch.
    pub fn send(&mut self, message: OutgoingMessage, publish_time: u64, now: Instant) -> Result<SendId, SendError> {
        if matches!(self.state, State::Closing | State::Closed) {
            return Err(SendError::AlreadyClosed);
        }
        let id = self.next_send;
        self.next_send += 1;
        let pending = Pending { message, publish_time, place: Place::Nowhere, attempts: 0, last_error: None };
        self.sends.insert(id, pending);
        self.dispatch(id, false, now);
        self.release(now);
        Ok(id)
    }

    /// Applies a newer layout.
    pub fn on_layout(&mut self, layout: Rc<Layout>, now: Instant) {
        if layout.epoch() <= self.layout.epoch() || matches!(self.state, State::Closed) {
            return;
        }
        let old = std::mem::replace(&mut self.layout, layout);
        let dropped: Vec<u64> = self.segments.keys().copied().filter(|id| !self.layout.is_active(*id)).collect();
        for id in dropped {
            self.drop_segment(id, &old, now);
        }
        // Under an exclusive access mode, claim the new segments too. While creating, `create` waits for those
        // instead of the dropped ones; once created, a failure shows on the sends routed there.
        if self.exclusive() && matches!(self.state, State::Creating | State::Open) {
            let active: Vec<u64> = self.layout.active().map(|s| s.id).collect();
            for &id in &active {
                if self.state == State::Creating && !self.segments.contains_key(&id) {
                    self.awaiting_create.push(id);
                }
                self.segment(id, now);
            }
            if self.state == State::Creating {
                self.awaiting_create.retain(|id| active.contains(id));
            }
        }
        self.release(now);
    }

    /// The `CommandProducer` registering `segment`'s producer as `producer_id`; the driver sets the request id.
    pub fn register_command(&self, segment: u64, producer_id: u64) -> Option<BaseCommand> {
        let s = self.segments.get(&segment)?;
        let mut metadata: Vec<KeyValue> = self.options.properties.iter().map(|(k, v)| key_value(k, v)).collect();
        if s.legacy {
            metadata.push(key_value(V5_MANAGED_PROPERTY.0, V5_MANAGED_PROPERTY.1));
        }
        let configured = self.options.name.as_ref().map(|name| format!("{name}-seg-{segment}"));
        let producer = CommandProducer {
            topic: s.topic.clone(),
            producer_id,
            producer_name: configured.clone().or_else(|| s.name.clone()),
            user_provided_producer_name: Some(configured.is_some()),
            encrypted: Some(false),
            metadata,
            schema: self.options.schema.clone().into(),
            epoch: Some(s.attempts.saturating_sub(1)),
            producer_access_mode: Some(self.options.access_mode),
            topic_epoch: s.topic_epoch,
            txn_enabled: Some(false),
            ..Default::default()
        };
        Some(BaseCommand { r#type: Type::PRODUCER.into(), producer: producer.into(), ..Default::default() })
    }

    /// The broker registered `segment`'s producer. `max_message_size` is the connection's.
    pub fn on_registered(
        &mut self,
        segment: u64,
        generation: u64,
        producer_id: u64,
        success: &CommandProducerSuccess,
        max_message_size: usize,
        now: Instant,
    ) {
        let initial = self.options.initial_sequence_id;
        let send_timeout = self.options.send_timeout;
        let closing = matches!(self.state, State::Closing | State::Closed);
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if s.link != (Link::Connecting { generation }) {
            return;
        }
        if closing || s.dropped {
            // Too late: close it unused.
            s.link = Link::Closing { generation };
            self.actions.push_back(Action::CloseProducer { segment, generation });
            return;
        }
        s.link = Link::Ready { generation, producer_id };
        s.name = Some(success.producer_name.clone());
        s.schema_version = success.schema_version.clone().filter(|v| !v.is_empty());
        s.topic_epoch = success.topic_epoch;
        s.max_message_size = max_message_size;
        s.registered_once = true;
        s.backoff.reset();
        // A fresh producer takes the broker's last sequence id, as Java's does, unless one was configured.
        if s.next_sequence == 0 && initial.is_none() {
            let last = success.last_sequence_id.unwrap_or(-1);
            s.last_published = last;
            s.next_sequence = (last + 1).max(0) as u64;
        }
        if let Some(timeout) = send_timeout {
            for queued in &mut s.queue {
                queued.deadline.get_or_insert(now + timeout);
            }
        }
        if self.state == State::Creating {
            self.awaiting_create.retain(|&id| id != segment);
            if self.awaiting_create.is_empty() {
                self.state = State::Open;
                self.actions.push_back(Action::Created(Ok(())));
            }
        }
        self.write(segment);
    }

    /// Registering `segment`'s producer failed.
    pub fn on_register_failed(&mut self, segment: u64, generation: u64, error: RegisterError, now: Instant) {
        let state = self.state;
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if s.link != (Link::Connecting { generation }) {
            return;
        }
        let error = match error {
            RegisterError::Server { error: ServerError::TopicTerminatedError, message } => {
                return self.segment_gone(
                    segment,
                    SendError::Server { error: ServerError::TopicTerminatedError, message },
                    now,
                );
            }
            RegisterError::Server { error, message } if !retriable(error) => SendError::Server { error, message },
            RegisterError::Server { error, message } => {
                if s.registered_once || now < s.first_deadline {
                    return self.retry_later(segment, now);
                }
                SendError::Unavailable(format!("{error:?}: {message}"))
            }
            RegisterError::Unavailable(reason) => {
                if s.registered_once || now < s.first_deadline {
                    return self.retry_later(segment, now);
                }
                SendError::Unavailable(reason)
            }
        };
        // Refused for good: its messages fail, and a later message tries again.
        s.link = Link::Idle { retry_at: None };
        let failed: Vec<SendId> = s.queue.drain(..).map(|q| q.send).collect();
        for send in failed {
            self.fail(send, error.clone());
        }
        if state == State::Creating {
            // As in Java, a producer that can't be created closes: the segments it did claim are let go.
            self.awaiting_create.clear();
            self.state = State::Open;
            self.actions.push_back(Action::Created(Err(error)));
            self.close(now);
            return;
        }
        self.release(now);
    }

    /// A `CommandSendReceipt` for `segment`'s producer.
    pub fn on_receipt(&mut self, segment: u64, generation: u64, receipt: &CommandSendReceipt, now: Instant) {
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if !s.is_current(generation) {
            return;
        }
        let Some(head) = s.queue.front().filter(|q| q.sent) else { return };
        let head_sequence = head.frame.as_ref().map(|(sequence, ..)| *sequence).unwrap_or_default();
        if receipt.sequence_id < head_sequence {
            // For a message that timed out already.
            return;
        }
        if receipt.sequence_id > head_sequence {
            self.actions.push_back(Action::Disconnect { segment, generation });
            return;
        }
        let head = s.queue.pop_front().unwrap();
        let highest = receipt.highest_sequence_id.map(|h| h as i64).unwrap_or(-1);
        s.last_published = s.last_published.max(receipt.sequence_id as i64).max(highest);
        let entry = receipt.message_id.as_option().map_or(EntryId::EARLIEST, |id| EntryId {
            ledger_id: id.ledgerId as i64,
            entry_id: id.entryId as i64,
            partition: -1,
            batch_index: 0,
            batch_size: 1,
        });
        self.finish(head.send, Ok(MessageId::new(segment as i64, entry)));
        self.after_resolution(segment, now);
    }

    /// A `CommandSendError` for `segment`'s producer.
    pub fn on_send_error(&mut self, segment: u64, generation: u64, error: &CommandSendError, now: Instant) {
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if !s.is_current(generation) {
            return;
        }
        let head = s.queue.front().filter(|q| q.sent).map(|q| (q.send, q.frame.as_ref().map(|f| f.0)));
        match error.error {
            ServerError::TopicTerminatedError => {
                let gone = SendError::Server { error: error.error, message: error.message.clone() };
                self.segment_gone(segment, gone, now);
            }
            ServerError::NotAllowedError => {
                if let Some((send, Some(sequence))) = head
                    && sequence == error.sequence_id
                {
                    s.queue.pop_front();
                    self.fail(send, SendError::Server { error: error.error, message: error.message.clone() });
                    self.after_resolution(segment, now);
                }
            }
            // A checksum error, or anything else: register again and write everything again, as Java does.
            _ => self.actions.push_back(Action::Disconnect { segment, generation }),
        }
    }

    /// `segment`'s producer lost its connection, or the broker closed it (`CommandCloseProducer`).
    pub fn on_disconnected(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(s) = self.segments.get_mut(&segment) else { return };
        match s.link {
            Link::Closing { generation: g } if g == generation => self.on_producer_closed(segment, generation, now),
            Link::Ready { generation: g, .. } | Link::Connecting { generation: g } if g == generation => {
                for queued in &mut s.queue {
                    queued.sent = false;
                }
                if s.dropped {
                    // The layout dropped it: nothing it had unacknowledged was stored after the close.
                    self.retire_queue(segment, now);
                } else {
                    self.retry_later(segment, now);
                }
            }
            _ => {}
        }
    }

    /// Our `CommandCloseProducer` for `segment` was answered, or its connection closed.
    pub fn on_producer_closed(&mut self, segment: u64, generation: u64, now: Instant) {
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if s.link != (Link::Closing { generation }) {
            return;
        }
        s.link = Link::Closed;
        // The broker answers once its pending writes are done: what is left was not stored.
        if matches!(self.state, State::Closing | State::Closed) {
            let left: Vec<SendId> = s.queue.drain(..).map(|q| q.send).collect();
            for send in left {
                self.fail(send, SendError::AlreadyClosed);
            }
        } else {
            self.retire_queue(segment, now);
        }
        self.forget_if_done(segment);
        self.check_closed();
        self.release(now);
    }

    /// Starts closing: new sends fail, waiting ones fail, and each segment's producer closes once the broker has
    /// answered what it was sent.
    pub fn close(&mut self, now: Instant) {
        if matches!(self.state, State::Closing | State::Closed) {
            return;
        }
        let creating = self.state == State::Creating;
        self.state = State::Closing;
        if creating {
            self.awaiting_create.clear();
            self.actions.push_back(Action::Created(Err(SendError::AlreadyClosed)));
        }
        let waiting: Vec<SendId> = self
            .sends
            .iter()
            .filter(|(_, p)| matches!(p.place, Place::Held | Place::Parked(_)))
            .map(|(id, _)| *id)
            .collect();
        for send in waiting {
            self.fail(send, SendError::AlreadyClosed);
        }
        self.clear_hold();
        let ids: Vec<u64> = self.segments.keys().copied().collect();
        for id in ids {
            let s = self.segments.get_mut(&id).unwrap();
            s.parked.clear();
            s.retiring = false;
            s.retirement_check = None;
            let failed: Vec<SendId> = match s.link {
                Link::Ready { generation, .. } => {
                    // What was written waits for its receipt; what wasn't won't be written.
                    let unsent = s.queue.iter().filter(|q| !q.sent).map(|q| q.send).collect();
                    s.queue.retain(|q| q.sent);
                    s.link = Link::Closing { generation };
                    self.actions.push_back(Action::CloseProducer { segment: id, generation });
                    unsent
                }
                // A registration still running is closed when it completes (`on_registered`).
                Link::Connecting { .. } | Link::Closing { .. } => s.queue.drain(..).map(|q| q.send).collect(),
                Link::Idle { .. } | Link::Gone | Link::Closed => {
                    s.link = Link::Closed;
                    s.queue.drain(..).map(|q| q.send).collect()
                }
            };
            for send in failed {
                self.fail(send, SendError::AlreadyClosed);
            }
        }
        let _ = now;
        self.check_closed();
    }

    /// When [`Producer::handle_timeout`] must be called next.
    pub fn poll_timeout(&self) -> Option<Instant> {
        self.segments
            .values()
            .flat_map(|s| {
                let retry = match s.link {
                    Link::Idle { retry_at } => retry_at,
                    _ => None,
                };
                let deadline = s.queue.front().and_then(|q| q.deadline);
                [retry, deadline, s.retirement_check.map(|(_, at)| at)]
            })
            .flatten()
            .min()
    }

    /// Lets time pass: reconnects segments whose backoff is over, fails messages past their send timeout, and runs
    /// the retirement checks.
    pub fn handle_timeout(&mut self, now: Instant) {
        let exclusive = self.exclusive();
        let ids: Vec<u64> = self.segments.keys().copied().collect();
        for id in ids {
            let Some(s) = self.segments.get_mut(&id) else { continue };
            if let Link::Idle { retry_at: Some(at) } = s.link
                && at <= now
            {
                if s.queue.is_empty() && !exclusive {
                    // Nothing to send: connect again when something is.
                    s.link = Link::Idle { retry_at: None };
                } else {
                    self.connect(id, now);
                }
            }
            let s = self.segments.get_mut(&id).unwrap();
            let mut expired = Vec::new();
            while s.queue.front().is_some_and(|q| q.deadline.is_some_and(|d| d <= now)) {
                expired.push(s.queue.pop_front().unwrap().send);
            }
            for send in expired {
                self.fail(send, SendError::Timeout);
            }
            if self.segments[&id].retirement_check.is_some_and(|(_, at)| at <= now) {
                self.check_retirement(id, now);
            }
        }
        self.release(now);
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    // --- Routing and dispatch ---

    fn exclusive(&self) -> bool {
        self.options.access_mode != ProducerAccessMode::Shared
    }

    /// Routes a send and queues it on its segment. A send from the application is held while it must wait behind
    /// a retiring segment's messages; a `released` one is the one being released.
    fn dispatch(&mut self, id: SendId, released: bool, now: Instant) {
        let key = self.sends[&id].message.key.clone();
        if !released && self.holding && self.is_held_key(key.as_deref()) {
            self.sends.get_mut(&id).unwrap().place = Place::Held;
            self.held.push_back(id);
            self.waited += 1;
            return;
        }
        let routed = match &key {
            Some(key) => self.layout.route_key(KeyHash::of(key.as_bytes())),
            None => {
                let turn = self.round_robin;
                self.round_robin += 1;
                self.layout.route_keyless(turn)
            }
        };
        let segment = match routed {
            Ok(segment) => segment,
            Err(error) => return self.fail(id, error.into()),
        };
        self.segment(segment, now);
        let send_timeout = self.options.send_timeout;
        let s = self.segments.get_mut(&segment).unwrap();
        if s.link == Link::Gone {
            // Its topic is terminated: no new producer for it, the message waits for the next layout.
            let error = SendError::Server {
                error: ServerError::TopicTerminatedError,
                message: format!("segment {segment} is gone"),
            };
            self.park(id, segment, error);
            if !self.segments[&segment].retiring {
                self.start_retiring(segment, now);
            }
            return;
        }
        let deadline = send_timeout.filter(|_| s.registered_once).map(|timeout| now + timeout);
        s.queue.push_back(Queued { send: id, frame: None, sent: false, deadline });
        self.sends.get_mut(&id).unwrap().place = Place::Queued(segment);
        self.write(segment);
    }

    /// The producer of a segment of the current layout, created and connecting if new.
    fn segment(&mut self, id: u64, now: Instant) {
        if self.segments.contains_key(&id) {
            if matches!(self.segments[&id].link, Link::Idle { retry_at: None }) {
                self.connect(id, now);
            }
            return;
        }
        let info = self.layout.segment(id).expect("routed to a segment of the layout");
        let (initial_last, initial_next) = match self.options.initial_sequence_id {
            Some(initial) => (initial, (initial + 1).max(0) as u64),
            None => (-1, 0),
        };
        let segment = Segment {
            topic: info.topic.clone(),
            legacy: info.is_legacy(),
            link: Link::Idle { retry_at: None },
            attempts: 0,
            topic_epoch: None,
            name: None,
            schema_version: None,
            max_message_size: usize::MAX,
            next_sequence: initial_next,
            last_published: initial_last,
            registered_once: false,
            first_deadline: now + self.options.operation_timeout,
            backoff: Backoff::new(RECONNECT_BACKOFF.0, RECONNECT_BACKOFF.1),
            queue: VecDeque::new(),
            retiring: false,
            parked: Vec::new(),
            retirement_check: None,
            dropped: false,
        };
        self.segments.insert(id, segment);
        self.connect(id, now);
    }

    fn connect(&mut self, id: u64, _now: Instant) {
        let generation = self.next_generation;
        self.next_generation += 1;
        let s = self.segments.get_mut(&id).unwrap();
        s.attempts += 1;
        s.link = Link::Connecting { generation };
        self.actions.push_back(Action::Connect { segment: id, generation, topic: s.topic.clone() });
    }

    fn retry_later(&mut self, id: u64, now: Instant) {
        self.random ^= self.random << 13;
        self.random ^= self.random >> 7;
        self.random ^= self.random << 17;
        let jitter = (self.random >> 11) as f64 / (1u64 << 53) as f64;
        let s = self.segments.get_mut(&id).unwrap();
        let delay = s.backoff.next_delay(jitter);
        s.link = Link::Idle { retry_at: Some(now + delay) };
    }

    /// Writes the segment's messages that aren't on its current registration yet, in order.
    fn write(&mut self, id: u64) {
        let Some(s) = self.segments.get_mut(&id) else { return };
        let Link::Ready { producer_id, .. } = s.link else { return };
        let mut too_large = Vec::new();
        for queued in s.queue.iter_mut().filter(|q| !q.sent) {
            if queued.frame.is_none() {
                let pending = &self.sends[&queued.send];
                let sequence = match pending.message.sequence_id {
                    Some(sequence) => sequence,
                    None => {
                        let sequence = s.next_sequence;
                        s.next_sequence += 1;
                        sequence
                    }
                };
                let name = s.name.clone().unwrap_or_default();
                let (metadata, payload) = encode_message(
                    &name,
                    sequence,
                    pending.publish_time,
                    &pending.message,
                    s.schema_version.as_deref(),
                );
                let size = payload.len() + metadata.encoded_len() as usize;
                if size > s.max_message_size {
                    too_large.push((queued.send, size));
                    continue;
                }
                queued.frame = Some((sequence, metadata, payload));
            }
            let (sequence, metadata, payload) = queued.frame.clone().unwrap();
            let send = CommandSend {
                producer_id,
                sequence_id: sequence,
                highest_sequence_id: Some(sequence),
                ..Default::default()
            };
            let command = BaseCommand { r#type: Type::SEND.into(), send: send.into(), ..Default::default() };
            let (command, metadata) = (Box::new(command), Box::new(metadata));
            self.actions.push_back(Action::Send { segment: id, command, metadata, payload });
            queued.sent = true;
        }
        if !too_large.is_empty() {
            let limit = s.max_message_size;
            s.queue.retain(|q| !too_large.iter().any(|(send, _)| *send == q.send));
            for (send, size) in too_large {
                self.fail(send, SendError::TooLarge { size, limit });
            }
        }
    }

    // --- Segments that go away ---

    /// The segment's topic is terminated: its messages are parked, and it retires.
    fn segment_gone(&mut self, id: u64, error: SendError, now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        s.link = Link::Gone;
        let queued: Vec<SendId> = s.queue.drain(..).map(|q| q.send).collect();
        for send in queued {
            self.park(send, id, error.clone());
        }
        if !self.segments[&id].retiring {
            self.start_retiring(id, now);
        }
        self.release(now);
    }

    /// The layout dropped a segment: its unwritten messages are parked now, the written ones once the broker
    /// confirms it stored nothing more (the close answer), and it retires.
    fn drop_segment(&mut self, id: u64, old: &Rc<Layout>, now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        s.dropped = true;
        let idle = s.queue.is_empty() && s.parked.is_empty() && !s.retiring;
        if !s.retiring && !idle {
            s.retiring = true;
            self.hold_scopes.push((id, Some(old.clone())));
            self.holding = true;
        }
        let s = self.segments.get_mut(&id).unwrap();
        s.retirement_check = None;
        match s.link {
            Link::Ready { generation, .. } => {
                s.link = Link::Closing { generation };
                self.actions.push_back(Action::CloseProducer { segment: id, generation });
                self.retire_unsent(id, now);
            }
            Link::Connecting { .. } => self.retire_queue(id, now),
            Link::Idle { .. } => {
                self.retire_queue(id, now);
                self.segments.get_mut(&id).unwrap().link = Link::Closed;
            }
            Link::Gone | Link::Closing { .. } | Link::Closed => {}
        }
        self.forget_if_done(id);
    }

    /// Parks the messages of a retiring segment that are not written on its registration.
    fn retire_unsent(&mut self, id: u64, _now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        let unsent: Vec<SendId> = s.queue.iter().filter(|q| !q.sent).map(|q| q.send).collect();
        s.queue.retain(|q| q.sent);
        let error = gone_error(id);
        for send in unsent {
            self.park(send, id, error.clone());
        }
    }

    /// Parks every message left on a retiring segment.
    fn retire_queue(&mut self, id: u64, _now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        let left: Vec<SendId> = s.queue.drain(..).map(|q| q.send).collect();
        let error = gone_error(id);
        for send in left {
            self.park(send, id, error.clone());
        }
    }

    fn park(&mut self, send: SendId, segment: u64, error: SendError) {
        let pending = self.sends.get_mut(&send).unwrap();
        if pending.attempts >= MAX_SEND_ATTEMPTS {
            return self.fail(send, error);
        }
        pending.attempts += 1;
        pending.last_error = Some(error);
        pending.place = Place::Parked(segment);
        self.waited += 1;
        self.segments.get_mut(&segment).unwrap().parked.push(send);
    }

    /// A segment is gone while the layout may still have it: hold the new sends of its keys, and check on it.
    fn start_retiring(&mut self, id: u64, now: Instant) {
        let layout = self.layout.is_active(id).then(|| self.layout.clone());
        let s = self.segments.get_mut(&id).unwrap();
        s.retiring = true;
        if layout.is_some() {
            s.retirement_check = Some((1, now + retirement_delay(1)));
        }
        self.hold_scopes.push((id, layout));
        self.holding = true;
    }

    fn check_retirement(&mut self, id: u64, now: Instant) {
        let still_active = self.layout.is_active(id);
        let s = self.segments.get_mut(&id).unwrap();
        let Some((check, _)) = s.retirement_check else { return };
        if !s.retiring || !still_active {
            s.retirement_check = None;
            return;
        }
        if check < MAX_SEND_ATTEMPTS || s.queue.iter().any(|q| q.sent) {
            s.retirement_check = Some((check + 1, now + retirement_delay(check + 1)));
            return;
        }
        // No new layout came: its parked messages fail with why they were parked.
        s.retirement_check = None;
        s.retiring = false;
        let expired: Vec<SendId> = std::mem::take(&mut s.parked);
        for send in expired {
            let error = self.sends[&send].last_error.clone().unwrap_or_else(|| gone_error(id));
            self.fail(send, error);
        }
    }

    fn after_resolution(&mut self, segment: u64, now: Instant) {
        self.forget_if_done(segment);
        self.check_closed();
        self.release(now);
    }

    /// Forgets a segment the layout dropped once nothing waits on it.
    fn forget_if_done(&mut self, id: u64) {
        let Some(s) = self.segments.get(&id) else { return };
        let done = s.dropped
            && s.queue.is_empty()
            && s.parked.is_empty()
            && matches!(s.link, Link::Closed | Link::Gone | Link::Idle { .. });
        if done && !s.retiring {
            self.segments.remove(&id);
        }
    }

    /// Whether a new send of `key` waits behind a retiring segment: whether one of them owned the key.
    fn is_held_key(&self, key: Option<&str>) -> bool {
        let Some(key) = key else { return false };
        let hash = KeyHash::of(key.as_bytes());
        self.hold_scopes.iter().any(|(segment, layout)| match layout {
            None => true,
            Some(layout) => layout.route_key(hash).is_ok_and(|s| s == *segment) || layout.route_key(hash).is_err(),
        })
    }

    /// Routes again the parked messages of the segments that have retired, in call order, then, once no segment is
    /// retiring, the held ones.
    fn release(&mut self, now: Instant) {
        if matches!(self.state, State::Closing | State::Closed) {
            return;
        }
        while self.holding {
            let mut batch = Vec::new();
            let mut retiring = false;
            let ids: Vec<u64> = self.segments.keys().copied().collect();
            for id in ids {
                let active = self.layout.is_active(id);
                let s = self.segments.get_mut(&id).unwrap();
                if !s.retiring {
                    continue;
                }
                let in_flight = !s.queue.is_empty();
                if !in_flight && !active {
                    batch.append(&mut s.parked);
                    s.retiring = false;
                    self.forget_if_done(id);
                } else {
                    retiring = true;
                }
            }
            if batch.is_empty() {
                if retiring {
                    return;
                }
                if self.held.is_empty() {
                    self.clear_hold();
                    return;
                }
                batch.extend(self.held.drain(..));
            }
            // Retired segments of a merge interleave: restore the call order.
            batch.sort_unstable();
            for send in batch {
                if let Some(pending) = self.sends.get_mut(&send) {
                    pending.place = Place::Nowhere;
                    self.dispatch(send, true, now);
                }
            }
        }
    }

    fn clear_hold(&mut self) {
        self.holding = false;
        self.hold_scopes.clear();
    }

    // --- Completion ---

    fn finish(&mut self, send: SendId, result: Result<MessageId, SendError>) {
        if self.sends.remove(&send).is_some() {
            self.actions.push_back(Action::Done { send, result });
        }
    }

    fn fail(&mut self, send: SendId, error: SendError) {
        if let Some(pending) = self.sends.get(&send)
            && let Place::Parked(segment) = pending.place
            && let Some(s) = self.segments.get_mut(&segment)
        {
            s.parked.retain(|&p| p != send);
        }
        self.held.retain(|&h| h != send);
        self.finish(send, Err(error));
    }

    fn check_closed(&mut self) {
        if self.state != State::Closing {
            return;
        }
        // A registration still running doesn't hold the close up: it is closed unused when it completes.
        let closing = self.segments.values().any(|s| matches!(s.link, Link::Closing { .. }));
        if !closing {
            self.state = State::Closed;
            self.actions.push_back(Action::Closed);
        }
    }
}

impl Segment {
    /// Whether events from registration `generation` still apply: it is the current one, or the one closing.
    fn is_current(&self, generation: u64) -> bool {
        matches!(self.link, Link::Ready { generation: g, .. } | Link::Closing { generation: g } if g == generation)
    }
}

fn retriable(error: ServerError) -> bool {
    matches!(
        error,
        ServerError::ServiceNotReady
            | ServerError::TooManyRequests
            | ServerError::MetadataError
            | ServerError::PersistenceError
            | ServerError::UnknownError
            | ServerError::ProducerBlockedQuotaExceededError
    )
}

fn retirement_delay(check: u32) -> Duration {
    Duration::from_millis((100 * u64::from(check)).min(500))
}

fn gone_error(segment: u64) -> SendError {
    SendError::Server { error: ServerError::TopicTerminatedError, message: format!("segment {segment} is gone") }
}

fn key_value(key: &str, value: &str) -> KeyValue {
    KeyValue { key: key.to_string(), value: value.to_string(), ..Default::default() }
}

/// Encodes one message as a batch of one (docs/5-segment-engine.md §2): the entry's metadata, stamped with the
/// key's entry-bucket hash (0 without a key), and the batch payload with the message's own metadata.
pub fn encode_message(
    producer_name: &str,
    sequence_id: u64,
    publish_time: u64,
    message: &OutgoingMessage,
    schema_version: Option<&[u8]>,
) -> (MessageMetadata, Bytes) {
    let bucket = message.key.as_ref().map_or(0, |key| u32::from(KeyHash::of(key.as_bytes()).bucket_hash()));
    let metadata = MessageMetadata {
        producer_name: producer_name.to_string(),
        sequence_id,
        publish_time,
        partition_key: message.key.clone(),
        partition_key_b64_encoded: message.key.as_ref().map(|_| false),
        replicate_to: message.replicate_to.clone().unwrap_or_default(),
        num_messages_in_batch: Some(1),
        schema_version: schema_version.map(<[u8]>::to_vec),
        deliver_at_time: message.deliver_at,
        highest_sequence_id: Some(sequence_id),
        entry_hash_min: Some(bucket),
        entry_hash_max: Some(bucket),
        ..Default::default()
    };
    let single = SingleMessageMetadata {
        properties: message.properties.iter().map(|(k, v)| key_value(k, v)).collect(),
        partition_key: message.key.clone(),
        partition_key_b64_encoded: message.key.as_ref().map(|_| false),
        payload_size: message.payload.len() as i32,
        event_time: message.event_time,
        sequence_id: Some(sequence_id),
        ..Default::default()
    };
    let single_size = single.encoded_len() as usize;
    let mut payload = BytesMut::with_capacity(4 + single_size + message.payload.len());
    payload.put_u32(single_size as u32);
    payload.put_slice(&single.encode_to_vec());
    payload.put_slice(&message.payload);
    (metadata, payload.freeze())
}

#[cfg(test)]
mod tests {
    //! Edge paths of the state machine, driven by hand. The scenarios against a simulated cluster are in
    //! `apache-pulsar-testkit`'s `tests/producer.rs`.

    use super::*;
    use crate::proto::{MessageIdData, ScalableTopicDAG, SegmentInfoProto, SegmentState};

    const TOPIC: &str = "topic://t/n/x";

    /// A layout: `(id, start, end, active)` per segment.
    fn layout(epoch: u64, segments: &[(u64, u32, u32, bool)]) -> Rc<Layout> {
        let segments = segments
            .iter()
            .map(|&(id, start, end, active)| SegmentInfoProto {
                segment_id: id,
                hash_start: start,
                hash_end: end,
                state: if active { SegmentState::ACTIVE } else { SegmentState::SEALED },
                ..Default::default()
            })
            .collect();
        Rc::new(Layout::from_dag(&ScalableTopicDAG { epoch, segments, ..Default::default() }, TOPIC).unwrap())
    }

    fn options(mode: ProducerAccessMode) -> ProducerOptions {
        ProducerOptions {
            name: Some("p".into()),
            access_mode: mode,
            send_timeout: None,
            initial_sequence_id: None,
            properties: Vec::new(),
            schema: None,
            operation_timeout: Duration::from_secs(30),
        }
    }

    fn keyed(key: &str) -> OutgoingMessage {
        OutgoingMessage { key: Some(key.into()), payload: Bytes::from_static(b"x"), ..Default::default() }
    }

    fn keyless() -> OutgoingMessage {
        OutgoingMessage { payload: Bytes::from_static(b"x"), ..Default::default() }
    }

    /// A key routed to `[start, end]`.
    fn key_in(start: u16, end: u16) -> String {
        (0..)
            .map(|i| format!("k{i}"))
            .find(|k| (start..=end).contains(&KeyHash::of(k.as_bytes()).segment_hash()))
            .unwrap()
    }

    struct Test {
        producer: Producer,
        now: Instant,
    }

    impl Test {
        fn new(mode: ProducerAccessMode, layout: Rc<Layout>) -> Self {
            let now = Instant::now();
            Self { producer: Producer::new(options(mode), layout, now), now }
        }

        fn actions(&mut self) -> Vec<Action> {
            std::iter::from_fn(|| self.producer.poll_action()).collect()
        }

        /// The `(segment, generation)` of each connect among `actions`.
        fn connects(actions: &[Action]) -> Vec<(u64, u64)> {
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Connect { segment, generation, .. } => Some((*segment, *generation)),
                    _ => None,
                })
                .collect()
        }

        fn sent(actions: &[Action]) -> Vec<(u64, u64)> {
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Send { segment, command, .. } => Some((*segment, command.send.sequence_id)),
                    _ => None,
                })
                .collect()
        }

        fn done(actions: &[Action]) -> Vec<(SendId, Result<MessageId, SendError>)> {
            actions
                .iter()
                .filter_map(|a| match a {
                    Action::Done { send, result } => Some((*send, result.clone())),
                    _ => None,
                })
                .collect()
        }

        fn send(&mut self, message: OutgoingMessage) -> SendId {
            self.producer.send(message, 0, self.now).unwrap()
        }

        fn register(&mut self, segment: u64, generation: u64, producer_id: u64) {
            let success = CommandProducerSuccess { producer_name: format!("p-seg-{segment}"), ..Default::default() };
            self.producer.on_registered(segment, generation, producer_id, &success, usize::MAX, self.now);
        }

        fn receipt(&mut self, segment: u64, generation: u64, sequence_id: u64, entry: Option<u64>) {
            let receipt = CommandSendReceipt {
                sequence_id,
                message_id: entry.map(|e| MessageIdData { ledgerId: 1, entryId: e, ..Default::default() }).into(),
                ..Default::default()
            };
            self.producer.on_receipt(segment, generation, &receipt, self.now);
        }

        fn send_error(&mut self, segment: u64, generation: u64, sequence_id: u64, error: ServerError) {
            let error = CommandSendError { sequence_id, error, message: "refused".into(), ..Default::default() };
            self.producer.on_send_error(segment, generation, &error, self.now);
        }

        fn advance(&mut self, by: Duration) {
            self.now += by;
            self.producer.handle_timeout(self.now);
        }
    }

    fn one_segment() -> Rc<Layout> {
        layout(1, &[(0, 0, 0xffff, true)])
    }

    #[test]
    fn receipts_are_matched_against_the_head_of_the_queue() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let first = t.send(keyless());
        let actions = t.actions();
        let [(0, generation)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
        t.register(0, generation, 7);
        let second = t.send(keyless());
        assert_eq!(Test::sent(&t.actions()), [(0, 0), (0, 1)]);
        // A receipt past the head: the two sides disagree, the connection is closed to start over.
        t.receipt(0, generation, 5, Some(1));
        assert_eq!(t.actions(), [Action::Disconnect { segment: 0, generation }]);
        // The head's receipt completes it; one without a position completes as earliest.
        t.receipt(0, generation, 0, Some(10));
        t.receipt(0, generation, 1, None);
        let done = Test::done(&t.actions());
        assert_eq!(done.len(), 2);
        assert_eq!(done[0].0, first);
        assert_eq!(done[0].1.as_ref().unwrap().entry.entry_id, 10);
        assert_eq!((done[1].0, done[1].1.as_ref().unwrap().entry), (second, EntryId::EARLIEST));
        // A late receipt, behind the head, is dropped; so is one for another registration.
        let third = t.send(keyless());
        t.actions();
        t.receipt(0, generation, 1, Some(11));
        t.receipt(0, generation + 1, 2, Some(12));
        assert!(t.actions().is_empty());
        t.receipt(0, generation, 2, Some(12));
        assert_eq!(Test::done(&t.actions())[0].0, third);
        assert_eq!(t.producer.last_sequence_id(), 2);
    }

    #[test]
    fn send_errors() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let first = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        t.register(0, generation, 7);
        let second = t.send(keyless());
        t.actions();
        // NotAllowed fails the head only, and only if it is the one meant.
        t.send_error(0, generation, 1, ServerError::NotAllowedError);
        assert!(t.actions().is_empty());
        t.send_error(0, generation, 0, ServerError::NotAllowedError);
        let done = Test::done(&t.actions());
        assert_eq!(done.len(), 1);
        assert!(
            matches!(&done[0], (send, Err(SendError::Server { error: ServerError::NotAllowedError, .. })) if *send == first)
        );
        // Anything else but a terminated topic: register again and write everything again.
        t.send_error(0, generation, 1, ServerError::ChecksumError);
        assert_eq!(t.actions(), [Action::Disconnect { segment: 0, generation }]);
        // For another registration: ignored.
        t.send_error(0, generation + 1, 1, ServerError::TopicTerminatedError);
        assert!(t.actions().is_empty());
        // A terminated topic: the segment is gone, its messages wait for the next layout.
        t.send_error(0, generation, 1, ServerError::TopicTerminatedError);
        assert!(Test::done(&t.actions()).is_empty());
        assert_eq!(t.producer.sends_parked_or_held(), 1);
        assert!(t.producer.poll_timeout().is_some(), "a retirement check is due");
        // The next layout moves it to the successor.
        t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
        let actions = t.actions();
        let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
        t.register(1, next, 8);
        assert_eq!(Test::sent(&t.actions()), [(1, 0)]);
        t.receipt(1, next, 0, Some(1));
        assert_eq!(Test::done(&t.actions())[0].0, second);
    }

    #[test]
    fn registration_failures() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let refused = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        // For another registration: ignored.
        t.producer.on_register_failed(0, generation + 1, RegisterError::Unavailable("x".into()), t.now);
        assert!(t.actions().is_empty());
        // Refused for good: its messages fail, and the next send registers again.
        let denied = RegisterError::Server { error: ServerError::AuthorizationError, message: "no".into() };
        t.producer.on_register_failed(0, generation, denied, t.now);
        let done = Test::done(&t.actions());
        assert!(
            matches!(&done[..], [(send, Err(SendError::Server { error: ServerError::AuthorizationError, .. }))] if *send == refused)
        );
        let retried = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        // Worth retrying, within the first registration's time: after a backoff.
        let busy = RegisterError::Server { error: ServerError::ServiceNotReady, message: "later".into() };
        t.producer.on_register_failed(0, generation, busy, t.now);
        assert!(t.actions().is_empty());
        t.advance(Duration::from_secs(1));
        let generation = Test::connects(&t.actions())[0].1;
        // Past it: the messages fail.
        t.advance(Duration::from_secs(30));
        t.producer.on_register_failed(0, generation, RegisterError::Unavailable("down".into()), t.now);
        let done = Test::done(&t.actions());
        assert!(matches!(&done[..], [(send, Err(SendError::Unavailable(_)))] if *send == retried));
        let late = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        let busy = RegisterError::Server { error: ServerError::ServiceNotReady, message: "later".into() };
        t.producer.on_register_failed(0, generation, busy, t.now);
        let done = Test::done(&t.actions());
        assert!(matches!(&done[..], [(send, Err(SendError::Unavailable(_)))] if *send == late));
        // A terminated topic: gone, and the message waits.
        let waits = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
        t.producer.on_register_failed(0, generation, gone, t.now);
        assert!(Test::done(&t.actions()).is_empty());
        assert_eq!(t.producer.outstanding(), 1);
        assert_eq!(t.producer.oldest_outstanding(), Some(waits));
    }

    #[test]
    fn a_message_is_parked_at_most_ten_times() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let send = t.send(keyless());
        let mut segment = 0;
        for epoch in 2..=12 {
            let generation = Test::connects(&t.actions()).last().copied().unwrap().1;
            let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
            t.producer.on_register_failed(segment, generation, gone, t.now);
            if epoch == 12 {
                break;
            }
            // Each time, a layout seals it and the message moves to the successor, which is gone too.
            segment += 1;
            let segments: Vec<_> =
                (0..segment).map(|id| (id, 0, 0xffff, false)).chain([(segment, 0, 0xffff, true)]).collect();
            t.producer.on_layout(layout(epoch, &segments), t.now);
        }
        let done = Test::done(&t.actions());
        assert!(
            matches!(&done[..], [(s, Err(SendError::Server { error: ServerError::TopicTerminatedError, .. }))] if *s == send),
            "{done:?}"
        );
    }

    #[test]
    fn a_layout_drops_a_ready_segment_once_its_producer_is_closed() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let send = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        t.register(0, generation, 7);
        t.actions();
        // Sealed with the message written and no receipt yet: close the producer, and wait for its answer.
        t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
        assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
        // The answer: what wasn't stored goes to the successor.
        t.producer.on_disconnected(0, generation, t.now);
        let actions = t.actions();
        let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
        t.register(1, next, 8);
        t.actions();
        t.receipt(1, next, 0, Some(3));
        let done = Test::done(&t.actions());
        assert_eq!(done[0].0, send);
        assert_eq!(done[0].1.as_ref().unwrap().segment_id, 1);
    }

    #[test]
    fn a_dropped_segment_that_disconnects_parks_its_messages() {
        let mut t =
            Test::new(ProducerAccessMode::Shared, layout(1, &[(0, 0, 0x7fff, true), (1, 0x8000, 0xffff, true)]));
        let key = key_in(0, 0x7fff);
        let send = t.send(keyed(&key));
        let generation = Test::connects(&t.actions())[0].1;
        // Still registering when the layout drops it: its message moves at once.
        t.producer
            .on_layout(layout(2, &[(0, 0, 0x7fff, false), (1, 0x8000, 0xffff, true), (2, 0, 0x7fff, true)]), t.now);
        let actions = t.actions();
        assert_eq!(Test::connects(&actions).iter().map(|c| c.0).collect::<Vec<_>>(), [2]);
        // The late registration is closed unused.
        t.register(0, generation, 7);
        assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
        t.producer.on_producer_closed(0, generation, t.now);
        assert!(t.actions().is_empty());
        assert_eq!(t.producer.oldest_outstanding(), Some(send));
    }

    #[test]
    fn close_closes_registrations_that_complete_late() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        let send = t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        t.producer.close(t.now);
        let actions = t.actions();
        assert!(actions.contains(&Action::Closed), "a registration still running doesn't hold close up");
        assert!(matches!(&Test::done(&actions)[..], [(s, Err(SendError::AlreadyClosed))] if *s == send));
        t.register(0, generation, 7);
        assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
        t.producer.on_producer_closed(0, generation, t.now);
        assert!(t.actions().is_empty());
        // Closing again does nothing.
        t.producer.close(t.now);
        assert!(t.actions().is_empty());
    }

    #[test]
    fn close_while_creating_fails_create() {
        let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
        t.actions();
        t.producer.close(t.now);
        let actions = t.actions();
        assert!(actions.contains(&Action::Created(Err(SendError::AlreadyClosed))));
        assert!(actions.contains(&Action::Closed));
    }

    #[test]
    fn exclusive_producers_claim_new_segments_from_a_layout() {
        let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
        let generation = Test::connects(&t.actions())[0].1;
        t.register(0, generation, 7);
        assert_eq!(t.actions(), [Action::Created(Ok(()))]);
        t.producer
            .on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0x7fff, true), (2, 0x8000, 0xffff, true)]), t.now);
        let actions = t.actions();
        let mut claimed: Vec<u64> = Test::connects(&actions).iter().map(|c| c.0).collect();
        claimed.sort();
        assert_eq!(claimed, [1, 2]);
        // A stale or equal layout changes nothing.
        t.producer.on_layout(one_segment(), t.now);
        assert!(t.actions().is_empty());
    }

    #[test]
    fn an_idle_segment_reconnects_only_when_needed() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        t.register(0, generation, 7);
        t.actions();
        t.receipt(0, generation, 0, Some(1));
        t.actions();
        t.producer.on_disconnected(0, generation, t.now);
        // Nothing to send when the backoff ends: no new registration until something is.
        t.advance(Duration::from_secs(1));
        assert!(Test::connects(&t.actions()).is_empty());
        t.send(keyless());
        assert_eq!(Test::connects(&t.actions()).len(), 1);
    }

    #[test]
    fn a_key_no_segment_covers_fails_its_send() {
        let mut t = Test::new(ProducerAccessMode::Shared, layout(1, &[(0, 0, 0x7fff, true)]));
        let key = key_in(0x8000, 0xffff);
        let send = t.send(keyed(&key));
        let done = Test::done(&t.actions());
        assert!(matches!(&done[..], [(s, Err(SendError::Routing(LayoutError::Uncovered(_))))] if *s == send));
    }

    #[test]
    fn an_exclusive_create_follows_a_split_that_lands_meanwhile() {
        let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
        let generation = Test::connects(&t.actions())[0].1;
        // The segment splits before the claim is answered: create waits for its children instead.
        t.producer
            .on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0x7fff, true), (2, 0x8000, 0xffff, true)]), t.now);
        let actions = t.actions();
        let children = Test::connects(&actions);
        assert_eq!(children.iter().map(|c| c.0).collect::<Vec<_>>(), [1, 2]);
        // The sealed segment's claim fails: it is gone, and doesn't fail the create.
        let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
        t.producer.on_register_failed(0, generation, gone, t.now);
        assert!(t.actions().is_empty());
        // The gone segment had left the layout already: a keyed send goes to its child, written once registered.
        let send = t.send(keyed("k"));
        t.register(1, children[0].1, 8);
        assert!(t.actions().is_empty(), "still waiting for the other child");
        t.register(2, children[1].1, 9);
        let actions = t.actions();
        assert!(actions.contains(&Action::Created(Ok(()))), "{actions:?}");
        assert_eq!(Test::sent(&actions).len(), 1);
        assert_eq!(t.producer.oldest_outstanding(), Some(send));
    }

    #[test]
    fn a_retirement_check_ends_when_the_layout_moves_on() {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
        t.producer.on_register_failed(0, generation, gone, t.now);
        assert!(t.producer.poll_timeout().is_some());
        // The layout drops the segment before the check is due: the message moves on, and the check is void.
        t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
        assert_eq!(Test::connects(&t.actions()).iter().map(|c| c.0).collect::<Vec<_>>(), [1]);
        t.advance(Duration::from_secs(1));
        assert!(Test::done(&t.actions()).is_empty());
    }
}
