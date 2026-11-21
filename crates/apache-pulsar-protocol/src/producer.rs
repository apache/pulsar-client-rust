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
//!   its own sequence ids. A message gets its sequence id when it goes into a batch, once the segment's producer is
//!   registered, and keeps it, and its entry's bytes, when the entry is written again after a reconnect.
//! - **Every entry is a batch**, stamped with the range of its messages' entry-bucket hashes (*key-shared* §3). A
//!   segment has one open batch per entry bucket; they close together, on the maximum messages, bytes or delay of
//!   [`Batching`], as Java's `EntryBucketBatchContainer` does. Without batching, each message is an entry of its own.
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

use buffa::{Message, SizeCache};
use bytes::{BufMut, Bytes, BytesMut};

use crate::backoff::{Backoff, Jitter};
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
    /// How messages are grouped into entries; `None` puts each message in an entry of its own.
    pub batching: Option<Batching>,
}

/// How each segment's producer groups messages into entries (docs/5-segment-engine.md §2), as Java's
/// `BatchingPolicy` configures it.
///
/// A segment has one open batch per entry bucket, and they close together, as in Java's `EntryBucketBatchContainer`:
/// when the messages in them reach `max_messages` or their payloads `max_bytes`, or `max_delay` after the first went
/// in. That bounds the wait batching adds to a message, as Java's `BatchingPolicy` documents it; Java's timer counts
/// from the last batch sent, so a batch can wait a little longer there. Their entries then go out in the order of
/// their sequence ids, each entry's being its highest message's: the broker drops an entry whose sequence id isn't
/// above the last one it took from the producer, as a duplicate.
///
/// A message that would take the open batches over `max_bytes` closes them first, so a message too large for the
/// broker goes alone, and fails alone. A message with a delivery time or replication clusters, which are its
/// entry's, goes in an entry of its own, after the open batches, as Java's does with a delivery time; so does one
/// whose sequence id is at or below the last one given, which the broker may drop as a duplicate, with its entry.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub struct Batching {
    /// The most messages a segment's open batches hold together; 0 for no limit.
    pub max_messages: usize,
    /// The most payload bytes they hold together, and at most the broker's largest message, which 0 stands for.
    pub max_bytes: usize,
    /// How long the first message in them waits for others.
    pub max_delay: Duration,
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

/// Why registering a segment's producer or consumer failed, as the driver saw it.
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

/// A message routed to a segment whose producer isn't registered: it gets its sequence id once it is.
#[derive(Debug)]
struct Waiting {
    send: SendId,
    deadline: Option<Instant>,
}

/// A message in an open batch.
#[derive(Debug)]
struct Batched {
    send: SendId,
    sequence: u64,
    /// Its entry-bucket hash: 0 without a key.
    hash: u16,
    deadline: Option<Instant>,
}

/// A segment's open batches, one per entry bucket, which close together (see [`Batching`]).
#[derive(Debug, Default)]
struct Batches {
    /// Each bucket's messages, in call order.
    buckets: BTreeMap<usize, Vec<Batched>>,
    messages: usize,
    bytes: usize,
    /// When they close: the maximum delay after their first message.
    close_at: Option<Instant>,
}

impl Batching {
    /// The most payload bytes the open batches hold, on a connection that takes messages up to `max_message_size`:
    /// no more than that, so that only a message too large on its own, or the metadata of many, can take an entry
    /// past it.
    fn byte_limit(&self, max_message_size: usize) -> usize {
        match self.max_bytes {
            0 => max_message_size,
            max => max.min(max_message_size),
        }
    }
}

impl Batches {
    fn is_empty(&self) -> bool {
        self.messages == 0
    }

    /// Whether a message with a payload of `size` bytes can join them. They are never full by count: they close as
    /// they get there.
    fn fits(&self, size: usize, batching: &Batching, max_message_size: usize) -> bool {
        self.bytes + size <= batching.byte_limit(max_message_size)
    }

    fn full(&self, batching: &Batching, max_message_size: usize) -> bool {
        let bytes = self.bytes >= batching.byte_limit(max_message_size);
        bytes || (batching.max_messages > 0 && self.messages >= batching.max_messages)
    }

    fn add(&mut self, bucket: usize, message: Batched, size: usize) {
        self.buckets.entry(bucket).or_default().push(message);
        self.messages += 1;
        self.bytes += size;
    }

    fn sends(self) -> impl Iterator<Item = SendId> {
        self.buckets.into_values().flatten().map(|b| b.send)
    }
}

/// An entry: a closed batch, written once the segment's producer is registered, and again, unchanged, after a
/// reconnect.
#[derive(Debug)]
struct Entry {
    /// Its messages, in batch order.
    sends: Vec<SendId>,
    /// Its sequence id: its messages' highest, as Java gives an entry-bucket batch.
    sequence: u64,
    metadata: MessageMetadata,
    payload: Bytes,
    /// Whether it was written on the current registration, so a receipt or an error may come for it.
    sent: bool,
    /// Its messages' earliest deadline.
    deadline: Option<Instant>,
}

#[derive(Debug)]
struct Segment {
    topic: String,
    legacy: bool,
    /// The start hashes of its entry buckets 1..N-1; a batch holds one bucket's messages.
    bucket_splits: Vec<u16>,
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
    /// The highest sequence id given to a message.
    last_pushed: i64,
    registered_once: bool,
    /// When the first registration stops retrying.
    first_deadline: Instant,
    backoff: Backoff,
    /// Its messages, oldest first: entries, then the open batches, then those waiting for a registration.
    entries: VecDeque<Entry>,
    batches: Batches,
    waiting: VecDeque<Waiting>,
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
    /// The retiring segments since the hold started, with the layout each was active in.
    hold_scopes: Vec<(u64, Rc<Layout>)>,
    holding: bool,
    /// Segments whose producers must all register before `create` completes.
    awaiting_create: Vec<u64>,
    actions: VecDeque<Action>,
    next_send: SendId,
    next_generation: u64,
    round_robin: u64,
    jitter: Jitter,
    /// How many times a send was parked or held, for tests and metrics.
    waited: u64,
    /// The last send a flush covers: a batch it goes into closes at once, however much later it gets there.
    flushed: Option<SendId>,
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
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            waited: 0,
            flushed: None,
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
            s.last_pushed = s.last_pushed.max(last);
            s.next_sequence = (last + 1).max(0) as u64;
        }
        // The messages that got into a batch got their deadlines then.
        if let Some(timeout) = send_timeout {
            for waiting in &mut s.waiting {
                waiting.deadline.get_or_insert(now + timeout);
            }
        }
        if self.state == State::Creating {
            self.awaiting_create.retain(|&id| id != segment);
            if self.awaiting_create.is_empty() {
                self.state = State::Open;
                self.actions.push_back(Action::Created(Ok(())));
            }
        }
        self.write(segment, now);
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
        for send in s.take_all() {
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
        let Some(head) = s.entries.front().filter(|e| e.sent) else { return };
        if receipt.sequence_id < head.sequence {
            // For an entry that timed out already.
            return;
        }
        if receipt.sequence_id > head.sequence {
            self.actions.push_back(Action::Disconnect { segment, generation });
            return;
        }
        let head = s.entries.pop_front().unwrap();
        let highest = receipt.highest_sequence_id.map(|h| h as i64).unwrap_or(-1);
        s.last_published = s.last_published.max(receipt.sequence_id as i64).max(highest);
        let batch_size = head.sends.len() as i32;
        for (batch_index, send) in (0..).zip(head.sends) {
            let entry = receipt.message_id.as_option().map_or(EntryId::EARLIEST, |id| EntryId {
                ledger_id: id.ledgerId as i64,
                entry_id: id.entryId as i64,
                partition: -1,
                batch_index,
                batch_size,
            });
            self.finish(send, Ok(MessageId::new(segment as i64, entry)));
        }
        self.after_resolution(segment, now);
    }

    /// A `CommandSendError` for `segment`'s producer.
    pub fn on_send_error(&mut self, segment: u64, generation: u64, error: &CommandSendError, now: Instant) {
        let Some(s) = self.segments.get_mut(&segment) else { return };
        if !s.is_current(generation) {
            return;
        }
        let head_sequence = s.entries.front().filter(|e| e.sent).map(|e| e.sequence);
        match error.error {
            ServerError::TopicTerminatedError => {
                let gone = SendError::Server { error: error.error, message: error.message.clone() };
                self.segment_gone(segment, gone, now);
            }
            ServerError::NotAllowedError => {
                if head_sequence == Some(error.sequence_id) {
                    let head = s.entries.pop_front().unwrap();
                    for send in head.sends {
                        self.fail(send, SendError::Server { error: error.error, message: error.message.clone() });
                    }
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
            // A registration in progress fails instead, and a segment the layout dropped is closing.
            Link::Ready { generation: g, .. } if g == generation => {
                for entry in &mut s.entries {
                    entry.sent = false;
                }
                self.retry_later(segment, now);
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
            for send in s.take_all() {
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
                    // Its entries are written, and wait for their receipts or the close's answer. What its open
                    // batches hold was never written.
                    debug_assert!(s.entries.iter().all(|e| e.sent), "a ready segment writes its entries");
                    debug_assert!(s.waiting.is_empty(), "a ready segment batches what it gets");
                    s.link = Link::Closing { generation };
                    self.actions.push_back(Action::CloseProducer { segment: id, generation });
                    std::mem::take(&mut s.batches).sends().collect()
                }
                // A registration still running is closed when it completes (`on_registered`).
                Link::Connecting { .. } | Link::Closing { .. } => s.take_all(),
                Link::Idle { .. } | Link::Gone | Link::Closed => {
                    s.link = Link::Closed;
                    s.take_all()
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
                // Deadlines grow in a segment's order: the oldest of the entries, and of the waiting messages, is
                // first. The open batches close before their messages' deadlines, unless the delay is the longer.
                let entry = s.entries.front().and_then(|e| e.deadline);
                let waiting = s.waiting.front().and_then(|w| w.deadline);
                [retry, entry, waiting, s.batches.close_at, s.retirement_check.map(|(_, at)| at)]
            })
            .flatten()
            .min()
    }

    /// Lets time pass: reconnects segments whose backoff is over, closes open batches whose delay is over, fails
    /// messages past their send timeout, and runs the retirement checks.
    pub fn handle_timeout(&mut self, now: Instant) {
        let exclusive = self.exclusive();
        let ids: Vec<u64> = self.segments.keys().copied().collect();
        for id in ids {
            let Some(s) = self.segments.get_mut(&id) else { continue };
            if let Link::Idle { retry_at: Some(at) } = s.link
                && at <= now
            {
                if s.is_empty() && !exclusive {
                    // Nothing to send: connect again when something is.
                    s.link = Link::Idle { retry_at: None };
                } else {
                    self.connect(id, now);
                }
            }
            if self.segments[&id].batches.close_at.is_some_and(|at| at <= now) {
                self.close_batches(id);
            }
            // An entry fails whole: its messages are stored together or not at all.
            let s = self.segments.get_mut(&id).unwrap();
            let mut expired = Vec::new();
            while s.entries.front().is_some_and(|e| e.deadline.is_some_and(|d| d <= now)) {
                expired.extend(s.entries.pop_front().unwrap().sends);
            }
            while s.waiting.front().is_some_and(|w| w.deadline.is_some_and(|d| d <= now)) {
                expired.push(s.waiting.pop_front().unwrap().send);
            }
            for send in expired {
                self.fail(send, SendError::Timeout);
            }
            self.write_entries(id);
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
            self.start_retiring(segment, now);
            return;
        }
        let deadline = send_timeout.filter(|_| s.registered_once).map(|timeout| now + timeout);
        s.waiting.push_back(Waiting { send: id, deadline });
        self.sends.get_mut(&id).unwrap().place = Place::Queued(segment);
        self.write(segment, now);
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
            bucket_splits: info.bucket_splits.clone(),
            link: Link::Idle { retry_at: None },
            attempts: 0,
            topic_epoch: None,
            name: None,
            schema_version: None,
            max_message_size: usize::MAX,
            next_sequence: initial_next,
            last_published: initial_last,
            last_pushed: initial_last,
            registered_once: false,
            first_deadline: now + self.options.operation_timeout,
            backoff: Backoff::new(RECONNECT_BACKOFF.0, RECONNECT_BACKOFF.1),
            entries: VecDeque::new(),
            batches: Batches::default(),
            waiting: VecDeque::new(),
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
        let jitter = self.jitter.next();
        let s = self.segments.get_mut(&id).unwrap();
        let delay = s.backoff.next_delay(jitter);
        s.link = Link::Idle { retry_at: Some(now + delay) };
    }

    /// Once the segment's producer is registered: puts the messages waiting for it into batches, and writes the
    /// entries not written on the registration yet.
    fn write(&mut self, id: u64, now: Instant) {
        let s = &self.segments[&id];
        if !matches!(s.link, Link::Ready { .. }) {
            return;
        }
        // Messages a flush covers close their batches, once all of them joined.
        let flushed = s.waiting.front().is_some_and(|w| self.flushed.is_some_and(|last| w.send <= last));
        while let Some(waiting) = self.segments.get_mut(&id).unwrap().waiting.pop_front() {
            self.batch(id, waiting, now);
        }
        if flushed {
            self.close_batches(id);
        }
        self.write_entries(id);
    }

    /// Gives a message its sequence id and puts it in its bucket's batch. The open batches close before it when it
    /// can't join them, and after it when they are full or it goes alone (see [`Batching`]).
    fn batch(&mut self, id: u64, waiting: Waiting, now: Instant) {
        let message = &self.sends[&waiting.send].message;
        let s = self.segments.get_mut(&id).unwrap();
        let sequence = match message.sequence_id {
            Some(sequence) => sequence,
            None => {
                let sequence = s.next_sequence;
                s.next_sequence += 1;
                sequence
            }
        };
        let duplicate = sequence as i64 <= s.last_pushed;
        s.last_pushed = s.last_pushed.max(sequence as i64);
        let alone = duplicate || message.deliver_at.is_some() || message.replicate_to.is_some();
        let batching = self.options.batching.filter(|_| !alone);
        let hash = message.key.as_ref().map_or(0, |key| KeyHash::of(key.as_bytes()).bucket_hash());
        let bucket = s.bucket_splits.iter().take_while(|&&split| split <= hash).count();
        let size = message.payload.len();
        if !batching.is_some_and(|b| s.batches.fits(size, &b, s.max_message_size)) {
            self.close_batches(id);
        }
        let s = self.segments.get_mut(&id).unwrap();
        let batched = Batched { send: waiting.send, sequence, hash, deadline: waiting.deadline };
        s.batches.add(bucket, batched, size);
        match batching.filter(|b| !s.batches.full(b, s.max_message_size)) {
            Some(b) => {
                s.batches.close_at.get_or_insert(now + b.max_delay);
            }
            None => self.close_batches(id),
        }
    }

    /// Closes the segment's open batches, each into an entry. The entries go in the order of their sequence ids:
    /// the broker takes a producer's entries only in increasing sequence order.
    fn close_batches(&mut self, id: u64) {
        let s = self.segments.get_mut(&id).unwrap();
        let mut batches: Vec<Vec<Batched>> = std::mem::take(&mut s.batches).buckets.into_values().collect();
        batches.sort_by_key(|batch| batch.iter().map(|b| b.sequence).max());
        for batch in batches {
            self.close_entry(id, batch);
        }
    }

    /// Encodes a closed batch as an entry, at the end of the segment's queue, or fails its messages if the broker
    /// can't take it.
    fn close_entry(&mut self, id: u64, batch: Vec<Batched>) {
        let s = self.segments.get_mut(&id).unwrap();
        let messages: Vec<(&Batched, &Pending)> = batch.iter().map(|b| (b, &self.sends[&b.send])).collect();
        let name = s.name.as_deref().unwrap_or_default();
        let (metadata, payload) = encode_entry(name, s.schema_version.as_deref(), &messages);
        let size = payload.len() + metadata.encoded_len() as usize;
        if size > s.max_message_size {
            let limit = s.max_message_size;
            for b in batch {
                self.fail(b.send, SendError::TooLarge { size, limit });
            }
            return;
        }
        let deadline = batch.iter().filter_map(|b| b.deadline).min();
        let sends = batch.into_iter().map(|b| b.send).collect();
        let entry = Entry { sends, sequence: metadata.sequence_id, metadata, payload, sent: false, deadline };
        s.entries.push_back(entry);
    }

    /// Writes, in order, the entries not written on the segment's current registration: the last ones in its queue.
    fn write_entries(&mut self, id: u64) {
        let s = self.segments.get_mut(&id).unwrap();
        let Link::Ready { producer_id, .. } = s.link else { return };
        let written = s.entries.len() - s.entries.iter().rev().take_while(|e| !e.sent).count();
        for entry in s.entries.range_mut(written..) {
            let messages = entry.sends.len() as i32;
            let send = CommandSend {
                producer_id,
                sequence_id: entry.sequence,
                highest_sequence_id: Some(entry.sequence),
                num_messages: (messages > 1).then_some(messages),
                ..Default::default()
            };
            let command = BaseCommand { r#type: Type::SEND.into(), send: send.into(), ..Default::default() };
            let (command, metadata) = (Box::new(command), Box::new(entry.metadata.clone()));
            self.actions.push_back(Action::Send { segment: id, command, metadata, payload: entry.payload.clone() });
            entry.sent = true;
        }
    }

    /// Lets no message sent so far wait out the batching delay: the open batches close and are written, and the
    /// messages not in a batch yet, waiting for a registration or a layout, close theirs once they are in them.
    pub fn flush(&mut self) {
        self.flushed = self.next_send.checked_sub(1);
        let ids: Vec<u64> = self.segments.keys().copied().collect();
        for id in ids {
            self.close_batches(id);
            self.write_entries(id);
        }
    }

    // --- Segments that go away ---

    /// The segment's topic is terminated: its messages are parked, and it retires.
    fn segment_gone(&mut self, id: u64, error: SendError, now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        s.link = Link::Gone;
        for send in s.take_all() {
            self.park(send, id, error.clone());
        }
        self.start_retiring(id, now);
        self.release(now);
    }

    /// The layout dropped a segment: its unwritten messages are parked now, the written ones once the broker
    /// confirms it stored nothing more (the close answer), and it retires.
    fn drop_segment(&mut self, id: u64, old: &Rc<Layout>, now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        s.dropped = true;
        let idle = s.is_empty() && s.parked.is_empty() && !s.retiring;
        if !s.retiring && !idle {
            s.retiring = true;
            self.hold_scopes.push((id, old.clone()));
            self.holding = true;
        }
        let s = self.segments.get_mut(&id).unwrap();
        s.retirement_check = None;
        match s.link {
            Link::Ready { generation, .. } => {
                s.link = Link::Closing { generation };
                // Its entries are written: what the broker didn't store comes back with the close's answer.
                self.actions.push_back(Action::CloseProducer { segment: id, generation });
                let error = gone_error(id);
                for send in std::mem::take(&mut s.batches).sends().collect::<Vec<_>>() {
                    self.park(send, id, error.clone());
                }
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

    /// Parks every message left on a retiring segment.
    fn retire_queue(&mut self, id: u64, _now: Instant) {
        let left = self.segments.get_mut(&id).unwrap().take_all();
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
    ///
    /// A segment the layout no longer has holds nothing back: it was idle when the layout dropped it (or it would be
    /// retiring already), so none of its keys has a message waiting on it.
    fn start_retiring(&mut self, id: u64, now: Instant) {
        let active = self.layout.is_active(id);
        let s = self.segments.get_mut(&id).unwrap();
        if std::mem::replace(&mut s.retiring, true) || !active {
            return;
        }
        s.retirement_check = Some((1, now + retirement_delay(1)));
        self.hold_scopes.push((id, self.layout.clone()));
        self.holding = true;
    }

    /// A check is due only for a segment retiring while the layout has it: a layout that drops it, or a close,
    /// cancels the check.
    fn check_retirement(&mut self, id: u64, now: Instant) {
        let s = self.segments.get_mut(&id).unwrap();
        let Some((check, _)) = s.retirement_check else { return };
        debug_assert!(s.retiring, "a retirement check for a segment not retiring");
        // A gone segment queues nothing: its messages are parked.
        debug_assert!(s.is_empty(), "a gone segment with queued messages");
        if check < MAX_SEND_ATTEMPTS {
            s.retirement_check = Some((check + 1, now + retirement_delay(check + 1)));
            return;
        }
        // No new layout came: its parked messages fail with why they were parked.
        s.retirement_check = None;
        s.retiring = false;
        let expired: Vec<SendId> = std::mem::take(&mut s.parked);
        for send in expired {
            let error =
                self.sends[&send].last_error.clone().expect("a message is parked with the error that parked it");
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
            && s.is_empty()
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
        // A key a layout can't route is held, rather than risk its order.
        self.hold_scopes.iter().any(|(segment, layout)| !layout.route_key(hash).is_ok_and(|s| s != *segment))
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
                let in_flight = !s.is_empty();
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
            // Parked and held sends are waiting, so they are all here.
            for send in batch {
                self.sends.get_mut(&send).expect("a waiting send").place = Place::Nowhere;
                self.dispatch(send, true, now);
            }
        }
    }

    fn clear_hold(&mut self) {
        self.holding = false;
        self.hold_scopes.clear();
    }

    // --- Completion ---

    fn finish(&mut self, send: SendId, result: Result<MessageId, SendError>) {
        let finished = self.sends.remove(&send);
        debug_assert!(finished.is_some(), "send {send} finished twice");
        self.actions.push_back(Action::Done { send, result });
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

    /// Whether it holds no message: none in an entry, an open batch, or waiting for a registration.
    fn is_empty(&self) -> bool {
        self.entries.is_empty() && self.batches.is_empty() && self.waiting.is_empty()
    }

    /// Takes every message it holds, in call order.
    fn take_all(&mut self) -> Vec<SendId> {
        let mut sends: Vec<SendId> = self.entries.drain(..).flat_map(|e| e.sends).collect();
        sends.extend(std::mem::take(&mut self.batches).sends());
        sends.extend(self.waiting.drain(..).map(|w| w.send));
        // A segment's entries go out in sequence order, which isn't call order across buckets.
        sends.sort_unstable();
        sends
    }
}

/// Whether a broker's refusal to register a producer or consumer is worth retrying.
pub(crate) fn retriable(error: ServerError) -> bool {
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

/// Encodes a batch as one entry (docs/5-segment-engine.md §2): the entry's metadata, which takes its first message's
/// key, publish time, delivery time and replication clusters, as Java's batch container does, with the entry's
/// sequence id, its messages' highest, and a stamp of the range of their entry-bucket hashes; and the batch payload,
/// each message with its own metadata.
fn encode_entry(
    producer_name: &str,
    schema_version: Option<&[u8]>,
    batch: &[(&Batched, &Pending)],
) -> (MessageMetadata, Bytes) {
    let first = &batch[0].1;
    let sequence = batch.iter().map(|(b, _)| b.sequence).max().unwrap_or_default();
    let (min, max) = batch.iter().fold((u16::MAX, 0), |(min, max), (b, _)| (min.min(b.hash), max.max(b.hash)));
    let metadata = MessageMetadata {
        producer_name: producer_name.to_string(),
        sequence_id: sequence,
        publish_time: first.publish_time,
        partition_key: first.message.key.clone(),
        partition_key_b64_encoded: first.message.key.as_ref().map(|_| false),
        replicate_to: first.message.replicate_to.clone().unwrap_or_default(),
        num_messages_in_batch: Some(batch.len() as i32),
        schema_version: schema_version.map(<[u8]>::to_vec),
        deliver_at_time: first.message.deliver_at,
        highest_sequence_id: Some(sequence),
        entry_hash_min: Some(u32::from(min)),
        entry_hash_max: Some(u32::from(max)),
        ..Default::default()
    };
    let size = batch.iter().map(|(_, p)| 16 + p.message.payload.len()).sum();
    let mut payload = BytesMut::with_capacity(size);
    for (batched, pending) in batch {
        let message = &pending.message;
        let single = SingleMessageMetadata {
            properties: message.properties.iter().map(|(k, v)| key_value(k, v)).collect(),
            partition_key: message.key.clone(),
            partition_key_b64_encoded: message.key.as_ref().map(|_| false),
            payload_size: message.payload.len() as i32,
            event_time: message.event_time,
            sequence_id: Some(batched.sequence),
            ..Default::default()
        };
        let mut cache = SizeCache::new();
        payload.put_u32(single.compute_size(&mut cache));
        single.write_to(&mut cache, &mut payload);
        payload.put_slice(&message.payload);
    }
    (metadata, payload.freeze())
}
