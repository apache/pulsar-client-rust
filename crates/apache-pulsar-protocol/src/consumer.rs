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

//! The consumer of one segment, sans IO: its subscription, flow control, acknowledgments and redeliveries
//! (docs/5-segment-engine.md §3).
//!
//! A [`SegmentConsumer`] is a subscription's consumer on one segment topic, as Java's v4 `ConsumerImpl` is under the
//! V5 consumers. The driver registers it on a connection and feeds it the outcome, the entries and commands the
//! broker pushes, and the passing of time; the application takes its messages and acknowledges them. It hands back
//! [`Action`]s: connect, send a command, close.
//!
//! - **Flow control.** Once subscribed, it grants the broker the receiver queue size in permits, then grants more,
//!   in one `CommandFlow`, each time the application has taken half as many messages. A message it leaves out
//!   (acknowledged already in its batch, compacted out, or in an entry it can't read) counts as taken.
//! - **Acknowledgments** are grouped: those made within the ack group time go out in one `CommandAck`, or at once
//!   when the group reaches its maximum size. An ack of a message in a batch acknowledges its index, and the broker
//!   drops the entry once every index is acknowledged. Acks are fire and forget: nothing waits for the broker.
//! - **Cumulative acks** acknowledge every message up to a position. On an Exclusive subscription one `CommandAck`
//!   carries it. A `Key_Shared` subscription forbids them: the consumer acknowledges one by one every message it gave
//!   the application up to the one at the position, in the order it gave them (*key-shared* §4).
//! - **Draining.** Once paused, the consumer gives the application nothing more, and [`SegmentConsumer::is_settled`]
//!   tells when the application has acknowledged everything it was given. It never acknowledges anything itself. The
//!   Stream consumer drains a segment before it lets go of it (*key-shared* §4).
//! - **Negative acks** wait out the negative ack delay, then go out together as one
//!   `CommandRedeliverUnacknowledgedMessages` per thousand entries. A batch comes back whole, as in Java.
//! - **Reconnecting.** After a connection loss it drops the messages the application hasn't taken, since the broker
//!   delivers them again, and subscribes again with backoff. The acks made meanwhile go out before the new permits.
//! - **The end of the topic.** The broker sends `CommandReachedEndOfTopic` to the consumers of a terminated segment
//!   once their subscription has acknowledged everything there. [`SegmentConsumer::is_drained`] then tells the layer
//!   above that it can let the segment go.
//! - **Readers.** A reader ([`SegmentConsumer::reader`]) has a subscription that isn't durable, so it keeps its own
//!   position: where it starts, then the last message the application took. Each subscribe starts after it, as Java's
//!   v4 reader does. The broker sends the entry of the start position again, and the reader leaves out what it read
//!   already ([`reached`]). A reader that starts at the latest position waits, without permits, until the layer above
//!   tells it the segment's last message id ([`SegmentConsumer::resolve`]): a checkpoint taken before its first
//!   message then names that message, not the moving latest.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::time::{Duration, Instant};

use crate::backoff::{Backoff, Jitter};
use crate::entry::{self, ReceivedMessage};
use crate::frame::PayloadSection;
use crate::message_id::EntryId;
use crate::producer::{RegisterError, retriable};
use crate::proto::base_command::Type;
use crate::proto::command_ack::{AckType, ValidationError};
use crate::proto::command_subscribe::{InitialPosition, SubType};
use crate::proto::{
    BaseCommand, CommandAck, CommandFlow, CommandMessage, CommandRedeliverUnacknowledgedMessages, CommandSubscribe,
    IntRange, KeySharedMeta, KeySharedMode, KeyValue, MessageIdData, Schema,
};

/// How many entries one `CommandRedeliverUnacknowledgedMessages` names at most, as in Java.
const REDELIVER_CHUNK: usize = 1000;

const RECONNECT_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(60));

/// A consumer's configuration. The defaults are Java's.
#[derive(Debug, Clone)]
pub struct ConsumerOptions {
    pub subscription: String,
    pub sub_type: SubType,
    /// The name the broker knows the consumer by; without one, the broker picks it.
    pub consumer_name: Option<String>,
    /// Where a new subscription starts.
    pub initial_position: InitialPosition,
    /// How many messages the broker may push ahead of the application.
    pub receiver_queue_size: u32,
    /// How long acks wait to go out together.
    pub ack_group_time: Duration,
    /// How many acks go out at once, whatever the time.
    pub max_ack_group_size: usize,
    /// How long a negatively acknowledged message waits before the broker is asked to deliver it again.
    pub negative_ack_delay: Duration,
    pub priority_level: Option<i32>,
    /// The subscription's properties, set when it is created.
    pub subscription_properties: Vec<(String, String)>,
    /// The consumer's properties.
    pub properties: Vec<(String, String)>,
    /// The schema declared on subscribing; `None` for raw bytes.
    pub schema: Option<Schema>,
    /// How long the first subscribe may keep failing with errors worth retrying.
    pub operation_timeout: Duration,
    /// For a `Key_Shared` subscription, the segment's entry-bucket boundaries: it subscribes STICKY with entry-bucket
    /// dispatch, declaring them as its hash ranges (*key-shared* §4).
    pub bucket_ranges: Vec<IntRange>,
}

impl Default for ConsumerOptions {
    fn default() -> Self {
        Self {
            subscription: String::new(),
            sub_type: SubType::Shared,
            consumer_name: None,
            initial_position: InitialPosition::Latest,
            receiver_queue_size: 1000,
            ack_group_time: Duration::from_millis(100),
            max_ack_group_size: 1000,
            negative_ack_delay: Duration::from_secs(60),
            priority_level: None,
            subscription_properties: Vec::new(),
            properties: Vec::new(),
            schema: None,
            operation_timeout: Duration::from_secs(30),
            bucket_ranges: Vec::new(),
        }
    }
}

/// What the consumer asks of its driver.
#[derive(Debug, Clone, PartialEq)]
pub enum Action {
    /// Look up the segment's topic, connect, register a consumer id, and send
    /// [`SegmentConsumer::subscribe_command`]. Report with `on_subscribed` or `on_subscribe_failed`.
    Connect { generation: u64 },
    /// Send a command that has no response on the consumer's connection: a flow, an ack or a redelivery request.
    Send(Box<BaseCommand>),
    /// Close the consumer on the broker with `CommandCloseConsumer`, then report with `on_closed`.
    CloseConsumer { generation: u64 },
    /// How the first subscribe went.
    Subscribed(Result<(), RegisterError>),
    /// The consumer is closed: by the application, or because its segment can't be subscribed to any more.
    Closed,
}

/// The consumer's registration on a connection.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Link {
    /// Not subscribed, and waiting for the backoff before trying again.
    Idle {
        retry_at: Instant,
    },
    Connecting {
        generation: u64,
    },
    Ready {
        generation: u64,
        consumer_id: u64,
    },
    /// Closed while subscribing: the subscription is closed when it completes.
    Abandoned {
        generation: u64,
    },
    /// Our `CommandCloseConsumer` is out.
    Closing {
        generation: u64,
    },
    Closed,
}

/// An entry, by ledger and entry id.
type EntryKey = (u64, u64);

/// See the module documentation.
#[derive(Debug)]
pub struct SegmentConsumer {
    options: ConsumerOptions,
    segment_id: i64,
    topic: String,
    link: Link,
    generation: u64,
    subscribed_once: bool,
    first_deadline: Instant,
    backoff: Backoff,
    jitter: Jitter,
    /// Messages received and not taken by the application yet.
    incoming: VecDeque<ReceivedMessage>,
    /// Messages taken, or left out, since the last permits went out.
    taken: u32,
    /// Acks waiting to go out: whole entries, and the batches with only some indexes acknowledged, as the bitset of
    /// their indexes still unacknowledged.
    acks: BTreeSet<EntryKey>,
    batch_acks: BTreeMap<EntryKey, Vec<u64>>,
    /// The cumulative ack waiting to go out.
    cumulative: Option<EntryId>,
    /// The last cumulative ack that went out on this registration: one at or before it adds nothing. The next
    /// registration forgets it, since the broker may deliver again what it covered.
    cumulative_sent: Option<EntryId>,
    ack_deadline: Option<Instant>,
    /// Negatively acknowledged entries, and when to ask for them again.
    nacks: BTreeMap<EntryKey, Instant>,
    /// On a `Key_Shared` subscription, the messages given to the application and not acknowledged yet, in the order
    /// given, which a redelivery takes out of the segment's order. A message given again keeps its place.
    given: VecDeque<EntryId>,
    /// The same messages, to look them up.
    given_ids: BTreeSet<EntryId>,
    /// On the others, the last message given to the application, and the highest position it acknowledged
    /// cumulatively.
    last_given: Option<EntryId>,
    acked_up_to: Option<EntryId>,
    /// Draining: the application gets nothing more.
    paused: bool,
    end_of_topic: bool,
    /// A reader's position: the last message taken, or where it starts. `None` for a durable subscription.
    position: Option<EntryId>,
    /// Why it gave up: a subscribe that failed for good.
    failure: Option<RegisterError>,
    closed: bool,
    actions: VecDeque<Action>,
}

impl SegmentConsumer {
    /// A consumer of segment `segment_id`, whose topic is `topic`, subscribing now.
    pub fn new(options: ConsumerOptions, segment_id: i64, topic: String, now: Instant) -> Self {
        let first_deadline = now + options.operation_timeout;
        let mut consumer = Self {
            options,
            segment_id,
            topic,
            link: Link::Connecting { generation: 1 },
            generation: 1,
            subscribed_once: false,
            first_deadline,
            backoff: Backoff::new(RECONNECT_BACKOFF.0, RECONNECT_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            incoming: VecDeque::new(),
            taken: 0,
            acks: BTreeSet::new(),
            batch_acks: BTreeMap::new(),
            cumulative: None,
            cumulative_sent: None,
            ack_deadline: None,
            nacks: BTreeMap::new(),
            given: VecDeque::new(),
            given_ids: BTreeSet::new(),
            last_given: None,
            acked_up_to: None,
            paused: false,
            end_of_topic: false,
            position: None,
            failure: None,
            closed: false,
            actions: VecDeque::new(),
        };
        consumer.actions.push_back(Action::Connect { generation: 1 });
        consumer
    }

    /// A reader of segment `segment_id`, subscribing now, that starts after `start`: [`EntryId::EARLIEST`],
    /// [`EntryId::LATEST`] (see [`SegmentConsumer::resolve`]), or a message's id. Its subscription is not durable,
    /// and its name is the options' subscription.
    pub fn reader(options: ConsumerOptions, segment_id: i64, topic: String, start: EntryId, now: Instant) -> Self {
        let mut reader = Self::new(options, segment_id, topic, now);
        reader.position = Some(start);
        reader
    }

    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// A reader's position: the last message taken, or where it starts.
    pub fn position(&self) -> Option<EntryId> {
        self.position
    }

    /// The generation of its subscription, while it is subscribed.
    pub fn subscription(&self) -> Option<u64> {
        match self.link {
            Link::Ready { generation, .. } => Some(generation),
            _ => None,
        }
    }

    /// Why it gave up, if it did: the error of a subscribe that failed for good.
    pub fn failure(&self) -> Option<&RegisterError> {
        self.failure.as_ref()
    }

    /// The `CommandSubscribe` to register the consumer with, as `consumer_id`. The driver sets its request id.
    pub fn subscribe_command(&self, consumer_id: u64) -> BaseCommand {
        let key_values = |pairs: &[(String, String)]| -> Vec<KeyValue> {
            pairs
                .iter()
                .map(|(key, value)| KeyValue { key: key.clone(), value: value.clone(), ..Default::default() })
                .collect()
        };
        let subscribe = CommandSubscribe {
            topic: self.topic.clone(),
            subscription: self.options.subscription.clone(),
            subType: self.options.sub_type,
            consumer_id,
            consumer_name: self.options.consumer_name.clone(),
            priority_level: self.options.priority_level,
            metadata: key_values(&self.options.properties),
            schema: self.options.schema.clone().into(),
            initialPosition: Some(self.options.initial_position),
            subscription_properties: key_values(&self.options.subscription_properties),
            keySharedMeta: (self.options.sub_type == SubType::Key_Shared)
                .then(|| KeySharedMeta {
                    keySharedMode: KeySharedMode::STICKY,
                    hashRanges: self.options.bucket_ranges.clone(),
                    entryBucketDispatch: Some(true),
                    ..Default::default()
                })
                .into(),
            // A reader's subscription lasts as long as its consumer, and starts after its position.
            durable: self.position.map(|_| false),
            start_message_id: self.position.map(|position| position.to_proto()).into(),
            ..Default::default()
        };
        BaseCommand { r#type: Type::SUBSCRIBE.into(), subscribe: subscribe.into(), ..Default::default() }
    }

    /// The broker registered the consumer of `generation` as `consumer_id`.
    pub fn on_subscribed(&mut self, generation: u64, consumer_id: u64) {
        if self.link == (Link::Abandoned { generation }) {
            // Closed meanwhile: close the subscription unused.
            self.link = Link::Closing { generation };
            self.actions.push_back(Action::CloseConsumer { generation });
            return;
        }
        if self.link != (Link::Connecting { generation }) {
            return;
        }
        self.link = Link::Ready { generation, consumer_id };
        self.backoff.reset();
        self.cumulative_sent = None;
        // A reader at the latest position waits for where that is.
        if self.position != Some(EntryId::LATEST) {
            self.start(consumer_id);
        }
    }

    /// A reader at the latest position learns where that is, from the segment's last message id asked on its
    /// subscription of `generation`, and starts reading after it. The messages the broker sends from before that
    /// position, published while the reader subscribed, are left out.
    pub fn resolve(&mut self, generation: u64, position: EntryId) {
        let Link::Ready { generation: current, consumer_id } = self.link else { return };
        if current == generation && self.position == Some(EntryId::LATEST) {
            self.position = Some(position);
            self.start(consumer_id);
        }
    }

    /// Completes the first subscribe, and grants the broker its permits.
    fn start(&mut self, consumer_id: u64) {
        if !std::mem::replace(&mut self.subscribed_once, true) {
            self.actions.push_back(Action::Subscribed(Ok(())));
        }
        // The acks made while subscribing go first, as in Java.
        self.flush_acks();
        self.taken = 0;
        self.send(flow(consumer_id, self.options.receiver_queue_size));
    }

    /// Subscribing the consumer of `generation` failed. Errors worth retrying are retried with backoff: for as long
    /// as the operation timeout the first time, and without end after that. Any other error closes the consumer.
    pub fn on_subscribe_failed(&mut self, generation: u64, error: RegisterError, now: Instant) {
        if self.link == (Link::Abandoned { generation }) {
            self.link = Link::Closed;
            return;
        }
        if self.link != (Link::Connecting { generation }) {
            return;
        }
        let worth_retrying = match &error {
            RegisterError::Server { error, .. } => retriable(*error),
            RegisterError::Unavailable(_) => true,
        };
        if worth_retrying && (self.subscribed_once || now < self.first_deadline) {
            return self.retry_later(now);
        }
        self.link = Link::Closed;
        self.failure = Some(error.clone());
        if !self.subscribed_once {
            self.actions.push_back(Action::Subscribed(Err(error)));
        }
        self.finish_close();
    }

    /// An entry the broker pushed for the consumer of `generation`.
    pub fn on_message(&mut self, generation: u64, message: &CommandMessage, section: &PayloadSection) {
        let Link::Ready { generation: current, consumer_id } = self.link else { return };
        if current != generation {
            return;
        }
        match entry::decode(self.segment_id, message, section) {
            Ok(decoded) => {
                self.taken += decoded.skipped;
                for message in decoded.messages {
                    // A reader leaves out what it read before it subscribed.
                    if self.position.is_some_and(|position| reached(&position, &message.id.entry)) {
                        self.taken += 1;
                    } else {
                        self.incoming.push_back(message);
                    }
                }
            }
            Err(error) => {
                // A corrupt entry is dropped with the reason; one this client can't read stays unacknowledged.
                let id = &message.message_id;
                if let Some(validation) = error.validation_error() {
                    self.send(ack(consumer_id, vec![entry_id(id.ledgerId, id.entryId)], Some(validation)));
                }
                self.taken += 1;
            }
        }
        self.grant_permits();
    }

    /// The broker says the segment's topic is terminated and the subscription has acknowledged everything there.
    pub fn on_reached_end_of_topic(&mut self, generation: u64) {
        if matches!(self.link, Link::Ready { generation: current, .. } if current == generation) {
            self.end_of_topic = true;
        }
    }

    /// The consumer of `generation` lost its connection, or the broker closed it.
    pub fn on_disconnected(&mut self, generation: u64, now: Instant) {
        match self.link {
            Link::Ready { generation: current, .. } if current == generation => {
                // The broker delivers them again.
                self.incoming.clear();
                self.retry_later(now);
            }
            Link::Closing { generation: current } | Link::Abandoned { generation: current }
                if current == generation =>
            {
                self.on_closed(generation);
            }
            _ => {}
        }
    }

    /// Our `CommandCloseConsumer` for `generation` was answered, or its connection closed.
    pub fn on_closed(&mut self, generation: u64) {
        if matches!(self.link, Link::Closing { generation: current } | Link::Abandoned { generation: current } if current == generation)
        {
            self.link = Link::Closed;
            self.finish_close();
        }
    }

    /// The next message for the application, if one is in and the consumer isn't paused.
    pub fn next_message(&mut self) -> Option<ReceivedMessage> {
        if self.paused {
            return None;
        }
        let message = self.incoming.pop_front()?;
        if let Some(position) = &mut self.position {
            *position = message.id.entry;
        }
        self.taken += 1;
        self.grant_permits();
        if self.options.sub_type == SubType::Key_Shared {
            if self.given_ids.insert(message.id.entry) {
                self.given.push_back(message.id.entry);
            }
        } else {
            self.last_given = Some(message.id.entry);
        }
        Some(message)
    }

    /// How many messages are in, waiting for the application: none while paused.
    pub fn available(&self) -> usize {
        if self.paused { 0 } else { self.incoming.len() }
    }

    /// Acknowledges a message this consumer delivered.
    pub fn acknowledge(&mut self, id: &EntryId, now: Instant) {
        if self.given_ids.remove(id) {
            self.given.retain(|given| given != id);
        }
        self.record_ack(id);
        self.group_acks(now);
    }

    /// Acknowledges every message up to and including `position` (*client-behavior* §6). On an Exclusive
    /// subscription, one cumulative ack carries it, unless one at or after it went out already on this registration.
    /// A `Key_Shared` subscription forbids cumulative acks: every message given to the application up to the one at
    /// `position`, in the order given, is acknowledged on its own (*key-shared* §4). Not by position, as the spec
    /// says: a redelivery, such as the messages of a member that crashed, can come after messages further on in the
    /// segment. By position, an ack of the redelivered message would leave those given before it, which a drain then
    /// waits for in vain, and an ack of one of those would cover a redelivered message the application took later.
    /// Not for a Shared subscription, where nobody acknowledges cumulatively.
    pub fn acknowledge_cumulative(&mut self, position: &EntryId, now: Instant) {
        if self.options.sub_type == SubType::Key_Shared {
            if self.given_ids.contains(position) {
                while let Some(id) = self.given.pop_front() {
                    self.given_ids.remove(&id);
                    self.record_ack(&id);
                    if id == *position {
                        break;
                    }
                }
                self.group_acks(now);
            }
            return;
        }
        self.acked_up_to = self.acked_up_to.max(Some(*position));
        if Some(*position) <= self.cumulative.max(self.cumulative_sent) {
            return;
        }
        self.cumulative = Some(*position);
        self.group_acks(now);
    }

    /// Asks for a message again after the negative ack delay. The broker delivers its whole entry again.
    pub fn negative_acknowledge(&mut self, id: &EntryId, now: Instant) {
        self.nacks.insert((id.ledger_id as u64, id.entry_id as u64), now + self.options.negative_ack_delay);
    }

    /// Whether the segment is done for this consumer: its topic is terminated, the subscription acknowledged
    /// everything there, and this consumer has nothing left to deliver or to acknowledge.
    pub fn is_drained(&self) -> bool {
        self.end_of_topic
            && self.incoming.is_empty()
            && self.acks.is_empty()
            && self.batch_acks.is_empty()
            && self.cumulative.is_none()
    }

    /// Stops giving the application messages, to drain the consumer before it closes: the messages in stay for the
    /// broker to deliver again once it does. For good.
    pub fn pause(&mut self) {
        self.paused = true;
    }

    /// Whether the application acknowledged every message it was given: each one on a `Key_Shared` subscription, the
    /// last one, cumulatively, on the others.
    pub fn is_settled(&self) -> bool {
        self.given.is_empty() && self.last_given <= self.acked_up_to
    }

    /// Whether the consumer is closed and nothing of it is left on a connection: no subscribe or close in flight.
    /// [`Action::Closed`] can come before that: a consumer closed while subscribing still closes the subscription
    /// once the subscribe completes.
    pub fn is_finished(&self) -> bool {
        self.link == Link::Closed
    }

    /// Closes the consumer: its acks go out first. The messages not taken yet, and the negative acks still
    /// waiting, are dropped: the broker delivers those messages again. Can be called again.
    pub fn close(&mut self) {
        self.incoming.clear();
        self.nacks.clear();
        match self.link {
            Link::Ready { generation, .. } => {
                self.flush_acks();
                self.link = Link::Closing { generation };
                self.actions.push_back(Action::CloseConsumer { generation });
            }
            Link::Connecting { generation } => {
                self.link = Link::Abandoned { generation };
                self.finish_close();
            }
            Link::Idle { .. } => {
                self.link = Link::Closed;
                self.finish_close();
            }
            Link::Abandoned { .. } | Link::Closing { .. } | Link::Closed => {}
        }
    }

    /// When the consumer next has something to do: send its acks, ask for negatively acknowledged messages, or
    /// subscribe again.
    pub fn poll_timeout(&self) -> Option<Instant> {
        let retry = match self.link {
            Link::Idle { retry_at } => Some(retry_at),
            _ => None,
        };
        [retry, self.ack_deadline, self.nacks.values().min().copied()].into_iter().flatten().min()
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        if self.ack_deadline.is_some_and(|deadline| deadline <= now) {
            self.flush_acks();
        }
        let due: Vec<EntryKey> = self.nacks.iter().filter(|(_, at)| **at <= now).map(|(key, _)| *key).collect();
        for key in &due {
            self.nacks.remove(key);
        }
        // Not subscribed: the broker delivers everything unacknowledged again on its own.
        if let Link::Ready { consumer_id, .. } = self.link {
            for chunk in due.chunks(REDELIVER_CHUNK) {
                let ids = chunk.iter().map(|(ledger, entry)| entry_id(*ledger, *entry)).collect();
                self.send(redeliver(consumer_id, ids));
            }
        }
        if let Link::Idle { retry_at } = self.link
            && retry_at <= now
        {
            self.generation += 1;
            self.link = Link::Connecting { generation: self.generation };
            self.actions.push_back(Action::Connect { generation: self.generation });
        }
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    fn retry_later(&mut self, now: Instant) {
        let delay = self.backoff.next_delay(self.jitter.next());
        self.link = Link::Idle { retry_at: now + delay };
    }

    fn finish_close(&mut self) {
        if !std::mem::replace(&mut self.closed, true) {
            self.actions.push_back(Action::Closed);
        }
    }

    /// Grants the broker more permits once half the receiver queue was taken.
    fn grant_permits(&mut self) {
        let Link::Ready { consumer_id, .. } = self.link else { return };
        if self.taken >= (self.options.receiver_queue_size / 2).max(1) {
            let permits = std::mem::take(&mut self.taken);
            self.send(flow(consumer_id, permits));
        }
    }

    /// Takes note of an individual ack: a whole entry, or one index of a batch.
    fn record_ack(&mut self, id: &EntryId) {
        let key = (id.ledger_id as u64, id.entry_id as u64);
        self.nacks.remove(&key);
        if id.batch_index < 0 || id.batch_size <= 1 {
            self.batch_acks.remove(&key);
            self.acks.insert(key);
        } else {
            let unacked = self.batch_acks.entry(key).or_insert_with(|| all_set(id.batch_size as usize));
            let index = id.batch_index as usize;
            unacked[index / 64] &= !(1 << (index % 64));
            if unacked.iter().all(|word| *word == 0) {
                self.batch_acks.remove(&key);
                self.acks.insert(key);
            }
        }
    }

    /// Sends the acks waiting at once if the group is full or there is no grouping, else at the ack group time.
    fn group_acks(&mut self, now: Instant) {
        if self.options.ack_group_time.is_zero()
            || self.acks.len() + self.batch_acks.len() >= self.options.max_ack_group_size
        {
            self.flush_acks();
        } else {
            self.ack_deadline.get_or_insert(now + self.options.ack_group_time);
        }
    }

    /// Sends the acks waiting, when subscribed; otherwise they wait for the subscription.
    fn flush_acks(&mut self) {
        self.ack_deadline = None;
        let Link::Ready { consumer_id, .. } = self.link else { return };
        if let Some(position) = self.cumulative.take() {
            self.cumulative_sent = Some(position);
            self.send(cumulative_ack(consumer_id, &position));
        }
        if self.acks.is_empty() && self.batch_acks.is_empty() {
            return;
        }
        // Whole entries first, then batch indexes, as Java writes them.
        let mut ids: Vec<MessageIdData> =
            std::mem::take(&mut self.acks).into_iter().map(|(ledger, entry)| entry_id(ledger, entry)).collect();
        for ((ledger, entry), unacked) in std::mem::take(&mut self.batch_acks) {
            let mut id = entry_id(ledger, entry);
            id.ack_set = unacked.into_iter().map(|word| word as i64).collect();
            ids.push(id);
        }
        self.send(ack(consumer_id, ids, None));
    }

    fn send(&mut self, command: BaseCommand) {
        self.actions.push_back(Action::Send(Box::new(command)));
    }
}

/// Whether a reader at `position` read `id` already: `id` is in an earlier entry, or in the same entry and not after
/// `position`. As in Java's v4 consumer, a message of a batch compares by its index, and a message that isn't in a
/// batch is its whole entry.
pub fn reached(position: &EntryId, id: &EntryId) -> bool {
    match (id.ledger_id, id.entry_id).cmp(&(position.ledger_id, position.entry_id)) {
        std::cmp::Ordering::Less => true,
        std::cmp::Ordering::Equal => id.batch_index < 0 || id.batch_index <= position.batch_index,
        std::cmp::Ordering::Greater => false,
    }
}

/// The bitset of a batch of `size` messages with every index set, in `u64` words.
fn all_set(size: usize) -> Vec<u64> {
    let mut words = vec![u64::MAX; size.div_ceil(64)];
    if !size.is_multiple_of(64) {
        words[size / 64] = (1 << (size % 64)) - 1;
    }
    words
}

fn entry_id(ledger: u64, entry: u64) -> MessageIdData {
    MessageIdData { ledgerId: ledger, entryId: entry, ..Default::default() }
}

fn flow(consumer_id: u64, permits: u32) -> BaseCommand {
    let flow = CommandFlow { consumer_id, messagePermits: permits, ..Default::default() };
    BaseCommand { r#type: Type::FLOW.into(), flow: flow.into(), ..Default::default() }
}

fn ack(consumer_id: u64, ids: Vec<MessageIdData>, validation_error: Option<ValidationError>) -> BaseCommand {
    let ack = CommandAck {
        consumer_id,
        ack_type: AckType::Individual,
        message_id: ids,
        validation_error,
        ..Default::default()
    };
    BaseCommand { r#type: Type::ACK.into(), ack: ack.into(), ..Default::default() }
}

/// A cumulative ack up to `position`. Inside a batch, the indexes after it stay unacknowledged: they are the bits its
/// ack set keeps, as Java writes them.
fn cumulative_ack(consumer_id: u64, position: &EntryId) -> BaseCommand {
    let mut id = entry_id(position.ledger_id as u64, position.entry_id as u64);
    let index = position.batch_index;
    if index >= 0 && index < position.batch_size - 1 {
        let mut unacked = all_set(position.batch_size as usize);
        for acked in 0..=index as usize {
            unacked[acked / 64] &= !(1 << (acked % 64));
        }
        id.ack_set = unacked.into_iter().map(|word| word as i64).collect();
    }
    let ack = CommandAck { consumer_id, ack_type: AckType::Cumulative, message_id: vec![id], ..Default::default() };
    BaseCommand { r#type: Type::ACK.into(), ack: ack.into(), ..Default::default() }
}

fn redeliver(consumer_id: u64, ids: Vec<MessageIdData>) -> BaseCommand {
    let redeliver = CommandRedeliverUnacknowledgedMessages { consumer_id, message_ids: ids, ..Default::default() };
    BaseCommand {
        r#type: Type::REDELIVER_UNACKNOWLEDGED_MESSAGES.into(),
        redeliverUnacknowledgedMessages: redeliver.into(),
        ..Default::default()
    }
}
