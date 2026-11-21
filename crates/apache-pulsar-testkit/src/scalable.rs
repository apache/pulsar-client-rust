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

//! A simulated cluster for scalable topics: topics and their layouts, segments, and the producers, subscriptions and
//! readers on them, over any number of connections.
//!
//! [`World`] is sans IO like the rest of the testkit: a test, or a socket server, feeds each connection's bytes to
//! [`World::input`] and writes out what [`World::output`] returns. Admin operations ([`World::split`],
//! [`World::merge`], [`World::terminate`]) behave like the 5.0.1 broker's: sealing a segment terminates its topic,
//! which disconnects its producers with `CommandCloseProducer`, then the new layout goes to every watch. The
//! children of a split or a merge start with only the subscriptions registered on the scalable topic, which a Queue
//! consumer's subscribe doesn't register, and a Stream or Checkpoint consumer's registration does. The controller's garbage collection of a sealed segment is two steps, which a test takes in either
//! order: [`World::prune`] drops it from the layout, [`World::delete_segment`] deletes its topic.
//!
//! A segment's Shared subscriptions dispatch like the broker's Shared dispatcher: the entries to redeliver first, in
//! position order, then the ones not read yet, each to the next consumer with permits on the highest priority
//! level, round robin. Subscriptions outlive their consumers, take acks per message, batch indexes included, and tell
//! their consumers when they acked everything of a terminated segment, with `CommandReachedEndOfTopic`.
//!
//! A reader's subscription isn't durable: it starts where the broker places such a cursor (the start id's entry,
//! which goes out again), goes with its consumer, and is told the segment ended only if it starts at the end, since
//! readers don't acknowledge. `CommandGetLastMessageId` is answered with the segment's last message id.
//!
//! Schemas are a topic's, as on the broker: one history for all its segments. A producer's schema takes the version
//! of an equal one, or becomes the next version unless its type differs from the latest's; a consumer's schema must
//! have the latest's type, and becomes the topic's first on a segment with no producer, consumer or entry. A
//! mismatch is refused with `IncompatibleSchema`. Unlike the broker, the world checks no compatibility within a type.
//!
//! Unlike the broker, the world hands a consumer one entry a turn, where the broker hands it up to
//! `dispatcherMaxRoundRobinBatchSize`; and tells a consumer the topic ended once, where the broker tells it again on
//! later acks and reads.
//!
//! Exclusive subscriptions and `Key_Shared` ones by entry bucket, and rewinds, are in the `subscriptions` module; the
//! controller, which registers Stream and Checkpoint consumers and assigns them segments, in [`controller`].

pub mod controller;
mod subscriptions;

use std::collections::{BTreeMap, BTreeSet, HashMap, VecDeque};

use apache_pulsar_protocol::entry::{self, ReceivedMessage};
use apache_pulsar_protocol::frame::{Frame, FrameDecoder, PayloadSection, encode_command, encode_payload_command};
use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_ack::AckType;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseConsumer, CommandCloseProducer, CommandGetLastMessageIdResponse, CommandMessage,
    CommandProducerSuccess, CommandReachedEndOfTopic, CommandScalableTopicUpdate, CommandSendError, CommandSendReceipt,
    CompressionType, FeatureFlags, MessageIdData, MessageMetadata, ProducerAccessMode, ScalableTopicDAG, Schema,
    SegmentInfoProto, SegmentState, ServerError, SingleMessageMetadata,
};
use buffa::{EnumValue, Message};
use bytes::{Buf, Bytes, BytesMut};

use crate::{LookupAnswer, reply};

/// A connection, as the world knows it.
pub type ConnId = usize;

/// A message a segment stored.
#[derive(Debug, Clone, PartialEq)]
pub struct Stored {
    pub segment: u64,
    pub ledger_id: u64,
    pub entry_id: u64,
    pub producer_name: String,
    pub sequence_id: u64,
    /// The key of the entry's first message. `None` in a compressed entry: the world doesn't decompress.
    pub key: Option<String>,
    /// The payload of the entry's first message. Empty in a compressed entry.
    pub value: Bytes,
    pub metadata: MessageMetadata,
    /// The first message's own metadata, in a batch. `None` in a compressed entry.
    pub single: Option<SingleMessageMetadata>,
    /// The entry's payload as it came: one message or a whole batch, compressed or not. Consumers get it as is.
    pub payload: Bytes,
}

impl Stored {
    /// Every message of the entry, as a consumer reads them: decompressed, and split out of its batch.
    pub fn messages(&self) -> Vec<ReceivedMessage> {
        let section = PayloadSection {
            broker_entry_metadata: None,
            checksum_valid: Some(true),
            metadata: self.metadata.encode_to_vec().into(),
            payload: self.payload.clone(),
        };
        let id = MessageIdData { ledgerId: self.ledger_id, entryId: self.entry_id, ..Default::default() };
        let message = CommandMessage { message_id: id.into(), ..Default::default() };
        entry::decode(self.segment as i64, &message, &section).expect("an entry a consumer can't read").messages
    }
}

#[derive(Debug)]
struct Segment {
    info: SegmentInfoProto,
    /// The topic its producers and consumers attach to.
    name: String,
    ledger_id: u64,
    entries: Vec<Stored>,
    terminated: bool,
    /// Dropped from the layout by [`World::prune`].
    pruned: bool,
    /// Its topic was deleted by [`World::delete_segment`]: it can't be found any more.
    deleted: bool,
    /// Subscriptions by name. They outlive their consumers.
    subscriptions: BTreeMap<String, Subscription>,
    /// Producers, in registration order: `(connection, producer id, name)`.
    producers: Vec<(ConnId, u64, String)>,
    exclusive: bool,
    /// Wait-for-exclusive producers queued behind the current one: `(connection, producer id, request id, name)`.
    waiting: VecDeque<(ConnId, u64, u64, String)>,
    /// Bumped by each exclusive producer; a producer that comes back with an older one was fenced.
    topic_epoch: u64,
    /// The highest sequence id stored per producer name, for deduplication.
    last_sequence: HashMap<String, i64>,
}

/// A subscription on a segment, or a reader's.
#[derive(Debug, Default)]
struct Subscription {
    /// A reader's subscription: not durable, it goes with its consumer.
    reader: bool,
    /// The entries not acked yet, from the subscription's initial position on, each with its batch indexes not
    /// acked yet.
    unacked: BTreeMap<u64, BTreeSet<u32>>,
    /// The next entry to read: the ones before it were dispatched already.
    read: u64,
    /// Entries to dispatch again, before reading more.
    redeliver: BTreeSet<u64>,
    /// How many times each entry was redelivered.
    redeliveries: HashMap<u64, u32>,
    /// In the order they subscribed.
    consumers: Vec<Consumer>,
    /// Where the round robin looks for the next consumer first.
    next: usize,
    /// Its type, which the first of its consumers sets.
    kind: subscriptions::Kind,
}

#[derive(Debug)]
struct Consumer {
    conn: ConnId,
    id: u64,
    name: String,
    /// The lowest level is served first.
    priority: i32,
    /// The messages it can take. A batch can take it below zero.
    permits: i64,
    /// The entries sent to it and not acked yet.
    pending: BTreeSet<u64>,
    /// Whether it was told the topic ended.
    ended: bool,
}

#[derive(Debug)]
struct Topic {
    epoch: u64,
    segments: BTreeMap<u64, Segment>,
    next_segment: u64,
    /// The schemas registered, oldest first: a schema's version is its index. As on the broker, a scalable topic
    /// keeps one history, which all its segments share.
    schemas: Vec<Schema>,
}

#[derive(Debug, Default)]
struct Conn {
    decoder: FrameDecoder,
    input: BytesMut,
    out: BytesMut,
    open: bool,
    /// Watch sessions: session id to topic.
    sessions: HashMap<u64, String>,
    /// Producers: producer id to `(topic, segment)`.
    producers: HashMap<u64, (String, u64)>,
    /// Producers the broker closed: their late sends are ignored, as the 5.0.1 broker's `recentlyClosedProducers`.
    closed_producers: std::collections::HashSet<u64>,
    /// Consumers: consumer id to `(topic, segment, subscription)`.
    consumers: HashMap<u64, (String, u64, String)>,
}

/// The simulated cluster. See the module documentation.
#[derive(Debug)]
pub struct World {
    pub features: FeatureFlags,
    /// How to answer `CommandLookupTopic`, for every topic.
    pub lookup: LookupAnswer,
    /// Whether a lookup of a missing topic creates it, with one segment.
    pub auto_create: bool,
    /// Whether segments discard a sequence id at or below the last one stored for the producer name.
    pub dedup: bool,
    /// While set, receipts are held back, until [`World::release_receipts`]. A terminate sends them first, as the
    /// broker completes its pending writes before it terminates a topic.
    pub hold_receipts: bool,
    /// While set, sends are not processed, as if still on the way, until [`World::release_sends`].
    pub hold_sends: bool,
    /// Errors to answer the next sends with, one each, instead of storing them.
    pub fail_sends: VecDeque<ServerError>,
    /// While set, subscribes wait unanswered until [`World::release_subscribes`], as while the broker creates the
    /// consumer: closing the consumer meanwhile discards it, and its subscribe never gets an answer.
    pub hold_subscribes: bool,
    /// Errors to answer the next subscribes with, one each, instead of subscribing.
    pub fail_subscribes: VecDeque<ServerError>,
    /// Errors to answer the next subscribes to a topic with, one each, before [`World::fail_subscribes`].
    pub fail_subscribes_to: HashMap<String, VecDeque<ServerError>>,
    /// Topics whose subscribes wait unanswered until [`World::release_subscribes`], as [`World::hold_subscribes`]
    /// makes every subscribe wait.
    pub hold_subscribes_to: BTreeSet<String>,
    /// While set, `CommandGetLastMessageId` waits unanswered until [`World::release_last_message_ids`].
    pub hold_last_message_ids: bool,
    /// Errors to answer the next `CommandGetLastMessageId` with, one each.
    pub fail_last_message_ids: VecDeque<ServerError>,
    /// While set, scalable-topic lookups get no answer.
    pub silent_lookups: bool,
    /// Every command received, per connection.
    pub log: Vec<(ConnId, BaseCommand)>,
    /// The topics' controllers.
    pub controller: controller::Controller,
    topics: BTreeMap<String, Topic>,
    conns: BTreeMap<ConnId, Conn>,
    held: Vec<(ConnId, BaseCommand)>,
    held_sends: Vec<(ConnId, Frame)>,
    held_subscribes: Vec<(ConnId, BaseCommand)>,
    held_last_message_ids: Vec<(ConnId, BaseCommand)>,
    next_conn: ConnId,
    next_ledger: u64,
    next_name: u64,
}

impl Default for World {
    fn default() -> Self {
        Self {
            features: FeatureFlags {
                supports_auth_refresh: Some(true),
                supports_broker_entry_metadata: Some(true),
                supports_partial_producer: Some(true),
                supports_topic_watchers: Some(true),
                supports_scalable_topics: Some(true),
                ..Default::default()
            },
            lookup: LookupAnswer::Silent,
            auto_create: true,
            dedup: false,
            hold_receipts: false,
            hold_sends: false,
            fail_sends: VecDeque::new(),
            hold_subscribes: false,
            fail_subscribes: VecDeque::new(),
            fail_subscribes_to: HashMap::new(),
            hold_subscribes_to: BTreeSet::new(),
            hold_last_message_ids: false,
            fail_last_message_ids: VecDeque::new(),
            silent_lookups: false,
            log: Vec::new(),
            topics: BTreeMap::new(),
            conns: BTreeMap::new(),
            held: Vec::new(),
            held_sends: Vec::new(),
            held_subscribes: Vec::new(),
            held_last_message_ids: Vec::new(),
            next_conn: 0,
            next_ledger: 100,
            next_name: 0,
            controller: controller::Controller::default(),
        }
    }
}

impl World {
    /// A client connected.
    pub fn connect(&mut self) -> ConnId {
        let id = self.next_conn;
        self.next_conn += 1;
        self.conns.insert(id, Conn { open: true, ..Default::default() });
        id
    }

    /// The client went away: its producers and sessions go with it.
    pub fn disconnected(&mut self, conn: ConnId) {
        self.drop_conn(conn);
    }

    /// Closes a connection from the broker side, as a broker restart would. The server stops serving it.
    pub fn kill(&mut self, conn: ConnId) {
        self.drop_conn(conn);
    }

    /// Whether the broker still serves the connection.
    pub fn is_open(&self, conn: ConnId) -> bool {
        self.conns.get(&conn).is_some_and(|c| c.open)
    }

    /// The connections the broker still serves.
    pub fn open_connections(&self) -> Vec<ConnId> {
        self.conns.iter().filter(|(_, c)| c.open).map(|(id, _)| *id).collect()
    }

    /// Bytes the client sent on `conn`.
    pub fn input(&mut self, conn: ConnId, bytes: &[u8]) {
        let Some(c) = self.conns.get_mut(&conn) else { return };
        c.input.extend_from_slice(bytes);
        loop {
            let Some(c) = self.conns.get_mut(&conn).filter(|c| c.open) else { return };
            let Some(frame) = c.decoder.decode(&mut c.input).expect("the client sent a bad frame") else { return };
            self.handle(conn, frame);
        }
    }

    /// What the broker sends on `conn` next.
    pub fn output(&mut self, conn: ConnId) -> Bytes {
        self.conns.get_mut(&conn).map(|c| c.out.split().freeze()).unwrap_or_default()
    }

    /// The commands of `kind` the client sent, on any connection.
    pub fn received(&self, kind: Type) -> Vec<&BaseCommand> {
        self.log.iter().filter(|(_, c)| c.r#type == EnumValue::Known(kind)).map(|(_, c)| c).collect()
    }

    /// Creates a topic with `segments` segments splitting the hash ring evenly.
    pub fn create_topic(&mut self, topic: &str, segments: u32) {
        assert!(segments > 0 && topic.starts_with("topic://"), "bad topic {topic} or segment count");
        let ledger = &mut self.next_ledger;
        let mut topic_state = Topic { epoch: 0, segments: BTreeMap::new(), next_segment: 0, schemas: Vec::new() };
        let parent = &topic["topic://".len()..];
        for i in 0..segments {
            let start = (u64::from(i) * 0x1_0000 / u64::from(segments)) as u32;
            let end = (u64::from(i + 1) * 0x1_0000 / u64::from(segments) - 1) as u32;
            let id = topic_state.next_segment;
            topic_state.next_segment += 1;
            *ledger += 1;
            let info = SegmentInfoProto {
                segment_id: id,
                hash_start: start,
                hash_end: end,
                state: SegmentState::ACTIVE,
                ..Default::default()
            };
            topic_state.segments.insert(id, Segment::new(info, segment_name(parent, start, end, id), *ledger));
        }
        self.topics.insert(topic.to_string(), topic_state);
    }

    /// Creates a synthetic layout over a `persistent://` topic: one legacy segment per partition, or one for a
    /// non-partitioned topic (`partitions == 0`).
    pub fn create_legacy_topic(&mut self, topic: &str, partitions: u32) {
        assert!(topic.starts_with("persistent://"), "bad topic {topic}");
        let mut topic_state = Topic { epoch: 0, segments: BTreeMap::new(), next_segment: 0, schemas: Vec::new() };
        let names: Vec<String> = match partitions {
            0 => vec![topic.to_string()],
            n => (0..n).map(|i| format!("{topic}-partition-{i}")).collect(),
        };
        for (id, name) in names.into_iter().enumerate() {
            self.next_ledger += 1;
            let info = SegmentInfoProto {
                segment_id: id as u64,
                hash_start: 0,
                hash_end: 0xffff,
                state: SegmentState::ACTIVE,
                legacy_topic_name: Some(name.clone()),
                ..Default::default()
            };
            topic_state.segments.insert(id as u64, Segment::new(info, name, self.next_ledger));
            topic_state.next_segment += 1;
        }
        self.topics.insert(topic.to_string(), topic_state);
    }

    /// Splits an active segment into two halves, like `pulsar-admin scalable-topics split-segment`. Returns the
    /// ids of the lower and upper halves.
    pub fn split(&mut self, topic: &str, segment: u64) -> (u64, u64) {
        let (start, end) = {
            let info = &self.topic(topic).segments[&segment].info;
            assert_eq!(info.state, SegmentState::ACTIVE, "segment {segment} is not active");
            (info.hash_start, info.hash_end)
        };
        assert!(end > start, "segment {segment} is one hash wide");
        let middle = start + (end - start).div_ceil(2);
        let ids = self.replace(topic, &[segment], &[(start, middle - 1), (middle, end)]);
        (ids[0], ids[1])
    }

    /// Merges two adjacent active segments into one, like `pulsar-admin scalable-topics merge-segments`.
    pub fn merge(&mut self, topic: &str, a: u64, b: u64) -> u64 {
        let range = |w: &World, id: u64| {
            let info = &w.topic(topic).segments[&id].info;
            assert_eq!(info.state, SegmentState::ACTIVE, "segment {id} is not active");
            (info.hash_start, info.hash_end)
        };
        let (a_range, b_range) = (range(self, a), range(self, b));
        let (low, high) = if a_range.0 < b_range.0 { (a_range, b_range) } else { (b_range, a_range) };
        assert_eq!(low.1 + 1, high.0, "segments {a} and {b} are not adjacent");
        self.replace(topic, &[a, b], &[(low.0, high.1)])[0]
    }

    /// Terminates a segment's topic without changing the layout, as `POST …/segments/…/terminate` does.
    pub fn terminate(&mut self, topic: &str, segment: u64) {
        // The broker completes its pending writes first.
        self.release_receipts();
        let producers = {
            let seg = self.topic_mut(topic).segments.get_mut(&segment).expect("no such segment");
            seg.terminated = true;
            seg.exclusive = false;
            seg.waiting.clear();
            std::mem::take(&mut seg.producers)
        };
        for (conn, producer_id, _) in producers {
            self.close_producer(conn, producer_id);
        }
        // Consumers stay, to drain it.
        self.dispatch(topic, segment);
    }

    /// Unloads a segment's topic, as a broker does before another broker takes it: its producers and consumers are
    /// closed, with `CommandCloseProducer` and `CommandCloseConsumer`, and come back on their own.
    pub fn unload(&mut self, topic: &str, segment: u64) {
        let producers = {
            let seg = self.topic_mut(topic).segments.get_mut(&segment).expect("no such segment");
            seg.exclusive = false;
            std::mem::take(&mut seg.producers)
        };
        for (conn, producer_id, _) in producers {
            self.close_producer(conn, producer_id);
        }
        for (conn, consumer_id) in self.consumers_on(topic, segment) {
            let place = self.conn(conn).consumers.remove(&consumer_id).unwrap();
            self.remove_consumer(conn, consumer_id, &place);
            self.push(conn, consumer_closed(consumer_id));
        }
    }

    /// Drops a sealed segment from the layout, in a new epoch, as the controller's garbage collection does once the
    /// segment is past retention: its children no longer list it as a parent. Its topic stays until
    /// [`World::delete_segment`].
    pub fn prune(&mut self, topic: &str, segment: u64) {
        let state = self.topic_mut(topic);
        let seg = state.segments.get_mut(&segment).expect("no such segment");
        assert_eq!(seg.info.state, SegmentState::SEALED, "segment {segment} is not sealed");
        seg.pruned = true;
        let (parents, children) = (seg.info.parent_ids.clone(), seg.info.child_ids.clone());
        for id in children {
            state.segments.get_mut(&id).unwrap().info.parent_ids.retain(|&p| p != segment);
        }
        for id in parents {
            state.segments.get_mut(&id).unwrap().info.child_ids.retain(|&c| c != segment);
        }
        state.epoch += 1;
        self.push_layout(topic);
    }

    /// Deletes a segment's topic, as the controller does once it pruned the segment: its consumers are closed with
    /// `CommandCloseConsumer`, its subscriptions go, and a subscribe to it fails with `TopicNotFound`.
    pub fn delete_segment(&mut self, topic: &str, segment: u64) {
        for (conn, consumer_id) in self.consumers_on(topic, segment) {
            self.conn(conn).consumers.remove(&consumer_id);
            self.push(conn, consumer_closed(consumer_id));
        }
        let seg = self.topic_mut(topic).segments.get_mut(&segment).expect("no such segment");
        seg.deleted = true;
        seg.subscriptions.clear();
    }

    /// Processes the sends held while [`World::hold_sends`] was set, in order.
    pub fn release_sends(&mut self) {
        // A connection that went took its sends with it.
        let held: Vec<(ConnId, Frame)> =
            std::mem::take(&mut self.held_sends).into_iter().filter(|(conn, _)| self.is_open(*conn)).collect();
        for (conn, frame) in held {
            let command = BaseCommand::decode_from_slice(&frame.command).unwrap();
            self.send(conn, &command, &frame);
        }
    }

    /// Answers the `CommandGetLastMessageId` held while [`World::hold_last_message_ids`] was set, in order.
    pub fn release_last_message_ids(&mut self) {
        for (conn, command) in std::mem::take(&mut self.held_last_message_ids) {
            self.last_message_id(conn, &command);
        }
    }

    /// Processes the subscribes held while [`World::hold_subscribes`] was set, in order.
    pub fn release_subscribes(&mut self) {
        // A connection that went took its subscribes with it.
        let held: Vec<(ConnId, BaseCommand)> =
            std::mem::take(&mut self.held_subscribes).into_iter().filter(|(conn, _)| self.is_open(*conn)).collect();
        for (conn, command) in held {
            self.subscribe(conn, &command);
        }
    }

    /// Stores an entry in a segment as if another client produced it, a batch and/or compressed as `metadata`
    /// says, and dispatches it. Returns its `(ledger id, entry id)`.
    ///
    /// The world doesn't decompress: a compressed entry's [`Stored::key`], [`Stored::value`] and [`Stored::single`]
    /// stay empty. Consumers get `payload` as given.
    pub fn publish_raw(&mut self, topic: &str, segment: u64, metadata: MessageMetadata, payload: Bytes) -> (u64, u64) {
        assert!(!self.topic(topic).segments[&segment].terminated, "segment {segment} was terminated");
        let at = self.store(topic, segment, metadata.sequence_id, metadata, payload);
        self.dispatch(topic, segment);
        at
    }

    /// The messages a subscription has not acked yet: a batch counts its indexes not acked.
    pub fn backlog(&self, topic: &str, segment: u64, subscription: &str) -> usize {
        self.topic(topic).segments[&segment].subscriptions[subscription].unacked.values().map(BTreeSet::len).sum()
    }

    /// The consumers a subscription has: none before it exists.
    pub fn consumer_count(&self, topic: &str, segment: u64, subscription: &str) -> usize {
        self.topic(topic).segments[&segment].subscriptions.get(subscription).map_or(0, |s| s.consumers.len())
    }

    /// The active segment of a managed topic that a producer sends `key` to.
    pub fn route(&self, topic: &str, key: &str) -> u64 {
        let layout = Layout::from_dag(&self.dag(topic), topic).unwrap();
        layout.route_key(KeyHash::of(key.as_bytes())).unwrap()
    }

    /// What the topic's segments stored: in the order a reader that drains parents before children would see,
    /// by creation epoch, then segment id, then position.
    pub fn stored(&self, topic: &str) -> Vec<Stored> {
        let mut segments: Vec<&Segment> = self.topic(topic).segments.values().collect();
        segments.sort_by_key(|s| (s.info.created_at_epoch, s.info.segment_id));
        segments.iter().flat_map(|s| s.entries.iter().cloned()).collect()
    }

    /// What one segment stored, in order.
    pub fn stored_in(&self, topic: &str, segment: u64) -> Vec<Stored> {
        self.topic(topic).segments[&segment].entries.clone()
    }

    /// The schemas registered on a topic, oldest first: a schema's version is its index.
    pub fn schemas(&self, topic: &str) -> &[Schema] {
        &self.topic(topic).schemas
    }

    /// Registers a schema on a topic, as a producer with it does, and returns its version. Panics if the broker would
    /// refuse it: its type isn't the latest schema's.
    pub fn register_schema(&mut self, topic: &str, schema: Schema) -> Vec<u8> {
        self.add_schema(topic, Some(&schema)).expect("a schema the topic refuses")
    }

    /// The messages of [`World::stored`]'s entries, in the same order.
    pub fn messages(&self, topic: &str) -> Vec<ReceivedMessage> {
        self.stored(topic).iter().flat_map(Stored::messages).collect()
    }

    /// Pushes the topic's layout again to every watch, with the epoch it already has.
    pub fn repush_layout(&mut self, topic: &str) {
        self.push_layout(topic);
    }

    /// Pushes `error` to every watch of the topic, as a broker that can't serve them for now does. Their sessions
    /// stay open.
    pub fn fail_watchers(&mut self, topic: &str, error: ServerError) {
        for (conn, session_id) in self.watchers(topic) {
            let update = CommandScalableTopicUpdate {
                session_id,
                error: Some(error),
                message: Some("try again later".into()),
                ..Default::default()
            };
            let update = BaseCommand {
                r#type: Type::SCALABLE_TOPIC_UPDATE.into(),
                scalableTopicUpdate: update.into(),
                ..Default::default()
            };
            self.push(conn, update);
        }
    }

    /// Sends the receipts held while [`World::hold_receipts`] was set.
    pub fn release_receipts(&mut self) {
        for (conn, receipt) in std::mem::take(&mut self.held) {
            self.push(conn, receipt);
        }
    }

    /// The consumers on a segment: `(connection, consumer id)`.
    fn consumers_on(&self, topic: &str, segment: u64) -> Vec<(ConnId, u64)> {
        self.conns
            .iter()
            .flat_map(|(conn, c)| c.consumers.iter().map(move |(id, place)| (*conn, *id, place)))
            .filter(|(_, _, (t, s, _))| (t.as_str(), *s) == (topic, segment))
            .map(|(conn, id, _)| (conn, id))
            .collect()
    }

    fn topic(&self, topic: &str) -> &Topic {
        &self.topics[topic]
    }

    fn topic_mut(&mut self, topic: &str) -> &mut Topic {
        self.topics.get_mut(topic).expect("no such topic")
    }

    /// A connection the world accepted: it knows each one for good.
    fn conn(&mut self, conn: ConnId) -> &mut Conn {
        self.conns.get_mut(&conn).expect("a connection the world accepted")
    }

    /// Closes a producer from the broker side: its later sends are ignored.
    fn close_producer(&mut self, conn: ConnId, producer_id: u64) {
        let c = self.conn(conn);
        c.producers.remove(&producer_id);
        c.closed_producers.insert(producer_id);
        self.push(conn, close_producer(producer_id));
    }

    /// Seals `old` and adds segments over `ranges`, as children of all of `old`, in a new epoch.
    fn replace(&mut self, topic: &str, old: &[u64], ranges: &[(u32, u32)]) -> Vec<u64> {
        for &segment in old {
            self.terminate(topic, segment);
        }
        let parent = topic.strip_prefix("topic://").expect("only managed topics split and merge").to_string();
        let mut ledgers = Vec::new();
        for _ in ranges {
            self.next_ledger += 1;
            ledgers.push(self.next_ledger);
        }
        let state = self.topic_mut(topic);
        state.epoch += 1;
        let epoch = state.epoch;
        let mut ids = Vec::new();
        for (&(start, end), ledger) in ranges.iter().zip(ledgers) {
            let id = state.next_segment;
            state.next_segment += 1;
            let info = SegmentInfoProto {
                segment_id: id,
                hash_start: start,
                hash_end: end,
                state: SegmentState::ACTIVE,
                parent_ids: old.to_vec(),
                created_at_epoch: epoch,
                ..Default::default()
            };
            state.segments.insert(id, Segment::new(info, segment_name(&parent, start, end, id), ledger));
            ids.push(id);
        }
        for segment in old {
            let info = &mut state.segments.get_mut(segment).unwrap().info;
            info.state = SegmentState::SEALED;
            info.sealed_at_epoch = Some(epoch);
            info.child_ids = ids.clone();
        }
        self.provision(topic, &ids);
        self.push_layout(topic);
        ids
    }

    fn push_layout(&mut self, topic: &str) {
        let dag = self.dag(topic);
        for (conn, session_id) in self.watchers(topic) {
            self.push(conn, layout_update(session_id, topic, dag.clone()));
        }
        self.rebalance(topic);
    }

    /// The sessions watching `topic`, on the open connections.
    fn watchers(&self, topic: &str) -> Vec<(ConnId, u64)> {
        self.conns
            .iter()
            .filter(|(_, c)| c.open)
            .flat_map(|(id, c)| c.sessions.iter().filter(|(_, t)| *t == topic).map(move |(s, _)| (*id, *s)))
            .collect()
    }

    fn dag(&self, topic: &str) -> ScalableTopicDAG {
        let state = self.topic(topic);
        ScalableTopicDAG {
            epoch: state.epoch,
            segments: state.segments.values().filter(|s| !s.pruned).map(|s| s.info.clone()).collect(),
            controller_broker_url: self.controller.url.clone(),
            ..Default::default()
        }
    }

    fn drop_conn(&mut self, conn: ConnId) {
        let Some(c) = self.conns.get_mut(&conn) else { return };
        c.open = false;
        let producers: Vec<(u64, (String, u64))> = c.producers.drain().collect();
        let consumers: Vec<(u64, (String, u64, String))> = c.consumers.drain().collect();
        c.sessions.clear();
        self.members_disconnected(conn);
        for (producer_id, (topic, segment)) in producers {
            self.remove_producer(conn, producer_id, &topic, segment);
        }
        // All of them go before what they had pending goes to the consumers left.
        for (consumer_id, place) in &consumers {
            self.remove_consumer(conn, *consumer_id, place);
        }
        for (_, (topic, segment, _)) in consumers {
            self.dispatch(&topic, segment);
        }
        for topic in self.topics.values_mut() {
            for segment in topic.segments.values_mut() {
                segment.waiting.retain(|(c, ..)| *c != conn);
            }
        }
    }

    fn push(&mut self, conn: ConnId, command: BaseCommand) {
        // A closed connection gets nothing more.
        let Some(c) = self.conns.get_mut(&conn).filter(|c| c.open) else { return };
        encode_command(&command, &mut c.out).unwrap();
    }

    fn handle(&mut self, conn: ConnId, frame: Frame) {
        let command = BaseCommand::decode_from_slice(&frame.command).expect("client sent an undecodable command");
        self.log.push((conn, command.clone()));
        let EnumValue::Known(kind) = command.r#type else { return };
        match kind {
            Type::CONNECT => {
                let connected = reply::connected(self.features.clone());
                self.push(conn, connected);
            }
            Type::PING => self.push(conn, reply::pong()),
            Type::LOOKUP if self.lookup != LookupAnswer::Silent => {
                let answer = reply::lookup(command.lookupTopic.request_id, &self.lookup);
                self.push(conn, answer);
            }
            Type::SCALABLE_TOPIC_LOOKUP if self.silent_lookups => {}
            Type::SCALABLE_TOPIC_LOOKUP => self.scalable_lookup(conn, &command),
            Type::SCALABLE_TOPIC_CLOSE => {
                self.conn(conn).sessions.remove(&command.scalableTopicClose.session_id);
            }
            Type::PRODUCER => self.producer(conn, &command),
            Type::SEND if self.hold_sends => self.held_sends.push((conn, frame)),
            Type::SEND => self.send(conn, &command, &frame),
            Type::CLOSE_PRODUCER => {
                // The broker answers once the producer's pending writes are done.
                self.release_receipts();
                let close = &command.close_producer;
                let c = self.conn(conn);
                c.closed_producers.insert(close.producer_id);
                // One queued for exclusive access has no segment yet.
                if let Some((topic, segment)) = c.producers.remove(&close.producer_id) {
                    self.remove_producer(conn, close.producer_id, &topic, segment);
                }
                // Closing a queued wait-for-exclusive producer cancels it.
                for topic in self.topics.values_mut() {
                    for segment in topic.segments.values_mut() {
                        segment.waiting.retain(|(c, id, ..)| (*c, *id) != (conn, close.producer_id));
                    }
                }
                self.push(conn, reply::success(close.request_id));
            }
            Type::SUBSCRIBE if self.hold_subscribes || self.hold_subscribes_to.contains(&command.subscribe.topic) => {
                self.held_subscribes.push((conn, command));
            }
            Type::SUBSCRIBE => self.subscribe(conn, &command),
            Type::SCALABLE_TOPIC_SUBSCRIBE => self.register(conn, &command),
            Type::SCALABLE_TOPIC_UNSUBSCRIBE => self.unregister(conn, &command),
            Type::FLOW => self.flow(conn, &command),
            Type::ACK => self.ack(conn, &command),
            Type::REDELIVER_UNACKNOWLEDGED_MESSAGES => self.redeliver(conn, &command),
            Type::CLOSE_CONSUMER => self.close_consumer(conn, &command),
            Type::GET_LAST_MESSAGE_ID if self.hold_last_message_ids => self.held_last_message_ids.push((conn, command)),
            Type::GET_LAST_MESSAGE_ID => self.last_message_id(conn, &command),
            _ => {}
        }
    }

    /// Answers `CommandGetLastMessageId` with the id of the segment's last message, as the broker does: the last
    /// entry, with the index of its last message for a batch; `-1:-1` for an empty segment.
    fn last_message_id(&mut self, conn: ConnId, command: &BaseCommand) {
        let get = &command.getLastMessageId;
        let place = self.conns.get(&conn).and_then(|c| c.consumers.get(&get.consumer_id)).cloned();
        let refusal = match place {
            None => Some(ServerError::ConsumerNotFound),
            Some(_) => self.fail_last_message_ids.pop_front(),
        };
        if let Some(error) = refusal {
            let error = reply::error(get.request_id, error, &format!("no last message id: {error:?}"));
            return self.push(conn, error);
        }
        let (topic, segment, _) = place.expect("a consumer, for a request not refused");
        let last = match self.topic(&topic).segments[&segment].entries.last() {
            Some(stored) => MessageIdData {
                ledgerId: stored.ledger_id,
                entryId: stored.entry_id,
                batch_index: Some(stored.metadata.num_messages_in_batch.map_or(-1, |count| count - 1)),
                ..Default::default()
            },
            None => {
                MessageIdData { ledgerId: u64::MAX, entryId: u64::MAX, batch_index: Some(-1), ..Default::default() }
            }
        };
        let response = CommandGetLastMessageIdResponse {
            last_message_id: last.into(),
            request_id: get.request_id,
            ..Default::default()
        };
        self.push(
            conn,
            BaseCommand {
                r#type: Type::GET_LAST_MESSAGE_ID_RESPONSE.into(),
                getLastMessageIdResponse: response.into(),
                ..Default::default()
            },
        );
    }

    fn scalable_lookup(&mut self, conn: ConnId, command: &BaseCommand) {
        let lookup = &command.scalableTopicLookup;
        let topic = lookup.topic.clone();
        if !self.topics.contains_key(&topic) {
            if self.auto_create && lookup.create_if_missing() && topic.starts_with("topic://") {
                self.create_topic(&topic, 1);
            } else {
                let update = CommandScalableTopicUpdate {
                    session_id: lookup.session_id,
                    error: Some(ServerError::TopicNotFound),
                    message: Some(format!("topic {topic} not found")),
                    ..Default::default()
                };
                self.push(
                    conn,
                    BaseCommand {
                        r#type: Type::SCALABLE_TOPIC_UPDATE.into(),
                        scalableTopicUpdate: update.into(),
                        ..Default::default()
                    },
                );
                return;
            }
        }
        self.conn(conn).sessions.insert(lookup.session_id, topic.clone());
        let update = layout_update(lookup.session_id, &topic, self.dag(&topic));
        self.push(conn, update);
    }

    fn find_segment(&self, name: &str) -> Option<(String, u64)> {
        self.topics.iter().find_map(|(topic, state)| {
            state.segments.values().find(|s| s.name == name && !s.deleted).map(|s| (topic.clone(), s.info.segment_id))
        })
    }

    fn producer(&mut self, conn: ConnId, command: &BaseCommand) {
        let producer = &command.producer;
        let request_id = producer.request_id;
        let Some((topic, segment)) = self.find_segment(&producer.topic) else {
            let error = reply::error(request_id, ServerError::TopicNotFound, "no such segment");
            self.push(conn, error);
            return;
        };
        let name = match &producer.producer_name {
            Some(name) if !name.is_empty() => name.clone(),
            _ => {
                self.next_name += 1;
                format!("sim-{}", self.next_name)
            }
        };
        let mode = producer.producer_access_mode();
        let dedup = self.dedup;
        let seg = self.topics.get_mut(&topic).unwrap().segments.get_mut(&segment).unwrap();
        if seg.terminated {
            let error = reply::error(request_id, ServerError::TopicTerminatedError, "the topic was terminated");
            self.push(conn, error);
            return;
        }
        let schema_version = match self.add_schema(&topic, producer.schema.as_option()) {
            Ok(version) => version,
            Err(message) => {
                self.push(conn, reply::error(request_id, ServerError::IncompatibleSchema, &message));
                return;
            }
        };
        let seg = self.topics.get_mut(&topic).unwrap().segments.get_mut(&segment).unwrap();
        // A producer coming back with the epoch it was given, after a newer one took the topic, was fenced.
        if producer.topic_epoch.is_some_and(|epoch| epoch < seg.topic_epoch) {
            let error = reply::error(request_id, ServerError::ProducerFenced, "the producer was fenced");
            self.push(conn, error);
            return;
        }
        let conflict = seg.exclusive || !seg.waiting.is_empty();
        match mode {
            ProducerAccessMode::Shared if conflict => {
                let error = reply::error(request_id, ServerError::ProducerBusy, "the topic has an exclusive producer");
                self.push(conn, error);
                return;
            }
            ProducerAccessMode::Exclusive if conflict || !seg.producers.is_empty() => {
                let error = reply::error(request_id, ServerError::ProducerFenced, "the topic has producers");
                self.push(conn, error);
                return;
            }
            ProducerAccessMode::WaitForExclusive if conflict || !seg.producers.is_empty() => {
                seg.waiting.push_back((conn, producer.producer_id, request_id, name.clone()));
                let queued = producer_success(request_id, &name, -1, false);
                self.push(conn, queued);
                return;
            }
            ProducerAccessMode::ExclusiveWithFencing => {
                let fenced = std::mem::take(&mut seg.producers);
                seg.exclusive = false;
                for (c, id, _) in fenced {
                    self.close_producer(c, id);
                }
            }
            _ => {}
        }
        let seg = self.topics.get_mut(&topic).unwrap().segments.get_mut(&segment).unwrap();
        seg.exclusive = mode != ProducerAccessMode::Shared;
        let topic_epoch = seg.exclusive.then(|| {
            seg.topic_epoch += 1;
            seg.topic_epoch
        });
        seg.producers.push((conn, producer.producer_id, name.clone()));
        let last = if dedup { seg.last_sequence.get(&name).copied().unwrap_or(-1) } else { -1 };
        self.conn(conn).producers.insert(producer.producer_id, (topic, segment));
        let mut success = producer_success(request_id, &name, last, true);
        let answer = success.producer_success.get_or_insert_default();
        answer.topic_epoch = topic_epoch;
        answer.schema_version = Some(schema_version);
        self.push(conn, success);
    }

    /// Adds a producer's schema to the topic's, as the broker does: one equal to a version already there takes that
    /// version, one of another type than the latest is refused, and any other becomes the next version. The version
    /// is the broker's: 8 bytes, big endian, and none for a producer without a schema.
    fn add_schema(&mut self, topic: &str, schema: Option<&Schema>) -> Result<Vec<u8>, String> {
        let Some(schema) = schema else { return Ok(Vec::new()) };
        let schemas = &mut self.topic_mut(topic).schemas;
        let version = match schemas.iter().position(|registered| same_schema(registered, schema)) {
            Some(version) => version,
            None => match schemas.last() {
                Some(latest) if latest.r#type != schema.r#type => {
                    let (exists, new) = (latest.r#type, schema.r#type);
                    return Err(format!("Incompatible schema: exists schema type {exists:?}, new schema type {new:?}"));
                }
                _ => {
                    schemas.push(schema.clone());
                    schemas.len() - 1
                }
            },
        };
        Ok((version as u64).to_be_bytes().to_vec())
    }

    /// Checks a consumer's schema, as the broker does on subscribing: on a segment with no producer, no consumer and
    /// no entry, of a topic without a schema, it becomes the topic's; otherwise the topic's latest schema must have
    /// its type.
    fn check_schema(&mut self, topic: &str, segment: u64, schema: Option<&Schema>) -> Option<ServerError> {
        let schema = schema?;
        let topic = self.topic_mut(topic);
        let seg = &topic.segments[&segment];
        let idle = seg.producers.is_empty()
            && seg.entries.is_empty()
            && seg.subscriptions.values().all(|subscription| subscription.consumers.is_empty());
        match topic.schemas.last() {
            None if idle => {
                topic.schemas.push(schema.clone());
                None
            }
            Some(latest) if latest.r#type == schema.r#type => None,
            _ => Some(ServerError::IncompatibleSchema),
        }
    }

    fn remove_producer(&mut self, conn: ConnId, producer_id: u64, topic: &str, segment: u64) {
        let seg = self.topics.get_mut(topic).unwrap().segments.get_mut(&segment).unwrap();
        seg.producers.retain(|(c, id, _)| (*c, *id) != (conn, producer_id));
        let alone = seg.producers.is_empty();
        seg.exclusive &= !alone;
        // The first wait-for-exclusive producer in line gets the topic, once nobody has it.
        let Some((c, id, request_id, name)) = (if alone { seg.waiting.pop_front() } else { None }) else { return };
        seg.exclusive = true;
        seg.topic_epoch += 1;
        let topic_epoch = seg.topic_epoch;
        seg.producers.push((c, id, name.clone()));
        self.conn(c).producers.insert(id, (topic.to_string(), segment));
        let mut success = producer_success(request_id, &name, -1, true);
        success.producer_success.get_or_insert_default().topic_epoch = Some(topic_epoch);
        self.push(c, success);
    }

    fn send(&mut self, conn: ConnId, command: &BaseCommand, frame: &Frame) {
        let send = &command.send;
        let Some((topic, segment)) = self.conns.get(&conn).and_then(|c| c.producers.get(&send.producer_id)).cloned()
        else {
            // A send for a producer the broker closed is ignored; for one it never had, the broker closes the
            // connection.
            if !self.conns.get(&conn).is_some_and(|c| c.closed_producers.contains(&send.producer_id)) {
                self.drop_conn(conn);
            }
            return;
        };
        let section = frame.payload.as_ref().expect("a send without a payload");
        assert_eq!(section.checksum_valid, Some(true), "a send with a bad checksum");
        let metadata = MessageMetadata::decode_from_slice(&section.metadata).expect("undecodable metadata");
        let seg = self.topics.get_mut(&topic).unwrap().segments.get_mut(&segment).unwrap();
        let refusal =
            if seg.terminated { Some(ServerError::TopicTerminatedError) } else { self.fail_sends.pop_front() };
        if let Some(error) = refusal {
            let error = CommandSendError {
                producer_id: send.producer_id,
                sequence_id: send.sequence_id,
                error,
                message: format!("the broker refused the send: {error:?}"),
                ..Default::default()
            };
            self.push(
                conn,
                BaseCommand { r#type: Type::SEND_ERROR.into(), send_error: error.into(), ..Default::default() },
            );
            return;
        }
        let name = metadata.producer_name.clone();
        let highest = send.highest_sequence_id.unwrap_or(send.sequence_id);
        let duplicate = self.dedup && seg.last_sequence.get(&name).is_some_and(|&last| highest as i64 <= last);
        let (ledger_id, entry_id) = if duplicate {
            (u64::MAX, u64::MAX) // -1, -1: how the broker acknowledges a duplicate
        } else {
            seg.last_sequence.insert(name, highest as i64);
            self.store(&topic, segment, send.sequence_id, metadata, section.payload.clone())
        };
        let receipt = CommandSendReceipt {
            producer_id: send.producer_id,
            sequence_id: send.sequence_id,
            highest_sequence_id: send.highest_sequence_id,
            message_id: MessageIdData { ledgerId: ledger_id, entryId: entry_id, ..Default::default() }.into(),
            ..Default::default()
        };
        let receipt =
            BaseCommand { r#type: Type::SEND_RECEIPT.into(), send_receipt: receipt.into(), ..Default::default() };
        if self.hold_receipts {
            self.held.push((conn, receipt));
        } else {
            self.push(conn, receipt);
        }
        // Once written, the entry goes to the subscriptions' consumers.
        self.dispatch(&topic, segment);
    }

    /// Appends an entry to a segment, and to the backlog of each of its subscriptions.
    fn store(
        &mut self,
        topic: &str,
        segment: u64,
        sequence_id: u64,
        metadata: MessageMetadata,
        payload: Bytes,
    ) -> (u64, u64) {
        let seg = self.topics.get_mut(topic).unwrap().segments.get_mut(&segment).unwrap();
        // The world doesn't decompress: a compressed entry shows no first message.
        let (key, value, single) = match metadata.compression() {
            CompressionType::NONE => first_message(&metadata, &payload),
            _ => (None, Bytes::new(), None),
        };
        let stored = Stored {
            segment,
            ledger_id: seg.ledger_id,
            entry_id: seg.entries.len() as u64,
            producer_name: metadata.producer_name.clone(),
            sequence_id,
            key,
            value,
            metadata,
            single,
            payload,
        };
        for sub in seg.subscriptions.values_mut() {
            sub.unacked.insert(stored.entry_id, batch_indexes(&stored));
        }
        let at = (stored.ledger_id, stored.entry_id);
        seg.entries.push(stored);
        at
    }

    fn subscribe(&mut self, conn: ConnId, command: &BaseCommand) {
        let subscribe = &command.subscribe;
        let request_id = subscribe.request_id;
        let found = self.find_segment(&subscribe.topic);
        // A subscription that isn't durable is a reader's.
        let reader = subscribe.durable == Some(false);
        let refusal = match &found {
            None => Some(ServerError::TopicNotFound),
            Some((topic, segment)) => self
                .check_schema(topic, *segment, subscribe.schema.as_option())
                .or_else(|| self.refusal(topic, *segment, subscribe)),
        }
        .or_else(|| match self.fail_subscribes_to.get_mut(&subscribe.topic).and_then(VecDeque::pop_front) {
            Some(error) => Some(error),
            None => self.fail_subscribes.pop_front(),
        });
        if let Some(error) = refusal {
            let error = reply::error(request_id, error, &format!("the broker refused the subscribe: {error:?}"));
            self.push(conn, error);
            return;
        }
        let (topic, segment) = found.expect("a segment, for a subscribe not refused");
        // The broker would close the one there and answer `ServiceNotReady`: not simulated.
        let id = subscribe.consumer_id;
        assert!(!self.conns[&conn].consumers.contains_key(&id), "consumer {id} is on connection {conn} already");
        let seg = self.topics.get_mut(&topic).unwrap().segments.get_mut(&segment).unwrap();
        // Where a new subscription starts. One that exists stays where it is.
        let initial = match subscribe.initialPosition() {
            InitialPosition::Latest => seg.entries.len(),
            InitialPosition::Earliest => 0,
        };
        // A reader starts at its start id, as the broker places a cursor that isn't durable: the earliest id at the
        // first entry; the id of a message at its entry, which goes out again, the reader leaving out what it read;
        // the latest id at the initial position.
        let start = match subscribe.start_message_id.as_option() {
            Some(id) if id.ledgerId == u64::MAX => 0,
            Some(id) if id.ledgerId == seg.ledger_id && id.entryId < seg.entries.len() as u64 => id.entryId as usize,
            _ => initial,
        };
        let entries = &seg.entries[start..];
        let sub = seg.subscriptions.entry(subscribe.subscription.clone()).or_insert_with(|| Subscription {
            reader,
            unacked: entries.iter().map(|e| (e.entry_id, batch_indexes(e))).collect(),
            ..Default::default()
        });
        sub.join(subscribe);
        sub.consumers.push(Consumer {
            conn,
            id,
            name: subscribe.consumer_name.clone().unwrap_or_default(),
            priority: subscribe.priority_level.unwrap_or(0),
            permits: 0,
            pending: BTreeSet::new(),
            ended: false,
        });
        let place = (topic.clone(), segment, subscribe.subscription.clone());
        self.conn(conn).consumers.insert(id, place);
        self.push(conn, reply::success(request_id));
        // A consumer that joins a subscription at the end of a terminated segment learns it right away.
        self.dispatch(&topic, segment);
    }

    fn flow(&mut self, conn: ConnId, command: &BaseCommand) {
        let flow = &command.flow;
        let Some((topic, segment, name)) = self.consumer(conn, flow.consumer_id) else { return };
        let sub = self.subscription(&topic, segment, &name);
        let at = sub.position(conn, flow.consumer_id);
        sub.consumers[at].permits += i64::from(flow.messagePermits);
        self.dispatch(&topic, segment);
    }

    fn ack(&mut self, conn: ConnId, command: &BaseCommand) {
        let ack = &command.ack;
        if ack.ack_type == AckType::Cumulative {
            return self.ack_cumulative(conn, ack);
        }
        let Some((topic, segment, name)) = self.consumer(conn, ack.consumer_id) else { return };
        let ledger = self.topic(&topic).segments[&segment].ledger_id;
        let sub = self.subscription(&topic, segment, &name);
        for id in ack.message_id.iter().filter(|id| id.ledgerId == ledger) {
            // A corrupt entry is acked whole: the client drops all of it.
            let ack_set = (ack.validation_error.is_none() && !id.ack_set.is_empty()).then_some(&id.ack_set[..]);
            sub.ack(id.entryId, ack_set);
        }
        self.dispatch(&topic, segment);
    }

    fn redeliver(&mut self, conn: ConnId, command: &BaseCommand) {
        let redeliver = &command.redeliverUnacknowledgedMessages;
        let Some((topic, segment, name)) = self.consumer(conn, redeliver.consumer_id) else { return };
        let ledger = self.topic(&topic).segments[&segment].ledger_id;
        let sub = self.subscription(&topic, segment, &name);
        let at = sub.position(conn, redeliver.consumer_id);
        let pending = &mut sub.consumers[at].pending;
        // No ids: everything pending on the consumer. Ids not pending on it are ignored, as the broker does.
        let entries: Vec<u64> = if redeliver.message_ids.is_empty() {
            std::mem::take(pending).into_iter().collect()
        } else {
            let ids = redeliver.message_ids.iter().filter(|id| id.ledgerId == ledger);
            ids.map(|id| id.entryId).filter(|entry| pending.remove(entry)).collect()
        };
        // Asked for again: one redelivery more each.
        for entry in &entries {
            *sub.redeliveries.entry(*entry).or_default() += 1;
        }
        sub.redeliver.extend(entries);
        self.dispatch(&topic, segment);
    }

    fn close_consumer(&mut self, conn: ConnId, command: &BaseCommand) {
        let close = &command.close_consumer;
        // Closing a consumer the broker is still creating discards it: its subscribe gets no answer.
        self.held_subscribes.retain(|(c, held)| (*c, held.subscribe.consumer_id) != (conn, close.consumer_id));
        // A consumer the broker doesn't know is closed already.
        self.push(conn, reply::success(close.request_id));
        let Some(place) = self.conn(conn).consumers.remove(&close.consumer_id) else { return };
        self.remove_consumer(conn, close.consumer_id, &place);
        self.dispatch(&place.0, place.1);
    }

    /// Takes a consumer off its subscription: the entries it had pending wait to go out again. As in the broker,
    /// that is no redelivery to count, and the counts go once the last consumer goes.
    fn remove_consumer(&mut self, conn: ConnId, id: u64, (topic, segment, name): &(String, u64, String)) {
        let sub = self.subscription(topic, *segment, name);
        let consumer = sub.consumers.remove(sub.position(conn, id));
        sub.redeliver.extend(consumer.pending);
        if sub.consumers.is_empty() {
            sub.redeliveries.clear();
            // A reader's subscription goes with its consumer.
            if sub.reader {
                let seg = self.topics.get_mut(topic).unwrap().segments.get_mut(segment).unwrap();
                seg.subscriptions.remove(name);
            }
        }
    }

    /// Where a consumer of the connection is: `(topic, segment, subscription)`.
    fn consumer(&self, conn: ConnId, id: u64) -> Option<(String, u64, String)> {
        self.conns[&conn].consumers.get(&id).cloned()
    }

    fn subscription(&mut self, topic: &str, segment: u64, name: &str) -> &mut Subscription {
        let seg = self.topics.get_mut(topic).unwrap().segments.get_mut(&segment).unwrap();
        seg.subscriptions.get_mut(name).expect("the subscription of a consumer")
    }

    /// Dispatches what each subscription of the segment has for its consumers. One that acked everything of a
    /// terminated segment tells each of its consumers, once, that the topic ended.
    fn dispatch(&mut self, topic: &str, segment: u64) {
        let World { topics, conns, .. } = self;
        let seg = topics.get_mut(topic).unwrap().segments.get_mut(&segment).unwrap();
        for sub in seg.subscriptions.values_mut() {
            while let Some((at, entry)) = sub.next(&seg.entries) {
                let stored = &seg.entries[entry as usize];
                let message = sub.deliver(at, stored);
                let out = &mut conns.get_mut(&sub.consumers[at].conn).expect("a consumer's connection").out;
                encode_payload_command(&message, &stored.metadata, &stored.payload, out).unwrap();
            }
            let ended = seg.terminated && sub.unacked.is_empty();
            for consumer in sub.consumers.iter_mut().filter(|c| ended && !c.ended) {
                consumer.ended = true;
                let out = &mut conns.get_mut(&consumer.conn).expect("a consumer's connection").out;
                encode_command(&reached_end_of_topic(consumer.id), out).unwrap();
            }
        }
        self.check_drains(topic);
    }
}

impl Subscription {
    /// The next entry to dispatch and the consumer it goes to, by its position: the first entry to redeliver, else
    /// the first one not read yet, to the next consumer with permits on the highest priority level, round robin.
    fn next_delivery(&mut self) -> Option<(usize, u64)> {
        let unread = self.unacked.range(self.read..).next().map(|(entry, _)| entry);
        let entry = *self.redeliver.first().or(unread)?;
        let count = self.consumers.len();
        let ready = (0..count).map(|i| (self.next + i) % count).filter(|&i| self.consumers[i].permits > 0);
        // The first of the lowest level, from where the round robin is.
        let at = ready.min_by_key(|&i| self.consumers[i].priority)?;
        self.next = at + 1;
        Some((at, entry))
    }

    /// Gives an entry to the consumer at `at`, and returns the message that sends it. The entry stays pending on the
    /// consumer until acked, and costs it the messages not acked in it: a consumer with any permit gets a whole
    /// batch, as the broker's dispatcher gives each consumer it picks at least one entry.
    fn deliver(&mut self, at: usize, stored: &Stored) -> BaseCommand {
        let entry = stored.entry_id;
        let unacked = &self.unacked[&entry];
        let consumer = &mut self.consumers[at];
        consumer.permits -= unacked.len() as i64;
        consumer.pending.insert(entry);
        self.redeliver.remove(&entry);
        self.read = self.read.max(entry + 1);
        // Only a batch with indexes acked says which ones are left.
        let partial = unacked.len() < stored.metadata.num_messages_in_batch() as usize;
        let message = CommandMessage {
            consumer_id: consumer.id,
            message_id: MessageIdData { ledgerId: stored.ledger_id, entryId: entry, ..Default::default() }.into(),
            redelivery_count: self.redeliveries.get(&entry).copied(),
            ack_set: if partial { bitset(unacked) } else { Vec::new() },
            ..Default::default()
        };
        BaseCommand { r#type: Type::MESSAGE.into(), message: message.into(), ..Default::default() }
    }

    /// Acks the batch indexes whose bit `ack_set` clears, or the whole entry without one. An entry with every
    /// index acked leaves the consumer it was pending on, and is never dispatched again.
    fn ack(&mut self, entry: u64, ack_set: Option<&[i64]>) {
        // One acked already stays acked, as the broker's cursor skips a deleted position.
        let Some(unacked) = self.unacked.get_mut(&entry) else { return };
        unacked.retain(|&index| ack_set.is_some_and(|words| is_set(words, index)));
        if unacked.is_empty() {
            self.unacked.remove(&entry);
            self.redeliver.remove(&entry);
            for consumer in &mut self.consumers {
                consumer.pending.remove(&entry);
            }
        }
    }

    /// Where a consumer is in [`Subscription::consumers`].
    fn position(&self, conn: ConnId, id: u64) -> usize {
        self.consumers.iter().position(|c| (c.conn, c.id) == (conn, id)).expect("a consumer of the subscription")
    }
}

impl Segment {
    fn new(info: SegmentInfoProto, name: String, ledger_id: u64) -> Self {
        Self {
            info,
            name,
            ledger_id,
            entries: Vec::new(),
            terminated: false,
            pruned: false,
            deleted: false,
            producers: Vec::new(),
            exclusive: false,
            waiting: VecDeque::new(),
            topic_epoch: 0,
            last_sequence: HashMap::new(),
            subscriptions: BTreeMap::new(),
        }
    }
}

/// Whether two schemas are the same version, as the broker compares them: by type and definition.
fn same_schema(a: &Schema, b: &Schema) -> bool {
    a.r#type == b.r#type && a.schema_data == b.schema_data
}

fn segment_name(parent: &str, start: u32, end: u32, id: u64) -> String {
    format!("segment://{parent}/{start:04x}-{end:04x}-{id}")
}

fn layout_update(session_id: u64, topic: &str, dag: ScalableTopicDAG) -> BaseCommand {
    let update = CommandScalableTopicUpdate {
        session_id,
        dag: dag.into(),
        resolved_topic_name: Some(topic.to_string()),
        ..Default::default()
    };
    BaseCommand { r#type: Type::SCALABLE_TOPIC_UPDATE.into(), scalableTopicUpdate: update.into(), ..Default::default() }
}

fn producer_success(request_id: u64, name: &str, last_sequence_id: i64, ready: bool) -> BaseCommand {
    let success = CommandProducerSuccess {
        request_id,
        producer_name: name.to_string(),
        last_sequence_id: Some(last_sequence_id),
        producer_ready: Some(ready),
        ..Default::default()
    };
    BaseCommand { r#type: Type::PRODUCER_SUCCESS.into(), producer_success: success.into(), ..Default::default() }
}

fn close_producer(producer_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::CLOSE_PRODUCER.into(),
        close_producer: CommandCloseProducer { producer_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

/// The broker closing a consumer.
fn consumer_closed(consumer_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::CLOSE_CONSUMER.into(),
        close_consumer: CommandCloseConsumer { consumer_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

fn reached_end_of_topic(consumer_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::REACHED_END_OF_TOPIC.into(),
        reachedEndOfTopic: CommandReachedEndOfTopic { consumer_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

/// The batch indexes of an entry: one for an entry that isn't a batch.
fn batch_indexes(stored: &Stored) -> BTreeSet<u32> {
    (0..stored.metadata.num_messages_in_batch() as u32).collect()
}

/// Batch indexes as Java's `BitSet.toLongArray`, the layout of an `ack_set`: index i is bit i % 64 of word i / 64,
/// up to the last word with a bit set.
fn bitset(indexes: &BTreeSet<u32>) -> Vec<i64> {
    let mut words = vec![0; indexes.last().map_or(0, |&last| last as usize / 64 + 1)];
    for &index in indexes {
        words[index as usize / 64] |= 1 << (index % 64);
    }
    words
}

/// Whether an `ack_set` has the bit of a batch index set. The words it leaves out are zeros.
fn is_set(ack_set: &[i64], index: u32) -> bool {
    ack_set.get(index as usize / 64).is_some_and(|&word| (word >> (index % 64)) & 1 == 1)
}

/// The key and payload of an entry's first message: a batch's first message, or the entry itself.
fn first_message(
    metadata: &MessageMetadata,
    payload: &Bytes,
) -> (Option<String>, Bytes, Option<SingleMessageMetadata>) {
    if metadata.num_messages_in_batch.is_none() {
        return (metadata.partition_key.clone(), payload.clone(), None);
    }
    let mut batch = payload.clone();
    let size = batch.get_u32() as usize;
    let single = SingleMessageMetadata::decode_from_slice(&batch[..size]).expect("undecodable message metadata");
    batch.advance(size);
    let value = batch.slice(..single.payload_size as usize);
    (single.partition_key.clone(), value, Some(single))
}
