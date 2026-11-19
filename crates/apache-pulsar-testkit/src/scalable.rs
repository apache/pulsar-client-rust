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

//! A simulated cluster for scalable topics: topics and their layouts, segments, and the producers on them, over
//! any number of connections.
//!
//! [`World`] is sans IO like the rest of the testkit: a test, or a socket server, feeds each connection's bytes to
//! [`World::input`] and writes out what [`World::output`] returns. Admin operations ([`World::split`],
//! [`World::merge`], [`World::terminate`]) behave like the 5.0.1 broker's: sealing a segment terminates its topic,
//! which disconnects its producers with `CommandCloseProducer`, then the new layout goes to every watch.

use std::collections::{BTreeMap, HashMap, VecDeque};

use apache_pulsar_protocol::frame::{Frame, FrameDecoder, encode_command};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseProducer, CommandProducerSuccess, CommandScalableTopicUpdate, CommandSendError,
    CommandSendReceipt, FeatureFlags, MessageIdData, MessageMetadata, ProducerAccessMode, ScalableTopicDAG,
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
    /// The key of the entry's first message.
    pub key: Option<String>,
    /// The payload of the entry's first message.
    pub value: Bytes,
    pub metadata: MessageMetadata,
    /// The first message's own metadata, in a batch.
    pub single: Option<SingleMessageMetadata>,
}

#[derive(Debug)]
struct Segment {
    info: SegmentInfoProto,
    /// The topic its producers attach to.
    name: String,
    ledger_id: u64,
    entries: Vec<Stored>,
    terminated: bool,
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

#[derive(Debug)]
struct Topic {
    epoch: u64,
    segments: BTreeMap<u64, Segment>,
    next_segment: u64,
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
    /// While set, scalable-topic lookups get no answer.
    pub silent_lookups: bool,
    /// Every command received, per connection.
    pub log: Vec<(ConnId, BaseCommand)>,
    topics: BTreeMap<String, Topic>,
    conns: BTreeMap<ConnId, Conn>,
    held: Vec<(ConnId, BaseCommand)>,
    held_sends: Vec<(ConnId, Frame)>,
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
            silent_lookups: false,
            log: Vec::new(),
            topics: BTreeMap::new(),
            conns: BTreeMap::new(),
            held: Vec::new(),
            held_sends: Vec::new(),
            next_conn: 0,
            next_ledger: 100,
            next_name: 0,
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
            let frame = match c.decoder.decode(&mut c.input) {
                Ok(Some(frame)) => frame,
                Ok(None) => return,
                Err(error) => panic!("the client sent a bad frame: {error}"),
            };
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
        let mut topic_state = Topic { epoch: 0, segments: BTreeMap::new(), next_segment: 0 };
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
        let mut topic_state = Topic { epoch: 0, segments: BTreeMap::new(), next_segment: 0 };
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
            if let Some(c) = self.conns.get_mut(&conn) {
                c.producers.remove(&producer_id);
                c.closed_producers.insert(producer_id);
            }
            self.push(conn, close_producer(producer_id));
        }
    }

    /// Processes the sends held while [`World::hold_sends`] was set, in order.
    pub fn release_sends(&mut self) {
        for (conn, frame) in std::mem::take(&mut self.held_sends) {
            if self.is_open(conn) {
                let command = BaseCommand::decode_from_slice(&frame.command).unwrap();
                self.send(conn, &command, &frame);
            }
        }
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

    /// Pushes the topic's layout again to every watch, with the epoch it already has.
    pub fn repush_layout(&mut self, topic: &str) {
        self.push_layout(topic);
    }

    /// Sends the receipts held while [`World::hold_receipts`] was set.
    pub fn release_receipts(&mut self) {
        for (conn, receipt) in std::mem::take(&mut self.held) {
            self.push(conn, receipt);
        }
    }

    fn topic(&self, topic: &str) -> &Topic {
        self.topics.get(topic).unwrap_or_else(|| panic!("no topic {topic}"))
    }

    fn topic_mut(&mut self, topic: &str) -> &mut Topic {
        self.topics.get_mut(topic).unwrap_or_else(|| panic!("no topic {topic}"))
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
        self.push_layout(topic);
        ids
    }

    fn push_layout(&mut self, topic: &str) {
        let dag = self.dag(topic);
        let watchers: Vec<(ConnId, u64)> = self
            .conns
            .iter()
            .filter(|(_, c)| c.open)
            .flat_map(|(id, c)| c.sessions.iter().filter(|(_, t)| *t == topic).map(move |(s, _)| (*id, *s)))
            .collect();
        for (conn, session_id) in watchers {
            self.push(conn, layout_update(session_id, topic, dag.clone()));
        }
    }

    fn dag(&self, topic: &str) -> ScalableTopicDAG {
        let state = self.topic(topic);
        ScalableTopicDAG {
            epoch: state.epoch,
            segments: state.segments.values().map(|s| s.info.clone()).collect(),
            ..Default::default()
        }
    }

    fn drop_conn(&mut self, conn: ConnId) {
        let Some(c) = self.conns.get_mut(&conn) else { return };
        c.open = false;
        let producers: Vec<(u64, (String, u64))> = c.producers.drain().collect();
        c.sessions.clear();
        for (producer_id, (topic, segment)) in producers {
            self.remove_producer(conn, producer_id, &topic, segment);
        }
        for topic in self.topics.values_mut() {
            for segment in topic.segments.values_mut() {
                segment.waiting.retain(|(c, ..)| *c != conn);
            }
        }
    }

    fn push(&mut self, conn: ConnId, command: BaseCommand) {
        if let Some(c) = self.conns.get_mut(&conn).filter(|c| c.open) {
            encode_command(&command, &mut c.out).unwrap();
        }
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
            Type::LOOKUP => {
                if self.lookup != LookupAnswer::Silent {
                    let answer = reply::lookup(command.lookupTopic.request_id, &self.lookup);
                    self.push(conn, answer);
                }
            }
            Type::SCALABLE_TOPIC_LOOKUP if self.silent_lookups => {}
            Type::SCALABLE_TOPIC_LOOKUP => self.scalable_lookup(conn, &command),
            Type::SCALABLE_TOPIC_CLOSE => {
                if let Some(c) = self.conns.get_mut(&conn) {
                    c.sessions.remove(&command.scalableTopicClose.session_id);
                }
            }
            Type::PRODUCER => self.producer(conn, &command),
            Type::SEND if self.hold_sends => self.held_sends.push((conn, frame)),
            Type::SEND => self.send(conn, &command, &frame),
            Type::CLOSE_PRODUCER => {
                // The broker answers once the producer's pending writes are done.
                self.release_receipts();
                let close = &command.close_producer;
                let segment = self.conns.get_mut(&conn).and_then(|c| {
                    c.closed_producers.insert(close.producer_id);
                    c.producers.remove(&close.producer_id)
                });
                if let Some((topic, segment)) = segment {
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
            _ => {}
        }
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
        if let Some(c) = self.conns.get_mut(&conn) {
            c.sessions.insert(lookup.session_id, topic.clone());
        }
        let update = layout_update(lookup.session_id, &topic, self.dag(&topic));
        self.push(conn, update);
    }

    fn find_segment(&self, name: &str) -> Option<(String, u64)> {
        self.topics.iter().find_map(|(topic, state)| {
            state.segments.values().find(|s| s.name == name).map(|s| (topic.clone(), s.info.segment_id))
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
                    if let Some(cs) = self.conns.get_mut(&c) {
                        cs.producers.remove(&id);
                        cs.closed_producers.insert(id);
                    }
                    self.push(c, close_producer(id));
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
        if let Some(c) = self.conns.get_mut(&conn) {
            c.producers.insert(producer.producer_id, (topic, segment));
        }
        let mut success = producer_success(request_id, &name, last, true);
        success.producer_success.get_or_insert_default().topic_epoch = topic_epoch;
        self.push(conn, success);
    }

    fn remove_producer(&mut self, conn: ConnId, producer_id: u64, topic: &str, segment: u64) {
        let Some(seg) = self.topics.get_mut(topic).and_then(|t| t.segments.get_mut(&segment)) else { return };
        seg.producers.retain(|(c, id, _)| (*c, *id) != (conn, producer_id));
        if seg.producers.is_empty() {
            seg.exclusive = false;
            // The first wait-for-exclusive producer in line gets the topic.
            if let Some((c, id, request_id, name)) = seg.waiting.pop_front() {
                seg.exclusive = true;
                seg.topic_epoch += 1;
                let topic_epoch = seg.topic_epoch;
                seg.producers.push((c, id, name.clone()));
                if let Some(cs) = self.conns.get_mut(&c) {
                    cs.producers.insert(id, (topic.to_string(), segment));
                }
                let mut success = producer_success(request_id, &name, -1, true);
                success.producer_success.get_or_insert_default().topic_epoch = Some(topic_epoch);
                self.push(c, success);
            }
        }
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
            let (key, value, single) = first_message(&metadata, &section.payload);
            let entry_id = seg.entries.len() as u64;
            seg.entries.push(Stored {
                segment,
                ledger_id: seg.ledger_id,
                entry_id,
                producer_name: name.clone(),
                sequence_id: send.sequence_id,
                key,
                value,
                metadata,
                single,
            });
            seg.last_sequence.insert(name, highest as i64);
            (seg.ledger_id, entry_id)
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
            producers: Vec::new(),
            exclusive: false,
            waiting: VecDeque::new(),
            topic_epoch: 0,
            last_sequence: HashMap::new(),
        }
    }
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
