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

//! Drives the sans-IO [`Producer`] against a simulated [`World`], doing what the driver does for each
//! [`Action`], on a clock the test moves by hand. Deterministic: no sockets, no sleeps.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

use apache_pulsar_protocol::frame::{FrameDecoder, encode_command, encode_payload_command};
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::message_id::MessageId;
use apache_pulsar_protocol::producer::{
    Action, OutgoingMessage, Producer, ProducerOptions, RegisterError, SendError, SendId,
};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseProducer, CommandConnect, CommandScalableTopicLookup, CompressionType, ProducerAccessMode,
};
use buffa::{EnumValue, Message};
use bytes::BytesMut;

use crate::Clock;
use crate::scalable::{ConnId, World};

/// The wall clock the harness stamps messages with.
const PUBLISH_TIME: u64 = 1_700_000_000_000;

/// See the module documentation.
#[derive(Debug)]
pub struct Harness {
    pub world: World,
    pub producer: Producer,
    pub clock: Clock,
    topic: String,
    conn: ConnId,
    session_id: u64,
    next_id: u64,
    /// Registered producers: producer id to `(segment, generation)`.
    producers: HashMap<u64, (u64, u64)>,
    /// Registrations waiting for an answer: request id to `(segment, generation, producer id)`.
    registering: HashMap<u64, (u64, u64, u64)>,
    /// Closes waiting for an answer: request id to `(segment, generation, producer id)`. The producer's receipts
    /// keep coming until the answer.
    closing: HashMap<u64, (u64, u64, u64)>,
    /// Finished sends.
    pub done: BTreeMap<SendId, Result<MessageId, SendError>>,
    pub created: Option<Result<(), SendError>>,
    pub closed: bool,
    /// How many `CommandProducer` the harness sent, per segment.
    pub registrations: BTreeMap<u64, usize>,
}

/// Options for a test producer: shared, named `p`, no send timeout, no compression, and each message in an entry of
/// its own, so that a send goes out without the clock moving.
pub fn options() -> ProducerOptions {
    ProducerOptions {
        name: Some("p".into()),
        access_mode: ProducerAccessMode::Shared,
        send_timeout: None,
        initial_sequence_id: None,
        properties: Vec::new(),
        schema: None,
        operation_timeout: Duration::from_secs(30),
        batching: None,
        compression: CompressionType::NONE,
    }
}

impl Harness {
    /// Opens a watch on `topic` in `world`, and creates a producer on its first layout.
    pub fn new(mut world: World, topic: &str, options: ProducerOptions) -> Harness {
        let conn = world.connect();
        let clock = Clock::default();
        let mut harness = Harness {
            world,
            // Replaced once the first layout is in.
            producer: Producer::new(options.clone(), Rc::new(empty_layout()), clock.now()),
            clock,
            topic: topic.to_string(),
            conn,
            session_id: 0,
            next_id: 1,
            producers: HashMap::new(),
            registering: HashMap::new(),
            closing: HashMap::new(),
            done: BTreeMap::new(),
            created: None,
            closed: false,
            registrations: BTreeMap::new(),
        };
        harness.handshake();
        let layout = harness.first_layout();
        harness.producer = Producer::new(options, Rc::new(layout), harness.clock.now());
        harness.pump();
        harness
    }

    /// Sends a message; its value is the payload.
    pub fn send(&mut self, key: Option<&str>, value: &str) -> SendId {
        let message = OutgoingMessage {
            key: key.map(str::to_string),
            payload: bytes::Bytes::copy_from_slice(value.as_bytes()),
            ..Default::default()
        };
        self.send_message(message)
    }

    pub fn send_message(&mut self, message: OutgoingMessage) -> SendId {
        let id = self.producer.send(message, PUBLISH_TIME, self.clock.now()).expect("the producer refused a send");
        self.pump();
        id
    }

    /// Sends the open batches at once.
    pub fn flush(&mut self) {
        self.producer.flush();
        self.pump();
    }

    /// Moves the clock, lets the producer act on it, and pumps.
    pub fn advance(&mut self, by: Duration) {
        self.clock.advance(by);
        // Every deadline that passes within `by` fires, in order.
        while let Some(at) = self.producer.poll_timeout() {
            if at > self.clock.now() {
                break;
            }
            self.producer.handle_timeout(at.max(self.clock.now()));
            self.pump();
        }
        self.producer.handle_timeout(self.clock.now());
        self.pump();
    }

    /// Advances the clock in small steps until `until` holds, for up to `limit`.
    pub fn run_until(&mut self, limit: Duration, until: impl Fn(&Harness) -> bool) {
        let step = Duration::from_millis(10);
        let mut elapsed = Duration::ZERO;
        while !until(self) {
            assert!(elapsed < limit, "condition not met within {limit:?}");
            self.advance(step);
            elapsed += step;
        }
    }

    pub fn close(&mut self) {
        self.producer.close(self.clock.now());
        self.pump();
    }

    /// The result of a send, once finished.
    pub fn result(&self, send: SendId) -> Option<&Result<MessageId, SendError>> {
        self.done.get(&send)
    }

    /// The segment a successful send landed on.
    pub fn segment_of(&self, send: SendId) -> u64 {
        self.done[&send].as_ref().expect("the send failed").segment_id as u64
    }

    /// Drops the connection, as a network failure would: the producer's registrations are lost, and the watch
    /// reopens on a new connection.
    pub fn drop_connection(&mut self) {
        self.world.kill(self.conn);
        self.pump();
    }

    /// Does what the producer and the world ask until neither has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(action) = self.producer.poll_action() {
                progressed = true;
                self.act(action);
            }
            let output = self.world.output(self.conn);
            if !output.is_empty() {
                progressed = true;
                self.answer(&output);
            }
            // The world closed the connection, or the test did: connect again.
            if !self.world.is_open(self.conn) {
                progressed = true;
                self.reconnect();
            }
            if !progressed {
                return;
            }
        }
    }

    fn handshake(&mut self) {
        let connect = BaseCommand {
            r#type: Type::CONNECT.into(),
            connect: CommandConnect {
                client_version: "testkit".into(),
                protocol_version: Some(21),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        self.write(&connect);
        let _connected = self.world.output(self.conn);
    }

    fn open_session(&mut self) {
        self.session_id += 1;
        let lookup = BaseCommand {
            r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
            scalableTopicLookup: CommandScalableTopicLookup {
                session_id: self.session_id,
                topic: self.topic.clone(),
                create_if_missing: Some(true),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        self.write(&lookup);
    }

    fn first_layout(&mut self) -> Layout {
        self.open_session();
        let output = self.world.output(self.conn);
        let command = decode(&output).into_iter().next().expect("no answer to the scalable lookup");
        let update = &command.scalableTopicUpdate;
        assert!(update.error.is_none(), "lookup failed: {:?} {:?}", update.error, update.message);
        Layout::from_dag(&update.dag, update.resolved_topic_name.as_deref().unwrap_or(&self.topic)).unwrap()
    }

    fn reconnect(&mut self) {
        self.world.disconnected(self.conn);
        self.conn = self.world.connect();
        self.handshake();
        // What was registered on the old connection, or being closed there, is gone with it.
        self.closing.clear();
        let now = self.clock.now();
        for (_, (segment, generation)) in std::mem::take(&mut self.producers) {
            self.producer.on_disconnected(segment, generation, now);
        }
        for (_, (segment, generation, _)) in std::mem::take(&mut self.registering) {
            let lost = RegisterError::Unavailable("disconnected".into());
            self.producer.on_register_failed(segment, generation, lost, now);
        }
        self.open_session();
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Connect { segment, generation, .. } => {
                let producer_id = self.next_id;
                let request_id = self.next_id + 1;
                self.next_id += 2;
                let mut command = self.producer.register_command(segment, producer_id).expect("no such segment");
                command.producer.get_or_insert_default().request_id = request_id;
                self.registering.insert(request_id, (segment, generation, producer_id));
                *self.registrations.entry(segment).or_default() += 1;
                self.write(&command);
            }
            Action::Send { command, metadata, payload, .. } => {
                let mut buf = BytesMut::new();
                encode_payload_command(&command, &metadata, &payload, &mut buf).unwrap();
                self.world.input(self.conn, &buf);
            }
            Action::CloseProducer { segment, generation } => {
                // Without a producer, its connection is gone, and the producer heard it.
                let registered = self.producers.iter().find(|(_, v)| **v == (segment, generation));
                let Some((&producer_id, _)) = registered else { return };
                let request_id = self.next_id;
                self.next_id += 1;
                self.closing.insert(request_id, (segment, generation, producer_id));
                let close = BaseCommand {
                    r#type: Type::CLOSE_PRODUCER.into(),
                    close_producer: CommandCloseProducer { producer_id, request_id, ..Default::default() }.into(),
                    ..Default::default()
                };
                self.write(&close);
            }
            // The pump connects again.
            Action::Disconnect { .. } => self.world.kill(self.conn),
            Action::Done { send, result } => {
                assert!(self.done.insert(send, result).is_none(), "send {send} finished twice");
            }
            Action::Created(result) => self.created = Some(result),
            Action::Closed => self.closed = true,
        }
    }

    fn answer(&mut self, output: &[u8]) {
        let now = self.clock.now();
        for command in decode(output) {
            let EnumValue::Known(kind) = command.r#type else { continue };
            // What the world sends on a connection only ever answers what the harness wrote on it, or concerns a
            // producer still registered there.
            match kind {
                Type::PRODUCER_SUCCESS => {
                    let success = &command.producer_success;
                    // A wait-for-exclusive producer in line: the answer that counts comes once it has the topic.
                    if success.producer_ready == Some(false) {
                        continue;
                    }
                    let (segment, generation, producer_id) = self.registering.remove(&success.request_id).unwrap();
                    self.producers.insert(producer_id, (segment, generation));
                    self.producer.on_registered(segment, generation, producer_id, success, 5 << 20, now);
                }
                // The world refuses registrations only: it closes every producer it is asked to.
                Type::ERROR => {
                    let error = &command.error;
                    let (segment, generation, _) = self.registering.remove(&error.request_id).unwrap();
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    self.producer.on_register_failed(segment, generation, refused, now);
                }
                Type::SUCCESS => {
                    let (segment, generation, producer_id) = self.closing.remove(&command.success.request_id).unwrap();
                    self.producers.remove(&producer_id);
                    self.producer.on_producer_closed(segment, generation, now);
                }
                Type::SEND_RECEIPT => {
                    let receipt = &command.send_receipt;
                    let (segment, generation) = self.producers[&receipt.producer_id];
                    self.producer.on_receipt(segment, generation, receipt, now);
                }
                Type::SEND_ERROR => {
                    let error = &command.send_error;
                    let (segment, generation) = self.producers[&error.producer_id];
                    self.producer.on_send_error(segment, generation, error, now);
                }
                Type::CLOSE_PRODUCER => {
                    let (segment, generation) = self.producers.remove(&command.close_producer.producer_id).unwrap();
                    self.producer.on_disconnected(segment, generation, now);
                }
                // The only other command the world sends: a layout, on the session open now.
                _ => {
                    let update = &command.scalableTopicUpdate;
                    assert_eq!((update.session_id, update.error), (self.session_id, None), "{command:?}");
                    let topic = update.resolved_topic_name.as_deref().unwrap_or(&self.topic);
                    let layout = Layout::from_dag(&update.dag, topic).unwrap();
                    self.producer.on_layout(Rc::new(layout), now);
                }
            }
        }
    }

    fn write(&mut self, command: &BaseCommand) {
        let mut buf = BytesMut::new();
        encode_command(command, &mut buf).unwrap();
        self.world.input(self.conn, &buf);
    }
}

pub(crate) fn decode(bytes: &[u8]) -> Vec<BaseCommand> {
    let decoder = FrameDecoder::default();
    let mut buf = BytesMut::from(bytes);
    let mut out = Vec::new();
    while let Some(frame) = decoder.decode(&mut buf).expect("the world sent a bad frame") {
        out.push(BaseCommand::decode_from_slice(&frame.command).expect("undecodable command"));
    }
    out
}

pub(crate) fn empty_layout() -> Layout {
    let dag = apache_pulsar_protocol::proto::ScalableTopicDAG::default();
    Layout::from_dag(&dag, "topic://harness/placeholder/x").unwrap()
}
