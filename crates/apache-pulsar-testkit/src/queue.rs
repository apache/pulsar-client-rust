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

//! Drives a [`QueueConsumer`] against a simulated [`World`], doing what the driver does for each [`Action`], on a
//! clock the test moves by hand. One connection carries the topic's layout watch and every segment's consumer.
//! Deterministic: no sockets, no sleeps.

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::message_id::MessageId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandCloseConsumer, CommandConnect, CommandScalableTopicLookup};
use apache_pulsar_protocol::queue_consumer::{Action, QueueConsumer};
use buffa::{EnumValue, Message};
use bytes::BytesMut;

use crate::Clock;
use crate::producer::{decode, empty_layout};
use crate::scalable::{ConnId, World};

/// See the module documentation.
#[derive(Debug)]
pub struct QueueHarness {
    pub world: World,
    pub queue: QueueConsumer,
    pub clock: Clock,
    topic: String,
    conn: ConnId,
    session_id: u64,
    next_id: u64,
    /// Subscribes waiting for an answer: request id to `(segment, generation, consumer id)`.
    subscribing: HashMap<u64, (u64, u64, u64)>,
    /// Registered consumers, until the answer to their close: consumer id to `(segment, generation)`.
    registered: HashMap<u64, (u64, u64)>,
    /// Closes waiting for an answer: request id to consumer id.
    closing: HashMap<u64, u64>,
    /// How the subscribe went.
    pub subscribed: Option<Result<(), RegisterError>>,
    pub closed: bool,
    /// How many `CommandSubscribe` the harness sent, per segment.
    pub subscribes: BTreeMap<u64, usize>,
}

impl QueueHarness {
    /// Opens a watch on `topic` in `world`, and subscribes a Queue consumer on its first layout.
    pub fn new(mut world: World, topic: &str, options: ConsumerOptions) -> QueueHarness {
        let conn = world.connect();
        let clock = Clock::default();
        let mut harness = QueueHarness {
            world,
            // Replaced once the first layout is in.
            queue: QueueConsumer::new(options.clone(), Rc::new(empty_layout()), clock.now()),
            clock,
            topic: topic.to_string(),
            conn,
            session_id: 0,
            next_id: 1,
            subscribing: HashMap::new(),
            registered: HashMap::new(),
            closing: HashMap::new(),
            subscribed: None,
            closed: false,
            subscribes: BTreeMap::new(),
        };
        harness.handshake();
        harness.open_session();
        let command = decode(&harness.world.output(harness.conn)).pop().expect("no answer to the scalable lookup");
        let update = &command.scalableTopicUpdate;
        assert!(update.error.is_none(), "lookup failed: {:?} {:?}", update.error, update.message);
        let layout =
            Layout::from_dag(&update.dag, update.resolved_topic_name.as_deref().unwrap_or(&harness.topic)).unwrap();
        harness.queue = QueueConsumer::new(options, Rc::new(layout), harness.clock.now());
        harness.pump();
        harness
    }

    /// Every message in, as the application takes them, after what the world has for the consumer.
    pub fn take(&mut self) -> Vec<ReceivedMessage> {
        self.pump();
        let taken = std::iter::from_fn(|| self.queue.next_message()).collect();
        self.pump();
        taken
    }

    /// The values of every message in, as text.
    pub fn take_values(&mut self) -> Vec<String> {
        self.take().iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
    }

    pub fn ack(&mut self, id: &MessageId) {
        self.queue.acknowledge(id, self.clock.now());
        self.pump();
    }

    pub fn nack(&mut self, id: &MessageId) {
        self.queue.negative_acknowledge(id, self.clock.now());
        self.pump();
    }

    pub fn close(&mut self) {
        self.queue.close();
        self.pump();
    }

    /// Moves the clock, lets the consumer act on every deadline that passes, in order, and pumps.
    pub fn advance(&mut self, by: Duration) {
        let until = self.clock.now() + by;
        while let Some(at) = self.queue.poll_timeout().filter(|at| *at <= until) {
            self.clock.advance(at.saturating_duration_since(self.clock.now()));
            self.queue.handle_timeout(self.clock.now());
            self.pump();
        }
        self.clock.advance(until.saturating_duration_since(self.clock.now()));
        self.queue.handle_timeout(self.clock.now());
        self.pump();
    }

    /// Drops the connection, as a network failure would: the consumers' registrations are lost, and the watch
    /// reopens on a new connection.
    pub fn drop_connection(&mut self) {
        self.world.kill(self.conn);
        self.pump();
    }

    /// Does what the consumer and the world ask until neither has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(action) = self.queue.poll_action() {
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
        let connect =
            CommandConnect { client_version: "testkit".into(), protocol_version: Some(21), ..Default::default() };
        self.write(&BaseCommand { r#type: Type::CONNECT.into(), connect: connect.into(), ..Default::default() });
        let _connected = self.world.output(self.conn);
    }

    fn open_session(&mut self) {
        self.session_id += 1;
        let lookup = CommandScalableTopicLookup {
            session_id: self.session_id,
            topic: self.topic.clone(),
            create_if_missing: Some(true),
            ..Default::default()
        };
        self.write(&BaseCommand {
            r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
            scalableTopicLookup: lookup.into(),
            ..Default::default()
        });
    }

    fn reconnect(&mut self) {
        self.world.disconnected(self.conn);
        self.conn = self.world.connect();
        self.handshake();
        // What was registered or subscribing on the old connection is gone with it, a consumer being closed too.
        self.closing.clear();
        let now = self.clock.now();
        for (_, (segment, generation)) in std::mem::take(&mut self.registered) {
            self.queue.on_disconnected(segment, generation, now);
        }
        for (_, (segment, generation, _)) in std::mem::take(&mut self.subscribing) {
            let lost = RegisterError::Unavailable("disconnected".into());
            self.queue.on_subscribe_failed(segment, generation, lost, now);
        }
        self.open_session();
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Connect { segment, generation, .. } => {
                let (consumer_id, request_id) = (self.next_id, self.next_id + 1);
                self.next_id += 2;
                let mut command = self.queue.subscribe_command(segment, consumer_id).expect("a segment's consumer");
                command.subscribe.get_or_insert_default().request_id = request_id;
                self.subscribing.insert(request_id, (segment, generation, consumer_id));
                *self.subscribes.entry(segment).or_default() += 1;
                self.write(&command);
            }
            Action::Send { command, .. } => self.write(&command),
            Action::CloseConsumer { segment, generation } => {
                // Without a registration, the broker closed the consumer, and the Queue consumer heard it.
                let registered = self.registered.iter().find(|(_, v)| **v == (segment, generation));
                let Some((&consumer_id, _)) = registered else { return };
                let request_id = self.next_id;
                self.next_id += 1;
                self.closing.insert(request_id, consumer_id);
                let close = CommandCloseConsumer { consumer_id, request_id, ..Default::default() };
                self.write(&BaseCommand {
                    r#type: Type::CLOSE_CONSUMER.into(),
                    close_consumer: close.into(),
                    ..Default::default()
                });
            }
            Action::Subscribed(result) => self.subscribed = Some(result),
            Action::Closed => self.closed = true,
        }
    }

    fn answer(&mut self, output: &[u8]) {
        let decoder = FrameDecoder::default();
        let mut buf = BytesMut::from(output);
        let now = self.clock.now();
        while let Some(frame) = decoder.decode(&mut buf).expect("the world sent a bad frame") {
            let command = BaseCommand::decode_from_slice(&frame.command).expect("undecodable command");
            let EnumValue::Known(kind) = command.r#type else { panic!("unknown command {command:?}") };
            // What the world sends on a connection answers what the harness wrote on it, or concerns a consumer or
            // the watch session registered there.
            match kind {
                Type::SUCCESS => {
                    let request = command.success.request_id;
                    if let Some((segment, generation, consumer_id)) = self.subscribing.remove(&request) {
                        self.registered.insert(consumer_id, (segment, generation));
                        self.queue.on_subscribed(segment, generation, consumer_id);
                    } else {
                        let consumer_id = self.closing.remove(&request).expect("an answer to a close");
                        // The broker may have closed it meanwhile.
                        let Some((segment, generation)) = self.registered.remove(&consumer_id) else { continue };
                        self.queue.on_closed(segment, generation);
                    }
                }
                Type::ERROR => {
                    let error = &command.error;
                    let (segment, generation, _) =
                        self.subscribing.remove(&error.request_id).expect("an answer to a subscribe");
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    self.queue.on_subscribe_failed(segment, generation, refused, now);
                }
                Type::MESSAGE => {
                    let (segment, generation) = self.registered[&command.message.consumer_id];
                    let section = frame.payload.as_ref().expect("a message without a payload");
                    self.queue.on_message(segment, generation, &command.message, section);
                }
                Type::REACHED_END_OF_TOPIC => {
                    let (segment, generation) = self.registered[&command.reachedEndOfTopic.consumer_id];
                    self.queue.on_reached_end_of_topic(segment, generation);
                }
                // The broker closed the consumer: an unload, a deleted segment.
                Type::CLOSE_CONSUMER => {
                    let (segment, generation) = self.registered.remove(&command.close_consumer.consumer_id).unwrap();
                    self.queue.on_disconnected(segment, generation, now);
                }
                // The only other command the world sends: a layout, on the session open now.
                _ => {
                    let update = &command.scalableTopicUpdate;
                    assert_eq!((update.session_id, update.error), (self.session_id, None), "{command:?}");
                    let layout =
                        Layout::from_dag(&update.dag, update.resolved_topic_name.as_deref().unwrap_or(&self.topic))
                            .unwrap();
                    self.queue.on_layout(Rc::new(layout), now);
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
