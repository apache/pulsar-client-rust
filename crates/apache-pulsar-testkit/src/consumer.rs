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

//! Drives a [`SegmentConsumer`] against a simulated [`World`], doing what the driver does for each [`Action`], on a
//! clock the test moves by hand. Deterministic: no sockets, no sleeps.

use std::collections::HashMap;
use std::time::Duration;

use apache_pulsar_protocol::consumer::{Action, ConsumerOptions, SegmentConsumer};
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
use apache_pulsar_protocol::message_id::MessageId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandCloseConsumer, CommandConnect};
use buffa::{EnumValue, Message};
use bytes::BytesMut;

use crate::Clock;
use crate::scalable::{ConnId, World};

/// See the module documentation.
#[derive(Debug)]
pub struct ConsumerHarness {
    pub world: World,
    pub consumer: SegmentConsumer,
    pub clock: Clock,
    conn: ConnId,
    next_id: u64,
    /// Subscribes waiting for an answer: request id to `(generation, consumer id)`.
    subscribing: HashMap<u64, (u64, u64)>,
    /// Registered consumers, until the answer to their close: consumer id to generation.
    registered: HashMap<u64, u64>,
    /// Closes waiting for an answer: request id to consumer id.
    closing: HashMap<u64, u64>,
    /// How the first subscribe went.
    pub subscribed: Option<Result<(), RegisterError>>,
    pub closed: bool,
}

impl ConsumerHarness {
    /// A consumer of segment `segment`, whose topic is `segment_topic`, subscribing now.
    pub fn new(mut world: World, segment_topic: &str, segment: u64, options: ConsumerOptions) -> ConsumerHarness {
        let conn = world.connect();
        let clock = Clock::default();
        let consumer = SegmentConsumer::new(options, segment as i64, segment_topic.to_string(), clock.now());
        let mut harness = ConsumerHarness {
            world,
            consumer,
            clock,
            conn,
            next_id: 1,
            subscribing: HashMap::new(),
            registered: HashMap::new(),
            closing: HashMap::new(),
            subscribed: None,
            closed: false,
        };
        harness.handshake();
        harness.pump();
        harness
    }

    /// Every message in, as the application takes them, after what the world has for the consumer.
    pub fn take(&mut self) -> Vec<ReceivedMessage> {
        self.pump();
        let taken = std::iter::from_fn(|| self.consumer.next_message()).collect();
        self.pump();
        taken
    }

    /// The values of every message in, as text.
    pub fn take_values(&mut self) -> Vec<String> {
        self.take().iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
    }

    pub fn ack(&mut self, id: &MessageId) {
        self.consumer.acknowledge(&id.entry, self.clock.now());
        self.pump();
    }

    pub fn nack(&mut self, id: &MessageId) {
        self.consumer.negative_acknowledge(&id.entry, self.clock.now());
        self.pump();
    }

    pub fn close(&mut self) {
        self.consumer.close();
        self.pump();
    }

    /// Moves the clock, lets the consumer act on every deadline that passes, in order, and pumps.
    pub fn advance(&mut self, by: Duration) {
        let until = self.clock.now() + by;
        while let Some(at) = self.consumer.poll_timeout().filter(|at| *at <= until) {
            self.clock.advance(at.saturating_duration_since(self.clock.now()));
            self.consumer.handle_timeout(self.clock.now());
            self.pump();
        }
        self.clock.advance(until.saturating_duration_since(self.clock.now()));
        self.consumer.handle_timeout(self.clock.now());
        self.pump();
    }

    /// Drops the connection, as a network failure would.
    pub fn drop_connection(&mut self) {
        self.world.kill(self.conn);
        self.pump();
    }

    /// Does what the consumer and the world ask until neither has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(action) = self.consumer.poll_action() {
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

    fn reconnect(&mut self) {
        self.world.disconnected(self.conn);
        self.conn = self.world.connect();
        self.handshake();
        // What was registered or subscribing on the old connection is gone with it, a consumer being closed too.
        self.closing.clear();
        let now = self.clock.now();
        for (_, generation) in std::mem::take(&mut self.registered) {
            self.consumer.on_disconnected(generation, now);
        }
        for (_, (generation, _)) in std::mem::take(&mut self.subscribing) {
            self.consumer.on_subscribe_failed(generation, RegisterError::Unavailable("disconnected".into()), now);
        }
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Connect { generation } => {
                let (consumer_id, request_id) = (self.next_id, self.next_id + 1);
                self.next_id += 2;
                let mut command = self.consumer.subscribe_command(consumer_id);
                command.subscribe.get_or_insert_default().request_id = request_id;
                self.subscribing.insert(request_id, (generation, consumer_id));
                self.write(&command);
            }
            Action::Send(command) => self.write(&command),
            Action::CloseConsumer { generation } => {
                let registered = self.registered.iter().find(|(_, g)| **g == generation);
                let (&consumer_id, _) = registered.expect("a registered consumer to close");
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
            // What the world sends on a connection answers what the harness wrote on it, or concerns a consumer still
            // registered there.
            match kind {
                Type::SUCCESS => {
                    let request = command.success.request_id;
                    if let Some((generation, consumer_id)) = self.subscribing.remove(&request) {
                        self.registered.insert(consumer_id, generation);
                        self.consumer.on_subscribed(generation, consumer_id);
                    } else {
                        let consumer_id = self.closing.remove(&request).expect("an answer to a close");
                        let generation = self.registered.remove(&consumer_id).unwrap();
                        self.consumer.on_closed(generation);
                    }
                }
                Type::ERROR => {
                    let error = &command.error;
                    let (generation, _) = self.subscribing.remove(&error.request_id).expect("an answer to a subscribe");
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    self.consumer.on_subscribe_failed(generation, refused, now);
                }
                Type::MESSAGE => {
                    let generation = self.registered[&command.message.consumer_id];
                    let section = frame.payload.as_ref().expect("a message without a payload");
                    self.consumer.on_message(generation, &command.message, section);
                }
                Type::REACHED_END_OF_TOPIC => {
                    let generation = self.registered[&command.reachedEndOfTopic.consumer_id];
                    self.consumer.on_reached_end_of_topic(generation);
                }
                // The broker closed the consumer: an unload, or the topic moved.
                _ => {
                    assert_eq!(kind, Type::CLOSE_CONSUMER, "{command:?}");
                    let generation = self.registered.remove(&command.close_consumer.consumer_id).unwrap();
                    self.consumer.on_disconnected(generation, now);
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
