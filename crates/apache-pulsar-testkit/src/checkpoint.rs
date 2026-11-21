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

//! Drives a [`CheckpointConsumer`] against a simulated [`World`], doing what the driver does for each [`Action`], on
//! a clock the test moves by hand. One connection carries the topic's layout watch and every segment's reader.
//! Deterministic: no sockets, no sleeps.
//!
//! [`CheckpointConsumer`]: apache_pulsar_protocol::checkpoint_consumer::CheckpointConsumer
//! [`Action`]: apache_pulsar_protocol::checkpoint_consumer::Action
//! [`World`]: crate::scalable::World

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::Duration;

use apache_pulsar_protocol::checkpoint::Checkpoint;
use apache_pulsar_protocol::checkpoint_consumer::{Action, CheckpointConsumer};
use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::message_id::EntryId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseConsumer, CommandConnect, CommandGetLastMessageId, CommandScalableTopicLookup,
};
use buffa::{EnumValue, Message};
use bytes::BytesMut;

use crate::Clock;
use crate::producer::{decode, empty_layout};
use crate::scalable::{ConnId, World};

/// See the module documentation.
#[derive(Debug)]
pub struct CheckpointHarness {
    pub world: World,
    pub consumer: CheckpointConsumer,
    pub clock: Clock,
    topic: String,
    conn: ConnId,
    session_id: u64,
    next_id: u64,
    /// Subscribes waiting for an answer: request id to `(segment, generation, consumer id)`.
    subscribing: HashMap<u64, (u64, u64, u64)>,
    /// Registered readers, until the answer to their close: consumer id to `(segment, generation)`.
    registered: HashMap<u64, (u64, u64)>,
    /// Closes waiting for an answer: request id to consumer id.
    closing: HashMap<u64, u64>,
    /// Last message ids asked, waiting for an answer: request id to `(segment, generation)`.
    asking: HashMap<u64, (u64, u64)>,
    /// How the create went.
    pub subscribed: Option<Result<(), RegisterError>>,
    pub closed: bool,
    /// How many `CommandSubscribe` the harness sent, per segment.
    pub subscribes: BTreeMap<u64, usize>,
}

impl CheckpointHarness {
    /// Opens a watch on `topic` in `world`, and creates a Checkpoint consumer on its first layout, starting at `start`.
    pub fn new(mut world: World, topic: &str, options: ConsumerOptions, start: Checkpoint) -> CheckpointHarness {
        let conn = world.connect();
        let clock = Clock::default();
        let mut harness = CheckpointHarness {
            world,
            // Replaced once the first layout is in.
            consumer: CheckpointConsumer::new(
                options.clone(),
                Checkpoint::Earliest,
                Rc::new(empty_layout()),
                clock.now(),
            ),
            clock,
            topic: topic.to_string(),
            conn,
            session_id: 0,
            next_id: 1,
            subscribing: HashMap::new(),
            registered: HashMap::new(),
            closing: HashMap::new(),
            asking: HashMap::new(),
            subscribed: None,
            closed: false,
            subscribes: BTreeMap::new(),
        };
        harness.handshake();
        harness.open_session();
        let command = decode(&harness.world.output(harness.conn)).pop().expect("no answer to the scalable lookup");
        let update = &command.scalableTopicUpdate;
        assert!(update.error.is_none(), "lookup failed: {:?} {:?}", update.error, update.message);
        let layout = Layout::from_dag(&update.dag, &harness.topic).unwrap();
        harness.consumer = CheckpointConsumer::new(options, start, Rc::new(layout), harness.clock.now());
        harness.pump();
        harness
    }

    /// Every message in, as the application gets them, after what the world has for the consumer.
    pub fn take(&mut self) -> Vec<ReceivedMessage> {
        self.take_up_to(usize::MAX)
    }

    /// Up to `max` messages, as the application gets them.
    pub fn take_up_to(&mut self, max: usize) -> Vec<ReceivedMessage> {
        self.pump();
        let mut taken = Vec::new();
        while taken.len() < max
            && let Some(message) = self.consumer.next_message()
        {
            self.consumer.deliver(&message.id);
            taken.push(message);
            self.pump();
        }
        taken
    }

    /// The values of every message in, as text.
    pub fn take_values(&mut self) -> Vec<String> {
        values(&self.take())
    }

    /// The consumer's checkpoint, through its bytes, as an application stores and restores it.
    pub fn checkpoint(&self) -> Checkpoint {
        Checkpoint::from_bytes(&self.consumer.checkpoint().to_bytes()).unwrap()
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

    /// Drops the connection, as a network failure would: the readers' registrations are lost, and the watch reopens
    /// on a new connection.
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
        // What was registered, subscribing or asked on the old connection is gone with it.
        self.closing.clear();
        let now = self.clock.now();
        for (_, (segment, generation)) in std::mem::take(&mut self.registered) {
            self.consumer.on_disconnected(segment, generation, now);
        }
        for (_, (segment, generation, _)) in std::mem::take(&mut self.subscribing) {
            let lost = RegisterError::Unavailable("disconnected".into());
            self.consumer.on_subscribe_failed(segment, generation, lost, now);
        }
        for (_, (segment, generation)) in std::mem::take(&mut self.asking) {
            self.consumer.on_last_message_id_failed(segment, generation, now);
        }
        self.open_session();
    }

    fn act(&mut self, action: Action) {
        match action {
            Action::Connect { segment, generation, .. } => {
                let (consumer_id, request_id) = (self.next_id, self.next_id + 1);
                self.next_id += 2;
                let mut command = self.consumer.subscribe_command(segment, consumer_id).expect("a segment's reader");
                command.subscribe.get_or_insert_default().request_id = request_id;
                self.subscribing.insert(request_id, (segment, generation, consumer_id));
                *self.subscribes.entry(segment).or_default() += 1;
                self.write(&command);
            }
            Action::Send { command, .. } => self.write(&command),
            Action::CloseConsumer { segment, generation } => {
                // Without a registration, the broker closed the reader, and the consumer heard it.
                let Some(consumer_id) = self.consumer_id(segment, generation) else { return };
                let request_id = self.request_id();
                self.closing.insert(request_id, consumer_id);
                let close = CommandCloseConsumer { consumer_id, request_id, ..Default::default() };
                self.write(&BaseCommand {
                    r#type: Type::CLOSE_CONSUMER.into(),
                    close_consumer: close.into(),
                    ..Default::default()
                });
            }
            Action::GetLastMessageId { segment, generation } => {
                // Without a registration, the broker closed the reader, and the consumer heard it.
                let Some(consumer_id) = self.consumer_id(segment, generation) else { return };
                let request_id = self.request_id();
                self.asking.insert(request_id, (segment, generation));
                let get = CommandGetLastMessageId { consumer_id, request_id, ..Default::default() };
                self.write(&BaseCommand {
                    r#type: Type::GET_LAST_MESSAGE_ID.into(),
                    getLastMessageId: get.into(),
                    ..Default::default()
                });
            }
            Action::Subscribed(result) => self.subscribed = Some(result),
            Action::Closed => self.closed = true,
        }
    }

    fn consumer_id(&self, segment: u64, generation: u64) -> Option<u64> {
        self.registered.iter().find(|(_, v)| **v == (segment, generation)).map(|(id, _)| *id)
    }

    fn request_id(&mut self) -> u64 {
        self.next_id += 1;
        self.next_id - 1
    }

    fn answer(&mut self, output: &[u8]) {
        let decoder = FrameDecoder::default();
        let mut buf = BytesMut::from(output);
        let now = self.clock.now();
        while let Some(frame) = decoder.decode(&mut buf).expect("the world sent a bad frame") {
            let command = BaseCommand::decode_from_slice(&frame.command).expect("undecodable command");
            let EnumValue::Known(kind) = command.r#type else { panic!("unknown command {command:?}") };
            // What the world sends on a connection answers what the harness wrote on it, or concerns a reader or the
            // watch session registered there.
            match kind {
                Type::SUCCESS => {
                    let request = command.success.request_id;
                    if let Some((segment, generation, consumer_id)) = self.subscribing.remove(&request) {
                        self.registered.insert(consumer_id, (segment, generation));
                        self.consumer.on_subscribed(segment, generation, consumer_id);
                    } else {
                        let consumer_id = self.closing.remove(&request).expect("an answer to a close");
                        // The broker may have closed it meanwhile.
                        let Some((segment, generation)) = self.registered.remove(&consumer_id) else { continue };
                        self.consumer.on_closed(segment, generation);
                    }
                }
                Type::ERROR => {
                    let error = &command.error;
                    if let Some((segment, generation)) = self.asking.remove(&error.request_id) {
                        self.consumer.on_last_message_id_failed(segment, generation, now);
                        continue;
                    }
                    let (segment, generation, _) =
                        self.subscribing.remove(&error.request_id).expect("an answer to a subscribe");
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    self.consumer.on_subscribe_failed(segment, generation, refused, now);
                }
                Type::GET_LAST_MESSAGE_ID_RESPONSE => {
                    let response = &command.getLastMessageIdResponse;
                    let (segment, generation) = self.asking.remove(&response.request_id).expect("an asked id");
                    let id = EntryId::from_proto(&response.last_message_id);
                    self.consumer.on_last_message_id(segment, generation, id);
                }
                Type::MESSAGE => {
                    let (segment, generation) = self.registered[&command.message.consumer_id];
                    let section = frame.payload.as_ref().expect("a message without a payload");
                    self.consumer.on_message(segment, generation, &command.message, section);
                }
                Type::REACHED_END_OF_TOPIC => {
                    let (segment, generation) = self.registered[&command.reachedEndOfTopic.consumer_id];
                    self.consumer.on_reached_end_of_topic(segment, generation);
                }
                // The broker closed the reader: an unload, a deleted segment.
                Type::CLOSE_CONSUMER => {
                    let (segment, generation) = self.registered.remove(&command.close_consumer.consumer_id).unwrap();
                    self.consumer.on_disconnected(segment, generation, now);
                }
                // The only other command the world sends: a layout, on the session open now.
                _ => {
                    let update = &command.scalableTopicUpdate;
                    assert_eq!((update.session_id, update.error), (self.session_id, None), "{command:?}");
                    let layout = Layout::from_dag(&update.dag, &self.topic).unwrap();
                    self.consumer.on_layout(Rc::new(layout), now);
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

/// The messages' values, as text.
pub fn values(messages: &[ReceivedMessage]) -> Vec<String> {
    messages.iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}
