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

//! Drives Stream consumers against a simulated [`World`], doing what the driver does for each [`Action`], on a clock
//! the test moves by hand. Each member of the group has its own connection, which carries its registration with the
//! topic's controller and its segments' consumers. Deterministic: no sockets, no sleeps.

use std::collections::{BTreeMap, BTreeSet, HashMap};
use std::time::Duration;

use apache_pulsar_protocol::assignment::AssignmentSession;
use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::message_id::MessageId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseConsumer, CommandConnect, CommandScalableTopicClose, CommandScalableTopicLookup,
};
use apache_pulsar_protocol::stream_consumer::{Action, StreamConsumer};
use buffa::{EnumValue, Message};
use bytes::BytesMut;

use crate::Clock;
use crate::producer::decode;
use crate::scalable::{ConnId, World};

/// See the module documentation.
#[derive(Debug)]
pub struct StreamHarness {
    pub world: World,
    pub clock: Clock,
    topic: String,
    pub members: Vec<Member>,
    next_id: u64,
}

/// A Stream consumer of the group, and what its driver keeps.
#[derive(Debug)]
pub struct Member {
    pub consumer: StreamConsumer,
    conn: ConnId,
    /// Registrations waiting for an answer: request id to `(generation, consumer id)`.
    registering: HashMap<u64, (u64, u64)>,
    /// The registration: `(generation, consumer id)`.
    session: Option<(u64, u64)>,
    /// Unsubscribes waiting for an answer: request id to generation.
    unregistering: HashMap<u64, u64>,
    /// Subscribes waiting for an answer: request id to `(segment, generation, consumer id)`.
    subscribing: HashMap<u64, (u64, u64, u64)>,
    /// Registered segment consumers, until the answer to their close: consumer id to `(segment, generation)`.
    registered: HashMap<u64, (u64, u64)>,
    /// Closes waiting for an answer: request id to consumer id.
    closing: HashMap<u64, u64>,
    /// How the subscribe went.
    pub subscribed: Option<Result<(), RegisterError>>,
    pub closed: bool,
    /// How many `CommandSubscribe` it sent, per segment.
    pub subscribes: BTreeMap<u64, usize>,
    /// Its process died: nothing drives it any more.
    crashed: bool,
}

impl StreamHarness {
    pub fn new(world: World, topic: &str) -> StreamHarness {
        StreamHarness { world, clock: Clock::default(), topic: topic.to_string(), members: Vec::new(), next_id: 1 }
    }

    /// A new member named `name`: it looks up the topic's layout on its connection, as the driver does to find the
    /// controller, and subscribes. Returns its index.
    pub fn join(&mut self, name: &str, options: ConsumerOptions) -> usize {
        let conn = self.world.connect();
        handshake(&mut self.world, conn);
        let lookup = CommandScalableTopicLookup {
            session_id: 1,
            topic: self.topic.clone(),
            create_if_missing: Some(true),
            ..Default::default()
        };
        let lookup = BaseCommand {
            r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
            scalableTopicLookup: lookup.into(),
            ..Default::default()
        };
        write(&mut self.world, conn, &lookup);
        let command = decode(&self.world.output(conn)).pop().expect("no answer to the scalable lookup");
        let layout = Layout::from_dag(&command.scalableTopicUpdate.dag, &self.topic).unwrap();
        let close = CommandScalableTopicClose { session_id: 1, ..Default::default() };
        let close = BaseCommand {
            r#type: Type::SCALABLE_TOPIC_CLOSE.into(),
            scalableTopicClose: close.into(),
            ..Default::default()
        };
        write(&mut self.world, conn, &close);
        let initial: BTreeSet<u64> = layout.segments().map(|s| s.id).collect();
        let consumer = StreamConsumer::new(options, self.topic.clone(), name.into(), initial, self.clock.now());
        self.members.push(Member {
            consumer,
            conn,
            registering: HashMap::new(),
            session: None,
            unregistering: HashMap::new(),
            subscribing: HashMap::new(),
            registered: HashMap::new(),
            closing: HashMap::new(),
            subscribed: None,
            closed: false,
            subscribes: BTreeMap::new(),
            crashed: false,
        });
        self.pump();
        self.members.len() - 1
    }

    /// Every message the member has in, as the application takes them, after what the world has for it.
    pub fn take(&mut self, member: usize) -> Vec<ReceivedMessage> {
        self.pump();
        let now = self.clock.now();
        let consumer = &mut self.members[member].consumer;
        let taken = std::iter::from_fn(|| consumer.next_message(now)).collect();
        self.pump();
        taken
    }

    /// The values of every message the member has in, as text.
    pub fn take_values(&mut self, member: usize) -> Vec<String> {
        self.take(member).iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
    }

    pub fn ack(&mut self, member: usize, id: &MessageId) {
        self.members[member].consumer.acknowledge_cumulative(id, self.clock.now());
        self.pump();
    }

    pub fn close(&mut self, member: usize) {
        self.members[member].consumer.close(self.clock.now());
        self.pump();
    }

    /// Moves the clock, lets every member act on each deadline that passes, in order, and pumps.
    pub fn advance(&mut self, by: Duration) {
        let until = self.clock.now() + by;
        loop {
            let next = self.members.iter().filter(|m| !m.crashed).filter_map(|m| m.consumer.poll_timeout());
            let next = next.min().filter(|at| *at <= until);
            let at = next.unwrap_or(until);
            self.clock.advance(at.saturating_duration_since(self.clock.now()));
            for member in self.members.iter_mut().filter(|member| !member.crashed) {
                member.consumer.handle_timeout(self.clock.now());
            }
            self.pump();
            if next.is_none() {
                return;
            }
        }
    }

    /// Drops a member's connection, as a network failure would: its registration and its segments' consumers are
    /// lost, and come back on a new connection.
    pub fn drop_connection(&mut self, member: usize) {
        self.world.kill(self.members[member].conn);
        self.pump();
    }

    /// Stops a member for good, as if its process died: its connection closes, and nothing drives it any more. The
    /// controller keeps it registered for the grace period.
    pub fn crash(&mut self, member: usize) {
        self.members[member].crashed = true;
        self.world.kill(self.members[member].conn);
        self.pump();
    }

    /// Does what the members and the world ask until none has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            let alive: Vec<usize> = (0..self.members.len()).filter(|&at| !self.members[at].crashed).collect();
            for at in alive {
                while let Some(action) = self.members[at].consumer.poll_action() {
                    progressed = true;
                    self.act(at, action);
                }
                let output = self.world.output(self.members[at].conn);
                if !output.is_empty() {
                    progressed = true;
                    self.answer(at, &output);
                }
                // The world closed the connection, or the test did: connect again.
                if !self.world.is_open(self.members[at].conn) {
                    progressed = true;
                    self.reconnect(at);
                }
            }
            if !progressed {
                return;
            }
        }
    }

    fn ids(&mut self, n: u64) -> u64 {
        let first = self.next_id;
        self.next_id += n;
        first
    }

    fn act(&mut self, at: usize, action: Action) {
        match action {
            Action::Register { generation } => {
                let (consumer_id, request_id) = (self.ids(1), self.ids(1));
                let member = &mut self.members[at];
                let mut command = member.consumer.register_command(consumer_id);
                command.scalableTopicSubscribe.get_or_insert_default().request_id = request_id;
                member.registering.insert(request_id, (generation, consumer_id));
                write(&mut self.world, member.conn, &command);
            }
            Action::Unregister { generation } => {
                let request_id = self.ids(1);
                let member = &mut self.members[at];
                let (_, consumer_id) = member.session.filter(|(g, _)| *g == generation).expect("a registration");
                let mut command = AssignmentSession::unregister_command(consumer_id);
                command.scalableTopicUnsubscribe.get_or_insert_default().request_id = request_id;
                member.unregistering.insert(request_id, generation);
                write(&mut self.world, member.conn, &command);
            }
            Action::Connect { segment, generation, .. } => {
                let (consumer_id, request_id) = (self.ids(1), self.ids(1));
                let member = &mut self.members[at];
                let mut command =
                    member.consumer.subscribe_command(segment, consumer_id).expect("a segment's consumer");
                command.subscribe.get_or_insert_default().request_id = request_id;
                member.subscribing.insert(request_id, (segment, generation, consumer_id));
                *member.subscribes.entry(segment).or_default() += 1;
                write(&mut self.world, member.conn, &command);
            }
            Action::Send { command, .. } => write(&mut self.world, self.members[at].conn, &command),
            Action::CloseConsumer { segment, generation } => {
                let request_id = self.ids(1);
                let member = &mut self.members[at];
                // The Stream consumer hears of the broker closing one before it asks to close it.
                let registered = member.registered.iter().find(|(_, v)| **v == (segment, generation));
                let (&consumer_id, _) = registered.expect("a registered consumer");
                member.closing.insert(request_id, consumer_id);
                let close = CommandCloseConsumer { consumer_id, request_id, ..Default::default() };
                let close = BaseCommand {
                    r#type: Type::CLOSE_CONSUMER.into(),
                    close_consumer: close.into(),
                    ..Default::default()
                };
                write(&mut self.world, member.conn, &close);
            }
            Action::Subscribed(result) => self.members[at].subscribed = Some(result),
            Action::Closed => self.members[at].closed = true,
        }
    }

    fn answer(&mut self, at: usize, output: &[u8]) {
        let decoder = FrameDecoder::default();
        let mut buf = BytesMut::from(output);
        let now = self.clock.now();
        let member = &mut self.members[at];
        while let Some(frame) = decoder.decode(&mut buf).expect("the world sent a bad frame") {
            let command = BaseCommand::decode_from_slice(&frame.command).expect("undecodable command");
            let EnumValue::Known(kind) = command.r#type else { panic!("unknown command {command:?}") };
            match kind {
                Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE => {
                    let response = &command.scalableTopicSubscribeResponse;
                    let (generation, consumer_id) =
                        member.registering.remove(&response.request_id).expect("an answer to a registration");
                    match response.error {
                        Some(error) => {
                            let message = response.message.clone().unwrap_or_default();
                            let refused = RegisterError::Server { error, message };
                            member.consumer.on_register_failed(generation, refused, now);
                        }
                        None => {
                            member.session = Some((generation, consumer_id));
                            member.consumer.on_registered(generation, &response.assignment, now);
                        }
                    }
                }
                Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE => {
                    let update = &command.scalableTopicAssignmentUpdate;
                    let (generation, consumer_id) = member.session.expect("a registration");
                    assert_eq!(update.consumer_id, consumer_id, "an update for another registration");
                    member.consumer.on_assignment(generation, &update.assignment, now);
                }
                Type::SUCCESS => {
                    let request = command.success.request_id;
                    if let Some((segment, generation, consumer_id)) = member.subscribing.remove(&request) {
                        member.registered.insert(consumer_id, (segment, generation));
                        member.consumer.on_subscribed(segment, generation, consumer_id, now);
                    } else if let Some(generation) = member.unregistering.remove(&request) {
                        member.session = None;
                        member.consumer.on_unregistered(generation);
                    } else {
                        let consumer_id = member.closing.remove(&request).expect("an answer to a close");
                        // The broker may have closed it meanwhile.
                        let Some((segment, generation)) = member.registered.remove(&consumer_id) else { continue };
                        member.consumer.on_closed(segment, generation, now);
                    }
                }
                Type::ERROR => {
                    let error = &command.error;
                    let (segment, generation, _) =
                        member.subscribing.remove(&error.request_id).expect("an answer to a subscribe");
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    member.consumer.on_subscribe_failed(segment, generation, refused, now);
                }
                Type::MESSAGE => {
                    let (segment, generation) = member.registered[&command.message.consumer_id];
                    let section = frame.payload.as_ref().expect("a message without a payload");
                    member.consumer.on_message(segment, generation, &command.message, section, now);
                }
                // The Stream consumer has no use for it.
                Type::REACHED_END_OF_TOPIC => {}
                // The only other command the world sends a Stream consumer: the broker closed a segment's consumer.
                _ => {
                    let (segment, generation) = member.registered.remove(&command.close_consumer.consumer_id).unwrap();
                    member.consumer.on_disconnected(segment, generation, now);
                }
            }
        }
    }

    fn reconnect(&mut self, at: usize) {
        let now = self.clock.now();
        self.world.disconnected(self.members[at].conn);
        let conn = self.world.connect();
        handshake(&mut self.world, conn);
        let member = &mut self.members[at];
        member.conn = conn;
        // What was registered or subscribing on the old connection is gone with it, a consumer being closed too.
        member.closing.clear();
        for (_, (segment, generation)) in std::mem::take(&mut member.registered) {
            member.consumer.on_disconnected(segment, generation, now);
        }
        for (_, (segment, generation, _)) in std::mem::take(&mut member.subscribing) {
            let lost = RegisterError::Unavailable("disconnected".into());
            member.consumer.on_subscribe_failed(segment, generation, lost, now);
        }
        for (_, (generation, _)) in std::mem::take(&mut member.registering) {
            member.consumer.on_register_failed(generation, RegisterError::Unavailable("disconnected".into()), now);
        }
        if let Some((generation, _)) = member.session.take() {
            member.consumer.on_session_lost(generation, now);
        }
        for (_, generation) in std::mem::take(&mut member.unregistering) {
            member.consumer.on_unregistered(generation);
        }
    }
}

fn handshake(world: &mut World, conn: ConnId) {
    let connect = CommandConnect { client_version: "testkit".into(), protocol_version: Some(21), ..Default::default() };
    write(world, conn, &BaseCommand { r#type: Type::CONNECT.into(), connect: connect.into(), ..Default::default() });
    let _connected = world.output(conn);
}

fn write(world: &mut World, conn: ConnId, command: &BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(command, &mut buf).unwrap();
    world.input(conn, &buf);
}
