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

//! Drives [`CheckpointConsumer`]s against a simulated [`World`], doing what the driver does for each [`Action`], on
//! a clock the test moves by hand. Deterministic: no sockets, no sleeps.
//!
//! - [`CheckpointHarness`] drives a consumer on its own: one connection carries the topic's layout watch and every
//!   segment's reader.
//! - [`GroupHarness`] drives the members of a group, each on its own connection, which carries its registration with
//!   the topic's controller and its readers.
//!
//! [`CheckpointHarness`]: crate::checkpoint::CheckpointHarness
//! [`GroupHarness`]: crate::checkpoint::GroupHarness
//! [`CheckpointConsumer`]: apache_pulsar_protocol::checkpoint_consumer::CheckpointConsumer
//! [`Action`]: apache_pulsar_protocol::checkpoint_consumer::Action
//! [`World`]: crate::scalable::World

use std::collections::{BTreeMap, HashMap};
use std::rc::Rc;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::assignment::AssignmentSession;
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
    wire: Wire,
    session_id: u64,
    next_id: u64,
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
            wire: Wire::new(conn),
            session_id: 0,
            next_id: 1,
            subscribed: None,
            closed: false,
            subscribes: BTreeMap::new(),
        };
        handshake(&mut harness.world, conn);
        harness.open_session();
        let command = decode(&harness.world.output(conn)).pop().expect("no answer to the scalable lookup");
        let update = &command.scalableTopicUpdate;
        assert!(update.error.is_none(), "lookup failed: {:?} {:?}", update.error, update.message);
        let layout =
            Layout::from_dag(&update.dag, update.resolved_topic_name.as_deref().unwrap_or(&harness.topic)).unwrap();
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
            self.consumer.deliver(&message);
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
        self.world.kill(self.wire.conn);
        self.pump();
    }

    /// Does what the consumer and the world ask until neither has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            while let Some(action) = self.consumer.poll_action() {
                progressed = true;
                match self.wire.act(&mut self.world, &mut self.next_id, &self.consumer, action) {
                    Some(Report::Subscribe(segment)) => *self.subscribes.entry(segment).or_default() += 1,
                    Some(Report::Subscribed(result)) => self.subscribed = Some(result),
                    Some(Report::Closed) => self.closed = true,
                    None => {}
                }
            }
            let output = self.world.output(self.wire.conn);
            if !output.is_empty() {
                progressed = true;
                let now = self.clock.now();
                // The only commands that aren't for the consumer's readers: the layouts, on the session open now.
                for command in self.wire.answer(&mut self.consumer, &output, now) {
                    let update = &command.scalableTopicUpdate;
                    assert_eq!((update.session_id, update.error), (self.session_id, None), "{command:?}");
                    let layout =
                        Layout::from_dag(&update.dag, update.resolved_topic_name.as_deref().unwrap_or(&self.topic))
                            .unwrap();
                    self.consumer.on_layout(Rc::new(layout), now);
                }
            }
            // The world closed the connection, or the test did: connect again.
            if !self.world.is_open(self.wire.conn) {
                progressed = true;
                self.reconnect();
            }
            if !progressed {
                return;
            }
        }
    }

    fn open_session(&mut self) {
        self.session_id += 1;
        let lookup = CommandScalableTopicLookup {
            session_id: self.session_id,
            topic: self.topic.clone(),
            create_if_missing: Some(true),
            ..Default::default()
        };
        let lookup = BaseCommand {
            r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
            scalableTopicLookup: lookup.into(),
            ..Default::default()
        };
        write(&mut self.world, self.wire.conn, &lookup);
    }

    fn reconnect(&mut self) {
        self.world.disconnected(self.wire.conn);
        let conn = self.world.connect();
        handshake(&mut self.world, conn);
        self.wire.lost(conn, &mut self.consumer, self.clock.now());
        self.open_session();
    }
}

/// See the module documentation.
#[derive(Debug)]
pub struct GroupHarness {
    pub world: World,
    pub clock: Clock,
    topic: String,
    group: String,
    pub members: Vec<Member>,
    next_id: u64,
}

/// A member of the group, and what its driver keeps.
#[derive(Debug)]
pub struct Member {
    pub consumer: CheckpointConsumer,
    wire: Wire,
    /// How the create went.
    pub subscribed: Option<Result<(), RegisterError>>,
    pub closed: bool,
    /// How many `CommandSubscribe` it sent, per segment.
    pub subscribes: BTreeMap<u64, usize>,
    /// Its process died: nothing drives it any more.
    crashed: bool,
}

impl GroupHarness {
    /// The group `group` of Checkpoint consumers of `topic`, the name the broker resolved, in `world`. Members join it
    /// with [`GroupHarness::join`].
    pub fn new(world: World, topic: &str, group: &str) -> GroupHarness {
        let (topic, group) = (topic.to_string(), group.to_string());
        GroupHarness { world, clock: Clock::default(), topic, group, members: Vec::new(), next_id: 1 }
    }

    /// A new member named `name`, starting at `start`: it registers with the topic's controller on a connection of
    /// its own. Returns its index.
    pub fn join(&mut self, name: &str, options: ConsumerOptions, start: Checkpoint) -> usize {
        let conn = self.world.connect();
        handshake(&mut self.world, conn);
        let (topic, group) = (self.topic.clone(), self.group.clone());
        let consumer = CheckpointConsumer::grouped(options, start, topic, group, name.into(), self.clock.now());
        let member = Member {
            consumer,
            wire: Wire::new(conn),
            subscribed: None,
            closed: false,
            subscribes: BTreeMap::new(),
            crashed: false,
        };
        self.members.push(member);
        self.pump();
        self.members.len() - 1
    }

    /// Every message the member has in, as the application gets them, after what the world has for it.
    pub fn take(&mut self, member: usize) -> Vec<ReceivedMessage> {
        self.pump();
        let mut taken = Vec::new();
        while let Some(message) = self.members[member].consumer.next_message() {
            self.members[member].consumer.deliver(&message);
            taken.push(message);
            self.pump();
        }
        taken
    }

    /// The values of every message the member has in, as text.
    pub fn take_values(&mut self, member: usize) -> Vec<String> {
        values(&self.take(member))
    }

    /// The member's checkpoint, through its bytes, as an application stores and restores it.
    pub fn checkpoint(&self, member: usize) -> Checkpoint {
        Checkpoint::from_bytes(&self.members[member].consumer.checkpoint().to_bytes()).unwrap()
    }

    pub fn close(&mut self, member: usize) {
        self.members[member].consumer.close();
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

    /// Drops a member's connection, as a network failure would: its registration and its readers are lost, and come
    /// back on a new connection.
    pub fn drop_connection(&mut self, member: usize) {
        self.world.kill(self.members[member].wire.conn);
        self.pump();
    }

    /// Stops a member for good, as if its process died: its connection closes, and nothing drives it any more. The
    /// controller keeps it registered for the grace period.
    pub fn crash(&mut self, member: usize) {
        self.members[member].crashed = true;
        self.world.kill(self.members[member].wire.conn);
        self.pump();
    }

    /// Does what the members and the world ask until none has anything left to do.
    pub fn pump(&mut self) {
        loop {
            let mut progressed = false;
            for member in self.members.iter_mut().filter(|member| !member.crashed) {
                while let Some(action) = member.consumer.poll_action() {
                    progressed = true;
                    match member.wire.act(&mut self.world, &mut self.next_id, &member.consumer, action) {
                        Some(Report::Subscribe(segment)) => *member.subscribes.entry(segment).or_default() += 1,
                        Some(Report::Subscribed(result)) => member.subscribed = Some(result),
                        Some(Report::Closed) => member.closed = true,
                        None => {}
                    }
                }
                let output = self.world.output(member.wire.conn);
                if !output.is_empty() {
                    progressed = true;
                    let others = member.wire.answer(&mut member.consumer, &output, self.clock.now());
                    assert!(others.is_empty(), "a member opened no layout watch: {others:?}");
                }
                // The world closed the connection, or the test did: connect again.
                if !self.world.is_open(member.wire.conn) {
                    progressed = true;
                    self.world.disconnected(member.wire.conn);
                    let conn = self.world.connect();
                    handshake(&mut self.world, conn);
                    member.wire.lost(conn, &mut member.consumer, self.clock.now());
                }
            }
            if !progressed {
                return;
            }
        }
    }
}

/// What a driver keeps for a consumer: its connection, and what is registered and in flight there.
#[derive(Debug)]
struct Wire {
    conn: ConnId,
    /// Subscribes waiting for an answer: request id to `(segment, generation, consumer id)`.
    subscribing: HashMap<u64, (u64, u64, u64)>,
    /// Registered readers, until the answer to their close: consumer id to `(segment, generation)`.
    registered: HashMap<u64, (u64, u64)>,
    /// Closes waiting for an answer: request id to consumer id.
    closing: HashMap<u64, u64>,
    /// Last message ids asked, waiting for an answer: request id to `(segment, generation)`.
    asking: HashMap<u64, (u64, u64)>,
    /// A member's registrations waiting for an answer: request id to `(generation, consumer id)`.
    registering: HashMap<u64, (u64, u64)>,
    /// A member's registration: `(generation, consumer id)`.
    session: Option<(u64, u64)>,
    /// A member's unsubscribes waiting for an answer: request id to generation.
    unregistering: HashMap<u64, u64>,
}

/// What a consumer's action tells the test.
enum Report {
    /// A `CommandSubscribe` went out for the segment's reader.
    Subscribe(u64),
    Subscribed(Result<(), RegisterError>),
    Closed,
}

impl Wire {
    fn new(conn: ConnId) -> Wire {
        Wire {
            conn,
            subscribing: HashMap::new(),
            registered: HashMap::new(),
            closing: HashMap::new(),
            asking: HashMap::new(),
            registering: HashMap::new(),
            session: None,
            unregistering: HashMap::new(),
        }
    }

    /// Does what the consumer asks, on the connection, with ids from `ids`.
    fn act(
        &mut self,
        world: &mut World,
        ids: &mut u64,
        consumer: &CheckpointConsumer,
        action: Action,
    ) -> Option<Report> {
        let mut id = || {
            *ids += 1;
            *ids - 1
        };
        match action {
            Action::Register { generation } => {
                let (consumer_id, request_id) = (id(), id());
                let mut command = consumer.register_command(consumer_id).expect("a member of a group");
                command.scalableTopicSubscribe.get_or_insert_default().request_id = request_id;
                self.registering.insert(request_id, (generation, consumer_id));
                write(world, self.conn, &command);
            }
            Action::Unregister { generation } => {
                let request_id = id();
                let (_, consumer_id) = self.session.filter(|(g, _)| *g == generation).expect("a registration");
                let mut command = AssignmentSession::unregister_command(consumer_id);
                command.scalableTopicUnsubscribe.get_or_insert_default().request_id = request_id;
                self.unregistering.insert(request_id, generation);
                write(world, self.conn, &command);
            }
            Action::Connect { segment, generation, .. } => {
                let (consumer_id, request_id) = (id(), id());
                let mut command = consumer.subscribe_command(segment, consumer_id).expect("a segment's reader");
                command.subscribe.get_or_insert_default().request_id = request_id;
                self.subscribing.insert(request_id, (segment, generation, consumer_id));
                write(world, self.conn, &command);
                return Some(Report::Subscribe(segment));
            }
            Action::Send { command, .. } => write(world, self.conn, &command),
            Action::CloseConsumer { segment, generation } => {
                // Without a registration, the broker closed the reader, and the consumer heard it.
                let consumer_id = self.consumer_id(segment, generation)?;
                let request_id = id();
                self.closing.insert(request_id, consumer_id);
                let close = CommandCloseConsumer { consumer_id, request_id, ..Default::default() };
                let close = BaseCommand {
                    r#type: Type::CLOSE_CONSUMER.into(),
                    close_consumer: close.into(),
                    ..Default::default()
                };
                write(world, self.conn, &close);
            }
            Action::GetLastMessageId { segment, generation } => {
                // Without a registration, the broker closed the reader, and the consumer heard it.
                let consumer_id = self.consumer_id(segment, generation)?;
                let request_id = id();
                self.asking.insert(request_id, (segment, generation));
                let get = CommandGetLastMessageId { consumer_id, request_id, ..Default::default() };
                let get = BaseCommand {
                    r#type: Type::GET_LAST_MESSAGE_ID.into(),
                    getLastMessageId: get.into(),
                    ..Default::default()
                };
                write(world, self.conn, &get);
            }
            Action::Subscribed(result) => return Some(Report::Subscribed(result)),
            Action::Closed => return Some(Report::Closed),
        }
        None
    }

    fn consumer_id(&self, segment: u64, generation: u64) -> Option<u64> {
        self.registered.iter().find(|(_, v)| **v == (segment, generation)).map(|(id, _)| *id)
    }

    /// Feeds the consumer what the world sent on the connection: what answers what the driver wrote on it, or concerns
    /// a reader or the registration there. Returns the other commands: the layouts of a watch.
    fn answer(&mut self, consumer: &mut CheckpointConsumer, output: &[u8], now: Instant) -> Vec<BaseCommand> {
        let decoder = FrameDecoder::default();
        let mut buf = BytesMut::from(output);
        let mut others = Vec::new();
        while let Some(frame) = decoder.decode(&mut buf).expect("the world sent a bad frame") {
            let command = BaseCommand::decode_from_slice(&frame.command).expect("undecodable command");
            let EnumValue::Known(kind) = command.r#type else { panic!("unknown command {command:?}") };
            match kind {
                Type::SUCCESS => {
                    let request = command.success.request_id;
                    if let Some((segment, generation, consumer_id)) = self.subscribing.remove(&request) {
                        self.registered.insert(consumer_id, (segment, generation));
                        consumer.on_subscribed(segment, generation, consumer_id);
                    } else if let Some(generation) = self.unregistering.remove(&request) {
                        self.session = None;
                        consumer.on_unregistered(generation);
                    } else {
                        let consumer_id = self.closing.remove(&request).expect("an answer to a close");
                        // The broker may have closed it meanwhile.
                        let Some((segment, generation)) = self.registered.remove(&consumer_id) else { continue };
                        consumer.on_closed(segment, generation, now);
                    }
                }
                Type::ERROR => {
                    let error = &command.error;
                    if let Some((segment, generation)) = self.asking.remove(&error.request_id) {
                        consumer.on_last_message_id_failed(segment, generation, now);
                        continue;
                    }
                    let (segment, generation, _) =
                        self.subscribing.remove(&error.request_id).expect("an answer to a subscribe");
                    let refused = RegisterError::Server { error: error.error, message: error.message.clone() };
                    consumer.on_subscribe_failed(segment, generation, refused, now);
                }
                Type::GET_LAST_MESSAGE_ID_RESPONSE => {
                    let response = &command.getLastMessageIdResponse;
                    let (segment, generation) = self.asking.remove(&response.request_id).expect("an asked id");
                    let id = EntryId::from_proto(&response.last_message_id);
                    consumer.on_last_message_id(segment, generation, id, now);
                }
                Type::MESSAGE => {
                    let (segment, generation) = self.registered[&command.message.consumer_id];
                    let section = frame.payload.as_ref().expect("a message without a payload");
                    consumer.on_message(segment, generation, &command.message, section);
                }
                Type::REACHED_END_OF_TOPIC => {
                    let (segment, generation) = self.registered[&command.reachedEndOfTopic.consumer_id];
                    consumer.on_reached_end_of_topic(segment, generation);
                }
                // The broker closed the reader: an unload, a deleted segment.
                Type::CLOSE_CONSUMER => {
                    let (segment, generation) = self.registered.remove(&command.close_consumer.consumer_id).unwrap();
                    consumer.on_disconnected(segment, generation, now);
                }
                Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE => {
                    let response = &command.scalableTopicSubscribeResponse;
                    let (generation, consumer_id) =
                        self.registering.remove(&response.request_id).expect("an answer to a registration");
                    match response.error {
                        Some(error) => {
                            let message = response.message.clone().unwrap_or_default();
                            consumer.on_register_failed(generation, RegisterError::Server { error, message }, now);
                        }
                        None => {
                            self.session = Some((generation, consumer_id));
                            consumer.on_registered(generation, &response.assignment, now);
                        }
                    }
                }
                Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE => {
                    let update = &command.scalableTopicAssignmentUpdate;
                    let (generation, consumer_id) = self.session.expect("a registration");
                    assert_eq!(update.consumer_id, consumer_id, "an update for another registration");
                    consumer.on_assignment(generation, &update.assignment, now);
                }
                _ => others.push(command),
            }
        }
        others
    }

    /// The connection closed, and the driver moved to `conn`: what was registered, subscribing or asked on the old one
    /// is gone with it, a reader being closed and the registration included.
    fn lost(&mut self, conn: ConnId, consumer: &mut CheckpointConsumer, now: Instant) {
        self.conn = conn;
        self.closing.clear();
        for (_, (segment, generation)) in std::mem::take(&mut self.registered) {
            consumer.on_disconnected(segment, generation, now);
        }
        for (_, (segment, generation, _)) in std::mem::take(&mut self.subscribing) {
            let lost = RegisterError::Unavailable("disconnected".into());
            consumer.on_subscribe_failed(segment, generation, lost, now);
        }
        for (_, (segment, generation)) in std::mem::take(&mut self.asking) {
            consumer.on_last_message_id_failed(segment, generation, now);
        }
        for (_, (generation, _)) in std::mem::take(&mut self.registering) {
            consumer.on_register_failed(generation, RegisterError::Unavailable("disconnected".into()), now);
        }
        if let Some((generation, _)) = self.session.take() {
            consumer.on_session_lost(generation, now);
        }
        for (_, generation) in std::mem::take(&mut self.unregistering) {
            consumer.on_unregistered(generation);
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

/// The messages' values, as text.
pub fn values(messages: &[ReceivedMessage]) -> Vec<String> {
    messages.iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}
