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

//! Test support for `apache-pulsar`.
//!
//! The simulated broker answers the client's protocol state machines over in-memory byte buffers, on a clock the
//! test moves by hand: no sockets, no sleeps, no threads (docs/8-quality.md §2). Tests script what it answers,
//! and when it stays silent.

use std::collections::HashMap;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::connection::{ConnectOptions, Connection, Event};
use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
use apache_pulsar_protocol::lookup::{BrokerAddress, HostPort, Lookup, LookupError, LookupStep};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_lookup_topic_response::LookupType;
use apache_pulsar_protocol::proto::{
    AuthData, BaseCommand, CommandAuthChallenge, CommandConnected, CommandError, CommandLookupTopicResponse,
    CommandPing, CommandPong, CommandSuccess, FeatureFlags, ServerError,
};
use buffa::{EnumValue, Message};
use bytes::{Bytes, BytesMut};

/// A clock that moves only when the test says so.
#[derive(Debug, Clone, Copy)]
pub struct Clock {
    now: Instant,
}

impl Default for Clock {
    fn default() -> Self {
        Self { now: Instant::now() }
    }
}

impl Clock {
    pub fn now(&self) -> Instant {
        self.now
    }

    pub fn advance(&mut self, by: Duration) -> Instant {
        self.now += by;
        self.now
    }
}

/// Encodes a command as a frame.
pub fn frame(command: &BaseCommand) -> Bytes {
    let mut buf = BytesMut::new();
    encode_command(command, &mut buf).unwrap();
    buf.freeze()
}

/// Decodes every complete frame in `bytes`, as owned commands.
pub fn decode_all(bytes: &[u8]) -> Vec<BaseCommand> {
    let mut buf = BytesMut::from(bytes);
    let decoder = FrameDecoder::default();
    let mut out = Vec::new();
    while let Some(frame) = decoder.decode(&mut buf).expect("client sent an invalid frame") {
        out.push(BaseCommand::decode_from_slice(&frame.command).expect("client sent an undecodable command"));
    }
    assert!(buf.is_empty(), "client sent a partial frame");
    out
}

/// Builders for the commands a broker sends.
pub mod producer;
pub mod scalable;

pub mod reply {
    use super::*;

    pub fn connected(features: FeatureFlags) -> BaseCommand {
        BaseCommand {
            r#type: Type::CONNECTED.into(),
            connected: CommandConnected {
                server_version: "Pulsar Server (simulated)".into(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: features.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    pub fn ping() -> BaseCommand {
        BaseCommand { r#type: Type::PING.into(), ping: CommandPing::default().into(), ..Default::default() }
    }

    pub fn pong() -> BaseCommand {
        BaseCommand { r#type: Type::PONG.into(), pong: CommandPong::default().into(), ..Default::default() }
    }

    pub fn success(request_id: u64) -> BaseCommand {
        BaseCommand {
            r#type: Type::SUCCESS.into(),
            success: CommandSuccess { request_id, ..Default::default() }.into(),
            ..Default::default()
        }
    }

    pub fn error(request_id: u64, error: ServerError, message: &str) -> BaseCommand {
        BaseCommand {
            r#type: Type::ERROR.into(),
            error: CommandError { request_id, error, message: message.into(), ..Default::default() }.into(),
            ..Default::default()
        }
    }

    pub fn auth_challenge(data: &[u8]) -> BaseCommand {
        BaseCommand {
            r#type: Type::AUTH_CHALLENGE.into(),
            authChallenge: CommandAuthChallenge {
                server_version: Some("Pulsar Server (simulated)".into()),
                challenge: AuthData {
                    auth_method_name: Some("token".into()),
                    auth_data: Some(data.to_vec()),
                    ..Default::default()
                }
                .into(),
                protocol_version: Some(21),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    /// A lookup answer. `url` is used for both the plaintext and the TLS broker URL, with the scheme swapped.
    pub fn lookup(request_id: u64, answer: &LookupAnswer) -> BaseCommand {
        let mut response = CommandLookupTopicResponse { request_id, ..Default::default() };
        match answer {
            LookupAnswer::Connect { broker, proxy } => {
                response.response = Some(LookupType::Connect);
                response.brokerServiceUrl = Some(format!("pulsar://{broker}"));
                response.brokerServiceUrlTls = Some(format!("pulsar+ssl://{broker}"));
                response.proxy_through_service_url = Some(*proxy);
                response.authoritative = Some(true);
            }
            LookupAnswer::Redirect { broker, authoritative } => {
                response.response = Some(LookupType::Redirect);
                response.brokerServiceUrl = Some(format!("pulsar://{broker}"));
                response.brokerServiceUrlTls = Some(format!("pulsar+ssl://{broker}"));
                response.authoritative = Some(*authoritative);
            }
            LookupAnswer::Fail { error, message } => {
                response.response = Some(LookupType::Failed);
                response.error = Some(*error);
                response.message = Some(message.clone());
            }
            LookupAnswer::Silent => unreachable!("a silent answer sends nothing"),
        }
        BaseCommand { r#type: Type::LOOKUP_RESPONSE.into(), lookupTopicResponse: response.into(), ..Default::default() }
    }
}

/// How a simulated broker answers a lookup.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupAnswer {
    Connect {
        broker: HostPort,
        proxy: bool,
    },
    Redirect {
        broker: HostPort,
        authoritative: bool,
    },
    Fail {
        error: ServerError,
        message: String,
    },
    /// Never answer.
    Silent,
}

/// Answers a command with the commands to send back.
pub type Responder = Box<dyn FnMut(&BaseCommand) -> Vec<BaseCommand>>;

/// A scripted broker.
///
/// It answers `CONNECT` with `CONNECTED` and its feature flags, `PING` with `PONG`, lookups from its lookup
/// table, and anything else through [`SimBroker::on_command`], if set. Everything the client sent is kept in
/// [`SimBroker::received`].
pub struct SimBroker {
    pub features: FeatureFlags,
    /// What to answer to a lookup of each topic. Topics missing here get [`SimBroker::default_lookup`].
    pub lookups: HashMap<String, LookupAnswer>,
    pub default_lookup: LookupAnswer,
    /// Refuse the connection with this error instead of `CONNECTED`.
    pub refuse: Option<ServerError>,
    /// Don't answer `CONNECT` or `PING`.
    pub mute: bool,
    /// Don't answer `PING`, while answering everything else.
    pub ignore_pings: bool,
    /// Answers to other commands.
    pub on_command: Option<Responder>,
    pub received: Vec<BaseCommand>,
}

impl std::fmt::Debug for SimBroker {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("SimBroker").field("features", &self.features).field("received", &self.received.len()).finish()
    }
}

impl SimBroker {
    /// A broker with scalable topics.
    pub fn new() -> Self {
        Self::with_features(FeatureFlags {
            supports_scalable_topics: Some(true),
            supports_auth_refresh: Some(true),
            supports_topic_watchers: Some(true),
            ..Default::default()
        })
    }

    pub fn with_features(features: FeatureFlags) -> Self {
        Self {
            features,
            lookups: HashMap::new(),
            default_lookup: LookupAnswer::Fail { error: ServerError::TopicNotFound, message: "not found".into() },
            refuse: None,
            mute: false,
            ignore_pings: false,
            on_command: None,
            received: Vec::new(),
        }
    }

    /// Takes the client's bytes and returns the broker's answer.
    pub fn handle(&mut self, bytes: &[u8]) -> Bytes {
        let mut out = BytesMut::new();
        for command in decode_all(bytes) {
            for answer in self.answer(&command) {
                encode_command(&answer, &mut out).unwrap();
            }
            self.received.push(command);
        }
        out.freeze()
    }

    /// Takes the complete frames at the front of `buf`, as read from a socket, and returns the broker's answer.
    /// A partial frame stays in `buf` for the next read.
    pub fn feed(&mut self, buf: &mut BytesMut) -> Bytes {
        let decoder = FrameDecoder::default();
        let mut out = BytesMut::new();
        while let Some(frame) = decoder.decode(buf).expect("client sent an invalid frame") {
            let command = BaseCommand::decode_from_slice(&frame.command).expect("client sent an undecodable command");
            for answer in self.answer(&command) {
                encode_command(&answer, &mut out).unwrap();
            }
            self.received.push(command);
        }
        out.freeze()
    }

    fn answer(&mut self, command: &BaseCommand) -> Vec<BaseCommand> {
        match command.r#type {
            EnumValue::Known(Type::CONNECT) if self.mute => vec![],
            EnumValue::Known(Type::CONNECT) => match self.refuse {
                Some(error) => vec![reply::error(0, error, "refused")],
                None => vec![reply::connected(self.features.clone())],
            },
            EnumValue::Known(Type::PING) if self.mute || self.ignore_pings => vec![],
            EnumValue::Known(Type::PING) => vec![reply::pong()],
            EnumValue::Known(Type::PONG) => vec![],
            EnumValue::Known(Type::LOOKUP) => {
                let request = command.lookupTopic.as_option().expect("lookup without its body");
                let answer = self.lookups.get(&request.topic).unwrap_or(&self.default_lookup);
                match answer {
                    LookupAnswer::Silent => vec![],
                    answer => vec![reply::lookup(request.request_id, answer)],
                }
            }
            _ => match &mut self.on_command {
                Some(on_command) => on_command(command),
                None => vec![],
            },
        }
    }

    /// The commands of a type the client sent.
    pub fn received_of(&self, kind: Type) -> Vec<&BaseCommand> {
        self.received.iter().filter(|c| c.r#type == EnumValue::Known(kind)).collect()
    }
}

impl Default for SimBroker {
    fn default() -> Self {
        Self::new()
    }
}

/// A client connection wired to a simulated broker.
#[derive(Debug)]
pub struct Link {
    pub client: Connection,
    pub broker: SimBroker,
    pub clock: Clock,
}

impl Link {
    pub fn new(options: ConnectOptions, broker: SimBroker) -> Self {
        let clock = Clock::default();
        Self { client: Connection::new(options, clock.now()), broker, clock }
    }

    /// Delivers bytes both ways until neither side has anything more to say, and returns the client's events.
    pub fn pump(&mut self) -> Vec<Event> {
        let mut events = Vec::new();
        loop {
            let mut progressed = false;
            while let Some(bytes) = self.client.poll_transmit() {
                let answer = self.broker.handle(&bytes);
                if !answer.is_empty() {
                    self.client.handle_input(&answer, self.clock.now());
                }
                progressed = true;
            }
            while let Some(event) = self.client.poll_event() {
                events.push(event);
                progressed = true;
            }
            if !progressed {
                return events;
            }
        }
    }

    /// Sends bytes from the broker to the client, as if it pushed them, then pumps.
    pub fn push(&mut self, command: &BaseCommand) -> Vec<Event> {
        self.client.handle_input(&frame(command), self.clock.now());
        self.pump()
    }

    /// Moves the clock, runs the client's timers, then pumps.
    pub fn advance(&mut self, by: Duration) -> Vec<Event> {
        let now = self.clock.advance(by);
        if self.client.poll_timeout().is_some_and(|t| t <= now) {
            self.client.handle_timeout(now);
        }
        self.pump()
    }
}

/// Several simulated brokers by address, for lookups across redirects and proxies.
#[derive(Debug, Default)]
pub struct SimCluster {
    pub brokers: HashMap<HostPort, SimBroker>,
}

impl SimCluster {
    pub fn add(&mut self, address: HostPort, broker: SimBroker) {
        self.brokers.insert(address, broker);
    }

    /// Runs a lookup to the end, connecting to each physical address it asks for, the way the client's driver
    /// does. Every connection sends `CONNECT` with the `proxy_to_broker_url` of its address.
    pub fn run_lookup(&mut self, mut lookup: Lookup) -> Result<BrokerAddress, LookupError> {
        let mut target = lookup.target().clone();
        loop {
            let broker =
                self.brokers.remove(&target.physical).unwrap_or_else(|| panic!("no broker at {}", target.physical));
            let options = ConnectOptions {
                proxy_to_broker_url: target.through_proxy().then(|| target.logical.to_string()),
                ..Default::default()
            };
            let mut link = Link::new(options, broker);
            link.pump();
            let request_id =
                link.client.request(link.clock.now(), |id| lookup.command(id)).expect("connection not ready");
            let events = link.pump();
            self.brokers.insert(target.physical.clone(), link.broker);
            let frame = events
                .into_iter()
                .find_map(|event| match event {
                    Event::Response { request_id: id, result: Ok(frame) } if id == request_id => Some(frame),
                    _ => None,
                })
                .expect("no lookup response");
            match lookup.on_response(&frame)? {
                LookupStep::Ask(next) => target = next,
                LookupStep::Found(address) => return Ok(address),
            }
        }
    }
}

/// `host:port`.
pub fn hp(s: &str) -> HostPort {
    let (host, port) = s.rsplit_once(':').unwrap();
    HostPort { host: host.to_string(), port: port.parse().unwrap() }
}
