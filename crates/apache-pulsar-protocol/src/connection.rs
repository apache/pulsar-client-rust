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

//! The connection state machine, sans IO (docs/3-protocol.md §4).
//!
//! The driver feeds it the bytes it reads ([`Connection::handle_input`]) and the passing of time
//! ([`Connection::handle_timeout`] once [`Connection::poll_timeout`] is reached), writes out what
//! [`Connection::poll_transmit`] hands back, and reacts to [`Connection::poll_event`]. It never touches a socket
//! or a clock: every input carries `now`.
//!
//! It covers the handshake, request deadlines, keepalive, authentication challenges and feature gating. Pushed
//! commands come out as [`Event::Command`] for the layer above to route by producer, consumer, session or watch
//! id.

use std::collections::{BTreeSet, HashMap, VecDeque};
use std::fmt;
use std::time::{Duration, Instant};

use buffa::EnumValue;
use bytes::{Bytes, BytesMut};

use crate::frame::{Frame, FrameDecoder, FrameError, encode_command, encode_payload_command};
use crate::proto::__buffa::view::BaseCommandView;
use crate::proto::base_command::Type;
use crate::proto::{
    AuthData, BaseCommand, CommandAuthResponse, CommandConnect, CommandPing, CommandPong, FeatureFlags,
    MessageMetadata, ServerError,
};

/// The protocol version this client implements.
pub const PROTOCOL_VERSION: i32 = 21;

/// The data a broker sends in an auth challenge to ask the client to refresh its credentials. Java's
/// `AuthData.REFRESH_AUTH_DATA_BYTES`.
pub const AUTH_REFRESH_SENTINEL: &[u8] = b"PulsarAuthRefresh";

/// How many challenge rounds one authentication exchange may take before the connection fails, as in Java.
pub const MAX_AUTH_CHALLENGE_ROUNDS: u32 = 10;

/// What the client sends in `CONNECT`, and the connection's timeouts.
#[derive(Clone)]
pub struct ConnectOptions {
    /// Reported to the broker as the client version, for example `Pulsar-Rust-v0.1.0`.
    pub client_version: String,
    /// The authentication method name.
    pub auth_method: Option<String>,
    /// The initial authentication data. Never printed by `Debug`.
    pub auth_data: Option<Vec<u8>>,
    /// The broker to reach through a Pulsar proxy (docs/4-connections.md §2). An empty string asks the proxy to
    /// pair the connection with any broker.
    pub proxy_to_broker_url: Option<String>,
    /// How long the handshake may take, from creation to `CONNECTED`.
    pub handshake_timeout: Duration,
    /// How long a request may wait for its response.
    pub operation_timeout: Duration,
    /// How often to ping an otherwise idle broker. A ping still unanswered at the next tick closes the
    /// connection.
    pub keepalive_interval: Duration,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            client_version: concat!("Pulsar-Rust-v", env!("CARGO_PKG_VERSION")).to_string(),
            auth_method: None,
            auth_data: None,
            proxy_to_broker_url: None,
            handshake_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
        }
    }
}

impl fmt::Debug for ConnectOptions {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ConnectOptions")
            .field("client_version", &self.client_version)
            .field("auth_method", &self.auth_method)
            .field("auth_data", &self.auth_data.as_ref().map(|_| "<redacted>"))
            .field("proxy_to_broker_url", &self.proxy_to_broker_url)
            .field("handshake_timeout", &self.handshake_timeout)
            .field("operation_timeout", &self.operation_timeout)
            .field("keepalive_interval", &self.keepalive_interval)
            .finish()
    }
}

/// What the broker said about itself in `CONNECTED`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BrokerInfo {
    pub server_version: String,
    pub protocol_version: i32,
    pub max_message_size: Option<u32>,
    pub features: BrokerFeatures,
}

/// The broker's `FeatureFlags` that the client acts on.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct BrokerFeatures {
    pub auth_refresh: bool,
    pub scalable_topics: bool,
    pub tc_metadata_discovery: bool,
    pub topic_watchers: bool,
}

/// Where the connection is.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum State {
    AwaitConnected,
    Ready,
    Closed,
}

/// The kinds of id a connection allocates. Each kind has its own sequence.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum IdKind {
    /// `producer_id`.
    Producer,
    /// `consumer_id`, shared by segment consumers and assignment sessions so that they never collide.
    Consumer,
    /// `session_id` of a layout watch.
    Session,
    /// `watch_id` of a namespace or TC assignment watch.
    Watch,
}

/// What happened on the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The handshake completed.
    Connected(BrokerInfo),
    /// A request completed: its response, or why it failed.
    Response { request_id: u64, result: Result<Frame, RequestError> },
    /// A command the broker pushed, for the producer, consumer, session or watch it names.
    Command(Frame),
    /// The broker asks for authentication data. The driver gets it from the provider, then calls
    /// [`Connection::auth_response`]. `refresh` means the broker asks for fresh credentials, not a continuation
    /// of a multi-round exchange.
    AuthChallenge { refresh: bool, challenge: Bytes },
    /// The connection can't be used any more. Requests still pending have failed with it; the driver closes the
    /// socket.
    Closed(ConnectionError),
}

/// Why a request failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum RequestError {
    #[error("broker error {error:?}: {message}")]
    Server { error: EnumValue<ServerError>, message: String },
    #[error("no response within the operation timeout")]
    Timeout,
}

/// Why a connection closed, or refused a command.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum ConnectionError {
    #[error("broker refused the connection: {error:?}: {message}")]
    Refused { error: EnumValue<ServerError>, message: String },
    #[error(transparent)]
    Frame(#[from] FrameError),
    #[error("undecodable command: {0}")]
    Decode(String),
    #[error("unexpected {0:?} during the handshake")]
    Unexpected(EnumValue<Type>),
    #[error("no CONNECTED within the handshake timeout")]
    HandshakeTimeout,
    #[error("no answer to a keepalive ping")]
    KeepaliveTimeout,
    #[error("authentication exceeded {MAX_AUTH_CHALLENGE_ROUNDS} challenge rounds")]
    TooManyAuthRounds,
    #[error("authentication failed: {0}")]
    Authentication(String),
    #[error("the broker does not support {0}")]
    UnsupportedBroker(&'static str),
    #[error("the connection is not ready")]
    NotReady,
    #[error("the connection is closed")]
    Closed,
    #[error("disconnected: {0}")]
    Disconnected(String),
}

impl ConnectionError {
    /// Whether reconnecting can help. Refusals for authentication or authorization, and an unsupported broker,
    /// won't be fixed by trying again.
    pub fn is_retryable(&self) -> bool {
        match self {
            Self::Refused { error, .. } => !matches!(
                error,
                EnumValue::Known(
                    ServerError::AuthenticationError | ServerError::AuthorizationError | ServerError::NotAllowedError
                )
            ),
            Self::UnsupportedBroker(_) | Self::TooManyAuthRounds | Self::Authentication(_) => false,
            _ => true,
        }
    }
}

/// One connection to a broker or proxy.
#[derive(Debug)]
pub struct Connection {
    options: ConnectOptions,
    state: State,
    decoder: FrameDecoder,
    read_buf: BytesMut,
    write_buf: BytesMut,
    events: VecDeque<Event>,
    broker: Option<BrokerInfo>,
    handshake_deadline: Instant,
    next_request_id: u64,
    pending: HashMap<u64, Instant>,
    deadlines: BTreeSet<(Instant, u64)>,
    ids: HashMap<IdKind, u64>,
    next_keepalive: Option<Instant>,
    awaiting_pong: bool,
    auth_rounds: u32,
    auth_in_progress: bool,
}

impl Connection {
    /// A connection that starts by sending `CONNECT`.
    pub fn new(options: ConnectOptions, now: Instant) -> Self {
        let mut connection = Self {
            handshake_deadline: now + options.handshake_timeout,
            options,
            state: State::AwaitConnected,
            decoder: FrameDecoder::default(),
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            events: VecDeque::new(),
            broker: None,
            next_request_id: 0,
            pending: HashMap::new(),
            deadlines: BTreeSet::new(),
            ids: HashMap::new(),
            next_keepalive: None,
            awaiting_pong: false,
            auth_rounds: 0,
            auth_in_progress: false,
        };
        let connect = connect_command(&connection.options);
        connection.write(&connect);
        connection
    }

    pub fn state(&self) -> State {
        self.state
    }

    /// The broker's handshake answer, once connected.
    pub fn broker(&self) -> Option<&BrokerInfo> {
        self.broker.as_ref()
    }

    /// The number of requests waiting for a response.
    pub fn pending_requests(&self) -> usize {
        self.pending.len()
    }

    /// A new id of the given kind, unique on this connection.
    pub fn allocate_id(&mut self, kind: IdKind) -> u64 {
        let next = self.ids.entry(kind).or_insert(0);
        let id = *next;
        *next += 1;
        id
    }

    /// Sends a request. `build` makes the command from the request id it is given. The response, or the failure,
    /// comes back as [`Event::Response`] with that id.
    pub fn request(&mut self, now: Instant, build: impl FnOnce(u64) -> BaseCommand) -> Result<u64, ConnectionError> {
        self.check_ready()?;
        let request_id = self.next_request_id;
        let command = build(request_id);
        self.check_feature(&command)?;
        self.next_request_id += 1;
        let deadline = now + self.options.operation_timeout;
        self.pending.insert(request_id, deadline);
        self.deadlines.insert((deadline, request_id));
        self.write(&command);
        Ok(request_id)
    }

    /// Sends a command that has no response, such as a flow permit or an ack.
    pub fn send(&mut self, command: &BaseCommand) -> Result<(), ConnectionError> {
        self.check_ready()?;
        self.check_feature(command)?;
        self.write(command);
        Ok(())
    }

    /// Sends a payload command: a `CommandSend` with its metadata and payload.
    pub fn send_payload(
        &mut self,
        command: &BaseCommand,
        metadata: &MessageMetadata,
        payload: &[u8],
    ) -> Result<(), ConnectionError> {
        self.check_ready()?;
        encode_payload_command(command, metadata, payload, &mut self.write_buf)?;
        Ok(())
    }

    /// Answers an [`Event::AuthChallenge`] with the provider's data.
    pub fn auth_response(&mut self, auth_data: Vec<u8>) {
        if self.state == State::Closed {
            return;
        }
        self.auth_in_progress = false;
        let response = BaseCommand {
            r#type: Type::AUTH_RESPONSE.into(),
            authResponse: CommandAuthResponse {
                client_version: Some(self.options.client_version.clone()),
                response: AuthData {
                    auth_method_name: self.options.auth_method.clone(),
                    auth_data: Some(auth_data),
                    ..Default::default()
                }
                .into(),
                protocol_version: Some(PROTOCOL_VERSION),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        self.write(&response);
    }

    /// Fails the connection because the authentication provider could not answer a challenge.
    pub fn auth_failed(&mut self, reason: String) {
        self.close(ConnectionError::Authentication(reason));
    }

    /// Closes the connection locally. Pending requests fail with [`ConnectionError::Closed`].
    pub fn close(&mut self, error: ConnectionError) {
        if self.state == State::Closed {
            return;
        }
        self.state = State::Closed;
        self.pending.clear();
        self.deadlines.clear();
        self.events.push_back(Event::Closed(error));
    }

    /// Feeds bytes read from the socket.
    pub fn handle_input(&mut self, data: &[u8], now: Instant) {
        if self.state == State::Closed {
            return;
        }
        self.read_buf.extend_from_slice(data);
        while self.state != State::Closed {
            match self.decoder.decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    self.awaiting_pong = false;
                    if let Err(error) = self.handle_frame(frame, now) {
                        self.close(error);
                    }
                }
                Ok(None) => return,
                Err(error) => self.close(error.into()),
            }
        }
    }

    /// The bytes to write to the socket next, if any.
    pub fn poll_transmit(&mut self) -> Option<Bytes> {
        (!self.write_buf.is_empty()).then(|| self.write_buf.split().freeze())
    }

    /// The next event, if any.
    pub fn poll_event(&mut self) -> Option<Event> {
        self.events.pop_front()
    }

    /// When [`Connection::handle_timeout`] must be called next, if ever.
    pub fn poll_timeout(&self) -> Option<Instant> {
        match self.state {
            State::Closed => None,
            State::AwaitConnected => Some(self.handshake_deadline),
            State::Ready => {
                let request = self.deadlines.first().map(|(deadline, _)| *deadline);
                match (request, self.next_keepalive) {
                    (Some(a), Some(b)) => Some(a.min(b)),
                    (a, b) => a.or(b),
                }
            }
        }
    }

    /// Lets time pass: fails the requests whose deadline is reached, and runs the keepalive.
    pub fn handle_timeout(&mut self, now: Instant) {
        match self.state {
            State::Closed => {}
            State::AwaitConnected => {
                if now >= self.handshake_deadline {
                    self.close(ConnectionError::HandshakeTimeout);
                }
            }
            State::Ready => {
                while let Some(&(deadline, request_id)) = self.deadlines.first() {
                    if deadline > now {
                        break;
                    }
                    self.deadlines.pop_first();
                    self.pending.remove(&request_id);
                    self.events.push_back(Event::Response { request_id, result: Err(RequestError::Timeout) });
                }
                if self.next_keepalive.is_some_and(|tick| tick <= now) {
                    if self.awaiting_pong {
                        self.close(ConnectionError::KeepaliveTimeout);
                        return;
                    }
                    self.awaiting_pong = true;
                    self.next_keepalive = Some(now + self.options.keepalive_interval);
                    let ping = BaseCommand {
                        r#type: Type::PING.into(),
                        ping: CommandPing::default().into(),
                        ..Default::default()
                    };
                    self.write(&ping);
                }
            }
        }
    }

    fn check_ready(&self) -> Result<(), ConnectionError> {
        match self.state {
            State::Ready => Ok(()),
            State::AwaitConnected => Err(ConnectionError::NotReady),
            State::Closed => Err(ConnectionError::Closed),
        }
    }

    /// C15: scalable-topic commands go only to a broker that advertises them.
    fn check_feature(&self, command: &BaseCommand) -> Result<(), ConnectionError> {
        let features = self.broker.as_ref().map(|b| b.features).unwrap_or_default();
        let scalable = matches!(
            command.r#type,
            EnumValue::Known(
                Type::SCALABLE_TOPIC_LOOKUP
                    | Type::SCALABLE_TOPIC_CLOSE
                    | Type::SCALABLE_TOPIC_SUBSCRIBE
                    | Type::SCALABLE_TOPIC_UNSUBSCRIBE
                    | Type::WATCH_SCALABLE_TOPICS
                    | Type::WATCH_SCALABLE_TOPICS_CLOSE
            )
        );
        if scalable && !features.scalable_topics {
            return Err(ConnectionError::UnsupportedBroker("scalable topics"));
        }
        let tc_watch =
            matches!(command.r#type, EnumValue::Known(Type::WATCH_TC_ASSIGNMENTS | Type::WATCH_TC_ASSIGNMENTS_CLOSE));
        if tc_watch && !features.tc_metadata_discovery {
            return Err(ConnectionError::UnsupportedBroker("transaction coordinator discovery"));
        }
        Ok(())
    }

    fn handle_frame(&mut self, frame: Frame, now: Instant) -> Result<(), ConnectionError> {
        let command = frame.command_view().map_err(|e| ConnectionError::Decode(e.to_string()))?;
        let kind = command.r#type;
        match kind {
            EnumValue::Known(Type::PING) => {
                let pong = BaseCommand {
                    r#type: Type::PONG.into(),
                    pong: CommandPong::default().into(),
                    ..Default::default()
                };
                self.write(&pong);
                return Ok(());
            }
            EnumValue::Known(Type::PONG) => return Ok(()),
            EnumValue::Known(Type::AUTH_CHALLENGE) => {
                let challenge = command.authChallenge.as_option().and_then(|c| c.challenge.as_option());
                let data = challenge.and_then(|c| c.auth_data).unwrap_or_default();
                let refresh = data == AUTH_REFRESH_SENTINEL;
                // The broker never pipelines challenges; one arriving during a round is dropped, unless it asks
                // for a refresh, which starts over.
                if self.auth_in_progress && !refresh {
                    return Ok(());
                }
                if refresh {
                    self.auth_rounds = 0;
                } else {
                    self.auth_rounds += 1;
                    if self.auth_rounds > MAX_AUTH_CHALLENGE_ROUNDS {
                        return Err(ConnectionError::TooManyAuthRounds);
                    }
                }
                self.auth_in_progress = true;
                let challenge = frame.command.slice_ref(data);
                self.events.push_back(Event::AuthChallenge { refresh, challenge });
                return Ok(());
            }
            _ => {}
        }

        if self.state == State::AwaitConnected {
            return match kind {
                EnumValue::Known(Type::CONNECTED) => {
                    let info = broker_info(&command);
                    if let Some(max) = info.max_message_size {
                        self.decoder.set_max_message_size(max);
                    }
                    self.state = State::Ready;
                    self.next_keepalive = Some(now + self.options.keepalive_interval);
                    self.broker = Some(info.clone());
                    self.events.push_back(Event::Connected(info));
                    Ok(())
                }
                EnumValue::Known(Type::ERROR) => {
                    let (error, message) = command
                        .error
                        .as_option()
                        .map(|e| (EnumValue::Known(e.error), e.message.to_string()))
                        .unwrap_or((EnumValue::Known(ServerError::UnknownError), String::new()));
                    Err(ConnectionError::Refused { error, message })
                }
                other => Err(ConnectionError::Unexpected(other)),
            };
        }

        if let Some(request_id) = response_request_id(&command) {
            // A response after its deadline, or to an unknown id, is dropped.
            if let Some(deadline) = self.pending.remove(&request_id) {
                self.deadlines.remove(&(deadline, request_id));
                let result = match command.error.as_option() {
                    Some(error) if kind == EnumValue::Known(Type::ERROR) => Err(RequestError::Server {
                        error: EnumValue::Known(error.error),
                        message: error.message.to_string(),
                    }),
                    _ => Ok(frame.clone()),
                };
                self.events.push_back(Event::Response { request_id, result });
            }
            return Ok(());
        }
        self.events.push_back(Event::Command(frame.clone()));
        Ok(())
    }

    fn write(&mut self, command: &BaseCommand) {
        // A command built by this crate is far below the frame limits.
        encode_command(command, &mut self.write_buf).expect("command frame");
    }
}

/// The request id of a response command, or `None` for a pushed command.
pub fn response_request_id(command: &BaseCommandView<'_>) -> Option<u64> {
    let EnumValue::Known(kind) = command.r#type else { return None };
    macro_rules! id {
        ($field:ident) => {
            command.$field.as_option().map(|c| c.request_id)
        };
    }
    match kind {
        Type::SUCCESS => id!(success),
        Type::ERROR => id!(error),
        Type::PRODUCER_SUCCESS => id!(producer_success),
        Type::LOOKUP_RESPONSE => id!(lookupTopicResponse),
        Type::PARTITIONED_METADATA_RESPONSE => id!(partitionMetadataResponse),
        Type::CONSUMER_STATS_RESPONSE => id!(consumerStatsResponse),
        Type::GET_LAST_MESSAGE_ID_RESPONSE => id!(getLastMessageIdResponse),
        Type::GET_TOPICS_OF_NAMESPACE_RESPONSE => id!(getTopicsOfNamespaceResponse),
        Type::GET_SCHEMA_RESPONSE => id!(getSchemaResponse),
        Type::GET_OR_CREATE_SCHEMA_RESPONSE => id!(getOrCreateSchemaResponse),
        Type::NEW_TXN_RESPONSE => id!(newTxnResponse),
        Type::ADD_PARTITION_TO_TXN_RESPONSE => id!(addPartitionToTxnResponse),
        Type::ADD_SUBSCRIPTION_TO_TXN_RESPONSE => id!(addSubscriptionToTxnResponse),
        Type::END_TXN_RESPONSE => id!(endTxnResponse),
        Type::END_TXN_ON_PARTITION_RESPONSE => id!(endTxnOnPartitionResponse),
        Type::END_TXN_ON_SUBSCRIPTION_RESPONSE => id!(endTxnOnSubscriptionResponse),
        Type::TC_CLIENT_CONNECT_RESPONSE => id!(tcClientConnectResponse),
        Type::WATCH_TOPIC_LIST_SUCCESS => id!(watchTopicListSuccess),
        Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE => id!(scalableTopicSubscribeResponse),
        Type::ACK_RESPONSE => command.ackResponse.as_option().and_then(|c| c.request_id),
        _ => None,
    }
}

fn connect_command(options: &ConnectOptions) -> BaseCommand {
    BaseCommand {
        r#type: Type::CONNECT.into(),
        connect: CommandConnect {
            client_version: options.client_version.clone(),
            auth_method_name: options.auth_method.clone(),
            auth_data: options.auth_data.clone(),
            protocol_version: Some(PROTOCOL_VERSION),
            proxy_to_broker_url: options.proxy_to_broker_url.clone(),
            // Each flag is turned on with the feature that honors it: partial producers come with milestone 2.
            feature_flags: FeatureFlags {
                supports_auth_refresh: Some(true),
                supports_broker_entry_metadata: Some(true),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn broker_info(command: &BaseCommandView<'_>) -> BrokerInfo {
    let connected = command.connected.as_option();
    let flags = connected.and_then(|c| c.feature_flags.as_option());
    let flag = |get: fn(&crate::proto::__buffa::view::FeatureFlagsView<'_>) -> Option<bool>| {
        flags.and_then(get).unwrap_or(false)
    };
    BrokerInfo {
        server_version: connected.map(|c| c.server_version.to_string()).unwrap_or_default(),
        protocol_version: connected.and_then(|c| c.protocol_version).unwrap_or(0),
        max_message_size: connected.and_then(|c| c.max_message_size).and_then(|m| u32::try_from(m).ok()),
        features: BrokerFeatures {
            auth_refresh: flag(|f| f.supports_auth_refresh),
            scalable_topics: flag(|f| f.supports_scalable_topics),
            tc_metadata_discovery: flag(|f| f.supports_tc_metadata_discovery),
            topic_watchers: flag(|f| f.supports_topic_watchers),
        },
    }
}
