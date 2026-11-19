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
//! The driver feeds it the bytes it reads ([`Connection::handle_input`]), writes out what it hands back
//! ([`Connection::poll_transmit`]) and reacts to its events ([`Connection::poll_event`]). It never touches a
//! socket or a clock.
//!
//! So far this covers the handshake and keepalive replies: `CONNECT`, then `CONNECTED` or an error, and `PONG`
//! for every `PING`. Requests, id routing, authentication challenges and feature gating come next (milestone 1).

use std::collections::VecDeque;

use buffa::EnumValue;
use bytes::{Bytes, BytesMut};

use crate::frame::{Frame, FrameDecoder, FrameError, encode_command};
use crate::proto::__buffa::view::BaseCommandView;
use crate::proto::base_command::Type;
use crate::proto::{BaseCommand, CommandConnect, CommandPong, FeatureFlags, ServerError};

/// The protocol version this client implements.
pub const PROTOCOL_VERSION: i32 = 21;

/// What the client sends in `CONNECT`.
#[derive(Debug, Clone)]
pub struct ConnectOptions {
    /// Reported to the broker as the client version, for example `Pulsar-Rust-v0.1.0`.
    pub client_version: String,
    /// The authentication method name and its initial data, if any.
    pub auth: Option<(String, Vec<u8>)>,
    /// The broker to reach through a Pulsar proxy (docs/4-connections.md §2).
    pub proxy_to_broker_url: Option<String>,
}

impl Default for ConnectOptions {
    fn default() -> Self {
        Self {
            client_version: concat!("Pulsar-Rust-v", env!("CARGO_PKG_VERSION")).to_string(),
            auth: None,
            proxy_to_broker_url: None,
        }
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

/// What happened on the connection.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Event {
    /// The handshake completed.
    Connected(BrokerInfo),
    /// The connection can't be used any more. The driver closes the socket.
    Closed(ConnectionError),
}

/// Why a connection closed.
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
}

/// One connection to a broker or proxy.
#[derive(Debug)]
pub struct Connection {
    state: State,
    decoder: FrameDecoder,
    read_buf: BytesMut,
    write_buf: BytesMut,
    events: VecDeque<Event>,
    broker: Option<BrokerInfo>,
}

impl Connection {
    /// A connection that starts by sending `CONNECT`.
    pub fn new(options: &ConnectOptions) -> Self {
        let mut connection = Self {
            state: State::AwaitConnected,
            decoder: FrameDecoder::default(),
            read_buf: BytesMut::new(),
            write_buf: BytesMut::new(),
            events: VecDeque::new(),
            broker: None,
        };
        connection.send(&connect_command(options));
        connection
    }

    pub fn state(&self) -> State {
        self.state
    }

    /// The broker's handshake answer, once connected.
    pub fn broker(&self) -> Option<&BrokerInfo> {
        self.broker.as_ref()
    }

    /// Feeds bytes read from the socket.
    pub fn handle_input(&mut self, data: &[u8]) {
        if self.state == State::Closed {
            return;
        }
        self.read_buf.extend_from_slice(data);
        loop {
            match self.decoder.decode(&mut self.read_buf) {
                Ok(Some(frame)) => {
                    if let Err(error) = self.handle_frame(&frame) {
                        self.close(error);
                    }
                }
                Ok(None) => return,
                Err(error) => self.close(error.into()),
            }
            if self.state == State::Closed {
                return;
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

    fn handle_frame(&mut self, frame: &Frame) -> Result<(), ConnectionError> {
        let command = frame.command_view().map_err(|e| ConnectionError::Decode(e.to_string()))?;
        match command.r#type {
            EnumValue::Known(Type::PING) => {
                self.send(&BaseCommand {
                    r#type: Type::PONG.into(),
                    pong: CommandPong::default().into(),
                    ..Default::default()
                });
                Ok(())
            }
            EnumValue::Known(Type::PONG) => Ok(()),
            EnumValue::Known(Type::CONNECTED) if self.state == State::AwaitConnected => {
                let info = broker_info(&command);
                if let Some(max) = info.max_message_size {
                    self.decoder.set_max_message_size(max);
                }
                self.state = State::Ready;
                self.broker = Some(info.clone());
                self.events.push_back(Event::Connected(info));
                Ok(())
            }
            EnumValue::Known(Type::ERROR) if self.state == State::AwaitConnected => {
                let error = command.error.as_option().map(|e| (e.error, e.message.to_string()));
                let (error, message) = error.unwrap_or((ServerError::UnknownError, String::new()));
                Err(ConnectionError::Refused { error: EnumValue::Known(error), message })
            }
            other if self.state == State::AwaitConnected => Err(ConnectionError::Unexpected(other)),
            // Everything after the handshake is milestone 1.
            _ => Ok(()),
        }
    }

    fn send(&mut self, command: &BaseCommand) {
        // A command built by this crate is far below the frame limits.
        encode_command(command, &mut self.write_buf).expect("command frame");
    }

    fn close(&mut self, error: ConnectionError) {
        self.state = State::Closed;
        self.events.push_back(Event::Closed(error));
    }
}

fn connect_command(options: &ConnectOptions) -> BaseCommand {
    let (auth_method_name, auth_data) = match &options.auth {
        Some((method, data)) => (Some(method.clone()), Some(data.clone())),
        None => (None, None),
    };
    BaseCommand {
        r#type: Type::CONNECT.into(),
        connect: CommandConnect {
            client_version: options.client_version.clone(),
            auth_method_name,
            auth_data,
            protocol_version: Some(PROTOCOL_VERSION),
            proxy_to_broker_url: options.proxy_to_broker_url.clone(),
            // Each flag is turned on with the feature that honors it: authentication refresh and partial
            // producers come with milestones 1 and 2.
            feature_flags: FeatureFlags { supports_broker_entry_metadata: Some(true), ..Default::default() }.into(),
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{CommandConnected, CommandError, CommandPing};

    fn broker_frame(command: &BaseCommand) -> BytesMut {
        let mut buf = BytesMut::new();
        encode_command(command, &mut buf).unwrap();
        buf
    }

    fn connected(scalable: bool) -> BaseCommand {
        BaseCommand {
            r#type: Type::CONNECTED.into(),
            connected: CommandConnected {
                server_version: "Pulsar Server5.0.1".into(),
                protocol_version: Some(21),
                max_message_size: Some(5 * 1024 * 1024),
                feature_flags: FeatureFlags { supports_scalable_topics: Some(scalable), ..Default::default() }.into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    fn sent_commands(connection: &mut Connection) -> Vec<BaseCommand> {
        use buffa::Message;
        let mut bytes = BytesMut::from(&connection.poll_transmit().unwrap_or_default()[..]);
        let mut out = Vec::new();
        while let Some(frame) = FrameDecoder::default().decode(&mut bytes).unwrap() {
            out.push(BaseCommand::decode_from_slice(&frame.command).unwrap());
        }
        out
    }

    #[test]
    fn handshake() {
        let mut connection = Connection::new(&ConnectOptions::default());
        let sent = sent_commands(&mut connection);
        assert_eq!(sent.len(), 1);
        let connect = sent[0].connect.as_option().unwrap();
        assert_eq!(sent[0].r#type, EnumValue::Known(Type::CONNECT));
        assert_eq!(connect.protocol_version, Some(PROTOCOL_VERSION));
        assert!(connect.client_version.starts_with("Pulsar-Rust-v"));

        // The answer arrives in two reads.
        let frame = broker_frame(&connected(true));
        connection.handle_input(&frame[..3]);
        assert_eq!(connection.poll_event(), None);
        connection.handle_input(&frame[3..]);
        let Some(Event::Connected(info)) = connection.poll_event() else { panic!("not connected") };
        assert!(info.features.scalable_topics);
        assert_eq!(info.protocol_version, 21);
        assert_eq!(connection.state(), State::Ready);
    }

    #[test]
    fn ping_gets_a_pong() {
        let mut connection = Connection::new(&ConnectOptions::default());
        sent_commands(&mut connection);
        connection.handle_input(&broker_frame(&connected(false)));
        let ping = BaseCommand { r#type: Type::PING.into(), ping: CommandPing::default().into(), ..Default::default() };
        connection.handle_input(&broker_frame(&ping));
        let sent = sent_commands(&mut connection);
        assert_eq!(sent.len(), 1);
        assert_eq!(sent[0].r#type, EnumValue::Known(Type::PONG));
    }

    #[test]
    fn refused_connection_closes() {
        let mut connection = Connection::new(&ConnectOptions::default());
        let error = BaseCommand {
            r#type: Type::ERROR.into(),
            error: CommandError {
                request_id: 0,
                error: ServerError::AuthenticationError,
                message: "no".into(),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        connection.handle_input(&broker_frame(&error));
        assert!(matches!(connection.poll_event(), Some(Event::Closed(ConnectionError::Refused { .. }))));
        assert_eq!(connection.state(), State::Closed);
    }
}
