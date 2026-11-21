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

//! Service URLs, broker addresses and topic lookup, sans IO (docs/4-connections.md §1–§3).
//!
//! A [`Lookup`] decides where each `CommandLookupTopic` goes and what its answer means; the driver owns the
//! connections it goes over.

use std::fmt;

use buffa::EnumValue;

use crate::frame::Frame;
use crate::proto::command_lookup_topic_response::LookupType;
use crate::proto::{BaseCommand, CommandLookupTopic, ServerError, base_command::Type};

/// Java's `maxLookupRedirects`.
pub const MAX_LOOKUP_REDIRECTS: u32 = 20;

/// A host and port.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct HostPort {
    pub host: String,
    pub port: u16,
}

impl fmt::Display for HostPort {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        if self.host.contains(':') {
            write!(f, "[{}]:{}", self.host, self.port)
        } else {
            write!(f, "{}:{}", self.host, self.port)
        }
    }
}

/// A URL could not be parsed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
#[error("invalid URL {url:?}: {reason}")]
pub struct UrlError {
    pub url: String,
    pub reason: &'static str,
}

/// A `pulsar://` or `pulsar+ssl://` service URL, possibly with several hosts.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct ServiceUrl {
    pub tls: bool,
    pub hosts: Vec<HostPort>,
}

impl ServiceUrl {
    /// Parses `pulsar://host[:port][,host[:port]…]` or `pulsar+ssl://…`. The ports default to 6650 and 6651.
    pub fn parse(url: &str) -> Result<Self, UrlError> {
        let error = |reason| UrlError { url: url.to_string(), reason };
        let (tls, rest) = if let Some(rest) = url.strip_prefix("pulsar://") {
            (false, rest)
        } else if let Some(rest) = url.strip_prefix("pulsar+ssl://") {
            (true, rest)
        } else {
            return Err(error("expected pulsar:// or pulsar+ssl://"));
        };
        let rest = rest.trim_end_matches('/');
        if rest.contains('/') {
            return Err(error("a service URL has no path"));
        }
        let default_port = if tls { 6651 } else { 6650 };
        let hosts = rest
            .split(',')
            .map(|host| parse_host_port(host, default_port).ok_or_else(|| error("bad host or port")))
            .collect::<Result<Vec<_>, _>>()?;
        Ok(Self { tls, hosts })
    }
}

/// Parses one broker URL from a lookup answer: `pulsar://host:port` or `pulsar+ssl://host:port`.
pub fn parse_broker_url(url: &str) -> Result<HostPort, UrlError> {
    let service = ServiceUrl::parse(url)?;
    match <[HostPort; 1]>::try_from(service.hosts) {
        Ok([host]) => Ok(host),
        Err(_) => Err(UrlError { url: url.to_string(), reason: "expected a single host" }),
    }
}

fn parse_host_port(s: &str, default_port: u16) -> Option<HostPort> {
    let (host, port) = if let Some(rest) = s.strip_prefix('[') {
        let (host, after) = rest.split_once(']')?;
        let port = match after.strip_prefix(':') {
            Some(port) => port.parse().ok()?,
            None if after.is_empty() => default_port,
            None => return None,
        };
        (host, port)
    } else {
        match s.rsplit_once(':') {
            Some((host, port)) => (host, port.parse().ok()?),
            None => (s, default_port),
        }
    };
    (!host.is_empty()).then(|| HostPort { host: host.to_string(), port })
}

/// Where a connection goes. `logical` is the broker it talks to; `physical` the host it connects to. They
/// differ only when the broker is reached through a proxy, which `CONNECT` tells where to forward
/// (`proxy_to_broker_url`). The pool is keyed by both.
#[derive(Debug, Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct BrokerAddress {
    pub logical: HostPort,
    pub physical: HostPort,
}

impl BrokerAddress {
    /// A broker connected to directly.
    pub fn direct(host: HostPort) -> Self {
        Self { logical: host.clone(), physical: host }
    }

    /// Any broker behind the proxy at `proxy`: the proxy picks one.
    pub fn behind(proxy: HostPort) -> Self {
        Self { logical: HostPort { host: String::new(), port: 0 }, physical: proxy }
    }

    /// Whether the connection goes through a proxy.
    pub fn through_proxy(&self) -> bool {
        self.logical != self.physical
    }

    /// What `CommandConnect.proxy_to_broker_url` asks the proxy for: the logical broker, or, empty, any broker.
    pub fn proxy_target(&self) -> String {
        if self.logical.host.is_empty() { String::new() } else { self.logical.to_string() }
    }
}

/// Why a lookup failed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LookupError {
    #[error("lookup failed: {error:?}: {message}")]
    Server { error: EnumValue<ServerError>, message: String },
    #[error("more than {MAX_LOOKUP_REDIRECTS} lookup redirects")]
    TooManyRedirects,
    #[error("empty lookup response")]
    EmptyResponse,
    #[error("lookup answer without a {0} broker URL")]
    MissingUrl(&'static str),
    #[error(transparent)]
    Url(#[from] UrlError),
    #[error("undecodable lookup response: {0}")]
    Decode(String),
}

/// What to do after a lookup answer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum LookupStep {
    /// Send [`Lookup::command`] to this broker.
    Ask(BrokerAddress),
    /// The topic is served at this address.
    Found(BrokerAddress),
}

/// One topic lookup, across its redirects.
#[derive(Debug, Clone)]
pub struct Lookup {
    topic: String,
    tls: bool,
    target: BrokerAddress,
    authoritative: bool,
    redirects: u32,
}

impl Lookup {
    /// A lookup of `topic` that starts at `service`, one of the service URL's hosts.
    pub fn new(topic: impl Into<String>, service: HostPort, tls: bool) -> Self {
        Self { topic: topic.into(), tls, target: BrokerAddress::direct(service), authoritative: false, redirects: 0 }
    }

    /// The topic looked up.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// Where the next request goes.
    pub fn target(&self) -> &BrokerAddress {
        &self.target
    }

    /// How many redirects have been followed.
    pub fn redirects(&self) -> u32 {
        self.redirects
    }

    /// The request to send to [`Lookup::target`].
    pub fn command(&self, request_id: u64) -> BaseCommand {
        BaseCommand {
            r#type: Type::LOOKUP.into(),
            lookupTopic: CommandLookupTopic {
                topic: self.topic.clone(),
                request_id,
                authoritative: Some(self.authoritative),
                ..Default::default()
            }
            .into(),
            ..Default::default()
        }
    }

    /// Interprets the answer to [`Lookup::command`], as the Java client does (`BinaryProtoLookupService`).
    pub fn on_response(&mut self, frame: &Frame) -> Result<LookupStep, LookupError> {
        let command = frame.command_view().map_err(|e| LookupError::Decode(e.to_string()))?;
        let response = command.lookupTopicResponse.as_option().ok_or(LookupError::EmptyResponse)?;
        match response.response {
            Some(LookupType::Redirect | LookupType::Connect) => {}
            Some(LookupType::Failed) | None => {
                return Err(match response.error {
                    Some(error) => LookupError::Server {
                        error: EnumValue::Known(error),
                        message: response.message.unwrap_or_default().to_string(),
                    },
                    None => LookupError::EmptyResponse,
                });
            }
        }
        let url = if self.tls {
            response.brokerServiceUrlTls.ok_or(LookupError::MissingUrl("TLS"))?
        } else {
            response.brokerServiceUrl.ok_or(LookupError::MissingUrl("plaintext"))?
        };
        let broker = parse_broker_url(url)?;
        if response.response == Some(LookupType::Redirect) {
            self.redirects += 1;
            if self.redirects > MAX_LOOKUP_REDIRECTS {
                return Err(LookupError::TooManyRedirects);
            }
            self.authoritative = response.authoritative.unwrap_or(false);
            self.target = BrokerAddress::direct(broker);
            return Ok(LookupStep::Ask(self.target.clone()));
        }
        if response.proxy_through_service_url.unwrap_or(false) {
            // Keep connecting where the lookup went, and have that proxy forward to the broker.
            Ok(LookupStep::Found(BrokerAddress { logical: broker, physical: self.target.physical.clone() }))
        } else {
            Ok(LookupStep::Found(BrokerAddress::direct(broker)))
        }
    }
}
