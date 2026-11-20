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

//! Errors (docs/7-api.md §6, *error-model* §2).

use std::fmt;
use std::sync::Arc;

use apache_pulsar_protocol::connection::{ConnectionError, RequestError};
use apache_pulsar_protocol::lookup::{LookupError, UrlError};
use apache_pulsar_protocol::proto::ServerError;
use buffa::EnumValue;

/// The category of an [`Error`]: what the application can act on.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    NotFound,
    Authentication,
    Authorization,
    Timeout,
    AlreadyClosed,
    ProducerBusy,
    ProducerFenced,
    ConsumerBusy,
    IncompatibleSchema,
    TopicTerminated,
    Connect,
    NotConnected,
    ProducerQueueFull,
    MemoryBufferFull,
    /// A message larger than the broker accepts. It fails alone: the producer stays usable.
    MessageTooLarge,
    TransactionConflict,
    Crypto,
    InvalidConfiguration,
    InvalidServiceUrl,
    UnsupportedBroker,
    /// A failure outside the other categories, such as an unexpected broker error.
    Other,
}

/// An error from the client.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

/// `Result` with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    pub fn new(kind: ErrorKind, message: impl Into<String>) -> Self {
        Self { kind, message: message.into(), source: None }
    }

    pub(crate) fn with_source(
        kind: ErrorKind,
        message: impl Into<String>,
        source: impl std::error::Error + Send + Sync + 'static,
    ) -> Self {
        Self { kind, message: message.into(), source: Some(Arc::new(source)) }
    }

    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// Whether trying again may succeed: connection and timeout failures, not configuration or permission ones.
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Connect | ErrorKind::NotConnected | ErrorKind::Timeout)
    }

    pub(crate) fn closed() -> Self {
        Self::new(ErrorKind::AlreadyClosed, "the client is closed")
    }

    /// The broker's error code, when the broker refused the operation.
    pub(crate) fn server_error(&self) -> Option<(ServerError, String)> {
        let source = self.source.as_deref()?;
        let (error, message) = if let Some(RequestError::Server { error, message }) = source.downcast_ref() {
            (*error, message)
        } else if let Some(LookupError::Server { error, message }) = source.downcast_ref() {
            (*error, message)
        } else {
            return None;
        };
        match error {
            EnumValue::Known(error) => Some((error, message.clone())),
            EnumValue::Unknown(_) => None,
        }
    }
}

impl fmt::Debug for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("Error").field("kind", &self.kind).field("message", &self.message).finish()
    }
}

impl fmt::Display for Error {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        write!(f, "{:?}: {}", self.kind, self.message)
    }
}

impl std::error::Error for Error {
    fn source(&self) -> Option<&(dyn std::error::Error + 'static)> {
        self.source.as_deref().map(|e| e as &(dyn std::error::Error + 'static))
    }
}

/// The category of a broker error code.
pub(crate) fn server_error_kind(error: EnumValue<ServerError>) -> ErrorKind {
    use ServerError::*;
    match error {
        EnumValue::Known(TopicNotFound | SubscriptionNotFound | ConsumerNotFound) => ErrorKind::NotFound,
        EnumValue::Known(AuthenticationError) => ErrorKind::Authentication,
        EnumValue::Known(AuthorizationError) => ErrorKind::Authorization,
        EnumValue::Known(ProducerBusy) => ErrorKind::ProducerBusy,
        EnumValue::Known(ProducerFenced) => ErrorKind::ProducerFenced,
        EnumValue::Known(ConsumerBusy) => ErrorKind::ConsumerBusy,
        EnumValue::Known(IncompatibleSchema) => ErrorKind::IncompatibleSchema,
        EnumValue::Known(TopicTerminatedError) => ErrorKind::TopicTerminated,
        EnumValue::Known(TransactionConflict) => ErrorKind::TransactionConflict,
        EnumValue::Known(ServiceNotReady | TooManyRequests) => ErrorKind::NotConnected,
        _ => ErrorKind::Other,
    }
}

impl From<ConnectionError> for Error {
    fn from(error: ConnectionError) -> Self {
        let kind = match &error {
            ConnectionError::Refused { error, .. } => match server_error_kind(*error) {
                ErrorKind::Other => ErrorKind::Connect,
                kind => kind,
            },
            ConnectionError::UnsupportedBroker(_) => ErrorKind::UnsupportedBroker,
            ConnectionError::TooManyAuthRounds | ConnectionError::Authentication(_) => ErrorKind::Authentication,
            ConnectionError::HandshakeTimeout => ErrorKind::Timeout,
            ConnectionError::NotReady => ErrorKind::NotConnected,
            _ => ErrorKind::Connect,
        };
        Self::with_source(kind, error.to_string(), error)
    }
}

impl From<RequestError> for Error {
    fn from(error: RequestError) -> Self {
        let kind = match &error {
            RequestError::Server { error, .. } => server_error_kind(*error),
            RequestError::Timeout => ErrorKind::Timeout,
        };
        Self::with_source(kind, error.to_string(), error)
    }
}

impl From<LookupError> for Error {
    fn from(error: LookupError) -> Self {
        let kind = match &error {
            LookupError::Server { error, .. } => server_error_kind(*error),
            _ => ErrorKind::Other,
        };
        Self::with_source(kind, error.to_string(), error)
    }
}

impl From<UrlError> for Error {
    fn from(error: UrlError) -> Self {
        Self::with_source(ErrorKind::InvalidServiceUrl, error.to_string(), error)
    }
}
