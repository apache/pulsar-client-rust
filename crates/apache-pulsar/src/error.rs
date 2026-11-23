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

/// The category of an [`Error`]: what the application can branch on (*error-model* §2).
///
/// The client retries what it can resolve itself: a lost connection, a broker that isn't ready, a segment that a
/// split or a merge replaced. Those reach the application only once the retries run out, mostly as
/// [`Timeout`](Self::Timeout). The other kinds come back at once, and the client doesn't retry them:
///
/// - [`NotFound`](Self::NotFound), [`Authentication`](Self::Authentication), [`Authorization`](Self::Authorization),
///   [`IncompatibleSchema`](Self::IncompatibleSchema), [`InvalidConfiguration`](Self::InvalidConfiguration) and
///   [`InvalidServiceUrl`](Self::InvalidServiceUrl) fail the same way again until the configuration, the
///   credentials or the topic changes;
/// - [`ProducerBusy`](Self::ProducerBusy), [`ProducerFenced`](Self::ProducerFenced) and
///   [`ConsumerBusy`](Self::ConsumerBusy) come from the topic's access mode or the subscription's type;
/// - after [`Timeout`](Self::Timeout), [`MemoryBufferFull`](Self::MemoryBufferFull) and
///   [`MessageTooLarge`](Self::MessageTooLarge), the application decides whether to try again.
///
/// A failed send fails that message only, and a failed receive or acknowledgment loses nothing: the producer or the
/// consumer stays usable, unless the error is [`AlreadyClosed`](Self::AlreadyClosed).
///
/// More kinds may come in later releases, so a `match` on a kind needs a wildcard arm.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum ErrorKind {
    /// The topic doesn't exist and the broker doesn't create it, or the broker doesn't know the subscription or
    /// the consumer named.
    NotFound,
    /// The broker refused the credentials, or the [`Authentication`](crate::Authentication) provider failed to
    /// supply them.
    Authentication,
    /// The credentials are valid, but they don't allow the operation on the topic.
    Authorization,
    /// The operation didn't complete in time: a lookup, a create or a subscribe within the client's operation
    /// timeout, a message within the producer's send timeout, a connection within the connect timeout. A failure the
    /// client kept retrying, such as a broker it can't connect to, ends as a timeout too, with the last failure in its
    /// message.
    Timeout,
    /// The client, the producer or the consumer is closed: by `close`, or as the client closed. Messages a producer
    /// still held when it closed fail with it too.
    AlreadyClosed,
    /// The topic doesn't take this producer: an exclusive producer holds it, another producer has the same name, or
    /// the topic has as many producers as it allows.
    ProducerBusy,
    /// An exclusive producer can't have the topic, which other producers hold, or a newer exclusive producer took it:
    /// the producer is fenced out. Create a new producer to try again.
    ProducerFenced,
    /// The subscription doesn't take this consumer: its type allows one consumer and another is attached, or the
    /// consumer's key ranges overlap another consumer's.
    ConsumerBusy,
    /// The schema isn't compatible with the topic's. It fails a consumer's subscribe or create, an exclusive
    /// producer's create, and a Shared producer's sends to a segment, where it registers with its first message.
    IncompatibleSchema,
    /// The topic is terminated and takes no more messages. The producer moves the messages of a segment that a split
    /// or a merge sealed to its successors by itself: a message fails with this kind only after ten tries.
    TopicTerminated,
    /// The client can't connect to a broker. Retried by the client, as a timeout once the retries run out: a create or
    /// a subscribe fails with this kind when the broker that serves the topic can't be reached for its first layout,
    /// as in Java.
    Connect,
    /// The connection to a broker closed, or the broker isn't ready to serve the topic. Retried by the client.
    NotConnected,
    /// A producer's queue of pending messages is full. Not raised: the client's memory limit is the only bound on
    /// what producers hold, as in Java V5.
    ProducerQueueFull,
    /// The client's memory limit is reached, and the producer doesn't wait for room
    /// ([`ProducerBuilder::block_if_queue_full`](crate::ProducerBuilder::block_if_queue_full)). Room comes back as
    /// the brokers store the messages in flight.
    MemoryBufferFull,
    /// A message larger than the broker accepts. It fails alone: the producer stays usable.
    MessageTooLarge,
    /// A transaction conflicts with another, or can't go on. Not raised yet: transactions come in a later release.
    TransactionConflict,
    /// Encrypting or decrypting a message failed. Not raised yet: end-to-end encryption comes in a later release.
    Crypto,
    /// A builder's settings, or an argument, are invalid: a topic or a subscription name that is missing or
    /// malformed, a codec this build leaves out, a schema used where it doesn't apply, TLS files that can't be read,
    /// bytes that aren't a message id or a checkpoint.
    InvalidConfiguration,
    /// The service URL is malformed: it takes `pulsar://` or `pulsar+ssl://`, and one or more `host:port`.
    InvalidServiceUrl,
    /// The broker doesn't support scalable topics, which take Pulsar 5.0 or later (C15).
    UnsupportedBroker,
    /// A failure outside the other categories: a value that doesn't encode with the schema or a payload that doesn't
    /// decode with it, an answer from a broker that the client can't read, a broker error with no category of its
    /// own, an event loop that can't start.
    Other,
}

/// An error from the client: its [`ErrorKind`], a message, and the failure that caused it, if any, as its
/// [`source`](std::error::Error::source).
///
/// It prints as its kind and its message, as in `Timeout: no layout for topic://acme/orders/events within the
/// timeout`. Cheap to clone.
#[derive(Clone)]
pub struct Error {
    kind: ErrorKind,
    message: String,
    source: Option<Arc<dyn std::error::Error + Send + Sync>>,
}

/// `Result` with this crate's [`Error`].
pub type Result<T, E = Error> = std::result::Result<T, E>;

impl Error {
    /// An error of `kind`, such as one an [`Authentication`](crate::Authentication) provider returns.
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

    /// The category of the error.
    pub fn kind(&self) -> ErrorKind {
        self.kind
    }

    /// The message, without the kind.
    pub(crate) fn message(&self) -> &str {
        &self.message
    }

    /// Whether trying again may succeed: connection and timeout failures, not configuration or permission ones.
    pub(crate) fn is_retryable(&self) -> bool {
        matches!(self.kind, ErrorKind::Connect | ErrorKind::NotConnected | ErrorKind::Timeout)
    }

    pub(crate) fn closed() -> Self {
        Self::new(ErrorKind::AlreadyClosed, "the client is closed")
    }

    /// The broker's error code, when the broker refused the operation, or refused the connection with an error the
    /// client doesn't retry, such as credentials it rejects. A refusal the client retries, as a lookup does, counts as
    /// a failure to connect.
    pub(crate) fn server_error(&self) -> Option<(ServerError, String)> {
        let source = self.source.as_deref()?;
        let (error, message) = if let Some(RequestError::Server { error, message }) = source.downcast_ref() {
            (*error, message)
        } else if let Some(LookupError::Server { error, message }) = source.downcast_ref() {
            (*error, message)
        } else if let Some(ConnectionError::Refused { error, message }) = source.downcast_ref()
            && !self.is_retryable()
        {
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

/// The category of a broker error code, as Java V5 maps its v4 exception (`V5ExceptionMappings`) where it has one.
pub(crate) fn server_error_kind(error: EnumValue<ServerError>) -> ErrorKind {
    use ServerError::*;
    match error {
        EnumValue::Known(TopicNotFound | SubscriptionNotFound | ConsumerNotFound) => ErrorKind::NotFound,
        EnumValue::Known(AuthenticationError) => ErrorKind::Authentication,
        EnumValue::Known(AuthorizationError) => ErrorKind::Authorization,
        EnumValue::Known(ProducerBusy) => ErrorKind::ProducerBusy,
        EnumValue::Known(ProducerFenced) => ErrorKind::ProducerFenced,
        // Key_Shared ranges that overlap another consumer's: a subscription constraint, as for Java.
        EnumValue::Known(ConsumerBusy | ConsumerAssignError) => ErrorKind::ConsumerBusy,
        EnumValue::Known(IncompatibleSchema) => ErrorKind::IncompatibleSchema,
        EnumValue::Known(TopicTerminatedError) => ErrorKind::TopicTerminated,
        EnumValue::Known(TransactionConflict) => ErrorKind::TransactionConflict,
        // A transaction no longer open, or that its coordinator no longer knows: it can't go on, as for Java V5.
        EnumValue::Known(InvalidTxnStatus | TransactionNotFound) => ErrorKind::TransactionConflict,
        EnumValue::Known(ServiceNotReady | TooManyRequests) => ErrorKind::NotConnected,
        // A name the broker refuses is the application's to change. Java V5 has no category for it.
        EnumValue::Known(InvalidTopicName) => ErrorKind::InvalidConfiguration,
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
