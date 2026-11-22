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

//! The local client: one event loop's client, `!Send`, for applications that run their own compio loops
//! (docs/2-architecture.md §4.2).
//!
//! Create it inside a compio runtime. It spawns its tasks on that runtime only.

use std::cell::{Cell, RefCell};
use std::future::Future;
use std::rc::Rc;
use std::sync::Arc;
use std::time::Instant;

use compio::runtime::JoinHandle;

use apache_pulsar_protocol::backoff::Backoff;
use apache_pulsar_protocol::connection::RequestError;
use apache_pulsar_protocol::frame::Frame;
use apache_pulsar_protocol::lookup::{BrokerAddress, HostPort, Lookup, LookupStep, ServiceUrl};
use apache_pulsar_protocol::proto::{
    self, BaseCommand, CommandGetSchema, ServerError, base_command::Type as CommandType,
};
use buffa::{EnumValue, MessageView};

use crate::config::{ClientBuilder, Local};
use crate::conn::ConnHandle;
use crate::error::{Error, ErrorKind, Result};
use crate::memory::MemoryLimit;
use crate::pool::Pool;
use crate::schema::Schema;
use crate::watch::Watches;

/// A producer on this loop. See [`crate::Producer`].
pub type Producer<T> = crate::Producer<T, Local>;
/// Builds a producer on this loop: [`PulsarClient::new_producer`].
pub type ProducerBuilder<T> = crate::ProducerBuilder<T, Local>;
/// A message being built: [`Producer::new_message`].
pub type MessageBuilder<'a, T> = crate::MessageBuilder<'a, T, Local>;
/// A Queue consumer on this loop. See [`crate::QueueConsumer`].
pub type QueueConsumer<T> = crate::QueueConsumer<T, Local>;
/// Builds a Queue consumer on this loop: [`PulsarClient::new_queue_consumer`].
pub type QueueConsumerBuilder<T> = crate::QueueConsumerBuilder<T, Local>;
/// A Stream consumer on this loop. See [`crate::StreamConsumer`].
pub type StreamConsumer<T> = crate::StreamConsumer<T, Local>;
/// Builds a Stream consumer on this loop: [`PulsarClient::new_stream_consumer`].
pub type StreamConsumerBuilder<T> = crate::StreamConsumerBuilder<T, Local>;
/// A Checkpoint consumer on this loop. See [`crate::CheckpointConsumer`].
pub type CheckpointConsumer<T> = crate::CheckpointConsumer<T, Local>;
/// Builds a Checkpoint consumer on this loop: [`PulsarClient::new_checkpoint_consumer`].
pub type CheckpointConsumerBuilder<T> = crate::CheckpointConsumerBuilder<T, Local>;

/// Asks a producer's or consumer's task to close.
type Close = Box<dyn Fn()>;

/// The client of one event loop. Cheap to clone; clones are the same client.
#[derive(Clone)]
pub struct PulsarClient {
    inner: Rc<Inner>,
}

struct Inner {
    pool: Pool,
    watches: Watches,
    /// The producers' and consumers' tasks, with the way to ask each to close.
    tasks: RefCell<Vec<(Close, JoinHandle<()>)>>,
    closed: Cell<bool>,
    /// The next service URL host to try first.
    next_host: Cell<usize>,
}

impl std::fmt::Debug for PulsarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("local::PulsarClient").field("pool", &self.inner.pool).finish()
    }
}

impl PulsarClient {
    pub fn builder() -> ClientBuilder<Local> {
        ClientBuilder::default()
    }

    pub(crate) fn from_config(config: crate::config::ClientConfig) -> Self {
        Self {
            inner: Rc::new(Inner {
                pool: Pool::new(Rc::new(config)),
                watches: Watches::default(),
                tasks: RefCell::new(Vec::new()),
                closed: Cell::new(false),
                next_host: Cell::new(0),
            }),
        }
    }

    /// A producer, in the topic's layout, that publishes values with `schema`, on this loop. See
    /// [`ProducerBuilder`](crate::ProducerBuilder).
    pub fn new_producer<T: 'static>(&self, schema: Schema<T>) -> ProducerBuilder<T> {
        crate::ProducerBuilder::new_local(self.clone(), schema)
    }

    /// A Queue consumer, on every segment of the topic's layout, that reads values with `schema`, on this loop. See
    /// [`QueueConsumerBuilder`](crate::QueueConsumerBuilder).
    pub fn new_queue_consumer<T: 'static>(&self, schema: Schema<T>) -> QueueConsumerBuilder<T> {
        crate::QueueConsumerBuilder::new_local(self.clone(), schema)
    }

    /// A Stream consumer, on the segments the topic's controller assigns it, that reads values with `schema`, on this
    /// loop. See [`StreamConsumerBuilder`](crate::StreamConsumerBuilder).
    pub fn new_stream_consumer<T: 'static>(&self, schema: Schema<T>) -> StreamConsumerBuilder<T> {
        crate::StreamConsumerBuilder::new_local(self.clone(), schema)
    }

    /// A Checkpoint consumer, with a reader on every segment of the topic's layout, that reads values with `schema`,
    /// on this loop. See [`CheckpointConsumerBuilder`](crate::CheckpointConsumerBuilder).
    pub fn new_checkpoint_consumer<T: 'static>(&self, schema: Schema<T>) -> CheckpointConsumerBuilder<T> {
        crate::CheckpointConsumerBuilder::new_local(self.clone(), schema)
    }

    /// Closes the producers and consumers, then every connection. Later operations fail with [`ErrorKind::AlreadyClosed`]. Can
    /// be called again.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.replace(true) {
            return Ok(());
        }
        let tasks = std::mem::take(&mut *self.inner.tasks.borrow_mut());
        for (close, _) in &tasks {
            close();
        }
        for (_, task) in tasks {
            let _ = task.await;
        }
        self.inner.pool.close().await;
        Ok(())
    }

    /// Runs a producer's or consumer's task on this loop, owned by the client, which calls `close` to close it on
    /// close.
    pub(crate) fn adopt(&self, close: impl Fn() + 'static, task: impl Future<Output = ()> + 'static) -> Result<()> {
        self.check_open()?;
        let mut tasks = self.inner.tasks.borrow_mut();
        tasks.retain(|(_, task)| !task.is_finished());
        tasks.push((Box::new(close), compio::runtime::spawn(task)));
        Ok(())
    }

    pub(crate) fn memory(&self) -> &Arc<MemoryLimit> {
        &self.inner.pool.config().memory
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    fn check_open(&self) -> Result<()> {
        if self.is_closed() { Err(Error::closed()) } else { Ok(()) }
    }

    /// Finds the broker that serves `topic`, retrying connection failures with backoff, across the service URL's
    /// hosts, until the operation timeout.
    pub(crate) async fn lookup(&self, topic: &str) -> Result<BrokerAddress> {
        let tls = self.inner.pool.config().service.tls;
        self.on_service(&format!("lookup of {topic}"), |host| self.lookup_from(topic, Lookup::new(topic, host, tls)))
            .await
    }

    /// The schema of `topic` at `version`, from a broker of the service URL, as Java asks for it: `None` if the topic
    /// has no schema of that version, as Java reads the broker's `TopicNotFound`. Retries connection failures as a
    /// lookup does.
    pub(crate) async fn schema(&self, topic: &str, version: &[u8]) -> Result<Option<proto::Schema>> {
        self.on_service(&format!("the schema of {topic}"), |host| async move {
            let connection = self.inner.pool.get(&BrokerAddress::direct(host)).await?;
            let (topic, version) = (topic.to_string(), version.to_vec());
            let frame = connection.request(move |request_id| get_schema_command(topic, version, request_id)).await?;
            schema_response(&frame)
        })
        .await
    }

    /// Runs `attempt` on a host of the service URL, retrying connection failures with backoff, across the hosts, until
    /// the operation timeout. `what` names the attempt for the timeout's error.
    async fn on_service<T, F: Future<Output = Result<T>>>(
        &self,
        what: &str,
        attempt: impl Fn(HostPort) -> F,
    ) -> Result<T> {
        self.check_open()?;
        let config = self.inner.pool.config();
        let deadline = Instant::now() + config.operation_timeout;
        let hosts = &config.service.hosts;
        let mut backoff = Backoff::default();
        loop {
            let index = self.inner.next_host.get() % hosts.len();
            let host = hosts[index].clone();
            match attempt(host.clone()).await {
                Ok(found) => return Ok(found),
                Err(error) if error.is_retryable() => {
                    // Start the next attempt, and the next lookup, at another host.
                    self.inner.next_host.set(index + 1);
                    let delay = backoff.next_delay(jitter());
                    if Instant::now() + delay >= deadline {
                        return Err(Error::new(ErrorKind::Timeout, format!("{what} timed out: {error}")));
                    }
                    tracing::debug!(%host, %error, ?delay, "{what} failed, retrying");
                    compio::time::sleep(delay).await;
                    self.check_open()?;
                }
                Err(error) => return Err(error),
            }
        }
    }

    async fn lookup_from(&self, topic: &str, mut lookup: Lookup) -> Result<BrokerAddress> {
        loop {
            let connection = self.inner.pool.get(lookup.target()).await?;
            let request = lookup.clone();
            let frame = connection.request(move |id| request.command(id)).await?;
            match lookup.on_response(&frame)? {
                LookupStep::Ask(next) => tracing::debug!(%topic, broker = %next.physical, "lookup redirected"),
                LookupStep::Found(address) => return Ok(address),
            }
        }
    }

    /// The loop's layout watches.
    pub(crate) fn watches(&self) -> &Watches {
        &self.inner.watches
    }

    pub(crate) fn operation_timeout(&self) -> std::time::Duration {
        self.inner.pool.config().operation_timeout
    }

    /// The service URL the client was built with.
    pub(crate) fn service(&self) -> &ServiceUrl {
        &self.inner.pool.config().service
    }

    /// A ready connection to `address`, from the pool.
    pub(crate) async fn connection(&self, address: &BrokerAddress) -> Result<ConnHandle> {
        self.check_open()?;
        self.inner.pool.get(address).await
    }

    #[cfg(test)]
    pub(crate) fn pool(&self) -> &Pool {
        &self.inner.pool
    }
}

impl ClientBuilder<Local> {
    /// Creates the client. Must be called inside a compio runtime, which the client's tasks then run on.
    pub async fn build(self) -> Result<PulsarClient> {
        Ok(PulsarClient::from_config(self.into_config()?))
    }
}

/// A random number in `[0, 1)` for backoff jitter. Quality doesn't matter, only spread.
pub(crate) fn jitter() -> f64 {
    use std::hash::{BuildHasher, RandomState};
    (RandomState::new().hash_one(Instant::now()) >> 11) as f64 / (1u64 << 53) as f64
}

/// Asks for the schema of `topic` at `version`.
fn get_schema_command(topic: String, version: Vec<u8>, request_id: u64) -> BaseCommand {
    let get = CommandGetSchema { request_id, topic, schema_version: Some(version), ..Default::default() };
    BaseCommand { r#type: CommandType::GET_SCHEMA.into(), getSchema: get.into(), ..Default::default() }
}

/// The schema in a `CommandGetSchemaResponse`: `None` for the broker's `TopicNotFound`, which it answers for a topic
/// without a schema of the version asked. An answer with neither a schema nor an error fails.
fn schema_response(frame: &Frame) -> Result<Option<proto::Schema>> {
    let command = frame.command_view().ok().and_then(|view| view.to_owned_message().ok()).unwrap_or_default();
    let response = command.getSchemaResponse.as_option().cloned().unwrap_or_default();
    match (response.error_code, response.schema.as_option()) {
        (Some(ServerError::TopicNotFound), _) => Ok(None),
        (Some(error), _) => {
            let message = response.error_message.unwrap_or_default();
            Err(RequestError::Server { error: EnumValue::Known(error), message }.into())
        }
        (None, Some(schema)) => Ok(Some(schema.clone())),
        (None, None) => Err(Error::new(ErrorKind::Other, "the broker's answer has no schema")),
    }
}

#[cfg(test)]
mod tests {
    use apache_pulsar_protocol::frame::{FrameDecoder, encode_command};
    use apache_pulsar_protocol::proto::CommandGetSchemaResponse;
    use bytes::BytesMut;

    use super::*;

    fn frame(command: BaseCommand) -> Frame {
        let mut bytes = BytesMut::new();
        encode_command(&command, &mut bytes).unwrap();
        FrameDecoder::default().decode(&mut bytes).unwrap().unwrap()
    }

    fn response(response: CommandGetSchemaResponse) -> Frame {
        let kind = CommandType::GET_SCHEMA_RESPONSE.into();
        frame(BaseCommand { r#type: kind, getSchemaResponse: response.into(), ..Default::default() })
    }

    /// The broker's answers to `CommandGetSchema`, as Java reads them.
    #[test]
    fn schema_responses() {
        let schema = proto::Schema { name: "acme/orders/events".into(), ..Default::default() };
        let found = CommandGetSchemaResponse { schema: schema.clone().into(), ..Default::default() };
        assert_eq!(schema_response(&response(found)).unwrap(), Some(schema));
        let missing = CommandGetSchemaResponse { error_code: Some(ServerError::TopicNotFound), ..Default::default() };
        assert_eq!(schema_response(&response(missing)).unwrap(), None);
        let failed = CommandGetSchemaResponse {
            error_code: Some(ServerError::MetadataError),
            error_message: Some("no metadata".into()),
            ..Default::default()
        };
        let error = schema_response(&response(failed)).unwrap_err();
        assert!(error.to_string().contains("no metadata"), "{error}");
        // Neither a schema nor an error: nothing to keep for the version.
        let empty = schema_response(&response(CommandGetSchemaResponse::default())).unwrap_err();
        assert_eq!(empty.kind(), ErrorKind::Other);
        let success = frame(BaseCommand { r#type: CommandType::SUCCESS.into(), ..Default::default() });
        assert_eq!(schema_response(&success).unwrap_err().kind(), ErrorKind::Other);
    }
}
