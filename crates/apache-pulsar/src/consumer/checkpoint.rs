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

//! The Checkpoint consumer (docs/7-api.md §4, docs/6-scalable-topics.md §6).

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::checkpoint as wire;
use apache_pulsar_protocol::checkpoint_consumer::CheckpointConsumer as Reader;
use apache_pulsar_protocol::consumer::ConsumerOptions;
use futures_channel::oneshot;
use futures_util::Stream;

use super::task::Machine;
use super::{Link, Message, Request, consumer_closed, invalid, proto_schema};
use crate::checkpoint::Checkpoint;
use crate::client::{LoopSlot, PulsarClient};
use crate::config::{Blocking, Local, ThreadSafe};
use crate::error::Result;
use crate::local;
use crate::producer::{ApiClient, normalize_topic};
use crate::schema::{Schema, SchemaInfo};

/// Builds a Checkpoint consumer: `client.new_checkpoint_consumer(schema)`. `Api` is [`ThreadSafe`] or [`Local`], as
/// for the client.
#[must_use = "a builder does nothing until `create` is awaited"]
pub struct CheckpointConsumerBuilder<T, Api = ThreadSafe> {
    client: ApiClient,
    schema: Schema<T>,
    settings: Settings,
    _api: PhantomData<Api>,
}

#[derive(Debug, Clone)]
struct Settings {
    topic: Option<String>,
    start: Checkpoint,
    consumer_name: Option<String>,
}

impl Default for Settings {
    fn default() -> Self {
        // Java's default start.
        Self { topic: None, start: Checkpoint::latest(), consumer_name: None }
    }
}

impl<T, Api> std::fmt::Debug for CheckpointConsumerBuilder<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointConsumerBuilder").field("settings", &self.settings).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> CheckpointConsumerBuilder<T, Api> {
    /// The topic: `topic://tenant/namespace/name` for a scalable topic. A `persistent://` name, or the short forms
    /// `tenant/namespace/name` and `name`, address a regular topic through the migration bridge. Required.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.settings.topic = Some(topic.into());
        self
    }

    /// Where to start reading: [`Checkpoint::latest`] by default, [`Checkpoint::earliest`], or a checkpoint taken
    /// with [`CheckpointConsumer::checkpoint`], which the consumer reads on from.
    ///
    /// At the latest position, the consumer starts at the end of each segment the topic has when it is created. The
    /// segments a split or a merge adds later are read from their start, as are those a restored checkpoint doesn't
    /// name: they hold only messages newer than the start.
    pub fn start_position(mut self, start: Checkpoint) -> Self {
        self.settings.start = start;
        self
    }

    /// The consumer's name. Each segment's reader is named `<name>-seg-<id>`; without a name, the brokers pick them.
    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.settings.consumer_name = Some(name.into());
        self
    }

    fn options(&self, operation_timeout: Duration) -> Result<(String, ConsumerOptions, wire::Checkpoint)> {
        let (topic, options) = reader_options(&self.settings, self.schema.info(), operation_timeout)?;
        Ok((topic, options, self.settings.start.0.clone()))
    }
}

/// The topic and the readers' options, once checked. Not generic: one copy serves every schema and client.
fn reader_options(
    settings: &Settings,
    schema: Option<&SchemaInfo>,
    operation_timeout: Duration,
) -> Result<(String, ConsumerOptions)> {
    let topic = normalize_topic(settings.topic.as_deref())?;
    let options = ConsumerOptions {
        subscription: reader_subscription(),
        consumer_name: settings.consumer_name.clone(),
        schema: proto_schema(schema),
        operation_timeout,
        ..ConsumerOptions::default()
    };
    Ok((topic, options))
}

/// The name of the readers' subscriptions, which don't outlive them: `reader-` and ten random hex digits, as Java's
/// v4 reader picks one.
fn reader_subscription() -> String {
    use std::hash::{BuildHasher, RandomState};
    let random = RandomState::new().hash_one(Instant::now());
    format!("reader-{:010x}", random & 0xff_ffff_ffff)
}

impl<T: 'static> CheckpointConsumerBuilder<T, ThreadSafe> {
    pub(crate) fn new(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the consumer, on the client's least loaded event loop, with a reader on every segment of the topic.
    pub async fn create(self) -> Result<CheckpointConsumer<T>> {
        let ApiClient::ThreadSafe(client) = &self.client else { unreachable!() };
        let (topic, options, start) = self.options(client.operation_timeout())?;
        let (slot, index) = client.place();
        let link = client
            .run_on(index, move |local| Box::pin(async move { create_on(local, topic, options, start).await }))
            .await?;
        Ok(CheckpointConsumer::from_link(link, self.schema, self.settings.consumer_name, Some(slot)))
    }
}

impl<T: 'static> CheckpointConsumerBuilder<T, Blocking> {
    pub(crate) fn new_blocking(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the consumer, on the client's least loaded event loop, and blocks until every segment's reader is
    /// subscribed.
    pub fn create(self) -> Result<crate::blocking::CheckpointConsumer<T>> {
        let builder = CheckpointConsumerBuilder::<T, ThreadSafe> {
            client: self.client,
            schema: self.schema,
            settings: self.settings,
            _api: PhantomData,
        };
        futures_executor::block_on(builder.create()).map(crate::blocking::CheckpointConsumer::new)
    }
}

impl<T: 'static> CheckpointConsumerBuilder<T, Local> {
    pub(crate) fn new_local(client: local::PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::Local(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the consumer, on the local client's loop, with a reader on every segment of the topic.
    pub async fn create(self) -> Result<CheckpointConsumer<T, Local>> {
        let ApiClient::Local(client) = &self.client else { unreachable!() };
        let (topic, options, start) = self.options(client.operation_timeout())?;
        let link = create_on(client.clone(), topic, options, start).await?;
        Ok(CheckpointConsumer::from_link(link, self.schema, self.settings.consumer_name, None))
    }
}

/// Creates a Checkpoint consumer on `client`'s loop, and waits for every segment's reader to be subscribed.
async fn create_on(
    client: local::PulsarClient,
    topic: String,
    options: ConsumerOptions,
    start: wire::Checkpoint,
) -> Result<Link> {
    let machine = move |layout| Machine::Checkpoint(Reader::new(options, start, layout, Instant::now()));
    super::start(client, topic, Box::new(machine)).await
}

/// A Checkpoint consumer: a reader on every segment of a topic, and no subscription on the brokers (*client-api*
/// §4.3). The application keeps where it is as a [`Checkpoint`] ([`CheckpointConsumer::checkpoint`]), and restores
/// the consumer from it. The messages of a key come in the order they were published, across splits and merges.
/// Cheap to clone: clones are the same consumer. `Api` is [`ThreadSafe`] (the default), whose consumer is
/// `Send + Sync`, or [`Local`].
///
/// Dropping the last clone closes the consumer in the background; [`CheckpointConsumer::close`] waits for the close.
pub struct CheckpointConsumer<T, Api = ThreadSafe> {
    shared: Arc<Shared>,
    schema: Schema<T>,
    _api: PhantomData<Api>,
}

struct Shared {
    link: Link,
    consumer_name: Option<String>,
    /// The thread-safe client's loop, to count the consumer off its load.
    _slot: Option<LoopSlot>,
}

impl<T, Api> Clone for CheckpointConsumer<T, Api> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), schema: self.schema.clone(), _api: PhantomData }
    }
}

impl<T, Api> std::fmt::Debug for CheckpointConsumer<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("CheckpointConsumer").field("topic", &self.shared.link.topic).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> CheckpointConsumer<T, Api> {
    fn from_link(link: Link, schema: Schema<T>, consumer_name: Option<String>, slot: Option<LoopSlot>) -> Self {
        let shared = Shared { link, consumer_name, _slot: slot };
        Self { shared: Arc::new(shared), schema, _api: PhantomData }
    }

    /// The topic, as the broker resolved it: `topic://tenant/namespace/name`.
    pub fn topic(&self) -> &str {
        &self.shared.link.topic
    }

    /// The name the consumer was given, if any.
    pub fn consumer_name(&self) -> Option<&str> {
        self.shared.consumer_name.as_deref()
    }

    /// The next message. Fails only once the consumer is closed.
    ///
    /// Receives are answered in the order they are first polled. A receive dropped before it completes loses no
    /// message: the next receive gets it, and checkpoints taken meanwhile don't count it as received.
    pub async fn receive(&self) -> Result<Message<T>> {
        let mut messages = self.shared.link.receive(1, None).await?;
        Ok(self.message(messages.pop().expect("a receive without a deadline gets its message")))
    }

    /// The next message, or `None` if none comes within `timeout`.
    pub async fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message<T>>> {
        let messages = self.shared.link.receive(1, Some(Instant::now() + timeout)).await?;
        Ok(messages.into_iter().next().map(|message| self.message(message)))
    }

    /// Up to `max` messages: as soon as there are `max`, or once `timeout` passes with those there are, none
    /// included.
    pub async fn receive_batch(&self, max: usize, timeout: Duration) -> Result<Vec<Message<T>>> {
        if max == 0 {
            return Err(invalid("a batch receive takes at least one message"));
        }
        let messages = self.shared.link.receive(max, Some(Instant::now() + timeout)).await?;
        Ok(messages.into_iter().map(|message| self.message(message)).collect())
    }

    /// The messages, as a stream that ends once the consumer is closed.
    pub fn stream(&self) -> impl Stream<Item = Message<T>> + '_ {
        futures_util::stream::unfold(self, |consumer| async move {
            consumer.receive().await.ok().map(|message| (message, consumer))
        })
    }

    /// Where the application is: for each segment, the last message a receive returned, or where the segment's
    /// reading starts. A consumer created at it reads on from there. It asks nothing of the brokers. Fails only once
    /// the consumer is closed.
    pub async fn checkpoint(&self) -> Result<Checkpoint> {
        let (reply, checkpoint) = oneshot::channel();
        self.shared.link.requests.unbounded_send(Request::Checkpoint { reply }).map_err(consumer_closed)?;
        checkpoint.await.map(Checkpoint).map_err(consumer_closed)
    }

    /// Closes the consumer: closes every segment's reader. The receives waiting fail with
    /// [`ErrorKind::AlreadyClosed`](crate::ErrorKind::AlreadyClosed). Can be called again.
    pub async fn close(&self) -> Result<()> {
        self.shared.link.close().await
    }

    fn message(&self, message: apache_pulsar_protocol::entry::ReceivedMessage) -> Message<T> {
        self.shared.link.message(message, &self.schema)
    }
}
