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

//! Producers (docs/7-api.md §3).

mod task;

pub(crate) use task::Request;

use std::marker::PhantomData;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use apache_pulsar_protocol::producer::{Batching, OutgoingMessage, Producer as Machine, ProducerOptions};
use apache_pulsar_protocol::proto;
use futures_channel::{mpsc, oneshot};

use crate::client::{LoopSlot, PulsarClient};
use crate::config::{Blocking, Local, ThreadSafe};
use crate::error::{Error, ErrorKind, Result};
use crate::local;
use crate::memory::{MemoryLimit, PER_MESSAGE_OVERHEAD};
use crate::message_id::MessageId;
use crate::schema::Schema;
use task::Task;

/// Who may write to a topic (*client-api* §3.1).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum ProducerAccessMode {
    /// Any number of producers.
    #[default]
    Shared,
    /// One producer: creating a second fails.
    Exclusive,
    /// One producer: a new one fences the old one out.
    ExclusiveWithFencing,
    /// One producer: a new one waits until it gets the topic.
    WaitForExclusive,
}

impl ProducerAccessMode {
    fn to_proto(self) -> proto::ProducerAccessMode {
        match self {
            Self::Shared => proto::ProducerAccessMode::Shared,
            Self::Exclusive => proto::ProducerAccessMode::Exclusive,
            Self::ExclusiveWithFencing => proto::ProducerAccessMode::ExclusiveWithFencing,
            Self::WaitForExclusive => proto::ProducerAccessMode::WaitForExclusive,
        }
    }
}

/// How a producer groups messages into entries, which the broker stores and dispatches whole (Java's
/// `BatchingPolicy`). On by default, as in Java: a message waits up to `max_publish_delay` for others to join it.
///
/// Each segment's producer has one open batch per entry bucket of the segment, so that an entry holds keys of one
/// bucket only, as consumers sharing a segment by bucket need. The batches of a segment close together, as Java's
/// do, once they hold `max_messages` messages or `max_size` bytes of payload between them, or `max_publish_delay`
/// after their first message: no message waits longer for its batch. (Java's timer counts from the last batch sent,
/// so a batch can wait a little longer there.) A message with a delivery time or replication clusters goes in an
/// entry of its own.
///
/// ```
/// # use std::time::Duration;
/// # use apache_pulsar::BatchingPolicy;
/// let policy = BatchingPolicy { max_publish_delay: Duration::from_millis(5), ..Default::default() };
/// assert_eq!(policy.max_messages, 1000);
/// ```
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct BatchingPolicy {
    /// Whether messages are batched; without, each message is an entry of its own.
    pub enabled: bool,
    /// How long the first message of a batch waits for others: 1 ms by default.
    pub max_publish_delay: Duration,
    /// The most messages in a segment's batches; 1000 by default, 0 for no limit.
    pub max_messages: usize,
    /// The most payload bytes in a segment's batches: 128 KiB by default, and at most the broker's largest message,
    /// which 0 stands for.
    pub max_size: usize,
}

impl Default for BatchingPolicy {
    /// Java's defaults: on, 1 ms, 1000 messages, 128 KiB.
    fn default() -> Self {
        Self { enabled: true, max_publish_delay: Duration::from_millis(1), max_messages: 1000, max_size: 128 << 10 }
    }
}

impl BatchingPolicy {
    /// No batching: each message is an entry of its own.
    pub fn disabled() -> Self {
        Self { enabled: false, ..Self::default() }
    }

    fn to_options(&self) -> Option<Batching> {
        self.enabled.then_some(Batching {
            max_messages: self.max_messages,
            max_bytes: self.max_size,
            max_delay: self.max_publish_delay,
        })
    }
}

/// Builds a producer: `client.new_producer(schema)`. `Api` is [`ThreadSafe`] or [`Local`], as for the client.
#[must_use = "a builder does nothing until `create` is awaited"]
pub struct ProducerBuilder<T, Api = ThreadSafe> {
    client: ApiClient,
    schema: Schema<T>,
    settings: Settings,
    _api: PhantomData<Api>,
}

/// The client a builder creates its producer or consumer with.
pub(crate) enum ApiClient {
    ThreadSafe(PulsarClient),
    Local(local::PulsarClient),
}

#[derive(Debug, Clone)]
struct Settings {
    topic: Option<String>,
    name: Option<String>,
    access_mode: ProducerAccessMode,
    send_timeout: Option<Duration>,
    block_if_queue_full: bool,
    initial_sequence_id: Option<i64>,
    properties: Vec<(String, String)>,
    batching: BatchingPolicy,
}

impl Default for Settings {
    fn default() -> Self {
        Self {
            topic: None,
            name: None,
            access_mode: ProducerAccessMode::Shared,
            // Java's default.
            send_timeout: Some(Duration::from_secs(30)),
            // V5's default: wait for room rather than fail.
            block_if_queue_full: true,
            initial_sequence_id: None,
            properties: Vec::new(),
            batching: BatchingPolicy::default(),
        }
    }
}

impl<T, Api> std::fmt::Debug for ProducerBuilder<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ProducerBuilder").field("settings", &self.settings).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> ProducerBuilder<T, Api> {
    /// The topic: `topic://tenant/namespace/name` for a scalable topic. A `persistent://` name, or the short forms
    /// `tenant/namespace/name` and `name`, address a regular topic through the migration bridge. Required.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.settings.topic = Some(topic.into());
        self
    }

    /// The producer's name. Without one, each broker picks a name for the producer of each segment.
    pub fn producer_name(mut self, name: impl Into<String>) -> Self {
        self.settings.name = Some(name.into());
        self
    }

    /// [`ProducerAccessMode::Shared`] by default. Under an exclusive mode, `create` claims every segment.
    pub fn access_mode(mut self, mode: ProducerAccessMode) -> Self {
        self.settings.access_mode = mode;
        self
    }

    /// How long a message may wait to be stored once it is on its way to a broker; [`Duration::ZERO`] for no
    /// limit. 30 s by default. A message that times out fails alone: the producer stays usable.
    pub fn send_timeout(mut self, timeout: Duration) -> Self {
        self.settings.send_timeout = (!timeout.is_zero()).then_some(timeout);
        self
    }

    /// At the client's memory limit, whether a send waits for room (the default) or fails with
    /// [`ErrorKind::MemoryBufferFull`]. The memory limit is the only bound on what producers hold.
    pub fn block_if_queue_full(mut self, block: bool) -> Self {
        self.settings.block_if_queue_full = block;
        self
    }

    /// The sequence id before the first message's, for deduplication.
    pub fn initial_sequence_id(mut self, sequence_id: i64) -> Self {
        self.settings.initial_sequence_id = Some(sequence_id);
        self
    }

    /// A property sent to the broker with the producer.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.properties.push((key.into(), value.into()));
        self
    }

    /// How messages are grouped into entries: [`BatchingPolicy::default`], Java's, unless set.
    pub fn batching_policy(mut self, policy: BatchingPolicy) -> Self {
        self.settings.batching = policy;
        self
    }

    fn options(&self, operation_timeout: Duration) -> ProducerOptions {
        ProducerOptions {
            batching: self.settings.batching.to_options(),
            name: self.settings.name.clone(),
            access_mode: self.settings.access_mode.to_proto(),
            send_timeout: self.settings.send_timeout,
            initial_sequence_id: self.settings.initial_sequence_id,
            properties: self.settings.properties.clone(),
            schema: self.schema.info().to_proto(),
            operation_timeout,
        }
    }
}

impl<T: 'static> ProducerBuilder<T, ThreadSafe> {
    pub(crate) fn new(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the producer, on the client's least loaded event loop.
    pub async fn create(self) -> Result<Producer<T>> {
        let ApiClient::ThreadSafe(client) = &self.client else { unreachable!() };
        let topic = normalize_topic(self.settings.topic.as_deref())?;
        let (slot, index) = client.place();
        let options = self.options(client.operation_timeout());
        let link =
            client.run_on(index, move |local| Box::pin(async move { create_on(local, topic, options).await })).await?;
        Ok(Producer::from_link(link, self.schema, client.memory().clone(), self.settings, Some(slot)))
    }
}

impl<T: 'static> ProducerBuilder<T, Blocking> {
    pub(crate) fn new_blocking(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the producer, on the client's least loaded event loop, and blocks until it is ready.
    pub fn create(self) -> Result<crate::blocking::Producer<T>> {
        let builder = ProducerBuilder::<T, ThreadSafe> {
            client: self.client,
            schema: self.schema,
            settings: self.settings,
            _api: PhantomData,
        };
        futures_executor::block_on(builder.create()).map(crate::blocking::Producer::new)
    }
}

impl<T: 'static> ProducerBuilder<T, Local> {
    pub(crate) fn new_local(client: local::PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::Local(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Creates the producer, on the local client's loop.
    pub async fn create(self) -> Result<Producer<T, Local>> {
        let ApiClient::Local(client) = &self.client else { unreachable!() };
        let topic = normalize_topic(self.settings.topic.as_deref())?;
        let options = self.options(client.operation_timeout());
        let link = create_on(client.clone(), topic, options).await?;
        Ok(Producer::from_link(link, self.schema, client.memory().clone(), self.settings, None))
    }
}

/// What a created producer's handle holds of its task.
pub(crate) struct Link {
    requests: mpsc::UnboundedSender<Request>,
    topic: String,
    last_sequence: Arc<AtomicI64>,
}

/// Creates a producer on `client`'s loop: opens the layout watch, starts the producer's task, and waits for it to
/// be created.
async fn create_on(client: local::PulsarClient, topic: String, options: ProducerOptions) -> Result<Link> {
    let watch = client.watches().open(&client, &topic).await?;
    let machine = Machine::new(options, watch.current(), Instant::now());
    let resolved = machine.topic().to_string();
    let (requests, receiver) = mpsc::unbounded();
    let (created, is_created) = oneshot::channel();
    let last_sequence = Arc::new(AtomicI64::new(-1));
    let task = Task::new(machine, client.clone(), watch, receiver, last_sequence.clone(), created);
    let closer = requests.clone();
    let close = move || {
        let _ = closer.unbounded_send(Request::Close { reply: None });
    };
    client.adopt(close, task.run())?;
    is_created.await.map_err(producer_closed)??;
    Ok(Link { requests, topic: resolved, last_sequence })
}

/// A producer on one topic. Cheap to clone: clones are the same producer. `Api` is [`ThreadSafe`] (the default),
/// whose producer is `Send + Sync`, or [`Local`].
///
/// Dropping the last clone closes the producer in the background; [`Producer::close`] waits for the close.
pub struct Producer<T, Api = ThreadSafe> {
    shared: Arc<Shared>,
    schema: Schema<T>,
    _api: PhantomData<Api>,
}

struct Shared {
    link: Link,
    memory: Arc<MemoryLimit>,
    name: Option<String>,
    block_if_queue_full: bool,
    /// The thread-safe client's loop, to count the producer off its load.
    _slot: Option<LoopSlot>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        let _ = self.link.requests.unbounded_send(Request::Close { reply: None });
    }
}

impl Shared {
    /// Queues a message on the producer's task, once it has room under the memory limit, and waits for it to be
    /// stored. With `flush`, its batch goes out at once.
    async fn send(&self, message: OutgoingMessage, flush: bool) -> Result<MessageId> {
        let room = message.payload.len() as u64 + PER_MESSAGE_OVERHEAD;
        let room = self.memory.reserve(room, self.block_if_queue_full).await?;
        let (reply, done) = oneshot::channel();
        // A request the task never takes gives its room back as it drops.
        let request = Request::Send { message, publish_time: now_millis(), room, flush, reply };
        self.link.requests.unbounded_send(request).map_err(producer_closed)?;
        done.await.map_err(producer_closed)?
    }
}

/// The error of an operation on a closed producer: its task is gone, or went before answering.
fn producer_closed<E>(_: E) -> Error {
    Error::new(ErrorKind::AlreadyClosed, "the producer is closed")
}

impl<T, Api> Clone for Producer<T, Api> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), schema: self.schema.clone(), _api: PhantomData }
    }
}

impl<T, Api> std::fmt::Debug for Producer<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Producer")
            .field("topic", &self.shared.link.topic)
            .field("name", &self.shared.name)
            .finish_non_exhaustive()
    }
}

impl<T: 'static, Api> Producer<T, Api> {
    fn from_link(
        link: Link,
        schema: Schema<T>,
        memory: Arc<MemoryLimit>,
        settings: Settings,
        slot: Option<LoopSlot>,
    ) -> Self {
        let shared = Shared {
            link,
            memory,
            name: settings.name,
            block_if_queue_full: settings.block_if_queue_full,
            _slot: slot,
        };
        Self { shared: Arc::new(shared), schema, _api: PhantomData }
    }

    /// The topic, as the broker resolved it: `topic://tenant/namespace/name`.
    pub fn topic(&self) -> &str {
        &self.shared.link.topic
    }

    /// The name the producer was given, if any.
    pub fn producer_name(&self) -> Option<&str> {
        self.shared.name.as_deref()
    }

    /// The highest sequence id the brokers confirmed so far, or the initial one; -1 before any.
    pub fn last_sequence_id(&self) -> i64 {
        self.shared.link.last_sequence.load(Ordering::Acquire)
    }

    /// A message to fill in and send.
    pub fn new_message(&self) -> MessageBuilder<'_, T, Api> {
        MessageBuilder { producer: self, value: None, message: OutgoingMessage::default() }
    }

    /// Sends a message with only a value. See [`MessageBuilder::send`].
    pub async fn send(&self, value: T) -> Result<MessageId> {
        self.new_message().value(value).send().await
    }

    /// Waits for every message sent before the call to be stored, and fails if one of them failed. The open batches
    /// go out at once, without waiting out the batching delay.
    pub async fn flush(&self) -> Result<()> {
        let (reply, done) = oneshot::channel();
        self.shared.link.requests.unbounded_send(Request::Flush { reply }).map_err(producer_closed)?;
        done.await.map_err(producer_closed)?
    }

    /// Closes the producer: waits for the messages the brokers already have, fails those still waiting, in an open
    /// batch or for a layout, a connection or a segment's producer, with [`ErrorKind::AlreadyClosed`], and closes every
    /// segment's producer. Later sends fail. Can be called again. Call [`Producer::flush`] first to publish
    /// everything.
    pub async fn close(&self) -> Result<()> {
        let (reply, done) = oneshot::channel();
        if self.shared.link.requests.unbounded_send(Request::Close { reply: Some(reply) }).is_err() {
            // The task is gone: closed already.
            return Ok(());
        }
        done.await.unwrap_or(Ok(()))
    }

    async fn dispatch(&self, value: Option<T>, mut message: OutgoingMessage, flush: bool) -> Result<MessageId> {
        let value = value.ok_or_else(|| Error::new(ErrorKind::InvalidConfiguration, "a message needs a value"))?;
        message.payload = self.schema.encode(&value)?;
        self.shared.send(message, flush).await
    }
}

/// A message being built: [`Producer::new_message`].
#[must_use = "a message is sent only when `send` is awaited"]
pub struct MessageBuilder<'a, T, Api = ThreadSafe> {
    producer: &'a Producer<T, Api>,
    value: Option<T>,
    message: OutgoingMessage,
}

impl<T, Api> std::fmt::Debug for MessageBuilder<'_, T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("MessageBuilder").field("key", &self.message.key).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> MessageBuilder<'_, T, Api> {
    /// The value, encoded with the producer's schema. Required.
    pub fn value(mut self, value: T) -> Self {
        self.value = Some(value);
        self
    }

    /// The key: messages with the same key are stored in send order, across segment splits and merges.
    pub fn key(mut self, key: impl Into<String>) -> Self {
        self.message.key = Some(key.into());
        self
    }

    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.message.properties.push((key.into(), value.into()));
        self
    }

    pub fn event_time(mut self, time: SystemTime) -> Self {
        self.message.event_time = Some(millis(time));
        self
    }

    /// A sequence id for deduplication, instead of the producer's next one.
    pub fn sequence_id(mut self, sequence_id: u64) -> Self {
        self.message.sequence_id = Some(sequence_id);
        self
    }

    /// Delivers the message to consumers no sooner than `delay` from now.
    pub fn deliver_after(mut self, delay: Duration) -> Self {
        self.message.deliver_at = Some(now_millis() as i64 + delay.as_millis() as i64);
        self
    }

    /// Delivers the message to consumers no sooner than `time`.
    pub fn deliver_at(mut self, time: SystemTime) -> Self {
        self.message.deliver_at = Some(millis(time) as i64);
        self
    }

    /// The clusters to replicate the message to, instead of the namespace's.
    pub fn replication_clusters(mut self, clusters: impl IntoIterator<Item = impl Into<String>>) -> Self {
        self.message.replicate_to = Some(clusters.into_iter().map(Into::into).collect());
        self
    }

    /// Sends the message, and completes with its id once it is stored.
    ///
    /// The message is queued when the future is first polled, so sends are published in the order their futures
    /// are first polled, which is call order for code that awaits each one or drives them with `buffered`,
    /// `FuturesOrdered` or `join_all`. Spawning each send as a task of its own gives no order. Dropping the future
    /// after its first poll doesn't recall the message: it may still be published. The producer's send timeout
    /// bounds how long a message waits.
    ///
    /// With batching, on by default, the message waits up to the batching delay (1 ms by default) for others to
    /// join its batch. A loop that awaits each send pays that delay for every message: pipeline the sends, as above,
    /// or call [`Producer::flush`], or turn batching off ([`ProducerBuilder::batching_policy`]).
    pub async fn send(self) -> Result<MessageId> {
        self.producer.dispatch(self.value, self.message, false).await
    }

    /// Sends the message with its batch at once, as Java's blocking send does.
    pub(crate) async fn send_and_flush(self) -> Result<MessageId> {
        self.producer.dispatch(self.value, self.message, true).await
    }
}

/// The canonical form of a topic name, as Java's `TopicName.get` gives it: a short name is a `persistent://` topic
/// in `public/default`, or in the namespace given.
pub(crate) fn normalize_topic(topic: Option<&str>) -> Result<String> {
    let invalid = |message: String| Error::new(ErrorKind::InvalidConfiguration, message);
    let topic = topic.ok_or_else(|| invalid("a topic is required".into()))?;
    if let Some((domain, rest)) = topic.split_once("://") {
        return match domain {
            "topic" | "persistent" if rest.split('/').count() >= 3 && !rest.split('/').any(str::is_empty) => {
                Ok(topic.to_string())
            }
            "non-persistent" => Err(invalid(format!("non-persistent topics are not supported: {topic}"))),
            _ => Err(invalid(format!("not a topic name: {topic}"))),
        };
    }
    match topic.split('/').collect::<Vec<_>>()[..] {
        [name] if !name.is_empty() => Ok(format!("persistent://public/default/{name}")),
        [tenant, namespace, name] if ![tenant, namespace, name].contains(&"") => {
            Ok(format!("persistent://{tenant}/{namespace}/{name}"))
        }
        _ => Err(invalid(format!("not a topic name: {topic}"))),
    }
}

fn millis(time: SystemTime) -> u64 {
    time.duration_since(UNIX_EPOCH).map_or(0, |d| d.as_millis() as u64)
}

fn now_millis() -> u64 {
    millis(SystemTime::now())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn topic_names() {
        let n = |t: &str| normalize_topic(Some(t));
        assert_eq!(n("topic://a/b/c").unwrap(), "topic://a/b/c");
        assert_eq!(n("persistent://a/b/c").unwrap(), "persistent://a/b/c");
        assert_eq!(n("orders").unwrap(), "persistent://public/default/orders");
        assert_eq!(n("acme/sales/orders").unwrap(), "persistent://acme/sales/orders");
        for bad in ["non-persistent://a/b/c", "segment://a/b/c/0000-ffff-0", "a/b", "", "topic://a/b", "topic://a//c"] {
            assert_eq!(n(bad).unwrap_err().kind(), ErrorKind::InvalidConfiguration, "{bad}");
        }
        assert_eq!(normalize_topic(None).unwrap_err().kind(), ErrorKind::InvalidConfiguration);
    }
}
