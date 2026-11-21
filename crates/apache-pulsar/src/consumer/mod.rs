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

//! Consumers (docs/7-api.md §4) and the messages they receive (§5).

mod stream;
mod stream_task;
mod task;

pub use stream::{StreamConsumer, StreamConsumerBuilder};
pub(crate) use task::Request;

use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::proto;
use apache_pulsar_protocol::proto::command_subscribe::InitialPosition;
use apache_pulsar_protocol::queue_consumer::QueueConsumer as Machine;
use futures_channel::{mpsc, oneshot};
use futures_util::Stream;

use crate::client::{LoopSlot, PulsarClient};
use crate::config::{Blocking, Local, ThreadSafe};
use crate::error::{Error, ErrorKind, Result};
use crate::local;
use crate::message_id::MessageId;
use crate::producer::{ApiClient, normalize_topic};
use crate::schema::{Schema, SchemaInfo};
use task::Task;

/// Where a new subscription starts on each segment (*client-api* §4).
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq, Hash)]
#[non_exhaustive]
pub enum SubscriptionInitialPosition {
    /// At the oldest message the segment has.
    Earliest,
    /// After the newest: only the messages published from then on.
    #[default]
    Latest,
}

/// Builds a Queue consumer: `client.new_queue_consumer(schema)`. `Api` is [`ThreadSafe`] or [`Local`], as for the
/// client.
#[must_use = "a builder does nothing until `subscribe` is awaited"]
pub struct QueueConsumerBuilder<T, Api = ThreadSafe> {
    client: ApiClient,
    schema: Schema<T>,
    settings: Settings,
    _api: PhantomData<Api>,
}

#[derive(Debug, Clone)]
struct Settings {
    topic: Option<String>,
    subscription: Option<String>,
    consumer_name: Option<String>,
    initial_position: SubscriptionInitialPosition,
    receiver_queue_size: u32,
    ack_group_time: Duration,
    max_ack_group_size: usize,
    negative_ack_delay: Duration,
    priority_level: Option<i32>,
    subscription_properties: Vec<(String, String)>,
    properties: Vec<(String, String)>,
}

impl Default for Settings {
    fn default() -> Self {
        // Java's defaults.
        let options = ConsumerOptions::default();
        Self {
            topic: None,
            subscription: None,
            consumer_name: None,
            initial_position: SubscriptionInitialPosition::Latest,
            receiver_queue_size: options.receiver_queue_size,
            ack_group_time: options.ack_group_time,
            max_ack_group_size: options.max_ack_group_size,
            negative_ack_delay: options.negative_ack_delay,
            priority_level: None,
            subscription_properties: Vec::new(),
            properties: Vec::new(),
        }
    }
}

impl<T, Api> std::fmt::Debug for QueueConsumerBuilder<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueConsumerBuilder").field("settings", &self.settings).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> QueueConsumerBuilder<T, Api> {
    /// The topic: `topic://tenant/namespace/name` for a scalable topic. A `persistent://` name, or the short forms
    /// `tenant/namespace/name` and `name`, address a regular topic through the migration bridge. Required.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.settings.topic = Some(topic.into());
        self
    }

    /// The subscription: consumers with the same subscription share its messages. Required.
    pub fn subscription_name(mut self, name: impl Into<String>) -> Self {
        self.settings.subscription = Some(name.into());
        self
    }

    /// The consumer's name. Each segment's consumer is named `<name>-seg-<id>`; without a name, the brokers pick
    /// them.
    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.settings.consumer_name = Some(name.into());
        self
    }

    /// Where a new subscription starts: [`SubscriptionInitialPosition::Latest`] by default. It applies to the
    /// segments the topic has when the consumer subscribes. The segments a split or a merge adds later are read from
    /// their start: the broker creates them without the subscription, and what they stored before the consumer
    /// subscribed would be missed otherwise.
    pub fn subscription_initial_position(mut self, position: SubscriptionInitialPosition) -> Self {
        self.settings.initial_position = position;
        self
    }

    /// How many messages each segment's broker may push ahead of the application: 1000 by default, at least 1.
    pub fn receiver_queue_size(mut self, size: u32) -> Self {
        self.settings.receiver_queue_size = size;
        self
    }

    /// How long acknowledgments wait to go out together: 100 ms by default. [`Duration::ZERO`] sends each at once.
    pub fn acknowledgment_group_time(mut self, time: Duration) -> Self {
        self.settings.ack_group_time = time;
        self
    }

    /// How many acknowledgments go out together at most, whatever the group time: 1000 by default, at least 1.
    pub fn max_acknowledgment_group_size(mut self, size: usize) -> Self {
        self.settings.max_ack_group_size = size;
        self
    }

    /// How long a negatively acknowledged message waits before the broker delivers it again: 60 s by default.
    pub fn negative_ack_redelivery_delay(mut self, delay: Duration) -> Self {
        self.settings.negative_ack_delay = delay;
        self
    }

    /// The consumer's priority in the subscription: the brokers give messages to consumers of the lowest level first.
    pub fn priority_level(mut self, level: i32) -> Self {
        self.settings.priority_level = Some(level);
        self
    }

    /// A property of the subscription, set when it is created.
    pub fn subscription_property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.subscription_properties.push((key.into(), value.into()));
        self
    }

    /// A property sent to the broker with the consumer.
    pub fn property(mut self, key: impl Into<String>, value: impl Into<String>) -> Self {
        self.settings.properties.push((key.into(), value.into()));
        self
    }

    fn options(&self, operation_timeout: Duration) -> Result<(String, ConsumerOptions)> {
        consumer_options(&self.settings, self.schema.info(), operation_timeout)
    }
}

/// The topic and the segment consumers' options, once checked. Not generic: one copy serves every schema and client.
fn consumer_options(
    settings: &Settings,
    schema: Option<&SchemaInfo>,
    operation_timeout: Duration,
) -> Result<(String, ConsumerOptions)> {
    let topic = normalize_topic(settings.topic.as_deref())?;
    let Some(subscription) = settings.subscription.clone().filter(|name| !name.is_empty()) else {
        return Err(invalid("a subscription name is required"));
    };
    if settings.receiver_queue_size == 0 || settings.max_ack_group_size == 0 {
        return Err(invalid("the receiver queue size and the acknowledgment group size must be at least 1"));
    }
    let initial_position = match settings.initial_position {
        SubscriptionInitialPosition::Earliest => InitialPosition::Earliest,
        SubscriptionInitialPosition::Latest => InitialPosition::Latest,
    };
    let options = ConsumerOptions {
        subscription,
        consumer_name: settings.consumer_name.clone(),
        initial_position,
        receiver_queue_size: settings.receiver_queue_size,
        ack_group_time: settings.ack_group_time,
        max_ack_group_size: settings.max_ack_group_size,
        negative_ack_delay: settings.negative_ack_delay,
        priority_level: settings.priority_level,
        subscription_properties: settings.subscription_properties.clone(),
        properties: settings.properties.clone(),
        schema: schema.map(|info| proto::Schema {
            name: info.name.clone(),
            schema_data: info.data.clone(),
            r#type: info.kind,
            ..Default::default()
        }),
        operation_timeout,
        ..ConsumerOptions::default()
    };
    Ok((topic, options))
}

fn invalid(message: &str) -> Error {
    Error::new(ErrorKind::InvalidConfiguration, message)
}

impl<T: 'static> QueueConsumerBuilder<T, ThreadSafe> {
    pub(crate) fn new(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the client's least loaded event loop, to every segment of the topic.
    pub async fn subscribe(self) -> Result<QueueConsumer<T>> {
        let ApiClient::ThreadSafe(client) = &self.client else { unreachable!() };
        let (topic, options) = self.options(client.operation_timeout())?;
        let (slot, index) = client.place();
        let link = client
            .run_on(index, move |local| Box::pin(async move { subscribe_on(local, topic, options).await }))
            .await?;
        Ok(QueueConsumer::from_link(link, self.schema, self.settings, Some(slot)))
    }
}

impl<T: 'static> QueueConsumerBuilder<T, Blocking> {
    pub(crate) fn new_blocking(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the client's least loaded event loop, and blocks until every segment is subscribed.
    pub fn subscribe(self) -> Result<crate::blocking::QueueConsumer<T>> {
        let builder = QueueConsumerBuilder::<T, ThreadSafe> {
            client: self.client,
            schema: self.schema,
            settings: self.settings,
            _api: PhantomData,
        };
        futures_executor::block_on(builder.subscribe()).map(crate::blocking::QueueConsumer::new)
    }
}

impl<T: 'static> QueueConsumerBuilder<T, Local> {
    pub(crate) fn new_local(client: local::PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::Local(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the local client's loop, to every segment of the topic.
    pub async fn subscribe(self) -> Result<QueueConsumer<T, Local>> {
        let ApiClient::Local(client) = &self.client else { unreachable!() };
        let (topic, options) = self.options(client.operation_timeout())?;
        let link = subscribe_on(client.clone(), topic, options).await?;
        Ok(QueueConsumer::from_link(link, self.schema, self.settings, None))
    }
}

/// What a subscribed consumer's handle holds of its task. Dropping it closes the consumer in the background: with
/// the last handle, or when nobody is left to take it, as a subscribe dropped on another thread leaves it.
pub(crate) struct Link {
    requests: mpsc::UnboundedSender<Request>,
    topic: Arc<str>,
}

impl Drop for Link {
    fn drop(&mut self) {
        let _ = self.requests.unbounded_send(Request::Close { reply: None });
    }
}

/// Subscribes a Queue consumer on `client`'s loop: opens the layout watch, starts the consumer's task, and waits for
/// every segment to be subscribed.
async fn subscribe_on(client: local::PulsarClient, topic: String, options: ConsumerOptions) -> Result<Link> {
    let watch = client.watches().open(&client, &topic).await?;
    let machine = Machine::new(options, watch.current(), Instant::now());
    let resolved = Arc::from(machine.topic());
    let (requests, receiver) = mpsc::unbounded();
    let (subscribed, is_subscribed) = oneshot::channel();
    let task = Task::new(machine, client.clone(), watch, receiver, subscribed);
    let closer = requests.clone();
    let close = move || {
        let _ = closer.unbounded_send(Request::Close { reply: None });
    };
    client.adopt(close, task.run())?;
    is_subscribed.await.map_err(consumer_closed)??;
    Ok(Link { requests, topic: resolved })
}

/// A Queue consumer: a Shared subscription on every segment of a topic, whose messages it delivers in no particular
/// order across segments (*client-api* §4). Each message is acknowledged on its own. Cheap to clone: clones are the
/// same consumer. `Api` is [`ThreadSafe`] (the default), whose consumer is `Send + Sync`, or [`Local`].
///
/// Dropping the last clone closes the consumer in the background; [`QueueConsumer::close`] waits for the close.
pub struct QueueConsumer<T, Api = ThreadSafe> {
    shared: Arc<Shared>,
    schema: Schema<T>,
    _api: PhantomData<Api>,
}

struct Shared {
    link: Link,
    subscription: String,
    consumer_name: Option<String>,
    /// The thread-safe client's loop, to count the consumer off its load.
    _slot: Option<LoopSlot>,
}

impl Shared {
    /// Up to `max` messages: once there are, or at the deadline with what there is.
    async fn receive(&self, max: usize, deadline: Option<Instant>) -> Result<Vec<ReceivedMessage>> {
        let (reply, answer) = oneshot::channel();
        let request = Request::Receive { max, deadline, reply };
        self.link.requests.unbounded_send(request).map_err(consumer_closed)?;
        let mut pending = Pending { answer, requests: self.link.requests.clone() };
        (&mut pending.answer).await.map_err(consumer_closed)
    }
}

/// A receive's answer on its way. Dropped with the answer in, it gives the messages back, for the next receives: a
/// receive dropped before it completes loses no message.
struct Pending {
    answer: oneshot::Receiver<Vec<ReceivedMessage>>,
    requests: mpsc::UnboundedSender<Request>,
}

impl Drop for Pending {
    fn drop(&mut self) {
        self.answer.close();
        if let Ok(Some(messages)) = self.answer.try_recv() {
            let _ = self.requests.unbounded_send(Request::Return(messages));
        }
    }
}

/// The error of an operation on a closed consumer: its task is gone, or went before answering.
fn consumer_closed<E>(_: E) -> Error {
    Error::new(ErrorKind::AlreadyClosed, "the consumer is closed")
}

impl<T, Api> Clone for QueueConsumer<T, Api> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), schema: self.schema.clone(), _api: PhantomData }
    }
}

impl<T, Api> std::fmt::Debug for QueueConsumer<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("QueueConsumer")
            .field("topic", &self.shared.link.topic)
            .field("subscription", &self.shared.subscription)
            .finish_non_exhaustive()
    }
}

impl<T: 'static, Api> QueueConsumer<T, Api> {
    fn from_link(link: Link, schema: Schema<T>, settings: Settings, slot: Option<LoopSlot>) -> Self {
        let subscription = settings.subscription.unwrap_or_default();
        let shared = Shared { link, subscription, consumer_name: settings.consumer_name, _slot: slot };
        Self { shared: Arc::new(shared), schema, _api: PhantomData }
    }

    /// The topic, as the broker resolved it: `topic://tenant/namespace/name`.
    pub fn topic(&self) -> &str {
        &self.shared.link.topic
    }

    pub fn subscription(&self) -> &str {
        &self.shared.subscription
    }

    /// The name the consumer was given, if any.
    pub fn consumer_name(&self) -> Option<&str> {
        self.shared.consumer_name.as_deref()
    }

    /// The next message. Fails only once the consumer is closed.
    ///
    /// Receives are answered in the order they are first polled. A receive dropped before it completes loses no
    /// message: the next receive gets it.
    pub async fn receive(&self) -> Result<Message<T>> {
        let mut messages = self.shared.receive(1, None).await?;
        Ok(self.message(messages.pop().expect("a receive without a deadline gets its message")))
    }

    /// The next message, or `None` if none comes within `timeout`.
    pub async fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message<T>>> {
        let messages = self.shared.receive(1, Some(Instant::now() + timeout)).await?;
        Ok(messages.into_iter().next().map(|message| self.message(message)))
    }

    /// Up to `max` messages: as soon as there are `max`, or once `timeout` passes with those there are, none
    /// included.
    pub async fn receive_batch(&self, max: usize, timeout: Duration) -> Result<Vec<Message<T>>> {
        if max == 0 {
            return Err(invalid("a batch receive takes at least one message"));
        }
        let messages = self.shared.receive(max, Some(Instant::now() + timeout)).await?;
        Ok(messages.into_iter().map(|message| self.message(message)).collect())
    }

    /// The messages, as a stream that ends once the consumer is closed.
    pub fn stream(&self) -> impl Stream<Item = Message<T>> + '_ {
        futures_util::stream::unfold(self, |consumer| async move {
            consumer.receive().await.ok().map(|message| (message, consumer))
        })
    }

    /// Acknowledges a message. The ack goes out with the others of its group; the broker doesn't confirm it. Fails
    /// only once the consumer is closed. A message of a segment the consumer no longer has is acknowledged already
    /// or gone: its ack does nothing, as in Java.
    pub async fn acknowledge(&self, id: &MessageId) -> Result<()> {
        let (reply, done) = oneshot::channel();
        self.shared.link.requests.unbounded_send(Request::Ack { id: id.0.clone(), reply }).map_err(consumer_closed)?;
        done.await.map_err(consumer_closed)?
    }

    /// Asks for a message again after the negative ack delay: the broker delivers its whole entry again. Fails only
    /// once the consumer is closed.
    pub fn negative_acknowledge(&self, id: &MessageId) -> Result<()> {
        self.shared.link.requests.unbounded_send(Request::Nack { id: id.0.clone() }).map_err(consumer_closed)
    }

    /// Closes the consumer: sends the acks waiting, then closes every segment's consumer. The receives waiting fail
    /// with [`ErrorKind::AlreadyClosed`], and the messages not received yet go to the subscription's other
    /// consumers. Can be called again.
    pub async fn close(&self) -> Result<()> {
        let (reply, done) = oneshot::channel();
        if self.shared.link.requests.unbounded_send(Request::Close { reply: Some(reply) }).is_err() {
            // The task is gone: closed already.
            return Ok(());
        }
        done.await.unwrap_or(Ok(()))
    }

    fn message(&self, message: ReceivedMessage) -> Message<T> {
        Message {
            id: MessageId(message.id.clone()),
            message,
            topic: self.shared.link.topic.clone(),
            schema: self.schema.clone(),
        }
    }
}

/// A message a consumer received (*data-model* §3). Immutable, and cheap to clone.
pub struct Message<T> {
    id: MessageId,
    message: ReceivedMessage,
    topic: Arc<str>,
    schema: Schema<T>,
}

impl<T> Clone for Message<T> {
    fn clone(&self) -> Self {
        Self {
            id: self.id.clone(),
            message: self.message.clone(),
            topic: self.topic.clone(),
            schema: self.schema.clone(),
        }
    }
}

impl<T> std::fmt::Debug for Message<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Message")
            .field("id", &self.id)
            .field("topic", &self.topic)
            .field("key", &self.message.key)
            .field("size", &self.size())
            .finish_non_exhaustive()
    }
}

impl<T> Message<T> {
    /// The value, decoded with the consumer's schema each time.
    pub fn value(&self) -> Result<T> {
        self.schema.decode(&self.message.payload)
    }

    /// The payload as it came, without a copy.
    pub fn data(&self) -> &[u8] {
        &self.message.payload
    }

    pub fn id(&self) -> &MessageId {
        &self.id
    }

    pub fn key(&self) -> Option<&str> {
        self.message.key.as_deref()
    }

    /// The properties the producer set, in the order it set them.
    pub fn properties(&self) -> impl Iterator<Item = (&str, &str)> {
        self.message.properties.iter().map(|property| (property.key.as_str(), property.value.as_str()))
    }

    /// When the producer published the message.
    pub fn publish_time(&self) -> SystemTime {
        UNIX_EPOCH + Duration::from_millis(self.message.entry.publish_time)
    }

    /// The event time the producer set, if any.
    pub fn event_time(&self) -> Option<SystemTime> {
        self.message.event_time.map(|millis| UNIX_EPOCH + Duration::from_millis(millis))
    }

    pub fn sequence_id(&self) -> u64 {
        self.message.sequence_id
    }

    /// The name of the producer that published the message.
    pub fn producer_name(&self) -> Option<&str> {
        Some(self.message.entry.producer_name.as_str()).filter(|name| !name.is_empty())
    }

    /// The topic the message came from, as the broker resolved it: the scalable topic, not its segment.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// How many times the broker delivered the message before, as far as it counted.
    pub fn redelivery_count(&self) -> u32 {
        self.message.redelivery_count
    }

    /// The payload's size, in bytes.
    pub fn size(&self) -> usize {
        self.message.payload.len()
    }

    /// The cluster the message was replicated from, for a replicated message.
    pub fn replicated_from(&self) -> Option<&str> {
        self.message.entry.replicated_from.as_deref()
    }
}
