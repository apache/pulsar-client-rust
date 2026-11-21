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

//! Stream consumers (docs/7-api.md §4, docs/6-scalable-topics.md §5).

use std::collections::BTreeSet;
use std::hash::{BuildHasher, RandomState};
use std::marker::PhantomData;
use std::sync::Arc;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::stream_consumer::StreamConsumer as Machine;
use futures_channel::{mpsc, oneshot};
use futures_util::Stream;

use super::stream_task::{Request, Task};
use super::{Message, Settings, SubscriptionInitialPosition, consumer_closed, consumer_options, invalid};
use crate::client::{LoopSlot, PulsarClient};
use crate::config::{Blocking, Local, ThreadSafe};
use crate::error::Result;
use crate::local;
use crate::message_id::MessageId;
use crate::producer::ApiClient;
use crate::schema::Schema;

/// Builds a Stream consumer: `client.new_stream_consumer(schema)`. `Api` is [`ThreadSafe`] or [`Local`], as for the
/// client.
#[must_use = "a builder does nothing until `subscribe` is awaited"]
pub struct StreamConsumerBuilder<T, Api = ThreadSafe> {
    client: ApiClient,
    schema: Schema<T>,
    settings: Settings,
    _api: PhantomData<Api>,
}

impl<T, Api> std::fmt::Debug for StreamConsumerBuilder<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConsumerBuilder").field("settings", &self.settings).finish_non_exhaustive()
    }
}

impl<T: 'static, Api> StreamConsumerBuilder<T, Api> {
    /// The topic: `topic://tenant/namespace/name` for a scalable topic. A `persistent://` name, or the short forms
    /// `tenant/namespace/name` and `name`, address a regular topic through the migration bridge. Required.
    pub fn topic(mut self, topic: impl Into<String>) -> Self {
        self.settings.topic = Some(topic.into());
        self
    }

    /// The subscription: the consumers with the same subscription share the topic, each key going to one of them.
    /// Required.
    pub fn subscription_name(mut self, name: impl Into<String>) -> Self {
        self.settings.subscription = Some(name.into());
        self
    }

    /// The consumer's name. The topic's controller knows the consumer by it: one that comes back under the same name
    /// within the controller's grace period gets the same segments. Each segment's consumer is named
    /// `<name>-seg-<id>`. Without a name, the consumer gets a random one.
    pub fn consumer_name(mut self, name: impl Into<String>) -> Self {
        self.settings.consumer_name = Some(name.into());
        self
    }

    /// Where a new subscription starts: [`SubscriptionInitialPosition::Latest`] by default. It applies to the
    /// segments the topic has when the consumer subscribes; a segment a split or a merge adds later is read from its
    /// start. The topic's controller hands out a segment only once the segments before it are read: at `Latest`, on a
    /// topic split before the subscription existed, the consumer skips what producers store in the newer segments until
    /// it gets them.
    pub fn subscription_initial_position(mut self, position: SubscriptionInitialPosition) -> Self {
        self.settings.initial_position = position;
        self
    }

    /// How long acknowledgments wait to go out together: 100 ms by default. [`Duration::ZERO`] sends each at once.
    pub fn acknowledgment_group_time(mut self, time: Duration) -> Self {
        self.settings.ack_group_time = time;
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

    /// The topic, the consumer's name and the segment consumers' options, once checked.
    fn options(&self, operation_timeout: Duration) -> Result<(String, String, ConsumerOptions)> {
        let (topic, options) = consumer_options(&self.settings, self.schema.info(), operation_timeout)?;
        let name = self
            .settings
            .consumer_name
            .clone()
            .filter(|name| !name.is_empty())
            .unwrap_or_else(|| random_name("stream"));
        Ok((topic, name, options))
    }
}

/// A name for a consumer of `kind` without one, as Java's: `v5-<kind>-` and 8 random letters and digits.
pub(super) fn random_name(kind: &str) -> String {
    const ALPHABET: &[u8] = b"abcdefghijklmnopqrstuvwxyzABCDEFGHIJKLMNOPQRSTUVWXYZ0123456789";
    let mut bits = RandomState::new().hash_one(Instant::now());
    let suffix: String = (0..8)
        .map(|_| {
            let c = ALPHABET[(bits % ALPHABET.len() as u64) as usize];
            bits /= ALPHABET.len() as u64;
            c as char
        })
        .collect();
    format!("v5-{kind}-{suffix}")
}

impl<T: 'static> StreamConsumerBuilder<T, ThreadSafe> {
    pub(crate) fn new(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the client's least loaded event loop: registers with the topic's controller, and subscribes to
    /// every segment it assigns.
    pub async fn subscribe(self) -> Result<StreamConsumer<T>> {
        let ApiClient::ThreadSafe(client) = &self.client else { unreachable!() };
        let (topic, name, options) = self.options(client.operation_timeout())?;
        let (slot, index) = client.place();
        let consumer_name = name.clone();
        let link = client
            .run_on(index, move |local| Box::pin(async move { subscribe_on(local, topic, name, options).await }))
            .await?;
        Ok(StreamConsumer::from_link(link, self.schema, self.settings, consumer_name, Some(slot)))
    }
}

impl<T: 'static> StreamConsumerBuilder<T, Blocking> {
    pub(crate) fn new_blocking(client: PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::ThreadSafe(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the client's least loaded event loop, and blocks until every assigned segment is subscribed.
    pub fn subscribe(self) -> Result<crate::blocking::StreamConsumer<T>> {
        let builder = StreamConsumerBuilder::<T, ThreadSafe> {
            client: self.client,
            schema: self.schema,
            settings: self.settings,
            _api: PhantomData,
        };
        futures_executor::block_on(builder.subscribe()).map(crate::blocking::StreamConsumer::new)
    }
}

impl<T: 'static> StreamConsumerBuilder<T, Local> {
    pub(crate) fn new_local(client: local::PulsarClient, schema: Schema<T>) -> Self {
        Self { client: ApiClient::Local(client), schema, settings: Settings::default(), _api: PhantomData }
    }

    /// Subscribes, on the local client's loop: registers with the topic's controller, and subscribes to every segment
    /// it assigns.
    pub async fn subscribe(self) -> Result<StreamConsumer<T, Local>> {
        let ApiClient::Local(client) = &self.client else { unreachable!() };
        let (topic, name, options) = self.options(client.operation_timeout())?;
        let link = subscribe_on(client.clone(), topic, name.clone(), options).await?;
        Ok(StreamConsumer::from_link(link, self.schema, self.settings, name, None))
    }
}

/// What a subscribed Stream consumer's handle holds of its task. Dropping it closes the consumer in the background.
pub(crate) struct Link {
    requests: mpsc::UnboundedSender<Request>,
    topic: Arc<str>,
}

impl Drop for Link {
    fn drop(&mut self) {
        let _ = self.requests.unbounded_send(Request::Close { reply: None });
    }
}

/// Subscribes a Stream consumer on `client`'s loop: reads the topic's layout, starts the consumer's task, and waits
/// for its registration and every assigned segment to be subscribed.
async fn subscribe_on(
    client: local::PulsarClient,
    topic: String,
    name: String,
    options: ConsumerOptions,
) -> Result<Link> {
    // Held until subscribed: the task's first look for the controller shares it.
    let watch = client.watches().open(&client, &topic).await?;
    let layout = watch.current();
    let initial: BTreeSet<u64> = layout.segments().map(|segment| segment.id).collect();
    let machine = Machine::new(options, layout.topic().to_string(), name, initial, Instant::now());
    let resolved = Arc::from(machine.topic());
    let (requests, receiver) = mpsc::unbounded();
    let (subscribed, is_subscribed) = oneshot::channel();
    let task = Task::new(machine, client.clone(), topic, receiver, subscribed);
    let closer = requests.clone();
    let close = move || {
        let _ = closer.unbounded_send(Request::Close { reply: None });
    };
    client.adopt(close, task.run())?;
    is_subscribed.await.map_err(consumer_closed)??;
    Ok(Link { requests, topic: resolved })
}

/// A Stream consumer: a subscription in which each key's messages go to one consumer, in order, also across a split
/// or a merge (*client-api* §4.1). The topic's controller spreads the segments, or their entry buckets, over the
/// consumers of the subscription. Acknowledgments are cumulative. Cheap to clone: clones are the same consumer. `Api`
/// is [`ThreadSafe`] (the default), whose consumer is `Send + Sync`, or [`Local`].
///
/// Dropping the last clone closes the consumer in the background; [`StreamConsumer::close`] waits for the close.
pub struct StreamConsumer<T, Api = ThreadSafe> {
    shared: Arc<Shared>,
    schema: Schema<T>,
    _api: PhantomData<Api>,
}

struct Shared {
    link: Link,
    subscription: String,
    consumer_name: String,
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

impl<T, Api> Clone for StreamConsumer<T, Api> {
    fn clone(&self) -> Self {
        Self { shared: self.shared.clone(), schema: self.schema.clone(), _api: PhantomData }
    }
}

impl<T, Api> std::fmt::Debug for StreamConsumer<T, Api> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("StreamConsumer")
            .field("topic", &self.shared.link.topic)
            .field("subscription", &self.shared.subscription)
            .field("consumer_name", &self.shared.consumer_name)
            .finish_non_exhaustive()
    }
}

impl<T: 'static, Api> StreamConsumer<T, Api> {
    fn from_link(link: Link, schema: Schema<T>, settings: Settings, name: String, slot: Option<LoopSlot>) -> Self {
        let subscription = settings.subscription.unwrap_or_default();
        let shared = Shared { link, subscription, consumer_name: name, _slot: slot };
        Self { shared: Arc::new(shared), schema, _api: PhantomData }
    }

    /// The topic, as the broker resolved it: `topic://tenant/namespace/name`.
    pub fn topic(&self) -> &str {
        &self.shared.link.topic
    }

    pub fn subscription(&self) -> &str {
        &self.shared.subscription
    }

    /// The name the controller knows the consumer by: the one it was given, or a random one.
    pub fn consumer_name(&self) -> &str {
        &self.shared.consumer_name
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

    /// Acknowledges the message and every message received before it, from every part of the topic (*client-api*
    /// §4.1). The ack goes out with the others of its group; the broker doesn't confirm it. Fails only once the
    /// consumer is closed.
    pub async fn acknowledge_cumulative(&self, id: &MessageId) -> Result<()> {
        let (reply, done) = oneshot::channel();
        self.shared.link.requests.unbounded_send(Request::Ack { id: id.0.clone(), reply }).map_err(consumer_closed)?;
        done.await.map_err(consumer_closed)?
    }

    /// Closes the consumer: sends the acks waiting, closes every segment's consumer, and leaves the controller's
    /// group, which rebalances at once. The receives waiting fail with
    /// [`ErrorKind::AlreadyClosed`](crate::ErrorKind::AlreadyClosed), and the messages not acknowledged go to the
    /// subscription's other consumers. Can be called again.
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
