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

//! The blocking API: the thread-safe client, producers and consumers, for code without an async runtime
//! (docs/7-api.md §1).
//!
//! Each call blocks the calling thread until its operation completes. Don't call them from inside an event loop
//! or an async task: block on the thread-safe API's futures there instead.

use std::time::{Duration, SystemTime};

use futures_executor::block_on;

use crate::checkpoint::Checkpoint;
use crate::config::{Blocking, ClientBuilder};
use crate::consumer::Message;
use crate::error::Result;
use crate::message_id::MessageId;
use crate::schema::Schema;

/// A blocking Pulsar client. `Send + Sync`, and cheap to clone.
#[derive(Clone, Debug)]
pub struct PulsarClient {
    inner: crate::PulsarClient,
}

impl PulsarClient {
    pub fn builder() -> ClientBuilder<Blocking> {
        ClientBuilder::default()
    }

    /// A producer that publishes values with `schema`. See [`crate::ProducerBuilder`].
    pub fn new_producer<T: 'static>(&self, schema: Schema<T>) -> crate::ProducerBuilder<T, Blocking> {
        crate::ProducerBuilder::new_blocking(self.inner.clone(), schema)
    }

    /// A Queue consumer that reads values with `schema`. See [`crate::QueueConsumerBuilder`].
    pub fn new_queue_consumer<T: 'static>(&self, schema: Schema<T>) -> crate::QueueConsumerBuilder<T, Blocking> {
        crate::QueueConsumerBuilder::new_blocking(self.inner.clone(), schema)
    }

    /// A Stream consumer that reads values with `schema`. See [`crate::StreamConsumerBuilder`].
    pub fn new_stream_consumer<T: 'static>(&self, schema: Schema<T>) -> crate::StreamConsumerBuilder<T, Blocking> {
        crate::StreamConsumerBuilder::new_blocking(self.inner.clone(), schema)
    }

    /// A Checkpoint consumer that reads values with `schema`. See [`crate::CheckpointConsumerBuilder`].
    pub fn new_checkpoint_consumer<T: 'static>(
        &self,
        schema: Schema<T>,
    ) -> crate::CheckpointConsumerBuilder<T, Blocking> {
        crate::CheckpointConsumerBuilder::new_blocking(self.inner.clone(), schema)
    }

    /// Closes the client, its producers and consumers first. Can be called again.
    pub fn close(&self) -> Result<()> {
        block_on(self.inner.close())
    }
}

impl ClientBuilder<Blocking> {
    /// How many event loops the client runs. 1 by default.
    pub fn event_loops(mut self, loops: usize) -> Self {
        self.event_loops = loops;
        self
    }

    /// Starts the event loops and creates the client.
    pub fn build(self) -> Result<PulsarClient> {
        let inner = block_on(self.retype::<crate::ThreadSafe>().build())?;
        Ok(PulsarClient { inner })
    }
}

/// A blocking producer. `Send + Sync`, and cheap to clone. See [`crate::Producer`].
pub struct Producer<T> {
    inner: crate::Producer<T>,
}

impl<T> Clone for Producer<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> std::fmt::Debug for Producer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: 'static> Producer<T> {
    pub(crate) fn new(inner: crate::Producer<T>) -> Self {
        Self { inner }
    }

    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub fn producer_name(&self) -> Option<&str> {
        self.inner.producer_name()
    }

    pub fn last_sequence_id(&self) -> i64 {
        self.inner.last_sequence_id()
    }

    pub fn new_message(&self) -> MessageBuilder<'_, T> {
        MessageBuilder { inner: self.inner.new_message() }
    }

    /// Sends a message with only a value, and returns its id once it is stored. Its batch goes out at once, as with
    /// Java's blocking send: a loop of blocking sends doesn't wait out the batching delay for each.
    pub fn send(&self, value: T) -> Result<MessageId> {
        block_on(self.inner.new_message().value(value).send_and_flush())
    }

    /// Waits for every message sent before the call to be stored.
    pub fn flush(&self) -> Result<()> {
        block_on(self.inner.flush())
    }

    /// See [`crate::Producer::close`].
    pub fn close(&self) -> Result<()> {
        block_on(self.inner.close())
    }
}

/// A message being built: [`Producer::new_message`]. See [`crate::MessageBuilder`].
#[must_use = "a message is sent only by `send`"]
pub struct MessageBuilder<'a, T> {
    inner: crate::MessageBuilder<'a, T>,
}

impl<T> std::fmt::Debug for MessageBuilder<'_, T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: 'static> MessageBuilder<'_, T> {
    pub fn value(self, value: T) -> Self {
        Self { inner: self.inner.value(value) }
    }

    pub fn key(self, key: impl Into<String>) -> Self {
        Self { inner: self.inner.key(key) }
    }

    pub fn property(self, key: impl Into<String>, value: impl Into<String>) -> Self {
        Self { inner: self.inner.property(key, value) }
    }

    pub fn event_time(self, time: SystemTime) -> Self {
        Self { inner: self.inner.event_time(time) }
    }

    pub fn sequence_id(self, sequence_id: u64) -> Self {
        Self { inner: self.inner.sequence_id(sequence_id) }
    }

    pub fn deliver_after(self, delay: Duration) -> Self {
        Self { inner: self.inner.deliver_after(delay) }
    }

    pub fn deliver_at(self, time: SystemTime) -> Self {
        Self { inner: self.inner.deliver_at(time) }
    }

    pub fn replication_clusters(self, clusters: impl IntoIterator<Item = impl Into<String>>) -> Self {
        Self { inner: self.inner.replication_clusters(clusters) }
    }

    /// Sends the message, and returns its id once it is stored. Its batch goes out at once, as with
    /// [`Producer::send`].
    pub fn send(self) -> Result<MessageId> {
        block_on(self.inner.send_and_flush())
    }
}

/// A blocking Queue consumer. `Send + Sync`, and cheap to clone. See [`crate::QueueConsumer`].
pub struct QueueConsumer<T> {
    inner: crate::QueueConsumer<T>,
}

impl<T> Clone for QueueConsumer<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> std::fmt::Debug for QueueConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: 'static> QueueConsumer<T> {
    pub(crate) fn new(inner: crate::QueueConsumer<T>) -> Self {
        Self { inner }
    }

    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub fn subscription(&self) -> &str {
        self.inner.subscription()
    }

    pub fn consumer_name(&self) -> Option<&str> {
        self.inner.consumer_name()
    }

    /// Blocks until the next message.
    pub fn receive(&self) -> Result<Message<T>> {
        block_on(self.inner.receive())
    }

    /// Blocks until the next message, for up to `timeout`.
    pub fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message<T>>> {
        block_on(self.inner.receive_timeout(timeout))
    }

    /// Blocks until there are `max` messages, or `timeout` passes. See [`crate::QueueConsumer::receive_batch`].
    pub fn receive_batch(&self, max: usize, timeout: Duration) -> Result<Vec<Message<T>>> {
        block_on(self.inner.receive_batch(max, timeout))
    }

    pub fn acknowledge(&self, id: &MessageId) -> Result<()> {
        block_on(self.inner.acknowledge(id))
    }

    pub fn negative_acknowledge(&self, id: &MessageId) -> Result<()> {
        self.inner.negative_acknowledge(id)
    }

    /// See [`crate::QueueConsumer::close`].
    pub fn close(&self) -> Result<()> {
        block_on(self.inner.close())
    }
}

/// A blocking Stream consumer. `Send + Sync`, and cheap to clone. See [`crate::StreamConsumer`].
pub struct StreamConsumer<T> {
    inner: crate::StreamConsumer<T>,
}

impl<T> Clone for StreamConsumer<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> std::fmt::Debug for StreamConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: 'static> StreamConsumer<T> {
    pub(crate) fn new(inner: crate::StreamConsumer<T>) -> Self {
        Self { inner }
    }

    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub fn subscription(&self) -> &str {
        self.inner.subscription()
    }

    pub fn consumer_name(&self) -> &str {
        self.inner.consumer_name()
    }

    /// Blocks until the next message.
    pub fn receive(&self) -> Result<Message<T>> {
        block_on(self.inner.receive())
    }

    /// Blocks until the next message, for up to `timeout`.
    pub fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message<T>>> {
        block_on(self.inner.receive_timeout(timeout))
    }

    /// Blocks until there are `max` messages, or `timeout` passes. See [`crate::StreamConsumer::receive_batch`].
    pub fn receive_batch(&self, max: usize, timeout: Duration) -> Result<Vec<Message<T>>> {
        block_on(self.inner.receive_batch(max, timeout))
    }

    /// See [`crate::StreamConsumer::acknowledge_cumulative`].
    pub fn acknowledge_cumulative(&self, id: &MessageId) -> Result<()> {
        block_on(self.inner.acknowledge_cumulative(id))
    }

    /// See [`crate::StreamConsumer::close`].
    pub fn close(&self) -> Result<()> {
        block_on(self.inner.close())
    }
}

/// A blocking Checkpoint consumer. `Send + Sync`, and cheap to clone. See [`crate::CheckpointConsumer`].
pub struct CheckpointConsumer<T> {
    inner: crate::CheckpointConsumer<T>,
}

impl<T> Clone for CheckpointConsumer<T> {
    fn clone(&self) -> Self {
        Self { inner: self.inner.clone() }
    }
}

impl<T> std::fmt::Debug for CheckpointConsumer<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        self.inner.fmt(f)
    }
}

impl<T: 'static> CheckpointConsumer<T> {
    pub(crate) fn new(inner: crate::CheckpointConsumer<T>) -> Self {
        Self { inner }
    }

    pub fn topic(&self) -> &str {
        self.inner.topic()
    }

    pub fn consumer_name(&self) -> Option<&str> {
        self.inner.consumer_name()
    }

    /// Blocks until the next message.
    pub fn receive(&self) -> Result<Message<T>> {
        block_on(self.inner.receive())
    }

    /// Blocks until the next message, for up to `timeout`.
    pub fn receive_timeout(&self, timeout: Duration) -> Result<Option<Message<T>>> {
        block_on(self.inner.receive_timeout(timeout))
    }

    /// Blocks until there are `max` messages, or `timeout` passes. See [`crate::CheckpointConsumer::receive_batch`].
    pub fn receive_batch(&self, max: usize, timeout: Duration) -> Result<Vec<Message<T>>> {
        block_on(self.inner.receive_batch(max, timeout))
    }

    /// See [`crate::CheckpointConsumer::checkpoint`].
    pub fn checkpoint(&self) -> Result<Checkpoint> {
        block_on(self.inner.checkpoint())
    }

    /// See [`crate::CheckpointConsumer::close`].
    pub fn close(&self) -> Result<()> {
        block_on(self.inner.close())
    }
}
