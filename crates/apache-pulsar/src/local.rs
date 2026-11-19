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
use futures_channel::mpsc;

use apache_pulsar_protocol::backoff::Backoff;
use apache_pulsar_protocol::lookup::{BrokerAddress, Lookup, LookupStep};

use crate::config::{ClientBuilder, Local};
use crate::conn::ConnHandle;
use crate::error::{Error, ErrorKind, Result};
use crate::memory::MemoryLimit;
use crate::pool::Pool;
use crate::producer::Request;
use crate::schema::Schema;
use crate::watch::Watches;

/// A producer on this loop. See [`crate::Producer`].
pub type Producer<T> = crate::Producer<T, Local>;
/// Builds a producer on this loop: [`PulsarClient::new_producer`].
pub type ProducerBuilder<T> = crate::ProducerBuilder<T, Local>;
/// A message being built: [`Producer::new_message`].
pub type MessageBuilder<'a, T> = crate::MessageBuilder<'a, T, Local>;

/// The client of one event loop. Cheap to clone; clones are the same client.
#[derive(Clone)]
pub struct PulsarClient {
    inner: Rc<Inner>,
}

struct Inner {
    pool: Pool,
    watches: Watches,
    /// The producers' tasks, with the way to ask each to close.
    tasks: RefCell<Vec<(mpsc::UnboundedSender<Request>, JoinHandle<()>)>>,
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

    /// Closes the producers, then every connection. Later operations fail with [`ErrorKind::AlreadyClosed`]. Can
    /// be called again.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.replace(true) {
            return Ok(());
        }
        let tasks = std::mem::take(&mut *self.inner.tasks.borrow_mut());
        for (requests, _) in &tasks {
            let _ = requests.unbounded_send(Request::Close { reply: None });
        }
        for (_, task) in tasks {
            let _ = task.await;
        }
        self.inner.pool.close().await;
        Ok(())
    }

    /// Runs a producer's task on this loop, owned by the client, which closes it on close.
    pub(crate) fn adopt(
        &self,
        requests: mpsc::UnboundedSender<Request>,
        task: impl Future<Output = ()> + 'static,
    ) -> Result<()> {
        self.check_open()?;
        let mut tasks = self.inner.tasks.borrow_mut();
        tasks.retain(|(_, task)| !task.is_finished());
        tasks.push((requests, compio::runtime::spawn(task)));
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
        self.check_open()?;
        let config = self.inner.pool.config();
        let deadline = Instant::now() + config.operation_timeout;
        let hosts = &config.service.hosts;
        let mut backoff = Backoff::default();
        loop {
            let index = self.inner.next_host.get() % hosts.len();
            let host = hosts[index].clone();
            match self.lookup_from(topic, Lookup::new(topic, host.clone(), config.service.tls)).await {
                Ok(address) => return Ok(address),
                Err(error) if error.is_retryable() => {
                    // Start the next attempt, and the next lookup, at another host.
                    self.inner.next_host.set(index + 1);
                    let delay = backoff.next_delay(jitter());
                    if Instant::now() + delay >= deadline {
                        return Err(Error::new(ErrorKind::Timeout, format!("lookup of {topic} timed out: {error}")));
                    }
                    tracing::debug!(%topic, %host, %error, ?delay, "lookup failed, retrying");
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
