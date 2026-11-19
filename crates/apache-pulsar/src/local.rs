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

use std::cell::Cell;
use std::rc::Rc;
use std::time::Instant;

use apache_pulsar_protocol::backoff::Backoff;
use apache_pulsar_protocol::lookup::{BrokerAddress, Lookup, LookupStep};

use crate::config::{ClientBuilder, Local};
use crate::conn::ConnHandle;
use crate::error::{Error, ErrorKind, Result};
use crate::pool::Pool;

/// The client of one event loop. Cheap to clone; clones are the same client.
#[derive(Clone)]
pub struct PulsarClient {
    inner: Rc<Inner>,
}

struct Inner {
    pool: Pool,
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
                closed: Cell::new(false),
                next_host: Cell::new(0),
            }),
        }
    }

    /// Closes every connection. Later operations fail with [`ErrorKind::AlreadyClosed`]. Can be called again.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.replace(true) {
            return Ok(());
        }
        self.inner.pool.close().await;
        Ok(())
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    fn check_open(&self) -> Result<()> {
        if self.is_closed() { Err(Error::closed()) } else { Ok(()) }
    }

    /// Finds the broker that serves `topic`, retrying connection failures with backoff, across the service URL's
    /// hosts, until the operation timeout. Public only for the integration tests, until producers exist.
    #[doc(hidden)]
    pub async fn lookup(&self, topic: &str) -> Result<BrokerAddress> {
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

    /// A ready connection to `address`, from the pool. Public only for the integration tests.
    #[doc(hidden)]
    pub async fn connection(&self, address: &BrokerAddress) -> Result<ConnHandle> {
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
fn jitter() -> f64 {
    use std::hash::{BuildHasher, RandomState};
    (RandomState::new().hash_one(Instant::now()) >> 11) as f64 / (1u64 << 53) as f64
}
