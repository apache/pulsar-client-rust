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

//! The connection pool of one loop (docs/4-connections.md §2, §4).
//!
//! One connection per (logical, physical) address. Callers that ask for an address while it is being connected
//! share that attempt (coalescing). A closed connection is replaced on the next request for its address.

use std::cell::RefCell;
use std::collections::HashMap;
use std::rc::Rc;

use apache_pulsar_protocol::lookup::BrokerAddress;
use futures_util::FutureExt;
use futures_util::future::{LocalBoxFuture, Shared};

use crate::config::ClientConfig;
use crate::conn::{self, ConnHandle};
use crate::error::{Error, Result};

type Connecting = Shared<LocalBoxFuture<'static, Result<ConnHandle>>>;

enum Slot {
    Connecting(Connecting),
    Ready(ConnHandle),
}

#[derive(Clone)]
pub(crate) struct Pool {
    inner: Rc<PoolInner>,
}

struct PoolInner {
    config: Rc<ClientConfig>,
    slots: RefCell<HashMap<BrokerAddress, Slot>>,
    closed: std::cell::Cell<bool>,
    #[cfg(test)]
    attempts: std::cell::Cell<usize>,
}

impl std::fmt::Debug for Pool {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Pool").field("connections", &self.inner.slots.borrow().len()).finish()
    }
}

impl Pool {
    pub fn new(config: Rc<ClientConfig>) -> Self {
        Self {
            inner: Rc::new(PoolInner {
                config,
                slots: RefCell::new(HashMap::new()),
                closed: std::cell::Cell::new(false),
                #[cfg(test)]
                attempts: std::cell::Cell::new(0),
            }),
        }
    }

    /// A ready connection to `address`: the pooled one, the one being made, or a new one.
    pub async fn get(&self, address: &BrokerAddress) -> Result<ConnHandle> {
        if self.inner.closed.get() {
            return Err(Error::closed());
        }
        let connecting = {
            let mut slots = self.inner.slots.borrow_mut();
            match slots.get(address) {
                Some(Slot::Ready(handle)) if !handle.is_closed() => return Ok(handle.clone()),
                Some(Slot::Connecting(connecting)) => connecting.clone(),
                _ => {
                    #[cfg(test)]
                    self.inner.attempts.set(self.inner.attempts.get() + 1);
                    let config = self.inner.config.clone();
                    let target = address.clone();
                    let connecting = async move { conn::open(target, &config).await }.boxed_local().shared();
                    slots.insert(address.clone(), Slot::Connecting(connecting.clone()));
                    connecting
                }
            }
        };
        let result = connecting.clone().await;
        self.settle(address, &connecting, &result);
        if self.inner.closed.get() {
            if let Ok(handle) = &result {
                handle.close().await;
            }
            return Err(Error::closed());
        }
        result
    }

    /// The first waiter to finish settles the slot, if it still holds this attempt.
    fn settle(&self, address: &BrokerAddress, connecting: &Connecting, result: &Result<ConnHandle>) {
        let mut slots = self.inner.slots.borrow_mut();
        if let Some(Slot::Connecting(current)) = slots.get(address)
            && current.ptr_eq(connecting)
        {
            match result {
                Ok(handle) if !self.inner.closed.get() => {
                    slots.insert(address.clone(), Slot::Ready(handle.clone()));
                }
                _ => {
                    slots.remove(address);
                }
            }
        }
    }

    /// Closes every connection. Later requests fail with `AlreadyClosed`.
    pub async fn close(&self) {
        self.inner.closed.set(true);
        let slots: Vec<Slot> = self.inner.slots.borrow_mut().drain().map(|(_, slot)| slot).collect();
        for slot in slots {
            if let Slot::Ready(handle) = slot {
                handle.close().await;
            }
        }
    }

    #[cfg(test)]
    pub fn connect_attempts(&self) -> usize {
        self.inner.attempts.get()
    }

    pub fn config(&self) -> &ClientConfig {
        &self.inner.config
    }
}
