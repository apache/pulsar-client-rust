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

//! Apache Pulsar client for scalable topics.
//!
//! Status: milestone 2. Producers publish to scalable topics, and to regular topics through the migration bridge,
//! in the thread-safe ([`PulsarClient`]), [`local`] and [`blocking`] APIs; consumers come next. See `DESIGN.md`
//! and `PLAN.md` in the repository.

pub mod auth;
pub mod blocking;
mod client;
mod config;
mod conn;
mod error;
pub mod local;
mod memory;
mod message_id;
mod net;
mod pool;
mod producer;
mod schema;
mod watch;

pub use auth::{Authentication, TlsAuthentication, Token};
pub use client::PulsarClient;
pub use config::{Blocking, ClientBuilder, Local, ThreadSafe};
pub use error::{Error, ErrorKind, Result};
pub use message_id::MessageId;
pub use producer::{MessageBuilder, Producer, ProducerAccessMode, ProducerBuilder};
pub use schema::Schema;

#[cfg(test)]
mod producer_tests;
#[cfg(test)]
mod tests;
