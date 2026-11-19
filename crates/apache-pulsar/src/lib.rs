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
//! Status: milestone 1. The client connects, authenticates and looks up topics; producers and consumers come
//! next. See `DESIGN.md` and `PLAN.md` in the repository.

// The connection layer is used by tests until producers and consumers use it (milestone 2).
#![cfg_attr(not(test), allow(dead_code))]

pub mod auth;
mod client;
mod config;
mod conn;
mod error;
pub mod local;
mod net;
mod pool;

pub use auth::{Authentication, TlsAuthentication, Token};
pub use client::PulsarClient;
pub use config::{ClientBuilder, Local, ThreadSafe};
pub use error::{Error, ErrorKind, Result};

#[cfg(test)]
mod tests;
