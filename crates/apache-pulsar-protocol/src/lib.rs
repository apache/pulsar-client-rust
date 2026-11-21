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

//! The sans-IO protocol core of `apache-pulsar`.
//!
//! This crate is an implementation detail of `apache-pulsar`. Its API is not stable: `apache-pulsar` depends
//! on an exact version of it.

pub mod backoff;
pub mod checkpoint;
pub mod checkpoint_consumer;
pub mod connection;
pub mod consumer;
pub mod defaults;
pub mod entry;
pub mod frame;
pub mod hash;
pub mod layout;
pub mod lookup;
pub mod message_id;
pub mod producer;
pub mod queue_consumer;

/// Generated from the vendored `PulsarApi.proto` by `cargo run -p xtask -- codegen`.
///
/// proto2 `[default = …]` values are not applied by the generated code: use the accessors in [`defaults`].
#[allow(clippy::all, missing_debug_implementations, non_snake_case, non_camel_case_types)]
pub mod proto {
    include!("proto/pulsar.proto.mod.rs");
}
