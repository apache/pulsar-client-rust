<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# 1. Scope

> Next: [2. Architecture](2-architecture.md).

## 1. Goals

1. **A conformant scalable-topics client.** It implements the spec's required core (*conformance* §2,
   C1–C15) and no optional capability halfway (*conformance* §4).
2. **It also works on existing topics.** Through the migration bridge (*compatibility* §1), a
   `persistent://` topic, partitioned or not, is used with the same API, and keys keep their v4
   key-to-partition routing. This is also how pulsar-rs users move to this client.
3. **Idiomatic Rust.** Each spec operation maps to Rust conventions, and the type system enforces what the
   spec allows: for example, only a Stream consumer has a cumulative ack ([7](7-api.md)).
4. **Thread-per-core performance.** All the state of a producer or consumer lives on one event loop, with
   no locks and no contended atomics on its data path ([2](2-architecture.md)).
5. **Usable from any runtime.** The default API is thread-safe and its futures don't depend on compio, so
   tokio, smol and blocking code can use it. Applications that are thread-per-core themselves can embed the
   single-loop API in their own compio runtime.
6. **An official SDK.** It is developed in `apache/pulsar-client-rust`, released through the ASF, and kept
   in step with the spec's change process (PIP-494).

## 2. Initial scope

The first release covers the required core, the migration bridge and the producer policies that real
workloads need. Everything else is built on top of that.

| Area | Initial | Notes |
|---|---|---|
| Layout tracking, routing, segment-gone retry (C1–C4) | Yes | [6](6-scalable-topics.md) |
| Producer: dedup, access modes, flush, close (C5, C6) | Yes | |
| Stream consumer, including the consumer side of entry-bucketing (C7) | Yes | *key-shared* §9 makes this side mandatory. |
| Queue consumer (C8) | Yes | |
| Checkpoint consumer, ungrouped and grouped (C9) | Yes | |
| MessageId and Checkpoint encoding (C10) | Yes | Java's byte format ([7](7-api.md)). |
| Schemas (C11) | Yes | Bytes, string, primitives, key/value, JSON and Avro first, then the rest ([7](7-api.md)). |
| Auto-creation, error model, resource lifetime, feature gating (C12–C15) | Yes | |
| Migration bridge | Yes | Goal 2. |
| Batching with producer-side entry-bucketing | Yes | Every entry uses the batch format from the start. Batches hold one message at first, then several messages of one bucket ([5](5-segment-engine.md) §2, [Plan](../PLAN.md)). |
| Compression | Yes | LZ4, ZSTD, ZLIB and Snappy. |
| Authentication | Token and mTLS first, then OAuth2 client credentials | [4](4-connections.md) |
| Dead-letter policy | Soon after | Optional capability. |
| Transactions | Later | Optional capability. Needs the TC discovery watch (*wire-protocol* §8). |
| Namespace subscription | Later | Optional capability (*client-api* §4.4). |
| End-to-end encryption, chunking | Later | Optional capabilities. |

## 3. Broker requirements

The client talks only to brokers that advertise `supports_scalable_topics` in `CommandConnected`, which
means Pulsar 5.0 or later. C15 forbids sending scalable-topic commands to any other broker
(*compatibility* §3).

This also applies to the migration bridge: a `persistent://` topic is resolved through
`CommandScalableTopicLookup` too (*compatibility* §1). So "works on existing topics" means existing topics
on a 5.0 broker. On an older broker, the first create or subscribe fails with a typed error when it opens the layout
watch, as in the Java client.

There is no fallback for brokers before 5.0. Applications on older brokers upgrade the broker first, or keep
using a classic client until then.

## 4. Platforms and toolchain

- **Linux:** the main target. It uses io_uring when the kernel and seccomp profile allow it, and falls back
  to the polling driver otherwise. Default seccomp profiles in containers often block io_uring, so the
  fallback is on by default.
- **macOS:** supported, mainly for development, on the polling driver (kqueue).
- **Windows:** compio supports IOCP. At first we only build on Windows in CI; it is tested once the core is
  stable.
- **Rust:** edition 2024. The minimum supported Rust version follows what compio needs, and is stated in
  `Cargo.toml` and checked in CI.

## 5. Non-goals for now

- A classic (v4) public API.
- Bindings for other languages (§6).
- An admin REST client, beyond a test-only helper for driving splits and merges ([8](8-quality.md)).
- Geo-replication of scalable topics, which the spec doesn't cover yet.
- HTTP service URLs and HTTP lookup: the client uses the binary protocol only.
- Kerberos, Athenz and SASL authentication.
- A tokio driver. The sans-IO core leaves room for one if there is demand ([2](2-architecture.md)).
- WebAssembly.

## 6. Later phases

Once the Rust SDK is proven, it can become the core that other language clients wrap, possibly with a
classic-API crate. The design keeps this possible without building it now:

- the thread-safe API owns its threads and doesn't need a runtime from the caller;
- configuration is plain data (structs with public fields, behind builders), which maps onto a C ABI or
  onto bindings like PyO3 and napi-rs;
- the single-topic segment engine ([5](5-segment-engine.md)) already implements classic producers and
  consumers on one topic, which a classic API would need.

## 7. Open questions

1. **Windows.** When does Windows become a tested platform?
2. **Minimum Rust version.** Should we promise something stronger than "what compio needs"?
