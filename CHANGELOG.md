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

# Changelog

Each release's notes say which version of the
[Scalable Topics client specification](https://github.com/apache/pulsar/tree/master/spec/scalable-topics) it conforms
to, and which of the specification's optional capabilities it implements ([docs/8-quality.md](docs/8-quality.md) §3).

## 0.1.0 (draft)

*A draft until the release is prepared ([RELEASE.md](RELEASE.md), step 1), when it gets its date.*

The first release of the Rust client of Apache Pulsar, for the scalable topics of Pulsar 5.0 and later: the
`apache-pulsar` crate, and `apache-pulsar-protocol`, its protocol core, which isn't a stable API.

### Conformance

Conforms to version 1.0 of the Scalable Topics client specification. It implements the required core, C1 to C15
([*conformance*](https://github.com/apache/pulsar/blob/master/spec/scalable-topics/conformance.md) §2), and these of
the optional capabilities (§3):

| Capability | In 0.1.0 |
|---|---|
| Entry-bucketing, producer side | Yes: each batch holds one entry bucket's messages, and each entry is stamped with its bucket hashes. |
| Migration bridge | Yes: `persistent://` topics, partitioned or not, and short topic names. |
| Chunking, compression, custom batching | Compression: LZ4, ZLIB, ZSTD and Snappy. No chunking, and no custom batching. |
| Transactions | No. |
| Namespace subscription | No. |
| Dead-letter | No. |
| End-to-end encryption | No. |

### What it has

- **Producers:** each key routed to the segment that covers it, and its order kept across splits and merges;
  deduplication by sequence id; the shared, exclusive, exclusive-with-fencing and wait-for-exclusive access modes;
  flush and close; a client memory limit; batches by entry bucket, as Java V5's `BatchingPolicy` sets them; and
  compression.
- **Queue consumers:** every segment of a topic, kept in step with its layout; individual and negative acks, and
  redelivery.
- **Stream consumers:** the segments the topic's controller assigns, each key in order; cumulative acks across
  segments; segments shared by entry bucket when the consumers outnumber them.
- **Checkpoint consumers:** every segment, parents before their children, or a group's share of them; checkpoints the
  application keeps and restores across layout changes.
- **Message ids and checkpoints** in the Java V5 client's bytes, so that they move between the two clients.
- **Schemas**, named and encoded as Java's: bytes, string, the primitive types, key/value, JSON, Avro, Protobuf
  (`PROTOBUF_NATIVE` and Java V5's `PROTOBUF`), and generic records, each message decoded with the schema of its
  version. A schema the topic refuses fails with `IncompatibleSchema`.
- **Regular topics** through the migration bridge, with the v4 key-to-partition routing, before, during and after a
  topic's migration.
- **Connections:** TLS, token and mTLS authentication, lookups with redirects, proxies, keepalive, and reconnection
  with backoff.
- **Three APIs:** thread-safe, whose futures run on any runtime; local, on the application's own compio loop; and
  blocking.

### Requirements

- Apache Pulsar 5.0 or later: the client talks only to brokers that advertise scalable topics.
- Rust 1.95 or later.
- Linux, on io_uring where the kernel allows it and on the polling driver otherwise, and macOS. Windows builds, but
  isn't tested yet.

### Known limitations

- Key/value has Java's inline encoding only. The separated one, with the key in the message's key, comes later.
- Java V5's `autoProduceBytes()` and `generic(SchemaInfo)` aren't implemented.
- Neither this client nor Java's reads a `PROTOBUF` schema's messages as generic records ([DESIGN.md](DESIGN.md), J22).
- A Stream consumer, or a member of a Checkpoint consumer group, can't use a regular topic before the topic migrates:
  the 5.0.1 broker's controller doesn't serve one ([DESIGN.md](DESIGN.md), gap 28 and J18).
- No metrics yet. Logs go through `tracing`.
- Not in this release: OAuth2 authentication, the dead-letter policy, namespace subscriptions, transactions,
  end-to-end encryption and chunking ([PLAN.md](PLAN.md), after the first release).
