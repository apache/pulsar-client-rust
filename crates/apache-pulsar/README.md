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

# apache-pulsar

The Rust client of [Apache Pulsar](https://pulsar.apache.org) for scalable topics, built from the
[Scalable Topics client specification](https://github.com/apache/pulsar/tree/master/spec/scalable-topics) on the
[compio](https://github.com/compio-rs/compio) runtime.

Producers keep each key's order across segment splits and merges. Queue, Stream and Checkpoint consumers read a topic
as the specification defines them, and regular `persistent://` topics work through the migration bridge. The client
comes in three APIs: thread-safe, whose futures run on any runtime; local, for applications that run compio
themselves; and blocking.

The [API documentation](https://docs.rs/apache-pulsar) has the examples. The
[release notes](https://github.com/apache/pulsar-client-rust/blob/main/CHANGELOG.md) say which version of the
specification each release conforms to, and which of its optional capabilities it implements.

## Requirements

- **Broker:** Apache Pulsar 5.0 or later. The client talks only to brokers that advertise scalable topics.
- **Rust:** 1.95 or later.
- **Platforms:** Linux, on io_uring where the kernel allows it and on the polling driver otherwise, and macOS.
  Windows builds, but isn't tested yet.

## Features

All on by default:

| Feature | What it adds |
|---|---|
| `lz4`, `zlib`, `snappy`, `zstd` | The compression codecs. `zstd` binds the C `libzstd`, so building it needs a C compiler. |
| `json`, `avro` | The JSON and Avro schemas, through `serde` and `apache-avro`. |
| `protobuf` | The Protobuf schemas, `PROTOBUF_NATIVE` and Java's `PROTOBUF`, for buffa's messages. |
| `prost` | The same schemas for prost's messages. Turns on `protobuf`. |

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
