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

# Apache Pulsar Rust client

A Rust client for [Apache Pulsar](https://pulsar.apache.org) scalable topics, built from the
[Scalable Topics client specification](https://github.com/apache/pulsar/tree/master/spec/scalable-topics) on the
[compio](https://github.com/compio-rs/compio) runtime.

**Status:** milestone 3. Producers publish to scalable topics, keeping per-key order across segment splits and
merges, and to regular `persistent://` topics through the migration bridge. Queue consumers read every segment of
a topic, following its splits and merges, and acknowledge each message. Both work in the thread-safe, local and
blocking APIs, with token and mTLS authentication, directly or through a proxy. The Stream and Checkpoint
consumers come next. The design is in [DESIGN.md](DESIGN.md), the milestones in [PLAN.md](PLAN.md).

```rust
let client = PulsarClient::builder().service_url("pulsar://localhost:6650").build().await?;
let producer = client.new_producer(Schema::string()).topic("topic://public/default/orders").create().await?;
let id = producer.new_message().key("user-42").value("hello".to_string()).send().await?;
producer.close().await?;

let consumer = client.new_queue_consumer(Schema::string())
    .topic("topic://public/default/orders")
    .subscription_name("workers")
    .subscribe().await?;
let message = consumer.receive().await?;
consumer.acknowledge(message.id()).await?;
client.close().await?;
```

## Layout

| Path | What it is |
|---|---|
| `crates/apache-pulsar` | The client: producers, Queue consumers, layout watches, connections, the pool, lookup, authentication, TLS, the memory limit, and the thread-safe, local and blocking APIs. |
| `crates/apache-pulsar-protocol` | The sans-IO protocol core: generated protobuf code, framing, the connection, producer and consumer state machines, entry decoding, layouts and routing, message ids. Internal; not a stable API. |
| `crates/apache-pulsar-testkit` | The simulated broker and cluster, and the deterministic tests of the protocol state machines. Not published. |
| `checks/` | Milestone 0 checks: compio behavior, and the handshake against a real broker. |
| `tools/java-vectors` | Generates test vectors from the Java client. |
| `xtask` | Repository tasks: protobuf code generation. |

## Building and testing

```bash
cargo test --workspace
```

The generated protobuf code is committed, so building needs no `protoc`. After changing the vendored
`crates/apache-pulsar-protocol/proto/PulsarApi.proto`, regenerate it (needs `protoc` 33.1, the version CI checks
with):

```bash
cargo run -p xtask -- codegen
```

The handshake against a real broker:

```bash
docker run -d --rm --name pulsar -p 6650:6650 -p 8080:8080 apachepulsar/pulsar:5.0.1 bin/pulsar standalone
```

```bash
cargo run -p first-contact -- 127.0.0.1:6650
```

The integration tests start a broker and a proxy in Docker, with TLS, token and mTLS authentication, and remove
them when they finish. `cargo test` skips them; they need Docker running:

```bash
cargo test -p apache-pulsar --test integration
```

## License

[Apache License 2.0](LICENSE).
