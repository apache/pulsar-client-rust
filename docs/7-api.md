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

# 7. Public API

> Previous: [6. Scalable topics](6-scalable-topics.md). Next: [8. Quality](8-quality.md).

The API gives each spec operation an idiomatic Rust form (*conventions* §5) and lets the type system enforce
what the spec allows. Names follow the Java V5 API (*java-mapping*) where that reads naturally in Rust, so
cross-SDK behavior stays easy to compare (PIP-494 §6). The mapping is in §10.

The shapes below are proposals; details will move as the code is written.

## 1. Three APIs, one implementation

| Module | Types | For |
|---|---|---|
| `apache_pulsar` | `Send + Sync` handles. Futures that run on any executor. | Everyone; the default. |
| `apache_pulsar::local` | A `!Send` client, used inside a compio runtime the caller runs; its producers run on that runtime. A producer's handle only holds a channel to its task, so it is `Send` too. | Thread-per-core applications ([2](2-architecture.md) §4.2). |
| `apache_pulsar::blocking` | Blocking wrappers over the default API. | Code without an async runtime (*client-api* §1). |

The three expose the same operations with the same names, and share one implementation. A blocking call must
not be made from inside a loop.

## 2. Client

```rust
let client = PulsarClient::builder()
    .service_url("pulsar+ssl://broker.example.com:6651")
    .authentication(Token::new(token))
    .event_loops(2)                       // thread-safe API only
    .build().await?;
// ...
client.close().await?;                    // graceful; later operations fail with AlreadyClosed
```

The client is created once and shared (*client-api* §2). It is cheap to clone: clones refer to the same client.

## 3. Producer

```rust
let producer = client.new_producer(Schema::string())
    .topic("topic://acme/orders/events")
    .producer_name("orders-writer")
    .access_mode(ProducerAccessMode::Shared)
    .send_timeout(Duration::from_secs(30))
    .create().await?;

let id: MessageId = producer.new_message()
    .key("user-42")
    .value("hello".to_string())
    .property("source", "web")
    .send().await?;

producer.send("no key".to_string()).await?;   // shortcut for a message with only a value
producer.flush().await?;
producer.close().await?;
```

**Batching** is on by default, with the defaults of Java V5's `BatchingPolicy`: a message waits up to 1 ms for others
to join its batch, which holds up to 1000 messages or 128 KiB. Each segment's producer keeps one batch per entry
bucket, so that consumers sharing a segment by bucket get whole keys ([5](5-segment-engine.md) §2).
`BatchingPolicy::disabled()` sends each message in an entry of its own:

```rust
let producer = client.new_producer(Schema::string())
    .topic("topic://acme/orders/events")
    .batching_policy(BatchingPolicy { max_publish_delay: Duration::from_millis(5), ..Default::default() })
    .create().await?;
```

**Compression** is off by default, as in Java. `CompressionPolicy::of(CompressionType::Zstd)` compresses each entry's
payload with ZSTD; LZ4, ZLIB and Snappy are the others ([3](3-protocol.md) §6). Each codec is a feature of the crate,
all on by default, and a codec the build leaves out fails `create` with `InvalidConfiguration`.

### 3.1 Send ordering and pipelining

`send()` returns an ordinary lazy Rust future: the message is queued when the future is **first polled**, and
the future completes with the message id once the broker has stored it. Send order is therefore the order in
which send futures are first polled, which for normal code is the order of the calls. This meets the spec's
ordering rule (*client-api* §3.2) without an unusual API.

A sequential `send().await` loop keeps one message in flight at a time, and with batching on, waits out the
batching delay for each. The blocking API's `send` sends its batch at once, as Java's blocking send does. To
pipeline, drive many send futures from one task with a combinator that polls them in order. `buffered(n)` keeps up to `n` sends in flight and
yields results in send order, while it keeps taking new messages from the source:

```rust
let results = stream::iter(orders)
    .map(|order| producer.new_message().key(&order.user).value(order).send())
    .buffered(1000);                     // up to 1000 in flight, results in order
pin_mut!(results);
while let Some(result) = results.next().await {
    let id = result?;
}
```

Collecting futures first (`join_all`, or pushing into `FuturesOrdered`) also works, but nothing is queued until
the collection is first polled. That suits a known batch, not a continuous source. That these combinators make
their first polls in creation order is checked by a test (`pipelining_combinators_keep_send_order`), not assumed.

The pitfalls, which the rustdoc of `send` and the examples call out:

- Spawning each send as its own task (`tokio::spawn(producer.send(a))`) gives no ordering guarantee, because the
  tasks may start in any order.
- Dropping a send future after its first poll, for example when a `timeout` around it expires, does not cancel the
  message. It is already queued, and may still be published. Only its result is lost. The producer's send timeout
  is what bounds how long a message can wait.

**Queue full.** The client's memory limit (`memory_limit`, 64 MiB by default) is the only bound on what
producers hold, as in the Java V5 client: each message counts its payload plus 1 KiB, from its first poll until it
completes. At the limit a send waits for room, by default (`block_if_queue_full(true)`, V5's default), or fails
with `MemoryBufferFull`. Waiting sends get room in the order they first polled. As in Java, a reservation succeeds
while usage is at or below the limit, so one message may go over it. There is no pending-message count limit, so
nothing raises `ProducerQueueFull`; the kind stays for the spec's category.

## 4. Consumers

Each consumer type has only the acknowledgment model the spec allows it (*client-api* §4).

```rust
let stream = client.new_stream_consumer(Schema::string())
    .topic("topic://acme/orders/events")      // or .namespace("acme/orders") later
    .subscription_name("billing")
    .subscription_initial_position(SubscriptionInitialPosition::Earliest)
    .subscribe().await?;
let msg = stream.receive().await?;
stream.acknowledge_cumulative(msg.id()).await?;

let queue = client.new_queue_consumer(Schema::string())
    .topic("topic://acme/orders/events")
    .subscription_name("workers")
    .subscribe().await?;
let msg = queue.receive().await?;
queue.acknowledge(msg.id()).await?;           // or queue.negative_acknowledge(msg.id())

let reader = client.new_checkpoint_consumer(Schema::string())
    .topic("topic://acme/orders/events")
    .start_position(Checkpoint::from_bytes(&saved)?)
    .create().await?;
let msg = reader.receive().await?;
let position: Checkpoint = reader.checkpoint().await?;
```

- **Receive:** `receive()`, `receive_timeout(d)` (returns `Ok(None)` when the time is up), and
  `receive_batch(max, timeout)`, which waits until it has `max` messages or the time is up, as Java's
  `receiveMulti` does. Java's Queue consumer has no `receiveMulti`
  ([J12](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)). Receives are answered in the order they
  are first polled, and one dropped before it completes loses no message: the next receive gets it. Each consumer
  also offers a `futures::Stream` view, which ends when the consumer closes.
- **Acknowledge:** the future completes when the ack has been accepted by the consumer's ack tracker, not when
  the broker confirms it. Grouped acks go out on the ack group timer ([5](5-segment-engine.md) §3). A
  failure can only be local: the consumer is closed. A Queue consumer ignores the id of a segment it no longer
  has, drained or gone from the layout, as Java does: it can't tell it from an id of another consumer.
- **Stream consumer:** `acknowledge_cumulative` covers, through the message's position vector, everything the
  consumer gave before it on every segment it still holds ([6](6-scalable-topics.md) §5.2). Its builder takes what
  *client-api* §4.1 lists, but `read_compacted`, `replicate_subscription_state` and encryption, which come later.
  The topic's controller knows the consumer by its name: one without a name gets a random one, which
  `consumer_name()` returns.
- **Checkpoint:** a Checkpoint consumer acknowledges nothing. `start_position` is `Checkpoint::latest()` by
  default, as in Java, and `consumer_name` names each segment's reader `<name>-seg-<id>`. `checkpoint()` asks
  nothing of the brokers: it names, for each segment, the last message a receive returned, so a receive dropped
  with messages doesn't move it. Restored with `Checkpoint::from_bytes`, a checkpoint resumes the consumer exactly
  there ([6](6-scalable-topics.md) §6.1). With `.consumer_group(group)`, the consumer is a member of a group: it
  reads the segments the topic's controller assigns it, its checkpoint names only those, and a key keeps its order
  within a segment only ([6](6-scalable-topics.md) §6.2). A member without a name gets a random one, which
  `consumer_name()` returns.

## 5. Messages

`Message<T>` exposes every field of *data-model* §3: `value()`, `data()`, `id()`, `key()`, `properties()`,
`publish_time()`, `event_time()`, `sequence_id()`, `producer_name()`, `topic()`, `redelivery_count()`, `size()`
and `replicated_from()`. `topic()` is the scalable topic, not the segment, where Java's Queue consumer gives the
segment ([J7](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)).

`value()` decodes the payload with the consumer's schema each time it is called and returns `Result<T>`.
`data()` borrows the raw payload, which shares the receive buffer and is not copied. Messages are immutable.

## 6. Errors

```rust
pub struct Error { /* kind, message, source */ }
impl Error { pub fn kind(&self) -> ErrorKind; }

#[non_exhaustive]
pub enum ErrorKind {
    NotFound, Authentication, Authorization, Timeout, AlreadyClosed,
    ProducerBusy, ProducerFenced, ConsumerBusy, IncompatibleSchema, TopicTerminated,
    Connect, NotConnected, ProducerQueueFull, MemoryBufferFull, MessageTooLarge, TransactionConflict,
    Crypto, InvalidConfiguration, InvalidServiceUrl, UnsupportedBroker,
}
```

- The kinds are the spec's categories (*error-model* §2), and `NotFound` is distinct from generic failures.
  The spec groups ProducerBusy and Fenced; they are separate kinds here, as the broker sends them
  ([spec gap 13](../DESIGN.md#spec-gaps-to-raise-upstream)).
- `UnsupportedBroker` is the failure when a broker doesn't advertise scalable topics (C15). The spec has no
  category for it, and Java uses `FeatureNotSupportedException` ([spec gap 8](../DESIGN.md#spec-gaps-to-raise-upstream)).
- `MessageTooLarge` is the failure of a message larger than the broker's `max_message_size`. It fails alone,
  and the producer stays usable. The spec has no category for it, and Java V5 raises a plain
  `PulsarClientException` ([spec gap 12](../DESIGN.md#spec-gaps-to-raise-upstream)).
- The spec's `Interrupted` has no Rust counterpart, since Rust threads are not interrupted.
- An async operation never fails before its future exists: every failure comes out of the future
  (*error-model* §1). Nothing in the public API panics on bad input; that is an `InvalidConfiguration` error.

## 7. Message identifiers and checkpoints

`MessageId` and `Checkpoint` are opaque (*data-model* §4–§5):

```rust
let bytes: Vec<u8> = id.to_bytes();
let id = MessageId::from_bytes(&bytes)?;
MessageId::earliest(); MessageId::latest();
Checkpoint::earliest(); Checkpoint::latest();
```

Their byte format is the Java V5 client's (`MessageIdV5`, `CheckpointV5`), so values persisted by one SDK can
be restored by the other ([spec gap 2](../DESIGN.md#spec-gaps-to-raise-upstream)):

- **MessageId:** big endian. The segment id (`i64`), then the length-prefixed v4 `MessageIdData` protobuf bytes,
  then the position vector (a count, then the segment id and length-prefixed id bytes of each entry), then the
  parent topic (length −1 when absent), then the cross-topic vector (count −1 when absent).
- **Checkpoint:** a type byte (0 regular, 1 earliest, 2 latest). A regular checkpoint then has a count and the
  segment id and length-prefixed v4 id bytes of each entry.
- **Decoding** accepts anything Java writes, including older ids without the trailing sections.
- **Encoding** writes entries sorted by key, so the same id always produces the same bytes. Java writes them in
  hash-map order, so equal ids from Java can have different bytes, but each side decodes the other.

`MessageId` implements `Ord` with the order of Java's `compareTo`, so ids from one topic compare in a
consistent way. As in Java, the two sentinels have no segment, so both sort before every message's id
([spec gap 18](../DESIGN.md#spec-gaps-to-raise-upstream)).

## 8. Schemas

```rust
let producer = client.new_producer(Schema::int64()).topic("topic://acme/orders/totals").create().await?;
assert_eq!(Schema::int64().info().schema_type(), SchemaType::Int64);   // named "INT64", as in Java
```

`Schema<T>` is a struct, cheap to clone, that encodes and decodes values of `T` and holds the `SchemaInfo` the broker
is told about: Java V5's name, type and definition. Every built-in schema is named and encoded as Java's, so the two
clients read each other's messages.

The schema is topic-level: the broker keeps one history of schemas for a topic, which all its segments share, and
checks a producer's or a consumer's schema as it registers on a segment (*data-model* §6). An incompatibility fails
with `IncompatibleSchema`:

- a consumer's `subscribe()`, and a Checkpoint consumer's `create()`;
- an exclusive producer's `create()`, as it registers on every segment;
- a Shared producer's sends: it registers on a segment with the first message routed there, as Java V5's does, so
  its `create()` succeeds ([spec gap 30](../DESIGN.md#spec-gaps-to-raise-upstream)). It stays usable, and its next
  message tries again.

A topic that has messages but no schema refuses a schema, as the broker adds none to an active topic. Raw bytes
declare no schema, as in Java, and read any topic.

Built-in schemas come in this order:

1. bytes, string, and the primitive types: `bool`, `int8`, `int16`, `int32`, `int64`, `float32` and `float64`;
2. key/value: `Schema::key_value(key, value)`, with Java's inline encoding, the key and the value in the payload.
   Java V5's factory has none ([J21](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)), but its v4
   schema reads and writes the same messages. Java's other encoding, which puts the key in the message's key, comes
   later;
3. generic records: `Schema::auto_consume()`, Java V5's `Schema.autoConsume()`, whose values are `GenericRecord`s,
   each decoded with the schema its message was written with. The consumer fetches the schema of each version from
   the broker, once, before it delivers the first message of it, and holds the messages after it meanwhile; a message
   without a version reads as bytes. A fetch the broker fails fails that message's `value()`, and the next message of
   the version fetches again. The Avro schemas fetch their messages' schemas the same way. The broker checks no
   generic consumer's schema, so it reads any topic; producers don't take the schema. The data model requires generic
   records, so they are part of conformance;
4. JSON and Avro: `Schema::json()` and `Schema::avro()`, Java V5's `Schema.json(Class)` and `Schema.avro(Class)`,
   for a record type that `serde` serializes and apache-avro describes (`#[derive(AvroSchema)]`). Each is a feature,
   `json` and `avro`, on by default; both describe their records in Avro, as Java does, so both take apache-avro.
   - JSON values go through `serde_json`. A message reads whichever version wrote it, as in Java: a field the type
     doesn't have is left, and an `Option` field the message doesn't have is `None`.
   - Avro values are datums, without a header. A consumer fetches the schema of each version, as for generic
     records, and reads each message with it: Avro's resolution fills in what a later version adds, and takes the
     fields in whichever order the writer's schema has them, as Java's reflection orders them its own way. A
     key/value schema reads an Avro key or value with its part of the writer's.
   - Java's parser takes names that apache-avro refuses, such as the namespace that ends in `$` older Java clients
     give a nested class's record: a definition's names become Avro's before it is parsed.
   - Generic records of either are `serde_json`'s and apache-avro's values, with Java V5's `fields()` and
     `field(name)`;
5. Protobuf's native type: `Schema::protobuf_native()` for a buffa type and `Schema::prost_native(descriptors)` for a
   prost type, Java's `PROTOBUF_NATIVE` (v4's `Schema.PROTOBUF_NATIVE`; Java V5's factory has none,
   [J22](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)). Two features, on by default: `protobuf`,
   which reads descriptors with buffa's and takes buffa's types, and `prost`, prost's.
   - The broker is told the descriptors of the message's file and of those it imports, as Java tells it, and checks
     a new version's against them. A buffa type brings its descriptors (buffa-build's `generate_reflection`); a
     prost type is given the `FileDescriptorSet` it was generated from, and its `prost::Name`.
   - Generic records of it are buffa's dynamic messages, read with the descriptors their version declares, with
     Java V5's `fields()` and `field(name)`;
6. Java V5's Protobuf type: `Schema::protobuf()` for a buffa type and `Schema::prost(descriptors)` for a prost type,
   Java V5's `Schema.protobuf(Class)`, type `PROTOBUF`, behind the same features, with the same descriptors.
   - The broker is told Avro's description of the message, as Java tells it: what avro-protobuf makes of the
     descriptors, a record for each message, written as Avro writes it, with `PROTOBUF_NATIVE`'s properties. The
     broker compares two definitions as Avro schemas, so a Rust producer joins the version a Java producer
     registered.
   - Where Java's description fails, so do `protobuf()` and `prost()`, with `InvalidConfiguration`: for a group, or
     a default that Avro can't hold, a `bytes` field's or a float's infinity or NaN.
   - Generic records of it fail to decode: the definition describes the message in Avro, not how Protobuf encodes
     it, and Java's own `autoConsume()` can't subscribe to such a topic
     ([J22](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)). `PROTOBUF_NATIVE` has them.

## 9. Lifetime, close and drop

- Producers and consumers stay valid until closed. They recover from transient failures by themselves
  (*client-api* §2).
- `close()` is graceful, takes `&self`, and can be called again. A producer waits for the messages the brokers
  already have and fails, with `AlreadyClosed`, those still waiting for a layout, a connection or a segment
  producer, as Java's does; `flush()` first publishes everything. A consumer flushes its pending acks.
- Rust has no async drop. Dropping the last handle starts the same graceful close in the background, on the
  loop, without waiting for it. Code that needs to know when the close is done calls `close().await`.

## 10. Rust mapping

| Spec | Rust |
|---|---|
| Client | `PulsarClient` (`PulsarClient::builder()`), and `local::PulsarClient`, `blocking::PulsarClient` |
| CreateProducer | `client.new_producer(schema)` → `ProducerBuilder<T>` → `.create().await` |
| Send | `producer.new_message()` → `MessageBuilder<T>` (`.value`, `.key`, `.property`, `.event_time`, `.sequence_id`, `.deliver_after`/`.deliver_at`, `.transaction`, `.replication_clusters`) → `.send().await` → `MessageId`; `producer.send(value)` |
| Last sequence id / Flush / Close | `producer.last_sequence_id()` / `.flush().await` / `.close().await` |
| Access modes | `ProducerAccessMode::{Shared, Exclusive, ExclusiveWithFencing, WaitForExclusive}` |
| Stream subscribe | `client.new_stream_consumer(schema)` → `StreamConsumerBuilder<T>` → `.subscribe().await` |
| Queue subscribe | `client.new_queue_consumer(schema)` → `QueueConsumerBuilder<T>` → `.subscribe().await` |
| Checkpoint create | `client.new_checkpoint_consumer(schema)` → `CheckpointConsumerBuilder<T>` (`.start_position`, `.consumer_group`) → `.create().await` |
| Receive | `.receive().await`, `.receive_timeout(d).await`, `.receive_batch(n, d).await` |
| AcknowledgeCumulative (Stream) | `stream.acknowledge_cumulative(&id).await` |
| Acknowledge / NegativeAcknowledge (Queue) | `queue.acknowledge(&id).await` / `queue.negative_acknowledge(&id)` |
| Checkpoint (capture) | `reader.checkpoint().await` → `Checkpoint` |
| Initial position | `SubscriptionInitialPosition::{Earliest, Latest}` |
| Message identifier / Checkpoint | `MessageId`, `Checkpoint` (`to_bytes`, `from_bytes`, `earliest`, `latest`) |
| Error categories | `Error::kind()` → `ErrorKind` |
| NewTransaction / Commit / Abort (later) | `client.new_transaction().await` → `Transaction` (`.commit().await`, `.abort().await`, `.state()`) |

## 11. Configuration

Builders take policy types that mirror Java's `config` package: `BatchingPolicy`, `CompressionPolicy`,
`ChunkingPolicy`, `BackoffPolicy`, `TlsPolicy`, `DeadLetterPolicy`, and so on. Policies are plain structs with
public fields and a `Default`, and they can be deserialized behind a `serde` feature. That keeps configuration
easy to load from files, and to map onto other languages later ([1](1-scope.md) §6).

## 12. Open questions

1. **Message value decoding:** decode on each `value()` call as proposed, or once at receive and cache?
2. **`MessageId` order:** Java's `compareTo` orders by segment id first, which is not publish order across
   segments. Is `Ord` the right trait, or should it be an explicit comparison method?
3. **Local and thread-safe types:** separate types with the same methods (proposed), or one set of types
   generic over the runtime model?
4. **Transactional acks:** `acknowledge_cumulative_in(&txn, &id)`, or an option on the call?
