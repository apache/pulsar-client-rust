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
| `apache_pulsar::local` | `!Send` handles, used inside a compio runtime the caller runs. | Thread-per-core applications ([2](2-architecture.md) §4.2). |
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
    .batching(BatchingPolicy::default())
    .compression(CompressionPolicy::lz4())
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

### 3.1 Send ordering and pipelining

`send()` returns an ordinary lazy Rust future: the message is queued when the future is **first polled**, and
the future completes with the message id once the broker has stored it. Send order is therefore the order in
which send futures are first polled, which for normal code is the order of the calls. This meets the spec's
ordering rule (*client-api* §3.2) without an unusual API.

A sequential `send().await` loop keeps one message in flight at a time. To pipeline, drive many send futures
from one task with a combinator that polls them in order. `buffered(n)` keeps up to `n` sends in flight and
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
their first polls in creation order is checked by a test in M2 ([Plan](../PLAN.md)), not assumed.

The pitfalls, which the rustdoc of `send` and the examples call out:

- Spawning each send as its own task (`tokio::spawn(producer.send(a))`) gives no ordering guarantee, because the
  tasks may start in any order.
- Dropping a send future after its first poll, for example when a `timeout` around it expires, does not cancel the
  message. It is already queued, and may still be published. Only its result is lost. The producer's send timeout
  is what bounds how long a message can wait.

**Queue full.** When the pending-message or memory limit is reached, a send waits for room if the producer
was built with `block_if_queue_full(true)`. Otherwise it fails with `ProducerQueueFull`, or with
`MemoryBufferFull` for the client's memory limit. Waiting sends get room in the order they first polled.

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
  `receive_batch(max, timeout)`. Each consumer also offers a `futures::Stream` view.
- **Acknowledge:** the future completes when the ack has been accepted by the consumer's ack tracker, not when
  the broker confirms it. Grouped acks go out on the ack group timer ([5](5-segment-engine.md) §3). A
  failure can only be local: the consumer is closed, or the message id is not from this consumer.

## 5. Messages

`Message<T>` exposes every field of *data-model* §3: `value()`, `data()`, `id()`, `key()`, `properties()`,
`publish_time()`, `event_time()`, `sequence_id()`, `producer_name()`, `topic()`, `redelivery_count()`, `size()`
and `replicated_from()`.

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
    Connect, NotConnected, ProducerQueueFull, MemoryBufferFull, TransactionConflict,
    Crypto, InvalidConfiguration, InvalidServiceUrl, UnsupportedBroker,
}
```

- The kinds are the spec's categories (*error-model* §2), and `NotFound` is distinct from generic failures.
  The spec groups ProducerBusy and Fenced; they are separate kinds here.
- `UnsupportedBroker` is the failure when a broker doesn't advertise scalable topics (C15). The spec has no
  category for it, and Java uses `FeatureNotSupportedException` ([spec gap 8](../DESIGN.md#spec-gaps-to-raise-upstream)).
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
consistent way.

## 8. Schemas

```rust
pub trait Schema<T> {
    fn info(&self) -> &SchemaInfo;
    fn encode(&self, value: &T) -> Result<Bytes>;
    fn decode(&self, data: &[u8], schema_version: Option<&[u8]>) -> Result<T>;
}
```

The schema is topic-level, and the broker checks compatibility on connect (*data-model* §6). An incompatibility
fails `create()` or `subscribe()` with `IncompatibleSchema`. Built-in schemas come in this order:

1. bytes, string, the primitive types and key/value;
2. JSON through `serde`, and Avro through `apache-avro`, each behind a feature;
3. Protobuf (native), and generic records (`AUTO_CONSUME`). C11 requires generic records, so they are part of
   conformance.

## 9. Lifetime, close and drop

- Producers and consumers stay valid until closed. They recover from transient failures by themselves
  (*client-api* §2).
- `close()` is graceful, takes `&self`, and can be called again: a producer finishes its pending sends, and a
  consumer flushes its pending acks.
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
