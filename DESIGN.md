# Apache Pulsar Rust client: design

This is the design of `apache-pulsar`, a Rust client for Apache Pulsar, built from the
[Scalable Topics client specification](https://github.com/apache/pulsar/tree/master/spec/scalable-topics)
(v1.0, [PIP-494](https://github.com/apache/pulsar/blob/master/pip/pip-494.md)). It is meant to become an
officially supported Pulsar SDK, developed in `apache/pulsar-client-rust` and released through the ASF
process.

Two things shape it. The first is the specification, which is the API and behavior contract. The second is
a thread-per-core runtime model on [compio](https://github.com/compio-rs/compio), which does completion-based
I/O: io_uring on Linux, IOCP on Windows, and a polling backend elsewhere.

**Status:** first draft, for discussion. Each document ends with its open questions.

## Documents

| | Document | What it covers |
|---|---|---|
| 1 | [Scope](docs/1-scope.md) | Goals, the conformance target, broker and platform requirements, non-goals and later phases. |
| 2 | [Architecture](docs/2-architecture.md) | The layers and crates, the sans-IO core, event loops, the thread-safe and local APIs, and the state that crosses loops. |
| 3 | [Protocol core](docs/3-protocol.md) | Framing, the protobuf library, the connection state machine, feature gating and the buffer model. |
| 4 | [Connections](docs/4-connections.md) | Service URLs, lookup, connection pools, proxies, reconnection, TLS and authentication. |
| 5 | [Segment engine](docs/5-segment-engine.md) | The single-topic producer, consumer and reader that run on each segment. |
| 6 | [Scalable topics](docs/6-scalable-topics.md) | Layout tracking, routing, the producer, the three consumer types, assignment, key-shared and the migration bridge. |
| 7 | [Public API](docs/7-api.md) | The Rust API: types, builders, sending, acknowledgment, errors, schemas, identifiers, close and the blocking API. |
| 8 | [Quality](docs/8-quality.md) | Observability, testing, compatibility, versioning and releases. |
| | [Plan](PLAN.md) | Milestones, from removing the main risks to a conformant client. |

Spec references are written as *document §section*, for example *client-behavior §6*. They name the
documents in `spec/scalable-topics/` of `apache/pulsar`. The design cites the spec and does not restate it.

## Decisions

| Decision | State | Where |
|---|---|---|
| Home: `apache/pulsar-client-rust`, Apache-2.0, ASF releases | Decided | — |
| Crate name: `apache-pulsar` (`pulsar` belongs to pulsar-rs; `apache-avro` is the precedent) | Decided | — |
| New codebase, not a fork of pulsar-rs. pulsar-rs is deprecated once this client is ready | Decided | — |
| Target: the spec's required core (C1–C15) plus the migration bridge for `persistent://` topics | Decided | [1](docs/1-scope.md) |
| Runtime: compio, accepted although it is pre-1.0 | Decided | [2](docs/2-architecture.md) |
| One single-threaded client per event loop; a thread-safe API over N loops; the local `!Send` API is public too | Decided | [2](docs/2-architecture.md) |
| Sans-IO protocol core, driven by a thin compio layer | Decided | [2](docs/2-architecture.md), [3](docs/3-protocol.md) |
| Serialized MessageId and Checkpoint use the Java V5 encoding | Decided | [7](docs/7-api.md) |
| `tracing` for logs; metrics through [nanometer](https://github.com/merlimat/nanometer), named as in the Java client | Decided | [8](docs/8-quality.md) |
| Port the scenarios of the Java V5 tests | Decided | [8](docs/8-quality.md) |
| A base for other language SDKs, possibly with a classic-API crate | Later phase | [1](docs/1-scope.md) |
| Protobuf library: `buffa`, with proto2-default accessors and open enums for `BaseCommand.type` | Decided | [3](docs/3-protocol.md) |
| `send` shape: a plain lazy future, with ordering covered in rustdoc and examples | Leaning | [7](docs/7-api.md) |
| Crate split and which crates carry semver guarantees | Proposed | [2](docs/2-architecture.md) |
| Producer batching: the batch format for every entry from the start, one message per batch first, then multi-message batches with entry-bucketing | Decided | [5](docs/5-segment-engine.md), [Plan](PLAN.md) |
| Brokers before 5.0: not supported, with no classic fallback | Decided | [1](docs/1-scope.md) |
| Codecs: `lz4_flex`, `flate2` on `miniz_oxide`, `snap`, and `zstd` on the C `libzstd`, each behind a feature, all on by default | Decided | [3](docs/3-protocol.md) |

## Spec gaps to raise upstream

PIP-494 §6 asks SDK authors to implement the spec, not the Java source, and to send clarifications back.
Wherever this design follows the Java client because the spec is silent or inconsistent, the item goes
here.

| # | Gap | What this client does | Status |
|---|---|---|---|
| 1 | *conformance* C2 gives the routing hash as `Murmur3_32 & 0xFFFF`. *client-behavior* §2, *wire-protocol* §5 and Java use the high 16 bits. | High 16 bits. | [apache/pulsar#27663](https://github.com/apache/pulsar/pull/27663) |
| 2 | Serialized MessageId and Checkpoint must be restorable (*data-model* §4–§5, *stability* §4.3), but no byte format is defined. Mixed-language deployments need one. | Java's `MessageIdV5` / `CheckpointV5` encoding. | PIP to propose |
| 3 | *wire-protocol* §5 says the client produces to "the owning broker from `segment_brokers`". Java resolves each segment topic through the standard lookup, which works behind a Pulsar proxy and honors `proxy_through_service_url`. *wire-protocol* §4 also doesn't say which broker the layout watch is opened on; Java does a standard lookup on the topic name first. | Standard lookup for both, like Java. | To raise |
| 4 | *client-behavior* §8 and *wire-protocol* §6.2 say to connect to `controller_broker_url` directly. Java has two fallbacks the spec doesn't mention. Behind a proxy it pairs to any broker, which forwards the subscribe. With an empty controller URL it uses the service URL. | Java's fallbacks. | To raise |
| 5 | Epoch rule for layouts: "greater than" in *client-behavior* §1 and *wire-protocol* §2, "≥" in *wire-protocol* §11. | `>` for layouts, `≥` for assignments. | To raise |
| 6 | *key-shared* §9 says a client MUST gate producer stamping on observed broker support, but no signal for that support is defined. Java does not gate; it stamps whenever it batches. | Same as Java. | To raise |
| 7 | Per-key order across a split for the ungrouped checkpoint consumer needs a sealed parent read to its end before its children. *client-behavior* §7 and *wire-protocol* §6.3 don't say so; Java does it from the layout's lineage. For managed assignments (Stream, grouped Checkpoint) the spec doesn't say who orders parents before children. The 5.0.1 controller orders a Stream group's segments, handing a segment out once its ancestors are drained, but nothing orders a Checkpoint group's: it has no cursor that would show a parent drained, and an assignment carries no lineage, so Java's members read their segments independently. | Parent-first for ungrouped readers; managed assignments as in Java: a grouped Checkpoint consumer keeps a key's order within a segment only. | To raise |
| 8 | C15 requires refusing a broker without scalable-topic support, but *error-model* §2 has no category for that failure. Java raises `FeatureNotSupportedException`. | `ErrorKind::UnsupportedBroker`. | To raise |
| 9 | *client-api* §2 and §3.3 say a graceful close awaits pending operations and completes pending publishes. Java V5's close waits for what the brokers already have, and fails with `AlreadyClosed` the sends still waiting for a layout, a connection or a segment producer; its tests pin that behavior. | Java's. | To raise |
| 10 | *client-api* §3.1 and *error-model* §2 have a pending-send queue that can be full (`ProducerQueueFull`). Java V5 bounds producers by the client memory limit only, and never raises it. | Java's: the memory limit only; the kind exists, unused. | To raise |
| 11 | *client-behavior* §1 says to apply a layout only if its epoch is greater than the last applied. Java's `DagWatchClient` applies every update it receives. | The spec's: `>`, as in gap 5. | To raise |
| 12 | A message larger than the broker's `max_message_size` can't be sent, but *error-model* §2 has no category for that failure. Java v4 raises `InvalidMessageException`, which Java V5 doesn't map: its send fails with a plain `PulsarClientException`. | `ErrorKind::MessageTooLarge`. | To raise |
| 13 | *error-model* §2 has one category, ProducerBusy / Fenced, for failures the broker tells apart and an application handles differently. The broker sends `ProducerBusy` to a shared producer while an exclusive one holds the topic, for a producer name already connected, and at the topic's producer limits. It sends `ProducerFenced` to an exclusive producer while others hold the topic, and to one that a newer exclusive producer took the topic from. Java V5 maps `ProducerFenced` to `ProducerBusy`. | Two kinds, `ProducerBusy` and `ProducerFenced`, as the broker sends them. | To raise |
| 14 | *client-api* §3.2 gives a producer one monotonically increasing sequence, deduplicated by (producer name, sequence identifier), and C5 asks for the last published id. Java V5 runs a v4 producer per segment, named `<name>-seg-<id>`, each with its own sequence that starts after the broker's last id for that name. Ids increase per segment only: a message routed to another segment takes that segment's next id, and `lastSequenceId()` is the highest over the segments. | Java's. | To raise |
| 15 | *client-api* §3.1 has a send timeout, but doesn't say when a message's clock starts. In Java V5 the timeout is each segment's v4 producer's: it starts when the message reaches that producer, so the time spent waiting for a layout, for a gone segment's successors or for a segment producer's creation doesn't count, and a message routed to another segment starts again. | Java's. | To raise |
| 16 | *wire-protocol* §4 defines an error in a session's first `CommandScalableTopicUpdate`: the create or subscribe fails. It doesn't say what an error in a later update means, whether the session survives it, or whether the client should open it again. In Java, `ClientCnx` drops the session and `DagWatchClient` only logs the error; from the code, no layout arrives after that, even once the connection closes. The broker sends an error only in answer to the lookup today, so neither client meets a later one yet. | Logs it and keeps the session, and opens a new one when the connection closes. | To raise |
| 17 | *client-api* §3.2 has the broker discard duplicates, but doesn't say how the send of a discarded message completes. The broker acknowledges it at position -1:-1, and Java completes the send successfully, with that position in the segment. | Java's. | To raise |
| 18 | *data-model* §4 asks for a total order within a topic, and for two sentinels: earliest, and latest, the next message to be published. It doesn't place the sentinels in the order. Java's `MessageIdV5` gives both segment id -1, so `latest` sorts before every message's id. | Java's order. | To raise |
| 19 | *client-behavior* §5 and *wire-protocol* §6.1 have a Queue consumer subscribe to every active and sealed segment, but don't say when it lets go of a sealed segment it has read to the end: it can hold a subscription on every sealed segment until the segment leaves the layout. Java's `ScalableQueueConsumer` means to close the segment's consumer at the end of the topic, but that branch never runs. | Lets go once the broker reports the end of the topic and every message received from the segment is acknowledged. | To raise |
| 20 | *client-behavior* §5 doesn't say at which position a Queue consumer subscribes to the children of a split or a merge. The broker creates the children with the subscriptions registered on the scalable topic, and a Queue consumer's subscribe registers none. Java subscribes every segment at the configured position, `Latest` by default, so it loses what a producer stores in the children before the new layout reaches the consumer. Against a 5.0.1 broker, a child had no Queue subscription right after a split, and a consumer at `Latest` that subscribed to the children as Java does received none of the 10 messages they stored before it attached. | The configured position for the first layout's segments, the earliest for those a later layout adds. That narrows the hole without closing it: only the broker can, by giving the children their parents' subscriptions. | To raise |
| 21 | *client-behavior* §5 has a Queue consumer subscribe to each segment, and doesn't say who registers its subscription on the scalable topic. The controller knows only the registered subscriptions, so it knows no Queue subscription. Besides gap 20, its garbage collection of a sealed segment past retention checks the backlog of the registered subscriptions only: it deletes the segment, and with it the messages a Queue subscription hasn't acknowledged there. | Java's: it registers nothing. | To raise |
| 22 | *client-api* §3.1 and *conformance* §3 leave compression to a producer policy, and the spec doesn't define the codecs' formats. Java's ZLIB codec flushes its stream and never finishes it, so an entry has no final block and no checksum. A reader that decodes to the end of the stream, as zlib libraries do by default, rejects every entry a Java producer compressed with ZLIB, and a consumer acknowledges each one as corrupt. The spec should say that a reader decodes the recorded size and doesn't need the end of the stream. | Decodes the recorded size and no further, as Java's `Inflater` does. | To raise |
| 23 | *wire-protocol* §6.3 and *client-behavior* §7 don't say how a checkpoint consumer learns that a sealed segment is read to its end, which it needs to read the children (gap 7). The 5.0.1 broker sends `CommandReachedEndOfTopic` to a reader only once the reader has acknowledged everything, though a checkpoint involves no acknowledgment. Java's v4 readers acknowledge each message; Java V5 asks for the last message id once the layout says the segment is sealed. | Readers don't acknowledge; the last message id once sealed, as Java V5. | To raise |
| 24 | A checkpoint consumer started at `latest` must name, in a checkpoint taken before its first message, where it started, or a restore starts at a later latest and skips messages. The spec doesn't say so. Java resolves `latest` to each segment's last message id first. | The same, asked on the reader's own subscription before its first permits. | To raise |
| 25 | *wire-protocol* §6.3 doesn't give a reader's start semantics. The broker sends the entry of a reader's start position again, and the client must leave out what it read, as Java's v4 consumer does: a message of a batch by its index, one not in a batch as its whole entry. | Java's v4 rule. | To raise |
| 26 | The controller's garbage collection deletes a sealed segment's topic past retention, while a layout can still list it, but the spec doesn't say what a checkpoint consumer does with it. Java skips a segment whose reader fails with `TopicNotFound`. | Java's: skipped, and left out of checkpoints. | To raise |
| 27 | *key-shared* §4 and *client-behavior* §6 turn a Stream consumer's cumulative ack on a shared segment into individual acks of every delivered message "at or before" the recorded position. A redelivery, such as the messages of a member that crashed, can come after messages further on in the segment. By position, an ack of the redelivered message leaves the messages given before it unacknowledged, and a drain that waits for them never ends; an ack of one of those covers a redelivered message the application took after it. Java acks by position. | Acks every message given up to the one at the recorded position, in the order given. | To raise |

## Java client and broker fixes to propose upstream

Where the Java V5 client or the broker departs from the spec, or has to change once a gap above is settled. The
two in the broker, J14 and J16, lose data and go first.

| # | Problem | Fix | Status |
|---|---|---|---|
| J1 | `DagWatchClient` applies every layout update it receives, whatever its epoch. *client-behavior* §1 says to apply only a greater one (gap 11). | Ignore an update whose epoch isn't greater than the last applied. | To propose |
| J2 | `AsyncProducerV5.flush()` doesn't check whether the producer is closed: after close, it succeeds. *client-api* §2 says a closed client rejects new operations, and *error-model* §2 makes that `AlreadyClosed`. | Fail with `AlreadyClosedException` once closed, as this client does. | To propose |
| J3 | `MessageIdV5` sorts `LATEST`, the next message to be published, before every message's id, because both sentinels have segment id -1 (gap 18). | Sort `EARLIEST` first and `LATEST` last. | To propose |
| J4 | `V5ExceptionMappings` has no entry for v4's `InvalidMessageException`: a message over the broker's limit fails with a plain `PulsarClientException` (gap 12). | A V5 exception of its own, for the spec's new category. | After gap 12 |
| J5 | `V5ExceptionMappings` maps v4's `ProducerFencedException` to `ProducerBusyException`, and the V5 API has no fenced exception (gap 13). | A `ProducerFencedException`, if the spec splits the category. | After gap 13 |
| J6 | After the first layout, an error update makes `ClientCnx` drop the session, while `DagWatchClient.onError` only logs it. From the code, no layout arrives after that: a later disconnect notifies only the sessions still registered, so producers and consumers keep the last layout for good (gap 16). | Open the session again with backoff, as after a connection loss. | To propose |
| J7 | A Queue consumer's message gives the segment topic as its `topic()`. *data-model* §3 asks for the canonical topic the message belongs to, and *terminology* keeps segments out of what applications see. | Give the scalable topic's `topic://` name, as the namespace consumer already does, and as this client does. | To propose |
| J8 | `ScalableQueueConsumer.negativeAcknowledge` hands v4 the message id alone, which counts as redelivery count 0, so a configured `negativeAckRedeliveryBackoff` always gives its minimum delay. | Pass the message's redelivery count to the backoff. | To propose |
| J9 | `ScalableQueueConsumer` closes a sealed segment's consumer when a receive fails with `TopicTerminatedException`. But in v4 the end of the topic only sets a flag, and no receive fails that way, so the segment stays subscribed until it leaves the layout (gap 19). | Close the segment's consumer once the end of the topic is reached and its messages are acknowledged, as this client does. | To propose |
| J10 | `QueueConsumer.consumerName()` returns null without a configured name, while its javadoc says it never does. | Give the consumer a generated name and derive its segments' names from it, or correct the javadoc. | To propose |
| J11 | `QueueConsumerBuilderV5.subscribeAsync` throws `UnsupportedOperationException` for a `non-persistent://` topic before it returns a future. *error-model* §1 says an operation that returns a future signals every failure through it. | Return a failed future. | To propose |
| J12 | `QueueConsumer` has no multi-message receive, which *client-api* §4 says a client should provide. The Stream and Checkpoint consumers have `receiveMulti`. | Add `receiveMulti` to `QueueConsumer`, like this client's `receive_batch`. | To propose |
| J13 | `ScalableQueueConsumer` subscribes the children of a split or a merge at the configured initial position. At `Latest`, the default, it loses what a producer stored in them before the layout reached it (gap 20). | Subscribe the segments a later layout adds at `Earliest`, and keep the configured position for those of the first layout, as this client does. | To propose |
| J14 | In the broker, `ScalableTopicController` creates a split's or a merge's children with the subscriptions registered on the scalable topic, and a Queue consumer's never is (gap 20). Until a consumer subscribes to a child, the Queue subscription doesn't hold the child's ledgers, so what the child stores can be trimmed at its first rollover. And a subscription with no consumer running during the split is created on the children at the initial position when one comes back: at `Latest`, it skips what they stored meanwhile. No client can close this. | Create on the children every subscription their parents have, as it already does for the registered ones. | To propose first: loses data |
| J15 | `CompressionCodecZLib` flushes the deflater and never finishes the stream, so its entries lack the final block and checksum that zlib readers expect (gap 22). Finishing is compatible: the C++ and Go clients already write finished streams, and the Java, C++ and Go readers all stop at the recorded size, so they read both forms. Entries already written stay unfinished, so every reader has to accept them anyway. | Finish the stream, so that new entries are standard zlib, as this client's are. | Optional |
| J16 | In the broker, the `ScalableTopicController` garbage collection prunes a sealed segment past retention once the registered subscriptions have drained it, and with none registered, at once. It then deletes the segment's topic, and with it the messages a Queue subscription hasn't acknowledged there (gap 21). | Check the backlog of every subscription on the segment's topic before pruning it, as it already does for a legacy segment. | To propose first: loses data |
| J17 | `ScalableCheckpointConsumer` opens its per-segment readers without the `__pulsar.v5.managed=true` metadata that the producer and the Queue and Stream consumers carry. On a legacy segment the broker counts each reader as a v4 client, and refuses to migrate the topic while one is attached, unless forced. | Give the readers the marker, as the producer and the other consumers have it. | To propose |
| J20 | A batch takes its first message's replication clusters (`Commands.initBatchMessageMetadata`), and `ProducerImpl.canAddToBatch` doesn't check them: every message of a batch is replicated to the first one's clusters, though *client-api* §3.2 sets them per message. | Send a message with replication clusters in an entry of its own, as one with a delivery time already is, as this client does. | To propose |

## Reference material

- Spec: `spec/scalable-topics/` in `apache/pulsar`, version 1.0.
- Java reference client: `pulsar-client-v5/src/main/java/org/apache/pulsar/client/impl/v5/` and the API in
  `pulsar-client-api-v5/`. Java's mapping of the spec is *java-mapping*.
- Protocol: `pulsar-common/src/main/proto/PulsarApi.proto`. The scalable-topic commands are `BaseCommand`
  fields 70–82, and broker support is advertised by `FeatureFlags.supports_scalable_topics`.
