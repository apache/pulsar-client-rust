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
| 7 | Per-key order across a split for the ungrouped checkpoint consumer needs a sealed parent read to its end before its children. *client-behavior* §7 and *wire-protocol* §6.3 don't say so; Java does it from the layout's lineage. For managed assignments (Stream, grouped Checkpoint) the spec doesn't say who orders parents before children. | Parent-first for ungrouped readers; managed assignments as in Java. | To raise |
| 8 | C15 requires refusing a broker without scalable-topic support, but *error-model* §2 has no category for that failure. Java raises `FeatureNotSupportedException`. | `ErrorKind::UnsupportedBroker`. | To raise |
| 9 | *client-api* §2 and §3.3 say a graceful close awaits pending operations and completes pending publishes. Java V5's close waits for what the brokers already have, and fails with `AlreadyClosed` the sends still waiting for a layout, a connection or a segment producer; its tests pin that behavior. | Java's. | To raise |
| 10 | *client-api* §3.1 and *error-model* §2 have a pending-send queue that can be full (`ProducerQueueFull`). Java V5 bounds producers by the client memory limit only, and never raises it. | Java's: the memory limit only; the kind exists, unused. | To raise |
| 11 | *client-behavior* §1 says to apply a layout only if its epoch is greater than the last applied. Java's `DagWatchClient` applies every update it receives. | The spec's: `>`, as in gap 5. | To raise |
| 12 | A message larger than the broker's `max_message_size` can't be sent, but *error-model* §2 has no category for that failure. Java v4 raises `InvalidMessageException`, which Java V5 doesn't map: its send fails with a plain `PulsarClientException`. | `ErrorKind::MessageTooLarge`. | To raise |
| 23 | *wire-protocol* §6.3 and *client-behavior* §7 don't say how a checkpoint consumer learns that a sealed segment is read to its end, which it needs to read the children (gap 7). The 5.0.1 broker sends `CommandReachedEndOfTopic` to a reader only once the reader has acknowledged everything, though a checkpoint involves no acknowledgment. Java's v4 readers acknowledge each message; Java V5 asks for the last message id once the layout says the segment is sealed. | Readers don't acknowledge; the last message id once sealed, as Java V5. | To raise |
| 24 | A checkpoint consumer started at `latest` must name, in a checkpoint taken before its first message, where it started, or a restore starts at a later latest and skips messages. The spec doesn't say so. Java resolves `latest` to each segment's last message id first. | The same, asked on the reader's own subscription before its first permits. | To raise |
| 25 | *wire-protocol* §6.3 doesn't give a reader's start semantics. The broker sends the entry of a reader's start position again, and the client must leave out what it read, as Java's v4 consumer does: a message of a batch by its index, one not in a batch as its whole entry. | Java's v4 rule. | To raise |
| 26 | The controller's garbage collection deletes a sealed segment's topic past retention, while a layout can still list it, but the spec doesn't say what a checkpoint consumer does with it. Java skips a segment whose reader fails with `TopicNotFound`. | Java's: skipped, and left out of checkpoints. | To raise |

## Reference material

- Spec: `spec/scalable-topics/` in `apache/pulsar`, version 1.0.
- Java reference client: `pulsar-client-v5/src/main/java/org/apache/pulsar/client/impl/v5/` and the API in
  `pulsar-client-api-v5/`. Java's mapping of the spec is *java-mapping*.
- Protocol: `pulsar-common/src/main/proto/PulsarApi.proto`. The scalable-topic commands are `BaseCommand`
  fields 70–82, and broker support is advertised by `FeatureFlags.supports_scalable_topics`.
