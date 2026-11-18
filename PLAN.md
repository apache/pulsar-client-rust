# Plan

Milestones, in order. Milestone 0 removes the main risks before anything is built on them. Each later
milestone is a vertical slice, from the wire up to the public API, and is done when its checks pass in CI. The
design is in [DESIGN.md](DESIGN.md).

## M0: Foundations and risks

Settle what everything else depends on.

- **Repository:** a Cargo workspace with `apache-pulsar`, `apache-pulsar-protocol` and `apache-pulsar-testkit`;
  ASF license headers, `LICENSE`, `NOTICE`; CI on Linux and macOS; `cargo-deny`.
- **Protobuf:** generate with `buffa`, with an open enum for `BaseCommand.type`, add the proto2-default accessors
  and the test that checks them against every `[default]` in the proto ([3](docs/3-protocol.md)). Vendor
  `PulsarApi.proto` from a pinned `apache/pulsar` commit, commit the generated code, and add a script that
  refreshes it and checks it in CI.
- **compio:** small programs that check, on Linux with io_uring, Linux with the polling driver, and macOS
  ([2](docs/2-architecture.md) §5):
  - TLS through `compio-tls` and rustls;
  - waking a loop from another thread;
  - timers;
  - dropping a future while its operation is in flight;
  - DNS resolution.
- **First contact:** the codec and the `CONNECT` handshake against a broker container built from
  `apache/pulsar` master, reading the broker's feature flags.
- **Vectors from Java:** a small Java program, run against the `apache/pulsar` build, generates:
  - routing vectors for keys (raw hash, segment hash, bucket hash, and the v4 partition for N partitions);
  - golden bytes for `MessageIdV5` and `CheckpointV5`.

**Checks:** the handshake passes against the container in CI. Rust's Murmur3 matches every vector. The compio
checks pass on all three setups, or the design is adjusted where they don't.

## M1: Connections and the two client models

- The connection state machine: requests and responses, keepalive, feature-flag gating, token and mTLS
  authentication, auth refresh ([3](docs/3-protocol.md)).
- The per-loop pool keyed by logical and physical address; lookup with redirects and proxies; reconnection with
  backoff and coalescing ([4](docs/4-connections.md)).
- The simulated broker's skeleton in `apache-pulsar-testkit`.
- The local client, and the thread-safe client over N loops: placement, the hop, admission counters, close
  ([2](docs/2-architecture.md) §4).

**Checks:** lookups against the container, directly and through a Pulsar proxy. Disconnect and timeout scenarios
pass on the simulated broker.

## M2: Producer, one message per batch

- Every entry in the batch format, one message per batch, with its bucket stamp ([5](docs/5-segment-engine.md) §2).
- The layout watch, routing, and the scalable producer: lazy per-segment producers, access modes, dedup, send
  timeout, the queue and memory limits, flush and close ([6](docs/6-scalable-topics.md) §2–§3).
- The barrier that keeps per-key order across splits and merges.
- Routing over a synthetic legacy layout, for `persistent://` topics.
- MessageId encoding, errors, and the producer in the default, local and blocking APIs ([7](docs/7-api.md)).

**Checks:** C1–C6 and the producer side of C12–C15. Ported scenarios from `ScalableTopicProducerTest`,
`ScalableTopicProducerOrderingTest`, `V5ProducerSegmentGoneTest`, `V5ProducerLayoutChangeOrderingTest`,
`V5ProducerAccessModeTest`, `V5ProducerBackpressureTest`, `V5ProducerClosingCompletionTest` and
`V5ScalableTopicAutoCreateTest`. Golden MessageId bytes round-trip with Java. A test confirms that the
pipelining patterns in the rustdoc (`buffered`, `FuturesOrdered`, `join_all`) keep send order
([7](docs/7-api.md) §3.1).

## M3: Queue consumer

- The segment consumer: Shared subscriptions, flow control, parsing of batched and compressed entries (other
  clients produce them), grouped acks, negative acks and redelivery ([5](docs/5-segment-engine.md) §3).
- The Queue consumer: attach to every segment, keep that set in step with the layout, one receive queue, ack
  routing by segment ([6](docs/6-scalable-topics.md) §4).

**Checks:** C8. Ported scenarios from `ScalableQueueConsumerReconcileTest`, `ScalableQueueConsumerReceiveLoopTest`
and `V5MultiTopicQueueConsumerTest`. Java producer ⇄ Rust consumer.

## M4: Stream consumer

- The assignment session: register, apply updates, reconnect within grace, unsubscribe on close.
- Exclusive per-segment subscriptions, position vectors, and cumulative ack fanned out across segments.
- The consumer side of entry-bucketing: `Key_Shared` attach, individual acks on shared segments, drain before a
  mode flip or a release ([6](docs/6-scalable-topics.md) §5).

**Checks:** C7. Ported scenarios from `ScalableStreamConsumerReconcileTest`, `V5StreamConsumerAckWatermarkTest`
and `V5StreamConsumerDagReplayTest`, plus more consumers than segments, so that segments are shared.

## M5: Checkpoint consumer

- Readers on every segment, parents first, checkpoint capture and restore, the grouped mode
  ([6](docs/6-scalable-topics.md) §6).
- Checkpoint encoding.

**Checks:** C9 and C10. Ported scenarios from `ScalableCheckpointConsumerTest`,
`ScalableCheckpointConsumerReconcileTest` and the `V5CheckpointConsumer*Test` classes. Checkpoints round-trip with
Java across a layout change.

## M6: Batching and compression

- Multi-message batches: one open batch per entry bucket, with the stamp ([5](docs/5-segment-engine.md) §2).
- LZ4, ZSTD, ZLIB and Snappy on the producer side.
- A first round of benchmarks ([8](docs/8-quality.md) §6).

**Checks:** ported scenarios from `V5ProducerCompressionTest`. Key affinity holds with batched producers and shared
segments. A performance baseline is recorded.

## M7: Conformance and a first release

- The rest of the schemas, generic records included (C11, [7](docs/7-api.md) §8).
- End-to-end tests of the migration bridge: partitioned and non-partitioned `persistent://` topics, with v4 clients
  running alongside.
- An audit of the error model, rustdoc and examples (send pipelining among them), and the conformance suite
  ([8](docs/8-quality.md) §2.2).
- A first ASF release.

**Checks:** the conformance checklist C1–C15 is complete, and `tests/PORTING.md` shows the Java scenarios in scope
covered.

## After the first release

In rough order: OAuth2 authentication, dead-letter policy, namespace subscription, transactions, end-to-end
encryption, chunking, Windows as a tested platform. Then the later phase: a base for other language SDKs
([1](docs/1-scope.md) §6).

After M2, the three consumer milestones (M3–M5) can proceed in parallel. They share the segment consumer from M3,
so its subscription and ack parts land first.
