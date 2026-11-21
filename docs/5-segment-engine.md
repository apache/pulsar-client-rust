# 5. Segment engine

> Previous: [4. Connections](4-connections.md). Next: [6. Scalable topics](6-scalable-topics.md).

The segment engine is the classic Pulsar producer, consumer and reader for **one topic**: a `segment://`
topic, or the `persistent://` topic wrapped by a legacy segment. The scalable layer creates one per segment
it uses ([6](6-scalable-topics.md)). Applications never see it. A classic-API crate could build on it later
([1](1-scope.md) §6).

Each object is a protocol state machine in `apache-pulsar-protocol` (registration, pending requests, flow
permits, ack tracking), driven by a task on its loop that connects it through the connection layer
([4](4-connections.md)) and runs its timers.

## 1. Lifecycle

All three objects follow the same lifecycle:

```
Connecting ──registered──► Ready ──connection lost──► Reconnecting ──► Ready
     │                       │                              │
     └──────────── fatal error or close ───────────────────►┴──► Closed
```

- **Connecting:** look up the topic, get a pooled connection, register (`CommandProducer`, or
  `CommandSubscribe`).
- **Reconnecting:** re-register with backoff. A producer then resends its pending messages, in order.
- **Fatal errors** are the ones the scalable layer has to act on: the topic was terminated or deleted, the
  producer was fenced, the schema is incompatible, or authorization was denied. The engine reports them up and
  does not retry them.
- `CommandCloseProducer` or `CommandCloseConsumer` from the broker (an unload, for example) means
  reconnect, not a fatal error.

## 2. Producer

**Registration.** `CommandProducer` carries the topic, the producer name, the access mode, the schema and the
producer's properties. The scalable layer names each per-segment producer `<producerName>-seg-<segmentId>` when
the application named the producer (*wire-protocol* §5); otherwise the broker names it. A legacy segment's
producer also carries the property `__pulsar.v5.managed=true`. `CommandProducerSuccess` returns the last sequence
id the broker has stored for that name, which dedup needs (C5). For `wait-for-exclusive` it first answers "not
ready", then again on the same request once the producer gets the topic; the request waits without a deadline
meanwhile.

**Sequence ids.** Each segment's producer has its own sequence ids, as Java's v4 producers do. They start after the
configured initial id, or, without one, after the broker's last id for the name. A message gets its id when it is
first written, keeps it, with its bytes, when it is written again after a reconnect, and takes a new one when it
is routed to another segment. An id set by the application is used as is and doesn't move the counter. The
producer's last sequence id is the highest the brokers confirmed, over its segments' producers
([spec gap 14](../DESIGN.md#spec-gaps-to-raise-upstream)).

**Sending.** Messages flow through:

```
  admitted ──► batch container ──► compress ──► frame ──► pending queue ──receipt──► completed
```

- The **pending queue** keeps sent, unacknowledged entries in send order. Receipts (`CommandSendReceipt`)
  complete them in order, matched against the head of the queue, as in Java: a receipt for an earlier sequence id
  is late and ignored, one for a later id means the two sides disagree, and the connection is closed so that the
  producer registers again. After a reconnect the whole queue is resent, in order, before anything new. A
  receipt at -1:-1, for a duplicate the broker discarded, completes its message with that position, as in Java
  ([spec gap 17](../DESIGN.md#spec-gaps-to-raise-upstream)).
- The **send timeout** fails each message whose deadline passes, without invalidating the producer
  (*client-api* §2). A message's clock starts once its segment's producer is registered, and starts again on
  another segment, as in Java ([spec gap 15](../DESIGN.md#spec-gaps-to-raise-upstream)).
- A message larger than the broker's `max_message_size` fails alone.
- **Batch format, always.** Every entry is a batch: `num_messages_in_batch` in the metadata, and a
  `SingleMessageMetadata` before each message. There is one encoding path, whatever the batch size.
- **One message per batch first.** A one-message batch lies in one bucket by construction, so it is allowed
  without bucketing (*key-shared* §3). It is stamped `entry_hash_min = entry_hash_max =` the message's bucket hash.
  A keyless message has bucket hash 0, as in Java (`EntryBucketBatchContainer`).
- **Then multi-message batches.** One open batch per entry bucket of the segment (*key-shared* §3). A batch
  closes on the maximum messages, the maximum bytes or the maximum delay, and is stamped with the smallest and
  largest bucket hash in it. A segment with one bucket is the same code with one open batch.
- End-to-end encryption publishes one message per entry, as in Java.
- **Compression** applies to each entry's payload. The metadata records the codec and the uncompressed size.
- **Chunking** is a later capability ([1](1-scope.md) §2).

**Segment gone.** When a send fails because the topic was terminated, or the producer is closed because its
segment sealed, the engine hands its unacknowledged messages back to the scalable layer in their original
order. The scalable layer re-routes them (*client-behavior* §4, [6](6-scalable-topics.md) §3).

The broker writes what it stored before it terminates a topic, and before it answers `CommandCloseProducer`. So
once a terminated segment's producer is closed, by the broker or by us, what it sent and never got a receipt for
was not stored, and goes to the successors without being duplicated.

**Close** sends `CommandCloseProducer` and waits for the answer, which comes once the broker has written what it
had: those messages complete. What was never written fails with `AlreadyClosed`, as in Java.

All of a scalable producer's segment producers are state in one sans-IO machine (`apache-pulsar-protocol`'s
`producer` module), run by one task on its loop.

## 3. Consumer

**Registration.** `CommandSubscribe` carries the subscription, the type (Exclusive, Shared, or `Key_Shared`
STICKY with `entryBucketDispatch` and the segment's boundary list in `hashRanges`, *key-shared* §4), the
initial position, `read_compacted`, `replicate_subscription_state` and the schema.

**Flow control.** The consumer grants the broker permits up to its receiver queue size (`CommandFlow`), and
grants more as the application takes messages, once half the queue is free. A message the consumer leaves out
(acknowledged already in its batch, compacted out, or in an entry it can't read) counts as taken, since the broker
counted it.

**Receiving.** A `CommandMessage` is checked, decompressed, and split into the messages of its batch
(`SingleMessageMetadata`); as in Java, an entry is a batch when its metadata sets `num_messages_in_batch`. Messages
already acknowledged in a batch, per the broker's `ack_set`, and messages compacted out are skipped. Each message
carries its segment id up to the scalable layer, which uses it to route acks.

- A corrupt entry (a bad checksum or metadata, a payload that doesn't decompress to its recorded size, a malformed
  batch) is acknowledged with the reason, `validation_error`, so that the broker drops it, as Java does.
- An encrypted entry, or a chunk of a larger message, is left unacknowledged for a consumer that can read it, until
  encryption and chunking come ([Plan](../PLAN.md)). Java fails such an entry the same way by default.

**Acknowledging.**

- Individual and cumulative acks, with batch-index acks inside a batch. An ack names the indexes acknowledged
  since the last one, and the broker drops the entry once every index is acknowledged.
- Acks are grouped by an ack group time (100 ms by default, as in Java) and sent as one `CommandAck` per
  group, or at once when a group reaches 1000. They are fire-and-forget: the scalable-topics API exposes no
  broker-confirmed acks, and the Java V5 `acknowledge` methods return `void`. So the engine never asks for ack
  receipts.
- Negative acks collect for a redelivery delay (60 s by default, as in Java), then go out as
  `CommandRedeliverUnacknowledgedMessages`, a thousand entries at most per command. It names entries, so a negative
  ack of one message of a batch brings the batch back, less the indexes acknowledged meanwhile, as in Java. The
  redelivery count comes back on the redelivered messages.
- Transactional acks are a later capability.

**Draining.** For the Stream consumer's mode flips and segment releases (*key-shared* §4), the consumer can
stop delivering new messages and report when every message it delivered has been acknowledged. It never
acknowledges anything to get there.

**The end of a segment.** The broker tells the consumers of a terminated segment once their subscription has
acknowledged everything there (`CommandReachedEndOfTopic`). A consumer is then drained when it has nothing left to
deliver and its acks are out. The Queue consumer lets the segment go then ([6](6-scalable-topics.md) §4).

**Reconnecting.** Messages received but not yet delivered to the application are dropped, since the broker
redelivers them. The consumer then resubscribes, with backoff, and the broker resumes from the subscription's
durable cursor. The acks made meanwhile go out before the new permits, as in Java. Readers work differently (§4).

## 4. Reader

A reader is a consumer on an Exclusive subscription that isn't durable, which starts after a message id
(`start_message_id`), and acknowledges nothing. It keeps its position: where it starts, then the last message taken
from it. A subscription that isn't durable has no cursor, so each subscribe, a reconnect's too, starts there. The
broker sends the entry of that position again, and the reader leaves out what it read, as Java's v4 consumer does:
a message of a batch by its index, one not in a batch as its whole entry. Earliest and latest map to the protocol's
sentinel message ids. A reader at latest gets no permits until it is told where that is, the segment's last
message id, so that a checkpoint can name it. There is no seek: a restore creates the readers at the saved positions
([6](6-scalable-topics.md) §6.1).

## 5. Dead-letter (later)

For the Queue consumer: a message whose redelivery count passes the limit is published to the dead-letter
topic and then acknowledged on its segment (*client-api* §4.2). The dead-letter producer is a scalable
producer like any other.

## 6. Open questions

1. **Ack timeout and redelivery backoff:** in the first consumer milestone, or later?
