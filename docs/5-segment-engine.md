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
configured initial id, or, without one, after the broker's last id for the name. A message gets its id when it goes
into a batch, once the segment's producer is registered, keeps it, with its entry's bytes, when the entry is
written again after a reconnect, and takes a new one when it is routed to another segment. An id set by the
application is used as is and doesn't move the counter. The producer's last sequence id is the highest the brokers
confirmed, over its segments' producers ([spec gap 14](../DESIGN.md#spec-gaps-to-raise-upstream)).

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
  `SingleMessageMetadata` before each message. There is one encoding path, whatever the batch size; Java sends a
  lone message as a plain entry.
- **One batch per entry bucket.** A segment's producer keeps one open batch per entry bucket of the segment
  (*key-shared* §3), so that an entry holds keys of one bucket only, and stamps each entry with the smallest and
  largest bucket hash in it. A keyless message has bucket hash 0, as in Java (`EntryBucketBatchContainer`). A
  segment with one bucket is the same code with one open batch.
- **The batches of a segment close together**, as in Java's `EntryBucketBatchContainer`: once the messages in them
  reach the maximum count, or their payloads the maximum bytes, which is at most the broker's largest message, or
  the maximum delay after the first went in. The delay so bounds each message's wait, as Java's `BatchingPolicy`
  documents it; Java's timer counts from the last batch sent, so a batch can wait a little longer there. Their
  entries go out in the order of their sequence ids, each entry's being its highest message's, while every message
  keeps its own in its `SingleMessageMetadata`. The order matters: the broker drops, as a duplicate, an entry whose
  sequence id isn't above the last one it took from the producer, which batches closing one by one would send.
- **What goes alone.** A message that would take the open batches past the maximum bytes closes them first, so a
  message too large for the broker fails alone. A message with a delivery time or replication clusters, which are
  its entry's, goes in an entry of its own, after the open batches: Java does so for a delivery time, but gives
  every message of a batch its first message's clusters
  ([J20](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)). So does a message whose sequence id is at
  or below the last one given, as in Java: the broker may drop it as a duplicate, and its entry with it.
- **The defaults** are Java V5's `BatchingPolicy`: on, a 1 ms delay, 1000 messages, 128 KiB. Without batching, each
  message is an entry of its own. A flush closes the open batches, and covers the messages still waiting for a
  registration or a layout too; the blocking API's send flushes, as Java's blocking send does.
- **An entry is stored whole or not at all.** Its messages time out together, at its first message's deadline, and
  a close fails what the open batches hold, never written, as Java fails its batch container.
- End-to-end encryption publishes one message per entry, as in Java.
- **Compression** applies to each entry's payload, a whole batch, whatever its size: Java leaves payloads of 4 KiB
  or less as they are, a v4 setting V5 doesn't expose. The metadata records the codec and the uncompressed size, and
  the broker's limit applies to the entry as compressed ([3](3-protocol.md) §6).
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
- A cumulative ack, for the Stream consumer ([6](6-scalable-topics.md) §5.2), goes out once per group: the highest
  position acknowledged, inside a batch with the later indexes left in its ack set, as Java writes it. One at or
  before an ack that went out already on the registration adds nothing; a new registration forgets them, since the
  broker may deliver again what they covered and may never have had them. A `Key_Shared` subscription forbids
  cumulative acks: there the consumer acknowledges one by one each message it gave the application up to the one at
  the position, in the order it gave them, since a redelivery can come after messages further on in the segment
  ([spec gap 27](../DESIGN.md#spec-gaps-to-raise-upstream)).
- Negative acks collect for a redelivery delay (60 s by default, as in Java), then go out as
  `CommandRedeliverUnacknowledgedMessages`, a thousand entries at most per command. It names entries, so a negative
  ack of one message of a batch brings the batch back, less the indexes acknowledged meanwhile, as in Java. The
  redelivery count comes back on the redelivered messages.
- Transactional acks are a later capability.

**Draining.** For the Stream consumer's mode flips and segment releases (*key-shared* §4), the consumer can
stop delivering new messages and report when every message it delivered has been acknowledged: each one on a
`Key_Shared` subscription, and on an Exclusive one the last one given, against the highest cumulative ack. It never
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
