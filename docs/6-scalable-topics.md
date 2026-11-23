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

# 6. Scalable topics

> Previous: [5. Segment engine](5-segment-engine.md). Next: [7. Public API](7-api.md).

The scalable layer turns a set of segments into one producer or one consumer. Segments, layouts and
assignments are internal; the application never sees them (*conformance* §4). Everything in this document
runs on one loop, as plain state machines fed by the driver ([2](2-architecture.md)).

## 1. Topic names

- `topic://tenant/namespace/name` is the canonical form (*data-model* §1).
- `persistent://…` and the short form (`name`, or `tenant/namespace/name`) address an existing topic through
  the migration bridge (§8).
- The name the broker returns in `resolved_topic_name` is authoritative and is the one the client reports
  (*wire-protocol* §4).

## 2. Layout watch

A **layout watch** holds a topic's current `ScalableTopicDAG` and pushes changes to the objects that use it.

- **One watch per topic per loop,** shared by every producer and Queue or ungrouped Checkpoint consumer of
  that topic on the loop, and closed with the last of them. Stream and grouped Checkpoint consumers use one
  only to find the controller, then close it, as Java does: their segments come from the assignment (§5).
- **Opening:** connect as described in [4](4-connections.md) §3, check `supports_scalable_topics`, and send
  `CommandScalableTopicLookup`. If the first `CommandScalableTopicUpdate` carries an `error`, the create or
  subscribe that opened the watch fails with the mapped category, `NotFound` included (*error-model* §2). An
  error in a later update is logged and the session kept: a new one opens when the connection closes. Java
  drops the session instead ([spec gap 16](../DESIGN.md#spec-gaps-to-raise-upstream)).
- **Updates:** a layout is applied only if its epoch is greater than the last applied (*client-behavior* §1,
  [spec gap 5](../DESIGN.md#spec-gaps-to-raise-upstream)), or if it is a migrated topic's first layout, which the
  5.0.1 broker gives the epoch of the synthetic layout it replaces, 0 (§8,
  [spec gap 29](../DESIGN.md#spec-gaps-to-raise-upstream)). Each applied layout reaches every user of the watch as
  a new immutable snapshot.
- **Reconnecting:** after a connection loss the watch re-sends the lookup with backoff, and keeps serving
  the last layout until a new one arrives.

A layout snapshot keeps, per segment: id, hash range, state (active or sealed), parents and children,
bucket boundaries, the legacy topic name if there is one, and the segment topic name (*wire-protocol*
§2–§3). It also keeps the sorted list of active segments, for routing.

## 3. Producer

### 3.1 Routing

Routing must match every other client exactly (C2, *client-behavior* §2):

```
h       = murmur3_32_raw(key_utf8)        // once per key
segment = active segment containing (h >>> 16)
bucket  = bucket of the segment containing (h & 0xFFFF)   // entry-bucketing
keyless → round-robin over the active segments
all segments legacy → signSafeMod(murmur3_32(key_utf8), N)  // v4 partition routing
```

The raw hash (Java's `Murmur3_32Hash.makeRawHash`) and the v4 partitioning hash (`makeHash`) are not the same
function. Both are checked against vectors generated from the Java client, before anything is built on them
([8](8-quality.md)).

### 3.2 Per-segment producers

- A per-segment producer is created **lazily**, on the first message routed to its segment
  (*client-behavior* §2).
- Under `exclusive`, `exclusive-with-fencing` and `wait-for-exclusive`, the producer attaches to every
  active segment while it is being created. A conflict then fails `create()`, not the first send. New
  segments from a later split are attached as soon as the layout arrives.
- Each has its own sequence ids ([5](5-segment-engine.md) §2). The last published sequence id (C5) is the highest
  one completed across segments, as in Java.
- A segment that is gone gets no new producer: messages routed to it wait for the next layout.

### 3.3 Order across splits and merges

All sends of a producer go through one state machine, run by one task on its loop, so they reach each segment's
queue in call order (*client-behavior* §3). The mechanism follows Java's `ScalableTopicProducer`: a segment that
goes away **retires**.

- It retires when its topic is terminated (a send or a registration fails with `TopicTerminated`), or when a layout
  drops it while it still has messages.
- Its messages that the broker didn't store are **parked**: at once for those never written, and for the written
  ones once its producer is closed, since the broker answers the close only after writing what it stored
  ([5](5-segment-engine.md) §2).
- New sends whose key the retiring segment owned, in the layout it was active in, are **held**, in call order.
  Other keys, and keyless sends, keep flowing.
- Once the layout no longer has the segment and nothing of it is in flight, its parked messages are routed again
  in call order, merging the parents of a merge, then the held ones. Each successor so gets a key's older
  messages before its newer ones.

A message is parked at most ten times; past that its send fails with the last error, as in Java
(*client-behavior* §4). A segment that is gone while the layout still has it gets ten checks, `min(100 ms × n,
500 ms)` apart (4 s in all), before its parked messages fail. Ported scenarios from
`ScalableTopicProducerOrderingTest`, `V5ProducerSegmentGoneTest` and `V5ProducerLayoutChangeOrderingTest` check it,
deterministically on the simulated broker and with a split during traffic on a real one.

### 3.4 Flush and close

*Flush* waits for every message sent before the call, on every segment, and fails if one of them failed, as
Java's does. Their batches go out at once, without waiting out the batching delay ([5](5-segment-engine.md) §2). On a closed producer it fails with `AlreadyClosed` (*client-api* §2), where Java's succeeds
([J2](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)). *Close* fails the sends still waiting
(parked, held, or not written yet) with `AlreadyClosed`, closes every per-segment producer, which waits for what
the brokers already have, releases its layout watch, and can be called again safely (*client-api* §3.3). It
follows Java: it does not flush, and a registration still waiting for exclusive access doesn't hold it up. An
application that wants everything published flushes first.

## 4. Queue consumer

- Subscribes Shared to **every** active and sealed segment in the layout, and keeps that set in step with
  each new layout: it attaches new segments and detaches the ones that leave (*client-behavior* §5).
- **The initial position** applies to the segments of the first layout. A segment a later layout adds is
  subscribed at the earliest position: it is the child of a split or a merge, created after the subscription,
  and the broker creates the children with the subscriptions registered on the scalable topic, which a Queue
  consumer's subscribe doesn't register. Java subscribes it at the initial position, so a consumer at `Latest`
  (the default) loses what a producer stored in the children before the consumer heard of the layout
  ([spec gap 20](../DESIGN.md#spec-gaps-to-raise-upstream)). A consumer created on a layout that is already stale
  can likewise see a few messages from before it subscribed.

  This narrows the hole without closing it. Until the consumer subscribes to a child, no subscription holds the
  child's ledgers, so a ledger rolled over and trimmed meanwhile is lost, though a rollover takes at least 10 minutes
  by default. And a subscription with no consumer running during the split is created on the children at the
  initial position when one comes back. Only the broker can close it, by giving the children their parents'
  subscriptions when it creates them ([J14](../DESIGN.md#java-client-and-broker-fixes-to-propose-upstream)).
- **Subscribe is all or nothing**, as in Java: it completes once every segment is subscribed, those of layouts
  that come meanwhile included, and fails, closing every segment's consumer, if one can't be.
- **Retries.** A segment whose consumer gives up, because a subscribe failed for good, is subscribed again after
  a backoff shared by all segments (100 ms doubling to 30 s, reset once every segment is subscribed), or at once
  when a new layout comes, as Java's reconcile does.
- Receives take messages from the per-segment consumers, which buffer up to the receiver queue size each, as
  Java's do. Segments with messages ready take turns, so a busy segment can't starve the others. A message is
  taken only for a receive, so the broker's permits follow what the application takes.
- An ack or negative ack goes to the per-segment consumer named by the segment id in the message id. One for a
  segment the consumer no longer has does nothing, as in Java.
- **Detach once drained.** The consumer doesn't wait for a sealed segment to leave the layout. It closes its
  per-segment consumer as soon as the segment is drained for this consumer. That means the broker has signaled
  the end of the topic (`CommandReachedEndOfTopic`, which Java sees as `TopicTerminatedException`), and every
  message this consumer received from the segment has been acknowledged. Requiring the acks as well keeps a late
  ack or negative ack from being lost after the close. Java means to close at the end of the topic, but that code
  never runs, so its segment consumers stay subscribed until the segment leaves the layout
  ([spec gap 19](../DESIGN.md#spec-gaps-to-raise-upstream)). A segment that leaves the layout before it is
  drained is detached anyway.
- **Garbage collection.** The controller doesn't know a Queue consumer's subscription: nothing registers it on
  the scalable topic. Once a sealed segment is past retention, the controller checks only the registered
  subscriptions' backlog before it deletes the segment, so it can delete messages a Queue subscription hasn't
  acknowledged. Only the broker can fix this, by checking every subscription on the segment
  ([spec gap 21](../DESIGN.md#spec-gaps-to-raise-upstream)).
- Dead-letter (later): [5](5-segment-engine.md) §5.

## 5. Stream consumer

### 5.1 Assignment session

The consumer registers with the topic's controller (*wire-protocol* §6.2, *client-behavior* §8). The session is a
state machine of its own (`assignment`), which the grouped Checkpoint consumer shares (§6.2).

1. Find the controller ([4](4-connections.md) §3): behind a Pulsar proxy, pair with any broker through it; otherwise
   connect to the layout's `controller_broker_url`, or, while the layout names none, to the service URL. A topic's
   first layout names none until its controller is elected. The 5.0.1 broker never sets `controller_broker_url_tls`,
   so a client on TLS uses the service URL.
2. Send `CommandScalableTopicSubscribe`, type `STREAM`, under the consumer's name: the controller knows a member by its
   name, across reconnects. A consumer without a name gets a random one, `v5-stream-` and 8 letters and digits, as in
   Java. The response carries the first assignment, or an error that fails `subscribe()`; so does a connection lost
   before it, as in Java.
3. Apply each assignment whose `layout_epoch` is at least the last applied
   ([spec gap 5](../DESIGN.md#spec-gaps-to-raise-upstream)). An assignment that repeats a segment unchanged leaves it
   alone.
4. On connection loss, register again with a backoff from 100 ms doubling to 30 s. The segments' consumers go on
   meanwhile. Within the controller's grace period (60 s) the same assignment comes back; after it, a rebalanced one.
5. On close, send `CommandScalableTopicUnsubscribe`, once a registration in flight settles, so that the controller
   rebalances at once. Its outcome never fails the close.

Applying an assignment, segment by segment:

| Change | Action |
|---|---|
| Added, `bucket_ranges` empty | Subscribe Exclusive. |
| Added, `bucket_ranges` set | Subscribe `Key_Shared` by entry bucket, declaring those boundaries (*key-shared* §4). |
| Removed | Drain, then close. |
| Mode flip (empty ⇄ set) | Drain, close, then subscribe in the new mode. |

*Drain* means stop delivering from the segment, and wait until the application has acknowledged everything it was
given from it ([5](5-segment-engine.md) §3); the messages still in go to the next owner, in order. The client never
acknowledges on the application's behalf. A segment comes back only once its old consumer is closed. Each segment's
consumer is named `<name>-seg-<id>`, and one on a legacy segment carries the V5-managed property, as the Queue
consumer's do, and as in Java.

While another member still holds a segment the controller moved, the broker refuses the subscribe with `ConsumerBusy`
or `ConsumerAssignError`. `subscribe()` waits those out, up to the operation timeout, as Java's does, and fails on any
other refusal. Later, a segment whose consumer gives up is subscribed again after a backoff shared by every segment,
100 ms doubling to 30 s, or at once when an assignment comes.

**What the 5.0.1 controller does.** It hands a segment out only once every ancestor of it is drained for the
subscription, and takes a drained sealed segment back. That keeps a key's older messages first across splits and
merges, so the client adds no ordering of its own ([spec gap 7](../DESIGN.md#spec-gaps-to-raise-upstream)). While
members don't outnumber the segments, each segment goes whole to one member; past that, the members share segments
by entry bucket, and those beyond the topic's buckets get nothing until one leaves. A new topic's segment has four
buckets, and each child of a split gets half its parent's.

**The initial position** applies to the segments of the layout the consumer subscribes on. A segment created after
that is subscribed at the earliest position, as in the Queue consumer (§4). That matters only for a consumer whose
registration lapsed while the segment was created. While a member of the subscription is registered, the broker
creates a split's or a merge's children with the subscription, at their start. Once none is, it creates them without
it: observed on a 5.0.1 broker after the last member's unsubscribe, and the same from the controller's code past a
grace period. Java subscribes every segment at the initial position.

Two cases stay lossy at `Latest`, and only the broker can close them, by creating the subscription on every segment
when a member registers:

- A consumer that subscribes after such a split has the children in its first layout, so it subscribes them at
  `Latest`: it skips what they stored before.
- A new subscription on a topic split before it: the controller hands out the children only once the parent is
  drained, which it sees from a poll every 2 s or more, and the children are then subscribed at their end. Against a
  5.0.1 broker, none of 10 messages sent to the children right after `subscribe()` returned arrived.

### 5.2 Position vector and cumulative ack

When the application takes a message, the message records the position vector: the last message given to the
application from every segment the consumer holds, its own included (*client-behavior* §6). Taking it at delivery, not
at arrival, keeps prefetched messages out of it. A cumulative ack of that message then:

- on an **Exclusive** segment, acks cumulatively up to the recorded position, unless an ack at or after it went out
  already on that consumer's registration;
- on a **shared** segment, where the broker forbids cumulative acks, acks individually every message given from that
  segment up to the one at the recorded position, in the order given, and not yet acknowledged. *key-shared* §4 says
  every message at or before the position, and Java does that, but a redelivery can come after messages further on in
  the segment: the messages of a member that crashed go to another after its own. By position, an ack of the
  redelivered message would leave those given before it, and the drain that waits for them would never end; an ack of
  one of those would cover a redelivered message the application took after it
  ([spec gap 27](../DESIGN.md#spec-gaps-to-raise-upstream)). That needs the messages given and not acknowledged, in
  order, per shared segment;
- on a segment the consumer no longer holds, does nothing, as in Java.

Each message holds its own copy of the vector, a map with an entry per segment the consumer holds. The persistent maps
this document proposed, to share the copies, come from MPL-licensed crates (`im`, `rpds`), which `deny.toml` doesn't
allow; a copy costs O(segments held), and §10 keeps the question open. Serializing a message id writes its vector in
full (Java's format, [7](7-api.md) §7).

## 6. Checkpoint consumer

### 6.1 Ungrouped

- A layout watch, and one reader per segment, active or sealed, in step with each new layout (*client-behavior*
  §5, *wire-protocol* §6.3). A reader is a segment consumer ([5](5-segment-engine.md) §4) on an Exclusive
  subscription that isn't durable, under a random `reader-…` name, as Java's v4 reader has. A segment that a layout
  drops leaves the checkpoint, and its reader is closed.
- **Where a reader starts.** Each subscribe names the reader's position: where it starts, then the last message
  taken from it, so a reconnect resumes there. The broker sends the entry of that position again, and the reader
  leaves out what it read, as Java's v4 consumer does: a message of a batch by its index, a message not in a batch
  as its whole entry ([spec gap 25](../DESIGN.md#spec-gaps-to-raise-upstream)).
- **Parents first:** a segment's messages are delivered only once every parent that is still in the layout has
  been read to its end, or is gone. The parents hold the older messages of the segment's keys, so this keeps
  per-key order across a split or a merge. Its reader subscribes at once and buffers meanwhile, as Java's does. The
  spec doesn't state it; the Java client does it from the layout's lineage
  ([spec gap 7](../DESIGN.md#spec-gaps-to-raise-upstream)).
- **The end of a sealed segment.** Once a layout says a segment is sealed, the consumer asks for its last message
  id (`CommandGetLastMessageId`), which no longer changes, since the controller terminates a segment before it
  commits the layout that seals it. The segment is read to its end once its reader took that message. Then its
  reader is closed, and its position stays in the checkpoint, so that a restore doesn't read it again. The broker
  sends `CommandReachedEndOfTopic` to a reader only once the reader has acknowledged everything, and these readers
  don't acknowledge: it comes only to a reader that starts at the end
  ([spec gap 23](../DESIGN.md#spec-gaps-to-raise-upstream)). Java asks for the last message id too.
- **`checkpoint()`** snapshots, for each segment, the position of the last message a receive returned, not of
  anything prefetched. A receive dropped with messages gives them back, and the checkpoint goes back to before
  them. Being on one loop makes the snapshot atomic without locks (*client-behavior* §7). Segments nothing was
  received from are in it too, at their start: a restore would otherwise read them from their beginning.
- **Restore:** each segment in the checkpoint resumes right after its saved position. Segments created after
  the checkpoint start at their beginning, behind the parents-first rule.
- **A segment that is gone**, its topic deleted while the layout still has it (by the controller's garbage
  collection, past retention), is skipped: its subscribe fails with `TopicNotFound`. It leaves the checkpoint and
  holds no child back ([spec gap 26](../DESIGN.md#spec-gaps-to-raise-upstream)). The controller creates a split's
  children before it commits their layout, so a segment that isn't found was deleted.
- **`latest`:** a `latest` start becomes "the end of each segment in the first layout". Segments that appear
  later are then read from their beginning, like the segments missing from a restored checkpoint. Each of those
  readers subscribes at the latest position without permits, asks for the segment's last message id, and reads
  after it, leaving out what the broker sends from before it. A checkpoint taken before any message then names
  that message, not a moving latest, and restores what the consumer read
  ([spec gap 24](../DESIGN.md#spec-gaps-to-raise-upstream)). Java asks a separate reader before it attaches the
  real one.
- **Create** is all or nothing, as in Java: it completes once every reader is subscribed and knows where it
  starts, and fails, closing them all, if one can't be. A reader that gives up later is attached again after a
  backoff shared by all segments (100 ms doubling to 30 s), or at once when a new layout comes.
- A reader on a legacy segment carries the V5-managed property, as the Queue consumer's do, so that a migration
  doesn't count it as a v4 client. Java's readers don't.

These rules follow the Java client (`ScalableCheckpointConsumer`, `CheckpointV5.latestOf`). Ported scenarios from
`ScalableCheckpointConsumerTest`, `ScalableCheckpointConsumerReconcileTest` and the `V5CheckpointConsumer*Test`
classes check them on the simulated cluster (`crates/apache-pulsar-testkit/tests/checkpoint_consumer.rs`), and
integration tests on the real broker, a checkpoint round trip with the Java client across a split and a merge
among them.

### 6.2 Grouped

A member of a group registers with the topic's controller like the Stream consumer (§5.1), with type `CHECKPOINT`,
the group as the subscription, and its name; one without a name gets a random one, `v5-checkpoint-` and 8 letters
and digits, as in Java. It reads the segments the controller assigns it with the readers of §6.1, each named
`<name>-seg-<id>`. No cursor is persisted: each member starts from its own start position or restored checkpoint
(*client-api* §4.3), and its checkpoint names only its own segments.

- **What the 5.0.1 controller does.** It assigns a group every segment of the layout, active and sealed, each whole
  to one member, round robin over the members sorted by name; members beyond the segments get none. It orders
  nothing for a group: no cursor shows it how far the group read a parent, so it hands out a split's children at
  once.
- **No parents first.** An assignment carries no lineage, so a member reads each of its segments as messages come,
  as Java's does, and asks for no sealed segment's end: no child waits for it. A key's messages keep their order
  within a segment, but not across a split or a merge, whether the parent and the child go to one member or two
  ([spec gap 7](../DESIGN.md#spec-gaps-to-raise-upstream)).
- **A segment that leaves the assignment** gives nothing more, not even what its reader had in, and leaves the
  checkpoint; its reader is closed. One that comes back gets a new reader once the last one is closed and the last
  message id it asked for, if any, is answered: a member's readers share one subscription, on which the broker
  refuses a second reader, and the answer must reach no other reader. The new reader starts at the member's start,
  as in Java. A message taken from a reader before its segment left, and given to the application only after, as a
  batch receive waiting for more does, moves no checkpoint: each message carries its segment's attachment, which a
  segment that comes back renews, as Java fences its positions by reader attachment.
- **The latest position** applies to each segment a member gets, whenever it gets it: an assignment doesn't tell a
  segment created after the member started from one moved to it. Java does the same. A segment that a restored
  checkpoint doesn't name is read from its start.
- **Create and close.** The create completes once the controller registered the member and every reader of its
  assignment is subscribed and knows where it starts; a member assigned nothing completes too. A refused
  registration, or a connection lost before its answer, fails the create, as in Java. Closing unsubscribes, so that
  the controller hands the member's segments over at once, and closes the readers.
- **Reconnects.** The registration comes back as the Stream consumer's does (§5.1). The readers go on meanwhile, and
  within the controller's grace period the same assignment comes back.

Ported scenarios from `V5CheckpointConsumerGroupTest`, the grouped ones of `V5CheckpointConsumerDagReplayTest` and
`V5CheckpointConsumerUntouchedSegmentTest`, and the managed ones of `ScalableCheckpointConsumerTest` and
`ScalableCheckpointConsumerReconcileTest`, check these rules on the simulated cluster
(`crates/apache-pulsar-testkit/tests/checkpoint_group.rs`), and integration tests on the real broker.

## 7. Namespace subscription (later)

A membership watch (*wire-protocol* §7) feeds the set of topics. Each topic gets its own Stream consumer on the
same loop, all feeding one receive queue, with a position vector spanning topics and segments. A topic leaving
the set is detached without any ack (*client-behavior* §6).

## 8. Migration bridge

A `persistent://` name resolves to a **synthetic layout** of legacy segments, each wrapping a classic topic:
one segment for a non-partitioned topic, one per partition for a partitioned one (*compatibility* §1).

- The per-segment producers and consumers attach to the wrapped `persistent://` names directly.
- While every active segment is legacy, keyed messages route mod-N (§3.1). So each key keeps its v4 partition,
  and v4 and v5 clients can run side by side during a migration.
- Each per-segment producer and consumer on a legacy segment carries `__pulsar.v5.managed=true`. The broker's
  migration refuses a topic while a client without it, a v4 one, is attached, unless forced.
- The 5.0.1 controller serves only migrated topics, so the Stream consumer and the members of a Checkpoint consumer
  group can't use a topic that isn't migrated yet: their create fails
  ([spec gap 28](../DESIGN.md#spec-gaps-to-raise-upstream)).
- A cutover to managed segments shows up as an ordinary layout change: legacy segments seal and managed
  successors appear, each a child of every legacy segment. §3.3 and §6.1 handle it like any split. The 5.0.1 broker
  gives the new layout the synthetic one's epoch, which the watch allows for (§2).

## 9. Transactions (later)

Coordinator discovery goes through the TC assignment watch when the broker supports it (*wire-protocol* §8),
and a transaction's partitions are the segment topics it wrote to. [2](2-architecture.md) §4.3 covers
transactions that span loops.

## 10. Open questions

1. **Position vector cost** with many segments, and across topics for namespace consumers: measure the plain copies
   (§5.2) at realistic segment counts, against a structurally shared map under an allowed license.
