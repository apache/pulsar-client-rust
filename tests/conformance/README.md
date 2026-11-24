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

# Conformance

The Scalable Topics Client Specification (*conformance*) defines what a conformant client is: the required core,
C1–C15 (§2), and the tests that validate interoperability (§5). This directory holds both for this client:

- **The suite** (§1–§3 below): the cases of *conformance* §5, described for any SDK, each with what this client runs
  for it. They need no Rust: another SDK can run the same cases against the same broker and data.
- **The checklist** (§4): each of C1–C15, and the tests that show this client meets it.

## 1. The environment

The cases that need a broker run against Apache Pulsar 5.0.1, the first release with scalable topics, from the
`apachepulsar/pulsar:5.0.1` image, in standalone mode. A harness provides:

- **The broker**, with automatic topic creation on, and the `public/default` namespace.
- **Admin access**, through `pulsar-admin` in the broker's container: `scalable-topics create`, `split-segment`,
  `merge-segments`, `get-metadata`, `stats` and `segment-stats`; `namespaces create` and `unload`. `pulsar-admin`
  5.0.1 has no command to turn a topic's automatic splits and merges off, which some cases need: they
  `POST {"enabled": false}` to `/admin/v2/scalable/<tenant>/<namespace>/<topic>/autoScalePolicy`.
- **The reference client**, Java's, version 5.0.1, from the broker's image, with two small programs
  ([`RoutingTool`](../../crates/apache-pulsar/tests/integration/RoutingTool.java),
  [`CheckpointTool`](../../crates/apache-pulsar/tests/integration/CheckpointTool.java)) for the cases that compare
  the SDK under test with it.
- **A way to kill the broker and start it again**: stopping its container with `SIGKILL` and starting it. A case that
  does it runs alone, once the others are done.

A case's topics are its own: it names them with a random suffix, so that cases run side by side and the suite runs
again on the same broker.

Some cases also use vectors that the Java client generated (`tools/java-vectors`), which need no broker:
[`routing.json`](../../crates/apache-pulsar-protocol/tests/vectors/routing.json),
[`message_id.json`](../../crates/apache-pulsar-protocol/tests/vectors/message_id.json) and
[`checkpoint.json`](../../crates/apache-pulsar-protocol/tests/vectors/checkpoint.json).

## 2. The cases

Each case has an id, the requirements it checks, what it does, and when it passes. *This client* names what runs it
here. "Every message" means every message the case sent; "in order" means, for each key, in the order the key's
messages were sent.

### Routing

**`routing-vectors`** (C2; no broker). For each key of `routing.json`: the raw and masked 32-bit Murmur3 hashes of
its UTF-8 bytes, the segment hash (the high 16 bits), the entry-bucket hash (the low 16 bits), and the partition a
v4 producer picks among N partitions, for each N listed. *Passes* when the SDK computes every one of them as listed.
*This client:* [`routing_vectors.rs`](../../crates/apache-pulsar-protocol/tests/routing_vectors.rs)
`routing_matches_java`.

**`routing-across-clients`** (C2). A topic of four segments, the second then split, so that the five active segments
cover ranges of two sizes. The SDK's producer and the reference client's each send one message for each of 128 keys
(`key-0` to `key-127`). *Passes* when, for every key, both messages went to the same segment, the one whose range
holds the key's segment hash in the layout `get-metadata` returns, and every segment got keys. A message's segment is
read from its id: the reference client prints it. *This client:* `conformance::routing_across_clients`.

**`legacy-routing`** (C2, the migration bridge). A partitioned `persistent://` topic of three partitions. The SDK's
producer sends keyed messages to it. *Passes* when each key's messages are in the partition a v4 producer picks
(`routing.json`'s partition for N = 3), read back with the reference client. *This client:*
`conformance::legacy_routing`.

### Layout changes during traffic

**`split-during-produce`** (C1, C3, C4). The SDK's producer sends 16 keys in rounds, with sends in flight, while the
topic's segment splits. *Passes* when no send fails, the topic's messages are stored in more than one segment, and the
reference client reads every key's messages in the order they were sent, the parent's before the children's. *This
client:* `conformance::split_during_produce`.

**`split-during-consume`** (C1, C8). A Queue consumer receives while the producer sends and the segment splits.
*Passes* when the consumer gets every message once, from more than one segment, and the drained parent keeps no
consumer of the subscription. *This client:* `conformance::split_during_consume`.

**`split-and-merge-under-load`** (C1, C3, C4, C7, C8). A topic of two segments, its automatic splits and merges off.
A producer sends 16 keys in rounds without pause, with sends in flight, while segment 0 splits into segments 2 and 3,
and those merge into segment 4; a Queue and a Stream consumer receive meanwhile, and acknowledge what they receive.
The producer goes on for 20 rounds after the merge. *Passes* when no send fails, the Queue consumer receives every
message once, from segments 0 to 4, and the Stream consumer receives every message once, in order. *This client:*
`conformance::split_and_merge_under_load`.

### Failover

Both cases run the same clients: a producer whose sends never time out, a Queue consumer, a Stream consumer and a
Checkpoint consumer, from the earliest position, on a topic of two segments in a namespace of the case's own. The
producer sends a message every 50 ms, over 5 keys. Once 20 are sent and a send to each segment has completed, the topic
fails over; the producer sends 20 more once it is back. The consumers receive meanwhile, the Queue and Stream consumers
acknowledging. A message may come twice after a failover; what counts is the first time each comes. *Passes* when no
send fails, and each consumer receives every message, the Stream and the Checkpoint consumers each key's in order.

**`failover-by-unload`** (C14, C7, C8, C9). Once the topic's stats list the producer on both segments, the namespace is
unloaded (`namespaces unload`): the broker closes the topic's producers and consumers and gives the topic up, as when
its ownership moves to another broker. *Passes* as above, and the topic's stats show the producer registered again on
both segments, each with a new `connectedSince`.
*This client:* `conformance::failover_by_unload`.

**`failover-by-broker-restart`** (C14, C7, C8, C9). The broker is killed, every connection dropping, and started
again. It runs alone. *Passes* as above. *This client:* `conformance::failover_by_broker_restart`.

### Cumulative acknowledgment

**`cumulative-ack-across-segments`** (C7). A Stream consumer of a topic of two segments, its automatic splits and
merges off. The keys are chosen by their hash, so that the messages go to the two segments in turn.

1. 20 messages, each received before the next is sent, so that they come from the two segments in turn. The consumer
   acknowledges cumulatively the tenth. *Passes* when the subscription's backlog over the topic is 10, and a consumer
   of the subscription created next receives exactly the last 10, and no other. It acknowledges them all.
2. 10 more messages, received in turn as above and not acknowledged. Segment 0 splits; 10 messages go to segment 1 and
   to segment 0's children in turn. The children's messages wait for their parent to be drained, so only segment 1's
   come. The consumer acknowledges cumulatively the last of them. *Passes* when the children's messages then come, and
   the acknowledgment of the last one leaves no backlog.

*This client:* `conformance::cumulative_ack_across_segments`.

### Message ids and checkpoints

**`id-and-checkpoint-vectors`** (C10; no broker). The SDK decodes each message id of `message_id.json` and each
checkpoint of `checkpoint.json`, and encodes them again. *Passes* when the decoded values are those listed, and the
encodings are the listed bytes (compared after decoding where `byte_exact` is false).
*This client:* [`message_id_vectors.rs`](../../crates/apache-pulsar-protocol/tests/message_id_vectors.rs),
[`checkpoint_vectors.rs`](../../crates/apache-pulsar-protocol/tests/checkpoint_vectors.rs).

**`checkpoint-round-trip-across-layout-changes`** (C9, C10). A topic of one segment, and keyed messages before each
step. The reference client's Checkpoint consumer reads part of the topic from the earliest position and takes a
checkpoint; the segment splits; the SDK's Checkpoint consumer starts from that checkpoint, reads part of what is
left, and takes its own; the two children merge; the reference client starts from the SDK's checkpoint and reads the
rest. *Passes* when, between them, they read every message once, every key in order. *This client:*
`conformance::checkpoint_round_trip_across_layout_changes`.

## 3. Running the suite here

The cases against a broker are trials of the integration tests, named `conformance::<id>` with the id's dashes as
underscores. The harness starts the broker, a proxy and the reference client in Docker:

```bash
cargo test -p apache-pulsar --test integration conformance::
```

The cases with vectors are the protocol crate's tests:

```bash
cargo test -p apache-pulsar-protocol --test routing_vectors --test message_id_vectors --test checkpoint_vectors
```

The Stream consumers go through the proxy: the broker names the controller by an address of its internal listener,
which the host can't reach.

## 4. The checklist

Each requirement of *conformance* §2, and the tests that show this client meets it, by layer
([docs/8-quality.md](../../docs/8-quality.md) §2): *vectors* and *unit* tests of the state machines
(`apache-pulsar-protocol`), the *simulated cluster* (`apache-pulsar-testkit`), the *driver*, the client over loopback
sockets (`apache-pulsar`'s own tests), and the *broker*, a 5.0.1 one (the integration tests, the suite's cases among
them). Each list names the tests that check the requirement most directly.

**C1. Resolve a topic and track its layout through the DAG watch, applying updates by epoch.**
- Unit: [`layout.rs`][p-layout] `a_greater_epoch_or_a_migration_supersedes`, `errors`; [`watch.rs`][d-watch]
  `a_malformed_layout_is_refused`, `a_migration_replaces_the_synthetic_layout`.
- Simulated cluster: [`lookup.rs`][tk-lookup] (direct answers, redirects, proxies); [`producer.rs`][tk-producer]
  `a_migration_made_while_disconnected_is_followed`; [`queue_consumer.rs`][tk-queue]
  `follows_splits_and_merges_without_losing_a_message`.
- Driver: [`producer_tests.rs`][d-producer] `a_repeated_layout_is_ignored`,
  `a_watch_reopens_its_session_through_failed_lookups`, `a_watch_outlives_an_error_on_its_session`,
  `a_layout_that_never_comes_times_create_out`.
- Broker: `conformance::split_during_produce`, `conformance::split_and_merge_under_load`.

**C2. Routing: a key by the high 16 bits of its Murmur3 hash to the active segment covering it; keyless round robin;
an all-legacy layout by v4's mod N.**
- Vectors: [`routing_vectors.rs`][p-routing] `routing_matches_java` (`routing-vectors`).
- Unit: [`layout.rs`][p-layout] `routes_keys_by_the_high_half`, `keyless_goes_round_robin_in_range_order`,
  `legacy_layout_routes_like_v4_partitions`.
- Simulated cluster: [`producer.rs`][tk-producer] `routes_keys_by_hash_and_keyless_round_robin`,
  `a_legacy_topic_routes_like_v4_partitions`, `sends_a_stamped_batch_of_one_per_message` (segment producers created
  lazily); [`producer_edges.rs`][tk-producer-edges] `a_key_no_segment_covers_fails_its_send`.
- Broker: `conformance::routing_across_clients`, `conformance::legacy_routing`.

**C3. Per-key publish order, across splits and merges; asynchronous sends in call order.**
- Simulated cluster: [`producer.rs`][tk-producer] `per_key_order_holds_across_a_split_with_traffic`,
  `per_key_order_holds_across_a_merge_with_traffic`, `per_key_order_holds_across_consecutive_splits`,
  `a_split_resends_in_flight_messages_before_newer_ones`, `only_the_keys_of_a_retiring_segment_are_held`;
  [`producer_batching.rs`][tk-batching] `a_split_routes_the_open_batches_again_in_order`.
- Driver: [`producer_tests.rs`][d-producer] `keys_keep_their_order_across_a_split_during_traffic`,
  `pipelining_combinators_keep_send_order`.
- Broker: `conformance::split_during_produce`, `conformance::split_and_merge_under_load`;
  [`bridge.rs`][it-bridge] `a_producer_keeps_key_order_across_a_migration`.

**C4. Transparent segment-gone retry when producing.**
- Simulated cluster: [`producer.rs`][tk-producer] `a_send_refused_by_a_terminated_topic_waits_for_the_layout`,
  `a_gone_segment_gets_no_new_producer_and_its_sends_fail_in_the_end`; [`producer_edges.rs`][tk-producer-edges]
  `a_message_is_parked_at_most_ten_times`, `a_gone_segment_retires_again_for_new_messages`,
  `a_closing_segment_that_turns_out_terminated`, `a_dropped_segment_that_disconnects_parks_its_messages`.
- Broker: `conformance::split_during_produce`, `conformance::split_and_merge_under_load` (sends in flight as segments
  seal; none fails).

**C5. Deduplication by monotonic sequence ids; the last published id exposed.**
- Simulated cluster: [`producer.rs`][tk-producer]
  `reconnecting_writes_unacknowledged_messages_again_with_their_sequence_ids`,
  `sequence_ids_start_after_the_initial_or_the_brokers_last`, `an_application_sequence_id_is_kept`,
  `a_duplicate_is_acknowledged_but_not_stored`; [`producer_batching.rs`][tk-batching]
  `a_sequence_id_that_may_be_a_duplicate_goes_alone`.
- Driver: [`producer_tests.rs`][d-producer] `local_producer_sends_flushes_and_closes`,
  `every_message_field_reaches_the_wire` (`last_sequence_id`).
- Broker: [`main.rs`][it-main] `batches_pass_the_brokers_deduplication`.

**C6. The access modes: shared, exclusive, exclusive with fencing, wait for exclusive; exclusive ones claim every
segment at create.**
- Simulated cluster: [`producer.rs`][tk-producer] `exclusive_claims_every_segment_at_create`,
  `a_failed_exclusive_create_releases_what_it_claimed`, `exclusive_with_fencing_fences_the_older_producer`,
  `wait_for_exclusive_is_created_once_the_holder_leaves`, `a_waiting_producer_that_reconnects_waits_again`,
  `a_shared_producer_is_refused_while_an_exclusive_one_holds_the_topic`; [`producer_edges.rs`][tk-producer-edges]
  `exclusive_producers_claim_new_segments_from_a_layout`, `an_exclusive_create_follows_a_split_that_lands_meanwhile`.
- Driver: [`producer_tests.rs`][d-producer] `exclusive_conflicts_fail_create`, `exclusive_with_fencing_takes_the_topic`,
  `wait_for_exclusive_outlasts_the_operation_timeout`.
- Broker: [`main.rs`][it-main] `exclusive_producers_conflict_at_create`,
  `exclusive_with_fencing_fences_the_older_producer`.

**C7. The Stream consumer: per-key order; a cumulative ack advancing every segment by its position vector; the
broker's cursor; the controller's assignments, rebalances, reconnects and grace period.**
- Unit: [`stream_consumer.rs`][p-stream] (the assignment session: registration, modes, drains, reconnects, close);
  [`consumer.rs`][p-consumer] `cumulative_acks_go_out_once_a_group`,
  `a_key_shared_consumer_acknowledges_in_the_order_given`.
- Simulated cluster: [`stream_consumer.rs`][tk-stream] `a_cumulative_ack_covers_every_segment`,
  `an_ack_covers_what_was_given_before_it`, `an_ack_leaves_what_was_given_after_it`,
  `keys_keep_their_order_across_two_splits`, `a_split_while_consuming_keeps_each_key_in_order`,
  `members_beyond_the_segments_share_them_by_bucket`, `within_the_grace_period_the_assignment_holds`,
  `past_the_grace_period_the_group_rebalances`, `a_member_comes_back_after_a_connection_loss`.
- Driver: [`stream_tests.rs`][d-stream].
- Broker: `conformance::cumulative_ack_across_segments`, `conformance::split_and_merge_under_load`, the failover cases;
  [`main.rs`][it-main] `a_stream_consumer_reads_sealed_backlog_first_in_key_order`,
  `stream_consumers_share_a_segment_by_bucket`, `a_closed_stream_consumer_hands_its_segments_over_at_once`.

**C8. The Queue consumer: parallel delivery; individual and negative acks; every active and sealed segment; acks
routed by segment.**
- Unit: [`consumer.rs`][p-consumer] `acks_go_out_together`, `negative_acks_ask_for_their_entries_again`.
- Simulated cluster: [`queue_consumer.rs`][tk-queue] `receives_from_every_segment_and_acknowledges_on_each`,
  `a_consumer_subscribing_again_gets_only_what_was_not_acknowledged`,
  `follows_splits_and_merges_without_losing_a_message`,
  `a_new_earliest_subscription_replays_the_sealed_parent_and_the_children`,
  `a_negative_ack_brings_the_message_back_after_the_delay`, `acks_of_segments_it_does_not_have_are_ignored`.
- Driver: [`consumer_tests.rs`][d-consumer].
- Broker: `conformance::split_during_consume`, `conformance::split_and_merge_under_load`; [`main.rs`][it-main]
  `a_queue_consumer_reads_and_acknowledges_every_segment`, `queue_consumers_share_a_subscription`,
  `a_negative_ack_brings_a_message_back`, `batch_index_acks_settle_whole_batches`.

**C9. The Checkpoint consumer: a position held by the application; an atomic checkpoint across segments, captured and
restored; ungrouped and grouped.**
- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint], [`checkpoint_group.rs`][tk-group].
- Driver: [`checkpoint_tests.rs`][d-checkpoint].
- Broker: `conformance::checkpoint_round_trip_across_layout_changes`; [`main.rs`][it-main]
  `a_checkpoint_consumer_resumes_across_a_split`, `a_latest_checkpoint_consumer_reads_a_later_split`,
  `checkpoint_group_members_share_the_segments`, `a_checkpoint_group_member_reads_the_sealed_backlog`.

**C10. Opaque, serializable, comparable message ids, and opaque, serializable checkpoints that later versions
restore.**
- Vectors: [`message_id_vectors.rs`][p-ids], [`checkpoint_vectors.rs`][p-checkpoints] (`id-and-checkpoint-vectors`):
  the bytes of the Java client, which every later release must read ([docs/8-quality.md](../../docs/8-quality.md)
  §3).
- Unit: [`message_id.rs`][d-message-id] `round_trips_and_orders`; [`checkpoint.rs`][d-checkpoint-type]
  `round_trips_and_prints`. `MessageId` and `Checkpoint` have no public fields.
- Broker: `conformance::checkpoint_round_trip_across_layout_changes`.

**C11. A topic-level schema; the broker's compatibility check surfaced as a typed error.**
- Driver: [`schema_tests.rs`][d-schema] `a_topic_has_one_schema_and_its_messages_carry_the_version`,
  `a_schema_of_another_type_is_refused`, `a_topic_with_data_and_no_schema_refuses_a_typed_consumer`.
- Broker: [`schemas.rs`][it-schemas] `a_schema_of_another_type_is_refused`,
  `the_first_consumer_of_a_new_topic_registers_its_schema`, and Java ⇄ Rust for each schema.

**C12. Auto-creation honored; a missing topic surfaced as `NotFound`.**
- Driver: [`producer_tests.rs`][d-producer] `a_missing_topic_is_not_found`.
- Broker: [`main.rs`][it-main] `a_missing_topic_is_not_found_without_auto_creation`; most broker tests use topics they
  never create, which the broker creates.

**C13. Typed error categories; asynchronous operations fail through their futures; transient failures retried
inside.**
- Unit and simulated cluster: [`connection.rs`][tk-connection] `which_failures_are_worth_reconnecting_for`,
  `refused_connection_is_not_retryable_for_auth_errors`; [`producer.rs`][tk-producer]
  `the_send_timeout_fails_late_messages_and_the_producer_carries_on`, `a_forbidden_message_fails_alone`.
- Driver: [`errors.rs`][d-errors]; [`task.rs`][d-producer-task] `send_failures_map_to_kinds`;
  [`consumer/task.rs`][d-consumer-task] `subscribe_failures_map_to_kinds`; [`producer_tests.rs`][d-producer]
  `invalid_configuration_fails_create`; [`consumer_tests.rs`][d-consumer] `invalid_configuration_fails_subscribe`.
  Every operation that can fail returns a `Result`, through its future when it is asynchronous.
- Broker: [`main.rs`][it-main] `missing_credentials_are_refused` (`Authentication`),
  `exclusive_producers_conflict_at_create`; [`schemas.rs`][it-schemas] `a_schema_of_another_type_is_refused`
  (`IncompatibleSchema`).

**C14. Long-lived producers and consumers that heal themselves; a graceful, idempotent close; thread safety.**
- Simulated cluster: [`queue_consumer.rs`][tk-queue] `keeps_consuming_and_following_the_layout_after_a_connection_loss`;
  [`consumer.rs`][tk-consumer] `an_unload_closes_the_consumer_and_it_comes_back`; [`stream_consumer.rs`][tk-stream]
  `a_member_comes_back_after_a_connection_loss`.
- Driver: [`tests.rs`][d-tests] `reconnects_after_the_broker_drops_the_connection`,
  `request_in_flight_when_the_connection_drops_is_retried`, `client_is_send_and_sync`,
  `thread_safe_client_runs_on_its_loops_from_any_executor`; [`producer_tests.rs`][d-producer]
  `a_lost_connection_writes_unacknowledged_messages_again`, `close_completes_the_sends_the_broker_has`,
  `local_producer_sends_flushes_and_closes` (closed twice), `thread_safe_producers_spread_over_the_loops`;
  [`consumer_tests.rs`][d-consumer] `a_lost_connection_brings_back_what_was_not_acknowledged`,
  `a_consumer_the_broker_closes_subscribes_again`, `closing_the_client_closes_its_consumers_and_fails_their_receives`;
  [`stream_tests.rs`][d-stream] `a_lost_connection_registers_again`; [`checkpoint_tests.rs`][d-checkpoint]
  `a_lost_connection_resumes_after_what_was_received`.
- Broker: `conformance::failover_by_unload`, `conformance::failover_by_broker_restart`.

**C15. No scalable-topic command to a broker that doesn't advertise them.**
- Unit and simulated cluster: [`connection.rs`][tk-connection] `scalable_commands_need_the_broker_feature`,
  `connect_advertises_the_features_the_client_honors`.
- Driver: [`producer_tests.rs`][d-producer] `a_broker_without_scalable_topics_is_refused`
  (`UnsupportedBroker`); [`driver.rs`][d-driver] `a_request_the_broker_does_not_support_fails_at_once`.

[p-layout]: ../../crates/apache-pulsar-protocol/tests/layout.rs
[p-routing]: ../../crates/apache-pulsar-protocol/tests/routing_vectors.rs
[p-stream]: ../../crates/apache-pulsar-protocol/tests/stream_consumer.rs
[p-consumer]: ../../crates/apache-pulsar-protocol/tests/consumer.rs
[p-ids]: ../../crates/apache-pulsar-protocol/tests/message_id_vectors.rs
[p-checkpoints]: ../../crates/apache-pulsar-protocol/tests/checkpoint_vectors.rs
[tk-lookup]: ../../crates/apache-pulsar-testkit/tests/lookup.rs
[tk-connection]: ../../crates/apache-pulsar-testkit/tests/connection.rs
[tk-producer]: ../../crates/apache-pulsar-testkit/tests/producer.rs
[tk-producer-edges]: ../../crates/apache-pulsar-testkit/tests/producer_edges.rs
[tk-batching]: ../../crates/apache-pulsar-testkit/tests/producer_batching.rs
[tk-consumer]: ../../crates/apache-pulsar-testkit/tests/consumer.rs
[tk-queue]: ../../crates/apache-pulsar-testkit/tests/queue_consumer.rs
[tk-stream]: ../../crates/apache-pulsar-testkit/tests/stream_consumer.rs
[tk-checkpoint]: ../../crates/apache-pulsar-testkit/tests/checkpoint_consumer.rs
[tk-group]: ../../crates/apache-pulsar-testkit/tests/checkpoint_group.rs
[d-watch]: ../../crates/apache-pulsar/src/watch.rs
[d-tests]: ../../crates/apache-pulsar/src/tests.rs
[d-driver]: ../../crates/apache-pulsar/src/tests/driver.rs
[d-errors]: ../../crates/apache-pulsar/src/tests/errors.rs
[d-producer]: ../../crates/apache-pulsar/src/producer_tests.rs
[d-producer-task]: ../../crates/apache-pulsar/src/producer/task.rs
[d-consumer]: ../../crates/apache-pulsar/src/consumer_tests.rs
[d-consumer-task]: ../../crates/apache-pulsar/src/consumer/task.rs
[d-stream]: ../../crates/apache-pulsar/src/stream_tests.rs
[d-checkpoint]: ../../crates/apache-pulsar/src/checkpoint_tests.rs
[d-checkpoint-type]: ../../crates/apache-pulsar/src/checkpoint.rs
[d-message-id]: ../../crates/apache-pulsar/src/message_id.rs
[d-schema]: ../../crates/apache-pulsar/src/schema_tests.rs
[it-main]: ../../crates/apache-pulsar/tests/integration/main.rs
[it-bridge]: ../../crates/apache-pulsar/tests/integration/bridge.rs
[it-schemas]: ../../crates/apache-pulsar/tests/integration/schemas.rs
