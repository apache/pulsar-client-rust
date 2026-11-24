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

# Porting the Java tests

The Java V5 client's tests are the reference for this client's: their scenarios are ported here, not their code
([8](../docs/8-quality.md) §2.1). This file lists each Java test class that uses the V5 API, with the tests here that
cover its scenarios, and says why the rest have none.

The classes are those of [apache/pulsar](https://github.com/apache/pulsar) at `a0e7f7de62`: the 22 test classes of
`pulsar-client-v5`, and the 128 elsewhere that import `org.apache.pulsar.client.api.v5`: 150 classes, 694 tests. Java's
tests added since are not mapped yet.

## Statuses

A Java test is *covered* when a test here checks the same client behaviour under the same conditions and the same
outcome, at any layer: a unit test of a state machine, a scenario on the simulated cluster, the driver over loopback
sockets, or the client against a 5.0.1 broker ([8](../docs/8-quality.md) §2). The others:

- *partly covered*: a test here checks part of it; the entry says what isn't, and why.
- *not ported*: in scope, with no test here; the entry says why.
- *Java-only*: it tests the Java client's internals: executors, Java builder and configuration plumbing, v4 adapters,
  class loading, Java exception classes.
- *the broker's*: it tests what the broker or the admin does, the client only driving it; where the client sees an
  outcome, the entry says where that is tested.
- *needing several brokers*: the integration tests run one broker; the entry says where the client's side is tested.
- *out of scope*: Pulsar Functions, the proxy and SASL ([8](../docs/8-quality.md) §2.1), and the features that come
  after the first release ([1](../docs/1-scope.md)).

| Tests | |
|---|---:|
| Covered | 303 |
| Partly covered | 2 |
| Not ported | 10 |
| Java-only | 104 |
| The broker's | 80 |
| Needing several brokers | 18 |
| Out of scope | 177 |
| **All** | **694** |

A class is *ported* when each of its tests in scope is covered, and *partly ported* when one is only partly covered or
not ported. Of the 150 classes, 71 are ported, 5 partly ported and 3 not ported; of the others, whose tests are out of
scope or have no counterpart here, 12 are Java-only, 15 the broker's, 4 need several brokers, 35 are out of scope, and 5
have no tests.

## In scope, not ported

The tests in scope that no test here covers, or covers only in part:

- `PulsarClientBuilderV5Test.testProxyServiceUrlIsValidatedToo`: the client has no proxy URL option: SNI proxy routing
  is an open question ([4](../docs/4-connections.md) §7).
- `SchemaFactoryTest.testSchemaInfoOfRoundTrips`: Rust builds a `SchemaInfo` only from a schema or the broker's answer;
  a public constructor would serve `Schema::generic(SchemaInfo)`, which the spec's standard set doesn't name and PLAN.md
  doesn't plan yet.
- `SchemaFactoryTest.testGenericSchemaFromAvroDefinition`: the client has no `Schema::generic(SchemaInfo)`, which the
  spec's standard set doesn't name and PLAN.md doesn't plan yet.
- `SchemaFactoryTest.testAutoProduceBytesOfWrapsBase`: the client has no auto-produce bytes schema, which the spec's
  standard set doesn't name and PLAN.md doesn't plan yet.
- `AuthMetricsTest`, 3 tests (`recordsCredentialDurationOnSuccessAndNoFailureCounter`, `recordsTerminalFailure`,
  `recordsTransientFailure`): the client records no metrics yet.
- `ClientCnxAsyncAuthTest.connectWriteFailureFailsConnectionFutureWithWriteCause`: nothing makes a write fail on a
  loopback socket.
- `V5EntryBucketDispatchTest.testRapidSubscribeCloseChurnLeavesGroupClean` (partly covered): the client's side is
  covered: a close while registering, and clean leaves. Repeated churn, and a rejoin under the same name racing a leave,
  test how the controller converges and tells sessions apart, which is the broker's.
- `V5MultiBrokerControllerFailoverTest.testStreamConsumerFollowsControllerLeaderShutdown`: with a producer of the topic
  on the same client, as here, a Stream consumer registers again with the old controller, a bug that
  [`stream_tests.rs`][d-stream] `follows_the_controller_while_a_producer_holds_the_layout_watch` pins, ignored until it
  is fixed. Without the producer, the consumer follows the controller ([`stream_tests.rs`][d-stream]
  `follows_the_controller_the_layout_names_across_brokers`).
- `V5ProcessingTimeoutTest.testUnackedMessageIsRedeliveredAfterProcessingTimeout`: the client has no processing (ack)
  timeout: [5](../docs/5-segment-engine.md) §6 leaves it open.
- `V5TlsProducerConsumerTest.testV5MtlsViaTlsPolicy` (partly covered): the mTLS handshake is covered, with the
  certificate from `TlsAuthentication`; the client has no TLS option for a certificate apart from authentication.

## `pulsar-client-v5`

The Java client's own tests, mostly unit tests of its internals with mocks. Their scenarios map to unit and
simulated-cluster tests here, and to the driver's.

### `CheckpointV5Test`

Ported. 9 tests: 7 covered and 2 Java-only. `CheckpointV5` bytes: positions and sentinels round-trip, malformed bytes
are refused. The protocol vectors port it; null input and Java collection immutability have no Rust counterpart.

- Unit: [`checkpoint_vectors.rs`][p-checkpoints] `round_trips_positions_and_sentinels`, `encodes_like_java`,
  `decodes_every_java_checkpoint`, `rejects_malformed_bytes`; [`checkpoint.rs`][d-checkpoint-type]
  `round_trips_and_prints`.
- Java-only:
  - `testSegmentPositionsIsImmutable`: Java's unmodifiable map; the public Rust `Checkpoint` is opaque and hands out no
    positions.
  - `testFromByteArrayRejectsNull`: a `&[u8]` can't be null.

### `ClientSegmentLayoutTest`

Ported. 8 tests: 6 covered and 2 Java-only. How a DAG becomes the client's layout: epoch, active segments in hash order,
segment names, lineage, broker and controller URLs. The protocol layout tests cover it, the controller's plain and TLS
URLs included; the consumers that read parents first cover the lineage.

- Unit: [`layout.rs`][p-layout] `names_segments_like_java`, `errors`, `keyless_goes_round_robin_in_range_order`,
  `gives_the_controller_url_for_plain_or_tls_connections`.
- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `a_child_waits_until_its_parent_is_read_to_its_end`,
  `keeps_per_key_order_across_a_merge_while_reading`.
- Driver: [`stream_tests.rs`][d-stream] `receives_in_order_and_acknowledges_cumulatively`,
  `finds_the_controller_the_layout_names_or_any_broker_behind_a_proxy`,
  `follows_the_controller_the_layout_names_across_brokers`.
- Java-only:
  - `testReturnedCollectionsAreImmutable`: Java's unmodifiable collections; a Rust `Layout` is immutable, shared behind
    `Rc`, and hands out iterators and shared references.
  - `testSegmentBrokerUrlsCollected`: Java collects them and reads none; Rust doesn't keep them, each segment going
    through a standard lookup (DESIGN gap 3).

### `CredentialOffloadThroughV5BuilderTest`

Java-only. 3 tests: 3 Java-only. PIP-478's bridge for v4 credential plugins: whether a wrapped v4 plugin keeps its
offload to a blocking executor or runs raw. Rust has no v4 plugins, and its `Authentication` providers return futures
(`crates/apache-pulsar/src/auth.rs`), so a provider keeps its own blocking work off the loop.

- Java-only:
  - `credentialV4PluginStaysWrappedAndOffloadsGetAuthData`: v4 plugin adapter and executor offload.
  - `tlsOnlyV4PluginRunsRawWithoutOffloadWrapper`: v4 adapter resolution.
  - `combinedTlsAndCredentialV4WithoutTlsPolicyRunsRawToPreserveTls`: v4 adapter resolution; one Rust provider can give
    both (`client_certificate`, `auth_data`).

### `DagWatchClientTest`

Ported. 7 tests: 5 covered and 2 Java-only. The layout watch: reconnecting after a disconnect but not while the client
closes, handing layouts to the producer or consumer that opened it, and failing its start with the lookup's category.
The driver's watch tests and the state machines' "layouts that come meanwhile" tests cover it; Java's listener threading
has no counterpart.

- Simulated cluster: [`queue_consumer.rs`][tk-queue]
  `the_subscribe_waits_for_the_segments_of_layouts_that_come_meanwhile`; [`checkpoint_consumer.rs`][tk-checkpoint]
  `the_create_waits_for_the_readers_of_layouts_that_come_meanwhile`; [`producer_edges.rs`][tk-producer-edges]
  `an_exclusive_create_follows_a_split_that_lands_meanwhile`.
- Driver: [`producer_tests.rs`][d-producer] `a_watch_reopens_its_session_through_failed_lookups`,
  `a_held_watch_stops_with_its_client`, `a_repeated_layout_is_ignored`, `a_watch_outlives_an_error_on_its_session`,
  `a_missing_topic_is_not_found`; [`errors.rs`][d-errors] `broker_error_codes_map_to_kinds`.
- Java-only:
  - `testLayoutArrivingWhileListenerRunsIsDeliveredAfterIt`: Java's I/O thread versus the listener's thread; a Rust
    watch and its holders share one loop.
  - `testListenerErrorDoesNotStopLaterLayouts`: listener callbacks; Rust has none.

### `MessageIdV5Test`

Ported. 18 tests: 13 covered and 5 Java-only. `MessageIdV5`: identity by segment and position, the position vector left
out, order, bytes with their vectors, sentinels, printing. Java's golden vectors cover the bytes.

- Unit: [`message_id_vectors.rs`][p-ids] `ids_hash_and_order_as_they_compare`, `sentinels_and_order`,
  `encodes_like_java`, `decodes_every_java_id`, `rejects_malformed_bytes`; [`message_id.rs`][d-message-id]
  `round_trips_and_orders`.
- Java-only:
  - `testConstructorRejectsNullV4MessageId`, `testFromByteArrayRejectsNull`: no null in Rust.
  - `testConstructorWithoutPositionVectorUsesEmptyMap`: a constructor overload; the protocol's `MessageId::new` takes no
    vector and starts empty, as a decoded id without one is ([`message_id_vectors.rs`][p-ids]
    `decodes_ids_without_the_trailing_sections`).
  - `testPositionVectorIsImmutable`: Java's defensive copy; Rust owns its map, and the public `MessageId` is opaque.
  - `testCompareToRejectsForeignType`: Java's interface admits other implementations; Rust's `MessageId` is one type.

### `PulsarClientBuilderV5Test`

Partly ported. 34 tests: 11 covered, 1 not ported, 20 Java-only and 2 out of scope. The V5 client builder: service-URL
checks, the connection policy, and PIP-478's TLS policies, TLS factories and plugins by class name. Rust has the URL
checks and TLS options, tested by their effect on a handshake; factories, keystores, class loading and the v4
configuration mapping are Java's, and Rust has no proxy URL.

- Simulated cluster: [`lookup.rs`][tk-lookup] `service_urls`, `ipv6_hosts`.
- Driver: [`options.rs`][d-options] `the_builder_refuses_incomplete_or_contradictory_options`,
  `bad_trust_files_are_refused`, `mutual_tls_presents_the_client_certificate`,
  `tls_1_2_brokers_with_relaxed_verification`; [`tests.rs`][d-tests] `tls_verification_options`,
  `tls_with_a_trusted_ca`; [`errors.rs`][d-errors] `request_lookup_and_url_failures_map_to_kinds`.
- Not ported: `testProxyServiceUrlIsValidatedToo`: the client has no proxy URL option: SNI proxy routing is an open
  question ([4](../docs/4-connections.md) §7).
- Java-only:
  - `connectionBackoffInitialAndMaxAreApplied`: asserts v4 `ClientConfigurationData` only; Rust's reconnect backoff,
    exponential with jitter (docs/4-connections.md §4), has no option.
  - `connectionBackoffRejectsMultiplierAndJitterTheClientCannotApply`: what v4's configuration can't carry.
  - `genericV4TlsFilePluginMaterialIsFoldedIntoClientDefault`: the v4 plugin fold; a Rust provider hands its PEM over
    through `client_certificate`, and a bad one fails the build ([`options.rs`][d-options]
    `bad_client_certificates_are_refused`).
  - `genericV4InMemoryOnlyTlsMaterialDoesNotFailBuild`: the v4 plugin fold.
  - `testTlsFactoryDoesNotEnableTransportTls`: the `PulsarTlsFactory` SPI; in Rust only the URL's scheme picks TLS.
  - `testTlsFactoryLeavesAnSslUrlEnabled`: the TLS factory SPI; the scheme's TLS flag is checked in
    [`lookup.rs`][tk-lookup] `service_urls`.
  - `twoClientsFromOneBuilderDoNotShareATlsFactory`: TLS factory lifecycle; Rust's `build` consumes its builder.
  - `anAdoptedTlsFactoryIsHandedToOneClientOnly`, `aBuildThatReachedTheFactorySpendsItAndOneThatDidNotDoesNot`,
    `anAdoptedFactoryIsClaimedByOneBuildEvenUnderConcurrentBuilds`, `anAdoptedFactoryWhoseInitializeFailsIsStillClosed`,
    `aFailedBuildLeavesTheAdoptedFactoryUsable`: TLS factory SPI.
  - `aFactoryThatFailsInitializeAndCloseWithOneExceptionKeepsTheRealFailure`: Java exception suppression.
  - `aSecondClientDoesNotInheritTheFirstPluginsCertificate`: builder reuse and the v4 plugin fold; Rust's `build`
    consumes its builder.
  - `testAuthenticationPluginAndParamsInstantiatesAuthentication`: plugins by class name.
  - `testAuthenticationPluginNotFoundIsWrapped`: class loading.
  - `testCrossFormatFoldPemTrustWithKeyStoreIdentityFailsLoud`: keystore formats and the v4 fold; Rust reads PEM only.
  - `testCrossFormatFoldKeyStoreTrustWithPemIdentityFailsLoud`: keystore formats and the v4 fold.
  - `testSameFormatFoldPreservesKeyStoreTrust`: JKS and PKCS12 keystores; Rust reads PEM.
  - `testFoldPreservesJsseProvider`: JSSE providers.
- Out of scope: `anIdpOnlyPolicyDoesNotEnableBrokerTls`, `anIdpOnlyPolicySurvivesTheClientBuild`: OAuth2.

### `ScalableCheckpointConsumerReconcileTest`

Ported. 3 tests: 3 covered. The Checkpoint consumer's readers kept in step with layouts and assignments: a failed attach
retried, one retry at a time, a segment moved away closed while another reader attaches. Ported on the simulated
cluster, grouped and ungrouped.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `attaches_again_a_reader_that_failed_to_subscribe`,
  `a_reader_that_keeps_failing_gets_one_retry_at_a_time`, `a_new_layout_attaches_at_once_the_readers_that_gave_up`,
  `lets_go_of_a_segment_the_layout_drops_while_another_reader_subscribes`; [`checkpoint_group.rs`][tk-group]
  `closes_a_segment_moved_away_while_another_reader_subscribes`.

### `ScalableCheckpointConsumerTest`

Ported. 5 tests: 2 covered and 3 Java-only. Resolving a latest start: Java's separate lookup reader, closed before the
lookup completes, and late answers after the assignment changed. Rust asks the last message id on each reader's own
subscription (DESIGN gap 24), so the lookup reader has no counterpart; the late answers are ported.

- Simulated cluster: [`checkpoint_group.rs`][tk-group] `a_late_latest_position_names_nothing_for_a_segment_moved_away`,
  `a_late_latest_position_reaches_no_later_reader`; [`checkpoint_consumer.rs`][tk-checkpoint]
  `a_late_last_message_id_names_nothing_for_a_segment_the_layout_dropped`.
- Java-only:
  - `testLookupCompletesOnceTheReaderIsClosed`: Java's extra lookup reader; Rust asks on the segment reader's own
    subscription (gap 24).
  - `testFailedLookupFailsOnceTheReaderIsClosed`: the same reader; Rust asks a failed one again
    ([`checkpoint_consumer.rs`][tk-checkpoint] `asks_again_for_a_last_message_id_the_broker_failed_to_give`).
  - `testFailedCloseKeepsTheLookupResult`: the same reader.

### `ScalableConsumerClientTest`

Ported. 3 tests: 1 covered and 2 Java-only. The assignment session handing assignments to its consumer's listener. In
Rust the consumer is the session, one state machine on one loop: the threading has no counterpart, an update during the
initial attach does.

- Simulated cluster: [`checkpoint_group.rs`][tk-group] `closes_a_segment_moved_away_while_another_reader_subscribes`.
- Java-only:
  - `testAssignmentArrivingWhileListenerRunsIsDeliveredAfterIt`: I/O thread versus caller thread.
  - `testListenerErrorDoesNotStopLaterDeliveries`: listener callbacks.

### `ScalableQueueConsumerReceiveLoopTest`

Ported. 7 tests: 2 covered and 5 Java-only. Java's per-segment receive loop, pulling from a v4 consumer into the shared
receive queue. Rust has no loop: the broker pushes up to the permits, and a sealed segment's consumer closes once
drained. Backpressure and the end of a segment map; re-arming and executor retries don't.

- Simulated cluster: [`consumer.rs`][tk-consumer] `permits_bound_what_the_broker_pushes`, `a_terminated_segment_drains`;
  [`queue_consumer.rs`][tk-queue] `lets_go_of_a_sealed_segment_once_drained_while_the_layout_still_has_it`.
- Java-only:
  - `closeWhilePausedDoesNotRearm`: loop re-arming; a Rust close closes each segment's consumer
    ([`queue_consumer.rs`][tk-queue] `close_sends_the_acks_then_closes_every_segment`).
  - `failedReadinessDoesNotRearm`: loop re-arming.
  - `transientReceiveErrorRetriesOnInternalExecutor`: v4 receive futures and executors.
  - `throwingSinkRetriesOnInternalExecutor`: sink exceptions and executors.
  - `rejectedRetryDoesNotMutateReceiveFailure`: executor rejection.

### `ScalableQueueConsumerReconcileTest`

Ported. 4 tests: 4 covered. The Queue consumer's segments kept in step with layouts: one retry at a time with backoff, a
new failure not waiting out an older backoff, new segments subscribed while another subscribe is pending, a dropped
segment closed once subscribed. Ported on the simulated cluster.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `retries_a_segment_that_keeps_failing_one_retry_at_a_time`,
  `retries_a_new_failure_without_waiting_out_an_earlier_backoff`,
  `subscribes_new_segments_while_another_subscribe_is_pending`,
  `closes_a_segment_that_left_the_layout_once_its_subscribe_completes`.

### `ScalableStreamConsumerReconcileTest`

Ported. 5 tests: 5 covered. The Stream consumer's reconcile against hand-made assignments: retries and their backoff,
and draining a segment moved away. Ported one for one in the protocol crate.

- Unit: [`stream_consumer.rs`][p-stream] `keeps_at_most_one_retry_waiting`,
  `a_new_failure_is_retried_without_the_earlier_backoff`, `a_retry_goes_on_while_a_segment_drains`,
  `an_assignment_applies_while_a_segment_drains`, `a_segment_comes_back_only_once_its_release_drained`.

### `ScalableTopicProducerOrderingTest`

Ported. 8 tests: 8 covered. Per-key order across layout changes with sends in flight, held and parked, and what closing
the client does to them. Ported on the simulated cluster and the driver.

- Simulated cluster: [`producer.rs`][tk-producer] `a_split_resends_in_flight_messages_before_newer_ones`,
  `only_the_keys_of_a_retiring_segment_are_held`, `per_key_order_holds_across_consecutive_splits`,
  `messages_moved_to_a_child_that_splits_reach_the_grandchild_in_order`,
  `close_fails_waiting_sends_and_awaits_written_ones`, `close_does_not_wait_for_a_pending_registration`;
  [`producer_edges.rs`][tk-producer-edges] `close_before_registration_fails_what_waits_for_it`.
- Driver: [`producer_tests.rs`][d-producer] `closing_the_client_closes_its_producers`, `a_send_after_close_fails`,
  `closing_the_client_waits_for_a_producer_already_closing`,
  `a_waiting_producer_leaves_the_line_when_its_client_closes`.

### `ScalableTopicProducerTest`

Ported. 4 tests: 3 covered and 1 out of scope. Which batching a segment's producer gets: entry-bucket batching whenever
batching is on, a single bucket included, and none with encryption or batching off. Rust stamps every entry; without
batching, each holds one message.

- Simulated cluster: [`producer_batching.rs`][tk-batching] `an_entry_holds_one_bucket_and_is_stamped_with_its_range`;
  [`producer.rs`][tk-producer] `sends_a_stamped_batch_of_one_per_message`.
- Driver: [`producer_tests.rs`][d-producer] `pipelined_sends_share_entries_unless_batching_is_off`.
- Out of scope: `testEncryptionDisablesBatching`: end-to-end encryption.

### `SchemaFactoryTest`

Partly ported. 6 tests: 1 covered, 3 not ported and 2 Java-only. V5 schema factories added for the CLI (`SchemaInfo.of`,
`Schema.generic`, `Schema.autoProduceBytesOf`) and conversions from v4. Rust has auto-consume and generic records;
`Schema::generic(SchemaInfo)` and auto-produce bytes aren't planned yet (PLAN.md, M7).

- Unit: [`schema/generic.rs`][d-generic] `generic_key_values`.
- Not ported:
  - `testSchemaInfoOfRoundTrips`: Rust builds a `SchemaInfo` only from a schema or the broker's answer; a public
    constructor would serve `Schema::generic(SchemaInfo)`, which the spec's standard set doesn't name and PLAN.md
    doesn't plan yet.
  - `testGenericSchemaFromAvroDefinition`: the client has no `Schema::generic(SchemaInfo)`, which the spec's standard
    set doesn't name and PLAN.md doesn't plan yet.
  - `testAutoProduceBytesOfWrapsBase`: the client has no auto-produce bytes schema, which the spec's standard set
    doesn't name and PLAN.md doesn't plan yet.
- Java-only:
  - `testAutoConsumeUnwrapsToV4AutoConsumeSchema`: the v4 adapter; Rust's auto-consume is tested in
    [`schema/generic.rs`][d-generic] `auto_consume_is_for_consumers_only`, [`schema_tests.rs`][d-schema]
    `a_generic_consumer_reads_any_topic_and_registers_no_schema`.
  - `testGenericRecordConversion`: v4 to V5 conversion; Rust gives a nested record in its format's own value
    (`GenericValue::Avro` with `Value::Record`, a JSON object).

### `SegmentRouterTest`

Ported. 14 tests: 14 covered. Routing: a key by the high 16 bits of its Murmur3 hash over hash ranges, keyless
round-robin, legacy layouts mod N, a legacy segment beside managed ones, and the hash's segment and bucket halves.
Ported with Java's vectors.

- Unit: [`layout.rs`][p-layout] `routes_keys_by_the_high_half`, `errors`, `keyless_goes_round_robin_in_range_order`,
  `legacy_layout_routes_like_v4_partitions`, `a_legacy_segment_beside_a_managed_one_routes_by_hash_range`;
  [`routing_vectors.rs`][p-routing] `routing_matches_java`; [`hash.rs`][p-hash] `reference_values`.
- Simulated cluster: [`producer.rs`][tk-producer] `routes_keys_by_hash_and_keyless_round_robin`,
  `sends_a_stamped_batch_of_one_per_message`, `a_legacy_topic_routes_like_v4_partitions`.

### `V5AuthenticationLoaderTest`

Java-only. 6 tests: 6 Java-only. Loading authentication plugins by class name with string or map parameters, bridging v4
plugins, and the built-in "none". Rust takes a provider value (the `Authentication` trait), with no class loading and no
v4 plugins.

- Java-only:
  - `v5NativePluginLoadedByNameIsConfiguredAndDrivesBinaryTransport`: class loading and params strings.
  - `v5NativePluginAcceptsJsonParams`: params strings.
  - `v5NativePluginLoadedByNameWithParamsMap`, `blankClassNameLoadsDisabledAuthentication`: class loading.
  - `v4PluginStillLoadsThroughLegacyAdapter`: the v4 adapter.
  - `theUnauthenticatedCaseCanDriveABinaryConnection`: the no-auth plugin's capability; a Rust client without a provider
    connects without one ([`tests.rs`][d-tests] `lookup_over_the_pool`).

### `V5ExceptionMappingsTest`

Ported. 4 tests: 2 covered and 2 Java-only. How v4 failures become V5 exceptions of the spec's error categories, keeping
message and cause. Rust maps broker codes and protocol failures to `ErrorKind` directly, each to its spec category. Java
differs twice: it keeps exceptions of its own, which the spec doesn't name, for failing to get authentication data and
for an unsupported method, which are `Authentication` here; and it makes a fenced producer ProducerBusy, where Rust
keeps a kind of its own (DESIGN.md gap 13, J5).

- Driver: [`errors.rs`][d-errors] `broker_error_codes_map_to_kinds`, `connection_failures_map_to_kinds`,
  `request_lookup_and_url_failures_map_to_kinds`, `errors_print_their_kind_and_message_and_keep_their_source`;
  [`producer/task.rs`][d-producer-task] `send_failures_map_to_kinds`; [`consumer/task.rs`][d-consumer-task]
  `subscribe_failures_map_to_kinds`.
- Java-only:
  - `testToV5ExceptionTypesASubtypeByItsClosestMappedSupertype`: Java's exception class hierarchy.
  - `testToV5ExceptionReturnsV5ExceptionsAsIs`: Java's future exception wrapping.

### `V5InteropTest`

Java-only. 2 tests: 2 Java-only. The v4/V5 interop helpers: converting schemas, and finding the v4 message under a V5
one. Rust has no v4 API (docs/1-scope.md §5).

- Java-only: `testSchemaRoundTrip`, `testV4Message`: v4 interop.

### `V5ReceiveQueueTest`

Ported. 28 tests: 23 covered and 5 Java-only. The V5 consumers' shared receive queue: receive forms, timeouts, close,
cancellation, watermark flow control over its segment loops, and the order of waiting receives. Rust has no queue above
its segment consumers: each segment holds up to the receiver queue size through the broker's permits, refilled once half
is taken, so the watermark tests map to the permits, and the receive loop's and executors' tests are Java's.

- Unit: [`consumer.rs`][p-consumer] `subscribes_and_grants_permits_as_messages_are_taken`.
- Simulated cluster: [`consumer.rs`][tk-consumer] `permits_bound_what_the_broker_pushes`;
  [`queue_consumer.rs`][tk-queue] `segments_with_messages_take_turns`.
- Driver: [`consumer_tests.rs`][d-consumer] `a_receive_whose_time_is_up_takes_the_messages_in`,
  `receives_with_a_timeout_and_in_batches`, `a_negative_ack_brings_the_message_back`,
  `the_blocking_api_receives_from_a_plain_thread`, `closing_the_client_closes_its_consumers_and_fails_their_receives`,
  `the_stream_view_ends_when_the_consumer_closes`, `receives_what_a_producer_sends_and_acknowledges_it`,
  `a_dropped_receive_loses_no_message`, `taking_half_the_receiver_queue_grants_as_many_permits_again`,
  `waiting_receives_are_answered_in_the_order_they_were_polled`.
- Java-only:
  - `fastPathGrantsCapacityImmediatelyWhenBufferHasRoom`, `fastPathHoldsUpToTheHighWatermark`: executor fast path.
  - `closeReleasesPausedProducers`: receive loops.
  - `blockingReceiveDoesNotDependOnTheExecutor`: executor.
  - `directReceiveDuringPausePublicationStillResumesTheProducer`: a race in the queue's pause.

### `V5UtilsTest`

Ported. 5 tests: 5 covered. Normalising the topic a V5 builder is given. Ported in the producer's topic-name test.

- Unit: [`producer/mod.rs`][d-producer-mod] `topic_names`.
- Driver: [`producer_tests.rs`][d-producer] `invalid_configuration_fails_create`.
- Against 5.0.1: [`main.rs`][it-main] `a_short_name_is_a_regular_topic`.

## `pulsar-client` and `pulsar-client-api-v5`

Authentication as the V5 client drives it, through `pulsar-client`'s connection, and the API module's own tests.

### `AuthInitializationRetryTest`

Ported. 1 test: 1 covered. A failed `initializeAsync` isn't cached: the next connection initializes again. Rust has no
init step; each new connection asks the provider again.

- Driver: [`driver.rs`][d-driver] `a_provider_that_failed_is_asked_again_for_the_next_connection`.

### `AuthMetricsTest`

Not ported. 3 tests: 3 not ported. The client's auth metrics around a credential fetch:
`pulsar.client.auth.credential.duration`, and `pulsar.client.auth.failure` classified terminal or transient. The Rust
client records no metrics yet: [8](../docs/8-quality.md) §1 designs them, and no milestone schedules them.

- Not ported: `recordsCredentialDurationOnSuccessAndNoFailureCounter`, `recordsTerminalFailure`,
  `recordsTransientFailure`: the client records no metrics yet.

### `BinaryAuthenticationExchangeTest`

Ported. 4 tests: 4 covered. Which `CommandAuthChallenge` payload re-produces the credential and which goes to the
challenge handler: only `PulsarAuthRefresh` is a refresh.

- Simulated cluster: [`connection.rs`][tk-connection] `auth_refresh_and_challenge_rounds`,
  `only_the_refresh_sentinel_asks_for_fresh_credentials`.
- Driver: [`tests.rs`][d-tests] `token_authentication_and_refresh`; [`driver.rs`][d-driver]
  `only_a_refresh_is_answered_with_fresh_auth_data`, `challenges_are_answered_by_the_provider`.

### `BoundInitContextFallbackTest`

Java-only. 2 tests: 2 Java-only. The plugin init context's scheduler and blocking executor, never null when a component
binds only some services. A Rust provider gets no init context and no executors.

- Java-only: `aBoundContextWithNoSchedulerStillSuppliesOne`, `boundExecutorsAreUsedWhenSupplied`: plugin executors.

### `ClientAuthenticationFailsLoudlyTest`

Java-only. 3 tests: 3 Java-only. Two loud failures of Java's v5 auth plumbing: a plugin without the binary capability,
and Java serialization of a configuration holding a v5 plugin. Rust's `Authentication` trait is binary-only, and nothing
serializes a client configuration.

- Java-only:
  - `aPluginThatCannotServeTheBinaryTransportFailsTheBuild`: capability lookup; every Rust `Authentication` serves the
    binary protocol. The nearest Rust rule, a TLS provider on a `pulsar://` URL failing the build, is checked by
    [`options.rs`][d-options] `the_builder_refuses_incomplete_or_contradictory_options`.
  - `serializingAConfigurationCarryingAV5PluginIsRefused`, `aConfigurationUsingTheStringFormStillSerializes`: Java
    serialization.

### `ClientAuthenticationLifecycleTest`

Ported. 3 tests: 1 covered and 2 Java-only. The client's v5 auth slot following the client's lifecycle: a runtime swap,
the close, a reused builder. Only the close is client behaviour Rust has: closing the client releases its provider.

- Driver: [`tests.rs`][d-tests] `closing_the_client_releases_its_authentication_provider`.
- Java-only:
  - `swappingTheAuthenticationSwapsWhatTheClientDrives`: v4 cluster failover; neither the V5 API nor Rust swaps a
    client's authentication. A Rust provider rotates its own credential through `Token::from_fn` ([`tests.rs`][d-tests]
    `token_authentication_and_refresh`).
  - `aSecondClientFromTheSameBuilderUsesTheSecondCredential`: Java's builder hands its configuration object to the
    client; Rust's `build` consumes the builder, which isn't `Clone`.

### `ClientCnxAsyncAuthTest`

Partly ported. 10 tests: 8 covered, 1 not ported and 1 Java-only. `ClientCnx` driving async credentials: connect and
refresh, failures and a timeout at connect, stale and late answers, the cap on challenge rounds. Rust tests each
behaviour but a failed `CONNECT` write.

- Simulated cluster: [`connection.rs`][tk-connection] `auth_refresh_and_challenge_rounds`,
  `a_closed_connection_ignores_everything`, `a_refresh_starts_the_challenge_rounds_over`.
- Driver: [`tests.rs`][d-tests] `token_authentication_and_refresh`; [`driver.rs`][d-driver]
  `a_failing_authentication_provider_is_an_authentication_failure`, `a_refresh_supersedes_one_still_pending`,
  `a_socket_closed_while_the_credential_is_fetched_fails_the_connection`,
  `the_connect_timeout_covers_the_authentication_provider`; [`errors.rs`][d-errors] `connection_failures_map_to_kinds`.
- Not ported: `connectWriteFailureFailsConnectionFutureWithWriteCause`: nothing makes a write fail on a loopback socket.
- Java-only: `continuationRunsOnEventExecutorNotTheCompletingThread`: Netty executor hop. Rust's connection and socket
  belong to one `!Send` task on one loop; a provider future completing elsewhere only wakes it.

### `CredentialOffloadTest`

Java-only. 4 tests: 4 Java-only. A blocking credential supplier (OAuth2's, the token's) runs on the client's blocking
executor, never on the caller's thread, the event loop. Rust offloads nothing: a provider is a future polled on the
connection's loop, and one doing blocking work offloads it itself.

- Java-only:
  - `slowCredentialSupplierRunsOnBlockingExecutorNotCaller`: blocking executor; OAuth2 itself comes after the first
    release.
  - `slowFileTokenSupplierRunsOnBlockingExecutorNotCaller`: blocking executor. Rust's `Token::from_fn` is async and
    polled on the loop with no offload, and Rust has no `file:` token.
  - `supplierFailureCompletesFutureExceptionallyNotSynchronously`: a Rust provider returns a `Result` future, so it can
    fail no other way. A fetch failing at a refresh is checked by [`driver.rs`][d-driver]
    `a_provider_failing_a_refresh_closes_the_connection`, and at connect by [`driver.rs`][d-driver]
    `a_failing_authentication_provider_is_an_authentication_failure`.
  - `withoutBoundExecutorRunsOnTheSharedPoolNotTheCaller`: executor fallback; the proxy is out of scope too.

### `LegacyV4AuthenticationAdapterTest`

Java-only. 8 tests: 8 Java-only. The adapter that runs a v4 `Authentication` plugin under the v5 SPI: which adapter it
picks, unwrapping, deferred calls and the plugin's lifecycle. Rust has no v4 plugins.

- Java-only:
  - `tokenAuthIsWrappedAsBinaryDataProvider`: v4 adapter. Rust's token presents the same: [`options.rs`][d-options]
    `token_providers`, [`tests.rs`][d-tests] `token_authentication_and_refresh`.
  - `wrapSelectsCredentialAdapterForToken`, `wrapSelectsTlsAdapterForTlsAndUnwrapsBackToV4`,
    `unwrapV4ReturnsEmptyForNativePlugin`: v4 adapter.
  - `wrapAndConfigurePerformNoV4CredentialCalls`, `aTlsMethodPluginIsStartedAndClosedThroughTheAdapter`: v4 plugin
    lifecycle.
  - `credentialAdapterExposesOnlyProbedCapabilities`: capability probing.
  - `capabilityMethodBeforeInitializationFailsFast`: v5 init phase.

### `TlsAuthenticationTest`

Ported. 1 test: 1 covered. The built-in mTLS plugin: method name `tls` and empty auth data, the certificate going at the
TLS handshake.

- Driver: [`options.rs`][d-options] `tls_authentication_reads_its_pem_files`,
  `mutual_tls_presents_the_client_certificate`.
- Against 5.0.1: [`main.rs`][it-main] `tls_with_client_certificate`.

## `pulsar-broker`: the V5 client against a broker

The `V5*Test` classes and their kin: the V5 client against brokers the test starts. Their scenarios map to the tests
against 5.0.1 here, and to the simulated cluster's where a broker can't be made to do what the test needs.

### `TopicAutoCreationAuthorizationTest`

The broker's. 9 tests: 9 the broker's. The broker's `AuthorizationProvider#allowTopicAutoCreationAsync` gates
auto-creation on binary and HTTP lookups, admin calls and proxied lookups; eight methods drive it with the v4 client or
raw HTTP, one with the V5 producer. The client only surfaces the refusal.

- The broker's:
  - `testProducerAutoCreationFollowsAuthorization`: the authorization provider decides. Rust surfaces a refused
    auto-creation as `NotFound` for a scalable topic ([`main.rs`][it-main]
    `a_missing_topic_is_not_found_without_auto_creation`, refused by namespace policy there); nothing checks it for a
    regular topic.
  - `testConsumerAutoCreationFollowsAuthorization`: the authorization provider decides. A consumer refused a missing
    topic gets `NotFound` ([`consumer_tests.rs`][d-consumer] `a_missing_topic_is_not_found`, [`main.rs`][it-main]
    `a_missing_topic_is_not_found_without_auto_creation`), refused there by namespace policy rather than authorization.
  - `testHttpLookupAutoCreationFollowsAuthorization`: the HTTP lookup service's authorization. Rust has no HTTP lookup:
    `ServiceUrl::parse` refuses `http://` ([`lookup.rs`][tk-lookup] `service_urls`).
  - `testPartitionedAutoCreationFollowsAuthorization`: authorization and partitioned-topic metadata.
  - `testMissingPartitionsOfExistingPartitionedTopicAreCreated`: partition creation for existing metadata.
  - `testAdminCreateSubscriptionAutoCreationFollowsAuthorization`: admin REST API.
  - `testExistingTopicDoesNotDependOnAutoCreationCheck`: the lookup's authorization path.
  - `testProxiedHttpLookupAutoCreationFollowsOriginalPrincipal`: raw HTTP with proxy headers; no client.
  - `testScalableTopicAutoCreationFollowsAuthorization`: authorization of the scalable lookup's auto-creation. A
    producer or consumer refused a missing topic gets `NotFound` ([`producer_tests.rs`][d-producer]
    `a_missing_topic_is_not_found`, [`main.rs`][it-main] `a_missing_topic_is_not_found_without_auto_creation`), refused
    there by namespace policy rather than authorization.

### `V5AsyncApisTest`

Ported. 6 tests: 6 covered. Java's `async()` views of the producer and the three consumers, on a one-segment topic.
Rust's API is async to begin with (the blocking API is the extra one), so its ordinary driver tests are these scenarios.

- Driver: [`producer_tests.rs`][d-producer] `flush_waits_for_the_sends_before_it`,
  `flush_outlasts_the_sends_that_complete_first`, `pipelined_sends_share_entries_unless_batching_is_off`,
  `local_producer_sends_flushes_and_closes`; [`consumer_tests.rs`][d-consumer]
  `receives_what_a_producer_sends_and_acknowledges_it`; [`stream_tests.rs`][d-stream]
  `receives_in_order_and_acknowledges_cumulatively`; [`checkpoint_tests.rs`][d-checkpoint] `resumes_from_a_checkpoint`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively`,
  `a_checkpoint_consumer_resumes_across_a_split`.

### `V5AuthErrorTest`

Ported. 5 tests: 4 covered and 1 out of scope. A typed Authorization or Authentication error for each request a broker
can refuse: the topic lookup, the namespace watch, the controller and segment subscribes, segment producers, and
credentials. Rust covers the refusals on the simulated broker, and refused credentials for a producer and a Stream
consumer against 5.0.1, whose integration broker runs with authorization off.

- Simulated cluster: [`producer_edges.rs`][tk-producer-edges] `registration_failures`,
  `an_exclusive_create_refused_for_authorization_fails`; [`connection.rs`][tk-connection]
  `refused_connection_is_not_retryable_for_auth_errors`.
- Driver: [`producer_tests.rs`][d-producer] `a_refused_lookup_fails_create`, `a_refused_segment_lookup_fails_the_send`;
  [`consumer_tests.rs`][d-consumer] `a_refused_lookup_fails_the_subscribe`,
  `a_refused_subscribe_fails_with_the_brokers_error_and_closes_the_rest`; [`stream_tests.rs`][d-stream]
  `a_refused_lookup_fails_the_subscribe`, `a_refused_registration_or_a_bad_builder_fails_the_subscribe`;
  [`checkpoint_tests.rs`][d-checkpoint] `a_refused_lookup_fails_the_create`, `a_refused_registration_fails_the_create`,
  `a_refused_reader_fails_the_create_and_closes_the_rest`; [`errors.rs`][d-errors] `broker_error_codes_map_to_kinds`,
  `a_refused_handshake_is_the_brokers_refusal_unless_it_is_retried`.
- Against 5.0.1: [`main.rs`][it-main] `missing_credentials_are_refused`.
- Out of scope: `testNamespaceWatchNotAuthorized`: namespace subscription.

### `V5AutoRebucketTest`

The broker's. 2 tests: 2 the broker's. The controller's PIP-486 policy on a cold topic: rebucket a segment, rather than
split it, for surplus Stream consumers, and leave a Checkpoint group's surplus alone. Both are controller decisions; the
client's side of each is covered.

- The broker's:
  - `testConsumerSurplusOnColdTopicRebucketsInsteadOfSplitting`: the rollover is the controller's decision. The client's
    side, five Stream consumers following it and sharing the successor with each key whole and in order, is tested
    against 5.0.1 ([`buckets.rs`][it-buckets] `more_stream_consumers_than_buckets_get_a_rebucket`).
  - `testCheckpointGroupSurplusOnColdTopicDoesNotRebucket`: the controller's choice, read from metadata. The client's
    side is covered: members register as Checkpoint consumers ([`checkpoint_group.rs`][tk-group]
    `a_member_reads_the_children_of_a_split_without_waiting_for_the_parent` passes only with that registration on the
    simulated controller), and on a segment of four entry buckets the surplus members stay idle
    ([`checkpoint_group.rs`][tk-group] `members_outnumbering_the_segments_deliver_each_message_once`).

### `V5CheckpointConsumerBasicTest`

Ported. 6 tests: 6 covered. An ungrouped Checkpoint consumer on a one-segment topic: earliest and latest starts,
checkpoint and resume, through bytes too, the receive timeout and the topic accessor. Each has a simulated-cluster or
loopback counterpart.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `reads_from_the_earliest`,
  `latest_skips_what_the_segment_had`, `resumes_from_a_checkpoint`.
- Driver: [`checkpoint_tests.rs`][d-checkpoint]
  `a_latest_consumer_reads_what_comes_after_it_and_its_checkpoint_says_so`, `resumes_from_a_checkpoint`,
  `receives_with_a_timeout_in_batches_and_as_a_stream`.

### `V5CheckpointConsumerDagReplayTest`

Ported. 4 tests: 4 covered. A Checkpoint consumer created at the earliest position after one or two splits, ungrouped
and as a group member, reads the sealed backlog and the children's messages. All four run on the simulated cluster; the
grouped one-split case also against 5.0.1.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint]
  `an_earliest_consumer_reads_sealed_parents_and_their_children`, `keeps_per_key_order_starting_after_splits`;
  [`checkpoint_group.rs`][tk-group] `an_earliest_member_created_after_a_split_reads_the_sealed_backlog`,
  `an_earliest_member_created_after_two_splits_reads_all_the_backlog`.
- Against 5.0.1: [`main.rs`][it-main] `a_checkpoint_group_member_reads_the_sealed_backlog`.

### `V5CheckpointConsumerGroupTest`

Ported. 8 tests: 7 covered and 1 the broker's. Checkpoint consumer groups (sharing, surplus members, scaling up, no
registration left behind), and an ungrouped consumer's batch receive and resumes, also across a split and a merge.

- Simulated cluster: [`checkpoint_group.rs`][tk-group] `members_of_a_group_share_the_segments`,
  `members_outnumbering_the_segments_deliver_each_message_once`,
  `a_member_joining_at_the_latest_position_reads_what_follows`; [`world.rs`][tk-world]
  `a_checkpoint_group_neither_shares_nor_waits_for_parents`; [`checkpoint_consumer.rs`][tk-checkpoint]
  `resumes_from_a_checkpoint`, `keeps_per_key_order_resuming_a_checkpoint_across_a_split`,
  `resumes_a_checkpoint_across_a_merge`.
- Driver: [`checkpoint_tests.rs`][d-checkpoint] `members_of_a_group_share_the_segments`, `resumes_from_a_checkpoint`.
- Against 5.0.1: [`main.rs`][it-main] `checkpoint_group_members_share_the_segments`,
  `a_checkpoint_consumer_resumes_across_a_split`, `a_checkpoint_round_trips_with_java_across_layout_changes`.
- The broker's: `testNoMetadataLeftAfterAllGroupMembersClose`: the controller's session eviction and its stats. Client
  side covered: a member that closes leaves the group at once ([`checkpoint_group.rs`][tk-group]
  `close_leaves_the_group`, [`checkpoint_tests.rs`][d-checkpoint] `members_of_a_group_share_the_segments`); the
  simulated controller's eviction after the grace period: [`checkpoint_group.rs`][tk-group]
  `members_that_went_away_leave_no_registration_behind`.

### `V5CheckpointConsumerLatestStartTest`

Ported. 3 tests: 3 covered. An ungrouped Checkpoint consumer started at latest reads the segments a later split or merge
creates from their start, and those present at its start from their end. Java stalls the client's IO threads so that the
producer writes to the children first; the simulated cluster stores the messages before the consumer takes in the new
layout.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint]
  `a_latest_consumer_reads_the_children_of_a_split_from_their_start`,
  `a_latest_consumer_reads_a_merged_segment_from_its_start`,
  `a_latest_consumer_skips_what_the_segments_at_its_start_had`.
- Driver: [`checkpoint_tests.rs`][d-checkpoint] `a_latest_consumer_lets_go_of_a_sealed_parent_at_its_end`.
- Against 5.0.1: [`main.rs`][it-main] `a_latest_checkpoint_consumer_reads_a_later_split`.

### `V5CheckpointConsumerOrderingTest`

Ported. 6 tests: 6 covered. Per-key order of an ungrouped Checkpoint consumer: a key's messages on a sealed parent
before those on its children, while reading through a split or a merge, starting after splits, resuming a checkpoint,
and past a deleted parent. All on the simulated cluster; the resume and the caught-up split also against 5.0.1.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `keeps_per_key_order_across_a_split_while_reading`,
  `keeps_per_key_order_across_a_merge_while_reading`, `delivers_the_children_of_a_split_while_caught_up`,
  `keeps_per_key_order_starting_after_splits`, `keeps_per_key_order_resuming_a_checkpoint_across_a_split`,
  `a_deleted_parent_does_not_hold_its_children`.
- Against 5.0.1: [`main.rs`][it-main] `a_latest_checkpoint_consumer_reads_a_later_split`,
  `a_checkpoint_consumer_resumes_across_a_split`, `a_checkpoint_consumer_skips_a_deleted_segment`.

### `V5CheckpointConsumerSkipDeletedSegmentTest`

Ported. 1 test: 1 covered. A segment whose topic was deleted while the layout still lists it, as the controller's
garbage collection does, is skipped silently, and the other segment is read.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `skips_a_deleted_segment`.
- Against 5.0.1: [`main.rs`][it-main] `a_checkpoint_consumer_skips_a_deleted_segment`.

### `V5CheckpointConsumerUntouchedSegmentTest`

Ported. 5 tests: 5 covered. A checkpoint names every segment the consumer reads, those it got nothing from included, so
a restore replays nothing: a latest start (ungrouped and grouped), a chained restore, a sealed parent read to its end,
and a checkpoint taken at a latest start while messages arrive.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `a_checkpoint_names_the_segments_nothing_came_from`,
  `a_chained_restore_names_the_segments_nothing_came_from`, `a_restore_keeps_a_sealed_segment_read_to_its_end`,
  `a_checkpoint_at_a_latest_start_restores_what_the_consumer_read`; [`checkpoint_group.rs`][tk-group]
  `a_latest_member_checkpoint_names_the_segments_nothing_came_from`.
- Driver: [`checkpoint_tests.rs`][d-checkpoint]
  `a_latest_consumer_reads_what_comes_after_it_and_its_checkpoint_says_so`.

### `V5ClientBuilderConfigTest`

Ported. 4 tests: 1 covered and 3 Java-only. Reflects into the wrapped v4 client's `ClientConfigurationData` to check
that four builder values arrive. Three are options Rust doesn't have and the spec doesn't name; the memory limit Rust
has, and tests by its effect.

- Driver: [`producer_tests.rs`][d-producer] `the_memory_limit_fails_or_holds_sends`,
  `sends_wait_for_room_at_the_memory_limit`.
- Java-only:
  - `testListenerNamePropagates`: v4 configuration plumbing; Rust's `ClientBuilder` has no listener-name option.
  - `testDescriptionPropagates`: v4 configuration plumbing; Rust has no client description option.
  - `testOpenTelemetryPropagates`: v4 configuration plumbing; Rust has no OpenTelemetry handle option.

### `V5ClientLifecycleTest`

Ported. 3 tests: 1 covered, 1 Java-only and 1 out of scope. The client's `closeAsync`, `shutdown` and
`newTransactionAsync`.

- Driver: [`tests.rs`][d-tests] `thread_safe_client_runs_on_its_loops_from_any_executor`;
  [`producer_tests.rs`][d-producer] `closing_the_client_closes_its_producers`.
- Java-only: `testShutdownDelegatesToV4`: delegation to v4; Rust has no `shutdown`, only `close`, which can be called
  again.
- Out of scope: `testNewTransactionAsyncReturnsOpenTransaction`: transactions.

### `V5ConcurrentProducersConsumersTest`

Ported. 1 test: 1 covered. Four producer threads and three Queue consumer threads on one 4-segment topic and
subscription: each message consumed exactly once, and every consumer gets some. Ported on the thread-safe client.

- Driver: [`producer_tests.rs`][d-producer] `thread_safe_producers_spread_over_the_loops`,
  `concurrent_producers_and_consumers_get_every_message_once`; [`consumer_tests.rs`][d-consumer]
  `thread_safe_consumers_receive_on_any_thread`.
- Against 5.0.1: [`main.rs`][it-main] `queue_consumers_share_a_subscription`.

### `V5ConsumerPriorityLevelTest`

The broker's. 2 tests: 2 the broker's. Priority dispatch on a Shared subscription is the broker's: the client only sends
each segment consumer's level and grants permits. Rust covers that part, the level on every segment's subscribe and
permits that follow what the application takes, but no Rust test runs two consumers at different levels.

- The broker's:
  - `testHigherPriorityConsumerReceivesAllWhenPrefetchAvailable`: the dispatcher serves the lowest level first. The
    client's part is covered: [`queue_consumer.rs`][tk-queue] `each_segment_consumer_gets_the_options`,
    [`consumer_tests.rs`][d-consumer] `a_negative_ack_brings_the_message_back` (the builder's `priority_level` on the
    subscribe); the resulting distribution is not checked.
  - `testLowerPriorityConsumerReceivesOverflow`: the dispatcher's overflow to the next level once the first has no
    permits. The client's part, its permits bounding what the broker pushes, is covered: [`consumer.rs`][tk-consumer]
    `permits_bound_what_the_broker_pushes`, [`consumer.rs`][p-consumer]
    `subscribes_and_grants_permits_as_messages_are_taken`.

### `V5ConsumerReceiverQueueSizeTest`

Ported. 2 tests: 2 covered. Reads by reflection each segment's v4 receiver queue size; the scenario behind it, every
segment consumer prefetching the configured depth, is client behaviour Rust has, checked as the FLOW permits on the
wire.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `each_segment_consumer_gets_the_options`.
- Driver: [`consumer_tests.rs`][d-consumer] `a_negative_ack_brings_the_message_back`.

### `V5ConsumerSubscriptionPropertiesTest`

Ported. 2 tests: 2 covered. Reads by reflection each segment's v4 consumer configuration; the scenario, the subscription
properties going out with every segment's subscribe, is client behaviour Rust checks on the wire.

- Unit: [`consumer.rs`][p-consumer] `subscribes_and_grants_permits_as_messages_are_taken`.
- Simulated cluster: [`queue_consumer.rs`][tk-queue] `each_segment_consumer_gets_the_options`.
- Driver: [`consumer_tests.rs`][d-consumer] `a_negative_ack_brings_the_message_back`.

### `V5CumulativeAckTest`

Ported. 5 tests: 4 covered and 1 out of scope. A Stream consumer's cumulative ack: one position vector covering every
segment, mid-stream, across a split, after an aborted transaction, and retried after a disconnect (at 5.0.1,
`testRetryingAnAckThatFailedWhileDisconnectedAdvancesTheCursor`).

- Unit: [`consumer.rs`][p-consumer] `a_new_registration_takes_the_same_cumulative_ack_again`,
  `a_cumulative_ack_made_while_subscribing_goes_first`,
  `a_cumulative_ack_made_while_disconnected_goes_first_on_the_next_subscription`.
- Simulated cluster: [`stream_consumer.rs`][tk-stream] `a_cumulative_ack_covers_every_segment`,
  `a_split_while_consuming_keeps_each_key_in_order`; [`consumer.rs`][tk-consumer]
  `a_reconnect_brings_back_what_was_not_acknowledged`.
- Driver: [`stream_tests.rs`][d-stream] `receives_in_order_and_acknowledges_cumulatively`,
  `follows_a_split_once_the_parent_is_drained`.
- Against 5.0.1: [`conformance.rs`][it-conformance] `cumulative_ack_across_segments`.
- Out of scope: `testOrdinaryAckAfterAbortedTransactionalAckAdvancesTheCursor`: transactions.

### `V5DAGFollowingTest`

Ported. 2 tests: 2 covered. A Queue consumer follows the layout's DAG: a new Earliest subscription reads sealed segments
too, and an existing one follows a chain of layout changes.

- Simulated cluster: [`queue_consumer.rs`][tk-queue]
  `a_new_earliest_subscription_replays_the_sealed_parent_and_the_children`,
  `follows_splits_and_merges_without_losing_a_message`, `subscribes_new_segments_while_another_subscribe_is_pending`.
- Against 5.0.1: [`main.rs`][it-main] `a_split_during_consumption_loses_no_message`.

### `V5DagWatchAutoReconnectTest`

Ported. 3 tests: 3 covered. The layout watch reopens after its connection drops, and producers and Queue consumers carry
on; Java force-closes the channel by reflection, the Rust tests kill the connections. The same watch code serves Rust's
producers and consumers, and the testkit's harnesses reopen the session the way the driver does.

- Simulated cluster: [`producer.rs`][tk-producer] `a_migration_made_while_disconnected_is_followed`;
  [`queue_consumer.rs`][tk-queue] `keeps_consuming_and_following_the_layout_after_a_connection_loss`.
- Driver: [`producer_tests.rs`][d-producer] `a_lost_connection_writes_unacknowledged_messages_again`,
  `a_watch_reopens_its_session_through_failed_lookups`; [`consumer_tests.rs`][d-consumer]
  `a_lost_connection_brings_back_what_was_not_acknowledged`.

### `V5EntryBucketDispatchTest`

Partly ported. 7 tests: 6 covered and 1 partly covered. Stream consumers on an entry-bucketed segment: one alone, two
sharing it by bucket, and the hand-offs (mode flips and releases) as members join, acknowledge late, leave and churn.

- Unit: [`stream_consumer.rs`][p-stream] `a_mode_flip_drains_then_resubscribes`,
  `a_retry_goes_on_while_a_segment_drains`, `a_segment_comes_back_only_once_its_release_drained`,
  `a_close_while_registering_leaves_once_registered`; [`consumer.rs`][p-consumer]
  `a_paused_consumer_settles_once_what_it_gave_is_acknowledged`.
- Simulated cluster: [`stream_consumer.rs`][tk-stream] `receives_in_order_and_acknowledges_cumulatively`,
  `members_beyond_the_segments_share_them_by_bucket`, `a_member_joining_mid_traffic_keeps_each_key_in_order`,
  `a_segment_moves_once_what_was_taken_from_it_is_acknowledged`,
  `a_member_closed_while_registering_leaves_once_registered`.
- Driver: [`stream_tests.rs`][d-stream] `a_dropped_subscribe_closes_the_consumer`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively`,
  `stream_consumers_share_a_segment_by_bucket`, `stream_consumers_share_batched_entries_by_bucket`,
  `a_closed_stream_consumer_hands_its_segments_over_at_once`.
- Partly covered: `testRapidSubscribeCloseChurnLeavesGroupClean`: the client's side is covered: a close while
  registering, and clean leaves. Repeated churn, and a rejoin under the same name racing a leave, test how the
  controller converges and tells sessions apart, which is the broker's.

### `V5KeyRoutingTest`

Ported. 2 tests: 2 covered. Key routing on a 4-segment topic: a key always goes to the same segment, so its order holds;
many keys spread with nothing lost or duplicated.

- Unit: [`routing_vectors.rs`][p-routing] `routing_matches_java`.
- Simulated cluster: [`producer.rs`][tk-producer] `routes_keys_by_hash_and_keyless_round_robin`.
- Driver: [`producer_tests.rs`][d-producer] `pipelining_combinators_keep_send_order`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_sealed_backlog_first_in_key_order`,
  `a_queue_consumer_reads_and_acknowledges_every_segment`.

### `V5LayoutChangeDuringCreateTest`

Ported. 3 tests: 3 covered. A layout that reaches the watch while a producer or consumer is being created must be
applied once it exists. In Rust a watch's holder starts from the watch's current layout and gets every later one
(`crates/apache-pulsar/src/watch.rs` `Watches::open`).

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint]
  `the_create_waits_for_the_readers_of_layouts_that_come_meanwhile`; [`queue_consumer.rs`][tk-queue]
  `the_subscribe_waits_for_the_segments_of_layouts_that_come_meanwhile`; [`producer_edges.rs`][tk-producer-edges]
  `an_exclusive_create_follows_a_split_that_lands_meanwhile`.
- Driver: [`producer_tests.rs`][d-producer] `a_producer_created_after_a_split_sends_to_the_children`.
- Against 5.0.1: [`bridge.rs`][it-bridge] `a_producer_keeps_key_order_across_a_migration`.

### `V5MessageMetadataTest`

Ported. 6 tests: 6 covered. Message builder metadata, read back by a Queue consumer: event time, properties, delayed
delivery. Rust checks each field on the wire and back, and delayed delivery as a consumer sees it, against 5.0.1.

- Simulated cluster: [`producer_batching.rs`][tk-batching] `a_delivery_time_or_replication_clusters_go_alone`.
- Driver: [`consumer_tests.rs`][d-consumer] `receives_what_a_producer_sends_and_acknowledges_it`,
  `messages_show_what_their_metadata_says`; [`producer_tests.rs`][d-producer] `every_message_field_reaches_the_wire`.
- Against 5.0.1: [`main.rs`][it-main] `delayed_messages_wait_for_their_time`.

### `V5MessageReplicationClustersTest`

Ported. 1 test: 1 covered. `replicationClusters` on a message lands in the metadata the broker stores; Java reads it by
reflection on the consumer's v4 message.

- Simulated cluster: [`producer_batching.rs`][tk-batching] `a_delivery_time_or_replication_clusters_go_alone`.
- Driver: [`producer_tests.rs`][d-producer] `every_message_field_reaches_the_wire`,
  `the_blocking_api_has_the_whole_producer`.

### `V5MigrationEndToEndTest`

Ported. 3 tests: 2 covered and 1 the broker's. A regular topic's migration to a scalable one, with V5 clients on it
throughout, and the lock-out of v4 producers afterwards.

- Simulated cluster: [`producer.rs`][tk-producer] `per_key_order_holds_across_a_migration_with_traffic`.
- Against 5.0.1: [`bridge.rs`][it-bridge] `consumers_follow_a_migration`,
  `a_producer_keeps_key_order_across_a_migration`.
- The broker's: `testV4ProducerLockedOutAfterMigration`: the migration terminates the regular topic; no V5 client is
  involved. Rust's bridge tests check it anyway with Java's v4 producer: [`bridge.rs`][it-bridge]
  `a_v4_client_holds_off_a_migration_unless_forced`, [`bridge.rs`][it-bridge]
  `v4_and_rust_clients_across_a_forced_migration`.

### `V5MultiBrokerControllerFailoverTest`

Not ported. 2 tests: 1 not ported and 1 needing several brokers. Controller-leader failover across brokers: a Stream
consumer must register with the broker that takes the controller lock over. The integration tests run one broker; on
three simulated brokers a Stream consumer follows the controller its layout names, but not with a producer of the topic
on the same client: a bug, pinned by an ignored test until it is fixed.

- Not ported: `testStreamConsumerFollowsControllerLeaderShutdown`: with a producer of the topic on the same client, as
  here, a Stream consumer registers again with the old controller, a bug that [`stream_tests.rs`][d-stream]
  `follows_the_controller_while_a_producer_holds_the_layout_watch` pins, ignored until it is fixed. Without the
  producer, the consumer follows the controller ([`stream_tests.rs`][d-stream]
  `follows_the_controller_the_layout_names_across_brokers`).
- Need several brokers: `testStreamConsumerSubscribesAfterControllerFailover`: the client's side is tested on three
  simulated brokers, a registration looking up the controller the current layout names once the first one is gone
  ([`stream_tests.rs`][d-stream] `follows_the_controller_the_layout_names_across_brokers`).

### `V5MultiBrokerNamespaceDeleteTest`

Needs several brokers. 6 tests: 6 needing several brokers. Namespace and tenant deletes with scalable topics on the
multi-broker cluster's Oxia store, which refuses to delete a node that has children. Broker-only: the client just
publishes, so a multi-broker CI would add no client path.

- Need several brokers:
  - `testDeleteNamespaceWithScalableTopicIsRejected`: the multi-broker cluster on Oxia; broker-only, no client path.
  - `testForceDeleteNamespaceWithScalableTopic`, `testDeleteNamespaceAfterDeletingItsScalableTopic`,
    `testDeleteNamespaceDeletesSegmentTopicsLeftBehind`, `testForceDeleteTenantWithScalableTopic`: the same;
    broker-only.
  - `testDeleteNamespaceWithNodeLeftUnderItIsConflict`: the same; broker-only, no client at all.

### `V5MultiBrokerScalableTopicDeleteTest`

Needs several brokers. 4 tests: 4 needing several brokers. A scalable topic's delete must stop its controllers on every
broker before the metadata delete removes the controller lock. Broker-only: no client is used.

- Need several brokers:
  - `deleteThroughNonLeaderBrokerStopsController`, `deleteThroughLeaderStopsFollowingControllers`,
    `deleteThroughNonLeaderStopsFollowingControllers`: several brokers; broker-only.
  - `followerTakesOverReleasedController`: several brokers; broker-only (its client-visible side is
    `V5MultiBrokerControllerFailoverTest`).

### `V5MultiBrokerScalableTopicStatsTest`

Needs several brokers. 1 test: 1 needing several brokers. Stats across brokers: topic stats redirect to the controller
leader, which collects every segment's from its owner; segment stats redirect to the owner.

- Need several brokers: `testStatsRedirectToLeaderAndFanOutAcrossBrokers`: several brokers; broker/admin-only. The
  producer names it reads are the client's, `<name>-seg-<id>` per segment, checked in [`producer.rs`][tk-producer]
  `sends_a_stamped_batch_of_one_per_message`.

### `V5MultiBrokerScalableTopicTest`

Needs several brokers. 6 tests: 6 needing several brokers. Scalable topics on a 3-broker cluster: lookups converging on
segment owners, controller leadership spread, admin redirects to the leader, and Stream and Checkpoint-group members
reaching the controller through other brokers. Three have a client path, covered on one broker or the simulated cluster;
three are the broker's or the admin's alone.

- Need several brokers:
  - `testProduceConsumeAcrossBrokers`: several brokers. Client path covered: [`clients.rs`][d-clients]
    `lookups_follow_redirects` (a lookup redirected to the owner), [`main.rs`][it-main]
    `a_queue_consumer_reads_and_acknowledges_every_segment` (one broker).
  - `testControllerLeadershipDistributesAcrossBrokers`: several brokers; broker-only, no client.
  - `testSplitFromNonLeaderBrokerRedirectsToOwner`, `testMergeFromNonLeaderBrokerRedirectsToOwner`: several brokers;
    admin-only.
  - `testStreamConsumerControllerCoordinationAcrossBrokers`: on one broker, two Stream members split four segments and
    each message goes to one of them ([`stream_consumer.rs`][tk-stream]
    `members_split_the_segments_and_each_message_goes_to_one`, [`main.rs`][it-main]
    `stream_consumers_split_the_segments`).
  - `testCheckpointConsumerControllerCoordinationAcrossBrokers`: several brokers. One-broker twin: [`main.rs`][it-main]
    `checkpoint_group_members_share_the_segments`, [`checkpoint_group.rs`][tk-group]
    `members_of_a_group_share_the_segments`.

### `V5MultipleConsumersTest`

Ported. 5 tests: 5 covered. Several consumers of one topic: Queue consumers share a subscription, Checkpoint consumers
each read everything, and Stream consumers split the segments, keep them on a rejoin and take them over past the grace
period.

- Simulated cluster: [`checkpoint_consumer.rs`][tk-checkpoint] `each_reader_is_exclusive_and_not_durable`;
  [`stream_consumer.rs`][tk-stream] `within_the_grace_period_the_assignment_holds`,
  `members_split_the_segments_and_each_message_goes_to_one`, `past_the_grace_period_the_group_rebalances`,
  `a_member_back_under_its_name_takes_the_same_keys_again`.
- Driver: [`checkpoint_tests.rs`][d-checkpoint]
  `a_latest_consumer_reads_what_comes_after_it_and_its_checkpoint_says_so`,
  `consumers_from_the_earliest_position_each_read_every_message`.
- Against 5.0.1: [`main.rs`][it-main] `queue_consumers_share_a_subscription`,
  `stream_consumers_share_a_segment_by_bucket`, `stream_consumers_split_the_segments`,
  `a_closed_stream_consumer_hands_its_segments_over_at_once`.

### `V5ProcessingTimeoutTest`

Not ported. 1 test: 1 not ported. The processing (ack) timeout: a message received and not acknowledged in time is
redelivered. The Rust client has no such option: [5](../docs/5-segment-engine.md) §6 leaves it open.

- Not ported: `testUnackedMessageIsRedeliveredAfterProcessingTimeout`: the client has no processing (ack) timeout:
  [5](../docs/5-segment-engine.md) §6 leaves it open.

### `V5ProducerAccessModeTest`

Ported. 4 tests: 4 covered. Shared, Exclusive and WaitForExclusive producers on a one-segment topic.

- Simulated cluster: [`producer.rs`][tk-producer] `exclusive_claims_every_segment_at_create`,
  `wait_for_exclusive_is_created_once_the_holder_leaves`, `close_does_not_wait_for_a_pending_registration`.
- Driver: [`producer_tests.rs`][d-producer] `thread_safe_producers_spread_over_the_loops`,
  `exclusive_conflicts_fail_create`, `wait_for_exclusive_outlasts_the_operation_timeout`,
  `a_waiting_producer_leaves_the_line_when_its_client_closes`.
- Against 5.0.1: [`main.rs`][it-main] `exclusive_producers_conflict_at_create`.

### `V5ProducerBackpressureTest`

Ported. 9 tests: 7 covered and 2 Java-only. The client memory limit as the producer's only backpressure: a send waits or
fails at the limit, and every reservation comes back. Rust reserves room on the caller's side with an async wait, so the
Java tests about its IO, dispatch and completion threads have no counterpart. A send dropped after its first poll isn't
recalled in Rust: it is stored, and its room comes back once it completes.

- Unit: [`memory.rs`][d-memory] `admits_until_the_limit_is_passed`, `waiters_get_room_in_order`.
- Driver: [`producer_tests.rs`][d-producer] `sends_wait_for_room_at_the_memory_limit`,
  `the_memory_limit_fails_or_holds_sends`, `the_blocking_api_sends_from_a_plain_thread`,
  `a_message_above_the_brokers_limit_fails`, `dropped_sends_keep_their_room_until_they_complete`,
  `flush_fails_when_a_send_it_waits_for_fails`, `keys_keep_their_order_across_a_split_during_traffic`,
  `sends_waiting_for_room_go_through_a_split_and_give_it_back`.
- Java-only:
  - `sendFromAnIoThreadFailsFastInsteadOfBlocking`: Java's IO threads must never park. Rust's wait for room never blocks
    a loop (a local client's sends wait on the loop's own thread in `sends_wait_for_room_at_the_memory_limit`).
  - `anAcknowledgementThatBeatsTheCallbackIsDeliveredOffTheDispatchThread`: a callback-registration race on Java's
    dispatch thread; a Rust send completes through a oneshot channel.

### `V5ProducerBatchingTest`

Ported. 4 tests: 4 covered. Default, disabled and custom batching policies deliver every message of pipelined sends
once.

- Simulated cluster: [`producer_batching.rs`][tk-batching] `the_open_batches_close_the_delay_after_their_first_message`,
  `the_open_batches_close_at_the_most_messages`.
- Driver: [`producer_tests.rs`][d-producer] `pipelined_sends_share_entries_unless_batching_is_off`.
- Against 5.0.1: [`main.rs`][it-main] `a_java_consumer_reads_rust_batches`.

### `V5ProducerClosingCompletionTest`

Ported. 1 test: 1 covered. A send that fails because its client is closing must still complete, though Java delivers the
failure on an executor the close shuts down.

- Driver: [`producer_tests.rs`][d-producer] `closing_the_client_closes_its_producers`, `a_send_after_close_fails`.

### `V5ProducerCompressionTest`

Ported. 6 tests: 6 covered. A compressible 4 KiB payload read back through a Queue consumer, with each codec, none, and
no policy set.

- Simulated cluster: [`producer_compression.rs`][tk-compression] `a_payload_reads_back_with_each_codec`.
- Driver: [`producer_tests.rs`][d-producer] `entries_are_compressed_with_the_policys_codec`.
- Against 5.0.1: [`main.rs`][it-main] `a_payload_reads_back_with_each_codec`.

### `V5ProducerFlowControlTest`

Ported. 4 tests: 3 covered and 1 Java-only. Reflects into the per-segment v4 producers' configuration for `sendTimeout`
and `blockIfQueueFull`. Rust has one producer state machine, and tests the options by their effect.

- Simulated cluster: [`producer.rs`][tk-producer] `the_send_timeout_fails_late_messages_and_the_producer_carries_on`.
- Driver: [`producer_tests.rs`][d-producer] `flush_fails_when_a_send_it_waits_for_fails`,
  `sends_wait_for_room_at_the_memory_limit`, `the_memory_limit_fails_or_holds_sends`.
- Java-only: `testSendTimeoutAppliesToEverySegment`: per-segment copies of v4 configuration; Rust's single state machine
  gives every segment's messages their deadline from the producer's one `send_timeout`.

### `V5ProducerLayoutChangeOrderingTest`

Ported. 3 tests: 3 covered. Per-key order of async sends in flight while the layout changes: the sends of a sealed
segment go to its successors before newer ones.

- Simulated cluster: [`producer.rs`][tk-producer] `per_key_order_holds_across_a_split_with_traffic`,
  `per_key_order_holds_across_a_merge_with_traffic`, `per_key_order_holds_across_consecutive_splits`.
- Driver: [`producer_tests.rs`][d-producer] `keys_keep_their_order_across_a_split_during_traffic`.
- Against 5.0.1: [`main.rs`][it-main] `keys_keep_their_order_across_a_split`.

### `V5ProducerSegmentGoneTest`

Ported. 3 tests: 3 covered. A segment terminated under the layout: its sends wait for the next layout without new
segment producers, and a closing client fails them at once.

- Simulated cluster: [`producer.rs`][tk-producer] `a_gone_segment_gets_no_new_producer_and_its_sends_fail_in_the_end`,
  `close_fails_waiting_sends_and_awaits_written_ones`; [`producer_edges.rs`][tk-producer-edges]
  `a_gone_segment_retires_again_for_new_messages`, `close_fails_what_the_broker_did_not_store`,
  `close_closes_registrations_that_complete_late`.
- Driver: [`producer_tests.rs`][d-producer] `closing_the_client_closes_its_producers`.

### `V5ProducerSequenceIdTest`

Ported. 4 tests: 4 covered. `lastSequenceId`, `initialSequenceId`, an application's sequence id, and the broker's
deduplication, on a one-segment topic.

- Simulated cluster: [`producer.rs`][tk-producer]
  `reconnecting_writes_unacknowledged_messages_again_with_their_sequence_ids`,
  `sequence_ids_start_after_the_initial_or_the_brokers_last`, `an_application_sequence_id_is_kept`,
  `a_duplicate_is_acknowledged_but_not_stored`; [`producer_batching.rs`][tk-batching]
  `a_sequence_id_that_may_be_a_duplicate_goes_alone`.
- Driver: [`producer_tests.rs`][d-producer] `local_producer_sends_flushes_and_closes`,
  `every_message_field_reaches_the_wire`, `the_blocking_api_has_the_whole_producer`.

### `V5ProducerSplitRetryTest`

Ported. 1 test: 1 covered. Sends issued right after a split, before the client has the new layout, retry on a child
without failing.

- Simulated cluster: [`producer.rs`][tk-producer] `a_send_refused_by_a_terminated_topic_waits_for_the_layout`,
  `a_split_resends_in_flight_messages_before_newer_ones`.
- Against 5.0.1: [`main.rs`][it-main] `a_split_during_consumption_loses_no_message`.

### `V5QueueConsumerBasicTest`

Ported. 5 tests: 5 covered. A Queue consumer's basics on one segment: ordered receive and ack, negative ack, receive
timeout, keys and properties, accessors.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `receives_from_every_segment_and_acknowledges_on_each`,
  `a_negative_ack_brings_the_message_back_after_the_delay`; [`consumer.rs`][tk-consumer]
  `receives_acknowledges_and_resumes_after_what_it_acked`.
- Driver: [`consumer_tests.rs`][d-consumer] `receives_with_a_timeout_and_in_batches`,
  `a_negative_ack_brings_the_message_back`, `receives_what_a_producer_sends_and_acknowledges_it`,
  `messages_show_what_their_metadata_says`, `the_blocking_api_receives_from_a_plain_thread`.
- Against 5.0.1: [`main.rs`][it-main] `a_negative_ack_brings_a_message_back`,
  `a_queue_consumer_reads_what_java_produced`.

### `V5QueueConsumerMultiSegmentAckTest`

Ported. 2 tests: 2 covered. Individual acks on a four-segment topic reach every segment: a consumer subscribing again
gets exactly what wasn't acknowledged.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `receives_from_every_segment_and_acknowledges_on_each`,
  `a_consumer_subscribing_again_gets_only_what_was_not_acknowledged`.
- Against 5.0.1: [`main.rs`][it-main] `a_queue_consumer_reads_and_acknowledges_every_segment`.

### `V5RegularTopicInteropTest`

Ported. 4 tests: 4 covered. V5 clients on regular `persistent://` topics through the synthetic layout, beside v4
clients.

- Unit: [`layout.rs`][p-layout] `legacy_layout_routes_like_v4_partitions`; [`producer/mod.rs`][d-producer-mod]
  `topic_names`.
- Simulated cluster: [`producer.rs`][tk-producer] `a_legacy_topic_routes_like_v4_partitions`,
  `a_non_partitioned_legacy_topic_is_one_segment`.
- Driver: [`producer_tests.rs`][d-producer] `invalid_configuration_fails_create`.
- Against 5.0.1: [`main.rs`][it-main] `a_partitioned_topic_routes_like_v4`, `a_short_name_is_a_regular_topic`,
  `a_queue_consumer_reads_compressed_java_batches`; [`bridge.rs`][it-bridge]
  `v4_and_rust_clients_share_a_regular_topic`.

### `V5ScalableSubscriptionAdminTest`

The broker's. 1 test: 1 the broker's. The admin's `createSubscription` registers a QUEUE subscription on the scalable
topic and creates its cursor on every segment, so a consumer joining later gets what was sent before.

- The broker's: `testPreCreatedSubscriptionRetainsPreProductionMessages`: the admin's. A consumer's initial position
  applies only to a new subscription: the client passes it on with each segment's subscribe, and the broker keeps an
  existing subscription's cursors.

### `V5ScalableTopicAutoCreateTest`

Ported. 4 tests: 4 covered. A missing `topic://` is created, with one segment, by the lookup of a producer or consumer,
unless the namespace forbids it, when the lookup fails with NotFound. Ported for the producer and each kind of consumer,
on the simulated cluster and against 5.0.1.

- Driver: [`producer_tests.rs`][d-producer] `a_missing_topic_is_not_found`; [`stream_tests.rs`][d-stream]
  `a_missing_topic_is_created_or_not_found`; [`consumer_tests.rs`][d-consumer] `a_missing_topic_is_not_found`;
  [`checkpoint_tests.rs`][d-checkpoint] `a_missing_topic_is_not_found`.
- Against 5.0.1: [`main.rs`][it-main] `keys_keep_their_order_across_a_split`,
  `a_split_during_consumption_loses_no_message`, `a_stream_consumer_creates_a_missing_topic`,
  `a_missing_topic_is_not_found_without_auto_creation`; [`schemas.rs`][it-schemas]
  `the_first_consumer_of_a_new_topic_registers_its_schema`.

### `V5ScalableTopicDeleteTest`

The broker's. 3 tests: 3 the broker's. A scalable topic's delete must stop its controller: no re-election under the
deleted path, no registration against the deleted layout, a fresh controller for a topic created again. Two methods send
the scalable subscribe raw.

- The broker's:
  - `deleteReleasesElectedController`: controller and metadata.
  - `subscribeToDeletedTopicFails`: [`stream_tests.rs`][d-stream]
    `a_refused_registration_or_a_bad_builder_fails_the_subscribe`, [`stream_consumer.rs`][tk-stream]
    `a_refused_registration_fails_the_subscribe` cover a refused registration failing the subscribe; 5.0.1's same
    refusal gives `ErrorKind::Other` in [`bridge.rs`][it-bridge] `a_checkpoint_group_needs_a_migrated_topic`. The raw
    command skips the layout watch that both clients open first.
  - `recreatedTopicGetsFreshController`: the controller's; nothing client-side.

### `V5ScalableTopicStatsTest`

The broker's. 5 tests: 5 the broker's. The scalable-topic stats API: the topic-level snapshot (DAG, producers,
subscriptions, backlog), per-segment stats, meters that queries leave alone, legacy segments, the split DAG, Stream
sessions. The names in it come from the client, which Rust checks.

- The broker's:
  - `testTopicStatsAggregateAcrossSegments`: [`producer.rs`][tk-producer] `sends_a_stamped_batch_of_one_per_message`,
    [`queue_consumer.rs`][tk-queue] `each_segment_consumer_gets_the_options`, [`layout.rs`][p-layout]
    `names_segments_like_java` check the client-side names (segment producers and consumers, segment topics).
  - `testStatsQueriesPreserveClassicTrafficCountersAndRates`: broker meters; no client.
  - `testStatsOfMigratedTopicIncludeLegacySegments`: stats.
  - `testStatsReflectSplitDag`: stats. The children's names as the client builds them: [`layout.rs`][p-layout]
    `names_segments_like_java`.
  - `testStreamConsumerSessionsInStats`: [`stream_consumer.rs`][p-stream]
    `registers_as_a_stream_consumer_named_after_it` checks the registration under the consumer's name.

### `V5SchemaEvolutionTest`

Ported. 3 tests: 3 covered. A scalable topic has one schema history, shared by its segments across a split: compatible
versions join it, incompatible ones are refused.

- Driver: [`schema_tests.rs`][d-schema] `a_topic_has_one_schema_and_its_messages_carry_the_version`,
  `avro_consumers_read_each_version_with_its_schema`.
- Against 5.0.1: [`schemas.rs`][it-schemas] `avro_records_evolve_across_a_split`.

### `V5SchemaRoundtripTest`

Ported. 12 tests: 12 covered. A value of each `Schema.*` factory through a producer and a Queue consumer of the same
schema. PLAN.md's M7 lists these schemas as done, typed and as generic records.

- Unit: [`schema.rs`][d-schema-types] `bytes_and_text`, `primitives_encode_as_java_does`.
- Driver: [`schema_tests.rs`][d-schema] `a_topic_with_data_and_no_schema_refuses_a_typed_consumer`,
  `values_of_each_schema_reach_a_consumer`, `records_reach_a_consumer`,
  `protobuf_messages_described_in_avro_reach_a_consumer`; [`consumer_tests.rs`][d-consumer]
  `receives_what_a_producer_sends_and_acknowledges_it`.
- Against 5.0.1: [`schemas.rs`][it-schemas] `primitive_values_cross_between_java_and_rust`,
  `records_cross_between_java_and_rust`, `protobuf_messages_cross_between_java_and_rust`.

### `V5SeekSubscriptionTest`

The broker's. 3 tests: 3 the broker's. Admin seek by timestamp and clear-backlog over a scalable subscription's segment
cursors; the Queue consumer only observes the result. The Rust client has no seek or admin API; redelivery after an
admin seek is covered for the Stream consumer only, and no Queue consumer test rewinds.

- The broker's:
  - `testSeekRewindsCursorAcrossSingleSegment`: the per-segment cursor reset by timestamp. The redelivery is covered for
    a Stream consumer, not a Queue one: [`main.rs`][it-main] `a_rewound_stream_subscription_takes_the_same_ack_again`
    (`pulsar-admin scalable-topics seek`), [`stream_consumer.rs`][tk-stream]
    `a_rewound_subscription_takes_the_same_ack_again`.
  - `testSeekToleratesEmptyChildSegmentsAfterSplit`: a regression in the seek over empty ledgers; the client sees
    nothing.
  - `testClearBacklogDropsAllUndeliveredMessages`: the clear-backlog operation; the client part is an ordinary subscribe
    and receive.

### `V5SegmentLoadReporterTest`

The broker's. 4 tests: 4 the broker's. The broker's segment load reports (PIP-483) and segment-owner metrics; the client
only produces.

- The broker's:
  - `testSweepWritesSegmentLoadRecords`: metadata, metrics.
  - `testOwnerMetricsObserveControllerAndTerminatedSegment`: metrics.
  - `testMigrationTerminationTracksLegacyOwnerWithoutPolling`: metrics (a v4 producer only).
  - `testNonSegmentTopicsProduceNoLoadRecords`: the sweep (a v4 producer only).

### `V5SegmentMergeTest`

Ported. 1 test: 1 covered. A merge of two active segments while a producer sends and a Queue consumer receives: nothing
lost or duplicated.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `follows_splits_and_merges_without_losing_a_message`;
  [`producer.rs`][tk-producer] `per_key_order_holds_across_a_merge_with_traffic`.
- Against 5.0.1: [`conformance.rs`][it-conformance] `split_and_merge_under_load`.

### `V5SegmentRebucketTest`

Ported. 2 tests: 1 covered and 1 the broker's. The admin rebucket rollover (the segment seals; a successor with the same
range and a new bucket count takes over): a producer and an ordered Stream consumer across it, and the endpoint's
validation. Ported against 5.0.1, through the admin endpoint, which `pulsar-admin` 5.0.1 has no command for.

- Against 5.0.1: [`buckets.rs`][it-buckets] `keys_keep_their_order_across_a_rebucket`.
- The broker's: `testRebucketRejectsInvalidRequests`: admin REST validation; the Rust client has no admin API.

### `V5SegmentReconciliationTest`

The broker's. 4 tests: 4 the broker's. The broker's rule for auto-creating a `segment://` topic on connect, only while
the layout lists the segment as active, tested by calling `BrokerService.isAllowAutoTopicCreationAsync` directly: no
client is involved.

- The broker's:
  - `testActiveSegmentIsAutoCreatable`: the broker's auto-create gate, called directly.
  - `testSealedSegmentIsNotAutoCreatable`: the same gate. Rust's consumers meet its outcome, a sealed segment's deleted
    topic staying gone, in [`queue_consumer.rs`][tk-queue]
    `a_segment_deleted_before_the_layout_drops_it_is_retried_until_the_layout_comes` and
    [`checkpoint_consumer.rs`][tk-checkpoint] `a_segment_deleted_while_read_is_skipped`.
  - `testUnknownSegmentIsNotAutoCreatable`, `testSegmentUnderMissingParentIsNotAutoCreatable`: the same gate.

### `V5SegmentSplitTest`

Ported. 1 test: 1 covered. A split while a producer sends and a Queue consumer receives: nothing lost or duplicated.

- Simulated cluster: [`queue_consumer.rs`][tk-queue] `follows_splits_and_merges_without_losing_a_message`;
  [`producer.rs`][tk-producer] `per_key_order_holds_across_a_split_with_traffic`.
- Against 5.0.1: [`main.rs`][it-main] `a_split_during_consumption_loses_no_message`; [`conformance.rs`][it-conformance]
  `split_and_merge_under_load`.

### `V5SmokeTest`

Ported. 1 test: 1 covered. The longest V5 path, end to end, on a one-segment topic.

- Driver: [`consumer_tests.rs`][d-consumer] `receives_what_a_producer_sends_and_acknowledges_it`.
- Against 5.0.1: [`main.rs`][it-main] `a_payload_reads_back_with_each_codec`.

### `V5StreamConsumerAckWatermarkTest`

Ported. 1 test: 1 covered. A cumulative ack made before the segment's delivery restarted must not suppress the re-ack of
the redelivered position; Java times the rewind with an internal hook, Rust checks the same outcome.

- Unit: [`consumer.rs`][p-consumer] `a_new_registration_takes_the_same_cumulative_ack_again`.
- Simulated cluster: [`stream_consumer.rs`][tk-stream] `a_rewound_subscription_takes_the_same_ack_again`.
- Against 5.0.1: [`main.rs`][it-main] `a_rewound_stream_subscription_takes_the_same_ack_again`.

### `V5StreamConsumerAutoReconnectTest`

Ported. 2 tests: 2 covered. The Stream consumer's controller session reconnects on its own, and within the grace period
keeps its assignment.

- Unit: [`stream_consumer.rs`][p-stream] `registers_again_after_a_connection_loss`.
- Simulated cluster: [`stream_consumer.rs`][tk-stream] `a_member_comes_back_after_a_connection_loss`,
  `within_the_grace_period_the_assignment_holds`.
- Driver: [`stream_tests.rs`][d-stream] `a_lost_connection_registers_again`.
- Against 5.0.1: [`conformance.rs`][it-conformance] `failover_by_unload`, `failover_by_broker_restart`.

### `V5StreamConsumerBasicTest`

Ported. 4 tests: 4 covered. A Stream consumer's basics on one segment: ordered receive with a cumulative ack, batch
receive, receive timeout, accessors.

- Simulated cluster: [`stream_consumer.rs`][tk-stream] `receives_in_order_and_acknowledges_cumulatively`.
- Driver: [`stream_tests.rs`][d-stream] `receives_with_a_timeout_and_in_batches`,
  `receives_in_order_and_acknowledges_cumulatively`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively`.

### `V5StreamConsumerDagReplayTest`

Ported. 3 tests: 3 covered. A new Earliest Stream subscription is assigned sealed segments too, and reads their backlog
once; the controller hands out the children once the parent is drained.

- Simulated cluster: [`stream_consumer.rs`][tk-stream] `an_earliest_subscription_reads_a_sealed_parent_first`,
  `keys_keep_their_order_across_two_splits`, `a_new_member_gets_nothing_acknowledged_again`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_sealed_backlog_first_in_key_order`.

### `V5StreamConsumerOrderingTest`

Ported. 2 tests: 2 covered. Per-key order across a lineage of splits: no descendant is read while an ancestor has
backlog, an empty segment included.

- Simulated cluster: [`stream_consumer.rs`][tk-stream] `keys_keep_their_order_across_two_splits`,
  `an_empty_segment_does_not_let_its_children_ahead_of_its_parent`, `keys_keep_their_order_across_an_empty_segment`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_sealed_backlog_first_in_key_order`.

### `V5SubscriptionInitialPositionTest`

Ported. 4 tests: 4 covered. Earliest sees earlier messages and Latest skips them, for the Queue and Stream consumers.

- Unit: [`stream_consumer.rs`][p-stream] `attaches_each_segment_in_the_mode_its_assignment_gives`.
- Simulated cluster: [`queue_consumer.rs`][tk-queue] `latest_skips_what_the_segments_had_and_earliest_does_not`;
  [`consumer.rs`][tk-consumer] `a_new_subscription_at_the_latest_skips_what_came_before`;
  [`stream_consumer.rs`][tk-stream] `receives_in_order_and_acknowledges_cumulatively`.
- Driver: [`stream_tests.rs`][d-stream] `a_consumer_at_latest_skips_what_was_stored_before`.
- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively`.

### `V5TlsProducerConsumerTest`

Partly ported. 2 tests: 1 covered and 1 partly covered. mTLS produce and consume against a broker that requires a
trusted client certificate, the certificate given in the TLS policy, without and with the `tls` auth method. Rust takes
a client certificate only from an `Authentication`: `TlsAuthentication`, which names the `tls` method.

- Driver: [`options.rs`][d-options] `mutual_tls_presents_the_client_certificate`,
  `tls_1_2_brokers_with_relaxed_verification`.
- Against 5.0.1: [`main.rs`][it-main] `tls_with_client_certificate`.
- Partly covered: `testV5MtlsViaTlsPolicy`: the mTLS handshake is covered, with the certificate from
  `TlsAuthentication`; the client has no TLS option for a certificate apart from authentication.

## `pulsar-broker`: scalable topics' admin and broker services

Broker tests that drive a scalable topic through the V5 client: permissions, migration, legacy segments, namespace
deletes and schemas. Most of what they check is the broker's.

### `ScalableTopicLegacySegmentTest`

Ported. 13 tests: 2 covered and 11 the broker's. A migrated regular topic kept as sealed legacy parent segments. 2 of 13
methods are client scenarios, both covered; the other 11 are the controller's GC, deletes and subscription admin
reaching the legacy `persistent://` topics. All setups but the seek test's have a V5 producer follow the migration,
which Rust covers ([`producer.rs`][tk-producer] `per_key_order_holds_across_a_migration_with_traffic`).

- Simulated cluster: [`stream_consumer.rs`][tk-stream] `reads_a_migrated_topics_legacy_segments_first`.
- Against 5.0.1: [`bridge.rs`][it-bridge] `a_migrated_topic_is_read_legacy_segments_first`.
- The broker's:
  - `testGcDeletesPrunedLegacyTopic`: controller GC, metadata, schema registry.
  - `testGcDeletesPrunedLegacyPartitions`, `testGcPrunesDrainedLegacyParent`, `testGcKeepsLegacyParentWithBacklog`,
    `testGcPrunesLegacyParentWhoseTopicWasDeleted`: controller GC.
  - `testGcKeepsLegacyParentWithBacklogOfSubscriptionFromBeforeTheMigration`: controller GC (the check J16 asks for on
    managed segments too).
  - `testDeleteScalableTopicDeletesLegacyTopic`, `testDeleteScalableTopicDeletesLegacyPartitions`: admin delete.
  - `testClearBacklogSkipsLegacyBacklog`, `testSeekResetsLegacyCursorToTimestamp`,
    `testDeleteSubscriptionDeletesLegacyCursor`: admin.

### `ScalableTopicMigrationPermissionsTest`

The broker's. 5 tests: 5 the broker's. The broker's migration copies a regular topic's own grants to its `topic://`
name, under token authentication and default authorization; the V5 client only produces and consumes. Rust's 5.0.1
broker runs with authorization off, so only the client's side is covered, without grants: following a migration, and
reaching the migrated topic by its `topic://` name.

- The broker's:
  - `testProducerKeepsProducingThroughMigration`: [`bridge.rs`][it-bridge] `consumers_follow_a_migration`,
    [`bridge.rs`][it-bridge] `a_producer_keeps_key_order_across_a_migration` cover the client side (the producer follows
    the migration; a later Queue consumer reads the legacy, then the new segments) without authorization. The grant copy
    is the broker's.
  - `testMigratedTopicIsAccessibleThroughItsScalableName`: the grants on the `topic://` name are the broker's. Without
    them, a producer and a Queue consumer reach a migrated topic by its `topic://` name, and read what was stored before
    the migration ([`bridge.rs`][it-bridge] `a_migrated_topic_is_reached_by_its_scalable_name`).
  - `testMigrationCopiesOnlyThePermissionsOfTheTopic`: admin `getPermissions` only; no client.
  - `testCreationIsRefusedWhileAMigrationCopiesPermissions`: admin and the creation lock; no client.
  - `testMigrationToAnExistingScalableTopicGrantsNothingOnIt`: admin only; no client.

### `ScalableTopicNamespaceDeleteTest`

The broker's. 7 tests: 7 the broker's. Namespace and tenant deletes with scalable topics: refused without force; forced,
they remove the records, segment ledgers and schemas. The V5 client only publishes, and in one test reads.

- The broker's:
  - `testDeleteNamespaceWithScalableTopicIsRejected`, `testForceDeleteNamespaceWithSameNamedNonPersistentTopic`: admin.
  - `testForceDeleteNamespaceDeletesScalableTopic`: admin, metadata, schema registry.
  - `testDeleteNamespaceDeletesSegmentTopicsLeftBehind`: metadata.
  - `testRecreatedNamespaceDoesNotResurrectScalableTopic`: the old schema and data are gone; the client side is a plain
    produce and earliest read.
  - `testForceDeleteTenantDeletesScalableTopic`, `testForceDeleteNamespaceDeletesMigratedTopic`: admin, metadata.

### `ScalableTopicNamespaceDeleteZooKeeperTest`

The broker's. 2 tests: 2 the broker's. The same namespace deletes on ZooKeeper, which refuses to delete a node that has
children.

- The broker's: `testForceDeleteNamespaceWithScalableTopic`, `testDeleteNamespaceAfterDeletingItsScalableTopic`: admin,
  metadata.

### `ScalableTopicPermissionsTest`

The broker's. 10 tests: 10 the broker's. Authorization of the `segment://` producers and consumers by the grants on the
`topic://` name, and the scalable-topic permission endpoints. The decisions are the broker's and need authorization on,
which Rust's 5.0.1 broker lacks; Rust covers the typed errors they produce, on the simulated broker.

- The broker's:
  - `testTopicPermissionsAuthorizeProduceAndConsume`: [`main.rs`][it-main]
    `a_queue_consumer_reads_and_acknowledges_every_segment`, [`main.rs`][it-main]
    `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively` cover the client's side, without authorization.
  - `testNoTopicPermissionsCannotProduceOrConsume`: the authorization is the broker's. The client's side is covered: a
    create or subscribe whose topic lookup is refused fails with `Authorization` ([`producer_tests.rs`][d-producer]
    `a_refused_lookup_fails_create`, [`consumer_tests.rs`][d-consumer] `a_refused_lookup_fails_the_subscribe`,
    [`stream_tests.rs`][d-stream] `a_refused_lookup_fails_the_subscribe`, [`checkpoint_tests.rs`][d-checkpoint]
    `a_refused_lookup_fails_the_create`), and the lookup isn't retried ([`clients.rs`][d-clients]
    `a_lookup_the_broker_refuses_is_not_retried`).
  - `testSegmentsAllowOnlyTheActionsGrantedOnTheTopic`: [`producer_edges.rs`][tk-producer-edges]
    `registration_failures`, [`producer_tests.rs`][d-producer] `a_refused_segment_lookup_fails_the_send`,
    [`consumer_tests.rs`][d-consumer] `a_refused_subscribe_fails_with_the_brokers_error_and_closes_the_rest` cover the
    client outcomes on the simulated broker: a segment registration refused with `AuthorizationError` fails the send
    (the driver test through a refused segment lookup, kind `Authorization`), and a refused segment subscribe fails the
    Queue subscribe with `Authorization`.
  - `testSegmentsCreatedBySplitAreAuthorizedWithTheTopicPermissions`: [`main.rs`][it-main]
    `a_split_during_consumption_loses_no_message`, [`queue_consumer.rs`][tk-queue]
    `follows_splits_and_merges_without_losing_a_message` cover the client side, without authorization.
  - `testGrantGetAndRevokePermissions`: the admin endpoints. The create's refusal after the revoke is the refused topic
    lookup of `testNoTopicPermissionsCannotProduceOrConsume`, covered there.
  - `testGrantGetAndRevokeRolesWithReservedCharacters`: admin REST encoding; no client.
  - `testRevokeDisconnectsSegmentProducers`, `testRevokeDisconnectsSegmentConsumers`: stats only; no client-side
    assertion.
  - `testPermissionsOfMissingTopic`, `testDeleteRemovesTopicPermissions`: admin only; no client.

### `ScalableTopicRegularTopicDeleteSchemaTest`

The broker's. 6 tests: 6 the broker's. A migrated `persistent://` topic and a same-named `non-persistent://` one share
the scalable topic's schema id: deleting them must keep the schema, which only the scalable topic's delete removes.
Schema registry behaviour; the client only produces.

- The broker's:
  - `testDeleteMigratedTopicKeepsSchema`, `testDeleteUnloadedMigratedTopicKeepsSchema`,
    `testDeleteMigratedPartitionedTopicKeepsSchema`, `testDeleteNonPersistentTopicKeepsSchema`,
    `testDeleteScalableTopicDeletesSchemaKeptByMigratedTopicDelete`: schema registry.
  - `testInactiveTopicGcOfMigratedTopicKeepsSchema`: topic GC, schema registry.

### `ScalableTopicSegmentDeleteSchemaTest`

The broker's. 3 tests: 3 the broker's. A segment's delete or garbage collection keeps the topic-wide schema, and the
topic's delete removes it. One client outcome rides along, a Checkpoint consumer skipping a deleted parent, which Rust
covers.

- The broker's:
  - `testDeleteScalableTopicDeletesSchema`: schema registry.
  - `testDeleteSegmentKeepsTopicSchema`: [`checkpoint_consumer.rs`][tk-checkpoint]
    `a_deleted_parent_does_not_hold_its_children`, [`main.rs`][it-main] `a_checkpoint_consumer_skips_a_deleted_segment`
    cover the client outcome: a segment the layout still lists, whose topic is gone, is skipped (gap 26).
  - `testGcPruneKeepsTopicSchema`: GC and schema registry. The client reads a layout without the parent, nothing
    specific; a prune while reading is [`checkpoint_consumer.rs`][tk-checkpoint]
    `lets_go_of_a_segment_the_layout_drops_while_another_reader_subscribes`.

## Tools and packaging

The command-line client, and the tests of how the Java client is packaged.

### `AdminV5MessagingTest`

Ported. 1 test: 1 covered. A packaging test: the admin and V5 clients in one process against a broker container. Its
admin half (a tenant, namespace policies read through Jackson) is Java packaging; its messaging half is client behaviour
Rust has.

- Against 5.0.1: [`main.rs`][it-main] `a_stream_consumer_reads_in_order_and_acknowledges_cumulatively`.

### `ClasspathTest`

Java-only. 1 test: 1 Java-only. Shading of the admin-v5 artifact: Java packaging.

- Java-only: `implementationsAreIsolated`: shading.

### `ClientClasspathTest`

Java-only. 1 test: 1 Java-only. The aggregate client jar, shaded or not: Java packaging.

- Java-only: `aggregateSupportsV4V5AndAdmin`: shading.

### `ClientJavaCompatibilityTest`

Java-only. 3 tests: 3 Java-only. The client, admin, CLI, WebSocket, Functions and IO artifacts load and run on the
client's minimum Java version: Java packaging.

- Java-only:
  - `loadClientImplementationsOnClientJavaVersion`, `loadClientToolsOnClientJavaVersion`: Java version compatibility.
  - `runUserFunctionAndSinkOnClientJavaVersion`: Java version compatibility; Pulsar Functions are out of scope anyway.

### `PulsarClientToolTest`

Java-only. 8 tests: 8 Java-only. The `pulsar-client` CLI's translation of `client.conf` keys into a `TlsPolicy`: tooling
and configuration plumbing, and Rust has no CLI. Where a client rule lies behind a test, the note names the Rust test of
it.

- Java-only:
  - `testHostnameVerificationDefaultsOnWhenConfKeyAbsent`: CLI conf translation. Rust's defaults (verification on,
    insecure off) are checked by [`tests.rs`][d-tests] `tls_verification_options`.
  - `testExplicitHostnameVerificationValueWins`: CLI conf translation. Turning verification off is checked by
    [`tests.rs`][d-tests] `tls_verification_options`.
  - `testNoTlsPolicyForPlaintextUrl`: CLI conf translation.
  - `testPemBranchUsesConfCertAndKeyPaths`: CLI conf translation; Rust takes the same files through
    `tls_trust_certs_file` and `TlsAuthentication::from_pem_files`.
  - `testKeyStoreTlsTranslatesToKeystorePolicy`: Java keystores; Rust reads PEM only.
  - `testKeyStoreTypesDefaultToJksLikeV4`: Java keystores.
  - `testJsseProviderConfKeyIsHonored`: JSSE provider; Rust uses rustls.
  - `testJsseProviderConfKeyIsHonoredForKeystoreFormat`: JSSE provider.

### `TestCmdProduce`

Java-only. 3 tests: 3 Java-only. The CLI's `produce` command: its WebSocket URL, its `--value-schema` parsing and its
JSON-to-Avro conversion. Rust has no CLI.

- Java-only:
  - `testGetWebSocketProduceUri`: CLI over the WebSocket API.
  - `testBuildValueSchema`: CLI argument parsing; Rust has no `autoProduceBytes` (PLAN.md M7).
  - `generateAvroMessageBodies`: CLI tooling.

## Out of scope

The classes whose tests are all out of scope, by what they test:

- Transactions: `SegmentSubscriptionDeleteTxnAckRecordsTest` (1), `ScalableTopicSegmentDeleteTxnStateTest` (8),
  `V5TransactionPublishOrderTest` (1), `V5TransactionRecoveryTest` (4), `V5TransactionScalableTest` (8),
  `V5TransactionTest` (10), `V5TransactionTimeoutTest` (1), `PerformanceTransactionTest` (3),
  `Oauth2PerformanceTransactionTest` (1), `TcMetadataDiscoveryTest` (3), `V5ScalableTopicTransactionTest` (6).
- Dead-letter policy: `V5DeadLetterPolicyTest` (4).
- End-to-end encryption: `V5EncryptionTest` (4), `EncryptionPolicyTest` (9), `PemFileKeyProviderTest` (5).
- Namespace subscription: consumers of several topics or of a namespace, and the namespace watch behind them:
  `V5MultiTopicQueueConsumerTest` (3), `V5MultiTopicStreamConsumerTest` (5), `V5NamespaceConsumerTopicAttachTest` (6),
  `V5NamespaceConsumerTopicChangeDuringCreateTest` (3), `V5NamespaceConsumerTopicDeletionTest` (3),
  `V5NamespaceStreamConsumerTopicDeletionTest` (3), `ScalableTopicsWatcherTest` (8), `V5ScalableTopicsWatcherTest` (4).
- Pulsar Functions and IO: `PulsarFunctionV5E2ETest` (3), `PulsarSourceV5E2ETest` (3), `ContextImplTest` (29),
  `LazyPulsarClientV5Test` (2), `V5MessageIdAdapterTest` (1), `V5ProducerAdapterTest` (5), `V5ProducerFactoryTest` (5).
- The proxy: `ProxyScalableTopicsTest` (1), `SslContextFallbackSynthesisTest` (4).
- SASL: `SaslAuthenticationV5BinaryOffloadTest` (1), `SaslAuthenticationV5HttpTest` (4).
- HTTP authentication, for HTTP lookups and the admin: the client speaks the binary protocol only
  ([1](../docs/1-scope.md) §5): `HttpAuthenticationDriverTest` (10).

And without tests of their own: the base classes `V5ClientBaseTest`, `V5MultiBrokerClientBaseTest` and
`AbstractPulsarE2ETest`; `Examples`, the API's usage examples, which only compile; and `PulsarClientToolForceBatchNum`,
a helper of the command-line client's tests.

[d-checkpoint]: ../crates/apache-pulsar/src/checkpoint_tests.rs
[d-checkpoint-type]: ../crates/apache-pulsar/src/checkpoint.rs
[d-clients]: ../crates/apache-pulsar/src/tests/clients.rs
[d-consumer]: ../crates/apache-pulsar/src/consumer_tests.rs
[d-consumer-task]: ../crates/apache-pulsar/src/consumer/task.rs
[d-driver]: ../crates/apache-pulsar/src/tests/driver.rs
[d-errors]: ../crates/apache-pulsar/src/tests/errors.rs
[d-generic]: ../crates/apache-pulsar/src/schema/generic.rs
[d-memory]: ../crates/apache-pulsar/src/memory.rs
[d-message-id]: ../crates/apache-pulsar/src/message_id.rs
[d-options]: ../crates/apache-pulsar/src/tests/options.rs
[d-producer]: ../crates/apache-pulsar/src/producer_tests.rs
[d-producer-mod]: ../crates/apache-pulsar/src/producer/mod.rs
[d-producer-task]: ../crates/apache-pulsar/src/producer/task.rs
[d-schema]: ../crates/apache-pulsar/src/schema_tests.rs
[d-schema-types]: ../crates/apache-pulsar/src/schema.rs
[d-stream]: ../crates/apache-pulsar/src/stream_tests.rs
[d-tests]: ../crates/apache-pulsar/src/tests.rs
[it-bridge]: ../crates/apache-pulsar/tests/integration/bridge.rs
[it-buckets]: ../crates/apache-pulsar/tests/integration/buckets.rs
[it-conformance]: ../crates/apache-pulsar/tests/integration/conformance.rs
[it-main]: ../crates/apache-pulsar/tests/integration/main.rs
[it-schemas]: ../crates/apache-pulsar/tests/integration/schemas.rs
[p-checkpoints]: ../crates/apache-pulsar-protocol/tests/checkpoint_vectors.rs
[p-consumer]: ../crates/apache-pulsar-protocol/tests/consumer.rs
[p-hash]: ../crates/apache-pulsar-protocol/src/hash.rs
[p-ids]: ../crates/apache-pulsar-protocol/tests/message_id_vectors.rs
[p-layout]: ../crates/apache-pulsar-protocol/tests/layout.rs
[p-routing]: ../crates/apache-pulsar-protocol/tests/routing_vectors.rs
[p-stream]: ../crates/apache-pulsar-protocol/tests/stream_consumer.rs
[tk-batching]: ../crates/apache-pulsar-testkit/tests/producer_batching.rs
[tk-checkpoint]: ../crates/apache-pulsar-testkit/tests/checkpoint_consumer.rs
[tk-compression]: ../crates/apache-pulsar-testkit/tests/producer_compression.rs
[tk-connection]: ../crates/apache-pulsar-testkit/tests/connection.rs
[tk-consumer]: ../crates/apache-pulsar-testkit/tests/consumer.rs
[tk-group]: ../crates/apache-pulsar-testkit/tests/checkpoint_group.rs
[tk-lookup]: ../crates/apache-pulsar-testkit/tests/lookup.rs
[tk-producer]: ../crates/apache-pulsar-testkit/tests/producer.rs
[tk-producer-edges]: ../crates/apache-pulsar-testkit/tests/producer_edges.rs
[tk-queue]: ../crates/apache-pulsar-testkit/tests/queue_consumer.rs
[tk-stream]: ../crates/apache-pulsar-testkit/tests/stream_consumer.rs
[tk-world]: ../crates/apache-pulsar-testkit/tests/world.rs
