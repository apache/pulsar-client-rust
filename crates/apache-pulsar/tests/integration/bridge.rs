// Licensed to the Apache Software Foundation (ASF) under one
// or more contributor license agreements.  See the NOTICE file
// distributed with this work for additional information
// regarding copyright ownership.  The ASF licenses this file
// to you under the Apache License, Version 2.0 (the
// "License"); you may not use this file except in compliance
// with the License.  You may obtain a copy of the License at
//
//   http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing,
// software distributed under the License is distributed on an
// "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
// KIND, either express or implied.  See the License for the
// specific language governing permissions and limitations
// under the License.

//! The migration bridge (docs/6-scalable-topics.md §8): Rust clients on regular `persistent://` topics, through the
//! synthetic layouts the broker gives them, beside Java v4 clients, before, while and after the broker migrates a
//! topic to a scalable one.
//!
//! The v4 clients are `pulsar-client consume` and `pulsar-perf produce`, running in the broker container while the
//! Rust clients work.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::time::{Duration, Instant};

use apache_pulsar::{Checkpoint, ErrorKind, Message, MessageId, Schema, SubscriptionInitialPosition};
use apache_pulsar_protocol::hash::KeyHash;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;

use super::{
    Env, SUB, by_key, checkpoint_member, drain, keyed, proxy_client, publish_keyed, queue_consumer, read, receive_all,
    run, sorted_values, stats, stream_all, stream_consumer, unique, wait_for,
};

pub(super) const TESTS: &[(&str, fn(&Env))] = &[
    ("v4_and_rust_clients_share_a_regular_topic", v4_and_rust_clients_share_a_regular_topic),
    ("v4_and_rust_consumers_share_a_subscription", v4_and_rust_consumers_share_a_subscription),
    ("rust_clients_on_a_regular_topic_are_marked_v5_managed", rust_clients_on_a_regular_topic_are_marked_v5_managed),
    ("a_v4_client_holds_off_a_migration_unless_forced", a_v4_client_holds_off_a_migration_unless_forced),
    ("a_checkpoint_group_needs_a_migrated_topic", a_checkpoint_group_needs_a_migrated_topic),
    ("a_producer_keeps_key_order_across_a_migration", a_producer_keeps_key_order_across_a_migration),
    ("consumers_follow_a_migration", consumers_follow_a_migration),
    ("a_migrated_topic_is_read_legacy_segments_first", a_migrated_topic_is_read_legacy_segments_first),
    ("v4_and_rust_clients_across_a_forced_migration", v4_and_rust_clients_across_a_forced_migration),
];

/// What a broker should do and 5.0.1 doesn't: run with `--ignored`.
pub(super) const IGNORED: &[(&str, fn(&Env))] =
    &[("a_stream_consumer_reads_a_regular_topic", a_stream_consumer_reads_a_regular_topic)];

/// A regular topic of its own, with `partitions` partitions, or none. Returns its `persistent://` name, and the
/// `tenant/namespace/name` that the scalable-topic commands take.
fn regular_topic(env: &Env, name: &str, partitions: u32) -> (String, String) {
    let name = unique(name);
    let topic = format!("persistent://public/default/{name}");
    match partitions {
        0 => env.admin(&["topics", "create", &topic]),
        n => env.admin(&["topics", "create-partitioned-topic", "-p", &n.to_string(), &topic]),
    };
    (topic, format!("public/default/{name}"))
}

/// The stats of each topic that `topic` stores in: its partitions, or itself.
fn stored_in(env: &Env, topic: &str, partitions: u32) -> Vec<serde_json::Value> {
    if partitions == 0 {
        return vec![stats(env, &["topics", "stats", topic])];
    }
    let all = stats(env, &["topics", "partitioned-stats", "--per-partition", topic]);
    (0..partitions).map(|i| all["partitions"][format!("{topic}-partition-{i}")].clone()).collect()
}

/// How many messages the topics `topic` stores in took.
fn messages_in(env: &Env, topic: &str, partitions: u32) -> usize {
    stored_in(env, topic, partitions).iter().map(|s| s["msgInCounter"].as_u64().unwrap() as usize).sum()
}

/// How many consumers `subscription` has in a topic's stats.
fn consumers(stats: &serde_json::Value, subscription: &str) -> usize {
    stats["subscriptions"][subscription]["consumers"].as_array().map_or(0, Vec::len)
}

/// Whether every topic `topic` stores in has a consumer on `subscription`.
fn attached(env: &Env, topic: &str, partitions: u32, subscription: &str) -> bool {
    stored_in(env, topic, partitions).iter().all(|s| consumers(s, subscription) == 1)
}

/// Whether every topic `topic` stores in has a consumer on `subscription`, and has no message left for it.
fn attached_and_drained(env: &Env, topic: &str, partitions: u32, subscription: &str) -> bool {
    stored_in(env, topic, partitions)
        .iter()
        .all(|s| consumers(s, subscription) == 1 && s["subscriptions"][subscription]["msgBacklog"] == 0)
}

/// The metadata of every producer and consumer in a topic's stats.
fn client_metadata(stats: &serde_json::Value) -> Vec<serde_json::Value> {
    let publishers = stats["publishers"].as_array().into_iter().flatten();
    let subscriptions = stats["subscriptions"].as_object().into_iter().flat_map(|s| s.values());
    let consumers = subscriptions.flat_map(|s| s["consumers"].as_array().into_iter().flatten());
    publishers.chain(consumers).map(|client| client["metadata"].clone()).collect()
}

/// Polls `condition` for up to a minute: a Java client takes a few seconds to start.
fn eventually(what: &str, condition: impl Fn() -> bool) {
    let deadline = Instant::now() + Duration::from_secs(60);
    while !condition() {
        assert!(Instant::now() < deadline, "timed out: {what}");
        std::thread::sleep(Duration::from_millis(250));
    }
}

/// The segment a message was read from: the first part of its id, `segment:ledger:entry:index`.
fn segment_of<T>(message: &Message<T>) -> u64 {
    segment(message.id())
}

/// The segment of a message id: the first part of `segment:ledger:entry:index`.
fn segment(id: &MessageId) -> u64 {
    id.to_string().split(':').next().unwrap().parse().unwrap()
}

/// The partition a v4 producer sends `key` to: on a synthetic layout, the id of the legacy segment.
fn partition_of(key: &str, partitions: u32) -> u64 {
    if partitions == 0 { 0 } else { u64::from(KeyHash::of(key.as_bytes()).partition(partitions)) }
}

fn text(messages: &[Message<Vec<u8>>]) -> Vec<String> {
    messages.iter().map(|m| String::from_utf8(m.value().unwrap()).unwrap()).collect()
}

/// Java's `V5RegularTopicInteropTest`, with the Java clients running throughout: on a regular topic, partitioned and
/// not, a v4 producer and a v4 consumer run beside a Rust producer, Queue consumer and Checkpoint consumer. Every
/// consumer gets every message both producers stored, and the Rust producer sends each key to the partition a v4
/// producer would.
fn v4_and_rust_clients_share_a_regular_topic(env: &Env) {
    for partitions in [0, 3] {
        let (topic, _) = regular_topic(env, "side-by-side", partitions);
        let v4_consumer = env.v4_consumer(&topic, "v4", "Exclusive");
        eventually("the v4 consumer attaches", || attached(env, &topic, partitions, "v4"));
        let v4_producer = env.v4_producer(&topic);
        let publishing = |s: &serde_json::Value| s["publishers"].as_array().is_some_and(|p| !p.is_empty());
        eventually("the v4 producer attaches", || stored_in(env, &topic, partitions).iter().all(publishing));
        let (stored, sent) = run(async {
            let client = proxy_client(env).build().await.unwrap();
            let queue = queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
            let checkpoint = client.new_checkpoint_consumer(Schema::bytes()).topic(&topic);
            let checkpoint = checkpoint.start_position(Checkpoint::earliest()).create().await.unwrap();
            let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
            let sent: BTreeMap<String, String> = (0..30).map(|i| (format!("rust-{i:02}"), format!("k{i}"))).collect();
            for (value, key) in &sent {
                producer.new_message().key(key.clone()).value(value.clone().into_bytes()).send().await.unwrap();
            }
            env.stop(v4_producer);
            let stored = messages_in(env, &topic, partitions);
            assert!(stored > sent.len(), "{partitions} partitions: the v4 producer stored nothing ({stored} in all)");
            let from_queue = receive_all(&queue, stored).await;
            assert_eq!(from_queue.len(), stored, "{partitions} partitions");
            for message in from_queue.iter().filter(|m| m.key().is_some()) {
                let key = message.key().unwrap();
                assert_eq!(segment_of(message), partition_of(key, partitions), "{partitions} partitions, key {key}");
            }
            let read_by_queue: BTreeSet<String> = text(&from_queue).into_iter().collect();
            assert!(sent.keys().all(|value| read_by_queue.contains(value)), "{partitions} partitions");
            assert_eq!(text(&read(&checkpoint, stored).await).len(), stored, "{partitions} partitions");
            assert!(checkpoint.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());
            client.close().await.unwrap();
            (stored, sent)
        });
        eventually("the v4 consumer reads everything", || attached_and_drained(env, &topic, partitions, "v4"));
        let from_v4 = env.stop(v4_consumer);
        assert_eq!(from_v4.len(), stored, "{partitions} partitions");
        let read_by_v4: BTreeSet<&str> = from_v4.iter().map(|m| m.value.as_str()).collect();
        assert!(sent.keys().all(|value| read_by_v4.contains(value.as_str())), "{partitions} partitions");
    }
}

/// A v4 consumer and a Rust Queue consumer on one Shared subscription of a partitioned topic, as while an
/// application's consumers move to the V5 client one by one: each message goes to one of them.
fn v4_and_rust_consumers_share_a_subscription(env: &Env) {
    let partitions = 2;
    let (topic, _) = regular_topic(env, "shared-subscription", partitions);
    let v4_consumer = env.v4_consumer(&topic, SUB, "Shared");
    eventually("the v4 consumer attaches", || attached(env, &topic, partitions, SUB));
    let sent: BTreeSet<String> = (0..100).map(|i| format!("v-{i:03}")).collect();
    let from_rust = run(async {
        let client = proxy_client(env).build().await.unwrap();
        let consumer = queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
        let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        for (i, value) in sent.iter().enumerate() {
            producer.new_message().key(format!("k{i}")).value(value.clone().into_bytes()).send().await.unwrap();
        }
        // Until none comes for a while: the others went to the v4 consumer.
        let mut got = Vec::new();
        while let Some(message) = consumer.receive_timeout(Duration::from_secs(3)).await.unwrap() {
            consumer.acknowledge(message.id()).await.unwrap();
            got.push(message);
        }
        client.close().await.unwrap();
        text(&got)
    });
    eventually("the v4 consumer acknowledges the rest", || attached_and_drained(env, &topic, partitions, SUB));
    let from_v4: Vec<String> = env.stop(v4_consumer).into_iter().map(|m| m.value).collect();
    assert!(!from_rust.is_empty() && !from_v4.is_empty(), "rust {} v4 {}", from_rust.len(), from_v4.len());
    let mut all = from_rust;
    all.extend(from_v4);
    assert_eq!(all.len(), sent.len(), "each message goes to one consumer");
    assert_eq!(all.into_iter().collect::<BTreeSet<_>>(), sent);
}

/// Every producer and consumer the Rust client attaches to a regular topic carries `__pulsar.v5.managed=true`, by
/// which the broker's migration tells it from a v4 client: with only those attached, a topic migrates without
/// `--force`. The Stream consumer and the members of a Checkpoint consumer group can't use a regular topic on 5.0.1
/// (spec gap 28).
fn rust_clients_on_a_regular_topic_are_marked_v5_managed(env: &Env) {
    for partitions in [0, 2] {
        let (topic, path) = regular_topic(env, "marked", partitions);
        run(async {
            let client = proxy_client(env).build().await.unwrap();
            let queue = queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
            let checkpoint = client.new_checkpoint_consumer(Schema::bytes()).topic(&topic).create().await.unwrap();
            let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
            // A segment's producer starts with its first message: one to each partition.
            for partition in 0..u64::from(partitions.max(1)) {
                let key = (0..).map(|i| format!("k{i}")).find(|key| partition_of(key, partitions) == partition);
                producer.new_message().key(key.unwrap()).value(b"marked".to_vec()).send().await.unwrap();
            }
            // In each partition: the producer, the Queue consumer, and the Checkpoint consumer's reader.
            let all_three = |s: &serde_json::Value| client_metadata(s).len() == 3;
            eventually("every client attaches", || stored_in(env, &topic, partitions).iter().all(all_three));
            for stats in stored_in(env, &topic, partitions) {
                for metadata in client_metadata(&stats) {
                    assert_eq!(metadata["__pulsar.v5.managed"], "true", "{partitions} partitions: {stats}");
                }
            }
            env.admin(&["scalable-topics", "migrate", &path]);
            drop((queue, checkpoint, producer));
            client.close().await.unwrap();
        });
    }
}

/// Java's `testMigrateRejectsLegacyV4ConnectionWithoutForce`: the broker refuses to migrate a topic while a v4 client
/// is attached to it, and the topic stays a regular one, which the Rust producer keeps writing to. Forced, the
/// migration terminates the regular topic: the v4 consumer still reads all that was stored there, and a v4 producer
/// stores nothing more (`V5MigrationEndToEndTest.testV4ProducerLockedOutAfterMigration`).
fn a_v4_client_holds_off_a_migration_unless_forced(env: &Env) {
    let partitions = 2;
    let (topic, path) = regular_topic(env, "held-off", partitions);
    let v4_consumer = env.v4_consumer(&topic, "v4", "Exclusive");
    eventually("the v4 consumer attaches", || attached(env, &topic, partitions, "v4"));
    let sent: Vec<String> = (0..30).map(|i| format!("v-{i:02}")).collect();
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        for (i, value) in sent.iter().enumerate() {
            if i == 20 {
                let refused = env.try_admin(&["scalable-topics", "migrate", &path]).unwrap_err();
                assert!(refused.contains("2 legacy v4 client connection(s) still attached"), "{refused}");
                assert!(env.try_admin(&["scalable-topics", "get-metadata", &path]).is_err(), "migrated anyway");
            }
            producer.new_message().key(format!("k{i}")).value(value.clone().into_bytes()).send().await.unwrap();
        }
        env.admin(&["scalable-topics", "migrate", "--force", &path]);
        client.close().await.unwrap();
    });
    eventually("the v4 consumer reads the regular topic to its end", || {
        attached_and_drained(env, &topic, partitions, "v4")
    });
    let mut from_v4: Vec<String> = env.stop(v4_consumer).into_iter().map(|m| m.value).collect();
    from_v4.sort();
    assert_eq!(from_v4, sent);
    let stored = messages_in(env, &topic, partitions);
    let v4_producer = env.v4_producer(&topic);
    // Refused once it is up and asks for the topic: its JVM takes a few seconds to start.
    eventually("the v4 producer is refused", || env.output(&v4_producer).contains("TopicTerminated"));
    env.stop_output(v4_producer);
    assert_eq!(messages_in(env, &topic, partitions), stored);
}

/// The members of a Checkpoint consumer group can't use a regular topic on 5.0.1: the topic's controller, which
/// registers them, serves migrated topics only (spec gap 28). The create fails.
fn a_checkpoint_group_needs_a_migrated_topic(env: &Env) {
    let (topic, _) = regular_topic(env, "group-unmigrated", 2);
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let builder = client.new_checkpoint_consumer(Schema::string()).topic(&topic).consumer_group("audit");
        let error = builder.start_position(Checkpoint::earliest()).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Other, "{error}");
        assert!(error.to_string().contains("Failed to initialize controller"), "{error}");
        client.close().await.unwrap();
    });
}

/// What the bridge should give a Stream consumer, and the 5.0.1 broker doesn't (spec gap 28): a Stream consumer reads
/// a regular topic, every key in order, as it reads a scalable one.
fn a_stream_consumer_reads_a_regular_topic(env: &Env) {
    let (topic, _) = regular_topic(env, "stream-unmigrated", 2);
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut sent = BTreeMap::new();
        publish_keyed(&producer, "v", 30, &mut sent).await;
        let consumer = stream_consumer(&client, &topic, "solo").await;
        let got = stream_all(&consumer, 30).await;
        assert_eq!(by_key(got.iter().map(|m| (m.key().unwrap().to_string(), m.value().unwrap()))), sent);
        client.close().await.unwrap();
    });
}

/// Java's `V5MigrationEndToEndTest`: the topic migrates while a producer sends, two rounds of keys always in flight. No
/// send fails, and a producer created afterwards on the same client sends to the new segments. A Stream consumer reads
/// everything after the migration, every key in the order sent: the controller hands it the new segments once it has
/// drained the legacy ones, so the order shows the producer stored each key's messages on the legacy segment first.
fn a_producer_keeps_key_order_across_a_migration(env: &Env) {
    for partitions in [0, 2] {
        let (topic, path) = regular_topic(env, "migrating-producer", partitions);
        let keys: Vec<String> = (0..8).map(|i| format!("k{i}")).collect();
        let migrated = AtomicBool::new(false);
        let mut sent = std::thread::scope(|scope| {
            let (started, migrate_now) = std::sync::mpsc::channel();
            let (migrated, path) = (&migrated, &path);
            scope.spawn(move || {
                // Once the first sends are in; the producer keeps sending meanwhile.
                migrate_now.recv().unwrap();
                env.admin(&["scalable-topics", "migrate", path]);
                migrated.store(true, Ordering::Release);
            });
            run(async {
                let client = proxy_client(env).build().await.unwrap();
                let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
                let mut sent: BTreeMap<String, Vec<String>> = BTreeMap::new();
                let mut sends = FuturesOrdered::new();
                let (mut round, mut after) = (0, 0);
                // Ten rounds after the migration, or a bound that only a stuck migration reaches.
                while after < 10 && round < 1500 {
                    if round == 5 {
                        started.send(()).unwrap();
                    }
                    for key in &keys {
                        let value = format!("{key}-{round:04}");
                        sends.push_back(producer.new_message().key(key.clone()).value(value.clone()).send());
                        sent.entry(key.clone()).or_default().push(value);
                    }
                    while sends.len() > 2 * keys.len() {
                        sends.next().await.unwrap().unwrap();
                    }
                    after += usize::from(migrated.load(Ordering::Acquire));
                    round += 1;
                    compio::time::sleep(Duration::from_millis(20)).await;
                }
                while let Some(result) = sends.next().await {
                    result.unwrap();
                }
                assert!(migrated.load(Ordering::Acquire), "the migration did not complete");
                let late = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
                let id = late.new_message().key("late").value("late".to_string()).send().await.unwrap();
                assert!(segment(&id) >= u64::from(partitions.max(1)), "{partitions} partitions: late sent to {id}");
                client.close().await.unwrap();
                sent
            })
        });
        sent.insert("late".into(), vec!["late".into()]);
        run(async {
            let client = proxy_client(env).build().await.unwrap();
            let consumer = stream_consumer(&client, &topic, "reader").await;
            let got = stream_all(&consumer, sent.values().map(Vec::len).sum()).await;
            assert_eq!(by_key(keyed(&got)), sent, "{partitions} partitions");
            client.close().await.unwrap();
        });
    }
}

/// A Queue consumer and a Checkpoint consumer attached while the topic migrates follow it: each gets every message
/// once, from the legacy segments, then from the new ones, the Checkpoint consumer every key in order. A checkpoint
/// taken before the migration resumes after it, and a Queue consumer created after the migration reads the whole topic.
fn consumers_follow_a_migration(env: &Env) {
    for partitions in [0, 2] {
        let (topic, path) = regular_topic(env, "migrating-consumers", partitions);
        run(async {
            let client = proxy_client(env).build().await.unwrap();
            let queue = queue_consumer(&client, Schema::string(), &topic, SubscriptionInitialPosition::Earliest).await;
            let checkpoint = client.new_checkpoint_consumer(Schema::string()).topic(&topic);
            let checkpoint = checkpoint.start_position(Checkpoint::earliest()).create().await.unwrap();
            let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
            let mut sent = BTreeMap::new();
            publish_keyed(&producer, "pre", 20, &mut sent).await;
            let first = keyed(&read(&checkpoint, 10).await);
            let resume_from = checkpoint.checkpoint().await.unwrap();
            env.admin(&["scalable-topics", "migrate", &path]);
            publish_keyed(&producer, "post", 20, &mut sent).await;
            let rest = keyed(&read(&checkpoint, 30).await);
            assert!(checkpoint.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());
            assert_eq!(by_key(first.iter().cloned().chain(rest.iter().cloned())), sent, "{partitions} partitions");
            let mut all: Vec<String> = sent.values().flatten().cloned().collect();
            all.sort();
            assert_eq!(sorted_values(&receive_all(&queue, all.len()).await), all, "{partitions} partitions");
            let resumed = client.new_checkpoint_consumer(Schema::string()).topic(&topic).start_position(resume_from);
            let resumed = resumed.create().await.unwrap();
            assert_eq!(by_key(keyed(&read(&resumed, rest.len()).await)), by_key(rest), "{partitions} partitions");
            let late = client.new_queue_consumer(Schema::string()).topic(&topic).subscription_name("late");
            let late = late.subscription_initial_position(SubscriptionInitialPosition::Earliest);
            let late = late.subscribe().await.unwrap();
            assert_eq!(sorted_values(&receive_all(&late, all.len()).await), all, "{partitions} partitions");
            client.close().await.unwrap();
        });
    }
}

/// Java's `testStreamConsumerReadsLegacyParentFirst` and `testCheckpointConsumerGroupReadsLegacyParent`: on a topic
/// migrated with a backlog, a Stream consumer reads the legacy segments first, with consumers there marked as managed
/// by a V5 client, then the new segments, every key in order. A member of a Checkpoint consumer group reads both.
fn a_migrated_topic_is_read_legacy_segments_first(env: &Env) {
    let partitions = 2;
    let (topic, path) = regular_topic(env, "migrated-backlog", partitions);
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut sent = BTreeMap::new();
        publish_keyed(&producer, "pre", 20, &mut sent).await;
        env.admin(&["scalable-topics", "migrate", &path]);
        publish_keyed(&producer, "post", 20, &mut sent).await;
        let stream = stream_consumer(&client, &topic, "solo").await;
        // It holds the legacy segments until it has acknowledged what they have.
        wait_for(|| attached(env, &topic, partitions, SUB), "the Stream consumer reads the legacy segments").await;
        assert_all_marked(env, &topic, partitions);
        assert_eq!(by_key(keyed(&stream_all(&stream, 40).await)), sent);
        stream.close().await.unwrap();
        // A member's readers stay on the segments assigned to it, the legacy ones included.
        let member = checkpoint_member(&client, &topic, "solo").await;
        let one_reader = |s: &serde_json::Value| client_metadata(s).len() == 1;
        let readers = || stored_in(env, &topic, partitions).iter().all(one_reader);
        wait_for(readers, "the group member's readers attach to the legacy segments").await;
        assert_all_marked(env, &topic, partitions);
        assert_eq!(drain(&member).await, sent.into_values().flatten().collect::<BTreeSet<_>>());
        client.close().await.unwrap();
    });
}

/// Asserts that every producer and consumer attached to the topics `topic` stores in is marked as managed by a V5
/// client.
fn assert_all_marked(env: &Env, topic: &str, partitions: u32) {
    for stats in stored_in(env, topic, partitions) {
        for metadata in client_metadata(&stats) {
            assert_eq!(metadata["__pulsar.v5.managed"], "true", "{stats}");
        }
    }
}

/// v4 and Rust clients side by side through a migration, forced since v4 clients are attached. The v4 consumer reads
/// what the regular topic stored up to the migration, and the v4 producer stores nothing after it. The Rust producer
/// carries on into the new segments, and the Rust Queue consumer gets every message both producers stored.
fn v4_and_rust_clients_across_a_forced_migration(env: &Env) {
    let partitions = 2;
    let (topic, path) = regular_topic(env, "forced", partitions);
    let v4_consumer = env.v4_consumer(&topic, "v4", "Exclusive");
    eventually("the v4 consumer attaches", || attached(env, &topic, partitions, "v4"));
    let v4_producer = env.v4_producer(&topic);
    let publishing = |s: &serde_json::Value| s["publishers"].as_array().is_some_and(|p| !p.is_empty());
    eventually("the v4 producer attaches", || stored_in(env, &topic, partitions).iter().all(publishing));
    let (counted, from_v4, rust) = run(async {
        let client = proxy_client(env).build().await.unwrap();
        let queue = queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
        let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        let rust: Vec<String> = (0..40).map(|i| format!("rust-{i:02}")).collect();
        for (i, value) in rust.iter().enumerate() {
            if i == 20 {
                env.admin(&["scalable-topics", "migrate", "--force", &path]);
            }
            let message = producer.new_message().key(format!("k{i}")).value(value.clone().into_bytes());
            let id = message.send().await.unwrap();
            assert_eq!(segment(&id) >= u64::from(partitions), i >= 20, "{value} sent to {id}");
        }
        // The regular topic is terminated: what it stored is final, and the v4 consumer reads all of it. Its count of
        // messages in can be greater: the broker counts a message when it takes the send, before storing it, so it
        // also counts the v4 producer's sends that the termination failed.
        let counted = messages_in(env, &topic, partitions);
        let drained = || attached_and_drained(env, &topic, partitions, "v4");
        wait_for(drained, "the v4 consumer reads the regular topic to its end").await;
        let from_v4 = env.stop(v4_consumer);
        let from_queue = text(&receive_all(&queue, from_v4.len() + 20).await);
        assert_eq!(from_queue.len(), from_v4.len() + 20);
        let read_by_queue: BTreeSet<&String> = from_queue.iter().collect();
        assert!(rust.iter().all(|value| read_by_queue.contains(value)));
        client.close().await.unwrap();
        (counted, from_v4, rust)
    });
    eventually("the v4 producer is refused", || env.output(&v4_producer).contains("TopicTerminated"));
    env.stop_output(v4_producer);
    assert_eq!(messages_in(env, &topic, partitions), counted);
    let read_by_v4: BTreeSet<&str> = from_v4.iter().map(|m| m.value.as_str()).collect();
    assert!(rust[..20].iter().all(|value| read_by_v4.contains(value.as_str())));
    assert!(rust[20..].iter().all(|value| !read_by_v4.contains(value.as_str())));
}
