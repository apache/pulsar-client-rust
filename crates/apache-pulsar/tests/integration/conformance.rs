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

//! The conformance suite against a real broker (docs/8-quality.md §2.2), from the spec's test guidance
//! (*conformance* §5). `tests/conformance/README.md` describes each case for any SDK, under the id its trial is named
//! after here: `conformance::<id>`, so that `cargo test -p apache-pulsar --test integration conformance::` runs the
//! suite. Cases that an earlier test already covers name that test rather than repeat it.

use std::collections::{BTreeMap, BTreeSet};
use std::sync::atomic::{AtomicBool, Ordering};
use std::sync::{Mutex, OnceLock};
use std::time::{Duration, Instant};

use apache_pulsar::{Checkpoint, Message, Schema, SubscriptionInitialPosition, local};
use apache_pulsar_protocol::hash::KeyHash;
use futures_util::stream::FuturesOrdered;
use futures_util::{FutureExt, StreamExt};

use super::{
    Env, a_checkpoint_round_trips_with_java_across_layout_changes, a_partitioned_topic_routes_like_v4,
    a_split_during_consumption_loses_no_message, env, keys_keep_their_order_across_a_split, proxy_client, run, stats,
    stream_backlog, stream_consumer, token_client, unique, wait_for,
};

/// The cases that run beside the other tests.
pub(super) const TESTS: &[(&str, fn(&Env))] = &[
    ("conformance::routing_across_clients", routing_across_clients),
    ("conformance::legacy_routing", a_partitioned_topic_routes_like_v4),
    ("conformance::split_during_produce", keys_keep_their_order_across_a_split),
    ("conformance::split_during_consume", a_split_during_consumption_loses_no_message),
    ("conformance::split_and_merge_under_load", split_and_merge_under_load),
    ("conformance::failover_by_unload", failover_by_unload),
    ("conformance::cumulative_ack_across_segments", cumulative_ack_across_segments),
    (
        "conformance::checkpoint_round_trip_across_layout_changes",
        a_checkpoint_round_trips_with_java_across_layout_changes,
    ),
];

/// The cases that take the broker down: they run once the others are done, one at a time.
pub(super) const EXCLUSIVE: &[(&str, fn(&Env))] =
    &[("conformance::failover_by_broker_restart", failover_by_broker_restart)];

/// `routing-across-clients`: on a layout of segments of unequal ranges, the Java client and this one send each key to
/// the same segment, the one whose range holds the key's hash.
fn routing_across_clients(env: &Env) {
    let name = unique("routing");
    let path = format!("public/default/{name}");
    let topic = format!("topic://{path}");
    // Four segments of a quarter of the ring each, then the second split in two eighths.
    env.admin(&["scalable-topics", "create", "-s", "4", &path]);
    env.admin(&["scalable-topics", "split-segment", "-s", "1", &path]);
    let ranges = active_ranges(env, &path);
    assert_eq!(ranges.len(), 5, "the layout: {ranges:?}");
    let keys: Vec<String> = (0..128).map(|i| format!("key-{i}")).collect();
    let rust: BTreeMap<String, u64> = run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut routed = BTreeMap::new();
        for key in &keys {
            let id = producer.new_message().key(key.clone()).value(format!("rust-{key}")).send().await.unwrap();
            routed.insert(key.clone(), segment_of(&id.to_string()));
        }
        client.close().await.unwrap();
        routed
    });
    let java = env.java_route(&topic, &keys);
    for key in &keys {
        let hash = KeyHash::of(key.as_bytes()).segment_hash();
        let covering: Vec<u64> =
            ranges.iter().filter(|(_, (start, end))| (*start..=*end).contains(&hash)).map(|(id, _)| *id).collect();
        assert_eq!(covering.len(), 1, "key {key}, hash {hash:#06x}: segments {covering:?} of {ranges:?}");
        assert_eq!((rust[key], java[key]), (covering[0], covering[0]), "key {key}, hash {hash:#06x}");
    }
    let used: BTreeSet<u64> = rust.values().copied().collect();
    assert_eq!(used.len(), ranges.len(), "every segment got keys: {used:?}");
}

/// The active segments of scalable topic `path`, by id, with the range of hashes each covers.
fn active_ranges(env: &Env, path: &str) -> BTreeMap<u64, (u16, u16)> {
    let metadata = stats(env, &["scalable-topics", "get-metadata", path]);
    let segments = metadata["segments"].as_object().unwrap_or_else(|| panic!("no segments in {metadata}"));
    let field = |segment: &serde_json::Value, name: &str| {
        segment[name].as_u64().unwrap_or_else(|| panic!("no {name} in {segment}"))
    };
    segments
        .values()
        .filter(|segment| segment["state"] == "ACTIVE")
        .map(|segment| {
            let range = &segment["hashRange"];
            let bound = |name| u16::try_from(field(range, name)).unwrap();
            (field(segment, "segmentId"), (bound("start"), bound("end")))
        })
        .collect()
}

/// The segment a message id, as it prints, names: `segment:ledger:entry:index`, or Java's `MessageIdV5{segment=…`.
fn segment_of(id: &str) -> u64 {
    let segment = id.split_once("segment=").map_or(id, |(_, rest)| rest);
    segment.split([':', ',']).next().and_then(|s| s.parse().ok()).unwrap_or_else(|| panic!("no segment in {id}"))
}

/// `split-and-merge-under-load`: a producer sends keyed messages without pause while a segment splits and its children
/// merge again; a Queue and a Stream consumer read meanwhile. No send fails, both consumers get every message once, and
/// the Stream consumer gets each key's messages in the order they were sent.
fn split_and_merge_under_load(env: &Env) {
    let name = unique("split-merge");
    let path = format!("public/default/{name}");
    let topic = format!("topic://{path}");
    env.admin(&["scalable-topics", "create", "-s", "2", &path]);
    // Keep the layout to the admin's changes.
    env.disable_auto_scale(&path);
    let keys: Vec<String> = (0..16).map(|i| format!("k{i}")).collect();
    let (changes_done, producing) = (AtomicBool::new(false), AtomicBool::new(true));
    std::thread::scope(|scope| {
        let (started, change_now) = std::sync::mpsc::channel();
        let (changes_done, path) = (&changes_done, &path);
        scope.spawn(move || {
            change_now.recv().unwrap();
            // Segment 0 splits into 2 and 3, which merge into 4.
            env.admin(&["scalable-topics", "split-segment", "-s", "0", path]);
            env.admin(&["scalable-topics", "merge-segments", "--segment-id-1", "2", "--segment-id-2", "3", path]);
            changes_done.store(true, Ordering::Release);
        });
        run(async {
            let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
            let proxied = proxy_client(env).build().await.unwrap();
            let queue = client
                .new_queue_consumer(Schema::string())
                .topic(&topic)
                .subscription_name("queue")
                .subscription_initial_position(SubscriptionInitialPosition::Earliest)
                .subscribe()
                .await
                .unwrap();
            let stream = stream_consumer(&proxied, &topic, "ordered").await;
            let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
            let sent = Mutex::new(BTreeMap::<String, Vec<String>>::new());
            let produce = async {
                let mut sends = FuturesOrdered::new();
                let (mut round, mut after) = (0, 0);
                // Twenty rounds after the merge, or a bound that only a stuck split or merge reaches.
                while after < 20 && round < 3000 {
                    if round == 5 {
                        started.send(()).unwrap();
                    }
                    for key in &keys {
                        let value = format!("{key}-{round:04}");
                        sends.push_back(producer.new_message().key(key.clone()).value(value.clone()).send());
                        sent.lock().unwrap().entry(key.clone()).or_default().push(value);
                    }
                    while sends.len() > 2 * keys.len() {
                        sends.next().await.unwrap().unwrap();
                    }
                    after += usize::from(changes_done.load(Ordering::Acquire));
                    round += 1;
                    compio::time::sleep(Duration::from_millis(20)).await;
                }
                while let Some(result) = sends.next().await {
                    result.unwrap();
                }
                producing.store(false, Ordering::Release);
            };
            let total = || sent.lock().unwrap().values().map(Vec::len).sum::<usize>();
            let read_queue = async {
                let mut got = Vec::new();
                while producing.load(Ordering::Acquire) || got.len() < total() {
                    let Some(message) = queue.receive_timeout(Duration::from_secs(30)).await.unwrap() else { break };
                    queue.acknowledge(message.id()).await.unwrap();
                    got.push(message);
                }
                got
            };
            let read_stream = async {
                let mut got = Vec::new();
                while producing.load(Ordering::Acquire) || got.len() < total() {
                    let Some(message) = stream.receive_timeout(Duration::from_secs(30)).await.unwrap() else { break };
                    stream.acknowledge_cumulative(message.id()).await.unwrap();
                    got.push(message);
                }
                got
            };
            let ((), from_queue, from_stream) = futures_util::join!(produce, read_queue, read_stream);
            assert!(changes_done.load(Ordering::Acquire), "the split and the merge did not complete");
            let sent = sent.into_inner().unwrap();
            let segments: BTreeSet<u64> = from_queue.iter().map(|m| segment_of(&m.id().to_string())).collect();
            assert!(
                segments.is_superset(&BTreeSet::from([0, 1, 2, 3, 4])),
                "the layout changed under load: {segments:?}"
            );
            let mut expected: Vec<String> = sent.values().flatten().cloned().collect();
            expected.sort();
            let mut queued: Vec<String> = from_queue.iter().map(|m| m.value().unwrap()).collect();
            queued.sort();
            assert_eq!(queued, expected, "the Queue consumer gets every message once");
            assert_eq!(per_key(&from_stream), sent, "the Stream consumer gets every key in order");
            client.close().await.unwrap();
            proxied.close().await.unwrap();
        });
    });
}

/// The values of `messages` by key, in the order received.
fn per_key(messages: &[Message<String>]) -> BTreeMap<String, Vec<String>> {
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in messages {
        keys.entry(message.key().unwrap().to_string()).or_default().push(message.value().unwrap());
    }
    keys
}

/// `cumulative-ack-across-segments`: a Stream consumer's cumulative ack of a message settles what came before it on
/// every segment, sealed ones included, and nothing that came after it.
fn cumulative_ack_across_segments(env: &Env) {
    let name = unique("cumulative");
    let path = format!("public/default/{name}");
    let topic = format!("topic://{path}");
    env.admin(&["scalable-topics", "create", "-s", "2", &path]);
    env.disable_auto_scale(&path);
    // Keys of segment 0, which covers the lower half of the ring, and of segment 1, in turn.
    let (low, high): (Vec<String>, Vec<String>) =
        (0..64).map(|i| format!("k{i}")).partition(|key| KeyHash::of(key.as_bytes()).segment_hash() < 0x8000);
    let key = |i: usize| if i.is_multiple_of(2) { &low[i / 2 % 4] } else { &high[i / 2 % 4] };
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let send = async |prefix: &str, i: usize| {
            let id = producer.new_message().key(key(i).clone()).value(format!("{prefix}-{i:02}")).send().await;
            segment_of(&id.unwrap().to_string())
        };
        let receive = async |consumer: &local::StreamConsumer<String>| {
            consumer.receive_timeout(Duration::from_secs(30)).await.unwrap().expect("a message")
        };
        let values = |messages: &[Message<String>]| -> BTreeSet<String> {
            messages.iter().map(|m| m.value().unwrap()).collect()
        };

        // Two active segments, each message received before the next is sent, so that they come in turn from each:
        // the ack of the tenth settles the nine before it, from both segments, and none after it.
        let first = stream_consumer(&client, &topic, "first").await;
        let mut got = Vec::new();
        for i in 0..20 {
            assert_eq!(send("active", i).await, (i % 2) as u64);
            got.push(receive(&first).await);
        }
        first.acknowledge_cumulative(got[9].id()).await.unwrap();
        wait_for(|| stream_backlog(env, &name) == 10, "the ack settles what came before it").await;
        first.close().await.unwrap();
        let again = stream_consumer(&client, &topic, "again").await;
        let mut rest = Vec::new();
        for _ in 0..10 {
            rest.push(receive(&again).await);
        }
        assert_eq!(values(&rest), values(&got[10..]), "what came after the ack comes again");
        assert!(again.receive_timeout(Duration::from_secs(1)).await.unwrap().is_none(), "and only that");
        again.acknowledge_cumulative(rest.last().unwrap().id()).await.unwrap();
        wait_for(|| stream_backlog(env, &name) == 0, "the last ack settles both segments").await;

        // A sealed segment: segment 0 splits with messages delivered and not acknowledged. Its children's messages wait
        // until it is drained, which the ack of a message of segment 1 does.
        let mut delivered = Vec::new();
        for i in 0..10 {
            send("sealed", i).await;
            delivered.push(receive(&again).await);
        }
        env.admin(&["scalable-topics", "split-segment", "-s", "0", &path]);
        for i in 0..10 {
            // Segment 1, and the children 2 and 3 of segment 0, in turn.
            let segment = send("split", i).await;
            assert!(
                if i.is_multiple_of(2) { segment == 2 || segment == 3 } else { segment == 1 },
                "message {i}: {segment}"
            );
        }
        let mut from_one = Vec::new();
        for _ in 0..5 {
            from_one.push(receive(&again).await);
        }
        let segments: BTreeSet<u64> = from_one.iter().map(|m| segment_of(&m.id().to_string())).collect();
        assert_eq!(segments, BTreeSet::from([1]), "the children wait for their parent");
        again.acknowledge_cumulative(from_one.last().unwrap().id()).await.unwrap();
        delivered.extend(from_one);
        for _ in 0..5 {
            let message = receive(&again).await;
            assert!([2, 3].contains(&segment_of(&message.id().to_string())), "a child's: {message:?}");
            delivered.push(message);
        }
        assert_eq!(values(&delivered).len(), 20);
        again.acknowledge_cumulative(delivered.last().unwrap().id()).await.unwrap();
        wait_for(|| stream_backlog(env, &name) == 0, "the last ack settles every segment").await;
        client.close().await.unwrap();
    });
}

/// How a failover case takes the topic away from the clients.
#[derive(Debug, Clone, Copy)]
enum Failover {
    /// `pulsar-admin namespaces unload`: the broker closes the topic's producers and consumers, and gives the topic up,
    /// as when its ownership moves to another broker.
    Unload,
    /// The broker is killed and started again: every connection drops, and nothing answers until it is back.
    BrokerRestart,
}

/// `failover-by-unload`.
fn failover_by_unload(env: &Env) {
    failover(env, Failover::Unload);
}

/// `failover-by-broker-restart`.
fn failover_by_broker_restart(env: &Env) {
    failover(env, Failover::BrokerRestart);
}

/// The failover cases: a producer sends without pause while the topic fails over, its sends queued as they are made,
/// and a Queue, a Stream and a Checkpoint consumer receive meanwhile. The producer carries on, with no send failing,
/// and each consumer resumes: it gets every message, the Stream and the Checkpoint consumers each key's in the order
/// sent. Messages may come twice after a failover (at least once), but the first time each comes is in order.
fn failover(env: &Env, failover: Failover) {
    // A namespace of its own, to unload.
    let namespace = format!("public/{}", unique("failover"));
    env.admin(&["namespaces", "create", &namespace]);
    let path = format!("{namespace}/orders");
    let topic = format!("topic://{path}");
    env.admin(&["scalable-topics", "create", "-s", "2", &path]);
    let (failed_over, producing) = (AtomicBool::new(false), AtomicBool::new(true));
    // The producer's registrations on the two segments, before the unload.
    let registered = OnceLock::new();
    std::thread::scope(|scope| {
        let (start, fail_over) = std::sync::mpsc::channel();
        let (failed_over, namespace, path, registered) = (&failed_over, &namespace, &path, &registered);
        scope.spawn(move || {
            fail_over.recv().unwrap();
            match failover {
                Failover::Unload => {
                    // The keys go to both segments: wait for the producer on each.
                    let deadline = Instant::now() + Duration::from_secs(30);
                    let before = loop {
                        let before = producers(env, path);
                        if before.len() == 2 || Instant::now() > deadline {
                            break before;
                        }
                        std::thread::sleep(Duration::from_millis(200));
                    };
                    assert_eq!(before.len(), 2, "the producer on each segment: {before:?}");
                    registered.set(before).unwrap();
                    env.admin(&["namespaces", "unload", namespace]);
                }
                Failover::BrokerRestart => env.restart_broker(),
            }
            failed_over.store(true, Ordering::Release);
        });
        run(async {
            let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
            let proxied = proxy_client(env).build().await.unwrap();
            // No send times out while the broker is away.
            let producer = client
                .new_producer(Schema::string())
                .topic(&topic)
                .send_timeout(Duration::ZERO)
                .create()
                .await
                .unwrap();
            let queue = client
                .new_queue_consumer(Schema::string())
                .topic(&topic)
                .subscription_name("queue")
                .subscription_initial_position(SubscriptionInitialPosition::Earliest)
                .subscribe()
                .await
                .unwrap();
            let stream = stream_consumer(&proxied, &topic, "ordered").await;
            let checkpoint = client.new_checkpoint_consumer(Schema::string()).topic(&topic);
            let checkpoint = checkpoint.start_position(Checkpoint::earliest()).create().await.unwrap();
            let sent = Mutex::new(BTreeMap::<String, Vec<String>>::new());
            let total = || sent.lock().unwrap().values().map(Vec::len).sum::<usize>();
            let produce = async {
                let mut sends = FuturesOrdered::new();
                let mut after = 0;
                // Twenty messages first, then the failover; a message every 50 ms until it is over, and twenty more.
                for i in 0..6000 {
                    let (key, value) = (format!("k{}", i % 5), format!("m-{i:04}"));
                    sends.push_back(producer.new_message().key(key.clone()).value(value.clone()).send());
                    sent.lock().unwrap().entry(key).or_default().push(value);
                    // A send's message is queued when its future is first polled: poll the new one, and take the
                    // ones that completed, without waiting.
                    while let Some(Some(result)) = sends.next().now_or_never() {
                        result.expect("a send made across the failover");
                    }
                    if i == 20 {
                        start.send(()).unwrap();
                    }
                    after += usize::from(failed_over.load(Ordering::Acquire));
                    if after == 20 {
                        break;
                    }
                    compio::time::sleep(Duration::from_millis(50)).await;
                }
                while let Some(result) = sends.next().await {
                    result.expect("a send made across the failover");
                }
                producing.store(false, Ordering::Release);
            };
            let deadline = Instant::now() + Duration::from_secs(240);
            let reading = |got: &[Message<String>]| {
                (producing.load(Ordering::Acquire) || firsts(got) < total()) && Instant::now() < deadline
            };
            let wait = Duration::from_secs(1);
            let read_queue = async {
                let mut got = Vec::new();
                while reading(&got) {
                    if let Some(message) = queue.receive_timeout(wait).await.unwrap() {
                        queue.acknowledge(message.id()).await.unwrap();
                        got.push(message);
                    }
                }
                got
            };
            let read_stream = async {
                let mut got = Vec::new();
                while reading(&got) {
                    if let Some(message) = stream.receive_timeout(wait).await.unwrap() {
                        stream.acknowledge_cumulative(message.id()).await.unwrap();
                        got.push(message);
                    }
                }
                got
            };
            let read_checkpoint = async {
                let mut got = Vec::new();
                while reading(&got) {
                    got.extend(checkpoint.receive_timeout(wait).await.unwrap());
                }
                got
            };
            let ((), from_queue, from_stream, from_checkpoint) =
                futures_util::join!(produce, read_queue, read_stream, read_checkpoint);
            assert!(failed_over.load(Ordering::Acquire), "the failover did not complete");
            if let Some(before) = registered.get() {
                // The broker did let go of the producer: it is back on both segments, registered again.
                let again = producers(env, path);
                let since = |producers: &BTreeMap<String, String>| producers.values().cloned().collect::<BTreeSet<_>>();
                assert_eq!(again.len(), 2, "{again:?}");
                assert!(since(before).is_disjoint(&since(&again)), "before {before:?}, after {again:?}");
            }
            let sent = sent.into_inner().unwrap();
            let as_sets = |got: &[Message<String>]| -> BTreeMap<String, BTreeSet<String>> {
                per_key_first(got).into_iter().map(|(key, values)| (key, values.into_iter().collect())).collect()
            };
            let expected: BTreeMap<String, BTreeSet<String>> =
                sent.iter().map(|(key, values)| (key.clone(), values.iter().cloned().collect())).collect();
            assert_eq!(as_sets(&from_queue), expected, "the Queue consumer resumes");
            assert_eq!(per_key_first(&from_stream), sent, "the Stream consumer resumes, every key in order");
            assert_eq!(per_key_first(&from_checkpoint), sent, "the Checkpoint consumer resumes, every key in order");
            client.close().await.unwrap();
            proxied.close().await.unwrap();
        });
    });
}

/// How many messages of `messages` came for the first time.
fn firsts(messages: &[Message<String>]) -> usize {
    messages.iter().map(|m| m.value().unwrap()).collect::<BTreeSet<_>>().len()
}

/// The values of `messages` by key, each the first time it came, in the order received.
fn per_key_first(messages: &[Message<String>]) -> BTreeMap<String, Vec<String>> {
    let mut seen = BTreeSet::new();
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in messages {
        let value = message.value().unwrap();
        if seen.insert(value.clone()) {
            keys.entry(message.key().unwrap().to_string()).or_default().push(value);
        }
    }
    keys
}

/// The producers of scalable topic `path`, one per segment it writes to, by name, with when each registered: the
/// topic stats' `producers`.
fn producers(env: &Env, path: &str) -> BTreeMap<String, String> {
    let topic = stats(env, &["scalable-topics", "stats", path]);
    let producers = topic["producers"].as_array().map_or(&[][..], Vec::as_slice);
    let since = |producer: &serde_json::Value| {
        let name = producer["producerName"].as_str().expect("a producer's name");
        (name.to_string(), producer["connectedSince"].as_str().expect("when a producer registered").to_string())
    };
    producers.iter().map(since).collect()
}
