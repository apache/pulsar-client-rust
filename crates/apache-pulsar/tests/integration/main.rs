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

//! Integration tests against a real broker and proxy, which the harness starts in Docker (`env.rs`).
//!
//! `cargo test` skips this target; run it with `cargo test -p apache-pulsar --test integration`. The broker and
//! the proxy start before the first test, serve them all, and are removed when the run ends. On a failure, the
//! last lines of their logs are printed. What the Rust producer stored is read back with the Java client, and what
//! the Java client stored is read with the Rust consumer, both Java clients run in the broker container.

mod env;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use apache_pulsar::{
    Checkpoint, ClientBuilder, ErrorKind, Local, Message, ProducerAccessMode, Schema, SubscriptionInitialPosition,
    TlsAuthentication, Token, local,
};
use apache_pulsar_protocol::hash::KeyHash;
use env::Env;
use futures_util::StreamExt;
use futures_util::stream::FuturesOrdered;
use libtest_mimic::{Arguments, Trial};

const TESTS: &[(&str, fn(&Env))] = &[
    ("plaintext_broker", plaintext_broker),
    ("plaintext_proxy", plaintext_proxy),
    ("tls_with_token", tls_with_token),
    ("tls_with_client_certificate", tls_with_client_certificate),
    ("missing_credentials_are_refused", missing_credentials_are_refused),
    ("a_java_consumer_reads_what_was_produced", a_java_consumer_reads_what_was_produced),
    ("keys_keep_their_order_across_a_split", keys_keep_their_order_across_a_split),
    ("a_partitioned_topic_routes_like_v4", a_partitioned_topic_routes_like_v4),
    ("a_short_name_is_a_regular_topic", a_short_name_is_a_regular_topic),
    ("exclusive_producers_conflict_at_create", exclusive_producers_conflict_at_create),
    ("exclusive_with_fencing_fences_the_older_producer", exclusive_with_fencing_fences_the_older_producer),
    ("a_missing_topic_is_not_found_without_auto_creation", a_missing_topic_is_not_found_without_auto_creation),
    ("a_queue_consumer_reads_and_acknowledges_every_segment", a_queue_consumer_reads_and_acknowledges_every_segment),
    ("a_queue_consumer_reads_what_java_produced", a_queue_consumer_reads_what_java_produced),
    ("a_queue_consumer_reads_compressed_java_batches", a_queue_consumer_reads_compressed_java_batches),
    ("a_split_during_consumption_loses_no_message", a_split_during_consumption_loses_no_message),
    ("a_latest_consumer_gets_what_the_children_stored_first", a_latest_consumer_gets_what_the_children_stored_first),
    ("queue_consumers_share_a_subscription", queue_consumers_share_a_subscription),
    ("a_negative_ack_brings_a_message_back", a_negative_ack_brings_a_message_back),
    ("batch_index_acks_settle_whole_batches", batch_index_acks_settle_whole_batches),
    ("a_checkpoint_consumer_resumes_across_a_split", a_checkpoint_consumer_resumes_across_a_split),
    ("a_latest_checkpoint_consumer_reads_a_later_split", a_latest_checkpoint_consumer_reads_a_later_split),
    ("a_checkpoint_consumer_skips_a_deleted_segment", a_checkpoint_consumer_skips_a_deleted_segment),
    (
        "a_checkpoint_round_trips_with_java_across_layout_changes",
        a_checkpoint_round_trips_with_java_across_layout_changes,
    ),
];

fn main() {
    let args = Arguments::from_args();
    // Listing the tests needs no environment.
    let env = (!args.list).then(|| Arc::new(Env::start()));
    let trials = TESTS
        .iter()
        .map(|&(name, test)| {
            let env = env.clone();
            Trial::test(name, move || {
                test(env.as_deref().expect("the environment is started unless listing"));
                Ok(())
            })
        })
        .collect();
    let conclusion = libtest_mimic::run(&args, trials);
    if conclusion.has_failed()
        && let Some(env) = &env
    {
        env.print_logs();
    }
    // Remove the containers before exiting: `exit` runs no destructors.
    drop(env);
    conclusion.exit();
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

/// A topic of its own for each test and run.
fn unique(name: &str) -> String {
    use std::hash::{BuildHasher, RandomState};
    format!("{name}-{:x}", RandomState::new().hash_one(std::time::Instant::now()) & 0xff_ffff)
}

fn token_client(url: &str, token: Option<String>) -> ClientBuilder<Local> {
    let builder = local::PulsarClient::builder().service_url(url).operation_timeout(Duration::from_secs(20));
    match token {
        Some(token) => builder.authentication(Token::new(token)),
        None => builder,
    }
}

/// Creates a producer on a new scalable topic through `builder`, sends a keyed and a keyless message, and closes.
async fn produce_once(builder: ClientBuilder<Local>) -> apache_pulsar::Result<()> {
    let client = builder.build().await?;
    let topic = format!("topic://public/default/{}", unique("connect"));
    let result = async {
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await?;
        assert_eq!(producer.topic(), topic);
        producer.new_message().key("k").value("keyed".to_string()).send().await?;
        producer.send("keyless".to_string()).await?;
        producer.close().await
    }
    .await;
    client.close().await?;
    result
}

fn plaintext_broker(env: &Env) {
    run(async { produce_once(token_client(env::BROKER_URL, Some(env.token.clone()))).await.unwrap() });
}

fn plaintext_proxy(env: &Env) {
    run(async { produce_once(token_client(env::PROXY_URL, Some(env.token.clone()))).await.unwrap() });
}

fn tls_with_token(env: &Env) {
    for url in [env::BROKER_TLS_URL, env::PROXY_TLS_URL] {
        run(async {
            let builder = token_client(url, Some(env.token.clone())).tls_trust_certs_file(env.ca());
            produce_once(builder).await.unwrap_or_else(|e| panic!("{url}: {e}"));
        });
    }
}

fn tls_with_client_certificate(env: &Env) {
    for url in [env::BROKER_TLS_URL, env::PROXY_TLS_URL] {
        run(async {
            let builder = local::PulsarClient::builder()
                .service_url(url)
                .tls_trust_certs_file(env.ca())
                .authentication(TlsAuthentication::from_pem_files(env.client_cert(), env.client_key()).unwrap());
            produce_once(builder).await.unwrap_or_else(|e| panic!("{url}: {e}"));
        });
    }
}

fn missing_credentials_are_refused(_: &Env) {
    run(async {
        let error = produce_once(token_client(env::BROKER_URL, None)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
        let error = produce_once(token_client(env::BROKER_URL, Some("not-a-token".into()))).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    });
}

/// Rust producer, Java consumer: what one stores, the other reads, keys and values.
fn a_java_consumer_reads_what_was_produced(env: &Env) {
    let topic = format!("topic://public/default/{}", unique("interop"));
    let sent: Vec<(Option<String>, String)> =
        (0..10).map(|i| ((i % 3 != 0).then(|| format!("key-{}", i % 4)), format!("value-{i}"))).collect();
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        for (key, value) in &sent {
            let mut message = producer.new_message().value(value.clone()).property("source", "rust");
            if let Some(key) = key {
                message = message.key(key.clone());
            }
            message.send().await.unwrap();
        }
        client.close().await.unwrap();
    });
    let mut received: Vec<(Option<String>, String)> =
        env.consume(&topic, sent.len()).into_iter().map(|m| (m.key, m.value)).collect();
    received.sort_by(|a, b| a.1.cmp(&b.1));
    let mut expected = sent;
    expected.sort_by(|a, b| a.1.cmp(&b.1));
    assert_eq!(received, expected);
}

/// Java's `V5ProducerLayoutChangeOrderingTest`, against the real broker: a split while sends are in flight, and
/// every key's messages stored in send order, the parent's before the children's.
///
/// The producer keeps going until the split has completed, and some more, however long `pulsar-admin` takes.
fn keys_keep_their_order_across_a_split(env: &Env) {
    use std::sync::atomic::{AtomicBool, Ordering};
    let name = unique("split");
    let topic = format!("topic://public/default/{name}");
    let keys: Vec<String> = (0..16).map(|i| format!("k{i}")).collect();
    let split_done = AtomicBool::new(false);
    let rounds = std::thread::scope(|scope| {
        let (started, split_now) = std::sync::mpsc::channel();
        let split_done = &split_done;
        scope.spawn(move || {
            // Split once the first sends are in; the producer keeps sending meanwhile.
            split_now.recv().unwrap();
            env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")]);
            split_done.store(true, Ordering::Release);
        });
        run(async {
            let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
            let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
            let mut sends = FuturesOrdered::new();
            let (mut round, mut after_split) = (0, 0);
            // Twenty rounds after the split, or a bound that only a stuck split reaches.
            while after_split < 20 && round < 3000 {
                if round == 5 {
                    started.send(()).unwrap();
                }
                for key in &keys {
                    sends.push_back(producer.new_message().key(key.clone()).value(format!("{key}-{round:04}")).send());
                }
                // Keep about two rounds in flight.
                while sends.len() > 2 * keys.len() {
                    sends.next().await.unwrap().unwrap();
                }
                if split_done.load(Ordering::Acquire) {
                    after_split += 1;
                }
                round += 1;
                compio::time::sleep(Duration::from_millis(20)).await;
            }
            while let Some(result) = sends.next().await {
                result.unwrap();
            }
            client.close().await.unwrap();
            assert!(split_done.load(Ordering::Acquire), "the split did not complete");
            round
        })
    });
    let consumed = env.consume(&topic, keys.len() * rounds);
    // Children have higher segment ids than their parent: segment, then position, is the order stored.
    let mut by_key: BTreeMap<String, Vec<((u64, i64, i64), String)>> = BTreeMap::new();
    for message in &consumed {
        let position = message.position.expect("a V5 message id");
        by_key.entry(message.key.clone().unwrap()).or_default().push((position, message.value.clone()));
    }
    let segments: std::collections::BTreeSet<u64> = consumed.iter().map(|m| m.position.unwrap().0).collect();
    assert!(segments.len() > 1, "the split didn't happen while producing: segments {segments:?}");
    for (key, mut messages) in by_key {
        messages.sort();
        let values: Vec<String> = messages.into_iter().map(|(_, value)| value).collect();
        let expected: Vec<String> = (0..rounds).map(|round| format!("{key}-{round:04}")).collect();
        assert_eq!(values, expected, "key {key}");
    }
}

/// The migration bridge: a partitioned `persistent://` topic is a synthetic layout of legacy segments, and keys
/// go to the partition a v4 producer would pick.
fn a_partitioned_topic_routes_like_v4(env: &Env) {
    let name = unique("legacy");
    let topic = format!("persistent://public/default/{name}");
    let partitions = 3;
    env.admin(&["topics", "create-partitioned-topic", "-p", &partitions.to_string(), &topic]);
    let keys: Vec<String> = (0..12).map(|i| format!("user-{i}")).collect();
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        for key in &keys {
            producer.new_message().key(key.clone()).value(key.clone()).send().await.unwrap();
        }
        client.close().await.unwrap();
    });
    let mut expected: BTreeMap<u32, Vec<String>> = BTreeMap::new();
    for key in &keys {
        expected.entry(KeyHash::of(key.as_bytes()).partition(partitions)).or_default().push(key.clone());
    }
    for (partition, keys) in expected {
        let consumed = env.consume(&format!("{topic}-partition-{partition}"), keys.len());
        let got: Vec<String> = consumed.into_iter().map(|m| m.key.unwrap()).collect();
        assert_eq!(got, keys, "partition {partition}");
    }
}

/// A short name is a regular topic in `public/default`, as in Java: a synthetic layout with one legacy segment.
fn a_short_name_is_a_regular_topic(env: &Env) {
    let name = unique("short");
    env.admin(&["topics", "create", &format!("persistent://public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&name).create().await.unwrap();
        producer.new_message().key("k").value("one".to_string()).send().await.unwrap();
        producer.send("two".to_string()).await.unwrap();
        client.close().await.unwrap();
    });
    let consumed = env.consume(&format!("persistent://public/default/{name}"), 2);
    let values: Vec<String> = consumed.into_iter().map(|m| m.value).collect();
    assert_eq!(values, ["one", "two"]);
}

/// Java's `testExclusiveRejectsSecondProducer`: the conflict fails create.
fn exclusive_producers_conflict_at_create(env: &Env) {
    let topic = format!("topic://public/default/{}", unique("exclusive"));
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let first = client
            .new_producer(Schema::string())
            .topic(&topic)
            .access_mode(ProducerAccessMode::Exclusive)
            .create()
            .await
            .unwrap();
        let second = client
            .new_producer(Schema::string())
            .topic(&topic)
            .access_mode(ProducerAccessMode::Exclusive)
            .create()
            .await;
        let error = second.unwrap_err();
        assert!(matches!(error.kind(), ErrorKind::ProducerFenced | ErrorKind::ProducerBusy), "{error}");
        first.send("still mine".to_string()).await.unwrap();
        client.close().await.unwrap();
    });
}

/// A producer with `ExclusiveWithFencing` fences the older one out: the broker refuses the older one's return, since
/// it carries the topic epoch it was given, and the newer one keeps working.
fn exclusive_with_fencing_fences_the_older_producer(env: &Env) {
    let topic = format!("topic://public/default/{}", unique("fencing"));
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let fencing = || {
            client
                .new_producer(Schema::string())
                .topic(&topic)
                .access_mode(ProducerAccessMode::ExclusiveWithFencing)
                .create()
        };
        let older = fencing().await.unwrap();
        older.send("before".to_string()).await.unwrap();
        let newer = fencing().await.unwrap();
        let error = older.send("after".to_string()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ProducerFenced, "{error}");
        newer.send("mine".to_string()).await.unwrap();
        client.close().await.unwrap();
    });
    let values: Vec<String> = env.consume(&topic, 2).into_iter().map(|m| m.value).collect();
    assert_eq!(values, ["before", "mine"]);
}

/// Java's `testNoAutoCreateWhenNamespaceDisallows`: a missing topic is NotFound when the namespace forbids
/// creating it.
fn a_missing_topic_is_not_found_without_auto_creation(env: &Env) {
    let namespace = format!("public/{}", unique("strict"));
    env.admin(&["namespaces", "create", &namespace]);
    env.admin(&["namespaces", "set-auto-topic-creation", &namespace, "--disable"]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let error = client
            .new_producer(Schema::string())
            .topic(format!("topic://{namespace}/absent"))
            .create()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "{error}");
        client.close().await.unwrap();
    });
}

const SUB: &str = "workers";

/// A Queue consumer of `topic` on the `workers` subscription, from where `position` says.
async fn queue_consumer<T: 'static>(
    client: &local::PulsarClient,
    schema: Schema<T>,
    topic: &str,
    position: SubscriptionInitialPosition,
) -> local::QueueConsumer<T> {
    let builder = client.new_queue_consumer(schema).topic(topic).subscription_name(SUB);
    builder.subscription_initial_position(position).subscribe().await.unwrap()
}

/// What `pulsar-admin` prints for `args`, as JSON: a topic's or a segment's stats.
fn stats(env: &Env, args: &[&str]) -> serde_json::Value {
    let output = env.admin(args);
    serde_json::from_str(&output).unwrap_or_else(|error| panic!("{args:?} printed no JSON ({error}): {output}"))
}

/// Receives and acknowledges messages until there are `count`, or none comes for a while.
async fn receive_all<T: 'static>(consumer: &local::QueueConsumer<T>, count: usize) -> Vec<Message<T>> {
    let mut got = Vec::new();
    while got.len() < count {
        let Some(message) = consumer.receive_timeout(Duration::from_secs(30)).await.unwrap() else { break };
        consumer.acknowledge(message.id()).await.unwrap();
        got.push(message);
    }
    got
}

fn sorted_values(messages: &[Message<String>]) -> Vec<String> {
    let mut values: Vec<String> = messages.iter().map(|m| m.value().unwrap()).collect();
    values.sort();
    values
}

/// Java's `V5QueueConsumerMultiSegmentAckTest`: a Queue consumer reads every segment, its acks reach each one, and a
/// consumer subscribing again finds nothing left.
fn a_queue_consumer_reads_and_acknowledges_every_segment(env: &Env) {
    let name = unique("queue");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "4", &format!("public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let earliest = SubscriptionInitialPosition::Earliest;
        let consumer = queue_consumer(&client, Schema::string(), &topic, earliest).await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut expected = Vec::new();
        for i in 0..40 {
            let value = format!("v-{i:02}");
            producer.new_message().key(format!("k{i}")).value(value.clone()).send().await.unwrap();
            expected.push(value);
        }
        let got = receive_all(&consumer, expected.len()).await;
        assert_eq!(sorted_values(&got), expected);
        let segments: std::collections::BTreeSet<String> =
            got.iter().map(|m| m.id().to_string().split(':').next().unwrap().to_string()).collect();
        assert_eq!(segments.len(), 4, "every segment delivered: {segments:?}");
        assert!(got.iter().all(|m| m.topic() == topic));
        // The acks go out with the close.
        consumer.close().await.unwrap();
        let again = queue_consumer(&client, Schema::string(), &topic, earliest).await;
        assert!(again.receive_timeout(Duration::from_secs(1)).await.unwrap().is_none());
        client.close().await.unwrap();
    });
}

/// Java producer, Rust consumer: what the V5 CLI publishes to a scalable topic, keys and properties included.
fn a_queue_consumer_reads_what_java_produced(env: &Env) {
    let name = unique("java-to-rust");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "2", &format!("public/default/{name}")]);
    let messages = ["-m", "alpha", "-m", "beta", "-m", "gamma"];
    env.produce(&topic, &[&messages[..], &["-vs", "string", "-k", "user-1", "-p", "source=java"]].concat());
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let consumer = queue_consumer(&client, Schema::string(), &topic, SubscriptionInitialPosition::Earliest).await;
        let got = receive_all(&consumer, 3).await;
        assert_eq!(sorted_values(&got), ["alpha", "beta", "gamma"]);
        for message in &got {
            assert_eq!(message.key(), Some("user-1"));
            assert_eq!(message.properties().collect::<Vec<_>>(), [("source", "java")]);
            assert!(message.producer_name().is_some());
        }
        client.close().await.unwrap();
    });
}

/// A Java producer's batches, compressed with each codec, read through the migration bridge: `pulsar-perf` writes a
/// regular topic, which the Queue consumer reads as a layout of one legacy segment. `pulsar-perf` sends more than
/// it is asked to, as it counts the sends completed: the broker's count of messages in is the reference.
fn a_queue_consumer_reads_compressed_java_batches(env: &Env) {
    for codec in ["NONE", "LZ4", "ZLIB", "ZSTD", "SNAPPY"] {
        let topic = format!("persistent://public/default/{}", unique(&format!("codec-{}", codec.to_lowercase())));
        env.perf_produce(&topic, codec, 100, 64);
        let stored = stats(env, &["topics", "stats", &topic])["msgInCounter"].as_u64().unwrap() as usize;
        assert!(stored >= 100, "{codec}: {stored} stored");
        run(async {
            let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
            let consumer =
                queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
            let mut got = Vec::new();
            while let Some(message) = consumer.receive_timeout(Duration::from_secs(2)).await.unwrap() {
                consumer.acknowledge(message.id()).await.unwrap();
                got.push(message);
            }
            let ids: std::collections::BTreeSet<String> = got.iter().map(|m| m.id().to_string()).collect();
            assert_eq!((got.len(), ids.len()), (stored, stored), "{codec}");
            assert!(got.iter().all(|m| m.size() == 64), "{codec}");
            // `segment:ledger:entry:index`: messages after the first of a batch show it was one.
            let batched = got.iter().filter(|m| !m.id().to_string().ends_with(":0")).count();
            assert!(batched > 0, "{codec}: no batch of more than one message");
            client.close().await.unwrap();
        });
    }
}

/// Java's `V5SegmentSplitTest`: a split while the producer sends and the consumer receives, and every message
/// received once.
fn a_split_during_consumption_loses_no_message(env: &Env) {
    let name = unique("consume-split");
    let topic = format!("topic://public/default/{name}");
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let consumer = queue_consumer(&client, Schema::string(), &topic, SubscriptionInitialPosition::Earliest).await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut expected = Vec::new();
        let mut got = Vec::new();
        for i in 0..60 {
            if i == 30 {
                env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")]);
            }
            let value = format!("v-{i:02}");
            producer.new_message().key(format!("k{i}")).value(value.clone()).send().await.unwrap();
            expected.push(value);
            got.extend(receive_all(&consumer, 1).await);
        }
        got.extend(receive_all(&consumer, expected.len() - got.len()).await);
        assert_eq!(sorted_values(&got), expected);
        let segments: std::collections::BTreeSet<String> =
            got.iter().map(|m| m.id().to_string().split(':').next().unwrap().to_string()).collect();
        assert!(segments.len() > 1, "the split didn't happen while consuming: {segments:?}");
        // The parent, drained, is let go while the layout still has it: its subscription keeps no consumer.
        let parent = format!("segment://public/default/{name}/0000-ffff-0");
        let deadline = std::time::Instant::now() + Duration::from_secs(20);
        loop {
            let subscription = stats(env, &["scalable-topics", "segment-stats", &parent])["subscriptions"][SUB].clone();
            assert!(subscription.is_object(), "no subscription {SUB} on {parent}");
            let consumers = subscription["consumers"].as_array().map_or(0, Vec::len);
            if consumers == 0 {
                break;
            }
            assert!(std::time::Instant::now() < deadline, "the drained parent keeps {consumers} consumer(s)");
            compio::time::sleep(Duration::from_millis(200)).await;
        }
        client.close().await.unwrap();
    });
}

/// A Queue consumer at `Latest`, subscribed before a split, gets what a producer stored in the children before the
/// consumer heard of the split. Its loop is held while the split and the sends happen, so the children get those
/// messages before the consumer subscribes to them. Java subscribes to them at `Latest`, and loses them, unless the
/// broker gave the children the subscription: whether it did is printed.
fn a_latest_consumer_gets_what_the_children_stored_first(env: &Env) {
    let name = unique("latest-split");
    let topic = format!("topic://public/default/{name}");
    let rt = compio::runtime::Runtime::new().unwrap();
    let (client, consumer) = rt.block_on(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let consumer = queue_consumer(&client, Schema::string(), &topic, SubscriptionInitialPosition::Latest).await;
        (client, consumer)
    });
    // The consumer's loop doesn't run until the next `block_on`.
    env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")]);
    let child = format!("segment://public/default/{name}/0000-7fff-1");
    let carried = env.admin(&["scalable-topics", "segment-stats", &child]).contains(&format!("\"{SUB}\""));
    eprintln!("the broker gave a split's children the Queue consumer's subscription: {carried}");
    let sent: Vec<String> = (0..10).map(|i| format!("early-{i}")).collect();
    std::thread::scope(|scope| {
        scope.spawn(|| {
            run(async {
                let producer_client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
                let producer = producer_client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
                for (i, value) in sent.iter().enumerate() {
                    producer.new_message().key(format!("k{i}")).value(value.clone()).send().await.unwrap();
                }
                producer_client.close().await.unwrap();
            })
        });
    });
    rt.block_on(async {
        let got = receive_all(&consumer, sent.len()).await;
        let mut expected = sent.clone();
        expected.sort();
        assert_eq!(sorted_values(&got), expected);
        client.close().await.unwrap();
    });
}

/// Java's `testQueueConsumersShareLoad`: consumers of one subscription share its messages, each delivered to one.
fn queue_consumers_share_a_subscription(env: &Env) {
    let name = unique("share");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "2", &format!("public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let earliest = SubscriptionInitialPosition::Earliest;
        let a = queue_consumer(&client, Schema::string(), &topic, earliest).await;
        let b = queue_consumer(&client, Schema::string(), &topic, earliest).await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let expected: Vec<String> = (0..100).map(|i| format!("v-{i:03}")).collect();
        for (i, value) in expected.iter().enumerate() {
            producer.new_message().key(format!("k{i}")).value(value.clone()).send().await.unwrap();
        }
        let (from_a, from_b) = futures_util::join!(receive_all(&a, 100), receive_all(&b, 100));
        assert!(!from_a.is_empty() && !from_b.is_empty(), "a: {}, b: {}", from_a.len(), from_b.len());
        let mut all = from_a;
        all.extend(from_b);
        assert_eq!(sorted_values(&all), expected);
        client.close().await.unwrap();
    });
}

/// Java's `testNegativeAckCausesRedelivery`.
fn a_negative_ack_brings_a_message_back(env: &Env) {
    let topic = format!("topic://public/default/{}", unique("nack"));
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let consumer = client
            .new_queue_consumer(Schema::string())
            .topic(&topic)
            .subscription_name(SUB)
            .negative_ack_redelivery_delay(Duration::from_millis(200))
            .subscribe()
            .await
            .unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        producer.send("again".to_string()).await.unwrap();
        let first = consumer.receive().await.unwrap();
        assert_eq!(first.redelivery_count(), 0);
        consumer.negative_acknowledge(first.id()).unwrap();
        let again = consumer.receive_timeout(Duration::from_secs(30)).await.unwrap().expect("redelivered");
        assert_eq!((again.id(), again.value().unwrap(), again.redelivery_count()), (first.id(), "again".into(), 1));
        consumer.acknowledge(again.id()).await.unwrap();
        client.close().await.unwrap();
    });
}

/// Batch-index acks against the real broker. Each message of Java's batches is acknowledged on its own, without
/// grouping, so that no ack names a whole entry: the broker settles each batch from its indexes. A consumer
/// subscribing again then finds nothing left.
fn batch_index_acks_settle_whole_batches(env: &Env) {
    let topic = format!("persistent://public/default/{}", unique("index-acks"));
    env.perf_produce(&topic, "NONE", 100, 16);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let consumer = client
            .new_queue_consumer(Schema::bytes())
            .topic(&topic)
            .subscription_name(SUB)
            .subscription_initial_position(SubscriptionInitialPosition::Earliest)
            .acknowledgment_group_time(Duration::ZERO)
            .subscribe()
            .await
            .unwrap();
        let (mut got, mut batched) = (0, 0);
        while let Some(message) = consumer.receive_timeout(Duration::from_secs(2)).await.unwrap() {
            consumer.acknowledge(message.id()).await.unwrap();
            got += 1;
            // `segment:ledger:entry:index`: messages after the first of a batch show it was one.
            batched += usize::from(!message.id().to_string().ends_with(":0"));
        }
        assert!(got >= 100 && batched > 0, "{got} received, {batched} after the first of their batch");
        consumer.close().await.unwrap();
        let again = queue_consumer(&client, Schema::bytes(), &topic, SubscriptionInitialPosition::Earliest).await;
        let left = again.receive_timeout(Duration::from_secs(2)).await.unwrap();
        assert!(left.is_none(), "acknowledged, and delivered again: {left:?}");
        client.close().await.unwrap();
    });
    let backlog = &stats(env, &["topics", "stats", &topic])["subscriptions"][SUB]["msgBacklog"];
    assert_eq!(backlog.as_u64(), Some(0), "{backlog}");
}

/// Publishes `count` messages `<prefix>-<i>`, with keys `k0`..`k4` in turn, and records them per key.
async fn publish_keyed(
    producer: &local::Producer<String>,
    prefix: &str,
    count: usize,
    sent: &mut BTreeMap<String, Vec<String>>,
) {
    for i in 0..count {
        let (key, value) = (format!("k{}", i % 5), format!("{prefix}-{i}"));
        producer.new_message().key(key.clone()).value(value.clone()).send().await.unwrap();
        sent.entry(key).or_default().push(value);
    }
}

/// Receives `count` messages, each within the time a split or a merge takes to show.
async fn read<T: 'static>(consumer: &local::CheckpointConsumer<T>, count: usize) -> Vec<Message<T>> {
    let mut got = Vec::new();
    while got.len() < count {
        match consumer.receive_timeout(Duration::from_secs(30)).await.unwrap() {
            Some(message) => got.push(message),
            None => panic!("received {} of {count} messages", got.len()),
        }
    }
    got
}

/// The messages' values, per key, in the order received.
fn by_key(messages: impl IntoIterator<Item = (String, String)>) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for (key, value) in messages {
        out.entry(key).or_default().push(value);
    }
    out
}

fn keyed(messages: &[Message<String>]) -> Vec<(String, String)> {
    messages.iter().map(|m| (m.key().unwrap().to_string(), m.value().unwrap())).collect()
}

fn hex(bytes: &[u8]) -> String {
    bytes.iter().map(|b| format!("{b:02x}")).collect()
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
}

/// Java's `V5CheckpointConsumerOrderingTest.testPerKeyOrderResumingCheckpointAcrossSplit`, on the broker: a consumer
/// restored from a checkpoint taken before a split reads the rest of the sealed parent, then its children, every key
/// in order.
fn a_checkpoint_consumer_resumes_across_a_split(env: &Env) {
    let name = unique("checkpoint-split");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "1", &format!("public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut sent = BTreeMap::new();
        publish_keyed(&producer, "pre", 20, &mut sent).await;
        let first =
            client.new_checkpoint_consumer(Schema::string()).topic(&topic).start_position(Checkpoint::earliest());
        let first = first.create().await.unwrap();
        let mut received = keyed(&read(&first, 10).await);
        let checkpoint = first.checkpoint().await.unwrap();
        first.close().await.unwrap();
        env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")]);
        publish_keyed(&producer, "post", 20, &mut sent).await;
        let resumed = client.new_checkpoint_consumer(Schema::string()).topic(&topic).start_position(checkpoint);
        let resumed = resumed.create().await.unwrap();
        received.extend(keyed(&read(&resumed, 30).await));
        assert_eq!(by_key(received), sent);
        assert!(resumed.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());
        client.close().await.unwrap();
    });
}

/// Java's `V5CheckpointConsumerLatestStartTest`, on the broker: a consumer at the latest position reads the children
/// of a later split from their start.
fn a_latest_checkpoint_consumer_reads_a_later_split(env: &Env) {
    let name = unique("checkpoint-latest");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "1", &format!("public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        producer.new_message().key("k").value("history".to_string()).send().await.unwrap();
        let consumer = client.new_checkpoint_consumer(Schema::string()).topic(&topic).create().await.unwrap();
        producer.new_message().key("k").value("before".to_string()).send().await.unwrap();
        assert_eq!(read(&consumer, 1).await[0].value().unwrap(), "before");
        env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")]);
        let mut sent = BTreeMap::new();
        publish_keyed(&producer, "post-split", 20, &mut sent).await;
        assert_eq!(by_key(keyed(&read(&consumer, 20).await)), sent);
        client.close().await.unwrap();
    });
}

/// Java's `V5CheckpointConsumerSkipDeletedSegmentTest`, on the broker: a segment whose topic is deleted while the
/// layout still has it is skipped, and the others are read.
fn a_checkpoint_consumer_skips_a_deleted_segment(env: &Env) {
    let name = unique("checkpoint-deleted");
    let topic = format!("topic://public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "2", &format!("public/default/{name}")]);
    run(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut kept = Vec::new();
        for i in 0..20 {
            let key = format!("key-{i}");
            let value = format!("v-{i}");
            let id = producer.new_message().key(key).value(value.clone()).send().await.unwrap();
            if id.to_string().starts_with("1:") {
                kept.push(value);
            }
        }
        assert!(!kept.is_empty() && kept.len() < 20, "both segments need messages: {kept:?}");
        env.delete_segment(&format!("segment://public/default/{name}/0000-7fff-0"));
        let consumer = client.new_checkpoint_consumer(Schema::string()).topic(&topic);
        let consumer = consumer.start_position(Checkpoint::earliest()).create().await.unwrap();
        let got = read(&consumer, kept.len()).await;
        kept.sort();
        assert_eq!(sorted_values(&got), kept);
        assert!(consumer.receive_timeout(Duration::from_millis(500)).await.unwrap().is_none());
        client.close().await.unwrap();
    });
}

/// A checkpoint round-trips with the Java client across layout changes (*conformance* §5): Java reads part of a
/// topic and takes a checkpoint; a split; Rust restores Java's checkpoint, reads part of what is left and takes its own;
/// a merge; Java restores Rust's checkpoint and reads the rest. Between them they read every message once, every key
/// in order.
fn a_checkpoint_round_trips_with_java_across_layout_changes(env: &Env) {
    let name = unique("checkpoint-java");
    let topic = format!("topic://public/default/{name}");
    let path = format!("public/default/{name}");
    env.admin(&["scalable-topics", "create", "-s", "1", &path]);
    let rt = compio::runtime::Runtime::new().unwrap();
    let (client, producer) = rt.block_on(async {
        let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        (client, producer)
    });
    let mut sent = BTreeMap::new();
    rt.block_on(publish_keyed(&producer, "pre", 20, &mut sent));
    let (mut received, java_checkpoint) = env.java_checkpoint_read(&topic, "earliest", 8);
    env.admin(&["scalable-topics", "split-segment", "-s", "0", &path]);
    rt.block_on(publish_keyed(&producer, "post", 20, &mut sent));
    let rust_checkpoint = rt.block_on(async {
        let start = Checkpoint::from_bytes(&unhex(&java_checkpoint)).unwrap();
        let consumer = client.new_checkpoint_consumer(Schema::string()).topic(&topic).start_position(start);
        let consumer = consumer.create().await.unwrap();
        received.extend(keyed(&read(&consumer, 20).await));
        let checkpoint = consumer.checkpoint().await.unwrap();
        consumer.close().await.unwrap();
        checkpoint
    });
    env.admin(&["scalable-topics", "merge-segments", "--segment-id-1", "1", "--segment-id-2", "2", &path]);
    rt.block_on(publish_keyed(&producer, "after", 10, &mut sent));
    let (rest, _) = env.java_checkpoint_read(&topic, &hex(&rust_checkpoint.to_bytes()), 22);
    received.extend(rest);
    assert_eq!(by_key(received), sent);
    rt.block_on(client.close()).unwrap();
}
