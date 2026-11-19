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
//! last lines of their logs are printed. What the Rust producer stored is read back with the Java client, run in
//! the broker container.

mod env;

use std::collections::BTreeMap;
use std::sync::Arc;
use std::time::Duration;

use apache_pulsar::{ClientBuilder, ErrorKind, Local, ProducerAccessMode, Schema, TlsAuthentication, Token, local};
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
fn keys_keep_their_order_across_a_split(env: &Env) {
    let name = unique("split");
    let topic = format!("topic://public/default/{name}");
    let keys: Vec<String> = (0..16).map(|i| format!("k{i}")).collect();
    let rounds = 60;
    let split = std::thread::scope(|scope| {
        let (started, split_now) = std::sync::mpsc::channel();
        let admin = scope.spawn(move || {
            // Split once the first sends are in; the producer keeps sending meanwhile.
            split_now.recv().unwrap();
            env.admin(&["scalable-topics", "split-segment", "-s", "0", &format!("public/default/{name}")])
        });
        run(async {
            let client = token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap();
            let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
            let mut sends = FuturesOrdered::new();
            for round in 0..rounds {
                if round == 5 {
                    started.send(()).unwrap();
                }
                for key in &keys {
                    sends.push_back(producer.new_message().key(key.clone()).value(format!("{key}-{round:03}")).send());
                }
                // Keep about two rounds in flight, and pace the rounds so the split lands mid-stream.
                while sends.len() > 2 * keys.len() {
                    sends.next().await.unwrap().unwrap();
                }
                compio::time::sleep(Duration::from_millis(50)).await;
            }
            while let Some(result) = sends.next().await {
                result.unwrap();
            }
            client.close().await.unwrap();
        });
        admin.join().unwrap()
    });
    let _ = split;
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
        let expected: Vec<String> = (0..rounds).map(|round| format!("{key}-{round:03}")).collect();
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
