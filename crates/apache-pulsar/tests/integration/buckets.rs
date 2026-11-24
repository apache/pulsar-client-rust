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

//! Entry buckets (*key-shared*): a segment rolled over to a successor of the same range with more buckets, by the
//! admin or by the controller for more Stream consumers than buckets. The simulated cluster has no rollover.

use std::collections::BTreeMap;

use apache_pulsar::{Message, Schema};
use apache_pulsar_protocol::hash::KeyHash;

use super::{Env, SUB, proxy_client, run, stats, stream_all, stream_consumer, unique, wait_for};

pub(super) const TESTS: &[(&str, fn(&Env))] = &[
    ("keys_keep_their_order_across_a_rebucket", keys_keep_their_order_across_a_rebucket),
    ("more_stream_consumers_than_buckets_get_a_rebucket", more_stream_consumers_than_buckets_get_a_rebucket),
];

/// The active segments of scalable topic `path`, as `get-metadata` gives them.
fn active_segments(env: &Env, path: &str) -> Vec<serde_json::Value> {
    let metadata = stats(env, &["scalable-topics", "get-metadata", path]);
    let segments = metadata["segments"].as_object().unwrap_or_else(|| panic!("no segments in {metadata}"));
    segments.values().filter(|segment| segment["state"] == "ACTIVE").cloned().collect()
}

/// The number of entry buckets of a segment as `get-metadata` gives it.
fn buckets(segment: &serde_json::Value) -> usize {
    segment["entryBucketSplits"].as_array().map_or(0, Vec::len) + 1
}

/// The values of `messages` by key, in the order received.
fn per_key(messages: &[Message<String>]) -> BTreeMap<String, Vec<String>> {
    let mut keys: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in messages {
        keys.entry(message.key().unwrap().to_string()).or_default().push(message.value().unwrap());
    }
    keys
}

/// Java's `V5SegmentRebucketTest.testRebucketMidFlowPreservesPerKeyOrder`: the admin rolls the one segment over to a
/// successor with eight buckets between two rounds of sends, and a Stream consumer gets every key in order, the
/// sealed segment's messages first.
fn keys_keep_their_order_across_a_rebucket(env: &Env) {
    let name = unique("rebucket");
    let path = format!("public/default/{name}");
    let topic = format!("topic://{path}");
    env.admin(&["scalable-topics", "create", "-s", "1", &path]);
    env.disable_auto_scale(&path);
    let keys: Vec<String> = (0..16).map(|i| format!("key-{i}")).collect();
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let consumer = stream_consumer(&client, &topic, "solo").await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut sent: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let send_rounds = async |rounds: std::ops::Range<usize>, sent: &mut BTreeMap<String, Vec<String>>| {
            for round in rounds {
                for key in &keys {
                    let value = format!("{key}-{round:02}");
                    producer.new_message().key(key.clone()).value(value.clone()).send().await.unwrap();
                    sent.entry(key.clone()).or_default().push(value);
                }
            }
        };
        send_rounds(0..10, &mut sent).await;
        env.rebucket(&path, 0, 8);
        let rolled_over = || {
            let active = active_segments(env, &path);
            active.len() == 1 && active[0]["segmentId"] != 0 && buckets(&active[0]) == 8
        };
        wait_for(rolled_over, "the segment rolls over to one of eight buckets").await;
        send_rounds(10..20, &mut sent).await;
        let got = stream_all(&consumer, 320).await;
        assert_eq!(per_key(&got), sent);
        client.close().await.unwrap();
    });
}

/// Java's `V5AutoRebucketTest.testConsumerSurplusOnColdTopicRebucketsInsteadOfSplitting`, the consumers' side: five
/// Stream consumers on a one-segment topic of four buckets, with no traffic. The controller rolls the segment over to
/// eight buckets rather than split it, and the five share the successor: each key goes whole to one of them, in order.
fn more_stream_consumers_than_buckets_get_a_rebucket(env: &Env) {
    let name = unique("auto-rebucket");
    let path = format!("public/default/{name}");
    let topic = format!("topic://{path}");
    env.admin(&["scalable-topics", "create", "-s", "1", &path]);
    // Two keys of each of the eight buckets of the successor.
    let mut keys = Vec::new();
    for bucket in 0..8u16 {
        let of_bucket =
            (0..).map(|i| format!("key-{i}")).filter(|key| KeyHash::of(key.as_bytes()).bucket_hash() >> 13 == bucket);
        keys.extend(of_bucket.take(2));
    }
    run(async {
        let client = proxy_client(env).build().await.unwrap();
        let mut members = Vec::new();
        for i in 0..5 {
            members.push(stream_consumer(&client, &topic, &format!("member-{i}")).await);
        }
        let rebucketed = || {
            let active = active_segments(env, &path);
            active.len() == 1 && active[0]["segmentId"] != 0 && buckets(&active[0]) == 8
        };
        wait_for(rebucketed, "the controller rolls the segment over to eight buckets").await;
        let successor = active_segments(env, &path)[0]["segmentId"].as_u64().unwrap();
        let attached = || {
            let topic_stats = stats(env, &["scalable-topics", "stats", &path]);
            let segment = topic_stats["layout"]["segments"][successor.to_string()]["name"].as_str().map(str::to_string);
            segment.is_some_and(|segment| {
                let stats = stats(env, &["scalable-topics", "segment-stats", &segment]);
                stats["subscriptions"][SUB]["consumers"].as_array().map_or(0, Vec::len) == 5
            })
        };
        wait_for(attached, "the five consumers share the successor").await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        let mut sent: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for round in 0..5 {
            for key in &keys {
                let value = format!("{key}-{round}");
                producer.new_message().key(key.clone()).value(value.clone()).send().await.unwrap();
                sent.entry(key.clone()).or_default().push(value);
            }
        }
        let total = keys.len() * 5;
        let got = futures_util::future::join_all(members.iter().map(|member| stream_all(member, total))).await;
        let mut all: BTreeMap<String, Vec<String>> = BTreeMap::new();
        for (i, messages) in got.iter().enumerate() {
            for (key, values) in per_key(messages) {
                assert!(all.insert(key.clone(), values).is_none(), "key {key} went to member {i} and another");
            }
        }
        assert_eq!(all, sent, "every key whole on one member, in order");
        let busy = got.iter().filter(|messages| !messages.is_empty()).count();
        assert!(busy > 1, "the members share the successor: {busy} received");
        client.close().await.unwrap();
    });
}
