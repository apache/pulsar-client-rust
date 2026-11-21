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

//! The Stream consumer against the simulated cluster and its controller. Ported scenarios from Java's
//! `V5StreamConsumerBasicTest`, `V5StreamConsumerDagReplayTest`, `V5StreamConsumerOrderingTest`,
//! `V5StreamConsumerAutoReconnectTest`, `V5StreamConsumerAckWatermarkTest` and the Stream consumer's case of
//! `ScalableTopicLegacySegmentTest`, and members that share segments.

use std::time::Duration;

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_ack::AckType;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{MessageMetadata, ServerError};
use apache_pulsar_testkit::scalable::World;
use apache_pulsar_testkit::stream::StreamHarness;
use bytes::Bytes;

const TOPIC: &str = "topic://acme/orders/events";
const SUB: &str = "billing";

fn options() -> ConsumerOptions {
    ConsumerOptions {
        subscription: SUB.into(),
        initial_position: InitialPosition::Earliest,
        // Acks go out at once: the backlogs show them.
        ack_group_time: Duration::ZERO,
        operation_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// Stores a one-message entry in `segment`, with `key`, stamped with the key's bucket hash as a producer does.
fn publish(world: &mut World, segment: u64, key: &str, value: &str) {
    publish_to(world, TOPIC, segment, key, value);
}

/// Stores a one-message entry in `segment` of `topic`, as [`publish`] does.
fn publish_to(world: &mut World, topic: &str, segment: u64, key: &str, value: &str) {
    let hash = u32::from(KeyHash::of(key.as_bytes()).bucket_hash());
    let metadata = MessageMetadata {
        producer_name: "p".into(),
        partition_key: Some(key.into()),
        entry_hash_min: Some(hash),
        entry_hash_max: Some(hash),
        ..Default::default()
    };
    world.publish_raw(topic, segment, metadata, Bytes::copy_from_slice(value.as_bytes()));
}

fn values(messages: &[ReceivedMessage]) -> Vec<String> {
    messages.iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}

/// The `subType` of each `CommandSubscribe` sent to a segment's topic.
fn subscribe_types(h: &StreamHarness, segment_topic: &str) -> Vec<SubType> {
    h.world
        .received(Type::SUBSCRIBE)
        .iter()
        .filter(|c| c.subscribe.topic == segment_topic)
        .map(|c| c.subscribe.subType)
        .collect()
}

#[test]
fn receives_in_order_and_acknowledges_cumulatively() {
    let mut world = world(1);
    for i in 0..50 {
        publish(&mut world, 0, &format!("k{}", i % 7), &format!("m{i}"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    assert_eq!(h.members[solo].subscribed, Some(Ok(())));
    // The segment is the member's alone: Exclusive, named after the member.
    let subscribe = h.world.received(Type::SUBSCRIBE)[0].subscribe.clone();
    assert_eq!((subscribe.subType, subscribe.consumer_name.as_deref()), (SubType::Exclusive, Some("solo-seg-0")));
    let taken = h.take(solo);
    let expected: Vec<String> = (0..50).map(|i| format!("m{i}")).collect();
    assert_eq!(values(&taken), expected);
    let last = &taken[49].id;
    assert_eq!(last.positions.len(), 1);
    h.ack(solo, last);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
    assert!(h.take(solo).is_empty());
    assert_eq!(subscribe_types(&h, "segment://acme/orders/events/0000-ffff-0"), [SubType::Exclusive]);
}

/// The segment a key goes to among `ranges`: `(segment, first hash, last hash)`.
fn route(key: &str, ranges: &[(u64, u16, u16)]) -> u64 {
    let hash = KeyHash::of(key.as_bytes()).segment_hash();
    ranges.iter().find(|(_, start, end)| (*start..=*end).contains(&hash)).unwrap().0
}

/// One cumulative ack covers every segment: the message's position vector holds the last message given from each.
#[test]
fn a_cumulative_ack_covers_every_segment() {
    let mut world = world(2);
    for i in 0..3 {
        publish(&mut world, 0, "a", &format!("a{i}"));
        publish(&mut world, 1, "b", &format!("b{i}"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let taken = h.take(solo);
    // The segments take turns.
    assert_eq!(values(&taken), ["a0", "b0", "a1", "b1", "a2", "b2"]);
    let first = &taken[0].id;
    assert_eq!(first.positions.keys().collect::<Vec<_>>(), [&0]);
    // b0's vector holds a0 too: acknowledging it covers a0.
    h.ack(solo, &taken[1].id);
    assert_eq!((h.world.backlog(TOPIC, 0, SUB), h.world.backlog(TOPIC, 1, SUB)), (2, 2));
    h.ack(solo, &taken[4].id);
    assert_eq!((h.world.backlog(TOPIC, 0, SUB), h.world.backlog(TOPIC, 1, SUB)), (0, 1));
}

/// Java's `testEarliestSubscribePostSplitReadsSealedBacklog`: a subscription created after a split reads the sealed
/// parent's backlog, and the controller hands out the children only once the parent is drained.
#[test]
fn an_earliest_subscription_reads_a_sealed_parent_first() {
    let mut world = world(1);
    for i in 0..5 {
        publish(&mut world, 0, &format!("k{i}"), &format!("pre-{i}"));
    }
    let (low, high) = world.split(TOPIC, 0);
    let children = [(low, 0, 0x7fff), (high, 0x8000, 0xffff)];
    for i in 0..6 {
        let key = format!("kk{i}");
        publish(&mut world, route(&key, &children), &key, &format!("post-{i}"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    assert_eq!(h.world.assigned(TOPIC, SUB, "solo"), [(0, false)]);
    let pre = h.take(solo);
    assert_eq!(values(&pre), ["pre-0", "pre-1", "pre-2", "pre-3", "pre-4"]);
    assert_eq!((h.world.consumer_count(TOPIC, low, SUB), h.world.consumer_count(TOPIC, high, SUB)), (0, 0));
    // Drained: the parent goes, the children come.
    h.ack(solo, &pre[4].id);
    assert_eq!(h.world.assigned(TOPIC, SUB, "solo"), [(low, false), (high, false)]);
    let mut post = values(&h.take(solo));
    post.sort();
    assert_eq!(post, ["post-0", "post-1", "post-2", "post-3", "post-4", "post-5"]);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
}

/// Java's `testStreamConsumerReadsLegacyParentFirst`: on a migrated topic, found by its scalable name, a Stream
/// consumer reads the legacy segments first, as a consumer managed by a V5 client, and the controller hands out the
/// managed segments once the legacy ones are drained.
#[test]
fn reads_a_migrated_topics_legacy_segments_first() {
    let legacy = "persistent://acme/orders/legacy";
    let mut world = World::default();
    world.create_legacy_topic(legacy, 2);
    publish_to(&mut world, legacy, 0, "k0", "pre-0");
    publish_to(&mut world, legacy, 1, "k1", "pre-1");
    let managed = world.migrate(legacy);
    publish_to(&mut world, legacy, managed[0], "k2", "post-0");
    publish_to(&mut world, legacy, managed[1], "k3", "post-1");
    let mut h = StreamHarness::new(world, "topic://acme/orders/legacy");
    let solo = h.join("solo", options());
    assert_eq!(h.world.assigned(legacy, SUB, "solo"), [(0, false), (1, false)]);
    let pre = h.take(solo);
    let mut read = values(&pre);
    read.sort();
    assert_eq!(read, ["pre-0", "pre-1"]);
    for message in &pre {
        h.ack(solo, &message.id);
    }
    assert_eq!(h.world.assigned(legacy, SUB, "solo"), [(managed[0], false), (managed[1], false)]);
    let mut post = values(&h.take(solo));
    post.sort();
    assert_eq!(post, ["post-0", "post-1"]);
    let marked: Vec<bool> =
        h.world.received(Type::SUBSCRIBE).iter().map(|c| !c.subscribe.metadata.is_empty()).collect();
    assert_eq!(marked, [true, true, false, false]);
}

/// Java's `testEarliestSubscribeAfterTwoSplitsReadsAllBacklog` and `V5StreamConsumerOrderingTest`: across two
/// generations of splits, a key's messages on a segment all come before its messages on the segment's descendants.
#[test]
fn keys_keep_their_order_across_two_splits() {
    let mut world = world(1);
    let keys: Vec<String> = (0..16).map(|k| format!("key-{k}")).collect();
    let mut sent: Vec<(String, String)> = Vec::new();
    let mut publish_round = |world: &mut World, ranges: &[(u64, u16, u16)], generation: &str| {
        for i in 0..3 {
            for key in &keys {
                let value = format!("{key}-{generation}-{i}");
                publish(world, route(key, ranges), key, &value);
                sent.push((key.clone(), value));
            }
        }
    };
    publish_round(&mut world, &[(0, 0, 0xffff)], "gen0");
    let (low, high) = world.split(TOPIC, 0);
    publish_round(&mut world, &[(low, 0, 0x7fff), (high, 0x8000, 0xffff)], "gen1");
    let (lower, upper) = world.split(TOPIC, low);
    publish_round(&mut world, &[(lower, 0, 0x3fff), (upper, 0x4000, 0x7fff), (high, 0x8000, 0xffff)], "gen2");
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let mut received: Vec<ReceivedMessage> = Vec::new();
    loop {
        // Nothing of a descendant while the root has backlog.
        if h.world.backlog(TOPIC, 0, SUB) > 0 {
            assert!([low, high, lower, upper].iter().all(|&s| h.world.consumer_count(TOPIC, s, SUB) == 0));
        }
        let taken = h.take(solo);
        let Some(last) = taken.last() else { break };
        h.ack(solo, &last.id.clone());
        received.extend(taken);
    }
    assert_eq!(received.len(), sent.len());
    for key in &keys {
        let of = |pairs: Vec<(String, String)>| {
            pairs.into_iter().filter(|(k, _)| k == key).map(|(_, v)| v).collect::<Vec<_>>()
        };
        let got =
            received.iter().map(|m| (m.key.clone().unwrap(), String::from_utf8(m.payload.to_vec()).unwrap())).collect();
        assert_eq!(of(got), of(sent.clone()), "per-key order of {key}");
    }
}

/// Java's `testPerKeyOrderAcrossAnEmptySegment`: a segment split before anything was published to it is drained at
/// once, but its children still wait for its parent.
#[test]
fn an_empty_segment_does_not_let_its_children_ahead_of_its_parent() {
    let mut world = world(1);
    publish(&mut world, 0, "k", "gen0");
    let (low, high) = world.split(TOPIC, 0);
    let (lower, upper) = world.split(TOPIC, low);
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    // The root, and the empty child's sibling, whose parent isn't drained either: only the root is assignable.
    assert_eq!(h.world.assigned(TOPIC, SUB, "solo"), [(0, false)]);
    let taken = h.take(solo);
    assert_eq!(values(&taken), ["gen0"]);
    h.ack(solo, &taken[0].id);
    let assigned: Vec<u64> = h.world.assigned(TOPIC, SUB, "solo").into_iter().map(|(s, _)| s).collect();
    assert_eq!(assigned, [lower, upper, high]);
}

/// Java's `testSealedBacklogNotRedeliveredAfterFirstConsumerDrains`.
#[test]
fn a_new_member_gets_nothing_acknowledged_again() {
    let mut world = world(1);
    for i in 0..3 {
        publish(&mut world, 0, &format!("k{i}"), &format!("pre-{i}"));
    }
    world.split(TOPIC, 0);
    let mut h = StreamHarness::new(world, TOPIC);
    let first = h.join("first", options());
    let taken = h.take(first);
    assert_eq!(taken.len(), 3);
    h.ack(first, &taken[2].id);
    h.close(first);
    assert!(h.members[first].closed);
    let second = h.join("second", options());
    assert!(h.take(second).is_empty());
}

/// Java's `testStreamConsumerSurvivesConnectionDrop`: after a connection loss the member registers again, and its
/// segments' consumers subscribe again.
#[test]
fn a_member_comes_back_after_a_connection_loss() {
    let mut world = world(2);
    publish(&mut world, 0, "a", "first");
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let taken = h.take(solo);
    h.ack(solo, &taken[0].id);
    h.drop_connection(solo);
    h.advance(Duration::from_millis(200));
    publish(&mut h.world, 1, "b", "second");
    assert_eq!(h.take_values(solo), ["second"]);
    assert_eq!(h.world.received(Type::SCALABLE_TOPIC_SUBSCRIBE).len(), 2);
}

/// Java's `testReconnectWithinGracePreservesAssignment`: a member back within the grace period keeps its segments,
/// and the others hear nothing of it.
#[test]
fn within_the_grace_period_the_assignment_holds() {
    let mut h = StreamHarness::new(world(4), TOPIC);
    let alice = h.join("alice", options());
    let bob = h.join("bob", options());
    let (alice_segments, bob_segments) = (h.world.assigned(TOPIC, SUB, "alice"), h.world.assigned(TOPIC, SUB, "bob"));
    assert_eq!((alice_segments.len(), bob_segments.len()), (2, 2));
    let updates = h.world.log.len();
    h.drop_connection(alice);
    h.advance(Duration::from_millis(200));
    assert_eq!(h.world.assigned(TOPIC, SUB, "alice"), alice_segments);
    assert_eq!(h.world.assigned(TOPIC, SUB, "bob"), bob_segments);
    assert_eq!(h.members[alice].consumer.segments().count(), 2);
    assert_eq!(h.members[bob].consumer.segments().count(), 2);
    assert!(h.world.log.len() > updates);
}

/// Past the grace period of a member that went away, the controller rebalances: the others take its segments, and
/// give one back when it comes back.
#[test]
fn past_the_grace_period_the_group_rebalances() {
    let mut h = StreamHarness::new(world(2), TOPIC);
    let alice = h.join("alice", options());
    let bob = h.join("bob", options());
    // Bob's subscribe reached the segment before Alice let go of it: he tries again.
    assert_eq!((h.members[bob].subscribed.clone(), h.members[bob].consumer.segments().count()), (None, 0));
    h.advance(Duration::from_millis(200));
    assert_eq!((h.members[bob].subscribed.clone(), h.members[bob].consumer.segments().count()), (Some(Ok(())), 1));
    h.crash(alice);
    assert_eq!(h.world.assigned(TOPIC, SUB, "bob").len(), 1, "within the grace period");
    h.world.end_grace(TOPIC, SUB, "alice");
    h.pump();
    assert_eq!(h.members[bob].consumer.segments().count(), 2);
    // Alice is back, in a new process.
    let alice = h.join("alice", options());
    h.advance(Duration::from_millis(200));
    assert_eq!(h.members[alice].subscribed, Some(Ok(())));
    assert_eq!(h.members[alice].consumer.segments().count(), 1);
    assert_eq!(h.members[bob].consumer.segments().count(), 1);
}

/// Java's `V5StreamConsumerAckWatermarkTest`: once the broker rewinds the subscription and delivers everything again,
/// the application's ack of a position it acknowledged before goes out again.
#[test]
fn a_rewound_subscription_takes_the_same_ack_again() {
    let mut world = world(1);
    for i in 0..5 {
        publish(&mut world, 0, "k", &format!("v{i}"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let taken = h.take(solo);
    let last = taken[4].id.clone();
    h.ack(solo, &last);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
    h.world.rewind(TOPIC, 0, SUB);
    h.pump();
    h.advance(Duration::from_millis(200));
    assert_eq!(h.take(solo).len(), 5);
    h.ack(solo, &last);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
}

const SEGMENT: &str = "segment://acme/orders/events/0000-ffff-0";

/// Each key's values, in the order a member took them.
fn by_key(messages: &[ReceivedMessage]) -> std::collections::BTreeMap<String, Vec<String>> {
    let mut keys: std::collections::BTreeMap<String, Vec<String>> = Default::default();
    for m in messages {
        keys.entry(m.key.clone().unwrap()).or_default().push(String::from_utf8(m.payload.to_vec()).unwrap());
    }
    keys
}

/// More members than segments: the controller shares the segment by entry bucket (*key-shared* §4). The first member
/// drains its Exclusive consumer before it subscribes again `Key_Shared`, and the second waits for that; then each
/// key goes to one member only, and each member acknowledges its own messages, one by one.
#[test]
fn members_beyond_the_segments_share_them_by_bucket() {
    let mut world = world(1);
    world.set_buckets(TOPIC, 0, 4);
    for i in 0..8 {
        publish(&mut world, 0, &format!("k{i}"), &format!("before-{i}"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let alice = h.join("alice", options());
    let held = h.take(alice);
    assert_eq!(held.len(), 8);
    let bob = h.join("bob", options());
    assert_eq!(h.world.assigned(TOPIC, SUB, "alice"), [(0, true)]);
    assert_eq!(h.world.assigned(TOPIC, SUB, "bob"), [(0, true)]);
    // Alice still holds what she was given: her Exclusive consumer stays, and Bob can't attach.
    h.advance(Duration::from_millis(200));
    assert_eq!(h.members[bob].subscribed, None);
    assert_eq!(subscribe_types(&h, SEGMENT).iter().filter(|t| **t == SubType::Exclusive).count(), 1);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 1);
    // Once Alice acknowledged it, her consumer goes, and both attach Key_Shared.
    h.ack(alice, &held[7].id);
    h.advance(Duration::from_millis(500));
    assert_eq!(h.members[bob].subscribed, Some(Ok(())));
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 2);
    let key_shared =
        h.world.received(Type::SUBSCRIBE).into_iter().filter(|c| c.subscribe.subType == SubType::Key_Shared);
    for command in key_shared {
        let meta = command.subscribe.keySharedMeta.as_option().unwrap();
        assert_eq!(meta.hashRanges.len(), 4);
        assert_eq!(meta.entryBucketDispatch, Some(true));
    }
    for i in 0..40 {
        publish(&mut h.world, 0, &format!("k{}", i % 10), &format!("after-{i}"));
    }
    let (got_alice, got_bob) = (h.take(alice), h.take(bob));
    assert_eq!(got_alice.len() + got_bob.len(), 40);
    assert!(!got_alice.is_empty() && !got_bob.is_empty());
    let (keys_alice, keys_bob) = (by_key(&got_alice), by_key(&got_bob));
    assert!(keys_alice.keys().all(|key| !keys_bob.contains_key(key)), "a key goes to one member");
    for values in keys_alice.values().chain(keys_bob.values()) {
        let mut sorted = values.clone();
        sorted.sort_by_key(|v| v["after-".len()..].parse::<u32>().unwrap());
        assert_eq!(values, &sorted, "in order");
    }
    // Each acknowledges its own messages: on a shared segment, one by one.
    h.ack(alice, &got_alice.last().unwrap().id);
    h.ack(bob, &got_bob.last().unwrap().id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
    let cumulative = h.world.received(Type::ACK).iter().filter(|c| c.ack.ack_type == AckType::Cumulative).count();
    assert_eq!(cumulative, 1, "only Alice's Exclusive consumer acknowledged cumulatively");

    // Bob leaves: Alice gets the segment whole again, once she settles what she holds of it. With four buckets over
    // two members, Alice owns buckets 0 and 1: bucket hashes below 0x8000.
    let key = (0..).map(|i| format!("a{i}")).find(|k| KeyHash::of(k.as_bytes()).bucket_hash() < 0x8000).unwrap();
    publish(&mut h.world, 0, &key, "last");
    let last = h.take(alice);
    assert_eq!(values(&last), ["last"]);
    h.close(bob);
    assert!(h.members[bob].closed);
    assert_eq!(h.world.assigned(TOPIC, SUB, "alice"), [(0, false)]);
    let exclusive =
        |h: &StreamHarness| subscribe_types(h, SEGMENT).iter().filter(|t| **t == SubType::Exclusive).count();
    let before = exclusive(&h);
    h.advance(Duration::from_millis(200));
    assert_eq!(exclusive(&h), before, "not before Alice acknowledged what she holds");
    h.ack(alice, &last[0].id);
    h.advance(Duration::from_millis(200));
    assert_eq!(exclusive(&h), before + 1);
    assert_eq!(h.members[alice].consumer.segments().count(), 1);
}

/// With fewer buckets than members, the members left over get nothing, until one leaves.
#[test]
fn members_beyond_the_buckets_wait() {
    let mut world = world(1);
    publish(&mut world, 0, "k", "v");
    let mut h = StreamHarness::new(world, TOPIC);
    let alice = h.join("alice", options());
    let bob = h.join("bob", options());
    assert_eq!((h.members[bob].subscribed.clone(), h.world.assigned(TOPIC, SUB, "bob")), (Some(Ok(())), vec![]));
    assert!(h.take(bob).is_empty());
    let taken = h.take(alice);
    h.close(alice);
    // What Alice took and didn't acknowledge goes to Bob.
    assert_eq!(h.world.assigned(TOPIC, SUB, "bob"), [(0, false)]);
    assert_eq!(values(&h.take(bob)), values(&taken));
}

/// Two members share a segment, and Bob crashes holding a message: the broker gives it to Alice after hers, which are
/// further on in the segment. Returns Alice, the messages she took before, and Bob's as she took it.
fn crash_handover() -> (StreamHarness, usize, Vec<ReceivedMessage>, ReceivedMessage) {
    let mut world = world(1);
    world.set_buckets(TOPIC, 0, 4);
    let mut h = StreamHarness::new(world, TOPIC);
    let alice = h.join("alice", options());
    let bob = h.join("bob", options());
    h.advance(Duration::from_millis(200));
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 2);
    // With four buckets over two members, Alice owns the bucket hashes below 0x8000, and Bob the others.
    let key = |alices: bool| {
        (0..).map(|i| format!("k{i}")).find(|k| (KeyHash::of(k.as_bytes()).bucket_hash() < 0x8000) == alices).unwrap()
    };
    publish(&mut h.world, 0, &key(false), "bob's");
    publish(&mut h.world, 0, &key(true), "alice's 1");
    publish(&mut h.world, 0, &key(true), "alice's 2");
    assert_eq!(values(&h.take(bob)), ["bob's"]);
    let hers = h.take(alice);
    assert_eq!(values(&hers), ["alice's 1", "alice's 2"]);
    h.crash(bob);
    let mut handed = h.take(alice);
    assert_eq!(values(&handed), ["bob's"]);
    (h, alice, hers, handed.remove(0))
}

/// A cumulative ack of Bob's message, the last Alice took, covers hers too, though they are further on in the segment.
/// So once Bob's grace period ends and the controller gives Alice the segment whole, her consumer drains and flips to
/// Exclusive. By position, her messages would stay unacknowledged, and the flip would wait for them for good.
#[test]
fn an_ack_covers_what_was_given_before_it() {
    let (mut h, alice, _, bobs) = crash_handover();
    h.ack(alice, &bobs.id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
    let exclusive =
        |h: &StreamHarness| subscribe_types(h, SEGMENT).iter().filter(|t| **t == SubType::Exclusive).count();
    let before = exclusive(&h);
    h.world.end_grace(TOPIC, SUB, "bob");
    h.advance(Duration::from_millis(200));
    assert_eq!(h.world.assigned(TOPIC, SUB, "alice"), [(0, false)]);
    assert_eq!(exclusive(&h), before + 1);
}

/// A cumulative ack of Alice's last message leaves Bob's, which she took after it and may not have handled yet.
#[test]
fn an_ack_leaves_what_was_given_after_it() {
    let (mut h, alice, hers, _) = crash_handover();
    h.ack(alice, &hers[1].id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 1);
}

/// A split while a member consumes: the controller gives the children the subscription at their start, so they keep
/// what producers store there before the member gets them, once the parent is drained. Each key keeps its order. Once
/// the parent is pruned from the layout, the children no longer wait for it.
#[test]
fn a_split_while_consuming_keeps_each_key_in_order() {
    let mut world = world(1);
    for i in 0..4 {
        publish(&mut world, 0, &format!("k{i}"), &format!("{i}-a"));
    }
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let first = h.take(solo);
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    let children = [(low, 0, 0x7fff), (high, 0x8000, 0xffff)];
    for i in 0..4 {
        let key = format!("k{i}");
        publish(&mut h.world, route(&key, &children), &key, &format!("{i}-b"));
    }
    assert_eq!(h.world.backlog(TOPIC, low, SUB) + h.world.backlog(TOPIC, high, SUB), 4, "kept for the subscription");
    assert!(h.take(solo).is_empty(), "the parent isn't drained");
    h.ack(solo, &first[3].id);
    let second = h.take(solo);
    assert_eq!(second.len(), 4);
    let mut keys = by_key(&first);
    for (key, values) in by_key(&second) {
        keys.entry(key).or_default().extend(values);
    }
    for (key, values) in keys {
        let i = &key[1..];
        assert_eq!(values, [format!("{i}-a"), format!("{i}-b")]);
    }
    h.ack(solo, &second[3].id);
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.world.assigned(TOPIC, SUB, "solo"), [(low, false), (high, false)]);
}

/// The controller refuses the registration: the subscribe fails.
#[test]
fn a_refused_registration_fails_the_subscribe() {
    let mut world = world(1);
    world.controller.fail_registrations.push_back(ServerError::AuthorizationError);
    let mut h = StreamHarness::new(world, TOPIC);
    let solo = h.join("solo", options());
    let Some(Err(RegisterError::Server { error, .. })) = h.members[solo].subscribed.clone() else { panic!() };
    assert_eq!((error, h.members[solo].closed), (ServerError::AuthorizationError, true));
    // So does a connection lost before the first answer.
    let mut h = StreamHarness::new(self::world(1), TOPIC);
    h.world.controller.hold_registrations = true;
    let solo = h.join("solo", options());
    h.drop_connection(solo);
    assert!(matches!(h.members[solo].subscribed, Some(Err(RegisterError::Unavailable(_)))));
}

/// Closed while its registration waits, a member leaves once registered.
#[test]
fn a_member_closed_while_registering_leaves_once_registered() {
    let mut h = StreamHarness::new(world(1), TOPIC);
    h.world.controller.hold_registrations = true;
    let solo = h.join("solo", options());
    h.close(solo);
    assert!(!h.members[solo].closed);
    h.world.release_registrations();
    h.pump();
    assert!(h.members[solo].closed);
    assert_eq!(h.members[solo].subscribed, None);
    assert_eq!(h.world.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE).len(), 1);
    assert_eq!(h.world.assigned(TOPIC, SUB, "solo"), []);
}

/// A connection lost with a segment's subscribe in flight, or as the member leaves: the subscribe is tried again, and
/// the close completes anyway.
#[test]
fn a_connection_lost_mid_subscribe_or_mid_close() {
    let mut h = StreamHarness::new(world(1), TOPIC);
    h.world.hold_subscribes = true;
    let solo = h.join("solo", options());
    h.drop_connection(solo);
    h.world.hold_subscribes = false;
    h.advance(Duration::from_millis(500));
    assert_eq!(h.members[solo].subscribed, Some(Ok(())));
    let now = h.clock.now();
    h.members[solo].consumer.close(now);
    h.drop_connection(solo);
    assert!(h.members[solo].closed);
}

/// The broker closes a segment's consumer as the member closes it: the close completes.
#[test]
fn a_close_crossing_an_unload() {
    let mut h = StreamHarness::new(world(1), TOPIC);
    let solo = h.join("solo", options());
    let now = h.clock.now();
    h.members[solo].consumer.close(now);
    h.world.unload(TOPIC, 0);
    h.pump();
    assert!(h.members[solo].closed);
}

/// A rebalance while a member is in its grace period leaves it out; three members on two segments with buckets:
/// the surplus one shares the first segment.
#[test]
fn surplus_members_share_the_first_segments() {
    let mut world = world(2);
    world.set_buckets(TOPIC, 0, 4);
    world.set_buckets(TOPIC, 1, 4);
    let mut h = StreamHarness::new(world, TOPIC);
    for name in ["a", "b", "c"] {
        h.join(name, options());
    }
    h.advance(Duration::from_secs(1));
    assert_eq!(h.world.assigned(TOPIC, SUB, "a"), [(0, true)]);
    assert_eq!(h.world.assigned(TOPIC, SUB, "b"), [(0, true)]);
    assert_eq!(h.world.assigned(TOPIC, SUB, "c"), [(1, false)]);
    assert!(h.members.iter().all(|m| m.subscribed == Some(Ok(()))));
    // C goes away. The split's parent has no backlog: its children are assignable at once. C, in its grace
    // period, still counts, but hears nothing.
    h.crash(2);
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.world.assigned(TOPIC, SUB, "a"), [(low, false)]);
    assert_eq!(h.world.assigned(TOPIC, SUB, "b"), [(high, false)]);
    assert_eq!(h.world.assigned(TOPIC, SUB, "c"), [(1, false)]);
    h.world.end_grace(TOPIC, SUB, "c");
    h.advance(Duration::from_secs(1));
    assert_eq!(h.world.assigned(TOPIC, SUB, "a"), [(low, false), (1, false)]);
    assert_eq!(h.members[0].consumer.segments().collect::<Vec<_>>(), [1, low]);
}
