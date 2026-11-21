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

//! The producer's batches against the simulated world, deterministically (docs/5-segment-engine.md §2): the three
//! ways a segment's open batches close, one entry bucket per entry and its stamp, sequence ids, the messages that go
//! alone, and what becomes of the open batches on a flush, a close, a reconnect, a split and a timeout. Then key
//! affinity: Stream members sharing a segment by bucket get each key whole, and in order, from batched entries.

use std::collections::BTreeMap;
use std::time::Duration;

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::producer::{Batching, OutgoingMessage, ProducerOptions, SendError};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_subscribe::InitialPosition;
use apache_pulsar_testkit::producer::{Harness, options};
use apache_pulsar_testkit::scalable::World;
use apache_pulsar_testkit::stream::StreamHarness;
use bytes::Bytes;

const TOPIC: &str = "topic://acme/orders/events";

fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// A producer whose segments' open batches close at `max_messages`, `max_bytes` or `delay_ms`.
fn batching(max_messages: usize, max_bytes: usize, delay_ms: u64) -> ProducerOptions {
    let max_delay = Duration::from_millis(delay_ms);
    ProducerOptions { batching: Some(Batching { max_messages, max_bytes, max_delay }), ..options() }
}

fn message(key: &str, value: impl Into<Bytes>) -> OutgoingMessage {
    OutgoingMessage { key: Some(key.into()), payload: value.into(), ..Default::default() }
}

/// How many messages each stored entry holds, in order.
fn entry_sizes(h: &Harness) -> Vec<i32> {
    h.world.stored(TOPIC).iter().map(|s| s.metadata.num_messages_in_batch.unwrap()).collect()
}

/// The values stored, as a reader sees them.
fn values(h: &Harness) -> Vec<String> {
    h.world.messages(TOPIC).iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}

fn numbered(prefix: &str, count: usize) -> Vec<String> {
    (0..count).map(|i| format!("{prefix}{i}")).collect()
}

/// A key whose segment hash falls in `[start, end]`.
fn key_in(start: u16, end: u16) -> String {
    (0..)
        .map(|i| format!("key-{i}"))
        .find(|k| (start..=end).contains(&KeyHash::of(k.as_bytes()).segment_hash()))
        .unwrap()
}

/// The `nth` key in entry bucket `bucket` of a segment with four.
fn key_in_bucket(bucket: u32, nth: usize) -> String {
    let in_bucket = |k: &String| u32::from(KeyHash::of(k.as_bytes()).bucket_hash()) * 4 / 0x1_0000 == bucket;
    (0..).map(|i| format!("key-{i}")).filter(in_bucket).nth(nth).unwrap()
}

#[test]
fn the_open_batches_close_at_the_most_messages() {
    let mut h = Harness::new(world(1), TOPIC, batching(3, 0, 10));
    let sends: Vec<_> = numbered("m", 7).iter().map(|v| h.send(Some("k"), v)).collect();
    // Two full batches go out at once; the last message waits for the delay.
    assert_eq!(entry_sizes(&h), [3, 3]);
    assert_eq!(h.result(sends[6]), None);
    h.advance(Duration::from_millis(9));
    assert_eq!(entry_sizes(&h), [3, 3]);
    h.advance(Duration::from_millis(1));
    assert_eq!(entry_sizes(&h), [3, 3, 1]);
    assert_eq!(values(&h), numbered("m", 7));
    assert!(sends.iter().all(|&s| h.result(s).unwrap().is_ok()));
}

#[test]
fn the_open_batches_close_at_the_most_bytes() {
    let mut h = Harness::new(world(1), TOPIC, batching(0, 8, 10));
    for value in ["aaaa", "bbbb", "cccc", "dddd", "eeee"] {
        h.send(Some("k"), value);
    }
    assert_eq!(entry_sizes(&h), [2, 2]);
    // A message that doesn't fit closes them first, and one over the limit goes alone.
    h.send(Some("k"), "larger than eight");
    assert_eq!(entry_sizes(&h), [2, 2, 1, 1]);
    h.send(Some("k"), "abc");
    h.send(Some("k"), "defgh");
    assert_eq!(entry_sizes(&h), [2, 2, 1, 1, 2]);
    assert_eq!(values(&h), ["aaaa", "bbbb", "cccc", "dddd", "eeee", "larger than eight", "abc", "defgh"]);
}

#[test]
fn the_open_batches_close_the_delay_after_their_first_message() {
    let mut h = Harness::new(world(1), TOPIC, batching(100, 0, 10));
    h.send(Some("k"), "first");
    h.advance(Duration::from_millis(6));
    h.send(Some("k"), "second");
    h.advance(Duration::from_millis(3));
    assert!(h.world.stored(TOPIC).is_empty());
    h.advance(Duration::from_millis(1));
    assert_eq!(entry_sizes(&h), [2]);
    // The next batches wait the delay from their own first message.
    h.send(Some("k"), "third");
    h.advance(Duration::from_millis(9));
    assert_eq!(entry_sizes(&h), [2]);
    h.advance(Duration::from_millis(1));
    assert_eq!(entry_sizes(&h), [2, 1]);
}

/// Java's `EntryBucketBatchContainer`: one batch per entry bucket, stamped with the range of its messages' bucket
/// hashes, and the entries in the order of their sequence ids, each its highest message's.
#[test]
fn an_entry_holds_one_bucket_and_is_stamped_with_its_range() {
    let mut world = world(1);
    world.set_buckets(TOPIC, 0, 4);
    world.dedup = true;
    let mut h = Harness::new(world, TOPIC, batching(100, 0, 10));
    let (a, b, c) = (key_in_bucket(2, 0), key_in_bucket(2, 1), key_in_bucket(0, 0));
    // Sequence ids 0 to 5. A keyless message is in bucket 0.
    let sent = [Some(&a), Some(&c), None, Some(&b), Some(&a), None];
    let sends: Vec<_> =
        sent.iter().enumerate().map(|(i, key)| h.send(key.map(String::as_str), &format!("m{i}"))).collect();
    h.advance(Duration::from_millis(10));

    // Bucket 2's entry goes first: its highest sequence id is 4, bucket 0's is 5. The broker, deduplicating, takes
    // both, which it would not in the other order.
    let hash = |key: &str| u32::from(KeyHash::of(key.as_bytes()).bucket_hash());
    let stored = h.world.stored(TOPIC);
    assert_eq!(stored.len(), 2);
    for (entry, sequence, stamp) in
        [(&stored[0], 4, (hash(&a).min(hash(&b)), hash(&a).max(hash(&b)))), (&stored[1], 5, (0, hash(&c)))]
    {
        let metadata = &entry.metadata;
        assert_eq!(
            (entry.sequence_id, metadata.sequence_id, metadata.highest_sequence_id),
            (sequence, sequence, Some(sequence))
        );
        assert_eq!((metadata.entry_hash_min, metadata.entry_hash_max), (Some(stamp.0), Some(stamp.1)));
        assert_eq!(metadata.num_messages_in_batch, Some(3));
    }
    assert_eq!(values(&h), ["m0", "m3", "m4", "m1", "m2", "m5"]);
    // Each message keeps its own sequence id.
    let sequences: Vec<u64> = h.world.messages(TOPIC).iter().map(|m| m.sequence_id).collect();
    assert_eq!(sequences, [0, 3, 4, 1, 2, 5]);
    let writes: Vec<_> = h
        .world
        .received(Type::SEND)
        .iter()
        .map(|c| (c.send.sequence_id, c.send.highest_sequence_id, c.send.num_messages))
        .collect();
    assert_eq!(writes, [(4, Some(4), Some(3)), (5, Some(5), Some(3))]);
    // A send completes with the id a consumer reads its message with: its entry, its index and the batch's size.
    for message in h.world.messages(TOPIC) {
        let index: usize = String::from_utf8(message.payload.to_vec()).unwrap()[1..].parse().unwrap();
        assert_eq!(h.result(sends[index]), Some(&Ok(message.id.clone())));
    }
}

/// Java's `ProducerImpl`: a sequence id at or below the last one given may be a duplicate, which the broker drops
/// with its entry, so it goes alone.
#[test]
fn a_sequence_id_that_may_be_a_duplicate_goes_alone() {
    let mut world = world(1);
    world.dedup = true;
    let mut h = Harness::new(world, TOPIC, batching(100, 0, 10));
    let send = |h: &mut Harness, sequence: u64| {
        h.send_message(OutgoingMessage { sequence_id: Some(sequence), ..message("k", sequence.to_string()) })
    };
    let ten = send(&mut h, 10);
    let eleven = send(&mut h, 11);
    let again = send(&mut h, 11);
    let twelve = send(&mut h, 12);
    h.advance(Duration::from_millis(10));
    assert_eq!(entry_sizes(&h), [2, 1]);
    assert_eq!(values(&h), ["10", "11", "12"]);
    let dropped = h.result(again).unwrap().as_ref().unwrap();
    assert_eq!((dropped.entry.ledger_id, dropped.entry.entry_id), (-1, -1));
    for stored in [ten, eleven, twelve] {
        assert!(h.result(stored).unwrap().as_ref().unwrap().entry.ledger_id >= 0);
    }
}

/// A delivery time and replication clusters are the entry's: such a message goes alone, after the open batches.
#[test]
fn a_delivery_time_or_replication_clusters_go_alone() {
    let mut h = Harness::new(world(1), TOPIC, batching(100, 0, 10));
    h.send(Some("k"), "m0");
    h.send(Some("k"), "m1");
    h.send_message(OutgoingMessage { deliver_at: Some(1_900_000_000_000), ..message("k", "delayed") });
    h.send(Some("k"), "m3");
    h.send_message(OutgoingMessage { replicate_to: Some(vec!["east".into()]), ..message("k", "east only") });
    h.send(Some("k"), "m5");
    h.advance(Duration::from_millis(10));
    assert_eq!(values(&h), ["m0", "m1", "delayed", "m3", "east only", "m5"]);
    assert_eq!(entry_sizes(&h), [2, 1, 1, 1, 1]);
    let stored = h.world.stored(TOPIC);
    assert_eq!(stored[1].metadata.deliver_at_time, Some(1_900_000_000_000));
    assert_eq!(stored[3].metadata.replicate_to, ["east"]);
    assert!(stored.iter().enumerate().all(|(i, s)| i == 1 || s.metadata.deliver_at_time.is_none()));
    assert!(stored.iter().enumerate().all(|(i, s)| i == 3 || s.metadata.replicate_to.is_empty()));
}

#[test]
fn a_message_too_large_for_the_broker_fails_alone() {
    // The batches' byte limit is the broker's own, 5 MiB in the harness.
    let mut h = Harness::new(world(1), TOPIC, batching(100, 0, 10));
    let small = h.send(Some("k"), "small");
    let large = h.send_message(message("k", vec![0; 6 << 20]));
    let after = h.send(Some("k"), "after");
    h.advance(Duration::from_millis(10));
    assert!(matches!(h.result(large), Some(Err(SendError::TooLarge { limit, .. })) if *limit == 5 << 20));
    assert_eq!(values(&h), ["small", "after"]);
    assert!(h.result(small).unwrap().is_ok() && h.result(after).unwrap().is_ok());
    // At the broker's limit, the messages' own metadata takes an entry of several over it: it fails whole, as in
    // Java. Below it, the default, only a message too large on its own can fail.
    let halves = [h.send_message(message("k", vec![1; 5 << 19])), h.send_message(message("k", vec![2; 5 << 19]))];
    for half in halves {
        assert!(matches!(h.result(half), Some(Err(SendError::TooLarge { .. }))), "{:?}", h.result(half));
    }
}

/// A byte limit above the broker's is the broker's: messages that fit it alone don't fail together.
#[test]
fn the_byte_limit_is_at_most_the_brokers() {
    let mut h = Harness::new(world(1), TOPIC, batching(100, 10 << 20, 10));
    let sends = [h.send_message(message("k", vec![1; 3 << 20])), h.send_message(message("k", vec![2; 3 << 20]))];
    h.advance(Duration::from_millis(10));
    assert!(sends.iter().all(|&s| h.result(s).unwrap().is_ok()));
    assert_eq!(entry_sizes(&h), [1, 1]);
}

#[test]
fn a_flush_sends_the_open_batches_at_once() {
    let mut h = Harness::new(world(2), TOPIC, batching(100, 0, 1000));
    for value in numbered("m", 4) {
        h.send(None, &value);
    }
    assert!(h.world.stored(TOPIC).is_empty());
    h.flush();
    assert_eq!(h.done.len(), 4);
    // Keyless messages go round robin: one entry on each segment.
    assert_eq!(entry_sizes(&h), [2, 2]);
}

/// The open batches were never written: a close fails them, as Java's fails its batch container.
#[test]
fn close_fails_what_the_open_batches_hold() {
    let mut h = Harness::new(world(1), TOPIC, batching(100, 0, 10));
    let written = h.send(Some("k"), "written");
    h.flush();
    let open = h.send(Some("k"), "open");
    h.close();
    assert!(h.closed);
    assert!(h.result(written).unwrap().is_ok());
    assert_eq!(h.result(open), Some(&Err(SendError::AlreadyClosed)));
    assert_eq!(values(&h), ["written"]);
}

/// After a reconnect, the entries written before go again unchanged, with their sequence ids, then the open batch.
#[test]
fn a_reconnect_writes_the_same_entries_again() {
    let mut h = Harness::new(world(1), TOPIC, batching(2, 0, 10));
    h.send(Some("k"), "m0");
    h.world.hold_sends = true;
    for value in ["m1", "m2", "m3", "m4"] {
        h.send(Some("k"), value);
    }
    // Two entries written and still on their way, m4 in an open batch. The connection goes, with them.
    h.world.hold_sends = false;
    h.drop_connection();
    h.run_until(Duration::from_secs(5), |h| h.done.len() == 5);
    assert_eq!(values(&h), numbered("m", 5));
    assert_eq!(entry_sizes(&h), [2, 2, 1]);
    let writes: Vec<_> = h.world.received(Type::SEND).iter().map(|c| c.send.sequence_id).collect();
    assert_eq!(writes, [1, 3, 1, 3, 4]);
}

/// The open batches of a segment that splits go to its children, in call order, before the newer messages.
#[test]
fn a_split_routes_the_open_batches_again_in_order() {
    let mut h = Harness::new(world(1), TOPIC, batching(100, 0, 10));
    let (a, b) = (key_in(0, 0x7fff), key_in(0x8000, 0xffff));
    h.send(Some(&a), "a1");
    h.send(Some(&b), "b1");
    h.world.split(TOPIC, 0);
    h.pump();
    h.send(Some(&a), "a2");
    h.send(Some(&b), "b2");
    h.run_until(Duration::from_secs(5), |h| h.done.len() == 4);
    assert!(h.world.stored_in(TOPIC, 0).is_empty());
    let mut by_key: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in h.world.messages(TOPIC) {
        by_key.entry(message.key.unwrap()).or_default().push(String::from_utf8(message.payload.to_vec()).unwrap());
    }
    assert_eq!(by_key[&a], ["a1", "a2"]);
    assert_eq!(by_key[&b], ["b1", "b2"]);
    assert!(h.producer.sends_parked_or_held() > 0);
}

/// An entry is stored whole or not at all: its messages time out together, on its first message's deadline.
#[test]
fn an_entry_times_out_whole() {
    let options = ProducerOptions { send_timeout: Some(Duration::from_millis(100)), ..batching(100, 0, 10) };
    let mut h = Harness::new(world(1), TOPIC, options);
    h.world.hold_receipts = true;
    let first = h.send(Some("k"), "m0");
    h.advance(Duration::from_millis(5));
    let second = h.send(Some("k"), "m1");
    h.advance(Duration::from_millis(5));
    assert_eq!(entry_sizes(&h), [2]);
    h.advance(Duration::from_millis(89));
    assert_eq!(h.result(first), None);
    h.advance(Duration::from_millis(1));
    assert_eq!(h.result(first), Some(&Err(SendError::Timeout)));
    assert_eq!(h.result(second), Some(&Err(SendError::Timeout)));
}

/// Key affinity with batched producers and shared segments (docs/6-scalable-topics.md §5): every entry holds one
/// bucket's keys, so the members sharing a segment by bucket each get whole keys, in send order. The producer's
/// entries are stored once both members share the segment: a member alone on it owns every bucket.
#[test]
fn members_sharing_a_segment_by_bucket_get_whole_keys_from_batches() {
    let buckets = |segments| {
        let mut world = world(segments);
        world.set_buckets(TOPIC, 0, 4);
        world
    };
    let mut producer = Harness::new(buckets(1), TOPIC, batching(10, 0, 1));
    let keys: Vec<String> = (0..16).map(|i| format!("key-{i}")).collect();
    let mut sent: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for round in 0..20 {
        for key in &keys {
            let value = format!("{key}-{round}");
            producer.send(Some(key), &value);
            sent.entry(key.clone()).or_default().push(value);
        }
    }
    producer.flush();
    assert_eq!(producer.done.len(), 320);
    let entries = producer.world.stored(TOPIC);
    assert!(entries.iter().any(|s| s.metadata.num_messages_in_batch > Some(1)), "no batch of several messages");

    let options = ConsumerOptions {
        subscription: "billing".into(),
        initial_position: InitialPosition::Earliest,
        ack_group_time: Duration::ZERO,
        operation_timeout: Duration::from_secs(1),
        ..Default::default()
    };
    let mut consumers = StreamHarness::new(buckets(1), TOPIC);
    let members = [consumers.join("alice", options.clone()), consumers.join("bob", options)];
    consumers.advance(Duration::from_millis(500));
    assert!(members.iter().all(|&m| consumers.members[m].subscribed == Some(Ok(()))));
    for entry in entries {
        consumers.world.publish_raw(TOPIC, 0, entry.metadata, entry.payload);
    }
    let mut got: [BTreeMap<String, Vec<String>>; 2] = Default::default();
    for _ in 0..20 {
        for (i, &member) in members.iter().enumerate() {
            for message in consumers.take(member) {
                let value = String::from_utf8(message.payload.to_vec()).unwrap();
                got[i].entry(message.key.clone().unwrap()).or_default().push(value);
                consumers.ack(member, &message.id);
            }
        }
        consumers.advance(Duration::from_millis(100));
    }
    assert!(got.iter().all(|g| !g.is_empty()), "both members get keys: {got:?}");
    for (key, values) in &sent {
        let holders: Vec<&Vec<String>> = got.iter().filter_map(|g| g.get(key)).collect();
        assert_eq!(holders, [values], "{key} whole, on one member, in order");
    }
}
