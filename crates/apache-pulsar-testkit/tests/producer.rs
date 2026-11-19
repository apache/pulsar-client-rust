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

//! The scalable producer's state machine against the simulated world, deterministically.
//!
//! Ported scenarios from the Java client's `ScalableTopicProducerOrderingTest`, `V5ProducerSegmentGoneTest`,
//! `V5ProducerAccessModeTest`, `V5ProducerLayoutChangeOrderingTest` and `V5ScalableTopicAutoCreateTest`, plus the
//! segment producer's own behavior: encoding, sequence ids, receipts, reconnects and the send timeout.

use std::collections::BTreeMap;
use std::time::Duration;

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::producer::{OutgoingMessage, SendError};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{ProducerAccessMode, ServerError};
use apache_pulsar_testkit::producer::{Harness, options};
use apache_pulsar_testkit::scalable::World;

const TOPIC: &str = "topic://acme/orders/events";

fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// A key whose segment hash falls in `[start, end]`.
fn key_in(start: u16, end: u16, nth: usize) -> String {
    (0..)
        .map(|i| format!("key-{i}"))
        .filter(|k| (start..=end).contains(&KeyHash::of(k.as_bytes()).segment_hash()))
        .nth(nth)
        .unwrap()
}

/// The values stored per key, in the order a reader would see them.
fn by_key(harness: &Harness) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for stored in harness.world.stored(TOPIC) {
        let value = String::from_utf8(stored.value.to_vec()).unwrap();
        out.entry(stored.key.unwrap_or_default()).or_default().push(value);
    }
    out
}

fn values_in(harness: &Harness, segment: u64) -> Vec<String> {
    harness.world.stored_in(TOPIC, segment).iter().map(|s| String::from_utf8(s.value.to_vec()).unwrap()).collect()
}

#[test]
fn sends_a_stamped_batch_of_one_per_message() {
    let mut h = Harness::new(world(1), TOPIC, options());
    assert_eq!(h.created, Some(Ok(())));
    // Segment producers are created lazily: none yet.
    assert!(h.world.received(Type::PRODUCER).is_empty());
    let keyed = h.send(Some("user-42"), "hello");
    let keyless = h.send(None, "world");
    let id = h.result(keyed).unwrap().as_ref().unwrap().clone();
    assert_eq!(id.segment_id, 0);
    assert_eq!((id.entry.batch_index, id.entry.batch_size), (0, 1));
    assert!(h.result(keyless).unwrap().is_ok());
    // One registration, named after the producer.
    let registrations = h.world.received(Type::PRODUCER);
    assert_eq!(registrations.len(), 1);
    assert_eq!(registrations[0].producer.producer_name.as_deref(), Some("p-seg-0"));
    assert_eq!(registrations[0].producer.topic, "segment://acme/orders/events/0000-ffff-0");

    let stored = h.world.stored(TOPIC);
    assert_eq!(stored.len(), 2);
    for (entry, (key, sequence)) in stored.iter().zip([(Some("user-42"), 0u64), (None, 1)]) {
        let metadata = &entry.metadata;
        assert_eq!(metadata.producer_name, "p-seg-0");
        assert_eq!(metadata.sequence_id, sequence);
        assert_eq!(metadata.num_messages_in_batch, Some(1));
        let bucket = key.map_or(0, |k| u32::from(KeyHash::of(k.as_bytes()).bucket_hash()));
        assert_eq!((metadata.entry_hash_min, metadata.entry_hash_max), (Some(bucket), Some(bucket)));
        assert_eq!(entry.key.as_deref(), key);
    }
    // The message's own metadata, in the batch payload.
    let first = &stored[0];
    assert_eq!(&first.value[..], b"hello");
    let single = first.single.as_ref().expect("a batch");
    assert_eq!(single.partition_key.as_deref(), Some("user-42"));
    assert_eq!(single.partition_key_b64_encoded, Some(false));
    assert_eq!((single.payload_size, single.sequence_id), (5, Some(0)));
    assert_eq!(stored[1].single.as_ref().unwrap().partition_key, None);
    let send = h.world.received(Type::SEND)[0].send.clone();
    assert_eq!((send.sequence_id, send.highest_sequence_id, send.num_messages), (0, Some(0), None));
}

#[test]
fn routes_keys_by_hash_and_keyless_round_robin() {
    let mut h = Harness::new(world(2), TOPIC, options());
    let low = key_in(0, 0x7fff, 0);
    let high = key_in(0x8000, 0xffff, 0);
    let sends = [h.send(Some(&low), "l"), h.send(Some(&high), "h"), h.send(None, "x"), h.send(None, "y")];
    let segments: Vec<u64> = sends.iter().map(|&s| h.segment_of(s)).collect();
    assert_eq!(&segments[..2], [0, 1]);
    // Keyless goes round-robin over the active segments, in hash order.
    assert_eq!(&segments[2..], [0, 1]);
    // Each segment has its own producer and its own sequence ids.
    assert_eq!(h.world.stored_in(TOPIC, 0).iter().map(|s| s.sequence_id).collect::<Vec<_>>(), [0, 1]);
    assert_eq!(h.world.stored_in(TOPIC, 1).iter().map(|s| s.sequence_id).collect::<Vec<_>>(), [0, 1]);
    assert_eq!(h.registrations.values().copied().collect::<Vec<_>>(), [1, 1]);
}

/// Java: `resendsTheSendsOfASealedSegmentBeforeTheNewerOnes`. a1 and b1 are in flight on segment 0 when it
/// splits; a2 and b2 come after. Each successor gets the older message first.
#[test]
fn a_split_resends_in_flight_messages_before_newer_ones() {
    let mut h = Harness::new(world(1), TOPIC, options());
    let (a, b) = (key_in(0, 0x7fff, 0), key_in(0x8000, 0xffff, 0));
    h.send(Some(&a), "a0");
    h.world.hold_sends = true;
    let a1 = h.send(Some(&a), "a1");
    let b1 = h.send(Some(&b), "b1");
    h.world.split(TOPIC, 0);
    h.pump();
    let a2 = h.send(Some(&a), "a2");
    let b2 = h.send(Some(&b), "b2");
    // The held sends reach the broker after the producer was closed: ignored, as the broker does.
    h.world.hold_sends = false;
    h.world.release_sends();
    h.run_until(Duration::from_secs(5), |h| h.done.len() == 5);
    assert_eq!(values_in(&h, 0), ["a0"]);
    assert_eq!(values_in(&h, 1), ["a1", "a2"]);
    assert_eq!(values_in(&h, 2), ["b1", "b2"]);
    // Each id names the segment the message landed on.
    assert_eq!([a1, a2, b1, b2].map(|s| h.segment_of(s)), [1, 1, 2, 2]);
    assert_eq!(h.producer.outstanding(), 0);
}

/// Java: `holdsOnlyTheSendsOfTheKeysOfTheRetiringSegment`. Segment 1 is terminated before any new layout: a1 is
/// parked and a2, of the same key, held; other keys and keyless sends keep flowing. Once segment 1 splits, a1 and
/// a2 land on its successor, in order.
#[test]
fn only_the_keys_of_a_retiring_segment_are_held() {
    let mut h = Harness::new(world(2), TOPIC, options());
    let a = key_in(0, 0x7fff, 0);
    let b = key_in(0x8000, 0xffff, 0);
    h.send(Some(&a), "a0");
    h.send(Some(&b), "b0");
    h.world.hold_sends = true;
    let a1 = h.send(Some(&a), "a1");
    h.world.terminate(TOPIC, 0);
    h.world.hold_sends = false;
    h.world.release_sends();
    // The producer reconnects and finds the segment terminated.
    h.run_until(Duration::from_secs(5), |h| h.world.received(Type::PRODUCER).len() >= 3);
    let a2 = h.send(Some(&a), "a2");
    let b1 = h.send(Some(&b), "b1");
    let k1 = h.send(None, "k1");
    let k2 = h.send(None, "k2");
    // Other keys, and keyless sends, are not held. A keyless send that lands on the gone segment waits too.
    assert!(h.result(b1).is_some_and(|r| r.is_ok()));
    assert!(h.result(a1).is_none() && h.result(a2).is_none());
    assert!([k1, k2].iter().any(|&k| h.result(k).is_some_and(|r| r.is_ok())));
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    h.run_until(Duration::from_secs(5), |h| h.producer.outstanding() == 0);
    assert_eq!(
        values_in(&h, low).into_iter().chain(values_in(&h, high)).filter(|v| v.starts_with('a')).collect::<Vec<_>>(),
        ["a1", "a2"]
    );
    assert_eq!(h.segment_of(a1), h.segment_of(a2));
}

/// Java: `V5ProducerLayoutChangeOrderingTest`, on the simulated broker: many keys, a split mid-stream, every key in
/// call order.
#[test]
fn per_key_order_holds_across_a_split_with_traffic() {
    per_key_order(1, |h| {
        h.world.split(TOPIC, 0);
    });
}

#[test]
fn per_key_order_holds_across_a_merge_with_traffic() {
    per_key_order(2, |h| {
        h.world.merge(TOPIC, 0, 1);
    });
}

#[test]
fn per_key_order_holds_across_consecutive_splits() {
    per_key_order(1, |h| {
        let (low, _) = h.world.split(TOPIC, 0);
        h.world.split(TOPIC, low);
    });
}

fn per_key_order(segments: u32, change: impl FnOnce(&mut Harness)) {
    let mut h = Harness::new(world(segments), TOPIC, options());
    let keys: Vec<String> = (0..40).map(|i| format!("k{i}")).collect();
    let mut sent: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut send = |h: &mut Harness, round: usize| {
        for key in &keys {
            let value = format!("{key}-{round}");
            h.send(Some(key), &value);
            sent.entry(key.clone()).or_default().push(value);
        }
    };
    for round in 0..5 {
        send(&mut h, round);
    }
    // Some in flight when the layout changes.
    h.world.hold_sends = true;
    for round in 5..8 {
        send(&mut h, round);
    }
    change(&mut h);
    h.pump();
    for round in 8..10 {
        send(&mut h, round);
    }
    h.world.hold_sends = false;
    h.world.release_sends();
    h.pump();
    for round in 10..15 {
        send(&mut h, round);
    }
    h.run_until(Duration::from_secs(10), |h| h.producer.outstanding() == 0);
    assert!(h.done.values().all(Result::is_ok), "a send failed");
    assert_eq!(by_key(&h), sent);
    // Without this the test could pass without the layout change touching any send.
    assert!(h.producer.sends_parked_or_held() > 0, "no send waited for the layout change");
}

/// Java: `aSealedSegmentGetsNoNewProducerForTheSendsThatFindItGone`. A segment terminated without a new layout gets
/// no new producer for the sends that find it gone, and they fail once the retry budget is spent.
#[test]
fn a_gone_segment_gets_no_new_producer_and_its_sends_fail_in_the_end() {
    let mut h = Harness::new(world(1), TOPIC, options());
    h.send(None, "first");
    h.world.terminate(TOPIC, 0);
    h.pump();
    let sends: Vec<u64> = (0..5).map(|i| h.send(None, &format!("m{i}"))).collect();
    // One reconnect, refused: no producer after that.
    h.run_until(Duration::from_secs(60), |h| sends.iter().all(|s| h.result(*s).is_some()));
    assert_eq!(h.registrations[&0], 2);
    for send in sends {
        assert!(
            matches!(h.result(send), Some(Err(SendError::Server { error: ServerError::TopicTerminatedError, .. }))),
            "{:?}",
            h.result(send)
        );
    }
}

#[test]
fn reconnecting_writes_unacknowledged_messages_again_with_their_sequence_ids() {
    let mut w = world(1);
    w.dedup = true;
    let mut h = Harness::new(w, TOPIC, options());
    h.send(Some("k"), "m0");
    h.world.hold_sends = true;
    let pending: Vec<u64> = (1..4).map(|i| h.send(Some("k"), &format!("m{i}"))).collect();
    h.drop_connection();
    h.world.hold_sends = false;
    h.run_until(Duration::from_secs(5), |h| pending.iter().all(|s| h.result(*s).is_some()));
    let stored = h.world.stored_in(TOPIC, 0);
    assert_eq!(stored.iter().map(|s| s.sequence_id).collect::<Vec<_>>(), [0, 1, 2, 3]);
    assert_eq!(values_in(&h, 0), ["m0", "m1", "m2", "m3"]);
    assert_eq!(h.producer.last_sequence_id(), 3);
}

#[test]
fn sequence_ids_start_after_the_initial_or_the_brokers_last() {
    let mut opts = options();
    opts.initial_sequence_id = Some(10);
    let mut h = Harness::new(world(1), TOPIC, opts);
    assert_eq!(h.producer.last_sequence_id(), 10);
    h.send(None, "a");
    assert_eq!(h.world.stored_in(TOPIC, 0)[0].sequence_id, 11);
    assert_eq!(h.producer.last_sequence_id(), 11);

    // Without one, a fresh producer continues from what the broker stored for its name (deduplication).
    let mut w = world(1);
    w.dedup = true;
    let mut first = Harness::new(w, TOPIC, options());
    for i in 0..3 {
        first.send(None, &format!("{i}"));
    }
    first.close();
    let mut second = Harness::new(first.world, TOPIC, options());
    second.send(None, "next");
    assert_eq!(second.world.stored_in(TOPIC, 0).last().unwrap().sequence_id, 3);
}

#[test]
fn an_application_sequence_id_is_kept() {
    let mut h = Harness::new(world(1), TOPIC, options());
    let message = OutgoingMessage { sequence_id: Some(77), payload: "x".into(), ..Default::default() };
    h.producer.send(message, 0, h.clock.now()).unwrap();
    h.pump();
    h.send(None, "y");
    let sequences: Vec<u64> = h.world.stored_in(TOPIC, 0).iter().map(|s| s.sequence_id).collect();
    // The counter is not moved by an application's id.
    assert_eq!(sequences, [77, 0]);
}

#[test]
fn the_send_timeout_fails_late_messages_and_the_producer_carries_on() {
    let mut opts = options();
    opts.send_timeout = Some(Duration::from_secs(3));
    let mut h = Harness::new(world(1), TOPIC, opts);
    h.send(None, "registered first");
    h.world.hold_receipts = true;
    let late = h.send(None, "late");
    h.advance(Duration::from_secs(2));
    assert!(h.result(late).is_none());
    h.advance(Duration::from_secs(2));
    assert_eq!(h.result(late), Some(&Err(SendError::Timeout)));
    h.world.hold_receipts = false;
    h.world.release_receipts();
    h.pump();
    let next = h.send(None, "next");
    assert!(h.result(next).unwrap().is_ok());
}

#[test]
fn a_message_above_the_brokers_limit_fails_alone() {
    let mut h = Harness::new(world(1), TOPIC, options());
    let big = "x".repeat(6 << 20);
    let too_large = h.send(None, &big);
    let fine = h.send(None, "fine");
    assert!(matches!(h.result(too_large), Some(Err(SendError::TooLarge { .. }))));
    assert!(h.result(fine).unwrap().is_ok());
}

// --- Close ---

/// Java: `closingTheClientFailsTheWaitingSendsWhenTheirReleaseIsDropped` and V5ProducerSegmentGoneTest's
/// `closingTheClientFailsTheSendsWaitingForTheLayout`: parked and held sends fail with AlreadyClosed at once.
#[test]
fn close_fails_waiting_sends_and_awaits_written_ones() {
    let mut h = Harness::new(world(2), TOPIC, options());
    let a = key_in(0, 0x7fff, 0);
    let b = key_in(0x8000, 0xffff, 0);
    h.send(Some(&a), "a0");
    h.send(Some(&b), "b0");
    h.world.terminate(TOPIC, 0);
    h.pump();
    let a1 = h.send(Some(&a), "a1");
    h.run_until(Duration::from_secs(5), |h| h.world.received(Type::PRODUCER).len() >= 3);
    let a2 = h.send(Some(&a), "a2");
    // Written to segment 1, receipt still on its way.
    h.world.hold_receipts = true;
    let b1 = h.send(Some(&b), "b1");
    h.close();
    assert_eq!(h.result(a1), Some(&Err(SendError::AlreadyClosed)));
    assert_eq!(h.result(a2), Some(&Err(SendError::AlreadyClosed)));
    // The broker answers the close once its write is done: b1 succeeds.
    h.world.release_receipts();
    h.pump();
    assert!(h.result(b1).unwrap().is_ok());
    assert!(h.closed);
    assert_eq!(h.producer.send(OutgoingMessage::default(), 0, h.clock.now()), Err(SendError::AlreadyClosed));
}

/// Java: `closingTheClientDoesNotWaitForASegmentProducerThatIsNotCreatedYet`: close completes while a
/// wait-for-exclusive registration is pending; the late producer is closed unused.
#[test]
fn close_does_not_wait_for_a_pending_registration() {
    let mut w = world(1);
    let mut incumbent = Harness::new(std::mem::take(&mut w), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    assert_eq!(incumbent.created, Some(Ok(())));
    let mut waiting =
        Harness::new(std::mem::take(&mut incumbent.world), TOPIC, exclusive(ProducerAccessMode::WaitForExclusive));
    assert_eq!(waiting.created, None);
    waiting.close();
    assert!(waiting.closed);
    assert_eq!(waiting.created, Some(Err(SendError::AlreadyClosed)));
    assert!(waiting.world.received(Type::SEND).is_empty());
}

// --- Access modes ---

fn exclusive(mode: ProducerAccessMode) -> apache_pulsar_protocol::producer::ProducerOptions {
    let mut opts = options();
    opts.access_mode = mode;
    opts
}

/// Java: `testExclusiveRejectsSecondProducer`: the conflict fails create, on every segment claimed up front.
#[test]
fn exclusive_claims_every_segment_at_create() {
    let mut first = Harness::new(world(2), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    assert_eq!(first.created, Some(Ok(())));
    assert_eq!(first.registrations.len(), 2, "both segments claimed before any send");
    let second = Harness::new(std::mem::take(&mut first.world), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    assert!(
        matches!(&second.created, Some(Err(SendError::Server { error: ServerError::ProducerFenced, .. }))),
        "{:?}",
        second.created
    );
}

/// A create that fails on one segment lets go of the others it claimed, as Java's does by closing.
#[test]
fn a_failed_exclusive_create_releases_what_it_claimed() {
    let low = key_in(0, 0x7fff, 0);
    let high = key_in(0x8000, 0xffff, 0);
    // A shared producer on the upper segment only.
    let mut shared = Harness::new(world(2), TOPIC, options());
    shared.send(Some(&high), "shared");
    let mut exclusive_attempt =
        Harness::new(std::mem::take(&mut shared.world), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    assert!(matches!(exclusive_attempt.created, Some(Err(_))), "{:?}", exclusive_attempt.created);
    assert!(exclusive_attempt.closed, "a failed create closes the producer");
    // The lower segment is free again for anyone.
    let mut other = Harness::new(std::mem::take(&mut exclusive_attempt.world), TOPIC, options());
    let sent = other.send(Some(&low), "free");
    assert!(other.result(sent).unwrap().is_ok(), "{:?}", other.result(sent));
}

/// Java: `testWaitForExclusiveSucceedsAfterFirstReleases`.
#[test]
fn wait_for_exclusive_is_created_once_the_holder_leaves() {
    let mut first = Harness::new(world(1), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    let world = std::mem::take(&mut first.world);
    let mut second = Harness::new(world, TOPIC, exclusive(ProducerAccessMode::WaitForExclusive));
    assert_eq!(second.created, None);
    // The holder leaves: the world hands the topic to the waiting producer.
    first.world = std::mem::take(&mut second.world);
    first.close();
    second.world = std::mem::take(&mut first.world);
    second.pump();
    assert_eq!(second.created, Some(Ok(())));
    let sent = second.send(Some("k"), "mine now");
    assert!(second.result(sent).unwrap().is_ok());
}

// --- Topics ---

#[test]
fn a_legacy_topic_routes_like_v4_partitions() {
    let topic = "persistent://acme/orders/legacy";
    let mut w = World::default();
    w.create_legacy_topic(topic, 3);
    let mut h = Harness::new(w, topic, options());
    for key in ["a", "b", "c", "d", "e"] {
        let send = h.send(Some(key), key);
        assert_eq!(h.segment_of(send), u64::from(KeyHash::of(key.as_bytes()).partition(3)), "{key}");
    }
    let registration = &h.world.received(Type::PRODUCER)[0].producer;
    assert!(registration.topic.starts_with("persistent://acme/orders/legacy-partition-"));
    assert!(registration.metadata.iter().any(|kv| kv.key == "__pulsar.v5.managed" && kv.value == "true"));
}

// --- Send errors ---

/// A checksum error, or any refusal but a terminated topic or a forbidden message: the producer registers again
/// and writes the message again, and it is stored once.
#[test]
fn a_refused_send_is_written_again_after_registering_again() {
    let mut h = Harness::new(world(1), TOPIC, options());
    h.send(None, "first");
    h.world.fail_sends.push_back(ServerError::ChecksumError);
    let refused = h.send(None, "second");
    h.run_until(Duration::from_secs(5), |h| h.result(refused).is_some());
    assert!(h.result(refused).unwrap().is_ok());
    assert_eq!(values_in(&h, 0), ["first", "second"]);
    assert_eq!(h.registrations[&0], 2, "registered again");
}

#[test]
fn a_forbidden_message_fails_alone() {
    let mut h = Harness::new(world(1), TOPIC, options());
    h.world.fail_sends.push_back(ServerError::NotAllowedError);
    let forbidden = h.send(None, "forbidden");
    let fine = h.send(None, "fine");
    assert!(matches!(h.result(forbidden), Some(Err(SendError::Server { error: ServerError::NotAllowedError, .. }))));
    assert!(h.result(fine).unwrap().is_ok());
    assert_eq!(values_in(&h, 0), ["fine"]);
}

/// A send refused because its segment's topic was terminated: the message moves to the successor once the layout
/// says so.
#[test]
fn a_send_refused_by_a_terminated_topic_waits_for_the_layout() {
    let mut h = Harness::new(world(1), TOPIC, options());
    h.send(None, "first");
    h.world.fail_sends.push_back(ServerError::TopicTerminatedError);
    let refused = h.send(None, "moved");
    assert!(h.result(refused).is_none());
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    h.run_until(Duration::from_secs(5), |h| h.result(refused).is_some());
    assert!([low, high].contains(&h.segment_of(refused)));
}

// --- Access modes, continued ---

/// A newer producer with `ExclusiveWithFencing` fences the older one out: the older one's return, with the epoch
/// it was given, is refused, and its sends fail.
#[test]
fn exclusive_with_fencing_fences_the_older_producer() {
    let mut older = Harness::new(world(1), TOPIC, exclusive(ProducerAccessMode::ExclusiveWithFencing));
    older.send(None, "before");
    let mut newer =
        Harness::new(std::mem::take(&mut older.world), TOPIC, exclusive(ProducerAccessMode::ExclusiveWithFencing));
    assert_eq!(newer.created, Some(Ok(())));
    older.world = std::mem::take(&mut newer.world);
    older.pump();
    let late = older.send(None, "after");
    older.run_until(Duration::from_secs(5), |h| h.result(late).is_some());
    assert!(
        matches!(older.result(late), Some(Err(SendError::Server { error: ServerError::ProducerFenced, .. }))),
        "{:?}",
        older.result(late)
    );
    newer.world = std::mem::take(&mut older.world);
    let mine = newer.send(None, "mine");
    assert!(newer.result(mine).unwrap().is_ok());
    assert_eq!(values_in(&newer, 0), ["before", "mine"]);
}

/// A shared producer can't write while an exclusive one holds the topic.
#[test]
fn a_shared_producer_is_refused_while_an_exclusive_one_holds_the_topic() {
    let mut holder = Harness::new(world(1), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    let mut shared = Harness::new(std::mem::take(&mut holder.world), TOPIC, options());
    let refused = shared.send(None, "refused");
    assert!(matches!(shared.result(refused), Some(Err(SendError::Server { error: ServerError::ProducerBusy, .. }))));
}

/// A producer waiting for exclusive access leaves the line when its connection drops, and joins it again.
#[test]
fn a_waiting_producer_that_reconnects_waits_again() {
    let mut holder = Harness::new(world(1), TOPIC, exclusive(ProducerAccessMode::Exclusive));
    let mut waiting =
        Harness::new(std::mem::take(&mut holder.world), TOPIC, exclusive(ProducerAccessMode::WaitForExclusive));
    waiting.drop_connection();
    waiting.run_until(Duration::from_secs(5), |h| h.registrations[&0] >= 2);
    assert_eq!(waiting.created, None);
    holder.world = std::mem::take(&mut waiting.world);
    holder.close();
    waiting.world = std::mem::take(&mut holder.world);
    waiting.pump();
    assert_eq!(waiting.created, Some(Ok(())));
}

#[test]
fn a_non_partitioned_legacy_topic_is_one_segment() {
    let topic = "persistent://acme/orders/plain";
    let mut w = World::default();
    w.create_legacy_topic(topic, 0);
    let mut h = Harness::new(w, topic, options());
    for key in ["a", "b", "c"] {
        let send = h.send(Some(key), key);
        assert_eq!(h.segment_of(send), 0);
    }
    assert_eq!(h.world.received(Type::PRODUCER)[0].producer.topic, topic);
}
