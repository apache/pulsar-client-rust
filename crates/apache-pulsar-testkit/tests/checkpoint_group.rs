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

//! The members of a group of Checkpoint consumers against the simulated cluster: the controller's assignment, the
//! segments that move between members, the latest position, checkpoints, the registration and leaving the group.
//! Ports the grouped scenarios of Java's `V5CheckpointConsumerGroupTest`, and the managed ones of
//! `ScalableCheckpointConsumerTest` and `ScalableCheckpointConsumerReconcileTest`.

use std::collections::BTreeSet;
use std::time::Duration;

use apache_pulsar_protocol::checkpoint::Checkpoint;
use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::message_id::{EntryId, SegmentVector};
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_subscribe::SubType;
use apache_pulsar_protocol::proto::{KeyValue, MessageMetadata, ServerError};
use apache_pulsar_testkit::checkpoint::GroupHarness;
use apache_pulsar_testkit::scalable::World;
use bytes::Bytes;

const TOPIC: &str = "topic://acme/orders/events";
const GROUP: &str = "audit";

fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// The world's name for a segment of [`TOPIC`], from its hash range.
fn name(range: &str, id: u64) -> String {
    format!("segment://acme/orders/events/{range}-{id}")
}

/// A member's readers' options: each member's readers have a subscription of their own, as the client picks one.
fn options(reader: &str) -> ConsumerOptions {
    ConsumerOptions { subscription: reader.into(), operation_timeout: Duration::from_secs(1), ..Default::default() }
}

/// Stores a one-message entry in a segment, as any producer would.
fn publish(world: &mut World, segment: u64, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    world.publish_raw(TOPIC, segment, metadata, Bytes::copy_from_slice(value.as_bytes()));
}

/// Publishes `count` keyed messages, each in the segment that owns its key, as a producer routes them. Returns them.
fn publish_keyed(world: &mut World, prefix: &str, count: usize) -> BTreeSet<String> {
    let mut sent = BTreeSet::new();
    for i in 0..count {
        let key = format!("k-{i}");
        let hash = u32::from(KeyHash::of(key.as_bytes()).bucket_hash());
        let metadata = MessageMetadata {
            producer_name: "other".into(),
            partition_key: Some(key.clone()),
            entry_hash_min: Some(hash),
            entry_hash_max: Some(hash),
            publish_time: 1,
            ..Default::default()
        };
        let value = format!("{prefix}-{i}");
        let segment = world.route(TOPIC, &key);
        world.publish_raw(TOPIC, segment, metadata, Bytes::from(value.clone()));
        sent.insert(value);
    }
    sent
}

/// The segments a checkpoint names.
fn named(checkpoint: &Checkpoint) -> Vec<i64> {
    let Checkpoint::Positions(positions) = checkpoint else { panic!("not positions: {checkpoint:?}") };
    positions.keys().copied().collect()
}

/// The segments the controller assigns to a member now, in id order.
fn assigned(h: &GroupHarness, member: &str) -> Vec<u64> {
    let mut segments: Vec<u64> =
        h.world.assigned(TOPIC, GROUP, member).into_iter().map(|(segment, _)| segment).collect();
    segments.sort();
    segments
}

// --- V5CheckpointConsumerGroupTest ---

/// Each member reads its own segments, and each message reaches one member.
#[test]
fn members_of_a_group_share_the_segments() {
    let mut h = GroupHarness::new(world(4), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    assert_eq!((h.members[a].subscribed.clone(), h.members[b].subscribed.clone()), (Some(Ok(())), Some(Ok(()))));
    assert_eq!((assigned(&h, "member-0"), assigned(&h, "member-1")), (vec![0, 2], vec![1, 3]));
    let sent = publish_keyed(&mut h.world, "v", 100);

    let got_a: BTreeSet<String> = h.take_values(a).into_iter().collect();
    let got_b: BTreeSet<String> = h.take_values(b).into_iter().collect();

    assert!(!got_a.is_empty() && !got_b.is_empty(), "a={} b={}", got_a.len(), got_b.len());
    assert!(got_a.is_disjoint(&got_b));
    assert_eq!(got_a.union(&got_b).cloned().collect::<BTreeSet<_>>(), sent);
    // A member's checkpoint names its own segments.
    assert_eq!((named(&h.checkpoint(a)), named(&h.checkpoint(b))), (vec![0, 2], vec![1, 3]));
    assert_eq!(h.members[a].consumer.segments().collect::<Vec<_>>(), [0, 2]);
}

/// With more members than segments, the segment goes to one member, and the others idle: a reader can't share it.
#[test]
fn members_outnumbering_the_segments_deliver_each_message_once() {
    let mut h = GroupHarness::new(world(1), TOPIC, GROUP);
    let members: Vec<usize> = ["member-0", "member-1", "member-2"]
        .iter()
        .zip(["reader-a", "reader-b", "reader-c"])
        .map(|(member, reader)| h.join(member, options(reader), Checkpoint::Earliest))
        .collect();
    let sent = publish_keyed(&mut h.world, "v", 100);

    let got: Vec<Vec<String>> = members.iter().map(|m| h.take_values(*m)).collect();

    assert!(members.iter().all(|m| h.members[*m].subscribed == Some(Ok(()))));
    assert_eq!(got.iter().map(Vec::len).collect::<Vec<_>>(), [100, 0, 0]);
    assert_eq!(got[0].iter().cloned().collect::<BTreeSet<_>>(), sent);
    assert_eq!(h.world.consumer_count(TOPIC, 0, "reader-a"), 1);
}

/// A member that joins at the latest position reads what comes after it joined, in the segments it gets.
#[test]
fn a_member_joining_at_the_latest_position_reads_what_follows() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    let first = publish_keyed(&mut h.world, "p1", 30);
    assert_eq!(h.take_values(a).into_iter().collect::<BTreeSet<_>>(), first);

    let b = h.join("member-1", options("reader-b"), Checkpoint::Latest);
    let second = publish_keyed(&mut h.world, "p2", 30);
    let got_a: BTreeSet<String> = h.take_values(a).into_iter().collect();
    let got_b: BTreeSet<String> = h.take_values(b).into_iter().collect();

    assert!(!got_a.is_empty() && !got_b.is_empty(), "a={} b={}", got_a.len(), got_b.len());
    assert_eq!(got_a.union(&got_b).cloned().collect::<BTreeSet<_>>(), second);
    assert!(got_a.is_disjoint(&got_b));
}

/// Members that went away leave nothing behind once the controller's grace period ends: a newcomer gets every
/// segment.
#[test]
fn members_that_went_away_leave_no_registration_behind() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    h.crash(a);
    h.crash(b);
    // Within their grace period, they keep their share.
    let c = h.join("member-2", options("reader-c"), Checkpoint::Earliest);
    assert_eq!(h.members[c].subscribed, Some(Ok(())));
    assert!(assigned(&h, "member-2").is_empty());

    h.world.end_grace(TOPIC, GROUP, "member-0");
    h.world.end_grace(TOPIC, GROUP, "member-1");
    h.pump();
    publish(&mut h.world, 0, "zero");
    publish(&mut h.world, 1, "one");

    assert_eq!(assigned(&h, "member-2"), [0, 1]);
    assert_eq!(h.take_values(c), ["zero", "one"]);
}

// --- V5CheckpointConsumerDagReplayTest and V5CheckpointConsumerUntouchedSegmentTest, grouped ---

/// A member created at the earliest position after a split reads the sealed parent's backlog and the children's: the
/// controller holds no segment of a group back.
#[test]
fn an_earliest_member_created_after_a_split_reads_the_sealed_backlog() {
    let mut world = world(1);
    let mut sent = publish_keyed(&mut world, "pre", 30);
    world.split(TOPIC, 0);
    sent.extend(publish_keyed(&mut world, "post", 30));
    let mut h = GroupHarness::new(world, TOPIC, GROUP);

    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);

    assert_eq!(assigned(&h, "member-0"), [0, 1, 2]);
    assert_eq!(h.take_values(a).into_iter().collect::<BTreeSet<_>>(), sent);
}

#[test]
fn an_earliest_member_created_after_two_splits_reads_all_the_backlog() {
    let mut world = world(1);
    let mut sent = publish_keyed(&mut world, "p1", 20);
    let (_, high) = world.split(TOPIC, 0);
    sent.extend(publish_keyed(&mut world, "p2", 20));
    world.split(TOPIC, high);
    sent.extend(publish_keyed(&mut world, "p3", 20));
    let mut h = GroupHarness::new(world, TOPIC, GROUP);

    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);

    assert_eq!(assigned(&h, "member-0").len(), 5);
    assert_eq!(h.take_values(a).into_iter().collect::<BTreeSet<_>>(), sent);
}

/// The checkpoint of a member started at the latest position names the segments it got nothing from: restored, in
/// another group that assigns it every segment at once, it reads nothing from before the checkpoint.
#[test]
fn a_latest_member_checkpoint_names_the_segments_nothing_came_from() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    publish_keyed(&mut h.world, "history", 40);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    publish(&mut h.world, 0, "new");
    assert_eq!(h.take_values(a), ["new"]);
    let checkpoint = h.checkpoint(a);
    assert_eq!(named(&checkpoint), [0, 1]);
    h.close(a);

    let mut resumed = GroupHarness::new(std::mem::take(&mut h.world), TOPIC, "audit-resumed");
    let b = resumed.join("member-0", options("reader-b"), checkpoint);

    assert_eq!(resumed.members[b].subscribed, Some(Ok(())));
    assert!(resumed.take_values(b).is_empty());
}

// --- ScalableCheckpointConsumerTest and ScalableCheckpointConsumerReconcileTest, managed ---

/// A segment moved to another member gives nothing more, not even what its reader had in: it leaves the checkpoint,
/// and its reader closes. The other member reads it from its own start.
#[test]
fn a_segment_moved_away_gives_nothing_more_and_leaves_the_checkpoint() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    publish(&mut h.world, 0, "zero");
    publish(&mut h.world, 1, "one");
    h.pump();
    assert_eq!(h.members[a].consumer.available(), 2);

    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);

    assert_eq!(h.take_values(a), ["zero"]);
    assert_eq!(named(&h.checkpoint(a)), [0]);
    assert_eq!(h.world.consumer_count(TOPIC, 1, "reader-a"), 0);
    assert_eq!(h.take_values(b), ["one"]);
}

/// The readers of the segments an assignment moves away close at once, while the reader of another still
/// subscribes.
#[test]
fn closes_a_segment_moved_away_while_another_reader_subscribes() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.hold_subscribes_to.insert(name("8000-ffff", 1));
    let b = h.join("member-b", options("reader-b"), Checkpoint::Earliest);
    assert_eq!((h.members[b].subscribed.clone(), h.world.consumer_count(TOPIC, 0, "reader-b")), (None, 1));

    // Sorted first by name, the newcomer takes segment 0.
    let a = h.join("member-a", options("reader-a"), Checkpoint::Earliest);

    assert_eq!(h.world.consumer_count(TOPIC, 0, "reader-b"), 0);
    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    assert_eq!(h.members[b].subscribed, None);
    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.members[b].subscribed, Some(Ok(())));
}

/// A message taken from a segment's reader before the segment moved away, and given to the application only after, as
/// a batch receive that waits for more does, moves no checkpoint and rewinds none: not once the segment left, and not
/// once it came back, with a new reader that starts elsewhere.
#[test]
fn a_message_of_an_earlier_attachment_moves_no_checkpoint() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    publish(&mut h.world, 1, "old");
    h.pump();
    let old = h.members[a].consumer.next_message().unwrap();
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    assert_eq!(h.members[a].consumer.deliver(&old), None);
    assert_eq!(named(&h.checkpoint(a)), [0]);

    publish(&mut h.world, 1, "while-away");
    h.close(b);
    let start = h.checkpoint(a);
    assert_eq!(named(&start), [0, 1]);
    assert_eq!(h.members[a].consumer.deliver(&old), None);
    h.members[a].consumer.rewind(1, old.attachment, EntryId::EARLIEST);

    assert_eq!(h.checkpoint(a), start);
    publish(&mut h.world, 1, "new");
    assert_eq!(h.take_values(a), ["new"]);
}

/// A segment that comes back while its last reader still closes gets a new reader once that one is closed: the broker
/// would refuse a second one on the member's subscription. The new reader starts at the member's start, as in Java.
#[test]
fn a_segment_that_comes_back_waits_for_its_last_reader_to_close() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    publish(&mut h.world, 1, "one");
    let segment = name("8000-ffff", 1);
    h.world.hold_subscribes_to.insert(segment.clone());
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    // Segment 1 comes back while member-0's last reader of it still waits for its subscribe.
    h.close(b);
    assert_eq!(assigned(&h, "member-0"), [0, 1]);
    assert_eq!(h.members[a].subscribes[&1], 1);

    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.pump();

    assert_eq!(h.members[a].subscribes[&1], 2);
    assert_eq!(h.world.consumer_count(TOPIC, 1, "reader-a"), 1);
    assert_eq!(h.take_values(a), ["one"]);
    assert!(h.members[b].closed);
}

/// A segment that leaves while its latest position is asked gets no position from the late answer; one that comes
/// back is resolved anew, by its new reader, which waits for that answer.
#[test]
fn a_late_latest_position_reaches_no_later_reader() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.hold_last_message_ids = true;
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    assert_eq!(h.members[a].subscribed, None);
    h.close(b);
    // The last reader is closed, but its latest position is still asked: the new reader waits for the answer.
    assert_eq!(h.world.consumer_count(TOPIC, 1, "reader-a"), 0);
    assert_eq!(h.members[a].subscribes[&1], 1);
    publish(&mut h.world, 1, "before");

    h.world.hold_last_message_ids = false;
    h.world.release_last_message_ids();
    h.pump();
    publish(&mut h.world, 1, "after");

    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    assert_eq!(h.members[a].subscribes[&1], 2);
    assert_eq!(h.world.received(Type::GET_LAST_MESSAGE_ID).len(), 3);
    assert_eq!(h.take_values(a), ["after"]);
}

/// The answer to a latest position asked by a reader moved away names nothing: the checkpoint names only the
/// member's segments.
#[test]
fn a_late_latest_position_names_nothing_for_a_segment_moved_away() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.hold_last_message_ids = true;
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    h.world.hold_last_message_ids = false;
    h.world.release_last_message_ids();
    h.pump();

    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    assert_eq!(named(&h.checkpoint(a)), [0]);
}

/// A latest position the broker failed to give is asked again only while the segment stays: one moved away is not.
#[test]
fn a_failed_latest_position_is_not_asked_again_for_a_segment_moved_away() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.fail_last_message_ids.extend([ServerError::ServiceNotReady, ServerError::ServiceNotReady]);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    assert_eq!(h.members[a].subscribed, None);
    h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    h.advance(Duration::from_secs(1));

    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    assert_eq!(h.world.received(Type::GET_LAST_MESSAGE_ID).len(), 3);
    assert_eq!(named(&h.checkpoint(a)), [0]);
    assert_eq!(h.members[a].consumer.poll_timeout(), None);
}

/// A member at the latest position starts each segment it gets at its end then, also one that comes back to it.
#[test]
fn a_latest_member_starts_each_segment_it_gets_at_its_end() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    publish(&mut h.world, 0, "old-0");
    publish(&mut h.world, 1, "old-1");
    let a = h.join("member-0", options("reader-a"), Checkpoint::Latest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    assert_eq!(h.take_values(b), ["old-1"]);
    publish(&mut h.world, 1, "while-away");
    h.close(b);
    publish(&mut h.world, 0, "new-0");
    publish(&mut h.world, 1, "new-1");

    assert_eq!(h.take_values(a), ["new-0", "new-1"]);
}

/// A member restored from its checkpoint reads on after it in the segments it names, and from their start in the
/// others.
#[test]
fn a_restored_member_reads_on_from_its_checkpoint() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    publish(&mut h.world, 0, "zero-0");
    publish(&mut h.world, 0, "zero-1");
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    assert_eq!(h.take_values(a), ["zero-0", "zero-1"]);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    let checkpoint = h.checkpoint(a);
    assert_eq!(named(&checkpoint), [0]);
    h.close(a);
    h.close(b);
    publish(&mut h.world, 0, "zero-2");
    publish(&mut h.world, 1, "one-0");

    let c = h.join("member-0", options("reader-c"), checkpoint);

    assert_eq!(h.take_values(c), ["zero-2", "one-0"]);
}

/// A group has no parents first: the controller assigns a split's children at once, and a member reads them as their
/// messages come, the parent's included, as Java's does.
#[test]
fn a_member_reads_the_children_of_a_split_without_waiting_for_the_parent() {
    let mut h = GroupHarness::new(world(1), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    publish(&mut h.world, 0, "parent-0");
    publish(&mut h.world, 0, "parent-1");
    let (low, high) = h.world.split(TOPIC, 0);
    publish(&mut h.world, low, "low-0");
    publish(&mut h.world, high, "high-0");
    h.pump();

    assert_eq!(h.members[a].consumer.segments().collect::<Vec<_>>(), [0, low, high]);
    assert_eq!(h.take_values(a), ["parent-0", "low-0", "high-0", "parent-1"]);
    // No end of a sealed segment is asked for: no child waits for it.
    assert!(h.world.received(Type::GET_LAST_MESSAGE_ID).is_empty());
}

/// Each reader of a member is named after it, on a subscription of the member's that is Exclusive and not durable,
/// and marked as managed by a V5 client on a legacy segment.
#[test]
fn readers_are_named_after_their_member() {
    let topic = "persistent://acme/orders/legacy";
    let mut world = World::default();
    world.create_legacy_topic(topic, 2);
    let mut h = GroupHarness::new(world, topic, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    assert_eq!(h.members[a].subscribed, Some(Ok(())));

    let managed = KeyValue { key: "__pulsar.v5.managed".into(), value: "true".into(), ..Default::default() };
    let subscribes = h.world.received(Type::SUBSCRIBE);
    let names: Vec<&str> = subscribes.iter().map(|c| c.subscribe.consumer_name.as_deref().unwrap()).collect();
    assert_eq!(names, ["member-0-seg-0", "member-0-seg-1"]);
    for command in subscribes {
        let subscribe = &command.subscribe;
        assert_eq!((subscribe.subscription.as_str(), subscribe.subType), ("reader-a", SubType::Exclusive));
        assert_eq!(subscribe.durable, Some(false));
        assert_eq!(subscribe.metadata, std::slice::from_ref(&managed));
    }
    let registration = &h.world.received(Type::SCALABLE_TOPIC_SUBSCRIBE)[0].scalableTopicSubscribe;
    assert_eq!((registration.subscription.as_str(), registration.consumer_name.as_str()), (GROUP, "member-0"));
}

// --- The registration ---

/// The create completes once the controller registered the member, and every segment it assigned is read.
#[test]
fn the_create_completes_once_registered() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.controller.hold_registrations = true;
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    assert_eq!(h.members[a].subscribed, None);
    assert!(h.world.received(Type::SUBSCRIBE).is_empty());

    h.world.release_registrations();
    h.pump();

    assert_eq!(h.members[a].subscribed, Some(Ok(())));
    assert_eq!(h.members[a].consumer.segments().collect::<Vec<_>>(), [0, 1]);
}

/// A registration the controller refuses fails the create, and closes the consumer.
#[test]
fn a_refused_registration_fails_the_create() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.controller.fail_registrations.push_back(ServerError::AuthorizationError);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);

    let Some(Err(RegisterError::Server { error, .. })) = &h.members[a].subscribed else { panic!() };
    assert_eq!(*error, ServerError::AuthorizationError);
    assert!(h.members[a].closed && h.members[a].consumer.is_closed());
    assert!(h.world.received(Type::SUBSCRIBE).is_empty());
}

/// A connection lost before the first registration is answered fails the create, as in Java.
#[test]
fn a_connection_lost_before_the_registration_fails_the_create() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.controller.hold_registrations = true;
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    h.drop_connection(a);

    assert_eq!(h.members[a].subscribed, Some(Err(RegisterError::Unavailable("disconnected".into()))));
    assert!(h.members[a].closed);
}

/// A member that loses its connection registers again, and keeps its segments within the grace period: its readers
/// read on where they stopped.
#[test]
fn a_member_registers_again_after_losing_its_connection() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    publish(&mut h.world, 0, "zero-0");
    assert_eq!(h.take_values(a), ["zero-0"]);

    h.drop_connection(a);
    publish(&mut h.world, 0, "zero-1");
    publish(&mut h.world, 1, "one-0");
    h.advance(Duration::from_secs(1));

    assert_eq!(h.world.received(Type::SCALABLE_TOPIC_SUBSCRIBE).len(), 2);
    assert_eq!(assigned(&h, "member-0"), [0, 1]);
    let mut got = h.take_values(a);
    got.sort();
    assert_eq!(got, ["one-0", "zero-1"]);
}

/// Closing leaves the group at once: the other members get its segments.
#[test]
fn close_leaves_the_group() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    let b = h.join("member-1", options("reader-b"), Checkpoint::Earliest);
    publish(&mut h.world, 0, "zero");

    h.close(a);

    assert!(h.members[a].closed);
    assert_eq!(h.world.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE).len(), 1);
    assert_eq!(assigned(&h, "member-1"), [0, 1]);
    assert_eq!(h.take_values(b), ["zero"]);
}

/// A member closed while it registers leaves the group once the registration is answered.
#[test]
fn close_while_registering_leaves_once_registered() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    h.world.controller.hold_registrations = true;
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    h.close(a);
    assert!(!h.members[a].closed);

    h.world.release_registrations();
    h.pump();

    assert!(h.members[a].closed);
    assert_eq!(h.members[a].subscribed, None);
    assert!(h.world.received(Type::SUBSCRIBE).is_empty());
    assert_eq!(h.world.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE).len(), 1);
}

/// A close whose connection goes before the controller answers is complete all the same: the grace period takes
/// over.
#[test]
fn a_close_whose_connection_goes_is_complete() {
    let mut h = GroupHarness::new(world(2), TOPIC, GROUP);
    let a = h.join("member-0", options("reader-a"), Checkpoint::Earliest);
    h.members[a].consumer.close();
    h.drop_connection(a);

    assert!(h.members[a].closed);
    assert!(h.world.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE).is_empty());
}

/// A member started at a checkpoint that names none of its segments reads them from their start.
#[test]
fn a_checkpoint_naming_none_of_its_segments_reads_them_from_their_start() {
    let mut h = GroupHarness::new(world(1), TOPIC, GROUP);
    publish(&mut h.world, 0, "zero");
    let elsewhere = Checkpoint::Positions(SegmentVector::from([(7, EntryId::EARLIEST)]));
    let a = h.join("member-0", options("reader-a"), elsewhere);

    assert_eq!(h.take_values(a), ["zero"]);
}
