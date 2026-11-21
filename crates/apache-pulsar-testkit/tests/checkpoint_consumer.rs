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

//! A Checkpoint consumer against the simulated cluster: a reader on every segment, parents read before their
//! children, the end of sealed segments, deleted segments, the latest position, checkpoints and their restore across
//! splits and merges. Ports the scenarios of Java's `ScalableCheckpointConsumerTest`,
//! `ScalableCheckpointConsumerReconcileTest`, `V5CheckpointConsumerBasicTest`, `V5CheckpointConsumerDagReplayTest`,
//! `V5CheckpointConsumerOrderingTest`, `V5CheckpointConsumerSkipDeletedSegmentTest`,
//! `V5CheckpointConsumerUntouchedSegmentTest`, `V5CheckpointConsumerLatestStartTest`, and the ungrouped ones of
//! `V5CheckpointConsumerGroupTest`. The grouped ones come with the grouped mode.

use std::collections::BTreeMap;
use std::time::Duration;

use apache_pulsar_protocol::checkpoint::Checkpoint;
use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::message_id::{EntryId, SegmentVector};
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{KeyValue, MessageMetadata, ServerError, SingleMessageMetadata};
use apache_pulsar_testkit::checkpoint::{CheckpointHarness, values};
use apache_pulsar_testkit::scalable::World;
use buffa::Message;
use bytes::{BufMut, Bytes, BytesMut};

const TOPIC: &str = "topic://acme/orders/events";
/// The readers' subscription.
const READER: &str = "reader-0123456789";
/// Keys of the ordering scenarios.
const KEYS: usize = 8;

fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// The world's name for a segment of [`TOPIC`], from its hash range.
fn name(range: &str, id: u64) -> String {
    format!("segment://acme/orders/events/{range}-{id}")
}

fn options() -> ConsumerOptions {
    ConsumerOptions { subscription: READER.into(), operation_timeout: Duration::from_secs(1), ..Default::default() }
}

fn create(world: World, start: Checkpoint) -> CheckpointHarness {
    CheckpointHarness::new(world, TOPIC, options(), start)
}

/// Creates a consumer on the world `h` leaves, after closing `h`.
fn restore(mut h: CheckpointHarness, start: Checkpoint) -> CheckpointHarness {
    h.close();
    create(std::mem::take(&mut h.world), start)
}

/// Stores a one-message entry in a segment, as any producer would.
fn publish(world: &mut World, segment: u64, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    world.publish_raw(TOPIC, segment, metadata, Bytes::copy_from_slice(value.as_bytes()));
}

/// Stores a batch, as any producer would.
fn publish_batch(world: &mut World, segment: u64, values: &[&str]) {
    let mut payload = BytesMut::new();
    for value in values {
        let single = SingleMessageMetadata { payload_size: value.len() as i32, ..Default::default() };
        payload.put_u32(single.encoded_len());
        payload.put_slice(&single.encode_to_vec());
        payload.put_slice(value.as_bytes());
    }
    let metadata = MessageMetadata {
        producer_name: "other".into(),
        publish_time: 1,
        num_messages_in_batch: Some(values.len() as i32),
        ..Default::default()
    };
    world.publish_raw(TOPIC, segment, metadata, payload.freeze());
}

/// Publishes `rounds` rounds of one message per key, keys interleaved, each in the segment that owns its key, as a
/// producer routes them; and records them per key in `sent`.
fn publish_keyed(world: &mut World, prefix: &str, rounds: usize, sent: &mut BTreeMap<String, Vec<String>>) {
    for i in 0..rounds {
        for k in 0..KEYS {
            let key = format!("key-{k}");
            let value = format!("{key}-{prefix}-{i}");
            let metadata = MessageMetadata {
                producer_name: "other".into(),
                partition_key: Some(key.clone()),
                publish_time: 1,
                ..Default::default()
            };
            let segment = world.route(TOPIC, &key);
            world.publish_raw(TOPIC, segment, metadata, Bytes::from(value.clone()));
            sent.entry(key).or_default().push(value);
        }
    }
}

/// The messages' values, per key, in the order received.
fn by_key(messages: &[ReceivedMessage]) -> BTreeMap<String, Vec<String>> {
    let mut received: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in messages {
        let value = String::from_utf8(message.payload.to_vec()).unwrap();
        received.entry(message.key.clone().unwrap()).or_default().push(value);
    }
    received
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

fn numbered(prefix: &str, range: std::ops::Range<usize>) -> Vec<String> {
    range.map(|i| format!("{prefix}-{i}")).collect()
}

/// The segments a checkpoint names.
fn named(checkpoint: &Checkpoint) -> Vec<i64> {
    let Checkpoint::Positions(positions) = checkpoint else { panic!("not positions: {checkpoint:?}") };
    positions.keys().copied().collect()
}

/// How many `CommandGetLastMessageId` the world got.
fn asked(h: &CheckpointHarness) -> usize {
    h.world.received(Type::GET_LAST_MESSAGE_ID).len()
}

// --- V5CheckpointConsumerBasicTest ---

#[test]
fn reads_from_the_earliest() {
    let mut world = world(1);
    for i in 0..5 {
        publish(&mut world, 0, &format!("msg-{i}"));
    }
    let mut h = create(world, Checkpoint::Earliest);
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.consumer.topic(), TOPIC);
    assert_eq!(h.take_values(), numbered("msg", 0..5));
    assert!(h.take().is_empty());
}

#[test]
fn latest_skips_what_the_segment_had() {
    let mut world = world(1);
    for i in 0..5 {
        publish(&mut world, 0, &format!("pre-{i}"));
    }
    let mut h = create(world, Checkpoint::Latest);
    assert!(h.take().is_empty());
    for i in 0..3 {
        publish(&mut h.world, 0, &format!("post-{i}"));
    }
    assert_eq!(h.take_values(), numbered("post", 0..3));
}

/// `testCheckpointAndResume`, `testCheckpointSerializationRoundtrip` and `testResumesFromClientProvidedCheckpoint`:
/// the harness restores checkpoints from their bytes.
#[test]
fn resumes_from_a_checkpoint() {
    let mut world = world(1);
    for i in 0..10 {
        publish(&mut world, 0, &format!("msg-{i}"));
    }
    let mut first = create(world, Checkpoint::Earliest);
    assert_eq!(values(&first.take_up_to(5)), numbered("msg", 0..5));
    // What the reader fetched beyond the application doesn't count.
    let checkpoint = first.checkpoint();
    let mut resumed = restore(first, checkpoint);
    assert_eq!(resumed.take_values(), numbered("msg", 5..10));
}

/// A checkpoint restored inside a batch reads the rest of the batch, then what follows it.
#[test]
fn a_restore_inside_a_batch_reads_the_rest_of_it() {
    let mut world = world(1);
    publish_batch(&mut world, 0, &["a", "b", "c"]);
    publish(&mut world, 0, "d");
    let mut first = create(world, Checkpoint::Earliest);
    assert_eq!(values(&first.take_up_to(2)), ["a", "b"]);
    let checkpoint = first.checkpoint();
    let mut resumed = restore(first, checkpoint);
    assert_eq!(resumed.take_values(), ["c", "d"]);
}

// --- V5CheckpointConsumerUntouchedSegmentTest ---

/// `testLatestStartKeepsUntouchedSegment`: a checkpoint names every segment the consumer reads, those it received
/// nothing from included, so that a restore doesn't read their history.
#[test]
fn a_checkpoint_names_the_segments_nothing_came_from() {
    let mut world = world(2);
    for i in 0..20 {
        publish(&mut world, i as u64 % 2, &format!("history-{i}"));
    }
    let mut first = create(world, Checkpoint::Latest);
    publish(&mut first.world, 1, "new-0");
    assert_eq!(first.take_values(), ["new-0"]);
    let checkpoint = first.checkpoint();
    assert_eq!(named(&checkpoint), [0, 1]);
    let mut resumed = restore(first, checkpoint);
    assert!(resumed.take().is_empty(), "messages from before the checkpoint");
}

/// `testChainedRestoreKeepsUntouchedSegment`.
#[test]
fn a_chained_restore_names_the_segments_nothing_came_from() {
    let mut world = world(2);
    for i in 0..40 {
        publish(&mut world, i as u64 % 2, &format!("history-{i}"));
    }
    let mut first = create(world, Checkpoint::Earliest);
    assert_eq!(first.take().len(), 40);
    let after_history = first.checkpoint();
    let mut second = restore(first, after_history);
    publish(&mut second.world, 0, "new-0");
    assert_eq!(second.take_values(), ["new-0"]);
    let chained = second.checkpoint();
    let mut resumed = restore(second, chained);
    assert!(resumed.take().is_empty(), "messages from before the checkpoint");
}

/// `testRestoreKeepsSealedSegmentReadToItsEnd`: restored at the end of a sealed parent, a consumer reads nothing of
/// it, and its next checkpoint still names it.
#[test]
fn a_restore_keeps_a_sealed_segment_read_to_its_end() {
    let mut world = world(1);
    for i in 0..20 {
        publish(&mut world, 0, &format!("history-{i}"));
    }
    let mut first = create(world, Checkpoint::Earliest);
    assert_eq!(first.take().len(), 20);
    let at_parent_end = first.checkpoint();
    first.close();
    let mut world = std::mem::take(&mut first.world);
    let (low, _) = world.split(TOPIC, 0);
    let mut second = create(world, at_parent_end);
    publish(&mut second.world, low, "new-0");
    assert_eq!(second.take_values(), ["new-0"]);
    // The parent's reader is let go of once it is known to be at the end.
    assert_eq!(second.consumer.segments().collect::<Vec<_>>(), [1, 2]);
    let chained = second.checkpoint();
    assert_eq!(named(&chained), [0, 1, 2]);
    let mut resumed = restore(second, chained);
    assert!(resumed.take().is_empty(), "messages from before the checkpoint");
}

/// `testCheckpointAtLatestStartMatchesLiveConsumer`: a checkpoint taken before the first message names where the
/// consumer starts, also when messages come in while the readers attach; the restore reads what the consumer read.
#[test]
fn a_checkpoint_at_a_latest_start_restores_what_the_consumer_read() {
    let mut world = world(2);
    for i in 0..10 {
        publish(&mut world, i as u64 % 2, &format!("before-{i}"));
    }
    world.hold_last_message_ids = true;
    let mut live = create(world, Checkpoint::Latest);
    // The readers subscribed at the latest position, and wait to know where that is: no permits yet.
    assert_eq!(live.subscribed, None);
    assert!(live.world.received(Type::FLOW).is_empty());
    for i in 0..3 {
        publish(&mut live.world, i % 2, &format!("between-{i}"));
    }
    live.world.hold_last_message_ids = false;
    live.world.release_last_message_ids();
    live.pump();
    assert_eq!(live.subscribed, Some(Ok(())));
    let at_start = live.checkpoint();
    for i in 0..10 {
        publish(&mut live.world, i as u64 % 2, &format!("after-{i}"));
    }
    let delivered = sorted(live.take_values());
    assert_eq!(delivered, sorted(numbered("after", 0..10)));
    let mut restored = restore(live, at_start);
    assert_eq!(sorted(restored.take_values()), delivered);
}

// --- V5CheckpointConsumerLatestStartTest ---

/// `testReadsSplitChildrenFromTheirBeginning`: what a producer stores in the children before the consumer hears of
/// the split is read.
#[test]
fn a_latest_consumer_reads_the_children_of_a_split_from_their_start() {
    let mut h = create(world(1), Checkpoint::Latest);
    publish(&mut h.world, 0, "before");
    assert_eq!(h.take_values(), ["before"]);
    let (low, high) = h.world.split(TOPIC, 0);
    let mut sent = Vec::new();
    for i in 0..10 {
        let value = format!("post-split-{i}");
        publish(&mut h.world, if i % 2 == 0 { low } else { high }, &value);
        sent.push(value);
    }
    assert_eq!(sorted(h.take_values()), sorted(sent));
}

/// `testReadsMergedChildFromItsBeginning`.
#[test]
fn a_latest_consumer_reads_a_merged_segment_from_its_start() {
    let mut h = create(world(2), Checkpoint::Latest);
    publish(&mut h.world, 1, "before");
    assert_eq!(h.take_values(), ["before"]);
    let merged = h.world.merge(TOPIC, 0, 1);
    for i in 0..5 {
        publish(&mut h.world, merged, &format!("post-merge-{i}"));
    }
    assert_eq!(h.take_values(), numbered("post-merge", 0..5));
}

/// `testSkipsMessagesOfSegmentsPresentAtStart`: a latest consumer reads nothing of the history of a sealed parent and
/// its children, all in its first layout.
#[test]
fn a_latest_consumer_skips_what_the_segments_at_its_start_had() {
    let mut world = world(1);
    publish(&mut world, 0, "pre-split");
    let (low, high) = world.split(TOPIC, 0);
    publish(&mut world, low, "post-split-0");
    publish(&mut world, high, "post-split-1");
    let mut h = create(world, Checkpoint::Latest);
    assert_eq!(h.subscribed, Some(Ok(())));
    // The sealed parent is at its end from the start: the broker says so, and its reader is let go of.
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [low, high]);
    publish(&mut h.world, low, "after-start-0");
    publish(&mut h.world, high, "after-start-1");
    assert_eq!(sorted(h.take_values()), ["after-start-0", "after-start-1"]);
}

// --- V5CheckpointConsumerDagReplayTest ---

/// An earliest consumer created after two generations of splits reads every message, sealed segments included.
#[test]
fn an_earliest_consumer_reads_sealed_parents_and_their_children() {
    let mut world = world(1);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut world, "p1", 3, &mut sent);
    let (low, _) = world.split(TOPIC, 0);
    publish_keyed(&mut world, "p2", 3, &mut sent);
    world.split(TOPIC, low);
    publish_keyed(&mut world, "p3", 3, &mut sent);
    let mut h = create(world, Checkpoint::Earliest);
    assert_eq!(by_key(&h.take()), sent);
}

// --- V5CheckpointConsumerOrderingTest ---

/// `testPerKeyOrderAcrossSplitWhileReading`: the consumer is behind when the split comes, with part of the parent's
/// messages not read yet.
#[test]
fn keeps_per_key_order_across_a_split_while_reading() {
    let options = ConsumerOptions { receiver_queue_size: 10, ..options() };
    let mut h = CheckpointHarness::new(world(1), TOPIC, options, Checkpoint::Earliest);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut h.world, "pre", 20, &mut sent);
    let mut received = h.take_up_to(5);
    h.world.split(TOPIC, 0);
    h.pump();
    publish_keyed(&mut h.world, "post", 5, &mut sent);
    received.extend(h.take());
    assert_eq!(by_key(&received), sent);
}

/// `testPerKeyOrderAcrossMergeWhileReading`.
#[test]
fn keeps_per_key_order_across_a_merge_while_reading() {
    let options = ConsumerOptions { receiver_queue_size: 10, ..options() };
    let mut h = CheckpointHarness::new(world(2), TOPIC, options, Checkpoint::Earliest);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut h.world, "pre", 20, &mut sent);
    let mut received = h.take_up_to(5);
    h.world.merge(TOPIC, 0, 1);
    h.pump();
    publish_keyed(&mut h.world, "post", 5, &mut sent);
    received.extend(h.take());
    assert_eq!(by_key(&received), sent);
}

/// `testSplitWhileCaughtUpDeliversChildren`: the parent is read to its end, and its reader waits for more, when the
/// split seals it.
#[test]
fn delivers_the_children_of_a_split_while_caught_up() {
    let mut h = create(world(1), Checkpoint::Earliest);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut h.world, "pre", 1, &mut sent);
    let mut received = h.take();
    h.world.split(TOPIC, 0);
    publish_keyed(&mut h.world, "post", 1, &mut sent);
    received.extend(h.take());
    assert_eq!(by_key(&received), sent);
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
}

/// `testPerKeyOrderWhenStartingAfterSplits`: two generations of sealed segments before the active ones.
#[test]
fn keeps_per_key_order_starting_after_splits() {
    let mut world = world(1);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut world, "gen0", 5, &mut sent);
    let (low, high) = world.split(TOPIC, 0);
    publish_keyed(&mut world, "gen1", 5, &mut sent);
    world.split(TOPIC, low);
    world.split(TOPIC, high);
    publish_keyed(&mut world, "gen2", 5, &mut sent);
    let mut h = create(world, Checkpoint::Earliest);
    assert_eq!(by_key(&h.take()), sent);
}

/// `testPerKeyOrderResumingCheckpointAcrossSplit`, and `testCheckpointResumeAcrossSplit`.
#[test]
fn keeps_per_key_order_resuming_a_checkpoint_across_a_split() {
    let mut world = world(1);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut world, "pre", 10, &mut sent);
    let mut first = create(world, Checkpoint::Earliest);
    let consumed = by_key(&first.take_up_to(KEYS * 5));
    let checkpoint = first.checkpoint();
    first.close();
    let mut world = std::mem::take(&mut first.world);
    world.split(TOPIC, 0);
    publish_keyed(&mut world, "post", 5, &mut sent);
    let mut resumed = create(world, checkpoint);
    let expected: BTreeMap<String, Vec<String>> = sent
        .iter()
        .map(|(key, values)| (key.clone(), values[consumed.get(key).map_or(0, Vec::len)..].to_vec()))
        .collect();
    assert_eq!(by_key(&resumed.take()), expected);
}

/// `testCheckpointResumeAcrossMerge`: both sealed parents read on from where the checkpoint has them, then the merged
/// segment.
#[test]
fn resumes_a_checkpoint_across_a_merge() {
    let mut world = world(2);
    let mut sent = BTreeMap::new();
    publish_keyed(&mut world, "pre", 4, &mut sent);
    let mut first = create(world, Checkpoint::Earliest);
    let consumed = by_key(&first.take_up_to(KEYS * 2));
    let checkpoint = first.checkpoint();
    first.close();
    let mut world = std::mem::take(&mut first.world);
    world.merge(TOPIC, 0, 1);
    publish_keyed(&mut world, "post", 2, &mut sent);
    let mut resumed = create(world, checkpoint);
    let expected: BTreeMap<String, Vec<String>> = sent
        .iter()
        .map(|(key, values)| (key.clone(), values[consumed.get(key).map_or(0, Vec::len)..].to_vec()))
        .collect();
    assert_eq!(by_key(&resumed.take()), expected);
}

/// `testDeletedParentDoesNotHoldChildren`: the parent's topic is gone while the layout still has it.
#[test]
fn a_deleted_parent_does_not_hold_its_children() {
    let mut world = world(1);
    publish(&mut world, 0, "pre");
    let (low, high) = world.split(TOPIC, 0);
    for i in 0..4 {
        publish(&mut world, if i % 2 == 0 { low } else { high }, &format!("post-{i}"));
    }
    world.delete_segment(TOPIC, 0);
    let mut h = create(world, Checkpoint::Earliest);
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(sorted(h.take_values()), numbered("post", 0..4));
    assert_eq!(named(&h.checkpoint()), [1, 2]);
}

// --- V5CheckpointConsumerSkipDeletedSegmentTest ---

/// A segment whose topic is gone is skipped: the create succeeds, and the others are read.
#[test]
fn skips_a_deleted_segment() {
    let mut world = world(2);
    for i in 0..10 {
        publish(&mut world, i as u64 % 2, &format!("msg-{i}"));
    }
    world.delete_segment(TOPIC, 0);
    let mut h = create(world, Checkpoint::Earliest);
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.take_values(), ["msg-1", "msg-3", "msg-5", "msg-7", "msg-9"]);
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1]);
    assert_eq!(h.subscribes[&0], 1, "not subscribed again");
}

/// A segment deleted while it is read is skipped from then on, and holds back no child.
#[test]
fn a_segment_deleted_while_read_is_skipped() {
    let mut h = create(world(1), Checkpoint::Earliest);
    publish(&mut h.world, 0, "a");
    publish(&mut h.world, 0, "b");
    assert_eq!(values(&h.take_up_to(1)), ["a"]);
    let (low, _) = h.world.split(TOPIC, 0);
    publish(&mut h.world, low, "child");
    h.pump();
    // "b" is not read yet when the parent's topic goes.
    h.world.delete_segment(TOPIC, 0);
    h.pump();
    h.advance(Duration::from_millis(105));
    assert_eq!(h.take_values(), ["child"]);
    assert_eq!(named(&h.checkpoint()), [1, 2]);
}

// --- ScalableCheckpointConsumerTest, ScalableCheckpointConsumerReconcileTest ---

/// Java's lookup of the latest position can fail: it is asked again, and the reader waits meanwhile.
#[test]
fn asks_again_for_a_last_message_id_the_broker_failed_to_give() {
    let mut world = world(1);
    publish(&mut world, 0, "old");
    world.fail_last_message_ids.push_back(ServerError::ServiceNotReady);
    let mut h = create(world, Checkpoint::Latest);
    assert_eq!((h.subscribed.clone(), asked(&h)), (None, 1));
    h.advance(Duration::from_millis(999));
    assert_eq!(asked(&h), 1);
    h.advance(Duration::from_millis(1));
    assert_eq!((h.subscribed.clone(), asked(&h)), (Some(Ok(())), 2));
    publish(&mut h.world, 0, "new");
    assert_eq!(h.take_values(), ["new"]);
}

/// `testLateLookupRecordsNoPositionForARemovedSegment`, ungrouped: a segment the layout drops while its latest
/// position is asked leaves no position behind.
#[test]
fn a_late_last_message_id_names_nothing_for_a_segment_the_layout_dropped() {
    let mut world = world(1);
    publish(&mut world, 0, "old");
    world.split(TOPIC, 0);
    world.hold_last_message_ids = true;
    let mut h = create(world, Checkpoint::Latest);
    h.world.prune(TOPIC, 0);
    h.pump();
    h.world.hold_last_message_ids = false;
    h.world.release_last_message_ids();
    h.pump();
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(named(&h.checkpoint()), [1, 2]);
}

/// The end of a segment sealed while its latest position is asked is asked once more: an answer from before the seal
/// isn't final.
#[test]
fn asks_again_for_the_end_of_a_segment_sealed_while_its_position_was_asked() {
    let mut world = world(1);
    world.hold_last_message_ids = true;
    let mut h = create(world, Checkpoint::Latest);
    assert_eq!(asked(&h), 1);
    let (low, _) = h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(asked(&h), 1, "one ask at a time");
    h.world.hold_last_message_ids = false;
    h.world.release_last_message_ids();
    h.pump();
    assert_eq!((h.subscribed.clone(), asked(&h)), (Some(Ok(())), 2));
    publish(&mut h.world, low, "child");
    assert_eq!(h.take_values(), ["child"]);
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
}

/// A last message id asked on a subscription the broker closed meanwhile is asked again on the next one.
#[test]
fn asks_again_for_a_last_message_id_lost_with_its_subscription() {
    let mut world = world(1);
    world.hold_last_message_ids = true;
    let mut h = create(world, Checkpoint::Latest);
    h.world.unload(TOPIC, 0);
    h.pump();
    h.world.hold_last_message_ids = false;
    // The held one is answered that the reader is gone.
    h.world.release_last_message_ids();
    h.advance(Duration::from_millis(105));
    assert_eq!((h.subscribed.clone(), asked(&h), h.subscribes[&0]), (Some(Ok(())), 2, 2));
}

/// `retriesAReaderThatFailedToAttach`: a reader that can't subscribe after a split is attached again after the
/// backoff; the other child's is left alone.
#[test]
fn attaches_again_a_reader_that_failed_to_subscribe() {
    let mut h = create(world(1), Checkpoint::Earliest);
    h.world.fail_subscribes_to.insert(name("0000-7fff", 1), [ServerError::ConsumerBusy].into());
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!((h.subscribes[&1], h.subscribes[&2]), (1, 1));
    let retry = h.consumer.poll_timeout().expect("a retry") - h.clock.now();
    assert!((Duration::from_millis(90)..=Duration::from_millis(110)).contains(&retry), "{retry:?}");
    h.advance(retry);
    assert_eq!((h.subscribes[&1], h.subscribes[&2]), (2, 1));
    assert_eq!(h.consumer.poll_timeout(), None);
    publish(&mut h.world, 1, "child");
    assert_eq!(h.take_values(), ["child"]);
}

/// `keepsAtMostOneRetryScheduled`: a reader that keeps failing gets one retry at a time, further apart each time.
#[test]
fn a_reader_that_keeps_failing_gets_one_retry_at_a_time() {
    let mut h = create(world(1), Checkpoint::Earliest);
    let busy = [ServerError::ConsumerBusy, ServerError::ConsumerBusy, ServerError::ConsumerBusy];
    h.world.fail_subscribes_to.insert(name("0000-7fff", 1), busy.into());
    h.world.split(TOPIC, 0);
    h.pump();
    let mut delays = Vec::new();
    for attempt in 2..=4 {
        let retry = h.consumer.poll_timeout().expect("a retry") - h.clock.now();
        delays.push(retry);
        h.advance(retry);
        assert_eq!(h.subscribes[&1], attempt);
    }
    assert!(delays[0] < delays[1] && delays[1] < delays[2], "{delays:?}");
    assert_eq!(h.consumer.poll_timeout(), None);
}

/// A new layout attaches at once the readers waiting for a retry.
#[test]
fn a_new_layout_attaches_at_once_the_readers_that_gave_up() {
    let mut h = create(world(2), Checkpoint::Earliest);
    h.world.fail_subscribes_to.insert(name("0000-7fff", 0), [ServerError::ConsumerBusy].into());
    h.world.unload(TOPIC, 0);
    h.pump();
    // The reader subscribes again after its own backoff, and gives up.
    h.advance(Duration::from_millis(105));
    assert_eq!(h.subscribes[&0], 2);
    assert!(h.consumer.poll_timeout().is_some(), "a retry");
    h.world.split(TOPIC, 1);
    h.pump();
    assert_eq!(h.subscribes[&0], 3);
    assert_eq!(h.consumer.poll_timeout(), None);
}

/// `closesARevokedSegmentWhileAnotherReaderAttaches`, ungrouped: a segment the layout drops is let go of at once,
/// while another reader still subscribes.
#[test]
fn lets_go_of_a_segment_the_layout_drops_while_another_reader_subscribes() {
    let mut h = create(world(1), Checkpoint::Earliest);
    // Not read: segment 0 isn't at its end.
    publish(&mut h.world, 0, "a");
    h.world.hold_subscribes_to.insert(name("0000-7fff", 1));
    h.world.split(TOPIC, 0);
    h.pump();
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.world.consumer_count(TOPIC, 0, READER), 0);
    assert_eq!(named(&h.checkpoint()), [1, 2]);
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
}

// --- Readers ---

#[test]
fn each_reader_is_exclusive_and_not_durable() {
    let mut world = world(1);
    world.split(TOPIC, 0);
    let options = ConsumerOptions {
        consumer_name: Some("c".into()),
        // A reader is Exclusive, at the latest initial position, whatever it was given.
        sub_type: SubType::Shared,
        initial_position: InitialPosition::Earliest,
        ..options()
    };
    let mut h = CheckpointHarness::new(world, TOPIC, options, Checkpoint::Earliest);
    assert_eq!(h.subscribed, Some(Ok(())));
    let subscribes = h.world.received(Type::SUBSCRIBE);
    let names: Vec<_> = subscribes.iter().map(|c| c.subscribe.consumer_name.clone().unwrap()).collect();
    assert_eq!(names, ["c-seg-0", "c-seg-1", "c-seg-2"]);
    for command in subscribes {
        let subscribe = &command.subscribe;
        assert_eq!((subscribe.subscription.as_str(), subscribe.subType), (READER, SubType::Exclusive));
        assert_eq!((subscribe.durable, subscribe.initialPosition), (Some(false), Some(InitialPosition::Latest)));
        assert_eq!(EntryId::from_proto(&subscribe.start_message_id), EntryId::EARLIEST);
        assert!(subscribe.metadata.is_empty());
    }
    h.close();
    assert!(h.closed && h.consumer.is_closed());
    // A reader's subscription goes with it.
    for segment in 0..3 {
        assert_eq!(h.world.consumer_count(TOPIC, segment, READER), 0);
    }
}

#[test]
fn readers_on_legacy_segments_are_marked_as_managed_by_a_v5_client() {
    let mut world = World::default();
    world.create_legacy_topic("persistent://acme/orders/legacy", 2);
    let h = CheckpointHarness::new(world, "persistent://acme/orders/legacy", options(), Checkpoint::Earliest);
    assert_eq!(h.subscribed, Some(Ok(())));
    let managed = KeyValue { key: "__pulsar.v5.managed".into(), value: "true".into(), ..Default::default() };
    for command in h.world.received(Type::SUBSCRIBE) {
        assert_eq!(command.subscribe.metadata, std::slice::from_ref(&managed));
    }
}

/// A reader that loses its connection subscribes again after the last message taken: the broker sends its entry
/// again, which the reader leaves out.
#[test]
fn a_reader_resumes_after_the_last_message_taken() {
    let mut world = world(1);
    for i in 0..10 {
        publish(&mut world, 0, &format!("msg-{i}"));
    }
    let options = ConsumerOptions { receiver_queue_size: 4, ..options() };
    let mut h = CheckpointHarness::new(world, TOPIC, options, Checkpoint::Earliest);
    let taken = h.take_up_to(3);
    assert_eq!(values(&taken), numbered("msg", 0..3));
    h.drop_connection();
    h.advance(Duration::from_millis(105));
    let resubscribe = h.world.received(Type::SUBSCRIBE).last().unwrap().subscribe.clone();
    assert_eq!(EntryId::from_proto(&resubscribe.start_message_id), taken[2].id.entry);
    assert_eq!(h.take_values(), numbered("msg", 3..10));
    // The broker closing it, as an unload does, is the same.
    h.world.unload(TOPIC, 0);
    h.pump();
    publish(&mut h.world, 0, "msg-10");
    h.advance(Duration::from_millis(105));
    assert_eq!(h.take_values(), ["msg-10"]);
}

/// A reader at the end of a sealed segment that loses its connection before it knows the end asks for it again.
#[test]
fn a_reader_at_the_end_of_a_sealed_segment_finds_it_again_after_a_reconnect() {
    let mut h = create(world(1), Checkpoint::Earliest);
    publish(&mut h.world, 0, "a");
    assert_eq!(h.take_values(), ["a"]);
    h.world.hold_last_message_ids = true;
    let (low, _) = h.world.split(TOPIC, 0);
    h.pump();
    h.drop_connection();
    h.world.hold_last_message_ids = false;
    h.advance(Duration::from_millis(105));
    publish(&mut h.world, low, "child");
    assert_eq!(h.take_values(), ["child"]);
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
}

/// Messages given back by a receive that went away no longer count as delivered.
#[test]
fn a_checkpoint_rewinds_to_before_the_messages_given_back() {
    let mut world = world(1);
    for i in 0..3 {
        publish(&mut world, 0, &format!("msg-{i}"));
    }
    let mut h = create(world, Checkpoint::Earliest);
    let first = h.consumer.next_message().unwrap();
    let previous = h.consumer.deliver(&first.id).unwrap();
    assert_eq!(previous, EntryId::EARLIEST);
    let second = h.consumer.next_message().unwrap();
    let before_second = h.consumer.deliver(&second.id).unwrap();
    assert_eq!(before_second, first.id.entry);
    h.consumer.rewind(second.id.segment_id, before_second);
    let checkpoint = h.checkpoint();
    assert_eq!(checkpoint, Checkpoint::Positions(SegmentVector::from([(0, first.id.entry)])));
    let mut resumed = restore(h, checkpoint);
    assert_eq!(resumed.take_values(), ["msg-1", "msg-2"]);
}

/// Delivered or given back, a message of a segment the consumer no longer reads changes nothing.
#[test]
fn messages_of_segments_it_no_longer_reads_change_no_checkpoint() {
    let mut h = create(world(1), Checkpoint::Earliest);
    publish(&mut h.world, 0, "a");
    h.pump();
    let message = h.consumer.next_message().unwrap();
    h.world.split(TOPIC, 0);
    h.world.delete_segment(TOPIC, 0);
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.consumer.deliver(&message.id), None);
    h.consumer.rewind(0, EntryId::EARLIEST);
    assert_eq!(named(&h.checkpoint()), [1, 2]);
}

// --- Create and close ---

#[test]
fn the_create_fails_and_closes_every_reader_if_one_cannot_subscribe() {
    let mut world = world(2);
    world.fail_subscribes_to.insert(name("8000-ffff", 1), [ServerError::AuthorizationError].into());
    let h = create(world, Checkpoint::Earliest);
    let Some(Err(RegisterError::Server { error, .. })) = &h.subscribed else { panic!("{:?}", h.subscribed) };
    assert_eq!(*error, ServerError::AuthorizationError);
    assert!(h.closed);
    assert_eq!(h.world.consumer_count(TOPIC, 0, READER), 0);
}

/// The create waits for the readers of a layout that comes meanwhile.
#[test]
fn the_create_waits_for_the_readers_of_layouts_that_come_meanwhile() {
    let mut world = world(1);
    world.hold_subscribes = true;
    let mut h = create(world, Checkpoint::Earliest);
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribed, None);
    h.world.hold_subscribes = false;
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
}

/// A reader that subscribed once and gives up while others still subscribe doesn't fail the create: it is attached
/// again.
#[test]
fn a_reader_that_gives_up_after_subscribing_does_not_fail_the_create() {
    let mut world = world(2);
    world.hold_subscribes_to.insert(name("8000-ffff", 1));
    let mut h = create(world, Checkpoint::Earliest);
    h.world.fail_subscribes_to.insert(name("0000-7fff", 0), [ServerError::ConsumerBusy].into());
    h.world.unload(TOPIC, 0);
    h.pump();
    h.advance(Duration::from_millis(105));
    assert_eq!((h.subscribed.clone(), h.subscribes[&0]), (None, 2));
    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.advance(Duration::from_millis(110));
    assert_eq!((h.subscribed.clone(), h.subscribes[&0]), (Some(Ok(())), 3));
}

#[test]
fn close_while_creating_closes_each_reader_once_its_subscribe_completes() {
    let mut world = world(2);
    world.hold_subscribes = true;
    let mut h = create(world, Checkpoint::Earliest);
    h.close();
    assert!(h.closed && h.consumer.is_closed());
    h.world.hold_subscribes = false;
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.world.received(Type::CLOSE_CONSUMER).len(), 2);
    assert_eq!(h.subscribed, None);
    // Closing again does nothing.
    h.close();
    assert_eq!(h.world.received(Type::CLOSE_CONSUMER).len(), 2);
}

/// A closed consumer follows no layout.
#[test]
fn a_closed_consumer_follows_no_layout() {
    let mut h = create(world(1), Checkpoint::Earliest);
    h.close();
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.world.received(Type::SUBSCRIBE).len(), 1);
}

/// A child's messages wait while its parent isn't read to its end: they aren't available yet.
#[test]
fn a_child_waits_until_its_parent_is_read_to_its_end() {
    let mut h = create(world(1), Checkpoint::Earliest);
    publish(&mut h.world, 0, "parent");
    let (low, _) = h.world.split(TOPIC, 0);
    publish(&mut h.world, low, "child");
    h.pump();
    assert_eq!(h.consumer.available(), 1);
    assert_eq!(values(&h.take_up_to(1)), ["parent"]);
    assert_eq!(h.consumer.available(), 1);
    assert_eq!(h.take_values(), ["child"]);
}

/// A latest position inside a batch names the batch's last message: the batch isn't read again.
#[test]
fn a_latest_start_after_a_batch_reads_what_follows_it() {
    let mut world = world(1);
    publish_batch(&mut world, 0, &["a", "b", "c"]);
    let mut first = create(world, Checkpoint::Latest);
    let at_start = first.checkpoint();
    let Checkpoint::Positions(positions) = &at_start else { panic!("{at_start:?}") };
    assert_eq!(positions[&0].batch_index, 2);
    publish(&mut first.world, 0, "d");
    assert_eq!(first.take_values(), ["d"]);
    let mut restored = restore(first, at_start);
    assert_eq!(restored.take_values(), ["d"]);
}

/// The reader of a segment the layout dropped, closing when its connection goes, is closed all the same.
#[test]
fn a_reader_closing_when_its_connection_goes_is_closed() {
    let mut h = create(world(1), Checkpoint::Earliest);
    // Not read: segment 0 isn't at its end.
    publish(&mut h.world, 0, "a");
    h.world.split(TOPIC, 0);
    h.pump();
    h.world.prune(TOPIC, 0);
    h.drop_connection();
    h.advance(Duration::from_millis(105));
    assert_eq!(h.consumer.segments().collect::<Vec<_>>(), [1, 2]);
    assert_eq!(h.subscribes[&0], 1, "not subscribed again");
}

/// Readers that give up together share one retry.
#[test]
fn readers_that_give_up_together_share_one_retry() {
    let mut h = create(world(2), Checkpoint::Earliest);
    for (range, id) in [("0000-7fff", 0), ("8000-ffff", 1)] {
        h.world.fail_subscribes_to.insert(name(range, id), [ServerError::ConsumerBusy].into());
    }
    h.drop_connection();
    h.advance(Duration::from_millis(105));
    assert_eq!((h.subscribes[&0], h.subscribes[&1]), (2, 2));
    let retry = h.consumer.poll_timeout().expect("one retry") - h.clock.now();
    h.advance(retry);
    assert_eq!((h.subscribes[&0], h.subscribes[&1]), (3, 3));
    assert_eq!(h.consumer.poll_timeout(), None);
}

/// A subscribe lost with its connection is sent again.
#[test]
fn a_subscribe_lost_with_its_connection_is_sent_again() {
    let mut world = world(1);
    world.hold_subscribes = true;
    let mut h = create(world, Checkpoint::Earliest);
    h.world.hold_subscribes = false;
    h.drop_connection();
    h.advance(Duration::from_millis(105));
    assert_eq!((h.subscribed.clone(), h.subscribes[&0]), (Some(Ok(())), 2));
}
