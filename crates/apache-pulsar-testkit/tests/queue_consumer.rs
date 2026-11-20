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

//! A Queue consumer against the simulated cluster: every segment of the layout, in step with splits, merges and
//! garbage collection; acks routed by segment; retries; the all-or-nothing subscribe; close. Ports the scenarios of
//! Java's `ScalableQueueConsumerReconcileTest`, `V5QueueConsumerBasicTest`, `V5QueueConsumerMultiSegmentAckTest`,
//! `V5SubscriptionInitialPositionTest`, `V5DAGFollowingTest`, `V5SegmentSplitTest`, `V5SegmentMergeTest`,
//! `V5ConsumerReceiverQueueSizeTest`, `V5ConsumerSubscriptionPropertiesTest` and `V5DagWatchAutoReconnectTest`.

use std::time::Duration;

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{KeyValue, MessageMetadata, ServerError};
use apache_pulsar_testkit::queue::QueueHarness;
use apache_pulsar_testkit::scalable::World;
use bytes::Bytes;

const TOPIC: &str = "topic://acme/orders/events";
const SUB: &str = "workers";

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
    ConsumerOptions {
        subscription: SUB.into(),
        initial_position: InitialPosition::Earliest,
        negative_ack_delay: Duration::from_secs(1),
        operation_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn latest() -> ConsumerOptions {
    ConsumerOptions { initial_position: InitialPosition::Latest, ..options() }
}

/// Stores a one-message entry in a segment of [`TOPIC`], as any producer would.
fn publish(world: &mut World, segment: u64, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    world.publish_raw(TOPIC, segment, metadata, Bytes::copy_from_slice(value.as_bytes()));
}

fn segments(h: &QueueHarness) -> Vec<u64> {
    h.queue.segments().collect()
}

fn positions(h: &QueueHarness) -> Vec<Option<InitialPosition>> {
    h.world.received(Type::SUBSCRIBE).iter().map(|c| c.subscribe.initialPosition).collect()
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue { key: key.into(), value: value.into(), ..Default::default() }
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

/// How long until the consumer's next deadline.
fn next_deadline(h: &QueueHarness) -> Duration {
    h.queue.poll_timeout().expect("a deadline") - h.clock.now()
}

#[test]
fn receives_from_every_segment_and_acknowledges_on_each() {
    let mut world = world(4);
    for segment in 0..4 {
        for i in 0..2 {
            publish(&mut world, segment, &format!("v-{segment}-{i}"));
        }
    }
    let mut h = QueueHarness::new(world, TOPIC, options());
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.queue.topic(), TOPIC);
    assert_eq!(segments(&h), [0, 1, 2, 3]);
    let taken = h.take();
    assert_eq!(taken.len(), 8);
    for message in &taken {
        let value = String::from_utf8(message.payload.to_vec()).unwrap();
        assert!(value.starts_with(&format!("v-{}-", message.id.segment_id)), "{value} from {:?}", message.id);
        h.ack(&message.id);
    }
    h.advance(Duration::from_millis(100));
    for segment in 0..4 {
        assert_eq!(h.world.backlog(TOPIC, segment, SUB), 0, "segment {segment}");
    }
    h.close();
    assert!(h.closed);
    // Subscribing again finds nothing left.
    let mut again = QueueHarness::new(h.world, TOPIC, options());
    assert!(again.take().is_empty());
}

#[test]
fn a_consumer_subscribing_again_gets_only_what_was_not_acknowledged() {
    let mut world = world(4);
    for i in 0..12 {
        publish(&mut world, i % 4, &format!("v-{i}"));
    }
    let mut h = QueueHarness::new(world, TOPIC, options());
    let taken = h.take();
    assert_eq!(taken.len(), 12);
    let (acked, unacked) = taken.split_at(5);
    for message in acked {
        h.ack(&message.id);
    }
    // The acks go out with the close.
    h.close();
    let unacked: Vec<String> = unacked.iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect();
    let mut again = QueueHarness::new(h.world, TOPIC, options());
    assert_eq!(sorted(again.take_values()), sorted(unacked));
}

#[test]
fn segments_with_messages_take_turns() {
    let mut world = world(2);
    for i in 0..3 {
        publish(&mut world, 0, &format!("a{i}"));
    }
    publish(&mut world, 1, "b0");
    let mut h = QueueHarness::new(world, TOPIC, options());
    assert_eq!(h.queue.available(), 4);
    assert_eq!(h.take_values(), ["a0", "b0", "a1", "a2"]);
}

#[test]
fn latest_skips_what_the_segments_had_and_earliest_does_not() {
    let mut world = world(1);
    for i in 0..3 {
        publish(&mut world, 0, &format!("pre-{i}"));
    }
    let mut late = QueueHarness::new(world, TOPIC, ConsumerOptions { subscription: "late".into(), ..latest() });
    assert!(late.take().is_empty());
    publish(&mut late.world, 0, "post");
    assert_eq!(late.take_values(), ["post"]);
    let mut early = QueueHarness::new(late.world, TOPIC, options());
    assert_eq!(early.take_values(), ["pre-0", "pre-1", "pre-2", "post"]);
}

#[test]
fn a_negative_ack_brings_the_message_back_after_the_delay() {
    let mut world = world(2);
    publish(&mut world, 1, "a");
    let mut h = QueueHarness::new(world, TOPIC, options());
    let first = h.take();
    h.nack(&first[0].id);
    h.advance(Duration::from_millis(999));
    assert!(h.take().is_empty());
    h.advance(Duration::from_millis(1));
    let again = h.take();
    assert_eq!((&again[0].id, again[0].redelivery_count), (&first[0].id, 1));
}

#[test]
fn each_segment_consumer_gets_the_options() {
    let options = ConsumerOptions {
        // A Queue consumer is Shared on every segment, whatever it was given.
        sub_type: SubType::Exclusive,
        consumer_name: Some("c".into()),
        receiver_queue_size: 9,
        priority_level: Some(2),
        subscription_properties: vec![("region".into(), "us-east-1".into())],
        properties: vec![("app".into(), "billing".into())],
        ..options()
    };
    let h = QueueHarness::new(world(3), TOPIC, options);
    assert_eq!(h.subscribed, Some(Ok(())));
    let subscribes = h.world.received(Type::SUBSCRIBE);
    let names: Vec<_> = subscribes.iter().map(|c| c.subscribe.consumer_name.clone().unwrap()).collect();
    assert_eq!(names, ["c-seg-0", "c-seg-1", "c-seg-2"]);
    for command in subscribes {
        let subscribe = &command.subscribe;
        assert_eq!((subscribe.subscription.as_str(), subscribe.subType), (SUB, SubType::Shared));
        assert_eq!(subscribe.priority_level, Some(2));
        assert_eq!(subscribe.subscription_properties, [kv("region", "us-east-1")]);
        assert_eq!(subscribe.metadata, [kv("app", "billing")]);
    }
    // Each segment's consumer has the whole receiver queue, as in Java.
    let permits: Vec<u32> = h.world.received(Type::FLOW).iter().map(|c| c.flow.messagePermits).collect();
    assert_eq!(permits, [9, 9, 9]);
}

#[test]
fn consumers_on_legacy_segments_are_marked_as_managed_by_a_v5_client() {
    let mut world = World::default();
    world.create_legacy_topic("persistent://acme/orders/old", 2);
    let h = QueueHarness::new(world, "persistent://acme/orders/old", options());
    assert_eq!(h.subscribed, Some(Ok(())));
    let subscribes = h.world.received(Type::SUBSCRIBE);
    let topics: Vec<&str> = subscribes.iter().map(|c| c.subscribe.topic.as_str()).collect();
    assert_eq!(topics, ["persistent://acme/orders/old-partition-0", "persistent://acme/orders/old-partition-1"]);
    for command in subscribes {
        assert_eq!(command.subscribe.metadata, [kv("__pulsar.v5.managed", "true")]);
        assert_eq!(command.subscribe.consumer_name, None);
    }
}

#[test]
fn follows_splits_and_merges_without_losing_a_message() {
    let mut world = world(1);
    publish(&mut world, 0, "g0");
    let mut h = QueueHarness::new(world, TOPIC, options());
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(segments(&h), [0, low, high]);
    publish(&mut h.world, low, "g1-low");
    publish(&mut h.world, high, "g1-high");
    let merged = h.world.merge(TOPIC, low, high);
    h.pump();
    publish(&mut h.world, merged, "g2");
    let taken = h.take();
    let values = taken.iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect();
    assert_eq!(sorted(values), ["g0", "g1-high", "g1-low", "g2"]);
    for message in &taken {
        h.ack(&message.id);
    }
    h.advance(Duration::from_millis(100));
    // The sealed segments are drained: only the merged one is left.
    assert_eq!(segments(&h), [merged]);
}

#[test]
fn subscribes_the_children_of_a_split_from_their_start_even_at_latest() {
    let mut world = world(1);
    publish(&mut world, 0, "before");
    let mut h = QueueHarness::new(world, TOPIC, latest());
    // A producer writes to a child before the consumer hears of the split. The broker created the children without
    // this consumer's subscription: subscribing at the initial position, as Java does, would miss it.
    let (low, high) = h.world.split(TOPIC, 0);
    publish(&mut h.world, high, "early");
    h.pump();
    publish(&mut h.world, low, "later");
    assert_eq!(sorted(h.take_values()), ["early", "later"]);
    let (latest, earliest) = (Some(InitialPosition::Latest), Some(InitialPosition::Earliest));
    assert_eq!(positions(&h), [latest, earliest, earliest]);
}

#[test]
fn a_new_earliest_subscription_replays_the_sealed_parent_and_the_children() {
    let mut world = world(1);
    publish(&mut world, 0, "parent");
    let (low, high) = world.split(TOPIC, 0);
    publish(&mut world, low, "low");
    publish(&mut world, high, "high");
    let mut h = QueueHarness::new(world, TOPIC, options());
    assert_eq!(sorted(h.take_values()), ["high", "low", "parent"]);
}

#[test]
fn lets_go_of_a_sealed_segment_once_drained_while_the_layout_still_has_it() {
    let mut world = world(1);
    publish(&mut world, 0, "a");
    publish(&mut world, 0, "b");
    let mut h = QueueHarness::new(world, TOPIC, options());
    let taken = h.take();
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    h.ack(&taken[0].id);
    h.advance(Duration::from_millis(100));
    // A message is still to acknowledge: not drained.
    assert_eq!(segments(&h), [0, low, high]);
    h.ack(&taken[1].id);
    h.advance(Duration::from_millis(100));
    assert_eq!(segments(&h), [low, high]);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
    // A late ack of its messages does nothing; a newer layout doesn't subscribe to it again, nor does one that drops
    // it.
    h.ack(&taken[1].id);
    h.world.split(TOPIC, low);
    h.pump();
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribes[&0], 1);
    assert_eq!(h.world.received(Type::ACK).len(), 2);
}

#[test]
fn closes_a_segment_that_left_the_layout_once_its_subscribe_completes() {
    let mut world = world(1);
    world.hold_subscribes_to.insert(name("8000-ffff", 2));
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    // Segment 2 splits and leaves the layout while its subscribe is pending.
    h.world.split(TOPIC, 2);
    h.world.prune(TOPIC, 2);
    h.pump();
    assert_eq!(segments(&h), [1, 3, 4]);
    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.subscribes[&2], 1);
    assert_eq!(h.world.consumer_count(TOPIC, 2, SUB), 0);
}

#[test]
fn subscribes_new_segments_while_another_subscribe_is_pending() {
    let mut world = world(1);
    world.hold_subscribes_to.insert(name("0000-7fff", 1));
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    h.world.split(TOPIC, 2);
    h.pump();
    assert_eq!((h.subscribes[&1], h.subscribes[&3], h.subscribes[&4]), (1, 1, 1));
    assert_eq!(h.world.consumer_count(TOPIC, 3, SUB), 1);
    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.pump();
    assert_eq!((h.subscribes[&1], h.world.consumer_count(TOPIC, 1, SUB)), (1, 1));
}

#[test]
fn retries_a_segment_that_keeps_failing_one_retry_at_a_time() {
    let mut world = world(1);
    world.fail_subscribes_to.insert(name("0000-7fff", 1), vec![ServerError::NotAllowedError; 10].into());
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribes[&1], 1);
    assert!((95..=105).contains(&next_deadline(&h).as_millis()), "{:?}", next_deadline(&h));
    // A newer layout subscribes to it again at once; failing again, it waits for the next retry.
    h.world.split(TOPIC, 2);
    h.pump();
    assert_eq!(h.subscribes[&1], 2);
    h.advance(next_deadline(&h));
    assert_eq!(h.subscribes[&1], 3);
    assert!(h.queue.poll_timeout().is_some(), "the retry is rescheduled");
    // The segments subscribed are not subscribed again, and the application never hears of it.
    assert_eq!((h.subscribes[&0], h.subscribes[&2], h.subscribes[&3], h.subscribes[&4]), (1, 1, 1, 1));
    assert_eq!(h.subscribed, Some(Ok(())));
    assert!(!h.closed);
}

#[test]
fn retries_a_new_failure_without_waiting_out_an_earlier_backoff() {
    let mut world = world(1);
    world.fail_subscribes_to.insert(name("0000-7fff", 1), vec![ServerError::NotAllowedError; 3].into());
    world.fail_subscribes_to.insert(name("8000-bfff", 3), vec![ServerError::NotAllowedError; 10].into());
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    // Each failed retry doubles the backoff.
    h.advance(next_deadline(&h));
    h.advance(next_deadline(&h));
    assert_eq!(h.subscribes[&1], 3);
    assert!(next_deadline(&h).as_millis() >= 380, "{:?}", next_deadline(&h));
    // A newer layout subscribes to segment 1 at once, which succeeds. Segment 2 then splits, and segment 3 fails:
    // it is retried after the first delay, not the longer one segment 1 had reached.
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribes[&1], 4);
    assert_eq!(h.queue.poll_timeout(), None);
    h.world.split(TOPIC, 2);
    h.pump();
    assert!(next_deadline(&h).as_millis() <= 105, "{:?}", next_deadline(&h));
    h.advance(next_deadline(&h));
    assert_eq!(h.subscribes[&3], 2);
}

#[test]
fn segments_that_give_up_together_share_one_retry() {
    let mut world = world(1);
    for (range, id) in [("0000-7fff", 1), ("8000-ffff", 2)] {
        world.fail_subscribes_to.insert(name(range, id), [ServerError::NotAllowedError].into());
    }
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    assert!(next_deadline(&h).as_millis() <= 105, "{:?}", next_deadline(&h));
    h.advance(next_deadline(&h));
    assert_eq!((h.subscribes[&1], h.subscribes[&2]), (2, 2));
    assert_eq!((h.world.consumer_count(TOPIC, 1, SUB), h.world.consumer_count(TOPIC, 2, SUB)), (1, 1));
}

#[test]
fn a_segment_whose_consumer_gives_up_is_subscribed_again_at_its_initial_position() {
    let mut h = QueueHarness::new(world(1), TOPIC, latest());
    // The broker unloads the segment, then refuses its consumer's next subscribe for good.
    h.world.fail_subscribes_to.insert(name("0000-ffff", 0), [ServerError::NotAllowedError].into());
    h.world.unload(TOPIC, 0);
    h.pump();
    h.advance(Duration::from_millis(110));
    assert_eq!(h.subscribes[&0], 2);
    h.advance(Duration::from_millis(110));
    assert_eq!(h.subscribes[&0], 3);
    assert_eq!(positions(&h), [Some(InitialPosition::Latest); 3]);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 1);
}

#[test]
fn the_subscribe_waits_for_the_segments_of_layouts_that_come_meanwhile() {
    let mut world = world(1);
    world.hold_subscribes = true;
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribed, None);
    h.world.hold_subscribes = false;
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!((h.subscribes[&0], h.subscribes[&1], h.subscribes[&2]), (1, 1, 1));
}

#[test]
fn the_subscribe_fails_if_a_segment_of_a_layout_that_came_meanwhile_cannot_be_subscribed() {
    let mut world = world(1);
    world.hold_subscribes_to.insert(name("0000-ffff", 0));
    world.fail_subscribes_to.insert(name("8000-ffff", 2), [ServerError::NotAllowedError].into());
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    assert!(
        matches!(&h.subscribed, Some(Err(RegisterError::Server { error: ServerError::NotAllowedError, .. }))),
        "{:?}",
        h.subscribed
    );
    assert!(h.closed);
    assert_eq!(h.world.consumer_count(TOPIC, 1, SUB), 0);
    // Segment 0's subscribe, still on its way, is closed once it completes.
    h.world.hold_subscribes_to.clear();
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
}

#[test]
fn the_subscribe_does_not_wait_for_a_segment_that_leaves_the_layout() {
    let mut world = world(1);
    world.hold_subscribes_to.insert(name("0000-ffff", 0));
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribed, None);
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(segments(&h), [1, 2]);
}

#[test]
fn a_segment_that_fails_with_an_error_worth_retrying_is_retried_by_its_consumer() {
    let mut world = world(2);
    world.fail_subscribes_to.insert(name("0000-7fff", 0), [ServerError::ServiceNotReady].into());
    let mut h = QueueHarness::new(world, TOPIC, options());
    assert_eq!(h.subscribed, None);
    h.advance(Duration::from_millis(105));
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.subscribes[&0], 2);
}

#[test]
fn the_subscribe_fails_and_closes_every_segment_if_one_cannot_be_subscribed() {
    let mut world = world(2);
    world.fail_subscribes_to.insert(name("8000-ffff", 1), [ServerError::AuthorizationError].into());
    let mut h = QueueHarness::new(world, TOPIC, options());
    assert!(
        matches!(&h.subscribed, Some(Err(RegisterError::Server { error: ServerError::AuthorizationError, .. }))),
        "{:?}",
        h.subscribed
    );
    assert!(h.closed && h.queue.is_closed());
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
    h.advance(Duration::from_secs(1));
    assert_eq!((h.subscribes[&0], h.subscribes[&1]), (1, 1));
}

#[test]
fn a_segment_deleted_before_the_layout_drops_it_is_retried_until_the_layout_comes() {
    let mut world = world(1);
    publish(&mut world, 0, "a");
    let mut h = QueueHarness::new(world, TOPIC, options());
    // Taken, not acknowledged: segment 0 isn't drained.
    let taken = h.take();
    h.world.split(TOPIC, 0);
    h.pump();
    // The broker deletes the segment's topic before the watch hears of the layout without it.
    h.world.delete_segment(TOPIC, 0);
    h.pump();
    h.advance(Duration::from_millis(105));
    assert_eq!(h.subscribes[&0], 2);
    assert!(h.queue.poll_timeout().is_some(), "a retry");
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(h.queue.poll_timeout(), None);
    h.ack(&taken[0].id);
    h.advance(Duration::from_secs(1));
    assert_eq!(h.subscribes[&0], 2);
    assert_eq!((h.subscribed.clone(), h.closed), (Some(Ok(())), false));
    assert!(h.world.received(Type::ACK).is_empty());
}

#[test]
fn a_segment_the_layout_drops_is_let_go_before_its_topic_is_deleted() {
    let mut world = world(1);
    publish(&mut world, 0, "a");
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.take();
    let (low, high) = h.world.split(TOPIC, 0);
    h.pump();
    h.world.prune(TOPIC, 0);
    h.pump();
    assert_eq!(segments(&h), [low, high]);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
    h.world.delete_segment(TOPIC, 0);
    h.pump();
    h.advance(Duration::from_secs(1));
    assert_eq!(h.subscribes[&0], 1);
}

#[test]
fn close_sends_the_acks_then_closes_every_segment() {
    let mut world = world(2);
    publish(&mut world, 0, "a");
    publish(&mut world, 1, "b");
    let mut h = QueueHarness::new(world, TOPIC, options());
    let taken = h.take();
    for message in &taken {
        h.ack(&message.id);
    }
    // In, and not taken: the broker delivers it again.
    publish(&mut h.world, 0, "c");
    h.pump();
    h.close();
    assert!(h.closed);
    assert_eq!((h.world.backlog(TOPIC, 0, SUB), h.world.backlog(TOPIC, 1, SUB)), (1, 0));
    assert_eq!((h.world.consumer_count(TOPIC, 0, SUB), h.world.consumer_count(TOPIC, 1, SUB)), (0, 0));
    assert!(h.queue.next_message().is_none());
    // Closing again does nothing, nor do acks or layouts.
    h.close();
    h.ack(&taken[0].id);
    h.world.split(TOPIC, 0);
    h.pump();
    assert_eq!(h.subscribes.len(), 2);
}

#[test]
fn close_while_subscribing_closes_each_subscription_once_it_completes() {
    let mut world = world(2);
    world.hold_subscribes = true;
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.close();
    assert_eq!(h.subscribed, None);
    assert!(h.closed);
    h.world.hold_subscribes = false;
    h.world.release_subscribes();
    h.pump();
    assert_eq!((h.world.consumer_count(TOPIC, 0, SUB), h.world.consumer_count(TOPIC, 1, SUB)), (0, 0));
}

#[test]
fn keeps_consuming_and_following_the_layout_after_a_connection_loss() {
    let mut world = world(1);
    publish(&mut world, 0, "a");
    let mut h = QueueHarness::new(world, TOPIC, options());
    // Not acknowledged: the broker delivers it again.
    assert_eq!(h.take_values(), ["a"]);
    h.drop_connection();
    h.advance(Duration::from_millis(105));
    assert_eq!(h.take_values(), ["a"]);
    let (low, _) = h.world.split(TOPIC, 0);
    h.pump();
    publish(&mut h.world, low, "b");
    assert_eq!(h.take_values(), ["b"]);
}

#[test]
fn acks_of_segments_it_does_not_have_are_ignored() {
    let mut world = world(1);
    publish(&mut world, 0, "a");
    let mut h = QueueHarness::new(world, TOPIC, options());
    let mut id = h.take()[0].id.clone();
    id.segment_id = 7;
    h.ack(&id);
    h.nack(&id);
    h.advance(Duration::from_secs(2));
    assert!(h.world.received(Type::ACK).is_empty());
    assert!(h.world.received(Type::REDELIVER_UNACKNOWLEDGED_MESSAGES).is_empty());
}

#[test]
fn a_subscribe_lost_with_its_connection_is_sent_again() {
    let mut world = world(1);
    world.hold_subscribes = true;
    let mut h = QueueHarness::new(world, TOPIC, options());
    h.drop_connection();
    h.world.hold_subscribes = false;
    h.advance(Duration::from_millis(105));
    assert_eq!(h.subscribed, Some(Ok(())));
    assert_eq!(h.subscribes[&0], 2);
}
