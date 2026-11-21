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

//! A segment consumer against the simulated cluster: what it receives, acknowledges and gets again, across
//! reconnects, unloads, failures and the end of a segment.

use std::time::Duration;

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{
    CompressionType, EncryptionKeys, IntRange, MessageMetadata, ServerError, SingleMessageMetadata,
};
use apache_pulsar_testkit::consumer::ConsumerHarness;
use apache_pulsar_testkit::producer::{Harness, options as producer_options};
use apache_pulsar_testkit::scalable::World;
use buffa::Message;
use bytes::{BufMut, Bytes, BytesMut};

const TOPIC: &str = "topic://acme/orders/events";
/// The world's name for the one segment of [`TOPIC`].
const SEGMENT: &str = "segment://acme/orders/events/0000-ffff-0";
const SUB: &str = "workers";

fn world() -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    world
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

/// Stores a one-message entry, as any producer would.
fn publish(world: &mut World, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    world.publish_raw(TOPIC, 0, metadata, Bytes::copy_from_slice(value.as_bytes()));
}

/// A batch's payload: each message's metadata, length-prefixed, then its value.
fn batch(values: &[&str]) -> Bytes {
    let mut out = BytesMut::new();
    for value in values {
        let single = SingleMessageMetadata { payload_size: value.len() as i32, ..Default::default() };
        out.put_u32(single.encoded_len());
        out.put_slice(&single.encode_to_vec());
        out.put_slice(value.as_bytes());
    }
    out.freeze()
}

#[test]
fn receives_acknowledges_and_resumes_after_what_it_acked() {
    let mut world = world();
    for value in ["a", "b", "c"] {
        publish(&mut world, value);
    }
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    assert_eq!(h.subscribed, Some(Ok(())));
    let messages = h.take();
    let values: Vec<&[u8]> = messages.iter().map(|m| &m.payload[..]).collect();
    assert_eq!(values, [b"a", b"b", b"c"]);
    assert_eq!((messages[0].id.segment_id, messages[0].entry.producer_name.as_str()), (0, "other"));
    for message in &messages[..2] {
        h.ack(&message.id);
    }
    h.advance(Duration::from_millis(100));
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 1);
    h.close();
    assert!(h.closed);
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
    // A consumer of the subscription gets what wasn't acknowledged, then what comes next.
    let mut h = ConsumerHarness::new(std::mem::take(&mut h.world), SEGMENT, 0, options());
    publish(&mut h.world, "d");
    assert_eq!(h.take_values(), ["c", "d"]);
}

#[test]
fn a_new_subscription_at_the_latest_skips_what_came_before() {
    let mut world = world();
    publish(&mut world, "before");
    let mut h = ConsumerHarness::new(
        world,
        SEGMENT,
        0,
        ConsumerOptions { initial_position: InitialPosition::Latest, ..options() },
    );
    assert!(h.take().is_empty());
    publish(&mut h.world, "after");
    assert_eq!(h.take_values(), ["after"]);
}

/// The broker pushes no more than the permits granted: the receiver queue first, then half of it at a time as
/// messages are taken.
#[test]
fn permits_bound_what_the_broker_pushes() {
    let mut world = world();
    for i in 0..10 {
        publish(&mut world, &format!("m{i}"));
    }
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, ConsumerOptions { receiver_queue_size: 4, ..options() });
    assert_eq!(h.consumer.available(), 4);
    let taken: Vec<usize> = (0..4).map(|_| h.take().len()).collect();
    assert_eq!(taken, [4, 4, 2, 0]);
}

/// Another client's entries: a compressed batch, acknowledged index by index across a reconnect.
#[test]
fn a_compressed_batch_from_another_client() {
    let mut world = world();
    let raw = batch(&["x", "y", "z"]);
    let metadata = MessageMetadata {
        producer_name: "java".into(),
        num_messages_in_batch: Some(3),
        compression: Some(CompressionType::ZSTD),
        uncompressed_size: Some(raw.len() as u32),
        ..Default::default()
    };
    world.publish_raw(TOPIC, 0, metadata, zstd::bulk::compress(&raw, 3).unwrap().into());
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    let messages = h.take();
    let ids: Vec<(i32, i32)> = messages.iter().map(|m| (m.id.entry.batch_index, m.id.entry.batch_size)).collect();
    assert_eq!(ids, [(0, 3), (1, 3), (2, 3)]);
    h.ack(&messages[1].id);
    h.advance(Duration::from_millis(100));
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 2);
    // The batch comes back without what was acknowledged.
    h.drop_connection();
    h.advance(Duration::from_millis(200));
    let again = h.take();
    assert_eq!(again.iter().map(|m| &m.payload[..]).collect::<Vec<_>>(), [b"x", b"z"]);
    for message in &again {
        h.ack(&message.id);
    }
    h.advance(Duration::from_millis(100));
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
}

#[test]
fn a_negative_ack_brings_the_message_back_later() {
    let mut world = world();
    publish(&mut world, "a");
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    let first = h.take();
    assert_eq!(first[0].redelivery_count, 0);
    h.nack(&first[0].id);
    h.advance(Duration::from_millis(999));
    assert!(h.take().is_empty());
    h.advance(Duration::from_millis(1));
    let again = h.take();
    assert_eq!((&again[0].payload[..], again[0].redelivery_count), (&b"a"[..], 1));
}

/// A corrupt entry is dropped, with the reason; an encrypted one is left for a consumer that can read it.
#[test]
fn a_corrupt_entry_is_dropped_and_an_unreadable_one_left() {
    let mut world = world();
    let corrupt =
        MessageMetadata { compression: Some(CompressionType::LZ4), uncompressed_size: Some(10), ..Default::default() };
    world.publish_raw(TOPIC, 0, corrupt, Bytes::from_static(b"not lz4"));
    let encrypted = MessageMetadata { encryption_keys: vec![EncryptionKeys::default()], ..Default::default() };
    world.publish_raw(TOPIC, 0, encrypted, Bytes::from_static(b"secret"));
    publish(&mut world, "plain");
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    assert_eq!(h.take_values(), ["plain"]);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 2);
}

/// Once the subscription acknowledged everything of a terminated segment, the broker says so, and the consumer is
/// drained.
#[test]
fn a_terminated_segment_drains() {
    let mut world = world();
    publish(&mut world, "a");
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    let message = h.take().remove(0);
    h.world.terminate(TOPIC, 0);
    h.pump();
    assert!(!h.consumer.is_drained());
    h.ack(&message.id);
    assert!(!h.consumer.is_drained());
    h.advance(Duration::from_millis(100));
    assert!(h.consumer.is_drained());
}

/// After a connection loss, what wasn't acknowledged comes back, and the acks made meanwhile go out first.
#[test]
fn a_reconnect_brings_back_what_was_not_acknowledged() {
    let mut world = world();
    for value in ["a", "b", "c"] {
        publish(&mut world, value);
    }
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    let messages = h.take();
    h.drop_connection();
    h.ack(&messages[0].id);
    h.advance(Duration::from_millis(200));
    // Not a redelivery that counts, as for the broker.
    let again = h.take();
    assert_eq!(
        again.iter().map(|m| (&m.payload[..], m.redelivery_count)).collect::<Vec<_>>(),
        [(&b"b"[..], 0), (&b"c"[..], 0)]
    );
}

#[test]
fn subscribe_failures_and_waits() {
    // Worth retrying.
    let mut w = world();
    w.fail_subscribes.push_back(ServerError::ServiceNotReady);
    let mut h = ConsumerHarness::new(w, SEGMENT, 0, options());
    assert_eq!(h.subscribed, None);
    h.advance(Duration::from_millis(200));
    assert_eq!(h.subscribed, Some(Ok(())));

    // Not worth retrying.
    let mut w = world();
    w.fail_subscribes.push_back(ServerError::AuthorizationError);
    let h = ConsumerHarness::new(w, SEGMENT, 0, options());
    assert!(matches!(h.subscribed, Some(Err(RegisterError::Server { error: ServerError::AuthorizationError, .. }))));
    assert!(h.closed);

    // A subscribe held while the connection drops is made again.
    let mut w = world();
    w.hold_subscribes = true;
    let mut h = ConsumerHarness::new(w, SEGMENT, 0, options());
    h.world.hold_subscribes = false;
    h.drop_connection();
    h.advance(Duration::from_millis(200));
    assert_eq!(h.subscribed, Some(Ok(())));

    // A consumer closed while its subscribe is held closes the subscription once it completes.
    let mut w = world();
    w.hold_subscribes = true;
    let mut h = ConsumerHarness::new(w, SEGMENT, 0, options());
    h.close();
    assert!(h.closed);
    h.world.release_subscribes();
    h.pump();
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
}

/// An unload closes the producers and consumers of the segment; the consumer subscribes again and gets what it
/// didn't acknowledge.
#[test]
fn an_unload_closes_the_consumer_and_it_comes_back() {
    let mut producer = Harness::new(world(), TOPIC, producer_options());
    producer.send(None, "a");
    let mut h = ConsumerHarness::new(std::mem::take(&mut producer.world), SEGMENT, 0, options());
    assert_eq!(h.take_values(), ["a"]);
    h.world.unload(TOPIC, 0);
    h.pump();
    assert_eq!(h.world.consumer_count(TOPIC, 0, SUB), 0);
    h.advance(Duration::from_millis(200));
    assert_eq!(h.take_values(), ["a"]);
}

/// A close sends the acks made so far; what wasn't acknowledged stays for the subscription's other consumers.
#[test]
fn a_close_sends_its_acks_first() {
    let mut world = world();
    publish(&mut world, "a");
    publish(&mut world, "b");
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options());
    let messages = h.take();
    h.ack(&messages[0].id);
    h.close();
    assert!(h.closed);
    assert_eq!((h.world.consumer_count(TOPIC, 0, SUB), h.world.backlog(TOPIC, 0, SUB)), (0, 1));
}

/// On an Exclusive subscription a cumulative ack covers everything before it. After the broker rewinds the
/// subscription, the application's ack of a position it acknowledged before goes out again.
#[test]
fn an_exclusive_consumer_acknowledges_cumulatively() {
    let mut world = world();
    for value in ["a", "b", "c"] {
        publish(&mut world, value);
    }
    let options = ConsumerOptions { sub_type: SubType::Exclusive, ack_group_time: Duration::ZERO, ..options() };
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options);
    let taken = h.take();
    h.ack_cumulative(&taken[1].id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 1);
    h.ack_cumulative(&taken[2].id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
    // Rewound: the broker closes the consumer, which subscribes again and gets everything again.
    h.world.rewind(TOPIC, 0, SUB);
    h.pump();
    assert_eq!((h.world.backlog(TOPIC, 0, SUB), h.world.consumer_count(TOPIC, 0, SUB)), (3, 0));
    h.advance(Duration::from_millis(200));
    assert_eq!(h.take_values(), ["a", "b", "c"]);
    h.ack_cumulative(&taken[2].id);
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 0);
}

/// On a Key_Shared subscription by entry bucket, a cumulative ack acknowledges one by one what the consumer was given.
#[test]
fn a_key_shared_consumer_acknowledges_what_it_was_given() {
    let mut world = world();
    for value in ["a", "b", "c"] {
        publish(&mut world, value);
    }
    let options = ConsumerOptions {
        sub_type: SubType::Key_Shared,
        bucket_ranges: vec![IntRange { start: 0, end: 0xffff, ..Default::default() }],
        consumer_name: Some("solo".into()),
        ..options()
    };
    let mut h = ConsumerHarness::new(world, SEGMENT, 0, options);
    let taken = h.take();
    assert_eq!(taken.len(), 3);
    h.ack_cumulative(&taken[1].id);
    h.advance(Duration::from_millis(100));
    assert_eq!(h.world.backlog(TOPIC, 0, SUB), 1);
    h.consumer.pause();
    assert!(!h.consumer.is_settled());
    h.ack_cumulative(&taken[2].id);
    assert!(h.consumer.is_settled());
}
