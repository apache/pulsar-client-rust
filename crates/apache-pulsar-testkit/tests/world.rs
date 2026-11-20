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

//! The simulated cluster's own behavior, on raw frames: what the tests of the producer and the consumer rely on.

use apache_pulsar_protocol::frame::{FrameDecoder, encode_command, encode_payload_command};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_ack::{AckType, ValidationError};
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandAck, CommandCloseConsumer, CommandConnect, CommandFlow, CommandPing, CommandProducer,
    CommandRedeliverUnacknowledgedMessages, CommandSend, CommandSubscribe, CommandUnsubscribe, CompressionType,
    MessageIdData, MessageMetadata, ServerError, SingleMessageMetadata,
};
use apache_pulsar_testkit::decode_all;
use apache_pulsar_testkit::scalable::World;
use buffa::{EnumValue, Message};
use bytes::{BufMut, Bytes, BytesMut};

const TOPIC: &str = "topic://acme/orders/events";
/// The only segment of [`TOPIC`], created with one.
const SEGMENT: &str = "segment://acme/orders/events/0000-ffff-0";
/// The ledger of [`SEGMENT`].
const LEDGER: u64 = 101;
const SUB: &str = "sub";

fn write(world: &mut World, conn: usize, command: &BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(command, &mut buf).unwrap();
    world.input(conn, &buf);
}

fn connected(world: &mut World) -> usize {
    let conn = world.connect();
    let connect = BaseCommand {
        r#type: Type::CONNECT.into(),
        connect: CommandConnect { client_version: "raw".into(), protocol_version: Some(21), ..Default::default() }
            .into(),
        ..Default::default()
    };
    write(world, conn, &connect);
    assert_eq!(decode_all(&world.output(conn))[0].r#type, EnumValue::Known(Type::CONNECTED));
    conn
}

fn register(world: &mut World, conn: usize, topic: &str, producer_id: u64) -> BaseCommand {
    let producer = BaseCommand {
        r#type: Type::PRODUCER.into(),
        producer: CommandProducer { topic: topic.into(), producer_id, request_id: producer_id, ..Default::default() }
            .into(),
        ..Default::default()
    };
    write(world, conn, &producer);
    decode_all(&world.output(conn)).remove(0)
}

/// A Shared subscription to [`SEGMENT`], at the default position.
fn shared(subscription: &str, consumer_id: u64) -> CommandSubscribe {
    CommandSubscribe {
        topic: SEGMENT.into(),
        subscription: subscription.into(),
        subType: SubType::Shared,
        consumer_id,
        request_id: 100 + consumer_id,
        ..Default::default()
    }
}

/// Sends a subscribe, and returns what the broker sent since.
fn subscribe(world: &mut World, conn: usize, subscribe: CommandSubscribe) -> Vec<BaseCommand> {
    let command = BaseCommand { r#type: Type::SUBSCRIBE.into(), subscribe: subscribe.into(), ..Default::default() };
    write(world, conn, &command);
    decode_all(&world.output(conn))
}

fn flow(world: &mut World, conn: usize, consumer_id: u64, permits: u32) {
    let flow = CommandFlow { consumer_id, messagePermits: permits, ..Default::default() };
    write(world, conn, &BaseCommand { r#type: Type::FLOW.into(), flow: flow.into(), ..Default::default() });
}

/// An individual ack of `ids`.
fn acks(consumer_id: u64, ids: Vec<MessageIdData>) -> CommandAck {
    CommandAck { consumer_id, message_id: ids, ..Default::default() }
}

fn ack(world: &mut World, conn: usize, ack: CommandAck) {
    write(world, conn, &BaseCommand { r#type: Type::ACK.into(), ack: ack.into(), ..Default::default() });
}

fn redeliver(world: &mut World, conn: usize, consumer_id: u64, ids: Vec<MessageIdData>) {
    let redeliver = CommandRedeliverUnacknowledgedMessages { consumer_id, message_ids: ids, ..Default::default() };
    let command = BaseCommand {
        r#type: Type::REDELIVER_UNACKNOWLEDGED_MESSAGES.into(),
        redeliverUnacknowledgedMessages: redeliver.into(),
        ..Default::default()
    };
    write(world, conn, &command);
}

/// Closes a consumer, and returns what the broker sent since.
fn close(world: &mut World, conn: usize, consumer_id: u64) -> Vec<BaseCommand> {
    let close = CommandCloseConsumer { consumer_id, request_id: 200 + consumer_id, ..Default::default() };
    let command =
        BaseCommand { r#type: Type::CLOSE_CONSUMER.into(), close_consumer: close.into(), ..Default::default() };
    write(world, conn, &command);
    decode_all(&world.output(conn))
}

/// The id of an entry of [`SEGMENT`].
fn id(entry: u64) -> MessageIdData {
    MessageIdData { ledgerId: LEDGER, entryId: entry, ..Default::default() }
}

/// The id of a batch entry, with the bits of the indexes left unacked set.
fn indexes(entry: u64, ack_set: &[i64]) -> MessageIdData {
    MessageIdData { ack_set: ack_set.to_vec(), ..id(entry) }
}

/// An uncompressed batch of `values`, as a producer writes it.
fn batch(values: &[&str]) -> (MessageMetadata, Bytes) {
    let mut payload = BytesMut::new();
    for value in values {
        let single = SingleMessageMetadata { payload_size: value.len() as i32, ..Default::default() }.encode_to_vec();
        payload.put_u32(single.len() as u32);
        payload.put_slice(&single);
        payload.put_slice(value.as_bytes());
    }
    let metadata = MessageMetadata {
        producer_name: "other".into(),
        num_messages_in_batch: Some(values.len() as i32),
        ..Default::default()
    };
    (metadata, payload.freeze())
}

/// Stores a batch of `values` in [`SEGMENT`], and returns its entry id.
fn publish(world: &mut World, values: &[&str]) -> u64 {
    let (metadata, payload) = batch(values);
    world.publish_raw(TOPIC, 0, metadata, payload).1
}

/// A message's `(consumer id, entry id, redelivery count)`.
fn message(command: &BaseCommand) -> (u64, u64, u32) {
    assert_eq!(command.r#type, Type::MESSAGE, "not a message: {command:?}");
    let message = &command.message;
    (message.consumer_id, message.message_id.entryId, message.redelivery_count.unwrap_or(0))
}

/// The messages the broker sent on `conn`, as [`message`] shows them. Anything else fails.
fn delivered(world: &mut World, conn: usize) -> Vec<(u64, u64, u32)> {
    decode_all(&world.output(conn)).iter().map(message).collect()
}

fn types(commands: &[BaseCommand]) -> Vec<Type> {
    commands.iter().map(|c| c.r#type.as_known().unwrap()).collect()
}

#[test]
fn answers_pings_and_ignores_what_it_doesnt_simulate() {
    let mut world = World::default();
    let conn = connected(&mut world);
    write(
        &mut world,
        conn,
        &BaseCommand { r#type: Type::PING.into(), ping: CommandPing::default().into(), ..Default::default() },
    );
    assert_eq!(decode_all(&world.output(conn))[0].r#type, EnumValue::Known(Type::PONG));
    let unsubscribe = BaseCommand {
        r#type: Type::UNSUBSCRIBE.into(),
        unsubscribe: CommandUnsubscribe::default().into(),
        ..Default::default()
    };
    write(&mut world, conn, &unsubscribe);
    assert!(world.output(conn).is_empty());
    assert_eq!(world.received(Type::UNSUBSCRIBE).len(), 1);
}

#[test]
fn a_producer_on_an_unknown_segment_is_not_found() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let answer = register(&mut world, conn, "segment://acme/orders/events/0000-ffff-9", 1);
    assert_eq!(answer.error.error, ServerError::TopicNotFound);
}

#[test]
fn a_send_for_an_unknown_producer_closes_the_connection() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 42, sequence_id: 0, ..Default::default() }.into(),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_payload_command(&send, &MessageMetadata::default(), b"x", &mut buf).unwrap();
    world.input(conn, &buf);
    assert!(!world.is_open(conn));
    assert!(world.open_connections().is_empty());
}

/// An entry that isn't a batch, as other clients send a lone message: its key and value are the entry's own.
#[test]
fn stores_plain_entries_too() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let success = register(&mut world, conn, "segment://acme/orders/events/0000-ffff-0", 1);
    assert_eq!(success.producer_success.producer_name, "sim-1");
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 1, sequence_id: 0, ..Default::default() }.into(),
        ..Default::default()
    };
    let metadata =
        MessageMetadata { producer_name: "sim-1".into(), partition_key: Some("k".into()), ..Default::default() };
    let mut buf = BytesMut::new();
    encode_payload_command(&send, &metadata, b"plain", &mut buf).unwrap();
    world.input(conn, &buf);
    let stored = world.stored_in(TOPIC, 0);
    assert_eq!((stored[0].key.as_deref(), &stored[0].value[..]), (Some("k"), &b"plain"[..]));
    assert!(stored[0].single.is_none());
    assert_eq!(&stored[0].payload[..], b"plain");
}

/// A new subscription starts after the last entry, by default, or at the first. One that exists stays where it is.
#[test]
fn a_subscription_starts_at_the_latest_or_the_earliest_entry() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    publish(&mut world, &["a"]);
    publish(&mut world, &["b"]);
    assert_eq!(types(&subscribe(&mut world, conn, shared("latest", 1))), [Type::SUCCESS]);
    let earliest = CommandSubscribe { initialPosition: Some(InitialPosition::Earliest), ..shared("earliest", 2) };
    assert_eq!(types(&subscribe(&mut world, conn, earliest)), [Type::SUCCESS]);
    assert_eq!((world.backlog(TOPIC, 0, "latest"), world.backlog(TOPIC, 0, "earliest")), (0, 2));
    publish(&mut world, &["c"]);
    assert_eq!((world.backlog(TOPIC, 0, "latest"), world.backlog(TOPIC, 0, "earliest")), (1, 3));
    flow(&mut world, conn, 1, 10);
    assert_eq!(delivered(&mut world, conn), [(1, 2, 0)]);
    flow(&mut world, conn, 2, 10);
    assert_eq!(delivered(&mut world, conn), [(2, 0, 0), (2, 1, 0), (2, 2, 0)]);
    // Joining from the earliest doesn't move a subscription back.
    let again = CommandSubscribe { initialPosition: Some(InitialPosition::Earliest), ..shared("latest", 3) };
    assert_eq!(types(&subscribe(&mut world, conn, again)), [Type::SUCCESS]);
    flow(&mut world, conn, 3, 10);
    assert!(world.output(conn).is_empty());
    assert_eq!((world.backlog(TOPIC, 0, "latest"), world.consumer_count(TOPIC, 0, "latest")), (1, 2));
}

/// Each entry goes to the next consumer with permits, round robin. One without permits is skipped until a flow
/// gives it some.
#[test]
fn dispatches_round_robin_to_the_consumers_with_permits() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    subscribe(&mut world, conn, shared(SUB, 2));
    for value in ["a", "b", "c", "d", "e"] {
        publish(&mut world, &[value]);
    }
    // Nobody has permits yet.
    assert!(world.output(conn).is_empty());
    flow(&mut world, conn, 1, 2);
    assert_eq!(delivered(&mut world, conn), [(1, 0, 0), (1, 1, 0)]);
    flow(&mut world, conn, 2, 5);
    assert_eq!(delivered(&mut world, conn), [(2, 2, 0), (2, 3, 0), (2, 4, 0)]);
    flow(&mut world, conn, 1, 5);
    for value in ["f", "g", "h", "i"] {
        publish(&mut world, &[value]);
    }
    assert_eq!(delivered(&mut world, conn), [(1, 5, 0), (2, 6, 0), (1, 7, 0), (2, 8, 0)]);
}

/// The consumers of the highest priority level, the lowest number, get the entries while they have permits.
#[test]
fn the_highest_priority_level_is_served_first() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, CommandSubscribe { priority_level: Some(1), ..shared(SUB, 1) });
    subscribe(&mut world, conn, shared(SUB, 2));
    subscribe(&mut world, conn, CommandSubscribe { priority_level: Some(0), ..shared(SUB, 3) });
    for consumer in 1..=3 {
        flow(&mut world, conn, consumer, 2);
    }
    for value in ["a", "b", "c", "d", "e"] {
        publish(&mut world, &[value]);
    }
    let to: Vec<u64> = delivered(&mut world, conn).iter().map(|&(consumer, ..)| consumer).collect();
    assert_eq!(to, [2, 3, 2, 3, 1]);
}

/// A batch costs its messages in permits, and a consumer with any permit gets a whole one: the broker gives each
/// consumer it picks at least one entry.
#[test]
fn a_batch_costs_its_messages_in_permits() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 2);
    publish(&mut world, &["a", "b", "c"]);
    publish(&mut world, &["d"]);
    // The batch takes the consumer to -1.
    assert_eq!(delivered(&mut world, conn), [(1, 0, 0)]);
    flow(&mut world, conn, 1, 1);
    assert!(world.output(conn).is_empty());
    flow(&mut world, conn, 1, 1);
    assert_eq!(delivered(&mut world, conn), [(1, 1, 0)]);
    assert_eq!(world.backlog(TOPIC, 0, SUB), 4);
}

/// Acks are per message: a whole entry, or the batch indexes an `ack_set` clears. A batch with indexes acked goes
/// out again with the bitset of the ones left, in the layout of Java's `BitSet.toLongArray`, and costs only those.
#[test]
fn acks_whole_entries_and_batch_indexes() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 68);
    publish(&mut world, &["a"]);
    publish(&mut world, &["x"; 66]);
    let first = decode_all(&world.output(conn));
    assert_eq!(first.iter().map(message).collect::<Vec<_>>(), [(1, 0, 0), (1, 1, 0)]);
    assert!(first[1].message.ack_set.is_empty());
    assert_eq!(world.backlog(TOPIC, 0, SUB), 67);
    ack(&mut world, conn, acks(1, vec![id(0)]));
    // Acking it again changes nothing, nor does acking indexes of it.
    ack(&mut world, conn, acks(1, vec![id(0), indexes(0, &[0])]));
    assert_eq!(world.backlog(TOPIC, 0, SUB), 66);
    // Index 65 acked: bits 0 to 64 are left.
    ack(&mut world, conn, acks(1, vec![indexes(1, &[-1, 1])]));
    assert_eq!(world.backlog(TOPIC, 0, SUB), 65);
    // The consumer has 1 permit left, and the batch costs it 65.
    redeliver(&mut world, conn, 1, vec![]);
    let again = decode_all(&world.output(conn));
    assert_eq!((message(&again[0]), &again[0].message.ack_set[..]), ((1, 1, 1), &[-1, 1][..]));
    flow(&mut world, conn, 1, 65);
    // The words an ack_set leaves out are zeros: index 64 is acked, and the last word goes.
    ack(&mut world, conn, acks(1, vec![indexes(1, &[-1])]));
    redeliver(&mut world, conn, 1, vec![]);
    let again = decode_all(&world.output(conn));
    assert_eq!((message(&again[0]), &again[0].message.ack_set[..]), ((1, 1, 2), &[-1][..]));
    assert_eq!(world.backlog(TOPIC, 0, SUB), 64);
    ack(&mut world, conn, acks(1, vec![id(1)]));
    assert_eq!(world.backlog(TOPIC, 0, SUB), 0);
    // Nothing is left to redeliver.
    redeliver(&mut world, conn, 1, vec![]);
    assert!(world.output(conn).is_empty());
}

/// A corrupt entry is acked whole, whatever its `ack_set` says: the client drops it. A cumulative ack is ignored, as
/// the broker does on a Shared subscription.
#[test]
fn a_corrupt_entry_is_acked_whole_and_a_cumulative_ack_ignored() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 10);
    publish(&mut world, &["a", "b"]);
    assert_eq!(delivered(&mut world, conn), [(1, 0, 0)]);
    ack(&mut world, conn, CommandAck { ack_type: AckType::Cumulative, ..acks(1, vec![id(0)]) });
    assert_eq!(world.backlog(TOPIC, 0, SUB), 2);
    let corrupt = acks(1, vec![indexes(0, &[0b10])]);
    ack(&mut world, conn, CommandAck { validation_error: Some(ValidationError::DecompressionError), ..corrupt });
    assert_eq!(world.backlog(TOPIC, 0, SUB), 0);
}

/// A redeliver request sends the entries it names again, if pending on the consumer, or all of them without ids,
/// a redelivery more each, to the next consumers with permits.
#[test]
fn redelivers_on_request_with_a_count_more() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 10);
    for value in ["a", "b", "c"] {
        publish(&mut world, &[value]);
    }
    let first = decode_all(&world.output(conn));
    assert_eq!(first.iter().map(message).collect::<Vec<_>>(), [(1, 0, 0), (1, 1, 0), (1, 2, 0)]);
    // No count until there is one, as the broker sends.
    assert!(first.iter().all(|m| m.message.redelivery_count.is_none()));
    // There is no entry 7.
    redeliver(&mut world, conn, 1, vec![id(1), id(7)]);
    assert_eq!(delivered(&mut world, conn), [(1, 1, 1)]);
    subscribe(&mut world, conn, shared(SUB, 2));
    flow(&mut world, conn, 2, 10);
    // Entry 0 is pending on consumer 1, not 2.
    redeliver(&mut world, conn, 2, vec![id(0)]);
    assert!(world.output(conn).is_empty());
    redeliver(&mut world, conn, 1, vec![]);
    assert_eq!(delivered(&mut world, conn), [(2, 0, 1), (1, 1, 2), (2, 2, 1)]);
    // Acks are the subscription's: consumer 1 acks what is now pending on 2.
    ack(&mut world, conn, acks(1, vec![id(0)]));
    redeliver(&mut world, conn, 2, vec![]);
    assert_eq!(delivered(&mut world, conn), [(1, 2, 2)]);
}

/// A consumer that closes, or whose connection drops, leaves what it had pending to the others. As in the broker,
/// that counts no redelivery, and a subscription forgets its counts once its last consumer goes; it keeps the
/// entries.
#[test]
fn a_consumer_that_goes_leaves_its_entries_to_the_others() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let (a, b) = (connected(&mut world), connected(&mut world));
    subscribe(&mut world, a, shared(SUB, 1));
    subscribe(&mut world, a, shared(SUB, 2));
    flow(&mut world, a, 1, 2);
    publish(&mut world, &["a"]);
    publish(&mut world, &["b"]);
    assert_eq!(delivered(&mut world, a), [(1, 0, 0), (1, 1, 0)]);
    flow(&mut world, a, 2, 2);
    let closed = close(&mut world, a, 1);
    assert_eq!(closed[0].success.request_id, 201);
    assert_eq!(closed[1..].iter().map(message).collect::<Vec<_>>(), [(2, 0, 0), (2, 1, 0)]);
    // Consumer 3 goes with the connection too: the entries skip it.
    subscribe(&mut world, a, shared(SUB, 3));
    flow(&mut world, a, 3, 5);
    subscribe(&mut world, b, shared(SUB, 4));
    flow(&mut world, b, 4, 5);
    world.disconnected(a);
    assert_eq!(delivered(&mut world, b), [(4, 0, 0), (4, 1, 0)]);
    assert_eq!(world.consumer_count(TOPIC, 0, SUB), 1);
    // Asked for again: one redelivery each.
    redeliver(&mut world, b, 4, vec![]);
    assert_eq!(delivered(&mut world, b), [(4, 0, 1), (4, 1, 1)]);
    assert_eq!(types(&close(&mut world, b, 4)), [Type::SUCCESS]);
    assert_eq!((world.consumer_count(TOPIC, 0, SUB), world.backlog(TOPIC, 0, SUB)), (0, 2));
    subscribe(&mut world, b, shared(SUB, 5));
    flow(&mut world, b, 5, 5);
    assert_eq!(delivered(&mut world, b), [(5, 0, 0), (5, 1, 0)]);
}

/// A subscription that acked everything of a terminated segment tells each of its consumers, once, that the topic
/// ended, the ones that join later too.
#[test]
fn the_end_of_topic_comes_once_everything_is_acked() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 10);
    publish(&mut world, &["a"]);
    assert_eq!(delivered(&mut world, conn), [(1, 0, 0)]);
    // One with nothing to ack hears it at once.
    subscribe(&mut world, conn, shared("idle", 2));
    world.terminate(TOPIC, 0);
    let ended = decode_all(&world.output(conn));
    assert_eq!((types(&ended), ended[0].reachedEndOfTopic.consumer_id), (vec![Type::REACHED_END_OF_TOPIC], 2));
    ack(&mut world, conn, acks(1, vec![id(0)]));
    let ended = decode_all(&world.output(conn));
    assert_eq!((types(&ended), ended[0].reachedEndOfTopic.consumer_id), (vec![Type::REACHED_END_OF_TOPIC], 1));
    flow(&mut world, conn, 1, 10);
    assert!(world.output(conn).is_empty());
    // One that joins hears it after its subscribe succeeds.
    let joined = subscribe(&mut world, conn, shared(SUB, 3));
    assert_eq!(types(&joined), [Type::SUCCESS, Type::REACHED_END_OF_TOPIC]);
    assert_eq!(joined[1].reachedEndOfTopic.consumer_id, 3);
    // A new subscription after the last entry has nothing to ack.
    assert_eq!(types(&subscribe(&mut world, conn, shared("late", 4))), [Type::SUCCESS, Type::REACHED_END_OF_TOPIC]);
    assert_eq!(world.backlog(TOPIC, 0, "late"), 0);
    // One from the earliest drains the segment first.
    let early = CommandSubscribe { initialPosition: Some(InitialPosition::Earliest), ..shared("early", 5) };
    assert_eq!(types(&subscribe(&mut world, conn, early)), [Type::SUCCESS]);
    flow(&mut world, conn, 5, 10);
    assert_eq!(delivered(&mut world, conn), [(5, 0, 0)]);
    ack(&mut world, conn, acks(5, vec![id(0)]));
    assert_eq!(types(&decode_all(&world.output(conn))), [Type::REACHED_END_OF_TOPIC]);
}

/// Subscribes fail on an unknown segment, for a type other than Shared, or as told, and wait for their answer while
/// held. Commands for a consumer the broker doesn't have are ignored, and closing one succeeds.
#[test]
fn subscribes_fail_or_wait_as_told() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    world.create_legacy_topic("persistent://acme/orders/legacy", 0);
    let conn = connected(&mut world);
    let unknown = CommandSubscribe { topic: "segment://acme/orders/events/0000-ffff-9".into(), ..shared(SUB, 1) };
    assert_eq!(subscribe(&mut world, conn, unknown)[0].error.error, ServerError::TopicNotFound);
    let exclusive = CommandSubscribe { subType: SubType::Exclusive, ..shared(SUB, 1) };
    assert_eq!(subscribe(&mut world, conn, exclusive)[0].error.error, ServerError::NotAllowedError);
    world.fail_subscribes.push_back(ServerError::ServiceNotReady);
    let refused = subscribe(&mut world, conn, shared(SUB, 1));
    assert_eq!((refused[0].error.request_id, refused[0].error.error), (101, ServerError::ServiceNotReady));
    assert_eq!(world.consumer_count(TOPIC, 0, SUB), 0);
    publish(&mut world, &["a"]);
    flow(&mut world, conn, 1, 10);
    ack(&mut world, conn, acks(1, vec![id(0)]));
    redeliver(&mut world, conn, 1, vec![]);
    assert!(world.output(conn).is_empty());
    assert_eq!(types(&close(&mut world, conn, 1)), [Type::SUCCESS]);
    // A legacy segment goes by the topic's own name.
    let legacy = CommandSubscribe { topic: "persistent://acme/orders/legacy".into(), ..shared(SUB, 1) };
    assert_eq!(types(&subscribe(&mut world, conn, legacy)), [Type::SUCCESS]);

    world.hold_subscribes = true;
    assert!(subscribe(&mut world, conn, shared(SUB, 2)).is_empty());
    assert!(subscribe(&mut world, conn, shared(SUB, 3)).is_empty());
    // Closed before the broker created it: discarded, its subscribe never answered.
    assert_eq!(types(&close(&mut world, conn, 3)), [Type::SUCCESS]);
    let gone = connected(&mut world);
    assert!(subscribe(&mut world, gone, shared(SUB, 4)).is_empty());
    world.disconnected(gone);
    assert_eq!(world.consumer_count(TOPIC, 0, SUB), 0);
    world.hold_subscribes = false;
    world.release_subscribes();
    let answers = decode_all(&world.output(conn));
    assert_eq!((types(&answers), answers[0].success.request_id), (vec![Type::SUCCESS], 102));
    assert_eq!(world.consumer_count(TOPIC, 0, SUB), 1);
}

/// An entry another client produced goes to consumers as it came. The world doesn't decompress: a compressed entry
/// shows no first message.
#[test]
fn publish_raw_dispatches_an_entry_as_it_came() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 10);
    let metadata = MessageMetadata {
        producer_name: "other".into(),
        sequence_id: 7,
        compression: Some(CompressionType::LZ4),
        uncompressed_size: Some(64),
        num_messages_in_batch: Some(2),
        ..Default::default()
    };
    let payload = Bytes::from_static(b"\x04\x22\x4d\x18 not really lz4");
    assert_eq!(world.publish_raw(TOPIC, 0, metadata.clone(), payload.clone()), (LEDGER, 0));
    let stored = &world.stored_in(TOPIC, 0)[0];
    assert!(stored.key.is_none() && stored.value.is_empty() && stored.single.is_none());
    assert_eq!((&stored.producer_name[..], stored.sequence_id, &stored.payload), ("other", 7, &payload));
    assert_eq!(world.backlog(TOPIC, 0, SUB), 2);
    let mut out = BytesMut::from(&world.output(conn)[..]);
    let frame = FrameDecoder::default().decode(&mut out).unwrap().unwrap();
    assert!(out.is_empty());
    assert_eq!(message(&BaseCommand::decode_from_slice(&frame.command).unwrap()), (1, 0, 0));
    let section = frame.payload.unwrap();
    assert_eq!(section.checksum_valid, Some(true));
    assert_eq!(MessageMetadata::decode_from_slice(&section.metadata).unwrap(), metadata);
    assert_eq!(section.payload, payload);
}

/// What a producer sends goes to the consumers once stored, after its receipt.
#[test]
fn a_producers_entries_reach_the_consumers() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    register(&mut world, conn, SEGMENT, 1);
    subscribe(&mut world, conn, shared(SUB, 1));
    flow(&mut world, conn, 1, 10);
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 1, sequence_id: 0, ..Default::default() }.into(),
        ..Default::default()
    };
    let (metadata, payload) = batch(&["hello"]);
    let mut buf = BytesMut::new();
    encode_payload_command(&send, &metadata, &payload, &mut buf).unwrap();
    world.input(conn, &buf);
    assert_eq!(types(&decode_all(&world.output(conn))), [Type::SEND_RECEIPT, Type::MESSAGE]);
    let stored = &world.stored_in(TOPIC, 0)[0];
    assert_eq!((&stored.value[..], &stored.payload), (&b"hello"[..], &payload));
}
