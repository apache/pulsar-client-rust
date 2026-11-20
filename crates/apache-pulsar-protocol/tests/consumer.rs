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

//! The segment consumer's state machine, driven by hand: permits, grouped acks, negative acks, reconnects,
//! failures and close. The scenarios against a simulated broker are in the testkit.

use std::time::{Duration, Instant};

use apache_pulsar_protocol::consumer::{Action, ConsumerOptions, SegmentConsumer};
use apache_pulsar_protocol::frame::PayloadSection;
use apache_pulsar_protocol::message_id::EntryId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_ack::ValidationError;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandMessage, EncryptionKeys, KeyValue, MessageIdData, MessageMetadata, ServerError,
    SingleMessageMetadata,
};
use buffa::{EnumValue, Message};
use bytes::{BufMut, Bytes, BytesMut};

const TOPIC: &str = "segment://t/n/x/0000-ffff-3";

fn options() -> ConsumerOptions {
    ConsumerOptions {
        subscription: "sub".into(),
        consumer_name: Some("c-seg-3".into()),
        receiver_queue_size: 4,
        ack_group_time: Duration::from_millis(100),
        max_ack_group_size: 3,
        negative_ack_delay: Duration::from_secs(1),
        operation_timeout: Duration::from_secs(1),
        ..Default::default()
    }
}

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue { key: key.into(), value: value.into(), ..Default::default() }
}

/// A one-message entry at `entry` of ledger 10.
fn single(entry: u64) -> (CommandMessage, PayloadSection) {
    entry_with(entry, MessageMetadata::default(), Bytes::from_static(b"m"), Vec::new())
}

/// A batch of `count` messages at `entry` of ledger 10; `ack_set` as the broker sends it.
fn batch(entry: u64, count: i32, ack_set: Vec<i64>) -> (CommandMessage, PayloadSection) {
    let mut payload = BytesMut::new();
    for _ in 0..count {
        let single = SingleMessageMetadata { payload_size: 1, ..Default::default() };
        payload.put_u32(single.encoded_len());
        payload.put_slice(&single.encode_to_vec());
        payload.put_u8(b'm');
    }
    let metadata = MessageMetadata { num_messages_in_batch: Some(count), ..Default::default() };
    entry_with(entry, metadata, payload.freeze(), ack_set)
}

fn entry_with(
    entry: u64,
    metadata: MessageMetadata,
    payload: Bytes,
    ack_set: Vec<i64>,
) -> (CommandMessage, PayloadSection) {
    let message = CommandMessage {
        consumer_id: 7,
        message_id: MessageIdData { ledgerId: 10, entryId: entry, ..Default::default() }.into(),
        ack_set,
        ..Default::default()
    };
    let section = PayloadSection {
        broker_entry_metadata: None,
        checksum_valid: Some(true),
        metadata: metadata.encode_to_vec().into(),
        payload,
    };
    (message, section)
}

fn id(entry: i64, batch_index: i32, batch_size: i32) -> EntryId {
    EntryId { ledger_id: 10, entry_id: entry, partition: -1, batch_index, batch_size }
}

struct Test {
    consumer: SegmentConsumer,
    now: Instant,
}

impl Test {
    fn new(options: ConsumerOptions) -> Self {
        let now = Instant::now();
        Self { consumer: SegmentConsumer::new(options, 3, TOPIC.into(), now), now }
    }

    /// A consumer subscribed as consumer 7, with its first actions taken.
    fn subscribed(options: ConsumerOptions) -> Self {
        let mut test = Self::new(options);
        test.actions();
        test.consumer.on_subscribed(1, 7);
        test.actions();
        test
    }

    fn actions(&mut self) -> Vec<Action> {
        std::iter::from_fn(|| self.consumer.poll_action()).collect()
    }

    fn deliver(&mut self, generation: u64, (message, section): (CommandMessage, PayloadSection)) {
        self.consumer.on_message(generation, &message, &section);
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
        self.consumer.handle_timeout(self.now);
    }

    fn take(&mut self, n: usize) {
        for _ in 0..n {
            self.consumer.next_message().expect("a message");
        }
    }
}

fn commands(actions: &[Action]) -> Vec<&BaseCommand> {
    actions
        .iter()
        .filter_map(|a| match a {
            Action::Send(command) => Some(&**command),
            _ => None,
        })
        .collect()
}

fn flows(actions: &[Action]) -> Vec<u32> {
    commands(actions)
        .iter()
        .filter(|c| c.r#type == EnumValue::Known(Type::FLOW))
        .map(|c| c.flow.messagePermits)
        .collect()
}

/// Each ack's `(ledger, entry, ack_set)` ids, and its validation error.
type Ack = (Vec<(u64, u64, Vec<i64>)>, Option<ValidationError>);

fn acks(actions: &[Action]) -> Vec<Ack> {
    commands(actions)
        .iter()
        .filter(|c| c.r#type == EnumValue::Known(Type::ACK))
        .map(|c| {
            let ids = c.ack.message_id.iter().map(|id| (id.ledgerId, id.entryId, id.ack_set.clone())).collect();
            (ids, c.ack.validation_error)
        })
        .collect()
}

fn redelivered(actions: &[Action]) -> Vec<Vec<u64>> {
    commands(actions)
        .iter()
        .filter(|c| c.r#type == EnumValue::Known(Type::REDELIVER_UNACKNOWLEDGED_MESSAGES))
        .map(|c| c.redeliverUnacknowledgedMessages.message_ids.iter().map(|id| id.entryId).collect())
        .collect()
}

#[test]
fn the_defaults_are_javas() {
    let options = ConsumerOptions::default();
    assert_eq!((options.sub_type, options.initial_position), (SubType::Shared, InitialPosition::Latest));
    assert_eq!((options.receiver_queue_size, options.max_ack_group_size), (1000, 1000));
    assert_eq!(
        (options.ack_group_time, options.negative_ack_delay),
        (Duration::from_millis(100), Duration::from_secs(60))
    );
    assert_eq!(options.operation_timeout, Duration::from_secs(30));
}

#[test]
fn subscribes_and_grants_permits_as_messages_are_taken() {
    let mut options = options();
    options.priority_level = Some(2);
    options.properties = vec![("role".into(), "worker".into())];
    options.subscription_properties = vec![("team".into(), "a".into())];
    let mut t = Test::new(options);
    assert_eq!(t.consumer.topic(), TOPIC);
    assert_eq!(t.actions(), [Action::Connect { generation: 1 }]);
    let command = t.consumer.subscribe_command(7);
    let subscribe = &command.subscribe;
    assert_eq!((subscribe.topic.as_str(), subscribe.subscription.as_str(), subscribe.consumer_id), (TOPIC, "sub", 7));
    assert_eq!((subscribe.subType, subscribe.consumer_name.as_deref()), (SubType::Shared, Some("c-seg-3")));
    assert_eq!((subscribe.initialPosition, subscribe.priority_level), (Some(InitialPosition::Latest), Some(2)));
    assert_eq!(
        (subscribe.metadata.clone(), subscribe.subscription_properties.clone()),
        (vec![kv("role", "worker")], vec![kv("team", "a")])
    );

    t.consumer.on_subscribed(1, 7);
    let actions = t.actions();
    assert_eq!(actions[0], Action::Subscribed(Ok(())));
    assert_eq!(flows(&actions), [4]);
    for entry in 0..3 {
        t.deliver(1, single(entry));
    }
    assert_eq!(t.consumer.available(), 3);
    let first = t.consumer.next_message().unwrap();
    assert_eq!((first.id.segment_id, first.id.entry.entry_id), (3, 0));
    assert!(t.actions().is_empty());
    // Half the receiver queue taken: as many permits again.
    t.take(1);
    assert_eq!(flows(&t.actions()), [2]);
    t.take(1);
    assert!(t.consumer.next_message().is_none());
    assert!(t.actions().is_empty());
}

/// The broker counts every message of an entry against the permits, those the consumer leaves out too.
#[test]
fn messages_left_out_count_as_taken() {
    let mut t = Test::subscribed(options());
    // Two of three acknowledged already.
    t.deliver(1, batch(0, 3, vec![0b001]));
    assert_eq!((t.consumer.available(), flows(&t.actions())), (1, vec![2]));
    // A corrupt entry is dropped with the reason.
    let (message, mut section) = single(1);
    section.checksum_valid = Some(false);
    t.consumer.on_message(1, &message, &section);
    assert_eq!(acks(&t.actions()), [(vec![(10, 1, vec![])], Some(ValidationError::ChecksumMismatch))]);
    // An encrypted one is left for another consumer.
    let encrypted = MessageMetadata { encryption_keys: vec![EncryptionKeys::default()], ..Default::default() };
    t.deliver(1, entry_with(2, encrypted, Bytes::from_static(b"?"), Vec::new()));
    let actions = t.actions();
    assert!(acks(&actions).is_empty());
    assert_eq!(flows(&actions), [2]);
}

#[test]
fn acks_go_out_together() {
    let mut t = Test::subscribed(options());
    t.consumer.acknowledge(&id(0, -1, 0), t.now);
    t.consumer.acknowledge(&id(1, -1, 0), t.now);
    assert!(t.actions().is_empty());
    assert_eq!(t.consumer.poll_timeout(), Some(t.now + Duration::from_millis(100)));
    // The group is full: out at once.
    t.consumer.acknowledge(&id(2, -1, 0), t.now);
    assert_eq!(acks(&t.actions()), [(vec![(10, 0, vec![]), (10, 1, vec![]), (10, 2, vec![])], None)]);
    assert_eq!(t.consumer.poll_timeout(), None);

    // An index of a batch, then the rest of it: whole entries first, then batch indexes.
    t.consumer.acknowledge(&id(5, 0, 3), t.now);
    t.consumer.acknowledge(&id(4, -1, 0), t.now);
    t.advance(Duration::from_millis(99));
    assert!(t.actions().is_empty());
    t.advance(Duration::from_millis(1));
    assert_eq!(acks(&t.actions()), [(vec![(10, 4, vec![]), (10, 5, vec![0b110])], None)]);
    // A group names only the indexes acknowledged in it: the broker merges them into what it has, and drops the
    // entry once none is left.
    t.consumer.acknowledge(&id(5, 1, 3), t.now);
    t.consumer.acknowledge(&id(5, 2, 3), t.now);
    // A batch of one is a whole entry.
    t.consumer.acknowledge(&id(6, 0, 1), t.now);
    t.advance(Duration::from_millis(100));
    assert_eq!(acks(&t.actions()), [(vec![(10, 6, vec![]), (10, 5, vec![0b001])], None)]);
    // Every index acknowledged within one group is a whole entry.
    t.consumer.acknowledge(&id(8, 0, 2), t.now);
    t.consumer.acknowledge(&id(8, 1, 2), t.now);
    t.advance(Duration::from_millis(100));
    assert_eq!(acks(&t.actions()), [(vec![(10, 8, vec![])], None)]);
    // Past 64 messages the bitset takes more words.
    for index in 0..65 {
        t.consumer.acknowledge(&id(7, index, 66), t.now);
    }
    t.advance(Duration::from_millis(100));
    assert_eq!(acks(&t.actions()), [(vec![(10, 7, vec![0, 0b10])], None)]);
    // A batch of exactly 64 fills one word.
    t.consumer.acknowledge(&id(9, 63, 64), t.now);
    t.advance(Duration::from_millis(100));
    assert_eq!(acks(&t.actions()), [(vec![(10, 9, vec![i64::MAX])], None)]);

    // No group time: each ack goes out alone.
    let mut options = options();
    options.ack_group_time = Duration::ZERO;
    let mut t = Test::subscribed(options);
    t.consumer.acknowledge(&id(0, -1, 0), t.now);
    assert_eq!(acks(&t.actions()), [(vec![(10, 0, vec![])], None)]);
    // A timer with nothing to send sends nothing.
    t.advance(Duration::from_secs(1));
    assert!(t.actions().is_empty());
}

#[test]
fn acks_made_while_subscribing_go_first() {
    let mut t = Test::new(options());
    t.actions();
    t.consumer.acknowledge(&id(0, -1, 0), t.now);
    t.advance(Duration::from_millis(100));
    assert!(t.actions().is_empty());
    t.consumer.on_subscribed(1, 7);
    let actions = t.actions();
    assert_eq!(actions[0], Action::Subscribed(Ok(())));
    assert_eq!(acks(&actions[1..2]), [(vec![(10, 0, vec![])], None)]);
    assert_eq!(flows(&actions[2..]), [4]);
}

#[test]
fn negative_acks_ask_for_their_entries_again() {
    let mut t = Test::subscribed(options());
    // Two messages of one batch: one entry.
    t.consumer.negative_acknowledge(&id(0, 0, 2), t.now);
    t.consumer.negative_acknowledge(&id(0, 1, 2), t.now);
    t.consumer.negative_acknowledge(&id(1, -1, 0), t.now);
    // Acknowledged after all.
    t.consumer.acknowledge(&id(1, -1, 0), t.now);
    assert_eq!(t.consumer.poll_timeout(), Some(t.now + Duration::from_millis(100)));
    t.advance(Duration::from_millis(999));
    assert!(redelivered(&t.actions()).is_empty());
    t.advance(Duration::from_millis(1));
    assert_eq!(redelivered(&t.actions()), [vec![0]]);
    assert_eq!(t.consumer.poll_timeout(), None);

    // A thousand entries a request.
    for entry in 0..1001 {
        t.consumer.negative_acknowledge(&id(entry, -1, 0), t.now);
    }
    t.advance(Duration::from_secs(1));
    let requests = redelivered(&t.actions());
    assert_eq!(requests.iter().map(Vec::len).collect::<Vec<_>>(), [1000, 1]);

    // Without a subscription, the broker delivers them again anyway.
    t.consumer.negative_acknowledge(&id(9, -1, 0), t.now);
    t.consumer.on_disconnected(1, t.now);
    t.advance(Duration::from_secs(1));
    assert!(redelivered(&t.actions()).is_empty());
}

#[test]
fn a_reconnect_drops_what_the_application_has_not_taken() {
    let mut t = Test::subscribed(options());
    t.deliver(1, single(0));
    t.deliver(1, single(1));
    t.take(1);
    t.consumer.on_disconnected(1, t.now);
    assert_eq!(t.consumer.available(), 0);
    let retry = t.consumer.poll_timeout().unwrap();
    assert!(retry >= t.now + Duration::from_millis(95) && retry <= t.now + Duration::from_millis(105));
    t.advance(Duration::from_millis(50));
    assert!(t.actions().is_empty());
    t.advance(Duration::from_millis(100));
    assert_eq!(t.actions(), [Action::Connect { generation: 2 }]);
    // Late news from the first connection.
    t.deliver(1, single(2));
    t.consumer.on_disconnected(1, t.now);
    assert_eq!(t.consumer.available(), 0);
    // Subscribed again: full permits, and no second outcome.
    t.consumer.on_subscribed(2, 8);
    assert_eq!(t.actions(), [Action::Send(Box::new(permits(8, 4)))]);
}

fn permits(consumer_id: u64, permits: u32) -> BaseCommand {
    let flow =
        apache_pulsar_protocol::proto::CommandFlow { consumer_id, messagePermits: permits, ..Default::default() };
    BaseCommand { r#type: Type::FLOW.into(), flow: flow.into(), ..Default::default() }
}

#[test]
fn subscribe_failures() {
    let refused = |error| RegisterError::Server { error, message: "no".into() };
    // Worth retrying, within the operation timeout.
    let mut t = Test::new(options());
    t.actions();
    t.consumer.on_subscribe_failed(1, refused(ServerError::ServiceNotReady), t.now);
    t.advance(Duration::from_millis(200));
    assert_eq!(t.actions(), [Action::Connect { generation: 2 }]);
    t.consumer.on_subscribe_failed(2, RegisterError::Unavailable("down".into()), t.now);
    t.advance(Duration::from_millis(300));
    assert_eq!(t.actions(), [Action::Connect { generation: 3 }]);
    // Past it, the subscribe fails.
    t.advance(Duration::from_secs(1));
    t.consumer.on_subscribe_failed(3, refused(ServerError::ServiceNotReady), t.now);
    let failed = Action::Subscribed(Err(refused(ServerError::ServiceNotReady)));
    assert_eq!(t.actions(), [failed, Action::Closed]);

    // Not worth retrying.
    let mut t = Test::new(options());
    t.actions();
    t.consumer.on_subscribe_failed(1, refused(ServerError::TopicNotFound), t.now);
    assert_eq!(t.actions(), [Action::Subscribed(Err(refused(ServerError::TopicNotFound))), Action::Closed]);

    // Once subscribed, errors worth retrying are retried without end, and any other closes the consumer.
    let mut t = Test::subscribed(options());
    t.consumer.on_disconnected(1, t.now);
    t.advance(Duration::from_secs(5));
    t.actions();
    t.consumer.on_subscribe_failed(2, RegisterError::Unavailable("down".into()), t.now);
    t.advance(Duration::from_secs(5));
    assert_eq!(t.actions(), [Action::Connect { generation: 3 }]);
    t.consumer.on_subscribe_failed(3, refused(ServerError::TopicNotFound), t.now);
    assert_eq!(t.actions(), [Action::Closed]);
}

#[test]
fn close() {
    // Subscribed: the acks go out, then the close.
    let mut t = Test::subscribed(options());
    t.deliver(1, single(0));
    t.consumer.negative_acknowledge(&id(1, -1, 0), t.now);
    t.consumer.acknowledge(&id(2, -1, 0), t.now);
    t.consumer.close();
    let actions = t.actions();
    assert_eq!(acks(&actions), [(vec![(10, 2, vec![])], None)]);
    assert_eq!(actions.last(), Some(&Action::CloseConsumer { generation: 1 }));
    assert_eq!((t.consumer.available(), t.consumer.poll_timeout()), (0, None));
    t.consumer.close();
    t.consumer.on_closed(2);
    assert!(t.actions().is_empty());
    t.consumer.on_closed(1);
    assert_eq!(t.actions(), [Action::Closed]);
    t.consumer.on_closed(1);
    assert!(t.actions().is_empty());

    // Subscribing: closed now; the subscription is closed unused when it completes.
    let mut t = Test::new(options());
    t.actions();
    t.consumer.close();
    assert_eq!(t.actions(), [Action::Closed]);
    t.consumer.on_subscribed(1, 7);
    assert_eq!(t.actions(), [Action::CloseConsumer { generation: 1 }]);
    t.consumer.on_closed(1);
    assert!(t.actions().is_empty());
    // Or not at all, if it fails.
    let mut t = Test::new(options());
    t.actions();
    t.consumer.close();
    t.consumer.on_subscribe_failed(1, RegisterError::Unavailable("down".into()), t.now);
    assert_eq!(t.actions(), [Action::Closed]);

    // Waiting to subscribe again.
    let mut t = Test::subscribed(options());
    t.consumer.on_disconnected(1, t.now);
    t.consumer.close();
    assert_eq!(t.actions(), [Action::Closed]);

    // A connection that closes under a close completes it.
    let mut t = Test::subscribed(options());
    t.consumer.close();
    t.actions();
    t.consumer.on_disconnected(1, t.now);
    assert_eq!(t.actions(), [Action::Closed]);
    let mut t = Test::new(options());
    t.actions();
    t.consumer.close();
    t.actions();
    t.consumer.on_disconnected(1, t.now);
    t.consumer.on_subscribed(1, 7);
    assert!(t.actions().is_empty());
}

#[test]
fn a_segment_drains() {
    let mut t = Test::subscribed(options());
    t.deliver(1, single(0));
    // An end of the topic from an older subscription is no news.
    t.consumer.on_reached_end_of_topic(0);
    t.consumer.on_reached_end_of_topic(1);
    assert!(!t.consumer.is_drained());
    t.take(1);
    t.consumer.acknowledge(&id(0, -1, 0), t.now);
    assert!(!t.consumer.is_drained(), "the ack is still to go out");
    t.advance(Duration::from_millis(100));
    t.consumer.acknowledge(&id(1, 0, 2), t.now);
    assert!(!t.consumer.is_drained());
    t.advance(Duration::from_millis(100));
    assert!(t.consumer.is_drained());
}

#[test]
fn news_from_another_subscription_is_ignored() {
    let mut t = Test::new(options());
    t.actions();
    t.consumer.on_subscribed(2, 7);
    t.consumer.on_subscribe_failed(2, RegisterError::Unavailable("down".into()), t.now);
    t.deliver(1, single(0));
    t.consumer.on_disconnected(2, t.now);
    t.consumer.on_closed(1);
    t.consumer.on_reached_end_of_topic(1);
    assert!(t.actions().is_empty());
    assert_eq!(t.consumer.available(), 0);
    t.consumer.on_subscribed(1, 7);
    t.actions();
    t.deliver(2, single(0));
    assert_eq!(t.consumer.available(), 0);
}
