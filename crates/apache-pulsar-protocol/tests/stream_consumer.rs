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

//! The Stream consumer's state machine, driven by hand with the assignments a test chooses: its registration,
//! reconciles, retries, drains and close. Ported scenarios from Java's `ScalableStreamConsumerReconcileTest`. The
//! scenarios against a simulated controller are in the testkit.

use std::collections::{BTreeMap, BTreeSet};
use std::time::{Duration, Instant};

use apache_pulsar_protocol::assignment::AssignmentSession;
use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::frame::PayloadSection;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_subscribe::{InitialPosition, SubType};
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandMessage, IntRange, MessageIdData, MessageMetadata, ScalableAssignedSegment,
    ScalableConsumerAssignment, ScalableConsumerType, ServerError,
};
use apache_pulsar_protocol::stream_consumer::{Action, StreamConsumer};
use buffa::{EnumValue, Message};
use bytes::Bytes;

const TOPIC: &str = "topic://t/n/x";

fn options() -> ConsumerOptions {
    ConsumerOptions {
        subscription: "sub".into(),
        receiver_queue_size: 10,
        ack_group_time: Duration::ZERO,
        operation_timeout: Duration::from_secs(10),
        ..Default::default()
    }
}

fn topic_of(segment: u64) -> String {
    format!("segment://t/n/x/0000-ffff-{segment}")
}

/// An assignment of `segments`, each with its bucket ranges: none for a whole one.
fn assignment(epoch: u64, segments: &[(u64, &[(i32, i32)])]) -> ScalableConsumerAssignment {
    let segments = segments
        .iter()
        .map(|&(segment_id, ranges)| ScalableAssignedSegment {
            segment_id,
            hash_end: 0xffff,
            segment_topic: topic_of(segment_id),
            bucket_ranges: ranges.iter().map(|&(start, end)| IntRange { start, end, ..Default::default() }).collect(),
            ..Default::default()
        })
        .collect();
    ScalableConsumerAssignment { layout_epoch: epoch, segments, ..Default::default() }
}

fn whole(epoch: u64, segments: &[u64]) -> ScalableConsumerAssignment {
    let segments: Vec<(u64, &[(i32, i32)])> = segments.iter().map(|&s| (s, &[][..])).collect();
    assignment(epoch, &segments)
}

fn busy() -> RegisterError {
    RegisterError::Server { error: ServerError::ConsumerBusy, message: "busy".into() }
}

/// A Stream consumer, and the driver's view of it.
struct Test {
    consumer: StreamConsumer,
    now: Instant,
    /// The registration in flight, or done: its generation.
    registration: Option<u64>,
    /// The subscribe of each segment in flight: its generation.
    connecting: BTreeMap<u64, u64>,
    /// The consumer id of each subscribed segment, and its generation.
    subscribed: BTreeMap<u64, (u64, u64)>,
    /// Closes in flight: segment to generation.
    closing: BTreeMap<u64, u64>,
    /// Subscribe attempts per segment.
    attempts: BTreeMap<u64, u32>,
    /// What every subscribe of each segment declared: its type and ranges.
    modes: BTreeMap<u64, Vec<(SubType, usize)>>,
    unregistering: Option<u64>,
    outcome: Option<Result<(), RegisterError>>,
    closed: bool,
    next_id: u64,
}

impl Test {
    fn new(initial: &[u64]) -> Self {
        let now = Instant::now();
        let initial: BTreeSet<u64> = initial.iter().copied().collect();
        let consumer = StreamConsumer::new(options(), TOPIC.into(), "c".into(), initial, now);
        let mut test = Test {
            consumer,
            now,
            registration: None,
            connecting: BTreeMap::new(),
            subscribed: BTreeMap::new(),
            closing: BTreeMap::new(),
            attempts: BTreeMap::new(),
            modes: BTreeMap::new(),
            unregistering: None,
            outcome: None,
            closed: false,
            next_id: 1,
        };
        test.actions();
        test
    }

    /// Registered with `assignment`, its segments subscribed.
    fn registered(assignment: &ScalableConsumerAssignment) -> Self {
        let mut test = Test::new(&[]);
        test.register(assignment);
        test.subscribe_all();
        assert_eq!(test.outcome, Some(Ok(())));
        test
    }

    /// Does the actions, takes note of them, and returns the commands sent.
    fn actions(&mut self) -> Vec<BaseCommand> {
        let mut sent = Vec::new();
        while let Some(action) = self.consumer.poll_action() {
            match action {
                Action::Register { generation } => self.registration = Some(generation),
                Action::Unregister { generation } => self.unregistering = Some(generation),
                Action::Connect { segment, generation, topic } => {
                    assert_eq!(topic, topic_of(segment));
                    let command = self.consumer.subscribe_command(segment, 0).unwrap();
                    let ranges = command.subscribe.keySharedMeta.as_option().map_or(0, |m| m.hashRanges.len());
                    self.modes.entry(segment).or_default().push((command.subscribe.subType, ranges));
                    *self.attempts.entry(segment).or_default() += 1;
                    self.connecting.insert(segment, generation);
                }
                Action::Send { command, .. } => sent.push(*command),
                Action::CloseConsumer { segment, generation } => {
                    self.closing.insert(segment, generation);
                }
                Action::Subscribed(result) => self.outcome = Some(result),
                Action::Closed => self.closed = true,
            }
        }
        sent
    }

    fn register(&mut self, assignment: &ScalableConsumerAssignment) {
        let generation = self.registration.expect("a registration");
        self.consumer.on_registered(generation, assignment, self.now);
        self.actions();
    }

    fn push(&mut self, assignment: &ScalableConsumerAssignment) {
        let generation = self.registration.expect("a registration");
        self.consumer.on_assignment(generation, assignment, self.now);
        self.actions();
    }

    /// Answers the subscribe of `segment` in flight.
    fn answer(&mut self, segment: u64, refusal: Option<RegisterError>) {
        let generation = self.connecting.remove(&segment).expect("a subscribe in flight");
        match refusal {
            Some(error) => self.consumer.on_subscribe_failed(segment, generation, error, self.now),
            None => {
                self.next_id += 1;
                self.subscribed.insert(segment, (self.next_id, generation));
                self.consumer.on_subscribed(segment, generation, self.next_id, self.now);
            }
        }
        self.actions();
    }

    fn subscribe_all(&mut self) {
        while let Some(&segment) = self.connecting.keys().next() {
            self.answer(segment, None);
        }
    }

    /// Answers the closes in flight.
    fn close_all(&mut self) {
        for (segment, generation) in std::mem::take(&mut self.closing) {
            self.subscribed.remove(&segment);
            self.consumer.on_closed(segment, generation, self.now);
            self.actions();
        }
    }

    /// The broker pushes a one-message entry at `entry` of ledger 10 to `segment`'s consumer.
    fn deliver(&mut self, segment: u64, entry: u64) {
        let (consumer_id, generation) = self.subscribed[&segment];
        let message = CommandMessage {
            consumer_id,
            message_id: MessageIdData { ledgerId: 10, entryId: entry, ..Default::default() }.into(),
            ..Default::default()
        };
        let section = PayloadSection {
            broker_entry_metadata: None,
            checksum_valid: Some(true),
            metadata: MessageMetadata::default().encode_to_vec().into(),
            payload: Bytes::from_static(b"m"),
        };
        self.consumer.on_message(segment, generation, &message, &section, self.now);
        self.actions();
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
        self.consumer.handle_timeout(self.now);
        self.actions();
    }

    /// The time until the consumer's next deadline.
    fn wait(&self) -> Option<Duration> {
        self.consumer.poll_timeout().map(|at| at.saturating_duration_since(self.now))
    }
}

fn cumulative_acks(commands: &[BaseCommand]) -> Vec<u64> {
    commands.iter().filter(|c| c.r#type == EnumValue::Known(Type::ACK)).map(|c| c.ack.message_id[0].entryId).collect()
}

#[test]
fn registers_as_a_stream_consumer_named_after_it() {
    let t = Test::new(&[]);
    assert_eq!((t.consumer.topic(), t.consumer.name(), t.registration), (TOPIC, "c", Some(1)));
    let command = t.consumer.register_command(42);
    let subscribe = &command.scalableTopicSubscribe;
    assert_eq!((subscribe.topic.as_str(), subscribe.subscription.as_str()), (TOPIC, "sub"));
    assert_eq!((subscribe.consumer_name.as_str(), subscribe.consumer_id), ("c", 42));
    assert_eq!(subscribe.consumer_type, ScalableConsumerType::STREAM);
    let unsubscribe = AssignmentSession::unregister_command(42);
    assert_eq!(unsubscribe.scalableTopicUnsubscribe.consumer_id, 42);
}

/// A whole segment is subscribed Exclusive, a shared one `Key_Shared` with its buckets; a segment of the layout it
/// subscribed on at the initial position, a later one at the earliest; one of a legacy topic carries the V5 marker.
#[test]
fn attaches_each_segment_in_the_mode_its_assignment_gives() {
    let mut t = Test::new(&[0, 1]);
    let mut legacy = assignment(3, &[(0, &[]), (1, &[(0, 0x7fff), (0x8000, 0xffff)]), (2, &[])]);
    legacy.segments.push(ScalableAssignedSegment {
        segment_id: 3,
        segment_topic: "persistent://t/n/old".into(),
        ..Default::default()
    });
    t.consumer.on_registered(1, &legacy, t.now);
    let commands: BTreeMap<u64, BaseCommand> =
        (0..4).map(|s| (s, t.consumer.subscribe_command(s, 9).unwrap())).collect();
    let sub = |s: u64| commands[&s].subscribe.clone();
    assert_eq!(
        (sub(0).subType, sub(1).subType, sub(2).subType),
        (SubType::Exclusive, SubType::Key_Shared, SubType::Exclusive)
    );
    assert_eq!(sub(1).keySharedMeta.as_option().unwrap().hashRanges.len(), 2);
    assert_eq!(sub(0).consumer_name.as_deref(), Some("c-seg-0"));
    assert_eq!(
        (sub(0).initialPosition, sub(2).initialPosition),
        (Some(InitialPosition::Latest), Some(InitialPosition::Earliest))
    );
    assert!(sub(3).metadata.iter().any(|kv| kv.key == "__pulsar.v5.managed"));
    assert!(sub(0).metadata.is_empty());
    assert_eq!(t.consumer.subscribe_command(7, 9), None);
}

/// The subscribe completes once registered and every assigned segment is subscribed.
#[test]
fn subscribes_once_every_assigned_segment_is() {
    let mut t = Test::new(&[]);
    t.actions();
    t.register(&whole(0, &[0, 1]));
    assert_eq!(t.outcome, None);
    t.answer(0, None);
    assert_eq!(t.outcome, None);
    t.answer(1, None);
    assert_eq!(t.outcome, Some(Ok(())));
    assert_eq!(t.consumer.segments().collect::<Vec<_>>(), [0, 1]);
    // An empty assignment: nothing to wait for.
    let mut t = Test::new(&[]);
    t.register(&whole(0, &[]));
    assert_eq!(t.outcome, Some(Ok(())));
}

/// Java's `keepsAtMostOneRetryScheduled`: a segment's subscribe keeps being refused, as while its previous owner holds
/// it. Each push of the assignment retries it at once; at most one retry waits.
#[test]
fn keeps_at_most_one_retry_waiting() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.push(&whole(0, &[0, 1]));
    t.answer(1, Some(busy()));
    assert_eq!(t.attempts[&1], 1);
    let waiting = t.wait().expect("a retry");
    assert!(waiting <= Duration::from_millis(105), "{waiting:?}");
    for _ in 0..2 {
        t.push(&whole(0, &[0, 1]));
        t.answer(1, Some(busy()));
    }
    assert_eq!(t.attempts[&1], 3);
    t.advance(t.wait().unwrap());
    assert_eq!(t.attempts[&1], 4);
    t.answer(1, Some(busy()));
    assert!(t.wait().is_some(), "retried again while it keeps failing");
}

/// Java's `retriesANewFailureWithoutWaitingOutAnEarlierBackoff`: the backoff grows while a segment keeps failing, and
/// starts over once every segment is subscribed.
#[test]
fn a_new_failure_is_retried_without_the_earlier_backoff() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.push(&whole(0, &[0, 1]));
    t.answer(1, Some(busy()));
    for _ in 0..2 {
        t.advance(t.wait().unwrap());
        t.answer(1, Some(busy()));
    }
    assert!(t.wait().unwrap() >= Duration::from_millis(380), "the backoff grew: {:?}", t.wait());
    t.push(&whole(0, &[0, 1]));
    t.answer(1, None);
    assert_eq!(t.wait(), None);
    t.push(&whole(0, &[0, 1, 2]));
    t.answer(2, Some(busy()));
    assert!(t.wait().unwrap() < Duration::from_millis(200), "a new failure is retried soon: {:?}", t.wait());
}

/// Java's `keepsTheRetryThatFiresWhileASegmentDrains`: a segment moves to another member while the application holds
/// its message; the retry of another segment goes on meanwhile, and the drained segment closes once acknowledged.
#[test]
fn a_retry_goes_on_while_a_segment_drains() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.deliver(0, 5);
    let message = t.consumer.next_message(t.now).unwrap();
    t.actions();
    t.push(&whole(0, &[0, 1]));
    t.answer(1, Some(busy()));
    t.push(&whole(0, &[1]));
    t.answer(1, Some(busy()));
    // Segment 0 drains: it gives nothing more, and isn't closed yet.
    assert!(t.closing.is_empty());
    assert_eq!(t.consumer.segments().collect::<Vec<_>>(), Vec::<u64>::new());
    t.advance(t.wait().unwrap());
    assert_eq!(t.attempts[&1], 3);
    t.answer(1, Some(busy()));
    assert!(t.closing.is_empty());
    t.consumer.acknowledge_cumulative(&message.id, t.now);
    let sent = t.actions();
    assert_eq!(cumulative_acks(&sent), [5]);
    assert!(t.closing.contains_key(&0), "drained: it closes");
    assert!(t.wait().is_some(), "the refused segment is still retried");
}

/// Java's `appliesAnAssignmentThatArrivedDuringAFailedReconcileRightAway`.
#[test]
fn an_assignment_applies_while_a_segment_drains() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.deliver(0, 5);
    let _held = t.consumer.next_message(t.now).unwrap();
    t.push(&whole(0, &[1]));
    t.answer(1, Some(busy()));
    t.push(&whole(0, &[1, 2]));
    assert_eq!(t.attempts.get(&2), Some(&1), "applied at once");
}

/// Java's `resubscribesASegmentOnlyAfterItsReleaseDrained`: a segment that moves away and back while the application
/// holds its message is subscribed again only once that message is acknowledged and the old consumer closed.
#[test]
fn a_segment_comes_back_only_once_its_release_drained() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.deliver(0, 5);
    let message = t.consumer.next_message(t.now).unwrap();
    t.push(&whole(0, &[1]));
    t.push(&whole(0, &[0, 1]));
    assert_eq!(t.attempts[&0], 1, "not again while it drains");
    t.consumer.acknowledge_cumulative(&message.id, t.now);
    t.actions();
    assert_eq!(t.attempts[&0], 1);
    t.close_all();
    assert_eq!(t.attempts[&0], 2, "again once closed");
    t.answer(0, None);
    t.deliver(0, 6);
    assert_eq!(t.consumer.next_message(t.now).map(|m| m.id.entry.entry_id), Some(6));
}

/// A mode flip drains the segment, closes its consumer, and subscribes it again in the other mode; an assignment that
/// repeats a segment unchanged leaves it alone; an older one is ignored.
#[test]
fn a_mode_flip_drains_then_resubscribes() {
    let mut t = Test::registered(&whole(2, &[0]));
    t.push(&whole(2, &[0]));
    t.push(&whole(1, &[]));
    assert_eq!(t.attempts[&0], 1);
    t.deliver(0, 5);
    let message = t.consumer.next_message(t.now).unwrap();
    t.push(&assignment(3, &[(0, &[(0, 0x7fff), (0x8000, 0xffff)])]));
    assert!(t.closing.is_empty(), "drains first");
    t.consumer.acknowledge_cumulative(&message.id, t.now);
    t.actions();
    t.close_all();
    assert_eq!(t.modes[&0], [(SubType::Exclusive, 0), (SubType::Key_Shared, 2)]);
}

/// A failure of the first registration fails the subscribe; nothing is left to leave.
#[test]
fn the_first_registration_failing_fails_the_subscribe() {
    let mut t = Test::new(&[]);
    let refused = RegisterError::Server { error: ServerError::TopicNotFound, message: "no".into() };
    t.consumer.on_register_failed(1, refused.clone(), t.now);
    t.actions();
    assert_eq!((t.outcome, t.closed, t.unregistering), (Some(Err(refused)), true, None));
    assert!(t.consumer.is_closed());
}

/// A segment refused for another reason than a rebalance, or past the operation timeout, fails the subscribe. The
/// consumer leaves the group, and closes the segments it subscribed.
#[test]
fn a_segment_refused_for_good_fails_the_subscribe() {
    let mut t = Test::new(&[]);
    t.register(&whole(0, &[0, 1]));
    t.answer(0, None);
    let refused = RegisterError::Server { error: ServerError::AuthorizationError, message: "no".into() };
    t.answer(1, Some(refused.clone()));
    assert_eq!(t.outcome, Some(Err(refused)));
    assert_eq!(t.unregistering, Some(1));
    t.close_all();
    assert!(!t.closed, "the unsubscribe is out");
    t.consumer.on_unregistered(1);
    t.actions();
    assert!(t.closed);
    // A rebalance's refusal past the deadline.
    let mut t = Test::new(&[]);
    t.register(&whole(0, &[0]));
    t.answer(0, Some(busy()));
    assert_eq!(t.outcome, None, "waited out");
    t.now += Duration::from_secs(10);
    t.consumer.handle_timeout(t.now);
    t.actions();
    t.answer(0, Some(busy()));
    assert_eq!(t.outcome, Some(Err(busy())));
}

/// After a connection loss the consumer registers again after a backoff, keeps its segments meanwhile, and retries a
/// registration that fails. Pushes and answers for older registrations change nothing.
#[test]
fn registers_again_after_a_connection_loss() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.consumer.on_session_lost(2, t.now);
    t.consumer.on_assignment(2, &whole(0, &[]), t.now);
    assert_eq!(t.consumer.segments().count(), 1);
    t.consumer.on_session_lost(1, t.now);
    t.consumer.on_assignment(1, &whole(1, &[]), t.now);
    assert_eq!(t.consumer.segments().count(), 1, "not while unregistered");
    assert!(t.wait().unwrap() <= Duration::from_millis(105));
    t.advance(t.wait().unwrap());
    assert_eq!(t.registration, Some(2));
    t.consumer.on_register_failed(1, RegisterError::Unavailable("old".into()), t.now);
    t.consumer.on_register_failed(2, RegisterError::Unavailable("down".into()), t.now);
    t.consumer.on_registered(1, &whole(1, &[]), t.now);
    t.actions();
    assert!(t.wait().unwrap() > Duration::from_millis(150), "the backoff grew");
    t.advance(t.wait().unwrap());
    assert_eq!(t.registration, Some(3));
    t.register(&whole(0, &[0]));
    assert_eq!(t.consumer.segments().count(), 1);
    assert_eq!(t.attempts[&0], 1, "the same assignment changes nothing");
}

/// Closed while registering, it leaves once the registration completes; a registration that fails leaves nothing to
/// leave.
#[test]
fn a_close_while_registering_leaves_once_registered() {
    let mut t = Test::new(&[]);
    t.consumer.close(t.now);
    t.consumer.close(t.now);
    t.actions();
    assert!(!t.closed);
    t.consumer.on_registered(1, &whole(0, &[0]), t.now);
    t.actions();
    assert_eq!((t.unregistering, t.attempts.get(&0)), (Some(1), None));
    t.consumer.on_unregistered(2);
    t.consumer.on_unregistered(1);
    t.actions();
    assert_eq!((t.closed, t.outcome.clone()), (true, None));
    let mut t = Test::new(&[]);
    t.consumer.close(t.now);
    t.consumer.on_register_failed(1, RegisterError::Unavailable("down".into()), t.now);
    t.actions();
    assert!(t.closed);
}

/// Closed while waiting to register again, it has nothing to leave: it closes its segments, without draining them.
#[test]
fn a_close_while_unregistered_closes_the_segments() {
    let mut t = Test::registered(&whole(0, &[0]));
    t.deliver(0, 5);
    let _held = t.consumer.next_message(t.now).unwrap();
    t.consumer.on_session_lost(1, t.now);
    t.consumer.close(t.now);
    t.actions();
    assert_eq!(t.unregistering, None);
    assert!(t.closing.contains_key(&0), "no drain on close");
    t.close_all();
    assert!(t.closed);
}

/// A cumulative ack skips the segments of the vector the consumer no longer holds.
#[test]
fn a_cumulative_ack_skips_the_segments_let_go_of() {
    let mut t = Test::registered(&whole(0, &[0, 1]));
    t.deliver(0, 5);
    t.deliver(1, 7);
    let first = t.consumer.next_message(t.now).unwrap();
    let second = t.consumer.next_message(t.now).unwrap();
    assert_eq!(second.id.positions.len(), 2);
    assert_eq!(t.consumer.available(), 0);
    t.push(&whole(0, &[1]));
    t.consumer.acknowledge_cumulative(&first.id, t.now);
    assert_eq!(cumulative_acks(&t.actions()), [5]);
    t.close_all();
    t.consumer.acknowledge_cumulative(&second.id, t.now);
    assert_eq!(cumulative_acks(&t.actions()), [7]);
}
