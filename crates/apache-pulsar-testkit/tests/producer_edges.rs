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

//! Edge paths of the producer's state machine, driven by hand: what a broker rarely does, and races. The scenarios
//! against a simulated cluster are in `producer.rs`.

use std::rc::Rc;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::layout::{Layout, LayoutError};
use apache_pulsar_protocol::message_id::{EntryId, MessageId};
use apache_pulsar_protocol::producer::{
    Action, Batching, OutgoingMessage, Producer, ProducerOptions, RegisterError, SendError, SendId,
};
use apache_pulsar_protocol::proto::{
    CommandProducerSuccess, CommandSendError, CommandSendReceipt, CompressionType, MessageIdData, ProducerAccessMode,
    ScalableTopicDAG, SegmentInfoProto, SegmentState, ServerError,
};
use bytes::Bytes;

const TOPIC: &str = "topic://t/n/x";

/// A layout: `(id, start, end, active)` per segment.
fn layout(epoch: u64, segments: &[(u64, u32, u32, bool)]) -> Rc<Layout> {
    let segments = segments
        .iter()
        .map(|&(id, start, end, active)| SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: if active { SegmentState::ACTIVE } else { SegmentState::SEALED },
            ..Default::default()
        })
        .collect();
    Rc::new(Layout::from_dag(&ScalableTopicDAG { epoch, segments, ..Default::default() }, TOPIC).unwrap())
}

fn options(mode: ProducerAccessMode) -> ProducerOptions {
    ProducerOptions {
        name: Some("p".into()),
        access_mode: mode,
        send_timeout: None,
        initial_sequence_id: None,
        properties: Vec::new(),
        schema: None,
        operation_timeout: Duration::from_secs(30),
        batching: None,
        compression: CompressionType::NONE,
    }
}

fn keyed(key: &str) -> OutgoingMessage {
    OutgoingMessage { key: Some(key.into()), payload: Bytes::from_static(b"x"), ..Default::default() }
}

fn keyless() -> OutgoingMessage {
    OutgoingMessage { payload: Bytes::from_static(b"x"), ..Default::default() }
}

/// A key routed to `[start, end]`.
fn key_in(start: u16, end: u16) -> String {
    (0..).map(|i| format!("k{i}")).find(|k| (start..=end).contains(&KeyHash::of(k.as_bytes()).segment_hash())).unwrap()
}

struct Test {
    producer: Producer,
    now: Instant,
}

impl Test {
    fn new(mode: ProducerAccessMode, layout: Rc<Layout>) -> Self {
        let now = Instant::now();
        Self { producer: Producer::new(options(mode), layout, now), now }
    }

    fn actions(&mut self) -> Vec<Action> {
        std::iter::from_fn(|| self.producer.poll_action()).collect()
    }

    /// The `(segment, generation)` of each connect among `actions`.
    fn connects(actions: &[Action]) -> Vec<(u64, u64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Connect { segment, generation, .. } => Some((*segment, *generation)),
                _ => None,
            })
            .collect()
    }

    fn sent(actions: &[Action]) -> Vec<(u64, u64)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Send { segment, command, .. } => Some((*segment, command.send.sequence_id)),
                _ => None,
            })
            .collect()
    }

    fn done(actions: &[Action]) -> Vec<(SendId, Result<MessageId, SendError>)> {
        actions
            .iter()
            .filter_map(|a| match a {
                Action::Done { send, result } => Some((*send, result.clone())),
                _ => None,
            })
            .collect()
    }

    fn send(&mut self, message: OutgoingMessage) -> SendId {
        self.producer.send(message, 0, self.now).unwrap()
    }

    fn register(&mut self, segment: u64, generation: u64, producer_id: u64) {
        let success = CommandProducerSuccess { producer_name: format!("p-seg-{segment}"), ..Default::default() };
        self.producer.on_registered(segment, generation, producer_id, &success, usize::MAX, self.now);
    }

    fn receipt(&mut self, segment: u64, generation: u64, sequence_id: u64, entry: Option<u64>) {
        let receipt = CommandSendReceipt {
            sequence_id,
            message_id: entry.map(|e| MessageIdData { ledgerId: 1, entryId: e, ..Default::default() }).into(),
            ..Default::default()
        };
        self.producer.on_receipt(segment, generation, &receipt, self.now);
    }

    fn send_error(&mut self, segment: u64, generation: u64, sequence_id: u64, error: ServerError) {
        let error = CommandSendError { sequence_id, error, message: "refused".into(), ..Default::default() };
        self.producer.on_send_error(segment, generation, &error, self.now);
    }

    fn advance(&mut self, by: Duration) {
        self.now += by;
        self.producer.handle_timeout(self.now);
    }
}

fn one_segment() -> Rc<Layout> {
    layout(1, &[(0, 0, 0xffff, true)])
}

#[test]
fn receipts_are_matched_against_the_head_of_the_queue() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let first = t.send(keyless());
    let actions = t.actions();
    let [(0, generation)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
    t.register(0, generation, 7);
    let second = t.send(keyless());
    assert_eq!(Test::sent(&t.actions()), [(0, 0), (0, 1)]);
    // A receipt past the head: the two sides disagree, the connection is closed to start over.
    t.receipt(0, generation, 5, Some(1));
    assert_eq!(t.actions(), [Action::Disconnect { segment: 0, generation }]);
    // The head's receipt completes it; one without a position completes as earliest.
    t.receipt(0, generation, 0, Some(10));
    t.receipt(0, generation, 1, None);
    let done = Test::done(&t.actions());
    assert_eq!(done.len(), 2);
    assert_eq!(done[0].0, first);
    assert_eq!(done[0].1.as_ref().unwrap().entry.entry_id, 10);
    assert_eq!((done[1].0, done[1].1.as_ref().unwrap().entry), (second, EntryId::EARLIEST));
    // A late receipt, behind the head, is dropped; so is one for another registration.
    let third = t.send(keyless());
    t.actions();
    t.receipt(0, generation, 1, Some(11));
    t.receipt(0, generation + 1, 2, Some(12));
    assert!(t.actions().is_empty());
    t.receipt(0, generation, 2, Some(12));
    assert_eq!(Test::done(&t.actions())[0].0, third);
    assert_eq!(t.producer.last_sequence_id(), 2);
}

#[test]
fn send_errors() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let first = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    let second = t.send(keyless());
    t.actions();
    // NotAllowed fails the head only, and only if it is the one meant.
    t.send_error(0, generation, 1, ServerError::NotAllowedError);
    assert!(t.actions().is_empty());
    t.send_error(0, generation, 0, ServerError::NotAllowedError);
    let done = Test::done(&t.actions());
    assert_eq!(done.len(), 1);
    assert!(
        matches!(&done[0], (send, Err(SendError::Server { error: ServerError::NotAllowedError, .. })) if *send == first)
    );
    // Anything else but a terminated topic: register again and write everything again.
    t.send_error(0, generation, 1, ServerError::ChecksumError);
    assert_eq!(t.actions(), [Action::Disconnect { segment: 0, generation }]);
    // For another registration: ignored.
    t.send_error(0, generation + 1, 1, ServerError::TopicTerminatedError);
    assert!(t.actions().is_empty());
    // A terminated topic: the segment is gone, its messages wait for the next layout.
    t.send_error(0, generation, 1, ServerError::TopicTerminatedError);
    assert!(Test::done(&t.actions()).is_empty());
    assert_eq!(t.producer.sends_parked_or_held(), 1);
    assert!(t.producer.poll_timeout().is_some(), "a retirement check is due");
    // The next layout moves it to the successor.
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
    let actions = t.actions();
    let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
    t.register(1, next, 8);
    assert_eq!(Test::sent(&t.actions()), [(1, 0)]);
    t.receipt(1, next, 0, Some(1));
    assert_eq!(Test::done(&t.actions())[0].0, second);
}

#[test]
fn registration_failures() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let refused = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    // For another registration: ignored.
    t.producer.on_register_failed(0, generation + 1, RegisterError::Unavailable("x".into()), t.now);
    assert!(t.actions().is_empty());
    // Refused for good: its messages fail, and the next send registers again.
    let denied = RegisterError::Server { error: ServerError::AuthorizationError, message: "no".into() };
    t.producer.on_register_failed(0, generation, denied, t.now);
    let done = Test::done(&t.actions());
    assert!(
        matches!(&done[..], [(send, Err(SendError::Server { error: ServerError::AuthorizationError, .. }))] if *send == refused)
    );
    let retried = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    // Worth retrying, within the first registration's time: after a backoff.
    let busy = RegisterError::Server { error: ServerError::ServiceNotReady, message: "later".into() };
    t.producer.on_register_failed(0, generation, busy, t.now);
    assert!(t.actions().is_empty());
    t.advance(Duration::from_secs(1));
    let generation = Test::connects(&t.actions())[0].1;
    // Past it: the messages fail.
    t.advance(Duration::from_secs(30));
    t.producer.on_register_failed(0, generation, RegisterError::Unavailable("down".into()), t.now);
    let done = Test::done(&t.actions());
    assert!(matches!(&done[..], [(send, Err(SendError::Unavailable(_)))] if *send == retried));
    let late = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    let busy = RegisterError::Server { error: ServerError::ServiceNotReady, message: "later".into() };
    t.producer.on_register_failed(0, generation, busy, t.now);
    let done = Test::done(&t.actions());
    assert!(matches!(&done[..], [(send, Err(SendError::Unavailable(_)))] if *send == late));
    // A terminated topic: gone, and the message waits.
    let waits = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
    t.producer.on_register_failed(0, generation, gone, t.now);
    assert!(Test::done(&t.actions()).is_empty());
    assert_eq!(t.producer.outstanding(), 1);
    assert_eq!(t.producer.oldest_outstanding(), Some(waits));
}

/// Credentials refused as a registered segment's producer registers again, after a lost connection: retried, as the
/// connection was, and its messages wait for a provider with fresh ones. A first registration fails with them.
#[test]
fn refused_credentials_are_retried_once_registered() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.receipt(0, generation, 0, Some(1));
    t.producer.on_disconnected(0, generation, t.now);
    t.actions();
    let waiting = t.send(keyless());
    t.advance(Duration::from_secs(1));
    let generation = Test::connects(&t.actions())[0].1;
    let refused = RegisterError::Server { error: ServerError::AuthenticationError, message: "expired".into() };
    t.producer.on_register_failed(0, generation, refused.clone(), t.now);
    assert!(Test::done(&t.actions()).is_empty(), "the message waits");
    t.advance(Duration::from_secs(1));
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 8);
    assert_eq!(Test::sent(&t.actions()), [(0, 1)]);
    t.receipt(0, generation, 1, Some(2));
    assert!(matches!(&Test::done(&t.actions())[..], [(send, Ok(_))] if *send == waiting));

    // A first registration fails with them.
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let refused_send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.producer.on_register_failed(0, generation, refused, t.now);
    let done = Test::done(&t.actions());
    assert!(
        matches!(&done[..], [(send, Err(SendError::Server { error: ServerError::AuthenticationError, .. }))] if *send == refused_send)
    );
}

#[test]
fn a_message_is_parked_at_most_ten_times() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let mut segment = 0;
    for epoch in 2..=12 {
        let generation = Test::connects(&t.actions()).last().copied().unwrap().1;
        let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
        t.producer.on_register_failed(segment, generation, gone, t.now);
        if epoch == 12 {
            break;
        }
        // Each time, a layout seals it and the message moves to the successor, which is gone too.
        segment += 1;
        let segments: Vec<_> =
            (0..segment).map(|id| (id, 0, 0xffff, false)).chain([(segment, 0, 0xffff, true)]).collect();
        t.producer.on_layout(layout(epoch, &segments), t.now);
    }
    let done = Test::done(&t.actions());
    assert!(
        matches!(&done[..], [(s, Err(SendError::Server { error: ServerError::TopicTerminatedError, .. }))] if *s == send),
        "{done:?}"
    );
}

#[test]
fn a_layout_drops_a_ready_segment_once_its_producer_is_closed() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.actions();
    // Sealed with the message written and no receipt yet: close the producer, and wait for its answer.
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    // The answer: what wasn't stored goes to the successor.
    t.producer.on_disconnected(0, generation, t.now);
    let actions = t.actions();
    let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
    t.register(1, next, 8);
    t.actions();
    t.receipt(1, next, 0, Some(3));
    let done = Test::done(&t.actions());
    assert_eq!(done[0].0, send);
    assert_eq!(done[0].1.as_ref().unwrap().segment_id, 1);
}

/// A layout that drops a ready segment parks what its open batches hold at once, since it was never written, and
/// its entries with the close's answer. All of them go to the successor, in call order.
#[test]
fn a_layout_drops_a_ready_segment_with_open_batches() {
    let batching = Batching { max_messages: 2, max_bytes: 0, max_delay: Duration::from_millis(10) };
    let options = ProducerOptions { batching: Some(batching), ..options(ProducerAccessMode::Shared) };
    let now = Instant::now();
    let mut t = Test { producer: Producer::new(options, one_segment(), now), now };
    let sends = [t.send(keyless()), t.send(keyless())];
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    assert_eq!(Test::sent(&t.actions()), [(0, 1)]);
    let open = t.send(keyless());
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    assert_eq!(t.producer.sends_parked_or_held(), 1);
    // The close's answer: the entry wasn't stored.
    t.producer.on_disconnected(0, generation, t.now);
    assert_eq!(t.producer.sends_parked_or_held(), 3);
    let actions = t.actions();
    let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
    t.register(1, next, 8);
    assert_eq!(Test::sent(&t.actions()), [(1, 1)]);
    t.receipt(1, next, 1, Some(3));
    t.advance(Duration::from_millis(10));
    let mut actions = t.actions();
    assert_eq!(Test::sent(&actions), [(1, 2)]);
    t.receipt(1, next, 2, Some(4));
    actions.extend(t.actions());
    let done: Vec<(SendId, i64)> =
        Test::done(&actions).into_iter().map(|(send, result)| (send, result.unwrap().segment_id)).collect();
    assert_eq!(done, [(sends[0], 1), (sends[1], 1), (open, 1)]);
}

/// A flush covers the messages still waiting for their segment's registration: they go out together once it is
/// registered, without waiting out the delay.
#[test]
fn a_flush_covers_messages_waiting_for_a_registration() {
    let batching = Batching { max_messages: 0, max_bytes: 0, max_delay: Duration::from_secs(60) };
    let options = ProducerOptions { batching: Some(batching), ..options(ProducerAccessMode::Shared) };
    let mut t = Test { producer: Producer::new(options, one_segment(), Instant::now()), now: Instant::now() };
    t.send(keyless());
    t.send(keyless());
    t.producer.flush();
    let generation = Test::connects(&t.actions())[0].1;
    let later = t.send(keyless());
    t.register(0, generation, 7);
    // The flushed two and the later one, which joined before the batches closed, in one entry.
    assert_eq!(Test::sent(&t.actions()), [(0, 2)]);
    t.receipt(0, generation, 2, Some(3));
    let done = Test::done(&t.actions());
    assert_eq!(done.len(), 3);
    assert_eq!(done[2].0, later);
}

/// A message routed to a segment while its producer reconnects waits there with its clock running, and times out
/// before it gets into a batch.
#[test]
fn a_message_waiting_for_a_reconnect_times_out() {
    let options =
        ProducerOptions { send_timeout: Some(Duration::from_millis(100)), ..options(ProducerAccessMode::Shared) };
    let mut t = Test { producer: Producer::new(options, one_segment(), Instant::now()), now: Instant::now() };
    t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.receipt(0, generation, 0, Some(1));
    t.producer.on_disconnected(0, generation, t.now);
    t.actions();
    let late = t.send(keyless());
    t.advance(Duration::from_millis(100));
    assert_eq!(Test::done(&t.actions()), [(late, Err(SendError::Timeout))]);
}

#[test]
fn a_dropped_segment_that_disconnects_parks_its_messages() {
    let mut t = Test::new(ProducerAccessMode::Shared, layout(1, &[(0, 0, 0x7fff, true), (1, 0x8000, 0xffff, true)]));
    let key = key_in(0, 0x7fff);
    let send = t.send(keyed(&key));
    let generation = Test::connects(&t.actions())[0].1;
    // Still registering when the layout drops it: its message moves at once.
    t.producer.on_layout(layout(2, &[(0, 0, 0x7fff, false), (1, 0x8000, 0xffff, true), (2, 0, 0x7fff, true)]), t.now);
    let actions = t.actions();
    assert_eq!(Test::connects(&actions).iter().map(|c| c.0).collect::<Vec<_>>(), [2]);
    // The late registration is closed unused.
    t.register(0, generation, 7);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    t.producer.on_producer_closed(0, generation, t.now);
    assert!(t.actions().is_empty());
    assert_eq!(t.producer.oldest_outstanding(), Some(send));
}

#[test]
fn close_closes_registrations_that_complete_late() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.producer.close(t.now);
    let actions = t.actions();
    assert!(actions.contains(&Action::Closed), "a registration still running doesn't hold close up");
    assert!(matches!(&Test::done(&actions)[..], [(s, Err(SendError::AlreadyClosed))] if *s == send));
    t.register(0, generation, 7);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    t.producer.on_producer_closed(0, generation, t.now);
    assert!(t.actions().is_empty());
    // Closing again does nothing.
    t.producer.close(t.now);
    assert!(t.actions().is_empty());
}

#[test]
fn close_while_creating_fails_create() {
    let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
    t.actions();
    t.producer.close(t.now);
    let actions = t.actions();
    assert!(actions.contains(&Action::Created(Err(SendError::AlreadyClosed))));
    assert!(actions.contains(&Action::Closed));
}

#[test]
fn exclusive_producers_claim_new_segments_from_a_layout() {
    let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    assert_eq!(t.actions(), [Action::Created(Ok(()))]);
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0x7fff, true), (2, 0x8000, 0xffff, true)]), t.now);
    let actions = t.actions();
    let mut claimed: Vec<u64> = Test::connects(&actions).iter().map(|c| c.0).collect();
    claimed.sort();
    assert_eq!(claimed, [1, 2]);
    // A stale or equal layout changes nothing.
    t.producer.on_layout(one_segment(), t.now);
    assert!(t.actions().is_empty());
}

#[test]
fn an_idle_segment_reconnects_only_when_needed() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.actions();
    t.receipt(0, generation, 0, Some(1));
    t.actions();
    t.producer.on_disconnected(0, generation, t.now);
    // Nothing to send when the backoff ends: no new registration until something is.
    t.advance(Duration::from_secs(1));
    assert!(Test::connects(&t.actions()).is_empty());
    t.send(keyless());
    assert_eq!(Test::connects(&t.actions()).len(), 1);
}

#[test]
fn a_key_no_segment_covers_fails_its_send() {
    let mut t = Test::new(ProducerAccessMode::Shared, layout(1, &[(0, 0, 0x7fff, true)]));
    let key = key_in(0x8000, 0xffff);
    let send = t.send(keyed(&key));
    let done = Test::done(&t.actions());
    assert!(matches!(&done[..], [(s, Err(SendError::Routing(LayoutError::Uncovered(_))))] if *s == send));
}

#[test]
fn an_exclusive_create_follows_a_split_that_lands_meanwhile() {
    let mut t = Test::new(ProducerAccessMode::Exclusive, one_segment());
    let generation = Test::connects(&t.actions())[0].1;
    // The segment splits before the claim is answered: create waits for its children instead.
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0x7fff, true), (2, 0x8000, 0xffff, true)]), t.now);
    let actions = t.actions();
    let children = Test::connects(&actions);
    assert_eq!(children.iter().map(|c| c.0).collect::<Vec<_>>(), [1, 2]);
    // The sealed segment's claim fails: it is gone, and doesn't fail the create.
    let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
    t.producer.on_register_failed(0, generation, gone, t.now);
    assert!(t.actions().is_empty());
    // The gone segment had left the layout already: a keyed send goes to its child, written once registered.
    let send = t.send(keyed("k"));
    t.register(1, children[0].1, 8);
    assert!(t.actions().is_empty(), "still waiting for the other child");
    t.register(2, children[1].1, 9);
    let actions = t.actions();
    assert!(actions.contains(&Action::Created(Ok(()))), "{actions:?}");
    assert_eq!(Test::sent(&actions).len(), 1);
    assert_eq!(t.producer.oldest_outstanding(), Some(send));
}

#[test]
fn a_retirement_check_ends_when_the_layout_moves_on() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
    t.producer.on_register_failed(0, generation, gone, t.now);
    assert!(t.producer.poll_timeout().is_some());
    // The layout drops the segment before the check is due: the message moves on, and the check is void.
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
    assert_eq!(Test::connects(&t.actions()).iter().map(|c| c.0).collect::<Vec<_>>(), [1]);
    t.advance(Duration::from_secs(1));
    assert!(Test::done(&t.actions()).is_empty());
}

#[test]
fn events_from_an_older_registration_are_ignored() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    // Not the registration in progress.
    t.register(0, generation + 7, 1);
    t.producer.on_producer_closed(0, generation, t.now);
    t.producer.on_disconnected(0, generation + 7, t.now);
    assert!(t.actions().is_empty());
    t.register(0, generation, 1);
    assert_eq!(Test::sent(&t.actions()), [(0, 0)]);
    // And for segments it doesn't have.
    t.register(9, 0, 2);
    t.producer.on_register_failed(9, 0, RegisterError::Unavailable("x".into()), t.now);
    t.receipt(9, 0, 0, None);
    t.send_error(9, 0, 0, ServerError::NotAllowedError);
    t.producer.on_disconnected(9, 0, t.now);
    t.producer.on_producer_closed(9, 0, t.now);
    assert!(t.actions().is_empty());
    assert!(t.producer.register_command(9, 1).is_none());
}

/// The broker answers a close once it has written what it stored: a message it sent no receipt for wasn't stored,
/// and fails.
#[test]
fn close_fails_what_the_broker_did_not_store() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.actions();
    t.producer.close(t.now);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    t.producer.on_producer_closed(0, generation, t.now);
    let actions = t.actions();
    assert!(matches!(&Test::done(&actions)[..], [(s, Err(SendError::AlreadyClosed))] if *s == send));
    assert!(actions.contains(&Action::Closed));
}

/// Once a gone segment's retirement runs out, its messages fail; a new message routed to it waits again, for a
/// new round of checks.
#[test]
fn a_gone_segment_retires_again_for_new_messages() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let first = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
    t.producer.on_register_failed(0, generation, gone, t.now);
    // Ten checks, then the message fails.
    for _ in 0..10 {
        t.advance(Duration::from_millis(500));
    }
    let done = Test::done(&t.actions());
    assert!(
        matches!(&done[..], [(s, Err(SendError::Server { error: ServerError::TopicTerminatedError, .. }))] if *s == first)
    );
    assert!(t.producer.poll_timeout().is_none());
    // No new producer for the gone segment: the next message waits for a layout, with checks again.
    let second = t.send(keyless());
    assert!(Test::connects(&t.actions()).is_empty());
    assert!(t.producer.poll_timeout().is_some());
    assert_eq!(t.producer.oldest_outstanding(), Some(second));
}

/// A segment the layout dropped is closing; if its topic turns out terminated meanwhile, it is gone, retiring
/// already, and its messages still move on.
#[test]
fn a_closing_segment_that_turns_out_terminated() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    t.actions();
    t.producer.on_layout(layout(2, &[(0, 0, 0xffff, false), (1, 0, 0xffff, true)]), t.now);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
    t.send_error(0, generation, 0, ServerError::TopicTerminatedError);
    let actions = t.actions();
    let [(1, next)] = Test::connects(&actions)[..] else { panic!("{actions:?}") };
    t.register(1, next, 8);
    t.actions();
    t.receipt(1, next, 0, Some(1));
    assert_eq!(Test::done(&t.actions())[0].0, send);
}

/// A key a layout no longer routes, while a segment retires, waits rather than risk its order.
#[test]
fn a_key_with_no_route_in_a_retiring_layout_is_held() {
    let mut t = Test::new(ProducerAccessMode::Shared, layout(1, &[(0, 0, 0x7fff, true), (1, 0x8000, 0xffff, true)]));
    let low = key_in(0, 0x7fff);
    t.send(keyed(&low));
    let generation = Test::connects(&t.actions())[0].1;
    let gone = RegisterError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
    t.producer.on_register_failed(0, generation, gone, t.now);
    // Segment 0 retires with the layout it was active in; a key of segment 1 isn't held, a key of 0 is.
    let high = key_in(0x8000, 0xffff);
    t.send(keyed(&high));
    t.send(keyed(&low));
    assert_eq!(t.producer.sends_parked_or_held(), 2);
}

/// A close before a segment's producer is registered fails the messages waiting for it, none of them written, and
/// doesn't wait for the registration: one that completes anyway is closed unused.
#[test]
fn close_before_registration_fails_what_waits_for_it() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.producer.close(t.now);
    let actions = t.actions();
    assert!(matches!(&Test::done(&actions)[..], [(s, Err(SendError::AlreadyClosed))] if *s == send), "{actions:?}");
    assert_eq!(actions.last(), Some(&Action::Closed));
    t.register(0, generation, 7);
    assert_eq!(t.actions(), [Action::CloseProducer { segment: 0, generation }]);
}

/// Messages carry the schema version the broker gave their segment's producer; an empty one is none.
#[test]
fn messages_carry_their_producers_schema_version() {
    for (given, carried) in [(Some(vec![0, 0, 0, 1]), Some(vec![0, 0, 0, 1])), (Some(vec![]), None), (None, None)] {
        let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
        t.send(keyless());
        let generation = Test::connects(&t.actions())[0].1;
        let success =
            CommandProducerSuccess { producer_name: "p-seg-0".into(), schema_version: given, ..Default::default() };
        t.producer.on_registered(0, generation, 7, &success, usize::MAX, t.now);
        let actions = t.actions();
        let [Action::Send { metadata, .. }] = &actions[..] else { panic!("{actions:?}") };
        assert_eq!(metadata.schema_version, carried);
    }
}

/// A close while a segment's producer reconnects fails the messages waiting to be written again: the broker may
/// or may not have stored them, and nothing will tell.
#[test]
fn close_while_reconnecting_fails_what_waits_to_be_written_again() {
    let mut t = Test::new(ProducerAccessMode::Shared, one_segment());
    let send = t.send(keyless());
    let generation = Test::connects(&t.actions())[0].1;
    t.register(0, generation, 7);
    assert_eq!(Test::sent(&t.actions()), [(0, 0)]);
    t.producer.on_disconnected(0, generation, t.now);
    t.producer.close(t.now);
    let actions = t.actions();
    assert!(matches!(&Test::done(&actions)[..], [(s, Err(SendError::AlreadyClosed))] if *s == send), "{actions:?}");
    assert_eq!(actions.last(), Some(&Action::Closed));
}
