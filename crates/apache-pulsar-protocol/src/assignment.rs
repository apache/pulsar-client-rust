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

//! The assignment session with a topic's controller, sans IO (docs/6-scalable-topics.md §5.1, *client-behavior* §8):
//! the registration of a Stream or grouped Checkpoint consumer, and the assignments the controller hands it.
//!
//! An [`AssignmentSession`] registers with `CommandScalableTopicSubscribe` under the consumer's name: the controller
//! knows a member by its name, across reconnects. The driver reports how the registration went, the assignments the
//! controller pushes, and the loss of the connection; the session hands back each assignment to apply, one whose
//! layout epoch is at least the last one applied (*wire-protocol* §11). As in Java's `ScalableConsumerClient`:
//!
//! - A failure of the first registration fails the consumer's subscribe.
//! - After a connection loss it registers again, with a backoff from 100 ms doubling to 30 s. Within the controller's
//!   grace period the same assignment comes back; after it, a new one.
//! - Closing sends `CommandScalableTopicUnsubscribe`, once a registration in flight settles, so that the controller
//!   rebalances at once. Its outcome doesn't matter: the grace period is the fallback.

use std::collections::{BTreeMap, VecDeque};
use std::time::{Duration, Instant};

use crate::backoff::{Backoff, Jitter};
use crate::producer::RegisterError;
use crate::proto::base_command::Type;
use crate::proto::{
    BaseCommand, CommandScalableTopicSubscribe, CommandScalableTopicUnsubscribe, ScalableConsumerAssignment,
    ScalableConsumerType,
};

/// Java's `ScalableConsumerClient` registers again from 100 ms up to 30 s.
const RETRY_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// What the session asks of its driver.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum Action {
    /// Register with the topic's controller: find it, connect, register a consumer id for the assignments it pushes,
    /// and send [`AssignmentSession::register_command`]. Report with `on_registered` or `on_register_failed`, the
    /// assignments pushed after with `on_assignment`, and the loss of the connection with `on_lost`.
    Register { generation: u64 },
    /// Send [`AssignmentSession::unregister_command`] on the connection of the registration of `generation`, then
    /// report with `on_unregistered`, however it went.
    Unregister { generation: u64 },
}

/// A segment the controller assigned.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct AssignedSegment {
    /// The topic to attach to: the `segment://` one, or the `persistent://` one that a legacy segment wraps.
    pub topic: String,
    /// The segment's entry-bucket boundaries, when the controller shares the segment between members; none when the
    /// consumer owns it whole (*key-shared* §4).
    pub bucket_ranges: Vec<(i32, i32)>,
}

/// The segments assigned, by id.
pub type Assignment = BTreeMap<u64, AssignedSegment>;

/// What a report means for the consumer.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Outcome {
    /// Nothing changes.
    Nothing,
    /// Apply this assignment.
    Apply(Assignment),
    /// The first registration failed: so does the subscribe.
    Failed(RegisterError),
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum State {
    /// Not registered, and waiting for the backoff before registering again.
    Idle {
        retry_at: Instant,
    },
    Registering {
        generation: u64,
    },
    Registered {
        generation: u64,
    },
    /// Closed while registering: it unregisters once the registration settles.
    Abandoned {
        generation: u64,
    },
    /// Our unsubscribe is out.
    Unregistering {
        generation: u64,
    },
    Done,
}

/// See the module documentation.
#[derive(Debug)]
pub struct AssignmentSession {
    kind: ScalableConsumerType,
    topic: String,
    subscription: String,
    name: String,
    state: State,
    generation: u64,
    registered_once: bool,
    backoff: Backoff,
    jitter: Jitter,
    /// The layout epoch of the assignment last applied.
    epoch: Option<u64>,
    actions: VecDeque<Action>,
}

impl AssignmentSession {
    /// A session of a `kind` consumer of `topic`, the name the broker resolved, on `subscription`, registering now as
    /// `name`.
    pub fn new(kind: ScalableConsumerType, topic: String, subscription: String, name: String, now: Instant) -> Self {
        AssignmentSession {
            kind,
            topic,
            subscription,
            name,
            state: State::Registering { generation: 1 },
            generation: 1,
            registered_once: false,
            backoff: Backoff::new(RETRY_BACKOFF.0, RETRY_BACKOFF.1),
            jitter: Jitter::new(now.elapsed().as_nanos() as u64),
            epoch: None,
            actions: VecDeque::from([Action::Register { generation: 1 }]),
        }
    }

    /// The name it registers under.
    pub fn name(&self) -> &str {
        &self.name
    }

    /// Whether the controller has the consumer registered now.
    pub fn is_registered(&self) -> bool {
        matches!(self.state, State::Registered { .. })
    }

    /// Whether it is closed, and nothing of it is left on a connection.
    pub fn is_done(&self) -> bool {
        self.state == State::Done
    }

    /// The `CommandScalableTopicSubscribe` to register with, as `consumer_id`. The driver sets its request id.
    pub fn register_command(&self, consumer_id: u64) -> BaseCommand {
        let subscribe = CommandScalableTopicSubscribe {
            topic: self.topic.clone(),
            subscription: self.subscription.clone(),
            consumer_name: self.name.clone(),
            consumer_id,
            consumer_type: self.kind,
            ..Default::default()
        };
        BaseCommand {
            r#type: Type::SCALABLE_TOPIC_SUBSCRIBE.into(),
            scalableTopicSubscribe: subscribe.into(),
            ..Default::default()
        }
    }

    /// The `CommandScalableTopicUnsubscribe` that ends the registration of `consumer_id`. The driver sets its request
    /// id.
    pub fn unregister_command(consumer_id: u64) -> BaseCommand {
        let unsubscribe = CommandScalableTopicUnsubscribe { consumer_id, ..Default::default() };
        BaseCommand {
            r#type: Type::SCALABLE_TOPIC_UNSUBSCRIBE.into(),
            scalableTopicUnsubscribe: unsubscribe.into(),
            ..Default::default()
        }
    }

    /// The controller registered the consumer, for the registration of `generation`, with its assignment.
    pub fn on_registered(&mut self, generation: u64, assignment: &ScalableConsumerAssignment) -> Outcome {
        match self.state {
            State::Registering { generation: current } if current == generation => {
                self.state = State::Registered { generation };
                self.registered_once = true;
                self.backoff.reset();
                self.apply(assignment)
            }
            // Closed meanwhile: leave at once.
            State::Abandoned { generation: current } if current == generation => {
                self.state = State::Unregistering { generation };
                self.actions.push_back(Action::Unregister { generation });
                Outcome::Nothing
            }
            _ => Outcome::Nothing,
        }
    }

    /// The registration of `generation` failed. The first one fails the subscribe; the others are retried.
    pub fn on_register_failed(&mut self, generation: u64, error: RegisterError, now: Instant) -> Outcome {
        match self.state {
            State::Registering { generation: current } if current == generation => {
                if self.registered_once {
                    self.register_later(now);
                    return Outcome::Nothing;
                }
                self.state = State::Done;
                Outcome::Failed(error)
            }
            State::Abandoned { generation: current } if current == generation => {
                self.state = State::Done;
                Outcome::Nothing
            }
            _ => Outcome::Nothing,
        }
    }

    /// The controller pushed an assignment to the registration of `generation`.
    pub fn on_assignment(&mut self, generation: u64, assignment: &ScalableConsumerAssignment) -> Outcome {
        if self.state != (State::Registered { generation }) {
            return Outcome::Nothing;
        }
        self.apply(assignment)
    }

    /// The connection of the registration of `generation` closed: register again, after the backoff.
    pub fn on_lost(&mut self, generation: u64, now: Instant) {
        if self.state == (State::Registered { generation }) {
            self.register_later(now);
        }
    }

    /// The unsubscribe of the registration of `generation` was answered, or failed.
    pub fn on_unregistered(&mut self, generation: u64) {
        if self.state == (State::Unregistering { generation }) {
            self.state = State::Done;
        }
    }

    /// Leaves the group: unregisters, once a registration in flight settles. Can be called again.
    pub fn close(&mut self) {
        self.state = match self.state {
            State::Registered { generation } => {
                self.actions.push_back(Action::Unregister { generation });
                State::Unregistering { generation }
            }
            State::Registering { generation } => State::Abandoned { generation },
            State::Idle { .. } => State::Done,
            state => state,
        };
    }

    /// When the session next registers again.
    pub fn poll_timeout(&self) -> Option<Instant> {
        match self.state {
            State::Idle { retry_at } => Some(retry_at),
            _ => None,
        }
    }

    pub fn handle_timeout(&mut self, now: Instant) {
        if let State::Idle { retry_at } = self.state
            && retry_at <= now
        {
            self.generation += 1;
            self.state = State::Registering { generation: self.generation };
            self.actions.push_back(Action::Register { generation: self.generation });
        }
    }

    pub fn poll_action(&mut self) -> Option<Action> {
        self.actions.pop_front()
    }

    /// An assignment to apply, unless it is older than the last one.
    fn apply(&mut self, assignment: &ScalableConsumerAssignment) -> Outcome {
        if self.epoch.is_some_and(|epoch| assignment.layout_epoch < epoch) {
            return Outcome::Nothing;
        }
        self.epoch = Some(assignment.layout_epoch);
        let segments = assignment.segments.iter().map(|segment| {
            let ranges = segment.bucket_ranges.iter().map(|range| (range.start, range.end)).collect();
            (segment.segment_id, AssignedSegment { topic: segment.segment_topic.clone(), bucket_ranges: ranges })
        });
        Outcome::Apply(segments.collect())
    }

    fn register_later(&mut self, now: Instant) {
        let delay = self.backoff.next_delay(self.jitter.next());
        self.state = State::Idle { retry_at: now + delay };
    }
}
