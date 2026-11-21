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

//! The topics' controllers: the groups of Stream and Checkpoint consumers registered on a subscription, and the
//! segments assigned to each member, as the 5.0.1 broker's `SubscriptionCoordinator` computes them.
//!
//! - A member registers with `CommandScalableTopicSubscribe`, under its consumer name, which identifies it across
//!   reconnects. A new member rebalances the group: every connected member gets its assignment, the new one as the
//!   answer to its subscribe. One registering again under a name the group has keeps its assignment, and is the only
//!   one to get it.
//! - A member whose connection closes stays registered for the grace period, which a test ends with
//!   [`World::end_grace`]. `CommandScalableTopicUnsubscribe` removes a member at once, and rebalances the group.
//! - The assignment takes the segments of the layout, active and sealed, that are assignable, sorted by hash range
//!   then id, and the members sorted by name. A sealed segment drained for the subscription isn't assignable, nor is a
//!   segment with an ancestor not drained yet: that keeps each key's older messages first. While members don't
//!   outnumber segments, each segment goes whole to one of them, round robin. Past that, the surplus members share
//!   segments by entry bucket, one more on each segment in turn while it has a bucket to spare, and each member of a
//!   shared segment gets its bucket boundaries. A Checkpoint group never shares a segment, and has no ancestor order.
//! - A sealed segment is drained once the subscription has nothing left to acknowledge there. The broker polls for
//!   that, from 2 s on; the world sees it at once, and rebalances. A new layout rebalances too.
//! - A split's or a merge's children get the subscriptions of the groups with members, at their start.

use apache_pulsar_protocol::proto::{
    CommandScalableTopicAssignmentUpdate, CommandScalableTopicSubscribeResponse, IntRange, ScalableAssignedSegment,
    ScalableConsumerAssignment, ScalableConsumerType,
};

use super::*;

/// The controllers of every topic, and the knobs a test sets.
#[derive(Debug, Default)]
pub struct Controller {
    /// The `controller_broker_url` every layout carries.
    pub url: Option<String>,
    /// Errors to answer the next registrations with, one each.
    pub fail_registrations: VecDeque<ServerError>,
    /// While set, registrations wait unanswered until [`World::release_registrations`].
    pub hold_registrations: bool,
    held: Vec<(ConnId, BaseCommand)>,
    /// The groups, by topic and subscription.
    groups: BTreeMap<(String, String), Group>,
}

#[derive(Debug, Default)]
struct Group {
    checkpoint: bool,
    /// By name.
    members: BTreeMap<String, Member>,
    /// The sealed segments drained for the subscription.
    drained: BTreeSet<u64>,
}

#[derive(Debug)]
struct Member {
    /// Its connection and consumer id; none while in its grace period.
    link: Option<(ConnId, u64)>,
    /// The subscribe its next assignment answers.
    unanswered: Option<u64>,
}

impl World {
    /// Ends a disconnected member's grace period, as the broker evicts it: the group rebalances.
    pub fn end_grace(&mut self, topic: &str, subscription: &str, name: &str) {
        let key = (topic.to_string(), subscription.to_string());
        let group = self.controller.groups.get_mut(&key).expect("no such group");
        assert!(group.members.get(name).is_some_and(|m| m.link.is_none()), "{name} is not in its grace period");
        group.members.remove(name);
        self.push_assignments(&key, None);
    }

    /// Answers the registrations held while [`Controller::hold_registrations`] was set, in order.
    pub fn release_registrations(&mut self) {
        self.controller.hold_registrations = false;
        for (conn, command) in std::mem::take(&mut self.controller.held) {
            self.register(conn, &command);
        }
    }

    /// Gives a segment `buckets` entry buckets of equal width, as the broker's `EntryBucketSplits.equalWidth`.
    pub fn set_buckets(&mut self, topic: &str, segment: u64, buckets: u32) {
        let info = &mut self.topic_mut(topic).segments.get_mut(&segment).expect("no such segment").info;
        info.entry_bucket_splits = (1..buckets).map(|i| i * 0x1_0000 / buckets).collect();
    }

    /// The segments assigned to a member now, each with whether it is shared.
    pub fn assigned(&self, topic: &str, subscription: &str, name: &str) -> Vec<(u64, bool)> {
        let key = (topic.to_string(), subscription.to_string());
        let assignment = self.assignment(&key);
        let segments = assignment.get(name).map(Vec::as_slice).unwrap_or_default();
        segments.iter().map(|s| (s.segment_id, !s.bucket_ranges.is_empty())).collect()
    }

    /// A `CommandScalableTopicSubscribe`: registers a member, or one coming back.
    pub(super) fn register(&mut self, conn: ConnId, command: &BaseCommand) {
        if self.controller.hold_registrations {
            return self.controller.held.push((conn, command.clone()));
        }
        let subscribe = &command.scalableTopicSubscribe;
        let topic = self.find_topic(&subscribe.topic);
        let refusal = match topic {
            Some(_) => self.controller.fail_registrations.pop_front(),
            None => Some(ServerError::TopicNotFound),
        };
        if let Some(error) = refusal {
            let response = CommandScalableTopicSubscribeResponse {
                request_id: subscribe.request_id,
                error: Some(error),
                message: Some(format!("the controller refused the registration: {error:?}")),
                ..Default::default()
            };
            return self.push(conn, subscribe_response(response));
        }
        let key = (topic.expect("a topic the world has"), subscribe.subscription.clone());
        let group = self.controller.groups.entry(key.clone()).or_default();
        group.checkpoint |= subscribe.consumer_type == ScalableConsumerType::CHECKPOINT;
        let member = Member { link: Some((conn, subscribe.consumer_id)), unanswered: Some(subscribe.request_id) };
        let back = group.members.insert(subscribe.consumer_name.clone(), member).is_some();
        // One coming back keeps its assignment, and alone gets it.
        self.push_assignments(&key, back.then_some(subscribe.consumer_name.as_str()));
    }

    /// A `CommandScalableTopicUnsubscribe`: the member of that consumer id on the connection leaves, if it is there.
    pub(super) fn unregister(&mut self, conn: ConnId, command: &BaseCommand) {
        let unsubscribe = &command.scalableTopicUnsubscribe;
        let link = Some((conn, unsubscribe.consumer_id));
        let left = self.controller.groups.iter_mut().find_map(|(key, group)| {
            let name = group.members.iter().find(|(_, m)| m.link == link).map(|(name, _)| name.clone())?;
            group.members.remove(&name);
            Some(key.clone())
        });
        self.push(conn, reply::success(unsubscribe.request_id));
        if let Some(key) = left {
            self.push_assignments(&key, None);
        }
    }

    /// A connection closed: its members wait out their grace period.
    pub(super) fn members_disconnected(&mut self, conn: ConnId) {
        for member in self.controller.groups.values_mut().flat_map(|group| group.members.values_mut()) {
            if member.link.is_some_and(|(c, _)| c == conn) {
                member.link = None;
            }
        }
    }

    /// Gives a split's or a merge's children the subscriptions of the topic's groups with members, at their start.
    pub(super) fn provision(&mut self, topic: &str, children: &[u64]) {
        let subscriptions: Vec<String> = self
            .controller
            .groups
            .iter()
            .filter(|((t, _), group)| t == topic && !group.members.is_empty())
            .map(|((_, subscription), _)| subscription.clone())
            .collect();
        let state = self.topic_mut(topic);
        for child in children {
            let segment = state.segments.get_mut(child).expect("a child");
            for subscription in &subscriptions {
                segment.subscriptions.insert(subscription.clone(), Subscription::default());
            }
        }
    }

    /// A new layout: every group of the topic rebalances.
    pub(super) fn rebalance(&mut self, topic: &str) {
        for key in self.groups_of(topic) {
            self.refresh_drains(&key);
            self.push_assignments(&key, None);
        }
    }

    /// After a dispatch: the groups that see a sealed segment drained for them rebalance.
    pub(super) fn check_drains(&mut self, topic: &str) {
        for key in self.groups_of(topic) {
            if self.refresh_drains(&key) {
                self.push_assignments(&key, None);
            }
        }
    }

    /// Takes note of the sealed segments drained for a group: those where its subscription has nothing left to
    /// acknowledge. Returns whether there are new ones.
    fn refresh_drains(&mut self, key: &(String, String)) -> bool {
        let drained: Vec<u64> = self.topics[&key.0]
            .segments
            .values()
            .filter(|s| !s.pruned && s.info.state == SegmentState::SEALED)
            .filter(|s| s.subscriptions.get(&key.1).is_some_and(|sub| sub.unacked.is_empty()))
            .map(|s| s.info.segment_id)
            .collect();
        let group = self.controller.groups.get_mut(key).expect("a group of the topic");
        let before = group.drained.len();
        group.drained.extend(drained);
        group.drained.len() > before
    }

    fn groups_of(&self, topic: &str) -> Vec<(String, String)> {
        self.controller.groups.keys().filter(|(t, _)| t == topic).cloned().collect()
    }

    /// Sends each connected member of a group its assignment, or only `only`.
    fn push_assignments(&mut self, key: &(String, String), only: Option<&str>) {
        let epoch = self.topics[&key.0].epoch;
        let assignment = self.assignment(key);
        let group = self.controller.groups.get_mut(key).expect("a group");
        let mut out = Vec::new();
        for (name, member) in group.members.iter_mut().filter(|(name, _)| only.is_none_or(|only| only == *name)) {
            let Some((conn, consumer_id)) = member.link else { continue };
            let segments = assignment.get(name).cloned().unwrap_or_default();
            let assignment = ScalableConsumerAssignment { layout_epoch: epoch, segments, ..Default::default() };
            let command = match member.unanswered.take() {
                Some(request_id) => subscribe_response(CommandScalableTopicSubscribeResponse {
                    request_id,
                    assignment: assignment.into(),
                    ..Default::default()
                }),
                None => BaseCommand {
                    r#type: Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE.into(),
                    scalableTopicAssignmentUpdate: CommandScalableTopicAssignmentUpdate {
                        consumer_id,
                        assignment: assignment.into(),
                        ..Default::default()
                    }
                    .into(),
                    ..Default::default()
                },
            };
            out.push((conn, command));
        }
        for (conn, command) in out {
            self.push(conn, command);
        }
    }

    /// Each member's segments, as the broker's `computeAssignment` gives them.
    fn assignment(&self, key: &(String, String)) -> BTreeMap<String, Vec<ScalableAssignedSegment>> {
        let group = &self.controller.groups[key];
        let state = &self.topics[&key.0];
        let layout: BTreeMap<u64, &Segment> =
            state.segments.iter().filter(|(_, s)| !s.pruned).map(|(id, s)| (*id, s)).collect();
        let mut segments: Vec<&Segment> =
            layout.values().copied().filter(|s| group.checkpoint || assignable(s, &layout, &group.drained)).collect();
        segments.sort_by_key(|s| (s.info.hash_start, s.info.hash_end, s.info.segment_id));
        let names: Vec<&String> = group.members.keys().collect();
        let assigned = |segment: &Segment, ranges: Vec<IntRange>| ScalableAssignedSegment {
            segment_id: segment.info.segment_id,
            hash_start: segment.info.hash_start,
            hash_end: segment.info.hash_end,
            segment_topic: segment.name.clone(),
            bucket_ranges: ranges,
            ..Default::default()
        };
        let mut out: BTreeMap<String, Vec<ScalableAssignedSegment>> = BTreeMap::new();
        if names.len() <= segments.len() || group.checkpoint {
            for (segment, name) in segments.iter().zip(names.iter().cycle()) {
                out.entry((*name).clone()).or_default().push(assigned(segment, Vec::new()));
            }
            return out;
        }
        // Segments first; the surplus members share the segments with buckets to spare, one more each in turn.
        let mut owners = vec![1; segments.len()];
        let mut surplus = names.len() - segments.len();
        while surplus > 0 && (0..segments.len()).any(|i| owners[i] < buckets(segments[i])) {
            for (i, segment) in segments.iter().enumerate() {
                if surplus > 0 && owners[i] < buckets(segment) {
                    owners[i] += 1;
                    surplus -= 1;
                }
            }
        }
        let mut members = names.into_iter();
        for (segment, count) in segments.iter().zip(owners) {
            let ranges = if count == 1 { Vec::new() } else { bucket_ranges(segment) };
            for name in members.by_ref().take(count) {
                out.entry(name.clone()).or_default().push(assigned(segment, ranges.clone()));
            }
        }
        out
    }
}

/// Whether a segment can be assigned: not a sealed one drained already, and every ancestor in the layout drained.
fn assignable(segment: &Segment, layout: &BTreeMap<u64, &Segment>, drained: &BTreeSet<u64>) -> bool {
    let sealed = segment.info.state == SegmentState::SEALED;
    if sealed && drained.contains(&segment.info.segment_id) {
        return false;
    }
    let mut ancestors: Vec<u64> = segment.info.parent_ids.clone();
    while let Some(id) = ancestors.pop() {
        // One missing from the layout was pruned, with its own ancestors.
        let Some(ancestor) = layout.get(&id) else { continue };
        if !drained.contains(&id) {
            return false;
        }
        ancestors.extend(&ancestor.info.parent_ids);
    }
    true
}

/// How many entry buckets a segment has.
fn buckets(segment: &Segment) -> usize {
    segment.info.entry_bucket_splits.len() + 1
}

/// A segment's bucket boundaries, as the broker's `EntryBucketSplits.ranges`.
fn bucket_ranges(segment: &Segment) -> Vec<IntRange> {
    let splits = &segment.info.entry_bucket_splits;
    let starts = std::iter::once(0).chain(splits.iter().map(|&s| s as i32));
    let ends = splits.iter().map(|&s| s as i32 - 1).chain(std::iter::once(0xffff));
    starts.zip(ends).map(|(start, end)| IntRange { start, end, ..Default::default() }).collect()
}

fn subscribe_response(response: CommandScalableTopicSubscribeResponse) -> BaseCommand {
    BaseCommand {
        r#type: Type::SCALABLE_TOPIC_SUBSCRIBE_RESPONSE.into(),
        scalableTopicSubscribeResponse: response.into(),
        ..Default::default()
    }
}
