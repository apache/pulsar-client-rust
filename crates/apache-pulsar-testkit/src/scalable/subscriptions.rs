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

//! The subscription types besides Shared, as the 5.0.1 broker's dispatchers serve them.
//!
//! - **Exclusive:** one consumer at a time; another's subscribe fails with `ConsumerBusy`. Only an Exclusive
//!   subscription takes cumulative acks: the broker ignores them on the others.
//! - **`Key_Shared` by entry bucket** (*key-shared* §4): every consumer declares the segment's bucket boundaries,
//!   which the first one's fix for the subscription, and one declaring others fails with `ConsumerAssignError`. The
//!   buckets are spread over the consumers sorted by name, then id, consumer `j` of `k` owning the slice
//!   `[j·N/k, (j+1)·N/k)`, as the broker's `EntryBucketConsumerSelector` does. An entry goes whole to the owner of its
//!   bucket: by its stamp, `entry_hash_min`, else by its key's bucket hash, else bucket 0. A bucket that moved is
//!   held back from its new owner while another consumer still has entries of it to acknowledge, and the entries
//!   after a held one in its bucket wait with it, which keeps each key in order.
//! - A consumer of another type than the subscription's consumers fails with `ConsumerBusy`, as the broker's
//!   "subscription is of different type" does. Failover, and `Key_Shared` by key, are not simulated.
//!
//! [`World::rewind`] moves a subscription back to the start of a segment, as a reset of its cursor does.

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::proto::{CommandAck, CommandSubscribe};

use super::*;

/// A subscription's type.
#[derive(Debug, Clone, Default, PartialEq)]
pub(super) enum Kind {
    #[default]
    Shared,
    Exclusive,
    /// `Key_Shared` by entry bucket, with the buckets' inclusive hash ranges.
    Buckets(Vec<(u32, u32)>),
}

impl World {
    /// Why the broker refuses a consumer on a segment, if it does: it is of a type the world doesn't simulate, or that
    /// the subscription's consumers don't let in.
    pub(super) fn refusal(&self, topic: &str, segment: u64, subscribe: &CommandSubscribe) -> Option<ServerError> {
        let kind = match kind_of(subscribe) {
            Ok(kind) => kind,
            Err(error) => return Some(error),
        };
        let subscription = self.topics[topic].segments[&segment].subscriptions.get(&subscribe.subscription)?;
        match (&subscription.kind, &kind) {
            _ if subscription.consumers.is_empty() => None,
            (Kind::Buckets(fixed), Kind::Buckets(declared)) if fixed != declared => {
                Some(ServerError::ConsumerAssignError)
            }
            (Kind::Shared, Kind::Shared) | (Kind::Buckets(_), Kind::Buckets(_)) => None,
            _ => Some(ServerError::ConsumerBusy),
        }
    }

    /// Moves a subscription back to the start of a segment, as resetting its cursor does (`scalable-topics seek`
    /// resets it on every segment): the broker closes the subscription's consumers there with `CommandCloseConsumer`,
    /// and delivers everything again once they come back.
    pub fn rewind(&mut self, topic: &str, segment: u64, subscription: &str) {
        for (conn, consumer_id) in self.consumers_on(topic, segment) {
            let place = self.conn(conn).consumers[&consumer_id].clone();
            if place.2 == subscription {
                self.conn(conn).consumers.remove(&consumer_id);
                self.remove_consumer(conn, consumer_id, &place);
                self.push(conn, consumer_closed(consumer_id));
            }
        }
        let seg = self.topic_mut(topic).segments.get_mut(&segment).expect("no such segment");
        let all: BTreeMap<u64, BTreeSet<u32>> = seg.entries.iter().map(|e| (e.entry_id, batch_indexes(e))).collect();
        let sub = seg.subscriptions.get_mut(subscription).expect("no such subscription");
        (sub.unacked, sub.read) = (all, 0);
        sub.redeliver.clear();
        self.dispatch(topic, segment);
    }

    /// A cumulative ack: everything up to its position, on an Exclusive subscription.
    pub(super) fn ack_cumulative(&mut self, conn: ConnId, ack: &CommandAck) {
        let Some((topic, segment, name)) = self.consumer(conn, ack.consumer_id) else { return };
        let ledger = self.topic(&topic).segments[&segment].ledger_id;
        let sub = self.subscription(&topic, segment, &name);
        let exclusive = sub.kind == Kind::Exclusive;
        let Some(id) = ack.message_id.first().filter(|id| exclusive && id.ledgerId == ledger) else { return };
        let before: Vec<u64> = sub.unacked.range(..id.entryId).map(|(entry, _)| *entry).collect();
        for entry in before {
            sub.ack(entry, None);
        }
        sub.ack(id.entryId, (!id.ack_set.is_empty()).then_some(&id.ack_set[..]));
        self.dispatch(&topic, segment);
    }
}

impl Subscription {
    /// A consumer joins: the first one sets the subscription's type.
    pub(super) fn join(&mut self, subscribe: &CommandSubscribe) {
        if self.consumers.is_empty() {
            self.kind = kind_of(subscribe).expect("a consumer the subscription let in");
        }
    }

    /// The next entry to dispatch and the consumer it goes to: by bucket on a `Key_Shared` subscription, and as on a
    /// Shared one otherwise, an Exclusive subscription having a single consumer.
    pub(super) fn next(&mut self, entries: &[Stored]) -> Option<(usize, u64)> {
        let Kind::Buckets(buckets) = &self.kind else { return self.next_delivery() };
        let (at, entry) = self.next_by_bucket(buckets, entries)?;
        // The entries read past, held back, go first next time, as in the broker's replay set.
        let skipped: Vec<u64> = self.unacked.range(self.read..entry.max(self.read)).map(|(entry, _)| *entry).collect();
        self.redeliver.extend(skipped);
        Some((at, entry))
    }

    /// The first entry, the ones to redeliver first, whose bucket's owner has permits and no other consumer still has
    /// entries of the bucket to acknowledge. A bucket held back holds back its later entries too.
    fn next_by_bucket(&self, buckets: &[(u32, u32)], entries: &[Stored]) -> Option<(usize, u64)> {
        let mut order: Vec<usize> = (0..self.consumers.len()).collect();
        order.sort_by_key(|&at| (&self.consumers[at].name, self.consumers[at].id));
        let (n, k) = (buckets.len(), order.len());
        let mut owners = vec![None; n];
        for (j, &at) in order.iter().enumerate() {
            owners[j * n / k..(j + 1) * n / k].fill(Some(at));
        }
        let bucket = |entry: u64| bucket_of(buckets, entry_hash(&entries[entry as usize]));
        let mut held = BTreeSet::new();
        let unread = self.unacked.range(self.read..).map(|(entry, _)| *entry);
        for entry in self.redeliver.iter().copied().chain(unread) {
            let b = bucket(entry);
            if held.contains(&b) {
                continue;
            }
            let draining = |at: usize| {
                let others = self.consumers.iter().enumerate().filter(|(other, _)| *other != at);
                others.flat_map(|(_, c)| &c.pending).any(|&pending| bucket(pending) == b)
            };
            match owners[b] {
                Some(at) if self.consumers[at].permits > 0 && !draining(at) => return Some((at, entry)),
                _ => held.insert(b),
            };
        }
        None
    }
}

/// The type a consumer subscribes with, or why the world refuses it.
fn kind_of(subscribe: &CommandSubscribe) -> Result<Kind, ServerError> {
    match subscribe.subType {
        SubType::Shared => Ok(Kind::Shared),
        SubType::Exclusive => Ok(Kind::Exclusive),
        // Key_Shared by entry bucket; Failover and Key_Shared by key are not simulated.
        _ => {
            let meta = subscribe.keySharedMeta.as_option();
            let by_bucket =
                meta.filter(|meta| subscribe.subType == SubType::Key_Shared && meta.entryBucketDispatch == Some(true));
            let meta = by_bucket.ok_or(ServerError::NotAllowedError)?;
            let ranges: Vec<(u32, u32)> = meta.hashRanges.iter().map(|r| (r.start as u32, r.end as u32)).collect();
            if tiles_the_ring(&ranges) { Ok(Kind::Buckets(ranges)) } else { Err(ServerError::ConsumerAssignError) }
        }
    }
}

/// Whether ranges are ascending, contiguous and inclusive, from 0 to 0xffff, as the broker requires.
fn tiles_the_ring(ranges: &[(u32, u32)]) -> bool {
    let mut next = 0;
    for &(start, end) in ranges {
        if start != next || end < start {
            return false;
        }
        next = end + 1;
    }
    next == 0x1_0000
}

/// The bucket hash an entry is dispatched by: its stamp, else its first message's key's, else 0.
fn entry_hash(stored: &Stored) -> u32 {
    match (stored.metadata.entry_hash_min, &stored.key) {
        (Some(stamp), _) => stamp,
        (None, Some(key)) => u32::from(KeyHash::of(key.as_bytes()).bucket_hash()),
        (None, None) => 0,
    }
}

/// The bucket whose range holds `hash`.
fn bucket_of(buckets: &[(u32, u32)], hash: u32) -> usize {
    buckets.iter().position(|&(start, end)| (start..=end).contains(&hash)).expect("buckets tile the ring")
}
