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

//! A scalable topic's layout, and the producer's routing over it (docs/6-scalable-topics.md §2, §3.1).
//!
//! A [`Layout`] is an immutable snapshot of one `ScalableTopicDAG`. Routing follows *client-behavior* §2 and the
//! Java client's `SegmentRouter`: a key goes to the active segment whose hash range holds the high 16 bits of its
//! raw Murmur3 hash; a keyless message goes round-robin over the active segments, in hash-range order; and when
//! every active segment is legacy, a key goes to the segment whose id is its v4 partition.

use std::collections::BTreeMap;

use crate::hash::KeyHash;
use crate::proto::{ScalableTopicDAG, SegmentState};

/// One segment of a layout.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Segment {
    pub id: u64,
    /// The inclusive range of segment hashes it owns.
    pub hash_start: u16,
    pub hash_end: u16,
    pub active: bool,
    pub parents: Vec<u64>,
    pub children: Vec<u64>,
    /// The `persistent://` topic a legacy segment wraps.
    pub legacy_topic: Option<String>,
    /// The start hashes of buckets 1..N-1; empty for a single bucket.
    pub bucket_splits: Vec<u16>,
    /// The topic its producers and consumers attach to: the `segment://` name, or the wrapped topic of a legacy
    /// segment.
    pub topic: String,
}

impl Segment {
    pub fn is_legacy(&self) -> bool {
        self.legacy_topic.is_some()
    }

    fn covers(&self, hash: u16) -> bool {
        (self.hash_start..=self.hash_end).contains(&hash)
    }
}

/// Why a layout can't be built or routed.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum LayoutError {
    #[error("the topic name {0:?} is not a topic:// name")]
    TopicName(String),
    #[error("segment {id} has hash range {start:#x}..={end:#x}, outside the 16-bit ring")]
    HashRange { id: u64, start: u32, end: u32 },
    #[error("the layout has no active segment")]
    NoActiveSegment,
    #[error("no active segment covers segment hash {0:#06x}")]
    Uncovered(u16),
    #[error("the legacy layout has no segment for partition {partition} of {partitions}")]
    NoPartition { partition: u32, partitions: u32 },
}

/// An immutable snapshot of a topic's segments.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Layout {
    epoch: u64,
    topic: String,
    segments: BTreeMap<u64, Segment>,
    /// Ids of the active segments, by hash-range start.
    active: Vec<u64>,
    /// The controller leader's `pulsar://` and `pulsar+ssl://` URLs, when the broker gives them.
    controller: (Option<String>, Option<String>),
}

impl Layout {
    /// Builds a layout from a DAG the broker sent for `topic`, the `topic://tenant/namespace/name` it resolved
    /// the lookup to. Only managed segments need that name, for their `segment://` names: legacy segments carry
    /// the topic they wrap.
    pub fn from_dag(dag: &ScalableTopicDAG, topic: &str) -> Result<Layout, LayoutError> {
        let parent = topic.strip_prefix("topic://").filter(|rest| rest.split('/').count() >= 3);
        let mut segments = BTreeMap::new();
        for info in &dag.segments {
            let ring = |hash: u32| u16::try_from(hash).ok();
            let (Some(hash_start), Some(hash_end)) = (ring(info.hash_start), ring(info.hash_end)) else {
                return Err(LayoutError::HashRange { id: info.segment_id, start: info.hash_start, end: info.hash_end });
            };
            let legacy_topic = info.legacy_topic_name.clone().filter(|name| !name.is_empty());
            let topic = match (&legacy_topic, parent) {
                (Some(name), _) => name.clone(),
                (None, Some(parent)) => {
                    format!("segment://{parent}/{hash_start:04x}-{hash_end:04x}-{}", info.segment_id)
                }
                (None, None) => return Err(LayoutError::TopicName(topic.to_string())),
            };
            let segment = Segment {
                id: info.segment_id,
                hash_start,
                hash_end,
                active: info.state == SegmentState::ACTIVE,
                parents: info.parent_ids.clone(),
                children: info.child_ids.clone(),
                legacy_topic,
                // Split points are 16-bit hashes; anything larger is clamped to the ring.
                bucket_splits: info.entry_bucket_splits.iter().map(|&s| s.min(u16::MAX.into()) as u16).collect(),
                topic,
            };
            segments.insert(segment.id, segment);
        }
        let mut active: Vec<u64> = segments.values().filter(|s| s.active).map(|s| s.id).collect();
        active.sort_by_key(|id| segments[id].hash_start);
        let url = |url: &Option<String>| url.clone().filter(|url| !url.is_empty());
        let controller = (url(&dag.controller_broker_url), url(&dag.controller_broker_url_tls));
        Ok(Layout { epoch: dag.epoch, topic: topic.to_string(), segments, active, controller })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The resolved `topic://` name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    /// The URL of the topic's controller leader, for a client using TLS or not, if the broker gives it.
    pub fn controller(&self, tls: bool) -> Option<&str> {
        if tls { self.controller.1.as_deref() } else { self.controller.0.as_deref() }
    }

    pub fn segment(&self, id: u64) -> Option<&Segment> {
        self.segments.get(&id)
    }

    /// Every segment, active and sealed, by id.
    pub fn segments(&self) -> impl Iterator<Item = &Segment> {
        self.segments.values()
    }

    /// The active segments, by hash-range start.
    pub fn active(&self) -> impl Iterator<Item = &Segment> {
        self.active.iter().map(|id| &self.segments[id])
    }

    pub fn is_active(&self, id: u64) -> bool {
        self.segments.get(&id).is_some_and(|s| s.active)
    }

    /// Whether every active segment is legacy: a synthetic layout over a `persistent://` topic, routed like a v4
    /// partitioned topic.
    pub fn all_legacy(&self) -> bool {
        self.active().all(Segment::is_legacy)
    }

    /// The active segment for a key.
    pub fn route_key(&self, key: KeyHash) -> Result<u64, LayoutError> {
        if self.active.is_empty() {
            return Err(LayoutError::NoActiveSegment);
        }
        if self.all_legacy() {
            let partitions = self.active.len() as u32;
            let partition = key.partition(partitions);
            return self
                .active
                .iter()
                .copied()
                .find(|&id| id == u64::from(partition))
                .ok_or(LayoutError::NoPartition { partition, partitions });
        }
        let hash = key.segment_hash();
        self.active().find(|s| s.covers(hash)).map(|s| s.id).ok_or(LayoutError::Uncovered(hash))
    }

    /// The active segment for the `turn`-th keyless message: round-robin in hash-range order.
    pub fn route_keyless(&self, turn: u64) -> Result<u64, LayoutError> {
        if self.active.is_empty() {
            return Err(LayoutError::NoActiveSegment);
        }
        Ok(self.active[(turn % self.active.len() as u64) as usize])
    }
}
