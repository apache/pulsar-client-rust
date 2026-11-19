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
        Ok(Layout { epoch: dag.epoch, topic: topic.to_string(), segments, active })
    }

    pub fn epoch(&self) -> u64 {
        self.epoch
    }

    /// The resolved `topic://` name.
    pub fn topic(&self) -> &str {
        &self.topic
    }

    pub fn segment(&self, id: u64) -> Option<&Segment> {
        self.segments.get(&id)
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

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::SegmentInfoProto;

    fn info(id: u64, start: u32, end: u32, active: bool) -> SegmentInfoProto {
        SegmentInfoProto {
            segment_id: id,
            hash_start: start,
            hash_end: end,
            state: if active { SegmentState::ACTIVE } else { SegmentState::SEALED },
            ..Default::default()
        }
    }

    fn layout(segments: Vec<SegmentInfoProto>) -> Layout {
        let dag = ScalableTopicDAG { epoch: 3, segments, ..Default::default() };
        Layout::from_dag(&dag, "topic://acme/orders/events").unwrap()
    }

    #[test]
    fn names_segments_like_java() {
        let layout = layout(vec![info(0, 0, 0xffff, false), info(1, 0, 0x7fff, true), info(2, 0x8000, 0xffff, true)]);
        assert_eq!(layout.epoch(), 3);
        assert_eq!(layout.segment(0).unwrap().topic, "segment://acme/orders/events/0000-ffff-0");
        assert_eq!(layout.segment(2).unwrap().topic, "segment://acme/orders/events/8000-ffff-2");
        assert_eq!(layout.active().map(|s| s.id).collect::<Vec<_>>(), [1, 2]);
        assert!(!layout.is_active(0));
    }

    #[test]
    fn routes_keys_by_the_high_half() {
        let layout = layout(vec![info(2, 0x8000, 0xffff, true), info(1, 0, 0x7fff, true)]);
        for key in ["a", "b", "user-42", "", "日本"] {
            let hash = KeyHash::of(key.as_bytes());
            let expected = if hash.segment_hash() < 0x8000 { 1 } else { 2 };
            assert_eq!(layout.route_key(hash), Ok(expected), "{key}");
        }
    }

    #[test]
    fn keyless_goes_round_robin_in_range_order() {
        let layout = layout(vec![info(5, 0x8000, 0xffff, true), info(9, 0, 0x7fff, true)]);
        let turns: Vec<u64> = (0..4).map(|turn| layout.route_keyless(turn).unwrap()).collect();
        assert_eq!(turns, [9, 5, 9, 5]);
    }

    #[test]
    fn legacy_layout_routes_like_v4_partitions() {
        let legacy = |id: u64| SegmentInfoProto {
            legacy_topic_name: Some(format!("persistent://acme/orders/events-partition-{id}")),
            ..info(id, 0, 0xffff, true)
        };
        let layout = layout((0..3).map(legacy).collect());
        assert!(layout.all_legacy());
        assert_eq!(layout.segment(1).unwrap().topic, "persistent://acme/orders/events-partition-1");
        for key in ["a", "b", "c", "user-42"] {
            let hash = KeyHash::of(key.as_bytes());
            assert_eq!(layout.route_key(hash), Ok(u64::from(hash.partition(3))), "{key}");
        }
    }

    #[test]
    fn errors() {
        let gap = layout(vec![info(1, 0, 0x7fff, true)]);
        let high = (0..).map(|i: u32| KeyHash::of(&i.to_be_bytes())).find(|h| h.segment_hash() >= 0x8000).unwrap();
        assert_eq!(gap.route_key(high), Err(LayoutError::Uncovered(high.segment_hash())));
        let sealed = layout(vec![info(1, 0, 0xffff, false)]);
        assert_eq!(sealed.route_keyless(0), Err(LayoutError::NoActiveSegment));
        assert_eq!(sealed.route_key(KeyHash::of(b"k")), Err(LayoutError::NoActiveSegment));
        let dag = ScalableTopicDAG { segments: vec![info(1, 0, 0x1_0000, true)], ..Default::default() };
        assert!(matches!(Layout::from_dag(&dag, "topic://a/b/c"), Err(LayoutError::HashRange { .. })));
        // A managed segment needs a topic:// parent for its name; a legacy one doesn't.
        let managed = ScalableTopicDAG { segments: vec![info(1, 0, 0xffff, true)], ..Default::default() };
        assert!(matches!(Layout::from_dag(&managed, "persistent://a/b/c"), Err(LayoutError::TopicName(_))));
        let legacy =
            SegmentInfoProto { legacy_topic_name: Some("persistent://a/b/c".into()), ..info(0, 0, 0xffff, true) };
        let legacy = ScalableTopicDAG { segments: vec![legacy], ..Default::default() };
        assert!(Layout::from_dag(&legacy, "persistent://a/b/c").is_ok());
    }
}
