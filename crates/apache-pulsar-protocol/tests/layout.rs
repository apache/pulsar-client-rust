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

//! Layouts from DAGs: segment names, and routing.

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::layout::{Layout, LayoutError};
use apache_pulsar_protocol::proto::{ScalableTopicDAG, SegmentInfoProto, SegmentState};

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
    let legacy = SegmentInfoProto { legacy_topic_name: Some("persistent://a/b/c".into()), ..info(0, 0, 0xffff, true) };
    let legacy = ScalableTopicDAG { segments: vec![legacy], ..Default::default() };
    assert!(Layout::from_dag(&legacy, "persistent://a/b/c").is_ok());
}

/// A segment's entry bucket splits are kept, each bounded to the 16-bit hash range.
#[test]
fn keeps_bucket_splits() {
    let mut segment = info(0, 0, 0xffff, true);
    segment.entry_bucket_splits = vec![0x100, 0x1_0000];
    assert_eq!(layout(vec![segment]).segment(0).unwrap().bucket_splits, [0x100, 0xffff]);
}
