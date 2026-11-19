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

//! MessageId bytes against golden vectors from the Java client (tools/java-vectors).

use std::collections::BTreeMap;

use apache_pulsar_protocol::message_id::{EntryId, MessageId, MessageIdError, SegmentVector};
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    segment_id: i64,
    v4: V4,
    position_vector: Vec<Position>,
    parent_topic: Option<String>,
    multi_topic_vector: Option<Vec<Topic>>,
    bytes_hex: String,
    /// False when Java wrote a vector in hash-map order, which our sorted encoding need not reproduce.
    byte_exact: bool,
}

#[derive(Deserialize)]
struct V4 {
    ledger_id: i64,
    entry_id: i64,
    partition: i32,
    batch_index: i32,
    batch_size: i32,
    bytes_hex: String,
}

#[derive(Deserialize)]
struct Position {
    segment_id: i64,
    v4: V4,
}

#[derive(Deserialize)]
struct Topic {
    topic: String,
    segments: Vec<Position>,
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
}

fn entry(v4: &V4) -> EntryId {
    EntryId {
        ledger_id: v4.ledger_id,
        entry_id: v4.entry_id,
        partition: v4.partition,
        batch_index: v4.batch_index,
        batch_size: v4.batch_size,
    }
}

fn vector(positions: &[Position]) -> SegmentVector {
    positions.iter().map(|p| (p.segment_id, entry(&p.v4))).collect()
}

fn vectors() -> Vec<Case> {
    let vectors: Vectors = serde_json::from_str(include_str!("vectors/message_id.json")).unwrap();
    assert!(vectors.cases.len() >= 35, "too few message id vectors");
    vectors.cases
}

#[test]
fn decodes_every_java_id() {
    for case in vectors() {
        let name = &case.name;
        let id = MessageId::from_bytes(&unhex(&case.bytes_hex)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(id.segment_id, case.segment_id, "{name}");
        let expected = entry(&case.v4);
        assert_eq!(id.entry, expected, "{name}");
        assert_eq!(id.entry.batch_size, expected.batch_size, "{name}: batch size");
        assert_eq!(id.positions, vector(&case.position_vector), "{name}");
        assert_eq!(id.parent_topic, case.parent_topic, "{name}");
        let topics = case
            .multi_topic_vector
            .as_ref()
            .map(|topics| topics.iter().map(|t| (t.topic.clone(), vector(&t.segments))).collect::<BTreeMap<_, _>>());
        assert_eq!(id.topics, topics, "{name}");
    }
}

#[test]
fn encodes_like_java() {
    for case in vectors() {
        let name = &case.name;
        assert_eq!(entry(&case.v4).to_bytes(), unhex(&case.v4.bytes_hex), "{name}: v4 bytes");
        let mut id = MessageId::new(case.segment_id, entry(&case.v4));
        id.positions = vector(&case.position_vector);
        id.parent_topic = case.parent_topic.clone();
        id.topics = case
            .multi_topic_vector
            .as_ref()
            .map(|topics| topics.iter().map(|t| (t.topic.clone(), vector(&t.segments))).collect());
        let bytes = id.to_bytes();
        if case.byte_exact {
            assert_eq!(bytes, unhex(&case.bytes_hex), "{name}");
        } else {
            assert_eq!(bytes.len(), case.bytes_hex.len() / 2, "{name}: size");
        }
        let back = MessageId::from_bytes(&bytes).unwrap();
        assert_eq!((back.positions, back.parent_topic, back.topics), (id.positions, id.parent_topic, id.topics));
    }
}

#[test]
fn sentinels_and_order() {
    assert_eq!(MessageId::earliest().entry, EntryId::EARLIEST);
    assert!(MessageId::earliest() < MessageId::latest());
    let at = |segment_id, ledger_id, entry_id, batch_index| {
        MessageId::new(segment_id, EntryId { ledger_id, entry_id, partition: -1, batch_index, batch_size: 0 })
    };
    // Segment first, then ledger, entry and batch index; the batch size is not compared.
    assert!(at(0, 9, 9, -1) < at(1, 0, 0, -1));
    assert!(at(1, 1, 2, -1) < at(1, 2, 0, -1));
    assert!(at(1, 1, 2, -1) < at(1, 1, 2, 0));
    let mut sized = at(1, 1, 2, 0);
    sized.entry.batch_size = 7;
    assert_eq!(sized, at(1, 1, 2, 0));
}

/// Older Java clients wrote ids without the trailing sections: the segment id and the position only.
#[test]
fn decodes_ids_without_the_trailing_sections() {
    let id = MessageId::new(3, EntryId { ledger_id: 1, entry_id: 2, partition: -1, batch_index: -1, batch_size: 0 });
    let position = id.entry.to_bytes();
    let short = [&3i64.to_be_bytes()[..], &(position.len() as i32).to_be_bytes(), &position].concat();
    let decoded = MessageId::from_bytes(&short).unwrap();
    assert_eq!(decoded, id);
    assert_eq!((decoded.positions.len(), decoded.parent_topic, decoded.topics), (0, None, None));
}

#[test]
fn rejects_malformed_bytes() {
    assert_eq!(MessageId::from_bytes(&[0; 11]), Err(MessageIdError::Truncated));
    let mut bytes = MessageId::new(3, EntryId::EARLIEST).to_bytes();
    bytes[11] = 0xff; // the v4 length now runs past the end
    assert_eq!(MessageId::from_bytes(&bytes), Err(MessageIdError::Length("position")));
    let mut bytes = MessageId::new(3, EntryId::EARLIEST).to_bytes();
    bytes[8] = 0xff; // a negative v4 length
    assert_eq!(MessageId::from_bytes(&bytes), Err(MessageIdError::Length("position")));
    let mut bytes = MessageId::new(3, EntryId::EARLIEST).to_bytes();
    bytes.truncate(bytes.len() - 2); // the cross-topic count is cut
    assert_eq!(MessageId::from_bytes(&bytes), Err(MessageIdError::Truncated));
}

#[test]
fn ids_hash_and_order_as_they_compare() {
    use std::collections::{BTreeSet, HashSet};
    let entry = |batch_index, batch_size| EntryId { ledger_id: 1, entry_id: 2, partition: -1, batch_index, batch_size };
    // The batch size is carried, not compared: the same entry in a set once.
    let entries: HashSet<EntryId> = [entry(0, 1), entry(0, 5), entry(1, 5)].into_iter().collect();
    assert_eq!(entries.len(), 2);
    let ordered: BTreeSet<EntryId> = [entry(1, 5), entry(-1, 0), entry(0, 1)].into_iter().collect();
    assert_eq!(ordered.iter().map(|e| e.batch_index).collect::<Vec<_>>(), [-1, 0, 1]);
    assert!(entry(0, 1).partial_cmp(&entry(1, 1)).unwrap().is_lt());
    let ids: HashSet<MessageId> =
        [MessageId::new(1, entry(0, 1)), MessageId::new(1, entry(0, 9)), MessageId::new(2, entry(0, 1))]
            .into_iter()
            .collect();
    assert_eq!(ids.len(), 2);
}

#[test]
fn rejects_malformed_trailing_sections() {
    let mut base = MessageId::new(3, EntryId::EARLIEST);
    base.parent_topic = Some("topic://t/n/x".into());
    let bytes = base.to_bytes();
    // A parent topic that isn't UTF-8.
    let topic_at = bytes.len() - 4 - "topic://t/n/x".len();
    let mut bad = bytes.clone();
    bad[topic_at] = 0xff;
    assert_eq!(MessageId::from_bytes(&bad), Err(MessageIdError::Utf8));
    // A cross-topic vector with a negative topic length.
    let mut with_topics = base.clone();
    with_topics.topics = Some([("topic://t/n/y".to_string(), SegmentVector::new())].into_iter().collect());
    let mut bad = with_topics.to_bytes();
    let count_at = bytes.len() - 4;
    bad[count_at + 4..count_at + 8].copy_from_slice(&(-5i32).to_be_bytes());
    assert_eq!(MessageId::from_bytes(&bad), Err(MessageIdError::Length("topic")));
    // A position vector whose entry runs past the end.
    let mut with_positions = MessageId::new(3, EntryId::EARLIEST);
    with_positions.positions.insert(4, EntryId::LATEST);
    let mut bad = with_positions.to_bytes();
    let entry_length_at = 12 + EntryId::EARLIEST.to_bytes().len() + 4 + 8;
    bad[entry_length_at..entry_length_at + 4].copy_from_slice(&1000i32.to_be_bytes());
    assert_eq!(MessageId::from_bytes(&bad), Err(MessageIdError::Length("position")));
    // A position that isn't a MessageIdData.
    let mut bad = MessageId::new(3, EntryId::EARLIEST).to_bytes();
    bad.truncate(12);
    bad[8..12].copy_from_slice(&0i32.to_be_bytes());
    bad.extend_from_slice(&[0, 0, 0, 0]);
    assert!(MessageId::from_bytes(&bad).is_ok(), "an empty position decodes with defaults");
    assert!(matches!(EntryId::from_bytes(&[0xff, 0xff]), Err(MessageIdError::Protobuf(_))));
}
