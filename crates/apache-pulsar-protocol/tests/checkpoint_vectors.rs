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

//! Checkpoint bytes against golden vectors from the Java client (tools/java-vectors), and the cases of Java's
//! `CheckpointV5Test`.

use apache_pulsar_protocol::checkpoint::{Checkpoint, CheckpointError};
use apache_pulsar_protocol::message_id::{EntryId, MessageIdError, SegmentVector};
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    name: String,
    r#type: String,
    positions: Vec<Position>,
    bytes_hex: String,
    /// False when Java wrote the positions in hash-map order, which our sorted encoding need not reproduce.
    byte_exact: bool,
}

#[derive(Deserialize)]
struct Position {
    segment_id: i64,
    v4: V4,
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

fn checkpoint(case: &Case) -> Checkpoint {
    match case.r#type.as_str() {
        "earliest" => Checkpoint::Earliest,
        "latest" => Checkpoint::Latest,
        "regular" => Checkpoint::Positions(case.positions.iter().map(|p| (p.segment_id, entry(&p.v4))).collect()),
        other => panic!("{}: unknown type {other}", case.name),
    }
}

fn vectors() -> Vec<Case> {
    let vectors: Vectors = serde_json::from_str(include_str!("vectors/checkpoint.json")).unwrap();
    assert!(vectors.cases.len() >= 15, "too few checkpoint vectors");
    vectors.cases
}

#[test]
fn decodes_every_java_checkpoint() {
    for case in vectors() {
        let name = &case.name;
        let decoded = Checkpoint::from_bytes(&unhex(&case.bytes_hex)).unwrap_or_else(|e| panic!("{name}: {e}"));
        assert_eq!(decoded, checkpoint(&case), "{name}");
        // The batch size is carried, though not compared.
        if let Checkpoint::Positions(positions) = &decoded {
            for p in &case.positions {
                assert_eq!(positions[&p.segment_id].batch_size, p.v4.batch_size, "{name}: batch size");
            }
        }
    }
}

#[test]
fn encodes_like_java() {
    for case in vectors() {
        let name = &case.name;
        for p in &case.positions {
            assert_eq!(entry(&p.v4).to_bytes(), unhex(&p.v4.bytes_hex), "{name}: v4 bytes");
        }
        let expected = checkpoint(&case);
        let bytes = expected.to_bytes();
        if case.byte_exact {
            assert_eq!(bytes, unhex(&case.bytes_hex), "{name}");
        } else {
            assert_eq!(bytes.len(), case.bytes_hex.len() / 2, "{name}: size");
        }
        assert_eq!(Checkpoint::from_bytes(&bytes).unwrap(), expected, "{name}: round trip");
    }
}

/// `CheckpointV5Test`: positions round-trip, empty ones too, and the sentinels are a single type byte.
#[test]
fn round_trips_positions_and_sentinels() {
    let at = |ledger_id, entry_id| EntryId { ledger_id, entry_id, partition: 0, batch_index: -1, batch_size: 0 };
    let positions: SegmentVector = [(0, at(10, 20)), (1, at(30, 40)), (2, at(50, 60))].into_iter().collect();
    let regular = Checkpoint::Positions(positions);
    assert_eq!(Checkpoint::from_bytes(&regular.to_bytes()).unwrap(), regular);
    let empty = Checkpoint::Positions(SegmentVector::new());
    assert_eq!(Checkpoint::from_bytes(&empty.to_bytes()).unwrap(), empty);
    for sentinel in [Checkpoint::Earliest, Checkpoint::Latest] {
        let bytes = sentinel.to_bytes();
        assert_eq!(bytes.len(), 1);
        assert_eq!(Checkpoint::from_bytes(&bytes).unwrap(), sentinel);
    }
}

#[test]
fn rejects_malformed_bytes() {
    assert_eq!(Checkpoint::from_bytes(&[]), Err(CheckpointError::Empty));
    assert_eq!(Checkpoint::from_bytes(&[127, 0, 0, 0, 0, 0, 0, 0, 0]), Err(CheckpointError::Type(127)));
    // A count with no room for it.
    let truncated = Checkpoint::from_bytes(&[0, 0, 0]);
    assert_eq!(truncated, Err(CheckpointError::Positions(MessageIdError::Truncated)));
    assert_eq!(
        truncated.unwrap_err().to_string(),
        "the checkpoint's positions are malformed: the message id is truncated"
    );
    // One position, whose length runs past the end.
    let mut bytes = Checkpoint::Positions([(3, EntryId::EARLIEST)].into_iter().collect()).to_bytes();
    bytes[13] = 0x7f;
    assert_eq!(Checkpoint::from_bytes(&bytes), Err(CheckpointError::Positions(MessageIdError::Length("position"))));
}

/// Java reads no further than the positions, and reads a negative count as none.
#[test]
fn decodes_as_leniently_as_java() {
    let mut bytes = Checkpoint::Positions([(1, EntryId::LATEST)].into_iter().collect()).to_bytes();
    bytes.extend_from_slice(b"trailing");
    assert_eq!(Checkpoint::from_bytes(&bytes).unwrap(), Checkpoint::Positions([(1, EntryId::LATEST)].into()));
    let negative = [0, 0xff, 0xff, 0xff, 0xff];
    assert_eq!(Checkpoint::from_bytes(&negative).unwrap(), Checkpoint::Positions(SegmentVector::new()));
}
