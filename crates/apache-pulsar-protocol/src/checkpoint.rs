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

//! Checkpoints, in the Java V5 client's byte format (docs/7-api.md §7).
//!
//! A [`Checkpoint`] is where a checkpoint consumer starts reading a topic: at the earliest or the latest position of
//! its segments, or at a position in each segment it names. The bytes are Java's `CheckpointV5.toByteArray`, big
//! endian: a type byte, 0 for positions, 1 for earliest and 2 for latest; then, for positions, the segment vector of
//! a message id (an `i32` count, then per segment its `i64` id and its length-prefixed `MessageIdData`).
//!
//! Encoding writes the segments in id order, so equal checkpoints give equal bytes; Java writes them in hash-map
//! order, which decodes the same. Decoding accepts anything Java's `CheckpointV5.fromByteArray` does.

use crate::message_id::{self, MessageIdError, SegmentVector};

const POSITIONS: u8 = 0;
const EARLIEST: u8 = 1;
const LATEST: u8 = 2;

/// A position across the segments of a topic: Java's `CheckpointV5`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub enum Checkpoint {
    /// The start of every segment.
    Earliest,
    /// The end of every segment.
    Latest,
    /// A position in each segment it names: the last message read from it, or where its reading starts.
    Positions(SegmentVector),
}

/// Why bytes are not a checkpoint.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum CheckpointError {
    #[error("the checkpoint is empty")]
    Empty,
    #[error("the checkpoint has the unknown type {0}")]
    Type(u8),
    #[error("the checkpoint's positions are malformed: {0}")]
    Positions(#[from] MessageIdError),
}

impl Checkpoint {
    pub fn to_bytes(&self) -> Vec<u8> {
        match self {
            Checkpoint::Earliest => vec![EARLIEST],
            Checkpoint::Latest => vec![LATEST],
            Checkpoint::Positions(positions) => {
                let mut out = vec![POSITIONS];
                message_id::put_vector(&mut out, positions);
                out
            }
        }
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<Checkpoint, CheckpointError> {
        let (&kind, rest) = bytes.split_first().ok_or(CheckpointError::Empty)?;
        match kind {
            POSITIONS => Ok(Checkpoint::Positions(message_id::read_vector(rest)?)),
            EARLIEST => Ok(Checkpoint::Earliest),
            LATEST => Ok(Checkpoint::Latest),
            other => Err(CheckpointError::Type(other)),
        }
    }
}
