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

//! Message identifiers, in the Java V5 client's byte format (docs/7-api.md §7).
//!
//! A [`MessageId`] is a position in one segment, an [`EntryId`], plus what a consumer attaches to it: the position
//! vector, and for a namespace consumer the parent topic and the cross-topic vector. The bytes are Java's
//! `MessageIdV5.toByteArray`, big endian:
//!
//! ```text
//! segment id                 i64
//! v4 id                      i32 length, MessageIdData protobuf
//! position vector            i32 count, then per entry: i64 segment id, i32 length, MessageIdData
//! parent topic               i32 length (-1 when absent), UTF-8
//! cross-topic vector         i32 count (-1 when absent), then per topic: i32 length, UTF-8, then a segment vector
//! ```
//!
//! Decoding accepts ids without the trailing sections, as older Java clients wrote them. Encoding writes vectors in
//! key order, so equal ids give equal bytes; Java writes them in hash-map order, which decodes the same.

use std::cmp::Ordering;
use std::collections::BTreeMap;
use std::hash::{Hash, Hasher};

use buffa::Message;

use crate::proto::MessageIdData;

/// A position in one topic: Java's v4 `MessageIdImpl`, or `BatchMessageIdImpl` when it has a batch index.
///
/// Equality, hashing and order use the ledger, entry, partition and batch index, as Java's do; the batch size
/// is carried but not compared.
#[derive(Debug, Clone, Copy)]
pub struct EntryId {
    pub ledger_id: i64,
    pub entry_id: i64,
    /// The v4 partition, or -1.
    pub partition: i32,
    /// The index in the batch, or -1 for an id that isn't in a batch.
    pub batch_index: i32,
    pub batch_size: i32,
}

impl EntryId {
    /// Java's `MessageId.earliest`.
    pub const EARLIEST: EntryId =
        EntryId { ledger_id: -1, entry_id: -1, partition: -1, batch_index: -1, batch_size: 0 };
    /// Java's `MessageId.latest`.
    pub const LATEST: EntryId =
        EntryId { ledger_id: i64::MAX, entry_id: i64::MAX, partition: -1, batch_index: -1, batch_size: 0 };

    pub fn from_proto(id: &MessageIdData) -> EntryId {
        EntryId {
            ledger_id: id.ledgerId as i64,
            entry_id: id.entryId as i64,
            partition: id.partition(),
            batch_index: id.batch_index(),
            batch_size: id.batch_size.unwrap_or(0),
        }
    }

    /// The protobuf form, with the fields Java's `MessageIdImpl.writeMessageIdData` sets: the partition only when
    /// there is one, the batch index only in a batch, and the batch size always.
    pub fn to_proto(&self) -> MessageIdData {
        MessageIdData {
            ledgerId: self.ledger_id as u64,
            entryId: self.entry_id as u64,
            partition: (self.partition >= 0).then_some(self.partition),
            batch_index: (self.batch_index != -1).then_some(self.batch_index),
            batch_size: (self.batch_size > -1).then_some(self.batch_size),
            ..Default::default()
        }
    }

    /// Java's `MessageId.toByteArray`.
    pub fn to_bytes(&self) -> Vec<u8> {
        self.to_proto().encode_to_vec()
    }

    /// Java's `MessageId.fromByteArray`. Chunked ids and batch ack sets decode to their position only.
    pub fn from_bytes(bytes: &[u8]) -> Result<EntryId, MessageIdError> {
        let id = MessageIdData::decode_from_slice(bytes).map_err(|e| MessageIdError::Protobuf(e.to_string()))?;
        Ok(EntryId::from_proto(&id))
    }

    fn key(&self) -> (i64, i64, i32, i32) {
        (self.ledger_id, self.entry_id, self.partition, self.batch_index)
    }
}

impl PartialEq for EntryId {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for EntryId {}

impl Hash for EntryId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

impl PartialOrd for EntryId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Java's `MessageIdAdv.compareTo`: ledger, entry, partition, then batch index.
impl Ord for EntryId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

/// Why bytes are not a message id.
#[derive(Debug, Clone, PartialEq, Eq, thiserror::Error)]
pub enum MessageIdError {
    #[error("the message id is truncated")]
    Truncated,
    #[error("the message id has an invalid {0} length")]
    Length(&'static str),
    #[error("the message id's topic name is not UTF-8")]
    Utf8,
    #[error("the message id's position is not decodable: {0}")]
    Protobuf(String),
}

/// A segment's latest delivered position, per segment id.
pub type SegmentVector = BTreeMap<i64, EntryId>;

/// The id of a message on a scalable topic: Java's `MessageIdV5`.
///
/// Equality, hashing and order use the segment id and the entry only, as Java's do: the vectors a consumer
/// attaches are carried, not compared.
#[derive(Debug, Clone)]
pub struct MessageId {
    /// The segment, or [`MessageId::NO_SEGMENT`].
    pub segment_id: i64,
    pub entry: EntryId,
    /// The position vector of a Stream consumer's message.
    pub positions: SegmentVector,
    /// The scalable topic a namespace consumer delivered the message from.
    pub parent_topic: Option<String>,
    /// A namespace consumer's position vector, per topic.
    pub topics: Option<BTreeMap<String, SegmentVector>>,
}

impl MessageId {
    /// The segment id of the earliest and latest sentinels.
    pub const NO_SEGMENT: i64 = -1;

    pub fn new(segment_id: i64, entry: EntryId) -> MessageId {
        MessageId { segment_id, entry, positions: SegmentVector::new(), parent_topic: None, topics: None }
    }

    /// Java's `MessageIdV5.EARLIEST`.
    pub fn earliest() -> MessageId {
        MessageId::new(Self::NO_SEGMENT, EntryId::EARLIEST)
    }

    /// Java's `MessageIdV5.LATEST`.
    pub fn latest() -> MessageId {
        MessageId::new(Self::NO_SEGMENT, EntryId::LATEST)
    }

    pub fn to_bytes(&self) -> Vec<u8> {
        let mut out = Vec::with_capacity(64);
        out.extend_from_slice(&self.segment_id.to_be_bytes());
        put_bytes(&mut out, &self.entry.to_bytes());
        put_vector(&mut out, &self.positions);
        match &self.parent_topic {
            Some(topic) => put_bytes(&mut out, topic.as_bytes()),
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        }
        match &self.topics {
            Some(topics) => {
                out.extend_from_slice(&(topics.len() as i32).to_be_bytes());
                for (topic, vector) in topics {
                    put_bytes(&mut out, topic.as_bytes());
                    put_vector(&mut out, vector);
                }
            }
            None => out.extend_from_slice(&(-1i32).to_be_bytes()),
        }
        out
    }

    pub fn from_bytes(bytes: &[u8]) -> Result<MessageId, MessageIdError> {
        let mut reader = Reader(bytes);
        if bytes.len() < 12 {
            return Err(MessageIdError::Truncated);
        }
        let segment_id = reader.i64()?;
        let entry = EntryId::from_bytes(reader.sized("position")?)?;
        let mut id = MessageId::new(segment_id, entry);
        if !reader.is_empty() {
            id.positions = reader.vector()?;
        }
        if !reader.is_empty() {
            let length = reader.i32()?;
            if length >= 0 {
                id.parent_topic = Some(reader.text(length as usize)?);
            }
        }
        if !reader.is_empty() {
            let count = reader.i32()?;
            if count >= 0 {
                let mut topics = BTreeMap::new();
                for _ in 0..count {
                    let length = reader.i32()?;
                    let length = usize::try_from(length).map_err(|_| MessageIdError::Length("topic"))?;
                    let topic = reader.text(length)?;
                    topics.insert(topic, reader.vector()?);
                }
                id.topics = Some(topics);
            }
        }
        Ok(id)
    }

    fn key(&self) -> (i64, &EntryId) {
        (self.segment_id, &self.entry)
    }
}

impl PartialEq for MessageId {
    fn eq(&self, other: &Self) -> bool {
        self.key() == other.key()
    }
}

impl Eq for MessageId {}

impl Hash for MessageId {
    fn hash<H: Hasher>(&self, state: &mut H) {
        self.key().hash(state);
    }
}

impl PartialOrd for MessageId {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

/// Java's `MessageIdV5.compareTo`: the segment id, then the entry.
impl Ord for MessageId {
    fn cmp(&self, other: &Self) -> Ordering {
        self.key().cmp(&other.key())
    }
}

fn put_bytes(out: &mut Vec<u8>, bytes: &[u8]) {
    out.extend_from_slice(&(bytes.len() as i32).to_be_bytes());
    out.extend_from_slice(bytes);
}

/// Writes a segment vector: an `i32` count, then per segment its `i64` id and its length-prefixed position. A
/// checkpoint's positions are one too.
pub(crate) fn put_vector(out: &mut Vec<u8>, vector: &SegmentVector) {
    out.extend_from_slice(&(vector.len() as i32).to_be_bytes());
    for (segment_id, entry) in vector {
        out.extend_from_slice(&segment_id.to_be_bytes());
        put_bytes(out, &entry.to_bytes());
    }
}

/// Reads a segment vector, ignoring what follows it.
pub(crate) fn read_vector(bytes: &[u8]) -> Result<SegmentVector, MessageIdError> {
    Reader(bytes).vector()
}

struct Reader<'a>(&'a [u8]);

impl<'a> Reader<'a> {
    fn is_empty(&self) -> bool {
        self.0.is_empty()
    }

    fn take(&mut self, n: usize) -> Result<&'a [u8], MessageIdError> {
        if n > self.0.len() {
            return Err(MessageIdError::Truncated);
        }
        let (head, rest) = self.0.split_at(n);
        self.0 = rest;
        Ok(head)
    }

    fn i32(&mut self) -> Result<i32, MessageIdError> {
        Ok(i32::from_be_bytes(self.take(4)?.try_into().unwrap()))
    }

    fn i64(&mut self) -> Result<i64, MessageIdError> {
        Ok(i64::from_be_bytes(self.take(8)?.try_into().unwrap()))
    }

    /// A length-prefixed slice.
    fn sized(&mut self, what: &'static str) -> Result<&'a [u8], MessageIdError> {
        let length = usize::try_from(self.i32()?).map_err(|_| MessageIdError::Length(what))?;
        if length > self.0.len() {
            return Err(MessageIdError::Length(what));
        }
        self.take(length)
    }

    fn text(&mut self, length: usize) -> Result<String, MessageIdError> {
        let bytes = self.take(length)?;
        String::from_utf8(bytes.to_vec()).map_err(|_| MessageIdError::Utf8)
    }

    fn vector(&mut self) -> Result<SegmentVector, MessageIdError> {
        let count = self.i32()?;
        let mut vector = SegmentVector::new();
        for _ in 0..count.max(0) {
            let segment_id = self.i64()?;
            vector.insert(segment_id, EntryId::from_bytes(self.sized("position")?)?);
        }
        Ok(vector)
    }
}
