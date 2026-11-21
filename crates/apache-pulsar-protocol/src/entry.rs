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

//! Entries as a consumer receives them: checked, decompressed, and split into their messages
//! (docs/5-segment-engine.md §3).
//!
//! An entry is one message, or a batch of them, as a producer stored it. [`decode`] checks it, decompresses its
//! payload with the codec its metadata names, and splits a batch into its messages, leaving out those the broker
//! says were acknowledged (`ack_set`) and those compaction removed. As in Java, an entry is a batch when its
//! metadata sets `num_messages_in_batch`. The messages share the decompressed buffer: each payload is a slice of it.
//!
//! An entry that can't be read is one of two kinds ([`DecodeError::validation_error`]). A corrupt one is
//! acknowledged with the reason, so that the broker drops it, as Java does. One this client can't read yet, an
//! encrypted one or a chunk of a larger message, is left unacknowledged for a consumer that can.

use std::sync::Arc;

use buffa::Message;
use bytes::{Buf, Bytes};

use crate::compression;
use crate::frame::PayloadSection;
use crate::message_id::{EntryId, MessageId};
use crate::proto::command_ack::ValidationError;
use crate::proto::{CommandMessage, CompressionType, KeyValue, MessageMetadata, SingleMessageMetadata};

/// A message of an entry, as the consumer hands it to the application.
#[derive(Debug, Clone)]
pub struct ReceivedMessage {
    pub id: MessageId,
    pub payload: Bytes,
    /// How many times the broker delivered the message before.
    pub redelivery_count: u32,
    pub key: Option<String>,
    pub ordering_key: Option<Vec<u8>>,
    pub properties: Vec<KeyValue>,
    /// Milliseconds since the epoch, when the producer set it.
    pub event_time: Option<u64>,
    pub sequence_id: u64,
    /// The producer sent no value at all, rather than an empty one.
    pub null_value: bool,
    /// The entry's metadata, which the messages of a batch share: the producer's name, the publish time, the schema
    /// version.
    pub entry: Arc<MessageMetadata>,
    /// For a Checkpoint consumer, its attachment to the segment the message came from, which a segment that leaves a
    /// group member's assignment and comes back renews: a message of an earlier one moves no checkpoint. Zero
    /// otherwise.
    pub attachment: u64,
}

impl ReceivedMessage {
    fn single(id: MessageId, payload: Bytes, redelivery_count: u32, entry: Arc<MessageMetadata>) -> Self {
        Self {
            id,
            payload,
            redelivery_count,
            key: entry.partition_key.clone(),
            ordering_key: entry.ordering_key.clone(),
            properties: entry.properties.clone(),
            event_time: entry.event_time,
            sequence_id: entry.sequence_id,
            null_value: entry.null_value == Some(true),
            entry,
            attachment: 0,
        }
    }

    fn in_batch(
        id: MessageId,
        payload: Bytes,
        redelivery_count: u32,
        single: SingleMessageMetadata,
        entry: &Arc<MessageMetadata>,
    ) -> Self {
        Self {
            // A producer sets each message's sequence id; otherwise they follow the entry's.
            sequence_id: single.sequence_id.unwrap_or(entry.sequence_id + id.entry.batch_index as u64),
            id,
            payload,
            redelivery_count,
            key: single.partition_key,
            ordering_key: single.ordering_key,
            properties: single.properties,
            event_time: single.event_time,
            null_value: single.null_value == Some(true),
            entry: entry.clone(),
            attachment: 0,
        }
    }
}

/// The messages of an entry, and how many others it held.
#[derive(Debug)]
pub struct Decoded {
    pub messages: Vec<ReceivedMessage>,
    /// Messages of the batch left out: acknowledged already, or compacted out. The broker counted them against the
    /// consumer's permits all the same.
    pub skipped: u32,
}

/// Why an entry can't be read.
#[derive(Debug, Clone, Copy, PartialEq, Eq, thiserror::Error)]
pub enum DecodeError {
    #[error("the entry's checksum doesn't match")]
    Checksum,
    #[error("the entry's metadata is not decodable")]
    Metadata,
    #[error("the entry doesn't decompress to the size its metadata gives")]
    Decompression,
    #[error("the entry's batch is malformed")]
    Batch,
    #[error("the entry is encrypted")]
    Encrypted,
    #[error("the entry is a chunk of a larger message")]
    Chunked,
    #[error("the entry is compressed with {0:?}, which this build leaves out")]
    Codec(CompressionType),
}

impl DecodeError {
    /// What the consumer tells the broker as it drops a corrupt entry, as Java does; `None` for an entry to leave for
    /// a consumer that can read it.
    pub fn validation_error(&self) -> Option<ValidationError> {
        match self {
            Self::Checksum | Self::Metadata => Some(ValidationError::ChecksumMismatch),
            Self::Decompression => Some(ValidationError::DecompressionError),
            Self::Batch => Some(ValidationError::BatchDeSerializeError),
            Self::Encrypted | Self::Chunked | Self::Codec(_) => None,
        }
    }
}

/// Reads an entry that the broker pushed to the consumer of segment `segment_id`.
pub fn decode(segment_id: i64, message: &CommandMessage, section: &PayloadSection) -> Result<Decoded, DecodeError> {
    if section.checksum_valid == Some(false) {
        return Err(DecodeError::Checksum);
    }
    let metadata = MessageMetadata::decode_from_slice(&section.metadata).ok().ok_or(DecodeError::Metadata)?;
    if !metadata.encryption_keys.is_empty() {
        return Err(DecodeError::Encrypted);
    }
    if metadata.num_chunks_from_msg.is_some_and(|chunks| chunks > 1) {
        return Err(DecodeError::Chunked);
    }
    let size = metadata.uncompressed_size.unwrap_or_default() as usize;
    let payload = compression::decompress(metadata.compression(), section.payload.clone(), size)?;
    let id = &message.message_id;
    let message_id = |batch_index, batch_size| {
        let entry = EntryId {
            ledger_id: id.ledgerId as i64,
            entry_id: id.entryId as i64,
            partition: id.partition(),
            batch_index,
            batch_size,
        };
        MessageId::new(segment_id, entry)
    };
    let redelivery_count = message.redelivery_count.unwrap_or_default();
    let metadata = Arc::new(metadata);
    let Some(count) = metadata.num_messages_in_batch else {
        let single = ReceivedMessage::single(message_id(-1, 0), payload, redelivery_count, metadata);
        return Ok(Decoded { messages: vec![single], skipped: 0 });
    };
    let mut batch = Batch(payload);
    let mut decoded = Decoded { messages: Vec::new(), skipped: 0 };
    for index in 0..u32::try_from(count).ok().ok_or(DecodeError::Batch)? {
        let (single, payload) = batch.next()?;
        if single.compacted_out == Some(true) || !unacked(&message.ack_set, index as usize) {
            decoded.skipped += 1;
            continue;
        }
        let id = message_id(index as i32, count);
        decoded.messages.push(ReceivedMessage::in_batch(id, payload, redelivery_count, single, &metadata));
    }
    Ok(decoded)
}

/// Whether index `index` of a batch is still to deliver. The broker's `ack_set` is the bitset of the indexes not
/// acknowledged yet, laid out as Java's `BitSet.toLongArray`: none means all.
fn unacked(ack_set: &[i64], index: usize) -> bool {
    ack_set.is_empty() || ack_set.get(index / 64).is_some_and(|word| (word >> (index % 64)) & 1 == 1)
}

/// The messages of a batch, in order: each a big-endian `u32` length, its `SingleMessageMetadata`, then its payload.
struct Batch(Bytes);

impl Batch {
    fn next(&mut self) -> Result<(SingleMessageMetadata, Bytes), DecodeError> {
        let size = self.take(4)?.get_u32() as usize;
        let single = SingleMessageMetadata::decode_from_slice(&self.take(size)?).ok().ok_or(DecodeError::Batch)?;
        let payload = usize::try_from(single.payload_size).ok().ok_or(DecodeError::Batch)?;
        Ok((single, self.take(payload)?))
    }

    fn take(&mut self, n: usize) -> Result<Bytes, DecodeError> {
        if n > self.0.len() {
            return Err(DecodeError::Batch);
        }
        Ok(self.0.split_to(n))
    }
}
