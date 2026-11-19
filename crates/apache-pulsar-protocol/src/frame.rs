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

//! Frame encoding and decoding, in both directions (docs/3-protocol.md §3).
//!
//! ```text
//! simple command   [total_size u32][command_size u32][BaseCommand]
//! payload command  [total_size u32][command_size u32][BaseCommand]
//!                  ([0x0e02 u16][size u32][BrokerEntryMetadata])      broker to client only, optional
//!                  [0x0e01 u16][crc32c u32][metadata_size u32][MessageMetadata][payload]
//! ```
//!
//! All integers are big endian. `total_size` counts everything after itself. The CRC32C covers everything after
//! the checksum.

use buffa::{Message, SizeCache};
use bytes::{Buf, BufMut, Bytes, BytesMut};

use crate::proto::{BaseCommand, MessageMetadata};

/// The magic number before the checksum of a payload command.
pub const MAGIC_CRC32C: u16 = 0x0e01;
/// The magic number before the broker entry metadata of a payload command.
pub const MAGIC_BROKER_ENTRY_METADATA: u16 = 0x0e02;
/// The broker's default `maxMessageSize`, used until `CommandConnected` says otherwise.
pub const DEFAULT_MAX_MESSAGE_SIZE: u32 = 5 * 1024 * 1024;
/// Room for the command and the metadata on top of the largest message, as the Java client allows.
pub const FRAME_OVERHEAD: u32 = 10 * 1024;

/// A frame could not be encoded or decoded.
#[derive(Debug, Clone, thiserror::Error, PartialEq, Eq)]
pub enum FrameError {
    #[error("frame of {size} bytes exceeds the limit of {limit} bytes")]
    TooLarge { size: u64, limit: u32 },
    #[error("malformed frame: {0}")]
    Malformed(&'static str),
}

/// Appends a simple command frame to `buf`.
pub fn encode_command(command: &BaseCommand, buf: &mut BytesMut) -> Result<(), FrameError> {
    let mut cache = SizeCache::new();
    let command_size = command.compute_size(&mut cache);
    let total = 4 + u64::from(command_size);
    let total = u32::try_from(total).map_err(|_| FrameError::TooLarge { size: total, limit: u32::MAX })?;
    buf.reserve(4 + total as usize);
    buf.put_u32(total);
    buf.put_u32(command_size);
    command.write_to(&mut cache, buf);
    Ok(())
}

/// Appends a payload command frame (a `CommandSend`, or a `CommandMessage` from the simulated broker) to `buf`.
pub fn encode_payload_command(
    command: &BaseCommand,
    metadata: &MessageMetadata,
    payload: &[u8],
    buf: &mut BytesMut,
) -> Result<(), FrameError> {
    let mut command_cache = SizeCache::new();
    let command_size = command.compute_size(&mut command_cache);
    let mut metadata_cache = SizeCache::new();
    let metadata_size = metadata.compute_size(&mut metadata_cache);
    let total = 4 + u64::from(command_size) + 2 + 4 + 4 + u64::from(metadata_size) + payload.len() as u64;
    let total = u32::try_from(total).map_err(|_| FrameError::TooLarge { size: total, limit: u32::MAX })?;
    buf.reserve(4 + total as usize);
    buf.put_u32(total);
    buf.put_u32(command_size);
    command.write_to(&mut command_cache, buf);
    buf.put_u16(MAGIC_CRC32C);
    let checksum_at = buf.len();
    buf.put_u32(0);
    let checked_from = buf.len();
    buf.put_u32(metadata_size);
    metadata.write_to(&mut metadata_cache, buf);
    buf.put_slice(payload);
    let checksum = crc32c::crc32c(&buf[checked_from..]);
    buf[checksum_at..checked_from].copy_from_slice(&checksum.to_be_bytes());
    Ok(())
}

/// A decoded frame. Every part is a slice of the bytes read from the socket, without a copy.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct Frame {
    /// The encoded `BaseCommand`. Decode it with [`Frame::command_view`].
    pub command: Bytes,
    /// The payload section of a payload command.
    pub payload: Option<PayloadSection>,
}

/// The part of a payload command after the `BaseCommand`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct PayloadSection {
    /// The encoded `BrokerEntryMetadata`, when the broker sent one.
    pub broker_entry_metadata: Option<Bytes>,
    /// Whether the CRC32C matched. `None` if the frame has no checksum section.
    pub checksum_valid: Option<bool>,
    /// The encoded `MessageMetadata`.
    pub metadata: Bytes,
    /// The message payload: one message, or a batch.
    pub payload: Bytes,
}

impl Frame {
    /// Decodes the command, borrowing from the frame.
    pub fn command_view(&self) -> Result<crate::proto::__buffa::view::BaseCommandView<'_>, buffa::DecodeError> {
        use buffa::MessageView;
        crate::proto::__buffa::view::BaseCommandView::decode_view(&self.command)
    }
}

/// Splits frames off a read buffer.
#[derive(Debug, Clone)]
pub struct FrameDecoder {
    max_frame_size: u32,
}

impl Default for FrameDecoder {
    fn default() -> Self {
        Self::new(DEFAULT_MAX_MESSAGE_SIZE)
    }
}

impl FrameDecoder {
    /// A decoder that accepts messages up to `max_message_size`, plus the frame overhead.
    pub fn new(max_message_size: u32) -> Self {
        Self { max_frame_size: max_message_size.saturating_add(FRAME_OVERHEAD) }
    }

    /// Raises or lowers the limit, after `CommandConnected` brings the broker's `max_message_size`.
    pub fn set_max_message_size(&mut self, max_message_size: u32) {
        self.max_frame_size = max_message_size.saturating_add(FRAME_OVERHEAD);
    }

    /// Splits the next complete frame off the front of `buf`, or returns `None` until one has arrived.
    ///
    /// An error means the stream can't be parsed any further: the connection must be closed.
    pub fn decode(&self, buf: &mut BytesMut) -> Result<Option<Frame>, FrameError> {
        if buf.len() < 4 {
            return Ok(None);
        }
        let total = u32::from_be_bytes(buf[..4].try_into().unwrap());
        if total > self.max_frame_size {
            return Err(FrameError::TooLarge { size: u64::from(total), limit: self.max_frame_size });
        }
        if total < 4 {
            return Err(FrameError::Malformed("total size smaller than the command size field"));
        }
        if buf.len() < 4 + total as usize {
            buf.reserve(4 + total as usize - buf.len());
            return Ok(None);
        }
        buf.advance(4);
        let frame = buf.split_to(total as usize).freeze();
        parse_frame(frame).map(Some)
    }
}

fn parse_frame(mut frame: Bytes) -> Result<Frame, FrameError> {
    let command_size = frame.get_u32() as usize;
    if command_size > frame.len() {
        return Err(FrameError::Malformed("command size larger than the frame"));
    }
    let command = frame.split_to(command_size);
    if frame.is_empty() {
        return Ok(Frame { command, payload: None });
    }

    let mut broker_entry_metadata = None;
    if peek_u16(&frame) == Some(MAGIC_BROKER_ENTRY_METADATA) {
        frame.advance(2);
        let size = read_len(&mut frame, "broker entry metadata")?;
        broker_entry_metadata = Some(frame.split_to(size));
    }

    let mut checksum_valid = None;
    if peek_u16(&frame) == Some(MAGIC_CRC32C) {
        frame.advance(2);
        if frame.len() < 4 {
            return Err(FrameError::Malformed("truncated checksum"));
        }
        let expected = frame.get_u32();
        checksum_valid = Some(crc32c::crc32c(&frame) == expected);
    }

    let metadata_size = read_len(&mut frame, "message metadata")?;
    let metadata = frame.split_to(metadata_size);
    Ok(Frame {
        command,
        payload: Some(PayloadSection { broker_entry_metadata, checksum_valid, metadata, payload: frame }),
    })
}

fn peek_u16(buf: &Bytes) -> Option<u16> {
    (buf.len() >= 2).then(|| u16::from_be_bytes([buf[0], buf[1]]))
}

fn read_len(buf: &mut Bytes, what: &'static str) -> Result<usize, FrameError> {
    if buf.len() < 4 {
        return Err(FrameError::Malformed(what));
    }
    let size = buf.get_u32() as usize;
    if size > buf.len() {
        return Err(FrameError::Malformed(what));
    }
    Ok(size)
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::proto::{CommandPing, CommandSend, base_command::Type};
    use buffa::MessageView;

    fn ping() -> BaseCommand {
        BaseCommand { r#type: Type::PING.into(), ping: CommandPing::default().into(), ..Default::default() }
    }

    #[test]
    fn simple_command_layout() {
        let mut buf = BytesMut::new();
        encode_command(&ping(), &mut buf).unwrap();
        // BaseCommand{type: PING(18), ping: {}} is 08 12 92 01 00: 5 bytes (ping is field 18).
        assert_eq!(&buf[..], &[0, 0, 0, 9, 0, 0, 0, 5, 0x08, 18, 0x92, 0x01, 0x00]);
    }

    #[test]
    fn decodes_split_reads_and_back_to_back_frames() {
        let mut wire = BytesMut::new();
        encode_command(&ping(), &mut wire).unwrap();
        encode_command(&ping(), &mut wire).unwrap();
        let decoder = FrameDecoder::default();
        let mut buf = BytesMut::new();
        let mut frames = Vec::new();
        for byte in wire {
            buf.put_u8(byte);
            while let Some(frame) = decoder.decode(&mut buf).unwrap() {
                frames.push(frame);
            }
        }
        assert_eq!(frames.len(), 2);
        assert!(buf.is_empty());
        let view = frames[0].command_view().unwrap();
        assert_eq!(view.r#type, buffa::EnumValue::Known(Type::PING));
        assert!(frames[0].payload.is_none());
    }

    #[test]
    fn payload_command_round_trip_with_checksum() {
        let send = BaseCommand {
            r#type: Type::SEND.into(),
            send: CommandSend { producer_id: 7, sequence_id: 42, num_messages: Some(1), ..Default::default() }.into(),
            ..Default::default()
        };
        let metadata = MessageMetadata {
            producer_name: "p".into(),
            sequence_id: 42,
            publish_time: 1_763_456_789_123,
            num_messages_in_batch: Some(1),
            ..Default::default()
        };
        let mut buf = BytesMut::new();
        encode_payload_command(&send, &metadata, b"hello", &mut buf).unwrap();

        let frame = FrameDecoder::default().decode(&mut buf).unwrap().unwrap();
        let section = frame.payload.clone().unwrap();
        assert_eq!(section.checksum_valid, Some(true));
        assert_eq!(&section.payload[..], b"hello");
        let decoded = crate::proto::__buffa::view::MessageMetadataView::decode_view(&section.metadata).unwrap();
        assert_eq!(decoded.producer_name, "p");
        assert_eq!(decoded.num_messages_in_batch(), 1);
        let command = frame.command_view().unwrap();
        assert_eq!(command.send.as_option().unwrap().sequence_id, 42);
    }

    #[test]
    fn corrupt_payload_fails_the_checksum() {
        let send = BaseCommand { r#type: Type::SEND.into(), ..Default::default() };
        let mut buf = BytesMut::new();
        encode_payload_command(&send, &MessageMetadata::default(), b"hello", &mut buf).unwrap();
        let last = buf.len() - 1;
        buf[last] ^= 0xff;
        let frame = FrameDecoder::default().decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.payload.unwrap().checksum_valid, Some(false));
    }

    #[test]
    fn broker_entry_metadata_is_split_off() {
        // A frame as the broker sends it to a consumer: command, broker entry metadata, then checksum section.
        let command = ping();
        let mut inner = BytesMut::new();
        encode_payload_command(&command, &MessageMetadata::default(), b"x", &mut inner).unwrap();
        let command_size = u32::from_be_bytes(inner[4..8].try_into().unwrap()) as usize;
        let (head, tail) = inner.split_at(8 + command_size);
        let entry_metadata = [0x08, 0x01];
        let mut wire = BytesMut::new();
        wire.put_u32((head.len() - 4 + 2 + 4 + entry_metadata.len() + tail.len()) as u32);
        wire.put_slice(&head[4..]);
        wire.put_u16(MAGIC_BROKER_ENTRY_METADATA);
        wire.put_u32(entry_metadata.len() as u32);
        wire.put_slice(&entry_metadata);
        wire.put_slice(tail);

        let frame = FrameDecoder::default().decode(&mut wire).unwrap().unwrap();
        let section = frame.payload.unwrap();
        assert_eq!(section.broker_entry_metadata.as_deref(), Some(&entry_metadata[..]));
        assert_eq!(section.checksum_valid, Some(true));
        assert_eq!(&section.payload[..], b"x");
    }

    #[test]
    fn oversized_frame_is_rejected_before_it_is_buffered() {
        let mut buf = BytesMut::from(&[0x7f, 0xff, 0xff, 0xff][..]);
        assert!(matches!(FrameDecoder::new(1024).decode(&mut buf), Err(FrameError::TooLarge { .. })));
    }

    #[test]
    fn unknown_command_type_stays_unknown() {
        // type = 999 (not a known command), then nothing else.
        let mut buf = BytesMut::new();
        buf.put_u32(4 + 3);
        buf.put_u32(3);
        buf.put_slice(&[0x08, 0xe7, 0x07]);
        let frame = FrameDecoder::default().decode(&mut buf).unwrap().unwrap();
        assert_eq!(frame.command_view().unwrap().r#type, buffa::EnumValue::Unknown(999));
    }
}
