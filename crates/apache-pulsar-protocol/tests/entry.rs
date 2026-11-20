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

//! Entries as a consumer reads them: single messages, batches, what the broker acknowledged already, the four
//! codecs, and the entries that can't be read.

use std::io::Write;

use apache_pulsar_protocol::entry::{DecodeError, Decoded, decode};
use apache_pulsar_protocol::frame::PayloadSection;
use apache_pulsar_protocol::message_id::{EntryId, MessageId};
use apache_pulsar_protocol::proto::command_ack::ValidationError;
use apache_pulsar_protocol::proto::{
    CommandMessage, CompressionType, EncryptionKeys, KeyValue, MessageIdData, MessageMetadata, SingleMessageMetadata,
};
use buffa::Message;
use bytes::{BufMut, Bytes, BytesMut};

fn kv(key: &str, value: &str) -> KeyValue {
    KeyValue { key: key.into(), value: value.into(), ..Default::default() }
}

fn command(ack_set: Vec<i64>) -> CommandMessage {
    CommandMessage {
        consumer_id: 1,
        message_id: MessageIdData { ledgerId: 10, entryId: 20, ..Default::default() }.into(),
        redelivery_count: Some(2),
        ack_set,
        ..Default::default()
    }
}

fn section(metadata: &MessageMetadata, payload: impl Into<Bytes>) -> PayloadSection {
    PayloadSection {
        broker_entry_metadata: None,
        checksum_valid: Some(true),
        metadata: metadata.encode_to_vec().into(),
        payload: payload.into(),
    }
}

/// A batch's payload: each message's metadata, length-prefixed, then its value.
fn batch(messages: &[(SingleMessageMetadata, &str)]) -> Bytes {
    let mut out = BytesMut::new();
    for (single, value) in messages {
        let single = SingleMessageMetadata { payload_size: value.len() as i32, ..single.clone() };
        out.put_u32(single.encoded_len());
        out.put_slice(&single.encode_to_vec());
        out.put_slice(value.as_bytes());
    }
    out.freeze()
}

fn batch_metadata(count: i32) -> MessageMetadata {
    MessageMetadata {
        producer_name: "p".into(),
        sequence_id: 40,
        publish_time: 1_000,
        num_messages_in_batch: Some(count),
        ..Default::default()
    }
}

fn values(decoded: &Decoded) -> Vec<&str> {
    decoded.messages.iter().map(|m| std::str::from_utf8(&m.payload).unwrap()).collect()
}

#[test]
fn a_single_message() {
    let metadata = MessageMetadata {
        producer_name: "p".into(),
        sequence_id: 7,
        publish_time: 1_000,
        partition_key: Some("k".into()),
        ordering_key: Some(vec![1, 2]),
        properties: vec![kv("a", "1")],
        event_time: Some(5),
        ..Default::default()
    };
    let decoded = decode(3, &command(Vec::new()), &section(&metadata, "hello")).unwrap();
    assert_eq!((values(&decoded), decoded.skipped), (vec!["hello"], 0));
    let message = &decoded.messages[0];
    let id = EntryId { ledger_id: 10, entry_id: 20, partition: -1, batch_index: -1, batch_size: 0 };
    assert_eq!(message.id, MessageId::new(3, id));
    assert_eq!(message.id.entry.batch_size, 0);
    assert_eq!((message.key.as_deref(), message.ordering_key.as_deref()), (Some("k"), Some(&[1u8, 2][..])));
    assert_eq!((message.sequence_id, message.event_time, message.redelivery_count), (7, Some(5), 2));
    assert_eq!((message.properties.clone(), message.null_value), (vec![kv("a", "1")], false));
    assert_eq!((message.entry.producer_name.as_str(), message.entry.publish_time), ("p", 1_000));
}

#[test]
fn a_batch_is_split_into_its_messages() {
    let first = SingleMessageMetadata {
        partition_key: Some("a".into()),
        properties: vec![kv("n", "0")],
        event_time: Some(9),
        sequence_id: Some(40),
        ..Default::default()
    };
    // No sequence id of its own: it follows the entry's.
    let second = SingleMessageMetadata { null_value: Some(true), ..Default::default() };
    let payload = batch(&[(first, "zero"), (second, "")]);
    let decoded = decode(3, &command(Vec::new()), &section(&batch_metadata(2), payload)).unwrap();
    assert_eq!((values(&decoded), decoded.skipped), (vec!["zero", ""], 0));
    let [zero, one] = &decoded.messages[..] else { unreachable!() };
    assert_eq!((zero.id.entry.batch_index, zero.id.entry.batch_size, one.id.entry.batch_index), (0, 2, 1));
    assert_eq!(
        (zero.key.as_deref(), zero.event_time, zero.sequence_id, zero.null_value),
        (Some("a"), Some(9), 40, false)
    );
    assert_eq!(zero.properties, vec![kv("n", "0")]);
    assert_eq!((one.key.as_deref(), one.sequence_id, one.null_value), (None, 41, true));
    assert_eq!(one.entry.producer_name, "p");
}

/// The broker marks the indexes already acknowledged in `ack_set` (a cleared bit), and compaction marks the
/// messages it removed: neither is delivered, and both count as skipped.
#[test]
fn acknowledged_and_compacted_messages_are_left_out() {
    let compacted = SingleMessageMetadata { compacted_out: Some(true), ..Default::default() };
    let payload =
        batch(&[(Default::default(), "a"), (Default::default(), "b"), (compacted, "c"), (Default::default(), "d")]);
    let decoded = decode(3, &command(vec![0b1101]), &section(&batch_metadata(4), payload)).unwrap();
    assert_eq!((values(&decoded), decoded.skipped), (vec!["a", "d"], 2));
    // An index past the end of the bitset is acknowledged, as a Java `BitSet` reads it.
    let messages: Vec<(SingleMessageMetadata, &str)> = (0..66).map(|_| (Default::default(), "m")).collect();
    let decoded = decode(3, &command(vec![-1]), &section(&batch_metadata(66), batch(&messages))).unwrap();
    assert_eq!((decoded.messages.len(), decoded.skipped), (64, 2));
}

#[test]
fn every_codec_decompresses() {
    let raw = batch(&[(Default::default(), "one"), (Default::default(), "two")]);
    let mut zlib = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    zlib.write_all(&raw).unwrap();
    let codecs = [
        (CompressionType::LZ4, lz4_flex::block::compress(&raw)),
        (CompressionType::ZLIB, zlib.finish().unwrap()),
        (CompressionType::ZSTD, zstd::bulk::compress(&raw, 3).unwrap()),
        (CompressionType::SNAPPY, snap::raw::Encoder::new().compress_vec(&raw).unwrap()),
    ];
    for (codec, compressed) in codecs {
        let metadata = MessageMetadata {
            compression: Some(codec),
            uncompressed_size: Some(raw.len() as u32),
            ..batch_metadata(2)
        };
        let decoded = decode(3, &command(Vec::new()), &section(&metadata, compressed.clone())).unwrap();
        assert_eq!(values(&decoded), ["one", "two"], "{codec:?}");
        // A size that isn't the one recorded, or a stream that isn't the codec's, fails.
        let wrong_size = MessageMetadata { uncompressed_size: Some(raw.len() as u32 + 1), ..metadata.clone() };
        let error = decode(3, &command(Vec::new()), &section(&wrong_size, compressed)).unwrap_err();
        assert_eq!(error, DecodeError::Decompression, "{codec:?}");
        let error = decode(3, &command(Vec::new()), &section(&metadata, "not compressed")).unwrap_err();
        assert_eq!(error, DecodeError::Decompression, "{codec:?}");
    }
}

#[test]
fn entries_that_cant_be_read() {
    let fails =
        |metadata: &MessageMetadata, payload: Bytes| decode(3, &command(Vec::new()), &section(metadata, payload));
    let single = MessageMetadata::default();
    let mut corrupt = section(&single, "x");
    corrupt.checksum_valid = Some(false);
    assert_eq!(decode(3, &command(Vec::new()), &corrupt).unwrap_err(), DecodeError::Checksum);
    let mut garbled = section(&single, "x");
    garbled.metadata = Bytes::from_static(&[0xff, 0xff]);
    assert_eq!(decode(3, &command(Vec::new()), &garbled).unwrap_err(), DecodeError::Metadata);
    let encrypted = MessageMetadata { encryption_keys: vec![EncryptionKeys::default()], ..Default::default() };
    assert_eq!(fails(&encrypted, "x".into()).unwrap_err(), DecodeError::Encrypted);
    let chunk = MessageMetadata { num_chunks_from_msg: Some(2), ..Default::default() };
    assert_eq!(fails(&chunk, "x".into()).unwrap_err(), DecodeError::Chunked);
    // A lone chunk is a whole message.
    let whole = MessageMetadata { num_chunks_from_msg: Some(1), ..Default::default() };
    assert_eq!(values(&fails(&whole, "x".into()).unwrap()), ["x"]);

    // Batches cut short or with sizes that don't hold.
    let full = batch(&[(Default::default(), "abc")]);
    for cut in [2, 5, full.len() - 1] {
        assert_eq!(fails(&batch_metadata(1), full.slice(..cut)).unwrap_err(), DecodeError::Batch, "cut at {cut}");
    }
    let mut not_metadata = BytesMut::new();
    not_metadata.put_u32(2);
    not_metadata.put_slice(&[0xff, 0xff]);
    assert_eq!(fails(&batch_metadata(1), not_metadata.freeze()).unwrap_err(), DecodeError::Batch);
    let negative = SingleMessageMetadata { payload_size: -1, ..Default::default() };
    let mut negative_size = BytesMut::new();
    negative_size.put_u32(negative.encoded_len());
    negative_size.put_slice(&negative.encode_to_vec());
    assert_eq!(fails(&batch_metadata(1), negative_size.freeze()).unwrap_err(), DecodeError::Batch);
    assert_eq!(fails(&batch_metadata(-1), full).unwrap_err(), DecodeError::Batch);

    // A corrupt entry is dropped with the reason; one this client can't read is left for another consumer.
    let reasons = [
        (DecodeError::Checksum, Some(ValidationError::ChecksumMismatch)),
        (DecodeError::Metadata, Some(ValidationError::ChecksumMismatch)),
        (DecodeError::Decompression, Some(ValidationError::DecompressionError)),
        (DecodeError::Batch, Some(ValidationError::BatchDeSerializeError)),
        (DecodeError::Encrypted, None),
        (DecodeError::Chunked, None),
        (DecodeError::Codec(CompressionType::LZ4), None),
    ];
    for (error, validation) in reasons {
        assert_eq!(error.validation_error(), validation, "{error}");
    }
}
