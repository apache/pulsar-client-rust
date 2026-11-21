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

//! The producer's codecs: what each writes, a consumer reads back, in the formats Java's readers take.

use apache_pulsar_protocol::compression::{compress, supports};
use apache_pulsar_protocol::entry::decode;
use apache_pulsar_protocol::frame::PayloadSection;
use apache_pulsar_protocol::proto::{CommandMessage, CompressionType, MessageIdData, MessageMetadata};
use buffa::Message;
use bytes::Bytes;

const CODECS: [CompressionType; 4] =
    [CompressionType::LZ4, CompressionType::ZLIB, CompressionType::ZSTD, CompressionType::SNAPPY];

/// What a consumer reads from an entry of one message, `payload` compressed with `codec`.
fn read(codec: CompressionType, compressed: Bytes, size: usize) -> Bytes {
    let metadata =
        MessageMetadata { compression: Some(codec), uncompressed_size: Some(size as u32), ..Default::default() };
    let section = PayloadSection {
        broker_entry_metadata: None,
        checksum_valid: Some(true),
        metadata: metadata.encode_to_vec().into(),
        payload: compressed,
    };
    let message = CommandMessage { message_id: MessageIdData::default().into(), ..Default::default() };
    decode(0, &message, &section).unwrap().messages.remove(0).payload
}

#[test]
fn every_codec_round_trips() {
    let payload = Bytes::from("x".repeat(4096) + "and something else, then more x".repeat(10).as_str());
    assert!(supports(CompressionType::NONE));
    assert_eq!(compress(CompressionType::NONE, payload.clone()), payload);
    for codec in CODECS {
        assert!(supports(codec), "{codec:?}");
        let compressed = compress(codec, payload.clone());
        assert!(compressed.len() < payload.len() / 10, "{codec:?}: {} bytes", compressed.len());
        assert_eq!(read(codec, compressed, payload.len()), payload, "{codec:?}");
    }
}

/// Java's `Deflater` writes a stream it never finishes; this one is finished, with its final block and checksum, as
/// zlib readers expect. Java's, C++'s and Go's readers, which stop at the recorded size, take both.
#[test]
fn the_zlib_stream_is_finished() {
    let payload = Bytes::from("a zlib stream ".repeat(100));
    let compressed = compress(CompressionType::ZLIB, payload.clone());
    let mut out = Vec::with_capacity(payload.len());
    let mut inflater = flate2::Decompress::new(true);
    let status = inflater.decompress_vec(&compressed, &mut out, flate2::FlushDecompress::Finish).unwrap();
    assert_eq!(status, flate2::Status::StreamEnd);
    assert_eq!(out, payload);
    assert_eq!(inflater.total_in(), compressed.len() as u64, "nothing after the end of the stream");
}
