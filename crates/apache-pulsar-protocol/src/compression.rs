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

//! The compression codecs (docs/3-protocol.md §6), each behind a feature, all on by default.
//!
//! A producer compresses an entry's payload whole, a batch's every message with it, and records the codec and the
//! uncompressed size in the entry's metadata; a consumer decompresses it to that size. The formats are Java's: LZ4
//! and Snappy blocks, without a frame, ZSTD frames, at Java's level, and ZLIB streams with their header. Java's ZLIB
//! writer flushes its stream and never finishes it, so a reader decodes the recorded size and no further; this one
//! finishes its stream, which every reader takes.

use bytes::Bytes;

use crate::entry::DecodeError;
use crate::proto::CompressionType;

/// Java's ZSTD level.
#[cfg(feature = "zstd")]
const ZSTD_LEVEL: i32 = 3;

/// Whether this build has `codec`.
pub fn supports(codec: CompressionType) -> bool {
    match codec {
        CompressionType::NONE => true,
        CompressionType::LZ4 => cfg!(feature = "lz4"),
        CompressionType::ZLIB => cfg!(feature = "zlib"),
        CompressionType::ZSTD => cfg!(feature = "zstd"),
        CompressionType::SNAPPY => cfg!(feature = "snappy"),
    }
}

/// An entry's payload compressed with `codec`, which this build must have ([`supports`]); as it is without one.
#[cfg_attr(not(any(feature = "lz4", feature = "zlib", feature = "zstd", feature = "snappy")), allow(unreachable_code))]
pub fn compress(codec: CompressionType, payload: Bytes) -> Bytes {
    let compressed: Vec<u8> = match codec {
        CompressionType::NONE => return payload,
        #[cfg(feature = "lz4")]
        CompressionType::LZ4 => lz4_flex::block::compress(&payload),
        #[cfg(feature = "zlib")]
        CompressionType::ZLIB => deflate(&payload),
        #[cfg(feature = "zstd")]
        CompressionType::ZSTD => zstd::bulk::compress(&payload, ZSTD_LEVEL).expect("ZSTD compresses in memory"),
        #[cfg(feature = "snappy")]
        CompressionType::SNAPPY => snap::raw::Encoder::new().compress_vec(&payload).expect("an entry fits Snappy"),
        #[cfg(not(all(feature = "lz4", feature = "zlib", feature = "zstd", feature = "snappy")))]
        codec => unreachable!("this build leaves out {codec:?}"),
    };
    Bytes::from(compressed)
}

/// A finished ZLIB stream, at the default level, as Java's `Deflater` has it: with the zlib header, the final block
/// and the checksum.
#[cfg(feature = "zlib")]
fn deflate(payload: &[u8]) -> Vec<u8> {
    use std::io::Write;
    let mut encoder = flate2::write::ZlibEncoder::new(Vec::new(), flate2::Compression::default());
    encoder.write_all(payload).expect("writes to memory");
    encoder.finish().expect("writes to memory")
}

/// The payload as the producer had it, `size` bytes long.
#[cfg_attr(
    not(any(feature = "lz4", feature = "zlib", feature = "zstd", feature = "snappy")),
    allow(unreachable_code, unused_variables)
)]
pub(crate) fn decompress(codec: CompressionType, payload: Bytes, size: usize) -> Result<Bytes, DecodeError> {
    let decompressed: Option<Vec<u8>> = match codec {
        CompressionType::NONE => return Ok(payload),
        #[cfg(feature = "lz4")]
        CompressionType::LZ4 => lz4_flex::block::decompress(&payload, size).ok(),
        #[cfg(feature = "zlib")]
        CompressionType::ZLIB => inflate(&payload, size),
        #[cfg(feature = "zstd")]
        CompressionType::ZSTD => zstd::bulk::decompress(&payload, size).ok(),
        #[cfg(feature = "snappy")]
        CompressionType::SNAPPY => snap::raw::Decoder::new().decompress_vec(&payload).ok(),
        #[cfg(not(all(feature = "lz4", feature = "zlib", feature = "zstd", feature = "snappy")))]
        codec => return Err(DecodeError::Codec(codec)),
    };
    // Each codec checks its own stream, but not that it holds what the producer recorded.
    decompressed.filter(|out| out.len() == size).map(Bytes::from).ok_or(DecodeError::Decompression)
}

/// ZLIB, as Java's `Deflater` writes it: with the zlib header, and flushed but never finished, so without the final
/// block and checksum. As Java's `Inflater` does, it reads the size recorded and no further, which takes a finished
/// stream too.
#[cfg(feature = "zlib")]
fn inflate(payload: &[u8], size: usize) -> Option<Vec<u8>> {
    use std::io::Read;
    let mut out = vec![0; size];
    flate2::read::ZlibDecoder::new(payload).read_exact(&mut out).ok()?;
    Some(out)
}
