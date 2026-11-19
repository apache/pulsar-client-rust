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

//! Key hashing for routing (docs/6-scalable-topics.md §3.1, *client-behavior* §2).
//!
//! One 32-bit MurmurHash3 (x86, seed 0) of the key's UTF-8 bytes drives both rings: the high 16 bits pick the
//! segment, the low 16 bits pick the entry bucket within it. The v4 partition routing of an all-legacy layout
//! uses the same hash with bit 31 cleared, modulo the partition count. Both match the Java client exactly, which
//! `tests/routing_vectors.rs` checks against vectors generated from it.

const C1: u32 = 0xcc9e_2d51;
const C2: u32 = 0x1b87_3593;

/// The raw 32-bit MurmurHash3 of `key`, seed 0. Java's `Murmur3_32Hash.makeRawHash`.
pub fn murmur3_32(key: &[u8]) -> u32 {
    let mut h1: u32 = 0;
    let (chunks, tail) = key.as_chunks::<4>();
    for chunk in chunks {
        h1 ^= mix_k1(u32::from_le_bytes(*chunk));
        h1 = h1.rotate_left(13).wrapping_mul(5).wrapping_add(0xe654_6b64);
    }
    let mut k1: u32 = 0;
    for (i, byte) in tail.iter().enumerate() {
        k1 |= u32::from(*byte) << (8 * i);
    }
    h1 ^= mix_k1(k1);
    h1 ^= key.len() as u32;
    fmix(h1)
}

fn mix_k1(k1: u32) -> u32 {
    k1.wrapping_mul(C1).rotate_left(15).wrapping_mul(C2)
}

fn fmix(mut h: u32) -> u32 {
    h ^= h >> 16;
    h = h.wrapping_mul(0x85eb_ca6b);
    h ^= h >> 13;
    h = h.wrapping_mul(0xc2b2_ae35);
    h ^ (h >> 16)
}

/// A key's position on both 16-bit rings, computed once per key.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub struct KeyHash(u32);

impl KeyHash {
    /// Hashes a key's UTF-8 bytes.
    pub fn of(key: &[u8]) -> Self {
        Self(murmur3_32(key))
    }

    /// The raw 32-bit hash.
    pub fn raw(self) -> u32 {
        self.0
    }

    /// The segment-routing hash: the high 16 bits.
    pub fn segment_hash(self) -> u16 {
        (self.0 >> 16) as u16
    }

    /// The entry-bucket hash (PIP-486): the low 16 bits.
    pub fn bucket_hash(self) -> u16 {
        self.0 as u16
    }

    /// The v4 partition of the key among `partitions` partitions: Java's
    /// `signSafeMod(Murmur3_32Hash.makeHash(key), partitions)`, where `makeHash` clears bit 31.
    ///
    /// # Panics
    ///
    /// If `partitions` is 0.
    pub fn partition(self, partitions: u32) -> u32 {
        (self.0 & 0x7fff_ffff) % partitions
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn reference_values() {
        // Reference MurmurHash3_x86_32 values, seed 0.
        assert_eq!(murmur3_32(b""), 0);
        assert_eq!(murmur3_32(b"hello"), 0x248b_fa47);
        assert_eq!(murmur3_32(b"The quick brown fox jumps over the lazy dog"), 0x2e4f_f723);
    }
}
