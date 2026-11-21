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

//! The producer's compression against the simulated world. Ported scenarios from Java's
//! `V5ProducerCompressionTest`: a payload compressed with each codec, or none, reads back as it was sent. Then a batch
//! compressed whole, and the broker's limit on the compressed entry.

use std::time::Duration;

use apache_pulsar_protocol::producer::{Batching, OutgoingMessage, ProducerOptions, SendError};
use apache_pulsar_protocol::proto::CompressionType;
use apache_pulsar_testkit::producer::{Harness, options};
use apache_pulsar_testkit::scalable::World;

const TOPIC: &str = "topic://acme/orders/events";

/// Java's: redundant enough to compress.
const PAYLOAD: &str = "xxxxxxxxxxxxxxxx";

fn world() -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    world
}

fn compressed_with(codec: CompressionType) -> ProducerOptions {
    ProducerOptions { compression: codec, ..options() }
}

fn values(h: &Harness) -> Vec<String> {
    h.world.messages(TOPIC).iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}

/// Java's `testCompressionDisabled`, `testCompressionLz4`, `testCompressionZlib`, `testCompressionZstd`,
/// `testCompressionSnappy` and `testNoCompressionPolicySetUsesDefault`.
#[test]
fn a_payload_reads_back_with_each_codec() {
    let payload = PAYLOAD.repeat(256);
    for codec in [
        CompressionType::NONE,
        CompressionType::LZ4,
        CompressionType::ZLIB,
        CompressionType::ZSTD,
        CompressionType::SNAPPY,
    ] {
        let mut h = Harness::new(world(), TOPIC, compressed_with(codec));
        let send = h.send(Some("k"), &payload);
        assert!(h.result(send).unwrap().is_ok(), "{codec:?}");
        assert_eq!(values(&h), std::slice::from_ref(&payload), "{codec:?}");
        let stored = &h.world.stored(TOPIC)[0];
        if codec == CompressionType::NONE {
            assert_eq!((stored.metadata.compression, stored.metadata.uncompressed_size), (None, None));
        } else {
            // The batch payload, the message's own metadata included, compressed.
            let size = stored.metadata.uncompressed_size.unwrap() as usize;
            assert_eq!(stored.metadata.compression, Some(codec));
            assert!(size > payload.len() && stored.payload.len() < size / 10, "{codec:?}: {}", stored.payload.len());
        }
    }
    // A producer that sets no policy doesn't compress.
    assert_eq!(options().compression, CompressionType::NONE);
}

/// A batch is compressed whole: its messages share one compressed payload.
#[test]
fn a_batch_is_compressed_whole() {
    let batching = Batching { max_messages: 100, max_bytes: 0, max_delay: Duration::from_millis(10) };
    let options = ProducerOptions { batching: Some(batching), ..compressed_with(CompressionType::ZSTD) };
    let mut h = Harness::new(world(), TOPIC, options);
    let sent: Vec<String> = (0..50).map(|i| format!("{PAYLOAD}-{i:02}")).collect();
    for value in &sent {
        h.send(Some("k"), value);
    }
    h.advance(Duration::from_millis(10));
    let stored = h.world.stored(TOPIC);
    assert_eq!(stored.len(), 1);
    assert_eq!(stored[0].metadata.num_messages_in_batch, Some(50));
    assert_eq!(stored[0].metadata.compression, Some(CompressionType::ZSTD));
    assert!(stored[0].payload.len() < stored[0].metadata.uncompressed_size.unwrap() as usize / 4);
    assert_eq!(values(&h), sent);
}

/// The broker's limit is on what it stores: a message over it that compresses below it is stored; one that doesn't
/// fails alone.
#[test]
fn the_brokers_limit_is_on_the_compressed_entry() {
    let mut h = Harness::new(world(), TOPIC, compressed_with(CompressionType::LZ4));
    let compressible = h.send_message(message(vec![0; 6 << 20]));
    assert!(h.result(compressible).unwrap().is_ok());
    let mut state = 0x9e37_79b9_7f4a_7c15_u64;
    let noise: Vec<u8> = (0..6 << 20)
        .map(|_| {
            // xorshift: bytes LZ4 can't shrink.
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            state as u8
        })
        .collect();
    let incompressible = h.send_message(message(noise));
    assert!(matches!(h.result(incompressible), Some(Err(SendError::TooLarge { .. }))));
    assert_eq!(h.world.stored(TOPIC).len(), 1);
}

fn message(payload: Vec<u8>) -> OutgoingMessage {
    OutgoingMessage { key: Some("k".into()), payload: payload.into(), ..Default::default() }
}
