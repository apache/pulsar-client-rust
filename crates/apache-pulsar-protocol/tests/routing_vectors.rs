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

//! Routing hashes against vectors generated from the Java client (tools/java-vectors).

use apache_pulsar_protocol::hash::KeyHash;
use serde::Deserialize;

#[derive(Deserialize)]
struct Vectors {
    partition_counts: Vec<u32>,
    cases: Vec<Case>,
}

#[derive(Deserialize)]
struct Case {
    key: String,
    key_utf8_hex: String,
    raw_hash: i32,
    masked_hash: i32,
    segment_hash: u16,
    bucket_hash: u16,
    partitions: Vec<u32>,
}

fn unhex(hex: &str) -> Vec<u8> {
    (0..hex.len()).step_by(2).map(|i| u8::from_str_radix(&hex[i..i + 2], 16).unwrap()).collect()
}

#[test]
fn routing_matches_java() {
    let vectors: Vectors = serde_json::from_str(include_str!("vectors/routing.json")).unwrap();
    assert!(vectors.cases.len() > 100, "too few routing vectors");
    assert!(vectors.cases.iter().any(|c| c.raw_hash < 0), "no vector with bit 31 set");
    for case in &vectors.cases {
        assert_eq!(case.key.as_bytes(), unhex(&case.key_utf8_hex), "key {:?}: UTF-8 differs", case.key);
        let hash = KeyHash::of(case.key.as_bytes());
        assert_eq!(hash.raw() as i32, case.raw_hash, "key {:?}: raw hash", case.key);
        assert_eq!((hash.raw() & 0x7fff_ffff) as i32, case.masked_hash, "key {:?}: masked hash", case.key);
        assert_eq!(hash.segment_hash(), case.segment_hash, "key {:?}: segment hash", case.key);
        assert_eq!(hash.bucket_hash(), case.bucket_hash, "key {:?}: bucket hash", case.key);
        for (n, expected) in vectors.partition_counts.iter().zip(&case.partitions) {
            assert_eq!(hash.partition(*n), *expected, "key {:?}: partition of {n}", case.key);
        }
    }
}
