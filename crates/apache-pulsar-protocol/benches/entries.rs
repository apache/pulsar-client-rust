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

//! Micro-benchmarks of the producer's and the consumer's work on entries, sans IO (docs/8-quality.md §6): the
//! producer's state machine batching and encoding messages, each codec compressing a batch, and the consumer's
//! decoding of it. `cargo bench -p apache-pulsar-protocol`; the baseline is in docs/8 §6.

use std::hint::black_box;
use std::rc::Rc;
use std::time::{Duration, Instant};

use apache_pulsar_protocol::compression::compress;
use apache_pulsar_protocol::entry::decode;
use apache_pulsar_protocol::frame::PayloadSection;
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::producer::{Action, Batching, OutgoingMessage, Producer, ProducerOptions};
use apache_pulsar_protocol::proto::{
    CommandMessage, CommandProducerSuccess, CommandSendReceipt, CompressionType, MessageIdData, MessageMetadata,
    ProducerAccessMode, ScalableTopicDAG, SegmentInfoProto, SegmentState, SingleMessageMetadata,
};
use buffa::Message;
use bytes::Bytes;
use criterion::{BenchmarkId, Criterion, Throughput, criterion_group, criterion_main};

const MESSAGES: usize = 1000;

/// A segment with the four entry buckets of a new topic.
fn layout() -> Rc<Layout> {
    let segment = SegmentInfoProto {
        segment_id: 0,
        hash_start: 0,
        hash_end: 0xffff,
        state: SegmentState::ACTIVE,
        entry_bucket_splits: vec![0x4000, 0x8000, 0xc000],
        ..Default::default()
    };
    let dag = ScalableTopicDAG { epoch: 1, segments: vec![segment], ..Default::default() };
    Rc::new(Layout::from_dag(&dag, "topic://bench/ns/topic").unwrap())
}

/// A producer whose segment's producer is registered, and the `(segment, generation)` of its registration.
fn registered(batching: Option<Batching>) -> (Producer, (u64, u64)) {
    let options = ProducerOptions {
        name: Some("bench".into()),
        access_mode: ProducerAccessMode::Shared,
        send_timeout: None,
        initial_sequence_id: None,
        properties: Vec::new(),
        schema: None,
        operation_timeout: Duration::from_secs(30),
        batching,
        compression: CompressionType::NONE,
    };
    let now = Instant::now();
    let mut producer = Producer::new(options, layout(), now);
    producer.send(OutgoingMessage::default(), 0, now).unwrap();
    let link = std::iter::from_fn(|| producer.poll_action())
        .find_map(|action| match action {
            Action::Connect { segment, generation, .. } => Some((segment, generation)),
            _ => None,
        })
        .unwrap();
    let success = CommandProducerSuccess { producer_name: "bench-seg-0".into(), ..Default::default() };
    producer.on_registered(link.0, link.1, 1, &success, 5 << 20, now);
    answer(&mut producer, link);
    (producer, link)
}

/// Answers every entry the producer wrote with its receipt, as the broker does.
fn answer(producer: &mut Producer, (segment, generation): (u64, u64)) {
    let now = Instant::now();
    let sent: Vec<u64> = std::iter::from_fn(|| producer.poll_action())
        .filter_map(|action| match action {
            Action::Send { command, .. } => Some(command.send.sequence_id),
            _ => None,
        })
        .collect();
    for sequence_id in sent {
        let message_id = MessageIdData { ledgerId: 1, entryId: sequence_id, ..Default::default() };
        let receipt = CommandSendReceipt { sequence_id, message_id: message_id.into(), ..Default::default() };
        producer.on_receipt(segment, generation, &receipt, now);
    }
    while producer.poll_action().is_some() {}
}

/// The producer's work for each message, from the send to its completion: routing, batching by bucket, encoding
/// the entries and matching their receipts. 16 keys, so every bucket has a batch.
fn producer(c: &mut Criterion) {
    let mut group = c.benchmark_group("producer");
    group.throughput(Throughput::Elements(MESSAGES as u64));
    let keys: Vec<String> = (0..16).map(|i| format!("key-{i}")).collect();
    for size in [100, 1024] {
        let payload = Bytes::from(vec![b'x'; size]);
        let batched = Batching { max_messages: 1000, max_bytes: 128 << 10, max_delay: Duration::from_millis(1) };
        for (name, batching) in [("batched", Some(batched)), ("unbatched", None)] {
            let (mut producer, link) = registered(batching);
            group.bench_function(BenchmarkId::new(name, size), |b| {
                b.iter(|| {
                    let now = Instant::now();
                    for i in 0..MESSAGES {
                        let message = OutgoingMessage {
                            key: Some(keys[i % keys.len()].clone()),
                            payload: payload.clone(),
                            ..Default::default()
                        };
                        black_box(producer.send(message, 0, now).unwrap());
                    }
                    producer.flush();
                    answer(&mut producer, link);
                })
            });
        }
    }
    group.finish();
}

/// A batch's payload as a producer encodes it: 1000 messages of about 100 bytes of JSON, as an application might
/// send, each after its own metadata.
fn batch_payload() -> Bytes {
    let mut out = Vec::new();
    for i in 0..MESSAGES {
        let value = format!(
            r#"{{"order":{i},"user":"user-{}","amount":{}.{:02},"currency":"EUR","status":"paid"}}"#,
            i % 97,
            i * 7 % 1000,
            i % 100
        );
        let single = SingleMessageMetadata {
            payload_size: value.len() as i32,
            sequence_id: Some(i as u64),
            ..Default::default()
        };
        out.extend_from_slice(&single.encoded_len().to_be_bytes());
        out.extend_from_slice(&single.encode_to_vec());
        out.extend_from_slice(value.as_bytes());
    }
    Bytes::from(out)
}

/// Each codec compressing a batch, and a consumer decompressing it and splitting it into its messages.
fn codecs(c: &mut Criterion) {
    let payload = batch_payload();
    let mut group = c.benchmark_group("codec");
    group.throughput(Throughput::Bytes(payload.len() as u64));
    for codec in [
        CompressionType::NONE,
        CompressionType::LZ4,
        CompressionType::ZLIB,
        CompressionType::ZSTD,
        CompressionType::SNAPPY,
    ] {
        let name = format!("{codec:?}");
        // Without a codec, the payload goes as it is: only its decoding is worth a look.
        if codec != CompressionType::NONE {
            group.bench_function(BenchmarkId::new("compress", &name), |b| {
                b.iter(|| black_box(compress(codec, black_box(payload.clone()))))
            });
        }
        let compressed = compress(codec, payload.clone());
        let metadata = MessageMetadata {
            compression: (codec != CompressionType::NONE).then_some(codec),
            uncompressed_size: Some(payload.len() as u32),
            num_messages_in_batch: Some(MESSAGES as i32),
            ..Default::default()
        };
        let section = PayloadSection {
            broker_entry_metadata: None,
            checksum_valid: Some(true),
            metadata: metadata.encode_to_vec().into(),
            payload: compressed,
        };
        let message = CommandMessage { message_id: MessageIdData::default().into(), ..Default::default() };
        group.bench_function(BenchmarkId::new("decode", &name), |b| {
            b.iter(|| black_box(decode(0, &message, &section).unwrap()))
        });
    }
    group.finish();
}

criterion_group!(benches, producer, codecs);
criterion_main!(benches);
