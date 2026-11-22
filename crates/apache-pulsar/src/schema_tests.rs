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

//! Schemas over real loopback sockets, against the simulated world of `apache-pulsar-testkit`: values of each
//! built-in schema through a producer and a consumer, one schema for a topic with its version on every message,
//! C11's `IncompatibleSchema`, for producers and for each kind of consumer, and generic records, decoded with the
//! schema each message was written with.

use std::time::Duration;

use apache_pulsar_protocol::proto::schema::Type;
use apache_pulsar_protocol::proto::{MessageMetadata, ServerError, base_command};
use bytes::Bytes;

use crate::error::ErrorKind;
use apache_pulsar_testkit::scalable::World;

use crate::producer_tests::{TOPIC, WorldServer, local_client, run};
use crate::{
    Checkpoint, GenericValue, KeyValue, ProducerAccessMode, Schema, SchemaType, SubscriptionInitialPosition, local,
};

/// A world with [`TOPIC`], and no schema registered yet.
fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

/// The types of the schemas registered on [`TOPIC`], oldest first.
fn schema_types(server: &WorldServer) -> Vec<Type> {
    server.world.borrow().schemas(TOPIC).iter().map(|schema| schema.r#type).collect()
}

/// A Queue consumer of [`TOPIC`] from its start.
async fn queue<T: 'static>(
    client: &local::PulsarClient,
    schema: Schema<T>,
    subscription: &str,
) -> crate::Result<local::QueueConsumer<T>> {
    let builder = client.new_queue_consumer(schema).topic(TOPIC).subscription_name(subscription);
    builder.subscription_initial_position(SubscriptionInitialPosition::Earliest).subscribe().await
}

/// Sends `value` with `schema` on a topic of its own, and reads it back with a consumer of the same schema, which
/// registered the schema as it subscribed first: its name, type, definition and properties.
async fn round_trip<T: Clone + PartialEq + std::fmt::Debug + 'static>(schema: Schema<T>, value: T) {
    let server = WorldServer::start(world(1)).await;
    let client = local_client(&server).await;
    let consumer = queue(&client, schema.clone(), "sub").await.unwrap();
    let producer = client.new_producer(schema.clone()).topic(TOPIC).create().await.unwrap();
    producer.send(value.clone()).await.unwrap();
    assert_eq!(consumer.receive().await.unwrap().value().unwrap(), value);
    assert_eq!(server.world.borrow().schemas(TOPIC), [schema.info().to_proto().unwrap()]);
    client.close().await.unwrap();
}

#[test]
fn values_of_each_schema_reach_a_consumer() {
    run(async {
        round_trip(Schema::bool(), true).await;
        round_trip(Schema::int8(), -7).await;
        round_trip(Schema::int16(), 12345).await;
        round_trip(Schema::int32(), 123_456_789).await;
        round_trip(Schema::int64(), 1_234_567_890_123).await;
        round_trip(Schema::float32(), 1.1).await;
        round_trip(Schema::float64(), 1.1).await;
        let pair = KeyValue { key: "k".to_string(), value: 7 };
        round_trip(Schema::key_value(Schema::string(), Schema::int64()), pair).await;
    });
}

/// The schema is the topic's, not a segment's: the producers of every segment, before and after a split, register
/// the same version, which each message carries. A producer of raw bytes declares no schema, and its messages carry
/// no version.
#[test]
fn a_topic_has_one_schema_and_its_messages_carry_the_version() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let a = client.new_producer(Schema::int32()).topic(TOPIC).create().await.unwrap();
        let b = client.new_producer(Schema::int32()).topic(TOPIC).create().await.unwrap();
        for i in 0..4 {
            a.send(i).await.unwrap();
            b.send(i).await.unwrap();
        }
        server.with(|w| w.split(TOPIC, 0));
        for i in 4..8 {
            a.send(i).await.unwrap();
        }
        let raw = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        raw.send(vec![0, 0, 0, 8]).await.unwrap();
        assert_eq!(schema_types(&server), [Type::Int32]);
        let messages = server.world.borrow().messages(TOPIC);
        let versions: Vec<Option<Vec<u8>>> = messages.iter().map(|m| m.entry.schema_version.clone()).collect();
        let first = Some(0u64.to_be_bytes().to_vec());
        assert_eq!(versions.iter().filter(|version| **version == first).count(), 12, "{versions:?}");
        assert_eq!(versions.iter().filter(|version| version.is_none()).count(), 1, "{versions:?}");
        let segments: std::collections::BTreeSet<i64> = messages.iter().map(|m| m.id.segment_id).collect();
        assert_eq!(segments.len(), 4, "the messages went to both segments, then to both children");
        client.close().await.unwrap();
    });
}

/// C11: a schema of another type than the topic's fails with `IncompatibleSchema`. A Shared producer registers on a
/// segment with its first message there, as Java V5's does, so the message fails, and so does the next: the producer
/// stays usable, and tries again. An exclusive producer registers as it's created, and each consumer as it subscribes.
#[test]
fn a_schema_of_another_type_is_refused() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        let shared = client.new_producer(Schema::int32()).topic(TOPIC).create().await.unwrap();
        assert_eq!(shared.send(1).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        assert_eq!(shared.send(2).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let exclusive = client.new_producer(Schema::int32()).topic(TOPIC).access_mode(ProducerAccessMode::Exclusive);
        assert_eq!(exclusive.create().await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        assert_eq!(queue(&client, Schema::int32(), "q").await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let stream = client.new_stream_consumer(Schema::int32()).topic(TOPIC).subscription_name("s").subscribe();
        assert_eq!(stream.await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let reader = client.new_checkpoint_consumer(Schema::int32()).topic(TOPIC).create();
        assert_eq!(reader.await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        // The topic's schema is as it was, and its type, or raw bytes, still read it.
        assert_eq!(schema_types(&server), [Type::String]);
        let typed = queue(&client, Schema::string(), "typed").await.unwrap();
        assert_eq!(typed.receive().await.unwrap().value().unwrap(), "first");
        let raw = queue(&client, Schema::bytes(), "raw").await.unwrap();
        assert_eq!(raw.receive().await.unwrap().value().unwrap(), b"first");
        client.close().await.unwrap();
    });
}

/// A consumer that subscribes to a topic before anything else registers its schema, which then refuses a producer
/// of another type.
#[test]
fn the_first_consumer_of_an_idle_topic_registers_its_schema() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = queue(&client, Schema::int64(), "sub").await.unwrap();
        assert_eq!(schema_types(&server), [Type::Int64]);
        let text = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        assert_eq!(text.send("x".to_string()).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let numbers = client.new_producer(Schema::int64()).topic(TOPIC).create().await.unwrap();
        numbers.send(5).await.unwrap();
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), 5);
        client.close().await.unwrap();
    });
}

/// A topic that has messages but no schema refuses a consumer with one, as the broker refuses to add a schema to an
/// active topic. Raw bytes still read it.
#[test]
fn a_topic_with_data_and_no_schema_refuses_a_typed_consumer() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        producer.send(b"raw".to_vec()).await.unwrap();
        assert_eq!(queue(&client, Schema::string(), "typed").await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let raw = queue(&client, Schema::bytes(), "raw").await.unwrap();
        assert_eq!(raw.receive().await.unwrap().value().unwrap(), b"raw");
        assert!(schema_types(&server).is_empty());
        client.close().await.unwrap();
    });
}

/// How many times the consumers asked the world for a schema.
fn schema_fetches(server: &WorldServer) -> usize {
    let log = &server.world.borrow().log;
    log.iter().filter(|(_, command)| command.r#type == base_command::Type::GET_SCHEMA).count()
}

/// Stores a one-message entry with a schema version, as another client would.
fn publish(server: &WorldServer, version: Option<&[u8]>, payload: Bytes) {
    let schema_version = version.map(<[u8]>::to_vec);
    let metadata =
        MessageMetadata { producer_name: "other".into(), publish_time: 1, schema_version, ..Default::default() };
    server.with(|w| w.publish_raw(TOPIC, 0, metadata, payload));
}

fn pair(key: GenericValue, value: GenericValue) -> GenericValue {
    GenericValue::KeyValue(Box::new(KeyValue { key, value }))
}

/// Sends `value` with `schema` on a topic of its own, and reads it back as a generic record, whose schema the consumer
/// fetched.
async fn generic_round_trip<T: 'static>(schema: Schema<T>, value: T, expected: GenericValue) {
    let server = WorldServer::start(world(1)).await;
    let client = local_client(&server).await;
    let producer = client.new_producer(schema.clone()).topic(TOPIC).create().await.unwrap();
    producer.send(value).await.unwrap();
    let consumer = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
    let record = consumer.receive().await.unwrap().value().unwrap();
    assert_eq!((record.schema_type(), record.into_value()), (schema.info().schema_type(), expected));
    assert_eq!(schema_fetches(&server), 1);
    client.close().await.unwrap();
}

#[test]
fn generic_records_of_each_schema() {
    run(async {
        generic_round_trip(Schema::string(), "text".to_string(), GenericValue::String("text".into())).await;
        generic_round_trip(Schema::bool(), true, GenericValue::Boolean(true)).await;
        generic_round_trip(Schema::int8(), -7, GenericValue::Int8(-7)).await;
        generic_round_trip(Schema::int16(), 12345, GenericValue::Int16(12345)).await;
        generic_round_trip(Schema::int32(), 123_456_789, GenericValue::Int32(123_456_789)).await;
        generic_round_trip(Schema::int64(), 1 << 40, GenericValue::Int64(1 << 40)).await;
        generic_round_trip(Schema::float32(), 1.5, GenericValue::Float(1.5)).await;
        generic_round_trip(Schema::float64(), -2.5, GenericValue::Double(-2.5)).await;
        let schema = Schema::key_value(Schema::string(), Schema::int64());
        let expected = pair(GenericValue::String("k".into()), GenericValue::Int64(7));
        generic_round_trip(schema, KeyValue { key: "k".to_string(), value: 7 }, expected).await;
    });
}

/// A generic consumer decodes each message with the schema of its version, which it fetches once: the versions of a
/// topic's history decode as each was written, and a message without a version, or of a version the topic doesn't
/// have, reads as bytes.
#[test]
fn each_message_is_decoded_with_the_schema_of_its_version() {
    run(async {
        let first = Schema::key_value(Schema::string(), Schema::int32());
        let second = Schema::key_value(first.clone(), Schema::bool());
        let mut world = world(1);
        // Two versions of one type, as after a change the broker found compatible.
        let v0 = world.register_schema(TOPIC, first.info().to_proto().unwrap());
        let v1 = world.register_schema(TOPIC, second.info().to_proto().unwrap());
        let server = WorldServer::start(world).await;
        let nested = KeyValue { key: KeyValue { key: "b".to_string(), value: 2 }, value: true };
        publish(&server, Some(&v0), first.encode(&KeyValue { key: "a".to_string(), value: 1 }).unwrap());
        publish(&server, Some(&v1), second.encode(&nested).unwrap());
        publish(&server, None, Bytes::from_static(b"raw"));
        publish(&server, Some(&v0), first.encode(&KeyValue { key: "c".to_string(), value: 3 }).unwrap());
        publish(&server, Some(&9u64.to_be_bytes()), Bytes::from_static(b"unknown"));
        let client = local_client(&server).await;
        let consumer = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
        let mut read = Vec::new();
        for _ in 0..5 {
            let record = consumer.receive().await.unwrap().value().unwrap();
            read.push((record.schema_type(), record.into_value()));
        }
        let kv = |key: &str, value| pair(GenericValue::String(key.into()), GenericValue::Int32(value));
        let expected = [
            (SchemaType::KeyValue, kv("a", 1)),
            (SchemaType::KeyValue, pair(kv("b", 2), GenericValue::Boolean(true))),
            (SchemaType::Bytes, GenericValue::Bytes(b"raw".to_vec())),
            (SchemaType::KeyValue, kv("c", 3)),
            (SchemaType::Bytes, GenericValue::Bytes(b"unknown".to_vec())),
        ];
        assert_eq!(read, expected);
        assert_eq!(schema_fetches(&server), 3, "one for each version");
        client.close().await.unwrap();
    });
}

/// The broker checks no generic consumer's schema, nor counts the consumer: the topic's first typed consumer still
/// registers its schema, and a topic with messages but no schema takes a generic consumer, which reads them as bytes.
#[test]
fn a_generic_consumer_reads_any_topic_and_registers_no_schema() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let _generic = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
        assert!(schema_types(&server).is_empty());
        let _typed = queue(&client, Schema::int64(), "typed").await.unwrap();
        assert_eq!(schema_types(&server), [Type::Int64]);
        client.close().await.unwrap();

        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        producer.send(b"raw".to_vec()).await.unwrap();
        let generic = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
        let record = generic.receive().await.unwrap().value().unwrap();
        assert_eq!(
            (record.schema_type(), record.into_value()),
            (SchemaType::Bytes, GenericValue::Bytes(b"raw".to_vec()))
        );
        assert_eq!(schema_fetches(&server), 0);
        client.close().await.unwrap();
    });
}

/// A fetch that the broker fails fails the value of the message that waited for it, which keeps its payload; the
/// next message of that version fetches again. A failure the client retries, such as a broker not ready, only
/// delays the message.
#[test]
fn a_failed_schema_fetch_fails_one_message() {
    run(async {
        let mut world = world(1);
        world.fail_schemas.extend([ServerError::ServiceNotReady, ServerError::MetadataError]);
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::int32()).topic(TOPIC).create().await.unwrap();
        producer.send(1).await.unwrap();
        producer.send(2).await.unwrap();
        let consumer = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
        let failed = consumer.receive().await.unwrap();
        assert!(failed.value().unwrap_err().to_string().contains("MetadataError"));
        assert_eq!(failed.data(), 1i32.to_be_bytes());
        let next = consumer.receive().await.unwrap().value().unwrap();
        assert_eq!(next.into_value(), GenericValue::Int32(2));
        assert_eq!(schema_fetches(&server), 3, "a retry, then a fetch again");
        client.close().await.unwrap();
    });
}

/// Messages wait for their schema in order: while the fetch is on its way, receives get nothing, and the messages
/// after come in order once it arrives. A consumer closed meanwhile closes.
#[test]
fn messages_wait_for_their_schema_in_order() {
    run(async {
        let mut world = world(1);
        world.hold_schemas = true;
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let raw = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        let typed = client.new_producer(Schema::int32()).topic(TOPIC).create().await.unwrap();
        raw.send(b"a".to_vec()).await.unwrap();
        typed.send(1).await.unwrap();
        raw.send(b"b".to_vec()).await.unwrap();
        let consumer = queue(&client, Schema::auto_consume(), "generic").await.unwrap();
        let first = consumer.receive().await.unwrap().value().unwrap();
        assert_eq!(first.into_value(), GenericValue::Bytes(b"a".to_vec()));
        let waiting = consumer.receive_batch(2, Duration::from_millis(100)).await.unwrap();
        assert!(waiting.is_empty(), "the second message waits for its schema, and the third behind it");
        server.with(World::release_schemas);
        let rest = consumer.receive_batch(2, Duration::from_secs(5)).await.unwrap();
        let values: Vec<GenericValue> = rest.iter().map(|message| message.value().unwrap().into_value()).collect();
        assert_eq!(values, [GenericValue::Int32(1), GenericValue::Bytes(b"b".to_vec())]);

        server.with(|w| w.hold_schemas = true);
        let late = queue(&client, Schema::auto_consume(), "late").await.unwrap();
        assert_eq!(late.receive().await.unwrap().value().unwrap().into_value(), GenericValue::Bytes(b"a".to_vec()));
        assert!(late.receive_timeout(Duration::from_millis(50)).await.unwrap().is_none());
        late.close().await.unwrap();
        server.with(World::release_schemas);
        client.close().await.unwrap();
    });
}

/// Stream and Checkpoint consumers fetch the schemas too.
#[test]
fn each_kind_of_consumer_reads_generic_records() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::int16()).topic(TOPIC).create().await.unwrap();
        producer.send(5).await.unwrap();
        let stream = client.new_stream_consumer(Schema::auto_consume()).topic(TOPIC).subscription_name("s");
        let stream = stream.subscription_initial_position(SubscriptionInitialPosition::Earliest).subscribe();
        let stream = stream.await.unwrap();
        assert_eq!(stream.receive().await.unwrap().value().unwrap().into_value(), GenericValue::Int16(5));
        let reader = client.new_checkpoint_consumer(Schema::auto_consume()).topic(TOPIC);
        let reader = reader.start_position(Checkpoint::earliest()).create().await.unwrap();
        assert_eq!(reader.receive().await.unwrap().value().unwrap().into_value(), GenericValue::Int16(5));
        client.close().await.unwrap();
    });
}

/// Producers don't take a generic schema, nor any consumer a key/value schema with a generic part, as it reads
/// key/value messages whole.
#[test]
fn generic_schemas_where_they_dont_serve() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::auto_consume()).topic(TOPIC).create().await;
        assert_eq!(producer.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        let part = || Schema::key_value(Schema::string(), Schema::auto_consume());
        assert_eq!(queue(&client, part(), "q").await.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        let stream = client.new_stream_consumer(part()).topic(TOPIC).subscription_name("s").subscribe();
        assert_eq!(stream.await.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        let reader = client.new_checkpoint_consumer(part()).topic(TOPIC).create();
        assert_eq!(reader.await.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        assert!(schema_types(&server).is_empty());
        client.close().await.unwrap();
    });
}
