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
//! built-in schema through a producer and a consumer, one schema for a topic with its version on every message, and
//! C11's `IncompatibleSchema`, for producers and for each kind of consumer.

use apache_pulsar_protocol::proto::schema::Type;

use crate::error::ErrorKind;
use apache_pulsar_testkit::scalable::World;

use crate::producer_tests::{TOPIC, WorldServer, local_client, run};
use crate::{KeyValue, ProducerAccessMode, Schema, SubscriptionInitialPosition, local};

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
