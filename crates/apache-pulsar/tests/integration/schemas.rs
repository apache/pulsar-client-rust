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

//! Schemas against the broker: C11's `IncompatibleSchema`, and each schema with the Java client, in both directions.

use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;

use apache_pulsar::{ErrorKind, KeyValue, ProducerAccessMode, Schema, SubscriptionInitialPosition, local};

use crate::env::{self, Env};
use crate::{run, token_client, unique};

pub const TESTS: &[(&str, fn(&Env))] = &[
    ("a_schema_of_another_type_is_refused", a_schema_of_another_type_is_refused),
    ("the_first_consumer_of_a_new_topic_registers_its_schema", the_first_consumer_of_a_new_topic_registers_its_schema),
    (
        "a_topic_with_data_and_no_schema_refuses_a_typed_consumer",
        a_topic_with_data_and_no_schema_refuses_a_typed_consumer,
    ),
    ("primitive_values_cross_between_java_and_rust", primitive_values_cross_between_java_and_rust),
    ("key_values_cross_between_java_and_rust", key_values_cross_between_java_and_rust),
];

fn new_topic(name: &str) -> String {
    format!("topic://public/default/{}", unique(name))
}

async fn client(env: &Env) -> local::PulsarClient {
    token_client(env::BROKER_URL, Some(env.token.clone())).build().await.unwrap()
}

/// A Queue consumer of `topic` from its start.
async fn queue<T: 'static>(
    client: &local::PulsarClient,
    schema: Schema<T>,
    topic: &str,
    subscription: &str,
) -> apache_pulsar::Result<local::QueueConsumer<T>> {
    let builder = client.new_queue_consumer(schema).topic(topic).subscription_name(subscription);
    builder.subscription_initial_position(SubscriptionInitialPosition::Earliest).subscribe().await
}

/// Takes `count` messages from a consumer, acknowledging each, and returns their values.
async fn take<T: 'static>(consumer: &local::QueueConsumer<T>, count: usize) -> Vec<T> {
    let mut values = Vec::new();
    while values.len() < count {
        let message = consumer.receive_timeout(Duration::from_secs(30)).await.unwrap();
        let message = message.unwrap_or_else(|| panic!("received {} of {count} messages", values.len()));
        values.push(message.value().unwrap());
        consumer.acknowledge(message.id()).await.unwrap();
    }
    values
}

/// C11: a schema of another type than the topic's fails with `IncompatibleSchema`: a Shared producer's message, as
/// it registers on the segment with its first message there, as Java's does; an exclusive producer's create; and each
/// kind of consumer's subscribe. The topic's own type, and raw bytes, still read it.
fn a_schema_of_another_type_is_refused(env: &Env) {
    let topic = new_topic("schema-refused");
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        let shared = client.new_producer(Schema::int32()).topic(&topic).create().await.unwrap();
        assert_eq!(shared.send(1).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let exclusive = client.new_producer(Schema::int32()).topic(&topic).access_mode(ProducerAccessMode::Exclusive);
        assert_eq!(exclusive.create().await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let refused = queue(&client, Schema::int32(), &topic, "queue").await.unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::IncompatibleSchema, "{refused}");
        let stream = client.new_stream_consumer(Schema::int32()).topic(&topic).subscription_name("stream");
        let refused = stream.subscribe().await.unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::IncompatibleSchema, "{refused}");
        let refused = client.new_checkpoint_consumer(Schema::int32()).topic(&topic).create().await.unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::IncompatibleSchema, "{refused}");
        let typed = queue(&client, Schema::string(), &topic, "typed").await.unwrap();
        assert_eq!(take(&typed, 1).await, ["first"]);
        let raw = queue(&client, Schema::bytes(), &topic, "raw").await.unwrap();
        assert_eq!(take(&raw, 1).await, [b"first".to_vec()]);
        client.close().await.unwrap();
    });
}

/// A consumer that subscribes to a topic before any producer registers its schema there, which then refuses a
/// producer of another type.
fn the_first_consumer_of_a_new_topic_registers_its_schema(env: &Env) {
    let topic = new_topic("schema-consumer-first");
    run(async {
        let client = client(env).await;
        let consumer = queue(&client, Schema::int64(), &topic, "sub").await.unwrap();
        let text = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        assert_eq!(text.send("x".to_string()).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let numbers = client.new_producer(Schema::int64()).topic(&topic).create().await.unwrap();
        numbers.send(5).await.unwrap();
        assert_eq!(take(&consumer, 1).await, [5]);
        client.close().await.unwrap();
    });
}

/// A topic that has messages but no schema refuses a consumer with a schema, as the broker adds no schema to an active
/// topic. Raw bytes still read it.
fn a_topic_with_data_and_no_schema_refuses_a_typed_consumer(env: &Env) {
    let topic = new_topic("schema-none");
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        producer.send(b"raw".to_vec()).await.unwrap();
        let refused = queue(&client, Schema::string(), &topic, "typed").await.unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::IncompatibleSchema, "{refused}");
        let raw = queue(&client, Schema::bytes(), &topic, "raw").await.unwrap();
        assert_eq!(take(&raw, 1).await, [b"raw".to_vec()]);
        client.close().await.unwrap();
    });
}

/// Values of string and of each primitive schema, from a Rust producer to a Java consumer, and from a Java producer to
/// a Rust consumer. The Java tool takes and prints the values as Java writes them.
fn primitive_values_cross_between_java_and_rust(env: &Env) {
    both_ways(env, "string", Schema::string(), &["hello", "with spaces"], parsed);
    both_ways(env, "bool", Schema::bool(), &["true", "false"], parsed);
    both_ways(env, "int8", Schema::int8(), &["-128", "0", "127"], parsed);
    both_ways(env, "int16", Schema::int16(), &["-32768", "12345", "32767"], parsed);
    both_ways(env, "int32", Schema::int32(), &["-2147483648", "123456789", "2147483647"], parsed);
    let longs = ["-9223372036854775808", "1234567890123", "9223372036854775807"];
    both_ways(env, "int64", Schema::int64(), &longs, parsed);
    both_ways(env, "float32", Schema::float32(), &["-1.5", "1.1", "3.4028235E38"], parsed);
    both_ways(env, "float64", Schema::float64(), &["-2.5", "1.1", "1.7976931348623157E308"], parsed);
}

/// Key/value pairs of a string and an int32, inline in the payload. Java V5's factory has no key/value schema, so the
/// Java tool takes v4's, through `V5Interop`.
fn key_values_cross_between_java_and_rust(env: &Env) {
    let schema = Schema::key_value(Schema::string(), Schema::int32());
    let pair = |text: &str| {
        let (key, value) = text.split_once('=').unwrap();
        KeyValue { key: key.to_string(), value: value.parse().unwrap() }
    };
    both_ways(env, "kv", schema, &["a=1", "b=-2", "=0"], pair);
}

/// A value as Java prints it, parsed.
fn parsed<T: FromStr<Err: Debug>>(text: &str) -> T {
    text.parse().unwrap()
}

/// Sends `values` with `schema` from Rust and reads them with the Java schema named `name`, then the other way, each
/// on a topic of its own. Messages without a key spread over the segments, so the values may come in another order.
/// `parse` reads a value as the Java tool takes and prints it.
fn both_ways<T>(env: &Env, name: &str, schema: Schema<T>, values: &[&str], parse: impl Fn(&str) -> T)
where
    T: Clone + Debug + PartialEq + 'static,
{
    let expected: Vec<T> = values.iter().map(|value| parse(value)).collect();
    let same = |got: &[T]| got.len() == expected.len() && expected.iter().all(|value| got.contains(value));

    let topic = new_topic(&format!("rust-{name}"));
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(schema.clone()).topic(&topic).create().await.unwrap();
        for value in &expected {
            producer.send(value.clone()).await.unwrap();
        }
        client.close().await.unwrap();
    });
    let read: Vec<T> = env.java_consume(name, &topic, values.len()).iter().map(|value| parse(value)).collect();
    assert!(same(&read), "{name}: Java read {read:?} of {expected:?}");

    let topic = new_topic(&format!("java-{name}"));
    env.java_produce(name, &topic, values);
    run(async {
        let client = client(env).await;
        let consumer = queue(&client, schema, &topic, "rust").await.unwrap();
        let read = take(&consumer, values.len()).await;
        assert!(same(&read), "{name}: Rust read {read:?} of {expected:?}");
        client.close().await.unwrap();
    });
}
