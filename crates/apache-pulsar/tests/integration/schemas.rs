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

//! Schemas against the broker: C11's `IncompatibleSchema`, and each schema with the Java client, in both directions,
//! typed and as generic records.

use std::collections::BTreeMap;
use std::fmt::Debug;
use std::str::FromStr;
use std::time::Duration;

use apache_avro::types::Value;
use apache_pulsar::{
    ErrorKind, GenericValue, KeyValue, ProducerAccessMode, Schema, SchemaType, SubscriptionInitialPosition, local,
};
use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use buffa::Message as _;
use buffa_descriptor::generated::descriptor::{FileDescriptorProto, FileDescriptorSet};

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
    ("records_cross_between_java_and_rust", records_cross_between_java_and_rust),
    ("avro_records_evolve_across_a_split", avro_records_evolve_across_a_split),
    ("protobuf_schemas_declare_what_java_declares", protobuf_schemas_declare_what_java_declares),
    ("protobuf_messages_cross_between_java_and_rust", protobuf_messages_cross_between_java_and_rust),
    ("generic_records_cross_between_java_and_rust", generic_records_cross_between_java_and_rust),
    ("a_generic_consumer_decodes_each_message_by_its_version", a_generic_consumer_decodes_each_message_by_its_version),
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

/// A record of the JSON and Avro schemas, as the Java tool's `Pojo`: Java's nullable `String` is an `Option`.
#[derive(Debug, Clone, PartialEq, serde::Serialize, serde::Deserialize, apache_avro::AvroSchema)]
struct Pojo {
    name: Option<String>,
    age: i32,
}

/// A `Pojo` as the Java tool takes and prints it: `name:age`, `null` for no name.
fn pojo(text: &str) -> Pojo {
    let (name, age) = text.split_once(':').unwrap();
    Pojo { name: (name != "null").then(|| name.to_string()), age: age.parse().unwrap() }
}

/// Records of the JSON and the Avro schemas, both ways, Java's described by its reflection and Rust's by its derive:
/// the broker checks each against the other, and the fields come in another order.
fn records_cross_between_java_and_rust(env: &Env) {
    let records = ["alice:30", "bob:42", "null:7"];
    both_ways(env, "json", Schema::<Pojo>::json(), &records, pojo);
    both_ways(env, "avro", Schema::<Pojo>::avro(), &records, pojo);
}

/// The second version of Java's V5 evolution test's record, which adds a nullable field.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, apache_avro::AvroSchema)]
struct V2 {
    i: i32,
    j: Option<i32>,
}

/// A second version that requires its new field, which a record of the first can't fill.
#[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, apache_avro::AvroSchema)]
struct Bad {
    i: i32,
    j: i32,
}

/// Java's `V5SchemaEvolutionTest`, across the two clients: under BACKWARD, Java writes the first version, the segment
/// splits, and Rust registers the second on the topic's one history and writes it to the children; a consumer of the
/// second version, Rust's and Java's, reads each record with its version's schema, the first version's with the new
/// field null. A version that requires its new field is refused, and isn't registered.
fn avro_records_evolve_across_a_split(env: &Env) {
    let namespace = format!("public/{}", unique("evolution"));
    env.admin(&["namespaces", "create", &namespace]);
    env.admin(&["namespaces", "set-schema-compatibility-strategy", &namespace, "--compatibility", "BACKWARD"]);
    let path = format!("{namespace}/{}", unique("records"));
    env.admin(&["scalable-topics", "create", "-s", "1", &path]);
    let topic = format!("topic://{path}");
    env.java_produce("avro-v1", &topic, &["1", "2"]);
    env.admin(&["scalable-topics", "split-segment", "-s", "0", &path]);
    let expected = [V2 { i: 1, j: None }, V2 { i: 2, j: None }, V2 { i: 3, j: Some(30) }, V2 { i: 4, j: Some(40) }];
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(Schema::<V2>::avro()).topic(&topic).create().await.unwrap();
        for (key, record) in [("a", V2 { i: 3, j: Some(30) }), ("b", V2 { i: 4, j: Some(40) })] {
            producer.new_message().key(key).value(record).send().await.unwrap();
        }
        let bad = client.new_producer(Schema::<Bad>::avro()).topic(&topic).create().await.unwrap();
        assert_eq!(bad.send(Bad { i: 5, j: 50 }).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let consumer = queue(&client, Schema::<V2>::avro(), &topic, "rust").await.unwrap();
        let mut read = take(&consumer, 4).await;
        read.sort_by_key(|record| record.i);
        assert_eq!(read, expected);
        client.close().await.unwrap();
    });
    let mut read = env.java_consume("avro-v2", &topic, 4);
    read.sort();
    assert_eq!(read, ["1:null", "2:null", "3:30", "4:40"]);
    let versions = env.admin(&["schemas", "get", "-a", &topic]);
    assert_eq!(
        versions.matches("\"type\" : \"AVRO\"").count(),
        2,
        "Java's first version and Rust's second:\n{versions}"
    );
}

/// A schema as the Java tool prints it: its name, type, definition and properties.
fn java_info(text: &str) -> (String, String, String, BTreeMap<String, String>) {
    let line = |prefix: &str| text.lines().find_map(|line| line.strip_prefix(prefix)).unwrap_or_default().to_string();
    let properties = text.lines().filter_map(|line| line.strip_prefix("property ")?.split_once('='));
    let properties = properties.map(|(key, value)| (key.to_string(), value.to_string())).collect();
    (line("name "), line("type "), line("definition "), properties)
}

/// The files of a descriptor set, by name.
fn files(set: &[u8]) -> BTreeMap<String, FileDescriptorProto> {
    let set = FileDescriptorSet::decode_from_slice(set).unwrap();
    set.file.into_iter().map(|file| (file.name.clone().unwrap_or_default(), file)).collect()
}

/// A `PROTOBUF_NATIVE` definition: its descriptor set's files, and the message's and its file's names.
fn native(definition: &str) -> (BTreeMap<String, FileDescriptorProto>, String, String) {
    let json: serde_json::Value = serde_json::from_str(definition).unwrap();
    let set = STANDARD.decode(json["fileDescriptorSet"].as_str().unwrap()).unwrap();
    let name = |key: &str| json[key].as_str().unwrap().to_string();
    (files(&set), name("rootMessageTypeName"), name("rootFileDescriptorName"))
}

/// Rust declares what Java 5.0.1 declares for the same message, `PROTOBUF_NATIVE` as v4's `Schema.PROTOBUF_NATIVE`:
/// the same descriptors, names and properties. For buffa's well-known types, with nesting, enums and imports, and
/// for a prost type given the descriptor set Java declared.
fn protobuf_schemas_declare_what_java_declares(env: &Env) {
    let declared = |message: &str, info: &apache_pulsar::SchemaInfo| {
        let java = java_info(&env.java_schema_info("protobuf-native", message));
        assert_eq!((java.0.as_str(), java.1.as_str()), (info.name(), "PROTOBUF_NATIVE"), "{message}");
        assert_eq!(native(&java.2), native(std::str::from_utf8(info.definition()).unwrap()), "{message}");
        assert_eq!(&java.3, info.properties(), "{message}");
        java.2
    };
    declared("timestamp", Schema::<buffa_types::google::protobuf::Timestamp>::protobuf_native().info());
    declared("struct", Schema::<buffa_types::google::protobuf::Struct>::protobuf_native().info());
    declared("type", Schema::<buffa_types::google::protobuf::Type>::protobuf_native().info());
    let java = declared("duration", Schema::<buffa_types::google::protobuf::Duration>::protobuf_native().info());
    let json: serde_json::Value = serde_json::from_str(&java).unwrap();
    let set = STANDARD.decode(json["fileDescriptorSet"].as_str().unwrap()).unwrap();
    declared("duration", Schema::<prost_types::Duration>::prost_native(&set).unwrap().info());
}

/// A `google.protobuf.Timestamp` written as `seconds:nanos`, as the Java tool takes and prints it.
fn timestamp(text: &str) -> buffa_types::google::protobuf::Timestamp {
    let (seconds, nanos) = text.split_once(':').unwrap();
    buffa_types::google::protobuf::Timestamp {
        seconds: seconds.parse().unwrap(),
        nanos: nanos.parse().unwrap(),
        ..Default::default()
    }
}

/// `PROTOBUF_NATIVE` messages both ways, of a buffa type and of a prost type, typed and as generic records, with
/// Java's v4 schema of the same message: the broker checks each side's descriptors against the other's.
fn protobuf_messages_cross_between_java_and_rust(env: &Env) {
    let times = ["5:7", "0:0", "-1:999999999"];
    let schema = Schema::<buffa_types::google::protobuf::Timestamp>::protobuf_native();
    both_ways(env, "protobuf-native", schema.clone(), &times, timestamp);
    let json: serde_json::Value = serde_json::from_slice(schema.info().definition()).unwrap();
    let set = STANDARD.decode(json["fileDescriptorSet"].as_str().unwrap()).unwrap();
    let prost = Schema::<prost_types::Timestamp>::prost_native(&set).unwrap();
    let prost_timestamp = |text: &str| {
        let time = timestamp(text);
        prost_types::Timestamp { seconds: time.seconds, nanos: time.nanos }
    };
    both_ways(env, "protobuf-native", prost, &times, prost_timestamp);

    let topic = new_topic("java-generic-protobuf");
    env.java_produce("protobuf-native", &topic, &["5:7"]);
    run(async {
        let client = client(env).await;
        let consumer = queue(&client, Schema::auto_consume(), &topic, "rust").await.unwrap();
        let record = take(&consumer, 1).await.remove(0);
        assert_eq!(record.schema_type(), SchemaType::ProtobufNative);
        assert_eq!(record.fields(), ["seconds", "nanos"]);
        assert_eq!(record.field("seconds"), Some(GenericValue::Protobuf(buffa_descriptor::Value::I64(5))));
        assert_eq!(record.field("nanos"), Some(GenericValue::Protobuf(buffa_descriptor::Value::I32(7))));
        client.close().await.unwrap();
    });
    let topic = new_topic("rust-generic-protobuf");
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(schema).topic(&topic).create().await.unwrap();
        producer.send(timestamp("5:7")).await.unwrap();
        client.close().await.unwrap();
    });
    assert_eq!(env.java_consume("auto", &topic, 1), ["PROTOBUF_NATIVE seconds: 5 nanos: 7"]);
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

/// Generic records, both ways: a Rust consumer of `Schema::auto_consume()` reads what Java writes with each schema, and
/// a Java consumer of `Schema.autoConsume()` what Rust writes, each decoding with the schema the broker recorded.
fn generic_records_cross_between_java_and_rust(env: &Env) {
    let string = GenericValue::String("hello".to_string());
    generic_both_ways(env, "string", Schema::string(), "hello", "hello".to_string(), string, "STRING hello");
    generic_both_ways(env, "bool", Schema::bool(), "true", true, GenericValue::Boolean(true), "BOOLEAN true");
    generic_both_ways(env, "int8", Schema::int8(), "-7", -7, GenericValue::Int8(-7), "INT8 -7");
    generic_both_ways(env, "int16", Schema::int16(), "12345", 12345, GenericValue::Int16(12345), "INT16 12345");
    generic_both_ways(env, "int32", Schema::int32(), "-123", -123, GenericValue::Int32(-123), "INT32 -123");
    let long = GenericValue::Int64(1 << 40);
    generic_both_ways(env, "int64", Schema::int64(), "1099511627776", 1 << 40, long, "INT64 1099511627776");
    generic_both_ways(env, "float32", Schema::float32(), "1.5", 1.5, GenericValue::Float(1.5), "FLOAT 1.5");
    generic_both_ways(env, "float64", Schema::float64(), "-2.5", -2.5, GenericValue::Double(-2.5), "DOUBLE -2.5");
    let schema = Schema::key_value(Schema::string(), Schema::int32());
    let pair = KeyValue { key: "a".to_string(), value: 1 };
    let generic = KeyValue { key: GenericValue::String("a".to_string()), value: GenericValue::Int32(1) };
    generic_both_ways(env, "kv", schema, "a=1", pair, GenericValue::KeyValue(Box::new(generic)), "KEY_VALUE a=1");
    // Records, each read with its writer's schema: Java's orders the fields of the same record otherwise.
    let alice = Pojo { name: Some("alice".to_string()), age: 30 };
    let json = GenericValue::Json(serde_json::json!({"age": 30, "name": "alice"}));
    let java = r#"JSON {"name":"alice","age":30}"#;
    generic_both_ways(env, "json", Schema::<Pojo>::json(), "alice:30", alice.clone(), json, java);
    let name = Value::Union(1, Box::new(Value::String("alice".to_string())));
    let avro = GenericValue::Avro(Value::Record(vec![("age".to_string(), Value::Int(30)), ("name".to_string(), name)]));
    let java = r#"AVRO {"name": "alice", "age": 30}"#;
    generic_both_ways(env, "avro", Schema::<Pojo>::avro(), "alice:30", alice, avro, java);
}

/// Sends `value` with `schema` from Java, as the tool takes it (`text`), and reads it in Rust as a generic record,
/// `generic`; then sends it from Rust, and reads it in Java as a generic record, which the tool prints as `java`.
fn generic_both_ways<T: 'static>(
    env: &Env,
    name: &str,
    schema: Schema<T>,
    text: &str,
    value: T,
    generic: GenericValue,
    java: &str,
) {
    let topic = new_topic(&format!("java-generic-{name}"));
    env.java_produce(name, &topic, &[text]);
    let schema_type = schema.info().schema_type();
    run(async {
        let client = client(env).await;
        let consumer = queue(&client, Schema::auto_consume(), &topic, "rust").await.unwrap();
        let record = take(&consumer, 1).await.remove(0);
        assert_eq!((record.schema_type(), record.into_value()), (schema_type, generic), "{name}");
        client.close().await.unwrap();
    });

    let topic = new_topic(&format!("rust-generic-{name}"));
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(schema).topic(&topic).create().await.unwrap();
        producer.send(value).await.unwrap();
        client.close().await.unwrap();
    });
    assert_eq!(env.java_consume("auto", &topic, 1), [java], "{name}");
}

/// A generic consumer decodes each message with the schema of its version: on a topic with a schema, the messages of
/// a producer of raw bytes have none, and read as bytes. It asks for the schema on the service URL, here the proxy's.
/// The broker doesn't count a generic consumer as it checks a schema: the first typed consumer of a new topic still
/// registers its own, and a topic with messages but no schema takes a generic consumer.
fn a_generic_consumer_decodes_each_message_by_its_version(env: &Env) {
    let topic = new_topic("generic-versions");
    run(async {
        let client = client(env).await;
        let proxied = crate::proxy_client(env).build().await.unwrap();
        let generic = queue(&proxied, Schema::auto_consume(), &topic, "generic").await.unwrap();
        let _typed = queue(&client, Schema::int32(), &topic, "typed").await.unwrap();
        let text = client.new_producer(Schema::string()).topic(&topic).create().await.unwrap();
        assert_eq!(text.send("x".to_string()).await.unwrap_err().kind(), ErrorKind::IncompatibleSchema);
        let raw = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        let numbers = client.new_producer(Schema::int32()).topic(&topic).create().await.unwrap();
        raw.send(b"raw".to_vec()).await.unwrap();
        numbers.send(7).await.unwrap();
        let read: Vec<(SchemaType, GenericValue)> =
            take(&generic, 2).await.into_iter().map(|record| (record.schema_type(), record.into_value())).collect();
        let bytes = (SchemaType::Bytes, GenericValue::Bytes(b"raw".to_vec()));
        assert!(read.contains(&bytes) && read.contains(&(SchemaType::Int32, GenericValue::Int32(7))), "{read:?}");
        proxied.close().await.unwrap();
        client.close().await.unwrap();
    });

    let topic = new_topic("generic-no-schema");
    run(async {
        let client = client(env).await;
        let producer = client.new_producer(Schema::bytes()).topic(&topic).create().await.unwrap();
        producer.send(b"raw".to_vec()).await.unwrap();
        let generic = queue(&client, Schema::auto_consume(), &topic, "generic").await.unwrap();
        let record = take(&generic, 1).await.remove(0);
        assert_eq!(
            (record.schema_type(), record.into_value()),
            (SchemaType::Bytes, GenericValue::Bytes(b"raw".to_vec()))
        );
        client.close().await.unwrap();
    });
}
