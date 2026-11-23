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

//! Schemas: a struct's values as JSON and as Avro, a key/value pair, and generic records, which a consumer reads with
//! the schema each message was written with. JSON and Avro take the crate's `json` and `avro` features, on by default;
//! the struct derives `serde`'s traits and apache-avro's `AvroSchema`. Java's client reads and writes the same messages.
//!
//! `cargo run --example schemas`. Each schema has a topic of its own, the first argument and a suffix: a topic keeps one
//! history of schemas, and refuses a schema of another type. `PULSAR_URL` sets another service URL.

use std::error::Error;
use std::time::Duration;

use apache_avro::AvroSchema;
use apache_pulsar::{KeyValue, PulsarClient, Schema, SubscriptionInitialPosition};
use serde::{Deserialize, Serialize};

#[derive(Debug, Clone, Serialize, Deserialize, AvroSchema)]
struct Order {
    id: i64,
    user: String,
    total: f64,
    /// A nullable field, as Java's reflection makes one of a class.
    note: Option<String>,
}

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let base = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/schemas".into());
    let client = PulsarClient::builder().service_url(url).build().await?;
    let order = Order { id: 1, user: "user-42".into(), total: 99.5, note: None };

    // JSON, through serde_json, with the record described to the broker in Avro, as Java's `Schema.json` does.
    let json = round_trip(&client, Schema::<Order>::json(), &format!("{base}-json"), order.clone()).await?;
    println!("JSON: {json:?}");

    // Avro: each message is read with the schema version that wrote it, resolved to `Order`.
    let avro = round_trip(&client, Schema::<Order>::avro(), &format!("{base}-avro"), order.clone()).await?;
    println!("Avro: {avro:?}");

    // A key and a value in the payload, each with its own schema: Java's inline key/value encoding.
    let schema = Schema::key_value(Schema::string(), Schema::<Order>::avro());
    let pair = KeyValue { key: order.user.clone(), value: order };
    let pair = round_trip(&client, schema, &format!("{base}-key-value"), pair).await?;
    println!("key/value: {} => {:?}", pair.key, pair.value);

    // Generic records: a consumer that doesn't know `Order` reads the Avro topic, fetching each version's schema.
    let generic = client
        .new_queue_consumer(Schema::auto_consume())
        .topic(format!("{base}-avro"))
        .subscription_name("schemas-generic")
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    while let Some(message) = generic.receive_timeout(Duration::from_secs(5)).await? {
        let record = message.value()?;
        let (schema, fields, user) = (record.schema_type(), record.fields(), record.field("user"));
        println!("generic record, written with {schema:?}: fields {fields:?}, user {user:?}");
        generic.acknowledge(message.id()).await?;
    }
    generic.close().await?;
    client.close().await?;
    Ok(())
}

/// Sends `value` to `topic` with `schema`, and reads it back with the same schema. The consumer subscribes first, and
/// registers the schema on a new topic.
async fn round_trip<T: 'static>(
    client: &PulsarClient,
    schema: Schema<T>,
    topic: &str,
    value: T,
) -> Result<T, Box<dyn Error>> {
    let consumer = client
        .new_queue_consumer(schema.clone())
        .topic(topic)
        .subscription_name("schemas")
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    let producer = client.new_producer(schema).topic(topic).create().await?;
    producer.send(value).await?;
    let message = consumer.receive().await?;
    consumer.acknowledge(message.id()).await?;
    producer.close().await?;
    consumer.close().await?;
    Ok(message.value()?)
}
