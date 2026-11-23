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

//! The blocking API, for code without an async runtime: each call blocks its thread until the operation completes.
//! Don't call it from an async task, which would block the task's executor too.
//!
//! `cargo run --example blocking`. `PULSAR_URL` sets another service URL, and the first argument another topic.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{Schema, blocking};

fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/blocking".into());

    let client = blocking::PulsarClient::builder().service_url(url).build()?;
    // Subscribed first: a new subscription starts at the latest message by default.
    let consumer =
        client.new_queue_consumer(Schema::string()).topic(&topic).subscription_name("blocking").subscribe()?;
    let producer = client.new_producer(Schema::string()).topic(&topic).create()?;

    for i in 0..10 {
        // A blocking send sends its batch at once, as Java's does, so a loop of them doesn't wait out the batching
        // delay for each.
        let id = producer.new_message().key(format!("key-{}", i % 3)).value(format!("message {i}")).send()?;
        println!("stored message {i} as {id}");
    }

    while let Some(message) = consumer.receive_timeout(Duration::from_secs(5))? {
        println!("{:?}: {}", message.key(), message.value()?);
        consumer.acknowledge(message.id())?;
    }

    producer.close()?;
    consumer.close()?;
    client.close()?;
    Ok(())
}
