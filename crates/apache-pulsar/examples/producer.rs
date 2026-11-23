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

//! A producer on the default, thread-safe API, from a Tokio runtime: keyed messages with properties, a flush and a
//! close.
//!
//! Against a broker on this machine: `cargo run --example producer`. `PULSAR_URL` sets another service URL, and the
//! first argument another topic.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{PulsarClient, Schema};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/orders".into());

    // One client per application: it is cheap to clone, and its futures run on any executor.
    let client = PulsarClient::builder().service_url(url).build().await?;
    let producer = client
        .new_producer(Schema::string())
        .topic(&topic)
        .producer_name("orders-writer")
        .send_timeout(Duration::from_secs(30))
        .create()
        .await?;

    // The messages of a key are stored in the order they are sent, across the topic's splits and merges.
    for (i, user) in ["user-1", "user-2", "user-1"].into_iter().enumerate() {
        let id = producer
            .new_message()
            .key(user)
            .value(format!("order {i} of {user}"))
            .property("source", "web")
            .send()
            .await?;
        println!("stored order {i} of {user} as {id}");
    }

    // A message with only a value. Each send above waits for its message to be stored, and so for the batching
    // delay too: pipelining.rs has many messages in flight at once.
    let id = producer.send("no key".into()).await?;
    println!("stored a message without a key as {id}");

    // Every message sent before is stored once the flush completes; closing doesn't fail them.
    producer.flush().await?;
    producer.close().await?;
    client.close().await?;
    Ok(())
}
