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

//! A Stream consumer: the topic's controller spreads the topic's segments over the consumers of a subscription, so
//! that each key's messages go to one consumer, in order. One cumulative acknowledgment covers everything the
//! consumer received before, on every segment.
//!
//! `cargo run --example stream_consumer -- <topic> <name>`; run it twice with two names, and the two share the
//! topic. It reads the subscription from its start, and stops once no message comes for 10 s. `PULSAR_URL` sets
//! another service URL.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{PulsarClient, Schema, SubscriptionInitialPosition};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let mut args = std::env::args().skip(1);
    let topic = args.next().unwrap_or_else(|| "topic://public/default/orders".into());
    let name = args.next().unwrap_or_else(|| "billing-1".into());

    let client = PulsarClient::builder().service_url(url).build().await?;
    let consumer = client
        .new_stream_consumer(Schema::string())
        .topic(&topic)
        .subscription_name("billing")
        // The controller knows the consumer by its name: one that comes back under it soon gets its segments back.
        .consumer_name(name)
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await?;
    println!("subscribed as {}", consumer.consumer_name());

    let mut last = None;
    let mut unacknowledged = 0;
    while let Some(message) = consumer.receive_timeout(Duration::from_secs(10)).await? {
        println!("{:?}: {}", message.key(), message.value()?);
        unacknowledged += 1;
        // One acknowledgment for every 100 messages: each covers every message received before it.
        if unacknowledged == 100 {
            consumer.acknowledge_cumulative(message.id()).await?;
            unacknowledged = 0;
        }
        last = Some(message);
    }
    if let Some(message) = last {
        consumer.acknowledge_cumulative(message.id()).await?;
    }

    // Leaves the controller's group, which hands this consumer's segments to the others at once.
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
