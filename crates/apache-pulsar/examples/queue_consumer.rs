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

//! A Queue consumer: a Shared subscription on every segment of the topic. The consumers of a subscription share its
//! messages, each one acknowledged on its own; a negative acknowledgment asks for a message again.
//!
//! `cargo run --example queue_consumer`, with `cargo run --example producer` before or meanwhile: it reads the
//! subscription from its start, and stops once no message comes for 10 s. `PULSAR_URL` sets another service URL, and
//! the first argument another topic.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{PulsarClient, Schema, SubscriptionInitialPosition};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/orders".into());

    let client = PulsarClient::builder().service_url(url).build().await?;
    let consumer = client
        .new_queue_consumer(Schema::string())
        .topic(&topic)
        .subscription_name("order-workers")
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .negative_ack_redelivery_delay(Duration::from_secs(1))
        .subscribe()
        .await?;

    while let Some(message) = consumer.receive_timeout(Duration::from_secs(10)).await? {
        let order = message.value()?;
        // Work that fails now and may succeed later: the broker delivers the message again after the delay, with its
        // redelivery count one higher.
        if message.redelivery_count() == 0 && order.ends_with("user-2") {
            println!("{order}: try again later");
            consumer.negative_acknowledge(message.id())?;
            continue;
        }
        println!("{order} (key {:?}, redelivery count {})", message.key(), message.redelivery_count());
        // The acknowledgment goes out with the others of its group, within 100 ms by default.
        consumer.acknowledge(message.id()).await?;
    }

    // Sends the acknowledgments still waiting, and leaves the subscription's messages to its other consumers.
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
