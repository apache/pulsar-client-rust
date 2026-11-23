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

//! A Checkpoint consumer: no subscription on the brokers. The application keeps where it is as a `Checkpoint`, here
//! in a file, and a consumer created at it reads on from there: run it twice, and the second run reads only what
//! came after the first.
//!
//! `cargo run --example checkpoint_consumer`, with `cargo run --example producer` before. It stops once no message
//! comes for 5 s. `PULSAR_URL` sets another service URL, the first argument another topic, and the second the file.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{Checkpoint, PulsarClient, Schema};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let mut args = std::env::args().skip(1);
    let topic = args.next().unwrap_or_else(|| "topic://public/default/orders".into());
    let file = args.next().map_or_else(|| std::env::temp_dir().join("apache-pulsar-orders.checkpoint"), Into::into);

    // Where the last run stopped, or the topic's start. Java's client reads and writes the same bytes.
    let start = match std::fs::read(&file) {
        Ok(bytes) => Checkpoint::from_bytes(&bytes)?,
        Err(_) => Checkpoint::earliest(),
    };
    println!("starting at {start:?}");

    let client = PulsarClient::builder().service_url(url).build().await?;
    let consumer =
        client.new_checkpoint_consumer(Schema::string()).topic(&topic).start_position(start).create().await?;

    // A key's messages come in the order they were published, across the topic's splits and merges.
    while let Some(message) = consumer.receive_timeout(Duration::from_secs(5)).await? {
        println!("{:?}: {}", message.key(), message.value()?);
    }

    // For each segment, the last message a receive returned: a consumer created at it reads on after them. It asks
    // nothing of the brokers.
    let checkpoint = consumer.checkpoint().await?;
    std::fs::write(&file, checkpoint.to_bytes())?;
    println!("saved {checkpoint:?} to {}", file.display());

    consumer.close().await?;
    client.close().await?;
    Ok(())
}
