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

//! Pipelining sends: many messages in flight from one task, stored in the order they were sent. A send queues its
//! message when its future is first polled, so a combinator that polls the futures in order keeps their order:
//! `buffered`, `FuturesOrdered` and `join_all`.
//!
//! Two ways lose it. Spawning each send as a task of its own: the tasks start in any order. And a send future dropped
//! after its first poll, as when a timeout around it expires, doesn't recall its message, which may still be stored:
//! only its result is lost. The producer's send timeout bounds how long a message waits.
//!
//! `cargo run --example pipelining`. `PULSAR_URL` sets another service URL, and the first argument another topic.

use std::error::Error;
use std::pin::pin;
use std::time::Instant;

use apache_pulsar::{PulsarClient, Schema};
use futures::future::join_all;
use futures::stream::{self, FuturesOrdered, StreamExt};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/pipelined".into());
    let client = PulsarClient::builder().service_url(url).build().await?;
    let producer = client.new_producer(Schema::string()).topic(&topic).create().await?;

    // A continuous source: up to 1000 sends in flight, their results in send order, while `buffered` keeps taking
    // messages from the source.
    let started = Instant::now();
    let orders = (0..10_000).map(|i| (format!("user-{}", i % 100), format!("order {i}")));
    let sends = stream::iter(orders).map(|(user, order)| producer.new_message().key(user).value(order).send());
    let mut results = pin!(sends.buffered(1000));
    let mut stored = 0;
    while let Some(result) = results.next().await {
        result?;
        stored += 1;
    }
    println!("buffered: {stored} messages stored in {:?}", started.elapsed());

    // A known batch: `FuturesOrdered` polls its futures in the order they were pushed, and yields their results in
    // that order. Nothing is sent until it is first polled.
    let started = Instant::now();
    let mut batch: FuturesOrdered<_> = (0..1000).map(|i| producer.send(format!("batch {i}"))).collect();
    let mut last = None;
    while let Some(result) = batch.next().await {
        last = Some(result?);
    }
    println!("FuturesOrdered: 1000 messages in {:?}, the last stored as {}", started.elapsed(), last.unwrap());

    // `join_all`: the same order, with every result at the end.
    let started = Instant::now();
    let results = join_all((0..1000).map(|i| producer.send(format!("joined {i}")))).await;
    let ids = results.into_iter().collect::<Result<Vec<_>, _>>()?;
    println!("join_all: {} messages in {:?}", ids.len(), started.elapsed());

    producer.close().await?;
    client.close().await?;
    Ok(())
}
