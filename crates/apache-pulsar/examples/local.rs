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

//! The local API, for applications that run their own compio event loops, one per core: each thread has a client of
//! its own, whose producers and consumers run on that thread's loop, so no operation hops to another thread. Here two
//! threads produce, and a third reads what they stored.
//!
//! `cargo run --example local`. `PULSAR_URL` sets another service URL, and the first argument another topic.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{Schema, SubscriptionInitialPosition, local};

const THREADS: usize = 2;
const MESSAGES: usize = 10;

fn main() -> Result<(), Box<dyn Error + Send + Sync>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar://localhost:6650".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/per-core".into());

    // Subscribed before the producers start, so that it gets all they send.
    let reader = {
        let (url, topic) = (url.clone(), topic.clone());
        let (subscribed, is_subscribed) = std::sync::mpsc::channel();
        let reader =
            std::thread::spawn(move || compio::runtime::Runtime::new()?.block_on(read(url, topic, subscribed)));
        // A failed subscribe ends the thread, and drops the sender.
        if is_subscribed.recv().is_err() {
            return reader.join().expect("the reader panicked");
        }
        reader
    };

    let producers: Vec<_> = (0..THREADS)
        .map(|core| {
            let (url, topic) = (url.clone(), topic.clone());
            std::thread::spawn(move || compio::runtime::Runtime::new()?.block_on(produce(core, url, topic)))
        })
        .collect();
    for producer in producers {
        producer.join().expect("a producer panicked")?;
    }
    reader.join().expect("the reader panicked")
}

/// Runs on its thread's loop: a client of the loop's own, and a producer on it.
async fn produce(core: usize, url: String, topic: String) -> Result<(), Box<dyn Error + Send + Sync>> {
    let client = local::PulsarClient::builder().service_url(url).build().await?;
    let producer = client.new_producer(Schema::string()).topic(topic).create().await?;
    for i in 0..MESSAGES {
        producer
            .new_message()
            .key(format!("core-{core}"))
            .value(format!("message {i} from core {core}"))
            .send()
            .await?;
    }
    producer.close().await?;
    client.close().await?;
    Ok(())
}

/// Reads what the producers send, as a stream, until it has them all, or none comes for 10 s.
async fn read(
    url: String,
    topic: String,
    subscribed: std::sync::mpsc::Sender<()>,
) -> Result<(), Box<dyn Error + Send + Sync>> {
    use futures::StreamExt;

    let client = local::PulsarClient::builder().service_url(url).build().await?;
    let consumer = client
        .new_queue_consumer(Schema::string())
        .topic(topic)
        .subscription_name("per-core-reader")
        .subscription_initial_position(SubscriptionInitialPosition::Latest)
        .subscribe()
        .await?;
    subscribed.send(())?;
    let mut messages = std::pin::pin!(consumer.stream().take(THREADS * MESSAGES));
    loop {
        let next = compio::time::timeout(Duration::from_secs(10), messages.next()).await;
        let Ok(Some(message)) = next else { break };
        println!("{:?}: {}", message.key(), message.value()?);
        consumer.acknowledge(message.id()).await?;
    }
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
