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

//! `pulsar-perf`: the producer's and the Queue consumer's throughput and latency against a broker (docs/8-quality.md
//! §6).
//!
//! Its options, its reports every 10 seconds and its summary lines are those of Java's `pulsar-perf produce` and
//! `pulsar-perf consume`, so that the two clients compare line for line on the same broker:
//!
//! ```text
//! pulsar-perf produce -u pulsar://localhost:6650 -s 1024 -m 1000000 -r 5000000 topic://public/default/perf
//! pulsar-perf consume -u pulsar://localhost:6650 -m 1000000 topic://public/default/perf
//! ```

use std::convert::Infallible;
use std::io::IsTerminal;
use std::pin::pin;
use std::sync::mpsc::{self, RecvTimeoutError};
use std::sync::{Arc, Mutex};
use std::task::Poll;
use std::time::{Duration, Instant, SystemTime};

use apache_pulsar::{
    BatchingPolicy, CompressionPolicy, CompressionType, MessageId, Producer, ProducerBuilder, PulsarClient,
    QueueConsumer, QueueConsumerBuilder, Schema, SubscriptionInitialPosition, local,
};
use clap::{Parser, Subcommand, ValueEnum};
use futures_util::future::{Either, select};
use futures_util::stream::{FuturesUnordered, StreamExt};
use hdrhistogram::Histogram;
use tracing_subscriber::EnvFilter;
use tracing_subscriber::filter::LevelFilter;

/// What the client counts against its memory limit for a message, besides its payload.
const PER_MESSAGE_OVERHEAD: u64 = 1024;

/// How often a run prints its throughput and latency since the last report, as pulsar-perf does.
const REPORT_INTERVAL: Duration = Duration::from_secs(10);

/// The highest send latency the histograms tell apart, in microseconds: a minute.
const MAX_SEND_LATENCY_MICROS: u64 = 60_000_000;

/// The highest latency of a received message, from its publish time, the histogram tells apart, in milliseconds: ten
/// days, as pulsar-perf's.
const MAX_RECEIVE_LATENCY_MILLIS: u64 = 10 * 24 * 3600 * 1000;

#[derive(Parser)]
#[command(about = "Throughput and latency of the producer and the Queue consumer, as Java's pulsar-perf measures them")]
struct Args {
    #[command(subcommand)]
    command: Command,
}

#[derive(Subcommand)]
enum Command {
    Produce(Produce),
    Consume(Consume),
}

/// Publishes `-m` messages of `-s` bytes, or for `--test-duration` seconds, at `-r` per second or as fast as they go.
#[derive(clap::Args)]
struct Produce {
    topic: String,
    #[arg(short = 'u', long = "service-url", default_value = "pulsar://localhost:6650")]
    service_url: String,
    /// The client's memory limit, which bounds the messages in flight, as `-ml` does Java's: `64M` by default.
    #[arg(long = "memory-limit", alias = "ml", default_value = "64M", value_parser = bytes)]
    memory_limit: u64,
    /// Message size, in bytes: uppercase letters, as pulsar-perf's.
    #[arg(short = 's', long = "size", default_value_t = 1024)]
    size: usize,
    /// How many messages to send; 0 for no limit.
    #[arg(short = 'm', long = "num-messages", default_value_t = 1_000_000)]
    messages: u64,
    /// How long to send for, in seconds; 0 for no limit.
    #[arg(long = "test-duration", alias = "time", default_value_t = 0)]
    test_duration: u64,
    /// Messages per second; 0 for as fast as they go.
    #[arg(short = 'r', long = "rate", default_value_t = 0)]
    rate: u64,
    /// The most messages in flight; 0, the default, for as many as the memory limit holds, the only bound a Java V5
    /// producer has.
    #[arg(short = 'o', long = "max-outstanding", default_value_t = 0)]
    max_outstanding: usize,
    /// The batching delay, in milliseconds.
    #[arg(short = 'b', long = "batch-time-window", default_value_t = 1.0)]
    batch_time_ms: f64,
    #[arg(long = "disable-batching")]
    disable_batching: bool,
    #[arg(short = 'z', long = "compression", value_enum, default_value_t = Codec::None)]
    compression: Codec,
    /// How many keys to send with, in turn; none by default, as pulsar-perf.
    #[arg(short = 'k', long = "keys", default_value_t = 0)]
    keys: usize,
    /// Latencies in the first seconds are left out.
    #[arg(long = "warmup-time", default_value_t = 1.0)]
    warmup_seconds: f64,
    /// The thread-safe client's event loops, the producer on one of them, apart from the thread making the load; 0
    /// for the local client, on that thread.
    #[arg(long = "loops", default_value_t = 1)]
    loops: usize,
}

/// Receives and acknowledges `-m` messages with a Queue consumer, from the earliest.
#[derive(clap::Args)]
struct Consume {
    topic: String,
    #[arg(short = 'u', long = "service-url", default_value = "pulsar://localhost:6650")]
    service_url: String,
    #[arg(short = 'm', long = "num-messages", default_value_t = 1_000_000)]
    messages: u64,
    #[arg(short = 'q', long = "receiver-queue-size", default_value_t = 1000)]
    receiver_queue_size: u32,
    #[arg(long = "subscription", default_value = "perf")]
    subscription: String,
    /// The thread-safe client's event loops; 0 for the local client, on the thread receiving.
    #[arg(long = "loops", default_value_t = 1)]
    loops: usize,
}

#[derive(Clone, Copy, ValueEnum)]
enum Codec {
    #[value(name = "NONE", alias = "none")]
    None,
    #[value(name = "LZ4", alias = "lz4")]
    Lz4,
    #[value(name = "ZLIB", alias = "zlib")]
    Zlib,
    #[value(name = "ZSTD", alias = "zstd")]
    Zstd,
    #[value(name = "SNAPPY", alias = "snappy")]
    Snappy,
}

impl Codec {
    fn policy(self) -> CompressionPolicy {
        CompressionPolicy::of(match self {
            Codec::None => CompressionType::None,
            Codec::Lz4 => CompressionType::Lz4,
            Codec::Zlib => CompressionType::Zlib,
            Codec::Zstd => CompressionType::Zstd,
            Codec::Snappy => CompressionType::Snappy,
        })
    }
}

fn main() {
    let args = Args::parse();
    // The client's logs and this tool's, at INFO unless `RUST_LOG` says otherwise.
    tracing_subscriber::fmt()
        .with_env_filter(EnvFilter::builder().with_default_directive(LevelFilter::INFO.into()).from_env_lossy())
        .with_ansi(std::io::stdout().is_terminal())
        .init();
    let runtime = compio::runtime::Runtime::new().expect("a runtime");
    let result = runtime.block_on(async {
        match args.command {
            Command::Produce(produce) => produce.run().await,
            Command::Consume(consume) => consume.run().await,
        }
    });
    if let Err(error) = result {
        tracing::error!("{error}");
        std::process::exit(1);
    }
}

impl Produce {
    async fn run(self) -> apache_pulsar::Result<()> {
        if self.loops == 0 {
            let client = local::PulsarClient::builder()
                .service_url(&self.service_url)
                .memory_limit(self.memory_limit)
                .build()
                .await?;
            let producer = self.configure(client.new_producer(Schema::bytes())).create().await?;
            self.drive(&producer).await;
            producer.close().await?;
            client.close().await
        } else {
            let client = PulsarClient::builder()
                .service_url(&self.service_url)
                .memory_limit(self.memory_limit)
                .event_loops(self.loops)
                .build()
                .await?;
            let producer = self.configure(client.new_producer(Schema::bytes())).create().await?;
            self.drive(&producer).await;
            producer.close().await?;
            client.close().await
        }
    }

    fn configure<Api>(&self, builder: ProducerBuilder<Vec<u8>, Api>) -> ProducerBuilder<Vec<u8>, Api> {
        let batching = if self.disable_batching {
            BatchingPolicy::disabled()
        } else {
            let delay = Duration::from_secs_f64(self.batch_time_ms / 1000.0);
            BatchingPolicy { max_publish_delay: delay, ..Default::default() }
        };
        builder
            .topic(&self.topic)
            .batching_policy(batching)
            .compression_policy(self.compression.policy())
            .send_timeout(Duration::ZERO)
    }

    /// Sends the messages, and prints their throughput and latency every 10 seconds and over the run.
    async fn drive<Api>(&self, producer: &Producer<Vec<u8>, Api>) {
        let payload = letters(self.size);
        let keys: Vec<String> = (0..self.keys).map(|i| format!("key-{i}")).collect();
        let mut latencies = Histogram::<u64>::new_with_bounds(1, MAX_SEND_LATENCY_MICROS, 3).expect("a histogram");
        // As many in flight as the memory limit holds, by default: the limit binds as it does Java's V5 producer,
        // and the loop waits for completions, letting the producer's task run, where it would wait for room.
        let max_outstanding = match self.max_outstanding {
            0 => (self.memory_limit / (self.size as u64 + PER_MESSAGE_OVERHEAD)).max(1) as usize,
            n => n,
        };
        let mut in_flight = FuturesUnordered::new();
        let start = Instant::now();
        let warmup = start + Duration::from_secs_f64(self.warmup_seconds);
        let end = (self.test_duration > 0).then(|| start + Duration::from_secs(self.test_duration));
        let report = Arc::new(Mutex::new(Report::new(start, MAX_SEND_LATENCY_MICROS, produced)));
        let mut sent = 0;
        let mut record = |(sent, latency, result): (Instant, Duration, apache_pulsar::Result<MessageId>)| {
            result.expect("a send failed");
            let micros = (sent >= warmup).then_some(latency.as_micros() as u64);
            if let Some(micros) = micros {
                latencies.saturating_record(micros);
            }
            report.lock().expect("the report").record(self.size, micros);
        };
        reporting(&report, async {
            while (self.messages == 0 || sent < self.messages) && end.is_none_or(|end| Instant::now() < end) {
                let i = sent;
                sent += 1;
                // Ahead of the rate: wait, taking completions meanwhile.
                let at = (self.rate > 0).then(|| start + Duration::from_secs_f64(i as f64 / self.rate as f64));
                if let Some(at) = at.filter(|at| *at > Instant::now()) {
                    let mut sleep = pin!(compio::time::sleep_until(at));
                    loop {
                        match select(sleep.as_mut(), in_flight.next()).await {
                            Either::Left(_) => break,
                            Either::Right((Some(done), _)) => record(done),
                            // Nothing in flight: only the time is left to wait for.
                            Either::Right((None, _)) => {
                                sleep.as_mut().await;
                                break;
                            }
                        }
                    }
                }
                while in_flight.len() >= max_outstanding {
                    record(in_flight.next().await.expect("sends in flight"));
                }
                let mut message = producer.new_message().value(payload.clone());
                if !keys.is_empty() {
                    message = message.key(keys[i as usize % keys.len()].clone());
                }
                // Polled once at once, so that it is queued now, in call order. Its latency runs from there to its
                // completion.
                let mut send = Box::pin(async move {
                    let sent = Instant::now();
                    let result = message.send().await;
                    (sent, sent.elapsed(), result)
                });
                match futures_util::poll!(send.as_mut()) {
                    Poll::Ready(done) => record(done),
                    Poll::Pending => in_flight.push(send),
                }
            }
            while let Some(done) = in_flight.next().await {
                record(done);
            }
        })
        .await;
        let elapsed = start.elapsed().as_secs_f64();
        let rate = sent as f64 / elapsed;
        let mbits = rate * self.size as f64 * 8.0 / 1024.0 / 1024.0;
        tracing::info!("Aggregated throughput stats --- {sent} records sent --- {rate:.3} msg/s --- {mbits:.3} Mbit/s");
        let ms = |micros: u64| micros as f64 / 1000.0;
        tracing::info!(
            "Aggregated latency stats --- Latency: mean: {:7.3} ms - med: {:7.3} - 95pct: {:7.3} - 99pct: {:7.3} - \
             99.9pct: {:7.3} - 99.99pct: {:7.3} - Max: {:7.3}",
            latencies.mean() / 1000.0,
            ms(latencies.value_at_quantile(0.5)),
            ms(latencies.value_at_quantile(0.95)),
            ms(latencies.value_at_quantile(0.99)),
            ms(latencies.value_at_quantile(0.999)),
            ms(latencies.value_at_quantile(0.9999)),
            ms(latencies.max()),
        );
    }
}

impl Consume {
    async fn run(self) -> apache_pulsar::Result<()> {
        if self.loops == 0 {
            let client = local::PulsarClient::builder().service_url(&self.service_url).build().await?;
            let consumer = self.configure(client.new_queue_consumer(Schema::bytes())).subscribe().await?;
            self.drive(&consumer).await?;
            consumer.close().await?;
            client.close().await
        } else {
            let client = PulsarClient::builder().service_url(&self.service_url).event_loops(self.loops).build().await?;
            let consumer = self.configure(client.new_queue_consumer(Schema::bytes())).subscribe().await?;
            self.drive(&consumer).await?;
            consumer.close().await?;
            client.close().await
        }
    }

    fn configure<Api>(&self, builder: QueueConsumerBuilder<Vec<u8>, Api>) -> QueueConsumerBuilder<Vec<u8>, Api> {
        builder
            .topic(&self.topic)
            .subscription_name(&self.subscription)
            .subscription_initial_position(SubscriptionInitialPosition::Earliest)
            .receiver_queue_size(self.receiver_queue_size)
    }

    /// Receives and acknowledges the messages, and prints their throughput and latency every 10 seconds, and their
    /// throughput over the run.
    async fn drive<Api>(&self, consumer: &QueueConsumer<Vec<u8>, Api>) -> apache_pulsar::Result<()> {
        let mut bytes = 0;
        let start = Instant::now();
        let report = Arc::new(Mutex::new(Report::new(start, MAX_RECEIVE_LATENCY_MILLIS, received)));
        reporting(&report, async {
            for _ in 0..self.messages {
                let message = consumer.receive().await?;
                bytes += message.size();
                // From the publish time, in whole milliseconds, as pulsar-perf's: none on a clock behind the broker's.
                let millis = SystemTime::now()
                    .duration_since(message.publish_time())
                    .ok()
                    .map(|latency| latency.as_millis() as u64);
                report.lock().expect("the report").record(message.size(), millis);
                consumer.acknowledge(message.id()).await?;
            }
            Ok::<_, apache_pulsar::Error>(())
        })
        .await?;
        let elapsed = start.elapsed().as_secs_f64();
        let rate = self.messages as f64 / elapsed;
        let mbits = bytes as f64 * 8.0 / 1024.0 / 1024.0 / elapsed;
        tracing::info!(
            "Aggregated throughput stats --- {} records received --- {rate:.3} msg/s --- {mbits:.3} Mbit/s",
            self.messages
        );
        Ok(())
    }
}

/// What a run did since its last report, which it prints every 10 seconds, as pulsar-perf does.
struct Report {
    /// The run's messages so far.
    total: u64,
    /// The messages since the last report, their bytes, and their latencies.
    messages: u64,
    bytes: u64,
    latencies: Histogram<u64>,
    /// When the last report was, or the run started.
    last: Instant,
    /// The report's line: of the run's messages so far, the messages and the Mbit per second since the last report,
    /// and their latencies.
    line: fn(u64, f64, f64, &Histogram<u64>) -> String,
}

impl Report {
    fn new(start: Instant, max_latency: u64, line: fn(u64, f64, f64, &Histogram<u64>) -> String) -> Self {
        let latencies = Histogram::new_with_bounds(1, max_latency, 3).expect("a histogram");
        Report { total: 0, messages: 0, bytes: 0, latencies, last: start, line }
    }

    /// A message done, of `bytes`, with its latency unless it is left out.
    fn record(&mut self, bytes: usize, latency: Option<u64>) {
        self.total += 1;
        self.messages += 1;
        self.bytes += bytes as u64;
        if let Some(latency) = latency {
            self.latencies.saturating_record(latency);
        }
    }

    /// When the next report falls due.
    fn next(&self) -> Instant {
        self.last + REPORT_INTERVAL
    }

    /// The report's line, if it is due at `now`, which the next report counts from.
    fn due(&mut self, now: Instant) -> Option<String> {
        let elapsed = now.saturating_duration_since(self.last);
        if elapsed < REPORT_INTERVAL {
            return None;
        }
        let seconds = elapsed.as_secs_f64();
        let rate = self.messages as f64 / seconds;
        let mbits = self.bytes as f64 * 8.0 / 1024.0 / 1024.0 / seconds;
        let line = (self.line)(self.total, rate, mbits, &self.latencies);
        self.messages = 0;
        self.bytes = 0;
        self.latencies.reset();
        self.last = now;
        Some(line)
    }
}

/// A producer's report, as pulsar-perf prints it, of latencies in microseconds. A failed send ends the run, so no
/// report has one.
fn produced(total: u64, rate: f64, mbits: f64, latencies: &Histogram<u64>) -> String {
    let ms = |micros: u64| micros as f64 / 1000.0;
    format!(
        "Throughput produced: {total:7} msg --- {rate:8.1} msg/s --- {mbits:8.1} Mbit/s --- failure {:8.1} msg/s --- \
         Latency: mean: {:7.3} ms - med: {:7.3} - 95pct: {:7.3} - 99pct: {:7.3} - 99.9pct: {:7.3} - 99.99pct: {:7.3} \
         - Max: {:7.3}",
        0.0,
        latencies.mean() / 1000.0,
        ms(latencies.value_at_quantile(0.5)),
        ms(latencies.value_at_quantile(0.95)),
        ms(latencies.value_at_quantile(0.99)),
        ms(latencies.value_at_quantile(0.999)),
        ms(latencies.value_at_quantile(0.9999)),
        ms(latencies.max()),
    )
}

/// A consumer's report, as pulsar-perf prints it, of latencies in milliseconds.
fn received(total: u64, rate: f64, mbits: f64, latencies: &Histogram<u64>) -> String {
    format!(
        "Throughput received: {total:7} msg --- {rate:.3} msg/s --- {mbits:.3} Mbit/s --- Latency: mean: {:.3} ms - \
         med: {} - 95pct: {} - 99pct: {} - 99.9pct: {} - 99.99pct: {} - Max: {}",
        latencies.mean(),
        latencies.value_at_quantile(0.5),
        latencies.value_at_quantile(0.95),
        latencies.value_at_quantile(0.99),
        latencies.value_at_quantile(0.999),
        latencies.value_at_quantile(0.9999),
        latencies.max(),
    )
}

/// Runs `work` to its end, logging `report` every 10 seconds meanwhile, from a thread of its own, as pulsar-perf's
/// main thread does: a timer on the runtime would cost every turn of the work's loop, and would wait for the work to.
async fn reporting<T>(report: &Arc<Mutex<Report>>, work: impl Future<Output = T>) -> T {
    let (done, finished) = mpsc::channel::<Infallible>();
    let reports = {
        let report = Arc::clone(report);
        std::thread::spawn(move || {
            loop {
                let next = report.lock().expect("the report").next();
                match finished.recv_timeout(next.saturating_duration_since(Instant::now())) {
                    Ok(never) => match never {},
                    Err(RecvTimeoutError::Timeout) => {
                        if let Some(line) = report.lock().expect("the report").due(Instant::now()) {
                            tracing::info!("{line}");
                        }
                    }
                    Err(RecvTimeoutError::Disconnected) => break,
                }
            }
        })
    };
    let output = work.await;
    drop(done);
    reports.join().expect("the reports' thread");
    output
}

/// A size in bytes, with an optional `K`, `M` or `G` suffix of powers of 1024, as pulsar-perf's `-ml` takes.
fn bytes(text: &str) -> Result<u64, String> {
    let (number, unit) = match text.char_indices().find(|(_, c)| !c.is_ascii_digit()) {
        Some((at, _)) => text.split_at(at),
        None => (text, ""),
    };
    let shift = match unit.to_ascii_uppercase().as_str() {
        "" | "B" => 0,
        "K" | "KB" => 10,
        "M" | "MB" => 20,
        "G" | "GB" => 30,
        _ => return Err(format!("not a size: {text}")),
    };
    number.parse::<u64>().map(|n| n << shift).map_err(|error| format!("not a size: {text}: {error}"))
}

/// `size` uppercase letters, as pulsar-perf's payload: random, with a fixed seed.
fn letters(size: usize) -> Vec<u8> {
    let mut state = 0x2545_f491_4f6c_dd1d_u64;
    (0..size)
        .map(|_| {
            state ^= state << 13;
            state ^= state >> 7;
            state ^= state << 17;
            b'A' + (state % 26) as u8
        })
        .collect()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn sizes_take_pulsar_perfs_suffixes() {
        assert_eq!(bytes("1024"), Ok(1024));
        assert_eq!(bytes("64M"), Ok(64 << 20));
        assert_eq!(bytes("32k"), Ok(32 << 10));
        assert_eq!(bytes("2GB"), Ok(2 << 30));
        assert!(bytes("64X").is_err() && bytes("M").is_err());
    }

    #[test]
    fn producers_report_every_10_seconds() {
        let start = Instant::now();
        let mut report = Report::new(start, MAX_SEND_LATENCY_MICROS, produced);
        for micros in [100, 200, 300, 400] {
            report.record(128 * 1024, Some(micros));
        }
        // In the warm-up: counted, without its latency.
        report.record(128 * 1024, None);
        assert_eq!(report.due(start + Duration::from_secs(9)), None);
        assert_eq!(
            report.due(start + Duration::from_secs(10)).as_deref(),
            Some(
                "Throughput produced:       5 msg ---      0.5 msg/s ---      0.5 Mbit/s --- failure      0.0 msg/s --- \
                 Latency: mean:   0.250 ms - med:   0.200 - 95pct:   0.400 - 99pct:   0.400 - 99.9pct:   0.400 - \
                 99.99pct:   0.400 - Max:   0.400"
            )
        );
        assert_eq!(report.due(start + Duration::from_secs(19)), None, "10 seconds from the last report");
        assert_eq!(
            report.due(start + Duration::from_secs(20)).as_deref(),
            Some(
                "Throughput produced:       5 msg ---      0.0 msg/s ---      0.0 Mbit/s --- failure      0.0 msg/s --- \
                 Latency: mean:   0.000 ms - med:   0.000 - 95pct:   0.000 - 99pct:   0.000 - 99.9pct:   0.000 - \
                 99.99pct:   0.000 - Max:   0.000"
            ),
            "an interval without messages"
        );
    }

    #[test]
    fn consumers_report_every_10_seconds() {
        let start = Instant::now();
        let mut report = Report::new(start, MAX_RECEIVE_LATENCY_MILLIS, received);
        for millis in [5, 10, 20] {
            report.record(1024, Some(millis));
        }
        assert_eq!(
            report.due(start + Duration::from_secs(10)).as_deref(),
            Some(
                "Throughput received:       3 msg --- 0.300 msg/s --- 0.002 Mbit/s --- Latency: mean: 11.667 ms - \
                 med: 10 - 95pct: 20 - 99pct: 20 - 99.9pct: 20 - 99.99pct: 20 - Max: 20"
            )
        );
    }

    #[test]
    fn payloads_are_uppercase_letters() {
        let payload = letters(1000);
        assert_eq!(payload.len(), 1000);
        assert!(payload.iter().all(u8::is_ascii_uppercase));
        assert_eq!(payload, letters(1000), "the same every run");
    }
}
