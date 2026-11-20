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

//! The thread-safe client: N event loops, each on its own thread with its own local client
//! (docs/2-architecture.md §4.1).
//!
//! Every operation is sent to a loop as a job and completes through a oneshot, so its future runs on any
//! executor.

use std::future::Future;
use std::sync::atomic::{AtomicBool, AtomicUsize, Ordering};
use std::sync::{Arc, Mutex};
use std::thread;

use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;
use futures_util::future::LocalBoxFuture;
use futures_util::stream::FuturesUnordered;

use crate::config::{ClientBuilder, ClientConfig, ThreadSafe};
use crate::consumer::QueueConsumerBuilder;
use crate::error::{Error, ErrorKind, Result};
use crate::local;
use crate::memory::MemoryLimit;
use crate::producer::ProducerBuilder;
use crate::schema::Schema;

type Job = Box<dyn FnOnce(local::PulsarClient) -> LocalBoxFuture<'static, ()> + Send>;

enum Message {
    Job(Job),
    /// Close the loop's client, then answer once the loop's runtime is gone.
    Shutdown(oneshot::Sender<()>),
}

struct Loop {
    sender: mpsc::UnboundedSender<Message>,
    /// Producers and consumers pinned to this loop, for placement.
    load: AtomicUsize,
}

/// A producer's or consumer's place on a loop: it counts toward the loop's load until dropped.
pub(crate) struct LoopSlot {
    client: Arc<Inner>,
    index: usize,
}

impl Drop for LoopSlot {
    fn drop(&mut self) {
        self.client.loops[self.index].load.fetch_sub(1, Ordering::Relaxed);
    }
}

/// A Pulsar client. `Send + Sync`, and cheap to clone: clones are the same client.
#[derive(Clone)]
pub struct PulsarClient {
    inner: Arc<Inner>,
}

struct Inner {
    loops: Vec<Loop>,
    closed: AtomicBool,
    threads: Mutex<Vec<thread::JoinHandle<()>>>,
    memory: Arc<MemoryLimit>,
    operation_timeout: std::time::Duration,
}

impl std::fmt::Debug for PulsarClient {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("PulsarClient").field("event_loops", &self.inner.loops.len()).finish()
    }
}

impl PulsarClient {
    pub fn builder() -> ClientBuilder<ThreadSafe> {
        ClientBuilder::default()
    }

    /// Closes the client: every loop closes its connections, then its thread exits. Later operations fail with
    /// [`ErrorKind::AlreadyClosed`]. Can be called again.
    pub async fn close(&self) -> Result<()> {
        if self.inner.closed.swap(true, Ordering::AcqRel) {
            return Ok(());
        }
        let mut done = Vec::new();
        for event_loop in &self.inner.loops {
            let (ack, acked) = oneshot::channel();
            if event_loop.sender.unbounded_send(Message::Shutdown(ack)).is_ok() {
                done.push(acked);
            }
        }
        for acked in done {
            let _ = acked.await;
        }
        // Each thread answered as its last act, so joining doesn't wait.
        for thread in self.inner.threads.lock().unwrap().drain(..) {
            let _ = thread.join();
        }
        Ok(())
    }

    /// A producer, in the topic's layout, that publishes values with `schema`. See [`ProducerBuilder`].
    pub fn new_producer<T: 'static>(&self, schema: Schema<T>) -> ProducerBuilder<T> {
        ProducerBuilder::new(self.clone(), schema)
    }

    /// A Queue consumer, on every segment of the topic's layout, that reads values with `schema`. See
    /// [`QueueConsumerBuilder`].
    pub fn new_queue_consumer<T: 'static>(&self, schema: Schema<T>) -> QueueConsumerBuilder<T> {
        QueueConsumerBuilder::new(self.clone(), schema)
    }

    /// The loop for a new producer or consumer: the least loaded, by count. The slot counts it there until dropped.
    pub(crate) fn place(&self) -> (LoopSlot, usize) {
        let loads = self.inner.loops.iter().map(|l| l.load.load(Ordering::Relaxed));
        let index = loads.enumerate().min_by_key(|(_, load)| *load).map(|(i, _)| i).unwrap_or(0);
        self.inner.loops[index].load.fetch_add(1, Ordering::Relaxed);
        (LoopSlot { client: self.inner.clone(), index }, index)
    }

    pub(crate) fn memory(&self) -> &Arc<MemoryLimit> {
        &self.inner.memory
    }

    pub(crate) fn operation_timeout(&self) -> std::time::Duration {
        self.inner.operation_timeout
    }

    #[cfg(test)]
    pub(crate) fn loads(&self) -> Vec<usize> {
        self.inner.loops.iter().map(|l| l.load.load(Ordering::Relaxed)).collect()
    }

    /// Runs `job` on loop `index`, with that loop's local client, and returns its result.
    pub(crate) fn run_on<T, F>(&self, index: usize, job: F) -> impl Future<Output = Result<T>> + Send + 'static
    where
        T: Send + 'static,
        F: FnOnce(local::PulsarClient) -> LocalBoxFuture<'static, Result<T>> + Send + 'static,
    {
        let (reply, result) = oneshot::channel();
        let job: Job = Box::new(move |client| {
            Box::pin(async move {
                let _ = reply.send(job(client).await);
            })
        });
        let sent = if self.inner.closed.load(Ordering::Acquire) {
            false
        } else {
            self.inner.loops[index % self.inner.loops.len()].sender.unbounded_send(Message::Job(job)).is_ok()
        };
        async move {
            if !sent {
                return Err(Error::closed());
            }
            result.await.map_err(|_| Error::closed())?
        }
    }

    #[cfg(test)]
    pub(crate) fn take_threads(&self) -> Vec<thread::JoinHandle<()>> {
        std::mem::take(&mut *self.inner.threads.lock().unwrap())
    }

    #[cfg(test)]
    pub(crate) fn threads_finished(&self) -> bool {
        self.inner.threads.lock().unwrap().iter().all(|t| t.is_finished())
    }
}

impl Drop for Inner {
    fn drop(&mut self) {
        // The last handle is gone: each loop sees its channel end, closes its client and exits, in the background.
        self.closed.store(true, Ordering::Release);
    }
}

/// How an event loop starts: the thread it runs on, and its runtime. `build` uses the system's; tests swap them to
/// make a loop fail to start.
#[derive(Clone, Copy)]
pub(crate) struct LoopStarter {
    pub spawn: SpawnLoop,
    pub runtime: MakeRuntime,
}

/// Spawns the thread named by the first argument, running the second.
pub(crate) type SpawnLoop = fn(String, Box<dyn FnOnce() + Send>) -> std::io::Result<thread::JoinHandle<()>>;

/// Creates an event loop's runtime.
pub(crate) type MakeRuntime = fn() -> std::io::Result<compio::runtime::Runtime>;

impl LoopStarter {
    pub const SYSTEM: Self = Self {
        spawn: |name, run| thread::Builder::new().name(name).spawn(run),
        runtime: compio::runtime::Runtime::new,
    };
}

impl ClientBuilder<ThreadSafe> {
    /// Starts the event loops and creates the client.
    pub async fn build(self) -> Result<PulsarClient> {
        self.build_with(LoopStarter::SYSTEM).await
    }

    /// [`ClientBuilder::build`], starting the event loops with `starter`.
    pub(crate) async fn build_with(self, starter: LoopStarter) -> Result<PulsarClient> {
        let loops = self.event_loops;
        let config = self.into_config()?;
        let mut handles = Vec::with_capacity(loops);
        let mut threads = Vec::with_capacity(loops);
        let mut started = Vec::with_capacity(loops);
        let config_for_client = config.clone();
        for index in 0..loops {
            let (sender, receiver) = mpsc::unbounded();
            let (ready, is_ready) = oneshot::channel();
            let config = config.clone();
            let runtime = starter.runtime;
            let run = Box::new(move || run_loop(index, runtime, config, receiver, ready));
            let thread = (starter.spawn)(format!("pulsar-loop-{index}"), run)
                .map_err(|e| Error::with_source(ErrorKind::Other, "can't start an event loop thread", e))?;
            handles.push(Loop { sender, load: AtomicUsize::new(0) });
            threads.push(thread);
            started.push(is_ready);
        }
        let client = PulsarClient {
            inner: Arc::new(Inner {
                loops: handles,
                closed: AtomicBool::new(false),
                threads: Mutex::new(threads),
                memory: config_for_client.memory.clone(),
                operation_timeout: config_for_client.operation_timeout,
            }),
        };
        for is_ready in started {
            if let Err(error) =
                is_ready.await.unwrap_or_else(|_| Err(Error::new(ErrorKind::Other, "an event loop failed to start")))
            {
                let _ = client.close().await;
                return Err(error);
            }
        }
        Ok(client)
    }
}

fn run_loop(
    index: usize,
    runtime: MakeRuntime,
    config: ClientConfig,
    mut receiver: mpsc::UnboundedReceiver<Message>,
    ready: oneshot::Sender<Result<()>>,
) {
    let runtime = match runtime() {
        Ok(runtime) => runtime,
        Err(e) => {
            let _ = ready.send(Err(Error::with_source(ErrorKind::Other, "can't start an event loop", e)));
            return;
        }
    };
    tracing::info!(event_loop = index, driver = ?runtime.driver_type(), "event loop started");
    let ack = runtime.block_on(async move {
        let client = local::PulsarClient::from_config(config);
        let _ = ready.send(Ok(()));
        let mut jobs = FuturesUnordered::new();
        let ack = loop {
            futures_util::select_biased! {
                message = receiver.next() => match message {
                    Some(Message::Job(job)) => jobs.push(job(client.clone())),
                    Some(Message::Shutdown(ack)) => break Some(ack),
                    None => break None,
                },
                () = jobs.select_next_some() => {}
            }
        };
        let _ = client.close().await;
        // Jobs still running end with the client closed.
        while jobs.next().await.is_some() {}
        ack
    });
    drop(runtime);
    if let Some(ack) = ack {
        let _ = ack.send(());
    }
}
