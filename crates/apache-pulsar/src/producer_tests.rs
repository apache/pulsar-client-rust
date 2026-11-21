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

//! Producers over real loopback sockets, against the simulated world of `apache-pulsar-testkit`.
//!
//! The ordering and retry decisions are tested deterministically in the testkit; these tests check the driver:
//! the producer task, its layout watch, registrations, pushes, timers, the memory limit, the two client models,
//! and the send-order promise of the rustdoc.

use std::cell::RefCell;
use std::collections::{BTreeMap, HashMap};
use std::future::poll_fn;
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Poll, Waker};
use std::time::Duration;

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::lookup::HostPort;
use apache_pulsar_protocol::proto::FeatureFlags;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_testkit::LookupAnswer;
use apache_pulsar_testkit::scalable::{ConnId, World};
use bytes::Bytes;
use compio::io::compat::AsyncStream;
use compio::net::{TcpListener, TcpStream};
use compio::runtime::JoinHandle;
use futures_util::FutureExt;
use futures_util::io::{AsyncRead, AsyncWrite};
use futures_util::stream::{FuturesOrdered, StreamExt};

use crate::error::ErrorKind;
use crate::{BatchingPolicy, ProducerAccessMode, PulsarClient, Schema, local};

pub(crate) const TOPIC: &str = "topic://acme/orders/events";

/// The simulated world behind a loopback listener. Every connection can be pushed to at any time.
pub(crate) struct WorldServer {
    address: HostPort,
    pub(crate) world: Rc<RefCell<World>>,
    wakers: Rc<RefCell<HashMap<ConnId, Waker>>>,
    _task: JoinHandle<()>,
}

impl WorldServer {
    pub(crate) async fn start(mut world: World) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let address = HostPort { host: "127.0.0.1".into(), port };
        world.lookup = LookupAnswer::Connect { broker: address.clone(), proxy: false };
        let world = Rc::new(RefCell::new(world));
        let wakers: Rc<RefCell<HashMap<ConnId, Waker>>> = Rc::default();
        let task = {
            let (world, wakers) = (world.clone(), wakers.clone());
            compio::runtime::spawn(async move {
                let mut connections = Vec::new();
                loop {
                    let Ok((tcp, _)) = listener.accept().await else { return };
                    connections.push(compio::runtime::spawn(serve(tcp, world.clone(), wakers.clone())));
                }
            })
        };
        Self { address, world, wakers, _task: task }
    }

    pub(crate) fn url(&self) -> String {
        format!("pulsar://{}", self.address)
    }

    /// Changes the world, and lets every connection send what it now has to.
    pub(crate) fn with<R>(&self, change: impl FnOnce(&mut World) -> R) -> R {
        let result = change(&mut self.world.borrow_mut());
        for waker in self.wakers.borrow().values() {
            waker.wake_by_ref();
        }
        result
    }

    pub(crate) fn received(&self, kind: Type) -> usize {
        self.world.borrow().received(kind).len()
    }
}

/// Serves one connection: what the client writes goes to the world, what the world has for it goes out.
async fn serve(tcp: TcpStream, world: Rc<RefCell<World>>, wakers: Rc<RefCell<HashMap<ConnId, Waker>>>) {
    let conn = world.borrow_mut().connect();
    let mut stream = Box::pin(AsyncStream::new(tcp));
    let mut read_buf = vec![0; 64 * 1024];
    let mut outgoing: Option<Bytes> = None;
    poll_fn(|cx| {
        wakers.borrow_mut().insert(conn, cx.waker().clone());
        loop {
            let mut progressed = false;
            match stream.as_mut().poll_read(cx, &mut read_buf) {
                Poll::Ready(Ok(0)) | Poll::Ready(Err(_)) => {
                    world.borrow_mut().disconnected(conn);
                    return Poll::Ready(());
                }
                Poll::Ready(Ok(n)) => {
                    world.borrow_mut().input(conn, &read_buf[..n]);
                    // Other connections may have something to send now: a layout, a closed producer.
                    for (other, waker) in wakers.borrow().iter() {
                        if *other != conn {
                            waker.wake_by_ref();
                        }
                    }
                    progressed = true;
                }
                Poll::Pending => {}
            }
            if !world.borrow().is_open(conn) {
                // Killed by the test, or closed by the broker: drop the socket.
                wakers.borrow_mut().remove(&conn);
                return Poll::Ready(());
            }
            if outgoing.is_none() {
                let bytes = world.borrow_mut().output(conn);
                outgoing = (!bytes.is_empty()).then_some(bytes);
            }
            if let Some(bytes) = &mut outgoing {
                match stream.as_mut().poll_write(cx, bytes) {
                    Poll::Ready(Ok(n)) => {
                        let _ = bytes.split_to(n);
                        if bytes.is_empty() {
                            outgoing = None;
                        }
                        progressed = true;
                    }
                    Poll::Ready(Err(_)) => return Poll::Ready(()),
                    Poll::Pending => {}
                }
            }
            let _ = Pin::new(&mut stream).as_mut().poll_flush(cx);
            if !progressed {
                return Poll::Pending;
            }
        }
    })
    .await
}

pub(crate) fn world(segments: u32) -> World {
    let mut world = World::default();
    world.create_topic(TOPIC, segments);
    world
}

pub(crate) fn run<F: std::future::Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

pub(crate) async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting until {what}");
        compio::time::sleep(Duration::from_millis(10)).await;
    }
}

pub(crate) async fn local_client(server: &WorldServer) -> local::PulsarClient {
    local::PulsarClient::builder()
        .service_url(server.url())
        .operation_timeout(Duration::from_secs(5))
        .build()
        .await
        .unwrap()
}

/// The values stored per key, as a reader draining parents first would see them.
fn stored_by_key(server: &WorldServer) -> BTreeMap<String, Vec<String>> {
    let mut out: BTreeMap<String, Vec<String>> = BTreeMap::new();
    for message in server.world.borrow().messages(TOPIC) {
        out.entry(message.key.unwrap_or_default())
            .or_default()
            .push(String::from_utf8(message.payload.to_vec()).unwrap());
    }
    out
}

/// The values stored, in the order a reader draining parents first would see them.
fn stored_values(server: &WorldServer) -> Vec<String> {
    server.world.borrow().messages(TOPIC).iter().map(|m| String::from_utf8(m.payload.to_vec()).unwrap()).collect()
}

#[test]
fn local_producer_sends_flushes_and_closes() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let producer =
            client.new_producer(Schema::string()).topic(TOPIC).producer_name("orders").create().await.unwrap();
        assert_eq!(producer.topic(), TOPIC);
        assert_eq!(producer.last_sequence_id(), -1);
        let keyed = producer.new_message().key("user-42").value("hello".to_string()).property("a", "b").send().await;
        let keyed = keyed.unwrap();
        let keyless = producer.send("world".to_string()).await.unwrap();
        assert_ne!(keyed, keyless);
        assert_eq!(MessageIdSegment::of(&keyed), MessageIdSegment::of(&keyed));
        producer.flush().await.unwrap();
        assert_eq!(server.world.borrow().messages(TOPIC).len(), 2);
        // Segment producers are named after the producer.
        let names: Vec<String> = server
            .world
            .borrow()
            .received(Type::PRODUCER)
            .iter()
            .map(|c| c.producer.producer_name.clone().unwrap())
            .collect();
        assert!(names.iter().all(|n| n.starts_with("orders-seg-")), "{names:?}");
        assert!(producer.last_sequence_id() >= 0);
        producer.close().await.unwrap();
        producer.close().await.unwrap();
        assert_eq!(producer.send("late".into()).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert!(server.received(Type::CLOSE_PRODUCER) >= 1);
        client.close().await.unwrap();
    });
}

/// The segment of an id, through its bytes, as an application can't see it otherwise.
struct MessageIdSegment;

impl MessageIdSegment {
    fn of(id: &crate::MessageId) -> i64 {
        apache_pulsar_protocol::message_id::MessageId::from_bytes(&id.to_bytes()).unwrap().segment_id
    }
}

#[test]
fn keys_keep_their_order_across_a_split_during_traffic() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let keys: Vec<String> = (0..20).map(|i| format!("k{i}")).collect();
        let mut expected: BTreeMap<String, Vec<String>> = BTreeMap::new();
        let mut sends = FuturesOrdered::new();
        for round in 0..30 {
            if round == 10 {
                server.with(|w| w.hold_sends = true);
            }
            if round == 15 {
                server.with(|w| {
                    w.split(TOPIC, 0);
                    w.hold_sends = false;
                    w.release_sends();
                });
            }
            for key in &keys {
                let value = format!("{key}-{round}");
                expected.entry(key.clone()).or_default().push(value.clone());
                sends.push_back(producer.new_message().key(key.clone()).value(value).send());
            }
            if (10..15).contains(&round) {
                // The broker holds the sends: get them written, without waiting for them.
                let _ = sends.next().now_or_never();
                continue;
            }
            // Drive the sends a little between rounds, in order.
            for _ in 0..keys.len() / 2 {
                if let Some(result) = sends.next().await {
                    result.unwrap();
                }
            }
        }
        while let Some(result) = sends.next().await {
            result.unwrap();
        }
        assert_eq!(stored_by_key(&server), expected);
        let segments: std::collections::BTreeSet<i64> =
            server.world.borrow().messages(TOPIC).iter().map(|m| m.id.segment_id).collect();
        assert!(segments.len() > 1, "the split was not reached: {segments:?}");
        producer.close().await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn a_missing_topic_is_not_found() {
    run(async {
        let mut world = World::default();
        world.auto_create = false;
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let error = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "{error}");
        // Created on demand when allowed.
        server.with(|w| w.auto_create = true);
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        producer.send(b"x".to_vec()).await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn a_broker_without_scalable_topics_is_refused() {
    run(async {
        let mut world = world(1);
        world.features = FeatureFlags { supports_scalable_topics: Some(false), ..Default::default() };
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let error = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedBroker, "{error}");
        // Nothing scalable was sent to it (C15).
        assert_eq!(server.received(Type::SCALABLE_TOPIC_LOOKUP), 0);
        // Not even when asked to: the connection keeps the command, and stays up.
        let address = client.lookup(TOPIC).await.unwrap();
        let connection = client.connection(&address).await.unwrap();
        let close = apache_pulsar_protocol::proto::BaseCommand {
            r#type: Type::SCALABLE_TOPIC_CLOSE.into(),
            ..Default::default()
        };
        connection.send(close).unwrap();
        client.lookup(TOPIC).await.unwrap();
        assert_eq!(server.received(Type::SCALABLE_TOPIC_CLOSE), 0);
        assert!(!connection.is_closed());
        client.close().await.unwrap();
    });
}

#[test]
fn invalid_configuration_fails_create() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        for topic in [None, Some("non-persistent://a/b/c")] {
            let mut builder = client.new_producer(Schema::bytes());
            if let Some(topic) = topic {
                builder = builder.topic(topic);
            }
            assert_eq!(builder.create().await.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        }
        client.close().await.unwrap();
    });
}

#[test]
fn exclusive_conflicts_fail_create() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let first = client
            .new_producer(Schema::bytes())
            .topic(TOPIC)
            .access_mode(ProducerAccessMode::Exclusive)
            .create()
            .await
            .unwrap();
        // Both segments were claimed at create.
        assert_eq!(server.received(Type::PRODUCER), 2);
        let second =
            client.new_producer(Schema::bytes()).topic(TOPIC).access_mode(ProducerAccessMode::Exclusive).create().await;
        assert_eq!(second.unwrap_err().kind(), ErrorKind::ProducerFenced);
        first.send(b"still mine".to_vec()).await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn the_memory_limit_fails_or_holds_sends() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client =
            local::PulsarClient::builder().service_url(server.url()).memory_limit(4 * 1024).build().await.unwrap();
        let producer =
            client.new_producer(Schema::bytes()).topic(TOPIC).block_if_queue_full(false).create().await.unwrap();
        producer.send(vec![0; 10]).await.unwrap();
        server.with(|w| w.hold_receipts = true);
        // Each message counts its payload plus 1 KiB. A reservation succeeds while usage is at or below the
        // limit: the fourth goes over it, and the fifth doesn't fit.
        let mut pending = FuturesOrdered::new();
        for _ in 0..4 {
            pending.push_back(producer.send(vec![0; 10]));
        }
        assert!(pending.next().now_or_never().is_none(), "the receipts are held");
        let full = producer.send(vec![0; 10]).await.unwrap_err();
        assert_eq!(full.kind(), ErrorKind::MemoryBufferFull);
        server.with(|w| {
            w.hold_receipts = false;
            w.release_receipts();
        });
        while let Some(result) = pending.next().await {
            result.unwrap();
        }
        wait_until("the memory is released", || client.memory().used() == 0).await;
        client.close().await.unwrap();
    });
}

#[test]
fn dropping_the_last_handle_closes_the_producer() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        let clone = producer.clone();
        producer.send(b"a".to_vec()).await.unwrap();
        drop(producer);
        clone.send(b"b".to_vec()).await.unwrap();
        drop(clone);
        wait_until("the producer closes", || server.received(Type::CLOSE_PRODUCER) == 1).await;
        wait_until("the watch closes", || server.received(Type::SCALABLE_TOPIC_CLOSE) == 1).await;
        client.close().await.unwrap();
    });
}

#[test]
fn closing_the_client_closes_its_producers() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        producer.send(b"a".to_vec()).await.unwrap();
        client.close().await.unwrap();
        assert_eq!(server.received(Type::CLOSE_PRODUCER), 1);
        assert_eq!(producer.send(b"b".to_vec()).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
    });
}

#[test]
fn a_lost_connection_writes_unacknowledged_messages_again() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("before".to_string()).await.unwrap();
        server.with(|w| w.hold_sends = true);
        let mut pending = FuturesOrdered::new();
        for i in 0..3 {
            pending.push_back(producer.send(format!("m{i}")));
        }
        // Get the sends written, then break every connection.
        let _ = compio::time::timeout(Duration::from_millis(200), pending.next()).await;
        server.with(|w| {
            w.hold_sends = false;
            for conn in w.open_connections() {
                w.kill(conn);
            }
        });
        while let Some(result) = pending.next().await {
            result.unwrap();
        }
        assert_eq!(stored_values(&server), ["before", "m0", "m1", "m2"]);
        client.close().await.unwrap();
    });
}

/// The rustdoc's pipelining patterns keep send order (docs/7-api.md §3.1, a check of milestone 2): `buffered`,
/// `FuturesOrdered` and `join_all` make their first polls in creation order.
#[test]
fn pipelining_combinators_keep_send_order() {
    run(async {
        let server = WorldServer::start(world(4)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let send = |i: usize| producer.new_message().key("one-key").value(format!("{i:04}")).send();

        let results: Vec<_> = futures_util::stream::iter(0..100).map(send).buffered(16).collect::<Vec<_>>().await;
        assert!(results.iter().all(Result::is_ok));
        let ordered: FuturesOrdered<_> = (100..200).map(send).collect();
        assert!(ordered.collect::<Vec<_>>().await.iter().all(Result::is_ok));
        let joined = futures_util::future::join_all((200..300).map(send)).await;
        assert!(joined.iter().all(Result::is_ok));

        let stored = stored_by_key(&server);
        let expected: Vec<String> = (0..300).map(|i| format!("{i:04}")).collect();
        assert_eq!(stored["one-key"], expected);
        client.close().await.unwrap();
    });
}

#[test]
fn thread_safe_producers_spread_over_the_loops() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(2)).await;
        let url = server.url();
        (server, url)
    });
    // The server runs on this thread's runtime, polled while the test waits on it.
    let client = std::thread::scope(|scope| {
        let handle = scope.spawn(|| {
            futures_executor::block_on(async {
                let client = PulsarClient::builder().service_url(url).event_loops(2).build().await.unwrap();
                let a = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
                let b = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
                assert_eq!(client.loads(), [1, 1]);
                // Producers are Send + Sync: send from other threads.
                std::thread::scope(|threads| {
                    for (i, producer) in [&a, &b].into_iter().enumerate() {
                        threads.spawn(move || {
                            futures_executor::block_on(async {
                                for j in 0..20 {
                                    producer
                                        .new_message()
                                        .key(format!("t{i}"))
                                        .value(format!("{j}"))
                                        .send()
                                        .await
                                        .unwrap();
                                }
                            })
                        });
                    }
                });
                drop(a);
                b.close().await.unwrap();
                client
            })
        });
        rt.block_on(async {
            while !handle.is_finished() {
                compio::time::sleep(Duration::from_millis(5)).await;
            }
        });
        handle.join().unwrap()
    });
    let stored = rt.block_on(async { stored_by_key(&server) });
    for i in 0..2 {
        let expected: Vec<String> = (0..20).map(|j| j.to_string()).collect();
        assert_eq!(stored[&format!("t{i}")], expected);
    }
    let close = std::thread::spawn(move || futures_executor::block_on(client.close()));
    rt.block_on(async {
        while !close.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    close.join().unwrap().unwrap();
}

#[test]
fn the_blocking_api_sends_from_a_plain_thread() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(1)).await;
        let url = server.url();
        (server, url)
    });
    let worker = std::thread::spawn(move || {
        let client = crate::blocking::PulsarClient::builder().service_url(url).build().unwrap();
        // A blocking send sends its batch at once, as Java's does: it never waits out this delay.
        let policy = BatchingPolicy { max_publish_delay: Duration::from_secs(60), ..Default::default() };
        let started = std::time::Instant::now();
        let producer = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .producer_name("sync")
            .batching_policy(policy)
            .create()
            .unwrap();
        let first = producer.new_message().key("k").value("one".to_string()).send().unwrap();
        let second = producer.send("two".to_string()).unwrap();
        assert!(first < second, "one segment: ids in publish order");
        assert!(started.elapsed() < Duration::from_secs(30), "a blocking send waited for the batching delay");
        producer.flush().unwrap();
        producer.close().unwrap();
        client.close().unwrap();
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    worker.join().unwrap();
    let values: Vec<String> = rt.block_on(async { stored_values(&server) });
    assert_eq!(values, ["one", "two"]);
}

/// Pipelined sends share entries, as batching is on by default; without it, each message is an entry of its own.
#[test]
fn pipelined_sends_share_entries_unless_batching_is_off() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let send = |i: usize| producer.new_message().key("k").value(format!("{i:03}")).send();
        let ids: Vec<_> = futures_util::future::join_all((0..100).map(send)).await;
        let ids: Vec<_> = ids.into_iter().map(Result::unwrap).collect();
        assert!(ids.windows(2).all(|pair| pair[0] < pair[1]), "ids in send order");
        let entries = server.world.borrow().stored(TOPIC).len();
        assert!(entries < 100, "{entries} entries for 100 messages");
        let expected: Vec<String> = (0..100).map(|i| format!("{i:03}")).collect();
        assert_eq!(stored_values(&server), expected);

        let unbatched = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .batching_policy(BatchingPolicy::disabled())
            .create()
            .await
            .unwrap();
        let results = futures_util::future::join_all((0..10).map(|i| unbatched.send(format!("u{i}")))).await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(server.world.borrow().stored(TOPIC).len(), entries + 10);
        client.close().await.unwrap();
    });
}

/// A flush sends the open batches at once, without waiting out the batching delay.
#[test]
fn a_flush_sends_the_open_batches() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let policy = BatchingPolicy { max_publish_delay: Duration::from_secs(60), ..Default::default() };
        let producer =
            client.new_producer(Schema::string()).topic(TOPIC).batching_policy(policy).create().await.unwrap();
        let mut sends: FuturesOrdered<_> = (0..3).map(|i| producer.send(format!("m{i}"))).collect();
        assert!(sends.next().now_or_never().is_none(), "the batch waits for the delay");
        let flushed = compio::time::timeout(Duration::from_secs(10), producer.flush()).await;
        flushed.expect("the flush waited for the delay").unwrap();
        while let Some(result) = sends.next().await {
            result.unwrap();
        }
        assert_eq!(server.world.borrow().stored(TOPIC).len(), 1);
        client.close().await.unwrap();
    });
}

/// A local client with a short operation timeout, for tests that wait past it.
async fn quick_client(server: &WorldServer) -> local::PulsarClient {
    local::PulsarClient::builder()
        .service_url(server.url())
        .operation_timeout(Duration::from_secs(1))
        .build()
        .await
        .unwrap()
}

#[test]
fn flush_waits_for_the_sends_before_it() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        server.with(|w| w.hold_receipts = true);
        let mut pending = FuturesOrdered::new();
        for i in 0..3 {
            pending.push_back(producer.send(format!("m{i}")));
        }
        assert!(pending.next().now_or_never().is_none(), "the receipts are held");
        let flush = producer.flush();
        futures_util::pin_mut!(flush);
        assert!(compio::time::timeout(Duration::from_millis(200), &mut flush).await.is_err(), "flush waits");
        server.with(|w| {
            w.hold_receipts = false;
            w.release_receipts();
        });
        flush.await.unwrap();
        // Everything before the flush is done when it is.
        for _ in 0..3 {
            pending.next().now_or_never().expect("done before the flush").unwrap().unwrap();
        }
        // Nothing pending: a flush returns at once.
        producer.flush().await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn flush_fails_when_a_send_it_waits_for_fails() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .send_timeout(Duration::from_millis(300))
            .create()
            .await
            .unwrap();
        producer.send("first".to_string()).await.unwrap();
        server.with(|w| w.hold_receipts = true);
        let late = producer.send("late".to_string());
        futures_util::pin_mut!(late);
        assert!(late.as_mut().now_or_never().is_none());
        let error = producer.flush().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        assert_eq!(late.await.unwrap_err().kind(), ErrorKind::Timeout);
        client.close().await.unwrap();
    });
}

/// The broker answers a close once it has written what it stored: those sends succeed, even though their
/// receipts and the close's answer arrive together.
#[test]
fn close_completes_the_sends_the_broker_has() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        server.with(|w| w.hold_receipts = true);
        let mut pending = FuturesOrdered::new();
        for i in 0..3 {
            pending.push_back(producer.send(format!("m{i}")));
        }
        assert!(pending.next().now_or_never().is_none(), "the receipts are held");
        // Let the sends reach the broker before closing.
        wait_until("the sends are stored", || server.world.borrow().messages(TOPIC).len() == 4).await;
        producer.close().await.unwrap();
        while let Some(result) = pending.next().await {
            result.unwrap();
        }
        client.close().await.unwrap();
    });
}

/// A wait-for-exclusive producer waits past the operation timeout for the holder to go: the broker's "not ready"
/// answer suspends the request's deadline.
#[test]
fn wait_for_exclusive_outlasts_the_operation_timeout() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = quick_client(&server).await;
        let holder = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .access_mode(ProducerAccessMode::Exclusive)
            .create()
            .await
            .unwrap();
        let waiting = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .access_mode(ProducerAccessMode::WaitForExclusive)
            .create();
        futures_util::pin_mut!(waiting);
        assert!(compio::time::timeout(Duration::from_secs(2), &mut waiting).await.is_err(), "still waiting");
        holder.close().await.unwrap();
        let waiting = waiting.await.unwrap();
        waiting.send("mine now".to_string()).await.unwrap();
        client.close().await.unwrap();
    });
}

/// Java: `asyncSendsBlockTheCallerAtTheMemoryLimit`. Sends wait for room at the limit, by default, and all go
/// through; the memory comes back.
#[test]
fn sends_wait_for_room_at_the_memory_limit() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client =
            local::PulsarClient::builder().service_url(server.url()).memory_limit(8 * 1024).build().await.unwrap();
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        // About 2 KiB each: a handful fit, the rest wait.
        let results: Vec<_> = futures_util::stream::iter(0..200)
            .map(|_| producer.send(vec![7; 1000]))
            .buffered(64)
            .collect::<Vec<_>>()
            .await;
        assert!(results.iter().all(Result::is_ok));
        assert_eq!(server.world.borrow().messages(TOPIC).len(), 200);
        wait_until("the memory is released", || client.memory().used() == 0).await;
        client.close().await.unwrap();
    });
}

/// Every field of a message, and of the producer, reaches the wire as Java writes it.
#[test]
fn every_message_field_reaches_the_wire() {
    use std::time::{SystemTime, UNIX_EPOCH};
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let builder = client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .producer_name("fields")
            .property("team", "orders")
            .initial_sequence_id(41);
        assert!(format!("{builder:?}").contains("fields"));
        let producer = builder.create().await.unwrap();
        assert_eq!(producer.producer_name(), Some("fields"));
        assert_eq!(producer.last_sequence_id(), 41);
        assert!(format!("{producer:?}").contains(TOPIC));
        let event_time = UNIX_EPOCH + Duration::from_millis(1_700_000_000_123);
        let deliver_at = UNIX_EPOCH + Duration::from_millis(1_900_000_000_000);
        let message = producer
            .new_message()
            .key("k")
            .value("v".to_string())
            .property("a", "1")
            .property("b", "2")
            .event_time(event_time)
            .sequence_id(100)
            .deliver_at(deliver_at)
            .replication_clusters(["east", "west"]);
        assert!(format!("{message:?}").contains("\"k\""));
        message.send().await.unwrap();
        let before = SystemTime::now().duration_since(UNIX_EPOCH).unwrap().as_millis() as i64;
        producer.new_message().value("later".to_string()).deliver_after(Duration::from_secs(60)).send().await.unwrap();

        {
            let world = server.world.borrow();
            let stored = world.stored(TOPIC);
            let (first, second) = (&stored[0], &stored[1]);
            let single = first.single.as_ref().unwrap();
            assert_eq!((first.metadata.sequence_id, single.sequence_id), (100, Some(100)));
            assert_eq!(first.metadata.partition_key.as_deref(), Some("k"));
            assert_eq!(single.event_time, Some(1_700_000_000_123));
            let properties: Vec<(&str, &str)> =
                single.properties.iter().map(|p| (p.key.as_str(), p.value.as_str())).collect();
            assert_eq!(properties, [("a", "1"), ("b", "2")]);
            assert_eq!(first.metadata.deliver_at_time, Some(1_900_000_000_000));
            assert_eq!(first.metadata.replicate_to, ["east", "west"]);
            // The application's id doesn't move the counter: the next is the one after the initial.
            assert_eq!(second.metadata.sequence_id, 42);
            assert!(second.metadata.deliver_at_time.unwrap() >= before + 60_000);
            let registration = &world.received(Type::PRODUCER)[0].producer;
            assert!(registration.metadata.iter().any(|kv| kv.key == "team" && kv.value == "orders"));
        }
        client.close().await.unwrap();
    });
}

/// A send the broker refuses with a checksum error is written again after the producer registers again.
#[test]
fn a_refused_send_is_written_again() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        server.with(|w| w.fail_sends.push_back(apache_pulsar_protocol::proto::ServerError::ChecksumError));
        producer.send("second".to_string()).await.unwrap();
        assert_eq!(stored_values(&server), ["first", "second"]);
        assert_eq!(server.received(Type::PRODUCER), 2, "registered again");
        client.close().await.unwrap();
    });
}

#[test]
fn a_send_after_close_fails() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        producer.send("first".to_string()).await.unwrap();
        let error = producer.new_message().key("k").send().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration, "{error}");
        // Polled in that order: the close is queued before the send.
        let (closed, sent) = futures_util::join!(producer.close(), producer.send("late".to_string()));
        closed.unwrap();
        assert_eq!(sent.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(producer.flush().await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(client.memory().used(), 0);
        client.close().await.unwrap();
    });
}

#[test]
fn a_message_above_the_brokers_limit_fails() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        let error = producer.send(vec![0; 6 << 20]).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::MessageTooLarge, "{error}");
        producer.send(b"fine".to_vec()).await.unwrap();
        assert_eq!(client.memory().used(), 0);
        client.close().await.unwrap();
    });
}

/// A lookup the broker refuses fails the sends routed to the segment, with the broker's category.
#[test]
fn a_refused_segment_lookup_fails_the_send() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        server.with(|w| {
            w.lookup = LookupAnswer::Fail {
                error: apache_pulsar_protocol::proto::ServerError::AuthorizationError,
                message: "not you".into(),
            }
        });
        let error = producer.send(b"x".to_vec()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authorization, "{error}");
        client.close().await.unwrap();
    });
}

#[test]
fn a_layout_that_never_comes_times_create_out() {
    run(async {
        let mut world = world(1);
        world.silent_lookups = true;
        let server = WorldServer::start(world).await;
        let client = quick_client(&server).await;
        let error = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        // The watch closes its session, once.
        wait_until("the watch closes its session", || server.received(Type::SCALABLE_TOPIC_CLOSE) == 1).await;
        client.close().await.unwrap();
        assert_eq!(server.received(Type::SCALABLE_TOPIC_CLOSE), 1);
    });
}

/// A layout pushed again with the epoch already applied is ignored, and the producer carries on.
#[test]
fn a_repeated_layout_is_ignored() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::bytes()).topic(TOPIC).create().await.unwrap();
        producer.send(b"a".to_vec()).await.unwrap();
        server.with(|w| w.repush_layout(TOPIC));
        producer.send(b"b".to_vec()).await.unwrap();
        assert_eq!(server.received(Type::PRODUCER), 1, "nothing was re-created");
        client.close().await.unwrap();
    });
}

/// `Debug` names things without leaking secrets, for every handle.
#[test]
fn debug_output() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        assert!(format!("{client:?}").contains("local::PulsarClient"));
        let address = client.lookup(TOPIC).await.unwrap();
        let connection = client.connection(&address).await.unwrap();
        assert!(format!("{connection:?}").contains("ConnHandle"));
        assert!(format!("{:?}", Schema::string()).contains("String"));
        assert_eq!(format!("{:?}", Schema::bytes()), "Schema(bytes)");
        let id = crate::MessageId::earliest();
        assert_eq!(format!("{id:?}"), id.to_string());
        let watch = client.watches().open(&client, TOPIC).await.unwrap();
        assert!(format!("{watch:?}").contains(TOPIC));
        // A closed connection's handle refuses work.
        connection.close().await;
        let refused = connection.register(apache_pulsar_protocol::connection::IdKind::Producer).await;
        assert_eq!(refused.unwrap_err().kind(), ErrorKind::NotConnected);
        assert_eq!(connection.send(Default::default()).unwrap_err().kind(), ErrorKind::NotConnected);
        drop(watch);
        client.close().await.unwrap();
    });
}

#[test]
fn the_blocking_api_has_the_whole_producer() {
    use std::time::{SystemTime, UNIX_EPOCH};
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(1)).await;
        let url = server.url();
        (server, url)
    });
    let worker = std::thread::spawn(move || {
        let client = crate::blocking::PulsarClient::builder().service_url(url).event_loops(1).build().unwrap();
        assert!(format!("{client:?}").contains("PulsarClient"));
        let producer = client.new_producer(Schema::string()).topic(TOPIC).producer_name("sync").create().unwrap();
        let clone = producer.clone();
        assert!(format!("{clone:?}").contains(TOPIC));
        assert_eq!((producer.topic(), producer.producer_name()), (TOPIC, Some("sync")));
        let message = producer
            .new_message()
            .key("k")
            .value("all".to_string())
            .property("a", "1")
            .event_time(UNIX_EPOCH + Duration::from_secs(1))
            .sequence_id(7)
            .deliver_after(Duration::from_secs(1))
            .deliver_at(SystemTime::now())
            .replication_clusters(["here"]);
        assert!(format!("{message:?}").contains("\"k\""));
        message.send().unwrap();
        producer.flush().unwrap();
        assert_eq!(producer.last_sequence_id(), 7);
        producer.close().unwrap();
        client.close().unwrap();
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    worker.join().unwrap();
    let stored = rt.block_on(async { server.world.borrow().stored(TOPIC) });
    assert_eq!(stored[0].metadata.replicate_to, ["here"]);
}

/// An exclusive-with-fencing producer takes the topic from the exclusive one holding it, which is fenced: its
/// sends fail from then on.
#[test]
fn exclusive_with_fencing_takes_the_topic() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let exclusive = |mode| client.new_producer(Schema::string()).topic(TOPIC).access_mode(mode).create();
        let fenced = exclusive(ProducerAccessMode::Exclusive).await.unwrap();
        fenced.send("before".to_string()).await.unwrap();
        let fencing = exclusive(ProducerAccessMode::ExclusiveWithFencing).await.unwrap();
        fencing.send("mine now".to_string()).await.unwrap();
        let error = fenced.send("too late".to_string()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::ProducerFenced, "{error}");
        assert_eq!(stored_by_key(&server)[""], ["before", "mine now"]);
        client.close().await.unwrap();
    });
}

/// A wait-for-exclusive producer still in line when its client closes fails to create, and leaves the broker's
/// line: the producer the broker queued is closed.
#[test]
fn a_waiting_producer_leaves_the_line_when_its_client_closes() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let holder_client = local_client(&server).await;
        let holder = holder_client
            .new_producer(Schema::string())
            .topic(TOPIC)
            .access_mode(ProducerAccessMode::Exclusive)
            .create()
            .await
            .unwrap();
        let client = local_client(&server).await;
        let waiting = {
            let client = client.clone();
            compio::runtime::spawn(async move {
                let builder = client.new_producer(Schema::string()).topic(TOPIC);
                builder.access_mode(ProducerAccessMode::WaitForExclusive).create().await
            })
        };
        wait_until("the broker queues the waiting producer", || server.received(Type::PRODUCER) == 2).await;
        client.close().await.unwrap();
        assert_eq!(waiting.await.unwrap().unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(server.received(Type::CLOSE_PRODUCER), 1, "the queued producer is closed");
        // Nobody is in line: the holder's topic goes to no one when it closes.
        holder.close().await.unwrap();
        assert_eq!(server.received(Type::PRODUCER), 2);
        holder_client.close().await.unwrap();
    });
}

/// A watch held while its client closes stops, rather than open its session again.
#[test]
fn a_held_watch_stops_with_its_client() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let watch = client.watches().open(&client, TOPIC).await.unwrap();
        assert!(watch.is_running());
        client.close().await.unwrap();
        wait_until("the watch stops", || !watch.is_running()).await;
    });
}

/// A watch whose connection breaks opens its session again, with backoff, through the lookups that fail
/// meanwhile, and keeps its layout throughout.
#[test]
fn a_watch_reopens_its_session_through_failed_lookups() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let watch = client.watches().open(&client, TOPIC).await.unwrap();
        let lookups = server.received(Type::LOOKUP);
        let refused = LookupAnswer::Fail {
            error: apache_pulsar_protocol::proto::ServerError::AuthorizationError,
            message: "not now".into(),
        };
        let answer = server.with(|w| {
            for conn in w.open_connections() {
                w.kill(conn);
            }
            std::mem::replace(&mut w.lookup, refused)
        });
        wait_until("a lookup is refused", || server.received(Type::LOOKUP) > lookups).await;
        assert_eq!(watch.current().epoch(), 0);
        server.with(|w| w.lookup = answer);
        wait_until("the session is open again", || server.received(Type::SCALABLE_TOPIC_LOOKUP) == 2).await;
        assert!(watch.is_running());
        client.close().await.unwrap();
    });
}

/// A flush waits for the last send before it, even while the ones before that complete: here a message on a
/// segment whose producer is registered already, while the next waits for its segment's producer.
#[test]
fn flush_outlasts_the_sends_that_complete_first() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let key_in = |range: std::ops::RangeInclusive<u16>| {
            (0..).map(|i| format!("k{i}")).find(|k| range.contains(&KeyHash::of(k.as_bytes()).segment_hash())).unwrap()
        };
        let (low, high) = (key_in(0..=0x7fff), key_in(0x8000..=0xffff));
        producer.new_message().key(&low).value("warm".to_string()).send().await.unwrap();
        let first = producer.new_message().key(&low).value("first".to_string()).send();
        let last = producer.new_message().key(&high).value("last".to_string()).send();
        let (first, last, flushed) = futures_util::join!(first, last, producer.flush());
        first.unwrap();
        last.unwrap();
        flushed.unwrap();
        assert_eq!(server.received(Type::PRODUCER), 2);
        client.close().await.unwrap();
    });
}

/// A broker-side error on a watch's session, after its first layout, passes: the session stays, and the next
/// layout comes through it.
#[test]
fn a_watch_outlives_an_error_on_its_session() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let watch = client.watches().open(&client, TOPIC).await.unwrap();
        server.with(|w| w.fail_watchers(TOPIC, apache_pulsar_protocol::proto::ServerError::ServiceNotReady));
        server.with(|w| w.split(TOPIC, 0));
        wait_until("the split's layout is applied", || watch.current().epoch() == 1).await;
        assert_eq!(server.received(Type::SCALABLE_TOPIC_LOOKUP), 1);
        client.close().await.unwrap();
    });
}
