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

//! Stream consumers over real loopback sockets, against the simulated world of `apache-pulsar-testkit` and its
//! controller.
//!
//! The consumer's decisions are tested deterministically in the testkit; these tests check the driver: the
//! registration with the controller, the segments' subscribes and pushes, timers, receives and their cancellation,
//! the client models and the blocking API.

use std::time::Duration;

use apache_pulsar_protocol::hash::KeyHash;
use apache_pulsar_protocol::lookup::HostPort;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{MessageMetadata, ServerError};
use apache_pulsar_testkit::LookupAnswer;
use bytes::Bytes;
use futures_util::future::Either;
use futures_util::{FutureExt, StreamExt};

use crate::error::ErrorKind;
use crate::producer_tests::{TOPIC, WorldServer, local_client, run, wait_until, world};
use crate::{Message, PulsarClient, Schema, SubscriptionInitialPosition, local};

const SUB: &str = "billing";

/// A Stream consumer of [`TOPIC`] from its start, named `name`.
async fn consumer(client: &local::PulsarClient, name: &str) -> local::StreamConsumer<String> {
    client
        .new_stream_consumer(Schema::string())
        .topic(TOPIC)
        .subscription_name(SUB)
        .consumer_name(name)
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .acknowledgment_group_time(Duration::ZERO)
        .subscribe()
        .await
        .unwrap()
}

/// Stores a one-message entry with `key` in a segment, stamped as a producer stamps it.
fn publish(server: &WorldServer, segment: u64, key: &str, value: &str) {
    let hash = u32::from(KeyHash::of(key.as_bytes()).bucket_hash());
    let metadata = MessageMetadata {
        producer_name: "other".into(),
        publish_time: 1,
        partition_key: Some(key.into()),
        entry_hash_min: Some(hash),
        entry_hash_max: Some(hash),
        ..Default::default()
    };
    server.with(|w| w.publish_raw(TOPIC, segment, metadata, Bytes::copy_from_slice(value.as_bytes())));
}

fn values(messages: &[Message<String>]) -> Vec<String> {
    messages.iter().map(|m| m.value().unwrap()).collect()
}

fn backlog(server: &WorldServer, segment: u64) -> usize {
    server.world.borrow().backlog(TOPIC, segment, SUB)
}

#[test]
fn receives_in_order_and_acknowledges_cumulatively() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        publish(&server, 0, "a", "a0");
        publish(&server, 1, "b", "b0");
        publish(&server, 0, "a", "a1");
        let consumer = consumer(&client, "solo").await;
        assert_eq!((consumer.topic(), consumer.subscription(), consumer.consumer_name()), (TOPIC, SUB, "solo"));
        assert!(format!("{:?}", consumer.clone()).starts_with("StreamConsumer {"));
        let mut got = Vec::new();
        for _ in 0..3 {
            got.push(consumer.receive().await.unwrap());
        }
        let mut a: Vec<String> = values(&got).into_iter().filter(|v| v.starts_with('a')).collect();
        assert_eq!(a, ["a0", "a1"], "a key keeps its order");
        a.clear();
        assert_eq!(got[0].topic(), TOPIC);
        // The last message's vector covers both segments.
        consumer.acknowledge_cumulative(got[2].id()).await.unwrap();
        wait_until("the acks are in", || backlog(&server, 0) + backlog(&server, 1) == 0).await;
        consumer.close().await.unwrap();
        consumer.close().await.unwrap();
        assert_eq!(server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE), 1);
        assert_eq!(consumer.receive().await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.acknowledge_cumulative(got[0].id()).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        client.close().await.unwrap();
    });
}

/// Without a name, a consumer gets a random one, which it registers under.
#[test]
fn a_consumer_without_a_name_gets_one() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let builder = client.new_stream_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB);
        assert!(format!("{builder:?}").starts_with("StreamConsumerBuilder { settings: Settings { topic: Some("));
        let builder = builder.subscription_property("team", "billing").property("host", "a");
        let consumer = builder.consumer_name("").subscribe().await.unwrap();
        let name = consumer.consumer_name().to_string();
        assert!(name.starts_with("v5-stream-") && name.len() == 18, "{name}");
        assert_eq!(server.world.borrow().assigned(TOPIC, SUB, &name), [(0, false)]);
        let subscribe = server.world.borrow().received(Type::SUBSCRIBE)[0].subscribe.clone();
        assert_eq!(subscribe.consumer_name, Some(format!("{name}-seg-0")));
        assert_eq!(
            (subscribe.subscription_properties[0].key.as_str(), subscribe.metadata[0].key.as_str()),
            ("team", "host")
        );
        client.close().await.unwrap();
    });
}

#[test]
fn receives_with_a_timeout_and_in_batches() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client, "solo").await;
        assert!(consumer.receive_timeout(Duration::from_millis(20)).await.unwrap().is_none());
        for i in 0..3 {
            publish(&server, 0, "k", &format!("m{i}"));
        }
        let batch = consumer.receive_batch(5, Duration::from_millis(200)).await.unwrap();
        assert_eq!(values(&batch), ["m0", "m1", "m2"]);
        assert_eq!(
            consumer.receive_batch(0, Duration::ZERO).await.unwrap_err().kind(),
            ErrorKind::InvalidConfiguration
        );
        publish(&server, 0, "k", "m3");
        let message = consumer.receive_timeout(Duration::from_secs(5)).await.unwrap().unwrap();
        assert_eq!(message.value().unwrap(), "m3");
        client.close().await.unwrap();
    });
}

#[test]
fn a_dropped_receive_loses_no_message() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client, "solo").await;
        // Dropped before a message comes: the message goes to the next receive.
        let mut early = Box::pin(consumer.receive());
        assert!((&mut early).now_or_never().is_none());
        drop(early);
        compio::time::sleep(Duration::from_millis(20)).await;
        // Dropped once answered, before it is polled again: it gives the message back.
        let mut answered = Box::pin(consumer.receive());
        assert!((&mut answered).now_or_never().is_none());
        publish(&server, 0, "k", "a");
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(answered);
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "a");
        // The stream view ends once the consumer closes.
        publish(&server, 0, "k", "b");
        let mut stream = Box::pin(consumer.stream());
        assert_eq!(stream.next().await.unwrap().value().unwrap(), "b");
        consumer.close().await.unwrap();
        assert!(stream.next().await.is_none());
        client.close().await.unwrap();
    });
}

/// After a split, the consumer reads the parent to its end, acknowledges it, and moves to the children, which kept
/// what producers stored there meanwhile.
#[test]
fn follows_a_split_once_the_parent_is_drained() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client, "solo").await;
        publish(&server, 0, "k", "before");
        let first = consumer.receive().await.unwrap();
        let (low, high) = server.with(|w| w.split(TOPIC, 0));
        let key = |segment: u64| {
            let child = |k: &String| (KeyHash::of(k.as_bytes()).segment_hash() >= 0x8000) == (segment == high);
            (0..).map(|i| format!("k{i}")).find(child).unwrap()
        };
        publish(&server, low, &key(low), "low");
        publish(&server, high, &key(high), "high");
        assert!(consumer.receive_timeout(Duration::from_millis(100)).await.unwrap().is_none());
        consumer.acknowledge_cumulative(first.id()).await.unwrap();
        let mut rest = values(&consumer.receive_batch(2, Duration::from_secs(5)).await.unwrap());
        rest.sort();
        assert_eq!(rest, ["high", "low"]);
        wait_until("the parent is let go of", || server.world.borrow().consumer_count(TOPIC, 0, SUB) == 0).await;
        client.close().await.unwrap();
    });
}

/// The controller the layout names is connected to directly; behind a proxy, the consumer pairs with any broker.
#[test]
fn finds_the_controller_the_layout_names_or_any_broker_behind_a_proxy() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.controller.url = Some(server.url()));
        let client = local_client(&server).await;
        consumer(&client, "named").await;
        let proxied = |w: &apache_pulsar_testkit::scalable::World| {
            w.received(Type::CONNECT).iter().filter(|c| c.connect.proxy_to_broker_url.as_deref() == Some("")).count()
        };
        assert_eq!(proxied(&server.world.borrow()), 0);
        client.close().await.unwrap();

        let server = WorldServer::start(world(1)).await;
        let broker = HostPort { host: "broker.internal".into(), port: 6650 };
        server.with(|w| w.lookup = LookupAnswer::Connect { broker, proxy: true });
        let client = local_client(&server).await;
        consumer(&client, "proxied").await;
        assert_eq!(proxied(&server.world.borrow()), 1);
        client.close().await.unwrap();
    });
}

/// A refused registration fails the subscribe with the controller's error; a builder missing what it needs fails
/// before anything is sent.
#[test]
fn a_refused_registration_or_a_bad_builder_fails_the_subscribe() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.controller.fail_registrations.push_back(ServerError::AuthorizationError));
        let client = local_client(&server).await;
        let subscribe = client.new_stream_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe();
        assert_eq!(subscribe.await.unwrap_err().kind(), ErrorKind::Authorization);
        for builder in [
            client.new_stream_consumer(Schema::bytes()).subscription_name(SUB),
            client.new_stream_consumer(Schema::bytes()).topic(TOPIC),
        ] {
            assert_eq!(builder.subscribe().await.unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        }
        assert_eq!(server.received(Type::SCALABLE_TOPIC_SUBSCRIBE), 1);
        client.close().await.unwrap();
    });
}

/// After a connection loss the consumer registers again, and its segments subscribe again.
#[test]
fn a_lost_connection_registers_again() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client, "solo").await;
        publish(&server, 0, "k", "a");
        let first = consumer.receive().await.unwrap();
        consumer.acknowledge_cumulative(first.id()).await.unwrap();
        wait_until("the ack is in", || backlog(&server, 0) == 0).await;
        server.with(|w| {
            for conn in w.open_connections() {
                w.kill(conn);
            }
        });
        wait_until("it registers again", || server.received(Type::SCALABLE_TOPIC_SUBSCRIBE) == 2).await;
        publish(&server, 0, "k", "b");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "b");
        // The broker closes the segment's consumer: it subscribes again.
        let subscribes = server.received(Type::SUBSCRIBE);
        server.with(|w| w.unload(TOPIC, 0));
        wait_until("it subscribes again", || server.received(Type::SUBSCRIBE) == subscribes + 1).await;
        client.close().await.unwrap();
    });
}

#[test]
fn dropping_the_last_handle_closes_the_consumer() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client, "solo").await;
        drop(consumer.clone());
        compio::time::sleep(Duration::from_millis(50)).await;
        assert_eq!(server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE), 0);
        drop(consumer);
        wait_until("it left the group", || server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE) == 1).await;
        client.close().await.unwrap();
    });
}

/// A subscribe dropped while its registration waits: the consumer leaves the group once registered.
#[test]
fn a_dropped_subscribe_closes_the_consumer() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.controller.hold_registrations = true);
        let client = local_client(&server).await;
        let mut subscribe =
            Box::pin(client.new_stream_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe());
        let held =
            Box::pin(wait_until("the registration is held", || server.received(Type::SCALABLE_TOPIC_SUBSCRIBE) == 1));
        assert!(matches!(futures_util::future::select(&mut subscribe, held).await, Either::Right(_)));
        drop(subscribe);
        server.with(|w| w.release_registrations());
        wait_until("it left the group", || server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE) == 1).await;
        wait_until("its segment is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        client.close().await.unwrap();
    });
}

/// Closing while a segment's subscribe is on its way: the consumer leaves the group, and closes the subscribe as it
/// goes, which the broker still had to answer.
#[test]
fn closing_while_subscribing_closes_the_subscribes_on_their_way() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.hold_subscribes = true);
        let client = local_client(&server).await;
        let subscribe = client.new_stream_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe();
        let mut subscribe = Box::pin(subscribe);
        let held = Box::pin(wait_until("the subscribe is held", || server.received(Type::SUBSCRIBE) == 1));
        assert!(matches!(futures_util::future::select(&mut subscribe, held).await, Either::Right(_)));
        client.close().await.unwrap();
        assert_eq!(subscribe.await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE), 1);
        assert_eq!(server.received(Type::CLOSE_CONSUMER), 1);
    });
}

/// A segment's subscribe refused while another member still holds the segment is tried again.
#[test]
fn a_busy_segment_is_subscribed_again() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.fail_subscribes.push_back(ServerError::ConsumerBusy));
        let client = local_client(&server).await;
        consumer(&client, "solo").await;
        assert_eq!(server.received(Type::SUBSCRIBE), 2);
        client.close().await.unwrap();
    });
}

#[test]
fn the_blocking_api_receives_from_a_plain_thread() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(1)).await;
        let url = server.url();
        (server, url)
    });
    publish(&server, 0, "k", "a");
    publish(&server, 0, "k", "b");
    let worker = std::thread::spawn(move || {
        let client = crate::blocking::PulsarClient::builder().service_url(url).build().unwrap();
        let consumer = client
            .new_stream_consumer(Schema::string())
            .topic(TOPIC)
            .subscription_name(SUB)
            .consumer_name("blocking")
            .subscription_initial_position(SubscriptionInitialPosition::Earliest)
            .subscribe()
            .unwrap();
        assert_eq!((consumer.topic(), consumer.subscription(), consumer.consumer_name()), (TOPIC, SUB, "blocking"));
        assert!(format!("{:?}", consumer.clone()).starts_with("StreamConsumer {"));
        let first = consumer.receive().unwrap();
        let rest = consumer.receive_batch(5, Duration::from_millis(100)).unwrap();
        assert_eq!((first.value().unwrap(), values(&rest)), ("a".to_string(), vec!["b".to_string()]));
        consumer.acknowledge_cumulative(rest[0].id()).unwrap();
        assert!(consumer.receive_timeout(Duration::from_millis(10)).unwrap().is_none());
        consumer.close().unwrap();
        client.close().unwrap();
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    worker.join().unwrap();
    rt.block_on(async { wait_until("the acks are in", || backlog(&server, 0) == 0).await });
}

#[test]
fn thread_safe_consumers_receive_on_any_thread() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(2)).await;
        let url = server.url();
        (server, url)
    });
    for i in 0..4 {
        publish(&server, i % 2, &format!("k{}", i % 2), &format!("m{i}"));
    }
    let worker = std::thread::spawn(move || {
        futures_executor::block_on(async {
            let client = PulsarClient::builder().service_url(url).event_loops(2).build().await.unwrap();
            let consumer = client
                .new_stream_consumer(Schema::string())
                .topic(TOPIC)
                .subscription_name(SUB)
                .subscription_initial_position(SubscriptionInitialPosition::Earliest)
                .subscribe()
                .await
                .unwrap();
            assert_eq!(client.loads().iter().sum::<usize>(), 1);
            let mut got = Vec::new();
            for _ in 0..4 {
                got.push(consumer.receive().await.unwrap());
            }
            consumer.acknowledge_cumulative(got[3].id()).await.unwrap();
            consumer.close().await.unwrap();
            client.close().await.unwrap();
            values(&got)
        })
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let mut got = worker.join().unwrap();
    got.sort();
    assert_eq!(got, ["m0", "m1", "m2", "m3"]);
}

/// Thread-safe consumers, their messages and their futures can move across threads.
#[allow(dead_code)]
fn thread_safe_types_are_send(consumer: &crate::StreamConsumer<String>, id: &crate::MessageId) {
    fn send_sync<T: Send + Sync>(_: &T) {}
    fn send<T: Send>(_: T) {}
    send_sync(consumer);
    send(consumer.receive());
    send(consumer.receive_batch(1, Duration::ZERO));
    send(consumer.acknowledge_cumulative(id));
    send(consumer.close());
    send(consumer.stream());
}
