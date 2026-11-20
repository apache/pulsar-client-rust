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

//! Queue consumers over real loopback sockets, against the simulated world of `apache-pulsar-testkit`.
//!
//! The consumer's decisions are tested deterministically in the testkit; these tests check the driver: the
//! consumer's task, its subscribes, pushes, timers, receives and their cancellation, the client models and the
//! blocking API.

use std::time::{Duration, UNIX_EPOCH};

use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{KeyValue, MessageMetadata, ServerError};
use bytes::Bytes;
use futures_util::future::Either;
use futures_util::{FutureExt, StreamExt};

use crate::error::ErrorKind;
use crate::producer_tests::{TOPIC, WorldServer, local_client, run, wait_until, world};
use crate::{Message, PulsarClient, Schema, SubscriptionInitialPosition, local};

const SUB: &str = "workers";

/// A consumer of [`TOPIC`] from its start.
async fn consumer(client: &local::PulsarClient) -> local::QueueConsumer<String> {
    client
        .new_queue_consumer(Schema::string())
        .topic(TOPIC)
        .subscription_name(SUB)
        .subscription_initial_position(SubscriptionInitialPosition::Earliest)
        .subscribe()
        .await
        .unwrap()
}

/// Stores a one-message entry in a segment, as another client would.
fn publish(server: &WorldServer, segment: u64, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    publish_with(server, segment, metadata, value);
}

fn publish_with(server: &WorldServer, segment: u64, metadata: MessageMetadata, value: &str) {
    let payload = Bytes::copy_from_slice(value.as_bytes());
    server.with(|w| w.publish_raw(TOPIC, segment, metadata, payload));
}

fn values(messages: &[Message<String>]) -> Vec<String> {
    messages.iter().map(|m| m.value().unwrap()).collect()
}

fn backlog(server: &WorldServer, segment: u64) -> usize {
    server.world.borrow().backlog(TOPIC, segment, SUB)
}

fn consumers(server: &WorldServer, segment: u64) -> usize {
    server.world.borrow().consumer_count(TOPIC, segment, SUB)
}

#[test]
fn receives_what_a_producer_sends_and_acknowledges_it() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let consumer = client
            .new_queue_consumer(Schema::string())
            .topic(TOPIC)
            .subscription_name(SUB)
            .consumer_name("c")
            .subscribe()
            .await
            .unwrap();
        assert_eq!((consumer.topic(), consumer.subscription(), consumer.consumer_name()), (TOPIC, SUB, Some("c")));
        let producer = client.new_producer(Schema::string()).topic(TOPIC).producer_name("p").create().await.unwrap();
        let event_time = UNIX_EPOCH + Duration::from_secs(1_700_000_000);
        let message = producer.new_message().key("k").value("hello".to_string()).property("a", "b");
        let sent = message.event_time(event_time).send().await.unwrap();
        let message = consumer.receive().await.unwrap();
        assert_eq!(message.id(), &sent);
        assert_eq!((message.value().unwrap(), message.data()), ("hello".to_string(), &b"hello"[..]));
        assert_eq!((message.key(), message.properties().collect::<Vec<_>>()), (Some("k"), vec![("a", "b")]));
        assert_eq!(message.event_time(), Some(event_time));
        assert!(message.publish_time() > event_time);
        assert!(message.producer_name().unwrap().starts_with("p-seg-"), "{:?}", message.producer_name());
        // The scalable topic, not its segment.
        assert_eq!(message.topic(), TOPIC);
        assert_eq!((message.sequence_id(), message.redelivery_count(), message.size()), (0, 0, 5));
        assert_eq!(message.replicated_from(), None);
        consumer.acknowledge(message.id()).await.unwrap();
        wait_until("the ack is in", || backlog(&server, 0) + backlog(&server, 1) == 0).await;
        consumer.close().await.unwrap();
        consumer.close().await.unwrap();
        assert_eq!(consumer.receive().await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.acknowledge(message.id()).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.negative_acknowledge(message.id()).unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(server.received(Type::CLOSE_CONSUMER), 2);
        client.close().await.unwrap();
    });
}

#[test]
fn messages_show_what_their_metadata_says() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        let metadata = MessageMetadata {
            publish_time: 1_000,
            sequence_id: 7,
            replicated_from: Some("east".into()),
            partition_key: Some("k".into()),
            properties: vec![KeyValue { key: "a".into(), value: "b".into(), ..Default::default() }],
            ..Default::default()
        };
        publish_with(&server, 0, metadata, "x");
        let message = consumer.receive().await.unwrap();
        assert_eq!((message.key(), message.sequence_id(), message.replicated_from()), (Some("k"), 7, Some("east")));
        assert_eq!(message.publish_time(), UNIX_EPOCH + Duration::from_secs(1));
        assert_eq!((message.event_time(), message.producer_name()), (None, None));
        assert_eq!(message.properties().collect::<Vec<_>>(), [("a", "b")]);
        let debug = format!("{:?}", message.clone());
        assert!(debug.starts_with("Message { id: 0:") && debug.contains("size: 1"), "{debug}");
        client.close().await.unwrap();
    });
}

#[test]
fn receives_with_a_timeout_and_in_batches() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        assert!(consumer.receive_timeout(Duration::from_millis(20)).await.unwrap().is_none());
        assert!(consumer.receive_batch(3, Duration::from_millis(20)).await.unwrap().is_empty());
        let error = consumer.receive_batch(0, Duration::ZERO).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
        for i in 0..5 {
            publish(&server, 0, &format!("m{i}"));
        }
        // As soon as there are three.
        let batch = consumer.receive_batch(3, Duration::from_secs(60)).await.unwrap();
        assert_eq!(values(&batch), ["m0", "m1", "m2"]);
        // Two left: they come once the time is up.
        let batch = consumer.receive_batch(3, Duration::from_millis(50)).await.unwrap();
        assert_eq!(values(&batch), ["m3", "m4"]);
        publish(&server, 0, "m5");
        let message = consumer.receive_timeout(Duration::from_secs(60)).await.unwrap().unwrap();
        assert_eq!(message.value().unwrap(), "m5");
        client.close().await.unwrap();
    });
}

#[test]
fn a_receive_whose_time_is_up_takes_the_messages_in() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        for value in ["a", "b", "c"] {
            publish(&server, 0, value);
        }
        // Once they are in, a receive without time gets them, as a batch with time left does.
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "a");
        compio::time::sleep(Duration::from_millis(100)).await;
        let message = consumer.receive_timeout(Duration::ZERO).await.unwrap().unwrap();
        assert_eq!(message.value().unwrap(), "b");
        assert_eq!(values(&consumer.receive_batch(5, Duration::ZERO).await.unwrap()), ["c"]);
        client.close().await.unwrap();
    });
}

#[test]
fn a_dropped_receive_loses_no_message() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        // Dropped before a message comes: the message goes to the next receive.
        let mut early = Box::pin(consumer.receive());
        assert!((&mut early).now_or_never().is_none());
        drop(early);
        publish(&server, 0, "a");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "a");
        // Dropped once answered, before it is polled again: it gives the message back.
        let mut answered = Box::pin(consumer.receive());
        assert!((&mut answered).now_or_never().is_none());
        publish(&server, 0, "b");
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(answered);
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "b");
        // A batch dropped with some of its messages gives them back too.
        let mut batch = Box::pin(consumer.receive_batch(2, Duration::from_secs(60)));
        assert!((&mut batch).now_or_never().is_none());
        publish(&server, 0, "c");
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(batch);
        publish(&server, 0, "d");
        assert_eq!(values(&consumer.receive_batch(2, Duration::from_secs(60)).await.unwrap()), ["c", "d"]);
        client.close().await.unwrap();
    });
}

#[test]
fn a_negative_ack_brings_the_message_back() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = client
            .new_queue_consumer(Schema::string())
            .topic(TOPIC)
            .subscription_name(SUB)
            .subscription_initial_position(SubscriptionInitialPosition::Earliest)
            .negative_ack_redelivery_delay(Duration::from_millis(50))
            .acknowledgment_group_time(Duration::ZERO)
            .max_acknowledgment_group_size(10)
            .receiver_queue_size(10)
            .priority_level(1)
            .subscription_property("region", "eu")
            .property("app", "billing")
            .subscribe()
            .await
            .unwrap();
        publish(&server, 0, "a");
        let first = consumer.receive().await.unwrap();
        consumer.negative_acknowledge(first.id()).unwrap();
        let again = consumer.receive().await.unwrap();
        assert_eq!((again.id(), again.redelivery_count()), (first.id(), 1));
        // Without grouping, the ack goes out at once.
        consumer.acknowledge(again.id()).await.unwrap();
        wait_until("the ack is in", || backlog(&server, 0) == 0).await;
        {
            let world = server.world.borrow();
            let subscribe = &world.received(Type::SUBSCRIBE)[0].subscribe;
            assert_eq!((subscribe.priority_level, subscribe.subscription_properties.len()), (Some(1), 1));
            assert_eq!(world.received(Type::FLOW)[0].flow.messagePermits, 10);
        }
        client.close().await.unwrap();
    });
}

#[test]
fn the_stream_view_ends_when_the_consumer_closes() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        publish(&server, 0, "a");
        publish(&server, 1, "b");
        let mut stream = std::pin::pin!(consumer.stream());
        let mut got =
            vec![stream.next().await.unwrap().value().unwrap(), stream.next().await.unwrap().value().unwrap()];
        got.sort();
        assert_eq!(got, ["a", "b"]);
        consumer.close().await.unwrap();
        assert!(stream.next().await.is_none());
        client.close().await.unwrap();
    });
}

#[test]
fn follows_a_split_and_lets_go_of_the_drained_parent() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let mut expected = Vec::new();
        for round in ["before", "after"] {
            if round == "after" {
                server.with(|w| w.split(TOPIC, 0));
            }
            for i in 0..10 {
                let value = format!("{round}-{i}");
                producer.new_message().key(format!("k{i}")).value(value.clone()).send().await.unwrap();
                expected.push(value);
            }
        }
        let mut got = Vec::new();
        for _ in 0..20 {
            let message = consumer.receive().await.unwrap();
            consumer.acknowledge(message.id()).await.unwrap();
            got.push(message.value().unwrap());
        }
        got.sort();
        expected.sort();
        assert_eq!(got, expected);
        wait_until("the parent is let go", || consumers(&server, 0) == 0).await;
        assert_eq!((consumers(&server, 1), consumers(&server, 2)), (1, 1));
        client.close().await.unwrap();
    });
}

#[test]
fn dropping_the_last_handle_closes_the_consumer() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        let clone = consumer.clone();
        drop(consumer);
        compio::time::sleep(Duration::from_millis(20)).await;
        assert_eq!(consumers(&server, 0), 1);
        drop(clone);
        wait_until("the consumer is closed", || consumers(&server, 0) == 0).await;
        client.close().await.unwrap();
    });
}

#[test]
fn closing_the_client_closes_its_consumers_and_fails_their_receives() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        let mut receive = Box::pin(consumer.receive());
        assert!((&mut receive).now_or_never().is_none());
        client.close().await.unwrap();
        assert_eq!(receive.await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(server.received(Type::CLOSE_CONSUMER), 1);
    });
}

#[test]
fn a_lost_connection_brings_back_what_was_not_acknowledged() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        publish(&server, 0, "a");
        let first = consumer.receive().await.unwrap();
        server.with(|w| {
            for conn in w.open_connections() {
                w.kill(conn);
            }
        });
        let again = consumer.receive().await.unwrap();
        assert_eq!(again.id(), first.id());
        client.close().await.unwrap();
    });
}

#[test]
fn a_consumer_the_broker_closes_subscribes_again() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = consumer(&client).await;
        publish(&server, 0, "a");
        let first = consumer.receive().await.unwrap();
        // The broker unloads the segment: it closes its consumers, which come back.
        server.with(|w| w.unload(TOPIC, 0));
        let again = consumer.receive().await.unwrap();
        assert_eq!(again.id(), first.id());
        assert_eq!(server.received(Type::SUBSCRIBE), 2);
        client.close().await.unwrap();
    });
}

#[test]
fn closing_while_subscribing_closes_the_subscribes_on_their_way() {
    run(async {
        let mut world = world(1);
        world.hold_subscribes = true;
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let mut subscribe =
            Box::pin(client.new_queue_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe());
        let held = Box::pin(wait_until("the subscribe is held", || server.received(Type::SUBSCRIBE) == 1));
        assert!(matches!(futures_util::future::select(&mut subscribe, held).await, Either::Right(_)));
        client.close().await.unwrap();
        assert_eq!(subscribe.await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        // The broker discards the consumer it was creating.
        assert_eq!(server.received(Type::CLOSE_CONSUMER), 1);
        server.with(|w| w.release_subscribes());
        assert_eq!(consumers(&server, 0), 0);
    });
}

#[test]
fn a_refused_subscribe_fails_with_the_brokers_error_and_closes_the_rest() {
    run(async {
        let mut world = world(2);
        world.fail_subscribes.push_back(ServerError::AuthorizationError);
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let subscribe = client.new_queue_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe();
        let error = subscribe.await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authorization, "{error}");
        wait_until("the other segment's consumer is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        assert_eq!(consumers(&server, 1), 0);
        client.close().await.unwrap();
    });
}

#[test]
fn a_dropped_subscribe_closes_the_consumer() {
    run(async {
        let mut world = world(1);
        world.hold_subscribes = true;
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let mut subscribe =
            Box::pin(client.new_queue_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe());
        let held = Box::pin(wait_until("the subscribe is held", || server.received(Type::SUBSCRIBE) == 1));
        assert!(matches!(futures_util::future::select(&mut subscribe, held).await, Either::Right(_)));
        drop(subscribe);
        server.with(|w| {
            w.hold_subscribes = false;
            w.release_subscribes();
        });
        wait_until("the consumer is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        client.close().await.unwrap();
    });
}

#[test]
fn invalid_configuration_fails_subscribe() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let builder = || client.new_queue_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB);
        assert!(format!("{:?}", builder()).starts_with("QueueConsumerBuilder { settings: Settings { topic: Some("));
        let cases = [
            (client.new_queue_consumer(Schema::bytes()).subscription_name(SUB), "no topic"),
            (client.new_queue_consumer(Schema::bytes()).topic(TOPIC), "no subscription"),
            (builder().subscription_name(""), "an empty subscription"),
            (builder().receiver_queue_size(0), "no receiver queue"),
            (builder().max_acknowledgment_group_size(0), "no acknowledgment group"),
        ];
        for (builder, case) in cases {
            assert_eq!(builder.subscribe().await.unwrap_err().kind(), ErrorKind::InvalidConfiguration, "{case}");
        }
        assert_eq!(server.received(Type::SUBSCRIBE), 0);
        client.close().await.unwrap();
    });
}

#[test]
fn the_blocking_api_receives_from_a_plain_thread() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let server = WorldServer::start(world(2)).await;
        let url = server.url();
        (server, url)
    });
    publish(&server, 0, "a");
    publish(&server, 1, "b");
    let worker = std::thread::spawn(move || {
        let client = crate::blocking::PulsarClient::builder().service_url(url).build().unwrap();
        let consumer = client
            .new_queue_consumer(Schema::string())
            .topic(TOPIC)
            .subscription_name(SUB)
            .subscription_initial_position(SubscriptionInitialPosition::Earliest)
            .subscribe()
            .unwrap();
        assert_eq!((consumer.topic(), consumer.subscription(), consumer.consumer_name()), (TOPIC, SUB, None));
        assert!(format!("{:?}", consumer.clone()).starts_with("QueueConsumer {"));
        let first = consumer.receive().unwrap();
        let rest = consumer.receive_batch(5, Duration::from_millis(100)).unwrap();
        let mut got = vec![first.value().unwrap()];
        got.extend(values(&rest));
        got.sort();
        assert_eq!(got, ["a", "b"]);
        consumer.negative_acknowledge(first.id()).unwrap();
        for message in [&first].into_iter().chain(&rest) {
            consumer.acknowledge(message.id()).unwrap();
        }
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
    rt.block_on(async { wait_until("the acks are in", || backlog(&server, 0) + backlog(&server, 1) == 0).await });
}

#[test]
fn a_thread_safe_subscribe_dropped_on_its_way_closes_the_consumer() {
    let rt = compio::runtime::Runtime::new().unwrap();
    let (server, url) = rt.block_on(async {
        let mut world = world(1);
        world.hold_subscribes = true;
        let server = WorldServer::start(world).await;
        let url = server.url();
        (server, url)
    });
    let worker = std::thread::spawn(move || {
        let client = futures_executor::block_on(PulsarClient::builder().service_url(url).build()).unwrap();
        let subscribe = client.new_queue_consumer(Schema::bytes()).topic(TOPIC).subscription_name(SUB).subscribe();
        // Gone after its first poll, which sends it to a loop.
        assert!(subscribe.now_or_never().is_none());
        assert_eq!(client.loads(), [0]);
        client
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let client = worker.join().unwrap();
    rt.block_on(async {
        wait_until("the subscribe is held", || server.received(Type::SUBSCRIBE) == 1).await;
        server.with(|w| {
            w.hold_subscribes = false;
            w.release_subscribes();
        });
        wait_until("the consumer is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
    });
    let close = std::thread::spawn(move || futures_executor::block_on(client.close()));
    rt.block_on(async {
        while !close.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    close.join().unwrap().unwrap();
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
        publish(&server, i % 2, &format!("m{i}"));
    }
    let worker = std::thread::spawn(move || {
        futures_executor::block_on(async {
            let client = PulsarClient::builder().service_url(url).event_loops(2).build().await.unwrap();
            let consumer = client
                .new_queue_consumer(Schema::string())
                .topic(TOPIC)
                .subscription_name(SUB)
                .subscription_initial_position(SubscriptionInitialPosition::Earliest)
                .subscribe()
                .await
                .unwrap();
            assert_eq!(client.loads().iter().sum::<usize>(), 1);
            // Consumers are Send + Sync: receive from other threads.
            let got = std::thread::scope(|threads| {
                let receivers: Vec<_> = (0..2)
                    .map(|_| {
                        let consumer = &consumer;
                        threads.spawn(move || {
                            futures_executor::block_on(async {
                                let a = consumer.receive().await.unwrap();
                                let b = consumer.receive().await.unwrap();
                                vec![a.value().unwrap(), b.value().unwrap()]
                            })
                        })
                    })
                    .collect();
                receivers.into_iter().flat_map(|r| r.join().unwrap()).collect::<Vec<_>>()
            });
            consumer.close().await.unwrap();
            client.close().await.unwrap();
            got
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
fn thread_safe_types_are_send(consumer: &crate::QueueConsumer<String>, id: &crate::MessageId) {
    fn send_sync<T: Send + Sync>(_: &T) {}
    fn send<T: Send>(_: T) {}
    send_sync(consumer);
    send(consumer.receive());
    send(consumer.receive_batch(1, Duration::ZERO));
    send(consumer.acknowledge(id));
    send(consumer.close());
    send(consumer.stream());
}
