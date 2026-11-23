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

//! Checkpoint consumers over real loopback sockets, against the simulated world of `apache-pulsar-testkit`.
//!
//! The consumer's decisions are tested deterministically in the testkit; these tests check what the driver adds for
//! it: its readers' last message ids, its checkpoints and how receives move them, and the client models.

use std::time::Duration;

use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{FeatureFlags, MessageMetadata, ServerError};
use apache_pulsar_testkit::LookupAnswer;
use apache_pulsar_testkit::scalable::World;
use bytes::Bytes;
use futures_util::{FutureExt, StreamExt};

use crate::error::ErrorKind;
use crate::producer_tests::{TOPIC, WorldServer, local_client, run, wait_until, world};
use crate::{Checkpoint, Message, PulsarClient, Schema, local};

/// A consumer of [`TOPIC`] from `start`.
async fn reader(client: &local::PulsarClient, start: Checkpoint) -> local::CheckpointConsumer<String> {
    client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).start_position(start).create().await.unwrap()
}

/// Stores a one-message entry in a segment, as another client would.
fn publish(server: &WorldServer, segment: u64, value: &str) {
    let metadata = MessageMetadata { producer_name: "other".into(), publish_time: 1, ..Default::default() };
    let payload = Bytes::copy_from_slice(value.as_bytes());
    server.with(|w| w.publish_raw(TOPIC, segment, metadata, payload));
}

fn values(messages: &[Message<String>]) -> Vec<String> {
    messages.iter().map(|m| m.value().unwrap()).collect()
}

fn sorted(mut values: Vec<String>) -> Vec<String> {
    values.sort();
    values
}

#[test]
fn resumes_from_a_checkpoint() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        for i in 0..6 {
            publish(&server, i % 2, &format!("m{i}"));
        }
        let client = local_client(&server).await;
        let builder = client
            .new_checkpoint_consumer(Schema::string())
            .topic(TOPIC)
            .start_position(Checkpoint::earliest())
            .consumer_name("orders-reader");
        assert!(format!("{builder:?}").starts_with("CheckpointConsumerBuilder {"));
        let consumer = builder.create().await.unwrap();
        assert_eq!((consumer.topic(), consumer.consumer_name()), (TOPIC, Some("orders-reader")));
        assert!(format!("{:?}", consumer.clone()).starts_with("CheckpointConsumer {"));
        let first = consumer.receive_batch(4, Duration::from_secs(60)).await.unwrap();
        let checkpoint = consumer.checkpoint().await.unwrap();
        consumer.close().await.unwrap();
        // Closing again is fine, and sends nothing more (C14).
        consumer.close().await.unwrap();
        assert_eq!(server.received(Type::CLOSE_CONSUMER), 2);
        assert_eq!(consumer.receive().await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.receive_timeout(Duration::ZERO).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.receive_batch(1, Duration::ZERO).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(consumer.checkpoint().await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        // Restored from its bytes, as an application keeps it.
        let resumed = reader(&client, Checkpoint::from_bytes(&checkpoint.to_bytes()).unwrap()).await;
        let rest = resumed.receive_batch(6, Duration::from_millis(200)).await.unwrap();
        let mut all = values(&first);
        all.extend(values(&rest));
        assert_eq!(sorted(all), ["m0", "m1", "m2", "m3", "m4", "m5"]);
        client.close().await.unwrap();
    });
}

/// The latest position is where the segment's last message id says, asked once the reader subscribes: a checkpoint
/// taken right away restores what the consumer reads.
#[test]
fn a_latest_consumer_reads_what_comes_after_it_and_its_checkpoint_says_so() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        publish(&server, 0, "old");
        let client = local_client(&server).await;
        // Latest is the default.
        let consumer = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).create().await.unwrap();
        assert_eq!(server.received(Type::GET_LAST_MESSAGE_ID), 1);
        let at_start = consumer.checkpoint().await.unwrap();
        publish(&server, 0, "new");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "new");
        let restored = reader(&client, at_start).await;
        assert_eq!(restored.receive().await.unwrap().value().unwrap(), "new");
        assert!(restored.receive_timeout(Duration::from_millis(50)).await.unwrap().is_none());
        client.close().await.unwrap();
    });
}

/// Checkpoint consumers have no subscription on the brokers: two from the earliest position don't share the messages,
/// each reads every one, on every segment. Java's `V5MultipleConsumersTest.testCheckpointConsumersEachSeeFullStream`.
#[test]
fn consumers_from_the_earliest_position_each_read_every_message() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let sent: Vec<String> = (0..10).map(|i| format!("m{i}")).collect();
        for (i, value) in sent.iter().enumerate() {
            publish(&server, i as u64 % 2, value);
        }
        let client = local_client(&server).await;
        let a = reader(&client, Checkpoint::earliest()).await;
        let b = reader(&client, Checkpoint::earliest()).await;
        for consumer in [&a, &b] {
            let got = consumer.receive_batch(sent.len(), Duration::from_secs(60)).await.unwrap();
            assert_eq!(sorted(values(&got)), sent);
        }
        client.close().await.unwrap();
    });
}

/// A split seals the parent: once its reader is at the end the broker gives for it, the reader is let go of, and
/// the children's messages come.
#[test]
fn follows_a_split_and_reads_the_parent_first() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::earliest()).await;
        publish(&server, 0, "parent");
        let (low, high) = server.with(|w| w.split(TOPIC, 0));
        publish(&server, low, "low");
        publish(&server, high, "high");
        let first = consumer.receive().await.unwrap();
        assert_eq!(first.value().unwrap(), "parent");
        let rest = consumer.receive_batch(2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(sorted(values(&rest)), ["high", "low"]);
        wait_until("the parent's reader is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        client.close().await.unwrap();
    });
}

/// A latest consumer of a topic with a sealed parent: the broker tells the parent's reader, at its end, that the topic
/// ended.
#[test]
fn a_latest_consumer_lets_go_of_a_sealed_parent_at_its_end() {
    run(async {
        let mut world = world(1);
        let (low, _) = world.split(TOPIC, 0);
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::latest()).await;
        wait_until("the parent's reader is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        publish(&server, low, "after");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "after");
        client.close().await.unwrap();
    });
}

/// Messages a dropped receive gives back don't count in a checkpoint until a receive returns them.
#[test]
fn a_dropped_receive_gives_its_messages_back_and_moves_no_checkpoint() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::earliest()).await;
        let at_start = consumer.checkpoint().await.unwrap();
        let mut answered = Box::pin(consumer.receive_batch(2, Duration::from_secs(60)));
        assert!((&mut answered).now_or_never().is_none());
        publish(&server, 0, "a");
        publish(&server, 0, "b");
        compio::time::sleep(Duration::from_millis(100)).await;
        drop(answered);
        assert_eq!(consumer.checkpoint().await.unwrap(), at_start);
        assert_eq!(values(&consumer.receive_batch(2, Duration::from_secs(60)).await.unwrap()), ["a", "b"]);
        assert_ne!(consumer.checkpoint().await.unwrap(), at_start);
        client.close().await.unwrap();
    });
}

/// A last message id the broker fails to give is asked again a second later.
#[test]
fn a_last_message_id_the_broker_fails_to_give_is_asked_again() {
    run(async {
        let mut world = world(1);
        world.fail_last_message_ids.push_back(ServerError::ServiceNotReady);
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::latest()).await;
        assert_eq!(server.received(Type::GET_LAST_MESSAGE_ID), 2);
        publish(&server, 0, "new");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "new");
        client.close().await.unwrap();
    });
}

#[test]
fn a_refused_reader_fails_the_create_and_closes_the_rest() {
    run(async {
        let mut world = world(2);
        let refused = [ServerError::AuthorizationError].into();
        world.fail_subscribes_to.insert("segment://acme/orders/events/8000-ffff-1".into(), refused);
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let error = client
            .new_checkpoint_consumer(Schema::string())
            .topic(TOPIC)
            .start_position(Checkpoint::earliest())
            .create()
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authorization);
        wait_until("the other reader is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        let error = client.new_checkpoint_consumer(Schema::string()).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
        client.close().await.unwrap();
    });
}

/// With auto-creation off, a create on a topic that doesn't exist fails with NotFound. Java's
/// `V5ScalableTopicAutoCreateTest.testNoAutoCreateWhenNamespaceDisallows`.
#[test]
fn a_missing_topic_is_not_found() {
    run(async {
        let mut world = World::default();
        world.auto_create = false;
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let error = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "{error}");
        client.close().await.unwrap();
    });
}

/// A broker that doesn't advertise scalable topics is refused: the create fails before it asks the broker for the
/// topic's layout (C15).
#[test]
fn a_broker_without_scalable_topics_is_refused() {
    run(async {
        let mut world = world(1);
        world.features = FeatureFlags { supports_scalable_topics: Some(false), ..Default::default() };
        let server = WorldServer::start(world).await;
        let client = local_client(&server).await;
        let error = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).create().await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedBroker, "{error}");
        assert_eq!((server.received(Type::SCALABLE_TOPIC_LOOKUP), server.received(Type::SUBSCRIBE)), (0, 0));
        client.close().await.unwrap();
    });
}

/// A reader that loses its connection subscribes again after what the application got.
#[test]
fn a_lost_connection_resumes_after_what_was_received() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::earliest()).await;
        for value in ["a", "b", "c"] {
            publish(&server, 0, value);
        }
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "a");
        server.with(|w| {
            for conn in w.open_connections() {
                w.kill(conn);
            }
        });
        let rest = consumer.receive_batch(2, Duration::from_secs(60)).await.unwrap();
        assert_eq!(values(&rest), ["b", "c"]);
        client.close().await.unwrap();
    });
}

#[test]
fn receives_with_a_timeout_in_batches_and_as_a_stream() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = reader(&client, Checkpoint::earliest()).await;
        assert!(consumer.receive_timeout(Duration::from_millis(20)).await.unwrap().is_none());
        let error = consumer.receive_batch(0, Duration::ZERO).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
        publish(&server, 0, "a");
        let message = consumer.receive_timeout(Duration::from_secs(60)).await.unwrap().unwrap();
        assert_eq!(message.value().unwrap(), "a");
        publish(&server, 0, "b");
        let stream = consumer.stream();
        futures_util::pin_mut!(stream);
        assert_eq!(stream.next().await.unwrap().value().unwrap(), "b");
        let closing = consumer.clone();
        compio::runtime::spawn(async move { closing.close().await }).detach();
        assert!(stream.next().await.is_none());
        client.close().await.unwrap();
    });
}

#[test]
fn the_blocking_api_reads_from_a_plain_thread() {
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
            .new_checkpoint_consumer(Schema::string())
            .topic(TOPIC)
            .start_position(Checkpoint::earliest())
            .create()
            .unwrap();
        assert_eq!((consumer.topic(), consumer.consumer_name()), (TOPIC, None));
        assert!(format!("{:?}", consumer.clone()).starts_with("CheckpointConsumer {"));
        let first = consumer.receive().unwrap();
        let rest = consumer.receive_batch(5, Duration::from_millis(100)).unwrap();
        let mut got = vec![first.value().unwrap()];
        got.extend(values(&rest));
        assert_eq!(sorted(got), ["a", "b"]);
        assert!(consumer.receive_timeout(Duration::from_millis(10)).unwrap().is_none());
        let checkpoint = consumer.checkpoint().unwrap();
        consumer.close().unwrap();
        let resumed =
            client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).start_position(checkpoint).create().unwrap();
        assert!(resumed.receive_timeout(Duration::from_millis(50)).unwrap().is_none());
        client.close().unwrap();
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    worker.join().unwrap();
}

#[test]
fn thread_safe_consumers_read_on_any_thread() {
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
                .new_checkpoint_consumer(Schema::string())
                .topic(TOPIC)
                .start_position(Checkpoint::earliest())
                .create()
                .await
                .unwrap();
            assert_eq!(client.loads().iter().sum::<usize>(), 1);
            let got = std::thread::scope(|threads| {
                let consumer = &consumer;
                let receiver = threads.spawn(move || {
                    futures_executor::block_on(consumer.receive_batch(4, Duration::from_secs(60))).unwrap()
                });
                values(&receiver.join().unwrap())
            });
            let checkpoint = consumer.checkpoint().await.unwrap();
            consumer.close().await.unwrap();
            client.close().await.unwrap();
            (got, checkpoint)
        })
    });
    rt.block_on(async {
        while !worker.is_finished() {
            compio::time::sleep(Duration::from_millis(5)).await;
        }
    });
    let (got, checkpoint) = worker.join().unwrap();
    assert_eq!(sorted(got), ["m0", "m1", "m2", "m3"]);
    assert_eq!(format!("{checkpoint:?}"), "Checkpoint{0: 101:1, 1: 102:1}");
}

const GROUP: &str = "audit";

/// A member of [`GROUP`] named `name`, from the earliest position.
async fn member(client: &local::PulsarClient, name: &str) -> local::CheckpointConsumer<String> {
    let builder = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).consumer_group(GROUP);
    builder.consumer_name(name).start_position(Checkpoint::earliest()).create().await.unwrap()
}

fn assigned(server: &WorldServer, name: &str) -> Vec<u64> {
    server.world.borrow().assigned(TOPIC, GROUP, name).into_iter().map(|(segment, _)| segment).collect()
}

/// The members of a group share the segments: each reads those the controller assigns it, which move as members come
/// and go, and a member leaves the group as it closes.
#[test]
fn members_of_a_group_share_the_segments() {
    run(async {
        let server = WorldServer::start(world(2)).await;
        let client = local_client(&server).await;
        let first = member(&client, "member-0").await;
        assert_eq!((first.consumer_name(), assigned(&server, "member-0")), (Some("member-0"), vec![0, 1]));
        assert_eq!(server.received(Type::SCALABLE_TOPIC_SUBSCRIBE), 1);
        let second = member(&client, "member-1").await;
        // Segment 1 moved: the first member's reader of it is closed.
        wait_until("the moved segment's reader is closed", || server.received(Type::CLOSE_CONSUMER) == 1).await;
        assert_eq!((assigned(&server, "member-0"), assigned(&server, "member-1")), (vec![0], vec![1]));
        publish(&server, 0, "zero");
        publish(&server, 1, "one");
        assert_eq!(first.receive().await.unwrap().value().unwrap(), "zero");
        assert_eq!(second.receive().await.unwrap().value().unwrap(), "one");
        assert_eq!(format!("{:?}", second.checkpoint().await.unwrap()), "Checkpoint{1: 102:0}");
        second.close().await.unwrap();
        assert_eq!(server.received(Type::SCALABLE_TOPIC_UNSUBSCRIBE), 1);
        // Its segment came back: the first member reads it from its own start.
        assert_eq!(first.receive().await.unwrap().value().unwrap(), "one");
        client.close().await.unwrap();
    });
}

/// A member without a name gets a random one, which it registers under, and names its readers after.
#[test]
fn a_member_without_a_name_gets_one() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let builder = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).consumer_group(GROUP);
        let consumer = builder.consumer_name("").create().await.unwrap();
        let name = consumer.consumer_name().unwrap().to_string();
        assert!(name.starts_with("v5-checkpoint-") && name.len() == 22, "{name}");
        assert_eq!(assigned(&server, &name), [0]);
        let subscribe = server.world.borrow().received(Type::SUBSCRIBE)[0].subscribe.clone();
        assert_eq!(subscribe.consumer_name, Some(format!("{name}-seg-0")));
        // An empty group is none.
        let alone = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).consumer_group("").create().await;
        assert_eq!(alone.unwrap().consumer_name(), None);
        assert_eq!(server.received(Type::SCALABLE_TOPIC_SUBSCRIBE), 1);
        client.close().await.unwrap();
    });
}

/// A registration the controller refuses fails the create with its error.
#[test]
fn a_refused_registration_fails_the_create() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| w.controller.fail_registrations.push_back(ServerError::AuthorizationError));
        let client = local_client(&server).await;
        let create = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).consumer_group(GROUP).create();
        assert_eq!(create.await.unwrap_err().kind(), ErrorKind::Authorization);
        assert_eq!(server.received(Type::SUBSCRIBE), 0);
        client.close().await.unwrap();
    });
}

/// A lookup of the topic the broker refuses fails the create, alone or in a group, with the broker's category, before
/// the topic's layout is asked for. Java's `V5AuthErrorTest.testLookupNotAuthorized`.
#[test]
fn a_refused_lookup_fails_the_create() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        server.with(|w| {
            w.lookup = LookupAnswer::Fail { error: ServerError::AuthorizationError, message: "not you".into() }
        });
        let client = local_client(&server).await;
        let alone = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).create();
        assert_eq!(alone.await.unwrap_err().kind(), ErrorKind::Authorization);
        let member = client.new_checkpoint_consumer(Schema::string()).topic(TOPIC).consumer_group(GROUP).create();
        assert_eq!(member.await.unwrap_err().kind(), ErrorKind::Authorization);
        assert_eq!(server.received(Type::SCALABLE_TOPIC_LOOKUP), 0);
        assert_eq!((server.received(Type::SCALABLE_TOPIC_SUBSCRIBE), server.received(Type::SUBSCRIBE)), (0, 0));
        client.close().await.unwrap();
    });
}

/// A member that loses its connection registers again, and reads on where it stopped.
#[test]
fn a_member_registers_again_after_a_lost_connection() {
    run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let consumer = member(&client, "member-0").await;
        publish(&server, 0, "a");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "a");
        server.with(|w| {
            for conn in w.open_connections() {
                w.kill(conn);
            }
        });
        wait_until("it registers again", || server.received(Type::SCALABLE_TOPIC_SUBSCRIBE) == 2).await;
        publish(&server, 0, "b");
        assert_eq!(consumer.receive().await.unwrap().value().unwrap(), "b");
        client.close().await.unwrap();
    });
}

/// Thread-safe Checkpoint consumers, and their futures, can move across threads.
#[allow(dead_code)]
fn thread_safe_types_are_send(consumer: &crate::CheckpointConsumer<String>) {
    fn send_sync<T: Send + Sync>(_: &T) {}
    fn send<T: Send>(_: T) {}
    send_sync(consumer);
    send(consumer.receive());
    send(consumer.receive_batch(1, Duration::ZERO));
    send(consumer.checkpoint());
    send(consumer.close());
    send(consumer.stream());
}
