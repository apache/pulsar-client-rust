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

//! The simulated cluster's own behavior, on raw frames: what the tests of the producer rely on.

use apache_pulsar_protocol::frame::{encode_command, encode_payload_command};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandConnect, CommandPing, CommandProducer, CommandSend, CommandUnsubscribe, MessageMetadata,
    ServerError,
};
use apache_pulsar_testkit::decode_all;
use apache_pulsar_testkit::scalable::World;
use buffa::EnumValue;
use bytes::BytesMut;

const TOPIC: &str = "topic://acme/orders/events";

fn write(world: &mut World, conn: usize, command: &BaseCommand) {
    let mut buf = BytesMut::new();
    encode_command(command, &mut buf).unwrap();
    world.input(conn, &buf);
}

fn connected(world: &mut World) -> usize {
    let conn = world.connect();
    let connect = BaseCommand {
        r#type: Type::CONNECT.into(),
        connect: CommandConnect { client_version: "raw".into(), protocol_version: Some(21), ..Default::default() }
            .into(),
        ..Default::default()
    };
    write(world, conn, &connect);
    assert_eq!(decode_all(&world.output(conn))[0].r#type, EnumValue::Known(Type::CONNECTED));
    conn
}

fn register(world: &mut World, conn: usize, topic: &str, producer_id: u64) -> BaseCommand {
    let producer = BaseCommand {
        r#type: Type::PRODUCER.into(),
        producer: CommandProducer { topic: topic.into(), producer_id, request_id: producer_id, ..Default::default() }
            .into(),
        ..Default::default()
    };
    write(world, conn, &producer);
    decode_all(&world.output(conn)).remove(0)
}

#[test]
fn answers_pings_and_ignores_what_it_doesnt_simulate() {
    let mut world = World::default();
    let conn = connected(&mut world);
    write(
        &mut world,
        conn,
        &BaseCommand { r#type: Type::PING.into(), ping: CommandPing::default().into(), ..Default::default() },
    );
    assert_eq!(decode_all(&world.output(conn))[0].r#type, EnumValue::Known(Type::PONG));
    let unsubscribe = BaseCommand {
        r#type: Type::UNSUBSCRIBE.into(),
        unsubscribe: CommandUnsubscribe::default().into(),
        ..Default::default()
    };
    write(&mut world, conn, &unsubscribe);
    assert!(world.output(conn).is_empty());
    assert_eq!(world.received(Type::UNSUBSCRIBE).len(), 1);
}

#[test]
fn a_producer_on_an_unknown_segment_is_not_found() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let answer = register(&mut world, conn, "segment://acme/orders/events/0000-ffff-9", 1);
    assert_eq!(answer.error.error, ServerError::TopicNotFound);
}

#[test]
fn a_send_for_an_unknown_producer_closes_the_connection() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 42, sequence_id: 0, ..Default::default() }.into(),
        ..Default::default()
    };
    let mut buf = BytesMut::new();
    encode_payload_command(&send, &MessageMetadata::default(), b"x", &mut buf).unwrap();
    world.input(conn, &buf);
    assert!(!world.is_open(conn));
    assert!(world.open_connections().is_empty());
}

/// An entry that isn't a batch, as other clients send a lone message: its key and value are the entry's own.
#[test]
fn stores_plain_entries_too() {
    let mut world = World::default();
    world.create_topic(TOPIC, 1);
    let conn = connected(&mut world);
    let success = register(&mut world, conn, "segment://acme/orders/events/0000-ffff-0", 1);
    assert_eq!(success.producer_success.producer_name, "sim-1");
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 1, sequence_id: 0, ..Default::default() }.into(),
        ..Default::default()
    };
    let metadata =
        MessageMetadata { producer_name: "sim-1".into(), partition_key: Some("k".into()), ..Default::default() };
    let mut buf = BytesMut::new();
    encode_payload_command(&send, &metadata, b"plain", &mut buf).unwrap();
    world.input(conn, &buf);
    let stored = world.stored_in(TOPIC, 0);
    assert_eq!((stored[0].key.as_deref(), &stored[0].value[..]), (Some("k"), &b"plain"[..]));
    assert!(stored[0].single.is_none());
}
