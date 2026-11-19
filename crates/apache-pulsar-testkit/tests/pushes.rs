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

//! Which owner each pushed command goes to (`push_target`), and the cases the connection keeps a request open.

use apache_pulsar_protocol::connection::{IdKind, push_target};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::command_topic_migrated::ResourceType;
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandActiveConsumerChange, CommandCloseConsumer, CommandCloseProducer, CommandMessage, CommandPing,
    CommandReachedEndOfTopic, CommandScalableTopicAssignmentUpdate, CommandScalableTopicUpdate, CommandSendError,
    CommandSendReceipt, CommandTopicMigrated, CommandWatchScalableTopicsUpdate, CommandWatchTcAssignmentsUpdate,
    CommandWatchTopicUpdate,
};
use buffa::{Message, MessageView};

fn target(command: BaseCommand) -> Option<(IdKind, u64)> {
    let bytes = command.encode_to_vec();
    let view = apache_pulsar_protocol::proto::__buffa::view::BaseCommandView::decode_view(&bytes).unwrap();
    push_target(&view)
}

fn command(kind: Type, fill: impl FnOnce(&mut BaseCommand)) -> BaseCommand {
    let mut command = BaseCommand { r#type: kind.into(), ..Default::default() };
    fill(&mut command);
    command
}

#[test]
fn pushed_commands_go_to_their_owner() {
    let cases = [
        (
            command(Type::SEND_RECEIPT, |c| {
                c.send_receipt = CommandSendReceipt { producer_id: 1, ..Default::default() }.into()
            }),
            (IdKind::Producer, 1),
        ),
        (
            command(Type::SEND_ERROR, |c| {
                c.send_error = CommandSendError { producer_id: 2, ..Default::default() }.into()
            }),
            (IdKind::Producer, 2),
        ),
        (
            command(Type::CLOSE_PRODUCER, |c| {
                c.close_producer = CommandCloseProducer { producer_id: 3, ..Default::default() }.into()
            }),
            (IdKind::Producer, 3),
        ),
        (
            command(Type::MESSAGE, |c| c.message = CommandMessage { consumer_id: 4, ..Default::default() }.into()),
            (IdKind::Consumer, 4),
        ),
        (
            command(Type::ACTIVE_CONSUMER_CHANGE, |c| {
                c.active_consumer_change = CommandActiveConsumerChange { consumer_id: 5, ..Default::default() }.into()
            }),
            (IdKind::Consumer, 5),
        ),
        (
            command(Type::REACHED_END_OF_TOPIC, |c| {
                c.reachedEndOfTopic = CommandReachedEndOfTopic { consumer_id: 6, ..Default::default() }.into()
            }),
            (IdKind::Consumer, 6),
        ),
        (
            command(Type::CLOSE_CONSUMER, |c| {
                c.close_consumer = CommandCloseConsumer { consumer_id: 7, ..Default::default() }.into()
            }),
            (IdKind::Consumer, 7),
        ),
        (
            command(Type::SCALABLE_TOPIC_ASSIGNMENT_UPDATE, |c| {
                c.scalableTopicAssignmentUpdate =
                    CommandScalableTopicAssignmentUpdate { consumer_id: 8, ..Default::default() }.into()
            }),
            (IdKind::Consumer, 8),
        ),
        (
            command(Type::SCALABLE_TOPIC_UPDATE, |c| {
                c.scalableTopicUpdate = CommandScalableTopicUpdate { session_id: 9, ..Default::default() }.into()
            }),
            (IdKind::Session, 9),
        ),
        (
            command(Type::WATCH_SCALABLE_TOPICS_UPDATE, |c| {
                c.watchScalableTopicsUpdate =
                    CommandWatchScalableTopicsUpdate { watch_id: 10, ..Default::default() }.into()
            }),
            (IdKind::Watch, 10),
        ),
        (
            command(Type::WATCH_TC_ASSIGNMENTS_UPDATE, |c| {
                c.watchTcAssignmentsUpdate =
                    CommandWatchTcAssignmentsUpdate { watch_id: 11, ..Default::default() }.into()
            }),
            (IdKind::Watch, 11),
        ),
        (
            command(Type::WATCH_TOPIC_UPDATE, |c| {
                c.watchTopicUpdate = CommandWatchTopicUpdate { watcher_id: 12, ..Default::default() }.into()
            }),
            (IdKind::Watch, 12),
        ),
        (
            command(Type::TOPIC_MIGRATED, |c| {
                c.topicMigrated = CommandTopicMigrated {
                    resource_id: 13,
                    resource_type: ResourceType::Producer,
                    ..Default::default()
                }
                .into()
            }),
            (IdKind::Producer, 13),
        ),
        (
            command(Type::TOPIC_MIGRATED, |c| {
                c.topicMigrated = CommandTopicMigrated {
                    resource_id: 14,
                    resource_type: ResourceType::Consumer,
                    ..Default::default()
                }
                .into()
            }),
            (IdKind::Consumer, 14),
        ),
    ];
    for (command, expected) in cases {
        let kind = command.r#type;
        assert_eq!(target(command), Some(expected), "{kind:?}");
    }
    // Not a push: nobody owns it.
    assert_eq!(target(command(Type::PING, |c| c.ping = CommandPing::default().into())), None);
}
