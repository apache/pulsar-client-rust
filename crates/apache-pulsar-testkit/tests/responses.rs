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

//! Which commands answer a request, and which are pushed: every response type completes the request it names.

use apache_pulsar_protocol::connection::{ConnectOptions, Event};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{self, BaseCommand};
use apache_pulsar_testkit::{Link, SimBroker};
use buffa::EnumValue;

/// A response of `$kind`, in field `$field` of type `$message`, to request `id`.
macro_rules! response {
    ($kind:ident, $field:ident, $message:ident) => {
        (Type::$kind, |id| BaseCommand {
            r#type: Type::$kind.into(),
            $field: proto::$message { request_id: id, ..Default::default() }.into(),
            ..Default::default()
        })
    };
}

/// A response type, and how to build one for a request id.
type Response = (Type, fn(u64) -> BaseCommand);

const RESPONSES: &[Response] = &[
    response!(SUCCESS, success, CommandSuccess),
    response!(ERROR, error, CommandError),
    response!(PRODUCER_SUCCESS, producer_success, CommandProducerSuccess),
    response!(LOOKUP_RESPONSE, lookupTopicResponse, CommandLookupTopicResponse),
    response!(PARTITIONED_METADATA_RESPONSE, partitionMetadataResponse, CommandPartitionedTopicMetadataResponse),
    response!(CONSUMER_STATS_RESPONSE, consumerStatsResponse, CommandConsumerStatsResponse),
    response!(GET_LAST_MESSAGE_ID_RESPONSE, getLastMessageIdResponse, CommandGetLastMessageIdResponse),
    response!(GET_TOPICS_OF_NAMESPACE_RESPONSE, getTopicsOfNamespaceResponse, CommandGetTopicsOfNamespaceResponse),
    response!(GET_SCHEMA_RESPONSE, getSchemaResponse, CommandGetSchemaResponse),
    response!(GET_OR_CREATE_SCHEMA_RESPONSE, getOrCreateSchemaResponse, CommandGetOrCreateSchemaResponse),
    response!(NEW_TXN_RESPONSE, newTxnResponse, CommandNewTxnResponse),
    response!(ADD_PARTITION_TO_TXN_RESPONSE, addPartitionToTxnResponse, CommandAddPartitionToTxnResponse),
    response!(ADD_SUBSCRIPTION_TO_TXN_RESPONSE, addSubscriptionToTxnResponse, CommandAddSubscriptionToTxnResponse),
    response!(END_TXN_RESPONSE, endTxnResponse, CommandEndTxnResponse),
    response!(END_TXN_ON_PARTITION_RESPONSE, endTxnOnPartitionResponse, CommandEndTxnOnPartitionResponse),
    response!(END_TXN_ON_SUBSCRIPTION_RESPONSE, endTxnOnSubscriptionResponse, CommandEndTxnOnSubscriptionResponse),
    response!(TC_CLIENT_CONNECT_RESPONSE, tcClientConnectResponse, CommandTcClientConnectResponse),
    response!(WATCH_TOPIC_LIST_SUCCESS, watchTopicListSuccess, CommandWatchTopicListSuccess),
    response!(SCALABLE_TOPIC_SUBSCRIBE_RESPONSE, scalableTopicSubscribeResponse, CommandScalableTopicSubscribeResponse),
    (Type::ACK_RESPONSE, |id| BaseCommand {
        r#type: Type::ACK_RESPONSE.into(),
        ackResponse: proto::CommandAckResponse { request_id: Some(id), ..Default::default() }.into(),
        ..Default::default()
    }),
];

/// A request the simulated broker leaves unanswered, so the test answers it.
fn unsubscribe(request_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::UNSUBSCRIBE.into(),
        unsubscribe: proto::CommandUnsubscribe { consumer_id: 0, request_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

fn connected() -> Link {
    let mut link = Link::new(ConnectOptions::default(), SimBroker::new());
    link.pump();
    link
}

#[test]
fn every_response_type_completes_the_request_it_names() {
    let mut link = connected();
    for (kind, response) in RESPONSES {
        let id = link.client.request(link.clock.now(), unsubscribe).unwrap();
        let events = link.push(&response(id));
        assert!(
            matches!(events.as_slice(), [Event::Response { request_id, .. }] if *request_id == id),
            "{kind:?}: {events:?}"
        );
    }
    assert_eq!(link.client.pending_requests(), 0);
}

#[test]
fn commands_without_a_request_id_are_pushed() {
    let mut link = connected();
    // An ack response for an ack sent without a request id.
    let ack = BaseCommand {
        r#type: Type::ACK_RESPONSE.into(),
        ackResponse: proto::CommandAckResponse::default().into(),
        ..Default::default()
    };
    assert!(matches!(link.push(&ack).as_slice(), [Event::Command(_)]));

    // A command type newer than this client.
    let unknown = BaseCommand { r#type: EnumValue::Unknown(9999), ..Default::default() };
    assert!(matches!(link.push(&unknown).as_slice(), [Event::Command(_)]));
}
