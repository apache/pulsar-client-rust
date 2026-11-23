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

//! The kind each protocol failure becomes, and how errors print.

use std::error::Error as _;

use apache_pulsar_protocol::connection::{ConnectionError, RequestError};
use apache_pulsar_protocol::frame::FrameError;
use apache_pulsar_protocol::lookup::{LookupError, ServiceUrl};
use apache_pulsar_protocol::proto::ServerError;
use buffa::EnumValue;

use crate::error::{Error, ErrorKind, server_error_kind};

#[test]
fn broker_error_codes_map_to_kinds() {
    use ServerError::*;
    let cases = [
        (TopicNotFound, ErrorKind::NotFound),
        (SubscriptionNotFound, ErrorKind::NotFound),
        (ConsumerNotFound, ErrorKind::NotFound),
        (AuthenticationError, ErrorKind::Authentication),
        (AuthorizationError, ErrorKind::Authorization),
        (ProducerBusy, ErrorKind::ProducerBusy),
        (ProducerFenced, ErrorKind::ProducerFenced),
        (ConsumerBusy, ErrorKind::ConsumerBusy),
        // Key_Shared ranges that overlap another consumer's, as Java V5 maps it.
        (ConsumerAssignError, ErrorKind::ConsumerBusy),
        (IncompatibleSchema, ErrorKind::IncompatibleSchema),
        (TopicTerminatedError, ErrorKind::TopicTerminated),
        (TransactionConflict, ErrorKind::TransactionConflict),
        (InvalidTxnStatus, ErrorKind::TransactionConflict),
        (TransactionNotFound, ErrorKind::TransactionConflict),
        (ServiceNotReady, ErrorKind::NotConnected),
        (TooManyRequests, ErrorKind::NotConnected),
        (InvalidTopicName, ErrorKind::InvalidConfiguration),
        (PersistenceError, ErrorKind::Other),
    ];
    for (code, kind) in cases {
        assert_eq!(server_error_kind(EnumValue::Known(code)), kind, "{code:?}");
    }
    assert_eq!(server_error_kind(EnumValue::Unknown(9999)), ErrorKind::Other);
}

#[test]
fn connection_failures_map_to_kinds() {
    let kind = |error: ConnectionError| Error::from(error).kind();
    let refused = |code| ConnectionError::Refused { error: EnumValue::Known(code), message: "refused".into() };
    assert_eq!(kind(refused(ServerError::AuthorizationError)), ErrorKind::Authorization);
    // A refusal with no more precise kind is a failure to connect.
    assert_eq!(kind(refused(ServerError::UnknownError)), ErrorKind::Connect);
    assert_eq!(kind(ConnectionError::UnsupportedBroker("scalable topics")), ErrorKind::UnsupportedBroker);
    assert_eq!(kind(ConnectionError::TooManyAuthRounds), ErrorKind::Authentication);
    assert_eq!(kind(ConnectionError::Authentication("expired".into())), ErrorKind::Authentication);
    assert_eq!(kind(ConnectionError::HandshakeTimeout), ErrorKind::Timeout);
    assert_eq!(kind(ConnectionError::NotReady), ErrorKind::NotConnected);
    assert_eq!(kind(ConnectionError::KeepaliveTimeout), ErrorKind::Connect);
    assert_eq!(kind(ConnectionError::Frame(FrameError::Malformed("bad"))), ErrorKind::Connect);
}

#[test]
fn request_lookup_and_url_failures_map_to_kinds() {
    let server = RequestError::Server { error: EnumValue::Known(ServerError::TopicNotFound), message: "gone".into() };
    assert_eq!(Error::from(server).kind(), ErrorKind::NotFound);
    assert_eq!(Error::from(RequestError::Timeout).kind(), ErrorKind::Timeout);

    let lookup = LookupError::Server { error: EnumValue::Known(ServerError::AuthorizationError), message: "no".into() };
    assert_eq!(Error::from(lookup).kind(), ErrorKind::Authorization);
    assert_eq!(Error::from(LookupError::TooManyRedirects).kind(), ErrorKind::Other);

    let url = ServiceUrl::parse("http://broker:8080").unwrap_err();
    assert_eq!(Error::from(url).kind(), ErrorKind::InvalidServiceUrl);
}

#[test]
fn errors_print_their_kind_and_message_and_keep_their_source() {
    let error = Error::from(ConnectionError::HandshakeTimeout);
    assert_eq!(error.to_string(), "Timeout: no CONNECTED within the handshake timeout");
    assert_eq!(
        format!("{error:?}"),
        r#"Error { kind: Timeout, message: "no CONNECTED within the handshake timeout" }"#
    );
    assert_eq!(error.source().unwrap().to_string(), "no CONNECTED within the handshake timeout");
    assert!(Error::new(ErrorKind::Other, "plain").source().is_none());
}

/// The broker's refusal in an error, which a producer's registration reports as such; anything else leaves the
/// producer without a broker to ask, for now.
#[test]
fn the_brokers_refusal_is_found_in_an_error() {
    use apache_pulsar_protocol::producer::RegisterError;

    let refusal = |error| RequestError::Server { error, message: "no".into() };
    let refused = Error::from(refusal(EnumValue::Known(ServerError::ProducerBusy)));
    assert_eq!(refused.server_error(), Some((ServerError::ProducerBusy, "no".into())));
    let lookup = LookupError::Server { error: EnumValue::Known(ServerError::TopicNotFound), message: "gone".into() };
    assert_eq!(Error::from(lookup).server_error(), Some((ServerError::TopicNotFound, "gone".into())));
    // A code this client doesn't know, a failure that isn't the broker's, and an error with no source.
    assert_eq!(Error::from(refusal(EnumValue::Unknown(9999))).server_error(), None);
    assert_eq!(Error::from(RequestError::Timeout).server_error(), None);
    assert_eq!(Error::closed().server_error(), None);

    let registration = RegisterError::from(refused);
    assert!(matches!(registration, RegisterError::Server { error: ServerError::ProducerBusy, .. }), "{registration:?}");
    let registration = RegisterError::from(Error::closed());
    assert!(matches!(&registration, RegisterError::Unavailable(m) if m.contains("closed")), "{registration:?}");
}

/// A broker that refuses the handshake with credentials it rejects has refused the registration the connection was
/// for, which isn't retried; a refusal the client retries, as a lookup does, stays a failure to connect. Credentials
/// the provider couldn't supply count as rejected.
#[test]
fn a_refused_handshake_is_the_brokers_refusal_unless_it_is_retried() {
    use apache_pulsar_protocol::producer::RegisterError;

    let refused = |code| Error::from(ConnectionError::Refused { error: EnumValue::Known(code), message: "no".into() });
    assert_eq!(
        refused(ServerError::AuthenticationError).server_error(),
        Some((ServerError::AuthenticationError, "no".into()))
    );
    assert_eq!(
        refused(ServerError::AuthorizationError).server_error(),
        Some((ServerError::AuthorizationError, "no".into()))
    );
    assert_eq!(refused(ServerError::ServiceNotReady).server_error(), None);
    assert_eq!(refused(ServerError::UnknownError).server_error(), None);
    assert!(matches!(RegisterError::from(refused(ServerError::UnknownError)), RegisterError::Unavailable(_)));

    let rejected = RegisterError::from(Error::from(ConnectionError::Authentication("expired".into())));
    let expected = RegisterError::Server {
        error: ServerError::AuthenticationError,
        message: "authentication failed: expired".into(),
    };
    assert_eq!(rejected, expected);
}

/// A Shared producer registers on a segment with its first message there. A segment's broker that refuses the
/// credentials fails that send with the refusal, at once, not with a timeout once the retries run out.
#[test]
fn a_segment_broker_refusing_the_credentials_fails_the_send() {
    use crate::Schema;
    use crate::producer_tests::{TOPIC, WorldServer, local_client, world};
    use apache_pulsar_testkit::{LookupAnswer, SimBroker};

    super::run(async {
        let server = WorldServer::start(world(1)).await;
        let client = local_client(&server).await;
        let producer = client.new_producer(Schema::string()).topic(TOPIC).create().await.unwrap();
        let refusing = super::SimServer::start(None, |_| {
            let mut broker = SimBroker::new();
            broker.refuse = Some(ServerError::AuthenticationError);
            super::Behavior { broker, close_after: None, push_after_connect: vec![] }
        })
        .await;
        server.with(|world| world.lookup = LookupAnswer::Connect { broker: refusing.address.clone(), proxy: false });
        let error = producer.send("refused".into()).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
        client.close().await.unwrap();
    });
}
