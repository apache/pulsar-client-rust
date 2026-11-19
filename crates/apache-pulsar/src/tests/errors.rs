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
        (IncompatibleSchema, ErrorKind::IncompatibleSchema),
        (TopicTerminatedError, ErrorKind::TopicTerminated),
        (TransactionConflict, ErrorKind::TransactionConflict),
        (ServiceNotReady, ErrorKind::NotConnected),
        (TooManyRequests, ErrorKind::NotConnected),
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
