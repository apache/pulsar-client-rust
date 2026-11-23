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

//! The connection state machine against the simulated broker, on a hand-moved clock.

use std::time::Duration;

use apache_pulsar_protocol::connection::{
    AUTH_REFRESH_SENTINEL, ConnectOptions, ConnectionError, Event, IdKind, MAX_AUTH_CHALLENGE_ROUNDS, RequestError,
    State,
};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{
    AuthData, BaseCommand, CommandAuthChallenge, CommandScalableTopicLookup, FeatureFlags, ServerError,
};
use apache_pulsar_testkit::{Link, SimBroker, reply};
use buffa::EnumValue;

fn connected() -> Link {
    let mut link = Link::new(ConnectOptions::default(), SimBroker::new());
    let events = link.pump();
    assert!(matches!(events.as_slice(), [Event::Connected(_)]), "{events:?}");
    link
}

fn closed_with(events: &[Event]) -> Option<&ConnectionError> {
    events.iter().find_map(|e| match e {
        Event::Closed(error) => Some(error),
        _ => None,
    })
}

#[test]
fn connect_advertises_the_features_the_client_honors() {
    let link = connected();
    let connect = link.broker.received_of(Type::CONNECT)[0].connect.as_option().unwrap().clone();
    assert_eq!(connect.protocol_version, Some(21));
    let flags = connect.feature_flags.as_option().unwrap();
    assert_eq!(flags.supports_auth_refresh, Some(true));
    assert_eq!(flags.supports_broker_entry_metadata, Some(true));
    assert_eq!(flags.supports_partial_producer, None);
    assert_eq!(link.client.state(), State::Ready);
    assert!(link.client.broker().unwrap().features.scalable_topics);
}

#[test]
fn handshake_times_out() {
    let mut broker = SimBroker::new();
    broker.mute = true;
    let mut link = Link::new(ConnectOptions::default(), broker);
    assert!(link.pump().is_empty());
    assert!(link.advance(Duration::from_secs(9)).is_empty());
    let events = link.advance(Duration::from_secs(1));
    assert_eq!(closed_with(&events), Some(&ConnectionError::HandshakeTimeout));
}

#[test]
fn refused_connection_is_not_retryable_for_auth_errors() {
    let mut broker = SimBroker::new();
    broker.refuse = Some(ServerError::AuthenticationError);
    let mut link = Link::new(ConnectOptions::default(), broker);
    let events = link.pump();
    let error = closed_with(&events).unwrap();
    assert!(matches!(error, ConnectionError::Refused { .. }));
    assert!(!error.is_retryable());
}

#[test]
fn requests_complete_by_id_in_any_order() {
    let mut link = connected();
    let now = link.clock.now();
    let a = link.client.request(now, ping_like).unwrap();
    let b = link.client.request(now, ping_like).unwrap();
    assert_ne!(a, b);
    assert_eq!(link.client.pending_requests(), 2);

    let events = link.push(&reply::error(b, ServerError::TopicNotFound, "no such topic"));
    assert!(matches!(
        events.as_slice(),
        [Event::Response { request_id, result: Err(RequestError::Server { error: EnumValue::Known(ServerError::TopicNotFound), .. }) }]
            if *request_id == b
    ));
    let events = link.push(&reply::success(a));
    assert!(matches!(events.as_slice(), [Event::Response { request_id, result: Ok(_) }] if *request_id == a));
    assert_eq!(link.client.pending_requests(), 0);
    // A second answer to the same id is dropped.
    assert!(link.push(&reply::success(a)).is_empty());
}

#[test]
fn requests_time_out_and_late_responses_are_dropped() {
    let mut link = connected();
    let now = link.clock.now();
    let id = link.client.request(now, ping_like).unwrap();
    assert!(link.advance(Duration::from_secs(29)).iter().all(|e| !matches!(e, Event::Response { .. })));
    let events = link.advance(Duration::from_secs(1));
    assert!(
        events.iter().any(
            |e| matches!(e, Event::Response { request_id, result: Err(RequestError::Timeout) } if *request_id == id)
        )
    );
    assert!(link.push(&reply::success(id)).is_empty());
}

#[test]
fn keepalive_pings_and_closes_when_the_pong_is_missing() {
    let mut link = connected();
    link.advance(Duration::from_secs(30));
    assert_eq!(link.broker.received_of(Type::PING).len(), 1, "a ping after the interval");
    // The pong came back, so the next tick pings again instead of closing.
    let events = link.advance(Duration::from_secs(30));
    assert!(closed_with(&events).is_none());
    assert_eq!(link.broker.received_of(Type::PING).len(), 2);

    // Now the broker goes quiet.
    link.broker.mute = true;
    link.advance(Duration::from_secs(30));
    let events = link.advance(Duration::from_secs(30));
    assert_eq!(closed_with(&events), Some(&ConnectionError::KeepaliveTimeout));
}

#[test]
fn any_frame_counts_as_a_keepalive_answer() {
    let mut link = connected();
    link.broker.mute = true;
    link.advance(Duration::from_secs(30));
    link.push(&reply::ping());
    let events = link.advance(Duration::from_secs(30));
    assert!(closed_with(&events).is_none(), "{events:?}");
}

#[test]
fn broker_ping_gets_a_pong() {
    let mut link = connected();
    link.push(&reply::ping());
    assert_eq!(link.broker.received_of(Type::PONG).len(), 1);
}

#[test]
fn auth_refresh_and_challenge_rounds() {
    let mut link = connected();
    let events = link.push(&reply::auth_challenge(AUTH_REFRESH_SENTINEL));
    assert!(matches!(events.as_slice(), [Event::AuthChallenge { refresh: true, .. }]));
    // A non-refresh challenge while a round is in progress is dropped.
    assert!(link.push(&reply::auth_challenge(b"more")).is_empty());
    link.client.auth_response(b"new-token".to_vec());
    link.pump();
    let response = link.broker.received_of(Type::AUTH_RESPONSE)[0].authResponse.as_option().unwrap().clone();
    assert_eq!(response.response.as_option().unwrap().auth_data.as_deref(), Some(&b"new-token"[..]));

    // A broker that never stops challenging fails the connection.
    for round in 1..=MAX_AUTH_CHALLENGE_ROUNDS {
        let events = link.push(&reply::auth_challenge(b"again"));
        assert!(matches!(events.as_slice(), [Event::AuthChallenge { refresh: false, .. }]), "round {round}");
        link.client.auth_response(b"x".to_vec());
    }
    let events = link.push(&reply::auth_challenge(b"again"));
    assert_eq!(closed_with(&events), Some(&ConnectionError::TooManyAuthRounds));
}

/// Ten challenge rounds, a refresh, then ten more: the refresh starts the count over, so a connection the broker
/// refreshes now and then is never failed by the cap. Java's
/// `ClientCnxAsyncAuthTest.refreshResetsChallengeRoundCounter`.
#[test]
fn a_refresh_starts_the_challenge_rounds_over() {
    let mut link = connected();
    let answer_rounds = |link: &mut Link| {
        for round in 1..=MAX_AUTH_CHALLENGE_ROUNDS {
            let events = link.push(&reply::auth_challenge(b"again"));
            assert!(
                matches!(events.as_slice(), [Event::AuthChallenge { refresh: false, .. }]),
                "round {round}: {events:?}"
            );
            link.client.auth_response(b"answer".to_vec());
        }
    };
    answer_rounds(&mut link);
    let events = link.push(&reply::auth_challenge(AUTH_REFRESH_SENTINEL));
    assert!(matches!(events.as_slice(), [Event::AuthChallenge { refresh: true, .. }]), "{events:?}");
    link.client.auth_response(b"new-token".to_vec());
    answer_rounds(&mut link);
    assert_eq!(link.client.state(), State::Ready);
}

/// Only `PulsarAuthRefresh` asks for fresh credentials. `PulsarAuthInit`, an empty payload, an absent one and any
/// other are challenges, handed to the driver as they came. Java's
/// `BinaryAuthenticationExchangeTest.isRefreshSentinelMatchesOnlyTheRefreshPayload`.
#[test]
fn only_the_refresh_sentinel_asks_for_fresh_credentials() {
    let classify = |command: &BaseCommand| {
        let mut link = connected();
        match link.push(command).as_slice() {
            [Event::AuthChallenge { refresh, challenge }] => (*refresh, challenge.to_vec()),
            events => panic!("one challenge expected: {events:?}"),
        }
    };
    let challenge = |challenge: CommandAuthChallenge| BaseCommand {
        r#type: Type::AUTH_CHALLENGE.into(),
        authChallenge: challenge.into(),
        ..Default::default()
    };
    assert_eq!(classify(&reply::auth_challenge(AUTH_REFRESH_SENTINEL)), (true, AUTH_REFRESH_SENTINEL.to_vec()));
    // Java's `AuthData.INIT_AUTH_DATA`.
    assert_eq!(classify(&reply::auth_challenge(b"PulsarAuthInit")), (false, b"PulsarAuthInit".to_vec()));
    assert_eq!(classify(&reply::auth_challenge(b"")), (false, vec![]));
    assert_eq!(classify(&reply::auth_challenge(b"challenge")), (false, b"challenge".to_vec()));
    // Auth data without its bytes, and no auth data at all.
    let without_bytes = CommandAuthChallenge { challenge: AuthData::default().into(), ..Default::default() };
    assert_eq!(classify(&challenge(without_bytes)), (false, vec![]));
    assert_eq!(classify(&challenge(CommandAuthChallenge::default())), (false, vec![]));
}

#[test]
fn scalable_commands_need_the_broker_feature() {
    let mut link = Link::new(ConnectOptions::default(), SimBroker::with_features(FeatureFlags::default()));
    link.pump();
    let now = link.clock.now();
    let session_id = link.client.allocate_id(IdKind::Session);
    let error = link.client.send(&scalable_lookup(session_id)).unwrap_err();
    assert_eq!(error, ConnectionError::UnsupportedBroker("scalable topics"));
    assert!(!error.is_retryable());
    // Ordinary requests still work.
    assert!(link.client.request(now, ping_like).is_ok());

    let mut link = connected();
    assert!(link.client.send(&scalable_lookup(0)).is_ok());
}

#[test]
fn ids_are_allocated_per_kind() {
    let mut link = connected();
    assert_eq!(link.client.allocate_id(IdKind::Producer), 0);
    assert_eq!(link.client.allocate_id(IdKind::Producer), 1);
    assert_eq!(link.client.allocate_id(IdKind::Consumer), 0);
    assert_eq!(link.client.allocate_id(IdKind::Session), 0);
}

#[test]
fn close_fails_later_requests() {
    let mut link = connected();
    let now = link.clock.now();
    link.client.request(now, ping_like).unwrap();
    link.client.close(ConnectionError::Closed);
    assert_eq!(link.client.pending_requests(), 0);
    assert_eq!(link.client.request(now, ping_like).unwrap_err(), ConnectionError::Closed);
    assert_eq!(closed_with(&link.pump()), Some(&ConnectionError::Closed));
}

#[test]
fn pushed_commands_come_out_as_commands() {
    let mut link = connected();
    let push = BaseCommand {
        r#type: Type::SCALABLE_TOPIC_UPDATE.into(),
        scalableTopicUpdate: apache_pulsar_protocol::proto::CommandScalableTopicUpdate {
            session_id: 3,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let events = link.push(&push);
    assert!(matches!(events.as_slice(), [Event::Command(_)]));
}

#[test]
fn auth_data_is_not_printed() {
    let options =
        ConnectOptions { auth_method: Some("token".into()), auth_data: Some(b"secret".to_vec()), ..Default::default() };
    let printed = format!("{options:?}");
    assert!(!printed.contains("secret") && !printed.contains("115, 101"), "{printed}");
}

/// A request the simulated broker leaves unanswered, so the test answers it: an unsubscribe.
fn ping_like(request_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::UNSUBSCRIBE.into(),
        unsubscribe: apache_pulsar_protocol::proto::CommandUnsubscribe {
            consumer_id: 0,
            request_id,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

fn scalable_lookup(session_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
        scalableTopicLookup: CommandScalableTopicLookup {
            session_id,
            topic: "topic://t/n/x".into(),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    }
}

#[test]
fn the_handshake_rejects_anything_but_connected() {
    let mut broker = SimBroker::new();
    broker.mute = true;
    let mut link = Link::new(ConnectOptions::default(), broker);
    link.pump();
    let now = link.clock.now();
    assert_eq!(link.client.request(now, ping_like).unwrap_err(), ConnectionError::NotReady);
    // Time short of the handshake deadline changes nothing.
    link.client.handle_timeout(now + Duration::from_secs(5));
    assert_eq!(link.client.state(), State::AwaitConnected);

    let events = link.push(&reply::success(0));
    assert_eq!(closed_with(&events), Some(&ConnectionError::Unexpected(EnumValue::Known(Type::SUCCESS))));
}

#[test]
fn a_closed_connection_ignores_everything() {
    let mut link = connected();
    let now = link.clock.now();
    link.client.close(ConnectionError::Closed);
    link.client.close(ConnectionError::KeepaliveTimeout);
    link.client.auth_response(b"late".to_vec());
    link.client.handle_input(&apache_pulsar_testkit::frame(&reply::ping()), now);
    link.client.handle_timeout(now + Duration::from_secs(3600));
    assert_eq!(link.client.poll_timeout(), None);
    assert_eq!(link.client.poll_transmit(), None);
    assert_eq!(link.pump(), vec![Event::Closed(ConnectionError::Closed)]);
}

#[test]
fn a_provider_that_fails_closes_the_connection() {
    let mut link = connected();
    link.push(&reply::auth_challenge(AUTH_REFRESH_SENTINEL));
    link.client.auth_failed("token expired".into());
    let events = link.pump();
    let error = closed_with(&events).unwrap();
    assert_eq!(error, &ConnectionError::Authentication("token expired".into()));
    assert!(!error.is_retryable());
}

#[test]
fn a_keepalive_tick_leaves_later_request_deadlines() {
    let options = ConnectOptions { operation_timeout: Duration::from_secs(60), ..Default::default() };
    let mut link = Link::new(options, SimBroker::new());
    link.pump();
    let id = link.client.request(link.clock.now(), ping_like).unwrap();
    assert!(link.advance(Duration::from_secs(30)).is_empty());
    assert_eq!(link.broker.received_of(Type::PING).len(), 1);
    assert_eq!(link.client.pending_requests(), 1);
    let events = link.advance(Duration::from_secs(30));
    assert!(
        events.iter().any(
            |e| matches!(e, Event::Response { request_id, result: Err(RequestError::Timeout) } if *request_id == id)
        )
    );
}

#[test]
fn transaction_coordinator_watches_need_the_broker_feature() {
    let watch = BaseCommand {
        r#type: Type::WATCH_TC_ASSIGNMENTS.into(),
        watchTcAssignments: apache_pulsar_protocol::proto::CommandWatchTcAssignments {
            watch_id: 0,
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    let mut link = connected();
    assert_eq!(
        link.client.send(&watch).unwrap_err(),
        ConnectionError::UnsupportedBroker("transaction coordinator discovery")
    );

    let features = FeatureFlags { supports_tc_metadata_discovery: Some(true), ..Default::default() };
    let mut link = Link::new(ConnectOptions::default(), SimBroker::with_features(features));
    link.pump();
    assert!(link.client.send(&watch).is_ok());
}

#[test]
fn which_failures_are_worth_reconnecting_for() {
    let refused = |error| ConnectionError::Refused { error: EnumValue::Known(error), message: String::new() };
    assert!(refused(ServerError::ServiceNotReady).is_retryable());
    assert!(!refused(ServerError::AuthorizationError).is_retryable());
    assert!(!refused(ServerError::NotAllowedError).is_retryable());
    assert!(ConnectionError::KeepaliveTimeout.is_retryable());
    assert!(ConnectionError::Disconnected("reset".into()).is_retryable());
    assert!(!ConnectionError::TooManyAuthRounds.is_retryable());
}

#[test]
fn the_simulated_broker_answers_other_commands_through_its_responder() {
    let broker = SimBroker {
        on_command: Some(Box::new(|command| {
            let request_id = command.unsubscribe.as_option().map(|u| u.request_id).unwrap();
            vec![reply::success(request_id)]
        })),
        ..Default::default()
    };
    let mut link = Link::new(ConnectOptions::default(), broker);
    link.pump();
    let id = link.client.request(link.clock.now(), ping_like).unwrap();
    let events = link.pump();
    assert!(matches!(events.as_slice(), [Event::Response { request_id, result: Ok(_) }] if *request_id == id));
    assert!(format!("{:?}", link.broker).contains("received: 2"));
}
