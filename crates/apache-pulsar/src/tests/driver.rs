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

//! The connection driver: writes the socket takes only in part, a read that fails, connect timeouts, requests
//! that fail before reaching the broker, authentication challenges, pushed commands and refusals.

use super::*;

#[test]
fn burst_larger_than_the_socket_buffers_goes_out_in_parts() {
    // The broker runs on the client's thread, so it reads nothing while the driver writes: a burst larger than
    // what the loopback socket buffers hold must be written in parts, with the rest parked until the broker
    // drains it.
    run(async {
        let (acceptor, ca_file) = tls_fixture();
        let server = SimServer::start(Some(acceptor), self_serving).await;
        let url = format!("pulsar+ssl://localhost:{}", server.address.port);
        let client =
            local::PulsarClient::builder().service_url(url).tls_trust_certs_file(ca_file.path()).build().await.unwrap();
        client.lookup(TOPIC).await.unwrap();
        // 256 lookups of 128 KiB topic names: 32 MiB, sent from one poll of the driver.
        let topic = "t".repeat(128 * 1024);
        let lookups: Vec<_> = (0..256).map(|_| client.lookup(&topic)).collect();
        let results = futures_util::future::join_all(lookups).await;
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(server.received(Type::LOOKUP).len(), 257);
        assert_eq!(server.connections.get(), 1);
        client.close().await.unwrap();
    });
}

#[test]
fn tls_connection_dropped_without_close_notify_is_replaced() {
    // A broker that drops the TCP connection without a TLS close_notify makes the read fail, rather than end.
    run(async {
        let (acceptor, ca_file) = tls_fixture();
        let server =
            SimServer::start(Some(acceptor), |address| Behavior { close_after: Some(2), ..self_serving(address) })
                .await;
        let url = format!("pulsar+ssl://localhost:{}", server.address.port);
        let client =
            local::PulsarClient::builder().service_url(url).tls_trust_certs_file(ca_file.path()).build().await.unwrap();
        for _ in 0..3 {
            client.lookup(TOPIC).await.unwrap();
        }
        assert_eq!(server.connections.get(), 3);
        client.close().await.unwrap();
    });
}

fn connect_to(server: &SimServer) -> BrokerAddress {
    BrokerAddress::direct(server.address.clone())
}

fn lookup_request(server: &SimServer) -> impl FnOnce(u64) -> BaseCommand + 'static {
    let lookup = apache_pulsar_protocol::lookup::Lookup::new(TOPIC, server.address.clone(), false);
    move |id| lookup.command(id)
}

fn behavior(broker: SimBroker) -> Behavior {
    Behavior { broker, close_after: None, push_after_connect: vec![] }
}

#[test]
fn requests_on_a_closed_connection_fail() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        assert!(format!("{connection:?}").contains("closed: false"));
        connection.close().await;
        connection.close().await;
        assert!(connection.is_closed());
        let error = connection.request(lookup_request(&server)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotConnected, "{error}");
        client.close().await.unwrap();
    });
}

#[test]
fn the_connect_timeout_covers_the_tls_handshake() {
    run(async {
        // TCP connects through the listener's backlog, but nothing ever answers the TLS handshake.
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let client = local::PulsarClient::builder()
            .service_url(format!("pulsar+ssl://127.0.0.1:{port}"))
            .allow_insecure_connection(true)
            .connect_timeout(Duration::from_millis(200))
            .build()
            .await
            .unwrap();
        let address = BrokerAddress::direct(HostPort { host: "127.0.0.1".into(), port });
        let error = client.connection(&address).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        client.close().await.unwrap();
    });
}

#[test]
fn the_connect_timeout_covers_the_authentication_provider() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .authentication(Token::from_fn(std::future::pending))
            .connect_timeout(Duration::from_millis(200))
            .build()
            .await
            .unwrap();
        let error = client.connection(&connect_to(&server)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        assert!(server.received(Type::CONNECT).is_empty());
        client.close().await.unwrap();
    });
}

#[test]
fn a_request_the_broker_does_not_support_fails_at_once() {
    run(async {
        let features = apache_pulsar_protocol::proto::FeatureFlags::default();
        let server = SimServer::start(None, move |_| behavior(SimBroker::with_features(features.clone()))).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        let error = connection
            .request(|id| BaseCommand {
                r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
                scalableTopicLookup: apache_pulsar_protocol::proto::CommandScalableTopicLookup {
                    session_id: id,
                    topic: "topic://t/n/x".into(),
                    ..Default::default()
                }
                .into(),
                ..Default::default()
            })
            .await
            .unwrap_err();
        assert_eq!(error.kind(), ErrorKind::UnsupportedBroker, "{error}");
        assert!(!connection.is_closed());
        client.close().await.unwrap();
    });
}

#[test]
fn a_request_past_its_deadline_before_the_timer_runs_times_out() {
    run(async {
        let server = SimServer::start(None, |address| {
            let mut behavior = self_serving(address);
            behavior.broker.default_lookup = LookupAnswer::Silent;
            behavior
        })
        .await;
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .operation_timeout(Duration::ZERO)
            .build()
            .await
            .unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        let error = connection.request(lookup_request(&server)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        client.close().await.unwrap();
    });
}

#[test]
fn challenges_are_answered_by_the_provider() {
    run(async {
        let challenge = reply::auth_challenge(b"next-round");
        let server = SimServer::start(None, move |address| Behavior {
            push_after_connect: vec![challenge.clone()],
            ..self_serving(address)
        })
        .await;
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .authentication(Token::new("token"))
            .build()
            .await
            .unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        wait_until("the challenge is answered", || !server.received(Type::AUTH_RESPONSE).is_empty()).await;
        let response = server.received(Type::AUTH_RESPONSE)[0].authResponse.as_option().unwrap().clone();
        assert_eq!(response.response.as_option().unwrap().auth_data.as_deref(), Some(&b"token"[..]));
        assert!(!connection.is_closed());
        client.close().await.unwrap();
    });
}

#[test]
fn a_provider_failing_a_refresh_closes_the_connection() {
    run(async {
        let refresh = reply::auth_challenge(AUTH_REFRESH_SENTINEL);
        let server = SimServer::start(None, move |address| Behavior {
            push_after_connect: vec![refresh.clone()],
            ..self_serving(address)
        })
        .await;
        let calls = Arc::new(AtomicUsize::new(0));
        let counter = calls.clone();
        let token = Token::from_fn(move || {
            let first = counter.fetch_add(1, Ordering::SeqCst) == 0;
            async move {
                if first { Ok("token".into()) } else { Err(crate::Error::new(ErrorKind::Authentication, "expired")) }
            }
        });
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .authentication(token)
            .build()
            .await
            .unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        wait_until("the connection closes", || connection.is_closed()).await;
        assert_eq!(calls.load(Ordering::SeqCst), 2);
        assert!(server.received(Type::AUTH_RESPONSE).is_empty());
        client.close().await.unwrap();
    });
}

#[test]
fn a_challenge_without_a_provider_closes_the_connection() {
    run(async {
        let challenge = reply::auth_challenge(b"credentials?");
        let server = SimServer::start(None, move |address| Behavior {
            push_after_connect: vec![challenge.clone()],
            ..self_serving(address)
        })
        .await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        wait_until("the connection closes", || connection.is_closed()).await;
        client.close().await.unwrap();
    });
}

#[test]
fn pushed_commands_leave_the_connection_open() {
    run(async {
        let close_producer = BaseCommand {
            r#type: Type::CLOSE_PRODUCER.into(),
            close_producer: apache_pulsar_protocol::proto::CommandCloseProducer {
                producer_id: 1,
                request_id: 0,
                ..Default::default()
            }
            .into(),
            ..Default::default()
        };
        let server = SimServer::start(None, move |address| Behavior {
            push_after_connect: vec![close_producer.clone()],
            ..self_serving(address)
        })
        .await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        // The lookup goes out after the pushed command came in, on the same connection.
        client.lookup(TOPIC).await.unwrap();
        assert_eq!(server.connections.get(), 1);
        client.close().await.unwrap();
    });
}

#[test]
fn a_refused_connection_says_why() {
    run(async {
        let server = SimServer::start(None, |address| {
            let mut behavior = self_serving(address);
            behavior.broker.refuse = Some(apache_pulsar_protocol::proto::ServerError::AuthorizationError);
            behavior
        })
        .await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let error = client.connection(&connect_to(&server)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authorization, "{error}");
        client.close().await.unwrap();
    });
}

#[test]
fn streams_print_whether_they_are_encrypted() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let plain = crate::net::connect(&server.address, None).await.unwrap();
        assert_eq!(format!("{plain:?}"), "Stream::Plain");

        let (acceptor, _) = tls_fixture();
        let server = SimServer::start(Some(acceptor), self_serving).await;
        let config = local::PulsarClient::builder()
            .service_url("pulsar+ssl://localhost:6651")
            .allow_insecure_connection(true)
            .into_config()
            .unwrap();
        let tls = crate::net::connect(&server.address, config.tls.as_ref()).await.unwrap();
        assert_eq!(format!("{tls:?}"), "Stream::Tls");
    });
}

#[test]
fn a_request_queued_behind_a_close_fails() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let connection = client.connection(&connect_to(&server)).await.unwrap();
        // The close is queued first: the driver stops before it takes the request.
        let ((), result) = futures_util::join!(connection.close(), connection.request(lookup_request(&server)));
        assert_eq!(result.unwrap_err().kind(), ErrorKind::NotConnected);
        client.close().await.unwrap();
    });
}

#[test]
fn connections_through_a_proxy_name_the_broker() {
    run(async {
        let proxy = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(proxy.service_url("pulsar")).build().await.unwrap();
        let broker = HostPort { host: "broker-1.internal".into(), port: 6650 };
        let address = BrokerAddress { logical: broker, physical: proxy.address.clone() };
        let connection = client.connection(&address).await.unwrap();
        assert_eq!(connection.address(), &address);
        let connect = proxy.received(Type::CONNECT)[0].connect.as_option().unwrap().clone();
        assert_eq!(connect.proxy_to_broker_url.as_deref(), Some("broker-1.internal:6650"));
        client.close().await.unwrap();
    });
}
