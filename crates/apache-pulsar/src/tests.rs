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

//! The driver, pool and clients over real loopback sockets, against the simulated broker.
//!
//! The protocol decisions are tested deterministically in `apache-pulsar-testkit`; these tests check that the
//! compio layer moves the bytes and the time correctly: concurrent reads and writes, plain and TLS, timeouts,
//! keepalive, reconnects, coalescing, authentication and the thread-safe client's loops.

use std::cell::{Cell, RefCell};
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicUsize, Ordering};
use std::time::Duration;

use apache_pulsar_protocol::connection::AUTH_REFRESH_SENTINEL;
use apache_pulsar_protocol::lookup::{BrokerAddress, HostPort};
use apache_pulsar_protocol::proto::BaseCommand;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_testkit::{LookupAnswer, SimBroker, frame, reply};
use buffa::EnumValue;
use bytes::BytesMut;
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWrite, AsyncWriteExt};
use compio::net::TcpListener;
use compio::runtime::JoinHandle;
use compio::tls::TlsAcceptor;

use crate::error::ErrorKind;
use crate::{PulsarClient, Token, local};

mod clients;
mod driver;
mod errors;
mod options;

/// How the server treats one accepted connection.
struct Behavior {
    broker: SimBroker,
    /// Close the socket after this many commands from the client.
    close_after: Option<usize>,
    /// Commands to push once the client is connected.
    push_after_connect: Vec<BaseCommand>,
}

/// Simulated brokers behind a loopback listener, one per accepted connection.
struct SimServer {
    address: HostPort,
    log: Rc<RefCell<Vec<BaseCommand>>>,
    connections: Rc<Cell<usize>>,
    _task: JoinHandle<()>,
}

impl SimServer {
    async fn start(tls: Option<TlsAcceptor>, behavior: impl Fn(&HostPort) -> Behavior + 'static) -> Self {
        let listener = TcpListener::bind("127.0.0.1:0").await.unwrap();
        let port = listener.local_addr().unwrap().port();
        let address = HostPort { host: "127.0.0.1".into(), port };
        let log = Rc::new(RefCell::new(Vec::new()));
        let connections = Rc::new(Cell::new(0));
        let task = {
            let (log, connections, address) = (log.clone(), connections.clone(), address.clone());
            compio::runtime::spawn(async move {
                let mut tasks = Vec::new();
                loop {
                    let Ok((tcp, _)) = listener.accept().await else { return };
                    connections.set(connections.get() + 1);
                    let behavior = behavior(&address);
                    let log = log.clone();
                    let tls = tls.clone();
                    tasks.push(compio::runtime::spawn(async move {
                        match tls {
                            None => serve(tcp, behavior, log).await,
                            Some(acceptor) => {
                                if let Ok(stream) = acceptor.accept(tcp).await {
                                    serve(stream, behavior, log).await
                                }
                            }
                        }
                    }));
                }
            })
        };
        Self { address, log, connections, _task: task }
    }

    fn service_url(&self, scheme: &str) -> String {
        format!("{scheme}://{}", self.address)
    }

    fn received(&self, kind: Type) -> Vec<BaseCommand> {
        self.log.borrow().iter().filter(|c| c.r#type == EnumValue::Known(kind)).cloned().collect()
    }

    /// What the client answered to the broker's challenges, in order.
    fn auth_responses(&self) -> Vec<String> {
        let answer = |command: &BaseCommand| {
            let data = command.authResponse.as_option().unwrap().response.as_option().unwrap().auth_data.clone();
            String::from_utf8(data.unwrap()).unwrap()
        };
        self.received(Type::AUTH_RESPONSE).iter().map(answer).collect()
    }
}

async fn serve<S: AsyncRead + AsyncWrite>(mut stream: S, mut behavior: Behavior, log: Rc<RefCell<Vec<BaseCommand>>>) {
    let mut pending = BytesMut::new();
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        buf.clear();
        let BufResult(result, returned) = stream.read(buf).await;
        buf = returned;
        match result {
            Ok(0) | Err(_) => return,
            Ok(_) => pending.extend_from_slice(&buf),
        }
        let seen = behavior.broker.received.len();
        let mut answer = behavior.broker.feed(&mut pending).to_vec();
        let new = behavior.broker.received[seen..].to_vec();
        if new.iter().any(|c| c.r#type == EnumValue::Known(Type::CONNECT)) {
            for push in behavior.push_after_connect.drain(..) {
                answer.extend_from_slice(&frame(&push));
            }
        }
        log.borrow_mut().extend(new);
        if !answer.is_empty() {
            let BufResult(result, _) = stream.write_all(answer).await;
            if result.is_err() || stream.flush().await.is_err() {
                return;
            }
        }
        if behavior.close_after.is_some_and(|n| behavior.broker.received.len() >= n) {
            return;
        }
    }
}

/// A broker that answers every lookup with itself.
fn self_serving(address: &HostPort) -> Behavior {
    let mut broker = SimBroker::new();
    broker.default_lookup = LookupAnswer::Connect { broker: address.clone(), proxy: false };
    Behavior { broker, close_after: None, push_after_connect: vec![] }
}

/// A self-serving broker that challenges the client: with the first of `challenges` once it is connected, and with
/// each next one once it answered the one before.
fn challenging(address: &HostPort, challenges: Vec<BaseCommand>) -> Behavior {
    let mut challenges = challenges.into_iter();
    let mut behavior =
        Behavior { push_after_connect: challenges.next().into_iter().collect(), ..self_serving(address) };
    // The client's answers are the only commands the simulated broker leaves to its responder.
    behavior.broker.on_command = Some(Box::new(move |_| challenges.next().into_iter().collect()));
    behavior
}

/// Waits for `condition`, polling, for up to 10 s: CI machines stall, so tests wait for states, not for times.
async fn wait_until(what: &str, condition: impl Fn() -> bool) {
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !condition() {
        assert!(std::time::Instant::now() < deadline, "timed out waiting until {what}");
        compio::time::sleep(Duration::from_millis(10)).await;
    }
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

const TOPIC: &str = "segment://public/default/t/0000-ffff-0";

#[test]
fn lookup_over_the_pool() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let found = client.lookup(TOPIC).await.unwrap();
        assert_eq!(found, BrokerAddress::direct(server.address.clone()));
        // The lookup and the found broker are the same address: one connection serves both.
        let again = client.lookup(TOPIC).await.unwrap();
        assert_eq!(again, found);
        assert_eq!(server.connections.get(), 1);
        assert_eq!(server.received(Type::LOOKUP).len(), 2);
        client.close().await.unwrap();
        client.close().await.unwrap();
        assert_eq!(client.lookup(TOPIC).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
    });
}

#[test]
fn concurrent_requests_for_an_address_share_one_connection() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let address = BrokerAddress::direct(server.address.clone());
        let gets: Vec<_> = (0..8).map(|_| client.connection(&address)).collect();
        let connections = futures_util::future::join_all(gets).await;
        assert!(connections.iter().all(|c| c.is_ok()));
        assert_eq!(client.pool().connect_attempts(), 1);
        assert_eq!(server.connections.get(), 1);
        client.close().await.unwrap();
    });
}

#[test]
fn many_requests_in_flight_on_one_connection() {
    // Writes keep going while the read side is parked waiting for answers.
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let lookups: Vec<_> = (0..200).map(|_| client.lookup(TOPIC)).collect();
        let results = futures_util::future::join_all(lookups).await;
        assert!(results.iter().all(|r| r.is_ok()));
        assert_eq!(server.received(Type::LOOKUP).len(), 200);
        client.close().await.unwrap();
    });
}

#[test]
fn reconnects_after_the_broker_drops_the_connection() {
    run(async {
        // Each connection serves CONNECT and one lookup, then closes.
        let server = SimServer::start(None, |address| Behavior { close_after: Some(2), ..self_serving(address) }).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        for _ in 0..3 {
            // A lookup that finds the closed connection still in the pool fails on it, and retries on a new one.
            client.lookup(TOPIC).await.unwrap();
        }
        assert_eq!(server.connections.get(), 3);
        client.close().await.unwrap();
    });
}

#[test]
fn unanswered_request_times_out() {
    run(async {
        let server = SimServer::start(None, |address| {
            let mut behavior = self_serving(address);
            behavior.broker.default_lookup = LookupAnswer::Silent;
            behavior
        })
        .await;
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .operation_timeout(Duration::from_millis(300))
            .build()
            .await
            .unwrap();
        let started = std::time::Instant::now();
        let error = client.lookup(TOPIC).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Timeout, "{error}");
        assert!(started.elapsed() < Duration::from_secs(2));
        client.close().await.unwrap();
    });
}

#[test]
fn keepalive_closes_a_silent_connection() {
    run(async {
        let server = SimServer::start(None, |address| {
            let mut behavior = self_serving(address);
            behavior.broker.ignore_pings = true;
            behavior
        })
        .await;
        let quiet = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .keepalive_interval(Duration::from_millis(100))
            .build()
            .await
            .unwrap();
        let address = BrokerAddress::direct(server.address.clone());
        let connection = quiet.connection(&address).await.unwrap();
        assert!(connection.broker().features.scalable_topics);
        // A ping at 100 ms goes unanswered; the tick at 200 ms closes the connection.
        wait_until("the connection closes", || connection.is_closed()).await;
        assert_eq!(server.received(Type::PING).len(), 1);
        // The pool replaces it.
        let replaced = quiet.connection(&address).await.unwrap();
        assert!(!replaced.is_closed());
        assert_eq!(server.connections.get(), 2);
        quiet.close().await.unwrap();
    });
}

#[test]
fn keepalive_keeps_an_answering_connection() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .keepalive_interval(Duration::from_millis(100))
            .build()
            .await
            .unwrap();
        let address = BrokerAddress::direct(server.address.clone());
        let connection = client.connection(&address).await.unwrap();
        wait_until("three pings", || server.received(Type::PING).len() >= 3).await;
        assert!(!connection.is_closed());
        assert_eq!(connection.address(), &address);
        client.close().await.unwrap();
    });
}

/// A token fetched for `CONNECT`, and again for each refresh the broker asks for: each refresh is answered in band
/// with the next token, over the one connection, which stays open. Java's
/// `ClientCnxAsyncAuthTest.refreshRepublishesCredentialWithoutDisconnect`.
#[test]
fn token_authentication_and_refresh() {
    run(async {
        let refresh = reply::auth_challenge(AUTH_REFRESH_SENTINEL);
        let server =
            SimServer::start(None, move |address| challenging(address, vec![refresh.clone(), refresh.clone()])).await;
        let fetched = Arc::new(AtomicUsize::new(0));
        let counter = fetched.clone();
        let token = Token::from_fn(move || {
            let n = counter.fetch_add(1, Ordering::SeqCst);
            async move { Ok(format!("token-{n}")) }
        });
        let client = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .authentication(token)
            .build()
            .await
            .unwrap();
        client.lookup(TOPIC).await.unwrap();
        wait_until("both refreshes are answered", || server.auth_responses().len() >= 2).await;
        let connect = server.received(Type::CONNECT)[0].connect.as_option().unwrap().clone();
        assert_eq!(connect.auth_method_name.as_deref(), Some("token"));
        assert_eq!(connect.auth_data.as_deref(), Some(&b"token-0"[..]));
        assert_eq!(server.auth_responses(), ["token-1", "token-2"]);
        assert_eq!(fetched.load(Ordering::SeqCst), 3);
        // The connection still serves, so no new one sent a second CONNECT.
        client.lookup(TOPIC).await.unwrap();
        assert_eq!(server.received(Type::CONNECT).len(), 1);
        client.close().await.unwrap();
    });
}

#[test]
fn tls_with_a_trusted_ca() {
    run(async {
        let (acceptor, ca_file) = tls_fixture();
        let server = SimServer::start(Some(acceptor), self_serving).await;
        let url = format!("pulsar+ssl://localhost:{}", server.address.port);
        let client =
            local::PulsarClient::builder().service_url(url).tls_trust_certs_file(ca_file.path()).build().await.unwrap();
        // The lookup answer names 127.0.0.1, which the certificate also covers.
        let lookups: Vec<_> = (0..50).map(|_| client.lookup(TOPIC)).collect();
        assert!(futures_util::future::join_all(lookups).await.iter().all(|r| r.is_ok()));
        client.close().await.unwrap();

        // Without trusting the CA, every handshake fails until the operation timeout.
        let url = format!("pulsar+ssl://localhost:{}", server.address.port);
        let untrusting = local::PulsarClient::builder()
            .service_url(url)
            .tls_trust_platform_roots(true)
            .operation_timeout(Duration::from_millis(500))
            .build()
            .await
            .unwrap();
        assert!(untrusting.lookup(TOPIC).await.is_err());
        untrusting.close().await.unwrap();
    });
}

#[test]
fn tls_verification_options() {
    run(async {
        // The certificate names another host than the one the client connects to.
        let (acceptor, ca_file) = tls_fixture_for(&["other.example"]);
        let server = SimServer::start(Some(acceptor), |address| {
            let mut behavior = self_serving(address);
            // Answer with the address the client dialed, so the lookup's broker is the same host.
            behavior.broker.default_lookup = LookupAnswer::Connect { broker: address.clone(), proxy: false };
            behavior
        })
        .await;
        let url = format!("pulsar+ssl://127.0.0.1:{}", server.address.port);
        let builder =
            || local::PulsarClient::builder().service_url(url.clone()).operation_timeout(Duration::from_millis(500));
        let attempt = |builder: crate::ClientBuilder<crate::Local>| async move {
            let client = builder.build().await.unwrap();
            let result = client.lookup(TOPIC).await;
            client.close().await.unwrap();
            result
        };

        // Trusted CA, wrong name: refused by default, accepted with hostname verification off.
        assert!(attempt(builder().tls_trust_certs_file(ca_file.path())).await.is_err());
        attempt(builder().tls_trust_certs_file(ca_file.path()).tls_hostname_verification(false)).await.unwrap();

        // Untrusted CA: hostname verification off doesn't help; only an insecure connection accepts it.
        let (_, other_ca) = tls_fixture_for(&["other.example"]);
        assert!(
            attempt(builder().tls_trust_certs_file(other_ca.path()).tls_hostname_verification(false)).await.is_err()
        );
        attempt(builder().tls_trust_certs_file(other_ca.path()).allow_insecure_connection(true)).await.unwrap();
    });
}

#[test]
fn request_in_flight_when_the_connection_drops_is_retried() {
    run(async {
        // The first connection takes the lookup and closes without answering; later ones answer.
        let accepted = Rc::new(Cell::new(0));
        let counter = accepted.clone();
        let server = SimServer::start(None, move |address| {
            counter.set(counter.get() + 1);
            let mut behavior = self_serving(address);
            if counter.get() == 1 {
                behavior.broker.default_lookup = LookupAnswer::Silent;
                behavior.close_after = Some(2);
            }
            behavior
        })
        .await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let found = client.lookup(TOPIC).await.unwrap();
        assert_eq!(found, BrokerAddress::direct(server.address.clone()));
        assert_eq!(accepted.get(), 2);
        assert_eq!(server.received(Type::LOOKUP).len(), 2);
        client.close().await.unwrap();
    });
}

#[test]
fn dropping_the_last_client_handle_stops_its_loops() {
    let client =
        futures_executor::block_on(PulsarClient::builder().service_url("pulsar://127.0.0.1:1").event_loops(3).build())
            .unwrap();
    let threads = client.take_threads();
    assert_eq!(threads.len(), 3);
    let clone = client.clone();
    drop(client);
    std::thread::sleep(Duration::from_millis(50));
    assert!(threads.iter().all(|t| !t.is_finished()), "a clone still holds the client");
    drop(clone);
    let deadline = std::time::Instant::now() + Duration::from_secs(10);
    while !threads.iter().all(|t| t.is_finished()) {
        assert!(std::time::Instant::now() < deadline, "loop threads still running after the client was dropped");
        std::thread::sleep(Duration::from_millis(10));
    }
}

/// Closing the client releases its authentication provider, whose close is its drop: once the thread-safe client's
/// `close` has stopped its loops, nothing holds the provider, though a handle to the client remains. The local
/// client's goes with its last handle, once `close` has ended the connections that used it. Java's
/// `ClientAuthenticationLifecycleTest.closingTheClientClosesAV5NativePlugin`.
#[test]
fn closing_the_client_releases_its_authentication_provider() {
    /// Counts its drops.
    struct Counted(Arc<AtomicUsize>);
    impl crate::Authentication for Counted {
        fn method_name(&self) -> &str {
            "counted"
        }
        fn auth_data(&self) -> crate::auth::AuthFuture {
            Box::pin(async { Ok(Vec::new()) })
        }
    }
    impl Drop for Counted {
        fn drop(&mut self) {
            self.0.fetch_add(1, Ordering::SeqCst);
        }
    }

    let drops = Arc::new(AtomicUsize::new(0));
    let builder = PulsarClient::builder().service_url("pulsar://127.0.0.1:1").event_loops(2);
    let client = futures_executor::block_on(builder.authentication(Counted(drops.clone())).build()).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 0);
    futures_executor::block_on(client.close()).unwrap();
    assert_eq!(drops.load(Ordering::SeqCst), 1);

    let drops = Arc::new(AtomicUsize::new(0));
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let local = local::PulsarClient::builder()
            .service_url(server.service_url("pulsar"))
            .authentication(Counted(drops.clone()))
            .build()
            .await
            .unwrap();
        local.lookup(TOPIC).await.unwrap();
        local.close().await.unwrap();
        drop(local);
        assert_eq!(drops.load(Ordering::SeqCst), 1);
    });
}

#[test]
fn lookup_moves_to_the_next_service_host() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        // Nothing listens on the first host.
        let dead = TcpListener::bind("127.0.0.1:0").await.unwrap().local_addr().unwrap().port();
        let url = format!("pulsar://127.0.0.1:{dead},{}", server.address);
        let client = local::PulsarClient::builder().service_url(url).build().await.unwrap();
        client.lookup(TOPIC).await.unwrap();
        client.close().await.unwrap();
    });
}

#[test]
fn thread_safe_client_runs_on_its_loops_from_any_executor() {
    // The broker runs on its own thread and runtime; the client's futures are driven by a plain executor.
    let (address_tx, address_rx) = std::sync::mpsc::channel();
    let (stop_tx, stop_rx) = futures_channel::oneshot::channel::<()>();
    let broker = std::thread::spawn(move || {
        run(async move {
            let server = SimServer::start(None, self_serving).await;
            address_tx.send(server.address.clone()).unwrap();
            let _ = stop_rx.await;
        })
    });
    let address = address_rx.recv().unwrap();
    futures_executor::block_on(async {
        let client =
            PulsarClient::builder().service_url(format!("pulsar://{address}")).event_loops(2).build().await.unwrap();
        let a = client.run_on(0, |local| Box::pin(async move { local.lookup(TOPIC).await }));
        let b = client.run_on(1, |local| Box::pin(async move { local.lookup(TOPIC).await }));
        let (a, b) = futures_util::join!(a, b);
        assert_eq!(a.unwrap(), BrokerAddress::direct(address.clone()));
        assert_eq!(b.unwrap(), BrokerAddress::direct(address.clone()));
        let other = client.clone();
        client.close().await.unwrap();
        other.close().await.unwrap();
        assert!(client.threads_finished());
        let after = client.run_on(0, |local| Box::pin(async move { local.lookup(TOPIC).await })).await;
        assert_eq!(after.unwrap_err().kind(), ErrorKind::AlreadyClosed);
    });
    stop_tx.send(()).unwrap();
    broker.join().unwrap();
}

#[test]
fn client_is_send_and_sync() {
    fn check<T: Send + Sync + Clone + 'static>() {}
    check::<PulsarClient>();
}

/// A TLS acceptor with a certificate for `localhost` and `127.0.0.1`, and a file with that certificate to trust.
fn tls_fixture() -> (TlsAcceptor, TempFile) {
    tls_fixture_for(&["localhost", "127.0.0.1"])
}

/// A TLS acceptor with a self-signed certificate for `names`, and a file with that certificate to trust.
fn tls_fixture_for(names: &[&str]) -> (TlsAcceptor, TempFile) {
    use compio::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};
    let names = names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    let certified = rcgen::generate_simple_self_signed(names).unwrap();
    let cert: CertificateDer<'static> = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()));
    let provider = Arc::new(compio::rustls::crypto::ring::default_provider());
    let config = compio::rustls::ServerConfig::builder_with_provider(provider)
        .with_safe_default_protocol_versions()
        .unwrap()
        .with_no_client_auth()
        .with_single_cert(vec![cert], key)
        .unwrap();
    (TlsAcceptor::from(Arc::new(config)), TempFile::new(certified.cert.pem().as_bytes()))
}

struct TempFile(std::path::PathBuf);

impl TempFile {
    fn new(contents: &[u8]) -> Self {
        use std::hash::{BuildHasher, RandomState};
        let name = format!("apache-pulsar-test-{:x}.pem", RandomState::new().hash_one(std::time::Instant::now()));
        let path = std::env::temp_dir().join(name);
        std::fs::write(&path, contents).unwrap();
        Self(path)
    }

    fn path(&self) -> &std::path::Path {
        &self.0
    }
}

impl Drop for TempFile {
    fn drop(&mut self) {
        let _ = std::fs::remove_file(&self.0);
    }
}
