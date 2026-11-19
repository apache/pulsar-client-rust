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

//! The local client's lookups, its pool closing under a connection attempt, and how the clients print.

use apache_pulsar_protocol::proto::ServerError;

use super::*;

/// A broker that answers every lookup with `answer`.
fn answering(answer: LookupAnswer) -> impl Fn(&HostPort) -> Behavior + 'static {
    move |_| {
        let mut broker = SimBroker::new();
        broker.default_lookup = answer.clone();
        Behavior { broker, close_after: None, push_after_connect: vec![] }
    }
}

#[test]
fn a_lookup_the_broker_refuses_is_not_retried() {
    run(async {
        let refusal = LookupAnswer::Fail { error: ServerError::TopicNotFound, message: "no such topic".into() };
        let server = SimServer::start(None, answering(refusal)).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let error = client.lookup(TOPIC).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::NotFound, "{error}");
        assert_eq!(server.received(Type::LOOKUP).len(), 1);
        client.close().await.unwrap();
    });
}

#[test]
fn lookups_follow_redirects() {
    run(async {
        let owner = SimServer::start(None, self_serving).await;
        let redirect = LookupAnswer::Redirect { broker: owner.address.clone(), authoritative: true };
        let service = SimServer::start(None, answering(redirect)).await;
        let client = local::PulsarClient::builder().service_url(service.service_url("pulsar")).build().await.unwrap();
        let found = client.lookup(TOPIC).await.unwrap();
        assert_eq!(found, BrokerAddress::direct(owner.address.clone()));
        let asked = owner.received(Type::LOOKUP)[0].lookupTopic.as_option().unwrap().clone();
        assert_eq!(asked.authoritative, Some(true));
        client.close().await.unwrap();
    });
}

#[test]
fn closing_while_a_connection_is_being_made() {
    run(async {
        let server = SimServer::start(None, self_serving).await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let address = BrokerAddress::direct(server.address.clone());
        // The connection attempt starts, then the client closes before it completes.
        let (connected, closed) = futures_util::join!(client.connection(&address), client.close());
        closed.unwrap();
        assert_eq!(connected.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        // The pool refuses later requests itself, too.
        assert_eq!(client.pool().get(&address).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
        assert_eq!(server.connections.get(), 1);
    });
}

#[test]
fn closing_while_a_connection_attempt_fails() {
    run(async {
        let server = SimServer::start(None, |address| {
            let mut behavior = self_serving(address);
            behavior.broker.refuse = Some(ServerError::ServiceNotReady);
            behavior
        })
        .await;
        let client = local::PulsarClient::builder().service_url(server.service_url("pulsar")).build().await.unwrap();
        let address = BrokerAddress::direct(server.address.clone());
        let (connected, closed) = futures_util::join!(client.connection(&address), client.close());
        closed.unwrap();
        assert_eq!(connected.unwrap_err().kind(), ErrorKind::AlreadyClosed);
    });
}

#[test]
fn clients_print_their_loops_and_connections() {
    run(async {
        let local = local::PulsarClient::builder().service_url("pulsar://127.0.0.1:1").build().await.unwrap();
        let printed = format!("{local:?}");
        assert!(printed.contains("local::PulsarClient") && printed.contains("connections: 0"), "{printed}");
    });
    let client =
        futures_executor::block_on(PulsarClient::builder().service_url("pulsar://127.0.0.1:1").event_loops(2).build())
            .unwrap();
    assert!(format!("{client:?}").contains("event_loops: 2"));
    // With nothing placed yet, the first loop is the least loaded; the slot counts on it until dropped.
    let (slot, index) = client.place();
    assert_eq!((index, client.loads()), (0, vec![1, 0]));
    drop(slot);
    assert_eq!(client.loads(), [0, 0]);
    futures_executor::block_on(client.close()).unwrap();
}

fn lookup_job(
    local: local::PulsarClient,
) -> futures_util::future::LocalBoxFuture<'static, crate::Result<BrokerAddress>> {
    Box::pin(async move { local.lookup(TOPIC).await })
}

#[test]
fn jobs_run_on_their_loop_until_the_client_closes() {
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
        assert!(!client.threads_finished());
        assert_eq!(client.run_on(1, lookup_job).await.unwrap(), BrokerAddress::direct(address.clone()));
        client.close().await.unwrap();
        assert_eq!(client.run_on(1, lookup_job).await.unwrap_err().kind(), ErrorKind::AlreadyClosed);
    });
    stop_tx.send(()).unwrap();
    broker.join().unwrap();
}

/// Builds a thread-safe client with two event loops, started by `starter`, and returns why it failed.
fn build_failure(starter: crate::client::LoopStarter) -> crate::Error {
    let builder = PulsarClient::builder().service_url("pulsar://127.0.0.1:1").event_loops(2);
    match futures_executor::block_on(builder.build_with(starter)) {
        Ok(_) => panic!("the client started"),
        Err(error) => error,
    }
}

#[test]
fn a_loop_thread_that_cannot_be_spawned_fails_the_build() {
    use crate::client::LoopStarter;
    let starter = LoopStarter {
        // The first loop starts; the second one's thread can't be spawned.
        spawn: |name, run| {
            if name.ends_with("-1") {
                Err(std::io::Error::other("no more threads"))
            } else {
                (LoopStarter::SYSTEM.spawn)(name, run)
            }
        },
        ..LoopStarter::SYSTEM
    };
    let error = build_failure(starter);
    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(error.to_string().contains("can't start an event loop thread"), "{error}");
}

#[test]
fn a_loop_without_a_runtime_fails_the_build() {
    use crate::client::LoopStarter;
    let starter = LoopStarter { runtime: || Err(std::io::Error::other("no I/O driver")), ..LoopStarter::SYSTEM };
    let error = build_failure(starter);
    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(error.to_string().contains("can't start an event loop"), "{error}");
    assert!(std::error::Error::source(&error).unwrap().to_string().contains("no I/O driver"));
}

#[test]
fn a_loop_thread_that_exits_without_answering_fails_the_build() {
    use crate::client::LoopStarter;
    let starter = LoopStarter {
        spawn: |name, run| {
            drop(run);
            std::thread::Builder::new().name(name).spawn(|| {})
        },
        ..LoopStarter::SYSTEM
    };
    let error = build_failure(starter);
    assert_eq!(error.kind(), ErrorKind::Other);
    assert!(error.to_string().contains("an event loop failed to start"), "{error}");
}
