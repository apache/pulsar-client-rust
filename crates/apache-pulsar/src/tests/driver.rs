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

//! The connection driver's socket paths: writes the socket takes only in part, and a read that fails.

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
