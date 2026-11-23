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

//! TLS and authentication: a `pulsar+ssl://` service URL, the CA certificate to trust, and a token, or a client
//! certificate for mutual TLS. It sends a message and reads it back.
//!
//!     PULSAR_URL=pulsar+ssl://broker.example.com:6651 PULSAR_TLS_TRUST_CERTS=ca.cert.pem \
//!         PULSAR_TOKEN_FILE=client.token cargo run --example authentication
//!
//! `PULSAR_TOKEN` gives the token itself, and `PULSAR_TLS_CERT` and `PULSAR_TLS_KEY`, PEM files, a client certificate
//! instead of a token. Without `PULSAR_TLS_TRUST_CERTS`, the platform's roots are trusted. The first argument sets
//! another topic.

use std::error::Error;
use std::time::Duration;

use apache_pulsar::{ErrorKind, PulsarClient, Schema, SubscriptionInitialPosition, TlsAuthentication, Token};

#[tokio::main]
async fn main() -> Result<(), Box<dyn Error>> {
    let url = std::env::var("PULSAR_URL").unwrap_or_else(|_| "pulsar+ssl://localhost:6651".into());
    let topic = std::env::args().nth(1).unwrap_or_else(|| "topic://public/default/secured".into());

    let mut builder = PulsarClient::builder().service_url(url);
    if let Ok(ca) = std::env::var("PULSAR_TLS_TRUST_CERTS") {
        // The CA that signed the broker's certificate, which must name the broker's host.
        builder = builder.tls_trust_certs_file(ca);
    }
    if let (Ok(cert), Ok(key)) = (std::env::var("PULSAR_TLS_CERT"), std::env::var("PULSAR_TLS_KEY")) {
        // Mutual TLS: the client's certificate is the credential, and its common name the role.
        builder = builder.authentication(TlsAuthentication::from_pem_files(cert, key)?);
    } else if let Ok(file) = std::env::var("PULSAR_TOKEN_FILE") {
        // Read for each new connection, and when the broker asks for fresh credentials: a token renewed in the file is
        // taken without a restart. The provider runs on the client's event loop, not on Tokio, so it reads the file
        // with the standard library.
        builder = builder.authentication(Token::from_fn(move || {
            let read = std::fs::read_to_string(&file);
            let file = file.clone();
            async move {
                read.map(|token| token.trim().to_string()).map_err(|error| {
                    apache_pulsar::Error::new(ErrorKind::Authentication, format!("can't read {file}: {error}"))
                })
            }
        }));
    } else if let Ok(token) = std::env::var("PULSAR_TOKEN") {
        builder = builder.authentication(Token::new(token));
    }
    let client = builder.build().await?;

    let consumer = client
        .new_queue_consumer(Schema::string())
        .topic(&topic)
        .subscription_name("secured")
        .subscription_initial_position(SubscriptionInitialPosition::Latest)
        .subscribe()
        .await?;
    let producer = client.new_producer(Schema::string()).topic(&topic).create().await?;
    let id = producer.send("over TLS".into()).await?;
    println!("stored a message as {id}");
    if let Some(message) = consumer.receive_timeout(Duration::from_secs(10)).await? {
        println!("read it back: {}", message.value()?);
        consumer.acknowledge(message.id()).await?;
    }

    producer.close().await?;
    consumer.close().await?;
    client.close().await?;
    Ok(())
}
