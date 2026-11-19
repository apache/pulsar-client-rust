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

//! Integration tests against a real broker and proxy, which the harness starts in Docker (`env.rs`).
//!
//! `cargo test` skips this target; run it with `cargo test -p apache-pulsar --test integration`. The broker and
//! the proxy start before the first test, serve them all, and are removed when the run ends. On a failure, the
//! last lines of their logs are printed.

mod env;

use std::sync::Arc;
use std::time::Duration;

use apache_pulsar::{ClientBuilder, ErrorKind, Local, TlsAuthentication, Token, local};
use apache_pulsar_protocol::lookup::{BrokerAddress, HostPort, ServiceUrl};
use env::Env;
use libtest_mimic::{Arguments, Trial};

const TESTS: &[(&str, fn(&Env))] = &[
    ("plaintext_broker", plaintext_broker),
    ("plaintext_proxy", plaintext_proxy),
    ("tls_with_token", tls_with_token),
    ("tls_with_client_certificate", tls_with_client_certificate),
    ("missing_credentials_are_refused", missing_credentials_are_refused),
];

fn main() {
    let args = Arguments::from_args();
    // Listing the tests needs no environment.
    let env = (!args.list).then(|| Arc::new(Env::start()));
    let trials = TESTS
        .iter()
        .map(|&(name, test)| {
            let env = env.clone();
            Trial::test(name, move || {
                test(env.as_deref().expect("the environment is started unless listing"));
                Ok(())
            })
        })
        .collect();
    let conclusion = libtest_mimic::run(&args, trials);
    if conclusion.has_failed()
        && let Some(env) = &env
    {
        env.print_logs();
    }
    // Remove the containers before exiting: `exit` runs no destructors.
    drop(env);
    conclusion.exit();
}

fn run<F: std::future::Future>(f: F) -> F::Output {
    compio::runtime::Runtime::new().unwrap().block_on(f)
}

fn topic() -> String {
    use std::hash::{BuildHasher, RandomState};
    format!("persistent://public/default/rust-it-{:x}", RandomState::new().hash_one(std::time::Instant::now()))
}

fn host_of(url: &str) -> HostPort {
    ServiceUrl::parse(url).unwrap().hosts.remove(0)
}

/// Looks a topic up, then connects where the answer says. Standalone brokers with a loopback advertised address,
/// and proxies, answer "through the service URL", so the connection then goes to the service host.
async fn lookup_and_connect(builder: ClientBuilder<Local>) -> apache_pulsar::Result<BrokerAddress> {
    let client = builder.operation_timeout(Duration::from_secs(10)).build().await?;
    let result = async {
        let found = client.lookup(&topic()).await?;
        let connection = client.connection(&found).await?;
        assert!(connection.broker().features.scalable_topics, "{:?}", connection.broker());
        Ok(found)
    }
    .await;
    client.close().await?;
    result
}

fn token_client(url: &str, token: Option<String>) -> ClientBuilder<Local> {
    let builder = local::PulsarClient::builder().service_url(url);
    match token {
        Some(token) => builder.authentication(Token::new(token)),
        None => builder,
    }
}

fn plaintext_broker(env: &Env) {
    run(async {
        let found = lookup_and_connect(token_client(env::BROKER_URL, Some(env.token.clone()))).await.unwrap();
        assert_eq!(found.physical, host_of(env::BROKER_URL), "{found:?}");
    });
}

fn plaintext_proxy(env: &Env) {
    run(async {
        let found = lookup_and_connect(token_client(env::PROXY_URL, Some(env.token.clone()))).await.unwrap();
        assert!(found.through_proxy(), "{found:?}");
        assert_eq!(found.physical, host_of(env::PROXY_URL));
    });
}

fn tls_with_token(env: &Env) {
    for url in [env::BROKER_TLS_URL, env::PROXY_TLS_URL] {
        run(async {
            let builder = token_client(url, Some(env.token.clone())).tls_trust_certs_file(env.ca());
            lookup_and_connect(builder).await.unwrap_or_else(|e| panic!("{url}: {e}"));
        });
    }
}

fn tls_with_client_certificate(env: &Env) {
    for url in [env::BROKER_TLS_URL, env::PROXY_TLS_URL] {
        run(async {
            let builder = local::PulsarClient::builder()
                .service_url(url)
                .tls_trust_certs_file(env.ca())
                .authentication(TlsAuthentication::from_pem_files(env.client_cert(), env.client_key()).unwrap());
            lookup_and_connect(builder).await.unwrap_or_else(|e| panic!("{url}: {e}"));
        });
    }
}

fn missing_credentials_are_refused(_: &Env) {
    run(async {
        let error = lookup_and_connect(token_client(env::BROKER_URL, None)).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
        let error = lookup_and_connect(token_client(env::BROKER_URL, Some("not-a-token".into()))).await.unwrap_err();
        assert_eq!(error.kind(), ErrorKind::Authentication, "{error}");
    });
}
