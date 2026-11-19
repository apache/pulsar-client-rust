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

//! Client options: what the builder refuses, the TLS setups it accepts, and the authentication providers.

use compio::rustls;
use compio::rustls::pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer};

use super::*;
use crate::auth::Authentication;
use crate::{ClientBuilder, Local, TlsAuthentication};

const TLS_URL: &str = "pulsar+ssl://broker:6651";

fn error_of<T>(result: crate::Result<T>) -> crate::Error {
    match result {
        Ok(_) => panic!("expected an error"),
        Err(error) => error,
    }
}

fn tls_builder() -> ClientBuilder<Local> {
    local::PulsarClient::builder().service_url(TLS_URL)
}

/// A self-signed certificate for `names`, with its key, in PEM.
fn certificate(names: &[&str]) -> (Vec<u8>, Vec<u8>) {
    let certified =
        rcgen::generate_simple_self_signed(names.iter().map(|n| n.to_string()).collect::<Vec<_>>()).unwrap();
    (certified.cert.pem().into_bytes(), certified.signing_key.serialize_pem().into_bytes())
}

/// A TLS acceptor speaking `versions`, with a certificate for `names`, and a file to trust that certificate. With
/// `client_ca`, it requires a client certificate signed by it.
fn server_config(
    versions: &[&'static rustls::SupportedProtocolVersion],
    client_ca: Option<&[u8]>,
    names: &[&str],
) -> (TlsAcceptor, TempFile) {
    let names = names.iter().map(|n| n.to_string()).collect::<Vec<_>>();
    let certified = rcgen::generate_simple_self_signed(names).unwrap();
    let cert: CertificateDer<'static> = certified.cert.der().clone();
    let key = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()));
    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let builder =
        rustls::ServerConfig::builder_with_provider(provider.clone()).with_protocol_versions(versions).unwrap();
    let builder = match client_ca {
        None => builder.with_no_client_auth(),
        Some(pem) => {
            use rustls::pki_types::pem::PemObject;
            let mut roots = rustls::RootCertStore::empty();
            roots.add(CertificateDer::from_pem_slice(pem).unwrap()).unwrap();
            let verifier =
                rustls::server::WebPkiClientVerifier::builder_with_provider(Arc::new(roots), provider).build().unwrap();
            builder.with_client_cert_verifier(verifier)
        }
    };
    let config = builder.with_single_cert(vec![cert], key).unwrap();
    (TlsAcceptor::from(Arc::new(config)), TempFile::new(certified.cert.pem().as_bytes()))
}

/// Builds a client from `builder`, looks `TOPIC` up, and closes it.
async fn lookup_with(builder: ClientBuilder<Local>) -> crate::Result<BrokerAddress> {
    let client = builder.operation_timeout(Duration::from_millis(500)).build().await.unwrap();
    let result = client.lookup(TOPIC).await;
    client.close().await.unwrap();
    result
}

#[test]
fn the_builder_refuses_incomplete_or_contradictory_options() {
    let error = error_of(futures_executor::block_on(PulsarClient::builder().build()));
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration, "{error}");
    let error = error_of(futures_executor::block_on(local::PulsarClient::builder().build()));
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration, "{error}");

    let error = error_of(local::PulsarClient::builder().service_url("http://broker:8080").into_config());
    assert_eq!(error.kind(), ErrorKind::InvalidServiceUrl, "{error}");

    let tls = TlsAuthentication::from_pem(Vec::new(), Vec::new());
    let error =
        error_of(local::PulsarClient::builder().service_url("pulsar://broker:6650").authentication(tls).into_config());
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
    assert!(error.to_string().contains("pulsar+ssl://"), "{error}");

    let builder = PulsarClient::builder().service_url("pulsar://broker:6650").event_loops(0);
    let error = error_of(futures_executor::block_on(builder.build()));
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration, "{error}");
    // The local builder has no setter for the loop count, but runs the same validation. The coverage report counts
    // each instantiation of the generic `into_config` on its own, so the local one is checked here too.
    let mut builder = local::PulsarClient::builder().service_url("pulsar://broker:6650");
    builder.event_loops = 0;
    assert_eq!(error_of(builder.into_config()).kind(), ErrorKind::InvalidConfiguration);
}

#[test]
fn options_print_without_credentials() {
    let builder = local::PulsarClient::builder()
        .service_url("pulsar://broker:6650")
        .authentication(Token::new("secret-token"))
        .connect_timeout(Duration::from_secs(3));
    let printed = format!("{builder:?}");
    assert!(printed.contains("pulsar://broker:6650") && printed.contains(r#"Some("token")"#), "{printed}");
    assert!(!printed.contains("secret-token"), "{printed}");

    let config = builder.into_config().unwrap();
    assert_eq!(config.connect_timeout, Duration::from_secs(3));
    let printed = format!("{config:?}");
    assert!(printed.contains("broker") && !printed.contains("secret-token"), "{printed}");
}

#[test]
fn token_providers() {
    let token = Token::new("abc");
    assert_eq!(token.method_name(), "token");
    assert_eq!(futures_executor::block_on(token.auth_data()).unwrap(), b"abc");
    // A single-round method answers a challenge with fresh data.
    assert_eq!(futures_executor::block_on(token.challenge(b"more")).unwrap(), b"abc");
    assert!(token.client_certificate().is_none());
    assert_eq!(format!("{token:?}"), r#"Token { source: "static", .. }"#);

    let fetched = Token::from_fn(|| async { Ok("fetched".to_string()) });
    assert_eq!(futures_executor::block_on(fetched.auth_data()).unwrap(), b"fetched");
    assert_eq!(format!("{fetched:?}"), r#"Token { source: "callback", .. }"#);
}

#[test]
fn tls_authentication_reads_its_pem_files() {
    let (chain, key) = certificate(&["client"]);
    let (chain_file, key_file) = (TempFile::new(&chain), TempFile::new(&key));
    let tls = TlsAuthentication::from_pem_files(chain_file.path(), key_file.path()).unwrap();
    assert_eq!(tls.method_name(), "tls");
    assert!(futures_executor::block_on(tls.auth_data()).unwrap().is_empty());
    assert_eq!(tls.client_certificate().unwrap().chain_pem, chain);
    let printed = format!("{tls:?}");
    assert!(printed.contains("<redacted>") && !printed.contains("PRIVATE KEY"), "{printed}");

    let error = TlsAuthentication::from_pem_files(chain_file.path(), "/nonexistent/key.pem").unwrap_err();
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
    assert!(error.to_string().contains("/nonexistent/key.pem"), "{error}");
}

#[test]
fn bad_trust_files_are_refused() {
    let refused = |path: &std::path::Path| {
        let error = error_of(tls_builder().tls_trust_certs_file(path).into_config());
        assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
        error.to_string()
    };
    assert!(refused(std::path::Path::new("/nonexistent/ca.pem")).contains("can't read"));
    // Not base64 inside the PEM block; and base64 that isn't a certificate.
    let file = TempFile::new(b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n");
    assert!(refused(file.path()).contains("bad certificate"));
    let file = TempFile::new(b"-----BEGIN CERTIFICATE-----\nAAAA\n-----END CERTIFICATE-----\n");
    assert!(refused(file.path()).contains("bad certificate"));
}

#[test]
fn hostname_verification_off_still_needs_trusted_roots() {
    let builder = tls_builder().tls_trust_platform_roots(false).tls_hostname_verification(false);
    let error = error_of(builder.into_config());
    assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
    assert!(error.to_string().contains("TLS trust"), "{error}");
}

#[test]
fn bad_client_certificates_are_refused() {
    let (chain, key) = certificate(&["client"]);
    let configure = |chain: &[u8], key: &[u8]| {
        let tls = TlsAuthentication::from_pem(chain.to_vec(), key.to_vec());
        tls_builder().tls_trust_platform_roots(false).authentication(tls).into_config()
    };
    assert!(configure(&chain, &key).is_ok());
    let bad_chain = b"-----BEGIN CERTIFICATE-----\n!!!!\n-----END CERTIFICATE-----\n";
    assert!(error_of(configure(bad_chain, &key)).to_string().contains("client certificate"));
    assert!(error_of(configure(&chain, b"not a key")).to_string().contains("client key"));
    let other_key = rcgen::KeyPair::generate().unwrap().serialize_pem().into_bytes();
    assert!(error_of(configure(&chain, &other_key)).to_string().contains("client certificate"));
}

#[test]
fn tls_1_2_brokers_with_relaxed_verification() {
    run(async {
        // A TLS 1.2 broker whose certificate names another host than the one the client dials.
        let (acceptor, ca_file) = server_config(&[&rustls::version::TLS12], None, &["other.example"]);
        let server = SimServer::start(Some(acceptor), self_serving).await;
        let url = format!("pulsar+ssl://127.0.0.1:{}", server.address.port);
        let builder = || local::PulsarClient::builder().service_url(url.clone());

        lookup_with(builder().tls_trust_certs_file(ca_file.path()).tls_hostname_verification(false)).await.unwrap();
        lookup_with(builder().allow_insecure_connection(true)).await.unwrap();
        assert!(lookup_with(builder().tls_trust_certs_file(ca_file.path())).await.is_err());
    });
}

#[test]
fn mutual_tls_presents_the_client_certificate() {
    run(async {
        let (chain, key) = certificate(&["client"]);
        let versions = rustls::DEFAULT_VERSIONS;
        let (acceptor, ca_file) = server_config(versions, Some(&chain), &["localhost", "127.0.0.1"]);
        let server = SimServer::start(Some(acceptor), self_serving).await;
        let url = format!("pulsar+ssl://localhost:{}", server.address.port);
        let builder = || local::PulsarClient::builder().service_url(url.clone()).tls_trust_certs_file(ca_file.path());

        let tls = TlsAuthentication::from_pem(chain.clone(), key.clone());
        lookup_with(builder().authentication(tls)).await.unwrap();
        let connect = server.received(Type::CONNECT)[0].connect.as_option().unwrap().clone();
        assert_eq!(connect.auth_method_name.as_deref(), Some("tls"));

        // Without the certificate, the broker refuses the handshake.
        assert!(lookup_with(builder()).await.is_err());
    });
}
