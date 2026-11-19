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

//! Client options and their builder, shared by the thread-safe and the local client.

use std::fmt;
use std::marker::PhantomData;
use std::path::PathBuf;
use std::sync::Arc;
use std::time::Duration;

use apache_pulsar_protocol::lookup::ServiceUrl;
use compio::rustls::{self, pki_types};

use crate::auth::Authentication;
use crate::error::{Error, ErrorKind, Result};

/// Marks a builder for the thread-safe client.
#[derive(Debug, Clone, Copy)]
pub enum ThreadSafe {}

/// Marks a builder for the local client.
#[derive(Debug, Clone, Copy)]
pub enum Local {}

/// Builds a client. `PulsarClient::builder()` and `local::PulsarClient::builder()` both return one; the type
/// parameter only decides which client `build` makes.
pub struct ClientBuilder<Api> {
    service_url: Option<String>,
    authentication: Option<Arc<dyn Authentication>>,
    tls_trust_certs_file: Option<PathBuf>,
    tls_trust_platform_roots: Option<bool>,
    allow_insecure_connection: bool,
    tls_hostname_verification: bool,
    connect_timeout: Duration,
    operation_timeout: Duration,
    keepalive_interval: Duration,
    pub(crate) event_loops: usize,
    _api: PhantomData<Api>,
}

impl<Api> fmt::Debug for ClientBuilder<Api> {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientBuilder")
            .field("service_url", &self.service_url)
            .field("authentication", &self.authentication.as_ref().map(|a| a.method_name().to_string()))
            .field("event_loops", &self.event_loops)
            .finish_non_exhaustive()
    }
}

impl<Api> Default for ClientBuilder<Api> {
    fn default() -> Self {
        Self {
            service_url: None,
            authentication: None,
            tls_trust_certs_file: None,
            tls_trust_platform_roots: None,
            allow_insecure_connection: false,
            tls_hostname_verification: true,
            connect_timeout: Duration::from_secs(10),
            operation_timeout: Duration::from_secs(30),
            keepalive_interval: Duration::from_secs(30),
            // Leaning: one loop until benchmarks say otherwise (docs/2-architecture.md §5).
            event_loops: 1,
            _api: PhantomData,
        }
    }
}

impl<Api> ClientBuilder<Api> {
    /// `pulsar://host:port[,host:port…]` or `pulsar+ssl://…`. Required.
    pub fn service_url(mut self, url: impl Into<String>) -> Self {
        self.service_url = Some(url.into());
        self
    }

    pub fn authentication(mut self, authentication: impl Authentication) -> Self {
        self.authentication = Some(Arc::new(authentication));
        self
    }

    /// PEM file of CA certificates to trust. Without it the platform's roots are trusted.
    pub fn tls_trust_certs_file(mut self, path: impl Into<PathBuf>) -> Self {
        self.tls_trust_certs_file = Some(path.into());
        self
    }

    /// Whether to trust the platform's roots too. Defaults to `true` without a trust file, `false` with one.
    pub fn tls_trust_platform_roots(mut self, trust: bool) -> Self {
        self.tls_trust_platform_roots = Some(trust);
        self
    }

    /// Accept any server certificate. For tests only.
    pub fn allow_insecure_connection(mut self, allow: bool) -> Self {
        self.allow_insecure_connection = allow;
        self
    }

    /// Check that the server certificate names the host. On by default.
    pub fn tls_hostname_verification(mut self, verify: bool) -> Self {
        self.tls_hostname_verification = verify;
        self
    }

    /// TCP connect, TLS handshake and `CONNECT`, together. 10 s by default.
    pub fn connect_timeout(mut self, timeout: Duration) -> Self {
        self.connect_timeout = timeout;
        self
    }

    /// How long a request may wait for its response, such as a lookup. 30 s by default.
    pub fn operation_timeout(mut self, timeout: Duration) -> Self {
        self.operation_timeout = timeout;
        self
    }

    /// How often to ping an idle broker. 30 s by default.
    pub fn keepalive_interval(mut self, interval: Duration) -> Self {
        self.keepalive_interval = interval;
        self
    }

    pub(crate) fn into_config(self) -> Result<ClientConfig> {
        let url =
            self.service_url.ok_or_else(|| Error::new(ErrorKind::InvalidConfiguration, "a service URL is required"))?;
        let service = ServiceUrl::parse(&url)?;
        let tls = if service.tls {
            Some(Arc::new(tls_config(
                self.tls_trust_certs_file.as_deref(),
                self.tls_trust_platform_roots,
                self.allow_insecure_connection,
                self.tls_hostname_verification,
                self.authentication.as_deref(),
            )?))
        } else {
            if self.authentication.as_ref().is_some_and(|a| a.client_certificate().is_some()) {
                return Err(Error::new(
                    ErrorKind::InvalidConfiguration,
                    "TLS authentication needs a pulsar+ssl:// URL",
                ));
            }
            None
        };
        if self.event_loops == 0 {
            return Err(Error::new(ErrorKind::InvalidConfiguration, "at least one event loop is needed"));
        }
        Ok(ClientConfig {
            service,
            authentication: self.authentication,
            tls,
            connect_timeout: self.connect_timeout,
            operation_timeout: self.operation_timeout,
            keepalive_interval: self.keepalive_interval,
        })
    }
}

impl ClientBuilder<ThreadSafe> {
    /// How many event loops, each on its own thread, the client runs. 1 by default.
    pub fn event_loops(mut self, loops: usize) -> Self {
        self.event_loops = loops;
        self
    }
}

/// Validated client options.
#[derive(Clone)]
pub(crate) struct ClientConfig {
    pub service: ServiceUrl,
    pub authentication: Option<Arc<dyn Authentication>>,
    pub tls: Option<Arc<rustls::ClientConfig>>,
    pub connect_timeout: Duration,
    pub operation_timeout: Duration,
    pub keepalive_interval: Duration,
}

impl fmt::Debug for ClientConfig {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientConfig").field("service", &self.service).finish_non_exhaustive()
    }
}

fn invalid(message: impl Into<String>) -> impl FnOnce(rustls::Error) -> Error {
    let message = message.into();
    move |e| Error::with_source(ErrorKind::InvalidConfiguration, message, e)
}

fn tls_config(
    trust_file: Option<&std::path::Path>,
    platform_roots: Option<bool>,
    insecure: bool,
    hostname_verification: bool,
    authentication: Option<&dyn Authentication>,
) -> Result<rustls::ClientConfig> {
    use pki_types::pem::PemObject;
    use pki_types::{CertificateDer, PrivateKeyDer};

    let provider = Arc::new(rustls::crypto::ring::default_provider());
    let mut roots = rustls::RootCertStore::empty();
    if let Some(path) = trust_file {
        let pem = std::fs::read(path).map_err(|e| {
            Error::with_source(ErrorKind::InvalidConfiguration, format!("can't read {}", path.display()), e)
        })?;
        for cert in CertificateDer::pem_slice_iter(&pem) {
            let cert = cert.map_err(|e| {
                Error::new(ErrorKind::InvalidConfiguration, format!("bad certificate in {}: {e}", path.display()))
            })?;
            roots.add(cert).map_err(invalid(format!("bad certificate in {}", path.display())))?;
        }
    }
    if platform_roots.unwrap_or(trust_file.is_none()) {
        for cert in rustls_native_certs::load_native_certs().certs {
            // Some platform roots don't parse; skip them, as browsers do.
            let _ = roots.add(cert);
        }
    }

    let builder = rustls::ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(invalid("TLS protocol versions"))?;
    let builder = if insecure {
        builder.dangerous().with_custom_certificate_verifier(Arc::new(verify::AcceptAny(provider)))
    } else if !hostname_verification {
        let inner = rustls::client::WebPkiServerVerifier::builder_with_provider(Arc::new(roots), provider)
            .build()
            .map_err(|e| Error::new(ErrorKind::InvalidConfiguration, format!("TLS trust: {e}")))?;
        builder.dangerous().with_custom_certificate_verifier(Arc::new(verify::AnyName(inner)))
    } else {
        builder.with_root_certificates(roots)
    };

    let certificate = authentication.and_then(|a| a.client_certificate());
    match certificate {
        None => Ok(builder.with_no_client_auth()),
        Some(certificate) => {
            let chain = CertificateDer::pem_slice_iter(&certificate.chain_pem)
                .collect::<std::result::Result<Vec<_>, _>>()
                .map_err(|e| Error::new(ErrorKind::InvalidConfiguration, format!("client certificate: {e}")))?;
            let key = PrivateKeyDer::from_pem_slice(&certificate.key_pem)
                .map_err(|e| Error::new(ErrorKind::InvalidConfiguration, format!("client key: {e}")))?;
            builder.with_client_auth_cert(chain, key).map_err(invalid("client certificate"))
        }
    }
}

mod verify {
    use std::sync::Arc;

    use compio::rustls::client::WebPkiServerVerifier;
    use compio::rustls::client::danger::{HandshakeSignatureValid, ServerCertVerified, ServerCertVerifier};
    use compio::rustls::crypto::{CryptoProvider, verify_tls12_signature, verify_tls13_signature};
    use compio::rustls::pki_types::{CertificateDer, ServerName, UnixTime};
    use compio::rustls::{CertificateError, DigitallySignedStruct, Error, SignatureScheme};

    /// Accepts any certificate (`allow_insecure_connection`), still checking the handshake signatures.
    #[derive(Debug)]
    pub struct AcceptAny(pub Arc<CryptoProvider>);

    impl ServerCertVerifier for AcceptAny {
        fn verify_server_cert(
            &self,
            _: &CertificateDer<'_>,
            _: &[CertificateDer<'_>],
            _: &ServerName<'_>,
            _: &[u8],
            _: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            Ok(ServerCertVerified::assertion())
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls12_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            verify_tls13_signature(message, cert, dss, &self.0.signature_verification_algorithms)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.signature_verification_algorithms.supported_schemes()
        }
    }

    /// Verifies the chain against the trusted roots, but not the host name.
    #[derive(Debug)]
    pub struct AnyName(pub Arc<WebPkiServerVerifier>);

    impl ServerCertVerifier for AnyName {
        fn verify_server_cert(
            &self,
            end_entity: &CertificateDer<'_>,
            intermediates: &[CertificateDer<'_>],
            server_name: &ServerName<'_>,
            ocsp: &[u8],
            now: UnixTime,
        ) -> Result<ServerCertVerified, Error> {
            match self.0.verify_server_cert(end_entity, intermediates, server_name, ocsp, now) {
                Err(Error::InvalidCertificate(CertificateError::NotValidForName))
                | Err(Error::InvalidCertificate(CertificateError::NotValidForNameContext { .. })) => {
                    Ok(ServerCertVerified::assertion())
                }
                other => other,
            }
        }

        fn verify_tls12_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            self.0.verify_tls12_signature(message, cert, dss)
        }

        fn verify_tls13_signature(
            &self,
            message: &[u8],
            cert: &CertificateDer<'_>,
            dss: &DigitallySignedStruct,
        ) -> Result<HandshakeSignatureValid, Error> {
            self.0.verify_tls13_signature(message, cert, dss)
        }

        fn supported_verify_schemes(&self) -> Vec<SignatureScheme> {
            self.0.supported_verify_schemes()
        }
    }
}
