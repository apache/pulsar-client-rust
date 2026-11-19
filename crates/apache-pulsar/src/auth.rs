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

//! Authentication providers (docs/4-connections.md §6).
//!
//! A provider is shared by every loop of a client, so it is `Send + Sync`. It is called when a connection is
//! made and when the broker asks for fresh credentials, never per message.

use std::fmt;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;

use crate::error::{Error, ErrorKind, Result};

/// A future returned by an authentication provider.
pub type AuthFuture = Pin<Box<dyn Future<Output = Result<Vec<u8>>> + Send>>;

/// Supplies the credentials a connection presents.
pub trait Authentication: Send + Sync + 'static {
    /// The method name the broker selects its provider by: `token`, `tls`, …
    fn method_name(&self) -> &str;

    /// The data to send in `CONNECT`, and in answer to a refresh challenge. Called again for each.
    fn auth_data(&self) -> AuthFuture;

    /// Answers a challenge of a multi-round exchange. Single-round methods answer with fresh data.
    fn challenge(&self, _challenge: &[u8]) -> AuthFuture {
        self.auth_data()
    }

    /// The client certificate chain and private key, in PEM, for methods that authenticate with mutual TLS.
    fn client_certificate(&self) -> Option<ClientCertificate> {
        None
    }
}

/// A client certificate chain and its private key, in PEM.
#[derive(Clone)]
pub struct ClientCertificate {
    pub chain_pem: Vec<u8>,
    pub key_pem: Vec<u8>,
}

impl fmt::Debug for ClientCertificate {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("ClientCertificate").field("key_pem", &"<redacted>").finish_non_exhaustive()
    }
}

type TokenFn = dyn Fn() -> Pin<Box<dyn Future<Output = Result<String>> + Send>> + Send + Sync;

/// Token authentication: a static token, or one fetched by a callback each time a connection needs it.
#[derive(Clone)]
pub struct Token {
    source: TokenSource,
}

#[derive(Clone)]
enum TokenSource {
    Static(Arc<str>),
    Callback(Arc<TokenFn>),
}

impl Token {
    pub fn new(token: impl Into<String>) -> Self {
        Self { source: TokenSource::Static(token.into().into()) }
    }

    /// A token fetched by `fetch` whenever a connection is made or the broker asks for a refresh.
    pub fn from_fn<F, Fut>(fetch: F) -> Self
    where
        F: Fn() -> Fut + Send + Sync + 'static,
        Fut: Future<Output = Result<String>> + Send + 'static,
    {
        Self { source: TokenSource::Callback(Arc::new(move || Box::pin(fetch()))) }
    }
}

impl fmt::Debug for Token {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let source = match self.source {
            TokenSource::Static(_) => "static",
            TokenSource::Callback(_) => "callback",
        };
        f.debug_struct("Token").field("source", &source).finish_non_exhaustive()
    }
}

impl Authentication for Token {
    fn method_name(&self) -> &str {
        "token"
    }

    fn auth_data(&self) -> AuthFuture {
        match &self.source {
            TokenSource::Static(token) => {
                let token = token.as_bytes().to_vec();
                Box::pin(async move { Ok(token) })
            }
            TokenSource::Callback(fetch) => {
                let fetch = fetch.clone();
                Box::pin(async move { fetch().await.map(String::into_bytes) })
            }
        }
    }
}

/// Mutual TLS authentication: the client certificate is the credential. Needs a `pulsar+ssl://` service URL.
#[derive(Clone, Debug)]
pub struct TlsAuthentication {
    certificate: ClientCertificate,
}

impl TlsAuthentication {
    /// Reads the certificate chain and private key from PEM files.
    pub fn from_pem_files(chain: impl AsRef<std::path::Path>, key: impl AsRef<std::path::Path>) -> Result<Self> {
        let read = |path: &std::path::Path| {
            std::fs::read(path).map_err(|e| {
                Error::with_source(ErrorKind::InvalidConfiguration, format!("can't read {}", path.display()), e)
            })
        };
        Ok(Self::from_pem(read(chain.as_ref())?, read(key.as_ref())?))
    }

    pub fn from_pem(chain_pem: Vec<u8>, key_pem: Vec<u8>) -> Self {
        Self { certificate: ClientCertificate { chain_pem, key_pem } }
    }
}

impl Authentication for TlsAuthentication {
    fn method_name(&self) -> &str {
        "tls"
    }

    fn auth_data(&self) -> AuthFuture {
        Box::pin(async { Ok(Vec::new()) })
    }

    fn client_certificate(&self) -> Option<ClientCertificate> {
        Some(self.certificate.clone())
    }
}
