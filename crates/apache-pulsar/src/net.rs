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

//! TCP and TLS transport, through compio's poll-based adapters.
//!
//! The connection task polls reads and writes of one stream from one future, so a read can stay parked while
//! frames go out, and nothing is ever dropped in flight (docs/2-architecture.md §4.4, rule 5). The adapters keep
//! the in-flight operation and its buffer inside the stream between polls.

use std::io;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use apache_pulsar_protocol::lookup::HostPort;
use compio::io::compat::AsyncStream;
use compio::net::TcpStream;
use compio::rustls;
use compio::rustls::pki_types::ServerName;
use compio::tls::{TlsConnector, TlsStream};
use futures_util::io::{AsyncRead, AsyncWrite};

/// A connected stream, plain or TLS.
pub(crate) enum Stream {
    Plain(Pin<Box<AsyncStream<TcpStream>>>),
    Tls(Pin<Box<TlsStream<TcpStream>>>),
}

impl std::fmt::Debug for Stream {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str(match self {
            Self::Plain(_) => "Stream::Plain",
            Self::Tls(_) => "Stream::Tls",
        })
    }
}

/// Connects to `address`, with TLS if `tls` is given. Host names are resolved on compio's blocking pool.
pub(crate) async fn connect(address: &HostPort, tls: Option<&Arc<rustls::ClientConfig>>) -> io::Result<Stream> {
    let tcp = TcpStream::connect((address.host.as_str(), address.port)).await?;
    tcp.set_nodelay(true)?;
    match tls {
        None => Ok(Stream::Plain(Box::pin(AsyncStream::new(tcp)))),
        Some(config) => {
            ServerName::try_from(address.host.as_str()).map_err(|e| io::Error::new(io::ErrorKind::InvalidInput, e))?;
            let connector = TlsConnector::from(config.clone());
            let stream = connector.connect(&address.host, tcp).await?;
            Ok(Stream::Tls(Box::pin(stream)))
        }
    }
}

impl Stream {
    pub fn poll_read(&mut self, cx: &mut Context<'_>, buf: &mut [u8]) -> Poll<io::Result<usize>> {
        match self {
            Self::Plain(s) => s.as_mut().poll_read(cx, buf),
            Self::Tls(s) => s.as_mut().poll_read(cx, buf),
        }
    }

    pub fn poll_write(&mut self, cx: &mut Context<'_>, buf: &[u8]) -> Poll<io::Result<usize>> {
        match self {
            Self::Plain(s) => s.as_mut().poll_write(cx, buf),
            Self::Tls(s) => s.as_mut().poll_write(cx, buf),
        }
    }

    pub fn poll_flush(&mut self, cx: &mut Context<'_>) -> Poll<io::Result<()>> {
        match self {
            Self::Plain(s) => s.as_mut().poll_flush(cx),
            Self::Tls(s) => s.as_mut().poll_flush(cx),
        }
    }
}
