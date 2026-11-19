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

//! Check 1: a TLS handshake and an echo round trip through `compio-tls` and rustls, against a TLS
//! server on another loop thread with a self-signed certificate generated at run time.

use std::{
    sync::{Arc, mpsc},
    thread,
    time::{Duration, Instant},
};

use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    rustls::{
        self, ClientConfig, RootCertStore, ServerConfig,
        pki_types::{CertificateDer, PrivateKeyDer, PrivatePkcs8KeyDer},
    },
    tls::{TlsAcceptor, TlsConnector},
};

use crate::{CheckResult, ensure, note, rt};

const ALPN: &[u8] = b"pulsar-check";
const CHUNK: usize = 16 * 1024;
const ROUNDS: usize = 64;

pub fn run(cfg: &rt::Config) -> CheckResult {
    let provider = Arc::new(rustls::crypto::ring::default_provider());

    let certified =
        rcgen::generate_simple_self_signed(vec!["localhost".to_string()]).map_err(|e| format!("rcgen: {e}"))?;
    let cert_der: CertificateDer<'static> = certified.cert.der().clone();
    let key_der = PrivateKeyDer::Pkcs8(PrivatePkcs8KeyDer::from(certified.signing_key.serialize_der()));

    let mut server_config = ServerConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_no_client_auth()
        .with_single_cert(vec![cert_der.clone()], key_der)
        .map_err(|e| format!("server config: {e}"))?;
    server_config.alpn_protocols = vec![ALPN.to_vec()];
    let acceptor = TlsAcceptor::from(Arc::new(server_config));

    let mut roots = RootCertStore::empty();
    roots.add(cert_der).map_err(|e| format!("root store: {e}"))?;
    let mut client_config = ClientConfig::builder_with_provider(provider.clone())
        .with_safe_default_protocol_versions()
        .map_err(|e| e.to_string())?
        .with_root_certificates(roots)
        .with_no_client_auth();
    client_config.alpn_protocols = vec![ALPN.to_vec()];
    let connector = TlsConnector::from(Arc::new(client_config));

    // A client that trusts nothing, to see that verification really happens.
    let untrusting = TlsConnector::from(Arc::new(
        ClientConfig::builder_with_provider(provider)
            .with_safe_default_protocol_versions()
            .map_err(|e| e.to_string())?
            .with_root_certificates(RootCertStore::empty())
            .with_no_client_auth(),
    ));

    // Server loop on its own thread. It accepts two connections: the untrusting client, then the
    // echo client.
    let (addr_tx, addr_rx) = mpsc::channel();
    let server_cfg = cfg.clone();
    let server = thread::Builder::new()
        .name("tls-server-loop".into())
        .spawn(move || -> Result<u64, String> {
            let rt = rt::build(&server_cfg, "tls server")?;
            rt.block_on(async move {
                let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| format!("bind: {e}"))?;
                addr_tx.send(listener.local_addr().map_err(|e| e.to_string())?).map_err(|e| e.to_string())?;

                // The untrusting client aborts the handshake; the server sees an error.
                let (s, _) = listener.accept().await.map_err(|e| e.to_string())?;
                let _ = acceptor.accept(s).await;

                let (s, _) = listener.accept().await.map_err(|e| e.to_string())?;
                let mut tls = acceptor.accept(s).await.map_err(|e| format!("server handshake: {e}"))?;
                let mut echoed = 0u64;
                let mut buf = Vec::with_capacity(CHUNK);
                loop {
                    buf.clear();
                    let BufResult(res, b) = tls.read(buf).await;
                    buf = b;
                    let n = res.map_err(|e| format!("server read: {e}"))?;
                    if n == 0 {
                        break;
                    }
                    let BufResult(res, b) = tls.write_all(buf).await;
                    buf = b;
                    res.map_err(|e| format!("server write: {e}"))?;
                    tls.flush().await.map_err(|e| format!("server flush: {e}"))?;
                    echoed += n as u64;
                }
                let _ = tls.shutdown().await;
                Ok(echoed)
            })
        })
        .map_err(|e| e.to_string())?;

    let addr = match addr_rx.recv_timeout(Duration::from_secs(10)) {
        Ok(addr) => addr,
        Err(mpsc::RecvTimeoutError::Timeout) => return Err("server did not start".into()),
        Err(mpsc::RecvTimeoutError::Disconnected) => {
            return Err(match server.join() {
                Ok(Err(e)) => e,
                _ => "server did not start".to_string(),
            });
        }
    };

    let rt = rt::build(cfg, "tls client")?;
    let client_result: Result<(), String> = rt.block_on(async {
        let s = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        match untrusting.connect("localhost", s).await {
            Ok(_) => return Err("handshake succeeded without a trusted root".into()),
            Err(e) => note(format!("untrusting client rejected the cert as expected: {e}")),
        }

        let start = Instant::now();
        let s = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
        let mut tls = connector.connect("localhost", s).await.map_err(|e| format!("client handshake: {e}"))?;
        let handshake = start.elapsed();
        let alpn = tls.negotiated_alpn().map(|a| a.into_owned());
        ensure!(alpn.as_deref() == Some(ALPN), "ALPN not negotiated: {alpn:?}");
        note(format!("handshake over 127.0.0.1 in {handshake:?}, ALPN negotiated"));

        let start = Instant::now();
        let mut total = 0usize;
        for round in 0..ROUNDS {
            let payload: Vec<u8> = (0..CHUNK).map(|i| ((i + round * 7) % 251) as u8).collect();
            let BufResult(res, payload) = tls.write_all(payload).await;
            res.map_err(|e| format!("client write: {e}"))?;
            tls.flush().await.map_err(|e| format!("client flush: {e}"))?;
            let BufResult(res, back) = tls.read_exact(Vec::with_capacity(CHUNK)).await;
            res.map_err(|e| format!("client read: {e}"))?;
            ensure!(back == payload, "echo mismatch in round {round}");
            total += CHUNK;
        }
        tls.shutdown().await.map_err(|e| format!("client shutdown: {e}"))?;
        note(format!("echoed {} KiB in {ROUNDS} round trips in {:?}", total / 1024, start.elapsed()));
        Ok(())
    });

    let server_result = server.join().map_err(|_| "server thread panicked".to_string())?;
    client_result?;
    let echoed = server_result?;
    ensure!(echoed == (CHUNK * ROUNDS) as u64, "server echoed {echoed} bytes, expected {}", CHUNK * ROUNDS);
    Ok(())
}
