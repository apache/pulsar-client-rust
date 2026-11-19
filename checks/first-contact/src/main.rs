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

//! Milestone 0 "first contact": the CONNECT handshake against a real broker, over compio.
//!
//! ```text
//! cargo run -p first-contact -- [host:port]        default 127.0.0.1:6650
//! ```
//!
//! Exits with 0 if the broker answered `CONNECTED` and advertises `supports_scalable_topics`, 1 otherwise.

use std::process::ExitCode;
use std::time::Duration;

use apache_pulsar_protocol::connection::{ConnectOptions, Connection, Event};
use compio::BufResult;
use compio::io::{AsyncRead, AsyncWriteExt};
use compio::net::TcpStream;

#[compio::main]
async fn main() -> ExitCode {
    let address = std::env::args().nth(1).unwrap_or_else(|| "127.0.0.1:6650".to_string());
    match compio::time::timeout(Duration::from_secs(10), handshake(&address)).await {
        Ok(Ok(())) => ExitCode::SUCCESS,
        Ok(Err(e)) => {
            eprintln!("first contact with {address} failed: {e}");
            ExitCode::FAILURE
        }
        Err(_) => {
            eprintln!("first contact with {address} timed out");
            ExitCode::FAILURE
        }
    }
}

async fn handshake(address: &str) -> Result<(), String> {
    let mut stream = TcpStream::connect(address).await.map_err(|e| format!("connect: {e}"))?;
    let mut connection = Connection::new(&ConnectOptions::default());
    let mut buf = Vec::with_capacity(64 * 1024);
    loop {
        while let Some(bytes) = connection.poll_transmit() {
            let BufResult(result, _) = stream.write_all(bytes.to_vec()).await;
            result.map_err(|e| format!("write: {e}"))?;
        }
        if let Some(event) = connection.poll_event() {
            return match event {
                Event::Connected(info) => {
                    println!("connected to {address}");
                    println!("  server version:   {}", info.server_version);
                    println!("  protocol version: {}", info.protocol_version);
                    println!("  max message size: {:?}", info.max_message_size);
                    println!("  features:         {:?}", info.features);
                    if info.features.scalable_topics {
                        Ok(())
                    } else {
                        Err("the broker does not advertise supports_scalable_topics".into())
                    }
                }
                Event::Closed(error) => Err(error.to_string()),
            };
        }
        buf.clear();
        let BufResult(result, returned) = stream.read(buf).await;
        buf = returned;
        match result.map_err(|e| format!("read: {e}"))? {
            0 => return Err("the broker closed the connection".into()),
            _ => connection.handle_input(&buf),
        }
    }
}
