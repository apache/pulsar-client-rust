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

//! The task that owns one connection (docs/3-protocol.md §4).
//!
//! It drives the sans-IO [`Connection`] from a single future that polls, in turn, the requests handed to it,
//! the authentication provider, the socket's read and write sides and the connection's next deadline. One owner
//! and one future: no lock, and no read or write dropped in flight.

use std::cell::Cell;
use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Instant;

use apache_pulsar_protocol::connection::{BrokerInfo, ConnectOptions, Connection, ConnectionError, Event, State};
use apache_pulsar_protocol::frame::Frame;
use apache_pulsar_protocol::lookup::BrokerAddress;
use apache_pulsar_protocol::proto::BaseCommand;
use bytes::Bytes;
use compio::runtime::JoinHandle;
use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;

use crate::auth::AuthFuture;
use crate::config::ClientConfig;
use crate::error::{Error, ErrorKind, Result};
use crate::net::{self, Stream};

type Build = Box<dyn FnOnce(u64) -> BaseCommand>;
type Sleep = Pin<Box<dyn Future<Output = ()>>>;

enum Command {
    Request { build: Build, reply: oneshot::Sender<Result<Frame>> },
    Close,
}

/// A handle to a connection task. Cheap to clone; the task stops when the last handle is dropped.
#[derive(Clone)]
pub struct ConnHandle {
    inner: Rc<Inner>,
}

struct Inner {
    address: BrokerAddress,
    broker: BrokerInfo,
    commands: mpsc::UnboundedSender<Command>,
    closed: Rc<Cell<bool>>,
    task: Cell<Option<JoinHandle<()>>>,
}

impl std::fmt::Debug for ConnHandle {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("ConnHandle").field("address", &self.inner.address).field("closed", &self.is_closed()).finish()
    }
}

impl ConnHandle {
    pub fn address(&self) -> &BrokerAddress {
        &self.inner.address
    }

    pub fn broker(&self) -> &BrokerInfo {
        &self.inner.broker
    }

    pub fn is_closed(&self) -> bool {
        self.inner.closed.get()
    }

    /// Sends a request built from its request id, and waits for the response.
    pub async fn request(&self, build: impl FnOnce(u64) -> BaseCommand + 'static) -> Result<Frame> {
        let (reply, response) = oneshot::channel();
        self.inner
            .commands
            .unbounded_send(Command::Request { build: Box::new(build), reply })
            .map_err(|_| not_connected())?;
        response.await.map_err(|_| not_connected())?
    }

    /// Closes the connection and waits for its task to end.
    pub async fn close(&self) {
        let _ = self.inner.commands.unbounded_send(Command::Close);
        if let Some(task) = self.inner.task.take() {
            let _ = task.await;
        }
    }
}

fn not_connected() -> Error {
    Error::new(ErrorKind::NotConnected, "the connection closed")
}

/// Connects to `address` and completes the handshake within the connect timeout.
pub(crate) async fn open(address: BrokerAddress, config: &ClientConfig) -> Result<ConnHandle> {
    let started = Instant::now();
    let timeout = config.connect_timeout;
    let stream = compio::time::timeout(timeout, net::connect(&address.physical, config.tls.as_ref()))
        .await
        .map_err(|_| Error::new(ErrorKind::Timeout, format!("connecting to {} timed out", address.physical)))?
        .map_err(|e| Error::with_source(ErrorKind::Connect, format!("can't connect to {}", address.physical), e))?;

    // The provider may fetch a token: it counts against the connect timeout too.
    let (auth_method, auth_data) = match &config.authentication {
        Some(auth) => {
            let data = compio::time::timeout(timeout.saturating_sub(started.elapsed()), auth.auth_data())
                .await
                .map_err(|_| Error::new(ErrorKind::Timeout, "the authentication provider did not answer in time"))??;
            (Some(auth.method_name().to_string()), Some(data))
        }
        None => (None, None),
    };
    let options = ConnectOptions {
        auth_method,
        auth_data,
        proxy_to_broker_url: address.through_proxy().then(|| address.logical.to_string()),
        handshake_timeout: timeout.saturating_sub(started.elapsed()),
        operation_timeout: config.operation_timeout,
        keepalive_interval: config.keepalive_interval,
        ..Default::default()
    };
    let connection = Connection::new(options, Instant::now());
    let (commands, inbox) = mpsc::unbounded();
    let (ready, connected) = oneshot::channel();
    let closed = Rc::new(Cell::new(false));
    let driver = Driver {
        stream,
        connection,
        inbox,
        waiters: HashMap::new(),
        outgoing: None,
        read_buf: vec![0; 64 * 1024],
        timer: None,
        auth: None,
        auth_provider: config.authentication.clone(),
        ready: Some(ready),
        closed: closed.clone(),
        address: address.clone(),
    };
    let task = compio::runtime::spawn(driver.run());
    let broker = connected.await.map_err(|_| not_connected())??;
    tracing::debug!(address = %address.physical, logical = %address.logical, server = %broker.server_version, "connected");
    Ok(ConnHandle { inner: Rc::new(Inner { address, broker, commands, closed, task: Cell::new(Some(task)) }) })
}

struct Driver {
    stream: Stream,
    connection: Connection,
    inbox: mpsc::UnboundedReceiver<Command>,
    waiters: HashMap<u64, oneshot::Sender<Result<Frame>>>,
    /// Bytes taken from the connection but not all written yet.
    outgoing: Option<Bytes>,
    read_buf: Vec<u8>,
    timer: Option<(Instant, Sleep)>,
    auth: Option<AuthFuture>,
    auth_provider: Option<std::sync::Arc<dyn crate::auth::Authentication>>,
    ready: Option<oneshot::Sender<Result<BrokerInfo>>>,
    closed: Rc<Cell<bool>>,
    address: BrokerAddress,
}

impl Driver {
    async fn run(mut self) {
        poll_fn(|cx| self.poll(cx)).await;
        self.closed.set(true);
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            let mut progressed = false;
            progressed |= self.poll_inbox(cx);
            progressed |= self.poll_auth(cx);
            progressed |= self.poll_read(cx);
            self.drain_events();
            progressed |= self.poll_write(cx);
            progressed |= self.poll_timer(cx);
            // Once closed, nothing more is written: what is left would only reach a broker that is gone, or one
            // we are leaving.
            if self.connection.state() == State::Closed {
                self.drain_events();
                return Poll::Ready(());
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }

    fn poll_inbox(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(command) = self.inbox.poll_next_unpin(cx) {
            progressed = true;
            match command {
                Some(Command::Request { build, reply }) => match self.connection.request(Instant::now(), build) {
                    Ok(request_id) => {
                        self.waiters.insert(request_id, reply);
                    }
                    Err(error) => {
                        let _ = reply.send(Err(error.into()));
                    }
                },
                // Closing locally; or every handle is gone.
                Some(Command::Close) | None => {
                    self.connection.close(ConnectionError::Closed);
                    return true;
                }
            }
        }
        progressed
    }

    fn poll_auth(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(auth) = &mut self.auth else { return false };
        let Poll::Ready(result) = auth.as_mut().poll(cx) else { return false };
        self.auth = None;
        match result {
            Ok(data) => self.connection.auth_response(data),
            Err(error) => self.connection.auth_failed(error.to_string()),
        }
        true
    }

    fn poll_read(&mut self, cx: &mut Context<'_>) -> bool {
        if self.connection.state() == State::Closed {
            return false;
        }
        match self.stream.poll_read(cx, &mut self.read_buf) {
            Poll::Pending => false,
            Poll::Ready(Ok(0)) => {
                self.connection.close(ConnectionError::Disconnected("the broker closed the connection".into()));
                true
            }
            Poll::Ready(Ok(n)) => {
                self.connection.handle_input(&self.read_buf[..n], Instant::now());
                true
            }
            Poll::Ready(Err(error)) => {
                self.connection.close(ConnectionError::Disconnected(error.to_string()));
                true
            }
        }
    }

    fn poll_write(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        loop {
            if self.outgoing.is_none() {
                self.outgoing = self.connection.poll_transmit();
            }
            let Some(bytes) = &mut self.outgoing else { break };
            match self.stream.poll_write(cx, bytes) {
                Poll::Pending => return progressed,
                Poll::Ready(Ok(0)) | Poll::Ready(Err(_)) => {
                    self.outgoing = None;
                    self.connection.close(ConnectionError::Disconnected("write failed".into()));
                    return true;
                }
                Poll::Ready(Ok(n)) => {
                    progressed = true;
                    let _ = bytes.split_to(n);
                    if bytes.is_empty() {
                        self.outgoing = None;
                    }
                }
            }
        }
        // compio's adapters buffer writes: push them to the socket.
        match self.stream.poll_flush(cx) {
            Poll::Ready(Err(_)) => {
                self.connection.close(ConnectionError::Disconnected("flush failed".into()));
                true
            }
            _ => progressed,
        }
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(deadline) = self.connection.poll_timeout() else {
            self.timer = None;
            return false;
        };
        let now = Instant::now();
        if deadline <= now {
            self.timer = None;
            self.connection.handle_timeout(now);
            return true;
        }
        if self.timer.as_ref().is_none_or(|(at, _)| *at != deadline) {
            self.timer = Some((deadline, Box::pin(compio::time::sleep_until(deadline))));
        }
        let (_, sleep) = self.timer.as_mut().unwrap();
        if sleep.as_mut().poll(cx).is_ready() {
            self.timer = None;
            self.connection.handle_timeout(Instant::now());
            return true;
        }
        false
    }

    fn drain_events(&mut self) {
        while let Some(event) = self.connection.poll_event() {
            match event {
                Event::Connected(info) => {
                    if let Some(ready) = self.ready.take() {
                        let _ = ready.send(Ok(info));
                    }
                }
                Event::Response { request_id, result } => {
                    if let Some(waiter) = self.waiters.remove(&request_id) {
                        let _ = waiter.send(result.map_err(Error::from));
                    }
                }
                Event::Command(frame) => {
                    // Producers, consumers and watches register for these from milestone 2 on.
                    tracing::trace!(address = %self.address.physical, len = frame.command.len(), "pushed command");
                }
                Event::AuthChallenge { refresh, challenge } => match &self.auth_provider {
                    Some(provider) => {
                        self.auth = Some(if refresh { provider.auth_data() } else { provider.challenge(&challenge) });
                    }
                    None => self.connection.auth_failed("the broker asked for credentials, and none are set".into()),
                },
                Event::Closed(error) => {
                    tracing::debug!(address = %self.address.physical, %error, "connection closed");
                    self.closed.set(true);
                    let error = Error::from(error);
                    if let Some(ready) = self.ready.take() {
                        let _ = ready.send(Err(error.clone()));
                    }
                    for (_, waiter) in self.waiters.drain() {
                        let _ = waiter.send(Err(error.clone()));
                    }
                    self.inbox.close();
                }
            }
        }
    }
}
