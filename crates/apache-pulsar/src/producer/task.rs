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

//! The task that runs one producer on its loop.
//!
//! It owns the sans-IO [`Producer`] and its layout watch, and does what the producer asks: it looks up and
//! connects each segment, registers its producer there, writes its frames, closes it. It feeds back the outcome,
//! the commands the brokers push for each segment's producer, the layouts and the passing of time, and answers the
//! application's sends, flushes and close. Everything is polled from one future.

use std::collections::HashMap;
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::sync::Arc;
use std::sync::atomic::{AtomicI64, Ordering};
use std::task::{Context, Poll};
use std::time::Instant;

use apache_pulsar_protocol::connection::IdKind;
use apache_pulsar_protocol::frame::Frame;
use apache_pulsar_protocol::producer::{Action, OutgoingMessage, Producer, RegisterError, SendError, SendId};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandCloseProducer, CommandProducerSuccess};
use buffa::{EnumValue, MessageView};
use futures_channel::{mpsc, oneshot};
use futures_util::stream::{FuturesUnordered, SelectAll};
use futures_util::{Stream, StreamExt};

use crate::conn::{ConnHandle, Push};
use crate::error::{Error, ErrorKind, Result, server_error_kind};
use crate::local;
use crate::memory::MemoryLimit;
use crate::message_id::MessageId;
use crate::watch::LayoutWatch;

/// What a producer handle asks of its task.
pub(crate) enum Request {
    Send {
        message: OutgoingMessage,
        publish_time: u64,
        reserved: u64,
        reply: oneshot::Sender<Result<MessageId>>,
    },
    Flush {
        reply: oneshot::Sender<Result<()>>,
    },
    /// `None` when the last handle dropped: close in the background.
    Close {
        reply: Option<oneshot::Sender<Result<()>>>,
    },
}

type Sleep = Pin<Box<dyn Future<Output = ()>>>;
type Registration = Pin<Box<dyn Future<Output = Registered>>>;
type Closing = Pin<Box<dyn Future<Output = (u64, u64)>>>;
type Pushes = Pin<Box<dyn Stream<Item = (u64, u64, Push)>>>;
/// Where a registration in flight registered its producer id, once it did.
type Registering = Rc<std::cell::RefCell<Option<(ConnHandle, u64)>>>;

/// The outcome of registering a segment's producer.
struct Registered {
    segment: u64,
    generation: u64,
    outcome:
        std::result::Result<(ConnHandle, u64, mpsc::UnboundedReceiver<Push>, CommandProducerSuccess), RegisterError>,
}

/// A segment's producer as registered on a connection.
struct Link {
    generation: u64,
    connection: ConnHandle,
    producer_id: u64,
}

pub(crate) struct Task {
    producer: Producer,
    client: local::PulsarClient,
    watch: LayoutWatch,
    requests: mpsc::UnboundedReceiver<Request>,
    requests_open: bool,
    memory: Arc<MemoryLimit>,
    last_sequence: Arc<AtomicI64>,
    links: HashMap<u64, Link>,
    registrations: FuturesUnordered<Registration>,
    /// Registrations in flight, and where they registered, once they did: to close them if the producer goes
    /// before they complete.
    registering: HashMap<(u64, u64), Registering>,
    closes: FuturesUnordered<Closing>,
    pushes: SelectAll<Pushes>,
    replies: HashMap<SendId, (oneshot::Sender<Result<MessageId>>, u64)>,
    /// Flushes: the last send each waits for, the first failure among them, and where to answer.
    flushes: Vec<(SendId, Option<Error>, oneshot::Sender<Result<()>>)>,
    last_send: Option<SendId>,
    created: Option<oneshot::Sender<Result<()>>>,
    close_replies: Vec<oneshot::Sender<Result<()>>>,
    closed: bool,
    timer: Option<(Instant, Sleep)>,
}

impl Task {
    pub(crate) fn new(
        producer: Producer,
        client: local::PulsarClient,
        watch: LayoutWatch,
        requests: mpsc::UnboundedReceiver<Request>,
        memory: Arc<MemoryLimit>,
        last_sequence: Arc<AtomicI64>,
        created: oneshot::Sender<Result<()>>,
    ) -> Self {
        last_sequence.store(producer.last_sequence_id(), Ordering::Release);
        Self {
            producer,
            client,
            watch,
            requests,
            requests_open: true,
            memory,
            last_sequence,
            links: HashMap::new(),
            registrations: FuturesUnordered::new(),
            registering: HashMap::new(),
            closes: FuturesUnordered::new(),
            pushes: SelectAll::new(),
            replies: HashMap::new(),
            flushes: Vec::new(),
            last_send: None,
            created: Some(created),
            close_replies: Vec::new(),
            closed: false,
            timer: None,
        }
    }

    pub(crate) async fn run(mut self) {
        poll_fn(|cx| self.poll(cx)).await;
        // Requests that came in as the producer closed: their room goes back.
        self.requests.close();
        while let Ok(request) = self.requests.try_recv() {
            match request {
                Request::Send { reserved, reply, .. } => {
                    self.memory.release(reserved);
                    let _ = reply.send(Err(Error::new(ErrorKind::AlreadyClosed, "the producer is closed")));
                }
                Request::Flush { reply } | Request::Close { reply: Some(reply) } => {
                    let _ = reply.send(Ok(()));
                }
                Request::Close { reply: None } => {}
            }
        }
        // Registrations that never completed: close them, so a queued wait-for-exclusive producer leaves the
        // broker's line.
        let pending: Vec<(ConnHandle, u64)> =
            self.registering.values().filter_map(|registered| registered.borrow().clone()).collect();
        self.registrations = FuturesUnordered::new();
        for (connection, producer_id) in pending {
            let _ = connection.request(move |request_id| close_command(producer_id, request_id)).await;
            connection.unregister(IdKind::Producer, producer_id);
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            let mut progressed = false;
            progressed |= self.poll_requests(cx);
            progressed |= self.poll_layouts(cx);
            progressed |= self.poll_registrations(cx);
            // Pushes before close answers: a receipt read with the answer to a close came before it on the wire,
            // and must complete its message before the close fails what is left.
            progressed |= self.poll_pushes(cx);
            progressed |= self.poll_closes(cx);
            progressed |= self.poll_timer(cx);
            // An action may start a future (a registration, a close) that must be polled before going to sleep.
            progressed |= self.drain_actions();
            if self.closed && self.closes.is_empty() {
                return Poll::Ready(());
            }
            if !progressed {
                return Poll::Pending;
            }
        }
    }

    fn poll_requests(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while self.requests_open {
            let Poll::Ready(request) = self.requests.poll_next_unpin(cx) else { break };
            progressed = true;
            let now = Instant::now();
            match request {
                Some(Request::Send { message, publish_time, reserved, reply }) => {
                    match self.producer.send(message, publish_time, now) {
                        Ok(send) => {
                            self.last_send = Some(send);
                            self.replies.insert(send, (reply, reserved));
                        }
                        Err(error) => {
                            self.memory.release(reserved);
                            let _ = reply.send(Err(send_error(error)));
                        }
                    }
                }
                Some(Request::Flush { reply }) => match self.last_send {
                    Some(last) if self.producer.oldest_outstanding().is_some_and(|oldest| oldest <= last) => {
                        self.flushes.push((last, None, reply));
                    }
                    _ => {
                        let _ = reply.send(Ok(()));
                    }
                },
                Some(Request::Close { reply }) => {
                    if self.closed {
                        if let Some(reply) = reply {
                            let _ = reply.send(Ok(()));
                        }
                    } else {
                        self.close_replies.extend(reply);
                        self.producer.close(now);
                    }
                }
                // Every handle is gone: close in the background.
                None => {
                    self.requests_open = false;
                    self.producer.close(now);
                }
            }
        }
        progressed
    }

    fn poll_layouts(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some(layout)) = self.watch.updates.poll_next_unpin(cx) {
            progressed = true;
            self.producer.on_layout(layout, Instant::now());
        }
        progressed
    }

    fn poll_registrations(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some(registered)) = self.registrations.poll_next_unpin(cx) {
            progressed = true;
            let Registered { segment, generation, outcome } = registered;
            self.registering.remove(&(segment, generation));
            let now = Instant::now();
            match outcome {
                Ok((connection, producer_id, pushes, success)) => {
                    let max_message_size = connection.broker().max_message_size.map_or(5 << 20, |size| size as usize);
                    self.pushes.push(Box::pin(pushes.map(move |push| (segment, generation, push))));
                    self.links.insert(segment, Link { generation, connection, producer_id });
                    self.producer.on_registered(segment, generation, producer_id, &success, max_message_size, now);
                    self.last_sequence.store(self.producer.last_sequence_id(), Ordering::Release);
                }
                Err(error) => self.producer.on_register_failed(segment, generation, error, now),
            }
        }
        progressed
    }

    fn poll_closes(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some((segment, generation))) = self.closes.poll_next_unpin(cx) {
            progressed = true;
            self.producer.on_producer_closed(segment, generation, Instant::now());
        }
        progressed
    }

    fn poll_pushes(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some((segment, generation, push))) = self.pushes.poll_next_unpin(cx) {
            progressed = true;
            let now = Instant::now();
            let current = self.links.get(&segment).is_some_and(|link| link.generation == generation);
            match push {
                Push::Command(frame) => self.on_push(segment, generation, current, &frame, now),
                Push::Closed(_) => {
                    if current {
                        self.links.remove(&segment);
                    }
                    self.producer.on_disconnected(segment, generation, now);
                }
            }
        }
        progressed
    }

    fn on_push(&mut self, segment: u64, generation: u64, current: bool, frame: &Frame, now: Instant) {
        let Ok(command) = frame.command_view() else { return };
        let Ok(command) = command.to_owned_message() else { return };
        let EnumValue::Known(kind) = command.r#type else { return };
        match kind {
            Type::SEND_RECEIPT => {
                self.producer.on_receipt(segment, generation, &command.send_receipt, now);
                self.last_sequence.store(self.producer.last_sequence_id(), Ordering::Release);
            }
            Type::SEND_ERROR => self.producer.on_send_error(segment, generation, &command.send_error, now),
            // The broker closed the producer (an unload, a terminated topic) or moved the topic.
            Type::CLOSE_PRODUCER | Type::TOPIC_MIGRATED => {
                if current && let Some(link) = self.links.remove(&segment) {
                    link.connection.unregister(IdKind::Producer, link.producer_id);
                }
                self.producer.on_disconnected(segment, generation, now);
            }
            _ => {}
        }
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(deadline) = self.producer.poll_timeout() else {
            self.timer = None;
            return false;
        };
        let now = Instant::now();
        if deadline <= now {
            self.timer = None;
            self.producer.handle_timeout(now);
            return true;
        }
        if self.timer.as_ref().is_none_or(|(at, _)| *at != deadline) {
            self.timer = Some((deadline, Box::pin(compio::time::sleep_until(deadline))));
        }
        let (_, sleep) = self.timer.as_mut().unwrap();
        if sleep.as_mut().poll(cx).is_ready() {
            self.timer = None;
            self.producer.handle_timeout(Instant::now());
            return true;
        }
        false
    }

    fn drain_actions(&mut self) -> bool {
        let mut progressed = false;
        while let Some(action) = self.producer.poll_action() {
            progressed = true;
            match action {
                Action::Connect { segment, generation, topic } => self.connect(segment, generation, topic),
                Action::Send { segment, command, metadata, payload } => {
                    if let Some(link) = self.links.get(&segment) {
                        // A connection that just went is noticed from its pushes.
                        let _ = link.connection.send_payload(command, metadata, payload);
                    }
                }
                Action::CloseProducer { segment, generation } => self.close_producer(segment, generation),
                Action::Disconnect { segment, generation } => {
                    if let Some(link) = self.links.get(&segment).filter(|l| l.generation == generation) {
                        tracing::warn!(segment, "a receipt did not match the producer's queue: reconnecting");
                        let connection = link.connection.clone();
                        // The connection task ends the connection; its owners hear about it from their pushes.
                        let close = Box::pin(async move {
                            connection.close().await;
                            (u64::MAX, u64::MAX)
                        });
                        self.closes.push(close);
                    }
                }
                Action::Done { send, result } => self.done(send, result),
                Action::Created(result) => {
                    if let Some(created) = self.created.take() {
                        let _ = created.send(result.map_err(send_error));
                    }
                }
                Action::Closed => {
                    self.closed = true;
                    for reply in self.close_replies.drain(..) {
                        let _ = reply.send(Ok(()));
                    }
                }
            }
        }
        progressed
    }

    fn connect(&mut self, segment: u64, generation: u64, topic: String) {
        let Some(command) = self.producer.register_command(segment, 0) else { return };
        let client = self.client.clone();
        let registered = Rc::new(std::cell::RefCell::new(None));
        self.registering.insert((segment, generation), registered.clone());
        self.registrations.push(Box::pin(async move {
            let outcome = register(&client, &topic, command, &registered).await;
            Registered { segment, generation, outcome }
        }));
    }

    fn close_producer(&mut self, segment: u64, generation: u64) {
        let Some(link) = self.links.get(&segment).filter(|l| l.generation == generation) else {
            // Its connection is gone, and the producer with it.
            self.producer.on_producer_closed(segment, generation, Instant::now());
            return;
        };
        let connection = link.connection.clone();
        let producer_id = link.producer_id;
        self.closes.push(Box::pin(async move {
            // Its receipts keep coming until the answer: the broker answers once its pending writes are done.
            let _ = connection.request(move |request_id| close_command(producer_id, request_id)).await;
            connection.unregister(IdKind::Producer, producer_id);
            (segment, generation)
        }));
    }

    fn done(
        &mut self,
        send: SendId,
        result: std::result::Result<apache_pulsar_protocol::message_id::MessageId, SendError>,
    ) {
        let result = result.map(MessageId).map_err(send_error);
        if let Some((reply, reserved)) = self.replies.remove(&send) {
            // The room goes back first, so a send chained on this one finds it.
            self.memory.release(reserved);
            let _ = reply.send(result.clone());
        }
        // A flush fails if a send it waits for failed, as Java's does.
        if let Err(error) = &result {
            for (last, failure, _) in &mut self.flushes {
                if send <= *last {
                    failure.get_or_insert_with(|| error.clone());
                }
            }
        }
        let oldest = self.producer.oldest_outstanding();
        let (finished, waiting): (Vec<_>, Vec<_>) = std::mem::take(&mut self.flushes)
            .into_iter()
            .partition(|(last, ..)| oldest.is_none_or(|oldest| oldest > *last));
        self.flushes = waiting;
        for (_, failure, reply) in finished {
            let _ = reply.send(failure.map_or(Ok(()), Err));
        }
    }
}

/// Looks up a segment's topic, connects, and registers a producer for it.
async fn register(
    client: &local::PulsarClient,
    topic: &str,
    mut command: BaseCommand,
    registered: &std::cell::RefCell<Option<(ConnHandle, u64)>>,
) -> std::result::Result<(ConnHandle, u64, mpsc::UnboundedReceiver<Push>, CommandProducerSuccess), RegisterError> {
    let unavailable = |error: Error| RegisterError::Unavailable(error.to_string());
    let address = client.lookup(topic).await.map_err(|error| refused(&error).unwrap_or_else(|| unavailable(error)))?;
    let connection = client.connection(&address).await.map_err(unavailable)?;
    let (producer_id, pushes) = connection.register(IdKind::Producer).await.map_err(unavailable)?;
    command.producer.get_or_insert_default().producer_id = producer_id;
    *registered.borrow_mut() = Some((connection.clone(), producer_id));
    let response = connection
        .request(move |request_id| {
            let mut command = command;
            command.producer.get_or_insert_default().request_id = request_id;
            command
        })
        .await;
    let frame = match response {
        Ok(frame) => frame,
        Err(error) => {
            connection.unregister(IdKind::Producer, producer_id);
            return Err(refused(&error).unwrap_or_else(|| unavailable(error)));
        }
    };
    let success = frame
        .command_view()
        .ok()
        .and_then(|c| c.producer_success.as_option().and_then(|s| s.to_owned_message().ok()))
        .ok_or_else(|| RegisterError::Unavailable("not a producer success".into()))?;
    Ok((connection, producer_id, pushes, success))
}

/// The broker's refusal in an error, if it is one.
fn refused(error: &Error) -> Option<RegisterError> {
    error.server_error().map(|(error, message)| RegisterError::Server { error, message })
}

fn close_command(producer_id: u64, request_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::CLOSE_PRODUCER.into(),
        close_producer: CommandCloseProducer { producer_id, request_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

/// The public error for a send's failure.
pub(crate) fn send_error(error: SendError) -> Error {
    let kind = match &error {
        SendError::AlreadyClosed => ErrorKind::AlreadyClosed,
        SendError::Timeout | SendError::Unavailable(_) => ErrorKind::Timeout,
        SendError::Server { error, .. } => server_error_kind(EnumValue::Known(*error)),
        SendError::Routing(_) | SendError::TooLarge { .. } => ErrorKind::Other,
    };
    Error::with_source(kind, error.to_string(), error)
}
