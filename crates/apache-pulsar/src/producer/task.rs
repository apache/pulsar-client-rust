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

use std::cell::RefCell;
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
use apache_pulsar_protocol::proto::{
    BaseCommand, CommandCloseProducer, CommandProducerSuccess, MessageMetadata, ServerError,
};
use buffa::{EnumValue, MessageView};
use bytes::Bytes;
use futures_channel::{mpsc, oneshot};
use futures_util::stream::{FuturesUnordered, SelectAll};
use futures_util::{Stream, StreamExt};

use crate::conn::{ConnHandle, Push};
use crate::error::{Error, ErrorKind, Result, server_error_kind};
use crate::local;
use crate::memory::Reservation;
use crate::message_id::MessageId;
use crate::watch::LayoutWatch;

/// What a producer handle asks of its task.
pub(crate) enum Request {
    Send {
        message: OutgoingMessage,
        publish_time: u64,
        /// The message's room under the memory limit. It goes back when the message completes, or when the
        /// request drops untaken.
        room: Reservation,
        /// Whether its batch goes out at once, as for a blocking send.
        flush: bool,
        reply: oneshot::Sender<Result<MessageId>>,
    },
    Flush {
        reply: oneshot::Sender<Result<()>>,
    },
    /// `None` when the last handle dropped, or the client is closing: close in the background.
    Close {
        reply: Option<oneshot::Sender<Result<()>>>,
    },
}

type Sleep = Pin<Box<dyn Future<Output = ()>>>;
type Registration = Pin<Box<dyn Future<Output = Registered>>>;
type Closing = Pin<Box<dyn Future<Output = (u64, u64)>>>;
type Pushes = Pin<Box<dyn Stream<Item = (u64, u64, Push)>>>;
/// Where a registration in flight registered its producer id, once it did.
type Registering = Rc<RefCell<Option<(ConnHandle, u64)>>>;
/// A registered producer: its link, the commands pushed for it, and the broker's answer.
type Linked = (Link, mpsc::UnboundedReceiver<Push>, CommandProducerSuccess);

/// The outcome of registering a segment's producer.
struct Registered {
    segment: u64,
    generation: u64,
    outcome: std::result::Result<Linked, RegisterError>,
}

/// A segment's producer as registered on a connection. Dropping it unregisters the producer id there.
struct Link {
    generation: u64,
    connection: ConnHandle,
    producer_id: u64,
}

impl Drop for Link {
    fn drop(&mut self) {
        self.connection.unregister(IdKind::Producer, self.producer_id);
    }
}

pub(crate) struct Task {
    producer: Producer,
    client: local::PulsarClient,
    watch: LayoutWatch,
    /// The client keeps a sender of its own, to close the producer: the channel doesn't end while the task runs.
    requests: mpsc::UnboundedReceiver<Request>,
    last_sequence: Arc<AtomicI64>,
    links: HashMap<u64, Link>,
    registrations: FuturesUnordered<Registration>,
    /// Registrations in flight, and where they registered, once they did: to close them if the producer goes
    /// before they complete.
    registering: HashMap<(u64, u64), Registering>,
    closes: FuturesUnordered<Closing>,
    pushes: SelectAll<Pushes>,
    /// Sends in flight: where to answer each, and the room it holds.
    replies: HashMap<SendId, (oneshot::Sender<Result<MessageId>>, Reservation)>,
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
        last_sequence: Arc<AtomicI64>,
        created: oneshot::Sender<Result<()>>,
    ) -> Self {
        last_sequence.store(producer.last_sequence_id(), Ordering::Release);
        Self {
            producer,
            client,
            watch,
            requests,
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
        // Requests that came in as the producer closed fail as they drop: their senders see no answer coming, and
        // their room goes back.
        self.requests.close();
        while self.requests.try_recv().is_ok() {}
        // Registrations that never completed: close them, so a queued wait-for-exclusive producer leaves the
        // broker's line. Dropping them unregisters their producer ids.
        let pending: Vec<(ConnHandle, u64)> =
            self.registering.values().filter_map(|registered| registered.borrow().clone()).collect();
        self.registrations = FuturesUnordered::new();
        for (connection, producer_id) in pending {
            let _ = connection.request(move |request_id| close_command(producer_id, request_id)).await;
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
        while let Poll::Ready(Some(request)) = self.requests.poll_next_unpin(cx) {
            progressed = true;
            let now = Instant::now();
            match request {
                Request::Send { message, publish_time, room, flush, reply } => {
                    match self.producer.send(message, publish_time, now) {
                        Ok(send) => {
                            self.last_send = Some(send);
                            self.replies.insert(send, (reply, room));
                            if flush {
                                self.producer.flush();
                            }
                        }
                        // Its room goes back as it drops.
                        Err(error) => {
                            let _ = reply.send(Err(send_error(error)));
                        }
                    }
                }
                Request::Flush { reply } => {
                    self.producer.flush();
                    match self.last_send {
                        Some(last) if self.producer.oldest_outstanding().is_some_and(|oldest| oldest <= last) => {
                            self.flushes.push((last, None, reply));
                        }
                        _ => {
                            let _ = reply.send(Ok(()));
                        }
                    }
                }
                // The task ends once closed, so a close comes before that; closing again is a no-op.
                Request::Close { reply } => {
                    self.close_replies.extend(reply);
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
                Ok((link, pushes, success)) => {
                    let producer_id = link.producer_id;
                    let max_message_size =
                        link.connection.broker().max_message_size.map_or(5 << 20, |size| size as usize);
                    self.pushes.push(Box::pin(pushes.map(move |push| (segment, generation, push))));
                    self.links.insert(segment, link);
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
            match push {
                Push::Command(frame) => self.on_push(segment, generation, &frame),
                // The connection closed, and the producer with it.
                Push::Closed(_) => self.unlink(segment, generation),
            }
        }
        progressed
    }

    fn on_push(&mut self, segment: u64, generation: u64, frame: &Frame) {
        let Ok(command) = frame.command_view() else { return };
        let Ok(command) = command.to_owned_message() else { return };
        let now = Instant::now();
        match command.r#type {
            EnumValue::Known(Type::SEND_RECEIPT) => {
                self.producer.on_receipt(segment, generation, &command.send_receipt, now);
                self.last_sequence.store(self.producer.last_sequence_id(), Ordering::Release);
            }
            EnumValue::Known(Type::SEND_ERROR) => {
                self.producer.on_send_error(segment, generation, &command.send_error, now);
            }
            // The broker closed the producer (an unload, a terminated topic) or moved the topic: the only other
            // commands it pushes to a producer.
            _ => self.unlink(segment, generation),
        }
    }

    /// The segment's producer of `generation` is gone from its connection: forget its link, if it is still the
    /// current one, and tell the producer.
    fn unlink(&mut self, segment: u64, generation: u64) {
        self.links.retain(|&s, link| (s, link.generation) != (segment, generation));
        self.producer.on_disconnected(segment, generation, Instant::now());
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let Some(deadline) = self.producer.poll_timeout() else {
            self.timer = None;
            return false;
        };
        // A sleep only completes once its deadline has passed: either way, the deadline is reached.
        if deadline > Instant::now() && self.poll_sleep(deadline, cx).is_pending() {
            return false;
        }
        self.timer = None;
        self.producer.handle_timeout(Instant::now());
        true
    }

    /// Polls the sleep until `deadline`, starting a new one when the deadline changed.
    fn poll_sleep(&mut self, deadline: Instant, cx: &mut Context<'_>) -> Poll<()> {
        if self.timer.as_ref().is_none_or(|(at, _)| *at != deadline) {
            self.timer = Some((deadline, Box::pin(compio::time::sleep_until(deadline))));
        }
        let (_, sleep) = self.timer.as_mut().unwrap();
        sleep.as_mut().poll(cx)
    }

    fn drain_actions(&mut self) -> bool {
        let mut progressed = false;
        while let Some(action) = self.producer.poll_action() {
            progressed = true;
            match action {
                Action::Connect { segment, generation, topic } => self.connect(segment, generation, topic),
                Action::Send { segment, command, metadata, payload } => self.write(segment, command, metadata, payload),
                Action::CloseProducer { segment, generation } => self.close_producer(segment, generation),
                Action::Disconnect { segment, generation } => self.disconnect(segment, generation),
                Action::Done { send, result } => self.done(send, result),
                Action::Created(result) => {
                    let created = self.created.take().expect("a producer is created once");
                    let _ = created.send(result.map_err(send_error));
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
        let registered = Rc::new(RefCell::new(None));
        self.registering.insert((segment, generation), registered.clone());
        self.registrations.push(Box::pin(async move {
            let outcome = register(&client, &topic, generation, command, &registered).await;
            Registered { segment, generation, outcome }
        }));
    }

    /// Writes a message on its segment's connection. Without a link, the connection just went: the producer
    /// heard it from the pushes, and writes the message again once registered anew. A connection that is going
    /// is noticed from the pushes too.
    fn write(&self, segment: u64, command: Box<BaseCommand>, metadata: Box<MessageMetadata>, payload: Bytes) {
        let Some(link) = self.links.get(&segment) else { return };
        let _ = link.connection.send_payload(command, metadata, payload);
    }

    /// Closes the segment's producer. Without a link, the connection just went: the producer heard it from the
    /// pushes.
    fn close_producer(&mut self, segment: u64, generation: u64) {
        let Some(link) = self.links.remove(&segment) else { return };
        debug_assert_eq!(link.generation, generation);
        self.closes.push(Box::pin(async move {
            // Its receipts keep coming until the answer: the broker answers once its pending writes are done.
            let producer_id = link.producer_id;
            let _ = link.connection.request(move |request_id| close_command(producer_id, request_id)).await;
            (segment, generation)
        }));
    }

    /// The broker and the segment's producer disagree on what was sent: end the connection, so that the producer
    /// registers again and writes everything again. Its owners hear about it from their pushes.
    fn disconnect(&mut self, segment: u64, generation: u64) {
        let Some(link) = self.links.get(&segment).filter(|link| link.generation == generation) else { return };
        tracing::warn!(segment, "a receipt did not match the producer's queue: reconnecting");
        let connection = link.connection.clone();
        self.closes.push(Box::pin(async move {
            connection.close().await;
            (u64::MAX, u64::MAX)
        }));
    }

    fn done(
        &mut self,
        send: SendId,
        result: std::result::Result<apache_pulsar_protocol::message_id::MessageId, SendError>,
    ) {
        let result = result.map(MessageId).map_err(send_error);
        let (reply, room) = self.replies.remove(&send).expect("a send in flight");
        // The room goes back first, so a send chained on this one finds it.
        drop(room);
        let _ = reply.send(result.clone());
        // A flush fails if a send it waits for failed, as Java's does.
        if let Err(error) = &result {
            for (_, failure, _) in self.flushes.iter_mut().filter(|(last, ..)| send <= *last) {
                failure.get_or_insert_with(|| error.clone());
            }
        }
        // A flush is done once none of the sends it waits for is outstanding.
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
    generation: u64,
    mut command: BaseCommand,
    registered: &RefCell<Option<(ConnHandle, u64)>>,
) -> std::result::Result<Linked, RegisterError> {
    let address = client.lookup(topic).await?;
    let connection = client.connection(&address).await?;
    let (producer_id, pushes) = connection.register(IdKind::Producer).await?;
    // From here, a failure unregisters the producer id as the link drops.
    let link = Link { generation, connection, producer_id };
    command.producer.get_or_insert_default().producer_id = producer_id;
    *registered.borrow_mut() = Some((link.connection.clone(), producer_id));
    let frame = link
        .connection
        .request(move |request_id| {
            let mut command = command;
            command.producer.get_or_insert_default().request_id = request_id;
            command
        })
        .await?;
    let success = frame
        .command_view()
        .ok()
        .and_then(|c| c.producer_success.as_option().and_then(|s| s.to_owned_message().ok()))
        .ok_or(RegisterError::Unavailable("not a producer success".into()))?;
    Ok((link, pushes, success))
}

/// Why a producer or a consumer couldn't register: the broker's refusal, or no broker to ask. Credentials the broker
/// refused, or the provider couldn't supply, count as the broker's `AuthenticationError`, which isn't retried, as a
/// lookup doesn't retry them.
impl From<Error> for RegisterError {
    fn from(error: Error) -> Self {
        match error.server_error() {
            Some((error, message)) => RegisterError::Server { error, message },
            None if error.kind() == ErrorKind::Authentication => {
                RegisterError::Server { error: ServerError::AuthenticationError, message: error.message().to_string() }
            }
            None => RegisterError::Unavailable(error.to_string()),
        }
    }
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
        SendError::TooLarge { .. } => ErrorKind::MessageTooLarge,
        SendError::Routing(_) => ErrorKind::Other,
    };
    Error::with_source(kind, error.to_string(), error)
}

#[cfg(test)]
mod tests {
    use apache_pulsar_protocol::layout::LayoutError;
    use apache_pulsar_protocol::proto::ServerError;

    use super::*;

    #[test]
    fn send_failures_map_to_kinds() {
        let terminated = SendError::Server { error: ServerError::TopicTerminatedError, message: "sealed".into() };
        let cases = [
            (SendError::AlreadyClosed, ErrorKind::AlreadyClosed),
            (SendError::Timeout, ErrorKind::Timeout),
            (SendError::Unavailable("no broker".into()), ErrorKind::Timeout),
            (terminated, ErrorKind::TopicTerminated),
            (SendError::TooLarge { size: 6, limit: 5 }, ErrorKind::MessageTooLarge),
            (SendError::Routing(LayoutError::NoActiveSegment), ErrorKind::Other),
        ];
        for (failure, kind) in cases {
            let message = failure.to_string();
            let error = send_error(failure);
            assert_eq!(error.kind(), kind, "{message}");
            assert_eq!(error.to_string(), format!("{kind:?}: {message}"));
        }
    }
}
