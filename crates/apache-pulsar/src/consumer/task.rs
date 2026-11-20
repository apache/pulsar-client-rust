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

//! The task that runs one Queue consumer on its loop.
//!
//! It owns the sans-IO [`QueueConsumer`] and its layout watch, and does what the consumer asks: it looks up and
//! connects each segment, subscribes the segment's consumer there, sends its flows, acks and redelivery requests,
//! and closes it. It feeds back the outcome, the commands the brokers push for each segment's consumer, the layouts
//! and the passing of time. It hands the application its messages as receives ask for them, in the order they
//! asked, and takes its acks and its close. Everything is polled from one future.

use std::cell::RefCell;
use std::collections::{HashMap, VecDeque};
use std::future::{Future, poll_fn};
use std::pin::Pin;
use std::rc::Rc;
use std::task::{Context, Poll};
use std::time::Instant;

use apache_pulsar_protocol::connection::IdKind;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::frame::Frame;
use apache_pulsar_protocol::message_id::MessageId;
use apache_pulsar_protocol::producer::RegisterError;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandCloseConsumer};
use apache_pulsar_protocol::queue_consumer::{Action, QueueConsumer};
use buffa::{EnumValue, MessageView};
use futures_channel::{mpsc, oneshot};
use futures_util::stream::{FuturesUnordered, SelectAll};
use futures_util::{Stream, StreamExt};

use crate::conn::{ConnHandle, Push};
use crate::error::{Error, ErrorKind, Result, server_error_kind};
use crate::local;
use crate::watch::LayoutWatch;

/// What a consumer handle asks of its task.
pub(crate) enum Request {
    /// Up to `max` messages: answered once it has them, or at the deadline with what it has.
    Receive {
        max: usize,
        deadline: Option<Instant>,
        reply: oneshot::Sender<Vec<ReceivedMessage>>,
    },
    /// Messages a receive was answered with, and went away without taking: the next receives get them first.
    Return(Vec<ReceivedMessage>),
    Ack {
        id: MessageId,
        reply: oneshot::Sender<Result<()>>,
    },
    Nack {
        id: MessageId,
    },
    /// `None` when the last handle dropped, or the client is closing: close in the background.
    Close {
        reply: Option<oneshot::Sender<Result<()>>>,
    },
}

type Sleep = Pin<Box<dyn Future<Output = ()>>>;
type Subscription = Pin<Box<dyn Future<Output = Subscribed>>>;
type Closing = Pin<Box<dyn Future<Output = (u64, u64)>>>;
type Pushes = Pin<Box<dyn Stream<Item = (u64, u64, Push)>>>;
/// Where a subscribe in flight registered its consumer id, once it did.
type Registering = Rc<RefCell<Option<(ConnHandle, u64)>>>;
/// A subscribed consumer: its link, and the commands pushed for it.
type Linked = (Link, mpsc::UnboundedReceiver<Push>);

/// The outcome of subscribing a segment's consumer.
struct Subscribed {
    segment: u64,
    generation: u64,
    outcome: std::result::Result<Linked, RegisterError>,
}

/// A segment's consumer as registered on a connection. Dropping it unregisters the consumer id there.
struct Link {
    generation: u64,
    connection: ConnHandle,
    consumer_id: u64,
}

impl Drop for Link {
    fn drop(&mut self) {
        self.connection.unregister(IdKind::Consumer, self.consumer_id);
    }
}

/// A receive waiting for its messages.
struct Waiter {
    max: usize,
    deadline: Option<Instant>,
    got: Vec<ReceivedMessage>,
    reply: oneshot::Sender<Vec<ReceivedMessage>>,
}

pub(crate) struct Task {
    queue: QueueConsumer,
    client: local::PulsarClient,
    watch: LayoutWatch,
    /// The client keeps a sender of its own, to close the consumer: the channel doesn't end while the task runs.
    requests: mpsc::UnboundedReceiver<Request>,
    links: HashMap<u64, Link>,
    subscriptions: FuturesUnordered<Subscription>,
    /// Subscribes in flight, and where they registered, once they did: to close them if the consumer goes before
    /// they complete.
    registering: HashMap<(u64, u64), Registering>,
    closes: FuturesUnordered<Closing>,
    pushes: SelectAll<Pushes>,
    /// Receives, in the order they came.
    waiters: VecDeque<Waiter>,
    /// Messages given back by receives that went away, for the next ones.
    returned: VecDeque<ReceivedMessage>,
    subscribed: Option<oneshot::Sender<Result<()>>>,
    close_replies: Vec<oneshot::Sender<Result<()>>>,
    closed: bool,
    timer: Option<(Instant, Sleep)>,
}

impl Task {
    pub(crate) fn new(
        queue: QueueConsumer,
        client: local::PulsarClient,
        watch: LayoutWatch,
        requests: mpsc::UnboundedReceiver<Request>,
        subscribed: oneshot::Sender<Result<()>>,
    ) -> Self {
        Self {
            queue,
            client,
            watch,
            requests,
            links: HashMap::new(),
            subscriptions: FuturesUnordered::new(),
            registering: HashMap::new(),
            closes: FuturesUnordered::new(),
            pushes: SelectAll::new(),
            waiters: VecDeque::new(),
            returned: VecDeque::new(),
            subscribed: Some(subscribed),
            close_replies: Vec::new(),
            closed: false,
            timer: None,
        }
    }

    pub(crate) async fn run(mut self) {
        poll_fn(|cx| self.poll(cx)).await;
        // Requests that came in as the consumer closed fail as they drop: their senders see no answer coming.
        self.requests.close();
        while self.requests.try_recv().is_ok() {}
        // Subscribes that never completed: close them, so that the broker doesn't keep a consumer nobody reads.
        // Dropping them unregisters their consumer ids.
        let pending: Vec<(ConnHandle, u64)> =
            self.registering.values().filter_map(|registered| registered.borrow().clone()).collect();
        self.subscriptions = FuturesUnordered::new();
        for (connection, consumer_id) in pending {
            let _ = connection.request(move |request_id| close_command(consumer_id, request_id)).await;
        }
    }

    fn poll(&mut self, cx: &mut Context<'_>) -> Poll<()> {
        loop {
            let mut progressed = false;
            progressed |= self.poll_requests(cx);
            progressed |= self.poll_layouts(cx);
            progressed |= self.poll_subscriptions(cx);
            progressed |= self.poll_pushes(cx);
            progressed |= self.poll_closes(cx);
            // Before the timer: a receive whose time is up takes the messages in first.
            progressed |= self.serve();
            progressed |= self.poll_timer(cx);
            // An action may start a future (a subscribe, a close) that must be polled before going to sleep.
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
        // Once closing, requests wait for the end of the task, which fails them.
        while !self.queue.is_closed()
            && let Poll::Ready(Some(request)) = self.requests.poll_next_unpin(cx)
        {
            progressed = true;
            let now = Instant::now();
            match request {
                Request::Receive { max, deadline, reply } => {
                    self.waiters.push_back(Waiter { max, deadline, got: Vec::new(), reply });
                }
                Request::Return(messages) => self.give_back(messages),
                Request::Ack { id, reply } => {
                    self.queue.acknowledge(&id, now);
                    let _ = reply.send(Ok(()));
                }
                Request::Nack { id } => self.queue.negative_acknowledge(&id, now),
                Request::Close { reply } => {
                    self.close_replies.extend(reply);
                    self.queue.close();
                }
            }
        }
        progressed
    }

    fn poll_layouts(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some(layout)) = self.watch.updates.poll_next_unpin(cx) {
            progressed = true;
            self.queue.on_layout(layout, Instant::now());
        }
        progressed
    }

    fn poll_subscriptions(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some(subscribed)) = self.subscriptions.poll_next_unpin(cx) {
            progressed = true;
            let Subscribed { segment, generation, outcome } = subscribed;
            self.registering.remove(&(segment, generation));
            match outcome {
                Ok((link, pushes)) => {
                    let consumer_id = link.consumer_id;
                    self.pushes.push(Box::pin(pushes.map(move |push| (segment, generation, push))));
                    self.links.insert(segment, link);
                    self.queue.on_subscribed(segment, generation, consumer_id);
                }
                Err(error) => self.queue.on_subscribe_failed(segment, generation, error, Instant::now()),
            }
        }
        progressed
    }

    fn poll_pushes(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some((segment, generation, push))) = self.pushes.poll_next_unpin(cx) {
            progressed = true;
            match push {
                Push::Command(frame) => self.on_push(segment, generation, &frame),
                // The connection closed, and the consumer with it.
                Push::Closed(_) => self.unlink(segment, generation),
            }
        }
        progressed
    }

    fn on_push(&mut self, segment: u64, generation: u64, frame: &Frame) {
        let Ok(command) = frame.command_view() else { return };
        let Ok(command) = command.to_owned_message() else { return };
        match command.r#type {
            EnumValue::Known(Type::MESSAGE) => {
                // A message comes with its payload: the connection decodes it.
                let Some(section) = &frame.payload else { return };
                self.queue.on_message(segment, generation, &command.message, section);
            }
            EnumValue::Known(Type::REACHED_END_OF_TOPIC) => self.queue.on_reached_end_of_topic(segment, generation),
            // The broker closed the consumer (an unload, a deleted segment) or moved the topic: the only other
            // commands it pushes to a Shared consumer.
            _ => self.unlink(segment, generation),
        }
    }

    /// The segment's consumer of `generation` is gone from its connection: forget its link, if it is still the
    /// current one, and tell the consumer.
    fn unlink(&mut self, segment: u64, generation: u64) {
        self.links.retain(|&s, link| (s, link.generation) != (segment, generation));
        self.queue.on_disconnected(segment, generation, Instant::now());
    }

    fn poll_closes(&mut self, cx: &mut Context<'_>) -> bool {
        let mut progressed = false;
        while let Poll::Ready(Some((segment, generation))) = self.closes.poll_next_unpin(cx) {
            progressed = true;
            self.queue.on_closed(segment, generation);
        }
        progressed
    }

    fn poll_timer(&mut self, cx: &mut Context<'_>) -> bool {
        let receive = self.waiters.iter().filter_map(|waiter| waiter.deadline).min();
        let Some(deadline) = self.queue.poll_timeout().into_iter().chain(receive).min() else {
            self.timer = None;
            return false;
        };
        // A sleep only completes once its deadline has passed: either way, the deadline is reached.
        if deadline > Instant::now() && self.poll_sleep(deadline, cx).is_pending() {
            return false;
        }
        self.timer = None;
        let now = Instant::now();
        self.queue.handle_timeout(now);
        // The receives whose time is up get what they have.
        let (due, waiting): (VecDeque<Waiter>, VecDeque<Waiter>) = std::mem::take(&mut self.waiters)
            .into_iter()
            .partition(|waiter| waiter.deadline.is_some_and(|deadline| deadline <= now));
        self.waiters = waiting;
        for waiter in due {
            self.answer(waiter);
        }
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

    /// Hands the messages in to the receives, in the order they came: each takes what it asked for before the next
    /// gets any. A message is taken from the consumer only for a receive, so that the broker's permits follow what
    /// the application takes.
    fn serve(&mut self) -> bool {
        if self.queue.is_closed() {
            // The receives fail: their answer never comes. The broker delivers again the messages they had, and those
            // given back.
            self.waiters.clear();
            self.returned.clear();
            return false;
        }
        let mut progressed = false;
        while let Some(waiter) = self.waiters.front_mut() {
            // A receive that went away is answered at once: its answer comes back, with whatever it had.
            let gone = waiter.reply.is_canceled();
            while !gone && waiter.got.len() < waiter.max {
                let Some(message) = self.returned.pop_front().or_else(|| self.queue.next_message()) else { break };
                waiter.got.push(message);
                progressed = true;
            }
            if !gone && waiter.got.len() < waiter.max {
                break;
            }
            let waiter = self.waiters.pop_front().expect("the first receive");
            self.answer(waiter);
            progressed = true;
        }
        progressed
    }

    /// Answers a receive. One that went away gives its messages back, for the next ones.
    fn answer(&mut self, waiter: Waiter) {
        if let Err(messages) = waiter.reply.send(waiter.got) {
            self.give_back(messages);
        }
    }

    fn give_back(&mut self, messages: Vec<ReceivedMessage>) {
        for message in messages.into_iter().rev() {
            self.returned.push_front(message);
        }
    }

    fn drain_actions(&mut self) -> bool {
        let mut progressed = false;
        while let Some(action) = self.queue.poll_action() {
            progressed = true;
            match action {
                Action::Connect { segment, generation, topic } => self.connect(segment, generation, topic),
                Action::Send { segment, command } => self.send(segment, *command),
                Action::CloseConsumer { segment, generation } => self.close_consumer(segment, generation),
                Action::Subscribed(result) => {
                    let subscribed = self.subscribed.take().expect("a consumer subscribes once");
                    // Nobody waits for it any more, as the subscribe was dropped: close it, before the broker
                    // gives it messages that nobody takes.
                    if subscribed.send(result.map_err(subscribe_error)).is_err() {
                        self.queue.close();
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
        let Some(command) = self.queue.subscribe_command(segment, 0) else { return };
        let client = self.client.clone();
        let registered = Rc::new(RefCell::new(None));
        self.registering.insert((segment, generation), registered.clone());
        self.subscriptions.push(Box::pin(async move {
            let outcome = subscribe(&client, &topic, generation, command, &registered).await;
            Subscribed { segment, generation, outcome }
        }));
    }

    /// Sends a flow, an ack or a redelivery request on the segment consumer's connection. Without a link, the
    /// connection just went: the consumer heard it from the pushes. A connection that is going is noticed from the
    /// pushes too.
    fn send(&self, segment: u64, command: BaseCommand) {
        let Some(link) = self.links.get(&segment) else { return };
        let _ = link.connection.send(command);
    }

    /// Closes the segment's consumer on the broker. Without a link, the connection just went: the consumer heard it
    /// from the pushes.
    fn close_consumer(&mut self, segment: u64, generation: u64) {
        let Some(link) = self.links.remove(&segment) else { return };
        debug_assert_eq!(link.generation, generation);
        self.closes.push(Box::pin(async move {
            let consumer_id = link.consumer_id;
            let _ = link.connection.request(move |request_id| close_command(consumer_id, request_id)).await;
            (segment, generation)
        }));
    }
}

/// Looks up a segment's topic, connects, and subscribes a consumer there.
async fn subscribe(
    client: &local::PulsarClient,
    topic: &str,
    generation: u64,
    mut command: BaseCommand,
    registered: &RefCell<Option<(ConnHandle, u64)>>,
) -> std::result::Result<Linked, RegisterError> {
    let address = client.lookup(topic).await?;
    let connection = client.connection(&address).await?;
    let (consumer_id, pushes) = connection.register(IdKind::Consumer).await?;
    // From here, a failure unregisters the consumer id as the link drops.
    let link = Link { generation, connection, consumer_id };
    command.subscribe.get_or_insert_default().consumer_id = consumer_id;
    *registered.borrow_mut() = Some((link.connection.clone(), consumer_id));
    link.connection
        .request(move |request_id| {
            let mut command = command;
            command.subscribe.get_or_insert_default().request_id = request_id;
            command
        })
        .await?;
    Ok((link, pushes))
}

fn close_command(consumer_id: u64, request_id: u64) -> BaseCommand {
    BaseCommand {
        r#type: Type::CLOSE_CONSUMER.into(),
        close_consumer: CommandCloseConsumer { consumer_id, request_id, ..Default::default() }.into(),
        ..Default::default()
    }
}

/// The public error for a subscribe that failed.
pub(crate) fn subscribe_error(error: RegisterError) -> Error {
    match error {
        RegisterError::Server { error, message } => Error::new(server_error_kind(EnumValue::Known(error)), message),
        // Retried until the operation timeout.
        RegisterError::Unavailable(reason) => Error::new(ErrorKind::Timeout, reason),
    }
}

#[cfg(test)]
mod tests {
    use apache_pulsar_protocol::proto::ServerError;

    use super::*;

    #[test]
    fn subscribe_failures_map_to_kinds() {
        let refused = RegisterError::Server { error: ServerError::AuthorizationError, message: "no".into() };
        assert_eq!(subscribe_error(refused).kind(), ErrorKind::Authorization);
        let unavailable = subscribe_error(RegisterError::Unavailable("no broker".into()));
        assert_eq!((unavailable.kind(), unavailable.to_string()), (ErrorKind::Timeout, "Timeout: no broker".into()));
    }
}
