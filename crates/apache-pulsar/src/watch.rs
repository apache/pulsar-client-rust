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

//! Layout watches (docs/6-scalable-topics.md §2).
//!
//! One watch per topic per loop, shared by the producers and consumers of the topic on the loop, and closed with
//! the last of them. It finds a broker with a standard lookup on the topic name, as the Java client does, and
//! opens a `CommandScalableTopicLookup` session there. A layout is applied only if its epoch is greater than the
//! last applied (*client-behavior* §1). After a connection loss the watch reopens its session with backoff, and
//! keeps the last layout meanwhile.
//!
//! A failure before the first layout fails whoever opened the watch, as the Java client's `DagWatchClient` does:
//! the caller of `create()` sees it, rather than a retry it can't observe.

use std::cell::{Cell, RefCell};
use std::collections::HashMap;
use std::rc::{Rc, Weak};
use std::time::Duration;

use apache_pulsar_protocol::backoff::Backoff;
use apache_pulsar_protocol::connection::IdKind;
use apache_pulsar_protocol::layout::Layout;
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandScalableTopicClose, CommandScalableTopicLookup};
use buffa::{EnumValue, MessageView};
use compio::runtime::JoinHandle;
use futures_channel::{mpsc, oneshot};
use futures_util::StreamExt;

use crate::conn::{ConnHandle, Push};
use crate::error::{Error, ErrorKind, Result, server_error_kind};
use crate::local::{self, jitter};

/// Java's `DagWatchClient` reconnects from 100 ms up to 30 s.
const RECONNECT_BACKOFF: (Duration, Duration) = (Duration::from_millis(100), Duration::from_secs(30));

/// The watches of one loop, by topic name as the application wrote it.
#[derive(Default)]
pub(crate) struct Watches {
    by_topic: RefCell<HashMap<String, Weak<Shared>>>,
}

/// A user's hold on a watch: the current layout, and the ones applied after it. The watch closes when its last
/// holder drops.
pub(crate) struct LayoutWatch {
    shared: Rc<Shared>,
    /// Every layout applied after the one the holder started from.
    pub(crate) updates: mpsc::UnboundedReceiver<Rc<Layout>>,
}

impl std::fmt::Debug for LayoutWatch {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("LayoutWatch").field("topic", &self.shared.topic).finish()
    }
}

impl LayoutWatch {
    /// The layout now: the last one applied.
    pub(crate) fn current(&self) -> Rc<Layout> {
        self.shared.state.borrow().layout.clone().expect("a watch is handed out once it has a layout")
    }
}

struct Shared {
    topic: String,
    state: RefCell<State>,
    task: Cell<Option<JoinHandle<()>>>,
}

#[derive(Default)]
struct State {
    layout: Option<Rc<Layout>>,
    subscribers: Vec<mpsc::UnboundedSender<Rc<Layout>>>,
    /// Openers waiting for the first layout.
    waiters: Vec<oneshot::Sender<Result<()>>>,
    /// The session open now, to close when the watch goes.
    session: Option<(ConnHandle, u64)>,
}

impl Drop for Shared {
    fn drop(&mut self) {
        if let Some((connection, session_id)) = self.state.get_mut().session.take() {
            close_session(&connection, session_id);
        }
        // Dropping the handle cancels the task.
        drop(self.task.take());
    }
}

fn close_session(connection: &ConnHandle, session_id: u64) {
    let close = BaseCommand {
        r#type: Type::SCALABLE_TOPIC_CLOSE.into(),
        scalableTopicClose: CommandScalableTopicClose { session_id, ..Default::default() }.into(),
        ..Default::default()
    };
    // Best effort: a connection that is gone has closed the session with it.
    let _ = connection.send(close);
    connection.unregister(IdKind::Session, session_id);
}

impl Watches {
    /// Opens a watch on `topic`, or joins the loop's watch on it, and returns once there is a layout.
    pub(crate) async fn open(&self, client: &local::PulsarClient, topic: &str) -> Result<LayoutWatch> {
        let existing = self.by_topic.borrow().get(topic).and_then(Weak::upgrade);
        let shared = match existing {
            Some(shared) => shared,
            None => {
                let shared = Rc::new(Shared {
                    topic: topic.to_string(),
                    state: RefCell::new(State::default()),
                    task: Cell::new(None),
                });
                let task = compio::runtime::spawn(run(Rc::downgrade(&shared), client.clone(), topic.to_string()));
                shared.task.set(Some(task));
                self.by_topic.borrow_mut().insert(topic.to_string(), Rc::downgrade(&shared));
                shared
            }
        };
        // Wait for the first layout, unless there is one already.
        let waiting = {
            let mut state = shared.state.borrow_mut();
            match state.layout {
                Some(_) => None,
                None => {
                    let (ready, first) = oneshot::channel();
                    state.waiters.push(ready);
                    Some(first)
                }
            }
        };
        if let Some(first) = waiting {
            let outcome = first.await.unwrap_or_else(|_| Err(Error::closed()));
            if let Err(error) = outcome {
                // A failed watch is not reused: the next opener starts a new one.
                let mut by_topic = self.by_topic.borrow_mut();
                if by_topic.get(topic).is_some_and(|w| w.ptr_eq(&Rc::downgrade(&shared))) {
                    by_topic.remove(topic);
                }
                return Err(error);
            }
        }
        let (subscriber, updates) = mpsc::unbounded();
        shared.state.borrow_mut().subscribers.push(subscriber);
        Ok(LayoutWatch { shared, updates })
    }
}

/// How a session ended.
enum Ended {
    /// It can't be established: fatal before the first layout.
    Failed(Error),
    /// The connection closed after the session was established, or while establishing it after the first
    /// layout: open it again.
    Lost(Error),
}

async fn run(shared: Weak<Shared>, client: local::PulsarClient, topic: String) {
    let mut backoff = Backoff::new(RECONNECT_BACKOFF.0, RECONNECT_BACKOFF.1);
    loop {
        let ended = session(&shared, &client, &topic, &mut backoff).await;
        let Some(watch) = shared.upgrade() else { return };
        let established = watch.state.borrow().layout.is_some();
        match ended {
            Ended::Failed(error) | Ended::Lost(error) if !established => {
                for waiter in watch.state.borrow_mut().waiters.drain(..) {
                    let _ = waiter.send(Err(error.clone()));
                }
                return;
            }
            // After the first layout every failure is retried, as in Java: the holders keep the last layout.
            Ended::Failed(error) | Ended::Lost(error) => {
                if client.is_closed() {
                    return;
                }
                drop(watch);
                let delay = backoff.next_delay(jitter());
                tracing::info!(%topic, %error, ?delay, "layout watch lost its session, reopening it");
                compio::time::sleep(delay).await;
            }
        }
    }
}

/// Opens a session and serves its updates until it ends.
async fn session(shared: &Weak<Shared>, client: &local::PulsarClient, topic: &str, backoff: &mut Backoff) -> Ended {
    let connection = match connect(client, topic).await {
        Ok(connection) => connection,
        Err(error) if error.kind() == ErrorKind::UnsupportedBroker => return Ended::Failed(error),
        Err(error) => return Ended::Lost(error),
    };
    let (session_id, mut pushes) = match connection.register(IdKind::Session).await {
        Ok(registered) => registered,
        Err(error) => return Ended::Lost(error),
    };
    let lookup = BaseCommand {
        r#type: Type::SCALABLE_TOPIC_LOOKUP.into(),
        scalableTopicLookup: CommandScalableTopicLookup {
            session_id,
            topic: topic.to_string(),
            create_if_missing: Some(true),
            ..Default::default()
        }
        .into(),
        ..Default::default()
    };
    if let Err(error) = connection.send(lookup) {
        return Ended::Lost(error);
    }
    match shared.upgrade() {
        Some(watch) => watch.state.borrow_mut().session = Some((connection.clone(), session_id)),
        None => {
            close_session(&connection, session_id);
            return Ended::Lost(Error::closed());
        }
    }
    let operation_timeout = client.operation_timeout();
    loop {
        let waiting_for_first = shared.upgrade().is_some_and(|w| w.state.borrow().layout.is_none());
        let push = if waiting_for_first {
            // Nothing bounds the first answer but this: a broker that never answers must not hang create().
            match compio::time::timeout(operation_timeout, pushes.next()).await {
                Ok(push) => push,
                Err(_) => {
                    if let Some(watch) = shared.upgrade() {
                        watch.state.borrow_mut().session = None;
                    }
                    close_session(&connection, session_id);
                    let error = Error::new(ErrorKind::Timeout, format!("no layout for {topic} within the timeout"));
                    return Ended::Failed(error);
                }
            }
        } else {
            pushes.next().await
        };
        let Some(watch) = shared.upgrade() else { return Ended::Lost(Error::closed()) };
        match push {
            Some(Push::Command(frame)) => {
                if let Err(error) = apply(&watch, topic, &frame) {
                    if watch.state.borrow().layout.is_none() {
                        watch.state.borrow_mut().session = None;
                        close_session(&connection, session_id);
                        return Ended::Failed(error);
                    }
                    // After the first layout, a broker-side error on the session is transient, as in Java: the
                    // connection-closed path takes it from here.
                    tracing::warn!(%topic, %error, "layout watch error");
                } else {
                    backoff.reset();
                }
            }
            Some(Push::Closed(error)) => {
                watch.state.borrow_mut().session = None;
                return Ended::Lost(error);
            }
            None => {
                watch.state.borrow_mut().session = None;
                return Ended::Lost(Error::new(ErrorKind::NotConnected, "the connection closed"));
            }
        }
    }
}

/// A connection to the broker that serves `topic`, if it supports scalable topics (C15).
async fn connect(client: &local::PulsarClient, topic: &str) -> Result<ConnHandle> {
    let address = client.lookup(topic).await?;
    let connection = client.connection(&address).await?;
    if !connection.broker().features.scalable_topics {
        return Err(Error::new(
            ErrorKind::UnsupportedBroker,
            format!("the broker at {} does not support scalable topics", address.physical),
        ));
    }
    Ok(connection)
}

/// Applies a `CommandScalableTopicUpdate`: a newer layout reaches every holder; an error fails the session.
fn apply(watch: &Shared, topic: &str, frame: &apache_pulsar_protocol::frame::Frame) -> Result<()> {
    let command = frame.command_view().map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    if command.r#type != EnumValue::Known(Type::SCALABLE_TOPIC_UPDATE) {
        return Ok(());
    }
    let Some(update) = command.scalableTopicUpdate.as_option() else { return Ok(()) };
    if let Some(error) = update.error {
        let message = update.message.unwrap_or_default();
        return Err(Error::new(
            server_error_kind(EnumValue::Known(error)),
            format!("scalable topic lookup of {topic} failed: {error:?}: {message}"),
        ));
    }
    let Some(dag) = update.dag.as_option() else { return Ok(()) };
    let dag = dag.to_owned_message().map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    let mut state = watch.state.borrow_mut();
    let resolved = match update.resolved_topic_name {
        Some(name) => name.to_string(),
        None => state.layout.as_ref().map_or_else(|| topic.to_string(), |l| l.topic().to_string()),
    };
    let layout = Layout::from_dag(&dag, &resolved).map_err(|e| Error::new(ErrorKind::Other, e.to_string()))?;
    if let Some(current) = &state.layout
        && layout.epoch() <= current.epoch()
    {
        tracing::debug!(%topic, epoch = layout.epoch(), current = current.epoch(), "stale layout ignored");
        return Ok(());
    }
    tracing::info!(topic = %layout.topic(), epoch = layout.epoch(), active = layout.active().count(), "layout applied");
    let layout = Rc::new(layout);
    state.layout = Some(layout.clone());
    state.subscribers.retain(|subscriber| subscriber.unbounded_send(layout.clone()).is_ok());
    for waiter in state.waiters.drain(..) {
        let _ = waiter.send(Ok(()));
    }
    Ok(())
}
