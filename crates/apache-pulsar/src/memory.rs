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

//! The client's memory limit: the only bound on what producers hold (docs/2-architecture.md §4.1).
//!
//! Each send reserves its payload plus [`PER_MESSAGE_OVERHEAD`] before it is queued anywhere, on the caller's side
//! of the hop to the loop, and gives it back when it completes. As in Java's `MemoryLimitController`, a
//! reservation succeeds while usage is at or below the limit, so one message may go over it. A send that has to
//! wait gets room in the order it first asked.

use std::collections::VecDeque;
use std::future::Future;
use std::pin::Pin;
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::{Arc, Mutex};
use std::task::{Context, Poll, Waker};

use crate::error::{Error, ErrorKind, Result};

/// What each pending message is charged on top of its payload, for its bookkeeping: Java's
/// `PER_MESSAGE_OVERHEAD_BYTES`. It makes the limit bound the number of messages too.
pub(crate) const PER_MESSAGE_OVERHEAD: u64 = 1024;

/// See the module documentation.
#[derive(Debug)]
pub(crate) struct MemoryLimit {
    /// 0 for no limit.
    limit: u64,
    used: AtomicU64,
    waiters: Mutex<Waiters>,
}

#[derive(Debug, Default)]
struct Waiters {
    /// Tickets in the order they were taken, with the waker of each.
    queue: VecDeque<(u64, Waker)>,
    next_ticket: u64,
}

impl Waiters {
    /// Wakes the first in line: it may fit now.
    fn wake_first(&self) {
        if let Some((_, waker)) = self.queue.front() {
            waker.wake_by_ref();
        }
    }
}

impl MemoryLimit {
    pub(crate) fn new(limit: u64) -> Self {
        Self { limit, used: AtomicU64::new(0), waiters: Mutex::new(Waiters::default()) }
    }

    #[cfg(test)]
    pub(crate) fn used(&self) -> u64 {
        self.used.load(Ordering::Acquire)
    }

    /// Reserves `bytes`, waiting for room if `block`, or failing with [`ErrorKind::MemoryBufferFull`].
    pub(crate) fn reserve(self: &Arc<Self>, bytes: u64, block: bool) -> Reserve<'_> {
        Reserve { limit: self, bytes, block, ticket: None }
    }

    fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
        if self.limit > 0 {
            self.waiters.lock().unwrap().wake_first();
        }
    }

    /// One attempt: room while usage is at or below the limit.
    fn try_reserve(&self, bytes: u64) -> bool {
        let fits = |used: u64| (self.limit == 0 || used <= self.limit).then_some(used + bytes);
        self.used.try_update(Ordering::AcqRel, Ordering::Acquire, fits).is_ok()
    }
}

/// Room reserved under the limit. It goes back when dropped.
#[derive(Debug)]
pub(crate) struct Reservation {
    limit: Arc<MemoryLimit>,
    bytes: u64,
}

impl Drop for Reservation {
    fn drop(&mut self) {
        self.limit.release(self.bytes);
    }
}

/// The future of [`MemoryLimit::reserve`].
#[derive(Debug)]
pub(crate) struct Reserve<'a> {
    limit: &'a Arc<MemoryLimit>,
    bytes: u64,
    block: bool,
    ticket: Option<u64>,
}

impl Future for Reserve<'_> {
    type Output = Result<Reservation>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<Reservation>> {
        let this = &mut *self;
        let limit = this.limit;
        let reserved = || Poll::Ready(Ok(Reservation { limit: limit.clone(), bytes: this.bytes }));
        let mut waiters = limit.waiters.lock().unwrap();
        match this.ticket {
            None => {
                // Nobody waits: take room right away, if there is some.
                if waiters.queue.is_empty() && limit.try_reserve(this.bytes) {
                    return reserved();
                }
                if !this.block {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::MemoryBufferFull,
                        "the client memory limit is reached",
                    )));
                }
                let ticket = waiters.next_ticket;
                waiters.next_ticket += 1;
                waiters.queue.push_back((ticket, cx.waker().clone()));
                this.ticket = Some(ticket);
                Poll::Pending
            }
            Some(ticket) => {
                let first = waiters.queue.front().is_some_and(|(t, _)| *t == ticket);
                if first && limit.try_reserve(this.bytes) {
                    waiters.queue.pop_front();
                    // The next in line may fit too.
                    waiters.wake_first();
                    this.ticket = None;
                    return reserved();
                }
                for (_, waker) in waiters.queue.iter_mut().filter(|(t, _)| *t == ticket) {
                    waker.clone_from(cx.waker());
                }
                Poll::Pending
            }
        }
    }
}

impl Drop for Reserve<'_> {
    fn drop(&mut self) {
        // A send dropped while waiting leaves the line, and the next one may go.
        if let Some(ticket) = self.ticket.take() {
            let mut waiters = self.limit.waiters.lock().unwrap();
            waiters.queue.retain(|(t, _)| *t != ticket);
            waiters.wake_first();
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[test]
    fn admits_until_the_limit_is_passed() {
        let limit = Arc::new(MemoryLimit::new(100));
        let first = limit.reserve(60, false).now_or_never().unwrap().unwrap();
        // Usage 60 is under the limit: this one goes over it, as in Java.
        let second = limit.reserve(60, false).now_or_never().unwrap().unwrap();
        let full = limit.reserve(1, false).now_or_never().unwrap().unwrap_err();
        assert_eq!(full.kind(), ErrorKind::MemoryBufferFull);
        drop((first, second));
        assert_eq!(limit.used(), 0);
        let unlimited = Arc::new(MemoryLimit::new(0));
        drop(unlimited.reserve(u64::MAX / 2, false).now_or_never().unwrap().unwrap());
        assert_eq!(unlimited.used(), 0);
    }

    #[test]
    fn waiters_get_room_in_order() {
        let limit = Arc::new(MemoryLimit::new(10));
        let held = limit.reserve(20, false).now_or_never().unwrap().unwrap();
        let mut first = Box::pin(limit.reserve(5, true));
        let mut second = Box::pin(limit.reserve(5, true));
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        // A newcomer queues behind them, even though nothing is free anyway.
        let mut third = Box::pin(limit.reserve(1, true));
        assert!(third.as_mut().poll(&mut cx).is_pending());
        drop(held);
        // Only the head may take room.
        assert!(second.as_mut().poll(&mut cx).is_pending());
        let reserved = [first.as_mut().poll(&mut cx), second.as_mut().poll(&mut cx), third.as_mut().poll(&mut cx)];
        // Usage was 10, at the limit, when the third asked: it fits.
        assert!(reserved.iter().all(|r| matches!(r, Poll::Ready(Ok(_)))));
        assert_eq!(limit.used(), 11);
        drop(reserved);
        assert_eq!(limit.used(), 0);
    }

    #[test]
    fn a_dropped_waiter_leaves_the_line() {
        let limit = Arc::new(MemoryLimit::new(10));
        let held = limit.reserve(20, false).now_or_never().unwrap().unwrap();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut first = Box::pin(limit.reserve(5, true));
        let mut second = Box::pin(limit.reserve(5, true));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        drop(first);
        drop(held);
        assert!(second.as_mut().poll(&mut cx).is_ready());
        // The last one in line leaves an empty line.
        let held = limit.reserve(20, false).now_or_never().unwrap().unwrap();
        let mut alone = Box::pin(limit.reserve(5, true));
        assert!(alone.as_mut().poll(&mut cx).is_pending());
        drop(alone);
        drop(held);
        assert!(limit.reserve(1, false).now_or_never().unwrap().is_ok());
    }
}
