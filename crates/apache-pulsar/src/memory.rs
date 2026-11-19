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
use std::sync::Mutex;
use std::sync::atomic::{AtomicU64, Ordering};
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
    queue: VecDeque<(u64, Option<Waker>)>,
    next_ticket: u64,
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
    pub(crate) fn reserve(&self, bytes: u64, block: bool) -> Reserve<'_> {
        Reserve { limit: self, bytes, block, ticket: None }
    }

    pub(crate) fn release(&self, bytes: u64) {
        self.used.fetch_sub(bytes, Ordering::AcqRel);
        if self.limit > 0 {
            let waiters = self.waiters.lock().unwrap();
            if let Some((_, Some(waker))) = waiters.queue.front() {
                waker.wake_by_ref();
            }
        }
    }

    /// One attempt: room while usage is at or below the limit.
    fn try_reserve(&self, bytes: u64) -> bool {
        let mut current = self.used.load(Ordering::Acquire);
        loop {
            if self.limit > 0 && current > self.limit {
                return false;
            }
            match self.used.compare_exchange_weak(current, current + bytes, Ordering::AcqRel, Ordering::Acquire) {
                Ok(_) => return true,
                Err(actual) => current = actual,
            }
        }
    }
}

/// The future of [`MemoryLimit::reserve`].
#[derive(Debug)]
pub(crate) struct Reserve<'a> {
    limit: &'a MemoryLimit,
    bytes: u64,
    block: bool,
    ticket: Option<u64>,
}

impl Future for Reserve<'_> {
    type Output = Result<()>;

    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<Result<()>> {
        let this = &mut *self;
        let limit = this.limit;
        let mut waiters = limit.waiters.lock().unwrap();
        match this.ticket {
            None => {
                // Nobody waits: take room right away, if there is some.
                if waiters.queue.is_empty() && limit.try_reserve(this.bytes) {
                    return Poll::Ready(Ok(()));
                }
                if !this.block {
                    return Poll::Ready(Err(Error::new(
                        ErrorKind::MemoryBufferFull,
                        "the client memory limit is reached",
                    )));
                }
                let ticket = waiters.next_ticket;
                waiters.next_ticket += 1;
                waiters.queue.push_back((ticket, Some(cx.waker().clone())));
                this.ticket = Some(ticket);
                Poll::Pending
            }
            Some(ticket) => {
                let first = waiters.queue.front().is_some_and(|(t, _)| *t == ticket);
                if first && limit.try_reserve(this.bytes) {
                    waiters.queue.pop_front();
                    this.ticket = None;
                    // The next in line may fit too.
                    if let Some((_, Some(waker))) = waiters.queue.front() {
                        waker.wake_by_ref();
                    }
                    return Poll::Ready(Ok(()));
                }
                if let Some(entry) = waiters.queue.iter_mut().find(|(t, _)| *t == ticket) {
                    entry.1 = Some(cx.waker().clone());
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
            if let Some((_, Some(waker))) = waiters.queue.front() {
                waker.wake_by_ref();
            }
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures_util::FutureExt;

    #[test]
    fn admits_until_the_limit_is_passed() {
        let limit = MemoryLimit::new(100);
        assert!(limit.reserve(60, false).now_or_never().unwrap().is_ok());
        // Usage 60 is under the limit: this one goes over it, as in Java.
        assert!(limit.reserve(60, false).now_or_never().unwrap().is_ok());
        let full = limit.reserve(1, false).now_or_never().unwrap().unwrap_err();
        assert_eq!(full.kind(), ErrorKind::MemoryBufferFull);
        limit.release(60);
        limit.release(60);
        assert_eq!(limit.used(), 0);
        let unlimited = MemoryLimit::new(0);
        assert!(unlimited.reserve(u64::MAX / 2, false).now_or_never().unwrap().is_ok());
    }

    #[test]
    fn waiters_get_room_in_order() {
        let limit = MemoryLimit::new(10);
        limit.reserve(20, false).now_or_never().unwrap().unwrap();
        let mut first = Box::pin(limit.reserve(5, true));
        let mut second = Box::pin(limit.reserve(5, true));
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        // A newcomer queues behind them, even though nothing is free anyway.
        let mut third = Box::pin(limit.reserve(1, true));
        assert!(third.as_mut().poll(&mut cx).is_pending());
        limit.release(20);
        // Only the head may take room.
        assert!(second.as_mut().poll(&mut cx).is_pending());
        assert!(first.as_mut().poll(&mut cx).is_ready());
        assert!(second.as_mut().poll(&mut cx).is_ready());
        // Usage is 10, at the limit: the next fits.
        assert!(third.as_mut().poll(&mut cx).is_ready());
        drop((first, second, third));
        assert_eq!(limit.used(), 11);
    }

    #[test]
    fn a_dropped_waiter_leaves_the_line() {
        let limit = MemoryLimit::new(10);
        limit.reserve(20, false).now_or_never().unwrap().unwrap();
        let waker = futures_util::task::noop_waker();
        let mut cx = Context::from_waker(&waker);
        let mut first = Box::pin(limit.reserve(5, true));
        let mut second = Box::pin(limit.reserve(5, true));
        assert!(first.as_mut().poll(&mut cx).is_pending());
        assert!(second.as_mut().poll(&mut cx).is_pending());
        drop(first);
        limit.release(20);
        assert!(second.as_mut().poll(&mut cx).is_ready());
    }
}
