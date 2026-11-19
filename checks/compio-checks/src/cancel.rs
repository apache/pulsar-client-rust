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

//! Check 4: dropping a future while its operation is in flight.
//!
//! Buffers are tracked without unsafe code: a read buffer is a `BytesMut` split off an allocation
//! whose other half is kept as a frozen `Bytes` sentinel, and a write buffer is a `Bytes` with a kept
//! clone. `sentinel.is_unique()` turns true once the driver has released the buffer.
//!
//! What is asserted: the runtime and the socket keep working after a cancel, no buffer stays held
//! after the cancel completes, and nothing panics. What is only reported: whether bytes are lost when
//! a read is dropped while data arrives, and whether a dropped write still sends bytes. Safe code
//! cannot prove the absence of a use-after-free.

use std::{
    cell::Cell,
    future::Future,
    io::{Read, Write},
    pin::{Pin, pin},
    rc::Rc,
    sync::mpsc,
    task::{Context, Poll},
    thread,
    time::{Duration, Instant},
};

use bytes::{Bytes, BytesMut};
use compio::{
    BufResult,
    io::{AsyncRead, AsyncReadExt, AsyncWrite, AsyncWriteExt},
    net::{TcpListener, TcpStream},
    runtime::spawn,
    time::{sleep, timeout},
};
use futures::future::{Either, select};

use crate::{CheckResult, ensure, note, rt};

const CANCELS: usize = 2000;

/// Pending once, then ready: dropping a future raced against it drops it right after its operation
/// was submitted (the runtime polls the driver before re-polling a woken task).
struct YieldOnce(bool);

impl Future for YieldOnce {
    type Output = ();
    fn poll(mut self: Pin<&mut Self>, cx: &mut Context<'_>) -> Poll<()> {
        if self.0 {
            Poll::Ready(())
        } else {
            self.0 = true;
            cx.waker().wake_by_ref();
            Poll::Pending
        }
    }
}

/// Polls `fut` first, then `stop`; drops `fut` if `stop` finishes first. Unlike
/// `compio::time::timeout`, whose `select!` picks a random branch when both are ready, this never
/// drops a completed `fut`.
async fn biased<F: Future>(fut: F, stop: impl Future<Output = ()>) -> Option<F::Output> {
    let fut = pin!(fut);
    let stop = pin!(stop);
    match select(fut, stop).await {
        Either::Left((v, _)) => Some(v),
        Either::Right(_) => None,
    }
}

/// A read buffer of `cap` bytes and a sentinel sharing its allocation.
fn tracked_read_buf(cap: usize) -> (BytesMut, Bytes) {
    let mut whole = BytesMut::with_capacity(cap + 1);
    let buf = whole.split_off(1);
    (buf, whole.freeze())
}

async fn pair() -> Result<(TcpStream, TcpStream), String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    let (c, s) = futures::join!(TcpStream::connect(addr), listener.accept());
    Ok((c.map_err(|e| e.to_string())?, s.map_err(|e| e.to_string())?.0))
}

async fn round_trip(from: &TcpStream, to: &TcpStream, msg: &'static [u8]) -> Result<(), String> {
    let mut w = from;
    let mut r = to;
    let BufResult(res, _) = w.write_all(msg).await;
    res.map_err(|e| format!("write after cancel: {e}"))?;
    let BufResult(res, got) = r.read_exact(Vec::with_capacity(msg.len())).await;
    res.map_err(|e| format!("read after cancel: {e}"))?;
    ensure!(got == msg, "data corrupted after cancel: {got:?}");
    Ok(())
}

pub fn run(cfg: &rt::Config) -> CheckResult {
    let rt = rt::build(cfg, "loop")?;
    rt.block_on(async {
        note(read_drop().await?);
        note(accept_drop().await?);
        note(connect_drop().await?);
        note(write_drop().await?);
        note(task_drop().await?);
        note(read_race("compio::time::timeout (random select)", false).await?);
        note(read_race("biased select, read polled first", true).await?);
        Ok::<_, String>(())
    })?;
    drop(rt);
    note(runtime_drop(cfg)?);
    Ok(())
}

/// Drops pending reads right after submission and after a timer, many times.
async fn read_drop() -> Result<String, String> {
    let (client, server) = pair().await?;
    let mut sentinels = Vec::with_capacity(CANCELS);
    let mut held_after_drop = 0;
    for i in 0..CANCELS {
        let (buf, sentinel) = tracked_read_buf(4096);
        let mut r = &client;
        let res = if i % 10 == 0 {
            timeout(Duration::from_millis(1), r.read(buf)).await.ok()
        } else {
            biased(r.read(buf), YieldOnce(false)).await
        };
        ensure!(res.is_none(), "a read completed although no data was sent");
        if !sentinel.is_unique() {
            held_after_drop += 1;
        }
        sentinels.push(sentinel);
    }
    sleep(Duration::from_millis(50)).await;
    let leaked = sentinels.iter().filter(|s| !s.is_unique()).count();
    ensure!(leaked == 0, "{leaked} of {CANCELS} read buffers still held 50ms after their reads were dropped");
    round_trip(&server, &client, b"after read cancels").await?;
    Ok(format!(
        "read: {CANCELS} pending reads dropped; buffer still held by the driver right after drop: \
         {held_after_drop}/{CANCELS}; all released after 50ms; the stream reads intact data afterwards"
    ))
}

async fn accept_drop() -> Result<String, String> {
    let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| e.to_string())?;
    let addr = listener.local_addr().map_err(|e| e.to_string())?;
    for i in 0..CANCELS {
        let res = if i % 10 == 0 {
            timeout(Duration::from_millis(1), listener.accept()).await.ok()
        } else {
            biased(listener.accept(), YieldOnce(false)).await
        };
        ensure!(res.is_none(), "an accept completed with no client");
    }
    let (c, s) = futures::join!(TcpStream::connect(addr), listener.accept());
    let c = c.map_err(|e| format!("connect after accept cancels: {e}"))?;
    let (s, _) = s.map_err(|e| format!("accept after accept cancels: {e}"))?;
    round_trip(&c, &s, b"after accept cancels").await?;
    Ok(format!("accept: {CANCELS} pending accepts dropped; the listener accepts afterwards"))
}

/// Fills the accept queue of a listener that never accepts, so a connect stays pending, then drops it.
async fn connect_drop() -> Result<String, String> {
    use socket2::{Domain, Socket, Type};
    let sock = Socket::new(Domain::IPV4, Type::STREAM, None).map_err(|e| e.to_string())?;
    sock.bind(&std::net::SocketAddr::from(([127, 0, 0, 1], 0)).into()).map_err(|e| e.to_string())?;
    sock.listen(0).map_err(|e| e.to_string())?;
    let addr = sock.local_addr().map_err(|e| e.to_string())?.as_socket().ok_or("no socket addr")?;
    let mut established = Vec::new();
    let mut dropped = false;
    for _ in 0..512 {
        match timeout(Duration::from_millis(300), TcpStream::connect(addr)).await {
            Ok(Ok(s)) => established.push(s),
            Ok(Err(e)) => {
                return Ok(format!(
                    "connect: could not produce a pending connect (connect failed after {} queued: {e}); \
                     not verified",
                    established.len()
                ));
            }
            Err(_) => {
                dropped = true;
                break;
            }
        }
    }
    if !dropped {
        return Ok("connect: could not produce a pending connect (512 connects completed); not verified".into());
    }
    // The runtime still connects elsewhere.
    let (c, s) = pair().await?;
    round_trip(&c, &s, b"after connect cancel").await?;
    Ok(format!(
        "connect: a connect pending for 300ms (accept queue full after {} connections) was dropped; \
         new connections work afterwards",
        established.len()
    ))
}

/// Fills the send buffer until a write stays pending, drops it, then counts what the peer receives.
async fn write_drop() -> Result<String, String> {
    const CHUNK: usize = 256 * 1024;
    let (client, server) = pair().await?;
    let mut w = &client;
    let mut written = 0usize;
    let mut dropped = None;
    for i in 0..4096 {
        let data = Bytes::from(vec![(i % 251) as u8; CHUNK]);
        let sentinel = data.clone();
        match timeout(Duration::from_millis(50), w.write(data)).await {
            Ok(BufResult(Ok(n), _)) => written += n,
            Ok(BufResult(Err(e), _)) => return Err(format!("write: {e}")),
            Err(_) => {
                dropped = Some(sentinel);
                break;
            }
        }
    }
    let Some(sentinel) = dropped else {
        return Ok(format!("write: never blocked after {written} bytes; not verified"));
    };
    let held_right_after = !sentinel.is_unique();
    w.shutdown().await.map_err(|e| format!("shutdown: {e}"))?;
    let mut r = &server;
    let mut received = 0usize;
    loop {
        let BufResult(res, _) = r.read(Vec::with_capacity(1 << 20)).await;
        match res.map_err(|e| format!("drain: {e}"))? {
            0 => break,
            n => received += n,
        }
    }
    sleep(Duration::from_millis(20)).await;
    ensure!(sentinel.is_unique(), "the dropped write's buffer is still held after the peer drained");
    let extra = received as i64 - written as i64;
    Ok(format!(
        "write: a write pending on a full send buffer (after {written} bytes) was dropped; buffer held \
         right after drop: {held_right_after}, released later; peer received {received} bytes, \
         {extra} more than the completed writes reported{}",
        if extra != 0 { " (the dropped write's bytes went out partly or fully)" } else { "" }
    ))
}

/// Drops the `JoinHandle` of a task blocked in a read: compio cancels the task (unlike tokio, which
/// detaches it).
async fn task_drop() -> Result<String, String> {
    let (client, server) = pair().await?;
    let client = Rc::new(client);
    let resumed = Rc::new(Cell::new(false));
    let (buf, sentinel) = tracked_read_buf(64);
    let handle = spawn({
        let client = client.clone();
        let resumed = resumed.clone();
        async move {
            let mut r: &TcpStream = &client;
            let res = r.read(buf).await;
            resumed.set(true);
            res
        }
    });
    sleep(Duration::from_millis(5)).await;
    ensure!(!handle.is_finished(), "the read task finished with no data");
    drop(handle);
    sleep(Duration::from_millis(20)).await;
    ensure!(sentinel.is_unique(), "the cancelled task's read buffer is still held");
    round_trip(&server, &client, b"after task drop").await?;
    ensure!(!resumed.get(), "the task resumed after its JoinHandle was dropped");
    Ok("task: dropping a JoinHandle cancels the task and its in-flight read; buffer released, the \
        socket still works"
        .into())
}

/// A peer writes a stream of small records with short pauses while the loop reads it with reads
/// that are dropped after short timeouts. Reports how many bytes went missing: a read that
/// completed in the kernel and was then dropped loses its data.
async fn read_race(label: &str, use_biased: bool) -> Result<String, String> {
    const RECORDS: usize = 20_000;
    const RECORD: usize = 64;
    let std_listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
    let addr = std_listener.local_addr().map_err(|e| e.to_string())?;
    let (done_tx, done_rx) = mpsc::channel();
    let writer = thread::spawn(move || -> std::io::Result<usize> {
        let (mut s, _) = std_listener.accept()?;
        s.set_nodelay(true)?;
        let mut sent = 0;
        for i in 0..RECORDS {
            let rec = [(i % 251) as u8; RECORD];
            s.write_all(&rec)?;
            sent += RECORD;
            let until = Instant::now() + Duration::from_micros(20 + (i as u64 * 37) % 60);
            while Instant::now() < until {
                std::hint::spin_loop();
            }
        }
        let _ = done_tx.send(());
        Ok(sent)
    });
    let client = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
    let mut r = &client;
    let mut received = 0usize;
    let mut dropped = 0usize;
    let mut i = 0u64;
    loop {
        let d = Duration::from_micros([0, 30, 60, 100, 1000][(i % 5) as usize]);
        i += 1;
        let buf = Vec::with_capacity(4096);
        let res = if use_biased { biased(r.read(buf), sleep(d)).await } else { timeout(d, r.read(buf)).await.ok() };
        match res {
            Some(BufResult(Ok(0), _)) => break,
            Some(BufResult(Ok(n), _)) => received += n,
            Some(BufResult(Err(e), _)) => return Err(format!("read: {e}")),
            None => dropped += 1,
        }
        if done_rx.try_recv().is_ok() {
            // Writer finished; read the rest without timeouts.
            loop {
                let BufResult(res, _) = r.read(Vec::with_capacity(1 << 16)).await;
                match res.map_err(|e| format!("read: {e}"))? {
                    0 => break,
                    n => received += n,
                }
            }
            break;
        }
    }
    let sent = writer.join().map_err(|_| "writer panicked")?.map_err(|e| format!("writer: {e}"))?;
    let lost = sent - received;
    Ok(format!(
        "race, {label}: {i} reads, {dropped} dropped by the timer, {lost} of {sent} bytes lost{}",
        if lost > 0 { " (completed reads were dropped with their data)" } else { "" }
    ))
}

/// Drops a whole runtime while operations are in flight in detached tasks (what a loop does at
/// close if a task outlives it).
fn runtime_drop(cfg: &rt::Config) -> Result<String, String> {
    let (tx, rx) = mpsc::channel();
    let cfg = cfg.clone();
    thread::spawn(move || {
        let r = (|| -> Result<(Duration, usize), String> {
            let rt = rt::build(&cfg, "dropped runtime")?;
            let std_listener = std::net::TcpListener::bind("127.0.0.1:0").map_err(|e| e.to_string())?;
            let addr = std_listener.local_addr().map_err(|e| e.to_string())?;
            rt.block_on(async {
                let client = TcpStream::connect(addr).await.map_err(|e| e.to_string())?;
                let listener = TcpListener::bind("127.0.0.1:0").await.map_err(|e| e.to_string())?;
                spawn(async move {
                    let mut r = &client;
                    let _ = r.read(Vec::with_capacity(4096)).await;
                })
                .detach();
                spawn(async move {
                    let _ = listener.accept().await;
                })
                .detach();
                sleep(Duration::from_millis(5)).await;
                Ok::<_, String>(())
            })?;
            let (mut std_peer, _) = std_listener.accept().map_err(|e| e.to_string())?;
            let start = Instant::now();
            drop(rt);
            let took = start.elapsed();
            // The peer sees the client side closed.
            std_peer.set_read_timeout(Some(Duration::from_secs(2))).map_err(|e| e.to_string())?;
            // On io_uring, this thread (the ring's submitter) can get EINTR from a blocking
            // syscall after the ring is gone: count it and retry.
            let mut eintr = 0;
            let n = loop {
                match std_peer.read(&mut [0u8; 16]) {
                    Err(e) if e.kind() == std::io::ErrorKind::Interrupted => eintr += 1,
                    r => break r.map_err(|e| format!("peer read: {e}"))?,
                }
            };
            ensure!(n == 0, "peer read {n} bytes instead of EOF");
            Ok((took, eintr))
        })();
        let _ = tx.send(r);
    });
    let (took, eintr) = rx
        .recv_timeout(Duration::from_secs(10))
        .map_err(|_| "dropping a runtime with in-flight operations hung".to_string())??;
    Ok(format!(
        "runtime drop: a runtime with a pending read and accept in detached tasks dropped in {took:?}; \
         the peer saw EOF; a blocking read on the same thread right after the drop got EINTR {eintr} \
         time(s)"
    ))
}
