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

//! Check 2: waking a loop from another thread.
//!
//! The receivers are spawned tasks, so a wake goes through compio's remote-schedule path (the
//! executor's sync queue plus a driver notification), not the `block_on` root waker. While they wait,
//! no timer is pending on the runtime, so the driver blocks with no timeout: if a cross-thread wake
//! did not interrupt it, the check would hang and the watchdog would fail it.

use std::{
    thread,
    time::{Duration, Instant},
};

use compio::runtime::{Runtime, spawn};
use futures::{StreamExt, channel::oneshot};

use crate::{CheckResult, ensure, note, rt, summarize};

const SAMPLES: usize = 300;
const BURST: usize = 200;
const PING_PONG: usize = 1000;

/// Gaps between sends, so the loop goes back to sleeping in the driver every time.
fn gap(i: usize) -> Duration {
    Duration::from_micros(1000 + (i as u64 * 7919) % 3000)
}

/// True when the runtime has no timer pending, i.e. it will poll the driver with no timeout.
fn no_timer_pending() -> bool {
    Runtime::with_current(|r| r.current_timeout()).is_none()
}

pub fn run(cfg: &rt::Config) -> CheckResult {
    let rt = rt::build(cfg, "loop")?;

    // A: flume, std thread -> spawned task.
    let (tx, rx) = flume::unbounded::<Instant>();
    let sender = thread::spawn(move || {
        for i in 0..SAMPLES {
            thread::sleep(gap(i));
            tx.send(Instant::now()).unwrap();
        }
    });
    let (mut lat, timer_seen) = rt.block_on(async {
        spawn(async move {
            let mut lat = Vec::with_capacity(SAMPLES);
            let mut timer_seen = false;
            loop {
                timer_seen |= !no_timer_pending();
                match rx.recv_async().await {
                    Ok(sent) => lat.push(sent.elapsed()),
                    Err(_) => break,
                }
            }
            (lat, timer_seen)
        })
        .await
        .map_err(|e| format!("receiver task: {e:?}"))
    })?;
    sender.join().map_err(|_| "sender panicked")?;
    ensure!(!timer_seen, "a timer was pending while waiting; the check would not prove anything");
    ensure!(lat.len() == SAMPLES, "received {} of {SAMPLES}", lat.len());
    let s = summarize(&mut lat);
    let p99 = crate::percentile(&lat, 0.99);
    note(format!("flume std-thread -> idle loop wake latency: {s}"));
    ensure!(p99 < Duration::from_millis(10), "flume wake p99 too high: {s}");

    // B: futures::channel::mpsc, same shape.
    let (tx, mut rx) = futures::channel::mpsc::unbounded::<Instant>();
    let sender = thread::spawn(move || {
        for i in 0..SAMPLES {
            thread::sleep(gap(i));
            tx.unbounded_send(Instant::now()).unwrap();
        }
    });
    let mut lat = rt.block_on(async {
        spawn(async move {
            let mut lat = Vec::with_capacity(SAMPLES);
            while let Some(sent) = rx.next().await {
                lat.push(sent.elapsed());
            }
            lat
        })
        .await
        .map_err(|e| format!("receiver task: {e:?}"))
    })?;
    sender.join().map_err(|_| "sender panicked")?;
    ensure!(lat.len() == SAMPLES, "received {} of {SAMPLES}", lat.len());
    let s = summarize(&mut lat);
    let p99 = crate::percentile(&lat, 0.99);
    note(format!("futures mpsc std-thread -> idle loop wake latency: {s}"));
    ensure!(p99 < Duration::from_millis(10), "futures mpsc wake p99 too high: {s}");

    // C: the facade's hop: a request over flume, the answer over a oneshot that a plain thread
    // awaits with a runtime-agnostic executor.
    let (req_tx, req_rx) = flume::unbounded::<(u64, oneshot::Sender<u64>)>();
    let caller = thread::spawn(move || {
        let mut rtt = Vec::with_capacity(SAMPLES);
        for i in 0..SAMPLES as u64 {
            thread::sleep(gap(i as usize));
            let (otx, orx) = oneshot::channel();
            let start = Instant::now();
            req_tx.send((i, otx)).unwrap();
            let v = futures::executor::block_on(orx).expect("reply");
            rtt.push(start.elapsed());
            assert_eq!(v, i + 1);
        }
        rtt
    });
    rt.block_on(async {
        spawn(async move {
            while let Ok((v, reply)) = req_rx.recv_async().await {
                let _ = reply.send(v + 1);
            }
        })
        .await
        .map_err(|e| format!("server task: {e:?}"))
    })?;
    let mut rtt = caller.join().map_err(|_| "caller panicked")?;
    let s = summarize(&mut rtt);
    note(format!("round trip thread -> loop -> thread (flume + oneshot, block_on): {s}"));
    ensure!(crate::percentile(&rtt, 0.99) < Duration::from_millis(20), "hop p99 too high: {s}");

    // D: one remote burst waking more tasks than the executor's sync queue holds (64).
    let (ready_tx, ready_rx) = flume::bounded::<oneshot::Sender<Instant>>(BURST);
    let elapsed = rt.block_on(async move {
        let mut handles = Vec::with_capacity(BURST);
        for _ in 0..BURST {
            let (otx, orx) = oneshot::channel::<Instant>();
            ready_tx.send(otx).unwrap();
            handles.push(spawn(async move { orx.await.map(|t| t.elapsed()) }));
        }
        drop(ready_tx);
        let waker = thread::spawn(move || {
            // Let the tasks park first.
            thread::sleep(Duration::from_millis(50));
            let senders: Vec<_> = ready_rx.iter().collect();
            let now = Instant::now();
            for s in senders {
                let _ = s.send(now);
            }
        });
        let mut worst = Duration::ZERO;
        for h in handles {
            let d =
                h.await.map_err(|e| format!("burst task: {e:?}"))?.map_err(|_| "burst sender dropped".to_string())?;
            worst = worst.max(d);
        }
        waker.join().map_err(|_| "burst waker panicked")?;
        Ok::<_, String>(worst)
    })?;
    note(format!("burst of {BURST} remote wakes (sync queue is 64): all done, slowest after {elapsed:?}"));
    ensure!(elapsed < Duration::from_secs(1), "burst took {elapsed:?}");

    // E: loop to loop ping-pong over flume.
    let (ping_tx, ping_rx) = flume::bounded::<Instant>(1);
    let (pong_tx, pong_rx) = flume::bounded::<Instant>(1);
    let peer_cfg = cfg.clone();
    let peer = thread::spawn(move || -> Result<(), String> {
        let rt = rt::build(&peer_cfg, "peer loop")?;
        rt.block_on(async move {
            spawn(async move {
                while let Ok(t) = ping_rx.recv_async().await {
                    if pong_tx.send_async(t).await.is_err() {
                        break;
                    }
                }
            })
            .await
            .map_err(|e| format!("peer task: {e:?}"))
        })
    });
    let mut rtt = rt.block_on(async move {
        spawn(async move {
            let mut rtt = Vec::with_capacity(PING_PONG);
            for _ in 0..PING_PONG {
                ping_tx.send_async(Instant::now()).await.unwrap();
                let t = pong_rx.recv_async().await.unwrap();
                rtt.push(t.elapsed());
            }
            rtt
        })
        .await
        .map_err(|e| format!("ping task: {e:?}"))
    })?;
    peer.join().map_err(|_| "peer loop panicked")??;
    let s = summarize(&mut rtt);
    note(format!("loop <-> loop ping-pong over flume (back-to-back): {s}"));
    ensure!(crate::percentile(&rtt, 0.99) < Duration::from_millis(20), "loop-to-loop p99 too high: {s}");

    Ok(())
}
