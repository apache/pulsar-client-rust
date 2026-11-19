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

//! Check 5: DNS resolution through `compio::net::ToSocketAddrsAsync`. Only `localhost` and IP
//! literals are resolved, so nothing leaves the machine.
//!
//! compio-net 0.12 resolves names on Unix with `std::net::ToSocketAddrs` (getaddrinfo) inside
//! `compio::runtime::spawn_blocking`, which runs on the runtime's own blocking pool (the driver's
//! `AsyncifyPool`: unnamed threads, up to 256 per runtime, idle ones exit after 60s). This check
//! backs that reading with what it can observe.

use std::{
    cell::Cell,
    net::{IpAddr, SocketAddr},
    rc::Rc,
    thread,
    time::{Duration, Instant},
};

use compio::{
    net::ToSocketAddrsAsync,
    runtime::{spawn, spawn_blocking},
    time::sleep,
};
use futures::future::join_all;

use crate::{CheckResult, ensure, note, rt};

const CONCURRENT: usize = 32;

/// Threads in this process, where the OS makes that easy to read.
fn thread_count() -> Option<usize> {
    std::fs::read_dir("/proc/self/task").ok().map(|d| d.count())
}

async fn resolve(host: &str, port: u16) -> Result<Vec<SocketAddr>, String> {
    (host, port).to_socket_addrs_async().await.map(|it| it.collect()).map_err(|e| format!("resolve {host}: {e}"))
}

pub fn run(cfg: &rt::Config) -> CheckResult {
    let rt = rt::build(cfg, "loop")?;
    rt.block_on(async {
        let loop_thread = thread::current().id();

        let threads_before = thread_count();
        let start = Instant::now();
        let addrs = resolve("localhost", 6650).await?;
        let took = start.elapsed();
        ensure!(!addrs.is_empty(), "localhost resolved to nothing");
        ensure!(
            addrs.iter().all(|a| a.ip().is_loopback() && a.port() == 6650),
            "localhost resolved to non-loopback addresses: {addrs:?}"
        );
        note(format!("(\"localhost\", 6650) -> {addrs:?} in {took:?}"));

        let addrs: Vec<_> = "localhost:6651"
            .to_socket_addrs_async()
            .await
            .map_err(|e| format!("resolve localhost:6651: {e}"))?
            .collect();
        ensure!(addrs.iter().all(|a| a.port() == 6651), "bad port in {addrs:?}");

        // IP literals are parsed on the loop, without the pool.
        let start = Instant::now();
        let addrs = resolve("127.0.0.1", 6650).await?;
        ensure!(addrs == [SocketAddr::from(([127, 0, 0, 1], 6650))], "127.0.0.1 resolved to {addrs:?}");
        let addrs = resolve("::1", 6650).await?;
        ensure!(
            addrs.len() == 1 && addrs[0].ip() == IpAddr::from(std::net::Ipv6Addr::LOCALHOST),
            "::1 resolved to {addrs:?}"
        );
        note(format!("IP literals resolved inline in {:?}", start.elapsed()));

        // Where blocking work runs: the same pool `resolve_sock_addrs` uses.
        let (pool_thread, pool_name) = spawn_blocking(|| {
            let t = thread::current();
            (t.id(), t.name().map(str::to_string))
        })
        .await
        .map_err(|e| format!("spawn_blocking: {e:?}"))?;
        ensure!(pool_thread != loop_thread, "blocking work ran on the loop thread");
        note(format!("blocking pool thread: {pool_thread:?} name {pool_name:?}; loop thread: {loop_thread:?}"));

        // Many concurrent resolutions while a ticker on the loop measures stalls.
        let max_gap = Rc::new(Cell::new(Duration::ZERO));
        let stop = Rc::new(Cell::new(false));
        let ticker = spawn({
            let max_gap = max_gap.clone();
            let stop = stop.clone();
            async move {
                let mut last = Instant::now();
                while !stop.get() {
                    sleep(Duration::from_millis(1)).await;
                    let now = Instant::now();
                    max_gap.set(max_gap.get().max(now - last));
                    last = now;
                }
            }
        });
        let start = Instant::now();
        let results = join_all((0..CONCURRENT).map(|_| resolve("localhost", 6650))).await;
        let took = start.elapsed();
        let threads_during = thread_count();
        stop.set(true);
        ticker.await.map_err(|e| format!("ticker: {e:?}"))?;
        for r in results {
            r?;
        }
        note(format!(
            "{CONCURRENT} concurrent resolutions of localhost in {took:?}; longest gap between 1ms \
             ticks on the loop meanwhile: {:?}",
            max_gap.get()
        ));
        if let (Some(b), Some(d)) = (threads_before, threads_during) {
            note(format!(
                "process threads: {b} before the first resolution, {d} after the concurrent ones \
                 (pool threads linger up to 60s)"
            ));
        }
        ensure!(max_gap.get() < Duration::from_millis(100), "the loop stalled for {:?}", max_gap.get());

        // Dropping a resolution in flight: the blocking call keeps running on its pool thread; the
        // loop must not care.
        let fut = resolve("localhost", 6650);
        let dropped = futures::future::select(Box::pin(fut), Box::pin(async {})).await;
        drop(dropped);
        sleep(Duration::from_millis(20)).await;
        let addrs = resolve("localhost", 6650).await?;
        ensure!(!addrs.is_empty(), "resolution after a dropped one failed");
        note("a resolution dropped in flight leaves the runtime usable");

        Ok(())
    })
}
