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

//! Check 3: timers. `sleep`, `timeout` and `interval` fire, never early and within a lenient bound
//! (CI runners and the Docker Desktop VM jitter), including many concurrent timers.

use std::{
    cell::RefCell,
    rc::Rc,
    time::{Duration, Instant},
};

use compio::{
    runtime::spawn,
    time::{interval, sleep, sleep_until, timeout},
};

use crate::{CheckResult, ensure, note, percentile, rt, summarize};

const CONCURRENT: usize = 10_000;
/// Overshoot bounds. Generous on purpose: this checks "sane", not "precise". The median catches a timer with a
/// coarse tick; the maximum only catches one that is broken, since shared CI machines (macOS runners especially)
/// stall a thread for tens of milliseconds now and then.
///
/// The median's bound is wider on macOS. Its runners measure a median of 0.4 to 2.5 ms, but one measured 7.5 ms for
/// ten 100 ms sleeps, with all ten late by 7 to 10 ms, against Linux's 0.15 ms at worst. compio's timer queue is the
/// same code on every platform, so the Linux runs keep the tight bound on it.
const P50_BOUND: Duration =
    if cfg!(target_os = "macos") { Duration::from_millis(20) } else { Duration::from_millis(5) };
const MAX_BOUND: Duration = Duration::from_millis(250);
/// With 10,000 timers the 99th percentile is meaningful.
const CONCURRENT_P99_BOUND: Duration = Duration::from_millis(50);

pub fn run(cfg: &rt::Config) -> CheckResult {
    let rt = rt::build(cfg, "loop")?;
    rt.block_on(async {
        // Sequential sleeps of several lengths.
        for d in [
            Duration::from_micros(100),
            Duration::from_millis(1),
            Duration::from_millis(5),
            Duration::from_millis(20),
            Duration::from_millis(100),
        ] {
            let reps = if d >= Duration::from_millis(100) { 10 } else { 50 };
            let mut over = Vec::with_capacity(reps);
            for _ in 0..reps {
                let start = Instant::now();
                sleep(d).await;
                let el = start.elapsed();
                ensure!(el >= d, "sleep({d:?}) returned early, after {el:?}");
                over.push(el - d);
            }
            let s = summarize(&mut over);
            note(format!("sleep({d:?}) overshoot: {s}"));
            ensure!(percentile(&over, 0.5) <= P50_BOUND, "sleep({d:?}) overshoot too high: {s}");
            ensure!(*over.last().unwrap() <= MAX_BOUND, "sleep({d:?}) overshoot too high: {s}");
        }

        // A zero or past deadline completes at once.
        let start = Instant::now();
        sleep(Duration::ZERO).await;
        sleep_until(Instant::now() - Duration::from_millis(5)).await;
        ensure!(start.elapsed() < Duration::from_millis(5), "zero sleeps took {:?}", start.elapsed());

        // timeout: fires on a future that never completes, not on one that does.
        let start = Instant::now();
        let r = timeout(Duration::from_millis(10), std::future::pending::<()>()).await;
        let el = start.elapsed();
        ensure!(r.is_err(), "timeout on a pending future did not fire");
        ensure!(el >= Duration::from_millis(10), "timeout fired early, after {el:?}");
        ensure!(el < Duration::from_millis(10) + MAX_BOUND, "timeout fired late, after {el:?}");
        let r = timeout(Duration::from_millis(500), sleep(Duration::from_millis(1))).await;
        ensure!(r.is_ok(), "timeout fired although the inner future completed first");
        note(format!("timeout(10ms, pending) elapsed after {el:?}; completes when the inner future wins"));

        // interval: 50 ticks of 10ms, check the total does not drift.
        let mut iv = interval(Duration::from_millis(10));
        let start = Instant::now();
        iv.tick().await; // the first tick is immediate
        for _ in 0..50 {
            iv.tick().await;
        }
        let el = start.elapsed();
        ensure!(el >= Duration::from_millis(500), "50 ticks of 10ms in {el:?}");
        note(format!("interval(10ms): 50 ticks in {el:?}"));
        ensure!(el < Duration::from_millis(500) + MAX_BOUND, "interval drifted: 50 ticks in {el:?}");

        // Many concurrent timers with spread deadlines, each in its own task.
        let lateness = Rc::new(RefCell::new(Vec::with_capacity(CONCURRENT)));
        let early = Rc::new(RefCell::new(0usize));
        let start = Instant::now();
        let mut handles = Vec::with_capacity(CONCURRENT);
        let mut seed = 0x9E37_79B9_7F4A_7C15u64;
        for _ in 0..CONCURRENT {
            seed ^= seed << 13;
            seed ^= seed >> 7;
            seed ^= seed << 17;
            let deadline = start + Duration::from_micros(seed % 1_000_000);
            let lateness = lateness.clone();
            let early = early.clone();
            handles.push(spawn(async move {
                sleep_until(deadline).await;
                let now = Instant::now();
                if now < deadline {
                    *early.borrow_mut() += 1;
                } else {
                    lateness.borrow_mut().push(now - deadline);
                }
            }));
        }
        for h in handles {
            h.await.map_err(|e| format!("timer task: {e:?}"))?;
        }
        let total = start.elapsed();
        ensure!(*early.borrow() == 0, "{} of {CONCURRENT} timers fired early", early.borrow());
        let mut lat = lateness.borrow_mut();
        ensure!(lat.len() == CONCURRENT, "{} of {CONCURRENT} timers fired", lat.len());
        let s = summarize(&mut lat);
        note(format!("{CONCURRENT} concurrent timers over 1s: all fired, none early, lateness {s}, total {total:?}"));
        ensure!(percentile(&lat, 0.99) <= CONCURRENT_P99_BOUND, "concurrent timers too late: {s}");
        ensure!(*lat.last().unwrap() <= MAX_BOUND, "concurrent timers too late: {s}");

        Ok(())
    })
}
