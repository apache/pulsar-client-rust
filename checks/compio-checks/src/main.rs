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

//! Milestone 0 checks of the compio behavior the client depends on.
//!
//! ```text
//! compio-checks [--driver auto|io-uring|polling] [--expect io-uring|polling] [CHECK...]
//! ```
//!
//! CHECK is one of `tls`, `wake`, `timers`, `cancel`, `dns`; all of them run when none is given.
//! Each check runs on its own thread with its own runtime, and fails if the driver it observes is
//! not the expected one.

mod cancel;
mod dns;
mod rt;
mod timers;
mod tls;
mod wake;

use std::{process::ExitCode, sync::mpsc, thread, time::Duration};

use rt::{Config, DriverChoice};

/// What a check returns: why it failed, if it did. Findings are printed with [`note`] as they come,
/// so the ones made before a failure are not lost.
pub type CheckResult = Result<(), String>;

pub fn note(s: impl std::fmt::Display) {
    println!("  note: {s}");
}

type CheckFn = fn(&Config) -> CheckResult;

const CHECKS: &[(&str, CheckFn)] =
    &[("tls", tls::run), ("wake", wake::run), ("timers", timers::run), ("cancel", cancel::run), ("dns", dns::run)];

/// A check that has not finished after this long is reported as hung.
const CHECK_DEADLINE: Duration = Duration::from_secs(120);

fn usage() -> ExitCode {
    eprintln!(
        "usage: compio-checks [--driver auto|io-uring|polling] [--expect io-uring|polling] [CHECK...]\n\
         checks: {}",
        CHECKS.iter().map(|(n, _)| *n).collect::<Vec<_>>().join(", ")
    );
    ExitCode::from(2)
}

fn main() -> ExitCode {
    let mut requested = DriverChoice::Auto;
    let mut expected = None;
    let mut selected = Vec::new();
    let mut args = std::env::args().skip(1);
    while let Some(arg) = args.next() {
        match arg.as_str() {
            "--driver" => match args.next().as_deref().and_then(DriverChoice::parse) {
                Some(d) => requested = d,
                None => return usage(),
            },
            "--expect" => match args.next().as_deref().and_then(DriverChoice::parse) {
                Some(d) if d != DriverChoice::Auto => expected = Some(d),
                _ => return usage(),
            },
            "-h" | "--help" => return usage(),
            name => match CHECKS.iter().find(|(n, _)| *n == name) {
                Some(check) => selected.push(*check),
                None => return usage(),
            },
        }
    }
    if selected.is_empty() {
        selected = CHECKS.to_vec();
    }
    let config = Config::new(requested, expected);
    println!(
        "compio-checks on {}/{}: requested driver {}, expected {}",
        std::env::consts::OS,
        std::env::consts::ARCH,
        config.requested.name(),
        rt::driver_label(config.expected)
    );

    let mut results = Vec::new();
    for (name, check) in selected {
        println!("\n=== {name} ===");
        let (tx, rx) = mpsc::channel();
        let cfg = config.clone();
        thread::Builder::new()
            .name(format!("check-{name}"))
            .spawn(move || {
                let result = std::panic::catch_unwind(|| check(&cfg))
                    .unwrap_or_else(|p| Err(format!("panicked: {}", panic_message(&p))));
                let _ = tx.send(result);
            })
            .expect("spawn check thread");
        let result = match rx.recv_timeout(CHECK_DEADLINE) {
            Ok(r) => r,
            Err(_) => Err(format!("did not finish within {CHECK_DEADLINE:?} (hung)")),
        };
        match &result {
            Ok(()) => println!("--> {name}: PASS"),
            Err(e) => println!("--> {name}: FAIL: {e}"),
        }
        results.push((name, result));
    }

    println!("\n=== summary (driver {}) ===", rt::driver_label(config.expected));
    let mut failed = false;
    for (name, result) in &results {
        match result {
            Ok(_) => println!("PASS  {name}"),
            Err(e) => {
                failed = true;
                println!("FAIL  {name}: {e}");
            }
        }
    }
    // A hung check thread would keep the process alive; exit explicitly.
    let code = if failed { 1 } else { 0 };
    std::process::exit(code);
}

fn panic_message(p: &Box<dyn std::any::Any + Send>) -> String {
    if let Some(s) = p.downcast_ref::<&str>() {
        s.to_string()
    } else if let Some(s) = p.downcast_ref::<String>() {
        s.clone()
    } else {
        "unknown panic payload".to_string()
    }
}

/// Fails the check with a formatted message unless the condition holds.
#[macro_export]
macro_rules! ensure {
    ($cond:expr, $($fmt:tt)+) => {
        if !$cond {
            return Err(format!($($fmt)+));
        }
    };
}

/// Percentile of an already sorted slice.
pub fn percentile(sorted: &[Duration], p: f64) -> Duration {
    if sorted.is_empty() {
        return Duration::ZERO;
    }
    let idx = ((sorted.len() - 1) as f64 * p).round() as usize;
    sorted[idx]
}

/// Formats p50/p99/max of a set of durations.
pub fn summarize(samples: &mut [Duration]) -> String {
    samples.sort();
    format!(
        "n={} p50={:?} p99={:?} max={:?}",
        samples.len(),
        percentile(samples, 0.5),
        percentile(samples, 0.99),
        samples.last().copied().unwrap_or_default()
    )
}
