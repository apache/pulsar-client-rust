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

//! Building runtimes with a chosen driver, and asserting which driver is actually running.

use compio::{
    driver::{DriverType, ProactorBuilder},
    runtime::{Runtime, RuntimeBuilder},
};

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub enum DriverChoice {
    /// Let compio pick: on Linux with the fusion driver, io_uring with a silent fallback to polling.
    Auto,
    IoUring,
    Polling,
}

impl DriverChoice {
    pub fn parse(s: &str) -> Option<Self> {
        match s {
            "auto" => Some(Self::Auto),
            "io-uring" | "io_uring" | "iouring" => Some(Self::IoUring),
            "polling" | "poll" => Some(Self::Polling),
            _ => None,
        }
    }

    pub fn name(self) -> &'static str {
        match self {
            Self::Auto => "auto",
            Self::IoUring => "io-uring",
            Self::Polling => "polling",
        }
    }
}

#[derive(Debug, Clone)]
pub struct Config {
    pub requested: DriverChoice,
    /// The driver every runtime must report; never `Auto`.
    pub expected: DriverChoice,
}

impl Config {
    pub fn new(requested: DriverChoice, expected: Option<DriverChoice>) -> Self {
        let expected = expected.unwrap_or(match requested {
            DriverChoice::Auto if cfg!(target_os = "linux") => DriverChoice::IoUring,
            DriverChoice::Auto => DriverChoice::Polling,
            other => other,
        });
        Self { requested, expected }
    }
}

/// Human-readable name of a driver on this platform. compio's `DriverType::Poll` is the `polling`
/// crate, which is epoll on Linux and kqueue on macOS.
pub fn driver_label(d: DriverChoice) -> &'static str {
    match d {
        DriverChoice::IoUring => "io_uring",
        DriverChoice::Polling if cfg!(target_os = "linux") => "polling (epoll)",
        DriverChoice::Polling if cfg!(target_os = "macos") => "polling (kqueue)",
        DriverChoice::Polling => "polling",
        DriverChoice::Auto => "auto",
    }
}

pub fn observed(rt: &Runtime) -> Result<DriverChoice, String> {
    match rt.driver_type() {
        DriverType::IoUring => Ok(DriverChoice::IoUring),
        DriverType::Poll => Ok(DriverChoice::Polling),
        other => Err(format!("unexpected driver {other:?}")),
    }
}

/// Builds a runtime with the requested driver, prints the driver it reports, and fails unless it is
/// the expected one. Every runtime in the checks goes through here.
pub fn build(cfg: &Config, what: &str) -> Result<Runtime, String> {
    let mut proactor = ProactorBuilder::new();
    match cfg.requested {
        DriverChoice::Auto => {}
        DriverChoice::IoUring => {
            proactor.driver_type(DriverType::IoUring);
        }
        DriverChoice::Polling => {
            proactor.driver_type(DriverType::Poll);
        }
    }
    let rt = RuntimeBuilder::new()
        .with_proactor(proactor)
        .build()
        .map_err(|e| format!("building the {what} runtime ({}): {e}", cfg.requested.name()))?;
    let got = observed(&rt)?;
    println!(
        "  [{what}] driver: requested {}, observed {:?} = {}",
        cfg.requested.name(),
        rt.driver_type(),
        driver_label(got)
    );
    if got != cfg.expected {
        return Err(format!(
            "{what} runtime uses {} but {} was expected",
            driver_label(got),
            driver_label(cfg.expected)
        ));
    }
    Ok(rt)
}
