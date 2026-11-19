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

//! Exponential backoff with jitter, as the Java client's `Backoff`: 100 ms doubling up to 60 s, with ±5% jitter.

use std::time::Duration;

/// The delays between reconnect attempts.
#[derive(Debug, Clone)]
pub struct Backoff {
    initial: Duration,
    max: Duration,
    next: Duration,
}

impl Default for Backoff {
    fn default() -> Self {
        Self::new(Duration::from_millis(100), Duration::from_secs(60))
    }
}

impl Backoff {
    pub fn new(initial: Duration, max: Duration) -> Self {
        Self { initial, max, next: initial }
    }

    /// The next delay. `random` is uniform in `[0, 1)`; it spreads the delay over ±5% so that clients that lost
    /// a broker together don't come back together.
    pub fn next_delay(&mut self, random: f64) -> Duration {
        let current = self.next;
        self.next = (self.next * 2).min(self.max);
        current.mul_f64(1.0 + (random.clamp(0.0, 1.0) - 0.5) * 0.1)
    }

    /// Starts over, after a success.
    pub fn reset(&mut self) {
        self.next = self.initial;
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn doubles_up_to_the_max() {
        let mut backoff = Backoff::default();
        let delays: Vec<u128> = (0..12).map(|_| backoff.next_delay(0.5).as_millis()).collect();
        assert_eq!(delays, [100, 200, 400, 800, 1600, 3200, 6400, 12800, 25600, 51200, 60000, 60000]);
        backoff.reset();
        assert_eq!(backoff.next_delay(0.5), Duration::from_millis(100));
    }

    #[test]
    fn jitter_stays_within_five_percent() {
        assert_eq!(Backoff::default().next_delay(0.0), Duration::from_millis(95));
        assert_eq!(Backoff::default().next_delay(1.0), Duration::from_millis(105));
    }
}
