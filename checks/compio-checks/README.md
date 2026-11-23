<!--
Licensed to the Apache Software Foundation (ASF) under one
or more contributor license agreements.  See the NOTICE file
distributed with this work for additional information
regarding copyright ownership.  The ASF licenses this file
to you under the Apache License, Version 2.0 (the
"License"); you may not use this file except in compliance
with the License.  You may obtain a copy of the License at

  http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing,
software distributed under the License is distributed on an
"AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
KIND, either express or implied.  See the License for the
specific language governing permissions and limitations
under the License.
-->

# compio checks (milestone 0)

Small programs that check the compio 0.19 behavior the client depends on ([PLAN.md](../../PLAN.md) M0,
[docs/2-architecture.md](../../docs/2-architecture.md) §4 and §5). They run on three setups: Linux with
io_uring, Linux with the polling driver (epoll), and macOS (polling driver, kqueue).

| Check | What it does |
|---|---|
| `tls` | A rustls client and server through `compio::tls` (compio-tls 0.10) on two loop threads, with a self-signed cert made at run time by rcgen. Covers handshake, ALPN, a 1 MiB echo in 16 KiB round trips, close_notify, and a client with an empty root store that must be rejected. |
| `wake` | A plain thread wakes a spawned task on an idle loop that has no timer pending, so the driver blocks with no timeout. Tests flume and `futures::mpsc`, plus the facade hop (flume request, oneshot reply awaited with `futures::executor::block_on`). Also a burst of 200 remote wakes, more than the executor's 64-slot sync queue, and loop-to-loop ping-pong. |
| `timers` | `sleep` from 100 µs to 100 ms (never early, overshoot bounded), zero and past deadlines, `timeout` both ways, 50 `interval` ticks, and 10,000 concurrent timers in their own tasks. |
| `cancel` | Drops in-flight reads, accepts, connects and writes, drops a `JoinHandle`, and drops a runtime with operations pending. Asserts that the runtime and sockets still work and that the driver releases every buffer. Also measures data lost when a read is dropped while data is arriving. |
| `dns` | `ToSocketAddrsAsync` on `localhost` and on IP literals. Shows which thread the work runs on and whether the loop stalls during 32 concurrent resolutions. Also drops a resolution while it is in flight. Nothing leaves the machine. |

## Driver selection, and how it is asserted

Every runtime is built through `rt::build`, which:

1. applies `--driver`;
2. reads `Runtime::driver_type()`;
3. prints `requested …, observed …`;
4. fails the check unless the driver is the expected one.

`--driver` takes `auto`, `io-uring` or `polling` and calls `ProactorBuilder::driver_type`. With `auto`, the
expected driver defaults to io_uring on Linux and kqueue polling elsewhere; override it with
`--expect io-uring|polling`. compio reports `DriverType::Poll` for both epoll and kqueue, so the label comes from
the target OS.

What compio 0.19 does (compio-driver 0.12.5; observed below):

- **The `polling` feature is on (this crate's default).** Linux gets the *fusion* driver. With nothing forced, it
  tries io_uring and **falls back to epoll silently**: only a `warn!` through compio-log, which logs nothing
  unless compio's `enable_log` feature is on. Forcing `io-uring` has no fallback: building the runtime fails
  with `EPERM` when io_uring is blocked. Forcing `polling` gives epoll.
- **The `polling` feature is off (`--no-default-features`, compio's default features: io_uring only on
  Linux).** Building a runtime fails with `EPERM` when io_uring is blocked. `driver_type(Poll)` is **silently
  ignored**: the runtime runs io_uring anyway.
- **macOS.** Always the `polling` crate on kqueue. `driver_type` is ignored.

## Running

Build on the host first, so that `Cargo.lock` is current and `--locked` works with the repository mounted
read-only in Docker.

```sh
# macOS (or any host), natively
cargo run --release -p compio-checks                       # all checks, auto driver
cargo run --release -p compio-checks -- --driver polling tls cancel

# Linux, io_uring. Docker's default seccomp profile blocks io_uring, so lift it.
docker run --rm --security-opt seccomp=unconfined \
  -v "$PWD":/src:ro -v compio-checks-target:/target -v compio-checks-registry:/usr/local/cargo/registry \
  -e CARGO_TARGET_DIR=/target -w /src rust:1 \
  sh -c 'cargo build --locked --release -p compio-checks && /target/release/compio-checks --driver io-uring'

# Linux, polling (epoll) forced, with io_uring available, to show forcing works
docker run --rm --security-opt seccomp=unconfined -v compio-checks-target:/target rust:1 \
  /target/release/compio-checks --driver polling

# Linux, default seccomp: the silent fallback (fails, then passes with the right expectation)
docker run --rm -v compio-checks-target:/target rust:1 /target/release/compio-checks --driver auto tls
docker run --rm -v compio-checks-target:/target rust:1 /target/release/compio-checks --driver auto --expect polling

# Linux, io_uring-only build (no `polling` feature), in its own target dir
docker run --rm -v "$PWD":/src:ro -v compio-checks-target:/target \
  -v compio-checks-registry:/usr/local/cargo/registry -e CARGO_TARGET_DIR=/target/no-polling -w /src rust:1 \
  sh -c 'cargo build --locked --release -p compio-checks --no-default-features && /target/no-polling/release/compio-checks tls'
```

The process exits non-zero if any check fails. Each check runs on its own thread under a 120 s watchdog, so a
missed wake shows up as a failure, not a hang.

## Results

These were run on 2026-11-18 on two machines:

- **macOS:** macOS 26.6 on arm64, Rust 1.99.0.
- **Linux:** Docker Desktop 29.9.2 on that Mac. That is a linuxkit VM (kernel 7.0.14, aarch64) running the
  `rust:1` image (Rust 1.100.0, Debian trixie).

Nothing ran on a bare-metal Linux host or on x86_64.

| Check | Linux io_uring (`--driver io-uring`, seccomp unconfined) | Linux epoll (`--driver polling`) | macOS kqueue (`auto`) |
|---|---|---|---|
| driver observed | `IoUring` on all 8 runtimes | `Poll` (epoll) on all 8 | `Poll` (kqueue) on all 8 |
| tls | PASS. Handshake 0.29 ms, 1 MiB echo 3.7 ms, untrusted cert rejected | PASS. 0.28 ms, 5.2 ms | PASS. 0.58 ms, 7.1 ms |
| wake | PASS. Idle-loop wake p50 41 µs, p99 65 µs. Hop RTT p50 74 µs. Loop-to-loop p50 37 µs. Burst of 200 done in 0.12 ms | PASS. p50 43 µs, p99 86 µs. Hop p50 75 µs. Loop-to-loop p50 37 µs | PASS. p50 16 µs, p99 41 µs. Hop p50 21 µs. Loop-to-loop p50 8 µs. Burst 0.14 ms |
| timers | PASS. Never early. Overshoot p50 40 µs (100 µs sleep) to 2 ms (20 ms sleep), max 5 ms. 10k timers: p50 32 µs late, p99 178 µs, max 8.2 ms | PASS. Same profile, max 4.9 ms. 10k: p99 189 µs, max 7.6 ms | PASS. Overshoot about 1 ms for sleeps of 20 ms and more. 10k: p99 90 µs, max 7.8 ms |
| cancel | PASS. Read, accept, connect, write and task drops OK, no buffer held afterwards. **Race: `timeout` lost about 74 KB of 1.28 MB; biased select lost 3.8 to 5.5 KB** (5 runs). Runtime drop OK; **EINTR on the next blocking read on that thread** (every run) | PASS. Same. Race: `timeout` lost about 78 KB; **biased lost 0** | PASS. Same. Race: `timeout` lost about 78 KB; **biased lost 0** (4 runs) |
| dns | PASS. `localhost` resolves to `[::1, 127.0.0.1]` in 0.26 ms on an unnamed pool thread. 32 concurrent in 0.64 ms, loop ticks kept going (max gap 1.13 ms). Process threads 4 to 12 | PASS. Same; threads 3 to 14 | PASS. First resolution 1.3 to 23 ms across runs; 32 concurrent in 4.0 ms, max gap 1.13 ms |

Driver-selection runs:

| Setup | Observed | Result |
|---|---|---|
| `--driver auto`, seccomp unconfined | io_uring | all PASS |
| `--driver auto`, default seccomp | **epoll, silently** | FAIL with the default expectation (`tls server runtime uses polling (epoll) but io_uring was expected`); all PASS with `--expect polling` |
| `--driver io-uring`, default seccomp | – | FAIL: `building the tls server runtime (io-uring): Operation not permitted (os error 1)` |
| `--no-default-features`, `auto`, default seccomp | – | FAIL: `Operation not permitted (os error 1)` at runtime build |
| `--no-default-features`, `--driver polling`, unconfined | **io_uring** (request ignored) | FAIL: `tls server runtime uses io_uring but polling (epoll) was expected` |
| macOS, `--driver io-uring` | kqueue (request ignored) | FAIL: `tls server runtime uses polling (kqueue) but io_uring was expected` |

All six failures in the second table are intended. They show what each configuration does and that the
assertion catches it.

## Findings that bear on the design

1. **A dropped read can lose data that was already read.**
   - **With `compio::time::timeout` or `select!`, on every driver.** `compio::time::timeout` is built on
     `futures::select!`, which picks a random branch when both are ready. When the timer and the read complete
     in the same poll, about half the time the timer wins. The completed read result is then dropped with its
     bytes (about 6% of the stream in the stress test).
   - **On io_uring, even a read-first (biased) select loses data** (0.3 to 0.4%). The kernel can complete the
     recv after the last reap but before `AsyncCancel` is processed. The CQE is then reaped and discarded.
     Polling never loses data with a biased select.
   - Consequences:
     - A connection must have one long-lived read loop that is never wrapped in `timeout` or `select` with
       anything that can win.
     - Keepalive and request timeouts must be separate timers that never cancel the read.
     - The same holds for writes: a dropped `write_all` in the middle of a frame corrupts the framing. In these
       runs a dropped write that was blocked on a full buffer sent nothing extra, but the race was not stressed.
       Writes belong to one task that owns the write half.
   - This settles §5's "what happens to an in-flight operation when its future is dropped".
2. **Buffers are safe on cancel.** On every driver, the buffer of a dropped operation stays owned by the driver
   until the cancellation completes (2000 out of 2000 were still held right after the drop). They were all
   released within 50 ms (20 ms in the write and task cases), with no leak over 2000 cancels. This matches rule 4 of §4.4.
3. **Dropping a `JoinHandle` cancels the task** (tokio detaches it instead). Keeping a task alive takes an
   explicit `.detach()` or a stored handle. That fits "every spawned task belongs to an owner", but a
   carelessly dropped handle silently kills the task.
4. **The driver choice must be explicit.**
   - Without the `polling` feature, a blocked io_uring makes runtime creation fail, and a request for the
     polling driver is ignored.
   - With it, `auto` silently degrades to epoll.
   - Proposal: enable compio's `polling` feature in the client, make the driver a client option
     (auto, io_uring, polling), and log `Runtime::driver_type()` when each loop starts.
   - Containers under Docker's default seccomp (and many Kubernetes setups) will run epoll.
   - Cargo unifies features across a workspace build. This crate's `polling` feature turns on fusion for
     every crate in `cargo build --workspace`, but `apache-pulsar` built alone gets io_uring only. The client
     crate must enable `polling` itself to behave the same either way.
5. **Cross-thread wakes are prompt and do not wait for a timer.** They arrive in tens of microseconds through
   the executor's sync queue plus the driver's notifier (an eventfd on Linux for both drivers, an `EVFILT_USER` event
   on macOS). This holds even when the loop is blocked with no timeout. A burst larger than the 64-slot sync queue
   works.
   - One note from the source, not measured: when that queue is full, the waking thread spins (yields) until the
     loop drains it. A caller thread can stall behind a busy loop. Admission (§4.1) bounds this.
6. **Timers.** No timer fired early in any run, and overshoot is sub-millisecond to a few milliseconds (more on
   the Docker VM). compio keeps one `BTreeMap` timer queue per runtime, and 10,000 timers are fine. The few
   multi-millisecond outliers in the 10k test are the earliest deadlines, delayed while the loop was still
   spawning the 10,000 tasks.
7. **DNS runs on the runtime's own blocking pool.** compio-net calls `std::net::ToSocketAddrs` (getaddrinfo)
   inside `spawn_blocking`, on the driver's `AsyncifyPool`.
   - That pool is per runtime: up to 256 threads, which exit after 60 s idle or when the runtime is dropped.
     Unnamed threads were observed; the rest is from the source.
   - The loop is not blocked while it resolves.
   - Design-relevant, from the source:
     - A resolution cannot be cancelled. A dropped one keeps its pool thread until getaddrinfo returns.
     - When all 256 threads are busy, the loop thread itself spins on `yield_now` until one frees up, which
       breaks "never block a loop".
     - With N loops there can be N × 256 threads.
   - Options: `ProactorBuilder::reuse_thread_pool` with one shared, bounded pool; or a resolver of our own with
     a cap on concurrent lookups and a timeout.
8. **io_uring and EINTR after a runtime drop.** After an io_uring runtime was dropped with operations pending,
   a blocking `read` on the same thread returned `EINTR` once (every run, io_uring only). The likely cause, not
   confirmed, is task work queued to the submitting thread by the cancellations. Loop threads must not block
   anyway, but shutdown code that runs on a former loop thread must retry on `EINTR`. `std`'s `read` does not.
9. **TLS dependencies.**
   - This crate uses compio-tls through compio's `rustls` and `ring` features. compio declares compio-tls with
     `default-features = false`, so neither native-tls nor OpenSSL is pulled in.
   - The root `compio-tls = "0.10.0"` entry keeps compio-tls's default `native-tls` feature (OpenSSL on Linux).
     A member that uses it with `workspace = true` cannot turn that off.
   - rustls needs an explicit crypto provider (`ring` here).

## Not verified

- **Bare-metal Linux and x86_64.** Linux ran only in Docker Desktop's VM on aarch64. The CI snippet below has
  not been run.
- **No use-after-free.** Safe code cannot prove it, and ASan would not see the kernel writing into a freed
  buffer. What the checks do show is that the driver holds each buffer until its operation completes or is
  cancelled.
- **The write-side cancel race.** It was not stressed: does a dropped `send` that completes in the kernel after
  the cancel still put bytes on the wire?
- **DNS beyond `localhost`.** External names, a slow or unreachable DNS server, and pool saturation were not
  run; only the source was read.
- **Wake latency under load.** It was not measured with a busy loop or a full sync queue.
- **Older kernels**, where io_uring lacks some opcodes.

## GitHub Actions (not verified)

A job for `.github/workflows/ci.yml`:

```yaml
  compio-checks:
    strategy:
      fail-fast: false
      matrix:
        include:
          - { os: ubuntu-latest, driver: io-uring }
          - { os: ubuntu-latest, driver: polling }
          - { os: macos-latest, driver: auto }
    runs-on: ${{ matrix.os }}
    steps:
      - uses: actions/checkout@v4
      - run: rustup toolchain install stable --profile minimal && rustup default stable
      - run: cargo build --locked --release -p compio-checks
      - run: ./target/release/compio-checks --driver ${{ matrix.driver }}
```

The `io-uring` entry fails loudly, not silently, if the runner's kernel blocks io_uring (for example with
`kernel.io_uring_disabled`). The latency bounds are lenient, but shared macOS runners are noisy. If they flake,
widen the bounds rather than dropping the checks.
