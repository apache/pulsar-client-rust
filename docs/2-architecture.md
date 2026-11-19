# 2. Architecture

> Previous: [1. Scope](1-scope.md). Next: [3. Protocol core](3-protocol.md).

## 1. Layers

```
  application (tokio, smol, blocking code, or its own compio runtime)
        │
  ┌─────┴──────────────────────────────────────────────────────────────┐
  │ public API      thread-safe API · local API · blocking API     [7] │
  ├────────────────────────────────────────────────────────────────────┤
  │ scalable layer  layout watch · router · Stream/Queue/Checkpoint    │
  │                 consumers · assignment · position vectors      [6] │
  ├────────────────────────────────────────────────────────────────────┤
  │ segment engine  single-topic producer · consumer · reader      [5] │
  ├────────────────────────────────────────────────────────────────────┤
  │ connections     pool · lookup · auth · TLS · reconnect         [4] │
  ├────────────────────────────────────────────────────────────────────┤
  │ driver (compio) sockets · TLS streams · timers · tasks             │
  ├────────────────────────────────────────────────────────────────────┤
  │ protocol core   framing · commands · connection and producer/      │
  │ (sans-IO)       consumer protocol state machines               [3] │
  └────────────────────────────────────────────────────────────────────┘
```

The scalable layer is new orchestration over ordinary single-topic producers and consumers, one per segment.
That is how the spec describes it (*client-behavior* §5, *wire-protocol* §10), and how the Java reference
client works. So the segment engine is the classic Pulsar client for one topic, and the scalable layer never
touches the wire except through it, the connection layer, and the new scalable-topic commands.

## 2. Sans-IO

Everything except I/O, timers and task scheduling is plain code with no runtime. The protocol core takes
bytes and the current time as input, and produces bytes to send, events, and the next deadline. This is the
same shape `quinn-proto` and `rustls` use:

```rust
conn.handle_input(now, &bytes)?;          // decode frames, update state
while let Some(ev) = conn.poll_event() {} // e.g. ProducerReady, MessageReceived, LayoutUpdated
conn.poll_transmit(&mut out);             // encoded frames to write
conn.poll_timeout()                       // next keepalive / request deadline
```

The driver owns the socket and calls these. The segment engine's batching, ack grouping and flow control,
and the scalable layer's layout, routing and assignment logic, are written the same way: state machines
that the driver feeds with events and time.

This gives three things:

- **Deterministic tests.** A simulated broker built on the same codec drives the client through splits,
  reconnects, reassignments and failures without sockets or sleeps ([8](8-quality.md)). That is why the codec
  encodes and decodes both directions.
- **compio churn stays in one layer.** compio is pre-1.0; only the driver uses it.
- **Room for another driver** (for example tokio) later, without touching the protocol logic.

## 3. Crates

| Crate | Contents | Stability |
|---|---|---|
| `apache-pulsar` | Driver, connections, segment engine, scalable layer, and the public thread-safe, local and blocking APIs. | The only crate with semver promises. |
| `apache-pulsar-protocol` | Generated protobuf types, framing, and the sans-IO connection and producer/consumer protocol state machines. No I/O, no runtime. | Published because `apache-pulsar` depends on it. Documented as internal, kept at 0.x, and pinned by an exact version. |
| `apache-pulsar-testkit` | The simulated broker and test helpers. | Not published at first. |

Few published crates mean few semver commitments. The segment engine stays a module of `apache-pulsar`.
If a classic-API crate comes later, it either lives in `apache-pulsar` as a module or feature, or the engine
moves into its own crate then.

## 4. Event loops

A **loop** is one OS thread running one compio runtime. Each loop runs a **local client**: its connection
pool, lookups, layout watches, and the producers and consumers pinned to it.

Inside a loop, each piece of state is owned by one task: a connection, a layout watch, a producer, a
consumer. Other tasks on the loop reach it through local, unsynchronized channels, or through short borrows
that never span an `.await` (enforced with clippy's `await_holding_refcell_ref`). There are no locks, no
contended atomic read-modify-writes, and no cross-thread wakeups on the data path. Uncontended atomics remain,
such as the reference counts of `Bytes` buffers and the single-writer cells of nanometer metrics.

### 4.1 Thread-safe API (the default)

The spec requires the client, producers and consumers to be safe to use from many threads
(*client-behavior* §10, C14). The default API meets that on top of local clients:

- The `Client` owns N loop threads, one local client on each. Every loop runs the same code as the local API,
  so there is one implementation.
- When a producer or consumer is created, it is pinned to one loop for its lifetime, the least loaded one by
  number of producers and consumers. All its state, including its per-segment producers or consumers and
  their connections, lives there.
- Its handle is `Send + Sync`. An operation travels to the loop over a channel and completes through a
  oneshot. The futures depend on no runtime, so any executor can await them. Payloads move to the loop;
  they aren't copied.
- Admission is decided on the caller's side. The client's memory limit, the only bound on what producers hold
  (as in Java V5), is a counter that the handle reserves before the hop and the loop releases when the send
  completes. So "full", or waiting for room, is decided without a round trip. This counter is the only shared
  atomic on this path.

The price is one channel hop per operation between the caller's thread and the loop.

### 4.2 Local API

`apache_pulsar::local` exposes the local client directly. Its types are `!Send`, and they are created inside
a compio runtime the caller runs. It spawns tasks on that runtime only: no threads, and no global state other
than the process-wide metrics registry. It serves applications that are thread-per-core themselves and want
the client on their own loops, with no hop.

### 4.3 What crosses loops

Pinning keeps almost everything on one loop. These are the exceptions, and how each is handled:

| State | Handling |
|---|---|
| Client-wide memory limit | One shared counter in the thread-safe API, reserved at admission (§4.1); its loops' local clients share it. A local client used directly has its own: the same code, with atomics that are never contended. |
| Connections | Each loop has its own pool, so a client can open up to N connections to one broker. |
| Layout watches | Each loop watches the topics its own producers and consumers use. Two loops using one topic both watch it. |
| Transactions | A transaction is pinned to a loop like a producer, along with its coordinator handler. A producer or consumer on another loop registers its segment with the transaction through that loop before sending or acking in it. Only transactional operations pay for this. |
| Authentication providers | Shared and `Send + Sync`. They are called on connect and on auth refresh, never per message. |
| Client close | Fans out to all loops and waits for each to close gracefully. |
| Metrics | One nanometer handle per loop, summed when exported ([8](8-quality.md)). |
| Ids | Request, producer, consumer, session and watch ids are per connection, so per loop. Nothing to share. |

A namespace consumer keeps all its per-topic consumers on its own loop.

### 4.4 Rules on a loop

1. Never block a loop thread. The spec's ban on blocking an I/O thread on its own response
   (*client-behavior* §10) holds by construction: everything is async on one thread.
2. Never hold a `RefCell` borrow across `.await`.
3. Every spawned task belongs to an object that owns it and stops it on close: no detached tasks.
4. I/O uses owned buffers, so a future dropped while an operation is in flight leaves the buffer alive until
   the operation completes ([3](3-protocol.md)).
5. **Never drop a read or a write in flight.** Dropping a completed read loses the bytes it read. That happens
   under `compio::time::timeout`, whose select is random, on every driver, and on io_uring even under a select
   that polls the read first, because the kernel can finish the receive before the cancel. A dropped
   `write_all` can leave half a frame on the wire. So one task owns each connection's stream, and polls its
   reads and writes from a single future through compio's poll-based adapters (`compat::AsyncStream`, and
   `TlsStream`), which keep an operation and its buffer inside the stream between polls. A read stays parked
   while frames go out, with no lock, and nothing is cancelled. Keepalive and request deadlines are the
   connection state machine's own timers, polled from the same future (milestone 1, `conn.rs`).
6. **Dropping a `JoinHandle` cancels its task**, unlike tokio, where it detaches it. An owner keeps the handle
   of each task it runs.
7. **The driver choice is silent (proposed: make it explicit).** With compio's `polling` feature, Linux uses
   io_uring when it is available and falls back to epoll without a word, for example under Docker's default
   seccomp profile. Without the feature, a blocked io_uring makes the runtime fail to start, and a request for
   polling is ignored. Proposed: the client enables `polling`, makes the driver a client option, and each loop
   logs the driver it got.
8. **DNS goes through a blocking pool.** compio resolves names with `getaddrinfo` on its per-runtime pool of up
   to 256 threads. A resolution can't be cancelled, and when the pool is full the loop thread itself spins.
   Options: share one bounded pool across loops, or a resolver of our own with a cap and a timeout (open
   question 5).

The findings behind rules 5–8 come from [`checks/compio-checks`](../checks/compio-checks/README.md). They pass on Linux
with io_uring and with epoll (in Docker Desktop's VM, aarch64) and on macOS.

## 5. Open questions

1. **Default number of loops.** The Java client defaults its I/O threads to the number of cores. Since each
   loop has its own connection pool, a small default (1, or `min(cores, 4)`) seems better. Leaning: 1, until
   benchmarks say otherwise.
2. **Placement.** Least loaded by count is simple, but it ignores throughput, and it opens a second watch
   for a topic already watched on another loop. Do we need more than that?
3. **Task granularity on a loop.** One task per object is the starting point. A producer is one task that holds
   all its per-segment producers, with one channel hop from the handle and one to the connection's task per
   message (milestone 2). Measure whether those hops matter.
4. **compio behavior, verified in milestone 0** ([Plan](../PLAN.md)): see §4.4, rules 5–8. Still unverified:
   bare-metal and x86_64 Linux, older kernels, a stressed write-side cancel, wake latency under a busy loop, and
   channel choice inside a loop and across loops.
5. **DNS resolver.** A shared bounded pool behind compio's resolution, or a resolver of our own with a timeout.
   Leaning, for now: compio's resolution, bounded by the connect timeout.
