# 3. Protocol core

> Previous: [2. Architecture](2-architecture.md). Next: [4. Connections](4-connections.md).

`apache-pulsar-protocol` is the sans-IO core: the protocol types, framing, and the connection and
producer/consumer protocol state machines ([2](2-architecture.md) §2). It does no I/O and has no runtime.

## 1. The proto file

- `PulsarApi.proto` is vendored from a pinned `apache/pulsar` commit. The commit is recorded next to the file.
- The generated Rust code is committed, so building the client doesn't need `protoc`.
- A script refreshes both from a given commit. CI checks that the committed code matches the vendored file.
- The scalable-topic commands are `BaseCommand` fields 70–82 (*wire-protocol* §1).

## 2. Protobuf library

**Decided: [buffa](https://github.com/anthropics/buffa), with the conditions below.** A spike on the real
`PulsarApi.proto` compared buffa 0.9.2 with prost 0.14. It ran on an Apple M5 Pro, in release builds, with an
allocation-counting allocator.

| Decode | ns/op | Allocations |
|---|---|---|
| BaseCommand + MessageMetadata: buffa view | 200 | 3 |
| BaseCommand + MessageMetadata: buffa `OwnedView` (`Send + 'static`) | 234 | 3 |
| BaseCommand + MessageMetadata: prost | 495 | 9 |
| BaseCommand only: buffa view | 97 | 2 |
| BaseCommand only: prost | 206 | 0 |

| Encode a SEND frame, messages already built | ns/op | Allocations |
|---|---|---|
| buffa | 100 | 0 |
| prost | 115 | 0 |

**What decides it.** The views decode `BaseCommand` and `MessageMetadata` borrowing from the frame buffer: strings
like `producer_name` point into it. That is what the receive path wants ([§5](#5-buffers)). Both libraries encode
into a caller's buffer with a size pass first, which framing needs. In buffa that is `compute_size` then
`write_to`, which sizes once. The frames both produce are byte for byte the same.

**Conditions:**

1. **proto2 defaults.** buffa doesn't apply `[default = …]` to absent optional fields: `partition` comes back
   `None`, where prost returns −1. The proto has 12 optional fields with non-zero defaults, for example
   `partition = -1`, `CommandSend.num_messages = 1` and `durable = true`, and 4 enum fields whose default is
   their first value. The protocol crate wraps all 16 in accessors that apply the default, and a test reads every
   `[default]` in the vendored proto and checks that it has one.
2. **Unknown enum values.** By default buffa turns an unknown `BaseCommand.type` into the first variant
   (`CONNECT`). Codegen uses `open_enums_in` for that field, so an unknown command is seen as unknown.
3. **Required fields.** Neither library rejects a missing `required` field: an absent `ledgerId` decodes as 0.
   The decoder checks presence (`has_*` on views) where it matters.
4. **Version.** buffa is pre-1.0, so its exact version is pinned, and only the protocol crate depends on it.

**Costs:**

- The generated code is large: about 60k lines with views, against 2.8k for prost.
- The protocol crate builds slower: 8 s against 3 s in a rough measure.
- Nested views still box (`MessageFieldView`, buffa issue #429), hence the 2–3 allocations above. Lazy views
  allocate nothing for `BaseCommand`, but have no `OwnedView`.

**The alternative not taken.** prost handles proto2 defaults by itself and is far more widely used (about 723M
downloads, against about 12M), at about twice the decode cost. Only the protocol crate sees generated types, so the
choice can be revisited without touching the rest of the client.

## 3. Framing

```
simple command   [total_size u32][command_size u32][BaseCommand]
payload command  [total_size u32][command_size u32][BaseCommand]
                 [magic 0x0e01 u16][crc32c u32][metadata_size u32][MessageMetadata][payload]
```

- All integers are big endian. The CRC32C covers everything after the checksum. It uses a crate with hardware
  acceleration.
- On receive, a frame can carry a `BrokerEntryMetadata` section (magic `0x0e02`) before the checksum section. It is
  parsed when present.
- The largest accepted frame is the broker's `max_message_size` from `CommandConnected`, plus the frame overhead.
- The codec encodes and decodes **both** directions, so the simulated broker can use it ([8](8-quality.md) §2).

## 4. Connection state machine

```
Init ──CONNECT──► AwaitConnected ──CONNECTED──► Ready ──close / error──► Closed
                       │   ▲                     │  ▲
                AUTH_CHALLENGE                AUTH_CHALLENGE (refresh)
                       ▼   │                     ▼  │
                   AwaitAuthData             AwaitAuthData
```

- `CONNECT` advertises the latest protocol version the client implements (v21 today), the client version, the
  auth data and, through a proxy, `proxy_to_broker_url` ([4](4-connections.md) §2). `CONNECTED` brings the
  broker's protocol version, `max_message_size` and `FeatureFlags`.
- **Requests:** each connection allocates request ids and keeps a table of pending requests with deadlines.
  Responses (`CommandSuccess`, `CommandError`, `CommandProducerSuccess`, `CommandLookupTopicResponse`, and so on)
  complete them by request id.
- **Pushed commands** reach the object registered for their id:

  | Id | Commands |
  |---|---|
  | `producer_id` | `CommandSendReceipt`, `CommandSendError`, `CommandCloseProducer` |
  | `consumer_id` | `CommandMessage`, `CommandCloseConsumer`, `CommandActiveConsumerChange`, `CommandReachedEndOfTopic`, `CommandScalableTopicAssignmentUpdate` |
  | `session_id` | `CommandScalableTopicUpdate` |
  | `watch_id` | `CommandWatchScalableTopicsUpdate`, `CommandWatchTcAssignmentsUpdate` |

  The ids of each kind come from one allocator per connection. Segment consumers and assignment sessions share
  the `consumer_id` space, so they can never collide.
- **Keepalive:** the connection answers the broker's pings, and pings after an idle interval. A missed pong
  closes the connection.
- **Feature gating:** a connection keeps the broker's `FeatureFlags`. It refuses to send a scalable-topic command
  without `supports_scalable_topics`, with `UnsupportedBroker` (C15). `supports_tc_metadata_discovery` and
  `supports_auth_refresh` gate their features the same way.

The producer and consumer state machines on top of this ([5](5-segment-engine.md)) live in the same crate. They
track registration, pending sends and receipts, flow permits and acks per id, and they produce commands for the
connection to frame.

## 5. Buffers

- **Reading:** compio reads into owned buffers. The connection appends them to a `BytesMut`, and each complete
  frame is split off as `Bytes`, without a copy. A message's payload is a slice of its frame, so delivering it
  costs no copy either.
- **Memory:** a payload slice keeps its frame's allocation alive until the application drops the message. Memory
  accounting therefore counts frames, not payloads.
- **Writing:** commands are encoded straight into the connection's write buffer, and many frames go out in one
  write. The buffer is handed to compio and comes back when the write completes, so a dropped future is memory-safe.
  It is not lossless: a dropped read can lose bytes it already read, and a dropped write can leave half a frame,
  hence one read loop and one writer per connection ([2](2-architecture.md) §4.4, rule 5).

## 6. Compression codecs

| Codec | Crate | Notes |
|---|---|---|
| LZ4 | `lz4_flex` | Pure Rust. |
| ZLIB | `flate2` with `miniz_oxide` | Pure Rust. |
| Snappy | `snap` | Pure Rust. |
| ZSTD | `zstd` | Binds the C `libzstd`. There is no mature pure-Rust encoder; `ruzstd` only decodes well. |

Each codec sits behind a feature, all on by default.

## 7. Open questions

1. **Small messages pinning big buffers.** Should payloads below some size be copied out, so that one retained
   message doesn't keep a whole read buffer alive?
2. **ZSTD:** is the C dependency acceptable by default, given that it matters for cross-compiling and for future
   bindings?
3. **`CommandTopicMigrated`** (protocol v20), the redirect a blue-green migration sends: handle it in the first
   connection milestone, or later?
