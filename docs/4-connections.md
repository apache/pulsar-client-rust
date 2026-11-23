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

# 4. Connections

> Previous: [3. Protocol core](3-protocol.md). Next: [5. Segment engine](5-segment-engine.md).

Connections, lookup, authentication and TLS are standard Pulsar, applied at the segment level
(*wire-protocol* §10). This document covers the connection layer of one loop. Each loop has its own
([2](2-architecture.md) §4.3).

## 1. Service URL

- `pulsar://host:port` and `pulsar+ssl://host:port`, with several hosts allowed (`pulsar://a:6650,b:6650`).
  The client tries the hosts in turn when a connect fails.
- Host names are resolved asynchronously, without blocking the loop. How compio does that is checked in
  milestone 0 ([2](2-architecture.md) §5).
- HTTP service URLs are not supported ([1](1-scope.md) §5).

## 2. Addresses and the pool

A connection has a **logical address**, the broker it talks to, and a **physical address**, the host it
connects to. They are the same unless the lookup answered `proxy_through_service_url`. In that case the
client connects to the service URL and sets `CommandConnect.proxy_to_broker_url` to the logical address,
which is how a Pulsar proxy knows where to forward.

A Pulsar proxy answers lookups this way. So does a standalone broker whose advertised address is a loopback
address (`TopicLookupBase.shouldRedirectThroughServiceUrl`): the client then connects to the service URL, and the
broker ignores `proxy_to_broker_url`.

The pool is keyed by (logical, physical), with one connection per key by default, as in the Java client
(`connectionsPerBroker = 1`). The client never resolves or connects to a logical address it got this way, and
TLS verifies the physical host.

## 3. Lookup and topic resolution

| What | How |
|---|---|
| Open a layout watch for a topic | A standard `CommandLookupTopic` on the topic name, then `CommandScalableTopicLookup` on the resulting connection, as the Java client does (`DagWatchClient`). |
| Connect a per-segment producer, consumer or reader | A standard `CommandLookupTopic` on the segment topic (`segment://…`, or the wrapped `persistent://` name of a legacy segment), then connect to the result. This is what the Java client does, and it works behind a Pulsar proxy. *wire-protocol* §5 says to use `segment_brokers` instead ([spec gap 3](../DESIGN.md#spec-gaps-to-raise-upstream)). |
| Reach the controller (Stream and grouped Checkpoint consumers) | `controller_broker_url` from the layout, connected directly. Behind a proxy, connect to any broker through the proxy instead. If the layout has no controller URL yet, use the service URL. Both fallbacks follow the Java client (`ScalableConsumerClient`, [spec gap 4](../DESIGN.md#spec-gaps-to-raise-upstream)). |
| Find a transaction coordinator | The TC assignment watch when the broker advertises `supports_tc_metadata_discovery`, standard lookup otherwise (*wire-protocol* §8.1). Later phase. |

Lookup follows redirects, carrying the `authoritative` flag, for up to 20 hops (Java's `maxLookupRedirects`).
After that it fails with a typed error. Every connection checks `supports_scalable_topics` before it carries a
scalable-topic command (C15, [3](3-protocol.md)).

## 4. Reconnection

A connection that drops fails its in-flight requests with a retryable error, and tells the objects that use
it: producers, consumers, layout watches, assignment sessions. Each object re-establishes itself (lookup,
connect, re-register) with its own exponential backoff and jitter, and the application never sees it unless
the operation's deadline passes (*error-model* §3, *client-behavior* §9).

Concurrent reconnects to the same address share one connect attempt: the pool hands every waiter the same
pending connection (*client-behavior* §9, coalescing).

Deadlines:

| Timeout | Default | Covers |
|---|---|---|
| Connect | 10 s | TCP connect, TLS handshake, `CONNECT`/`CONNECTED`. |
| Operation | 30 s | Each request/response: lookup, producer and subscribe registration, schema. |
| Keepalive | 30 s | Ping/pong. A connection that misses a pong is closed and reconnected. |

The defaults match the Java client's, so mixed deployments behave alike.

## 5. TLS

- rustls, through `compio-tls`.
- Trust: the platform's roots, a configured CA bundle, or both. Hostname verification is on by default and
  can be turned off only explicitly (`allow_insecure_connection`), as in other Pulsar clients.
- mTLS: a client certificate and key, which also serve as the mTLS authentication method (§6).
- The rustls crypto provider (`aws-lc-rs`, which needs a C toolchain, or `ring`, which cross-compiles more
  easily) is an open question (§7).

## 6. Authentication

An authentication provider supplies the `CONNECT` auth data, and answers `CommandAuthChallenge` when the
broker refreshes credentials (`supports_auth_refresh`). Providers are `Send + Sync` and shared by all loops,
and a provider call is async so that it can fetch a token.

| Method | When |
|---|---|
| Token, static or from a callback | First |
| mTLS | First |
| OAuth2 client credentials | Next. It needs an HTTPS client on compio ([`cyper`](https://github.com/compio-rs/cyper) is 0.x), or a minimal token POST written on `compio-tls`. |
| Kerberos, Athenz, SASL | Not planned ([1](1-scope.md) §5) |

## 7. Open questions

1. **rustls crypto provider:** `aws-lc-rs` or `ring` by default. Leaning: `ring`, which milestone 1 uses.
2. **OAuth2 transport:** `cyper`, or a minimal HTTP/1.1 POST for the token endpoint only.
3. **SNI-based proxy routing** (`proxyServiceUrl` with SNI in the Java client): needed early, or later?
