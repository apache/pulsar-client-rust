#!/usr/bin/env bash
# Licensed to the Apache Software Foundation (ASF) under one
# or more contributor license agreements.  See the NOTICE file
# distributed with this work for additional information
# regarding copyright ownership.  The ASF licenses this file
# to you under the Apache License, Version 2.0 (the
# "License"); you may not use this file except in compliance
# with the License.  You may obtain a copy of the License at
#
#   http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing,
# software distributed under the License is distributed on an
# "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
# KIND, either express or implied.  See the License for the
# specific language governing permissions and limitations
# under the License.

# The performance baseline (docs/8-quality.md §6): this client's `perf` against Java's `pulsar-perf`, which drives the
# V5 client for a `topic://` topic. Each run has a standalone broker of its own, and its client runs in a container of
# its own on the broker's Docker network, so that both clients take the same path to the broker. Both are bounded by a
# 64 MiB client memory limit, the only bound a V5 producer has. `perf` drives the thread-safe client, with one event
# loop, from a thread of its own, as `pulsar-perf` sends from a thread of its own.
#
#   tools/perf/baseline.sh            all the runs, each client in turn
#   tools/perf/baseline.sh latency    the runs whose name holds `latency`
#
# Prints each run's summary lines, as both tools print them.

set -euo pipefail

IMAGE=${PULSAR_IMAGE:-apachepulsar/pulsar:5.0.1}
RUST_IMAGE=${RUST_IMAGE:-rust:1-bookworm}
NET=apr-perf
BROKER=apr-perf-broker
ROOT=$(cd "$(dirname "$0")/../.." && pwd)
BIN=$(mktemp -d)
FILTER=${1:-}

cleanup() {
    docker rm -f "$BROKER" >/dev/null 2>&1 || true
    docker network rm "$NET" >/dev/null 2>&1 || true
    rm -rf "$BIN"
}
trap cleanup EXIT

# `perf` for Linux, built in the official Rust image, with its target and registry cached in volumes.
docker run --rm -v "$ROOT:/src:ro" -v apr-perf-target:/target -v apr-perf-registry:/usr/local/cargo/registry \
    -e CARGO_TARGET_DIR=/target -w /src "$RUST_IMAGE" cargo build --release --locked -p perf
docker run --rm -v apr-perf-target:/target -v "$BIN:/out" "$RUST_IMAGE" cp /target/release/perf /out/perf

docker network create "$NET" >/dev/null

URL=pulsar://broker:6650
next=0

# A broker of its own for each run, so that none starts on what an earlier one stored.
broker() {
    docker rm -f "$BROKER" >/dev/null 2>&1 || true
    docker run -d --name "$BROKER" --hostname broker --network "$NET" --network-alias broker "$IMAGE" \
        bin/pulsar standalone --no-functions-worker --no-stream-storage >/dev/null
    until docker exec "$BROKER" bin/pulsar-admin brokers healthcheck >/dev/null 2>&1; do sleep 2; done
}

# A new scalable topic with `$1` segments.
topic() {
    next=$((next + 1))
    local name="perf-$$-$next"
    docker exec "$BROKER" bin/pulsar-admin scalable-topics create -s "$1" "public/default/$name" >/dev/null
    echo "topic://public/default/$name"
}

rust() {
    docker run --rm --network "$NET" -v "$BIN:/perf:ro" "$RUST_IMAGE" /perf/perf "$@" 2>&1 | grep -o 'Aggregated.*'
}

java() {
    docker run --rm --network "$NET" "$IMAGE" bin/pulsar-perf "$@" 2>&1 | grep -o 'Aggregated.*'
}

# The rate over a run of `$1` seconds, from the records its summary line counts.
rate() {
    local summary records
    summary=$(cat)
    echo "$summary"
    records=$(sed -n 's/.*--- \([0-9]*\) records sent.*/\1/p' <<<"$summary")
    [[ -n "$records" ]] && echo "Over the ${1}s run: $((records / $1)) msg/s"
}

# A run of `$3` seconds for each client: its name, the topic's segments, the seconds, then the options both tools take
# alike. pulsar-perf notices the end of a run on its 10-second ticks, which its summary's rate counts in: the rate over
# the run, the records over its seconds, is the one to compare.
produce() {
    local name=$1 segments=$2 seconds=$3
    shift 3
    [[ -z "$FILTER" || "$name" == *"$FILTER"* ]] || return 0
    echo "== $name: $*, $segments segment(s), ${seconds}s"
    broker
    echo "-- rust"
    rust produce -u "$URL" -m 0 --test-duration "$seconds" "$@" "$(topic "$segments")" | rate "$seconds"
    broker
    echo "-- java"
    java produce -u "$URL" -ml 64M -m 0 -time "$seconds" "${@/--disable-batching/-db}" "$(topic "$segments")" \
        | rate "$seconds"
}

# A Queue consumer of each client reading `$2` messages of 1 KiB from the earliest, on a broker of its own, which
# `perf` fills first. This client's twice: on the thread-safe client, whose receives and acks each cross to its loop's
# thread, and on the local client, on the thread receiving.
consume() {
    local name=$1 messages=$2 client topic
    [[ -z "$FILTER" || "$name" == *"$FILTER"* ]] || return 0
    echo "== $name: $messages messages of 1 KiB, 1 segment"
    for client in rust rust-local java; do
        broker
        topic=$(topic 1)
        rust produce -u "$URL" -s 1024 -m "$messages" -r 5000000 "$topic" >/dev/null
        echo "-- $client"
        case $client in
            rust) rust consume -u "$URL" -m "$messages" "$topic" ;;
            rust-local) rust consume -u "$URL" -m "$messages" --loops 0 "$topic" ;;
            java) java consume -u "$URL" -m "$messages" -ss perf -sp Earliest "$topic" ;;
        esac
    done
}

MAX=5000000

produce throughput-1k 1 20 -s 1024 -r $MAX
produce throughput-1k-4-segments 4 20 -s 1024 -r $MAX
produce throughput-100b 1 20 -s 100 -r $MAX
produce throughput-10k 1 20 -s 10240 -r $MAX
produce throughput-unbatched 1 20 -s 1024 -r $MAX --disable-batching
produce throughput-lz4 1 20 -s 1024 -r $MAX -z LZ4
produce throughput-zstd 1 20 -s 1024 -r $MAX -z ZSTD
# Past the broker's own warm-up, its JIT's.
produce latency-10k-per-second 1 60 -s 1024 -r 10000 --warmup-time 10
consume consume-1k 4000000
