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

# Java test vectors

This program generates the test vectors that the Rust client checks itself against, using the
Apache Pulsar 5.0.1 Java client from Maven Central. The vectors are committed, so the Rust tests do
not need a JVM.

It writes three files into `crates/apache-pulsar-protocol/tests/vectors/`:

- `routing.json`: for each key, the raw and masked Murmur3 32-bit hashes, the segment hash (high 16
  bits), the entry-bucket hash (low 16 bits), and the v4 partition for each partition count. The
  partitions come from `SegmentRouter.route` over a layout of legacy segments, which takes the v4
  `signSafeMod(makeHash(key), N)` path.
- `message_id.json`: `MessageIdV5.toByteArray()` for a set of ids, with the v4 id bytes.
- `checkpoint.json`: `CheckpointV5.toByteArray()` for a set of checkpoints, including the
  `EARLIEST`/`LATEST` sentinels and `latestOf`.

The generator is in package `org.apache.pulsar.client.impl.v5` so it can reach the package-private
API on the classpath. It decodes every id and checkpoint it encodes, compares the result field by
field, and fails if anything differs.

## Regenerating

Needs JDK 17 or newer and Maven 3.9. From this directory:

```
mvn -B compile exec:java
```

The output directory is set by the `vectors.dir` property in `pom.xml`. Run it twice and check that
`git diff` is empty: the output is meant to be identical between runs.

## Ordering in the encodings

Java writes position vectors and multi-topic vectors in `HashMap` iteration order. Cases where any
vector has more than one entry have `"byte_exact": false`; compare those after decoding. Two sources
of run-to-run variation are handled in the generator:

- The multi-topic section is written from a `HashMap<byte[], ...>`, whose order follows identity
  hash codes. For ids with two or more topics the generator encodes many times and keeps the
  lexicographically smallest result, so the committed bytes are a real Java encoding that does not
  change between runs.
- `MessageIdV5` and `CheckpointV5` copy their vectors with `Map.copyOf`, whose iteration order is
  salted per JVM. That only reaches the bytes when two segment ids share a `HashMap` bucket, so the
  multi-entry cases use segment ids that do not collide.
