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

# apache-pulsar-protocol

The sans-IO protocol core of [`apache-pulsar`](https://crates.io/crates/apache-pulsar), the Rust client of
[Apache Pulsar](https://pulsar.apache.org) for scalable topics: the Pulsar binary protocol's generated code, framing,
and the state machines of connections, producers and consumers.

**Not a stable API.** This crate is an implementation detail of `apache-pulsar`, which depends on an exact version of
it. Use `apache-pulsar`.

## License

[Apache License 2.0](https://www.apache.org/licenses/LICENSE-2.0).
