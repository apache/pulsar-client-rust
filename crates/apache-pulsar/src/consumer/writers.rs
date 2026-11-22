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

//! The schemas a consumer's messages were written with, which [`Schema::auto_consume`](crate::Schema::auto_consume)
//! and the Avro schemas decode them with. The consumer fetches the schema of a version before it delivers the first
//! message of it, by the topic of the message's segment, as the standard schema commands go to a segment
//! (*wire-protocol* §10).

use std::collections::HashMap;
use std::future::Future;
use std::pin::Pin;
use std::sync::Arc;
use std::task::{Context, Poll};

use apache_pulsar_protocol::consumer::ConsumerOptions;
use apache_pulsar_protocol::entry::ReceivedMessage;
use apache_pulsar_protocol::proto;
use apache_pulsar_protocol::proto::schema::Type;

use crate::error::{Error, Result};
use crate::local;
use crate::schema::SchemaInfo;

/// The schema a message was written with, for a consumer that decodes with it: none for a message without one, or of
/// a version the topic doesn't have; the error of its fetch, if that failed.
pub(crate) type Writer = std::result::Result<Option<Arc<SchemaInfo>>, Error>;

/// A message, and the schema it was written with, as a receive hands it to the application.
pub(crate) type Delivered = (ReceivedMessage, Writer);

type Fetch = Pin<Box<dyn Future<Output = Result<Option<proto::Schema>>>>>;

/// The schemas of a consumer's messages, by version.
pub(crate) struct Writers {
    client: local::PulsarClient,
    /// Whether the consumer decodes with them: only then are they fetched.
    wanted: bool,
    /// The topics of the segments the consumer connected to, whose messages' schemas are asked for by them.
    segments: HashMap<u64, String>,
    /// The schemas fetched, by version: a topic's history of schemas only grows. `None` for a version the topic
    /// doesn't have, whose messages read as bytes, as in Java.
    known: HashMap<Vec<u8>, Option<Arc<SchemaInfo>>>,
    /// The fetch in flight, and its version. Messages wait for their schema in order, the first one waiting holding
    /// back the others: one fetch at a time.
    fetching: Option<(Vec<u8>, Fetch)>,
    /// A fetch that failed, and its version: the message that waited for it takes the error.
    failed: Option<(Vec<u8>, Error)>,
}

/// Whether a consumer of `options` decodes with the schemas its messages were written with: one that declares
/// [`Schema::auto_consume`](crate::Schema::auto_consume)'s type, or Avro's, alone or in a key/value schema, as Java's
/// Avro schema reads each message with its version's. A key/value schema nested in another counts: it may hold Avro.
pub(crate) fn wanted(options: &ConsumerOptions) -> bool {
    let fetching = |schema: &proto::Schema, part: &str| {
        let key = format!("{part}.schema.type");
        let fetching = |kind: &str| matches!(kind, "AVRO" | "KEY_VALUE");
        schema.properties.iter().any(|property| property.key == key && fetching(&property.value))
    };
    options.schema.as_ref().is_some_and(|schema| match schema.r#type {
        Type::AutoConsume | Type::Avro => true,
        Type::KeyValue => fetching(schema, "key") || fetching(schema, "value"),
        _ => false,
    })
}

impl Writers {
    /// For a consumer that fetches the schemas if `wanted`.
    pub(crate) fn new(client: local::PulsarClient, wanted: bool) -> Self {
        Self { client, wanted, segments: HashMap::new(), known: HashMap::new(), fetching: None, failed: None }
    }

    /// Notes the topic of a segment the consumer connects to.
    pub(crate) fn connect(&mut self, segment: u64, topic: &str) {
        if self.wanted {
            self.segments.insert(segment, topic.to_string());
        }
    }

    /// The schema `message` was written with, or `None` while it is fetched: the message then waits for it, and the
    /// messages after it. A message without a schema version has none.
    pub(crate) fn resolve(&mut self, message: &ReceivedMessage) -> Option<Writer> {
        let version = match &message.entry.schema_version {
            Some(version) if self.wanted && !version.is_empty() => version,
            _ => return Some(Ok(None)),
        };
        if let Some(schema) = self.known.get(version) {
            return Some(Ok(schema.clone()));
        }
        if let Some((_, error)) = self.failed.take_if(|(failed, _)| failed == version) {
            return Some(Err(error));
        }
        if self.fetching.is_none() {
            // A message comes from a segment the consumer connected to.
            let topic = self.segments.get(&(message.id.segment_id as u64)).cloned().unwrap_or_default();
            let (client, asked) = (self.client.clone(), version.clone());
            self.fetching = Some((version.clone(), Box::pin(async move { client.schema(&topic, &asked).await })));
        }
        None
    }

    /// Polls the fetch in flight. True once it ends: the message waiting for it can go.
    pub(crate) fn poll(&mut self, cx: &mut Context<'_>) -> bool {
        let Some((_, fetch)) = &mut self.fetching else { return false };
        let Poll::Ready(result) = fetch.as_mut().poll(cx) else { return false };
        let (version, _) = self.fetching.take().expect("the fetch that ended");
        match result {
            Ok(schema) => {
                self.known.insert(version, schema.map(|schema| Arc::new(SchemaInfo::from_proto(&schema))));
            }
            Err(error) => self.failed = Some((version, error)),
        }
        true
    }
}

#[cfg(all(test, feature = "json", feature = "avro"))]
mod tests {
    use super::*;
    use crate::schema::Schema;

    #[derive(serde::Serialize, serde::Deserialize, apache_avro::AvroSchema)]
    struct Pojo {
        name: Option<String>,
    }

    fn fetches<T>(schema: Schema<T>) -> bool {
        wanted(&ConsumerOptions { schema: schema.info().to_proto(), ..ConsumerOptions::default() })
    }

    /// The consumers that fetch their messages' schemas: generic records', and Avro's, alone or in a key/value
    /// schema, nested ones included, as Java's Avro schema reads each message with its version's.
    #[test]
    fn the_consumers_that_fetch_their_messages_schemas() {
        assert!(fetches(Schema::auto_consume()));
        assert!(fetches(Schema::<Pojo>::avro()));
        assert!(fetches(Schema::key_value(Schema::string(), Schema::<Pojo>::avro())));
        assert!(fetches(Schema::key_value(Schema::<Pojo>::avro(), Schema::string())));
        let nested = Schema::key_value(Schema::string(), Schema::<Pojo>::avro());
        assert!(fetches(Schema::key_value(nested, Schema::bool())));
        assert!(!fetches(Schema::key_value(Schema::string(), Schema::<Pojo>::json())));
        assert!(!fetches(Schema::<Pojo>::json()));
        assert!(!fetches(Schema::string()));
        assert!(!fetches(Schema::bytes()));
    }
}
