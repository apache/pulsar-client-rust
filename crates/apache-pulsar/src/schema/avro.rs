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

//! Avro: Java's `Schema.avro`, records encoded as Avro datums, read with the schema each was written with.

use std::sync::Arc;

use apache_avro::AvroSchema;
use apache_avro::reader::datum::GenericDatumReader;
use apache_avro::types::Value;
use apache_avro::writer::datum::GenericDatumWriter;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::record::{Definitions, avro_error, info};
use super::{Generic, Schema, SchemaInfo, SchemaType};
use crate::error::{Error, Result};

impl<T: Serialize + DeserializeOwned + AvroSchema + 'static> Schema<T> {
    /// Records in Avro's binary encoding, as Java's `Schema.avro(Class)` encodes them: a datum, without a header. The
    /// schema is the one `T` derives, with `#[derive(AvroSchema)]` of apache-avro's `derive` feature: an `Option`
    /// field is nullable, with a default of null, as Java's reflection makes its nullable fields.
    ///
    /// Each message is read with the schema it was written with, which the consumer fetches from the broker once for
    /// each version, as Java does: Avro's resolution then fills in what this schema adds, such as a field of a later
    /// version, and the fields come in whichever order the writer's schema has them. A message without a version reads
    /// with this schema.
    pub fn avro() -> Self {
        let avro = Arc::new(Avro::new(T::get_schema()));
        let reader = avro.clone();
        Self {
            info: info(SchemaType::Avro, &avro.schema),
            encode: Arc::new(move |value: &T| avro.encode(value)),
            decode: Arc::new(move |data: &[u8], writer: Option<&SchemaInfo>| reader.decode(data, writer)),
            generic: Generic::No,
        }
    }
}

/// An Avro schema, and the schemas of other versions it reads messages written with.
struct Avro {
    schema: apache_avro::Schema,
    /// The definition the broker is told about, which a message written with this schema has.
    definition: Vec<u8>,
    writers: Definitions,
}

impl Avro {
    fn new(schema: apache_avro::Schema) -> Self {
        let definition = info(SchemaType::Avro, &schema).definition;
        Self { schema, definition, writers: Definitions::default() }
    }

    fn encode<T: Serialize>(&self, value: &T) -> Result<Bytes> {
        let writer = GenericDatumWriter::builder(&self.schema).build();
        writer.and_then(|writer| writer.write_ser_to_vec(value)).map(Bytes::from).map_err(unencodable)
    }

    /// Decodes a payload written with `writer`'s schema.
    fn decode<T: DeserializeOwned>(&self, data: &[u8], writer: Option<&SchemaInfo>) -> Result<T> {
        match self.writer(writer)? {
            None => {
                GenericDatumReader::builder(&self.schema).build().and_then(|reader| reader.read_deser(&mut &data[..]))
            }
            Some(writer) => self.resolve(&writer, data).and_then(|value| apache_avro::from_value(&value)),
        }
        .map_err(undecodable)
    }

    /// The schema a message was written with, if it isn't this one: an Avro schema's of another definition.
    fn writer(&self, writer: Option<&SchemaInfo>) -> Result<Option<Arc<apache_avro::Schema>>> {
        match writer {
            Some(writer) if writer.schema_type == SchemaType::Avro && writer.definition != self.definition => {
                self.writers.parse(&writer.definition).map(Some)
            }
            _ => Ok(None),
        }
    }

    /// A payload written with `writer`, read into this schema.
    fn resolve(&self, writer: &apache_avro::Schema, data: &[u8]) -> std::result::Result<Value, apache_avro::Error> {
        let reader = GenericDatumReader::builder(writer).reader_schema(&self.schema).build();
        reader.and_then(|reader| reader.read_value(&mut &data[..]))
    }
}

fn unencodable(error: apache_avro::Error) -> Error {
    avro_error("the value doesn't encode in Avro", error)
}

/// A payload that doesn't decode in Avro, or doesn't resolve into the reader's schema.
pub(super) fn undecodable(error: apache_avro::Error) -> Error {
    avro_error("the payload doesn't decode in Avro", error)
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::schema::KeyValue;
    use crate::schema::record::{Unencodable, properties};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, apache_avro::AvroSchema)]
    struct Pojo {
        name: Option<String>,
        age: i32,
    }

    /// A record's first version, and its second, which adds a nullable field, as Java's V5 evolution test has them.
    #[derive(Debug, PartialEq, Serialize, Deserialize, apache_avro::AvroSchema)]
    struct V1 {
        i: i32,
    }

    #[derive(Debug, PartialEq, Serialize, Deserialize, apache_avro::AvroSchema)]
    struct V2 {
        i: i32,
        j: Option<i32>,
    }

    /// A second version that adds a field without a default, which a record of the first can't fill.
    #[derive(Debug, PartialEq, Serialize, Deserialize, apache_avro::AvroSchema)]
    struct Bad {
        i: i32,
        j: i32,
    }

    /// A schema as the broker returns it, named after its topic.
    fn written_with(definition: &[u8]) -> SchemaInfo {
        SchemaInfo {
            name: "acme/orders/events".to_string(),
            schema_type: SchemaType::Avro,
            definition: definition.to_vec(),
            properties: properties(),
        }
    }

    #[test]
    fn records_encode_as_avro_datums() {
        let schema = Schema::<Pojo>::avro();
        let info = schema.info();
        assert_eq!((info.name(), info.schema_type(), info.properties()), ("", SchemaType::Avro, &properties()));
        let definition: serde_json::Value = serde_json::from_slice(info.definition()).unwrap();
        let fields = [
            serde_json::json!({"name": "name", "type": ["null", "string"], "default": null}),
            serde_json::json!({"name": "age", "type": "int"}),
        ];
        assert_eq!(definition, serde_json::json!({"type": "record", "name": "Pojo", "fields": fields}));
        let alice = Pojo { name: Some("alice".to_string()), age: 30 };
        // The name's union branch, 1, and its length, 5, then the age, 30, each a zig-zag varint.
        let payload = schema.encode(&alice).unwrap();
        assert_eq!(&payload[..], [2, 10, b'a', b'l', b'i', b'c', b'e', 60]);
        assert_eq!(schema.decode(&payload).unwrap(), alice);
        assert_eq!(schema.decode_with(&payload, Some(info)).unwrap(), alice, "its own version");
        let nameless = Pojo { name: None, age: -1 };
        assert_eq!(schema.decode(&schema.encode(&nameless).unwrap()).unwrap(), nameless);
    }

    /// A record reads with the schema it was written with: Java's reflection orders the fields of the same record
    /// otherwise, and an older Java client names it in a namespace that ends in `$`; and a version that adds a
    /// nullable field reads a record of the one before with it null, as Java's V5 evolution test does.
    #[test]
    fn records_read_with_the_schema_they_were_written_with() {
        let java = br#"{"type":"record","name":"Pojo","namespace":"SchemaTool$","fields":[{"name":"age","type":"int"},
            {"name":"name","type":["null","string"],"default":null}]}"#;
        let payload = [60, 2, 10, b'a', b'l', b'i', b'c', b'e'];
        let read = Schema::<Pojo>::avro().decode_with(&payload, Some(&written_with(java))).unwrap();
        assert_eq!(read, Pojo { name: Some("alice".to_string()), age: 30 });

        let (v1, v2) = (Schema::<V1>::avro(), Schema::<V2>::avro());
        let first = v1.encode(&V1 { i: 7 }).unwrap();
        let read = v2.decode_with(&first, Some(&written_with(v1.info().definition()))).unwrap();
        assert_eq!(read, V2 { i: 7, j: None });
        let second = v2.encode(&V2 { i: 8, j: Some(9) }).unwrap();
        assert_eq!(
            v2.decode_with(&second, Some(&written_with(v2.info().definition()))).unwrap(),
            V2 { i: 8, j: Some(9) }
        );
        // Without a version, or with a schema of another type, a message reads with the consumer's own schema.
        assert_eq!(v2.decode_with(&second, None).unwrap(), V2 { i: 8, j: Some(9) });
        assert_eq!(v2.decode_with(&second, Some(Schema::int32().info())).unwrap(), V2 { i: 8, j: Some(9) });
    }

    #[test]
    fn records_that_dont_encode_or_decode() {
        let error = Schema::<Unencodable>::avro().encode(&Unencodable).unwrap_err().to_string();
        assert!(error.contains("the value doesn't encode in Avro"), "{error}");
        let schema = Schema::<V2>::avro();
        let error = schema.decode(&[0x80]).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Avro"), "{error}");
        let error = schema.decode_with(&[], Some(&written_with(b"{"))).unwrap_err().to_string();
        assert!(error.contains("isn't JSON"), "{error}");
        let error = schema.decode_with(&[0x80], Some(&written_with(br#""int""#))).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Avro"), "{error}");
        // A record of the first version doesn't have the field the bad one requires: Avro's resolution can't fill it.
        let v1 = Schema::<V1>::avro();
        let first = v1.encode(&V1 { i: 7 }).unwrap();
        let error = Schema::<Bad>::avro().decode_with(&first, Some(&written_with(v1.info().definition()))).unwrap_err();
        assert!(error.to_string().contains("the payload doesn't decode in Avro"), "{error}");
    }

    /// A key/value schema reads each part with its part of the schema the message was written with: a later
    /// version's value reads the earlier one's record.
    #[test]
    fn key_values_read_each_part_with_its_writer() {
        let first = Schema::key_value(Schema::string(), Schema::<V1>::avro());
        let second = Schema::key_value(Schema::string(), Schema::<V2>::avro());
        let payload = first.encode(&KeyValue { key: "k".to_string(), value: V1 { i: 7 } }).unwrap();
        let read = second.decode_with(&payload, Some(first.info())).unwrap();
        assert_eq!(read, KeyValue { key: "k".to_string(), value: V2 { i: 7, j: None } });
        // A key/value schema whose parts don't read fails the message.
        let mut broken = first.info().clone();
        broken.definition.truncate(2);
        let error = second.decode_with(&payload, Some(&broken)).unwrap_err().to_string();
        assert!(error.contains("ends in its key"), "{error}");
        // A key/value schema nested in another hands its Avro part its part too.
        let (first, second) = (Schema::key_value(first, Schema::bool()), Schema::key_value(second, Schema::bool()));
        let inner = KeyValue { key: "k".to_string(), value: V1 { i: 8 } };
        let payload = first.encode(&KeyValue { key: inner, value: true }).unwrap();
        let read = second.decode_with(&payload, Some(first.info())).unwrap();
        assert_eq!((read.key.value, read.value), (V2 { i: 8, j: None }, true));
    }
}
