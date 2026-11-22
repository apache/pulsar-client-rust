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

//! Generic records: values decoded with the schema each message was written with ([`Schema::auto_consume`]).

use std::collections::BTreeMap;
use std::sync::Arc;

use super::{Generic, KeyValue, Schema, SchemaInfo, SchemaType, boolean, sized, split_inline, utf8};
use crate::error::{Error, ErrorKind, Result};

/// A value decoded with the schema its message was written with: the values of [`Schema::auto_consume`], as Java
/// V5's `GenericRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericRecord {
    schema_type: SchemaType,
    value: GenericValue,
}

impl GenericRecord {
    /// The type of the schema the message was written with: [`SchemaType::Bytes`] for a message without one.
    pub fn schema_type(&self) -> SchemaType {
        self.schema_type
    }

    /// The value, as Java V5's `nativeObject()`.
    pub fn value(&self) -> &GenericValue {
        &self.value
    }

    pub fn into_value(self) -> GenericValue {
        self.value
    }
}

/// The value of a generic record, by the type of the schema it was written with.
#[non_exhaustive]
#[derive(Debug, Clone, PartialEq)]
pub enum GenericValue {
    /// A null key or value of a key/value pair, as Java writes one.
    Null,
    /// Raw bytes, and the payload of a schema of a type Java V5 doesn't name ([`SchemaType::None`]).
    Bytes(Vec<u8>),
    String(String),
    Boolean(bool),
    Int8(i8),
    Int16(i16),
    Int32(i32),
    Int64(i64),
    Float(f32),
    Double(f64),
    /// A key and a value, each by its own schema.
    KeyValue(Box<KeyValue<GenericValue, GenericValue>>),
}

impl Schema<GenericRecord> {
    /// Values decoded with the schema each message was written with, whatever it is: Java V5's
    /// `Schema.autoConsume()`. Before it delivers a message, the consumer fetches the schema of the message's version
    /// from the broker, once for each version; a message without one reads as bytes. A fetch that fails fails the
    /// message's [`value`](crate::Message::value), and the next message of that version fetches again.
    ///
    /// The broker doesn't check this schema against the topic's, so it reads any topic, and registers none. It
    /// decodes the primitive types and key/value; JSON, Avro and Protobuf values fail to decode, as this client
    /// doesn't read them yet. Producers don't take it: they fail to create with [`ErrorKind::InvalidConfiguration`], as
    /// Java v4's do.
    pub fn auto_consume() -> Self {
        Self {
            info: SchemaInfo::new("AutoConsume", SchemaType::AutoConsume),
            encode: Arc::new(|_: &GenericRecord| {
                Err(Error::new(ErrorKind::InvalidConfiguration, "auto_consume() only decodes"))
            }),
            decode: Arc::new(decode),
            generic: Generic::Whole,
        }
    }
}

/// Decodes a payload with the schema it was written with: as bytes without one.
fn decode(data: &[u8], writer: Option<&SchemaInfo>) -> Result<GenericRecord> {
    let bytes = SchemaInfo::new("Bytes", SchemaType::Bytes);
    let writer = writer.unwrap_or(&bytes);
    Ok(GenericRecord { schema_type: writer.schema_type, value: decode_value(data, writer)? })
}

/// A payload's value, by the schema it was written with.
fn decode_value(data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
    let name = writer.schema_type.java_name();
    Ok(match writer.schema_type {
        SchemaType::Bytes | SchemaType::None => GenericValue::Bytes(data.to_vec()),
        SchemaType::String => GenericValue::String(utf8(data)?),
        SchemaType::Boolean => GenericValue::Boolean(sized(name, data, boolean)?),
        SchemaType::Int8 => GenericValue::Int8(sized(name, data, i8::from_be_bytes)?),
        SchemaType::Int16 => GenericValue::Int16(sized(name, data, i16::from_be_bytes)?),
        SchemaType::Int32 => GenericValue::Int32(sized(name, data, i32::from_be_bytes)?),
        SchemaType::Int64 => GenericValue::Int64(sized(name, data, i64::from_be_bytes)?),
        SchemaType::Float => GenericValue::Float(sized(name, data, f32::from_be_bytes)?),
        SchemaType::Double => GenericValue::Double(sized(name, data, f64::from_be_bytes)?),
        SchemaType::KeyValue => key_value(data, writer)?,
        _ => {
            return Err(Error::new(
                ErrorKind::Other,
                format!("generic records of {name} schemas aren't supported yet"),
            ));
        }
    })
}

/// A key/value payload, as Java's `KeyValueSchemaInfo` reads its schema: the key's and the value's schemas from its
/// properties and definition. Only the inline encoding is read: Java's other one puts the key in the message's key.
fn key_value(data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
    if writer.properties.get("kv.encoding.type").is_some_and(|encoding| encoding == "SEPARATED") {
        let message = "key/value messages with the key in the message's key aren't supported yet";
        return Err(Error::new(ErrorKind::Other, message));
    }
    let (key_definition, value_definition) = split_inline(&writer.definition)?;
    let (key_schema, value_schema) = (part(writer, "key", key_definition)?, part(writer, "value", value_definition)?);
    let (key, value) = split_inline(data)?;
    let read = |data: Option<&[u8]>, schema| data.map_or(Ok(GenericValue::Null), |data| decode_value(data, schema));
    Ok(GenericValue::KeyValue(Box::new(KeyValue { key: read(key, &key_schema)?, value: read(value, &value_schema)? })))
}

/// The schema of a key/value schema's key or value, `side`: its name, type and properties, which the key/value
/// schema's properties record, and its part of the key/value schema's definition. Without a type, it is bytes, as in
/// Java.
fn part(writer: &SchemaInfo, side: &str, definition: Option<&[u8]>) -> Result<SchemaInfo> {
    let property = |name: &str| writer.properties.get(&format!("{side}.schema.{name}"));
    let properties = match property("properties").filter(|json| !json.is_empty()) {
        Some(json) => serde_json::from_str(json).map_err(|e| {
            Error::new(ErrorKind::Other, format!("the key/value schema's {side} properties aren't JSON: {e}"))
        })?,
        None => BTreeMap::new(),
    };
    Ok(SchemaInfo {
        name: property("name").cloned().unwrap_or_default(),
        schema_type: property("type").map_or(SchemaType::Bytes, |name| SchemaType::from_java_name(name)),
        definition: definition.unwrap_or_default().to_vec(),
        properties,
    })
}

#[cfg(test)]
mod tests {
    use apache_pulsar_protocol::proto;
    use apache_pulsar_protocol::proto::schema::Type;

    use super::*;
    use crate::schema::TYPES;

    /// A value written with `schema`, read back as a generic record.
    fn generic<T>(schema: &Schema<T>, value: &T) -> GenericRecord {
        Schema::auto_consume().decode_with(&schema.encode(value).unwrap(), Some(schema.info())).unwrap()
    }

    fn record(schema_type: SchemaType, value: GenericValue) -> GenericRecord {
        GenericRecord { schema_type, value }
    }

    fn pair(key: GenericValue, value: GenericValue) -> GenericValue {
        GenericValue::KeyValue(Box::new(KeyValue { key, value }))
    }

    /// A schema as the broker could return it, named after its topic.
    fn writer(schema_type: SchemaType, properties: &[(&str, &str)], definition: &[u8]) -> SchemaInfo {
        SchemaInfo {
            name: "acme/orders/events".to_string(),
            schema_type,
            definition: definition.to_vec(),
            properties: properties.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect(),
        }
    }

    #[test]
    fn generic_records_of_each_primitive_type() {
        let string = generic(&Schema::string(), &"héllo".to_string());
        assert_eq!(string, record(SchemaType::String, GenericValue::String("héllo".into())));
        assert_eq!(generic(&Schema::bool(), &true), record(SchemaType::Boolean, GenericValue::Boolean(true)));
        assert_eq!(generic(&Schema::int8(), &-7), record(SchemaType::Int8, GenericValue::Int8(-7)));
        assert_eq!(generic(&Schema::int16(), &12345), record(SchemaType::Int16, GenericValue::Int16(12345)));
        let int32 = generic(&Schema::int32(), &-123_456_789);
        assert_eq!(int32, record(SchemaType::Int32, GenericValue::Int32(-123_456_789)));
        assert_eq!(generic(&Schema::int64(), &(1 << 40)), record(SchemaType::Int64, GenericValue::Int64(1 << 40)));
        assert_eq!(generic(&Schema::float32(), &1.5), record(SchemaType::Float, GenericValue::Float(1.5)));
        assert_eq!(generic(&Schema::float64(), &-2.5), record(SchemaType::Double, GenericValue::Double(-2.5)));
    }

    /// Raw bytes declare no schema, so their messages have none: they read as bytes, as do the messages of a schema
    /// of a type Java V5 doesn't name.
    #[test]
    fn messages_without_a_schema_read_as_bytes() {
        let read = Schema::auto_consume().decode_with(&[1, 2], None).unwrap();
        assert_eq!((read.schema_type(), read.value()), (SchemaType::Bytes, &GenericValue::Bytes(vec![1, 2])));
        assert_eq!(read.into_value(), GenericValue::Bytes(vec![1, 2]));
        let date = writer(SchemaType::None, &[], &[]);
        let read = Schema::auto_consume().decode_with(&[0; 8], Some(&date)).unwrap();
        assert_eq!(read, record(SchemaType::None, GenericValue::Bytes(vec![0; 8])));
    }

    #[test]
    fn values_that_dont_decode() {
        let auto = Schema::auto_consume();
        let refused = |data: &[u8], writer: &SchemaInfo| auto.decode_with(data, Some(writer)).unwrap_err().to_string();
        assert!(refused(&[0, 1], Schema::int32().info()).contains("a INT32 value takes 4 bytes"));
        assert!(refused(&[0xff], Schema::string().info()).contains("not UTF-8"));
        // The types this client doesn't decode yet fail, naming themselves.
        let later = [
            (SchemaType::Json, "JSON"),
            (SchemaType::Avro, "AVRO"),
            (SchemaType::Protobuf, "PROTOBUF"),
            (SchemaType::ProtobufNative, "PROTOBUF_NATIVE"),
        ];
        for (schema_type, name) in later {
            let error = refused(b"{}", &writer(schema_type, &[], &[]));
            assert!(error.contains(&format!("generic records of {name} schemas")), "{error}");
        }
    }

    #[test]
    fn generic_key_values() {
        let schema = Schema::key_value(Schema::string(), Schema::int32());
        let read = generic(&schema, &KeyValue { key: "k".to_string(), value: 1 });
        let expected = pair(GenericValue::String("k".into()), GenericValue::Int32(1));
        assert_eq!(read, record(SchemaType::KeyValue, expected));

        // Java writes a null key or value with length -1.
        let null_key = [0xff, 0xff, 0xff, 0xff, 0, 0, 0, 4, 0, 0, 0, 1];
        let read = Schema::auto_consume().decode_with(&null_key, Some(schema.info())).unwrap();
        assert_eq!(read.into_value(), pair(GenericValue::Null, GenericValue::Int32(1)));

        let nested = Schema::key_value(schema, Schema::bool());
        let read = generic(&nested, &KeyValue { key: KeyValue { key: "k".to_string(), value: -1 }, value: true });
        let inner = pair(GenericValue::String("k".into()), GenericValue::Int32(-1));
        assert_eq!(read.into_value(), pair(inner, GenericValue::Boolean(true)));
    }

    /// A key/value schema with parts missing, as Java's `KeyValueSchemaInfo` reads it: a part without a type is bytes,
    /// one without a name or properties has none.
    #[test]
    fn key_value_schemas_as_java_reads_them() {
        let bare = writer(SchemaType::KeyValue, &[("key.schema.properties", "")], &[0; 8]);
        let read = Schema::auto_consume().decode_with(&[0, 0, 0, 1, 7, 0, 0, 0, 0], Some(&bare)).unwrap();
        assert_eq!(read.into_value(), pair(GenericValue::Bytes(vec![7]), GenericValue::Bytes(Vec::new())));
        let key = part(&bare, "key", None).unwrap();
        assert_eq!((key.name(), key.schema_type(), key.definition()), ("", SchemaType::Bytes, &[][..]));
        assert!(key.properties().is_empty());
    }

    #[test]
    fn key_value_schemas_and_payloads_that_dont_decode() {
        let auto = Schema::auto_consume();
        let refused = |data: &[u8], writer: &SchemaInfo| auto.decode_with(data, Some(writer)).unwrap_err().to_string();
        let separated = writer(SchemaType::KeyValue, &[("kv.encoding.type", "SEPARATED")], &[0; 8]);
        assert!(refused(&[0, 0, 0, 0], &separated).contains("with the key in the message's key"));
        let not_json = writer(SchemaType::KeyValue, &[("value.schema.properties", "{")], &[0; 8]);
        assert!(refused(&[0; 8], &not_json).contains("the key/value schema's value properties aren't JSON"));
        let truncated = writer(SchemaType::KeyValue, &[], &[0, 0, 0]);
        assert!(refused(&[0; 8], &truncated).contains("ends in its key"));
        let schema = Schema::key_value(Schema::string(), Schema::int32());
        assert!(refused(&[0, 0, 0, 1, b'k', 0, 0, 0, 1, 0], schema.info()).contains("a INT32 value takes 4 bytes"));
        assert!(refused(&[0, 0, 0, 9], schema.info()).contains("ends in its key"));
    }

    #[test]
    fn auto_consume_is_for_consumers_only() {
        let auto = Schema::auto_consume();
        let info = auto.info();
        assert_eq!((info.name(), info.schema_type()), ("AutoConsume", SchemaType::AutoConsume));
        let declared = info.to_proto().unwrap();
        assert_eq!((declared.name.as_str(), declared.r#type), ("AutoConsume", Type::AutoConsume));
        assert!(declared.schema_data.is_empty() && declared.properties.is_empty());
        let value = record(SchemaType::Bytes, GenericValue::Bytes(Vec::new()));
        assert_eq!(auto.encode(&value).unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        assert_eq!(auto.check_producer().unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        assert!(auto.check_consumer().is_ok());
        // Nor does a key/value schema with a part of it serve consumers or producers.
        let part = Schema::key_value(Schema::string(), Schema::auto_consume());
        assert_eq!(part.info().properties()["value.schema.type"], "AUTO_CONSUME");
        assert_eq!(part.check_consumer().unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        assert_eq!(part.check_producer().unwrap_err().kind(), ErrorKind::InvalidConfiguration);
        let typed = Schema::int32();
        assert!(typed.check_producer().is_ok() && typed.check_consumer().is_ok());
    }

    /// The schema the broker returns, of each of its types: those Java V5 doesn't name read as none.
    #[test]
    fn schemas_the_broker_returns() {
        let kinds = [
            (Type::None, SchemaType::Bytes),
            (Type::String, SchemaType::String),
            (Type::Json, SchemaType::Json),
            (Type::Protobuf, SchemaType::Protobuf),
            (Type::Avro, SchemaType::Avro),
            (Type::Bool, SchemaType::Boolean),
            (Type::Int8, SchemaType::Int8),
            (Type::Int16, SchemaType::Int16),
            (Type::Int32, SchemaType::Int32),
            (Type::Int64, SchemaType::Int64),
            (Type::Float, SchemaType::Float),
            (Type::Double, SchemaType::Double),
            (Type::Date, SchemaType::None),
            (Type::Time, SchemaType::None),
            (Type::Timestamp, SchemaType::None),
            (Type::KeyValue, SchemaType::KeyValue),
            (Type::Instant, SchemaType::None),
            (Type::LocalDate, SchemaType::None),
            (Type::LocalTime, SchemaType::None),
            (Type::LocalDateTime, SchemaType::None),
            (Type::ProtobufNative, SchemaType::ProtobufNative),
            (Type::AutoConsume, SchemaType::AutoConsume),
            (Type::External, SchemaType::None),
        ];
        let property = proto::KeyValue { key: "k".to_string(), value: "v".to_string(), ..Default::default() };
        for (kind, schema_type) in kinds {
            let schema = proto::Schema {
                name: "acme/orders/events".to_string(),
                schema_data: vec![1],
                r#type: kind,
                properties: vec![property.clone()],
                ..Default::default()
            };
            let info = SchemaInfo::from_proto(&schema);
            assert_eq!(info, writer(schema_type, &[("k", "v")], &[1]), "{kind:?}");
        }
        // Java's names, which key/value schemas record, both ways.
        for (schema_type, _, name) in TYPES {
            assert_eq!((schema_type.java_name(), SchemaType::from_java_name(name)), (name, schema_type));
        }
        assert_eq!(SchemaType::from_java_name("DATE"), SchemaType::None);
    }
}
