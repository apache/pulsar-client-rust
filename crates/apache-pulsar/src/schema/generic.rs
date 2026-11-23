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

use std::sync::Arc;

#[cfg(feature = "avro")]
use apache_avro::reader::datum::GenericDatumReader;
#[cfg(feature = "protobuf")]
use buffa_descriptor::ReflectMessage as _;

#[cfg(feature = "avro")]
use super::avro::undecodable;
use super::{Definitions, Generic, KeyValue, Schema, SchemaInfo, SchemaType, boolean, sized, split_inline, utf8};
use crate::error::{Error, ErrorKind, Result};

/// A value decoded with the schema its message was written with: the values of [`Schema::auto_consume`], as Java
/// V5's `GenericRecord`.
#[derive(Debug, Clone, PartialEq)]
pub struct GenericRecord {
    schema_type: SchemaType,
    value: GenericValue,
    fields: Vec<String>,
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

    /// The names of a record's fields, in the order of the schema it was written with, as Java V5's `fields()`: none
    /// for a value that isn't a JSON or an Avro record.
    pub fn fields(&self) -> &[String] {
        &self.fields
    }

    /// A field of a JSON, an Avro or a Protobuf record, as Java V5's `field(name)`: its value, as Java's record holds
    /// it, an Avro union's branch without the union and a Protobuf field the message doesn't have its default; or
    /// `None` for a field the record doesn't have, and for a value that isn't a record.
    #[cfg_attr(not(any(feature = "json", feature = "avro", feature = "protobuf")), expect(unused_variables))]
    pub fn field(&self, name: &str) -> Option<GenericValue> {
        match &self.value {
            #[cfg(feature = "json")]
            GenericValue::Json(serde_json::Value::Object(fields)) => fields.get(name).cloned().map(GenericValue::Json),
            #[cfg(feature = "avro")]
            GenericValue::Avro(apache_avro::types::Value::Record(fields)) => {
                fields.iter().find(|(field, _)| field == name).map(|(_, value)| GenericValue::Avro(branch(value)))
            }
            #[cfg(feature = "protobuf")]
            GenericValue::Protobuf(buffa_descriptor::Value::Message(message)) => {
                let field = message.message_descriptor().field_by_name(name)?;
                Some(GenericValue::Protobuf(message.get(field).to_owned()))
            }
            _ => None,
        }
    }
}

/// An Avro value as Java's record holds it: a union's branch without the union.
#[cfg(feature = "avro")]
fn branch(value: &apache_avro::types::Value) -> apache_avro::types::Value {
    match value {
        apache_avro::types::Value::Union(_, branch) => (**branch).clone(),
        value => value.clone(),
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
    /// A value of a JSON schema, as `serde_json` reads it: an object for a record.
    #[cfg(feature = "json")]
    Json(serde_json::Value),
    /// A value of an Avro schema, as apache-avro reads it with the schema it was written with: a record's fields come
    /// in that schema's order.
    #[cfg(feature = "avro")]
    Avro(apache_avro::types::Value),
    /// A value of a Protobuf schema, as buffa reads it with the descriptors it was written with: a message for a
    /// record.
    #[cfg(feature = "protobuf")]
    Protobuf(buffa_descriptor::Value),
}

impl Schema<GenericRecord> {
    /// Values decoded with the schema each message was written with, whatever it is: Java V5's
    /// `Schema.autoConsume()`. Before it delivers a message, the consumer fetches the schema of the message's version
    /// from the broker, once for each version; a message without one reads as bytes. A fetch that fails fails the
    /// message's [`value`](crate::Message::value), and the next message of that version fetches again.
    ///
    /// The broker doesn't check this schema against the topic's, so it reads any topic, and registers none. It
    /// decodes the primitive types, key/value, and with their features JSON, Avro and Protobuf's `PROTOBUF_NATIVE`.
    /// Values of Java V5's `PROTOBUF` fail to decode: its definition describes the message in Avro, not how Protobuf
    /// encodes it, and Java's own `autoConsume()` can't read them either. Producers don't take it: they fail to
    /// create with [`ErrorKind::InvalidConfiguration`], as Java v4's do.
    pub fn auto_consume() -> Self {
        let definitions = Definitions::default();
        Self {
            info: SchemaInfo::new("AutoConsume", SchemaType::AutoConsume),
            encode: Arc::new(|_: &GenericRecord| {
                Err(Error::new(ErrorKind::InvalidConfiguration, "auto_consume() only decodes"))
            }),
            decode: Arc::new(move |data: &[u8], writer: Option<&SchemaInfo>| decode(&definitions, data, writer)),
            generic: Generic::Whole,
        }
    }
}

/// Decodes a payload with the schema it was written with: as bytes without one.
fn decode(definitions: &Definitions, data: &[u8], writer: Option<&SchemaInfo>) -> Result<GenericRecord> {
    let bytes = SchemaInfo::new("Bytes", SchemaType::Bytes);
    let writer = writer.unwrap_or(&bytes);
    let value = decode_value(definitions, data, writer)?;
    let fields = fields(definitions, &value, writer);
    Ok(GenericRecord { schema_type: writer.schema_type, value, fields })
}

/// A payload's value, by the schema it was written with.
fn decode_value(definitions: &Definitions, data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
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
        SchemaType::KeyValue => key_value(definitions, data, writer)?,
        SchemaType::Json => json(data)?,
        SchemaType::Avro => avro(definitions, data, writer)?,
        SchemaType::ProtobufNative => protobuf_native(definitions, data, writer)?,
        _ => {
            return Err(Error::new(ErrorKind::Other, format!("generic records of {name} schemas aren't supported")));
        }
    })
}

/// The names of a record's fields, in the order of the schema it was written with: an Avro record's and a Protobuf
/// message's own, and a JSON one's from the Avro schema that describes it, or its own if that doesn't describe a
/// record.
#[cfg_attr(not(feature = "json"), expect(unused_variables))]
fn fields(definitions: &Definitions, value: &GenericValue, writer: &SchemaInfo) -> Vec<String> {
    match value {
        #[cfg(feature = "json")]
        GenericValue::Json(serde_json::Value::Object(fields)) => {
            match definitions.avro.parse(&writer.definition).as_deref() {
                Ok(apache_avro::Schema::Record(record)) => {
                    record.fields.iter().map(|field| field.name.clone()).collect()
                }
                _ => fields.keys().cloned().collect(),
            }
        }
        #[cfg(feature = "avro")]
        GenericValue::Avro(apache_avro::types::Value::Record(fields)) => {
            fields.iter().map(|(name, _)| name.clone()).collect()
        }
        #[cfg(feature = "protobuf")]
        GenericValue::Protobuf(buffa_descriptor::Value::Message(message)) => {
            message.message_descriptor().fields().iter().map(|field| field.name().to_string()).collect()
        }
        _ => Vec::new(),
    }
}

#[cfg(feature = "json")]
fn json(data: &[u8]) -> Result<GenericValue> {
    let value = serde_json::from_slice(data);
    value.map(GenericValue::Json).map_err(|e| Error::new(ErrorKind::Other, format!("the payload isn't JSON: {e}")))
}

#[cfg(not(feature = "json"))]
fn json(_: &[u8]) -> Result<GenericValue> {
    Err(Error::new(ErrorKind::Other, "reading JSON records takes the `json` feature"))
}

/// An Avro payload, read with the schema it was written with.
#[cfg(feature = "avro")]
fn avro(definitions: &Definitions, data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
    let schema = definitions.avro.parse(&writer.definition)?;
    let value = GenericDatumReader::builder(&schema).build().and_then(|reader| reader.read_value(&mut &data[..]));
    value.map(GenericValue::Avro).map_err(undecodable)
}

#[cfg(not(feature = "avro"))]
fn avro(_: &Definitions, _: &[u8], _: &SchemaInfo) -> Result<GenericValue> {
    Err(Error::new(ErrorKind::Other, "reading Avro records takes the `avro` feature"))
}

/// A `PROTOBUF_NATIVE` payload, read with the descriptors it was written with.
#[cfg(feature = "protobuf")]
fn protobuf_native(definitions: &Definitions, data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
    let message = definitions.protobuf.read(&writer.definition, data)?;
    Ok(GenericValue::Protobuf(buffa_descriptor::Value::Message(message)))
}

#[cfg(not(feature = "protobuf"))]
fn protobuf_native(_: &Definitions, _: &[u8], _: &SchemaInfo) -> Result<GenericValue> {
    Err(Error::new(ErrorKind::Other, "reading Protobuf records takes the `protobuf` feature"))
}

/// A key/value payload, as Java's `KeyValueSchemaInfo` reads its schema: the key's and the value's schemas from its
/// properties and definition. Only the inline encoding is read: Java's other one puts the key in the message's key.
fn key_value(definitions: &Definitions, data: &[u8], writer: &SchemaInfo) -> Result<GenericValue> {
    if writer.properties.get("kv.encoding.type").is_some_and(|encoding| encoding == "SEPARATED") {
        let message = "key/value messages with the key in the message's key aren't supported yet";
        return Err(Error::new(ErrorKind::Other, message));
    }
    let (key_schema, value_schema) = writer.parts()?;
    let (key, value) = split_inline(data)?;
    let read = |data: Option<&[u8]>, schema| {
        data.map_or(Ok(GenericValue::Null), |data| decode_value(definitions, data, schema))
    };
    Ok(GenericValue::KeyValue(Box::new(KeyValue { key: read(key, &key_schema)?, value: read(value, &value_schema)? })))
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
        GenericRecord { schema_type, value, fields: Vec::new() }
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
        #[cfg(feature = "json")]
        assert!(refused(b"{", &writer(SchemaType::Json, &[], b"\"int\"")).contains("the payload isn't JSON"));
        #[cfg(feature = "avro")]
        {
            assert!(refused(&[], &writer(SchemaType::Avro, &[], b"\"int\"")).contains("doesn't decode in Avro"));
            assert!(refused(&[], &writer(SchemaType::Avro, &[], b"{")).contains("isn't JSON"));
        }
        // Without their features, JSON and Avro records fail to decode, naming the feature.
        #[cfg(not(feature = "json"))]
        assert!(refused(b"{}", &writer(SchemaType::Json, &[], &[])).contains("takes the `json` feature"));
        #[cfg(not(feature = "avro"))]
        assert!(refused(&[], &writer(SchemaType::Avro, &[], &[])).contains("takes the `avro` feature"));
        // The types this client doesn't decode yet fail, naming themselves.
        let error = refused(b"{}", &writer(SchemaType::Protobuf, &[], &[]));
        assert!(error.contains("generic records of PROTOBUF schemas"), "{error}");
        #[cfg(not(feature = "protobuf"))]
        assert!(refused(&[], &writer(SchemaType::ProtobufNative, &[], &[])).contains("takes the `protobuf` feature"));
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
    /// Java's definition of the record its tool writes: its reflection orders the fields otherwise than the record's
    /// JSON.
    #[cfg(any(feature = "json", feature = "avro"))]
    const JAVA_POJO: &[u8] = br#"{"type":"record","name":"Pojo","namespace":"SchemaTool","fields":[
        {"name":"age","type":"int"},{"name":"name","type":["null","string"],"default":null}]}"#;

    #[cfg(feature = "json")]
    #[test]
    fn generic_json_records() {
        use serde_json::json;

        let auto = Schema::auto_consume();
        let read = auto.decode_with(br#"{"name":"alice","age":30}"#, Some(&writer(SchemaType::Json, &[], JAVA_POJO)));
        let read = read.unwrap();
        assert_eq!(
            (read.schema_type(), read.value()),
            (SchemaType::Json, &GenericValue::Json(json!({"name": "alice", "age": 30})))
        );
        assert_eq!(read.fields(), ["age", "name"], "in the definition's order");
        assert_eq!(read.field("name"), Some(GenericValue::Json(json!("alice"))));
        assert_eq!(read.field("city"), None);
        // A definition that doesn't describe a record: the object's fields, in serde_json's order.
        let not_a_record = writer(SchemaType::Json, &[], br#""string""#);
        assert_eq!(auto.decode_with(br#"{"b":1,"a":2}"#, Some(&not_a_record)).unwrap().fields(), ["a", "b"]);
        // A value that isn't an object has no fields.
        let read = auto.decode_with(b"7", Some(&not_a_record)).unwrap();
        assert!(read.fields().is_empty() && read.field("a").is_none());
    }

    #[cfg(feature = "avro")]
    #[test]
    fn generic_avro_records() {
        use apache_avro::types::Value;

        let auto = Schema::auto_consume();
        let payload = [60, 2, 10, b'a', b'l', b'i', b'c', b'e'];
        let read = auto.decode_with(&payload, Some(&writer(SchemaType::Avro, &[], JAVA_POJO))).unwrap();
        assert_eq!(read.schema_type(), SchemaType::Avro);
        assert_eq!(read.fields(), ["age", "name"]);
        assert_eq!(read.field("age"), Some(GenericValue::Avro(Value::Int(30))));
        assert_eq!(read.field("name"), Some(GenericValue::Avro(Value::String("alice".to_string()))));
        assert_eq!(read.field("city"), None);
        let nameless = auto.decode_with(&[60, 0], Some(&writer(SchemaType::Avro, &[], JAVA_POJO))).unwrap();
        assert_eq!(nameless.field("name"), Some(GenericValue::Avro(Value::Null)), "the union's null branch");
        // A value that isn't a record has no fields.
        let read = auto.decode_with(&[60], Some(&writer(SchemaType::Avro, &[], br#""int""#))).unwrap();
        assert_eq!(read.value(), &GenericValue::Avro(Value::Int(30)));
        assert!(read.fields().is_empty() && read.field("a").is_none());
        // A key/value of an Avro record: each part read with its own schema.
        let schema = Schema::key_value(Schema::string(), Schema::<Pojo>::avro());
        let read = generic(&schema, &KeyValue { key: "k".to_string(), value: Pojo { name: None, age: 7 } });
        let record = Value::Record(vec![
            ("name".to_string(), Value::Union(0, Box::new(Value::Null))),
            ("age".to_string(), Value::Int(7)),
        ]);
        assert_eq!(read.into_value(), pair(GenericValue::String("k".into()), GenericValue::Avro(record)));
    }

    #[cfg(feature = "avro")]
    #[derive(Debug, PartialEq, serde::Serialize, serde::Deserialize, apache_avro::AvroSchema)]
    struct Pojo {
        name: Option<String>,
        age: i32,
    }

    /// A Protobuf message, read with the descriptors of the schema it was written with: its fields in its
    /// descriptor's order, and a field the message doesn't have read as its default, as in Java.
    #[cfg(feature = "protobuf")]
    #[test]
    fn generic_protobuf_records() {
        use buffa_descriptor::Value;
        use buffa_types::google::protobuf::Timestamp;

        let schema = Schema::<Timestamp>::protobuf_native();
        let read = generic(&schema, &Timestamp { seconds: 5, ..Default::default() });
        assert_eq!(read.schema_type(), SchemaType::ProtobufNative);
        assert!(matches!(read.value(), GenericValue::Protobuf(Value::Message(_))));
        assert_eq!(read.fields(), ["seconds", "nanos"]);
        assert_eq!(read.field("seconds"), Some(GenericValue::Protobuf(Value::I64(5))));
        assert_eq!(read.field("nanos"), Some(GenericValue::Protobuf(Value::I32(0))));
        assert_eq!(read.field("city"), None);
        let error = Schema::auto_consume().decode_with(&[0xff], Some(schema.info())).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Protobuf"), "{error}");
    }

    /// The fields of a richer message: a repeated one, an enum, a message the record doesn't have, and a map.
    #[cfg(feature = "protobuf")]
    #[test]
    fn generic_protobuf_records_of_each_kind_of_field() {
        use buffa_descriptor::Value;
        use buffa_types::google::protobuf::{Struct, Syntax, Type, value};

        let kind = Type { oneofs: vec!["o".to_string()], syntax: Syntax::SYNTAX_PROTO3.into(), ..Default::default() };
        let read = generic(&Schema::<Type>::protobuf_native(), &kind);
        assert_eq!(read.fields(), ["name", "fields", "oneofs", "options", "source_context", "syntax", "edition"]);
        let field = |name: &str| read.field(name);
        assert_eq!(field("oneofs"), Some(GenericValue::Protobuf(Value::List(vec![Value::String("o".to_string())]))));
        assert_eq!(field("fields"), Some(GenericValue::Protobuf(Value::List(Vec::new()))));
        assert_eq!(field("syntax"), Some(GenericValue::Protobuf(Value::EnumNumber(1))));
        let unset = field("source_context");
        assert!(
            matches!(unset, Some(GenericValue::Protobuf(Value::Message(_)))),
            "a message the record doesn't have: {unset:?}"
        );

        let entry = buffa_types::google::protobuf::Value {
            kind: Some(value::Kind::StringValue("v".to_string())),
            ..Default::default()
        };
        let structure = Struct { fields: [("k".to_string(), entry)].into_iter().collect(), ..Default::default() };
        let read = generic(&Schema::<Struct>::protobuf_native(), &structure);
        assert!(
            matches!(read.field("fields"), Some(GenericValue::Protobuf(Value::Map(_)))),
            "{:?}",
            read.field("fields")
        );
    }

    #[test]
    fn key_value_schemas_as_java_reads_them() {
        let bare = writer(SchemaType::KeyValue, &[("key.schema.properties", "")], &[0; 8]);
        let read = Schema::auto_consume().decode_with(&[0, 0, 0, 1, 7, 0, 0, 0, 0], Some(&bare)).unwrap();
        assert_eq!(read.into_value(), pair(GenericValue::Bytes(vec![7]), GenericValue::Bytes(Vec::new())));
        let key = bare.part("key", None).unwrap();
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
