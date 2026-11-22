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

//! Schemas: how values become payloads, and what the broker is told about them (docs/7-api.md §8).
//!
//! The schema is topic-level (*data-model* §6): the broker keeps one history of schemas for a scalable topic, which
//! every segment shares, and checks a producer's or a consumer's schema against it as it registers on a segment.

mod generic;

use std::collections::BTreeMap;
use std::sync::Arc;

use apache_pulsar_protocol::proto;
use apache_pulsar_protocol::proto::schema::Type;
use bytes::{BufMut, Bytes, BytesMut};

pub use self::generic::{GenericRecord, GenericValue};
use crate::error::{Error, ErrorKind, Result};

type Encode<T> = Arc<dyn Fn(&T) -> Result<Bytes> + Send + Sync>;
/// Decodes a payload. Only [`Schema::auto_consume`] reads the schema it was written with: `None` for a message
/// without one.
type Decode<T> = Arc<dyn Fn(&[u8], Option<&SchemaInfo>) -> Result<T> + Send + Sync>;

/// How values of type `T` are encoded, and the schema the broker is told about. Cheap to clone.
///
/// The built-in schemas encode as Java's do, so that the clients read each other's messages:
///
/// | Schema | `T` | Payload |
/// |---|---|---|
/// | `bytes()` | `Vec<u8>` | as given; the broker isn't told about a schema |
/// | `string()` | `String` | UTF-8 |
/// | `bool()` | `bool` | one byte, 1 or 0 |
/// | `int8()` to `int64()` | `i8` to `i64` | big endian |
/// | `float32()`, `float64()` | `f32`, `f64` | IEEE 754, big endian |
/// | `key_value(key, value)` | [`KeyValue<K, V>`] | the key's length, 4 bytes big endian, and bytes, then the value's |
/// | `auto_consume()` | [`GenericRecord`] | decoded with the schema each message was written with |
pub struct Schema<T> {
    info: SchemaInfo,
    encode: Encode<T>,
    decode: Decode<T>,
    generic: Generic,
}

/// Whether a schema decodes with the schema each message was written with.
#[derive(Debug, Clone, Copy, PartialEq, Eq)]
enum Generic {
    /// No: it decodes with its own.
    No,
    /// It does: [`Schema::auto_consume`].
    Whole,
    /// A key/value schema with a key or a value of [`Schema::auto_consume`], which neither producers nor consumers
    /// take.
    Part,
}

/// The type of a schema, as Java V5's `SchemaType` names it.
#[non_exhaustive]
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash)]
pub enum SchemaType {
    /// Raw bytes. A producer or consumer with this type declares no schema, as Java's.
    Bytes,
    String,
    /// [`Schema::bool`].
    Boolean,
    Int8,
    Int16,
    Int32,
    Int64,
    /// [`Schema::float32`].
    Float,
    /// [`Schema::float64`].
    Double,
    /// Java's JSON schema, which describes its records in Avro.
    Json,
    Avro,
    /// Java's `Schema.protobuf`, which describes its messages in Avro.
    Protobuf,
    /// Java's Protobuf schema that holds the messages' descriptors.
    ProtobufNative,
    /// [`Schema::key_value`].
    KeyValue,
    /// [`Schema::auto_consume`], which only consumers declare.
    AutoConsume,
    /// A type Java V5 doesn't name, such as Java v4's date and time types.
    None,
}

/// Each type, as the broker's protocol and Java name it. Bytes and none declare no schema; a schema of the protocol's
/// `None` type reads as bytes, as in Java.
const TYPES: [(SchemaType, Type, &str); 16] = [
    (SchemaType::Bytes, Type::None, "BYTES"),
    (SchemaType::String, Type::String, "STRING"),
    (SchemaType::Boolean, Type::Bool, "BOOLEAN"),
    (SchemaType::Int8, Type::Int8, "INT8"),
    (SchemaType::Int16, Type::Int16, "INT16"),
    (SchemaType::Int32, Type::Int32, "INT32"),
    (SchemaType::Int64, Type::Int64, "INT64"),
    (SchemaType::Float, Type::Float, "FLOAT"),
    (SchemaType::Double, Type::Double, "DOUBLE"),
    (SchemaType::Json, Type::Json, "JSON"),
    (SchemaType::Avro, Type::Avro, "AVRO"),
    (SchemaType::Protobuf, Type::Protobuf, "PROTOBUF"),
    (SchemaType::ProtobufNative, Type::ProtobufNative, "PROTOBUF_NATIVE"),
    (SchemaType::KeyValue, Type::KeyValue, "KEY_VALUE"),
    (SchemaType::AutoConsume, Type::AutoConsume, "AUTO_CONSUME"),
    (SchemaType::None, Type::None, "NONE"),
];

impl SchemaType {
    /// The name of Java's `SchemaType` constant, which a key/value schema records for its key's and value's.
    fn java_name(self) -> &'static str {
        TYPES.iter().find(|(schema_type, ..)| *schema_type == self).map_or("NONE", |(.., name)| name)
    }

    /// The type of Java's `SchemaType` constant `name`: [`SchemaType::None`] for one Java V5 doesn't have.
    fn from_java_name(name: &str) -> Self {
        TYPES.iter().find(|(.., java)| *java == name).map_or(SchemaType::None, |(schema_type, ..)| *schema_type)
    }

    /// The type of a schema the broker recorded: [`SchemaType::None`] for one Java V5 doesn't have.
    fn from_proto(kind: Type) -> Self {
        TYPES.iter().find(|(_, proto, _)| *proto == kind).map_or(SchemaType::None, |(schema_type, ..)| *schema_type)
    }
}

/// A schema as the broker records it: Java V5's `SchemaInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    name: String,
    schema_type: SchemaType,
    definition: Vec<u8>,
    properties: BTreeMap<String, String>,
}

impl SchemaInfo {
    fn new(name: &str, schema_type: SchemaType) -> Self {
        Self { name: name.to_string(), schema_type, definition: Vec::new(), properties: BTreeMap::new() }
    }

    /// Java's key/value schema: the key's and the value's definitions in its definition, as an inline key/value
    /// payload holds a key and a value, and their names, types and properties in its properties, with the encoding.
    fn key_value(key: &SchemaInfo, value: &SchemaInfo) -> Self {
        let mut properties = BTreeMap::from([("kv.encoding.type".to_string(), "INLINE".to_string())]);
        for (side, info) in [("key", key), ("value", value)] {
            let nested = serde_json::to_string(&info.properties).expect("strings are JSON");
            properties.insert(format!("{side}.schema.name"), info.name.clone());
            properties.insert(format!("{side}.schema.type"), info.schema_type.java_name().to_string());
            properties.insert(format!("{side}.schema.properties"), nested);
        }
        let definition = inline(&key.definition, &value.definition).to_vec();
        Self { name: "KeyValue".to_string(), schema_type: SchemaType::KeyValue, definition, properties }
    }

    /// The name, as Java names its schema of the same type.
    pub fn name(&self) -> &str {
        &self.name
    }

    pub fn schema_type(&self) -> SchemaType {
        self.schema_type
    }

    /// What the broker checks compatibility on, alongside the type. Empty for the primitive types.
    pub fn definition(&self) -> &[u8] {
        &self.definition
    }

    /// What else the schema records, such as a key/value schema's encoding. Empty for the primitive types.
    pub fn properties(&self) -> &BTreeMap<String, String> {
        &self.properties
    }

    /// What a producer or a consumer declares on registering: nothing for raw bytes, as Java declares nothing.
    pub(crate) fn to_proto(&self) -> Option<proto::Schema> {
        if self.schema_type == SchemaType::Bytes {
            return None;
        }
        let kind = TYPES.iter().find(|(schema_type, ..)| *schema_type == self.schema_type).map(|(_, kind, _)| *kind);
        let properties = self.properties.iter().map(|(key, value)| proto::KeyValue {
            key: key.clone(),
            value: value.clone(),
            ..Default::default()
        });
        Some(proto::Schema {
            name: self.name.clone(),
            schema_data: self.definition.clone(),
            r#type: kind.unwrap_or_default(),
            properties: properties.collect(),
            ..Default::default()
        })
    }

    /// A schema as the broker records it, such as the one a message was written with. The broker names it after
    /// the topic.
    pub(crate) fn from_proto(schema: &proto::Schema) -> Self {
        Self {
            name: schema.name.clone(),
            schema_type: SchemaType::from_proto(schema.r#type),
            definition: schema.schema_data.clone(),
            properties: schema.properties.iter().map(|p| (p.key.clone(), p.value.clone())).collect(),
        }
    }
}

impl<T> Clone for Schema<T> {
    fn clone(&self) -> Self {
        Self {
            info: self.info.clone(),
            encode: self.encode.clone(),
            decode: self.decode.clone(),
            generic: self.generic,
        }
    }
}

impl<T> std::fmt::Debug for Schema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.debug_struct("Schema").field("name", &self.info.name).field("type", &self.info.schema_type).finish()
    }
}

impl Schema<Vec<u8>> {
    /// Raw bytes, as given: Java's `Schema.bytes()`.
    pub fn bytes() -> Self {
        Self {
            info: SchemaInfo::new("Bytes", SchemaType::Bytes),
            encode: Arc::new(|value: &Vec<u8>| Ok(Bytes::copy_from_slice(value))),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| Ok(data.to_vec())),
            generic: Generic::No,
        }
    }
}

impl Schema<String> {
    /// UTF-8 text: Java's `Schema.string()`.
    pub fn string() -> Self {
        Self {
            info: SchemaInfo::new("String", SchemaType::String),
            encode: Arc::new(|value: &String| Ok(Bytes::copy_from_slice(value.as_bytes()))),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| utf8(data)),
            generic: Generic::No,
        }
    }
}

/// A string payload: UTF-8 text.
fn utf8(data: &[u8]) -> Result<String> {
    String::from_utf8(data.to_vec()).map_err(|e| Error::new(ErrorKind::Other, format!("the payload is not UTF-8: {e}")))
}

impl Schema<bool> {
    /// A boolean, as one byte: Java's `Schema.bool()`. Any byte but 0 reads as `true`, as in Java.
    pub fn bool() -> Self {
        Self::fixed("Boolean", SchemaType::Boolean, |value| [u8::from(*value)], boolean)
    }
}

/// A boolean's byte: any but 0 is `true`, as in Java.
fn boolean([byte]: [u8; 1]) -> bool {
    byte != 0
}

impl Schema<i8> {
    /// Java's `Schema.int8()`.
    pub fn int8() -> Self {
        Self::fixed("INT8", SchemaType::Int8, |value| value.to_be_bytes(), i8::from_be_bytes)
    }
}

impl Schema<i16> {
    /// Java's `Schema.int16()`.
    pub fn int16() -> Self {
        Self::fixed("INT16", SchemaType::Int16, |value| value.to_be_bytes(), i16::from_be_bytes)
    }
}

impl Schema<i32> {
    /// Java's `Schema.int32()`.
    pub fn int32() -> Self {
        Self::fixed("INT32", SchemaType::Int32, |value| value.to_be_bytes(), i32::from_be_bytes)
    }
}

impl Schema<i64> {
    /// Java's `Schema.int64()`.
    pub fn int64() -> Self {
        Self::fixed("INT64", SchemaType::Int64, |value| value.to_be_bytes(), i64::from_be_bytes)
    }
}

impl Schema<f32> {
    /// Java's `Schema.float32()`.
    pub fn float32() -> Self {
        Self::fixed("Float", SchemaType::Float, |value| value.to_be_bytes(), f32::from_be_bytes)
    }
}

impl Schema<f64> {
    /// Java's `Schema.float64()`.
    pub fn float64() -> Self {
        Self::fixed("Double", SchemaType::Double, |value| value.to_be_bytes(), f64::from_be_bytes)
    }
}

/// A key and a value: the values of [`Schema::key_value`], as Java's `KeyValue`.
#[derive(Debug, Clone, Copy, PartialEq, Eq, Hash, Default)]
pub struct KeyValue<K, V> {
    pub key: K,
    pub value: V,
}

impl<K: 'static, V: 'static> Schema<KeyValue<K, V>> {
    /// A key and a value in one payload, each encoded with its own schema, as Java's `Schema.KeyValue(key, value)`
    /// encodes them by default (`INLINE`): the key's length, 4 bytes big endian, and its bytes, then the value's. Java
    /// V5's factory has no key/value schema, but Java's other one reads and writes the same messages. Java can write
    /// a null key or value, which fails to decode here.
    ///
    /// The key is the payload's: a message's key, which routes it, is set apart (`MessageBuilder::key`).
    ///
    /// Neither producers nor consumers take a key or a value of [`Schema::auto_consume`], which reads key/value
    /// messages whole: they fail with [`ErrorKind::InvalidConfiguration`].
    pub fn key_value(key: Schema<K>, value: Schema<V>) -> Self {
        let info = SchemaInfo::key_value(&key.info, &value.info);
        let generic =
            if (key.generic, value.generic) == (Generic::No, Generic::No) { Generic::No } else { Generic::Part };
        let (encode_key, encode_value) = (key.encode, value.encode);
        let (decode_key, decode_value) = (key.decode, value.decode);
        Self {
            info,
            encode: Arc::new(move |pair: &KeyValue<K, V>| {
                Ok(inline(&encode_key(&pair.key)?, &encode_value(&pair.value)?))
            }),
            decode: Arc::new(move |data: &[u8], _: Option<&SchemaInfo>| {
                let (key, value) = split_inline(data)?;
                Ok(KeyValue {
                    key: decode_key(not_null(key, "key")?, None)?,
                    value: decode_value(not_null(value, "value")?, None)?,
                })
            }),
            generic,
        }
    }
}

/// An inline key/value payload, as Java writes it: the key's length, 4 bytes big endian, and its bytes, then the
/// value's.
fn inline(key: &[u8], value: &[u8]) -> Bytes {
    let mut payload = BytesMut::with_capacity(8 + key.len() + value.len());
    for part in [key, value] {
        payload.put_i32(part.len() as i32);
        payload.put_slice(part);
    }
    payload.freeze()
}

/// The key and the value of a key/value payload, each `None` if null.
type Parts<'a> = (Option<&'a [u8]>, Option<&'a [u8]>);

/// The key and the value of an inline key/value payload, each `None` if null, as Java writes a null one: length -1,
/// and no bytes. As in Java, bytes after the value are left.
fn split_inline(data: &[u8]) -> Result<Parts<'_>> {
    let (key, rest) = inline_part(data, "key")?;
    let (value, _) = inline_part(rest, "value")?;
    Ok((key, value))
}

/// One part of an inline key/value payload, and what follows it.
fn inline_part<'a>(data: &'a [u8], part: &str) -> Result<(Option<&'a [u8]>, &'a [u8])> {
    let truncated = || Error::new(ErrorKind::Other, format!("the key/value payload ends in its {part}"));
    let Some((length, rest)) = data.split_first_chunk::<4>() else { return Err(truncated()) };
    let Ok(length) = usize::try_from(i32::from_be_bytes(*length)) else { return Ok((None, rest)) };
    match rest.split_at_checked(length) {
        Some((bytes, rest)) => Ok((Some(bytes), rest)),
        None => Err(truncated()),
    }
}

/// A part of a key/value payload that a typed schema can decode: not a null one.
fn not_null<'a>(part: Option<&'a [u8]>, name: &str) -> Result<&'a [u8]> {
    part.ok_or_else(|| Error::new(ErrorKind::Other, format!("the key/value payload's {name} is null")))
}

impl<T: 'static> Schema<T> {
    /// A schema of values that take `N` bytes each, which a payload of another size can't hold.
    fn fixed<const N: usize>(
        name: &'static str,
        schema_type: SchemaType,
        to: fn(&T) -> [u8; N],
        from: fn([u8; N]) -> T,
    ) -> Self {
        Self {
            info: SchemaInfo::new(name, schema_type),
            encode: Arc::new(move |value: &T| Ok(Bytes::copy_from_slice(&to(value)))),
            decode: Arc::new(move |data: &[u8], _: Option<&SchemaInfo>| sized(name, data, from)),
            generic: Generic::No,
        }
    }
}

/// A value of `N` bytes, which a payload of another size can't hold.
fn sized<const N: usize, T>(name: &str, data: &[u8], from: fn([u8; N]) -> T) -> Result<T> {
    match <[u8; N]>::try_from(data) {
        Ok(bytes) => Ok(from(bytes)),
        Err(_) => Err(Error::new(
            ErrorKind::Other,
            format!("a {name} value takes {N} bytes, but the payload has {}", data.len()),
        )),
    }
}

impl<T> Schema<T> {
    /// The schema the broker is told about.
    pub fn info(&self) -> &SchemaInfo {
        &self.info
    }

    pub(crate) fn encode(&self, value: &T) -> Result<Bytes> {
        (self.encode)(value)
    }

    /// Decodes a payload written with `writer`, which only [`Schema::auto_consume`] reads: `None` for a message
    /// without a schema.
    pub(crate) fn decode_with(&self, data: &[u8], writer: Option<&SchemaInfo>) -> Result<T> {
        (self.decode)(data, writer)
    }

    #[cfg(test)]
    pub(crate) fn decode(&self, data: &[u8]) -> Result<T> {
        self.decode_with(data, None)
    }

    /// Fails for a schema a producer can't encode with: [`Schema::auto_consume`], which Java v4's producers refuse too,
    /// and a key/value schema with a part of it.
    pub(crate) fn check_producer(&self) -> Result<()> {
        self.generic.check_producer()
    }

    /// Fails for a key/value schema with a part of [`Schema::auto_consume`], which reads key/value messages whole.
    pub(crate) fn check_consumer(&self) -> Result<()> {
        self.generic.check_consumer()
    }
}

impl Generic {
    fn check_producer(self) -> Result<()> {
        match self {
            Generic::No => Ok(()),
            _ => Err(Error::new(ErrorKind::InvalidConfiguration, "auto_consume() is a schema for consumers only")),
        }
    }

    fn check_consumer(self) -> Result<()> {
        match self {
            Generic::Part => Err(Error::new(
                ErrorKind::InvalidConfiguration,
                "a key/value schema can't hold auto_consume(), which reads key/value messages whole",
            )),
            _ => Ok(()),
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn round_trip<T: PartialEq + std::fmt::Debug>(schema: Schema<T>, value: T, payload: &[u8]) {
        assert_eq!(&schema.encode(&value).unwrap()[..], payload, "{schema:?}");
        assert_eq!(schema.decode(payload).unwrap(), value, "{schema:?}");
        // A payload of another size is none of the schema's values.
        let mut longer = payload.to_vec();
        longer.push(0);
        assert_eq!(schema.decode(&longer).unwrap_err().kind(), ErrorKind::Other, "{schema:?}");
        assert_eq!(schema.decode(&[]).unwrap_err().kind(), ErrorKind::Other, "{schema:?}");
    }

    #[test]
    fn bytes_and_text() {
        let bytes = Schema::bytes();
        assert_eq!(&bytes.encode(&vec![1, 2, 3]).unwrap()[..], [1, 2, 3]);
        assert_eq!(bytes.decode(&[1, 2, 3]).unwrap(), [1, 2, 3]);
        let text = Schema::string();
        assert_eq!(text.decode(&text.encode(&"héllo".to_string()).unwrap()).unwrap(), "héllo");
        assert_eq!(text.decode(&[0xff]).unwrap_err().kind(), ErrorKind::Other);
    }

    /// The payloads Java's primitive schemas write for the same values.
    #[test]
    fn primitives_encode_as_java_does() {
        round_trip(Schema::bool(), true, &[1]);
        round_trip(Schema::bool(), false, &[0]);
        assert!(Schema::bool().decode(&[2]).unwrap());
        round_trip(Schema::int8(), -7, &[0xf9]);
        round_trip(Schema::int16(), 12345, &[0x30, 0x39]);
        round_trip(Schema::int32(), 123_456_789, &[0x07, 0x5b, 0xcd, 0x15]);
        round_trip(Schema::int64(), 1_234_567_890_123, &[0x00, 0x00, 0x01, 0x1f, 0x71, 0xfb, 0x04, 0xcb]);
        round_trip(Schema::float32(), 1.1, &[0x3f, 0x8c, 0xcc, 0xcd]);
        round_trip(Schema::float64(), 1.1, &[0x3f, 0xf1, 0x99, 0x99, 0x99, 0x99, 0x99, 0x9a]);
    }

    /// The names and types Java declares for the same schemas. Raw bytes declare nothing.
    #[test]
    fn schemas_declare_what_java_declares() {
        let declared = |info: &SchemaInfo| info.to_proto().map(|schema| (schema.name, schema.r#type));
        assert_eq!(declared(Schema::bytes().info()), None);
        assert_eq!(Schema::bytes().info().schema_type(), SchemaType::Bytes);
        let infos = [
            Schema::string().info().clone(),
            Schema::bool().info().clone(),
            Schema::int8().info().clone(),
            Schema::int16().info().clone(),
            Schema::int32().info().clone(),
            Schema::int64().info().clone(),
            Schema::float32().info().clone(),
            Schema::float64().info().clone(),
        ];
        let expected = [
            ("String", Type::String),
            ("Boolean", Type::Bool),
            ("INT8", Type::Int8),
            ("INT16", Type::Int16),
            ("INT32", Type::Int32),
            ("INT64", Type::Int64),
            ("Float", Type::Float),
            ("Double", Type::Double),
        ];
        for (info, (name, kind)) in infos.iter().zip(expected) {
            assert_eq!(declared(info), Some((name.to_string(), kind)));
            assert_eq!(info.name(), name);
            assert!(info.definition().is_empty());
        }
        let types: Vec<SchemaType> = infos.iter().map(SchemaInfo::schema_type).collect();
        assert_eq!(
            types,
            [
                SchemaType::String,
                SchemaType::Boolean,
                SchemaType::Int8,
                SchemaType::Int16,
                SchemaType::Int32,
                SchemaType::Int64,
                SchemaType::Float,
                SchemaType::Double
            ]
        );
        assert_eq!(format!("{:?}", Schema::int32()), r#"Schema { name: "INT32", type: Int32 }"#);
    }

    fn properties(entries: &[(&str, &str)]) -> BTreeMap<String, String> {
        entries.iter().map(|(key, value)| (key.to_string(), value.to_string())).collect()
    }

    /// The payload and the schema Java's `Schema.KeyValue(Schema.STRING, Schema.INT32)` writes.
    #[test]
    fn key_values_encode_as_java_does() {
        let schema = Schema::key_value(Schema::string(), Schema::int32());
        let pair = KeyValue { key: "k".to_string(), value: 1 };
        let payload = [0, 0, 0, 1, b'k', 0, 0, 0, 4, 0, 0, 0, 1];
        assert_eq!(&schema.encode(&pair).unwrap()[..], payload);
        assert_eq!(schema.decode(&payload).unwrap(), pair);
        let info = schema.info();
        assert_eq!(
            (info.name(), info.schema_type(), info.definition()),
            ("KeyValue", SchemaType::KeyValue, &[0; 8][..])
        );
        let expected = properties(&[
            ("key.schema.name", "String"),
            ("key.schema.properties", "{}"),
            ("key.schema.type", "STRING"),
            ("kv.encoding.type", "INLINE"),
            ("value.schema.name", "INT32"),
            ("value.schema.properties", "{}"),
            ("value.schema.type", "INT32"),
        ]);
        assert_eq!(info.properties(), &expected);
        let declared = info.to_proto().unwrap();
        assert_eq!((declared.name.as_str(), declared.r#type), ("KeyValue", Type::KeyValue));
        let declared: Vec<(String, String)> = declared.properties.into_iter().map(|p| (p.key, p.value)).collect();
        assert_eq!(declared, expected.into_iter().collect::<Vec<_>>());
    }

    /// A key/value schema within another records its definition and properties, as Java's does.
    #[test]
    fn a_key_value_schema_nests_as_javas_does() {
        let inner = Schema::key_value(Schema::string(), Schema::int32());
        let nested = Schema::key_value(inner, Schema::bool());
        let info = nested.info();
        assert_eq!(info.definition(), [0, 0, 0, 8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0]);
        let inner_properties = r#"{"key.schema.name":"String","key.schema.properties":"{}","key.schema.type":"STRING","kv.encoding.type":"INLINE","value.schema.name":"INT32","value.schema.properties":"{}","value.schema.type":"INT32"}"#;
        assert_eq!(info.properties()["key.schema.properties"], inner_properties);
        assert_eq!(info.properties()["key.schema.type"], "KEY_VALUE");
        assert_eq!(info.properties()["value.schema.name"], "Boolean");
        let value = KeyValue { key: KeyValue { key: "k".to_string(), value: -1 }, value: true };
        assert_eq!(nested.decode(&nested.encode(&value).unwrap()).unwrap(), value);
    }

    /// The names of Java's `SchemaType` constants, which key/value schemas record.
    #[test]
    fn key_value_schemas_name_the_types_as_java_does() {
        fn key_type<K: 'static>(key: Schema<K>) -> String {
            Schema::key_value(key, Schema::bytes()).info().properties()["key.schema.type"].clone()
        }
        let names = [
            key_type(Schema::bytes()),
            key_type(Schema::string()),
            key_type(Schema::bool()),
            key_type(Schema::int8()),
            key_type(Schema::int16()),
            key_type(Schema::int32()),
            key_type(Schema::int64()),
            key_type(Schema::float32()),
            key_type(Schema::float64()),
        ];
        assert_eq!(names, ["BYTES", "STRING", "BOOLEAN", "INT8", "INT16", "INT32", "INT64", "FLOAT", "DOUBLE"]);
    }

    /// Java writes a null key or value with length -1; a typed schema can't decode it. A payload that ends early
    /// isn't one.
    #[test]
    fn key_value_payloads_that_dont_decode() {
        let schema = Schema::key_value(Schema::string(), Schema::int32());
        let refused = |payload: &[u8]| schema.decode(payload).unwrap_err().to_string();
        assert!(refused(&[0xff, 0xff, 0xff, 0xff, 0, 0, 0, 4, 0, 0, 0, 1]).contains("key is null"));
        assert!(refused(&[0, 0, 0, 1, b'k', 0xff, 0xff, 0xff, 0xff]).contains("value is null"));
        assert!(refused(&[0, 0, 0]).contains("ends in its key"));
        assert!(refused(&[0, 0, 0, 2, b'k']).contains("ends in its key"));
        assert!(refused(&[0, 0, 0, 1, b'k', 0, 0, 0, 4, 0]).contains("ends in its value"));
        // A part that doesn't decode with its own schema.
        assert!(refused(&[0, 0, 0, 1, 0xff, 0, 0, 0, 4, 0, 0, 0, 1]).contains("not UTF-8"));
        assert_eq!(split_inline(&[0, 0, 0, 0, 0xff, 0xff, 0xff, 0xff, 9]).unwrap(), (Some(&[][..]), None));
    }
}
