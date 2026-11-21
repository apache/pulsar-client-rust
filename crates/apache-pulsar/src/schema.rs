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

use std::sync::Arc;

use apache_pulsar_protocol::proto;
use apache_pulsar_protocol::proto::schema::Type;
use bytes::Bytes;

use crate::error::{Error, ErrorKind, Result};

type Encode<T> = Arc<dyn Fn(&T) -> Result<Bytes> + Send + Sync>;
type Decode<T> = Arc<dyn Fn(&[u8]) -> Result<T> + Send + Sync>;

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
pub struct Schema<T> {
    info: SchemaInfo,
    encode: Encode<T>,
    decode: Decode<T>,
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
}

/// A schema as the broker records it: Java V5's `SchemaInfo`.
#[derive(Debug, Clone, PartialEq, Eq)]
pub struct SchemaInfo {
    name: String,
    schema_type: SchemaType,
    definition: Vec<u8>,
}

impl SchemaInfo {
    fn new(name: &str, schema_type: SchemaType) -> Self {
        Self { name: name.to_string(), schema_type, definition: Vec::new() }
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

    /// What a producer or a consumer declares on registering: nothing for raw bytes, as Java declares nothing.
    pub(crate) fn to_proto(&self) -> Option<proto::Schema> {
        let kind = match self.schema_type {
            SchemaType::Bytes => return None,
            SchemaType::String => Type::String,
            SchemaType::Boolean => Type::Bool,
            SchemaType::Int8 => Type::Int8,
            SchemaType::Int16 => Type::Int16,
            SchemaType::Int32 => Type::Int32,
            SchemaType::Int64 => Type::Int64,
            SchemaType::Float => Type::Float,
            SchemaType::Double => Type::Double,
        };
        Some(proto::Schema {
            name: self.name.clone(),
            schema_data: self.definition.clone(),
            r#type: kind,
            ..Default::default()
        })
    }
}

impl<T> Clone for Schema<T> {
    fn clone(&self) -> Self {
        Self { info: self.info.clone(), encode: self.encode.clone(), decode: self.decode.clone() }
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
            decode: Arc::new(|data: &[u8]| Ok(data.to_vec())),
        }
    }
}

impl Schema<String> {
    /// UTF-8 text: Java's `Schema.string()`.
    pub fn string() -> Self {
        Self {
            info: SchemaInfo::new("String", SchemaType::String),
            encode: Arc::new(|value: &String| Ok(Bytes::copy_from_slice(value.as_bytes()))),
            decode: Arc::new(|data: &[u8]| {
                String::from_utf8(data.to_vec())
                    .map_err(|e| Error::new(ErrorKind::Other, format!("the payload is not UTF-8: {e}")))
            }),
        }
    }
}

impl Schema<bool> {
    /// A boolean, as one byte: Java's `Schema.bool()`. Any byte but 0 reads as `true`, as in Java.
    pub fn bool() -> Self {
        Self::fixed("Boolean", SchemaType::Boolean, |value| [u8::from(*value)], |[byte]| byte != 0)
    }
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
            decode: Arc::new(move |data: &[u8]| match <[u8; N]>::try_from(data) {
                Ok(bytes) => Ok(from(bytes)),
                Err(_) => Err(wrong_size(name, N, data.len())),
            }),
        }
    }
}

/// A payload whose size doesn't fit its schema.
fn wrong_size(name: &str, expected: usize, size: usize) -> Error {
    Error::new(ErrorKind::Other, format!("a {name} value takes {expected} bytes, but the payload has {size}"))
}

impl<T> Schema<T> {
    /// The schema the broker is told about.
    pub fn info(&self) -> &SchemaInfo {
        &self.info
    }

    pub(crate) fn encode(&self, value: &T) -> Result<Bytes> {
        (self.encode)(value)
    }

    pub(crate) fn decode(&self, data: &[u8]) -> Result<T> {
        (self.decode)(data)
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
}
