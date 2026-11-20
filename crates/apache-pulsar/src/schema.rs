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

//! Schemas: how values become payloads (docs/7-api.md §8).
//!
//! The schema is topic-level; the broker checks compatibility when a producer or consumer connects. This
//! milestone has bytes and string; the rest come later.

use std::sync::Arc;

use apache_pulsar_protocol::proto::schema::Type;
use bytes::Bytes;

use crate::error::{Error, ErrorKind, Result};

type Encode<T> = Arc<dyn Fn(&T) -> Result<Bytes> + Send + Sync>;
type Decode<T> = Arc<dyn Fn(&[u8]) -> Result<T> + Send + Sync>;

/// How values of type `T` are encoded, and the schema the broker is told about. Cheap to clone.
pub struct Schema<T> {
    /// What `CommandProducer` declares; `None` for raw bytes, which Java doesn't declare either.
    info: Option<SchemaInfo>,
    encode: Encode<T>,
    decode: Decode<T>,
}

/// A schema as the broker sees it.
#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct SchemaInfo {
    pub(crate) name: String,
    pub(crate) kind: Type,
    pub(crate) data: Vec<u8>,
}

impl<T> Clone for Schema<T> {
    fn clone(&self) -> Self {
        Self { info: self.info.clone(), encode: self.encode.clone(), decode: self.decode.clone() }
    }
}

impl<T> std::fmt::Debug for Schema<T> {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        SchemaInfo::fmt_schema(self.info.as_ref(), f)
    }
}

impl SchemaInfo {
    /// A schema's `Debug`, the same for every value type.
    fn fmt_schema(info: Option<&SchemaInfo>, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        match info {
            Some(info) => f.debug_struct("Schema").field("name", &info.name).field("type", &info.kind).finish(),
            None => f.write_str("Schema(bytes)"),
        }
    }
}

impl Schema<Vec<u8>> {
    /// Raw bytes, as given.
    pub fn bytes() -> Self {
        Self {
            info: None,
            encode: Arc::new(|value: &Vec<u8>| Ok(Bytes::copy_from_slice(value))),
            decode: Arc::new(|data: &[u8]| Ok(data.to_vec())),
        }
    }
}

impl Schema<String> {
    /// UTF-8 text: Java's `Schema.string()`.
    pub fn string() -> Self {
        Self {
            info: Some(SchemaInfo { name: "String".into(), kind: Type::String, data: Vec::new() }),
            encode: Arc::new(|value: &String| Ok(Bytes::copy_from_slice(value.as_bytes()))),
            decode: Arc::new(|data: &[u8]| {
                String::from_utf8(data.to_vec())
                    .map_err(|e| Error::new(ErrorKind::Other, format!("the payload is not UTF-8: {e}")))
            }),
        }
    }
}

impl<T> Schema<T> {
    pub(crate) fn info(&self) -> Option<&SchemaInfo> {
        self.info.as_ref()
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

    #[test]
    fn built_in_schemas() {
        let bytes = Schema::bytes();
        assert_eq!(bytes.info(), None);
        assert_eq!(&bytes.encode(&vec![1, 2, 3]).unwrap()[..], [1, 2, 3]);
        assert_eq!(bytes.decode(&[1, 2, 3]).unwrap(), [1, 2, 3]);
        let text = Schema::string();
        assert_eq!(text.info().unwrap().kind, Type::String);
        assert_eq!(text.decode(&text.encode(&"héllo".to_string()).unwrap()).unwrap(), "héllo");
        assert_eq!(text.decode(&[0xff]).unwrap_err().kind(), ErrorKind::Other);
    }
}
