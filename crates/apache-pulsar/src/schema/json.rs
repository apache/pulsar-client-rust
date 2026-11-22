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

//! JSON: Java's `Schema.json`, records encoded as JSON objects and described in Avro.

use std::sync::Arc;

use apache_avro::AvroSchema;
use bytes::Bytes;
use serde::Serialize;
use serde::de::DeserializeOwned;

use super::record::info;
use super::{Generic, Schema, SchemaInfo, SchemaType};
use crate::error::{Error, ErrorKind, Result};

impl<T: Serialize + DeserializeOwned + AvroSchema + 'static> Schema<T> {
    /// Records as JSON objects, as Java's `Schema.json(Class)` encodes them, through `serde`. The broker is told the
    /// Avro schema `T` derives, as Java describes its JSON records in Avro, with `#[derive(AvroSchema)]` of
    /// apache-avro's `derive` feature: an `Option` field is nullable, as Java's reflection makes its nullable fields.
    ///
    /// Each message reads as `serde` reads JSON, whichever version wrote it, as in Java: a field `T` doesn't have is
    /// left, and an `Option` one the message doesn't have is `None`. Java leaves its null fields out, where `serde`
    /// writes them as `null` unless told otherwise: both read either.
    pub fn json() -> Self {
        Self {
            info: info(SchemaType::Json, &T::get_schema()),
            encode: Arc::new(|value: &T| encode(value)),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| decode(data)),
            generic: Generic::No,
        }
    }
}

fn encode<T: Serialize>(value: &T) -> Result<Bytes> {
    serde_json::to_vec(value).map(Bytes::from).map_err(unencodable)
}

fn decode<T: DeserializeOwned>(data: &[u8]) -> Result<T> {
    serde_json::from_slice(data).map_err(undecodable)
}

fn unencodable(error: serde_json::Error) -> Error {
    Error::new(ErrorKind::Other, format!("the value doesn't encode in JSON: {error}"))
}

fn undecodable(error: serde_json::Error) -> Error {
    Error::new(ErrorKind::Other, format!("the payload doesn't decode from JSON: {error}"))
}

#[cfg(test)]
mod tests {
    use serde::Deserialize;

    use super::*;
    use crate::schema::record::{Unencodable, properties};

    #[derive(Debug, Clone, PartialEq, Serialize, Deserialize, apache_avro::AvroSchema)]
    struct Pojo {
        name: Option<String>,
        age: i32,
    }

    #[test]
    fn records_encode_as_json_described_in_avro() {
        let schema = Schema::<Pojo>::json();
        let info = schema.info();
        assert_eq!((info.name(), info.schema_type(), info.properties()), ("", SchemaType::Json, &properties()));
        let described = serde_json::to_vec(&<Pojo as apache_avro::AvroSchema>::get_schema()).unwrap();
        assert_eq!(info.definition(), described, "in Avro");
        let alice = Pojo { name: Some("alice".to_string()), age: 30 };
        let payload = schema.encode(&alice).unwrap();
        assert_eq!(&payload[..], br#"{"name":"alice","age":30}"#);
        assert_eq!(schema.decode(&payload).unwrap(), alice);
        // As Java writes it: without its null fields, the fields in its order, and those this record doesn't have.
        let java = br#"{"age":7,"city":"Rome"}"#;
        assert_eq!(schema.decode_with(java, Some(info)).unwrap(), Pojo { name: None, age: 7 });
    }

    #[test]
    fn records_that_dont_encode_or_decode() {
        let error = Schema::<Unencodable>::json().encode(&Unencodable).unwrap_err().to_string();
        assert!(error.contains("the value doesn't encode in JSON"), "{error}");
        let error = Schema::<Pojo>::json().decode(br#"{"name":"bob"}"#).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode from JSON"), "{error}");
    }
}
