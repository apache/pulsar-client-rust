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

//! What the JSON and Avro schemas share: their records are described in Avro, as Java's are.

use std::collections::HashMap;
use std::sync::{Arc, Mutex, PoisonError};

use serde_json::Value as Json;

use super::{SchemaInfo, SchemaType};
use crate::error::{Error, ErrorKind, Result};

/// The schema the broker is told about for records that `definition` describes, as Java's: no name, the definition
/// as JSON, and the properties Java records on how it derived it, here from a Rust type, whose `Option` fields are
/// its nullable ones.
pub(super) fn info(schema_type: SchemaType, definition: &apache_avro::Schema) -> SchemaInfo {
    let properties = [("__alwaysAllowNull", "false"), ("__jsr310ConversionEnabled", "false")];
    SchemaInfo {
        name: String::new(),
        schema_type,
        definition: serde_json::to_vec(definition).expect("an Avro schema is JSON"),
        properties: properties.into_iter().map(|(key, value)| (key.to_string(), value.to_string())).collect(),
    }
}

/// An error of apache-avro's, as what failed.
pub(super) fn avro_error(what: &str, error: apache_avro::Error) -> Error {
    Error::new(ErrorKind::Other, format!("{what}: {error}"))
}

/// The Avro schemas of the definitions messages were written with, each parsed once.
#[derive(Default)]
pub(super) struct Definitions(Mutex<HashMap<Vec<u8>, Arc<apache_avro::Schema>>>);

impl Definitions {
    /// The schema `definition` describes. A definition that doesn't parse fails each time.
    pub(super) fn parse(&self, definition: &[u8]) -> Result<Arc<apache_avro::Schema>> {
        let mut parsed = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        if let Some(schema) = parsed.get(definition) {
            return Ok(schema.clone());
        }
        let schema = Arc::new(parse(definition)?);
        parsed.insert(definition.to_vec(), schema.clone());
        Ok(schema)
    }
}

/// An Avro definition, parsed as Java's parser takes it: Java doesn't validate names, and older Java clients name a
/// nested class's record in a namespace that ends in `$`, which apache-avro refuses. Names and namespaces become valid
/// Avro ones, each other character an underscore: they serve to resolve the definition's references, which change
/// alike, and records are matched without their namespace.
fn parse(definition: &[u8]) -> Result<apache_avro::Schema> {
    let mut json: Json = serde_json::from_slice(definition)
        .map_err(|e| Error::new(ErrorKind::Other, format!("the schema's definition isn't JSON: {e}")))?;
    names(&mut json);
    apache_avro::Schema::parse(json).map_err(|e| avro_error("the schema's definition isn't Avro", e))
}

/// Makes valid the names of a schema: of its named types, their aliases and namespaces, and the names that refer to
/// them. Fields keep theirs: they are the records' keys.
fn names(schema: &mut Json) {
    match schema {
        Json::String(name) => valid(name),
        Json::Array(branches) => branches.iter_mut().for_each(names),
        Json::Object(object) => {
            for (key, value) in object.iter_mut() {
                match (key.as_str(), value) {
                    ("name" | "namespace" | "aliases" | "type" | "items" | "values", value) => names(value),
                    ("fields", Json::Array(fields)) => {
                        fields.iter_mut().filter_map(|f| f.get_mut("type")).for_each(names)
                    }
                    _ => {}
                }
            }
        }
        _ => {}
    }
}

/// A name or a namespace, each character that Avro doesn't take in one an underscore.
fn valid(name: &mut String) {
    let takes = |c: char| c.is_ascii_alphanumeric() || c == '_' || c == '.';
    if !name.chars().all(takes) {
        *name = name.chars().map(|c| if takes(c) { c } else { '_' }).collect();
    }
}

/// A value that refuses to serialize, whose schema is an int.
#[cfg(test)]
#[derive(Debug, PartialEq, serde::Deserialize)]
pub(super) struct Unencodable;

#[cfg(test)]
impl serde::Serialize for Unencodable {
    fn serialize<S: serde::Serializer>(&self, _: S) -> std::result::Result<S::Ok, S::Error> {
        Err(serde::ser::Error::custom("refused"))
    }
}

#[cfg(test)]
impl apache_avro::AvroSchemaComponent for Unencodable {
    fn get_schema_in_ctxt(
        _: &mut std::collections::HashSet<apache_avro::schema::Name>,
        _: apache_avro::schema::NamespaceRef,
    ) -> apache_avro::Schema {
        apache_avro::Schema::Int
    }
}

#[cfg(test)]
pub(super) fn properties() -> std::collections::BTreeMap<String, String> {
    std::collections::BTreeMap::from([
        ("__alwaysAllowNull".to_string(), "false".to_string()),
        ("__jsr310ConversionEnabled".to_string(), "false".to_string()),
    ])
}

#[cfg(test)]
mod tests {
    use super::*;

    /// A definition as Java's reflection writes one for a static nested class: its namespace ends in `$`, in the
    /// record's, in a nested record's and in a reference to it, with fields, an array, a map, a union and an alias.
    const JAVA: &str = r#"{"type":"record","name":"Order","namespace":"com.acme.Shop$","aliases":["old.Shop$Order"],
        "doc":"an order, in com.acme.Shop$",
        "fields":[
            {"name":"line","type":{"type":"record","name":"Line","namespace":"com.acme.Shop$","fields":[
                {"name":"sku","type":"string"}]}},
            {"name":"lines","type":{"type":"array","items":"com.acme.Shop$.Line"}},
            {"name":"by","type":{"type":"map","values":"long"}},
            {"name":"note","type":["null","string"],"default":null}]}"#;

    #[test]
    fn javas_nested_classes_parse() {
        let definitions = Definitions::default();
        let schema = definitions.parse(JAVA.as_bytes()).unwrap();
        let json = serde_json::to_value(&*schema).unwrap();
        assert_eq!(json["namespace"], "com.acme.Shop_");
        assert_eq!(json["aliases"], serde_json::json!(["old.Shop_Order"]));
        assert_eq!(json["doc"], "an order, in com.acme.Shop$", "not a name");
        assert_eq!(json["fields"][0]["type"]["namespace"], "com.acme.Shop_");
        assert_eq!(json["fields"][1]["type"]["items"], "com.acme.Shop_.Line");
        assert!(Arc::ptr_eq(&schema, &definitions.parse(JAVA.as_bytes()).unwrap()), "parsed once");
    }

    #[test]
    fn definitions_that_dont_parse() {
        let definitions = Definitions::default();
        let error = definitions.parse(b"{").unwrap_err().to_string();
        assert!(error.contains("isn't JSON"), "{error}");
        let error = definitions.parse(br#"{"type":"record"}"#).unwrap_err().to_string();
        assert!(error.contains("isn't Avro"), "{error}");
        let error = definitions.parse(b"7").unwrap_err().to_string();
        assert!(error.contains("isn't Avro"), "{error}");
        // A field keeps its name, which its records' values are found by: one that Avro doesn't take, as Java can
        // write, fails to parse.
        let field = br#"{"type":"record","name":"R","fields":[{"name":"a$b","type":"int"}]}"#;
        let error = definitions.parse(field).unwrap_err().to_string();
        assert!(error.contains("a$b"), "{error}");
    }

    #[test]
    fn the_info_is_javas() {
        let info = info(SchemaType::Avro, &apache_avro::Schema::Int);
        assert_eq!((info.name(), info.schema_type(), info.definition()), ("", SchemaType::Avro, &b"\"int\""[..]));
        assert_eq!(info.properties(), &properties());
    }
}
