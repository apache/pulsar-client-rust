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

//! Protobuf: Java's two schemas of messages that their descriptors describe. `PROTOBUF_NATIVE` declares the
//! descriptors, and Java V5's `PROTOBUF` Avro's description of the message. The descriptors are read with buffa's: a
//! buffa type brings its own; another, such as a prost type, those of the `FileDescriptorSet` it was generated from.

use std::collections::{BTreeMap, HashMap, HashSet};
use std::sync::{Arc, Mutex, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use buffa::{Enumeration as _, Message as _};
use buffa_descriptor::generated::descriptor::field_descriptor_proto::{Label, Type};
use buffa_descriptor::generated::descriptor::{
    DescriptorProto, EnumDescriptorProto, FieldDescriptorProto, FileDescriptorProto, FileDescriptorSet,
};
use buffa_descriptor::{DescriptorPool, DynamicMessage, MessageDescriptor, MessageIndex, Reflectable};

use super::{Generic, Schema, SchemaInfo, SchemaType};
use crate::error::{Error, ErrorKind, Result};

impl<T: buffa::Message + Reflectable + Default + 'static> Schema<T> {
    /// Messages of a buffa type in Protobuf's encoding, declared as Java's `Schema.PROTOBUF_NATIVE(Class)` declares
    /// them: by the descriptors of the message's file and of the files it imports, which the broker checks a new
    /// version against. The type needs buffa's reflection (buffa-build's `generate_reflection`), which brings its
    /// descriptors.
    pub fn protobuf_native() -> Self {
        Self::buffa_messages(Self::message_type().native_info())
    }

    /// Messages of a buffa type in Protobuf's encoding, declared as Java V5's `Schema.protobuf(Class)` declares them:
    /// in Avro, as Avro's `ProtobufData` describes the message. The type needs buffa's reflection, as for
    /// [`Schema::protobuf_native`]. Fails with [`ErrorKind::InvalidConfiguration`] where Java's description fails: for
    /// a group, or a default that Avro can't hold, a `bytes` field's or a float's infinity or NaN.
    pub fn protobuf() -> Result<Self> {
        Ok(Self::buffa_messages(Self::message_type().avro_info()?))
    }

    /// The type's message type, by the descriptors it brings.
    fn message_type() -> MessageType {
        let message = T::default();
        let reflected = message.reflect();
        let full_name = reflected.message_descriptor().full_name();
        let message_type = MessageType::new(reflected.pool().clone(), full_name, ErrorKind::Other);
        message_type.expect("a buffa type's pool has its descriptor")
    }

    /// The type's messages, declared with `info`.
    fn buffa_messages(info: SchemaInfo) -> Self {
        Self {
            info,
            encode: Arc::new(|value: &T| Ok(value.encode_to_bytes())),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| T::decode_from_slice(data).map_err(undecodable)),
            generic: Generic::No,
        }
    }
}

#[cfg(feature = "prost")]
impl<T: prost::Message + prost::Name + Default + 'static> Schema<T> {
    /// Messages of a prost type in Protobuf's encoding, declared as [`Schema::protobuf_native`] declares a buffa
    /// type's. A prost type carries no descriptors: `descriptors` is the `FileDescriptorSet` it was generated from,
    /// with its file and those it imports, as prost-build writes it (`file_descriptor_set_path`). The type's full name
    /// is its `prost::Name` (prost-build's `enable_type_names`). Fails with [`ErrorKind::InvalidConfiguration`] if the
    /// descriptors don't describe it.
    pub fn prost_native(descriptors: &[u8]) -> Result<Self> {
        Ok(Self::prost_messages(Self::prost_type(descriptors)?.native_info()))
    }

    /// Messages of a prost type in Protobuf's encoding, declared as [`Schema::protobuf`] declares a buffa type's, by
    /// the descriptors [`Schema::prost_native`] takes. Fails with [`ErrorKind::InvalidConfiguration`] if they don't
    /// describe the type, or where Java's description of it fails.
    pub fn prost(descriptors: &[u8]) -> Result<Self> {
        Ok(Self::prost_messages(Self::prost_type(descriptors)?.avro_info()?))
    }

    /// The type's message type, by `descriptors`.
    fn prost_type(descriptors: &[u8]) -> Result<MessageType> {
        MessageType::decode(descriptors, &T::full_name(), ErrorKind::InvalidConfiguration)
    }

    /// The type's messages, declared with `info`.
    fn prost_messages(info: SchemaInfo) -> Self {
        Self {
            info,
            encode: Arc::new(|value: &T| Ok(bytes::Bytes::from(value.encode_to_vec()))),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| T::decode(data).map_err(prost_undecodable)),
            generic: Generic::No,
        }
    }
}

#[cfg(feature = "prost")]
fn prost_undecodable(error: prost::DecodeError) -> Error {
    Error::new(ErrorKind::Other, format!("the payload doesn't decode in Protobuf: {error}"))
}

fn undecodable(error: buffa::DecodeError) -> Error {
    Error::new(ErrorKind::Other, format!("the payload doesn't decode in Protobuf: {error}"))
}

/// A Protobuf message type, as its descriptors describe it: the pool of its file and of those the file imports, its
/// index there, and the files that describe it.
#[derive(Clone)]
pub(super) struct MessageType {
    pool: Arc<DescriptorPool>,
    index: MessageIndex,
    /// Its file and those the file imports, each once, every file after those it imports, without the source info a
    /// set can hold, as Java's descriptors have none.
    files: Vec<FileDescriptorProto>,
}

impl MessageType {
    /// The type `full_name` of a pool, which fails with `kind` if the pool doesn't have it, or lacks a file that its
    /// file needs: a pool links the types its files name, not the files they import.
    fn new(pool: Arc<DescriptorPool>, full_name: &str, kind: ErrorKind) -> Result<Self> {
        let missing = || Error::new(kind, format!("the descriptors have no message {full_name}"));
        let index = pool.message_index(full_name).ok_or_else(missing)?;
        let mut files = Vec::new();
        add(&pool, pool.file_containing_symbol(full_name).expect("a pool has its messages' files"), &mut files, kind)?;
        Ok(Self { pool, index, files })
    }

    /// The type `full_name` of a `FileDescriptorSet`, which fails with `kind` if the set doesn't describe it.
    fn decode(descriptors: &[u8], full_name: &str, kind: ErrorKind) -> Result<Self> {
        let pool = DescriptorPool::decode(descriptors)
            .map_err(|e| Error::new(kind, format!("the descriptors aren't a FileDescriptorSet: {e}")))?;
        Self::new(Arc::new(pool), full_name, kind)
    }

    fn descriptor(&self) -> &MessageDescriptor {
        self.pool.message(self.index)
    }

    /// What the broker is told for `PROTOBUF_NATIVE`, as Java's `ProtobufNativeSchema` tells it: the descriptor set
    /// of the message's file and its imports in base64, with the message's and the file's names.
    fn native_info(&self) -> SchemaInfo {
        let file = self.file();
        let set = FileDescriptorSet { file: self.files.clone(), ..Default::default() };
        let definition = format!(
            r#"{{"fileDescriptorSet":{},"rootMessageTypeName":{},"rootFileDescriptorName":{}}}"#,
            json(&STANDARD.encode(set.encode_to_vec())),
            json(self.descriptor().full_name()),
            json(file.name.as_deref().unwrap_or_default()),
        );
        self.info(SchemaType::ProtobufNative, definition)
    }

    /// What the broker is told for Java V5's `PROTOBUF`, as Java's `ProtobufSchema` tells it: Avro's description of
    /// the message. Fails where Java's description fails.
    fn avro_info(&self) -> Result<SchemaInfo> {
        let mut avro = Avro { pool: &self.pool, written: HashSet::new(), json: String::new() };
        avro.record(self.descriptor().full_name(), None)?;
        Ok(self.info(SchemaType::Protobuf, avro.json))
    }

    /// A schema of the message, as Java declares it: no name, `definition`, and Java's properties, the message's
    /// parsing info among them.
    fn info(&self, schema_type: SchemaType, definition: String) -> SchemaInfo {
        let properties = [
            ("__PARSING_INFO__", self.parsing_info()),
            ("__alwaysAllowNull", "true".to_string()),
            ("__jsr310ConversionEnabled", "false".to_string()),
        ];
        SchemaInfo {
            name: String::new(),
            schema_type,
            definition: definition.into_bytes(),
            properties: properties.into_iter().map(|(key, value)| (key.to_string(), value)).collect::<BTreeMap<_, _>>(),
        }
    }

    /// The file that declares the message.
    fn file(&self) -> &FileDescriptorProto {
        self.pool.file_containing_symbol(self.descriptor().full_name()).expect("a pool has its messages' files")
    }

    /// Java's `__PARSING_INFO__`: the message's fields as its file declares them, each its number, name, type and
    /// label, in JSON.
    fn parsing_info(&self) -> String {
        let fields: Vec<String> = (self.proto().field.iter())
            .map(|field| {
                format!(
                    r#"{{"number":{},"name":{},"type":{},"label":{},"definition":null}}"#,
                    field.number.unwrap_or_default(),
                    json(field.name.as_deref().unwrap_or_default()),
                    json(field.r#type.map_or("", |kind| kind.proto_name()).trim_start_matches("TYPE_")),
                    json(field.label.map_or("", |label| label.proto_name())),
                )
            })
            .collect();
        format!("[{}]", fields.join(","))
    }

    /// The message as its file declares it.
    fn proto(&self) -> &DescriptorProto {
        let full_name = self.descriptor().full_name();
        let (file, outer) = declared(&self.pool, full_name);
        named(scope(file, &outer).0, simple(full_name))
    }

    /// A payload's message, read with these descriptors.
    fn read(&self, data: &[u8]) -> Result<DynamicMessage> {
        DynamicMessage::decode(self.pool.clone(), self.index, data).map_err(undecodable)
    }
}

/// Adds a file of a pool to `files`, after those it imports, unless it is there: without its source info. Fails with
/// `kind` if the pool lacks a file it imports.
fn add(
    pool: &DescriptorPool,
    file: &FileDescriptorProto,
    files: &mut Vec<FileDescriptorProto>,
    kind: ErrorKind,
) -> Result<()> {
    if files.iter().all(|added| added.name != file.name) {
        for import in &file.dependency {
            let name = file.name.as_deref().unwrap_or_default();
            let missing = || Error::new(kind, format!("the descriptors lack {import}, which {name} imports"));
            add(pool, pool.file_by_name(import).ok_or_else(missing)?, files, kind)?;
        }
        let mut file = file.clone();
        file.source_code_info = Default::default();
        files.push(file);
    }
    Ok(())
}

/// Where the message or enum `full_name` of a pool is declared: its file, and the messages it is nested in, outermost
/// first.
fn declared<'a>(pool: &'a DescriptorPool, full_name: &str) -> (&'a FileDescriptorProto, Vec<&'a DescriptorProto>) {
    let file = pool.file_containing_symbol(full_name).expect("a pool has its types' files");
    let package = file.package.as_deref().unwrap_or_default();
    let local = full_name.strip_prefix(package).and_then(|name| name.strip_prefix('.')).unwrap_or(full_name);
    let mut names: Vec<&str> = local.split('.').collect();
    names.pop();
    let mut outer: Vec<&DescriptorProto> = Vec::new();
    for name in names {
        outer.push(named(scope(file, &outer).0, name));
    }
    (file, outer)
}

/// The messages and enums declared where a type nested in `outer`, of `file`, is: in the message it's nested in, or in
/// the file.
fn scope<'a>(
    file: &'a FileDescriptorProto,
    outer: &[&'a DescriptorProto],
) -> (&'a [DescriptorProto], &'a [EnumDescriptorProto]) {
    outer.last().map_or((&file.message_type, &file.enum_type), |outer| (&outer.nested_type, &outer.enum_type))
}

/// The message of `messages` named `name`.
fn named<'a>(messages: &'a [DescriptorProto], name: &str) -> &'a DescriptorProto {
    messages.iter().find(|message| message.name.as_deref() == Some(name)).expect("a file declares its messages")
}

/// The last part of a full name.
fn simple(full_name: &str) -> &str {
    full_name.rsplit('.').next().unwrap_or(full_name)
}

/// A string, as JSON writes it.
fn json(text: &str) -> String {
    serde_json::to_string(text).expect("a string is JSON")
}

/// Avro's description of a Protobuf message, as Java's `ProtobufSchema` declares it: what avro-protobuf's
/// `ProtobufData` makes of the descriptors, as Avro's `Schema.toString()` writes it. A record or an enum is written in
/// full where it first appears, and by name after.
struct Avro<'a> {
    pool: &'a DescriptorPool,
    /// The full names of the records and enums written.
    written: HashSet<String>,
    json: String,
}

impl Avro<'_> {
    /// The message `full_name`, a record, written where the namespace is `enclosing`.
    fn record(&mut self, full_name: &str, enclosing: Option<&str>) -> Result<()> {
        let pool = self.pool;
        let (file, outer) = declared(pool, full_name);
        let name = Name::new(full_name, file, &outer);
        if self.written_before(&name, enclosing) {
            return Ok(());
        }
        self.json += &format!(r#"{{"type":"record",{},"fields":["#, name.declared(enclosing));
        for (index, field) in named(scope(file, &outer).0, name.simple).field.iter().enumerate() {
            self.json += if index == 0 { "" } else { "," };
            self.field(full_name, field, name.space.as_deref())?;
        }
        self.json += "]}";
        Ok(())
    }

    /// A field of the message `message`, its type and its default, as avro-protobuf describes them: a repeated field as
    /// an array, which defaults to an empty one; an enum's default its first value; a message as a record, in a union
    /// with null unless it's required; and a required field without a default. The field's label and type are read as
    /// protoc writes them for proto2 and proto3: an edition's features don't change them, as they do Java's.
    fn field(&mut self, message: &str, field: &FieldDescriptorProto, enclosing: Option<&str>) -> Result<()> {
        let name = field.name.as_deref().unwrap_or_default();
        let refused = |what: &str| {
            let why = format!("{message}.{name} {what}, which Java's description of Protobuf in Avro refuses too");
            Error::new(ErrorKind::InvalidConfiguration, why)
        };
        let kind = field.r#type.unwrap_or(Type::TYPE_MESSAGE);
        if kind == Type::TYPE_GROUP {
            return Err(refused("is a group"));
        }
        let type_name = field.type_name.as_deref().unwrap_or_default().trim_start_matches('.');
        let repeated = field.label == Some(Label::LABEL_REPEATED);
        let required = field.label == Some(Label::LABEL_REQUIRED);
        self.json += &format!(r#"{{"name":{},"type":"#, json(name));
        self.json += if repeated { r#"{"type":"array","items":"# } else { "" };
        let implicit = match scalar(kind) {
            Some((avro, implicit)) => {
                self.json += avro;
                implicit.to_string()
            }
            None if kind == Type::TYPE_ENUM => {
                json(&self.enumeration(type_name, field.default_value.as_deref(), enclosing))
            }
            None => {
                let optional = !repeated && !required;
                self.json += if optional { r#"["null","# } else { "" };
                self.record(type_name, enclosing)?;
                self.json += if optional { "]" } else { "" };
                "null".to_string()
            }
        };
        self.json += if repeated { "}" } else { "" };
        let default = if required {
            None
        } else if repeated {
            Some("[]".to_string())
        } else if let Some(text) = field.default_value.as_deref() {
            Some(explicit(kind, text).ok_or_else(|| refused("has a default that Avro can't hold"))?)
        } else {
            Some(implicit)
        };
        self.json += &default.map_or(String::new(), |default| format!(r#","default":{default}"#));
        self.json += "}";
        Ok(())
    }

    /// The enum `full_name`, written where the namespace is `enclosing`, which defaults to `default`, else to its first
    /// value. Returns its first value.
    fn enumeration(&mut self, full_name: &str, default: Option<&str>, enclosing: Option<&str>) -> String {
        let (file, outer) = declared(self.pool, full_name);
        let name = Name::new(full_name, file, &outer);
        let declared =
            scope(file, &outer).1.iter().find(|enumeration| enumeration.name.as_deref() == Some(name.simple));
        let symbols: Vec<&str> = (declared.expect("a file declares its enums").value.iter())
            .map(|value| value.name.as_deref().unwrap_or_default())
            .collect();
        let first = symbols.first().copied().unwrap_or_default();
        if !self.written_before(&name, enclosing) {
            let symbols: Vec<String> = symbols.iter().map(|symbol| json(symbol)).collect();
            self.json += &format!(
                r#"{{"type":"enum",{},"symbols":[{}],"default":{}}}"#,
                name.declared(enclosing),
                symbols.join(","),
                json(default.unwrap_or(first)),
            );
        }
        first.to_string()
    }

    /// Whether `name` was written before, as Avro's `writeNameRef` asks; if so, writes it again by name, as
    /// [`Name::qualified`] gives it.
    fn written_before(&mut self, name: &Name, enclosing: Option<&str>) -> bool {
        let written = !self.written.insert(name.full());
        if written {
            self.json += &json(&name.qualified(enclosing));
        }
        written
    }
}

/// The Avro type of a scalar Protobuf type, as avro-protobuf gives it, and its default: none for an enum, a message or
/// a group.
fn scalar(kind: Type) -> Option<(&'static str, &'static str)> {
    use Type::*;
    Some(match kind {
        TYPE_BOOL => (r#""boolean""#, "false"),
        TYPE_FLOAT => (r#""float""#, "0.0"),
        TYPE_DOUBLE => (r#""double""#, "0.0"),
        TYPE_STRING => (r#"{"type":"string","avro.java.string":"String"}"#, r#""""#),
        TYPE_BYTES => (r#""bytes""#, r#""""#),
        TYPE_INT32 | TYPE_UINT32 | TYPE_SINT32 | TYPE_FIXED32 | TYPE_SFIXED32 => (r#""int""#, "0"),
        TYPE_INT64 | TYPE_UINT64 | TYPE_SINT64 | TYPE_FIXED64 | TYPE_SFIXED64 => (r#""long""#, "0"),
        TYPE_ENUM | TYPE_MESSAGE | TYPE_GROUP => return None,
    })
}

/// A default the descriptors set, in JSON: an unsigned number as Java's signed type holds it, and a float with its
/// shortest digits, as Rust writes them, which Avro reads as the number Java writes. `None` where Java's description
/// fails: for a `bytes` field's, a float's infinity or NaN, and a number that isn't one, which Java's descriptors
/// refuse.
fn explicit(kind: Type, text: &str) -> Option<String> {
    use Type::*;
    match kind {
        TYPE_BOOL => Some(text.eq_ignore_ascii_case("true").to_string()),
        TYPE_FLOAT => text.parse::<f32>().ok().filter(|value| value.is_finite()).map(|value| format!("{value:?}")),
        TYPE_DOUBLE => text.parse::<f64>().ok().filter(|value| value.is_finite()).map(|value| format!("{value:?}")),
        TYPE_UINT32 | TYPE_FIXED32 => text.parse::<u32>().ok().map(|value| value.cast_signed().to_string()),
        TYPE_UINT64 | TYPE_FIXED64 => text.parse::<u64>().ok().map(|value| value.cast_signed().to_string()),
        TYPE_STRING | TYPE_ENUM => Some(json(text)),
        TYPE_BYTES | TYPE_MESSAGE | TYPE_GROUP => None,
        TYPE_INT32 | TYPE_SINT32 | TYPE_SFIXED32 | TYPE_INT64 | TYPE_SINT64 | TYPE_SFIXED64 => {
            text.parse::<i64>().ok().map(|value| value.to_string())
        }
    }
}

/// Avro's type names, which a record or an enum of the same name can't go by alone.
const AVRO_TYPES: [&str; 14] = [
    "record", "enum", "array", "map", "union", "fixed", "string", "bytes", "int", "long", "float", "double", "boolean",
    "null",
];

/// The Avro name of a message or an enum: avro-protobuf's, which names the type's Java class.
struct Name<'a> {
    simple: &'a str,
    /// Its namespace: none for an empty one, as in Avro.
    space: Option<String>,
}

impl<'a> Name<'a> {
    /// The name of the type `full_name`, of `file`, nested in `outer`: in the file's Java package, else its package,
    /// then in the file's outer class unless the file has a class for each message, then in the messages the type is
    /// nested in, with `$` between classes.
    fn new(full_name: &'a str, file: &FileDescriptorProto, outer: &[&DescriptorProto]) -> Self {
        let options = &file.options;
        let package = options.java_package.as_deref().or(file.package.as_deref()).unwrap_or_default();
        let class = match (options.java_multiple_files, &options.java_outer_classname) {
            (Some(true), _) => String::new(),
            (_, Some(class)) => class.clone(),
            _ => outer_class(file.name.as_deref().unwrap_or_default()),
        };
        let inner: Vec<&str> = outer.iter().map(|message| message.name.as_deref().unwrap_or_default()).collect();
        let inner = inner.join("$");
        let dot = if class.is_empty() && inner.is_empty() { "" } else { "." };
        let dollar = if class.is_empty() || inner.is_empty() { "" } else { "$" };
        let space = format!("{package}{dot}{class}{dollar}{inner}");
        Self { simple: simple(full_name), space: (!space.is_empty()).then_some(space) }
    }

    fn full(&self) -> String {
        self.space.as_ref().map_or_else(|| self.simple.to_string(), |space| format!("{space}.{}", self.simple))
    }

    /// The name where its type is declared, as Avro's `Name.writeName` writes it: the name, then its namespace unless
    /// it's `enclosing`, an empty one for none where there's one.
    fn declared(&self, enclosing: Option<&str>) -> String {
        let namespace = match (self.space.as_deref(), enclosing) {
            (Some(space), _) if Some(space) != enclosing => format!(r#","namespace":{}"#, json(space)),
            (None, Some(_)) => r#","namespace":"""#.to_string(),
            _ => String::new(),
        };
        format!(r#""name":{}{namespace}"#, json(self.simple))
    }

    /// The name, as Avro's `Name.getQualified` gives it where the namespace is `enclosing`: the simple name in its own
    /// namespace, unless it's one of Avro's types, else the full name.
    fn qualified(&self, enclosing: Option<&str>) -> String {
        let own = self.space.is_some() && self.space.as_deref() == enclosing;
        if own && !AVRO_TYPES.contains(&self.simple) { self.simple.to_string() } else { self.full() }
    }
}

/// avro-protobuf's outer class of a file that names none: the file's base name without its extension, in camel case,
/// each part between underscores with its first letter upper-cased and the others lower-cased.
fn outer_class(file: &str) -> String {
    let base = file.rsplit('/').next().unwrap_or(file);
    let stem = base.rsplit_once('.').map_or(base, |(stem, _)| stem);
    stem.split('_').map(capitalized).collect()
}

fn capitalized(part: &str) -> String {
    let mut chars = part.chars();
    chars
        .next()
        .map_or_else(String::new, |first| first.to_uppercase().chain(chars.flat_map(char::to_lowercase)).collect())
}

/// The message types of the descriptor sets messages were written with, each read once.
#[derive(Default)]
pub(super) struct Parsed(Mutex<HashMap<Vec<u8>, MessageType>>);

impl Parsed {
    /// A payload written with a `PROTOBUF_NATIVE` schema's `definition`, read with its descriptors. A definition that
    /// doesn't read fails each time.
    pub(super) fn read(&self, definition: &[u8], data: &[u8]) -> Result<DynamicMessage> {
        let mut parsed = self.0.lock().unwrap_or_else(PoisonError::into_inner);
        let message_type = match parsed.get(definition) {
            Some(message_type) => message_type.clone(),
            None => {
                let message_type = native(definition)?;
                parsed.insert(definition.to_vec(), message_type.clone());
                message_type
            }
        };
        drop(parsed);
        message_type.read(data)
    }
}

/// The message type of a `PROTOBUF_NATIVE` definition, as Java writes one: the descriptor set in base64, and the
/// message's full name.
fn native(definition: &[u8]) -> Result<MessageType> {
    let unreadable = |what: String| Error::new(ErrorKind::Other, format!("the PROTOBUF_NATIVE definition {what}"));
    let json: serde_json::Value =
        serde_json::from_slice(definition).map_err(|e| unreadable(format!("isn't JSON: {e}")))?;
    let text = |name: &str| {
        json.get(name).and_then(serde_json::Value::as_str).ok_or_else(|| unreadable(format!("has no {name}")))
    };
    let set = STANDARD
        .decode(text("fileDescriptorSet")?)
        .map_err(|e| unreadable(format!("has a descriptor set that isn't base64: {e}")))?;
    MessageType::decode(&set, text("rootMessageTypeName")?, ErrorKind::Other)
}

#[cfg(test)]
mod tests {
    use buffa_descriptor::generated::descriptor::{
        EnumValueDescriptorProto, FieldDescriptorProto, FileOptions, MessageOptions, field_descriptor_proto,
    };
    use buffa_descriptor::{ReflectMessage as _, Value};
    use buffa_types::google::protobuf::{Api, Struct, Timestamp};

    use super::*;

    /// What Java 5.0.1 declares for `com.google.protobuf.Timestamp` with v4's `Schema.PROTOBUF_NATIVE`.
    const JAVA_TIMESTAMP: &str = concat!(
        r#"{"fileDescriptorSet":"Cv8BCh9nb29nbGUvcHJvdG9idWYvdGltZXN0YW1wLnByb3RvEg9nb29nbGUucHJvdG9idWYiOwoJVGltZXN0YW1wEhgK"#,
        r#"B3NlY29uZHMYASABKANSB3NlY29uZHMSFAoFbmFub3MYAiABKAVSBW5hbm9zQoUBChNjb20uZ29vZ2xlLnByb3RvYnVmQg5UaW1lc3RhbXBQcm90"#,
        r#"b1ABWjJnb29nbGUuZ29sYW5nLm9yZy9wcm90b2J1Zi90eXBlcy9rbm93bi90aW1lc3RhbXBwYvgBAaICA0dQQqoCHkdvb2dsZS5Qcm90b2J1Zi5X"#,
        r#"ZWxsS25vd25UeXBlc2IGcHJvdG8z","rootMessageTypeName":"google.protobuf.Timestamp","#,
        r#""rootFileDescriptorName":"google/protobuf/timestamp.proto"}"#
    );
    const JAVA_TIMESTAMP_FIELDS: &str = concat!(
        r#"[{"number":1,"name":"seconds","type":"INT64","label":"LABEL_OPTIONAL","definition":null},"#,
        r#"{"number":2,"name":"nanos","type":"INT32","label":"LABEL_OPTIONAL","definition":null}]"#
    );

    /// What Java 5.0.1 declares for `com.google.protobuf.Timestamp` with V5's `Schema.protobuf`.
    const JAVA_TIMESTAMP_AVRO: &str = concat!(
        r#"{"type":"record","name":"Timestamp","namespace":"com.google.protobuf","fields":[{"name":"seconds","type""#,
        r#":"long","default":0},{"name":"nanos","type":"int","default":0}]}"#,
    );

    /// Java 5.0.1's `Schema.protobuf` definition of `google.protobuf.Struct`: a map as an array of its entries'
    /// records, records of other namespaces, and records written again by name, `Struct` within itself.
    const JAVA_STRUCT_AVRO: &str = concat!(
        r#"{"type":"record","name":"Struct","namespace":"com.google.protobuf","fields":[{"name":"fields","type":{"t"#,
        r#"ype":"array","items":{"type":"record","name":"FieldsEntry","namespace":"com.google.protobuf.Struct","fie"#,
        r#"lds":[{"name":"key","type":{"type":"string","avro.java.string":"String"},"default":""},{"name":"value",""#,
        r#"type":["null",{"type":"record","name":"Value","namespace":"com.google.protobuf","fields":[{"name":"null_"#,
        r#"value","type":{"type":"enum","name":"NullValue","symbols":["NULL_VALUE"],"default":"NULL_VALUE"},"defaul"#,
        r#"t":"NULL_VALUE"},{"name":"number_value","type":"double","default":0.0},{"name":"string_value","type":{"t"#,
        r#"ype":"string","avro.java.string":"String"},"default":""},{"name":"bool_value","type":"boolean","default""#,
        r#":false},{"name":"struct_value","type":["null","Struct"],"default":null},{"name":"list_value","type":["nu"#,
        r#"ll",{"type":"record","name":"ListValue","fields":[{"name":"values","type":{"type":"array","items":"Value"#,
        r#""},"default":[]}]}],"default":null}]}],"default":null}]}},"default":[]}]}"#,
    );

    /// What Java 5.0.1's Avro description of Protobuf, avro-protobuf's `ProtobufData.getSchema`, makes of
    /// [`edge_cases`]'s `edge.Everything`, read with v4's `ProtobufNativeSchemaUtils`.
    const JAVA_EDGE_CASES_AVRO: &str = concat!(
        r#"{"type":"record","name":"Everything","namespace":"edge.MyEdgeCases","fields":[{"name":"id","type":"int"}"#,
        r#",{"name":"small","type":"int","default":-5},{"name":"big","type":"int","default":-1},{"name":"long_one","#,
        r#""type":"long","default":5000000000},{"name":"huge","type":"long","default":-1},{"name":"ratio","type":"f"#,
        r#"loat","default":1.0E10},{"name":"tiny","type":"double","default":0.001},{"name":"flag","type":"boolean","#,
        r#""default":true},{"name":"label","type":{"type":"string","avro.java.string":"String"},"default":"a \"b\"""#,
        r#"},{"name":"color","type":{"type":"enum","name":"Color","symbols":["RED","GREEN","BLUE"],"default":"BLUE""#,
        r#"},"default":"BLUE"},{"name":"other_color","type":"Color","default":"RED"},{"name":"inners","type":{"type"#,
        r#"":"array","items":{"type":"record","name":"Inner","namespace":"edge.MyEdgeCases$Everything","fields":[{""#,
        r#"name":"kind","type":{"type":"enum","name":"Kind","namespace":"edge.MyEdgeCases$Everything$Inner","symbol"#,
        r#"s":["A","B"],"default":"A"},"default":"A"},{"name":"scores","type":{"type":"array","items":"float"},"def"#,
        r#"ault":[]}]}},"default":[]},{"name":"inner","type":["null","edge.MyEdgeCases$Everything.Inner"],"default""#,
        r#":null},{"name":"counts","type":{"type":"array","items":{"type":"record","name":"CountsEntry","namespace""#,
        r#":"edge.MyEdgeCases$Everything","fields":[{"name":"key","type":{"type":"string","avro.java.string":"Strin"#,
        r#"g"},"default":""},{"name":"value","type":"long","default":0}]}},"default":[]},{"name":"rec","type":["nul"#,
        r#"l",{"type":"record","name":"record","fields":[{"name":"x","type":"int","default":0}]}],"default":null},{"#,
        r#""name":"rec2","type":["null","edge.MyEdgeCases.record"],"default":null},{"name":"bare","type":["null",{""#,
        r#"type":"record","name":"Bare","namespace":"","fields":[{"name":"data","type":"bytes","default":""},{"name"#,
        r#"":"f","type":"long","default":0}]}],"default":null},{"name":"thing","type":["null",{"type":"record","nam"#,
        r#"e":"Thing","namespace":"com.example.c.Outer","fields":[{"name":"n","type":"int","default":0}]}],"default"#,
        r#"":null},{"name":"plain","type":"long","default":0},{"name":"plain_double","type":"double","default":0.0}"#,
        r#",{"name":"plain_string","type":{"type":"string","avro.java.string":"String"},"default":""},{"name":"requ"#,
        r#"ired_inner","type":"edge.MyEdgeCases$Everything.Inner"}]}"#,
    );

    /// The descriptor set of a `PROTOBUF_NATIVE` definition.
    fn descriptors(definition: &[u8]) -> Vec<u8> {
        let json: serde_json::Value = serde_json::from_slice(definition).unwrap();
        STANDARD.decode(json["fileDescriptorSet"].as_str().unwrap()).unwrap()
    }

    fn names(set: &[u8]) -> Vec<String> {
        let set = FileDescriptorSet::decode_from_slice(set).unwrap();
        set.file.iter().map(|file| file.name.clone().unwrap_or_default()).collect()
    }

    /// A buffa type declares what Java declares for the same message: its file's descriptors, which are the same,
    /// its names and Java's properties.
    #[test]
    fn a_buffa_type_declares_what_java_declares() {
        let schema = Schema::<Timestamp>::protobuf_native();
        let info = schema.info();
        assert_eq!((info.name(), info.schema_type()), ("", SchemaType::ProtobufNative));
        let (ours, java): (serde_json::Value, serde_json::Value) =
            (serde_json::from_slice(info.definition()).unwrap(), serde_json::from_str(JAVA_TIMESTAMP).unwrap());
        assert_eq!(ours["rootMessageTypeName"], java["rootMessageTypeName"]);
        assert_eq!(ours["rootFileDescriptorName"], java["rootFileDescriptorName"]);
        let (ours, java) = (descriptors(info.definition()), descriptors(JAVA_TIMESTAMP.as_bytes()));
        assert_eq!(
            FileDescriptorSet::decode_from_slice(&ours).unwrap(),
            FileDescriptorSet::decode_from_slice(&java).unwrap()
        );
        let properties = info.properties();
        assert_eq!(properties["__PARSING_INFO__"], JAVA_TIMESTAMP_FIELDS);
        assert_eq!(
            (properties["__alwaysAllowNull"].as_str(), properties["__jsr310ConversionEnabled"].as_str()),
            ("true", "false")
        );
        assert_eq!(properties.len(), 3);

        let time = Timestamp { seconds: 5, nanos: 7, ..Default::default() };
        let payload = schema.encode(&time).unwrap();
        assert_eq!(&payload[..], [8, 5, 16, 7]);
        assert_eq!(schema.decode(&payload).unwrap(), time);
        let error = schema.decode(&[0xff]).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Protobuf"), "{error}");
    }

    /// The descriptor set holds the message's file and the files it imports, each once, after those it imports:
    /// `api.proto` imports `type.proto`, which imports `source_context.proto` too.
    #[test]
    fn the_descriptor_set_holds_the_imports() {
        let info = Schema::<Api>::protobuf_native().info().clone();
        let files = names(&descriptors(info.definition()));
        let expected = [
            "google/protobuf/source_context.proto",
            "google/protobuf/any.proto",
            "google/protobuf/type.proto",
            "google/protobuf/api.proto",
        ];
        assert_eq!(files, expected);
    }

    /// Java's `__PARSING_INFO__` for a message nested in another, and for one of a file without a package.
    #[test]
    fn parsing_info_of_nested_messages_and_messages_without_a_package() {
        let set = descriptors(Schema::<Struct>::protobuf_native().info().definition());
        let entry = MessageType::decode(&set, "google.protobuf.Struct.FieldsEntry", ErrorKind::Other).unwrap();
        let expected = concat!(
            r#"[{"number":1,"name":"key","type":"STRING","label":"LABEL_OPTIONAL","definition":null},"#,
            r#"{"number":2,"name":"value","type":"MESSAGE","label":"LABEL_OPTIONAL","definition":null}]"#
        );
        assert_eq!(entry.parsing_info(), expected);
        let structure = MessageType::decode(&set, "google.protobuf.Struct", ErrorKind::Other).unwrap();
        assert_eq!(
            structure.parsing_info(),
            r#"[{"number":1,"name":"fields","type":"MESSAGE","label":"LABEL_REPEATED","definition":null}]"#
        );

        let field = FieldDescriptorProto {
            name: Some("id".to_string()),
            number: Some(1),
            label: Some(field_descriptor_proto::Label::LABEL_REPEATED),
            r#type: Some(field_descriptor_proto::Type::TYPE_SINT64),
            ..Default::default()
        };
        let message = DescriptorProto { name: Some("Bare".to_string()), field: vec![field], ..Default::default() };
        let file = FileDescriptorProto {
            name: Some("bare.proto".to_string()),
            message_type: vec![message],
            ..Default::default()
        };
        let set = FileDescriptorSet { file: vec![file], ..Default::default() }.encode_to_vec();
        let bare = MessageType::decode(&set, "Bare", ErrorKind::Other).unwrap();
        assert_eq!(
            bare.parsing_info(),
            r#"[{"number":1,"name":"id","type":"SINT64","label":"LABEL_REPEATED","definition":null}]"#
        );
        let definition: serde_json::Value = serde_json::from_slice(&bare.native_info().definition).unwrap();
        assert_eq!(definition["rootFileDescriptorName"], "bare.proto");
    }

    /// A definition as Java writes it reads messages, each field by its descriptor, the type's descriptors parsed
    /// once.
    #[test]
    fn messages_read_with_javas_definition() {
        let parsed = Parsed::default();
        let message = parsed.read(JAVA_TIMESTAMP.as_bytes(), &[8, 5]).unwrap();
        let seconds = message.message_descriptor().field_by_name("seconds").unwrap();
        assert_eq!(message.get(seconds).to_owned(), Value::I64(5));
        let nanos = message.message_descriptor().field_by_name("nanos").unwrap();
        assert_eq!(message.get(nanos).to_owned(), Value::I32(0), "a field the message doesn't have reads its default");
        assert!(parsed.read(JAVA_TIMESTAMP.as_bytes(), &[16, 3]).is_ok());
        assert_eq!(parsed.0.lock().unwrap().len(), 1);
        let error = parsed.read(JAVA_TIMESTAMP.as_bytes(), &[0xff]).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Protobuf"), "{error}");
    }

    #[test]
    fn definitions_that_dont_read() {
        let parsed = Parsed::default();
        let refused = |definition: &str| parsed.read(definition.as_bytes(), &[]).unwrap_err().to_string();
        assert!(refused("{").contains("isn't JSON"));
        assert!(refused("{}").contains("has no fileDescriptorSet"));
        assert!(refused(r#"{"fileDescriptorSet":"!"}"#).contains("isn't base64"));
        assert!(
            refused(r#"{"fileDescriptorSet":"/w==","rootMessageTypeName":"A"}"#).contains("aren't a FileDescriptorSet")
        );
        let missing = JAVA_TIMESTAMP.replace("google.protobuf.Timestamp", "google.protobuf.Duration");
        assert!(refused(&missing).contains("have no message google.protobuf.Duration"));
    }

    /// A set as prost-build writes one, with source info and files the message doesn't need, declares what buffa's
    /// does: only the message's file and its imports, without their source info, which Java's descriptors don't have.
    /// A set that lacks a file the message's file imports is refused.
    #[cfg(feature = "prost")]
    #[test]
    fn a_set_as_prost_build_writes_one() {
        use buffa_descriptor::generated::descriptor::{SourceCodeInfo, source_code_info};

        let buffa = Schema::<Timestamp>::protobuf_native();
        let mut set = FileDescriptorSet::decode_from_slice(&descriptors(buffa.info().definition())).unwrap();
        let location =
            source_code_info::Location { leading_comments: Some("A time".to_string()), ..Default::default() };
        set.file[0].source_code_info = SourceCodeInfo { location: vec![location], ..Default::default() }.into();
        set.file.push(FileDescriptorProto { name: Some("unrelated.proto".to_string()), ..Default::default() });
        let schema = Schema::<prost_types::Timestamp>::prost_native(&set.encode_to_vec()).unwrap();
        assert_eq!(schema.info(), buffa.info());

        set.file[0].dependency.push("missing.proto".to_string());
        let refused = Schema::<prost_types::Timestamp>::prost_native(&set.encode_to_vec()).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::InvalidConfiguration);
        let expected = "lack missing.proto, which google/protobuf/timestamp.proto imports";
        assert!(refused.to_string().contains(expected), "{refused}");
    }

    /// A prost type, declared from the descriptor set it was generated from, declares what a buffa type does, with
    /// either of Java's Protobuf schemas.
    #[cfg(feature = "prost")]
    #[test]
    fn a_prost_type_declares_what_a_buffa_type_does() {
        let buffa = Schema::<Timestamp>::protobuf_native();
        let set = descriptors(buffa.info().definition());
        let schema = Schema::<prost_types::Timestamp>::prost_native(&set).unwrap();
        assert_eq!(schema.info(), buffa.info());
        let time = prost_types::Timestamp { seconds: 5, nanos: 7 };
        let payload = schema.encode(&time).unwrap();
        assert_eq!(&payload[..], [8, 5, 16, 7]);
        assert_eq!(schema.decode(&payload).unwrap(), time);
        let error = schema.decode(&[0xff]).unwrap_err().to_string();
        assert!(error.contains("the payload doesn't decode in Protobuf"), "{error}");
        let refused = Schema::<prost_types::Duration>::prost_native(&set).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::InvalidConfiguration);
        assert!(refused.to_string().contains("no message google.protobuf.Duration"), "{refused}");
        let refused = Schema::<prost_types::Timestamp>::prost_native(&[0xff]).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::InvalidConfiguration);

        let schema = Schema::<prost_types::Timestamp>::prost(&set).unwrap();
        assert_eq!(schema.info(), Schema::<Timestamp>::protobuf().unwrap().info());
        assert_eq!(schema.decode(&schema.encode(&time).unwrap()).unwrap(), time);
        let refused = Schema::<prost_types::Duration>::prost(&set).unwrap_err();
        assert_eq!(refused.kind(), ErrorKind::InvalidConfiguration);
    }

    /// A buffa type declares what Java V5's `Schema.protobuf` declares for the same message: Avro's description of it,
    /// as Java writes it, and the properties of Java's `PROTOBUF_NATIVE`.
    #[test]
    fn a_buffa_type_declares_what_java_v5_declares() {
        let schema = Schema::<Timestamp>::protobuf().unwrap();
        let info = schema.info();
        assert_eq!((info.name(), info.schema_type()), ("", SchemaType::Protobuf));
        assert_eq!(std::str::from_utf8(info.definition()).unwrap(), JAVA_TIMESTAMP_AVRO);
        assert_eq!(info.properties(), Schema::<Timestamp>::protobuf_native().info().properties());
        let time = Timestamp { seconds: 5, nanos: 7, ..Default::default() };
        let payload = schema.encode(&time).unwrap();
        assert_eq!(&payload[..], [8, 5, 16, 7]);
        assert_eq!(schema.decode(&payload).unwrap(), time);
        assert!(schema.decode(&[0xff]).is_err());

        let definition = Schema::<Struct>::protobuf().unwrap().info().definition().to_vec();
        assert_eq!(String::from_utf8(definition).unwrap(), JAVA_STRUCT_AVRO);
    }

    /// Avro's description of proto2's names and defaults, as Java's: an outer class named after the file, `$` between
    /// nested classes, each kind of default, unsigned ones as Java's signed types hold them, required fields without
    /// one, records and enums written again by name, by full name for one named as an Avro type, and the records of a
    /// file without a package in an empty namespace. Java writes a float's default as Java does, `1.0E10`, and this
    /// client as Rust does: the same number to Avro, which is how the broker compares two definitions.
    #[test]
    fn proto2_names_and_defaults_as_java_describes_them() {
        let message_type = MessageType::decode(&edge_cases(), "edge.Everything", ErrorKind::Other).unwrap();
        let definition = String::from_utf8(message_type.avro_info().unwrap().definition).unwrap();
        assert_eq!(definition, JAVA_EDGE_CASES_AVRO.replace("1.0E10", "10000000000.0"));
    }

    /// What Java's description refuses, this client's does: a group, a `bytes` field's default, and a float's
    /// infinity. Java 5.0.1 fails on each, with `Unexpected type: GROUP`, a `ClassCastException` and an
    /// `AvroTypeException`.
    #[test]
    fn what_javas_description_refuses() {
        let set = edge_cases();
        let refused = |message: &str| {
            let error = MessageType::decode(&set, message, ErrorKind::Other).unwrap().avro_info().unwrap_err();
            assert_eq!(error.kind(), ErrorKind::InvalidConfiguration);
            error.to_string()
        };
        assert!(refused("edge.WithGroup").contains("edge.WithGroup.g is a group"));
        assert!(refused("edge.WithBytes").contains("edge.WithBytes.b has a default that Avro can't hold"));
        assert!(refused("edge.WithInfinity").contains("edge.WithInfinity.d has a default that Avro can't hold"));
    }

    /// avro-protobuf's outer classes of files that name none. Where Java's camel case fails, without an extension or
    /// with an empty part between underscores, this client's names one all the same.
    #[test]
    fn outer_classes_of_files_that_name_none() {
        assert_eq!(outer_class("edge/my_edge_cases.proto"), "MyEdgeCases");
        assert_eq!(outer_class("fooBar.proto"), "Foobar");
        assert_eq!(outer_class("double__underscore.proto"), "DoubleUnderscore");
        assert_eq!(outer_class("no_extension"), "NoExtension");
    }

    /// Descriptors of proto2's corners, as protoc writes them: `edge/my_edge_cases.proto` in package `edge`, without
    /// Java options, which imports a file without a package and one with a Java package and outer class.
    fn edge_cases() -> Vec<u8> {
        use field_descriptor_proto::Label::{LABEL_OPTIONAL, LABEL_REPEATED, LABEL_REQUIRED};
        use field_descriptor_proto::Type::*;

        let f = |name: &str, number: i32, label: field_descriptor_proto::Label, kind: field_descriptor_proto::Type| {
            FieldDescriptorProto {
                name: Some(name.to_string()),
                number: Some(number),
                label: Some(label),
                r#type: Some(kind),
                ..Default::default()
            }
        };
        let typed = |mut field: FieldDescriptorProto, type_name: &str| {
            field.type_name = Some(type_name.to_string());
            field
        };
        let defaulted = |mut field: FieldDescriptorProto, default: &str| {
            field.default_value = Some(default.to_string());
            field
        };
        let message = |name: &str, field: Vec<FieldDescriptorProto>| DescriptorProto {
            name: Some(name.to_string()),
            field,
            ..Default::default()
        };
        let enumeration = |name: &str, values: &[&str]| EnumDescriptorProto {
            name: Some(name.to_string()),
            value: (values.iter().zip(0..))
                .map(|(value, number)| EnumValueDescriptorProto {
                    name: Some(value.to_string()),
                    number: Some(number),
                    ..Default::default()
                })
                .collect(),
            ..Default::default()
        };
        let bare =
            message("Bare", vec![f("data", 1, LABEL_OPTIONAL, TYPE_BYTES), f("f", 2, LABEL_OPTIONAL, TYPE_FIXED64)]);
        let other = FileDescriptorProto {
            name: Some("other.proto".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![bare],
            options: FileOptions { java_multiple_files: Some(true), ..Default::default() }.into(),
            ..Default::default()
        };
        let classes = FileOptions {
            java_package: Some("com.example.c".to_string()),
            java_outer_classname: Some("Outer".to_string()),
            ..Default::default()
        };
        let c = FileDescriptorProto {
            name: Some("c.proto".to_string()),
            package: Some("pkg.c".to_string()),
            syntax: Some("proto3".to_string()),
            message_type: vec![message("Thing", vec![f("n", 1, LABEL_OPTIONAL, TYPE_SFIXED32)])],
            options: classes.into(),
            ..Default::default()
        };
        let kind = typed(f("kind", 1, LABEL_OPTIONAL, TYPE_ENUM), ".edge.Everything.Inner.Kind");
        let mut inner = message("Inner", vec![kind, f("scores", 2, LABEL_REPEATED, TYPE_FLOAT)]);
        inner.enum_type = vec![enumeration("Kind", &["A", "B"])];
        let entry = vec![f("key", 1, LABEL_OPTIONAL, TYPE_STRING), f("value", 2, LABEL_OPTIONAL, TYPE_INT64)];
        let mut counts = message("CountsEntry", entry);
        counts.options = MessageOptions { map_entry: Some(true), ..Default::default() }.into();
        let mut everything = message(
            "Everything",
            vec![
                f("id", 1, LABEL_REQUIRED, TYPE_INT32),
                defaulted(f("small", 2, LABEL_OPTIONAL, TYPE_INT32), "-5"),
                defaulted(f("big", 3, LABEL_OPTIONAL, TYPE_UINT32), "4294967295"),
                defaulted(f("long_one", 4, LABEL_OPTIONAL, TYPE_INT64), "5000000000"),
                defaulted(f("huge", 5, LABEL_OPTIONAL, TYPE_UINT64), "18446744073709551615"),
                defaulted(f("ratio", 6, LABEL_OPTIONAL, TYPE_FLOAT), "1e+10"),
                defaulted(f("tiny", 7, LABEL_OPTIONAL, TYPE_DOUBLE), "0.001"),
                defaulted(f("flag", 8, LABEL_OPTIONAL, TYPE_BOOL), "true"),
                defaulted(f("label", 9, LABEL_OPTIONAL, TYPE_STRING), "a \"b\""),
                defaulted(typed(f("color", 10, LABEL_OPTIONAL, TYPE_ENUM), ".edge.Color"), "BLUE"),
                typed(f("other_color", 11, LABEL_OPTIONAL, TYPE_ENUM), ".edge.Color"),
                typed(f("inners", 12, LABEL_REPEATED, TYPE_MESSAGE), ".edge.Everything.Inner"),
                typed(f("inner", 13, LABEL_OPTIONAL, TYPE_MESSAGE), ".edge.Everything.Inner"),
                typed(f("counts", 14, LABEL_REPEATED, TYPE_MESSAGE), ".edge.Everything.CountsEntry"),
                typed(f("rec", 15, LABEL_OPTIONAL, TYPE_MESSAGE), ".edge.record"),
                typed(f("rec2", 16, LABEL_OPTIONAL, TYPE_MESSAGE), ".edge.record"),
                typed(f("bare", 17, LABEL_OPTIONAL, TYPE_MESSAGE), ".Bare"),
                typed(f("thing", 18, LABEL_OPTIONAL, TYPE_MESSAGE), ".pkg.c.Thing"),
                f("plain", 19, LABEL_OPTIONAL, TYPE_SINT64),
                f("plain_double", 20, LABEL_OPTIONAL, TYPE_DOUBLE),
                f("plain_string", 21, LABEL_OPTIONAL, TYPE_STRING),
                typed(f("required_inner", 22, LABEL_REQUIRED, TYPE_MESSAGE), ".edge.Everything.Inner"),
            ],
        );
        everything.nested_type = vec![inner, counts];
        let mut with_group =
            message("WithGroup", vec![typed(f("g", 1, LABEL_OPTIONAL, TYPE_GROUP), ".edge.WithGroup.G")]);
        with_group.nested_type = vec![message("G", vec![f("a", 2, LABEL_OPTIONAL, TYPE_INT32)])];
        let edge = FileDescriptorProto {
            name: Some("edge/my_edge_cases.proto".to_string()),
            package: Some("edge".to_string()),
            dependency: vec!["other.proto".to_string(), "c.proto".to_string()],
            message_type: vec![
                everything,
                message("record", vec![f("x", 1, LABEL_OPTIONAL, TYPE_SINT32)]),
                with_group,
                message("WithBytes", vec![defaulted(f("b", 1, LABEL_OPTIONAL, TYPE_BYTES), "abc")]),
                message("WithInfinity", vec![defaulted(f("d", 1, LABEL_OPTIONAL, TYPE_DOUBLE), "inf")]),
            ],
            enum_type: vec![enumeration("Color", &["RED", "GREEN", "BLUE"])],
            ..Default::default()
        };
        FileDescriptorSet { file: vec![other, c, edge], ..Default::default() }.encode_to_vec()
    }
}
