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

//! Protobuf: Java's `PROTOBUF_NATIVE` schema, messages that their descriptors describe. The descriptors are read with
//! buffa's: a buffa type brings its own; another, such as a prost type, those of the `FileDescriptorSet` it was
//! generated from.

use std::collections::{BTreeMap, HashMap};
use std::sync::{Arc, Mutex, PoisonError};

use base64::Engine as _;
use base64::engine::general_purpose::STANDARD;
use buffa::{Enumeration as _, Message as _};
use buffa_descriptor::generated::descriptor::{DescriptorProto, FileDescriptorProto, FileDescriptorSet};
use buffa_descriptor::{DescriptorPool, DynamicMessage, MessageDescriptor, MessageIndex, Reflectable};

use super::{Generic, Schema, SchemaInfo, SchemaType};
use crate::error::{Error, ErrorKind, Result};

impl<T: buffa::Message + Reflectable + Default + 'static> Schema<T> {
    /// Messages of a buffa type in Protobuf's encoding, declared as Java's `Schema.PROTOBUF_NATIVE(Class)` declares
    /// them: by the descriptors of the message's file and of the files it imports, which the broker checks a new
    /// version against. The type needs buffa's reflection (buffa-build's `generate_reflection`), which brings its
    /// descriptors.
    pub fn protobuf_native() -> Self {
        let message = T::default();
        let reflected = message.reflect();
        let full_name = reflected.message_descriptor().full_name();
        let message_type = MessageType::new(reflected.pool().clone(), full_name, ErrorKind::Other);
        Self {
            info: message_type.expect("a buffa type's pool has its descriptor").native_info(),
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
        let message_type = MessageType::decode(descriptors, &T::full_name(), ErrorKind::InvalidConfiguration)?;
        Ok(Self {
            info: message_type.native_info(),
            encode: Arc::new(|value: &T| Ok(bytes::Bytes::from(value.encode_to_vec()))),
            decode: Arc::new(|data: &[u8], _: Option<&SchemaInfo>| T::decode(data).map_err(prost_undecodable)),
            generic: Generic::No,
        })
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

    /// What the broker is told, as Java's `ProtobufNativeSchema` tells it: no name, the descriptor set of the
    /// message's file and its imports in base64, with the message's and the file's names, and Java's properties.
    fn native_info(&self) -> SchemaInfo {
        let file = self.file();
        let set = FileDescriptorSet { file: self.files.clone(), ..Default::default() };
        let definition = format!(
            r#"{{"fileDescriptorSet":{},"rootMessageTypeName":{},"rootFileDescriptorName":{}}}"#,
            json(&STANDARD.encode(set.encode_to_vec())),
            json(self.descriptor().full_name()),
            json(file.name.as_deref().unwrap_or_default()),
        );
        let properties = [
            ("__PARSING_INFO__", self.parsing_info()),
            ("__alwaysAllowNull", "true".to_string()),
            ("__jsr310ConversionEnabled", "false".to_string()),
        ];
        SchemaInfo {
            name: String::new(),
            schema_type: SchemaType::ProtobufNative,
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

    /// The message as its file declares it: a message of the file, or one nested in another.
    fn proto(&self) -> &DescriptorProto {
        let file = self.file();
        let full_name = self.descriptor().full_name();
        let package = file.package.as_deref().unwrap_or_default();
        let local = full_name.strip_prefix(package).and_then(|name| name.strip_prefix('.')).unwrap_or(full_name);
        let mut names = local.split('.');
        let outer = named(&file.message_type, names.next());
        names.fold(outer, |message, name| named(&message.nested_type, Some(name)))
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

/// The message of `messages` named `name`.
fn named<'a>(messages: &'a [DescriptorProto], name: Option<&str>) -> &'a DescriptorProto {
    messages.iter().find(|message| message.name.as_deref() == name).expect("a file declares its messages")
}

/// A string, as JSON writes it.
fn json(text: &str) -> String {
    serde_json::to_string(text).expect("a string is JSON")
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
    use buffa_descriptor::generated::descriptor::{FieldDescriptorProto, field_descriptor_proto};
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

    /// A prost type, declared from the descriptor set it was generated from, declares what a buffa type does.
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
    }
}
