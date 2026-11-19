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

//! Accessors that apply proto2 `[default = …]` values.
//!
//! buffa leaves an absent proto2 `optional` field as `None` and does not apply its declared default: an absent
//! `MessageIdData.partition` is `None`, where the protocol means −1. Every field whose declared default is not
//! the type's zero value gets an accessor here, on the message and on its view, named like the field:
//! `id.partition()` is `id.partition.unwrap_or(-1)`. Read those fields through the accessor.
//!
//! The test at the bottom reads every `[default]` in the vendored proto and fails if one is missing here, so a
//! proto refresh that adds a default cannot go unnoticed.

#![allow(non_snake_case)]
use crate::proto;
use crate::proto::__buffa::view;

macro_rules! defaults {
    ($( $msg:ident / $view:ident . $field:ident : $ty:ty = $value:expr ; )*) => {
        $(
            impl proto::$msg {
                #[doc = concat!("`", stringify!($field), "`, or its proto default `", stringify!($value), "` when absent.")]
                #[inline]
                pub fn $field(&self) -> $ty {
                    self.$field.unwrap_or($value)
                }
            }

            impl view::$view<'_> {
                #[doc = concat!("`", stringify!($field), "`, or its proto default `", stringify!($value), "` when absent.")]
                #[inline]
                pub fn $field(&self) -> $ty {
                    self.$field.unwrap_or($value)
                }
            }
        )*

        /// `(message, field, default of the message's accessor, default of the view's accessor)`, for the test.
        #[cfg(test)]
        fn table() -> Vec<(&'static str, &'static str, String, String)> {
            vec![$((
                stringify!($msg),
                stringify!($field),
                format!("{:?}", proto::$msg::default().$field()),
                format!("{:?}", view::$view::default().$field()),
            )),*]
        }
    };
}

use proto::CompressionType;
use proto::ProducerAccessMode;
use proto::command_get_topics_of_namespace::Mode;
use proto::command_subscribe::InitialPosition;

defaults! {
    MessageIdData / MessageIdDataView . partition: i32 = -1;
    MessageIdData / MessageIdDataView . batch_index: i32 = -1;
    MessageMetadata / MessageMetadataView . compression: CompressionType = CompressionType::NONE;
    MessageMetadata / MessageMetadataView . num_messages_in_batch: i32 = 1;
    CommandSubscribe / CommandSubscribeView . durable: bool = true;
    CommandSubscribe / CommandSubscribeView . initialPosition: InitialPosition = InitialPosition::Latest;
    CommandSubscribe / CommandSubscribeView . force_topic_creation: bool = true;
    CommandPartitionedTopicMetadata / CommandPartitionedTopicMetadataView . metadata_auto_creation_enabled: bool = true;
    CommandProducer / CommandProducerView . user_provided_producer_name: bool = true;
    CommandProducer / CommandProducerView . producer_access_mode: ProducerAccessMode = ProducerAccessMode::Shared;
    CommandSend / CommandSendView . num_messages: i32 = 1;
    CommandProducerSuccess / CommandProducerSuccessView . last_sequence_id: i64 = -1;
    CommandProducerSuccess / CommandProducerSuccessView . producer_ready: bool = true;
    CommandGetTopicsOfNamespace / CommandGetTopicsOfNamespaceView . mode: Mode = Mode::PERSISTENT;
    CommandGetTopicsOfNamespaceResponse / CommandGetTopicsOfNamespaceResponseView . changed: bool = true;
    CommandScalableTopicLookup / CommandScalableTopicLookupView . create_if_missing: bool = true;
}

#[cfg(test)]
mod tests {
    use std::collections::BTreeMap;

    /// Every `[default = …]` in the proto whose value is not `0` or `false`, as
    /// `(message, field) -> value`. Enum defaults are included even when they name the first value, so that an
    /// absent enum field is always read through an accessor.
    fn proto_defaults() -> BTreeMap<(String, String), String> {
        let proto = include_str!("../proto/PulsarApi.proto");
        let mut blocks: Vec<Option<String>> = Vec::new();
        let mut found = BTreeMap::new();
        let mut all = 0;
        for line in proto.lines() {
            let code = line.split("//").next().unwrap().trim();
            let words: Vec<&str> = code.split_whitespace().collect();
            if code.ends_with('{') {
                let message = (words.first() == Some(&"message")).then(|| words[1].trim_end_matches('{').to_string());
                blocks.push(message);
                continue;
            }
            if code.starts_with('}') {
                blocks.pop();
                continue;
            }
            // The field options, `[ default = V ]` with any spacing.
            let Some(options) = code.split_once('[').map(|(_, rest)| rest) else { continue };
            let Some(value) = options.trim_start().strip_prefix("default") else { continue };
            let value = value.trim_start().strip_prefix('=').expect("default without '='");
            let value = value.split([']', ',']).next().unwrap().trim().to_string();
            all += 1;
            if value == "0" || value == "false" {
                continue;
            }
            let field = words[2].to_string();
            let message = blocks.iter().rev().flatten().next().expect("field outside a message").clone();
            found.insert((message, field), value);
        }
        // Every occurrence of the word, however it is spaced, must have been parsed above.
        let occurrences = proto
            .lines()
            .map(|line| line.split("//").next().unwrap())
            .filter(|code| code.split(|c: char| !c.is_alphanumeric() && c != '_').any(|word| word == "default"))
            .count();
        assert_eq!(all, occurrences, "the parser missed a [default] in the proto");
        found
    }

    #[test]
    fn every_proto_default_has_an_accessor() {
        let expected = proto_defaults();
        let table = super::table();
        let mut covered = BTreeMap::new();
        for (message, field, owned, view) in table {
            assert_eq!(owned, view, "{message}.{field}: message and view accessors disagree");
            covered.insert((message.to_string(), field.to_string()), owned);
        }
        assert_eq!(covered, expected, "accessors in defaults.rs must match the proto's [default] values");
    }

    #[test]
    fn present_value_wins_over_default() {
        let id = crate::proto::MessageIdData { partition: Some(3), ..Default::default() };
        assert_eq!(id.partition(), 3);
        assert_eq!(id.batch_index(), -1);
    }
}
