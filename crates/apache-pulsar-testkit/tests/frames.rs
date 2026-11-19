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

//! Frames the decoder rejects, payload frames in their optional layouts, and what a connection does with
//! bytes it can't parse.

use apache_pulsar_protocol::connection::{ConnectOptions, ConnectionError, Event};
use apache_pulsar_protocol::frame::{
    DEFAULT_MAX_MESSAGE_SIZE, FRAME_OVERHEAD, Frame, FrameDecoder, FrameError, MAGIC_BROKER_ENTRY_METADATA,
    MAGIC_CRC32C,
};
use apache_pulsar_protocol::proto::base_command::Type;
use apache_pulsar_protocol::proto::{BaseCommand, CommandConnected, CommandSend, MessageMetadata};
use apache_pulsar_testkit::{Link, SimBroker};
use bytes::{BufMut, BytesMut};

fn decode(bytes: &[u8]) -> Result<Option<Frame>, FrameError> {
    FrameDecoder::default().decode(&mut BytesMut::from(bytes))
}

/// A frame of an empty command followed by `rest`.
fn with_empty_command(rest: &[u8]) -> Vec<u8> {
    let mut frame = Vec::new();
    frame.put_u32(4 + rest.len() as u32);
    frame.put_u32(0);
    frame.extend_from_slice(rest);
    frame
}

fn closed_with(events: &[Event]) -> Option<&ConnectionError> {
    events.iter().find_map(|e| match e {
        Event::Closed(error) => Some(error),
        _ => None,
    })
}

#[test]
fn malformed_frames_are_rejected() {
    let malformed = |what| Err(FrameError::Malformed(what));
    assert_eq!(decode(&[0, 0, 0, 3, 0, 0, 0]), malformed("total size smaller than the command size field"));
    assert_eq!(decode(&[0, 0, 0, 4, 0, 0, 0, 9]), malformed("command size larger than the frame"));

    let magic = MAGIC_CRC32C.to_be_bytes();
    assert_eq!(decode(&with_empty_command(&[magic[0], magic[1], 0, 0])), malformed("truncated checksum"));
    // A payload section too short to hold the metadata size, and one whose metadata size overruns it.
    assert_eq!(decode(&with_empty_command(&[0, 0])), malformed("message metadata"));
    assert_eq!(decode(&with_empty_command(&[0, 0, 0, 9, 1])), malformed("message metadata"));

    let magic = MAGIC_BROKER_ENTRY_METADATA.to_be_bytes();
    assert_eq!(decode(&with_empty_command(&[magic[0], magic[1], 0, 0, 0, 5, 1])), malformed("broker entry metadata"));
}

#[test]
fn payload_frames_without_a_checksum() {
    // Metadata size, metadata, payload: no checksum section.
    let frame = decode(&with_empty_command(&[0, 0, 0, 1, 0xaa, b'h', b'i'])).unwrap().unwrap();
    let section = frame.payload.unwrap();
    assert_eq!(section.checksum_valid, None);
    assert_eq!(section.broker_entry_metadata, None);
    assert_eq!(&section.metadata[..], &[0xaa]);
    assert_eq!(&section.payload[..], b"hi");

    // Broker entry metadata, then the same.
    let magic = MAGIC_BROKER_ENTRY_METADATA.to_be_bytes();
    let frame =
        decode(&with_empty_command(&[magic[0], magic[1], 0, 0, 0, 1, 0xbb, 0, 0, 0, 0, b'x'])).unwrap().unwrap();
    let section = frame.payload.unwrap();
    assert_eq!(section.broker_entry_metadata.as_deref(), Some(&[0xbb][..]));
    assert_eq!(section.checksum_valid, None);
    assert_eq!(&section.payload[..], b"x");
}

#[test]
fn payload_commands_go_out_with_a_checksum() {
    let mut link = Link::new(ConnectOptions::default(), SimBroker::new());
    link.pump();
    let send = BaseCommand {
        r#type: Type::SEND.into(),
        send: CommandSend { producer_id: 1, sequence_id: 7, ..Default::default() }.into(),
        ..Default::default()
    };
    let metadata = MessageMetadata { producer_name: "p".into(), sequence_id: 7, publish_time: 1, ..Default::default() };
    link.client.send_payload(&send, &metadata, b"hello").unwrap();

    let bytes = link.client.poll_transmit().unwrap();
    let frame = decode(&bytes).unwrap().unwrap();
    let section = frame.payload.unwrap();
    assert_eq!(section.checksum_valid, Some(true));
    assert_eq!(&section.payload[..], b"hello");
}

#[test]
fn payload_commands_wait_for_the_handshake() {
    let mut broker = SimBroker::new();
    broker.mute = true;
    let mut link = Link::new(ConnectOptions::default(), broker);
    link.pump();
    let send = BaseCommand { r#type: Type::SEND.into(), ..Default::default() };
    let error = link.client.send_payload(&send, &MessageMetadata::default(), b"x").unwrap_err();
    assert_eq!(error, ConnectionError::NotReady);
}

#[test]
fn bytes_that_do_not_parse_close_the_connection() {
    let mut link = Link::new(ConnectOptions::default(), SimBroker::new());
    link.pump();
    link.client.handle_input(&[0, 0, 0, 2, 0, 0], link.clock.now());
    let events = link.pump();
    assert_eq!(
        closed_with(&events),
        Some(&ConnectionError::Frame(FrameError::Malformed("total size smaller than the command size field")))
    );

    // A well-formed frame whose command does not decode.
    let mut link = Link::new(ConnectOptions::default(), SimBroker::new());
    link.pump();
    link.client.handle_input(&[0, 0, 0, 6, 0, 0, 0, 2, 0xff, 0xff], link.clock.now());
    let events = link.pump();
    assert!(matches!(closed_with(&events), Some(ConnectionError::Decode(_))), "{events:?}");
}

#[test]
fn without_a_max_message_size_the_default_limit_holds() {
    let mut broker = SimBroker::new();
    broker.mute = true;
    let mut link = Link::new(ConnectOptions::default(), broker);
    link.pump();
    let connected = BaseCommand {
        r#type: Type::CONNECTED.into(),
        connected: CommandConnected { server_version: "old".into(), protocol_version: Some(21), ..Default::default() }
            .into(),
        ..Default::default()
    };
    let events = link.push(&connected);
    assert!(matches!(events.as_slice(), [Event::Connected(info)] if info.max_message_size.is_none()), "{events:?}");

    let too_large = DEFAULT_MAX_MESSAGE_SIZE + FRAME_OVERHEAD + 1;
    link.client.handle_input(&too_large.to_be_bytes(), link.clock.now());
    let events = link.pump();
    assert!(matches!(closed_with(&events), Some(ConnectionError::Frame(FrameError::TooLarge { .. }))), "{events:?}");
}
