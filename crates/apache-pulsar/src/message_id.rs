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

//! Message identifiers (docs/7-api.md §7).

use std::fmt;

use apache_pulsar_protocol::message_id as wire;

use crate::error::{Error, ErrorKind, Result};

/// The identifier of a message. Opaque, comparable and serializable.
///
/// Its bytes are the Java V5 client's (`MessageIdV5`), so an id saved by one SDK can be restored by the other.
/// Ids order by segment, then by position in the segment, as Java's `compareTo` does: within one segment that is
/// publish order, across segments it is not. [`MessageId::earliest`] and [`MessageId::latest`] have no segment, so
/// both sort before the ids of every segment.
#[derive(Clone, PartialEq, Eq, Hash, PartialOrd, Ord)]
pub struct MessageId(pub(crate) wire::MessageId);

impl MessageId {
    /// The position before every message.
    pub fn earliest() -> MessageId {
        MessageId(wire::MessageId::earliest())
    }

    /// The position after every message.
    pub fn latest() -> MessageId {
        MessageId(wire::MessageId::latest())
    }

    /// The id's bytes, to store and restore with [`MessageId::from_bytes`].
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    /// Restores an id from [`MessageId::to_bytes`], or from bytes the Java client wrote.
    pub fn from_bytes(bytes: &[u8]) -> Result<MessageId> {
        wire::MessageId::from_bytes(bytes)
            .map(MessageId)
            .map_err(|e| Error::with_source(ErrorKind::InvalidConfiguration, "not a message id", e))
    }
}

impl fmt::Debug for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        fmt::Display::fmt(self, f)
    }
}

/// `segment:ledger:entry`, then `:batch-index` for a message in a batch.
impl fmt::Display for MessageId {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let entry = &self.0.entry;
        write!(f, "{}:{}:{}", self.0.segment_id, entry.ledger_id, entry.entry_id)?;
        if entry.batch_index >= 0 {
            write!(f, ":{}", entry.batch_index)?;
        }
        Ok(())
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn round_trips_and_orders() {
        let id = MessageId(wire::MessageId::new(
            2,
            wire::EntryId { ledger_id: 7, entry_id: 9, partition: -1, batch_index: 0, batch_size: 1 },
        ));
        assert_eq!(MessageId::from_bytes(&id.to_bytes()).unwrap(), id);
        assert_eq!(id.to_string(), "2:7:9:0");
        // As in Java, the sentinels have no segment (-1), so they sort before every segment's ids.
        assert!(MessageId::earliest() < MessageId::latest() && MessageId::latest() < id);
        assert_eq!(MessageId::from_bytes(b"short").unwrap_err().kind(), ErrorKind::InvalidConfiguration);
    }
}
