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

//! Checkpoints (docs/7-api.md §7).

use std::fmt;

use apache_pulsar_protocol::checkpoint as wire;

use crate::error::{Error, ErrorKind, Result};

/// A position across the segments of a topic, where a [`CheckpointConsumer`](crate::CheckpointConsumer) starts
/// reading. Opaque and serializable.
///
/// Its bytes are the Java V5 client's (`CheckpointV5`), so a checkpoint saved by one SDK can be restored by the other.
/// The application keeps them where it keeps its own state, and restores the consumer from them.
#[derive(Clone, PartialEq, Eq)]
pub struct Checkpoint(pub(crate) wire::Checkpoint);

impl Checkpoint {
    /// The start of every segment: everything the topic still has.
    pub fn earliest() -> Checkpoint {
        Checkpoint(wire::Checkpoint::Earliest)
    }

    /// The end of every segment: only the messages published from then on.
    pub fn latest() -> Checkpoint {
        Checkpoint(wire::Checkpoint::Latest)
    }

    /// The checkpoint's bytes, to store and restore with [`Checkpoint::from_bytes`].
    pub fn to_bytes(&self) -> Vec<u8> {
        self.0.to_bytes()
    }

    /// Restores a checkpoint from [`Checkpoint::to_bytes`], or from bytes the Java client wrote. Fails with
    /// [`InvalidConfiguration`](ErrorKind::InvalidConfiguration) for bytes that aren't a checkpoint.
    pub fn from_bytes(bytes: &[u8]) -> Result<Checkpoint> {
        wire::Checkpoint::from_bytes(bytes)
            .map(Checkpoint)
            .map_err(|e| Error::with_source(ErrorKind::InvalidConfiguration, "not a checkpoint", e))
    }
}

/// `earliest`, `latest`, or each segment's position: `{segment: ledger:entry, …}`, then `:batch-index` for a message
/// in a batch.
impl fmt::Debug for Checkpoint {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        let positions = match &self.0 {
            wire::Checkpoint::Earliest => return f.write_str("Checkpoint(earliest)"),
            wire::Checkpoint::Latest => return f.write_str("Checkpoint(latest)"),
            wire::Checkpoint::Positions(positions) => positions,
        };
        f.write_str("Checkpoint{")?;
        for (i, (segment, entry)) in positions.iter().enumerate() {
            let separator = if i == 0 { "" } else { ", " };
            write!(f, "{separator}{segment}: {}:{}", entry.ledger_id, entry.entry_id)?;
            if entry.batch_index >= 0 {
                write!(f, ":{}", entry.batch_index)?;
            }
        }
        f.write_str("}")
    }
}

#[cfg(test)]
mod tests {
    use apache_pulsar_protocol::message_id::EntryId;

    use super::*;

    #[test]
    fn round_trips_and_prints() {
        let at = |entry_id, batch_index| EntryId { ledger_id: 7, entry_id, partition: -1, batch_index, batch_size: 0 };
        let positions = [(0, at(9, -1)), (2, at(3, 1))].into_iter().collect();
        let checkpoint = Checkpoint(wire::Checkpoint::Positions(positions));
        assert_eq!(Checkpoint::from_bytes(&checkpoint.to_bytes()).unwrap(), checkpoint);
        assert_eq!(format!("{checkpoint:?}"), "Checkpoint{0: 7:9, 2: 7:3:1}");
        for (sentinel, printed) in
            [(Checkpoint::earliest(), "Checkpoint(earliest)"), (Checkpoint::latest(), "Checkpoint(latest)")]
        {
            assert_eq!(Checkpoint::from_bytes(&sentinel.to_bytes()).unwrap(), sentinel);
            assert_eq!(format!("{sentinel:?}"), printed);
        }
        let error = Checkpoint::from_bytes(&[9]).unwrap_err();
        assert_eq!(
            (error.kind(), error.to_string()),
            (ErrorKind::InvalidConfiguration, "InvalidConfiguration: not a checkpoint".into())
        );
    }
}
