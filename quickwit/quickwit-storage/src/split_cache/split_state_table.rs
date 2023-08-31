// Copyright (C) 2023 Quickwit, Inc.
//
// Quickwit is offered under the AGPL v3.0 and as commercial software.
// For commercial licensing, contact us at hello@quickwit.io.
//
// AGPL:
// This program is free software: you can redistribute it and/or modify
// it under the terms of the GNU Affero General Public License as
// published by the Free Software Foundation, either version 3 of the
// License, or (at your option) any later version.
//
// This program is distributed in the hope that it will be useful,
// but WITHOUT ANY WARRANTY; without even the implied warranty of
// MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE. See the
// GNU Affero General Public License for more details.
//
// You should have received a copy of the GNU Affero General Public License
// along with this program. If not, see <http://www.gnu.org/licenses/>.

use std::collections::HashMap;
use std::num::NonZeroU32;
use std::sync::atomic::{AtomicU32, Ordering};
use std::sync::Arc;

use ulid::Ulid;

#[repr(u8)]
#[derive(Clone, Copy, PartialEq, Eq)]
enum SplitState {
    Unknown = 0,
    // The file is currently being download. New download should not be initiated.
    Downloading = 1,
    // The file is downloaded and available for readers.
    Downloaded = 2,
    // A client is using this file. It cannot be candidate for eviction. It is available for other
    // readers.
    Protected { num_readers: NonZeroU32 },
}

impl SplitState {
    pub fn from_code(code: u32) -> SplitState {
        match code {
            0 => SplitState::Unknown,
            1 => SplitState::Downloading,
            2 => SplitState::Downloaded,
            n => SplitState::Protected {
                num_readers: NonZeroU32::new(n - 2).unwrap(),
            },
        }
    }

    pub fn to_code(self) -> u32 {
        match self {
            SplitState::Unknown => 0u32,
            SplitState::Downloading => 1u32,
            SplitState::Downloaded => 2u32,
            SplitState::Protected { num_readers } => num_readers.get() + 2u32,
        }
    }
}

pub(crate) struct DownloadToken(Option<Arc<AtomicU32>>);

impl Drop for DownloadToken {
    fn drop(&mut self) {
        if let Some(state) = self.0.take() {
            // 0 is mapped to None.
            state.store(0, Ordering::SeqCst);
        }
    }
}

impl DownloadToken {
    pub(crate) fn mark_download_as_successful(mut self) {
        if let Some(state) = self.0.take() {
            state.store(SplitState::Downloaded.to_code(), Ordering::SeqCst);
        }
    }
}

#[derive(Default)]
pub struct SplitStateTable {
    split_state_table: HashMap<Ulid, Arc<AtomicU32>>,
}

impl SplitStateTable {
    fn get_state(&self, split_id: Ulid) -> SplitState {
        let Some(split_code) = self.split_state_table.get(&split_id) else {
            return SplitState::Unknown;
        };
        SplitState::from_code(split_code.load(Ordering::SeqCst))
    }

    fn set_state(&mut self, split_id: Ulid, split_state: SplitState) {
        let split_state_code = split_state.to_code();
        let code = Arc::new(AtomicU32::new(split_state_code));
        self.split_state_table.insert(split_id, code);
    }

    pub(crate) fn start_download(&mut self, split_id: Ulid) -> Option<DownloadToken> {
        let split_state = self.get_state(split_id);
        // We are either already downloading the split or it has been downloaded.
        if split_state != SplitState::Unknown {
            return None;
        }
        let split_state = Arc::new(AtomicU32::new(SplitState::Downloading.to_code()));
        let download_token = DownloadToken(Some(split_state.clone()));
        self.split_state_table.insert(split_id, split_state);
        Some(download_token)
    }

    pub(crate) fn get_split_guard(&self, split_id: Ulid) -> Option<SplitGuard> {
        let Some(split_code) = self.split_state_table.get(&split_id) else {
            return None;
        };
        let split_state = SplitState::from_code(split_code.load(Ordering::SeqCst));
        match split_state {
            SplitState::Unknown | SplitState::Downloading => {
                return None;
            }
            SplitState::Downloaded | SplitState::Protected { .. } => {
                split_code.fetch_add(1, Ordering::SeqCst);
                return Some(SplitGuard {
                    split_code: split_code.clone(),
                });
            }
        }
    }
}

pub(crate) struct SplitGuard {
    split_code: Arc<AtomicU32>,
}

impl Drop for SplitGuard {
    fn drop(&mut self) {
        self.split_code.fetch_sub(1, Ordering::SeqCst);
    }
}

#[cfg(test)]
mod tests {
    use ulid::Ulid;

    use crate::split_cache::split_state_table::SplitStateTable;

    #[test]
    fn test_split_table_happy_path() {
        let my_split_id = Ulid::new();
        let mut split_state_table = SplitStateTable::default();
        let token_opt = split_state_table.start_download(my_split_id);
        assert!(token_opt.is_some());
        assert!(split_state_table.start_download(my_split_id).is_none());
        let token = token_opt.unwrap();
        assert!(split_state_table.get_split_guard(my_split_id).is_none());
        token.mark_download_as_successful();
        assert!(split_state_table.get_split_guard(my_split_id).is_some());
        assert!(split_state_table.start_download(my_split_id).is_none());
    }

    #[test]
    fn test_split_table_dropped_download_token() {
        let my_split_id = Ulid::new();
        let mut split_state_table = SplitStateTable::default();
        let token_opt = split_state_table.start_download(my_split_id);
        assert!(token_opt.is_some());
        let redundant_token_opt = split_state_table.start_download(my_split_id);
        assert!(redundant_token_opt.is_none());
        let token = token_opt.unwrap();
        assert!(split_state_table.get_split_guard(my_split_id).is_none());
        drop(token);
        assert!(split_state_table.start_download(my_split_id).is_some());
        assert!(split_state_table.get_split_guard(my_split_id).is_none());
    }
}
