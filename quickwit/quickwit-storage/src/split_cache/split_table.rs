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

use std::cmp::Ordering;
use std::collections::{BTreeSet, HashMap};
use std::time::Instant;

use quickwit_common::split_file;
use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use ulid::Ulid;

type LastAccessDate = u64;

#[derive(Clone, Copy)]
pub(crate) struct SplitKey {
    pub last_accessed: LastAccessDate,
    pub split_ulid: Ulid,
}

impl PartialOrd for SplitKey {
    fn partial_cmp(&self, other: &Self) -> Option<Ordering> {
        Some(self.cmp(other))
    }
}

impl Ord for SplitKey {
    fn cmp(&self, other: &Self) -> Ordering {
        (self.last_accessed, &self.split_ulid).cmp(&(other.last_accessed, &other.split_ulid))
    }
}

impl PartialEq for SplitKey {
    fn eq(&self, other: &Self) -> bool {
        (self.last_accessed, &self.split_ulid) == (other.last_accessed, &other.split_ulid)
    }
}

impl Eq for SplitKey {}

#[derive(Clone, Debug, Eq, PartialEq)]
enum Status {
    Candidate(CandidateSplit),
    Downloading,
    OnDisk { num_bytes: u64 },
}

pub struct SplitInfo {
    pub(crate) split_key: SplitKey,
    status: Status,
}

/// The split table keeps track of splits we know about (regardless of whether they have already
/// been downloaded or not).
pub struct SplitTable {
    on_disk_splits: BTreeSet<SplitKey>,
    downloading_split: BTreeSet<SplitKey>,
    candidate_splits: BTreeSet<SplitKey>,
    split_to_status: HashMap<Ulid, SplitInfo>,
    start_time: Instant,
    limits: SplitCacheLimits,
    on_disk_bytes: u64,
}

impl SplitTable {
    pub(crate) fn with_limits(limits: SplitCacheLimits) -> SplitTable {
        SplitTable {
            on_disk_splits: BTreeSet::default(),
            candidate_splits: BTreeSet::default(),
            downloading_split: BTreeSet::default(),
            split_to_status: HashMap::default(),
            start_time: Instant::now(),
            limits,
            on_disk_bytes: 0,
        }
    }
}

fn compute_timestamp(start: Instant) -> LastAccessDate {
    start.elapsed().as_micros() as u64
}

fn split_uri(root_uri: &Uri, split_ulid: Ulid) -> Uri {
    let split_filename: String = split_file(&split_ulid.to_string());
    root_uri.join(&split_filename).unwrap()
}

// TODO improve SplitGuard with Atomic
// Right only touch is helping.
pub(super) struct SplitGuard;

impl SplitTable {
    pub(super) fn get_split_guard(
        &mut self,
        split_ulid: Ulid,
        storage_uri: &Uri,
    ) -> Option<SplitGuard> {
        if let Status::OnDisk { .. } = self.touch(split_ulid, storage_uri) {
            Some(SplitGuard)
        } else {
            None
        }
    }

    fn remove(&mut self, split_ulid: Ulid) -> Option<SplitInfo> {
        let split_info = self.split_to_status.remove(&split_ulid)?;
        let split_queue: &mut BTreeSet<SplitKey> = match split_info.status {
            Status::Candidate { .. } => &mut self.candidate_splits,
            Status::Downloading => &mut self.downloading_split,
            Status::OnDisk { num_bytes } => {
                self.on_disk_bytes -= num_bytes;
                &mut self.on_disk_splits
            }
        };
        let is_in_queue = split_queue.remove(&split_info.split_key);
        assert!(is_in_queue);
        Some(split_info)
    }

    /// Insert a `split_info`. This methods assumes the split was not present in the split table
    /// to begin with. It will panic if the split was already present.
    ///
    /// Keep this method private.
    fn insert(&mut self, split_info: SplitInfo) {
        let was_not_in_queue = match split_info.status {
            Status::Candidate { .. } => self.candidate_splits.insert(split_info.split_key),
            Status::Downloading => self.downloading_split.insert(split_info.split_key),
            Status::OnDisk { num_bytes } => {
                self.on_disk_bytes += num_bytes;
                self.on_disk_splits.insert(split_info.split_key)
            }
        };
        assert!(was_not_in_queue);
        let split_ulid_was_absent = self
            .split_to_status
            .insert(split_info.split_key.split_ulid, split_info)
            .is_none();
        assert!(split_ulid_was_absent);
    }

    fn touch(&mut self, split_ulid: Ulid, storage_uri: &Uri) -> Status {
        let timestamp = compute_timestamp(self.start_time);
        self.mutate_split(split_ulid, |old_split_info| {
            if let Some(mut split_info) = old_split_info {
                split_info.split_key.last_accessed = timestamp;
                split_info
            } else {
                SplitInfo {
                    split_key: SplitKey {
                        split_ulid,
                        last_accessed: timestamp,
                    },
                    status: Status::Candidate(CandidateSplit {
                        storage_uri: storage_uri.clone(),
                        split_ulid,
                    }),
                }
            }
        })
    }

    fn mutate_split(
        &mut self,
        split_ulid: Ulid,
        mutate_fn: impl FnOnce(Option<SplitInfo>) -> SplitInfo,
    ) -> Status {
        let split_info_opt = self.remove(split_ulid);
        let new_split: SplitInfo = mutate_fn(split_info_opt);
        let new_status = new_split.status.clone();
        self.insert(new_split);
        new_status
    }

    fn change_split_status(&mut self, split_ulid: Ulid, status: Status) {
        let start_time = self.start_time;
        self.mutate_split(split_ulid, move |split_info_opt| {
            if let Some(mut split_info) = split_info_opt {
                split_info.status = status;
                split_info
            } else {
                SplitInfo {
                    split_key: SplitKey {
                        last_accessed: compute_timestamp(start_time),
                        split_ulid,
                    },
                    status,
                }
            }
        });
    }

    pub(crate) fn report(&mut self, storage_uri: Uri, split_ulid: Ulid) {
        self.mutate_split(split_ulid, move |split_info_opt| {
            if let Some(split_info) = split_info_opt {
                return split_info;
            }
            SplitInfo {
                split_key: SplitKey {
                    last_accessed: 0u64,
                    split_ulid,
                },
                status: Status::Candidate(CandidateSplit {
                    storage_uri,
                    split_ulid,
                }),
            }
        });
    }

    pub(crate) fn register_as_downloaded(&mut self, split_ulid: Ulid, num_bytes: u64) {
        self.change_split_status(
            split_ulid,
            Status::OnDisk {
                num_bytes: num_bytes,
            },
        );
    }

    /// Change the state of the given split from candidate to downloading state,
    /// and returns its URI.
    ///
    /// This function does NOT trigger the download itself. It is up to
    /// the caller to actually initiate the download.
    pub(crate) fn start_download(&mut self, split_ulid: Ulid) -> Option<CandidateSplit> {
        let mut split_info = self.remove(split_ulid)?;
        if let Status::Candidate(candidate_split) = split_info.status {
            Some(candidate_split)
        } else {
            self.insert(split_info);
            None
        }
    }

    pub(crate) fn best_candidate(&self) -> Option<SplitKey> {
        self.candidate_splits.last().copied()
    }

    fn is_out_of_limits(&self) -> bool {
        if self.candidate_splits.is_empty() {
            return false;
        }
        if self.on_disk_splits.len() + self.downloading_split.len()
            > self.limits.max_num_splits as usize
        {
            return true;
        }
        if self.on_disk_bytes > self.limits.max_num_bytes.get_bytes() {
            return true;
        }
        false
    }

    /// Evicts splits to reach the target limits.
    ///
    /// Returns false if the first candidate for eviction is
    /// fresher that the candidate split. (Note this is suboptimal.
    ///
    /// Returns None
    pub fn make_room_for_split(&mut self, last_access_date: LastAccessDate) -> Option<Vec<Ulid>> {
        let mut split_infos = Vec::new();
        while self.is_out_of_limits() {
            if let Some(first_split) = self.on_disk_splits.first() {
                if first_split.last_accessed > last_access_date {
                    // This is not worth doing the eviction.
                    break;
                }
                split_infos.extend(self.remove(first_split.split_ulid));
            } else {
                break;
            }
        }
        // We are still out of limits.
        // Let's not go through with the eviction, and reinsert the splits.
        if self.is_out_of_limits() {
            for split_info in split_infos {
                self.insert(split_info);
            }
            None
        } else {
            Some(
                split_infos
                    .into_iter()
                    .map(|split_info| split_info.split_key.split_ulid)
                    .collect(),
            )
        }
    }

    fn evict_one(&mut self) -> Option<SplitInfo> {
        let split_first = self.on_disk_splits.first().copied()?;
        let split_ulid = split_first.split_ulid;
        self.remove(split_first.split_ulid)
    }
}

#[derive(Clone, Debug, Eq, PartialEq)]
pub(crate) struct CandidateSplit {
    storage_uri: Uri,
    split_ulid: Ulid,
}
