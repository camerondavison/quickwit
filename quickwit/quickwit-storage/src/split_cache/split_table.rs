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

use crate::split_cache::ulid_from_split_uri;

type LastModifiedDate = u64;

#[derive(Clone, Copy)]
struct SplitKey {
    last_accessed: LastModifiedDate,
    split_ulid: Ulid,
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
    Candidate { uri: Uri },
    Downloading,
    OnDisk { num_bytes: u64 },
}

pub struct SplitInfo {
    split_key: SplitKey,
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

fn compute_timestamp(start: Instant) -> LastModifiedDate {
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
            Status::OnDisk { .. } => &mut self.on_disk_splits,
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
                let uri = split_uri(storage_uri, split_ulid);
                SplitInfo {
                    split_key: SplitKey {
                        split_ulid,
                        last_accessed: timestamp,
                    },
                    status: Status::Candidate { uri },
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
                    status: Status::Downloading,
                }
            }
        });
    }

    pub(crate) fn report(&mut self, split_uri: Uri) {
        let Some(split_ulid) = ulid_from_split_uri(split_uri.as_str()) else {
            return;
        };
        self.mutate_split(split_ulid, move |split_info_opt| {
            if let Some(split_info) = split_info_opt {
                return split_info;
            }
            SplitInfo {
                split_key: SplitKey {
                    last_accessed: 0u64,
                    split_ulid,
                },
                status: Status::Candidate { uri: split_uri },
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

    pub(crate) fn start_download(&mut self, split_ulid: Ulid) {
        self.change_split_status(split_ulid, Status::Downloading);
    }

    fn peek_best_candidate(&self) -> Option<&SplitInfo> {
        let split_key = self.candidate_splits.last()?;
        self.split_to_status.get(&split_key.split_ulid)
    }

    fn peek_lowest_on_disk(&self) -> Option<&SplitInfo> {
        let split_key = self.on_disk_splits.first()?;
        self.split_to_status.get(&split_key.split_ulid)
    }
}
