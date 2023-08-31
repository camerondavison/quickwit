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

use std::fs::File;
use std::io::{Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::Arc;

use async_trait::async_trait;
use tantivy::directory::OwnedBytes;
use ulid::Ulid;

use crate::split_cache::split_state_table::{SplitGuard, SplitStateTable};
use crate::StorageCache;

pub struct SplitCache {
    root_path: PathBuf,
    split_state_table: SplitStateTable,
}

struct LockFile {
    path: PathBuf,
    lock: Arc<()>,
}

impl SplitCache {
    pub fn new(root_path: PathBuf, split_state_table: SplitStateTable) -> SplitCache {
        SplitCache {
            root_path,
            split_state_table,
        }
    }
}

pub struct SplitFilepath {
    split_guard: SplitGuard,
    cached_split_file_path: PathBuf,
}

impl AsRef<Path> for SplitFilepath {
    fn as_ref(&self) -> &Path {
        &self.cached_split_file_path
    }
}

impl SplitCache {
    fn cached_split_filepath(&self, split_id: Ulid) -> PathBuf {
        let split_filename = quickwit_common::split_file(&split_id.to_string());
        self.root_path.join(split_filename)
    }
    // Returns a split guard object. As long as it is not dropped, the
    // split won't be evinced from the cache.
    fn get_split_guard(&self, split_id: Ulid) -> Option<SplitFilepath> {
        let split_guard = self.split_state_table.get_split_guard(split_id)?;
        Some(SplitFilepath {
            split_guard,
            cached_split_file_path: self.cached_split_filepath(split_id),
        })
    }

    // async fn acknowledge_split(split_uri: Uri) {
    //     todo!()
    // }
}

fn split_id_from_path(split_path: &Path) -> Option<Ulid> {
    let split_filename = split_path.file_name()?.to_str()?;
    let split_id_str = split_filename.strip_suffix(".split")?;
    Ulid::from_str(split_id_str).ok()
}

#[async_trait]
impl StorageCache for SplitCache {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_guard = self.get_split_guard(split_id)?;
        // We don't use async file io here because it spawn blocks anyway, and it feels dumb to
        // spawn block 3 times in a row.
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(&split_guard).ok()?;
            file.seek(SeekFrom::Start(byte_range.start as u64)).ok()?;
            let mut buf = Vec::with_capacity(byte_range.len());
            file.take(byte_range.len() as u64)
                .read_to_end(&mut buf)
                .ok()?;
            Some(OwnedBytes::new(buf))
        })
        .await
        .ok()?
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_guard = self.get_split_guard(split_id)?;
        // We don't use async file io here because it spawn blocks anyway, and it feels dumb to
        // spawn block 3 times in a row.
        tokio::task::spawn_blocking(move || {
            let mut file = File::open(&split_guard).ok()?;
            let mut buf = Vec::new();
            file.read_to_end(&mut buf).ok()?;
            Some(OwnedBytes::new(buf))
        })
        .await
        .ok()?
    }

    async fn put(&self, _path: PathBuf, _byte_range: Range<usize>, bytes: OwnedBytes) {}
    async fn put_all(&self, _path: PathBuf, _bytes: OwnedBytes) {}
}
