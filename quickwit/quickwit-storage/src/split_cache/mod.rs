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

// mod download_queue;
// mod split_state_table;
mod split_table;

use std::collections::HashMap;
use std::ffi::OsStr;
use std::fs::File;
use std::io::{self, Read, Seek, SeekFrom};
use std::ops::Range;
use std::path::{Path, PathBuf};
use std::str::FromStr;
use std::sync::{Arc, Mutex};

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use quickwit_config::SplitCacheLimits;
use quickwit_proto::search::ReportSplit;
use tantivy::directory::OwnedBytes;
use tracing::{error, warn};
use ulid::Ulid;

// use crate::split_cache::split_state_table::{SplitGuard, SplitStateTable};
use crate::split_cache::split_table::{SplitGuard, SplitTable};
use crate::{wrap_storage_with_cache, Storage, StorageCache};

struct SplitCacheBackingStorage {
    split_cache: Arc<SplitCache>,
    storage_root_uri: Uri,
}

pub struct SplitCache {
    root_path: PathBuf,
    split_table: Arc<Mutex<SplitTable>>,
}

impl SplitCache {
    pub fn wrap_storage(self_arc: Arc<Self>, storage: Arc<dyn Storage>) -> Arc<dyn Storage> {
        let cache = Arc::new(SplitCacheBackingStorage {
            split_cache: self_arc.clone(),
            storage_root_uri: storage.uri().clone(),
        });
        wrap_storage_with_cache(cache, storage)
    }

    pub fn with_root_path(root_path: PathBuf, limits: SplitCacheLimits) -> io::Result<SplitCache> {
        // todo load everything
        let mut available_files: HashMap<PathBuf, u64> = HashMap::default();
        for dir_entry_res in std::fs::read_dir(&root_path)? {
            let dir_entry = dir_entry_res?;
            let path = dir_entry.path();
            let meta = std::fs::metadata(&path)?;
            if meta.is_dir() {
                continue;
            }
            let ext = path.extension().and_then(OsStr::to_str).unwrap_or("");
            match ext {
                "temp" => {
                    if let Err(io_err) = std::fs::remove_file(&path) {
                        if io_err.kind() != io::ErrorKind::NotFound {
                            error!(path=?path, "Failed to remove temporary file.");
                        }
                    }
                }
                "split" => {
                    available_files.insert(path, meta.len());
                }

                _ => {
                    warn!(path=?path, "Unknown file in split cache directory. Ignoring.");
                }
            }
        }
        let split_table = SplitTable::with_limits(limits);
        let split_cache = SplitCache {
            root_path,
            split_table: Arc::new(Mutex::new(split_table)),
        };
        Ok(split_cache)
    }

    pub fn report_splits(&self, report_splits: Vec<ReportSplit>) {
        let mut split_table = self.split_table.lock().unwrap();
        for report_split in report_splits {
            match Uri::from_str(&report_split.split_uri) {
                Ok(split_uri) => {
                    split_table.report(split_uri);
                }
                Err(uri_err) => {
                    error!("Invalid split uri");
                }
            }
        }
    }
}

fn ulid_from_split_uri(split_uri: &str) -> Option<Ulid> {
    let (_, split_filename) = split_uri.rsplit_once("/")?;
    let split_name_str = split_filename.strip_suffix(".split")?;
    Ulid::from_str(split_name_str).ok()
}

pub struct SplitFilepath {
    _split_guard: SplitGuard,
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
    fn get_split_guard(&self, split_id: Ulid, storage_uri: &Uri) -> Option<SplitFilepath> {
        let split_guard = self
            .split_table
            .lock()
            .unwrap()
            .get_split_guard(split_id, storage_uri)?;
        Some(SplitFilepath {
            _split_guard: split_guard,
            cached_split_file_path: self.cached_split_filepath(split_id),
        })
    }
}

fn split_id_from_path(split_path: &Path) -> Option<Ulid> {
    let split_filename = split_path.file_name()?.to_str()?;
    let split_id_str = split_filename.strip_suffix(".split")?;
    Ulid::from_str(split_id_str).ok()
}

#[async_trait]
impl StorageCache for SplitCacheBackingStorage {
    async fn get(&self, path: &Path, byte_range: Range<usize>) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_guard = self
            .split_cache
            .get_split_guard(split_id, &self.storage_root_uri)?;
        // TODO touch file in cache.
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
        // TODO Remove file from cache if io error?
        .ok()?
    }

    async fn get_all(&self, path: &Path) -> Option<OwnedBytes> {
        let split_id = split_id_from_path(path)?;
        let split_guard = self
            .split_cache
            .get_split_guard(split_id, &self.storage_root_uri)?;
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

    async fn put(&self, _path: PathBuf, _byte_range: Range<usize>, _bytes: OwnedBytes) {}
    async fn put_all(&self, _path: PathBuf, _bytes: OwnedBytes) {}
}
