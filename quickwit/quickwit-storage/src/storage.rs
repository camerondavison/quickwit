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

use std::fmt;
use std::ops::Range;
use std::path::Path;

use async_trait::async_trait;
use quickwit_common::uri::Uri;
use tokio::io::AsyncWrite;

use crate::{BulkDeleteError, OwnedBytes, PutPayload, StorageErrorKind, StorageResult};

/// This trait is only used to make it build trait object with `AsyncWrite + Send + Unpin`.
pub trait SendableAsync: AsyncWrite + Send + Unpin {}
impl<W: AsyncWrite + Send + Unpin> SendableAsync for W {}

/// Storage meant to receive and serve quickwit's split.
///
/// Object storage are the primary target implementation of this trait,
/// and its interface is meant to allow for multipart download/upload.
///
/// Note that Storage does not have the notion of directory separators.
/// For underlying implementation where directory separator have meaning,
/// The implementation should treat directory separators as exactly the same way
/// object storage treat them. This means when directory separators a present
/// in the storage operation path, the storage implementation should create and remove transparently
/// these intermediate directories.
#[cfg_attr(any(test, feature = "testsuite"), mockall::automock)]
#[async_trait]
pub trait Storage: fmt::Debug + Send + Sync + 'static {
    /// Check storage connection if applicable
    async fn check_connectivity(&self) -> anyhow::Result<()>;

    /// Saves a file into the storage.
    async fn put(&self, path: &Path, payload: Box<dyn PutPayload>) -> StorageResult<()>;

    /// Copies the file associated to `Path` into an `AsyncWrite`.
    /// This function is required to call `.flush()` before it successfully returns.
    ///
    /// See also `copy_to_file`.
    ///
    /// async_trait Expansion of
    /// async fn copy_to(&self, path: &Path, output: &mut dyn SendableAsync) -> StorageResult<()>;
    ///
    /// Just putting the async form is breaking mockall.
    fn copy_to<'life0, 'life1, 'life2, 'async_trait>(
        &'life0 self,
        path: &'life1 Path,
        output: &'life2 mut dyn SendableAsync,
    ) -> ::core::pin::Pin<
        Box<
            dyn ::core::future::Future<Output = StorageResult<()>>
                + ::core::marker::Send
                + 'async_trait,
        >,
    >
    where
        'life0: 'async_trait,
        'life1: 'async_trait,
        'life2: 'async_trait,
        Self: 'async_trait;

    /// Downloads an entire file and writes it into a local file.
    /// `output_path` is expected to be a file path (not a directory path)
    /// without any existing file yet.
    ///
    /// If the call is successful, the file will be created.
    /// If not, the file may or may not have been created.
    ///
    /// See also `copy_to`.
    async fn copy_to_file(&self, path: &Path, output_path: &Path) -> StorageResult<()> {
        let mut file = tokio::fs::File::create(output_path).await?;
        self.copy_to(path, &mut file).await?;
        Ok(())
    }

    /// Downloads a slice of a file from the storage, and returns an in memory buffer
    async fn get_slice(&self, path: &Path, range: Range<usize>) -> StorageResult<OwnedBytes>;

    /// Downloads the entire content of a "small" file, returns an in memory buffer.
    /// For large files prefer `copy_to_file`.
    async fn get_all(&self, path: &Path) -> StorageResult<OwnedBytes>;

    /// Deletes a file.
    ///
    /// This method should return Ok(()) if the file did not exist.
    async fn delete(&self, path: &Path) -> StorageResult<()>;

    /// Deletes multiple files at once.
    ///
    /// The implementation may call `[`Storage::delete`] in a loop if the underlying storage does
    /// not support deleting objects in bulk. The request can fail partially, i.e. some objects are
    /// successfully deleted while others are not.
    async fn bulk_delete<'a>(&self, paths: &[&'a Path]) -> Result<(), BulkDeleteError>;

    /// Returns whether a file exists or not.
    async fn exists(&self, path: &Path) -> StorageResult<bool> {
        match self.file_num_bytes(path).await {
            Ok(_) => Ok(true),
            Err(storage_err) if storage_err.kind() == StorageErrorKind::NotFound => Ok(false),
            Err(other_storage_err) => Err(other_storage_err),
        }
    }

    /// Returns a file size.
    async fn file_num_bytes(&self, path: &Path) -> StorageResult<u64>;

    /// Returns an URI identifying the storage
    fn uri(&self) -> &Uri;
}
