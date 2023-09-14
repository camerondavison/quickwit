use std::num::NonZeroUsize;
use std::path::PathBuf;
use std::sync::{Arc, Mutex};

use tokio::sync::{Notify, OwnedSemaphorePermit, Semaphore};
use tracing::error;
use ulid::Ulid;

use crate::split_cache::split_table::{CandidateSplit, SplitTable};
use crate::StorageResolver;

struct DownloadOpportunity {
    // At this point, the split have already been removed from the split table.
    // The file however need to be deleted.
    splits_to_delete: Vec<Ulid>,
    split_to_download: CandidateSplit,
}

fn find_download_opportunity(split_table: &mut SplitTable) -> Option<DownloadOpportunity> {
    let best_candidate_split_key = split_table.best_candidate()?;
    let splits_to_delete: Vec<Ulid> =
        split_table.make_room_for_split(best_candidate_split_key.last_accessed)?;
    let split_to_download: CandidateSplit =
        split_table.start_download(best_candidate_split_key.split_ulid)?;
    Some(DownloadOpportunity {
        splits_to_delete,
        split_to_download,
    })
    // }
}
//     // split_cache_directory: &Path,
//     split_table: &mut SplitTable,
//     storage_resolver: &StorageResolver,
//     download_permit: OwnedSemaphorePermit) {
// }

async fn perform_download(
    download_opportunity: DownloadOpportunity,
    root_path: PathBuf,
    storage_resolver: StorageResolver,
    _download_permit: OwnedSemaphorePermit,
) -> anyhow::Result<()> {
    for split_to_delete in download_opportunity.splits_to_delete {
        let split_file_path = root_path.join(split_to_delete.to_string());
        if let Err(io_err) = std::fs::remove_file(split_file_path) {
            // This is an pretty critical error. The split size is not tracked anymore at this
            // point.
            error!("Failed to remove split file from cache directory.");
        }
    }
    // let storage = storage_resolver.resolve(&download_opportunity.split_to_download).await?;
    // storage.
    Ok(())
}

async fn spawn_download_task(
    root_path: PathBuf,
    shared_split_table: Arc<Mutex<SplitTable>>,
    storage_resolver: StorageResolver,
    num_concurrent_downloads: NonZeroUsize,
) -> Arc<Notify> {
    let notify = Arc::new(Notify::new());
    let notify_clone = notify.clone();
    let semaphore = Arc::new(Semaphore::new(num_concurrent_downloads.get()));
    let _ = tokio::task::spawn(async move {
        loop {
            // TODO Result
            let download_permit = Semaphore::acquire_owned(semaphore.clone()).await.unwrap();
            {
                // let mut split_table_lock = shared_split_table.lock().unwrap();
                if let Some(download_opportunity) =
                    find_download_opportunity(&mut shared_split_table.lock().unwrap())
                {
                    tokio::task::spawn(perform_download(
                        download_opportunity,
                        root_path.clone(),
                        storage_resolver.clone(),
                        download_permit,
                    ));
                    // tokio::task::spawn(async move {
                    //     perform_download(download_opportunity, &root_path_clone,
                    // storage_resolver_clone, download_permit) });
                }
                // spawn_download_task_if_any_opportunity(&mut *&mut split_table_lock,
                // &storage_resolver, download_permit);
            }
            notify.notified().await;
        }
    });
    notify_clone
}
