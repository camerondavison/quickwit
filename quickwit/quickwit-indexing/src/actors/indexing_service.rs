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

use std::collections::{HashMap, HashSet};
use std::fmt::Debug;
use std::path::PathBuf;
use std::sync::Arc;

use anyhow::Context;
use async_trait::async_trait;
use futures::future::try_join_all;
use itertools::Itertools;
use quickwit_actors::{
    Actor, ActorContext, ActorExitStatus, ActorHandle, ActorState, Handler, Healthz, Mailbox,
    Observation,
};
use quickwit_cluster::Cluster;
use quickwit_common::fs::get_cache_directory_path;
use quickwit_common::temp_dir;
use quickwit_config::{
    build_doc_mapper, IndexConfig, IndexerConfig, SourceConfig, INGEST_API_SOURCE_ID,
};
use quickwit_ingest::{DropQueueRequest, IngestApiService, ListQueuesRequest, QUEUES_DIR_NAME};
use quickwit_metastore::{IndexMetadata, ListIndexesQuery, Metastore};
use quickwit_proto::indexing::{
    ApplyIndexingPlanRequest, ApplyIndexingPlanResponse, IndexingError, IndexingPipelineId,
    IndexingTask,
};
use quickwit_proto::IndexUid;
use quickwit_storage::StorageResolver;
use serde::{Deserialize, Serialize};
use tokio::sync::Semaphore;
use tracing::{debug, error, info, warn};

use super::merge_pipeline::{MergePipeline, MergePipelineParams};
use super::MergePlanner;
use crate::models::{DetachIndexingPipeline, DetachMergePipeline, ObservePipeline, SpawnPipeline};
use crate::split_store::{LocalSplitStore, SplitStoreQuota};
use crate::{IndexingPipeline, IndexingPipelineParams, IndexingSplitStore, IndexingStatistics};

/// Name of the indexing directory, usually located at `<data_dir_path>/indexing`.
pub const INDEXING_DIR_NAME: &str = "indexing";

#[derive(Clone, Debug, Default, Eq, PartialEq, Serialize, Deserialize)]
pub struct IndexingServiceCounters {
    pub num_running_pipelines: usize,
    pub num_successful_pipelines: usize,
    pub num_failed_pipelines: usize,
    pub num_running_merge_pipelines: usize,
    pub num_deleted_queues: usize,
    pub num_delete_queue_failures: usize,
}

#[derive(Debug, Clone, Hash, Eq, PartialEq)]
pub struct MergePipelineId {
    index_uid: IndexUid,
    source_id: String,
}

impl<'a> From<&'a IndexingPipelineId> for MergePipelineId {
    fn from(pipeline_id: &'a IndexingPipelineId) -> Self {
        MergePipelineId {
            index_uid: pipeline_id.index_uid.clone(),
            source_id: pipeline_id.source_id.clone(),
        }
    }
}

struct MergePipelineHandle {
    mailbox: Mailbox<MergePlanner>,
    handle: ActorHandle<MergePipeline>,
}

pub struct IndexingService {
    node_id: String,
    indexing_root_directory: PathBuf,
    queue_dir_path: PathBuf,
    cluster: Cluster,
    metastore: Arc<dyn Metastore>,
    ingest_api_service_opt: Option<Mailbox<IngestApiService>>,
    storage_resolver: StorageResolver,
    indexing_pipeline_handles: HashMap<IndexingPipelineId, ActorHandle<IndexingPipeline>>,
    counters: IndexingServiceCounters,
    local_split_store: Arc<LocalSplitStore>,
    max_concurrent_split_uploads: usize,
    merge_pipeline_handles: HashMap<MergePipelineId, MergePipelineHandle>,
    cooperative_indexing_permits: Option<Arc<Semaphore>>,
}

impl Debug for IndexingService {
    fn fmt(&self, formatter: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        formatter
            .debug_struct("IndexingService")
            .field("cluster_id", &self.cluster.cluster_id())
            .field("self_node_id", &self.node_id)
            .field("indexing_root_directory", &self.indexing_root_directory)
            .finish()
    }
}

impl IndexingService {
    #[allow(clippy::too_many_arguments)]
    pub async fn new(
        node_id: String,
        data_dir_path: PathBuf,
        indexer_config: IndexerConfig,
        num_blocking_threads: usize,
        cluster: Cluster,
        metastore: Arc<dyn Metastore>,
        ingest_api_service_opt: Option<Mailbox<IngestApiService>>,
        storage_resolver: StorageResolver,
    ) -> anyhow::Result<IndexingService> {
        let split_store_space_quota = SplitStoreQuota::new(
            indexer_config.split_store_max_num_splits,
            indexer_config.split_store_max_num_bytes,
        );
        let split_cache_dir_path = get_cache_directory_path(&data_dir_path);
        let local_split_store =
            LocalSplitStore::open(split_cache_dir_path, split_store_space_quota).await?;
        let indexing_root_directory =
            temp_dir::create_or_purge_directory(&data_dir_path.join(INDEXING_DIR_NAME)).await?;
        let queue_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let cooperative_indexing_permits = if indexer_config.enable_cooperative_indexing {
            Some(Arc::new(Semaphore::new(num_blocking_threads)))
        } else {
            None
        };
        Ok(Self {
            node_id,
            indexing_root_directory,
            queue_dir_path,
            cluster,
            metastore,
            ingest_api_service_opt,
            storage_resolver,
            local_split_store: Arc::new(local_split_store),
            indexing_pipeline_handles: Default::default(),
            counters: Default::default(),
            max_concurrent_split_uploads: indexer_config.max_concurrent_split_uploads,
            merge_pipeline_handles: HashMap::new(),
            cooperative_indexing_permits,
        })
    }

    async fn detach_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> Result<ActorHandle<IndexingPipeline>, IndexingError> {
        let pipeline_handle = self
            .indexing_pipeline_handles
            .remove(pipeline_id)
            .ok_or_else(|| IndexingError::MissingPipeline {
                index_id: pipeline_id.index_uid.index_id().to_string(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        self.counters.num_running_pipelines -= 1;
        Ok(pipeline_handle)
    }

    async fn detach_merge_pipeline(
        &mut self,
        pipeline_id: &MergePipelineId,
    ) -> Result<ActorHandle<MergePipeline>, IndexingError> {
        let pipeline_handle = self
            .merge_pipeline_handles
            .remove(pipeline_id)
            .ok_or_else(|| IndexingError::MissingPipeline {
                index_id: pipeline_id.index_uid.index_id().to_string(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        self.counters.num_running_merge_pipelines -= 1;
        Ok(pipeline_handle.handle)
    }

    async fn observe_pipeline(
        &mut self,
        pipeline_id: &IndexingPipelineId,
    ) -> Result<Observation<IndexingStatistics>, IndexingError> {
        let pipeline_handle = self
            .indexing_pipeline_handles
            .get(pipeline_id)
            .ok_or_else(|| IndexingError::MissingPipeline {
                index_id: pipeline_id.index_uid.index_id().to_string(),
                source_id: pipeline_id.source_id.clone(),
            })?;
        let observation = pipeline_handle.observe().await;
        Ok(observation)
    }

    async fn spawn_pipeline(
        &mut self,
        ctx: &ActorContext<Self>,
        index_id: String,
        source_config: SourceConfig,
        pipeline_ord: usize,
    ) -> Result<IndexingPipelineId, IndexingError> {
        let index_metadata = self.index_metadata(ctx, &index_id).await?;
        let pipeline_id = IndexingPipelineId {
            index_uid: index_metadata.index_uid.clone(),
            source_id: source_config.source_id.clone(),
            node_id: self.node_id.clone(),
            pipeline_ord,
        };
        let index_config = index_metadata.into_index_config();
        self.spawn_pipeline_inner(ctx, pipeline_id.clone(), index_config, source_config)
            .await?;
        Ok(pipeline_id)
    }

    async fn spawn_pipeline_inner(
        &mut self,
        ctx: &ActorContext<Self>,
        pipeline_id: IndexingPipelineId,
        index_config: IndexConfig,
        source_config: SourceConfig,
    ) -> Result<(), IndexingError> {
        if self.indexing_pipeline_handles.contains_key(&pipeline_id) {
            return Err(IndexingError::PipelineAlreadyExists {
                index_id: pipeline_id.index_uid.index_id().to_string(),
                source_id: pipeline_id.source_id,
                pipeline_ord: pipeline_id.pipeline_ord,
            });
        }
        let indexing_directory = temp_dir::Builder::default()
            .join(pipeline_id.index_uid.index_id())
            .join(pipeline_id.index_uid.incarnation_id())
            .join(&pipeline_id.source_id)
            .join(&pipeline_id.pipeline_ord.to_string())
            .tempdir_in(&self.indexing_root_directory)
            .map_err(IndexingError::Io)?;
        let storage = self
            .storage_resolver
            .resolve(&index_config.index_uri)
            .await
            .map_err(|err| IndexingError::StorageResolverError(err.to_string()))?;
        let merge_policy =
            crate::merge_policy::merge_policy_from_settings(&index_config.indexing_settings);
        let split_store = IndexingSplitStore::new(storage.clone(), self.local_split_store.clone());

        let doc_mapper = build_doc_mapper(&index_config.doc_mapping, &index_config.search_settings)
            .map_err(IndexingError::InvalidParams)?;

        let merge_pipeline_params = MergePipelineParams {
            pipeline_id: pipeline_id.clone(),
            doc_mapper: doc_mapper.clone(),
            indexing_directory: indexing_directory.clone(),
            metastore: self.metastore.clone(),
            split_store: split_store.clone(),
            merge_policy: merge_policy.clone(),
            merge_max_io_num_bytes_per_sec: index_config
                .indexing_settings
                .resources
                .max_merge_write_throughput,
            max_concurrent_split_uploads: self.max_concurrent_split_uploads,
        };

        let merge_planner_mailbox = self
            .get_or_create_merge_pipeline(merge_pipeline_params, ctx)
            .await?;

        // The concurrent uploads budget is split in 2: 1/2 for the indexing pipeline, 1/2 for the
        // merge pipeline.
        let max_concurrent_split_uploads_index = (self.max_concurrent_split_uploads / 2).max(1);
        let max_concurrent_split_uploads_merge =
            (self.max_concurrent_split_uploads - max_concurrent_split_uploads_index).max(1);
        let pipeline_params = IndexingPipelineParams {
            pipeline_id: pipeline_id.clone(),
            doc_mapper,
            indexing_settings: index_config.indexing_settings.clone(),
            source_config,
            indexing_directory,
            metastore: self.metastore.clone(),
            storage,
            split_store,
            merge_policy,
            max_concurrent_split_uploads_index,
            max_concurrent_split_uploads_merge,
            queues_dir_path: self.queue_dir_path.clone(),
            cooperative_indexing_permits: self.cooperative_indexing_permits.clone(),
            merge_planner_mailbox,
        };
        let pipeline = IndexingPipeline::new(pipeline_params);
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(pipeline);
        self.indexing_pipeline_handles
            .insert(pipeline_id, pipeline_handle);
        self.counters.num_running_pipelines += 1;
        Ok(())
    }

    async fn index_metadata(
        &self,
        ctx: &ActorContext<Self>,
        index_id: &str,
    ) -> Result<IndexMetadata, IndexingError> {
        let _protect_guard = ctx.protect_zone();
        let index_metadata = self
            .metastore
            .index_metadata(index_id)
            .await
            .map_err(|err| IndexingError::MetastoreError(err.to_string()))?;
        Ok(index_metadata)
    }

    async fn handle_supervise(&mut self) -> Result<(), ActorExitStatus> {
        self.indexing_pipeline_handles
            .retain(
                |pipeline_id, pipeline_handle| match pipeline_handle.state() {
                    ActorState::Idle | ActorState::Paused | ActorState::Processing => true,
                    ActorState::Success => {
                        info!(
                            index_id=%pipeline_id.index_uid.index_id(),
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline exited successfully."
                        );
                        self.counters.num_successful_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                    ActorState::Failure => {
                        // This should never happen: Indexing Pipelines are not supposed to fail,
                        // and are themselves in charge of supervising the pipeline actors.
                        error!(
                            index_id=%pipeline_id.index_uid.index_id(),
                            source_id=%pipeline_id.source_id,
                            pipeline_ord=%pipeline_id.pipeline_ord,
                            "Indexing pipeline exited with failure. This should never happen."
                        );
                        self.counters.num_failed_pipelines += 1;
                        self.counters.num_running_pipelines -= 1;
                        false
                    }
                },
            );
        // Evict and kill merge pipelines that are not needed.
        let needed_merge_pipeline_ids: HashSet<MergePipelineId> = self
            .indexing_pipeline_handles
            .keys()
            .map(MergePipelineId::from)
            .collect();
        let current_merge_pipeline_ids: HashSet<MergePipelineId> =
            self.merge_pipeline_handles.keys().cloned().collect();
        for merge_pipeline_id_to_shut_down in
            current_merge_pipeline_ids.difference(&needed_merge_pipeline_ids)
        {
            if let Some((_, merge_pipeline_handle)) = self
                .merge_pipeline_handles
                .remove_entry(merge_pipeline_id_to_shut_down)
            {
                // We kill the merge pipeline to avoid waiting a merge operation to finish as it can
                // be long.
                info!(
                    index_id=%merge_pipeline_id_to_shut_down.index_uid.index_id(),
                    source_id=%merge_pipeline_id_to_shut_down.source_id,
                    "No more indexing pipeline on this index and source, killing merge pipeline."
                );
                merge_pipeline_handle.handle.kill().await;
            }
        }
        // Finally remove the merge pipeline with an exit status.
        self.merge_pipeline_handles
            .retain(|_, merge_pipeline_mailbox_handle| {
                merge_pipeline_mailbox_handle.handle.state().is_running()
            });
        self.counters.num_running_merge_pipelines = self.merge_pipeline_handles.len();
        self.update_cluster_running_indexing_tasks().await;
        Ok(())
    }

    async fn get_or_create_merge_pipeline(
        &mut self,
        merge_pipeline_params: MergePipelineParams,
        ctx: &ActorContext<Self>,
    ) -> Result<Mailbox<MergePlanner>, IndexingError> {
        let merge_pipeline_id = MergePipelineId::from(&merge_pipeline_params.pipeline_id);
        if let Some(merge_pipeline_mailbox_handle) =
            self.merge_pipeline_handles.get(&merge_pipeline_id)
        {
            return Ok(merge_pipeline_mailbox_handle.mailbox.clone());
        }
        let merge_pipeline = MergePipeline::new(merge_pipeline_params, ctx.spawn_ctx());
        let merge_planner_mailbox = merge_pipeline.merge_planner_mailbox().clone();
        let (_pipeline_mailbox, pipeline_handle) = ctx.spawn_actor().spawn(merge_pipeline);
        let merge_pipeline_mailbox_handle = MergePipelineHandle {
            mailbox: merge_planner_mailbox.clone(),
            handle: pipeline_handle,
        };
        self.merge_pipeline_handles
            .insert(merge_pipeline_id, merge_pipeline_mailbox_handle);
        self.counters.num_running_merge_pipelines += 1;
        Ok(merge_planner_mailbox)
    }

    /// Applies the indexing plan by:
    /// - Stopping the running pipelines not present in the provided plan.
    /// - Starting the pipelines that are not running.
    /// Note: the indexing is a list of `IndexingTask` and has no ordinal
    /// like a pipeline. We assign an ordinal for each `IndexingTask` from
    /// [0, n) with n the number of indexing tasks given a (index_id, source_id).
    async fn apply_indexing_plan(
        &mut self,
        ctx: &ActorContext<Self>,
        physical_indexing_plan_request: ApplyIndexingPlanRequest,
    ) -> Result<ApplyIndexingPlanResponse, IndexingError> {
        let mut updated_pipeline_ids: HashSet<IndexingPipelineId> = HashSet::new();
        let mut pipeline_ordinals: HashMap<&IndexingTask, usize> = HashMap::new();
        for indexing_task in physical_indexing_plan_request.indexing_tasks.iter() {
            let pipeline_ord = pipeline_ordinals.entry(indexing_task).or_insert(0);
            let pipeline_id = IndexingPipelineId {
                node_id: self.node_id.clone(),
                index_uid: IndexUid::from(indexing_task.index_uid.to_string()),
                source_id: indexing_task.source_id.clone(),
                pipeline_ord: *pipeline_ord,
            };
            *pipeline_ord += 1;
            updated_pipeline_ids.insert(pipeline_id);
        }

        let running_pipeline_ids: HashSet<IndexingPipelineId> =
            self.indexing_pipeline_handles.keys().cloned().collect();

        // Spawn new pipeline in the new plan that are not currently running
        let failed_spawning_pipeline_ids = self
            .spawn_pipelines(
                ctx,
                updated_pipeline_ids
                    .difference(&running_pipeline_ids)
                    .collect(),
            )
            .await?;

        // Shut down currently running pipelines that are missing in the new plan.
        self.shutdown_pipelines(
            running_pipeline_ids
                .difference(&updated_pipeline_ids)
                .collect(),
        )
        .await;

        self.update_cluster_running_indexing_tasks().await;

        if !failed_spawning_pipeline_ids.is_empty() {
            return Err(IndexingError::SpawnPipelinesError {
                pipeline_ids: failed_spawning_pipeline_ids,
            });
        }

        Ok(ApplyIndexingPlanResponse {})
    }

    /// Spawns the pipelines with supplied ids and returns a list of failed pipelines.
    async fn spawn_pipelines(
        &mut self,
        ctx: &ActorContext<Self>,
        added_pipeline_ids: Vec<&IndexingPipelineId>,
    ) -> Result<Vec<IndexingPipelineId>, IndexingError> {
        // We fetch the new indexes metadata.
        let indexes_metadata_futures = added_pipeline_ids
            .iter()
            // No need to emit two request for the same `index_uid`
            .unique_by(|pipeline_id| pipeline_id.index_uid.clone())
            .map(|pipeline_id| self.index_metadata(ctx, pipeline_id.index_uid.index_id()));
        let indexes_metadata = try_join_all(indexes_metadata_futures).await?;
        let indexes_metadata_by_index_id: HashMap<IndexUid, IndexMetadata> = indexes_metadata
            .into_iter()
            .map(|index_metadata| (index_metadata.index_uid.clone(), index_metadata))
            .collect();

        let mut failed_spawning_pipeline_ids: Vec<IndexingPipelineId> = Vec::new();

        // Add new pipelines.
        for new_pipeline_id in added_pipeline_ids {
            info!(pipeline_id=?new_pipeline_id, "Spawning indexing pipeline.");

            if let Some(index_metadata) =
                indexes_metadata_by_index_id.get(&new_pipeline_id.index_uid)
            {
                if let Some(source_config) = index_metadata.sources.get(&new_pipeline_id.source_id)
                {
                    if let Err(error) = self
                        .spawn_pipeline_inner(
                            ctx,
                            new_pipeline_id.clone(),
                            index_metadata.index_config.clone(),
                            source_config.clone(),
                        )
                        .await
                    {
                        error!(pipeline_id=?new_pipeline_id, err=?error, "Failed to spawn pipeline.");
                        failed_spawning_pipeline_ids.push(new_pipeline_id.clone());
                    }
                } else {
                    error!(pipeline_id=?new_pipeline_id, "Failed to spawn pipeline: source does not exist.");
                    failed_spawning_pipeline_ids.push(new_pipeline_id.clone());
                }
            } else {
                error!(
                    "Failed to spawn pipeline: index {} no longer exists.",
                    &new_pipeline_id.index_uid.to_string()
                );
                failed_spawning_pipeline_ids.push(new_pipeline_id.clone());
            }
        }

        Ok(failed_spawning_pipeline_ids)
    }

    /// Shuts down the pipelines with supplied ids and performs necessary cleanup.
    async fn shutdown_pipelines(&mut self, pipeline_ids: Vec<&IndexingPipelineId>) {
        for pipeline_id_to_remove in pipeline_ids.clone() {
            match self.detach_pipeline(pipeline_id_to_remove).await {
                Ok(pipeline_handle) => {
                    // Killing the pipeline ensure that all pipeline actors will stop.
                    pipeline_handle.kill().await;
                }
                Err(error) => {
                    // Just log the detach error, it can only come from a missing pipeline in the
                    // `indexing_pipeline_handles`.
                    error!(
                        pipeline_id=?pipeline_id_to_remove,
                        err=?error,
                        "Detach error.",
                    );
                }
            }
        }

        // If at least one ingest source has been removed, the related index has possibly been
        // deleted. Thus we run a garbage collect to remove queues of potentially deleted
        // indexes.
        let should_gc_ingest_api_queues = pipeline_ids
            .iter()
            .any(|pipeline_id_to_remove| pipeline_id_to_remove.source_id == INGEST_API_SOURCE_ID);
        if should_gc_ingest_api_queues {
            if let Err(error) = self.run_ingest_api_queues_gc().await {
                warn!(
                    err=?error,
                    "Ingest API queues garbage collect error.",
                );
            }
        }
    }

    /// Updates running indexing tasks in chitchat cluster state.
    async fn update_cluster_running_indexing_tasks(&self) {
        let indexing_tasks = self
            .indexing_pipeline_handles
            .keys()
            .map(|pipeline_id| IndexingTask {
                index_uid: pipeline_id.index_uid.to_string(),
                source_id: pipeline_id.source_id.clone(),
                shard_ids: Vec::new(),
            })
            // Sort indexing tasks so it's more readable for debugging purpose.
            .sorted_by(|left, right| {
                (&left.index_uid, &left.source_id).cmp(&(&right.index_uid, &right.source_id))
            })
            .collect_vec();

        if let Err(error) = self
            .cluster
            .update_self_node_indexing_tasks(&indexing_tasks)
            .await
        {
            error!(
                "Error when updating the cluster state with indexing running tasks: {}",
                error
            );
        }
    }

    /// Garbage collects ingest API queues of deleted indexes.
    async fn run_ingest_api_queues_gc(&mut self) -> anyhow::Result<()> {
        let Some(ingest_api_service) = &self.ingest_api_service_opt else {
            return Ok(());
        };
        let queues: HashSet<String> = ingest_api_service
            .ask_for_res(ListQueuesRequest {})
            .await
            .context("Failed to list queues.")?
            .queues
            .into_iter()
            .collect();
        debug!(queues=?queues, "List ingest API queues.");

        let index_ids: HashSet<String> = self
            .metastore
            .list_indexes_metadatas(ListIndexesQuery::All)
            .await
            .context("Failed to list queues")?
            .into_iter()
            .map(|index_metadata| index_metadata.index_id().to_string())
            .collect();
        debug!(index_ids=?index_ids, "List indexes.");

        let queue_ids_to_delete = queues.difference(&index_ids);

        for queue_id in queue_ids_to_delete {
            let delete_queue_res = ingest_api_service
                .ask_for_res(DropQueueRequest {
                    queue_id: queue_id.to_string(),
                })
                .await;
            if let Err(delete_queue_error) = delete_queue_res {
                error!(error=?delete_queue_error, queue_id=%queue_id, "queue-delete-failure");
                self.counters.num_delete_queue_failures += 1;
            } else {
                info!(queue_id=%queue_id, "queue-delete-success");
                self.counters.num_deleted_queues += 1;
            }
        }
        Ok(())
    }
}

#[async_trait]
impl Handler<ObservePipeline> for IndexingService {
    type Reply = Result<Observation<IndexingStatistics>, IndexingError>;

    async fn handle(
        &mut self,
        msg: ObservePipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        let observation = self.observe_pipeline(&msg.pipeline_id).await;
        Ok(observation)
    }
}

#[async_trait]
impl Handler<DetachIndexingPipeline> for IndexingService {
    type Reply = Result<ActorHandle<IndexingPipeline>, IndexingError>;

    async fn handle(
        &mut self,
        msg: DetachIndexingPipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.detach_pipeline(&msg.pipeline_id).await)
    }
}

#[async_trait]
impl Handler<DetachMergePipeline> for IndexingService {
    type Reply = Result<ActorHandle<MergePipeline>, IndexingError>;

    async fn handle(
        &mut self,
        msg: DetachMergePipeline,
        _ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.detach_merge_pipeline(&msg.pipeline_id).await)
    }
}

#[derive(Debug)]
struct SuperviseLoop;

#[async_trait]
impl Handler<SuperviseLoop> for IndexingService {
    type Reply = ();

    async fn handle(
        &mut self,
        _message: SuperviseLoop,
        ctx: &ActorContext<Self>,
    ) -> Result<(), ActorExitStatus> {
        self.handle_supervise().await?;
        ctx.schedule_self_msg(*quickwit_actors::HEARTBEAT, SuperviseLoop)
            .await;
        Ok(())
    }
}

#[async_trait]
impl Actor for IndexingService {
    type ObservableState = IndexingServiceCounters;

    fn observable_state(&self) -> Self::ObservableState {
        self.counters.clone()
    }

    async fn initialize(&mut self, ctx: &ActorContext<Self>) -> Result<(), ActorExitStatus> {
        self.run_ingest_api_queues_gc().await?;
        self.handle(SuperviseLoop, ctx).await
    }
}

#[async_trait]
impl Handler<SpawnPipeline> for IndexingService {
    type Reply = Result<IndexingPipelineId, IndexingError>;
    async fn handle(
        &mut self,
        message: SpawnPipeline,
        ctx: &ActorContext<Self>,
    ) -> Result<Result<IndexingPipelineId, IndexingError>, ActorExitStatus> {
        Ok(self
            .spawn_pipeline(
                ctx,
                message.index_id,
                message.source_config,
                message.pipeline_ord,
            )
            .await)
    }
}

#[async_trait]
impl Handler<ApplyIndexingPlanRequest> for IndexingService {
    type Reply = Result<ApplyIndexingPlanResponse, IndexingError>;

    async fn handle(
        &mut self,
        plan_request: ApplyIndexingPlanRequest,
        ctx: &ActorContext<Self>,
    ) -> Result<Self::Reply, ActorExitStatus> {
        Ok(self.apply_indexing_plan(ctx, plan_request).await)
    }
}

#[async_trait]
impl Handler<Healthz> for IndexingService {
    type Reply = bool;

    async fn handle(
        &mut self,
        _msg: Healthz,
        _ctx: &ActorContext<Self>,
    ) -> Result<bool, ActorExitStatus> {
        // In the future, check metrics such as available disk space.
        Ok(true)
    }
}

#[cfg(test)]
mod tests {
    use std::num::NonZeroUsize;
    use std::path::Path;
    use std::time::Duration;

    use chitchat::transport::ChannelTransport;
    use quickwit_actors::{Health, ObservationType, Supervisable, Universe, HEARTBEAT};
    use quickwit_cluster::create_cluster_for_test;
    use quickwit_common::rand::append_random_suffix;
    use quickwit_config::{
        IngestApiConfig, SourceConfig, SourceInputFormat, SourceParams, VecSourceParams,
    };
    use quickwit_ingest::{init_ingest_api, CreateQueueIfNotExistsRequest};
    use quickwit_metastore::{metastore_for_test, MockMetastore};
    use quickwit_proto::indexing::IndexingTask;

    use super::*;

    async fn spawn_indexing_service(
        data_dir_path: &Path,
        universe: &Universe,
        metastore: Arc<dyn Metastore>,
        cluster: Cluster,
    ) -> (Mailbox<IndexingService>, ActorHandle<IndexingService>) {
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path.to_path_buf(),
            indexer_config,
            num_blocking_threads,
            cluster,
            metastore,
            Some(ingest_api_service),
            storage_resolver.clone(),
        )
        .await
        .unwrap();
        universe.spawn_builder().spawn(indexing_server)
    }

    #[tokio::test]
    async fn test_indexing_service_spawn_observe_detach() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let index_uid = metastore.create_index(index_config).await.unwrap();
        metastore
            .add_source(index_uid.clone(), SourceConfig::ingest_api_default())
            .await
            .unwrap();
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) =
            spawn_indexing_service(temp_dir.path(), &universe, metastore, cluster).await;
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        // Test `spawn_pipeline`.
        let source_config_0 = SourceConfig {
            source_id: "test-indexing-service--source-0".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let spawn_pipeline_msg = SpawnPipeline {
            index_id: index_id.clone(),
            pipeline_ord: 0,
            source_config: source_config_0.clone(),
        };
        let pipeline_id = indexing_service
            .ask_for_res(spawn_pipeline_msg.clone())
            .await
            .unwrap();
        indexing_service
            .ask_for_res(spawn_pipeline_msg)
            .await
            .unwrap_err();
        assert_eq!(pipeline_id.index_uid.index_id(), index_id);
        assert_eq!(pipeline_id.source_id, source_config_0.source_id);
        assert_eq!(pipeline_id.node_id, "test-node");
        assert_eq!(pipeline_id.pipeline_ord, 0);
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            1
        );

        // Test `observe_pipeline`.
        let observation = indexing_service
            .ask_for_res(ObservePipeline {
                pipeline_id: pipeline_id.clone(),
            })
            .await
            .unwrap();
        assert_eq!(observation.obs_type, ObservationType::Alive);
        assert_eq!(observation.generation, 1);
        assert_eq!(observation.num_spawn_attempts, 1);

        // Test detach.
        let pipeline_handle = indexing_service
            .ask_for_res(DetachIndexingPipeline {
                pipeline_id: pipeline_id.clone(),
            })
            .await
            .unwrap();
        pipeline_handle.kill().await;
        let _merge_pipeline = indexing_service
            .ask_for_res(DetachMergePipeline {
                pipeline_id: MergePipelineId::from(&pipeline_id),
            })
            .await
            .unwrap();
        let observation = indexing_service_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 0);
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_supervise_pipelines() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        metastore.create_index(index_config).await.unwrap();

        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_server_handle) =
            spawn_indexing_service(temp_dir.path(), &universe, metastore, cluster).await;

        // Test `supervise_pipelines`
        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::Vec(VecSourceParams {
                docs: Vec::new(),
                batch_num_docs: 10,
                partition: "0".to_string(),
            }),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_ord: 0,
            })
            .await
            .unwrap();
        for _ in 0..2000 {
            let obs = indexing_server_handle.observe().await;
            if obs.num_successful_pipelines == 1 {
                // It may or may not panic
                universe.quit().await;
                return;
            }
            universe.sleep(Duration::from_millis(100)).await;
        }
        panic!("Pipeline not exited successfully.");
    }

    #[tokio::test]
    async fn test_indexing_service_apply_plan() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let index_uid = metastore.create_index(index_config).await.unwrap();
        metastore
            .add_source(index_uid.clone(), SourceConfig::ingest_api_default())
            .await
            .unwrap();
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) = spawn_indexing_service(
            temp_dir.path(),
            &universe,
            metastore.clone(),
            cluster.clone(),
        )
        .await;

        // Test `apply plan`.
        let source_config_1 = SourceConfig {
            source_id: "test-indexing-service--source-1".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        metastore
            .add_source(index_uid.clone(), source_config_1.clone())
            .await
            .unwrap();
        let metadata = metastore.index_metadata(index_id.as_str()).await.unwrap();
        let indexing_tasks = vec![
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest { indexing_tasks })
            .await
            .unwrap();
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            2
        );

        let source_config_2 = SourceConfig {
            source_id: "test-indexing-service--source-2".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(2).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(2).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        metastore
            .add_source(index_uid.clone(), source_config_2.clone())
            .await
            .unwrap();

        let indexing_tasks = vec![
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: INGEST_API_SOURCE_ID.to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: source_config_2.source_id.clone(),
                shard_ids: Vec::new(),
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        assert_eq!(
            indexing_service_handle
                .observe()
                .await
                .num_running_pipelines,
            4
        );

        cluster
            .wait_for_ready_members(
                |members| {
                    members
                        .iter()
                        .any(|member| member.indexing_tasks.len() == indexing_tasks.len())
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let self_member = &cluster.ready_members().await[0];

        assert_eq!(
            HashSet::<_>::from_iter(self_member.indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );
        let indexing_tasks = vec![
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: INGEST_API_SOURCE_ID.to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: "test-indexing-service--source-1".to_string(),
                shard_ids: Vec::new(),
            },
            IndexingTask {
                index_uid: metadata.index_uid.to_string(),
                source_id: source_config_2.source_id.clone(),
                shard_ids: Vec::new(),
            },
        ];
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: indexing_tasks.clone(),
            })
            .await
            .unwrap();
        let indexing_service_obs = indexing_service_handle.observe().await;
        assert_eq!(indexing_service_obs.num_running_pipelines, 3);
        assert_eq!(indexing_service_obs.num_deleted_queues, 0);
        assert_eq!(indexing_service_obs.num_delete_queue_failures, 0);

        indexing_service_handle.process_pending_and_observe().await;

        cluster
            .wait_for_ready_members(
                |members| {
                    members
                        .iter()
                        .any(|member| member.indexing_tasks.len() == indexing_tasks.len())
                },
                Duration::from_secs(5),
            )
            .await
            .unwrap();

        let self_member = &cluster.ready_members().await[0];

        assert_eq!(
            HashSet::<_>::from_iter(self_member.indexing_tasks.iter()),
            HashSet::from_iter(indexing_tasks.iter())
        );

        // Delete index and apply empty plan
        metastore.delete_index(index_uid).await.unwrap();
        indexing_service
            .ask_for_res(ApplyIndexingPlanRequest {
                indexing_tasks: Vec::new(),
            })
            .await
            .unwrap();
        let indexing_service_obs = indexing_service_handle.observe().await;
        assert_eq!(indexing_service_obs.num_running_pipelines, 0);
        assert_eq!(indexing_service_obs.num_deleted_queues, 1);
        assert_eq!(indexing_service_obs.num_delete_queue_failures, 0);
        indexing_service_handle.quit().await;
        universe.assert_quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_shut_down_merge_pipeline_when_no_indexing_pipeline() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();

        let index_id = append_random_suffix("test-indexing-service");
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);

        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        let index_uid = metastore.create_index(index_config).await.unwrap();
        metastore
            .add_source(index_uid.clone(), source_config.clone())
            .await
            .unwrap();

        // Test `IndexingService::new`.
        let temp_dir = tempfile::tempdir().unwrap();
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let universe = Universe::with_accelerated_time();
        let queues_dir_path = data_dir_path.join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let indexing_server = IndexingService::new(
            "test-node".to_string(),
            data_dir_path,
            indexer_config,
            num_blocking_threads,
            cluster.clone(),
            metastore.clone(),
            Some(ingest_api_service),
            storage_resolver.clone(),
        )
        .await
        .unwrap();
        let (indexing_server_mailbox, indexing_server_handle) =
            universe.spawn_builder().spawn(indexing_server);
        let pipeline_id = indexing_server_mailbox
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_ord: 0,
            })
            .await
            .unwrap();
        let observation = indexing_server_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);

        // Test `shutdown_pipeline`
        let pipeline = indexing_server_mailbox
            .ask_for_res(DetachIndexingPipeline { pipeline_id })
            .await
            .unwrap();
        pipeline.quit().await;

        // Let the service cleanup the merge pipelines.
        universe.sleep(*HEARTBEAT).await;

        let observation = indexing_server_handle.process_pending_and_observe().await;
        assert_eq!(observation.num_running_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 0);
        universe.sleep(*HEARTBEAT).await;
        // Check that the merge pipeline is also shut down as they are no more indexing pipeilne on
        // the index.
        assert!(universe.get_one::<MergePipeline>().is_none());
        // It may or may not panic
        universe.quit().await;
    }

    #[derive(Debug)]
    struct FreezePipeline;
    #[async_trait]
    impl Handler<FreezePipeline> for IndexingPipeline {
        type Reply = ();
        async fn handle(
            &mut self,
            _: FreezePipeline,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            tokio::time::sleep(*HEARTBEAT * 5).await;
            Ok(())
        }
    }

    #[derive(Debug)]
    struct ObservePipelineHealth(IndexingPipelineId);
    #[async_trait]
    impl Handler<ObservePipelineHealth> for IndexingService {
        type Reply = Health;
        async fn handle(
            &mut self,
            message: ObservePipelineHealth,
            _ctx: &ActorContext<Self>,
        ) -> Result<Self::Reply, ActorExitStatus> {
            Ok(self
                .indexing_pipeline_handles
                .get(&message.0)
                .unwrap()
                .check_health(true))
        }
    }

    #[tokio::test]
    async fn test_indexing_service_does_not_shut_down_pipelines_on_indexing_pipeline_freeze() {
        quickwit_common::setup_logging_for_tests();
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let index_id = append_random_suffix("test-indexing-service-indexing-pipeline-timeout");
        let index_uri = format!("ram:///indexes/{index_id}");
        let mut index_metadata = IndexMetadata::for_test(&index_id, &index_uri);
        let source_config = SourceConfig {
            source_id: "test-indexing-service--source".to_string(),
            max_num_pipelines_per_indexer: NonZeroUsize::new(1).unwrap(),
            desired_num_pipelines: NonZeroUsize::new(1).unwrap(),
            enabled: true,
            source_params: SourceParams::void(),
            transform_config: None,
            input_format: SourceInputFormat::Json,
        };
        index_metadata
            .sources
            .insert(source_config.source_id.clone(), source_config.clone());
        let mut metastore = MockMetastore::default();
        let index_metadata_clone = index_metadata.clone();
        metastore.expect_list_indexes_metadatas().returning(
            move |_list_indexes_query: ListIndexesQuery| Ok(vec![index_metadata_clone.clone()]),
        );
        metastore
            .expect_index_metadata()
            .returning(move |_| Ok(index_metadata.clone()));
        metastore.expect_list_splits().returning(|_| Ok(Vec::new()));
        let universe = Universe::new();
        let temp_dir = tempfile::tempdir().unwrap();
        let (indexing_service, indexing_service_handle) =
            spawn_indexing_service(temp_dir.path(), &universe, Arc::new(metastore), cluster).await;
        let _pipeline_id = indexing_service
            .ask_for_res(SpawnPipeline {
                index_id: index_id.clone(),
                source_config,
                pipeline_ord: 0,
            })
            .await
            .unwrap();
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_successful_pipelines, 0);

        let indexing_pipeline = universe.get_one::<IndexingPipeline>().unwrap();

        // Freeze pipeline during 5 heartbeats.
        indexing_pipeline
            .send_message(FreezePipeline)
            .await
            .unwrap();
        universe.sleep(*HEARTBEAT * 5).await;
        // Check that indexing and merge pipelines are still running.
        let observation = indexing_service_handle.observe().await;
        assert_eq!(observation.num_running_pipelines, 1);
        assert_eq!(observation.num_failed_pipelines, 0);
        assert_eq!(observation.num_running_merge_pipelines, 1);
        // Might generate panics
        universe.quit().await;
    }

    #[tokio::test]
    async fn test_indexing_service_ingest_api_gc() {
        let index_id = "test-ingest-api-gc-index".to_string();
        let index_uri = format!("ram:///indexes/{index_id}");
        let index_config = IndexConfig::for_test(&index_id, &index_uri);
        let transport = ChannelTransport::default();
        let cluster = create_cluster_for_test(Vec::new(), &["indexer"], &transport, true)
            .await
            .unwrap();
        let metastore = metastore_for_test();
        let index_uid = metastore.create_index(index_config).await.unwrap();

        // Setup ingest api objects
        let universe = Universe::with_accelerated_time();
        let temp_dir = tempfile::tempdir().unwrap();
        let queues_dir_path = temp_dir.path().join(QUEUES_DIR_NAME);
        let ingest_api_service =
            init_ingest_api(&universe, &queues_dir_path, &IngestApiConfig::default())
                .await
                .unwrap();
        let create_queue_req = CreateQueueIfNotExistsRequest {
            queue_id: index_id.clone(),
        };
        ingest_api_service
            .ask_for_res(create_queue_req)
            .await
            .unwrap();

        // Setup `IndexingService`
        let data_dir_path = temp_dir.path().to_path_buf();
        let indexer_config = IndexerConfig::for_test().unwrap();
        let num_blocking_threads = 1;
        let storage_resolver = StorageResolver::unconfigured();
        let mut indexing_server = IndexingService::new(
            "test-ingest-api-gc-node".to_string(),
            data_dir_path,
            indexer_config,
            num_blocking_threads,
            cluster.clone(),
            metastore.clone(),
            Some(ingest_api_service.clone()),
            storage_resolver.clone(),
        )
        .await
        .unwrap();

        indexing_server.run_ingest_api_queues_gc().await.unwrap();
        assert_eq!(indexing_server.counters.num_deleted_queues, 0);

        metastore.delete_index(index_uid).await.unwrap();

        indexing_server.run_ingest_api_queues_gc().await.unwrap();
        assert_eq!(indexing_server.counters.num_deleted_queues, 1);

        universe.assert_quit().await;
    }
}
