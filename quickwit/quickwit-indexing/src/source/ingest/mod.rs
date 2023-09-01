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
use std::fmt;
use std::sync::Arc;
use std::time::{Duration, Instant};

use anyhow::Context;
use async_trait::async_trait;
use futures::stream::FuturesUnordered;
use futures::StreamExt;
use quickwit_actors::{ActorExitStatus, Mailbox};
use quickwit_ingest::{IngesterPool, MultiFetchStream};
use quickwit_metastore::checkpoint::{PartitionId, Position, SourceCheckpoint};
use quickwit_metastore::Metastore;
use quickwit_proto::ingest::ingester::{
    FetchResponseV2, IngesterService, TruncateRequest, TruncateSubrequest,
};
use quickwit_proto::metastore::{AcquireShardsRequest, AcquireShardsSubrequest};
use quickwit_proto::types::NodeId;
use quickwit_proto::{IndexUid, PublishToken, ShardId, SourceId};
use serde_json::json;
use tokio::time;
use tracing::{debug, error, info, warn};
use ulid::Ulid;

use super::{
    Assignment, BatchBuilder, Source, SourceContext, SourceRuntimeArgs, TypedSourceFactory,
};
use crate::actors::DocProcessor;
use crate::models::{NewPublishLock, NewPublishToken, PublishLock};

pub struct IngestSourceFactory;

#[async_trait]
impl TypedSourceFactory for IngestSourceFactory {
    type Source = IngestSource;
    type Params = ();

    async fn typed_create_source(
        runtime_args: Arc<SourceRuntimeArgs>,
        _: Self::Params,
        checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self::Source> {
        IngestSource::try_new(runtime_args, checkpoint).await
    }
}

/// The [`ClientId`] is a unique identifier for a client of the ingest service and allows to
/// distinguish which indexers are streaming documents from a shard. It is also used to form a
/// publish token.
#[derive(Debug, Clone)]
struct ClientId {
    node_id: NodeId,
    index_uid: IndexUid,
    source_id: SourceId,
    pipeline_ord: usize,
}

impl ClientId {
    fn new(node_id: NodeId, index_uid: IndexUid, source_id: SourceId, pipeline_ord: usize) -> Self {
        Self {
            node_id,
            index_uid,
            source_id,
            pipeline_ord,
        }
    }

    fn client_id(&self) -> String {
        format!(
            "indexer/{}/{}/{}/{}",
            self.node_id, self.index_uid, self.source_id, self.pipeline_ord
        )
    }
}

struct AssignedShard {
    leader_id: NodeId,
    partition_id: PartitionId,
    current_position_inclusive: Position,
}

/// Streams documents from a set of shards.
pub struct IngestSource {
    client_id: ClientId,
    metastore: Arc<dyn Metastore>,
    ingester_pool: IngesterPool,
    assigned_shards: HashMap<ShardId, AssignedShard>,
    fetch_stream: MultiFetchStream,
    publish_lock: PublishLock,
    publish_token: PublishToken,
}

impl fmt::Debug for IngestSource {
    fn fmt(&self, formatter: &mut fmt::Formatter) -> fmt::Result {
        formatter.debug_struct("IngestSource").finish()
    }
}

impl IngestSource {
    pub async fn try_new(
        runtime_args: Arc<SourceRuntimeArgs>,
        _checkpoint: SourceCheckpoint,
    ) -> anyhow::Result<Self> {
        let node_id: NodeId = runtime_args.node_id().into();
        let client_id = ClientId::new(
            node_id.clone(),
            runtime_args.index_uid().clone(),
            runtime_args.source_id().to_string(),
            runtime_args.pipeline_ord(),
        );
        let metastore = runtime_args.metastore.clone();
        let ingester_pool = runtime_args.ingester_pool.clone();
        let assigned_shards = HashMap::new();
        let fetch_stream =
            MultiFetchStream::new(node_id, client_id.client_id(), ingester_pool.clone());
        let publish_lock = PublishLock::default();
        let publish_token = format!("{}:{}", client_id.client_id(), Ulid::new());

        Ok(Self {
            client_id,
            metastore,
            ingester_pool,
            assigned_shards,
            fetch_stream,
            publish_lock,
            publish_token,
        })
    }

    fn process_fetch_response(
        &mut self,
        batch_builder: &mut BatchBuilder,
        fetch_response: FetchResponseV2,
    ) -> anyhow::Result<()> {
        let assigned_shard = self
            .assigned_shards
            .get_mut(&fetch_response.shard_id)
            .expect("shard should be assigned");
        let partition_id = assigned_shard.partition_id.clone();

        for doc in fetch_response.docs() {
            batch_builder.push(doc);
        }
        let from_position_exclusive = assigned_shard.current_position_inclusive.clone();
        let to_position_inclusive = fetch_response
            .to_position_inclusive()
            .map(Position::from)
            .unwrap_or(Position::Beginning);
        batch_builder
            .checkpoint_delta
            .record_partition_delta(
                partition_id,
                from_position_exclusive,
                to_position_inclusive.clone(),
            )
            .context("failed to record partition delta")?;
        assigned_shard.current_position_inclusive = to_position_inclusive;
        Ok(())
    }
}

#[async_trait]
impl Source for IngestSource {
    async fn emit_batches(
        &mut self,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> Result<Duration, ActorExitStatus> {
        let now = Instant::now();
        let mut batch_builder = BatchBuilder::default();
        let deadline = time::sleep(*quickwit_actors::HEARTBEAT / 2);
        tokio::pin!(deadline);

        loop {
            tokio::select! {
                fetch_payload_opt = self.fetch_stream.next() => {
                    match fetch_payload_opt {
                        Some(Ok(fetch_payload)) => {
                            self.process_fetch_response(&mut batch_builder, fetch_payload)?;

                            if batch_builder.num_bytes >= 5 * 1024 * 1024 {
                                break;
                            }
                        },
                        Some(Err(error)) => {
                            error!(error=?error, "Failed to fetch payload.")
                            // TODO handle error
                        },
                        _ => panic!("Fetch stream terminated unexpectedly."), // FIXME
                    }
                }
                _ = &mut deadline => {
                    break;
                }
            }
            ctx.record_progress();
        }
        if !batch_builder.checkpoint_delta.is_empty() {
            debug!(
                num_docs=%batch_builder.docs.len(),
                num_bytes=%batch_builder.num_bytes,
                num_millis=%now.elapsed().as_millis(),
                "Sending doc batch to indexer."
            );
            let message = batch_builder.build();
            ctx.send_message(doc_processor_mailbox, message).await?;
        }
        Ok(Duration::default())
    }

    async fn assign_shards(
        &mut self,
        assignement: Assignment,
        doc_processor_mailbox: &Mailbox<DocProcessor>,
        ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        info!("new shard assignment: `{:?}`", assignement.shard_ids);

        ctx.protect_future(self.publish_lock.kill()).await;

        self.publish_lock = PublishLock::default();
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishLock(self.publish_lock.clone()),
        )
        .await?;

        self.publish_token = format!("{}:{}", self.client_id.client_id(), Ulid::new());
        ctx.send_message(
            doc_processor_mailbox,
            NewPublishToken(self.publish_token.clone()),
        )
        .await?;

        let acquire_shards_subrequest = AcquireShardsSubrequest {
            index_uid: self.client_id.index_uid.to_string(),
            source_id: self.client_id.source_id.clone(),
            shard_ids: assignement.shard_ids,
            publish_token: self.publish_token.clone(),
        };
        let acquire_shards_request = AcquireShardsRequest {
            subrequests: vec![acquire_shards_subrequest],
        };
        let acquire_shards_response = self
            .metastore
            .acquire_shards(acquire_shards_request)
            .await
            .context("failed to acquire shards")?;

        let Some(acquire_shards_subresponse) = acquire_shards_response
            .subresponses
            .into_iter()
            .find(|subresponse| {
                self.client_id.index_uid.as_str() == subresponse.index_uid
                    && subresponse.source_id == self.client_id.source_id
            })
        else {
            return Ok(());
        };
        self.assigned_shards.clear();
        self.fetch_stream.reset();

        for acquired_shard in acquire_shards_subresponse.acquired_shards {
            let leader_id: NodeId = acquired_shard.leader_id.into();
            let follower_id: Option<NodeId> = acquired_shard
                .follower_id
                .map(|follower_id| follower_id.into());
            let index_uid: IndexUid = acquired_shard.index_uid.into();
            let source_id: SourceId = acquired_shard.source_id;
            let shard_id = acquired_shard.shard_id;
            let partition_id = PartitionId::from(shard_id);
            let current_position_inclusive =
                Position::from(acquired_shard.publish_position_inclusive);
            let from_position_exclusive = current_position_inclusive.as_u64();
            let to_position_inclusive = None;

            if let Err(error) = self
                .fetch_stream
                .subscribe(
                    leader_id.clone(),
                    follower_id.clone(),
                    index_uid,
                    source_id,
                    shard_id,
                    from_position_exclusive,
                    to_position_inclusive,
                )
                .await
            {
                error!(error=%error, "failed to subscribe to shard");
                continue;
            }
            let assigned_shard = AssignedShard {
                leader_id,
                partition_id,
                current_position_inclusive,
            };
            self.assigned_shards.insert(shard_id, assigned_shard);
        }
        Ok(())
    }

    async fn suggest_truncate(
        &mut self,
        checkpoint: SourceCheckpoint,
        _ctx: &SourceContext,
    ) -> anyhow::Result<()> {
        let mut per_leader_truncate_subrequests: HashMap<&NodeId, Vec<TruncateSubrequest>> =
            HashMap::new();

        for (partition_id, position) in checkpoint.iter() {
            let shard_id = partition_id.as_u64().expect("shard ID should be a u64");
            let leader_id = &self
                .assigned_shards
                .get(&shard_id)
                .expect("shard should be assigned") // TODO: This is not true if `assign_shards` is called before `suggest_truncate`.
                .leader_id;
            let to_position_inclusive = position.as_u64().expect("position should be a u64");

            let truncate_subrequest = TruncateSubrequest {
                index_uid: self.client_id.index_uid.clone().into(),
                source_id: self.client_id.source_id.clone(),
                shard_id,
                to_position_inclusive,
            };
            per_leader_truncate_subrequests
                .entry(leader_id)
                .or_default()
                .push(truncate_subrequest);
        }
        let mut truncate_futures = FuturesUnordered::new();

        for (leader_id, truncate_subrequests) in per_leader_truncate_subrequests {
            let Some(mut ingester) = self.ingester_pool.get(leader_id).await else {
                warn!(
                    "failed to truncate shard: ingester `{}` is unavailable",
                    leader_id
                );
                continue;
            };
            let truncate_request = TruncateRequest {
                leader_id: leader_id.clone().into(),
                subrequests: truncate_subrequests,
            };
            let truncate_future = async move { ingester.truncate(truncate_request).await };
            truncate_futures.push(truncate_future);
        }
        while let Some(truncate_response_res) = truncate_futures.next().await {
            if let Err(error) = truncate_response_res {
                warn!("failed to truncate shard(s): {error}");
            }
        }
        Ok(())
    }

    fn name(&self) -> String {
        "IngestSource".to_string()
    }

    fn observable_state(&self) -> serde_json::Value {
        json!({})
    }
}
