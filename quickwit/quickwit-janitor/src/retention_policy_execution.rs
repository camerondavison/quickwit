// Copyright (C) 2022 Quickwit, Inc.
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

use std::sync::Arc;

use quickwit_actors::ActorContext;
use quickwit_config::{RetentionPolicy, RetentionPolicyCutoffReference};
use quickwit_metastore::{ListSplitsQuery, Metastore, SplitMetadata, SplitState};
use tracing::info;

use crate::actors::RetentionPolicyExecutor;

/// Detect all expired splits based a retention policy and
/// only mark them as `MarkedForDeletion`. Actual split deletion
/// is taken care of by the garbage collector.
///
/// * `index_id` - The target index id.
/// * `metastore` - The metastore managing the target index.
/// * `retention_policy` - The retention policy to used to evaluate the splits.
/// * `ctx_opt` - A context for reporting progress (only useful within quickwit actor).
pub async fn run_execute_retention_policy(
    index_id: &str,
    metastore: Arc<dyn Metastore>,
    retention_policy: &RetentionPolicy,
    ctx: &ActorContext<RetentionPolicyExecutor>,
) -> anyhow::Result<Vec<SplitMetadata>> {
    // Select splits that are published filter and older than retention period.
    let retention_period = retention_policy.retention_period()?;
    let base_query = ListSplitsQuery::for_index(index_id).with_split_state(SplitState::Published);
    let query = match retention_policy.cutoff_reference {
        RetentionPolicyCutoffReference::IndexingTimestamp => {
            base_query.with_age_on_indexing_end_timestamp(retention_period)
        }
        RetentionPolicyCutoffReference::SplitTimestampField => {
            base_query.with_age_on_split_timestamp_field(retention_period)
        }
    };

    let expired_splits: Vec<SplitMetadata> = ctx
        .protect_future(metastore.list_splits(query))
        .await?
        .into_iter()
        .map(|meta| meta.split_metadata)
        .collect();

    if expired_splits.is_empty() {
        return Ok(expired_splits);
    }

    info!(index_id=%index_id, num_splits=%expired_splits.len(), "retention-policy-mark-splits-for-deletion");
    // Change all expired splits state to MarkedForDeletion.
    let split_ids: Vec<&str> = expired_splits.iter().map(|meta| meta.split_id()).collect();
    ctx.protect_future(metastore.mark_splits_for_deletion(index_id, &split_ids))
        .await?;

    Ok(expired_splits)
}
