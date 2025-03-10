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
use std::fmt::{self, Display, Write};
use std::ops::Bound;
use std::str::FromStr;
use std::sync::Arc;
use std::time::Duration;

use async_trait::async_trait;
use itertools::Itertools;
use quickwit_common::uri::Uri;
use quickwit_common::PrettySample;
use quickwit_config::{
    validate_index_id_pattern, IndexConfig, MetastoreBackend, MetastoreConfig,
    PostgresMetastoreConfig, SourceConfig,
};
use quickwit_doc_mapper::tag_pruning::TagFilterAst;
use quickwit_proto::metastore::{
    DeleteQuery, DeleteTask, EntityKind, MetastoreError, MetastoreResult,
};
use quickwit_proto::{IndexUid, PublishToken};
use sqlx::migrate::Migrator;
use sqlx::postgres::{PgConnectOptions, PgDatabaseError, PgPoolOptions};
use sqlx::{ConnectOptions, Pool, Postgres, Transaction};
use tokio::sync::Mutex;
use tracing::log::LevelFilter;
use tracing::{debug, error, info, instrument, warn};

use crate::checkpoint::IndexCheckpointDelta;
use crate::metastore::instrumented_metastore::InstrumentedMetastore;
use crate::metastore::postgresql_model::{PgDeleteTask, PgIndex, PgSplit};
use crate::metastore::FilterRange;
use crate::{
    IndexMetadata, ListIndexesQuery, ListSplitsQuery, Metastore, MetastoreFactory,
    MetastoreResolverError, Split, SplitMaturity, SplitMetadata, SplitState,
};

static MIGRATOR: Migrator = sqlx::migrate!("migrations/postgresql");

// https://www.postgresql.org/docs/current/errcodes-appendix.html
mod pg_error_code {
    pub const FOREIGN_KEY_VIOLATION: &str = "23503";
    pub const UNIQUE_VIOLATION: &str = "23505";
}

/// Establishes a connection to the given database URI.
async fn establish_connection(
    connection_uri: &Uri,
    min_connections: usize,
    max_connections: usize,
    acquire_timeout: Duration,
    idle_timeout_opt: Option<Duration>,
    max_lifetime_opt: Option<Duration>,
) -> MetastoreResult<Pool<Postgres>> {
    let pool_options = PgPoolOptions::new()
        .min_connections(min_connections as u32)
        .max_connections(max_connections as u32)
        .acquire_timeout(acquire_timeout)
        .idle_timeout(idle_timeout_opt)
        .max_lifetime(max_lifetime_opt);
    let pg_connect_options: PgConnectOptions =
        PgConnectOptions::from_str(connection_uri.as_str())?.log_statements(LevelFilter::Info);
    pool_options
        .connect_with(pg_connect_options)
        .await
        .map_err(|error| {
            error!(connection_uri=%connection_uri, error=?error, "Failed to establish connection to database.");
            MetastoreError::Connection {
                message: error.to_string(),
            }
        })
}

/// Initialize the database.
/// The sql used for the initialization is stored in quickwit-metastore/migrations directory.
#[instrument(skip_all)]
async fn run_postgres_migrations(pool: &Pool<Postgres>) -> MetastoreResult<()> {
    let tx = pool.begin().await?;
    let migration_res = MIGRATOR.run(pool).await;
    if let Err(migration_err) = migration_res {
        tx.rollback().await?;
        error!(err=?migration_err, "Database migrations failed");
        return Err(MetastoreError::Internal {
            message: "Failed to run migration on Postgresql database.".to_string(),
            cause: migration_err.to_string(),
        });
    }
    tx.commit().await?;
    Ok(())
}

/// PostgreSQL metastore implementation.
#[derive(Clone)]
pub struct PostgresqlMetastore {
    uri: Uri,
    connection_pool: Pool<Postgres>,
}

impl fmt::Debug for PostgresqlMetastore {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        f.debug_struct("PostgresqlMetastore")
            .field("uri", &self.uri)
            .finish()
    }
}

impl PostgresqlMetastore {
    /// Creates a meta store given a database URI.
    pub async fn new(
        postgres_metastore_config: &PostgresMetastoreConfig,
        connection_uri: &Uri,
    ) -> MetastoreResult<Self> {
        let acquire_timeout = if cfg!(any(test, feature = "testsuite")) {
            Duration::from_secs(20)
        } else {
            Duration::from_secs(2)
        };
        let connection_pool = establish_connection(
            connection_uri,
            1,
            postgres_metastore_config.max_num_connections.get(),
            acquire_timeout,
            Some(Duration::from_secs(1)),
            None,
        )
        .await?;
        run_postgres_migrations(&connection_pool).await?;
        Ok(PostgresqlMetastore {
            uri: connection_uri.clone(),
            connection_pool,
        })
    }
}

/// Returns an Index object given an index_id or None if it does not exist.
async fn index_opt<'a, E>(executor: E, index_id: &str) -> MetastoreResult<Option<PgIndex>>
where E: sqlx::Executor<'a, Database = Postgres> {
    let index_opt: Option<PgIndex> = sqlx::query_as::<_, PgIndex>(
        r#"
        SELECT *
        FROM indexes
        WHERE index_id = $1
        FOR UPDATE
        "#,
    )
    .bind(index_id)
    .fetch_optional(executor)
    .await
    .map_err(|error| MetastoreError::Db {
        message: error.to_string(),
    })?;
    Ok(index_opt)
}

/// Returns an Index object given an index_uid or None if it does not exist.
async fn index_opt_for_uid<'a, E>(
    executor: E,
    index_uid: IndexUid,
) -> MetastoreResult<Option<PgIndex>>
where
    E: sqlx::Executor<'a, Database = Postgres>,
{
    let index_opt: Option<PgIndex> = sqlx::query_as::<_, PgIndex>(
        r#"
        SELECT *
        FROM indexes
        WHERE index_uid = $1
        FOR UPDATE
        "#,
    )
    .bind(index_uid.to_string())
    .fetch_optional(executor)
    .await
    .map_err(|error| MetastoreError::Db {
        message: error.to_string(),
    })?;
    Ok(index_opt)
}

async fn index_metadata(
    tx: &mut Transaction<'_, Postgres>,
    index_id: &str,
) -> MetastoreResult<IndexMetadata> {
    index_opt(tx.as_mut(), index_id)
        .await?
        .ok_or_else(|| {
            MetastoreError::NotFound(EntityKind::Index {
                index_id: index_id.to_string(),
            })
        })?
        .index_metadata()
}

/// Extends an existing SQL string with the generated filter range appended to the query.
///
/// This method is **not** SQL injection proof and should not be used with user-defined values.
fn write_sql_filter<V: Display>(
    sql: &mut String,
    field_name: impl Display,
    filter_range: &FilterRange<V>,
    value_formatter: impl Fn(&V) -> String,
) {
    match &filter_range.start {
        Bound::Included(value) => {
            let _ = write!(sql, " AND {} >= {}", field_name, (value_formatter)(value));
        }
        Bound::Excluded(value) => {
            let _ = write!(sql, " AND {} > {}", field_name, (value_formatter)(value));
        }
        Bound::Unbounded => {}
    };

    match &filter_range.end {
        Bound::Included(value) => {
            let _ = write!(sql, " AND {} <= {}", field_name, (value_formatter)(value));
        }
        Bound::Excluded(value) => {
            let _ = write!(sql, " AND {} < {}", field_name, (value_formatter)(value));
        }
        Bound::Unbounded => {}
    };
}

fn build_query_filter(mut sql: String, query: &ListSplitsQuery) -> String {
    // Note: `ListSplitsQuery` builder enforces a non empty `index_uids` list.
    let where_predicate: String = query
        .index_uids
        .iter()
        .map(|index_uid| format!("index_uid = '{index_uid}'"))
        .join(" OR ");
    sql.push_str(&format!(" WHERE ({where_predicate})"));

    if !query.split_states.is_empty() {
        let params = query
            .split_states
            .iter()
            .map(|v| format!("'{}'", v.as_str()))
            .join(", ");
        let _ = write!(sql, " AND split_state IN ({params})");
    }

    if let Some(tags) = query.tags.as_ref() {
        sql.push_str(" AND (");
        sql.push_str(&tags_filter_expression_helper(tags));
        sql.push(')');
    }

    match query.time_range.start {
        Bound::Included(v) => {
            let _ = write!(
                sql,
                " AND (time_range_end >= {v} OR time_range_end IS NULL)"
            );
        }
        Bound::Excluded(v) => {
            let _ = write!(sql, " AND (time_range_end > {v} OR time_range_end IS NULL)");
        }
        Bound::Unbounded => {}
    };

    match query.time_range.end {
        Bound::Included(v) => {
            let _ = write!(
                sql,
                " AND (time_range_start <= {v} OR time_range_start IS NULL)"
            );
        }
        Bound::Excluded(v) => {
            let _ = write!(
                sql,
                " AND (time_range_start < {v} OR time_range_start IS NULL)"
            );
        }
        Bound::Unbounded => {}
    };

    match &query.mature {
        Bound::Included(evaluation_datetime) => {
            let _ = write!(
                sql,
                " AND (maturity_timestamp = to_timestamp(0) OR to_timestamp({}) >= \
                 maturity_timestamp)",
                evaluation_datetime.unix_timestamp()
            );
        }
        Bound::Excluded(evaluation_datetime) => {
            let _ = write!(
                sql,
                " AND to_timestamp({}) < maturity_timestamp",
                evaluation_datetime.unix_timestamp()
            );
        }
        Bound::Unbounded => {}
    }

    // WARNING: Not SQL injection proof
    write_sql_filter(
        &mut sql,
        "update_timestamp",
        &query.update_timestamp,
        |val| format!("to_timestamp({val})"),
    );
    write_sql_filter(
        &mut sql,
        "create_timestamp",
        &query.create_timestamp,
        |val| format!("to_timestamp({val})"),
    );
    write_sql_filter(&mut sql, "delete_opstamp", &query.delete_opstamp, |val| {
        val.to_string()
    });

    if let Some(limit) = query.limit {
        let _ = write!(sql, " LIMIT {limit}");
    }

    if let Some(offset) = query.offset {
        let _ = write!(sql, " OFFSET {offset}");
    }

    sql
}

/// Returns the unix timestamp at which the split becomes mature.
/// If the split is mature (`SplitMaturity::Mature`), we return 0
/// as we don't want the maturity to depend on datetime.
fn split_maturity_timestamp(split_metadata: &SplitMetadata) -> i64 {
    match split_metadata.maturity {
        SplitMaturity::Mature => 0,
        SplitMaturity::Immature { maturation_period } => {
            split_metadata.create_timestamp + maturation_period.as_secs() as i64
        }
    }
}

fn convert_sqlx_err(index_id: &str, sqlx_err: sqlx::Error) -> MetastoreError {
    match &sqlx_err {
        sqlx::Error::Database(boxed_db_err) => {
            let pg_db_error = boxed_db_err.downcast_ref::<PgDatabaseError>();
            let pg_error_code = pg_db_error.code();
            let pg_error_table = pg_db_error.table();

            match (pg_error_code, pg_error_table) {
                (pg_error_code::FOREIGN_KEY_VIOLATION, _) => {
                    MetastoreError::NotFound(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (pg_error_code::UNIQUE_VIOLATION, Some(table)) if table.starts_with("indexes") => {
                    MetastoreError::AlreadyExists(EntityKind::Index {
                        index_id: index_id.to_string(),
                    })
                }
                (pg_error_code::UNIQUE_VIOLATION, _) => {
                    error!(pg_db_err=?boxed_db_err, "postgresql-error");
                    MetastoreError::Internal {
                        message: "Unique key violation.".to_string(),
                        cause: format!("DB error {boxed_db_err:?}"),
                    }
                }
                _ => {
                    error!(pg_db_err=?boxed_db_err, "postgresql-error");
                    MetastoreError::Db {
                        message: boxed_db_err.to_string(),
                    }
                }
            }
        }
        _ => {
            error!(err=?sqlx_err, "An error has occurred in the database operation.");
            MetastoreError::Db {
                message: sqlx_err.to_string(),
            }
        }
    }
}

/// This macro is used to systematically wrap the metastore
/// into transaction, commit them on Result::Ok and rollback on Error.
///
/// Note this is suboptimal.
/// Some of the methods actually did not require a transaction.
///
/// We still use this macro for them in order to make the code
/// "trivially correct".
macro_rules! run_with_tx {
    ($connection_pool:expr, $tx_refmut:ident, $x:block) => {{
        let mut tx: Transaction<'_, Postgres> = $connection_pool.begin().await?;
        let $tx_refmut = &mut tx;
        let op_fut = move || async move { $x };
        let op_result: MetastoreResult<_> = op_fut().await;
        if op_result.is_ok() {
            debug!("commit");
            tx.commit().await?;
        } else {
            warn!("rollback");
            tx.rollback().await?;
        }
        op_result
    }};
}

async fn mutate_index_metadata<E, M: FnOnce(&mut IndexMetadata) -> Result<bool, E>>(
    tx: &mut Transaction<'_, Postgres>,
    index_uid: IndexUid,
    mutate_fn: M,
) -> MetastoreResult<bool>
where
    MetastoreError: From<E>,
{
    let index_id = index_uid.index_id();
    let mut index_metadata = index_metadata(tx, index_id).await?;
    if index_metadata.index_uid != index_uid {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    let mutation_occurred = mutate_fn(&mut index_metadata)?;
    if !mutation_occurred {
        return Ok(mutation_occurred);
    }
    let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|error| {
        MetastoreError::JsonSerializeError {
            struct_name: "IndexMetadata".to_string(),
            message: error.to_string(),
        }
    })?;
    let update_index_res = sqlx::query(
        r#"
        UPDATE indexes
        SET index_metadata_json = $1
        WHERE index_uid = $2
        "#,
    )
    .bind(index_metadata_json)
    .bind(index_uid.to_string())
    .execute(tx.as_mut())
    .await?;
    if update_index_res.rows_affected() == 0 {
        return Err(MetastoreError::NotFound(EntityKind::Index {
            index_id: index_id.to_string(),
        }));
    }
    Ok(mutation_occurred)
}

#[async_trait]
impl Metastore for PostgresqlMetastore {
    async fn check_connectivity(&self) -> anyhow::Result<()> {
        self.connection_pool.acquire().await?;
        Ok(())
    }

    #[instrument(skip(self))]
    async fn list_indexes_metadatas(
        &self,
        query: ListIndexesQuery,
    ) -> MetastoreResult<Vec<IndexMetadata>> {
        let sql = match query {
            ListIndexesQuery::All => "SELECT * FROM indexes".to_string(),
            ListIndexesQuery::IndexIdPatterns(index_id_patterns) => {
                build_index_id_patterns_sql_query(index_id_patterns).map_err(|error| {
                    MetastoreError::Internal {
                        message: "Failed to build `list_indexes_metadatas` SQL query".to_string(),
                        cause: error.to_string(),
                    }
                })?
            }
        };
        let pg_indexes = sqlx::query_as::<_, PgIndex>(&sql)
            .fetch_all(&self.connection_pool)
            .await?;
        pg_indexes
            .into_iter()
            .map(|pg_index| pg_index.index_metadata())
            .collect()
    }

    #[instrument(skip(self), fields(index_id=&index_config.index_id))]
    async fn create_index(&self, index_config: IndexConfig) -> MetastoreResult<IndexUid> {
        let index_metadata = IndexMetadata::new(index_config);
        let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "IndexMetadata".to_string(),
                message: error.to_string(),
            }
        })?;
        sqlx::query(
            "INSERT INTO indexes (index_uid, index_id, index_metadata_json) VALUES ($1, $2, $3)",
        )
        .bind(index_metadata.index_uid.to_string())
        .bind(index_metadata.index_uid.index_id())
        .bind(&index_metadata_json)
        .execute(&self.connection_pool)
        .await
        .map_err(|error| convert_sqlx_err(index_metadata.index_id(), error))?;
        Ok(index_metadata.index_uid)
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn delete_index(&self, index_uid: IndexUid) -> MetastoreResult<()> {
        let delete_res = sqlx::query("DELETE FROM indexes WHERE index_uid = $1")
            .bind(index_uid.to_string())
            .execute(&self.connection_pool)
            .await?;
        if delete_res.rows_affected() == 0 {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id().to_string(),
            }));
        }
        Ok(())
    }

    #[instrument(skip(self, split_metadata_list), fields(split_ids))]
    async fn stage_splits(
        &self,
        index_uid: IndexUid,
        split_metadata_list: Vec<SplitMetadata>,
    ) -> MetastoreResult<()> {
        let mut split_ids = Vec::with_capacity(split_metadata_list.len());
        let mut time_range_start_list = Vec::with_capacity(split_metadata_list.len());
        let mut time_range_end_list = Vec::with_capacity(split_metadata_list.len());
        let mut tags_list = Vec::with_capacity(split_metadata_list.len());
        let mut split_metadata_json_list = Vec::with_capacity(split_metadata_list.len());
        let mut delete_opstamps = Vec::with_capacity(split_metadata_list.len());
        let mut maturity_timestamps = Vec::with_capacity(split_metadata_list.len());

        for split_metadata in split_metadata_list {
            let split_metadata_json = serde_json::to_string(&split_metadata).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    struct_name: "SplitMetadata".to_string(),
                    message: error.to_string(),
                }
            })?;
            split_metadata_json_list.push(split_metadata_json);

            let time_range_start = split_metadata
                .time_range
                .as_ref()
                .map(|range| *range.start());
            time_range_start_list.push(time_range_start);
            maturity_timestamps.push(split_maturity_timestamp(&split_metadata));

            let time_range_end = split_metadata.time_range.map(|range| *range.end());
            time_range_end_list.push(time_range_end);

            let tags: Vec<String> = split_metadata.tags.into_iter().collect();
            tags_list.push(sqlx::types::Json(tags));
            split_ids.push(split_metadata.split_id);
            delete_opstamps.push(split_metadata.delete_opstamp as i64);
        }
        tracing::Span::current().record("split_ids", format!("{split_ids:?}"));

        run_with_tx!(self.connection_pool, tx, {
            let upserted_split_ids: Vec<String> = sqlx::query_scalar(r#"
                INSERT INTO splits
                    (split_id, time_range_start, time_range_end, tags, split_metadata_json, delete_opstamp, maturity_timestamp, split_state, index_uid)
                SELECT
                    split_id,
                    time_range_start,
                    time_range_end,
                    ARRAY(SELECT json_array_elements_text(tags_json::json)) as tags,
                    split_metadata_json,
                    delete_opstamp,
                    to_timestamp(maturity_timestamp),
                    $8 as split_state,
                    $9 as index_uid
                FROM
                    UNNEST($1, $2, $3, $4, $5, $6, $7)
                    as tr(split_id, time_range_start, time_range_end, tags_json, split_metadata_json, delete_opstamp, maturity_timestamp)
                ON CONFLICT(split_id) DO UPDATE
                    SET
                        time_range_start = excluded.time_range_start,
                        time_range_end = excluded.time_range_end,
                        tags = excluded.tags,
                        split_metadata_json = excluded.split_metadata_json,
                        delete_opstamp = excluded.delete_opstamp,
                        maturity_timestamp = excluded.maturity_timestamp,
                        index_uid = excluded.index_uid,
                        update_timestamp = CURRENT_TIMESTAMP,
                        create_timestamp = CURRENT_TIMESTAMP
                    WHERE splits.split_id = excluded.split_id AND splits.split_state = 'Staged'
                RETURNING split_id;
                "#)
                .bind(&split_ids)
                .bind(time_range_start_list)
                .bind(time_range_end_list)
                .bind(tags_list)
                .bind(split_metadata_json_list)
                .bind(delete_opstamps)
                .bind(maturity_timestamps)
                .bind(SplitState::Staged.as_str())
                .bind(index_uid.to_string())
                .fetch_all(tx.as_mut())
                .await
                .map_err(|error| convert_sqlx_err(index_uid.index_id(), error))?;

            if upserted_split_ids.len() != split_ids.len() {
                let failed_split_ids: Vec<String> = split_ids
                    .into_iter()
                    .filter(|split_id| !upserted_split_ids.contains(split_id))
                    .collect();
                let entity = EntityKind::Splits {
                    split_ids: failed_split_ids,
                };
                let message = "splits are not staged".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }

            debug!(index_id=%index_uid.index_id(), num_splits=split_ids.len(), "Splits successfully staged.");

            Ok(())
        })
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn publish_splits<'a>(
        &self,
        index_uid: IndexUid,
        staged_split_ids: &[&'a str],
        replaced_split_ids: &[&'a str],
        checkpoint_delta_opt: Option<IndexCheckpointDelta>,
        _publish_token_opt: Option<PublishToken>,
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            let mut index_metadata = index_metadata(tx, index_uid.index_id()).await?;
            if index_metadata.index_uid != index_uid {
                return Err(MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_uid.index_id().to_string(),
                }));
            }
            if let Some(checkpoint_delta) = checkpoint_delta_opt {
                let source_id = checkpoint_delta.source_id.clone();
                index_metadata
                    .checkpoint
                    .try_apply_delta(checkpoint_delta)
                    .map_err(|error| {
                        let entity = EntityKind::CheckpointDelta {
                            index_id: index_uid.index_id().to_string(),
                            source_id,
                        };
                        let message = error.to_string();
                        MetastoreError::FailedPrecondition { entity, message }
                    })?;
            }
            let index_metadata_json = serde_json::to_string(&index_metadata).map_err(|error| {
                MetastoreError::JsonSerializeError {
                    struct_name: "IndexMetadata".to_string(),
                    message: error.to_string(),
                }
            })?;

            const PUBLISH_SPLITS_QUERY: &str = r#"
            -- Select the splits to update, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, input_splits.expected_split_state, splits.actual_split_state
                FROM (
                    SELECT split_id, 'Staged' AS expected_split_state
                    FROM UNNEST($3) AS staged_splits(split_id)
                    UNION
                    SELECT split_id, 'Published' AS expected_split_state
                    FROM UNNEST($4) AS published_splits(split_id)
                ) input_splits
                LEFT JOIN (
                    SELECT split_id, split_state AS actual_split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND (split_id = ANY($3) OR split_id = ANY($4))
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Update the index metadata with the new checkpoint.
            updated_index_metadata AS (
                UPDATE indexes
                SET
                    index_metadata_json = $2
                WHERE
                    index_uid = $1
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            actual_split_state != expected_split_state
                        )
            ),
            -- Publish the staged splits and mark the published splits for deletion.
            updated_splits AS (
                UPDATE splits
                SET
                    split_state = CASE split_state
                        WHEN 'Staged' THEN 'Published'
                        ELSE 'MarkedForDeletion'
                    END,
                    update_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC'),
                    publish_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                FROM input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            actual_split_state != expected_split_state
                    )
            )
            -- Report the outcome of the update query.
            SELECT
                COUNT(1) FILTER (WHERE actual_split_state = 'Staged' AND expected_split_state = 'Staged'),
                COUNT(1) FILTER (WHERE actual_split_state = 'Published' AND expected_split_state = 'Published'),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state IS NULL), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state != 'Staged' AND expected_split_state = 'Staged'), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE actual_split_state != 'Published' AND expected_split_state = 'Published'), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
            let (
                num_published_splits,
                num_marked_splits,
                not_found_split_ids,
                not_staged_split_ids,
                not_marked_split_ids,
            ): (i64, i64, Vec<String>, Vec<String>, Vec<String>) =
                sqlx::query_as(PUBLISH_SPLITS_QUERY)
                    .bind(index_uid.to_string())
                    .bind(index_metadata_json)
                    .bind(staged_split_ids)
                    .bind(replaced_split_ids)
                    .fetch_one(tx.as_mut())
                    .await
                    .map_err(|error| convert_sqlx_err(index_uid.index_id(), error))?;

            if !not_found_split_ids.is_empty() {
                return Err(MetastoreError::NotFound(EntityKind::Splits {
                    split_ids: not_found_split_ids,
                }));
            }
            if !not_staged_split_ids.is_empty() {
                let entity = EntityKind::Splits {
                    split_ids: not_staged_split_ids,
                };
                let message = "splits are not staged".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }
            if !not_marked_split_ids.is_empty() {
                let entity = EntityKind::Splits {
                    split_ids: not_marked_split_ids,
                };
                let message = "splits are not marked for deletion".to_string();
                return Err(MetastoreError::FailedPrecondition { entity, message });
            }
            info!(
                index_id=%index_uid.index_id(),
                "Published {} splits and marked {} splits for deletion successfully.",
                num_published_splits, num_marked_splits
            );
            Ok(())
        })
    }

    #[instrument(skip(self), fields(index_uids=query.index_uids.iter().join(",")))]
    async fn list_splits(&self, query: ListSplitsQuery) -> MetastoreResult<Vec<Split>> {
        let sql_base = "SELECT * FROM splits".to_string();
        let sql = build_query_filter(sql_base, &query);

        let pg_splits = sqlx::query_as::<_, PgSplit>(&sql)
            .fetch_all(&self.connection_pool)
            .await?;

        // If no splits were returned, maybe some indexes do not exist in the first place?
        // TODO: the file-backed metastore is more accurate as it checks for index existence before
        // returning splits. We could do the same here or remove index existence check `list_splits`
        // for all metastore implementations.
        if pg_splits.is_empty() {
            let index_ids_str: Vec<String> = query
                .index_uids
                .iter()
                .map(|index_uid| index_uid.index_id().to_string())
                .collect();
            let found_index_ids: HashSet<String> = self
                .list_indexes_metadatas(ListIndexesQuery::IndexIdPatterns(index_ids_str.clone()))
                .await?
                .into_iter()
                .map(|index_metadata| index_metadata.index_id().to_string())
                .collect();
            let not_found_index_ids: Vec<String> = index_ids_str
                .into_iter()
                .filter(|index_id| !found_index_ids.contains(index_id))
                .collect();
            if !not_found_index_ids.is_empty() {
                return Err(MetastoreError::NotFound(EntityKind::Indexes {
                    index_ids: not_found_index_ids,
                }));
            }
        }
        pg_splits
            .into_iter()
            .map(|pg_split| pg_split.try_into())
            .collect()
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn mark_splits_for_deletion<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        const MARK_SPLITS_FOR_DELETION_QUERY: &str = r#"
            -- Select the splits to update, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, splits.split_state
                FROM UNNEST($2) AS input_splits(split_id)
                LEFT JOIN (
                    SELECT split_id, split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND split_id = ANY($2)
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Mark the staged and published splits for deletion.
            marked_splits AS (
                UPDATE splits
                SET
                    split_state = 'MarkedForDeletion',
                    update_timestamp = (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                FROM input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND splits.split_state IN ('Staged', 'Published')
            )
            -- Report the outcome of the update query.
            SELECT
                COUNT(split_state),
                COUNT(1) FILTER (WHERE split_state IN ('Staged', 'Published')),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IS NULL), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
        let (num_found_splits, num_marked_splits, not_found_split_ids): (i64, i64, Vec<String>) =
            sqlx::query_as(MARK_SPLITS_FOR_DELETION_QUERY)
                .bind(index_uid.to_string())
                .bind(split_ids)
                .fetch_one(&self.connection_pool)
                .await
                .map_err(|error| convert_sqlx_err(index_uid.index_id(), error))?;

        if num_found_splits == 0
            && index_opt(&self.connection_pool, index_uid.index_id())
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id().to_string(),
            }));
        }
        info!(
            index_id=%index_uid.index_id(),
            "Marked {} splits for deletion, among which {} were newly marked.",
            split_ids.len() - not_found_split_ids.len(),
            num_marked_splits
        );
        if !not_found_split_ids.is_empty() {
            warn!(
                index_id=%index_uid.index_id(),
                split_ids=?PrettySample::new(&not_found_split_ids, 5),
                "{} splits were not found and could not be marked for deletion.",
                not_found_split_ids.len()
            );
        }
        Ok(())
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn delete_splits<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
    ) -> MetastoreResult<()> {
        const DELETE_SPLITS_QUERY: &str = r#"
            -- Select the splits to delete, regardless of their state.
            -- The left join make it possible to identify the splits that do not exist.
            WITH input_splits AS (
                SELECT input_splits.split_id, splits.split_state
                FROM UNNEST($2) AS input_splits(split_id)
                LEFT JOIN (
                    SELECT split_id, split_state
                    FROM splits
                    WHERE
                        index_uid = $1
                        AND split_id = ANY($2)
                    FOR UPDATE
                    ) AS splits
                USING (split_id)
            ),
            -- Delete the splits if and only if all the splits are marked for deletion.
            deleted_splits AS (
                DELETE FROM splits
                USING input_splits
                WHERE
                    splits.index_uid = $1
                    AND splits.split_id = input_splits.split_id
                    AND NOT EXISTS (
                        SELECT 1
                        FROM input_splits
                        WHERE
                            split_state IN ('Staged', 'Published')
                    )
            )
            -- Report the outcome of the delete query.
            SELECT
                COUNT(split_state),
                COUNT(1) FILTER (WHERE split_state = 'MarkedForDeletion'),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IN ('Staged', 'Published')), ARRAY[]::TEXT[]),
                COALESCE(ARRAY_AGG(split_id) FILTER (WHERE split_state IS NULL), ARRAY[]::TEXT[])
                FROM input_splits
        "#;
        let (num_found_splits, num_deleted_splits, not_deletable_split_ids, not_found_split_ids): (
            i64,
            i64,
            Vec<String>,
            Vec<String>,
        ) = sqlx::query_as(DELETE_SPLITS_QUERY)
            .bind(index_uid.to_string())
            .bind(split_ids)
            .fetch_one(&self.connection_pool)
            .await
            .map_err(|error| convert_sqlx_err(index_uid.index_id(), error))?;

        if num_found_splits == 0
            && index_opt_for_uid(&self.connection_pool, index_uid.clone())
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id().to_string(),
            }));
        }
        if !not_deletable_split_ids.is_empty() {
            let message = format!(
                "splits `{}` are not deletable",
                not_deletable_split_ids.join(", ")
            );
            let entity = EntityKind::Splits {
                split_ids: not_deletable_split_ids,
            };
            return Err(MetastoreError::FailedPrecondition { entity, message });
        }
        info!(index_id=%index_uid.index_id(), "Deleted {} splits from index.", num_deleted_splits);

        if !not_found_split_ids.is_empty() {
            warn!(
                index_id=%index_uid.index_id(),
                split_ids=?PrettySample::new(&not_found_split_ids, 5),
                "{} splits were not found and could not be deleted.",
                not_found_split_ids.len()
            );
        }
        Ok(())
    }

    #[instrument(skip(self), fields(index_id=index_id))]
    async fn index_metadata(&self, index_id: &str) -> MetastoreResult<IndexMetadata> {
        index_opt(&self.connection_pool, index_id)
            .await?
            .ok_or_else(|| {
                MetastoreError::NotFound(EntityKind::Index {
                    index_id: index_id.to_string(),
                })
            })?
            .index_metadata()
    }

    #[instrument(skip(self, source), fields(index_id=index_uid.index_id(), source_id=source.source_id))]
    async fn add_source(&self, index_uid: IndexUid, source: SourceConfig) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata::<MetastoreError, _>(
                tx,
                index_uid,
                |index_metadata: &mut IndexMetadata| {
                    index_metadata.add_source(source)?;
                    Ok(true)
                },
            )
            .await?;
            Ok(())
        })
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id(), source_id=source_id))]
    async fn toggle_source(
        &self,
        index_uid: IndexUid,
        source_id: &str,
        enable: bool,
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata(tx, index_uid, |index_metadata| {
                index_metadata.toggle_source(source_id, enable)
            })
            .await?;
            Ok(())
        })
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id(), source_id=source_id))]
    async fn delete_source(&self, index_uid: IndexUid, source_id: &str) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata(tx, index_uid, |index_metadata| {
                index_metadata.delete_source(source_id)
            })
            .await?;
            Ok(())
        })
    }

    #[instrument(skip(self), fields(index_id=index_uid.index_id(), source_id=source_id))]
    async fn reset_source_checkpoint(
        &self,
        index_uid: IndexUid,
        source_id: &str,
    ) -> MetastoreResult<()> {
        run_with_tx!(self.connection_pool, tx, {
            mutate_index_metadata(tx, index_uid, |index_metadata| {
                Ok::<_, MetastoreError>(index_metadata.checkpoint.reset_source(source_id))
            })
            .await?;
            Ok(())
        })
    }

    fn uri(&self) -> &Uri {
        &self.uri
    }

    /// Retrieves the last delete opstamp for a given `index_id`.
    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn last_delete_opstamp(&self, index_uid: IndexUid) -> MetastoreResult<u64> {
        let max_opstamp: i64 = sqlx::query_scalar(
            r#"
            SELECT COALESCE(MAX(opstamp), 0)
            FROM delete_tasks
            WHERE index_uid = $1
            "#,
        )
        .bind(index_uid.to_string())
        .fetch_one(&self.connection_pool)
        .await
        .map_err(|error| MetastoreError::Db {
            message: error.to_string(),
        })?;

        Ok(max_opstamp as u64)
    }

    /// Creates a delete task from a delete query.
    #[instrument(skip(self), fields(index_id=IndexUid::from(delete_query.index_uid.to_string()).index_id()))]
    async fn create_delete_task(&self, delete_query: DeleteQuery) -> MetastoreResult<DeleteTask> {
        let delete_query_json = serde_json::to_string(&delete_query).map_err(|error| {
            MetastoreError::JsonSerializeError {
                struct_name: "DeleteQuery".to_string(),
                message: error.to_string(),
            }
        })?;
        let (create_timestamp, opstamp): (sqlx::types::time::PrimitiveDateTime, i64) =
            sqlx::query_as(
                r#"
            INSERT INTO delete_tasks (index_uid, delete_query_json) VALUES ($1, $2)
            RETURNING create_timestamp, opstamp
            "#,
            )
            .bind(delete_query.index_uid.to_string())
            .bind(&delete_query_json)
            .fetch_one(&self.connection_pool)
            .await
            .map_err(|error| {
                convert_sqlx_err(
                    IndexUid::from(delete_query.index_uid.to_string()).index_id(),
                    error,
                )
            })?;

        Ok(DeleteTask {
            create_timestamp: create_timestamp.assume_utc().unix_timestamp(),
            opstamp: opstamp as u64,
            delete_query: Some(delete_query),
        })
    }

    /// Update splits delete opstamps.
    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn update_splits_delete_opstamp<'a>(
        &self,
        index_uid: IndexUid,
        split_ids: &[&'a str],
        delete_opstamp: u64,
    ) -> MetastoreResult<()> {
        if split_ids.is_empty() {
            return Ok(());
        }
        let update_res = sqlx::query(
            r#"
            UPDATE splits
            SET
                delete_opstamp = $1,
                -- The values we compare with are *before* the modification:
                update_timestamp = CASE
                    WHEN delete_opstamp != $1 THEN (CURRENT_TIMESTAMP AT TIME ZONE 'UTC')
                    ELSE update_timestamp
                END
            WHERE
                index_uid = $2
                AND split_id = ANY($3)
            "#,
        )
        .bind(delete_opstamp as i64)
        .bind(index_uid.to_string())
        .bind(split_ids)
        .execute(&self.connection_pool)
        .await?;

        // If no splits were updated, maybe the index does not exist in the first place?
        if update_res.rows_affected() == 0
            && index_opt_for_uid(&self.connection_pool, index_uid.clone())
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id().to_string(),
            }));
        }
        Ok(())
    }

    /// Lists the delete tasks with opstamp > `opstamp_start`.
    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn list_delete_tasks(
        &self,
        index_uid: IndexUid,
        opstamp_start: u64,
    ) -> MetastoreResult<Vec<DeleteTask>> {
        let pg_delete_tasks: Vec<PgDeleteTask> = sqlx::query_as::<_, PgDeleteTask>(
            r#"
                SELECT * FROM delete_tasks
                WHERE
                    index_uid = $1
                    AND opstamp > $2
                "#,
        )
        .bind(index_uid.to_string())
        .bind(opstamp_start as i64)
        .fetch_all(&self.connection_pool)
        .await?;
        pg_delete_tasks
            .into_iter()
            .map(|pg_delete_task| pg_delete_task.try_into())
            .collect()
    }

    /// Returns `num_splits` published splits with `split.delete_opstamp` < `delete_opstamp`.
    /// Results are ordered by ascending `split.delete_opstamp` and `split.publish_timestamp`
    /// values.
    #[instrument(skip(self), fields(index_id=index_uid.index_id()))]
    async fn list_stale_splits(
        &self,
        index_uid: IndexUid,
        delete_opstamp: u64,
        num_splits: usize,
    ) -> MetastoreResult<Vec<Split>> {
        let pg_stale_splits: Vec<PgSplit> = sqlx::query_as::<_, PgSplit>(
            r#"
                SELECT *
                FROM splits
                WHERE
                    index_uid = $1
                    AND delete_opstamp < $2
                    AND split_state = $3
                    AND (maturity_timestamp = to_timestamp(0) OR (CURRENT_TIMESTAMP AT TIME ZONE 'UTC') >= maturity_timestamp)
                ORDER BY delete_opstamp ASC, publish_timestamp ASC
                LIMIT $4
                "#,
        )
        .bind(index_uid.to_string())
        .bind(delete_opstamp as i64)
        .bind(SplitState::Published.as_str())
        .bind(num_splits as i64)
        .fetch_all(&self.connection_pool)
        .await?;

        // If no splits were returned, maybe the index does not exist in the first place?
        if pg_stale_splits.is_empty()
            && index_opt_for_uid(&self.connection_pool, index_uid.clone())
                .await?
                .is_none()
        {
            return Err(MetastoreError::NotFound(EntityKind::Index {
                index_id: index_uid.index_id().to_string(),
            }));
        }
        pg_stale_splits
            .into_iter()
            .map(|pg_split| pg_split.try_into())
            .collect()
    }
}

// We use dollar-quoted strings in Postgresql.
//
// In order to ensure that we do not risk SQL injection,
// we need to generate a string that does not appear in
// the literal we want to dollar quote.
fn generate_dollar_guard(s: &str) -> String {
    if !s.contains('$') {
        // That's our happy path here.
        return String::new();
    }
    let mut dollar_guard = String::new();
    loop {
        dollar_guard.push_str("QuickwitGuard");
        // This terminates because `dollar_guard`
        // will eventually be longer than s.
        if !s.contains(&dollar_guard) {
            return dollar_guard;
        }
    }
}

/// Takes a tag filters AST and returns a sql expression that can be used as
/// a filter.
fn tags_filter_expression_helper(tags: &TagFilterAst) -> String {
    match tags {
        TagFilterAst::And(child_asts) => {
            if child_asts.is_empty() {
                return "TRUE".to_string();
            }
            let expr_without_parenthesis = child_asts
                .iter()
                .map(tags_filter_expression_helper)
                .join(" AND ");
            format!("({expr_without_parenthesis})")
        }
        TagFilterAst::Or(child_asts) => {
            if child_asts.is_empty() {
                return "TRUE".to_string();
            }
            let expr_without_parenthesis = child_asts
                .iter()
                .map(tags_filter_expression_helper)
                .join(" OR ");
            format!("({expr_without_parenthesis})")
        }
        TagFilterAst::Tag { is_present, tag } => {
            let dollar_guard = generate_dollar_guard(tag);
            if *is_present {
                format!("${dollar_guard}${tag}${dollar_guard}$ = ANY(tags)")
            } else {
                format!("NOT (${dollar_guard}${tag}${dollar_guard}$ = ANY(tags))")
            }
        }
    }
}

/// Builds a SQL query that returns indexes which match at least one pattern in
/// `index_id_patterns`. For each pattern, we check if the pattern is valid and replace `*` by `%`
/// to build a SQL `LIKE` query.
fn build_index_id_patterns_sql_query(index_id_patterns: Vec<String>) -> anyhow::Result<String> {
    assert!(!index_id_patterns.is_empty());
    if index_id_patterns.iter().any(|pattern| pattern == "*") {
        return Ok("SELECT * FROM indexes".to_string());
    }
    let mut where_like_query = String::new();
    for (index_id_pattern_idx, index_id_pattern) in index_id_patterns.iter().enumerate() {
        validate_index_id_pattern(index_id_pattern).map_err(|error| MetastoreError::Internal {
            message: "Failed to build list indexes query".to_string(),
            cause: error.to_string(),
        })?;
        if index_id_pattern.contains('*') {
            let sql_pattern = index_id_pattern.replace('*', "%");
            let _ = write!(where_like_query, "index_id LIKE '{sql_pattern}'");
        } else {
            let _ = write!(where_like_query, "index_id = '{index_id_pattern}'");
        }
        if index_id_pattern_idx < index_id_patterns.len() - 1 {
            where_like_query.push_str(" OR ");
        }
    }
    Ok(format!("SELECT * FROM indexes WHERE {where_like_query}"))
}

/// A postgres metastore factory
#[derive(Clone, Default)]
pub struct PostgresqlMetastoreFactory {
    // In a normal run, this cache will contain a single Metastore.
    //
    // In contrast to the file backe metastore, we use a strong pointer here, so that Metastore
    // doesn't get dropped. This is done in order to keep the underlying connection pool to
    // postgres alive.
    cache: Arc<Mutex<HashMap<Uri, Arc<dyn Metastore>>>>,
}

impl PostgresqlMetastoreFactory {
    async fn get_from_cache(&self, uri: &Uri) -> Option<Arc<dyn Metastore>> {
        let cache_lock = self.cache.lock().await;
        cache_lock.get(uri).map(Arc::clone)
    }

    /// If there is a valid entry in the cache to begin with, we trash the new
    /// one and return the old one.
    ///
    /// This way we make sure that we keep only one instance associated
    /// to the key `uri` outside of this struct.
    async fn cache_metastore(&self, uri: Uri, metastore: Arc<dyn Metastore>) -> Arc<dyn Metastore> {
        let mut cache_lock = self.cache.lock().await;
        if let Some(metastore) = cache_lock.get(&uri) {
            return metastore.clone();
        }
        cache_lock.insert(uri, metastore.clone());
        metastore
    }
}

#[async_trait]
impl MetastoreFactory for PostgresqlMetastoreFactory {
    fn backend(&self) -> MetastoreBackend {
        MetastoreBackend::PostgreSQL
    }

    async fn resolve(
        &self,
        metastore_config: &MetastoreConfig,
        uri: &Uri,
    ) -> Result<Arc<dyn Metastore>, MetastoreResolverError> {
        if let Some(metastore) = self.get_from_cache(uri).await {
            debug!("using metastore from cache");
            return Ok(metastore);
        }
        debug!("metastore not found in cache");
        let postgresql_metastore_config = metastore_config.as_postgres().ok_or_else(|| {
            let message = format!(
                "Expected PostgreSQL metastore config, got `{:?}`.",
                metastore_config.backend()
            );
            MetastoreResolverError::InvalidConfig(message)
        })?;
        let postgresql_metastore = PostgresqlMetastore::new(postgresql_metastore_config, uri)
            .await
            .map_err(MetastoreResolverError::Initialization)?;
        let instrumented_metastore = InstrumentedMetastore::new(Box::new(postgresql_metastore));
        let unique_metastore_for_uri = self
            .cache_metastore(uri.clone(), Arc::new(instrumented_metastore))
            .await;
        Ok(unique_metastore_for_uri)
    }
}

#[cfg(test)]
#[async_trait]
impl crate::tests::test_suite::DefaultForTest for PostgresqlMetastore {
    async fn default_for_test() -> Self {
        // We cannot use a singleton here,
        // because sqlx needs the runtime used to create a connection to
        // not being dropped.
        //
        // Each unit test runs its own tokio Runtime, so a singleton would mean
        // tying the connection pool to the runtime of one unit test.
        // Concretely this results in a "IO driver has terminated"
        // once the first unit test finishes and its runtime is dropped.
        //
        // The number of connections to Postgres should not be
        // too catastrophic, as it is limited by the number of concurrent
        // unit tests running (= number of test-threads).
        dotenv::dotenv().ok();
        let uri: Uri = std::env::var("QW_TEST_DATABASE_URL")
            .expect("Environment variable `QW_TEST_DATABASE_URL` should be set.")
            .parse()
            .expect("Environment variable `QW_TEST_DATABASE_URL` should be a valid URI.");
        PostgresqlMetastore::new(&PostgresMetastoreConfig::default(), &uri)
            .await
            .expect("Failed to initialize test PostgreSQL metastore.")
    }
}

metastore_test_suite!(crate::PostgresqlMetastore);

#[cfg(test)]
mod tests {
    use quickwit_doc_mapper::tag_pruning::{no_tag, tag, TagFilterAst};
    use quickwit_proto::IndexUid;
    use time::OffsetDateTime;

    use super::{build_query_filter, tags_filter_expression_helper};
    use crate::metastore::postgresql_metastore::build_index_id_patterns_sql_query;
    use crate::{ListSplitsQuery, SplitState};

    fn test_tags_filter_expression_helper(tags_ast: TagFilterAst, expected: &str) {
        assert_eq!(tags_filter_expression_helper(&tags_ast), expected);
    }

    #[test]
    fn test_tags_filter_expression_single_tag() {
        let tags_ast = tag("my_field:titi");
        test_tags_filter_expression_helper(tags_ast, r#"$$my_field:titi$$ = ANY(tags)"#);
    }

    #[test]
    fn test_tags_filter_expression_not_tag() {
        test_tags_filter_expression_helper(
            no_tag("my_field:titi"),
            r#"NOT ($$my_field:titi$$ = ANY(tags))"#,
        );
    }

    #[test]
    fn test_tags_filter_expression_ands() {
        let tags_ast = TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2"), tag("tag:val3")]);
        test_tags_filter_expression_helper(
            tags_ast,
            "($$tag:val1$$ = ANY(tags) AND $$tag:val2$$ = ANY(tags) AND $$tag:val3$$ = ANY(tags))",
        );
    }

    #[test]
    fn test_tags_filter_expression_and_or() {
        let tags_ast = TagFilterAst::Or(vec![
            TagFilterAst::And(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);
        test_tags_filter_expression_helper(
            tags_ast,
            "(($$tag:val1$$ = ANY(tags) AND $$tag:val2$$ = ANY(tags)) OR $$tag:val3$$ = ANY(tags))",
        );
    }

    #[test]
    fn test_tags_filter_expression_and_or_correct_parenthesis() {
        let tags_ast = TagFilterAst::And(vec![
            TagFilterAst::Or(vec![tag("tag:val1"), tag("tag:val2")]),
            tag("tag:val3"),
        ]);
        test_tags_filter_expression_helper(
            tags_ast,
            r#"(($$tag:val1$$ = ANY(tags) OR $$tag:val2$$ = ANY(tags)) AND $$tag:val3$$ = ANY(tags))"#,
        );
    }

    #[test]
    fn test_tags_sql_injection_attempt() {
        let tags_ast = tag("tag:$$;DELETE FROM something_evil");
        test_tags_filter_expression_helper(
            tags_ast,
            "$QuickwitGuard$tag:$$;DELETE FROM something_evil$QuickwitGuard$ = ANY(tags)",
        );
        let tags_ast = tag("tag:$QuickwitGuard$;DELETE FROM something_evil");
        test_tags_filter_expression_helper(
            tags_ast,
            "$QuickwitGuardQuickwitGuard$tag:$QuickwitGuard$;DELETE FROM \
             something_evil$QuickwitGuardQuickwitGuard$ = ANY(tags)",
        );
    }

    #[test]
    fn test_single_sql_query_builder() {
        let index_uid = IndexUid::new("test-index");
        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Staged);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND split_state IN ('Staged')")
        );

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_split_state(SplitState::Published);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND split_state IN ('Published')")
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_split_states([SplitState::Published, SplitState::MarkedForDeletion]);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND split_state IN ('Published', \
                 'MarkedForDeletion')"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_update_timestamp_lt(51);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND update_timestamp < to_timestamp(51)")
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_create_timestamp_lte(55);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND create_timestamp <= to_timestamp(55)")
        );

        let maturity_evaluation_datetime = OffsetDateTime::from_unix_timestamp(55).unwrap();
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_mature(maturity_evaluation_datetime);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND (maturity_timestamp = to_timestamp(0) OR \
                 to_timestamp(55) >= maturity_timestamp)"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .retain_immature(maturity_evaluation_datetime);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND to_timestamp(55) < maturity_timestamp")
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_delete_opstamp_gte(4);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND delete_opstamp >= 4")
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_start_gt(45);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND (time_range_end > 45 OR time_range_end IS \
                 NULL)"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone()).with_time_range_end_lt(45);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND (time_range_start < 45 OR \
                 time_range_start IS NULL)"
            )
        );

        let query =
            ListSplitsQuery::for_index(index_uid.clone()).with_tags_filter(TagFilterAst::Tag {
                is_present: false,
                tag: "tag-2".to_string(),
            });
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}') AND (NOT ($$tag-2$$ = ANY(tags)))")
        );
    }

    #[test]
    fn test_combination_sql_query_builder() {
        let index_uid = IndexUid::new("test-index");
        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(0)
            .with_time_range_end_lt(40);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND (time_range_end > 0 OR time_range_end IS \
                 NULL) AND (time_range_start < 40 OR time_range_start IS NULL)"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(45)
            .with_delete_opstamp_gt(0);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND (time_range_end > 45 OR time_range_end IS \
                 NULL) AND delete_opstamp > 0"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_update_timestamp_lt(51)
            .with_create_timestamp_lte(63);
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND update_timestamp < to_timestamp(51) AND \
                 create_timestamp <= to_timestamp(63)"
            )
        );

        let query = ListSplitsQuery::for_index(index_uid.clone())
            .with_time_range_start_gt(90)
            .with_tags_filter(TagFilterAst::Tag {
                is_present: true,
                tag: "tag-1".to_string(),
            });
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(
                " WHERE (index_uid = '{index_uid}') AND ($$tag-1$$ = ANY(tags)) AND \
                 (time_range_end > 90 OR time_range_end IS NULL)"
            )
        );

        let index_uid_2 = IndexUid::new("test-index-2");
        let query =
            ListSplitsQuery::try_from_index_uids(vec![index_uid.clone(), index_uid_2.clone()])
                .unwrap();
        let sql = build_query_filter(String::new(), &query);
        assert_eq!(
            sql,
            format!(" WHERE (index_uid = '{index_uid}' OR index_uid = '{index_uid_2}')")
        );
    }

    #[test]
    fn test_index_id_pattern_like_query() {
        assert_eq!(
            &build_index_id_patterns_sql_query(vec!["*-index-*-last*".to_string()]).unwrap(),
            "SELECT * FROM indexes WHERE index_id LIKE '%-index-%-last%'"
        );
        assert_eq!(
            &build_index_id_patterns_sql_query(vec![
                "*-index-*-last*".to_string(),
                "another-index".to_string()
            ])
            .unwrap(),
            "SELECT * FROM indexes WHERE index_id LIKE '%-index-%-last%' OR index_id = \
             'another-index'"
        );
        assert_eq!(
            &build_index_id_patterns_sql_query(vec![
                "*-index-*-last**".to_string(),
                "another-index".to_string(),
                "*".to_string()
            ])
            .unwrap(),
            "SELECT * FROM indexes"
        );
        assert_eq!(
            build_index_id_patterns_sql_query(vec!["*-index-*-&-last**".to_string()])
                .unwrap_err()
                .to_string(),
            "Internal error: Failed to build list indexes query Cause: `Index ID pattern \
             `*-index-*-&-last**` is invalid. Patterns must match the following regular \
             expression: `^[a-zA-Z\\*][a-zA-Z0-9-_\\.\\*]{0,254}$`.`."
        );
    }
}
