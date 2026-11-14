use crate::{
    config::DatabaseConfig,
    store::{Error, Store},
};
use clap::Subcommand;
use sqlx::Row;
#[derive(Subcommand)]
pub enum DbCommand {
    /// Apply versioned schema migrations
    Migrate,
    /// Check schema and search configuration
    Check,
    /// Recompute catalog totals while services are stopped
    Reconcile,
    /// Rebuild derived ngram documents while services are stopped
    RebuildSearch,
    /// Refresh optimizer and full-text statistics after restoring a dump
    Analyze,
    /// Report estimated table usage and optional physical tablespace allocation
    Storage {
        /// Include InnoDB tablespaces and full-text auxiliaries (requires PROCESS)
        #[arg(long)]
        physical: bool,
    },
}
pub async fn run(config: &DatabaseConfig, command: DbCommand) -> Result<(), Error> {
    let store = Store::connect_with_timeout(config, false, 0).await?;
    match command {
        DbCommand::Migrate => store.migrate().await?,
        DbCommand::Check => store.check().await?,
        DbCommand::Reconcile => {
            store.check().await?;
            store.reconcile().await?;
        }
        DbCommand::RebuildSearch => {
            store.check().await?;
            store.rebuild_search_index().await?;
        }
        DbCommand::Analyze => {
            store.check().await?;
            store.analyze().await?;
        }
        DbCommand::Storage { physical } => {
            store.check().await?;
            let rows = sqlx::query("SELECT CAST(TABLE_NAME AS CHAR) AS TABLE_NAME, TABLE_ROWS, DATA_LENGTH, INDEX_LENGTH, DATA_FREE FROM information_schema.TABLES WHERE TABLE_SCHEMA=DATABASE() ORDER BY TABLE_NAME")
                .fetch_all(&store.pool).await?;
            let tables = rows
                .iter()
                .map(|row| {
                    Ok(serde_json::json!({
                        "table": row.try_get::<String, _>("TABLE_NAME")?,
                        "estimated_rows": row.try_get::<Option<u64>, _>("TABLE_ROWS")?,
                        "data_bytes": row.try_get::<Option<u64>, _>("DATA_LENGTH")?,
                        "index_bytes": row.try_get::<Option<u64>, _>("INDEX_LENGTH")?,
                        "free_bytes": row.try_get::<Option<u64>, _>("DATA_FREE")?,
                    }))
                })
                .collect::<Result<Vec<_>, Error>>()?;
            let spaces = if physical {
                let rows = sqlx::query("SELECT CAST(NAME AS CHAR) AS NAME, CAST(SPACE_TYPE AS CHAR) AS SPACE_TYPE, FILE_SIZE, ALLOCATED_SIZE FROM information_schema.INNODB_TABLESPACES WHERE SUBSTRING_INDEX(NAME,'/',1)=DATABASE() ORDER BY NAME")
                    .fetch_all(&store.pool).await?;
                Some(
                    rows.iter()
                        .map(|row| {
                            Ok(serde_json::json!({
                                "name": row.try_get::<String, _>("NAME")?,
                                "space_type": row.try_get::<String, _>("SPACE_TYPE")?,
                                "file_bytes": row.try_get::<u64, _>("FILE_SIZE")?,
                                "allocated_bytes": row.try_get::<u64, _>("ALLOCATED_SIZE")?,
                            }))
                        })
                        .collect::<Result<Vec<_>, Error>>()?,
                )
            } else {
                None
            };
            println!(
                "{}",
                serde_json::json!({"captured_at": crate::store::now(), "tables": tables, "tablespaces": spaces})
            );
            return Ok(());
        }
    }
    println!("database operation completed");
    Ok(())
}
