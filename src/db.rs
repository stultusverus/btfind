use crate::{
    config::DatabaseConfig,
    store::{Error, Store},
};
use clap::Subcommand;
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
    }
    println!("database operation completed");
    Ok(())
}
