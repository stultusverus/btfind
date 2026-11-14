use crate::{
    config::Config,
    store::{Error, ExpiryCursor, ExpiryPolicy, ExpiryReport},
};
use clap::Args;

#[derive(Args, Debug)]
pub struct Options {
    /// Require last sighting to be older than this many days (owner-selected)
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    older_than: u32,
    /// Require job, peer and attempt activity to be older than this many days
    #[arg(long, value_parser = clap::value_parser!(u32).range(1..))]
    inactive_for: u32,
    /// Maximum old incomplete rows examined per transaction
    #[arg(long, default_value_t = 128, value_parser = clap::value_parser!(u32).range(1..=500))]
    batch_size: u32,
    /// Maximum transactions in this invocation
    #[arg(long, default_value_t = 10, value_parser = clap::value_parser!(u32).range(1..=10000))]
    max_batches: u32,
    /// Resume after a previous report's TIMESTAMP:INFO_HASH cursor
    #[arg(long)]
    after: Option<ExpiryCursor>,
    /// Delete eligible rows; without this flag, only preview candidates
    #[arg(long, requires = "services_stopped")]
    apply: bool,
    /// Confirm crawler and web services have been stopped for this maintenance run
    #[arg(long, requires = "apply")]
    services_stopped: bool,
}

pub async fn run(config: &Config, options: Options) -> Result<ExpiryReport, Error> {
    let policy = ExpiryPolicy::new(
        options.older_than,
        options.inactive_for,
        options.batch_size,
        options.max_batches,
    )?;
    let store = crate::open_store(config, false)
        .await
        .map_err(crate::store::invalid)?;
    store
        .expire_incomplete(&policy, options.after, options.apply)
        .await
}

#[cfg(test)]
mod tests {
    use crate::Cli;
    use clap::Parser;

    #[test]
    fn expiry_requires_owner_periods_and_maintenance_for_apply() {
        for command in [
            "btfind prune",
            "btfind prune --older-than 30",
            "btfind prune --older-than 0 --inactive-for 7",
            "btfind prune --older-than 30 --inactive-for 0",
            "btfind prune --older-than 30 --inactive-for 7 --apply",
            "btfind prune --older-than 30 --inactive-for 7 --batch-size 501",
            "btfind prune --older-than 30 --inactive-for 7 --max-batches 0",
            "btfind prune --older-than 30 --inactive-for 7 --after invalid",
        ] {
            assert!(
                Cli::try_parse_from(command.split_whitespace()).is_err(),
                "{command}"
            );
        }
        for suffix in [vec![], vec!["--apply", "--services-stopped"]] {
            let mut args = vec![
                "btfind",
                "prune",
                "--older-than",
                "30",
                "--inactive-for",
                "7",
            ];
            args.extend(suffix);
            assert!(Cli::try_parse_from(args).is_ok());
        }
    }
}
