use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};
use tracing::{info, warn};

pub(super) async fn run_migrations(pool: &Pool<Postgres>) -> AnyResult<()> {
    sqlx::migrate!("./migrations").run(pool).await?;

    Ok(())
}

/// Drops all controller-owned tables and reruns migrations.
///
/// Intended for dev/test via `CONTROLLER_RESET_DB`.
pub(super) async fn reset_db(pool: &Pool<Postgres>) -> AnyResult<()> {
    warn!("Resetting controller database - dropping tables and rerunning migrations...");

    // Drop our tables and sqlx's migration bookkeeping so migrations re-apply.
    for (table, sql) in [
        ("metrics", "DROP TABLE IF EXISTS metrics CASCADE"),
        ("app_flows", "DROP TABLE IF EXISTS app_flows CASCADE"),
        ("flows", "DROP TABLE IF EXISTS flows CASCADE"),
        ("group_routes", "DROP TABLE IF EXISTS group_routes CASCADE"),
        (
            "group_members",
            "DROP TABLE IF EXISTS group_members CASCADE",
        ),
        ("groups", "DROP TABLE IF EXISTS groups CASCADE"),
        ("routes", "DROP TABLE IF EXISTS routes CASCADE"),
        ("nodes", "DROP TABLE IF EXISTS nodes CASCADE"),
        (
            "_sqlx_migrations",
            "DROP TABLE IF EXISTS _sqlx_migrations CASCADE",
        ),
    ] {
        sqlx::query(sql).execute(pool).await?;
        info!("Dropped table {}.", table);
    }

    run_migrations(pool).await?;
    Ok(())
}
