use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};
use tracing::{info, warn};

pub(super) async fn apply_schema(pool: &Pool<Postgres>) -> AnyResult<()> {
    for statement in SCHEMA_BOOTSTRAP {
        sqlx::query(statement).execute(pool).await?;
    }

    Ok(())
}

/// Drops all controller-owned tables and reapplies schema bootstrap.
///
/// Intended for dev/test via `CONTROLLER_RESET_DB`.
pub(super) async fn reset_db(pool: &Pool<Postgres>) -> AnyResult<()> {
    warn!("Resetting controller database - dropping tables and reapplying schema...");

    // Drop controller-owned tables so schema bootstrap can be reapplied.
    for (table, sql) in [
        ("metrics", "DROP TABLE IF EXISTS metrics CASCADE"),
        ("app_flows", "DROP TABLE IF EXISTS app_flows CASCADE"),
        ("flow_routes", "DROP TABLE IF EXISTS flow_routes CASCADE"),
        ("flows", "DROP TABLE IF EXISTS flows CASCADE"),
        ("group_routes", "DROP TABLE IF EXISTS group_routes CASCADE"),
        (
            "group_members",
            "DROP TABLE IF EXISTS group_members CASCADE",
        ),
        ("groups", "DROP TABLE IF EXISTS groups CASCADE"),
        ("routes", "DROP TABLE IF EXISTS routes CASCADE"),
        ("nodes", "DROP TABLE IF EXISTS nodes CASCADE"),
    ] {
        sqlx::query(sql).execute(pool).await?;
        info!("Dropped table {}.", table);
    }

    apply_schema(pool).await?;
    Ok(())
}

const SCHEMA_BOOTSTRAP: &[&str] = &[
    r#"
CREATE TABLE IF NOT EXISTS nodes (
    id SERIAL PRIMARY KEY,
    private_network_name TEXT,
    private_network_addr TEXT NOT NULL,
    public_network_addr TEXT NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS flows (
    id SERIAL PRIMARY KEY,
    src_node_id INTEGER NOT NULL,
    dst_node_id INTEGER NOT NULL,
    flow_len_type TEXT NOT NULL CHECK (flow_len_type IN ('bytes', 'duration')),
    flow_len_bytes BIGINT,
    flow_len_duration DOUBLE PRECISION,
    flow_rate INTEGER,
    flow_weight INTEGER,
    start_time BIGINT,
    finish_time BIGINT,
    is_finished BOOLEAN NOT NULL DEFAULT FALSE,
    is_probe BOOLEAN NOT NULL DEFAULT FALSE
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS routes (
    route_id SERIAL PRIMARY KEY,
    src_node_id INTEGER NOT NULL,
    dst_node_id INTEGER NOT NULL,
    edges JSONB NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS flow_routes (
    flow_id INTEGER NOT NULL,
    route_id INTEGER NOT NULL,
    PRIMARY KEY (flow_id, route_id),
    FOREIGN KEY (flow_id) REFERENCES flows(id) ON DELETE CASCADE,
    FOREIGN KEY (route_id) REFERENCES routes(route_id) ON DELETE CASCADE
)
"#,
    "CREATE INDEX IF NOT EXISTS idx_flow_routes_flow_id ON flow_routes(flow_id)",
    r#"
CREATE TABLE IF NOT EXISTS metrics (
    id SERIAL PRIMARY KEY,
    flow_id BYTEA NOT NULL,
    local_node_id INTEGER NOT NULL,
    remote_node_id INTEGER NOT NULL,
    bytes INTEGER NOT NULL,
    time_read TIMESTAMP WITH TIME ZONE NOT NULL
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS app_flows (
    id SERIAL PRIMARY KEY,
    flow_id BYTEA NOT NULL,
    start_time BIGINT NOT NULL,
    src_node_id INTEGER,
    dst_node_id INTEGER,
    is_finished BOOLEAN NOT NULL DEFAULT FALSE,
    finish_time BIGINT,
    route_id INTEGER,
    UNIQUE (flow_id, start_time)
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS groups (
    id SERIAL PRIMARY KEY,
    label TEXT UNIQUE NOT NULL,
    src_node_id INTEGER NOT NULL,
    group_ip TEXT UNIQUE NOT NULL,
    created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000)
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS group_members (
    group_id INTEGER NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    node_id INTEGER NOT NULL,
    joined_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000),
    PRIMARY KEY (group_id, node_id)
)
"#,
    r#"
CREATE TABLE IF NOT EXISTS group_routes (
    group_id INTEGER NOT NULL REFERENCES groups (id) ON DELETE CASCADE,
    tree_id INTEGER NOT NULL DEFAULT 0,
    src_node_id INTEGER NOT NULL,
    edges JSONB NOT NULL,
    weight DOUBLE PRECISION,
    updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT * 1000),
    PRIMARY KEY (group_id, tree_id),
    CONSTRAINT group_routes_tree_id_nonnegative CHECK (tree_id >= 0)
)
"#,
    "CREATE INDEX IF NOT EXISTS idx_group_routes_group_id ON group_routes (group_id)",
    "CREATE INDEX IF NOT EXISTS idx_group_routes_src_node_id ON group_routes (src_node_id)",
    r#"
CREATE OR REPLACE FUNCTION notify_group_membership_change()
RETURNS TRIGGER AS $$
DECLARE gid INTEGER;
DECLARE nid INTEGER;
BEGIN
    IF (TG_OP = 'INSERT') THEN
        gid := NEW.group_id;
        nid := NEW.node_id;
    ELSE
        gid := OLD.group_id;
        nid := OLD.node_id;
    END IF;
    PERFORM pg_notify(
        'sync_group_routes',
        '{"group_id":"' || gid || '","node_id":"' || nid || '"}'
    );
    RETURN COALESCE(NEW, OLD);
END;
$$ LANGUAGE plpgsql
"#,
    "DROP TRIGGER IF EXISTS group_membership_change_trigger ON group_members",
    r#"
CREATE TRIGGER group_membership_change_trigger
AFTER INSERT OR DELETE ON group_members
FOR EACH ROW
EXECUTE FUNCTION notify_group_membership_change()
"#,
    r#"
CREATE OR REPLACE FUNCTION notify_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'auto_sync_routes',
        '{"op":"' || TG_OP || '","route_id":"' || NEW.route_id || '"}'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"#,
    "DROP TRIGGER IF EXISTS new_flow_trigger ON routes",
    r#"
CREATE TRIGGER new_flow_trigger
AFTER INSERT OR UPDATE ON routes
FOR EACH ROW
EXECUTE FUNCTION notify_trigger_function()
"#,
    r#"
CREATE OR REPLACE FUNCTION notify_flow_trigger_function()
RETURNS TRIGGER AS $$
BEGIN
    PERFORM pg_notify(
        'auto_sync_flows',
        '{"newly_inserted_id":"' || NEW.id || '","src_node_id":"' || NEW.src_node_id || '","dst_node_id":"' || NEW.dst_node_id || '"}'
    );
    RETURN NEW;
END;
$$ LANGUAGE plpgsql
"#,
    "DROP TRIGGER IF EXISTS flow_notification_trigger ON flows",
    r#"
CREATE TRIGGER flow_notification_trigger
AFTER INSERT ON flows
FOR EACH ROW
EXECUTE FUNCTION notify_flow_trigger_function()
"#,
];
