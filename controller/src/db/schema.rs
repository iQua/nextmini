use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};
use tracing::{error, info};

/// Creates the tables in the database, if they do not exist yet.
pub(super) async fn create_db(pool: &Pool<Postgres>) {
    // private_network_name: Used to identify which private network (cluster) the node belongs to.
    // private_network_addr: Address of the node in the private network.
    // public_network_addr: Address of the node in the public network, when connecting to other private networks
    // over the public internet.
    // virtual_network_addr: Address of the node in the virtual network, established by Nextmini.
    // start_time: Timestamp when the dataplane observed the first payload packet.
    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS nodes (
            id SERIAL PRIMARY KEY,
            private_network_name TEXT,
            private_network_addr TEXT NOT NULL,
            public_network_addr TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create nodes table: {}", e);
    }

    // id: Unique identifier for the flow, automatically assigned by controller.
    // src_node_id: Source node ID for the flow.
    // dst_node_id: Destination node ID for the flow.
    // flow_len_type: Type of flow length specification ('bytes' or 'duration').
    // flow_len_bytes: Flow length in bytes (used when flow_len_type is 'bytes').
    // flow_len_duration: Flow length in seconds (used when flow_len_type is 'duration').
    // flow_rate: Optional flow rate in bytes per second.
    // flow_weight: Optional flow weight for scheduling.
    // is_finished: Whether this flow has completed.
    if let Err(e) = sqlx::query(
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
            is_finished BOOLEAN NOT NULL DEFAULT FALSE
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create flows table: {}", e);
    }

    // route_id: Unique identifier for the route, automatically assigned by controller.
    // src_node_id: Source node ID for the route.
    // dst_node_id: Destination node ID for the route.
    // edges: All edges in the route, as an array of node IDs. e.g. [[1, 2], [2, 3]]
    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS routes (
            route_id SERIAL PRIMARY KEY,
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            edges JSONB NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create routes table: {}", e);
    }

    // flow_routes: Relationship table documenting which flows use which routes.
    // This is a proper normalized design - the route assignment is a relationship,
    // not an inherent property of a flow. Flows may be assigned specific routes
    // by routing algorithms or bandwidth allocation policies.
    // flow_id: References flows.id - which flow is being routed
    // route_id: References routes.route_id - which route this flow should use
    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS flow_routes (
            flow_id INTEGER NOT NULL,
            route_id INTEGER NOT NULL,
            PRIMARY KEY (flow_id, route_id),
            FOREIGN KEY (flow_id) REFERENCES flows(id) ON DELETE CASCADE,
            FOREIGN KEY (route_id) REFERENCES routes(route_id) ON DELETE CASCADE
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create flow_routes table: {}", e);
    }

    // Index for fast lookups by flow_id (most common query pattern)
    if let Err(e) = sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_flow_routes_flow_id ON flow_routes(flow_id)
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create flow_routes index: {}", e);
    }

    if let Err(e) = sqlx::query(
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
    )
    .execute(pool)
    .await
    {
        error!("Failed to create metrics table: {}", e);
    }

    if let Err(e) = sqlx::query(
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
    )
    .execute(pool)
    .await
    {
        error!("Failed to create app_flows table: {}", e);
    }

    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS groups (
            id SERIAL PRIMARY KEY,
            label TEXT UNIQUE NOT NULL,
            src_node_id INTEGER NOT NULL,
            group_ip TEXT UNIQUE NOT NULL,
            created_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000)
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create groups table: {}", e);
    }

    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_members (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            node_id INTEGER NOT NULL,
            joined_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000),
            PRIMARY KEY (group_id, node_id)
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create group_members table: {}", e);
    }

    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS group_routes (
            group_id INTEGER NOT NULL REFERENCES groups(id) ON DELETE CASCADE,
            src_node_id INTEGER NOT NULL,
            edges JSONB NOT NULL,
            updated_at BIGINT DEFAULT (EXTRACT(EPOCH FROM NOW())::BIGINT*1000),
            PRIMARY KEY (group_id)
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create group_routes table: {}", e);
    }

    let create_membership_fn = r#"
        CREATE OR REPLACE FUNCTION notify_group_membership_change()
        RETURNS TRIGGER AS $$
        DECLARE gid INTEGER;
        BEGIN
            IF (TG_OP = 'INSERT') THEN
                gid := NEW.group_id;
            ELSE
                gid := OLD.group_id;
            END IF;
            PERFORM pg_notify('sync_group_routes', '{"group_id":"' || gid || '"}');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#;

    if let Err(e) = sqlx::query(create_membership_fn).execute(pool).await {
        error!("Failed to create membership trigger function: {}", e);
    }

    if let Err(e) = sqlx::query(
        r#"
        DROP TRIGGER IF EXISTS group_membership_change_trigger
        ON group_members;
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to drop existing membership trigger: {}", e);
    }

    let create_membership_trigger = r#"
        CREATE TRIGGER group_membership_change_trigger
        AFTER INSERT OR DELETE ON group_members
        FOR EACH ROW
        EXECUTE FUNCTION notify_group_membership_change();
    "#;

    if let Err(e) = sqlx::query(create_membership_trigger).execute(pool).await {
        error!("Failed to create membership trigger: {}", e);
    }

    // link_throughput: Stores measured underlay throughput between pairs of nodes.
    // src_node_id: The node that ran the iperf3 client.
    // dst_node_id: The node that ran the iperf3 server.
    // bandwidth_bps: Measured bandwidth in bits per second.
    // bandwidth_mbps: Measured bandwidth in megabits per second.
    // bytes_transferred: Total bytes transferred during the test.
    // duration_secs: Duration of the test in seconds.
    // protocol: "tcp" or "udp".
    // retransmits: Number of TCP retransmits (null for UDP).
    // jitter_ms: Jitter in milliseconds (UDP only).
    // measured_at: Timestamp when the measurement was taken.
    if let Err(e) = sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS link_throughput (
            id SERIAL PRIMARY KEY,
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            bandwidth_bps DOUBLE PRECISION NOT NULL,
            bandwidth_mbps DOUBLE PRECISION NOT NULL,
            bytes_transferred BIGINT NOT NULL,
            duration_secs DOUBLE PRECISION NOT NULL,
            protocol TEXT NOT NULL DEFAULT 'tcp',
            retransmits INTEGER,
            jitter_ms DOUBLE PRECISION,
            measured_at TIMESTAMP WITH TIME ZONE DEFAULT NOW(),
            UNIQUE (src_node_id, dst_node_id, protocol, measured_at)
        )
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create link_throughput table: {}", e);
    }

    // Create index for fast lookups by node pair
    if let Err(e) = sqlx::query(
        r#"
        CREATE INDEX IF NOT EXISTS idx_link_throughput_nodes
        ON link_throughput (src_node_id, dst_node_id)
        "#,
    )
    .execute(pool)
    .await
    {
        error!("Failed to create link_throughput index: {}", e);
    }

    // NOTE: Add new schema changes here so init/reset paths stay in sync.
}

/// Resets the entire database by dropping all known tables and recreating them.
pub(super) async fn reset_db(pool: &Pool<Postgres>) {
    info!("Resetting database - dropping all tables...");

    // drops the existing tables to ensure schema changes are applied
    for (table, sql) in [
        ("metrics", "DROP TABLE IF EXISTS metrics"),
        ("app_flows", "DROP TABLE IF EXISTS app_flows"),
        ("flow_routes", "DROP TABLE IF EXISTS flow_routes"),
        ("flows", "DROP TABLE IF EXISTS flows"),
        ("group_routes", "DROP TABLE IF EXISTS group_routes"),
        ("group_members", "DROP TABLE IF EXISTS group_members"),
        ("groups", "DROP TABLE IF EXISTS groups"),
        ("routes", "DROP TABLE IF EXISTS routes"),
        ("link_throughput", "DROP TABLE IF EXISTS link_throughput"),
        ("nodes", "DROP TABLE IF EXISTS nodes"),
    ] {
        if let Err(e) = sqlx::query(sql).execute(pool).await {
            error!("Failed to drop {} table: {}", table, e);
        }
    }

    create_db(pool).await;
}

pub(super) async fn schema_smoke_check(pool: &Pool<Postgres>) -> AnyResult<()> {
    sqlx::query("SELECT 1").execute(pool).await?;
    Ok(())
}
