/// Implements database initialization and notification setup.
use std::collections::HashSet;
use std::net::Ipv4Addr;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;

use futures_util::{SinkExt, StreamExt};
use sqlx::postgres::PgListener;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};

use crate::config;
use crate::models::{DbFlow, DbRoute, Group, GroupMember, Route};
use crate::utils::{
    allocate_multicast_ip, build_flows_for_node, build_group_routes_for_node,
    build_routes_for_node, merge_all_routes,
};
use crate::{NodeWriterMap, WebSocketWriter};
use nextmini_messages::{ControllerToDataplane, GroupRoutingTableEntry};
use tracing::{error, info, warn};

/// Creates the tables in the database, if they do not exist yet.
async fn create_db(pool: &Pool<Postgres>) {
    // private_network_name: Used to identify which private network (cluster) the node belongs to.
    // private_network_addr: Address of the node in the private network.
    // public_network_addr: Address of the node in the public network, when connecting to other private networks
    // over the public internet.
    // virtual_network_addr: Address of the node in the virtual network, established by Nextmini.
    // start_time: Timestamp when the dataplane observed the first payload packet.
    sqlx::query(
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
    .expect("Failed to create nodes table");

    // id: Unique identifier for the flow, automatically assigned by controller.
    // src_node_id: Source node ID for the flow.
    // dst_node_id: Destination node ID for the flow.
    // flow_len_type: Type of flow length specification ('bytes' or 'duration').
    // flow_len_bytes: Flow length in bytes (used when flow_len_type is 'bytes').
    // flow_len_duration: Flow length in seconds (used when flow_len_type is 'duration').
    // flow_rate: Optional flow rate in bytes per second.
    // flow_weight: Optional flow weight for scheduling.
    // is_finished: Whether this flow has completed.
    sqlx::query(
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
    .expect("Failed to create flows table");

    // route_id: Unique identifier for the route, automatically assigned by controller.
    // src_node_id: Source node ID for the route.
    // dst_node_id: Destination node ID for the route.
    // edges: All edges in the route, as an array of node IDs. e.g. [[1, 2], [2, 3]]
    sqlx::query(
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
    .expect("Failed to create routes table");

    sqlx::query(
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
    .expect("Failed to create metrics table");

    sqlx::query(
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
    .expect("Failed to create app_flows table");

    sqlx::query(
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
    .expect("Failed to create groups table");

    sqlx::query(
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
    .expect("Failed to create group_members table");

    sqlx::query(
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
    .expect("Failed to create group_routes table");

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

    sqlx::query(create_membership_fn)
        .execute(pool)
        .await
        .expect("Failed to create membership trigger function");

    let create_membership_trigger = r#"
        CREATE TRIGGER group_membership_change_trigger
        AFTER INSERT OR DELETE ON group_members
        FOR EACH ROW
        EXECUTE FUNCTION notify_group_membership_change();
    "#;

    sqlx::query(create_membership_trigger)
        .execute(pool)
        .await
        .expect("Failed to create membership trigger");

    // NOTE: Add new schema changes here so init/reset paths stay in sync.
}

// Resets the entire database.
async fn reset_db(pool: &Pool<Postgres>) {
    info!("Resetting database - dropping all tables...");

    // drops the existing tables to ensure schema changes are applied
    sqlx::query("DROP TABLE IF EXISTS metrics")
        .execute(pool)
        .await
        .expect("Failed to drop metrics table");

    sqlx::query("DROP TABLE IF EXISTS app_flows")
        .execute(pool)
        .await
        .expect("Failed to drop app_flows table");

    sqlx::query("DROP TABLE IF EXISTS flows")
        .execute(pool)
        .await
        .expect("Failed to drop flows table");

    sqlx::query("DROP TABLE IF EXISTS group_routes")
        .execute(pool)
        .await
        .expect("Failed to drop group_routes table");

    sqlx::query("DROP TABLE IF EXISTS group_members")
        .execute(pool)
        .await
        .expect("Failed to drop group_members table");

    sqlx::query("DROP TABLE IF EXISTS groups")
        .execute(pool)
        .await
        .expect("Failed to drop groups table");

    sqlx::query("DROP TABLE IF EXISTS routes")
        .execute(pool)
        .await
        .expect("Failed to drop routes table");

    sqlx::query("DROP TABLE IF EXISTS nodes")
        .execute(pool)
        .await
        .expect("Failed to drop nodes table");

    // recreates the tables with current schema
    create_db(pool).await;
}

/// Connects to and initializes the PostgreSQL database.
pub async fn init_db(config: &config::Config) -> Pool<Postgres> {
    // connects to the PostgreSQL database
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.db.user, config.db.password, config.db.host, config.db.port, config.db.database
    );

    info!("Connecting to PostgreSQL: {}", db_url);

    let pool = PgPoolOptions::new()
        .max_connections(100) // supports up to a maximum threshold of connections
        .min_connections(50) // maintains a minimum number of connections
        .connect(&db_url)
        .await
        .expect("Failed to connect to database");

    // ensures the database starts from a clean state every time
    reset_db(&pool).await;

    // adds routes derived from both custom routes and topology to the database
    let all_routes = merge_all_routes(config);
    info!("Adding all {} routes to the database.", all_routes.len());

    for (src_node_id, dst_node_id, edges) in all_routes {
        let edges_json: serde_json::Value =
            serde_json::to_value(&edges).expect("Failed to convert edges to JSON");

        let result = sqlx::query(
            r#"
            INSERT INTO routes (src_node_id, dst_node_id, edges)
            VALUES ($1, $2, $3)
            RETURNING route_id
            "#,
        )
        .bind(src_node_id as i32)
        .bind(dst_node_id as i32)
        .bind(edges_json)
        .fetch_optional(&pool)
        .await
        .expect("Failed to insert route");

        if let Some(row) = result {
            let route_id: i32 = row.get("route_id");
            info!(
                "Created route_id {} from {}→{} with {} edges.",
                route_id,
                src_node_id,
                dst_node_id,
                edges.len()
            );
        }
    }

    // adds custom flows from the configuration file
    info!("Adding custom flows from the configuration file.");

    for flow in config.flows.clone() {
        let src_node_id = flow.src_node_id as i32;
        let dst_node_id = flow.dst_node_id as i32;

        // converts the FlowLen to the database format
        let (flow_len_type, flow_len_bytes, flow_len_duration) = match flow.flow_spec.flow_len {
            nextmini_messages::FlowLen::Bytes(bytes) => ("bytes", Some(bytes as i64), None),
            nextmini_messages::FlowLen::Duration(duration) => ("duration", None, Some(duration)),
        };

        // inserts the flow with an auto-generated id
        let result = sqlx::query(
            r#"
            INSERT INTO flows (src_node_id, dst_node_id, flow_len_type, flow_len_bytes, flow_len_duration, flow_rate, flow_weight, is_finished)
            VALUES ($1, $2, $3, $4, $5, $6, $7, $8)
            RETURNING id
            "#,
        )
        .bind(src_node_id)
        .bind(dst_node_id)
        .bind(flow_len_type)
        .bind(flow_len_bytes)
        .bind(flow_len_duration)
        .bind(flow.flow_spec.flow_rate.map(|r| r as i32))
        .bind(flow.flow_spec.flow_weight.map(|w| w as i32))
        .bind(false) // is_finished defaults to false
        .fetch_optional(&pool)
        .await
        .expect("Failed to insert custom flow");

        if let Some(row) = result {
            let flow_id: i32 = row.get("id");
            info!(
                "Auto-assigned flow id {} to custom flow from node {} to node {} with {:?}",
                flow_id, src_node_id, dst_node_id, flow.flow_spec
            );
        }
    }

    pool
}

pub async fn create_group(
    db_pool: &Pool<Postgres>,
    label: &str,
    src_node_id: usize,
    base_addr: Ipv4Addr,
    mask: Ipv4Addr,
) -> AnyResult<Group> {
    let mut tx = db_pool.begin().await?;
    let next_id: i64 = sqlx::query_scalar("SELECT nextval('groups_id_seq')")
        .fetch_one(&mut *tx)
        .await?;

    let group_ip = allocate_multicast_ip(base_addr, mask, next_id as u32);
    let group_ip_string = group_ip.to_string();

    let group = sqlx::query_as::<_, Group>(
        r#"
        INSERT INTO groups (id, label, src_node_id, group_ip)
        VALUES ($1, $2, $3, $4)
        RETURNING id, label, src_node_id, group_ip
        "#,
    )
    .bind(next_id as i32)
    .bind(label)
    .bind(src_node_id as i32)
    .bind(&group_ip_string)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(group)
}

pub async fn add_group_member(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    node_id: usize,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO group_members (group_id, node_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(group_id)
    .bind(node_id as i32)
    .execute(db_pool)
    .await?;
    Ok(())
}

pub async fn remove_group_member(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    node_id: usize,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        DELETE FROM group_members
        WHERE group_id = $1 AND node_id = $2
        "#,
    )
    .bind(group_id)
    .bind(node_id as i32)
    .execute(db_pool)
    .await?;
    Ok(())
}

pub async fn load_group_directory(db_pool: &Pool<Postgres>) -> AnyResult<Vec<Group>> {
    let groups = sqlx::query_as::<_, Group>(
        r#"
        SELECT id, label, src_node_id, group_ip
        FROM groups
        ORDER BY id
        "#,
    )
    .fetch_all(db_pool)
    .await?;
    Ok(groups)
}

pub async fn load_group_members(
    db_pool: &Pool<Postgres>,
    group_id: i32,
) -> AnyResult<Vec<GroupMember>> {
    let members = sqlx::query_as::<_, GroupMember>(
        r#"
        SELECT group_id, node_id
        FROM group_members
        WHERE group_id = $1
        ORDER BY node_id
        "#,
    )
    .bind(group_id)
    .fetch_all(db_pool)
    .await?;
    Ok(members)
}

pub async fn upsert_group_routes(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    src_node_id: i32,
    edges: serde_json::Value,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO group_routes (group_id, src_node_id, edges)
        VALUES ($1, $2, $3)
        ON CONFLICT (group_id)
        DO UPDATE SET src_node_id = EXCLUDED.src_node_id, edges = EXCLUDED.edges, updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT*1000
        "#,
    )
    .bind(group_id)
    .bind(src_node_id)
    .bind(edges)
    .execute(db_pool)
    .await?;
    Ok(())
}

pub async fn setup_route_notification(db_pool: Arc<Pool<Postgres>>, node_ws: NodeWriterMap) {
    // creates the notification function and trigger
    let flow_table_name = "routes";

    let create_function_sql = r#"
        CREATE OR REPLACE FUNCTION notify_trigger_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('auto_sync_routes', '{"op":"' || TG_OP || '","route_id":"' || NEW.route_id || '"}');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#;

    let create_trigger_sql = format!(
        r#"
        CREATE TRIGGER new_flow_trigger
        AFTER INSERT OR UPDATE ON "{}"
        FOR EACH ROW
        EXECUTE FUNCTION notify_trigger_function();
        "#,
        flow_table_name
    );

    let check_trigger_sql = format!(
        r#"
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'new_flow_trigger'
        AND tgrelid = '"{}"'::regclass;
        "#,
        flow_table_name
    );

    let mut conn = db_pool
        .acquire()
        .await
        .expect("Failed to acquire connection");

    let row: Option<(i32,)> = sqlx::query_as(&check_trigger_sql)
        .fetch_optional(&mut *conn) // Dereference to get &mut PgConnection
        .await
        .expect("Failed to check trigger existence");

    if row.is_none() {
        sqlx::query(create_function_sql)
            .execute(&mut *conn)
            .await
            .expect("Failed to create notification function");
        sqlx::query(&create_trigger_sql)
            .execute(&mut *conn)
            .await
            .expect("Failed to create trigger");
    }

    // sets up a listener
    let mut listener = PgListener::connect_with(&db_pool)
        .await
        .expect("Failed to connect listener");
    listener
        .listen("auto_sync_routes")
        .await
        .expect("Failed to listen to auto_sync_routes");
    listener
        .listen("sync_routes")
        .await
        .expect("Failed to listen to sync_routes");

    // spawns a task to handle notifications by installing the routes to all available nodes
    tokio::spawn(async move {
        let mut stream = listener.into_stream(); // converts listener to stream

        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let channel = notif.channel();

                    if channel == "auto_sync_routes" {
                        // Nextmini does not support installing routes individually, so we need to find
                        // all routes from the database and re-install them all
                        info!("Installing route updates into the dataplane.");

                        let routes_db: Vec<DbRoute> =
                            sqlx::query_as::<_, DbRoute>("SELECT * FROM routes")
                                .fetch_all(&*db_pool)
                                .await
                                .expect("Failed to fetch routes");

                        let routes: Vec<Route> = routes_db
                            .iter()
                            .map(|r| {
                                let edges_i32: Vec<(i32, i32)> =
                                    serde_json::from_value(r.edges.clone()).unwrap_or_default();
                                let edges: Vec<(u32, u32)> = edges_i32
                                    .into_iter()
                                    .map(|(a, b)| (a as u32, b as u32))
                                    .collect();

                                Route {
                                    route_id: r.route_id as usize,
                                    src_node_id: r.src_node_id as u32,
                                    dst_node_id: r.dst_node_id as u32,
                                    edges,
                                }
                            })
                            .collect();

                        // sends install routes message to all nodes
                        let node_ws_guard = node_ws.read().await;

                        for (node_id, ws_arc) in node_ws_guard.iter() {
                            if let Some(msg) =
                                build_routes_for_node(routes.clone(), *node_id as u32)
                            {
                                let msg_binary = rmp_serde::to_vec(&msg).unwrap();

                                ws_arc
                                    .lock()
                                    .await
                                    .send(Message::binary(msg_binary))
                                    .await
                                    .unwrap();

                                info!("Installing routes on node {}.", node_id);
                            } else {
                                error!("No node to install flow to.");
                            }
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving notification: {}", e);
                }
            }
        }
    });
}

pub async fn setup_group_notification(db_pool: Arc<Pool<Postgres>>, node_ws: NodeWriterMap) {
    let mut listener = PgListener::connect_with(&db_pool)
        .await
        .expect("Failed to connect listener for multicast groups");
    listener
        .listen("sync_group_routes")
        .await
        .expect("Failed to listen to sync_group_routes");

    tokio::spawn(async move {
        let mut stream = listener.into_stream();
        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let payload = notif.payload();
                    let parsed = serde_json::from_str::<serde_json::Value>(payload)
                        .unwrap_or(serde_json::Value::Null);
                    let group_id_opt = parsed
                        .get("group_id")
                        .and_then(|v| v.as_str())
                        .and_then(|s| s.parse::<i32>().ok());

                    if let Some(group_id) = group_id_opt {
                        if let Err(err) =
                            recompute_and_push_group_routes(group_id, &db_pool, &node_ws).await
                        {
                            error!(
                                "Failed to recompute multicast routes for group {}: {}",
                                group_id, err
                            );
                        }
                    } else {
                        warn!("Ignored malformed sync_group_routes payload: {}", payload);
                    }
                }
                Err(e) => error!("Error receiving group notification: {}", e),
            }
        }
    });
}

async fn recompute_and_push_group_routes(
    group_id: i32,
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
) -> AnyResult<()> {
    let Some(group) = sqlx::query_as::<_, Group>(
        "SELECT id, label, src_node_id, group_ip FROM groups WHERE id = $1",
    )
    .bind(group_id)
    .fetch_optional(db_pool)
    .await?
    else {
        warn!(
            "Received multicast recompute for unknown group {}",
            group_id
        );
        return Ok(());
    };

    // Load previous DAG to clean up stale routes later.
    let previous_edges_value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT edges FROM group_routes WHERE group_id = $1")
            .bind(group_id)
            .fetch_optional(db_pool)
            .await?;

    let previous_edges: Vec<(u32, u32)> = previous_edges_value
        .as_ref()
        .map(|value| serde_json::from_value::<Vec<(u32, u32)>>(value.clone()).unwrap_or_default())
        .unwrap_or_default();

    let members = load_group_members(db_pool, group_id).await?;
    let member_node_ids: Vec<u32> = members.iter().map(|m| m.node_id as u32).collect();
    let member_node_set: HashSet<u32> = member_node_ids.iter().copied().collect();

    let mut dag_edges_set: HashSet<(u32, u32)> = HashSet::new();
    let mut dag_nodes: HashSet<u32> = HashSet::new();

    for member in &member_node_ids {
        let route_row = sqlx::query(
            r#"SELECT edges FROM routes WHERE src_node_id = $1 AND dst_node_id = $2 ORDER BY route_id LIMIT 1"#,
        )
        .bind(group.src_node_id)
        .bind(*member as i32)
        .fetch_optional(db_pool)
        .await?;

        let Some(row) = route_row else {
            warn!(
                "No unicast route from {} to member {} when recomputing multicast group {}.",
                group.src_node_id, member, group_id
            );
            continue;
        };

        let edges_json: serde_json::Value = row.get("edges");
        let edges_i32: Vec<(i32, i32)> = serde_json::from_value(edges_json).map_err(|e| {
            anyhow::anyhow!(
                "Failed to decode route edges for multicast member {} in group {}: {}",
                member,
                group_id,
                e
            )
        })?;

        if edges_i32.is_empty() {
            warn!(
                "Route from {} to member {} has no edges; skipping in multicast DAG.",
                group.src_node_id, member
            );
            continue;
        }

        for (from, to) in edges_i32 {
            let from_u32 = from as u32;
            let to_u32 = to as u32;
            dag_nodes.insert(from_u32);
            dag_nodes.insert(to_u32);
            dag_edges_set.insert((from_u32, to_u32));
        }
    }

    let mut dag_edges: Vec<(u32, u32)> = dag_edges_set.into_iter().collect();
    dag_edges.sort_unstable();

    // Persist DAG edges (even empty) for audit and diff.
    let dag_json = serde_json::to_value(
        dag_edges
            .iter()
            .map(|(a, b)| [(*a), (*b)])
            .collect::<Vec<[u32; 2]>>(),
    )?;
    upsert_group_routes(db_pool, group_id, group.src_node_id, dag_json).await?;

    // Determine which nodes need notifications (previous DAG participants + current DAG nodes + members + source).
    let mut nodes_to_notify: HashSet<u32> =
        previous_edges.iter().flat_map(|(a, b)| [*a, *b]).collect();
    nodes_to_notify.extend(dag_nodes.iter());
    nodes_to_notify.insert(group.src_node_id as u32);
    nodes_to_notify.extend(member_node_ids.iter());

    if nodes_to_notify.is_empty() {
        return Ok(());
    }

    let send_targets: Vec<(u32, Arc<Mutex<WebSocketWriter>>)> = {
        let guard = node_ws.read().await;
        nodes_to_notify
            .iter()
            .filter_map(|node| {
                guard
                    .get(&(*node as usize))
                    .map(|writer| (*node, Arc::clone(writer)))
            })
            .collect()
    };

    if send_targets.is_empty() {
        warn!(
            "No active websocket connections available for multicast group {} update.",
            group_id
        );
        return Ok(());
    }

    for (node_id, writer) in send_targets {
        let entry = build_group_routes_for_node(
            group.id as usize,
            group.src_node_id as u32,
            &dag_edges,
            node_id,
            &member_node_set,
        );
        let routes: Vec<GroupRoutingTableEntry> = entry.into_iter().collect();
        let message = ControllerToDataplane::InstallGroupRoutes {
            group_id: group.id as usize,
            src_node_id: group.src_node_id as usize,
            routes,
        };
        let payload = rmp_serde::to_vec(&message)?;

        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "Failed to send InstallGroupRoutes for group {} to node {}: {}",
                group_id, node_id, e
            );
        } else {
            info!(
                "Pushed InstallGroupRoutes for group {} to node {}.",
                group_id, node_id
            );
        }
    }

    Ok(())
}

pub async fn setup_flow_notification(db_pool: Arc<Pool<Postgres>>, node_ws: NodeWriterMap) {
    // creates the flow notification function and trigger
    let create_flow_function_sql = r#"
        CREATE OR REPLACE FUNCTION notify_flow_trigger_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('auto_sync_flows', '{"newly_inserted_id":"'|| NEW.id || '","src_node_id":"' || NEW.src_node_id || '","dst_node_id":"' || NEW.dst_node_id || '"}');
            RETURN NEW;
        END;
        $$ LANGUAGE plpgsql;
    "#;

    let create_flow_trigger_sql = r#"
        CREATE TRIGGER flow_notification_trigger
        AFTER INSERT ON "flows"
        FOR EACH ROW
        EXECUTE FUNCTION notify_flow_trigger_function();
    "#;

    let check_flow_trigger_sql = r#"
        SELECT 1
        FROM pg_trigger
        WHERE tgname = 'flow_notification_trigger'
        AND tgrelid = '"flows"'::regclass;
    "#;

    let mut conn = db_pool
        .acquire()
        .await
        .expect("Failed to acquire connection");

    // checks and creates a flow trigger if it doesn't exist
    let flow_row: Option<(i32,)> = sqlx::query_as(check_flow_trigger_sql)
        .fetch_optional(&mut *conn)
        .await
        .expect("Failed to check flow trigger existence");

    if flow_row.is_none() {
        sqlx::query(create_flow_function_sql)
            .execute(&mut *conn)
            .await
            .expect("Failed to create flow notification function");
        sqlx::query(create_flow_trigger_sql)
            .execute(&mut *conn)
            .await
            .expect("Failed to create flow trigger");
        info!("Created flow notification trigger.");
    }

    // sets up a listener for flow notifications
    let mut listener = PgListener::connect_with(&db_pool)
        .await
        .expect("Failed to connect listener");
    listener
        .listen("auto_sync_flows")
        .await
        .expect("Failed to listen to auto_sync_flows");

    // spawns a task to handle flow notifications by installing flows to relevant nodes
    tokio::spawn(async move {
        let mut stream = listener.into_stream();

        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let channel = notif.channel();

                    if channel == "auto_sync_flows" {
                        let payload = notif.payload();
                        info!("Received flow notification: {}", payload);

                        // parses and gets a newly inserted flow ID
                        match serde_json::from_str::<serde_json::Value>(payload) {
                            Ok(json) => {
                                if let Some(id) = json
                                    .get("newly_inserted_id")
                                    .and_then(|v| v.as_str())
                                    .and_then(|s| s.parse::<i32>().ok())
                                {
                                    match sqlx::query_as::<_, DbFlow>(
                                        "SELECT * FROM flows WHERE id = $1",
                                    )
                                    .bind(id)
                                    .fetch_one(&*db_pool)
                                    .await
                                    {
                                        Ok(flow) => {
                                            info!(
                                                "Installing newly inserted flow {} into the dataplane.",
                                                id
                                            );

                                            let node_ws_guard = node_ws.read().await;
                                            let msg = build_flows_for_node(vec![flow.clone()]);
                                            let msg_binary = rmp_serde::to_vec(&msg).unwrap();

                                            let src_node_id = flow.src_node_id as usize;
                                            let dst_node_id = flow.dst_node_id as usize;

                                            // sends to source node
                                            if let Some(ws_arc) = node_ws_guard.get(&src_node_id) {
                                                match ws_arc
                                                    .lock()
                                                    .await
                                                    .send(Message::binary(msg_binary.clone()))
                                                    .await
                                                {
                                                    Ok(_) => info!(
                                                        "Sent new flow {} to source node {}.",
                                                        id, src_node_id
                                                    ),
                                                    Err(e) => error!(
                                                        "Failed to send flow {} to source node {}: {}",
                                                        id, src_node_id, e
                                                    ),
                                                }
                                            }

                                            // sends to destination node
                                            if let Some(ws_arc) = node_ws_guard.get(&dst_node_id) {
                                                match ws_arc
                                                    .lock()
                                                    .await
                                                    .send(Message::binary(msg_binary.clone()))
                                                    .await
                                                {
                                                    Ok(_) => info!(
                                                        "Sent new flow {} to destination node {}.",
                                                        id, dst_node_id
                                                    ),
                                                    Err(e) => error!(
                                                        "Failed to send flow {} to destination node {}: {}",
                                                        id, dst_node_id, e
                                                    ),
                                                }
                                            }
                                        }
                                        Err(e) => error!(
                                            "Failed to fetch newly inserted flow {}: {}",
                                            id, e
                                        ),
                                    }
                                } else {
                                    error!(
                                        "Failed to parse newly_inserted_id from notification payload: {}",
                                        payload
                                    );
                                }
                            }
                            Err(e) => error!("Failed to parse notification payload as JSON: {}", e),
                        }
                    }
                }
                Err(e) => {
                    error!("Error receiving notification: {}", e);
                }
            }
        }
    });
}
