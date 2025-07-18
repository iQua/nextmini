/// Implements database initialization and notification setup.
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;

use futures_util::{SinkExt, StreamExt};
use sqlx::postgres::PgListener;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};

use crate::WebSocketWriter;
use crate::config;
use crate::models::{DbFlow, Route};
use crate::utils::{build_flows_for_node, build_routes_for_node};
use tracing::{error, info, warn};

/// Creates the tables in the database, if they do not exist yet.
async fn create_db(pool: &Pool<Postgres>) {
    // private_network_name: Used to identify which private network (cluster) the node belongs to.
    // private_network_addr: Address of the node in the private network.
    // public_network_addr: Address of the node in the public network, when connecting to other private networks
    // over the public internet.
    // virtual_network_addr: Address of the node in the virtual network, established by Nextmini.
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

    // route_id: Unique identifier for the route, automatically assigned by controller.
    // route: all hops in the route, as an array of node IDs. e.g. [1, 2, 3]
    // src_node_id: Source node ID in the route.
    // dst_node_id: Destination node ID in the route.
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS routes (
            route_id SERIAL PRIMARY KEY,
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            route INTEGER[] NOT NULL,
            UNIQUE (src_node_id, dst_node_id, route)
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to create routes table");

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
            is_finished BOOLEAN NOT NULL DEFAULT FALSE
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to create flows table");

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            flow_id BYTEA NOT NULL,
            local_node_id INTEGER NOT NULL,
            remote_node_id INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            time_read TIMESTAMP NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to create metrics table");
}

// Resets the entire database.
async fn reset_db(pool: &Pool<Postgres>) {
    // drops the existing tables to ensure schema changes are applied
    sqlx::query("DROP TABLE IF EXISTS metrics")
        .execute(pool)
        .await
        .expect("Failed to drop metrics table");

    sqlx::query("DROP TABLE IF EXISTS flows")
        .execute(pool)
        .await
        .expect("Failed to drop flows table");

    sqlx::query("DROP TABLE IF EXISTS routes")
        .execute(pool)
        .await
        .expect("Failed to drop routes table");

    sqlx::query("DROP TABLE IF EXISTS nodes")
        .execute(pool)
        .await
        .expect("Failed to drop nodes table");

    // recreates the tables with current schema
    sqlx::query(
        r#"
        CREATE TABLE nodes (
            id SERIAL PRIMARY KEY,
            private_network_name TEXT,
            private_network_addr TEXT NOT NULL,
            public_network_addr TEXT NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to recreate nodes table");

    sqlx::query(
        r#"
        CREATE TABLE routes (
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            route_id SERIAL PRIMARY KEY,
            route INTEGER[] NOT NULL,
            UNIQUE (src_node_id, dst_node_id, route)
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to recreate routes table");

    sqlx::query(
        r#"
        CREATE TABLE flows (
            id SERIAL PRIMARY KEY,
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            flow_len_type TEXT NOT NULL CHECK (flow_len_type IN ('bytes', 'duration')),
            flow_len_bytes BIGINT,
            flow_len_duration DOUBLE PRECISION,
            flow_rate INTEGER,
            flow_weight INTEGER,
            is_finished BOOLEAN NOT NULL DEFAULT FALSE
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to recreate flows table");

    sqlx::query(
        r#"
        CREATE TABLE metrics (
            id SERIAL PRIMARY KEY,
            flow_id BYTEA NOT NULL,
            local_node_id INTEGER NOT NULL,
            remote_node_id INTEGER NOT NULL,
            bytes INTEGER NOT NULL,
            time_read TIMESTAMP NOT NULL
        )
        "#,
    )
    .execute(pool)
    .await
    .expect("Failed to recreate metrics table");
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
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to database");

    // creates the database and tables if they do not exist, and then resets it
    create_db(&pool).await;
    reset_db(&pool).await;

    // adds the topology and direct links between neighbouring nodes as initial routes
    if let Some(preset_topology) = &config.topology.topology_type {
        let n_nodes = config.topology.n_nodes.unwrap_or(0);
        info!(
            "Adding the {:?} topology with {} nodes.",
            preset_topology, n_nodes
        );

        match preset_topology {
            config::PresetTopology::FullMesh => {
                for src_node in 1..=n_nodes {
                    for dest_node in 1..=n_nodes {
                        if src_node == dest_node {
                            continue; // skips loopback routes
                        }

                        let route_path = vec![src_node as i32, dest_node as i32];

                        let result = sqlx::query(
                            r#"
                            INSERT INTO routes (src_node_id, dst_node_id, route)
                            VALUES ($1, $2, $3)
                            ON CONFLICT (src_node_id, dst_node_id, route) DO NOTHING
                            RETURNING route_id
                            "#,
                        )
                        .bind(src_node as i32)
                        .bind(dest_node as i32)
                        .bind(&route_path)
                        .fetch_optional(&pool)
                        .await
                        .expect("Failed to insert full mesh route");

                        if let Some(row) = result {
                            let route_id: i32 = row.get("route_id");
                            info!(
                                "Created full mesh route ID {} from node {} to node {}",
                                route_id, src_node, dest_node
                            );
                        }
                    }
                }
            }
            config::PresetTopology::Ring => {
                // adds links between neighbouring nodes on the ring
                for src_node in 1..n_nodes {
                    let dest_node = src_node + 1;
                    let route_path = vec![src_node as i32, dest_node as i32];

                    let result = sqlx::query(
                        r#"
                        INSERT INTO routes (src_node_id, dst_node_id, route)
                        VALUES ($1, $2, $3)
                        ON CONFLICT (src_node_id, dst_node_id, route) DO NOTHING
                        RETURNING route_id
                        "#,
                    )
                    .bind(src_node as i32)
                    .bind(dest_node as i32)
                    .bind(&route_path)
                    .fetch_optional(&pool)
                    .await
                    .expect("Failed to insert ring route");

                    if let Some(row) = result {
                        let route_id: i32 = row.get("route_id");
                        info!(
                            "Created ring route ID {} from node {} to node {}",
                            route_id, src_node, dest_node
                        );
                    }
                }

                // adds ring closure: connects the last node back to the first node
                let route_path = vec![n_nodes as i32, 1];

                let result = sqlx::query(
                    r#"
                    INSERT INTO routes (src_node_id, dst_node_id, route)
                    VALUES ($1, $2, $3)
                    ON CONFLICT (src_node_id, dst_node_id, route) DO NOTHING
                    RETURNING route_id
                    "#,
                )
                .bind(n_nodes as i32)
                .bind(1)
                .bind(&route_path)
                .fetch_optional(&pool)
                .await
                .expect("Failed to insert ring closure route");

                if let Some(row) = result {
                    let route_id: i32 = row.get("route_id");
                    info!(
                        "Created ring closure route_id {} from node {} to node 1",
                        route_id, n_nodes
                    );
                }
            }
        }
    }

    // adds custom routes from the configuration file
    info!("Adding custom routes from the configuration file.");

    for route in config.routes.clone() {
        if route.route.is_empty() {
            warn!("Skipping empty route");
            continue;
        }

        // auto-infers src_node_id and dst_node_id from the route path
        let src_node_id = route.route[0] as i32;
        let dst_node_id = route.route[route.route.len() - 1] as i32;
        let route_path = route.route.iter().map(|&x| x as i32).collect::<Vec<_>>();

        // inserts route with an auto-generated route_id
        let result = sqlx::query(
            r#"
            INSERT INTO routes (src_node_id, dst_node_id, route)
            VALUES ($1, $2, $3)
            ON CONFLICT (src_node_id, dst_node_id, route) DO NOTHING
            RETURNING route_id
            "#,
        )
        .bind(src_node_id)
        .bind(dst_node_id)
        .bind(&route_path)
        .fetch_optional(&pool)
        .await
        .expect("Failed to insert custom route");

        if let Some(row) = result {
            let route_id: i32 = row.get("route_id");
            info!(
                "Auto-assigned route_id {} to custom route from node {} to node {} with path {:?}",
                route_id, src_node_id, dst_node_id, route.route
            );
        } else {
            info!(
                "Skipped duplicate route from node {} to node {} with path {:?}",
                src_node_id, dst_node_id, route.route
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

pub async fn setup_route_notification(
    db_pool: Arc<Pool<Postgres>>,

    // a hashmap from the node ID to its corresponding WebSocket sink
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) {
    // creates the notification function and trigger
    let flow_table_name = "routes";

    let create_function_sql = r#"
        CREATE OR REPLACE FUNCTION notify_trigger_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('auto_sync_routes', '{"op":"' || TG_OP || '","src_node_id":"' || NEW.src_node_id || '","dst_node_id":"' || NEW.dst_node_id || '","route_id":"'|| NEW.route_id || '","route":"' || array_to_string(NEW.route, ',') || '"}');
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

                        let routes: Vec<Route> = sqlx::query_as("SELECT * FROM routes")
                            .fetch_all(&*db_pool)
                            .await
                            .expect("Failed to fetch routes");

                        // sends install routes message to all nodes
                        let node_ws_guard = node_ws.read().await;

                        for (node_id, ws_arc) in node_ws_guard.iter() {
                            if let Some(msg) =
                                build_routes_for_node(routes.clone(), *node_id as i32)
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

pub async fn setup_flow_notification(
    db_pool: Arc<Pool<Postgres>>,

    // a hashmap from the node ID to its corresponding WebSocket sink
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) {
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
        info!("Created flow notification trigger");
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
