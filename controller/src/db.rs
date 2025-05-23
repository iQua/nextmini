/// Implements database initialization and notification setup.
use std::collections::HashMap;
use std::sync::Arc;

use tokio::sync::{Mutex, RwLock};
use tokio_tungstenite::tungstenite::Message;

use futures_util::{SinkExt, StreamExt};
use sqlx::postgres::PgListener;
use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres};

use crate::WebSocketWriter;
use crate::config;
use crate::models::Route;
use crate::utils::build_install_routes_message;

pub async fn init_db(config: &config::Config) -> Pool<Postgres> {
    // connects to the PostgreSQL database
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.db.user, config.db.password, config.db.host, config.db.port, config.db.database
    );

    println!("Connecting to PostgreSQL: {}", db_url);

    let pool = PgPoolOptions::new()
        .max_connections(5)
        .connect(&db_url)
        .await
        .expect("Failed to connect to database");

    // creates the tables in the database, if they do not exist yet
    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS nodes (
            id SERIAL PRIMARY KEY,
            private_network_name TEXT,
            private_network_addr TEXT NOT NULL,
            public_network_addr TEXT NOT NULL,
            virtual_network_addr TEXT NOT NULL,
            connections INTEGER[] NOT NULL
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("Failed to create nodes table");

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS routes (
            src_node_id INTEGER NOT NULL,
            dst_node_id INTEGER NOT NULL,
            route_id INTEGER NOT NULL,
            hops INTEGER[] NOT NULL,
            streams TEXT,
            PRIMARY KEY (src_node_id, dst_node_id, route_id)
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("Failed to create routes table");

    sqlx::query(
        r#"
        CREATE TABLE IF NOT EXISTS metrics (
            id SERIAL PRIMARY KEY,
            src_id INTEGER,
            dst_id INTEGER,
            route_id INTEGER,
            prev_hop_id INTEGER,
            hop_id INTEGER,
            flow_id INTEGER[],
            -- stream_id TEXT,
            time_read TIMESTAMP,
            bps INTEGER
        )
        "#,
    )
    .execute(&pool)
    .await
    .expect("Failed to create metrics table");

    if config.reset_db {
        // resets the entire database for a new session, if needed
        sqlx::query("TRUNCATE TABLE nodes, routes, metrics")
            .execute(&pool)
            .await
            .expect("Failed to reset database");
    } else {
        // the nodes table will always be reset
        sqlx::query("TRUNCATE TABLE nodes")
            .execute(&pool)
            .await
            .expect("Failed to reset database");
    }

    // adds initial routes from presets
    println!("Adding preset routes from the configuration file.");

    if let Some(preset_topology) = &config.routes_preset.preset_topology {
        let n_nodes = config.routes_preset.n_nodes.unwrap_or(0);
        let route_ids = config.routes_preset.route_ids.clone().unwrap_or(vec![0]);

        match preset_topology {
            config::PresetTopology::FullMesh => {
                for id in &route_ids {
                    for i in 1..=n_nodes {
                        for j in 1..=n_nodes {
                            if i == j {
                                continue;
                            }

                            let route = Route {
                                src_node_id: i as i32,
                                dst_node_id: j as i32,
                                route_id: *id as i32,
                                hops: vec![i as i32, j as i32],
                                // streams: Some("[]".to_string()),
                            };

                            sqlx::query(
                                r#"
                                INSERT INTO routes (src_node_id, dst_node_id, route_id, hops, streams)
                                VALUES ($1, $2, $3, $4, $5)
                                ON CONFLICT (src_node_id, dst_node_id, route_id)
                                DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams
                                "#
                            )
                            .bind(route.src_node_id)
                            .bind(route.dst_node_id)
                            .bind(route.route_id)
                            .bind(&route.hops)
                            // .bind(&route.streams)
                            .execute(&pool)
                            .await
                            .expect("Failed to insert full mesh route");
                        }
                    }
                }
            }
            config::PresetTopology::Ring => {
                for id in &route_ids {
                    for i in 1..n_nodes {
                        let j = i + 1;
                        let route = Route {
                            src_node_id: i as i32,
                            dst_node_id: j as i32,
                            route_id: *id as i32,
                            hops: vec![i as i32, j as i32],
                            // streams: Some("[]".to_string()),
                        };
                        sqlx::query(
                            r#"
                            INSERT INTO routes (src_node_id, dst_node_id, route_id, hops, streams)
                            VALUES ($1, $2, $3, $4, $5)
                            ON CONFLICT (src_node_id, dst_node_id, route_id)
                            DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams
                            "#,
                        )
                        .bind(route.src_node_id)
                        .bind(route.dst_node_id)
                        .bind(route.route_id)
                        .bind(&route.hops)
                        // .bind(&route.streams)
                        .execute(&pool)
                        .await
                        .expect("Failed to insert ring route");
                    }

                    let route = Route {
                        src_node_id: n_nodes as i32,
                        dst_node_id: 1,
                        route_id: *id as i32,
                        hops: vec![n_nodes as i32, 1],
                        // streams: Some("[]".to_string()),
                    };

                    sqlx::query(
                        r#"
                        INSERT INTO routes (src_node_id, dst_node_id, route_id, hops, streams)
                        VALUES ($1, $2, $3, $4, $5)
                        ON CONFLICT (src_node_id, dst_node_id, route_id)
                        DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams
                        "#,
                    )
                    .bind(route.src_node_id)
                    .bind(route.dst_node_id)
                    .bind(route.route_id)
                    .bind(&route.hops)
                    // .bind(&route.streams)
                    .execute(&pool)
                    .await
                    .expect("Failed to insert ring closure route");
                }
            }
        }
    }

    // adds initial routes from the configuration file
    println!("Adding initial routes from the configuration file.");

    for mut route in config.routes.clone() {
        // route.streams = Some(route.streams.unwrap_or_else(|| "[]".to_string()));

        sqlx::query(
            r#"
            INSERT INTO routes (src_node_id, dst_node_id, route_id, hops, streams)
            VALUES ($1, $2, $3, $4, $5)
            ON CONFLICT (src_node_id, dst_node_id, route_id)
            DO UPDATE SET hops = EXCLUDED.hops, streams = EXCLUDED.streams
            "#,
        )
        .bind(route.src_node_id as i32)
        .bind(route.dst_node_id as i32)
        .bind(route.route_id as i32)
        .bind(route.hops.iter().map(|&x| x as i32).collect::<Vec<_>>())
        // .bind(&route.streams)
        .execute(&pool)
        .await
        .expect("Failed to insert initial route");
    }

    pool
}

pub async fn setup_notification(
    db_pool: Arc<Pool<Postgres>>,
    config: config::Config,

    // a hashmap from the node ID to its corresponding WebSocket sink
    node_ws: Arc<RwLock<HashMap<usize, Arc<Mutex<WebSocketWriter>>>>>,
) {
    // Create the notification function and trigger
    let flow_table_name = "routes";

    let create_function_sql = r#"
        CREATE OR REPLACE FUNCTION notify_trigger_function()
        RETURNS TRIGGER AS $$
        BEGIN
            PERFORM pg_notify('auto_sync_routes', '{"op":"' || TG_OP || '","src_node_id":"' || NEW.src_node_id || '","dst_node_id":"' || NEW.dst_node_id || '","route_id":"'|| NEW.route_id || '","hops":"' || array_to_string(NEW.hops, ',') || '","streams":"'|| NEW.streams || '"}');
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

    // Set up listener
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

                    if (channel == "auto_sync_routes" && config.auto_db_sync)
                        || channel == "sync_routes"
                    {
                        // Strato does not support installing routes individually, so we need to find
                        // all routes from the database and re-install them all
                        println!("Installing route updates into the dataplane.");

                        let routes: Vec<Route> = sqlx::query_as("SELECT * FROM routes")
                            .fetch_all(&*db_pool)
                            .await
                            .expect("Failed to fetch routes");

                        // sends install routes message to all nodes
                        let node_ws_guard = node_ws.read().await;

                        for (node_id, ws_arc) in node_ws_guard.iter() {
                            if let Some(msg) = build_install_routes_message(
                                &config,
                                routes.clone(),
                                *node_id as i32,
                            ) {
                                let msg_binary = rmp_serde::to_vec(&msg).unwrap();

                                ws_arc
                                    .lock()
                                    .await
                                    .send(Message::binary(msg_binary))
                                    .await
                                    .unwrap();

                                println!("Installing routes on node {}.", node_id);
                            } else {
                                println!("Error: No node to install flow to.");
                            }
                        }
                    }
                }
                Err(e) => {
                    println!("Error receiving notification: {}", e);
                }
            }
        }
    });
}
