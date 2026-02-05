use sqlx::postgres::PgPoolOptions;
use sqlx::{Pool, Postgres, Row};
use tracing::{error, info};

use crate::config;
use crate::utils::merge_all_routes;

use super::migrations::{reset_db, run_migrations};

/// Connects to and initializes the PostgreSQL database.
pub async fn init_db(config: &config::Config) -> Pool<Postgres> {
    let db_url = format!(
        "postgres://{}:{}@{}:{}/{}",
        config.db.user, config.db.password, config.db.host, config.db.port, config.db.database
    );

    info!("Connecting to PostgreSQL at {}:{}/{}", config.db.host, config.db.port, config.db.database);

    let pool = PgPoolOptions::new()
        .max_connections(100)
        .min_connections(50)
        .connect(&db_url)
        .await
        .unwrap_or_else(|e| {
            panic!("Failed to connect to database: {}", e);
        });

    // Controls initial database reset for dev/test. Default: enabled, unless CONTROLLER_RESET_DB explicitly disables it.
    let reset_enabled = std::env::var("CONTROLLER_RESET_DB")
        .map(|v| matches!(v.as_str(), "1" | "true" | "yes"))
        .unwrap_or(true);
    if reset_enabled {
        reset_db(&pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to reset database: {}", e));
    } else {
        run_migrations(&pool)
            .await
            .unwrap_or_else(|e| panic!("Failed to run migrations: {}", e));
        info!("Database reset disabled via CONTROLLER_RESET_DB env var.");
    }

    seed_routes(&pool, config).await;
    seed_custom_flows(&pool, config).await;

    pool
}

async fn seed_routes(pool: &Pool<Postgres>, config: &config::Config) {
    let all_routes = merge_all_routes(config);
    info!("Adding all {} routes to the database.", all_routes.len());

    for (src_node_id, dst_node_id, edges) in all_routes {
        let edges_json: serde_json::Value = match serde_json::to_value(&edges) {
            Ok(v) => v,
            Err(e) => {
                error!("Failed to convert edges to JSON: {}", e);
                continue;
            }
        };

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
        .fetch_optional(pool)
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
}

async fn seed_custom_flows(pool: &Pool<Postgres>, config: &config::Config) {
    info!("Adding custom flows from the configuration file.");

    for flow in config.flows.clone() {
        let src_node_id = flow.src_node_id as i32;
        let dst_node_id = flow.dst_node_id as i32;

        if let Err(err) = flow.flow_spec.validate() {
            panic!(
                "Invalid custom flow {} -> {} in controller config: {}",
                src_node_id, dst_node_id, err
            );
        }

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
        .fetch_optional(pool)
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
}
