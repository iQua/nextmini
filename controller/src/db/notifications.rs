use std::collections::HashMap;
use std::sync::Arc;

use futures_util::{SinkExt, StreamExt};
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::sync::{Mutex, RwLockReadGuard};
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use crate::models::{DbFlow, DbRoute, Route};
use crate::utils::{build_flows_for_node, build_routes_for_node};
use crate::{NodeWriterMap, WebSocketWriter};
use nextmini_messages::FlowTransport;

use super::group_routes::recompute_and_push_group_routes;

type NodeWsGuard<'a> = RwLockReadGuard<'a, HashMap<usize, Arc<Mutex<WebSocketWriter>>>>;

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

    let mut conn = match db_pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            error!(
                "Failed to acquire connection for route trigger setup: {}",
                e
            );
            return;
        }
    };

    let row: Option<(i32,)> = match sqlx::query_as(&check_trigger_sql)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to check route trigger existence: {}", e);
            return;
        }
    };

    if row.is_none() {
        if let Err(e) = sqlx::query(create_function_sql).execute(&mut *conn).await {
            error!("Failed to create route notification function: {}", e);
        }
        if let Err(e) = sqlx::query(&create_trigger_sql).execute(&mut *conn).await {
            error!("Failed to create route trigger: {}", e);
        }
    }

    // sets up a listener
    let mut listener = match PgListener::connect_with(&db_pool).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to connect route listener: {}", e);
            return;
        }
    };
    if let Err(e) = listener.listen("auto_sync_routes").await {
        error!("Failed to listen to auto_sync_routes: {}", e);
        return;
    }
    if let Err(e) = listener.listen("sync_routes").await {
        error!("Failed to listen to sync_routes: {}", e);
        return;
    }

    // spawns a task to handle notifications by installing the routes to all available nodes
    tokio::spawn(async move {
        let mut stream = listener.into_stream(); // converts listener to stream

        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let channel = notif.channel();

                    if channel != "auto_sync_routes" {
                        continue;
                    }

                    // Nextmini does not support installing routes individually, so we need to find
                    // all routes from the database and re-install them all.
                    info!("Installing route updates into the dataplane.");

                    let routes_db: Vec<DbRoute> =
                        match sqlx::query_as::<_, DbRoute>("SELECT * FROM routes")
                            .fetch_all(&*db_pool)
                            .await
                        {
                            Ok(v) => v,
                            Err(e) => {
                                error!("Failed to fetch routes: {}", e);
                                continue;
                            }
                        };

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
                        if let Some(msg) = build_routes_for_node(routes.clone(), *node_id as u32) {
                            let msg_binary = rmp_serde::to_vec(&msg).unwrap();
                            if let Err(e) =
                                ws_arc.lock().await.send(Message::binary(msg_binary)).await
                            {
                                error!("Failed to install routes on node {}: {}", node_id, e);
                            } else {
                                info!("Installing routes on node {}.", node_id);
                            }
                        } else {
                            warn!("No routes to install for node {}.", node_id);
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
    let mut listener = match PgListener::connect_with(&db_pool).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to connect listener for multicast groups: {}", e);
            return;
        }
    };

    if let Err(e) = listener.listen("sync_group_routes").await {
        error!("Failed to listen to sync_group_routes: {}", e);
        return;
    }

    tokio::spawn(async move {
        let mut stream = listener.into_stream();
        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let payload = notif.payload();
                    let group_id_opt = parse_group_id(payload);

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

pub async fn setup_flow_notification(
    db_pool: Arc<Pool<Postgres>>,
    node_ws: NodeWriterMap,
    flow_transport: FlowTransport,
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

    let mut conn = match db_pool.acquire().await {
        Ok(c) => c,
        Err(e) => {
            error!("Failed to acquire connection for flow trigger setup: {}", e);
            return;
        }
    };

    // checks and creates a flow trigger if it doesn't exist
    let flow_row: Option<(i32,)> = match sqlx::query_as(check_flow_trigger_sql)
        .fetch_optional(&mut *conn)
        .await
    {
        Ok(r) => r,
        Err(e) => {
            error!("Failed to check flow trigger existence: {}", e);
            return;
        }
    };

    if flow_row.is_none() {
        if let Err(e) = sqlx::query(create_flow_function_sql)
            .execute(&mut *conn)
            .await
        {
            error!("Failed to create flow notification function: {}", e);
        }
        if let Err(e) = sqlx::query(create_flow_trigger_sql)
            .execute(&mut *conn)
            .await
        {
            error!("Failed to create flow trigger: {}", e);
        }
        info!("Created flow notification trigger.");
    }

    // sets up a listener for flow notifications
    let mut listener = match PgListener::connect_with(&db_pool).await {
        Ok(l) => l,
        Err(e) => {
            error!("Failed to connect flow listener: {}", e);
            return;
        }
    };
    if let Err(e) = listener.listen("auto_sync_flows").await {
        error!("Failed to listen to auto_sync_flows: {}", e);
        return;
    }

    // spawns a task to handle flow notifications by installing flows to relevant nodes
    tokio::spawn(async move {
        let mut stream = listener.into_stream();

        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let channel = notif.channel();
                    if channel != "auto_sync_flows" {
                        continue;
                    }

                    let payload = notif.payload();
                    info!("Received flow notification: {}", payload);

                    let Some(id) = parse_new_flow_id(payload) else {
                        error!(
                            "Failed to parse newly_inserted_id from notification payload: {}",
                            payload
                        );
                        continue;
                    };

                    let flow =
                        match sqlx::query_as::<_, DbFlow>("SELECT * FROM flows WHERE id = $1")
                            .bind(id)
                            .fetch_one(&*db_pool)
                            .await
                        {
                            Ok(flow) => flow,
                            Err(e) => {
                                error!("Failed to fetch newly inserted flow {}: {}", id, e);
                                continue;
                            }
                        };

                    info!("Installing newly inserted flow {} into the dataplane.", id);
                    let node_ws_guard = node_ws.read().await;
                    let msg = build_flows_for_node(vec![flow.clone()], flow_transport);
                    let msg_binary = rmp_serde::to_vec(&msg).unwrap();

                    let src_node_id = flow.src_node_id as usize;
                    let dst_node_id = flow.dst_node_id as usize;

                    send_to_node(&node_ws_guard, src_node_id, &msg_binary, id, "source").await;
                    send_to_node(&node_ws_guard, dst_node_id, &msg_binary, id, "destination").await;
                }
                Err(e) => {
                    error!("Error receiving notification: {}", e);
                }
            }
        }
    });
}

fn parse_group_id(payload: &str) -> Option<i32> {
    let parsed = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    parsed
        .get("group_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
}

fn parse_new_flow_id(payload: &str) -> Option<i32> {
    let json = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    json.get("newly_inserted_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
}

async fn send_to_node(
    node_ws_guard: &NodeWsGuard<'_>,
    node_id: usize,
    msg_binary: &[u8],
    flow_id: i32,
    label: &str,
) {
    if let Some(ws_arc) = node_ws_guard.get(&node_id) {
        match ws_arc
            .lock()
            .await
            .send(Message::binary(msg_binary.to_vec()))
            .await
        {
            Ok(_) => info!("Sent new flow {} to {} node {}.", flow_id, label, node_id),
            Err(e) => error!(
                "Failed to send flow {} to {} node {}: {}",
                flow_id, label, node_id, e
            ),
        }
    }
}
