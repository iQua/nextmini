use std::collections::HashSet;
use std::sync::Arc;

use futures_util::SinkExt;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, FlowTransport, GroupRoutingTableEntry};

use crate::db::{DbEvent, RecomputedGroupRoutes};
use crate::models::{DbFlow, DbFlowRoute, DbRoute, Route};
use crate::utils::{build_flows_for_node, build_group_routes_for_node, build_routes_for_node};
use crate::{NodeWriterMap, WebSocketWriter};

pub fn spawn_db_sync(
    db_pool: Arc<Pool<Postgres>>,
    node_ws: NodeWriterMap,
    mut receiver: mpsc::Receiver<DbEvent>,
    flow_transport: FlowTransport,
) -> tokio::task::JoinHandle<()> {
    tokio::spawn(async move {
        while let Some(event) = receiver.recv().await {
            match event {
                DbEvent::RoutesChanged => {
                    if let Err(e) = sync_routes(&db_pool, &node_ws).await {
                        error!("Failed to sync routes: {}", e);
                    }
                }
                DbEvent::FlowInserted { flow_id } => {
                    if let Err(e) = sync_flow(&db_pool, &node_ws, flow_transport, flow_id).await {
                        error!("Failed to sync flow {}: {}", flow_id, e);
                    }
                }
                DbEvent::GroupRoutesSync { group_id } => {
                    if let Err(e) = sync_group_routes(&db_pool, &node_ws, group_id).await {
                        error!("Failed to sync group routes for group {}: {}", group_id, e);
                    }
                }
            }
        }
        warn!("DB sync task exiting: event channel closed.");
    })
}

async fn sync_routes(db_pool: &Pool<Postgres>, node_ws: &NodeWriterMap) -> anyhow::Result<()> {
    info!("Syncing routes into dataplane connections.");

    let routes_db: Vec<DbRoute> = sqlx::query_as::<_, DbRoute>("SELECT * FROM routes")
        .fetch_all(db_pool)
        .await?;

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

    let send_targets: Vec<(usize, Arc<tokio::sync::Mutex<WebSocketWriter>>)> = {
        let guard = node_ws.read().await;
        guard
            .iter()
            .map(|(node_id, writer)| (*node_id, Arc::clone(writer)))
            .collect()
    };

    for (node_id, writer) in send_targets {
        if let Some(msg) = build_routes_for_node(routes.clone(), node_id as u32) {
            let msg_binary = rmp_serde::to_vec(&msg)?;
            if let Err(e) = writer.lock().await.send(Message::binary(msg_binary)).await {
                error!("Failed to install routes on node {}: {}", node_id, e);
            } else {
                info!("Installed routes on node {}.", node_id);
            }
        } else {
            warn!("No routes to install for node {}.", node_id);
        }
    }

    Ok(())
}

async fn sync_flow(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
    flow_transport: FlowTransport,
    flow_id: i32,
) -> anyhow::Result<()> {
    let flow = sqlx::query_as::<_, DbFlow>("SELECT * FROM flows WHERE id = $1")
        .bind(flow_id)
        .fetch_one(db_pool)
        .await?;

    // Query for any route pinning for this flow
    let flow_routes: Vec<DbFlowRoute> =
        sqlx::query_as::<_, DbFlowRoute>("SELECT * FROM flow_routes WHERE flow_id = $1")
            .bind(flow_id)
            .fetch_all(db_pool)
            .await?;

    let msg = build_flows_for_node(vec![flow.clone()], &flow_routes, flow_transport);
    let msg_binary = rmp_serde::to_vec(&msg)?;

    let src_node_id = flow.src_node_id as usize;
    let dst_node_id = flow.dst_node_id as usize;

    send_to_node(node_ws, src_node_id, &msg_binary, flow_id, "source").await;
    send_to_node(node_ws, dst_node_id, &msg_binary, flow_id, "destination").await;
    Ok(())
}

async fn sync_group_routes(
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
    group_id: i32,
) -> anyhow::Result<()> {
    let Some(plan) = crate::db::recompute_group_routes(group_id, db_pool).await? else {
        return Ok(());
    };

    let nodes_to_notify = multicast_nodes_to_notify(&plan);
    if nodes_to_notify.is_empty() {
        return Ok(());
    }

    let send_targets: Vec<(u32, Arc<tokio::sync::Mutex<WebSocketWriter>>)> = {
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
            plan.group.id as usize,
            plan.group.src_node_id as u32,
            &plan.dag_edges,
            node_id,
            &plan.member_node_set,
        );
        let routes: Vec<GroupRoutingTableEntry> = entry.into_iter().collect();
        let message = ControllerToDataplane::InstallGroupRoutes {
            group_id: plan.group.id as usize,
            src_node_id: plan.group.src_node_id as usize,
            routes,
        };
        let payload = rmp_serde::to_vec(&message)?;
        if let Err(e) = writer.lock().await.send(Message::binary(payload)).await {
            error!(
                "Failed to send InstallGroupRoutes for group {} to node {}: {}",
                plan.group.id, node_id, e
            );
        } else {
            info!(
                "Pushed InstallGroupRoutes for group {} to node {}.",
                plan.group.id, node_id
            );
        }
    }

    Ok(())
}

fn multicast_nodes_to_notify(plan: &RecomputedGroupRoutes) -> HashSet<u32> {
    let mut nodes: HashSet<u32> = plan
        .previous_edges
        .iter()
        .flat_map(|(a, b)| [*a, *b])
        .collect();
    nodes.extend(plan.dag_nodes.iter().copied());
    nodes.insert(plan.group.src_node_id as u32);
    nodes.extend(plan.member_node_ids.iter().copied());
    nodes
}

async fn send_to_node(
    node_ws: &NodeWriterMap,
    node_id: usize,
    msg_binary: &[u8],
    flow_id: i32,
    label: &str,
) {
    let ws_arc_opt = { node_ws.read().await.get(&node_id).cloned() };
    let Some(ws_arc) = ws_arc_opt else {
        return;
    };

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
