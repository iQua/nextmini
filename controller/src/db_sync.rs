use std::collections::HashSet;
use std::sync::{Arc, Mutex as StdMutex};

use futures_util::SinkExt;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, FlowTransport};

use crate::db::{DbEvent, RecomputedGroupRoutes};
use crate::models::{DbFlow, DbFlowRoute, DbRoute, Route};
use crate::utils::{
    LosslessSessionIdAllocator, build_flows_for_node, build_group_routes_for_node_multitree,
    build_routes_for_node,
};
use crate::{NodeWriterMap, WebSocketWriter};

pub fn spawn_db_sync(
    db_pool: Arc<Pool<Postgres>>,
    node_ws: NodeWriterMap,
    mut receiver: mpsc::Receiver<DbEvent>,
    flow_transport: FlowTransport,
    lossless_session_ids: Arc<StdMutex<LosslessSessionIdAllocator>>,
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
                    if let Err(e) = sync_flow(
                        &db_pool,
                        &node_ws,
                        flow_transport,
                        flow_id,
                        &lossless_session_ids,
                    )
                    .await
                    {
                        error!("Failed to sync flow {}: {}", flow_id, e);
                    }
                }
                DbEvent::GroupRoutesSync {
                    group_id,
                    prior_member_node_id,
                } => {
                    if let Err(e) =
                        sync_group_routes(&db_pool, &node_ws, group_id, prior_member_node_id).await
                    {
                        error!("Failed to sync group routes for group {}: {}", group_id, e);
                    }
                }
                DbEvent::ProbeRequested {
                    id,
                    from_node_id,
                    to_node_id,
                    probe_bytes,
                } => {
                    if let Err(e) =
                        send_probe_link(&node_ws, id, from_node_id, to_node_id, probe_bytes).await
                    {
                        error!("Failed to send probe request {}: {}", id, e);
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
    lossless_session_ids: &StdMutex<LosslessSessionIdAllocator>,
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

    let msg = {
        let mut allocator = lossless_session_ids
            .lock()
            .expect("lossless session-id allocator mutex poisoned");
        build_flows_for_node(
            vec![flow.clone()],
            &flow_routes,
            flow_transport,
            &mut allocator,
        )
    };
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
    prior_member_node_id: Option<u32>,
) -> anyhow::Result<()> {
    let Some(plan) = crate::db::recompute_group_routes(group_id, db_pool).await? else {
        return Ok(());
    };

    let nodes_to_notify = multicast_nodes_to_notify(&plan, prior_member_node_id);
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
        let routes = match build_group_routes_for_node_multitree(
            plan.group.id as usize,
            plan.group.src_node_id as u32,
            &plan.trees,
            node_id,
            &plan.member_node_set,
        ) {
            Ok(routes) => routes,
            Err(e) => {
                error!(
                    "Failed to build multi-tree InstallGroupRoutes payload for group {} node {}: {}",
                    plan.group.id, node_id, e
                );
                continue;
            }
        };
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

fn multicast_nodes_to_notify(
    plan: &RecomputedGroupRoutes,
    prior_member_node_id: Option<u32>,
) -> HashSet<u32> {
    let mut nodes: HashSet<u32> = plan
        .trees
        .iter()
        .flat_map(|tree| tree.edges.iter().flat_map(|(a, b)| [*a, *b]))
        .collect();
    nodes.insert(plan.group.src_node_id as u32);
    nodes.extend(plan.member_node_ids.iter().copied());
    if let Some(node_id) = prior_member_node_id {
        nodes.insert(node_id);
    }
    nodes
}

async fn send_probe_link(
    node_ws: &NodeWriterMap,
    request_id: i32,
    from_node_id: i32,
    to_node_id: i32,
    probe_bytes: i32,
) -> anyhow::Result<()> {
    let msg = ControllerToDataplane::ProbeLink {
        remote_node_id: to_node_id as usize,
        probe_id: request_id as u64,
        probe_bytes: probe_bytes as usize,
    };
    let msg_binary = rmp_serde::to_vec(&msg)?;

    let ws_arc = { node_ws.read().await.get(&(from_node_id as usize)).cloned() };
    let Some(ws_arc) = ws_arc else {
        anyhow::bail!("no websocket for node {}", from_node_id);
    };

    ws_arc
        .lock()
        .await
        .send(Message::binary(msg_binary))
        .await?;

    info!(
        "Sent ProbeLink {} to node {} (target node {}, {} bytes).",
        request_id, from_node_id, to_node_id, probe_bytes
    );
    Ok(())
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

#[cfg(test)]
mod tests {
    use std::collections::HashSet;

    use nextmini_messages::GroupRouteTree;

    use super::multicast_nodes_to_notify;
    use crate::db::RecomputedGroupRoutes;
    use crate::models::Group;

    #[test]
    fn multicast_nodes_to_notify_includes_prior_member_hint() {
        let plan = RecomputedGroupRoutes {
            group: Group {
                id: 8,
                src_node_id: 1,
                group_ip: "224.0.0.8".to_string(),
            },
            member_node_ids: vec![3],
            member_node_set: HashSet::from([3u32]),
            trees: vec![GroupRouteTree {
                tree_id: 0,
                weight: None,
                edges: vec![(1, 2)],
            }],
        };

        let nodes = multicast_nodes_to_notify(&plan, Some(9));
        assert!(
            nodes.contains(&9),
            "prior member hint should be included so member-only leaves get cleared"
        );
    }
}
