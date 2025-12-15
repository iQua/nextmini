use std::collections::HashSet;
use std::sync::Arc;

use anyhow::Result as AnyResult;
use futures_util::SinkExt;
use sqlx::{Pool, Postgres, Row};
use tokio::sync::Mutex;
use tokio_tungstenite::tungstenite::Message;
use tracing::{error, info, warn};

use nextmini_messages::{ControllerToDataplane, GroupRoutingTableEntry};

use crate::models::Group;
use crate::utils::build_group_routes_for_node;
use crate::{NodeWriterMap, WebSocketWriter};

use super::groups::{load_group_members, upsert_group_routes};

pub(super) async fn recompute_and_push_group_routes(
    group_id: i32,
    db_pool: &Pool<Postgres>,
    node_ws: &NodeWriterMap,
) -> AnyResult<()> {
    let Some(group) = load_group(db_pool, group_id).await? else {
        warn!(
            "Received multicast recompute for unknown group {}",
            group_id
        );
        return Ok(());
    };

    let previous_edges = load_previous_edges(db_pool, group_id).await?;
    let members = load_group_members(db_pool, group_id).await?;
    let member_node_ids: Vec<u32> = members.iter().map(|m| m.node_id as u32).collect();
    let member_node_set: HashSet<u32> = member_node_ids.iter().copied().collect();

    let (dag_edges, dag_nodes) = compute_multicast_dag(
        db_pool,
        group.src_node_id as i32,
        &member_node_ids,
        group_id,
    )
    .await?;

    persist_multicast_dag(db_pool, group_id, group.src_node_id, &dag_edges).await?;

    let nodes_to_notify = nodes_to_notify(
        &previous_edges,
        &dag_nodes,
        group.src_node_id as u32,
        &member_node_ids,
    );
    if nodes_to_notify.is_empty() {
        return Ok(());
    }

    let send_targets = resolve_send_targets(node_ws, &nodes_to_notify).await;
    if send_targets.is_empty() {
        warn!(
            "No active websocket connections available for multicast group {} update.",
            group_id
        );
        return Ok(());
    }

    push_group_routes_to_targets(&group, &dag_edges, &member_node_set, send_targets).await?;
    Ok(())
}

async fn load_group(db_pool: &Pool<Postgres>, group_id: i32) -> AnyResult<Option<Group>> {
    let group = sqlx::query_as::<_, Group>(
        "SELECT id, label, src_node_id, group_ip FROM groups WHERE id = $1",
    )
    .bind(group_id)
    .fetch_optional(db_pool)
    .await?;
    Ok(group)
}

async fn load_previous_edges(
    db_pool: &Pool<Postgres>,
    group_id: i32,
) -> AnyResult<Vec<(u32, u32)>> {
    let previous_edges_value: Option<serde_json::Value> =
        sqlx::query_scalar("SELECT edges FROM group_routes WHERE group_id = $1")
            .bind(group_id)
            .fetch_optional(db_pool)
            .await?;

    let edges = previous_edges_value
        .as_ref()
        .map(|value| serde_json::from_value::<Vec<(u32, u32)>>(value.clone()).unwrap_or_default())
        .unwrap_or_default();
    Ok(edges)
}

async fn compute_multicast_dag(
    db_pool: &Pool<Postgres>,
    src_node_id: i32,
    member_node_ids: &[u32],
    group_id: i32,
) -> AnyResult<(Vec<(u32, u32)>, HashSet<u32>)> {
    let mut dag_edges_set: HashSet<(u32, u32)> = HashSet::new();
    let mut dag_nodes: HashSet<u32> = HashSet::new();

    for member in member_node_ids {
        let route_row = sqlx::query(
            r#"SELECT edges FROM routes WHERE src_node_id = $1 AND dst_node_id = $2 ORDER BY route_id LIMIT 1"#,
        )
        .bind(src_node_id)
        .bind(*member as i32)
        .fetch_optional(db_pool)
        .await?;

        let Some(row) = route_row else {
            warn!(
                "No unicast route from {} to member {} when recomputing multicast group {}.",
                src_node_id, member, group_id
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
                src_node_id, member
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
    Ok((dag_edges, dag_nodes))
}

async fn persist_multicast_dag(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    src_node_id: i32,
    dag_edges: &[(u32, u32)],
) -> AnyResult<()> {
    // Persist DAG edges (even empty) for audit and diff.
    let dag_json = serde_json::to_value(
        dag_edges
            .iter()
            .map(|(a, b)| [*a, *b])
            .collect::<Vec<[u32; 2]>>(),
    )?;
    upsert_group_routes(db_pool, group_id, src_node_id, dag_json).await?;
    Ok(())
}

fn nodes_to_notify(
    previous_edges: &[(u32, u32)],
    dag_nodes: &HashSet<u32>,
    src_node_id: u32,
    member_node_ids: &[u32],
) -> HashSet<u32> {
    // Determine which nodes need notifications (previous DAG participants + current DAG nodes + members + source).
    let mut nodes: HashSet<u32> = previous_edges.iter().flat_map(|(a, b)| [*a, *b]).collect();
    nodes.extend(dag_nodes.iter());
    nodes.insert(src_node_id);
    nodes.extend(member_node_ids.iter().copied());
    nodes
}

async fn resolve_send_targets(
    node_ws: &NodeWriterMap,
    nodes_to_notify: &HashSet<u32>,
) -> Vec<(u32, Arc<Mutex<WebSocketWriter>>)> {
    let guard = node_ws.read().await;
    nodes_to_notify
        .iter()
        .filter_map(|node| {
            guard
                .get(&(*node as usize))
                .map(|writer| (*node, Arc::clone(writer)))
        })
        .collect()
}

async fn push_group_routes_to_targets(
    group: &Group,
    dag_edges: &[(u32, u32)],
    member_node_set: &HashSet<u32>,
    send_targets: Vec<(u32, Arc<Mutex<WebSocketWriter>>)>,
) -> AnyResult<()> {
    for (node_id, writer) in send_targets {
        let entry = build_group_routes_for_node(
            group.id as usize,
            group.src_node_id as u32,
            dag_edges,
            node_id,
            member_node_set,
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
                group.id, node_id, e
            );
        } else {
            info!(
                "Pushed InstallGroupRoutes for group {} to node {}.",
                group.id, node_id
            );
        }
    }
    Ok(())
}
