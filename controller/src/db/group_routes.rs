use std::collections::HashSet;

use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};
use tracing::warn;

use crate::models::Group;
use super::groups::load_group_members;

pub(crate) struct RecomputedGroupRoutes {
    pub group: Group,
    pub member_node_ids: Vec<u32>,
    pub member_node_set: HashSet<u32>,
    pub previous_edges: Vec<(u32, u32)>,
    pub dag_edges: Vec<(u32, u32)>,
    pub dag_nodes: HashSet<u32>,
}

pub(crate) async fn recompute_group_routes(
    group_id: i32,
    db_pool: &Pool<Postgres>,
) -> AnyResult<Option<RecomputedGroupRoutes>> {
    let Some(group) = load_group(db_pool, group_id).await? else {
        warn!(
            "Received multicast recompute for unknown group {}",
            group_id
        );
        return Ok(None);
    };

    // The controller no longer computes multicast DAGs from unicast routes.
    // Group DAG edges are expected to be provided externally (e.g., by an LP solver via SetGroupRoutes).
    let previous_edges = load_previous_edges(db_pool, group_id).await?;
    let members = load_group_members(db_pool, group_id).await?;
    let member_node_ids: Vec<u32> = members.iter().map(|m| m.node_id as u32).collect();
    let member_node_set: HashSet<u32> = member_node_ids.iter().copied().collect();

    // Just reuse the stored DAG edges for fan-out; membership changes only affect local delivery.
    let mut dag_nodes = HashSet::new();
    for (a, b) in &previous_edges {
        dag_nodes.insert(*a);
        dag_nodes.insert(*b);
    }
    let dag_edges = previous_edges.clone();

    Ok(Some(RecomputedGroupRoutes {
        group,
        member_node_ids,
        member_node_set,
        previous_edges,
        dag_edges,
        dag_nodes,
    }))
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
