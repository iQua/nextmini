use std::collections::HashSet;

use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres, Row};
use tracing::warn;

use crate::models::Group;

use super::groups::{load_group_members, upsert_group_routes};

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
    let dag_json = serde_json::to_value(
        dag_edges
            .iter()
            .map(|(a, b)| [*a, *b])
            .collect::<Vec<[u32; 2]>>(),
    )?;
    upsert_group_routes(db_pool, group_id, src_node_id, dag_json).await?;
    Ok(())
}
