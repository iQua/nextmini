use std::collections::HashSet;

use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};
use tracing::warn;

use super::groups::load_group_members;
use crate::models::{DbGroupRoute, Group};
use nextmini_messages::GroupRouteTree;

pub(crate) struct RecomputedGroupRoutes {
    pub group: Group,
    pub member_node_ids: Vec<u32>,
    pub member_node_set: HashSet<u32>,
    pub trees: Vec<GroupRouteTree>,
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

    // Group DAG edges are expected to be provided externally (e.g. via SetGroupRoutes*).
    let trees = load_group_trees(db_pool, group_id).await?;
    let members = load_group_members(db_pool, group_id).await?;
    let member_node_ids: Vec<u32> = members.iter().map(|m| m.node_id as u32).collect();
    let member_node_set: HashSet<u32> = member_node_ids.iter().copied().collect();

    Ok(Some(RecomputedGroupRoutes {
        group,
        member_node_ids,
        member_node_set,
        trees,
    }))
}

async fn load_group(db_pool: &Pool<Postgres>, group_id: i32) -> AnyResult<Option<Group>> {
    let group =
        sqlx::query_as::<_, Group>("SELECT id, src_node_id, group_ip FROM groups WHERE id = $1")
            .bind(group_id)
            .fetch_optional(db_pool)
            .await?;
    Ok(group)
}

async fn load_group_trees(
    db_pool: &Pool<Postgres>,
    group_id: i32,
) -> AnyResult<Vec<GroupRouteTree>> {
    let rows = sqlx::query_as::<_, DbGroupRoute>(
        r#"
        SELECT group_id, tree_id, src_node_id, weight, edges
        FROM group_routes
        WHERE group_id = $1
        ORDER BY tree_id ASC
        "#,
    )
    .bind(group_id)
    .fetch_all(db_pool)
    .await?;

    let mut trees = Vec::with_capacity(rows.len());
    for row in rows {
        let tree_id = usize::try_from(row.tree_id).map_err(|_| {
            anyhow::anyhow!(
                "group_routes tree_id must be non-negative (group_id={}, tree_id={})",
                group_id,
                row.tree_id
            )
        })?;

        trees.push(GroupRouteTree {
            tree_id,
            weight: row.weight,
            edges: decode_tree_edges(row.edges)?,
        });
    }

    Ok(trees)
}

fn decode_tree_edges(value: serde_json::Value) -> AnyResult<Vec<(u32, u32)>> {
    if let Ok(raw) = serde_json::from_value::<Vec<[u32; 2]>>(value.clone()) {
        return Ok(raw.into_iter().map(|pair| (pair[0], pair[1])).collect());
    }

    Ok(serde_json::from_value::<Vec<(u32, u32)>>(value)?)
}
