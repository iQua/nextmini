use std::net::Ipv4Addr;

use anyhow::Result as AnyResult;
use sqlx::{Pool, Postgres};

use crate::models::{Group, GroupMember};
use crate::utils::allocate_multicast_ip;

pub async fn create_group(
    db_pool: &Pool<Postgres>,
    label: &str,
    src_node_id: usize,
    base_addr: Ipv4Addr,
    mask: Ipv4Addr,
) -> AnyResult<Group> {
    let mut tx = db_pool.begin().await?;
    let next_id: i64 = sqlx::query_scalar("SELECT nextval('groups_id_seq')")
        .fetch_one(&mut *tx)
        .await?;

    let group_ip = allocate_multicast_ip(base_addr, mask, next_id as u32);
    let group_ip_string = group_ip.to_string();

    let group = sqlx::query_as::<_, Group>(
        r#"
        INSERT INTO groups (id, label, src_node_id, group_ip)
        VALUES ($1, $2, $3, $4)
        RETURNING id, label, src_node_id, group_ip
        "#,
    )
    .bind(next_id as i32)
    .bind(label)
    .bind(src_node_id as i32)
    .bind(&group_ip_string)
    .fetch_one(&mut *tx)
    .await?;

    tx.commit().await?;
    Ok(group)
}

pub async fn add_group_member(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    node_id: usize,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO group_members (group_id, node_id)
        VALUES ($1, $2)
        ON CONFLICT DO NOTHING
        "#,
    )
    .bind(group_id)
    .bind(node_id as i32)
    .execute(db_pool)
    .await?;
    Ok(())
}

pub async fn remove_group_member(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    node_id: usize,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        DELETE FROM group_members
        WHERE group_id = $1 AND node_id = $2
        "#,
    )
    .bind(group_id)
    .bind(node_id as i32)
    .execute(db_pool)
    .await?;
    Ok(())
}

pub async fn load_group_directory(db_pool: &Pool<Postgres>) -> AnyResult<Vec<Group>> {
    let groups = sqlx::query_as::<_, Group>(
        r#"
        SELECT id, label, src_node_id, group_ip
        FROM groups
        ORDER BY id
        "#,
    )
    .fetch_all(db_pool)
    .await?;
    Ok(groups)
}

pub async fn load_group_members(
    db_pool: &Pool<Postgres>,
    group_id: i32,
) -> AnyResult<Vec<GroupMember>> {
    let members = sqlx::query_as::<_, GroupMember>(
        r#"
        SELECT group_id, node_id
        FROM group_members
        WHERE group_id = $1
        ORDER BY node_id
        "#,
    )
    .bind(group_id)
    .fetch_all(db_pool)
    .await?;
    Ok(members)
}

pub(super) async fn upsert_group_routes(
    db_pool: &Pool<Postgres>,
    group_id: i32,
    src_node_id: i32,
    edges: serde_json::Value,
) -> AnyResult<()> {
    sqlx::query(
        r#"
        INSERT INTO group_routes (group_id, src_node_id, edges)
        VALUES ($1, $2, $3)
        ON CONFLICT (group_id)
        DO UPDATE SET src_node_id = EXCLUDED.src_node_id, edges = EXCLUDED.edges, updated_at = EXTRACT(EPOCH FROM NOW())::BIGINT*1000
        "#,
    )
    .bind(group_id)
    .bind(src_node_id)
    .bind(edges)
    .execute(db_pool)
    .await?;
    Ok(())
}
