use std::sync::Arc;

use futures_util::StreamExt;
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::events::DbEvent;

pub async fn setup_route_notification(db_pool: Arc<Pool<Postgres>>, sender: mpsc::Sender<DbEvent>) {
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

    tokio::spawn(async move {
        let mut stream = listener.into_stream();
        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    let channel = notif.channel();
                    if channel != "auto_sync_routes" && channel != "sync_routes" {
                        continue;
                    }
                    info!("Received route notification on channel {}.", channel);
                    if let Err(e) = sender.send(DbEvent::RoutesChanged).await {
                        warn!("Dropping route event (receiver closed): {}", e);
                        return;
                    }
                }
                Err(e) => error!("Error receiving route notification: {}", e),
            }
        }
    });
}

pub async fn setup_group_notification(db_pool: Arc<Pool<Postgres>>, sender: mpsc::Sender<DbEvent>) {
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
                        if let Err(e) = sender.send(DbEvent::GroupRoutesSync { group_id }).await {
                            warn!("Dropping group event (receiver closed): {}", e);
                            return;
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

pub async fn setup_flow_notification(db_pool: Arc<Pool<Postgres>>, sender: mpsc::Sender<DbEvent>) {
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

    tokio::spawn(async move {
        let mut stream = listener.into_stream();
        while let Some(notification) = stream.next().await {
            match notification {
                Ok(notif) => {
                    if notif.channel() != "auto_sync_flows" {
                        continue;
                    }
                    let payload = notif.payload();
                    info!("Received flow notification: {}", payload);
                    let Some(flow_id) = parse_new_flow_id(payload) else {
                        warn!("Ignored malformed auto_sync_flows payload: {}", payload);
                        continue;
                    };
                    if let Err(e) = sender.send(DbEvent::FlowInserted { flow_id }).await {
                        warn!("Dropping flow event (receiver closed): {}", e);
                        return;
                    }
                }
                Err(e) => error!("Error receiving flow notification: {}", e),
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
