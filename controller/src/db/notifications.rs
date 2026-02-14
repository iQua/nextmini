use std::sync::Arc;

use futures_util::StreamExt;
use sqlx::postgres::PgListener;
use sqlx::{Pool, Postgres};
use tokio::sync::mpsc;
use tracing::{error, info, warn};

use super::events::DbEvent;

pub async fn setup_route_notification(db_pool: Arc<Pool<Postgres>>, sender: mpsc::Sender<DbEvent>) {
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
                    let group_sync = parse_group_sync_payload(payload);
                    if let Some((group_id, prior_member_node_id)) = group_sync {
                        if let Err(e) = sender
                            .send(DbEvent::GroupRoutesSync {
                                group_id,
                                prior_member_node_id,
                            })
                            .await
                        {
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

fn parse_group_sync_payload(payload: &str) -> Option<(i32, Option<u32>)> {
    let parsed = serde_json::from_str::<serde_json::Value>(payload).ok()?;

    let group_id = parsed.get("group_id").and_then(parse_json_i32)?;
    let prior_member_node_id = parsed
        .get("node_id")
        .and_then(parse_json_i32)
        .and_then(|node_id| u32::try_from(node_id).ok());

    Some((group_id, prior_member_node_id))
}

fn parse_json_i32(value: &serde_json::Value) -> Option<i32> {
    match value {
        serde_json::Value::String(s) => s.parse::<i32>().ok(),
        serde_json::Value::Number(n) => n.as_i64().and_then(|v| i32::try_from(v).ok()),
        _ => None,
    }
}

fn parse_new_flow_id(payload: &str) -> Option<i32> {
    let json = serde_json::from_str::<serde_json::Value>(payload).ok()?;
    json.get("newly_inserted_id")
        .and_then(|v| v.as_str())
        .and_then(|s| s.parse::<i32>().ok())
}

#[cfg(test)]
mod tests {
    use super::parse_group_sync_payload;

    #[test]
    fn parse_group_sync_payload_accepts_legacy_payload_without_node_id() {
        let payload = r#"{"group_id":"42"}"#;
        let parsed = parse_group_sync_payload(payload).expect("legacy payload should parse");
        assert_eq!(parsed, (42, None));
    }

    #[test]
    fn parse_group_sync_payload_extracts_node_id_hint() {
        let payload = r#"{"group_id":"7","node_id":"19"}"#;
        let parsed = parse_group_sync_payload(payload).expect("payload should parse");
        assert_eq!(parsed, (7, Some(19)));
    }
}
