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
