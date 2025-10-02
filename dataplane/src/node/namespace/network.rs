use futures::TryStreamExt;
use rtnetlink::{AddressHandle, Handle, LinkBridge, LinkUnspec, LinkVeth, new_connection};
use std::{fmt, net::Ipv4Addr, str::FromStr};
use tokio::time::{Duration, sleep};
use tracing::{error, info};

#[derive(Debug)]
pub enum NetworkError {
    ConnectionError(rtnetlink::Error),
    OperationError(String),
    AddressParseError(std::net::AddrParseError),
    Other(std::io::Error),
}

impl fmt::Display for NetworkError {
    fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
        match self {
            NetworkError::ConnectionError(e) => write!(f, "Connection error: {}.", e),
            NetworkError::OperationError(msg) => write!(f, "Operation error: {}.", msg),
            NetworkError::AddressParseError(e) => write!(f, "Address parse error: {}.", e),
            NetworkError::Other(e) => write!(f, "IO error: {}.", e),
        }
    }
}

impl std::error::Error for NetworkError {}

// Implementing From<T> for NetworkError allows you to use '?' in functions that return Result<T, NetworkError>
impl From<rtnetlink::Error> for NetworkError {
    fn from(err: rtnetlink::Error) -> Self {
        NetworkError::ConnectionError(err)
    }
}

impl From<std::net::AddrParseError> for NetworkError {
    fn from(err: std::net::AddrParseError) -> Self {
        NetworkError::AddressParseError(err)
    }
}

impl From<std::io::Error> for NetworkError {
    fn from(err: std::io::Error) -> Self {
        NetworkError::Other(err)
    }
}

pub async fn prepare_net(
    bridge_name: String,
    bridge_ip: &str,
    subnet: u8,
    idx: usize,
) -> Result<(u32, u32, u32), NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    info!("Interact with bridge {bridge_name} at cidr {bridge_ip}/{subnet}.");

    // creates bridge if not exist
    let bridge_idx = match get_bridge_idx(&handle, bridge_name.clone()).await {
        Ok(idx) => {
            info!("The bridge {} already exist.", bridge_name);
            idx
        }
        Err(_) => create_bridge(bridge_name, bridge_ip, subnet).await?,
    };

    let (veth_idx, veth2_idx) = create_veth_pair(bridge_idx, idx).await?;

    Ok((bridge_idx, veth_idx, veth2_idx))
}

async fn get_bridge_idx(handle: &Handle, bridge_name: String) -> Result<u32, NetworkError> {
    // retrieves bridge index
    let bridge_idx = handle
        .link()
        .get()
        .match_name(bridge_name)
        .execute()
        .try_next()
        .await?
        .ok_or_else(|| NetworkError::OperationError("Failed to get bridge index.".to_string()))?
        .header
        .index;

    Ok(bridge_idx)
}

// Create and bring up a bridge.
async fn create_bridge(name: String, bridge_ip: &str, subnet: u8) -> Result<u32, NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    handle
        .link()
        .add(LinkBridge::new(&name).build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Create bridge with name {} failed: {}.",
                name.clone(),
                e
            ))
        })?;

    let bridge_idx = handle
        .link()
        .get()
        .match_name(name)
        .execute()
        .try_next()
        .await?
        .ok_or_else(|| NetworkError::OperationError("Failed to get bridge index.".to_string()))?
        .header
        .index;

    // adds ip address to bridge
    let bridge_addr = std::net::IpAddr::V4(Ipv4Addr::from_str(bridge_ip)?);
    AddressHandle::new(handle.clone())
        .add(bridge_idx, bridge_addr, subnet)
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!("Add IP address to bridge failed: {}.", e))
        })?;

    // sets bridge up
    handle
        .link()
        .set(LinkUnspec::new_with_index(bridge_idx).up().build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set bridge with idx {} to up failed: {}.",
                bridge_idx, e
            ))
        })?;

    Ok(bridge_idx)
}

async fn create_veth_pair(bridge_idx: u32, idx: usize) -> Result<(u32, u32), NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    // create veth interfaces with unique names based on idx
    let veth: String = format!("veth{}a", idx);
    let veth_2: String = format!("veth{}b", idx);

    handle
        .link()
        .add(LinkVeth::new(&veth, &veth_2).build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Create veth pair {} and {} failed: {}.",
                veth, veth_2, e
            ))
        })?;

    let veth_idx = handle
        .link()
        .get()
        .match_name(veth.clone())
        .execute()
        .try_next()
        .await?
        .ok_or_else(|| NetworkError::OperationError("Failed to get veth index.".to_string()))?
        .header
        .index;

    let veth_2_idx = handle
        .link()
        .get()
        .match_name(veth_2.clone())
        .execute()
        .try_next()
        .await?
        .ok_or_else(|| NetworkError::OperationError("Failed to get veth index.".to_string()))?
        .header
        .index;

    // sets master veth to bridge (attach to bridge BEFORE bringing up)
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(veth_idx)
                .controller(bridge_idx)
                .build(),
        )
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set veth with idx {} to bridge with idx {} failed: {}.",
                veth_idx, bridge_idx, e
            ))
        })?;

    // sets master veth up AFTER attaching to bridge
    // sets master veth to bridge (attach to bridge but DON'T bring up yet)
    // The veth master side should only be brought up AFTER the peer side is up
    // to avoid NO-CARRIER state during the race condition window
    Ok((veth_idx, veth_2_idx))
}

pub async fn join_veth_to_ns(veth_idx: u32, pid: u32) -> Result<(), NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    // sets veth to the process network namespace
    handle
        .link()
        .set(
            LinkUnspec::new_with_index(veth_idx)
                .setns_by_pid(pid)
                .build(),
        )
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set veth with idx {} to process with pid {} failed: {}.",
                veth_idx, pid, e
            ))
        })?;

    Ok(())
}

pub async fn bring_up_master_veth(veth_idx: u32) -> Result<(), NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    // Verify the interface exists before trying to bring it up
    let link = handle
        .link()
        .get()
        .match_index(veth_idx)
        .execute()
        .try_next()
        .await?;

    if link.is_none() {
        return Err(NetworkError::OperationError(format!(
            "Master veth with index {} not found",
            veth_idx
        )));
    }

    // Bring up the master veth interface AFTER the peer is configured
    // This prevents NO-CARRIER state that occurs when one end is up and the other is down
    handle
        .link()
        .set(LinkUnspec::new_with_index(veth_idx).up().build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set master veth with idx {} to up failed: {}.",
                veth_idx, e
            ))
        })?;

    Ok(())
}

pub async fn setup_veth_peer(
    veth_idx: u32,
    veth_name: &String,
    ns_ip: &String,
    subnet: u8,
) -> Result<(), NetworkError> {
    // Small initial delay to ensure interface has settled in new namespace
    sleep(Duration::from_millis(50)).await;

    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    info!(
        "Setup veth peer {} with ip: {}/{}.",
        veth_name, ns_ip, subnet
    );

    // Look up interface by NAME in this namespace to get correct index
    // Interface indices may not be reliable across namespace boundaries
    let actual_veth_idx = match handle
        .link()
        .get()
        .match_name(veth_name.clone())
        .execute()
        .try_next()
        .await?
    {
        Some(link) => link.header.index,
        None => {
            return Err(NetworkError::OperationError(format!(
                "Failed to find veth interface {} in namespace",
                veth_name
            )));
        }
    };

    info!(
        "Found veth {} with index {} in namespace",
        veth_name, actual_veth_idx
    );

    // sets veth peer address with limited retries
    let veth_2_addr = std::net::IpAddr::V4(Ipv4Addr::from_str(ns_ip)?);
    let max_retries = 20;
    let mut retry_count = 0;

    loop {
        match AddressHandle::new(handle.clone())
            .add(actual_veth_idx, veth_2_addr, subnet)
            .execute()
            .await
        {
            Ok(_) => {
                info!("Successfully added IP address to {}", veth_name);
                break;
            }
            Err(e) => {
                retry_count += 1;
                if retry_count >= max_retries {
                    return Err(NetworkError::OperationError(format!(
                        "Failed to add IP address to {} after {} retries: {}",
                        veth_name, max_retries, e
                    )));
                }
                error!(
                    "Failed to add IP to {} (attempt {}/{}), retrying in 200ms: {}",
                    veth_name, retry_count, max_retries, e
                );
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    // Bring up the veth peer interface
    handle
        .link()
        .set(LinkUnspec::new_with_index(actual_veth_idx).up().build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set veth {} with idx {} to up failed: {}.",
                veth_name, actual_veth_idx, e
            ))
        })?;

    info!("Successfully brought up veth peer {}", veth_name);

    // Small delay to ensure the link state propagates properly
    sleep(Duration::from_millis(50)).await;

    // sets lo interface to up
    let lo_idx = handle
        .link()
        .get()
        .match_name("lo".to_string())
        .execute()
        .try_next()
        .await?
        .ok_or_else(|| NetworkError::OperationError("Failed to get lo index.".to_string()))?
        .header
        .index;

    handle
        .link()
        .set(LinkUnspec::new_with_index(lo_idx).up().build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set lo interface with idx {} to up failed: {}.",
                lo_idx, e
            ))
        })?;

    Ok(())
}

pub async fn delete_namespace(bridge_idx: u32) -> Result<(), NetworkError> {
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    handle.link().del(bridge_idx).execute().await.map_err(|e| {
        NetworkError::OperationError(format!(
            "Delet bridge with idx {} failed: {}.",
            bridge_idx, e
        ))
    })?;

    Ok(())
}
