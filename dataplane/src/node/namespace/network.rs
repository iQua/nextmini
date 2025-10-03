use std::{fmt, net::Ipv4Addr, str::FromStr, sync::Arc, time::Instant};

use futures::TryStreamExt;
use once_cell::sync::OnceCell;
use rtnetlink::{
    AddressHandle, Handle, LinkBridge, LinkUnspec, LinkVeth, RouteMessageBuilder, new_connection,
};
use std::fs;
use std::io::Write;
use std::process::Command;
use tokio::time::timeout;
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

static GLOBAL_NETLINK: OnceCell<Arc<Handle>> = OnceCell::new();

async fn get_global_handle() -> Result<Arc<Handle>, NetworkError> {
    if let Some(h) = GLOBAL_NETLINK.get() {
        return Ok(h.clone());
    }

    // initializes once (synchronous new_connection call)
    let (connection, handle, _) = new_connection()?;
    tokio::spawn(connection);

    let arc = Arc::new(handle);
    let _ = GLOBAL_NETLINK.set(arc.clone());

    Ok(arc)
}

async fn new_connection_with_timeout(
    attempts: u32,
    timeout_ms: u64,
    backoff_ms: u64,
) -> Result<Handle, NetworkError> {
    for attempt in 1..=attempts {
        let start = Instant::now();

        match timeout(Duration::from_millis(timeout_ms), async {
            new_connection()
        })
        .await
        {
            Ok(Ok((conn, handle, _))) => {
                tokio::spawn(conn);
                let elapsed = start.elapsed().as_millis();
                info!(
                    "Child netlink connection established in {} ms (attempt {} out of {}).",
                    elapsed, attempt, attempts
                );

                return Ok(handle);
            }
            Ok(Err(e)) => {
                error!(
                    "Child netlink new_connection failed (attempt {} out of {}): {}",
                    attempt, attempts, e
                );
            }
            Err(_) => {
                error!(
                    "Child netlink new_connection timed out after {} ms (attempt {} out of {}).",
                    timeout_ms, attempt, attempts
                );
            }
        }

        if attempt < attempts {
            sleep(Duration::from_millis(backoff_ms)).await;
        }
    }

    Err(NetworkError::OperationError(
        "Failed to establish child netlink connection after retries.".to_string(),
    ))
}

pub async fn prepare_net(
    bridge_name: String,
    bridge_ip: &str,
    subnet: u8,
    idx: usize,
) -> Result<(u32, u32, u32), NetworkError> {
    let handle = get_global_handle().await?;

    // creates the bridge if it does not exist
    let bridge_idx = match get_bridge_idx(&handle, bridge_name.clone()).await {
        Ok(idx) => idx,
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
    let handle = get_global_handle().await?;

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
    AddressHandle::new((*handle).clone())
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
    let handle = get_global_handle().await?;

    // creates veth interfaces with unique names based on idx
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
    let handle = get_global_handle().await?;

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
    let handle = get_global_handle().await?;

    // brings up the master veth interface
    // Note: It's safe to bring this up now even if peer isn't ready yet,
    // as the carrier state will update automatically when peer comes up
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

/// Waits for a veth interface to establish carrier (link to peer).
/// Uses both interface flags and operstate to determine carrier. Also falls
/// back to checking /sys/class/net/<ifname>/carrier when available.
pub async fn wait_for_veth_carrier(
    veth_idx: u32,
    max_retries: u32,
    retry_interval_ms: u64,
) -> Result<(), NetworkError> {
    let handle = get_global_handle().await?;

    info!(
        "Waiting for veth interface {} to establish carrier.",
        veth_idx
    );

    for attempt in 1..=max_retries {
        match handle
            .link()
            .get()
            .match_index(veth_idx)
            .execute()
            .try_next()
            .await
        {
            Ok(Some(link)) => {
                // Checks if the link has carrier
                // In rtnetlink, we can check the operstate or flags
                // IFF_LOWER_UP (0x10000) indicates carrier is present
                let has_carrier = (link.header.flags.bits() & 0x10000) != 0;

                if has_carrier {
                    info!(
                        "Veth interface {} established carrier after {} attempts.",
                        veth_idx, attempt
                    );
                    return Ok(());
                }

                // logs progress in the rare cases where more than 10 attempts were tried
                if attempt % 10 == 0 {
                    info!(
                        "Waiting for veth {} carrier (attempt {} out of {}).",
                        veth_idx, attempt, max_retries
                    );
                }
            }
            Ok(None) => {
                return Err(NetworkError::OperationError(format!(
                    "Veth interface {} not found.",
                    veth_idx
                )));
            }
            Err(e) => {
                error!("Error checking veth {} carrier: {}.", veth_idx, e);
            }
        }

        sleep(Duration::from_millis(retry_interval_ms)).await;
    }

    Err(NetworkError::OperationError(format!(
        "Veth interface {} failed to establish carrier after {} attempts ({} seconds).",
        veth_idx,
        max_retries,
        (max_retries as u64 * retry_interval_ms) / 1000
    )))
}

pub async fn setup_veth_peer(
    veth_idx: u32,
    ns_ip: &String,
    subnet: u8,
) -> Result<(), NetworkError> {
    let handle = new_connection_with_timeout(3, 1500, 200).await?;

    // sets veth peer address
    let veth_2_addr = std::net::IpAddr::V4(Ipv4Addr::from_str(ns_ip)?);

    // keeps retrying until successful
    loop {
        match AddressHandle::new(handle.clone())
            .add(veth_idx, veth_2_addr, subnet)
            .execute()
            .await
        {
            Ok(_) => break,
            Err(e) => {
                error!("Retrying in 200 ms...{}.", e);
                sleep(Duration::from_millis(200)).await;
            }
        }
    }

    // brings up the veth peer interface
    handle
        .link()
        .set(LinkUnspec::new_with_index(veth_idx).up().build())
        .execute()
        .await
        .map_err(|e| {
            NetworkError::OperationError(format!(
                "Set veth with idx {} to up failed: {}.",
                veth_idx, e
            ))
        })?;

    // adds a small delay to ensure the link state propagates properly
    sleep(Duration::from_millis(10)).await;

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

/// Add a default route (0.0.0.0/0) via the specified gateway on the given interface index
/// inside the current network namespace. This enables outbound connectivity from the namespace
/// to networks reachable through the host bridge and beyond.
pub async fn add_default_route(veth_idx: u32, gateway_ip: &str) -> Result<(), NetworkError> {
    let handle = new_connection_with_timeout(3, 1500, 200).await?;

    let gateway = Ipv4Addr::from_str(gateway_ip)?;

    // Builds RouteMessage using the builder API from rtnetlink v0.18.
    let msg = RouteMessageBuilder::<Ipv4Addr>::new()
        .destination_prefix(Ipv4Addr::new(0, 0, 0, 0), 0)
        .gateway(gateway)
        .output_interface(veth_idx)
        .build();

    // Uses replace() to avoid errors if a default route already exists.
    match handle.route().add(msg).replace().execute().await {
        Ok(_) => {
            info!(
                "Installed default route via {} on ifindex {} inside namespace.",
                gateway_ip, veth_idx
            );
            Ok(())
        }
        Err(e) => {
            error!(
                "Failed to add default route via {} on ifindex {}: {}.",
                gateway_ip, veth_idx, e
            );
            Err(NetworkError::OperationError(format!(
                "Add default route failed: {}.",
                e
            )))
        }
    }
}

/// Enable IPv4 forwarding on the host if disabled.
pub fn ensure_ip_forward_enabled() -> Result<(), NetworkError> {
    match fs::read_to_string("/proc/sys/net/ipv4/ip_forward") {
        Ok(content) if content.trim() == "1" => Ok(()),
        _ => {
            let mut f = fs::OpenOptions::new()
                .write(true)
                .open("/proc/sys/net/ipv4/ip_forward")
                .map_err(NetworkError::Other)?;
            f.write_all(b"1").map_err(NetworkError::Other)?;
            info!("Enabled net.ipv4.ip_forward=1");
            Ok(())
        }
    }
}

/// Add iptables FORWARD rules to allow traffic between isobr0 and the outbound interface.
pub fn ensure_forward_rules(bridge_name: &str, outbound_if: &str) -> Result<(), NetworkError> {
    // First rule: isobr0 -> outbound: ACCEPT
    let check1 = Command::new("iptables")
        .args([
            "-C",
            "FORWARD",
            "-i",
            bridge_name,
            "-o",
            outbound_if,
            "-j",
            "ACCEPT",
        ])
        .status()
        .map_err(NetworkError::Other)?;
    if !check1.success() {
        let add1 = Command::new("iptables")
            .args([
                "-I",
                "FORWARD",
                "1",
                "-i",
                bridge_name,
                "-o",
                outbound_if,
                "-j",
                "ACCEPT",
            ])
            .status()
            .map_err(NetworkError::Other)?;
        if !add1.success() {
            error!(
                "Failed to apply iptables FORWARD rule: -i {} -o {} -j ACCEPT",
                bridge_name, outbound_if
            );
        }
    }

    // Second rule: outbound -> isobr0 RELATED,ESTABLISHED: ACCEPT
    let check2 = Command::new("iptables")
        .args([
            "-C",
            "FORWARD",
            "-i",
            outbound_if,
            "-o",
            bridge_name,
            "-m",
            "state",
            "--state",
            "RELATED,ESTABLISHED",
            "-j",
            "ACCEPT",
        ])
        .status()
        .map_err(NetworkError::Other)?;
    if !check2.success() {
        let add2 = Command::new("iptables")
            .args([
                "-I",
                "FORWARD",
                "1",
                "-i",
                outbound_if,
                "-o",
                bridge_name,
                "-m",
                "state",
                "--state",
                "RELATED,ESTABLISHED",
                "-j",
                "ACCEPT",
            ])
            .status()
            .map_err(NetworkError::Other)?;
        if !add2.success() {
            error!(
                "Failed to apply iptables FORWARD rule: -i {} -o {} -m state --state RELATED,ESTABLISHED -j ACCEPT",
                outbound_if, bridge_name
            );
        }
    }

    Ok(())
}

/// Add a MASQUERADE rule for the namespace subnet on the given outbound interface.
pub fn ensure_nat_masquerade(subnet_cidr: &str, outbound_if: &str) -> Result<(), NetworkError> {
    let check = Command::new("iptables")
        .args([
            "-t",
            "nat",
            "-C",
            "POSTROUTING",
            "-s",
            subnet_cidr,
            "-o",
            outbound_if,
            "-j",
            "MASQUERADE",
        ])
        .status()
        .map_err(NetworkError::Other)?;
    if !check.success() {
        let add = Command::new("iptables")
            .args([
                "-t",
                "nat",
                "-A",
                "POSTROUTING",
                "-s",
                subnet_cidr,
                "-o",
                outbound_if,
                "-j",
                "MASQUERADE",
            ])
            .status()
            .map_err(NetworkError::Other)?;
        if !add.success() {
            error!(
                "Failed to ensure NAT MASQUERADE on {} via {}",
                subnet_cidr, outbound_if
            );
        }
    }
    Ok(())
}

pub async fn delete_namespace(bridge_idx: u32) -> Result<(), NetworkError> {
    let handle = get_global_handle().await?;

    handle.link().del(bridge_idx).execute().await.map_err(|e| {
        NetworkError::OperationError(format!(
            "Delet bridge with idx {} failed: {}.",
            bridge_idx, e
        ))
    })?;

    Ok(())
}
