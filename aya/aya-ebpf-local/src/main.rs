#![no_std]
#![no_main]

use aya_common::SockKey;
use aya_ebpf::{
    bindings,
    helpers::bpf_msg_redirect_hash,
    macros::{map, sk_msg, sock_ops},
    maps::SockHash,
    programs::{SkMsgContext, SockOpsContext},
};
use aya_log_ebpf::info;

const SK_PASS: u32 = 1;

// Backend server IPs for load balancing
const BACKEND_1_IP: u32 = 0x0B0014AC; // 172.20.0.11 in network byte order
const BACKEND_2_IP: u32 = 0x0C0014AC; // 172.20.0.12 in network byte order
const BACKEND_3_IP: u32 = 0x0D0014AC; // 172.20.0.13 in network byte order
const FRONTEND_IP: u32 = 0x050014AC; // 172.20.0.5 in network byte order
const BACKEND_PORT: u32 = 8080;

// Simple round-robin counter (not thread-safe, but good for demo)
static mut ROUND_ROBIN_COUNTER: u32 = 0;

// Helper function to format IP address for logging
fn format_ip(ip: u32) -> (u8, u8, u8, u8) {
    (
        (ip & 0xFF) as u8,
        ((ip >> 8) & 0xFF) as u8,
        ((ip >> 16) & 0xFF) as u8,
        ((ip >> 24) & 0xFF) as u8,
    )
}

// Helper function to extract port from 32-bit field
fn extract_port(port_field: u32) -> u16 {
    // Try different extraction methods
    if port_field < 65536 {
        // If it's already a small number, use it directly
        port_field as u16
    } else {
        // Try extracting from different positions
        let method1 = (port_field & 0xFFFF) as u16; // Lower 16 bits
        let method2 = ((port_field >> 16) & 0xFFFF) as u16; // Upper 16 bits
        let method3 = u16::from_be((port_field & 0xFFFF) as u16); // Lower 16 bits, big endian
        let method4 = u16::from_be(((port_field >> 16) & 0xFFFF) as u16); // Upper 16 bits, big endian

        // Choose the most reasonable port number (typically 1-65535)
        if method1 > 0 && method1 <= 65535 && method1 != method2 {
            method1
        } else if method2 > 0 && method2 <= 65535 {
            method2
        } else if method3 > 0 && method3 <= 65535 {
            method3
        } else if method4 > 0 && method4 <= 65535 {
            method4
        } else {
            (port_field & 0xFFFF) as u16 // Fallback to lower 16 bits
        }
    }
}

#[map]
static REDIRECT_MAP: SockHash<SockKey> = SockHash::<SockKey>::with_max_entries(1024, 0);

// Helper function to select backend server for load balancing
fn select_backend_ip() -> u32 {
    unsafe {
        ROUND_ROBIN_COUNTER = (ROUND_ROBIN_COUNTER + 1) % 3;
        match ROUND_ROBIN_COUNTER {
            0 => BACKEND_1_IP,
            1 => BACKEND_2_IP,
            2 => BACKEND_3_IP,
            _ => BACKEND_1_IP, // fallback
        }
    }
}

#[sk_msg]
pub fn aya(ctx: SkMsgContext) -> u32 {
    match try_aya(ctx) {
        Ok(ret) => ret,
        Err(ret) => ret,
    }
}

fn try_aya(ctx: SkMsgContext) -> Result<u32, u32> {
    let local_ip4 = unsafe { (*ctx.msg).local_ip4 };
    let remote_ip4 = unsafe { (*ctx.msg).remote_ip4 };
    let local_port = extract_port(unsafe { (*ctx.msg).local_port });
    let remote_port = extract_port(unsafe { (*ctx.msg).remote_port });

    let src_ip = format_ip(local_ip4);
    let dst_ip = format_ip(remote_ip4);

    info!(
        &ctx,
        "sk_msg: processing message from {}.{}.{}.{}:{} to {}.{}.{}.{}:{}",
        src_ip.0,
        src_ip.1,
        src_ip.2,
        src_ip.3,
        local_port,
        dst_ip.0,
        dst_ip.1,
        dst_ip.2,
        dst_ip.3,
        remote_port
    );

    // Check if this is a connection to the frontend load balancer
    if remote_ip4 == FRONTEND_IP && remote_port as u32 == 80 {
        // Select a backend server for load balancing
        let target_backend = select_backend_ip();
        let backend_ip = format_ip(target_backend);

        info!(
            &ctx,
            "sk_msg: load balancing - redirecting to backend {}.{}.{}.{}:{}",
            backend_ip.0,
            backend_ip.1,
            backend_ip.2,
            backend_ip.3,
            BACKEND_PORT
        );

        // Create key for the target backend
        let backend_key = SockKey {
            sip4: local_ip4,
            dip4: target_backend,
            family: 2,
            sport: local_port as u32,
            dport: BACKEND_PORT,
        };

        // Try to redirect to the selected backend
        let ret = unsafe {
            bpf_msg_redirect_hash(
                ctx.msg,
                &REDIRECT_MAP as *const _ as *mut _,
                &backend_key as *const _ as *mut _,
                0,
            )
        };

        info!(
            &ctx,
            "sk_msg: backend redirect attempt return value: {}", ret
        );

        if ret == 0 {
            info!(&ctx, "sk_msg: backend redirect failed, trying original key");
            // Fall back to original key
            let original_key = SockKey {
                sip4: local_ip4,
                dip4: remote_ip4,
                family: 2,
                sport: local_port as u32,
                dport: remote_port as u32,
            };

            let ret2 = unsafe {
                bpf_msg_redirect_hash(
                    ctx.msg,
                    &REDIRECT_MAP as *const _ as *mut _,
                    &original_key as *const _ as *mut _,
                    0,
                )
            };

            if ret2 == 0 {
                info!(&ctx, "sk_msg: all redirects failed, passing normally");
                Ok(SK_PASS)
            } else {
                info!(&ctx, "sk_msg: original redirect succeeded");
                Ok(1)
            }
        } else {
            info!(&ctx, "sk_msg: backend redirect successful");
            Ok(1)
        }
    } else {
        // Normal redirect for non-load-balanced traffic
        let key = SockKey {
            sip4: local_ip4,
            dip4: remote_ip4,
            family: 2,
            sport: local_port as u32,
            dport: remote_port as u32,
        };

        let ret = unsafe {
            bpf_msg_redirect_hash(
                ctx.msg,
                &REDIRECT_MAP as *const _ as *mut _,
                &key as *const _ as *mut _,
                0,
            )
        };

        if ret == 0 {
            info!(&ctx, "sk_msg: standard redirect failed, passing normally");
            Ok(SK_PASS)
        } else {
            info!(&ctx, "sk_msg: standard redirect successful");
            Ok(1)
        }
    }
}

#[sock_ops]
pub fn sock_ops_prog(ctx: SockOpsContext) -> u32 {
    match try_sock_ops(ctx) {
        Ok(ret) => ret,
        Err(ret) => ret,
    }
}

fn try_sock_ops(ctx: SockOpsContext) -> Result<u32, u32> {
    let op = unsafe { (*ctx.ops).op };
    let family = unsafe { (*ctx.ops).family };

    match op {
        bindings::BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB
        | bindings::BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB => {
            // Only handle IPv4 connections
            if family != 2 {
                return Ok(0);
            }

            let local_ip4 = unsafe { (*ctx.ops).local_ip4 };
            let remote_ip4 = unsafe { (*ctx.ops).remote_ip4 };
            let raw_local_port = unsafe { (*ctx.ops).local_port };
            let raw_remote_port = unsafe { (*ctx.ops).remote_port };

            let extracted_local = extract_port(raw_local_port);
            let extracted_remote = extract_port(raw_remote_port);

            let src_ip = format_ip(local_ip4);
            let dst_ip = format_ip(remote_ip4);

            info!(
                &ctx,
                "sock_ops: new IPv4 connection {}.{}.{}.{}:{} -> {}.{}.{}.{}:{}",
                src_ip.0,
                src_ip.1,
                src_ip.2,
                src_ip.3,
                extracted_local,
                dst_ip.0,
                dst_ip.1,
                dst_ip.2,
                dst_ip.3,
                extracted_remote
            );

            // Always add the actual connection to the map
            let connection_key = SockKey {
                sip4: local_ip4,
                dip4: remote_ip4,
                family: family as u8,
                sport: extracted_local as u32,
                dport: extracted_remote as u32,
            };

            let mut key_mut = connection_key;
            match REDIRECT_MAP.update(&mut key_mut, unsafe { &mut *ctx.ops }, 0) {
                Ok(_) => {
                    info!(
                        &ctx,
                        "sock_ops: connection socket added to map - sport={} dport={}",
                        connection_key.sport,
                        connection_key.dport
                    );
                }
                Err(err) => {
                    info!(
                        &ctx,
                        "sock_ops: failed to add connection socket - error: {}", err
                    );
                }
            }

            // If this is a connection to a backend server, also create load balancer mapping
            if (remote_ip4 == BACKEND_1_IP
                || remote_ip4 == BACKEND_2_IP
                || remote_ip4 == BACKEND_3_IP)
                && extracted_remote == BACKEND_PORT as u16
            {
                info!(
                    &ctx,
                    "sock_ops: detected backend connection, creating load balancer mapping"
                );

                // Create a mapping from frontend to this backend for load balancing
                let lb_key = SockKey {
                    sip4: local_ip4,
                    dip4: FRONTEND_IP,
                    family: family as u8,
                    sport: extracted_local as u32,
                    dport: 80, // Frontend port
                };

                let mut lb_key_mut = lb_key;
                match REDIRECT_MAP.update(&mut lb_key_mut, unsafe { &mut *ctx.ops }, 0) {
                    Ok(_) => {
                        info!(
                            &ctx,
                            "sock_ops: load balancer mapping added - frontend->backend"
                        );
                    }
                    Err(err) => {
                        info!(
                            &ctx,
                            "sock_ops: failed to add load balancer mapping - error: {}", err
                        );
                    }
                }
            }

            Ok(0)
        }
        _ => Ok(0),
    }
}

// Helper function to convert IP address to readable format
fn ip_to_string(ip: u32) -> u32 {
    // For logging purposes, just return the IP as-is
    // The format_ip function handles the display
    ip
}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 13] = *b"Dual MIT/GPL\0";
