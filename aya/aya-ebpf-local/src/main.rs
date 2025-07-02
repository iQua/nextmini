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

// Helper function to format IP address for logging
fn format_ip(ip: u32) -> (u8, u8, u8, u8) {
    (
        (ip & 0xFF) as u8,
        ((ip >> 8) & 0xFF) as u8,
        ((ip >> 16) & 0xFF) as u8,
        ((ip >> 24) & 0xFF) as u8,
    )
}

#[map]
static REDIRECT_MAP: SockHash<SockKey> = SockHash::<SockKey>::with_max_entries(1024, 0);

#[sk_msg]
pub fn aya(ctx: SkMsgContext) -> u32 {
    match try_aya(ctx) {
        Ok(ret) => ret,
        Err(ret) => ret,
    }
}

fn try_aya(ctx: SkMsgContext) -> Result<u32, u32> {
    let key = extract_sock_key(&ctx)?;

    let src_ip = format_ip(key.sip4);
    let dst_ip = format_ip(key.dip4);
    info!(
        &ctx,
        "sk_msg: processing message from {}.{}.{}.{}:{} to {}.{}.{}.{}:{}",
        src_ip.0,
        src_ip.1,
        src_ip.2,
        src_ip.3,
        key.sport,
        dst_ip.0,
        dst_ip.1,
        dst_ip.2,
        dst_ip.3,
        key.dport
    );

    // Try to redirect the message to a socket in the map
    let ret = unsafe {
        bpf_msg_redirect_hash(
            ctx.msg,
            &REDIRECT_MAP as *const _ as *mut _,
            &key as *const _ as *mut _,
            0,
        )
    };

    if ret == 0 {
        info!(&ctx, "sk_msg: message redirected successfully");
        Ok(1) // Message redirected
    } else {
        info!(&ctx, "sk_msg: redirect failed, passing message normally");
        Ok(SK_PASS) // Pass message normally
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

            let key = SockKey {
                sip4: unsafe { (*ctx.ops).local_ip4 },
                dip4: unsafe { (*ctx.ops).remote_ip4 },
                family: family as u8,
                sport: unsafe { (*ctx.ops).local_port } as u32,
                dport: unsafe { (*ctx.ops).remote_port } as u32,
            };

            let src_ip = format_ip(key.sip4);
            let dst_ip = format_ip(key.dip4);
            info!(
                &ctx,
                "sock_ops: new IPv4 connection {}.{}.{}.{}:{} -> {}.{}.{}.{}:{}",
                src_ip.0,
                src_ip.1,
                src_ip.2,
                src_ip.3,
                key.sport,
                dst_ip.0,
                dst_ip.1,
                dst_ip.2,
                dst_ip.3,
                key.dport
            );

            // Add socket to the redirect map
            let mut key_mut = key;
            if let Err(_) = REDIRECT_MAP.update(&mut key_mut, unsafe { &mut *ctx.ops }, 0) {
                info!(&ctx, "sock_ops: failed to add socket to map");
            } else {
                info!(&ctx, "sock_ops: socket added to redirect map");
            }
            Ok(0)
        }
        _ => Ok(0),
    }
}

fn extract_sock_key(ctx: &SkMsgContext) -> Result<SockKey, u32> {
    let key = SockKey {
        sip4: unsafe { (*ctx.msg).local_ip4 },
        dip4: unsafe { (*ctx.msg).remote_ip4 },
        family: 2, // IPv4
        sport: unsafe { (*ctx.msg).local_port } as u32,
        dport: unsafe { (*ctx.msg).remote_port } as u32,
    };
    Ok(key)
}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 13] = *b"Dual MIT/GPL\0";
