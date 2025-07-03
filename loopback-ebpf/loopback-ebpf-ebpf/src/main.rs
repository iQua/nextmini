#![no_std]
#![no_main]

use aya_ebpf::{
    macros::{map, sk_msg, sock_ops},
    maps::SockHash,
    programs::{SkMsgContext, SockOpsContext},
};

use aya_ebpf::bindings::{
    BPF_F_INGRESS,
    sk_action,
    BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB,
    BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB,
    BPF_NOEXIST,
};

use aya_log_ebpf::info;
use loopback_ebpf_common::SockKey;

pub const CAPACITY: u32 = 1024;

#[map]
static SOCKHASH: SockHash<SockKey> = SockHash::<SockKey>::with_max_entries(CAPACITY, 0);

#[sk_msg]
// How this enables kernel to redirect?
pub fn bpf_redir(ctx: SkMsgContext) -> u32 {
    // Reverse the 4 tuple to check for dst socket
    let mut key = unsafe {
        SockKey {
            remote_ip4: (*ctx.msg).local_ip4,
            local_ip4: (*ctx.msg).remote_ip4,
            remote_port: (*ctx.msg).local_port,
            local_port: (*ctx.msg).remote_port,
        }
    };

    let ret = SOCKHASH.redirect_msg(&ctx, &mut key, BPF_F_INGRESS as u64);

    if ret == 1 {
        info!(&ctx, "redirect_msg succeed");
    } else {
        info!(&ctx, "redirect_msg failed");
    }

    // Pass anyway
    return sk_action::SK_PASS;
}

#[sock_ops]
pub fn bpf_sockmap(ctx: SockOpsContext) -> u32 {
    if ctx.family() != 2 {
        return 0;
    }
    match ctx.op() {
        BPF_SOCK_OPS_PASSIVE_ESTABLISHED_CB => {
            info!(
                &ctx,
                "passive established sport {} dport {}",
                ctx.local_port(),
                unsafe { u32::from_be((*ctx.ops).remote_port) }
            );
            bpf_sock_ops_ipv4(ctx);
        }
        BPF_SOCK_OPS_ACTIVE_ESTABLISHED_CB => {
            info!(
                &ctx,
                "active established sport {} dport {}",
                ctx.local_port(),
                unsafe { u32::from_be((*ctx.ops).remote_port) }
            );
            bpf_sock_ops_ipv4(ctx);
        }
        _ => {}
    }
    0
}

/// Insert the key and socket into the sock hash map
fn bpf_sock_ops_ipv4(ctx: SockOpsContext) {
    let mut key = SockKey {
        local_ip4: ctx.local_ip4(),
        remote_ip4: ctx.remote_ip4(),
        local_port: unsafe { u32::from_be((*ctx.ops).local_port) },
        remote_port: unsafe { u32::from_be((*ctx.ops).remote_port) },
    };

    let ops = unsafe { ctx.ops.as_mut().unwrap() };
    let ret = SOCKHASH.update(&mut key, ops, BPF_NOEXIST.into());

    match ret {
        Ok(_) => info!(&ctx, "SockHash update succeeded"),
        Err(_) => info!(&ctx, "SockHash update failed"),
    }
}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}