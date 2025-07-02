#![no_std]
#![no_main]

use aya_ebpf::{
    macros::{map, sk_msg},
    maps::SockHash,
    programs::SkMsgContext,
};
use aya_log_ebpf::info;
use aya_common::SockKey;

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
    info!(&ctx, "received a message on the socket");
    Ok(0)
}

#[cfg(not(test))]
#[panic_handler]
fn panic(_info: &core::panic::PanicInfo) -> ! {
    loop {}
}

#[link_section = "license"]
#[no_mangle]
static LICENSE: [u8; 13] = *b"Dual MIT/GPL\0";
