use aya::{maps::SockHash, programs::{SkMsg, SockOps, links::CgroupAttachMode}};
use aya_common::SockKey;
#[rustfmt::skip]
use log::{debug, warn, info};
use std::fs::File;
use tokio::signal;

#[tokio::main]
async fn main() -> anyhow::Result<()> {
    env_logger::init();

    // Bump the memlock rlimit. This is needed for older kernels that don't use the
    // new memcg based accounting, see https://lwn.net/Articles/837122/
    let rlim = libc::rlimit {
        rlim_cur: libc::RLIM_INFINITY,
        rlim_max: libc::RLIM_INFINITY,
    };
    let ret = unsafe { libc::setrlimit(libc::RLIMIT_MEMLOCK, &rlim) };
    if ret != 0 {
        debug!("remove limit on locked memory failed, ret is: {ret}");
    }

    // Load eBPF program
    let mut ebpf = aya::Ebpf::load(aya::include_bytes_aligned!(concat!(
        env!("OUT_DIR"),
        "/aya"
    )))?;
    if let Err(e) = aya_log::EbpfLogger::init(&mut ebpf) {
        warn!("failed to initialize eBPF logger: {e}");
    }

    // Get the socket hash map
    let sock_map: SockHash<_, SockKey> = ebpf.map("REDIRECT_MAP").unwrap().try_into()?;
    let map_fd = sock_map.fd().try_clone()?;

    // Load and attach sk_msg program
    let sk_msg_prog: &mut SkMsg = ebpf.program_mut("aya").unwrap().try_into()?;
    sk_msg_prog.load()?;
    sk_msg_prog.attach(&map_fd)?;
    info!("SkMsg program loaded and attached to socket map");

    // Load and attach sock_ops program
    let sock_ops_prog: &mut SockOps = ebpf.program_mut("sock_ops_prog").unwrap().try_into()?;
    sock_ops_prog.load()?;
    
    // Attach to cgroup (try multiple common paths)
    let cgroup_paths = ["/sys/fs/cgroup", "/sys/fs/cgroup/unified"];
    let mut attached = false;
    
    for cgroup_path in &cgroup_paths {
        if let Ok(cgroup_file) = File::open(cgroup_path) {
            if let Ok(_link_id) = sock_ops_prog.attach(cgroup_file, CgroupAttachMode::Single) {
                info!("SockOps program attached to cgroup: {}", cgroup_path);
                attached = true;
                break;
            }
        }
    }
    
    if !attached {
        warn!("Failed to attach SockOps program to any cgroup");
        warn!("Socket redirection may not work properly");
    }

    info!("eBPF socket redirect proxy is running...");
    info!("Socket connections will be captured and added to the redirect map");
    info!("Use 'watch -n 1 \"cat /proc/net/dev\"' to monitor network traffic");

    let ctrl_c = signal::ctrl_c();
    println!("Waiting for Ctrl-C...");
    ctrl_c.await?;
    println!("Exiting...");

    Ok(())
}
