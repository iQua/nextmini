use aya::{maps::SockHash, programs::{SkMsg, SockOps, CgroupAttachMode}};
use loopback_ebpf_common::SockKey;
#[rustfmt::skip]
use log::{debug, warn};
use tokio::net::{TcpListener, TcpStream};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
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

    // This will include your eBPF object file as raw bytes at compile-time and load it at
    // runtime. This approach is recommended for most real-world use cases. If you would
    // like to specify the eBPF program at runtime rather than at compile-time, you can
    // reach for `Bpf::load_file` instead.
    let mut ebpf = aya::Ebpf::load(aya::include_bytes_aligned!(concat!(
        env!("OUT_DIR"),
        "/loopback-ebpf"
    )))?;
    if let Err(e) = aya_log::EbpfLogger::init(&mut ebpf) {
        // This can happen if you remove all log statements from your eBPF program.
        warn!("failed to initialize eBPF logger: {e}");
    }

    // Attach sockops to cgroup
    let program: &mut SockOps = ebpf.program_mut("bpf_sockmap").unwrap().try_into()?;
    program.load()?;
    let cgroup = std::fs::File::open("/sys/fs/cgroup")?;
    program.attach(cgroup, CgroupAttachMode::Single)?;

    // Attach sk_msg to sock_hash
    let sock_hash: SockHash<_, SockKey> = SockHash::try_from(
        ebpf.map_mut("SOCKHASH")
            .ok_or_else(|| anyhow::anyhow!("SOCKHASH map not found"))?
    )?;
    let map_fd = sock_hash.fd().try_clone()?;
    let program: &mut SkMsg = ebpf.program_mut("bpf_redir").unwrap().try_into()?;
    program.load()?;
    program.attach(&map_fd)?;

    // TCP echo
    tokio::spawn(echo_server());
    tokio::time::sleep(tokio::time::Duration::from_millis(100)).await;
    echo_client().await?;

    // Exit
    let ctrl_c = signal::ctrl_c();
    println!("Waiting for Ctrl-C...");
    ctrl_c.await?;
    println!("Exiting...");

    Ok(())
}

async fn echo_server() {
    let listener = TcpListener::bind("127.0.0.1:8080").await.unwrap();
    let (mut socket, _) = listener.accept().await.unwrap();
    let mut buf = [0; 1024];
    let n = socket.read(&mut buf).await.unwrap();
    println!(
        "Server received: {}",
        std::str::from_utf8(&buf[..n]).unwrap()
    );
    socket.write_all(b"Hello Wesley!").await.unwrap();
}

async fn echo_client() -> anyhow::Result<()> {
    let mut stream = TcpStream::connect("127.0.0.1:8080").await?;
    stream.write_all(b"Hey Lucy!").await?;
    let mut buf = [0; 1024];
    let n = stream.read(&mut buf).await?;
    println!("Client received: {}", std::str::from_utf8(&buf[..n])?);
    Ok(())
}
