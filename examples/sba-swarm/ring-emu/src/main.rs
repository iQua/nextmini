use std::net::SocketAddr;

use anyhow::Result;
use bytes::{BufMut, BytesMut};
use clap::{ArgAction, Parser};
use tokio::io::{AsyncReadExt, AsyncWriteExt};
use tokio::net::{TcpListener, TcpStream};
use tokio::time::{sleep, Duration};
use tracing::{error, info, warn};

#[derive(Parser, Debug, Clone)]
#[command(name = "ring-emu", version, about = "Simple TCP ring all-reduce emulator (no MPI/NCCL)")]
struct Args {
    /// This node's index in the ring (0-based)
    #[arg(long)]
    rank: usize,

    /// Total number of nodes in the ring
    #[arg(long)]
    world_size: usize,

    /// This node's listening address (host:port)
    #[arg(long)]
    listen: SocketAddr,

    /// Next neighbor's address in the ring (host:port)
    #[arg(long)]
    next: SocketAddr,

    /// Number of f32 elements in the tensor
    #[arg(long, default_value_t = 1024)]
    numel: usize,

    /// Divide tensor into this many chunks
    #[arg(long, default_value_t = 8)]
    chunks: usize,

    /// Number of full ring all-reduce iterations to run
    #[arg(long, default_value_t = 1)]
    iters: usize,

    /// Optional startup delay (ms) to stagger connections
    #[arg(long, default_value_t = 0)]
    startup_delay_ms: u64,

    /// Print tensor summary each iteration
    #[arg(long, action = ArgAction::SetTrue, default_value_t = false)]
    verbose: bool,
}

#[tokio::main]
async fn main() -> Result<()> {
    tracing_subscriber::fmt::init();
    let args = Args::parse();

    info!(?args, "Starting ring-emu node");

    let tensor = vec![args.rank as f32; args.numel];
    let mut state = EmuState::new(args, tensor);
    state.run().await?;
    Ok(())
}

struct EmuState {
    args: Args,
    tensor: Vec<f32>,
}

impl EmuState {
    fn new(args: Args, tensor: Vec<f32>) -> Self {
        Self { args, tensor }
    }

    async fn run(&mut self) -> Result<()> {
        if self.args.startup_delay_ms > 0 {
            sleep(Duration::from_millis(self.args.startup_delay_ms)).await;
        }

        if self.args.world_size < 2 {
            warn!(world_size=%self.args.world_size, "world_size must be >= 2; nothing to do");
            return Ok(());
        }

        // Listener for prev neighbor to connect to us
        let listener = TcpListener::bind(self.args.listen).await?;
        info!(addr=?self.args.listen, "Listening for prev neighbor");

        // Attempt connection to next neighbor (retry until available)
        let mut next_stream = loop {
            match TcpStream::connect(self.args.next).await {
                Ok(s) => break s,
                Err(e) => {
                    error!(error=?e, next=?self.args.next, "Connect failed; retrying");
                    sleep(Duration::from_millis(500)).await;
                }
            }
        };
        next_stream.set_nodelay(true)?;
        info!(next=?self.args.next, "Connected to next neighbor");

        // Accept connection from previous neighbor
        let (mut prev_stream, prev_addr) = listener.accept().await?;
        prev_stream.set_nodelay(true)?;
        info!(?prev_addr, "Accepted prev neighbor connection");

        // Chunking. For simplicity, we allow arbitrary chunk count, but map indices modulo num_chunks.
        let num_chunks = self.args.chunks.max(1).min(self.args.numel);
        if num_chunks != self.args.world_size {
            warn!(num_chunks=%num_chunks, world_size=%self.args.world_size, "For exact ring semantics, prefer --chunks=world_size; mapping will wrap modulo");
        }
        let chunk_size = (self.args.numel + num_chunks - 1) / num_chunks;

        for iter in 0..self.args.iters {
            // Reduce-scatter phase
            for step in 0..self.args.world_size - 1 {
                let send_idx = (self.args.rank + self.args.world_size - step) % self.args.world_size;
                let recv_idx = (self.args.rank + self.args.world_size - step - 1) % self.args.world_size;

                let send_range = chunk_range(send_idx, num_chunks, chunk_size, self.args.numel);
                let recv_range = chunk_range(recv_idx, num_chunks, chunk_size, self.args.numel);

                let send_bytes = f32_slice_to_bytes(&self.tensor[send_range.clone()]);
                // Alternate order by parity to avoid write-write deadlocks
                if self.args.rank % 2 == 0 {
                    send_frame(&mut next_stream, &send_bytes).await?;
                    let recv_buf = recv_frame(&mut prev_stream).await?;
                    accumulate_into(&mut self.tensor[recv_range.clone()], &recv_buf);
                } else {
                    let recv_buf = recv_frame(&mut prev_stream).await?;
                    accumulate_into(&mut self.tensor[recv_range.clone()], &recv_buf);
                    send_frame(&mut next_stream, &send_bytes).await?;
                }
            }

            // All-gather phase
            for step in 0..self.args.world_size - 1 {
                let send_idx = (self.args.rank + self.args.world_size - step - 1) % self.args.world_size;
                let recv_idx = (self.args.rank + self.args.world_size - step - 2) % self.args.world_size;

                let send_range = chunk_range(send_idx, num_chunks, chunk_size, self.args.numel);
                let recv_range = chunk_range(recv_idx, num_chunks, chunk_size, self.args.numel);

                let send_bytes = f32_slice_to_bytes(&self.tensor[send_range.clone()]);
                if self.args.rank % 2 == 0 {
                    send_frame(&mut next_stream, &send_bytes).await?;
                    let recv_buf = recv_frame(&mut prev_stream).await?;
                    write_into(&mut self.tensor[recv_range.clone()], &recv_buf);
                } else {
                    let recv_buf = recv_frame(&mut prev_stream).await?;
                    write_into(&mut self.tensor[recv_range.clone()], &recv_buf);
                    send_frame(&mut next_stream, &send_bytes).await?;
                }
            }

            if self.args.verbose {
                let (mean, min, max) = summarize(&self.tensor);
                info!(iter, mean, min, max, "Iteration summary");
            }
        }

        Ok(())
    }
}

fn chunk_range(idx: usize, n_chunks: usize, chunk_size: usize, total: usize) -> std::ops::Range<usize> {
    let idx = idx % n_chunks;
    let start = idx.saturating_mul(chunk_size).min(total);
    let end = ((idx + 1) * chunk_size).min(total);
    start..end
}

fn f32_slice_to_bytes(slice: &[f32]) -> Vec<u8> {
    let mut buf = BytesMut::with_capacity(slice.len() * 4);
    for &v in slice {
        buf.put_f32_le(v);
    }
    buf.to_vec()
}

fn accumulate_into(dst: &mut [f32], src_bytes: &[u8]) {
    let mut i = 0usize;
    let mut off = 0usize;
    while i < dst.len() && off + 4 <= src_bytes.len() {
        let v = f32::from_le_bytes(src_bytes[off..off + 4].try_into().unwrap());
        dst[i] += v;
        i += 1;
        off += 4;
    }
}

fn write_into(dst: &mut [f32], src_bytes: &[u8]) {
    let mut i = 0usize;
    let mut off = 0usize;
    while i < dst.len() && off + 4 <= src_bytes.len() {
        let v = f32::from_le_bytes(src_bytes[off..off + 4].try_into().unwrap());
        dst[i] = v;
        i += 1;
        off += 4;
    }
}

fn summarize(v: &[f32]) -> (f32, f32, f32) {
    if v.is_empty() {
        return (0.0, 0.0, 0.0);
    }
    let mut sum = 0.0f32;
    let mut mn = f32::INFINITY;
    let mut mx = f32::NEG_INFINITY;
    for &x in v {
        sum += x;
        if x < mn { mn = x; }
        if x > mx { mx = x; }
    }
    (sum / (v.len() as f32), mn, mx)
}

async fn send_frame(stream: &mut TcpStream, payload: &[u8]) -> Result<()> {
    let mut hdr = [0u8; 8];
    hdr[..8].copy_from_slice(&(payload.len() as u64).to_le_bytes());
    stream.write_all(&hdr).await?;
    stream.write_all(payload).await?;
    Ok(())
}

async fn recv_frame(stream: &mut TcpStream) -> Result<Vec<u8>> {
    let mut hdr = [0u8; 8];
    stream.read_exact(&mut hdr).await?;
    let len = u64::from_le_bytes(hdr) as usize;
    let mut buf = vec![0u8; len];
    stream.read_exact(&mut buf).await?;
    Ok(buf)
}


