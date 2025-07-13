use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicU64, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() -> std::io::Result<()> {
    // listens on all interfaces on port 8080
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    println!("Server listening on port 8080.");

    // accepts connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection from: {}.", stream.peer_addr()?);
                // handles each connection in a new thread
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("Error handling client: {}.", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}.", e);
            }
        }
    }

    Ok(())
}

fn handle_client(mut stream: TcpStream) -> std::io::Result<()> {
    // sets read timeout to prevent blocking indefinitely
    stream.set_read_timeout(Some(Duration::from_secs(1)))?;
    let mut buffer = [0u8; 655350];
    let mut first_packet_received = false;

    let bytes_received = Arc::new(AtomicU64::new(0));
    // spawns a thread to print the stats
    let stats_counter = Arc::clone(&bytes_received);
    thread::spawn(move || {
        print_stats(stats_counter);
    });

    // main loop to receive data packets
    loop {
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                // updates counters
                bytes_received.fetch_add(n as u64, Ordering::Relaxed);

                if !first_packet_received {
                    println!("First packet received! Starting throughput measurement...");
                    first_packet_received = true;
                }
            }
            Ok(0) => {
                println!("Connection closed by client");
                break;
            }
            Ok(_) => {}
            Err(e) => {
                if e.kind() == std::io::ErrorKind::WouldBlock
                    || e.kind() == std::io::ErrorKind::TimedOut
                {
                    // timeout occurred, checks if we should continue
                    continue;
                }
                eprintln!("Error reading from client: {}.", e);
                break;
            }
        }
    }

    Ok(())
}
fn print_stats(bytes_received: Arc<AtomicU64>) {
    let mut last_bytes = 0u64;
    let mut last_time = Instant::now();

    loop {
        // log every second
        thread::sleep(Duration::from_secs(1));

        let current_bytes = bytes_received.load(Ordering::Relaxed);
        let current_time = Instant::now();

        let elapsed = current_time.duration_since(last_time).as_secs_f64();
        let bytes_diff = current_bytes - last_bytes;
        let throughput_gbps = (bytes_diff as f64 * 8.0) / (elapsed * 1000.0 * 1000.0 * 1000.0);

        println!(
            "Throughput: {:.2} Gbps, Total received: {:.2} GB.",
            throughput_gbps,
            current_bytes as f64 / (1000.0 * 1000.0 * 1000.0)
        );

        last_bytes = current_bytes;
        last_time = current_time;
    }
}
