use std::io::Read;
use std::net::{TcpListener, TcpStream};
use std::sync::atomic::{AtomicUsize, Ordering};
use std::sync::Arc;
use std::thread;
use std::time::{Duration, Instant};

fn main() -> std::io::Result<()> {
    // listens on all interfaces on port 8080
    let listener = TcpListener::bind("0.0.0.0:8080")?;
    println!("Server listening on port 8080");

    // accepts connections and process them serially
    for stream in listener.incoming() {
        match stream {
            Ok(stream) => {
                println!("New connection from: {}", stream.peer_addr()?);
                // handles each connection in a new thread
                thread::spawn(move || {
                    if let Err(e) = handle_client(stream) {
                        eprintln!("Error handling client: {}", e);
                    }
                });
            }
            Err(e) => {
                eprintln!("Connection failed: {}", e);
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
    let start_time = Instant::now();

    // shared counters for stats
    let total_bytes = Arc::new(AtomicUsize::new(0));
    let total_packets = Arc::new(AtomicUsize::new(0));

    // clones the counters for the stats thread
    let stats_bytes = Arc::clone(&total_bytes);
    let stats_packets = Arc::clone(&total_packets);

    // spawns a thread to print stats every second
    let stats_thread = thread::spawn(move || {
        let mut last_bytes = 0;
        let mut last_time = Instant::now();

        loop {
            thread::sleep(Duration::from_secs(1));

            let current_bytes = stats_bytes.load(Ordering::Relaxed);
            let current_packets = stats_packets.load(Ordering::Relaxed);
            let current_time = Instant::now();

            let bytes_delta = current_bytes - last_bytes;
            let time_delta = current_time.duration_since(last_time).as_secs_f64();

            // calculate throughput in MB/s
            let throughput = if time_delta > 0.0 {
                (bytes_delta as f64) / (1024.0 * 1024.0) / time_delta
            } else {
                0.0
            };

            println!(
                "Throughput: {:.2} MB/s, Total received: {:.2} MB ({} packets)",
                throughput,
                (current_bytes as f64) / (1024.0 * 1024.0),
                current_packets
            );

            last_bytes = current_bytes;
            last_time = current_time;

            // exits if no new data for 5 seconds
            if bytes_delta == 0 && current_time.duration_since(start_time).as_secs() > 5 {
                println!("No data received for 5 seconds, closing stats thread");
                break;
            }
        }
    });

    // main loop to receive data packets
    loop {
        match stream.read(&mut buffer) {
            Ok(n) if n > 0 => {
                // updates counters
                total_bytes.fetch_add(n, Ordering::Relaxed);
                total_packets.fetch_add(1, Ordering::Relaxed);

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
                eprintln!("Error reading from client: {}", e);
                break;
            }
        }
    }

    // waits for stats thread to finish
    if let Err(e) = stats_thread.join() {
        eprintln!("Stats thread panicked: {:?}", e);
    }

    let elapsed = start_time.elapsed();
    let total = total_bytes.load(Ordering::Relaxed);
    let packets = total_packets.load(Ordering::Relaxed);

    // prints final statistics
    println!("Session summary:");
    println!(
        "Total received: {:.2} MB in {} packets",
        (total as f64) / (1024.0 * 1024.0),
        packets
    );
    println!(
        "Average throughput: {:.2} MB/s",
        (total as f64) / (1024.0 * 1024.0) / elapsed.as_secs_f64()
    );
    println!("Time elapsed: {:.2} seconds", elapsed.as_secs_f64());

    Ok(())
}
