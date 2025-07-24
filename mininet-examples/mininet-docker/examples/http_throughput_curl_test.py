#!/usr/bin/env python
"""
HTTP throughput test with Mininet using curl
Creates linear topologies with 2, 6, and 10 hops (switches) between server
and client hosts and measures throughput using curl.

Topology per hop count (N hops):
    h1 -> s1 -> s2 -> ... -> sN -> h2

For each topology:
    * h1 runs a simple HTTP server on port 80 serving a small test file
    * h2 continuously downloads the file for `duration` seconds using curl
      and calculates throughput statistics (bytes/sec, KB/s, MB/s, Gbps).

Run with sudo:
    sudo python3 http_throughput_curl_test.py
"""
from mininet.net import Mininet
from mininet.node import Controller, OVSSwitch, Host
from mininet.log import setLogLevel, info
import time

# ---------------------------- Topology helpers -----------------------------

def create_linear_topology(num_switches: int) -> Mininet:
    """Create a linear topology with `num_switches` switches.

    Layout: h1 -- s1 -- s2 -- ... -- sN -- h2
    """
    net = Mininet(controller=Controller, switch=OVSSwitch, host=Host)

    info(f"*** Adding controller\n")
    net.addController('c0')

    info(f"*** Adding hosts\n")
    h1 = net.addHost('h1', ip='10.0.0.1/24')  # Server
    h2 = net.addHost('h2', ip='10.0.0.5/24')  # Client

    info(f"*** Adding {num_switches} switches\n")
    switches = []
    for i in range(1, num_switches + 1):
        switches.append(net.addSwitch(f's{i}'))

    info("*** Creating links\n")
    # h1 <-> s1
    net.addLink(h1, switches[0])
    # chain switches
    for i in range(num_switches - 1):
        net.addLink(switches[i], switches[i + 1])
    # last switch <-> h2
    net.addLink(switches[-1], h2)

    return net

# ---------------------------- Utility functions ----------------------------

def run_http_server(host):
    """Start a simple HTTP server on *host* (h1)."""
    info('*** Starting HTTP server on h1\n')
    # Create a very large file (100 GB) to ensure curl never finishes and runs at full speed
    # Use truncate for efficiency, as it creates a sparse file instantly.
    file_size_gb = 50
    info(f'*** Creating a {file_size_gb} GB sparse file for unlimited throughput test...\n')
    host.cmd(f'truncate -s {file_size_gb}G /tmp/large_test.dat')

    # Start HTTP server in background
    host.cmd('cd /tmp && python3 -m http.server 80 --bind 0.0.0.0 >/tmp/server.log 2>&1 &')
    # Give the server a moment to start
    time.sleep(2)
    info('*** HTTP server started on h1:80\n')


def run_throughput_test(client_host, server_ip: str, test_duration: int = 30):
    """
    Run a throughput test by downloading for a specified duration, then stopping.
    This allows curl to run at maximum speed without artificial --max-time constraints.
    """
    # ------------------------------------------------------------
    # Let curl download the *entire* 50G file and exit
    # normally so that it prints built-in performance stats.
    # ------------------------------------------------------------

    info('*** Starting full-file throughput test (no forced kill; curl will exit when the 50-GB file is fully received)...\n')

    # Curl will download the whole file, discard the payload, and
    # print both the average download speed and total duration.
    cmd = (
        f'curl -s -S -o /dev/null '
        f'-w "speed_bytes=%{{speed_download}},time_total=%{{time_total}}" '
        f'http://{server_ip}/large_test.dat'
    )

    # Run curl synchronously and capture its stdout.
    output = client_host.cmd(cmd).strip()

    # Example output: "speed_bytes=123456.78,time_total=42.13"

    if 'speed_bytes=' in output:
        try:
            # Split key=value pairs by comma then '='
            metrics = dict(pair.split('=', 1) for pair in output.split(','))

            speed_bps = float(metrics.get('speed_bytes', 0))
            duration_s = float(metrics.get('time_total', 0))

            speed_kbps = speed_bps / 1024
            speed_mbps = speed_bps / 1048576
            speed_gbps = (speed_bps * 8) / 1e9  # 10^9 bits per second

            info('=== Throughput Test Results (curl full-file download) ===\n')
            info(f'Total Time  : {duration_s:.2f} s\n')
            info(f'Average Speed: {speed_bps:,.2f} bytes/sec\n')
            info(f'Average Speed: {speed_kbps:,.2f} KB/s\n')
            info(f'Average Speed: {speed_mbps:,.2f} MB/s\n')
            info(f'Average Speed: {speed_gbps:,.3f} Gbps\n')

        except (ValueError, IndexError) as e:
            info(f'*** Could not parse curl output. Error: {e}. Output: "{output}"\n')
    else:
        info(f'*** Throughput test failed. Curl output: "{output}"\n')


# ---------------------------- Main routine ---------------------------------

def main():
    setLogLevel('info')

    hop_counts = [4, 6, 10, 15, 20, 25, 30, 40]
    # Use a longer duration for a more stable measurement
    duration = 30  # seconds per test

    for hops in hop_counts:
        info('\n' + '=' * 70 + '\n')
        info(f'*** Running test with {hops} hops ({hops} switches)\n')
        info('=' * 70 + '\n')
        net = create_linear_topology(hops)

        try:
            info('*** Starting network\n')
            net.start()

            # Allow network to stabilise
            time.sleep(3)

            # Test connectivity
            loss = net.pingAll()
            if loss > 0:
                info(f'*** Warning: {loss}% packet loss\n')

            h1 = net.get('h1')
            h2 = net.get('h2')

            # Start HTTP server
            run_http_server(h1)

            # Quick functional check with curl
            http_code = h2.cmd('curl -s -o /dev/null -w "%{http_code}" http://10.0.0.1/large_test.dat').strip()
            if http_code == '200':
                info(f'*** Basic curl check passed (HTTP {http_code}).\n')
            else:
                info(f'*** Basic curl check FAILED (HTTP {http_code}).\n')

            # Throughput test
            run_throughput_test(h2, '10.0.0.1', duration)

        except KeyboardInterrupt:
            info('*** Interrupted by user\n')
        finally:
            info('*** Stopping network\n')
            net.stop()
            # Give Mininet some time to clean up between runs
            time.sleep(2)

    info('\n*** All tests completed.\n')


if __name__ == '__main__':
    main()
