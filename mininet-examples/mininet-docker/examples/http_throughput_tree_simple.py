#!/usr/bin/env python
"""
HTTP throughput test with Mininet using TreeNet (simplified version)
Uses Mininet's built-in TreeNet but only tests between two hosts.

Topology variations tested:
- Tree depth=1, fanout=2: h1--s1--h2 (2 hosts, 1 switch)
- Tree depth=2, fanout=2: h1--s1--(s2,s3)--h2 (2 hosts, 3 switches)
- Tree depth=3, fanout=2: more complex tree structure

For each topology:
    * One host runs HTTP server, another acts as client
    * Measures throughput using curl similar to linear topology test

Run with sudo:
    sudo python3 http_throughput_tree_simple.py
"""
from mininet.net import Mininet
from mininet.node import Controller, OVSSwitch, Host
from mininet.topolib import TreeNet
from mininet.log import setLogLevel, info
import time

# ---------------------------- Utility functions (reused from linear test) ----------------------------

def run_http_server(host):
    """Start a simple HTTP server on *host*."""
    info('*** Starting HTTP server\n')
    # Create a large file for testing
    file_size_gb = 15
    info(f'*** Creating a {file_size_gb} GB sparse file for unlimited throughput test...\n')
    host.cmd(f'truncate -s {file_size_gb}G /tmp/large_test.dat')

    # Start HTTP server in background
    host.cmd('cd /tmp && python3 -m http.server 80 --bind 0.0.0.0 >/tmp/server.log 2>&1 &')
    time.sleep(2)
    info('*** HTTP server started on port 80\n')

def run_throughput_test(client_host, server_ip: str, test_duration: int = 30):
    """
    Run a throughput test by downloading for a specified duration, then stopping.
    """
    info(f'*** Starting unlimited throughput test (will run for ~{test_duration} seconds)...\n')

    # Run curl in background and capture its PID
    cmd = (
        f'curl -s -S -o /dev/null '
        f'-w "speed_bytes=%{{speed_download}}" '
        f'http://{server_ip}/large_test.dat'
    )

    # Start curl in background
    client_host.cmd(f'{cmd} > /tmp/curl_result.txt 2>&1 &')
    curl_pid = client_host.cmd('echo $!').strip()

    info(f'*** curl started with PID {curl_pid}, letting it run for {test_duration} seconds...\n')

    # Let it run for the specified duration
    time.sleep(test_duration)

    # Stop curl gracefully
    client_host.cmd(f'kill -TERM {curl_pid}')
    time.sleep(1)  # Give it time to finish

    # Read the result
    output = client_host.cmd('cat /tmp/curl_result.txt').strip()

    # Parse results
    if 'speed_bytes=' in output:
        try:
            speed_bps_str = output.split('=')[1]
            speed_bps = float(speed_bps_str)

            speed_kbps = speed_bps / 1024
            speed_mbps = speed_bps / 1048576
            speed_gbps = (speed_bps * 8) / 10**9 # Use standard Gbps (10^9)

            info('=== Throughput Test Results (curl built-in, unlimited) ===\n')
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

    # Test different tree configurations
    tree_configs = [
        (1, 2),  # depth=1, fanout=2 -> 2 hosts, 1 switch
        (2, 2),  # depth=2, fanout=2 -> 4 hosts, 3 switches
        (3, 2),  # depth=3, fanout=2 -> 8 hosts, 7 switches
    ]

    duration = 30  # seconds per test

    for depth, fanout in tree_configs:
        total_hosts = fanout ** depth
        info('\n' + '=' * 70 + '\n')
        info(f'*** Running test with TreeNet (depth={depth}, fanout={fanout})\n')
        info(f'*** Total hosts in tree: {total_hosts}, using h1 as server, h{total_hosts} as client\n')
        info('=' * 70 + '\n')

        # Create TreeNet (it's a complete network, not just a topology)
        net = TreeNet(depth=depth, fanout=fanout, switch=OVSSwitch, host=Host)

        try:
            info('*** Starting network\n')
            net.start()

            # Allow network to stabilize
            time.sleep(3)

            # Test connectivity
            info('*** Testing connectivity...\n')
            loss = net.pingAll()
            if loss > 0:
                info(f'*** Warning: {loss}% packet loss\n')
                # Try to debug connectivity issues
                info('*** Checking controller status...\n')
                controller = net.controllers[0]
                info(f'*** Controller status: {controller}\n')
                
                # Check switch connections
                for switch in net.switches:
                    info(f'*** Switch {switch.name}: {switch.connected()}\n')
                
                # Retry ping between h1 and last host specifically
                h1_debug = net.get('h1')
                h_last_debug = net.get(f'h{total_hosts}')
                info(f'*** Direct ping test h1->{last_host_name}:\n')
                result = h1_debug.cmd(f'ping -c 3 {h_last_debug.IP()}')
                info(f'{result}\n')
            else:
                info('*** All hosts can ping each other successfully\n')

            # Get first and last hosts (h1 as server, last host as client)
            h1 = net.get('h1')  # Server
            last_host_name = f'h{total_hosts}'
            h_last = net.get(last_host_name)  # Client (furthest from h1)

            # Display their IPs
            h1_ip = h1.IP()
            h_last_ip = h_last.IP()
            info(f'*** Server h1 IP: {h1_ip}\n')
            info(f'*** Client {last_host_name} IP: {h_last_ip}\n')

            # Start HTTP server on h1
            run_http_server(h1)

            # Quick functional check with curl
            http_code = h_last.cmd(f'curl -s -o /dev/null -w "%{{http_code}}" http://{h1_ip}/large_test.dat').strip()
            if http_code == '200':
                info(f'*** Basic curl check passed (HTTP {http_code})\n')
            else:
                info(f'*** Basic curl check FAILED (HTTP {http_code})\n')

            # Throughput test
            run_throughput_test(h_last, h1_ip, duration)

        except KeyboardInterrupt:
            info('*** Interrupted by user\n')
        finally:
            info('*** Stopping network\n')
            net.stop()
            time.sleep(2)

    info('\n*** All tree topology tests completed.\n')

if __name__ == '__main__':
    main()
