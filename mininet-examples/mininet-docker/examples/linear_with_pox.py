#!/usr/bin/env python

"""
Multi-hop topology script for testing TCP iperf performance with variable hop counts using external POX controller.
Tests TCP iperf performance between h1 and h2 across different hop counts: 1, 4, 7, 10, 13, 16, 19, 21 hops.
Each test creates a linear topology with the specified number of hops.
"""

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost, OVSBridge, RemoteController, UserSwitch
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
import time
import subprocess
import os
import signal
import socket


class MultiHopTopo(Topo):
    "Linear topology with variable number of hops"
    def build(self, hops=1):
        # Add source and destination hosts
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')

        # Add switches based on number of hops
        switches = []
        for i in range(hops):
            switch = self.addSwitch('s%d' % (i + 1))
            switches.append(switch)

        # Create linear connections
        # Connect h1 to first switch
        self.addLink(h1, switches[0])

        # Connect switches in series
        for i in range(len(switches) - 1):
            self.addLink(switches[i], switches[i + 1])

        # Connect last switch to h2
        self.addLink(switches[-1], h2)


def check_pox_controller_health():
    """Check if POX controller is responding on port 6634"""
    try:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(2)
        result = sock.connect_ex(('127.0.0.1', 6634))
        sock.close()
        return result == 0
    except:
        return False


def start_pox_controller():
    """Start POX controller with l2_learning app"""
    info("*** Starting POX controller for linear topology test...\n")
    # Start POX controller in background on port 6634 to avoid conflicts
    pox_cmd = ["python3", "/opt/pox/pox.py", "openflow.of_01", "--port=6634", "forwarding.l2_learning"]
    pox_process = subprocess.Popen(pox_cmd, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
    
    # Wait and check if controller is responding
    for i in range(10):  # Wait up to 10 seconds
        time.sleep(1)
        if check_pox_controller_health():
            info("*** POX controller is ready\n")
            return pox_process
        info("*** Waiting for POX controller to start (%d/10)...\n" % (i + 1))
    
    info("*** WARNING: POX controller may not be fully ready\n")
    return pox_process


def cleanup_network():
    """Clean up any remaining network components"""
    info("*** Cleaning up network...\n")
    try:
        # Kill any remaining iperf processes
        subprocess.run(['sudo', 'pkill', '-f', 'iperf'], stderr=subprocess.DEVNULL)
        # Clean up mininet
        subprocess.run(['sudo', 'mn', '-c'], stderr=subprocess.DEVNULL)
        time.sleep(2)
    except:
        pass


def run_perf_test(hops, pox_process=None):
    """Create network with specified hops and run TCP performance test"""
    info("*** Creating topology with %d hops\n" % hops)
    
    try:
        # Clean up before starting
        cleanup_network()
        
        topo = MultiHopTopo(hops=hops)

        # Use RemoteController to connect to POX
        net = Mininet(topo=topo,
                      controller=lambda name: RemoteController(name, ip='127.0.0.1', port=6634),
                      link=TCLink,
                      autoStaticArp=True)
        
        info("*** Starting network...\n")
        net.start()

        # Increased wait time for larger topologies - more generous timing
        convergence_time = max(10, hops * 3)  # Increased from hops * 2 to hops * 3
        info("*** Waiting %d seconds for network to settle (%d hops)...\n" % (convergence_time, hops))
        time.sleep(convergence_time)

        # Test connectivity with multiple attempts
        info("*** Testing network connectivity for %d hops\n" % hops)
        h1, h2 = net.getNodeByName('h1', 'h2')

        # Multiple ping attempts with increasing timeout
        ping_success = False
        for attempt in range(3):
            ping_timeout = str(15 + hops * 2)  # Increased timeout
            info("*** Ping attempt %d with %s-second timeout...\n" % (attempt + 1, ping_timeout))
            result = net.ping([h1, h2], timeout=ping_timeout)

            if result == 0:
                info("*** SUCCESS: Connectivity established on attempt %d\n" % (attempt + 1))
                ping_success = True
                break
            else:
                info("*** Ping attempt %d failed (%.0f%% loss), retrying...\n" % (attempt + 1, result))
                time.sleep(5)  # Wait before retry

        if not ping_success:
            info("*** WARNING: All ping attempts failed, but continuing with iperf test...\n")

        info("*** Starting iperf test from h1 to h2 (%d hops)\n" % hops)

        # Start iperf server on h2
        info("*** Starting iperf server on h2...\n")
        h2.cmd('iperf -s &')

        # Give server more time to start
        time.sleep(5)

        # Start iperf client on h1 with shorter test duration for larger topologies
        test_duration = max(5, min(10, 20 - hops))  # Shorter tests for more hops
        info("*** Running iperf client test for %d seconds (%d hops):\n" % (test_duration, hops))
        client_output = h1.cmd('iperf -c %s -t %d -i 1' % (h2.IP(), test_duration))
        info(client_output)

        # Stop the server
        h2.cmd('pkill iperf 2>&1')
        time.sleep(1)

        info("*** Test completed for %d hops\n" % hops)
        return True

    except Exception as e:
        info("*** ERROR in test for %d hops: %s\n" % (hops, str(e)))
        return False
        
    finally:
        try:
            if 'net' in locals():
                info("*** Stopping network for %d hops...\n" % hops)
                net.stop()
        except Exception as e:
            info("*** Error stopping network: %s\n" % str(e))
        
        # Additional cleanup between tests
        cleanup_network()
        
        # Longer wait between tests for system recovery
        wait_time = max(5, hops // 2)
        info("*** Waiting %d seconds before next test...\n" % wait_time)
        time.sleep(wait_time)


def multi_hop_performance_test():
    """Run performance tests across multiple hop counts with external POX controller"""
    # Test different hop counts, including large ones
    hop_counts = [2, 4, 7, 10, 13, 16, 19, 21, 25, 30]  # Added even larger topologies

    info("*** Starting multi-hop performance testing with POX controller\n")
    info("*** Testing hop counts: %s\n" % str(hop_counts))

    # Start POX controller once for all tests
    pox_process = start_pox_controller()
    
    try:
        results_summary = []

        for i, hops in enumerate(hop_counts):
            info("\n" + "="*60 + "\n")
            info("*** TESTING %d HOPS WITH POX CONTROLLER (%d/%d) ***\n" % (hops, i+1, len(hop_counts)))
            info("="*60 + "\n")

            # Check controller health before each test
            if not check_pox_controller_health():
                info("*** WARNING: POX controller not responding, attempting restart...\n")
                pox_process.terminate()
                time.sleep(3)
                pox_process = start_pox_controller()

            try:
                success = run_perf_test(hops, pox_process)
                if success:
                    results_summary.append("✓ %d hops: Test completed successfully" % hops)
                else:
                    results_summary.append("✗ %d hops: Test completed with errors" % hops)
                    
            except Exception as e:
                error_msg = "✗ %d hops: Test failed - %s" % (hops, str(e))
                info("*** ERROR: %s\n" % error_msg)
                results_summary.append(error_msg)
                # Continue with next test instead of stopping

            info("*** Completed test for %d hops, continuing to next...\n" % hops)

        # Print summary
        info("\n" + "="*60 + "\n")
        info("*** FINAL TEST SUMMARY ***\n")
        info("="*60 + "\n")
        for result in results_summary:
            info("%s\n" % result)
        info("="*60 + "\n")
        info("*** All tests completed!\n")
        
    except KeyboardInterrupt:
        info("*** Tests interrupted by user\n")
        
    finally:
        # Stop POX controller
        info("*** Stopping POX controller\n")
        try:
            pox_process.terminate()
            pox_process.wait(timeout=5)
        except subprocess.TimeoutExpired:
            pox_process.kill()
        except:
            pass
        
        # Final cleanup
        cleanup_network()


if __name__ == '__main__':
    setLogLevel('info')
    multi_hop_performance_test() 