#!/usr/bin/env python

"""
Multi-hop topology script for testing TCP iperf performance with variable hop counts.
Tests TCP iperf performance between h1 and h2 across different hop counts: 1, 4, 7, 10, 13, 16, 19, 21 hops.
Each test creates a linear topology with the specified number of hops.
"""

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost, OVSController, OVSBridge
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
import time


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


def run_perf_test(hops):
    """Create network with specified hops and run TCP performance test"""
    info("*** Creating topology with %d hops\n" % hops)
    topo = MultiHopTopo(hops=hops)
    net = Mininet(topo=topo,
                  controller=OVSController,
                  # switch=OVSBridge,
                  link=TCLink,
                  autoStaticArp=True)
    net.start()

    # Simplified wait time: 1s per hop, with a 3s minimum.
    convergence_time = max(3, hops)
    info("*** Waiting %d seconds for network to settle (%d hops)...\n" % (convergence_time, hops))
    time.sleep(convergence_time)

    # Simplified connectivity check
    info("*** Testing network connectivity for %d hops\n" % hops)
    h1, h2 = net.getNodeByName('h1', 'h2')

    # A single ping with a generous timeout (5s base + 1s per hop)
    ping_timeout = str(5 + hops)
    info("*** Pinging with a %s-second timeout...\n" % ping_timeout)
    result = net.ping([h1, h2], timeout=ping_timeout)

    if result == 0:
        info("*** SUCCESS: Connectivity established\n")
    else:
        info("*** WARNING: Connectivity check failed (%.0f%% loss), continuing test...\n" % result)

    # Hosts already obtained above

    info("*** Starting iperf test from h1 to h2 (%d hops)\n" % hops)

    # Start iperf server on h2
    h2.cmd('iperf -s &')

    # Give server time to start
    time.sleep(1)

    # Start iperf client on h1
    output = h1.cmd('iperf -c %s -t 10 -i 1' % h2.IP())
    info("*** Results for %d hops:\n" % hops)
    info(output)

    # Clean up
    h2.cmd('pkill iperf')
    net.stop()

    # Wait between tests
    time.sleep(2)


def multi_hop_performance_test():
    """Run performance tests across multiple hop counts"""
    # Test different hop counts: 1, 4, 7, 10, 13, 16, 19, 21
    hop_counts = [1, 4, 7, 10, 13, 16, 19, 21]

    info("*** Starting multi-hop performance testing\n")
    info("*** Testing hop counts: %s\n" % str(hop_counts))

    results_summary = []

    for hops in hop_counts:
        info("\n" + "="*60 + "\n")
        info("*** TESTING %d HOPS ***\n" % hops)
        info("="*60 + "\n")

        try:
            run_perf_test(hops)
            results_summary.append("✓ %d hops: Test completed" % hops)
        except Exception as e:
            error_msg = "✗ %d hops: Test failed - %s" % (hops, str(e))
            info("*** ERROR: %s\n" % error_msg)
            results_summary.append(error_msg)

    # Print summary
    info("\n" + "="*60 + "\n")
    info("*** TEST SUMMARY ***\n")
    info("="*60 + "\n")
    for result in results_summary:
        info("%s\n" % result)
    info("="*60 + "\n")


if __name__ == '__main__':
    setLogLevel('info')
    multi_hop_performance_test()
