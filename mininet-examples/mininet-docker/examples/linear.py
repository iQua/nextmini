#!/usr/bin/env python

"""
Linear topology script with 5 hosts and 4 hops forwarding.
Tests TCP iperf performance between h1 and h5.
Topology: h1 -- s1 -- h2 -- s2 -- h3 -- s3 -- h4 -- s4 -- h5
h1 ---- s1 ---- s2 ---- s3 ---- s4 ---- h5
              |       |       |
              h2      h3      h4
"""

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
import time


class LinearTopo(Topo):
    "Linear topology with 5 hosts and 4 switches"
    def build(self):
        # Add hosts
        h1 = self.addHost('h1')
        h2 = self.addHost('h2')
        h3 = self.addHost('h3')
        h4 = self.addHost('h4')
        h5 = self.addHost('h5')

        # Add switches
        s1 = self.addSwitch('s1')
        s2 = self.addSwitch('s2')
        s3 = self.addSwitch('s3')
        s4 = self.addSwitch('s4')

        # Create linear connections
        # h1 -- s1 -- s2 -- s3 -- s4 -- h5 (with h2, h3, h4 connected to intermediate switches)
        self.addLink(h1, s1)
        self.addLink(s1, s2)
        self.addLink(h2, s2)
        self.addLink(s2, s3)
        self.addLink(h3, s3)
        self.addLink(s3, s4)
        self.addLink(h4, s4)
        self.addLink(s4, h5)


def tcp_perf_test():
    "Create linear network and run TCP performance test from h1 to h5"
    topo = LinearTopo()
    net = Mininet(topo=topo,
                  link=TCLink,
                  autoStaticArp=True)
    net.start()

    # Verify connectivity
    info("*** Testing network connectivity\n")
    net.pingAll()

    info("*** Dumping host connections\n")
    dumpNodeConnections(net.hosts)

    # Get hosts h1 and h5 for iperf test
    h1, h5 = net.getNodeByName('h1', 'h5')

    info("*** Starting iperf test from h1 to h5 (4 hops forwarding)\n")

    # Start iperf server on h5
    h5.cmd('iperf -s &')

    # Give server time to start
    time.sleep(1)

    # Start iperf client on h1
    output = h1.cmd('iperf -c %s -t 10 -i 1' % h5.IP())
    info(output)

    # Clean up
    h5.cmd('pkill iperf')
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    tcp_perf_test()
