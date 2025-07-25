#!/usr/bin/env python

"""
Simple script to create a Mininet topology with 3 hosts
and run a TCP iperf test between nodes 1 and 3 to measure maximum throughput.
"""

from mininet.topo import Topo
from mininet.net import Mininet
from mininet.node import CPULimitedHost
from mininet.link import TCLink
from mininet.util import dumpNodeConnections
from mininet.log import setLogLevel, info
import time


class SingleSwitchTopo(Topo):
    "Single switch connected to n hosts."
    def build(self, n=3):
        switch = self.addSwitch('s1')
        for h in range(n):
            host = self.addHost(f'h{h+1}')
            self.addLink(host, switch)


def tcp_perf_test():
    "Create network and run TCP performance test for maximum throughput"
    topo = SingleSwitchTopo(n=3)
    net = Mininet(topo=topo,
                  link=TCLink,
                  autoStaticArp=True)
    net.start()

    # Verify connectivity
    info("*** Testing network connectivity\n")
    net.pingAll()

    info("*** Dumping host connections\n")
    dumpNodeConnections(net.hosts)

    # gets hosts.
    h1, h3 = net.getNodeByName('h1', 'h3')

    # starts iperf server.
    h3.cmd('iperf -s &')

    # starts iperf client.
    output = h1.cmd('iperf -c %s -t 5 -i 1' % h3.IP())
    info(output)

    h3.cmd('pkill iperf')
    net.stop()


if __name__ == '__main__':
    setLogLevel('info')
    tcp_perf_test()
