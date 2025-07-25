#!/usr/bin/python3
# copyright 2017 Peter Dordal
# licensed under the Apache 2.0 license

"""Mininet router topology example

A chain of N routers between two hosts.

   +----+      +----+      +----+           +----+      +----+
   | h1 |------| r1 |------| r2 |--- ..... ---| rN |------| h2 |
   +----+      +----+      +----+           +----+      +----+

Subnets are 10.0.0.0/24 to 10.0.N.0/24
The last IPv4 address byte (host byte) is 2 on the left and 1 on the right, for routers.
ri-eth0 is the interface on the left and ri-eth1 is on the right. IPv4 addresses look like:
   10.0.[k-1].2--r[k]--10.0.k.1

"""

from mininet.net import Mininet
from mininet.node import Node, OVSKernelSwitch, Controller, RemoteController
from mininet.cli import CLI
from mininet.link import TCLink
from mininet.topo import Topo
from mininet.log import setLogLevel, info
import argparse
import time

ENABLE_LEFT_TO_RIGHT_ROUTING = True		# tell all routers how to get to h2
ENABLE_RIGHT_TO_LEFT_ROUTING = True		# ditto for h1
ENABLE_RIP = False				# enable RIPv2; rip.py must be in same directory

class LinuxRouter( Node ):	# from the Mininet library
    "A Node with IP forwarding enabled."

    def config( self, **params ):
        super( LinuxRouter, self).config( **params )
        # Enable forwarding on the router
        info ('enabling forwarding on ', self)
        self.cmd( 'sysctl net.ipv4.ip_forward=1' )

    def terminate( self ):
        self.cmd( 'sysctl net.ipv4.ip_forward=0' )
        super( LinuxRouter, self ).terminate()


class RTopo(Topo):
    def __init__(self, **kwargs):
#    def build(self, **_kwargs):     # special names?
        super(RTopo, self).__init__(**kwargs)
        for key in kwargs:
           if key == 'N': N=kwargs[key]

        h1 = self.addHost( 'h1', ip=ip(0,10,24), defaultRoute='via '+ ip(0,2) )
        h2 = self.addHost( 'h2', ip=ip(N,10,24), defaultRoute='via '+ ip(N,1) )

        rlist = []

        for i in range(1,N+1):
            ri = self.addHost('r'+str(i), cls=LinuxRouter)
            rlist.append(ri)

        self.addLink( h1, rlist[0], intfName1 = 'h1-eth0', intfName2 = 'r1-eth0')

        for i in range(1,N):  # link from ri to r[i+1]
            self.addLink(rlist[i-1], rlist[i], inftname1 = 'r'+str(i)+'-eth1', inftname2 = 'r'+str(i+1)+'-eth0')

        self.addLink( rlist[N-1], h2, intfName1 = 'r'+str(N)+'-eth1', intfName2 = 'h2-eth0')


def run_router_test(N):
    """Create and test a router chain with N routers."""
    info("\n" + "="*60 + "\n")
    info(f"*** Testing with {N} routers (hops) ***\n")
    info("="*60 + "\n")

    rtopo = RTopo(N=N)
    net = Mininet(topo=rtopo, link=TCLink, autoSetMacs=True, controller=None)
    net.start()

    # Configure router interfaces and routes
    for i in range(1, N + 1):
        r = net['r' + str(i)]
        left_intf = 'r' + str(i) + '-eth0'
        right_intf = 'r' + str(i) + '-eth1'
        r.cmd(f'ifconfig {left_intf} {ip(i - 1, 2, 24)}')
        r.cmd(f'ifconfig {right_intf} {ip(i, 1, 24)}')
        rp_disable(r)

    h1, h2 = net['h1'], net['h2']

    # Set up static routes
    if ENABLE_LEFT_TO_RIGHT_ROUTING:
        for i in range(1, N + 1):
            r = net['r' + str(i)]
            # Add route to h2's subnet via the next router in the chain
            if i < N:
                next_hop_ip = ip(i, 2)
                r.cmd(f'ip route add to {ip(N, 0, 24)} via {next_hop_ip} dev r{i}-eth1')

    if ENABLE_RIGHT_TO_LEFT_ROUTING:
        for i in range(1, N + 1):
            r = net['r' + str(i)]
            # Add route to h1's subnet via the previous router in the chain
            if i > 1:
                # The gateway is the router to the left, r(i-1), which has IP ip(i-1, 1) on this subnet
                r.cmd(f'ip route add to {ip(0, 0, 24)} via {ip(i - 1, 1)} dev r{i}-eth0')

    info('*** Testing connectivity...\n')
    timeout = 2 * N
    result = net.ping([h1, h2], timeout=str(timeout))

    if result != 0:
        info(f"*** FAILURE: Connectivity check failed for {N} hops.\n")
    else:
        info("*** SUCCESS: Connectivity established.\n")
        info("*** Starting iperf test (server-side output only)...\n")

        # Start iperf server on h2
        h2.cmd('iperf -s &')
        time.sleep(1)

        # Start iperf client on h1, suppressing its output
        h1.cmd(f'iperf -c {h2.IP()} -t 10 > /dev/null 2>&1')

        # pkill on the h2 iperf process will cause its summary to print.
        # Redirect stderr to stdout is needed, so pkill won't complain if iperf is not running.
        result = h2.cmd('pkill iperf 2>&1')
        info(f"*** iperf results for {N} hops:\n{result}")

    net.stop()


def main():
    """Run performance tests across multiple hop counts."""
    hop_counts = [2, 4, 6, 8, 10, 12, 14, 16]

    for hops in hop_counts:
        try:
            run_router_test(hops)
        except Exception as e:
            info(f"\n*** ERROR on {hops}-hop test: {e}\n")
        time.sleep(2)

    info("\n" + "="*60 + "\n")
    info("*** All tests completed. ***\n")
    info("="*60 + "\n")


# The following generates IP addresses from a subnet number and a host number
# ip(4,2) returns 10.0.4.2, and ip(4,2,24) returns 10.0.4.2/24
def ip(subnet,host,prefix=None):
    addr = '10.0.'+str(subnet)+'.' + str(host)
    if prefix != None: addr = addr + '/' + str(prefix)
    return addr

# For some examples we need to disable the default blocking of forwarding of packets with no reverse path
def rp_disable(host):
    ifaces = host.cmd('ls /proc/sys/net/ipv4/conf')
    ifacelist = ifaces.split()    # default is to split on whitespace
    for iface in ifacelist:
       if iface != 'lo': host.cmd('sysctl net.ipv4.conf.' + iface + '.rp_filter=0')


setLogLevel('info')	# 'info' is normal; 'debug' is for when there are problems
main()

"""
Manual routing commands for N=3

r1: ip route add to 10.0.3.0/24 via 10.0.1.2 dev r1-eth1
r2: ip route add to 10.0.3.0/24 via 10.0.2.2 dev r2-eth2

r1: route add -net 10.0.3.0/24 gw 10.0.1.2
r2: route add -net 10.0.3.0/24 gw 10.0.2.2

r3: ip route add to 10.0.0.0/24 via 10.0.2.1 dev r3-eth0
r2: ip route add to 10.0.0.0/24 via 10.0.1.1 dev r2-eth0

r3: route add -net 10.0.0.0/24 gw 10.0.2.1
r2: route add -net 10.0.0.0/24 gw 10.0.1.1

"""
