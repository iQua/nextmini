#!/usr/bin/env python

"Create a 64-node tree network using OVS, and test connectivity using ping."

from mininet.log import setLogLevel, info
from mininet.node import OVSKernelSwitch, Host
from mininet.topolib import TreeNet


class HostV4( Host ):
    "Disable IPv6 and its awful neighbor discovery"
    def __init__( self, *args, **kwargs ):
        super( HostV4, self ).__init__( *args, **kwargs )
        cfgs = [ 'all.disable_ipv6=1', 'default.disable_ipv6=1',
                 'default.autoconf=0', 'lo.autoconf=0' ]
        for cfg in cfgs:
            self.cmd( 'sysctl -w net.ipv6.conf.' + cfg )


def treePing64():
    "Run ping test on 64-node tree network using OVS."

    info( "*** Testing Open vSwitch kernel datapath\n" )
    network = TreeNet( depth=2, fanout=8, switch=OVSKernelSwitch,
                       host=HostV4, waitConnected=True )
    result = network.run( network.pingAll )
    
    info( "\n*** Tree network ping results:\n" )
    info( "Open vSwitch kernel: %d%% packet loss\n" % result )
    info( '\n' )


if __name__ == '__main__':
    setLogLevel( 'info' )
    treePing64()