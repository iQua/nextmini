#!/usr/bin/env python

"""
Create a network where different switches are connected to
different controllers, by creating a custom Switch() subclass.
"""

from mininet.net import Mininet
from mininet.node import OVSSwitch, Controller, RemoteController
from mininet.topolib import TreeTopo
from mininet.log import setLogLevel
from mininet.cli import CLI

setLogLevel( 'info' )

# Use RemoteController to connect to already running controller on port 6633
# And create additional controllers on different ports
c0 = RemoteController( 'c0', ip='127.0.0.1', port=6633 )
c1 = Controller( 'c1', port=6634 )  # Use a different port for this controller
c2 = Controller( 'c2', port=6635 )  # Use a different port for this controller

cmap = { 's1': c0, 's2': c1, 's3': c2 }

class MultiSwitch( OVSSwitch ):
    "Custom Switch() subclass that connects to different controllers"
    def start( self, controllers ):
        return OVSSwitch.start( self, [ cmap[ self.name ] ] )


topo = TreeTopo( depth=2, fanout=2 )
net = Mininet( topo=topo, switch=MultiSwitch, build=False, waitConnected=True )
# Only add controllers that need to be started - not the remote one
for c in [ c1, c2 ]:
    net.addController(c)
net.build()
net.start()
CLI( net )
net.stop()
