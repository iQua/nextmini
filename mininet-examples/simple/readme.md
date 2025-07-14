## Description

This is a replica of simple example in Nextmini with Mininet. It has three nodes connected through a single switch. An `iperf` test is done to test the bandwith from node 1 to node 3.

_Note :This test is done with [Mininet](https://mininet.org/download/) installed on Ubuntu 22.04._

## Instruction

First test if Mininet is installed by the following commands:

```bash
mn --version
```

Then, run the python script to initiate the `iperf` test:

```bash
sudo python3 simple.py
```

## Result

```bash
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3
*** Adding switches:
s1
*** Adding links:
(h1, s1) (h2, s1) (h3, s1)
*** Configuring hosts
h1 h2 h3
*** Starting controller
c0
*** Starting 1 switches
s1 ...
*** Testing network connectivity
*** Ping: testing ping reachability
h1 -> h2 h3
h2 -> h1 h3
h3 -> h1 h2
*** Results: 0% dropped (6/6 received)
*** Dumping host connections
h1 h1-eth0:s1-eth1
h2 h2-eth0:s1-eth2
h3 h3-eth0:s1-eth3
------------------------------------------------------------
Client connecting to 10.0.0.3, TCP port 5001
TCP window size: 85.3 KByte (default)
------------------------------------------------------------
[  1] local 10.0.0.1 port 34474 connected with 10.0.0.3 port 5001
[ ID] Interval       Transfer     Bandwidth
[  1] 0.0000-1.0000 sec  1.69 GBytes  14.5 Gbits/sec
[  1] 1.0000-2.0000 sec  1.83 GBytes  15.7 Gbits/sec
[  1] 2.0000-3.0000 sec  2.00 GBytes  17.2 Gbits/sec
[  1] 3.0000-4.0000 sec  1.89 GBytes  16.2 Gbits/sec
[  1] 4.0000-5.0000 sec  1.95 GBytes  16.8 Gbits/sec
[  1] 0.0000-5.0034 sec  9.37 GBytes  16.1 Gbits/sec
*** Stopping 1 controllers
c0
*** Stopping 3 links
...
*** Stopping 1 switches
s1
*** Stopping 3 hosts
h1 h2 h3
*** Done
```
