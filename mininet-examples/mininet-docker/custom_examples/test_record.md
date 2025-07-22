```text
root@3153d62fa9a9:/opt/mininet-examples# python3 linear.py
*** Error setting resource limits. Mininet's performance may be affected.
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3 h4 h5
*** Adding switches:
s1 s2 s3 s4
*** Adding links:
(h1, s1) (h2, s2) (h3, s3) (h4, s4) (s1, s2) (s2, s3) (s3, s4) (s4, h5)
*** Configuring hosts
h1 h2 h3 h4 h5
*** Starting controller
c0
*** Starting 4 switches
s1 s2 s3 s4 ...
*** Testing network connectivity
*** Ping: testing ping reachability
h1 -> h2 h3 h4 h5
h2 -> h1 h3 h4 h5
h3 -> h1 h2 h4 h5
h4 -> h1 h2 h3 h5
h5 -> h1 h2 h3 h4
*** Results: 0% dropped (20/20 received)
*** Dumping host connections
h1 h1-eth0:s1-eth1
h2 h2-eth0:s2-eth2
h3 h3-eth0:s3-eth2
h4 h4-eth0:s4-eth2
h5 h5-eth0:s4-eth3
*** Starting iperf test from h1 to h5 (4 hops forwarding)
------------------------------------------------------------
Client connecting to 10.0.0.5, TCP port 5001
TCP window size: 85.0 KByte (default)
------------------------------------------------------------
[  1] local 10.0.0.1 port 42144 connected with 10.0.0.5 port 5001
[ ID] Interval       Transfer     Bandwidth
[  1] 0.0000-1.0000 sec  13.5 GBytes   116 Gbits/sec
[  1] 1.0000-2.0000 sec  13.0 GBytes   112 Gbits/sec
[  1] 2.0000-3.0000 sec  12.9 GBytes   111 Gbits/sec
[  1] 3.0000-4.0000 sec  12.7 GBytes   109 Gbits/sec
[  1] 4.0000-5.0000 sec  12.6 GBytes   108 Gbits/sec
[  1] 5.0000-6.0000 sec  13.0 GBytes   111 Gbits/sec
[  1] 6.0000-7.0000 sec  12.6 GBytes   109 Gbits/sec
[  1] 7.0000-8.0000 sec  12.8 GBytes   110 Gbits/sec
[  1] 8.0000-9.0000 sec  12.6 GBytes   109 Gbits/sec
[  1] 9.0000-10.0000 sec  12.7 GBytes   109 Gbits/sec
[  1] 0.0000-10.0084 sec   128 GBytes   110 Gbits/sec
*** Stopping 1 controllers
c0
*** Stopping 8 links
........
*** Stopping 4 switches
s1 s2 s3 s4
*** Stopping 5 hosts
h1 h2 h3 h4 h5
*** Done
```

```text
root@3153d62fa9a9:/opt/mininet-examples# python3 simple.py
*** Error setting resource limits. Mininet's performance may be affected.
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
TCP window size: 85.0 KByte (default)
------------------------------------------------------------
[  1] local 10.0.0.1 port 60236 connected with 10.0.0.3 port 5001
[ ID] Interval       Transfer     Bandwidth
[  1] 0.0000-1.0000 sec  19.1 GBytes   164 Gbits/sec
[  1] 1.0000-2.0000 sec  22.1 GBytes   190 Gbits/sec
[  1] 2.0000-3.0000 sec  22.5 GBytes   193 Gbits/sec
[  1] 3.0000-4.0000 sec  19.9 GBytes   171 Gbits/sec
[  1] 4.0000-5.0000 sec  20.9 GBytes   179 Gbits/sec
[  1] 0.0000-5.0033 sec   104 GBytes   179 Gbits/sec
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
Second time running the `iperf` in simple.py, the output is as follows:
```text
------------------------------------------------------------
Client connecting to 10.0.0.3, TCP port 5001
TCP window size: 85.0 KByte (default)
------------------------------------------------------------
[  1] local 10.0.0.1 port 49722 connected with 10.0.0.3 port 5001
[ ID] Interval       Transfer     Bandwidth
[  1] 0.0000-1.0000 sec  15.9 GBytes   137 Gbits/sec
[  1] 1.0000-2.0000 sec  15.8 GBytes   136 Gbits/sec
[  1] 2.0000-3.0000 sec  15.9 GBytes   137 Gbits/sec
[  1] 3.0000-4.0000 sec  15.8 GBytes   136 Gbits/sec
[  1] 4.0000-5.0000 sec  16.5 GBytes   142 Gbits/sec
[  1] 0.0000-5.0157 sec  80.1 GBytes   137 Gbits/sec
```
