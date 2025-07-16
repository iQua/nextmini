## multiping.py

```text
root@38883c844472:/opt/mininet-examples# python3 multiping.py
*** Error setting resource limits. Mininet's performance may be affected.
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20
*** Adding switches:
s1
*** Adding links:
(h1, s1) (h2, s1) (h3, s1) (h4, s1) (h5, s1) (h6, s1) (h7, s1) (h8, s1) (h9, s1) (h10, s1) (h11, s1) (h12, s1) (h13, s1) (h14, s1) (h15, s1) (h16, s1) (h17, s1) (h18, s1) (h19, s1) (h20, s1)
*** Configuring hosts
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20
*** Starting controller
c0
*** Starting 1 switches
s1 ...
*** Waiting for switches to connect
s1
*** Host h1 (10.0.0.1) will be pinging ips: 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4 10.0.0.200
*** Host h2 (10.0.0.2) will be pinging ips: 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4 10.0.0.200
*** Host h3 (10.0.0.3) will be pinging ips: 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4 10.0.0.200
*** Host h4 (10.0.0.4) will be pinging ips: 10.0.0.1 10.0.0.2 10.0.0.3 10.0.0.4 10.0.0.200
*** Host h5 (10.0.0.5) will be pinging ips: 10.0.0.5 10.0.0.6 10.0.0.7 10.0.0.8 10.0.0.200
*** Host h6 (10.0.0.6) will be pinging ips: 10.0.0.5 10.0.0.6 10.0.0.7 10.0.0.8 10.0.0.200
*** Host h7 (10.0.0.7) will be pinging ips: 10.0.0.5 10.0.0.6 10.0.0.7 10.0.0.8 10.0.0.200
*** Host h8 (10.0.0.8) will be pinging ips: 10.0.0.5 10.0.0.6 10.0.0.7 10.0.0.8 10.0.0.200
*** Host h9 (10.0.0.9) will be pinging ips: 10.0.0.9 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.200
*** Host h10 (10.0.0.10) will be pinging ips: 10.0.0.9 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.200
*** Host h11 (10.0.0.11) will be pinging ips: 10.0.0.9 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.200
*** Host h12 (10.0.0.12) will be pinging ips: 10.0.0.9 10.0.0.10 10.0.0.11 10.0.0.12 10.0.0.200
*** Host h13 (10.0.0.13) will be pinging ips: 10.0.0.13 10.0.0.14 10.0.0.15 10.0.0.16 10.0.0.200
*** Host h14 (10.0.0.14) will be pinging ips: 10.0.0.13 10.0.0.14 10.0.0.15 10.0.0.16 10.0.0.200
*** Host h15 (10.0.0.15) will be pinging ips: 10.0.0.13 10.0.0.14 10.0.0.15 10.0.0.16 10.0.0.200
*** Host h16 (10.0.0.16) will be pinging ips: 10.0.0.13 10.0.0.14 10.0.0.15 10.0.0.16 10.0.0.200
*** Host h17 (10.0.0.17) will be pinging ips: 10.0.0.17 10.0.0.18 10.0.0.19 10.0.0.20 10.0.0.200
*** Host h18 (10.0.0.18) will be pinging ips: 10.0.0.17 10.0.0.18 10.0.0.19 10.0.0.20 10.0.0.200
*** Host h19 (10.0.0.19) will be pinging ips: 10.0.0.17 10.0.0.18 10.0.0.19 10.0.0.20 10.0.0.200
*** Host h20 (10.0.0.20) will be pinging ips: 10.0.0.17 10.0.0.18 10.0.0.19 10.0.0.20 10.0.0.200
h1: 10.0.0.1 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.200 1 packets transmitted, 0 received, 100% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.5 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.13 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.17 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.1 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.9 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.14 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.18 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.2 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.10 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.6 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.11 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.7 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.15 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.19 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.3 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h5: 10.0.0.5 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h6: 10.0.0.6 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h3: 10.0.0.3 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h19: 10.0.0.19 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h14: 10.0.0.14 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h20: 10.0.0.20 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h12: 10.0.0.12 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h9: 10.0.0.9 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h18: 10.0.0.18 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h2: 10.0.0.2 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h8: 10.0.0.8 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h1: 10.0.0.1 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h7: 10.0.0.7 -> 10.0.0.8 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h10: 10.0.0.10 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h4: 10.0.0.4 -> 10.0.0.4 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h13: 10.0.0.13 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h16: 10.0.0.16 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h17: 10.0.0.17 -> 10.0.0.20 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h15: 10.0.0.15 -> 10.0.0.16 1 packets transmitted, 1 received, 0% packet loss, time 0ms
h11: 10.0.0.11 -> 10.0.0.12 1 packets transmitted, 1 received, 0% packet loss, time 0ms
*** Stopping 1 controllers
c0
*** Stopping 20 links
....................
*** Stopping 1 switches
s1
*** Stopping 20 hosts
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20
*** Done
```

## simpleperf.py

Changed the CPULimitedHost to Host, then the test result is as below:

```text
root@f8d908c037ea:/opt/mininet-examples# python3 simpleperf.py
*** Error setting resource limits. Mininet's performance may be affected.
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3 h4
*** Adding switches:
s1
*** Adding links:
(10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (h1, s1) (10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (h2, s1) (10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (h3, s1) (10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (h4, s1)
*** Configuring hosts
h1 h2 h3 h4
*** Starting controller
c0
*** Starting 1 switches
s1 ...(10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss) (10.00Mbit 5ms delay 10.00000% loss)
Dumping host connections
h1 h1-eth0:s1-eth1
h2 h2-eth0:s1-eth2
h3 h3-eth0:s1-eth3
h4 h4-eth0:s1-eth4
Testing bandwidth between h1 and h4 (lossy=True)
*** Iperf: testing UDP bandwidth between h1 and h4
*** Results: ['10M', '8.57 Mbits/sec', '8.57 Mbits/sec']
*** Stopping 1 controllers
c0
*** Stopping 4 links
....
*** Stopping 1 switches
s1
*** Stopping 4 hosts
h1 h2 h3 h4
*** Done
```

## Simple.py

```text
root@f8d908c037ea:/opt/mininet-examples# python3 simple.py
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
[  1] local 10.0.0.1 port 49600 connected with 10.0.0.3 port 5001
[ ID] Interval       Transfer     Bandwidth
[  1] 0.0000-1.0000 sec  10.4 GBytes  89.7 Gbits/sec
[  1] 1.0000-2.0000 sec  10.7 GBytes  92.3 Gbits/sec
[  1] 2.0000-3.0000 sec  10.6 GBytes  91.4 Gbits/sec
[  1] 3.0000-4.0000 sec  10.9 GBytes  93.7 Gbits/sec
[  1] 4.0000-5.0000 sec  11.1 GBytes  95.3 Gbits/sec
[  1] 0.0000-5.0109 sec  53.8 GBytes  92.3 Gbits/sec
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

Tested with simple test in Boston, the speed is around 90Gbits/sec.

## treeping64_ovs.py

Different from treeping64 example, this doesn't need UserSwitch.

```text
root@1902a4da3fdc:/opt/mininet-examples# python3 treeping64_ovs.py
*** Testing Open vSwitch kernel datapath
*** Error setting resource limits. Mininet's performance may be affected.
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
*** Adding switches:
s1 s2 s3 s4 s5 s6 s7 s8 s9
*** Adding links:
(s1, s2) (s1, s3) (s1, s4) (s1, s5) (s1, s6) (s1, s7) (s1, s8) (s1, s9) (s2, h1) (s2, h2) (s2, h3) (s2, h4) (s2, h5) (s2, h6) (s2, h7) (s2, h8) (s3, h9) (s3, h10) (s3, h11) (s3, h12) (s3, h13) (s3, h14) (s3, h15) (s3, h16) (s4, h17) (s4, h18) (s4, h19) (s4, h20) (s4, h21) (s4, h22) (s4, h23) (s4, h24) (s5, h25) (s5, h26) (s5, h27) (s5, h28) (s5, h29) (s5, h30) (s5, h31) (s5, h32) (s6, h33) (s6, h34) (s6, h35) (s6, h36) (s6, h37) (s6, h38) (s6, h39) (s6, h40) (s7, h41) (s7, h42) (s7, h43) (s7, h44) (s7, h45) (s7, h46) (s7, h47) (s7, h48) (s8, h49) (s8, h50) (s8, h51) (s8, h52) (s8, h53) (s8, h54) (s8, h55) (s8, h56) (s9, h57) (s9, h58) (s9, h59) (s9, h60) (s9, h61) (s9, h62) (s9, h63) (s9, h64)
*** Configuring hosts
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
*** Starting controller
c0
*** Starting 9 switches
s1 s2 s3 s4 s5 s6 s7 s8 s9 ...
*** Waiting for switches to connect
s1 s2 s3 s4 s5 s6 s7 s8 s9
*** Running test
*** Ping: testing ping reachability
h1 -> h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h2 -> h1 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h3 -> h1 h2 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h4 -> h1 h2 h3 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h5 -> h1 h2 h3 h4 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h6 -> h1 h2 h3 h4 h5 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h7 -> h1 h2 h3 h4 h5 h6 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h8 -> h1 h2 h3 h4 h5 h6 h7 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h9 -> h1 h2 h3 h4 h5 h6 h7 h8 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 X h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h10 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h11 h12 h13 h14 h15 X h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h11 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h12 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h13 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h14 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h15 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h16 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 X h60 h61 h62 h63 h64
h17 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h18 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h19 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h20 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h21 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h22 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h23 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h24 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h25 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h26 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h27 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h28 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h29 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h30 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h31 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h32 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h33 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h34 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h35 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h36 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h37 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h38 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h39 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h40 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h41 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h42 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h43 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h44 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h45 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h46 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h47 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h48 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h49 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h50 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h51 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h52 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h53 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h54 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
h55 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h56 h57 h58 h59 h60 h61 h62 h63 h64
h56 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h57 h58 h59 h60 h61 h62 h63 h64
h57 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h58 h59 h60 h61 h62 h63 h64
h58 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h59 h60 h61 h62 h63 h64
h59 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h60 h61 h62 h63 h64
h60 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h61 h62 h63 h64
h61 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h62 h63 h64
h62 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h63 h64
h63 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h64
h64 -> h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63
*** Results: 0% dropped (4029/4032 received)
*** Stopping 1 controllers
c0
*** Stopping 72 links
........................................................................
*** Stopping 9 switches
s1 s2 s3 s4 s5 s6 s7 s8 s9
*** Stopping 64 hosts
h1 h2 h3 h4 h5 h6 h7 h8 h9 h10 h11 h12 h13 h14 h15 h16 h17 h18 h19 h20 h21 h22 h23 h24 h25 h26 h27 h28 h29 h30 h31 h32 h33 h34 h35 h36 h37 h38 h39 h40 h41 h42 h43 h44 h45 h46 h47 h48 h49 h50 h51 h52 h53 h54 h55 h56 h57 h58 h59 h60 h61 h62 h63 h64
*** Done

*** Tree network ping results:
Open vSwitch kernel: 0% packet loss
```

## nat.py

firstly run:
```
python3 nat.py
```

Then, after you see `mininet>`

Once in the Mininet CLI, test internet connectivity from any host:
```bash
mininet> h1 ping -c 3 google.com
```

Try other internet-requiring commands:
```bash
mininet> h3 curl icanhazip.com
```

```text
root@1902a4da3fdc:/opt/mininet-examples# python3 nat.py
*** Error setting resource limits. Mininet's performance may be affected.
*** Creating network
*** Adding controller
*** Adding hosts:
h1 h2 h3 h4
*** Adding switches:
s1
*** Adding links:
(s1, h1) (s1, h2) (s1, h3) (s1, h4)
*** Configuring hosts
h1 h2 h3 h4
*** Adding "iface nat0-eth0 inet manual" to /etc/network/interfaces
*** Starting controller
c0
*** Starting 1 switches
s1 ...
*** Waiting for switches to connect
s1
*** Hosts are running and should have internet connectivity
*** Type 'exit' or control-D to shut down network
*** Starting CLI:
mininet> h1 ping -c 3 google.com
PING google.com (142.251.41.46) 56(84) bytes of data.
64 bytes from yyz12s08-in-f14.1e100.net (142.251.41.46): icmp_seq=1 ttl=115 time=3.98 ms
64 bytes from yyz12s08-in-f14.1e100.net (142.251.41.46): icmp_seq=2 ttl=115 time=2.55 ms
64 bytes from yyz12s08-in-f14.1e100.net (142.251.41.46): icmp_seq=3 ttl=115 time=2.67 ms

--- google.com ping statistics ---
3 packets transmitted, 3 received, 0% packet loss, time 2003ms
rtt min/avg/max/mdev = 2.551/3.066/3.976/0.645 ms

mininet> h3 curl icanhazip.com
142.150.238.6

```

## Controlnet.py

```bash
python3 controlnet.py
```

```text
ubuntu@ip-172-31-21-32:~/mininet/examples$ sudo python3 controlnet.py
* Creating Control Network
*** Creating network
*** Adding hosts:
c0 c1 c2 c3 root
*** Adding switches:
cs0
*** Adding links:
(c0, cs0) (c1, cs0) (c2, cs0) (c3, cs0) (root, cs0)
*** Configuring hosts
c0 c1 c2 c3 root
* Adding Control Network Controller
* Starting Control Network
*** Starting controller
cc0
*** Starting 1 switches
cs0 ...
*** Waiting for switches to connect
cs0
* Creating Data Network
*** Creating network
*** Adding hosts:
h1 h2 h3 h4
*** Adding switches:
s1 s2 s3
*** Adding links:
(s1, s2) (s1, s3) (s2, h1) (s2, h2) (s3, h3) (s3, h4)
*** Configuring hosts
h1 h2 h3 h4
* Adding Controllers to Data Network
* Starting Data Network
*** Starting controller
c0 c1 c2 c3
*** Starting 3 switches
s1 s2 s3
*** Waiting for switches to connect
s1 s2 s3
*** Starting CLI:
mininet> h1 ping h2
PING 10.0.0.2 (10.0.0.2) 56(84) bytes of data.
64 bytes from 10.0.0.2: icmp_seq=1 ttl=64 time=1.35 ms
64 bytes from 10.0.0.2: icmp_seq=2 ttl=64 time=0.577 ms
64 bytes from 10.0.0.2: icmp_seq=3 ttl=64 time=0.079 ms
64 bytes from 10.0.0.2: icmp_seq=4 ttl=64 time=0.062 ms
64 bytes from 10.0.0.2: icmp_seq=5 ttl=64 time=0.061 ms
^C
--- 10.0.0.2 ping statistics ---
5 packets transmitted, 5 received, 0% packet loss, time 4074ms
rtt min/avg/max/mdev = 0.061/0.426/1.352/0.503 ms

```
