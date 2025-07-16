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
