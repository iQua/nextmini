## Description
This is a replica of simple example in Nextmini with Mininet. It has three nodes connected through a single switch. An `iperf` test is done to test the bandwith from node 1 to node 3. 

*Note :This test is done with [Mininet](https://mininet.org/download/) installed on Ubuntu 22.04.*

## Instruction
First test if Mininet is installed by the following commands:
```bash
mn --version
```

Then, run the python script to initiate the `iperf` test:
```bash
sudo mn simple.py
```