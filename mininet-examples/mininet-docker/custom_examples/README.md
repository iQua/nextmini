# linear.py

Linear.py is using linear topology in Mininet, which is a chain of switches and hosts.

```bash
h1 ---- s1 ---- s2 ---- s3 ---- s4 ---- h5
              |       |       |
              h2      h3      h4
```

Testing is planned for [1, 4, 7, 10, 13, 16, 19, 21] hop configurations.

Due to the [OVS controller's 16-switch limitation](https://mininet.org/blog/2013/06/03/automating-controller-startup/), testing will be capped at 16 hops maximum.

# router.py

A chain of N routers between two hosts.

   +----+      +----+      +----+           +----+      +----+
   | h1 |------| r1 |------| r2 |--- ..... ---| rN |------| h2 |
   +----+      +----+      +----+           +----+      +----+

Take this [website](https://intronetworks.cs.luc.edu/current/html/mininet.html#ip-routers-in-a-line) as a reference.
