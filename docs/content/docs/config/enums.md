---
title: "Config Enums"
description: "Reference for all enum values used in Nextmini controller and dataplane configuration."
---

## Protocol

Transport protocol for inter-node communication.

| Value | Description |
|-------|-------------|
| `tcp` | TCP transport (default). |
| `udp` | UDP transport. |
| `quic` | QUIC transport with configurable congestion control. |

## FlowTransport

Transport for controller-managed flows.

| Value | Description |
|-------|-------------|
| `tcp` | TCP-based flow transport (default). |
| `lossless_unicast` | Lossless unicast with chunking and pacing. |

## SchedulingDiscipline

Scheduler discipline for packet queues.

| Value | Description |
|-------|-------------|
| `fifo` | First-in, first-out (default). |
| `wrr` | Weighted round-robin. |

## DropStrategy

Drop strategy when queues are full.

| Value | Description |
|-------|-------------|
| `taildrop` | Drop newest packets when queue is full (default). |
| `red` | Random Early Detection - probabilistic dropping before queue fills. |

## Feature

Packet processing mode.

| Value | Description |
|-------|-------------|
| `sequential` | Single processor per flow, guarantees packet order (default). |
| `concurrent` | Multiple processors, higher throughput but may reorder packets. |

## OperatingMode

Node operating mode.

| Value | Description |
|-------|-------------|
| `normal` | Standard operation (default). |
| `max` | Maximum throughput mode with connection-on-demand. |

## CongestionControl

QUIC congestion control algorithm.

| Value | Description |
|-------|-------------|
| `bbr` | BBR congestion control (default). |
| `cubic` | CUBIC congestion control. |

## RouteForwardingMode

How routes forward traffic.

| Value | Description |
|-------|-------------|
| `unicast` | Point-to-point forwarding (default). |
| `multicast` | Fan-out to multiple next hops. |
