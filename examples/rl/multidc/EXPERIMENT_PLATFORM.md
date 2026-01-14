# WAN RL Experiment Platform Summary

> **Experiment Series**: t2 (GPT-2 WAN Multicast Benchmark)

---

## 1. Cluster Overview

| Metric | Value |
|--------|-------|
| **Total Nodes** | 10 |
| **Trainer Nodes** | 1 |
| **Worker Nodes** | 6 |
| **Relay Nodes** | 3 |
| **Geographic Regions** | 8 (across 3 continents) |

### Geographic Distribution

```
                          ┌─────────────────────────────────────────────┐
                          │           Multi-DC WAN Topology             │
                          └─────────────────────────────────────────────┘

  North America                     Europe                          Asia
  ─────────────                     ──────                          ────
  🍁 Victoria, CA (2,3)             🇫🇮 Helsinki, FI (1) [Trainer]   🇸🇬 Singapore (6) [Relay]
  🍁 Toronto, CA (4)                🇩🇪 Frankfurt, DE (5)           🇮🇳 Bengaluru, IN (10)
  🇺🇸 Santa Clara, USA (7) [Relay]  🇳🇱 Amsterdam, NL (8)
                                    🇬🇧 London, UK (9) [Relay]
```

---

## 2. Node Hardware Specifications

| Node ID | Role | Region | CPU | Cores | Memory | OS |
|---------|------|--------|-----|-------|--------|------|
| **1** | Trainer | Helsinki, Finland 🇫🇮 | AMD EPYC-Genoa | 8 | 15 GB | Ubuntu 24.04 |
| **2** | Worker (rank 0) | Victoria, Canada 🍁 | Intel Core Broadwell | 8 | 29 GB | Ubuntu 24.04 |
| **3** | Worker (rank 1) | Victoria, Canada 🍁 | Intel Xeon Skylake | 16 | 176 GB | Ubuntu 24.04 |
| **4** | Worker (rank 2) | Toronto, Canada 🍁 | Intel Core i7-13700K | 24 | 91 GB | Ubuntu 24.04 |
| **5** | Worker (rank 3) | Frankfurt, Germany 🇩🇪 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |
| **6** | Relay | Singapore 🇸🇬 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |
| **7** | Relay | Santa Clara, USA 🇺🇸 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |
| **8** | Worker (rank 4) | Amsterdam, Netherlands 🇳🇱 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |
| **9** | Relay | London, UK 🇬🇧 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |
| **10** | Worker (rank 5) | Bengaluru, India 🇮🇳 | Intel Xeon Platinum 8168 | 4 | 8 GB | Ubuntu 24.04 |

### Hardware Summary

- **CPU Types**: 4 distinct processor families
  - AMD EPYC-Genoa (server-grade, trainer)
  - Intel Core i7-13700K (desktop, high-perf worker)
  - Intel Xeon Skylake/Platinum 8168 (cloud VMs)
  - Intel Core Broadwell (legacy cloud)
- **Total CPU Cores**: 80
- **Total Memory**: ~359 GB
- **OS**: Ubuntu 24.04 LTS (uniform)

---
