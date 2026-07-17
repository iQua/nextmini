# Stage 3 reservoir autoresearch log

All work in this log evaluates an extension **BEYOND the METTLE paper**. The paper supports
feedback and rate adaptation; it does not specify deterministic reservoir puncturing.

Date: 2026-07-16

## Objective and bounds

- Keep the existing wire and manifest unchanged.
- Use actual finite terminal counts, including the compressed tail.
- Compare sender reserve payload strategies at the cap geometry (`N=65,536`, `T=1,400`) against an
  independent 8 MiB sender budget.
- Sweep `c_wire in {1%, 2%, 4%}` by `c_reserve in {3%, 5%, 7%}` at `N=8,192`, with five BEC rates
  from 0.1% through 2% and two GE traces.
- Use deterministic seeds and exact two-sided 95% Clopper-Pearson intervals.

## H1 — reserve-only retention is affordable and faster than regeneration or spill

Status: kept.

At `c_wire=2%`, `c_reserve=5%`, the exact reserve has 3,277 symbols and 4,587,800 payload bytes.
Retention used 4,745,096 logical bytes including its index and fetched the complete reserve in
5.472 ms. Spill used 106,264 RAM bytes plus a 4,587,800-byte file and fetched in 8.041 ms, but raised
the initial encode/store time from 20.360 ms to 32.799 ms. Direct recomputation used a 1,400-byte
scratch buffer but took 139.887 ms after checking 1,877,884 candidate source/bin relationships.
All strategies produced digest `ba1631c0cde1e383`.

The largest swept reserve (`c_reserve=7%`) retained 4,587 symbols in 6,641,976 logical bytes,
remaining below the independent 8 MiB budget. Reserve-only retention is the prototype choice;
spill is a viable memory-pressure alternative, while per-bin recomputation is rejected on latency.

## H2 — some finite reservoir point will remain reliable across BEC and burst loss

Status: rejected by the 512-trial grid.

The strongest grid point was `c_wire=4%`, `c_reserve=7%`: actual initial overhead 4.0039%, actual
total overhead 11.0107%, and 803,600 sender reserve payload bytes at `N=8,192`, `T=1,400`. It reached
99.61% completion at BEC 2%, but only 90.63% on the short GE trace and 64.45% on the long GE trace.
No lower-overhead point improved the burst result.

## H3 — a 4,096-trial confirmation will overturn the burst-loss no-go

Status: rejected.

At the strongest point, confirmation produced:

| Channel | Completion | Exact 95% interval |
| --- | ---: | ---: |
| BEC 0.1% | 100.000% | 99.910–100.000% |
| BEC 0.5% | 100.000% | 99.910–100.000% |
| BEC 1.0% | 99.976% | 99.864–99.999% |
| BEC 1.5% | 99.927% | 99.786–99.985% |
| BEC 2.0% | 99.731% | 99.520–99.866% |
| GE short, stationary 1.089% | 91.040% | 90.124–91.897% |
| GE long, stationary 1.089% | 64.868% | 63.384–66.331% |

Decision: the mechanics satisfy their prototype invariants and memory bound, but the required
bursty-channel evidence does not justify Stage 3.2 integration.
