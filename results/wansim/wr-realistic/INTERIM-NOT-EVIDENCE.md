# INTERIM — not WR evidence

The CSV files beside this marker are a pre-final WR run retained only for forensic provenance.
They were rejected before commit because they predate the final temporal 100 ms FIFO jitter model
at revision `8b3d710`. Their `SHA256SUMS` file proves only the byte identity of those rejected
artifacts.

The final-model run on 2026-07-17/18 failed closed after 4:38:21 at the digitalocean-like
west-origin, 70% offered-load, jitter-on, carousel/hybrid-drop straggler cell with `K=8192` and
seed 15. The original all-or-nothing harness wrote no rows from that attempt.

Do not quote these CSVs as performance evidence. See `plans/wansim-wr-report.md` and the subsequent
WR triage report for status.
