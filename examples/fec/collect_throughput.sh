#!/usr/bin/env bash
set -euo pipefail

if [[ $# -ne 1 ]]; then
  echo "usage: $0 RUN_DIR" >&2
  exit 1
fi

RUN_DIR="$(cd "$1" && pwd)"
LOGS_DIR="$RUN_DIR/logs"
RUN_ID="$(basename "$RUN_DIR")"
CASE_NAME="$(printf '%s\n' "$RUN_ID" | sed -E 's/^[0-9]{8}-[0-9]{6}-(.*)-[0-9]{6}$/\1/')"
RUN_TSV="$RUN_DIR/throughput.tsv"
HISTORY_TSV="$(dirname "$RUN_DIR")/throughput-history.tsv"

strip_ansi() {
  sed -E $'s/\x1B\\[[0-9;]*[[:alpha:]]//g'
}

write_header() {
  printf 'run_id\tcase_name\tnode_id\tpayload_bytes\treceiver_mbps\tpayload_phase_ms\n'
}

emit_row() {
  local node_id="$1" payload_bytes="$2" receiver_mbps="$3" payload_phase_ms="$4"
  printf '%s\t%s\t%s\t%s\t%s\t%s\n' \
    "$RUN_ID" "$CASE_NAME" "$node_id" "$payload_bytes" "$receiver_mbps" "$payload_phase_ms"
}

extract_receiver_row() {
  local node_id="$1" log_path="$2" line payload_bytes receiver_mbps payload_phase_ms
  line="$(strip_ansi < "$log_path" | grep 'Lossless receiver payload-phase throughput' | tail -n 1 || true)"
  [[ -n "$line" ]] || return 0
  payload_bytes="$(printf '%s\n' "$line" | sed -nE 's/.*total_bytes=([0-9]+).*/\1/p')"
  receiver_mbps="$(printf '%s\n' "$line" | sed -nE 's/.*receiver_mbps=([0-9]+(\.[0-9]+)?).*/\1/p')"
  payload_phase_ms="$(printf '%s\n' "$line" | sed -nE 's/.*payload_phase_ms=([0-9]+).*/\1/p')"
  [[ -n "$payload_bytes" ]] || return 0
  [[ -n "$receiver_mbps" ]] || return 0
  emit_row "$node_id" "$payload_bytes" "$receiver_mbps" "$payload_phase_ms"
}

tmp_rows="$(mktemp)"
trap 'rm -f "$tmp_rows"' EXIT

write_header >"$RUN_TSV"
for log_path in "$LOGS_DIR"/node-*.log; do
  [[ -f "$log_path" ]] || continue
  node_id="${log_path##*/node-}"
  node_id="${node_id%.log}"
  extract_receiver_row "$node_id" "$log_path" >>"$tmp_rows"
done

sort -u "$tmp_rows" >>"$RUN_TSV"

if [[ ! -f "$HISTORY_TSV" ]]; then
  write_header >"$HISTORY_TSV"
fi

while IFS= read -r line; do
  grep -Fqx "$line" "$HISTORY_TSV" || printf '%s\n' "$line" >>"$HISTORY_TSV"
done < <(tail -n +2 "$RUN_TSV")

printf 'throughput summary -> %s\n' "$RUN_TSV"
