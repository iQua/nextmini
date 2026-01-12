#!/usr/bin/env bash
set -euo pipefail

if [[ "${EUID}" -ne 0 ]]; then
  echo "Run as root (e.g., sudo ${0})." >&2
  exit 1
fi

export DEBIAN_FRONTEND=noninteractive

echo "[bootstrap] OS:" "$(grep -E '^PRETTY_NAME=' /etc/os-release || true)"

echo "[bootstrap] Installing base tools (curl/rsync/iperf3)..."
apt-get update -y >/dev/null
apt-get install -y ca-certificates curl gnupg lsb-release rsync debconf-utils >/dev/null
echo "iperf3 iperf3/start_daemon boolean false" | debconf-set-selections >/dev/null 2>&1 || true
apt-get install -y iperf3 >/dev/null

if ! command -v docker >/dev/null 2>&1; then
  echo "[bootstrap] Installing Docker Engine (get.docker.com)..."
  curl -fsSL https://get.docker.com -o /tmp/get-docker.sh
  sh /tmp/get-docker.sh >/dev/null
  rm -f /tmp/get-docker.sh
else
  echo "[bootstrap] Docker already installed: $(docker --version || true)"
fi

echo "[bootstrap] Starting docker service..."
systemctl enable --now docker >/dev/null 2>&1 || true

if [[ -n "${SUDO_USER:-}" && "${SUDO_USER}" != "root" ]]; then
  echo "[bootstrap] Adding ${SUDO_USER} to docker group (re-login required)..."
  usermod -aG docker "${SUDO_USER}" || true
fi

echo "[bootstrap] Versions:"
docker --version || true
docker compose version || true
iperf3 --version | head -n1 || true
rsync --version | head -n1 || true

cat <<'MSG'
[bootstrap] Reminder for WAN experiments:
  - Open inbound TCP ports: 3000 (controller), 8080/8081 (nodes), and 22 (SSH).
  - Ensure the SSH user can run `docker` without sudo (use root or docker group + re-login).
MSG
