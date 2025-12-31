#!/usr/bin/env bash
set -euo pipefail

SUDO=""
if [[ "${EUID:-$(id -u)}" -ne 0 ]]; then
    SUDO="sudo"
fi

$SUDO bash -c '
# Only delete Nextmini-created veth devices (veth<N>a / veth<N>b) to avoid breaking Docker or
# other host networking that also uses veth pairs.
for dev in $(ip -o link show type veth 2>/dev/null | awk -F": " "{print $2}" | cut -d"@" -f1 | sort -u | grep -E "^veth[0-9]+[ab]$" || true); do
    echo "Deleting $dev"
    ip link del "$dev" 2>/dev/null || true
done

for br in $(ip -o link show type bridge 2>/dev/null | awk -F": " "{print $2}" | cut -d"@" -f1 | grep -E "^isobr" | sort -u); do
    echo "Deleting $br"
    ip link set "$br" down 2>/dev/null || true
    ip link del "$br" 2>/dev/null || true
done
'
echo "The virtual network environment has been successfully cleaned up. You can now run the controller containers again."
