#!/bin/bash
sudo bash -c '
for dev in $(ip -o link show type veth | awk -F": " "{print $2}" | cut -d"@" -f1 | sort -u); do
    echo "Deleting $dev"
    ip link del "$dev" 2>/dev/null || true
done
'
echo "Cleaned up veths successfully"
