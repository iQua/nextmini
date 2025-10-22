#!/bin/bash
set -e

# Nextmini Fly.io Cleanup Script
# Removes all Nextmini-related Fly.io resources

echo "🧹 Nextmini Fly.io Cleanup"
echo "=========================="
echo ""

# Check if flyctl is installed
if ! command -v flyctl &> /dev/null; then
    echo "❌ Error: flyctl is not installed"
    exit 1
fi

echo "Finding all Nextmini apps..."
APPS=$(flyctl apps list 2>/dev/null | grep -E "nextmini-" | awk '{print $1}' || true)

if [ -z "$APPS" ]; then
    echo "✅ No Nextmini apps found"
    exit 0
fi

echo ""
echo "Found the following Nextmini apps:"
echo "$APPS"
echo ""

read -p "⚠️  Delete ALL these apps? This cannot be undone! (yes/no): " -r
if [[ ! $REPLY =~ ^[Yy][Ee][Ss]$ ]]; then
    echo "Cleanup cancelled"
    exit 0
fi

echo ""
echo "Deleting apps..."
while IFS= read -r app; do
    if [ -n "$app" ]; then
        echo "  Deleting $app..."
        flyctl apps destroy "$app" -y || echo "    Warning: Failed to delete $app"
    fi
done <<< "$APPS"

echo ""
echo "✅ Cleanup complete!"
echo ""
echo "Note: This script only removes apps. If you want to remove volumes or other resources:"
echo "  - Check volumes: flyctl volumes list"
echo "  - Check secrets: flyctl secrets list -a <app-name>"
