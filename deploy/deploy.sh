#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LCE_DIR="$SCRIPT_DIR/../../lce"
FRONTEND_DIR="$SCRIPT_DIR/../../acemcp-relay-frontend"
RELAY_DIR="$SCRIPT_DIR/.."

echo "=== Pulling latest code ==="
git -C "$LCE_DIR" pull
git -C "$RELAY_DIR" pull
git -C "$FRONTEND_DIR" pull

echo "=== Building cloud client (lce-cloud.js) ==="
cd "$LCE_DIR"
pnpm install --frozen-lockfile
pnpm build:cloud
cp dist/lce-cloud.js "$FRONTEND_DIR/public/lce-cloud.js"
echo "  -> copied to frontend/public/lce-cloud.js"

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
docker compose up -d --build --no-deps lce relay frontend

echo "=== Done ==="
docker compose ps
