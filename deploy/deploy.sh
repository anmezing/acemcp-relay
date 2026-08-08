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

echo "=== Building cloud client ==="
cd "$LCE_DIR"
npm install --ignore-scripts --no-audit --no-fund 2>/dev/null
npx --yes esbuild src/cloud/entry.ts --bundle --platform=node --target=node20 --format=cjs --minify --outfile=dist/lce-cloud.cjs
cp dist/lce-cloud.cjs "$FRONTEND_DIR/public/lce-cloud.cjs"
cp src/cloud/boot.js "$FRONTEND_DIR/public/boot.js"
echo "  -> copied boot.js + lce-cloud.cjs to frontend/public/"

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
docker compose up -d --build --no-deps lce relay frontend

echo "=== Done ==="
docker compose ps
