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
BUILD_DIR=$(mktemp -d)
cp -r "$LCE_DIR/src" "$BUILD_DIR/src"
cd "$BUILD_DIR"
npm init -y >/dev/null 2>&1
npm install --ignore-scripts --no-audit --no-fund \
  @modelcontextprotocol/server @modelcontextprotocol/client ignore p-limit zod esbuild
npx esbuild src/cloud/entry.ts --bundle --platform=node --target=node20 --format=cjs --minify --outfile=lce-cloud.cjs
mkdir -p "$LCE_DIR/dist"
cp lce-cloud.cjs "$LCE_DIR/dist/"
cp "$LCE_DIR/src/cloud/boot.js" "$FRONTEND_DIR/public/boot.js"
cp lce-cloud.cjs "$FRONTEND_DIR/public/lce-cloud.cjs"
rm -rf "$BUILD_DIR"
echo "  -> boot.js + lce-cloud.cjs ready"

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
docker compose up -d --build --no-deps relay frontend

echo "=== Done ==="
docker compose ps
