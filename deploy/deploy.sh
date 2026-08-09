#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LCE_DIR="$SCRIPT_DIR/../../lce"
FRONTEND_DIR="$SCRIPT_DIR/../../acemcp-relay-frontend"
RELAY_DIR="$SCRIPT_DIR/.."

# 可选锁定部署版本：DEPLOY_REF_LCE / DEPLOY_REF_RELAY / DEPLOY_REF_FRONTEND
# 指定 tag 或 commit 时按该版本部署；不指定则跟随远端分支（开发期默认）。
update_repo() {
  local dir="$1" ref="$2"
  git -C "$dir" fetch --tags
  if [ -n "$ref" ]; then
    git -C "$dir" checkout --detach "$ref"
  else
    git -C "$dir" pull
  fi
}

echo "=== Updating code ==="
update_repo "$LCE_DIR" "${DEPLOY_REF_LCE:-}"
update_repo "$RELAY_DIR" "${DEPLOY_REF_RELAY:-}"
update_repo "$FRONTEND_DIR" "${DEPLOY_REF_FRONTEND:-}"

# cloud 客户端不在服务器上构建：它经 lce 仓库的 publish-cloud workflow
# 发布到 npm（@anmezing/lce-cloud），用户侧 npx 直接获取。

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
docker compose up -d --build --no-deps relay frontend

echo "=== Done ==="
docker compose ps
