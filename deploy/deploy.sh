#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LCE_DIR="${DEPLOY_LCE_DIR:-$SCRIPT_DIR/../../lce}"
FRONTEND_DIR="${DEPLOY_FRONTEND_DIR:-$SCRIPT_DIR/../../acemcp-relay-frontend}"
RELAY_DIR="${DEPLOY_RELAY_DIR:-$SCRIPT_DIR/..}"

# 可选锁定部署版本：DEPLOY_REF_LCE / DEPLOY_REF_RELAY / DEPLOY_REF_FRONTEND
# 指定 tag 或 commit 时按该版本部署；不指定则跟随远端分支（开发期默认）。
update_repo() {
  local dir="$1" ref="$2" branch="$3"
  git -C "$dir" fetch --tags origin
  if [ -n "$ref" ]; then
    git -C "$dir" checkout --detach "$ref"
  else
    if git -C "$dir" show-ref --verify --quiet "refs/heads/$branch"; then
      git -C "$dir" checkout "$branch"
    else
      git -C "$dir" checkout --track -b "$branch" "origin/$branch"
    fi
    git -C "$dir" merge --ff-only "origin/$branch"
  fi
}

verify_contract_snapshots() {
  local canonical="$LCE_DIR/docs/contracts/cloud-protocol.json"
  local snapshot
  for snapshot in \
    "$RELAY_DIR/contracts/cloud-protocol.json" \
    "$FRONTEND_DIR/contracts/cloud-protocol.json"; do
    if [ ! -f "$canonical" ] || [ ! -f "$snapshot" ]; then
      echo "ERROR: required cloud protocol contract is missing: $canonical or $snapshot" >&2
      return 1
    fi
    if ! cmp -s "$canonical" "$snapshot"; then
      echo "ERROR: cloud protocol snapshot is stale: $snapshot" >&2
      echo "Sync it from $canonical and commit all affected repositories before deploying." >&2
      return 1
    fi
  done
}

verify_deployment_configuration() {
  (
    cd "$SCRIPT_DIR"
    docker compose config >/dev/null
  )
}

prune_docker_resources() {
  if [ "${DEPLOY_PRUNE_DOCKER_RESOURCES:-true}" != "true" ]; then
    echo "=== Docker resource cleanup disabled ==="
    return
  fi

  local keep_storage="${DEPLOY_BUILD_CACHE_KEEP_STORAGE:-5GB}"
  local build_cache_age="${DEPLOY_BUILD_CACHE_MAX_AGE:-168h}"
  local unused_image_age="${DEPLOY_UNUSED_IMAGE_MAX_AGE:-168h}"

  if docker buildx prune --help 2>&1 | grep -q -- '--max-used-space'; then
    echo "=== Pruning unused Docker build cache (max used space: $keep_storage) ==="
    docker buildx prune --force --max-used-space "$keep_storage" || \
      echo "WARNING: Docker Buildx cache cleanup failed; deployment remains active" >&2
  else
    echo "=== Pruning unused Docker build cache older than $build_cache_age ==="
    docker builder prune --all --force --filter "until=$build_cache_age" || \
      echo "WARNING: Docker build cache cleanup failed; deployment remains active" >&2
  fi

  echo "=== Pruning unused Docker images older than $unused_image_age ==="
  docker image prune --all --force --filter "until=$unused_image_age" || \
    echo "WARNING: Docker image cleanup failed; deployment remains active" >&2
}

echo "=== Updating code ==="
update_repo "$LCE_DIR" "${DEPLOY_REF_LCE:-}" "${DEPLOY_BRANCH_LCE:-feat/multi-tenant-relay}"
update_repo "$RELAY_DIR" "${DEPLOY_REF_RELAY:-}" "${DEPLOY_BRANCH_RELAY:-main}"
update_repo "$FRONTEND_DIR" "${DEPLOY_REF_FRONTEND:-}" "${DEPLOY_BRANCH_FRONTEND:-main}"

echo "=== Verifying cross-repository contracts ==="
verify_contract_snapshots

echo "=== Verifying deployment configuration ==="
verify_deployment_configuration

echo "=== Applying host capacity settings ==="
"$SCRIPT_DIR/tune-host.sh"

# cloud 客户端不在服务器上构建：它经 lce 仓库的 publish-cloud workflow
# 发布本地客户端包；用户侧启动方式由控制台部署配置生成。

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
docker compose up -d --build --no-deps lce relay frontend

prune_docker_resources

echo "=== Done ==="
docker compose ps
