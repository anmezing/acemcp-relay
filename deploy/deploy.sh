#!/bin/bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LCE_DIR="$SCRIPT_DIR/../../lce"
FRONTEND_DIR="$SCRIPT_DIR/../../acemcp-relay-frontend"
RELAY_DIR="$SCRIPT_DIR/.."

# Production Compose must use the deployment-scoped env file. Do not silently
# fall back to the relay repository's local-development .env: that can point
# at a developer machine or validation VM instead of production services.
DEPLOY_ENV_FILE="${DEPLOY_ENV_FILE:-$SCRIPT_DIR/.env}"
if [[ "$DEPLOY_ENV_FILE" != /* ]]; then
  echo "ERROR: DEPLOY_ENV_FILE must be an absolute path when supplied: $DEPLOY_ENV_FILE" >&2
  exit 1
fi
if [[ ! -f "$DEPLOY_ENV_FILE" ]]; then
  echo "ERROR: production env file not found: $DEPLOY_ENV_FILE" >&2
  echo "Create it with: cp deploy/.env.example deploy/.env" >&2
  exit 1
fi

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

verify_compose_services_stable() {
  local settle_seconds="${DEPLOY_STABILITY_WAIT_SECONDS:-15}"
  if [[ ! "$settle_seconds" =~ ^[0-9]+$ ]]; then
    echo "ERROR: DEPLOY_STABILITY_WAIT_SECONDS must be a non-negative integer" >&2
    return 1
  fi

  local service container_id current_id state health before after
  local -A container_ids=()
  local -A restart_counts=()

  for service in "$@"; do
    container_id="$(docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" ps -q "$service")"
    if [ -z "$container_id" ]; then
      echo "ERROR: expected deployment service has no container: $service" >&2
      return 1
    fi
    container_ids["$service"]="$container_id"
    restart_counts["$service"]="$(docker inspect --format '{{.RestartCount}}' "$container_id")"
  done

  echo "=== Verifying container stability for ${settle_seconds}s ==="
  sleep "$settle_seconds"

  for service in "$@"; do
    container_id="${container_ids[$service]}"
    current_id="$(docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" ps -q "$service")"
    state="$(docker inspect --format '{{.State.Status}}' "$container_id" 2>/dev/null || true)"
    health="$(docker inspect --format '{{if .State.Health}}{{.State.Health.Status}}{{else}}none{{end}}' "$container_id" 2>/dev/null || true)"
    before="${restart_counts[$service]}"
    after="$(docker inspect --format '{{.RestartCount}}' "$container_id" 2>/dev/null || echo -1)"

    if [ "$current_id" != "$container_id" ] || [ "$state" != "running" ] || \
       { [ "$health" != "none" ] && [ "$health" != "healthy" ]; } || \
       [ "$after" -gt "$before" ]; then
      echo "ERROR: service failed post-deploy stability check: $service" >&2
      echo "       state=$state health=$health restarts=$before->$after" >&2
      docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" ps "$service" >&2 || true
      docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" logs --no-color --tail=100 "$service" >&2 || true
      return 1
    fi
  done
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

echo "=== Applying host capacity settings ==="
"$SCRIPT_DIR/tune-host.sh"

# cloud 客户端不在服务器上构建：它经 lce 仓库的 publish-cloud workflow
# 发布到 npm（@anmezing/lce-cloud），用户侧 npx 直接获取。

echo "=== Rebuilding Docker containers ==="
cd "$SCRIPT_DIR"
# Neo4j and its projector are production services. The GDS algorithm worker is
# intentionally a separate opt-in Compose profile; enabling it requires a
# validated GDS capability and an explicit operator action.
compose_env_args=(--env-file "$DEPLOY_ENV_FILE")
compose_profile_args=()
graph_algorithm_services=()
case "${DEPLOY_GRAPH_ALGORITHMS:-false}" in
  false)
    # If a previous rollout enabled the profile, make the normal rollout
    # converge back to the documented default instead of leaving the old
    # worker container running.
    echo "=== Ensuring graph algorithm worker is disabled ==="
    docker compose "${compose_env_args[@]}" --profile graph-algorithms rm -sf neo4j-algorithm-worker || true
    ;;
  true)
    compose_profile_args+=(--profile graph-algorithms)
    graph_algorithm_services+=(neo4j-algorithm-worker)
    ;;
  *)
    echo "ERROR: DEPLOY_GRAPH_ALGORITHMS must be true or false" >&2
    exit 1
    ;;
esac

deployment_services=(neo4j lce neo4j-projector relay frontend "${graph_algorithm_services[@]}")
docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" up -d --build --wait --wait-timeout "${DEPLOY_WAIT_TIMEOUT_SECONDS:-180}" \
  "${deployment_services[@]}"

verify_compose_services_stable "${deployment_services[@]}"
prune_docker_resources

echo "=== Done ==="
docker compose "${compose_env_args[@]}" "${compose_profile_args[@]}" ps
