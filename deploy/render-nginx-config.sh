#!/usr/bin/env bash
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
TEMPLATE_PATH="${NGINX_TEMPLATE_PATH:-$SCRIPT_DIR/nginx.conf.template}"
OUTPUT_PATH="${1:-${NGINX_OUTPUT_PATH:-$SCRIPT_DIR/nginx.conf.rendered}}"
ENV_FILE="${NGINX_ENV_FILE:-$SCRIPT_DIR/nginx.env}"
PYTHON_BIN="${PYTHON_BIN:-python3}"

if [[ ! -f "$ENV_FILE" ]]; then
  echo "ERROR: Nginx deployment environment file not found: $ENV_FILE" >&2
  echo "Copy $SCRIPT_DIR/nginx.env.example to $ENV_FILE and edit it for the target host." >&2
  exit 1
fi

# nginx.env is a dedicated operator-owned shell environment file. Do not point
# this at the application .env when it contains multiline secrets.
set -a
# shellcheck disable=SC1090
source "$ENV_FILE"
set +a

required_vars=(
  PUBLIC_SERVER_NAMES
  PRIMARY_PUBLIC_HOST
  TLS_CERTIFICATE_PATH
  TLS_CERTIFICATE_KEY_PATH
  NGINX_LISTEN_BACKLOG
  NGINX_CLIENT_MAX_BODY_SIZE
  NGINX_MCP_CLIENT_MAX_BODY_SIZE
  NGINX_MCP_UPSTREAM
  NGINX_FRONTEND_UPSTREAM
  NGINX_MCP_PROXY_TIMEOUT
  NGINX_FRONTEND_PROXY_TIMEOUT
)
for name in "${required_vars[@]}"; do
  if [[ -z "${!name:-}" ]]; then
    echo "ERROR: $name must be set in $ENV_FILE" >&2
    exit 1
  fi
done

if [[ ! "$NGINX_LISTEN_BACKLOG" =~ ^[1-9][0-9]*$ ]]; then
  echo "ERROR: NGINX_LISTEN_BACKLOG must be a positive integer" >&2
  exit 1
fi
for name in NGINX_CLIENT_MAX_BODY_SIZE NGINX_MCP_CLIENT_MAX_BODY_SIZE NGINX_MCP_PROXY_TIMEOUT NGINX_FRONTEND_PROXY_TIMEOUT; do
  if [[ ! "${!name}" =~ ^[1-9][0-9]*[kKmMgGsSmMhHdD]?$ ]]; then
    echo "ERROR: $name must be an Nginx size/duration literal such as 32m or 360s" >&2
    exit 1
  fi
done
for name in NGINX_MCP_UPSTREAM NGINX_FRONTEND_UPSTREAM; do
  if [[ ! "${!name}" =~ ^https?:// ]]; then
    echo "ERROR: $name must be an http(s) URL" >&2
    exit 1
  fi
done

"$PYTHON_BIN" - "$TEMPLATE_PATH" "$OUTPUT_PATH" "${required_vars[@]}" <<'PY'
import os
from pathlib import Path
import re
import sys

template_path = Path(sys.argv[1])
output_path = Path(sys.argv[2])
names = sys.argv[3:]
text = template_path.read_text(encoding="utf-8-sig")
for name in names:
    text = text.replace("${" + name + "}", os.environ[name])
unresolved = sorted(set(re.findall(r"\$\{([A-Z][A-Z0-9_]*)\}", text)))
if unresolved:
    raise SystemExit("unresolved deployment variables: " + ", ".join(unresolved))
output_path.parent.mkdir(parents=True, exist_ok=True)
output_path.write_text(text, encoding="utf-8")
PY

chmod 0644 "$OUTPUT_PATH"
echo "Rendered Nginx configuration: $OUTPUT_PATH"
