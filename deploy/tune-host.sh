#!/usr/bin/env bash
set -euo pipefail

positive_int() {
  local name="$1" value="$2"
  if [[ ! "$value" =~ ^[1-9][0-9]*$ ]]; then
    echo "ERROR: $name must be a positive integer, got: $value" >&2
    exit 1
  fi
}

SYSCTL_CONFIG_PATH="${DEPLOY_SYSCTL_CONFIG_PATH:-/etc/sysctl.d/99-lce-performance.conf}"
NGINX_MAIN_CONFIG="${DEPLOY_NGINX_MAIN_CONFIG:-/etc/nginx/nginx.conf}"
NGINX_SERVICE="${DEPLOY_NGINX_SERVICE:-nginx}"
POSTGRES_OS_USER="${DEPLOY_POSTGRES_OS_USER:-postgres}"
POSTGRES_SERVICE="${DEPLOY_POSTGRES_SERVICE:-postgresql}"

SOMAXCONN="${DEPLOY_SOMAXCONN:-8192}"
TCP_MAX_SYN_BACKLOG="${DEPLOY_TCP_MAX_SYN_BACKLOG:-8192}"
IP_LOCAL_PORT_MIN="${DEPLOY_IP_LOCAL_PORT_MIN:-10240}"
IP_LOCAL_PORT_MAX="${DEPLOY_IP_LOCAL_PORT_MAX:-65535}"
TCP_FIN_TIMEOUT="${DEPLOY_TCP_FIN_TIMEOUT:-30}"
NF_CONNTRACK_MAX="${DEPLOY_NF_CONNTRACK_MAX:-524288}"
NGINX_WORKER_RLIMIT_NOFILE="${DEPLOY_NGINX_WORKER_RLIMIT_NOFILE:-65535}"
NGINX_WORKER_CONNECTIONS="${DEPLOY_NGINX_WORKER_CONNECTIONS:-8192}"
POSTGRES_MAX_CONNECTIONS="${DEPLOY_POSTGRES_MAX_CONNECTIONS:-160}"
POSTGRES_RESERVED_CONNECTIONS="${DEPLOY_POSTGRES_RESERVED_CONNECTIONS:-5}"

for name in \
  SOMAXCONN TCP_MAX_SYN_BACKLOG IP_LOCAL_PORT_MIN IP_LOCAL_PORT_MAX \
  TCP_FIN_TIMEOUT NF_CONNTRACK_MAX NGINX_WORKER_RLIMIT_NOFILE \
  NGINX_WORKER_CONNECTIONS POSTGRES_MAX_CONNECTIONS POSTGRES_RESERVED_CONNECTIONS; do
  positive_int "$name" "${!name}"
done
if (( IP_LOCAL_PORT_MIN >= IP_LOCAL_PORT_MAX )); then
  echo "ERROR: DEPLOY_IP_LOCAL_PORT_MIN must be smaller than DEPLOY_IP_LOCAL_PORT_MAX" >&2
  exit 1
fi
if (( POSTGRES_RESERVED_CONNECTIONS >= POSTGRES_MAX_CONNECTIONS )); then
  echo "ERROR: DEPLOY_POSTGRES_RESERVED_CONNECTIONS must be smaller than DEPLOY_POSTGRES_MAX_CONNECTIONS" >&2
  exit 1
fi

cat <<EOF | sudo tee "$SYSCTL_CONFIG_PATH" >/dev/null
net.core.somaxconn = $SOMAXCONN
net.ipv4.tcp_max_syn_backlog = $TCP_MAX_SYN_BACKLOG
net.ipv4.ip_local_port_range = $IP_LOCAL_PORT_MIN $IP_LOCAL_PORT_MAX
net.ipv4.tcp_fin_timeout = $TCP_FIN_TIMEOUT
net.netfilter.nf_conntrack_max = $NF_CONNTRACK_MAX
EOF
sudo sysctl --system >/dev/null

if sudo grep -qE '^[[:space:]]*worker_rlimit_nofile' "$NGINX_MAIN_CONFIG"; then
  sudo sed -i -E "s/^[[:space:]]*worker_rlimit_nofile[[:space:]]+[0-9]+;/worker_rlimit_nofile $NGINX_WORKER_RLIMIT_NOFILE;/" "$NGINX_MAIN_CONFIG"
else
  sudo sed -i "/^[[:space:]]*worker_processes/a worker_rlimit_nofile $NGINX_WORKER_RLIMIT_NOFILE;" "$NGINX_MAIN_CONFIG"
fi
sudo sed -i -E "s/worker_connections[[:space:]]+[0-9]+;/worker_connections $NGINX_WORKER_CONNECTIONS;/" "$NGINX_MAIN_CONFIG"

sudo nginx -t
sudo systemctl reload "$NGINX_SERVICE"

current_max_connections="$(sudo -u "$POSTGRES_OS_USER" psql -Atqc 'SHOW max_connections')"
current_reserved_connections="$(sudo -u "$POSTGRES_OS_USER" psql -Atqc 'SHOW superuser_reserved_connections')"
postgres_restart_required=false
if (( current_max_connections < POSTGRES_MAX_CONNECTIONS )); then
  sudo -u "$POSTGRES_OS_USER" psql -v ON_ERROR_STOP=1 -q -c "ALTER SYSTEM SET max_connections = '$POSTGRES_MAX_CONNECTIONS'"
  postgres_restart_required=true
fi
if (( current_reserved_connections < POSTGRES_RESERVED_CONNECTIONS )); then
  sudo -u "$POSTGRES_OS_USER" psql -v ON_ERROR_STOP=1 -q -c "ALTER SYSTEM SET superuser_reserved_connections = '$POSTGRES_RESERVED_CONNECTIONS'"
  postgres_restart_required=true
fi
if [[ "$postgres_restart_required" == true ]]; then
  sudo systemctl restart "$POSTGRES_SERVICE"
fi
