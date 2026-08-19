#!/bin/bash
set -euo pipefail

cat <<'EOF' | sudo tee /etc/sysctl.d/99-lce-performance.conf >/dev/null
net.core.somaxconn = 8192
net.ipv4.tcp_max_syn_backlog = 8192
net.ipv4.ip_local_port_range = 10240 65535
net.ipv4.tcp_fin_timeout = 30
net.netfilter.nf_conntrack_max = 524288
EOF
sudo sysctl --system >/dev/null

nginx_conf=/etc/nginx/nginx.conf
if sudo grep -qE '^[[:space:]]*worker_rlimit_nofile' "$nginx_conf"; then
  sudo sed -i -E 's/^[[:space:]]*worker_rlimit_nofile[[:space:]]+[0-9]+;/worker_rlimit_nofile 65535;/' "$nginx_conf"
else
  sudo sed -i '/^[[:space:]]*worker_processes/a worker_rlimit_nofile 65535;' "$nginx_conf"
fi
sudo sed -i -E 's/worker_connections[[:space:]]+[0-9]+;/worker_connections 8192;/' "$nginx_conf"

sudo nginx -t
sudo systemctl reload nginx

current_max_connections="$(sudo -u postgres psql -Atqc 'SHOW max_connections')"
current_reserved_connections="$(sudo -u postgres psql -Atqc 'SHOW superuser_reserved_connections')"
postgres_restart_required=false
if (( current_max_connections < 160 )); then
  sudo -u postgres psql -v ON_ERROR_STOP=1 -q -c "ALTER SYSTEM SET max_connections = '160'"
  postgres_restart_required=true
fi
if (( current_reserved_connections < 5 )); then
  sudo -u postgres psql -v ON_ERROR_STOP=1 -q -c "ALTER SYSTEM SET superuser_reserved_connections = '5'"
  postgres_restart_required=true
fi
if [[ "$postgres_restart_required" == true ]]; then
  sudo systemctl restart postgresql
fi
