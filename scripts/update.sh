#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./scripts/update.sh" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_CONFIG=/etc/parquet/ssh_config
DEPLOY_KEY=/etc/parquet/github_deploy_key
STRATEGY_WAS_ACTIVE=0

if systemctl is-active --quiet parquet-strategy.service; then
  STRATEGY_WAS_ACTIVE=1
fi

cd "$ROOT_DIR"

if [[ -f "$SSH_CONFIG" && -f "$DEPLOY_KEY" ]]; then
  export GIT_SSH_COMMAND="ssh -F $SSH_CONFIG"
fi

git pull --ff-only
./install.sh

# install.sh recreates /opt/parquet/venv. If the isolated strategy worker was
# already running, restart it so it executes the newly installed entrypoint too.
# Preserve an intentionally stopped worker by restarting only when it was active
# before this update began.
if [[ "$STRATEGY_WAS_ACTIVE" -eq 1 ]]; then
  systemctl restart parquet-strategy.service
fi
