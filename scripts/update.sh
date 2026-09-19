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
ALLOW_NON_MAIN=0

if [[ "${1:-}" == "--allow-non-main" ]]; then
  ALLOW_NON_MAIN=1
  shift
fi

if [[ $# -ne 0 ]]; then
  echo "Usage: ./scripts/update.sh [--allow-non-main]" >&2
  exit 2
fi

if systemctl is-active --quiet parquet-strategy.service; then
  STRATEGY_WAS_ACTIVE=1
fi

cd "$ROOT_DIR"

if [[ -f "$SSH_CONFIG" && -f "$DEPLOY_KEY" ]]; then
  export GIT_SSH_COMMAND="ssh -F $SSH_CONFIG"
fi

CURRENT_BRANCH="$(git branch --show-current)"
if [[ -z "$CURRENT_BRANCH" ]]; then
  echo "ERROR: refusing to update from detached HEAD." >&2
  echo "Switch to main first: git switch main" >&2
  exit 3
fi

if [[ "$CURRENT_BRANCH" != "main" && "$ALLOW_NON_MAIN" -ne 1 ]]; then
  echo "ERROR: refusing to update non-main branch '$CURRENT_BRANCH'." >&2
  echo "Normal deployment: git switch main && ./scripts/update.sh" >&2
  echo "Intentional branch test: ./scripts/update.sh --allow-non-main" >&2
  exit 4
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
