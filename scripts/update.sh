#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./scripts/update.sh" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
SSH_CONFIG=/etc/parquet/ssh_config
DEPLOY_KEY=/etc/parquet/github_deploy_key

cd "$ROOT_DIR"

if [[ -f "$SSH_CONFIG" && -f "$DEPLOY_KEY" ]]; then
  export GIT_SSH_COMMAND="ssh -F $SSH_CONFIG"
fi

git pull --ff-only
./install.sh
