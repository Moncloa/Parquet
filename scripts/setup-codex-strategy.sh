#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./scripts/setup-codex-strategy.sh" >&2
  exit 1
fi

if ! id parquet-strategy >/dev/null 2>&1; then
  echo "parquet-strategy user is missing; run ./install.sh first" >&2
  exit 2
fi

if ! command -v npm >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y nodejs npm ca-certificates
fi

if ! command -v codex >/dev/null 2>&1; then
  npm install -g @openai/codex
fi

CODEX_BIN="$(command -v codex)"
CODEX_HOME=/var/lib/parquet-strategy/codex
HOME_DIR=/var/lib/parquet-strategy

install -d -m 0700 -o parquet-strategy -g parquet-strategy "$HOME_DIR" "$CODEX_HOME"
install -d -m 0700 -o parquet-strategy -g parquet-strategy "$HOME_DIR/work"

echo "Codex binary: $CODEX_BIN"
echo "Starting ChatGPT device authentication for isolated user parquet-strategy."
echo "Follow the URL/code printed by Codex. No OpenAI API key is required for this path."

runuser -u parquet-strategy -- env \
  HOME="$HOME_DIR" \
  CODEX_HOME="$CODEX_HOME" \
  PATH="/usr/local/bin:/usr/bin:/bin" \
  "$CODEX_BIN" login --device-auth

echo
runuser -u parquet-strategy -- env \
  HOME="$HOME_DIR" \
  CODEX_HOME="$CODEX_HOME" \
  PATH="/usr/local/bin:/usr/bin:/bin" \
  "$CODEX_BIN" login status

echo
echo "Codex authentication is configured for parquet-strategy."
echo "The strategy service is still not enabled automatically."
