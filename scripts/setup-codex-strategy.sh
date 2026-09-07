#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo bash ./scripts/setup-codex-strategy.sh" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
STRATEGY_GROUP=parquet-strategy
STRATEGY_USER=parquet-strategy
STRATEGY_HOME=/var/lib/parquet-strategy
EXCHANGE_DIR=/var/lib/parquet-exchange
STRATEGY_ETC=/etc/parquet-strategy
STRATEGY_ENV="$STRATEGY_ETC/strategy.env"
STRATEGY_SERVICE=/etc/systemd/system/parquet-strategy.service

if ! getent group "$STRATEGY_GROUP" >/dev/null 2>&1; then
  groupadd --system "$STRATEGY_GROUP"
fi
if ! id "$STRATEGY_USER" >/dev/null 2>&1; then
  useradd --system \
    --gid "$STRATEGY_GROUP" \
    --home "$STRATEGY_HOME" \
    --shell /usr/sbin/nologin \
    "$STRATEGY_USER"
fi
if ! id parquet >/dev/null 2>&1; then
  echo "Parquet service user is missing; run ./install.sh first" >&2
  exit 2
fi

# Only the broker user and isolated strategy user share this group. The strategy
# user is deliberately NOT added to group 'parquet', which protects /etc/parquet.
usermod -a -G "$STRATEGY_GROUP" parquet

install -d -m 0700 -o "$STRATEGY_USER" -g "$STRATEGY_GROUP" "$STRATEGY_HOME"
install -d -m 0700 -o "$STRATEGY_USER" -g "$STRATEGY_GROUP" \
  "$STRATEGY_HOME/codex" "$STRATEGY_HOME/work"
install -d -m 2770 -o root -g "$STRATEGY_GROUP" "$EXCHANGE_DIR"
install -d -m 2770 -o root -g "$STRATEGY_GROUP" \
  "$EXCHANGE_DIR/requests" "$EXCHANGE_DIR/results" "$EXCHANGE_DIR/errors"
install -d -m 0750 -o root -g "$STRATEGY_GROUP" "$STRATEGY_ETC"

if [[ ! -f "$STRATEGY_ENV" ]]; then
  cat > "$STRATEGY_ENV" <<'ENV'
PARQUET_STRATEGY_QUEUE=/var/lib/parquet-exchange
PARQUET_STRATEGY_WORK=/var/lib/parquet-strategy/work
CODEX_HOME=/var/lib/parquet-strategy/codex
PARQUET_CODEX_BINARY=codex
PARQUET_CODEX_REASONING_EFFORT=medium
PARQUET_CODEX_TIMEOUT_SECONDS=240
PARQUET_STRATEGY_POLL_SECONDS=5
PARQUET_CODEX_WEB_SEARCH=true
ENV
fi
chown root:"$STRATEGY_GROUP" "$STRATEGY_ENV"
chmod 0640 "$STRATEGY_ENV"

install -m 0644 "$ROOT_DIR/deploy/parquet-strategy.service" "$STRATEGY_SERVICE"
systemctl daemon-reload

if ! command -v npm >/dev/null 2>&1; then
  export DEBIAN_FRONTEND=noninteractive
  apt-get update
  apt-get install -y nodejs npm ca-certificates
fi

if ! command -v codex >/dev/null 2>&1; then
  npm install -g @openai/codex
fi

CODEX_BIN="$(command -v codex)"
echo "Codex binary: $CODEX_BIN"
echo "Starting ChatGPT device authentication for isolated user parquet-strategy."
echo "Follow the URL/code printed by Codex. No OpenAI API key is required for this path."

runuser -u "$STRATEGY_USER" -- env \
  HOME="$STRATEGY_HOME" \
  CODEX_HOME="$STRATEGY_HOME/codex" \
  PATH="/usr/local/bin:/usr/bin:/bin" \
  "$CODEX_BIN" login --device-auth

echo
runuser -u "$STRATEGY_USER" -- env \
  HOME="$STRATEGY_HOME" \
  CODEX_HOME="$STRATEGY_HOME/codex" \
  PATH="/usr/local/bin:/usr/bin:/bin" \
  "$CODEX_BIN" login status

# Restart the broker service so systemd picks up parquet's new supplementary
# parquet-strategy group membership. Strategy itself remains disabled/off.
systemctl restart parquet.service

echo
echo "Codex authentication is configured for parquet-strategy."
echo "Broker credentials remain under /etc/parquet and are inaccessible to this user."
echo "The strategy service is installed but has NOT been enabled or started."
echo "Next: set strategy.enabled: true in /etc/parquet/parquet.yaml, then run:"
echo "  systemctl enable --now parquet-strategy.service"
echo "  systemctl restart parquet.service"
