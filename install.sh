#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo ./install.sh" >&2
  exit 1
fi

ROOT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ETC_DIR=/etc/parquet
OPT_DIR=/opt/parquet
STATE_DIR=/var/lib/parquet
SERVICE=/etc/systemd/system/parquet.service
GITHUB_DEPLOY_KEY="$ETC_DIR/github_deploy_key"
GITHUB_DEPLOY_PUB="$ETC_DIR/github_deploy_key.pub"
GITHUB_KNOWN_HOSTS="$ETC_DIR/github_known_hosts"
GITHUB_SSH_CONFIG="$ETC_DIR/ssh_config"

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y \
  python3 python3-venv python3-pip \
  ca-certificates git curl openssh-client tzdata

if ! id parquet >/dev/null 2>&1; then
  useradd --system --home "$STATE_DIR" --shell /usr/sbin/nologin parquet
fi

install -d -m 0750 -o root -g parquet "$ETC_DIR"
install -d -m 0750 -o parquet -g parquet "$STATE_DIR"
install -d -m 0755 -o root -g root "$OPT_DIR"

# Pin GitHub's published ED25519 host key from the repository. This file is public
# material, not a secret. It prevents unattended updates from accepting an unknown host.
install -m 0644 -o root -g root \
  "$ROOT_DIR/deploy/github_known_hosts" "$GITHUB_KNOWN_HOSTS"

cat > "$GITHUB_SSH_CONFIG" <<EOF
Host github.com
  HostName github.com
  User git
  IdentityFile $GITHUB_DEPLOY_KEY
  IdentitiesOnly yes
  UserKnownHostsFile $GITHUB_KNOWN_HOSTS
  StrictHostKeyChecking yes
EOF
chown root:root "$GITHUB_SSH_CONFIG"
chmod 0644 "$GITHUB_SSH_CONFIG"

if [[ -f "$GITHUB_DEPLOY_KEY" ]]; then
  chown root:root "$GITHUB_DEPLOY_KEY"
  chmod 0600 "$GITHUB_DEPLOY_KEY"
  if ! ssh-keygen -y -f "$GITHUB_DEPLOY_KEY" >/dev/null 2>&1; then
    echo "ERROR: $GITHUB_DEPLOY_KEY is not a readable SSH private key." >&2
    exit 3
  fi

  if [[ -f "$GITHUB_DEPLOY_PUB" ]]; then
    chown root:root "$GITHUB_DEPLOY_PUB"
    chmod 0644 "$GITHUB_DEPLOY_PUB"
  fi

  if git -C "$ROOT_DIR" rev-parse --is-inside-work-tree >/dev/null 2>&1; then
    git -C "$ROOT_DIR" remote set-url origin git@github.com:Moncloa/Parquet.git
  fi
else
  echo "WARNING: $GITHUB_DEPLOY_KEY is missing. Private-repository updates will not work."
fi

if [[ ! -f "$ETC_DIR/parquet.yaml" ]]; then
  install -m 0640 -o root -g parquet \
    "$ROOT_DIR/config/parquet.example.yaml" "$ETC_DIR/parquet.yaml"
fi

if [[ ! -f "$ETC_DIR/parquet.env" ]]; then
  cat > "$ETC_DIR/parquet.env" <<'ENV'
PARQUET_CONFIG=/etc/parquet/parquet.yaml
PARQUET_PRIVATE_KEY=/etc/parquet/private_key.pem
PARQUET_PUBLIC_KEY=/etc/parquet/public_key.pem
PARQUET_GITHUB_TOKEN_FILE=/etc/parquet/github_token
ENV
  chown root:parquet "$ETC_DIR/parquet.env"
  chmod 0640 "$ETC_DIR/parquet.env"
fi

for key in private_key.pem public_key.pem; do
  if [[ -f "$ETC_DIR/$key" ]]; then
    chown root:parquet "$ETC_DIR/$key"
    [[ "$key" == "private_key.pem" ]] && chmod 0640 "$ETC_DIR/$key" || chmod 0644 "$ETC_DIR/$key"
  else
    echo "WARNING: $ETC_DIR/$key is missing (required before broker integration is enabled)."
  fi
done

if [[ -f "$ETC_DIR/github_token" ]]; then
  chown root:parquet "$ETC_DIR/github_token"
  chmod 0640 "$ETC_DIR/github_token"
fi

python3 -m venv "$OPT_DIR/venv"
"$OPT_DIR/venv/bin/pip" install --upgrade pip
"$OPT_DIR/venv/bin/pip" install "$ROOT_DIR"

install -m 0644 "$ROOT_DIR/deploy/parquet.service" "$SERVICE"
systemctl daemon-reload
systemctl enable parquet.service
systemctl restart parquet.service

sleep 1
if systemctl is-active --quiet parquet.service; then
  echo "Parquet installed and running."
  echo "Health: http://127.0.0.1:8787/health"
else
  echo "Parquet failed to start. Inspect: journalctl -u parquet -n 100 --no-pager" >&2
  exit 2
fi
