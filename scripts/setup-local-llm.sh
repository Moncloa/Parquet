#!/usr/bin/env bash
set -euo pipefail

if [[ ${EUID} -ne 0 ]]; then
  echo "Run as root: sudo bash ./scripts/setup-local-llm.sh" >&2
  exit 1
fi

OLLAMA_VERSION="${OLLAMA_VERSION:-0.34.0}"
OLLAMA_SHA256="${OLLAMA_SHA256:-cf95886728959aa09910bb34de5cca1cc5a8f68003b5597197d3f2c2d57c0804}"
MODEL="${PARQUET_LOCAL_LLM_MODEL:-hf.co/mradermacher/ODA-Fin-SFT-8B-GGUF:Q5_K_M}"
CONTEXT_LENGTH="${PARQUET_LOCAL_LLM_CONTEXT_LENGTH:-8192}"
CPU_QUOTA="${PARQUET_LOCAL_LLM_CPU_QUOTA:-500%}"
MEMORY_HIGH="${PARQUET_LOCAL_LLM_MEMORY_HIGH:-9G}"
MEMORY_MAX="${PARQUET_LOCAL_LLM_MEMORY_MAX:-11G}"
OLLAMA_URL="http://127.0.0.1:11434"

if [[ "$(uname -m)" != "x86_64" ]]; then
  echo "ERROR: this installer is pinned for Linux x86_64." >&2
  exit 2
fi

export DEBIAN_FRONTEND=noninteractive
apt-get update
apt-get install -y ca-certificates curl jq zstd

if ! id ollama >/dev/null 2>&1; then
  useradd --system --user-group --create-home --home-dir /usr/share/ollama --shell /usr/sbin/nologin ollama
fi

archive="$(mktemp --suffix=.tar.zst)"
trap 'rm -f "$archive"' EXIT
url="https://github.com/ollama/ollama/releases/download/v${OLLAMA_VERSION}/ollama-linux-amd64.tar.zst"

echo "Downloading Ollama v${OLLAMA_VERSION}..."
curl --proto '=https' --tlsv1.2 -fL "$url" -o "$archive"
echo "${OLLAMA_SHA256}  ${archive}" | sha256sum -c -

rm -rf /usr/lib/ollama
tar --zstd -xf "$archive" -C /usr

cat > /etc/systemd/system/ollama.service <<EOF
[Unit]
Description=Ollama local inference service for Parquet
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
User=ollama
Group=ollama
ExecStart=/usr/bin/ollama serve
Restart=always
RestartSec=3
Environment="HOME=/usr/share/ollama"
Environment="OLLAMA_HOST=127.0.0.1:11434"
Environment="OLLAMA_CONTEXT_LENGTH=${CONTEXT_LENGTH}"
Environment="OLLAMA_NUM_PARALLEL=1"
Environment="OLLAMA_MAX_LOADED_MODELS=1"
Environment="OLLAMA_KEEP_ALIVE=15m"
CPUQuota=${CPU_QUOTA}
MemoryHigh=${MEMORY_HIGH}
MemoryMax=${MEMORY_MAX}
OOMScoreAdjust=250
NoNewPrivileges=true
PrivateTmp=true
ProtectHome=true
ProtectSystem=strict
ReadWritePaths=/usr/share/ollama

[Install]
WantedBy=multi-user.target
EOF

systemctl daemon-reload
systemctl enable --now ollama.service
systemctl restart ollama.service

for _ in $(seq 1 60); do
  if curl -fsS "$OLLAMA_URL/api/version" >/dev/null 2>&1; then
    break
  fi
  sleep 1
done

if ! curl -fsS "$OLLAMA_URL/api/version" >/dev/null 2>&1; then
  echo "ERROR: Ollama did not become ready." >&2
  journalctl -u ollama.service -n 80 --no-pager >&2 || true
  exit 3
fi

echo "Pulling model: $MODEL"
ollama pull "$MODEL"

payload="$(jq -cn --arg model "$MODEL" '{model:$model}')"
curl -fsS -H 'content-type: application/json' -d "$payload" "$OLLAMA_URL/api/show" >/dev/null

echo
echo "Local LLM ready."
echo "  endpoint: $OLLAMA_URL"
echo "  model:    $MODEL"
echo "  context:  $CONTEXT_LENGTH"
echo "  CPU:      $CPU_QUOTA"
echo "  memory:   high=$MEMORY_HIGH max=$MEMORY_MAX"
echo
echo "Checks:"
echo "  systemctl status ollama.service --no-pager"
echo "  ollama list"
echo "  ollama ps"
echo "  journalctl -u ollama.service -n 100 --no-pager"
echo
echo "Alternative model example:"
echo "  PARQUET_LOCAL_LLM_MODEL=qwen3.5:9b bash ./scripts/setup-local-llm.sh"
echo
echo "Ollama remains bound to loopback and is not connected to broker execution by this script."
