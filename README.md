# Parquet

Parquet is an event-driven trading orchestrator designed to combine **ChatGPT market analysis** with a **deterministic local risk/execution layer**.

> Current status: **V0.1 foundation / SHADOW only. No real broker order is sent by this code.**

## Architecture

```text
ChatGPT Plus / Work
        ^  |
        |  v
 GitHub runtime PR
        ^  |
        |  v
      Parquet LXC
  +-------------------+
  | scheduler         |
  | market/watch      |
  | risk engine       |
  | persistence       |
  | execution adapter |
  +-------------------+
        |
   eToro (later)
```

The design deliberately separates responsibilities:

- ChatGPT proposes market context, watch conditions, trade proposals and an optional `next_review`.
- Parquet validates all machine-readable responses.
- The deterministic risk engine can reject proposals independently of ChatGPT.
- Broker execution is an adapter. V0.1 ships only a `ShadowExecutor`.
- Stop-loss is mandatory by default.
- Credentials never live in Git.

## Reproducible host contract

Host-provided files are kept outside the repository:

```text
/etc/parquet/private_key.pem
/etc/parquet/public_key.pem
/etc/parquet/github_token       # only required when GitHub bridge is enabled
```

The installer **never creates or overwrites the key pair**.

Everything else is reproducible from the repository.

## Install on a fresh Debian LXC

```bash
git clone https://github.com/Moncloa/Parquet.git
cd Parquet
sudo mkdir -p /etc/parquet
sudo cp /path/to/private_key.pem /etc/parquet/private_key.pem
sudo cp /path/to/public_key.pem /etc/parquet/public_key.pem
sudo ./install.sh
```

Then:

```bash
systemctl status parquet
curl http://127.0.0.1:8787/health
curl http://127.0.0.1:8787/status
```

Update later with:

```bash
./scripts/update.sh
```

## Configuration

First install creates `/etc/parquet/parquet.yaml` from `config/parquet.example.yaml` and preserves it on subsequent installs.

GitHub bridge is disabled by default:

```yaml
github:
  enabled: false
  repository: Moncloa/Parquet
  runtime_pr: 1
  token_file: /etc/parquet/github_token
```

Enable it only after creating the runtime PR and placing a GitHub token with permission to read/write its comments.

## ChatGPT message contract

ChatGPT -> Parquet comments are identified by:

```text
[PARQUET:ANALYSIS]
```

followed by a JSON object matching `MarketAnalysis`.

Parquet -> ChatGPT review requests use:

```text
[PARQUET:REVIEW_REQUEST]
```

The GitHub/ChatGPT event loop is intentionally isolated behind `GitHubBridge` because this is the most experimental part of the architecture and can later be replaced by Slack or another transport without touching the risk/execution code.

## Modes

- `shadow`: records what would have happened; no broker operation.
- `demo`: reserved for eToro demo adapter.
- `real`: reserved for Agent Portfolio production adapter.

V0.1 intentionally implements **shadow only**.

## Development

```bash
python -m venv .venv
source .venv/bin/activate
pip install -e '.[dev]'
pytest -q
ruff check .
mypy src
```

## Safety invariants

The intended production system must preserve these invariants:

1. No order without a valid stop-loss.
2. ChatGPT never controls final position sizing or portfolio limits.
3. Expired signals are rejected.
4. Duplicate/unknown broker outcomes are reconciled before retrying.
5. Loss limits can disable new trading independently of the LLM.
6. Existing broker-side protection must continue working if ChatGPT/GitHub is unavailable.
7. Real execution remains disabled until shadow and demo acceptance criteria are met.

## Roadmap

- [x] Typed ChatGPT message schema
- [x] GitHub runtime bridge foundation
- [x] Dynamic review queue
- [x] SQLite state/audit trail
- [x] Deterministic risk foundation
- [x] Shadow execution adapter
- [x] Health/status API
- [x] Idempotent LXC installer
- [x] CI
- [ ] Prove GitHub -> ChatGPT -> GitHub event round-trip
- [ ] eToro market data adapter
- [ ] Watch/trigger engine
- [ ] eToro demo execution + reconciliation
- [ ] Position manager
- [ ] Portfolio-level sizing/correlation rules
- [ ] Agent Portfolio real adapter
