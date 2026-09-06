# Parquet

Parquet is an event-driven trading orchestrator designed to combine **ChatGPT market analysis** with a **deterministic local risk/execution layer**.

> Current status: **V0.2 foundation / SHADOW only. No real broker order is sent by this code.**

## Architecture

```text
ChatGPT Plus / Work
        ^  |
        |  v
 private runtime channel
        ^  |
        |  v
      Parquet LXC
  +-------------------+
  | scheduler         |
  | watch engine      |
  | risk engine       |
  | persistence       |
  | execution adapter |
  +-------------------+
        |
   eToro (later)
```

The design deliberately separates responsibilities:

- ChatGPT proposes market context, deterministic watch conditions, trade proposals and an optional `next_review`.
- Parquet validates all machine-readable responses.
- Market-opening reviews are scheduled locally in each exchange's timezone, so DST differences are handled automatically.
- The Watch Engine evaluates price/candle triggers locally without calling ChatGPT continuously.
- Dynamic reviews and watch state persist in SQLite across restarts.
- The deterministic risk engine can reject proposals independently of ChatGPT.
- Broker execution is an adapter. V0.2 ships only a `ShadowExecutor`.
- Stop-loss is mandatory by default.
- Credentials never live in Git.

## Reproducible host contract

Host-provided files are kept outside the repository:

```text
/etc/parquet/private_key.pem
/etc/parquet/public_key.pem
/etc/parquet/github_token       # only required when private bridge is enabled
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

See [`docs/lxc-runbook.md`](docs/lxc-runbook.md) for deployment acceptance criteria.

## Structural reviews

Openings are defined in the timezone of the market rather than hard-coded in Madrid time:

```yaml
schedule:
  structural_reviews:
    - name: asia_open
      timezone: Asia/Tokyo
      hour: 9
      minute: 0
      offset_minutes: 1
    - name: europe_open
      timezone: Europe/Berlin
      hour: 9
      minute: 0
      offset_minutes: 1
    - name: wall_street_open
      timezone: America/New_York
      hour: 9
      minute: 30
      offset_minutes: 1
```

This deliberately handles the weeks in which US and European daylight-saving transitions do not coincide.

## ChatGPT contract

ChatGPT -> Parquet analyses are identified by:

```text
[PARQUET:ANALYSIS]
```

Parquet -> ChatGPT review requests use:

```text
[PARQUET:REVIEW_REQUEST]
```

The full schema and prompt guidance live in [`docs/chatgpt-analysis-contract.md`](docs/chatgpt-analysis-contract.md).

**Security:** this repository is currently suitable for source code, but runtime trading messages must use a private channel. The GitHub bridge is disabled by default. See [`SECURITY.md`](SECURITY.md).

## Modes

- `shadow`: records what would have happened; no broker operation.
- `demo`: reserved for eToro demo adapter.
- `real`: reserved for Agent Portfolio production adapter.

V0.2 intentionally implements **shadow only**.

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

1. No order without a valid stop-loss.
2. ChatGPT never controls final position sizing or portfolio limits.
3. Expired or stale signals are rejected.
4. Duplicate/unknown broker outcomes must be reconciled before retrying.
5. Loss limits can disable new trading independently of the LLM.
6. Existing broker-side protection must continue working if ChatGPT is unavailable.
7. Real execution remains disabled until shadow and demo acceptance criteria are met.

## Roadmap

- [x] Typed ChatGPT message schema
- [x] GitHub bridge abstraction
- [x] Dynamic review queue
- [x] Structural market-opening scheduler with timezone/DST handling
- [x] SQLite state/audit trail
- [x] Persistent review/watch state
- [x] Deterministic Watch Engine
- [x] Deterministic risk foundation
- [x] Shadow execution adapter
- [x] Health/status API
- [x] Idempotent LXC installer
- [x] CI: Ruff + strict mypy + pytest
- [ ] Validate a private ChatGPT event round-trip
- [ ] eToro market data adapter
- [ ] Wire live market observations into Watch Engine
- [ ] eToro demo execution + reconciliation
- [ ] Position manager
- [ ] Portfolio-level sizing/correlation rules
- [ ] Agent Portfolio real adapter
