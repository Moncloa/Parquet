# Parquet

Parquet is an event-driven trading orchestrator designed to combine **ChatGPT market analysis** with a **deterministic local risk/execution layer**.

> Current status: **V0.3 foundation / SHADOW only. No real broker order is sent by this code.**

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
- Broker execution is an adapter. V0.3 ships only a `ShadowExecutor`.
- Stop-loss is mandatory by default.
- Credentials never live in Git.

## Reproducible host contract

Host-provided credentials are kept outside the repository:

```text
/etc/parquet/github_deploy_key      # read-only clone/pull
/etc/parquet/github_deploy_key.pub  # optional public half
/etc/parquet/private_key.pem        # broker/auth material
/etc/parquet/public_key.pem         # broker/auth material
/etc/parquet/github_token           # only required when private runtime bridge is enabled
```

The installer **never creates or overwrites private credentials**. It installs the pinned GitHub host key and SSH configuration, creates the service environment and preserves host configuration across upgrades.

## Install on a fresh Debian LXC

Because `Moncloa/Parquet` is private, the first clone uses a repository-scoped **read-only GitHub deploy key**. See [`docs/lxc-runbook.md`](docs/lxc-runbook.md) for the complete bootstrap, including host-key fingerprint verification.

Once cloned:

```bash
cd Parquet
sudo ./install.sh
```

Then:

```bash
systemctl status parquet --no-pager
curl -fsS http://127.0.0.1:8787/health
curl -fsS http://127.0.0.1:8787/status
```

Future updates are deliberately simple:

```bash
sudo ./scripts/update.sh
```

The update path uses `/etc/parquet/github_deploy_key` and only accepts fast-forward Git updates.

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

**Security:** the source repository is private, but the runtime bridge remains disabled by default until token permissions and event behavior are validated in SHADOW mode. See [`SECURITY.md`](SECURITY.md).

## Modes

- `shadow`: records what would have happened; no broker operation.
- `demo`: reserved for eToro demo adapter.
- `real`: reserved for Agent Portfolio production adapter.

V0.3 intentionally implements **shadow only**.

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
- [x] Thread-safe SQLite access for API/worker use
- [x] Deterministic Watch Engine
- [x] Deterministic risk foundation
- [x] Shadow execution adapter
- [x] Health/status API
- [x] Idempotent LXC installer
- [x] Private-repository deploy-key update path
- [x] CI: Ruff + strict mypy + pytest
- [ ] Validate a private ChatGPT event round-trip
- [x] Read-only eToro REST market-data adapter
- [x] Wire market observations into Watch Engine
- [ ] Add continuous eToro polling/WebSocket service
- [ ] eToro demo execution + reconciliation
- [ ] Position manager
- [ ] Portfolio-level sizing/correlation rules
- [ ] Agent Portfolio real adapter
