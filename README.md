# Parquet

Parquet is an event-driven trading orchestrator designed to combine **ChatGPT market analysis** with a **deterministic local risk/execution layer**.

> Current status: **v0.10**. Read-only eToro integration, reconciliation, position management, dashboard, shadow/demo autonomous routing, and a **supervised real Agent Portfolio execution path** are implemented. Supervised real execution is disabled by default.

## Architecture

```text
ChatGPT / strategy
        |
        v
Trade proposal / watch
        |
        v
Deterministic execution gate
        |
        +--> risk engine
        +--> reconciliation = SYNCED?
        +--> eToro Agent Portfolio identity pinned?
        |
        v
Shadow / demo / supervised real adapter
        |
        v
      eToro
```

The design deliberately separates responsibilities:

- ChatGPT proposes market context, deterministic watch conditions, trade proposals and optional review times.
- Parquet validates all machine-readable responses.
- Market-opening reviews are scheduled locally in each exchange's timezone, so DST differences are handled automatically.
- The Watch Engine evaluates triggers locally without calling ChatGPT continuously.
- Dynamic reviews, watches, execution attempts and broker reconciliation state persist in SQLite.
- The deterministic risk engine can reject proposals independently of ChatGPT.
- eToro is treated as the broker source of truth for open positions and orders.
- Stop-loss is mandatory by default.
- Credentials never live in Git.

## eToro Agent Portfolio

Parquet is intended to use a dedicated eToro Agent Portfolio for real execution. Real writes are guarded by an explicit GCID pin and required OAuth/API scopes.

Configure the Agent Portfolio GCID in `/etc/parquet/parquet.yaml`:

```yaml
etoro:
  enabled: true
  api_key_file: /etc/parquet/etoro_api_key
  user_key_file: /etc/parquet/etoro_user_key
  base_url: https://public-api.etoro.com/api/v1
  execution_base_url: https://public-api.etoro.com/api/v2
  expected_gcid: 12345678
```

Then verify the authenticated identity without sending any order:

```bash
parquet validate
parquet etoro-check
```

`parquet etoro-check` calls eToro `/me`, prints the authenticated GCID and scopes, and exits non-zero if the token does not match the configured Agent Portfolio or lacks any required real/trading scope.

### Real order lifecycle

The supervised real path deliberately fails closed:

```text
PREPARED
   |
   v
fresh reconciliation + risk gate
   |
   v
validate Agent Portfolio GCID/scopes
   |
   v
persist X-Request-Id + SUBMITTING
   |
   v
POST order exactly once
   |
   v
read-only orders:lookup by referenceId
   |
   +--> Filled / partial execution --> reconcile broker position
   |
   +--> Rejected ------------------> REJECTED
   |
   +--> unresolved ----------------> OUTCOME_UNKNOWN + global execution block
```

The order POST is **never automatically retried**. If a timeout or ambiguous broker response occurs, Parquet uses the already-persisted request ID as eToro's `referenceId` to recover the outcome through a read-only lookup. If the result cannot be proven, subsequent execution is blocked until the inconsistency is resolved.

Opening shorts are normalized to eToro's `sellShort` transaction type.

Supervised real execution remains off unless explicitly enabled:

```yaml
execution:
  supervised_real_enabled: false
  supervised_real_max_amount_usd: 25.0
```

When enabled, the CLI still requires the exact attempt-bound confirmation:

```text
REAL <attempt_id>
```

Autonomous execution configuration remains limited to shadow/demo modes; the supervised real gate is separate.

## Reproducible host contract

Host-provided credentials are kept outside the repository. The exact paths are configurable; the normal installation uses files under `/etc/parquet/`.

The installer **never creates or overwrites private credentials**. It installs the pinned GitHub host key and SSH configuration, creates the service environment and preserves host configuration across upgrades.

## Install on a fresh Debian LXC

Because `Moncloa/Parquet` is private, the first clone uses a repository-scoped read-only GitHub deploy key. See [`docs/lxc-runbook.md`](docs/lxc-runbook.md) for the complete bootstrap.

From a root shell:

```bash
cd Parquet
./install.sh
```

Then:

```bash
systemctl status parquet --no-pager
curl -fsS http://127.0.0.1:8787/health
curl -fsS http://127.0.0.1:8787/status
```

Future updates:

```bash
./scripts/update.sh
```

The update path uses `/etc/parquet/github_deploy_key` and only accepts fast-forward Git updates.

## Positions dashboard

Parquet exposes a Git-graph-inspired managed-position view:

```text
GET /positions
GET /positions.json
```

The dashboard shows open and closed managed positions and P/L. Until exact closed-trade history is wired from eToro, closed P/L can be marked as estimated from the last observed unrealized value.

## Structural reviews

Openings are defined in the timezone of the market rather than hard-coded in a single local timezone:

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
2. ChatGPT does not bypass deterministic position sizing, portfolio limits or reconciliation gates.
3. Expired or stale signals are rejected.
4. Duplicate/unknown broker outcomes must be reconciled before another broker write.
5. A real submission receives and persists its unique request ID before the network write.
6. A real order POST is never blindly retried after a timeout or ambiguous response.
7. The authenticated eToro GCID and required scopes must match the pinned Agent Portfolio before a real write.
8. Loss limits can disable new trading independently of the LLM.
9. Existing broker-side protection continues working if ChatGPT is unavailable.
10. Supervised real execution is disabled by default.

## Roadmap

- [x] Typed ChatGPT message schema
- [x] GitHub bridge abstraction
- [x] Dynamic review queue
- [x] Structural scheduler with timezone/DST handling
- [x] SQLite state/audit trail
- [x] Persistent review/watch state
- [x] Deterministic Watch Engine
- [x] Deterministic risk foundation
- [x] Shadow execution adapter
- [x] Read-only eToro market/account adapter
- [x] Position manager and broker reconciliation
- [x] Positions dashboard
- [x] Supervised eToro Agent Portfolio real adapter
- [x] Agent Portfolio GCID/scope pinning
- [x] eToro order lookup and ambiguous-outcome recovery
- [x] CI: Ruff + strict mypy + pytest
- [ ] Exact eToro closed-trade history / realized P&L
- [ ] Continuous eToro streaming where supported
- [ ] Portfolio-level sizing/correlation refinements
