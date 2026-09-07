# Parquet

Parquet is an event-driven trading orchestrator that combines **LLM market strategy** with a **deterministic local risk/execution layer**.

> Current status: **v0.11**. eToro Agent Portfolio identity/reconciliation, position management, dashboard, supervised real-small execution, eToro order lookup/recovery, eligibility-aware ticket preparation, and an isolated Codex strategy worker are implemented. Real execution remains disabled by default.

## Architecture

```text
structural/manual/watch review
        |
        v
ReviewRequest + sanitized eToro/risk context
        |
        v
isolated parquet-strategy user
Codex CLI + web search
        |
        v
schema-validated MarketAnalysis
        |
        v
GitHub runtime audit channel
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
        +--> eToro eligibility / minimum size
        |
        v
shadow / demo / supervised real adapter
        |
        v
      eToro
```

The design deliberately separates responsibilities:

- Strategy proposes market context, deterministic watch conditions, trade proposals and optional review times.
- Parquet validates every machine-readable response and never lets the LLM choose final account exposure.
- Market-opening reviews are scheduled locally in each exchange timezone, including DST/calendar handling.
- The Watch Engine evaluates deterministic triggers locally without continuously calling the strategy model.
- Reviews, watches, proposals, execution attempts and reconciliation state persist in SQLite.
- eToro is the source of truth for open positions and orders.
- Stop-loss is mandatory by default.
- Credentials never live in Git.
- The Codex strategy process runs under a separate Unix identity that cannot access `/etc/parquet`.

## eToro Agent Portfolio

Parquet is intended to use a dedicated eToro Agent Portfolio for real execution. Real writes are guarded by an explicit GCID pin and required scopes.

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

Verify without sending an order:

```bash
parquet validate
parquet etoro-check
```

Runtime reconciliation verifies `/me` repeatedly and fails closed if the authenticated GCID or required real/trading scopes no longer match.

## Strategy worker

Parquet 0.11 closes the `REVIEW_REQUEST -> ANALYSIS` loop with an **isolated Codex CLI worker**. The broker process never launches Codex directly.

The handoff uses `/var/lib/parquet-exchange`:

```text
parquet -> sanitized ReviewRequest JSON -> parquet-strategy
parquet <- validated MarketAnalysis JSON <- parquet-strategy
```

`parquet-strategy` is deliberately not a member of the `parquet` group and the systemd unit marks `/etc/parquet` inaccessible. Therefore the strategy model does not receive the eToro API/user token or GitHub token.

Deploy 0.11 first, then perform the one-time isolated Codex setup:

```bash
bash ./scripts/setup-codex-strategy.sh
```

The script installs Codex CLI if required and runs ChatGPT device authentication as `parquet-strategy`. It **does not enable the strategy service automatically**.

After authentication, enable the strategy dispatcher in `/etc/parquet/parquet.yaml`:

```yaml
strategy:
  enabled: true
  provider: codex_cli
  queue_dir: /var/lib/parquet-exchange
```

Then start the isolated worker and restart Parquet:

```bash
systemctl enable --now parquet-strategy.service
systemctl restart parquet.service
```

See [`docs/strategy-worker.md`](docs/strategy-worker.md) for the security model and bootstrap procedure.

## Reviews and proposals

Request an immediate strategy review without touching the broker:

```bash
parquet request-review-now --reason manual_opportunity_scan
```

List persisted proposals:

```bash
parquet proposals
```

`NO TRADE` is a valid strategy result. Parquet never creates a proposal merely to keep capital busy or to test execution code.

To prepare a supervised real-small ticket from an active proposal:

```bash
parquet prepare-real-small <proposal_id>
```

Preparation is read-only at the broker. It requires fresh reconciliation/identity, a fresh bid/ask, deterministic gate approval and eToro eligibility. It queries eToro's minimum position amount and selects a size bounded by both risk sizing and `supervised_real_max_amount_usd`.

## Real order lifecycle

The supervised real path deliberately fails closed:

```text
PREPARED
   |
   v
fresh reconciliation + risk gate
   |
   v
Agent Portfolio GCID/scopes + eToro eligibility
   |
   v
persist X-Request-Id + SUBMITTING
   |
   v
POST order exactly once
   |
   v
read-only orders:lookup by order/reference ID
   |
   +--> Filled / partial execution --> reconcile broker position
   |
   +--> Rejected ------------------> REJECTED
   |
   +--> unresolved ----------------> OUTCOME_UNKNOWN + global execution block
```

The order POST is **never automatically retried**. If a timeout or ambiguous broker response occurs, Parquet uses the durable request/reference ID for read-only recovery. If the result cannot be proven, subsequent execution is blocked.

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

Autonomous broker execution remains limited to shadow/demo configuration; strategy autonomy and real-money autonomy are separate controls.

## Reproducible host contract

Host credentials live outside the repository. The normal installation uses `/etc/parquet/` and runs the broker service as the unprivileged `parquet` user.

For a fresh Debian LXC, see [`docs/lxc-runbook.md`](docs/lxc-runbook.md). From a root shell:

```bash
cd Parquet
./install.sh
```

Future updates:

```bash
./scripts/update.sh
```

The update path only accepts fast-forward Git updates.

## Runtime diagnostics

```bash
systemctl status parquet --no-pager
curl -fsS http://127.0.0.1:8787/health
curl -fsS http://127.0.0.1:8787/status
```

When strategy is enabled, `/health` also exposes worker heartbeat/readiness, pending requests, last strategy analysis ID and last error. It never exposes Codex credentials.

The positions dashboard is available at:

```text
GET /positions
GET /positions.json
```

## Structural reviews

Openings are defined in the timezone of the market rather than hard-coded to one local timezone:

```yaml
schedule:
  structural_reviews:
    - name: asia_open
      timezone: Asia/Tokyo
      calendar: XTKS
      hour: 9
      minute: 0
      offset_minutes: 1
    - name: europe_open
      timezone: Europe/Berlin
      calendar: XETR
      hour: 9
      minute: 0
      offset_minutes: 1
    - name: wall_street_open
      timezone: America/New_York
      calendar: XNYS
      hour: 9
      minute: 30
      offset_minutes: 1
```

## Strategy contract

Strategy analyses are identified by:

```text
[PARQUET:ANALYSIS]
```

Parquet review requests use:

```text
[PARQUET:REVIEW_REQUEST]
```

The schema and strategy guidance live in [`docs/chatgpt-analysis-contract.md`](docs/chatgpt-analysis-contract.md).

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
2. Strategy output cannot bypass deterministic sizing, portfolio limits or reconciliation gates.
3. Expired/stale signals and stale executable quotes are rejected.
4. Duplicate/unknown broker outcomes must be reconciled before another broker write.
5. A real submission persists its unique request ID before the network write.
6. A real order POST is never blindly retried after an ambiguous outcome.
7. The authenticated GCID/scopes must match the pinned Agent Portfolio before a real write.
8. Loss limits can disable new trading independently of the LLM.
9. Existing broker-side protection continues working if strategy/ChatGPT is unavailable.
10. The Codex strategy worker cannot read `/etc/parquet` and has no broker/GitHub credentials.
11. Strategy autonomy does not enable real-money execution.
12. Supervised real execution is disabled by default.

## Roadmap

- [x] Typed strategy message schema
- [x] GitHub bridge/audit channel
- [x] Dynamic and structural review queue
- [x] SQLite state/audit trail
- [x] Deterministic Watch Engine and risk gate
- [x] Read-only eToro market/account adapter
- [x] Agent Portfolio identity and scope pinning
- [x] Position manager and broker reconciliation
- [x] Positions dashboard
- [x] Supervised eToro Agent Portfolio real adapter
- [x] eToro order lookup and ambiguous-outcome recovery
- [x] eToro eligibility/minimum-size preflight
- [x] Supervised real-small ticket preparation
- [x] Immediate manual review requests
- [x] Isolated Codex strategy worker using ChatGPT login
- [x] CI: Ruff + strict mypy + pytest
- [ ] Exact eToro closed-trade history / realized P&L
- [ ] Continuous eToro streaming where supported
- [ ] Portfolio-level sizing/correlation refinements
- [ ] Unattended real execution after supervised validation milestones
