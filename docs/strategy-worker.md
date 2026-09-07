# Isolated Codex strategy worker

Parquet 0.11 can close the `REVIEW_REQUEST -> ANALYSIS` loop with Codex CLI while keeping broker credentials outside the LLM process.

## Security boundary

The broker service and strategy service run as different Unix users:

```text
parquet
  reads /etc/parquet (eToro + GitHub credentials)
  writes review_request events
        |
        v
/var/lib/parquet-exchange
  sanitized ReviewRequest JSON only
        |
        v
parquet-strategy
  cannot access /etc/parquet
  owns its own CODEX_HOME
  Codex: read-only sandbox, approval=never, ephemeral session
  writes schema-validated MarketAnalysis JSON
        |
        v
/var/lib/parquet-exchange
        |
        v
parquet dispatcher
  validates correlation + quote freshness again
  publishes [PARQUET:ANALYSIS] to GitHub
  normal Parquet ingestion persists proposals/watches
```

The strategy worker receives market/risk context, not eToro API keys, the eToro user token, or the GitHub token. It has no broker adapter and cannot submit an eToro order.

## Install and authenticate Codex

After deploying Parquet 0.11, run as root from the repository checkout:

```bash
bash ./scripts/setup-codex-strategy.sh
```

The script:

1. creates the isolated `parquet-strategy` user/group;
2. creates `/var/lib/parquet-exchange` as the only broker/strategy handoff;
3. installs the hardened `parquet-strategy.service` unit;
4. installs Codex CLI if it is missing;
5. starts `codex login --device-auth` as `parquet-strategy` with `CODEX_HOME=/var/lib/parquet-strategy/codex`;
6. restarts only `parquet.service` so its supplementary exchange-group membership is refreshed;
7. leaves `parquet-strategy.service` disabled and stopped.

The device login path is intended for headless/remote hosts. It uses the Codex/ChatGPT login rather than requiring an OpenAI API key.

## Enable strategy only after authentication

Edit `/etc/parquet/parquet.yaml`:

```yaml
strategy:
  enabled: true
  provider: codex_cli
  queue_dir: /var/lib/parquet-exchange
```

Then:

```bash
systemctl enable --now parquet-strategy.service
systemctl restart parquet.service
```

Check:

```bash
systemctl --no-pager --full status parquet-strategy.service
curl -fsS http://127.0.0.1:8787/health | /opt/parquet/venv/bin/python -m json.tool
```

Expected strategy fields once the worker heartbeat is current:

```json
{
  "strategy_enabled": true,
  "strategy_provider": "codex_cli",
  "strategy_worker_ready": true,
  "strategy_pending_requests": 0,
  "strategy_last_error": null
}
```

## First analysis test

The dispatcher deliberately initializes its cursor at the newest existing review event. Old reviews are not replayed when strategy is first enabled.

Create a new review after the worker is ready:

```bash
/opt/parquet/venv/bin/parquet request-review-now --reason strategy_bootstrap_test
```

Then inspect:

```bash
curl -fsS http://127.0.0.1:8787/status | /opt/parquet/venv/bin/python -m json.tool
/opt/parquet/venv/bin/parquet proposals
```

`NO TRADE` is a valid result and should not be converted into a proposal merely to exercise execution code.

## Codex worker policy

The worker invokes Codex non-interactively with:

- web search enabled;
- `--ask-for-approval never`;
- `--sandbox read-only`;
- `exec --ephemeral`;
- `--output-schema` generated from `MarketAnalysis`;
- no inherited `OPENAI_API_KEY`, `GITHUB_TOKEN`, or Parquet broker credential variables.

The prompt additionally tells the analyst not to read local files or execute shell commands. Web pages and request fields are treated as untrusted data rather than instructions.

Parquet validates the result after Codex returns and validates it a second time on the broker side before publishing it. Among other checks:

- `review_request_id` must exactly match the queued request;
- proposal/watch symbols must be present in the request;
- proposal/watch quotes must be present, non-stale and have bid/ask;
- proposal `generated_at` is mandatory;
- `EXECUTE` watches must reference a matching proposal;
- sources must be HTTPS URLs;
- credential-like output is rejected.

## Separation from execution

`strategy.enabled` only enables analysis generation. It does **not** alter:

```yaml
execution:
  supervised_real_enabled: false
  autonomous_enabled: false
  autonomous_mode: shadow
```

A strategy proposal remains data. It still has to pass Parquet's deterministic gate, eToro eligibility/minimum checks, identity/reconciliation checks, and the supervised execution adapter before any real broker POST can occur.
