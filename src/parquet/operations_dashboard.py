# ruff: noqa: E501
from __future__ import annotations

from html import escape
from typing import Any


def render_operations_dashboard(snapshot: dict[str, Any]) -> str:
    runtime = _mapping(snapshot.get("runtime"))
    overview = _mapping(snapshot.get("overview"))
    controls = _mapping(snapshot.get("controls"))
    system = _mapping(snapshot.get("system"))
    pending_reviews = _list(snapshot.get("pending_reviews"))
    reviews = _list(snapshot.get("reviews"))
    decisions = _list(snapshot.get("decisions"))
    positions = _mapping(snapshot.get("positions"))
    open_positions = _list(positions.get("open"))
    watches = _list(snapshot.get("watches"))
    providers = _mapping(controls.get("providers"))
    operational = _mapping(controls.get("operational"))
    operational_enabled = operational.get("enabled") is True
    operational_mode = str(operational.get("mode") or "unknown").lower()
    local_provider = _mapping(providers.get("local_ollama"))
    codex_provider = _mapping(providers.get("codex_cli"))
    latest_control = _mapping(controls.get("latest_review"))
    local_ready = local_provider.get("ready") is True
    codex_ready = codex_provider.get("ready") is True
    local_status = str(local_provider.get("status") or "Local provider unavailable")
    codex_status = str(codex_provider.get("status") or "Codex provider unavailable")
    latest_request_id = str(latest_control.get("request_id") or "")
    latest_state = str(latest_control.get("state") or "")
    resume_request_id = (
        latest_request_id
        if latest_state not in {"", "completed", "failed", "not_found"}
        else ""
    )

    reconciliation = str(system.get("reconciliation_state") or "UNKNOWN")
    rec_class = "ok" if reconciliation == "SYNCED" else "bad"
    identity = bool(system.get("identity_verified"))
    execution_uncertain = bool(system.get("execution_uncertain"))
    runtime_commit = str(runtime.get("commit") or "unknown")
    short_commit = runtime_commit[:8] if runtime_commit != "unknown" else "unknown"
    execution_mode = str(runtime.get("execution_mode") or "unknown").lower()
    execution_mode_class = {
        "real": "real",
        "demo": "demo",
        "shadow": "shadow",
    }.get(execution_mode, "shadow")

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Parquet · Operations</title>
<style>
:root {{
  color-scheme: dark;
  --bg:#0d1117; --panel:#161b22; --panel2:#0f141a; --border:#30363d;
  --text:#e6edf3; --muted:#8b949e; --blue:#58a6ff; --green:#3fb950;
  --red:#f85149; --yellow:#d29922; --purple:#a371f7;
}}
* {{ box-sizing:border-box; }}
html {{ scroll-behavior:smooth; }}
body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
main {{ width:min(1320px,calc(100% - 28px)); margin:0 auto; padding:26px 0 64px; }}
header {{ display:flex; justify-content:space-between; gap:18px; align-items:flex-start; margin-bottom:18px; }}
h1 {{ margin:0; font-size:25px; letter-spacing:-.025em; }}
.subtitle {{ color:var(--muted); margin-top:4px; }}
.status-row,.nav,.chips {{ display:flex; gap:7px; flex-wrap:wrap; }}
.pill,.nav a {{ border:1px solid var(--border); border-radius:999px; padding:4px 9px; color:var(--muted); text-decoration:none; background:var(--panel); font-size:12px; }}
.pill.ok {{ color:var(--green); border-color:#2b6a38; }} .pill.bad {{ color:var(--red); border-color:#7d2f2b; }}
.pill.warn {{ color:var(--yellow); border-color:#6f561f; }}
.pill.real {{ color:#f0883e; border-color:#9e6a03; }}
.pill.demo {{ color:var(--blue); border-color:#1f6feb; }}
.pill.shadow {{ color:var(--muted); border-color:var(--border); }}
.controls-grid {{ display:grid; grid-template-columns:repeat(2,minmax(0,1fr)); gap:10px; }}
.control-button {{ min-height:48px; border:1px solid var(--border); border-radius:8px; background:var(--panel2); color:var(--text); font:inherit; font-weight:650; padding:11px 14px; cursor:pointer; }}
.control-button.local {{ border-color:#2b6a38; }}
.control-button.codex {{ border-color:#1f6feb; }}
.control-button:disabled {{ opacity:.42; cursor:not-allowed; }}
.mode-switch {{ display:flex; gap:8px; margin-bottom:12px; flex-wrap:wrap; }}
.mode-option {{ border:1px solid var(--border); border-radius:999px; background:var(--panel2); color:var(--muted); padding:6px 10px; font:inherit; cursor:pointer; }}
.mode-option.active {{ color:var(--text); border-color:var(--blue); }}
.mode-option.operational.active {{ color:#f0883e; border-color:#9e6a03; }}
.mode-option:disabled {{ opacity:.42; cursor:not-allowed; }}
.execution-warning {{ display:none; margin:10px 0 0; padding:9px 10px; border:1px solid #9e6a03; border-radius:7px; color:#f0883e; background:#1a1510; font-size:12px; }}
.execution-warning.visible {{ display:block; }}
.control-note {{ color:var(--muted); font-size:12px; margin-top:9px; }}
.control-progress {{ margin-top:12px; border-top:1px solid var(--border); padding-top:12px; }}
.progress-track {{ height:7px; overflow:hidden; border-radius:999px; background:var(--panel2); border:1px solid var(--border); }}
.progress-fill {{ height:100%; width:0; background:var(--blue); transition:width .25s ease; }}
.control-result {{ margin-top:8px; color:#c9d1d9; }}
.nav {{ margin:12px 0 22px; }} .nav a:hover {{ color:var(--text); border-color:#6e7681; }}
.grid {{ display:grid; grid-template-columns:repeat(4,minmax(0,1fr)); gap:10px; }}
.metric,.panel {{ border:1px solid var(--border); background:var(--panel); border-radius:9px; }}
.metric {{ padding:13px 14px; min-height:84px; }}
.metric .label {{ color:var(--muted); font-size:11px; text-transform:uppercase; letter-spacing:.07em; }}
.metric .value {{ margin-top:4px; font-size:21px; font-weight:650; font-variant-numeric:tabular-nums; }}
.metric .hint {{ color:var(--muted); font-size:11px; margin-top:2px; }}
section {{ scroll-margin-top:14px; }}
.section-head {{ display:flex; justify-content:space-between; align-items:end; gap:15px; margin:28px 0 10px; }}
.section-head h2 {{ margin:0; font-size:15px; }} .section-head span {{ color:var(--muted); font-size:12px; }}
.panel {{ padding:14px; }}
.row {{ display:grid; grid-template-columns:150px minmax(0,1fr) 150px; gap:14px; align-items:start; padding:12px 0; border-top:1px solid var(--border); }}
.row:first-child {{ border-top:0; padding-top:0; }} .row:last-child {{ padding-bottom:0; }}
.time,.meta,.small {{ color:var(--muted); font-size:12px; }}
.title {{ font-weight:600; }} .summary {{ margin-top:4px; color:#c9d1d9; }}
.right {{ text-align:right; }}
.tag {{ display:inline-block; border:1px solid var(--border); border-radius:5px; padding:1px 5px; margin:0 4px 3px 0; color:var(--muted); font-size:11px; }}
.tag.green {{ color:var(--green); }} .tag.red {{ color:var(--red); }} .tag.yellow {{ color:var(--yellow); }} .tag.blue {{ color:var(--blue); }}
.empty {{ border:1px dashed var(--border); border-radius:8px; padding:13px; color:var(--muted); background:var(--panel2); }}
.position {{ display:grid; grid-template-columns:minmax(150px,1fr) minmax(260px,2fr) minmax(130px,.8fr); gap:14px; align-items:center; padding:12px 0; border-top:1px solid var(--border); }}
.position:first-child {{ border-top:0; padding-top:0; }} .position:last-child {{ padding-bottom:0; }}
.pnl {{ text-align:right; font-variant-numeric:tabular-nums; font-weight:650; }}
.positive {{ color:var(--green); }} .negative {{ color:var(--red); }} .neutral {{ color:var(--muted); }}
pre {{ white-space:pre-wrap; word-break:break-word; margin:7px 0 0; color:#c9d1d9; font:12px/1.45 ui-monospace,SFMono-Regular,Menlo,monospace; }}
code {{ font:12px ui-monospace,SFMono-Regular,Menlo,monospace; }}
footer {{ margin-top:24px; color:var(--muted); font-size:11px; }}
@media (max-width:900px) {{ .grid {{ grid-template-columns:repeat(2,minmax(0,1fr)); }} .row {{ grid-template-columns:1fr; }} .right {{ text-align:left; }} }}
@media (max-width:620px) {{ header {{ flex-direction:column; }} .grid {{ grid-template-columns:1fr; }} .controls-grid {{ grid-template-columns:1fr; }} .position {{ grid-template-columns:1fr; }} .pnl {{ text-align:left; }} }}
</style>
</head>
<body><main>
<header>
  <div>
    <h1>Parquet · Operations</h1>
    <div class="subtitle">De la revisión a la decisión, ejecución y resultado</div>
  </div>
  <div class="status-row">
    <span class="pill {rec_class}">{escape(reconciliation)}</span>
    <span class="pill {'ok' if identity else 'bad'}">GCID {'verified' if identity else 'unverified'}</span>
    <span class="pill {'bad' if execution_uncertain else 'ok'}">{'execution uncertain' if execution_uncertain else 'execution clear'}</span>
    <span class="pill {execution_mode_class}">{escape(execution_mode.upper())}</span>
  </div>
</header>

<div class="nav">
  <a href="#controls">Controls</a><a href="#overview">Overview</a><a href="#reviews">Reviews</a>
  <a href="#decisions">Decisions</a><a href="#positions">Positions</a><a href="#system">System</a>
</div>

<section id="controls" data-resume-request="{escape(resume_request_id)}" data-operational-mode="{escape(operational_mode)}">
<div class="section-head"><h2>Controles</h2><span>revisiones manuales</span></div>
<div class="panel">
  <div class="mode-switch">
    <button id="mode-analysis" class="mode-option active" type="button">Solo análisis</button>
    <button id="mode-operational" class="mode-option operational" type="button"{'' if operational_enabled else ' disabled'}>
      Operativa · {escape(operational_mode.upper())}
    </button>
  </div>
  <div class="controls-grid">
    <button id="review-local" class="control-button local" type="button" data-provider="local_ollama"{'' if local_ready else ' disabled'} title="{escape(local_status)}">
      Revisión local
    </button>
    <button id="review-codex" class="control-button codex" type="button" data-provider="codex_cli"{'' if codex_ready else ' disabled'} title="{escape(codex_status)}">
      Revisión Codex
    </button>
  </div>
  <div id="execution-warning" class="execution-warning">
    Modo operativo: una propuesta que supere reconciliación, identidad, riesgo, sizing, costes y demás gates podrá llegar al broker en modo {escape(operational_mode.upper())}.
  </div>
  <div class="control-note">
    Local: {escape("ready" if local_ready else local_status)} · Codex: {escape("ready" if codex_ready else codex_status)}.
    Solo análisis nunca ejecuta. Operativa usa exactamente el flujo autónomo configurado.
  </div>
  <div id="review-progress" class="control-progress" hidden>
    <div class="title" id="review-progress-title">Preparando revisión</div>
    <div class="progress-track" style="margin-top:8px"><div id="review-progress-fill" class="progress-fill"></div></div>
    <div class="small" id="review-progress-message" style="margin-top:7px"></div>
    <div class="control-result" id="review-progress-result"></div>
  </div>
</div>
</section>

<section id="overview">
<div class="grid">
  {_metric("Equity", _money(overview.get("equity_usd")), f"cash {_money(overview.get('available_cash_usd'))}")}
  {_metric("P/L abierto", _signed_money(overview.get("unrealized_pnl_usd")), "broker snapshot")}
  {_metric("Hoy", _signed_pct(overview.get("daily_pnl_pct")), f"semana {_signed_pct(overview.get('weekly_pnl_pct'))}")}
  {_metric("Posiciones", str(overview.get("open_positions") or 0), f"capital invertido {_money(overview.get('invested_usd'))}")}
  {_metric("P/L cerrado managed", _signed_money(overview.get("managed_closed_pnl_usd")), "~ estimado" if overview.get("managed_closed_pnl_estimated") else "ledger local")}
  {_metric("Comisiones", "pendiente", "histórico exacto aún no normalizado")}
  {_metric("Sizing real", f"{_num(runtime.get('position_min_pct'))}–{_num(runtime.get('position_max_pct'))}%", f"leverage máx. x{_num(runtime.get('max_leverage'))}")}
  {_metric("Runtime", escape(str(runtime.get("branch") or "unknown")), f"{escape(short_commit)} · v{escape(str(runtime.get('version') or 'unknown'))}")}
</div>
</section>

<section id="reviews">
<div class="section-head"><h2>Revisiones</h2><span>{len(pending_reviews)} siguientes · {len(reviews)} recientes</span></div>
<div class="panel">
  <div class="title">Siguientes revisiones</div>
  <div style="margin-top:8px">{_pending_reviews(pending_reviews)}</div>
</div>
<div class="panel" style="margin-top:10px">{_recent_reviews(reviews)}</div>
</section>

<section id="decisions">
<div class="section-head"><h2>Decisiones</h2><span>incluye NO TRADE y bloqueos deterministas</span></div>
<div class="panel">{_decisions(decisions)}</div>
</section>

<section id="positions">
<div class="section-head"><h2>Posiciones abiertas</h2><span>{len(open_positions)} abiertas · {len(watches)} watches activos</span></div>
<div class="panel">{_positions(open_positions)}</div>
<div class="panel" style="margin-top:10px">
  <div class="title">Watches activos</div>
  <div style="margin-top:8px">{_watches(watches)}</div>
</div>
</section>

<section id="system">
<div class="section-head"><h2>System</h2><span>estado operativo y trazabilidad</span></div>
<div class="panel">
  <div class="row"><div class="time">Runtime</div><div><div class="title">{escape(str(runtime.get("branch") or "unknown"))}</div><div class="meta"><code>{escape(runtime_commit)}</code></div></div><div class="right">{escape(str(runtime.get("strategy_provider") or "unknown"))}</div></div>
  <div class="row"><div class="time">Strategy</div><div><div class="title">Último éxito</div><div class="meta">{escape(str(system.get("strategy_last_success_at") or "—"))}</div>{_error(system.get("strategy_last_error"))}</div><div class="right">{int(system.get("strategy_pending_requests") or 0)} pending</div></div>
  <div class="row"><div class="time">Local screener</div><div><div class="title">{escape(str(system.get("local_screener_last_at") or "sin datos"))}</div>{_error(system.get("local_screener_last_error"))}</div><div></div></div>
  <div class="row"><div class="time">WebSocket</div><div><div class="title">Último mensaje</div><div class="meta">{escape(str(system.get("websocket_last_message_at") or "—"))}</div>{_error(system.get("websocket_last_error"))}</div><div></div></div>
  <div class="row"><div class="time">Reconciliation</div><div><div class="title">{escape(reconciliation)}</div>{_issues(system.get("reconciliation_issues"))}</div><div class="right">{escape(str(system.get("reconciliation_as_of") or "—"))}</div></div>
</div>
</section>

<footer>Auto-refresh cada 20 s cuando no hay una revisión manual en curso. P/L cerrado marcado como estimado procede del último P/L observado si eToro ya no muestra la posición; no equivale todavía al histórico exacto de trade/costes.</footer>
<script>{_control_script()}</script>
</main></body></html>"""



def _control_script() -> str:
    return r"""
(function () {
  const root = document.getElementById("controls");
  if (!root || root.dataset.initialized === "1") return;
  root.dataset.initialized = "1";

  const localButton = document.getElementById("review-local");
  const codexButton = document.getElementById("review-codex");
  const analysisMode = document.getElementById("mode-analysis");
  const operationalMode = document.getElementById("mode-operational");
  const executionWarning = document.getElementById("execution-warning");
  const panel = document.getElementById("review-progress");
  const title = document.getElementById("review-progress-title");
  const fill = document.getElementById("review-progress-fill");
  const message = document.getElementById("review-progress-message");
  const result = document.getElementById("review-progress-result");
  let activeRequest = root.dataset.resumeRequest || "";
  let running = Boolean(activeRequest);
  let allowExecution = false;
  window.parquetControlActive = running;

  function setButtonsDisabled(value) {
    [localButton, codexButton].forEach(function (button) {
      if (!button) return;
      const providerUnavailable = button.hasAttribute("data-provider-unavailable");
      button.disabled = value || providerUnavailable || button.dataset.initialDisabled === "1";
    });
  }

  [localButton, codexButton].forEach(function (button) {
    if (!button) return;
    if (button.disabled) button.dataset.initialDisabled = "1";
  });

  function renderStatus(data) {
    panel.hidden = false;
    const pct = Math.max(0, Math.min(100, Number(data.progress_pct || 0)));
    fill.style.width = pct + "%";
    title.textContent = String(data.state || "working").replaceAll("_", " ");
    message.textContent = data.message || "";
    result.textContent = "";

    if (data.analysis) {
      const proposalCount = Array.isArray(data.analysis.trade_proposals)
        ? data.analysis.trade_proposals.length : 0;
      const watchCount = Array.isArray(data.analysis.watch)
        ? data.analysis.watch.length : 0;
      let suffix = " · " + proposalCount + " proposal(s) · " + watchCount + " watch(es)";
      if (Array.isArray(data.execution) && data.execution.length) {
        suffix += " · " + data.execution.map(function (item) {
          return (item.symbol || "?") + ": " + (item.state || "unknown");
        }).join(" · ");
      }
      result.textContent = (data.analysis.summary || "Analysis completed") + suffix;
    }

    if (data.state === "completed" || data.state === "failed" || data.state === "not_found") {
      running = false;
      activeRequest = "";
      window.parquetControlActive = false;
      setButtonsDisabled(false);
    }
  }

  async function poll() {
    if (!activeRequest) return;
    try {
      const response = await fetch("/controls/reviews/" + encodeURIComponent(activeRequest), {
        cache: "no-store"
      });
      if (!response.ok) throw new Error("HTTP " + response.status);
      const data = await response.json();
      renderStatus(data);
      if (running) window.setTimeout(poll, 1500);
    } catch (error) {
      panel.hidden = false;
      title.textContent = "progress unavailable";
      message.textContent = String(error);
      if (running) window.setTimeout(poll, 3000);
    }
  }

  function selectMode(operational) {
    if (running) return;
    allowExecution = Boolean(operational);
    analysisMode.classList.toggle("active", !allowExecution);
    operationalMode.classList.toggle("active", allowExecution);
    executionWarning.classList.toggle("visible", allowExecution);
  }

  async function start(provider) {
    if (running) return;
    if (allowExecution) {
      const mode = (root.dataset.operationalMode || "unknown").toUpperCase();
      const accepted = window.confirm(
        "Revisión OPERATIVA " + mode + ".\n\n" +
        "Si el análisis genera una propuesta y supera todos los gates, Parquet puede abrir una posición.\n\n" +
        "¿Continuar?"
      );
      if (!accepted) return;
    }
    running = true;
    window.parquetControlActive = true;
    setButtonsDisabled(true);
    panel.hidden = false;
    title.textContent = "collecting context";
    message.textContent = "Preparing current market snapshot…";
    result.textContent = "";
    fill.style.width = "8%";

    try {
      const response = await fetch("/controls/reviews", {
        method: "POST",
        headers: {"Content-Type": "application/json"},
        body: JSON.stringify({
          provider: provider,
          allow_execution: allowExecution,
          confirmation: allowExecution ? "ALLOW EXECUTION" : null
        })
      });
      if (!response.ok) {
        const detail = await response.text();
        throw new Error("HTTP " + response.status + ": " + detail);
      }
      const data = await response.json();
      activeRequest = data.request_id || "";
      renderStatus(data);
      if (activeRequest && running) window.setTimeout(poll, 700);
    } catch (error) {
      running = false;
      window.parquetControlActive = false;
      title.textContent = "failed";
      message.textContent = String(error);
      fill.style.width = "100%";
      setButtonsDisabled(false);
    }
  }

  if (analysisMode) analysisMode.addEventListener("click", function () {
    selectMode(false);
  });
  if (operationalMode) operationalMode.addEventListener("click", function () {
    if (!operationalMode.disabled) selectMode(true);
  });

  if (localButton) localButton.addEventListener("click", function () {
    start("local_ollama");
  });
  if (codexButton) codexButton.addEventListener("click", function () {
    start("codex_cli");
  });

  if (activeRequest) {
    panel.hidden = false;
    setButtonsDisabled(true);
    poll();
  }

  window.setTimeout(function () {
    if (!window.parquetControlActive) location.reload();
  }, 20000);
})();
"""

def _metric(label: str, value: str, hint: str) -> str:
    return f'<div class="metric"><div class="label">{escape(label)}</div><div class="value">{value}</div><div class="hint">{hint}</div></div>'


def _pending_reviews(items: list[Any]) -> str:
    if not items:
        return '<div class="empty">No hay revisiones programadas</div>'
    rows: list[str] = []
    for raw in items:
        item = _mapping(raw)
        rows.append(
            f'<span class="tag blue">{escape(str(item.get("source") or "unknown"))}</span>'
            f'<strong>{escape(str(item.get("reason") or "review"))}</strong> '
            f'<span class="small">{escape(str(item.get("at") or "—"))}</span>'
        )
    return "<br>".join(rows)


def _recent_reviews(items: list[Any]) -> str:
    if not items:
        return '<div class="empty">Todavía no hay análisis persistidos</div>'
    rows: list[str] = []
    for raw in items:
        item = _mapping(raw)
        proposals = _list(item.get("trade_proposals"))
        watches = _list(item.get("watch"))
        no_trade = bool(item.get("no_trade"))
        outcome = '<span class="tag yellow">NO TRADE</span>' if no_trade else f'<span class="tag green">{len(proposals)} proposal</span>'
        rows.append(
            '<div class="row">'
            f'<div class="time">{escape(str(item.get("generated_at") or "—"))}</div>'
            '<div>'
            f'<div>{outcome}<span class="tag">{escape(str(item.get("market_regime") or "unknown"))}</span></div>'
            f'<div class="title">{escape(str(item.get("reason") or item.get("analysis_id") or "review"))}</div>'
            f'<div class="summary">{escape(str(item.get("summary") or "Sin resumen"))}</div>'
            f'{_proposal_tags(proposals)}'
            '</div>'
            f'<div class="right"><div>{len(watches)} watch</div><div class="small">{escape(str(item.get("analysis_id") or ""))}</div></div>'
            '</div>'
        )
    return "".join(rows)


def _proposal_tags(items: list[Any]) -> str:
    if not items:
        return ""
    tags: list[str] = []
    for raw in items:
        item = _mapping(raw)
        tags.append(
            f'<span class="tag green">{escape(str(item.get("symbol") or "?"))} '
            f'{escape(str(item.get("side") or ""))} '
            f'conf {_num(item.get("confidence"))}</span>'
        )
    return '<div style="margin-top:6px">' + "".join(tags) + "</div>"


def _decisions(items: list[Any]) -> str:
    if not items:
        return '<div class="empty">No hay decisiones persistidas todavía</div>'
    rows: list[str] = []
    for raw in items:
        item = _mapping(raw)
        outcome = str(item.get("outcome") or "UNKNOWN")
        css = "green" if outcome in {"REAL_EXECUTION", "SHADOW_EXECUTED"} else ("yellow" if outcome in {"NO_TRADE", "DEMO_PENDING"} else "red")
        reasons = _list(item.get("reasons"))
        reason_text = " · ".join(str(reason) for reason in reasons if reason)
        symbol = str(item.get("symbol") or "market")
        rows.append(
            '<div class="row">'
            f'<div class="time">{escape(str(item.get("at") or "—"))}</div>'
            '<div>'
            f'<div><span class="tag {css}">{escape(outcome)}</span><span class="tag">{escape(symbol)}</span></div>'
            f'<div class="summary">{escape(reason_text or "Sin motivo adicional persistido")}</div>'
            f'<div class="small">{escape(str(item.get("proposal_id") or item.get("analysis_id") or ""))}</div>'
            '</div>'
            f'<div class="right">{_gate_summary(item.get("gate"), item.get("preflight"))}</div>'
            '</div>'
        )
    return "".join(rows)


def _gate_summary(gate_raw: Any, preflight_raw: Any) -> str:
    gate = _mapping(gate_raw)
    preflight = _mapping(preflight_raw)
    bits: list[str] = []
    if gate.get("amount_usd") is not None:
        bits.append(f'gate {_money(gate.get("amount_usd"))}')
    if gate.get("spread_bps") is not None:
        bits.append(f'spread {_num(gate.get("spread_bps"))} bps')
    if gate.get("stop_distance_bps") is not None:
        stop_text = f'SL {_num(gate.get("stop_distance_bps"))} bps'
        if gate.get("minimum_stop_distance_bps") is not None:
            stop_text += f' / min {_num(gate.get("minimum_stop_distance_bps"))}'
        bits.append(stop_text)
    if preflight.get("chosen_virtual_capital_usd") is not None:
        bits.append(f'chosen {_money(preflight.get("chosen_virtual_capital_usd"))}')
    if preflight.get("what_if_total_cost_usd") is not None:
        bits.append(f'cost {_money(preflight.get("what_if_total_cost_usd"))}')
    return "<br>".join(escape(bit) for bit in bits) if bits else "—"


def _positions(items: list[Any]) -> str:
    if not items:
        return '<div class="empty">No hay posiciones abiertas</div>'
    rows: list[str] = []
    for raw in items:
        item = _mapping(raw)
        pnl = item.get("pnl_usd")
        css = "neutral"
        if isinstance(pnl, (int, float)):
            css = "positive" if pnl > 0 else ("negative" if pnl < 0 else "neutral")
        rows.append(
            '<div class="position">'
            f'<div><div class="title">{escape(str(item.get("symbol") or "?"))} <span class="tag">{escape(str(item.get("side") or ""))}</span></div><div class="meta">{escape(str(item.get("opened_at") or "—"))}</div></div>'
            f'<div class="small">Capital <strong>{_money(item.get("amount_usd"))}</strong> · Open <strong>{_num(item.get("open_rate"))}</strong> · SL <strong>{_num(item.get("stop_loss_rate"))}</strong> · TP <strong>{_num(item.get("take_profit_rate"))}</strong></div>'
            f'<div class="pnl {css}">{_signed_money(pnl)}<div class="small">{_signed_pct(item.get("pnl_pct"))}</div></div>'
            '</div>'
        )
    return "".join(rows)


def _watches(items: list[Any]) -> str:
    if not items:
        return '<div class="empty">No hay watches activos</div>'
    parts: list[str] = []
    for raw in items:
        item = _mapping(raw)
        trigger = _mapping(item.get("trigger"))
        parts.append(
            f'<span class="tag blue">{escape(str(item.get("symbol") or "?"))} '
            f'{escape(str(trigger.get("type") or ""))} {_num(trigger.get("price"))} '
            f'→ {escape(str(item.get("on_trigger") or "REASSESS"))}</span>'
        )
    return "".join(parts)


def _issues(raw: Any) -> str:
    items = _list(raw)
    if not items:
        return '<div class="meta">sin incidencias</div>'
    return "".join(
        f'<div class="meta">{escape(str(_mapping(item).get("code") or "issue"))}: {escape(str(_mapping(item).get("detail") or ""))}</div>'
        for item in items
    )


def _error(value: Any) -> str:
    if value in (None, ""):
        return ""
    return f'<div class="small negative">{escape(str(value))}</div>'


def _mapping(value: Any) -> dict[str, Any]:
    return value if isinstance(value, dict) else {}


def _list(value: Any) -> list[Any]:
    return value if isinstance(value, list) else []


def _money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return "$" + f"{value:,.2f}"


def _signed_money(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return f"{value:+,.2f} USD"


def _signed_pct(value: Any) -> str:
    if not isinstance(value, (int, float)):
        return "—"
    return f"{value:+.2f}%"


def _num(value: Any) -> str:
    if value is None:
        return "—"
    if isinstance(value, float):
        return f"{value:g}"
    return str(value)
