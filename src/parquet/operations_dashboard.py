# ruff: noqa: E501
from __future__ import annotations

from html import escape
from typing import Any


def render_operations_dashboard(snapshot: dict[str, Any]) -> str:
    runtime = _mapping(snapshot.get("runtime"))
    overview = _mapping(snapshot.get("overview"))
    system = _mapping(snapshot.get("system"))
    pending_reviews = _list(snapshot.get("pending_reviews"))
    reviews = _list(snapshot.get("reviews"))
    decisions = _list(snapshot.get("decisions"))
    positions = _mapping(snapshot.get("positions"))
    open_positions = _list(positions.get("open"))
    watches = _list(snapshot.get("watches"))

    reconciliation = str(system.get("reconciliation_state") or "UNKNOWN")
    rec_class = "ok" if reconciliation == "SYNCED" else "bad"
    identity = bool(system.get("identity_verified"))
    execution_uncertain = bool(system.get("execution_uncertain"))
    runtime_commit = str(runtime.get("commit") or "unknown")
    short_commit = runtime_commit[:8] if runtime_commit != "unknown" else "unknown"

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
@media (max-width:620px) {{ header {{ flex-direction:column; }} .grid {{ grid-template-columns:1fr; }} .position {{ grid-template-columns:1fr; }} .pnl {{ text-align:left; }} }}
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
    <span class="pill">{escape(str(runtime.get('execution_mode') or 'unknown')).upper()}</span>
  </div>
</header>

<div class="nav">
  <a href="#overview">Overview</a><a href="#reviews">Reviews</a><a href="#decisions">Decisions</a>
  <a href="#positions">Positions</a><a href="#system">System</a>
</div>

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

<footer>Auto-refresh cada 20 s. P/L cerrado marcado como estimado procede del último P/L observado si eToro ya no muestra la posición; no equivale todavía al histórico exacto de trade/costes.</footer>
<script>setTimeout(function(){{ location.reload(); }}, 20000);</script>
</main></body></html>"""


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
