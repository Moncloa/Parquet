from __future__ import annotations

from html import escape

from parquet.portfolio import ManagedPosition


def position_payload(position: ManagedPosition) -> dict[str, object]:
    pnl_usd = (
        position.last_unrealized_pnl_usd
        if position.status == "OPEN"
        else position.realized_pnl_usd
    )
    pnl_pct = None
    if pnl_usd is not None and position.amount_usd:
        pnl_pct = pnl_usd / position.amount_usd * 100.0
    return {
        "id": position.local_id,
        "broker_position_id": position.broker_position_id,
        "symbol": position.symbol,
        "side": position.side,
        "status": position.status,
        "amount_usd": position.amount_usd,
        "open_rate": position.open_rate,
        "stop_loss_rate": position.stop_loss_rate,
        "take_profit_rate": position.take_profit_rate,
        "opened_at": position.opened_at,
        "closed_at": position.closed_at,
        "pnl_usd": pnl_usd,
        "pnl_pct": pnl_pct,
        "pnl_estimated": position.pnl_estimated,
    }


def render_positions_dashboard(positions: list[ManagedPosition]) -> str:
    open_positions = sorted(
        (item for item in positions if item.status == "OPEN"),
        key=lambda item: item.opened_at,
        reverse=True,
    )
    closed_positions = sorted(
        (item for item in positions if item.status != "OPEN"),
        key=lambda item: item.closed_at or item.opened_at,
        reverse=True,
    )
    open_cards = "".join(_render_position(item, open_position=True) for item in open_positions)
    closed_cards = "".join(_render_position(item, open_position=False) for item in closed_positions)
    if not open_cards:
        open_cards = _empty("No hay posiciones abiertas")
    if not closed_cards:
        closed_cards = _empty("Todavía no hay posiciones cerradas registradas")

    return f"""<!doctype html>
<html lang="es">
<head>
<meta charset="utf-8">
<meta name="viewport" content="width=device-width, initial-scale=1">
<title>Parquet · Positions</title>
<style>
:root {{
  color-scheme: dark;
  --bg:#0d1117; --panel:#161b22; --panel2:#0d1117; --border:#30363d;
  --text:#e6edf3; --muted:#8b949e; --blue:#58a6ff; --green:#3fb950;
  --red:#f85149; --purple:#a371f7; --yellow:#d29922; --rail:#30363d;
}}
* {{ box-sizing:border-box; }}
body {{ margin:0; background:var(--bg); color:var(--text); font:14px/1.5 -apple-system,BlinkMacSystemFont,"Segoe UI",sans-serif; }}
main {{ width:min(1060px,calc(100% - 28px)); margin:0 auto; padding:34px 0 64px; }}
header {{ display:flex; justify-content:space-between; gap:20px; align-items:end; margin-bottom:26px; }}
h1 {{ margin:0; font-size:24px; letter-spacing:-.02em; }}
.subtitle {{ color:var(--muted); margin-top:4px; }}
.counts {{ display:flex; gap:8px; flex-wrap:wrap; }}
.badge {{ border:1px solid var(--border); background:var(--panel); padding:4px 9px; border-radius:999px; color:var(--muted); font-size:12px; }}
.section-title {{ margin:26px 0 12px; font-size:13px; color:var(--muted); text-transform:uppercase; letter-spacing:.08em; font-weight:600; }}
.timeline {{ position:relative; padding-left:46px; }}
.timeline:before {{ content:""; position:absolute; left:17px; top:0; bottom:0; width:2px; background:var(--rail); }}
.position {{ position:relative; margin:0 0 12px; border:1px solid var(--border); border-radius:8px; background:var(--panel); overflow:hidden; }}
.position:before {{ content:""; position:absolute; left:-37px; top:24px; width:12px; height:12px; border:3px solid var(--bg); border-radius:50%; background:var(--purple); box-shadow:0 0 0 2px var(--purple); }}
.position.open:before {{ background:var(--green); box-shadow:0 0 0 2px var(--green); }}
.position:after {{ content:""; position:absolute; left:-28px; top:29px; width:28px; height:2px; background:var(--rail); }}
.row {{ display:grid; grid-template-columns:minmax(160px,1.15fr) minmax(300px,2fr) minmax(155px,.8fr); gap:16px; padding:14px 16px; align-items:center; }}
.symbol {{ display:flex; gap:9px; align-items:center; font-weight:600; font-size:15px; }}
.side {{ font-size:11px; border:1px solid var(--border); border-radius:5px; padding:2px 5px; color:var(--muted); }}
.meta {{ color:var(--muted); font-size:12px; margin-top:3px; }}
.details {{ display:flex; gap:18px; flex-wrap:wrap; color:var(--muted); font-size:12px; }}
.details strong {{ color:var(--text); font-weight:500; }}
.pnl {{ text-align:right; font-variant-numeric:tabular-nums; }}
.pnl .abs {{ font-size:16px; font-weight:600; }}
.pnl .pct {{ font-size:12px; margin-top:1px; }}
.positive {{ color:var(--green); }} .negative {{ color:var(--red); }} .neutral {{ color:var(--muted); }}
.estimate {{ display:inline-block; margin-top:4px; color:var(--yellow); font-size:11px; }}
.empty {{ color:var(--muted); border:1px dashed var(--border); border-radius:8px; padding:16px; background:var(--panel2); }}
footer {{ color:var(--muted); font-size:11px; margin:22px 0 0 46px; }}
@media (max-width:760px) {{
  header {{ align-items:start; flex-direction:column; }}
  .timeline {{ padding-left:35px; }} .timeline:before {{ left:12px; }}
  .position:before {{ left:-29px; }} .position:after {{ left:-20px; width:20px; }}
  .row {{ grid-template-columns:1fr; gap:9px; }} .pnl {{ text-align:left; }}
}}
</style>
</head>
<body><main>
<header>
  <div><h1>Parquet · Positions</h1><div class="subtitle">Historial de ejecución en formato git graph</div></div>
  <div class="counts"><span class="badge">{len(open_positions)} abiertas</span><span class="badge">{len(closed_positions)} cerradas</span></div>
</header>
<div class="section-title">Open branch</div>
<div class="timeline">{open_cards}</div>
<div class="section-title">Closed history</div>
<div class="timeline">{closed_cards}</div>
<footer>* estimado = último P/L observado antes de que la posición desapareciera del snapshot del broker.</footer>
</main></body></html>"""


def _render_position(position: ManagedPosition, *, open_position: bool) -> str:
    pnl = position.last_unrealized_pnl_usd if open_position else position.realized_pnl_usd
    pnl_pct = None if pnl is None or not position.amount_usd else pnl / position.amount_usd * 100.0
    css = "neutral" if pnl is None or pnl == 0 else ("positive" if pnl > 0 else "negative")
    pnl_abs = "—" if pnl is None else f"{pnl:+.2f} USD"
    pnl_percent = "—" if pnl_pct is None else f"{pnl_pct:+.2f}%"
    event_at = position.opened_at if open_position else (position.closed_at or position.opened_at)
    state = "OPEN" if open_position else "CLOSED"
    estimate = "<div class=\"estimate\">~ resultado estimado</div>" if position.pnl_estimated else ""
    details = [
        f"<span>Capital <strong>{position.amount_usd:.2f} USD</strong></span>",
        _detail("Open", position.open_rate),
        _detail("SL", position.stop_loss_rate),
        _detail("TP", position.take_profit_rate),
    ]
    return f"""<article class="position {'open' if open_position else 'closed'}">
<div class="row">
  <div><div class="symbol">{escape(position.symbol)} <span class="side">{escape(position.side)}</span></div>
  <div class="meta">{state.lower()} · {event_at.astimezone().strftime('%Y-%m-%d %H:%M')}</div></div>
  <div class="details">{''.join(details)}</div>
  <div class="pnl {css}"><div class="abs">{pnl_abs}</div><div class="pct">{pnl_percent}</div>{estimate}</div>
</div></article>"""


def _detail(label: str, value: float | None) -> str:
    rendered = "—" if value is None else f"{value:g}"
    return f"<span>{label} <strong>{rendered}</strong></span>"


def _empty(message: str) -> str:
    return f"<div class=\"empty\">{escape(message)}</div>"
