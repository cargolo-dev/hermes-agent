from __future__ import annotations

import html as _html
import json
import logging
import os
import time
from html import escape as _html_escape
from pathlib import Path
from typing import Any

import requests

from hermes_constants import get_hermes_home

logger = logging.getLogger(__name__)

_DEFAULT_ROUTE_NAME = "cargolo-asr-ingest"


def _load_targets_from_route_config(route_name: str) -> list[dict[str, Any]]:
    subscriptions_path = get_hermes_home() / "webhook_subscriptions.json"
    if not subscriptions_path.exists():
        return []
    try:
        data = json.loads(subscriptions_path.read_text(encoding="utf-8"))
    except Exception as exc:
        logger.warning("Could not read webhook subscriptions for ASR ops notifications: %s", exc)
        return []
    route = data.get(route_name)
    if not isinstance(route, dict):
        return []
    targets: list[dict[str, Any]] = []
    for item in route.get("deliver_additional") or []:
        if not isinstance(item, dict) or item.get("deliver") != "webhook_forward":
            continue
        extra = item.get("deliver_extra") or {}
        url = str(extra.get("url") or "").strip()
        if not url:
            continue
        targets.append(
            {
                "url": url,
                "method": str(extra.get("method") or "POST").strip().upper() or "POST",
                "headers": dict(extra.get("headers") or {}),
                "source": f"route:{route_name}",
            }
        )
    return targets


def _load_targets(*, route_name: str, allow_route_fallback: bool) -> list[dict[str, Any]]:
    url = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_URL", "")).strip()
    if url:
        headers = {"Content-Type": "application/json"}
        auth_token = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_TOKEN", "")).strip()
        if auth_token:
            headers["Authorization"] = f"Bearer {auth_token}"
        return [
            {
                "url": url,
                "method": str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_METHOD", "POST") or "POST").strip().upper() or "POST",
                "headers": headers,
                "source": "env:HERMES_CARGOLO_ASR_OPS_WEBHOOK_URL",
            }
        ]
    if allow_route_fallback:
        return _load_targets_from_route_config(route_name)
    return []


def _priority_emoji(priority: Any) -> str:
    raw = str(priority or "").strip().lower()
    return {
        "low": "🟢",
        "medium": "🟡",
        "high": "🟠",
        "urgent": "🔴",
    }.get(raw, "⚪")


def _priority_label_de(priority: Any) -> str:
    raw = str(priority or "").strip().lower()
    return {
        "low": "niedrig",
        "medium": "mittel",
        "high": "hoch",
        "urgent": "urgent",
    }.get(raw, "")


def _priority_label(value: Any) -> str:
    label = _priority_label_de(value)
    return label or "mittel"


def _join_list(value: Any, *, empty: str, limit: int = 4) -> str:
    if isinstance(value, str):
        return value.strip() or empty
    if not isinstance(value, list):
        return empty
    items = [str(item).strip() for item in value if str(item).strip()]
    if not items:
        return empty
    shown = items[:limit]
    if len(items) > limit:
        shown.append(f"+{len(items) - limit} weitere")
    return ", ".join(shown)


def _format_summary_map(summary: Any) -> str:
    if not isinstance(summary, dict):
        return "Keine"
    ordered = []
    for key, label in (
        ("write_now", "sofort umsetzbar"),
        ("review", "Review"),
        ("not_yet_due", "noch nicht faellig"),
        ("not_yet_knowable", "noch nicht belastbar"),
        ("applied", "umgesetzt"),
        ("failed", "fehlgeschlagen"),
        ("skipped", "uebersprungen"),
    ):
        value = summary.get(key)
        if isinstance(value, int) and value > 0:
            ordered.append(f"{label}={value}")
    return ", ".join(ordered) if ordered else "Keine"


def _truncate(text: Any, limit: int = 220) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    return value[: limit - 3].rstrip() + "..."


def _truncate_sentence(text: Any, limit: int = 320) -> str:
    value = str(text or "").strip()
    if len(value) <= limit:
        return value
    cut = value[:limit].rsplit(" ", 1)[0].rstrip(" ,;:.-")
    return cut + " …"


def _run_label(run_type: str) -> str:
    return {
        "bootstrap_case": "Bootstrap",
        "bootstrap_cases_from_tms": "Bulk-Bootstrap",
        "process_event": "Mail-Ingest",
        "analysis_update": "Analyse-Update",
    }.get(run_type, run_type or "Run")


def _status_label(status: str, initialized: bool) -> str:
    mapping = {
        "bootstrapped": "initialisiert",
        "processed": "aktualisiert",
        "duplicate": "Dublette",
        "review_queue": "zur Prüfung",
    }
    base = mapping.get((status or "").strip(), status or "—")
    if initialized and status != "bootstrapped":
        base = f"{base} (neu)"
    return base


def _load_analysis_brief(path_value: Any) -> dict[str, Any]:
    path = str(path_value or "").strip()
    if not path:
        return {}
    try:
        candidate = Path(path)
        if not candidate.exists() or not candidate.is_file():
            return {}
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not load ASR analysis brief from %s", path, exc_info=True)
        return {}


def _load_tms_route(case_root: Path) -> dict[str, str]:
    try:
        data = json.loads((case_root / "tms_snapshot.json").read_text(encoding="utf-8"))
    except Exception:
        return {}
    detail = data.get("detail") if isinstance(data.get("detail"), dict) else {}
    origin_block = detail.get("origin") if isinstance(detail.get("origin"), dict) else {}
    dest_block = detail.get("destination") if isinstance(detail.get("destination"), dict) else {}
    return {
        "origin": str(origin_block.get("city") or "").strip(),
        "destination": str(dest_block.get("city") or "").strip(),
        "network": str(detail.get("network") or "").strip(),
        "shipment_uuid": str(data.get("shipment_uuid") or detail.get("shipment_uuid") or "").strip(),
    }


def _build_tms_url(shipment_uuid: str) -> str | None:
    if not shipment_uuid:
        return None
    base = os.getenv("HERMES_CARGOLO_TMS_BASE_URL", "https://api.cargolo.de").strip().rstrip("/")
    if not base:
        return None
    return f"{base}/admin/shipment_detail?shipment_uuid={shipment_uuid}"


def _build_review_links(order_id: str, suggestion_key: str) -> list[str]:
    """Return HTML anchor snippets for accept/reject review clicks, or []."""
    base = os.getenv("HERMES_CARGOLO_ASR_REVIEW_BASE_URL", "").strip().rstrip("/")
    if not base or not order_id:
        return []
    try:
        from .review import review_signing_available, sign_review_tokens
    except Exception:
        return []
    if not review_signing_available():
        return []
    try:
        tokens = sign_review_tokens(order_id=order_id, suggestion_key=suggestion_key)
    except Exception:
        logger.debug("review token signing failed", exc_info=True)
        return []
    accept_url = f"{base}?t={tokens['accepted']}"
    reject_url = f"{base}?t={tokens['rejected']}"
    return [
        f'<a href="{_html.escape(accept_url)}"><b>✅ Vorschlag richtig</b></a>',
        f'<a href="{_html.escape(reject_url)}"><b>✏️ War so nicht richtig</b></a>',
    ]


def _pick_top_risks(risk_flags: list, limit: int = 3) -> list[dict]:
    order = {"critical": 0, "high": 1, "medium": 2, "low": 3}
    ranked = sorted(
        (r for r in risk_flags if isinstance(r, dict)),
        key=lambda r: order.get(str(r.get("severity", "")).lower(), 9),
    )
    return ranked[:limit]


def _risk_short_text(risk: dict) -> str:
    reason = str(risk.get("reason") or "").strip()
    if not reason:
        return str(risk.get("code") or "Risiko").strip()
    return _truncate_sentence(reason, 140)


def _pick_next_step(brief: dict, fallback: str = "") -> str:
    actions = brief.get("internal_actions") if isinstance(brief.get("internal_actions"), list) else []
    blocking_first = [a for a in actions if isinstance(a, dict) and bool(a.get("blocking")) and a.get("action")]
    pool = blocking_first or [a for a in actions if isinstance(a, dict) and a.get("action")]
    if pool:
        return str(pool[0].get("action") or "").strip()
    return (fallback or "").strip()


def _build_bulk_message(run_type: str, payload: dict[str, Any]) -> str:
    total = payload.get("total_selected")
    success = payload.get("success_count")
    errors = payload.get("error_count")
    lines = [
        "🗂 <b>CARGOLO ASR · Bulk-Lauf</b>",
        f"<i>{_html.escape(_run_label(run_type))}</i>",
    ]
    stats = []
    if isinstance(total, int):
        stats.append(f"{total} ausgewählt")
    if isinstance(success, int):
        stats.append(f"{success} erfolgreich")
    if isinstance(errors, int) and errors > 0:
        stats.append(f"<b>{errors} Fehler</b>")
    if stats:
        lines.append(" · ".join(stats))
    return "<br>".join(lines)


def _build_summary_message(run_type: str, payload: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    order_id = result.get("order_id") or payload.get("order_id") or "-"
    status = str(result.get("status") or payload.get("status") or run_type).strip()
    history_sync_count = result.get("history_sync_count")
    history_sync_status = str(result.get("history_sync_status") or "").strip().lower()
    history_sync_error = str(result.get("history_sync_error") or "").strip()
    pending_summary = result.get("pending_action_summary") or {}
    applied_summary = result.get("applied_action_summary") or {}
    message_text = str(result.get("message") or payload.get("message") or "").strip()
    latest_subject = str(result.get("latest_subject") or payload.get("latest_subject") or "").strip()
    analysis_brief = _load_analysis_brief(result.get("analysis_brief_path"))
    analysis_priority = analysis_brief.get("priority") or result.get("analysis_priority")
    ops_summary = str(analysis_brief.get("ops_summary") or result.get("analysis_summary") or "").strip()
    risk_flags = analysis_brief.get("risk_flags") if isinstance(analysis_brief.get("risk_flags"), list) else []
    internal_actions = analysis_brief.get("internal_actions") if isinstance(analysis_brief.get("internal_actions"), list) else []

    applied_targets = _join_list(result.get("applied_action_targets"), empty="", limit=3)
    failed_targets = _join_list(result.get("failed_action_targets"), empty="", limit=2)

    recommendation = "Weiter beobachten"
    if internal_actions:
        first_action = internal_actions[0] if isinstance(internal_actions[0], dict) else {}
        action_text = str(first_action.get("action") or "").strip()
        if action_text:
            recommendation = action_text
    elif ops_summary:
        recommendation = ops_summary

    escalation = "keine akute Eskalation"
    if risk_flags:
        for risk in risk_flags:
            if not isinstance(risk, dict):
                continue
            severity = str(risk.get("severity") or "").strip().lower()
            reason = str(risk.get("reason") or risk.get("code") or "").strip()
            if severity in {"high", "critical"} and reason:
                escalation = reason
                break
    elif str(analysis_priority or "").strip().lower() == "urgent":
        escalation = "operative Dringlichkeit hoch"
    elif history_sync_error:
        escalation = "Mailhistorie nicht belastbar"

    pending_total = sum(int(value or 0) for value in pending_summary.values()) if isinstance(pending_summary, dict) else 0
    if history_sync_error:
        history_token = "Mailhistorie Fehler"
    elif history_sync_status == "ok" or (history_sync_status == "" and isinstance(history_sync_count, int)):
        history_token = f"Mail +{history_sync_count or 0}"
    elif history_sync_status == "skipped":
        history_token = "Mail unverändert"
    else:
        history_token = f"Mail {history_sync_status or 'unbekannt'}"

    tms_token = "keine TMS-Änderung"
    if applied_targets:
        tms_token = f"TMS: {applied_targets}"
    elif failed_targets:
        tms_token = f"TMS fehlgeschlagen: {failed_targets}"
    elif isinstance(applied_summary, dict) and int(applied_summary.get("applied", 0) or 0) > 0:
        tms_token = f"TMS: {int(applied_summary.get('applied', 0))} Änderung(en)"

    summary_line = _truncate(ops_summary or message_text or latest_subject or recommendation, 180)
    parts = [
        f"{order_id} | {status} | Priorität {_priority_label(analysis_priority)}",
        f"{history_token} | {tms_token} | Offen {pending_total}",
        f"Nächster Schritt: {_truncate(recommendation, 140)}",
    ]
    if escalation != "keine akute Eskalation":
        parts.append(f"Achtung: {_truncate(escalation, 140)}")
    if summary_line:
        parts.append(f"Kurzfazit: {summary_line}")
    return "\n".join(parts)


def _format_html_value(value: Any) -> str:
    if value is None:
        return "-"
    if isinstance(value, bool):
        return "ja" if value else "nein"
    if isinstance(value, (int, float)):
        return _html_escape(str(value))
    if isinstance(value, str):
        stripped = value.strip()
        return _html_escape(stripped) if stripped else "-"
    if isinstance(value, list):
        items = [item for item in value if item not in (None, "", [], {})]
        if not items:
            return "-"
        inner = "".join(f"<li>{_format_html_value(item)}</li>" for item in items)
        return f"<ul>{inner}</ul>"
    if isinstance(value, dict):
        if not value:
            return "-"
        rows = []
        for key, item in value.items():
            rows.append(
                f"<tr><th align='left'>{_html_escape(str(key))}</th><td>{_format_html_value(item)}</td></tr>"
            )
        return f"<table border='1' cellspacing='0' cellpadding='6'>{''.join(rows)}</table>"
    return _html_escape(str(value))


def _html_badge(text: Any, tone: str = "neutral") -> str:
    palette = {
        "neutral": ("#e5eefc", "#1d4ed8"),
        "good": ("#dcfce7", "#166534"),
        "warn": ("#fef3c7", "#92400e"),
        "danger": ("#fee2e2", "#b91c1c"),
        "dark": ("#e5e7eb", "#111827"),
    }
    bg, fg = palette.get(tone, palette["neutral"])
    return (
        f"<span style='display:inline-block;padding:4px 10px;border-radius:999px;"
        f"background:{bg};color:{fg};font-size:12px;font-weight:700;margin-right:6px;'>"
        f"{_html_escape(str(text or '-'))}</span>"
    )


def _html_section(title: str, body: str, subtitle: str | None = None) -> str:
    subtitle_html = (
        f"<div style='color:#cbd5e1;font-size:13px;margin-top:4px;'>{_html_escape(subtitle)}</div>"
        if subtitle
        else ""
    )
    return (
        "<section style='background:#111827;border:1px solid #334155;border-radius:16px;"
        "padding:20px;margin:18px 0;box-shadow:0 1px 2px rgba(15,23,42,0.35);color:#f8fafc;'>"
        f"<div style='font-size:20px;font-weight:800;color:#f8fafc;margin-bottom:14px;'>{_html_escape(title)}</div>"
        f"{subtitle_html}{body}</section>"
    )


def _html_fact_grid(items: list[tuple[str, Any]]) -> str:
    cards = []
    for label, value in items:
        cards.append(
            "<div style='background:#0f172a;border:1px solid #334155;border-radius:14px;padding:14px;'>"
            f"<div style='font-size:12px;font-weight:700;letter-spacing:0.04em;text-transform:uppercase;color:#94a3b8;margin-bottom:6px;'>{_html_escape(label)}</div>"
            f"<div style='font-size:16px;font-weight:700;color:#f8fafc;line-height:1.35;'>{_format_html_value(value)}</div>"
            "</div>"
        )
    return (
        "<div style='display:grid;grid-template-columns:repeat(auto-fit,minmax(220px,1fr));gap:12px;'>"
        + "".join(cards)
        + "</div>"
    )


def _html_list_block(title: str, items: list[Any], *, tone: str = "neutral", empty: str = "Keine") -> str:
    clean_items = [item for item in items if item not in (None, "", [], {})]
    if not clean_items:
        content = f"<div style='color:#cbd5e1;'>{_html_escape(empty)}</div>"
    else:
        lis = []
        for item in clean_items:
            lis.append(
                "<li style='margin:0 0 10px 0;padding-left:2px;color:#f8fafc;line-height:1.5;'>"
                f"{_format_html_value(item)}</li>"
            )
        content = f"<ul style='margin:10px 0 0 18px;padding:0;'>{''.join(lis)}</ul>"
    accent = {"neutral": "#cbd5e1", "good": "#86efac", "warn": "#fcd34d", "danger": "#fca5a5"}.get(tone, "#cbd5e1")
    return (
        f"<div style='background:#0f172a;border:1px solid #334155;border-left:5px solid {accent};border-radius:12px;padding:14px 16px;'>"
        f"<div style='font-size:14px;font-weight:800;color:#f8fafc;'>{_html_escape(title)}</div>{content}</div>"
    )


def _extract_section_value(case_report: dict[str, Any], *path: str) -> Any:
    node: Any = case_report.get("sections") if isinstance(case_report, dict) else None
    for key in path:
        if not isinstance(node, dict):
            return None
        node = node.get(key)
    if isinstance(node, dict) and "value" in node:
        return node.get("value")
    return node


def _priority_tone(value: Any) -> str:
    raw = str(value or "").strip().lower()
    if raw in {"urgent", "critical"}:
        return "danger"
    if raw == "high":
        return "warn"
    if raw == "medium":
        return "neutral"
    return "good"


def _build_overview_html(run_type: str, payload: dict[str, Any], case_report: dict[str, Any], analysis_brief: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    order_id = result.get("order_id") or payload.get("order_id") or "-"
    status = str(result.get("status") or payload.get("status") or run_type).strip() or "-"
    priority = analysis_brief.get("priority") or result.get("analysis_priority") or "mittel"
    history = result.get("history_sync_count")
    pending = result.get("pending_action_summary") or {}
    pending_total = sum(int(v or 0) for v in pending.values()) if isinstance(pending, dict) else 0
    action_list = analysis_brief.get("internal_actions") if isinstance(analysis_brief.get("internal_actions"), list) else []
    risk_flags = analysis_brief.get("risk_flags") if isinstance(analysis_brief.get("risk_flags"), list) else []
    top_action = "Weiter beobachten"
    if action_list and isinstance(action_list[0], dict):
        top_action = str(action_list[0].get("action") or top_action)

    shipment = _extract_section_value(case_report, "tms_mcp", "shipment") or {}
    route = None
    if isinstance(shipment, dict):
        origin = (shipment.get("origin_city") or {}).get("value") if isinstance(shipment.get("origin_city"), dict) else None
        destination = (shipment.get("destination_city") or {}).get("value") if isinstance(shipment.get("destination_city"), dict) else None
        if origin or destination:
            route = f"{origin or '-'} → {destination or '-'}"
    latest_subjects = _extract_section_value(case_report, "mail_history", "latest_subjects") or []
    missing_docs = _extract_section_value(case_report, "documents", "missing_types") or []
    open_questions = analysis_brief.get("open_questions") or _extract_section_value(case_report, "reconciliation", "open_questions") or []

    header = (
        "<div style='background:linear-gradient(135deg,#0f172a 0%,#1d4ed8 100%);color:#ffffff;"
        "border-radius:20px;padding:24px 24px 18px 24px;box-shadow:0 10px 30px rgba(29,78,216,0.20);'>"
        f"<div style='font-size:28px;font-weight:900;line-height:1.2;margin-bottom:10px;'>CARGOLO ASR – {_html_escape(str(order_id))}</div>"
        f"<div style='margin-bottom:12px;'>{_html_badge('Status ' + status, 'dark')}{_html_badge('Priorität ' + _priority_label(priority), _priority_tone(priority))}{_html_badge('Run ' + str(run_type), 'neutral')}</div>"
        f"<div style='font-size:16px;line-height:1.6;max-width:980px;'>{_format_html_value(analysis_brief.get('ops_summary') or result.get('analysis_summary') or result.get('message') or '-')}</div>"
        "</div>"
    )

    facts = _html_fact_grid([
        ("Route", route or "-"),
        ("Mailhistorie", f"+{history or 0}"),
        ("Offene Punkte", pending_total),
        ("Nächster Schritt", _truncate(top_action, 160)),
    ])

    review_html = ""
    review_links = _build_review_links(str(order_id or "").strip(), "next_step") if str(top_action or "").strip() else []
    if review_links:
        review_html = (
            "<div style='margin-top:14px;padding:14px 16px;background:#0f172a;border:1px solid #60a5fa;border-radius:12px;'>"
            "<div style='font-size:14px;font-weight:800;color:#dbeafe;margin-bottom:8px;'>Review zum vorgeschlagenen nächsten Schritt</div>"
            f"<div style='display:flex;gap:14px;flex-wrap:wrap;'>{''.join(review_links)}</div>"
            "</div>"
        )

    columns = (
        "<div style='display:grid;grid-template-columns:1.25fr 1fr;gap:16px;margin-top:16px;'>"
        + _html_list_block("Wichtigste Risiken", [r.get('reason') or r.get('code') for r in risk_flags[:4] if isinstance(r, dict)], tone="danger", empty="Keine akuten Risiken")
        + _html_list_block("Sofort im Blick behalten", [
            f"Fehlende Dokumente: {', '.join(str(x) for x in missing_docs[:4])}" if missing_docs else None,
            f"Neueste Mail: {latest_subjects[-1]}" if latest_subjects else None,
            f"Offene Fragen: {len(open_questions)}" if open_questions else None,
        ], tone="warn", empty="Keine Zusatzhinweise")
        + "</div>"
    )
    return header + "<div style='margin-top:18px;'>" + facts + review_html + columns + "</div>"


def _render_case_report_sections(sections: dict[str, Any]) -> str:
    parts: list[str] = []
    for section_name, section_body in sections.items():
        title = section_name.replace("_", " ").strip().title()
        body_parts: list[str] = []
        if isinstance(section_body, dict):
            for block_name, block_value in section_body.items():
                subtitle = block_name.replace("_", " ").strip().title()
                body_parts.append(
                    "<div style='margin:14px 0 0 0;padding-top:14px;border-top:1px solid #334155;'>"
                    f"<div style='font-size:15px;font-weight:800;color:#f8fafc;margin-bottom:8px;'>{_html_escape(subtitle)}</div>"
                    f"{_format_html_value(block_value)}</div>"
                )
        else:
            body_parts.append(_format_html_value(section_body))
        parts.append(_html_section(title, "".join(body_parts)))
    return "".join(parts)


def _load_case_report(path_value: Any) -> dict[str, Any]:
    path = str(path_value or "").strip()
    if not path:
        return {}
    try:
        candidate = Path(path)
        if not candidate.exists() or not candidate.is_file():
            return {}
        data = json.loads(candidate.read_text(encoding="utf-8"))
        return data if isinstance(data, dict) else {}
    except Exception:
        logger.debug("Could not load ASR case report from %s", path, exc_info=True)
        return {}


def _build_message(run_type: str, payload: dict[str, Any]) -> str:
    result = payload.get("processor_result") if isinstance(payload.get("processor_result"), dict) else {}
    summary_text = _build_summary_message(run_type, payload)
    case_report = _load_case_report(result.get("case_report_path"))
    analysis_brief = _load_analysis_brief(result.get("analysis_brief_path"))

    html_parts = [
        "<html><body style='margin:0;padding:0;background:#0b1220;font-family:Segoe UI,Arial,sans-serif;color:#f8fafc;'>",
        "<div style='max-width:1180px;margin:0 auto;padding:24px 18px 40px 18px;'>",
        _build_overview_html(run_type, payload, case_report, analysis_brief),
        _html_section("Webhook-Kurzfazit", f"<pre style='margin:0;white-space:pre-wrap;font-family:Consolas,Menlo,monospace;font-size:13px;color:#f8fafc;background:#0f172a;border:1px solid #334155;border-radius:12px;padding:14px;'>{_html_escape(summary_text)}</pre>"),
        "</div></body></html>",
    ]
    return "".join(html_parts)


def build_manual_ops_notification_body(
    *,
    run_type: str,
    payload: dict[str, Any],
    route_name: str = _DEFAULT_ROUTE_NAME,
    delivery_id: str | None = None,
    delivered_at: float | None = None,
) -> dict[str, Any]:
    message = _build_message(run_type, payload)
    message_text = _build_summary_message(run_type, payload)
    return {
        "route": route_name,
        "delivery_id": delivery_id or f"manual-{run_type}-{int(time.time() * 1000)}",
        "delivered_at": delivered_at if delivered_at is not None else time.time(),
        "message": message,
        "message_text": message_text,
        "message_format": "html",
        "payload": {
            "event_type": "cargolo_asr_manual_ops_notification",
            "run_type": run_type,
            **payload,
        },
    }


def send_manual_ops_notification(
    *,
    run_type: str,
    payload: dict[str, Any],
    route_name: str = _DEFAULT_ROUTE_NAME,
    allow_route_fallback: bool = False,
) -> dict[str, Any]:
    targets = _load_targets(route_name=route_name, allow_route_fallback=allow_route_fallback)
    if not targets:
        return {
            "enabled": False,
            "attempted": 0,
            "delivered": 0,
            "errors": [],
            "targets": [],
        }

    timeout_raw = str(os.getenv("HERMES_CARGOLO_ASR_OPS_WEBHOOK_TIMEOUT", "30")).strip()
    timeout = int(timeout_raw) if timeout_raw.isdigit() else 30
    body = build_manual_ops_notification_body(run_type=run_type, payload=payload, route_name=route_name)

    delivered = 0
    errors: list[str] = []
    target_urls: list[str] = []
    for target in targets:
        url = str(target.get("url") or "").strip()
        if not url:
            continue
        target_urls.append(url)
        method = str(target.get("method") or "POST").strip().upper() or "POST"
        headers = dict(target.get("headers") or {})
        headers.setdefault("Content-Type", "application/json")
        try:
            response = requests.request(method, url, json=body, headers=headers, timeout=timeout)
            response.raise_for_status()
            delivered += 1
        except Exception as exc:
            logger.warning("ASR manual ops notification failed for %s: %s", url, exc)
            errors.append(f"{url}: {exc}")

    return {
        "enabled": True,
        "attempted": len(target_urls),
        "delivered": delivered,
        "errors": errors,
        "targets": target_urls,
    }
