"""Local CARGOLO employee runtime loop.

This layer turns the agent-first `EmployeeResponse` into local, read-only work:
- no Teams sends
- no TMS writes
- no customer messages
- optional Honcho memory snapshot as contextual input only
"""

from __future__ import annotations

import html
import json
import re
from pathlib import Path
from typing import Any

from pydantic import BaseModel, ConfigDict, Field

from .employee_agent import BoundaryAction, EmployeeRequest, EmployeeResponse, ResponseMode, handle_employee_request
from .honcho_memory import HonchoMemorySnapshot, unavailable_honcho_snapshot
from .models import utc_now_iso
from .specialist_results import SpecialistResult, SpecialistStatus


class EmployeeRuntimeResult(BaseModel):
    model_config = ConfigDict(extra="allow", use_enum_values=False)

    employee_response: EmployeeResponse
    specialist_results: list[SpecialistResult] = Field(default_factory=list)
    draft_response: str | None = None
    memory_snapshot: HonchoMemorySnapshot = Field(default_factory=unavailable_honcho_snapshot)
    result_path: str | None = None
    should_send_to_teams: bool = False
    should_write_tms: bool = False
    should_send_customer_message: bool = False

    def to_audit_row(self) -> dict[str, Any]:
        return {
            "timestamp": utc_now_iso(),
            "employee_response": self.employee_response.to_audit_row(),
            "specialist_results": [result.to_dict() for result in self.specialist_results],
            "draft_response": self.draft_response,
            "memory_snapshot": self.memory_snapshot.to_dict(),
            "result_path": self.result_path,
            "should_send_to_teams": self.should_send_to_teams,
            "should_write_tms": self.should_write_tms,
            "should_send_customer_message": self.should_send_customer_message,
        }


def _default_root() -> Path:
    return Path.home() / ".hermes" / "cargolo_asr"


def _append_jsonl(path: Path, row: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("a", encoding="utf-8") as handle:
        handle.write(json.dumps(row, ensure_ascii=False, sort_keys=True) + "\n")


def _case_dir(root: Path, order_id: str | None) -> Path | None:
    if not order_id:
        return None
    return root / "orders" / order_id


def _read_text_if_exists(path: Path, *, max_chars: int = 4000) -> str | None:
    try:
        if path.exists() and path.is_file():
            return path.read_text(encoding="utf-8")[:max_chars]
    except UnicodeDecodeError:
        return "<binary/unreadable>"
    return None


def _read_json_if_exists(path: Path) -> dict[str, Any] | list[Any] | None:
    try:
        if not path.exists() or not path.is_file():
            return None
        # Case/TMS artifacts can be larger than the text-preview cap; JSON must
        # be read whole or parsing fails on truncated content.  Keep a generous
        # read-only safety cap so a broken artifact cannot consume unbounded RAM.
        if path.stat().st_size > 2_000_000:
            return None
        parsed = json.loads(path.read_text(encoding="utf-8"))
    except (UnicodeDecodeError, OSError, json.JSONDecodeError):
        return None
    if isinstance(parsed, (dict, list)):
        return parsed
    return None


def _read_jsonl_if_exists(path: Path, *, max_rows: int = 500) -> list[dict[str, Any]]:
    if not path.exists() or not path.is_file():
        return []
    rows: list[dict[str, Any]] = []
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            if len(rows) >= max_rows:
                break
            if not line.strip():
                continue
            try:
                payload = json.loads(line)
            except json.JSONDecodeError:
                continue
            if isinstance(payload, dict):
                rows.append(payload)
    except UnicodeDecodeError:
        return []
    return rows


def _first_existing_json(case_dir: Path, rels: tuple[str, ...]) -> tuple[Path, dict[str, Any] | list[Any]] | None:
    for rel in rels:
        path = case_dir / rel
        parsed = _read_json_if_exists(path)
        if parsed is not None:
            return path, parsed
    return None


def _result(
    *,
    agent: str,
    status: SpecialistStatus,
    summary: str,
    findings: list[dict[str, Any]],
    evidence_refs: list[str] | None = None,
    risks: list[dict[str, Any]] | None = None,
    recommended_actions: list[dict[str, Any]] | None = None,
    confidence: float | None = None,
    requires_human: bool = False,
) -> SpecialistResult:
    return SpecialistResult(
        agent=agent,
        status=status,
        confidence=confidence,
        summary=summary,
        findings=findings,
        risks=risks or [],
        recommended_actions=recommended_actions or [],
        evidence_refs=evidence_refs or [],
        requires_human=requires_human,
        write_intents=[],
    )


def _missing_result(agent: str, *, source: str, evidence_refs: list[str] | None = None) -> SpecialistResult:
    return _result(
        agent=agent,
        status=SpecialistStatus.NEEDS_REVIEW,
        confidence=0.2,
        summary=f"{source} fehlend/nicht lokal verfügbar.",
        findings=[{"source": source, "available": False}],
        risks=[{"type": "missing_local_source", "source": source}],
        recommended_actions=[{"action": "Quelle prüfen/synchronisieren", "source": source}],
        evidence_refs=evidence_refs,
        requires_human=True,
    )


def _execute_case_context(agent: str, case_dir: Path | None) -> SpecialistResult:
    if not case_dir:
        return _missing_result(agent, source="case_folder")
    exists = case_dir.exists()
    evidence_refs = [str(case_dir)] if exists else []
    parsed = _first_existing_json(case_dir, ("case_summary.json", "summary.json", "case.json")) if exists else None
    if parsed and isinstance(parsed[1], dict):
        summary = parsed[1]
        evidence_refs.append(str(parsed[0]))
        return _result(
            agent=agent,
            status=SpecialistStatus.OK,
            confidence=0.75,
            summary="Case folder lokal gelesen.",
            findings=[{"case_folder_exists": True, "path": str(case_dir), "summary": summary}],
            evidence_refs=evidence_refs,
        )
    return _result(
        agent=agent,
        status=SpecialistStatus.OK if exists else SpecialistStatus.NEEDS_REVIEW,
        confidence=0.55 if exists else 0.2,
        summary="Case folder vorhanden." if exists else "Case folder fehlend/nicht lokal verfügbar.",
        findings=[{"case_folder_exists": exists, "path": str(case_dir)}],
        risks=[] if exists else [{"type": "missing_case_folder"}],
        evidence_refs=evidence_refs,
        requires_human=not exists,
    )


def _normalize_messages(payload: dict[str, Any] | list[Any]) -> list[dict[str, Any]]:
    if isinstance(payload, list):
        return [item for item in payload if isinstance(item, dict)]
    for key in ("messages", "emails", "mail_history", "items"):
        values = payload.get(key)
        if isinstance(values, list):
            return [item for item in values if isinstance(item, dict)]
    return []


def _execute_mail_history(agent: str, case_dir: Path | None) -> SpecialistResult:
    if not case_dir:
        return _missing_result(agent, source="mail_history")
    parsed = _first_existing_json(case_dir, ("mail/history.json", "mail_history.json", "mail/messages.json"))
    if parsed:
        path, payload = parsed
        messages = _normalize_messages(payload)
        latest = messages[-1] if messages else {}
        finding = {
            "source": str(path.relative_to(case_dir)),
            "message_count": len(messages),
            "latest_subject": latest.get("subject"),
            "latest_from": latest.get("from") or latest.get("sender"),
            "latest_preview": (latest.get("body") or latest.get("text") or "")[:240],
        }
        status = SpecialistStatus.OK if messages else SpecialistStatus.NEEDS_REVIEW
        return _result(
            agent=agent,
            status=status,
            confidence=0.75 if messages else 0.35,
            summary=f"Mail-Historie lokal gelesen ({len(messages)} Nachrichten)." if messages else "Mail-Historie ohne Nachrichten gelesen.",
            findings=[finding],
            evidence_refs=[str(path)],
            requires_human=not messages,
        )
    for rel in ("mail/history.md", "mail_history.md"):
        path = case_dir / rel
        content = _read_text_if_exists(path)
        if content is not None:
            return _result(
                agent=agent,
                status=SpecialistStatus.OK,
                confidence=0.55,
                summary="Mail-Historie als Text lokal gelesen.",
                findings=[{"source": rel, "preview": content[:500]}],
                evidence_refs=[str(path)],
            )
    email_index = case_dir / "email_index.jsonl"
    rows = _read_jsonl_if_exists(email_index)
    if rows:
        latest = rows[-1]
        return _result(
            agent=agent,
            status=SpecialistStatus.OK,
            confidence=0.7,
            summary=f"Mail-Historie aus email_index gelesen ({len(rows)} Nachrichten).",
            findings=[{
                "source": "email_index.jsonl",
                "message_count": len(rows),
                "latest_subject": latest.get("subject"),
                "latest_from": latest.get("sender") or latest.get("from"),
                "latest_received_at": latest.get("received_at"),
                "latest_stored_paths": latest.get("stored_paths") if isinstance(latest.get("stored_paths"), list) else [],
            }],
            evidence_refs=[str(email_index)],
        )
    return _missing_result(agent, source="mail_history")


def _execute_tms_snapshot(agent: str, case_dir: Path | None) -> SpecialistResult:
    if not case_dir:
        return _missing_result(agent, source="tms_snapshot")
    parsed = _first_existing_json(case_dir, ("tms_snapshot.json", "tms/snapshot.json", "tms.json"))
    if parsed and isinstance(parsed[1], dict):
        path, snapshot = parsed
        return _result(
            agent=agent,
            status=SpecialistStatus.OK,
            confidence=0.8,
            summary="TMS Snapshot lokal gelesen.",
            findings=[{"source": str(path.relative_to(case_dir)), "snapshot": snapshot}],
            evidence_refs=[str(path)],
        )
    for rel in ("tms_snapshot.md", "tms/snapshot.md"):
        path = case_dir / rel
        content = _read_text_if_exists(path)
        if content is not None:
            return _result(
                agent=agent,
                status=SpecialistStatus.OK,
                confidence=0.5,
                summary="TMS Snapshot als Text lokal gelesen.",
                findings=[{"source": rel, "preview": content[:500]}],
                evidence_refs=[str(path)],
            )
    detail = _first_existing_json(case_dir, ("tms/shipment_detail.json", "tms/detail.json"))
    billing = _first_existing_json(case_dir, ("tms/billing_context.json",))
    requirements = _first_existing_json(case_dir, ("tms/document_requirements.json",))
    if detail and isinstance(detail[1], dict):
        snapshot = {"detail": detail[1]}
        refs = [str(detail[0])]
        if billing and isinstance(billing[1], dict):
            snapshot["billing_context"] = billing[1]
            refs.append(str(billing[0]))
        if requirements and isinstance(requirements[1], dict):
            snapshot["document_requirements"] = requirements[1]
            refs.append(str(requirements[0]))
        return _result(
            agent=agent,
            status=SpecialistStatus.OK,
            confidence=0.7,
            summary="TMS Detail-Kontext lokal gelesen.",
            findings=[{"source": str(detail[0].relative_to(case_dir)), "snapshot": snapshot}],
            evidence_refs=refs,
        )
    return _missing_result(agent, source="tms_snapshot")


def _execute_document_analyst(agent: str, case_dir: Path | None) -> SpecialistResult:
    if not case_dir:
        return _missing_result(agent, source="documents")
    docs_dir = case_dir / "documents"
    legacy_docs_dir = case_dir / "docs"
    evidence_refs: list[str] = [str(path) for path in (docs_dir, legacy_docs_dir) if path.exists()]
    parsed = _first_existing_json(case_dir, ("documents/analysis/latest_summary.json", "docs/analysis.json", "documents/analysis.json", "document_analysis.json"))
    if parsed and isinstance(parsed[1], dict):
        path, analysis = parsed
        documents = analysis.get("documents") if isinstance(analysis.get("documents"), list) else []
        missing = analysis.get("missing") or analysis.get("missing_documents") or []
        discrepancies = analysis.get("discrepancies") or analysis.get("issues") or []
        operational_flags = [flag for doc in documents if isinstance(doc, dict) for flag in (doc.get("operational_flags") or [])]
        unreadable_or_missing = [item for doc in documents if isinstance(doc, dict) for item in (doc.get("missing_or_unreadable") or [])]
        if not missing and unreadable_or_missing:
            missing = sorted({str(item) for item in unreadable_or_missing if item})[:10]
        if not discrepancies and operational_flags:
            discrepancies = sorted({str(item) for item in operational_flags if item})[:10]
        if not isinstance(missing, list):
            missing = [str(missing)]
        if not isinstance(discrepancies, list):
            discrepancies = [str(discrepancies)]
        evidence_refs.append(str(path))
        needs_review = bool(missing or discrepancies)
        return _result(
            agent=agent,
            status=SpecialistStatus.NEEDS_REVIEW if needs_review else SpecialistStatus.OK,
            confidence=0.75,
            summary="Dokumenten-Analyse lokal gelesen." if not needs_review else "Dokumentenlage mit offenen Punkten gelesen.",
            findings=[{"source": str(path.relative_to(case_dir)), "document_count": len(documents), "missing": missing, "discrepancies": discrepancies, "analysis": analysis}],
            risks=[{"type": "document_gap", "missing": missing, "discrepancies": discrepancies}] if needs_review else [],
            recommended_actions=[{"action": "Dokumentenlücke klären", "missing": missing, "discrepancies": discrepancies}] if needs_review else [],
            evidence_refs=evidence_refs,
            requires_human=needs_review,
        )
    files: list[str] = []
    for base, rels in ((docs_dir, ("inbound", "tms")), (legacy_docs_dir, ("",))):
        for rel in rels:
            folder = (base / rel) if rel else base
            if folder.exists():
                files.extend(p.name for p in folder.iterdir() if p.is_file())
    files = sorted(files)
    if files:
        return _result(
            agent=agent,
            status=SpecialistStatus.NEEDS_REVIEW,
            confidence=0.45,
            summary=f"Dokumentenordner lokal gelesen ({len(files)} Dateien), aber keine Analyse vorhanden.",
            findings=[{"docs_dir_exists": True, "files": files[:20], "analysis_available": False}],
            risks=[{"type": "missing_document_analysis", "files": files[:20]}],
            recommended_actions=[{"action": "Dokumenten-Analyse/Abgleich nachziehen", "files": files[:20]}],
            evidence_refs=evidence_refs,
            requires_human=True,
        )
    return _missing_result(agent, source="documents", evidence_refs=evidence_refs)


def _execute_stub_specialist(task: dict[str, Any], *, root: Path) -> SpecialistResult:
    agent = str(task.get("agent") or "unknown")
    order_id = task.get("order_id")
    case_dir = _case_dir(root, order_id)

    if agent == "case_context":
        return _execute_case_context(agent, case_dir)
    if agent == "mail_history":
        return _execute_mail_history(agent, case_dir)
    if agent == "tms_snapshot":
        return _execute_tms_snapshot(agent, case_dir)
    if agent == "document_analyst":
        return _execute_document_analyst(agent, case_dir)

    return _result(
        agent=agent,
        status=SpecialistStatus.NEEDS_REVIEW,
        confidence=0.1,
        summary=f"Specialist {agent} ist noch nicht implementiert/nicht lokal verfügbar.",
        findings=[{"placeholder": True, "agent": agent, "implemented": False}],
        risks=[{"type": "unimplemented_specialist", "agent": agent}],
        recommended_actions=[{"action": "Read-only Kontextquelle/Specialist anbinden", "agent": agent}],
        requires_human=True,
    )


def _first_result(results: list[SpecialistResult], agent: str) -> SpecialistResult | None:
    return next((result for result in results if result.agent == agent), None)


def _short_list(values: Any, *, limit: int = 3) -> tuple[list[str], int]:
    if not isinstance(values, list):
        values = [values] if values else []
    cleaned = [str(item).strip() for item in values if str(item).strip()]
    return cleaned[:limit], max(0, len(cleaned) - limit)


def _fmt_short_list(values: Any, *, limit: int = 3, empty: str = "nichts Konkretes offen") -> str:
    shown, remaining = _short_list(values, limit=limit)
    if not shown:
        return empty
    suffix = f" (+{remaining} weitere)" if remaining else ""
    return "; ".join(shown) + suffix


def _html_list(values: Any, *, limit: int = 3, empty: str = "Keine konkreten Punkte sichtbar.") -> str:
    shown, remaining = _short_list(values, limit=limit)
    if not shown:
        return f"<p>{html.escape(empty)}</p>"
    items = "".join(f"<li>{html.escape(item)}</li>" for item in shown)
    if remaining:
        items += f"<li>… plus {remaining} weitere Punkte im Detail.</li>"
    return f"<ul>{items}</ul>"


def _dig(data: Any, *path: str) -> Any:
    current = data
    for key in path:
        if not isinstance(current, dict):
            return None
        current = current.get(key)
    return current


def _compact_value(value: Any) -> str | None:
    if value is None:
        return None
    text = str(value).strip()
    if not text or text in {"0", "0.0", "None", "null"}:
        return None
    return text


def _extract_mrn_candidates_from_analysis(analysis: dict[str, Any]) -> list[str]:
    pattern = re.compile(r"\b\d{2}DE[A-Z0-9]{5,}\b", re.IGNORECASE)
    values: list[str] = []
    for doc in analysis.get("documents") or []:
        if not isinstance(doc, dict):
            continue
        if str(doc.get("doc_type") or "") != "customs_document":
            continue
        haystack = " ".join(
            str(part or "")
            for part in (
                doc.get("filename"),
                doc.get("summary"),
                " ".join(doc.get("operational_flags") or []),
            )
        )
        for match in pattern.findall(haystack):
            candidate = match.upper()
            if candidate not in values:
                values.append(candidate)
    return values


def _summarise_key_documents(analysis: dict[str, Any]) -> list[str]:
    wanted = {"commercial_invoice", "packing_list", "customs_document", "bill_of_lading"}
    priority = {
        "customs_document": 0,
        "commercial_invoice": 1,
        "packing_list": 2,
        "bill_of_lading": 3,
    }
    docs = [doc for doc in (analysis.get("documents") or []) if isinstance(doc, dict) and str(doc.get("doc_type") or "") in wanted]
    docs.sort(key=lambda doc: (priority.get(str(doc.get("doc_type")), 9), "proforma" in str(doc.get("summary") or "").lower(), str(doc.get("filename") or "")))
    bullets: list[str] = []
    for doc in docs:
        summary = _compact_value(doc.get("summary"))
        filename = _compact_value(doc.get("filename"))
        if not summary:
            continue
        label = {
            "commercial_invoice": "Handelsrechnung",
            "packing_list": "Packliste",
            "customs_document": "Zollbeleg",
            "bill_of_lading": "Frachtpapier",
        }.get(str(doc.get("doc_type")), "Dokument")
        source = f" ({filename})" if filename else ""
        bullet = f"{label}{source}: {summary}"
        if bullet not in bullets:
            bullets.append(bullet)
    return bullets[:5]


def _build_decision_context(
    *,
    snapshot: dict[str, Any] | None,
    analysis: dict[str, Any] | None,
    pending_target: str | None,
    pending_value: str | None,
    raw_missing: Any,
    raw_discrepancies: Any,
) -> dict[str, Any]:
    snapshot = snapshot if isinstance(snapshot, dict) else {}
    detail = snapshot.get("detail") if isinstance(snapshot.get("detail"), dict) else snapshot
    analysis = analysis if isinstance(analysis, dict) else {}

    current_mrn = _compact_value(_dig(detail, "customs", "customs_reference")) or _compact_value(snapshot.get("customs_reference"))
    customs_status = _compact_value(_dig(detail, "customs", "customs_status"))
    doc_mrns = _extract_mrn_candidates_from_analysis(analysis)
    container = _compact_value(_dig(detail, "freight_details", "container_number"))
    hbl = _compact_value(_dig(detail, "freight_details", "hbl_number"))
    container_type = _compact_value(_dig(detail, "freight_details", "container_type"))
    pol = _compact_value(_dig(detail, "freight_details", "pol_code"))
    pod = _compact_value(_dig(detail, "freight_details", "pod_code"))
    pieces = _compact_value(_dig(detail, "totals", "total_packages"))
    weight = _compact_value(_dig(detail, "totals", "total_weight_kg"))
    volume = _compact_value(_dig(detail, "totals", "total_volume_m3"))
    cargo_desc = None
    cargo_rows = detail.get("cargo") if isinstance(detail.get("cargo"), list) else []
    if cargo_rows and isinstance(cargo_rows[0], dict):
        cargo_desc = _compact_value(cargo_rows[0].get("goods_description"))
    delivery_notes = _compact_value(_dig(detail, "delivery", "delivery_restrictions")) or _compact_value(_dig(detail, "parties", "recipient", "notes"))

    decision: list[str] = []
    green: list[str] = []
    open_items: list[str] = []

    if pending_target and "customs_reference" in str(pending_target):
        if current_mrn and pending_value and current_mrn != pending_value:
            decision.append(f"MRN-Freigabe ist nicht deckungsgleich: offen angefragt ist {pending_value}, im TMS steht aktuell {current_mrn}.")
        elif pending_value:
            green.append(f"Offene MRN {pending_value} entspricht dem aktuellen TMS-Wert.")
        if doc_mrns and pending_value and pending_value not in doc_mrns:
            decision.append("Zollbeleg verweist auf " + ", ".join(doc_mrns) + f"; die freizugebende MRN {pending_value} taucht dort nicht auf.")
        elif doc_mrns:
            green.append("MRN aus Zollbeleg erkannt: " + ", ".join(doc_mrns) + ".")
        if customs_status:
            green.append(f"TMS-Zollstatus: {customs_status}.")

    if container or hbl:
        parts = []
        if hbl:
            parts.append(f"HBL {hbl}")
        if container:
            parts.append(f"Container {container}" + (f" ({container_type})" if container_type else ""))
        if pol or pod:
            parts.append(f"Routing {pol or '?'} → {pod or '?'}")
        green.append("Transport-Identifikation ist vorhanden: " + ", ".join(parts) + ".")

    if pieces or weight or volume:
        green.append(
            "Sendungsdaten: "
            + ", ".join(part for part in (f"{pieces} Packstücke" if pieces else None, f"{weight} kg" if weight else None, f"{volume} m³" if volume else None) if part)
            + (f"; Ware: {cargo_desc}" if cargo_desc else "")
            + "."
        )
    if delivery_notes:
        green.append(f"Nachlauf/Zustellung: {delivery_notes}")

    missing_types = analysis.get("missing_types")
    if isinstance(missing_types, list) and not missing_types:
        green.append("Pflichtdokumenttypen laut Analyse vollständig: Handelsrechnung, Packliste und Zollbeleg liegen vor.")

    raw_open = _short_list(raw_missing, limit=12)[0]
    low_signal_terms = ("carrier", "container", "eta", "etd", "hbl", "mbl", "pol", "pod")
    for item in raw_open:
        lower = item.lower()
        if any(term in lower for term in low_signal_terms) and (container or hbl or pol or pod):
            continue
        if "mrn" in lower and (current_mrn or doc_mrns):
            continue
        if item not in open_items:
            open_items.append(item)

    flags = [item for item in _short_list(raw_discrepancies, limit=10)[0] if item not in green]
    should_hold = bool(decision or open_items or flags)
    if decision:
        recommendation = "Nicht freigeben, bevor die MRN-Quelle geklärt ist."
        next_step = "MRN aus Zollbeleg/TMS gegen die gewünschte Freigabe abgleichen; bei Abweichung korrigieren statt bestätigen."
    elif open_items or flags:
        recommendation = "Nicht blind freigeben; die harten Identifikatoren sind da, aber einzelne Dokumentpunkte brauchen fachlichen Check."
        next_step = "Nur die entscheidungsrelevanten Dokumentpunkte prüfen, nicht die internen Transportaufträge als Blocker behandeln."
    else:
        recommendation = "Aus den lokalen Quellen sehe ich keinen fachlichen Blocker für die Freigabe."
        next_step = "Wenn der Wert fachlich passt, kann die Freigabe bestätigt werden."

    return {
        "decision": decision[:3],
        "green": green[:5],
        "open_items": open_items[:4],
        "flags": flags[:4],
        "key_docs": _summarise_key_documents(analysis),
        "recommendation": recommendation,
        "next_step": next_step,
        "should_hold": should_hold,
    }


def _human_field_label(value: str | None) -> str:
    raw = str(value or "").strip()
    labels = {
        "customs_reference": "Zollreferenz / MRN",
        "shipment.customs.customs_reference": "Zollreferenz / MRN",
        "hbl_number": "HBL",
        "mbl_number": "MBL",
        "hawb_number": "HAWB",
        "container_number": "Container-Nr.",
        "tracking_number": "Tracking-Nr.",
    }
    return labels.get(raw, raw or "Feld")


def _extract_pending_action_from_request(request: EmployeeRequest) -> tuple[str | None, str | None]:
    target = getattr(request, "pending_action_target", None)
    value = getattr(request, "pending_action_value", None)
    if target or value:
        return (str(target).strip() or None, str(value).strip() or None)
    # Backwards-compatible fallback for callers that only pass text.
    match = re.search(r"Offene Freigabe:\s*([^=:.]+)\s*=\s*([^\n.]+)", request.text or "", re.IGNORECASE)
    if match:
        return match.group(1).strip(), match.group(2).strip()
    return None, None


def _format_case_lage(response: EmployeeResponse, results: list[SpecialistResult], request: EmployeeRequest) -> str:
    order_id = response.order_id or "unbekannte AN"
    lines: list[str] = []

    route_bits: list[str] = []
    case = _first_result(results, "case_context")
    if case and case.findings:
        summary = case.findings[0].get("summary")
        if isinstance(summary, dict):
            for key in ("mode", "lane"):
                value = summary.get(key)
                if value:
                    route_bits.append(str(value))

    tms_status = None
    pickup = None
    tms_snapshot: dict[str, Any] | None = None
    tms = _first_result(results, "tms_snapshot")
    if tms and tms.findings:
        snapshot = tms.findings[0].get("snapshot")
        if isinstance(snapshot, dict):
            tms_snapshot = snapshot
            detail = snapshot.get("detail") if isinstance(snapshot.get("detail"), dict) else {}
            dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
            tms_status = snapshot.get("status") or detail.get("status") or detail.get("shipment_status")
            pickup = snapshot.get("pickup_date") or snapshot.get("pickup") or detail.get("pickup_date") or detail.get("pickup") or dates.get("pickup_date")

    mail_count = None
    latest_subject = None
    latest_from = None
    mail = _first_result(results, "mail_history")
    if mail and mail.findings:
        mail_count = mail.findings[0].get("message_count")
        latest_subject = mail.findings[0].get("latest_subject")
        latest_from = mail.findings[0].get("latest_from")

    docs_count = None
    missing: Any = []
    discrepancies: Any = []
    docs_analysis: dict[str, Any] | None = None
    docs = _first_result(results, "document_analyst")
    if docs and docs.findings:
        docs_count = docs.findings[0].get("document_count")
        missing = docs.findings[0].get("missing")
        discrepancies = docs.findings[0].get("discrepancies")
        analysis = docs.findings[0].get("analysis")
        if isinstance(analysis, dict):
            docs_analysis = analysis

    title = html.escape(order_id)
    route_text = f" · {html.escape(' / '.join(route_bits))}" if route_bits else ""

    status_html = html.escape(str(tms_status or "unbekannt"))
    pickup_html = f"<br><small>Pickup: {html.escape(str(pickup))}</small>" if pickup else ""
    mail_html = html.escape(str(mail_count if mail_count is not None else "–"))
    docs_html = html.escape(str(docs_count if docs_count is not None else "–"))

    latest_mail = ""
    if latest_subject:
        latest_mail = f"<br><small>Letzte Mail: „{html.escape(str(latest_subject))}“"
        if latest_from:
            latest_mail += f" von {html.escape(str(latest_from))}"
        latest_mail += "</small>"

    missing_sources = [result.agent for result in results if result.status == SpecialistStatus.NEEDS_REVIEW and any(risk.get("type") == "missing_local_source" for risk in result.risks)]
    source_note = ""
    if missing_sources:
        source_note = "<p><strong>Lokal fehlend/nicht lokal verfügbar:</strong> " + html.escape(", ".join(missing_sources)) + ".</p>"

    target, value = _extract_pending_action_from_request(request)
    field_label = _human_field_label(target)
    release_line = ""
    if target or value:
        release_line = (
            "<p><strong>Offene Freigabe:</strong> "
            f"{html.escape(field_label)} = <code>{html.escape(str(value or 'Wert'))}</code>. "
            "Ich habe daran nichts verändert.</p>"
        )

    decision_context = _build_decision_context(
        snapshot=tms_snapshot,
        analysis=docs_analysis,
        pending_target=target,
        pending_value=value,
        raw_missing=missing,
        raw_discrepancies=discrepancies,
    )
    recommendation = html.escape(str(decision_context["recommendation"]))
    next_step = html.escape(str(decision_context["next_step"]))

    visible_green = decision_context["green"][:3]
    visible_findings = (
        list(decision_context["decision"])
        + list(decision_context["open_items"])
        + list(decision_context["flags"])
    )[:3]
    visible_docs = decision_context["key_docs"][:2]
    if visible_docs:
        visible_green.extend(visible_docs[: max(0, 3 - len(visible_green))])

    green_html = _html_list(visible_green, limit=3, empty="Keine belastbaren Zusatzpunkte in den lokalen Quellen sichtbar.")
    findings_html = _html_list(visible_findings, limit=3, empty="Keine akuten operativen Blocker aus TMS/Mail/Dokumenten sichtbar.")

    body = f"""
<div>
  <h2>🔎 Fallprüfung {title}{route_text}</h2>
  <p><strong>Lage:</strong> TMS {status_html}; Mails {mail_html}; Dokumente {docs_html}.{pickup_html}</p>
  {latest_mail and f'<p><strong>Letzte Mail:</strong> {html.escape(str(latest_subject))}' + (f' von {html.escape(str(latest_from))}' if latest_from else '') + '</p>'}

  <h3>Auffällig</h3>
  {findings_html}
  {source_note}

  <h3>Belastbar</h3>
  {green_html}

  <h3>Empfehlung</h3>
  <p>{recommendation}</p>
  {release_line}

  <h3>Nächster Schritt</h3>
  <p>{next_step}</p>
</div>
"""
    return "".join(line.strip() for line in body.splitlines() if line.strip())


def _format_business_draft(response: EmployeeResponse, results: list[SpecialistResult], request: EmployeeRequest) -> str:
    order_id = response.order_id or "unbekannte Sendung"
    text_lower = (request.text or "").lower()
    recipient = "Dienstleister/Partner" if any(token in text_lower for token in ("dienstleister", "partner", "carrier", "reederei", "spedition")) else "Kunde"

    tms_status = None
    pickup = None
    docs_missing: Any = []
    latest_subject = None
    latest_from = None

    tms = _first_result(results, "tms_snapshot")
    if tms and tms.findings:
        snapshot = tms.findings[0].get("snapshot")
        if isinstance(snapshot, dict):
            detail = snapshot.get("detail") if isinstance(snapshot.get("detail"), dict) else {}
            dates = detail.get("dates") if isinstance(detail.get("dates"), dict) else {}
            tms_status = snapshot.get("status") or detail.get("status") or detail.get("shipment_status")
            pickup = snapshot.get("pickup_date") or detail.get("pickup_date") or dates.get("pickup_date")

    mail = _first_result(results, "mail_history")
    if mail and mail.findings:
        latest_subject = mail.findings[0].get("latest_subject")
        latest_from = mail.findings[0].get("latest_from")

    docs = _first_result(results, "document_analyst")
    if docs and docs.findings:
        docs_missing = docs.findings[0].get("missing")

    facts: list[str] = []
    if tms_status:
        facts.append(f"aktueller TMS-Status: {tms_status}")
    if pickup:
        facts.append(f"Pickup: {pickup}")
    missing_text = _fmt_short_list(docs_missing, limit=3, empty="keine konkrete Dokumentenlücke aus lokaler Analyse")
    if latest_subject:
        mail_part = f"letzte Mail: {latest_subject}"
        if latest_from:
            mail_part += f" von {latest_from}"
        facts.append(mail_part)

    fact_sentence = "; ".join(facts) if facts else "lokal liegen noch nicht genug belastbare Fakten für Details vor"
    subject = f"Status zu {order_id}"
    greeting = "Guten Tag," if recipient == "Kunde" else "Hallo zusammen,"
    draft_lines = [
        greeting,
        "",
        f"zu {order_id} ein kurzes Update: {fact_sentence}.",
        f"Dokumentenstand aus unserer lokalen Prüfung: {missing_text}.",
        "",
        "Wir prüfen die offenen Punkte intern weiter und melden uns, sobald der nächste belastbare Schritt feststeht.",
        "",
        "Viele Grüße",
        "CARGOLO ASR",
    ]
    draft_body = "<br>".join(html.escape(line) for line in draft_lines)
    source_note = ""
    missing_sources = [result.agent for result in results if result.status == SpecialistStatus.NEEDS_REVIEW and any(risk.get("type") == "missing_local_source" for risk in result.risks)]
    if missing_sources:
        source_note = "<p><small>Hinweis: Lokal fehlend/nicht verfügbar: " + html.escape(", ".join(missing_sources)) + ". Entwurf daher vor Versand fachlich prüfen.</small></p>"

    return (
        "<div>"
        f"<h2>✍️ Entwurf für {html.escape(recipient)} · {html.escape(order_id)}</h2>"
        "<p><strong>Nicht gesendet.</strong> Kein Kunden-/Partnerkontakt wurde ausgelöst.</p>"
        f"<p><strong>Betreff:</strong> {html.escape(subject)}</p>"
        f"<p>{draft_body}</p>"
        f"{source_note}"
        "<p><small>Draft-only ausgeführt: keine Mail gesendet, kein TMS-Write.</small></p>"
        "</div>"
    )


def _draft_for(response: EmployeeResponse, results: list[SpecialistResult], memory: HonchoMemorySnapshot, request: EmployeeRequest) -> str:
    # Honcho is currently kept outside the critical path. Only mention it when a caller explicitly supplied usable context.
    memory_note = " Honcho-Kontext berücksichtigt." if memory.available else ""
    if response.mode == ResponseMode.FREE_CHAT:
        return "Normale Hermes-Antwort möglich; keine CARGOLO-Side-Effects vorbereitet." + memory_note
    if response.mode == ResponseMode.DRAFT_ONLY:
        return _format_business_draft(response, results, request) + memory_note
    if response.mode == ResponseMode.GUARDED_ACTION_REQUIRED:
        if response.boundary_action == BoundaryAction.TEAMS_SEND:
            return "Teams Guard erforderlich; keine Teams-Nachricht gesendet. Draft/Review bleibt möglich." + memory_note
        if response.boundary_action == BoundaryAction.TMS_WRITE:
            return "TMS Guard erforderlich; keine TMS-Änderung ausgeführt. Draft/Review bleibt möglich." + memory_note
        action = response.boundary_action.value if response.boundary_action else "unknown"
        return f"Guard erforderlich für {action}; keine externe Aktion ausgeführt. Draft/Review bleibt möglich." + memory_note
    return _format_case_lage(response, results, request) + memory_note


def _read_marker_specialists(marker_path: Path) -> set[str]:
    parsed = _read_json_if_exists(marker_path)
    if not isinstance(parsed, dict):
        return set()
    specialists = parsed.get("specialists")
    if not isinstance(specialists, list):
        return set()
    return {str(item) for item in specialists if item}


def _sync_review_marker(case_root: Path, response: EmployeeResponse, results: list[SpecialistResult], draft_response: str) -> None:
    if response.mode != ResponseMode.CASE_ASSIST or not response.order_id:
        return

    marker_path = case_root / "orders" / response.order_id / "employee" / "review_required.json"
    review_results = [result for result in results if result.status == SpecialistStatus.NEEDS_REVIEW or result.requires_human]
    if not review_results:
        current_scope = {result.agent for result in results}
        marker_scope = _read_marker_specialists(marker_path)
        can_clear_marker = marker_path.exists() and (not marker_scope or marker_scope.issubset(current_scope))
        if can_clear_marker:
            marker_path.unlink()
        return

    marker_path.parent.mkdir(parents=True, exist_ok=True)
    current_scope = {result.agent for result in results}
    existing_scope = _read_marker_specialists(marker_path)
    current_review_scope = {result.agent for result in review_results}
    unresolved_out_of_scope = existing_scope - current_scope
    marker_specialists = sorted(current_review_scope | unresolved_out_of_scope)
    payload = {
        "timestamp": utc_now_iso(),
        "source": "employee_runtime",
        "order_id": response.order_id,
        "requires_human": True,
        "specialists": marker_specialists,
        "summary": draft_response,
        "risks": [risk for result in review_results for risk in result.risks],
        "recommended_actions": [action for result in review_results for action in result.recommended_actions],
        "evidence_refs": [ref for result in review_results for ref in result.evidence_refs],
        "should_send_to_teams": False,
        "should_write_tms": False,
        "should_send_customer_message": False,
    }
    marker_path.write_text(json.dumps(payload, ensure_ascii=False, sort_keys=True, indent=2) + "\n", encoding="utf-8")


def run_employee_runtime(
    request: EmployeeRequest,
    *,
    root: Path | None = None,
    memory_snapshot: HonchoMemorySnapshot | None = None,
) -> EmployeeRuntimeResult:
    """Run the local employee loop without external side effects."""

    case_root = root or _default_root()
    memory = memory_snapshot or unavailable_honcho_snapshot()
    response = handle_employee_request(request)
    results: list[SpecialistResult] = []
    result_path: Path | None = None

    if response.mode in (ResponseMode.CASE_ASSIST, ResponseMode.DRAFT_ONLY):
        for task in response.specialist_plan.tasks:
            if task.get("mode") != "read_only":
                continue
            results.append(_execute_stub_specialist(task, root=case_root))

    if response.mode == ResponseMode.CASE_ASSIST:
        result_path = case_root / "orders" / str(response.order_id) / "employee" / "specialist_results.jsonl"
        for row in results:
            _append_jsonl(result_path, row.to_dict())


    draft_response = _draft_for(response, results, memory, request)
    _sync_review_marker(case_root, response, results, draft_response)

    return EmployeeRuntimeResult(
        employee_response=response,
        specialist_results=results,
        draft_response=draft_response,
        memory_snapshot=memory,
        result_path=str(result_path) if result_path else None,
        should_send_to_teams=False,
        should_write_tms=False,
        should_send_customer_message=False,
    )
