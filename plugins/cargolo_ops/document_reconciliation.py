from __future__ import annotations

from typing import Any

from .document_schema import normalize_document_type, normalize_mode


MISSING_ONLY_NOTE = "missing_documents_are_inventory_context_not_risk"


def reconcile_documents(*, order_id: str, tms_snapshot: dict[str, Any], registry: dict[str, Any]) -> dict[str, Any]:
    """Small deterministic reconciliation layer for document monitoring.

    The heavy semantic extraction lives in `document_analysis.py`. This function
    keeps the operator-facing risk logic consistent: missing expected documents
    are inventory context, while unreadable/present-document contradictions drive
    review/risk.
    """
    detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    mode = normalize_mode((detail or {}).get("network") or (detail or {}).get("transport_mode") or tms_snapshot.get("mode"))
    expected = [normalize_document_type(x) for x in registry.get("expected_types", []) if str(x or "").strip()]
    received = [normalize_document_type(x) for x in registry.get("received_types", []) if str(x or "").strip()]
    missing = sorted(set(expected) - set(received))

    findings: list[dict[str, Any]] = []
    for row in registry.get("received_documents", []) or []:
        if not isinstance(row, dict):
            continue
        status = str(row.get("analysis_status") or "").lower()
        if status in {"error", "missing_file"}:
            findings.append({
                "type": "document_unreadable" if status == "error" else "local_file_missing",
                "severity": "medium",
                "filename": row.get("filename"),
                "summary": row.get("analysis_summary") or status,
            })
    for row in registry.get("analyzed_documents", []) or []:
        if not isinstance(row, dict):
            continue
        for flag in row.get("operational_flags") or []:
            findings.append({
                "type": "document_flag",
                "severity": "medium",
                "filename": row.get("filename"),
                "summary": str(flag),
            })
        for item in row.get("missing_or_unreadable") or []:
            findings.append({
                "type": "document_open_question",
                "severity": "low",
                "filename": row.get("filename"),
                "summary": str(item),
            })
    for gap in registry.get("tms_mirroring_gaps", []) or []:
        if isinstance(gap, dict):
            findings.append({
                "type": "tms_mirroring_gap",
                "severity": "medium" if gap.get("mirror_status") == "download_failed" else "low",
                "filename": gap.get("filename") or gap.get("label"),
                "summary": f"TMS upload not mirrored locally: {gap.get('mirror_status')}",
            })

    max_severity = "low"
    if any(row.get("severity") == "high" for row in findings):
        max_severity = "high"
    elif any(row.get("severity") == "medium" for row in findings):
        max_severity = "medium"

    return {
        "version": 1,
        "order_id": order_id,
        "mode": mode,
        "expected_types": sorted(set(expected)),
        "received_types": sorted(set(received)),
        "missing_types": missing,
        "missing_policy": MISSING_ONLY_NOTE,
        "findings": findings,
        "risk": max_severity,
        "needs_human_review": bool(findings),
    }
