#!/usr/bin/env python3
"""Hermes-backed agent review hook for CARGOLO document monitor.

Reads an agent evidence packet JSON on stdin and returns a compact JSON review
on stdout. The caller remains responsible for safety guardrails and writeback;
this script only produces wording/decision guidance.
"""
from __future__ import annotations

import json
import os
import re
import subprocess
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parents[1]
HERMES_BIN = os.environ.get("HERMES_BIN") or str(REPO_ROOT / "venv-py312" / "bin" / "hermes")
HERMES_REVIEW_TOOLSETS = os.environ.get("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_TOOLSETS", "no_tools")


def _review_timeout() -> int:
    try:
        return max(1, int(os.environ.get("HERMES_CARGOLO_DOCUMENT_AGENT_REVIEW_TIMEOUT", "90") or 90))
    except Exception:
        return 90


def _extract_json(text: str) -> dict | None:
    raw = (text or "").strip()
    if not raw:
        return None
    candidates = [raw]
    fence = re.search(r"```(?:json)?\s*(\{.*?\})\s*```", raw, flags=re.S | re.I)
    if fence:
        candidates.insert(0, fence.group(1))
    start = raw.find("{")
    end = raw.rfind("}")
    if start >= 0 and end > start:
        candidates.append(raw[start : end + 1])
    for candidate in candidates:
        try:
            parsed = json.loads(candidate)
        except Exception:
            continue
        if isinstance(parsed, dict):
            return parsed
    return None


def main() -> int:
    try:
        packet = json.loads(sys.stdin.read() or "{}")
    except Exception as exc:
        print(json.dumps({"error": f"invalid input json: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 2
    if not isinstance(packet, dict) or packet.get("contract") != "agent_first_document_review_v1":
        print(json.dumps({"error": "unsupported packet"}, ensure_ascii=False), file=sys.stderr)
        return 2

    compact_packet = json.dumps(packet, ensure_ascii=False, separators=(",", ":"))
    prompt = f"""
Du bist Hermes als interner CARGOLO-ASR-Mitarbeiter im Document Monitor.
Bewerte NUR das folgende Evidenzpaket. Nicht raten. Keine TMS-Änderung, keine Kundenmail, keine externen Tools.
Schreibe keine Debugpfade/ASRCTX/Dateipfade in die Antwort.

Gib ausschließlich valides JSON zurück, genau in dieser Form:
{{
  "sections": {{
    "lage": "1 kurzer deutscher Satz",
    "abgleich": "konkreter TMS-Dokument-Abgleich oder klarer Vorbehalt",
    "auffaellig": "wichtigste Auffälligkeit oder unauffällig",
    "empfehlung": "operative Empfehlung, menschlich und knapp",
    "naechster_schritt": "konkreter nächster Schritt"
  }},
  "decision": "no_action|observe|manual_review|queue_review_card",
  "priority": "low|medium|high|critical",
  "needs_review": true/false,
  "confidence": "low|medium|high"
}}

Safety-Regeln:
- Wenn safe_tms_review_intents vorhanden sind: nicht verstecken; decision mindestens queue_review_card oder manual_review.
- Wenn safe_tms_review_intents leer sind: KEINE Review-Karte, Freigabe-Karte oder TMS-Karte erwähnen oder anfordern; decision darf dann nicht queue_review_card sein. Schreibe stattdessen "fachlich/manuell prüfen" oder "im TMS gegenprüfen".
- Review-Karten gibt es nur für sichere Zielwerte aus safe_tms_review_intents. Direkt schreibbar sind mbl_number, hbl_number, hawb_number, container_number, customs_reference, estimated_delivery_date und actual_delivery_date. etd_main_carriage/atd_main_carriage sind nur fachliche Review-only-Karten ohne direkten TMS-Write. Für POL, POD, Gewicht, Packstücke, Vessel oder Freitext niemals eine Karte vorschlagen.
- Blocker/high findings nicht downgraden.
- Wenn Quellen fehlen, Vorbehalt nennen statt raten.
- Teams-Stil: Weltklasse interner Mitarbeiter, kompakt, deutsch.

EVIDENZPAKET:
{compact_packet}
""".strip()

    try:
        cmd = [HERMES_BIN, "-z", prompt, "--ignore-rules", "--toolsets", HERMES_REVIEW_TOOLSETS]
        completed = subprocess.run(
            cmd,
            cwd=str(REPO_ROOT),
            text=True,
            capture_output=True,
            timeout=_review_timeout(),
            check=False,
        )
    except Exception as exc:
        print(json.dumps({"error": f"hermes invocation failed: {exc}"}, ensure_ascii=False), file=sys.stderr)
        return 3
    parsed = _extract_json(completed.stdout)
    if parsed:
        # Hermes CLI can return a non-zero process code in some headless/script
        # contexts even after printing a valid final response. For this hook the
        # contract is the JSON payload on stdout; accept it if it validates.
        print(json.dumps(parsed, ensure_ascii=False))
        return 0
    if completed.returncode != 0:
        print(completed.stderr or completed.stdout, file=sys.stderr)
        return completed.returncode or 3
    print(completed.stdout, file=sys.stderr)
    return 4


if __name__ == "__main__":
    raise SystemExit(main())
