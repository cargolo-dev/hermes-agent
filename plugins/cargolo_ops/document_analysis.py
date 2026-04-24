from __future__ import annotations

import base64
import json
import mimetypes
import re
import shutil
import subprocess
import zipfile
from pathlib import Path
from typing import Any
from xml.etree import ElementTree as ET

from agent.auxiliary_client import _resolve_task_provider_model, call_llm, extract_content_or_reasoning
from hermes_cli.config import load_config

from .models import utc_now_iso

ANALYSIS_VERSION = "2026-04-13-doc-analysis-v2-native-openrouter"
_IMAGE_EXTENSIONS = {
    ".png": "image/png",
    ".jpg": "image/jpeg",
    ".jpeg": "image/jpeg",
    ".webp": "image/webp",
    ".gif": "image/gif",
    ".bmp": "image/bmp",
}
_MATCH_TOKEN_STOPWORDS = {
    "BILL",
    "LADING",
    "DRAFT",
    "DOC",
    "DOCX",
    "FILE",
    "ATT",
    "UPLOAD",
    "CONTAINER",
}


class DocumentAnalysisError(RuntimeError):
    pass


def _json_blob(text: str) -> dict[str, Any]:
    payload = (text or "").strip()
    if not payload:
        raise DocumentAnalysisError("empty document-analysis response")
    try:
        return json.loads(payload)
    except Exception:
        pass
    start = payload.find("{")
    end = payload.rfind("}")
    if start == -1 or end == -1 or end <= start:
        raise DocumentAnalysisError("no JSON object found in document-analysis response")
    return json.loads(payload[start:end + 1])


def _guess_mime_type(path: Path, hinted_mime_type: str | None = None) -> str:
    hinted = str(hinted_mime_type or "").strip().lower()
    guessed, _ = mimetypes.guess_type(path.name)
    if hinted and hinted not in {"application/octet-stream", "binary/octet-stream"}:
        return hinted
    if guessed:
        return guessed
    if hinted:
        return hinted
    return "application/octet-stream"


def _is_image_document(path: Path, mime_type: str) -> bool:
    return mime_type.startswith("image/") or path.suffix.lower() in _IMAGE_EXTENSIONS


def _data_url(path: Path, mime_type: str) -> str:
    return f"data:{mime_type};base64,{base64.b64encode(path.read_bytes()).decode('ascii')}"


def _text_data_url(text: str) -> str:
    return f"data:text/plain;base64,{base64.b64encode(text.encode('utf-8')).decode('ascii')}"


def _extract_docx_text(path: Path) -> str:
    paragraphs: list[str] = []
    try:
        with zipfile.ZipFile(path, "r") as zf:
            xml_bytes = zf.read("word/document.xml")
    except KeyError as exc:
        raise DocumentAnalysisError(f"DOCX-Inhalt fehlt: {path.name}") from exc
    except zipfile.BadZipFile as exc:
        raise DocumentAnalysisError(f"Ungültige DOCX-Datei: {path.name}") from exc

    try:
        root = ET.fromstring(xml_bytes)
    except ET.ParseError as exc:
        raise DocumentAnalysisError(f"DOCX-XML konnte nicht gelesen werden: {path.name}") from exc

    namespace = {"w": "http://schemas.openxmlformats.org/wordprocessingml/2006/main"}
    for paragraph in root.findall(".//w:p", namespace):
        parts = []
        for text_node in paragraph.findall(".//w:t", namespace):
            if text_node.text:
                parts.append(text_node.text)
        line = "".join(parts).strip()
        if line:
            paragraphs.append(line)
    text = "\n".join(paragraphs).strip()
    if not text:
        raise DocumentAnalysisError(f"DOCX enthält keinen extrahierbaren Text: {path.name}")
    return text


def _extract_doc_text(path: Path) -> str:
    antiword = shutil.which("antiword")
    if not antiword:
        raise DocumentAnalysisError(
            f"Legacy-DOC kann ohne antiword derzeit nicht vorverarbeitet werden: {path.name}"
        )
    try:
        result = subprocess.run(
            [antiword, str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise DocumentAnalysisError(f"antiword-Fehler für {path.name}: {stderr or exc}") from exc
    except Exception as exc:
        raise DocumentAnalysisError(f"Legacy-DOC konnte nicht gelesen werden: {path.name}: {exc}") from exc

    text = (result.stdout or "").strip()
    if not text:
        raise DocumentAnalysisError(f"Legacy-DOC enthält keinen extrahierbaren Text: {path.name}")
    return text


def _extract_xlsx_text(path: Path) -> str:
    try:
        with zipfile.ZipFile(path, "r") as zf:
            shared_strings: list[str] = []
            if "xl/sharedStrings.xml" in zf.namelist():
                shared_root = ET.fromstring(zf.read("xl/sharedStrings.xml"))
                ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
                for item in shared_root.findall(".//x:si", ns):
                    parts = [node.text or "" for node in item.findall(".//x:t", ns)]
                    shared_strings.append("".join(parts).strip())

            worksheet_names = sorted(name for name in zf.namelist() if name.startswith("xl/worksheets/") and name.endswith(".xml"))
            lines: list[str] = []
            ns = {"x": "http://schemas.openxmlformats.org/spreadsheetml/2006/main"}
            for worksheet_name in worksheet_names:
                root = ET.fromstring(zf.read(worksheet_name))
                for row in root.findall(".//x:sheetData/x:row", ns):
                    values: list[str] = []
                    for cell in row.findall("x:c", ns):
                        cell_type = cell.attrib.get("t", "")
                        if cell_type == "inlineStr":
                            parts = [node.text or "" for node in cell.findall(".//x:t", ns)]
                            value = "".join(parts).strip()
                        else:
                            value_node = cell.find("x:v", ns)
                            raw_value = (value_node.text or "").strip() if value_node is not None and value_node.text else ""
                            if not raw_value:
                                value = ""
                            elif cell_type == "s":
                                try:
                                    value = shared_strings[int(raw_value)].strip()
                                except Exception:
                                    value = raw_value
                            else:
                                value = raw_value
                        if value:
                            values.append(value)
                    if values:
                        lines.append("\t".join(values))
    except zipfile.BadZipFile as exc:
        raise DocumentAnalysisError(f"Ungültige XLSX-Datei: {path.name}") from exc
    except ET.ParseError as exc:
        raise DocumentAnalysisError(f"XLSX-XML konnte nicht gelesen werden: {path.name}") from exc
    except KeyError as exc:
        raise DocumentAnalysisError(f"XLSX-Inhalt fehlt: {path.name}") from exc

    text = "\n".join(line for line in lines if line.strip()).strip()
    if not text:
        raise DocumentAnalysisError(f"XLSX enthält keinen extrahierbaren Text: {path.name}")
    return text


def _extract_xls_text(path: Path) -> str:
    strings_cmd = shutil.which("strings")
    if not strings_cmd:
        raise DocumentAnalysisError(
            f"Legacy-XLS kann ohne strings derzeit nicht vorverarbeitet werden: {path.name}"
        )
    try:
        result = subprocess.run(
            [strings_cmd, "-n", "4", str(path)],
            check=True,
            capture_output=True,
            text=True,
            timeout=60,
        )
    except subprocess.CalledProcessError as exc:
        stderr = (exc.stderr or "").strip()
        raise DocumentAnalysisError(f"strings-Fehler für {path.name}: {stderr or exc}") from exc
    except Exception as exc:
        raise DocumentAnalysisError(f"Legacy-XLS konnte nicht gelesen werden: {path.name}: {exc}") from exc

    text = "\n".join(line.strip() for line in (result.stdout or "").splitlines() if line.strip()).strip()
    if not text:
        raise DocumentAnalysisError(f"Legacy-XLS enthält keinen extrahierbaren Text: {path.name}")
    return text


def _prepare_document_transport(path: Path, mime_type: str) -> tuple[str, str, dict[str, Any], str]:
    suffix = path.suffix.lower()
    text_filename = f"{path.stem}.txt"
    if suffix == ".docx" or mime_type == "application/vnd.openxmlformats-officedocument.wordprocessingml.document":
        return text_filename, "text/plain", {}, _text_data_url(_extract_docx_text(path))
    if suffix == ".doc" or mime_type == "application/msword":
        return text_filename, "text/plain", {}, _text_data_url(_extract_doc_text(path))
    if suffix == ".xlsx" or mime_type == "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet":
        return text_filename, "text/plain", {}, _text_data_url(_extract_xlsx_text(path))
    if suffix == ".xls" or mime_type == "application/vnd.ms-excel":
        return text_filename, "text/plain", {}, _text_data_url(_extract_xls_text(path))
    return path.name, mime_type, _document_extra_body(mime_type), _data_url(path, mime_type)


def _document_analysis_config() -> dict[str, Any]:
    try:
        config = load_config() or {}
    except Exception:
        config = {}
    aux = config.get("auxiliary", {}) if isinstance(config, dict) else {}
    task_cfg = aux.get("document_analysis", {}) if isinstance(aux, dict) else {}
    return task_cfg if isinstance(task_cfg, dict) else {}


def _pdf_engine() -> str:
    cfg = _document_analysis_config()
    engine = str(cfg.get("pdf_engine", "") or "native").strip().lower()
    return engine or "native"


def _document_messages(*, order_id: str, filename: str, mime_type: str, registry_types: list[str], expected_types: list[str], tms_snapshot: dict[str, Any], file_data_url: str) -> list[dict[str, Any]]:
    tms_detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    return [
        {
            "role": "system",
            "content": (
                "Du analysierst ein Logistikdokument für CARGOLO ASR direkt als Dateiinput. "
                "Antworte ausschließlich als JSON ohne Markdown. "
                "Human-readable Werte auf Deutsch. "
                "Nutze Speditions-/Forwarding-Semantik: Rechnung, Packliste, AWB, B/L, MRN, ETA/ETD, POL/POD, Carrier, Laufweg, Zoll, Billing."
            ),
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Analysiere dieses Dokument und liefere strikt JSON mit genau diesen Schlüsseln:\n"
                        "{\n"
                        '  "doc_type": "commercial_invoice|billing|packing_list|air_waybill|bill_of_lading|proof_of_delivery|mrn|customs_document|unknown",\n'
                        '  "confidence": "low|medium|high",\n'
                        '  "summary": "kurze deutsche Zusammenfassung",\n'
                        '  "shipment_numbers": ["AN-..."],\n'
                        '  "references": ["..."],\n'
                        '  "suggested_registry_types": ["..."],\n'
                        '  "extracted_fields": {"invoice_number": "", "document_number": "", "amount": "", "currency": "", "carrier": "", "pol": "", "pod": "", "eta": "", "etd": "", "mrn": ""},\n'
                        '  "missing_or_unreadable": ["..."],\n'
                        '  "consistency_notes": ["..."],\n'
                        '  "operational_flags": ["..."],\n'
                        '  "reply_relevance": "none|low|medium|high"\n'
                        "}\n\n"
                        "WICHTIGE REGEL: 'commercial_invoice' bedeutet nur die Handelsrechnung der Ware des Kunden. "
                        "Interne, agentenseitige oder Lieferanten-Rechnungen an CARGOLO, Hartmann oder andere Spediteure sind 'billing', nicht 'commercial_invoice'.\n\n"
                        f"Auftrag: {order_id}\n"
                        f"Datei: {filename}\n"
                        f"MIME: {mime_type}\n"
                        f"Bereits heuristisch erkannte Typen: {registry_types}\n"
                        f"Im TMS erwartete Dokumenttypen: {expected_types}\n"
                        f"TMS-Detail-Auszug: {json.dumps(tms_detail or {}, ensure_ascii=False)[:3000]}"
                    ),
                },
                {
                    "type": "file",
                    "file": {
                        "filename": filename,
                        "file_data": file_data_url,
                    },
                },
            ],
        },
    ]


def _image_document_messages(*, order_id: str, filename: str, mime_type: str, registry_types: list[str], expected_types: list[str], tms_snapshot: dict[str, Any], image_data_url: str) -> list[dict[str, Any]]:
    tms_detail = tms_snapshot.get("detail") if isinstance(tms_snapshot, dict) else {}
    return [
        {
            "role": "system",
            "content": "Du analysierst ein Logistikdokumentbild für CARGOLO ASR direkt im nativen Modellinput. Antworte ausschließlich als JSON ohne Markdown. Alle erklärenden Texte auf Deutsch.",
        },
        {
            "role": "user",
            "content": [
                {
                    "type": "text",
                    "text": (
                        "Analysiere dieses Dokumentbild. Gib strikt JSON zurück mit den Schlüsseln "
                        "doc_type, confidence, summary, shipment_numbers, references, suggested_registry_types, extracted_fields, missing_or_unreadable, consistency_notes, operational_flags, reply_relevance.\n"
                        "WICHTIGE REGEL: 'commercial_invoice' bedeutet nur die Handelsrechnung der Ware des Kunden. Interne, agentenseitige oder Lieferanten-Rechnungen an CARGOLO/Hartmann sind 'billing'.\n"
                        f"Auftrag: {order_id}\nDatei: {filename}\nHeuristische Typen: {registry_types}\nTMS-erwartet: {expected_types}\n"
                        f"TMS-Detail-Auszug: {json.dumps(tms_detail or {}, ensure_ascii=False)[:2000]}"
                    ),
                },
                {
                    "type": "image_url",
                    "image_url": {"url": image_data_url},
                },
            ],
        },
    ]


def _normalize_doc_type(value: str) -> str:
    lowered = (value or "unknown").strip().lower().replace("-", "_").replace(" ", "_")
    mapping = {
        "invoice": "commercial_invoice",
        "commercial_invoice": "commercial_invoice",
        "billing": "billing",
        "supplier_invoice": "billing",
        "vendor_invoice": "billing",
        "agent_invoice": "billing",
        "cost_invoice": "billing",
        "packing_list": "packing_list",
        "air_waybill": "air_waybill",
        "awb": "air_waybill",
        "bill_of_lading": "bill_of_lading",
        "bl": "bill_of_lading",
        "proof_of_delivery": "proof_of_delivery",
        "pod": "proof_of_delivery",
        "mrn": "mrn",
        "customs_document": "customs_document",
        "unknown": "unknown",
    }
    return mapping.get(lowered, lowered or "unknown")


def _coerce_internal_invoice_doc_type(*, doc_type: str, payload: dict[str, Any], path: Path) -> str:
    normalized = _normalize_doc_type(doc_type)
    if normalized != "commercial_invoice":
        return normalized
    text_blob = "\n".join([
        str(path.name or ""),
        str(payload.get("summary") or ""),
        str(payload.get("references") or ""),
        str(payload.get("extracted_fields") or ""),
    ]).lower()
    invoice_like = any(token in text_blob for token in ("invoice", "rechnung", "inv-") )
    if invoice_like and any(token in text_blob for token in ("hartmann", "cargolo")):
        return "billing"
    return normalized


def _document_extra_body(mime_type: str) -> dict[str, Any]:
    if mime_type != "application/pdf":
        return {}
    return {
        "plugins": [
            {
                "id": "file-parser",
                "pdf": {"engine": _pdf_engine()},
            }
        ]
    }


def _match_tokens(*values: Any) -> set[str]:
    tokens: set[str] = set()
    for value in values:
        if value is None:
            continue
        if isinstance(value, (list, tuple, set)):
            tokens.update(_match_tokens(*value))
            continue
        if isinstance(value, dict):
            tokens.update(_match_tokens(*value.values()))
            continue
        text = str(value or "").upper()
        for token in re.findall(r"[A-Z0-9][A-Z0-9_-]{5,}", text):
            cleaned = token.strip("_-")
            if cleaned:
                tokens.add(cleaned)
        for token in re.split(r"[^A-Z0-9]+|[_-]+", text):
            cleaned = token.strip()
            if len(cleaned) >= 4:
                tokens.add(cleaned)
    return {token for token in tokens if token}


def _match_tms_documents(*, analysis: dict[str, Any], received_doc: dict[str, Any], registry: dict[str, Any]) -> list[dict[str, Any]]:
    received_type = _normalize_doc_type(str(analysis.get("doc_type") or "unknown"))
    tms_documents = [row for row in (registry.get("tms_documents") or []) if isinstance(row, dict)]
    type_candidates = [
        row for row in tms_documents
        if _normalize_doc_type(str(row.get("document_type") or row.get("label") or "")) == received_type
    ]
    if not type_candidates:
        return []

    received_tokens = _match_tokens(
        received_doc.get("filename"),
        analysis.get("filename"),
        analysis.get("references"),
        analysis.get("shipment_numbers"),
        analysis.get("extracted_fields"),
    )

    matches: list[dict[str, Any]] = []
    for row in type_candidates:
        match_basis = ["document_type"]
        tms_tokens = _match_tokens(
            row.get("filename"),
            row.get("label"),
            row.get("document_type"),
            row.get("tms_document_id"),
        )
        shared_tokens = sorted(received_tokens & tms_tokens)
        strong_shared_tokens = [
            token for token in shared_tokens
            if token not in _MATCH_TOKEN_STOPWORDS and (any(ch.isdigit() for ch in token) or len(token) >= 12)
        ]
        if strong_shared_tokens:
            match_basis.append("filename_token")
        elif len(type_candidates) == 1:
            match_basis.append("single_type_candidate")
        else:
            continue

        matches.append({
            "tms_document_id": row.get("tms_document_id"),
            "document_type": row.get("document_type"),
            "label": row.get("label"),
            "filename": row.get("filename"),
            "status": row.get("status"),
            "required": bool(row.get("required")),
            "match_basis": match_basis,
            "shared_tokens": strong_shared_tokens,
        })
    return matches


def _analyze_single_document(*, order_id: str, path: Path, received_doc: dict[str, Any], registry: dict[str, Any], tms_snapshot: dict[str, Any]) -> dict[str, Any]:
    original_mime_type = _guess_mime_type(path, received_doc.get("mime_type"))
    provider, model, base_url, api_key = _resolve_task_provider_model("document_analysis")
    if provider == "auto":
        provider = "openrouter"
    registry_types = list(received_doc.get("detected_types") or [])
    expected_types = list(registry.get("expected_types") or [])

    transport_filename, mime_type, extra_body, data_url = _prepare_document_transport(path, original_mime_type)

    if _is_image_document(path, mime_type):
        messages = _image_document_messages(
            order_id=order_id,
            filename=transport_filename,
            mime_type=mime_type,
            registry_types=registry_types,
            expected_types=expected_types,
            tms_snapshot=tms_snapshot,
            image_data_url=data_url,
        )
        extra_body = {}
        transport = "native_image"
    else:
        messages = _document_messages(
            order_id=order_id,
            filename=transport_filename,
            mime_type=mime_type,
            registry_types=registry_types,
            expected_types=expected_types,
            tms_snapshot=tms_snapshot,
            file_data_url=data_url,
        )
        transport = "converted_text" if mime_type == "text/plain" and path.suffix.lower() in {".doc", ".docx", ".xls", ".xlsx"} else "native_file"

    response = call_llm(
        task="document_analysis",
        provider=provider,
        model=model,
        base_url=base_url,
        api_key=api_key,
        messages=messages,
        max_tokens=1400,
        extra_body=extra_body,
    )

    payload = _json_blob(extract_content_or_reasoning(response))
    doc_type = _coerce_internal_invoice_doc_type(doc_type=str(payload.get("doc_type", "unknown")), payload=payload, path=path)
    suggested_types = [
        _normalize_doc_type(str(item))
        for item in (payload.get("suggested_registry_types") or [])
        if str(item).strip()
    ]
    if doc_type == "billing":
        suggested_types = ["billing" if item == "commercial_invoice" else item for item in suggested_types]
    extracted_fields = payload.get("extracted_fields") if isinstance(payload.get("extracted_fields"), dict) else {}
    return {
        "analysis_version": ANALYSIS_VERSION,
        "analyzed_at": utc_now_iso(),
        "provider": provider,
        "model": model,
        "base_url": base_url,
        "filename": path.name,
        "stored_path": str(path),
        "mime_type": mime_type,
        "source_message_id": received_doc.get("message_id"),
        "sha256": received_doc.get("sha256"),
        "transport_method": transport,
        "pdf_engine": _pdf_engine() if mime_type == "application/pdf" else None,
        "doc_type": doc_type,
        "confidence": str(payload.get("confidence", "medium") or "medium"),
        "summary": str(payload.get("summary", "") or ""),
        "shipment_numbers": [str(item) for item in (payload.get("shipment_numbers") or []) if str(item).strip()],
        "references": [str(item) for item in (payload.get("references") or []) if str(item).strip()],
        "suggested_registry_types": sorted(set(suggested_types + ([doc_type] if doc_type != "unknown" else []))),
        "extracted_fields": extracted_fields,
        "missing_or_unreadable": [str(item) for item in (payload.get("missing_or_unreadable") or []) if str(item).strip()],
        "consistency_notes": [str(item) for item in (payload.get("consistency_notes") or []) if str(item).strip()],
        "operational_flags": [str(item) for item in (payload.get("operational_flags") or []) if str(item).strip()],
        "reply_relevance": str(payload.get("reply_relevance", "low") or "low"),
    }


def analyze_case_documents(*, order_id: str, case_root: Path, registry: dict[str, Any], tms_snapshot: dict[str, Any]) -> tuple[dict[str, Any], list[str]]:
    analysis_root = case_root / "documents" / "analysis"
    analysis_root.mkdir(parents=True, exist_ok=True)

    summaries: list[dict[str, Any]] = []
    open_questions: list[str] = []
    received_documents = [row for row in registry.get("received_documents", []) if isinstance(row, dict)]
    updated_received_documents: list[dict[str, Any]] = []

    for row in received_documents:
        stored_path = Path(str(row.get("stored_path") or ""))
        if not stored_path.exists():
            row = dict(row)
            row["analysis_status"] = "missing_file"
            updated_received_documents.append(row)
            open_questions.append(f"Dokumentdatei fehlt lokal: {row.get('filename') or stored_path.name}")
            continue

        sha = str(row.get("sha256") or stored_path.stem or stored_path.name)
        artifact_path = analysis_root / f"doc_{sha[:64]}.json"
        try:
            analysis = _analyze_single_document(
                order_id=order_id,
                path=stored_path,
                received_doc=row,
                registry=registry,
                tms_snapshot=tms_snapshot,
            )
            tms_matches = _match_tms_documents(analysis=analysis, received_doc=row, registry=registry)
            artifact_path.write_text(json.dumps(analysis, ensure_ascii=False, indent=2), encoding="utf-8")
            row = dict(row)
            row["analysis_status"] = "analyzed"
            row["analysis_path"] = str(artifact_path)
            row["analysis_doc_type"] = analysis.get("doc_type")
            row["analysis_confidence"] = analysis.get("confidence")
            row["analysis_summary"] = analysis.get("summary")
            row["tms_matches"] = tms_matches
            analysis_types = set(analysis.get("suggested_registry_types", []))
            if analysis.get("doc_type") == "billing":
                analysis_types.discard("commercial_invoice")
            existing_types = set(row.get("detected_types", []))
            if analysis.get("doc_type") == "billing":
                existing_types.discard("commercial_invoice")
            row["detected_types"] = sorted(existing_types | analysis_types)
            summaries.append({
                "filename": row.get("filename"),
                "doc_type": analysis.get("doc_type"),
                "confidence": analysis.get("confidence"),
                "summary": analysis.get("summary"),
                "analysis_path": str(artifact_path),
                "tms_matches": tms_matches,
                "operational_flags": analysis.get("operational_flags", []),
                "missing_or_unreadable": analysis.get("missing_or_unreadable", []),
            })
            open_questions.extend(analysis.get("missing_or_unreadable", []))
        except Exception as exc:
            error_payload = {
                "analysis_version": ANALYSIS_VERSION,
                "status": "error",
                "error": str(exc),
                "filename": row.get("filename") or stored_path.name,
                "stored_path": str(stored_path),
                "generated_at": utc_now_iso(),
            }
            artifact_path.write_text(json.dumps(error_payload, ensure_ascii=False, indent=2), encoding="utf-8")
            row = dict(row)
            row["analysis_status"] = "error"
            row["analysis_path"] = str(artifact_path)
            row["analysis_summary"] = f"Dokumentanalyse fehlgeschlagen: {exc}"
            summaries.append({
                "filename": row.get("filename"),
                "doc_type": "unknown",
                "confidence": "low",
                "summary": row["analysis_summary"],
                "analysis_path": str(artifact_path),
                "operational_flags": ["DOCUMENT_ANALYSIS_ERROR"],
                "missing_or_unreadable": [str(exc)],
            })
            open_questions.append(f"Dokumentanalyse fehlgeschlagen für {row.get('filename')}: {exc}")
        updated_received_documents.append(row)

    analyzed_types = sorted({
        doc_type
        for row in updated_received_documents
        for doc_type in row.get("detected_types", [])
        if doc_type
    })
    missing_types = sorted(set(registry.get("expected_types", [])) - set(analyzed_types))
    tms_match_summary = [
        {
            "received_filename": row.get("filename"),
            "analysis_doc_type": row.get("analysis_doc_type"),
            "tms_document_id": match.get("tms_document_id"),
            "tms_filename": match.get("filename"),
            "match_basis": list(match.get("match_basis") or []),
        }
        for row in updated_received_documents
        if isinstance(row, dict)
        for match in (row.get("tms_matches") or [])
        if isinstance(match, dict)
    ]
    summary_payload = {
        "analysis_version": ANALYSIS_VERSION,
        "generated_at": utc_now_iso(),
        "order_id": order_id,
        "documents": summaries,
        "analyzed_types": analyzed_types,
        "missing_types": missing_types,
        "tms_match_summary": tms_match_summary,
        "open_questions": sorted(set(open_questions)),
    }
    (analysis_root / "latest_summary.json").write_text(json.dumps(summary_payload, ensure_ascii=False, indent=2), encoding="utf-8")

    updated_registry = dict(registry)
    updated_registry["analysis_version"] = ANALYSIS_VERSION
    updated_registry["analysis_generated_at"] = utc_now_iso()
    updated_registry["received_documents"] = updated_received_documents
    updated_registry["received_types"] = analyzed_types or list(updated_registry.get("received_types", []))
    updated_registry["missing_types"] = missing_types
    updated_registry["analyzed_documents"] = summaries
    updated_registry["tms_match_summary"] = tms_match_summary
    updated_registry["analysis_open_questions"] = sorted(set(open_questions))
    updated_registry["document_analysis_summary_path"] = str(analysis_root / "latest_summary.json")
    return updated_registry, sorted(set(open_questions))
