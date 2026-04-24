import json
import subprocess
import zipfile
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import patch

from plugins.cargolo_ops.document_analysis import analyze_case_documents


class _FakeResponse:
    def __init__(self, content: str):
        self.choices = [SimpleNamespace(message=SimpleNamespace(content=content, reasoning=None, reasoning_content=None, reasoning_details=None))]


def test_analyze_case_documents_with_separate_openrouter_model(tmp_path):
    case_root = tmp_path / "orders" / "AN-12345"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "invoice.txt"
    doc_path.write_text("Invoice INV-77 for AN-12345 amount EUR 1200", encoding="utf-8")

    registry = {
        "received_documents": [{
            "filename": "invoice.txt",
            "stored_path": str(doc_path),
            "mime_type": "text/plain",
            "sha256": "abc123",
            "message_id": "m1",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": ["commercial_invoice"],
        }],
        "expected_types": ["commercial_invoice", "packing_list"],
        "received_types": ["commercial_invoice"],
        "missing_types": ["packing_list"],
    }
    tms_snapshot = {"detail": {"documents": [{"document_type": "Commercial Invoice", "required": True}]}}
    llm_json = json.dumps({
        "doc_type": "commercial_invoice",
        "confidence": "high",
        "summary": "Rechnung mit Betrag und Referenz erkannt.",
        "shipment_numbers": ["AN-12345"],
        "references": ["INV-77"],
        "suggested_registry_types": ["commercial_invoice"],
        "extracted_fields": {"invoice_number": "INV-77", "amount": "1200", "currency": "EUR"},
        "missing_or_unreadable": [],
        "consistency_notes": ["Sendungsnummer passt zum Auftrag."],
        "operational_flags": ["DOCUMENT_OK"],
        "reply_relevance": "medium",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm):
        updated_registry, open_questions = analyze_case_documents(
            order_id="AN-12345",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    assert open_questions == []
    assert updated_registry["received_types"] == ["commercial_invoice"]
    assert updated_registry["missing_types"] == ["packing_list"]
    assert updated_registry["analyzed_documents"][0]["doc_type"] == "commercial_invoice"
    assert updated_registry["received_documents"][0]["analysis_status"] == "analyzed"
    assert updated_registry["received_documents"][0]["analysis_doc_type"] == "commercial_invoice"
    analysis_path = Path(updated_registry["received_documents"][0]["analysis_path"])
    assert analysis_path.exists()
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    assert payload["provider"] == "openrouter"
    assert payload["model"] == "google/gemini-3-flash-preview"
    assert payload["transport_method"] == "native_file"
    assert payload["extracted_fields"]["invoice_number"] == "INV-77"
    assert captured["task"] == "document_analysis"
    file_block = captured["messages"][1]["content"][1]
    assert file_block["type"] == "file"
    assert file_block["file"]["filename"] == "invoice.txt"
    assert file_block["file"]["file_data"].startswith("data:text/plain;base64,")
    summary_path = Path(updated_registry["document_analysis_summary_path"])
    assert summary_path.exists()


def test_pdf_document_analysis_uses_openrouter_native_pdf_plugin(tmp_path):
    case_root = tmp_path / "orders" / "AN-55555"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    pdf_path = inbound / "scan.pdf"
    pdf_path.write_bytes(b"%PDF-1.4 fake pdf bytes")

    registry = {
        "received_documents": [{
            "filename": "scan.pdf",
            "stored_path": str(pdf_path),
            "mime_type": "application/pdf",
            "sha256": "pdf123",
            "message_id": "m2",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["commercial_invoice"],
        "received_types": [],
        "missing_types": ["commercial_invoice"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "commercial_invoice",
        "confidence": "medium",
        "summary": "PDF nativ analysiert.",
        "shipment_numbers": ["AN-55555"],
        "references": [],
        "suggested_registry_types": ["commercial_invoice"],
        "extracted_fields": {},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.load_config", return_value={"auxiliary": {"document_analysis": {"pdf_engine": "native"}}}):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-55555",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    file_block = captured["messages"][1]["content"][1]
    assert file_block["type"] == "file"
    assert file_block["file"]["filename"] == "scan.pdf"
    assert file_block["file"]["file_data"].startswith("data:application/pdf;base64,")
    assert captured["extra_body"]["plugins"][0]["id"] == "file-parser"
    assert captured["extra_body"]["plugins"][0]["pdf"]["engine"] == "native"
    analysis_path = Path(updated_registry["received_documents"][0]["analysis_path"])
    payload = json.loads(analysis_path.read_text(encoding="utf-8"))
    assert payload["pdf_engine"] == "native"


def test_docx_document_analysis_extracts_text_and_sends_plaintext(tmp_path):
    case_root = tmp_path / "orders" / "AN-66666"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    docx_path = inbound / "draft-bl.docx"
    with zipfile.ZipFile(docx_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "word/document.xml",
            """
            <w:document xmlns:w="http://schemas.openxmlformats.org/wordprocessingml/2006/main">
              <w:body>
                <w:p><w:r><w:t>Draft Bill of Lading</w:t></w:r></w:p>
                <w:p><w:r><w:t>AN-66666</w:t></w:r></w:p>
              </w:body>
            </w:document>
            """.strip(),
        )

    registry = {
        "received_documents": [{
            "filename": "draft-bl.docx",
            "stored_path": str(docx_path),
            "mime_type": "application/vnd.openxmlformats-officedocument.wordprocessingml.document",
            "sha256": "docx123",
            "message_id": "m3",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["bill_of_lading"],
        "received_types": [],
        "missing_types": ["bill_of_lading"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "bill_of_lading",
        "confidence": "medium",
        "summary": "DOCX als Klartext analysiert.",
        "shipment_numbers": ["AN-66666"],
        "references": [],
        "suggested_registry_types": ["bill_of_lading"],
        "extracted_fields": {},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-66666",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    file_block = captured["messages"][1]["content"][1]
    assert file_block["type"] == "file"
    assert file_block["file"]["filename"] == "draft-bl.txt"
    assert file_block["file"]["file_data"].startswith("data:text/plain;base64,")
    assert captured["extra_body"] == {}
    payload = json.loads(Path(updated_registry["received_documents"][0]["analysis_path"]).read_text(encoding="utf-8"))
    assert payload["transport_method"] == "converted_text"
    assert payload["mime_type"] == "text/plain"


def test_doc_document_analysis_uses_antiword_when_available(tmp_path):
    case_root = tmp_path / "orders" / "AN-77777"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "draft-bl.doc"
    doc_path.write_bytes(b"\xd0\xcf\x11\xe0fake-doc")

    registry = {
        "received_documents": [{
            "filename": "draft-bl.doc",
            "stored_path": str(doc_path),
            "mime_type": "application/msword",
            "sha256": "doc123",
            "message_id": "m4",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["bill_of_lading"],
        "received_types": [],
        "missing_types": ["bill_of_lading"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "bill_of_lading",
        "confidence": "medium",
        "summary": "DOC via antiword analysiert.",
        "shipment_numbers": ["AN-77777"],
        "references": ["LH067"],
        "suggested_registry_types": ["bill_of_lading"],
        "extracted_fields": {},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.shutil.which", return_value="/usr/bin/antiword"), \
         patch("plugins.cargolo_ops.document_analysis.subprocess.run", return_value=subprocess.CompletedProcess(["antiword"], 0, stdout="DRAFT B/L LH067\nAN-77777\n", stderr="")) as mock_run:
        updated_registry, _ = analyze_case_documents(
            order_id="AN-77777",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    mock_run.assert_called_once()
    file_block = captured["messages"][1]["content"][1]
    assert file_block["file"]["filename"] == "draft-bl.txt"
    assert file_block["file"]["file_data"].startswith("data:text/plain;base64,")
    payload = json.loads(Path(updated_registry["received_documents"][0]["analysis_path"]).read_text(encoding="utf-8"))
    assert payload["transport_method"] == "converted_text"
    assert payload["mime_type"] == "text/plain"


def test_xlsx_document_analysis_extracts_text_and_sends_plaintext(tmp_path):
    case_root = tmp_path / "orders" / "AN-77888"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    xlsx_path = inbound / "invoice.xlsx"
    with zipfile.ZipFile(xlsx_path, "w", compression=zipfile.ZIP_DEFLATED) as zf:
        zf.writestr(
            "[Content_Types].xml",
            """<?xml version=\"1.0\" encoding=\"UTF-8\"?>
            <Types xmlns=\"http://schemas.openxmlformats.org/package/2006/content-types\">
              <Override PartName=\"/xl/workbook.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sheet.main+xml\"/>
              <Override PartName=\"/xl/worksheets/sheet1.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.worksheet+xml\"/>
              <Override PartName=\"/xl/sharedStrings.xml\" ContentType=\"application/vnd.openxmlformats-officedocument.spreadsheetml.sharedStrings+xml\"/>
            </Types>""".strip(),
        )
        zf.writestr(
            "xl/workbook.xml",
            """<workbook xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" xmlns:r=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships\"><sheets><sheet name=\"Sheet1\" sheetId=\"1\" r:id=\"rId1\"/></sheets></workbook>""",
        )
        zf.writestr(
            "xl/_rels/workbook.xml.rels",
            """<Relationships xmlns=\"http://schemas.openxmlformats.org/package/2006/relationships\"><Relationship Id=\"rId1\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/worksheet\" Target=\"worksheets/sheet1.xml\"/><Relationship Id=\"rId2\" Type=\"http://schemas.openxmlformats.org/officeDocument/2006/relationships/sharedStrings\" Target=\"sharedStrings.xml\"/></Relationships>""",
        )
        zf.writestr(
            "xl/sharedStrings.xml",
            """<sst xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\" count=\"4\" uniqueCount=\"4\"><si><t>Invoice</t></si><si><t>INV-77888</t></si><si><t>AN-77888</t></si><si><t>EUR 1200</t></si></sst>""",
        )
        zf.writestr(
            "xl/worksheets/sheet1.xml",
            """<worksheet xmlns=\"http://schemas.openxmlformats.org/spreadsheetml/2006/main\"><sheetData><row r=\"1\"><c r=\"A1\" t=\"s\"><v>0</v></c><c r=\"B1\" t=\"s\"><v>1</v></c></row><row r=\"2\"><c r=\"A2\" t=\"s\"><v>2</v></c><c r=\"B2\" t=\"s\"><v>3</v></c></row></sheetData></worksheet>""",
        )

    registry = {
        "received_documents": [{
            "filename": "invoice.xlsx",
            "stored_path": str(xlsx_path),
            "mime_type": "application/vnd.openxmlformats-officedocument.spreadsheetml.sheet",
            "sha256": "xlsx123",
            "message_id": "m4x",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["commercial_invoice"],
        "received_types": [],
        "missing_types": ["commercial_invoice"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "commercial_invoice",
        "confidence": "high",
        "summary": "XLSX als Klartext analysiert.",
        "shipment_numbers": ["AN-77888"],
        "references": ["INV-77888"],
        "suggested_registry_types": ["commercial_invoice"],
        "extracted_fields": {"invoice_number": "INV-77888", "amount": "1200", "currency": "EUR"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-77888",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    file_block = captured["messages"][1]["content"][1]
    assert file_block["file"]["filename"] == "invoice.txt"
    assert file_block["file"]["file_data"].startswith("data:text/plain;base64,")
    payload = json.loads(Path(updated_registry["received_documents"][0]["analysis_path"]).read_text(encoding="utf-8"))
    assert payload["transport_method"] == "converted_text"
    assert payload["mime_type"] == "text/plain"


def test_xls_document_analysis_uses_strings_fallback_and_sends_plaintext(tmp_path):
    case_root = tmp_path / "orders" / "AN-77999"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    xls_path = inbound / "packing-list.xls"
    xls_path.write_bytes(b"\xd0\xcf\x11\xe0fake-xls")

    registry = {
        "received_documents": [{
            "filename": "packing-list.xls",
            "stored_path": str(xls_path),
            "mime_type": "application/vnd.ms-excel",
            "sha256": "xls123",
            "message_id": "m4y",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["packing_list"],
        "received_types": [],
        "missing_types": ["packing_list"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "packing_list",
        "confidence": "medium",
        "summary": "XLS via Textfallback analysiert.",
        "shipment_numbers": ["AN-77999"],
        "references": [],
        "suggested_registry_types": ["packing_list"],
        "extracted_fields": {},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)
    captured = {}

    def _fake_call_llm(**kwargs):
        captured.update(kwargs)
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.shutil.which", return_value="/usr/bin/strings"), \
         patch("plugins.cargolo_ops.document_analysis.subprocess.run", return_value=subprocess.CompletedProcess(["strings"], 0, stdout="Packing List\nAN-77999\n10 CTNS\n", stderr="")) as mock_run:
        updated_registry, _ = analyze_case_documents(
            order_id="AN-77999",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    mock_run.assert_called_once()
    file_block = captured["messages"][1]["content"][1]
    assert file_block["file"]["filename"] == "packing-list.txt"
    assert file_block["file"]["file_data"].startswith("data:text/plain;base64,")
    payload = json.loads(Path(updated_registry["received_documents"][0]["analysis_path"]).read_text(encoding="utf-8"))
    assert payload["transport_method"] == "converted_text"
    assert payload["mime_type"] == "text/plain"


def test_document_analysis_matches_bill_of_lading_against_tms_documents(tmp_path):
    case_root = tmp_path / "orders" / "AN-88888"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "draft-ZIHWBK260412LH067-Q.doc"
    doc_path.write_bytes(b"\xd0\xcf\x11\xe0fake-doc")

    registry = {
        "received_documents": [{
            "filename": "draft-ZIHWBK260412LH067-Q.doc",
            "stored_path": str(doc_path),
            "mime_type": "application/msword",
            "sha256": "doc888",
            "message_id": "m8",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "tms_documents": [{
            "tms_document_id": "tms-bl-1",
            "label": "Bill of Lading",
            "document_type": "bill_of_lading",
            "required": False,
            "status": "uploaded",
            "filename": "049718_att_32_draft-ZIHWBK260412LH067-Q.doc",
        }],
        "expected_types": ["commercial_invoice"],
        "received_types": [],
        "missing_types": ["commercial_invoice"],
    }
    tms_snapshot = {"detail": {}}
    llm_json = json.dumps({
        "doc_type": "bill_of_lading",
        "confidence": "high",
        "summary": "DOC via antiword analysiert.",
        "shipment_numbers": ["AN-88888"],
        "references": ["ZIHWBK260412LH067-Q"],
        "suggested_registry_types": ["bill_of_lading"],
        "extracted_fields": {"document_number": "ZIHWBK260412LH067-Q"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)

    def _fake_call_llm(**kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.shutil.which", return_value="/usr/bin/antiword"), \
         patch("plugins.cargolo_ops.document_analysis.subprocess.run", return_value=subprocess.CompletedProcess(["antiword"], 0, stdout="DRAFT B/L ZIHWBK260412LH067-Q\nAN-88888\n", stderr="")):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-88888",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    matched_doc = updated_registry["received_documents"][0]
    assert matched_doc["tms_matches"][0]["tms_document_id"] == "tms-bl-1"
    assert matched_doc["tms_matches"][0]["match_basis"] == ["document_type", "filename_token"]
    assert updated_registry["tms_match_summary"][0]["received_filename"] == "draft-ZIHWBK260412LH067-Q.doc"
    assert updated_registry["tms_match_summary"][0]["tms_document_id"] == "tms-bl-1"


def test_document_analysis_avoids_cross_matching_multiple_bill_of_ladings(tmp_path):
    case_root = tmp_path / "orders" / "AN-99999"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    first_doc = inbound / "draft-ZIHWBK260412LH067-Q.doc"
    second_doc = inbound / "draft-ZIHWBK260412LH082-Q (2).doc"
    first_doc.write_bytes(b"\xd0\xcf\x11\xe0first-doc")
    second_doc.write_bytes(b"\xd0\xcf\x11\xe0second-doc")

    registry = {
        "received_documents": [
            {
                "filename": first_doc.name,
                "stored_path": str(first_doc),
                "mime_type": "application/msword",
                "sha256": "doc999a",
                "message_id": "m9a",
                "received_at": "2026-04-13T10:00:00Z",
                "detected_types": [],
            },
            {
                "filename": second_doc.name,
                "stored_path": str(second_doc),
                "mime_type": "application/msword",
                "sha256": "doc999b",
                "message_id": "m9b",
                "received_at": "2026-04-13T10:01:00Z",
                "detected_types": [],
            },
        ],
        "tms_documents": [
            {
                "tms_document_id": "tms-bl-067",
                "label": "Bill of Lading",
                "document_type": "bill_of_lading",
                "required": False,
                "status": "uploaded",
                "filename": "049718_att_32_draft-ZIHWBK260412LH067-Q.doc",
            },
            {
                "tms_document_id": "tms-bl-082",
                "label": "Bill of Lading",
                "document_type": "bill_of_lading",
                "required": False,
                "status": "uploaded",
                "filename": "049718_att_31_draft-ZIHWBK260412LH082-Q_2.doc",
            },
        ],
        "expected_types": [],
        "received_types": [],
        "missing_types": [],
    }
    tms_snapshot = {"detail": {}}
    llm_payloads = [
        json.dumps({
            "doc_type": "bill_of_lading",
            "confidence": "high",
            "summary": "First DOC via antiword analysiert.",
            "shipment_numbers": ["AN-99999"],
            "references": ["ZIHWBK260412LH067-Q"],
            "suggested_registry_types": ["bill_of_lading"],
            "extracted_fields": {"document_number": "ZIHWBK260412LH067-Q"},
            "missing_or_unreadable": [],
            "consistency_notes": [],
            "operational_flags": [],
            "reply_relevance": "low",
        }, ensure_ascii=False),
        json.dumps({
            "doc_type": "bill_of_lading",
            "confidence": "high",
            "summary": "Second DOC via antiword analysiert.",
            "shipment_numbers": ["AN-99999"],
            "references": ["ZIHWBK260412LH082-Q"],
            "suggested_registry_types": ["bill_of_lading"],
            "extracted_fields": {"document_number": "ZIHWBK260412LH082-Q"},
            "missing_or_unreadable": [],
            "consistency_notes": [],
            "operational_flags": [],
            "reply_relevance": "low",
        }, ensure_ascii=False),
    ]

    def _fake_call_llm(**kwargs):
        return _FakeResponse(llm_payloads.pop(0))

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.shutil.which", return_value="/usr/bin/antiword"), \
         patch("plugins.cargolo_ops.document_analysis.subprocess.run", side_effect=[
             subprocess.CompletedProcess(["antiword"], 0, stdout="DRAFT B/L ZIHWBK260412LH067-Q\nAN-99999\n", stderr=""),
             subprocess.CompletedProcess(["antiword"], 0, stdout="DRAFT B/L ZIHWBK260412LH082-Q\nAN-99999\n", stderr=""),
         ]):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-99999",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    first_matches = updated_registry["received_documents"][0]["tms_matches"]
    second_matches = updated_registry["received_documents"][1]["tms_matches"]
    assert [row["tms_document_id"] for row in first_matches] == ["tms-bl-067"]
    assert [row["tms_document_id"] for row in second_matches] == ["tms-bl-082"]


def test_supplier_invoice_to_forwarder_is_billing_not_commercial_invoice(tmp_path):
    case_root = tmp_path / "orders" / "AN-42424"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "mfd-agent-invoice.pdf"
    doc_path.write_bytes(b"%PDF-1.4 fake pdf bytes")

    registry = {
        "received_documents": [{
            "filename": doc_path.name,
            "stored_path": str(doc_path),
            "mime_type": "application/pdf",
            "sha256": "bill42424",
            "message_id": "m-billing",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["commercial_invoice", "packing_list"],
        "received_types": [],
        "missing_types": ["commercial_invoice", "packing_list"],
    }
    tms_snapshot = {"detail": {"company_name": "Party-Event", "documents": [{"document_type": "Commercial Invoice", "required": True}]}}
    llm_json = json.dumps({
        "doc_type": "billing",
        "confidence": "high",
        "summary": "Agenten-/Lieferantenrechnung an Hartmann/CARGOLO, nicht Handelsrechnung der Ware.",
        "shipment_numbers": ["AN-42424"],
        "references": ["INV-MFD-1"],
        "suggested_registry_types": ["billing"],
        "extracted_fields": {"invoice_number": "INV-MFD-1", "amount": "6020", "currency": "USD"},
        "missing_or_unreadable": [],
        "consistency_notes": ["Rechnungsempfänger ist Hartmann International, nicht der Warenkäufer."],
        "operational_flags": ["SUPPLIER_INVOICE"],
        "reply_relevance": "low",
    }, ensure_ascii=False)

    def _fake_call_llm(**kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.load_config", return_value={"auxiliary": {"document_analysis": {"pdf_engine": "native"}}}):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-42424",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    assert updated_registry["received_types"] == ["billing"]
    assert updated_registry["missing_types"] == ["commercial_invoice", "packing_list"]
    assert updated_registry["received_documents"][0]["analysis_doc_type"] == "billing"


def test_internal_forwarder_invoice_is_coerced_from_commercial_invoice_to_billing(tmp_path):
    case_root = tmp_path / "orders" / "AN-42425"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "hartmann-invoice.pdf"
    doc_path.write_bytes(b"%PDF-1.4 fake pdf bytes")

    registry = {
        "received_documents": [{
            "filename": doc_path.name,
            "stored_path": str(doc_path),
            "mime_type": "application/pdf",
            "sha256": "bill42425",
            "message_id": "m-billing-2",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["commercial_invoice"],
        "received_types": [],
        "missing_types": ["commercial_invoice"],
    }
    tms_snapshot = {"detail": {"company_name": "Party-Event"}}
    llm_json = json.dumps({
        "doc_type": "commercial_invoice",
        "confidence": "high",
        "summary": "Eingangsrechnung von Henan MFD International Logistics an Hartmann International über Frachtkosten für AN-42425.",
        "shipment_numbers": ["AN-42425"],
        "references": ["INV-MFD-2"],
        "suggested_registry_types": ["commercial_invoice"],
        "extracted_fields": {"invoice_number": "INV-MFD-2", "amount": "6020", "currency": "USD", "carrier": "Henan MFD International Logistics Co.,Ltd"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)

    def _fake_call_llm(**kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.load_config", return_value={"auxiliary": {"document_analysis": {"pdf_engine": "native"}}}):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-42425",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    assert updated_registry["received_types"] == ["billing"]
    assert updated_registry["missing_types"] == ["commercial_invoice"]
    assert updated_registry["received_documents"][0]["analysis_doc_type"] == "billing"


def test_offer_with_forwarder_name_is_not_coerced_to_billing(tmp_path):
    case_root = tmp_path / "orders" / "AN-42426"
    inbound = case_root / "documents" / "inbound"
    inbound.mkdir(parents=True, exist_ok=True)
    doc_path = inbound / "hartmann-offer.pdf"
    doc_path.write_bytes(b"%PDF-1.4 fake pdf bytes")

    registry = {
        "received_documents": [{
            "filename": doc_path.name,
            "stored_path": str(doc_path),
            "mime_type": "application/pdf",
            "sha256": "offer42426",
            "message_id": "m-offer",
            "received_at": "2026-04-13T10:00:00Z",
            "detected_types": [],
        }],
        "expected_types": ["commercial_invoice"],
        "received_types": [],
        "missing_types": ["commercial_invoice"],
    }
    tms_snapshot = {"detail": {"company_name": "Party-Event"}}
    llm_json = json.dumps({
        "doc_type": "commercial_invoice",
        "confidence": "medium",
        "summary": "Preisangebot von Hartmann International für Bahnfracht nach Duisburg.",
        "shipment_numbers": ["AN-42426"],
        "references": ["QUOTE-1"],
        "suggested_registry_types": ["quotation"],
        "extracted_fields": {"amount": "7700", "currency": "EUR"},
        "missing_or_unreadable": [],
        "consistency_notes": [],
        "operational_flags": [],
        "reply_relevance": "low",
    }, ensure_ascii=False)

    def _fake_call_llm(**kwargs):
        return _FakeResponse(llm_json)

    with patch("plugins.cargolo_ops.document_analysis._resolve_task_provider_model", return_value=("openrouter", "google/gemini-3-flash-preview", None, None)), \
         patch("plugins.cargolo_ops.document_analysis.call_llm", side_effect=_fake_call_llm), \
         patch("plugins.cargolo_ops.document_analysis.load_config", return_value={"auxiliary": {"document_analysis": {"pdf_engine": "native"}}}):
        updated_registry, _ = analyze_case_documents(
            order_id="AN-42426",
            case_root=case_root,
            registry=registry,
            tms_snapshot=tms_snapshot,
        )

    assert updated_registry["received_documents"][0]["analysis_doc_type"] == "commercial_invoice"
    assert updated_registry["received_types"] == ["commercial_invoice", "quotation"]
