from pathlib import Path

from plugins.cargolo_ops.analysis import _parse_specialist_outputs, _synthesize_brief
from plugins.cargolo_ops.models import ProcessingResult


def test_parse_specialist_outputs_recovers_case_local_ops_action_file(tmp_path: Path):
    analysis_dir = tmp_path / "analysis"
    analysis_dir.mkdir()
    (analysis_dir / "ops_action.json").write_text(
        """
        {
          "role": "ops_action",
          "summary": "Operative Bewertung aus Fallartefakt.",
          "priority": "high",
          "recommended_actions": [
            {
              "action": "Zolldokumente prüfen",
              "urgency": "high",
              "owner_role": "customs",
              "reason": "ETA nähert sich.",
              "blocking": true
            }
          ],
          "sla_risk": "medium",
          "handoff_needed": true,
          "watch_items": ["ETA 09.05.2026"],
          "confidence": "high",
          "files_used": ["/tmp/case_state.json"]
        }
        """,
        encoding="utf-8",
    )

    specialists = _parse_specialist_outputs(
        [{"summary": "Ich habe die Datei analysis/ops_action.json geschrieben."}],
        case_root=tmp_path,
    )

    assert len(specialists) == 1
    assert specialists[0].role == "ops_action"
    assert specialists[0].summary == "Operative Bewertung aus Fallartefakt."


def test_parse_specialist_outputs_does_not_raise_when_delegate_summary_has_no_json(tmp_path: Path):
    specialists = _parse_specialist_outputs(
        [{"summary": "Kein JSON in der Subagent-Antwort."}],
        case_root=tmp_path,
    )

    assert specialists == []


def test_synthesize_brief_uses_deterministic_fallback_without_specialists():
    result = ProcessingResult(
        status="processed",
        order_id="AN-11886",
        case_root="/tmp/AN-11886",
        timeline_entry="Sendung verarbeitet; TMS geprüft.",
    )

    brief = _synthesize_brief(result, [])

    assert brief.order_id == "AN-11886"
    assert brief.priority == "medium"
    assert brief.ops_summary == "Sendung verarbeitet; TMS geprüft."
    assert brief.provenance["synthesis_mode"] == "fallback_from_specialists"
