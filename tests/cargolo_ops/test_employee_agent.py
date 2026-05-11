from __future__ import annotations

from plugins.cargolo_ops.employee_agent import (
    BoundaryAction,
    ContextNeed,
    EmployeeRequest,
    ResponseMode,
    SpecialistPlan,
    handle_employee_request,
)


def test_normal_non_cargolo_chat_stays_free_agent_mode() -> None:
    request = EmployeeRequest(text="Kannst du mir den Unterschied zwischen ETA und ETD erklären?", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.FREE_CHAT
    assert response.agent_first is True
    assert response.requires_guard is False
    assert response.boundary_action is BoundaryAction.NONE
    assert response.context_needs == []
    assert response.specialist_plan == SpecialistPlan(tasks=[])
    assert response.should_send_to_teams is False
    assert response.should_write_tms is False
    assert response.can_answer_normally is True


def test_customer_send_or_reply_words_are_guarded_not_auto_sent() -> None:
    for text in (
        "Sende dem Kunden bitte, dass wir die CI noch brauchen.",
        "Antworte dem Kunden kurz mit Danke für die Unterlagen.",
        "Mail dem Kunden bitte die Rückfrage zur PL.",
    ):
        response = handle_employee_request(EmployeeRequest(text=text, channel="telegram"))

        assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
        assert response.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_SEND
        assert response.requires_guard is True
        assert response.should_send_customer_message is False
        assert response.can_answer_normally is True
        assert response.draft_instruction


def test_customer_reply_request_is_draft_only_not_send() -> None:
    request = EmployeeRequest(
        text="Schreib dem Kunden kurz, dass wir die Commercial Invoice noch brauchen.",
        channel="telegram",
    )

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.DRAFT_ONLY
    assert response.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_DRAFT
    assert response.requires_guard is False
    assert response.can_answer_normally is True
    assert response.should_send_customer_message is False
    assert response.draft_instruction
    assert "nicht senden" in response.safety_notes[0].lower()


def test_explicit_customer_draft_request_stays_draft_only() -> None:
    request = EmployeeRequest(
        text="Mach mir nur einen Entwurf an den Kunden, dass wir die CI noch brauchen.",
        channel="telegram",
    )

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.DRAFT_ONLY
    assert response.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_DRAFT
    assert response.requires_guard is False
    assert response.should_send_customer_message is False


def test_case_question_builds_dynamic_context_plan_without_being_stiff() -> None:
    request = EmployeeRequest(text="Was ist mit AN-11755 los? Schau bitte kurz in Mails und TMS.", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.CASE_ASSIST
    assert response.order_id == "AN-11755"
    assert response.can_answer_normally is True
    assert response.requires_guard is False
    assert response.context_needs == [ContextNeed.CASE_FOLDER, ContextNeed.MAIL_HISTORY, ContextNeed.TMS_SNAPSHOT]
    assert [task["agent"] for task in response.specialist_plan.tasks] == ["case_context", "mail_history", "tms_snapshot"]
    assert all(task["mode"] == "read_only" for task in response.specialist_plan.tasks)
    assert response.should_send_to_teams is False


def test_teams_thread_read_request_is_case_assist_not_send_guard() -> None:
    request = EmployeeRequest(text="Fasse den Teams-Thread zu AN-11755 zusammen", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.CASE_ASSIST
    assert response.boundary_action is BoundaryAction.NONE
    assert response.requires_guard is False
    assert ContextNeed.TEAMS_THREAD in response.context_needs
    assert response.should_send_to_teams is False


def test_tms_write_request_becomes_guarded_action_not_agent_reply_only() -> None:
    request = EmployeeRequest(text="Bitte trage MRN 26DE99999 in AN-11755 im TMS ein", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
    assert response.boundary_action is BoundaryAction.TMS_WRITE
    assert response.requires_guard is True
    assert response.can_answer_normally is True
    assert response.should_write_tms is False
    assert response.specialist_plan.tasks == []
    assert "Approval" in response.guard_reason


def test_customer_draft_mentioning_mrn_is_not_tms_write_guard() -> None:
    request = EmployeeRequest(text="Schreib dem Kunden die MRN 26DE99999 für AN-11755", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.DRAFT_ONLY
    assert response.boundary_action is BoundaryAction.CUSTOMER_MESSAGE_DRAFT
    assert response.should_send_customer_message is False
    assert response.should_write_tms is False


def test_common_german_tms_field_write_imperatives_are_guarded() -> None:
    for text in (
        "Setze MRN 26DE99999 in AN-11755",
        "Update MRN 26DE99999 in AN-11755",
        "Aktualisier HBL HBL123 in AN-11755",
    ):
        response = handle_employee_request(EmployeeRequest(text=text, channel="telegram"))

        assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
        assert response.boundary_action is BoundaryAction.TMS_WRITE
        assert response.should_write_tms is False
        assert response.specialist_plan.tasks == []


def test_tms_field_write_without_word_tms_is_still_guarded() -> None:
    request = EmployeeRequest(text="Bitte MRN 26DE99999 in AN-11755 eintragen", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
    assert response.boundary_action is BoundaryAction.TMS_WRITE
    assert response.requires_guard is True
    assert response.should_write_tms is False
    assert response.specialist_plan.tasks == []


def test_informational_update_about_mrn_is_case_assist_not_tms_write() -> None:
    for text in (
        "Gib mir ein Update zur MRN für AN-11755",
        "Was ist das Update zur MRN bei AN-11755?",
    ):
        response = handle_employee_request(EmployeeRequest(text=text, channel="telegram"))

        assert response.mode == ResponseMode.CASE_ASSIST
        assert response.boundary_action is BoundaryAction.NONE
        assert response.requires_guard is False
        assert response.should_write_tms is False


def test_teams_send_request_is_guarded_but_normal_draft_remains_possible() -> None:
    request = EmployeeRequest(text="Poste das Update zu AN-11755 in Teams: Dokumente sind in Prüfung", channel="telegram")

    response = handle_employee_request(request)

    assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
    assert response.boundary_action is BoundaryAction.TEAMS_SEND
    assert response.requires_guard is True
    assert response.should_send_to_teams is False
    assert response.order_id == "AN-11755"
    assert response.can_answer_normally is True
    assert response.draft_instruction


def test_document_upload_and_cron_outbound_are_guarded_boundaries() -> None:
    examples = (
        ("Lade die CI für AN-11755 ins TMS hoch", BoundaryAction.DOCUMENT_UPLOAD),
        ("Richte einen Cron ein, der Teams automatisch jeden Morgen über AN-11755 informiert", BoundaryAction.CRON_OUTBOUND),
    )
    for text, action in examples:
        response = handle_employee_request(EmployeeRequest(text=text, channel="telegram"))

        assert response.mode == ResponseMode.GUARDED_ACTION_REQUIRED
        assert response.boundary_action is action
        assert response.requires_guard is True
        assert response.should_send_to_teams is False
        assert response.should_write_tms is False
        assert response.should_send_customer_message is False


def test_agent_first_response_can_be_serialized_for_audit_without_side_effects() -> None:
    request = EmployeeRequest(text="Prüf AN-11755 komplett mit Dokumenten und Mailhistorie", channel="teams")

    response = handle_employee_request(request)
    row = response.to_audit_row()

    assert row["mode"] == "case_assist"
    assert row["agent_first"] is True
    assert row["should_send_to_teams"] is False
    assert row["should_write_tms"] is False
    assert row["should_send_customer_message"] is False
    assert row["specialist_plan"]["tasks"]
