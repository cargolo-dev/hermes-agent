from __future__ import annotations

import pytest


@pytest.fixture(autouse=True)
def _disable_live_cargolo_tms_writes(monkeypatch):
    """Prevent cargolo_ops tests from touching the live TMS write backend.

    Some cargolo_ops tests use realistic shipment numbers like AN-10874 / BU-4638.
    Without an explicit stub, helper paths such as _add_transport_internal_note()
    can hit the configured live MCP write provider and mutate production shipments.
    Keep test runs hermetic by disabling the write provider and immediate repo sync
    unless an individual test deliberately overrides these with its own patch.
    """

    monkeypatch.setattr(
        "plugins.cargolo_ops.processor.build_tms_write_provider_from_env",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        "plugins.cargolo_ops.writeback_actions.build_tms_write_provider_from_env",
        lambda: None,
        raising=False,
    )
    monkeypatch.setattr(
        "plugins.cargolo_ops.processor._sync_orders_repo_immediately",
        lambda order_id: None,
        raising=False,
    )