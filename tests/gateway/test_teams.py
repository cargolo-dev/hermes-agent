"""Tests for the Microsoft Teams platform adapter plugin."""

import asyncio
import json
import os
import sys
import types
from pathlib import Path
from types import SimpleNamespace
from unittest.mock import AsyncMock, MagicMock, patch

import httpx
import pytest

from gateway.config import Platform, PlatformConfig, HomeChannel
from plugins.teams_pipeline.models import TeamsMeetingRef, TeamsMeetingSummaryPayload
from tests.gateway._plugin_adapter_loader import load_plugin_adapter


# ---------------------------------------------------------------------------
# SDK Mock — install in sys.modules before importing the adapter
# ---------------------------------------------------------------------------

def _ensure_teams_mock():
    """Install a teams SDK mock in sys.modules if the real package isn't present."""
    if "microsoft_teams" in sys.modules and hasattr(sys.modules["microsoft_teams"], "__file__"):
        return

    # Build the module hierarchy
    microsoft_teams = types.ModuleType("microsoft_teams")
    microsoft_teams_apps = types.ModuleType("microsoft_teams.apps")
    microsoft_teams_api = types.ModuleType("microsoft_teams.api")
    microsoft_teams_api_activities = types.ModuleType("microsoft_teams.api.activities")
    microsoft_teams_api_activities_typing = types.ModuleType("microsoft_teams.api.activities.typing")
    microsoft_teams_api_activities_invoke = types.ModuleType("microsoft_teams.api.activities.invoke")
    microsoft_teams_api_activities_invoke_adaptive_card = types.ModuleType(
        "microsoft_teams.api.activities.invoke.adaptive_card"
    )
    microsoft_teams_common = types.ModuleType("microsoft_teams.common")
    microsoft_teams_common_http = types.ModuleType("microsoft_teams.common.http")
    microsoft_teams_common_http_client = types.ModuleType("microsoft_teams.common.http.client")
    microsoft_teams_api_models = types.ModuleType("microsoft_teams.api.models")
    microsoft_teams_api_models_adaptive_card = types.ModuleType("microsoft_teams.api.models.adaptive_card")
    microsoft_teams_api_models_invoke_response = types.ModuleType("microsoft_teams.api.models.invoke_response")
    microsoft_teams_cards = types.ModuleType("microsoft_teams.cards")
    microsoft_teams_apps_http = types.ModuleType("microsoft_teams.apps.http")
    microsoft_teams_apps_http_adapter = types.ModuleType("microsoft_teams.apps.http.adapter")

    # App class mock
    class MockApp:
        def __init__(self, **kwargs):
            self._client_id = kwargs.get("client_id")
            self.server = MagicMock()
            self.server.handle_request = AsyncMock(return_value={"status": 200, "body": None})
            self.credentials = MagicMock()
            self.credentials.client_id = self._client_id

        @property
        def id(self):
            return self._client_id

        def on_message(self, func):
            self._message_handler = func
            return func

        def on_card_action(self, func):
            self._card_action_handler = func
            return func

        async def initialize(self):
            pass

        async def send(self, conversation_id, activity):
            result = MagicMock()
            result.id = "sent-activity-id"
            return result

        async def start(self, port=3978):
            pass

        async def stop(self):
            pass

    microsoft_teams_apps.App = MockApp
    microsoft_teams_apps.ActivityContext = MagicMock
    microsoft_teams_common_http_client.ClientOptions = MagicMock

    # MessageActivity mock
    microsoft_teams_api.MessageActivity = MagicMock
    microsoft_teams_api.ConversationReference = MagicMock
    microsoft_teams_api.MessageActivityInput = MagicMock

    # TypingActivityInput mock
    class MockTypingActivityInput:
        pass

    microsoft_teams_api_activities_typing.TypingActivityInput = MockTypingActivityInput

    # Adaptive card invoke activity mock
    microsoft_teams_api_activities_invoke_adaptive_card.AdaptiveCardInvokeActivity = MagicMock

    # Adaptive card response mocks
    microsoft_teams_api_models_adaptive_card.AdaptiveCardActionCardResponse = MagicMock
    microsoft_teams_api_models_adaptive_card.AdaptiveCardActionMessageResponse = MagicMock

    # Invoke response mocks
    class MockInvokeResponse:
        def __init__(self, status=200, body=None):
            self.status = status
            self.body = body

    microsoft_teams_api_models_invoke_response.InvokeResponse = MockInvokeResponse
    microsoft_teams_api_models_invoke_response.AdaptiveCardInvokeResponse = MagicMock

    # Cards mocks
    class MockAdaptiveCard:
        def with_version(self, v):
            return self

        def with_body(self, body):
            return self

        def with_actions(self, actions):
            return self

    microsoft_teams_cards.AdaptiveCard = MockAdaptiveCard
    microsoft_teams_cards.ExecuteAction = MagicMock
    microsoft_teams_cards.TextBlock = MagicMock

    # HttpRequest TypedDict mock
    def HttpRequest(body=None, headers=None):
        return {"body": body, "headers": headers}

    # HttpResponse TypedDict mock
    HttpResponse = dict
    HttpMethod = str
    from typing import Callable
    HttpRouteHandler = Callable

    microsoft_teams_apps_http_adapter.HttpRequest = HttpRequest
    microsoft_teams_apps_http_adapter.HttpResponse = HttpResponse
    microsoft_teams_apps_http_adapter.HttpMethod = HttpMethod
    microsoft_teams_apps_http_adapter.HttpRouteHandler = HttpRouteHandler

    # Wire the hierarchy
    for name, mod in {
        "microsoft_teams": microsoft_teams,
        "microsoft_teams.apps": microsoft_teams_apps,
        "microsoft_teams.api": microsoft_teams_api,
        "microsoft_teams.api.activities": microsoft_teams_api_activities,
        "microsoft_teams.api.activities.typing": microsoft_teams_api_activities_typing,
        "microsoft_teams.api.activities.invoke": microsoft_teams_api_activities_invoke,
        "microsoft_teams.api.activities.invoke.adaptive_card": microsoft_teams_api_activities_invoke_adaptive_card,
        "microsoft_teams.common": microsoft_teams_common,
        "microsoft_teams.common.http": microsoft_teams_common_http,
        "microsoft_teams.common.http.client": microsoft_teams_common_http_client,
        "microsoft_teams.api.models": microsoft_teams_api_models,
        "microsoft_teams.api.models.adaptive_card": microsoft_teams_api_models_adaptive_card,
        "microsoft_teams.api.models.invoke_response": microsoft_teams_api_models_invoke_response,
        "microsoft_teams.cards": microsoft_teams_cards,
        "microsoft_teams.apps.http": microsoft_teams_apps_http,
        "microsoft_teams.apps.http.adapter": microsoft_teams_apps_http_adapter,
    }.items():
        sys.modules.setdefault(name, mod)


_ensure_teams_mock()

# Load plugins/platforms/teams/adapter.py under a unique module name
# (plugin_adapter_teams) so it cannot collide with sibling plugin adapters.
_teams_mod = load_plugin_adapter("teams")

_teams_mod.TEAMS_SDK_AVAILABLE = True
_teams_mod.AIOHTTP_AVAILABLE = True

# Ensure SDK symbols that were None (import failed on Python <3.12) are
# replaced with the mocked versions so runtime calls don't silently no-op.
import sys as _sys
_mt = _sys.modules.get("microsoft_teams.api.activities.typing")
if _mt and _teams_mod.TypingActivityInput is None:
    _teams_mod.TypingActivityInput = _mt.TypingActivityInput

TeamsAdapter = _teams_mod.TeamsAdapter
TeamsSummaryWriter = _teams_mod.TeamsSummaryWriter
check_requirements = _teams_mod.check_requirements
check_teams_requirements = _teams_mod.check_teams_requirements
validate_config = _teams_mod.validate_config
register = _teams_mod.register


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _make_config(**extra):
    return PlatformConfig(enabled=True, extra=extra)


# ---------------------------------------------------------------------------
# Tests: Requirements
# ---------------------------------------------------------------------------

class TestTeamsRequirements:
    def test_returns_false_when_sdk_missing(self, monkeypatch):
        monkeypatch.setattr(_teams_mod, "TEAMS_SDK_AVAILABLE", False)
        assert check_requirements() is False

    def test_returns_false_when_aiohttp_missing(self, monkeypatch):
        monkeypatch.setattr(_teams_mod, "AIOHTTP_AVAILABLE", False)
        assert check_requirements() is False

    def test_returns_true_when_deps_available(self, monkeypatch):
        monkeypatch.setattr(_teams_mod, "TEAMS_SDK_AVAILABLE", True)
        monkeypatch.setattr(_teams_mod, "AIOHTTP_AVAILABLE", True)
        assert check_requirements() is True

    def test_alias_matches(self, monkeypatch):
        monkeypatch.setattr(_teams_mod, "TEAMS_SDK_AVAILABLE", True)
        monkeypatch.setattr(_teams_mod, "AIOHTTP_AVAILABLE", True)
        assert check_teams_requirements() is True

    def test_validate_config_with_env(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "test-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "test-secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "test-tenant")
        assert validate_config(_make_config()) is True

    def test_validate_config_from_extra(self, monkeypatch):
        monkeypatch.delenv("TEAMS_CLIENT_ID", raising=False)
        monkeypatch.delenv("TEAMS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("TEAMS_TENANT_ID", raising=False)
        cfg = _make_config(client_id="id", client_secret="x", tenant_id="tenant")
        assert validate_config(cfg) is True

    def test_validate_config_missing(self, monkeypatch):
        monkeypatch.delenv("TEAMS_CLIENT_ID", raising=False)
        monkeypatch.delenv("TEAMS_CLIENT_SECRET", raising=False)
        monkeypatch.delenv("TEAMS_TENANT_ID", raising=False)
        assert validate_config(_make_config()) is False

    def test_validate_config_missing_tenant(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "test-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "test-secret")
        monkeypatch.delenv("TEAMS_TENANT_ID", raising=False)
        assert validate_config(_make_config()) is False


# ---------------------------------------------------------------------------
# Tests: Adapter Init
# ---------------------------------------------------------------------------

class TestTeamsAdapterInit:
    def test_reads_config_from_extra(self):
        config = _make_config(
            client_id="cfg-id",
            client_secret="cfg-secret",
            tenant_id="cfg-tenant",
        )
        adapter = TeamsAdapter(config)
        assert adapter._client_id == "cfg-id"
        assert adapter._client_secret == "cfg-secret"
        assert adapter._tenant_id == "cfg-tenant"

    def test_falls_back_to_env_vars(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "env-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "env-secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "env-tenant")
        adapter = TeamsAdapter(_make_config())
        assert adapter._client_id == "env-id"
        assert adapter._client_secret == "env-secret"
        assert adapter._tenant_id == "env-tenant"

    def test_default_port(self):
        adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
        assert adapter._port == 3978

    def test_custom_port_from_extra(self):
        adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant", port=4000))
        assert adapter._port == 4000

    def test_custom_port_from_env(self, monkeypatch):
        monkeypatch.setenv("TEAMS_PORT", "5000")
        adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
        assert adapter._port == 5000

    def test_platform_value(self):
        adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
        assert adapter.platform.value == "teams"


# ---------------------------------------------------------------------------
# Tests: Plugin registration
# ---------------------------------------------------------------------------

class TestTeamsPluginRegistration:

    def test_register_calls_ctx(self):
        ctx = MagicMock()
        register(ctx)
        ctx.register_platform.assert_called_once()

    def test_register_name(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs["name"] == "teams"

    def test_register_auth_env_vars(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs["allowed_users_env"] == "TEAMS_ALLOWED_USERS"
        assert kwargs["allow_all_env"] == "TEAMS_ALLOW_ALL_USERS"

    def test_register_max_message_length(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs["max_message_length"] == 28000

    def test_register_has_setup_fn(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args[1]
        assert callable(kwargs.get("setup_fn"))

    def test_register_has_platform_hint(self):
        ctx = MagicMock()
        register(ctx)
        kwargs = ctx.register_platform.call_args[1]
        assert kwargs.get("platform_hint")


# ---------------------------------------------------------------------------
# Tests: Interactive setup (import fix regression — #18325 / #19173)
# ---------------------------------------------------------------------------

class TestTeamsInteractiveSetup:
    def test_interactive_setup_persists_credentials(self, tmp_path, monkeypatch):
        """Regression for #19173: interactive_setup must import prompt helpers
        from hermes_cli.cli_output (not hermes_cli.config) and persist
        credentials to .env without crashing.
        """
        hermes_home = tmp_path / "hermes"
        monkeypatch.setenv("HERMES_HOME", str(hermes_home))

        import hermes_cli.cli_output as cli_output_mod

        answers = iter(["client-id", "client-secret", "tenant-id", "aad-1, aad-2"])
        monkeypatch.setattr(cli_output_mod, "prompt", lambda *_a, **_kw: next(answers))
        monkeypatch.setattr(cli_output_mod, "prompt_yes_no", lambda *_a, **_kw: True)
        monkeypatch.setattr(cli_output_mod, "print_info", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli_output_mod, "print_success", lambda *_a, **_kw: None)
        monkeypatch.setattr(cli_output_mod, "print_warning", lambda *_a, **_kw: None)

        _teams_mod.interactive_setup()

        env_text = (hermes_home / ".env").read_text(encoding="utf-8")
        assert "TEAMS_CLIENT_ID=client-id" in env_text
        assert "TEAMS_TENANT_ID=tenant-id" in env_text

class TestTeamsConnect:
    @pytest.mark.anyio
    async def test_connect_fails_without_sdk(self, monkeypatch):
        monkeypatch.setattr(_teams_mod, "TEAMS_SDK_AVAILABLE", False)
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        result = await adapter.connect()
        assert result is False

    @pytest.mark.anyio
    async def test_connect_fails_without_credentials(self):
        adapter = TeamsAdapter(_make_config())
        adapter._client_id = ""
        adapter._client_secret = ""
        adapter._tenant_id = ""
        result = await adapter.connect()
        assert result is False

    @pytest.mark.anyio
    async def test_disconnect_cleans_up(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        adapter._running = True
        mock_runner = AsyncMock()
        adapter._runner = mock_runner
        adapter._app = MagicMock()

        await adapter.disconnect()
        assert adapter._running is False
        assert adapter._app is None
        assert adapter._runner is None
        mock_runner.cleanup.assert_awaited_once()


# ---------------------------------------------------------------------------
# Tests: Send
# ---------------------------------------------------------------------------

class TestTeamsSend:
    @pytest.mark.anyio
    async def test_send_returns_error_without_app(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = None
        result = await adapter.send("conv-id", "Hello")
        assert result.success is False
        assert "not initialized" in result.error

    @pytest.mark.anyio
    async def test_send_calls_app_send(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "msg-123"
        mock_app = MagicMock()
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app

        result = await adapter.send("conv-id", "Hello")
        assert result.success is True
        assert result.message_id == "msg-123"
        mock_app.send.assert_awaited_once_with("conv-id", "Hello")

    @pytest.mark.anyio
    async def test_send_preserves_plain_text_line_breaks_for_teams(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "msg-123"
        mock_app = MagicMock()
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app

        result = await adapter.send("conv-id", "AN-1\n- Punkt <prüfen>")
        assert result.success is True
        mock_app.send.assert_awaited_once_with("conv-id", "AN-1<br>- Punkt &lt;prüfen&gt;")

    @pytest.mark.anyio
    async def test_send_keeps_explicit_html_payloads(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "msg-123"
        mock_app = MagicMock()
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app

        result = await adapter.send("conv-id", "<b>AN-1</b><br>- Punkt")
        assert result.success is True
        mock_app.send.assert_awaited_once_with("conv-id", "<b>AN-1</b><br>- Punkt")

    @pytest.mark.anyio
    async def test_send_handles_error(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        mock_app = MagicMock()
        mock_app.send = AsyncMock(side_effect=Exception("Network error"))
        adapter._app = mock_app

        result = await adapter.send("conv-id", "Hello")
        assert result.success is False
        assert "Network error" in result.error

    @pytest.mark.anyio
    async def test_send_typing(self):
        adapter = TeamsAdapter(_make_config(
            client_id="id", client_secret="x", tenant_id="tenant",
        ))
        mock_app = MagicMock()
        mock_app.send = AsyncMock()
        adapter._app = mock_app

        await adapter.send_typing("conv-id")
        mock_app.send.assert_awaited_once()
        call_args = mock_app.send.call_args
        assert call_args[0][0] == "conv-id"


def _make_summary_payload():
    return TeamsMeetingSummaryPayload(
        meeting_ref=TeamsMeetingRef(meeting_id="meeting-123"),
        title="Weekly Sync",
        summary="Discussed launch readiness.",
        key_decisions=["Proceed with staged rollout."],
        action_items=["Send launch checklist."],
        risks=["QA sign-off still pending."],
    )


class TestTeamsSummaryWriter:
    @pytest.mark.anyio
    async def test_incoming_webhook_posts_summary_text(self):
        seen = {}

        def _handler(request: httpx.Request) -> httpx.Response:
            seen["url"] = str(request.url)
            seen["body"] = json.loads(request.content.decode("utf-8"))
            return httpx.Response(200, json={"ok": True})

        writer = TeamsSummaryWriter(transport=httpx.MockTransport(_handler))
        payload = _make_summary_payload()

        result = await writer.write_summary(
            payload,
            {
                "delivery_mode": "incoming_webhook",
                "incoming_webhook_url": "https://example.test/teams-webhook",
            },
        )

        assert result["delivery_mode"] == "incoming_webhook"
        assert seen["url"] == "https://example.test/teams-webhook"
        assert "Weekly Sync" in seen["body"]["text"]
        assert "Proceed with staged rollout." in seen["body"]["text"]

    @pytest.mark.anyio
    async def test_graph_delivery_posts_to_channel(self):
        graph_client = SimpleNamespace(
            post_json=AsyncMock(return_value={"id": "msg-123", "webUrl": "https://teams.example/messages/123"})
        )
        writer = TeamsSummaryWriter(graph_client=graph_client)
        payload = _make_summary_payload()

        result = await writer.write_summary(
            payload,
            {
                "delivery_mode": "graph",
                "team_id": "team-1",
                "channel_id": "channel-1",
            },
        )

        assert result["target_type"] == "channel"
        assert result["message_id"] == "msg-123"
        graph_client.post_json.assert_awaited_once()
        path = graph_client.post_json.await_args.args[0]
        body = graph_client.post_json.await_args.kwargs["json_body"]
        assert path == "/teams/team-1/channels/channel-1/messages"
        assert body["body"]["contentType"] == "html"
        assert "Weekly Sync" in body["body"]["content"]

    @pytest.mark.anyio
    async def test_graph_delivery_falls_back_to_platform_home_channel(self):
        graph_client = SimpleNamespace(post_json=AsyncMock(return_value={"id": "msg-home"}))
        platform_config = PlatformConfig(
            enabled=True,
            extra={"team_id": "team-home", "delivery_mode": "graph"},
            home_channel=HomeChannel(
                platform=Platform("teams"),
                chat_id="channel-home",
                name="Teams Home",
            ),
        )
        writer = TeamsSummaryWriter(platform_config=platform_config, graph_client=graph_client)

        await writer.write_summary(_make_summary_payload(), {})

        graph_client.post_json.assert_awaited_once()
        assert graph_client.post_json.await_args.args[0] == "/teams/team-home/channels/channel-home/messages"

    @pytest.mark.anyio
    async def test_existing_record_is_reused_without_force_resend(self):
        graph_client = SimpleNamespace(post_json=AsyncMock())
        writer = TeamsSummaryWriter(graph_client=graph_client)
        existing = {"delivery_mode": "graph", "message_id": "msg-existing"}

        result = await writer.write_summary(
            _make_summary_payload(),
            {
                "delivery_mode": "graph",
                "team_id": "team-1",
                "channel_id": "channel-1",
            },
            existing_record=existing,
        )

        assert result == existing
        graph_client.post_json.assert_not_awaited()


# ---------------------------------------------------------------------------
# Tests: Message Handling
# ---------------------------------------------------------------------------

class TestTeamsMessageHandling:
    def _make_activity(
        self,
        *,
        text="Hello",
        from_id="user-123",
        from_aad_id="aad-456",
        from_name="Test User",
        conversation_id="19:abc@thread.v2",
        conversation_type="personal",
        tenant_id="tenant-789",
        activity_id="activity-001",
        attachments=None,
        reply_to_id=None,
        channel_data=None,
    ):
        activity = MagicMock()
        activity.text = text
        activity.id = activity_id
        activity.from_ = MagicMock()
        activity.from_.id = from_id
        activity.from_.aad_object_id = from_aad_id
        activity.from_.name = from_name
        activity.conversation = MagicMock()
        activity.conversation.id = conversation_id
        activity.conversation.conversation_type = conversation_type
        activity.conversation.name = "Test Chat"
        activity.conversation.tenant_id = tenant_id
        activity.attachments = attachments or []
        if channel_data is not None:
            activity.channel_data = channel_data
        if reply_to_id is not None:
            activity.reply_to_id = reply_to_id
        return activity

    def _make_ctx(self, activity):
        ctx = MagicMock()
        ctx.activity = activity
        return ctx

    @pytest.mark.anyio
    async def test_personal_message_creates_dm_event(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(conversation_type="personal")
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args[0][0]
        assert event.source.chat_type == "dm"

    @pytest.mark.anyio
    async def test_group_message_creates_group_event(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(conversation_type="groupChat")
        await adapter._on_message(self._make_ctx(activity))

        event = adapter.handle_message.call_args[0][0]
        assert event.source.chat_type == "group"

    @pytest.mark.anyio
    async def test_channel_message_creates_channel_event(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(conversation_type="channel")
        await adapter._on_message(self._make_ctx(activity))

        event = adapter.handle_message.call_args[0][0]
        assert event.source.chat_type == "channel"

    @pytest.mark.anyio
    async def test_user_id_uses_aad_object_id(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(from_aad_id="aad-stable-id", from_id="teams-id")
        await adapter._on_message(self._make_ctx(activity))

        event = adapter.handle_message.call_args[0][0]
        assert event.source.user_id == "aad-stable-id"

    @pytest.mark.anyio
    async def test_self_message_filtered(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(from_id="bot-id")
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()

    @pytest.mark.anyio
    async def test_bot_mention_stripped_from_text(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="<at>Hermes</at> what is the weather?",
            from_id="user-id",
        )
        await adapter._on_message(self._make_ctx(activity))

        event = adapter.handle_message.call_args[0][0]
        assert event.text == "what is the weather?"

    @pytest.mark.anyio
    async def test_nested_channel_data_reply_id_is_forwarded_to_message_event(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="Antwort auf Karte",
            channel_data={"message": {"replyToId": "teams-card-nested"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        event = adapter.handle_message.call_args[0][0]
        assert event.reply_to_message_id == "teams-card-nested"

    @pytest.mark.asyncio
    async def test_cargolo_reply_loop_routes_tms_language_to_agent_decision(self, tmp_path, monkeypatch):
        monkeypatch.setenv("HERMES_HOME", str(tmp_path))
        from plugins.cargolo_ops.teams_reply_loop import build_card_context, record_sent_card

        record_sent_card(
            context=build_card_context(
                route_name="cargolo-asr-ops-teams",
                delivery_id="delivery-1",
                payload={"processor_result": {"order_id": "AN-11755"}, "activity_event": {"id": 1200}},
                message_id="teams-card-1",
                chat_id="19:abc@thread.v2",
            )
        )

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "ack-1"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="Ja bitte TMS MRN 26DE123 aktualisieren",
            activity_id="reply-activity-1",
            reply_to_id="teams-card-1",
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args[0][0]
        assert "AN-11755" in event.text
        assert "cargolo_asr_record_teams_tms_intent" in event.text
        mock_app.send.assert_not_awaited()
        assert (tmp_path / "cargolo_asr" / "orders" / "AN-11755" / "teams" / "replies.jsonl").exists()

    @pytest.mark.asyncio
    async def test_cargolo_employee_dedicated_channel_routes_to_safe_handoff(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        handoff_calls = []

        def fake_handle_teams_employee_message(**kwargs):
            handoff_calls.append(kwargs)
            return {
                "handled": True,
                "response_text": "Lage: AN-11755 | Keine externe Aktion ausgeführt.",
                "should_write_tms": False,
                "should_send_customer_message": False,
            }

        route_calls = []

        def fake_route_teams_ops_message(**kwargs):
            route_calls.append(kwargs)
            return {"handled": False, "reason": "no_deterministic_ops_command"}

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.route_teams_ops_message", fake_route_teams_ops_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["cargolo-hermes"],
        ))
        mock_result = MagicMock()
        mock_result.id = "employee-ack"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="Was ist mit AN-11755 los?",
            activity_id="msg-employee-dedicated",
            conversation_type="channel",
            channel_data={"channel": {"id": "cargolo-hermes"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert "Lage: AN-11755" in mock_app.send.call_args[0][1]
        assert route_calls[0]["text"] == "Was ist mit AN-11755 los?"
        assert handoff_calls[0]["channel_id"] == "cargolo-hermes"
        assert handoff_calls[0]["text"] == "Was ist mit AN-11755 los?"
        assert handoff_calls[0]["user_id"] == "aad-456"
        assert handoff_calls[0]["user_name"] == "Test User"

    @pytest.mark.asyncio
    async def test_cargolo_employee_dedicated_free_chat_falls_through_to_generic_hermes(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        handoff_calls = []

        def fake_handle_teams_employee_message(**kwargs):
            handoff_calls.append(kwargs)
            return {
                "handled": False,
                "reason": "generic_hermes_chat",
                "classification": "free_chat",
                "passthrough_text": kwargs.get("text"),
                "response_text": None,
            }

        route_calls = []

        def fake_route_teams_ops_message(**kwargs):
            route_calls.append(kwargs)
            return {"handled": False, "reason": "no_deterministic_ops_command"}

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.route_teams_ops_message", fake_route_teams_ops_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["cargolo-hermes"],
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="erzähl mal einen witz",
            activity_id="msg-employee-free-chat",
            conversation_type="channel",
            channel_data={"channel": {"id": "cargolo-hermes"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        mock_app.send.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args[0][0]
        assert event.text == "erzähl mal einen witz"
        assert len(handoff_calls) >= 1
        assert route_calls[0]["text"] == "erzähl mal einen witz"

    @pytest.mark.asyncio
    async def test_cargolo_employee_dedicated_pending_command_uses_ops_router_before_handoff(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        def fake_handle_teams_employee_message(**kwargs):
            raise AssertionError("deterministic pending command must not fall through to employee free-chat handoff")

        route_calls = []

        def fake_route_teams_ops_message(**kwargs):
            route_calls.append(kwargs)
            return {
                "handled": True,
                "classification": "pending_tms_reviews",
                "response_text": "CARGOLO Teams Ops · Offene TMS-Freigaben",
                "teams_tms_review_cards": [{
                    "action_id": "abc123",
                    "order_id": "AN-11755",
                    "target": "customs_reference",
                    "value": "26DE99999",
                    "operator": "Dominik",
                }],
            }

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.route_teams_ops_message", fake_route_teams_ops_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["cargolo-hermes"],
        ))
        mock_result = MagicMock()
        mock_result.id = "pending-ack"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()
        adapter.send_cargolo_asr_tms_review_card = AsyncMock(return_value=MagicMock(success=True))

        activity = self._make_activity(
            text="offene Freigaben",
            activity_id="msg-dedicated-pending",
            conversation_type="channel",
            channel_data={"channel": {"id": "cargolo-hermes"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert "Offene TMS-Freigaben" in mock_app.send.call_args[0][1]
        adapter.send_cargolo_asr_tms_review_card.assert_awaited_once()
        assert route_calls[0]["text"] == "offene Freigaben"
        assert route_calls[0]["root"].name == "cargolo_asr"

    @pytest.mark.asyncio
    async def test_cargolo_employee_shared_channel_requires_teams_mention(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        handoff_calls = []

        def fake_handle_teams_employee_message(**kwargs):
            handoff_calls.append(kwargs)
            return {"handled": True, "response_text": "Lage: AN-11755 | Keine externe Aktion ausgeführt."}

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["cargolo-hermes"],
        ))
        mock_result = MagicMock()
        mock_result.id = "employee-mention-ack"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="<at>Hermes CARGOLO</at> Was ist mit AN-11755 los?",
            activity_id="msg-employee-mention",
            conversation_type="channel",
            channel_data={"channel": {"id": "shared-ops"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert handoff_calls[0]["channel_id"] == "shared-ops"
        assert handoff_calls[0]["text"] == "@Hermes Was ist mit AN-11755 los?"

    @pytest.mark.asyncio
    async def test_cargolo_employee_dedicated_allowlist_can_use_conversation_id_when_channel_data_differs(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        handoff_calls = []

        def fake_handle_teams_employee_message(**kwargs):
            handoff_calls.append(kwargs)
            return {"handled": True, "response_text": "Lage: AN-11755 | Keine externe Aktion ausgeführt."}

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["19:logged-thread@thread.v2"],
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="Was ist mit AN-11755 los?",
            activity_id="msg-employee-conv-allowlist",
            conversation_id="19:logged-thread@thread.v2",
            conversation_type="channel",
            channel_data={"channel": {"id": "opaque-real-channel-id"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert handoff_calls[0]["channel_id"] == "19:logged-thread@thread.v2"
        assert handoff_calls[0]["text"] == "Was ist mit AN-11755 los?"

    @pytest.mark.asyncio
    async def test_cargolo_employee_dedicated_top_level_an_message_bypasses_card_fallback(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            raise AssertionError("top-level dedicated-channel messages must not enter ASR card reply fallback")

        handoff_calls = []

        def fake_handle_teams_employee_message(**kwargs):
            handoff_calls.append(kwargs)
            return {"handled": True, "response_text": "Employee-Handoff ok"}

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["19:logged-thread@thread.v2"],
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="<at>CARGOLO Hermes</at> channel-check AN-11755 read-only",
            activity_id="msg-employee-top-level-an",
            conversation_id="19:logged-thread@thread.v2",
            conversation_type="channel",
            channel_data={"channel": {"id": "opaque-real-channel-id"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert handoff_calls[0]["channel_id"] == "19:logged-thread@thread.v2"
        assert handoff_calls[0]["text"] == "channel-check AN-11755 read-only"

    @pytest.mark.asyncio
    async def test_cargolo_dedicated_flattened_card_quote_stays_in_reply_loop(self, monkeypatch):
        reply_calls = []

        def fake_handle_teams_message(**kwargs):
            reply_calls.append(kwargs)
            return {"handled": True, "response_text": "Reply-Loop ok"}

        def fake_handle_teams_employee_message(**kwargs):
            raise AssertionError("flattened quoted cards must stay in reply loop")

        monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
        monkeypatch.setattr("plugins.cargolo_ops.teams_employee_handoff.handle_teams_employee_message", fake_handle_teams_employee_message, raising=False)

        adapter = TeamsAdapter(_make_config(
            client_id="bot-id",
            client_secret="x",
            tenant_id="tenant",
            cargolo_employee_handoff_enabled=True,
            cargolo_employee_dedicated_channel_ids=["19:logged-thread@thread.v2"],
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text=(
                "<at>CARGOLO Hermes</at> Display NameAN-11755 | Dokument-Check | "
                "TMS-Aktion: Review | MRN 26DE12345\nBitte TMS MRN 26DE99999 eintragen"
            ),
            activity_id="msg-flattened-card-quote",
            conversation_id="19:logged-thread@thread.v2",
            conversation_type="channel",
            channel_data={"channel": {"id": "opaque-real-channel-id"}},
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert reply_calls[0]["text"].startswith("Display NameAN-11755")

    @pytest.mark.asyncio
    async def test_cargolo_ops_status_command_is_handled_before_generic_chat(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        def fake_route_teams_ops_message(**kwargs):
            return {
                "handled": True,
                "classification": "ops_status",
                "response_text": "CARGOLO Teams Ops · Status\n- Dokumenten-Monitor: scheduled · last=ok",
            }

        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_reply_loop.handle_teams_message",
            fake_handle_teams_message,
            raising=False,
        )
        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_ops_router.route_teams_ops_message",
            fake_route_teams_ops_message,
            raising=False,
        )
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "ops-status-ack"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(text="<at>Hermes CARGOLO</at> status", activity_id="msg-ops-status")
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert "CARGOLO Teams Ops" in mock_app.send.call_args[0][1]

    @pytest.mark.asyncio
    async def test_cargolo_ops_case_command_flows_to_employee_agent_prompt(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_card_context"}

        def fake_route_teams_ops_message(**kwargs):
            return {
                "handled": False,
                "allow_generic_chat": True,
                "classification": "case_deep_dive_request",
                "agent_prompt": "Rolle: ASR Ops Coordinator\nTeams-Nachricht: prüfe AN-12345 komplett",
            }

        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_reply_loop.handle_teams_message",
            fake_handle_teams_message,
            raising=False,
        )
        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_ops_router.route_teams_ops_message",
            fake_route_teams_ops_message,
            raising=False,
        )
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(text="<at>Hermes CARGOLO</at> prüfe AN-12345 komplett", activity_id="msg-case-check")
        await adapter._on_message(self._make_ctx(activity))

        mock_app.send.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args[0][0]
        assert event.text.startswith("Rolle: ASR Ops Coordinator")
        assert "AN-12345" in event.text

    @pytest.mark.asyncio
    async def test_cargolo_asr_tms_request_without_context_is_guarded_from_generic_chat(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {"handled": False, "reason": "no_context"}

        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_reply_loop.handle_teams_message",
            fake_handle_teams_message,
            raising=False,
        )
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        mock_result = MagicMock()
        mock_result.id = "guard-ack"
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock(return_value=mock_result)
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="<at>Hermes CARGOLO</at> Bitte TMS MRN 26DE99999 eintragen für AN-11755",
            activity_id="msg-guard",
        )
        await adapter._on_message(self._make_ctx(activity))

        adapter.handle_message.assert_not_awaited()
        mock_app.send.assert_awaited_once()
        assert "nicht eindeutig" in mock_app.send.call_args[0][1]


    @pytest.mark.asyncio
    async def test_cargolo_context_note_flows_to_generic_agent_prompt(self, monkeypatch):
        def fake_handle_teams_message(**kwargs):
            return {
                "handled": False,
                "allow_generic_chat": True,
                "asr_context_saved": True,
                "agent_prompt": "ASR agent prompt for BU-4664: antworte intelligent, kein TMS-Write.",
            }

        monkeypatch.setattr(
            "plugins.cargolo_ops.teams_reply_loop.handle_teams_message",
            fake_handle_teams_message,
            raising=False,
        )
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        mock_app = MagicMock()
        mock_app.id = "bot-id"
        mock_app.send = AsyncMock()
        adapter._app = mock_app
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(
            text="Display NameBU-4664 | TMS-Aktion: Review | MRN 26DE12345\nich bin kurz im termin",
            activity_id="msg-asr-agent",
            reply_to_id="teams-card-bu",
        )
        await adapter._on_message(self._make_ctx(activity))

        mock_app.send.assert_not_awaited()
        adapter.handle_message.assert_awaited_once()
        event = adapter.handle_message.call_args[0][0]
        assert event.text.startswith("ASR agent prompt for BU-4664")
        assert event.reply_to_message_id == "teams-card-bu"

    @pytest.mark.anyio
    async def test_deduplication(self):
        adapter = TeamsAdapter(_make_config(
            client_id="bot-id", client_secret="x", tenant_id="tenant",
        ))
        adapter._app = MagicMock()
        adapter._app.id = "bot-id"
        adapter.handle_message = AsyncMock()

        activity = self._make_activity(activity_id="msg-dup-001", from_id="user-id")
        ctx = self._make_ctx(activity)

        await adapter._on_message(ctx)
        await adapter._on_message(ctx)

        assert adapter.handle_message.await_count == 1


def _install_capture_card_response(monkeypatch):
    class CaptureCard:
        def __init__(self):
            self.version = None
            self.body = None
            self.actions = None

        def with_version(self, value):
            self.version = value
            return self

        def with_body(self, body):
            self.body = body
            return self

        def with_actions(self, actions):
            self.actions = actions
            return self

    monkeypatch.setattr(_teams_mod, "AdaptiveCard", CaptureCard)
    monkeypatch.setattr(_teams_mod, "TextBlock", lambda **kwargs: SimpleNamespace(**kwargs))
    monkeypatch.setattr(
        _teams_mod,
        "AdaptiveCardActionCardResponse",
        lambda value: SimpleNamespace(kind="card", value=value),
    )
    return CaptureCard


@pytest.mark.anyio
async def test_cargolo_asr_approve_button_routes_to_safe_handler(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    _install_capture_card_response(monkeypatch)
    calls = []

    def fake_process(**kwargs):
        calls.append(kwargs)
        return {"handled": True, "status": "applied", "order_id": "AN-11755", "response_text": "✅ umgesetzt"}

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_tms_approve",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"

    response = await adapter._on_card_action(ctx)

    assert response.status == 200
    assert calls
    assert calls[0]["data"]["hermes_action"] == "cargolo_asr_tms_approve"
    assert calls[0]["user_id"] == "aad-1"
    assert calls[0]["user_name"] == "Dominik"
    assert response.body.kind == "card"
    assert response.body.value.actions is None
    assert any("Ins TMS geschrieben" in block.text for block in response.body.value.body)
    assert any("Buttons deaktiviert" in block.text for block in response.body.value.body)


@pytest.mark.anyio
async def test_cargolo_asr_reject_button_replaces_card_without_actions(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    _install_capture_card_response(monkeypatch)
    calls = []

    def fake_process(**kwargs):
        calls.append(kwargs)
        return {
            "handled": True,
            "status": "rejected",
            "order_id": "AN-11755",
            "response_text": "❌ Abgelehnt für AN-11755: customs_reference = 26DE99999 wurde nicht ins TMS geschrieben.",
        }

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_tms_reject",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"

    response = await adapter._on_card_action(ctx)

    assert response.status == 200
    assert calls
    assert response.body.kind == "card"
    assert response.body.value.actions is None
    assert any("Buttons deaktiviert" in block.text for block in response.body.value.body)


@pytest.mark.anyio
async def test_cargolo_asr_case_check_button_keeps_review_card_active(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    adapter._app = MagicMock()
    adapter.send = AsyncMock()
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    calls = []

    monkeypatch.setattr(
        _teams_mod,
        "AdaptiveCardActionMessageResponse",
        lambda value: SimpleNamespace(kind="message", value=value),
    )
    monkeypatch.setattr(
        _teams_mod,
        "AdaptiveCardActionCardResponse",
        lambda value: SimpleNamespace(kind="card", value=value),
    )

    def fake_process(**kwargs):
        calls.append(kwargs)
        return {"handled": True, "status": "case_check_completed", "response_text": "🔎 Fall geprüft"}

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_case_check",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"
    ctx.activity.conversation.id = "chat-1"

    response = await adapter._on_card_action(ctx)
    for _ in range(20):
        if calls:
            break
        await asyncio.sleep(0.01)

    assert response.status == 200
    assert response.body.kind == "message"
    assert "Fallprüfung für AN-11755 läuft" in response.body.value


@pytest.mark.anyio
async def test_cargolo_asr_case_check_button_routes_to_safe_handler(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    adapter._app = MagicMock()
    adapter.send = AsyncMock()
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    calls = []

    def fake_process(**kwargs):
        calls.append(kwargs)
        return {"handled": True, "status": "case_check_requested", "response_text": "🔎 queued"}

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_case_check",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"
    ctx.activity.conversation.id = "chat-1"

    response = await adapter._on_card_action(ctx)
    for _ in range(20):
        if adapter.send.await_count >= 2 and calls:
            break
        await asyncio.sleep(0.01)

    assert response.status == 200
    assert adapter.send.await_count == 2
    first_send = adapter.send.await_args_list[0]
    assert first_send.args[0] == "chat-1"
    assert "Fallprüfung für AN-11755 läuft" in first_send.args[1]
    adapter.send.assert_any_await("chat-1", "🔎 queued")
    assert calls
    assert calls[0]["data"]["hermes_action"] == "cargolo_asr_case_check"
    assert calls[0]["user_id"] == "aad-1"
    assert calls[0]["user_name"] == "Dominik"


@pytest.mark.anyio
async def test_cargolo_asr_correct_button_routes_to_safe_handler(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    _install_capture_card_response(monkeypatch)
    calls = []

    def fake_process(**kwargs):
        calls.append(kwargs)
        return {"handled": True, "status": "correction_requested", "order_id": "AN-11755", "response_text": "✏️ correction"}

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_tms_correct",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"

    response = await adapter._on_card_action(ctx)

    assert response.status == 200
    assert calls
    assert calls[0]["data"]["hermes_action"] == "cargolo_asr_tms_correct"
    assert response.body.kind == "card"
    assert response.body.value.actions is None
    assert any("Korrektur angefordert" in block.text for block in response.body.value.body)
    assert any("Buttons deaktiviert" in block.text for block in response.body.value.body)


@pytest.mark.anyio
async def test_cargolo_asr_approval_blocked_replaces_card_without_actions(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    monkeypatch.setenv("TEAMS_ALLOWED_USERS", "aad-1")
    _install_capture_card_response(monkeypatch)

    def fake_process(**kwargs):
        return {
            "handled": True,
            "status": "approval_blocked",
            "order_id": "AN-11755",
            "response_text": "⚠️ Freigabe erkannt, aber Live-TMS-Writeback ist deaktiviert. Ich schreibe nichts ins TMS.",
        }

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_tms_approve",
        "order_id": "AN-11755",
        "action_id": "abc123",
        "target": "customs_reference",
        "value": "26DE99999",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"

    response = await adapter._on_card_action(ctx)

    assert response.status == 200
    assert response.body.kind == "card"
    assert response.body.value.actions is None
    assert any("TMS-Write blockiert" in block.text for block in response.body.value.body)
    assert any("Buttons deaktiviert" in block.text for block in response.body.value.body)


@pytest.mark.anyio
async def test_cargolo_asr_button_default_denies_when_allowed_users_missing(monkeypatch):
    adapter = TeamsAdapter(_make_config(client_id="id", client_secret="x", tenant_id="tenant"))
    monkeypatch.delenv("TEAMS_ALLOWED_USERS", raising=False)
    monkeypatch.delenv("TEAMS_ALLOW_ALL_USERS", raising=False)

    def fake_process(**kwargs):
        raise AssertionError("unauthorized button must not reach CARGOLO handler")

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.process_teams_tms_card_action", fake_process)

    ctx = MagicMock()
    ctx.activity.value.action.data = {
        "hermes_action": "cargolo_asr_tms_reject",
        "order_id": "AN-11755",
        "action_id": "abc123",
    }
    ctx.activity.from_.aad_object_id = "aad-1"
    ctx.activity.from_.id = "teams-user-id"
    ctx.activity.from_.name = "Dominik"

    response = await adapter._on_card_action(ctx)

    assert response.status == 200


@pytest.mark.anyio
async def test_cargolo_ops_pending_command_sends_interactive_review_cards(monkeypatch):
    def fake_handle_teams_message(**kwargs):
        return {"handled": False, "reason": "no_card_context"}

    def fake_route_teams_ops_message(**kwargs):
        return {
            "handled": True,
            "classification": "pending_tms_reviews",
            "response_text": "CARGOLO Teams Ops · Offene TMS-Freigaben",
            "teams_tms_review_cards": [{
                "action_id": "abc123",
                "order_id": "AN-11755",
                "target": "customs_reference",
                "value": "26DE99999",
                "operator": "Dominik",
            }],
        }

    monkeypatch.setattr("plugins.cargolo_ops.teams_reply_loop.handle_teams_message", fake_handle_teams_message, raising=False)
    monkeypatch.setattr("plugins.cargolo_ops.teams_ops_router.route_teams_ops_message", fake_route_teams_ops_message, raising=False)
    adapter = TeamsAdapter(_make_config(client_id="bot-id", client_secret="x", tenant_id="tenant"))
    mock_result = MagicMock()
    mock_result.id = "ops-pending-ack"
    mock_app = MagicMock()
    mock_app.id = "bot-id"
    mock_app.send = AsyncMock(return_value=mock_result)
    adapter._app = mock_app
    adapter.handle_message = AsyncMock()
    adapter.send_cargolo_asr_tms_review_card = AsyncMock(return_value=MagicMock(success=True))

    helper = TestTeamsMessageHandling()
    activity = helper._make_activity(text="<at>Hermes CARGOLO</at> offene Freigaben", activity_id="msg-pending")
    await adapter._on_message(helper._make_ctx(activity))

    adapter.handle_message.assert_not_awaited()
    mock_app.send.assert_awaited_once()
    adapter.send_cargolo_asr_tms_review_card.assert_awaited_once()
    assert adapter.send_cargolo_asr_tms_review_card.call_args[0][1]["action_id"] == "abc123"

# ── _standalone_send (out-of-process cron delivery) ──────────────────────


class _FakeAiohttpResponse:
    def __init__(self, status: int, payload, text_body: str = ""):
        self.status = status
        self._payload = payload
        self._text = text_body or (str(payload) if payload is not None else "")

    async def json(self):
        return self._payload

    async def text(self):
        return self._text

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None


class _FakeAiohttpSession:
    """Scripted aiohttp.ClientSession with a queue of responses so tests
    can assert calls in order."""

    def __init__(self, scripts):
        self._scripts = list(scripts)
        self.calls: list[tuple[str, dict]] = []

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return None

    def post(self, url, **kwargs):
        self.calls.append((url, kwargs))
        if not self._scripts:
            raise AssertionError(f"No scripted response for POST {url}")
        return self._scripts.pop(0)


def _install_fake_aiohttp(monkeypatch, session):
    """Replace ``aiohttp`` in ``sys.modules`` so ``import aiohttp as _aiohttp``
    inside ``_standalone_send`` picks up our fake."""
    fake_aiohttp = types.SimpleNamespace(
        ClientSession=lambda timeout=None: session,
        ClientTimeout=lambda total=None: None,
    )
    monkeypatch.setitem(sys.modules, "aiohttp", fake_aiohttp)


class TestTeamsStandaloneSend:

    @pytest.mark.asyncio
    async def test_standalone_send_acquires_token_and_posts_activity(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "client-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "tenant")
        monkeypatch.delenv("TEAMS_SERVICE_URL", raising=False)

        token_resp = _FakeAiohttpResponse(200, {"access_token": "the-token"})
        activity_resp = _FakeAiohttpResponse(200, {"id": "msg-99"})
        session = _FakeAiohttpSession([token_resp, activity_resp])
        _install_fake_aiohttp(monkeypatch, session)

        result = await _teams_mod._standalone_send(
            PlatformConfig(enabled=True, extra={}),
            "19:abc@thread.skype",
            "hello cron",
        )

        assert result == {"success": True, "message_id": "msg-99"}
        assert len(session.calls) == 2

        token_url, token_kwargs = session.calls[0]
        assert "login.microsoftonline.com/tenant/oauth2/v2.0/token" in token_url
        assert token_kwargs["data"]["client_id"] == "client-id"
        assert token_kwargs["data"]["client_secret"] == "secret"
        assert token_kwargs["data"]["scope"] == "https://api.botframework.com/.default"

        activity_url, activity_kwargs = session.calls[1]
        # Default service URL when TEAMS_SERVICE_URL is unset
        assert "smba.trafficmanager.net" in activity_url
        assert "/v3/conversations/19:abc@thread.skype/activities" in activity_url
        assert activity_kwargs["headers"]["Authorization"] == "Bearer the-token"
        assert activity_kwargs["json"]["text"] == "hello cron"
        assert activity_kwargs["json"]["type"] == "message"

    @pytest.mark.asyncio
    async def test_standalone_send_returns_error_when_unconfigured(self, monkeypatch):
        for var in ("TEAMS_CLIENT_ID", "TEAMS_CLIENT_SECRET", "TEAMS_TENANT_ID"):
            monkeypatch.delenv(var, raising=False)

        result = await _teams_mod._standalone_send(
            PlatformConfig(enabled=True, extra={}),
            "19:abc@thread.skype",
            "hi",
        )

        assert "error" in result
        assert "TEAMS_CLIENT_ID" in result["error"]

    @pytest.mark.asyncio
    async def test_standalone_send_propagates_token_failure(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "client-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "tenant")

        token_resp = _FakeAiohttpResponse(
            401,
            {"error": "unauthorized_client"},
            text_body='{"error":"unauthorized_client"}',
        )
        session = _FakeAiohttpSession([token_resp])
        _install_fake_aiohttp(monkeypatch, session)

        result = await _teams_mod._standalone_send(
            PlatformConfig(enabled=True, extra={}),
            "19:abc@thread.skype",
            "hi",
        )

        assert "error" in result
        assert "401" in result["error"]
        assert "token" in result["error"].lower()

    @pytest.mark.asyncio
    async def test_standalone_send_rejects_off_allowlist_service_url(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "client-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "tenant")
        # SSRF attempt: point us at an attacker-controlled host
        monkeypatch.setenv("TEAMS_SERVICE_URL", "https://attacker.example.com/teams/")

        # If the allowlist check fails to fire, the fake session will assert
        # because no scripts are queued; a passing test means we returned
        # before any HTTP call.
        session = _FakeAiohttpSession([])
        _install_fake_aiohttp(monkeypatch, session)

        result = await _teams_mod._standalone_send(
            PlatformConfig(enabled=True, extra={}),
            "19:abc@thread.skype",
            "hi",
        )

        assert "error" in result
        assert "allowlist" in result["error"].lower()
        assert len(session.calls) == 0, "must not call any HTTP endpoint with a tampered service URL"

    @pytest.mark.asyncio
    async def test_standalone_send_rejects_chat_id_with_path_traversal(self, monkeypatch):
        monkeypatch.setenv("TEAMS_CLIENT_ID", "client-id")
        monkeypatch.setenv("TEAMS_CLIENT_SECRET", "secret")
        monkeypatch.setenv("TEAMS_TENANT_ID", "tenant")
        monkeypatch.delenv("TEAMS_SERVICE_URL", raising=False)

        session = _FakeAiohttpSession([])
        _install_fake_aiohttp(monkeypatch, session)

        # Attempt to break out of /v3/conversations/<id>/activities via a `/`
        result = await _teams_mod._standalone_send(
            PlatformConfig(enabled=True, extra={}),
            "19:abc/activities/19:other@thread.skype",
            "hi",
        )

        assert "error" in result
        assert "Bot Framework conversation ID" in result["error"]
        assert len(session.calls) == 0

