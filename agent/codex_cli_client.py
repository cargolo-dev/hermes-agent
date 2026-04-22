"""OpenAI-compatible shim that forwards Hermes requests to `codex exec`.

This adapter lets Hermes use the local Codex CLI as a subprocess-backed model
for delegated agents. Each completion request spawns a short-lived `codex exec`
process, feeds the full conversation transcript as one prompt, and converts the
final text response back into the minimal shape Hermes expects from an OpenAI
chat-completions client.
"""

from __future__ import annotations

import os
import shlex
import subprocess
import tempfile
from pathlib import Path
from types import SimpleNamespace
from typing import Any

from agent.copilot_acp_client import _extract_tool_calls_from_text, _format_messages_as_prompt

_DEFAULT_TIMEOUT_SECONDS = 900.0


def _resolve_command() -> str:
    return os.getenv("HERMES_CODEX_CLI_COMMAND", "").strip() or "codex"


def _resolve_args() -> list[str]:
    raw = os.getenv("HERMES_CODEX_CLI_ARGS", "").strip()
    if not raw:
        return []
    return shlex.split(raw)


class _CodexCLIChatCompletions:
    def __init__(self, client: "CodexCLIClient"):
        self._client = client

    def create(self, **kwargs: Any) -> Any:
        return self._client._create_chat_completion(**kwargs)


class _CodexCLIChatNamespace:
    def __init__(self, client: "CodexCLIClient"):
        self.completions = _CodexCLIChatCompletions(client)


class CodexCLIClient:
    """Minimal OpenAI-client-compatible facade for Codex CLI."""

    def __init__(
        self,
        *,
        api_key: str | None = None,
        base_url: str | None = None,
        default_headers: dict[str, str] | None = None,
        acp_command: str | None = None,
        acp_args: list[str] | None = None,
        acp_cwd: str | None = None,
        command: str | None = None,
        args: list[str] | None = None,
        **_: Any,
    ):
        self.api_key = api_key or "codex-cli"
        self.base_url = base_url or "cli://codex"
        self._default_headers = dict(default_headers or {})
        self._command = acp_command or command or _resolve_command()
        self._args = list(acp_args or args or _resolve_args())
        self._cwd = str(Path(acp_cwd or os.getcwd()).resolve())
        self.chat = _CodexCLIChatNamespace(self)
        self.is_closed = False

    def close(self) -> None:
        self.is_closed = True

    def _create_chat_completion(
        self,
        *,
        model: str | None = None,
        messages: list[dict[str, Any]] | None = None,
        timeout: float | None = None,
        tools: list[dict[str, Any]] | None = None,
        tool_choice: Any = None,
        **_: Any,
    ) -> Any:
        prompt_text = _format_messages_as_prompt(
            messages or [],
            model=model,
            tools=tools,
            tool_choice=tool_choice,
        )
        timeout_value = timeout or _DEFAULT_TIMEOUT_SECONDS
        try:
            timeout_seconds = float(timeout_value)
        except Exception:
            timeout_seconds = float(
                getattr(timeout_value, "read", None)
                or getattr(timeout_value, "timeout", None)
                or _DEFAULT_TIMEOUT_SECONDS
            )
        response_text = self._run_prompt(
            prompt_text,
            model=model,
            timeout_seconds=timeout_seconds,
        )

        tool_calls, cleaned_text = _extract_tool_calls_from_text(response_text)
        usage = SimpleNamespace(
            prompt_tokens=0,
            completion_tokens=0,
            total_tokens=0,
            prompt_tokens_details=SimpleNamespace(cached_tokens=0),
        )
        assistant_message = SimpleNamespace(
            content=cleaned_text,
            tool_calls=tool_calls,
            reasoning=None,
            reasoning_content=None,
            reasoning_details=None,
        )
        finish_reason = "tool_calls" if tool_calls else "stop"
        choice = SimpleNamespace(message=assistant_message, finish_reason=finish_reason)
        return SimpleNamespace(
            choices=[choice],
            usage=usage,
            model=model or "codex-cli",
        )

    def _run_prompt(self, prompt_text: str, *, model: str | None, timeout_seconds: float) -> str:
        with tempfile.NamedTemporaryFile(prefix="hermes-codex-last-message-", suffix=".txt", delete=False) as handle:
            output_path = Path(handle.name)

        cmd = [self._command, *self._args, "exec", "--skip-git-repo-check", "--sandbox", "read-only", "--color", "never", "--output-last-message", str(output_path), "--ephemeral", "-"]
        if self._cwd:
            cmd.extend(["--cd", self._cwd])
        if model:
            cmd.extend(["--model", model])

        env = os.environ.copy()
        env.setdefault("CODEX_DISABLE_TELEMETRY", "1")
        if self.api_key and self.api_key not in {"codex-cli", "***", "copilot-acp"}:
            env.setdefault("OPENAI_API_KEY", self.api_key)

        try:
            proc = subprocess.run(
                cmd,
                input=prompt_text,
                stdout=subprocess.PIPE,
                stderr=subprocess.PIPE,
                text=True,
                cwd=self._cwd,
                timeout=timeout_seconds,
                env=env,
            )
        except FileNotFoundError as exc:
            raise RuntimeError(
                f"Could not start Codex CLI command '{self._command}'. Install Codex CLI or set HERMES_CODEX_CLI_COMMAND."
            ) from exc
        except subprocess.TimeoutExpired as exc:
            raise TimeoutError("Timed out waiting for Codex CLI response.") from exc

        try:
            response_text = output_path.read_text(encoding="utf-8").strip()
        except Exception:
            response_text = ""
        finally:
            try:
                output_path.unlink(missing_ok=True)
            except Exception:
                pass

        combined_output = "\n".join(part for part in [proc.stdout.strip(), proc.stderr.strip()] if part).strip()
        if proc.returncode != 0:
            raise RuntimeError(combined_output or "Codex CLI exited with a non-zero status.")
        if not response_text:
            raise RuntimeError(combined_output or "Codex CLI did not produce a final response.")
        return response_text
