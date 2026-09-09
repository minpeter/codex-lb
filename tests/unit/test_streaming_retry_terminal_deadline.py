from __future__ import annotations

from unittest.mock import AsyncMock

import pytest

from app.core.clients.proxy import ProxyResponseError
from app.core.errors import openai_error
from app.core.openai.requests import ResponsesRequest
from app.modules.api_keys.service import ApiKeysService, ApiKeyUsageReservationData
from app.modules.proxy import service as facade
from app.modules.proxy._service.streaming import retry
from tests.unit.test_streaming_retry_virtual_time import (
    _make_account,
    _make_api_key_data,
    _make_proxy_settings,
    _RequestLogsRecorder,
    _SettingsCache,
    _virtual_service,
)


@pytest.mark.asyncio
@pytest.mark.parametrize("status", [500, 502])
async def test_retry_preserves_deadline_and_single_terminal_health(monkeypatch, status):
    settings = _make_proxy_settings()
    settings.deterministic_failover_enabled = False
    settings.http_responses_stream_request_budget_seconds = 1.0 if status == 500 else 75.0
    service, clock, scheduler = _virtual_service(_RequestLogsRecorder())
    account = _make_account("retry-contract")
    attempts = []
    effects = []
    monkeypatch.setattr(facade, "get_settings", lambda: settings)
    monkeypatch.setattr(facade, "get_settings_cache", lambda: _SettingsCache(settings))
    monkeypatch.setattr(retry, "backoff_seconds", lambda _: 2.5)
    monkeypatch.setattr(
        service, "_select_account_with_budget_compatible",
        AsyncMock(return_value=facade.AccountSelection(account=account, error_message=None)),
    )
    monkeypatch.setattr(service, "_ensure_fresh_with_budget", AsyncMock(return_value=account))
    monkeypatch.setattr(service, "_write_request_log", AsyncMock())

    async def health(*args, **kwargs):
        effects.append("health")

    async def release(*args, **kwargs):
        effects.append("release")
        return True

    async def upstream(*args, **kwargs):
        attempts.append(clock.monotonic())
        if len(attempts) == 1:
            raise ProxyResponseError(status, openai_error("server_error", "fixture"))
        yield 'data: {"type":"response.completed","response":{"id":"unexpected"}}\n\n'

    monkeypatch.setattr(service, "_handle_stream_error", health)
    monkeypatch.setattr(ApiKeysService, "release_usage_reservation", release)
    monkeypatch.setattr(service, "_stream_once", upstream)
    key = _make_api_key_data("retry-key") if status == 502 else None
    reservation = (
        ApiKeyUsageReservationData(reservation_id="retry-r", key_id=key.id, model="gpt-5.1") if key else None
    )

    async def consume():
        return [
            event async for event in service._stream_with_retry(
                ResponsesRequest(model="gpt-5.1", instructions="hi", input=[], stream=True), {},
                codex_session_affinity=False, propagate_http_errors=False, openai_cache_affinity=False,
                api_key=key, api_key_reservation=reservation, suppress_text_done_events=False,
                request_transport="http", upstream_stream_transport_override="http",
            )
        ]

    try:
        task = scheduler.create_task(consume())
        await scheduler.drain()
        if status == 500:
            await scheduler.advance(1.0)
            assert task.done(), "backoff must end at the request deadline"
        events = await task
        assert attempts == [1000.0]
        assert "response.failed" in events[-1]
        if status == 502:
            assert effects == ["release", "health"]
        else:
            assert "upstream_request_timeout" in events[-1]
    finally:
        await scheduler.cancel_owned_tasks()
