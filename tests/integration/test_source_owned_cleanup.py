from __future__ import annotations

import asyncio
from contextlib import AsyncExitStack
from datetime import datetime, timezone
from types import SimpleNamespace
from unittest.mock import AsyncMock

import pytest
from fastapi import FastAPI
from httpx import ASGITransport, AsyncClient

from app.core.utils.sse import CODEX_KEEPALIVE_FRAME
from app.db.models import ModelSource
from app.modules.api_keys.service import ApiKeyData, ApiKeyUsageReservationData
from app.modules.model_sources import forwarding
from app.modules.proxy import api

pytestmark = pytest.mark.integration


def _source() -> ModelSource:
    return ModelSource(
        id="cleanup-source",
        name="cleanup-source",
        kind="openai_compatible",
        base_url="http://source.invalid/v1",
        is_enabled=True,
        models=[],
    )


@pytest.mark.asyncio
async def test_initial_heartbeat_close_releases_eager_source_lease(monkeypatch: pytest.MonkeyPatch) -> None:
    stack = AsyncExitStack()
    close = AsyncMock()
    stack.push_async_callback(close)
    # No content attribute: the heartbeat must precede any upstream body read.
    monkeypatch.setattr(forwarding, "_open_source_stream", AsyncMock(return_value=(stack, SimpleNamespace(status=200))))
    stream = await forwarding.stream_responses(_source(), {"model": "cleanup-model", "stream": True})
    wrapped = api._wrap_source_responses_public_stream(
        stream.body, enforce_openai_sdk_contract=False, native_codex_heartbeat=False
    )
    try:
        assert await anext(wrapped) == CODEX_KEEPALIVE_FRAME
        await api._aclose_stream(wrapped)
        print(f"heartbeat_received=True lease_closed_after_wrapper_aclose={close.await_count == 1}")
        close.assert_awaited_once_with()
    finally:
        await api._aclose_stream(wrapped)
        await stack.aclose()  # Also clean up the deliberately stranded lease on RED.


@pytest.mark.asyncio
@pytest.mark.parametrize("streaming", [False, True])
async def test_responses_asgi_setup_cancellation_releases_reservation(
    monkeypatch: pytest.MonkeyPatch, streaming: bool
) -> None:
    source = _source()
    key = ApiKeyData(
        id="cleanup-key",
        name="cleanup-key",
        key_prefix="test",
        allowed_models=None,
        enforced_model=None,
        enforced_reasoning_effort=None,
        enforced_service_tier=None,
        expires_at=None,
        is_active=True,
        created_at=datetime(2026, 1, 1, tzinfo=timezone.utc),
        last_used_at=None,
    )
    reservation = ApiKeyUsageReservationData(reservation_id="cleanup-reservation", key_id=key.id, model="cleanup-model")
    started = asyncio.Event()
    outbound_gate = asyncio.Event()
    release_started = asyncio.Event()
    release_gate = asyncio.Event()
    release_finished = asyncio.Event()

    async def forward(*args: object, **kwargs: object) -> None:
        started.set()
        await outbound_gate.wait()

    async def release(owned: ApiKeyUsageReservationData) -> None:
        assert owned is reservation
        release_started.set()
        await release_gate.wait()
        release_finished.set()

    reserve = AsyncMock(return_value=reservation)
    release_mock = AsyncMock(side_effect=release)
    monkeypatch.setattr(api, "_enforce_request_limits", reserve)
    monkeypatch.setattr(api, "_release_reservation", release_mock)
    monkeypatch.setattr(api, "stream_source_responses" if streaming else "forward_source_responses", forward)
    monkeypatch.setattr(
        api,
        "_select_responses_model_source_with_continuity",
        AsyncMock(return_value=((source, "cleanup-model"), False)),
    )
    monkeypatch.setattr(api, "_rate_limit_headers_for_request", AsyncMock(return_value={}))
    monkeypatch.setattr(api, "_prohibit_fast_mode_enabled", AsyncMock(return_value=False))
    app = FastAPI()
    app.include_router(api.v1_router)
    app.dependency_overrides[api.validate_proxy_api_key] = lambda: key
    app.dependency_overrides[api.get_proxy_context] = lambda: SimpleNamespace()

    async with AsyncClient(transport=ASGITransport(app=app), base_url="http://test") as client:
        task = asyncio.create_task(
            client.post("/v1/responses", json={"model": "cleanup-model", "input": "hi", "stream": streaming})
        )
        release_waiter = asyncio.create_task(release_started.wait())
        try:
            await asyncio.wait_for(started.wait(), 2)
            task.cancel()
            done, _ = await asyncio.wait({task, release_waiter}, timeout=2, return_when=asyncio.FIRST_COMPLETED)
            assert done
            print(
                f"stream={streaming} reservation_acquired={reserve.await_count} "
                f"release_calls={release_mock.await_count}"
            )
            assert release_started.is_set()
            assert not release_finished.is_set()
            # A second cancellation hits an outstanding release, not a sleep or race.
            task.cancel()
            release_gate.set()
            with pytest.raises(asyncio.CancelledError):
                await asyncio.wait_for(task, 2)
            assert release_finished.is_set()
            reserve.assert_awaited_once()
            assert reserve.call_args.args == (key,)
            release_mock.assert_awaited_once_with(reservation)
        finally:
            outbound_gate.set()
            release_gate.set()
            task.cancel()
            release_waiter.cancel()
            await asyncio.wait_for(asyncio.gather(task, release_waiter, return_exceptions=True), 2)
