"""Exercise expiry through the real WebSocket sender, reader, gate and correlation.

Only request policy, account selection and persistence are local doubles. The
Starlette downstream speaks ASGI over queues; upstream queues retain late frames
even while close is blocked, so retirement cannot pass by discarding input.
"""

from __future__ import annotations

import asyncio
import json
from contextlib import asynccontextmanager
from types import SimpleNamespace
from typing import Any, AsyncIterator, cast
from unittest.mock import AsyncMock

import pytest
from fastapi import WebSocket
from starlette.types import Message

from app.core.clients.proxy_websocket import UpstreamWebSocket, UpstreamWebSocketMessage
from app.db.models import Account
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service.websocket import mixin as websocket_mixin
from app.modules.proxy.work_admission import WorkAdmissionController
from tests.simulation.virtual_time import VirtualClock, VirtualScheduler

pytestmark = [pytest.mark.integration, pytest.mark.asyncio]


class _Upstream:
    def __init__(self) -> None:
        self.frames: asyncio.Queue[UpstreamWebSocketMessage] = asyncio.Queue()
        self.sent: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.sent_inputs: list[str] = []
        self.receiving = asyncio.Event()
        self.close_started = asyncio.Event()
        self.finish_close = asyncio.Event()
        self.closed = False

    async def send_text(self, text: str) -> None:
        assert not self.closed
        payload = json.loads(text)
        self.sent_inputs.append(payload["input"])
        self.sent.put_nowait(payload)

    async def receive(self) -> UpstreamWebSocketMessage:
        self.receiving.set()
        return await self.frames.get()

    async def close(self) -> None:
        self.close_started.set()
        await self.finish_close.wait()
        self.closed = True

    def emit(self, event_type: str, response_id: str) -> None:
        self.frames.put_nowait(
            UpstreamWebSocketMessage(
                kind="text",
                text=json.dumps({"type": event_type, "response": {"id": response_id, "output": []}}),
            )
        )


class _Session:
    def __init__(self, monkeypatch: pytest.MonkeyPatch) -> None:
        self.clock = VirtualClock()
        self.scheduler = VirtualScheduler(self.clock)
        self.states: dict[str, proxy_service._WebSocketRequestState] = {}
        self.incoming: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.outgoing: asyncio.Queue[dict[str, Any]] = asyncio.Queue()
        self.events: list[dict[str, Any]] = []
        self.old = _Upstream()
        self.fresh = _Upstream()
        self.fresh.finish_close.set()
        self.connections: list[_Upstream] = []
        self.b_at_gate = asyncio.Event()
        self.b_has_gate = asyncio.Event()
        self.release_b_admission = asyncio.Event()
        self.b_at_account = asyncio.Event()
        self.release_b_account = asyncio.Event()
        self.a_failure_delivery = asyncio.Event()
        self.release_a_failure = asyncio.Event()
        self.release_a_failure.set()
        self.logs: list[dict[str, Any]] = []

        @asynccontextmanager
        async def repo_factory() -> AsyncIterator[SimpleNamespace]:
            yield SimpleNamespace(api_keys=object())

        self.service = proxy_service.ProxyService(
            cast(proxy_service.ProxyRepoFactory, repo_factory), clock=self.clock, scheduler=self.scheduler
        )
        settings = SimpleNamespace(
            prefer_earlier_reset_accounts=False,
            sticky_threads_enabled=False,
            openai_cache_affinity_max_age_seconds=0,
            prohibit_fast_mode=False,
            proxy_downstream_websocket_idle_timeout_seconds=30.0,
            proxy_request_budget_seconds=5.0,
            stream_idle_timeout_seconds=30.0,
            sse_keepalive_interval_seconds=0.0,
        )
        admission = WorkAdmissionController(
            token_refresh_limit=1,
            websocket_connect_limit=1,
            response_create_limit=2,
            compact_response_create_limit=1,
            scheduler=self.scheduler,
        )
        real_work_acquire = admission.acquire_response_create
        real_admit = self.service._acquire_request_state_response_create_admission

        async def work_acquire(*, compact: bool = False) -> Any:
            lease = await real_work_acquire(compact=compact)
            if "B" in self.states and self.states["B"].response_create_gate_acquired:
                self.b_has_gate.set()
                await self.release_b_admission.wait()
            return lease

        async def admit(state: proxy_service._WebSocketRequestState, **kwargs: Any) -> None:
            if state.request_id == "B":
                self.b_at_gate.set()
            await real_admit(state, **kwargs)

        async def prepare(payload: dict[str, Any], **_kwargs: Any) -> proxy_service._PreparedWebSocketRequest:
            name = payload["input"]
            text = json.dumps(payload)
            state = proxy_service._WebSocketRequestState(
                request_id=name,
                model="gpt-5.5",
                service_tier=None,
                reasoning_effort=None,
                api_key_reservation=None,
                started_at=self.clock.monotonic(),
                request_text=text,
            )
            self.states[name] = state
            return proxy_service._PreparedWebSocketRequest(
                text_data=text, request_state=state, affinity_policy=proxy_service._AffinityPolicy()
            )

        async def connect(*_args: Any, **_kwargs: Any) -> tuple[Account, UpstreamWebSocket]:
            upstream = self.old if not self.connections else self.fresh
            self.connections.append(upstream)
            return cast(Account, SimpleNamespace(id="account-expiry", codex_installation_id=None)), cast(
                UpstreamWebSocket, upstream
            )

        async def account_lease(*, request_id: str, **_kwargs: Any) -> None:
            if request_id == "B":
                self.b_at_account.set()
                await self.release_b_account.wait()

        async def log(**kwargs: Any) -> None:
            self.logs.append(kwargs)

        async def send(message: Message) -> None:
            if message["type"] == "websocket.send":
                event = json.loads(message["text"])
                self.events.append(event)
                self.outgoing.put_nowait(event)
                if event["type"] == "response.failed" and event["response"]["id"] == "A":
                    self.a_failure_delivery.set()
                    await self.release_a_failure.wait()

        self.websocket = WebSocket({"type": "websocket", "path": "/responses", "headers": []}, self.incoming.get, send)
        monkeypatch.setattr(proxy_service, "get_settings", lambda: settings)
        monkeypatch.setattr(
            proxy_service, "get_settings_cache", lambda: SimpleNamespace(get=AsyncMock(return_value=settings))
        )
        monkeypatch.setattr(proxy_service, "_routing_strategy", lambda _settings: "usage_weighted")
        monkeypatch.setattr(websocket_mixin, "effective_account_concurrency_caps", lambda _settings: {})
        monkeypatch.setattr(websocket_mixin, "responses_model_is_source_owned", AsyncMock(return_value=False))
        monkeypatch.setattr(self.service, "_websocket_continuity_state_for_request", lambda *_args, **_kwargs: None)
        monkeypatch.setattr(self.service, "_prepare_websocket_response_create_request", prepare)
        monkeypatch.setattr(self.service, "_connect_proxy_websocket", connect)
        monkeypatch.setattr(self.service, "_get_work_admission", lambda: admission)
        monkeypatch.setattr(admission, "acquire_response_create", work_acquire)
        monkeypatch.setattr(self.service, "_acquire_request_state_response_create_admission", admit)
        monkeypatch.setattr(self.service, "_acquire_account_response_create_lease_or_overload", account_lease)
        monkeypatch.setattr(self.service, "_write_request_log", log)
        monkeypatch.setattr(self.service._load_balancer, "record_success", AsyncMock())
        self.task: asyncio.Task[None] | None = None

    async def start(self) -> None:
        self.incoming.put_nowait({"type": "websocket.connect"})
        await self.websocket.accept()
        self.task = self.scheduler.create_task(
            self.service.proxy_responses_websocket(
                self.websocket, {}, codex_session_affinity=False, openai_cache_affinity=False, api_key=None
            )
        )
        self.submit("A")
        assert (await asyncio.wait_for(self.old.sent.get(), 2))["input"] == "A"
        await asyncio.wait_for(self.old.receiving.wait(), 2)

    def submit(self, name: str) -> None:
        self.incoming.put_nowait(
            {
                "type": "websocket.receive",
                "text": json.dumps({"type": "response.create", "model": "gpt-5.5", "input": name}),
            }
        )

    async def stop(self) -> None:
        self.old.finish_close.set()
        self.release_b_admission.set()
        self.release_b_account.set()
        self.release_a_failure.set()
        if self.task is not None:
            self.incoming.put_nowait({"type": "websocket.disconnect", "code": 1000})
            await asyncio.wait_for(self.task, 2)
        await self.scheduler.cancel_owned_tasks()
        assert not self.service._background_cleanup_tasks


@pytest.mark.parametrize("registered_before_retirement", [False, True], ids=["gate-waiter", "account-waiter"])
async def test_uncorrelated_expiry_fences_actual_sender_and_late_frames(
    monkeypatch: pytest.MonkeyPatch, registered_before_retirement: bool
) -> None:
    session = _Session(monkeypatch)
    try:
        await session.start()
        await session.scheduler.advance(1)
        session.release_a_failure.clear()
        session.submit("B")
        await asyncio.wait_for(session.b_at_gate.wait(), 2)
        assert not session.b_has_gate.is_set()
        assert session.old.sent_inputs == ["A"]

        await session.scheduler.advance(4)
        await asyncio.wait_for(session.a_failure_delivery.wait(), 2)
        await asyncio.wait_for(session.b_has_gate.wait(), 2)
        if registered_before_retirement:
            session.release_b_admission.set()
            await asyncio.wait_for(session.b_at_account.wait(), 2)
        session.release_a_failure.set()
        await asyncio.wait_for(session.old.close_started.wait(), 2)

        # The reader is inside close, but the socket can still yield late A
        # frames. Releasing B's remaining admission waits must not send to it.
        session.old.emit("response.created", "resp_A")
        session.old.emit("response.completed", "resp_A")
        session.release_b_admission.set()
        session.release_b_account.set()
        await asyncio.wait_for(session.b_at_account.wait(), 2)
        await session.scheduler.drain()
        assert session.old.sent_inputs == ["A"]
        assert session.connections == [session.old]
        assert session.states["B"].response_id is None
        assert session.states["B"].response_create_sent_at is None
        assert session.old.frames.qsize() == 2

        session.old.finish_close.set()
        if registered_before_retirement:
            # Already queued B is failed closed, not resurrected by the sender.
            await session.scheduler.drain()
            assert [(event["type"], event["response"]["id"]) for event in session.events] == [
                ("response.failed", "A"),
                ("response.failed", "B"),
            ]
        else:
            assert (await asyncio.wait_for(session.fresh.sent.get(), 2))["input"] == "B"
            session.fresh.emit("response.created", "resp_B")
            session.fresh.emit("response.completed", "resp_B")
            assert (await asyncio.wait_for(session.outgoing.get(), 2))["type"] == "response.failed"
            assert (await asyncio.wait_for(session.outgoing.get(), 2))["response"]["id"] == "resp_B"
            assert (await asyncio.wait_for(session.outgoing.get(), 2))["type"] == "response.completed"
            assert session.states["B"].response_id == "resp_B"
        assert session.old.sent_inputs == ["A"]
        assert not any(log["request_id"] == "resp_A" for log in session.logs)
        assert session.logs[0]["error_code"] == "upstream_request_timeout"
    finally:
        await session.stop()


async def test_correlated_expiry_preserves_socket_and_younger_completion(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session(monkeypatch)
    session.release_b_admission.set()
    session.release_b_account.set()
    try:
        await session.start()
        session.old.emit("response.created", "resp_A")
        assert (await asyncio.wait_for(session.outgoing.get(), 2))["type"] == "response.created"
        await session.scheduler.advance(1)
        session.submit("B")
        assert (await asyncio.wait_for(session.old.sent.get(), 2))["input"] == "B"
        session.old.emit("response.created", "resp_B")
        assert (await asyncio.wait_for(session.outgoing.get(), 2))["response"]["id"] == "resp_B"
        await session.scheduler.advance(4)
        failure = await asyncio.wait_for(session.outgoing.get(), 2)
        assert (failure["type"], failure["response"]["id"]) == ("response.failed", "resp_A")
        assert not session.old.close_started.is_set()

        session.old.emit("response.completed", "resp_A")
        assert (await asyncio.wait_for(session.outgoing.get(), 2))["response"]["id"] == "resp_A"
        assert not any(log["request_id"] == "resp_B" for log in session.logs)
        session.old.emit("response.completed", "resp_B")
        assert (await asyncio.wait_for(session.outgoing.get(), 2))["response"]["id"] == "resp_B"
        assert [(log["request_id"], log["status"]) for log in session.logs] == [
            ("resp_A", "error"),
            ("resp_B", "success"),
        ]
        assert session.connections == [session.old]
        assert session.states["B"].response_id == "resp_B"
    finally:
        await session.stop()


async def test_late_created_cannot_complete_younger_request_after_expiry(monkeypatch: pytest.MonkeyPatch) -> None:
    session = _Session(monkeypatch)
    session.release_b_admission.set()
    session.release_b_account.set()
    try:
        await session.start()
        await session.scheduler.advance(1)
        session.submit("B")
        await asyncio.wait_for(session.b_at_gate.wait(), 2)
        await session.scheduler.advance(4)
        await asyncio.wait_for(session.b_at_account.wait(), 2)
        await session.scheduler.drain()
        session.old.emit("response.created", "resp_A")
        session.old.emit("response.completed", "resp_A")
        await session.scheduler.drain()

        # Baseline really sends B and binds/completes it as resp_A. Check the
        # ownership result before checking retirement, not merely a flag.
        assert session.states["B"].response_id is None, session.logs
        assert session.old.sent_inputs == ["A"]
        assert not any(log["status"] == "success" for log in session.logs)
        assert session.old.close_started.is_set()
        assert session.old.frames.qsize() == 2
    finally:
        await session.stop()
