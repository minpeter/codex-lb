"""Streaming retry/stream-once timing sites are owned by the injected scheduler.

``streaming/retry.py`` and ``streaming/mixin.py`` spawn settlement/close
owners, back off between transient retries, chunk capacity-recovery waits and
stamp latencies. Under ``VirtualScheduler`` each of those must park on a
virtual timer or be registered as an owned task; under the real defaults they
are the same ``asyncio``/``time`` calls as before.
"""

from __future__ import annotations

import asyncio
import json
from types import SimpleNamespace
from typing import Any, AsyncIterator, Coroutine, cast
from unittest.mock import AsyncMock

import pytest

from app.core.balancer.types import UpstreamError
from app.core.clients.proxy import ProxyResponseError
from app.core.crypto import TokenEncryptor
from app.core.errors import openai_error
from app.core.openai.requests import ResponsesRequest
from app.core.utils.time import utcnow
from app.db.models import Account, AccountStatus
from app.modules.accounts.repository import AccountsRepository
from app.modules.api_keys.repository import ApiKeysRepository
from app.modules.api_keys.service import ApiKeyData, ApiKeyUsageReservationData
from app.modules.proxy import service as proxy_service
from app.modules.proxy._service.streaming import retry as streaming_retry_module
from app.modules.proxy._service.support import _TransientStreamError
from app.modules.proxy.capability_lineage_repository import CapabilityLineageRepository
from app.modules.proxy.load_balancer import AccountLease, AccountSelection
from app.modules.proxy.repo_bundle import ProxyRepositories
from app.modules.proxy.sticky_repository import StickySessionsRepository
from app.modules.request_logs.repository import RequestLogsRepository
from app.modules.usage.repository import AdditionalUsageRepository, UsageRepository
from tests.simulation.virtual_time import VirtualClock, VirtualScheduler

pytestmark = pytest.mark.unit


class _RecordingScheduler(VirtualScheduler):
    """Virtual scheduler that remembers which coroutines it was asked to own."""

    def __init__(self, clock: VirtualClock) -> None:
        super().__init__(clock)
        self.spawned: list[str] = []

    def create_task(self, coroutine: Coroutine[Any, Any, Any], *, name: str | None = None) -> asyncio.Task[Any]:
        self.spawned.append(name or getattr(coroutine, "__qualname__", repr(coroutine)))
        return super().create_task(coroutine, name=name)


class _RequestLogsRecorder:
    def __init__(self) -> None:
        self.calls: list[dict[str, object]] = []

    async def add_log(self, **kwargs: object) -> None:
        self.calls.append(dict(kwargs))


class _RepoContext:
    def __init__(self, request_logs: _RequestLogsRecorder) -> None:
        capability_lineage = AsyncMock(spec=CapabilityLineageRepository)
        capability_lineage.is_required.return_value = False
        capability_lineage.require.return_value = ("test-marker",)
        accounts = AsyncMock()
        accounts.get_by_id_fresh.return_value = None
        self._repos = ProxyRepositories(
            accounts=cast(AccountsRepository, accounts),
            usage=cast(UsageRepository, AsyncMock()),
            request_logs=cast(RequestLogsRepository, request_logs),
            sticky_sessions=cast(StickySessionsRepository, AsyncMock()),
            api_keys=cast(ApiKeysRepository, AsyncMock()),
            additional_usage=cast(AdditionalUsageRepository, AsyncMock()),
            capability_lineage=capability_lineage,
        )

    async def __aenter__(self) -> ProxyRepositories:
        return self._repos

    async def __aexit__(self, exc_type: object, exc: object, tb: object) -> bool:
        return False


def _repo_factory(request_logs: _RequestLogsRecorder) -> proxy_service.ProxyRepoFactory:
    def factory() -> _RepoContext:
        return _RepoContext(request_logs)

    return factory


class _SettingsCache:
    def __init__(self, settings: object) -> None:
        self._settings = settings

    async def get(self) -> object:
        return self._settings


def _make_proxy_settings() -> SimpleNamespace:
    return SimpleNamespace(
        prefer_earlier_reset_accounts=False,
        prefer_earlier_reset_window="secondary",
        sticky_threads_enabled=False,
        sticky_reallocation_budget_threshold_pct=95.0,
        upstream_stream_transport="default",
        openai_cache_affinity_max_age_seconds=300,
        openai_prompt_cache_key_derivation_enabled=True,
        routing_strategy="usage_weighted",
        proxy_request_budget_seconds=75.0,
        compact_request_budget_seconds=75.0,
        transcription_request_budget_seconds=120.0,
        upstream_compact_timeout_seconds=None,
        http_responses_session_bridge_gateway_safe_mode=False,
        trace_channels=frozenset(),
        proxy_token_refresh_limit=32,
        proxy_upstream_websocket_connect_limit=64,
        proxy_account_response_create_limit=4,
        proxy_account_stream_limit=8,
        proxy_account_stream_recovery_reserve=1,
        proxy_api_key_fair_share_congestion_threshold_pct=0,
        proxy_response_create_limit=64,
        proxy_compact_response_create_limit=16,
        proxy_admission_wait_timeout_seconds=10.0,
        max_sse_event_bytes=16 * 1024 * 1024,
        http_responses_session_bridge_instance_id="test-instance",
        http_responses_session_bridge_instance_ring=[],
        http_responses_session_bridge_anchor_poison_failure_threshold=7,
        http_downstream_transport_policy="smart",
    )


def _make_account(account_id: str) -> Account:
    encryptor = TokenEncryptor()
    now = utcnow()
    return Account(
        id=account_id,
        chatgpt_account_id=account_id,
        email=f"{account_id}@example.com",
        plan_type="plus",
        access_token_encrypted=encryptor.encrypt("access-token"),
        refresh_token_encrypted=encryptor.encrypt("refresh-token"),
        id_token_encrypted=encryptor.encrypt("id-token"),
        last_refresh=now,
        status=AccountStatus.ACTIVE,
        deactivation_reason=None,
    )


def _make_api_key_data(key_id: str) -> ApiKeyData:
    return ApiKeyData(
        id=key_id,
        name=key_id,
        key_prefix=f"sk-{key_id[:8]}",
        allowed_models=None,
        enforced_model=None,
        enforced_reasoning_effort=None,
        enforced_service_tier=None,
        expires_at=None,
        is_active=True,
        created_at=utcnow(),
        last_used_at=None,
    )


def _virtual_service(
    request_logs: _RequestLogsRecorder,
) -> tuple[proxy_service.ProxyService, VirtualClock, _RecordingScheduler]:
    clock = VirtualClock(monotonic_value=1_000.0)
    scheduler = _RecordingScheduler(clock)
    service = proxy_service.ProxyService(_repo_factory(request_logs), clock=clock, scheduler=scheduler)
    return service, clock, scheduler


def _payload_retry_after_seconds(event: str) -> int:
    return int(json.loads(event.split("data: ", 1)[1])["retry_after_seconds"])


@pytest.mark.asyncio
async def test_account_capacity_recovery_wait_chunks_on_the_injected_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    clock = VirtualClock(monotonic_value=100.0)
    scheduler = VirtualScheduler(clock)
    monkeypatch.setattr(streaming_retry_module, "_ACCOUNT_SELECTION_RECOVERY_HEARTBEAT_SECONDS", 10.0)
    events: list[str] = []

    async def consume() -> None:
        async for event in streaming_retry_module._iter_account_capacity_recovery_wait(
            request_id="req_virtual_capacity_wait",
            model="gpt-5.5",
            account_id="account-virtual-capacity",
            error_message="Account stream concurrency limit reached",
            recovery_sleep_seconds=25.0,
            remaining_budget_seconds=60.0,
            emit_keepalives=True,
            stage="selection",
            scheduler=scheduler,
            clock=clock,
        ):
            events.append(event)

    consumer = scheduler.create_task(consume())
    try:
        await scheduler.drain()
        assert not consumer.done()
        assert [_payload_retry_after_seconds(event) for event in events] == [25]
        # Heartbeat chunks park on virtual timers, never on wall-clock sleeps.
        assert scheduler.pending_timers == 1

        await scheduler.advance(10.0)
        assert [_payload_retry_after_seconds(event) for event in events] == [25, 15]

        await scheduler.advance(10.0)
        assert [_payload_retry_after_seconds(event) for event in events] == [25, 15, 5]
        assert not consumer.done()

        await scheduler.advance(5.0)
        await consumer

        assert clock.monotonic() == pytest.approx(125.0)
        assert scheduler.pending_timers == 0
    finally:
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_transient_retry_backoff_and_stream_close_owners_use_the_injected_scheduler(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    settings = _make_proxy_settings()
    request_logs = _RequestLogsRecorder()
    service, clock, scheduler = _virtual_service(request_logs)
    account = _make_account("acc_virtual_backoff")
    attempts = 0

    monkeypatch.setattr(proxy_service, "get_settings_cache", lambda: _SettingsCache(settings))
    monkeypatch.setattr(proxy_service, "get_settings", lambda: settings)
    monkeypatch.setattr(streaming_retry_module, "backoff_seconds", lambda _attempt: 2.5)
    monkeypatch.setattr(
        service,
        "_select_account_with_budget_compatible",
        AsyncMock(return_value=AccountSelection(account=account, error_message=None)),
    )
    monkeypatch.setattr(service, "_ensure_fresh_with_budget", AsyncMock(side_effect=lambda account, **_k: account))
    monkeypatch.setattr(service, "_handle_stream_error", AsyncMock())
    monkeypatch.setattr(service, "_write_request_log", AsyncMock())
    monkeypatch.setattr(service._load_balancer, "record_success", AsyncMock())
    monkeypatch.setattr(service._load_balancer, "record_errors", AsyncMock())

    async def fake_stream_once(_account: Account, *_args: object, **_kwargs: object) -> AsyncIterator[str]:
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise _TransientStreamError(
                "server_error",
                cast(UpstreamError, {"message": "upstream hiccup", "code": "server_error"}),
            )
        yield 'data: {"type":"response.completed","response":{"id":"resp_virtual_backoff"}}\n\n'

    monkeypatch.setattr(service, "_stream_once", fake_stream_once)
    payload = ResponsesRequest.model_validate({"model": "gpt-5.1", "instructions": "hi", "input": [], "stream": True})

    async def collect() -> list[str]:
        return [
            chunk
            async for chunk in service._stream_with_retry(
                payload,
                {"session_id": "sid-virtual-backoff"},
                codex_session_affinity=False,
                propagate_http_errors=False,
                openai_cache_affinity=False,
                api_key=None,
                api_key_reservation=None,
                suppress_text_done_events=False,
                request_transport="http",
                upstream_stream_transport_override="http",
            )
        ]

    consumer = scheduler.create_task(collect())
    try:
        await scheduler.drain()
        # The first attempt failed and the retry is parked on the backoff timer.
        assert attempts == 1
        assert not consumer.done()
        assert scheduler.pending_timers == 1

        await scheduler.advance(2.0)
        assert attempts == 1
        assert not consumer.done()

        await scheduler.advance(0.5)
        chunks = await consumer

        assert attempts == 2
        assert any("response.completed" in chunk for chunk in chunks)
        assert clock.monotonic() == pytest.approx(1_002.5)
        # Both attempts closed their inner stream through an owned task.
        assert [name for name in scheduler.spawned if name.startswith("stream-inner-close-")] != []
        assert all(task.done() for task in scheduler.owned_tasks)
    finally:
        await scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
async def test_stream_once_api_key_heartbeat_is_scheduler_owned(monkeypatch: pytest.MonkeyPatch) -> None:
    request_logs = _RequestLogsRecorder()
    service, clock, scheduler = _virtual_service(request_logs)
    account = _make_account("acc_virtual_heartbeat")
    api_key = _make_api_key_data("key_virtual_heartbeat")
    reservation = ApiKeyUsageReservationData(
        reservation_id="resv_virtual_heartbeat",
        key_id=api_key.id,
        model="gpt-5.4",
    )
    heartbeat_stop_events: list[asyncio.Event] = []

    async def fake_heartbeat(**kwargs: object) -> None:
        stop_event = cast(asyncio.Event, kwargs["stop_event"])
        heartbeat_stop_events.append(stop_event)
        await stop_event.wait()

    async def fake_stream(
        payload: object,
        headers: object,
        access_token: object,
        account_id: object,
        base_url: object = None,
        raise_for_status: bool = False,
        enforce_openai_sdk_contract: bool = True,
    ) -> AsyncIterator[str]:
        del payload, headers, access_token, account_id, base_url, raise_for_status, enforce_openai_sdk_contract
        assert heartbeat_stop_events, "the reservation heartbeat must be running before the upstream stream starts"
        yield 'data: {"type":"response.completed","response":{"id":"resp_virtual_heartbeat"}}\n\n'

    monkeypatch.setattr(service, "_run_api_key_reservation_heartbeat", fake_heartbeat)
    monkeypatch.setattr(proxy_service, "core_stream_responses", fake_stream)
    payload = ResponsesRequest.model_validate({"model": "gpt-5.4", "instructions": "hi", "input": [], "stream": True})

    chunks = [
        chunk
        async for chunk in service._stream_once(
            account,
            payload,
            {"session_id": "sid-virtual-heartbeat"},
            "req_virtual_heartbeat",
            False,
            request_started_at=clock.monotonic(),
            api_key=api_key,
            api_key_reservation=reservation,
            settlement=proxy_service._StreamSettlement(),
            suppress_text_done_events=False,
            upstream_stream_transport=None,
            request_transport="http",
        )
    ]

    assert any("response.completed" in chunk for chunk in chunks)
    assert [name for name in scheduler.spawned if name.endswith("fake_heartbeat")] != []
    assert len(heartbeat_stop_events) == 1
    assert heartbeat_stop_events[0].is_set()
    await scheduler.drain()
    assert scheduler.owned_tasks == frozenset()
    assert await service.drain_persistence_tasks(timeout_seconds=1.0)


async def _overload_retry_case(
    monkeypatch: pytest.MonkeyPatch,
    *,
    post_refresh: bool = False,
    error_code: str = "server_is_overloaded",
    http_error: bool = False,
    guard: str | None = None,
    no_sibling: bool = False,
    keyed: bool = False,
    hold_failed_release: bool = False,
) -> SimpleNamespace:
    settings = _make_proxy_settings()
    if guard == "single_account":
        settings.routing_strategy = "single_account"
        settings.single_account_id = "overload-a"
    service, clock, scheduler = _virtual_service(_RequestLogsRecorder())
    account_a = _make_account("overload-a")
    account_b = _make_account("overload-b")
    attempts: list[str] = []
    selections: list[dict[str, Any]] = []
    leases: list[AccountLease] = []
    released: list[AccountLease] = []
    effects: list[str] = []
    failed_release_entered = asyncio.Event()
    failed_release_gate = asyncio.Event()
    second_dispatch = asyncio.Event()
    api_key = _make_api_key_data("overload-key") if keyed else None
    reservation = (
        ApiKeyUsageReservationData(reservation_id="overload-reservation", key_id=api_key.id, model="gpt-5.1")
        if api_key is not None
        else None
    )
    monkeypatch.setattr(proxy_service, "get_settings", lambda: settings)
    monkeypatch.setattr(proxy_service, "get_settings_cache", lambda: _SettingsCache(settings))
    monkeypatch.setattr(proxy_service, "_STREAM_MAX_ACCOUNT_ATTEMPTS", 1 if guard == "attempt_limit" else 3)
    monkeypatch.setattr(proxy_service, "_MAX_TRANSIENT_SAME_ACCOUNT_RETRIES", 3)
    monkeypatch.setattr(streaming_retry_module, "backoff_seconds", lambda _attempt: 2.5)
    monkeypatch.setattr(service, "_ensure_fresh_with_budget", AsyncMock(side_effect=lambda account, **_k: account))
    monkeypatch.setattr(service, "_write_request_log", AsyncMock())
    monkeypatch.setattr(service, "_write_stream_preflight_error", AsyncMock())
    monkeypatch.setattr(service, "_resolve_websocket_previous_response_owner", AsyncMock(return_value=account_a.id))
    monkeypatch.setattr(service, "_resolve_compact_turn_state_owner", AsyncMock(return_value=account_a.id))
    monkeypatch.setattr(service._load_balancer, "record_success", AsyncMock())
    record_errors = AsyncMock()
    monkeypatch.setattr(service._load_balancer, "record_errors", record_errors)

    async def health(_account: Account, _error: UpstreamError, code: str, **_kwargs: Any) -> None:
        # The initial 401's existing health path is outside this regression.
        if code in {"server_is_overloaded", "overloaded_error"}:
            effects.append("health")
            if keyed:
                assert "settle" in effects

    async def settle(*_args: Any, **kwargs: Any) -> bool:
        effects.append("settle")
        if any(account_id == account_b.id for account_id in attempts) and keyed:
            assert kwargs.get("wait_for_settlement") is True
        return True

    monkeypatch.setattr(service, "_handle_stream_error", health)
    monkeypatch.setattr(service, "_settle_stream_api_key_usage", settle)
    monkeypatch.setattr(service, "_release_unsettled_stream_api_key_usage", AsyncMock(return_value=True))
    real_release = service._load_balancer.release_account_lease

    async def release(lease: AccountLease | None) -> None:
        if lease is None:
            return
        released.append(lease)
        await real_release(lease)
        if hold_failed_release and lease.account_id == account_a.id:
            failed_release_entered.set()
            await failed_release_gate.wait()

    monkeypatch.setattr(service._load_balancer, "release_account_lease", release)

    async def select(**kwargs: Any) -> AccountSelection:
        # Keep both production selector layers: this replaces only the final
        # pool boundary and uses real lease admission/release accounting.
        selections.append(dict(kwargs))
        excluded = set(kwargs.get("exclude_account_ids") or ())
        required = kwargs.get("required_account_id")
        candidates = [account_a] if no_sibling else [account_a, account_b]
        for account in candidates:
            if account.id in excluded or (required is not None and account.id != required):
                continue
            lease = await service._load_balancer.acquire_account_lease(
                account.id,
                kind=kwargs["lease_kind"],
                estimated_tokens=kwargs["estimated_lease_tokens"],
                concurrency_caps=kwargs["concurrency_caps"],
                api_key_id=kwargs["api_key_id"],
            )
            if lease is not None:
                leases.append(lease)
                return AccountSelection(account=account, error_message=None, lease=lease)
        return AccountSelection(account=None, error_message="No eligible sibling", error_code="no_accounts")

    monkeypatch.setattr(service._load_balancer, "select_account", select)
    payload_data: dict[str, Any] = {"model": "gpt-5.1", "instructions": "hi", "input": [], "stream": True}
    headers: dict[str, str] = {}
    if guard == "previous_response":
        payload_data["previous_response_id"] = "resp_owner"
    if guard == "account_payload":
        payload_data["input"] = [{"type": "item_reference", "id": "item_owner"}]
    if guard == "turn_state":
        headers["x-codex-turn-state"] = "owner-turn-state"
    payload = ResponsesRequest.model_validate(payload_data)

    async def stream_once(account: Account, *_args: Any, **kwargs: Any) -> AsyncIterator[str]:
        attempts.append(account.id)
        if post_refresh and len(attempts) == 1:
            raise ProxyResponseError(401, openai_error("invalid_api_key", "expired"))
        overload_attempt = 2 if post_refresh else 1
        if len(attempts) > overload_attempt:
            second_dispatch.set()
        if len(attempts) == overload_attempt or (hold_failed_release and account.id == account_a.id):
            if guard == "budget":
                # Budget expiry is an injected boundary, not a wall-clock race.
                monkeypatch.setattr(service, "_remaining_budget_seconds", lambda _deadline: 0.0)
            if guard == "visible":
                kwargs["settlement"].downstream_visible = True
                yield 'data: {"type":"response.output_text.delta","delta":"visible"}\n\n'
            if guard == "terminal":
                kwargs["settlement"].status = "error"
                yield 'data: {"type":"response.failed","response":{"error":{"code":"server_is_overloaded"}}}\n\n'
                raise proxy_service._TerminalStreamError(
                    error_code, cast(UpstreamError, {"code": error_code, "message": "terminal"})
                )
            if http_error:
                raise ProxyResponseError(500, openai_error(error_code, "upstream overloaded"))
            raise _TransientStreamError(error_code, cast(UpstreamError, {"code": error_code, "message": "hiccup"}))
        kwargs["settlement"].status = "success"
        kwargs["settlement"].record_success = True
        yield 'data: {"type":"response.completed","response":{"id":"resp_overload_ok"}}\n\n'

    monkeypatch.setattr(service, "_stream_once", stream_once)

    async def collect() -> list[str]:
        return [
            chunk
            async for chunk in service._stream_with_retry(
                payload,
                headers,
                codex_session_affinity=False,
                openai_cache_affinity=False,
                propagate_http_errors=False,
                api_key=api_key,
                api_key_reservation=reservation,
                suppress_text_done_events=False,
                request_transport="http",
                upstream_stream_transport_override="http",
                rewritten_file_account_id=account_a.id if guard == "file" else None,
                file_account_resolution_complete=True,
            )
        ]

    return SimpleNamespace(
        service=service,
        clock=clock,
        scheduler=scheduler,
        collect=collect,
        attempts=attempts,
        selections=selections,
        leases=leases,
        released=released,
        effects=effects,
        record_errors=record_errors,
        second_dispatch=second_dispatch,
        failed_release_entered=failed_release_entered,
        failed_release_gate=failed_release_gate,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("post_refresh,http_error", [(False, False), (False, True), (True, False)])
@pytest.mark.parametrize("error_code", ["server_is_overloaded", "overloaded_error"])
async def test_overload_admits_sibling_before_same_account_retry(
    monkeypatch: pytest.MonkeyPatch,
    post_refresh: bool,
    http_error: bool,
    error_code: str,
) -> None:
    case = await _overload_retry_case(
        monkeypatch,
        post_refresh=post_refresh,
        http_error=http_error,
        error_code=error_code,
        keyed=True,
    )
    consumer = case.scheduler.create_task(case.collect())
    try:
        # Advance the known old backoff once so RED fails on actual attempts
        # [A,A], not on a missing symbol or an unobserved scheduler deadline.
        await case.scheduler.advance(2.5)
        await asyncio.wait_for(case.second_dispatch.wait(), timeout=1.0)
        chunks = await asyncio.wait_for(consumer, timeout=1.0)
        expected = ["overload-a", "overload-b"]
        if post_refresh:
            expected.insert(0, "overload-a")
        assert case.attempts == expected
        assert json.loads(chunks[-1].split("data: ", 1)[1])["type"] == "response.completed"
        assert len(case.selections) == 2  # retained admission, no third selection
        initial, replacement = case.selections
        assert set(initial["exclude_account_ids"]) == set()
        assert set(replacement["exclude_account_ids"]) == {"overload-a"}
        for key in (
            "model",
            "service_tier",
            "account_ids",
            "require_security_work_authorized",
            "lease_kind",
            "estimated_lease_tokens",
            "concurrency_caps",
            "api_key_id",
            "sticky_key",
            "sticky_kind",
            "sticky_source",
            "reallocate_sticky",
            "routing_strategy",
        ):
            assert initial[key] == replacement[key]
        assert case.effects == ["settle", "health"]
        case.record_errors.assert_not_awaited()
        assert case.released == case.leases
        assert await case.service._load_balancer.account_pressure_snapshot("overload-a") == (0, 0, 0.0)
        assert await case.service._load_balancer.account_pressure_snapshot("overload-b") == (0, 0, 0.0)
    finally:
        await case.scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("post_refresh", [False, True])
@pytest.mark.parametrize(
    "guard",
    [
        "no_sibling",
        "generic",
        "file",
        "turn_state",
        "previous_response",
        "account_payload",
        "single_account",
        "attempt_limit",
    ],
)
async def test_overload_preserves_same_account_fallback_and_owner_guards(
    monkeypatch: pytest.MonkeyPatch,
    post_refresh: bool,
    guard: str,
) -> None:
    case = await _overload_retry_case(
        monkeypatch,
        post_refresh=post_refresh,
        guard=guard,
        no_sibling=guard == "no_sibling",
        error_code="server_error" if guard == "generic" else "server_is_overloaded",
    )
    consumer = case.scheduler.create_task(case.collect())
    try:
        await case.scheduler.drain()
        assert not case.second_dispatch.is_set()
        assert case.scheduler.pending_timers == 1
        await case.scheduler.advance(2.5)
        await asyncio.wait_for(case.second_dispatch.wait(), timeout=1.0)
        chunks = await asyncio.wait_for(consumer, timeout=1.0)
        assert case.attempts == ["overload-a"] * (3 if post_refresh else 2)
        assert json.loads(chunks[-1].split("data: ", 1)[1])["type"] == "response.completed"
        assert len(case.selections) == (2 if guard == "no_sibling" else 1)
        assert case.released == case.leases
        case.record_errors.assert_not_awaited()
    finally:
        await case.scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("post_refresh", [False, True])
@pytest.mark.parametrize("guard", ["visible", "terminal", "budget"])
async def test_overload_does_not_expand_replay_or_budget(
    monkeypatch: pytest.MonkeyPatch,
    post_refresh: bool,
    guard: str,
) -> None:
    case = await _overload_retry_case(monkeypatch, post_refresh=post_refresh, guard=guard)
    consumer = case.scheduler.create_task(case.collect())
    try:
        chunks = await asyncio.wait_for(consumer, timeout=1.0)
        assert case.attempts == ["overload-a"] * (2 if post_refresh else 1)
        assert len(case.selections) == 1
        assert not case.second_dispatch.is_set()
        assert any(json.loads(chunk.split("data: ", 1)[1])["type"] == "response.failed" for chunk in chunks)
        assert case.released == case.leases
    finally:
        await case.scheduler.cancel_owned_tasks()


@pytest.mark.asyncio
@pytest.mark.parametrize("post_refresh", [False, True])
async def test_overload_retained_admission_is_released_if_cancelled_before_consumption(
    monkeypatch: pytest.MonkeyPatch,
    post_refresh: bool,
) -> None:
    case = await _overload_retry_case(
        monkeypatch,
        post_refresh=post_refresh,
        keyed=True,
        hold_failed_release=True,
    )
    # The release signal is installed before the request can admit a sibling.
    consumer = case.scheduler.create_task(case.collect())
    try:
        await case.scheduler.advance(5.0)
        await asyncio.wait_for(case.failed_release_entered.wait(), timeout=1.0)
        assert [lease.account_id for lease in case.leases] == ["overload-a", "overload-b"]
        assert "overload-b" not in case.attempts
        assert (await case.service._load_balancer.account_pressure_snapshot("overload-b"))[1] == 1
        consumer.cancel()
        with pytest.raises(asyncio.CancelledError):
            await asyncio.wait_for(consumer, timeout=1.0)
        await case.scheduler.drain()
        assert await case.service.drain_persistence_tasks(timeout_seconds=1.0)
        assert case.released == case.leases
        assert await case.service._load_balancer.account_pressure_snapshot("overload-b") == (0, 0, 0.0)
        assert case.effects == ["settle", "health"]
        case.record_errors.assert_not_awaited()
    finally:
        case.failed_release_gate.set()
        await case.scheduler.cancel_owned_tasks()
