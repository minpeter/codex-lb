from __future__ import annotations

import asyncio
import json
from collections.abc import Awaitable, Callable
from unittest.mock import AsyncMock

import aiohttp
import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer

from app.core.clients import proxy, proxy_websocket
from app.core.clients.codex_version import CodexVersionCache, get_codex_version_cache
from app.core.openai import model_refresh_scheduler as scheduler_module
from app.core.openai.requests import ResponsesRequest

pytestmark = pytest.mark.integration


@pytest.mark.asyncio
async def test_follower_version_reaches_loopback_responses_without_rewriting_native_headers(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    cache = get_codex_version_cache()
    await cache.invalidate()
    fetch = AsyncMock(return_value="9.9.9")
    reconciled = asyncio.Event()
    received: list[dict[str, str]] = []

    class Follower:
        async def run_if_leader(self, fn: Callable[[], Awaitable[object]]) -> None:
            return None

    async def reconcile() -> None:
        reconciled.set()

    async def respond(request: web.Request) -> web.Response:
        body = await request.json()
        assert body["model"] == "gpt-5.6-sol"
        received.append({key.lower(): value for key, value in request.headers.items()})
        return web.json_response({"id": "resp_loopback", "object": "response", "status": "completed", "output": []})

    monkeypatch.setattr(CodexVersionCache, "_fetch_latest_version", fetch)
    monkeypatch.setattr(scheduler_module, "_get_leader_election", lambda: Follower())
    monkeypatch.setattr(scheduler_module, "reconcile_model_registry_from_store", reconcile)
    model_fetch = AsyncMock(side_effect=AssertionError("follower must not fetch models"))
    monkeypatch.setattr(scheduler_module, "fetch_models_for_plan", model_fetch)
    monkeypatch.setattr(proxy, "discover_native_egress_client", lambda: None)
    scheduler = scheduler_module.ModelRefreshScheduler(interval_seconds=3600, enabled=True)
    app = web.Application()
    app.router.add_post("/codex/responses", respond)
    try:
        async with TestServer(app) as server, aiohttp.ClientSession(trust_env=False) as session:
            for phase, version in [("cold", "0.153.4"), ("warm", "9.9.9")]:
                if phase == "warm":
                    await scheduler.start()
                    async with asyncio.timeout(2):
                        await reconciled.wait()
                    fetch.assert_awaited_once_with()
                inbound = {
                    "User-Agent": "OpenAI/Python 2.24.0",
                    "version": "sdk",
                    "originator": "sdk",
                    "x-stainless-lang": "python",
                    "x-codex-turn-state": "fixture-continuity",
                    "X-Codex-Routing-Hint": "model=untrusted;tier=priority",
                }
                for builder in (
                    proxy._build_upstream_headers,
                    proxy._build_upstream_websocket_headers,
                    proxy._build_upstream_transcribe_headers,
                    proxy_websocket._build_upstream_websocket_headers,
                ):
                    headers = builder(inbound, "fixture-token", "fixture-account")
                    assert headers["version"] == version
                    assert headers["User-Agent"].startswith(f"codex_cli_rs/{version} ")

                for tier in (None, "priority"):
                    events = [
                        event
                        async for event in proxy.stream_responses(
                            payload=ResponsesRequest(
                                model="gpt-5.6-sol", instructions="", input="OK", stream=False, service_tier=tier
                            ),
                            headers=inbound,
                            access_token="fixture-token",
                            account_id="fixture-account",
                            base_url=str(server.make_url("")),
                            session=session,
                            suppress_live_usage=True,
                            synthesize_routing_hint=True,
                        )
                    ]
                    assert len(events) == 1
                    assert json.loads(events[0].split("data: ", 1)[1])["type"] == "response.completed"
                    wire = received[-1]
                    assert wire["version"] == version
                    assert wire["user-agent"].startswith(f"codex_cli_rs/{version} ")
                    assert wire["originator"] == "codex_cli_rs"
                    assert wire["x-codex-turn-state"] == "fixture-continuity"
                    assert "x-stainless-lang" not in wire
                    assert wire["x-codex-routing-hint"] == (
                        "model=gpt-5.6-sol;tier=priority" if tier else "model=gpt-5.6-sol"
                    )
                    print(
                        json.dumps({"phase": phase, "version": wire["version"], "hint": wire["x-codex-routing-hint"]})
                    )

            for user_agent, originator in (
                ("codex_exec/0.142.1", "codex_exec"),
                ("OpenAI/Node 5.0.0", "codex_sdk_ts"),
            ):
                native = {"User-Agent": user_agent, "originator": originator, "version": "0.142.1"}
                async for _event in proxy.stream_responses(
                    payload=ResponsesRequest(model="gpt-5.6-sol", instructions="", input="OK", stream=False),
                    headers=native,
                    access_token="fixture-token",
                    account_id="fixture-account",
                    base_url=str(server.make_url("")),
                    session=session,
                    suppress_live_usage=True,
                ):
                    pass
                assert received[-1]["user-agent"] == user_agent
                assert received[-1]["originator"] == originator
                assert received[-1]["version"] == "0.142.1"
                assert "x-codex-routing-hint" not in received[-1]
                print(json.dumps({"phase": "native", "user-agent": user_agent, "version": received[-1]["version"]}))
            fetch.assert_awaited_once_with()
            model_fetch.assert_not_awaited()
    finally:
        await scheduler.stop()
        await cache.invalidate()
