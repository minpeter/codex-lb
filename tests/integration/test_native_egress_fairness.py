from __future__ import annotations

import asyncio
import json
import stat
import sys
from pathlib import Path
from typing import cast

import aiohttp
import pytest

from app.core.clients.codex import CodexClient
from app.core.clients.native_egress import SubprocessNativeEgressClient
from app.core.clients.proxy import ProxyResponseError, stream_responses
from app.core.openai.requests import ResponsesRequest
from app.core.upstream_proxy import ResolvedProxyEndpoint, ResolvedUpstreamRoute


class _UnexpectedPythonSession:
    def post(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("native direct request must not fall back to Python")

    async def request(self, *_args: object, **_kwargs: object) -> object:
        raise AssertionError("native routed request must not fall back to Python")


@pytest.mark.asyncio
@pytest.mark.parametrize("routed", [False, True], ids=["direct", "routed"])
@pytest.mark.parametrize("mode", ["sse", "json", "error"])
async def test_buffered_legacy_helper_burst_reaches_responses_surface(tmp_path: Path, routed: bool, mode: str) -> None:
    response = {"id": "resp_burst", "object": "response", "status": "completed", "output": []}
    error = {"error": {"code": "rate_limit_exceeded", "type": "rate_limit_error", "message": "busy"}}
    deltas = [{"type": "response.output_text.delta", "delta": str(index)} for index in range(8192)]
    terminal = {"type": "response.completed", "response": response}
    if mode == "sse":
        wire = "".join(f"data: {json.dumps(event)}\n\n" for event in [*deltas, terminal])
        # The legacy helper carries raw HTTP body chunks, not framed SSE events.
        # Coalescing and splitting across event separators exercises that contract.
        chunks = [wire[offset : offset + 4096] for offset in range(0, len(wire), 4096)]
    else:
        chunks = list(json.dumps(error if mode == "error" else response))
    helper = tmp_path / "native-helper"
    helper.write_text(
        f"#!{sys.executable}\n"
        + f"chunks = {chunks!r}\nstatus = {429 if mode == 'error' else 200}\n"
        + f"content_type = {'text/event-stream' if mode == 'sse' else 'application/json'!r}\n"
        + f"routed = {routed!r}\n"
        + r"""
import base64
import json
import sys
hello = json.loads(sys.stdin.readline())
assert hello == {"type": "client_hello", "min_protocol_version": 1, "max_protocol_version": 1}
print(json.dumps({"type": "server_hello", "protocol_version": 1, "capabilities": [
    "failure_provenance_v1", "http", "http2_profile_v1", "websocket", "websocket_send_ack"
]}), flush=True)
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": request_id}), flush=True)
        continue
    assert command["type"] == "request"
    assert command["method"] == "POST"
    assert command.get("proxy_url") == ("http://127.0.0.1:12345" if routed else None)
    assert json.loads(base64.b64decode(command["body"]))["model"] == "gpt-5.4"
    events = [{"type": "head", "request_id": request_id, "status": status,
               "http_version": "HTTP/2.0", "headers": [["content-type", content_type]]}]
    events.extend({"type": "chunk", "request_id": request_id,
                   "data": base64.b64encode(chunk.encode()).decode()} for chunk in chunks)
    events.append({"type": "end", "request_id": request_id})
    sys.stdout.write("".join(json.dumps(event) + "\n" for event in events))
    sys.stdout.flush()
""",
        encoding="utf-8",
    )
    helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
    native = SubprocessNativeEgressClient(helper)
    session = _UnexpectedPythonSession()
    route = (
        ResolvedUpstreamRoute(
            mode="account_bound",
            pool_id="burst",
            endpoint=ResolvedProxyEndpoint("local", "http", "127.0.0.1", 12345),
        )
        if routed
        else None
    )

    async def collect() -> list[object]:
        return [
            json.loads(next(line.removeprefix("data: ") for line in block.splitlines() if line.startswith("data: ")))
            async for block in stream_responses(
                ResponsesRequest(model="gpt-5.4", instructions="", input="probe", stream=mode != "json"),
                {},
                "local-test-token",
                "local-test-account",
                base_url="https://upstream.invalid",
                raise_for_status=True,
                upstream_stream_transport_override="http",
                native_egress_client=native,
                codex_client=CodexClient(session, native_egress_client=native),
                route=route,
                allow_direct_egress=False,
                suppress_live_usage=True,
                session=cast(aiohttp.ClientSession, session),
            )
        ]

    try:
        async with asyncio.timeout(5):
            if mode == "error":
                with pytest.raises(ProxyResponseError) as caught:
                    await collect()
                assert caught.value.status_code == 429
                assert caught.value.payload == error
            else:
                result = await collect()
                assert result == ([*deltas, terminal] if mode == "sse" else [terminal])
            assert not native._streams
            assert native._generation == 1
            assert native._process is not None and native._process.returncode is None
    finally:
        await asyncio.wait_for(native.aclose(), timeout=2)
