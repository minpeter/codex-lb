from __future__ import annotations

import asyncio
import base64
import contextlib
import json
import os
import stat
import sys
from pathlib import Path

import pytest

import app.core.clients.native_egress as native_egress_module
from app.core.clients.native_egress import (
    NativeEgressError,
    NativeEgressProtocolError,
    NativeEgressRequest,
    NativeEgressTransportError,
    NativeEgressUnavailable,
    NativeWebSocketMessage,
    NativeWebSocketRequest,
    SubprocessNativeEgressClient,
    close_discovered_native_egress_client,
    discover_native_egress_client,
)

_HELPER_PROTOCOL_PREAMBLE = r"""
import json
import sys

hello = json.loads(sys.stdin.readline())
assert hello == {
    "type": "client_hello",
    "min_protocol_version": 1,
    "max_protocol_version": 1,
}
print(json.dumps({
    "type": "server_hello",
    "protocol_version": 1,
    "capabilities": [
        "failure_provenance_v1",
        "http",
        "http2_profile_v1",
        "websocket",
        "websocket_send_ack",
    ],
}), flush=True)
"""


def _write_helper(path: Path, source: str) -> None:
    if source.startswith("#!/usr/bin/env python3\n"):
        source = source.replace(
            "#!/usr/bin/env python3\n",
            f"#!{sys.executable}\n{_HELPER_PROTOCOL_PREAMBLE}\n",
            1,
        )
    path.write_text(source, encoding="utf-8")
    path.chmod(path.stat().st_mode | stat.S_IXUSR)


def _echo_helper_source() -> str:
    return """#!/usr/bin/env python3
import base64
import json
import sys

for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": request_id}), flush=True)
        continue
    assert command["headers"] == [["accept", "text/event-stream"]]
    body = base64.b64decode(command["body"] or "")
    head = {
        "type": "head",
        "request_id": request_id,
        "status": 200,
        "http_version": "HTTP/2.0",
        "headers": [["content-type", "text/event-stream"]],
    }
    print(json.dumps(head), flush=True)
    payload = command["url"].rsplit("/", 1)[-1].encode() + b":" + body
    print(json.dumps({
        "type": "chunk",
        "request_id": request_id,
        "data": base64.b64encode(payload).decode(),
    }), flush=True)
    print(json.dumps({"type": "end", "request_id": request_id}), flush=True)
"""


@pytest.mark.asyncio
async def test_subprocess_native_egress_reuses_process_and_streams_response(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(helper, _echo_helper_source())
    client = SubprocessNativeEgressClient(helper)
    request = NativeEgressRequest(
        method="POST",
        url="https://example.test/codex/one",
        headers={"accept": "text/event-stream"},
        body=b"request-body",
    )

    first = await client.request(request)
    process = client._process
    assert first.status == 200
    assert first.http_version == "HTTP/2.0"
    assert first.raw_headers == (("content-type", "text/event-stream"),)
    assert first.headers["Content-Type"] == "text/event-stream"
    assert await first.read() == b"one:request-body"

    second = await client.request(
        NativeEgressRequest(
            method="POST",
            url="https://example.test/codex/two",
            headers={"accept": "text/event-stream"},
            body=b"next",
        )
    )
    assert await second.read() == b"two:next"
    assert client._process is process
    assert process is not None and process.returncode is None

    await client.aclose()
    assert process.returncode is not None


@pytest.mark.asyncio
async def test_subprocess_native_egress_rejects_incompatible_helper(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    helper.write_text(
        """#!/usr/bin/env python3
import json
import sys

json.loads(sys.stdin.readline())
print(json.dumps({
    "type": "server_hello",
    "protocol_version": 2,
    "capabilities": [],
}), flush=True)
sys.stdin.read()
""",
        encoding="utf-8",
    )
    helper.write_text(helper.read_text().replace("#!/usr/bin/env python3", f"#!{sys.executable}", 1))
    helper.chmod(helper.stat().st_mode | stat.S_IXUSR)
    client = SubprocessNativeEgressClient(helper)

    with pytest.raises(NativeEgressProtocolError, match="unsupported protocol version"):
        await client.request(NativeEgressRequest(method="GET", url="https://example.test", headers={}))

    assert client._process is None


@pytest.mark.asyncio
async def test_subprocess_native_egress_demultiplexes_interleaved_requests(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import base64
import json
import sys

requests = []
for line in sys.stdin:
    command = json.loads(line)
    if command["type"] == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": command["request_id"]}), flush=True)
        continue
    requests.append(command)
    if len(requests) != 2:
        continue
    for request in reversed(requests):
        print(json.dumps({
            "type": "head", "request_id": request["request_id"], "status": 200,
            "http_version": "HTTP/2.0", "headers": [],
        }), flush=True)
    for request in requests:
        payload = request["url"].rsplit("/", 1)[-1].encode()
        print(json.dumps({
            "type": "chunk", "request_id": request["request_id"],
            "data": base64.b64encode(payload).decode(),
        }), flush=True)
    for request in reversed(requests):
        print(json.dumps({"type": "end", "request_id": request["request_id"]}), flush=True)
    requests.clear()
""",
    )
    client = SubprocessNativeEgressClient(helper)

    left, right = await asyncio.gather(
        client.request(NativeEgressRequest(method="GET", url="https://example.test/left", headers={})),
        client.request(NativeEgressRequest(method="GET", url="https://example.test/right", headers={})),
    )
    left_body, right_body = await asyncio.gather(left.read(), right.read())

    assert left_body == b"left"
    assert right_body == b"right"
    await client.aclose()


@pytest.mark.asyncio
async def test_response_close_cancels_only_owned_request(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import base64
import json
import sys

for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": request_id}), flush=True)
        continue
    print(json.dumps({
        "type": "head", "request_id": request_id, "status": 200,
        "http_version": "HTTP/2.0", "headers": [],
    }), flush=True)
    if command["url"].endswith("/fast"):
        print(json.dumps({
            "type": "chunk", "request_id": request_id,
            "data": base64.b64encode(b"fast").decode(),
        }), flush=True)
        print(json.dumps({"type": "end", "request_id": request_id}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    slow = await client.request(NativeEgressRequest(method="GET", url="https://example.test/slow", headers={}))
    process = client._process

    await asyncio.wait_for(slow.aclose(), timeout=2.0)
    fast = await client.request(NativeEgressRequest(method="GET", url="https://example.test/fast", headers={}))

    assert await fast.read() == b"fast"
    assert client._process is process
    assert process is not None and process.returncode is None
    await client.aclose()


@pytest.mark.asyncio
async def test_helper_death_fails_generation_and_later_request_restarts(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    generation_file = tmp_path / "generation"
    _write_helper(
        helper,
        f"""#!/usr/bin/env python3
import base64
import json
import os
import pathlib
import sys

generation_file = pathlib.Path({str(generation_file)!r})
generation = int(generation_file.read_text()) + 1 if generation_file.exists() else 1
generation_file.write_text(str(generation))
if generation == 1:
    requests = [json.loads(sys.stdin.readline()), json.loads(sys.stdin.readline())]
    os._exit(7)
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({{"type": "cancelled", "request_id": request_id}}), flush=True)
        continue
    print(json.dumps({{
        "type": "head", "request_id": request_id, "status": 200,
        "http_version": "HTTP/2.0", "headers": [],
    }}), flush=True)
    print(json.dumps({{
        "type": "chunk", "request_id": request_id,
        "data": base64.b64encode(b"restarted").decode(),
    }}), flush=True)
    print(json.dumps({{"type": "end", "request_id": request_id}}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)

    failures = await asyncio.gather(
        client.request(NativeEgressRequest(method="POST", url="https://example.test/one", headers={})),
        client.request(NativeEgressRequest(method="POST", url="https://example.test/two", headers={})),
        return_exceptions=True,
    )
    assert all(isinstance(result, NativeEgressError) for result in failures)
    old_generation = client._generation

    restarted = await client.request(
        NativeEgressRequest(method="GET", url="https://example.test/restarted", headers={})
    )

    assert await restarted.read() == b"restarted"
    assert client._generation == old_generation + 1
    assert generation_file.read_text() == "2"
    await client.aclose()


@pytest.mark.asyncio
async def test_subprocess_native_egress_rejects_missing_helper(tmp_path: Path) -> None:
    client = SubprocessNativeEgressClient(tmp_path / "missing")

    with pytest.raises(NativeEgressUnavailable):
        await client.request(NativeEgressRequest(method="GET", url="https://example.test", headers={}))


@pytest.mark.asyncio
async def test_subprocess_native_egress_rejects_invalid_first_event(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import json
import sys
for line in sys.stdin:
    command = json.loads(line)
    if command["type"] == "request":
        print(json.dumps({"type": "chunk", "request_id": command["request_id"], "data": ""}), flush=True)
    else:
        print(json.dumps({"type": "cancelled", "request_id": command["request_id"]}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)

    with pytest.raises(NativeEgressProtocolError, match="head event"):
        await client.request(NativeEgressRequest(method="GET", url="https://example.test", headers={}))
    await client.aclose()


@pytest.mark.asyncio
async def test_subprocess_native_egress_buffers_json_error_body(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import base64
import json
import sys
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    head = {
        "type": "head", "request_id": request_id, "status": 429,
        "http_version": "HTTP/2.0", "headers": [],
    }
    print(json.dumps(head), flush=True)
    body = json.dumps({"error": {"code": "rate_limit_exceeded"}}).encode()
    print(json.dumps({"type": "chunk", "request_id": request_id, "data": base64.b64encode(body).decode()}), flush=True)
    print(json.dumps({"type": "end", "request_id": request_id}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    response = await client.request(NativeEgressRequest(method="GET", url="https://example.test", headers={}))

    assert await response.json() == {"error": {"code": "rate_limit_exceeded"}}
    assert await response.read() == b'{"error": {"code": "rate_limit_exceeded"}}'
    await client.aclose()


@pytest.mark.asyncio
async def test_subprocess_native_egress_preserves_helper_failure_provenance(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import json
import sys
for line in sys.stdin:
    command = json.loads(line)
    print(json.dumps({
        "type": "error",
        "request_id": command["request_id"],
        "message": "native upstream connection failed",
        "failure_phase": "connect",
        "retryable_same_contract": True,
        "is_tls_verification_failure": True,
    }), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)

    with pytest.raises(NativeEgressTransportError) as exc_info:
        await client.request(NativeEgressRequest(method="GET", url="https://example.test", headers={}))

    assert exc_info.value.failure_phase == "connect"
    assert exc_info.value.retryable_same_contract is True
    assert exc_info.value.is_tls_verification_failure is True
    await client.aclose()


@pytest.mark.asyncio
async def test_client_close_is_idempotent_and_prevents_restart(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(helper, _echo_helper_source())
    client = SubprocessNativeEgressClient(helper)
    response = await client.request(
        NativeEgressRequest(
            method="GET",
            url="https://example.test/one",
            headers={"accept": "text/event-stream"},
        )
    )
    await response.read()
    process = client._process

    await client.aclose()
    await client.aclose()

    assert process is not None and process.returncode is not None
    with pytest.raises(NativeEgressUnavailable, match="closed"):
        await client.request(
            NativeEgressRequest(
                method="GET",
                url="https://example.test/two",
                headers={"accept": "text/event-stream"},
            )
        )


@pytest.mark.asyncio
@pytest.mark.parametrize("overflow", ["bytes", "events"])
async def test_client_close_does_not_hang_when_stream_queue_is_full(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, overflow: str
) -> None:
    observed = _observe_helper_events(monkeypatch)
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import base64
import json
import sys
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": request_id}), flush=True)
        continue
    print(json.dumps({
        "type": "head", "request_id": request_id, "status": 200,
        "http_version": "HTTP/2.0", "headers": [],
    }), flush=True)
    if command["url"].endswith("/slow-consumer"):
        mode = dict(command["headers"])["overflow"]
        # 48 large chunks hit bytes; 4097 empty chunks hit only event capacity.
        data = base64.b64encode(b"x" * (1024 * 1024)).decode() if mode == "bytes" else ""
        for _ in range(48 if mode == "bytes" else 4097):
            print(json.dumps({
                "type": "chunk", "request_id": request_id, "data": data,
            }), flush=True)
    else:
        print(json.dumps({
            "type": "chunk", "request_id": request_id,
            "data": base64.b64encode(b"ok").decode(),
        }), flush=True)
    print(json.dumps({"type": "end", "request_id": request_id}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    try:
        stalled = await client.request(
            NativeEgressRequest(method="GET", url="https://example.test/slow-consumer", headers={"overflow": overflow})
        )
        process = client._process
        cancelled = await _wait_for_helper_event(observed, "cancelled")
        assert cancelled["request_id"] == stalled._request_id
        assert stalled._request_id not in client._streams
        assert isinstance(stalled._events, native_egress_module._BoundedEventQueue)
        assert stalled._events.queued_bytes == 0
        assert stalled._events.qsize() == 1

        healthy = await client.request(
            NativeEgressRequest(method="GET", url="https://example.test/healthy", headers={})
        )
        assert await asyncio.wait_for(healthy.read(), timeout=2.0) == b"ok"
        with pytest.raises(NativeEgressTransportError) as exc_info:
            await stalled.read()
        assert exc_info.value.failure_phase == "consumer_backpressure"
        assert client._process is process
        assert process is not None and process.returncode is None
    finally:
        await asyncio.wait_for(client.aclose(), timeout=2.0)


def test_native_helper_is_discovered_only_by_fixed_path(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    helper = tmp_path / "codex-lb-native-egress"
    _write_helper(helper, "#!/bin/sh\nexit 0\n")
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")
    discover_native_egress_client.cache_clear()

    client = discover_native_egress_client()

    assert client is not None
    assert client.executable == helper
    discover_native_egress_client.cache_clear()


@pytest.mark.asyncio
async def test_close_discovered_helper_awaits_process_and_clears_cache(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    helper = tmp_path / "codex-lb-native-egress"
    _write_helper(helper, _echo_helper_source())
    monkeypatch.setenv("PATH", f"{tmp_path}{os.pathsep}{os.environ.get('PATH', '')}")
    discover_native_egress_client.cache_clear()
    client = discover_native_egress_client()
    assert client is not None
    response = await client.request(
        NativeEgressRequest(
            method="GET",
            url="https://example.test/one",
            headers={"accept": "text/event-stream"},
        )
    )
    await response.read()
    process = client._process

    await close_discovered_native_egress_client()

    assert process is not None and process.returncode is not None
    replacement = discover_native_egress_client()
    assert replacement is not None and replacement is not client
    await close_discovered_native_egress_client()


def _websocket_helper_source() -> str:
    return """#!/usr/bin/env python3
import base64
import json
import sys

for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    kind = command["type"]
    if kind == "websocket_connect":
        assert command["headers"] == [["user-agent", "codex-cli"], ["sec-websocket-protocol", "openai"]]
        assert command["ping_interval_ms"] == 20000
        assert command["ping_timeout_ms"] is None
        print(json.dumps({
            "type": "websocket_open", "request_id": request_id, "status": 101,
            "headers": [["sec-websocket-protocol", "openai"]],
        }), flush=True)
    elif kind == "websocket_send_text":
        print(json.dumps({
            "type": "websocket_text", "request_id": request_id,
            "text": "echo:" + command["text"],
        }), flush=True)
        print(json.dumps({
            "type": "websocket_sent", "request_id": request_id,
            "command_id": command["command_id"],
        }), flush=True)
    elif kind == "websocket_send_binary":
        print(json.dumps({
            "type": "websocket_binary", "request_id": request_id,
            "data": command["data"],
        }), flush=True)
        print(json.dumps({
            "type": "websocket_sent", "request_id": request_id,
            "command_id": command["command_id"],
        }), flush=True)
    elif kind == "websocket_close":
        print(json.dumps({
            "type": "websocket_sent", "request_id": request_id,
            "command_id": command["command_id"],
        }), flush=True)
        print(json.dumps({
            "type": "websocket_close", "request_id": request_id,
            "code": command["code"], "reason": command["reason"],
        }), flush=True)
    elif kind == "cancel":
        print(json.dumps({"type": "cancelled", "request_id": request_id}), flush=True)
"""


@pytest.mark.asyncio
async def test_native_websocket_routes_frames_and_send_acknowledgements(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(helper, _websocket_helper_source())
    client = SubprocessNativeEgressClient(helper)
    websocket = await client.websocket(
        NativeWebSocketRequest(
            url="wss://example.test/codex/responses",
            headers={"user-agent": "codex-cli", "sec-websocket-protocol": "openai"},
            connect_timeout_seconds=2,
            max_message_bytes=1024,
        )
    )

    assert websocket.status == 101
    assert websocket.response_header("Sec-WebSocket-Protocol") == "openai"
    text_receive = asyncio.create_task(websocket.receive())
    await websocket.send_text("turn")
    assert await text_receive == NativeWebSocketMessage(kind="text", text="echo:turn")

    binary_receive = asyncio.create_task(websocket.receive())
    await websocket.send_bytes(b"\x00\xff")
    assert await binary_receive == NativeWebSocketMessage(kind="binary", data=b"\x00\xff")

    process = client._process
    await websocket.close(code=1000, reason="done")
    assert await websocket.receive() == NativeWebSocketMessage(kind="close", close_code=1000, close_reason="done")
    with pytest.raises(NativeEgressTransportError, match="closed"):
        await asyncio.wait_for(websocket.receive(), timeout=0.1)
    assert client._process is process
    assert process is not None and process.returncode is None
    await client.aclose()


@pytest.mark.asyncio
async def test_native_websocket_close_is_idempotent_after_peer_close_race(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import json
import sys

for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "websocket_connect":
        print(json.dumps({
            "type": "websocket_open", "request_id": request_id,
            "status": 101, "headers": [],
        }), flush=True)
    elif command["type"] == "websocket_send_text":
        print(json.dumps({
            "type": "websocket_sent", "request_id": request_id,
            "command_id": command["command_id"],
        }), flush=True)
        print(json.dumps({
            "type": "websocket_close", "request_id": request_id,
            "code": 1000, "reason": "peer done",
        }), flush=True)
    elif command["type"] == "websocket_close":
        print(json.dumps({
            "type": "websocket_error", "request_id": request_id,
            "command_id": command["command_id"],
            "message": "native websocket is not active",
            "failure_phase": "setup", "retryable_same_contract": False,
            "is_tls_verification_failure": False,
            "status": None, "headers": [], "body": None,
        }), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    websocket = await client.websocket(
        NativeWebSocketRequest(
            url="wss://example.test/codex/responses",
            headers={},
            connect_timeout_seconds=2,
            max_message_bytes=1024,
        )
    )

    await websocket.send_text("finish")
    peer_close = await websocket.receive()
    assert peer_close == NativeWebSocketMessage(
        kind="close",
        close_code=1000,
        close_reason="peer done",
    )
    await websocket.close()
    await websocket.close()
    await client.aclose()


@pytest.mark.asyncio
async def test_native_websocket_connections_are_isolated(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(helper, _websocket_helper_source())
    client = SubprocessNativeEgressClient(helper)
    request = NativeWebSocketRequest(
        url="wss://example.test/codex/responses",
        headers={"user-agent": "codex-cli", "sec-websocket-protocol": "openai"},
        connect_timeout_seconds=2,
        max_message_bytes=1024,
    )
    left, right = await asyncio.gather(client.websocket(request), client.websocket(request))

    left_receive = asyncio.create_task(left.receive())
    right_receive = asyncio.create_task(right.receive())
    await asyncio.gather(left.send_text("left"), right.send_text("right"))

    assert await left_receive == NativeWebSocketMessage(kind="text", text="echo:left")
    assert await right_receive == NativeWebSocketMessage(kind="text", text="echo:right")
    await asyncio.gather(left.close(), right.close())
    await client.aclose()


@pytest.mark.asyncio
async def test_native_websocket_preserves_handshake_denial(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import base64
import json
import sys
command = json.loads(sys.stdin.readline())
print(json.dumps({
    "type": "websocket_error", "request_id": command["request_id"],
    "command_id": None, "message": "native websocket handshake failed",
    "failure_phase": "connect", "retryable_same_contract": False,
    "status": 429, "headers": [["content-type", "application/json"]],
    "body": base64.b64encode(b'{"error":{"code":"rate_limit_exceeded"}}').decode(),
}), flush=True)
for line in sys.stdin:
    command = json.loads(line)
    print(json.dumps({"type": "cancelled", "request_id": command["request_id"]}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)

    with pytest.raises(NativeEgressTransportError) as exc_info:
        await client.websocket(
            NativeWebSocketRequest(
                url="wss://example.test/codex/responses",
                headers={},
                connect_timeout_seconds=2,
                max_message_bytes=1024,
            )
        )

    assert exc_info.value.status_code == 429
    assert exc_info.value.headers == (("content-type", "application/json"),)
    assert exc_info.value.body == b'{"error":{"code":"rate_limit_exceeded"}}'
    await client.aclose()


@pytest.mark.asyncio
async def test_native_websocket_preserves_liveness_timeout_phase(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import json
import sys
command = json.loads(sys.stdin.readline())
request_id = command["request_id"]
print(json.dumps({"type": "websocket_open", "request_id": request_id, "status": 101, "headers": []}), flush=True)
print(json.dumps({
    "type": "websocket_error", "request_id": request_id,
    "command_id": None, "message": "native websocket pong timed out",
    "failure_phase": "liveness_timeout", "retryable_same_contract": False,
    "status": None, "headers": [], "body": None,
}), flush=True)
for line in sys.stdin:
    command = json.loads(line)
    print(json.dumps({"type": "cancelled", "request_id": command["request_id"]}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    websocket = await client.websocket(
        NativeWebSocketRequest(
            url="wss://example.test/codex/responses",
            headers={},
            connect_timeout_seconds=2,
            max_message_bytes=1024,
            ping_interval_seconds=0.02,
            ping_timeout_seconds=0.05,
        )
    )

    with pytest.raises(NativeEgressTransportError) as exc_info:
        await websocket.receive()

    assert exc_info.value.failure_phase == "liveness_timeout"
    await client.aclose()


@pytest.mark.asyncio
async def test_native_websocket_helper_death_fails_pending_send_without_replay(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        """#!/usr/bin/env python3
import json
import os
import sys
command = json.loads(sys.stdin.readline())
print(json.dumps({
    "type": "websocket_open", "request_id": command["request_id"],
    "status": 101, "headers": [],
}), flush=True)
json.loads(sys.stdin.readline())
os._exit(9)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    websocket = await client.websocket(
        NativeWebSocketRequest(
            url="wss://example.test/codex/responses",
            headers={},
            connect_timeout_seconds=2,
            max_message_bytes=1024,
        )
    )

    with pytest.raises(NativeEgressError):
        await asyncio.wait_for(websocket.send_text("ambiguous"), timeout=2)

    assert client._generation == 1
    await client.aclose()


def _observe_helper_events(monkeypatch: pytest.MonkeyPatch) -> asyncio.Queue[dict[str, object]]:
    observed: asyncio.Queue[dict[str, object]] = asyncio.Queue()
    read_event = native_egress_module._read_event

    async def observe(stdout: asyncio.StreamReader) -> dict[str, object]:
        event = await read_event(stdout)
        observed.put_nowait({"type": event.get("type"), "request_id": event.get("request_id")})
        return event

    monkeypatch.setattr(native_egress_module, "_read_event", observe)
    return observed


async def _wait_for_helper_event(observed: asyncio.Queue[dict[str, object]], kind: str) -> dict[str, object]:
    async with asyncio.timeout(10):
        while True:
            event = await observed.get()
            if event.get("type") == kind:
                return event


@pytest.mark.asyncio
@pytest.mark.parametrize("count,decoded_size", [(2000, 5), (16, 3 * 512 * 1024)])
async def test_buffered_chunk_burst_completes(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, count: int, decoded_size: int
) -> None:
    observed = _observe_helper_events(monkeypatch)
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        f"""#!/usr/bin/env python3
import base64
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({{"type": "cancelled", "request_id": request_id}}), flush=True)
        continue
    print(json.dumps({{
        "type": "head", "request_id": request_id, "status": 200,
        "http_version": "HTTP/2.0", "headers": [],
    }}), flush=True)
    data = base64.b64encode(b"z" * {decoded_size}).decode()
    for _ in range({count}):
        print(json.dumps({{"type": "chunk", "request_id": request_id, "data": data}}), flush=True)
    print(json.dumps({{"type": "end", "request_id": request_id}}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    try:
        response = await client.request(NativeEgressRequest(method="GET", url="https://example.test/burst", headers={}))
        terminal = await _wait_for_helper_event(observed, "end")
        assert terminal["request_id"] == response._request_id
        # No consumer runs until the complete burst has passed the real pipe reader.
        assert isinstance(response._events, native_egress_module._BoundedEventQueue)
        assert response._events.maxsize == 4096
        assert response._events._max_bytes == 32 * 1024 * 1024
        assert response._events.queued_bytes == count * len(base64.b64encode(b"z" * decoded_size))
        assert await asyncio.wait_for(response.read(), timeout=10) == b"z" * (count * decoded_size)
        assert response._events.queued_bytes == 0
        assert response._request_id not in client._streams
    finally:
        await asyncio.wait_for(client.aclose(), timeout=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("kind", ["chunk", "websocket_text"])
async def test_reader_schedules_subscribed_consumer_between_buffered_events(kind: str) -> None:
    depths: list[int] = []

    class ObservedQueue(native_egress_module._BoundedEventQueue):
        def put_nowait(self, item: dict[str, object] | BaseException) -> None:
            if isinstance(item, dict):
                depths.append(self.qsize())
            super().put_nowait(item)

    events = ObservedQueue(max_events=4096, max_bytes=32 * 1024 * 1024)
    stdout = asyncio.StreamReader()

    class ExitedTransport(asyncio.SubprocessTransport):
        def get_pid(self) -> int:
            return 0

        def get_returncode(self) -> int:
            return 0

    loop = asyncio.get_running_loop()
    protocol = asyncio.subprocess.SubprocessStreamProtocol(limit=65536, loop=loop)
    protocol.stdout = stdout
    process = asyncio.subprocess.Process(ExitedTransport(), protocol, loop)
    client = SubprocessNativeEgressClient("unused")
    client._generation = 1
    client._streams["active"] = (1, events)
    subscribed = asyncio.Event()

    async def consume() -> list[dict[str, object] | BaseException]:
        subscribed.set()
        return [await events.get(), await events.get()]

    consumer = asyncio.create_task(consume())
    reader = asyncio.create_task(client._read_process(process, 1))
    burst = [{"type": kind, "request_id": "active", "data": "YQ==", "text": "a"}] * 2
    try:
        async with asyncio.timeout(2):
            await subscribed.wait()
            # Both readline calls now complete synchronously. Queue capacity
            # cannot explain the ordering: only two of 4096 slots are needed.
            stdout.feed_data(b"".join(json.dumps(event).encode() + b"\n" for event in burst))
            assert await consumer == burst
        assert depths == [0, 0]
        assert events.queued_bytes == 0
    finally:
        reader.cancel()
        consumer.cancel()
        for task in (reader, consumer):
            with contextlib.suppress(asyncio.CancelledError):
                await task


@pytest.mark.asyncio
async def test_bounded_event_queue_charges_projected_bytes_and_releases_on_get() -> None:
    queue = native_egress_module._BoundedEventQueue(max_events=4, max_bytes=10)
    queue.put_nowait({"type": "chunk", "data": "abcd"})
    queue.put_nowait({"type": "websocket_text", "text": "efgh"})
    assert queue.queued_bytes == 8 and not queue.full()
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait({"type": "chunk", "data": "ijk"})
    assert queue.queued_bytes == 8
    queue.put_nowait({"type": "chunk", "data": "ij"})
    queue.put_nowait({"type": "end"})
    assert queue.queued_bytes == 10 and queue.full()
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait({"type": "cancelled"})
    assert await queue.get() == {"type": "chunk", "data": "abcd"}
    assert queue.queued_bytes == 6 and not queue.full()
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait({"type": "chunk", "data": "12345"})
    queue.put_nowait({"type": "chunk", "data": "1234"})
    while not queue.empty():
        queue.get_nowait()
    assert queue.queued_bytes == 0
    queue.put_nowait(RuntimeError("failure"))
    assert queue.queued_bytes == 0
    queue.get_nowait()
    queue.put_nowait({"type": "websocket_text", "text": "\u00e9\u00e9"})
    assert queue.queued_bytes == 4
    await queue.get()
    queue.put_nowait({"type": "chunk", "data": "x" * 64})
    assert queue.queued_bytes == 64
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait({"type": "chunk", "data": "y"})
    queue.put_nowait({"type": "end"})
    queue.put_nowait({"type": "error"})
    queue.put_nowait({"type": "cancelled"})
    with pytest.raises(asyncio.QueueFull):
        queue.put_nowait(RuntimeError("failure"))
    while not queue.empty():
        await queue.get()
    assert queue.queued_bytes == 0


def test_generation_failure_drains_bytes_only_for_owned_generation(tmp_path: Path) -> None:
    client = SubprocessNativeEgressClient(tmp_path / "unused")
    old = native_egress_module._new_stream_queue()
    current = native_egress_module._new_stream_queue()
    for queue in (old, current):
        queue.put_nowait({"type": "chunk", "data": "eA=="})
    client._generation = 2
    client._streams = {"1:1": (1, old), "2:1": (2, current)}
    failure = NativeEgressTransportError("helper failed", failure_phase="helper_exit")
    client._fail_generation(1, failure)
    assert old.queued_bytes == 0
    assert old.get_nowait() is failure
    assert current.queued_bytes == 4
    assert client._streams == {"2:1": (2, current)}
    client._finish_request("2:1", 1, current)
    client._finish_request("2:1", 2, old)
    assert client._streams == {"2:1": (2, current)}
    client._fail_generation(2, failure)
    assert current.queued_bytes == 0
    assert current.get_nowait() is failure
    assert not client._streams


@pytest.mark.asyncio
async def test_websocket_helper_budget_retains_separate_message_cap(tmp_path: Path) -> None:
    helper = tmp_path / "native-helper"
    _write_helper(helper, _websocket_helper_source())
    client = SubprocessNativeEgressClient(helper)
    try:
        websocket = await client.websocket(
            NativeWebSocketRequest(
                url="wss://example.test/responses",
                headers={"user-agent": "codex-cli", "sec-websocket-protocol": "openai"},
                connect_timeout_seconds=2,
                max_message_bytes=1024,
            )
        )
        assert isinstance(websocket._events, native_egress_module._BoundedEventQueue)
        assert websocket._events.maxsize == 4096
        assert websocket._events._max_bytes == 32 * 1024 * 1024
        assert websocket._messages.maxsize == 64
        # Await each acknowledged frame, without consuming downstream messages.
        for _ in range(64):
            await websocket.send_text("x")
        with pytest.raises(NativeEgressTransportError) as exc_info:
            await asyncio.wait_for(websocket.send_text("overflow"), timeout=2)
        assert exc_info.value.failure_phase == "consumer_backpressure"
        await asyncio.wait_for(websocket._pump_task, timeout=2)
        assert websocket._completed
        assert websocket._request_id not in client._streams
        assert websocket._events.queued_bytes == 0
    finally:
        await asyncio.wait_for(client.aclose(), timeout=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("data", [None, "%%%"])
async def test_malformed_chunk_releases_charge_and_cancels_owner(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch, data: str | None
) -> None:
    observed = _observe_helper_events(monkeypatch)
    helper = tmp_path / "native-helper"
    _write_helper(
        helper,
        f"""#!/usr/bin/env python3
for line in sys.stdin:
    command = json.loads(line)
    request_id = command["request_id"]
    if command["type"] == "cancel":
        print(json.dumps({{"type": "cancelled", "request_id": request_id}}), flush=True)
        continue
    print(json.dumps({{
        "type": "head", "request_id": request_id, "status": 200,
        "http_version": "HTTP/2.0", "headers": [],
    }}), flush=True)
    print(json.dumps({{"type": "chunk", "request_id": request_id, "data": {data!r}}}), flush=True)
""",
    )
    client = SubprocessNativeEgressClient(helper)
    try:
        response = await client.request(NativeEgressRequest(method="GET", url="https://example.test/bad", headers={}))
        with pytest.raises(NativeEgressProtocolError):
            await asyncio.wait_for(response.read(), timeout=2)
        cancelled = await _wait_for_helper_event(observed, "cancelled")
        assert cancelled["request_id"] == response._request_id
        assert response._request_id not in client._streams
        assert isinstance(response._events, native_egress_module._BoundedEventQueue)
        assert response._events.queued_bytes == 0
    finally:
        await asyncio.wait_for(client.aclose(), timeout=2)


@pytest.mark.asyncio
@pytest.mark.parametrize("frame", [b"not-json\n", b"[]\n", b"\xff\n"])
async def test_helper_rejects_malformed_bounded_frames(frame: bytes) -> None:
    reader = asyncio.StreamReader(limit=native_egress_module._NATIVE_EVENT_LINE_LIMIT)
    reader.feed_data(frame)
    with pytest.raises(NativeEgressProtocolError):
        await native_egress_module._read_event(reader)


@pytest.mark.asyncio
async def test_helper_line_limit_is_unchanged_and_enforced() -> None:
    assert native_egress_module._NATIVE_EVENT_LINE_LIMIT == 24 * 1024 * 1024
    reader = asyncio.StreamReader(limit=native_egress_module._NATIVE_EVENT_LINE_LIMIT)
    reader.feed_data(b"x" * (native_egress_module._NATIVE_EVENT_LINE_LIMIT + 1))
    with pytest.raises(ValueError):
        await native_egress_module._read_event(reader)
