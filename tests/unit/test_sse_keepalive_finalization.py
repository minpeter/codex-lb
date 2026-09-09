from __future__ import annotations

import asyncio
import gc
from collections.abc import AsyncIterator

import pytest
from aiohttp import ClientSession, web
from aiohttp.test_utils import TestServer

from app.core.utils import sse


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [None, RuntimeError, asyncio.CancelledError])
async def test_close_observes_read_finished_after_heartbeat(
    monkeypatch: pytest.MonkeyPatch, failure: type[Exception] | type[asyncio.CancelledError] | None
) -> None:
    release = asyncio.Event()
    closed = asyncio.Event()
    loop = asyncio.get_running_loop()
    finished = loop.create_future()
    unhandled = []
    previous_handler = loop.get_exception_handler()
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    real_wait = sse.wait_on_shared_future
    first = True

    async def timeout_first_read(task, **kwargs):
        nonlocal first
        if first:
            first = False
            task.add_done_callback(lambda _task: finished.set_result(None))
            # Zero timeout deterministically expires this waiter. The actual
            # shared-future fanout stays installed, as after a real heartbeat.
            return await real_wait(task, timeout=0)
        return await real_wait(task, **kwargs)

    async def source() -> AsyncIterator[str]:
        try:
            await release.wait()
            if failure is not None:
                raise failure("upstream ended")
            for chunk in ():
                yield chunk
        finally:
            closed.set()

    monkeypatch.setattr(sse, "wait_on_shared_future", timeout_first_read)
    try:
        async with asyncio.timeout(5):
            stream = sse.inject_sse_keepalives(source(), 60)
            assert await anext(stream) == sse.SSE_KEEPALIVE_FRAME
            release.set()
            await finished
            await stream.aclose()
            assert closed.is_set()
            del stream
            gc.collect()
            assert unhandled == []
    finally:
        loop.set_exception_handler(previous_handler)


@pytest.mark.asyncio
async def test_normal_consumption_still_propagates_upstream_failure():
    async def source() -> AsyncIterator[str]:
        yield "data: first\n\n"
        raise RuntimeError("upstream failed")

    stream = sse.inject_sse_keepalives(source(), 60)
    async with asyncio.timeout(5):
        assert await anext(stream) == "data: first\n\n"
        with pytest.raises(RuntimeError, match="upstream failed"):
            await anext(stream)


@pytest.mark.asyncio
async def test_http_heartbeat_then_completed_read_closes_without_unhandled_task(monkeypatch: pytest.MonkeyPatch):
    loop = asyncio.get_running_loop()
    release = asyncio.Event()
    read_done = loop.create_future()
    handler_done = loop.create_future()
    unhandled = []
    previous_handler = loop.get_exception_handler()
    real_wait = sse.wait_on_shared_future

    async def heartbeat_wait(task, **kwargs):
        task.add_done_callback(lambda _task: read_done.set_result(None))
        return await real_wait(task, timeout=0)

    async def source() -> AsyncIterator[str]:
        await release.wait()
        for chunk in ():
            yield chunk

    async def handler(request: web.Request) -> web.StreamResponse:
        response = web.StreamResponse(headers={"Content-Type": "text/event-stream"})
        await response.prepare(request)
        stream = sse.inject_sse_keepalives(source(), 60)
        try:
            await response.write((await anext(stream)).encode())
            await read_done
        finally:
            await stream.aclose()
            del stream
            gc.collect()
        await response.write_eof()
        handler_done.set_result(None)
        return response

    monkeypatch.setattr(sse, "wait_on_shared_future", heartbeat_wait)
    loop.set_exception_handler(lambda _loop, context: unhandled.append(context))
    app = web.Application()
    app.router.add_get("/events", handler)
    try:
        async with asyncio.timeout(5), TestServer(app) as server, ClientSession() as client:
            async with client.get(server.make_url("/events")) as response:
                assert response.status == 200
                assert response.content_type == "text/event-stream"
                assert await response.content.readexactly(len(sse.SSE_KEEPALIVE_FRAME)) == (
                    sse.SSE_KEEPALIVE_FRAME.encode()
                )
                release.set()
                assert await response.read() == b""
            await handler_done
            assert unhandled == []
    finally:
        loop.set_exception_handler(previous_handler)
