import json
from dataclasses import asdict

import pytest

from app.core.openai.chat_responses import iter_chat_chunks
from scripts.qa.fast_benchmark import ChatSSEParser, SSEParser, Usage


def test_crlf_split_does_not_terminate_multiline_event():
    parser = SSEParser()
    parser.feed(b'data: {"type":"response.output_text.delta",\r', 1.0)
    parser.feed(b'\ndata: "delta":"hello"}\r\n\r\n', 2.0)
    assert parser.response.first_text_timestamp == 2.0


def chat_frame(**values: object) -> bytes:
    return b"data: " + json.dumps(values, ensure_ascii=False).encode() + b"\r\n\r\n"


def chat_wire() -> bytes:
    return (
        chat_frame(choices=[{"index": 0, "delta": {"content": "café 한글"}}])
        + chat_frame(choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}])
        + chat_frame(
            choices=[],
            usage={
                "prompt_tokens": 100,
                "completion_tokens": 500,
                "completion_tokens_details": {"reasoning_tokens": 100},
                "prompt_tokens_details": {"cached_tokens": 50},
            },
        )
        + b"data: [DONE]\r\n\r\n"
    )


@pytest.mark.parametrize("split", range(len(chat_wire()) + 1))
def test_chat_finish_then_usage_is_successful(split: int) -> None:
    parser = ChatSSEParser()
    parser.feed(chat_wire()[:split], 4.0)
    parser.feed(chat_wire()[split:], 4.0)
    result = parser.finish()
    assert result.first_text_timestamp == result.last_text_timestamp == 4.0
    assert result.terminal_timestamp == 4.0
    assert result.terminal_type == "chat.stop"
    assert parser.done
    assert result.usage == Usage(100, 500, 100, 50)
    assert result.errors == []
    assert "café" not in json.dumps(asdict(result), ensure_ascii=False)


@pytest.mark.parametrize("newline", [b"\n", b"\r", b"\r\n"])
def test_chat_bytewise_framing(newline: bytes) -> None:
    parser = ChatSSEParser()
    for byte in chat_wire().replace(b"\r\n", newline):
        parser.feed(bytes([byte]), 4.0)
    assert parser.finish().errors == []


def test_chat_timing_excludes_reasoning_refusal_and_tools() -> None:
    parser = ChatSSEParser()
    deltas = [
        {"role": "assistant", "content": ""},
        {"reasoning_content": "secret", "refusal": "secret", "tool_calls": []},
        {"content": "first"},
        {"content": "last"},
    ]
    for timestamp, delta in enumerate(deltas):
        parser.feed(chat_frame(choices=[{"index": 0, "delta": delta}]), float(timestamp))
    parser.feed(chat_wire()[chat_wire().index(b"data: ", 6) :], 5.0)
    result = parser.finish()
    assert (result.first_text_timestamp, result.last_text_timestamp, result.terminal_timestamp) == (2.0, 3.0, 5.0)
    assert result.visible_delta_count == 2
    assert result.errors == []
    assert "secret" not in json.dumps(asdict(result))


@pytest.mark.parametrize("reason", ["length", "tool_calls", "function_call", "content_filter", "secret"])
def test_chat_unsuccessful_finishes_are_sanitized(reason: str) -> None:
    parser = ChatSSEParser()
    parser.feed(chat_frame(choices=[{"index": 0, "delta": {}, "finish_reason": reason}]) + b"data: [DONE]\n\n", 1.0)
    result = parser.finish()
    safe_reason = "other" if reason == "secret" else reason
    assert result.terminal_type == "chat." + safe_reason
    assert "chat_" + safe_reason in result.errors
    assert "secret" not in json.dumps(asdict(result))


@pytest.mark.parametrize(
    "wire, error",
    [
        (b"data: [DONE]\n\n", "done_without_chat_finish"),
        (chat_wire().removesuffix(b"data: [DONE]\r\n\r\n"), "missing_chat_done"),
        (chat_frame(error={"code": "server_error", "message": "secret"}) + b"data: [DONE]\n\n", "chat_error"),
        (b"data: []\n\n", "invalid_event_shape"),
        (b"data: {bad}\n\n", "invalid_sse_json"),
    ],
)
def test_chat_truncation_and_errors(wire: bytes, error: str) -> None:
    parser = ChatSSEParser()
    parser.feed(wire, 1.0)
    result = parser.finish()
    assert error in result.errors
    assert "secret" not in json.dumps(asdict(result))


def test_chat_missing_usage_is_nullable() -> None:
    parser = ChatSSEParser()
    parser.feed(chat_frame(choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}]) + b"data: [DONE]\n\n", 1.0)
    result = parser.finish()
    assert result.usage == Usage()
    assert "missing_terminal_reasoning_tokens" in result.measurement_qualifications


def test_chat_consumes_actual_b_serialization() -> None:
    lines = [
        'data: {"type":"response.output_text.delta","delta":"visible"}\n\n',
        'data: {"type":"response.completed","response":{"usage":{"input_tokens":100,"output_tokens":500,'
        '"output_tokens_details":{"reasoning_tokens":100},"input_tokens_details":{"cached_tokens":50}}}}\n\n',
    ]
    parser = ChatSSEParser()
    for timestamp, frame in enumerate(iter_chat_chunks(lines, "test-model", created=1, include_usage=True)):
        parser.feed(frame.encode(), float(timestamp))
    result = parser.finish()
    assert result.errors == []
    assert result.usage == Usage(100, 500, 100, 50)
    assert result.terminal_type == "chat.stop"
    assert result.first_text_timestamp == 0.0
    assert result.terminal_timestamp == 1.0
