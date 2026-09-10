from __future__ import annotations

import asyncio
import json
import os
import sys
from dataclasses import asdict
from pathlib import Path
from unittest.mock import AsyncMock, MagicMock

import aiohttp
import pytest

from app.core.openai.chat_requests import ChatCompletionsRequest
from app.core.openai.requests import ResponsesRequest
from scripts.qa.fast_benchmark import (
    METRICS,
    MODELS,
    PROMPT,
    Config,
    ParsedResponse,
    SSEParser,
    Surface,
    Trial,
    Usage,
    bootstrap_interval,
    load_config,
    main,
    metrics,
    request,
    request_payload,
    run,
    schedule,
    summarize,
    surface_url,
)


@pytest.mark.asyncio
async def test_load_config_accepts_configured_model(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "base_url": "https://example.test/v1",
                "models": ["gpt-5.6-sol"],
                "keys": {"gpt-5.6-sol": "dummy"},
                "reasoning_effort": {"gpt-5.6-sol": "low"},
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    config = load_config(path)
    assert config.models == ("gpt-5.6-sol",)
    trials = schedule(2, 10, config.models)
    assert len(trials) == 20
    assert sum(t.fast for t in trials) == 10
    assert sum(t.fast for t in trials[::2]) == 5
    assert {t.model for t in trials} == {"gpt-5.6-sol"}

    async def fake_request(session, cfg, trial, timeout, surface):
        payload = request_payload(cfg, trial, surface)
        assert payload["model"] == "gpt-5.6-sol"
        assert payload["reasoning"] == {"effort": "low"}
        assert payload.get("service_tier") == ("priority" if trial.fast else None)
        return sample(trial.model, trial.round, trial.fast, 1.0)

    monkeypatch.setattr("scripts.qa.fast_benchmark.request", fake_request)
    result = await run(config, 10, 2, 5.0, journal=tmp_path / "run.jsonl")
    assert len(result["samples"]) == 20
    assert set(result["summary"]["per_model"]) == {"gpt-5.6-sol"}
    assert result["summary"]["per_model"]["gpt-5.6-sol"]["paired"]["e2e_seconds"]["n"] == 10


@pytest.mark.asyncio
async def test_load_config_propagates_multiple_custom_models(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "base_url": "https://example.test/v1",
                "models": ["custom-low", "custom-minimal"],
                "keys": {
                    "custom-low": "dummy-low",
                    "custom-minimal": "dummy-minimal",
                    "ignored-model": "dummy-ignored",
                },
                "reasoning_effort": {
                    "custom-minimal": "minimal",
                    "ignored-model": "minimal",
                },
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        ),
        encoding="utf-8",
    )
    path.chmod(0o600)
    config = load_config(path)
    assert config.models == ("custom-low", "custom-minimal")
    assert config.keys == {
        "custom-low": "dummy-low",
        "custom-minimal": "dummy-minimal",
    }
    assert config.reasoning_effort == {
        "custom-low": "low",
        "custom-minimal": "minimal",
    }

    seen_trials = []
    seen_payloads = []

    async def fake_request(session, cfg, trial, timeout, surface):
        seen_trials.append(trial)
        seen_payloads.append(request_payload(cfg, trial, surface))
        return {
            **sample(trial.model, trial.round, trial.fast, 1.0),
            "pair_id": trial.pair_id,
        }

    monkeypatch.setattr("scripts.qa.fast_benchmark.request", fake_request)
    journal = tmp_path / "run.jsonl"
    result = await run(config, 4, 23, 5.0, journal=journal)

    assert len(seen_trials) == 16
    assert {trial.model for trial in seen_trials} == {"custom-low", "custom-minimal"}
    for first, second in zip(seen_trials[::2], seen_trials[1::2], strict=True):
        assert first.model == second.model
        assert first.pair_id == second.pair_id
        assert first.fast is not second.fast
    for model in config.models:
        starts = [
            seen_trials[index].fast for index in range(0, len(seen_trials), 2) if seen_trials[index].model == model
        ]
        assert sum(starts) == 2

    assert {(payload["model"], payload["reasoning"]["effort"]) for payload in seen_payloads} == {
        ("custom-low", "low"),
        ("custom-minimal", "minimal"),
    }
    assert sum("service_tier" in payload for payload in seen_payloads) == 8
    assert set(result["summary"]["per_model"]) == {"custom-low", "custom-minimal"}
    for model in config.models:
        model_summary = result["summary"]["per_model"][model]
        assert model_summary["attempts"] == 8
        assert model_summary["paired"]["e2e_seconds"]["n"] == 4
    assert [json.loads(line) for line in journal.read_text().splitlines()] == [
        {**row, "seed": 23} for row in result["samples"]
    ]


@pytest.mark.parametrize(
    ("models", "keys"),
    [
        (None, dict.fromkeys(MODELS, "dummy")),
        ([], {}),
        ("sol", {"sol": "dummy"}),
        ([""], {"": "dummy"}),
        (["sol sol"], {"sol sol": "dummy"}),
        (["sol", "sol"], {"sol": "dummy"}),
    ],
)
def test_load_config_rejects_invalid_model_names(tmp_path: Path, models: object, keys: dict[str, str]) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "base_url": "https://example.test/v1",
                "models": models,
                "keys": keys,
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        )
    )
    path.chmod(0o600)
    with pytest.raises(ValueError, match="models must"):
        load_config(path)


@pytest.mark.parametrize("models", [[2], [{}]])
def test_load_config_rejects_non_string_model_names(tmp_path: Path, models: list[object]) -> None:
    path = tmp_path / "config.json"
    path.write_text(
        json.dumps(
            {
                "base_url": "https://example.test/v1",
                "models": models,
                "keys": dict.fromkeys(MODELS, "dummy"),
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        )
    )
    path.chmod(0o600)
    with pytest.raises(ValueError, match="models must"):
        load_config(path)


def event(kind: str, **values: object) -> bytes:
    return b"data: " + json.dumps({"type": kind, **values}, ensure_ascii=False).encode() + b"\r\n\r\n"


def usage() -> dict[str, object]:
    return {
        "input_tokens": 100,
        "output_tokens": 500,
        "output_tokens_details": {"reasoning_tokens": 100},
        "input_tokens_details": {"cached_tokens": 50},
    }


def terminal(kind: str = "response.completed") -> bytes:
    return event(kind, response={"usage": usage(), "service_tier": "priority"})


def wire() -> bytes:
    return (
        b"\xef\xbb\xbf: heartbeat\r\n\r\nevent: response.created\r\n"
        b'data: {"type":"response.created",\r\n'
        b'data: "response":{"service_tier":"default"}}\r\n\r\n'
        + event("response.reasoning_text.delta", delta="do not retain")
        + event("response.output_text.delta", delta="café 한글")
        + terminal()
    )


@pytest.mark.parametrize("split", range(len(wire()) + 1))
def test_every_byte_boundary_utf8_crlf_and_multiline(split: int) -> None:
    parser = SSEParser()
    parser.feed(wire()[:split], 4.0)
    parser.feed(wire()[split:], 4.0)
    result = parser.finish()
    assert result.errors == []
    assert result.usage == Usage(100, 500, 100, 50)
    assert result.visible_delta_count == 1
    assert result.first_text_timestamp == result.last_text_timestamp == 4.0
    assert result.terminal_timestamp == 4.0
    assert [(e.index, e.event, e.service_tier) for e in result.event_tiers] == [
        (0, "response.created", "default"),
        (1, "response.reasoning_text.delta", None),
        (2, "response.output_text.delta", None),
        (3, "response.completed", "priority"),
    ]
    assert "delta" not in asdict(result)
    assert "café" not in json.dumps(asdict(result))


def test_split_crlf_is_not_an_extra_blank_line() -> None:
    parser = SSEParser()
    parser.feed(b'data: {"type":"response.output_text.delta",\r', 0.0)
    parser.feed(b'\ndata: "delta":"visible"}\r', 1.0)
    assert parser.response.first_text_timestamp is None
    parser.feed(b"\n\r", 2.0)
    assert parser.response.first_text_timestamp == 2.0
    parser.feed(b"\n" + terminal(), 3.0)
    assert parser.finish().errors == []


@pytest.mark.parametrize("newline", [b"\n", b"\r", b"\r\n"])
def test_byte_at_a_time_and_all_line_endings(newline: bytes) -> None:
    parser = SSEParser()
    data = wire().replace(b"\r\n", newline)
    for byte in data:
        parser.feed(bytes([byte]), 0.0)
    assert parser.finish().errors == []
    assert parser.response.first_text_timestamp == 0.0


def test_only_nonempty_visible_deltas_set_timing_and_zero_is_preserved() -> None:
    parser = SSEParser()
    parser.feed(event("response.reasoning_text.delta", delta="secret"), -2.0)
    parser.feed(event("response.output_text.delta", delta=""), -1.0)
    assert parser.response.first_text_timestamp is None
    parser.feed(event("response.output_text.delta", delta="one"), 0.0)
    parser.feed(event("response.output_text.done", text="one"), 1.0)
    parser.feed(event("response.function_call_arguments.delta", delta="{}"), 2.0)
    parser.feed(event("response.output_text.delta", delta="two"), 3.0)
    parser.feed(terminal(), 4.0)
    parser.feed(b"data: [DONE]\r\n\r\n", 5.0)
    result = parser.finish()
    assert (
        result.first_text_timestamp,
        result.last_text_timestamp,
        result.terminal_timestamp,
    ) == (0.0, 3.0, 4.0)
    assert result.visible_delta_count == 2
    assert result.errors == []


@pytest.mark.parametrize("kind", ["response.failed", "response.incomplete", "error"])
def test_failure_terminals_are_failures_and_never_leak_messages(kind: str) -> None:
    parser = SSEParser()
    details = {
        "code": "server_error",
        "message": "secret response text https://secret.invalid",
    }
    data = event(kind, **details) if kind == "error" else event(kind, response={"error": details, "usage": usage()})
    parser.feed(data, 2.0)
    result = parser.finish()
    assert result.terminal_type == kind
    assert result.terminal_timestamp == 2.0
    assert kind.replace(".", "_") in result.errors
    assert result.terminal_error_code == "server_error"
    assert "secret" not in json.dumps(asdict(result))


@pytest.mark.parametrize("data", [event("tool.completed"), b"data: [DONE]\n\n"])
def test_non_responses_terminals_do_not_succeed(data: bytes) -> None:
    parser = SSEParser()
    parser.feed(data, 1.0)
    result = parser.finish()
    assert result.terminal_type is None
    assert "missing_response_terminal" in result.errors


@pytest.mark.parametrize("raw", [b"[]", b"null", b"123", b'{"type":[]}', b'"secret"', b"{invalid"])
def test_invalid_json_shapes_are_reported_without_echo(raw: bytes) -> None:
    parser = SSEParser()
    parser.feed(b"data: " + raw + b"\n\n", 0.0)
    result = parser.finish()
    assert any(e in result.errors for e in ("invalid_event_shape", "invalid_sse_json"))
    assert "secret" not in json.dumps(asdict(result))


def test_completed_response_missing_optional_accounting_is_successful() -> None:
    parser = SSEParser()
    parser.feed(event("response.output_text.delta", delta="visible"), 0.0)
    parser.feed(
        event(
            "response.completed",
            response={
                "usage": {
                    "input_tokens": 100,
                    "output_tokens": 500,
                    "output_tokens_details": {"reasoning_tokens": 100},
                }
            },
        ),
        1.0,
    )
    result = parser.finish()
    assert result.errors == []
    assert result.measurement_qualifications == ["missing_terminal_cached_tokens"]
    measured = metrics(result, 0.0, 2.0)
    assert measured["e2e_seconds"] == 2.0
    assert measured["ttfo_seconds"] == 0.0
    assert measured["visible_tokens_per_e2e_second"] == 200.0
    assert measured["total_output_tokens_per_terminal_second"] == 500.0


def test_missing_required_tps_accounting_is_qualified_not_failed() -> None:
    parser = SSEParser()
    parser.feed(event("response.output_text.delta", delta="visible"), 0.0)
    parser.feed(event("response.completed", response={"usage": {"input_tokens": 100}}), 1.0)
    result = parser.finish()
    assert result.errors == []
    assert "missing_terminal_output_tokens" in result.measurement_qualifications
    measured = metrics(result, 0.0, 2.0)
    assert measured["e2e_seconds"] == 2.0
    assert measured["visible_tokens_per_e2e_second"] is None
    assert measured["visible_tokens"] is None


@pytest.mark.parametrize(
    "payload",
    [
        {"type": "error", "error": {"code": "server_is_overloaded", "message": "secret"}},
        {"error": {"code": "server_is_overloaded", "message": "secret"}},
        {
            "type": "error",
            "response": {"error": {"code": "server_is_overloaded", "message": "secret"}},
        },
    ],
)
def test_responses_error_code_is_preserved_from_supported_envelopes(payload: dict[str, object]) -> None:
    parser = SSEParser()
    parser.feed(b"data: " + json.dumps(payload).encode() + b"\n\n", 1.0)
    result = parser.finish()
    assert result.terminal_type == "error"
    assert result.terminal_error_code == "server_is_overloaded"
    assert "error" in result.errors
    assert "secret" not in json.dumps(asdict(result))


def test_missing_usage_unknown_reasoning_and_unterminated_event() -> None:
    parser = SSEParser()
    parser.feed(event("response.completed", response={}), 1.0)
    result = parser.finish()
    assert "missing_terminal_output_tokens" in result.measurement_qualifications
    assert metrics(result, 0.0, 2.0)["visible_tokens"] is None
    parser = SSEParser()
    parser.feed(terminal().rstrip(b"\r\n"), 1.0)
    assert "missing_response_terminal" in parser.finish().errors
    parser = SSEParser()
    parser.feed(b"data: \xc3", 0.0)
    assert "invalid_utf8" in parser.finish().errors


def test_metrics_exact_formulas_and_distinct_denominators() -> None:
    parsed = ParsedResponse(
        first_text_timestamp=12.0,
        last_text_timestamp=16.0,
        terminal_timestamp=18.0,
        terminal_type="response.completed",
        usage=Usage(100, 500, 100, 50),
    )
    result = metrics(parsed, 10.0, 20.0)
    assert result == {
        "e2e_seconds": 10.0,
        "ttfo_seconds": 2.0,
        "last_visible_seconds": 6.0,
        "terminal_seconds": 8.0,
        "output_span_seconds": 4.0,
        "visible_tokens": 400,
        "visible_tokens_per_e2e_second": 40.0,
        "approx_visible_tokens_per_output_second": 100.0,
        "approx_output_rate_unavailable_reason": None,
        "total_output_tokens_per_terminal_second": 62.5,
    }


@pytest.mark.parametrize("span", [0.0, -1.0])
def test_invalid_span_is_null_with_reason(span: float) -> None:
    parsed = ParsedResponse(first_text_timestamp=0.0, last_text_timestamp=span, usage=Usage(1, 20, 0, 0))
    result = metrics(parsed, 0.0, 2.0)
    assert result["ttfo_seconds"] == 0.0
    assert result["approx_visible_tokens_per_output_second"] is None
    assert result["approx_output_rate_unavailable_reason"] == "non_positive_output_span"
    parsed.usage.reasoning_tokens = None
    assert metrics(parsed, 0.0, 2.0)["visible_tokens"] is None


def test_balanced_reproducible_adjacent_pairs_per_model() -> None:
    rows = schedule(17, 6)
    assert rows == schedule(17, 6)
    assert len(rows) == 36
    for i in range(0, len(rows), 2):
        first, second = rows[i : i + 2]
        assert first.model == second.model
        assert first.pair_id == second.pair_id
        assert first.fast is not second.fast
    for model in MODELS:
        starts = [rows[i].fast for i in range(0, len(rows), 2) if rows[i].model == model]
        assert sum(starts) == 3
        assert all(a != b for a, b in zip(starts, starts[1:]))
    assert len({row.pair_id for row in rows}) == 18
    with pytest.raises(ValueError):
        schedule(17, 3)


def sample(model: str, round_id: int, fast: bool, value: float) -> dict:
    return {
        "model": model,
        "round": round_id,
        "pair_id": f"pair-{round_id}",
        "fast": fast,
        "errors": [],
        **dict.fromkeys(METRICS, value),
    }


def test_repeats_are_not_discarded_and_intervals_are_per_model() -> None:
    rows = []
    for model_index, model in enumerate(MODELS):
        for round_id in range(4):
            standard = 10.0 + round_id * 10
            rows.extend(
                [
                    sample(model, round_id, False, standard),
                    sample(model, round_id, True, standard - model_index - 1),
                ]
            )
    result = summarize(rows, seed=19)
    assert result == summarize(rows, seed=19)
    for model_index, model in enumerate(MODELS):
        data = result["per_model"][model]
        assert data["standard"]["metrics"]["e2e_seconds"]["median"] == 25.0
        assert data["standard"]["metrics"]["visible_tokens_per_e2e_second"]["median"] == 25.0
        paired = data["paired"]["e2e_seconds"]
        assert paired["n"] == 4
        assert paired["priority_wins"] == 4
        assert len({d["pair_id"] for d in paired["deltas"]}) == 4
        assert paired["mean_delta_bootstrap_95pct"] == [-model_index - 1.0] * 2
    rows[1]["errors"] = ["response_failed"]
    assert summarize(rows, 19)["per_model"][MODELS[0]]["paired"]["e2e_seconds"]["excluded_pairs"] == 1


def test_bootstrap_nonconstant_seeded_and_small_sample_undefined() -> None:
    interval = bootstrap_interval([-5.0, 1.0, 9.0], 88)
    assert interval == bootstrap_interval([-5.0, 1.0, 9.0], 88)
    assert interval is not None and interval[0] <= 1.0 <= interval[1]
    assert bootstrap_interval([1.0], 88) is None


def config() -> Config:
    return Config(
        "https://invalid.example/v1",
        dict.fromkeys(MODELS, "dummy-test-key"),
        dict.fromkeys(MODELS, "low"),
    )


def test_payload_standard_omits_tier_and_effort_is_model_specific() -> None:
    cfg = config()
    cfg.reasoning_effort[MODELS[1]] = "minimal"
    assert "service_tier" not in request_payload(cfg, Trial(0, "p", MODELS[0], False))
    payload = request_payload(cfg, Trial(0, "p", MODELS[1], True))
    assert payload["service_tier"] == "priority"
    assert payload["reasoning"] == {"effort": "minimal"}


def test_chat_payload_uses_b_schema() -> None:
    payload = request_payload(config(), Trial(0, "p", MODELS[0], False), "chat")
    assert payload.get("messages") == [{"role": "user", "content": PROMPT}]
    assert payload.get("reasoning_effort") == "low"
    assert payload.get("max_completion_tokens") == 1600
    assert payload.get("stream_options") == {"include_usage": True}
    assert "input" not in payload


@pytest.mark.parametrize("surface", ["responses", "chat", "codex"])
@pytest.mark.parametrize("model", MODELS)
def test_surface_payloads_match_arms_and_b_schema(surface: Surface, model: str) -> None:
    cfg = config()
    cfg.reasoning_effort[model] = "minimal"
    standard = request_payload(cfg, Trial(0, "p", model, False), surface)
    priority = request_payload(cfg, Trial(0, "p", model, True), surface)
    assert priority == {**standard, "service_tier": "priority"}
    assert "service_tier" not in standard
    assert standard["store"] is False
    if surface == "chat":
        schema = ChatCompletionsRequest.model_validate(standard)
        assert schema.max_completion_tokens == 1600
        assert schema.stream_options and schema.stream_options.include_usage
        normalized = schema.to_responses_request()
    else:
        normalized = ResponsesRequest.model_validate({"instructions": "", **standard})
        assert standard["max_output_tokens"] == 1600
    assert normalized.reasoning and normalized.reasoning.effort == "minimal"
    assert normalized.instructions == ""
    assert normalized.input == [{"role": "user", "content": [{"type": "input_text", "text": PROMPT}]}]
    assert normalized.model == model


@pytest.mark.parametrize("base", ["https://b.invalid/v1", "http://b.invalid:8080/v1"])
def test_surface_urls_stay_on_b(base: str) -> None:
    cfg = Config(base, config().keys, config().reasoning_effort)
    assert surface_url(cfg, "responses") == base + "/responses"
    assert surface_url(cfg, "chat") == base + "/chat/completions"
    assert surface_url(cfg, "codex") == base.removesuffix("/v1") + "/backend-api/codex/responses"


def test_omitted_models_preserve_config_schedule_and_summary_defaults(tmp_path: Path) -> None:
    path = tmp_path / "defaults.json"
    path.write_text(
        json.dumps(
            {
                "base_url": "https://example.test/v1",
                "keys": dict.fromkeys(MODELS, "dummy"),
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        )
    )
    path.chmod(0o600)
    loaded = load_config(path)
    assert loaded.models == config().models == ("gpt-6-astra", "gpt-5.6-luna", "gpt-5.6-terra")
    assert loaded.keys == dict.fromkeys(MODELS, "dummy")
    assert loaded.reasoning_effort == dict.fromkeys(MODELS, "low")
    assert loaded.headers == {}
    headers = {"originator": "synthetic-client"}
    positional = Config(loaded.base_url, loaded.keys, loaded.reasoning_effort, headers)
    assert positional.headers == headers
    assert positional.models == MODELS
    trials = schedule(17, 10)
    assert trials == schedule(17, 10, loaded.models)
    assert len(trials) == 60
    rows = [sample(trial.model, trial.round, trial.fast, 1.0) for trial in trials]
    assert summarize(rows, 17) == summarize(rows, 17, loaded.models)
    assert tuple(summarize([], 17)["per_model"]) == MODELS


def test_default_payload_is_unchanged() -> None:
    assert request_payload(config(), Trial(0, "p", MODELS[0], False)) == {
        "model": MODELS[0],
        "input": PROMPT,
        "stream": True,
        "store": False,
        "reasoning": {"effort": "low"},
        "max_output_tokens": 1600,
    }


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["responses", "chat", "codex"])
@pytest.mark.parametrize("fast", [False, True])
async def test_optional_provider_headers_reach_all_arms(tmp_path: Path, surface: Surface, fast: bool) -> None:
    headers = {"User-Agent": "test-agent/1", "originator": "test-agent", "version": "1"}
    path = tmp_path / "headers.json"
    path.write_text(
        json.dumps(
            {
                "base_url": config().base_url,
                "keys": config().keys,
                "headers": headers,
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        )
    )
    path.chmod(0o600)
    cfg = load_config(path)
    context = AsyncMock()
    context.__aenter__.return_value = MagicMock(status=400, headers={})
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post.return_value = context
    await request(session, cfg, Trial(0, "p", MODELS[0], fast), 5.0, surface)
    sent = session.post.call_args.kwargs
    assert {name: sent["headers"].get(name) for name in headers} == headers
    assert sent["headers"]["Authorization"] == "Bearer " + cfg.keys[MODELS[0]]
    assert sent["headers"]["Accept"] == "text/event-stream"
    assert sent["json"].get("service_tier") == ("priority" if fast else None)


def test_protected_config_attestations(tmp_path: Path) -> None:
    duplicate_path = tmp_path / "duplicate.json"
    duplicate_path.write_text(
        json.dumps(
            {
                "base_url": config().base_url,
                "keys": config().keys,
                "headers": {"X-Test": "one", "x-test": "two"},
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
        )
    )
    duplicate_path.chmod(0o600)
    with pytest.raises(ValueError, match="headers"):
        load_config(duplicate_path)
    path = tmp_path / "config.json"
    value = {
        "base_url": "https://invalid.example/v1",
        "keys": dict.fromkeys(MODELS, "dummy"),
        "standard_tier_unenforced_verified": True,
        "single_account_per_model_verified": True,
    }
    path.write_text(json.dumps(value))
    path.chmod(0o600)
    assert load_config(path).reasoning_effort == dict.fromkeys(MODELS, "low")
    path.chmod(0o644)
    with pytest.raises(ValueError):
        load_config(path)
    path.chmod(0o600)
    value["standard_tier_unenforced_verified"] = False
    path.write_text(json.dumps(value))
    with pytest.raises(ValueError):
        load_config(path)


@pytest.mark.asyncio
async def test_request_drains_to_terminal_closes_context_and_reports_metrics(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    consumed = []

    async def chunks():
        for i, block in enumerate(
            [
                event("response.created", response={"service_tier": "default"}),
                event("response.output_text.delta", delta="first"),
                event("response.output_text.delta", delta="last"),
                terminal(),
            ]
        ):
            consumed.append(i)
            yield block
        raise AssertionError("must stop after Responses terminal, not wait for EOF")

    ticks = iter([10.0, 11.0, 12.0, 16.0, 18.0, 20.0])
    monkeypatch.setattr("scripts.qa.fast_benchmark.time", MagicMock(monotonic=lambda: next(ticks)))
    response = MagicMock(
        status=200,
        content_type="text/event-stream",
        headers={"x-request-id": "req-test"},
    )
    response.content.iter_any.return_value = chunks()
    context = AsyncMock()
    context.__aenter__.return_value = response
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post.return_value = context
    result = await request(session, config(), Trial(0, "p", MODELS[0], False), 5.0)
    assert consumed == [0, 1, 2, 3]
    context.__aexit__.assert_awaited_once()
    assert result["errors"] == []
    assert result["http_status"] == 200 and result["request_id"] == "req-test"
    assert result["input_tokens"] == 100
    assert result["visible_tokens_per_e2e_second"] == 40.0
    assert result["approx_visible_tokens_per_output_second"] == 100.0
    assert result["last_visible_seconds"] == 6.0
    assert result["total_output_tokens_per_terminal_second"] == 62.5
    assert "service_tier" not in session.post.call_args.kwargs["json"]
    assert session.post.call_args.kwargs["allow_redirects"] is False


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [aiohttp.ClientConnectionError, TimeoutError, OSError])
async def test_transport_errors_are_sanitized_and_context_closed(
    failure: type[Exception],
) -> None:
    context = AsyncMock()
    context.__aenter__.side_effect = failure("dummy-test-key https://private.invalid/user-content")
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post.return_value = context
    result = await request(session, config(), Trial(0, "p", MODELS[0], True), 5.0)
    assert result["errors"]
    assert "dummy-test-key" not in json.dumps(result)
    assert "private.invalid" not in json.dumps(result)
    # __aenter__ failed, so Python does not invoke __aexit__.
    context.__aexit__.assert_not_awaited()


@pytest.mark.asyncio
async def test_chat_request_drains_usage_and_stops_at_done(monkeypatch: pytest.MonkeyPatch) -> None:
    from tests.unit.test_fast_benchmark_framing import chat_frame

    consumed = []
    blocks = [
        chat_frame(choices=[{"index": 0, "delta": {"content": "first"}}], service_tier="priority"),
        chat_frame(choices=[{"index": 0, "delta": {"content": "last"}}]),
        chat_frame(choices=[{"index": 0, "delta": {}, "finish_reason": "stop"}]),
        chat_frame(
            choices=[],
            usage={
                "prompt_tokens": 100,
                "completion_tokens": 500,
                "completion_tokens_details": {"reasoning_tokens": 100},
                "prompt_tokens_details": {"cached_tokens": 50},
            },
        ),
        b"data: [DONE]\n\n",
    ]

    async def chunks():
        for i, block in enumerate(blocks):
            consumed.append(i)
            yield block
        raise AssertionError("must stop at DONE, not wait for EOF")

    ticks = iter([10.0, 12.0, 16.0, 18.0, 19.0, 19.5, 20.0])
    monkeypatch.setattr("scripts.qa.fast_benchmark.time", MagicMock(monotonic=lambda: next(ticks)))
    response = MagicMock(status=200, content_type="text/event-stream", headers={"x-request-id": "req-chat"})
    response.content.iter_any.return_value = chunks()
    context = AsyncMock()
    context.__aenter__.return_value = response
    session = MagicMock(spec=aiohttp.ClientSession)
    session.post.return_value = context
    result = await request(session, config(), Trial(0, "p", MODELS[0], True), 5.0, "chat")
    assert consumed == list(range(5))
    context.__aexit__.assert_awaited_once()
    assert result["errors"] == []
    assert result["surface"] == "chat" and result["route"] == "/v1/chat/completions"
    assert result["request_id"] == "req-chat"
    assert result["terminal_seconds"] == 8.0 and result["e2e_seconds"] == 10.0
    assert result["ttfo_seconds"] == 2.0 and result["last_visible_seconds"] == 6.0
    assert result["input_tokens"] == 100 and result["output_tokens"] == 500
    assert result["reasoning_tokens"] == 100 and result["cached_tokens"] == 50
    assert result["event_tiers"][0]["service_tier"] == "priority"
    assert session.post.call_args.args[0] == "https://invalid.example/v1/chat/completions"
    assert session.post.call_args.kwargs["allow_redirects"] is False
    assert "dummy-test-key" not in json.dumps(result)


@pytest.mark.asyncio
@pytest.mark.parametrize("surface", ["responses", "chat", "codex"])
async def test_campaign_preserves_surface_pair_model_grouping(
    surface: Surface, monkeypatch: pytest.MonkeyPatch
) -> None:
    async def fake_request(session, cfg, trial, timeout, selected_surface):
        assert selected_surface == surface
        return {**sample(trial.model, trial.round, trial.fast, 1.0), "surface": selected_surface}

    monkeypatch.setattr("scripts.qa.fast_benchmark.request", fake_request)
    result = await run(config(), 2, 17, 5.0, surface)
    assert result["surface"] == surface
    assert len(result["samples"]) == 12
    for model in MODELS:
        assert result["summary"]["per_model"][model]["paired"]["e2e_seconds"]["n"] == 2


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", [RuntimeError, asyncio.CancelledError])
async def test_journal_survives_later_failure(
    failure: type[BaseException], tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    journal = tmp_path / "campaign.jsonl"
    completed = []
    fsync = MagicMock(wraps=os.fsync)
    monkeypatch.setattr("scripts.qa.fast_benchmark.os.fsync", fsync)

    async def fake_request(session, cfg, trial, timeout, surface):
        if completed:
            assert [json.loads(line) for line in journal.read_text().splitlines()] == completed
            raise failure("interrupted")
        row = {
            **sample(trial.model, trial.round, trial.fast, 1.0),
            "surface": surface,
            "request_id": "req-first",
            "ttfo_seconds": 0.25,
            "seed": 17,
        }
        completed.append(row)
        return row

    monkeypatch.setattr("scripts.qa.fast_benchmark.request", fake_request)
    with pytest.raises(failure):
        await run(config(), 2, 17, 5.0, "chat", journal=journal)
    assert [json.loads(line) for line in journal.read_text().splitlines()] == completed
    fsync.assert_called_once()


@pytest.mark.asyncio
async def test_existing_journal_never_replays(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = tmp_path / "old.jsonl"
    journal.write_text('{"request_id":"old"}\n')
    requester = AsyncMock()
    monkeypatch.setattr("scripts.qa.fast_benchmark.request", requester)
    with pytest.raises(FileExistsError):
        await run(config(), 2, 17, 5.0, journal=journal)
    requester.assert_not_awaited()
    assert journal.read_text() == '{"request_id":"old"}\n'


@pytest.mark.asyncio
async def test_campaign_timeout_rejects_invalid_limits(tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    journal = tmp_path / "bounded.jsonl"
    requester = AsyncMock()
    monkeypatch.setattr("scripts.qa.fast_benchmark.request", requester)
    for invalid in [0.0, -1.0, float("inf"), float("nan")]:
        with pytest.raises(ValueError):
            await run(config(), 2, 17, 5.0, campaign_timeout=invalid)
    requester.assert_not_awaited()
    assert not journal.exists()


@pytest.mark.parametrize("explicit", [False, True])
def test_cli_journal_selection(explicit: bool, tmp_path: Path, monkeypatch: pytest.MonkeyPatch) -> None:
    output = tmp_path / "campaign.json"
    journal = tmp_path / "explicit.jsonl" if explicit else output.with_suffix(".jsonl")
    runner = AsyncMock(return_value={"samples": []})
    monkeypatch.setattr("scripts.qa.fast_benchmark.run", runner)
    monkeypatch.setattr("scripts.qa.fast_benchmark.load_config", lambda path: config())
    argv = ["fast_benchmark.py", "unused.json", "--output", str(output), "--rounds", "2", "--campaign-timeout", "900"]
    if explicit:
        argv += ["--journal", str(journal)]
    monkeypatch.setattr(sys, "argv", argv)
    assert main() == 0
    assert runner.call_args.kwargs == {"journal": journal, "campaign_timeout": 900.0}
    assert runner.call_args.args[1] == 2
    assert json.loads(output.read_text()) == {"samples": []}


@pytest.mark.parametrize("surface", [None, "responses", "chat", "codex"])
def test_cli_selects_surface(
    surface: str | None, monkeypatch: pytest.MonkeyPatch, capsys: pytest.CaptureFixture[str]
) -> None:
    runner = AsyncMock(return_value={"samples": []})
    monkeypatch.setattr("scripts.qa.fast_benchmark.run", runner)
    monkeypatch.setattr("scripts.qa.fast_benchmark.load_config", lambda path: config())
    monkeypatch.setattr(sys, "argv", ["fast_benchmark.py", "unused.json"] + (["--surface", surface] if surface else []))
    assert main() == 0
    assert runner.call_args.args[-1] == (surface or "responses")
    assert json.loads(capsys.readouterr().out) == {"samples": []}
