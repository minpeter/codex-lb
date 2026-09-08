#!/usr/bin/env python3
"""Sequential paired B HTTP benchmark; network access occurs only via the CLI.

Config is a mode-0600 JSON file with base_url (API prefix, e.g. /v1), keys
(model -> key), reasoning_effort (optional model -> low|minimal), and boolean
standard_tier_unenforced_verified and single_account_per_model_verified
attestations supplied by the lead. No provider/key-management calls are made.

Example invocation: python fast_benchmark.py /protected/config.json --rounds 4
The lead must verify effort support and account pinning before invoking it.
"""

from __future__ import annotations

import argparse
import asyncio
import codecs
import json
import math
import os
import random
import re
import stat
import statistics
import time
from contextlib import nullcontext
from dataclasses import asdict, dataclass, field
from pathlib import Path
from typing import Any, Literal
from urllib.parse import urlsplit

import aiohttp

MODELS = ("gpt-6-astra", "gpt-5.6-luna", "gpt-5.6-terra")
PROMPT = (
    "Write approximately 350 words explaining how a bicycle derailleur shifts gears. "
    "Use four short sections: mechanism, choosing a gear, a worked example of approaching "
    "a steep hill, and safe shifting and maintenance. Explain the chain, cassette, "
    "cable tension, and cadence in language a new rider can understand. Give concrete "
    "instructions and explain why each helps. Return only the explanation, without a "
    "preamble, word count, or follow-up question."
)
TERMINALS = {"response.completed", "response.failed", "response.incomplete", "error"}
EVENT_TYPES = TERMINALS | {
    "response.created",
    "response.queued",
    "response.in_progress",
    "response.output_item.added",
    "response.output_item.done",
    "response.content_part.added",
    "response.content_part.done",
    "response.output_text.delta",
    "response.output_text.done",
    "response.reasoning_text.delta",
    "response.reasoning_text.done",
    "response.reasoning_summary_text.delta",
    "response.reasoning_summary_text.done",
    "response.reasoning_summary_part.added",
    "response.reasoning_summary_part.done",
    "response.function_call_arguments.delta",
    "response.function_call_arguments.done",
    "response.refusal.delta",
    "response.refusal.done",
}
TIERS = {"auto", "default", "standard", "priority", "flex", "scale"}
ERROR_CODES = {
    "server_error",
    "rate_limit_exceeded",
    "invalid_request_error",
    "invalid_api_key",
    "insufficient_quota",
    "model_not_found",
    "content_filter",
    "max_output_tokens",
    "server_is_overloaded",
    "authentication_error",
    "permission_denied",
}
RATE_METRICS = {
    "visible_tokens_per_e2e_second",
    "approx_visible_tokens_per_output_second",
}
METRICS = (
    "e2e_seconds",
    "ttfo_seconds",
    "visible_tokens_per_e2e_second",
    "approx_visible_tokens_per_output_second",
    "total_output_tokens_per_terminal_second",
)
Effort = Literal["low", "minimal"]
Surface = Literal["responses", "chat", "codex"]


@dataclass(frozen=True)
class Config:
    base_url: str
    keys: dict[str, str] = field(repr=False)
    reasoning_effort: dict[str, Effort]


def load_config(path: Path) -> Config:
    # Read only the explicitly supplied protected file; never echo its contents/errors.
    with path.open(encoding="utf-8") as handle:
        mode = os.fstat(handle.fileno())
        if not stat.S_ISREG(mode.st_mode) or mode.st_mode & 0o077:
            raise ValueError("config must be a regular file with no group/other permissions")
        try:
            value = json.load(handle)
        except json.JSONDecodeError:
            raise ValueError("invalid config JSON") from None
    if not isinstance(value, dict):
        raise ValueError("config must be an object")
    if value.get("standard_tier_unenforced_verified") is not True:
        raise ValueError("lead must attest standard key tier is unenforced")
    if value.get("single_account_per_model_verified") is not True:
        raise ValueError("lead must attest account pinning per model")
    url = value.get("base_url")
    if not isinstance(url, str):
        raise ValueError("base_url must be an HTTP API prefix")
    try:
        parts = urlsplit(url)
        valid_url = (
            parts.scheme in {"http", "https"}
            and parts.hostname
            and not parts.username
            and not parts.password
            and not parts.query
            and not parts.fragment
        )
    except ValueError:
        valid_url = False
    if not valid_url:
        raise ValueError("base_url must be HTTP(S), without userinfo, query or fragment")
    keys = value.get("keys")
    efforts = value.get("reasoning_effort", {})
    if not isinstance(keys, dict) or not isinstance(efforts, dict):
        raise ValueError("keys and reasoning_effort must be model maps")
    resolved: dict[str, Effort] = {}
    for model in MODELS:
        key = keys.get(model)
        if not isinstance(key, str) or not key or any(char.isspace() for char in key):
            raise ValueError("each model requires a nonempty key without whitespace")
        effort = efforts.get(model, "low")
        if effort not in ("low", "minimal"):
            raise ValueError("reasoning effort must be low or minimal")
        resolved[model] = effort
    return Config(url.rstrip("/"), {model: keys[model] for model in MODELS}, resolved)


@dataclass
class Usage:
    input_tokens: int | None = None
    output_tokens: int | None = None
    reasoning_tokens: int | None = None
    cached_tokens: int | None = None


def token_count(value: object) -> int | None:
    return value if type(value) is int and value >= 0 else None


def parse_usage(value: object) -> Usage:
    if not isinstance(value, dict):
        return Usage()
    output = value.get("output_tokens_details")
    inputs = value.get("input_tokens_details")
    return Usage(
        token_count(value.get("input_tokens")),
        token_count(value.get("output_tokens")),
        token_count(output.get("reasoning_tokens")) if isinstance(output, dict) else None,
        token_count(inputs.get("cached_tokens")) if isinstance(inputs, dict) else None,
    )


@dataclass
class EventTier:
    index: int
    event: str
    timestamp: float
    service_tier: str | None


@dataclass
class ParsedResponse:
    first_text_timestamp: float | None = None
    last_text_timestamp: float | None = None
    terminal_timestamp: float | None = None
    terminal_type: str | None = None
    terminal_error_code: str | None = None
    usage: Usage = field(default_factory=Usage)
    event_tiers: list[EventTier] = field(default_factory=list)
    errors: list[str] = field(default_factory=list)
    measurement_qualifications: list[str] = field(default_factory=list)
    visible_delta_count: int = 0

    def error(self, code: str) -> None:
        if code not in self.errors:
            self.errors.append(code)


class SSEParser:
    """Incremental UTF-8 and SSE framing, including split CRLF and multiline data.

    Delta text is transient JSON input only: never retained in ParsedResponse.
    Incomplete events are discarded at EOF per SSE semantics. Bounds prevent an
    untrusted stream from retaining unlimited lines/events before the wall timeout.
    """

    def __init__(self) -> None:
        self.response = ParsedResponse()
        self._decoder = codecs.getincrementaldecoder("utf-8")("strict")
        self._line = ""
        self._data: list[str] = []
        self._size = 0
        self._after_cr = False
        self._first_character = True
        self._discard_event = False
        self._finished = False
        self._index = 0

    @property
    def done(self) -> bool:
        return self.response.terminal_type is not None

    def feed(self, chunk: bytes, timestamp: float) -> None:
        if self._finished or self.done:
            return
        try:
            text = self._decoder.decode(chunk)
        except UnicodeDecodeError:
            self.response.error("invalid_utf8")
            self._finished = True
            self._line = ""
            self._data.clear()
            return
        for char in text:
            if self._first_character:
                self._first_character = False
                if char == "\ufeff":
                    continue
            if self._after_cr:
                self._after_cr = False
                if char == "\n":
                    continue
            if char in "\r\n":
                self._consume_line(timestamp)
                self._after_cr = char == "\r"
                if self.done:
                    break
            else:
                self._size += 1
                if self._size > 1_048_576:
                    self._discard_event = True
                    self.response.error("sse_event_too_large")
                    self._line = ""
                    self._data.clear()
                if not self._discard_event:
                    self._line += char

    def _consume_line(self, timestamp: float) -> None:
        line, self._line = self._line, ""
        if not line:
            if self._data and not self._discard_event:
                self._consume_event("\n".join(self._data), timestamp)
            self._data.clear()
            self._size = 0
            self._discard_event = False
            return
        field_name, separator, value = line.partition(":")
        if separator and value.startswith(" "):
            value = value[1:]
        if field_name == "data":
            self._data.append(value)

    def _consume_event(self, raw: str, timestamp: float) -> None:
        if raw == "[DONE]":
            self.response.error("done_without_response_terminal")
            return
        try:
            obj = json.loads(raw)
        except (json.JSONDecodeError, RecursionError):
            self.response.error("invalid_sse_json")
            return
        if not isinstance(obj, dict):
            self.response.error("invalid_event_shape")
            return
        event = obj.get("type")
        if not isinstance(event, str):
            event = "error" if isinstance(obj.get("error"), dict) else ""
        if not event:
            self.response.error("invalid_event_shape")
            return
        response = obj.get("response")
        envelope = response if isinstance(response, dict) else {}
        error = obj.get("error")
        if not isinstance(error, dict):
            error = envelope.get("error")
        if event == "error" and not isinstance(error, dict) and "code" in obj:
            error = obj
        tier = envelope.get("service_tier", obj.get("service_tier"))
        safe_tier = tier if isinstance(tier, str) and tier in TIERS else None
        if tier is not None and safe_tier is None:
            self.response.error("unknown_service_tier")
        if len(self.response.event_tiers) < 10000:
            self.response.event_tiers.append(
                EventTier(
                    self._index,
                    event if event in EVENT_TYPES else "other",
                    timestamp,
                    safe_tier,
                )
            )
        else:
            self.response.error("event_metadata_limit")
        self._index += 1
        if event == "response.output_text.delta":
            delta = obj.get("delta")
            if not isinstance(delta, str):
                self.response.error("invalid_text_delta")
            elif delta:
                if self.response.first_text_timestamp is None:
                    self.response.first_text_timestamp = timestamp
                self.response.last_text_timestamp = timestamp
                self.response.visible_delta_count += 1
        if event not in TERMINALS:
            return
        self.response.terminal_type = event
        self.response.terminal_timestamp = timestamp
        self.response.usage = parse_usage(envelope.get("usage"))
        if event != "response.completed":
            self.response.error(event.replace(".", "_"))
            if not isinstance(error, dict):
                error = envelope.get("incomplete_details")
            code = error.get("code", error.get("reason")) if isinstance(error, dict) else None
            self.response.terminal_error_code = code if isinstance(code, str) and code in ERROR_CODES else "other"
        elif not isinstance(response, dict):
            self.response.error("invalid_terminal_response")

    def finish(self) -> ParsedResponse:
        if not self._finished:
            try:
                self._decoder.decode(b"", final=True)
            except UnicodeDecodeError:
                self.response.error("invalid_utf8")
        self._finished = True
        self._line = ""
        self._data.clear()
        if self.response.terminal_type is None:
            self.response.error("missing_response_terminal")
        for name, value in asdict(self.response.usage).items():
            if value is None:
                self.response.measurement_qualifications.append(f"missing_terminal_{name}")
        return self.response


class ChatSSEParser(SSEParser):
    """A finish chunk is not EOF: usage arrives before the Chat-only DONE marker."""

    def __init__(self) -> None:
        super().__init__()
        self._done = False

    @property
    def done(self) -> bool:
        return self._done

    def _consume_event(self, raw: str, timestamp: float) -> None:
        if raw == "[DONE]":
            self._done = True
            if self.response.terminal_type is None:
                self.response.error("done_without_chat_finish")
            return
        try:
            obj = json.loads(raw)
        except (json.JSONDecodeError, RecursionError):
            self.response.error("invalid_sse_json")
            return
        if not isinstance(obj, dict):
            self.response.error("invalid_event_shape")
            return
        error = obj.get("error")
        if error is not None:
            self.response.terminal_type = "chat.error"
            self.response.terminal_timestamp = timestamp
            code = error.get("code", error.get("type")) if isinstance(error, dict) else None
            self.response.terminal_error_code = code if isinstance(code, str) and code in ERROR_CODES else "other"
            self.response.error("chat_error")
            return
        choices = obj.get("choices")
        if not isinstance(choices, list) or len(choices) > 1:
            self.response.error("invalid_event_shape")
            return
        tier = obj.get("service_tier")
        safe_tier = tier if isinstance(tier, str) and tier in TIERS else None
        if tier is not None and safe_tier is None:
            self.response.error("unknown_service_tier")
        if len(self.response.event_tiers) < 10000:
            self.response.event_tiers.append(
                EventTier(self._index, "chat.chunk" if choices else "chat.usage", timestamp, safe_tier)
            )
        else:
            self.response.error("event_metadata_limit")
        self._index += 1
        if obj.get("usage") is not None:
            value = obj["usage"]
            if isinstance(value, dict):
                self.response.usage = parse_usage(
                    {
                        "input_tokens": value.get("prompt_tokens"),
                        "output_tokens": value.get("completion_tokens"),
                        "input_tokens_details": value.get("prompt_tokens_details"),
                        "output_tokens_details": value.get("completion_tokens_details"),
                    }
                )
            else:
                self.response.error("invalid_chat_usage")
        if not choices:
            return
        choice = choices[0]
        if not isinstance(choice, dict) or choice.get("index") != 0:
            self.response.error("invalid_event_shape")
            return
        if self.response.terminal_type is not None:
            self.response.error("chat_choice_after_finish")
            return
        delta = choice.get("delta")
        if not isinstance(delta, dict):
            self.response.error("invalid_event_shape")
            return
        content = delta.get("content")
        if content is not None and not isinstance(content, str):
            self.response.error("invalid_text_delta")
        elif content:
            if self.response.first_text_timestamp is None:
                self.response.first_text_timestamp = timestamp
            self.response.last_text_timestamp = timestamp
            self.response.visible_delta_count += 1
        finish = choice.get("finish_reason")
        if finish is not None:
            safe_finish = (
                finish
                if isinstance(finish, str)
                and finish in {"stop", "length", "content_filter", "tool_calls", "function_call"}
                else "other"
            )
            self.response.terminal_type = "chat." + safe_finish
            self.response.terminal_timestamp = timestamp
            if safe_finish != "stop":
                self.response.error("chat_" + safe_finish)

    def finish(self) -> ParsedResponse:
        if not self._done:
            self.response.error("missing_chat_done")
        return super().finish()


@dataclass(frozen=True)
class Trial:
    round: int
    pair_id: str
    model: str
    fast: bool


def schedule(seed: int, rounds: int) -> list[Trial]:
    if rounds < 2 or rounds % 2:
        raise ValueError("rounds must be positive and even for exact AB/BA balance")
    rng = random.Random(seed)
    starts = {model: bool(rng.getrandbits(1)) for model in MODELS}
    result = []
    for round_id in range(rounds):
        for model in rng.sample(list(MODELS), len(MODELS)):
            first = starts[model] ^ bool(round_id % 2)
            for fast in (first, not first):
                result.append(Trial(round_id, f"{round_id}:{model}", model, fast))
    return result


def metrics(parsed: ParsedResponse, started: float, ended: float) -> dict[str, Any]:
    first = parsed.first_text_timestamp
    last = parsed.last_text_timestamp
    terminal = parsed.terminal_timestamp
    output = parsed.usage.output_tokens
    reasoning = parsed.usage.reasoning_tokens
    visible = output - reasoning if output is not None and reasoning is not None and output >= reasoning else None
    e2e = ended - started
    duration = terminal - started if terminal is not None else None
    span = last - first if first is not None and last is not None else None
    reason = None
    if visible is None:
        reason = "unknown_or_invalid_token_accounting"
    elif span is None:
        reason = "no_visible_text_delta"
    elif span <= 0:
        reason = "non_positive_output_span"
    return {
        "e2e_seconds": e2e,
        "ttfo_seconds": first - started if first is not None else None,
        "last_visible_seconds": last - started if last is not None else None,
        "terminal_seconds": duration,
        "output_span_seconds": span,
        "visible_tokens": visible,
        "visible_tokens_per_e2e_second": visible / e2e if visible is not None and e2e > 0 else None,
        "approx_visible_tokens_per_output_second": visible / span
        if visible is not None and span is not None and span > 0
        else None,
        "approx_output_rate_unavailable_reason": reason,
        "total_output_tokens_per_terminal_second": (
            output / duration if output is not None and duration is not None and duration > 0 else None
        ),
    }


def bootstrap_interval(values: list[float], seed: int, draws: int = 2000) -> list[float] | None:
    """Seeded percentile 95% CI for mean paired delta; undefined for <2 pairs."""
    if len(values) < 2:
        return None
    rng = random.Random(seed)
    estimates = sorted(statistics.mean(rng.choices(values, k=len(values))) for _ in range(draws))

    def quantile(p: float) -> float:
        position = (len(estimates) - 1) * p
        lower = math.floor(position)
        upper = math.ceil(position)
        return estimates[lower] + (estimates[upper] - estimates[lower]) * (position - lower)

    return [quantile(0.025), quantile(0.975)]


def summarize(samples: list[dict[str, Any]], seed: int) -> dict[str, Any]:
    per_model: dict[str, Any] = {}
    for model in MODELS:
        rows = [row for row in samples if row["model"] == model]
        groups: dict[tuple[int, str], dict[bool, dict[str, Any]]] = {}
        for row in rows:
            arms = groups.setdefault((row["round"], row["pair_id"]), {})
            if row["fast"] in arms:
                raise ValueError("duplicate pair arm")
            arms[row["fast"]] = row
        result: dict[str, Any] = {
            "attempts": len(rows),
            "failed_requests": sum(bool(r["errors"]) for r in rows),
        }
        for fast, arm_name in ((False, "standard"), (True, "priority")):
            good = [row for row in rows if row["fast"] == fast and not row["errors"]]
            medians = {}
            for name in METRICS:
                values = [row[name] for row in good if row[name] is not None]
                medians[name] = {
                    "n": len(values),
                    "median": statistics.median(values) if values else None,
                }
            result[arm_name] = {"successful_requests": len(good), "metrics": medians}
        paired = {}
        for name in METRICS:
            deltas: list[dict[str, Any]] = []
            for (round_id, pair_id), arms in groups.items():
                if set(arms) != {False, True} or any(row["errors"] for row in arms.values()):
                    continue
                standard, priority = arms[False][name], arms[True][name]
                if standard is not None and priority is not None:
                    deltas.append(
                        {
                            "round": round_id,
                            "pair_id": pair_id,
                            "delta": priority - standard,
                        }
                    )
            values: list[float] = [row["delta"] for row in deltas]
            higher_wins = name in RATE_METRICS or name == "total_output_tokens_per_terminal_second"
            paired[name] = {
                "n": len(values),
                "excluded_pairs": len(groups) - len(values),
                "deltas": deltas,
                "median_delta": statistics.median(values) if values else None,
                "mean_delta": statistics.mean(values) if values else None,
                "priority_wins": sum(v > 0 if higher_wins else v < 0 for v in values),
                "standard_wins": sum(v < 0 if higher_wins else v > 0 for v in values),
                "ties": sum(v == 0 for v in values),
                "mean_delta_bootstrap_95pct": bootstrap_interval(values, seed),
            }
        result["paired"] = paired
        per_model[model] = result
    return {
        "per_model": per_model,
        "bootstrap_seed": seed,
        "bootstrap_draws": 2000,
        "delta_direction": "priority minus standard",
        "bootstrap_method": "paired percentile mean, 95%",
        "caution": "Small samples and cache/order effects limit inference; tiers do not prove scheduling priority.",
    }


def request_payload(config: Config, trial: Trial, surface: Surface = "responses") -> dict[str, Any]:
    payload: dict[str, Any] = {
        "model": trial.model,
        "input": PROMPT,
        "stream": True,
        "store": False,
        "reasoning": {"effort": config.reasoning_effort[trial.model]},
        # Includes reasoning; the prompt targets roughly 300-600 visible tokens.
        "max_output_tokens": 1600,
    }
    if surface == "chat":
        payload.pop("input")
        payload.pop("reasoning")
        payload.pop("max_output_tokens")
        payload.update(
            messages=[{"role": "user", "content": PROMPT}],
            reasoning_effort=config.reasoning_effort[trial.model],
            max_completion_tokens=1600,
            stream_options={"include_usage": True},
        )
    elif surface == "codex":
        payload["instructions"] = ""
        payload["input"] = [{"role": "user", "content": [{"type": "input_text", "text": PROMPT}]}]
    if trial.fast:
        payload["service_tier"] = "priority"
    return payload


def surface_url(config: Config, surface: Surface) -> str:
    if surface == "codex":
        parts = urlsplit(config.base_url)
        return f"{parts.scheme}://{parts.netloc}/backend-api/codex/responses"
    return config.base_url + ("/chat/completions" if surface == "chat" else "/responses")


async def request(
    session: aiohttp.ClientSession,
    config: Config,
    trial: Trial,
    timeout: float,
    surface: Surface = "responses",
) -> dict[str, Any]:
    started = time.monotonic()
    parser = ChatSSEParser() if surface == "chat" else SSEParser()
    url = surface_url(config, surface)
    status = None
    request_id = None
    errors: list[str] = []
    try:
        # asyncio.timeout is an exact wall bound independent of aiohttp timeout rounding.
        async with asyncio.timeout(timeout):
            async with session.post(
                url,
                json=request_payload(config, trial, surface),
                headers={
                    "Authorization": f"Bearer {config.keys[trial.model]}",
                    "Accept": "text/event-stream",
                },
                allow_redirects=False,
                timeout=aiohttp.ClientTimeout(total=None),
            ) as response:
                status = response.status
                raw_id = response.headers.get("x-request-id")
                if raw_id is not None:
                    if re.fullmatch(r"[A-Za-z0-9_.:-]{1,200}", raw_id) and not any(
                        key in raw_id for key in config.keys.values()
                    ):
                        request_id = raw_id
                    else:
                        errors.append("invalid_request_id")
                if not 200 <= status < 300:
                    errors.append("http_error")
                elif response.content_type != "text/event-stream":
                    errors.append("unexpected_content_type")
                else:
                    async for chunk in response.content.iter_any():
                        parser.feed(chunk, time.monotonic())
                        if parser.done:
                            break
    except TimeoutError:
        errors.append("request_wall_timeout")
    except aiohttp.ClientError:
        # Exception strings can contain auth, response bodies, or sensitive URLs.
        errors.append("http_transport_error")
    except OSError:
        errors.append("os_transport_error")
    parsed = parser.finish()
    ended = time.monotonic()
    errors.extend(parsed.errors)
    if parsed.first_text_timestamp is None:
        errors.append("no_visible_text_delta")
    if (
        parsed.usage.output_tokens is not None
        and parsed.usage.reasoning_tokens is not None
        and parsed.usage.reasoning_tokens > parsed.usage.output_tokens
    ):
        errors.append("invalid_token_accounting")
    return {
        **asdict(trial),
        "surface": surface,
        "route": urlsplit(url).path,
        **metrics(parsed, started, ended),
        **asdict(parsed.usage),
        "http_status": status,
        "request_id": request_id,
        "terminal_type": parsed.terminal_type,
        "terminal_error_code": parsed.terminal_error_code,
        "errors": errors,
        "measurement_qualifications": parsed.measurement_qualifications,
        "visible_delta_count": parsed.visible_delta_count,
        "event_tiers": [{**asdict(event), "timestamp": event.timestamp - started} for event in parsed.event_tiers],
    }


async def run(
    config: Config,
    rounds: int,
    seed: int,
    timeout: float,
    surface: Surface = "responses",
    *,
    journal: Path | None = None,
    campaign_timeout: float | None = None,
) -> dict[str, Any]:
    trials = schedule(seed, rounds)
    if not math.isfinite(timeout) or timeout <= 0:
        raise ValueError("timeout must be positive and finite")
    if campaign_timeout is not None and (not math.isfinite(campaign_timeout) or campaign_timeout <= 0):
        raise ValueError("campaign timeout must be positive and finite")
    samples = []
    # Exclusive creation prevents mixing campaigns or silently replaying an interrupted run.
    with journal.open("x", encoding="utf-8") if journal else nullcontext() as checkpoint:
        async with (
            asyncio.timeout(campaign_timeout),
            aiohttp.ClientSession(connector=aiohttp.TCPConnector(limit=1), trust_env=False) as session,
        ):
            for trial in trials:
                row = await request(session, config, trial, timeout, surface)
                samples.append(row)
                if checkpoint is not None:
                    checkpoint.write(json.dumps({**row, "seed": seed}, allow_nan=False) + "\n")
                    checkpoint.flush()
                    os.fsync(checkpoint.fileno())
    return {
        "schema_version": 1,
        "surface": surface,
        "route": urlsplit(surface_url(config, surface)).path,
        "seed": seed,
        "rounds": rounds,
        "concurrency": 1,
        "request_wall_timeout_seconds": timeout,
        "reasoning_effort": config.reasoning_effort,
        "standard_tier_unenforced_verified_by_lead": True,
        "single_account_per_model_verified_by_lead": True,
        "visible_token_accounting": "output minus reasoning; not tokenizer measured",
        "output_rate": "approximate; accounted visible tokens divided by first-to-last delta span",
        "timing": "monotonic seconds; E2E includes response context close; terminal is parsed event receipt",
        "target_visible_tokens": [300, 600],
        "target_words": 350,
        "samples": samples,
        "summary": summarize(samples, seed),
    }


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("config", type=Path)
    parser.add_argument("--rounds", "--repeats", type=int, default=4)
    parser.add_argument("--seed", type=int, default=20260908)
    parser.add_argument("--timeout", type=float, default=120.0)
    parser.add_argument("--surface", choices=("responses", "chat", "codex"), default="responses")
    parser.add_argument("--output", type=Path)
    parser.add_argument("--journal", type=Path, help="new JSONL checkpoint; defaults to --output with .jsonl suffix")
    parser.add_argument(
        "--campaign-timeout", type=float, help="wall limit for the whole run; completed requests stay journaled"
    )
    args = parser.parse_args()
    try:
        journal = args.journal or (args.output.with_suffix(".jsonl") if args.output else None)
        if args.output and journal and args.output.resolve() == journal.resolve():
            raise ValueError("output and journal must be different paths")
        if args.output and args.output.exists():
            raise ValueError("output already exists; use a new campaign path")
        config = load_config(args.config)
        result = asyncio.run(
            run(
                config,
                args.rounds,
                args.seed,
                args.timeout,
                args.surface,
                journal=journal,
                campaign_timeout=args.campaign_timeout,
            )
        )
    except ValueError as error:
        parser.error(str(error))
    except FileExistsError:
        parser.error("journal already exists; use a new campaign path; no requests replayed")
    except TimeoutError:
        parser.error("campaign wall timeout; completed requests remain in the journal if configured")
    except OSError:
        parser.error("configuration, runtime or checkpoint I/O failed; completed journal records are retained")
    text = json.dumps(result, indent=2, allow_nan=False) + "\n"
    if args.output:
        args.output.write_text(text, encoding="utf-8")
    else:
        print(text, end="")
    return int(any(row["errors"] for row in result["samples"]))


if __name__ == "__main__":
    raise SystemExit(main())
