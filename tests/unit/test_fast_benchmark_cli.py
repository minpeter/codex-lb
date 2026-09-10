from __future__ import annotations

import asyncio
import json
import socket
import sys
from pathlib import Path

import pytest
from aiohttp import web

from scripts.qa.fast_benchmark import PROMPT


@pytest.mark.asyncio
async def test_real_cli_sol_campaign_preserves_wire_and_journal(tmp_path: Path) -> None:
    # Given a loopback-only server with synthetic credentials and usage.
    key = "dummy-loopback-credential"
    text = "synthetic-output-redaction-sentinel"
    headers = {"User-Agent": "synthetic-client/1", "originator": "synthetic-client", "version": "1"}
    received = []

    async def respond(request: web.Request) -> web.Response:
        body = await request.json()
        received.append(
            {
                "model": body.get("model"),
                "effort": body.get("reasoning", {}).get("effort"),
                "tier": body.get("service_tier"),
                "headers_match": all(request.headers.get(name) == value for name, value in headers.items()),
                "authorization_matches": request.headers.get("Authorization") == "Bearer " + key,
                "accept_matches": request.headers.get("Accept") == "text/event-stream",
                "payload_matches": body
                == {
                    "model": "gpt-5.6-sol",
                    "input": PROMPT,
                    "stream": True,
                    "store": False,
                    "reasoning": {"effort": "low"},
                    "max_output_tokens": 1600,
                    **({"service_tier": "priority"} if "service_tier" in body else {}),
                },
            }
        )
        events = [
            {"type": "response.output_text.delta", "delta": text},
            {"type": "response.output_text.delta", "delta": text},
            {
                "type": "response.completed",
                "response": {
                    "service_tier": body.get("service_tier", "default"),
                    "usage": {
                        "input_tokens": 100,
                        "output_tokens": 500,
                        "output_tokens_details": {"reasoning_tokens": 100},
                        "input_tokens_details": {"cached_tokens": 50},
                    },
                },
            },
        ]
        wire = "".join("data: " + json.dumps(event) + "\n\n" for event in events)
        return web.Response(
            text=wire, content_type="text/event-stream", headers={"x-request-id": f"req-local-{len(received)}"}
        )

    app = web.Application()
    app.router.add_post("/v1/responses", respond)
    runner = web.AppRunner(app, access_log=None)
    config = tmp_path / "config.json"
    output = tmp_path / "campaign.json"
    journal = tmp_path / "campaign.jsonl"
    process = None
    receipt: dict[str, object] = {"provider_requests": 0}
    with socket.socket() as listener:
        listener.bind(("127.0.0.1", 0))
        listener.listen()
        port = listener.getsockname()[1]
        try:
            await runner.setup()
            site = web.SockSite(runner, listener)
            await site.start()  # Bound listener readiness, never sleep/poll.
            config.touch(mode=0o600)
            value = {
                "base_url": f"http://127.0.0.1:{port}/v1",
                "models": ["gpt-5.6-sol"],
                "keys": {"gpt-5.6-sol": key},
                "headers": headers,
                "standard_tier_unenforced_verified": True,
                "single_account_per_model_verified": True,
            }
            config.write_text(json.dumps(value))
            command = [
                sys.executable,
                "-B",
                "scripts/qa/fast_benchmark.py",
                str(config),
                "--rounds",
                "10",
                "--seed",
                "17",
                "--timeout",
                "5",
                "--campaign-timeout",
                "60",
                "--surface",
                "responses",
                "--output",
                str(output),
                "--journal",
                str(journal),
            ]
            receipt["command"] = command
            receipt["config_mode"] = oct(config.stat().st_mode & 0o777)
            # When the real executable runs through its public CLI.
            process = await asyncio.create_subprocess_exec(
                *command,
                cwd=Path(__file__).resolve().parents[2],
                stdout=asyncio.subprocess.PIPE,
                stderr=asyncio.subprocess.PIPE,
            )
            stdout, stderr = await asyncio.wait_for(process.communicate(), timeout=75)
            (tmp_path / "stdout.log").write_bytes(stdout)
            (tmp_path / "stderr.log").write_bytes(stderr)
            receipt["exit_code"] = process.returncode
            assert process.returncode == 0, stderr.decode()
            # Then both treatment arms preserve the envelope and accounting.
            result = json.loads(output.read_text())
            samples = result["samples"]
            assert len(received) == len(samples) == 20
            assert sum(row["tier"] == "priority" for row in received) == 10
            assert sum(row["tier"] is None for row in received) == 10
            assert all(row["model"] == "gpt-5.6-sol" and row["effort"] == "low" for row in received)
            for flag in ("headers_match", "authorization_matches", "accept_matches", "payload_matches"):
                assert all(row[flag] for row in received)
            for index, (sample, sent) in enumerate(zip(samples, received, strict=True), start=1):
                assert sample["fast"] == (sent["tier"] == "priority")
                assert sample["model"] == sent["model"]
                assert sample["errors"] == []
                assert sample["request_id"] == f"req-local-{index}"
                assert sample["terminal_type"] == "response.completed"
                assert sample["http_status"] == 200
                assert [
                    sample[name] for name in ("input_tokens", "output_tokens", "reasoning_tokens", "cached_tokens")
                ] == [100, 500, 100, 50]
                assert sample["visible_tokens"] == 400
                assert sample["visible_delta_count"] == 2
                assert sample["event_tiers"][-1]["service_tier"] == ("priority" if sample["fast"] else "default")
            for first, second in zip(samples[::2], samples[1::2], strict=True):
                assert first["pair_id"] == second["pair_id"]
                assert first["fast"] is not second["fast"]
            assert sum(row["fast"] for row in samples[::2]) == 5
            assert set(result["summary"]["per_model"]) == {"gpt-5.6-sol"}
            summary = result["summary"]["per_model"]["gpt-5.6-sol"]
            assert summary["paired"]["e2e_seconds"]["n"] == 10
            assert summary["standard"]["successful_requests"] == summary["priority"]["successful_requests"] == 10
            assert [json.loads(line) for line in journal.read_text().splitlines()] == [
                {**row, "seed": 17} for row in samples
            ]
            serialized = output.read_text() + journal.read_text() + stdout.decode() + stderr.decode()
            assert key not in serialized and text not in serialized
            receipt.update(request_count=20, priority=10, standard=10, journal_equal=True, redacted=True)
        finally:
            if process is not None and process.returncode is None:
                process.kill()
                await asyncio.wait_for(process.communicate(), timeout=5)
            await asyncio.wait_for(runner.cleanup(), timeout=5)
            config.unlink(missing_ok=True)
            receipt["process_reaped"] = process is None or process.returncode is not None
            receipt["listener_closed"] = not runner.sites
            receipt["config_removed"] = not config.exists()
            (tmp_path / "requests.json").write_text(json.dumps(received, indent=2) + "\n")
            (tmp_path / "receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
