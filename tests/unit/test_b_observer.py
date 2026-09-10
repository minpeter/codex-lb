"""Deterministic observer behavior and real local command boundaries."""

from __future__ import annotations

import importlib.util
import json
import os
import sqlite3
import subprocess
import sys
import threading
from datetime import UTC, datetime, timedelta
from http.server import BaseHTTPRequestHandler, HTTPServer
from pathlib import Path

import pytest

spec = importlib.util.spec_from_file_location("b_observer", Path(__file__).parents[2] / "scripts/ops/b_observer.py")
assert spec is not None and spec.loader is not None
observer = importlib.util.module_from_spec(spec)
sys.modules[spec.name] = observer
spec.loader.exec_module(observer)

AT = datetime(2026, 9, 10, tzinfo=UTC)


def snapshot(**changes):
    values = dict(
        at=AT,
        rows=20,
        requests=20,
        successes=20,
        no_accounts=0,
        top_share=0.4,
        active_accounts=4,
        fresh_accounts=4,
        remaining_weight=200,
        health="up",
        log_ids=[],
        exception_ids=[],
        logs_capped=False,
    )
    return observer.Snapshot(**(values | changes))


def advance(snap, previous=None):
    return observer.analyze(snap, previous, observer.Thresholds())


def codes(record):
    return [event.code for event in record.events]


def test_healthy_spike_duplicate_recovery():
    healthy = advance(snapshot())
    spike = advance(snapshot(at=AT + timedelta(minutes=5), successes=16), healthy)
    stable = advance(snapshot(at=AT + timedelta(minutes=10), successes=16), spike)
    recovered = advance(snapshot(at=AT + timedelta(minutes=15), successes=19), stable)
    assert [r.traffic for r in (healthy, spike, stable, recovered)] == ["healthy", "spike", "spike", "recovered"]
    assert codes(spike) == ["traffic.spike"]
    assert stable.events == []
    assert codes(recovered) == ["traffic.recovered"]
    assert advance(snapshot(at=AT + timedelta(minutes=20)), recovered).events == []


@pytest.mark.parametrize(
    "count,success,expected",
    [(0, 0, "noTraffic"), (19, 0, "insufficientSample"), (20, 17, "healthy"), (20, 16, "spike"), (20, 0, "spike")],
)
def test_entry_boundaries(count, success, expected):
    result = advance(snapshot(rows=count, requests=count, successes=success))
    assert result.traffic == expected
    assert result.success_ratio == (success / count if count else None)
    assert result.error_ratio == ((count - success) / count if count else None)


@pytest.mark.parametrize("success,expected", [(18, "spike"), (19, "recovered"), (20, "recovered")])
def test_recovery_boundaries(success, expected):
    prior = advance(snapshot(successes=0))
    result = advance(snapshot(at=AT + timedelta(minutes=5), successes=success), prior)
    assert result.traffic == expected


def test_no_traffic_does_not_recover_incident():
    spike = advance(snapshot(successes=0))
    idle = advance(snapshot(at=AT + timedelta(minutes=5), rows=0, requests=0, successes=0), spike)
    result = advance(snapshot(at=AT + timedelta(minutes=10), successes=18), idle)
    assert idle.traffic == "noTraffic"
    assert result.traffic == "spike"


def test_capacity_missing_quota_and_concentration_transitions():
    healthy = advance(snapshot())
    missing = advance(
        snapshot(at=AT + timedelta(minutes=5), fresh_accounts=0, remaining_weight=0, top_share=0.8), healthy
    )
    assert missing.capacity == "unknown"
    assert set(codes(missing)) == {"quota.stale_or_missing", "capacity.unknown", "concentration.high"}
    assert (
        advance(
            snapshot(at=AT + timedelta(minutes=10), fresh_accounts=0, remaining_weight=0, top_share=0.8), missing
        ).events
        == []
    )
    low = advance(snapshot(remaining_weight=20))
    assert low.capacity == "low"
    assert advance(snapshot(remaining_weight=20.01)).capacity == "normal"


def test_aggregate_no_accounts_exception_and_logged_isolation_dedupe():
    alias = "a" * 64
    prior = advance(snapshot())
    error = snapshot(at=AT + timedelta(minutes=5), no_accounts=3, exception_ids=["b" * 64], log_ids=[alias, alias])
    result = advance(error, prior)
    assert set(codes(result)) == {"no_accounts.present", "exception.new", "isolation.logged_change"}
    assert next(e.count for e in result.events if e.code == "isolation.logged_change") == 1
    stable = advance(error.model_copy(update={"at": AT + timedelta(minutes=10), "no_accounts": 9}), result)
    assert stable.events == []
    cleared = advance(snapshot(at=AT + timedelta(minutes=15)), stable)
    again = advance(error.model_copy(update={"at": AT + timedelta(minutes=20)}), cleared)
    assert codes(again) == ["no_accounts.present"]


def test_external_health_and_log_coverage_transitions():
    prior = advance(snapshot())
    down = advance(snapshot(at=AT + timedelta(minutes=5), health="down", logs_capped=True), prior)
    assert set(codes(down)) == {"health.down", "logs.capped"}
    assert advance(snapshot(at=AT + timedelta(minutes=10), health="down", logs_capped=True), down).events == []
    assert set(codes(advance(snapshot(at=AT + timedelta(minutes=15)), down))) == {"health.up", "logs.complete"}


def test_history_recovers_dedupe_and_rejects_partial_record(tmp_path):
    journal = tmp_path / "history.jsonl"
    first = observer.observe(snapshot(successes=0), journal, "fixture-target")
    assert observer.observe(snapshot(successes=0), journal, "fixture-target") is None
    second = observer.observe(snapshot(at=AT + timedelta(minutes=5), successes=0), journal, "fixture-target")
    assert first is not None and second is not None
    assert second.events == []
    assert len(journal.read_text().splitlines()) == 2
    assert first.traffic == second.traffic == "spike"
    with pytest.raises(ValueError):
        observer.observe(snapshot(at=AT + timedelta(minutes=10)), journal, "other-target")
    with journal.open("a") as stream:
        stream.write('{"partial":')
    with pytest.raises(ValueError):
        observer.observe(snapshot(at=AT + timedelta(minutes=10)), journal, "fixture-target")


def test_log_parser_hashes_identifiers_and_never_retains_messages():
    logs = (
        "2026-09-10T00:00:01Z Account overload isolation engaged account_id=private@example.test "
        "level=2 isolation_seconds=900 Authorization: bearer-secret\n"
        "2026-09-10T00:00:02Z ValueError: private-error-secret\n"
    )
    isolation, exceptions = observer.parse_logs(logs)
    assert len(isolation) == len(exceptions) == 1
    assert all(len(value) == 64 for value in isolation + exceptions)
    assert observer.parse_logs(logs + logs) == (isolation, exceptions)
    assert observer.parse_logs(logs.replace("private-error-secret", "different"))[1] == exceptions


def test_remote_sqlite_contract_is_read_only_and_counts_unique_requests(tmp_path):
    db = tmp_path / "store.db"
    with sqlite3.connect(db) as conn:
        conn.executescript("""
            CREATE TABLE request_logs(id INTEGER, request_id TEXT, account_id TEXT, status TEXT,
                                      error_code TEXT, upstream_error_code TEXT, requested_at TEXT);
            CREATE TABLE accounts(id TEXT, status TEXT, delete_requested_at TEXT);
            CREATE TABLE usage_history(id INTEGER, account_id TEXT, window TEXT, used_percent REAL,
                                       reset_at REAL, window_minutes INTEGER, recorded_at TEXT);
            INSERT INTO accounts VALUES ('secret-account', 'active', NULL);
            INSERT INTO request_logs VALUES (1, 'request-secret', 'secret-account', 'error', 'no_accounts', NULL,
                                            '2026-09-09 23:59:00.000000');
            INSERT INTO request_logs VALUES (2, 'request-secret', 'secret-account', 'success', NULL, NULL,
                                            '2026-09-09 23:59:01.000000');
            INSERT INTO request_logs VALUES (3, 'second', 'secret-account', 'error', NULL, NULL,
                                            '2026-09-10 00:00:00.000000');
        """)
    before = db.read_bytes()
    result = subprocess.run(
        [sys.executable, "-c", observer.REMOTE, str(db), "2026-09-09 23:55:00.000000", "2026-09-10 00:00:00.000000"],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    values = json.loads(result.stdout)
    assert (values["rows"], values["requests"], values["successes"], values["no_accounts"]) == (2, 1, 1, 1)
    assert values["fresh_accounts"] == 0
    assert "secret" not in result.stdout
    assert db.read_bytes() == before


def test_cli_once_restarts_and_emits_only_transitions(tmp_path):
    fixture = tmp_path / "snapshot.json"
    journal = tmp_path / "history.jsonl"
    command = [
        sys.executable,
        "scripts/ops/b_observer.py",
        "--container",
        "synthetic-b",
        "--base-url",
        "http://127.0.0.1:1",
        "--journal",
        str(journal),
        "--once",
        "--snapshot",
        str(fixture),
    ]
    outputs = []
    for index, successes in enumerate([20, 16, 16, 19]):
        fixture.write_text(snapshot(at=AT + timedelta(minutes=5 * index), successes=successes).model_dump_json())
        proc = subprocess.run(command, capture_output=True, text=True, timeout=10)
        assert proc.returncode == 0, proc.stderr
        outputs.append([json.loads(line)["code"] for line in proc.stdout.splitlines()])
    assert outputs[1:] == [["traffic.spike"], [], ["traffic.recovered"]]
    records = [json.loads(line)["analysis"] for line in journal.read_text().splitlines()]
    assert [record["traffic"] for record in records] == ["healthy", "spike", "spike", "recovered"]
    assert records[-1]["rows_delta"] == 0
    assert os.stat(journal).st_mode & 0o777 == 0o600


@pytest.mark.parametrize("url", ["http://user:secret@localhost", "http://localhost/?key=secret", "ftp://localhost"])
def test_cli_rejects_credential_bearing_or_non_http_targets(tmp_path, url):
    proc = subprocess.run(
        [
            sys.executable,
            "scripts/ops/b_observer.py",
            "--container",
            "synthetic-b",
            "--base-url",
            url,
            "--journal",
            str(tmp_path / "history"),
            "--once",
        ],
        capture_output=True,
        text=True,
        timeout=10,
    )
    assert proc.returncode != 0
    assert "secret" not in proc.stdout + proc.stderr


@pytest.mark.parametrize("body", [b"[]", b'{"checks":null}', b'{"checks":{"database":"ok"}}'])
def test_cli_docker_and_http_boundaries(tmp_path, body):
    """Execute the real CLI against a Docker protocol fixture and loopback HTTP."""
    docker = tmp_path / "docker"
    receipt = tmp_path / "commands.jsonl"
    docker.write_text(
        f"#!{sys.executable}\n" + "import json,os,sys\n"
        "from datetime import datetime\n"
        "args=sys.argv[1:]\n"
        "with open(os.environ['OBSERVER_COMMANDS'],'a') as stream: stream.write(json.dumps(args)+'\\n')\n"
        "if args[0]=='exec':\n"
        " assert args[:6]==['exec','-i','synthetic-b','python','-','/var/lib/codex-lb/store.db']\n"
        " assert (datetime.fromisoformat(args[7])-datetime.fromisoformat(args[6])).total_seconds()==300\n"
        " assert sys.stdin.read()\n"
        " print(json.dumps(dict(rows=20,requests=20,successes=int(os.environ['OBSERVER_SUCCESSES']),"
        " no_accounts=0,top_share=0.4,active_accounts=4,fresh_accounts=4,remaining_weight=200)))\n"
        "elif args[0]=='logs':\n"
        " assert args[-1]=='synthetic-b' and args[args.index('--tail')+1]=='2000'\n"
        " print('2026-09-10T00:00:00Z ValueError: bearer-secret')\n"
        "else: raise AssertionError(args)\n"
    )
    docker.chmod(0o700)
    requests = []
    done = threading.Event()

    class Handler(BaseHTTPRequestHandler):
        def do_GET(self):
            requests.append((self.path, dict(self.headers)))
            self.send_response(200)
            self.send_header("Content-Length", str(len(body)))
            self.end_headers()
            self.wfile.write(body)

        def log_message(self, format: str, *args) -> None:
            return

    server = HTTPServer(("127.0.0.1", 0), Handler)
    server.timeout = 10

    def serve():
        try:
            for _ in range(4):
                server.handle_request()
        finally:
            done.set()

    worker = threading.Thread(target=serve)
    worker.start()
    journal = tmp_path / "history.jsonl"
    outputs = []
    try:
        for successes in (20, 16, 16, 19):
            proc = subprocess.run(
                [
                    sys.executable,
                    "scripts/ops/b_observer.py",
                    "--container",
                    "synthetic-b",
                    "--base-url",
                    f"http://127.0.0.1:{server.server_port}",
                    "--once",
                    "--journal",
                    str(journal),
                ],
                env=os.environ
                | {
                    "PATH": str(tmp_path) + os.pathsep + os.environ["PATH"],
                    "OBSERVER_COMMANDS": str(receipt),
                    "OBSERVER_SUCCESSES": str(successes),
                },
                text=True,
                capture_output=True,
                timeout=15,
            )
            outputs.append(proc)
        assert done.wait(10)
    finally:
        worker.join(45)
        server.server_close()
    assert not worker.is_alive()
    assert [proc.returncode for proc in outputs] == [0, 0, 0, 0], [proc.stderr for proc in outputs]
    assert [[json.loads(line)["code"] for line in proc.stdout.splitlines()] for proc in outputs[1:]] == [
        ["traffic.spike"],
        [],
        ["traffic.recovered"],
    ]
    records = [json.loads(line)["analysis"] for line in journal.read_text().splitlines()]
    assert records[-1]["snapshot"]["health"] == ("up" if b'"ok"' in body else "down")
    assert len(requests) == 4
    assert all(path == "/health/ready" and "Authorization" not in headers for path, headers in requests)
    assert len(receipt.read_text().splitlines()) == 8
    assert "bearer-secret" not in journal.read_text()
    print(
        json.dumps(
            {
                "cli_exits": [proc.returncode for proc in outputs],
                "states": [r["traffic"] for r in records],
                "http_gets": len(requests),
                "docker_commands": 8,
                "listener_closed": server.fileno() == -1,
                "worker_joined": not worker.is_alive(),
                "secret_absent": True,
            }
        )
    )


@pytest.mark.parametrize(
    "age,minutes,reset,expected", [(900, 10080, 1, 1), (901, 10080, 1, 0), (5, 0, 1, 0), (5, 10080, 0, 0)]
)
def test_sqlite_quota_freshness_boundaries(tmp_path, age, minutes, reset, expected):
    db = tmp_path / "quota.db"
    with sqlite3.connect(db) as connection:
        connection.executescript("""
            CREATE TABLE request_logs(id INTEGER, request_id TEXT, account_id TEXT, status TEXT,
                                      error_code TEXT, upstream_error_code TEXT, requested_at TEXT);
            CREATE TABLE accounts(id TEXT, status TEXT, delete_requested_at TEXT);
            CREATE TABLE usage_history(id INTEGER, account_id TEXT, window TEXT, used_percent REAL,
                                       reset_at REAL, window_minutes INTEGER, recorded_at TEXT);
            INSERT INTO accounts VALUES ('private-account', 'active', NULL);
        """)
        recorded = (AT - timedelta(seconds=age)).replace(tzinfo=None).isoformat(" ", timespec="microseconds")
        connection.execute(
            "INSERT INTO usage_history VALUES (1, ?, 'primary', 80, ?, ?, ?)",
            ("private-account", AT.timestamp() + reset, minutes, recorded),
        )
    result = subprocess.run(
        [sys.executable, "-c", observer.REMOTE, str(db), "2026-09-09 23:55:00.000000", "2026-09-10 00:00:00.000000"],
        capture_output=True,
        text=True,
        check=True,
        timeout=10,
    )
    data = json.loads(result.stdout)
    assert data["fresh_accounts"] == expected
    assert data["remaining_weight"] == 20 * expected
    assert data["requests"] == 0 and data["top_share"] is None
