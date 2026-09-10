"""Read-only B snapshots. The JSONL journal is the single durable state store."""

from __future__ import annotations

import argparse
import fcntl
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.parse
import urllib.request
from datetime import UTC, datetime, timedelta
from pathlib import Path
from typing import Annotated, Literal

from pydantic import BaseModel, ConfigDict, Field, model_validator

# Runs inside the explicitly selected B container; no app imports or refresh jobs.
REMOTE = r"""
import collections,contextlib,datetime,json,sqlite3,sys,urllib.parse
lo,hi=sys.argv[2:4]
end=datetime.datetime.fromisoformat(hi).replace(tzinfo=datetime.timezone.utc)
with contextlib.closing(sqlite3.connect('file:'+urllib.parse.quote(sys.argv[1])+'?mode=ro',uri=True,timeout=5)) as c:
 c.row_factory=sqlite3.Row
 c.execute('PRAGMA query_only=ON'); c.execute('BEGIN')
 deadline=datetime.datetime.now(datetime.timezone.utc).timestamp()+8
 c.set_progress_handler(lambda: datetime.datetime.now(datetime.timezone.utc).timestamp()>deadline,10000)
 rows=c.execute('SELECT request_id,account_id,status,error_code,upstream_error_code FROM request_logs '
 'WHERE requested_at>=? AND requested_at<? ORDER BY requested_at,id',(lo,hi)).fetchall()
 last={r['request_id']:r for r in rows}
 shares=collections.Counter(r['account_id'] for r in rows if r['account_id'] is not None)
 accounts={r[0] for r in c.execute('SELECT id FROM accounts WHERE status=? AND delete_requested_at IS NULL',
 ('active',))}
 quota=c.execute('SELECT account_id,used_percent,reset_at,window_minutes,recorded_at FROM '
 '(SELECT *,ROW_NUMBER() OVER (PARTITION BY account_id,window ORDER BY recorded_at DESC,id DESC) rn '
 'FROM usage_history WHERE window=? AND recorded_at<?) WHERE rn=1',('primary',hi)).fetchall()
 fresh=[r for r in quota if r['account_id'] in accounts and r['window_minutes'] is not None
 and r['window_minutes']>0 and r['reset_at'] is not None and r['reset_at']>end.timestamp()
 and 0<=(end-datetime.datetime.fromisoformat(r['recorded_at'])
 .replace(tzinfo=datetime.timezone.utc)).total_seconds()<=900]
 result=dict(rows=len(rows),requests=len(last),successes=sum(r['status']=='success' for r in last.values()),
 no_accounts=sum(r['error_code']=='no_accounts' or r['upstream_error_code']=='no_accounts' for r in rows),
 active_accounts=len(accounts),fresh_accounts=len(fresh),
 remaining_weight=sum(max(0,100-r['used_percent']) for r in fresh),
 top_share=max(shares.values())/sum(shares.values()) if shares else None)
 c.rollback()
 print(json.dumps(result))
"""

Count = Annotated[int, Field(ge=0)]
Digest = Annotated[str, Field(pattern=r"^[a-f0-9]{64}$")]


class Snapshot(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)
    at: datetime
    rows: Count
    requests: Count
    successes: Count
    no_accounts: Count
    top_share: Annotated[float, Field(ge=0, le=1)] | None
    active_accounts: Count
    fresh_accounts: Count
    remaining_weight: Annotated[float, Field(ge=0)]
    health: Literal["up", "down"]
    log_ids: list[Digest]
    exception_ids: list[Digest]
    logs_capped: bool

    @model_validator(mode="after")
    def check_counts(self) -> Snapshot:
        if (
            self.at.tzinfo is None
            or self.successes > self.requests
            or self.requests > self.rows
            or self.no_accounts > self.rows
            or self.fresh_accounts > self.active_accounts
        ):
            raise ValueError("invalid observation counts or timestamp")
        return self


class Thresholds(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid", allow_inf_nan=False)
    minimum_sample: Annotated[int, Field(ge=1)] = 20
    spike_error_ratio: Annotated[float, Field(gt=0, le=1)] = 0.20
    recovery_error_ratio: Annotated[float, Field(ge=0, lt=1)] = 0.05
    low_capacity_weight: Annotated[float, Field(ge=0)] = 20
    high_concentration: Annotated[float, Field(gt=0, le=1)] = 0.80

    @model_validator(mode="after")
    def check_hysteresis(self) -> Thresholds:
        if self.recovery_error_ratio >= self.spike_error_ratio:
            raise ValueError("recovery must be below entry threshold")
        return self


class Event(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    code: str
    count: int | None = None


class Analysis(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    at: datetime
    snapshot: Snapshot
    traffic: Literal["noTraffic", "insufficientSample", "healthy", "spike", "recovered"]
    incident: bool
    recovered: bool
    success_ratio: float | None
    error_ratio: float | None
    capacity: Literal["unknown", "low", "normal"]
    quota: Literal["fresh", "stale_or_missing"]
    concentration: Literal["unknown", "high", "normal"]
    seen_logs: list[Digest]
    seen_exceptions: list[Digest]
    events: list[Event]
    rows_delta: int | None
    requests_delta: int | None


def analyze(snapshot: Snapshot, previous: Analysis | None, thresholds: Thresholds) -> Analysis:
    """Pure state transition; an empty or small sample cannot clear an incident."""
    n = snapshot.requests
    error = (n - snapshot.successes) / n if n else None
    incident = previous.incident if previous else False
    recovered = previous.recovered if previous else False
    if n >= thresholds.minimum_sample and error is not None:
        if error >= thresholds.spike_error_ratio:
            incident, recovered = True, False
        elif incident and error <= thresholds.recovery_error_ratio:
            incident, recovered = False, True
    traffic = "spike" if incident else "recovered" if recovered else "healthy"
    if n < thresholds.minimum_sample:
        traffic = "insufficientSample" if n else "noTraffic"
    quota = (
        "fresh"
        if snapshot.active_accounts > 0 and snapshot.fresh_accounts == snapshot.active_accounts
        else "stale_or_missing"
    )
    capacity = (
        "unknown"
        if quota != "fresh"
        else "low"
        if snapshot.remaining_weight <= thresholds.low_capacity_weight
        else "normal"
    )
    concentration = (
        "unknown"
        if snapshot.top_share is None
        else "high"
        if snapshot.top_share >= thresholds.high_concentration
        else "normal"
    )
    current = [
        traffic,
        capacity,
        quota,
        concentration,
        snapshot.health,
        "capped" if snapshot.logs_capped else "complete",
    ]
    prior = (
        [
            previous.traffic,
            previous.capacity,
            previous.quota,
            previous.concentration,
            previous.snapshot.health,
            "capped" if previous.snapshot.logs_capped else "complete",
        ]
        if previous
        else [None] * 6
    )
    events = [
        Event(code=f"{name}.{now}")
        for name, now, before in zip(
            ["traffic", "capacity", "quota", "concentration", "health", "logs"], current, prior
        )
        if now != before
    ]
    if bool(snapshot.no_accounts) != bool(previous.snapshot.no_accounts if previous else 0):
        events.append(Event(code="no_accounts.present" if snapshot.no_accounts else "no_accounts.cleared"))
    seen_logs = set(previous.seen_logs) if previous else set()
    seen_exceptions = set(previous.seen_exceptions) if previous else set()
    for code, values, seen in [
        ("isolation.logged_change", snapshot.log_ids, seen_logs),
        ("exception.new", snapshot.exception_ids, seen_exceptions),
    ]:
        new = set(values) - seen
        if new:
            events.append(Event(code=code, count=len(new)))
        seen.update(values)
    return Analysis(
        at=snapshot.at,
        snapshot=snapshot,
        traffic=traffic,
        incident=incident,
        recovered=recovered,
        success_ratio=snapshot.successes / n if n else None,
        error_ratio=error,
        capacity=capacity,
        quota=quota,
        concentration=concentration,
        seen_logs=sorted(seen_logs),
        seen_exceptions=sorted(seen_exceptions),
        events=events,
        rows_delta=snapshot.rows - previous.snapshot.rows if previous else None,
        requests_delta=n - previous.snapshot.requests if previous else None,
    )


def parse_logs(text: str) -> tuple[list[str], list[str]]:
    """Hash only timestamp/account/level/duration tuples and exception class names."""
    isolation, exceptions = set(), set()
    for line in text.splitlines():
        match = re.search(
            r"Account overload isolation engaged account_id=(\S+) level=(\d+) isolation_seconds=([0-9.]+)", line
        )
        if match:
            isolation.add(hashlib.sha256(json.dumps([line.split()[0], *match.groups()]).encode()).hexdigest())
        match = re.search(r"\b([A-Za-z_][\w.]*(?:Error|Exception)):", line)
        if match:
            exceptions.add(hashlib.sha256(match[1].encode()).hexdigest())
    return sorted(isolation), sorted(exceptions)


class Record(BaseModel):
    model_config = ConfigDict(extra="forbid")
    target: str
    analysis: Analysis


def observe(snapshot: Snapshot, journal: Path, target: str) -> Analysis | None:
    """Lock, recover, append and fsync before emitting anything; fail on corruption."""
    journal.parent.mkdir(parents=True, exist_ok=True)
    fd = os.open(journal, os.O_RDWR | os.O_CREAT | os.O_APPEND | os.O_NOFOLLOW, 0o600)
    with os.fdopen(fd, "r+") as stream:
        fcntl.flock(stream, fcntl.LOCK_EX | fcntl.LOCK_NB)
        os.fchmod(stream.fileno(), 0o600)
        previous = None
        for line in stream:
            record = Record.model_validate_json(line)
            if not line.endswith("\n") or record.target != target:
                raise ValueError("incomplete journal or target mismatch")
            if previous and record.analysis.at <= previous.at:
                raise ValueError("non-monotonic journal")
            previous = record.analysis
        if previous and snapshot.at <= previous.at:
            if snapshot == previous.snapshot:
                return None
            raise ValueError("non-monotonic observation")
        result = analyze(snapshot, previous, Thresholds())
        stream.write(Record(target=target, analysis=result).model_dump_json() + "\n")
        stream.flush()
        os.fsync(stream.fileno())
    directory = os.open(journal.parent, os.O_RDONLY | os.O_DIRECTORY)
    try:
        os.fsync(directory)
    finally:
        os.close(directory)
    return result


class NoRedirect(urllib.request.HTTPRedirectHandler):
    def redirect_request(self, req, fp, code, msg, headers, newurl):
        raise urllib.error.HTTPError(req.full_url, code, "redirect refused", headers, fp)


def collect(container: str, base_url: str, at: datetime) -> Snapshot:
    """Read only the fixed B database, bounded Docker logs, and unauthenticated readiness."""
    lo = at - timedelta(seconds=300)

    def sqltime(d: datetime) -> str:
        return d.astimezone(UTC).replace(tzinfo=None).isoformat(" ", timespec="microseconds")

    db = subprocess.run(
        ["docker", "exec", "-i", container, "python", "-", "/var/lib/codex-lb/store.db", sqltime(lo), sqltime(at)],
        input=REMOTE,
        capture_output=True,
        text=True,
        timeout=15,
        check=True,
    )
    logs = subprocess.run(
        [
            "docker",
            "logs",
            "--since",
            lo.isoformat(),
            "--until",
            at.isoformat(),
            "--tail",
            "2000",
            "--timestamps",
            container,
        ],
        capture_output=True,
        text=True,
        timeout=10,
        check=True,
    )
    lines = (logs.stdout + logs.stderr).splitlines()
    isolation, exceptions = parse_logs("\n".join(lines))
    health = "down"
    opener = urllib.request.build_opener(urllib.request.ProxyHandler({}), NoRedirect())
    try:
        with opener.open(base_url.rstrip("/") + "/health/ready", timeout=5) as response:
            data = json.loads(response.read(4097))
            checks = data.get("checks") if isinstance(data, dict) else None
            if response.status == 200 and isinstance(checks, dict) and checks.get("database") == "ok":
                health = "up"
    except (urllib.error.URLError, TimeoutError, OSError, TypeError, ValueError):
        health = "down"
    return Snapshot.model_validate(
        {
            **json.loads(db.stdout),
            "at": at,
            "health": health,
            "log_ids": isolation,
            "exception_ids": exceptions,
            "logs_capped": len(lines) >= 2000,
        }
    )


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--container", required=True)
    parser.add_argument("--base-url", required=True)
    parser.add_argument("--journal", type=Path, required=True)
    parser.add_argument("--once", action="store_true")
    parser.add_argument("--snapshot", type=Path, help="sanitized Snapshot JSON; requires --once, no I/O to B")
    parser.add_argument("--interval", type=float, default=300)
    args = parser.parse_args()
    url = urllib.parse.urlsplit(args.base_url)
    if (
        url.scheme not in {"http", "https"}
        or not url.hostname
        or url.username
        or url.password
        or url.query
        or url.fragment
        or url.path not in {"", "/"}
        or not re.fullmatch(r"[A-Za-z0-9][A-Za-z0-9_.-]*", args.container)
        or not 0 < args.interval <= 86400
        or (args.snapshot and not args.once)
    ):
        raise ValueError("invalid observer configuration")
    target = hashlib.sha256(
        json.dumps([args.container, args.base_url.rstrip("/"), bool(args.snapshot)]).encode()
    ).hexdigest()
    while True:
        snapshot = (
            Snapshot.model_validate_json(args.snapshot.read_text())
            if args.snapshot
            else collect(args.container, args.base_url, datetime.now(UTC))
        )
        result = observe(snapshot, args.journal, target)
        if result:
            for event in result.events:
                print(event.model_dump_json(), flush=True)
        if args.once:
            return 0
        time.sleep(args.interval)


if __name__ == "__main__":
    try:
        sys.exit(main())
    except KeyboardInterrupt:
        sys.exit(130)
    except (ValueError, OSError, subprocess.SubprocessError):
        # Boundary errors may contain Docker logs, credentials, or untrusted fixture data.
        print('{"code":"observer.failed"}', file=sys.stderr)
        sys.exit(2)
