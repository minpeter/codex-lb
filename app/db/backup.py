from __future__ import annotations

import re
from datetime import datetime, timezone
from pathlib import Path

from app.db.sqlite_utils import sqlite_connection


def _backup_name(source: Path, timestamp: datetime) -> str:
    return f"{source.stem}.pre-migrate-{timestamp.strftime('%Y%m%dT%H%M%SZ')}{source.suffix}"


def _next_backup_path(source: Path, timestamp: datetime) -> Path:
    base_name = _backup_name(source, timestamp)
    candidate = source.parent / base_name
    prefix = f"{source.stem}.pre-migrate-{timestamp.strftime('%Y%m%dT%H%M%SZ')}-"
    numbered = [
        int(path.name.removeprefix(prefix).removesuffix(source.suffix))
        for path in source.parent.glob(f"{prefix}*{source.suffix}")
        if path.name.removeprefix(prefix).removesuffix(source.suffix).isdigit()
    ]
    if not candidate.exists() and not numbered:
        return candidate

    sequence = max(numbered, default=0) + 1
    return source.parent / f"{prefix}{sequence}{source.suffix}"


def list_sqlite_pre_migration_backups(source: Path) -> list[Path]:
    pattern = f"{source.stem}.pre-migrate-*{source.suffix}"
    return sorted((path for path in source.parent.glob(pattern) if path.is_file()))


def _sqlite_backup(source: Path, backup_path: Path) -> None:
    source_mode = source.stat().st_mode
    with sqlite_connection(source) as source_conn:
        with sqlite_connection(backup_path) as backup_conn:
            source_conn.backup(backup_conn)
            backup_conn.execute("PRAGMA journal_mode=DELETE")
    backup_path.chmod(source_mode)


def create_sqlite_pre_migration_backup(
    source: Path,
    *,
    max_files: int,
    now: datetime | None = None,
) -> Path:
    if max_files < 1:
        raise ValueError("max_files must be >= 1")
    if not source.exists():
        raise FileNotFoundError(f"sqlite database not found: {source}")

    timestamp = now or datetime.now(timezone.utc)
    backup_path = _next_backup_path(source, timestamp)

    _sqlite_backup(source, backup_path)

    backups = list_sqlite_pre_migration_backups(source)

    def backup_order(path: Path) -> tuple[str, int]:
        match = re.search(r"-(\d+)" + re.escape(source.suffix) + "$", path.name)
        base = path.name.removesuffix(source.suffix)
        if match:
            base = base[: -len(match.group(1)) - 1]
        return (base, int(match.group(1)) if match else 0)

    backups.sort(key=backup_order)
    old_backups = [backup for backup in backups if backup != backup_path]
    excess = len(backups) - max_files
    for old_backup in old_backups[:max(0, excess)]:
        old_backup.unlink()

    return backup_path
