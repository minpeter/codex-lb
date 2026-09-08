from __future__ import annotations

import asyncio
import contextlib
import logging
import time
from collections.abc import AsyncIterator

from sqlalchemy.exc import SQLAlchemyError

from app.core.auth import token_expiry_epoch_ms
from app.core.auth.refresh import RefreshError, get_token_refresh_timeout_override
from app.core.config.settings import Settings, get_settings
from app.core.crypto import TokenEncryptor
from app.core.utils.shared_future import wait_on_shared_future
from app.core.utils.time import to_utc_naive
from app.db.models import Account, AccountStatus
from app.db.session import get_background_session
from app.modules.accounts.remote_repository import RemoteCredentialsRepository, SyncResult
from app.modules.accounts.remote_source import RemoteCredentialClient, SourceExport, SourceSnapshot, source_unavailable
from app.modules.accounts.repository import AccountsRepository
from app.modules.proxy.account_cache import (
    clear_account_routing_unavailable,
    get_account_selection_cache,
    mark_account_routing_unavailable,
    propagate_account_routing_change,
)

logger = logging.getLogger(__name__)
_source: RemoteCredentials | None = None


def remote_mode_enabled() -> bool:
    return get_settings().remote_credential_source_url is not None


def get_remote_credentials() -> RemoteCredentials:
    if _source is None:
        raise source_unavailable()
    return _source


def set_remote_credentials(source: RemoteCredentials | None) -> None:
    global _source
    _source = source


class RemoteCredentials:
    """One dedicated B authority. Serialize source reads through their commit.

    Missing source accounts are soft-disabled, never deleted, preserving history.
    All detached refresh tasks own database sessions and are drained on shutdown.
    """

    def __init__(self, settings: Settings) -> None:
        self._settings = settings
        self.client = RemoteCredentialClient(settings)
        self._lock = asyncio.Lock()
        self._snapshot: SourceSnapshot | None = None
        self._flights: dict[str, asyncio.Task[Account]] = {}
        self._failures: dict[str, float] = {}
        self._source_retry_at = 0.0
        self._scheduler: asyncio.Task[None] | None = None
        self._closed = False
        self._encryptor = TokenEncryptor()

    async def start(self) -> None:
        try:
            async with get_background_session() as session:
                await RemoteCredentialsRepository(session).scrub_refresh_tokens()
        except BaseException:
            await self.client.close()
            raise
        self._scheduler = asyncio.create_task(self._run(), name="remote-credential-sync")

    async def close(self) -> None:
        self._closed = True
        tasks = list(self._flights.values())
        for task in tasks:
            task.cancel()
        if self._scheduler is not None:
            self._scheduler.cancel()
        if tasks:
            await asyncio.gather(*tasks, return_exceptions=True)
        if self._scheduler is not None:
            await asyncio.gather(self._scheduler, return_exceptions=True)
        await self.client.close()
        if _source is self:
            set_remote_credentials(None)

    async def _run(self) -> None:
        while True:
            try:
                await self.sync()
            except RefreshError:
                logger.warning("Remote credential synchronization unavailable; cached state retained")
            await asyncio.sleep(self._settings.remote_credential_source_sync_interval_seconds)

    def _usable(self, account: Account, *, margin_seconds: float = 30.0) -> bool:
        expiry = token_expiry_epoch_ms(self._encryptor.decrypt(account.access_token_encrypted))
        return expiry is not None and expiry > (time.time() + margin_seconds) * 1000

    def _source_allows(self, account_id: str) -> bool:
        if self._snapshot is None:
            return True  # cached boot during source outage
        return any(
            item.account_id == account_id
            and item.status
            not in {
                AccountStatus.PAUSED,
                AccountStatus.DEACTIVATED,
            }
            for item in self._snapshot.accounts
        )

    async def ensure_fresh(self, account: Account, *, force: bool = False) -> Account:
        if self._closed or not self._source_allows(account.id):
            raise source_unavailable()
        if not force and self._usable(account, margin_seconds=0):
            return account
        if self._failures.get(account.id, 0) > time.monotonic():
            raise source_unavailable()
        task = self._flights.get(account.id)
        if task is None:
            task = asyncio.create_task(self._refresh(account.id), name="remote-credential-refresh")
            self._flights[account.id] = task
            task.add_done_callback(lambda done: self._settle(account.id, done))
        budget = get_token_refresh_timeout_override()
        try:
            async with asyncio.timeout(budget):
                return await wait_on_shared_future(task)
        except TimeoutError:
            raise source_unavailable() from None

    def _settle(self, account_id: str, task: asyncio.Task[Account]) -> None:
        self._flights.pop(account_id, None)
        if not task.cancelled():
            task.exception()  # retrieve failures even when every waiter disconnected

    async def _refresh(self, account_id: str) -> Account:
        try:
            result = await self.sync(force_account=account_id)
            for account in result.accounts:
                if (
                    account.id == account_id
                    and self._source_allows(account_id)
                    and self._usable(account, margin_seconds=0)
                ):
                    return account
            raise source_unavailable()
        except RefreshError:
            self._failures[account_id] = time.monotonic() + 5.0
            raise

    async def sync(self, *, force_account: str | None = None) -> SyncResult:
        try:
            async with asyncio.timeout(self._settings.remote_credential_source_timeout_seconds), self._lock:
                if self._closed or time.monotonic() < self._source_retry_at:
                    raise source_unavailable()
                snapshot = await self.client.snapshot()
                async with get_background_session() as session:
                    rows = {row.id: row for row in await AccountsRepository(session).list_accounts()}
                    session.expunge_all()
                exports: dict[str, SourceExport] = {}
                pending: list[str] = []
                for item in snapshot.accounts:
                    row = rows.get(item.account_id)
                    if (
                        row is None
                        or item.account_id == force_account
                        or to_utc_naive(item.last_refresh_at) > to_utc_naive(row.last_refresh)
                        or not self._usable(row)
                    ):
                        pending.append(item.account_id)
                queue = iter(pending)

                async def export_worker() -> None:
                    for account_id in queue:
                        exports[account_id] = await self.client.export(account_id)

                # Four network workers; no DB session exists while they run.
                workers = [asyncio.create_task(export_worker()) for _ in range(min(4, len(pending)))]
                try:
                    await asyncio.gather(*workers)
                finally:
                    for worker in workers:
                        worker.cancel()
                    await asyncio.gather(*workers, return_exceptions=True)
                async with get_background_session() as session:
                    result = await RemoteCredentialsRepository(session).apply(snapshot, exports)
                self._snapshot = snapshot
                for account in result.accounts:
                    if account.status in {AccountStatus.PAUSED, AccountStatus.DEACTIVATED}:
                        mark_account_routing_unavailable(account.id)
                    else:
                        clear_account_routing_unavailable(account.id)
                get_account_selection_cache().invalidate()
                await propagate_account_routing_change()
                logger.info("Remote credential sync completed updated=%d disabled=%d", result.updated, result.disabled)
                return result
        except (RefreshError, TimeoutError, SQLAlchemyError):
            if time.monotonic() >= self._source_retry_at:
                self._source_retry_at = time.monotonic() + 5.0
            raise source_unavailable() from None

    @contextlib.asynccontextmanager
    async def local_reactivation(self, account_id: str) -> AsyncIterator[None]:
        async with self._lock:
            if self._snapshot is None or not self._source_allows(account_id):
                raise source_unavailable()
            yield
