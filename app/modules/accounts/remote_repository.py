from __future__ import annotations

from dataclasses import dataclass

from sqlalchemy import select, update
from sqlalchemy.ext.asyncio import AsyncSession

from app.core.crypto import TokenEncryptor
from app.core.utils.time import to_utc_naive
from app.db.models import Account, AccountStatus
from app.db.session import sqlite_writer_section
from app.modules.accounts.remote_source import SourceExport, SourceSnapshot
from app.modules.accounts.repository import AccountsRepository

SOURCE_STATE_PREFIX = "remote_source:"


@dataclass(frozen=True)
class SyncResult:
    updated: int
    disabled: int
    accounts: tuple[Account, ...]


class RemoteCredentialsRepository:
    def __init__(self, session: AsyncSession) -> None:
        self._session = session
        self._encryptor = TokenEncryptor()

    async def scrub_refresh_tokens(self) -> None:
        # Dedicated B may boot with an older imported cache. Scrub before serving,
        # even if A is down. This is not a native credential store in this mode.
        async with sqlite_writer_section():
            await self._session.execute(update(Account).values(refresh_token_encrypted=self._encryptor.encrypt("")))
            await self._session.commit()

    async def apply(self, snapshot: SourceSnapshot, exports: dict[str, SourceExport]) -> SyncResult:
        async with sqlite_writer_section():
            repo = AccountsRepository(self._session)
            rows = {row.id: row for row in await repo.list_accounts(refresh_existing=True)}
            updated = 0
            disabled = 0
            source_ids = {item.account_id for item in snapshot.accounts}
            pending_delete_ids = set(
                await self._session.scalars(
                    select(Account.id).where(Account.delete_requested_at.is_not(None))
                )
            )
            for item in snapshot.accounts:
                if item.account_id in pending_delete_ids:
                    continue
                row = rows.get(item.account_id)
                exported = exports.get(item.account_id)
                credentials_replaced = False
                local_pause = row is not None and row.status == AccountStatus.PAUSED and row.deactivation_reason is None
                if row is None:
                    # Insert the source ID exactly; never identity-merge/upsert.
                    if exported is None:
                        continue
                    row = Account(
                        id=item.account_id,
                        email=item.email,
                        plan_type=item.plan_type,
                        access_token_encrypted=self._encryptor.encrypt(exported.tokens.access_token.get_secret_value()),
                        refresh_token_encrypted=self._encryptor.encrypt(""),
                        id_token_encrypted=self._encryptor.encrypt(exported.tokens.id_token.get_secret_value()),
                        last_refresh=to_utc_naive(exported.codex_auth_json.last_refresh),
                        status=item.status,
                    )
                    self._session.add(row)
                    await self._session.flush()
                    rows[row.id] = row
                    updated += 1
                elif exported is not None:
                    incoming_refresh = to_utc_naive(exported.codex_auth_json.last_refresh)
                    if incoming_refresh >= to_utc_naive(row.last_refresh):
                        credentials_replaced = (
                            self._encryptor.decrypt(row.access_token_encrypted)
                            != exported.tokens.access_token.get_secret_value()
                        )
                        changed = await self._session.scalar(
                            update(Account)
                            .where(
                                Account.id == row.id,
                                Account.access_token_encrypted == row.access_token_encrypted,
                                Account.last_refresh <= incoming_refresh,
                                Account.delete_requested_at.is_(None),
                            )
                            .values(
                                access_token_encrypted=self._encryptor.encrypt(
                                    exported.tokens.access_token.get_secret_value()
                                ),
                                refresh_token_encrypted=self._encryptor.encrypt(""),
                                id_token_encrypted=self._encryptor.encrypt(exported.tokens.id_token.get_secret_value()),
                                last_refresh=incoming_refresh,
                            )
                            .returning(Account.id)
                        )
                        updated += int(changed is not None)
                        credentials_replaced = credentials_replaced and changed is not None
                row.chatgpt_account_id = item.chatgpt_account_id
                row.email = item.email
                row.plan_type = item.plan_type
                row.workspace_id = item.workspace_id
                row.workspace_label = item.workspace_label
                row.seat_type = item.seat_type
                # A local pause (no source marker) survives every source state.
                if local_pause:
                    continue
                source_owned = (row.deactivation_reason or "").startswith(SOURCE_STATE_PREFIX)
                if (
                    item.status == AccountStatus.ACTIVE
                    and row.status != AccountStatus.ACTIVE
                    and not source_owned
                    and not credentials_replaced
                ):
                    continue  # unchanged source credentials do not repair local upstream health
                reason = None if item.status == AccountStatus.ACTIVE else SOURCE_STATE_PREFIX + item.status.value
                if row.status != item.status or row.deactivation_reason != reason:
                    disabled += int(item.status in {AccountStatus.PAUSED, AccountStatus.DEACTIVATED})
                reset = item.reset_at_primary or item.reset_at_secondary
                await self._session.execute(
                    update(Account)
                    .where(
                        Account.id == row.id,
                        Account.status == row.status,
                        Account.deactivation_reason == row.deactivation_reason,
                        Account.delete_requested_at.is_(None),
                    )
                    .values(
                        status=item.status,
                        deactivation_reason=reason,
                        reset_at=int(reset.timestamp()) if reset is not None else None,
                    )
                )
            for row in rows.values():
                if row.id not in source_ids:
                    # Soft-disable, retaining accounting and stable identity.
                    if row.status != AccountStatus.DEACTIVATED:
                        disabled += 1
                    if row.status != AccountStatus.PAUSED or row.deactivation_reason is not None:
                        await self._session.execute(
                            update(Account)
                            .where(
                                Account.id == row.id,
                                Account.status == row.status,
                                Account.deactivation_reason == row.deactivation_reason,
                            )
                            .values(
                                status=AccountStatus.DEACTIVATED, deactivation_reason=SOURCE_STATE_PREFIX + "removed"
                            )
                        )
            await self._session.commit()
            result = await self._session.scalars(select(Account).execution_options(populate_existing=True))
            accounts = tuple(result.all())
            self._session.expunge_all()
            return SyncResult(updated, disabled, accounts)
