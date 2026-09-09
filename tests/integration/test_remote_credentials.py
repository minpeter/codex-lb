from __future__ import annotations

import asyncio
import base64
import json
from dataclasses import dataclass, field
from datetime import datetime
from typing import cast

import pytest
from aiohttp import web
from aiohttp.test_utils import TestServer
from httpx import ASGITransport, AsyncClient

from app.core.auth import refresh as refresh_module
from app.core.auth.refresh import RefreshError
from app.core.config.settings import get_settings
from app.core.crypto import TokenEncryptor
from app.db.models import Account, AccountStatus
from app.db.session import get_background_session
from app.modules.accounts import remote_credentials as remote_module
from app.modules.accounts.auth_manager import AccountsRepositoryPort, AuthManager
from app.modules.accounts.remote_credentials import RemoteCredentials, set_remote_credentials
from app.modules.accounts.repository import AccountsRepository
from app.modules.accounts.service import AccountsService, AccountStateTransitionError
from app.modules.proxy.account_cache import get_account_selection_cache, is_account_routing_unavailable

pytestmark = pytest.mark.integration


def token(version: str = "one", *, expired: bool = False) -> str:
    payload = base64.urlsafe_b64encode(json.dumps({"exp": 1 if expired else 4102444800, "v": version}).encode())
    return "e30." + payload.decode().rstrip("=") + ".fake"


@dataclass
class Source:
    accounts: list[dict] = field(
        default_factory=lambda: [
            {
                "accountId": "source-id",
                "chatgptAccountId": "chatgpt-id",
                "email": "fake@example.com",
                "planType": "plus",
                "status": "active",
                "lastRefreshAt": "2026-01-01T00:00:00Z",
            }
        ]
    )
    access: str = field(default_factory=token)
    last_refresh: str = "2026-01-01T00:00:00Z"
    session: str = "session-one"
    logins: int = 0
    lists: int = 0
    exports: int = 0
    exchanges: int = 0
    failure_status: int = 200
    malformed: bool = False
    reject_sessions: bool = False
    export_started: asyncio.Event = field(default_factory=asyncio.Event)
    export_release: asyncio.Event = field(default_factory=asyncio.Event)
    hold_export: bool = False
    four_exports_started: asyncio.Event = field(default_factory=asyncio.Event)

    async def login(self, request: web.Request) -> web.Response:
        assert await request.json() == {"password": "fake-dashboard-password"}
        self.logins += 1
        response = web.json_response({"authenticated": True, "role": "admin", "totpRequiredOnLogin": False})
        response.set_cookie("dashboard_session", self.session)
        return response

    async def listing(self, request: web.Request) -> web.Response:
        self.lists += 1
        if self.reject_sessions or request.cookies.get("dashboard_session") != self.session:
            return web.Response(status=401)
        if self.failure_status != 200:
            return web.Response(status=self.failure_status, text="fake-secret-must-not-leak")
        return web.json_response({} if self.malformed else {"accounts": self.accounts})

    async def export(self, request: web.Request) -> web.Response:
        if request.cookies.get("dashboard_session") != self.session:
            return web.Response(status=401)
        self.exports += 1
        access, last_refresh = self.access, self.last_refresh
        if self.exports == 4:
            self.four_exports_started.set()
        self.export_started.set()
        if self.hold_export:
            await self.export_release.wait()
        return web.json_response(
            {
                "account": {"accountId": request.match_info["account_id"]},
                "tokens": {"accessToken": access, "idToken": "fake-id", "refreshToken": "fake-source-refresh"},
                "codexAuthJson": {"last_refresh": last_refresh, "tokens": {"refresh_token": "fake-source-refresh"}},
            }
        )

    async def exchange(self, request: web.Request) -> web.Response:
        self.exchanges += 1
        return web.json_response({"access_token": "wrong-native-token"})


@pytest.fixture
async def source(monkeypatch):
    state = Source()
    app = web.Application()
    app.router.add_post("/source/api/dashboard-auth/password/login", state.login)
    app.router.add_get("/source/api/accounts", state.listing)
    app.router.add_post("/source/api/accounts/{account_id}/export/auth", state.export)
    app.router.add_post("/oauth/token", state.exchange)
    async with TestServer(app) as server:
        monkeypatch.setenv("CODEX_LB_REMOTE_CREDENTIAL_SOURCE_URL", str(server.make_url("/source")))
        monkeypatch.setenv("CODEX_LB_REMOTE_CREDENTIAL_SOURCE_PASSWORD", "fake-dashboard-password")
        monkeypatch.setattr(refresh_module, "AUTH_BASE_URL", str(server.make_url("")).rstrip("/"))
        get_settings.cache_clear()
        yield state
    get_settings.cache_clear()


@pytest.fixture
async def replica(source, db_setup):
    remote = RemoteCredentials(get_settings())
    set_remote_credentials(remote)
    try:
        yield remote
    finally:
        await remote.close()


async def stored() -> Account:
    async with get_background_session() as session:
        result = await AccountsRepository(session).get_by_id("source-id")
        assert result is not None
        session.expunge_all()
        return result


async def test_source_outage_keeps_cached_token_until_actual_expiry(replica, source, monkeypatch):
    await replica.sync()
    account = await stored()
    source.failure_status = 503
    monkeypatch.setattr(remote_module.time, "time", lambda: 4102444790.0)

    result = await replica.ensure_fresh(account)

    assert TokenEncryptor().decrypt(result.access_token_encrypted) == source.access
    assert source.exchanges == 0
    monkeypatch.setattr(remote_module.time, "time", lambda: 4102444801.0)
    with pytest.raises(RefreshError):
        await replica.ensure_fresh(account)
    assert source.exchanges == 0


async def test_sync_creates_source_identity_without_refresh_material(replica, source):
    generation = get_account_selection_cache().generation
    await replica.sync()
    row = await stored()
    assert row.id == "source-id"
    assert TokenEncryptor().decrypt(row.access_token_encrypted) == source.access
    assert TokenEncryptor().decrypt(row.refresh_token_encrypted) == ""
    assert row.last_refresh == datetime(2026, 1, 1)
    assert get_account_selection_cache().generation > generation


async def test_unchanged_snapshot_does_not_export_again(replica, source):
    await replica.sync()
    await replica.sync()
    assert source.exports == 1
    assert (await stored()).last_refresh == datetime(2026, 1, 1)


async def test_rotation_updates_existing_row_and_preserves_local_pause(replica, source):
    await replica.sync()
    async with get_background_session() as session:
        await AccountsRepository(session).update_status("source-id", AccountStatus.PAUSED)
    source.last_refresh = source.accounts[0]["lastRefreshAt"] = "2026-02-01T00:00:00Z"
    source.access = token("two")
    await replica.sync()
    row = await stored()
    assert row.status == AccountStatus.PAUSED
    assert TokenEncryptor().decrypt(row.access_token_encrypted) == source.access
    assert row.last_refresh == datetime(2026, 2, 1)


@pytest.mark.parametrize("status", ["paused", "deactivated", "reauth_required", "active"])
async def test_source_state_propagates(replica, source, status):
    await replica.sync()
    source.accounts[0]["status"] = status
    await replica.sync()
    assert (await stored()).status.value == status
    assert is_account_routing_unavailable("source-id") == (status in {"paused", "deactivated"})


async def test_source_reactivation_propagates(replica, source):
    source.accounts[0]["status"] = "paused"
    await replica.sync()
    source.accounts[0]["status"] = "active"
    await replica.sync()
    assert (await stored()).status == AccountStatus.ACTIVE
    assert not is_account_routing_unavailable("source-id")


async def test_pending_delete_snapshot_does_not_recreate_account(replica, source):
    await replica.sync()
    async with get_background_session() as session:
        row = await AccountsRepository(session).get_by_id("source-id")
        assert row is not None
        row.delete_requested_at = datetime(2026, 1, 2)
        await session.commit()

    await replica.sync()

    async with get_background_session() as session:
        row = await AccountsRepository(session).get_by_id("source-id")
        assert row is not None
        assert row.delete_requested_at == datetime(2026, 1, 2)


async def test_valid_empty_snapshot_disables_without_deleting(replica, source):
    await replica.sync()
    source.accounts.clear()
    await replica.sync()
    assert (await stored()).status == AccountStatus.DEACTIVATED
    assert is_account_routing_unavailable("source-id")


@pytest.mark.parametrize("malformed", [False, True])
async def test_source_failure_preserves_cached_state(replica, source, malformed):
    await replica.sync()
    previous = await stored()
    source.malformed = malformed
    source.failure_status = 200 if malformed else 503
    with pytest.raises(RefreshError) as error:
        await replica.sync()
    assert not error.value.is_permanent
    current = await stored()
    assert current.status == previous.status
    assert current.access_token_encrypted == previous.access_token_encrypted
    manager = AuthManager(cast(AccountsRepositoryPort, None))
    assert await manager.ensure_fresh(current) is current


async def test_session_expiry_relogs_once(replica, source):
    await replica.sync()
    source.session = "session-two"
    await replica.sync()
    assert source.logins == 2
    assert source.lists == 3


async def test_persistent_401_has_one_retry_and_cooldown(replica, source):
    source.reject_sessions = True
    for _ in range(2):
        with pytest.raises(RefreshError):
            await replica.sync()
    assert source.logins == 2
    assert source.lists == 2


@pytest.mark.parametrize("entry", ["ensure_fresh", "refresh_account", "_run_refresh"])
async def test_forced_and_background_refresh_use_source_not_oauth(replica, source, entry):
    await replica.sync()
    row = await stored()
    source.access = token("forced")
    manager = AuthManager(cast(AccountsRepositoryPort, None))
    method = getattr(manager, entry)
    result = await method(row, **({"force": True} if entry == "ensure_fresh" else {}))
    assert TokenEncryptor().decrypt(result.access_token_encrypted) == source.access
    assert source.exports == 2
    assert source.exchanges == 0


async def test_low_level_refresh_never_exchanges_over_http(replica, source):
    with pytest.raises(RefreshError) as error:
        await refresh_module.refresh_access_token("fake-source-refresh", allow_direct_egress=True)
    assert not error.value.is_permanent
    assert source.exchanges == 0


async def test_unchanged_expired_access_is_transient_and_cooled_down(replica, source):
    source.access = token(expired=True)
    await replica.sync()
    row = await stored()
    for _ in range(2):
        with pytest.raises(RefreshError) as error:
            await replica.ensure_fresh(row)
        assert not error.value.is_permanent
    assert source.exports == 2
    assert (await stored()).status == AccountStatus.ACTIVE


async def test_source_paused_account_cannot_be_locally_reactivated(replica, source):
    source.accounts[0]["status"] = "paused"
    await replica.sync()
    async with get_background_session() as session:
        with pytest.raises(AccountStateTransitionError):
            await AccountsService(AccountsRepository(session)).reactivate_account("source-id")
    assert (await stored()).status == AccountStatus.PAUSED


@pytest.mark.parametrize("guarded", [False, True])
async def test_stale_background_state_write_cannot_reactivate_source_pause(replica, source, guarded):
    source.accounts[0]["status"] = "paused"
    await replica.sync()
    row = await stored()
    async with get_background_session() as session:
        repo = AccountsRepository(session)
        if guarded:
            changed = await repo.update_status_if_current(
                row.id,
                AccountStatus.ACTIVE,
                expected_status=row.status,
                expected_deactivation_reason=row.deactivation_reason,
            )
        else:
            changed = await repo.update_status(row.id, AccountStatus.ACTIVE)
    assert changed is False
    assert (await stored()).status == AccountStatus.PAUSED


@pytest.mark.parametrize(
    "status",
    [
        AccountStatus.RATE_LIMITED,
        AccountStatus.QUOTA_EXCEEDED,
        AccountStatus.REAUTH_REQUIRED,
        AccountStatus.DEACTIVATED,
    ],
)
async def test_unchanged_source_active_preserves_local_health(replica, status):
    await replica.sync()
    async with get_background_session() as session:
        await AccountsRepository(session).update_status("source-id", status, "local-health", reset_at=4102444800)
    await replica.sync()
    row = await stored()
    assert row.status == status
    assert row.deactivation_reason == "local-health"
    assert row.reset_at == 4102444800


async def test_initial_sync_uses_four_bounded_export_workers(replica, source):
    source.accounts = [dict(source.accounts[0], accountId=f"source-{i}") for i in range(19)]
    source.hold_export = True
    async with asyncio.TaskGroup() as tasks:
        sync = tasks.create_task(replica.sync())
        await asyncio.wait_for(source.four_exports_started.wait(), 2)
        assert source.exports == 4
        source.export_release.set()
    assert len(sync.result().accounts) == 19


async def test_concurrent_waiters_share_owned_refresh_after_cancellation(replica, source, monkeypatch):
    await replica.sync()
    row = await stored()
    source.export_started.clear()
    source.hold_export = True
    source.access = token("shared")
    joined = asyncio.Event()
    waiters = 0
    original_wait = remote_module.wait_on_shared_future

    async def observe_wait(task):
        nonlocal waiters
        waiters += 1
        if waiters == 2:
            joined.set()
        return await original_wait(task)

    monkeypatch.setattr(remote_module, "wait_on_shared_future", observe_wait)
    async with asyncio.TaskGroup() as tasks:
        first = tasks.create_task(replica.ensure_fresh(row, force=True))
        await asyncio.wait_for(source.export_started.wait(), 2)
        second = tasks.create_task(replica.ensure_fresh(row, force=True))
        await asyncio.wait_for(joined.wait(), 2)
        first.cancel()
        source.export_release.set()
    assert first.cancelled()
    assert TokenEncryptor().decrypt(second.result().access_token_encrypted) == source.access
    assert TokenEncryptor().decrypt((await stored()).access_token_encrypted) == source.access
    assert source.exports == 2
    assert source.exchanges == 0


async def test_scheduler_and_request_cannot_commit_credentials_backwards(replica, source):
    await replica.sync()
    row = await stored()
    source.export_started.clear()
    source.hold_export = True
    async with asyncio.TaskGroup() as tasks:
        tasks.create_task(replica.ensure_fresh(row, force=True))
        await asyncio.wait_for(source.export_started.wait(), 2)
        source.last_refresh = source.accounts[0]["lastRefreshAt"] = "2026-02-01T00:00:00Z"
        source.access = token("latest")
        entered = asyncio.Event()

        async def scheduled_sync():
            entered.set()
            await replica.sync()

        tasks.create_task(scheduled_sync())
        await asyncio.wait_for(entered.wait(), 2)
        source.export_release.set()
    assert TokenEncryptor().decrypt((await stored()).access_token_encrypted) == source.access
    assert (await stored()).last_refresh == datetime(2026, 2, 1)


async def test_new_credentials_can_repair_local_reauth(replica, source):
    await replica.sync()
    async with get_background_session() as session:
        await AccountsRepository(session).update_status("source-id", AccountStatus.REAUTH_REQUIRED, "local-health")
    source.last_refresh = source.accounts[0]["lastRefreshAt"] = "2026-02-01T00:00:00Z"
    source.access = token("repaired")
    await replica.sync()
    assert (await stored()).status == AccountStatus.ACTIVE


async def test_source_pause_overrides_local_quota(replica, source):
    await replica.sync()
    async with get_background_session() as session:
        await AccountsRepository(session).update_status("source-id", AccountStatus.QUOTA_EXCEEDED, "local-health")
    source.accounts[0]["status"] = "paused"
    await replica.sync()
    assert (await stored()).status == AccountStatus.PAUSED
    assert is_account_routing_unavailable("source-id")


async def test_real_app_lifecycle_lists_mirrors_and_rejects_local_creation(source, app_instance, monkeypatch):
    completed = asyncio.Event()
    original_sync = RemoteCredentials.sync

    async def observed_sync(self, **kwargs):
        result = await original_sync(self, **kwargs)
        completed.set()
        return result

    monkeypatch.setattr(RemoteCredentials, "sync", observed_sync)
    async with app_instance.router.lifespan_context(app_instance):
        await asyncio.wait_for(completed.wait(), 5)
        remote = remote_module.get_remote_credentials()
        async with AsyncClient(transport=ASGITransport(app=app_instance), base_url="http://testserver") as client:
            listing = await client.get("/api/accounts")
            assert listing.status_code == 200
            assert [row["accountId"] for row in listing.json()["accounts"]] == ["source-id"]
            imported = await client.post("/api/accounts/import", files={"auth_json": ("auth.json", b"{}")})
            assert imported.status_code == 400
            oauth = await client.post("/api/oauth/start", json={"forceMethod": "device"})
            assert oauth.status_code == 502
            assert oauth.json()["error"]["code"] == "remote_credential_mode"
    assert remote.client.session.closed
    assert remote._scheduler is not None and remote._scheduler.done()
    with pytest.raises(RefreshError):
        remote_module.get_remote_credentials()


async def test_cached_boot_scrubs_refresh_even_when_source_is_down(replica, source, monkeypatch):
    await replica.sync()
    async with get_background_session() as session:
        row = await AccountsRepository(session).get_by_id("source-id")
        assert row is not None
        row.refresh_token_encrypted = TokenEncryptor().encrypt("fake-legacy-refresh")
        await session.commit()
    source.failure_status = 503
    failed = asyncio.Event()
    original_sync = replica.sync

    async def observed_sync():
        try:
            return await original_sync()
        finally:
            failed.set()

    monkeypatch.setattr(replica, "sync", observed_sync)
    await replica.start()
    await asyncio.wait_for(failed.wait(), 2)
    row = await stored()
    assert TokenEncryptor().decrypt(row.refresh_token_encrypted) == ""
    assert row.status == AccountStatus.ACTIVE
    assert await replica.ensure_fresh(row) is row
