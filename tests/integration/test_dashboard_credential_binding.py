from __future__ import annotations

import pyotp
import pytest
from httpx import ASGITransport, AsyncClient

from app.core.auth import totp as totp_module
from app.core.auth.dashboard_mode import DashboardAuthMode
from app.core.config.settings import get_settings
from app.modules.dashboard_auth.service import DASHBOARD_SESSION_COOKIE

pytestmark = pytest.mark.integration


def _trusted_header_env(monkeypatch: pytest.MonkeyPatch) -> None:
    monkeypatch.setenv("CODEX_LB_DASHBOARD_AUTH_MODE", DashboardAuthMode.TRUSTED_HEADER)
    monkeypatch.setenv("CODEX_LB_FIREWALL_TRUST_PROXY_HEADERS", "true")
    monkeypatch.setenv("CODEX_LB_FIREWALL_TRUSTED_PROXY_CIDRS", "127.0.0.1/32")
    monkeypatch.setenv("CODEX_LB_DASHBOARD_AUTH_PROXY_HEADER", "Remote-User")
    monkeypatch.setenv("FORWARDED_ALLOW_IPS", "127.0.0.1")
    get_settings.cache_clear()


@pytest.mark.asyncio
async def test_trusted_proxy_xff_is_projected_but_raw_peer_authorizes(
    monkeypatch: pytest.MonkeyPatch, app_instance
) -> None:
    _trusted_header_env(monkeypatch)
    async with app_instance.router.lifespan_context(app_instance):
        transport = ASGITransport(app=app_instance, client=("127.0.0.1", 42000))
        async with AsyncClient(transport=transport, base_url="http://dashboard.example") as client:
            response = await client.get(
                "/api/settings",
                headers={"Remote-User": "proxy-user", "X-Forwarded-For": "203.0.113.9"},
            )

    assert response.status_code == 200


@pytest.mark.asyncio
async def test_password_rotation_invalidates_old_cookie_for_access_management_and_totp(async_client) -> None:
    setup = await async_client.post("/api/dashboard-auth/password/setup", json={"password": "old-password-123"})
    assert setup.status_code == 200
    login = await async_client.post("/api/dashboard-auth/password/login", json={"password": "old-password-123"})
    assert login.status_code == 200
    old_cookie = async_client.cookies.get(DASHBOARD_SESSION_COOKIE)
    assert old_cookie

    changed = await async_client.post(
        "/api/dashboard-auth/password/change",
        json={"currentPassword": "old-password-123", "newPassword": "new-password-456"},
    )
    assert changed.status_code == 200

    async_client.cookies = type(async_client.cookies)()
    async_client.cookies.set(DASHBOARD_SESSION_COOKIE, old_cookie)
    session_response = await async_client.get("/api/dashboard-auth/session")
    assert session_response.status_code == 200
    assert session_response.json()["authenticated"] is False
    assert (await async_client.get("/api/settings")).status_code == 401
    assert (
        await async_client.post(
            "/api/dashboard-auth/password/change",
            json={"currentPassword": "old-password-123", "newPassword": "another-password-789"},
        )
    ).status_code == 401
    assert (await async_client.post("/api/dashboard-auth/totp/setup/start", json={})).status_code == 401

    fresh_login = await async_client.post("/api/dashboard-auth/password/login", json={"password": "new-password-456"})
    assert fresh_login.status_code == 200
    assert (await async_client.get("/api/settings")).status_code == 200


@pytest.mark.asyncio
async def test_same_password_totp_reenrollment_rejects_old_verified_cookie(async_client, monkeypatch) -> None:
    now = 2_000_000_010
    monkeypatch.setattr(totp_module, "time", lambda: now)
    setup = await async_client.post("/api/dashboard-auth/password/setup", json={"password": "password-12345"})
    assert setup.status_code == 200
    first_start = await async_client.post("/api/dashboard-auth/totp/setup/start", json={})
    assert first_start.status_code == 200
    first_secret = first_start.json()["secret"]
    first_confirm = await async_client.post(
        "/api/dashboard-auth/totp/setup/confirm",
        json={"secret": first_secret, "code": pyotp.TOTP(first_secret).at(now)},
    )
    assert first_confirm.status_code == 200
    enable = await async_client.put(
        "/api/settings",
        json={"stickyThreadsEnabled": False, "preferEarlierResetAccounts": False, "totpRequiredOnLogin": True},
    )
    assert enable.status_code == 200
    await async_client.post("/api/dashboard-auth/logout", json={})
    login = await async_client.post("/api/dashboard-auth/password/login", json={"password": "password-12345"})
    assert login.status_code == 200
    verified = await async_client.post(
        "/api/dashboard-auth/totp/verify", json={"code": pyotp.TOTP(first_secret).at(now)}
    )
    assert verified.status_code == 200
    old_cookie = async_client.cookies.get(DASHBOARD_SESSION_COOKIE)
    assert old_cookie

    now += 30
    disabled = await async_client.post(
        "/api/dashboard-auth/totp/disable", json={"code": pyotp.TOTP(first_secret).at(now)}
    )
    assert disabled.status_code == 200
    setup_again = await async_client.post("/api/dashboard-auth/totp/setup/start", json={})
    assert setup_again.status_code == 200

    second_start = setup_again
    assert second_start.status_code == 200
    second_secret = second_start.json()["secret"]
    second_confirm = await async_client.post(
        "/api/dashboard-auth/totp/setup/confirm",
        json={"secret": second_secret, "code": pyotp.TOTP(second_secret).at(now)},
    )
    assert second_confirm.status_code == 200
    enable_again = await async_client.put(
        "/api/settings",
        json={"stickyThreadsEnabled": False, "preferEarlierResetAccounts": False, "totpRequiredOnLogin": True},
    )
    assert enable_again.status_code == 200
    async_client.cookies = type(async_client.cookies)()
    async_client.cookies.set(DASHBOARD_SESSION_COOKIE, old_cookie)
    assert (await async_client.get("/api/settings")).status_code == 401
    stale_session = await async_client.get("/api/dashboard-auth/session")
    assert stale_session.status_code == 200
    assert stale_session.json()["authenticated"] is False


@pytest.mark.asyncio
async def test_password_removal_rejects_old_cookie_and_rebootstrap_requires_fresh_session(async_client) -> None:
    setup = await async_client.post("/api/dashboard-auth/password/setup", json={"password": "old-password-123"})
    assert setup.status_code == 200
    login = await async_client.post("/api/dashboard-auth/password/login", json={"password": "old-password-123"})
    assert login.status_code == 200
    old_cookie = async_client.cookies.get(DASHBOARD_SESSION_COOKIE)
    assert old_cookie

    removed = await async_client.request(
        "DELETE", "/api/dashboard-auth/password", json={"password": "old-password-123"}
    )
    assert removed.status_code == 200

    async_client.cookies = type(async_client.cookies)()
    async_client.cookies.set(DASHBOARD_SESSION_COOKIE, old_cookie)
    assert (await async_client.get("/api/settings")).status_code == 200
    # Local no-password access is intentionally allowed; the stale cookie must not
    # become an authenticated management session or authorize TOTP setup.
    assert (await async_client.post("/api/dashboard-auth/totp/setup/start", json={})).status_code == 401

    rebootstrap = await async_client.post("/api/dashboard-auth/password/setup", json={"password": "fresh-password-456"})
    assert rebootstrap.status_code == 200

    # The old cookie must remain unusable after a new password is bootstrapped.
    async_client.cookies = type(async_client.cookies)()
    async_client.cookies.set(DASHBOARD_SESSION_COOKIE, old_cookie)
    stale_session = await async_client.get("/api/dashboard-auth/session")
    assert stale_session.status_code == 200
    assert stale_session.json()["authenticated"] is False
    assert (await async_client.post("/api/dashboard-auth/totp/setup/start", json={})).status_code == 401
    assert (await async_client.get("/api/settings")).status_code == 401
    stale_change = await async_client.post(
        "/api/dashboard-auth/password/change",
        json={"currentPassword": "old-password-123", "newPassword": "another-password-789"},
    )
    assert stale_change.status_code == 401

    fresh_login = await async_client.post("/api/dashboard-auth/password/login", json={"password": "fresh-password-456"})
    assert fresh_login.status_code == 200
    assert (await async_client.get("/api/settings")).status_code == 200
