from __future__ import annotations

from typing import cast

import pytest
from pydantic import ValidationError

from app.core.auth.guardian import build_auth_guardian_scheduler
from app.core.auth.refresh import RefreshError
from app.core.config.settings import Settings, get_settings
from app.core.crypto import TokenEncryptor
from app.core.utils.time import utcnow
from app.db.models import Account, AccountStatus
from app.modules.accounts.auth_manager import AccountsRepositoryPort, AuthManager
from app.modules.accounts.remote_source import RemoteCredentialClient, SourceExport, SourceSnapshot

pytestmark = pytest.mark.unit


@pytest.fixture
def remote_mode(monkeypatch):
    monkeypatch.setenv("CODEX_LB_REMOTE_CREDENTIAL_SOURCE_URL", "http://127.0.0.1:2467")
    monkeypatch.setenv("CODEX_LB_REMOTE_CREDENTIAL_SOURCE_PASSWORD", "fake-dashboard-password")
    get_settings.cache_clear()
    yield
    get_settings.cache_clear()


def account() -> Account:
    encryptor = TokenEncryptor()
    return Account(
        id="source-account",
        chatgpt_account_id="upstream-account",
        email="fake@example.com",
        plan_type="plus",
        access_token_encrypted=encryptor.encrypt("old-access"),
        refresh_token_encrypted=encryptor.encrypt("fake-refresh-never-exchange"),
        id_token_encrypted=encryptor.encrypt("fake-id"),
        last_refresh=utcnow(),
        status=AccountStatus.ACTIVE,
    )


async def test_forced_refresh_fails_closed_without_remote_runtime(remote_mode, monkeypatch):
    value = account()
    manager = AuthManager(cast(AccountsRepositoryPort, None))
    native_calls = []

    async def native_refresh(self, value):
        native_calls.append(value.id)
        return value

    monkeypatch.setattr(AuthManager, "refresh_account", native_refresh)
    with pytest.raises(RefreshError) as error:
        await manager.ensure_fresh(value, force=True)
    assert not error.value.is_permanent
    assert native_calls == []


@pytest.mark.parametrize("url", ["ftp://source", "https://u:p@source", "https://source?secret=x", "https://source#x"])
def test_source_settings_reject_unsafe_url(url):
    with pytest.raises(ValidationError):
        Settings(remote_credential_source_url=url, remote_credential_source_password="fake")


def test_source_settings_require_exactly_one_password():
    with pytest.raises(ValidationError):
        Settings(remote_credential_source_url="https://source")


def test_source_settings_defaults_and_secret_redaction():
    settings = Settings(remote_credential_source_url="https://source/", remote_credential_source_password="fake-secret")
    assert settings.remote_credential_source_url == "https://source"
    assert settings.remote_credential_source_timeout_seconds == 8
    assert settings.remote_credential_source_sync_interval_seconds == 60
    assert "fake-secret" not in repr(settings)


def test_source_settings_reject_both_password_sources(tmp_path):
    with pytest.raises(ValidationError):
        Settings(
            remote_credential_source_url="https://source",
            remote_credential_source_password="fake",
            remote_credential_source_password_file=tmp_path / "password",
        )


async def test_password_file_and_client_cleanup(tmp_path):
    password = tmp_path / "password"
    password.write_text("fake-file-password\n")
    settings = Settings(remote_credential_source_url="https://source", remote_credential_source_password_file=password)
    client = RemoteCredentialClient(settings)
    assert client._password.get_secret_value() == "fake-file-password"
    await client.close()
    assert client.session.closed


def test_guardian_disabled_in_remote_mode(remote_mode):
    assert not build_auth_guardian_scheduler().enabled


def test_export_discards_all_refresh_material():
    exported = SourceExport.model_validate(
        {
            "account": {"accountId": "source-id"},
            "tokens": {"accessToken": "fake-access", "idToken": "fake-id", "refreshToken": "fake-refresh-secret"},
            "codexAuthJson": {
                "last_refresh": "2026-01-01T00:00:00Z",
                "tokens": {"refresh_token": "fake-refresh-secret"},
            },
            "opencodeAuthJson": {"openai": {"refresh": "fake-refresh-secret"}},
        }
    )
    assert "refresh_token" not in type(exported.tokens).model_fields
    assert "fake-refresh-secret" not in exported.model_dump_json()
    assert "fake-access" not in repr(exported)


@pytest.mark.parametrize("payload", [{}, {"accounts": None}, {"accounts": [{}]}, {"accounts": [{"status": "unknown"}]}])
def test_malformed_snapshot_is_not_empty(payload):
    with pytest.raises(ValidationError):
        SourceSnapshot.model_validate(payload)


def test_valid_empty_snapshot_is_explicit():
    assert SourceSnapshot.model_validate({"accounts": []}).accounts == ()
