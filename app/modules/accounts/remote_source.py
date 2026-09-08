from __future__ import annotations

import asyncio
from datetime import datetime
from typing import Literal
from urllib.parse import quote

import aiohttp
from pydantic import BaseModel, ConfigDict, Field, SecretStr, ValidationError
from pydantic.alias_generators import to_camel

from app.core.auth.refresh import RefreshError
from app.core.config.settings import Settings
from app.db.models import AccountStatus


def source_unavailable() -> RefreshError:
    return RefreshError("remote_credentials_unavailable", "Remote credentials unavailable", False, transport_error=True)


class SourceModel(BaseModel):
    model_config = ConfigDict(alias_generator=to_camel, populate_by_name=True, frozen=True, extra="ignore")


class SourceAccount(SourceModel):
    account_id: str = Field(min_length=1)
    chatgpt_account_id: str | None = None
    email: str
    plan_type: str
    status: AccountStatus
    last_refresh_at: datetime
    workspace_id: str | None = None
    workspace_label: str | None = None
    seat_type: str | None = None
    reset_at_primary: datetime | None = None
    reset_at_secondary: datetime | None = None


class SourceSnapshot(SourceModel):
    # Required: {} is malformed, not an authoritative empty pool.
    accounts: tuple[SourceAccount, ...]


class SourceTokens(SourceModel):
    access_token: SecretStr
    id_token: SecretStr
    # Deliberately no refresh-token field, including nested export formats.


class SourceExportIdentity(SourceModel):
    account_id: str


class SourceExportTime(SourceModel):
    last_refresh: datetime = Field(alias="last_refresh")


class SourceExport(SourceModel):
    account: SourceExportIdentity
    tokens: SourceTokens
    codex_auth_json: SourceExportTime


class SourceLogin(SourceModel):
    authenticated: Literal[True]
    role: Literal["admin"]
    totp_required_on_login: Literal[False]


class RemoteCredentialClient:
    def __init__(self, settings: Settings) -> None:
        assert settings.remote_credential_source_url is not None
        self._url = settings.remote_credential_source_url
        password = settings.remote_credential_source_password
        if password is None:
            assert settings.remote_credential_source_password_file is not None
            try:
                password = SecretStr(settings.remote_credential_source_password_file.read_text().strip())
            except OSError:
                raise source_unavailable() from None
        if not password.get_secret_value():
            raise source_unavailable()
        self._password = password
        self.session = aiohttp.ClientSession(
            cookie_jar=aiohttp.CookieJar(unsafe=True),
            timeout=aiohttp.ClientTimeout(total=settings.remote_credential_source_timeout_seconds),
            connector=aiohttp.TCPConnector(limit=4),
            trust_env=False,
        )
        self._login_lock = asyncio.Lock()
        self._login_generation = 0

    async def close(self) -> None:
        await self.session.close()

    async def _login(self, generation: int) -> None:
        async with self._login_lock:
            if self._login_generation != generation:
                return
            async with self.session.post(
                f"{self._url}/api/dashboard-auth/password/login",
                json={"password": self._password.get_secret_value()},
                allow_redirects=False,
            ) as response:
                if response.status != 200:
                    raise source_unavailable()
                SourceLogin.model_validate_json(await self._body(response))
            self._login_generation += 1

    async def _body(self, response: aiohttp.ClientResponse) -> bytes:
        body = bytearray()
        async for chunk in response.content.iter_chunked(65536):
            body.extend(chunk)
            if len(body) > 4 * 1024 * 1024:
                raise source_unavailable()
        return bytes(body)

    async def _request[T: BaseModel](self, method: str, path: str, model: type[T]) -> T:
        try:
            if self._login_generation == 0:
                await self._login(0)
            for attempt in range(2):
                generation = self._login_generation
                async with self.session.request(method, f"{self._url}{path}", allow_redirects=False) as response:
                    if response.status == 200:
                        return model.model_validate_json(await self._body(response))
                    if response.status != 401 or attempt:
                        raise source_unavailable()
                await self._login(generation)
        except (aiohttp.ClientError, TimeoutError, ValidationError):
            raise source_unavailable() from None
        raise source_unavailable()

    async def snapshot(self) -> SourceSnapshot:
        result = await self._request("GET", "/api/accounts", SourceSnapshot)
        if len({item.account_id for item in result.accounts}) != len(result.accounts):
            raise source_unavailable()
        return result

    async def export(self, account_id: str) -> SourceExport:
        result = await self._request("POST", f"/api/accounts/{quote(account_id, safe='')}/export/auth", SourceExport)
        if result.account.account_id != account_id or not result.tokens.access_token.get_secret_value():
            raise source_unavailable()
        return result
