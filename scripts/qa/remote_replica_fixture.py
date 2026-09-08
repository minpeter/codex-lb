"""Local-only replica QA: uv run python scripts/qa/remote_replica_fixture.py --port 2467.

Source URL: http://127.0.0.1:2467/source; upstream: http://127.0.0.1:2467/backend-api.
Login password: qa-source-password. POST /control/state with {"action": "rotate"},
source-down, source-up, pause, remove, expire-session, or reject-old-token.
Pause/remove affect source metadata only; source-up restores availability only.
Rotate accepts the new and immediately previous token; reject-old-token retires
that previous token. Restart resets all state. Counters count attempts, including
rejected requests. No timers, external requests, or genuine credentials are used.
Uses the project's environment and schemas intentionally, not a PEP 723 sandbox.
"""

from __future__ import annotations

import argparse
import base64
import hashlib
import hmac
import json
from collections.abc import Awaitable, Callable
from dataclasses import dataclass, field
from datetime import UTC, datetime, timedelta
from typing import Final, Literal, assert_never

from aiohttp import web
from pydantic import BaseModel, ConfigDict, ValidationError

from app.core.auth import IdTokenClaims, OpenAIAuthClaims
from app.core.openai.models import OpenAIError, OpenAIErrorEnvelope, OpenAIResponsePayload, ResponseUsage
from app.core.openai.requests import ResponsesRequest
from app.core.types import JsonObject
from app.core.usage.models import RateLimitPayload, UsagePayload, UsageWindow
from app.modules.accounts.schemas import (
    AccountAuthExportResponse,
    AccountAuthExportTokens,
    AccountOpenCodeAuthExportAccount,
    AccountsResponse,
    AccountSummary,
    CodexAuthJson,
    CodexAuthTokens,
    OpenCodeAuthJson,
    OpenCodeOAuthAuth,
)
from app.modules.dashboard_auth.schemas import DashboardAuthSessionResponse, PasswordLoginRequest
from app.modules.dashboard_auth.service import DASHBOARD_SESSION_COOKIE

ACCOUNT_ID: Final = "remote-account-1"
EMAIL: Final = "replica-qa@example.invalid"
PASSWORD: Final = "qa-source-password"


class ControlRequest(BaseModel):
    model_config = ConfigDict(frozen=True, extra="forbid")
    action: Literal["rotate", "source-down", "source-up", "pause", "remove", "expire-session", "reject-old-token"]


class State(BaseModel):
    """Mutable, token-free control surface; handlers update counters and switches."""

    generation: int = 1
    source_up: bool = True
    account_status: Literal["active", "paused"] = "active"
    account_present: bool = True
    accept_previous_token: bool = True
    login_attempts: int = 0
    list_attempts: int = 0
    export_attempts: int = 0
    inference_attempts: int = 0
    usage_attempts: int = 0
    refresh_attempts: int = 0


class Event(BaseModel):
    model_config = ConfigDict(frozen=True)
    type: str
    sequence_number: int = 0
    response: OpenAIResponsePayload | None = None
    output_index: int | None = None
    content_index: int | None = None
    item_id: str | None = None
    item: JsonObject | None = None
    part: JsonObject | None = None
    delta: str | None = None
    text: str | None = None
    logprobs: list[JsonObject] | None = None


def json_response(payload: BaseModel) -> web.Response:
    return web.Response(text=payload.model_dump_json(by_alias=True), content_type="application/json")


def error(status: int, code: str) -> web.Response:
    response = json_response(OpenAIErrorEnvelope(error=OpenAIError(code=code, type="fixture_error", message=code)))
    response.set_status(status)
    return response


def encode(raw: bytes) -> str:
    return base64.urlsafe_b64encode(raw).decode().rstrip("=")


@dataclass(slots=True)
class Fixture:
    """One event-loop-owned state machine, mutated only by incoming HTTP requests."""

    state: State = field(default_factory=State)
    sessions: set[str] = field(default_factory=set)
    started_at: datetime = field(default_factory=lambda: datetime.now(UTC).replace(microsecond=0))

    @property
    def refreshed_at(self) -> datetime:
        return self.started_at + timedelta(seconds=self.state.generation - 1)

    def token(self, generation: int, kind: str = "access") -> str:
        claims = IdTokenClaims.model_validate(
            {
                "email": EMAIL,
                "sub": "fixture-user-1",
                "exp": int(self.started_at.timestamp()) + 86400 + generation,
                "https://api.openai.com/auth": OpenAIAuthClaims(
                    chatgpt_account_id=ACCOUNT_ID, chatgpt_plan_type="plus"
                ),
            }
        ).model_dump(by_alias=True, exclude_none=True)
        claims.update(iat=int(self.started_at.timestamp()), jti=f"fixture-{kind}-{generation}")
        # Public fixture-only HMAC key: syntactically signed JWTs, never provider credentials.
        unsigned = encode(b'{"alg":"HS256","typ":"JWT"}') + "." + encode(json.dumps(claims).encode())
        return unsigned + "." + encode(hmac.digest(b"qa-fixture-only", unsigned.encode(), hashlib.sha256))

    def source_error(self, request: web.Request, *, session: bool = True) -> web.Response | None:
        if not self.state.source_up:
            return error(503, "fixture_source_down")
        if session and request.cookies.get(DASHBOARD_SESSION_COOKIE) not in self.sessions:
            return error(401, "authentication_required")
        return None

    def authorized(self, request: web.Request) -> bool:
        valid = {self.token(self.state.generation)}
        if self.state.accept_previous_token and self.state.generation > 1:
            valid.add(self.token(self.state.generation - 1))
        return request.headers.get("Authorization", "") in {f"Bearer {token}" for token in valid}

    async def login(self, request: web.Request) -> web.Response:
        self.state.login_attempts += 1
        if (failure := self.source_error(request, session=False)) is not None:
            return failure
        payload = PasswordLoginRequest.model_validate_json(await request.read())
        if payload.password != PASSWORD:
            return error(401, "invalid_credentials")
        session = f"fixture-session-{self.state.login_attempts}"
        self.sessions.add(session)
        response = json_response(
            DashboardAuthSessionResponse(
                authenticated=True,
                password_required=True,
                totp_required_on_login=False,
                totp_configured=False,
                password_session_active=True,
            )
        )
        response.set_cookie(DASHBOARD_SESSION_COOKIE, session, httponly=True, samesite="lax", max_age=86400, path="/")
        return response

    async def accounts(self, request: web.Request) -> web.Response:
        self.state.list_attempts += 1
        if (failure := self.source_error(request)) is not None:
            return failure
        account = AccountSummary(
            account_id=ACCOUNT_ID,
            chatgpt_account_id=ACCOUNT_ID,
            email=EMAIL,
            display_name="Replica QA",
            plan_type="plus",
            status=self.state.account_status,
            last_refresh_at=self.refreshed_at,
        )
        return json_response(AccountsResponse(accounts=[account] if self.state.account_present else []))

    async def export(self, request: web.Request) -> web.Response:
        self.state.export_attempts += 1
        if (failure := self.source_error(request)) is not None:
            return failure
        if not self.state.account_present or request.match_info["id"] != ACCOUNT_ID:
            return error(404, "account_not_found")
        access, identity = self.token(self.state.generation), self.token(self.state.generation, "id")
        refresh = "trap-refresh-token"
        expires = (int(self.started_at.timestamp()) + 86400 + self.state.generation) * 1000
        response = json_response(
            AccountAuthExportResponse(
                filename="replica-qa-auth.json",
                account=AccountOpenCodeAuthExportAccount(
                    account_id=ACCOUNT_ID, chatgpt_account_id=ACCOUNT_ID, email=EMAIL
                ),
                tokens=AccountAuthExportTokens(
                    access_token=access,
                    id_token=identity,
                    refresh_token=refresh,
                    expires_at_ms=expires,
                ),
                codex_auth_json=CodexAuthJson(
                    tokens=CodexAuthTokens(
                        access_token=access,
                        id_token=identity,
                        refresh_token=refresh,
                        account_id=ACCOUNT_ID,
                    ),
                    last_refresh=self.refreshed_at.isoformat().replace("+00:00", "Z"),
                ),
                opencode_auth_json=OpenCodeAuthJson(
                    openai=OpenCodeOAuthAuth(
                        access=access,
                        refresh=refresh,
                        expires=expires,
                        account_id=ACCOUNT_ID,
                    )
                ),
            )
        )
        response.headers.update(
            {"Cache-Control": "no-store, no-cache, must-revalidate, private", "Pragma": "no-cache", "Expires": "0"}
        )
        return response

    async def inference(self, request: web.Request) -> web.StreamResponse:
        self.state.inference_attempts += 1
        if not self.authorized(request):
            return error(401, "invalid_api_key")
        payload = ResponsesRequest.model_validate_json(await request.read())
        part: JsonObject = {"type": "output_text", "text": "REPLICA_OK", "annotations": [], "logprobs": []}
        item: JsonObject = {
            "id": "msg_fixture",
            "type": "message",
            "role": "assistant",
            "status": "completed",
            "content": [part],
        }
        result = OpenAIResponsePayload.model_validate(
            {
                "id": f"resp_fixture_{self.state.inference_attempts}",
                "object": "response",
                "status": "completed",
                "created_at": int(self.started_at.timestamp()),
                "model": payload.model,
                "output": [item],
                "usage": ResponseUsage(input_tokens=5, output_tokens=3, total_tokens=8),
            }
        )
        events = [
            Event(
                type="response.created",
                response=result.model_copy(
                    update={
                        "status": "in_progress",
                        "output": [],
                        "usage": None,
                    }
                ),
            ),
            Event(
                type="response.output_item.added", output_index=0, item={**item, "status": "in_progress", "content": []}
            ),
            Event(
                type="response.content_part.added",
                output_index=0,
                content_index=0,
                item_id="msg_fixture",
                part={**part, "text": ""},
            ),
            Event(
                type="response.output_text.delta",
                output_index=0,
                content_index=0,
                item_id="msg_fixture",
                delta="REPLICA_OK",
                logprobs=[],
            ),
            Event(
                type="response.output_text.done",
                output_index=0,
                content_index=0,
                item_id="msg_fixture",
                text="REPLICA_OK",
                logprobs=[],
            ),
            Event(type="response.content_part.done", output_index=0, content_index=0, item_id="msg_fixture", part=part),
            Event(type="response.output_item.done", output_index=0, item=item),
            Event(type="response.completed", response=result),
        ]
        stream = web.StreamResponse(headers={"Content-Type": "text/event-stream", "Cache-Control": "no-cache"})
        await stream.prepare(request)
        for sequence, event in enumerate(events):
            data = event.model_copy(update={"sequence_number": sequence}).model_dump_json(exclude_none=True)
            await stream.write(f"event: {event.type}\ndata: {data}\n\n".encode())
        await stream.write_eof()
        return stream

    async def usage(self, request: web.Request) -> web.Response:
        self.state.usage_attempts += 1
        if not self.authorized(request):
            return error(401, "invalid_api_key")
        return json_response(
            UsagePayload(
                plan_type="plus",
                rate_limit=RateLimitPayload(
                    primary_window=UsageWindow(
                        used_percent=0, limit_window_seconds=18000, reset_at=int(self.started_at.timestamp()) + 18000
                    ),
                ),
            )
        )

    async def refresh(self, request: web.Request) -> web.Response:
        self.state.refresh_attempts += 1
        return web.json_response(
            {"error": "invalid_grant", "error_description": "Fixture forbids OAuth refresh"}, status=400
        )

    async def control(self, request: web.Request) -> web.Response:
        if request.method == "POST":
            payload = ControlRequest.model_validate_json(await request.read())
            match payload.action:
                case "rotate":
                    self.state.generation += 1
                    self.state.accept_previous_token = True
                case "source-down":
                    self.state.source_up = False
                case "source-up":
                    self.state.source_up = True
                case "pause":
                    self.state.account_status = "paused"
                case "remove":
                    self.state.account_present = False
                case "expire-session":
                    self.sessions.clear()
                case "reject-old-token":
                    self.state.accept_previous_token = False
                case unreachable:
                    assert_never(unreachable)
        return json_response(self.state)


@web.middleware
async def parse_errors(
    request: web.Request, handler: Callable[[web.Request], Awaitable[web.StreamResponse]]
) -> web.StreamResponse:
    try:
        return await handler(request)
    except ValidationError:
        return error(400, "invalid_request")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--port", type=int, default=2467)
    args = parser.parse_args()
    fixture = Fixture()
    app = web.Application(middlewares=[parse_errors])
    app.add_routes(
        [
            web.post("/source/api/dashboard-auth/password/login", fixture.login),
            web.get("/source/api/accounts", fixture.accounts),
            web.post("/source/api/accounts/{id}/export/auth", fixture.export),
            web.post("/backend-api/codex/responses", fixture.inference),
            web.get("/backend-api/wham/usage", fixture.usage),
            web.post("/oauth/token", fixture.refresh),
            web.get("/control/state", fixture.control),
            web.post("/control/state", fixture.control),
        ]
    )
    web.run_app(app, host="127.0.0.1", port=args.port, access_log=None)


if __name__ == "__main__":
    main()
