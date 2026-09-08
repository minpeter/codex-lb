FROM ghcr.io/astral-sh/uv:0.12.8 AS uv-bin

FROM ghcr.io/soju06/codex-lb@sha256:fb558395cbfb52dd51af2caf9aef836238e01abeb5794eacc252d6fc3ce5102d

ARG SOURCE_REVISION
LABEL org.opencontainers.image.revision=$SOURCE_REVISION

USER root
COPY --from=uv-bin /uv /usr/local/bin/uv
COPY pyproject.toml uv.lock /app/
RUN UV_PROJECT_ENVIRONMENT=/opt/venv uv sync --frozen --no-dev \
    --no-install-project --extra metrics --extra tracing \
    && rm /usr/local/bin/uv

COPY --chown=app:app app /app/app
COPY --chown=app:app config /app/config
COPY --chown=app:app scripts /app/scripts

USER app
