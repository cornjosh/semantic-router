# Build from the repository root: docker build -f deploy/adaptive/Dockerfile .
FROM ghcr.io/astral-sh/uv:0.13.5 AS uv
FROM python:3.12.13-slim-bookworm
COPY --from=uv /uv /usr/local/bin/uv
WORKDIR /app
ENV UV_COMPILE_BYTECODE=1 UV_LINK_MODE=copy UV_PYTHON_DOWNLOADS=never \
    PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1
COPY pyproject.toml uv.lock README.md LICENSE ./
COPY semantic_router ./semantic_router
RUN uv sync --frozen --no-dev --no-editable --python /usr/local/bin/python \
    && mkdir /data \
    && chown 10001:10001 /data
ENV PATH="/app/.venv/bin:$PATH"
USER 10001:10001
VOLUME ["/data"]
EXPOSE 8080
ENTRYPOINT ["python", "-m", "semantic_router.adaptive", "--host", "0.0.0.0"]
CMD ["--config", "/etc/semantic-router/service.yaml"]
