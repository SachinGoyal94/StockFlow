# Single image shared by the webserver, daemon, and gRPC code-server
# containers (they differ only in the command docker-compose gives them).
#
# Multi-stage so the final image carries the installed package without
# pip/setuptools build tools.

FROM python:3.11-slim AS builder

WORKDIR /build
# Copy the whole src/ tree: its contents (pyproject.toml + the
# stock_pipeline package dir) land in stock_pipeline_build/ with the
# package structure preserved.
COPY src/ ./stock_pipeline_build/
# Build a wheel from the package so the final stage can install it with
# --no-cache-dir and no build tooling.
RUN pip wheel --no-deps --wheel-dir /wheels ./stock_pipeline_build
# Install runtime dependencies into a staging prefix for the final stage.
RUN pip install --prefix /install --no-cache-dir \
    dagster \
    dagster-webserver \
    dagster-postgres \
    requests \
    'psycopg[binary]' \
    /wheels/stock_pipeline-*.whl


FROM python:3.11-slim

# tini gives correct PID-1 signal handling (Ctrl+C / docker stop work right).
RUN apt-get update \
    && apt-get install -y --no-install-recommends tini \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /install /usr/local

# Non-root user for everything the containers actually run.
RUN useradd --create-home --uid 1000 dagster
USER dagster
WORKDIR /opt/dagster

# Dagster looks for instance config here; workspace.yaml is passed at
# startup by the webserver/daemon commands.
COPY --chown=dagster:dagster dagster.yaml /opt/dagster/dagster.yaml

ENV DAGSTER_HOME=/opt/dagster \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

ENTRYPOINT ["/usr/bin/tini", "--"]
