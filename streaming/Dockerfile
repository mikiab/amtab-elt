FROM ghcr.io/astral-sh/uv:0.13.5-python3.13-trixie-slim

RUN groupadd --system --gid 999 amtab \
 && useradd --system --gid 999 --uid 999 --create-home amtab

WORKDIR /amtab

# Disable buffering on stdout/stderr to prevent log loss during a crash
ENV PYTHONUNBUFFERED=1

ENV UV_COMPILE_BYTECODE=1
# Copy from the cache instead of linking since it's a mounted volume
ENV UV_LINK_MODE=copy

RUN --mount=type=cache,target=/root/.cache/uv \
    --mount=type=bind,source=uv.lock,target=uv.lock \
    --mount=type=bind,source=pyproject.toml,target=pyproject.toml \
    uv sync --locked --no-default-groups --no-install-project

COPY ./streaming/feed.py \
     ./streaming/stats.py \
     ./streaming/config.py \
     ./streaming/producer.py \
     ./streaming/consumer.py \
     /amtab/

ENV PATH="/amtab/.venv/bin:$PATH"

ENV AMTAB_DATA_LAKE_PATH=/data/bronze
RUN mkdir -p /data/bronze && chown amtab:amtab /data/bronze

USER amtab
