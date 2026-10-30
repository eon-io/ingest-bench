# Harness image for corpus generation, production, table tools and scoring. Leave the
# platform selectable for native builds. Pin uv as well as Python to keep the installer
# consistent across builds.
FROM ghcr.io/astral-sh/uv:0.9.30-python3.12-bookworm-slim

WORKDIR /app
# Copy dependencies across layers and compile bytecode during the build to reduce command
# startup time.
ENV UV_LINK_MODE=copy UV_COMPILE_BYTECODE=1 PATH="/app/.venv/bin:${PATH}"

# Install dependencies before copying source so code edits reuse the dependency layer.
# Include README.md for package metadata. Install the aws extra so the same image supports
# MSK IAM sites.
COPY pyproject.toml uv.lock README.md ./
RUN uv sync --frozen --no-dev --extra aws --no-install-project

COPY LICENSE NOTICE ./
COPY LICENSES ./LICENSES
COPY ingest_bench ./ingest_bench
COPY engines ./engines
COPY compactors ./compactors
COPY workloads ./workloads
RUN uv sync --frozen --no-dev --extra aws

# Give packaged commands a fixed location for presets and schemas.
ENV INGEST_BENCH_WORKLOADS=/app/workloads
# The build context has no Git history, so the builder supplies the commit that
# staging records in each run's facts and publication requires.
ARG HARNESS_COMMIT=""
ENV INGEST_BENCH_COMMIT=${HARNESS_COMMIT}

# Compose supplies a command string. Kubernetes Jobs override this entrypoint with argv.
# Shell callers needing graceful signal delivery must include exec.
ENTRYPOINT ["/bin/sh", "-c"]
CMD ["gen-corpus --help"]
