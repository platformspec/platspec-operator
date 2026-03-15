# Dockerfile — platspec-operator
#
# Build context: repository root
#
#   docker build -f Dockerfile -t platspec-operator .
#
# The operator image bakes in the bundled blueprints/ at /blueprints.
# Override at runtime with a volume mount or a BlueprintRegistry.

# ── Builder ───────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS builder

COPY --from=ghcr.io/astral-sh/uv:latest /uv /usr/local/bin/uv

WORKDIR /app

# Copy dependency manifest first for better layer caching.
COPY pyproject.toml uv.lock ./

# Install production dependencies without the project itself.
RUN uv sync --frozen --no-dev --no-install-project

# Copy source and install the project into site-packages (non-editable so the
# runtime stage needs only the venv, not the source tree).
COPY src/ src/
RUN uv sync --frozen --no-dev --no-editable

# ── Runtime ───────────────────────────────────────────────────────────────────
FROM python:3.13-slim AS runtime

# Install dependencies
RUN apt update && apt -y install git && apt clean

# Non-root user matching the security context in chart/values.yaml.
RUN groupadd -g 1000 platspec \
 && useradd -u 1000 -g platspec -s /sbin/nologin -d /app platspec

WORKDIR /app

# Copy the virtualenv. Builder also used /app so shebang paths are identical.
COPY --from=builder /app/.venv /app/.venv

# Bake blueprints into the image. A volume mount at /blueprints overrides this
# at runtime (production PVC, or Tilt live-sync for local dev).
COPY blueprints/ /blueprints/

ENV PATH="/app/.venv/bin:$PATH" \
    PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

USER 1000:1000

ENTRYPOINT ["platspec-operator"]
