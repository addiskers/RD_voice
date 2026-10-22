FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md alembic.ini ./
COPY engine ./engine
COPY examples ./examples
COPY scripts ./scripts
COPY deploy ./deploy
COPY ui ./ui

RUN pip install --upgrade pip && pip install ".[plivo-transport,postgres,s3]" && chmod +x deploy/worker-entrypoint.sh

# Download CPU models (Silero VAD, turn detector) into the image so workers start fast.
RUN python -c "from engine.worker.pipeline import prewarm_models; from engine.config import Settings; prewarm_models(Settings(_env_file=None))" || true

RUN mkdir -p /app/data/recordings /app/data/spool
EXPOSE 8080 8090 8091

# ROLE=api | worker. The worker goes through deploy/worker-entrypoint.sh so SIGTERM drains instead of dropping calls.
ENV ROLE=api
CMD ["sh", "-c", "if [ \"$ROLE\" = worker ]; then exec deploy/worker-entrypoint.sh; else exec gvox-api; fi"]
