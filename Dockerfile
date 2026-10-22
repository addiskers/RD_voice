FROM python:3.13-slim

ENV PYTHONUNBUFFERED=1 PIP_NO_CACHE_DIR=1
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends build-essential ca-certificates \
    && rm -rf /var/lib/apt/lists/*

COPY pyproject.toml README.md ./
COPY engine ./engine
COPY examples ./examples
COPY scripts ./scripts

RUN pip install --upgrade pip && pip install ".[plivo-transport]"

# Download CPU models (Silero VAD, turn detector) into the image so workers start fast.
RUN python -c "from engine.worker.pipeline import prewarm_models; from engine.config import Settings; prewarm_models(Settings(_env_file=None))" || true

RUN mkdir -p /app/data/recordings
EXPOSE 8080 8090 8091

# ROLE=api | worker
ENV ROLE=api
CMD ["sh", "-c", "if [ \"$ROLE\" = worker ]; then gvox-worker start; else gvox-api; fi"]
