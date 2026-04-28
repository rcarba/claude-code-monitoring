FROM python:3.12-slim

WORKDIR /app
COPY ingest.py pricing.py /app/

ENV CLAUDE_PROJECTS_DIR=/claude-projects \
    DB_PATH=/data/tokens.db \
    INTERVAL_SECONDS=300 \
    PYTHONUNBUFFERED=1

VOLUME ["/data"]

CMD ["python", "/app/ingest.py"]
