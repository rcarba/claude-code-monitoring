FROM python:3.12-slim

# Install Node.js 20 (ccusage requires >=20.19) and pre-install ccusage globally
# so it is on $PATH for the Python ingester to invoke.
RUN apt-get update \
    && apt-get install -y --no-install-recommends curl ca-certificates gnupg \
    && curl -fsSL https://deb.nodesource.com/setup_20.x | bash - \
    && apt-get install -y --no-install-recommends nodejs \
    && apt-get purge -y curl gnupg \
    && apt-get autoremove -y \
    && rm -rf /var/lib/apt/lists/* \
    && npm install -g ccusage@latest \
    && npm cache clean --force

# ccusage looks for transcripts at $HOME/.claude/projects. The compose file
# mounts the host JSONL dir at /claude-projects, so symlink it into HOME.
RUN mkdir -p /root/.claude \
    && ln -s /claude-projects /root/.claude/projects

WORKDIR /app
COPY ingest.py pricing.py /app/

ENV CLAUDE_PROJECTS_DIR=/claude-projects \
    DB_PATH=/data/tokens.db \
    INTERVAL_SECONDS=300 \
    PYTHONUNBUFFERED=1

VOLUME ["/data"]

CMD ["python", "/app/ingest.py"]
