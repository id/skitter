FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl && \
    curl -fsSL https://deb.nodesource.com/setup_22.x | bash - && \
    apt-get install -y --no-install-recommends nodejs && \
    rm -rf /var/lib/apt/lists/*

RUN npm install -g @anthropic-ai/claude-code

# Non-root user (required by claude --dangerously-skip-permissions)
RUN useradd -m -s /bin/bash skitter
USER skitter

WORKDIR /app
COPY --chown=skitter pyproject.toml .
COPY --chown=skitter skitter/ skitter/

USER root
RUN pip install --no-cache-dir .
USER skitter

ENTRYPOINT ["python", "-m", "skitter.worker"]
