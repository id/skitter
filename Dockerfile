FROM python:3.12-slim

RUN apt-get update && \
    apt-get install -y --no-install-recommends curl unzip && \
    curl -fsSL https://rclone.org/install.sh | bash && \
    apt-get purge -y unzip && apt-get autoremove -y && \
    rm -rf /var/lib/apt/lists/*

# Non-root user (required by claude --dangerously-skip-permissions)
RUN useradd -m -s /bin/bash skitter
USER skitter

# Claude CLI (binary — no Node.js needed)
RUN curl -fsSL https://claude.ai/install.sh | bash

# Codex CLI (binary from GitHub releases)
ARG CODEX_VERSION=0.112.0
ARG TARGETARCH
RUN mkdir -p /home/skitter/.local/bin && \
    ARCH=$([ "$TARGETARCH" = "arm64" ] && echo "aarch64" || echo "x86_64") && \
    curl -fsSL "https://github.com/openai/codex/releases/download/rust-v${CODEX_VERSION}/codex-${ARCH}-unknown-linux-gnu.tar.gz" \
    | tar xz -C /home/skitter/.local/bin/

ENV PATH="/home/skitter/.local/bin:$PATH"

WORKDIR /app
COPY --chown=skitter pyproject.toml .
COPY --chown=skitter skitter/ skitter/

USER root
RUN pip install --no-cache-dir .
USER skitter

COPY --chown=skitter entrypoint.sh /home/skitter/entrypoint.sh
ENTRYPOINT ["/home/skitter/entrypoint.sh"]
CMD ["python", "-m", "skitter.worker"]
