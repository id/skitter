FROM python:3.12-slim

RUN useradd -m -s /bin/bash skitter
USER skitter

WORKDIR /app
COPY --chown=skitter pyproject.toml .
COPY --chown=skitter skitter/ skitter/

USER root
RUN pip install --no-cache-dir .
USER skitter

CMD ["python", "-m", "skitter.supervisor"]
