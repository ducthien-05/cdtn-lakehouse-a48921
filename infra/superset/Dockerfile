FROM apache/superset:6.0.0

USER root

RUN /usr/local/bin/uv pip install \
      --python /app/.venv/bin/python \
      --no-cache \
      trino \
      psycopg2-binary \
    && /app/.venv/bin/python -c "import trino, psycopg2; print('Superset drivers installed')"

USER superset