FROM python:3.11-slim

ENV PYTHONDONTWRITEBYTECODE=1
ENV PYTHONUNBUFFERED=1

WORKDIR /app

RUN pip install --no-cache-dir --timeout 120 --retries 10 \
    boto3 \
    polars \
    psycopg2-binary \
    deltalake \
    clickhouse-connect \
    pyarrow \
    python-dotenv
