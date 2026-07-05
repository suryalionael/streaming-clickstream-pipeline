FROM python:3.12-slim

WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    gcc \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt confluent-kafka python-dateutil pytz pyyaml

COPY producer/ ./producer/
COPY storage/ ./storage/

ENV PYTHONPATH=/app
ENV PYTHONUNBUFFERED=1

HEALTHCHECK --interval=30s --timeout=10s --start-period=60s --retries=3 \
    CMD python -c "import confluent_kafka; print('ok')" || exit 1

CMD ["python", "-m", "producer.main"]
