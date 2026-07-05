FROM python:3.12-slim-bookworm

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    openjdk-17-jre-headless \
    procps \
    curl \
    && rm -rf /var/lib/apt/lists/*

COPY requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY spark/ /opt/spark/app/spark/
COPY storage/ /opt/spark/app/storage/

ENV PYTHONPATH=/opt/spark/app
ENV SPARK_MASTER=local[*]
ENV PYSPARK_PYTHON=python

RUN mkdir -p /opt/spark/app/data /opt/spark/app/checkpoints && \
    chmod -R 777 /opt/spark/app /opt/spark/app/data /opt/spark/app/checkpoints /tmp

WORKDIR /opt/spark/app

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD python -c "from pyspark.sql import SparkSession; SparkSession.builder.master('local').appName('healthcheck').getOrCreate().stop(); print('ok')" || exit 1

CMD ["python", "-m", "spark.main"]
