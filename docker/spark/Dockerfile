FROM bitnami/spark:3.5.3

USER root

RUN apt-get update && apt-get install -y --no-install-recommends \
    python3-pip \
    && rm -rf /var/lib/apt/lists/*

# Install Python dependencies
COPY requirements.txt /tmp/requirements.txt
RUN pip3 install --no-cache-dir -r /tmp/requirements.txt

# Copy application code
COPY spark/ /opt/spark/app/spark/
COPY storage/ /opt/spark/app/storage/

ENV PYTHONPATH=/opt/spark/app
ENV SPARK_MASTER=local[*]

WORKDIR /opt/spark/app

HEALTHCHECK --interval=30s --timeout=10s --start-period=120s --retries=3 \
    CMD python3 -c "import pyspark; print('ok')" || exit 1

CMD ["python3", "-m", "spark.main"]
