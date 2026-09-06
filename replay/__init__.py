"""Static snapshot export of the Gold-layer analytics tables.

Reads the existing Gold Delta tables when they're available locally, or
generates a realistic static sample otherwise, and writes both to
Parquet/CSV so the dashboard can run without Kafka, Spark, or Docker.
"""
