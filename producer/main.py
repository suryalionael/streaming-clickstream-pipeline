"""Synthetic clickstream event generator entry point."""

import logging
import sys
import time

from producer.config import config
from producer.generator import ClickstreamGenerator
from producer.kafka_client import KafkaProducerClient

logger = logging.getLogger(__name__)


def setup_logging() -> None:
    """Configure structured JSON logging."""
    log_format = config.logging.format
    log_level = getattr(logging, config.logging.level.upper(), logging.INFO)

    if log_format == "json":
        try:
            from pythonjsonlogger import jsonlogger

            formatter: logging.Formatter = jsonlogger.JsonFormatter(  # type: ignore[no-untyped-call]
                fmt="%(asctime)s %(name)s %(levelname)s %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
        except ImportError:
            formatter = logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
                datefmt="%Y-%m-%dT%H:%M:%S",
            )
    else:
        formatter = logging.Formatter(
            "%(asctime)s [%(levelname)s] %(name)s: %(message)s",
            datefmt="%Y-%m-%dT%H:%M:%S",
        )

    handler = logging.StreamHandler(sys.stdout)
    handler.setFormatter(formatter)

    logging.basicConfig(level=log_level, handlers=[handler], force=True)


def run_producer() -> None:
    """Run the clickstream generator and Kafka producer."""
    setup_logging()

    logger.info(
        "Starting clickstream producer",
        extra={
            "events_per_second": config.generator.events_per_second,
            "num_users": config.generator.num_users,
            "burst_mode": config.generator.burst_mode,
            "kafka_topic": config.kafka.topic,
        },
    )

    generator = ClickstreamGenerator(config.generator)
    producer = KafkaProducerClient(config.kafka)

    try:
        producer.start()

        rate = config.generator.events_per_second
        interval = 1.0 / rate if rate > 0 else 0.02
        last_report = time.time()
        events_sent = 0
        batch_size = max(1, rate // 10)

        while producer.is_running:
            batch_start = time.time()

            burst_mult = generator.get_burst_multiplier()
            current_batch_size = batch_size * burst_mult

            events = generator.generate_batch(current_batch_size)
            sent = producer.send_batch(events)
            events_sent += sent

            # Dynamic rate limiting
            elapsed = time.time() - batch_start
            sleep_time = max(0, interval * current_batch_size - elapsed)
            if sleep_time > 0:
                time.sleep(sleep_time)

            # Periodic metrics
            if time.time() - last_report > 30:
                actual_rate = events_sent / (time.time() - last_report)
                logger.info(
                    "Producer throughput",
                    extra={
                        "events_sent": events_sent,
                        "actual_rate": round(actual_rate, 2),
                        "target_rate": rate,
                        "active_sessions": len(generator.sessions),
                    },
                )
                events_sent = 0
                last_report = time.time()

            # Periodic flush
            if events_sent % 1000 == 0:
                producer.flush(timeout=5.0)

    except KeyboardInterrupt:
        logger.info("Received keyboard interrupt, shutting down")
    except Exception:
        logger.exception("Fatal producer error")
        sys.exit(1)
    finally:
        producer.stop()
        logger.info("Producer stopped")


def main() -> None:
    run_producer()


if __name__ == "__main__":
    main()
