"""Kafka producer client with retries, structured logging, and graceful shutdown."""

import json
import logging
import signal
import threading
import time
from typing import Any

from confluent_kafka import Producer as ConfluentProducer
from confluent_kafka.error import KafkaException

from producer.config import KafkaConfig
from producer.models import ClickstreamEvent

logger = logging.getLogger(__name__)


class KafkaProducerError(Exception):
    """Base exception for Kafka producer operations."""


class KafkaProducerClient:
    """Production-grade Kafka producer with retries, batching, and graceful shutdown."""

    def __init__(self, config: KafkaConfig) -> None:
        self.config = config
        self._producer: ConfluentProducer | None = None
        self._running = False
        self._shutdown = False
        self._shutdown_lock = threading.Lock()
        self._message_count = 0
        self._error_count = 0
        self._last_report_time = time.time()

        signal.signal(signal.SIGINT, self._handle_signal)
        signal.signal(signal.SIGTERM, self._handle_signal)

    def start(self) -> None:
        """Initialize the Kafka producer."""
        try:
            self._producer = ConfluentProducer(self.config.producer_config)
            self._running = True
            logger.info(
                "Kafka producer started",
                extra={
                    "bootstrap_servers": self.config.bootstrap_servers,
                    "topic": self.config.topic,
                    "partitions": self.config.partitions,
                },
            )
        except Exception as e:
            raise KafkaProducerError(f"Failed to create Kafka producer: {e}") from e

    def _handle_signal(self, signum: int, _frame: Any) -> None:
        """Handle shutdown signals gracefully via flag (non-blocking)."""
        signal_name = signal.Signals(signum).name
        logger.info(f"Received {signal_name}, initiating graceful shutdown")
        self._shutdown = True
        # Signal the main loop to stop instead of calling stop() from handler
        self._running = False

    def _delivery_callback(self, err: Any, msg: Any) -> None:
        """Callback for Kafka delivery reports."""
        if err is not None:
            self._error_count += 1
            logger.error(
                "Message delivery failed",
                extra={
                    "error": str(err),
                    "topic": msg.topic() if msg else None,
                    "partition": msg.partition() if msg else None,
                },
            )
        else:
            self._message_count += 1

    def send(self, event: ClickstreamEvent, timeout: float = 10.0) -> bool:
        """Send an event to Kafka with retries."""
        if self._shutdown or not self._running or self._producer is None:
            raise KafkaProducerError("Producer is not running")

        event_dict = event.to_dict()
        key = event.event_id
        value = json.dumps(event_dict, default=str)
        partition = hash(event.session_id) % max(self.config.partitions, 1)

        max_retries = 3
        for attempt in range(max_retries):
            try:
                self._producer.produce(
                    topic=self.config.topic,
                    key=key,
                    value=value,
                    partition=partition,
                    headers={
                        "event_type": event.event_type,
                        "content_type": "application/json",
                        "schema_version": "1.0",
                    },
                    callback=self._delivery_callback,
                )
                self._producer.poll(0)
                return True
            except BufferError:
                logger.warning(
                    "Producer queue full, flushing",
                    extra={"attempt": attempt + 1},
                )
                self._producer.flush(timeout)
            except KafkaException as e:
                logger.error(
                    "Kafka error sending event",
                    extra={
                        "attempt": attempt + 1,
                        "max_retries": max_retries,
                        "error": str(e),
                    },
                )
                if attempt < max_retries - 1:
                    time.sleep(0.5 * (attempt + 1))
                else:
                    raise KafkaProducerError(f"Failed to send after {max_retries} retries") from e
        return False

    def send_batch(self, events: list[ClickstreamEvent]) -> int:
        """Send a batch of events."""
        sent = 0
        for event in events:
            try:
                if self.send(event):
                    sent += 1
            except KafkaProducerError:
                logger.exception("Failed to send event in batch")
        return sent

    def send_dead_letter(self, event: dict[str, Any], error: str) -> None:
        """Send malformed events to dead letter topic."""
        if self._producer is None:
            return
        try:
            dead_letter_value = json.dumps(
                {
                    "original_event": event,
                    "error": error,
                    "timestamp": time.time(),
                },
                default=str,
            )
            self._producer.produce(
                topic=self.config.dead_letter_topic,
                key=str(event.get("event_id", "unknown")),
                value=dead_letter_value,
                headers={
                    "error_type": "schema_validation",
                    "content_type": "application/json",
                },
            )
            self._producer.poll(0)
        except Exception as e:
            logger.error(f"Failed to send dead letter event: {e}")

    def flush(self, timeout: float = 30.0) -> int:
        """Flush pending messages."""
        if self._producer is None:
            return 0
        remaining: int = self._producer.flush(timeout) or 0
        if remaining > 0:
            logger.warning(
                "Messages may not have been delivered",
                extra={"remaining": remaining},
            )
        return remaining

    def stop(self) -> None:
        """Graceful shutdown with final flush."""
        with self._shutdown_lock:
            if self._shutdown:
                return
            self._shutdown = True

        logger.info("Shutting down Kafka producer")
        self._running = False
        remaining = self.flush()
        self._report_metrics()
        logger.info(
            "Kafka producer shutdown complete",
            extra={"remaining_messages": remaining},
        )

    def _report_metrics(self) -> None:
        """Log producer metrics."""
        elapsed = time.time() - self._last_report_time
        rate = self._message_count / elapsed if elapsed > 0 else 0
        logger.info(
            "Producer metrics summary",
            extra={
                "messages_sent": self._message_count,
                "errors": self._error_count,
                "uptime_seconds": round(elapsed, 2),
                "throughput_mps": round(rate, 2),
            },
        )

    @property
    def is_running(self) -> bool:
        return self._running and not self._shutdown
