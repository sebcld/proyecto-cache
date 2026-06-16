

import json
import time

from confluent_kafka import Producer

from kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_METRICS
from kafka_layer.message import QueryMessage


def _make_metrics_producer() -> Producer:

    return Producer({
        "bootstrap.servers":   KAFKA_BOOTSTRAP_SERVERS,
        "client.id":           "metrics-publisher",
        "acks":                "all",
        "enable.idempotence":  True,
        "linger.ms":           20,
        "compression.type":    "snappy",
        "retries":             5,
        "retry.backoff.ms":    200,
    })


class MetricsPublisher:


    def __init__(self):
        self._producer = _make_metrics_producer()

    def publish_terminal(
        self,
        msg: QueryMessage,
        *,
        final_status: str,    # "success" o "dlq"
        latency_ms: float,
        cache_result: str,    # "hit" o "miss"
        consumer_id: str,
    ) -> None:

        event = {
            "timestamp":    int(time.time() * 1000),     # epoch ms
            "query_id":     msg.query_id,
            "query_type":   msg.query_type,
            "latency_ms":   round(float(latency_ms), 4),
            "cache_result": cache_result,
            "retry_count":  msg.retry_count,
            "final_status": final_status,
            "consumer_id":  consumer_id,
        }

        try:
            self._producer.produce(
                topic=TOPIC_METRICS,
                key=msg.query_id.encode("utf-8"),
                value=json.dumps(event).encode("utf-8"),
            )
            self._producer.poll(0)
        except BufferError as e:

            print(f"[metrics-publisher] WARN buffer full: {e}")
            self._producer.poll(1)
        except Exception as e:
            print(f"[metrics-publisher] WARN produce failed: {e}")

    def close(self) -> None:
        """Flush con timeout corto para no bloquear el cierre del worker."""
        try:
            self._producer.flush(timeout=5)
        except Exception as e:
            print(f"[metrics-publisher] WARN flush failed: {e}")
