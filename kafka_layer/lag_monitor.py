"""
lag_monitor.py — Backlog (consumer lag) del grupo de workers.

El "backlog" mide cuántos mensajes hay pendientes de consumir: por cada
partición es la diferencia entre el final del log (high watermark) y el último
offset commiteado por el grupo. Sumado sobre todas las particiones de `queries`
y `queries.retry` da el backlog total del sistema.

Es la señal clave para evaluar el escalado horizontal y los spikes:
  - backlog que crece  → los consumers no dan abasto (saturación).
  - backlog que drena   → el grupo se está poniendo al día.

Se usa AdminClient para descubrir las particiones de cada tópico, y un Consumer
para leer los offsets commiteados del grupo y los watermarks de cada partición
(operaciones que no requieren unirse al grupo, así no perturban el balanceo).

Uso:
    python -m kafka_layer.lag_monitor              # snapshot único
    python -m kafka_layer.lag_monitor --watch 1    # refresca cada 1s
"""

import sys
import time
import argparse

from confluent_kafka import Consumer, TopicPartition, KafkaException
from confluent_kafka.admin import AdminClient

from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS, CONSUMER_GROUP,
    TOPIC_QUERIES, TOPIC_RETRY,
)

# OFFSET_INVALID (-1001): el grupo aún no commiteó nada en esa partición.
_OFFSET_INVALID = -1001


def _discover_partitions(admin: AdminClient, topics: list[str]) -> list[TopicPartition]:
    """Lista las particiones existentes de los tópicos dados (vía AdminClient)."""
    md = admin.list_topics(timeout=10)
    tps = []
    for topic in topics:
        meta = md.topics.get(topic)
        if meta is None or meta.error is not None:
            continue
        for partition_id in meta.partitions:
            tps.append(TopicPartition(topic, partition_id))
    return tps


def get_lag(group: str = CONSUMER_GROUP,
            topics: tuple[str, ...] = (TOPIC_QUERIES, TOPIC_RETRY)) -> dict:
    """
    Calcula el backlog del grupo.

    Retorna:
      {
        "total_lag": int,
        "by_topic":  {topic: lag, ...},
        "by_partition": {"topic-part": {"committed", "high", "lag"}, ...}
      }
    """
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    tps = _discover_partitions(admin, list(topics))

    # Consumer efímero solo para consultar offsets/watermarks del grupo.
    consumer = Consumer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "group.id":           group,
        "enable.auto.commit": False,
    })

    by_topic: dict[str, int] = {t: 0 for t in topics}
    by_partition: dict[str, dict] = {}
    total = 0

    try:
        # El coordinador del grupo puede no estar listo justo tras arrancar el
        # broker (NOT_COORDINATOR / coordinador cargando): reintentamos breve.
        committed = None
        for attempt in range(5):
            try:
                committed = consumer.committed(tps, timeout=10)
                break
            except KafkaException as e:
                if attempt == 4:
                    raise
                time.sleep(1)

        for tp in committed:
            low, high = consumer.get_watermark_offsets(tp, timeout=10, cached=False)
            # Si el grupo no commiteó aún, lo pendiente es todo lo que hay (high-low).
            consumed = tp.offset if tp.offset is not None and tp.offset >= 0 else low
            lag = max(0, high - consumed)
            total += lag
            by_topic[tp.topic] = by_topic.get(tp.topic, 0) + lag
            by_partition[f"{tp.topic}-{tp.partition}"] = {
                "committed": tp.offset if tp.offset >= 0 else None,
                "high":      high,
                "lag":       lag,
            }
    finally:
        consumer.close()

    return {"total_lag": total, "by_topic": by_topic, "by_partition": by_partition}


def _print_lag(lag: dict) -> None:
    by_topic = " | ".join(f"{t}={n}" for t, n in lag["by_topic"].items())
    print(f"[lag] backlog total={lag['total_lag']:<6}  {by_topic}")


def main() -> int:
    parser = argparse.ArgumentParser(description="Monitor de backlog Kafka")
    parser.add_argument("--watch", type=float, default=0,
                        help="Refresca cada N segundos (0 = snapshot único)")
    args = parser.parse_args()

    if args.watch > 0:
        try:
            while True:
                _print_lag(get_lag())
                time.sleep(args.watch)
        except KeyboardInterrupt:
            return 130
    else:
        _print_lag(get_lag())
    return 0


if __name__ == "__main__":
    sys.exit(main())
