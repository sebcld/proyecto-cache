"""
Uso:
    python -m kafka_layer.topics
"""

import sys
import time

from confluent_kafka.admin import AdminClient, NewTopic
from confluent_kafka import KafkaError, KafkaException

from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_CONFIGS,
    REPLICATION_FACTOR,
)


def _build_new_topics() -> list[NewTopic]:
    """Construye la lista de NewTopic a partir de TOPIC_CONFIGS."""
    return [
        NewTopic(
            topic=name,
            num_partitions=cfg["partitions"],
            replication_factor=REPLICATION_FACTOR,
            config={"retention.ms": cfg["retention_ms"]},
        )
        for name, cfg in TOPIC_CONFIGS.items()
    ]


def create_topics() -> None:
    """
    Crea los topicos definidos en kafka_config.TOPIC_CONFIGS.
    """
    admin = AdminClient({"bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS})
    print(f"[topics] Conectando al broker en {KAFKA_BOOTSTRAP_SERVERS}...")

    new_topics = _build_new_topics()
    futures = admin.create_topics(new_topics, request_timeout=30)

    for topic, future in futures.items():
        try:
            future.result()
            cfg = TOPIC_CONFIGS[topic]
            print(f"[topics] ✓ Creado: {topic} "
                  f"(particiones={cfg['partitions']}, "
                  f"retencion={cfg['retention_ms']}ms)")
        except KafkaException as e:
            err = e.args[0]
            if err.code() == KafkaError.TOPIC_ALREADY_EXISTS:
                print(f"[topics] = Ya existe: {topic} (se mantiene)")
            else:
                print(f"[topics] ✗ Error en {topic}: {err}")
                raise


def main() -> int:
    """
    Reintenta hasta 10 veces con 3s entre intentos por si Kafka au
    estaterminando de levantar metadata cuando el healthcheck ya paso.
    """
    last_error: Exception | None = None
    for attempt in range(1, 11):
        try:
            create_topics()
            print("[topics] Listo.")
            return 0
        except Exception as e:
            last_error = e
            print(f"[topics] Intento {attempt}/10 fallo: {e}")
            time.sleep(3)

    print(f"[topics] FATAL: no se pudo crear los topicos. ultimo error: {last_error}")
    return 1


if __name__ == "__main__":
    sys.exit(main())
