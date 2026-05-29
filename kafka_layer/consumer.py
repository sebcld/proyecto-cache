"""
consumer.py ------ consume consultas desde Kafka y las procesa.

Cada worker:
  1. Se suscribe a queries y queries.retry con el mismo group.id
     (CONSUMER_GROUP). Kafka reparte las particiones entre todos los
     workers del grupo -> escalado horizontal automatico.
  2. Por cada mensaje, reconstruye la QueryMessage y la procesa pasando
     por la MISMA capa de cache de la Tarea 1 (CacheService.process_query).
  3. Hace commit MANUAL del offset SOLO tras manejar el mensaje
     (semantica at-least-once): si el worker muere antes del commit, el
     mensaje se reprocesa.

Manejo de fallos (retry / DLQ):
  - Si el procesamiento lanza excepcion:
      · retry_count < MAX_RETRIES -> se republica en queries.retry con
        retry_count+1 (mismo query_id, created_at preservado).
      · retry_count >= MAX_RETRIES -> se envia a queries.dlq
  - En ambos casos se hace commit del offset original: el mensaje ya fue
    "enrutado" a su siguiente destino, no debe reprocesarse desde aqui.

Inyeccion de fallos (config por envv)
  - FAIL_RATE             -> probabilidad de fallo aleatorio por consulta.
  - RESPONSES_DOWN        -> fuerza fallo de todo  procesamiento (backend caido).
  - ARTIFICIAL_LATENCY_MS -> latencia extra antes de procesar.

Uso:
    python -m kafka_layer.consumer
    docker compose ... up --scale consumer=4
"""

import os
import sys
import time
import random
import signal

import redis
from confluent_kafka import Consumer, Producer, KafkaError

from kafka_config import (
    KAFKA_BOOTSTRAP_SERVERS,
    TOPIC_QUERIES, TOPIC_RETRY, TOPIC_DLQ,
    CONSUMER_GROUP, MAX_RETRIES,
    FAIL_RATE, ARTIFICIAL_LATENCY_MS, RESPONSES_DOWN,
)
from kafka_layer.message import QueryMessage

from config import (
    DATASET_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB,
    CACHE_MAX_MEMORY, CACHE_EVICTION_POLICY, CACHE_TTL_SECONDS,
)
from data.loader import load_dataset, ZONE_AREAS_KM2
from query_engine.queries import QueryEngine
from cache.cache_service import CacheService
from metrics.metrics_store import MetricsStore



# Factories
# 

def make_consumer() -> Consumer:
    """
    Consumer con commit manual y reparto cooperativo de particiones.

    - enable.auto.commit=False -> controlamos el commit nosotros (at-least-once).
    - auto.offset.reset=earliest -> un grupo nuevo arranca desde el inicio.
    - cooperative-sticky -> rebalanceo incremental al escalar workers, sin
      detener todo el grupo (stop-the-world).
    """
    return Consumer({
        "bootstrap.servers":          KAFKA_BOOTSTRAP_SERVERS,
        "group.id":                   CONSUMER_GROUP,
        "enable.auto.commit":         False,
        "auto.offset.reset":          "earliest",
        "partition.assignment.strategy": "cooperative-sticky",
    })


def make_producer() -> Producer:
    """Producer auxiliar para republicar en retry/DLQ con durabilidad."""
    return Producer({
        "bootstrap.servers":  KAFKA_BOOTSTRAP_SERVERS,
        "client.id":          "consumer-republisher",
        "acks":               "all",
        "enable.idempotence": True,
    })



# Inyeccion de fallos 

class ProcessingError(Exception):
    """Error simulado de procesamiento (para ejercitar retry/DLQ)."""


def _maybe_inject_fault() -> None:
    """
    Aplica latencia artificial y, si corresponde, lanza ProcessingError.

    Se invoca ANTES de tocar la cache para simular un backend degradado o
    caido. La lógica fina (caida solo del Generador de Respuestas, ventanas
    temporales.
    """
    if ARTIFICIAL_LATENCY_MS > 0:
        time.sleep(ARTIFICIAL_LATENCY_MS / 1000.0)

    if RESPONSES_DOWN:
        raise ProcessingError("RESPONSES_DOWN: backend de respuestas caido")

    if FAIL_RATE > 0 and random.random() < FAIL_RATE:
        raise ProcessingError(f"fallo aleatorio (FAIL_RATE={FAIL_RATE})")


# Worker


class ConsumerWorker:
    def __init__(self):
        self.worker_id = os.getenv("HOSTNAME", f"worker-{os.getpid()}")
        self._running = True

        #  Dependencias de la Tarea 1 
        print(f"[consumer:{self.worker_id}] Cargando dataset...")
        data = load_dataset(DATASET_PATH, verbose=False)
        engine = QueryEngine(data, ZONE_AREAS_KM2)

        self._redis = redis.Redis(
            host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
            decode_responses=True,
        )
        # Metricas compartidas via Redis: todos los workers agregan al mismo
        # backend -> permite medir el grupo completo
        self._metrics = MetricsStore(redis_client=self._redis)
        self._cache = CacheService(
            redis_client=self._redis,
            query_engine=engine,
            metrics=self._metrics,
            ttl=CACHE_TTL_SECONDS,
        )

        #  Kafka 
        self._consumer = make_consumer()
        self._producer = make_producer()

    #  Republicacion a retry / DLQ 

    def _route_failure(self, msg: QueryMessage, reason: str) -> None:
        """Decide retry vs DLQ segun retry_count y republica el mensaje."""
        if msg.retry_count >= MAX_RETRIES:
            target = TOPIC_DLQ
            payload = msg  # se preserva tal cual para inspeccion post-mortem
            self._redis.incr("metrics:dlq")
            print(f"[consumer:{self.worker_id}] ✗ DLQ {msg.query_id} "
                  f"(retry_count={msg.retry_count}) — {reason}")
        else:
            target = TOPIC_RETRY
            payload = msg.with_retry()
            self._redis.incr("metrics:retried")
            print(f"[consumer:{self.worker_id}] ↻ retry {msg.query_id} "
                  f"-> intento {payload.retry_count}/{MAX_RETRIES} — {reason}")

        self._producer.produce(topic=target, value=payload.to_json_bytes())
        self._producer.poll(0)

    #  Procesamiento de un mensaje 

    def _handle(self, msg: QueryMessage) -> None:
        """Procesa una consulta; en caso de fallo la enruta a retry/DLQ."""
        try:
            _maybe_inject_fault()
            self._cache.process_query(msg.query_type, **msg.params)
            self._redis.incr("metrics:processed")
            if msg.retry_count > 0:
                # Se recupero tras uno o mas reintentos.
                self._redis.incr("metrics:recovered")
        except Exception as e:
            self._route_failure(msg, reason=str(e))

    #  Loop principal 

    def run(self) -> int:
        self._consumer.subscribe([TOPIC_QUERIES, TOPIC_RETRY])
        print(f"[consumer:{self.worker_id}] Suscrito a "
              f"[{TOPIC_QUERIES}, {TOPIC_RETRY}] group={CONSUMER_GROUP}")
        if FAIL_RATE or RESPONSES_DOWN or ARTIFICIAL_LATENCY_MS:
            print(f"[consumer:{self.worker_id}] Fallos: FAIL_RATE={FAIL_RATE} "
                  f"RESPONSES_DOWN={RESPONSES_DOWN} "
                  f"LATENCY_MS={ARTIFICIAL_LATENCY_MS}")

        try:
            while self._running:
                record = self._consumer.poll(1.0)
                if record is None:
                    continue
                if record.error():
                    if record.error().code() == KafkaError._PARTITION_EOF:
                        continue
                    print(f"[consumer:{self.worker_id}] error Kafka: "
                          f"{record.error()}", file=sys.stderr)
                    continue

                try:
                    msg = QueryMessage.from_json_bytes(record.value())
                except Exception as e:
                    # Mensaje no parseable: lo descartamos y commiteamos para
                    # no bloquear la particion (no es reintenable).
                    print(f"[consumer:{self.worker_id}] mensaje invalido "
                          f"descartado: {e}", file=sys.stderr)
                    self._consumer.commit(record, asynchronous=False)
                    continue

                self._handle(msg)

                # Commit MANUAL tras manejar el mensaje (at-least-once).
                self._consumer.commit(record, asynchronous=False)
        finally:
            print(f"[consumer:{self.worker_id}] Cerrando: flush + commit final...")
            self._producer.flush(10)
            self._consumer.close()
        return 0

    def stop(self, *_):
        print(f"[consumer:{self.worker_id}] Señal de parada recibida.")
        self._running = False


def main() -> int:
    worker = ConsumerWorker()
    signal.signal(signal.SIGTERM, worker.stop)
    signal.signal(signal.SIGINT, worker.stop)
    return worker.run()


if __name__ == "__main__":
    sys.exit(main())
