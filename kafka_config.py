
import os


KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")


TOPIC_QUERIES = "queries"            # flujo principal: producer -> consumers
TOPIC_RETRY   = "queries.retry"      # reintentos tras fallo temporal
TOPIC_DLQ     = "queries.dlq"        # mensajes que excedieron MAX_RETRIES

# Tarea 3: topico dedicado para eventos individuales de metrica.

TOPIC_METRICS = os.getenv("KAFKA_METRICS_TOPIC", "metrics-topic")

# Particionado:
#   - queries y queries.retry con 8 particiones permite escalar hasta 8
#     consumers concurrentes dentro del mismo grupo (cada uno toma ≥1 particonn).
#   - metrics-topic con 8 particiones para no ser cuello de botella del
#     plano de observabilidad y permitir que Spark consuma en paralelo.
NUM_PARTITIONS_QUERIES = int(os.getenv("KAFKA_PARTITIONS_QUERIES", 8))
NUM_PARTITIONS_RETRY   = int(os.getenv("KAFKA_PARTITIONS_RETRY", 8))
NUM_PARTITIONS_DLQ     = int(os.getenv("KAFKA_PARTITIONS_DLQ", 1))
NUM_PARTITIONS_METRICS = int(os.getenv("KAFKA_PARTITIONS_METRICS", 8))

# Replicacion: 1
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", 1))

# retncion: 1h en topics activos.
RETENTION_MS_QUERIES = os.getenv("KAFKA_RETENTION_MS_QUERIES", "3600000")
RETENTION_MS_RETRY   = os.getenv("KAFKA_RETENTION_MS_RETRY",   "3600000")
RETENTION_MS_DLQ     = os.getenv("KAFKA_RETENTION_MS_DLQ",     "86400000")
RETENTION_MS_METRICS = os.getenv("KAFKA_RETENTION_MS_METRICS", "3600000")

# Estructura usada por topics.py para crear todo de forma idempotente.
TOPIC_CONFIGS = {
    TOPIC_QUERIES: {"partitions": NUM_PARTITIONS_QUERIES, "retention_ms": RETENTION_MS_QUERIES},
    TOPIC_RETRY:   {"partitions": NUM_PARTITIONS_RETRY,   "retention_ms": RETENTION_MS_RETRY},
    TOPIC_DLQ:     {"partitions": NUM_PARTITIONS_DLQ,     "retention_ms": RETENTION_MS_DLQ},
    TOPIC_METRICS: {"partitions": NUM_PARTITIONS_METRICS, "retention_ms": RETENTION_MS_METRICS},
}

# 
# Consumidores
# 
# Todos los consumers comparten group.id para que Kafka distribuya las
# particiones automaicamente (balanceo horizontal).
CONSUMER_GROUP = os.getenv("KAFKA_CONSUMER_GROUP", "query-workers")

# Politica de reintentos
# 
# Tras MAX_RETRIES fallos consecutivos, la consulta se envia a la DLQ.
MAX_RETRIES = int(os.getenv("MAX_RETRIES", 3))


# Inyeccion de fallas (consumer)
# 
# Probabilidad de error aleatorio por consulta (0.0 - 1.0).
FAIL_RATE = float(os.getenv("FAIL_RATE", 0.0))

# Latencia artificial añadida al procesamiento (ms).
ARTIFICIAL_LATENCY_MS = float(os.getenv("ARTIFICIAL_LATENCY_MS", 0))

# Todos los fallos se inyectan en el camino del cache MISS (Generador de
# Respuestas). Los cache HIT NUNCA fallan: Redis sigue sirviendo lo que ya
# tiene, reflejando que la cache amortigua la caida del backend.

# Caida PERMANENTE: el Generador de Respuestas falla durante todo el run.
RESPONSES_DOWN = os.getenv("RESPONSES_DOWN", "false").lower() == "true"

# Caida TEMPORAL (escenario "falla temporal")
BACKEND_DOWN_START_S    = float(os.getenv("BACKEND_DOWN_START_S", 0))
BACKEND_DOWN_DURATION_S = float(os.getenv("BACKEND_DOWN_DURATION_S", 0))

# Backoff entre reintentos: al consumir de queries.retry, el worker espera a
# que hayan pasado al menos RETRY_BACKOFF_S desde last_attempt_at antes de
# reprocesar.
RETRY_BACKOFF_S = float(os.getenv("RETRY_BACKOFF_S", 2.0))
