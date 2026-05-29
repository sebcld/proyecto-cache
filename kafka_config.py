
import os

# 
# Dentro de docker: "kafka:9092" (listener interno PLAINTEXT)
# Desde el host:    "localhost:9094" (listener PLAINTEXT_HOST)
KAFKA_BOOTSTRAP_SERVERS = os.getenv("KAFKA_BOOTSTRAP_SERVERS", "kafka:9092")

# Topcos del sistema
# 
TOPIC_QUERIES = "queries"            # flujo principal: producer -> consumers
TOPIC_RETRY   = "queries.retry"      # reintentos tras fallo temporal
TOPIC_DLQ     = "queries.dlq"        # mensajes que excedieron MAX_RETRIES

# Particionado:
#   - queries y queries.retry con 8 particiones permite escalar hasta 8
#     consumers concurrentes dentro del mismo grupo (cada uno toma ≥1 particonn).
NUM_PARTITIONS_QUERIES = int(os.getenv("KAFKA_PARTITIONS_QUERIES", 8))
NUM_PARTITIONS_RETRY   = int(os.getenv("KAFKA_PARTITIONS_RETRY", 8))
NUM_PARTITIONS_DLQ     = int(os.getenv("KAFKA_PARTITIONS_DLQ", 1))

# Replicacion: 1 
REPLICATION_FACTOR = int(os.getenv("KAFKA_REPLICATION_FACTOR", 1))

# retncion: 1h en topics activos.
RETENTION_MS_QUERIES = os.getenv("KAFKA_RETENTION_MS_QUERIES", "3600000")
RETENTION_MS_RETRY   = os.getenv("KAFKA_RETENTION_MS_RETRY",   "3600000")
RETENTION_MS_DLQ     = os.getenv("KAFKA_RETENTION_MS_DLQ",     "86400000")

# Estructura usada por topics.py para crear todo de forma idempotente.
TOPIC_CONFIGS = {
    TOPIC_QUERIES: {"partitions": NUM_PARTITIONS_QUERIES, "retention_ms": RETENTION_MS_QUERIES},
    TOPIC_RETRY:   {"partitions": NUM_PARTITIONS_RETRY,   "retention_ms": RETENTION_MS_RETRY},
    TOPIC_DLQ:     {"partitions": NUM_PARTITIONS_DLQ,     "retention_ms": RETENTION_MS_DLQ},
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

# Si es True, el Generador de Respuestas se considera caido: TODAS las consultas con cache miss fallan inmediatamente. Sirve para simular la
# caida total del backend (escenario 4 del enunciado).
RESPONSES_DOWN = os.getenv("RESPONSES_DOWN", "false").lower() == "true"
