"""
kafka_metrics.py — Colector de métricas extendidas del pipeline asíncrono.

Los consumers escriben contadores en Redis a medida que procesan (backend
compartido): así N workers agregan al mismo lugar y podemos medir el grupo
completo desde fuera. Este módulo LEE esos contadores y calcula las métricas
que pide la rúbrica de la Tarea 2:

  - retry rate     → reintentos por mensaje completado
  - recovery rate  → fracción de mensajes fallidos que terminó recuperándose
  - DLQ rate       → fracción de mensajes que acabó en la dead-letter queue
  - hit/miss rate y latencias p50/p95 (heredadas de la Tarea 1)

Contadores leídos (claves Redis, escritas por consumer.py y MetricsStore):
  metrics:processed   éxitos de procesamiento (incluye recuperados)
  metrics:hits        cache hits
  metrics:misses      cache misses
  metrics:retried     republicaciones a queries.retry
  metrics:recovered   mensajes que tuvieron ≥1 reintento y luego tuvieron éxito
  metrics:dlq         mensajes enviados a queries.dlq
  metrics:latencies:hit / :miss   listas de latencias (ms)

Uso:
    python -m kafka_layer.kafka_metrics          # imprime snapshot actual
"""

import sys
import json

import redis

from config import REDIS_HOST, REDIS_PORT, REDIS_DB


COUNTER_KEYS = ["processed", "hits", "misses", "retried", "recovered", "dlq"]
LATENCY_KEYS = {"hit": "metrics:latencies:hit", "miss": "metrics:latencies:miss"}


def _percentile(values: list[float], pct: float) -> float:
    if not values:
        return 0.0
    s = sorted(values)
    idx = min(int(len(s) * pct), len(s) - 1)
    return round(s[idx], 4)


class KafkaMetrics:
    """Lee y agrega los contadores del pipeline desde Redis."""

    def __init__(self, redis_client: redis.Redis):
        self._redis = redis_client

    # ── Reset ──────────────────────────────────────────────────────────────
    def reset(self) -> None:
        """Borra todos los contadores y listas de latencia (no toca la caché)."""
        keys = [f"metrics:{k}" for k in COUNTER_KEYS]
        keys += ["metrics:evictions", *LATENCY_KEYS.values()]
        self._redis.delete(*keys)

    # ── Lectura ──────────────────────────────────────────────────────────────
    def _counter(self, name: str) -> int:
        val = self._redis.get(f"metrics:{name}")
        return int(val) if val is not None else 0

    def _latencies(self, which: str) -> list[float]:
        raw = self._redis.lrange(LATENCY_KEYS[which], 0, -1)
        return [float(x) for x in raw]

    # ── Snapshot ───────────────────────────────────────────────────────────
    def snapshot(self) -> dict:
        """
        Devuelve el resumen completo de métricas del pipeline.

        Denominadores:
          terminal = processed + dlq  → mensajes originales que alcanzaron un
                     estado final (éxito o DLQ). Base para success/dlq rate.
          failed_once = recovered + dlq → mensajes que fallaron al menos una
                     vez. Base para recovery rate.
        """
        c = {k: self._counter(k) for k in COUNTER_KEYS}

        terminal = c["processed"] + c["dlq"]
        failed_once = c["recovered"] + c["dlq"]
        total_cache = c["hits"] + c["misses"]

        hit_lat = self._latencies("hit")
        miss_lat = self._latencies("miss")
        all_lat = hit_lat + miss_lat

        def rate(num, den):
            return round(num / den, 4) if den > 0 else 0.0

        return {
            # Volúmenes
            "processed":        c["processed"],
            "hits":             c["hits"],
            "misses":           c["misses"],
            "retried":          c["retried"],
            "recovered":        c["recovered"],
            "dlq":              c["dlq"],
            "terminal":         terminal,
            # Tasas del pipeline (Tarea 2)
            "retries_per_msg":  rate(c["retried"], terminal),
            "recovery_rate":    rate(c["recovered"], failed_once),
            "dlq_rate":         rate(c["dlq"], terminal),
            "success_rate":     rate(c["processed"], terminal),
            # Caché (Tarea 1)
            "hit_rate":         rate(c["hits"], total_cache),
            "miss_rate":        rate(c["misses"], total_cache),
            # Latencias (ms)
            "latency_p50_ms":   _percentile(all_lat, 0.50),
            "latency_p95_ms":   _percentile(all_lat, 0.95),
            "hit_p50_ms":       _percentile(hit_lat, 0.50),
            "miss_p50_ms":      _percentile(miss_lat, 0.50),
        }

    def print_summary(self) -> None:
        s = self.snapshot()
        print("\n" + "═" * 56)
        print("  MÉTRICAS DEL PIPELINE KAFKA (Tarea 2)")
        print("═" * 56)
        print(f"  Procesados (éxito):    {s['processed']:,}")
        print(f"  Reintentos (eventos):  {s['retried']:,}")
        print(f"  Recuperados:           {s['recovered']:,}")
        print(f"  En DLQ:                {s['dlq']:,}")
        print(f"  Terminal (proc+dlq):   {s['terminal']:,}")
        print("  " + "─" * 52)
        print(f"  Retries / mensaje:     {s['retries_per_msg']:.3f}")
        print(f"  Recovery rate:         {s['recovery_rate']:.2%}")
        print(f"  DLQ rate:              {s['dlq_rate']:.2%}")
        print(f"  Success rate:          {s['success_rate']:.2%}")
        print("  " + "─" * 52)
        print(f"  Hit rate:              {s['hit_rate']:.2%}")
        print(f"  Latencia p50 / p95:    {s['latency_p50_ms']:.3f} / "
              f"{s['latency_p95_ms']:.3f} ms")
        print("═" * 56)


def main() -> int:
    r = redis.Redis(host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB,
                    decode_responses=True)
    metrics = KafkaMetrics(r)
    print(json.dumps(metrics.snapshot(), indent=2))
    return 0


if __name__ == "__main__":
    sys.exit(main())
