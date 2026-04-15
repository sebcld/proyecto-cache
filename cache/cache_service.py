"""
cache_service.py — Servicio de caché con Redis
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Intercepta consultas entrantes:
  - Cache HIT  → retorna el resultado almacenado en Redis
  - Cache MISS → delega al QueryEngine, almacena el resultado con TTL

Soporta configuración dinámica de:
  - Política de evicción (LRU, LFU, FIFO/random)
  - Tamaño máximo de caché
  - TTL por entrada
"""

import json
import time
from typing import Optional

import redis

from metrics.metrics_store import MetricsStore


class CacheService:
    """
    Capa de caché basada en Redis que envuelve al QueryEngine.

    Parámetros
    ----------
    redis_client : redis.Redis
        Conexión a Redis ya configurada.
    query_engine : QueryEngine
        Motor de consultas en memoria (Generador de Respuestas).
    metrics : MetricsStore
        Almacén de métricas del sistema.
    ttl : int
        Tiempo de vida por defecto de cada entrada en segundos.
    """

    def __init__(self, redis_client: redis.Redis, query_engine,
                 metrics: MetricsStore, ttl: int = 60):
        self._redis = redis_client
        self._engine = query_engine
        self._metrics = metrics
        self._ttl = ttl

        # Captura el baseline del contador acumulado de Redis para calcular
        # el delta por experimento (flushdb no resetea este contador).
        info = self._redis.info("stats")
        self._evicted_keys_baseline = info.get("evicted_keys", 0)

    # ── Configuración de Redis ───────────────────────────────────────────────

    def configure_redis(self, max_memory: str = "200mb",
                        eviction_policy: str = "allkeys-lru") -> None:
        """
        Configura los parámetros de caché en Redis.

        Parámetros
        ----------
        max_memory       : Límite de memoria (ej. "50mb", "200mb", "500mb")
        eviction_policy  : Política de evicción:
                           - "allkeys-lru" → LRU sobre todas las claves
                           - "allkeys-lfu" → LFU sobre todas las claves
                           - "allkeys-random" → FIFO-like (random eviction)
        """
        self._redis.config_set("maxmemory", max_memory)
        self._redis.config_set("maxmemory-policy", eviction_policy)
        print(f"[cache] Redis configurado: maxmemory={max_memory}, "
              f"policy={eviction_policy}")

    def set_ttl(self, ttl_seconds: int) -> None:
        """Actualiza el TTL por defecto para nuevas entradas."""
        self._ttl = ttl_seconds
        print(f"[cache] TTL actualizado a {ttl_seconds}s")

    # ── Procesamiento de consultas ───────────────────────────────────────────

    def process_query(self, query_type: str, **params) -> dict:
        """
        Procesa una consulta pasando por la capa de caché.

        1. Genera la cache_key según el tipo y parámetros
        2. Busca en Redis (HIT/MISS)
        3. En MISS, ejecuta la consulta vía QueryEngine y almacena
        4. Registra métricas

        Retorna el resultado con metadata adicional:
          - "from_cache": bool
          - "latency_ms": float (tiempo total incluyendo caché)
        """
        t0 = time.perf_counter()

        # Generar cache key
        cache_key = self._build_cache_key(query_type, **params)
        zone_id = params.get("zone_id", params.get("zone_a", ""))

        # Intentar obtener de Redis
        try:
            cached = self._redis.get(cache_key)
        except redis.RedisError as e:
            print(f"[cache] Error al leer Redis: {e}")
            cached = None

        if cached is not None:
            # ── CACHE HIT ────────────────────────────────────────────────
            result = json.loads(cached)
            latency_ms = (time.perf_counter() - t0) * 1000

            self._metrics.record_hit(
                query_type=query_type,
                cache_key=cache_key,
                latency_ms=latency_ms,
                zone_id=zone_id,
            )

            result["from_cache"] = True
            result["total_latency_ms"] = round(latency_ms, 4)
            return result

        # ── CACHE MISS ───────────────────────────────────────────────────
        # Verificar evictions antes de escribir
        pre_keys = self._redis.dbsize()

        # Ejecutar consulta en el motor (Generador de Respuestas)
        result = self._engine.run(query_type, **params)

        # Almacenar en Redis con TTL
        try:
            self._redis.setex(
                cache_key,
                self._ttl,
                json.dumps(result),
            )
        except redis.RedisError as e:
            print(f"[cache] Error al escribir Redis: {e}")

        # Detectar evictions (heurística: si el tamaño no creció)
        post_keys = self._redis.dbsize()
        if post_keys <= pre_keys and pre_keys > 0:
            self._metrics.record_eviction()

        latency_ms = (time.perf_counter() - t0) * 1000

        self._metrics.record_miss(
            query_type=query_type,
            cache_key=cache_key,
            latency_ms=latency_ms,
            zone_id=zone_id,
        )

        result["from_cache"] = False
        result["total_latency_ms"] = round(latency_ms, 4)
        return result

    # ── Generación de cache keys ─────────────────────────────────────────────

    @staticmethod
    def _build_cache_key(query_type: str, **params) -> str:
        """
        Genera la cache key según las convenciones del enunciado.

        Q1 → count:{zona_id}:conf={confidence_min}
        Q2 → area:{zona_id}:conf={confidence_min}
        Q3 → density:{zona_id}:conf={confidence_min}
        Q4 → compare:density:{zona_a}:{zona_b}:conf={confidence_min}
        Q5 → confidence_dist:{zona_id}:bins={bins}
        """
        if query_type == "Q1":
            return f"count:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif query_type == "Q2":
            return f"area:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif query_type == "Q3":
            return f"density:{params['zone_id']}:conf={params.get('confidence_min', 0.0)}"
        elif query_type == "Q4":
            return (f"compare:density:{params['zone_a']}:{params['zone_b']}"
                    f":conf={params.get('confidence_min', 0.0)}")
        elif query_type == "Q5":
            return f"confidence_dist:{params['zone_id']}:bins={params.get('bins', 5)}"
        else:
            # Fallback genérico
            return f"{query_type}:{json.dumps(params, sort_keys=True)}"

    # ── Utilidades ───────────────────────────────────────────────────────────

    def flush_cache(self) -> None:
        """Limpia todas las entradas de la caché."""
        self._redis.flushdb()
        print("[cache] Caché vaciada (FLUSHDB)")

    def get_cache_info(self) -> dict:
        """Retorna información del estado actual de Redis."""
        info = self._redis.info("memory")
        return {
            "used_memory_human":     info.get("used_memory_human", "?"),
            "used_memory_peak_human": info.get("used_memory_peak_human", "?"),
            "maxmemory_human":       info.get("maxmemory_human", "?"),
            "maxmemory_policy":      info.get("maxmemory_policy", "?"),
            "keys":                  self._redis.dbsize(),
            "ttl_default":           self._ttl,
        }

    def get_redis_evicted_keys(self) -> int:
        """
        Retorna el número de claves eviccionadas por Redis en este experimento.

        Lee el delta respecto al valor capturado al inicializar el servicio,
        ya que el contador 'evicted_keys' de Redis es acumulado desde que
        arrancó el servidor y flushdb() no lo resetea.
        """
        info = self._redis.info("stats")
        current = info.get("evicted_keys", 0)
        return current - self._evicted_keys_baseline
