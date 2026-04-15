"""
metrics_store.py — Almacenamiento y análisis de métricas del sistema
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Registra hits, misses, latencias, throughput y evictions.
Exporta resultados a CSV y genera resúmenes para el informe.
"""

import time
import json
import os
import csv
import threading
from collections import defaultdict
from typing import Optional

import redis


class MetricsStore:
    """
    Almacena métricas de rendimiento del sistema de caché.

    Soporta dos modos:
      - En memoria (para pruebas locales)
      - Redis (para entorno distribuido con Docker)

    Cada evento registrado contiene:
      - timestamp, query_type, zone_id, cache_key
      - hit (bool), latency_ms, source ("cache" | "engine")
    """

    def __init__(self, redis_client: Optional[redis.Redis] = None):
        self._redis = redis_client
        self._lock = threading.Lock()

        # Almacenamiento en memoria
        self._events: list[dict] = []
        self._hits = 0
        self._misses = 0
        self._evictions = 0
        self._start_time = time.time()

    # ── Registro de eventos ──────────────────────────────────────────────────

    def record_hit(self, query_type: str, cache_key: str,
                   latency_ms: float, zone_id: str = "") -> None:
        """Registra un cache hit."""
        with self._lock:
            self._hits += 1
            self._events.append({
                "timestamp":  time.time(),
                "query_type": query_type,
                "zone_id":    zone_id,
                "cache_key":  cache_key,
                "hit":        True,
                "latency_ms": round(latency_ms, 4),
                "source":     "cache",
            })

        if self._redis:
            self._redis.incr("metrics:hits")
            self._redis.lpush("metrics:latencies:hit",
                              str(round(latency_ms, 4)))

    def record_miss(self, query_type: str, cache_key: str,
                    latency_ms: float, zone_id: str = "") -> None:
        """Registra un cache miss."""
        with self._lock:
            self._misses += 1
            self._events.append({
                "timestamp":  time.time(),
                "query_type": query_type,
                "zone_id":    zone_id,
                "cache_key":  cache_key,
                "hit":        False,
                "latency_ms": round(latency_ms, 4),
                "source":     "engine",
            })

        if self._redis:
            self._redis.incr("metrics:misses")
            self._redis.lpush("metrics:latencies:miss",
                              str(round(latency_ms, 4)))

    def record_eviction(self) -> None:
        """Registra una evicción de la caché."""
        with self._lock:
            self._evictions += 1
        if self._redis:
            self._redis.incr("metrics:evictions")

    # ── Consulta de métricas ─────────────────────────────────────────────────

    def get_summary(self) -> dict:
        """
        Retorna un resumen completo de las métricas recopiladas.

        Incluye: hit_rate, miss_rate, throughput, latencia p50/p95,
        eviction_rate y cache_efficiency.
        """
        with self._lock:
            total = self._hits + self._misses
            elapsed_s = time.time() - self._start_time

            if total == 0:
                return {
                    "total_queries": 0,
                    "hits": 0,
                    "misses": 0,
                    "hit_rate": 0.0,
                    "miss_rate": 0.0,
                    "throughput_qps": 0.0,
                    "latency_p50_ms": 0.0,
                    "latency_p95_ms": 0.0,
                    "evictions": self._evictions,
                    "eviction_rate_per_min": 0.0,
                    "cache_efficiency": 0.0,
                    "elapsed_seconds": round(elapsed_s, 2),
                }

            # Latencias
            latencies = [e["latency_ms"] for e in self._events]
            latencies_sorted = sorted(latencies)
            p50_idx = int(len(latencies_sorted) * 0.50)
            p95_idx = min(int(len(latencies_sorted) * 0.95), len(latencies_sorted) - 1)

            # Hit / miss latencies promedio para cache_efficiency
            hit_lats = [e["latency_ms"] for e in self._events if e["hit"]]
            miss_lats = [e["latency_ms"] for e in self._events if not e["hit"]]
            avg_hit_lat = sum(hit_lats) / len(hit_lats) if hit_lats else 0
            avg_miss_lat = sum(miss_lats) / len(miss_lats) if miss_lats else 0

            # Cache efficiency = (hits * t_cache - misses * t_engine) / total
            cache_efficiency = (
                (self._hits * avg_hit_lat - self._misses * avg_miss_lat) / total
            )

            elapsed_min = elapsed_s / 60 if elapsed_s > 0 else 1

            return {
                "total_queries":       total,
                "hits":                self._hits,
                "misses":              self._misses,
                "hit_rate":            round(self._hits / total, 4),
                "miss_rate":           round(self._misses / total, 4),
                "throughput_qps":      round(total / elapsed_s, 2) if elapsed_s > 0 else 0,
                "latency_p50_ms":      round(latencies_sorted[p50_idx], 4),
                "latency_p95_ms":      round(latencies_sorted[p95_idx], 4),
                "avg_hit_latency_ms":  round(avg_hit_lat, 4),
                "avg_miss_latency_ms": round(avg_miss_lat, 4),
                "evictions":           self._evictions,
                "eviction_rate_per_min": round(self._evictions / elapsed_min, 2),
                "cache_efficiency":    round(cache_efficiency, 4),
                "elapsed_seconds":     round(elapsed_s, 2),
            }

    def get_events(self) -> list[dict]:
        """Retorna todos los eventos registrados (copia)."""
        with self._lock:
            return list(self._events)

    def get_per_query_summary(self) -> dict:
        """Retorna métricas desglosadas por tipo de consulta (Q1-Q5)."""
        with self._lock:
            by_type = defaultdict(lambda: {"hits": 0, "misses": 0, "latencies": []})
            for e in self._events:
                qt = e["query_type"]
                if e["hit"]:
                    by_type[qt]["hits"] += 1
                else:
                    by_type[qt]["misses"] += 1
                by_type[qt]["latencies"].append(e["latency_ms"])

            result = {}
            for qt, data in sorted(by_type.items()):
                total = data["hits"] + data["misses"]
                lats = sorted(data["latencies"])
                p50 = lats[int(len(lats) * 0.50)] if lats else 0
                p95 = lats[min(int(len(lats) * 0.95), len(lats) - 1)] if lats else 0
                result[qt] = {
                    "total":  total,
                    "hits":   data["hits"],
                    "misses": data["misses"],
                    "hit_rate": round(data["hits"] / total, 4) if total > 0 else 0,
                    "latency_p50_ms": round(p50, 4),
                    "latency_p95_ms": round(p95, 4),
                }
            return result

    # ── Exportación ──────────────────────────────────────────────────────────

    def export_events_csv(self, filepath: str) -> None:
        """Exporta todos los eventos a un archivo CSV."""
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        with self._lock:
            events = list(self._events)

        if not events:
            print(f"[metrics] Sin eventos para exportar.")
            return

        with open(filepath, "w", newline="") as f:
            writer = csv.DictWriter(f, fieldnames=events[0].keys())
            writer.writeheader()
            writer.writerows(events)

        print(f"[metrics] {len(events)} eventos exportados → {filepath}")

    def export_summary_json(self, filepath: str) -> None:
        """Exporta el resumen de métricas a un archivo JSON."""
        os.makedirs(os.path.dirname(filepath) or ".", exist_ok=True)
        summary = self.get_summary()
        summary["per_query"] = self.get_per_query_summary()

        with open(filepath, "w") as f:
            json.dump(summary, f, indent=2)

        print(f"[metrics] Resumen exportado → {filepath}")

    def reset(self) -> None:
        """Reinicia todas las métricas."""
        with self._lock:
            self._events.clear()
            self._hits = 0
            self._misses = 0
            self._evictions = 0
            self._start_time = time.time()
        if self._redis:
            self._redis.delete("metrics:hits", "metrics:misses",
                               "metrics:evictions",
                               "metrics:latencies:hit",
                               "metrics:latencies:miss")

    def print_summary(self) -> None:
        """Imprime un resumen formateado a consola."""
        s = self.get_summary()
        print("\n" + "═" * 60)
        print("  RESUMEN DE MÉTRICAS DEL SISTEMA DE CACHÉ")
        print("═" * 60)
        print(f"  Total consultas:       {s['total_queries']:,}")
        print(f"  Hits:                  {s['hits']:,}")
        print(f"  Misses:                {s['misses']:,}")
        print(f"  Hit rate:              {s['hit_rate']:.2%}")
        print(f"  Miss rate:             {s['miss_rate']:.2%}")
        print(f"  Throughput:            {s['throughput_qps']:.1f} q/s")
        print(f"  Latencia p50:          {s['latency_p50_ms']:.3f} ms")
        print(f"  Latencia p95:          {s['latency_p95_ms']:.3f} ms")
        print(f"  Avg hit latency:       {s['avg_hit_latency_ms']:.3f} ms")
        print(f"  Avg miss latency:      {s['avg_miss_latency_ms']:.3f} ms")
        print(f"  Evictions:             {s['evictions']:,}")
        print(f"  Eviction rate:         {s['eviction_rate_per_min']:.1f} /min")
        print(f"  Cache efficiency:      {s['cache_efficiency']:.4f}")
        print(f"  Tiempo total:          {s['elapsed_seconds']:.1f} s")
        print("═" * 60)

        per_q = self.get_per_query_summary()
        if per_q:
            print("\n  Desglose por tipo de consulta:")
            print(f"  {'Tipo':<6} {'Total':>7} {'Hits':>7} {'Misses':>7} "
                  f"{'Hit%':>8} {'p50ms':>8} {'p95ms':>8}")
            print("  " + "─" * 54)
            for qt, d in per_q.items():
                print(f"  {qt:<6} {d['total']:>7} {d['hits']:>7} {d['misses']:>7} "
                      f"{d['hit_rate']:>7.1%} {d['latency_p50_ms']:>8.3f} "
                      f"{d['latency_p95_ms']:>8.3f}")
        print()