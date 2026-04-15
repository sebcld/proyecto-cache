"""
run_experiments.py — Ejecuta todos los experimentos para el informe
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Automatiza la ejecución de combinaciones de:
  - Distribución de tráfico (zipf / uniform)
  - Política de evicción (LRU / LFU / FIFO)
  - Tamaño de caché (50MB / 200MB / 500MB)
  - TTL (30s / 60s / 120s)

Uso:
    python run_experiments.py              # todos los experimentos
    python run_experiments.py --quick      # subconjunto rápido
"""

import os
import sys
import json
import argparse
import time
import itertools

import redis

from config import DATASET_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB
from data.loader import load_dataset, ZONE_AREAS_KM2
from query_engine.queries import QueryEngine
from cache.cache_service import CacheService
from metrics.metrics_store import MetricsStore
from traffic_generator.generator import TrafficGenerator


# ──────────────────────────────────────────────────────────────────────────────
# Configuraciones experimentales
# ──────────────────────────────────────────────────────────────────────────────

DISTRIBUTIONS   = ["zipf", "uniform"]
EVICTION_POLICIES = ["allkeys-lru", "allkeys-lfu", "allkeys-random"]
CACHE_SIZES     = ["50mb", "200mb", "500mb"]
TTL_VALUES      = [30, 60, 120]

# Para modo --quick
QUICK_DISTRIBUTIONS = ["zipf", "uniform"]
QUICK_POLICIES      = ["allkeys-lru", "allkeys-lfu"]
QUICK_SIZES         = ["50mb", "200mb"]
QUICK_TTLS          = [60]


def run_single_experiment(engine, redis_client, distribution, eviction_policy,
                          cache_size, ttl, total_queries, qps, seed,
                          output_base_dir) -> dict:
    """Ejecuta un solo experimento y retorna el resumen."""

    tag = f"{distribution}_{eviction_policy}_{cache_size}_ttl{ttl}"
    print(f"\n{'─' * 60}")
    print(f"  EXPERIMENTO: {tag}")
    print(f"{'─' * 60}")

    # Reiniciar Redis
    redis_client.flushdb()

    # Inicializar métricas frescas
    metrics = MetricsStore(redis_client=redis_client)
    cache = CacheService(
        redis_client=redis_client,
        query_engine=engine,
        metrics=metrics,
        ttl=ttl,
    )
    cache.configure_redis(max_memory=cache_size, eviction_policy=eviction_policy)

    # Generar tráfico
    traffic = TrafficGenerator(
        distribution=distribution,
        zipf_param=1.5,
        seed=seed,
    )

    progress_interval = max(1, total_queries // 10)

    for query in traffic.generate(total_queries, qps):
        cache.process_query(query["query_type"], **query["params"])

        if query["seq"] % progress_interval == 0:
            s = metrics.get_summary()
            print(f"  [{query['seq']:>6}/{total_queries}] "
                  f"hit={s['hit_rate']:.1%} p50={s['latency_p50_ms']:.2f}ms")

    # Resumen
    summary = metrics.get_summary()
    summary["per_query"] = metrics.get_per_query_summary()
    summary["experiment"] = {
        "distribution":    distribution,
        "eviction_policy": eviction_policy,
        "cache_size":      cache_size,
        "ttl":             ttl,
        "total_queries":   total_queries,
        "qps":             qps,
    }
    summary["redis_evicted_keys"] = cache.get_redis_evicted_keys()

    # Guardar resultados
    output_dir = os.path.join(output_base_dir, tag)
    os.makedirs(output_dir, exist_ok=True)

    metrics.export_events_csv(os.path.join(output_dir, "events.csv"))
    with open(os.path.join(output_dir, "summary.json"), "w") as f:
        json.dump(summary, f, indent=2)

    print(f"  → hit_rate={summary['hit_rate']:.2%}  "
          f"throughput={summary['throughput_qps']:.0f} q/s  "
          f"evictions={summary['redis_evicted_keys']}")

    return summary


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--quick", action="store_true",
                        help="Ejecutar subconjunto rápido de experimentos")
    parser.add_argument("--total", type=int, default=1000,
                        help="Consultas por experimento")
    parser.add_argument("--qps", type=float, default=0,
                        help="QPS (0=sin límite)")
    parser.add_argument("--output-dir", type=str,
                        default="metrics/experiments",
                        help="Directorio base de salida")
    parser.add_argument("--dataset", type=str, default=DATASET_PATH)
    parser.add_argument("--seed", type=int, default=42)
    args = parser.parse_args()

    # Cargar dataset una sola vez
    print("[experiments] Cargando dataset...")
    data = load_dataset(args.dataset, verbose=True)
    engine = QueryEngine(data, ZONE_AREAS_KM2)

    # Conectar Redis
    redis_client = redis.Redis(
        host=REDIS_HOST, port=REDIS_PORT, db=REDIS_DB, decode_responses=True
    )
    redis_client.ping()
    print("[experiments] Redis conectado.")

    # Seleccionar configuraciones
    if args.quick:
        distributions = QUICK_DISTRIBUTIONS
        policies = QUICK_POLICIES
        sizes = QUICK_SIZES
        ttls = QUICK_TTLS
    else:
        distributions = DISTRIBUTIONS
        policies = EVICTION_POLICIES
        sizes = CACHE_SIZES
        ttls = TTL_VALUES

    combos = list(itertools.product(distributions, policies, sizes, ttls))
    total_experiments = len(combos)

    print(f"\n[experiments] Total de experimentos a ejecutar: {total_experiments}")
    print(f"[experiments] Consultas por experimento: {args.total:,}")
    print(f"[experiments] Output → {args.output_dir}/\n")

    all_summaries = []
    t_start = time.time()

    for i, (dist, policy, size, ttl) in enumerate(combos, 1):
        print(f"\n{'█' * 60}")
        print(f"  Experimento {i}/{total_experiments}")
        print(f"{'█' * 60}")

        summary = run_single_experiment(
            engine=engine,
            redis_client=redis_client,
            distribution=dist,
            eviction_policy=policy,
            cache_size=size,
            ttl=ttl,
            total_queries=args.total,
            qps=args.qps,
            seed=args.seed,
            output_base_dir=args.output_dir,
        )
        all_summaries.append(summary)

    # Guardar resumen comparativo
    elapsed = time.time() - t_start
    comparison_path = os.path.join(args.output_dir, "comparison.json")
    with open(comparison_path, "w") as f:
        json.dump(all_summaries, f, indent=2)

    print(f"\n{'═' * 60}")
    print(f"  TODOS LOS EXPERIMENTOS COMPLETADOS")
    print(f"  Total: {total_experiments} experimentos en {elapsed:.0f}s")
    print(f"  Comparación: {comparison_path}")
    print(f"{'═' * 60}\n")

    # Tabla resumen
    print(f"{'Distribución':<10} {'Política':<18} {'Tamaño':<8} "
          f"{'TTL':>4} {'Hit%':>7} {'p50ms':>7} {'p95ms':>7} {'QPS':>6}")
    print("─" * 78)
    for s in all_summaries:
        exp = s["experiment"]
        print(f"{exp['distribution']:<10} {exp['eviction_policy']:<18} "
              f"{exp['cache_size']:<8} {exp['ttl']:>4} "
              f"{s['hit_rate']:>6.1%} {s['latency_p50_ms']:>7.2f} "
              f"{s['latency_p95_ms']:>7.2f} {s['throughput_qps']:>6.0f}")


if __name__ == "__main__":
    main()