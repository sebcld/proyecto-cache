"""
main.py — Orquestador principal del sistema de caché
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Flujo:
  1. Carga el dataset en memoria (loader.py)
  2. Inicializa el QueryEngine (queries.py)
  3. Conecta a Redis y configura la caché (cache_service.py)
  4. Inicializa el MetricsStore (metrics_store.py)
  5. Ejecuta el TrafficGenerator (generator.py)
  6. Exporta métricas al finalizar

Uso:
    python main.py                          # config por defecto
    python main.py --distribution zipf      # forzar distribución
    python main.py --total 5000 --qps 100   # 5000 queries a 100 q/s
"""

import os
import sys
import json
import argparse
import time

import redis

# ── Imports del proyecto ─────────────────────────────────────────────────────
from config import (
    DATASET_PATH, REDIS_HOST, REDIS_PORT, REDIS_DB,
    CACHE_MAX_MEMORY, CACHE_EVICTION_POLICY, CACHE_TTL_SECONDS,
    TRAFFIC_DISTRIBUTION, ZIPF_PARAM, TOTAL_QUERIES,
    QUERIES_PER_SECOND, METRICS_OUTPUT_DIR,
)
from data.loader import load_dataset, ZONE_AREAS_KM2, ZONES
from query_engine.queries import QueryEngine
from cache.cache_service import CacheService
from metrics.metrics_store import MetricsStore
from traffic_generator.generator import TrafficGenerator


def parse_args():
    parser = argparse.ArgumentParser(
        description="Sistema de caché — Sistemas Distribuidos 2026-1"
    )
    parser.add_argument("--distribution", type=str,
                        default=TRAFFIC_DISTRIBUTION,
                        choices=["zipf", "uniform"],
                        help="Distribución de tráfico")
    parser.add_argument("--zipf-param", type=float, default=ZIPF_PARAM,
                        help="Parámetro s de Zipf")
    parser.add_argument("--total", type=int, default=TOTAL_QUERIES,
                        help="Número total de consultas")
    parser.add_argument("--qps", type=float, default=QUERIES_PER_SECOND,
                        help="Consultas por segundo (0=sin límite)")
    parser.add_argument("--ttl", type=int, default=CACHE_TTL_SECONDS,
                        help="TTL de caché en segundos")
    parser.add_argument("--max-memory", type=str, default=CACHE_MAX_MEMORY,
                        help="Memoria máxima de Redis (ej. 50mb, 200mb)")
    parser.add_argument("--eviction-policy", type=str,
                        default=CACHE_EVICTION_POLICY,
                        choices=["allkeys-lru", "allkeys-lfu", "allkeys-random"],
                        help="Política de evicción de Redis")
    parser.add_argument("--dataset", type=str, default=DATASET_PATH,
                        help="Ruta al archivo CSV del dataset")
    parser.add_argument("--output-dir", type=str, default=METRICS_OUTPUT_DIR,
                        help="Directorio de salida para métricas")
    parser.add_argument("--seed", type=int, default=42,
                        help="Semilla para reproducibilidad")
    parser.add_argument("--flush", action="store_true",
                        help="Vaciar la caché antes de iniciar")
    return parser.parse_args()


def wait_for_redis(host: str, port: int, timeout: int = 30) -> redis.Redis:
    """Espera a que Redis esté disponible (para Docker Compose)."""
    print(f"[main] Conectando a Redis en {host}:{port}...")
    r = redis.Redis(host=host, port=port, db=REDIS_DB, decode_responses=True)

    start = time.time()
    while time.time() - start < timeout:
        try:
            r.ping()
            print(f"[main] Redis conectado correctamente.")
            return r
        except redis.ConnectionError:
            time.sleep(1)

    raise ConnectionError(
        f"No se pudo conectar a Redis en {host}:{port} "
        f"después de {timeout}s"
    )


def run_simulation(args) -> dict:
    """
    Ejecuta la simulación completa del sistema.

    Retorna el resumen de métricas.
    """
    print("\n" + "█" * 60)
    print("  SISTEMA DE CACHÉ — SISTEMAS DISTRIBUIDOS 2026-1")
    print("█" * 60)

    # ── 1. Cargar dataset ────────────────────────────────────────────────────
    print(f"\n[main] Paso 1/5: Cargando dataset...")
    data = load_dataset(args.dataset, verbose=True)

    # ── 2. Inicializar QueryEngine ───────────────────────────────────────────
    print(f"[main] Paso 2/5: Inicializando motor de consultas...")
    engine = QueryEngine(data, ZONE_AREAS_KM2)

    # ── 3. Conectar a Redis y configurar caché ───────────────────────────────
    print(f"[main] Paso 3/5: Configurando caché Redis...")
    redis_client = wait_for_redis(REDIS_HOST, REDIS_PORT)

    metrics = MetricsStore()
    cache = CacheService(
        redis_client=redis_client,
        query_engine=engine,
        metrics=metrics,
        ttl=args.ttl,
    )

    cache.configure_redis(
        max_memory=args.max_memory,
        eviction_policy=args.eviction_policy,
    )

    if args.flush:
        cache.flush_cache()

    print(f"[cache] Estado inicial: {json.dumps(cache.get_cache_info(), indent=2)}")

    # ── 4. Generar tráfico ───────────────────────────────────────────────────
    print(f"\n[main] Paso 4/5: Ejecutando simulación de tráfico...")
    print(f"  Distribución:   {args.distribution}")
    print(f"  Total queries:  {args.total:,}")
    print(f"  QPS objetivo:   {args.qps}")
    print(f"  TTL:            {args.ttl}s")
    print(f"  Max memory:     {args.max_memory}")
    print(f"  Eviction policy: {args.eviction_policy}")
    print()

    traffic_gen = TrafficGenerator(
        distribution=args.distribution,
        zipf_param=args.zipf_param,
        seed=args.seed,
    )

    # Barra de progreso simple
    progress_interval = max(1, args.total // 20)

    for query in traffic_gen.generate(args.total, args.qps):
        result = cache.process_query(query["query_type"], **query["params"])

        if query["seq"] % progress_interval == 0 or query["seq"] == args.total:
            pct = query["seq"] / args.total * 100
            summary = metrics.get_summary()
            print(
                f"  [{query['seq']:>6}/{args.total}] {pct:5.1f}%  "
                f"hit_rate={summary['hit_rate']:.2%}  "
                f"p50={summary['latency_p50_ms']:.2f}ms  "
                f"throughput={summary['throughput_qps']:.0f} q/s"
            )

    # ── 5. Exportar métricas ─────────────────────────────────────────────────
    print(f"\n[main] Paso 5/5: Exportando métricas...")

    # Agregar evictions de Redis al store
    redis_evictions = cache.get_redis_evicted_keys()
    print(f"[cache] Evictions reportadas por Redis: {redis_evictions}")

    # Resumen final
    metrics.print_summary()

    # Crear directorio de salida con tag de configuración
    tag = f"{args.distribution}_ttl{args.ttl}_{args.max_memory}_{args.eviction_policy}"
    output_dir = os.path.join(args.output_dir, tag)
    os.makedirs(output_dir, exist_ok=True)

    metrics.export_events_csv(os.path.join(output_dir, "events.csv"))
    metrics.export_summary_json(os.path.join(output_dir, "summary.json"))

    # Guardar info de la distribución del tráfico
    dist_info = traffic_gen.get_distribution_info()
    with open(os.path.join(output_dir, "traffic_distribution.json"), "w") as f:
        json.dump(dist_info, f, indent=2)

    # Guardar info del cache al finalizar
    cache_info = cache.get_cache_info()
    cache_info["redis_evicted_keys"] = redis_evictions
    with open(os.path.join(output_dir, "cache_info.json"), "w") as f:
        json.dump(cache_info, f, indent=2)

    print(f"\n[main] Resultados guardados en: {output_dir}/")
    print(f"[main] Simulación completada.\n")

    return metrics.get_summary()


# ──────────────────────────────────────────────────────────────────────────────
# Punto de entrada
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    args = parse_args()
    summary = run_simulation(args)