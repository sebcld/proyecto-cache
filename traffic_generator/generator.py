"""
generator.py — Generador de Tráfico sintético
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Simula solicitudes de empresas de reparto y logística que consultan
zonas de Santiago. Genera consultas automáticamente con dos distribuciones:
  - Zipf (ley de potencia): algunas zonas/queries mucho más frecuentes
  - Uniforme: todas las combinaciones equiprobables

Las consultas se construyen a partir de las zonas predefinidas (Z1-Z5)
y los tipos de operación (Q1-Q5), sin interacción con BD externa.
"""

import time
import random
import itertools
from typing import Generator

import numpy as np

from data.loader import ZONES


# ──────────────────────────────────────────────────────────────────────────────
# Catálogo de consultas posibles
# ──────────────────────────────────────────────────────────────────────────────

ZONE_IDS = list(ZONES.keys())  # ["Z1", "Z2", "Z3", "Z4", "Z5"]
QUERY_TYPES = ["Q1", "Q2", "Q3", "Q4", "Q5"]
CONFIDENCE_VALUES = [0.0, 0.3, 0.5, 0.7, 0.9]
BINS_VALUES = [3, 5, 10]


def _build_query_catalog() -> list[dict]:
    """
    Construye el catálogo completo de consultas posibles.

    Cada entrada es un dict con:
      - query_type: str (Q1-Q5)
      - params: dict con los parámetros de la consulta

    Esto permite asignar un índice a cada consulta para la distribución Zipf.
    """
    catalog = []

    for zone_id in ZONE_IDS:
        for conf in CONFIDENCE_VALUES:
            # Q1: conteo por zona
            catalog.append({
                "query_type": "Q1",
                "params": {"zone_id": zone_id, "confidence_min": conf}
            })
            # Q2: área por zona
            catalog.append({
                "query_type": "Q2",
                "params": {"zone_id": zone_id, "confidence_min": conf}
            })
            # Q3: densidad por zona
            catalog.append({
                "query_type": "Q3",
                "params": {"zone_id": zone_id, "confidence_min": conf}
            })

    # Q4: comparaciones entre pares de zonas
    zone_pairs = list(itertools.combinations(ZONE_IDS, 2))
    for zone_a, zone_b in zone_pairs:
        for conf in CONFIDENCE_VALUES:
            catalog.append({
                "query_type": "Q4",
                "params": {"zone_a": zone_a, "zone_b": zone_b,
                           "confidence_min": conf}
            })

    # Q5: distribución de confianza
    for zone_id in ZONE_IDS:
        for bins in BINS_VALUES:
            catalog.append({
                "query_type": "Q5",
                "params": {"zone_id": zone_id, "bins": bins}
            })

    return catalog


# Catálogo global de todas las consultas posibles
QUERY_CATALOG = _build_query_catalog()


# ──────────────────────────────────────────────────────────────────────────────
# Distribuciones de tráfico
# ──────────────────────────────────────────────────────────────────────────────

def _zipf_weights(n: int, s: float = 1.5) -> np.ndarray:
    """
    Genera pesos según la distribución Zipf (ley de potencia).

    El elemento de rango k tiene peso proporcional a 1/k^s.
    Un 's' mayor concentra más tráfico en las primeras consultas.

    Parámetros
    ----------
    n : int   — número total de elementos
    s : float — exponente de Zipf (> 1.0 para distribución sesgada)
    """
    ranks = np.arange(1, n + 1, dtype=np.float64)
    weights = 1.0 / np.power(ranks, s)
    return weights / weights.sum()  # normalizar a probabilidades


def _uniform_weights(n: int) -> np.ndarray:
    """Genera pesos uniformes (todas las consultas equiprobables)."""
    return np.ones(n) / n


# ──────────────────────────────────────────────────────────────────────────────
# Generador de tráfico
# ──────────────────────────────────────────────────────────────────────────────

class TrafficGenerator:
    """
    Genera un flujo de consultas sintéticas para el sistema de caché.

    Parámetros
    ----------
    distribution : str
        "zipf" o "uniform"
    zipf_param : float
        Exponente de la distribución Zipf (sólo si distribution="zipf")
    seed : int or None
        Semilla para reproducibilidad
    """

    def __init__(self, distribution: str = "zipf",
                 zipf_param: float = 1.5,
                 seed: int = 42):
        self._distribution = distribution
        self._zipf_param = zipf_param
        self._catalog = QUERY_CATALOG
        self._n = len(self._catalog)
        self._rng = np.random.default_rng(seed)

        # Precalcular pesos
        if distribution == "zipf":
            # Mezclar el catálogo con seed fija para que Zipf
            # favorezca un subconjunto determinista de consultas
            shuffled_indices = self._rng.permutation(self._n)
            self._catalog = [self._catalog[i] for i in shuffled_indices]
            self._weights = _zipf_weights(self._n, zipf_param)
        else:
            self._weights = _uniform_weights(self._n)

        print(f"[traffic] Generador inicializado: distribution={distribution}, "
              f"catálogo={self._n} consultas posibles")

    def generate(self, total_queries: int,
                 queries_per_second: float = 0) -> Generator[dict, None, None]:
        """
        Genera un flujo de consultas.

        Parámetros
        ----------
        total_queries     : Número total de consultas a generar
        queries_per_second: Tasa objetivo (0 = sin throttling)

        Yields
        ------
        dict con "query_type", "params" y "seq" (número de secuencia)
        """
        interval = 1.0 / queries_per_second if queries_per_second > 0 else 0

        # Seleccionar índices según la distribución
        indices = self._rng.choice(
            self._n,
            size=total_queries,
            p=self._weights,
        )

        for seq, idx in enumerate(indices, start=1):
            t0 = time.perf_counter()

            query = self._catalog[idx].copy()
            query["seq"] = seq

            yield query

            # Throttling
            if interval > 0:
                elapsed = time.perf_counter() - t0
                sleep_time = interval - elapsed
                if sleep_time > 0:
                    time.sleep(sleep_time)

    def get_distribution_info(self) -> dict:
        """Retorna información sobre la distribución configurada."""
        top_k = 10
        top_indices = np.argsort(self._weights)[::-1][:top_k]
        top_queries = []
        for i, idx in enumerate(top_indices):
            q = self._catalog[idx]
            top_queries.append({
                "rank": i + 1,
                "probability": round(float(self._weights[idx]), 6),
                "query_type": q["query_type"],
                "params": q["params"],
            })

        return {
            "distribution": self._distribution,
            "zipf_param": self._zipf_param if self._distribution == "zipf" else None,
            "catalog_size": self._n,
            "top_queries": top_queries,
        }


# ──────────────────────────────────────────────────────────────────────────────
# Punto de entrada — prueba rápida del generador
# $ python traffic_generator/generator.py
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import json

    print("=" * 60)
    print(" Prueba del generador de tráfico (Zipf)")
    print("=" * 60)

    gen = TrafficGenerator(distribution="zipf", zipf_param=1.5, seed=42)

    info = gen.get_distribution_info()
    print(f"\nTop 10 consultas más probables (Zipf s={info['zipf_param']}):")
    for q in info["top_queries"]:
        print(f"  #{q['rank']:>2} p={q['probability']:.4f}  "
              f"{q['query_type']} {q['params']}")

    print(f"\nGenerando 20 consultas de muestra:")
    for query in gen.generate(total_queries=20):
        print(f"  [{query['seq']:>3}] {query['query_type']} → {query['params']}")

    print("\n" + "=" * 60)
    print(" Prueba del generador de tráfico (Uniforme)")
    print("=" * 60)

    gen_u = TrafficGenerator(distribution="uniform", seed=42)
    print(f"\nGenerando 20 consultas de muestra:")
    for query in gen_u.generate(total_queries=20):
        print(f"  [{query['seq']:>3}] {query['query_type']} → {query['params']}")