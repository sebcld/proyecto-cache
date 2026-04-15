"""
queries.py — Motor de consultas geoespaciales en memoria (Q1–Q5)
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Todas las consultas operan directamente sobre el dict de DataFrames
devuelto por load_dataset(). No hay acceso a base de datos en tiempo
de ejecución.

Uso típico:
    from data.loader import load_dataset, ZONE_AREAS_KM2
    from query_engine.queries import QueryEngine

    data = load_dataset("data/967_buildings.csv")
    engine = QueryEngine(data, ZONE_AREAS_KM2)

    result = engine.run("Q1", zone_id="Z1", confidence_min=0.7)
    print(result)
"""

import time
import numpy as np
import pandas as pd
from typing import Any


# ──────────────────────────────────────────────────────────────────────────────
# Helpers internos
# ──────────────────────────────────────────────────────────────────────────────

def _filter_confidence(df: pd.DataFrame, confidence_min: float) -> pd.DataFrame:
    """Retorna sólo las filas con confidence >= confidence_min."""
    if confidence_min <= 0.0:
        return df                          # evita filtrado innecesario
    return df[df["confidence"] >= confidence_min]


def _validate_zone(zone_id: str, data: dict) -> None:
    if zone_id not in data:
        raise KeyError(
            f"Zona '{zone_id}' no existe. Zonas válidas: {list(data.keys())}"
        )


def _wrap(query_type: str, cache_key: str, result: Any,
          elapsed_ms: float) -> dict:
    """Envuelve el resultado con metadatos uniformes para el sistema de caché."""
    return {
        "query":     query_type,
        "cache_key": cache_key,
        "result":    result,
        "elapsed_ms": round(elapsed_ms, 4),
    }


# Número de registros de ejemplo incluidos en la respuesta para inflar el
# payload y que los límites de maxmemory (50/200/500 MB) sean relevantes.
# Con ~600 registros por entrada y payload ~60 KB → catálogo (~1030) supera 50 MB.
SAMPLE_RECORDS_PER_RESPONSE = 1000
SAMPLE_RECORDS_PER_ZONE_Q4  = 300
SAMPLE_RECORDS_PER_BUCKET   = 100


def _sample_records(df: pd.DataFrame, n: int) -> list[dict]:
    """Toma los primeros n registros del DataFrame como 'muestra' determinista.

    Se serializan lat/lon/area/confidence. Es determinista para que la misma
    consulta siempre produzca el mismo payload (y pueda haber cache hits).
    """
    if df.empty or n <= 0:
        return []
    sub = df.head(n)
    return [
        {
            "lat":  round(float(r.latitude), 6),
            "lon":  round(float(r.longitude), 6),
            "area": round(float(r.area_in_meters), 3),
            "conf": round(float(r.confidence), 4),
        }
        for r in sub.itertuples(index=False)
    ]


# ──────────────────────────────────────────────────────────────────────────────
# Motor de consultas
# ──────────────────────────────────────────────────────────────────────────────

class QueryEngine:
    """
    Ejecuta las consultas Q1–Q5 sobre los datos precargados en memoria.

    Parámetros
    ----------
    data : dict[str, pd.DataFrame]
        Resultado de load_dataset(): { zona_id → DataFrame }.
    zone_areas_km2 : dict[str, float]
        Áreas precalculadas de cada bounding box (de ZONE_AREAS_KM2).
    """

    def __init__(self, data: dict[str, pd.DataFrame],
                 zone_areas_km2: dict[str, float]):
        self._data    = data
        self._areas   = zone_areas_km2

        # Mapa de tipo → método, para llamadas dinámicas desde el caché
        self._dispatch = {
            "Q1": self.q1_count,
            "Q2": self.q2_area,
            "Q3": self.q3_density,
            "Q4": self.q4_compare,
            "Q5": self.q5_confidence_dist,
        }

    # ── Interfaz genérica ────────────────────────────────────────────────────
    def run(self, query_type: str, **kwargs) -> dict:
        """
        Ejecuta la consulta indicada con los kwargs correspondientes.
        Retorna el dict completo con cache_key, result y elapsed_ms.

        Ejemplo:
            engine.run("Q1", zone_id="Z1", confidence_min=0.8)
            engine.run("Q4", zone_a="Z1", zone_b="Z3", confidence_min=0.5)
        """
        if query_type not in self._dispatch:
            raise ValueError(
                f"Tipo de consulta '{query_type}' no válido. "
                f"Opciones: {list(self._dispatch.keys())}"
            )
        return self._dispatch[query_type](**kwargs)

    # ──────────────────────────────────────────────────────────────────────────
    # Q1 — Conteo de edificios en una zona
    # Cache key: count:{zona_id}:conf={confidence_min}
    # ──────────────────────────────────────────────────────────────────────────
    def q1_count(self, zone_id: str, confidence_min: float = 0.0) -> dict:
        """
        Cuenta el número total de edificaciones dentro de la zona
        que superen el umbral de confianza mínimo.

        Parámetros
        ----------
        zone_id        : ID de zona (Z1–Z5)
        confidence_min : umbral mínimo de confianza [0.0, 1.0]

        Retorna
        -------
        { "count": int }
        """
        _validate_zone(zone_id, self._data)
        t0 = time.perf_counter()

        df      = _filter_confidence(self._data[zone_id], confidence_min)
        count   = len(df)
        sample  = _sample_records(df, SAMPLE_RECORDS_PER_RESPONSE)

        elapsed = (time.perf_counter() - t0) * 1000
        cache_key = f"count:{zone_id}:conf={confidence_min}"

        return _wrap("Q1", cache_key,
                     {"count": count, "sample": sample}, elapsed)

    # ──────────────────────────────────────────────────────────────────────────
    # Q2 — Área promedio y total de edificaciones
    # Cache key: area:{zona_id}:conf={confidence_min}
    # ──────────────────────────────────────────────────────────────────────────
    def q2_area(self, zone_id: str, confidence_min: float = 0.0) -> dict:
        """
        Calcula el área promedio (m²), área total (m²) y conteo
        de edificios en la zona que superen el umbral de confianza.

        Parámetros
        ----------
        zone_id        : ID de zona (Z1–Z5)
        confidence_min : umbral mínimo de confianza [0.0, 1.0]

        Retorna
        -------
        { "avg_area": float, "total_area": float, "n": int }
        """
        _validate_zone(zone_id, self._data)
        t0 = time.perf_counter()

        df    = _filter_confidence(self._data[zone_id], confidence_min)
        areas = df["area_in_meters"]
        n     = len(areas)

        if n == 0:
            result = {"avg_area": 0.0, "total_area": 0.0, "n": 0, "sample": []}
        else:
            result = {
                "avg_area":   float(areas.mean()),
                "total_area": float(areas.sum()),
                "n":          n,
                "sample":     _sample_records(df, SAMPLE_RECORDS_PER_RESPONSE),
            }

        elapsed   = (time.perf_counter() - t0) * 1000
        cache_key = f"area:{zone_id}:conf={confidence_min}"

        return _wrap("Q2", cache_key, result, elapsed)

    # ──────────────────────────────────────────────────────────────────────────
    # Q3 — Densidad de edificaciones por km²
    # Cache key: density:{zona_id}:conf={confidence_min}
    # ──────────────────────────────────────────────────────────────────────────
    def q3_density(self, zone_id: str, confidence_min: float = 0.0) -> dict:
        """
        Calcula la densidad de edificaciones por km² en la zona,
        normalizando por el área del bounding box.

        Parámetros
        ----------
        zone_id        : ID de zona (Z1–Z5)
        confidence_min : umbral mínimo de confianza [0.0, 1.0]

        Retorna
        -------
        { "density_per_km2": float, "count": int, "area_km2": float }
        """
        _validate_zone(zone_id, self._data)
        t0 = time.perf_counter()

        # Reutiliza Q1 internamente (sin overhead de _wrap)
        df       = _filter_confidence(self._data[zone_id], confidence_min)
        count    = len(df)
        area_km2 = self._areas[zone_id]
        density  = count / area_km2 if area_km2 > 0 else 0.0

        result = {
            "density_per_km2": round(density, 4),
            "count":           count,
            "area_km2":        round(area_km2, 4),
            "sample":          _sample_records(df, SAMPLE_RECORDS_PER_RESPONSE),
        }

        elapsed   = (time.perf_counter() - t0) * 1000
        cache_key = f"density:{zone_id}:conf={confidence_min}"

        return _wrap("Q3", cache_key, result, elapsed)

    # ──────────────────────────────────────────────────────────────────────────
    # Q4 — Comparación de densidad entre dos zonas
    # Cache key: compare:density:{zona_a}:{zona_b}:conf={confidence_min}
    # ──────────────────────────────────────────────────────────────────────────
    def q4_compare(self, zone_a: str, zone_b: str,
                   confidence_min: float = 0.0) -> dict:
        """
        Compara la densidad de edificaciones entre dos zonas y
        determina cuál tiene mayor densidad.

        Parámetros
        ----------
        zone_a         : ID de la primera zona  (Z1–Z5)
        zone_b         : ID de la segunda zona  (Z1–Z5)
        confidence_min : umbral mínimo de confianza [0.0, 1.0]

        Retorna
        -------
        {
          "zone_a": { "id", "density_per_km2", "count", "area_km2" },
          "zone_b": { "id", "density_per_km2", "count", "area_km2" },
          "winner": zona_id con mayor densidad  (None si son iguales)
        }
        """
        _validate_zone(zone_a, self._data)
        _validate_zone(zone_b, self._data)
        t0 = time.perf_counter()

        # Calcula densidad para ambas zonas reutilizando la lógica de Q3
        def _density_raw(zone_id: str) -> dict:
            df       = _filter_confidence(self._data[zone_id], confidence_min)
            count    = len(df)
            area_km2 = self._areas[zone_id]
            density  = count / area_km2 if area_km2 > 0 else 0.0
            return {
                "id":             zone_id,
                "density_per_km2": round(density, 4),
                "count":           count,
                "area_km2":        round(area_km2, 4),
                "sample":          _sample_records(df, SAMPLE_RECORDS_PER_ZONE_Q4),
            }

        da = _density_raw(zone_a)
        db = _density_raw(zone_b)

        if da["density_per_km2"] > db["density_per_km2"]:
            winner = zone_a
        elif db["density_per_km2"] > da["density_per_km2"]:
            winner = zone_b
        else:
            winner = None   # empate

        result = {"zone_a": da, "zone_b": db, "winner": winner}

        elapsed   = (time.perf_counter() - t0) * 1000
        cache_key = f"compare:density:{zone_a}:{zone_b}:conf={confidence_min}"

        return _wrap("Q4", cache_key, result, elapsed)

    # ──────────────────────────────────────────────────────────────────────────
    # Q5 — Distribución de confianza en una zona
    # Cache key: confidence_dist:{zona_id}:bins={bins}
    # ──────────────────────────────────────────────────────────────────────────
    def q5_confidence_dist(self, zone_id: str, bins: int = 5) -> dict:
        """
        Calcula la distribución del score de confianza en la zona,
        agrupado en `bins` intervalos de igual ancho entre 0 y 1.

        Parámetros
        ----------
        zone_id : ID de zona (Z1–Z5)
        bins    : número de intervalos (por defecto 5)

        Retorna
        -------
        {
          "bins": [
            { "bucket": i, "min": float, "max": float, "count": int },
            ...
          ],
          "total": int
        }
        """
        _validate_zone(zone_id, self._data)
        if bins < 1:
            raise ValueError(f"'bins' debe ser >= 1, se recibió {bins}")

        t0 = time.perf_counter()

        df_zone = self._data[zone_id]
        scores  = df_zone["confidence"].to_numpy()
        counts, edges = np.histogram(scores, bins=bins, range=(0.0, 1.0))

        distribution = []
        for i in range(bins):
            lo, hi = float(edges[i]), float(edges[i + 1])
            bucket_df = df_zone[
                (df_zone["confidence"] >= lo) & (df_zone["confidence"] < hi)
            ]
            distribution.append({
                "bucket": i,
                "min":    round(lo, 4),
                "max":    round(hi, 4),
                "count":  int(counts[i]),
                "sample": _sample_records(bucket_df, SAMPLE_RECORDS_PER_BUCKET),
            })

        result = {
            "bins":  distribution,
            "total": int(scores.size),
        }

        elapsed   = (time.perf_counter() - t0) * 1000
        cache_key = f"confidence_dist:{zone_id}:bins={bins}"

        return _wrap("Q5", cache_key, result, elapsed)


# ──────────────────────────────────────────────────────────────────────────────
# Punto de entrada — prueba rápida de todas las queries
# $ python query_engine/queries.py
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys
    import json

    # Ajusta el path para importar loader desde cualquier directorio
    sys.path.insert(0, ".")
    from data.loader import load_dataset, ZONE_AREAS_KM2

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/967_buildings.csv"
    data     = load_dataset(csv_path, verbose=True)
    engine   = QueryEngine(data, ZONE_AREAS_KM2)

    separator = "─" * 60

    print(separator)
    print("Q1 — Conteo de edificios en Z1 (conf >= 0.7)")
    r = engine.run("Q1", zone_id="Z1", confidence_min=0.7)
    print(json.dumps(r, indent=2))

    print(separator)
    print("Q2 — Área promedio y total en Z2 (conf >= 0.0)")
    r = engine.run("Q2", zone_id="Z2", confidence_min=0.0)
    print(json.dumps(r, indent=2))

    print(separator)
    print("Q3 — Densidad en Z4 (conf >= 0.5)")
    r = engine.run("Q3", zone_id="Z4", confidence_min=0.5)
    print(json.dumps(r, indent=2))

    print(separator)
    print("Q4 — Comparación Z1 vs Z3 (conf >= 0.0)")
    r = engine.run("Q4", zone_a="Z1", zone_b="Z3", confidence_min=0.0)
    print(json.dumps(r, indent=2))

    print(separator)
    print("Q5 — Distribución de confianza en Z5 (bins=5)")
    r = engine.run("Q5", zone_id="Z5", bins=5)
    print(json.dumps(r, indent=2))

    print(separator)
    print("Todas las consultas ejecutadas correctamente.")