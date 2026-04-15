"""
loader.py — Carga y partición del dataset Google Open Buildings
Región Metropolitana de Santiago

Al iniciar el sistema, este módulo:
  1. Lee el CSV desde disco
  2. Retiene sólo las columnas relevantes
  3. Filtra y particiona los registros en las 5 zonas predefinidas
  4. Precalcula el área (km²) de cada bounding box
  5. Retorna todo en memoria para que Q1–Q5 operen sin base de datos

Uso:
    from data.loader import load_dataset, ZONES, ZONE_AREAS_KM2
    data = load_dataset("data/967_buildings.csv")
"""

import os
import math
import pandas as pd

# ──────────────────────────────────────────────────────────────────────────────
# 1. Zonas predefinidas (bounding boxes)
#    Nota: en el enunciado las comas son decimales (notación española),
#    aquí se usan puntos para Python.
# ──────────────────────────────────────────────────────────────────────────────
ZONES: dict[str, dict] = {
    "Z1": {"name": "Providencia",     "lat_min": -33.445, "lat_max": -33.420, "lon_min": -70.640, "lon_max": -70.600},
    "Z2": {"name": "Las Condes",      "lat_min": -33.420, "lat_max": -33.390, "lon_min": -70.600, "lon_max": -70.550},
    "Z3": {"name": "Maipú",           "lat_min": -33.530, "lat_max": -33.490, "lon_min": -70.790, "lon_max": -70.740},
    "Z4": {"name": "Santiago Centro", "lat_min": -33.460, "lat_max": -33.430, "lon_min": -70.670, "lon_max": -70.630},
    "Z5": {"name": "Pudahuel",        "lat_min": -33.470, "lat_max": -33.430, "lon_min": -70.810, "lon_max": -70.760},
}

# Columnas requeridas del dataset
REQUIRED_COLS = ["latitude", "longitude", "area_in_meters", "confidence"]


# ──────────────────────────────────────────────────────────────────────────────
# 2. Cálculo del área de cada bounding box en km²
#    Usa proyección equirectangular centrada en la latitud media.
#    Válida para bounding boxes pequeñas (< 1° de lado).
# ──────────────────────────────────────────────────────────────────────────────
def _bbox_area_km2(lat_min: float, lat_max: float,
                   lon_min: float, lon_max: float) -> float:
    """
    Calcula el área aproximada de una bounding box en km².
    """
    R = 6371.0  # Radio medio de la Tierra en km
    lat_center_rad = math.radians((lat_min + lat_max) / 2)

    delta_lat_km = math.radians(abs(lat_max - lat_min)) * R
    delta_lon_km = math.radians(abs(lon_max - lon_min)) * R * math.cos(lat_center_rad)

    return delta_lat_km * delta_lon_km


def _compute_zone_areas() -> dict[str, float]:
    """Precalcula el área en km² de las 5 zonas al importar el módulo."""
    return {
        zone_id: _bbox_area_km2(
            z["lat_min"], z["lat_max"],
            z["lon_min"], z["lon_max"]
        )
        for zone_id, z in ZONES.items()
    }


# Disponible globalmente para Q3 y Q4: { "Z1": 2.34, "Z2": ..., ... }
ZONE_AREAS_KM2: dict[str, float] = _compute_zone_areas()


# ──────────────────────────────────────────────────────────────────────────────
# 3. Carga y partición del dataset
# ──────────────────────────────────────────────────────────────────────────────
def _validate_columns(df: pd.DataFrame, filepath: str) -> None:
    """Verifica que el CSV tenga las columnas mínimas necesarias."""
    missing = [c for c in REQUIRED_COLS if c not in df.columns]
    if missing:
        raise ValueError(
            f"El archivo '{filepath}' no contiene las columnas requeridas: {missing}\n"
            f"Columnas encontradas: {list(df.columns)}"
        )


def _filter_zone(df: pd.DataFrame, zone: dict) -> pd.DataFrame:
    """Filtra el DataFrame para quedarse sólo con los registros dentro de la bbox."""
    mask = (
        (df["latitude"]  >= zone["lat_min"]) & (df["latitude"]  <= zone["lat_max"]) &
        (df["longitude"] >= zone["lon_min"]) & (df["longitude"] <= zone["lon_max"])
    )
    return df[mask].reset_index(drop=True)


def load_dataset(filepath: str, verbose: bool = True) -> dict[str, pd.DataFrame]:
    """
    Carga el CSV del dataset Google Open Buildings y lo particiona
    en las 5 zonas de Santiago predefinidas.

    Parámetros
    ----------
    filepath : str
        Ruta al archivo CSV (p.ej. "data/967_buildings.csv").
        Acepta también archivos comprimidos (.csv.gz).
    verbose : bool
        Si True, imprime un resumen al finalizar la carga.

    Retorna
    -------
    dict[str, pd.DataFrame]
        Diccionario { zona_id → DataFrame } con los registros de cada zona.
        Cada DataFrame contiene sólo las columnas: latitude, longitude,
        area_in_meters, confidence.

    Excepciones
    -----------
    FileNotFoundError  — si el archivo no existe.
    ValueError         — si faltan columnas requeridas en el CSV.
    """
    # ── 3.1 Verificar que el archivo existe ──────────────────────────────────
    if not os.path.exists(filepath):
        raise FileNotFoundError(
            f"Dataset no encontrado: '{filepath}'\n"
            f"Directorio de trabajo actual: {os.getcwd()}"
        )

    # ── 3.2 Leer CSV (sólo las columnas necesarias) ──────────────────────────
    if verbose:
        print(f"[loader] Leyendo dataset: {filepath}")

    # Intentamos leer sólo las columnas requeridas para ahorrar memoria.
    # Si el CSV tiene columnas extra (p.ej. 'full_plus_code'), se ignoran.
    try:
        df_raw = pd.read_csv(
            filepath,
            usecols=REQUIRED_COLS,
            dtype={
                "latitude":       "float32",
                "longitude":      "float32",
                "area_in_meters": "float32",
                "confidence":     "float32",
            },
        )
    except ValueError:
        # Si usecols falla (nombres distintos en el CSV), leer todo y validar
        df_raw = pd.read_csv(filepath)
        _validate_columns(df_raw, filepath)
        df_raw = df_raw[REQUIRED_COLS].astype("float32")

    total_records = len(df_raw)
    if verbose:
        print(f"[loader] Total de registros en el CSV: {total_records:,}")

    # ── 3.3 Particionar por zona ─────────────────────────────────────────────
    data: dict[str, pd.DataFrame] = {}

    for zone_id, zone in ZONES.items():
        zone_df = _filter_zone(df_raw, zone)
        data[zone_id] = zone_df

    # ── 3.4 Resumen de carga ─────────────────────────────────────────────────
    if verbose:
        print("\n[loader] ── Resumen de carga ──────────────────────────────────")
        print(f"{'Zona':<6} {'Nombre':<20} {'Edificios':>10} {'Área (km²)':>12}")
        print("─" * 52)

        total_loaded = 0
        for zone_id, zone_df in data.items():
            n = len(zone_df)
            total_loaded += n
            print(
                f"{zone_id:<6} "
                f"{ZONES[zone_id]['name']:<20} "
                f"{n:>10,} "
                f"{ZONE_AREAS_KM2[zone_id]:>12.4f}"
            )

        print("─" * 52)
        print(f"{'TOTAL':<27} {total_loaded:>10,}")
        coverage = (total_loaded / total_records * 100) if total_records > 0 else 0
        print(f"\n[loader] Cobertura sobre el CSV total: {coverage:.1f}%")
        print("[loader] Dataset listo en memoria.\n")

    return data


# ──────────────────────────────────────────────────────────────────────────────
# 4. Punto de entrada directo — para verificar la carga rápidamente
#    $ python data/loader.py
#    $ python data/loader.py data/967_buildings.csv
# ──────────────────────────────────────────────────────────────────────────────
if __name__ == "__main__":
    import sys

    csv_path = sys.argv[1] if len(sys.argv) > 1 else "data/967_buildings.csv"

    data = load_dataset(csv_path, verbose=True)

    # Muestra las primeras 3 filas de cada zona como verificación rápida
    print("\n[loader] Muestra de datos por zona:")
    for zone_id, df in data.items():
        print(f"\n  {zone_id} — {ZONES[zone_id]['name']} (primeras 3 filas):")
        if df.empty:
            print("    (sin registros en esta zona)")
        else:
            print(df.head(3).to_string(index=False))