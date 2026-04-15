"""
config.py — Configuración central del sistema de caché
Proyecto: Plataforma de análisis — Sistemas Distribuidos 2026-1

Centraliza todos los parámetros configurables del sistema.
Los valores se pueden sobreescribir con variables de entorno.
"""

import os

# ──────────────────────────────────────────────────────────────────────────────
# Dataset
# ──────────────────────────────────────────────────────────────────────────────
DATASET_PATH = os.getenv("DATASET_PATH", "data/967_buildings.csv")

# ──────────────────────────────────────────────────────────────────────────────
# Redis / Caché
# ──────────────────────────────────────────────────────────────────────────────
REDIS_HOST = os.getenv("REDIS_HOST", "redis")
REDIS_PORT = int(os.getenv("REDIS_PORT", 6379))
REDIS_DB   = int(os.getenv("REDIS_DB", 0))

# Tamaño máximo de la caché en bytes (por defecto 200 MB)
# Opciones para experimentación: 50MB, 200MB, 500MB
CACHE_MAX_MEMORY = os.getenv("CACHE_MAX_MEMORY", "200mb")

# Política de evicción: allkeys-lru | allkeys-lfu | allkeys-random (FIFO-like)
CACHE_EVICTION_POLICY = os.getenv("CACHE_EVICTION_POLICY", "allkeys-lru")

# TTL por defecto en segundos para las entradas de caché
CACHE_TTL_SECONDS = int(os.getenv("CACHE_TTL_SECONDS", 60))

# ──────────────────────────────────────────────────────────────────────────────
# Generador de Tráfico
# ──────────────────────────────────────────────────────────────────────────────
# Distribución de tráfico: "zipf" o "uniform"
TRAFFIC_DISTRIBUTION = os.getenv("TRAFFIC_DISTRIBUTION", "zipf")

# Parámetro 's' de la distribución Zipf (> 1.0 = más concentrado)
ZIPF_PARAM = float(os.getenv("ZIPF_PARAM", 1.5))

# Número total de consultas a generar en la simulación
TOTAL_QUERIES = int(os.getenv("TOTAL_QUERIES", 1000))

# Tasa de consultas por segundo (0 = sin throttling)
QUERIES_PER_SECOND = float(os.getenv("QUERIES_PER_SECOND", 50))

# Valores posibles de confidence_min para las consultas
CONFIDENCE_VALUES = [0.0, 0.3, 0.5, 0.7, 0.9]

# Valores posibles de bins para Q5
BINS_VALUES = [3, 5, 10]

# ──────────────────────────────────────────────────────────────────────────────
# Métricas
# ──────────────────────────────────────────────────────────────────────────────
METRICS_OUTPUT_DIR = os.getenv("METRICS_OUTPUT_DIR", "metrics/results")

# ──────────────────────────────────────────────────────────────────────────────
# gRPC (comunicación entre servicios)
# ──────────────────────────────────────────────────────────────────────────────
CACHE_SERVICE_HOST = os.getenv("CACHE_SERVICE_HOST", "cache-service")
CACHE_SERVICE_PORT = int(os.getenv("CACHE_SERVICE_PORT", 50051))

QUERY_ENGINE_HOST = os.getenv("QUERY_ENGINE_HOST", "query-engine")
QUERY_ENGINE_PORT = int(os.getenv("QUERY_ENGINE_PORT", 50052))
