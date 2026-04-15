# Plataforma de Análisis de Caché — Sistemas Distribuidos 2026-1

Sistema distribuido de análisis geoespacial con caché Redis para el dataset Google Open Buildings (Región Metropolitana de Santiago).

## Arquitectura

```
┌────────────────┐     ┌──────────────┐     ┌────────────────────┐
│  Generador de  │────▶│  Sistema de  │────▶│  Generador de      │
│  Tráfico       │     │  Caché       │     │  Respuestas        │
│  (Zipf/Uniforme)│     │  (Redis)     │◀────│  (QueryEngine)     │
└────────────────┘     └──────┬───────┘     └────────────────────┘
                              │
                     ┌────────▼───────┐
                     │ Almacenamiento │
                     │ de Métricas    │
                     └────────────────┘
```

## Requisitos

- Docker y Docker Compose
- Dataset `967_buildings.csv` en `data/`

## Ejecución Rápida

```bash
# Ejecutar simulación por defecto (Zipf, LRU, 200MB, TTL=60s)
docker compose up --build app

# Ejecutar todos los experimentos comparativos
docker compose --profile experiments up --build experiments

# Solo Redis (para desarrollo local)
docker compose up redis
```

## Ejecución Local (sin Docker)

```bash
pip install -r requirements.txt

# Simulación por defecto
python main.py

# Con parámetros personalizados
python main.py --distribution uniform --total 5000 --qps 100 --ttl 30

# Ejecutar todos los experimentos
python run_experiments.py --total 1000

# Subconjunto rápido de experimentos
python run_experiments.py --quick --total 500
```

## Parámetros Configurables

| Parámetro | Opciones | Default |
|---|---|---|
| `--distribution` | `zipf`, `uniform` | `zipf` |
| `--eviction-policy` | `allkeys-lru`, `allkeys-lfu`, `allkeys-random` | `allkeys-lru` |
| `--max-memory` | `50mb`, `200mb`, `500mb` | `200mb` |
| `--ttl` | Segundos | `60` |
| `--total` | Número de consultas | `1000` |
| `--qps` | Consultas/segundo (0=sin límite) | `50` |

## Estructura del Proyecto

```
.
├── cache/
│   └── cache_service.py       # Servicio de caché con Redis
├── config.py                  # Configuración central
├── data/
│   ├── 967_buildings.csv      # Dataset Google Open Buildings
│   └── loader.py              # Carga y partición del dataset
├── docker-compose.yml         # Despliegue con Docker
├── Dockerfile
├── main.py                    # Orquestador principal
├── metrics/
│   ├── metrics_store.py       # Almacenamiento de métricas
│   ├── results/               # Salida de simulaciones individuales
│   └── experiments/           # Salida de experimentos comparativos
├── query_engine/
│   └── queries.py             # Motor de consultas Q1-Q5
├── requirements.txt
├── run_experiments.py          # Automatización de experimentos
└── traffic_generator/
    └── generator.py           # Generador de tráfico sintético
```

## Consultas Implementadas

- **Q1**: Conteo de edificios en una zona
- **Q2**: Área promedio y total de edificaciones
- **Q3**: Densidad de edificaciones por km²
- **Q4**: Comparación de densidad entre dos zonas
- **Q5**: Distribución de confianza en una zona

## Métricas Recopiladas

- **Hit rate / Miss rate**
- **Throughput** (consultas/segundo)
- **Latencia p50 / p95**
- **Eviction rate** (evictions/minuto)
- **Cache efficiency**: `(hits × t_cache − misses × t_engine) / total`

## Salida

Los resultados se guardan en `metrics/results/` o `metrics/experiments/`:

- `events.csv` — Todos los eventos individuales
- `summary.json` — Resumen de métricas
- `traffic_distribution.json` — Info de la distribución usada
- `cache_info.json` — Estado final de Redis
- `comparison.json` — Tabla comparativa (solo `run_experiments.py`)
