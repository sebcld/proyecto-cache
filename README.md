# Plataforma de Análisis de Caché — Sistemas Distribuidos 2026-1

Sistema distribuido de análisis geoespacial con caché Redis para el dataset Google Open Buildings (Santiago Región Metropolitana).

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

## Ejecución con Docker — Paso a Paso

### Paso 1 — Preparar el entorno

```bash
# Clonar o posicionarse en el directorio del proyecto
cd proyecto-cache

# Verificar que el dataset existe
ls data/967_buildings.csv
```

### Paso 2 — Construir las imágenes

```bash
docker compose build
```

Esto compila la imagen Python con todas las dependencias (`requirements.txt`).
Solo es necesario la primera vez o tras cambiar el código.

### Paso 3 — Opción A: Simulación rápida (un escenario)

Ejecuta una sola simulación con los parámetros por defecto:
**Zipf, LRU, 50 MB, TTL=2s, 5000 queries**.

```bash
docker compose up
```

Los resultados quedan en `metrics/results/zipf_ttl2_50mb_allkeys-lru/`.

Para limpiar el volumen de Redis entre corridas:

```bash
docker compose down -v && docker compose up
```

### Paso 4 — Opción B: Suite completa de experimentos (54 combinaciones)

Ejecuta todas las combinaciones de distribución × política × tamaño × TTL.
**No usar `docker compose up` para esto** — lanzaría `app` y `experiments` al mismo tiempo
y colisionarían en Redis.

```bash
docker compose --profile experiments run --rm experiments
```

Para seguir el progreso en tiempo real y guardar el log:

```bash
docker compose --profile experiments run --rm experiments 2>&1 | tee experiments.log
```

Los resultados quedan en `metrics/experiments/<tag>/` y el resumen comparativo
en `metrics/experiments/comparison.json`.

Tiempo estimado: **~5 min** (54 experimentos × 5000 queries a ~1000 q/s).

### Paso 5 — Generar gráficos

Requiere que existan resultados en `metrics/experiments/` (Paso 4).

```bash
docker compose --profile plots run --rm plots
```

Los gráficos se guardan en `metrics/plots/` como archivos `.png`.

### Flujo completo de una sola vez

```bash
# 1. Construir
docker compose build

# 2. Correr todos los experimentos
docker compose --profile experiments run --rm experiments

# 3. Generar gráficos
docker compose --profile plots run --rm plots

# 4. Ver los resultados
ls metrics/plots/
ls metrics/experiments/
```

### Limpiar todo y volver a empezar

```bash
# Detener contenedores, eliminar volumen de Redis y resultados anteriores
docker compose down -v
rm -rf metrics/experiments/ metrics/plots/ metrics/results/

# Volver a correr desde cero
docker compose build
docker compose --profile experiments run --rm experiments
docker compose --profile plots run --rm plots
```

## Ejecución Local (sin Docker)

```bash
pip install -r requirements.txt

# Simulación por defecto
python main.py

# Con parámetros personalizados
python main.py --distribution uniform --total 5000 --ttl 2

# Ejecutar todos los experimentos (54 combos)
python run_experiments.py --total 5000

# Subconjunto rápido de experimentos (8 combos)
python run_experiments.py --quick --total 5000

# Generar gráficos
python plot_results.py --input metrics/experiments --output metrics/plots
```

## Parámetros Configurables

| Parámetro | Opciones | Default |
|---|---|---|
| `--distribution` | `zipf`, `uniform` | `zipf` |
| `--eviction-policy` | `allkeys-lru`, `allkeys-lfu`, `allkeys-random` | `allkeys-lru` |
| `--max-memory` | `50mb`, `200mb`, `500mb` | `50mb` |
| `--ttl` | Segundos | `2` |
| `--total` | Número de consultas | `5000` |
| `--qps` | Consultas/segundo (0=sin límite) | `0` |

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
│   ├── experiments/           # Salida de experimentos comparativos
│   └── plots/                 # Gráficos generados por plot_results.py
├── plot_results.py            # Generación de gráficos (11 plots)
├── query_engine/
│   └── queries.py             # Motor de consultas Q1-Q5
├── requirements.txt
├── run_experiments.py         # Automatización de experimentos
└── traffic_generator/
    └── generator.py           # Generador de tráfico sintético
```

## Consultas Implementadas

| Query | Descripción | Cache key |
|---|---|---|
| **Q1** | Conteo de edificios en una zona | `count:{zone}:conf={c}` |
| **Q2** | Área promedio y total de edificaciones | `area:{zone}:conf={c}` |
| **Q3** | Densidad de edificaciones por km² | `density:{zone}:conf={c}` |
| **Q4** | Comparación de densidad entre dos zonas | `compare:density:{zA}:{zB}:conf={c}` |
| **Q5** | Distribución de confianza en una zona | `confidence_dist:{zone}:bins={b}` |

Cada respuesta incluye una muestra de hasta 1000 registros (lat/lon/area/conf) para que las entradas
ocupen ~70–140 KB en Redis y el catálogo total (~60 MB) supere el límite de 50 MB, forzando
evictions reales.

## Catálogo de Consultas

El generador de tráfico construye un catálogo de ~1530 entradas únicas:

| Tipo | Parámetros | Entradas |
|---|---|---|
| Q1, Q2, Q3 | 5 zonas × 60 umbrales de confianza | 900 |
| Q4 | C(5,2)=10 pares × 60 umbrales | 600 |
| Q5 | 5 zonas × 6 valores de bins | 30 |
| **Total** | | **1530** |

## Configuraciones Experimentales

| Dimensión | Valores | Observación |
|---|---|---|
| Distribuciones | `zipf`, `uniform` | Zipf s=1.5 |
| Políticas | `allkeys-lru`, `allkeys-lfu`, `allkeys-random` | random ≈ FIFO |
| Tamaño caché | `50mb`, `200mb`, `500mb` | 50mb fuerza evictions |
| TTL | `2s`, `10s`, `300s` | 2s expira durante la simulación |

Total: **54 combinaciones** (2 × 3 × 3 × 3).

## Métricas Recopiladas

| Métrica | Descripción |
|---|---|
| **Hit rate / Miss rate** | Fracción de consultas respondidas desde caché |
| **Throughput** | Consultas por segundo |
| **Latencia p50 / p95** | Percentiles de latencia de respuesta |
| **Eviction rate** | Claves expulsadas por Redis (contador `evicted_keys`) |
| **Cache efficiency** | `(hits × t_miss_avg − misses × t_hit_avg) / total` |

## Gráficos Generados (11)

| Archivo | Responde |
|---|---|
| `hit_rate_by_distribution` | Zipf vs Uniform: diferencia de hit rate |
| `hit_rate_by_policy` | LRU vs LFU vs FIFO a 50mb (presión real) |
| `hit_rate_by_cache_size` | Impacto de 50mb → 200mb → 500mb |
| `hit_rate_by_ttl` | Efecto de TTL sobre hit rate global |
| `ttl_per_query` | Efecto de TTL desglosado por tipo Q1–Q5 |
| `latency_comparison` | p50 y p95 por política y distribución |
| `throughput` | Throughput por política, tamaño y distribución |
| `cache_efficiency` | Eficiencia por tamaño y política |
| `per_query_breakdown` | Hit rate por tipo de consulta (Q1–Q5) |
| `heatmap_hit_rate` | Matriz política × tamaño a TTL=300s |
| `eviction_rate` | Evictions por tamaño, política y distribución |

## Diseño de Experimentos

Los parámetros se eligieron para que cada gráfico aísle **una sola variable**:

- **Comparación de políticas** → `cache_size=50mb`, `TTL=300s` (sin expiraciones, solo maxmemory)
- **Impacto del tamaño** → `TTL=300s` (sin expiraciones, solo capacidad)
- **Impacto del TTL** → `cache_size=200mb` (sin maxmemory, solo expiraciones)
- **Eviction rate** → `TTL=300s` para no mezclar el contador de evictions con TTL expiry

## Salida

Los resultados se guardan en `metrics/experiments/<tag>/`:

- `events.csv` — Todos los eventos individuales con timestamps y latencias
- `summary.json` — Resumen de métricas agregadas y por tipo de consulta
- `comparison.json` — Tabla comparativa de todos los experimentos (raíz de `experiments/`)
