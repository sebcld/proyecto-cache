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

---

# Tarea 2 — Arquitectura Asíncrona con Apache Kafka

La Tarea 2 introduce una **capa de mensajería asíncrona** entre el Generador de
Tráfico y el procesamiento, usando Apache Kafka. La Tarea 1 **no se modifica**:
el motor de consultas y la caché Redis se reutilizan tal cual (solo se importan).

## ¿Cómo funciona?

En vez de que el generador llame directo a la caché (modo síncrono de la Tarea 1),
ahora **publica cada consulta como un mensaje** en Kafka, y un grupo de *consumers*
las procesa de forma independiente y escalable.

```
                          ┌──────────── tópico: queries ───────────┐
┌──────────────┐ publica  │  [p0][p1][p2][p3][p4][p5][p6][p7]       │
│  Producer    │─────────▶│         (8 particiones)                 │
│ (TrafficGen) │          └────────────────┬────────────────────────┘
└──────────────┘                           │ consume (group.id compartido)
                                ┌───────────▼───────────┐
                                │   Consumers (1..N)     │  cache HIT ─▶ Redis
                                │   procesan vía la      │  cache MISS ─▶ QueryEngine
                                │   caché de la Tarea 1  │
                                └───────┬───────┬────────┘
                          fallo (MISS)  │       │ éxito → commit manual del offset
                       ┌────────────────▼─┐   ┌─▼────────────────────┐
                       │ tópico:           │   │ métricas compartidas │
                       │ queries.retry     │   │ en Redis             │
                       │ (reintentos)      │   └──────────────────────┘
                       └────────┬──────────┘
                  retry_count ≥ MAX_RETRIES
                                ▼
                       ┌───────────────────┐
                       │ tópico: queries.dlq│  (dead-letter queue)
                       └───────────────────┘
```

**Componentes clave:**

- **Producer** (`kafka_layer/producer.py`): reutiliza el `TrafficGenerator` y publica
  en `queries`. Soporta **modo spike** (QPS dinámico) y entrega idempotente (`acks=all`).
- **3 tópicos**: `queries` (flujo principal, 8 particiones), `queries.retry`
  (reintentos, 8 particiones), `queries.dlq` (mensajes agotados, 1 partición).
- **Consumers** (`kafka_layer/consumer.py`): comparten `group.id` → Kafka reparte
  las particiones automáticamente (**escalado horizontal**). Procesan vía la caché
  de la Tarea 1 y hacen **commit manual** del offset tras procesar (*at-least-once*).
- **Retry / DLQ**: si una consulta falla, se republica en `queries.retry` con
  `retry_count+1` (preservando `query_id`); tras `MAX_RETRIES` fallos va a `queries.dlq`.
- **Inyección de fallos** (`kafka_layer/faults.py`): simula la caída del Generador de
  Respuestas..
- **Métricas extendidas** (`kafka_layer/kafka_metrics.py`) y **backlog/lag**
  (`kafka_layer/lag_monitor.py`).

## Ejecución de la Tarea 2 — Paso a Paso

> Todos los comandos usan los **dos** archivos compose: el base
> (`docker-compose.yml`) y el override de Kafka (`docker-compose.kafka.yml`).
> Para un alias más corto:
> ```bash
> export COMPOSE="docker compose -f docker-compose.yml -f docker-compose.kafka.yml"
> ```

### Paso 1 — Levantar la infraestructura (Kafka + Redis)

```bash
$COMPOSE up -d --build kafka redis
$COMPOSE run --rm kafka-init   
```

### Paso 2 — Correr los 7 escenarios

El runner `run_kafka_experiments.py` orquesta los siete escenarios de evaluación,
inyecta fallos por variable de entorno, drena el pipeline entre cada uno y
exporta un JSON por escenario en `metrics/kafka_experiments/`.

```bash
# Los 7 escenarios 3000 mensajes c/u (~10 min)
for s in sync single temporal retries spike recovery failrate_sweep; do
  python3 run_kafka_experiments.py --scenario $s --total 3000
done

# Escalado horizontal (necesita más mensajes y 16 particiones)
python3 run_kafka_experiments.py --scenario scaling --total 4000
```

Escenarios: `sync` (baseline Tarea 1), `single` (Kafka + 1 consumer),
`scaling` (1/2/4/8/16 consumers), `temporal` (caída temporal del backend),
`retries` (fallos aleatorios), `spike` (pico de tráfico), `recovery`
(síncrono vs Kafka), `failrate_sweep` (barrido de FAIL_RATE para sensibilidad
del DLQ).

### Paso 3 — Generar los gráficos comparativos

```bash
$COMPOSE --profile plots run --rm plots python plot_kafka_results.py
```

Genera 7 PNGs en `metrics/plots/kafka/`:
`comparison_sync_vs_kafka`, `scaling_throughput`, `spike_backlog`,
`fault_metrics`, `recovery_comparison`, `recovery_time`, `failrate_sweep`.

### Paso 4 — Limpiar el entorno

```bash
$COMPOSE down -v
```



## Variables de Entorno (Tarea 2)

| Variable | Servicio | Default | Descripción |
|---|---|---|---|
| `TOTAL_QUERIES` | producer | `5000` | Consultas a publicar |
| `QPS` | producer | `50` | Tasa de publicación (0 = sin límite) |
| `SPIKE_QPS` / `SPIKE_DURATION_S` / `SPIKE_START_S` | producer | `0` / `0` / `5` | Ventana de pico de tráfico |
| `MAX_RETRIES` | consumer | `3` | Reintentos antes de DLQ |
| `FAIL_RATE` | consumer | `0.0` | Probabilidad de fallo por MISS (0–1) |
| `RESPONSES_DOWN` | consumer | `false` | Caída permanente del backend |
| `BACKEND_DOWN_START_S` / `BACKEND_DOWN_DURATION_S` | consumer | `0` / `0` | Ventana de caída temporal |
| `ARTIFICIAL_LATENCY_MS` | consumer | `0` | Latencia artificial por MISS |
| `RETRY_BACKOFF_S` | consumer | `2.0` | Espera entre reintentos |

## Métricas del Pipeline (Tarea 2)

| Métrica | Descripción |
|---|---|
| **retries_per_msg** | Reintentos promedio por mensaje completado |
| **recovery_rate** | Fracción de mensajes fallidos que terminó recuperándose |
| **dlq_rate** | Fracción de mensajes que acabó en la dead-letter queue |
| **success_rate** | Fracción de mensajes procesados con éxito |
| **backlog / lag** | Mensajes pendientes (high watermark − offset commiteado) |

---



