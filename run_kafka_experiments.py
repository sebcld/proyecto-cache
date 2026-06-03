#!/usr/bin/env python3
"""
Escenarios:
  1. sync_baseline    Tarea 1 síncrona (sin Kafka) — línea base.
  2. kafka_single     Kafka + 1 consumer, sin fallos.
  3. scaling          Kafka + N consumers (1,2,4,8) — escalado horizontal.
  4. temporal_failure Caída temporal del backend → recuperación automática.
  5. retries          Fallos aleatorios (FAIL_RATE) → retry/recovery/DLQ.
  6. spike            Pico de tráfico → evolución del backlog.
  7. recovery_compare Síncrono vs Kafka ante una caída: consultas perdidas.

Uso:
    python run_kafka_experiments.py                  # todos los escenarios
    python run_kafka_experiments.py --scenario spike
    python run_kafka_experiments.py --total 3000 --output-dir metrics/kafka_experiments
"""

import os
import re
import sys
import json
import time
import argparse
import subprocess
from datetime import datetime, timezone

# ──────────────────────────────────────────────────────────────────────────────
# Constantes / helpers de Docker
# ──────────────────────────────────────────────────────────────────────────────

COMPOSE = ["docker", "compose", "-f", "docker-compose.yml", "-f", "docker-compose.kafka.yml"]
KAFKA_CONTAINER = "kafka-broker"
REDIS_CONTAINER = "cache-redis"
TOPICS = ["queries", "queries.retry", "queries.dlq"]

METRIC_KEYS = ["processed", "hits", "misses", "retried", "recovered", "dlq", "evictions"]


def log(msg: str) -> None:
    print(f"[runner] {msg}", flush=True)


def run(cmd, env=None, capture=False, check=True):
    """Ejecuta un comando; env se MERGEA sobre el entorno actual."""
    full_env = {**os.environ, **(env or {})}
    return subprocess.run(cmd, env=full_env, check=check,
                          capture_output=capture, text=True)


# ── Redis ─────────────────────────────────────────────────────────────────────

def redis_cli(*args) -> str:
    r = run(["docker", "exec", REDIS_CONTAINER, "redis-cli", *args],
            capture=True, check=False)
    return (r.stdout or "").strip()


def flush_redis() -> None:
    redis_cli("flushall")


def reset_metrics() -> None:
    keys = [f"metrics:{k}" for k in METRIC_KEYS]
    keys += ["metrics:latencies:hit", "metrics:latencies:miss"]
    redis_cli("del", *keys)


def _int(val: str) -> int:
    try:
        return int(val)
    except (ValueError, TypeError):
        return 0


def terminal_count() -> int:
    """Mensajes en estado final = procesados con éxito + enviados a DLQ."""
    return _int(redis_cli("get", "metrics:processed")) + _int(redis_cli("get", "metrics:dlq"))


# ── Tópicos Kafka ───────────────────────────────────────────────────────────

def reset_topics(partitions: int = 8) -> None:
    """Borra y recrea los tópicos para arrancar cada escenario sin backlog viejo
    """
    for t in TOPICS:
        run(["docker", "exec", KAFKA_CONTAINER, "/opt/kafka/bin/kafka-topics.sh",
             "--bootstrap-server", "localhost:9092", "--delete", "--topic", t],
            check=False, capture=True)
    time.sleep(3)
    run(COMPOSE + ["run", "--rm",
                   "-e", f"KAFKA_PARTITIONS_QUERIES={partitions}",
                   "-e", f"KAFKA_PARTITIONS_RETRY={partitions}",
                   "kafka-init"], capture=True, check=False)


# ── Consumers ───────────────────────────────────────────────────────────────

def start_consumers(n: int, env: dict | None = None) -> None:
    run(COMPOSE + ["up", "-d", "--scale", f"consumer={n}", "consumer"],
        env=env or {}, capture=True)


def stop_consumers() -> None:
    run(COMPOSE + ["rm", "-sf", "consumer"], capture=True, check=False)


def wait_consumers_ready(n: int, timeout: float = 90.0) -> bool:
    """Espera a que el grupo query-workers tenga ≥ n miembros y estado Stable.
    """
    deadline = time.time() + timeout
    while time.time() < deadline:
        r = run(["docker", "exec", KAFKA_CONTAINER,
                 "/opt/kafka/bin/kafka-consumer-groups.sh",
                 "--bootstrap-server", "localhost:9092",
                 "--describe", "--group", "query-workers", "--state"],
                capture=True, check=False)
        for line in (r.stdout or "").splitlines():
            if "query-workers" not in line:
                continue
            parts = line.split()
            try:
                members = int(parts[-1])
            except (ValueError, IndexError):
                continue
            if "Stable" in line and members >= n:
                return True
        time.sleep(1)
    log(f"⚠ wait_consumers_ready: timeout esperando {n} consumer(s) Stable")
    return False


# ── Producer ────────────────────────────────────────────────────────────────

def run_producer(total: int, qps: float, extra_env: dict | None = None,
                 background: bool = False):
    e = {"TOTAL_QUERIES": str(total), "QPS": str(qps)}
    if extra_env:
        e.update(extra_env)
    cmd = COMPOSE + ["--profile", "producer", "run", "--rm", "producer"]
    if background:
        return subprocess.Popen(cmd, env={**os.environ, **e},
                                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL)
    run(cmd, env=e, capture=True)
    return None


# ── Snapshots de métricas / lag (corren dentro de un contenedor efímero) ──────

def snapshot_metrics() -> dict:
    r = run(COMPOSE + ["run", "--rm", "--no-deps", "consumer",
                       "python", "-m", "kafka_layer.kafka_metrics"],
            capture=True, check=False)
    out = r.stdout or ""
    i = out.find("{")
    if i < 0:
        return {}
    try:
        return json.loads(out[i:])
    except json.JSONDecodeError:
        return {}


def sample_lag() -> int:
    r = run(COMPOSE + ["run", "--rm", "--no-deps", "consumer",
                       "python", "-m", "kafka_layer.lag_monitor"],
            capture=True, check=False)
    m = re.search(r"total=(\d+)", r.stdout or "")
    return int(m.group(1)) if m else 0


def wait_drain(total: int, t0: float, timeout: float = 180.0,
               poll: float = 1.0) -> tuple[float, bool]:
    """Espera a que todos los mensajes alcancen estado final. Retorna (elapsed, ok)."""
    while time.time() - t0 < timeout:
        if terminal_count() >= total:
            return time.time() - t0, True
        time.sleep(poll)
    return time.time() - t0, False


# ──────────────────────────────────────────────────────────────────────────────
# Preparación común
# ──────────────────────────────────────────────────────────────────────────────

def prepare(consumers: int = 0, consumer_env: dict | None = None,
            partitions: int = 8) -> None:
    stop_consumers()
    reset_topics(partitions=partitions)
    flush_redis()
    reset_metrics()
    if consumers > 0:
        start_consumers(consumers, consumer_env)
        wait_consumers_ready(consumers)


# ──────────────────────────────────────────────────────────────────────────────
# Escenarios
# ──────────────────────────────────────────────────────────────────────────────

def scenario_sync_baseline(total: int) -> dict:
    """Tarea 1 síncrona: el generador llama directo a la caché, sin Kafka."""
    log("Escenario 1: baseline SÍNCRONO (Tarea 1, sin Kafka)")
    stop_consumers()
    flush_redis()
    out_dir = "metrics/kafka_experiments/_sync_baseline"
    tag = "zipf_ttl300_200mb_allkeys-lru"
    t0 = time.time()
    run(COMPOSE + ["run", "--rm", "app", "python", "main.py",
                   "--total", str(total), "--qps", "0",
                   "--ttl", "300", "--max-memory", "200mb",
                   "--eviction-policy", "allkeys-lru", "--distribution", "zipf",
                   "--output-dir", out_dir, "--flush"],
        capture=True, check=False)
    elapsed = time.time() - t0
    path = os.path.join(out_dir, tag, "summary.json")
    summary = {}
    if os.path.exists(path):
        with open(path) as f:
            summary = json.load(f)
    return {
        "scenario": "sync_baseline",
        "architecture": "synchronous",
        "total": total,
        "elapsed_s": round(elapsed, 2),
        "throughput_qps": round(total / elapsed, 1) if elapsed else 0,
        "hit_rate": summary.get("hit_rate"),
        "latency_p50_ms": summary.get("latency_p50_ms"),
        "latency_p95_ms": summary.get("latency_p95_ms"),
        "has_retry_mechanism": False,
    }


def _run_kafka_load(total: int, consumers: int, qps: float = 0,
                    consumer_env: dict | None = None,
                    producer_env: dict | None = None,
                    timeout: float = 180.0,
                    partitions: int = 8) -> dict:
    """Núcleo reutilizable: prepara, publica, drena y toma snapshot."""
    prepare(consumers=consumers, consumer_env=consumer_env, partitions=partitions)
    t0 = time.time()
    run_producer(total, qps, extra_env=producer_env)
    elapsed, ok = wait_drain(total, t0, timeout=timeout)
    metrics = snapshot_metrics()
    stop_consumers()
    return {
        "total": total,
        "consumers": consumers,
        "elapsed_s": round(elapsed, 2),
        "throughput_qps": round(total / elapsed, 1) if elapsed else 0,
        "drained": ok,
        "metrics": metrics,
    }


def scenario_kafka_single(total: int) -> dict:
    log("Escenario 2: Kafka + 1 consumer (sin fallos)")
    res = _run_kafka_load(total, consumers=1)
    res["scenario"] = "kafka_single"
    res["architecture"] = "kafka"
    return res


def scenario_scaling(total: int, levels=(1, 2, 4, 8, 16),
                     partitions: int = 16) -> dict:
    """Escalado horizontal. Sube las particiones a 16 para que cada consumer
    de N=16 pueda recibir asignación de una partición (Kafka asigna ≤1
    partición por consumer dentro de un mismo grupo)."""
    log(f"Escenario 3: escalado horizontal {levels} ({partitions} particiones)")
    runs = []
    for n in levels:
        log(f"  → {n} consumer(s)")
        r = _run_kafka_load(total, consumers=n, partitions=partitions)
        runs.append({
            "consumers": n,
            "elapsed_s": r["elapsed_s"],
            "throughput_qps": r["throughput_qps"],
            "drained": r["drained"],
        })
    base = runs[0]["throughput_qps"] or 1
    for r in runs:
        r["speedup_vs_1"] = round(r["throughput_qps"] / base, 2)
    return {"scenario": "scaling", "architecture": "kafka",
            "total": total, "runs": runs}


def scenario_temporal_failure(total: int, consumers: int = 2) -> dict:
    log("Escenario 4: caída temporal del backend + recuperación")
    # Ventana 3s < budget de reintentos (5×1s = 5s): los mensajes que fallan
    # durante la caída tienen presupuesto para retry hasta que el backend vuelva.
    env = {"BACKEND_DOWN_START_S": "0", "BACKEND_DOWN_DURATION_S": "3",
           "RETRY_BACKOFF_S": "1.0", "MAX_RETRIES": "5"}
    res = _run_kafka_load(total, consumers=consumers, consumer_env=env)
    res["scenario"] = "temporal_failure"
    res["architecture"] = "kafka"
    res["fault"] = env
    return res


def scenario_retries(total: int, consumers: int = 2, fail_rate: float = 0.3) -> dict:
    log(f"Escenario 5: fallos aleatorios FAIL_RATE={fail_rate}")
    env = {"FAIL_RATE": str(fail_rate), "RETRY_BACKOFF_S": "0.5"}
    res = _run_kafka_load(total, consumers=consumers, consumer_env=env)
    res["scenario"] = "retries"
    res["architecture"] = "kafka"
    res["fault"] = env
    return res


def scenario_spike(total: int, consumers: int = 1) -> dict:
    """1 consumer (capacidad ~180 q/s) + spike a 500 q/s para que el backlog
    crezca claramente por encima de la capacidad y luego se drene."""
    log("Escenario 6: spike de tráfico (backlog en el tiempo)")
    prepare(consumers=consumers)
    spike = {"SPIKE_QPS": "500", "SPIKE_DURATION_S": "10", "SPIKE_START_S": "3"}
    t0 = time.time()
    proc = run_producer(total, qps=30, extra_env=spike, background=True)

    samples = []
    max_backlog = 0
    # Muestrea mientras el producer publica y mientras se drena el backlog.
    while True:
        lag = sample_lag()
        elapsed = round(time.time() - t0, 1)
        samples.append({"t": elapsed, "backlog": lag})
        max_backlog = max(max_backlog, lag)
        producing = proc.poll() is None
        if not producing and (terminal_count() >= total or elapsed > 180):
            if lag == 0:
                break
        if elapsed > 200:
            break

    elapsed = time.time() - t0
    metrics = snapshot_metrics()
    stop_consumers()
    return {
        "scenario": "spike", "architecture": "kafka",
        "total": total, "consumers": consumers,
        "spike": spike,
        "elapsed_s": round(elapsed, 2),
        "max_backlog": max_backlog,
        "backlog_samples": samples,
        "metrics": metrics,
    }


def scenario_recovery_compare(total: int, consumers: int = 2) -> dict:
    """
    Compara recuperación síncrono vs Kafka ante una caída temporal del backend.
    """
    log("Escenario 7: recuperación síncrono vs Kafka")
    env = {"BACKEND_DOWN_START_S": "0", "BACKEND_DOWN_DURATION_S": "3",
           "RETRY_BACKOFF_S": "1.0", "MAX_RETRIES": "5"}
    res = _run_kafka_load(total, consumers=consumers, consumer_env=env)
    m = res["metrics"]
    recovered = m.get("recovered", 0)
    dlq = m.get("dlq", 0)
    failed_once = recovered + dlq
    return {
        "scenario": "recovery_compare",
        "total": total,
        "fault": env,
        "kafka": {
            "messages_failed_at_least_once": failed_once,
            "recovered_by_retry": recovered,
            "lost_to_dlq": dlq,
            "recovery_rate": m.get("recovery_rate"),
        },
        "synchronous_equivalent": {
            # Sin retry, todas las consultas que fallaron se habrían perdido.
            "messages_lost": failed_once,
            "recovery_rate": 0.0,
        },
        "messages_saved_by_kafka": recovered,
    }


def scenario_failrate_sweep(total: int, consumers: int = 2,
                            fail_rates=(0.1, 0.3, 0.5, 0.7, 0.9)) -> dict:
    """Barrido de FAIL_RATE """
    log(f"Escenario 8: barrido de FAIL_RATE {fail_rates}")
    runs = []
    for p in fail_rates:
        log(f"  → FAIL_RATE = {p}")
        env = {"FAIL_RATE": str(p), "RETRY_BACKOFF_S": "0.5",
               "MAX_RETRIES": "3"}
        r = _run_kafka_load(total, consumers=consumers,
                            consumer_env=env, timeout=240)
        m = r["metrics"]
        runs.append({
            "fail_rate":        p,
            "processed":        m.get("processed", 0),
            "dlq":              m.get("dlq", 0),
            "retried":          m.get("retried", 0),
            "recovered":        m.get("recovered", 0),
            "retries_per_msg":  m.get("retries_per_msg", 0),
            "recovery_rate":    m.get("recovery_rate", 0),
            "dlq_rate":         m.get("dlq_rate", 0),
            "success_rate":     m.get("success_rate", 0),
            "throughput_qps":   r["throughput_qps"],
        })
    return {"scenario": "failrate_sweep", "architecture": "kafka",
            "total": total, "consumers": consumers,
            "max_retries": 3, "retry_backoff_s": 0.5,
            "runs": runs}


SCENARIOS = {
    "sync":     scenario_sync_baseline,
    "single":   scenario_kafka_single,
    "scaling":  scenario_scaling,
    "temporal": scenario_temporal_failure,
    "retries":  scenario_retries,
    "spike":    scenario_spike,
    "recovery": scenario_recovery_compare,
    "failrate_sweep": scenario_failrate_sweep,
}
ORDER = ["sync", "single", "scaling", "temporal", "retries", "spike", "recovery",
         "failrate_sweep"]


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def parse_args():
    p = argparse.ArgumentParser(description="Runner de escenarios Tarea 2 (Kafka)")
    p.add_argument("--scenario", choices=["all", *SCENARIOS.keys()], default="all")
    p.add_argument("--total", type=int, default=2000,
                   help="Consultas por escenario (default 2000)")
    p.add_argument("--output-dir", default="metrics/kafka_experiments")
    return p.parse_args()


def main() -> int:
    args = parse_args()
    os.makedirs(args.output_dir, exist_ok=True)

    targets = ORDER if args.scenario == "all" else [args.scenario]
    results = {}

    for key in targets:
        try:
            res = SCENARIOS[key](args.total)
        except Exception as e:  # un escenario no debe tumbar el resto
            log(f"✗ Escenario '{key}' falló: {e}")
            res = {"scenario": key, "error": str(e)}
        results[key] = res
        path = os.path.join(args.output_dir, f"{key}.json")
        with open(path, "w") as f:
            json.dump(res, f, indent=2)
        log(f"✓ {key} → {path}")

    combined = {
        "generated_at": datetime.now(timezone.utc).isoformat(),
        "total_per_scenario": args.total,
        "scenarios": results,
    }
    combined_path = os.path.join(args.output_dir, "all_results.json")
    with open(combined_path, "w") as f:
        json.dump(combined, f, indent=2)
    log(f"✓ Resumen combinado → {combined_path}")

    stop_consumers()
    return 0


if __name__ == "__main__":
    sys.exit(main())
