#!/usr/bin/env python3
"""
plot_kafka_results.py — Visualizaciones comparativas de los escenarios de la Tarea 2.

Lee los JSON producidos por run_kafka_experiments.py y genera 5 gráficos en
metrics/plots/kafka/:

  - comparison_sync_vs_kafka.png  Síncrono (Tarea 1) vs Kafka (1 consumer)
  - scaling_throughput.png        Throughput y speedup vs N consumers
  - spike_backlog.png             Evolución del backlog durante un spike
  - fault_metrics.png             Recovery / DLQ / Success rate bajo fallos
  - recovery_comparison.png       Mensajes recuperados vs perdidos: sync vs Kafka

Uso:
    python3 plot_kafka_results.py
    python3 plot_kafka_results.py --input metrics/kafka_experiments --output metrics/plots/kafka
"""

import os
import json
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ── helpers ──────────────────────────────────────────────────────────────────

def load(input_dir: str, name: str):
    path = os.path.join(input_dir, f"{name}.json")
    if not os.path.exists(path):
        return None
    with open(path) as f:
        return json.load(f)


def save(fig, output_dir: str, name: str) -> None:
    os.makedirs(output_dir, exist_ok=True)
    path = os.path.join(output_dir, f"{name}.png")
    fig.tight_layout()
    fig.savefig(path, dpi=130, bbox_inches="tight")
    plt.close(fig)
    print(f"[plots] ✓ {path}")


# ── plots ────────────────────────────────────────────────────────────────────

def plot_sync_vs_kafka(sync, single, output_dir):
    if not sync or not single:
        print("[plots] skip sync_vs_kafka (faltan datos)"); return

    km = single.get("metrics", {})
    labels = ["Síncrono\n(Tarea 1)", "Kafka\n(1 consumer)"]
    throughput = [sync["throughput_qps"], single["throughput_qps"]]
    p50 = [sync.get("latency_p50_ms", 0), km.get("latency_p50_ms", 0)]
    p95 = [sync.get("latency_p95_ms", 0), km.get("latency_p95_ms", 0)]

    fig, axes = plt.subplots(1, 3, figsize=(13, 4.5))
    colors = ["#888888", "#1f77b4"]
    x = np.arange(2)

    for ax, vals, title, ylab, fmt in [
        (axes[0], throughput, "Throughput", "q/s", "{:.1f}"),
        (axes[1], p50,        "Latencia p50", "ms",  "{:.2f}"),
        (axes[2], p95,        "Latencia p95", "ms",  "{:.2f}"),
    ]:
        ax.bar(x, vals, color=colors)
        ax.set_xticks(x); ax.set_xticklabels(labels)
        ax.set_ylabel(ylab); ax.set_title(title)
        ax.grid(True, alpha=0.3, axis="y")
        for i, v in enumerate(vals):
            ax.text(i, v, fmt.format(v), ha="center", va="bottom")

    fig.suptitle("Sistema síncrono (Tarea 1) vs Kafka + 1 consumer", fontsize=13, y=1.02)
    save(fig, output_dir, "comparison_sync_vs_kafka")


def plot_scaling(scaling, output_dir):
    if not scaling:
        print("[plots] skip scaling"); return
    runs = scaling["runs"]
    n = [r["consumers"] for r in runs]
    tp = [r["throughput_qps"] for r in runs]
    sp = [r["speedup_vs_1"] for r in runs]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(12, 4.8))
    xs = np.arange(len(n))

    a1.bar(xs, tp, color="#1f77b4")
    a1.set_xticks(xs); a1.set_xticklabels([str(x) for x in n])
    a1.set_xlabel("Número de consumers")
    a1.set_ylabel("Throughput (q/s)")
    a1.set_title("Throughput agregado")
    a1.grid(True, alpha=0.3, axis="y")
    for i, v in enumerate(tp):
        a1.text(i, v, f"{v:.1f}", ha="center", va="bottom")

    a2.bar(xs, sp, color="#1f77b4", label="Observado")
    a2.plot(xs, n, "k--", alpha=0.5, marker="o", label="Ideal (lineal)")
    a2.set_xticks(xs); a2.set_xticklabels([str(x) for x in n])
    a2.set_xlabel("Número de consumers")
    a2.set_ylabel("Speedup vs N=1")
    a2.set_title("Speedup observado vs ideal")
    a2.grid(True, alpha=0.3, axis="y")
    a2.legend()
    for i, v in enumerate(sp):
        a2.text(i, v, f"{v:.2f}×", ha="center", va="bottom")

    fig.suptitle(f"Escalado horizontal (total={scaling['total']} mensajes)",
                 fontsize=13, y=1.02)
    save(fig, output_dir, "scaling_throughput")


def plot_spike_backlog(spike, output_dir):
    if not spike:
        print("[plots] skip spike"); return
    samples = spike["backlog_samples"]
    t = [s["t"] for s in samples]
    b = [s["backlog"] for s in samples]
    cfg = spike["spike"]
    t0 = float(cfg["SPIKE_START_S"]); dur = float(cfg["SPIKE_DURATION_S"])

    fig, ax = plt.subplots(figsize=(11, 5))
    ax.fill_between(t, b, step="post", alpha=0.25, color="#1f77b4")
    ax.plot(t, b, "-o", color="#1f77b4", markersize=4, linewidth=1.5)
    ax.axvspan(t0, t0 + dur, alpha=0.18, color="red",
               label=f"Pico ({cfg['SPIKE_QPS']} q/s × {int(dur)}s)")
    ax.axhline(y=spike["max_backlog"], color="red", linestyle=":", alpha=0.6)

    if b:
        idx_max = b.index(spike["max_backlog"])
        ax.annotate(f"max = {spike['max_backlog']}",
                    xy=(t[idx_max], spike["max_backlog"]),
                    xytext=(10, -15), textcoords="offset points",
                    color="red", fontweight="bold")

    ax.set_xlabel("Tiempo (s)")
    ax.set_ylabel("Backlog (mensajes pendientes en Kafka)")
    ax.set_title(f"Evolución del backlog durante un spike de tráfico — "
                 f"{spike['consumers']} consumers")
    ax.grid(True, alpha=0.3)
    ax.legend()
    save(fig, output_dir, "spike_backlog")


def plot_fault_metrics(temporal, retries, output_dir):
    scenarios = []
    if temporal:
        dur = temporal.get("fault", {}).get("BACKEND_DOWN_DURATION_S", "?")
        scenarios.append((f"Caída temporal\n(backend down {dur}s)", temporal["metrics"]))
    if retries:
        fr = retries.get("fault", {}).get("FAIL_RATE", "?")
        scenarios.append((f"Fallos aleatorios\n(FAIL_RATE={fr})", retries["metrics"]))
    if not scenarios:
        print("[plots] skip fault_metrics"); return

    labels = [s[0] for s in scenarios]
    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 4.8))
    xs = np.arange(len(labels))

    keys = [("recovery_rate", "Recovery rate", "#2ca02c"),
            ("dlq_rate",      "DLQ rate",      "#d62728"),
            ("success_rate",  "Success rate",  "#1f77b4")]
    w = 0.27
    for i, (k, lab, col) in enumerate(keys):
        vals = [s[1].get(k, 0) for s in scenarios]
        a1.bar(xs + (i - 1) * w, vals, w, label=lab, color=col)
        for j, v in enumerate(vals):
            a1.text(xs[j] + (i - 1) * w, v + 0.02, f"{v:.2f}",
                    ha="center", va="bottom", fontsize=9)
    a1.set_xticks(xs); a1.set_xticklabels(labels)
    a1.set_ylabel("Tasa"); a1.set_ylim(0, 1.18)
    a1.set_title("Tasas del pipeline bajo fallos")
    a1.legend(loc="upper right")
    a1.grid(True, alpha=0.3, axis="y")

    retries_vals = [s[1].get("retries_per_msg", 0) for s in scenarios]
    a2.bar(xs, retries_vals, color="#ff7f0e")
    a2.set_xticks(xs); a2.set_xticklabels(labels)
    a2.set_ylabel("Reintentos / mensaje terminal")
    a2.set_title("Reintentos promedio por mensaje")
    a2.grid(True, alpha=0.3, axis="y")
    for i, v in enumerate(retries_vals):
        a2.text(i, v, f"{v:.2f}", ha="center", va="bottom")

    fig.suptitle("Métricas del pipeline bajo escenarios de fallo",
                 fontsize=13, y=1.02)
    save(fig, output_dir, "fault_metrics")


def plot_recovery_time(scenarios, output_dir):
    """Recovery time: tiempo de vaciado de la cola tras un incidente.

    - spike:    desde el max_backlog hasta backlog=0 (drenado tras pico)
    - temporal: elapsed_s − baseline esperado (overhead por retries)
    - retries:  elapsed_s − baseline esperado (overhead por retries)

    Baseline esperado = total / throughput_de_referencia, donde el throughput
    de referencia se toma del escenario scaling con el mismo número de consumers.
    """
    spike = scenarios.get("spike")
    temporal = scenarios.get("temporal")
    retries_s = scenarios.get("retries")
    scaling = scenarios.get("scaling")

    # Throughput de referencia por número de consumers (del escenario scaling)
    ref_throughput = {}
    if scaling:
        for r in scaling.get("runs", []):
            ref_throughput[r["consumers"]] = r["throughput_qps"]

    bars = []

    # Spike: peak-to-zero
    if spike:
        samples = spike["backlog_samples"]
        if samples:
            peak = spike["max_backlog"]
            t_peak = next((s["t"] for s in samples if s["backlog"] == peak), None)
            t_zero = next((s["t"] for s in samples
                           if s["t"] > (t_peak or 0) and s["backlog"] == 0), None)
            if t_peak is not None and t_zero is not None:
                bars.append(("Spike\n(500 q/s × 10s)\n[peak → 0]",
                             t_zero - t_peak, "#1f77b4"))

    # Temporal y retries: overhead sobre baseline
    for key, label, color in [
        ("temporal", "Caída temporal\n(backend down 3s)", "#d62728"),
        ("retries",  "Fallos aleatorios\n(FAIL_RATE=0.3)",  "#ff7f0e"),
    ]:
        s = scenarios.get(key)
        if not s:
            continue
        n = s.get("consumers", 1)
        tp_ref = ref_throughput.get(n)
        if tp_ref:
            baseline = s["total"] / tp_ref
            overhead = max(0, s["elapsed_s"] - baseline)
            bars.append((label, overhead, color))

    if not bars:
        print("[plots] skip recovery_time"); return

    labels  = [b[0] for b in bars]
    values  = [b[1] for b in bars]
    colors  = [b[2] for b in bars]

    fig, ax = plt.subplots(figsize=(10, 5))
    x = np.arange(len(labels))
    ax.bar(x, values, color=colors)
    ax.set_xticks(x); ax.set_xticklabels(labels)
    ax.set_ylabel("Tiempo de recuperación (segundos)")
    ax.set_title("Recovery time: tiempo de vaciado de la cola tras el incidente")
    ax.grid(True, alpha=0.3, axis="y")
    for i, v in enumerate(values):
        ax.text(i, v, f"{v:.2f}s", ha="center", va="bottom", fontweight="bold")

    save(fig, output_dir, "recovery_time")


def plot_failrate_sweep(sweep, output_dir):
    """Barrido de FAIL_RATE: cómo evolucionan recovery/DLQ/success al aumentar
    la probabilidad de fallo por cache miss."""
    if not sweep:
        print("[plots] skip failrate_sweep"); return
    runs = sweep["runs"]
    p          = [r["fail_rate"]       for r in runs]
    recovery   = [r["recovery_rate"]   for r in runs]
    dlq_rate   = [r["dlq_rate"]        for r in runs]
    success    = [r["success_rate"]    for r in runs]
    retries    = [r["retries_per_msg"] for r in runs]

    fig, (a1, a2) = plt.subplots(1, 2, figsize=(13, 5))

    a1.plot(p, success,  "-o", label="Success rate", color="#1f77b4", linewidth=2)
    a1.plot(p, recovery, "-o", label="Recovery rate", color="#2ca02c", linewidth=2)
    a1.plot(p, dlq_rate, "-o", label="DLQ rate",     color="#d62728", linewidth=2)
    a1.set_xlabel("FAIL_RATE (probabilidad de fallo por cache MISS)")
    a1.set_ylabel("Tasa")
    a1.set_title("Efecto de FAIL_RATE sobre las tasas del pipeline")
    a1.legend(loc="center right")
    a1.grid(True, alpha=0.3)
    a1.set_ylim(-0.05, 1.08)

    a2.plot(p, retries, "-o", color="#ff7f0e", linewidth=2)
    a2.set_xlabel("FAIL_RATE")
    a2.set_ylabel("Reintentos / mensaje terminal")
    a2.set_title("Reintentos promedio por mensaje")
    a2.grid(True, alpha=0.3)
    for i, v in enumerate(retries):
        a2.text(p[i], v, f"{v:.2f}", ha="center", va="bottom", fontsize=9)

    mr = sweep.get("max_retries", "?")
    bf = sweep.get("retry_backoff_s", "?")
    fig.suptitle(f"Barrido de FAIL_RATE (MAX_RETRIES={mr}, RETRY_BACKOFF={bf}s)",
                 fontsize=13, y=1.02)
    save(fig, output_dir, "failrate_sweep")


def plot_recovery_comparison(recovery, output_dir):
    if not recovery:
        print("[plots] skip recovery"); return
    k = recovery["kafka"]
    s = recovery["synchronous_equivalent"]

    archs = ["Síncrono\n(Tarea 1)", "Kafka\n(Tarea 2)"]
    recovered = [0, k["recovered_by_retry"]]
    lost = [s["messages_lost"], k["lost_to_dlq"]]

    fig, ax = plt.subplots(figsize=(9, 5))
    x = np.arange(2)
    w = 0.35
    ax.bar(x - w / 2, recovered, w, label="Recuperadas", color="#2ca02c")
    ax.bar(x + w / 2, lost, w, label="Perdidas", color="#d62728")

    ax.set_xticks(x); ax.set_xticklabels(archs)
    ax.set_ylabel("Mensajes")
    fault = recovery.get("fault", {})
    dur = fault.get("BACKEND_DOWN_DURATION_S", "?")
    ax.set_title(f"Recuperación ante caída temporal del backend "
                 f"({dur}s)\n"
                 f"Total: {recovery['total']} mensajes, "
                 f"{k['messages_failed_at_least_once']} afectados por la caída")
    ax.grid(True, alpha=0.3, axis="y")
    ax.legend()

    for i, v in enumerate(recovered):
        if v > 0:
            ax.text(i - w / 2, v, str(v), ha="center", va="bottom", fontweight="bold")
    for i, v in enumerate(lost):
        if v > 0:
            ax.text(i + w / 2, v, str(v), ha="center", va="bottom", fontweight="bold")

    save(fig, output_dir, "recovery_comparison")


# ── main ─────────────────────────────────────────────────────────────────────

def main():
    ap = argparse.ArgumentParser()
    ap.add_argument("--input", default="metrics/kafka_experiments")
    ap.add_argument("--output", default="metrics/plots/kafka")
    args = ap.parse_args()

    if not os.path.isdir(args.input):
        print(f"[plots] ✗ No existe {args.input} — ejecuta antes run_kafka_experiments.py")
        return 1

    data = {k: load(args.input, k) for k in
            ["sync", "single", "scaling", "temporal", "retries", "spike", "recovery",
             "failrate_sweep"]}

    plot_sync_vs_kafka(data["sync"], data["single"], args.output)
    plot_scaling(data["scaling"], args.output)
    plot_spike_backlog(data["spike"], args.output)
    plot_fault_metrics(data["temporal"], data["retries"], args.output)
    plot_recovery_comparison(data["recovery"], args.output)
    plot_recovery_time(data, args.output)
    plot_failrate_sweep(data["failrate_sweep"], args.output)

    print(f"\n[plots] Gráficos en {args.output}/")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
