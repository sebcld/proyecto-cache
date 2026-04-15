"""
plot_results.py — Generación de gráficos para el informe
Proyecto: Plataforma de análisis de caché — Sistemas Distribuidos 2026-1

Lee los resultados de run_experiments.py y genera gráficos comparativos.

Uso:
    python plot_results.py
    python plot_results.py --input metrics/experiments --format pdf
"""

import os
import json
import argparse

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


# ──────────────────────────────────────────────────────────────────────────────
# Configuración visual
# ──────────────────────────────────────────────────────────────────────────────

plt.rcParams.update({
    "figure.figsize": (10, 6),
    "figure.dpi": 150,
    "font.size": 11,
    "axes.titlesize": 14,
    "axes.labelsize": 12,
    "legend.fontsize": 10,
    "axes.grid": True,
    "grid.alpha": 0.3,
})

COLORS = {
    "zipf":    "#2196F3",
    "uniform": "#FF9800",
    "allkeys-lru":    "#4CAF50",
    "allkeys-lfu":    "#9C27B0",
    "allkeys-random": "#F44336",
}

POLICY_LABELS = {
    "allkeys-lru":    "LRU",
    "allkeys-lfu":    "LFU",
    "allkeys-random": "FIFO (random)",
}

# Valores por defecto cuando se fija un parámetro
DEFAULT_SIZE = "200mb"
DEFAULT_TTL = 60
DEFAULT_POLICY = "allkeys-lru"


def load_comparison(input_dir: str) -> list[dict]:
    path = os.path.join(input_dir, "comparison.json")
    if os.path.exists(path):
        with open(path) as f:
            return json.load(f)

    summaries = []
    for subdir in sorted(os.listdir(input_dir)):
        sp = os.path.join(input_dir, subdir, "summary.json")
        if os.path.isfile(sp):
            with open(sp) as f:
                summaries.append(json.load(f))
    if not summaries:
        raise FileNotFoundError(f"No hay resultados en '{input_dir}'")
    return summaries


def filter_data(data, **kwargs):
    result = []
    for d in data:
        exp = d.get("experiment", {})
        if all(exp.get(k) == v for k, v in kwargs.items()):
            result.append(d)
    return result


def get_unique_values(data, key):
    return sorted(set(d["experiment"][key] for d in data))


def safe_get(data, metric, default=0):
    """Safely extract a metric from filtered data."""
    if data:
        return data[0].get(metric, default)
    return default


# ──────────────────────────────────────────────────────────────────────────────
# 1. Hit Rate por distribución de tráfico
# ──────────────────────────────────────────────────────────────────────────────

def plot_hit_rate_by_distribution(data, output_dir, fmt):
    fig, ax = plt.subplots()
    policies = get_unique_values(data, "eviction_policy")
    x = np.arange(len(policies))
    width = 0.35

    for i, dist in enumerate(["zipf", "uniform"]):
        rates = []
        for policy in policies:
            subset = filter_data(data, distribution=dist, eviction_policy=policy,
                                 cache_size=DEFAULT_SIZE, ttl=DEFAULT_TTL)
            rates.append(safe_get(subset, "hit_rate", 0) * 100)

        bars = ax.bar(x + i * width, rates, width,
                      label=dist.capitalize(), color=COLORS[dist], alpha=0.85)
        for bar, rate in zip(bars, rates):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{rate:.1f}%", ha="center", va="bottom", fontsize=9)

    ax.set_xlabel("Política de evicción")
    ax.set_ylabel("Hit Rate (%)")
    ax.set_title(f"Hit Rate por Distribución de Tráfico\n(caché={DEFAULT_SIZE}, TTL={DEFAULT_TTL}s)")
    ax.set_xticks(x + width / 2)
    ax.set_xticklabels([POLICY_LABELS.get(p, p) for p in policies])
    ax.legend()
    ax.set_ylim(0, 105)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"hit_rate_by_distribution.{fmt}"))
    plt.close()
    print(f"  ✓ hit_rate_by_distribution.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 2. Hit Rate por política de evicción
# ──────────────────────────────────────────────────────────────────────────────

def plot_hit_rate_by_policy(data, output_dir, fmt):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)

    for ax, dist in zip(axes, ["zipf", "uniform"]):
        policies = []
        rates = []
        colors = []

        for policy in ["allkeys-lru", "allkeys-lfu", "allkeys-random"]:
            subset = filter_data(data, distribution=dist, eviction_policy=policy,
                                 cache_size=DEFAULT_SIZE, ttl=DEFAULT_TTL)
            if subset:
                policies.append(POLICY_LABELS.get(policy, policy))
                rates.append(subset[0]["hit_rate"] * 100)
                colors.append(COLORS[policy])

        bars = ax.bar(policies, rates, color=colors, alpha=0.85)
        for bar, rate in zip(bars, rates):
            ax.text(bar.get_x() + bar.get_width() / 2, bar.get_height() + 0.5,
                    f"{rate:.1f}%", ha="center", fontsize=10)

        ax.set_title(f"Distribución: {dist.capitalize()}")
        ax.set_ylabel("Hit Rate (%)" if dist == "zipf" else "")
        ax.set_ylim(0, 105)

    fig.suptitle(f"Hit Rate por Política de Evicción (caché={DEFAULT_SIZE}, TTL={DEFAULT_TTL}s)",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"hit_rate_by_policy.{fmt}"))
    plt.close()
    print(f"  ✓ hit_rate_by_policy.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 3. Hit Rate por tamaño de caché
# ──────────────────────────────────────────────────────────────────────────────

def plot_hit_rate_by_cache_size(data, output_dir, fmt):
    fig, ax = plt.subplots()
    sizes = get_unique_values(data, "cache_size")

    for dist in ["zipf", "uniform"]:
        rates = []
        for size in sizes:
            subset = filter_data(data, distribution=dist, cache_size=size,
                                 eviction_policy=DEFAULT_POLICY, ttl=DEFAULT_TTL)
            rates.append(safe_get(subset, "hit_rate", 0) * 100)

        ax.plot(sizes, rates, marker="o", linewidth=2, markersize=8,
                label=dist.capitalize(), color=COLORS[dist])
        for i, rate in enumerate(rates):
            ax.annotate(f"{rate:.1f}%", (sizes[i], rate),
                        textcoords="offset points", xytext=(0, 10),
                        ha="center", fontsize=9)

    ax.set_xlabel("Tamaño de Caché")
    ax.set_ylabel("Hit Rate (%)")
    ax.set_title(f"Impacto del Tamaño de Caché en Hit Rate\n(política=LRU, TTL={DEFAULT_TTL}s)")
    ax.legend()
    ax.set_ylim(0, 105)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"hit_rate_by_cache_size.{fmt}"))
    plt.close()
    print(f"  ✓ hit_rate_by_cache_size.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 4. Hit Rate por TTL
# ──────────────────────────────────────────────────────────────────────────────

def plot_hit_rate_by_ttl(data, output_dir, fmt):
    fig, ax = plt.subplots()
    ttls = get_unique_values(data, "ttl")
    ttl_labels = [f"{t}s" for t in ttls]

    for dist in ["zipf", "uniform"]:
        rates = []
        for ttl in ttls:
            subset = filter_data(data, distribution=dist, ttl=ttl,
                                 eviction_policy=DEFAULT_POLICY,
                                 cache_size=DEFAULT_SIZE)
            rates.append(safe_get(subset, "hit_rate", 0) * 100)

        ax.plot(ttl_labels, rates, marker="s", linewidth=2, markersize=8,
                label=dist.capitalize(), color=COLORS[dist])
        for i, rate in enumerate(rates):
            ax.annotate(f"{rate:.1f}%", (ttl_labels[i], rate),
                        textcoords="offset points", xytext=(0, 10),
                        ha="center", fontsize=9)

    ax.set_xlabel("TTL (segundos)")
    ax.set_ylabel("Hit Rate (%)")
    ax.set_title(f"Impacto del TTL en Hit Rate\n(política=LRU, caché={DEFAULT_SIZE})")
    ax.legend()
    ax.set_ylim(0, 105)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"hit_rate_by_ttl.{fmt}"))
    plt.close()
    print(f"  ✓ hit_rate_by_ttl.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 5. Latencia p50 y p95
# ──────────────────────────────────────────────────────────────────────────────

def plot_latency_comparison(data, output_dir, fmt):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6))

    for ax, dist in zip(axes, ["zipf", "uniform"]):
        policies = ["allkeys-lru", "allkeys-lfu", "allkeys-random"]
        p50s, p95s, labels = [], [], []

        for policy in policies:
            subset = filter_data(data, distribution=dist, eviction_policy=policy,
                                 cache_size=DEFAULT_SIZE, ttl=DEFAULT_TTL)
            if subset:
                p50s.append(subset[0]["latency_p50_ms"])
                p95s.append(subset[0]["latency_p95_ms"])
                labels.append(POLICY_LABELS.get(policy, policy))

        x = np.arange(len(labels))
        width = 0.35
        ax.bar(x - width/2, p50s, width, label="p50", color="#42A5F5", alpha=0.85)
        ax.bar(x + width/2, p95s, width, label="p95", color="#EF5350", alpha=0.85)

        for i, (v50, v95) in enumerate(zip(p50s, p95s)):
            ax.text(i - width/2, v50, f"{v50:.2f}", ha="center", va="bottom", fontsize=8)
            ax.text(i + width/2, v95, f"{v95:.2f}", ha="center", va="bottom", fontsize=8)

        ax.set_xlabel("Política de evicción")
        ax.set_ylabel("Latencia (ms)")
        ax.set_title(f"Distribución: {dist.capitalize()}")
        ax.set_xticks(x)
        ax.set_xticklabels(labels)
        ax.legend()

    fig.suptitle(f"Latencia p50 y p95 por Política\n(caché={DEFAULT_SIZE}, TTL={DEFAULT_TTL}s)",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"latency_comparison.{fmt}"))
    plt.close()
    print(f"  ✓ latency_comparison.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 6. Throughput
# ──────────────────────────────────────────────────────────────────────────────

def plot_throughput(data, output_dir, fmt):
    fig, ax = plt.subplots()
    sizes = get_unique_values(data, "cache_size")

    configs, t_zipf, t_uniform = [], [], []

    for policy in ["allkeys-lru", "allkeys-lfu", "allkeys-random"]:
        for size in sizes:
            label = f"{POLICY_LABELS.get(policy, policy)}\n{size}"
            configs.append(label)
            z = filter_data(data, distribution="zipf", eviction_policy=policy,
                            cache_size=size, ttl=DEFAULT_TTL)
            u = filter_data(data, distribution="uniform", eviction_policy=policy,
                            cache_size=size, ttl=DEFAULT_TTL)
            t_zipf.append(safe_get(z, "throughput_qps"))
            t_uniform.append(safe_get(u, "throughput_qps"))

    x = np.arange(len(configs))
    width = 0.35
    ax.bar(x - width/2, t_zipf, width, label="Zipf", color=COLORS["zipf"], alpha=0.85)
    ax.bar(x + width/2, t_uniform, width, label="Uniform", color=COLORS["uniform"], alpha=0.85)

    ax.set_xlabel("Configuración")
    ax.set_ylabel("Throughput (consultas/s)")
    ax.set_title(f"Throughput por Configuración (TTL={DEFAULT_TTL}s)")
    ax.set_xticks(x)
    ax.set_xticklabels(configs, fontsize=7)
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"throughput.{fmt}"))
    plt.close()
    print(f"  ✓ throughput.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 7. Cache Efficiency
# ──────────────────────────────────────────────────────────────────────────────

def plot_cache_efficiency(data, output_dir, fmt):
    fig, ax = plt.subplots()
    sizes = get_unique_values(data, "cache_size")

    for policy in ["allkeys-lru", "allkeys-lfu", "allkeys-random"]:
        effs = []
        for size in sizes:
            subset = filter_data(data, distribution="zipf", eviction_policy=policy,
                                 cache_size=size, ttl=DEFAULT_TTL)
            effs.append(safe_get(subset, "cache_efficiency"))

        ax.plot(sizes, effs, marker="D", linewidth=2, markersize=8,
                label=POLICY_LABELS.get(policy, policy), color=COLORS[policy])

    ax.set_xlabel("Tamaño de Caché")
    ax.set_ylabel("Cache Efficiency")
    ax.set_title(f"Cache Efficiency por Tamaño y Política\n(distribución=Zipf, TTL={DEFAULT_TTL}s)")
    ax.legend()
    ax.axhline(y=0, color="gray", linestyle="--", alpha=0.5)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"cache_efficiency.{fmt}"))
    plt.close()
    print(f"  ✓ cache_efficiency.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 8. Desglose por tipo de consulta (Q1-Q5)
# ──────────────────────────────────────────────────────────────────────────────

def plot_per_query_breakdown(data, output_dir, fmt):
    fig, axes = plt.subplots(1, 2, figsize=(14, 6), sharey=True)
    colors_q = ["#2196F3", "#4CAF50", "#FF9800", "#9C27B0", "#F44336"]

    for ax, dist in zip(axes, ["zipf", "uniform"]):
        subset = filter_data(data, distribution=dist, eviction_policy=DEFAULT_POLICY,
                             cache_size=DEFAULT_SIZE, ttl=DEFAULT_TTL)
        if not subset or "per_query" not in subset[0]:
            continue

        per_q = subset[0]["per_query"]
        queries = sorted(per_q.keys())
        hit_rates = [per_q[q]["hit_rate"] * 100 for q in queries]

        bars = ax.bar(queries, hit_rates, color=colors_q[:len(queries)], alpha=0.85)
        for bar, rate in zip(bars, hit_rates):
            ax.text(bar.get_x() + bar.get_width()/2, bar.get_height() + 0.5,
                    f"{rate:.1f}%", ha="center", fontsize=10)

        ax.set_xlabel("Tipo de Consulta")
        ax.set_ylabel("Hit Rate (%)" if dist == "zipf" else "")
        ax.set_title(f"Distribución: {dist.capitalize()}")
        ax.set_ylim(0, 105)

    fig.suptitle(f"Hit Rate por Tipo de Consulta\n(LRU, caché={DEFAULT_SIZE}, TTL={DEFAULT_TTL}s)",
                 fontsize=14, fontweight="bold")
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"per_query_breakdown.{fmt}"))
    plt.close()
    print(f"  ✓ per_query_breakdown.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 9. Heatmap: Hit Rate por política × tamaño
# ──────────────────────────────────────────────────────────────────────────────

def plot_heatmap(data, output_dir, fmt):
    fig, axes = plt.subplots(1, 2, figsize=(14, 5))
    policies = ["allkeys-lru", "allkeys-lfu", "allkeys-random"]
    sizes = get_unique_values(data, "cache_size")
    policy_labels = [POLICY_LABELS.get(p, p) for p in policies]

    for ax, dist in zip(axes, ["zipf", "uniform"]):
        matrix = np.zeros((len(policies), len(sizes)))

        for i, policy in enumerate(policies):
            for j, size in enumerate(sizes):
                subset = filter_data(data, distribution=dist,
                                     eviction_policy=policy,
                                     cache_size=size, ttl=DEFAULT_TTL)
                if subset:
                    matrix[i, j] = subset[0]["hit_rate"] * 100

        im = ax.imshow(matrix, cmap="YlGn", aspect="auto", vmin=0, vmax=100)
        for i in range(len(policies)):
            for j in range(len(sizes)):
                color = "white" if matrix[i, j] > 70 else "black"
                ax.text(j, i, f"{matrix[i, j]:.1f}%",
                        ha="center", va="center", color=color, fontsize=11)

        ax.set_xticks(range(len(sizes)))
        ax.set_xticklabels(sizes)
        ax.set_yticks(range(len(policies)))
        ax.set_yticklabels(policy_labels)
        ax.set_xlabel("Tamaño de Caché")
        ax.set_ylabel("Política")
        ax.set_title(f"Distribución: {dist.capitalize()}")

    fig.suptitle(f"Heatmap: Hit Rate (%) — Política × Tamaño (TTL={DEFAULT_TTL}s)",
                 fontsize=14, fontweight="bold")
    fig.colorbar(im, ax=axes, label="Hit Rate (%)", shrink=0.8)
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"heatmap_hit_rate.{fmt}"))
    plt.close()
    print(f"  ✓ heatmap_hit_rate.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# 10. Eviction Rate por tamaño
# ──────────────────────────────────────────────────────────────────────────────

def plot_eviction_rate(data, output_dir, fmt):
    fig, ax = plt.subplots()
    sizes = get_unique_values(data, "cache_size")

    for policy in ["allkeys-lru", "allkeys-lfu", "allkeys-random"]:
        evictions = []
        for size in sizes:
            subset = filter_data(data, distribution="zipf",
                                 eviction_policy=policy,
                                 cache_size=size, ttl=DEFAULT_TTL)
            evictions.append(safe_get(subset, "redis_evicted_keys", 0))

        ax.plot(sizes, evictions, marker="^", linewidth=2, markersize=8,
                label=POLICY_LABELS.get(policy, policy), color=COLORS[policy])

    ax.set_xlabel("Tamaño de Caché")
    ax.set_ylabel("Total de claves eviccionadas")
    ax.set_title(f"Evictions por Tamaño de Caché\n(distribución=Zipf, TTL={DEFAULT_TTL}s)")
    ax.legend()
    plt.tight_layout()
    plt.savefig(os.path.join(output_dir, f"eviction_rate.{fmt}"))
    plt.close()
    print(f"  ✓ eviction_rate.{fmt}")


# ──────────────────────────────────────────────────────────────────────────────
# Main
# ──────────────────────────────────────────────────────────────────────────────

def main():
    parser = argparse.ArgumentParser(description="Genera gráficos del sistema de caché")
    parser.add_argument("--input", type=str, default="metrics/experiments")
    parser.add_argument("--output", type=str, default="metrics/plots")
    parser.add_argument("--format", type=str, default="png",
                        choices=["png", "pdf", "svg"])
    args = parser.parse_args()

    os.makedirs(args.output, exist_ok=True)

    print(f"\n[plot] Cargando resultados desde: {args.input}/")
    data = load_comparison(args.input)
    print(f"[plot] {len(data)} experimentos cargados.\n")

    plot_hit_rate_by_distribution(data, args.output, args.format)
    plot_hit_rate_by_policy(data, args.output, args.format)
    plot_hit_rate_by_cache_size(data, args.output, args.format)
    plot_hit_rate_by_ttl(data, args.output, args.format)
    plot_latency_comparison(data, args.output, args.format)
    plot_throughput(data, args.output, args.format)
    plot_cache_efficiency(data, args.output, args.format)
    plot_per_query_breakdown(data, args.output, args.format)
    plot_heatmap(data, args.output, args.format)
    plot_eviction_rate(data, args.output, args.format)

    print(f"\n[plot] ¡Listo! 10 gráficos en {args.output}/\n")


if __name__ == "__main__":
    main()