"""

Reutiliza la clase TrafficGenerator
Soporta dos modos de operacion:
  - Normal:  publica total consultas a tasa constante qps.
  - Spike :  durante la ventana [spike_start_s, spike_start_s+spike_duration_s]
             la tasa sube a spike_qps; fuera de ella vuelve a qps.

Configuracion del Producer:
  - acks=all              → el broker confirma tras escribir (durabilidad)
  - enable.idempotence    → previene duplicados ante reintentos internos
  - linger.ms=10          → batching ligero para mejorar throughput
  - compression=snappy    → reduce ancho de banda
  - partitioner por defecto → round-robin sticky (balanceo entre particiones)

Uso:
    python -m kafka_layer.producer --distribution zipf --total 5000 --qps 100
    python -m kafka_layer.producer --total 5000 --qps 50 \\
        --spike-qps 500 --spike-duration-s 5 --spike-start-s 10
"""

import os
import sys
import time
import argparse

from confluent_kafka import Producer

from kafka_config import KAFKA_BOOTSTRAP_SERVERS, TOPIC_QUERIES
from kafka_layer.message import QueryMessage
from traffic_generator.generator import TrafficGenerator


# Producer factory

def make_producer() -> Producer:
    """Construye un Producer con configuracion para durabilidad + throughput."""
    return Producer({
        "bootstrap.servers": KAFKA_BOOTSTRAP_SERVERS,
        "client.id":         "traffic-producer",
        "acks":              "all",
        "enable.idempotence": True,
        "linger.ms":         10,
        "compression.type":  "snappy",
        # Retries internos del cliente ante errores transitorios del broker.
        # Distintos de los retries de aplicacion (queries.retry).
        "retries":            5,
        "retry.backoff.ms":  200,
    })


def _delivery_callback(err, msg):
    """Callback invocado cuando un mensaje se confirma (o falla) en el broker."""
    if err is not None:
        print(f"[producer] delivery FAIL: {err} (topic={msg.topic()})",
              file=sys.stderr)


# Loop principal (con soporte de spike)

def publish_loop(
    producer: Producer,
    distribution: str,
    total: int,
    qps: float,
    spike_qps: float = 0,
    spike_duration_s: float = 0,
    spike_start_s: float = 0,
    zipf_param: float = 1.5,
    seed: int = 42,
) -> dict:
    """
    Publica total mensajes en TOPIC_QUERIES.

    Throttling dinamico: el QPS efectivo puede cambiar a `spike_qps`
    durante la ventana [spike_start_s, spike_start_s+spike_duration_s].
    Fuera de esa ventana se mantiene en `qps`.

    Retorna un dict con estadisticas del run (total publicado, duracion,
    si hubo spike, etc.) — util para que el runner las guarde junto con
    las metricas del consumer.
    """
    gen = TrafficGenerator(
        distribution=distribution,
        zipf_param=zipf_param,
        seed=seed,
    )

    spike_enabled = spike_qps > 0 and spike_duration_s > 0
    spike_end_s = spike_start_s + spike_duration_s

    print(f"[producer] Iniciando publicacion → topic={TOPIC_QUERIES}")
    print(f"[producer]   distribucion={distribution}  total={total}  qps={qps}")
    if spike_enabled:
        print(f"[producer]   SPIKE: {spike_qps} q/s durante "
              f"[{spike_start_s:.1f}s, {spike_end_s:.1f}s]")

    t_start = time.perf_counter()
    last_print = t_start
    in_spike = False
    published = 0

    #  qps=0 al generator interno: controlar el throttling aqui
    # para poder variarlo dinamicamente (modo spike).
    for query in gen.generate(total, queries_per_second=0):
        elapsed = time.perf_counter() - t_start

        # Determinar QPS objetivo segun ventana de spike.
        if spike_enabled and spike_start_s <= elapsed < spike_end_s:
            target_qps = spike_qps
            if not in_spike:
                print(f"[producer] ▲ SPIKE START @ {elapsed:.1f}s → {spike_qps:.0f} q/s")
                in_spike = True
        else:
            target_qps = qps
            if in_spike:
                print(f"[producer] ▼ SPIKE END   @ {elapsed:.1f}s → {qps:.0f} q/s")
                in_spike = False

        # Construir y publicar el mensaje.
        msg = QueryMessage.new(query["query_type"], query["params"])
        try:
            producer.produce(
                topic=TOPIC_QUERIES,
                value=msg.to_json_bytes(),
                callback=_delivery_callback,
            )
        except BufferError:
            # Cola interna llena: drenar callbacks y reintentar.
            producer.poll(0.5)
            producer.produce(
                topic=TOPIC_QUERIES,
                value=msg.to_json_bytes(),
                callback=_delivery_callback,
            )

        producer.poll(0)  # servir callbacks pendientes sin bloquear
        published += 1

        # Throttling al QPS objetivo 
        # de prueba 10..2000 q/s.
        if target_qps > 0:
            time.sleep(1.0 / target_qps)

        # Reporte de progreso cada 2s.
        now = time.perf_counter()
        if now - last_print >= 2.0:
            instant_qps = published / (now - t_start)
            mode = "SPIKE" if in_spike else "normal"
            print(f"[producer]   [{published}/{total}] "
                  f"avg={instant_qps:.0f} q/s  mode={mode}")
            last_print = now

    duration = time.perf_counter() - t_start
    print(f"[producer] Flushing pendientes...")
    producer.flush(30)
    avg_qps = published / duration if duration > 0 else 0
    print(f"[producer] ✓ Listo: {published} mensajes en {duration:.2f}s "
          f"(avg={avg_qps:.0f} q/s)")

    return {
        "published":         published,
        "duration_seconds":  round(duration, 2),
        "avg_qps":           round(avg_qps, 2),
        "distribution":      distribution,
        "target_qps":        qps,
        "spike_enabled":     spike_enabled,
        "spike_qps":         spike_qps if spike_enabled else None,
        "spike_duration_s":  spike_duration_s if spike_enabled else None,
        "spike_start_s":     spike_start_s if spike_enabled else None,
    }



# CLI

def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(
        description="Producer Kafka — Generador de Trafico (Tarea 2)"
    )
    parser.add_argument("--distribution", type=str,
                        default=os.getenv("DISTRIBUTION", "zipf"),
                        choices=["zipf", "uniform"])
    parser.add_argument("--total", type=int,
                        default=int(os.getenv("TOTAL_QUERIES", 5000)))
    parser.add_argument("--qps", type=float,
                        default=float(os.getenv("QPS", 50)))
    parser.add_argument("--spike-qps", type=float,
                        default=float(os.getenv("SPIKE_QPS", 0)),
                        help="QPS durante la ventana de spike (0 = desactivado)")
    parser.add_argument("--spike-duration-s", type=float,
                        default=float(os.getenv("SPIKE_DURATION_S", 0)),
                        help="Duracion del spike en segundos")
    parser.add_argument("--spike-start-s", type=float,
                        default=float(os.getenv("SPIKE_START_S", 5)),
                        help="Segundos desde el inicio hasta arrancar el spike")
    parser.add_argument("--zipf-param", type=float, default=1.5)
    parser.add_argument("--seed", type=int, default=42)
    return parser.parse_args()


def main() -> int:
    args = parse_args()
    producer = make_producer()
    try:
        publish_loop(
            producer=producer,
            distribution=args.distribution,
            total=args.total,
            qps=args.qps,
            spike_qps=args.spike_qps,
            spike_duration_s=args.spike_duration_s,
            spike_start_s=args.spike_start_s,
            zipf_param=args.zipf_param,
            seed=args.seed,
        )
        return 0
    except KeyboardInterrupt:
        print("\n[producer] Interrumpido por usuario. Flushing...")
        producer.flush(10)
        return 130


if __name__ == "__main__":
    sys.exit(main())
