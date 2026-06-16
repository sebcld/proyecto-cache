"""
Job de Spark Tarea 3.

Salidas soportadas:
  --output console        : imprime cada batch a stdout (depuracion, Fase 2)
  --output elasticsearch  : escribe al indice `metrics-aggregated` (Fase 3)
"""

import argparse
import json
import sys
import urllib.error
import urllib.request

from pyspark.sql import SparkSession
from pyspark.sql.functions import (
    col, count, expr, from_json, percentile_approx, sum as _sum, when, window,
)
from pyspark.sql.types import (
    DoubleType, IntegerType, LongType, StringType, StructField, StructType,
)


METRIC_SCHEMA = StructType([
    StructField("timestamp",    LongType(),    True),  # epoch milisegundos
    StructField("query_id",     StringType(),  True),
    StructField("query_type",   StringType(),  True),
    StructField("latency_ms",   DoubleType(),  True),
    StructField("cache_result", StringType(),  True),  # "hit" | "miss"
    StructField("retry_count",  IntegerType(), True),
    StructField("final_status", StringType(),  True),  # "success" | "dlq"
    StructField("consumer_id",  StringType(),  True),
])


def parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="Spark Structured Streaming sobre metrics-topic")
    p.add_argument("--output", choices=["console", "elasticsearch"], default="console",
                   help="Sink: 'console' (Fase 2 debug) o 'elasticsearch' (Fase 3).")
    p.add_argument("--kafka-bootstrap", default="kafka:9092",
                   help="Bootstrap servers de Kafka (default: kafka:9092 interno Docker).")
    p.add_argument("--topic", default="metrics-topic",
                   help="Topico de origen.")
    p.add_argument("--starting-offsets", default="earliest",
                   help="'earliest' (debug) o 'latest' (produccion).")
    p.add_argument("--window-duration", default="1 minute",
                   help="Duracion de la ventana (e.g. '1 minute', '30 seconds').")
    p.add_argument("--slide-duration", default="30 seconds",
                   help="Slide de la ventana (e.g. '30 seconds').")
    p.add_argument("--watermark", default="2 minutes",
                   help="Watermark para eventos tardios (e.g. '2 minutes').")
    p.add_argument("--trigger", default="10 seconds",
                   help="processingTime entre micro-batches.")
    p.add_argument("--checkpoint", default="/tmp/spark-checkpoint/metrics",
                   help="Directorio de checkpoint (requerido por structured streaming).")
    p.add_argument("--es-nodes", default="elasticsearch",
                   help="Hostname del nodo ES (Fase 3).")
    p.add_argument("--es-port", default="9200",
                   help="Puerto HTTP de ES (Fase 3).")
    p.add_argument("--es-index", default="metrics-aggregated",
                   help="Indice destino en ES (Fase 3).")
    return p.parse_args()


def build_spark_session() -> SparkSession:
    """Crea SparkSession local con pocas particiones para shuffle (carga moderada)."""
    spark = (
        SparkSession.builder
        .appName("MetricsStreaming-Tarea3")
        .config("spark.sql.shuffle.partitions", "4")
        .config("spark.sql.streaming.statefulOperator.checkCorrectness.enabled", "false")
        .getOrCreate()
    )
    spark.sparkContext.setLogLevel("WARN")
    return spark


def build_aggregated_stream(spark: SparkSession, args: argparse.Namespace):

    # 1) Kafka como stream binario.
    raw = (
        spark.readStream
        .format("kafka")
        .option("kafka.bootstrap.servers", args.kafka_bootstrap)
        .option("subscribe", args.topic)
        .option("startingOffsets", args.starting_offsets)

        .option("failOnDataLoss", "false")
        .load()
    )

    # 2) Parsear el JSON del campo value.
    parsed = (
        raw.selectExpr("CAST(value AS STRING) AS json_str")
        .select(from_json(col("json_str"), METRIC_SCHEMA).alias("e"))
        .select("e.*")
        # Descarta cualquier evento que no pueda parsearse (e.timestamp seria null).
        .where(col("timestamp").isNotNull())
    )

    # 3) Convertir epoch ms a TimestampType para event-time.
    with_event_time = parsed.withColumn(
        "event_time",
        (col("timestamp") / 1000.0).cast("timestamp"),
    )

    aggregated = (
        with_event_time
        .withWatermark("event_time", args.watermark)
        .groupBy(window(col("event_time"), args.window_duration, args.slide_duration))
        .agg(
            count(when(col("final_status") == "success", True)).alias("throughput"),

            
            percentile_approx(col("latency_ms"), 0.50).alias("latency_p50_ms"),
            percentile_approx(col("latency_ms"), 0.95).alias("latency_p95_ms"),

            # hit_rate y retry_rate sobre el TOTAL de eventos terminales de la ventana.
            (_sum(when(col("cache_result") == "hit", 1).otherwise(0)) /
             count("*")).alias("hit_rate"),
            (_sum(when(col("retry_count") > 0, 1).otherwise(0)) /
             count("*")).alias("retry_rate"),

            # DLQ y total para diagnostico y para que Kibana pueda graficar tasas.
            count(when(col("final_status") == "dlq", True)).alias("dlq_count"),
            count("*").alias("total_events"),
        )
        .select(
            col("window.start").alias("window_start"),
            col("window.end").alias("window_end"),
            col("throughput"),
            col("latency_p50_ms"),
            col("latency_p95_ms"),
            col("hit_rate"),
            col("retry_rate"),
            col("dlq_count"),
            col("total_events"),
        )
    )

    return aggregated


def start_console(stream, args):

    return (
        stream.writeStream
        .outputMode("update")
        .format("console")
        .option("truncate", False)
        .option("numRows", 50)
        .option("checkpointLocation", args.checkpoint + "/console")
        .trigger(processingTime=args.trigger)
        .start()
    )


def _bulk_post_to_es(rows, es_url, es_index):

    if not rows:
        return

    lines = []
    for row in rows:
        doc_id = str(row.get("window_start_ms"))
        meta = {"index": {"_index": es_index, "_id": doc_id}}
        lines.append(json.dumps(meta))
        lines.append(json.dumps(row))
    payload = ("\n".join(lines) + "\n").encode("utf-8")

    req = urllib.request.Request(
        url=f"{es_url}/_bulk",
        data=payload,
        method="POST",
        headers={"Content-Type": "application/x-ndjson"},
    )
    try:
        with urllib.request.urlopen(req, timeout=15) as resp:
            body = resp.read().decode("utf-8")
            if '"errors":true' in body:

                print(f"[es-sink] WARN bulk con errores parciales: {body[:400]}",
                      flush=True)
    except urllib.error.URLError as e:

        raise RuntimeError(f"bulk POST a ES fallo: {e}") from e


def start_elasticsearch(stream, args):

    es_url = f"http://{args.es_nodes}:{args.es_port}"
    es_index = args.es_index

    def upsert_batch(batch_df, batch_id):

        prepared = batch_df.selectExpr(
            "CAST(window_start AS LONG) * 1000 AS window_start_ms",
            "DATE_FORMAT(window_start, \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\") AS window_start_iso",
            "DATE_FORMAT(window_end,   \"yyyy-MM-dd'T'HH:mm:ss.SSSXXX\") AS window_end_iso",
            "throughput",
            "latency_p50_ms",
            "latency_p95_ms",
            "hit_rate",
            "retry_rate",
            "dlq_count",
            "total_events",
        )
        # Materializa filas como dicts (volumen bajo: pocas ventanas por trigger).
        rows = [r.asDict(recursive=True) for r in prepared.collect()]
        if rows:
            print(f"[es-sink] batch {batch_id}: {len(rows)} ventana(s) -> {es_index}",
                  flush=True)
            _bulk_post_to_es(rows, es_url, es_index)

    return (
        stream.writeStream
        .outputMode("update")
        .foreachBatch(upsert_batch)
        .option("checkpointLocation", args.checkpoint + "/elasticsearch")
        .trigger(processingTime=args.trigger)
        .start()
    )


def main() -> int:
    args = parse_args()
    print(f"[spark] window={args.window_duration} slide={args.slide_duration} "
          f"watermark={args.watermark} trigger={args.trigger}", flush=True)
    print(f"[spark] kafka={args.kafka_bootstrap} topic={args.topic} "
          f"startingOffsets={args.starting_offsets}", flush=True)
    print(f"[spark] output={args.output}", flush=True)

    spark = build_spark_session()
    aggregated = build_aggregated_stream(spark, args)

    if args.output == "console":
        query = start_console(aggregated, args)
    else:
        query = start_elasticsearch(aggregated, args)

    print(f"[spark] streaming query iniciada (id={query.id}). Esperando datos...",
          flush=True)
    query.awaitTermination()
    return 0


if __name__ == "__main__":
    sys.exit(main())
