#!/usr/bin/env python3
"""
Uso:
    python3 kibana/setup_dashboard.py
    python3 kibana/setup_dashboard.py --kibana-url http://localhost:5601

"""

import argparse
import json
import os
import sys
import urllib.error
import urllib.request


KBN_HEADERS = {
    "kbn-xsrf": "tarea3",
    "Content-Type": "application/json",
}

DATA_VIEW_NAME = "metrics-aggregated"
DATA_VIEW_PATTERN = "metrics-aggregated*"
DATA_VIEW_TIME_FIELD = "window_start_iso"

DASHBOARD_ID = "tarea3-metrics-dashboard"
DASHBOARD_TITLE = "Tarea 3 - Metricas en Tiempo Real"

VIZ_IDS = {
    "throughput":  "tarea3-viz-throughput",
    "latency":     "tarea3-viz-latency",
    "hit_rate":    "tarea3-viz-hit-rate",
    "retry_rate":  "tarea3-viz-retry-rate",
    "dlq_count":   "tarea3-viz-dlq-count",
}


# Helpers HTTP

def kbn(url: str, method: str = "GET", payload: dict | None = None) -> tuple[int, dict | str]:
    """Llamada simple a la API de Kibana con XSRF + JSON body opcional."""
    data = json.dumps(payload).encode() if payload is not None else None
    req = urllib.request.Request(url, data=data, method=method, headers=KBN_HEADERS)
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            body = resp.read().decode("utf-8")
            return resp.status, (json.loads(body) if body.startswith(("{", "[")) else body)
    except urllib.error.HTTPError as e:
        body = e.read().decode("utf-8")
        try:
            return e.code, json.loads(body)
        except Exception:
            return e.code, body


# Data view

def ensure_data_view(kbn_url: str) -> str:
    """Devuelve el id del data view. Si no existe, lo crea."""
    status, data = kbn(f"{kbn_url}/api/data_views")
    if status == 200 and isinstance(data, dict):
        for dv in data.get("data_view", []):
            if dv.get("name") == DATA_VIEW_NAME or dv.get("title") == DATA_VIEW_PATTERN:
                print(f"[kibana] data view existente: {dv['id']}")
                return dv["id"]

    # No existe, crear
    print(f"[kibana] creando data view '{DATA_VIEW_NAME}'...")
    status, data = kbn(
        f"{kbn_url}/api/data_views/data_view",
        method="POST",
        payload={
            "data_view": {
                "name": DATA_VIEW_NAME,
                "title": DATA_VIEW_PATTERN,
                "timeFieldName": DATA_VIEW_TIME_FIELD,
            }
        },
    )
    if status >= 300:
        raise RuntimeError(f"creacion de data view fallo: {status} {data}")
    dv_id = data["data_view"]["id"]
    print(f"[kibana]   id={dv_id}")
    return dv_id


# Visualizaciones Lens (lnsXY, linea simple)

def _xy_layer(layer_id: str, field: str, agg: str, label: str) -> dict:
    """Construye una capa Lens XY con 1 metrica agrupada por window_start_iso."""
    col_x = f"{layer_id}-x"
    col_y = f"{layer_id}-y"
    return {
        "columnOrder": [col_x, col_y],
        "columns": {
            col_x: {
                "label": "window_start_iso",
                "dataType": "date",
                "operationType": "date_histogram",
                "sourceField": DATA_VIEW_TIME_FIELD,
                "isBucketed": True,
                "scale": "interval",
                "params": {"interval": "auto", "includeEmptyRows": False, "dropPartials": False},
            },
            col_y: {
                "label": label,
                "dataType": "number",
                "operationType": agg,
                "sourceField": field,
                "isBucketed": False,
                "scale": "ratio",
                "params": {"emptyAsNull": True},
            },
        },
        "incompleteColumns": {},
    }, col_x, col_y


def build_lens_viz(
    title: str,
    description: str,
    dataview_id: str,
    series: list[dict],
) -> dict:
    """
    Construye un saved object de Lens (lnsXY) con N series superpuestas.

    series: lista de dicts con keys:
      - field:  campo del documento ES (ej. 'throughput')
      - agg:    operacion ('max', 'avg', 'sum')
      - label:  etiqueta visible
      - color:  '#rrggbb' opcional
    """
    layers_state = {}
    layers_viz = []
    references = []

    for i, s in enumerate(series):
        layer_id = f"layer{i}"
        layer_state, col_x, col_y = _xy_layer(layer_id, s["field"], s["agg"], s["label"])
        layer_state["indexPatternId"] = dataview_id
        layers_state[layer_id] = layer_state

        layer_viz = {
            "layerId": layer_id,
            "accessors": [col_y],
            "position": "top",
            "seriesType": "line",
            "showGridlines": False,
            "layerType": "data",
            "xAccessor": col_x,
        }
        # color por serie (Lens 8.11 acepta yConfig)
        if s.get("color"):
            layer_viz["yConfig"] = [{"forAccessor": col_y, "color": s["color"]}]
        layers_viz.append(layer_viz)

        references.append({
            "name": f"indexpattern-datasource-layer-{layer_id}",
            "type": "index-pattern",
            "id": dataview_id,
        })

    return {
        "attributes": {
            "title": title,
            "description": description,
            "visualizationType": "lnsXY",
            "state": {
                "datasourceStates": {"formBased": {"layers": layers_state}},
                "visualization": {
                    "preferredSeriesType": "line",
                    "legend": {"isVisible": True, "position": "bottom"},
                    "valueLabels": "hide",
                    "fittingFunction": "Linear",
                    "axisTitlesVisibilitySettings": {"x": False, "yLeft": True, "yRight": True},
                    "tickLabelsVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                    "labelsOrientation": {"x": 0, "yLeft": 0, "yRight": 0},
                    "gridlinesVisibilitySettings": {"x": True, "yLeft": True, "yRight": True},
                    "layers": layers_viz,
                },
                "query": {"query": "", "language": "kuery"},
                "filters": [],
            },
        },
        "references": references,
    }


def upsert_lens(kbn_url: str, viz_id: str, body: dict) -> None:
    """Crea o reemplaza un saved object lens con id estable."""
    url = f"{kbn_url}/api/saved_objects/lens/{viz_id}?overwrite=true"
    status, data = kbn(url, method="POST", payload=body)
    if status >= 300:
        raise RuntimeError(f"upsert lens '{viz_id}' fallo: {status} {data}")
    print(f"[kibana]   ✓ lens {viz_id}")


# Dashboard

def build_dashboard_panels() -> tuple[list[dict], list[dict]]:
    """
    Devuelve (panels, references) para el dashboard.
    """
    # (panel_index, gridData, viz_id_key)
    layout = [
        (0, {"x": 0,  "y": 0,  "w": 24, "h": 15}, "throughput"),
        (1, {"x": 24, "y": 0,  "w": 24, "h": 15}, "latency"),
        (2, {"x": 0,  "y": 15, "w": 24, "h": 15}, "hit_rate"),
        (3, {"x": 24, "y": 15, "w": 24, "h": 15}, "retry_rate"),
        (4, {"x": 0,  "y": 30, "w": 48, "h": 15}, "dlq_count"),
    ]
    panels = []
    refs = []
    for idx, grid, key in layout:
        panel_index = f"panel_{idx}"
        grid["i"] = panel_index
        ref_name = f"panel_{panel_index}"
        panels.append({
            "version": "8.11.4",
            "type": "lens",
            "gridData": grid,
            "panelIndex": panel_index,
            "embeddableConfig": {"enhancements": {}},
            "panelRefName": ref_name,
        })
        refs.append({
            "name": ref_name,
            "type": "lens",
            "id": VIZ_IDS[key],
        })
    return panels, refs


def build_dashboard(dataview_id: str) -> dict:
    panels, refs = build_dashboard_panels()
    search_source = {
        "query": {"query": "", "language": "kuery"},
        "filter": [],
    }
    return {
        "attributes": {
            "title": DASHBOARD_TITLE,
            "description": (
                "Dashboard de Tarea 3: agregaciones por ventana deslizante (1 min "
                "/ slide 30 s) producidas por el job Spark Structured Streaming "
                "sobre metrics-topic. Auto-refresh 10 s, rango 'ultima hora'."
            ),
            "hits": 0,
            "panelsJSON": json.dumps(panels),
            "optionsJSON": json.dumps({
                "useMargins": True,
                "syncColors": False,
                "syncTooltips": False,
                "syncCursor": True,
                "hidePanelTitles": False,
            }),
            "version": 3,
            "timeRestore": True,
            "timeTo": "now",
            "timeFrom": "now-1h",
            "refreshInterval": {"pause": False, "value": 10000},
            "kibanaSavedObjectMeta": {"searchSourceJSON": json.dumps(search_source)},
        },
        "references": refs,
    }


def upsert_dashboard(kbn_url: str, body: dict) -> None:
    url = f"{kbn_url}/api/saved_objects/dashboard/{DASHBOARD_ID}?overwrite=true"
    status, data = kbn(url, method="POST", payload=body)
    if status >= 300:
        raise RuntimeError(f"upsert dashboard fallo: {status} {data}")
    print(f"[kibana]   ✓ dashboard {DASHBOARD_ID}")

# Export NDJSON (para que el dashboard se pueda re-importar)
def export_ndjson(kbn_url: str, out_path: str) -> None:
    """
    Exporta el dashboard + las vizs (con includeReferencesDeep) como NDJSON.
    """
    payload = {
        "objects": [{"type": "dashboard", "id": DASHBOARD_ID}],
        "includeReferencesDeep": True,
    }
    req = urllib.request.Request(
        f"{kbn_url}/api/saved_objects/_export",
        data=json.dumps(payload).encode(),
        method="POST",
        headers=KBN_HEADERS,
    )
    with urllib.request.urlopen(req, timeout=30) as resp:
        body = resp.read()
    with open(out_path, "wb") as f:
        f.write(body)
    print(f"[kibana]   ✓ exportado -> {out_path}  ({len(body)} bytes)")


# Main
def main() -> int:
    p = argparse.ArgumentParser()
    p.add_argument("--kibana-url", default="http://localhost:5601")
    p.add_argument("--out", default=os.path.join(
        os.path.dirname(__file__) or ".", "metrics_dashboard.ndjson"))
    args = p.parse_args()

    print(f"[kibana] target: {args.kibana_url}")
    dv_id = ensure_data_view(args.kibana_url)

    print("[kibana] creando visualizaciones Lens...")

    # 1) Throughput
    upsert_lens(args.kibana_url, VIZ_IDS["throughput"], build_lens_viz(
        title="Throughput por ventana (consultas exitosas)",
        description="max(throughput) por bucket auto",
        dataview_id=dv_id,
        series=[{"field": "throughput", "agg": "max",
                 "label": "Throughput", "color": "#1f77b4"}],
    ))

    # 2) p50 y p95 superpuestas
    upsert_lens(args.kibana_url, VIZ_IDS["latency"], build_lens_viz(
        title="Latencia p50 y p95 (ms)",
        description="max(latency_p50_ms) y max(latency_p95_ms)",
        dataview_id=dv_id,
        series=[
            {"field": "latency_p50_ms", "agg": "max",
             "label": "p50 (ms)", "color": "#2ca02c"},
            {"field": "latency_p95_ms", "agg": "max",
             "label": "p95 (ms)", "color": "#d62728"},
        ],
    ))

    # 3) Hit rate
    upsert_lens(args.kibana_url, VIZ_IDS["hit_rate"], build_lens_viz(
        title="Hit rate (cache)",
        description="max(hit_rate) por bucket auto",
        dataview_id=dv_id,
        series=[{"field": "hit_rate", "agg": "max",
                 "label": "Hit rate", "color": "#9467bd"}],
    ))

    # 4) Retry rate
    upsert_lens(args.kibana_url, VIZ_IDS["retry_rate"], build_lens_viz(
        title="Retry rate",
        description="max(retry_rate) - fraccion con retry_count > 0",
        dataview_id=dv_id,
        series=[{"field": "retry_rate", "agg": "max",
                 "label": "Retry rate", "color": "#ff7f0e"}],
    ))

    # 5) DLQ count
    upsert_lens(args.kibana_url, VIZ_IDS["dlq_count"], build_lens_viz(
        title="DLQ count (mensajes descartados por ventana)",
        description="max(dlq_count)",
        dataview_id=dv_id,
        series=[{"field": "dlq_count", "agg": "max",
                 "label": "DLQ", "color": "#8c564b"}],
    ))

    print("[kibana] creando dashboard...")
    upsert_dashboard(args.kibana_url, build_dashboard(dv_id))

    print("[kibana] exportando NDJSON...")
    export_ndjson(args.kibana_url, args.out)

    print()
    print("=" * 60)
    print(f"OK. Dashboard accesible en:")
    print(f"  {args.kibana_url}/app/dashboards#/view/{DASHBOARD_ID}")
    print("=" * 60)
    return 0


if __name__ == "__main__":
    sys.exit(main())
