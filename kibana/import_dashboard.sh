#!/usr/bin/env bash
# import_dashboard.sh - Importa el dashboard de Tarea 3 en una instancia de
# Kibana 8 usando el NDJSON exportado.
#
# Uso:
#   ./kibana/import_dashboard.sh   
# http://localhost:5601
#   KIBANA_URL=http://kibana:5601 ./kibana/import_dashboard.sh
#
# El NDJSON contiene:
#   - 5 visualizaciones(throughput, latency, hit_rate, retry_rate, dlq)
#   - 1 dashboard 
#   - 1 referencia al data view metrics-aggregated (debe este existir antes)
set -euo pipefail

KIBANA_URL="${KIBANA_URL:-http://localhost:5601}"
NDJSON="$(dirname "$0")/metrics_dashboard.ndjson"

if [ ! -f "$NDJSON" ]; then
  echo "ERROR: no existe $NDJSON" >&2
  echo "       Genere primero el archivo con: python3 kibana/setup_dashboard.py" >&2
  exit 1
fi

echo "[kibana] importando $NDJSON -> $KIBANA_URL ..."
RESP=$(curl -sf -X POST \
  "$KIBANA_URL/api/saved_objects/_import?overwrite=true" \
  -H "kbn-xsrf: tarea3" \
  --form file=@"$NDJSON")

echo "$RESP" | python3 -m json.tool 2>/dev/null || echo "$RESP"

# Extraer success count para feedback
SUCCESS_COUNT=$(echo "$RESP" | python3 -c "import sys,json; print(json.load(sys.stdin).get('successCount', 0))" 2>/dev/null || echo "?")
echo
echo "[kibana] objetos importados: $SUCCESS_COUNT"
echo "[kibana] Dashboard accesible en:"
echo "  $KIBANA_URL/app/dashboards#/view/tarea3-metrics-dashboard"
