#!/usr/bin/env bash
# =============================================================
# experimentos/lanzar_pc2_experimento.sh
# Lanza analítica + semáforos + BD réplica en PC2 para una corrida
# del experimento. Idéntico a lanzar_pc2.sh pero deja los procesos en
# background y nombra los logs por escenario+diseño para no pisarlos.
#
# Uso:
#   bash lanzar_pc2_experimento.sh --escenario A --diseno simple
# =============================================================
set -e

ESCENARIO=""
DISENO="simple"
while [[ $# -gt 0 ]]; do
  case $1 in
    --escenario) ESCENARIO="$2"; shift 2 ;;
    --diseno)    DISENO="$2";    shift 2 ;;
    *) echo "Argumento desconocido: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# Limpiar procesos previos
pkill -f "base_datos_replica.py"   2>/dev/null || true
pkill -f "servicio_semaforos.py"   2>/dev/null || true
pkill -f "servicio_analitica.py"   2>/dev/null || true
sleep 1

# Borrar la BD réplica previa para que cada corrida arranque limpia
rm -f "$ROOT_DIR/pc2/trafico_replica.db"

SUF="${ESCENARIO}_${DISENO}"
echo "════════════════════════════════════════════════"
echo "  PC2 — Experimento $ESCENARIO ($DISENO)"
echo "════════════════════════════════════════════════"

python3 "$ROOT_DIR/pc2/base_datos_replica.py"  > "$LOG_DIR/bd_replica_${SUF}.log" 2>&1 &
BR=$!
sleep 0.5
python3 "$ROOT_DIR/pc2/servicio_semaforos.py"  > "$LOG_DIR/semaforos_${SUF}.log" 2>&1 &
SM=$!
sleep 0.5
python3 "$ROOT_DIR/pc2/servicio_analitica.py"  > "$LOG_DIR/analitica_${SUF}.log" 2>&1 &
AN=$!

echo "[bd_replica] PID=$BR"
echo "[semaforos]  PID=$SM"
echo "[analitica]  PID=$AN"
echo ""
echo "✅ PC2 listo. Para detener al final del experimento:"
echo "   pkill -f 'base_datos_replica|servicio_semaforos|servicio_analitica'"
