#!/usr/bin/env bash
# =============================================================
# experimentos/lanzar_pc1_experimento.sh
# Lanza el broker y los sensores de PC1 para una corrida del
# experimento de la Tabla 1.
#
# Uso:
#   bash lanzar_pc1_experimento.sh --escenario A --diseno simple
#   bash lanzar_pc1_experimento.sh --escenario A --diseno multihilo
#   bash lanzar_pc1_experimento.sh --escenario B --diseno simple
#   bash lanzar_pc1_experimento.sh --escenario B --diseno multihilo
#
# Escenarios:
#   A : 1 sensor de cada tipo (cám, esp, gps) generando datos cada 10s
#   B : 2 sensores de cada tipo (cám, esp, gps) generando datos cada 5s
#
# Tras lanzar, deja los procesos corriendo en background.
# Para detenerlos al final del experimento:
#   pkill -f "broker_zmq.py|sensor_camara.py|sensor_espira.py|sensor_gps.py"
# =============================================================
set -e

ESCENARIO=""
DISENO="simple"

# ── Parsear argumentos ───────────────────────────────────────
while [[ $# -gt 0 ]]; do
  case $1 in
    --escenario) ESCENARIO="$2"; shift 2 ;;
    --diseno)    DISENO="$2";    shift 2 ;;
    *) echo "Argumento desconocido: $1"; exit 1 ;;
  esac
done

if [[ "$ESCENARIO" != "A" && "$ESCENARIO" != "B" ]]; then
  echo "ERROR: --escenario debe ser A o B"
  exit 1
fi
if [[ "$DISENO" != "simple" && "$DISENO" != "multihilo" ]]; then
  echo "ERROR: --diseno debe ser simple o multihilo"
  exit 1
fi

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# Configuración por escenario (intersecciones e intervalo)
if [[ "$ESCENARIO" == "A" ]]; then
  # 1 sensor de cada tipo, cada 10 segundos.
  INTERSECCIONES=("C5")
  INTERVALO=10
else
  # 2 sensores de cada tipo, cada 5 segundos.
  INTERSECCIONES=("C5" "B3")
  INTERVALO=5
fi

# Limpiar procesos previos del experimento (no afecta otros experimentos)
pkill -f "broker_zmq.py" 2>/dev/null || true
pkill -f "sensor_camara.py" 2>/dev/null || true
pkill -f "sensor_espira.py" 2>/dev/null || true
pkill -f "sensor_gps.py" 2>/dev/null || true
sleep 1

echo "════════════════════════════════════════════════"
echo "  PC1 — Experimento $ESCENARIO ($DISENO)"
echo "  ${#INTERSECCIONES[@]} intersecciones, intervalo ${INTERVALO}s"
echo "════════════════════════════════════════════════"

# ── 1. Broker ────────────────────────────────────────────────
LOG_BROKER="$LOG_DIR/broker_${ESCENARIO}_${DISENO}.log"
python3 "$ROOT_DIR/pc1/broker_zmq.py" --modo "$DISENO" \
  > "$LOG_BROKER" 2>&1 &
BROKER_PID=$!
echo "[broker] PID=$BROKER_PID  log=$LOG_BROKER"
sleep 1

# ── 2. Sensores ──────────────────────────────────────────────
for INT_ID in "${INTERSECCIONES[@]}"; do
  INTER="INT-${INT_ID}"

  python3 "$ROOT_DIR/pc1/sensor_camara.py" \
    --interseccion "$INTER" --intervalo "$INTERVALO" \
    > "$LOG_DIR/cam_${INT_ID}_${ESCENARIO}_${DISENO}.log" 2>&1 &
  CPID=$!

  python3 "$ROOT_DIR/pc1/sensor_espira.py" \
    --interseccion "$INTER" --intervalo "$INTERVALO" \
    > "$LOG_DIR/esp_${INT_ID}_${ESCENARIO}_${DISENO}.log" 2>&1 &
  EPID=$!

  python3 "$ROOT_DIR/pc1/sensor_gps.py" \
    --interseccion "$INTER" --intervalo "$INTERVALO" \
    > "$LOG_DIR/gps_${INT_ID}_${ESCENARIO}_${DISENO}.log" 2>&1 &
  GPID=$!

  echo "[$INTER] CAM=$CPID  ESP=$EPID  GPS=$GPID"
done

echo ""
echo "✅ PC1 listo. Esperar ~5s antes de medir."
echo "   Para detener al final: pkill -f 'broker_zmq.py|sensor_'"
