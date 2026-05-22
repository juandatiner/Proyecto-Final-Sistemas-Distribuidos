#!/usr/bin/env bash
# =============================================================
# lanzar_pc1.sh
# Inicia todos los procesos del PC1:
#   - Broker ZeroMQ (simple o multihilo)
#   - Sensores: una cámara, espira y GPS por cada intersección
#               definida en la cuadrícula.
#
# Uso:
#   bash lanzar_pc1.sh                    # broker simple
#   bash lanzar_pc1.sh --multihilo        # broker multihilo (experimento)
#   bash lanzar_pc1.sh --intersecciones A1,B3,C5   # sensores específicos
# =============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
MODO_BROKER="simple"
# Intersecciones a cubrir (default: cuadrícula completa 5x5)
INTERSECCIONES=("A1" "A2" "A3" "A4" "A5"
                "B1" "B2" "B3" "B4" "B5"
                "C1" "C2" "C3" "C4" "C5"
                "D1" "D2" "D3" "D4" "D5"
                "E1" "E2" "E3" "E4" "E5")

# ── Parsear argumentos ────────────────────────────────────────
for arg in "$@"; do
  case $arg in
    --multihilo)
      MODO_BROKER="multihilo"
      shift
      ;;
    --intersecciones=*)
      IFS=',' read -ra INTERSECCIONES <<< "${arg#*=}"
      shift
      ;;
  esac
done

echo "════════════════════════════════════════════════"
echo "  PC1 — Iniciando servicios"
echo "  Broker modo: $MODO_BROKER"
echo "  Intersecciones: ${#INTERSECCIONES[@]}"
echo "════════════════════════════════════════════════"

# Directorio de logs
mkdir -p "$SCRIPT_DIR/logs"

# ── 1. Iniciar Broker ─────────────────────────────────────────
echo "[1/2] Iniciando Broker ZeroMQ ($MODO_BROKER)..."
python3 "$SCRIPT_DIR/pc1/broker_zmq.py" --modo "$MODO_BROKER" \
  > "$SCRIPT_DIR/logs/broker.log" 2>&1 &
BROKER_PID=$!
echo "  Broker PID: $BROKER_PID"
sleep 1   # dar tiempo al broker para hacer bind

# ── 2. Iniciar sensores ───────────────────────────────────────
echo "[2/2] Iniciando sensores para ${#INTERSECCIONES[@]} intersecciones..."

for INT_ID in "${INTERSECCIONES[@]}"; do
  INTER="INT-${INT_ID}"

  python3 "$SCRIPT_DIR/pc1/sensor_camara.py"  --interseccion "$INTER" \
    > "$SCRIPT_DIR/logs/cam_${INT_ID}.log"  2>&1 &
  CAM_PID=$!

  python3 "$SCRIPT_DIR/pc1/sensor_espira.py"  --interseccion "$INTER" \
    > "$SCRIPT_DIR/logs/esp_${INT_ID}.log"  2>&1 &
  ESP_PID=$!

  python3 "$SCRIPT_DIR/pc1/sensor_gps.py"    --interseccion "$INTER" \
    > "$SCRIPT_DIR/logs/gps_${INT_ID}.log"  2>&1 &
  GPS_PID=$!

  echo "  $INTER → CAM=$CAM_PID ESP=$ESP_PID GPS=$GPS_PID"
done

echo ""
echo "✅ PC1 iniciado. Logs en: $SCRIPT_DIR/logs/"
echo "   Para detener: kill \$(pgrep -f sensor_) \$(pgrep -f broker_zmq)"
echo ""

# Mantener el script activo para poder hacer Ctrl+C
wait $BROKER_PID
