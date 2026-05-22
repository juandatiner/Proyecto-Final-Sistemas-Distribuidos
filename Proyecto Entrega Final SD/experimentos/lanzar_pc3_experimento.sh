#!/usr/bin/env bash
# =============================================================
# experimentos/lanzar_pc3_experimento.sh
# Lanza la BD principal en PC3 y, en paralelo, ejecuta el script
# de medición que captura V1 y V2 durante 2 minutos. Al terminar
# la medición, deja los servicios corriendo (para que puedas
# pasar al siguiente experimento sin reiniciar todo) o los detiene
# si pasas --stop.
#
# Uso:
#   bash lanzar_pc3_experimento.sh --escenario A --diseno simple
#   bash lanzar_pc3_experimento.sh --escenario B --diseno multihilo --stop
# =============================================================
set -e

ESCENARIO=""
DISENO="simple"
STOP=0
DURACION=120
while [[ $# -gt 0 ]]; do
  case $1 in
    --escenario) ESCENARIO="$2"; shift 2 ;;
    --diseno)    DISENO="$2";    shift 2 ;;
    --duracion)  DURACION="$2";  shift 2 ;;
    --stop)      STOP=1;         shift   ;;
    *) echo "Argumento desconocido: $1"; exit 1 ;;
  esac
done

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
LOG_DIR="$ROOT_DIR/logs"
mkdir -p "$LOG_DIR"

# Limpiar procesos previos
pkill -f "base_datos_principal.py" 2>/dev/null || true
sleep 1

# Borrar BD principal para que cada corrida arranque desde 0
rm -f "$ROOT_DIR/pc3/trafico_principal.db"

SUF="${ESCENARIO}_${DISENO}"
echo "════════════════════════════════════════════════"
echo "  PC3 — Experimento $ESCENARIO ($DISENO)"
echo "════════════════════════════════════════════════"

# Lanzar BD principal en background
python3 "$ROOT_DIR/pc3/base_datos_principal.py" \
  > "$LOG_DIR/bd_principal_${SUF}.log" 2>&1 &
BD_PID=$!
echo "[bd_principal] PID=$BD_PID"

# Esperar que los servicios de las 3 PCs estén levantados antes de medir
echo "Esperando 8s para sincronizar las 3 PCs..."
sleep 8

# Ejecutar la medición (bloquea ~DURACION segundos)
python3 "$SCRIPT_DIR/medir_experimento.py" \
  --escenario "$ESCENARIO" --diseno "$DISENO" --duracion "$DURACION"

# Si se pidió --stop, parar la BD principal
if [[ "$STOP" == "1" ]]; then
  echo "Deteniendo BD principal (--stop)..."
  kill $BD_PID 2>/dev/null || true
fi

echo "✅ Experimento $SUF completado. Resultados en experimentos/resultados/"
