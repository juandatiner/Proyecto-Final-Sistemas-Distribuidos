#!/usr/bin/env bash
# =============================================================
# lanzar_pc3.sh
# Inicia todos los servicios del PC3:
#   - Base de Datos Principal (SQLite, PULL + heartbeat PUB)
#   - Servicio de Monitoreo y Consulta (CLI interactiva)
#
# IMPORTANTE: ejecutar ANTES que PC1 y PC2 para que el heartbeat
#             esté disponible cuando la analítica se conecte.
# Uso:
#   bash lanzar_pc3.sh
# =============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$SCRIPT_DIR/logs"

echo "════════════════════════════════════════════════"
echo "  PC3 — Iniciando servicios"
echo "════════════════════════════════════════════════"

# ── 1. Base de Datos Principal ────────────────────────────────
echo "[1/2] Iniciando BD Principal + Heartbeat..."
python3 "$SCRIPT_DIR/pc3/base_datos_principal.py" \
  > "$SCRIPT_DIR/logs/bd_principal.log" 2>&1 &
BD_PID=$!
echo "  BD Principal PID: $BD_PID"
sleep 1

# ── 2. Servicio de Monitoreo (interactivo, primer plano) ─────
echo "[2/2] Iniciando Servicio de Monitoreo..."
echo "       (Este servicio es interactivo — responderá en pantalla)"
echo ""
python3 "$SCRIPT_DIR/pc3/servicio_monitoreo.py"

# Si el monitoreo termina, detener BD principal también
kill $BD_PID 2>/dev/null || true
echo "PC3 detenido."
