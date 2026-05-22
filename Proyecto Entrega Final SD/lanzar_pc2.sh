#!/usr/bin/env bash
# =============================================================
# lanzar_pc2.sh
# Inicia todos los servicios del PC2:
#   - Base de Datos Réplica (SQLite, PULL)
#   - Servicio de Control de Semáforos (PULL)
#   - Servicio de Analítica (SUB/PUSH/REP)
#
# IMPORTANTE: ejecutar DESPUÉS de que PC1 esté corriendo.
# Uso:
#   bash lanzar_pc2.sh
# =============================================================
set -e

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
mkdir -p "$SCRIPT_DIR/logs"

echo "════════════════════════════════════════════════"
echo "  PC2 — Iniciando servicios"
echo "════════════════════════════════════════════════"

# ── 1. Base de Datos Réplica ──────────────────────────────────
echo "[1/3] Iniciando BD Réplica..."
python3 "$SCRIPT_DIR/pc2/base_datos_replica.py" \
  > "$SCRIPT_DIR/logs/bd_replica.log" 2>&1 &
BD_PID=$!
echo "  BD Réplica PID: $BD_PID"
sleep 0.5

# ── 2. Servicio de Semáforos ──────────────────────────────────
echo "[2/3] Iniciando Servicio de Semáforos..."
python3 "$SCRIPT_DIR/pc2/servicio_semaforos.py" \
  > "$SCRIPT_DIR/logs/semaforos.log" 2>&1 &
SEM_PID=$!
echo "  Semáforos PID: $SEM_PID"
sleep 0.5

# ── 3. Servicio de Analítica ──────────────────────────────────
echo "[3/3] Iniciando Servicio de Analítica..."
python3 "$SCRIPT_DIR/pc2/servicio_analitica.py" \
  > "$SCRIPT_DIR/logs/analitica.log" 2>&1 &
ANA_PID=$!
echo "  Analítica PID: $ANA_PID"

echo ""
echo "✅ PC2 iniciado. Logs en: $SCRIPT_DIR/logs/"
echo "   Para monitorear: tail -f $SCRIPT_DIR/logs/analitica.log"
echo ""

wait $ANA_PID
