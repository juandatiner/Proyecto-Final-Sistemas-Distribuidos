#!/usr/bin/env bash
# ============================================================
# lanzar_dashboard.sh  (corre en PC3)
# Inicia el dashboard web del sistema.  Asume que la BD principal
# y los demás servicios ya están corriendo (PC1, PC2, PC3).
#
# Uso:
#   bash lanzar_dashboard.sh
#   abrir en el navegador:   http://10.43.100.90:8080
# ============================================================
set -e
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

# Instalar dependencias del dashboard si faltan
python3 -c "import flask, flask_socketio" 2>/dev/null || \
  pip install --quiet flask flask-socketio

mkdir -p "$SCRIPT_DIR/logs"
echo "═════════════════════════════════════════════════════"
echo "  Dashboard en vivo — Tráfico Urbano"
echo "  http://0.0.0.0:8080  (acceder desde el navegador)"
echo "═════════════════════════════════════════════════════"
python3 "$SCRIPT_DIR/pc3/dashboard/server.py"
