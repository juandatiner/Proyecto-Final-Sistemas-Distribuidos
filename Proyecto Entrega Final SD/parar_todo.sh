#!/usr/bin/env bash
# =====================================================================
# parar_todo.sh   ←  SE EJECUTA EN TU MAC
# Apaga TODOS los servicios del sistema en las 3 PCs (incluido el
# dashboard). Úsalo antes de re-arrancar o al terminar una sesión.
# =====================================================================
set -u
PC1_USER="estudiante"; PC1_IP="10.43.99.128";  PC1_PASS="0r4nGut4n*24"
PC2_USER="estudiante"; PC2_IP="10.43.100.144"; PC2_PASS="Juan.2005"
PC3_USER="estudiante"; PC3_IP="10.43.100.90";  PC3_PASS="Canguro-21Nu"

if command -v sshpass >/dev/null 2>&1; then SP(){ sshpass -p "$1" "${@:2}"; }
else SP(){ "${@:2}"; }; fi
r1(){ SP "$PC1_PASS" ssh -o StrictHostKeyChecking=no "${PC1_USER}@${PC1_IP}" "$@"; }
r2(){ SP "$PC2_PASS" ssh -o StrictHostKeyChecking=no "${PC2_USER}@${PC2_IP}" "$@"; }
r3(){ SP "$PC3_PASS" ssh -o StrictHostKeyChecking=no "${PC3_USER}@${PC3_IP}" "$@"; }

echo "Apagando PC1 (broker + sensores)..."
r1 'pkill -9 -f "broker_zmq|sensor_camara|sensor_espira|sensor_gps"' 2>/dev/null || true
echo "Apagando PC2 (analítica + semáforos + réplica)..."
r2 'pkill -9 -f "servicio_analitica|servicio_semaforos|base_datos_replica"' 2>/dev/null || true
echo "Apagando PC3 (BD principal + monitoreo + dashboard)..."
r3 'pkill -9 -f "base_datos_principal|servicio_monitoreo|dashboard/server.py"' 2>/dev/null || true
echo "✅ Todo apagado."
