#!/usr/bin/env bash
# =====================================================================
# arrancar_todo.sh   <-  SE EJECUTA EN TU MAC
# Limpia las 3 PCs y lanza el sistema completo + dashboard, en orden.
# Usa el patron ( ... & ) para que el SSH suelte cada proceso y no se
# quede colgado.
# =====================================================================
set -u

PC1_USER="estudiante"; PC1_IP="10.43.99.128";  PC1_PASS="0r4nGut4n*24"
PC2_USER="estudiante"; PC2_IP="10.43.100.144"; PC2_PASS="Juan.2005"
PC3_USER="estudiante"; PC3_IP="10.43.100.90";  PC3_PASS="Canguro-21Nu"
RD="~/trafico_urbano"

SSHOPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -n"
if command -v sshpass >/dev/null 2>&1; then SP(){ sshpass -p "$1" "${@:2}"; }
else SP(){ "${@:2}"; }; fi
r1(){ SP "$PC1_PASS" ssh $SSHOPTS "${PC1_USER}@${PC1_IP}" "$@"; }
r2(){ SP "$PC2_PASS" ssh $SSHOPTS "${PC2_USER}@${PC2_IP}" "$@"; }
r3(){ SP "$PC3_PASS" ssh $SSHOPTS "${PC3_USER}@${PC3_IP}" "$@"; }

echo "════════════════════════════════════════════════"
echo "  [1/5] Limpiando procesos viejos en las 3 PCs"
echo "════════════════════════════════════════════════"
r1 'pkill -9 -f "broker_zmq|sensor_camara|sensor_espira|sensor_gps" 2>/dev/null; exit 0'
r2 'pkill -9 -f "servicio_analitica|servicio_semaforos|base_datos_replica" 2>/dev/null; exit 0'
r3 'pkill -9 -f "base_datos_principal|servicio_monitoreo|dashboard/server.py" 2>/dev/null; exit 0'
sleep 3

echo "  [2/5] PC3 -> BD principal"
r3 "cd ${RD} && mkdir -p logs && ( setsid python3 pc3/base_datos_principal.py >logs/bd_principal.log 2>&1 </dev/null & ); exit 0"
sleep 3

echo "  [3/5] PC2 -> replica + semaforos + analitica"
r2 "cd ${RD} && mkdir -p logs && ( setsid python3 pc2/base_datos_replica.py >logs/bd_replica.log 2>&1 </dev/null & ); exit 0"
sleep 1
r2 "cd ${RD} && ( setsid python3 pc2/servicio_semaforos.py >logs/semaforos.log 2>&1 </dev/null & ); exit 0"
sleep 1
r2 "cd ${RD} && ( setsid python3 pc2/servicio_analitica.py >logs/analitica.log 2>&1 </dev/null & ); exit 0"
sleep 3

echo "  [4/5] PC1 -> broker + 75 sensores"
r1 "cd ${RD} && mkdir -p logs && ( setsid bash lanzar_pc1.sh >logs/pc1_arranque.log 2>&1 </dev/null & ); exit 0"
sleep 4

echo "  [5/5] PC3 -> dashboard web"
r3 "cd ${RD} && ( setsid python3 pc3/dashboard/server.py >logs/dashboard.log 2>&1 </dev/null & ); exit 0"
sleep 2

echo ""
echo "════════════════════════════════════════════════"
echo "  ✅ Sistema completo arriba en las 3 PCs + dashboard"
echo "════════════════════════════════════════════════"
echo "  Ver el dashboard (terminal nueva, dejala abierta):"
echo "     ssh -L 8080:localhost:8080 ${PC3_USER}@${PC3_IP}"
echo "     navegador:  http://localhost:8080"
echo ""
echo "  Ver logs:"
echo "     ssh estudiante@${PC2_IP} 'tail -f ~/trafico_urbano/logs/analitica.log'"
echo ""
echo "  Apagar todo:  bash parar_todo.sh"
