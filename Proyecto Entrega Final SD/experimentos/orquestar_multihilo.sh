#!/usr/bin/env bash
# experimentos/orquestar_multihilo.sh  <- SE EJECUTA EN PC3
# Re-corre SOLO las 2 corridas multihilo (A y B) con el broker ya
# corregido. ~5 min. Las corridas simple y las specs ya las tienes.
set -u
PC1_USER="estudiante"; PC1_IP="10.43.99.128";  PC1_PASS="0r4nGut4n*24"
PC2_USER="estudiante"; PC2_IP="10.43.100.144"; PC2_PASS="Juan.2005"
REMOTE_DIR="~/trafico_urbano"; DURACION=120; WARMUP=8
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"; ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RES_DIR="$SCRIPT_DIR/resultados"; mkdir -p "$RES_DIR" "$ROOT_DIR/logs"
LOG="$RES_DIR/orquestador_multihilo.log"; : > "$LOG"
log(){ echo "$(date '+%H:%M:%S') | $*" | tee -a "$LOG"; }
SSHOPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -n"
if command -v sshpass >/dev/null 2>&1; then SP(){ sshpass -p "$1" "${@:2}"; }
else SP(){ "${@:2}"; }; fi
run_pc1(){ SP "$PC1_PASS" ssh $SSHOPTS "${PC1_USER}@${PC1_IP}" "$@"; }
run_pc2(){ SP "$PC2_PASS" ssh $SSHOPTS "${PC2_USER}@${PC2_IP}" "$@"; }
log "Verificando conexion..."; run_pc1 "echo ok">/dev/null 2>&1 || { log "ERROR PC1"; exit 1; }
run_pc2 "echo ok">/dev/null 2>&1 || { log "ERROR PC2"; exit 1; }
matar(){ pkill -9 -f base_datos_principal 2>/dev/null
  run_pc1 'pkill -9 -f "broker_zmq|sensor_camara|sensor_espira|sensor_gps" 2>/dev/null; exit 0'
  run_pc2 'pkill -9 -f "base_datos_replica|servicio_semaforos|servicio_analitica" 2>/dev/null; exit 0'; sleep 2; }
corrida(){ local ESC="$1" DIS="$2"; log "════ ${ESC}_${DIS} ════"; matar
  rm -f "$ROOT_DIR/pc3/trafico_principal.db" 2>/dev/null
  ( cd "$ROOT_DIR" && setsid python3 pc3/base_datos_principal.py >"$ROOT_DIR/logs/bd_principal_${ESC}_${DIS}.log" 2>&1 </dev/null & ); sleep 2
  run_pc2 "cd ${REMOTE_DIR} && rm -f pc2/trafico_replica.db; ( setsid python3 pc2/base_datos_replica.py >logs/bd_replica_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"; sleep 1
  run_pc2 "cd ${REMOTE_DIR} && ( setsid python3 pc2/servicio_semaforos.py >logs/semaforos_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"; sleep 1
  run_pc2 "cd ${REMOTE_DIR} && ( setsid python3 pc2/servicio_analitica.py >logs/analitica_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"; sleep 3
  run_pc1 "cd ${REMOTE_DIR} && ( setsid bash experimentos/lanzar_pc1_experimento.sh --escenario ${ESC} --diseno ${DIS} >logs/pc1_exp_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"
  log "Warmup ${WARMUP}s..."; sleep "$WARMUP"; log "Midiendo..."
  ( cd "$ROOT_DIR" && python3 experimentos/medir_experimento.py --escenario "$ESC" --diseno "$DIS" --duracion "$DURACION" ) | tee -a "$LOG"
  matar; log "${ESC}_${DIS} listo."; }
log "Re-corriendo SOLO multihilo (broker corregido)..."
corrida A multihilo
corrida B multihilo
log "════ MULTIHILO COMPLETADO ════"
cat "$RES_DIR/A_multihilo.csv" "$RES_DIR/B_multihilo.csv" 2>/dev/null | tee -a "$LOG"
