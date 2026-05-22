#!/usr/bin/env bash
# =====================================================================
# experimentos/orquestar_todo.sh   <-  SE EJECUTA EN PC3
# Corre las 4 corridas del experimento de la Tabla 1 de forma automatica.
# PC3 se conecta por SSH a PC1 y PC2, lanza los servicios, mide V1 y V2,
# y al final recoge las specs de hardware de las 3 maquinas.
# Usa el patron ( ... & ); exit 0  para que el SSH no se cuelgue.
# =====================================================================
set -u

PC1_USER="estudiante"; PC1_IP="10.43.99.128";  PC1_PASS="0r4nGut4n*24"
PC2_USER="estudiante"; PC2_IP="10.43.100.144"; PC2_PASS="Juan.2005"
REMOTE_DIR="~/trafico_urbano"
DURACION=120
WARMUP=8

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(dirname "$SCRIPT_DIR")"
RES_DIR="$SCRIPT_DIR/resultados"
mkdir -p "$RES_DIR" "$ROOT_DIR/logs"
LOG="$RES_DIR/orquestador.log"; : > "$LOG"
log(){ echo "$(date '+%H:%M:%S') | $*" | tee -a "$LOG"; }

SSHOPTS="-o StrictHostKeyChecking=no -o ConnectTimeout=10 -n"
if command -v sshpass >/dev/null 2>&1; then SP(){ sshpass -p "$1" "${@:2}"; }
else SP(){ "${@:2}"; }; fi
run_pc1(){ SP "$PC1_PASS" ssh $SSHOPTS "${PC1_USER}@${PC1_IP}" "$@"; }
run_pc2(){ SP "$PC2_PASS" ssh $SSHOPTS "${PC2_USER}@${PC2_IP}" "$@"; }

log "Verificando conexion SSH a PC1 y PC2..."
if ! run_pc1 "echo ok" >/dev/null 2>&1; then log "ERROR: no conecta a PC1"; exit 1; fi
if ! run_pc2 "echo ok" >/dev/null 2>&1; then log "ERROR: no conecta a PC2"; exit 1; fi
log "Conexion OK."

matar_todo(){
  pkill -9 -f "base_datos_principal" 2>/dev/null
  run_pc1 'pkill -9 -f "broker_zmq|sensor_camara|sensor_espira|sensor_gps" 2>/dev/null; exit 0'
  run_pc2 'pkill -9 -f "base_datos_replica|servicio_semaforos|servicio_analitica" 2>/dev/null; exit 0'
  sleep 2
}

corrida(){
  local ESC="$1" DIS="$2"
  log "════════ CORRIDA ${ESC}_${DIS} ════════"
  matar_todo

  # PC3 local: BD principal (limpia)
  rm -f "$ROOT_DIR/pc3/trafico_principal.db" 2>/dev/null
  ( cd "$ROOT_DIR" && setsid python3 pc3/base_datos_principal.py >"$ROOT_DIR/logs/bd_principal_${ESC}_${DIS}.log" 2>&1 </dev/null & )
  log "[PC3] BD principal lanzada."
  sleep 2

  # PC2 remoto: replica + semaforos + analitica (cada uno robusto)
  run_pc2 "cd ${REMOTE_DIR} && rm -f pc2/trafico_replica.db; ( setsid python3 pc2/base_datos_replica.py >logs/bd_replica_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"
  sleep 1
  run_pc2 "cd ${REMOTE_DIR} && ( setsid python3 pc2/servicio_semaforos.py >logs/semaforos_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"
  sleep 1
  run_pc2 "cd ${REMOTE_DIR} && ( setsid python3 pc2/servicio_analitica.py >logs/analitica_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"
  log "[PC2] replica + semaforos + analitica lanzados."
  sleep 3

  # PC1 remoto: broker + sensores del escenario
  run_pc1 "cd ${REMOTE_DIR} && ( setsid bash experimentos/lanzar_pc1_experimento.sh --escenario ${ESC} --diseno ${DIS} >logs/pc1_exp_${ESC}_${DIS}.log 2>&1 </dev/null & ); exit 0"
  log "[PC1] broker (${DIS}) + sensores (esc ${ESC}) lanzados."

  log "Warmup ${WARMUP}s..."; sleep "$WARMUP"
  log "Midiendo V1 (${DURACION}s) y V2..."
  ( cd "$ROOT_DIR" && python3 experimentos/medir_experimento.py --escenario "$ESC" --diseno "$DIS" --duracion "$DURACION" ) | tee -a "$LOG"

  matar_todo
  log "Corrida ${ESC}_${DIS} terminada."
}

specs_hw(){
  log "Recolectando specs de hardware..."
  { echo "=== HW PC3 ($(hostname)) ==="; lscpu | grep -E "Model name|Socket|Core|Thread|^CPU\(s\)"; free -h; uname -a; python3 --version; } > "$RES_DIR/specs_hw_pc3.txt" 2>&1
  run_pc1 "echo '=== HW PC1 ('\$(hostname)') ==='; lscpu | grep -E 'Model name|Socket|Core|Thread|^CPU\(s\)'; free -h; uname -a; python3 --version" > "$RES_DIR/specs_hw_pc1.txt" 2>&1
  run_pc2 "echo '=== HW PC2 ('\$(hostname)') ==='; lscpu | grep -E 'Model name|Socket|Core|Thread|^CPU\(s\)'; free -h; uname -a; python3 --version" > "$RES_DIR/specs_hw_pc2.txt" 2>&1
  log "Specs guardadas."
}

log "Iniciando bateria de 4 corridas (~12 min)..."
corrida A simple
corrida A multihilo
corrida B simple
corrida B multihilo
specs_hw
log "════════ TODAS LAS CORRIDAS COMPLETADAS ════════"
ls -la "$RES_DIR"/*.csv "$RES_DIR"/specs_hw_*.txt 2>/dev/null | tee -a "$LOG"
log "Empaqueta:  tar czf resultados_experimentos.tar.gz experimentos/resultados/"
