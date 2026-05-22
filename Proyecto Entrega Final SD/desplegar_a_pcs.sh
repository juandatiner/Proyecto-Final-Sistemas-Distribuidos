#!/usr/bin/env bash
# =====================================================================
# desplegar_a_pcs.sh   ←  SE EJECUTA EN TU MAC
# =====================================================================
# Copia el ZIP del proyecto a las 3 PCs, lo descomprime y deja todo
# listo (instala dependencias). Después de esto, cada PC tiene
# ~/trafico_urbano completo.
#
# REQUISITOS:
#   - El archivo Entrega2_TraficoUrbano.zip en la misma carpeta que
#     este script (tu carpeta Downloads/trafico_urbano).
#   - (Opcional) sshpass para no escribir las claves a mano:
#         macOS:  brew install hudochenkov/sshpass/sshpass
#
# USO:
#   1. Ajusta los USUARIOS de cada PC abajo (PC1_USER, etc.).
#   2. cd ~/Downloads/trafico_urbano
#   3. bash desplegar_a_pcs.sh
# =====================================================================
set -u

# ─────────────────────────────────────────────────────────────────
# CONFIG — AJUSTA LOS USUARIOS (las claves ya están)
# ─────────────────────────────────────────────────────────────────
PC1_USER="estudiante";  PC1_IP="10.43.99.128";   PC1_PASS="0r4nGut4n*24"
PC2_USER="estudiante";  PC2_IP="10.43.100.144";  PC2_PASS="Juan.2005"
PC3_USER="estudiante";  PC3_IP="10.43.100.90";   PC3_PASS="Canguro-21Nu"

ZIP="Entrega2_TraficoUrbano.zip"
REMOTE_HOME="~"          # se descomprime en el home de cada PC → ~/trafico_urbano

# ─────────────────────────────────────────────────────────────────
if [[ ! -f "$ZIP" ]]; then
  echo "ERROR: no encuentro $ZIP en $(pwd)."
  echo "Corre este script desde la carpeta donde está el ZIP."
  exit 1
fi

# Detectar sshpass
if command -v sshpass >/dev/null 2>&1; then
  USE_SSHPASS=1
  echo "→ sshpass detectado: no tendrás que escribir las claves."
else
  USE_SSHPASS=0
  echo "→ sshpass no instalado: te pedirá la clave de cada PC (varias veces)."
fi

desplegar() {
  local USER="$1" IP="$2" PASS="$3" NOMBRE="$4"
  echo ""
  echo "════════════════════════════════════════════════"
  echo "  Desplegando a $NOMBRE ($USER@$IP)"
  echo "════════════════════════════════════════════════"

  if [[ "$USE_SSHPASS" == "1" ]]; then
    sshpass -p "$PASS" scp -o StrictHostKeyChecking=no "$ZIP" "${USER}@${IP}:${REMOTE_HOME}/" \
      && echo "  ✓ ZIP copiado"
    sshpass -p "$PASS" ssh -o StrictHostKeyChecking=no "${USER}@${IP}" \
      "cd ${REMOTE_HOME} && rm -rf trafico_urbano && unzip -o ${ZIP} >/dev/null && cd trafico_urbano && chmod +x *.sh experimentos/*.sh 2>/dev/null && python3 -m pip install --quiet pyzmq flask flask-socketio 2>/dev/null; echo '  ✓ descomprimido y dependencias instaladas en' \$(hostname)"
  else
    scp -o StrictHostKeyChecking=no "$ZIP" "${USER}@${IP}:${REMOTE_HOME}/" \
      && echo "  ✓ ZIP copiado"
    ssh -o StrictHostKeyChecking=no "${USER}@${IP}" \
      "cd ${REMOTE_HOME} && rm -rf trafico_urbano && unzip -o ${ZIP} >/dev/null && cd trafico_urbano && chmod +x *.sh experimentos/*.sh 2>/dev/null && python3 -m pip install --quiet pyzmq flask flask-socketio 2>/dev/null; echo '  ✓ descomprimido y dependencias instaladas en' \$(hostname)"
  fi
}

desplegar "$PC1_USER" "$PC1_IP" "$PC1_PASS" "PC1"
desplegar "$PC2_USER" "$PC2_IP" "$PC2_PASS" "PC2"
desplegar "$PC3_USER" "$PC3_IP" "$PC3_PASS" "PC3"

echo ""
echo "════════════════════════════════════════════════"
echo "  ✅ Despliegue completo en las 3 PCs."
echo "  Cada una tiene ~/trafico_urbano listo."
echo "════════════════════════════════════════════════"
echo "  Siguiente paso (smoke test), en orden PC3 → PC2 → PC1:"
echo "    ssh ${PC3_USER}@${PC3_IP}  'cd ~/trafico_urbano && bash lanzar_pc3.sh'"
echo "    ssh ${PC2_USER}@${PC2_IP}  'cd ~/trafico_urbano && bash lanzar_pc2.sh'"
echo "    ssh ${PC1_USER}@${PC1_IP}  'cd ~/trafico_urbano && bash lanzar_pc1.sh'"
