"""
pc3/dashboard/server.py
=====================================================================
Dashboard en vivo del sistema de Tráfico Urbano (Entrega 2).

Levanta un servidor Flask + Flask-SocketIO en PC3 que:
  1. Se SUBSCRIBE al broker ZMQ (PC1) y reenvía cada evento al
     navegador en tiempo real vía WebSocket.
  2. Lee la BD principal (local) y la BD réplica (vía consulta REP
     a la analítica) para mostrar contadores en vivo.
  3. Suscribe al heartbeat de la BD principal para mostrar el estado
     PC3 = ACTIVO / CAÍDO en la pantalla.
  4. Expone endpoints REST para que el frontend dispare comandos
     manuales (ambulancia, forzar cambio de semáforo) — los reenvía
     a la analítica vía REQ/REP.

Uso:
    pip install flask flask-socketio eventlet
    python pc3/dashboard/server.py
    # y abrir http://<PC3_IP>:8080

Estructura de eventos emitidos al frontend:
  ws "evento_sensor"      -> {sensor_id, interseccion, topic, Q, Vp, Cv, ts}
  ws "estado_sistema"     -> {intersecciones: {INT-XX: {estado, Q, Vp, Cv}}}
  ws "heartbeat"          -> {pc3_activo: bool, last_beat: iso}
  ws "metricas"           -> {n_principal, n_replica, congestiones, prioridades}
  ws "evento_semaforo"    -> {interseccion, estado, modo, ts}
=====================================================================
"""
import json
import os
import sqlite3
import sys
import threading
import time
from datetime import datetime, timezone

import zmq
from flask import Flask, jsonify, render_template, request
from flask_socketio import SocketIO

# ── Imports del proyecto ─────────────────────────────────────────
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.dirname(
    os.path.abspath(__file__)))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

logger = configurar_logger("Dashboard")

# ─────────────────────────────────────────────────────────────────
# Inicialización Flask + SocketIO
# ─────────────────────────────────────────────────────────────────

app = Flask(__name__,
            template_folder=os.path.join(os.path.dirname(__file__), "templates"),
            static_folder=os.path.join(os.path.dirname(__file__), "static"))
app.config["SECRET_KEY"] = "trafico-urbano-isd-2026"
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

CONFIG = cargar_config()
PC1_IP = CONFIG["red"]["PC1_IP"]
PC2_IP = CONFIG["red"]["PC2_IP"]
PC3_IP = CONFIG["red"]["PC3_IP"]
PUERTOS = CONFIG["red"]["puertos"]

DB_PRINCIPAL = os.path.join(
    os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
    "trafico_principal.db"
)

# Estado en memoria
estado_global = {
    "intersecciones": {},          # {INT-XX: {estado, Q, Vp, Cv, semaforo, last_update}}
    "pc3_activo": True,
    "ultimo_heartbeat": None,
    "n_eventos_principal": 0,
    "n_eventos_replica": 0,
    "congestiones": 0,
    "prioridades": 0,
    "ambulancias_activas": [],     # vías actualmente con prioridad
}
lock = threading.Lock()

# Construir el listado de intersecciones a partir del config
INTERSECCIONES = []
for fila in CONFIG["ciudad"]["filas"]:
    for col in CONFIG["ciudad"]["columnas"]:
        INTERSECCIONES.append(f"INT-{fila}{col}")
for inter in INTERSECCIONES:
    estado_global["intersecciones"][inter] = {
        "estado": "DESCONOCIDO", "Q": 0, "Vp": 0, "Cv": 0,
        "semaforo": "ROJO", "last_update": None,
    }


# ─────────────────────────────────────────────────────────────────
# Hilo 1: SUBscriber al broker ZMQ (PC1) – eventos de sensores
# ─────────────────────────────────────────────────────────────────

def hilo_sub_broker():
    """Recibe los eventos de sensores en bruto y los emite al frontend."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PC1_IP}:{PUERTOS['broker_pub']}")
    for tpc in (b"camara", b"espira_inductiva", b"gps"):
        sub.setsockopt(zmq.SUBSCRIBE, tpc)
    logger.info(f"[broker-sub] conectado a {PC1_IP}:{PUERTOS['broker_pub']}")

    while True:
        try:
            partes = sub.recv_multipart()
            if len(partes) < 2:
                continue
            topic = partes[0].decode("utf-8")
            payload = json.loads(partes[1].decode("utf-8"))
            evento = {
                "topic": topic,
                "sensor_id": payload.get("sensor_id", ""),
                "interseccion": payload.get("interseccion", ""),
                "ts": payload.get("timestamp",
                                  payload.get("timestamp_inicio", "")),
                "Q":  payload.get("volumen", 0),
                "Vp": payload.get("velocidad_promedio", 0),
                "Cv": payload.get("vehiculos_contados", 0),
                "nivel_gps": payload.get("nivel_congestion", ""),
            }
            socketio.emit("evento_sensor", evento)
        except Exception as exc:
            logger.error(f"[broker-sub] {exc}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────
# Hilo 2: SUBscriber al heartbeat de la BD principal (PC3)
# ─────────────────────────────────────────────────────────────────

def hilo_sub_heartbeat():
    """Vigila el heartbeat de PC3 para mostrar verde/rojo en el panel."""
    ctx = zmq.Context.instance()
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{PC3_IP}:{PUERTOS['heartbeat_pub']}")
    sub.setsockopt(zmq.SUBSCRIBE, b"")
    sub.RCVTIMEO = 5000  # 5s
    logger.info(f"[heartbeat] conectado a {PC3_IP}:{PUERTOS['heartbeat_pub']}")
    timeout_seg = CONFIG.get("heartbeat", {}).get("timeout_seg", 9)

    while True:
        try:
            sub.recv()
            with lock:
                estado_global["pc3_activo"] = True
                estado_global["ultimo_heartbeat"] = datetime.now(
                    timezone.utc).isoformat()
            socketio.emit("heartbeat",
                          {"pc3_activo": True,
                           "last_beat": estado_global["ultimo_heartbeat"]})
        except zmq.error.Again:
            with lock:
                # Si pasó más del timeout, marcar como caído
                if estado_global["ultimo_heartbeat"]:
                    last = datetime.fromisoformat(
                        estado_global["ultimo_heartbeat"]).timestamp()
                    if time.time() - last > timeout_seg:
                        estado_global["pc3_activo"] = False
            socketio.emit("heartbeat",
                          {"pc3_activo": estado_global["pc3_activo"],
                           "last_beat": estado_global["ultimo_heartbeat"]})
        except Exception as exc:
            logger.error(f"[heartbeat] {exc}")
            time.sleep(1)


# ─────────────────────────────────────────────────────────────────
# Hilo 3: Polling al servicio de analítica (PC2) por estado_sistema
# ─────────────────────────────────────────────────────────────────

def hilo_poll_estado():
    """Cada 2s pide a la analítica el estado consolidado del sistema."""
    ctx = zmq.Context.instance()
    while True:
        try:
            sock = ctx.socket(zmq.REQ)
            sock.RCVTIMEO = 2000
            sock.SNDTIMEO = 2000
            sock.connect(f"tcp://{PC2_IP}:{PUERTOS['analitica_rep']}")
            sock.send(json.dumps({"tipo": "ESTADO_SISTEMA"}).encode("utf-8"))
            resp = json.loads(sock.recv().decode("utf-8"))
            sock.close(0)

            if resp.get("ok") and "data" in resp:
                with lock:
                    for inter, info in resp["data"].items():
                        if inter in estado_global["intersecciones"]:
                            estado_global["intersecciones"][inter].update({
                                "estado": info.get("estado_actual",
                                                   "DESCONOCIDO"),
                                "Q":  info.get("Q", 0),
                                "Vp": info.get("Vp", 0),
                                "Cv": info.get("Cv", 0),
                                "last_update": info.get("ts", None),
                            })
                    estado_global["pc3_activo_segun_analitica"] = \
                        resp.get("pc3_activo", True)
                socketio.emit("estado_sistema",
                              estado_global["intersecciones"])
        except Exception as exc:
            logger.warning(f"[poll-estado] {exc}")
        time.sleep(2)


# ─────────────────────────────────────────────────────────────────
# Hilo 4: Polling de métricas a las dos BDs
# ─────────────────────────────────────────────────────────────────

def _contar_bd(ruta: str) -> dict:
    if not os.path.exists(ruta):
        return {"eventos": 0, "congestiones": 0, "prioridades": 0}
    try:
        c = sqlite3.connect(ruta, timeout=2)
        cur = c.cursor()
        n_ev = cur.execute("SELECT COUNT(*) FROM eventos_sensores").fetchone()[0]
        n_cg = cur.execute("SELECT COUNT(*) FROM congestion_log").fetchone()[0]
        n_pr = cur.execute("SELECT COUNT(*) FROM prioridades_log").fetchone()[0]
        c.close()
        return {"eventos": n_ev, "congestiones": n_cg, "prioridades": n_pr}
    except sqlite3.Error:
        return {"eventos": 0, "congestiones": 0, "prioridades": 0}


def hilo_metricas():
    """Cada 3s actualiza los contadores de las BDs."""
    ruta_replica = os.path.join(
        os.path.dirname(os.path.dirname(os.path.dirname(
            os.path.abspath(__file__)))),
        "pc2", "trafico_replica.db"
    )
    while True:
        try:
            principal = _contar_bd(DB_PRINCIPAL)
            # La réplica solo es accesible localmente si el dashboard
            # corre en PC2. Como vive en PC3, pedimos los conteos a la
            # analítica vía un comando "METRICAS_REPLICA" (TODO: agregar
            # en analítica). Por ahora consultamos archivo si existe.
            replica = _contar_bd(ruta_replica)

            with lock:
                estado_global["n_eventos_principal"] = principal["eventos"]
                estado_global["n_eventos_replica"]   = replica["eventos"]
                estado_global["congestiones"]        = principal["congestiones"]
                estado_global["prioridades"]         = principal["prioridades"]
            socketio.emit("metricas", {
                "n_principal":   principal["eventos"],
                "n_replica":     replica["eventos"],
                "congestiones":  principal["congestiones"],
                "prioridades":   principal["prioridades"],
            })
        except Exception as exc:
            logger.error(f"[metricas] {exc}")
        time.sleep(3)


# ─────────────────────────────────────────────────────────────────
# REST endpoints – disparan comandos a la analítica
# ─────────────────────────────────────────────────────────────────

def _cmd_a_analitica(comando: dict) -> dict:
    ctx = zmq.Context.instance()
    sock = ctx.socket(zmq.REQ)
    sock.RCVTIMEO = 5000
    sock.SNDTIMEO = 5000
    sock.connect(f"tcp://{PC2_IP}:{PUERTOS['analitica_rep']}")
    try:
        t0 = time.perf_counter()
        sock.send(json.dumps(comando).encode("utf-8"))
        resp = json.loads(sock.recv().decode("utf-8"))
        latencia_ms = (time.perf_counter() - t0) * 1000.0
        resp["_latencia_ms"] = round(latencia_ms, 2)
        return resp
    except zmq.error.Again:
        return {"ok": False, "error": "timeout"}
    finally:
        sock.close(0)


@app.route("/")
def index():
    intersecciones = INTERSECCIONES
    return render_template("index.html",
                           intersecciones=intersecciones,
                           filas=CONFIG["ciudad"]["filas"],
                           columnas=CONFIG["ciudad"]["columnas"],
                           pc1_ip=PC1_IP, pc2_ip=PC2_IP, pc3_ip=PC3_IP)


@app.route("/api/estado")
def api_estado():
    with lock:
        return jsonify({
            "intersecciones": estado_global["intersecciones"],
            "pc3_activo":     estado_global["pc3_activo"],
            "n_principal":    estado_global["n_eventos_principal"],
            "n_replica":      estado_global["n_eventos_replica"],
            "congestiones":   estado_global["congestiones"],
            "prioridades":    estado_global["prioridades"],
        })


@app.route("/api/ambulancia", methods=["POST"])
def api_ambulancia():
    data = request.get_json() or {}
    vias = data.get("vias", [])
    if not isinstance(vias, list) or not vias:
        return jsonify({"ok": False, "error": "vias debe ser lista no vacía"}), 400
    resp = _cmd_a_analitica({"tipo": "AMBULANCIA", "vias": vias})
    socketio.emit("evento_semaforo",
                  {"accion": "AMBULANCIA", "vias": vias,
                   "ts": datetime.now(timezone.utc).isoformat(),
                   "ok": resp.get("ok", False)})
    return jsonify(resp)


@app.route("/api/forzar_semaforo", methods=["POST"])
def api_forzar_semaforo():
    data = request.get_json() or {}
    inter = data.get("interseccion")
    nuevo = data.get("nuevo_estado", "VERDE")
    if not inter:
        return jsonify({"ok": False, "error": "interseccion requerida"}), 400
    resp = _cmd_a_analitica({"tipo": "CAMBIAR_SEMAFORO",
                             "interseccion": inter,
                             "nuevo_estado": nuevo})
    socketio.emit("evento_semaforo",
                  {"accion": "CAMBIO_FORZADO", "interseccion": inter,
                   "estado": nuevo,
                   "ts": datetime.now(timezone.utc).isoformat(),
                   "ok": resp.get("ok", False)})
    return jsonify(resp)


@app.route("/api/ping_analitica")
def api_ping_analitica():
    return jsonify(_cmd_a_analitica({"tipo": "HEARTBEAT"}))


# ─────────────────────────────────────────────────────────────────
# Arranque
# ─────────────────────────────────────────────────────────────────

def lanzar_hilos():
    for fn in (hilo_sub_broker, hilo_sub_heartbeat,
               hilo_poll_estado, hilo_metricas):
        t = threading.Thread(target=fn, daemon=True, name=fn.__name__)
        t.start()


if __name__ == "__main__":
    logger.info("════════════════════════════════════════════════════════")
    logger.info(f"  Dashboard de Tráfico Urbano")
    logger.info(f"  PC1 (broker)    : {PC1_IP}:{PUERTOS['broker_pub']}")
    logger.info(f"  PC2 (analítica) : {PC2_IP}:{PUERTOS['analitica_rep']}")
    logger.info(f"  PC3 (heartbeat) : {PC3_IP}:{PUERTOS['heartbeat_pub']}")
    logger.info(f"  Servidor web    : http://0.0.0.0:8080")
    logger.info("════════════════════════════════════════════════════════")
    lanzar_hilos()
    socketio.run(app, host="0.0.0.0", port=8080,
                 debug=False, allow_unsafe_werkzeug=True)
