"""
pc3/base_datos_principal.py
=====================================================================
Base de Datos PRINCIPAL - PC3
Recibe datos vía PULL desde la analítica y los persiste en SQLite.
Publica heartbeats periódicos (PUB) para que la analítica detecte
si este nodo sigue activo.

Esquema idéntico a la réplica en PC2 (base_datos_replica.py).
También expone un socket REP para consultas del monitoreo.

Uso:
    python base_datos_principal.py
=====================================================================
"""
import zmq
import json
import sqlite3
import os
import sys
import signal
import threading
import time
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

# Importar funciones de consulta (mismo esquema que réplica)
sys.path.insert(0, os.path.join(os.path.dirname(__file__), '..', 'pc2'))
from base_datos_replica import (
    inicializar_bd, insertar_registro,
    consultar_historico, consultar_estado_puntual,
    consultar_congestion_historica
)

logger = configurar_logger("BD-Principal")

DB_PATH = os.path.join(os.path.dirname(__file__), "trafico_principal.db")


# ─────────────────────────────────────────────────────────────────
# Heartbeat publisher
# ─────────────────────────────────────────────────────────────────

def hilo_heartbeat(ctx: zmq.Context, config: dict,
                   stop_event: threading.Event) -> None:
    """
    Publica heartbeats periódicos al servicio de analítica en PC2.
    La analítica usa estos heartbeats para saber si PC3 está vivo.

    Args:
        ctx        : contexto ZMQ compartido
        config     : configuración del sistema
        stop_event : evento para detener el hilo
    """
    pub = ctx.socket(zmq.PUB)
    pub.bind(f"tcp://*:{config['red']['puertos']['heartbeat_pub']}")

    intervalo = config['heartbeat']['intervalo_seg']
    logger.info(f"Heartbeat PUB en *:{config['red']['puertos']['heartbeat_pub']} "
                f"(cada {intervalo}s)")

    while not stop_event.is_set():
        msg = json.dumps({
            "tipo": "heartbeat",
            "pc": "PC3",
            "ts": datetime.now(timezone.utc).isoformat()
        }).encode('utf-8')
        try:
            pub.send_multipart([b"heartbeat", msg])
            logger.debug("♥ heartbeat enviado")
        except zmq.ZMQError as e:
            if e.errno != zmq.ETERM:
                logger.error(f"Error heartbeat: {e}")
        stop_event.wait(intervalo)

    pub.close()


# ─────────────────────────────────────────────────────────────────
# Handler de consultas REP
# ─────────────────────────────────────────────────────────────────

def procesar_consulta_bd(conn: sqlite3.Connection, consulta: dict) -> dict:
    """
    Atiende consultas SQL al servicio de BD principal.

    Tipos soportados:
      HISTORICO      : historial de una intersección en rango de tiempo
      ESTADO_PUNTUAL : último registro de una intersección
      CONGESTION     : log de congestiones históricas
      ESTADISTICAS   : resumen general de la BD
    """
    tipo = consulta.get("tipo", "").upper()

    if tipo == "HISTORICO":
        rows = consultar_historico(
            conn,
            consulta.get("interseccion", ""),
            consulta.get("ts_inicio", ""),
            consulta.get("ts_fin", "")
        )
        return {"ok": True, "datos": rows, "total": len(rows)}

    elif tipo == "ESTADO_PUNTUAL":
        row = consultar_estado_puntual(conn, consulta.get("interseccion", ""))
        return {"ok": True, "datos": row}

    elif tipo == "CONGESTION":
        rows = consultar_congestion_historica(
            conn,
            consulta.get("ts_inicio"),
            consulta.get("ts_fin")
        )
        return {"ok": True, "datos": rows, "total": len(rows)}

    elif tipo == "ESTADISTICAS":
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) AS total FROM eventos_sensores")
        total_eventos = cur.fetchone()['total']
        cur.execute("SELECT COUNT(*) AS total FROM congestion_log")
        total_congestion = cur.fetchone()['total']
        cur.execute("SELECT COUNT(*) AS total FROM prioridades_log")
        total_prioridades = cur.fetchone()['total']
        cur.execute("""
            SELECT interseccion, COUNT(*) AS cnt, estado_trafico
            FROM eventos_sensores
            GROUP BY interseccion, estado_trafico
            ORDER BY interseccion
        """)
        por_interseccion = [dict(r) for r in cur.fetchall()]
        return {
            "ok": True,
            "datos": {
                "total_eventos": total_eventos,
                "total_congestion": total_congestion,
                "total_prioridades": total_prioridades,
                "por_interseccion": por_interseccion
            }
        }

    else:
        return {"ok": False, "error": f"Tipo de consulta desconocido: {tipo}"}


# ─────────────────────────────────────────────────────────────────
# Servicio principal
# ─────────────────────────────────────────────────────────────────

def iniciar_servicio(config: dict) -> None:
    """Inicia el servicio de BD principal con heartbeat y socket REP."""
    conn  = inicializar_bd(DB_PATH)
    _lock = threading.Lock()

    ctx = zmq.Context()

    # PULL: recibir datos de la analítica
    pull = ctx.socket(zmq.PULL)
    pull.bind(f"tcp://*:{config['red']['puertos']['db_main_pull']}")
    pull.setsockopt(zmq.RCVTIMEO, 500)

    # REP: responder consultas del monitoreo (pc3/servicio_monitoreo.py)
    rep = ctx.socket(zmq.REP)
    rep.bind(f"tcp://*:{config['red']['puertos']['analitica_rep'] + 10}")
    rep.setsockopt(zmq.RCVTIMEO, 200)

    logger.info(
        f"BD Principal PULL en *:{config['red']['puertos']['db_main_pull']}"
    )
    logger.info(
        f"BD Principal REP  en *:{config['red']['puertos']['analitica_rep'] + 10}"
    )

    # Iniciar heartbeat en hilo separado
    stop_event = threading.Event()
    t_hb = threading.Thread(
        target=hilo_heartbeat,
        args=(ctx, config, stop_event),
        name="Heartbeat",
        daemon=True
    )
    t_hb.start()

    conteo = 0

    def _shutdown(sig, frame):
        logger.info(f"Cerrando BD principal (total: {conteo} registros)")
        stop_event.set()
        pull.close()
        rep.close()
        ctx.term()
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    logger.info("═" * 50)
    logger.info("  BD Principal iniciada")
    logger.info("═" * 50)

    poller = zmq.Poller()
    poller.register(pull, zmq.POLLIN)
    poller.register(rep,  zmq.POLLIN)

    try:
        while True:
            eventos_zmq = dict(poller.poll(timeout=600))

            # ── Datos de la analítica → insertar ─────────────────
            if pull in eventos_zmq:
                try:
                    raw  = pull.recv()
                    data = json.loads(raw.decode('utf-8'))
                    with _lock:
                        insertar_registro(conn, data)
                    conteo += 1
                    if conteo % 100 == 0:
                        logger.info(f"BD Principal: {conteo} registros almacenados")
                    else:
                        logger.debug(
                            f"[INSERT] {data.get('tipo')} | "
                            f"int={data.get('interseccion', '?')} | total={conteo}"
                        )
                except zmq.Again:
                    pass
                except (json.JSONDecodeError, sqlite3.Error, zmq.ZMQError) as e:
                    logger.error(f"Error procesando dato: {e}")

            # ── Consultas del monitoreo → responder ───────────────
            if rep in eventos_zmq:
                try:
                    raw     = rep.recv()
                    consulta = json.loads(raw.decode('utf-8'))
                    with _lock:
                        respuesta = procesar_consulta_bd(conn, consulta)
                    rep.send(json.dumps(respuesta).encode('utf-8'))
                    logger.info(
                        f"Consulta atendida: {consulta.get('tipo')} → "
                        f"ok={respuesta.get('ok')}"
                    )
                except zmq.Again:
                    pass
                except Exception as e:
                    logger.error(f"Error en consulta REP: {e}")
                    try:
                        rep.send(json.dumps({"ok": False, "error": str(e)}).encode())
                    except Exception:
                        pass

    except KeyboardInterrupt:
        logger.info(f"BD Principal detenida. Total: {conteo} registros.")
    finally:
        stop_event.set()
        pull.close()
        rep.close()
        ctx.term()
        conn.close()


if __name__ == "__main__":
    cfg = cargar_config()
    iniciar_servicio(cfg)
