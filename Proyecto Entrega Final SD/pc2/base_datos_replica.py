"""
pc2/base_datos_replica.py
=====================================================================
Base de Datos RÉPLICA - PC2
Recibe datos vía PULL socket (patrón PUSH/PULL asíncrono) desde
el servicio de analítica y los persiste en SQLite.

Funciona como backup cuando el PC3 (BD principal) falla.
El esquema es idéntico al de la BD principal (pc3/base_datos_principal.py).

Uso:
    python base_datos_replica.py
=====================================================================
"""
import zmq
import json
import sqlite3
import os
import sys
import signal
import threading
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

logger = configurar_logger("BD-Replica")

DB_PATH = os.path.join(os.path.dirname(__file__), "trafico_replica.db")


# ─────────────────────────────────────────────────────────────────
# Inicialización de la BD SQLite
# ─────────────────────────────────────────────────────────────────

def inicializar_bd(ruta: str) -> sqlite3.Connection:
    """
    Crea o abre la BD SQLite y crea las tablas si no existen.

    Tablas:
      eventos_sensores : todos los eventos de sensores con métricas
      estados_semaforos: cambios de estado de semáforos
      congestion_log   : registros de congestión detectada
      prioridades_log  : activaciones de prioridad (ambulancias, etc.)
    """
    conn = sqlite3.connect(ruta, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    cur = conn.cursor()

    cur.executescript("""
        CREATE TABLE IF NOT EXISTS eventos_sensores (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT    NOT NULL,
            interseccion    TEXT    NOT NULL,
            topic           TEXT    NOT NULL,
            Q               REAL    DEFAULT 0,
            Vp              REAL    DEFAULT 0,
            Cv              REAL    DEFAULT 0,
            nivel_gps       TEXT    DEFAULT '',
            estado_trafico  TEXT    NOT NULL,
            raw_json        TEXT    NOT NULL
        );

        CREATE TABLE IF NOT EXISTS estados_semaforos (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT    NOT NULL,
            interseccion    TEXT    NOT NULL,
            estado          TEXT    NOT NULL,
            modo            TEXT    NOT NULL,
            duracion_seg    INTEGER DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS congestion_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT    NOT NULL,
            interseccion    TEXT    NOT NULL,
            nivel           TEXT    NOT NULL,
            Q               REAL    DEFAULT 0,
            Vp              REAL    DEFAULT 0,
            Cv              REAL    DEFAULT 0
        );

        CREATE TABLE IF NOT EXISTS prioridades_log (
            id              INTEGER PRIMARY KEY AUTOINCREMENT,
            timestamp       TEXT    NOT NULL,
            vias            TEXT    NOT NULL,
            origen          TEXT    DEFAULT 'sistema'
        );

        CREATE INDEX IF NOT EXISTS idx_eventos_ts    ON eventos_sensores(timestamp);
        CREATE INDEX IF NOT EXISTS idx_eventos_int   ON eventos_sensores(interseccion);
        CREATE INDEX IF NOT EXISTS idx_congestion_ts ON congestion_log(timestamp);
    """)
    conn.commit()
    logger.info(f"BD inicializada en: {ruta}")
    return conn


# ─────────────────────────────────────────────────────────────────
# Inserción de registros
# ─────────────────────────────────────────────────────────────────

def insertar_registro(conn: sqlite3.Connection, data: dict) -> None:
    """
    Inserta un registro en la tabla correspondiente según su tipo.

    Args:
        conn : conexión SQLite activa
        data : dict con el registro (campo 'tipo' determina la tabla)
    """
    tipo = data.get("tipo", "")
    cur  = conn.cursor()

    if tipo == "evento_sensor":
        metricas = data.get("metricas", {})
        cur.execute("""
            INSERT INTO eventos_sensores
                (timestamp, interseccion, topic, Q, Vp, Cv, nivel_gps,
                 estado_trafico, raw_json)
            VALUES (?,?,?,?,?,?,?,?,?)
        """, (
            data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            data.get("interseccion", ""),
            data.get("topic", ""),
            metricas.get("Q", 0),
            metricas.get("Vp", 0),
            metricas.get("Cv", 0),
            metricas.get("nivel_gps", ""),
            data.get("estado_trafico", ""),
            json.dumps(data.get("raw_evento", {}))
        ))

        # Si hay congestión, también registrar en congestion_log
        if data.get("estado_trafico") == "CONGESTION":
            cur.execute("""
                INSERT INTO congestion_log
                    (timestamp, interseccion, nivel, Q, Vp, Cv)
                VALUES (?,?,?,?,?,?)
            """, (
                data.get("timestamp", ""),
                data.get("interseccion", ""),
                "CONGESTION",
                metricas.get("Q", 0),
                metricas.get("Vp", 0),
                metricas.get("Cv", 0)
            ))

    elif tipo == "cambio_semaforo":
        cur.execute("""
            INSERT INTO estados_semaforos
                (timestamp, interseccion, estado, modo, duracion_seg)
            VALUES (?,?,?,?,?)
        """, (
            data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            data.get("interseccion", ""),
            data.get("estado", ""),
            data.get("modo", "NORMAL"),
            data.get("duracion_seg", 0)
        ))

    elif tipo == "prioridad":
        cur.execute("""
            INSERT INTO prioridades_log
                (timestamp, vias, origen)
            VALUES (?,?,?)
        """, (
            data.get("timestamp", datetime.now(timezone.utc).isoformat()),
            json.dumps(data.get("vias", [])),
            data.get("origen", "sistema")
        ))

    conn.commit()


# ─────────────────────────────────────────────────────────────────
# Consultas utilitarias (usadas por el monitoreo si PC3 falla)
# ─────────────────────────────────────────────────────────────────

def consultar_historico(conn: sqlite3.Connection,
                        interseccion: str,
                        ts_inicio: str,
                        ts_fin: str) -> list:
    """
    Consulta histórico de tráfico para una intersección en un rango de tiempo.

    Args:
        conn        : conexión SQLite
        interseccion: id de la intersección
        ts_inicio   : timestamp ISO inicio del rango
        ts_fin      : timestamp ISO fin del rango

    Returns:
        Lista de dicts con los registros encontrados.
    """
    cur = conn.cursor()
    cur.execute("""
        SELECT timestamp, interseccion, topic, Q, Vp, Cv, estado_trafico
        FROM   eventos_sensores
        WHERE  interseccion = ?
          AND  timestamp >= ?
          AND  timestamp <= ?
        ORDER  BY timestamp ASC
    """, (interseccion, ts_inicio, ts_fin))
    return [dict(row) for row in cur.fetchall()]


def consultar_estado_puntual(conn: sqlite3.Connection,
                             interseccion: str) -> dict:
    """Retorna el último evento registrado para la intersección."""
    cur = conn.cursor()
    cur.execute("""
        SELECT * FROM eventos_sensores
        WHERE  interseccion = ?
        ORDER  BY timestamp DESC
        LIMIT  1
    """, (interseccion,))
    row = cur.fetchone()
    return dict(row) if row else {}


def consultar_congestion_historica(conn: sqlite3.Connection,
                                   ts_inicio: str = None,
                                   ts_fin: str = None) -> list:
    """Retorna todos los registros de congestión en un rango de tiempo."""
    cur = conn.cursor()
    if ts_inicio and ts_fin:
        cur.execute("""
            SELECT * FROM congestion_log
            WHERE  timestamp >= ? AND timestamp <= ?
            ORDER  BY timestamp DESC
        """, (ts_inicio, ts_fin))
    else:
        cur.execute("SELECT * FROM congestion_log ORDER BY timestamp DESC LIMIT 100")
    return [dict(row) for row in cur.fetchall()]


# ─────────────────────────────────────────────────────────────────
# Servicio ZeroMQ
# ─────────────────────────────────────────────────────────────────

def iniciar_servicio(config: dict) -> None:
    """Inicia el servicio de BD réplica."""
    conn = inicializar_bd(DB_PATH)
    _lock = threading.Lock()

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.bind(f"tcp://*:{config['red']['puertos']['db_replica_pull']}")
    sock.setsockopt(zmq.RCVTIMEO, 1000)

    logger.info(
        f"BD Réplica escuchando en *:{config['red']['puertos']['db_replica_pull']}"
    )

    conteo = 0

    def _shutdown(sig, frame):
        logger.info(f"Cerrando BD réplica (total registros: {conteo})")
        sock.close()
        ctx.term()
        conn.close()
        sys.exit(0)

    signal.signal(signal.SIGINT, _shutdown)
    signal.signal(signal.SIGTERM, _shutdown)

    try:
        while True:
            try:
                raw = sock.recv()
            except zmq.Again:
                continue

            try:
                data = json.loads(raw.decode('utf-8'))
            except json.JSONDecodeError as e:
                logger.warning(f"JSON inválido recibido: {e}")
                continue

            with _lock:
                try:
                    insertar_registro(conn, data)
                    conteo += 1
                    if conteo % 100 == 0:
                        logger.info(f"BD Réplica: {conteo} registros almacenados")
                    else:
                        logger.debug(
                            f"[REPLICA] {data.get('tipo')} | "
                            f"int={data.get('interseccion', '?')} | "
                            f"total={conteo}"
                        )
                except sqlite3.Error as e:
                    logger.error(f"Error en BD réplica: {e}")

    except KeyboardInterrupt:
        logger.info(f"Servicio BD réplica detenido. Total: {conteo} registros.")
    finally:
        sock.close()
        ctx.term()
        conn.close()


if __name__ == "__main__":
    cfg = cargar_config()
    iniciar_servicio(cfg)
