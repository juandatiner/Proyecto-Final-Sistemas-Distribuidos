"""
pc2/servicio_analitica.py
=====================================================================
Servicio de Analítica - PC2  ← COMPONENTE CENTRAL DEL SISTEMA

Responsabilidades:
  1. SUBscribe a eventos del broker en PC1 (PUB/SUB).
  2. Agrega y analiza datos por intersección.
  3. Aplica reglas de tráfico (normal / congestión / prioridad).
  4. PUSH comandos al servicio de semáforos (PC2).
  5. PUSH datos a las dos BDs: réplica (PC2) y principal (PC3).
  6. REP socket para responder consultas/comandos del monitoreo (PC3).
  7. Detecta falla del PC3 y deja de enviarle datos si no responde.

Reglas definidas:
  NORMAL      : Q <  5  AND Vp > 35 AND Cv < 10
  MODERADO    : Q <  10 AND Vp > 15 AND Cv < 20  (entre normal y congestión)
  CONGESTION  : Q >= 10  OR Vp < 15  OR  Cv >= 20
  PRIORIDAD   : comando manual desde monitoreo

Uso:
    python servicio_analitica.py
=====================================================================
"""
import zmq
import json
import time
import threading
import sys
import os
from datetime import datetime, timezone
from collections import defaultdict

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

logger = configurar_logger("Analitica")


# ─────────────────────────────────────────────────────────────────
# Clasificación de tráfico
# ─────────────────────────────────────────────────────────────────

def clasificar_trafico(Q: float, Vp: float, Cv: float, config: dict) -> str:
    """
    Clasifica el estado del tráfico según las reglas del sistema.

    Args:
        Q  : longitud de cola (vehículos en espera, de cámara)
        Vp : velocidad promedio (km/h, de cámara o GPS)
        Cv : conteo vehicular en 30s (de espira inductiva)
        config: configuración del sistema

    Returns:
        'NORMAL' | 'MODERADO' | 'CONGESTION'
    """
    r = config['reglas_trafico']

    # Regla CONGESTION (OR entre condiciones críticas)
    if Q >= r['congestion']['Q_min'] or \
       Vp < r['congestion']['Vp_max'] or \
       Cv >= r['congestion']['Cv_min']:
        return "CONGESTION"

    # Regla NORMAL (AND)
    if Q < r['normal']['Q_max'] and \
       Vp > r['normal']['Vp_min'] and \
       Cv < r['normal']['Cv_max']:
        return "NORMAL"

    # Zona intermedia
    return "MODERADO"


def determinar_accion_semaforo(estado: str, interseccion: str) -> dict:
    """
    Determina la acción a enviar al servicio de semáforos
    según el estado de tráfico detectado.

    Args:
        estado       : 'NORMAL' | 'MODERADO' | 'CONGESTION'
        interseccion : id de la intersección afectada

    Returns:
        Dict con el comando para el servicio de semáforos.
    """
    if estado == "CONGESTION":
        return {
            "accion": "CAMBIAR",
            "interseccion": interseccion,
            "nuevo_estado": "VERDE",
            "modo": "CONGESTION"
        }
    elif estado == "NORMAL":
        return {
            "accion": "RESETEAR",
            "interseccion": interseccion
        }
    else:  # MODERADO
        return {
            "accion": "EXTENDER",
            "interseccion": interseccion,
            "segundos": 5
        }


# ─────────────────────────────────────────────────────────────────
# Estado de intersecciones
# ─────────────────────────────────────────────────────────────────

class EstadoInterseccion:
    """
    Mantiene el último valor conocido de cada sensor por intersección.
    Permite hacer la agregación cuando llegan datos de distintos sensores.
    """

    def __init__(self, interseccion: str):
        self.interseccion = interseccion
        self.Q:  float = 0.0   # de cámara
        self.Vp: float = 50.0  # de cámara o GPS
        self.Cv: float = 0.0   # de espira
        self.nivel_gps: str = "BAJA"
        self.ultimo_evento: str = ""
        self.estado_actual: str = "NORMAL"
        self.ts_ultima_actualizacion: str = ""
        # Hasta esta marca de tiempo (epoch) la interseccion mantiene
        # PRIORIDAD activa (ola verde). Mientras time.time() < prioridad_hasta,
        # los eventos de sensores NO sobrescriben el estado PRIORIDAD.
        self.prioridad_hasta: float = 0.0

    def actualizar_camara(self, evento: dict):
        self.Q  = evento.get('volumen', self.Q)
        self.Vp = evento.get('velocidad_promedio', self.Vp)
        self.ts_ultima_actualizacion = evento.get('timestamp', '')
        self.ultimo_evento = "camara"

    def actualizar_espira(self, evento: dict):
        self.Cv = evento.get('vehiculos_contados', self.Cv)
        self.ts_ultima_actualizacion = evento.get('timestamp_fin', '')
        self.ultimo_evento = "espira"

    def actualizar_gps(self, evento: dict):
        self.Vp        = evento.get('velocidad_promedio', self.Vp)
        self.nivel_gps = evento.get('nivel_congestion', self.nivel_gps)
        self.ts_ultima_actualizacion = evento.get('timestamp', '')
        self.ultimo_evento = "gps"

    def to_dict(self) -> dict:
        return {
            "interseccion": self.interseccion,
            "Q": self.Q,
            "Vp": self.Vp,
            "Cv": self.Cv,
            "nivel_gps": self.nivel_gps,
            "estado_actual": self.estado_actual,
            "ts": self.ts_ultima_actualizacion
        }


# ─────────────────────────────────────────────────────────────────
# Detector de falla del PC3 (heartbeat)
# ─────────────────────────────────────────────────────────────────

class MonitorPC3:
    """
    Monitorea el estado del PC3 mediante heartbeat.
    Si no recibe señal en `timeout_seg`, marca PC3 como caído.
    """

    def __init__(self, config: dict):
        self._timeout = config['heartbeat']['timeout_seg']
        self._ultimo_heartbeat = time.time()
        self._pc3_activo = True
        self._lock = threading.Lock()

    def registrar_heartbeat(self):
        with self._lock:
            self._ultimo_heartbeat = time.time()
            if not self._pc3_activo:
                logger.info("✅ PC3 recuperado — reanudando envío a BD principal")
            self._pc3_activo = True

    def verificar(self) -> bool:
        with self._lock:
            elapsed = time.time() - self._ultimo_heartbeat
            if elapsed > self._timeout and self._pc3_activo:
                logger.warning(
                    f"⚠️  PC3 no responde desde hace {elapsed:.1f}s "
                    f"(timeout={self._timeout}s) — usando réplica en PC2"
                )
                self._pc3_activo = False
            return self._pc3_activo

    @property
    def activo(self) -> bool:
        return self.verificar()


# ─────────────────────────────────────────────────────────────────
# Servicio principal
# ─────────────────────────────────────────────────────────────────

def iniciar_servicio(config: dict) -> None:
    """Inicia el servicio de analítica con todos sus sockets ZeroMQ."""
    red   = config['red']
    puertos = red['puertos']
    pc2_ip = red['PC2_IP']
    pc3_ip = red['PC3_IP']

    ctx = zmq.Context()

    # 1. SUB: recibir eventos del broker en PC1
    sub = ctx.socket(zmq.SUB)
    sub.connect(f"tcp://{red['PC1_IP']}:{puertos['broker_pub']}")
    sub.setsockopt_string(zmq.SUBSCRIBE, "camara")
    sub.setsockopt_string(zmq.SUBSCRIBE, "espira_inductiva")
    sub.setsockopt_string(zmq.SUBSCRIBE, "gps")
    logger.info(f"SUB conectado al broker PC1 → {red['PC1_IP']}:{puertos['broker_pub']}")

    # 2. PUSH: enviar comandos al servicio de semáforos (PC2 local)
    push_semaforos = ctx.socket(zmq.PUSH)
    push_semaforos.connect(f"tcp://{pc2_ip}:{puertos['semaforos_pull']}")
    logger.info(f"PUSH semáforos → {pc2_ip}:{puertos['semaforos_pull']}")

    # 3. PUSH: enviar datos a BD réplica (PC2 local)
    push_db_replica = ctx.socket(zmq.PUSH)
    push_db_replica.connect(f"tcp://{pc2_ip}:{puertos['db_replica_pull']}")
    logger.info(f"PUSH BD réplica → {pc2_ip}:{puertos['db_replica_pull']}")

    # 4. PUSH: enviar datos a BD principal (PC3, puede fallar)
    push_db_main = ctx.socket(zmq.PUSH)
    push_db_main.setsockopt(zmq.SNDTIMEO, 500)  # no bloquear si PC3 cae
    push_db_main.connect(f"tcp://{pc3_ip}:{puertos['db_main_pull']}")
    logger.info(f"PUSH BD principal → {pc3_ip}:{puertos['db_main_pull']}")

    # 5. REP: responder consultas del monitoreo (PC3)
    rep = ctx.socket(zmq.REP)
    rep.bind(f"tcp://*:{puertos['analitica_rep']}")
    rep.setsockopt(zmq.RCVTIMEO, 100)
    logger.info(f"REP monitoreo escuchando en *:{puertos['analitica_rep']}")

    # 6. SUB: recibir heartbeats de PC3
    sub_heartbeat = ctx.socket(zmq.SUB)
    sub_heartbeat.connect(f"tcp://{pc3_ip}:{puertos['heartbeat_pub']}")
    sub_heartbeat.setsockopt_string(zmq.SUBSCRIBE, "heartbeat")
    sub_heartbeat.setsockopt(zmq.RCVTIMEO, 100)

    # Estado del sistema
    estado_intersecciones: dict[str, EstadoInterseccion] = defaultdict(
        lambda: EstadoInterseccion("desconocido")
    )
    monitor_pc3 = MonitorPC3(config)

    logger.info("═" * 60)
    logger.info("  Servicio de Analítica iniciado")
    logger.info("═" * 60)

    def _push_a_bds(payload: dict):
        """Envía datos a la BD réplica y (si está activo) a la BD principal."""
        msg = json.dumps(payload).encode('utf-8')
        try:
            push_db_replica.send(msg)
        except zmq.ZMQError as e:
            logger.error(f"Error enviando a BD réplica: {e}")

        if monitor_pc3.activo:
            try:
                push_db_main.send(msg)
            except zmq.ZMQError:
                logger.warning("No se pudo enviar a BD principal (PC3 posiblemente caído)")
                monitor_pc3._pc3_activo = False

    def _procesar_consulta_monitoreo(consulta: dict) -> dict:
        """Atiende consultas/comandos del servicio de monitoreo."""
        tipo = consulta.get("tipo", "").upper()

        if tipo == "HEARTBEAT":
            monitor_pc3.registrar_heartbeat()
            return {"ok": True, "msg": "pong"}

        elif tipo == "ESTADO_INTERSECCION":
            inter = consulta.get("interseccion", "")
            if inter in estado_intersecciones:
                return {"ok": True, "data": estado_intersecciones[inter].to_dict()}
            return {"ok": False, "error": f"Intersección {inter} sin datos"}

        elif tipo == "ESTADO_SISTEMA":
            resumen = {k: v.to_dict() for k, v in estado_intersecciones.items()}
            return {"ok": True, "data": resumen, "pc3_activo": monitor_pc3.activo}

        elif tipo == "AMBULANCIA":
            vias = consulta.get("vias", [])
            if not vias:
                return {"ok": False, "error": "Se requiere lista de 'vias'"}
            cmd = {"accion": "PRIORIDAD", "vias": vias}
            push_semaforos.send(json.dumps(cmd).encode())
            # Marcar las vias como PRIORIDAD por 60s para que el estado
            # reportado (y el dashboard) muestren la ola verde.
            _expira = time.time() + 60
            for _via in vias:
                if _via not in estado_intersecciones:
                    estado_intersecciones[_via] = EstadoInterseccion(_via)
                estado_intersecciones[_via].estado_actual = "PRIORIDAD"
                estado_intersecciones[_via].prioridad_hasta = _expira
            logger.info(f"🚨 AMBULANCIA: prioridad activada en {vias}")
            _push_a_bds({
                "tipo": "prioridad",
                "vias": vias,
                "timestamp": datetime.now(timezone.utc).isoformat()
            })
            return {"ok": True, "msg": f"Prioridad activada en {vias}"}

        elif tipo == "CAMBIAR_SEMAFORO":
            inter   = consulta.get("interseccion", "")
            estado  = consulta.get("nuevo_estado", "VERDE")
            cmd = {"accion": "CAMBIAR", "interseccion": inter,
                   "nuevo_estado": estado, "modo": "PRIORIDAD"}
            push_semaforos.send(json.dumps(cmd).encode())
            # Reflejar el cambio manual en el estado por 30s.
            if inter:
                if inter not in estado_intersecciones:
                    estado_intersecciones[inter] = EstadoInterseccion(inter)
                # VERDE forzado -> PRIORIDAD (luz verde); ROJO forzado ->
                # ROJO_MANUAL (luz roja, distinto de congestion).
                estado_intersecciones[inter].estado_actual = (
                    "PRIORIDAD" if estado.upper() == "VERDE" else "ROJO_MANUAL")
                estado_intersecciones[inter].prioridad_hasta = time.time() + 30
            logger.info(f"Cambio directo: {inter} → {estado}")
            return {"ok": True}

        else:
            return {"ok": False, "error": f"Tipo de consulta desconocido: {tipo}"}

    # ── Bucle principal ───────────────────────────────────────────
    poller = zmq.Poller()
    poller.register(sub,           zmq.POLLIN)
    poller.register(rep,           zmq.POLLIN)
    poller.register(sub_heartbeat, zmq.POLLIN)

    try:
        while True:
            eventos_zmq = dict(poller.poll(timeout=500))

            # ── Heartbeat de PC3 ──────────────────────────────────
            if sub_heartbeat in eventos_zmq:
                try:
                    sub_heartbeat.recv_multipart()
                    monitor_pc3.registrar_heartbeat()
                except zmq.Again:
                    pass

            # ── Consultas/comandos del monitoreo ──────────────────
            if rep in eventos_zmq:
                try:
                    raw = rep.recv()
                    consulta = json.loads(raw.decode('utf-8'))
                    respuesta = _procesar_consulta_monitoreo(consulta)
                    rep.send(json.dumps(respuesta).encode('utf-8'))
                except zmq.Again:
                    pass
                except Exception as e:
                    logger.error(f"Error procesando consulta: {e}")
                    try:
                        rep.send(json.dumps({"ok": False, "error": str(e)}).encode())
                    except Exception:
                        pass

            # ── Eventos de sensores ───────────────────────────────
            if sub in eventos_zmq:
                try:
                    partes = sub.recv_multipart()
                    if len(partes) < 2:
                        continue
                    topic   = partes[0].decode('utf-8')
                    payload = json.loads(partes[1].decode('utf-8'))
                except Exception as e:
                    logger.warning(f"Error decodificando mensaje: {e}")
                    continue

                interseccion = payload.get("interseccion", "DESCONOCIDA")

                # Asegurar que existe el estado para esta intersección
                if interseccion not in estado_intersecciones:
                    estado_intersecciones[interseccion] = EstadoInterseccion(interseccion)
                est = estado_intersecciones[interseccion]

                # Actualizar estado local según tipo de sensor
                if topic == "camara":
                    est.actualizar_camara(payload)
                elif topic == "espira_inductiva":
                    est.actualizar_espira(payload)
                elif topic == "gps":
                    est.actualizar_gps(payload)

                # Clasificar tráfico con datos actuales.
                # Si hay PRIORIDAD activa (ola verde), se respeta y NO se
                # reclasifica hasta que expire.
                if est.prioridad_hasta > time.time():
                    nuevo_estado = est.estado_actual  # mantiene PRIORIDAD o ROJO_MANUAL
                else:
                    nuevo_estado = clasificar_trafico(est.Q, est.Vp, est.Cv, config)
                estado_anterior = est.estado_actual
                est.estado_actual = nuevo_estado

                # Log informativo
                logger.info(
                    f"[{topic.upper():<18}] {interseccion} | "
                    f"Q={est.Q:>3.0f} Vp={est.Vp:>5.1f} Cv={est.Cv:>3.0f} "
                    f"→ {nuevo_estado}"
                )

                # Tomar acción en semáforos si cambia el estado
                if nuevo_estado != estado_anterior:
                    accion = determinar_accion_semaforo(nuevo_estado, interseccion)
                    push_semaforos.send(json.dumps(accion).encode())
                    logger.info(f"  ↳ Acción semáforo: {accion['accion']} "
                                f"en {interseccion} (modo={nuevo_estado})")

                # Persistir en BDs
                registro_bd = {
                    "tipo": "evento_sensor",
                    "topic": topic,
                    "interseccion": interseccion,
                    "estado_trafico": nuevo_estado,
                    "metricas": est.to_dict(),
                    "raw_evento": payload,
                    "timestamp": datetime.now(timezone.utc).isoformat()
                }
                _push_a_bds(registro_bd)

    except KeyboardInterrupt:
        logger.info("Servicio de analítica detenido.")
    finally:
        for s in [sub, push_semaforos, push_db_replica, push_db_main,
                  rep, sub_heartbeat]:
            s.close()
        ctx.term()


if __name__ == "__main__":
    cfg = cargar_config()
    iniciar_servicio(cfg)
