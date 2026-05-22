"""
pc2/servicio_semaforos.py
=====================================================================
Servicio de Control de Semáforos - PC2
Recibe comandos del servicio de analítica (PUSH → PULL) y gestiona
el estado de todos los semáforos de la ciudad.

Cada semáforo tiene:
  - estado      : 'VERDE' o 'ROJO'
  - tiempo_restante: segundos hasta el próximo cambio de estado
  - modo        : 'NORMAL' | 'CONGESTION' | 'PRIORIDAD'

Comandos reconocidos (JSON):
  { "accion": "CAMBIAR",    "interseccion": "INT-C5", "nuevo_estado": "VERDE" }
  { "accion": "EXTENDER",   "interseccion": "INT-C5", "segundos": 15 }
  { "accion": "PRIORIDAD",  "vias": ["INT-A1","INT-A2","INT-A3"] }
  { "accion": "RESETEAR",   "interseccion": "INT-C5" }
  { "accion": "CONSULTAR",  "interseccion": "INT-C5" }

Uso:
    python servicio_semaforos.py
=====================================================================
"""
import zmq
import json
import time
import threading
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config, obtener_intersecciones
from common.logger import configurar_logger

logger = configurar_logger("Semaforos")


# ─────────────────────────────────────────────────────────────────
# Estado global de semáforos
# ─────────────────────────────────────────────────────────────────

class GestorSemaforos:
    """
    Mantiene el estado de todos los semáforos y sus timers.
    Thread-safe mediante un lock.
    """

    def __init__(self, config: dict):
        self.config = config
        self._lock = threading.Lock()

        # Obtener tiempos de la configuración
        self._t_verde_normal     = config['semaforos']['tiempo_verde_normal_seg']
        self._t_verde_congestion = config['semaforos']['tiempo_verde_congestion_seg']
        self._t_verde_prioridad  = config['semaforos']['tiempo_verde_prioridad_seg']
        self._t_rojo_normal      = config['semaforos']['tiempo_rojo_normal_seg']
        self._t_rojo_congestion  = config['semaforos']['tiempo_rojo_congestion_seg']

        # Inicializar semáforos para todas las intersecciones
        self._semaforos: dict[str, dict] = {}
        for interseccion in obtener_intersecciones(config):
            self._semaforos[interseccion] = {
                "interseccion": interseccion,
                "estado": "ROJO",
                "modo": "NORMAL",
                "tiempo_restante": self._t_rojo_normal,
                "ultimo_cambio": datetime.now(timezone.utc).isoformat()
            }

        # Iniciar hilo de temporización
        self._running = True
        self._timer_thread = threading.Thread(
            target=self._bucle_timer,
            name="Timer-Semaforos",
            daemon=True
        )
        self._timer_thread.start()
        logger.info(f"Gestor iniciado con {len(self._semaforos)} intersecciones")

    def _bucle_timer(self):
        """
        Hilo que actualiza los contadores de tiempo cada segundo
        y ejecuta los cambios de estado automáticos.
        """
        while self._running:
            time.sleep(1)
            with self._lock:
                for inter, sem in self._semaforos.items():
                    sem['tiempo_restante'] -= 1
                    if sem['tiempo_restante'] <= 0:
                        self._cambiar_estado_automatico(inter, sem)

    def _cambiar_estado_automatico(self, interseccion: str, sem: dict):
        """
        Aplica el cambio automático de estado al semáforo al expirar el timer.
        Debe llamarse con el lock ya adquirido.
        """
        nuevo_estado = "VERDE" if sem['estado'] == "ROJO" else "ROJO"
        sem['estado'] = nuevo_estado
        sem['ultimo_cambio'] = datetime.now(timezone.utc).isoformat()

        # Asignar duración según modo y nuevo estado
        if nuevo_estado == "VERDE":
            if sem['modo'] == "PRIORIDAD":
                sem['tiempo_restante'] = self._t_verde_prioridad
            elif sem['modo'] == "CONGESTION":
                sem['tiempo_restante'] = self._t_verde_congestion
            else:
                sem['tiempo_restante'] = self._t_verde_normal
        else:
            if sem['modo'] == "CONGESTION":
                sem['tiempo_restante'] = self._t_rojo_congestion
            else:
                sem['tiempo_restante'] = self._t_rojo_normal

        logger.info(
            f"[AUTO] {interseccion}: {sem['estado']} → "
            f"{'ROJO' if sem['estado']=='VERDE' else 'VERDE'} ... "
            f"ahora {sem['estado']} por {sem['tiempo_restante']}s"
        )

    # ── API pública ───────────────────────────────────────────────

    def cambiar_estado(self, interseccion: str, nuevo_estado: str,
                       modo: str = None) -> dict:
        """Fuerza el cambio de estado de un semáforo."""
        with self._lock:
            if interseccion not in self._semaforos:
                return {"ok": False, "error": f"Intersección {interseccion} no existe"}
            sem = self._semaforos[interseccion]
            estado_anterior = sem['estado']
            sem['estado'] = nuevo_estado.upper()
            sem['ultimo_cambio'] = datetime.now(timezone.utc).isoformat()
            if modo:
                sem['modo'] = modo.upper()

            # Reiniciar timer según nuevo estado y modo
            if sem['estado'] == "VERDE":
                sem['tiempo_restante'] = (
                    self._t_verde_prioridad if sem['modo'] == "PRIORIDAD" else
                    self._t_verde_congestion if sem['modo'] == "CONGESTION" else
                    self._t_verde_normal
                )
            else:
                sem['tiempo_restante'] = (
                    self._t_rojo_congestion if sem['modo'] == "CONGESTION" else
                    self._t_rojo_normal
                )

            logger.info(
                f"[CAMBIO] {interseccion}: {estado_anterior} → {sem['estado']} "
                f"| modo={sem['modo']} | dur={sem['tiempo_restante']}s"
            )
            return {"ok": True, "semaforo": dict(sem)}

    def extender_verde(self, interseccion: str, segundos: int) -> dict:
        """Extiende la fase verde de un semáforo en 'segundos' adicionales."""
        with self._lock:
            if interseccion not in self._semaforos:
                return {"ok": False, "error": f"Intersección {interseccion} no existe"}
            sem = self._semaforos[interseccion]
            if sem['estado'] != "VERDE":
                # Forzar verde antes de extender
                sem['estado'] = "VERDE"
                sem['tiempo_restante'] = segundos
            else:
                sem['tiempo_restante'] += segundos
            logger.info(
                f"[EXTENDER] {interseccion}: +{segundos}s verde | "
                f"restante={sem['tiempo_restante']}s"
            )
            return {"ok": True, "semaforo": dict(sem)}

    def activar_prioridad(self, vias: list) -> dict:
        """
        Activa ola verde en las intersecciones indicadas (e.g. paso ambulancia).
        El resto de intersecciones pasa a ROJO.
        """
        resultados = {}
        with self._lock:
            for inter, sem in self._semaforos.items():
                if inter in vias:
                    sem['estado'] = "VERDE"
                    sem['modo'] = "PRIORIDAD"
                    sem['tiempo_restante'] = self._t_verde_prioridad
                    sem['ultimo_cambio'] = datetime.now(timezone.utc).isoformat()
                    logger.info(f"[PRIORIDAD] {inter} → VERDE ({self._t_verde_prioridad}s)")
                else:
                    sem['estado'] = "ROJO"
                    sem['modo'] = "NORMAL"
                    sem['tiempo_restante'] = self._t_rojo_normal
                resultados[inter] = dict(sem)
        return {"ok": True, "resultados": resultados}

    def consultar(self, interseccion: str) -> dict:
        """Retorna el estado actual de un semáforo."""
        with self._lock:
            if interseccion not in self._semaforos:
                return {"ok": False, "error": f"Intersección {interseccion} no existe"}
            return {"ok": True, "semaforo": dict(self._semaforos[interseccion])}

    def consultar_todos(self) -> dict:
        """Retorna el estado de todos los semáforos."""
        with self._lock:
            return {k: dict(v) for k, v in self._semaforos.items()}

    def detener(self):
        self._running = False


# ─────────────────────────────────────────────────────────────────
# Servicio ZeroMQ
# ─────────────────────────────────────────────────────────────────

def iniciar_servicio(config: dict) -> None:
    """
    Inicia el servicio de control de semáforos.
    Escucha comandos por PULL socket desde la analítica.
    """
    gestor = GestorSemaforos(config)

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PULL)
    sock.bind(f"tcp://*:{config['red']['puertos']['semaforos_pull']}")
    sock.setsockopt(zmq.RCVTIMEO, 1000)

    logger.info(
        f"Servicio de semáforos escuchando en "
        f"*:{config['red']['puertos']['semaforos_pull']}"
    )

    try:
        while True:
            try:
                raw = sock.recv()
            except zmq.Again:
                continue

            try:
                cmd = json.loads(raw.decode('utf-8'))
            except json.JSONDecodeError:
                logger.warning(f"Comando JSON inválido recibido: {raw[:80]}")
                continue

            accion = cmd.get("accion", "").upper()

            if accion == "CAMBIAR":
                resultado = gestor.cambiar_estado(
                    cmd.get("interseccion", ""),
                    cmd.get("nuevo_estado", "ROJO"),
                    cmd.get("modo")
                )
                logger.info(f"CAMBIAR → {resultado}")

            elif accion == "EXTENDER":
                resultado = gestor.extender_verde(
                    cmd.get("interseccion", ""),
                    int(cmd.get("segundos", 15))
                )
                logger.info(f"EXTENDER → {resultado}")

            elif accion == "PRIORIDAD":
                resultado = gestor.activar_prioridad(cmd.get("vias", []))
                logger.info(f"PRIORIDAD activada en {cmd.get('vias', [])}")

            elif accion == "RESETEAR":
                resultado = gestor.cambiar_estado(
                    cmd.get("interseccion", ""),
                    "ROJO",
                    "NORMAL"
                )
                logger.info(f"RESETEAR → {resultado}")

            elif accion == "CONSULTAR":
                resultado = gestor.consultar(cmd.get("interseccion", ""))
                logger.info(f"CONSULTAR → {resultado}")

            else:
                logger.warning(f"Acción desconocida: {accion}")

    except KeyboardInterrupt:
        logger.info("Servicio de semáforos detenido.")
    finally:
        gestor.detener()
        sock.close()
        ctx.term()


if __name__ == "__main__":
    cfg = cargar_config()
    iniciar_servicio(cfg)
