"""
pc3/servicio_monitoreo.py
=====================================================================
Servicio de Monitoreo y Consulta - PC3
Interfaz de usuario (CLI) para:
  - Consultar estado actual del sistema.
  - Consultar histórico de tráfico por intersección y periodo.
  - Enviar indicaciones directas a la analítica (e.g. paso ambulancia).
  - Forzar cambios de semáforos.

Se comunica mediante REQ/REP con:
  - Servicio de analítica en PC2 (para comandos y estado en tiempo real)
  - BD principal en PC3 (para consultas históricas)
  - BD réplica en PC2 (fallback si BD principal falla)

Uso:
    python servicio_monitoreo.py
=====================================================================
"""
import zmq
import json
import sys
import os
import time
from datetime import datetime, timezone, timedelta
from typing import Optional

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config, obtener_intersecciones
from common.logger import configurar_logger

logger = configurar_logger("Monitoreo")

# ─────────────────────────────────────────────────────────────────
# Cliente de comunicación con analítica y BDs
# ─────────────────────────────────────────────────────────────────

class ClienteAnalitica:
    """Envía solicitudes REQ al servicio de analítica en PC2."""

    def __init__(self, config: dict):
        self._config  = config
        self._ctx     = zmq.Context()
        self._sock    = self._ctx.socket(zmq.REQ)
        self._sock.setsockopt(zmq.RCVTIMEO, 3000)
        self._sock.setsockopt(zmq.SNDTIMEO, 3000)
        pc2_ip = config['red']['PC2_IP']
        puerto = config['red']['puertos']['analitica_rep']
        self._sock.connect(f"tcp://{pc2_ip}:{puerto}")
        logger.info(f"Conectado a analítica PC2 → {pc2_ip}:{puerto}")

    def enviar(self, payload: dict) -> Optional[dict]:
        try:
            self._sock.send(json.dumps(payload).encode('utf-8'))
            raw = self._sock.recv()
            return json.loads(raw.decode('utf-8'))
        except zmq.ZMQError as e:
            logger.error(f"Error comunicando con analítica: {e}")
            # Recrear socket tras error
            self._sock.close()
            self._sock = self._ctx.socket(zmq.REQ)
            self._sock.setsockopt(zmq.RCVTIMEO, 3000)
            self._sock.setsockopt(zmq.SNDTIMEO, 3000)
            pc2_ip = self._config['red']['PC2_IP']
            puerto = self._config['red']['puertos']['analitica_rep']
            self._sock.connect(f"tcp://{pc2_ip}:{puerto}")
            return None

    def cerrar(self):
        self._sock.close()
        self._ctx.term()


class ClienteBD:
    """
    Consulta la BD principal (PC3) con fallback a réplica (PC2).
    """

    def __init__(self, config: dict):
        self._config = config
        self._ctx    = zmq.Context()
        self._sock_main    = None
        self._sock_replica = None
        self._pc3_activo   = True
        self._reconectar()

    def _reconectar(self):
        """Crea (o recrea) los sockets a BD principal y réplica."""
        if self._sock_main:
            self._sock_main.close()
        if self._sock_replica:
            self._sock_replica.close()

        red = self._config['red']

        self._sock_main = self._ctx.socket(zmq.REQ)
        self._sock_main.setsockopt(zmq.RCVTIMEO, 2000)
        self._sock_main.setsockopt(zmq.SNDTIMEO, 1000)
        self._sock_main.connect(
            f"tcp://{red['PC3_IP']}:{red['puertos']['analitica_rep'] + 10}"
        )

        self._sock_replica = self._ctx.socket(zmq.REQ)
        self._sock_replica.setsockopt(zmq.RCVTIMEO, 2000)
        self._sock_replica.setsockopt(zmq.SNDTIMEO, 1000)
        # La réplica no tiene REP propio en esta versión;
        # se conecta al mismo puerto de la analítica con tipo BD
        self._sock_replica.connect(
            f"tcp://{red['PC2_IP']}:{red['puertos']['db_replica_pull'] + 10}"
        )

    def consultar(self, payload: dict) -> Optional[dict]:
        """Consulta BD principal; si falla, usa réplica."""
        sock = self._sock_main if self._pc3_activo else self._sock_replica
        try:
            sock.send(json.dumps(payload).encode('utf-8'))
            raw = sock.recv()
            if not self._pc3_activo:
                logger.info("Consultando BD réplica (PC3 no disponible)")
            return json.loads(raw.decode('utf-8'))
        except zmq.ZMQError:
            if self._pc3_activo:
                logger.warning("BD Principal no responde → cambiando a réplica PC2")
                self._pc3_activo = False
                self._reconectar()
                # Reintentar con réplica
                try:
                    self._sock_replica.send(json.dumps(payload).encode('utf-8'))
                    raw = self._sock_replica.recv()
                    return json.loads(raw.decode('utf-8'))
                except zmq.ZMQError as e2:
                    logger.error(f"Error en réplica también: {e2}")
                    return None
            return None

    def cerrar(self):
        if self._sock_main:
            self._sock_main.close()
        if self._sock_replica:
            self._sock_replica.close()
        self._ctx.term()


# ─────────────────────────────────────────────────────────────────
# Interfaz de línea de comandos
# ─────────────────────────────────────────────────────────────────

MENU = """
╔══════════════════════════════════════════════════════════════╗
║          SISTEMA DE GESTIÓN INTELIGENTE DE TRÁFICO           ║
║                   Monitoreo y Consulta                        ║
╠══════════════════════════════════════════════════════════════╣
║  1. Estado actual de una intersección                         ║
║  2. Estado de TODO el sistema                                 ║
║  3. Historial de tráfico por intersección y período           ║
║  4. Historial de congestiones                                 ║
║  5. Estadísticas generales de la BD                           ║
║  6. Activar paso de ambulancia (ola verde)                    ║
║  7. Forzar cambio de semáforo en intersección                 ║
║  8. Verificar estado del sistema (ping a analítica)           ║
║  0. Salir                                                     ║
╚══════════════════════════════════════════════════════════════╝
"""


def pedir_interseccion(config: dict) -> str:
    intersecciones = obtener_intersecciones(config)
    print(f"\n  Intersecciones disponibles: {', '.join(intersecciones[:10])} ...")
    inter = input("  Ingrese intersección (ej. INT-C5): ").strip().upper()
    if not inter.startswith("INT-"):
        inter = f"INT-{inter}"
    return inter


def pedir_rango_fechas():
    print("  Formato: YYYY-MM-DDTHH:MM:SS (dejar vacío para últimas 2 horas)")
    ts_fin   = datetime.now(timezone.utc)
    ts_inicio = ts_fin - timedelta(hours=2)
    entrada_inicio = input(f"  Fecha inicio [{ts_inicio.isoformat()[:19]}]: ").strip()
    entrada_fin    = input(f"  Fecha fin    [{ts_fin.isoformat()[:19]}]: ").strip()
    ini = entrada_inicio if entrada_inicio else ts_inicio.isoformat()
    fin = entrada_fin    if entrada_fin    else ts_fin.isoformat()
    return ini, fin


def formatear_estado_trafico(estado: str) -> str:
    colores = {
        "NORMAL":    "\033[92m",   # Verde
        "MODERADO":  "\033[93m",   # Amarillo
        "CONGESTION":"\033[91m",   # Rojo
        "PRIORIDAD": "\033[95m",   # Magenta
    }
    reset = "\033[0m"
    return f"{colores.get(estado, '')}{estado}{reset}"


def mostrar_estado_interseccion(resp: dict):
    if not resp or not resp.get("ok"):
        print(f"  ⚠  Error: {resp.get('error', 'Sin respuesta') if resp else 'Sin respuesta'}")
        return
    d = resp.get("data", {})
    print(f"""
  ┌─ Intersección: {d.get('interseccion')}
  │  Estado:  {formatear_estado_trafico(d.get('estado_actual', '?'))}
  │  Q  (cola)        : {d.get('Q', 0):>6.1f} vehículos
  │  Vp (velocidad)   : {d.get('Vp', 0):>6.1f} km/h
  │  Cv (conteo/30s)  : {d.get('Cv', 0):>6.1f} vehículos
  │  GPS              : {d.get('nivel_gps', '?')}
  └─ Actualizado: {d.get('ts', '?')}
""")


def ejecutar_cli(analitica: ClienteAnalitica, bd: ClienteBD, config: dict):
    """Bucle principal de la CLI."""
    print("\n" + MENU)

    while True:
        try:
            opcion = input("\n  Opción > ").strip()
        except (EOFError, KeyboardInterrupt):
            print("\n  Saliendo...")
            break

        # ── Opción 1: estado de una intersección ─────────────────
        if opcion == "1":
            inter = pedir_interseccion(config)
            resp  = analitica.enviar({"tipo": "ESTADO_INTERSECCION",
                                      "interseccion": inter})
            mostrar_estado_interseccion(resp)

        # ── Opción 2: estado de todo el sistema ──────────────────
        elif opcion == "2":
            resp = analitica.enviar({"tipo": "ESTADO_SISTEMA"})
            if resp and resp.get("ok"):
                data = resp.get("data", {})
                print(f"\n  PC3 activo: {'✅' if resp.get('pc3_activo') else '❌'}")
                print(f"\n  {'Intersección':<14} {'Estado':<12} "
                      f"{'Q':>5} {'Vp':>7} {'Cv':>5}")
                print("  " + "─" * 50)
                for inter, d in sorted(data.items()):
                    est = d.get('estado_actual', '?')
                    print(
                        f"  {inter:<14} {formatear_estado_trafico(est):<20} "
                        f"{d.get('Q', 0):>5.1f} {d.get('Vp', 0):>7.1f} "
                        f"{d.get('Cv', 0):>5.1f}"
                    )
            else:
                print(f"  ⚠  Sin respuesta de la analítica")

        # ── Opción 3: historial por intersección ─────────────────
        elif opcion == "3":
            inter    = pedir_interseccion(config)
            ini, fin = pedir_rango_fechas()
            resp = bd.consultar({
                "tipo": "HISTORICO",
                "interseccion": inter,
                "ts_inicio": ini,
                "ts_fin": fin
            })
            if resp and resp.get("ok"):
                rows = resp.get("datos", [])
                print(f"\n  {len(rows)} registros para {inter} entre {ini} y {fin}")
                for r in rows[:20]:
                    print(
                        f"  [{r.get('timestamp','')[:19]}] "
                        f"{formatear_estado_trafico(r.get('estado_trafico','?')):<20} "
                        f"Q={r.get('Q',0):.0f} "
                        f"Vp={r.get('Vp',0):.1f} "
                        f"Cv={r.get('Cv',0):.0f}"
                    )
                if len(rows) > 20:
                    print(f"  ... (mostrando primeros 20 de {len(rows)})")
            else:
                print(f"  ⚠  Error en consulta: {resp}")

        # ── Opción 4: historial de congestiones ──────────────────
        elif opcion == "4":
            ini, fin = pedir_rango_fechas()
            resp = bd.consultar({
                "tipo": "CONGESTION",
                "ts_inicio": ini,
                "ts_fin": fin
            })
            if resp and resp.get("ok"):
                rows = resp.get("datos", [])
                print(f"\n  {len(rows)} congestiones registradas:")
                for r in rows[:30]:
                    print(
                        f"  [{r.get('timestamp','')[:19]}] "
                        f"{r.get('interseccion','?'):<14} "
                        f"Q={r.get('Q',0):.0f} "
                        f"Vp={r.get('Vp',0):.1f} "
                        f"Cv={r.get('Cv',0):.0f}"
                    )
            else:
                print(f"  ⚠  Error: {resp}")

        # ── Opción 5: estadísticas generales ─────────────────────
        elif opcion == "5":
            resp = bd.consultar({"tipo": "ESTADISTICAS"})
            if resp and resp.get("ok"):
                d = resp.get("datos", {})
                print(f"""
  Total eventos    : {d.get('total_eventos', 0)}
  Total congestión : {d.get('total_congestion', 0)}
  Total prioridades: {d.get('total_prioridades', 0)}
""")
            else:
                print(f"  ⚠  Error: {resp}")

        # ── Opción 6: ambulancia ──────────────────────────────────
        elif opcion == "6":
            print("\n  Ingrese las intersecciones de la ruta de la ambulancia.")
            print("  Ejemplo: INT-A1, INT-A2, INT-A3")
            entrada = input("  Intersecciones (separadas por coma): ").strip()
            vias    = [v.strip().upper() for v in entrada.split(",") if v.strip()]
            if not vias:
                print("  ⚠  No ingresó intersecciones.")
                continue
            resp = analitica.enviar({"tipo": "AMBULANCIA", "vias": vias})
            if resp and resp.get("ok"):
                print(f"  🚨 ¡Prioridad activada! Ola verde en: {vias}")
            else:
                print(f"  ⚠  Error: {resp}")

        # ── Opción 7: forzar cambio de semáforo ──────────────────
        elif opcion == "7":
            inter  = pedir_interseccion(config)
            estado = input("  Nuevo estado (VERDE/ROJO): ").strip().upper()
            if estado not in ("VERDE", "ROJO"):
                print("  ⚠  Estado inválido.")
                continue
            resp = analitica.enviar({
                "tipo": "CAMBIAR_SEMAFORO",
                "interseccion": inter,
                "nuevo_estado": estado
            })
            if resp and resp.get("ok"):
                print(f"  ✅ Semáforo {inter} cambiado a {estado}")
            else:
                print(f"  ⚠  Error: {resp}")

        # ── Opción 8: ping ────────────────────────────────────────
        elif opcion == "8":
            t0   = time.time()
            resp = analitica.enviar({"tipo": "ESTADO_SISTEMA"})
            dt   = (time.time() - t0) * 1000
            if resp:
                estado_pc3 = "✅ ACTIVO" if resp.get("pc3_activo") else "❌ CAÍDO"
                print(f"  Analítica PC2: ✅ responde ({dt:.1f} ms)")
                print(f"  PC3:           {estado_pc3}")
            else:
                print(f"  Analítica PC2: ❌ no responde")

        elif opcion == "0":
            print("  Hasta luego.")
            break

        else:
            print("  Opción no válida.")


if __name__ == "__main__":
    cfg      = cargar_config()
    analitica = ClienteAnalitica(cfg)
    bd        = ClienteBD(cfg)

    logger.info("Servicio de Monitoreo iniciado")

    try:
        ejecutar_cli(analitica, bd, cfg)
    except KeyboardInterrupt:
        print("\n  Interrumpido.")
    finally:
        analitica.cerrar()
        bd.cerrar()
