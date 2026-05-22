"""
pc1/sensor_camara.py
=====================================================================
Sensor de tipo CÁMARA - PC1
Genera EVENTO_LONGITUD_COLA (Lq):
  - volumen         : número de vehículos en espera del semáforo
  - velocidad_promedio: velocidad en la intersección (km/h)

Publica al broker ZeroMQ con topic "camara".

Uso:
    python sensor_camara.py --interseccion INT-C5
    python sensor_camara.py --interseccion INT-A1 --intervalo 5
=====================================================================
"""
import zmq
import json
import time
import random
import argparse
import sys
import os
from datetime import datetime, timezone

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

TOPIC = "camara"


def generar_evento(sensor_id: str, interseccion: str,
                   estado_semaforo: str = "ROJO") -> dict:
    """
    Genera un evento de cámara simulado.

    En estado ROJO: mayor acumulación de vehículos (volumen 0-25).
    En estado VERDE: flujo de vehículos (volumen 0-8).

    Args:
        sensor_id     : identificador del sensor (e.g. 'CAM-C5')
        interseccion  : identificador de intersección (e.g. 'INT-C5')
        estado_semaforo: 'ROJO' o 'VERDE' (afecta distribución de variables)

    Returns:
        Dict con el evento de cámara.
    """
    if estado_semaforo == "ROJO":
        # En rojo se acumulan vehículos
        volumen = random.randint(0, 25)
        # A mayor volumen, menor velocidad promedio
        velocidad = max(5, 50 - volumen * 1.5 + random.uniform(-3, 3))
    else:
        # En verde los vehículos fluyen
        volumen = random.randint(0, 8)
        velocidad = random.uniform(25, 50)

    return {
        "sensor_id": sensor_id,
        "tipo_sensor": "camara",
        "interseccion": interseccion,
        "volumen": volumen,
        "velocidad_promedio": round(velocidad, 1),
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def ejecutar_sensor(interseccion: str, intervalo: float, config: dict) -> None:
    """
    Bucle principal del sensor de cámara.
    Publica eventos al broker en PC1.

    Args:
        interseccion : id de la intersección monitorizada (e.g. 'INT-C5')
        intervalo    : segundos entre eventos
        config       : configuración del sistema
    """
    sensor_id = f"CAM-{interseccion.replace('INT-', '')}"
    logger = configurar_logger(sensor_id)

    pc1_ip = config['red']['PC1_IP']
    puerto = config['red']['puertos']['broker_sub']

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.connect(f"tcp://{pc1_ip}:{puerto}")

    # Pequeña pausa para que la conexión se establezca
    time.sleep(0.5)

    logger.info(f"Sensor iniciado | intersección={interseccion} | "
                f"intervalo={intervalo}s | broker={pc1_ip}:{puerto}")

    # Simula el estado del semáforo local (se actualiza por comandos externos en versión completa)
    estado_semaforo = "ROJO"
    ciclo = 0

    try:
        while True:
            ciclo += 1
            # Alterna estado para simulación realista
            if ciclo % 6 == 0:
                estado_semaforo = "VERDE" if estado_semaforo == "ROJO" else "ROJO"

            evento = generar_evento(sensor_id, interseccion, estado_semaforo)
            payload = json.dumps(evento).encode('utf-8')

            # Enviar con topic como primer frame (PUB/SUB en ZMQ usa prefijo de topic)
            sock.send_multipart([TOPIC.encode(), payload])

            logger.info(
                f"[Publicado] vol={evento['volumen']:>3} veh | "
                f"vel={evento['velocidad_promedio']:>5.1f} km/h | "
                f"semáforo={estado_semaforo}"
            )

            time.sleep(intervalo)

    except KeyboardInterrupt:
        logger.info("Sensor detenido por el usuario.")
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor de Cámara de Tráfico")
    parser.add_argument(
        "--interseccion", required=True,
        help="Identificador de intersección, e.g. INT-C5"
    )
    parser.add_argument(
        "--intervalo", type=float, default=None,
        help="Segundos entre eventos (default: valor en config.json)"
    )
    args = parser.parse_args()

    cfg = cargar_config()
    intervalo_cfg = cfg['sensores']['intervalo_camara_seg']
    intervalo = args.intervalo if args.intervalo else intervalo_cfg

    ejecutar_sensor(args.interseccion, intervalo, cfg)
