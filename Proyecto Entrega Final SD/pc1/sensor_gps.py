"""
pc1/sensor_gps.py
=====================================================================
Sensor GPS - PC1
Genera EVENTO_DENSIDAD_DE_TRAFICO (Dt):
  - nivel_congestion  : "ALTA" | "NORMAL" | "BAJA"
  - velocidad_promedio: velocidad GPS promedio en la vía (km/h)

Regla de clasificación (según enunciado):
  ALTA   → velocidad_promedio < 10 km/h
  NORMAL → 11 ≤ velocidad_promedio ≤ 39 km/h
  BAJA   → velocidad_promedio > 40 km/h

Publica al broker ZeroMQ con topic "gps".

Uso:
    python sensor_gps.py --interseccion INT-D2
    python sensor_gps.py --interseccion INT-D2 --intervalo 10
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

TOPIC = "gps"


def clasificar_congestion(velocidad: float) -> str:
    """
    Clasifica el nivel de congestión según la velocidad promedio GPS.

    Args:
        velocidad : velocidad promedio en km/h

    Returns:
        'ALTA', 'NORMAL' o 'BAJA'
    """
    if velocidad < 10:
        return "ALTA"
    elif velocidad <= 39:
        return "NORMAL"
    else:
        return "BAJA"


def generar_evento(sensor_id: str, interseccion: str) -> dict:
    """
    Genera un evento GPS simulado.

    Simula variación realista de velocidad usando una distribución
    que modela condiciones urbanas típicas.

    Args:
        sensor_id    : identificador del sensor (e.g. 'GPS-D2')
        interseccion : identificador de intersección

    Returns:
        Dict con el evento GPS.
    """
    # Velocidad base con variación aleatoria realista (distribución beta escalada)
    # Rango realista: 5–50 km/h en entorno urbano
    velocidad_base = random.betavariate(2, 2) * 45 + 5
    velocidad = round(velocidad_base + random.uniform(-3, 3), 1)
    velocidad = max(2.0, min(50.0, velocidad))  # Clamp al rango válido

    nivel = clasificar_congestion(velocidad)

    return {
        "sensor_id": sensor_id,
        "tipo_sensor": "gps",
        "interseccion": interseccion,
        "nivel_congestion": nivel,
        "velocidad_promedio": velocidad,
        "timestamp": datetime.now(timezone.utc).isoformat()
    }


def ejecutar_sensor(interseccion: str, intervalo: float, config: dict) -> None:
    """
    Bucle principal del sensor GPS.

    Args:
        interseccion : id de la intersección (e.g. 'INT-D2')
        intervalo    : segundos entre eventos
        config       : configuración del sistema
    """
    sensor_id = f"GPS-{interseccion.replace('INT-', '')}"
    logger = configurar_logger(sensor_id)

    pc1_ip = config['red']['PC1_IP']
    puerto = config['red']['puertos']['broker_sub']

    ctx = zmq.Context()
    sock = ctx.socket(zmq.PUB)
    sock.connect(f"tcp://{pc1_ip}:{puerto}")
    time.sleep(0.5)

    logger.info(f"Sensor iniciado | intersección={interseccion} | "
                f"intervalo={intervalo}s | broker={pc1_ip}:{puerto}")

    try:
        while True:
            evento = generar_evento(sensor_id, interseccion)
            payload = json.dumps(evento).encode('utf-8')
            sock.send_multipart([TOPIC.encode(), payload])

            logger.info(
                f"[Publicado] velocidad={evento['velocidad_promedio']:>5.1f} km/h | "
                f"nivel={evento['nivel_congestion']}"
            )

            time.sleep(intervalo)

    except KeyboardInterrupt:
        logger.info("Sensor detenido por el usuario.")
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor GPS de Tráfico")
    parser.add_argument(
        "--interseccion", required=True,
        help="Identificador de intersección, e.g. INT-D2"
    )
    parser.add_argument(
        "--intervalo", type=float, default=None,
        help="Segundos entre eventos (default: valor en config.json)"
    )
    args = parser.parse_args()

    cfg = cargar_config()
    intervalo_cfg = cfg['sensores']['intervalo_gps_seg']
    intervalo = args.intervalo if args.intervalo else intervalo_cfg

    ejecutar_sensor(args.interseccion, intervalo, cfg)
