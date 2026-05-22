"""
pc1/sensor_espira.py
=====================================================================
Sensor ESPIRA INDUCTIVA - PC1
Genera EVENTO_CONTEO_VEHICULAR (Cv):
  - vehiculos_contados: número de vehículos que pasaron sobre la espira
    en el intervalo de 30 segundos (coincide con ciclo de semáforo).

Publica al broker ZeroMQ con topic "espira_inductiva".

Uso:
    python sensor_espira.py --interseccion INT-B3
    python sensor_espira.py --interseccion INT-B3 --intervalo 30
=====================================================================
"""
import zmq
import json
import time
import random
import argparse
import sys
import os
from datetime import datetime, timezone, timedelta

sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config
from common.logger import configurar_logger

TOPIC = "espira_inductiva"


def generar_evento(sensor_id: str, interseccion: str, intervalo_seg: int) -> dict:
    """
    Genera un evento de espira inductiva simulado.

    Simula un conteo vehicular aleatorio que varía según hora del día.
    Durante horas pico (7-9h, 17-19h) el flujo es mayor.

    Args:
        sensor_id     : identificador del sensor (e.g. 'ESP-C5')
        interseccion  : identificador de intersección
        intervalo_seg : duración del intervalo de medición en segundos

    Returns:
        Dict con el evento de espira.
    """
    hora_actual = datetime.now().hour
    # Simular hora pico con mayor densidad vehicular
    en_hora_pico = (7 <= hora_actual <= 9) or (17 <= hora_actual <= 19)

    if en_hora_pico:
        vehiculos = random.randint(8, 25)
    else:
        vehiculos = random.randint(1, 15)

    ahora = datetime.now(timezone.utc)
    inicio = ahora - timedelta(seconds=intervalo_seg)

    return {
        "sensor_id": sensor_id,
        "tipo_sensor": "espira_inductiva",
        "interseccion": interseccion,
        "vehiculos_contados": vehiculos,
        "intervalo_segundos": intervalo_seg,
        "timestamp_inicio": inicio.isoformat(),
        "timestamp_fin": ahora.isoformat()
    }


def ejecutar_sensor(interseccion: str, intervalo: float, config: dict) -> None:
    """
    Bucle principal del sensor de espira inductiva.

    Args:
        interseccion : id de la intersección (e.g. 'INT-B3')
        intervalo    : segundos entre eventos (normalmente 30s)
        config       : configuración del sistema
    """
    sensor_id = f"ESP-{interseccion.replace('INT-', '')}"
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
            evento = generar_evento(sensor_id, interseccion, int(intervalo))
            payload = json.dumps(evento).encode('utf-8')
            sock.send_multipart([TOPIC.encode(), payload])

            logger.info(
                f"[Publicado] vehículos={evento['vehiculos_contados']:>3} | "
                f"intervalo={evento['intervalo_segundos']}s"
            )

            time.sleep(intervalo)

    except KeyboardInterrupt:
        logger.info("Sensor detenido por el usuario.")
    finally:
        sock.close()
        ctx.term()


if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Sensor de Espira Inductiva")
    parser.add_argument(
        "--interseccion", required=True,
        help="Identificador de intersección, e.g. INT-B3"
    )
    parser.add_argument(
        "--intervalo", type=float, default=None,
        help="Segundos entre eventos (default: valor en config.json)"
    )
    args = parser.parse_args()

    cfg = cargar_config()
    intervalo_cfg = cfg['sensores']['intervalo_espira_seg']
    intervalo = args.intervalo if args.intervalo else intervalo_cfg

    ejecutar_sensor(args.interseccion, intervalo, cfg)
