"""
common/logger.py
Configura un logger estándar con formato legible para todos los servicios.
"""
import logging
import sys
from datetime import datetime


def configurar_logger(nombre: str, nivel: int = logging.INFO) -> logging.Logger:
    """
    Crea y configura un logger con el nombre del servicio dado.

    Args:
        nombre: Nombre del servicio (e.g. 'BrokerZMQ', 'Analitica')
        nivel:  Nivel de logging (default: INFO)

    Returns:
        Logger configurado.
    """
    logger = logging.getLogger(nombre)
    logger.setLevel(nivel)

    if not logger.handlers:
        handler = logging.StreamHandler(sys.stdout)
        handler.setLevel(nivel)
        fmt = logging.Formatter(
            fmt='%(asctime)s | %(levelname)-8s | %(name)-20s | %(message)s',
            datefmt='%Y-%m-%dT%H:%M:%S'
        )
        handler.setFormatter(fmt)
        logger.addHandler(handler)

    return logger
