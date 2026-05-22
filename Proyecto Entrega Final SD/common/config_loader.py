"""
common/config_loader.py
Carga y valida la configuración del sistema desde config/config.json.
"""
import json
import os

_CONFIG_PATH = os.path.join(os.path.dirname(__file__), '..', 'config', 'config.json')
_config_cache = None


def cargar_config(path: str = None) -> dict:
    """
    Carga la configuración del sistema.
    Si ya fue cargada previamente, retorna la versión en caché.
    """
    global _config_cache
    if _config_cache is None:
        ruta = path or _CONFIG_PATH
        ruta = os.path.abspath(ruta)
        if not os.path.exists(ruta):
            raise FileNotFoundError(f"No se encontró el archivo de configuración: {ruta}")
        with open(ruta, 'r', encoding='utf-8') as f:
            _config_cache = json.load(f)
    return _config_cache


def obtener_intersecciones(config: dict) -> list:
    """
    Genera la lista de todas las intersecciones de la ciudad.
    Ejemplo de retorno: ['INT-A1', 'INT-A2', ..., 'INT-E5']
    """
    intersecciones = []
    for fila in config['ciudad']['filas']:
        for col in config['ciudad']['columnas']:
            intersecciones.append(f"INT-{fila}{col}")
    return intersecciones


def obtener_ip(config: dict, pc: str) -> str:
    """Retorna la IP del PC indicado. pc puede ser 'PC1', 'PC2' o 'PC3'."""
    return config['red'][f"{pc}_IP"]


def obtener_puerto(config: dict, nombre: str) -> int:
    """Retorna el puerto por nombre definido en config."""
    return config['red']['puertos'][nombre]
