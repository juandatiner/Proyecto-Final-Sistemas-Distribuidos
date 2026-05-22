"""
experimentos/medir_experimento.py
=====================================================================
Script de medición para los experimentos de la Tabla 1 del enunciado.

Ejecuta una corrida de DURACION segundos midiendo:
  Variable 1 - Cantidad de registros almacenados en BD principal en
               un intervalo de DURACION (default 120s = 2 min).
  Variable 2 - Tiempo desde que el usuario solicita ACTIVAR PRIORIDAD
               (ambulancia) hasta que llega la confirmación de que el
               semáforo cambió (REQ/REP a la analítica).

Pre-condiciones:
  * El sistema completo (PC1, PC2, PC3) está corriendo desde hace
    al menos 5 segundos (sensores publicando, analítica suscrita,
    BDs aceptando datos).
  * Este script se corre en el PC3 (donde está la BD principal).

Uso:
    python medir_experimento.py --escenario A --diseno simple
    python medir_experimento.py --escenario B --diseno multihilo \
        --duracion 120 --num_muestras_v2 5

Salida:
  CSV en  experimentos/resultados/{escenario}_{diseno}.csv
  JSON en experimentos/resultados/{escenario}_{diseno}.json
=====================================================================
"""
import argparse
import csv
import json
import os
import sqlite3
import sys
import time
from datetime import datetime, timezone

import zmq

# Para reutilizar el config loader del proyecto
sys.path.insert(0, os.path.dirname(os.path.dirname(os.path.abspath(__file__))))
from common.config_loader import cargar_config


def contar_eventos_bd(ruta_bd: str) -> int:
    """
    Cuenta filas en la tabla eventos_sensores de la BD principal/réplica.
    Si la BD no existe aún, retorna 0.
    """
    if not os.path.exists(ruta_bd):
        return 0
    conn = sqlite3.connect(ruta_bd)
    try:
        cur = conn.cursor()
        cur.execute("SELECT COUNT(*) FROM eventos_sensores")
        return cur.fetchone()[0]
    finally:
        conn.close()


def medir_variable_1(ruta_bd: str, duracion_seg: int, logger_print) -> dict:
    """
    Mide V1: cantidad de eventos almacenados en BD en `duracion_seg`.
    Retorna inicio, fin, delta y tasa por segundo.
    """
    logger_print(f"[V1] Snapshot inicial de la BD: {ruta_bd}")
    t_inicio = time.time()
    n_inicio = contar_eventos_bd(ruta_bd)
    logger_print(f"[V1] Eventos al inicio: {n_inicio:,}")

    logger_print(f"[V1] Esperando {duracion_seg}s ...")
    # Imprimir progreso cada 30s
    pasos = max(1, duracion_seg // 4)
    for i in range(0, duracion_seg, pasos):
        time.sleep(min(pasos, duracion_seg - i))
        n_parcial = contar_eventos_bd(ruta_bd)
        logger_print(f"[V1]   t+{i+pasos:3d}s -> {n_parcial:,} eventos "
                     f"(+{n_parcial - n_inicio:,})")

    t_fin = time.time()
    n_fin = contar_eventos_bd(ruta_bd)
    duracion_real = t_fin - t_inicio
    delta = n_fin - n_inicio
    tasa = delta / duracion_real if duracion_real > 0 else 0

    logger_print(f"[V1] Final: {n_fin:,} eventos. "
                 f"Δ = {delta:,} en {duracion_real:.1f}s ({tasa:.2f} reg/s)")

    return {
        "n_eventos_inicio":   n_inicio,
        "n_eventos_fin":      n_fin,
        "delta_eventos":      delta,
        "duracion_segundos":  duracion_real,
        "tasa_reg_por_seg":   round(tasa, 3),
    }


def medir_variable_2(ip_pc2: str, puerto_rep: int, num_muestras: int,
                     intersecciones_amb: list, logger_print) -> dict:
    """
    Mide V2: tiempo desde que el usuario emite el comando AMBULANCIA
    hasta que la analítica confirma que envió el cambio de semáforo.
    Toma `num_muestras` mediciones y devuelve estadísticas.
    """
    ctx = zmq.Context()
    sock = ctx.socket(zmq.REQ)
    sock.RCVTIMEO = 5000  # 5s timeout
    sock.SNDTIMEO = 5000
    sock.connect(f"tcp://{ip_pc2}:{puerto_rep}")
    logger_print(f"[V2] Conectado a analítica en {ip_pc2}:{puerto_rep}")

    tiempos_ms = []
    for i in range(num_muestras):
        # Alternar las vías para no usar siempre la misma
        vias = intersecciones_amb[i % len(intersecciones_amb)]
        comando = {"tipo": "AMBULANCIA", "vias": vias}
        try:
            t0 = time.perf_counter()
            sock.send(json.dumps(comando).encode("utf-8"))
            respuesta_raw = sock.recv()
            t1 = time.perf_counter()
            dt_ms = (t1 - t0) * 1000.0
            respuesta = json.loads(respuesta_raw.decode("utf-8"))
            ok = respuesta.get("ok", False)
            tiempos_ms.append(dt_ms)
            logger_print(f"[V2] muestra {i+1}/{num_muestras}: "
                         f"{dt_ms:6.2f} ms  ok={ok}  vias={vias}")
            time.sleep(2)  # pequeño espaciado entre solicitudes
        except zmq.error.Again:
            logger_print(f"[V2] muestra {i+1}: TIMEOUT")
        except Exception as exc:
            logger_print(f"[V2] muestra {i+1}: error {exc}")

    sock.close(0)
    ctx.term()

    if not tiempos_ms:
        return {"error": "no se obtuvieron muestras"}

    promedio = sum(tiempos_ms) / len(tiempos_ms)
    return {
        "muestras":      tiempos_ms,
        "promedio_ms":   round(promedio, 3),
        "minimo_ms":     round(min(tiempos_ms), 3),
        "maximo_ms":     round(max(tiempos_ms), 3),
        "n_muestras":    len(tiempos_ms),
    }


def main():
    parser = argparse.ArgumentParser(
        description="Medición de Variable 1 y Variable 2 (Tabla 1 del enunciado)"
    )
    parser.add_argument("--escenario", required=True, choices=["A", "B"],
                        help="A=1 sensor c/10s, B=2 sensores c/5s")
    parser.add_argument("--diseno", required=True,
                        choices=["simple", "multihilo"],
                        help="Diseño del broker ZMQ")
    parser.add_argument("--duracion", type=int, default=120,
                        help="Duración en segundos para V1 (default 120s)")
    parser.add_argument("--num_muestras_v2", type=int, default=5,
                        help="Número de mediciones de V2 (default 5)")
    parser.add_argument("--bd",
                        default=os.path.join(
                            os.path.dirname(os.path.dirname(os.path.abspath(__file__))),
                            "pc3", "trafico_principal.db"),
                        help="Ruta a la BD principal (PC3)")
    parser.add_argument("--salida_dir",
                        default=os.path.join(
                            os.path.dirname(os.path.abspath(__file__)),
                            "resultados"),
                        help="Directorio donde guardar CSV/JSON")
    args = parser.parse_args()

    cfg = cargar_config()
    ip_pc2 = cfg["red"]["PC2_IP"]
    puerto_rep = cfg["red"]["puertos"]["analitica_rep"]

    # Vías alternables para AMBULANCIA (3 ó 5 vías por solicitud)
    rutas_amb = [
        ["INT-A1", "INT-B1", "INT-C1"],
        ["INT-A2", "INT-B2", "INT-C2", "INT-D2", "INT-E2"],
        ["INT-C3", "INT-C4", "INT-C5"],
        ["INT-A1", "INT-B2", "INT-C3", "INT-D4", "INT-E5"],
        ["INT-E1", "INT-E2", "INT-E3"],
    ]

    os.makedirs(args.salida_dir, exist_ok=True)
    nombre_corrida = f"{args.escenario}_{args.diseno}"
    archivo_log = os.path.join(args.salida_dir, f"{nombre_corrida}.log")

    fh_log = open(archivo_log, "w", encoding="utf-8")

    def log(msg: str) -> None:
        linea = f"{datetime.now().isoformat(timespec='seconds')} | {msg}"
        print(linea, flush=True)
        fh_log.write(linea + "\n")
        fh_log.flush()

    log("=" * 60)
    log(f"EXPERIMENTO {nombre_corrida.upper()}")
    log(f"  escenario       : {args.escenario}")
    log(f"  diseño broker   : {args.diseno}")
    log(f"  duración V1     : {args.duracion}s")
    log(f"  muestras V2     : {args.num_muestras_v2}")
    log(f"  BD principal    : {args.bd}")
    log(f"  PC2 (analítica) : {ip_pc2}:{puerto_rep}")
    log("=" * 60)

    # Pequeña espera para que el sistema se estabilice
    log("Esperando 5s para estabilización del sistema...")
    time.sleep(5)

    # ── Medir Variable 1 ─────────────────────────────────────────
    v1 = medir_variable_1(args.bd, args.duracion, log)

    # ── Medir Variable 2 ─────────────────────────────────────────
    v2 = medir_variable_2(ip_pc2, puerto_rep, args.num_muestras_v2,
                          rutas_amb, log)

    # ── Persistir resultados ─────────────────────────────────────
    resultado = {
        "escenario":       args.escenario,
        "diseno":          args.diseno,
        "timestamp":       datetime.now(timezone.utc).isoformat(),
        "variable_1":      v1,
        "variable_2":      v2,
    }

    archivo_json = os.path.join(args.salida_dir, f"{nombre_corrida}.json")
    with open(archivo_json, "w", encoding="utf-8") as f:
        json.dump(resultado, f, indent=2, ensure_ascii=False)

    archivo_csv = os.path.join(args.salida_dir, f"{nombre_corrida}.csv")
    with open(archivo_csv, "w", newline="", encoding="utf-8") as f:
        w = csv.writer(f)
        w.writerow(["escenario", "diseno",
                    "v1_delta_eventos", "v1_duracion_s", "v1_tasa_reg_s",
                    "v2_promedio_ms", "v2_min_ms", "v2_max_ms", "v2_n"])
        w.writerow([args.escenario, args.diseno,
                    v1["delta_eventos"], round(v1["duracion_segundos"], 2),
                    v1["tasa_reg_por_seg"],
                    v2.get("promedio_ms", ""), v2.get("minimo_ms", ""),
                    v2.get("maximo_ms", ""), v2.get("n_muestras", 0)])

    log("=" * 60)
    log(f"Resultado guardado en:")
    log(f"  {archivo_json}")
    log(f"  {archivo_csv}")
    log(f"  {archivo_log}")
    log("=" * 60)
    fh_log.close()


if __name__ == "__main__":
    main()
