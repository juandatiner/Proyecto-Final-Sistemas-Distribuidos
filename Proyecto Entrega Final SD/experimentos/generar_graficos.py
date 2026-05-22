"""
experimentos/generar_graficos.py
=====================================================================
Toma los CSV de las 4 corridas (A_simple, A_multihilo, B_simple,
B_multihilo) producidos por medir_experimento.py y genera:

  - tabla1_completa.csv     ← Tabla 1 del enunciado, lista para copiar.
  - g1_variable1.png        ← V1 (eventos en 2 min) por escenario y diseño.
  - g2_variable2.png        ← V2 (latencia ambulancia) por escenario y diseño.
  - g3_escalabilidad.png    ← Comparación visual de escalabilidad.

Uso:
    python generar_graficos.py
    # busca CSVs en experimentos/resultados/
    # imprime la tabla por stdout y guarda los PNGs
=====================================================================
"""
import csv
import json
import os
import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np


SCRIPT_DIR = Path(__file__).resolve().parent
RESULTADOS = SCRIPT_DIR / "resultados"


def cargar_csv(nombre: str) -> dict:
    ruta = RESULTADOS / f"{nombre}.csv"
    if not ruta.exists():
        # Si falta una corrida, advertir y devolver placeholders
        print(f"⚠ {ruta} no existe — usando 0 como placeholder")
        return {"v1": 0, "v2_prom": 0, "v2_min": 0, "v2_max": 0}
    with ruta.open() as f:
        rows = list(csv.DictReader(f))
    if not rows:
        print(f"⚠ {ruta} sin filas")
        return {"v1": 0, "v2_prom": 0, "v2_min": 0, "v2_max": 0}
    r = rows[0]
    return {
        "v1":      int(r.get("v1_delta_eventos", 0) or 0),
        "v2_prom": float(r.get("v2_promedio_ms", 0) or 0),
        "v2_min":  float(r.get("v2_min_ms", 0) or 0),
        "v2_max":  float(r.get("v2_max_ms", 0) or 0),
    }


def main():
    A_S = cargar_csv("A_simple")
    A_M = cargar_csv("A_multihilo")
    B_S = cargar_csv("B_simple")
    B_M = cargar_csv("B_multihilo")

    # ── Tabla 1 consolidada ──────────────────────────────────────
    tabla = [
        ["Escenario", "Diseño",
         "V1 (eventos en 2 min)",
         "V2 prom (ms)", "V2 min (ms)", "V2 max (ms)"],
        ["A: 1 sensor c/10s", "Simple",
         A_S["v1"], round(A_S["v2_prom"], 2),
         round(A_S["v2_min"], 2), round(A_S["v2_max"], 2)],
        ["A: 1 sensor c/10s", "Multihilo",
         A_M["v1"], round(A_M["v2_prom"], 2),
         round(A_M["v2_min"], 2), round(A_M["v2_max"], 2)],
        ["B: 2 sensores c/5s", "Simple",
         B_S["v1"], round(B_S["v2_prom"], 2),
         round(B_S["v2_min"], 2), round(B_S["v2_max"], 2)],
        ["B: 2 sensores c/5s", "Multihilo",
         B_M["v1"], round(B_M["v2_prom"], 2),
         round(B_M["v2_min"], 2), round(B_M["v2_max"], 2)],
    ]

    print("\n" + "═" * 70)
    print("  TABLA 1 — CONSOLIDADA")
    print("═" * 70)
    for fila in tabla:
        print("  " + " | ".join(str(x).ljust(20) for x in fila))
    print("═" * 70)

    # Guardar tabla CSV
    with (RESULTADOS / "tabla1_completa.csv").open("w", newline="") as f:
        w = csv.writer(f)
        for fila in tabla:
            w.writerow(fila)

    # ── Gráfico 1: V1 ────────────────────────────────────────────
    escenarios = ["A: 1 sensor / 10s", "B: 2 sensores / 5s"]
    v1_simple    = [A_S["v1"], B_S["v1"]]
    v1_multihilo = [A_M["v1"], B_M["v1"]]
    x = np.arange(len(escenarios))
    w_bar = 0.35

    fig, ax = plt.subplots(figsize=(8, 4.5))
    b1 = ax.bar(x - w_bar/2, v1_simple,    w_bar, label="Diseño Simple",
                color="#4C72B0", edgecolor="white")
    b2 = ax.bar(x + w_bar/2, v1_multihilo, w_bar, label="Diseño Multihilo",
                color="#DD8452", edgecolor="white")
    ax.set_ylabel("Eventos almacenados en BD principal (2 min)")
    ax.set_title("Variable 1 — Throughput de la BD por escenario")
    ax.set_xticks(x)
    ax.set_xticklabels(escenarios)
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    for bars in (b1, b2):
        ax.bar_label(bars, padding=3, fmt="%d")
    fig.tight_layout()
    fig.savefig(RESULTADOS / "g1_variable1.png", dpi=130)
    plt.close(fig)

    # ── Gráfico 2: V2 ────────────────────────────────────────────
    v2_simple    = [A_S["v2_prom"], B_S["v2_prom"]]
    v2_multihilo = [A_M["v2_prom"], B_M["v2_prom"]]
    err_simple   = [(A_S["v2_prom"] - A_S["v2_min"], A_S["v2_max"] - A_S["v2_prom"]),
                    (B_S["v2_prom"] - B_S["v2_min"], B_S["v2_max"] - B_S["v2_prom"])]
    err_multi    = [(A_M["v2_prom"] - A_M["v2_min"], A_M["v2_max"] - A_M["v2_prom"]),
                    (B_M["v2_prom"] - B_M["v2_min"], B_M["v2_max"] - B_M["v2_prom"])]
    err_simple = np.array(err_simple).T
    err_multi  = np.array(err_multi).T

    fig, ax = plt.subplots(figsize=(8, 4.5))
    b1 = ax.bar(x - w_bar/2, v2_simple,    w_bar, yerr=err_simple,
                color="#4C72B0", edgecolor="white", label="Diseño Simple",
                capsize=4)
    b2 = ax.bar(x + w_bar/2, v2_multihilo, w_bar, yerr=err_multi,
                color="#DD8452", edgecolor="white", label="Diseño Multihilo",
                capsize=4)
    ax.set_ylabel("Latencia comando ambulancia (ms)")
    ax.set_title("Variable 2 — Latencia Usuario → Semáforo (REQ/REP)")
    ax.set_xticks(x)
    ax.set_xticklabels(escenarios)
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    for bars in (b1, b2):
        ax.bar_label(bars, padding=4, fmt="%.2f ms")
    fig.tight_layout()
    fig.savefig(RESULTADOS / "g2_variable2.png", dpi=130)
    plt.close(fig)

    # ── Gráfico 3: Escalabilidad (delta cuando aumenta carga) ───
    def safe_pct(viejo, nuevo):
        return ((nuevo - viejo) / viejo * 100) if viejo > 0 else 0
    delta_v1_s = safe_pct(A_S["v1"], B_S["v1"])
    delta_v1_m = safe_pct(A_M["v1"], B_M["v1"])
    delta_v2_s = safe_pct(A_S["v2_prom"], B_S["v2_prom"])
    delta_v2_m = safe_pct(A_M["v2_prom"], B_M["v2_prom"])

    fig, ax = plt.subplots(figsize=(8, 4.5))
    metricas = ["Δ V1 (eventos)", "Δ V2 (latencia)"]
    delta_simple = [delta_v1_s, delta_v2_s]
    delta_multi  = [delta_v1_m, delta_v2_m]
    x2 = np.arange(len(metricas))
    b1 = ax.bar(x2 - w_bar/2, delta_simple, w_bar,
                color="#4C72B0", edgecolor="white", label="Simple")
    b2 = ax.bar(x2 + w_bar/2, delta_multi, w_bar,
                color="#DD8452", edgecolor="white", label="Multihilo")
    ax.set_ylabel("Cambio porcentual A → B")
    ax.set_title("Escalabilidad: variación al aumentar la carga (1→2 sensores, 10s→5s)")
    ax.set_xticks(x2)
    ax.set_xticklabels(metricas)
    ax.axhline(0, color="black", linewidth=0.8)
    ax.legend()
    ax.grid(axis="y", linestyle=":", alpha=0.5)
    for bars in (b1, b2):
        ax.bar_label(bars, padding=3, fmt="%+.1f%%")
    fig.tight_layout()
    fig.savefig(RESULTADOS / "g3_escalabilidad.png", dpi=130)
    plt.close(fig)

    # ── Resumen JSON para uso del informe ───────────────────────
    resumen = {
        "tabla1": tabla,
        "delta_v1_simple_pct":    round(delta_v1_s, 2),
        "delta_v1_multihilo_pct": round(delta_v1_m, 2),
        "delta_v2_simple_pct":    round(delta_v2_s, 2),
        "delta_v2_multihilo_pct": round(delta_v2_m, 2),
        "datos_crudos": {"A_simple": A_S, "A_multihilo": A_M,
                         "B_simple": B_S, "B_multihilo": B_M},
    }
    with (RESULTADOS / "resumen_analisis.json").open("w", encoding="utf-8") as f:
        json.dump(resumen, f, indent=2, ensure_ascii=False)

    print(f"\n✅ Gráficos y tabla guardados en: {RESULTADOS}")


if __name__ == "__main__":
    main()
