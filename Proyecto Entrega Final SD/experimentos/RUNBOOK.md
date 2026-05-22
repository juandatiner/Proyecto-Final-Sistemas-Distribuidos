# Runbook — Experimentos de Rendimiento (Tabla 1)

Sigue estos pasos en orden. Total estimado: **40 minutos** (4 corridas × ~3 min de setup + 2 min de medición + 5 min de análisis).

## Pre-requisitos en cada PC

1. El proyecto `trafico_urbano/` está copiado en `~/trafico_urbano` en las 3 PCs.
2. Esta carpeta `experimentos/` está dentro del proyecto (sincronizar con scp/git/rsync).
3. Las IPs en `config/config.json` son las correctas:
   - PC1: `10.43.99.128`
   - PC2: `10.43.100.144`
   - PC3: `10.43.100.90`

Antes de empezar, ejecuta una vez en cada PC:
```bash
cd ~/trafico_urbano
chmod +x experimentos/*.sh
```

---

## Las 4 corridas

| # | Escenario | Diseño     | Comando |
|---|-----------|------------|---------|
| 1 | A         | simple     | `--escenario A --diseno simple`    |
| 2 | A         | multihilo  | `--escenario A --diseno multihilo` |
| 3 | B         | simple     | `--escenario B --diseno simple`    |
| 4 | B         | multihilo  | `--escenario B --diseno multihilo` |

**Escenario A**: 1 sensor de cada tipo (cám + esp + gps en INT-C5), intervalo 10s.
**Escenario B**: 2 sensores de cada tipo (intersecciones C5 y B3), intervalo 5s.

---

## Procedimiento por corrida

> Repetir 4 veces, una por cada fila de la tabla anterior. Sustituir `XX_YY` por la combinación correspondiente, ej.: `A_simple`, `A_multihilo`, `B_simple`, `B_multihilo`.

### Paso 1 — Abre 3 terminales

Cada una conectada por SSH a una PC distinta. Mantén las 3 terminales abiertas durante toda la corrida.

```bash
# Terminal 1
ssh usuario_pc1@10.43.99.128

# Terminal 2
ssh usuario_pc2@10.43.100.144

# Terminal 3
ssh usuario_pc3@10.43.100.90
```

### Paso 2 — Lanzar PC3 (PRIMERO)

En la terminal de PC3:
```bash
cd ~/trafico_urbano
bash experimentos/lanzar_pc3_experimento.sh --escenario A --diseno simple
```

⚠️ **No cierres este comando** hasta que termine. Bloquea la terminal por ~2.5 min.

### Paso 3 — Lanzar PC2 (mientras PC3 está esperando)

PC3 imprime "Esperando 8s para sincronizar..." → en ese momento corre en PC2:
```bash
cd ~/trafico_urbano
bash experimentos/lanzar_pc2_experimento.sh --escenario A --diseno simple
```

Vuelve al prompt en ~2 segundos (deja los procesos en background).

### Paso 4 — Lanzar PC1 (último)

Inmediatamente después, en PC1:
```bash
cd ~/trafico_urbano
bash experimentos/lanzar_pc1_experimento.sh --escenario A --diseno simple
```

Vuelve al prompt en ~2 segundos.

### Paso 5 — Esperar a que PC3 termine

PC3 medirá V1 (durante 120s) y luego V2 (~10s extra).
Al terminar imprimirá:
```
✅ Experimento A_simple completado. Resultados en experimentos/resultados/
```

### Paso 6 — Limpiar para la siguiente corrida

En PC1 y PC2:
```bash
pkill -f 'broker_zmq.py|sensor_camara.py|sensor_espira.py|sensor_gps.py'   # solo PC1
pkill -f 'base_datos_replica.py|servicio_semaforos.py|servicio_analitica.py'  # solo PC2
```

Repite los pasos 2–6 para los siguientes 3 experimentos cambiando los flags `--escenario` y `--diseno`.

---

## Recolectar los resultados

Al terminar las 4 corridas, en PC3:
```bash
cd ~/trafico_urbano
tar czf resultados_experimentos.tar.gz experimentos/resultados/ logs/
```

Mándale a Claude el archivo `resultados_experimentos.tar.gz` y los siguientes datos del HW de las 3 PCs (cópialos del comando `lscpu` y `free -h`):

```bash
echo "=== HW PC$(hostname) ==="
lscpu | head -20
free -h
uname -a
python3 --version
pip show pyzmq | grep -E "Name|Version"
```

---

## Troubleshooting

- **PC2 dice "Address already in use"**: queda un proceso anterior. Corre `pkill -f 'servicio_'` y reintenta.
- **PC3 imprime delta=0 eventos**: PC1 no llegó a publicar. Verifica IP en config.json y que PC1 ejecutó sin errores.
- **V2 timeout**: PC2 (analítica) no está corriendo o el firewall bloquea el puerto 5560.
- **Latencia V2 muy alta (>100ms)**: la red está saturada o hay otro proceso pesado. Repite la corrida.

---

## ¿Qué pasa por dentro?

`medir_experimento.py` (corre en PC3):
1. Toma un snapshot del `COUNT(*)` en `eventos_sensores` de la BD principal.
2. Espera 120s (imprimiendo progreso cada 30s).
3. Toma otro snapshot. La diferencia es **Variable 1**.
4. Abre socket REQ contra `PC2_IP:5560` (analítica).
5. Envía 5 comandos `AMBULANCIA` con `time.perf_counter()` antes y después.
6. La latencia promedio es **Variable 2**.
7. Guarda CSV + JSON + log en `experimentos/resultados/`.
