# Gestión Inteligente de Tráfico Urbano
## Introducción a Sistemas Distribuidos — 2026-30 · **Entrega 2**

Plataforma distribuida que monitorea, analiza y reacciona ante condiciones de
tráfico urbano usando **ZeroMQ** sobre 3 máquinas (PC1, PC2, PC3). Esta
entrega añade un **dashboard web en vivo** y los **experimentos de
rendimiento** comparando el broker single-thread vs multi-thread.

---

## Contenido del repositorio

```
trafico_urbano/
├── config/config.json               IPs, puertos, reglas
├── common/                          logger, config_loader
├── pc1/                             broker + 3 tipos de sensores (PUB/SUB)
├── pc2/                             analítica + semáforos + BD réplica
├── pc3/
│   ├── base_datos_principal.py      PULL → SQLite + heartbeat PUB + REP
│   ├── servicio_monitoreo.py        CLI interactivo
│   └── dashboard/                   ★ NUEVO ★ Flask + Flask-SocketIO
├── experimentos/                    ★ NUEVO ★ scripts Tabla 1 + RUNBOOK.md
├── lanzar_pcN.sh                    arrancar cada máquina
├── lanzar_dashboard.sh              ★ NUEVO ★
├── GUION_VIDEO.md                   ★ NUEVO ★ guion para grabar
└── README.md
```

---

## Pre-requisitos

- Python 3.9+
- `pip install pyzmq flask flask-socketio`

---

## Configuración

Editar `config/config.json` con las IPs reales:

```json
"red": {
    "PC1_IP": "10.43.99.128",
    "PC2_IP": "10.43.100.144",
    "PC3_IP": "10.43.100.90"
}
```

---

## Ejecución del sistema

### Opción rápida — un solo comando desde el Mac/cliente

```bash
bash desplegar_a_pcs.sh    # (una vez) copia el proyecto a las 3 PCs
bash arrancar_todo.sh      # limpia + lanza PC3->PC2->PC1 + dashboard
# ... usar el sistema ...
bash parar_todo.sh         # apaga todo
```

Procedimiento fijo completo en `OPERACION.md`.

### Opción manual — máquina por máquina

Orden de arranque: **PC3 -> PC2 -> PC1**.

```bash
# PC3 — BD principal + monitoreo
bash lanzar_pc3.sh

# PC2 — analítica + semáforos + BD réplica
bash lanzar_pc2.sh

# PC1 — broker + 75 sensores (5x5 intersecciones)
bash lanzar_pc1.sh
# o en modo multihilo:
bash lanzar_pc1.sh --multihilo

# PC3 — Dashboard en otra terminal
bash lanzar_dashboard.sh
# Abrir http://10.43.100.90:8080
```

> **Importante**: antes de relanzar un servicio, mata el anterior para evitar
> el error "Address already in use". El script `arrancar_todo.sh` ya hace
> esa limpieza automaticamente.

### Ver el dashboard desde otra maquina

- **Tunel SSH** (funciona donde funcione el SSH, tambien fuera de la U):
  `ssh -L 8080:localhost:8080 estudiante@10.43.100.90` y abrir
  `http://localhost:8080`.
- **Escritorio remoto** a PC3 y abrir ahi `http://localhost:8080`.

### Dashboard de respaldo offline

`dashboard_demo.html` se abre con doble clic en cualquier navegador, sin
servidor: simula las 3 PCs en JavaScript. Sirve para probar el look y como
respaldo en la sustentacion si falla la red.

---

## Dashboard en vivo (entrega 2)

Mapa 5x5 con estados de trafico, contadores de BD principal y replica,
heartbeat de PC3 visible, semaforos animados, stream de eventos en bruto del
broker y controles manuales. Para activar una ambulancia se puede escribir
las vias o marcar la ruta directamente sobre el mapa: clic en la interseccion
de inicio y en la de fin, y el dashboard rellena la ruta en linea recta
(misma fila o columna), rechazando rutas diagonales — coherente con el
supuesto de vias en un solo sentido.
Comunicacion: WebSocket directo navegador <-> servidor Flask en PC3, que a su
vez se subscribe al broker ZMQ y consulta a la analitica via REQ/REP.

---

## Tolerancia a fallos

Patrón **health check con heartbeat** PUB/SUB cada 3 s. Timeout configurable
(default 9 s). Cuando PC3 cae, la analítica deja de empujar a la BD
principal y continúa solo con la réplica. La operación es **transparente**
para sensores y monitoreo. Al reiniciar PC3, la analítica reconecta
automáticamente.

---

## Reglas de tráfico

| Estado     | Condición                          | Acción                        |
|------------|------------------------------------|-------------------------------|
| NORMAL     | `Q<5 AND Vp>35 AND Cv<10`          | Reset (15 s)                  |
| MODERADO   | `Q<10 AND Vp>15 AND Cv<20`         | Extender verde +5 s           |
| CONGESTIÓN | `Q≥10 OR Vp<15 OR Cv≥20`           | Verde 30 s                    |
| PRIORIDAD  | Comando manual                     | Ola verde 60 s en la ruta     |

---

## Puertos

| Puerto | Patrón     | Flujo                              |
|--------|------------|------------------------------------|
| 5555   | XSUB       | Sensores → Broker PC1              |
| 5556   | XPUB       | Broker PC1 → Analítica PC2         |
| 5557   | PUSH/PULL  | Analítica → Semáforos              |
| 5558   | PUSH/PULL  | Analítica → BD Réplica             |
| 5559   | PUSH/PULL  | Analítica → BD Principal           |
| 5560   | REQ/REP    | Monitoreo & Dashboard → Analítica  |
| 5561   | PUB/SUB    | BD Principal → Analítica heartbeat |
| 8080   | HTTP/WS    | Dashboard → navegadores            |

---

## Experimentos de rendimiento

Ver `experimentos/RUNBOOK.md`. Cuatro corridas:

| # | Escenario | Diseño     |
|---|-----------|------------|
| 1 | A (1 sensor c/10s)  | simple     |
| 2 | A (1 sensor c/10s)  | multihilo  |
| 3 | B (2 sensores c/5s) | simple     |
| 4 | B (2 sensores c/5s) | multihilo  |

Variables medidas: V1 = eventos en BD en 2 min; V2 = latencia de comando
ambulancia. Resultados en `Informe_Rendimiento.docx`.
