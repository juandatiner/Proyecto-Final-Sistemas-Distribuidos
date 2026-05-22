# Gestión Inteligente de Tráfico Urbano
### Sistema Distribuido con ZeroMQ — Entregas 2

**Universidad:** Pontificia Universidad Javeriana · Facultad de Ingeniería · Departamento de Ingeniería de Sistemas  
**Curso:** Introducción a los Sistemas Distribuidos — Periodo 2026-30  
**Equipo:** Juan David Rincón · Juan David Daza · Juan Carlos Santamaría Orjuela · Juan Felipe Gutiérrez Adarme

---

## Resumen ejecutivo

Plataforma distribuida en tres nodos físicos que monitorea, analiza y controla el tráfico de una ciudad simulada con **25 intersecciones** (cuadrícula 5×5). Cada intersección está equipada con tres tipos de sensores que publican eventos en tiempo real; una cadena de servicios ZeroMQ los clasifica, actúa sobre los semáforos y persiste todo en dos bases de datos SQLite redundantes. En la sesión de pruebas del 9 de abril de 2026 se procesaron **75 401 eventos**, se detectaron **25 629 congestiones** y se ejecutaron **11 activaciones de prioridad** para paso de ambulancia, con latencia promedio de **2.16 ms** extremo a extremo.

---

## Tabla de contenidos

1. [Arquitectura del sistema](#1-arquitectura-del-sistema)
2. [Estructura del repositorio](#2-estructura-del-repositorio)
3. [Componentes](#3-componentes)
4. [Patrones de comunicación ZeroMQ](#4-patrones-de-comunicación-zeromq)
5. [Lógica de tráfico y semáforos](#5-lógica-de-tráfico-y-semáforos)
6. [Tolerancia a fallos](#6-tolerancia-a-fallos)
7. [Modelo de seguridad](#7-modelo-de-seguridad)
8. [Especificaciones de hardware](#8-especificaciones-de-hardware)
9. [Pre-requisitos e instalación](#9-pre-requisitos-e-instalación)
10. [Configuración](#10-configuración)
11. [Ejecución](#11-ejecución)
12. [Monitoreo y verificación](#12-monitoreo-y-verificación)
13. [Dashboard web](#13-dashboard-web)
14. [Experimentos de rendimiento](#14-experimentos-de-rendimiento)
15. [Resultados obtenidos](#15-resultados-obtenidos)
16. [Hallazgos y limitaciones conocidas](#16-hallazgos-y-limitaciones-conocidas)
17. [Trabajo futuro](#17-trabajo-futuro)
18. [Evidencia](#18-evidencia)

---

## 1. Arquitectura del sistema

El sistema implementa una **arquitectura cliente-servidor distribuida en tres niveles**, con comunicación orientada a mensajes. Cada nivel se ejecuta en una máquina física distinta.

| Nivel | Nodo | IP | Hostname | Responsabilidad principal |
|-------|------|----|----------|--------------------------|
| Adquisición | PC1 | 10.43.99.128 | MIG187 | Broker ZMQ + 75 sensores simulados |
| Procesamiento | PC2 | 10.43.100.144 | MIG441 | Analítica + semáforos + BD réplica |
| Persistencia y consulta | PC3 | 10.43.100.90 | MIG387 | BD principal + monitoreo CLI + dashboard |

### Topología de comunicación

```
PC1  (10.43.99.128)               PC2  (10.43.100.144)              PC3  (10.43.100.90)
┌───────────────────────┐         ┌──────────────────────────┐      ┌──────────────────────────┐
│  sensor_camara  ×25   │─PUB──►  │                          │      │                          │
│  sensor_espira  ×25   │─PUB──►  │  broker_zmq              │      │  base_datos_principal    │
│  sensor_gps     ×25   │─PUB──►  │  XSUB:5555 / XPUB:5556  │      │  PULL:5559  REP:5570     │
│                       │         │          │               │      │  PUB:5561 (heartbeat)    │
│    75 procesos total  │         │          │ SUB           │      │          ▲               │
└───────────────────────┘         │          ▼               │      │          │ HB PUB/SUB   │
                                  │  servicio_analitica      │◄─────┤          │               │
                                  │  REP:5560                │      │  servicio_monitoreo      │
                                  │    │        │      │     │      │  CLI+REQ  ────────────►  │
                                  │    │PUSH    │PUSH  │PUSH │      │                          │
                                  │    ▼        ▼      ▼    │      │  dashboard/server.py     │
                                  │  semaforos réplica  ────────────►  Flask+SocketIO:8080     │
                                  │  PULL:5557  PULL:5558   │      │                          │
                                  └──────────────────────────┘      └──────────────────────────┘
```

### Procesos por nodo (verificados experimentalmente)

| Nodo | Procesos activos |
|------|-----------------|
| PC1 | 1 `broker_zmq.py` + 25 `sensor_camara.py` + 25 `sensor_espira.py` + 25 `sensor_gps.py` = **76 procesos** |
| PC2 | `servicio_analitica.py` + `servicio_semaforos.py` + `base_datos_replica.py` = **3 procesos** |
| PC3 | `base_datos_principal.py` + `servicio_monitoreo.py` (+ `dashboard/server.py` en Entrega 2) |

> Verificado con `SELECT COUNT(DISTINCT interseccion) FROM eventos_sensores` → resultado: **25** intersecciones con distribución uniforme (~2 701–2 703 eventos cada una), confirmando que los 75 procesos publican de forma consistente.

---

## 2. Estructura del repositorio

```
trafico_urbano/
│
├── config/
│   └── config.json                   # Única fuente de verdad: IPs, puertos, intervalos, reglas
│
├── common/
│   ├── __init__.py
│   ├── config_loader.py              # Carga config.json; genera lista de 25 intersecciones
│   └── logger.py                     # Logger con formato uniforme para todos los servicios
│
├── pc1/                              # Capa de adquisición
│   ├── broker_zmq.py                 # Proxy XSUB/XPUB (modo simple y multihilo)
│   ├── sensor_camara.py              # EVENTO_LONGITUD_COLA: publica Q y Vp
│   ├── sensor_espira.py              # EVENTO_CONTEO_VEHICULAR: publica Cv
│   └── sensor_gps.py                 # EVENTO_DENSIDAD_TRAFICO: publica Vp y nivel
│
├── pc2/                              # Capa de procesamiento y control
│   ├── servicio_analitica.py         # Motor central: clasifica tráfico, ordena semáforos
│   ├── servicio_semaforos.py         # Gestiona estado y timers de los 25 semáforos
│   └── base_datos_replica.py         # BD SQLite de respaldo (PULL de analítica)
│
├── pc3/                              # Capa de persistencia y consulta
│   ├── base_datos_principal.py       # BD SQLite principal + heartbeat PUB + REP consultas
│   ├── servicio_monitoreo.py         # CLI interactiva con 8 opciones (REQ a PC2 y PC3)
│   └── dashboard/                    # ★ Entrega 2
│       ├── server.py                 # Flask + Flask-SocketIO
│       ├── templates/index.html
│       └── static/
│           ├── dashboard.js
│           └── style.css
│
├── experimentos/                     # ★ Entrega 2
│   ├── RUNBOOK.md                    # Guía paso a paso para reproducir los 4 escenarios
│   ├── medir_experimento.py          # Mide V1 (COUNT*) y V2 (perf_counter REQ/REP)
│   ├── generar_graficos.py           # Consolida CSVs en Tabla 1 y gráficos
│   ├── orquestar_todo.sh             # Orquestador modo simple
│   ├── orquestar_multihilo.sh        # Orquestador modo multihilo
│   ├── lanzar_pc1_experimento.sh
│   ├── lanzar_pc2_experimento.sh
│   └── lanzar_pc3_experimento.sh
│
├── evidencia_entrega1/               # Logs, capturas y gráficos de la sesión 2026-04-09
├── evidencia_entrega2/               # CSVs, gráficos y JSON de los experimentos
│
├── arrancar_todo.sh                  # Orquestador maestro: despliega PC3→PC2→PC1→dashboard
├── parar_todo.sh                     # Apaga todos los procesos remotos
├── desplegar_a_pcs.sh                # Copia el proyecto a las 3 PCs vía scp
├── lanzar_pc1.sh / lanzar_pc2.sh / lanzar_pc3.sh
├── lanzar_dashboard.sh               # ★ Entrega 2
└── dashboard_demo.html               # Dashboard offline de respaldo (JavaScript puro)
```

---

## 3. Componentes

### PC1 — Capa de adquisición

#### `broker_zmq.py`
Intermediario ZeroMQ que desacopla los 75 sensores productores del único consumidor (analítica). Implementa dos modos de operación:

**Modo simple** (`--modo simple`, por defecto)  
Proxy XSUB/XPUB de un solo hilo. Todos los mensajes de todos los tópicos pasan por el mismo hilo usando `zmq.proxy()`.

**Modo multihilo** (`--modo multihilo`)  
Un hilo dedicado por tópico (`camara`, `espira_inductiva`, `gps`), más hilos RX y TX para los sockets externos. Permite procesar los tres tópicos en paralelo. Incluye una corrección crítica: el socket XSUB envía manualmente la suscripción `b"\x01"` al inicio para que los sensores PUB comiencen a transmitir — sin esto el throughput es cero.

```bash
python pc1/broker_zmq.py                   # simple
python pc1/broker_zmq.py --modo multihilo
```

#### Sensores (×25 por tipo)

| Archivo | Evento publicado | Tópico ZMQ | Intervalo | Variables |
|---------|-----------------|-----------|----------|-----------|
| `sensor_camara.py` | `EVENTO_LONGITUD_COLA` | `camara` | 5 s | Q (cola), Vp (velocidad) |
| `sensor_espira.py` | `EVENTO_CONTEO_VEHICULAR` | `espira_inductiva` | 30 s | Cv (conteo en 30 s) |
| `sensor_gps.py` | `EVENTO_DENSIDAD_TRAFICO` | `gps` | 10 s | Vp, nivel de congestión |

La proporción de eventos resultante es 6:3:1 (cámara:GPS:espira), que es exactamente la inversa de los intervalos (1/5 : 1/10 : 1/30), validando que los sensores publican a la frecuencia esperada.

```bash
python pc1/sensor_camara.py --interseccion INT-C5
python pc1/sensor_camara.py --interseccion INT-A1 --intervalo 5
```

---

### PC2 — Capa de procesamiento y control

#### `servicio_analitica.py`
Componente central del sistema. Mantiene el estado agregado de cada intersección (`EstadoInterseccion`: Q, Vp, Cv, nivel GPS) y opera cinco sockets ZMQ en paralelo mediante un poller:

| Socket | Patrón | Función |
|--------|--------|---------|
| SUB | PUB/SUB | Recibe los tres tópicos del broker en PC1 |
| PUSH | PUSH/PULL | Envía comandos al servicio de semáforos |
| PUSH | PUSH/PULL | Persiste eventos en BD réplica (PC2) |
| PUSH | PUSH/PULL | Persiste eventos en BD principal (PC3) — desactivado si PC3 cae |
| REP | REQ/REP | Atiende consultas y comandos del monitoreo y del dashboard |
| SUB | PUB/SUB | Recibe heartbeats de PC3 (clase `MonitorPC3`) |

Comandos REQ/REP que procesa:

| `tipo` | Descripción |
|--------|-------------|
| `ESTADO_INTERSECCION` | Estado actual (Q, Vp, Cv, clasificación) de una intersección |
| `ESTADO_SISTEMA` | Resumen de las 25 intersecciones + estado de PC3 |
| `AMBULANCIA` | Activa ola verde 60 s en una lista de vías; registra en BDs |
| `CAMBIAR_SEMAFORO` | Fuerza VERDE o ROJO directo en una intersección (30 s) |
| `HEARTBEAT` | Ping de vida desde el propio monitoreo |

#### `servicio_semaforos.py`
Mantiene el estado (VERDE/ROJO) y los temporizadores de los 25 semáforos mediante `GestorSemaforos` (thread-safe con lock). Reconoce cinco acciones:

| Acción | Efecto |
|--------|--------|
| `CAMBIAR` | Fuerza un estado concreto en una intersección |
| `EXTENDER` | Añade N segundos a la fase verde activa |
| `PRIORIDAD` | Activa ola verde 60 s en lista de vías; resto a ROJO |
| `RESETEAR` | Devuelve al ciclo estándar (15 s verde / 15 s rojo) |
| `CONSULTAR` | Devuelve el estado actual del semáforo |

Tiempos parametrizables en `config.json`:

| Modo | Verde | Rojo |
|------|------:|-----:|
| Normal | 15 s | 15 s |
| Congestión | 30 s | 10 s |
| Prioridad (ambulancia) | 60 s | — |

#### `base_datos_replica.py`
BD SQLite de alta disponibilidad en PC2. Recibe exactamente los mismos registros que la BD principal vía PUSH/PULL. Realiza inserts en bloques de 100 registros con un `commit()` por bloque. Sirve de fallback transparente cuando PC3 no está disponible.

---

### PC3 — Capa de persistencia y consulta

#### `base_datos_principal.py`
Base de datos maestra SQLite (`trafico_principal.db`). Funciona en tres modos concurrentes mediante hilos:

1. **PULL**: recibe registros de la analítica e inserta en bloques de 100.
2. **REP**: atiende consultas históricas del monitoreo en el puerto 5570.
3. **Heartbeat PUB**: publica un latido cada 3 s en el puerto 5561 para que la analítica monitorice su disponibilidad.

Tipos de consulta soportados:

| `tipo` | Descripción |
|--------|-------------|
| `HISTORICO` | Registros de una intersección en rango de timestamps |
| `ESTADO_PUNTUAL` | Último registro de una intersección |
| `CONGESTION` | Log histórico de eventos clasificados como CONGESTION |
| `ESTADISTICAS` | Totales de eventos, congestiones, prioridades y desglose por intersección |

#### `servicio_monitoreo.py`
CLI interactiva operacional. Se conecta vía REQ/REP a la analítica (PC2) para comandos en tiempo real y a la BD principal (PC3) para consultas históricas, con **fallback automático** a la BD réplica (PC2) si PC3 no responde.

```
╔══════════════════════════════════════════════════════════════╗
║  1. Estado actual de una intersección                        ║
║  2. Estado de TODO el sistema (25 intersecciones)            ║
║  3. Historial de tráfico por intersección y período          ║
║  4. Historial de congestiones                                ║
║  5. Estadísticas generales de la BD                          ║
║  6. Activar paso de ambulancia (ola verde)                   ║
║  7. Forzar cambio de semáforo en intersección                ║
║  8. Verificar estado del sistema (ping a analítica)          ║
╚══════════════════════════════════════════════════════════════╝
```

---

## 4. Patrones de comunicación ZeroMQ

La selección de cada patrón responde a una necesidad arquitectural concreta:

| Patrón | Conexión | Justificación |
|--------|----------|---------------|
| **PUB/SUB** | Sensores → Broker → Analítica | Los sensores son productores asíncronos múltiples; la analítica es un único consumidor que debe recibir todo. PUB/SUB desacopla productores de consumidores y soporta filtrado por tópico, esencial para distinguir tipos de sensor (`camara`, `gps`, `espira_inductiva`). |
| **PUSH/PULL** | Analítica → Semáforos / BD réplica / BD principal | Cola de trabajo asíncrona: la analítica no bloquea esperando al receptor. Si un consumidor (p. ej. PC3 caído) no procesa, ZMQ aplica HWM (*high-water mark*) y descarta sin afectar al productor. |
| **REQ/REP** | Monitoreo ↔ Analítica / BD principal | Las consultas del operador requieren respuesta síncrona. El operador debe conocer el resultado antes de continuar. Latencia consistente ~2 ms en todas las pruebas. |
| **PUB/SUB (heartbeat)** | BD principal → Analítica | Detección de fallos sin polling activo. La BD principal publica un latido cada 3 s; la analítica calcula el tiempo desde el último latido visto. Idiomático en ZMQ y eficiente en red. |

### Tabla de puertos

| Puerto | Patrón ZMQ | Dirección del flujo |
|-------:|-----------|---------------------|
| 5555 | XSUB | Sensores → Broker PC1 |
| 5556 | XPUB | Broker PC1 → Analítica PC2 |
| 5557 | PUSH/PULL | Analítica → Servicio Semáforos PC2 |
| 5558 | PUSH/PULL | Analítica → BD Réplica PC2 |
| 5559 | PUSH/PULL | Analítica → BD Principal PC3 |
| 5560 | REQ/REP | Monitoreo & Dashboard → Analítica PC2 |
| 5561 | PUB/SUB | BD Principal PC3 → Analítica (heartbeat) |
| 5570 | REQ/REP | Monitoreo → BD Principal PC3 (consultas históricas) |
| 8080 | HTTP/WS | Dashboard → Navegadores |

---

## 5. Lógica de tráfico y semáforos

### Variables de entrada

| Variable | Fuente | Descripción |
|----------|--------|-------------|
| **Q** | Sensor cámara | Longitud de cola (vehículos esperando el semáforo) |
| **Vp** | Cámara / GPS | Velocidad promedio en km/h |
| **Cv** | Espira inductiva | Conteo vehicular en ventana de 30 s |

### Reglas de clasificación (`clasificar_trafico()`)

| Estado | Condición lógica | Acción en semáforo | Eventos (sesión completa) | % |
|--------|-----------------|-------------------|--------------------------|---|
| `NORMAL` | Q < 5 **AND** Vp > 35 **AND** Cv < 10 | RESETEAR ciclo estándar (15 s verde / 15 s rojo) | 9 419 | 12.5 % |
| `MODERADO` | (no NORMAL) **AND** (no CONGESTIÓN) | EXTENDER fase verde +5 s | 40 161 | 53.5 % |
| `CONGESTION` | Q ≥ 10 **OR** Vp < 15 **OR** Cv ≥ 20 | CAMBIAR a VERDE 30 s (modo CONGESTION) | 25 629 | 34.0 % |
| `PRIORIDAD` | Comando manual del monitoreo | OLA VERDE 60 s en lista de vías | 11 activaciones | — |

La regla CONGESTION usa **OR** (cualquier indicador crítico activa la respuesta). La regla NORMAL usa **AND** (todos los indicadores deben ser favorables simultáneamente). El estado MODERADO ocupa el espacio intermedio.

### Ejemplos numéricos verificados en pruebas

| Intersección | Q | Vp | Cv | Clasificación | Acción |
|-------------|--:|---:|---:|--------------|--------|
| INT-B2 | 3 | 40 | 7 | NORMAL | RESETEAR |
| INT-A3 | 7 | 22 | 14 | MODERADO | EXTENDER +5 s |
| INT-C5 | 15 | 8 | 25 | CONGESTION | CAMBIAR a VERDE 30 s |

### Flujo completo de paso de ambulancia

1. Operador ingresa opción 6 en el CLI de monitoreo.
2. Ingresa la ruta: `INT-D1, INT-D2, INT-D3, INT-D4, INT-D5`.
3. Monitoreo envía `{"tipo":"AMBULANCIA","vias":[...]}` vía REQ a la analítica.
4. Analítica envía `{"accion":"PRIORIDAD","vias":[...]}` vía PUSH al servicio de semáforos.
5. Los 5 semáforos de la ruta D pasan a VERDE 60 s; los 20 restantes a ROJO.
6. El evento se registra en ambas BDs con tipo `prioridad`.
7. La analítica devuelve `{"ok":true}` vía REP al monitoreo.

**Verificado experimentalmente**: 11 ambulancias ejecutadas en la sesión de pruebas completa, todas con éxito y tiempo de respuesta promedio **2.16 ms**.

---

## 6. Tolerancia a fallos

### Modelo de fallos completo

| Componente | Fallo | Detección | Estrategia | Impacto |
|-----------|-------|-----------|-----------|---------|
| Sensor individual | Proceso termina | Ausencia de mensajes en tópico | Otros sensores siguen publicando | Pérdida parcial de datos para esa intersección |
| Broker ZMQ (PC1) | Proceso termina | Sensores no logran conectar | Sensores reintentan; reinicio manual del broker | Sistema sin datos hasta restart |
| Analítica (PC2) | Proceso termina | Buffer ZMQ se llena en sensores | Buffer procesa al reiniciar (HWM) | Lag temporal en clasificación |
| BD Réplica (PC2) | Proceso termina | Error PUSH desde analítica | Analítica registra error; sin réplica hasta restart | Pérdida de redundancia local |
| **BD Principal (PC3)** | **Proceso termina** | **Heartbeat timeout (9 s)** | **Cambio automático a solo-réplica; reanudación al recuperar** | **Sistema sigue operativo; brecha entre BDs** |
| Servicio Monitoreo (PC3) | Proceso termina | N/A (cliente) | Relanzamiento manual por operador | Imposibilidad de consultar |

### Mecanismo de heartbeat

El health check se implementa en dos componentes:

**Publicador** (`pc3/base_datos_principal.py`): hilo dedicado que publica `{"tipo":"heartbeat","pc":"PC3","ts":"..."}` en `tcp://*:5561` cada **3 segundos**.

**Suscriptor** (`pc2/servicio_analitica.py`, clase `MonitorPC3`):
- Suscribe al PUB de PC3 en `tcp://10.43.100.90:5561`.
- Mantiene `_ultimo_heartbeat` con `time.time()`.
- Timeout = **9 s** (3 × intervalo — tolera 2 latidos perdidos).
- Si `time.time() - _ultimo_heartbeat > 9.0`: marca PC3 como caído y deja de hacer PUSH a la BD principal.
- Al recibir el siguiente heartbeat: marca PC3 como recuperado y reanuda el PUSH automáticamente.

### Prueba PT-01 — Cronología cronometrada (2026-04-09 a las 15:02)

```
[t =  0.000 s]  Sistema estable — marcador inicial
[t =  2.360 s]  SIGKILL → base_datos_principal.py (PC3 cae)
[t = 11.480 s]  PC2 detecta: "PC3 no responde desde hace 10.2 s (timeout=9 s)"
[t = 15.480 s]  Restart base_datos_principal.py
[t = 18.800 s]  PC2 detecta: "PC3 recuperado — reanudando envío a BD principal"
```

| Métrica | Valor medido | Criterio de aceptación | Resultado |
|---------|------------:|----------------------|-----------|
| Tiempo de detección de fallo | **9.12 s** | < 10 s | Cumple |
| Tiempo de recuperación automática | **3.32 s** | Automático | Cumple |

Durante los ~13 s que PC3 estuvo caído:
- Los sensores siguieron generando eventos sin error.
- La analítica continuó procesando y clasificando.
- El servicio de semáforos siguió cambiando estados.
- La BD réplica acumuló registros de forma independiente.

### Resumen de pruebas

| Categoría | Pruebas | Pasaron | Métricas clave |
|-----------|--------:|--------:|----------------|
| Funcionales (PF) | 5 | 5/5 | 75 401 eventos procesados |
| Tolerancia a fallos (PT) | 4 | 4/4 | Detección 9.12 s, recuperación 3.32 s |
| Rendimiento (V1 y V2) | 2 | 2/2 | 8.3 reg/s, 2.16 ms latencia ambulancia |

### Limitación conocida: brecha de replicación

Los datos generados mientras PC3 está caído quedan únicamente en la réplica y **no se sincronizan automáticamente** cuando PC3 se recupera. En la sesión de pruebas se produjeron 4 episodios de caída de PC3, generando una brecha cuantificada de 11 530 eventos.

| Tabla | BD Principal (PC3) | BD Réplica (PC2) | Diferencia |
|-------|------------------:|----------------:|----------:|
| `eventos_sensores` | 75 401 | 86 931 | **+11 530** |
| `congestion_log` | 25 629 | 29 444 | **+3 815** |
| `prioridades_log` | 11 | 11 | **0** |

La tabla `prioridades_log` coincide en ambos lados (11 = 11) porque todas las activaciones de ambulancia se ejecutaron mientras PC3 estaba activo, lo que confirma que la brecha se debe exclusivamente a los episodios de caída.

---

## 7. Modelo de seguridad

| Aspecto | Implementación actual | Mejora recomendada |
|---------|----------------------|-------------------|
| **Autenticación** | Ninguna (red de laboratorio cerrada) | ZMQ CURVE (clave pública/privada por nodo) |
| **Confidencialidad** | Mensajes JSON en texto plano | TLS sobre los sockets ZMQ |
| **Integridad** | No verificada | HMAC-SHA256 en cada mensaje, llave por nodo |
| **Control de acceso** | Cualquier cliente en la red puede publicar | Lista blanca de IPs en el broker; ZAP authentication |
| **Auditoría** | Logs locales en cada PC con timestamp | Forwarder centralizado (rsyslog / ELK) |

**Justificación del nivel actual**: el contexto es una red privada de laboratorio sin acceso externo, con propósito puramente académico. ZMQ CURVE puede activarse cambiando 3 líneas en la inicialización del contexto cuando se requiera.

---

## 8. Especificaciones de hardware

| Aspecto | PC1 (sensores) | PC2 (analítica) | PC3 (BD ppal) |
|---------|---------------|----------------|--------------|
| Hostname / IP | MIG187 / 10.43.99.128 | MIG441 / 10.43.100.144 | MIG387 / 10.43.100.90 |
| CPU | Xeon Gold 6240R @2.40 GHz (4 vCPU) | Xeon Gold 6240R @2.40 GHz (4 vCPU) | Xeon E5-2650 v4 @2.20 GHz (4 vCPU) |
| RAM | 11 GiB | 11 GiB | 11 GiB |
| OS | Ubuntu 22.04 (6.8.0-94) | Ubuntu 22.04 (6.8.0-94) | Ubuntu 22.04 (6.8.0-110) |
| Python | 3.10.12 | 3.10.12 | 3.10.12 |
| pyzmq | 25+ | 25+ | 25+ |
| **Red** | **100 Mbps Ethernet LAN universitaria — latencia < 1 ms entre PCs** | | |

---

## 9. Pre-requisitos e instalación

**Python 3.9 o superior** en las tres máquinas.

```bash
# Instalación mínima (Entrega 1)
pip3 install pyzmq

# Instalación completa (Entrega 2 — incluye dashboard)
pip3 install pyzmq flask flask-socketio eventlet
```

**Verificación:**

```bash
python3 -c "import zmq; print('ZMQ:', zmq.__version__)"
python3 -c "import flask; print('Flask:', flask.__version__)"
```

---

## 10. Configuración

`config/config.json` es la **única fuente de verdad** sobre topología, puertos y parámetros operativos. Todos los procesos la cargan al iniciar vía `common/config_loader.py`. Basta modificar este archivo para reproducir el experimento en otra red.

```json
{
  "ciudad": {
    "filas": ["A", "B", "C", "D", "E"],
    "columnas": [1, 2, 3, 4, 5]
  },
  "sensores": {
    "intervalo_camara_seg": 5,
    "intervalo_espira_seg": 30,
    "intervalo_gps_seg": 10,
    "velocidad_max_kmh": 50
  },
  "red": {
    "PC1_IP": "10.43.99.128",
    "PC2_IP": "10.43.100.144",
    "PC3_IP": "10.43.100.90",
    "puertos": {
      "broker_sub": 5555,
      "broker_pub": 5556,
      "semaforos_pull": 5557,
      "db_replica_pull": 5558,
      "db_main_pull": 5559,
      "analitica_rep": 5560,
      "heartbeat_pub": 5561
    }
  },
  "semaforos": {
    "tiempo_verde_normal_seg": 15,
    "tiempo_verde_congestion_seg": 30,
    "tiempo_verde_prioridad_seg": 60,
    "tiempo_rojo_normal_seg": 15,
    "tiempo_rojo_congestion_seg": 10
  },
  "reglas_trafico": {
    "normal":     { "Q_max": 5,  "Vp_min": 35, "Cv_max": 10 },
    "congestion": { "Q_min": 10, "Vp_max": 15, "Cv_min": 20 }
  },
  "heartbeat": {
    "intervalo_seg": 3,
    "timeout_seg": 9
  }
}
```

La función `obtener_intersecciones(config)` construye la lista completa de 25 intersecciones (INT-A1 a INT-E5) mediante producto cartesiano de filas × columnas.

Cada sensor admite `--intervalo N` por línea de comandos para sobreescribir el valor del config sin modificar archivos, lo que permite ejecutar los escenarios E1 y E2 del experimento sin cambiar la configuración base.

### Parámetros de inicialización por componente

| Componente | Parámetros CLI | Fuente |
|-----------|---------------|--------|
| Sensor (cualquier tipo) | `--interseccion INT-XX`, `--intervalo N` | CLI + config.json |
| Broker ZMQ | `--modo simple\|multihilo` | CLI + config.json |
| Analítica | Ninguno | config.json |
| Semáforos | Ninguno | config.json |
| BD réplica / principal | Ninguno | config.json |
| Monitoreo | Ninguno | config.json |

---

## 11. Ejecución

### Opción rápida — orquestación completa desde el Mac

```bash
# 1. Copiar el proyecto a las 3 PCs (solo la primera vez)
bash desplegar_a_pcs.sh

# 2. Limpiar procesos anteriores y arrancar todo en orden PC3 → PC2 → PC1 → dashboard
bash arrancar_todo.sh

# 3. Apagar todo
bash parar_todo.sh
```

### Opción manual — nodo por nodo

El orden de arranque es **obligatorio**: **PC3 → PC2 → PC1**. El sistema tolera cualquier orden gracias al heartbeat y a los reintentos de ZMQ, pero arrancar PC1 antes que PC3 puede generar un lag inicial en la BD principal.

```bash
# ── TERMINAL EN PC3 ──────────────────────────────────────────────
cd ~/trafico_urbano
bash lanzar_pc3.sh
# Lanza: base_datos_principal.py + servicio_monitoreo.py

# ── TERMINAL EN PC2 ──────────────────────────────────────────────
cd ~/trafico_urbano
bash lanzar_pc2.sh
# Lanza: servicio_analitica.py + servicio_semaforos.py + base_datos_replica.py

# ── TERMINAL EN PC1 ──────────────────────────────────────────────
cd ~/trafico_urbano
bash lanzar_pc1.sh                   # broker simple + 75 sensores
bash lanzar_pc1.sh --multihilo       # broker multihilo + 75 sensores

# ── TERMINAL ADICIONAL EN PC3 (Entrega 2) ────────────────────────
bash lanzar_dashboard.sh
# Abrir: http://10.43.100.90:8080
```

> **Importante**: matar procesos anteriores antes de relanzar para evitar `Address already in use`. El script `arrancar_todo.sh` incluye esta limpieza automáticamente.

### Componentes individuales

```bash
# Broker
python3 pc1/broker_zmq.py --modo simple
python3 pc1/broker_zmq.py --modo multihilo

# Sensor individual
python3 pc1/sensor_camara.py --interseccion INT-C5
python3 pc1/sensor_espira.py --interseccion INT-C5 --intervalo 10
python3 pc1/sensor_gps.py    --interseccion INT-C5

# Servicios de PC2
python3 pc2/servicio_analitica.py
python3 pc2/servicio_semaforos.py
python3 pc2/base_datos_replica.py

# Servicios de PC3
python3 pc3/base_datos_principal.py
python3 pc3/servicio_monitoreo.py
python3 pc3/dashboard/server.py
```

### Cambiar el modo del broker en caliente

```bash
# Detener el broker actual
pkill -f 'broker_zmq.py'

# Relanzar en modo multihilo
cd ~/trafico_urbano && python3 pc1/broker_zmq.py --modo multihilo \
    > ~/trafico_urbano/logs/broker.log 2>&1 &
```

---

## 12. Monitoreo y verificación

### Seguimiento de logs en tiempo real

```bash
# PC1 — broker y sensores representativos
tail -f ~/trafico_urbano/logs/broker.log \
         ~/trafico_urbano/logs/cam_C5.log \
         ~/trafico_urbano/logs/gps_C5.log

# PC2 — analítica, semáforos y réplica en paralelo
tail -f ~/trafico_urbano/logs/analitica.log \
         ~/trafico_urbano/logs/semaforos.log \
         ~/trafico_urbano/logs/bd_replica.log

# PC2 — filtrar solo eventos de heartbeat y conmutación de BD
tail -f ~/trafico_urbano/logs/analitica.log \
    | grep -E "PC3|réplica|recuperado|responde"

# PC3 — BD principal
tail -f ~/trafico_urbano/logs/bd_principal.log
```

### Consultas SQL de verificación

```sql
-- Total de eventos en la sesión
SELECT COUNT(*) FROM eventos_sensores;

-- Distribución por tipo de sensor
SELECT topic, COUNT(*) AS total
FROM eventos_sensores
GROUP BY topic;

-- Clasificación del tráfico
SELECT estado_trafico, COUNT(*) AS total
FROM eventos_sensores
WHERE estado_trafico IS NOT NULL
GROUP BY estado_trafico;

-- Top 10 intersecciones con más congestión
SELECT interseccion, COUNT(*) AS congestions
FROM congestion_log
GROUP BY interseccion
ORDER BY 2 DESC
LIMIT 10;

-- Distribución uniforme por intersección (verifica los 75 procesos)
SELECT COUNT(DISTINCT interseccion) AS intersecciones_activas
FROM eventos_sensores;

-- Registros almacenados en los últimos 2 minutos (Variable 1)
SELECT COUNT(*) FROM eventos_sensores
WHERE timestamp >= datetime('now', 'localtime', '-2 minutes');
```

### Parar todos los procesos

```bash
# PC1
pkill -f 'broker_zmq.py|sensor_camara.py|sensor_espira.py|sensor_gps.py'

# PC2
pkill -f 'servicio_analitica.py|servicio_semaforos.py|base_datos_replica.py'

# PC3
pkill -f 'base_datos_principal.py|servicio_monitoreo.py|dashboard'

# Desde cualquier PC — todos los procesos del proyecto
pkill -f 'pc1/\|pc2/\|pc3/\|broker_zmq.py'
```

---

## 13. Dashboard web

Disponible en `http://10.43.100.90:8080` (Entrega 2). El servidor Flask en PC3 combina tres fuentes de datos en tiempo real:

- **ZMQ SUB** al broker de PC1: stream crudo de eventos de sensores.
- **SQLite local**: contadores de la BD principal.
- **REQ/REP** a la analítica: estado actual de las 25 intersecciones.

### Eventos WebSocket emitidos al navegador

| Evento | Contenido |
|--------|-----------|
| `evento_sensor` | Datos crudos (sensor_id, intersección, Q, Vp, Cv, timestamp) |
| `estado_sistema` | Mapa completo: `{INT-XX: {estado, Q, Vp, Cv}}` |
| `heartbeat` | `{pc3_activo: bool, last_beat: ISO}` |
| `metricas` | Contadores BD principal, réplica, congestiones, prioridades |
| `evento_semaforo` | `{interseccion, estado, modo, ts}` |

### Funcionalidades de la interfaz

- Mapa interactivo 5×5 con estado de tráfico coloreado (NORMAL/MODERADO/CONGESTION/PRIORIDAD).
- Indicador en vivo del heartbeat de PC3.
- Semáforos animados por intersección.
- Stream de eventos crudos del broker.
- Activación de ambulancia: clic en intersección inicial + clic en intersección final (solo rutas en línea recta — misma fila o columna). Rutas diagonales rechazadas, coherente con el supuesto de vías de un solo sentido.
- Forzar cambio de semáforo individual (VERDE/ROJO).

### Acceso remoto

```bash
# Túnel SSH (funciona fuera de la red universitaria)
ssh -L 8080:localhost:8080 estudiante@10.43.100.90
# Abrir: http://localhost:8080

# Escritorio remoto a PC3 → abrir http://localhost:8080
```

### Dashboard offline de respaldo

`dashboard_demo.html` se abre con doble clic en cualquier navegador sin servidor. Simula las 3 PCs en JavaScript puro. Sirve como respaldo para sustentaciones en caso de fallo de red.

---

## 14. Experimentos de rendimiento

### Variables medidas

| Variable | Descripción | Método de medición |
|----------|-------------|-------------------|
| **V1** | Eventos insertados en BD principal en una ventana de 2 minutos | `COUNT(*)` antes y después de 120 s exactos |
| **V2** | Latencia extremo a extremo del comando de ambulancia (usuario → analítica confirma) | `time.perf_counter()` antes del `send` y después del `recv` en REQ/REP |

Para V2 se toman 5 muestras por escenario, espaciadas 2 s, y se reportan promedio, mínimo y máximo.

### Diseño experimental

Se cruzaron dos factores independientes:

| Factor | Nivel 1 (Escenario A) | Nivel 2 (Escenario B) |
|--------|-----------------------|-----------------------|
| Número de sensores | 1 sensor de cada tipo en INT-C5 | 2 sensores de cada tipo (INT-C5 e INT-B3) |
| Intervalo | 10 s | 5 s |
| **Carga resultante** | **~18 eventos/min** | **~72 eventos/min** |

Para cada escenario se ejecutó el broker en modo **simple** (diseño original) y **multihilo** (diseño modificado), produciendo 4 corridas documentadas.

### Procedimiento de ejecución

Seguir `experimentos/RUNBOOK.md`. Orden de arranque obligatorio para cada corrida:

```bash
# Terminal en PC3 (primero)
bash experimentos/lanzar_pc3_experimento.sh --escenario A --diseno simple

# Terminal en PC2 (mientras PC3 espera sincronización)
bash experimentos/lanzar_pc2_experimento.sh --escenario A --diseno simple

# Terminal en PC1 (último)
bash experimentos/lanzar_pc1_experimento.sh --escenario A --diseno simple

# PC3 mide V1 durante 120 s y V2 con 5 muestras REQ/REP
# Al terminar: "Experimento A_simple completado."
```

Repetir las 4 combinaciones: `A_simple`, `A_multihilo`, `B_simple`, `B_multihilo`.

---

## 15. Resultados obtenidos

### Sesión de pruebas completa — Entrega 1 (2026-04-09)

| Métrica | BD Principal (PC3) | BD Réplica (PC2) |
|---------|------------------:|----------------:|
| Eventos de sensores | **75 401** | 86 931 |
| Congestiones detectadas | **25 629** | 29 444 |
| Prioridades (ambulancias) | **11** | 11 |

**Distribución por tipo de sensor (BD principal):**

| Sensor | Eventos | % | Intervalo |
|--------|--------:|--:|----------|
| Cámara | 45 023 | 60.0 % | 5 s |
| GPS | 22 525 | 30.0 % | 10 s |
| Espira inductiva | 7 500 | 10.0 % | 30 s |
| **Total** | **75 048** | **100 %** | |

La proporción 6:3:1 es la inversa exacta de los intervalos (1/5 : 1/10 : 1/30).

**Clasificación del tráfico (BD principal):**

| Estado | Eventos | % |
|--------|--------:|--:|
| MODERADO | 40 161 | 53.5 % |
| CONGESTION | 25 629 | 34.0 % |
| NORMAL | 9 419 | 12.5 % |

**Rendimiento (Entrega 1 — broker SIMPLE con 75 sensores activos):**

| Configuración | V1 — Registros en 2 min | Tasa (reg/s) |
|---------------|------------------------:|-------------:|
| Broker SIMPLE | 1 000 | 8.3 |
| Broker MULTIHILO | 1 000 | 8.3 |

---

### Tabla 1 — Experimentos de rendimiento, Entrega 2

| Escenario | Diseño | V1 (eventos / 2 min) | V2 prom (ms) | V2 mín (ms) | V2 máx (ms) |
|-----------|--------|---------------------:|-------------:|------------:|------------:|
| A: 1 sensor c/10 s | Simple | 36 | 2.24 | 1.82 | 3.23 |
| A: 1 sensor c/10 s | Multihilo | 36 | **1.94** | 1.50 | 3.01 |
| B: 2 sensores c/5 s | Simple | 144 | 2.22 | 1.63 | 3.12 |
| B: 2 sensores c/5 s | Multihilo | 144 | 2.21 | 1.94 | 2.93 |

---

## 16. Hallazgos y limitaciones conocidas

### Hallazgo 1: el cuello de botella es la persistencia, no el broker

Tanto en Entrega 1 (75 sensores, 8.3 reg/s) como en Entrega 2, los dos modos del broker produjeron idéntico throughput V1. La BD principal realiza inserts en **bloques de 100 registros** con un `commit()` por bloque; el throughput observado está determinado por la latencia del commit de SQLite (que fuerza fsync a disco) y la frecuencia con la que se cierran los bloques. Ninguna de las dos cambia al cambiar el modo del broker.

Para diferenciar los modos en throughput sería necesario medir en la analítica (mensajes recibidos por el socket SUB) y no en la BD, o llevar la carga a un punto donde el broker de un solo hilo se sature (decenas de sensores por intersección, intervalos en milisegundos).

### Hallazgo 2: V2 es estable e independiente del modo del broker

La latencia del comando de ambulancia (~2 ms) recorre la ruta **Monitoreo → Analítica → confirmación** vía REQ/REP, que no atraviesa el broker. Las pequeñas diferencias observadas (13 % de mejora en el escenario A con multihilo) reflejan la contención de CPU: al repartir el trabajo del broker en varios hilos, los demás servicios compiten menos por el ciclo de procesamiento.

### Hallazgo 3: el sistema escala linealmente con la carga

Al cuadruplicar la carga del escenario A al B (de ~18 a ~72 eventos/min, factor 4×), V1 escaló exactamente en el mismo factor (36 → 144) sin degradar V2, lo que confirma que el sistema operó siempre en régimen no saturado.

### Hallazgo 4: bug corregido en el broker multihilo

La versión inicial del broker multihilo tenía un defecto: el socket XSUB no propagaba la suscripción hacia los sensores PUB, lo que provocaba throughput cero. La corrección consiste en enviar manualmente `b"\x01"` (suscripción a todos los tópicos) al inicializar el broker. El byte `0x01` con prefijo vacío equivale a "suscribirse a todo"; ZMQ lo reenvía también a los PUB que se conecten más tarde.

### Limitación conocida: sin sincronización post-falla

Los datos generados mientras PC3 está caído no se sincronizan automáticamente al recuperarse. La brecha documentada en Entrega 1 fue de 11 530 eventos. Ver sección 6 para la cuantificación detallada.

---

## 17. Evidencia

### Entrega 1 — sesión 2026-04-09

| Archivo | Contenido |
|---------|-----------|
| `01_procesos_activos.txt` | Snapshot de procesos activos en las 3 PCs |
| `02_heartbeat_fallos.txt` | Logs completos de detección y recuperación de PC3 |
| `03_detecciones_congestion.txt` | Eventos clasificados como CONGESTION en la analítica |
| `04_cambios_semaforos.txt` | Logs del servicio de semáforos |
| `05_bd_principal.txt` | Logs de la BD principal recibiendo datos |
| `06_bd_replica.txt` | Logs de la BD réplica |
| `07_sensor_cam_C5.txt` | Muestra de log de un sensor cámara individual |
| `08_broker.txt` | Logs del broker ZMQ |
| `09_timing_ambulancia.txt` | Mediciones V2 (broker simple) |
| `10_variable1_2min.txt` | Mediciones V1 (broker simple) |
| `11_PT01_cronometrado.txt` | Cronología detallada de la prueba PT-01 |
| `12_variable1_multihilo.txt` | Mediciones V1 (broker multihilo) |
| `13_timing_ambulancia_multihilo.txt` | Mediciones V2 (broker multihilo) |
| `1_eventos_por_sensor.png` | Gráfico de barras: distribución por tipo de sensor |
| `2_clasificacion_trafico.png` | Pie chart: NORMAL / MODERADO / CONGESTION |
| `3_replicacion_bds.png` | Comparación BD Principal vs Réplica |
| `4_top_congestion.png` | Top 10 intersecciones con más congestión |
| `5_experimento_variables.png` | Variables del experimento (modo simple) |
| `6_topologia_sistema.png` | Topología desplegada de las 3 PCs |
| `7_comparativa_simple_vs_multihilo.png` | Comparación de los dos modos del broker |
| `8_PT01_cronologia.png` | Cronología cronometrada de PT-01 |
| `09_uml_despliegue.png` | Diagrama UML de despliegue |
| `10_uml_componentes.png` | Diagrama UML de componentes |
| `11_uml_clases.png` | Diagrama UML de clases |
| `12_uml_secuencia.png` | Diagrama UML de secuencia |
| `metricas.json` | Métricas estructuradas en JSON |

### Entrega 2 — experimentos de rendimiento

| Archivo | Contenido |
|---------|-----------|
| `A_simple.csv` / `A_multihilo.csv` | Datos crudos del escenario A |
| `B_simple.csv` / `B_multihilo.csv` | Datos crudos del escenario B |
| `tabla1_completa.csv` | Tabla 1 consolidada de los 4 escenarios |
| `resumen_analisis.json` | Resultados y deltas en JSON |
| `g1_variable1.png` | Throughput BD principal por escenario |
| `g2_variable2.png` | Latencia V2 por escenario |
| `g3_escalabilidad.png` | Variación porcentual al aumentar la carga A → B |
| `specs_hw_pc1.txt` / `specs_hw_pc2.txt` / `specs_hw_pc3.txt` | Hardware de cada nodo (`lscpu`, `free -h`, `uname`) |

---

*Datos experimentales recolectados en sesión de pruebas del 9 de abril de 2026 con los 3 PCs operativos en la red del laboratorio de la Pontificia Universidad Javeriana.*
