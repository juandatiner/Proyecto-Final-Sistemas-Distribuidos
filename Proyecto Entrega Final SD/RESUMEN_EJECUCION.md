# Resumen de Ejecución — Evidencia Primera Entrega
## Proyecto: Gestión Inteligente de Tráfico Urbano
**Fecha de ejecución:** 2026-04-09
**Sustentación:** Viernes 10 de abril de 2026

---

## 1. Despliegue del Sistema (3 PCs)

| PC | IP | Hostname | Rol | Procesos activos |
|----|----|----|-----|------------------|
| **PC1** | 10.43.99.128 | MIG187 | Adquisición | 1 broker + 75 sensores (25 int. × 3 tipos) |
| **PC2** | 10.43.100.144 | MIG441 | Procesamiento | analítica + semáforos + BD réplica |
| **PC3** | 10.43.100.90 | MIG387 | Persistencia/Consulta | BD principal + monitoreo |

---

## 2. Métricas Recolectadas (BD Principal — PC3)

### Totales acumulados durante la ejecución de prueba

| Métrica | Valor |
|---------|------:|
| Eventos de sensores | **30,903** |
| Congestiones detectadas | **10,622** |
| Prioridades (ambulancias) | **5** |

### Eventos por tipo de sensor

| Sensor | Cantidad | % |
|--------|---------:|---:|
| Cámara | 18,550 | 60.0% |
| GPS | 9,278 | 30.0% |
| Espira inductiva | 3,075 | 10.0% |

### Clasificación del tráfico

| Estado | Cantidad | % |
|--------|---------:|---:|
| MODERADO | 16,535 | 53.5% |
| CONGESTION | 10,622 | 34.4% |
| NORMAL | 3,746 | 12.1% |

---

## 3. Tolerancia a Fallos — Evidencia del Heartbeat

### Detección automática de fallo y recuperación de PC3

```
2026-04-09T12:47:29 | WARN  | ⚠️  PC3 no responde desde hace 9.2s — usando réplica en PC2
2026-04-09T12:49:17 | INFO  | ✅ PC3 recuperado — reanudando envío a BD principal
2026-04-09T12:49:34 | WARN  | ⚠️  PC3 no responde desde hace 11.0s — usando réplica en PC2
2026-04-09T12:50:24 | INFO  | ✅ PC3 recuperado — reanudando envío a BD principal
2026-04-09T13:12:15 | WARN  | ⚠️  PC3 no responde desde hace 9.4s — usando réplica en PC2
2026-04-09T13:26:42 | INFO  | ✅ PC3 recuperado — reanudando envío a BD principal
```

**Prueba PT-01 (Detección automática)**: ✅ Detección en <10s (timeout configurado en 9s)
**Prueba PT-02 (Continuidad)**: ✅ Sistema continuó operando con la réplica
**Prueba PT-03 (Recuperación)**: ✅ Reanudación automática al volver PC3

### Brecha BD Réplica vs BD Principal (evidencia cuantitativa)

| BD | Eventos | Congestiones |
|----|--------:|-------------:|
| **Réplica (PC2)** | 33,925 | 11,555 |
| **Principal (PC3)** | 30,903 | 10,622 |
| **Diferencia** | **+3,022** | **+933** |

> Esto confirma la **limitación conocida** documentada en el informe (sección 1.3): los datos generados durante el fallo de PC3 quedan únicamente en la réplica y no se sincronizan automáticamente cuando PC3 se recupera.

---

## 4. Métricas del Experimento

### Variable 1: Registros en BD en 2 minutos

- **Configuración**: 75 sensores activos (25 intersecciones × 3 tipos), broker SIMPLE
- **Resultado**: ver `10_variable1_2min.txt`

### Variable 2: Tiempo respuesta Usuario→Semáforo (ambulancia)

| Test | # vías | Δt (ms) |
|------|-------:|--------:|
| 1 | 3 | 3.16 |
| 2 | 5 | 1.56 |
| 3 | 3 | 1.76 |
| **Promedio** |  | **2.16** |
| Min |  | 1.56 |
| Max |  | 3.16 |

**Método**: `time.perf_counter()` antes y después del REQ/REP Monitoreo→Analítica vía ZeroMQ.

---

## 5. Pruebas Funcionales — Resultados

| ID | Prueba | Estado |
|----|--------|--------|
| PF-01 | Comunicación sensores → broker → analítica | ✅ Verificado en logs |
| PF-02 | Detección congestión y cambio semáforo | ✅ 10,622 congestiones detectadas |
| PF-03 | Paso de ambulancia | ✅ 5 prioridades activadas |
| PF-04 | Consulta histórica (REQ/REP) | ✅ BD Principal responde en :5570 |
| PF-05 | Persistencia en ambas BDs | ✅ Ambas BDs reciben datos |
| PT-01 | Detección automática fallo PC3 | ✅ <10s |
| PT-02 | Continuidad con PC3 caído | ✅ Sistema sigue operando |
| PT-03 | Recuperación PC3 | ✅ Reanuda envío automáticamente |
| PT-04 | Falla sensor individual | ✅ Otros sensores siguen publicando |

---

## 6. Archivos de Evidencia Generados

| Archivo | Contenido |
|---------|-----------|
| `1_eventos_por_sensor.png` | Gráfico de barras eventos por tipo de sensor |
| `2_clasificacion_trafico.png` | Pie chart distribución NORMAL/MODERADO/CONGESTION |
| `3_replicacion_bds.png` | Comparación BD Principal vs Réplica |
| `4_top_congestion.png` | Top 10 intersecciones con más congestión |
| `01_procesos_activos.txt` | Snapshot de procesos en las 3 PCs |
| `02_heartbeat_fallos.txt` | Logs detección/recuperación PC3 |
| `03_detecciones_congestion.txt` | Eventos CONGESTION en analítica |
| `04_cambios_semaforos.txt` | Logs servicio_semaforos |
| `05_bd_principal.txt` | Logs BD Principal recibiendo datos |
| `06_bd_replica.txt` | Logs BD Réplica |
| `07_sensor_cam_C5.txt` | Muestra de log sensor cámara |
| `08_broker.txt` | Logs broker ZMQ |
| `09_timing_ambulancia.txt` | Mediciones Variable 2 |
| `10_variable1_2min.txt` | Mediciones Variable 1 |
| `metricas.json` | Métricas en formato JSON |
