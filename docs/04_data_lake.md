# 4. Diseño del Data Lake

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 04/10/2026 · Ver también: [03 · Arquitectura](03_arquitectura.md) y [DECISIONS.md](../DECISIONS.md)

En este documento defino cómo organizo el Data Lake: qué guardo en cada zona, en qué formato, cómo lo particiono, cómo nombro las cosas, cuánto tiempo guardo los datos y qué tiene que cumplir un dato para pasar de una zona a la siguiente.

## 4.1 Estructura de carpetas

```
datalake/
├── landing/        archivos originales, tal cual llegan
│   ├── *.csv
│   └── usage_events_stream/*.jsonl
├── bronze/         una carpeta por fuente
├── silver/         tablas limpias y unidas
├── gold/           marts de negocio
├── quarantine/     registros que no pasan las reglas de calidad
└── _checkpoints/   estado interno del streaming (no se toca a mano)
```

## 4.2 Zonas, formato y contenido

| Zona | Qué guardo | Formato | ¿Se modifica? |
|---|---|---|---|
| **Landing** | Los archivos originales de la cátedra | CSV y JSONL | Nunca |
| **Bronze** | Los mismos datos, con tipos correctos y columnas de trazabilidad | Parquet | Solo se agregan datos nuevos |
| **Silver** | Datos limpios, unidos con los maestros, v1 y v2 unificadas, features calculadas | Parquet | Se regenera desde Bronze |
| **Gold** | Tablas resumidas para responder las consultas de negocio | Parquet | Se regenera desde Silver |
| **Quarantine** | Registros que fallaron alguna regla, con el motivo | Parquet | Solo se agregan datos nuevos |

Uso Parquet desde Bronze en adelante porque es un formato **columnar** y **comprimido**: ocupa menos espacio que un CSV y Spark lee solo las columnas que necesita, así que las consultas son más rápidas.

## 4.3 Tablas por zona

### Bronze (mismo grano que la fuente)

| Tabla | Fuente | Partición |
|---|---|---|
| `customers_orgs` | customers_orgs.csv | — |
| `users` | users.csv | — |
| `resources` | resources.csv | — |
| `support_tickets` | support_tickets.csv | — |
| `marketing_touches` | marketing_touches.csv | — |
| `nps_surveys` | nps_surveys.csv | — |
| `billing_monthly` | billing_monthly.csv | `month` |
| `usage_events` | usage_events_stream/*.jsonl | `event_date` |

### Silver

| Tabla | Contenido | Partición |
|---|---|---|
| `usage_events` | Eventos limpios, v1 y v2 unificados, unidos con organización y recurso, con flags de calidad y anomalía | `event_date` |
| `customers` | Organizaciones con NPS validado | — |
| `support_tickets` | Tickets con CSAT validado y fecha de resolución | — |
| `billing` | Facturas con FX aplicado según D-01 y montos en USD | `month` |

### Gold (marts)

| Mart | Grano (una fila por…) | Partición | Responde |
|---|---|---|---|
| `org_daily_usage_by_service` | organización + día + servicio | `usage_date` | Consultas 1 y 2 |
| `revenue_by_org_month` | organización + mes | `month` | Consulta 4 |
| `tickets_by_org_date` | organización + día + severidad | `date` | Consulta 3 |
| `genai_tokens_by_org_date` | organización + día | `date` | Consulta 5 |
| `cost_anomaly_mart` | organización + día + servicio | `date` | Anomalías de costo |

## 4.4 Particiones

**Decisión (D-03):** particiono **solo por fecha** (día o mes, según la tabla).

**Por qué:** casi todas las consultas y subconsultas se hacen por fecha, así que particionar por esa columna debería ser lo más eficiente. Lo confirman las 5 consultas obligatorias de la consigna:

| Consulta | Filtro de fecha |
|---|---|
| 1. Costos y requests por organización y servicio | rango de fechas |
| 2. Top-N servicios por costo | últimos 14 días |
| 3. Tickets críticos y SLA | últimos 30 días |
| 4. Revenue mensual | por mes |
| 5. Tokens GenAI | por día |

Cuando una consulta filtra por la misma columna con la que está particionada la tabla, Spark lee solo las carpetas de esas fechas y saltea el resto. Esto se llama **partition pruning** (poda de particiones).

**Por qué no particiono también por servicio:** la consigna lo sugiere como opción ("fecha y/o servicio"), pero con 43.200 eventos quedarían 60 días × 6 servicios = 360 carpetas de unas 120 filas cada una. Son muchos archivos chiquitos, y eso hace más lento a Spark (tiene que abrir cada archivo). Si el volumen creciera, revisaría esta decisión.

**Por qué no particiono los maestros:** son tablas chicas (entre 80 y 1.500 filas). Partirlas solo generaría archivos diminutos.

**Control de archivos:** antes de escribir, uso `coalesce` para juntar los datos en pocos archivos por partición y no generar cientos de archivos chicos.

## 4.5 Nombres (convenciones)

- Todo en `snake_case` y minúsculas: carpetas, tablas y columnas.
- Bronze conserva el nombre de la fuente (`customers_orgs`, `usage_events`).
- Las columnas de partición se llaman por lo que son: `event_date`, `usage_date`, `month`, `date`.
- Las columnas técnicas empiezan con su función: `ingest_ts`, `source_file`, `processed_ts`.
- Los flags de calidad terminan en `_flag` (por ejemplo, `anomaly_flag`).

## 4.6 Metadatos y trazabilidad

| Columna | Zona | Para qué |
|---|---|---|
| `ingest_ts` | Bronze | Saber cuándo se cargó cada fila |
| `source_file` | Bronze | Saber de qué archivo vino cada fila |
| `schema_version` | Bronze y Silver (eventos) | Distinguir eventos v1 y v2 |
| `processed_ts` | Silver y Gold | Saber cuándo se generó cada fila |
| flags de calidad (`*_flag`) | Silver | Marcar datos sospechosos sin borrarlos |
| `exchange_rate_original` | Silver (billing) | Guardar el FX original antes de aplicar D-01 |

Con `source_file` e `ingest_ts` puedo rastrear cualquier dato de Gold hasta el archivo original de Landing (**linaje**).

## 4.7 Retención (cuánto tiempo guardo los datos)

| Zona | Retención | Por qué |
|---|---|---|
| Landing | Indefinida | Es el original: si todo falla, se reprocesa desde acá |
| Bronze | Indefinida | Es la base para regenerar Silver y Gold |
| Silver y Gold | Se pueden borrar y regenerar | Se recalculan desde Bronze |
| Quarantine | 90 días (supuesto) | Tiempo razonable para revisar y corregir |
| `_checkpoints` | Mientras exista el streaming | Si se borran, el streaming vuelve a procesar todo desde cero |

## 4.8 Reglas de promoción (cuándo un dato pasa a la zona siguiente)

| Paso | Qué se tiene que cumplir | Si no se cumple |
|---|---|---|
| Landing → Bronze | El archivo se puede leer con el esquema definido y la clave no es nula | El registro va a quarantine |
| Bronze → Silver | Pasa las reglas de calidad (por ejemplo: `event_id` no nulo y único, `unit` no nulo si hay `value`, costo ≥ −0,01) | El registro va a quarantine con el nombre de la regla que falló |
| Silver → Gold | Los conteos cierran: lo que entra a la agregación coincide con lo que sale | No se publica el mart y queda registrado en el log |

Los costos muy altos (spikes) **no** van a quarantine: son datos válidos pero raros. Quedan en Silver marcados con `anomaly_flag`.

## 4.9 Escritura e idempotencia

Idempotencia significa que si ejecuto el pipeline dos veces, el resultado es el mismo y no se duplica nada.

- **Bronze (streaming):** deduplico por `event_id` y uso checkpoint, así un evento ya procesado no se vuelve a cargar.
- **Bronze (batch):** reescribo la tabla completa o la partición del mes.
- **Silver y Gold:** reescribo **solo las particiones afectadas** (modo `overwrite` dinámico por partición), en lugar de agregar filas encima.
