# Registro de decisiones
## ARCHIVOS .md REALIZADOS CON AYUDA DE CLAUDE IA (especialmente tablas y enlaces a documentación)

Acá anoto las decisiones técnicas que voy tomando en el proyecto: qué decidí, por qué y qué alternativas descarté.
Cada una tiene un número (D-01, D-02, …) para poder citarla desde los otros documentos.

---

## D-01 · Tipo de cambio en la facturación

**Fecha:** 04/10/2026 · **Estado:** decidida (la decisión es mía y la justifico acá)

**Qué encontré:** en `billing_monthly.csv`, el campo `exchange_rate_to_usd` cambia de una factura a otra aunque sean de la misma moneda y el mismo mes. La variación es de ±10 % y aparece en las tres monedas:

- USD: entre 0,85 y 1,12 (y un dólar siempre vale 1 dólar).
- EUR: promedio cercano a 1,10.
- ARS: promedio cercano a 0,0015.

Es decir, el euro de junio no vale lo mismo para todos los clientes. Esto no pasa en la realidad: es ruido que agregó el generador del dataset sintético.

**Qué decidí:** usar **un solo tipo de cambio por moneda y por mes**, calculado como el promedio de ese mes. Para USD lo fijo en 1. El valor original lo guardo en otra columna (`exchange_rate_original`) para no perderlo y poder revisarlo.

**Alternativas que descarté:**

- Respetar el valor de cada factura y solo forzar USD = 1: el euro y el peso seguirían con ruido.
- Usar todo tal cual y solo marcar las filas: el revenue en USD saldría distorsionado.

**Impacto:** consulta 4 (revenue mensual en USD) y mart `revenue_by_org_month`.

---

## D-02 · Patrón de arquitectura: Lambda

**Fecha:** 04/10/2026 · **Estado:** decidida

**Qué decidí:** usar el patrón **Lambda**, con dos caminos:

- **Batch:** para los CSV (clientes, usuarios, recursos, tickets, NPS y facturación). Se procesan una vez por día o por mes.
- **Streaming:** para los eventos de uso (`usage_events_stream/*.jsonl`), que llegan de a poco en micro-lotes.

Los dos caminos se juntan en la capa Gold.

**Por qué:**

- Los CSV son datos maestros que cambian poco (por ejemplo, 80 clientes y una facturación por mes). Tratarlos como stream no me aporta nada y complica el diseño.
- Los eventos sí llegan en forma continua, así que necesitan streaming.
- La consigna pide como mínimo "streaming de eventos y batch de maestros/facturación", que es justamente lo que propone Lambda.
- Es el patrón que mejor puedo explicar y defender.

**La desventaja (y cómo la manejo):** Lambda tiene dos caminos que mantener, y hay que cuidar que los datos coincidan cuando se unen. En mi caso el problema es menor, porque Spark usa la misma forma de trabajar (DataFrames) para batch y para Structured Streaming: son dos caminos, pero escritos con la misma herramienta.

**Alternativas que descarté:**

- **Kappa** (todo como stream): sirve cuando todas las fuentes ya son eventos, por ejemplo con Kafka. Acá los maestros son archivos CSV que llegan cada tanto.
- **Híbrido:** no encontré un motivo concreto que justifique mezclar los patrones.

**Evolución futura:** si el volumen creciera mucho, el paso siguiente sería una arquitectura **Lakehouse**, con Delta Lake o Iceberg sobre Parquet. Agrega transacciones, historial de versiones y actualizaciones sin duplicar, y permite procesar batch y streaming como si fueran un solo camino.

---

## D-03 · Particionar solo por fecha

**Fecha:** 04/10/2026 · **Estado:** decidida

**Qué decidí:** particionar las tablas grandes solo por fecha (`event_date`, `usage_date`, `date` o `month`, según la tabla). Los maestros chicos no se particionan.

**Por qué:** casi todas las consultas y subconsultas se hacen por fecha, así que esta partición debería ser la más eficiente. Las 5 consultas obligatorias filtran por fecha (rango, últimos 14 días, últimos 30 días, mes y día), y así Spark puede leer solo las carpetas necesarias (*partition pruning*).

**Alternativa que descarté:** particionar también por servicio. Con el volumen actual (43.200 eventos) quedarían 360 carpetas de unas 120 filas cada una: demasiados archivos chicos, que hacen más lento a Spark. La revisaría si el volumen creciera.

**Detalle:** ver [docs/04_data_lake.md](docs/04_data_lake.md).

---

## D-04 · Watermark amplio en el streaming

**Fecha:** 04/10/2026 · **Estado:** cerrada en el Parcial 2 (ver D-05)

**Qué encontré:** cada archivo JSONL trae eventos de los dos meses (julio y agosto) mezclados y desordenados.

**El problema:** el watermark descarta los eventos que llegan con más de cierto atraso. Con un valor típico (por ejemplo, 10 minutos), después del primer archivo Spark descartaría casi todos los eventos de julio por considerarlos tardíos.

**Qué decidí:**

- Usar un **watermark amplio**. El valor exacto lo defino en el Parcial 2, probándolo con los datos reales.
- No depender del watermark para que Gold quede correcto: en cada micro-lote (`foreachBatch`) recalculo los días que llegaron y reescribo esas particiones.

**Trade-off:** un watermark amplio hace que Spark guarde en memoria más estado para deduplicar. Con el volumen de este dataset no es un problema, pero con muchos más datos habría que revisarlo.

**Detalle:** ver [docs/05_flujos.md](docs/05_flujos.md).

---

## D-05 · Watermark de 60 días y deduplicación por event_id

**Fecha:** 24/11/2026 · **Estado:** decidida (cierra D-04)

**Qué decidí:**

- Watermark de **60 días** sobre `event_ts` (la fecha del evento, no la de carga).
- Deduplicación con `dropDuplicatesWithinWatermark(["event_id"])`.
- Lectura de **10 archivos por micro-lote** (`maxFilesPerTrigger = 10`), o sea 12 micro-lotes para los 120 archivos, con `trigger(availableNow=True)`.

**Por qué 60 días:** el dataset va del 03/07 al 31/08/2025 (unos 60 días) y cada archivo trae eventos de los dos meses mezclados. Con un watermark chico, después del primer micro-lote Spark daría por "tardíos" casi todos los eventos de julio y los descartaría. Con 60 días no se pierde ningún evento: entraron los 43.200.

**Late data:** aunque un evento llegue tarde, no rompe Gold. La segunda query (`foreachBatch`) recalcula **completos los días que aparecen en cada micro-lote**, leyendo de Bronze todos los eventos de esos días, y pisa solo esas particiones.

**Trade-off:** un watermark grande obliga a Spark a guardar más estado (los `event_id` vistos). Con este volumen el checkpoint ocupa 3 MB; con mucho más volumen habría que achicarlo.

**Evidencia:** prueba 2 de idempotencia: reenvié un archivo ya procesado con otro nombre; Spark leyó sus 360 filas y las descartó todas como duplicadas.

---

## D-06 · AstraDB: base vector, default_keyspace, Data API y claves de las tablas

**Fecha:** 24/11/2026 · **Estado:** decidida

**Qué decidí:**

- Base **Serverless (vector)**, que trae habilitada la Data API.
- Uso el keyspace **`default_keyspace`**: al intentar crear `cloud_analytics`, Astra me dijo que no tenía permisos con el plan gratuito.
- Me conecto desde Colab con **astrapy** (`DataAPIClient` + endpoint + token), como mostró el profesor en clase. El token va en Colab Secrets.
- Dos tablas **query-first**, una por consulta:

| Tabla | Clave primaria | Consulta que resuelve |
|---|---|---|
| `org_daily_usage_by_service` | `((org_id), usage_date, service)` | Costos y requests de una org en un rango de fechas |
| `top_services_14d` | `((org_id), cost_14d DESC, service)` | Top-N servicios por costo en los últimos 14 días |

**Por qué esas claves:** la partición es `org_id` porque todas las consultas son "de una organización" y así una consulta lee una sola partición (80 orgs, particiones acotadas). En la primera tabla el clustering por `usage_date` permite pedir un rango de fechas; en la segunda, el clustering por `cost_14d DESC` hace que Cassandra ya guarde los servicios ordenados de mayor a menor, así que el top-N es un `LIMIT` sin ordenar nada.

**Sobre el CQL:** esta base no tiene consola CQL en el portal. Dejé el CQL equivalente (CREATE TABLE y SELECT) en [cql/consultas.cql](cql/consultas.cql) y ejecuté las mismas consultas con la Data API (celda 15). El resultado está en [evidence/consultas_astra.txt](evidence/consultas_astra.txt).

**Idempotencia y errores:** `insert_many` en tablas funciona como **upsert** (pisa por clave primaria). Astra a veces devuelve un error temporal justo después de crear una tabla; como la carga es upsert, se puede reintentar sin duplicar.

**Alternativa que dejé para el final:** ejecutar el CQL real con `cassandra-driver` y el *Secure Connect Bundle* (ver BACKLOG).

---

## D-07 · Reglas de calidad, quarantine y control de archivos chicos

**Fecha:** 24/11/2026 · **Estado:** decidida

**Reglas de calidad** (entre Bronze y Silver; Bronze guarda todo, como dijo el profesor):

| Regla | Motivo en quarantine | Registros |
|---|---|---|
| La línea no se pudo leer | `registro_corrupto` | 0 |
| `event_id` no nulo | `event_id_nulo` | 0 |
| `cost_usd_increment >= -0.01` | `costo_negativo` | 211 |
| `unit` no nulo cuando hay `value` | `unit_nulo_con_value` | 2.033 |

- Marco en vez de descartar: cada registro guarda el **motivo** de la primera regla que falla.
- Las validaciones son null-safe (`coalesce(..., False)`), así un null no rompe la comparación.
- `value` llega a veces como texto: lo convierto con `try_cast`, que devuelve null en lugar de cortar el programa (Spark 4 tiene el modo ANSI activado).
- Los **spikes** (costos muy altos) no van a quarantine: son válidos. Su detección con MAD queda para el final (BACKLOG).

**Archivos chicos:** en la primera corrida el Data Lake quedó con **12.006 archivos Parquet** y Bronze ocupaba 72 MB (más que los 13 MB de JSONL originales). Pasaba porque cada micro-lote escribía un archivo por cada día × cada partición del shuffle (8). Lo corregí con `repartition("event_date")` antes de cada escritura (1 archivo por día):

| | Antes | Después |
|---|---|---|
| Archivos Parquet | 12.006 | 910 |
| Bronze eventos | 72 MB | 12 MB |
| Silver eventos | 62 MB | 3,3 MB |
| Quarantine | 21 MB | 1,2 MB |

**Evidencia:** [rutas_y_tamanios_ANTES.txt](evidence/rutas_y_tamanios_ANTES.txt) y [rutas_y_tamanios.txt](evidence/rutas_y_tamanios.txt).
