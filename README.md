# Cloud Provider Analytics
## ARCHIVOS .md REALIZADOS CON AYUDA DE CLAUDE IA (especialmente tablas y enlaces a documentación)

Proyecto integrador de **Minería de Datos II** · ISTEA · 2.º cuatrimestre 2026 · Prof. Diego Mosquera
Autor: Damián Silva (`damiann8n`) · Trabajo individual

Pipeline de datos con **PySpark** para el área de datos de un proveedor de nube: ingesta batch y streaming, Data Lake en Parquet (Landing → Bronze → Silver → Gold) y serving en **Cassandra/AstraDB** para analítica de **FinOps, Soporte y Producto**.

## Estado del proyecto

| Instancia | Fecha límite | Estado |
|---|---|---|
| Primera entrega · Diseño y fundación | 07/10/2026 19:00 h | ✅ Entregada |
| Segunda entrega · Implementación técnica | 25/11/2026 19:00 h | ✅ Entregada |
| Evaluación final · MVP y defensa | 09/12/2026 19:00 h | ⚪ Pendiente (ver [BACKLOG.md](BACKLOG.md)) |

## Segunda entrega · Implementación técnica (25/11/2026)

Esta tabla sigue el checklist de la consigna (sección 9.2) y dice dónde encontrar cada punto.

| Ítem de la consigna | Dónde está |
|---|---|
| Correcciones de la primera entrega | [DECISIONS.md](DECISIONS.md): D-01 decidida, D-04 cerrada con D-05 |
| Arquitectura actualizada | [03 · Arquitectura](docs/03_arquitectura.md), sección 3.5 |
| Tres maestros batch en Bronze | [01 · Pipeline](notebooks/01_pipeline.ipynb), celdas 4 y 5 (los 7 CSV) |
| Streaming con esquema, watermark, dedupe, late data y checkpoint | [01 · Pipeline](notebooks/01_pipeline.ipynb), celdas 6 y 7 · [D-05](DECISIONS.md) |
| Silver para eventos y al menos un maestro | Silver de eventos: [01 · Pipeline](notebooks/01_pipeline.ipynb), celdas 8, 9 y 11. El maestro `customers_orgs` se usa desde Bronze para el join; su Silver queda en el [BACKLOG](BACKLOG.md) |
| Tres features calculadas | `requests`, `cpu_hours`, `storage_gb_hours`, `daily_cost_usd`, `genai_tokens`, `carbon_kg` (celdas 9 y 10) |
| Tres reglas de calidad y quarantine | Celda 8 · [D-07](DECISIONS.md) · [muestra de quarantine](evidence/quarantine_muestra.csv) |
| Mart `org_daily_usage_by_service` en Gold | Celdas 10 y 11 |
| Keyspace y tabla Cassandra/AstraDB poblada | Celdas 13 y 14 · [D-06](DECISIONS.md) |
| Dos consultas CQL con resultados | [cql/consultas.cql](cql/consultas.cql) · celda 15 · [resultado](evidence/consultas_astra.txt) |
| Idempotencia demostrada | Celda 16 · [conteos antes/después](evidence/conteos_e_idempotencia.txt) |
| Gobierno preliminar y pruebas | [09 · Gobierno, calidad y pruebas](docs/09_gobierno_y_pruebas.md) |
| README Quickstart, logs y evidencias | Este README · carpeta [evidence/](evidence/) |
| Backlog final priorizado (incluye el plan de ML) | [BACKLOG.md](BACKLOG.md) |

### Resultados principales

| Capa | Filas |
|---|---|
| Landing (eventos JSONL) | 43.200 en 120 archivos |
| Bronze eventos | 43.200 |
| Quarantine | 2.244 (2.033 `unit_nulo_con_value` + 211 `costo_negativo`) |
| Silver eventos | 40.956 |
| Gold `org_daily_usage_by_service` | 11.050 |
| Gold `top_services_14d` | 262 |
| AstraDB | 11.050 + 262 filas |

Quarantine + Silver = 43.200: no se pierde ningún evento.

## Quickstart (Google Colab)

### Primera vez: credenciales de AstraDB

1. Crear una base **Serverless (vector)** en [astra.datastax.com](https://astra.datastax.com) y generar un *Application Token* con rol *Database Administrator*.
2. En Colab, abrir el panel **Secrets** (ícono de la llave) y cargar:
   - `ASTRA_DB_APPLICATION_TOKEN` → el token (`AstraCS:...`)
   - `ASTRA_DB_API_ENDPOINT` → el API Endpoint de la base
3. Activar **Notebook access** en los dos.

Las credenciales **nunca** se escriben en el notebook ni en el repositorio.

### Ejecución

1. Abrir el pipeline en Colab:
   [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/damiann8n/cloud-provider-analytics/blob/main/notebooks/01_pipeline.ipynb)
2. **Entorno de ejecución → Ejecutar todas** (tarda entre 5 y 8 minutos).
3. Si Colab pide permiso para usar los secrets, aceptar.

El notebook instala PySpark y astrapy, clona este repo, descomprime el dataset en `/content/datalake/landing` y corre todo el pipeline hasta AstraDB.

**Si aparece un error en la carga a AstraDB:** a veces el servidor devuelve un error temporal justo después de crear una tabla. La carga es un *upsert*, así que alcanza con volver a ejecutar esa celda: no duplica filas.

### Reinicio y limpieza

La primera celda borra `/content/datalake` y lo vuelve a crear desde el zip, así que **"Ejecutar todas" siempre arranca de cero** (incluidos los checkpoints). Las tablas de AstraDB se pisan por clave primaria.

**Requisitos:** cuenta de Google (Colab) · Python 3 · PySpark 4.0 y astrapy 2.x (se instalan solos) · Java (incluido en Colab) · cuenta gratuita de AstraDB.

## Primera entrega · Diseño y fundación (07/10/2026)

| Ítem de la consigna | Dónde está |
|---|---|
| Interpretación del caso y objetivos medibles | [01 · Problema y objetivos](docs/01_problema_y_objetivos.md) |
| Análisis 5V | [02 · 5V e inventario](docs/02_5v_e_inventario_fuentes.md) (2.1) y [07 · Matriz](docs/07_matriz_requisitos.md) (7.3) |
| Inventario y perfil de fuentes | [02 · 5V e inventario](docs/02_5v_e_inventario_fuentes.md) (2.2 y 2.3) |
| Arquitectura v1 y patrón justificado | [03 · Arquitectura](docs/03_arquitectura.md) y [D-02](DECISIONS.md) |
| Diseño Landing / Bronze / Silver / Gold | [04 · Data Lake](docs/04_data_lake.md) |
| Flujos batch y streaming | [05 · Flujos](docs/05_flujos.md) |
| Lógica MapReduce o equivalente | [06 · MapReduce](docs/06_mapreduce.md) |
| Matriz requisito–componente | [07 · Matriz](docs/07_matriz_requisitos.md) |
| Supuestos, riesgos, mitigaciones y estimación de esfuerzo | [08 · Supuestos, riesgos y plan](docs/08_supuestos_riesgos_plan.md) |
| Evidencia mínima de lectura y exploración de datos | [00 · Exploración](notebooks/00_exploracion_datos.ipynb) |
| Registro de decisiones | [DECISIONS.md](DECISIONS.md) |

## Estructura del repositorio

```
cloud-provider-analytics/
├── config/        Configuración externalizada (sin credenciales)
├── cql/           CREATE TABLE y consultas CQL de AstraDB
├── data/          Dataset de muestra (zip)
├── docs/          Documento de diseño por secciones
├── evidence/      Conteos, tamaños, muestras de quarantine y resultados de consultas
├── notebooks/     00 exploración · 01 pipeline completo
├── BACKLOG.md     Trabajo pendiente para el final
├── DECISIONS.md   Registro de decisiones
└── README.md
```

## Datos

Dataset sintético `cloud_provider_challenge_dataset_v1` provisto por la cátedra: 7 fuentes CSV (organizaciones, usuarios, recursos, tickets, marketing, NPS, facturación) y 120 archivos JSONL de eventos de uso (43.200 eventos, jul–ago 2025, esquema v1/v2).

## Convenciones

- **Nombres:** `snake_case` para archivos, columnas y tablas; documentos numerados (`NN_tema.md`).
- **Datos crudos inmutables:** el zip de `data/` actúa como Landing y nunca se modifica.
- **Sin secretos:** las credenciales van en Colab Secrets; nunca en el repositorio.
- **Commits:** en español, descriptivos y por unidad de cambio.
- **Decisiones técnicas:** se registran con identificador (`D-01`, `D-02`, …).

## Tecnologías

PySpark 4.0 · Spark Structured Streaming · Parquet · DataStax AstraDB (Cassandra) · astrapy (Data API) · Google Colab · GitHub
