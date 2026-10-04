# 7. Matriz requisito–componente

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 04/10/2026

Esta matriz me sirve para comprobar que cada requisito del proyecto tiene al menos un componente que lo resuelve, y que no hay componentes "de más" que no respondan a ningún requisito.

Los requisitos salen de tres lugares:

- los **requisitos técnicos** de la consigna (sección 4.4);
- las **consultas** que tienen que responderse (Q1–Q5 obligatorias y QA–QC complementarias, ver [doc 01](01_problema_y_objetivos.md));
- los **objetivos medibles** que definí (O1–O8, ver [doc 01](01_problema_y_objetivos.md)).

## 7.1 Matriz

Referencias de los componentes: **BA** = ingesta batch · **ST** = ingesta streaming · **BR** = Bronze · **SI** = Silver · **QU** = Quarantine · **GO** = Gold · **CA** = Cassandra/AstraDB · **TR** = capacidades transversales.

| # | Requisito | BA | ST | BR | SI | QU | GO | CA | TR |
|---|---|:-:|:-:|:-:|:-:|:-:|:-:|:-:|:-:|
| R01 | Ingesta batch de CSV a Parquet con esquema y columnas técnicas | ✔ | | ✔ | | | | | ✔ |
| R02 | Ingesta streaming con watermark, deduplicación y checkpoint | | ✔ | ✔ | | | | | |
| R03 | Reglas de calidad y quarantine | ✔ | ✔ | | ✔ | ✔ | | | ✔ |
| R04 | Compatibilidad de esquema v1 / v2 | | ✔ | ✔ | ✔ | | | | |
| R05 | Features (costo diario, requests, cpu, storage, tokens, carbono) | | | | ✔ | | ✔ | | |
| R06 | Detección de anomalías de costo | | | | ✔ | | ✔ | | |
| R07 | Marts de Gold para FinOps, Soporte y Producto | | | | | | ✔ | | |
| R08 | Serving en Cassandra con tablas por consulta | | | | | | ✔ | ✔ | |
| R09 | Idempotencia (reprocesar sin duplicar) | ✔ | ✔ | ✔ | ✔ | | ✔ | ✔ | |
| R10 | Performance (particiones y control de archivos) | | | ✔ | ✔ | | ✔ | | |
| R11 | Gobierno: metadatos, linaje, seguridad y observabilidad | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ | ✔ |
| Q1 | Costos y requests diarios por organización y servicio | | ✔ | | ✔ | | ✔ | ✔ | |
| Q2 | Top-N servicios por costo (últimos 14 días) | | ✔ | | ✔ | | ✔ | ✔ | |
| Q3 | Tickets críticos y SLA breach (últimos 30 días) | ✔ | | | ✔ | | ✔ | ✔ | |
| Q4 | Revenue mensual en USD con créditos e impuestos | ✔ | | | ✔ | | ✔ | ✔ | |
| Q5 | Tokens GenAI y costo estimado por día | | ✔ | | ✔ | | ✔ | ✔ | |
| QA | Costo unitario por servicio y región | | ✔ | | ✔ | | ✔ | | |
| QB | Tickets / SLA vs NPS y churn | ✔ | | | ✔ | | ✔ | | |
| QC | Carbono por USD gastado | | ✔ | | ✔ | | ✔ | | |

## 7.2 Cómo cumplo cada requisito

| # | Cómo lo cumplo | Objetivo | Dónde está |
|---|---|---|---|
| R01 | Leo los CSV con `spark.read.schema(...)`, agrego `ingest_ts` y `source_file` y escribo en Parquet | O7 | [05](05_flujos.md) |
| R02 | `readStream` de a un archivo, `withWatermark`, `dropDuplicates(["event_id"])` y checkpoint | O2, O3 | [05](05_flujos.md), D-04 |
| R03 | Reglas en el paso Bronze → Silver; lo que falla va a quarantine con el nombre de la regla | O1, O6 | [04](04_data_lake.md) |
| R04 | En Silver, `carbon_kg` y `genai_tokens` quedan nulos para los eventos v1 | O5 | [04](04_data_lake.md) |
| R05 | Calculo las features en Silver y las sumo por día en Gold | — | [06](06_mapreduce.md) |
| R06 | Marco con `anomaly_flag` los costos extremos (método a definir: MAD o percentiles) | O8 | [04](04_data_lake.md) |
| R07 | Cinco marts con grano definido | — | [04](04_data_lake.md) |
| R08 | Una tabla de Cassandra por consulta, sin joins | O4 | se detalla en el Parcial 2 |
| R09 | Deduplicación por clave, checkpoint y reescritura por partición | O2 | [04](04_data_lake.md) |
| R10 | Partición por fecha y `coalesce` antes de escribir | — | [04](04_data_lake.md), D-03 |
| R11 | `ingest_ts` / `source_file` en cada fila, credenciales fuera del repo, logs con conteos | O6, O7 | [03](03_arquitectura.md), [04](04_data_lake.md) |
| Q1–Q5 | Cada consulta sale de un mart de Gold cargado en Cassandra | O4 | [04](04_data_lake.md) |
| QA–QC | Salen de Silver o Gold, sin tabla de Cassandra propia (no son obligatorias) | — | [01](01_problema_y_objetivos.md) |

## 7.3 Las 5V y las decisiones de arquitectura

La consigna pide relacionar las 5V con lo que decidí. Así queda:

| V | Qué vi en los datos | Qué decidí | Componente |
|---|---|---|---|
| **Volumen** | 43.200 eventos en 60 días; en un proveedor real serían muchísimos más | Uso Spark (procesamiento distribuido), Parquet comprimido y particiones por fecha | BR, SI, GO · D-03 |
| **Velocidad** | Los eventos llegan de a poco, en micro-lotes y desordenados | Structured Streaming con watermark y checkpoint; patrón Lambda | ST · D-02, D-04 |
| **Variedad** | CSV, JSONL, JSON dentro del CSV y dos versiones de esquema | Esquema explícito por fuente y unificación v1/v2 en Silver | BA, ST, SI |
| **Veracidad** | Nulos, tipos mezclados, costos negativos, spikes, FX con ruido | Reglas de calidad, quarantine, flags de anomalía y D-01 | SI, QU · D-01 |
| **Valor** | Preguntas de FinOps, Soporte y Producto | Marts de Gold y tablas de Cassandra pensadas para cada consulta | GO, CA |

## 7.4 Control

- Todos los requisitos (R01–R11), las consultas (Q1–Q5, QA–QC) y las 5V tienen al menos un componente asignado.
- Todos los componentes cubren al menos un requisito: no hay ninguno de más.
- Gold y Silver son los componentes más cargados: si fallan, se caen casi todas las consultas. Por eso las reglas de calidad y los controles de conteo están ahí.
