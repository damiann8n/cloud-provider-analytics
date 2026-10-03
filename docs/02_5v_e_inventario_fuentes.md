# 2. Justificación Big Data (5V) e inventario de fuentes

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 2026-10-03 · Perfil obtenido sobre `cloud_provider_challenge_dataset_v1`

## 2.1 Análisis de las 5V

El dataset provisto es una **muestra sintética** (~13 MB, 80 organizaciones, 60 días). La justificación de Big Data se basa en el **escenario productivo que representa**; la muestra permite validar el diseño a escala reducida.

| V | Evidencia en el dataset | Proyección productiva (supuesto) | Implicancia de arquitectura |
|---|---|---|---|
| **Volumen** | 43.200 eventos en 60 días para 80 orgs (~720 eventos/día); 120 archivos JSONL de ~108 KB. | Un proveedor real con ~10.000 orgs y granularidad por minuto genera cientos de millones de eventos/mes (TB/año). | Procesamiento distribuido con Spark; Parquet columnar y particionado por fecha/servicio. |
| **Velocidad** | Eventos con timestamp por minuto, entregados fragmentados en micro-lotes; llegan **desordenados** dentro de cada archivo. | Flujo continuo 24×7. | Structured Streaming con trigger ~1 min, watermark para late data y checkpointing (objetivo O3: < 5 min). |
| **Variedad** | 7 CSV (maestros, facturación, tickets, encuestas, marketing) + JSONL semiestructurado; JSON embebido en `resources.tags_json`; esquema v1/v2 en eventos. | Nuevas versiones de esquema y nuevas fuentes. | Esquemas explícitos por fuente, capa Silver de conformado y manejo de evolución de esquema. |
| **Veracidad** | `value` nulo (877) o como string (1.309); `unit` nulo (2.075); 216 costos negativos; spikes (p99 = 16,7 USD vs máx. 317 USD); NPS fuera de rango; nulos en CSAT, créditos, last_login. | Calidad variable por origen. | Reglas de calidad, quarantine en Parquet, flags de anomalía (z-score/MAD), métricas de calidad por ejecución. |
| **Valor** | Permite responder Q1–Q5 y QA–QC (costos, revenue, SLA, GenAI, carbono). | Decisiones de FinOps, retención de clientes y producto. | Marts Gold por dominio y serving query-first en Cassandra/AstraDB. |

## 2.2 Inventario y perfil de fuentes

| Fuente | Filas | Grano | Clave natural | Frecuencia | Tipo de carga |
|---|---|---|---|---|---|
| `customers_orgs.csv` | 80 | 1 fila por organización | `org_id` | Diaria (maestro CRM) | Batch · SCD candidato |
| `users.csv` | 800 | 1 fila por usuario | `user_id` | Diaria | Batch |
| `resources.csv` | 400 | 1 fila por recurso cloud | `resource_id` | Diaria | Batch |
| `support_tickets.csv` | 1.000 | 1 fila por ticket | `ticket_id` | Diaria (incremental) | Batch |
| `marketing_touches.csv` | 1.500 | 1 fila por interacción | `touch_id` | Diaria | Batch (fuera de alcance analítico) |
| `nps_surveys.csv` | 92 | 1 fila por encuesta (org, fecha) | `org_id` + `survey_date` | Eventual | Batch |
| `billing_monthly.csv` | 240 | 1 fila por factura (org, mes) | `invoice_id` | Mensual (3 meses: jun–ago 2025) | Batch |
| `usage_events_stream/*.jsonl` | 43.200 | 1 fila por evento de métrica | `event_id` | Continua (micro-lotes) | Streaming |

## 2.3 Calidad, trazabilidad y riesgos por fuente

| Fuente | Problemas detectados | Tratamiento propuesto |
|---|---|---|
| `customers_orgs` | `nps_score`: 11 nulos y valores fuera de rango (−38 a 101; el NPS válido es −100..100); `is_enterprise` como texto. | Cast booleano; NPS fuera de rango → nulo + flag; nulos se mantienen (no imputar). |
| `users` | `last_login`: 139 nulos (usuarios sin login). | Nulo semántico ("nunca ingresó"); no es error. |
| `resources` | `tags_json`: 83 nulos; JSON embebido en CSV. | Parseo a `array<string>`; nulo → array vacío. |
| `support_tickets` | `resolved_at`: 240 nulos (tickets abiertos); `csat`: 254 nulos y valores fuera de escala (0, 6, 7 sobre escala 1–5). | Abierto = `resolved_at` nulo; CSAT fuera de 1–5 → nulo + flag. |
| `nps_surveys` | `nps_score`: 19 nulos; `comment`: 10 nulos; solo 60 de 80 orgs con encuesta. | Nulos se conservan; join left con orgs. |
| `billing_monthly` | `subtotal`: 13 negativos (ajustes/notas de crédito); `credits`: 137 nulos; 3 monedas (USD, ARS, EUR); **`exchange_rate_to_usd` ≠ 1 en las 160 facturas USD** (0,85–1,12). | `credits` nulo → 0; normalizar a USD con FX de la fila; regla de calidad: si `currency = USD` y FX ≠ 1 → **se fuerza FX = 1 y se marca `fx_corrected_flag`** (decisión D-01: contablemente, 1 USD = 1 USD). |
| `usage_events` | `value` nulo (877) o string numérico (1.309); `unit` nulo (2.075); 216 `cost_usd_increment` negativos (mín. −154,46); spikes (máx. 317,43); v1 (10.800, 03/07–17/07) sin `carbon_kg`/`genai_tokens`; v2 (32.400, 18/07–31/08); `genai_tokens` en 3.132 eventos; eventos desordenados. | Cast de `value` con fallback a nulo; reglas: `event_id` no nulo/único, `cost >= -0.01` (si no → flag), `unit` no nulo si hay `value`; unificación v1/v2 con columnas nulas; watermark para late data. |

### Integridad referencial

- 100 % de los `org_id` y `resource_id` de eventos existe en sus maestros.
- Sin duplicados por clave natural en ninguna fuente cruda. La deduplicación igual se implementa para garantizar **idempotencia ante reprocesos**.

### Trazabilidad

Ninguna fuente trae metadatos de ingesta. Se agregan en Bronze: `ingest_ts` y `source_file` (y `schema_version` en eventos).

### Riesgos de datos identificados

| Riesgo | Impacto | Mitigación |
|---|---|---|
| **Desfase temporal**: facturación desde jun-2025, eventos recién desde 03/07/2025. | No se puede conciliar uso vs facturación de junio. | Documentarlo como supuesto; conciliación solo jul–ago. |
| FX de USD distinto de 1. | Revenue en USD distorsionado. | Forzar FX = 1 + flag `fx_corrected_flag` (decisión D-01). |
| Escalas de NPS/CSAT inconsistentes. | Métricas de satisfacción sesgadas. | Validación de rango + flag. |
| Costos negativos y spikes. | Distorsión de costos y anomalías falsas. | Flag de anomalía con método robusto (MAD/percentiles), sin eliminar el dato. |
