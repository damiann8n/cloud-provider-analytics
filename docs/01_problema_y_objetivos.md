# 1. Interpretación del problema, usuarios y objetivos

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 2026-10-03

## 1.1 Problema

El área de datos de un proveedor de nube recibe datos crudos e inconsistentes desde dos tipos de fuentes:

- **Stream de eventos de uso** (`usage_events_stream/*.jsonl`), con evolución de esquema (v1 → v2 desde ~2025-07-18), tipos ambiguos, nulos, costos negativos y outliers.
- **Maestros batch** (CRM, usuarios, recursos, facturación, tickets, NPS, marketing) con nulos y valores ruidosos.

Hoy no existe una vista **confiable, conformada y consultable** de costos, uso y soporte por organización. El objetivo es construir un pipeline que ingiera, limpie, conforme y publique estos datos para su consumo analítico.

## 1.2 Usuarios y preguntas de negocio

### Preguntas principales (obligatorias, servidas desde Cassandra/AstraDB)

| # | Usuario | Pregunta |
|---|---|---|
| Q1 | FinOps | ¿Cuáles son los costos y requests diarios por organización y servicio en un rango de fechas? |
| Q2 | FinOps | ¿Cuáles son los Top-N servicios por costo acumulado en los últimos 14 días para una organización? |
| Q3 | Soporte | ¿Cómo evolucionan los tickets críticos y la tasa de SLA breach por día en los últimos 30 días? |
| Q4 | FinOps | ¿Cuál es el revenue mensual con créditos e impuestos aplicados, normalizado a USD? |
| Q5 | Producto / GenAI | ¿Cuántos tokens GenAI se consumen por día y a qué costo estimado? |

### Preguntas complementarias (valor agregado)

| # | Usuario | Pregunta | Fuentes |
|---|---|---|---|
| QA | FinOps | ¿Cuál es el costo unitario (USD/request, USD/cpu_hour, USD/GB-hour) por servicio y región? | usage_events |
| QB | Soporte / Customer Success | ¿Las organizaciones con más tickets críticos o SLA breach presentan peor NPS o estado `churned`? | support_tickets, nps_surveys, customers_orgs |
| QC | Producto / Sostenibilidad | ¿Qué servicios y regiones generan más `carbon_kg` por USD gastado? | usage_events (v2) |

Quedan como **extensiones fuera de alcance** (backlog): conversión por canal de marketing vs consumo (`marketing_touches`) y detección de usuarios/recursos ociosos (`users`, `resources`).

## 1.3 Objetivos medibles

| # | Objetivo | Métrica / verificación |
|---|---|---|
| O1 | Completitud | 100 % de los eventos leídos termina en Silver o en quarantine (sin pérdidas silenciosas). Conteo: `entrada = silver + quarantine`. |
| O2 | Idempotencia | 0 duplicados tras re-ejecutar el pipeline (conteos antes/después por clave natural). |
| O3 | Latencia | Evento disponible en Gold/Serving en **< 5 minutos** desde su llegada a Landing. |
| O4 | Serving query-first | Q1–Q5 se responden cada una con **una sola tabla** Cassandra, sin joins ni `ALLOW FILTERING`. |
| O5 | Evolución de esquema | Eventos v1 y v2 unificados en Silver sin pérdida de campos (`carbon_kg`, `genai_tokens` nulos en v1). |
| O6 | Observabilidad de calidad | Tasa de quarantine publicada por fuente y por regla en cada ejecución. |
| O7 | Linaje | 100 % de las filas en Bronze/Silver conserva `source_file` e `ingest_ts`. |
| O8 | Anomalías explicables | Cada flag de anomalía registra método (z-score / MAD), umbral y valor base, auditable por FinOps. |

### Justificación del umbral de latencia (O3)

- **Negocio:** FinOps necesita detectar spikes de costo dentro de la misma hora para actuar (apagar recursos, alertar al cliente). 5 minutos cubre ese caso con margen; latencia sub-segundo no aporta valor porque la facturación es diaria/mensual.
- **Técnica:** con un trigger de Structured Streaming de ~1 minuto, el procesamiento del micro-lote y la escritura a Gold y Cassandra entran holgadamente en 5 minutos en un entorno tipo Colab.
- **Trade-off (decisión abierta):** el watermark para late data agrega latencia a las agregaciones por ventana; su valor se define en el diseño del streaming.

## 1.4 Criterio de éxito global

Un usuario de FinOps, Soporte o Producto puede responder sus preguntas (Q1–Q5, QA–QC) consultando AstraDB o Gold, sin acceder a los datos crudos y con trazabilidad hasta el archivo de origen.
