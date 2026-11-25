# 9. Gobierno, calidad y pruebas (preliminar)

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 24/11/2026 · Ver también: [04 · Data Lake](04_data_lake.md) y [DECISIONS.md](../DECISIONS.md)

La consigna pide para esta entrega un **gobierno preliminar**: controles iniciales, metadatos, linaje, responsabilidades y acceso. Acá anoto lo que ya está implementado en el notebook `01_pipeline.ipynb`.

## 9.1 Controles de calidad

| Dónde | Control | Cómo lo verifico |
|---|---|---|
| CSV → Bronze | Esquema explícito por tabla y deduplicación por clave natural | Conteo leídas vs. en Bronze (celda 5) |
| JSONL → Bronze | Esquema explícito, modo `PERMISSIVE` con `_corrupt_record`, dedupe por `event_id` | Conteo final: 43.200 |
| Bronze → Silver | 3 reglas de la consigna + registro corrupto, con motivo (D-07) | Quarantine + Silver = Bronze (2.244 + 40.956 = 43.200) |
| Silver → Gold | La agregación se recalcula por día completo | Conteo de Gold igual en cada corrida (11.050) |

## 9.2 Metadatos y linaje

Cada evento guarda columnas que permiten seguirlo desde Gold hasta el archivo original:

| Columna | Capa | Para qué |
|---|---|---|
| `source_file` | Bronze y Silver | De qué archivo de Landing vino |
| `ingest_ts` | Bronze y Silver | Cuándo se cargó |
| `processed_ts` | Silver y Gold | Cuándo se procesó |
| `schema_version` | Bronze y Silver | Si el evento es v1 o v2 |
| `motivo` | Quarantine | Qué regla falló |

**Linaje:** Landing (`events_part_XXXX.jsonl`) → Bronze (`source_file`) → Silver (`event_id`, `source_file`) → Gold (`org_id`, `usage_date`, `service`) → AstraDB (misma clave).

## 9.3 Responsabilidades

Como el trabajo es individual, yo cumplo todos los roles. En un equipo real los separaría así:

| Rol | Responsabilidad |
|---|---|
| Ingeniero de datos | Ingesta, Bronze, Silver y Gold |
| Responsable de calidad | Reglas, revisión de quarantine |
| Dueño de datos FinOps | Definir qué consultas necesita y validar los marts |
| Administrador de AstraDB | Tokens, tablas y accesos |

## 9.4 Acceso y seguridad

- El token de AstraDB **no está en el repo ni en el notebook**: se lee desde **Colab Secrets** (`userdata.get`).
- El token tiene rol *Database Administrator* solo porque necesito crear tablas; para consultar alcanzaría uno de solo lectura.
- `config/config.json` no tiene credenciales: solo rutas, umbrales y nombres de tablas.
- Revisé que el notebook subido no contenga el token.

## 9.5 Observabilidad

- Cada celda imprime conteos (filas leídas, en Bronze, en quarantine, en Silver, en Gold).
- La query de Silver/Gold imprime cuántos días recalcula en cada micro-lote.
- La celda de evidencias guarda en `evidence/`: conteos, tamaños y cantidad de archivos, muestra de quarantine y resultado de las consultas.

## 9.6 Pruebas

| Prueba | Qué valida | Resultado |
|---|---|---|
| Conteos CSV → Bronze | No se pierden ni se duplican filas | 7 tablas iguales |
| Cierre Bronze = Quarantine + Silver | Ningún evento se pierde entre capas | 43.200 = 2.244 + 40.956 |
| Join con organizaciones | Todos los eventos tienen org | 0 eventos sin organización |
| Re-ejecución sin archivos nuevos | El checkpoint funciona | 0 filas nuevas |
| Re-entrega de un archivo ya procesado | La deduplicación funciona | 360 filas leídas, 0 agregadas |
| Recarga en AstraDB | El upsert no duplica | 156 filas de la org de ejemplo antes y después |
| Control de archivos chicos | `repartition` por día | 12.006 → 910 archivos |

Las pruebas automatizadas con datos chicos y `assert` quedan para el final (ver [BACKLOG.md](../BACKLOG.md)).

## 9.7 Manejo de errores

- Líneas JSON rotas: no cortan el stream, quedan en `_corrupt_record` y van a quarantine.
- Valores no numéricos en `value`: `try_cast` los deja en null en lugar de cortar la ejecución.
- Error temporal de AstraDB al cargar: se reintenta la celda; como es upsert, no duplica.
