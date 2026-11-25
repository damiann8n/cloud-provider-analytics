# Backlog para el MVP final (09/12/2026)
## ARCHIVOS .md REALIZADOS CON AYUDA DE CLAUDE IA (especialmente tablas y enlaces a documentación)

Acá ordeno lo que me falta para la entrega final, como pide la consigna después de la segunda entrega: **obligatorio**, **deseable** y **fuera de alcance**.

## Obligatorio (lo pide la consigna para el final)

| # | Tarea | Para qué |
|---|---|---|
| 0 | Silver de `customers_orgs` (NPS fuera de −100..100 a null) | Completar "Silver de al menos un maestro" de la segunda entrega |
| 1 | Silver de `billing_monthly` con el tipo de cambio de D-01 | Mart `revenue_by_org_month` · consulta 4 |
| 2 | Silver de `support_tickets` (CSAT entre 1 y 5) | Mart `tickets_by_org_date` · consulta 3 |
| 3 | Mart `genai_tokens_by_org_date` | Consulta 5 |
| 4 | Detección de anomalías con **MAD por servicio** (umbral 3,5) y mart `cost_anomaly_mart` | FinOps: costos raros sin borrarlos |
| 5 | Tablas en AstraDB y CQL para las consultas 3, 4 y 5 | Las 5 consultas obligatorias |
| 6 | Componente de Machine Learning (ver abajo) | Analítica o ML integrado |
| 7 | Diagrama final, diccionario de datos, presentación y video | Artefactos finales |
| 8 | Tests chicos (JSONL de 5 líneas con casos buenos y malos, con `assert`) | Pruebas automatizadas |

## Plan de Machine Learning

En la clase del 18/11 empezamos ML sobre Spark y todavía no vimos MLlib en código, así que para esta entrega dejo el plan (la consigna permite "una alternativa equivalente y su plan de cierre").

- **Problema:** segmentar las organizaciones por su perfil de consumo. Es **no supervisado** porque no tengo una etiqueta: no sé de antemano a qué grupo pertenece cada org.
- **Modelo:** **K-Means** de Spark MLlib.
- **Features (una fila por org):** costo total, requests, cpu_hours, storage_gb_hours, tokens GenAI, carbono, cantidad de servicios usados, tickets y NPS. Salen de Gold, que ya tengo.
- **Pipeline:** `VectorAssembler` → `StandardScaler` → `KMeans`, igual que el flujo de scikit-learn visto en clase pero traducido a MLlib.
- **Métrica:** *silhouette* para elegir la cantidad de grupos (k entre 2 y 6). No uso accuracy porque no hay etiqueta.
- **Uso:** guardar el segmento de cada org en Gold y en Astra, para que FinOps vea qué grupo gasta más.
- **Por qué Spark:** como vimos en clase, Spark aporta sobre todo en la preparación de features (agregar 43.200 eventos o muchos más) y en la inferencia batch. K-Means usa **paralelismo de datos**: cada worker calcula las distancias de su partición y después se combinan los centroides.
- **Limitación:** con 80 organizaciones el modelo es chico; el valor está en dejar el pipeline armado para cuando haya muchos más clientes.

## Deseable (si me da el tiempo)

| # | Tarea |
|---|---|
| 1 | Ejecutar el CQL real desde Colab con `cassandra-driver` y el Secure Connect Bundle (D-06) |
| 2 | Experimento con un watermark chico para mostrar cuántos eventos descarta (D-05) |
| 3 | Pasar la configuración a leerse desde `config/config.json` del repo en lugar de la celda |
| 4 | Silver de `users` y `resources` |
| 5 | Preguntas complementarias QA, QB y QC del documento 01 |

## Fuera de alcance

- Kafka u otro broker (el profesor aclaró que no se exige; la fuente streaming es la carpeta de JSONL).
- Análisis de marketing y de recursos ociosos.
- Delta Lake / Iceberg (queda como evolución futura en D-02).
- Dashboards conectados a AstraDB.
