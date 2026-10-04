# 3. Arquitectura v1

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 04/10/2026 · Patrón elegido: **Lambda** (ver [D-02](../DECISIONS.md))

## 3.1 Diagrama general

```mermaid
flowchart LR
    subgraph FUENTES["Fuentes"]
        CSV["7 archivos CSV<br/>clientes, usuarios, recursos,<br/>tickets, NPS, marketing, facturación"]
        JSONL["usage_events_stream<br/>120 archivos JSONL"]
    end

    subgraph LAKE["Data Lake · Parquet"]
        LAND["Landing<br/>datos crudos, no se modifican"]
        BB["Bronze batch<br/>maestros tipados"]
        BS["Bronze streaming<br/>eventos tipados"]
        SIL["Silver<br/>limpieza, joins,<br/>unión v1/v2, features"]
        QUA["Quarantine<br/>registros inválidos"]
        GOLD["Gold<br/>marts FinOps,<br/>Soporte y Producto"]
    end

    CAS[("Cassandra / AstraDB<br/>tablas por consulta")]
    USR["Consumo<br/>FinOps · Soporte · Producto"]

    CSV --> LAND
    JSONL --> LAND
    LAND -- "Batch · Spark<br/>diario / mensual" --> BB
    LAND -- "Structured Streaming<br/>watermark + dedupe + checkpoint" --> BS
    BB --> SIL
    BS --> SIL
    SIL -. "no cumple reglas" .-> QUA
    SIL --> GOLD
    GOLD -- "carga desde Spark" --> CAS
    CAS --> USR

    TRANS["Capacidades transversales: calidad · metadatos y linaje · seguridad · observabilidad"]
    CAS ~~~ TRANS

    classDef batch fill:#dbeafe,stroke:#1d4ed8,color:#0f172a
    classDef stream fill:#ffedd5,stroke:#c2410c,color:#0f172a
    classDef serve fill:#dcfce7,stroke:#15803d,color:#0f172a
    classDef bad fill:#fee2e2,stroke:#b91c1c,color:#0f172a
    class CSV,BB batch
    class JSONL,BS stream
    class CAS,USR serve
    class QUA bad
    classDef trans fill:#f3f4f6,stroke:#6b7280,color:#0f172a,stroke-dasharray:4 3
    class TRANS trans
```

**Cómo leerlo:** lo azul es el camino **batch** (los CSV) y lo naranja es el camino **streaming** (los eventos). Los dos se juntan en Silver y Gold. Lo verde es lo que ve el usuario final.

## 3.2 Componentes

| Componente | Qué hace | Herramienta |
|---|---|---|
| Fuentes | Los archivos que nos da la cátedra: 7 CSV y 120 JSONL | Archivos en el repo (`data/`) |
| Landing | Guardo los archivos tal cual llegan. No se tocan nunca | Carpeta del Data Lake |
| Ingesta batch | Leo los CSV con un esquema definido y los paso a Parquet | PySpark (`spark.read`) |
| Ingesta streaming | Leo los eventos a medida que aparecen, descarto duplicados y tolero los que llegan tarde | Spark Structured Streaming |
| Bronze | Mismos datos que la fuente, pero con tipos correctos y dos columnas extra: cuándo se cargaron (`ingest_ts`) y de qué archivo vienen (`source_file`) | Parquet particionado |
| Silver | Limpio, convierto tipos, uno eventos con los maestros, unifico v1 y v2 y calculo features | PySpark |
| Quarantine | Lo que no pasa las reglas de calidad se separa acá, para revisarlo y no perderlo | Parquet |
| Gold | Tablas listas para responder las preguntas de negocio (marts) | Parquet |
| Serving | Cargo los marts en tablas pensadas para cada consulta | Cassandra / AstraDB |
| Consumo | FinOps, Soporte y Producto consultan los resultados | CQL / herramientas de visualización |

## 3.3 Cómo encaja con Lambda

| Capa de Lambda | En mi proyecto |
|---|---|
| **Capa batch** | Camino de los CSV: Landing → Bronze batch → Silver |
| **Capa de velocidad** (*speed layer*) | Camino de los eventos: Landing → Bronze streaming → Silver |
| **Capa de servicio** (*serving layer*) | Gold → Cassandra / AstraDB |

## 3.4 Capacidades transversales

Son cosas que atraviesan todo el pipeline, no una sola etapa:

- **Calidad:** reglas de validación en cada paso y quarantine para lo que falla.
- **Metadatos y linaje:** cada fila guarda de qué archivo vino y cuándo se cargó, así puedo rastrearla hasta el origen.
- **Seguridad:** sin contraseñas ni tokens en el repo; las credenciales de AstraDB van por variables de entorno.
- **Observabilidad:** logs de cada ejecución con conteos (cuántos registros entraron, cuántos pasaron, cuántos fueron a quarantine).

El detalle de cada zona del Data Lake (formatos, particiones, nombres y retención) está en el documento 04.
