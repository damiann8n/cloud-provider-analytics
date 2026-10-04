# Cloud Provider Analytics

Proyecto integrador de **Minería de Datos II** · ISTEA · 2.º cuatrimestre 2026 · Prof. Diego Mosquera
Autor: Damián Silva (`damiann8n`) · Trabajo individual

Pipeline de datos con **PySpark** para el área de datos de un proveedor de nube: ingesta batch y streaming, Data Lake en Parquet (Landing → Bronze → Silver → Gold) y serving en **Cassandra/AstraDB** para analítica de **FinOps, Soporte y Producto**.

## Estado del proyecto

| Instancia | Fecha límite | Estado |
|---|---|---|
| Primera entrega · Diseño y fundación | 07/10/2026 19:00 h | 🟡 En curso |
| Segunda entrega · Implementación técnica | 18/11/2026 19:00 h | ⚪ Pendiente |
| Evaluación final · MVP y defensa | 09/12/2026 19:00 h | ⚪ Pendiente |

## Quickstart (Google Colab)

1. Abrir el notebook de exploración en Colab:
   [![Open In Colab](https://colab.research.google.com/assets/colab-badge.svg)](https://colab.research.google.com/github/damiann8n/cloud-provider-analytics/blob/main/notebooks/00_exploracion_datos.ipynb)
2. **Entorno de ejecución → Ejecutar todas.**

El notebook instala PySpark, clona este repositorio, descomprime el dataset de `data/` en `data/raw/` y ejecuta el perfil de las fuentes. No requiere credenciales ni pasos manuales.

**Requisitos:** cuenta de Google (Colab) · Python 3 · PySpark (se instala automáticamente) · Java (incluido en Colab).

## Estructura del repositorio

```
cloud-provider-analytics/
├── data/          Dataset de muestra (zip). data/raw/ se genera al ejecutar y no se versiona
├── docs/          Documento de diseño por secciones, diagramas y decisiones
├── notebooks/     Exploración y prácticas
└── README.md
```

Carpetas previstas para próximas entregas: `src/` (jobs), `tests/`, `config/`, `infra/` y `evidence/`.

## Documentación

| Documento | Contenido |
|---|---|
| [01 · Problema y objetivos](docs/01_problema_y_objetivos.md) | Problema, usuarios, preguntas de negocio y objetivos medibles |
| [02 · 5V e inventario de fuentes](docs/02_5v_e_inventario_fuentes.md) | Justificación Big Data, inventario, perfil y calidad de datos |
| [03 · Arquitectura v1](docs/03_arquitectura.md) | Diagrama, componentes y patrón Lambda |
| [04 · Diseño del Data Lake](docs/04_data_lake.md) | Zonas, tablas, particiones, nombres, retención y reglas de promoción |
| [05 · Flujos de datos](docs/05_flujos.md) | Flujo batch y flujo streaming paso a paso, con herramientas |
| [06 · Lógica MapReduce](docs/06_mapreduce.md) | Map, shuffle y reduce aplicados al mart principal, con ejemplo a mano |
| [07 · Matriz requisito–componente](docs/07_matriz_requisitos.md) | Trazabilidad entre requisitos, consultas, objetivos, 5V y componentes |
| [Registro de decisiones](DECISIONS.md) | Decisiones técnicas (D-01, D-02, …) con su justificación |
| [00 · Exploración de datos](notebooks/00_exploracion_datos.ipynb) | Evidencia de lectura y perfil con PySpark |

## Datos

Dataset sintético `cloud_provider_challenge_dataset_v1` provisto por la cátedra: 7 fuentes CSV (organizaciones, usuarios, recursos, tickets, marketing, NPS, facturación) y 120 archivos JSONL de eventos de uso (43.200 eventos, jul–ago 2025, esquema v1/v2).

## Convenciones

- **Nombres:** `snake_case` para archivos, columnas y tablas; documentos numerados (`NN_tema.md`).
- **Datos crudos inmutables:** el zip de `data/` actúa como Landing y nunca se modifica.
- **Sin secretos:** credenciales y tokens solo por variables de entorno; nunca en el repositorio.
- **Commits:** en español, descriptivos y por unidad de cambio.
- **Decisiones técnicas:** se registran con identificador (`D-01`, `D-02`, …).

## Tecnologías

PySpark · Spark Structured Streaming · Parquet · Cassandra / DataStax AstraDB · Google Colab · GitHub
