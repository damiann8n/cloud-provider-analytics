# Registro de decisiones

Acá anoto las decisiones técnicas que voy tomando en el proyecto: qué decidí, por qué y qué alternativas descarté.
Cada una tiene un número (D-01, D-02, …) para poder citarla desde los otros documentos.

---

## D-01 · Tipo de cambio en la facturación

**Fecha:** 04/10/2026 · **Estado:** decidida (a validar con el profesor)

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
