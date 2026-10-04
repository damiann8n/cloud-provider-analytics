# 8. Supuestos, riesgos, decisiones abiertas y plan de trabajo

> Proyecto: Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
> Versión: 1.0 · 04/10/2026

## 8.1 Supuestos

Son cosas que doy por ciertas para poder avanzar, aunque los datos o la consigna no lo digan de forma explícita. Si alguno resulta falso, tengo que revisar la decisión que depende de él.

| # | Supuesto | Por qué lo asumo | Qué depende de esto |
|---|---|---|---|
| S-01 | El tipo de cambio de la facturación tiene ruido sintético (±10 % por factura) | Facturas de la misma moneda y el mismo mes tienen FX distintos, incluso en USD | D-01 |
| S-02 | El NPS válido va de −100 a 100 y el CSAT de 1 a 5 | Son las escalas estándar de esas métricas | Reglas de calidad |
| S-03 | Los subtotales negativos de facturación son notas de crédito o ajustes, no errores | Son pocos (13) y la consigna menciona "créditos/ajustes" | Revenue (Q4) |
| S-04 | `credits` nulo significa que no hubo créditos (vale 0) | Es la lectura más razonable para un campo de descuento | Revenue (Q4) |
| S-05 | Un ticket sin `resolved_at` está abierto | Es lo que indica la consigna ("estado abierto/cerrado") | SLA (Q3) |
| S-06 | Los costos muy altos (spikes) son datos válidos pero raros, no errores | La consigna los llama "atípicos", no inválidos | Se marcan con flag, no van a quarantine |
| S-07 | El dataset es una muestra chica de un escenario real mucho más grande | Lo dice la consigna (Big Data) y lo uso para justificar las 5V | Doc 02 y arquitectura |
| S-08 | Alcanza con el plan gratuito de Colab y de AstraDB para este volumen | El dataset pesa unos 13 MB | Entorno de ejecución |

## 8.2 Riesgos y mitigaciones

Probabilidad e impacto: **A** = alto, **M** = medio, **B** = bajo.

| # | Riesgo | Prob. | Impacto | Mitigación |
|---|---|:-:|:-:|---|
| R-01 | El watermark descarta eventos válidos porque los archivos traen eventos desordenados | A | A | Watermark amplio y recálculo por día en `foreachBatch` (D-04). Lo pruebo apenas arranco el Parcial 2 |
| R-02 | La facturación empieza en junio y los eventos en julio: no puedo comparar uso contra facturación de junio | A | M | Lo documento y comparo solo julio y agosto |
| R-03 | El supuesto del FX (S-01) no es el que esperaba la cátedra | M | M | Consultarlo en la clase del 07/10. Guardo el FX original, así puedo cambiar la regla sin perder datos |
| R-04 | Problemas para conectar Spark con AstraDB desde Colab | M | A | Probar la conexión al principio del Parcial 2, no al final. Plan B: cargar con el driver de Python desde `foreachBatch`, que la consigna permite |
| R-05 | Las sesiones de Colab se cortan y se pierde el estado | M | M | El notebook se ejecuta de arriba hacia abajo sin pasos manuales; guardo logs y capturas como evidencia |
| R-06 | Tiempo justo: 6 h por semana sin margen (ver 8.4) | M | A | Priorizar lo obligatorio; si una etapa se atrasa, sumo horas esa semana y dejo lo opcional para el backlog |
| R-07 | Curva de aprendizaje con herramientas nuevas para mí (git, Structured Streaming, Cassandra) | A | M | Arrancar cada tema con una prueba chica antes de integrarlo al pipeline |
| R-08 | Subir credenciales de AstraDB al repo por error | B | A | Usar variables de entorno o los "Secrets" de Colab; nunca escribir el token en el notebook |

## 8.3 Decisiones abiertas y preguntas para el profesor

| # | Tema | Estado | Cuándo se cierra |
|---|---|---|---|
| A-01 | ¿El FX con ruido es intencional? ¿Está bien usar un promedio por moneda y mes? (D-01) | Pregunta para la clase del 07/10 | 07/10 |
| A-02 | Valor exacto del watermark (D-04) | Lo defino probando con los datos | Parcial 2 |
| A-03 | Método para detectar anomalías de costo: MAD, z-score o percentiles | Me inclino por MAD o percentiles porque los spikes distorsionan el promedio | Parcial 2 |
| A-04 | Diseño de las tablas de Cassandra (claves de partición y de orden) | Pendiente | Parcial 2 |
| A-05 | Componente de analítica o ML para el final | Pendiente | Después del Parcial 2 |

## 8.4 Estimación de esfuerzo

Tengo disponibles unas **6 horas por semana**. Del 04/10 al 09/12 son unas 9,5 semanas, es decir, **~57 horas**.

| Etapa | Período | Horas disponibles | Tareas principales | Horas estimadas |
|---|---|:-:|---|:-:|
| Cierre del Parcial 1 | hasta 07/10 | ~3 | Este documento, documento de diseño final, ajustes de redacción | ~5 |
| Parcial 2 | 08/10 → 18/11 | ~36 | Plan de correcciones (1), batch a Bronze (4), streaming a Bronze (6), Silver y calidad (8), mart de Gold (3), AstraDB y consultas (6), idempotencia, README y evidencias (5) | ~33 |
| Final | 19/11 → 09/12 | ~18 | Marts restantes y 5 consultas (6), analítica o ML (4), gobierno y documentación (3), presentación y video (4), ensayo de la defensa (2) | ~19 |
| **Total** | | **~57** | | **~57** |

**Conclusión:** el plan entra justo, sin margen para imprevistos (riesgo R-06). Si alguna etapa se atrasa, sumo horas esa semana y muevo lo opcional al backlog.

## 8.5 Roles y recursos

Como el trabajo es individual, cumplo todos los roles:

| Rol | Qué hago en ese rol |
|---|---|
| Analista de negocio | Entender el caso, definir preguntas y objetivos |
| Arquitecto de datos | Diseñar la arquitectura, el Data Lake y las decisiones técnicas |
| Ingeniero de datos | Programar la ingesta, las transformaciones y la carga |
| Analista de datos / ML | Calcular features, detectar anomalías y armar el componente analítico |
| Responsable de calidad | Definir reglas, revisar la quarantine y validar conteos |
| Documentación | README, diagramas, decisiones y evidencias |

**Recursos:**

| Recurso | Para qué | Costo |
|---|---|---|
| Google Colab | Ejecutar PySpark | Gratis |
| PySpark y Structured Streaming | Procesamiento batch y streaming | Gratis (open source) |
| GitHub | Repositorio y versionado | Gratis |
| DataStax AstraDB | Base Cassandra en la nube para el serving | Plan gratuito |
| VS Code y git | Edición y control de versiones | Gratis |

## 8.6 Próximos pasos

1. Entregar el Parcial 1 el **07/10 a las 19 h**.
2. En la clase del 07/10: hacer las preguntas de 8.3 y anotar el feedback.
3. Armar y versionar el **plan de correcciones**, con prioridad, fecha y evidencia esperada, como pide la consigna.
4. Empezar el Parcial 2 por lo más riesgoso: la prueba del watermark (R-01) y la conexión con AstraDB (R-04).
