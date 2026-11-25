-- Cloud Provider Analytics · Minería de Datos II · ISTEA 2C 2026
-- Serving en AstraDB (Cassandra). Keyspace: default_keyspace (ver D-06)
--
-- Modelo query-first: una tabla por consulta, sin joins, datos desnormalizados.
-- Las tablas se crean desde el notebook con astrapy (celda 13). Este es el CQL equivalente.

-- =====================================================================
-- Tabla 1: org_daily_usage_by_service
-- Consulta 1 de la consigna: costos y requests diarios por organizacion
-- y servicio en un rango de fechas.
-- Particion: org_id (cada consulta es de una sola org, 80 particiones acotadas).
-- Clustering: usage_date y service (permite pedir un rango de fechas ordenado).
-- =====================================================================
CREATE TABLE IF NOT EXISTS default_keyspace.org_daily_usage_by_service (
    org_id            text,
    usage_date        date,
    service           text,
    org_name          text,
    daily_cost_usd    double,
    requests          double,
    cpu_hours         double,
    storage_gb_hours  double,
    genai_tokens      bigint,
    carbon_kg         double,
    eventos           int,
    PRIMARY KEY ((org_id), usage_date, service)
) WITH CLUSTERING ORDER BY (usage_date ASC, service ASC);

-- Consulta 1: costos y requests de una organizacion entre dos fechas
SELECT usage_date, service, daily_cost_usd, requests
FROM default_keyspace.org_daily_usage_by_service
WHERE org_id = 'org_0lvsnujz'
  AND usage_date >= '2025-08-01' AND usage_date <= '2025-08-07';

-- =====================================================================
-- Tabla 2: top_services_14d
-- Consulta 2 de la consigna: top-N servicios por costo acumulado en los
-- ultimos 14 dias para una organizacion.
-- Particion: org_id. Clustering: cost_14d DESC, asi Cassandra guarda los
-- servicios ya ordenados de mayor a menor costo y el top-N es solo un LIMIT.
-- Se precalcula en Spark (Gold) para la ventana del 18/08 al 31/08/2025.
-- =====================================================================
CREATE TABLE IF NOT EXISTS default_keyspace.top_services_14d (
    org_id        text,
    cost_14d      double,
    service       text,
    requests_14d  double,
    window_end    date,
    PRIMARY KEY ((org_id), cost_14d, service)
) WITH CLUSTERING ORDER BY (cost_14d DESC, service ASC);

-- Consulta 2: top 3 servicios por costo en los ultimos 14 dias
SELECT service, cost_14d, requests_14d
FROM default_keyspace.top_services_14d
WHERE org_id = 'org_0lvsnujz'
LIMIT 3;
