-- =============================================================================
-- Setup inicial de Unity Catalog — Olist Lakehouse
-- =============================================================================
-- Ejecutar UNA SOLA VEZ por workspace, desde la SQL Console de Databricks.
-- Correr en workspace DEV y luego repetir en workspace PROD con el mismo script.
--
-- Resultado esperado:
--   olist (catalog)
--   ├── bronze  (schema)
--   ├── silver  (schema)
--   └── gold    (schema)
-- =============================================================================

CREATE CATALOG IF NOT EXISTS olist
  COMMENT 'Olist Lakehouse — portafolio Databricks end-to-end';

CREATE SCHEMA IF NOT EXISTS olist.bronze
  COMMENT 'Capa Bronze: ingestión raw desde Autoloader, todo STRING, append-only';

CREATE SCHEMA IF NOT EXISTS olist.silver
  COMMENT 'Capa Silver: limpieza, tipos, SCD1/SCD2 y CDF habilitado donde aplica';

CREATE SCHEMA IF NOT EXISTS olist.gold
  COMMENT 'Capa Gold: Materialized Views para consumo analítico y observabilidad';
