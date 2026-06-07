-- ============================================================================
-- Fase 5 · Batch 2 · 05 — Validación ANTES / DESPUÉS (READ-ONLY)
-- ============================================================================
-- OBJETIVO:
--   Capturar un "snapshot" de conteos, sumas e índices para comparar el estado
--   ANTES y DESPUÉS de cada cambio de Batch 2. Si algo no coincide donde debía,
--   se frena y se revierte (06). 100% SELECT.
--
-- USO: correr ENTERO antes del cambio (guardar salida), aplicar el cambio,
--   correr ENTERO de nuevo y comparar. Las cifras de tablas existentes NO deben
--   cambiar por crear índices o la tabla coarse (son aditivos).
-- ============================================================================

-- A) Conteos de las tablas que importan (no deben cambiar por cambios aditivos).
SELECT 'dimensionamiento_records'  AS tabla, COUNT(*) AS filas FROM dimensionamiento_records
UNION ALL
SELECT 'dim_summary_fino',  COUNT(*) FROM dimensionamiento_family_monthly_summary
UNION ALL
SELECT 'comparativa_rows',  COUNT(*) FROM comparativa_rows;

-- B) Sumas de control de records (deben ser idénticas antes/después).
SELECT
    COALESCE(SUM(cantidad_demandada), 0)    AS sum_cantidad,
    COALESCE(SUM(valorizacion_estimada), 0) AS sum_valorizacion,
    COUNT(*)                                AS filas
FROM dimensionamiento_records;

-- C) PARIDAD coarse vs fino (solo tras poblar 01): el coarse debe dar los MISMOS
--    totales que el summary fino agregado sin cliente, para el último run.
--    Si difieren ⇒ el coarse está mal poblado ⇒ revisar 01 / no usarlo.
WITH run AS (
    SELECT id FROM dimensionamiento_import_runs
    WHERE status = 'success'
    ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1
)
SELECT
    (SELECT COALESCE(SUM(total_cantidad),0)
       FROM dimensionamiento_family_monthly_summary_coarse c, run
      WHERE c.import_run_id = run.id)                      AS coarse_cantidad,
    (SELECT COALESCE(SUM(total_cantidad),0)
       FROM dimensionamiento_family_monthly_summary f, run
      WHERE f.import_run_id = run.id)                      AS fino_cantidad,
    (SELECT COALESCE(SUM(total_valorizacion),0)
       FROM dimensionamiento_family_monthly_summary_coarse c, run
      WHERE c.import_run_id = run.id)                      AS coarse_valorizacion,
    (SELECT COALESCE(SUM(total_valorizacion),0)
       FROM dimensionamiento_family_monthly_summary f, run
      WHERE f.import_run_id = run.id)                      AS fino_valorizacion;
-- NOTA: coarse_cantidad debe == fino_cantidad y coarse_valorizacion == fino_valorizacion
--       (la suma de totales no depende de agrupar o no por cliente).

-- D) Compresión lograda por el coarse (cuánto más chico que el fino).
WITH run AS (
    SELECT id FROM dimensionamiento_import_runs
    WHERE status = 'success' ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1
)
SELECT
    (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary_coarse c, run WHERE c.import_run_id = run.id) AS filas_coarse,
    (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary f, run WHERE f.import_run_id = run.id)        AS filas_fino;

-- E) Lista de índices (comparar set antes/después; los nuevos deben aparecer y
--    NINGUNO existente debe desaparecer salvo los dropeados a propósito en 04).
SELECT tablename, indexname
FROM pg_indexes
WHERE tablename IN (
    'dimensionamiento_records',
    'dimensionamiento_family_monthly_summary',
    'dimensionamiento_family_monthly_summary_coarse',
    'comparativa_rows',
    'forecast_main', 'forecast_valorizado', 'forecast_imp_hist',
    'forecast_fact_2026', 'forecast_product_labs'
)
ORDER BY tablename, indexname;

-- F) (Opcional) EXPLAIN de un endpoint sensible — correr a mano con filtros reales:
-- EXPLAIN (ANALYZE, BUFFERS)
-- SELECT plataforma, fecha_apertura FROM comparativa_rows
-- WHERE plataforma = '<algo>' AND fecha_apertura >= DATE '2025-01-01';
