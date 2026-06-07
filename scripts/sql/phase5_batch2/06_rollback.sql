-- ============================================================================
-- Fase 5 · Batch 2 · 06 — ROLLBACK de los cambios aditivos (01, 02) y de 04
-- ============================================================================
-- OBJETIVO:
--   Revertir cada cambio de Batch 2 de forma limpia. Ejecutar SOLO lo que
--   corresponda al cambio que se quiere deshacer. Todo es reversible.
--
-- ⚠️ Igual que en la creación: DROP/CREATE INDEX CONCURRENTLY NO van dentro de
--    una transacción. Correr en autocommit.
-- ============================================================================

-- ── Rollback de 01 (tabla coarse aditiva) ───────────────────────────────────
-- La tabla es nueva y nadie la lee aún → DROP es seguro.
-- (Si solo querés re-poblarla: TRUNCATE dimensionamiento_family_monthly_summary_coarse;)
DROP TABLE IF EXISTS dimensionamiento_family_monthly_summary_coarse;
--   (los índices ix_dim_coarse_* se borran junto con la tabla)

-- ── Rollback de 02 (índices nuevos en comparativa_rows) ──────────────────────
DROP INDEX CONCURRENTLY IF EXISTS ix_comp_rows_plataforma_fecha;
DROP INDEX CONCURRENTLY IF EXISTS ix_comp_rows_rubro_fecha;

-- ── Rollback de 04 (recrear índices forecast dropeados) ──────────────────────
-- Solo si se ejecutó el DROP de 04. Recrear con el DDL EXACTO guardado por la
-- consulta 2 de 04. Plantilla (reemplazar por el indexdef real guardado):
--
-- CREATE INDEX CONCURRENTLY idx_fc_main_codigo_serie ON forecast_main (...);
-- CREATE INDEX CONCURRENTLY idx_fc_main_perfil        ON forecast_main (...);
-- CREATE INDEX CONCURRENTLY idx_fc_val_cliente_id     ON forecast_valorizado (...);
-- CREATE INDEX CONCURRENTLY idx_fc_val_fecha          ON forecast_valorizado (...);
-- CREATE INDEX CONCURRENTLY idx_fc_labs_cdg           ON forecast_product_labs (...);
--
-- ⚠️ No inventar la definición: usar el indexdef guardado antes del DROP.

-- ── Verificación post-rollback (read-only) ───────────────────────────────────
-- Correr 05 de nuevo y confirmar que el set de índices/tablas volvió al estado previo.
