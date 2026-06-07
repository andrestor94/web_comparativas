-- ============================================================================
-- Fase 5 · Batch 2 · 04 — Índices forecast DUPLICADOS (PROPUESTA, NO EJECUTAR)
-- ============================================================================
-- OBJETIVO:
--   La familia forecast vieja tiene índices duplicados por convención de nombres
--   (idx_fc_* y ix_fc_* sobre las MISMAS columnas). Mantener los dos cuesta
--   espacio y enlentece las escrituras de la ingesta. Propuesta: dejar UNO por
--   par. Reversible (recrear desde el DDL guardado).
--
-- ⚠️ ESTE ARCHIVO ES PROPUESTA. Los DROP están COMENTADOS. NO se ejecuta hasta:
--    (1) backup verificado, (2) confirmar con la consulta 1 que son duplicados
--    EXACTOS sobre las mismas columnas, (3) guardar el DDL (consulta 2),
--    (4) autorización explícita.
--
-- RIESGO: Medio. Un DROP de índice es reversible (recrear), pero si se dropea el
--   equivocado puede degradar queries. Por eso: confirmar duplicado exacto + DDL
--   guardado + DROP CONCURRENTLY.
--
-- BACKUP REQUERIDO: Sí (snapshot) + DDL guardado (consulta 2 = el verdadero seguro).
--
-- VALIDACIÓN: tras cada DROP, EXPLAIN de las queries de forecast que usaban esa
--   columna debe seguir usando el índice gemelo que queda. Ver 05.
--
-- ROLLBACK: recrear el índice dropeado con su DDL (ver 06).
-- ============================================================================

-- 1) CONFIRMAR duplicados exactos en las tablas forecast viejas (READ-ONLY).
WITH idx AS (
    SELECT
        t.relname AS tabla,
        ix.relname AS indice,
        i.indkey::text AS cols_key,
        i.indisunique AS es_unico,
        pg_relation_size(ix.oid) AS bytes
    FROM pg_index i
    JOIN pg_class ix ON ix.oid = i.indexrelid
    JOIN pg_class t  ON t.oid  = i.indrelid
    JOIN pg_namespace n ON n.oid = t.relnamespace
    WHERE n.nspname = 'public'
      AND t.relname IN (
          'forecast_main', 'forecast_valorizado', 'forecast_imp_hist',
          'forecast_fact_2026', 'forecast_product_labs'
      )
)
SELECT tabla, cols_key,
       array_agg(indice ORDER BY indice) AS indices_duplicados,
       array_agg(pg_size_pretty(bytes) ORDER BY indice) AS tamanos,
       bool_or(es_unico) AS alguno_unico
FROM idx
GROUP BY tabla, cols_key
HAVING COUNT(*) > 1
ORDER BY tabla, cols_key;

-- 2) GUARDAR el DDL de los índices candidatos ANTES de dropear (seguro de rollback).
SELECT indexname, indexdef
FROM pg_indexes
WHERE tablename IN (
    'forecast_main', 'forecast_valorizado', 'forecast_imp_hist',
    'forecast_fact_2026', 'forecast_product_labs'
)
ORDER BY tablename, indexname;

-- ----------------------------------------------------------------------------
-- 3) DROP PROPUESTO (COMENTADO — NO EJECUTAR sin backup + OK + DDL guardado).
--    Regla: conservar el índice "ix_fc_*" (convención SQLAlchemy) y dropear el
--    duplicado "idx_fc_*", SOLO si la consulta 1 confirma que son sobre las
--    mismas columnas y ninguno es UNIQUE/usado por una constraint.
--    Descomentar de a uno, validar con EXPLAIN, y recién entonces el siguiente.
--
--    Duplicados detectados en el diagnóstico de Fase 4 (CONFIRMAR con consulta 1):
--      forecast_main:        idx_fc_main_codigo_serie  == ix_fc_main_codigo_serie
--                            idx_fc_main_perfil        == ix_fc_main_perfil
--      forecast_valorizado:  idx_fc_val_cliente_id     == ix_fc_val_cliente_id
--                            idx_fc_val_fecha          == ix_fc_val_fecha
--      forecast_product_labs:idx_fc_labs_cdg           == ix_fc_labs_codigo
--
-- DROP INDEX CONCURRENTLY IF EXISTS idx_fc_main_codigo_serie;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_fc_main_perfil;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_fc_val_cliente_id;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_fc_val_fecha;
-- DROP INDEX CONCURRENTLY IF EXISTS idx_fc_labs_cdg;
--
-- ⚠️ DROP INDEX CONCURRENTLY tampoco puede ir dentro de una transacción.
-- ============================================================================
