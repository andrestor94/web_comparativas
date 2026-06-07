-- ============================================================================
-- Fase 5 · Batch 2 · 03 — Auditoría de índices (READ-ONLY)
-- ============================================================================
-- OBJETIVO:
--   Reunir EVIDENCIA para decidir qué índices de dimensionamiento_records (≈27)
--   y dimensionamiento_family_monthly_summary (≈26) sobran. NO elimina nada.
--   Esto es 100% SELECT: se puede correr cuando quieras, incluso contra prod
--   (sigue siendo solo lectura). La decisión de retiro va a una fase posterior,
--   con backup y DDL guardado.
--
-- RIESGO: Nulo (solo lectura).
-- ============================================================================

-- 1) USO real de cada índice (idx_scan = 0 ⇒ candidato fuerte a retiro).
--    Incluye tamaño para priorizar: índices grandes + nunca usados = mejor ROI.
SELECT
    s.relname            AS tabla,
    s.indexrelname       AS indice,
    s.idx_scan           AS veces_usado,
    s.idx_tup_read       AS tuplas_leidas,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS tamano,
    i.indisunique        AS es_unico,
    i.indisprimary       AS es_pk
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.relname IN (
    'dimensionamiento_records',
    'dimensionamiento_family_monthly_summary'
)
ORDER BY s.relname, s.idx_scan ASC, pg_relation_size(s.indexrelid) DESC;

-- 2) DEFINICIÓN exacta de cada índice (guardar la salida: es el "backup" para
--    poder recrearlos si se retira alguno).
SELECT tablename, indexname, indexdef
FROM pg_indexes
WHERE tablename IN (
    'dimensionamiento_records',
    'dimensionamiento_family_monthly_summary',
    'comparativa_rows'
)
ORDER BY tablename, indexname;

-- 3) DUPLICADOS / REDUNDANTES: índices distintos sobre el MISMO conjunto de
--    columnas (mismo orden). Candidatos a quedarse con uno solo.
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
          'dimensionamiento_records',
          'dimensionamiento_family_monthly_summary'
      )
)
SELECT tabla, cols_key,
       COUNT(*)                       AS cantidad_indices,
       array_agg(indice ORDER BY indice) AS indices,
       array_agg(es_unico ORDER BY indice) AS unicos
FROM idx
GROUP BY tabla, cols_key
HAVING COUNT(*) > 1
ORDER BY tabla, cols_key;

-- 4) Tamaño total tabla vs índices (cuánto del peso es índice).
SELECT
    relname AS tabla,
    pg_size_pretty(pg_table_size(oid))   AS tamano_datos,
    pg_size_pretty(pg_indexes_size(oid)) AS tamano_indices,
    pg_size_pretty(pg_total_relation_size(oid)) AS tamano_total
FROM pg_class
WHERE relname IN (
    'dimensionamiento_records',
    'dimensionamiento_family_monthly_summary'
);

-- INTERPRETACIÓN:
--   - idx_scan = 0 tras semanas de uso real ⇒ proponer retiro (fase posterior).
--   - filas de la consulta 3 con cantidad_indices > 1 ⇒ duplicados exactos.
--   - NADA se elimina en este script. Se documenta y se decide con backup.
