-- ============================================================================
-- Fase 5 · Batch 2 · 07 — Cleanup de índices MUERTOS de summary fino (PROPUESTA)
-- ============================================================================
--  ⚠️  PROPUESTA / NO EJECUTAR. Los DROP están COMENTADOS. Tier 2.
--  Tabla objetivo: dimensionamiento_family_monthly_summary (SOLO esta; NO records).
--
-- OBJETIVO:
--   Retirar índices que no aportan, para bajar el bloat (índices 413 MB vs datos
--   129 MB = 3.2×) y aliviar la presión de cache en una instancia de 256 MB RAM.
--
-- EVIDENCIA DE NO USO (triple):
--   1) idx_scan = 0 en pg_stat_user_indexes (con stats_reset = NULL → acumulado,
--      no reseteado: ningún query usó el índice en toda la ventana).
--   2) EXPLAIN (read-only) de KPIs/series/geo/top-familias → "Parallel Seq Scan",
--      sin Index Scan sobre índices de dimensión del summary.
--   3) No es PK, no es la UNIQUE del upsert, no apareció usado en ningún EXPLAIN.
--
-- NATURALEZA: Tier 2. DROP de índice es REVERSIBLE (recrear desde el DDL exacto
--   capturado en la Sección 0). No toca datos ni la tabla. CONCURRENTLY = no
--   bloquea escrituras (y NO puede ir dentro de una transacción).
--
-- GATE (antes de descomentar cualquier DROP):
--   (a) export fresco en Render (Create export) tomado y anotado;
--   (b) Sección 0 corrida → DDL de rollback capturado + idx_scan=0 confirmado;
--   (c) horario de bajo tráfico; (d) autorización explícita.
--
-- REGLA: nunca se dropea automático. Un índice por vez (o tanda chica), medir,
--   y ante CUALQUIER duda sobre un índice → excluirlo de la tanda.
-- ============================================================================


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 0 — READ-ONLY: candidatos + DDL EXACTO de rollback (correr AHORA)
-- ════════════════════════════════════════════════════════════════════════════
-- Lista TODOS los índices idx_scan=0 de summary (excluye PK y UNIQUE solos), con
-- tamaño y el CREATE INDEX exacto para recrearlos. GUARDAR esta salida: ES el
-- rollback. (pg_get_indexdef = definición real del catálogo, sin adivinar.)
SELECT
    s.indexrelname                                   AS indice,
    pg_size_pretty(pg_relation_size(s.indexrelid))   AS tamano,
    s.idx_scan,
    s.idx_tup_read,
    pg_get_indexdef(s.indexrelid)                    AS create_ddl_rollback
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.relname = 'dimensionamiento_family_monthly_summary'
  AND s.idx_scan = 0
  AND NOT i.indisprimary
  AND NOT i.indisunique
ORDER BY pg_relation_size(s.indexrelid) DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 1 — INTOCABLES (NO se retiran, bajo ninguna circunstancia)
-- ════════════════════════════════════════════════════════════════════════════
--   • dimensionamiento_family_monthly_summary_pkey  (PRIMARY KEY)
--   • uq_dim_family_monthly_summary                 (UNIQUE — usado por el upsert
--                                                    de la ingesta; idx_scan=8)
--   • CUALQUIER índice con idx_scan > 0 (p. ej. ix_..._month idx_scan=6,
--     ix_dim_summary_client_month idx_scan=3, etc.)
--   • Los 7 índices de expresión "_norm" (NO son duplicados; falso positivo
--     cols_key=0; algunos en uso).
--   • Todo dimensionamiento_records (fuera de alcance de esta propuesta).


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 2 — DROP PROPUESTO (COMENTADO). TANDA 1 = el candidato más evidente.
-- ════════════════════════════════════════════════════════════════════════════
-- TANDA 1 (1 índice): ix_dim_sum_familia_qty — 80 MB — idx_scan=0
--   Razón candidato: el más grande sin uso; no está en el modelo ORM (creado por
--   una migración de perf); el dashboard hace seq scan y no lo toca.
--   DESCOMENTAR solo tras cumplir el GATE. Un índice, luego medir (Sección 4).
--
-- DROP INDEX CONCURRENTLY IF EXISTS ix_dim_sum_familia_qty;
--
-- TANDA 2 (a definir con la salida de la Sección 0): el resto de idx_scan=0 de
--   summary (los de ~10–12 MB). Se listan/descomentan recién tras revisar la
--   Sección 0 y confirmar que NINGUNO es PK/UNIQUE/usado. Ante duda → excluir.
--   (Placeholder — completar con nombres exactos de la Sección 0:)
-- DROP INDEX CONCURRENTLY IF EXISTS <indice_tanda2_1>;
-- DROP INDEX CONCURRENTLY IF EXISTS <indice_tanda2_2>;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 3 — ROLLBACK (recrear con el DDL EXACTO de la Sección 0)
-- ════════════════════════════════════════════════════════════════════════════
-- Para cada índice dropeado, recrearlo con el string EXACTO que devolvió la
-- columna create_ddl_rollback de la Sección 0, agregando CONCURRENTLY:
--
--   CREATE INDEX CONCURRENTLY <pegar aquí el create_ddl_rollback EXACTO>;
--
-- Ejemplo de forma (NO ejecutar; usar el real de la Sección 0):
-- CREATE INDEX CONCURRENTLY ix_dim_sum_familia_qty
--     ON public.dimensionamiento_family_monthly_summary (<columnas exactas>);
-- ⚠️ No inventar columnas: el DDL real sale de la Sección 0.


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 4 — VALIDACIÓN ANTES / DESPUÉS (read-only)
-- ════════════════════════════════════════════════════════════════════════════
-- 4.1  Peso de índices de la tabla (correr ANTES y DESPUÉS; debe BAJAR ~80 MB en T1):
SELECT pg_size_pretty(pg_indexes_size('dimensionamiento_family_monthly_summary'::regclass)) AS indices_summary,
       pg_size_pretty(pg_table_size('dimensionamiento_family_monthly_summary'::regclass))   AS datos_summary;

-- 4.2  La tabla y sus objetos críticos siguen existiendo (esperado: 3 filas):
SELECT indexname FROM pg_indexes
WHERE tablename = 'dimensionamiento_family_monthly_summary'
  AND indexname IN ('dimensionamiento_family_monthly_summary_pkey', 'uq_dim_family_monthly_summary');
-- (deben seguir apareciendo PK y UNIQUE)

-- 4.3  Conteo de filas de la tabla SIN cambios (un DROP de índice no toca datos):
SELECT COUNT(*) AS filas_summary FROM dimensionamiento_family_monthly_summary;

-- 4.4  EXPLAIN de los 3 endpoints principales ANTES y DESPUÉS (deben seguir igual:
--      Seq Scan; el plan no debería empeorar porque esos índices no se usaban).
--      Usar las queries del "Bloque 1" de EXPLAIN ya validadas (KPIs/series/geo/top).

-- 4.5  Smoke test en la UI: dashboard de Dimensiones (KPIs, series, geo, top-familias,
--      con y sin filtro is_client) → carga igual, sin 500, tiempos normales.


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 5 — PROTOCOLO DE EJECUCIÓN (cuando se autorice)
-- ════════════════════════════════════════════════════════════════════════════
-- 1. Correr Sección 0 → guardar la salida (candidatos + DDL de rollback).
-- 2. Render → Create export FRESCO → anotar fecha/hora.
-- 3. Correr Sección 4 (snapshot "ANTES": tamaño índices, EXPLAIN de los 3).
-- 4. Horario de BAJO TRÁFICO. Descomentar SOLO la TANDA 1 (1 índice).
--    Ejecutar el DROP INDEX CONCURRENTLY (en autocommit, NO dentro de transacción).
-- 5. Correr Sección 4 (snapshot "DESPUÉS"): índices ~80 MB menos, PK/UNIQUE
--    presentes, filas igual, EXPLAIN igual, smoke test OK.
-- 6. Si todo OK → recién entonces evaluar TANDA 2 (repetir el ciclo).
-- 7. Si aparece CUALQUIER error / EXPLAIN peor / pantalla rota → FRENAR y
--    ejecutar el rollback (Sección 3) de lo dropeado, luego re-validar.
-- ============================================================================
