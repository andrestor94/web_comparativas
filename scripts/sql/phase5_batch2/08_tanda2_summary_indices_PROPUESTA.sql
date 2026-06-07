-- ============================================================================
-- Fase 5 · Batch 2 · 08 — Tanda 2: cleanup índices muertos de summary (PROPUESTA)
-- ============================================================================
--  ⚠️  PROPUESTA / NO EJECUTAR. DROP comentados. Tier 2. Un índice por vez.
--  Tabla objetivo: dimensionamiento_family_monthly_summary (SOLO esta; NO records).
--  Continúa el cleanup tras Tanda 1 (ix_dim_sum_familia_qty, ya ejecutada y OK).
--
-- REGLA DE ORO:
--   • No se dropea automático. Primero se revisa la lista de la Sección 0.
--   • Un índice por sesión (no todos juntos), medir antes/después, frenar si falla.
--   • Ante CUALQUIER duda sobre un índice → excluirlo de la tanda.
--   • Antes de ejecutar cualquier DROP: EXPORT FRESCO en Render (Create export).
--
-- VENTANA DE OBSERVACIÓN:
--   Esta Tanda 2 se ejecuta DESPUÉS de 3–7 días de uso normal post-Tanda 1.
--   Correr la Sección 0 al inicio (inventario) y DE NUEVO al final de la ventana
--   (reconfirmar idx_scan=0 / idx_tup_read=0) antes de dropear nada.
-- ============================================================================


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 0 — READ-ONLY: inventario + categoría + DDL exacto de rollback
-- ════════════════════════════════════════════════════════════════════════════
-- Genera la lista REAL de candidatos (idx_scan=0, no PK, no UNIQUE), con tamaño,
-- contadores, el CREATE INDEX exacto para recrear, y auto-clasifica:
--   • 'CAUTELA-CLIENTE' si el índice toca cliente_visible / cliente_nombre / is_client
--   • 'DIMENSION-PURA'  en caso contrario
-- GUARDAR esta salida: es el inventario Y el rollback. Correr también al cerrar
-- la ventana de observación para reconfirmar.
SELECT
    s.indexrelname                                   AS indice,
    pg_size_pretty(pg_relation_size(s.indexrelid))   AS tamano,
    s.idx_scan,
    s.idx_tup_read,
    CASE
        WHEN pg_get_indexdef(s.indexrelid) ILIKE '%cliente%'
          OR pg_get_indexdef(s.indexrelid) ILIKE '%is_client%'
        THEN 'CAUTELA-CLIENTE'
        ELSE 'DIMENSION-PURA'
    END                                              AS categoria,
    pg_get_indexdef(s.indexrelid)                    AS create_ddl_rollback
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.relname = 'dimensionamiento_family_monthly_summary'
  AND s.idx_scan = 0
  AND s.idx_tup_read = 0
  AND NOT i.indisprimary
  AND NOT i.indisunique
ORDER BY categoria, pg_relation_size(s.indexrelid) DESC;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 1 — CATEGORÍAS (cómo se decide cada índice)
-- ════════════════════════════════════════════════════════════════════════════
-- (1) DIMENSIÓN PURA — candidatos directos (retirar primero, uno por uno):
--     idx_scan=0, idx_tup_read=0, no PK, no UNIQUE, no usados por EXPLAIN, y SIN
--     relación con filtros por cliente. Mismo perfil que la Tanda 1.
--     Observados (confirmar en Sección 0): ix_dim_sum_resultado_plat (~11 MB), y
--     el resto de ~10–11 MB que NO contengan cliente/is_client.
--
-- (2) CAUTELA-CLIENTE — NO tocar hasta tener EXPLAIN con filtro de cliente:
--     índices que tocan cliente_visible / cliente_nombre_homologado / is_client.
--     Observados: ix_dim_summary_visible_month (cliente_visible, ~8.4 MB),
--     ix_dim_sum_isclient_family_month (~12 MB), y similares con is_client.
--     Aunque hoy den idx_scan=0, el panel de perfiles FILTRA por cliente, así que
--     antes de dropear hay que confirmar con el EXPLAIN de la Sección 5.4.
--
-- (3) EXCLUIDOS / INTOCABLES — ver Sección 2.


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 2 — INTOCABLES (NO se retiran nunca)
-- ════════════════════════════════════════════════════════════════════════════
--   • dimensionamiento_family_monthly_summary_pkey   (PRIMARY KEY)
--   • uq_dim_family_monthly_summary                  (UNIQUE — upsert de ingesta)
--   • cualquier índice con idx_scan > 0 (ix_..._month idx_scan=6,
--     ix_dim_summary_client_month idx_scan=3, etc.)
--   • los 7 índices de expresión "_norm" (falso positivo de duplicados)
--   • cualquier índice dudoso (ante duda, excluir)
--   • TODO dimensionamiento_records (fuera de alcance)


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 3 — DROP PROPUESTO (COMENTADO). UN ÍNDICE POR VEZ.
-- ════════════════════════════════════════════════════════════════════════════
-- Completar con los nombres EXACTOS de la Sección 0, EMPEZANDO por DIMENSION-PURA.
-- Descomentar UNO, ejecutar, validar (Sección 5), y recién el siguiente.
-- (auto-commit ON en DBeaver: CONCURRENTLY no corre dentro de transacción)
--
--   --- DIMENSIÓN PURA (primero) ---
-- DROP INDEX CONCURRENTLY IF EXISTS public.<dim_pura_1>;
-- DROP INDEX CONCURRENTLY IF EXISTS public.<dim_pura_2>;
-- ...
--
--   --- CAUTELA-CLIENTE (solo tras EXPLAIN con filtro de cliente, Sección 5.4) ---
-- DROP INDEX CONCURRENTLY IF EXISTS public.<cautela_cliente_1>;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 4 — ROLLBACK (recrear con el DDL EXACTO de la Sección 0)
-- ════════════════════════════════════════════════════════════════════════════
-- Por cada índice dropeado, tomar su create_ddl_rollback de la Sección 0 y
-- reemplazar el inicio "CREATE INDEX" por "CREATE INDEX CONCURRENTLY IF NOT EXISTS".
-- El resto (tabla, USING btree, columnas) queda IDÉNTICO.
--
--   CREATE INDEX CONCURRENTLY IF NOT EXISTS <indice>
--       ON public.dimensionamiento_family_monthly_summary USING btree ( <columnas EXACTAS> );
-- ⚠️ No inventar columnas: usar el string real de la Sección 0.


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 5 — VALIDACIÓN ANTES / DESPUÉS (read-only; por cada índice)
-- ════════════════════════════════════════════════════════════════════════════
-- 5.1  Peso de índices ANTES y DESPUÉS (debe bajar ~el tamaño del índice):
SELECT pg_size_pretty(pg_indexes_size('dimensionamiento_family_monthly_summary'::regclass)) AS indices,
       pg_size_pretty(pg_table_size('dimensionamiento_family_monthly_summary'::regclass))   AS datos;

-- 5.2  PK y UNIQUE siguen existiendo (esperado: 2 filas):
SELECT indexname FROM pg_indexes
WHERE tablename = 'dimensionamiento_family_monthly_summary'
  AND indexname IN ('dimensionamiento_family_monthly_summary_pkey', 'uq_dim_family_monthly_summary')
ORDER BY indexname;

-- 5.3  El índice dropeado YA NO existe (esperado: 0 filas):
-- SELECT indexname FROM pg_indexes
-- WHERE tablename='dimensionamiento_family_monthly_summary' AND indexname = '<indice_dropeado>';

-- 5.4  EXPLAIN de control:
--   (a) Los 3 endpoints del Bloque 1 (Top familias / Geo / Serie, con is_client=true):
--       deben seguir con "Parallel Seq Scan" y el MISMO plan.
--   (b) SOLO para candidatos CAUTELA-CLIENTE, ANTES de dropearlos, correr un EXPLAIN
--       con filtro por cliente y confirmar que tampoco usan el índice:
-- EXPLAIN
-- SELECT familia, SUM(total_valorizacion) AS val
-- FROM dimensionamiento_family_monthly_summary
-- WHERE import_run_id = (SELECT id FROM dimensionamiento_import_runs
--                        WHERE status='success' ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1)
--   AND is_client = true
--   AND cliente_visible = '<pegar un cliente_visible real existente>'
-- GROUP BY familia;
--   → si aparece "Index Scan using ix_dim_summary_visible_month" (o similar) ⇒ SE USA
--     en el path de cliente ⇒ NO dropear ese índice (pasa a intocable).
--   → si sigue Seq Scan ⇒ confirma que tampoco se usa filtrando por cliente.

-- 5.5  Filas de la tabla SIN cambios:
SELECT COUNT(*) AS filas_summary FROM dimensionamiento_family_monthly_summary;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 6 — CHECKLIST DE SMOKE TEST (UI, tras cada DROP)
-- ════════════════════════════════════════════════════════════════════════════
-- Mercado Privado → Reporte de Perfiles / Dimensiones, CON y SIN filtro is_client:
--   [ ] KPIs cargan (total valorizado, cantidad, familias)
--   [ ] Serie temporal dibuja
--   [ ] Geo / provincias dibuja
--   [ ] Top familias lista
--   [ ] Resultados / clientes por resultado / consumo por familia cargan
--   [ ] Vista Artículo y Cliente de perfiles abren bien (¡clave si se tocó un
--       índice CAUTELA-CLIENTE!)
--   [ ] Sin 500 ni pantallas rotas; tiempos normales o mejores


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 7 — PROTOCOLO DE EJECUCIÓN (cuando se autorice, por índice)
-- ════════════════════════════════════════════════════════════════════════════
-- 1. (Fin de la ventana) Correr Sección 0 → confirmar idx_scan=0/idx_tup_read=0
--    de los candidatos. Guardar inventario + DDL.
-- 2. Render → Create export FRESCO → anotar fecha/hora.
-- 3. Sección 5 "ANTES" (tamaño índices, EXPLAIN de los 3; + 5.4b si es cliente).
-- 4. Bajo tráfico, auto-commit ON. Descomentar UN índice (DIMENSION-PURA primero).
--    DROP INDEX CONCURRENTLY.
-- 5. Sección 5 "DESPUÉS" + smoke test (Sección 6).
-- 6. Si OK → registrar en el changelog (docs/fase5_backup_rollback.md §5) y recién
--    entonces el siguiente índice. Si algo falla → rollback (Sección 4) y re-validar.


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 8 — QUERY DE RECONFIRMACIÓN POST-OBSERVACIÓN (read-only)
-- ════════════════════════════════════════════════════════════════════════════
-- Correr al cerrar la ventana de 3–7 días. Compara los candidatos: cuáles SIGUEN
-- muertos (idx_scan=0 Y idx_tup_read=0) y cuáles "despertaron" (≠0 ⇒ EXCLUIR).
SELECT
    s.indexrelname AS indice,
    pg_size_pretty(pg_relation_size(s.indexrelid)) AS tamano,
    s.idx_scan,
    s.idx_tup_read,
    CASE WHEN s.idx_scan = 0 AND s.idx_tup_read = 0
         THEN 'SIGUE MUERTO (candidato)'
         ELSE 'DESPERTÓ → EXCLUIR' END AS estado
FROM pg_stat_user_indexes s
JOIN pg_index i ON i.indexrelid = s.indexrelid
WHERE s.relname = 'dimensionamiento_family_monthly_summary'
  AND NOT i.indisprimary
  AND NOT i.indisunique
ORDER BY s.idx_scan, pg_relation_size(s.indexrelid) DESC;
-- (incluye TODOS los no-PK/no-UNIQUE para ver también si alguno que estaba en 0
--  empezó a usarse durante la ventana.)
-- ============================================================================
