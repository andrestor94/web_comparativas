-- ============================================================================
-- Fase 5 · Batch 2 · 01 — Segundo summary "grueso" de Dimensionamiento (ADITIVO)
-- ============================================================================
-- OBJETIVO:
--   Crear una tabla agregada NUEVA, SIN la dimensión "cliente", para alimentar
--   los paneles del dashboard que NO necesitan cliente (KPIs, series, geo,
--   top-familias, resultados). El summary fino casi no comprime (≈260k filas vs
--   319k de records) porque su clave incluye cliente_nombre_homologado +
--   cliente_visible (alta cardinalidad: decenas de miles). Sin esas columnas, la
--   tabla coarse debería ser mucho más chica → seq scan mucho más barato.
--
--   EVIDENCIA (EXPLAIN read-only, jun-2026): los endpoints KPIs/series/geo/
--   top-familias hacen "Parallel Seq Scan" sobre el summary fino y NO usan sus
--   índices de dimensión. Por eso el problema no es agregar índices, sino reducir
--   la cantidad de filas a escanear → exactamente lo que hace esta tabla coarse.
--
-- ALCANCE / NATURALEZA:
--   ADITIVO y REVERSIBLE. NO reemplaza a dimensionamiento_family_monthly_summary.
--   El código sigue leyendo las tablas actuales (la redirección de lecturas es el
--   Paso 3b, posterior y por separado). Por ahora la tabla solo existe y se puebla
--   para medir/validar.
--
-- QUÉ COLUMNAS TIENE:
--   month, plataforma, provincia, familia, unidad_negocio, subunidad_negocio,
--   resultado_participacion, is_identified, is_client  → clave de agregación
--   total_cantidad, total_valorizacion, total_registros, clientes_unicos → métricas
--   import_run_id → de qué corrida proviene
--   (id surrogate + UNIQUE de la clave para idempotencia del upsert)
--
-- DESDE QUÉ TABLA SE POBLA:
--   Desde dimensionamiento_records (fuente cruda, única fuente de verdad), con un
--   GROUP BY por la clave coarse. clientes_unicos = COUNT(DISTINCT cliente_visible)
--   por grupo (NO se puede sumar desde el fino, por eso se calcula desde records).
--   Lectura de records, escritura SOLO en la tabla nueva. No toca nada vivo.
--
-- CUÁNTAS FILAS REDUCE:
--   No se puede saber el número exacto sin medir, pero al salir cliente_* (alta
--   cardinalidad) de la clave, se espera una reducción GRANDE respecto de las
--   ~260k del fino (probablemente a un orden de magnitud menor). La Sección 0
--   lo MIDE exacto en read-only ANTES de crear nada.
--
-- VALIDACIÓN (cómo se confirma que los totales coinciden):
--   SUM(total_cantidad) y SUM(total_valorizacion) del coarse DEBEN igualar los del
--   summary fino para el MISMO import_run_id (la suma de totales no depende de
--   agrupar o no por cliente). Detalle en 05_validacion_antes_despues.sql (consulta C).
--   Si no coinciden → coarse mal poblado → DROP y revisar (no se redirige nada).
--
-- BACKUP / EXPORT FRESCO:
--   3a es ADITIVO: solo escribe en una tabla NUEVA; el rollback es DROP TABLE y NO
--   depende de ningún backup. Aun así, por protocolo, se RECOMIENDA crear un export
--   fresco en Render (Create export) justo antes de ejecutar, como punto de control.
--   El export es IMPRESCINDIBLE recién para los drops de índices (Tier 2), no acá.
--
-- COSTO OPERATIVO:
--   El INSERT lee TODO records (~355 MB) con GROUP BY + COUNT(DISTINCT) en una
--   instancia de 256 MB de RAM. Es un INSERT único → correr en BAJO TRÁFICO.
--
-- ROLLBACK: ver 06_rollback.sql (DROP TABLE de la tabla nueva).
-- NOTA SQLite: este script es PostgreSQL (prod). En local (SQLite) no aplica.
-- ============================================================================


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 0 — PRE-CHEQUEOS READ-ONLY (correr AHORA; NO crean ni tocan nada)
-- ════════════════════════════════════════════════════════════════════════════

-- 0.1  Confirmar el import_run que se va a usar.
SELECT id, status, finished_at
FROM dimensionamiento_import_runs
WHERE status = 'success'
ORDER BY finished_at DESC NULLS LAST, id DESC
LIMIT 1;

-- 0.2  ESTIMAR cuántas filas tendría el coarse, SIN crear la tabla.
--      (mismo GROUP BY que el INSERT, pero solo cuenta los grupos)
SELECT COUNT(*) AS filas_coarse_estimadas
FROM (
    SELECT 1
    FROM dimensionamiento_records r
    WHERE r.import_run_id = (
        SELECT ir.id FROM dimensionamiento_import_runs ir
        WHERE ir.status='success' ORDER BY ir.finished_at DESC NULLS LAST, ir.id DESC LIMIT 1)
    GROUP BY
        date_trunc('month', r.fecha)::date, r.plataforma, r.provincia, r.familia,
        r.unidad_negocio, r.subunidad_negocio, r.resultado_participacion,
        r.is_identified, r.is_client
) g;

-- 0.3  Comparar contra el fino del MISMO run (para ver la compresión esperada).
SELECT COUNT(*) AS filas_fino
FROM dimensionamiento_family_monthly_summary
WHERE import_run_id = (
    SELECT ir.id FROM dimensionamiento_import_runs ir
    WHERE ir.status='success' ORDER BY ir.finished_at DESC NULLS LAST, ir.id DESC LIMIT 1);
-- → filas_coarse_estimadas debería ser MUCHO menor que filas_fino. Si no, avisar
--   antes de ejecutar el resto (la tabla coarse no aportaría).


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 1 — CREAR LA TABLA (ADITIVO; ejecutar solo tras OK + export fresco)
-- ════════════════════════════════════════════════════════════════════════════
CREATE TABLE IF NOT EXISTS dimensionamiento_family_monthly_summary_coarse (
    id                       BIGSERIAL PRIMARY KEY,
    month                    DATE        NOT NULL,
    plataforma               VARCHAR(40) NOT NULL,
    provincia                VARCHAR(120),
    familia                  TEXT,
    unidad_negocio           TEXT,
    subunidad_negocio        TEXT,
    resultado_participacion  VARCHAR(120),
    is_identified            BOOLEAN     NOT NULL DEFAULT FALSE,
    is_client                BOOLEAN     NOT NULL DEFAULT FALSE,
    total_cantidad           DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_valorizacion       DOUBLE PRECISION NOT NULL DEFAULT 0,
    total_registros          INTEGER     NOT NULL DEFAULT 0,
    clientes_unicos          INTEGER     NOT NULL DEFAULT 0,
    import_run_id            INTEGER     NOT NULL,
    CONSTRAINT uq_dim_summary_coarse UNIQUE (
        month, plataforma, provincia, familia, unidad_negocio,
        subunidad_negocio, resultado_participacion, is_identified,
        is_client, import_run_id
    )
);


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 2 — POBLAR DESDE records (idempotente; BAJO TRÁFICO)
-- ════════════════════════════════════════════════════════════════════════════
INSERT INTO dimensionamiento_family_monthly_summary_coarse (
    month, plataforma, provincia, familia, unidad_negocio, subunidad_negocio,
    resultado_participacion, is_identified, is_client,
    total_cantidad, total_valorizacion, total_registros, clientes_unicos, import_run_id
)
SELECT
    date_trunc('month', r.fecha)::date          AS month,
    r.plataforma,
    r.provincia,
    r.familia,
    r.unidad_negocio,
    r.subunidad_negocio,
    r.resultado_participacion,
    r.is_identified,
    r.is_client,
    COALESCE(SUM(r.cantidad_demandada), 0)       AS total_cantidad,
    COALESCE(SUM(r.valorizacion_estimada), 0)    AS total_valorizacion,
    COUNT(*)                                     AS total_registros,
    COUNT(DISTINCT r.cliente_visible)            AS clientes_unicos,
    r.import_run_id
FROM dimensionamiento_records r
WHERE r.import_run_id = (
    SELECT ir.id
    FROM dimensionamiento_import_runs ir
    WHERE ir.status = 'success'
    ORDER BY ir.finished_at DESC NULLS LAST, ir.id DESC
    LIMIT 1
)
GROUP BY
    date_trunc('month', r.fecha)::date,
    r.plataforma, r.provincia, r.familia, r.unidad_negocio,
    r.subunidad_negocio, r.resultado_participacion,
    r.is_identified, r.is_client, r.import_run_id
ON CONFLICT ON CONSTRAINT uq_dim_summary_coarse DO NOTHING;


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 3 — ÍNDICES: DIFERIDOS A PROPÓSITO (NO crear ahora)
-- ════════════════════════════════════════════════════════════════════════════
-- Los EXPLAIN mostraron que el dashboard hace Seq Scan y NO usa los índices de
-- dimensión del summary. Agregarle índices al coarse ahora REPETIRÍA el bloat
-- (índices que no se usarían). La tabla coarse es chica → el seq scan es barato.
-- Solo crear alguno de estos SI un EXPLAIN sobre el coarse (con lecturas reales,
-- Paso 3b) demuestra que aporta. Por ahora quedan COMENTADOS:
--
-- CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_family_month
--     ON dimensionamiento_family_monthly_summary_coarse (is_client, familia, month);
-- CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_province_month
--     ON dimensionamiento_family_monthly_summary_coarse (is_client, provincia, month);
-- (la UNIQUE uq_dim_summary_coarse ya da el índice necesario para el upsert.)


-- ════════════════════════════════════════════════════════════════════════════
-- SECCIÓN 4 — VALIDACIÓN POST-POBLADO (read-only; ver también 05)
-- ════════════════════════════════════════════════════════════════════════════
-- 4.1  Compresión lograda.
-- SELECT
--   (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary_coarse) AS filas_coarse,
--   (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary)        AS filas_fino;
--
-- 4.2  Paridad de totales coarse vs fino (mismo run) → DEBEN coincidir:
-- WITH run AS (
--   SELECT id FROM dimensionamiento_import_runs WHERE status='success'
--   ORDER BY finished_at DESC NULLS LAST, id DESC LIMIT 1)
-- SELECT
--   (SELECT COALESCE(SUM(total_cantidad),0)    FROM dimensionamiento_family_monthly_summary_coarse c, run WHERE c.import_run_id=run.id) AS coarse_cant,
--   (SELECT COALESCE(SUM(total_cantidad),0)    FROM dimensionamiento_family_monthly_summary        f, run WHERE f.import_run_id=run.id) AS fino_cant,
--   (SELECT COALESCE(SUM(total_valorizacion),0) FROM dimensionamiento_family_monthly_summary_coarse c, run WHERE c.import_run_id=run.id) AS coarse_val,
--   (SELECT COALESCE(SUM(total_valorizacion),0) FROM dimensionamiento_family_monthly_summary        f, run WHERE f.import_run_id=run.id) AS fino_val;
-- → coarse_cant == fino_cant  y  coarse_val == fino_val.
