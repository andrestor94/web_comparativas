-- ============================================================================
-- Fase 5 · Batch 2 · 01 — Segundo summary "grueso" de Dimensionamiento (ADITIVO)
-- ============================================================================
-- OBJETIVO:
--   Crear una tabla agregada NUEVA, sin la dimensión "cliente", para alimentar
--   los paneles del dashboard que NO necesitan cliente (KPIs, series, geo,
--   top-familias, resultados). El summary actual casi no comprime (≈260k vs
--   319k de records) porque su clave incluye cliente_nombre_homologado +
--   cliente_visible (alta cardinalidad). Esta tabla agrupa SIN cliente, por lo
--   que debería ser mucho más chica y rápida de escanear.
--
-- ALCANCE / NATURALEZA:
--   ADITIVO. NO reemplaza a dimensionamiento_family_monthly_summary. El código
--   sigue leyendo las tablas actuales. La redirección de lecturas es una fase
--   posterior (cuando se valide). Mientras tanto esta tabla solo existe y se
--   puebla para medir/validar.
--
-- RIESGO: Bajo. Crea una tabla y la puebla desde records (solo lectura de
--   records, escritura en tabla nueva). No toca tablas vivas.
--
-- BACKUP REQUERIDO: Snapshot Render recomendado (es estructura nueva). No hay
--   riesgo de pérdida de datos existentes.
--
-- VALIDACIÓN: ver 05_validacion_antes_despues.sql (comparar SUM(total_cantidad)
--   y SUM(total_valorizacion) del coarse vs el summary fino, para el mismo
--   import_run_id, agrupando el fino sin cliente → deben coincidir).
--
-- ROLLBACK: ver 06_rollback.sql (DROP TABLE de la tabla nueva).
--
-- NOTA SQLite: este script es PostgreSQL (prod). En local (SQLite) no aplica.
-- ============================================================================

-- 1) Tabla nueva (aditiva). Misma forma que el summary fino, SIN columnas de cliente.
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

-- 2) Poblar para el ÚLTIMO import_run exitoso (idempotente: ON CONFLICT DO NOTHING).
--    Si preferís un run específico, reemplazá la subconsulta por: WHERE r.import_run_id = <ID>
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

-- 3) Índices para los patrones del dashboard (is_client + dimensión + month).
--    Aditivos sobre la tabla nueva (no usa CONCURRENTLY porque la tabla recién se
--    crea y nadie la consulta aún).
CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_family_month
    ON dimensionamiento_family_monthly_summary_coarse (is_client, familia, month);
CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_province_month
    ON dimensionamiento_family_monthly_summary_coarse (is_client, provincia, month);
CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_result_month
    ON dimensionamiento_family_monthly_summary_coarse (is_client, resultado_participacion, month);
CREATE INDEX IF NOT EXISTS ix_dim_coarse_isclient_unit_month
    ON dimensionamiento_family_monthly_summary_coarse (is_client, unidad_negocio, month);
CREATE INDEX IF NOT EXISTS ix_dim_coarse_platform_month
    ON dimensionamiento_family_monthly_summary_coarse (plataforma, month);

-- 4) Comparación rápida de tamaño (cuántas filas comprime vs el summary fino del mismo run).
--    (read-only; correr después de poblar)
-- SELECT
--   (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary_coarse) AS filas_coarse,
--   (SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary)        AS filas_fino;
