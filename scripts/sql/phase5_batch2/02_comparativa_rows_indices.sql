-- ============================================================================
-- Fase 5 · Batch 2 · 02 — Índices candidatos para comparativa_rows (Mercado Público)
-- ============================================================================
-- OBJETIVO:
--   Acelerar los filtros frecuentes del Reporte de Perfiles que hoy no tienen un
--   índice compuesto dedicado. Los endpoints filtran muy seguido por
--   plataforma y por rubro junto con un rango de fecha_apertura.
--
--   Índices existentes (confirmar con 03/pg_indexes): fecha_apertura,
--   (upload_id, proveedor), (descripcion, fecha_apertura), (proveedor, fecha_apertura),
--   (marca, fecha_apertura), (comprador, fecha_apertura).
--   FALTAN: (plataforma, fecha_apertura) y (rubro, fecha_apertura).
--
-- NATURALEZA: ADITIVO. Crear índices no cambia resultados, solo planes de ejecución.
--
-- RIESGO: Bajo. Con CONCURRENTLY no bloquea escrituras. Ocupa espacio en disco.
--
-- ⚠️ CONCURRENTLY NO puede ejecutarse dentro de una transacción. Correr cada
--    sentencia en autocommit (psql sin BEGIN; o DBeaver con autocommit ON).
--    En tablas grandes puede tardar; correr en horario de bajo tráfico.
--
-- BACKUP REQUERIDO: Snapshot Render recomendado (estructura). Reversible por DROP.
--
-- VALIDACIÓN: 03_auditoria + EXPLAIN (ANALYZE) de un endpoint que filtre por
--   plataforma/rubro + fecha, antes y después (ver 05). idx_scan del índice nuevo
--   debe crecer tras uso real.
--
-- ROLLBACK: ver 06 (DROP INDEX CONCURRENTLY de cada uno).
-- ============================================================================

-- Pre-chequeo (read-only): ¿ya existen? (no crear si la auditoría los muestra)
-- SELECT indexname, indexdef FROM pg_indexes
-- WHERE tablename = 'comparativa_rows' ORDER BY indexname;

-- Candidato 1: filtros por plataforma + rango de fecha.
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_comp_rows_plataforma_fecha
    ON comparativa_rows (plataforma, fecha_apertura);

-- Candidato 2: filtros por rubro + rango de fecha (usado en kpis/competidor/rubros).
CREATE INDEX CONCURRENTLY IF NOT EXISTS ix_comp_rows_rubro_fecha
    ON comparativa_rows (rubro, fecha_apertura);

-- NOTA: NO se proponen más índices "por las dudas". Cada índice nuevo se justifica
-- con EXPLAIN del endpoint real. Si la auditoría (03) muestra que alguno de estos
-- no se usa tras N semanas, es candidato a retiro en una fase posterior.
