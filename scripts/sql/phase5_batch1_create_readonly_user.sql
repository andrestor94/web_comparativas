-- ============================================================================
-- Fase 5 · Batch 1 — Usuario PostgreSQL de SOLO LECTURA para el parity check
-- ============================================================================
--
--  ⚠️  EJECUCIÓN MANUAL POR UN ADMINISTRADOR DE LA BASE. LEER ANTES DE CORRER.
--  ──────────────────────────────────────────────────────────────────────────
--   • NO se ejecuta desde la app.
--   • NO se ejecuta automáticamente.
--   • NO forma parte de las migraciones (`migrations.py` / Alembic).
--   • NO commitear contraseñas reales: la password va por placeholder y se
--     define a mano, fuera del repo.
--   • Objetivo único: habilitar `scripts/phase5_parity_check.py` con permisos
--     MÍNIMOS (SELECT en 2 tablas). Se ELIMINA al terminar la validación (§C).
--
--  REQUISITOS DEL QUE LO EJECUTA: conectado como el rol dueño/admin de la base
--  (en Render suele tener CREATEROLE). Crear roles requiere CREATEROLE o superuser.
--
--  CÓMO CORRERLO: con psql o DBeaver conectado como admin, ejecutar por bloques
--  (§A crear+permisos, §B verificar, y §C SOLO al final para limpiar).
--  Reemplazar los placeholders:
--     siem_ro_batch1        → nombre del rol (podés dejarlo así)
--     CAMBIAR_PASSWORD_SEGURA → password fuerte generada a mano (ver nota §A.0)
--     DBNAME_DE_RENDER      → nombre real de la base (Render lo muestra en Connections)
-- ============================================================================


-- ════════════════════════════════════════════════════════════════════════════
-- §A — CREAR EL USUARIO Y OTORGAR PERMISOS MÍNIMOS  (ejecuta el ADMIN)
-- ════════════════════════════════════════════════════════════════════════════

-- A.0  (RECOMENDADO) Para NO dejar la password en el historial de SQL, crear el
--      rol SIN password y setearla luego interactivamente en psql con:
--          \password siem_ro_batch1
--      Si usás el CREATE ROLE con PASSWORD de abajo, asegurate de NO commitear
--      el valor real (dejá el placeholder en el repo).

-- A.1  Crear el rol con login y atributos mínimos (sin poderes administrativos).
CREATE ROLE siem_ro_batch1 WITH
    LOGIN
    NOSUPERUSER
    NOCREATEDB
    NOCREATEROLE
    NOINHERIT
    NOREPLICATION
    PASSWORD 'CAMBIAR_PASSWORD_SEGURA';

-- A.2  Base: quitar todo lo heredable y dejar SOLO CONNECT.
REVOKE ALL ON DATABASE DBNAME_DE_RENDER FROM siem_ro_batch1;
GRANT CONNECT ON DATABASE DBNAME_DE_RENDER TO siem_ro_batch1;

-- A.3  Schema public: SOLO USAGE (poder "ver" el schema), nunca CREATE.
REVOKE ALL ON SCHEMA public FROM siem_ro_batch1;
GRANT USAGE ON SCHEMA public TO siem_ro_batch1;

-- A.4  SELECT únicamente sobre las DOS tablas que usa el parity check.
--      (No se otorga nada sobre ninguna otra tabla → no puede leer el resto.)
GRANT SELECT ON TABLE public.dimensionamiento_records TO siem_ro_batch1;
GRANT SELECT ON TABLE public.comparativa_rows        TO siem_ro_batch1;

-- A.5  (OPCIONAL, blindaje extra global — EVALUAR, afecta a TODOS los roles no
--       privilegiados, no solo a éste). En PostgreSQL < 15, el pseudo-rol PUBLIC
--       tiene CREATE en el schema public, así que cualquier rol podría crear
--       tablas ahí. PostgreSQL 15+ ya lo revoca por defecto. Si tu versión es
--       < 15 y querés impedir que ESTE (y cualquier) rol cree objetos en public,
--       el admin puede ejecutar (GLOBAL, pensarlo bien):
--   -- REVOKE CREATE ON SCHEMA public FROM PUBLIC;
--      (No es necesario en Render si corre PG 15/16. Por eso queda comentado.)


-- ════════════════════════════════════════════════════════════════════════════
-- §B — VERIFICAR QUE LOS PERMISOS SON LOS MÍNIMOS  (ejecuta el ADMIN; read-only)
-- ════════════════════════════════════════════════════════════════════════════

-- B.1  Atributos del rol: debe ser canlogin=t y todo lo demás = f.
SELECT rolname, rolsuper, rolcreatedb, rolcreaterole, rolcanlogin, rolreplication
FROM pg_roles
WHERE rolname = 'siem_ro_batch1';
-- Esperado: rolsuper=f, rolcreatedb=f, rolcreaterole=f, rolcanlogin=t, rolreplication=f

-- B.2  Privilegios de tabla: deben ser EXACTAMENTE 2 filas, ambas SELECT.
SELECT grantee, table_schema, table_name, privilege_type
FROM information_schema.role_table_grants
WHERE grantee = 'siem_ro_batch1'
ORDER BY table_name, privilege_type;
-- Esperado: solo (dimensionamiento_records, SELECT) y (comparativa_rows, SELECT)

-- B.3  Chequeos puntuales con has_*_privilege (t/f explícitos).
SELECT
    has_database_privilege('siem_ro_batch1', 'DBNAME_DE_RENDER', 'CONNECT')                AS db_connect,         -- t
    has_schema_privilege  ('siem_ro_batch1', 'public', 'USAGE')                            AS schema_usage,       -- t
    has_schema_privilege  ('siem_ro_batch1', 'public', 'CREATE')                           AS schema_create,      -- f (PG15+)
    has_table_privilege   ('siem_ro_batch1', 'public.dimensionamiento_records', 'SELECT')  AS dim_select,         -- t
    has_table_privilege   ('siem_ro_batch1', 'public.dimensionamiento_records', 'INSERT')  AS dim_insert,         -- f
    has_table_privilege   ('siem_ro_batch1', 'public.comparativa_rows', 'SELECT')          AS comp_select,        -- t
    has_table_privilege   ('siem_ro_batch1', 'public.comparativa_rows', 'UPDATE')          AS comp_update;        -- f

-- B.4  Confirmar que NO puede leer otra tabla sensible (ejemplo: users).
--      Debe devolver FALSE. (Si 'users' no existiera, probá con otra tabla real.)
SELECT has_table_privilege('siem_ro_batch1', 'public.users', 'SELECT') AS puede_leer_users;  -- esperado: f


-- ════════════════════════════════════════════════════════════════════════════
-- §C — ELIMINAR EL USUARIO AL TERMINAR LA VALIDACIÓN  (ejecuta el ADMIN)
-- ════════════════════════════════════════════════════════════════════════════
--   Correr SOLO cuando el parity check ya dio su resultado y no se necesita más.
--   Si hay sesiones abiertas con ese rol, primero cerrarlas (C.0).

-- C.0  (Si DROP ROLE se queja por sesiones activas) terminar conexiones del rol:
-- SELECT pg_terminate_backend(pid)
-- FROM pg_stat_activity
-- WHERE usename = 'siem_ro_batch1' AND pid <> pg_backend_pid();

-- C.1  Revocar permisos y borrar el rol (no posee objetos → DROP limpio).
REVOKE SELECT ON TABLE public.dimensionamiento_records FROM siem_ro_batch1;
REVOKE SELECT ON TABLE public.comparativa_rows        FROM siem_ro_batch1;
REVOKE USAGE  ON SCHEMA public                         FROM siem_ro_batch1;
REVOKE CONNECT ON DATABASE DBNAME_DE_RENDER            FROM siem_ro_batch1;
DROP ROLE IF EXISTS siem_ro_batch1;

-- C.2  Verificar que ya no existe (esperado: 0 filas).
-- SELECT rolname FROM pg_roles WHERE rolname = 'siem_ro_batch1';


-- ════════════════════════════════════════════════════════════════════════════
-- §D — USAR EL USUARIO PARA EL PARITY CHECK  (lo corre el dev, NO el admin)
-- ════════════════════════════════════════════════════════════════════════════
--   Ejemplo de DATABASE_URL (SIN credenciales reales — reemplazar placeholders):
--
--     postgresql://siem_ro_batch1:CAMBIAR_PASSWORD_SEGURA@HOST.render.com/DBNAME_DE_RENDER?sslmode=require
--
--   Comando exacto (Windows / PowerShell):
--
--     $env:DATABASE_URL = "postgresql://siem_ro_batch1:CAMBIAR_PASSWORD_SEGURA@HOST.render.com/DBNAME_DE_RENDER?sslmode=require"
--     python -X utf8 scripts/phase5_parity_check.py --confirm-remote
--     Remove-Item Env:\DATABASE_URL
--
--   Resultado que habilita merge/deploy de Batch 1:
--     Resumen: 4 PASS · 0 FAIL · 0 SKIP   +   RESULTADO: ✅ Paridad OK   (exit 0)
--
--   FRENAR si aparece cualquiera de:
--     • algún FAIL / "❌ HAY DIFERENCIAS" (exit 1)
--     • error de permisos (p. ej. "permission denied for table ...") → revisar §A.4
--     • timeout o caída de conexión → reintentar en bajo tráfico; si persiste, frenar
--   En cualquiera de esos casos: NO mergear, NO deployar, y revisar antes de seguir.
-- ============================================================================
