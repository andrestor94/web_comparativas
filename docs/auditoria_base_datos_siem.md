# Auditoría de Base de Datos — SIEM

> **Estado:** Fase 1 (diagnóstico) completada · Fase 2 (mejoras seguras) en curso.
> **Última actualización:** 2026-06 · **Alcance:** arquitectura de datos del proyecto `web_comparativas`.
>
> Este documento es la **fuente de verdad** del estado actual de la base de datos.
> Cualquier desarrollador debería poder entender el panorama sin rehacer la auditoría.

---

## 1. Resumen ejecutivo

SIEM creció módulo por módulo y la capa de datos acumuló tablas, columnas y parches.
La interfaz funciona, pero por detrás hay deuda técnica que afecta performance,
mantenibilidad y claridad. La buena noticia: el núcleo está **mejor de lo que aparenta**
(razonablemente indexado), y los problemas reales están **localizados**.

**Hallazgo central:** SIEM no tiene una sola base de datos, sino **tres mundos de datos
distintos** que conviene no mezclar. La mayor parte del riesgo no está en el ORM principal,
sino en (1) tablas `forecast_*` de producción que el código ORM no conoce, (2) consultas
analíticas que leen tablas grandes enteras y calculan en Python, (3) tablas "fantasma"
que se crean en cada deploy sin que nadie las use, y (4) un sistema de migraciones
artesanal sin versionado ni rollback.

**Regla de oro de esta auditoría:** no se borra nada, no se toca producción sin backup,
y toda mejora debe ser reversible y probada en local primero. PostgreSQL en Render es la
**fuente de verdad**.

---

## 2. Los tres mundos de datos

| Mundo | Motor | Gestión | Rol |
|---|---|---|---|
| **1. App principal** | SQLite (local) / PostgreSQL (Render) | SQLAlchemy ORM (`models.py`, `dimensionamiento/models.py`) | Corazón de la app: usuarios, uploads, pliegos, dimensionamiento, overrides |
| **2. Forecast (datos base)** | CSV/Parquet en disco (local) / tablas `forecast_*` en Postgres (Render) | **Fuera del ORM** — script de ingesta externo | Datos base del forecast (no los overrides) |
| **3. Indicadores Comerciales** | SQL Server externo (`ETL_Data`, `Fusion`) | Bridge PowerShell (`indicadores_db.py`) | Solo lectura externa, aislada del resto |

### Detalle de cada mundo

**Mundo 1 — App principal (SQLAlchemy)**
Configuración en `models.py`: usa `DATABASE_URL` si está presente (Postgres en Render),
si no cae a SQLite local (`web_comparativas/app.db`). Tiene un **guardia de seguridad**
que bloquea el arranque local si la `DATABASE_URL` apunta a un host de Render (evita que
desarrollo pegue contra producción). SQLite usa `StaticPool`; Postgres usa `QueuePool`
(pool_size=15, max_overflow=10) con auto-detección de conexión interna vs externa de Render.

**Mundo 2 — Forecast**
- **Local (SQLite):** lee archivos de `web_comparativas/data/forecast_data/`
  (`forecast_base_consolidado.csv`, `fact_forecast_valorizado.parquet` ~702k filas,
  `importe_historico.csv`, `facturacion_real_2026_sin_neg2.csv`, `Articulos 1.csv`,
  `clientes.csv`, etc.). Se cachean DataFrames en memoria (`_data_cache`).
- **Producción (Render):** lee tablas SQL `forecast_main`, `forecast_valorizado`,
  `forecast_imp_hist`, `forecast_fact_2026`, `forecast_product_labs` vía `pd.read_sql()`
  con queries filtradas (explícitamente evita cargar todo en memoria por riesgo de OOM).
- ⚠️ **Estas tablas `forecast_*` NO están en `models.py` ni en las migraciones.** Su
  esquema y su carga dependen de un proceso externo. Es el mayor punto ciego de la auditoría.

**Mundo 3 — Indicadores Comerciales**
`indicadores_db.py` se conecta a SQL Server vía un *bridge* PowerShell
(`sqlclient_bridge.ps1`) leyendo credenciales de un `.env` en
`Indicadores Comerciales/03 - Rentabilidad Negativa/backend/`. No toca `app.db` ni Postgres.
Es read-only y está correctamente aislado.

---

## 3. Inventario de tablas por módulo

> 44 tablas en el ORM. Las tablas `forecast_*` (mundo 2) y las de Indicadores (mundo 3)
> no están en el ORM y no se inventarían aquí más allá de la mención.

### Núcleo / Identidad
| Tabla | Función |
|---|---|
| `users` | Usuarios, roles, unidad de negocio, `access_scope`. Centro de casi todas las FK. |
| `groups`, `group_members` | Grupos N:N de usuarios. |
| `app_config` | Configuración clave/valor (incluye contraseña de reset). |
| `password_reset_requests` | Flujo interno de restablecimiento de contraseña. |
| `saved_views` | Preferencias de vista por usuario (filtros, columnas, densidad). |
| `notifications`, `email_notifications` | Avisos in-app y log de emails (con idempotencia). |

### Mercado Público / Comparativas
| Tabla | Función |
|---|---|
| `uploads` | Proceso cargado + metadatos + **BLOBs** (`original_content`, `normalized_content`, `dashboard_json`). Tabla pesada. |
| `comparativa_rows` | Fila desnormalizada parseada de los BLOBs. **Fuente de verdad del Reporte de Perfiles.** Bien indexada. |
| `comments` | Feedback/hilos asociados a un upload. |

### Forecast (solo overrides — los datos base son del mundo 2)
| Tabla | Función |
|---|---|
| `forecast_user_overrides` | Ajustes porcentuales del usuario sobre la base. |
| `forecast_manual_clients`, `forecast_manual_entries` | Clientes y líneas agregados manualmente. |
| `forecast_change_requests` | Registro de control de cambios (aprobaciones). No bloquea: solo audita. |

### Dimensionamiento / Mercado Privado
| Tabla | Función |
|---|---|
| `dimensionamiento_records` | ~319k filas. Base cruda. Bien indexada (múltiples índices compuestos). |
| `dimensionamiento_family_monthly_summary` | **Agregado mensual precalculado.** La pieza correcta para el dashboard. |
| `dimensionamiento_dashboard_snapshots` | Snapshot JSON del dashboard (evita recalcular). |
| `dimensionamiento_import_runs`, `dimensionamiento_import_errors` | Auditoría de ingestas. |

### Lectura de Pliegos (20 tablas, todas activas)
`pliego_solicitudes` (cabecera) + tablas hijas 1-fila-por-hoja-de-Excel: `pliego_archivos`,
`pliego_historial`, `pliego_excel_cargas`, `pliego_proceso`, `pliego_cronograma`,
`pliego_requisitos`, `pliego_garantias`, `pliego_renglones`, `pliego_documentos`,
`pliego_actos_admin`, `pliego_hallazgos`, `pliego_faltantes`, `pliego_trazabilidad`,
`pliego_fusion_cabecera`, `pliego_fusion_renglones`, `pliego_analitica`,
`pliego_control_carga` + overrides (`pliego_field_overrides`, `pliego_edit_history`).
Diseño deliberadamente normalizado y coherente. **No fusionar.**

### Help Desk / SIC
`tickets`, `ticket_messages`.

### Métricas de uso
`usage_events` (log granular, crece sin techo), `usage_sessions` (resumen agregado).

---

## 4. Clasificación de tablas

### 4.1 Activas (mantener, sin tocar)
Todo el núcleo, Mercado Público (`uploads`, `comparativa_rows`, `comments`),
Dimensionamiento (5 tablas), Pliegos (20 tablas), Tickets, Forecast overrides (4 tablas),
Notifications, métricas de uso, grupos, saved views, app_config.

### 4.2 Candidatas a revisión (NO eliminar — validar primero)
| Tabla | Por qué se revisa |
|---|---|
| `runs` | Del pipeline de procesamiento viejo. Solo aparece en `DELETE` de limpieza. ¿El procesamiento actual aún la escribe? |
| `normalized_files` | Ídem `runs`. Sin instanciación ni lectura activa. |
| `dashboards` | Ídem. Sin instanciación ni lectura activa. |
| `password_reset_requests` | Instanciada en `legacy_routes.py` pero sin endpoints de lectura visibles. Confirmar que el flujo sigue activo. |

### 4.3 Fantasma / legado (definidas pero sin flujo activo)
| Tabla | Estado |
|---|---|
| `chat_channels`, `chat_members`, `chat_messages` | Feature de chat tipo Teams **nunca terminada**. Sin router ni UI. Solo `DELETE` en reset masivo. |
| `revision_sessions` | Capa de curación de IA nunca implementada. Cero instanciaciones. |

> ⚠️ Estas tablas las **crea `Base.metadata.create_all` en cada deploy**, así que existen
> físicamente en producción aunque nadie las use. **No se eliminan en esta fase** — primero
> validar 0 uso en producción con `SELECT count(*)` y backup; luego renombrar a
> `zz_deprecated_*` (no `DROP`).

---

## 5. Riesgos encontrados

| # | Riesgo | Severidad | Detalle |
|---|---|---|---|
| R1 | Tablas `forecast_*` invisibles al ORM | 🔴 Alta | Viven en Postgres pero no en `models.py` ni migraciones. Esquema sin versionar, carga por script externo. |
| R2 | Lecturas completas + cálculo en Python | 🔴 Alta | Mercado Público/Privado descargan miles/millones de filas con `.all()` sin `LIMIT` y calculan medianas/agregados en Python. |
| R3 | Forecast carga overrides sin límite | 🟠 Media | `_fetch_override_records()` hace `.all()`; el path admin (`all_users`) trae overrides de todos los usuarios. |
| R4 | Migraciones sin versionado ni rollback | 🟠 Media | Funciones `ensure_*` corren en cada arranque; backfills pesados en background en cada deploy; sin registro de qué se aplicó. |
| R5 | BLOBs en `uploads` | 🟠 Media | `LargeBinary` + JSON en la misma tabla que se lista. Un `SELECT` descuidado arrastra megabytes por fila. |
| R6 | Divergencia local vs producción en Forecast | 🟠 Media | Local lee CSV/Parquet; producción lee SQL. Dos rutas de código → bugs solo-en-prod. |
| R7 | `usage_events` sin retención | 🟡 Baja | Log que crece indefinidamente, sin archivado/purga. |
| R8 | Inconsistencia de tipos datetime | 🟡 Baja | Pliegos usa `DateTime(timezone=True)`; el resto `utcnow` naive. |
| R9 | `create_all` materializa tablas fantasma | 🟡 Baja | Cada deploy recrea `chat_*`, `runs`, etc. en prod. |

---

## 6. Endpoints críticos de performance

1. 🔴 **Mercado Público — `articulos_por_proveedor`** (`mercado_publico_perfiles_router.py`):
   trae todo el histórico de precios de los proveedores del filtro sin `LIMIT`, arma
   diccionarios en Python.
2. 🔴 **Mercado Público — `/filtros`**: `DISTINCT` de compradores sin límite en SQL
   (se truncaba a 200 en Python). *Mitigado en Fase 2* (ver §7).
3. 🔴 **Mercado Privado — KPIs y evolución de precio** (`mercado_privado_perfiles_router.py`):
   mediana calculada en Python sobre `dimensionamiento_records` completo.
4. 🟠 **Dimensionamiento — fallback a tabla cruda** (`query_service.py`): si la summary se
   considera "no usable", se escanea `dimensionamiento_records`. *Observabilidad mejorada en
   Fase 2* (ver §7).
5. 🟠 **Forecast — overrides sin límite** (`forecast_service.py:617`) e iteración `.iterrows()`
   sobre ~700k filas en treemap/chart.

---

## 7. Recomendaciones por fase

### Fase 0 — Seguridad de datos (antes de TODO cambio estructural)
- Backup completo de Postgres producción (`pg_dump` / snapshot de Render).
- Export de tablas críticas a parquet/CSV.
- Script de verificación de conteos por tabla (antes/después).

### Fase 1 — Diagnóstico ✅ (este documento).

### Fase 2 — Optimización sin cambiar estructura (bajo riesgo) — EN CURSO
- **Hacer visible el fallback de Dimensionamiento** (logging con razón y gravedad). ✅
- **Empujar el cap de compradores a SQL** en `/filtros` (behavior-preserving). ✅
- **Instrumentar la carga de overrides de Forecast** (sin cambiar el cálculo). ✅
- Pendiente: mover medianas/agregados a SQL (`percentile_cont` en Postgres) con fallback SQLite.
- Pendiente: índices nuevos validados con `EXPLAIN ANALYZE`.

### Fase 3 — Migración ordenada (con Alembic) — NO IMPLEMENTAR AÚN
- Introducir Alembic con *baseline* del esquema actual (`alembic stamp head`).
- Modelar `forecast_*` y `upload_blobs` (separar BLOBs de `uploads`).
- Migrar datos con scripts auditables; mantener columnas viejas hasta validar.

### Fase 4 — Deprecación controlada — NO IMPLEMENTAR AÚN
- Renombrar tablas fantasma a `zz_deprecated_*` (no `DROP`) tras validar 0 uso + backup.
- `DROP` solo con confirmación humana explícita y backup vigente.

---

## 8. Checklist de seguridad antes de tocar producción

**Antes:**
- [ ] Backup `pg_dump` de producción tomado y verificado.
- [ ] Conteos por tabla guardados (`SELECT count(*)`).
- [ ] `EXPLAIN ANALYZE` de la query objetivo.
- [ ] Cambio probado en SQLite local.
- [ ] Cambio probado contra una copia de datos representativa.

**Después:**
- [ ] Conteos coinciden (o la diferencia es esperada y está documentada).
- [ ] Smoke test de cada módulo: Forecast, Mercado Público, Mercado Privado, Pliegos, Dimensionamiento.
- [ ] Tiempos de respuesta comparados contra baseline.
- [ ] Logs sin errores nuevos.
- [ ] Rollback probado / disponible.

---

## 9. Sistema de migraciones (estado actual)

Hoy: funciones idempotentes `ensure_*` en `migrations.py`, invocadas en orden en el
`startup` de `main.py`. Es defensivo (try/except por bloque) y funciona, **pero**:
- No registra qué se aplicó (sin `alembic_version` ni equivalente).
- No tiene *rollback* / *down migration*.
- Mezcla DDL con *backfills* pesados que corren en cada arranque/deploy.

**Recomendación (Fase 3):** adoptar **Alembic** (estándar SQLAlchemy, soporta SQLite y
Postgres) con `alembic stamp head` sobre el esquema actual como baseline, sin recrear nada.
**No implementar todavía.**

---

## 10. Preguntas pendientes de validación humana

1. **Forecast en producción:** ¿cómo se cargan/actualizan las tablas `forecast_main`,
   `forecast_valorizado`, etc. en Render? ¿Hay un script de ingesta documentado? (Máxima prioridad.)
2. **Chat (Teams-like):** ¿feature cancelada o pausada? Define si `chat_*` se deprecan.
3. **`revision_sessions`** (curación de IA): ¿se va a implementar?
4. **`runs` / `normalized_files` / `dashboards`:** ¿el procesamiento actual aún las escribe, o son 100% legado?
5. **`password_reset_requests`:** ¿el flujo de reset interno sigue en uso?
6. **Acceso a `pg_dump` de Render:** ¿se tiene, o hay que documentar el procedimiento paso a paso?

---

## 11. Convenciones para mantener la base hacia adelante

- **No agregar tablas sin un módulo que las consuma.** Si un feature se pausa, documentarlo aquí.
- **Toda tabla nueva** debe definirse en el ORM (`models.py` o submódulo), nunca solo por script externo.
- **Cálculos analíticos en SQL**, no en Python sobre datasets completos.
- **Listados nunca traen BLOBs**: seleccionar columnas explícitas.
- **Logs de fallback visibles**: si el código degrada a una ruta más lenta, debe loguearlo con razón y gravedad.
- **Compatibilidad SQLite ↔ PostgreSQL**: ramificar DDL/funciones por backend (`IS_SQLITE` / `IS_POSTGRES`).
- **Producción es la fuente de verdad**; local se alinea a ella, nunca al revés.
</content>
</invoke>
