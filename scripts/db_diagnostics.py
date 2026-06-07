#!/usr/bin/env python
"""
db_diagnostics.py — Diagnóstico READ-ONLY de la base de datos de SIEM.

Genera un reporte (Markdown) con el estado estructural de la base: tablas,
conteos, columnas, índices, PK, FK, tablas forecast_*, vacías, grandes y
advertencias de compatibilidad SQLite <-> PostgreSQL.

================================  SEGURIDAD  ================================
Este script es ESTRICTAMENTE de solo lectura:
  - Solo ejecuta SELECT e introspección (SQLAlchemy Inspector).
  - NUNCA ejecuta INSERT / UPDATE / DELETE.
  - NUNCA ejecuta CREATE / ALTER / DROP.
  - NUNCA imprime DATABASE_URL ni credenciales (host/usuario/password se
    enmascaran: solo se muestra backend + nombre de base).
  - Contra una base remota (PostgreSQL/Render) EXIGE el flag --confirm-remote
    para evitar ejecuciones accidentales contra producción.

No importa la app (web_comparativas) para no disparar migraciones ni efectos
de arranque: construye su propio engine a partir de DATABASE_URL.

================================  USO  =====================================
Local (SQLite, por defecto usa web_comparativas/app.db):
    python scripts/db_diagnostics.py --output docs/db_diagnostics_report.md

Apuntando a una base concreta por variable de entorno:
    DATABASE_URL=sqlite:///ruta/app.db python scripts/db_diagnostics.py

PRODUCCIÓN (PostgreSQL/Render) — SOLO con autorización explícita:
    DATABASE_URL=postgresql://... python scripts/db_diagnostics.py \
        --confirm-remote --estimate --output docs/db_diagnostics_prod.md
  (--estimate usa reltuples de PostgreSQL: rápido y sin escanear tablas)
"""
from __future__ import annotations

import argparse
import datetime as dt
import os
import sys
from pathlib import Path

try:
    from sqlalchemy import create_engine, inspect, text
    from sqlalchemy.engine import Engine
except Exception as exc:  # pragma: no cover
    print(f"[ERROR] SQLAlchemy no disponible: {exc}", file=sys.stderr)
    sys.exit(2)


# --------------------------------------------------------------------------
# Configuración de heurísticas (ajustables sin tocar la lógica)
# --------------------------------------------------------------------------
LARGE_TABLE_ROWS = 100_000  # umbral "tabla grande"

# Tablas legado / fantasma detectadas en la auditoría (candidatas a revisión).
# NO se borran ni se tocan: solo se marcan en el reporte.
REVIEW_CANDIDATE_TABLES = {
    "runs",
    "normalized_files",
    "dashboards",
    "chat_channels",
    "chat_members",
    "chat_messages",
    "revision_sessions",
    "password_reset_requests",
}

FORECAST_PREFIX = "forecast_"

# Tablas forecast_* que SÍ están modeladas en el ORM (overrides/manuales/aprobaciones).
FORECAST_TABLES_IN_ORM = {
    "forecast_user_overrides",
    "forecast_manual_clients",
    "forecast_manual_entries",
    "forecast_change_requests",
}
# Tablas forecast_* de DATOS BASE que NO están modeladas en el ORM (viven en
# PostgreSQL producción; en local son CSV/parquet). Objetivo de la Fase 3.
FORECAST_TABLES_NOT_IN_ORM = {
    "forecast_main",
    "forecast_valorizado",
    "forecast_imp_hist",
    "forecast_fact_2026",
    "forecast_product_labs",
}


# --------------------------------------------------------------------------
# Helpers de conexión (sin exponer credenciales)
# --------------------------------------------------------------------------
def _repo_root() -> Path:
    # scripts/db_diagnostics.py -> repo root es el padre de scripts/
    return Path(__file__).resolve().parent.parent


def _resolve_database_url() -> str:
    raw = (os.getenv("DATABASE_URL") or "").strip()
    if raw:
        # Normaliza el prefijo estilo Heroku/Render como hace la app.
        return raw.replace("postgres://", "postgresql://")
    # Fallback: SQLite local de la app.
    local_db = _repo_root() / "web_comparativas" / "app.db"
    return f"sqlite:///{local_db.as_posix()}"


def _safe_db_label(engine: Engine) -> str:
    """Etiqueta del backend + nombre de base SIN host/usuario/password."""
    url = engine.url
    backend = url.get_backend_name()
    db_name = url.database or "(memoria)"
    if backend == "sqlite":
        # Mostramos solo el nombre del archivo, no la ruta completa.
        db_name = Path(db_name).name if db_name else "(memoria)"
    return f"{backend} / db={db_name}"


def _is_remote(engine: Engine) -> bool:
    return engine.url.get_backend_name() != "sqlite"


# --------------------------------------------------------------------------
# Recolección de métricas (solo lectura)
# --------------------------------------------------------------------------
def _table_rowcount(engine: Engine, table: str, *, estimate: bool) -> int | None:
    backend = engine.url.get_backend_name()
    try:
        with engine.connect() as conn:
            if estimate and backend.startswith("postgresql"):
                # Estimación rápida vía estadísticas del planner (sin escanear).
                val = conn.execute(
                    text("SELECT reltuples::bigint FROM pg_class WHERE relname = :t"),
                    {"t": table},
                ).scalar()
                return int(val) if val is not None else None
            # Conteo exacto. Comillas dobles para identificadores seguros.
            val = conn.execute(text(f'SELECT COUNT(*) FROM "{table}"')).scalar()
            return int(val) if val is not None else 0
    except Exception:
        return None


def _table_size_bytes(engine: Engine, table: str) -> int | None:
    backend = engine.url.get_backend_name()
    if not backend.startswith("postgresql"):
        return None  # SQLite no expone tamaño por tabla de forma portable.
    try:
        with engine.connect() as conn:
            return int(
                conn.execute(
                    text("SELECT pg_total_relation_size(:t)"), {"t": table}
                ).scalar()
                or 0
            )
    except Exception:
        return None


def _human_bytes(n: int | None) -> str:
    if n is None:
        return "—"
    units = ["B", "KB", "MB", "GB", "TB"]
    f = float(n)
    for u in units:
        if f < 1024 or u == units[-1]:
            return f"{f:.0f} {u}" if u == "B" else f"{f:.1f} {u}"
        f /= 1024
    return f"{n} B"


def _compat_warnings(backend: str, columns: list[dict]) -> list[str]:
    """Advertencias de compatibilidad SQLite <-> PostgreSQL por columna."""
    warns: list[str] = []
    for col in columns:
        type_str = str(col.get("type", "")).upper()
        name = col.get("name", "?")
        # JSON: en SQLite es TEXT; en Postgres conviene JSONB.
        if "JSON" in type_str:
            warns.append(f"`{name}`: tipo JSON — verificar JSON vs JSONB entre SQLite/PostgreSQL")
        # BLOB / BYTEA pesados.
        if "BLOB" in type_str or "BYTEA" in type_str or "LARGEBINARY" in type_str:
            warns.append(f"`{name}`: binario (BLOB/BYTEA) — candidato a separar de la tabla")
        # Boolean: SQLite lo guarda como 0/1; revisar defaults.
        if backend.startswith("postgresql") and "BOOLEAN" in type_str:
            pass  # ok en PG
    return warns


# --------------------------------------------------------------------------
# Recolección de snapshot estructurado (solo lectura)
# --------------------------------------------------------------------------
def _pg_index_defs(engine: Engine) -> dict:
    """
    Mapa (tabla, indice) -> indexdef para PostgreSQL, leyendo pg_indexes (SOLO SELECT).
    Permite conservar la definición completa de índices funcionales/por expresión/
    parciales (que la reflexión devuelve con columnas en None). Vacío fuera de PostgreSQL.
    """
    if not engine.url.get_backend_name().startswith("postgresql"):
        return {}
    out: dict = {}
    try:
        with engine.connect() as conn:
            rows = conn.execute(
                text("SELECT tablename, indexname, indexdef FROM pg_indexes WHERE schemaname = 'public'")
            ).fetchall()
        for tbl, idx, ddl in rows:
            out[(tbl, idx)] = ddl
    except Exception as exc:
        print(f"[db_diagnostics][WARN] no se pudo leer pg_indexes: {exc}", file=sys.stderr)
        return {}
    return out


def collect_snapshot(engine: Engine, *, estimate: bool) -> dict:
    """
    Recolecta un snapshot estructural completo (sin filas de datos ni credenciales).
    Reutilizable para el reporte Markdown y para el export JSON (comparación).
    """
    insp = inspect(engine)
    backend = engine.url.get_backend_name()
    table_names = sorted(insp.get_table_names())
    index_defs = _pg_index_defs(engine)  # {(tabla, indice): indexdef} en PostgreSQL

    tables: dict[str, dict] = {}
    for t in table_names:
        try:
            columns = [
                {"name": c.get("name"), "type": str(c.get("type")), "nullable": bool(c.get("nullable"))}
                for c in insp.get_columns(t)
            ]
        except Exception:
            columns = []
        try:
            pk = insp.get_pk_constraint(t).get("constrained_columns") or []
        except Exception:
            pk = []
        try:
            fks = [
                {
                    "constrained_columns": fk.get("constrained_columns") or [],
                    "referred_table": fk.get("referred_table"),
                    "referred_columns": fk.get("referred_columns") or [],
                }
                for fk in insp.get_foreign_keys(t)
            ]
        except Exception:
            fks = []
        try:
            indexes = []
            for ix in insp.get_indexes(t):
                # column_names puede contener None para índices funcionales/por expresión.
                raw_cols = list(ix.get("column_names") or [])
                ddl = index_defs.get((t, ix.get("name")))
                indexes.append({
                    "name": ix.get("name"),
                    "columns": raw_cols,                       # se conserva tal cual (incluye None)
                    "unique": bool(ix.get("unique")),
                    "expressions": [str(e) for e in (ix.get("expressions") or [])] or None,
                    "indexdef": ddl,                           # definición completa (PostgreSQL)
                    "is_expression": any(c is None for c in raw_cols) or bool(ix.get("expressions")),
                    "is_partial": bool(ddl and " WHERE " in ddl.upper()),
                })
        except Exception as exc:
            # No perdemos el reporte por un índice raro: registramos warning y seguimos.
            print(f"[db_diagnostics][WARN] índices de {t}: {exc}", file=sys.stderr)
            indexes = []
        tables[t] = {
            "rows": _table_rowcount(engine, t, estimate=estimate),
            "size_bytes": _table_size_bytes(engine, t),
            "columns": columns,
            "pk": pk,
            "fks": fks,
            "indexes": indexes,
        }

    return {
        "meta": {
            "backend": backend,
            "db_label": _safe_db_label(engine),
            "generated_utc": dt.datetime.utcnow().isoformat(timespec="seconds"),
            "count_mode": "estimate" if (estimate and backend.startswith("postgresql")) else "exact",
        },
        "tables": tables,
    }


def write_json(snapshot: dict, path: Path) -> None:
    import json
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(json.dumps(snapshot, ensure_ascii=False, indent=2), encoding="utf-8")


def _render_index(ix: dict) -> str:
    """
    Renderiza un índice de forma ROBUSTA, incluso si tiene columnas por expresión
    (donde la reflexión devuelve None). No pierde información:
      - columnas None -> placeholder `<expression>`
      - marca funcional/expresión y parcial
      - conserva las expresiones y el indexdef completo cuando están disponibles.
    """
    name = ix.get("name") or "<sin nombre>"
    cols = [(c if c is not None else "<expression>") for c in (ix.get("columns") or [])]
    cols_str = ", ".join(cols) if cols else "—"
    uq = " (unique)" if ix.get("unique") else ""
    tags = []
    if ix.get("is_expression"):
        tags.append("funcional/expresión")
    if ix.get("is_partial"):
        tags.append("parcial")
    tag_str = f" · {', '.join(tags)}" if tags else ""
    line = f"- `{name}` [{cols_str}]{uq}{tag_str}"
    extra = []
    if ix.get("expressions"):
        extra.append(f"  - expresiones: {', '.join(ix['expressions'])}")
    if ix.get("indexdef"):
        extra.append(f"  - `{ix['indexdef']}`")
    if extra:
        line = line + "\n" + "\n".join(extra)
    return line


def _selftest() -> int:
    """
    Smoke test SIN base de datos: reproduce el caso que rompía el reporte
    (un índice por expresión con columns=[None]) más un índice parcial y uno
    normal, y verifica que build_report NO rompe y los representa sin perder info.
    """
    fake = {
        "meta": {
            "backend": "postgresql", "db_label": "selftest",
            "generated_utc": "1970-01-01T00:00:00", "count_mode": "exact",
        },
        "tables": {
            "demo": {
                "rows": 3,
                "size_bytes": None,
                "columns": [{"name": "id", "type": "INTEGER", "nullable": False}],
                "pk": ["id"],
                "fks": [],
                "indexes": [
                    {  # índice por expresión: column_names = [None]  (el caso del bug)
                        "name": "ix_expr", "columns": [None], "unique": False,
                        "expressions": ["lower((email)::text)"],
                        "indexdef": "CREATE INDEX ix_expr ON demo (lower(email))",
                        "is_expression": True, "is_partial": False,
                    },
                    {  # índice parcial
                        "name": "ix_partial", "columns": ["id"], "unique": False,
                        "expressions": None,
                        "indexdef": "CREATE INDEX ix_partial ON demo (id) WHERE id > 0",
                        "is_expression": False, "is_partial": True,
                    },
                    {  # índice normal
                        "name": "ix_normal", "columns": ["id"], "unique": True,
                        "expressions": None, "indexdef": None,
                        "is_expression": False, "is_partial": False,
                    },
                ],
            }
        },
    }
    report = build_report(fake)
    assert "<expression>" in report, "el índice por expresión no se renderizó con placeholder"
    assert "funcional/expresión" in report, "no se marcó el índice funcional/expresión"
    assert "parcial" in report, "no se marcó el índice parcial"
    assert "ix_normal" in report, "no se renderizó el índice normal"
    assert "lower(email)" in report, "no se conservó el indexdef del índice por expresión"
    print("[db_diagnostics][selftest] OK: índices por expresión/parciales/normales se "
          "renderizan sin romper y conservan indexdef.")
    return 0


# --------------------------------------------------------------------------
# Reporte
# --------------------------------------------------------------------------
def build_report(snapshot: dict) -> str:
    meta = snapshot["meta"]
    tables = snapshot["tables"]
    table_names = sorted(tables.keys())
    backend = meta["backend"]

    # Vistas derivadas para no reescribir el resto del render.
    rows_by_table = {t: tables[t]["rows"] for t in table_names}
    size_by_table = {t: tables[t]["size_bytes"] for t in table_names}
    cols_by_table = {t: tables[t]["columns"] for t in table_names}

    lines: list[str] = []
    w = lines.append

    w("# Reporte de diagnóstico de base de datos — SIEM")
    w("")
    w("> Generado por `scripts/db_diagnostics.py` (solo lectura).")
    w(f"> Fecha (UTC): {meta['generated_utc']}")
    w(f"> Motor: **{meta['db_label']}**  ·  conteo: **{'estimado' if meta['count_mode'] == 'estimate' else 'exacto'}**")
    w("")
    w("Este reporte es estructural. No contiene filas de datos ni credenciales.")
    w("")

    forecast_tables = [t for t in table_names if t.startswith(FORECAST_PREFIX)]
    forecast_not_in_orm = [t for t in forecast_tables if t not in FORECAST_TABLES_IN_ORM]
    empty_tables = [t for t in table_names if rows_by_table.get(t) == 0]
    large_tables = [t for t in table_names if (rows_by_table.get(t) or 0) >= LARGE_TABLE_ROWS]
    review_tables = [t for t in table_names if t in REVIEW_CANDIDATE_TABLES]

    w("## Resumen")
    w("")
    w(f"- Total de tablas: **{len(table_names)}**")
    w(f"- Tablas `forecast_*` totales: **{len(forecast_tables)}** "
      f"(en ORM: {len(forecast_tables) - len(forecast_not_in_orm)}, "
      f"NO en ORM / datos base: {len(forecast_not_in_orm)})")
    w(f"- Tablas vacías (0 filas): **{len(empty_tables)}**")
    w(f"- Tablas grandes (>= {LARGE_TABLE_ROWS:,} filas): **{len(large_tables)}**")
    w(f"- Tablas candidatas a revisión (legado/fantasma): **{len(review_tables)}**")
    w("")

    # --- Tabla resumen por tabla ---
    w("## Tablas (conteo, tamaño, PK, FK, índices)")
    w("")
    w("| Tabla | Filas | Tamaño | Cols | PK | FK | Índices | Notas |")
    w("|---|---:|---:|---:|---|---:|---:|---|")
    for t in table_names:
        rows = rows_by_table.get(t)
        rows_str = f"{rows:,}" if isinstance(rows, int) else "—"
        size_str = _human_bytes(size_by_table.get(t))
        cols = cols_by_table.get(t, [])
        pk = tables[t]["pk"]
        fks = tables[t]["fks"]
        idxs = tables[t]["indexes"]
        notes = []
        if t in forecast_not_in_orm:
            notes.append("forecast_* (NO en ORM — datos base)")
        elif t in forecast_tables:
            notes.append("forecast_* (en ORM)")
        if t in review_tables:
            notes.append("revisión")
        if rows == 0:
            notes.append("vacía")
        if (rows or 0) >= LARGE_TABLE_ROWS:
            notes.append("grande")
        pk_str = ",".join(pk) if pk else "—"
        w(f"| `{t}` | {rows_str} | {size_str} | {len(cols)} | {pk_str} | {len(fks)} | {len(idxs)} | {'; '.join(notes)} |")
    w("")

    # --- forecast_* detalle (solo las NO modeladas en el ORM: objetivo Fase 3) ---
    w("## Tablas `forecast_*` de datos base (NO modeladas en el ORM)")
    w("")
    w("> Estas son el objetivo de modelado de la Fase 3. En SQLite local normalmente")
    w("> NO aparecen (los datos base son CSV/parquet); existen como tablas en PostgreSQL")
    w("> producción. Las `forecast_*` que SÍ están en el ORM (overrides, manuales,")
    w("> aprobaciones) no se listan aquí porque ya están modeladas.")
    w("")
    if not forecast_not_in_orm:
        w("_No se detectaron tablas `forecast_*` de datos base en esta base._")
        w("> Esperable en SQLite local. Correr el diagnóstico contra PostgreSQL producción")
        w("> (con autorización) para inspeccionar su esquema real.")
    else:
        for t in forecast_not_in_orm:
            cols = cols_by_table.get(t, [])
            w(f"### `{t}` — {rows_by_table.get(t) if rows_by_table.get(t) is not None else '—'} filas")
            w("")
            w("| Columna | Tipo | Nullable |")
            w("|---|---|---|")
            for c in cols:
                w(f"| `{c.get('name')}` | {c.get('type')} | {c.get('nullable')} |")
            w("")
    w("")

    # --- Tablas vacías / candidatas a revisión ---
    w("## Tablas vacías")
    w("")
    w(", ".join(f"`{t}`" for t in empty_tables) if empty_tables else "_Ninguna._")
    w("")
    w("## Tablas candidatas a revisión (legado / fantasma)")
    w("")
    w("> Presentes en esta base. **No eliminar** sin validación + backup (ver plan de migración).")
    w("")
    if review_tables:
        for t in review_tables:
            w(f"- `{t}` — {rows_by_table.get(t) if rows_by_table.get(t) is not None else '—'} filas")
    else:
        w("_Ninguna de las candidatas conocidas está presente en esta base._")
    w("")

    # --- Advertencias de compatibilidad ---
    w("## Advertencias de compatibilidad SQLite <-> PostgreSQL")
    w("")
    any_warn = False
    for t in table_names:
        warns = _compat_warnings(backend, cols_by_table.get(t, []))
        if warns:
            any_warn = True
            w(f"- **`{t}`**")
            for ww in warns:
                w(f"  - {ww}")
    if not any_warn:
        w("_Sin advertencias de tipos detectadas._")
    w("")

    # --- Apéndice: detalle por tabla (columnas/tipos/nullable, PK, FK, índices) ---
    w("## Apéndice — detalle por tabla")
    w("")
    for t in table_names:
        try:
            info = tables[t]
            rows = info["rows"]
            rows_str = f"{rows:,}" if isinstance(rows, int) else "—"
            w(f"### `{t}` — {rows_str} filas")
            w("")
            w("**Columnas**")
            w("")
            w("| Columna | Tipo | Nullable |")
            w("|---|---|---|")
            for c in info["columns"]:
                w(f"| `{c['name']}` | {c['type']} | {c['nullable']} |")
            w("")
            pk_cols = [str(c) for c in (info["pk"] or []) if c is not None]
            w(f"**PK:** {', '.join(pk_cols) if pk_cols else '—'}")
            w("")
            if info["fks"]:
                w("**FK:**")
                for fk in info["fks"]:
                    cc = ", ".join(str(c) for c in fk["constrained_columns"])
                    rc = ", ".join(str(c) for c in fk["referred_columns"])
                    w(f"- ({cc}) → `{fk['referred_table']}` ({rc})")
            else:
                w("**FK:** —")
            w("")
            if info["indexes"]:
                w("**Índices:**")
                for ix in info["indexes"]:
                    w(_render_index(ix))
            else:
                w("**Índices:** —")
            w("")
        except Exception as exc:
            # Una tabla rara no debe romper el reporte completo.
            w(f"### `{t}` — (error al renderizar: {exc})")
            w("")
            print(f"[db_diagnostics][WARN] no se pudo renderizar la tabla {t}: {exc}", file=sys.stderr)
    w("---")
    w(f"_Backend analizado: {meta['db_label']}._")
    return "\n".join(lines) + "\n"


def main() -> int:
    parser = argparse.ArgumentParser(description="Diagnóstico READ-ONLY de la base de datos SIEM.")
    parser.add_argument(
        "--output", "-o",
        default=str(_repo_root() / "docs" / "db_diagnostics_report.md"),
        help="Ruta del reporte Markdown a generar.",
    )
    parser.add_argument(
        "--estimate",
        action="store_true",
        help="PostgreSQL: usar estimación de filas (reltuples) en vez de COUNT exacto.",
    )
    parser.add_argument(
        "--confirm-remote",
        action="store_true",
        help="Requerido para correr contra una base NO-SQLite (ej. PostgreSQL/Render).",
    )
    parser.add_argument(
        "--print",
        dest="to_stdout",
        action="store_true",
        help="Imprimir el reporte por stdout además de escribir el archivo.",
    )
    parser.add_argument(
        "--json",
        dest="json_path",
        default=None,
        help="Ruta opcional para exportar el snapshot estructural como JSON "
             "(útil para comparar local vs producción con scripts/db_compare.py).",
    )
    parser.add_argument(
        "--selftest",
        action="store_true",
        help="Smoke test SIN base de datos: valida el render de índices raros "
             "(por expresión / parciales). No conecta a ninguna base.",
    )
    args = parser.parse_args()

    if args.selftest:
        return _selftest()

    url = _resolve_database_url()
    # Engine read-only por convención (no ejecutamos DDL/DML en ningún punto).
    engine = create_engine(url, future=True)

    if _is_remote(engine) and not args.confirm_remote:
        print(
            "[ABORTADO] La URL apunta a una base NO-SQLite (posible producción).\n"
            f"           Motor: {_safe_db_label(engine)}\n"
            "           Reejecutá con --confirm-remote SOLO si tenés autorización explícita.\n"
            "           Este script es read-only, pero no se corre contra producción por defecto.",
            file=sys.stderr,
        )
        return 3

    print(f"[db_diagnostics] Analizando: {_safe_db_label(engine)}", flush=True)
    snapshot = collect_snapshot(engine, estimate=args.estimate)
    report = build_report(snapshot)

    out_path = Path(args.output)
    out_path.parent.mkdir(parents=True, exist_ok=True)
    out_path.write_text(report, encoding="utf-8")
    print(f"[db_diagnostics] Reporte escrito en: {out_path}", flush=True)

    if args.json_path:
        json_path = Path(args.json_path)
        write_json(snapshot, json_path)
        print(f"[db_diagnostics] Snapshot JSON escrito en: {json_path}", flush=True)

    if args.to_stdout:
        print(report)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
