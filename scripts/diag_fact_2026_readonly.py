"""
diag_fact_2026_readonly.py
==========================
Diagnóstico de SOLO LECTURA para entender la diferencia entre
local ($2.544M) y producción ($1.881M) para Perfil=DRO, Abr 2026.

NO modifica ningún dato.
NO toca tablas de usuarios.
NO hace INSERT / UPDATE / DELETE / TRUNCATE / DROP.

Ejecutar:
  PowerShell:
    $env:DATABASE_URL="postgresql://..."
    python scripts/diag_fact_2026_readonly.py

  CMD:
    set DATABASE_URL=postgresql://...
    python scripts/diag_fact_2026_readonly.py

Salida: imprime todo en stdout, lista de hipótesis y diagnóstico final.
"""
from __future__ import annotations

import os
import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

# ─── Conexión ─────────────────────────────────────────────────────────────────

def get_engine():
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        env_path = REPO_ROOT / "web_comparativas" / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8", errors="replace").splitlines():
                line = line.strip()
                if line.startswith("DATABASE_URL=") and not line.startswith("#"):
                    db_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    if not db_url or "postgresql" not in db_url:
        print("ERROR: DATABASE_URL no configurada o no es PostgreSQL.")
        print("Configurá antes de ejecutar:")
        print('  PowerShell: $env:DATABASE_URL="postgresql://user:pass@host/db"')
        print('  CMD:        set DATABASE_URL=postgresql://user:pass@host/db')
        sys.exit(1)

    from sqlalchemy import create_engine
    if "sslmode" not in db_url:
        db_url += ("&" if "?" in db_url else "?") + "sslmode=require"
    engine = create_engine(db_url, pool_pre_ping=True)
    # Mask credentials for logging
    try:
        host_part = db_url.split("@")[-1].split("?")[0]
        print(f"[DIAG] Conectando a: ...@{host_part}")
    except Exception:
        pass
    return engine


def run_sql(engine, sql: str, label: str) -> pd.DataFrame:
    """Ejecuta una query READ-ONLY y devuelve DataFrame. Muestra errores sin fallar."""
    try:
        from sqlalchemy import text
        with engine.connect() as conn:
            result = conn.execute(text(sql))
            df = pd.DataFrame(result.mappings().all())
        if not df.empty and "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        return df
    except Exception as exc:
        print(f"  [ERROR en {label}]: {exc}")
        return pd.DataFrame()


def fmt(v) -> str:
    try:
        return f"${float(v or 0):>20,.0f}"
    except Exception:
        return str(v)


# ─── Main ─────────────────────────────────────────────────────────────────────

def main():
    print("=" * 70)
    print("DIAGNÓSTICO FORECAST_FACT_2026 — SOLO LECTURA")
    print("=" * 70)
    print()

    engine = get_engine()
    from sqlalchemy import text
    with engine.connect() as conn:
        pg_ver = conn.execute(text("SELECT version()")).fetchone()[0]
        print(f"[DIAG] PostgreSQL: {pg_ver[:60]}")
    print()

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 1: Estructura de forecast_fact_2026
    # ──────────────────────────────────────────────────────────────────────────
    print("─" * 60)
    print("BLOQUE 1: Estructura de forecast_fact_2026")
    print("─" * 60)

    df_cols = run_sql(engine, """
        SELECT column_name, data_type, is_nullable
        FROM information_schema.columns
        WHERE table_name = 'forecast_fact_2026'
        ORDER BY ordinal_position
    """, "columnas")
    if df_cols.empty:
        print("  ⚠️  Tabla forecast_fact_2026 NO EXISTE en este PostgreSQL.")
        print("     → Hipótesis: la tabla nunca fue cargada en este entorno.")
        return
    print("  Columnas:")
    for _, row in df_cols.iterrows():
        print(f"    {row['column_name']:<30} {row['data_type']:<20} nullable={row['is_nullable']}")

    df_count = run_sql(engine, "SELECT COUNT(*) AS total FROM forecast_fact_2026", "count")
    total_rows = int(df_count["total"].iloc[0]) if not df_count.empty else 0
    print(f"\n  Total filas: {total_rows:,}")

    df_dates = run_sql(engine, """
        SELECT MIN(fecha) AS min_fecha, MAX(fecha) AS max_fecha
        FROM forecast_fact_2026
    """, "date_range")
    if not df_dates.empty:
        print(f"  Rango fechas: {df_dates['min_fecha'].iloc[0]} → {df_dates['max_fecha'].iloc[0]}")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 2: Perfiles disponibles (tipocli)
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("BLOQUE 2: Perfiles en forecast_fact_2026 (columna tipocli)")
    print("─" * 60)

    has_tipocli = "tipocli" in df_cols["column_name"].values
    has_perfil  = "perfil"  in df_cols["column_name"].values

    if has_tipocli:
        df_perfiles = run_sql(engine, """
            SELECT tipocli, COUNT(*) AS filas, SUM(imp_hist) AS total
            FROM forecast_fact_2026
            WHERE fecha >= '2026-01-01'
            GROUP BY tipocli
            ORDER BY total DESC NULLS LAST
        """, "perfiles_tipocli")
        if not df_perfiles.empty:
            print("  Por tipocli (columna enriquecida):")
            for _, row in df_perfiles.iterrows():
                marker = " ← DRO" if str(row.get("tipocli", "")).strip() == "DRO" else ""
                print(f"    tipocli={str(row['tipocli'])!r:<12} filas={int(row['filas']):>7,}  total={fmt(row['total'])}{marker}")
        else:
            print("  No hay datos para 2026 en tipocli.")
    else:
        print("  ⚠️  Columna tipocli NO existe.")

    if has_perfil:
        df_perf_raw = run_sql(engine, """
            SELECT perfil, COUNT(*) AS filas
            FROM forecast_fact_2026
            WHERE fecha >= '2026-01-01'
            GROUP BY perfil ORDER BY filas DESC LIMIT 20
        """, "perfiles_perfil")
        if not df_perf_raw.empty:
            print("  Por perfil (columna original del CSV):")
            for _, row in df_perf_raw.iterrows():
                print(f"    perfil={str(row['perfil'])!r:<20} filas={int(row['filas']):>7,}")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 3: DRO mes a mes por tipocli (método DIRECTO)
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("BLOQUE 3: DRO 2026 mes a mes — método DIRECTO (tipocli='DRO')")
    print("─" * 60)
    print("  (Este es el valor 'verdad' de la tabla, sin join con otras tablas)")

    if has_tipocli:
        df_dro_direct = run_sql(engine, """
            SELECT
                DATE_TRUNC('month', fecha) AS mes,
                COUNT(*)                   AS filas,
                COUNT(DISTINCT codigo_serie) AS series,
                COUNT(DISTINCT cliente_id) AS clientes,
                SUM(imp_hist)              AS total
            FROM forecast_fact_2026
            WHERE tipocli = 'DRO'
              AND fecha >= '2026-01-01'
              AND fecha < '2026-05-01'
            GROUP BY DATE_TRUNC('month', fecha)
            ORDER BY mes
        """, "dro_direct")
        if not df_dro_direct.empty:
            for _, row in df_dro_direct.iterrows():
                mes = pd.Timestamp(row["mes"]).strftime("%Y-%m")
                print(f"  {mes}: {fmt(row['total'])}  ({int(row['filas']):>7,} filas, {int(row['clientes'])} clientes, {int(row['series'])} series)")
            total_dro_direct = float(df_dro_direct["total"].sum())
            print(f"  {'TOTAL DRO Ene-Abr 2026':<20}: {fmt(total_dro_direct)}")
        else:
            print("  ⚠️  Sin filas DRO vía tipocli.")
    else:
        print("  ⚠️  No se puede consultar — columna tipocli no existe.")
        total_dro_direct = 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 4: DRO mes a mes por JOIN con forecast_valorizado (método PRODUCCIÓN)
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("BLOQUE 4: DRO 2026 mes a mes — método PRODUCCIÓN (JOIN con forecast_valorizado)")
    print("─" * 60)
    print("  (Esta es la query real que usa el endpoint /api/chart-data en producción)")

    df_dro_join = run_sql(engine, """
        SELECT
            DATE_TRUNC('month', ff.fecha) AS mes,
            COUNT(*)                       AS filas,
            COUNT(DISTINCT ff.cliente_id)  AS clientes,
            SUM(ff.imp_hist)               AS total
        FROM forecast_fact_2026 ff
        WHERE ff.fecha >= '2026-01-01'
          AND ff.fecha < '2026-05-01'
          AND CAST(ff.cliente_id AS TEXT) IN (
              SELECT DISTINCT CAST(fv.cliente_id AS TEXT)
              FROM forecast_valorizado fv
              WHERE fv.perfil = 'DRO'
          )
        GROUP BY DATE_TRUNC('month', ff.fecha)
        ORDER BY mes
    """, "dro_join")

    if not df_dro_join.empty:
        for _, row in df_dro_join.iterrows():
            mes = pd.Timestamp(row["mes"]).strftime("%Y-%m")
            print(f"  {mes}: {fmt(row['total'])}  ({int(row['filas']):>7,} filas, {int(row['clientes'])} clientes)")
        total_dro_join = float(df_dro_join["total"].sum())
        print(f"  {'TOTAL DRO Ene-Abr 2026':<20}: {fmt(total_dro_join)}")
    else:
        print("  ⚠️  Sin filas DRO vía JOIN.")
        total_dro_join = 0.0

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 5: ¿Cuántos clientes DRO están en cada tabla?
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("BLOQUE 5: Clientes DRO en forecast_valorizado vs forecast_fact_2026")
    print("─" * 60)

    df_fv_dro = run_sql(engine, """
        SELECT COUNT(DISTINCT cliente_id) AS clientes_dro_valorizado
        FROM forecast_valorizado
        WHERE perfil = 'DRO'
    """, "fv_dro_count")
    clientes_valorizado = int(df_fv_dro["clientes_dro_valorizado"].iloc[0]) if not df_fv_dro.empty else 0
    print(f"  Clientes DRO en forecast_valorizado: {clientes_valorizado}")

    if has_tipocli:
        df_ff_dro = run_sql(engine, """
            SELECT COUNT(DISTINCT cliente_id) AS clientes_dro_fact
            FROM forecast_fact_2026
            WHERE tipocli = 'DRO'
              AND fecha >= '2026-01-01'
        """, "ff_dro_count")
        clientes_fact = int(df_ff_dro["clientes_dro_fact"].iloc[0]) if not df_ff_dro.empty else 0
        print(f"  Clientes DRO en forecast_fact_2026 (tipocli): {clientes_fact}")

        # Clientes en fact_2026 DRO que NO están en valorizado
        df_missing = run_sql(engine, """
            SELECT COUNT(DISTINCT ff.cliente_id) AS clientes_no_en_valorizado
            FROM forecast_fact_2026 ff
            WHERE ff.tipocli = 'DRO'
              AND ff.fecha >= '2026-01-01'
              AND CAST(ff.cliente_id AS TEXT) NOT IN (
                  SELECT DISTINCT CAST(fv.cliente_id AS TEXT)
                  FROM forecast_valorizado fv
                  WHERE fv.perfil = 'DRO'
              )
        """, "missing_clients")
        missing = int(df_missing["clientes_no_en_valorizado"].iloc[0]) if not df_missing.empty else 0
        print(f"  Clientes DRO en fact_2026 que NO están en valorizado: {missing}")
        if missing > 0:
            print(f"  ⚠️  ESTOS {missing} CLIENTES son la causa de la diferencia")
            print(f"      El JOIN los excluye aunque tipocli='DRO' en forecast_fact_2026")

    # ──────────────────────────────────────────────────────────────────────────
    # BLOQUE 6: Valor Abr-2026 específico (comparación directa con el número del usuario)
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("─" * 60)
    print("BLOQUE 6: Abr-2026 DRO — comparación directa")
    print("─" * 60)

    apr_direct = 0.0
    apr_join = 0.0

    if has_tipocli:
        df_apr_d = run_sql(engine, """
            SELECT SUM(imp_hist) AS total
            FROM forecast_fact_2026
            WHERE tipocli = 'DRO'
              AND fecha >= '2026-04-01' AND fecha < '2026-05-01'
        """, "apr_direct")
        apr_direct = float(df_apr_d["total"].iloc[0]) if not df_apr_d.empty and df_apr_d["total"].iloc[0] is not None else 0.0

    df_apr_j = run_sql(engine, """
        SELECT SUM(ff.imp_hist) AS total
        FROM forecast_fact_2026 ff
        WHERE ff.fecha >= '2026-04-01' AND ff.fecha < '2026-05-01'
          AND CAST(ff.cliente_id AS TEXT) IN (
              SELECT DISTINCT CAST(fv.cliente_id AS TEXT)
              FROM forecast_valorizado fv
              WHERE fv.perfil = 'DRO'
          )
    """, "apr_join")
    apr_join = float(df_apr_j["total"].iloc[0]) if not df_apr_j.empty and df_apr_j["total"].iloc[0] is not None else 0.0

    local_value     = 2_544_857_468.0  # valor correcto local vía tipocli
    prod_visual     = 1_881_561_889.0  # valor que ve el usuario en producción

    print(f"  Abr-2026 DRO — directo (tipocli): {fmt(apr_direct)}")
    print(f"  Abr-2026 DRO — via JOIN valor.:   {fmt(apr_join)}")
    print(f"  Abr-2026 DRO — local correcto:    {fmt(local_value)}")
    print(f"  Abr-2026 DRO — producción visual: {fmt(prod_visual)}")

    diff_direct_local = apr_direct - local_value
    diff_join_local   = apr_join   - local_value
    diff_join_visual  = apr_join   - prod_visual

    print(f"\n  Diferencias:")
    print(f"    directo vs local:    {fmt(diff_direct_local)}  ({diff_direct_local/local_value*100:+.1f}%)")
    print(f"    JOIN vs local:       {fmt(diff_join_local)}  ({diff_join_local/local_value*100 if local_value else 0:+.1f}%)")
    print(f"    JOIN vs visual:      {fmt(diff_join_visual)}")

    # ──────────────────────────────────────────────────────────────────────────
    # DIAGNÓSTICO FINAL
    # ──────────────────────────────────────────────────────────────────────────
    print()
    print("=" * 70)
    print("DIAGNÓSTICO FINAL")
    print("=" * 70)

    TOL = 0.02  # 2% tolerancia

    def close(a, b):
        if b == 0:
            return a == 0
        return abs(a - b) / abs(b) < TOL

    if not has_tipocli:
        print("RESULTADO: tabla forecast_fact_2026 SIN columna tipocli")
        print("→ La carga fue incompleta o usó una versión vieja del script.")
        print("→ ACCIÓN: cargar desde CSV local con el script seguro.")

    elif close(apr_direct, local_value) and not close(apr_join, local_value):
        print("RESULTADO: forecast_fact_2026 tiene los datos CORRECTOS")
        print(f"  Abr-2026 DRO vía tipocli:   {fmt(apr_direct)}  ≈ local ✓")
        print(f"  Abr-2026 DRO vía JOIN:      {fmt(apr_join)}")
        print()
        print("CAUSA RAÍZ: el JOIN con forecast_valorizado excluye clientes")
        print("  que existen en forecast_fact_2026 (con tipocli=DRO) pero")
        print("  NO están en forecast_valorizado con perfil=DRO.")
        print()
        print("→ NO cargar el CSV — la tabla ya tiene los datos correctos.")
        print("→ ACCIÓN: corregir la query para usar tipocli directo")
        print("  O recargar forecast_valorizado con clientes DRO completos.")

    elif close(apr_direct, prod_visual) and close(apr_join, prod_visual):
        print("RESULTADO: forecast_fact_2026 tiene datos INCOMPLETOS/VIEJOS")
        print(f"  Abr-2026 DRO vía tipocli:   {fmt(apr_direct)}  ≈ producción visual")
        print(f"  Diferencia vs local:         {fmt(apr_direct - local_value)}")
        print()
        print("→ ACCIÓN: cargar el CSV local con el script seguro.")

    elif close(apr_direct, 0) and close(apr_join, 0):
        print("RESULTADO: forecast_fact_2026 NO tiene datos DRO")
        print("  La tabla existe pero no tiene filas DRO para 2026.")
        print("→ ACCIÓN: cargar el CSV local con el script seguro.")

    else:
        print("RESULTADO: situación no encaja en ningún caso predefinido")
        print(f"  directo: {fmt(apr_direct)}")
        print(f"  join:    {fmt(apr_join)}")
        print(f"  local:   {fmt(local_value)}")
        print(f"  visual:  {fmt(prod_visual)}")
        print("→ Compartir esta salida completa para análisis manual.")

    print()
    print("Tablas NO tocadas: forecast_user_overrides, forecast_manual_clients,")
    print("                   forecast_manual_entries, users, app.db")
    print("Este script fue SOLO DE LECTURA. Ningún dato fue modificado.")
    print("=" * 70)


if __name__ == "__main__":
    main()
