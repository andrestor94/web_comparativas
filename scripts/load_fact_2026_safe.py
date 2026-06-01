"""
load_fact_2026_safe.py
======================
Migración segura y auditada de facturacion_real_2026_sin_neg2.csv
hacia la tabla forecast_fact_2026 en PostgreSQL de producción.

Pasos:
  1. Leer y validar el CSV fuente (debe tener DRO Abr-2026 ≈ $2.544B).
  2. Conectar a PostgreSQL (vía DATABASE_URL).
  3. Backup de la tabla actual → forecast_fact_2026_backup_YYYYMMDD_HHMMSS
  4. Cargar CSV a staging → forecast_fact_2026_staging
  5. Comparar staging vs backup (totales por mes/perfil).
  6. Confirmar antes de reemplazar (input interactivo o --auto-confirm).
  7. TRUNCATE forecast_fact_2026 + INSERT desde staging.
  8. Recrear índices.
  9. Verificar tabla final.

Uso:
  # Con confirmación manual (recomendado):
  DATABASE_URL=postgresql://user:pass@host/db python scripts/load_fact_2026_safe.py

  # Sin confirmación (CI/automatizado - usar solo si backup ya validado):
  DATABASE_URL=postgresql://user:pass@host/db python scripts/load_fact_2026_safe.py --auto-confirm

  # Desde Render Shell (DATABASE_URL ya está disponible):
  python scripts/load_fact_2026_safe.py

Seguridad:
  - NO toca tablas de overrides de usuarios.
  - NO toca tablas de clientes manuales.
  - NO modifica forecast_user_overrides ni forecast_manual_entries.
  - Crea backup ANTES de cualquier escritura.
  - Usa staging, no modifica producción hasta validar.
  - Rollback disponible: restaurar desde backup.
"""
from __future__ import annotations

import csv as _csv
import datetime as dt
import gc
import logging
import os
import sys
from pathlib import Path

# ---------------------------------------------------------------------------
# Setup paths
# ---------------------------------------------------------------------------
REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))

import pandas as pd

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
    stream=sys.stdout,
)
logger = logging.getLogger("load_fact_2026")

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
AUTO_CONFIRM = "--auto-confirm" in sys.argv

CSV_PATH = REPO_ROOT / "web_comparativas" / "data" / "forecast_data" / "facturacion_real_2026_sin_neg2.csv"
CLIENTES_PATH = REPO_ROOT / "web_comparativas" / "data" / "forecast_data" / "clientes.csv"

PROD_TABLE     = "forecast_fact_2026"
STAGING_TABLE  = "forecast_fact_2026_staging"
TS             = dt.datetime.utcnow().strftime("%Y%m%d_%H%M%S")
BACKUP_TABLE   = f"forecast_fact_2026_backup_{TS}"

# Validation thresholds
DRO_APR_2026_MIN = 2_000_000_000   # > $2B expected
DRO_APR_2026_MAX = 4_000_000_000   # < $4B expected (sanity upper bound)
MIN_TOTAL_ROWS   = 100_000          # CSV should have 200k+ rows
MIN_DRO_ROWS     = 1_000            # CSV should have 5k+ DRO rows


# ---------------------------------------------------------------------------
# Step 1 — Validate CSV
# ---------------------------------------------------------------------------
def load_and_validate_csv() -> pd.DataFrame:
    logger.info("=== STEP 1: Validating CSV source ===")
    logger.info("CSV path: %s", CSV_PATH)

    if not CSV_PATH.exists():
        raise FileNotFoundError(
            f"CSV not found: {CSV_PATH}\n"
            "Este archivo debe existir en el entorno donde se ejecuta el script."
        )

    size_mb = CSV_PATH.stat().st_size / 1_048_576
    logger.info("CSV size: %.1f MB", size_mb)

    # Detect separator
    with open(str(CSV_PATH), "r", encoding="utf-8-sig", errors="replace") as fh:
        first_line = fh.readline()
        sep = ";" if first_line.count(";") > first_line.count(",") else ","
    logger.info("Detected separator: %r", sep)

    # Robust parser (handles embedded quotes in field values)
    rows: list[list[str]] = []
    with open(str(CSV_PATH), "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = _csv.reader(fh, delimiter=sep)
        header = next(reader)
        n_cols = len(header)
        bad_rows = 0
        for row in reader:
            if len(row) == n_cols:
                rows.append(row)
            elif len(row) == 1 and sep in row[0]:
                reparsed = next(_csv.reader([row[0]], delimiter=sep))
                if len(reparsed) == n_cols:
                    rows.append(reparsed)
                else:
                    bad_rows += 1
            else:
                bad_rows += 1

    df = pd.DataFrame(rows, columns=header)
    df.columns = [c.lower().strip().rstrip(";").rstrip(",") for c in df.columns]
    # Drop unnamed trailing columns
    df = df.loc[:, [c for c in df.columns if c]]

    logger.info("Rows parsed: %d (bad_rows skipped: %d)", len(df), bad_rows)

    # Parse types
    df["fecha"] = pd.to_datetime(df["fecha"], dayfirst=True, errors="coerce")
    df["fecha"] = df["fecha"].dt.to_period("M").dt.to_timestamp()
    df["imp_hist"] = pd.to_numeric(
        df["imp_hist"].astype(str)
            .str.replace(".", "", regex=False)
            .str.replace(",", ".", regex=False),
        errors="coerce",
    ).fillna(0)
    # Keep only 2026+ rows
    mask_2026 = df["fecha"] >= pd.Timestamp("2026-01-01")
    df = df[mask_2026].copy()
    logger.info("Rows after 2026 filter: %d", len(df))

    # Validate
    if len(df) < MIN_TOTAL_ROWS:
        raise ValueError(
            f"Too few rows ({len(df)} < {MIN_TOTAL_ROWS}). CSV may be malformed."
        )

    if "perfil" not in df.columns:
        raise ValueError("CSV missing 'perfil' column.")

    dro_apr = df[(df["perfil"] == "DRO") & (df["fecha"].dt.to_period("M").astype(str) == "2026-04")]["imp_hist"].sum()
    logger.info("DRO Apr-2026 in CSV: $%s", f"{dro_apr:,.0f}")

    if not (DRO_APR_2026_MIN <= dro_apr <= DRO_APR_2026_MAX):
        raise ValueError(
            f"DRO Apr-2026 validation FAILED: got {dro_apr:,.0f}, "
            f"expected between {DRO_APR_2026_MIN:,} and {DRO_APR_2026_MAX:,}."
        )

    dro_rows = (df["perfil"] == "DRO").sum()
    if dro_rows < MIN_DRO_ROWS:
        raise ValueError(f"Too few DRO rows ({dro_rows} < {MIN_DRO_ROWS}).")

    logger.info("CSV validation PASSED. DRO Apr-2026: $%s", f"{dro_apr:,.0f}")
    logger.info("CSV summary by perfil:")
    for perf, grp in df.groupby("perfil", dropna=False):
        logger.info("  %-6s  %7d rows  total=$%s", perf, len(grp), f"{grp['imp_hist'].sum():,.0f}")

    return df


# ---------------------------------------------------------------------------
# Step 2 — Enrich with tipocli from clientes.csv
# ---------------------------------------------------------------------------
def enrich_with_tipocli(df: pd.DataFrame) -> pd.DataFrame:
    logger.info("=== STEP 2: Enriching with tipocli ===")
    if "cliente_id" not in df.columns:
        logger.warning("No cliente_id column — skipping tipocli enrichment.")
        return df
    if not CLIENTES_PATH.exists():
        logger.warning("clientes.csv not found — skipping tipocli enrichment.")
        return df
    try:
        df_cli = pd.read_csv(str(CLIENTES_PATH), encoding="latin-1", low_memory=False)
        df_cli.columns = [c.lower().strip() for c in df_cli.columns]
        if "codigo" not in df_cli.columns or "tipocli" not in df_cli.columns:
            logger.warning("clientes.csv missing 'codigo' or 'tipocli' — skipping.")
            return df
        df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
        df["cliente_id"] = df["cliente_id"].astype(str).str.strip()
        n_before = len(df)
        df = df.merge(
            df_cli[["codigo", "tipocli"]].drop_duplicates("codigo"),
            left_on="cliente_id", right_on="codigo",
            how="left",
        ).drop(columns=["codigo"], errors="ignore")
        logger.info("tipocli enrichment: %d rows (was %d) — matched %d/%d",
                    len(df), n_before,
                    df["tipocli"].notna().sum(), n_before)
        del df_cli
        gc.collect()
    except Exception as exc:
        logger.warning("tipocli enrichment error: %s", exc)
    return df


# ---------------------------------------------------------------------------
# Step 3 — Connect to PostgreSQL
# ---------------------------------------------------------------------------
def get_pg_engine():
    logger.info("=== STEP 3: Connecting to PostgreSQL ===")
    db_url = os.environ.get("DATABASE_URL", "")
    if not db_url:
        # Try loading from local .env
        env_path = REPO_ROOT / "web_comparativas" / ".env"
        if env_path.exists():
            for line in env_path.read_text(encoding="utf-8").splitlines():
                line = line.strip()
                if line.startswith("DATABASE_URL=") and not line.startswith("#"):
                    db_url = line.split("=", 1)[1].strip().strip('"').strip("'")
                    break

    if not db_url or "postgresql" not in db_url:
        raise EnvironmentError(
            "DATABASE_URL not set or not a PostgreSQL URL.\n"
            "Set it before running:\n"
            "  DATABASE_URL=postgresql://user:pass@host/db python scripts/load_fact_2026_safe.py\n"
            "Or run from Render Shell where DATABASE_URL is auto-injected."
        )

    # Mask credentials for logging
    try:
        _parts = db_url.split("@")
        _host_db = _parts[-1]
        logger.info("Connecting to: ...@%s", _host_db)
    except Exception:
        logger.info("Connecting to PostgreSQL...")

    from sqlalchemy import create_engine
    # Render PostgreSQL sometimes needs sslmode=require
    if "?" not in db_url:
        db_url += "?sslmode=require"
    engine = create_engine(db_url, pool_pre_ping=True)
    with engine.connect() as conn:
        from sqlalchemy import text
        result = conn.execute(text("SELECT version()"))
        ver = result.fetchone()[0]
        logger.info("PostgreSQL connected: %s", ver[:80])
    return engine


# ---------------------------------------------------------------------------
# Step 4 — Backup current table
# ---------------------------------------------------------------------------
def backup_current_table(engine) -> dict:
    from sqlalchemy import text
    logger.info("=== STEP 4: Backing up current table ===")
    logger.info("Backup table: %s", BACKUP_TABLE)

    with engine.begin() as conn:
        # Check if prod table exists
        exists = conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            f"WHERE table_name = '{PROD_TABLE}')"
        )).scalar()

        if not exists:
            logger.info("Production table does not exist yet — skipping backup, creating fresh.")
            return {"rows": 0, "exists": False}

        # Create backup
        conn.execute(text(f"CREATE TABLE {BACKUP_TABLE} AS SELECT * FROM {PROD_TABLE}"))
        bk_rows = conn.execute(text(f"SELECT COUNT(*) FROM {BACKUP_TABLE}")).scalar()
        logger.info("Backup created: %s (%d rows)", BACKUP_TABLE, bk_rows)

        # Summary for comparison
        bk_summary = {}
        try:
            bk_df = pd.read_sql(
                f"SELECT perfil, fecha, SUM(imp_hist) AS total FROM {BACKUP_TABLE} "
                "WHERE fecha >= '2026-01-01' GROUP BY perfil, fecha ORDER BY perfil, fecha",
                conn,
            )
            if not bk_df.empty:
                bk_df["fecha"] = pd.to_datetime(bk_df["fecha"])
                for perf, grp in bk_df.groupby("perfil", dropna=False):
                    for _, row in grp.iterrows():
                        key = f"{perf}_{row['fecha'].strftime('%Y-%m')}"
                        bk_summary[key] = float(row["total"])
        except Exception as exc:
            logger.warning("Backup summary query error: %s", exc)

        return {"rows": bk_rows, "exists": True, "summary": bk_summary}


# ---------------------------------------------------------------------------
# Step 5 — Load staging
# ---------------------------------------------------------------------------
def load_staging(engine, df: pd.DataFrame) -> int:
    from sqlalchemy import text
    logger.info("=== STEP 5: Loading staging table ===")
    logger.info("Staging table: %s", STAGING_TABLE)

    with engine.begin() as conn:
        conn.execute(text(f"DROP TABLE IF EXISTS {STAGING_TABLE}"))

    df.to_sql(STAGING_TABLE, engine, if_exists="replace", index=False, chunksize=5000)

    with engine.connect() as conn:
        staging_rows = conn.execute(text(f"SELECT COUNT(*) FROM {STAGING_TABLE}")).scalar()

    logger.info("Staging loaded: %d rows", staging_rows)
    return staging_rows


# ---------------------------------------------------------------------------
# Step 6 — Compare staging vs backup
# ---------------------------------------------------------------------------
def compare_staging_vs_backup(engine, bk_info: dict) -> None:
    from sqlalchemy import text
    logger.info("=== STEP 6: Comparing staging vs backup ===")

    with engine.connect() as conn:
        try:
            st_df = pd.read_sql(
                f"SELECT perfil, fecha, SUM(imp_hist) AS total FROM {STAGING_TABLE} "
                "WHERE fecha >= '2026-01-01' GROUP BY perfil, fecha ORDER BY perfil, fecha",
                conn,
            )
        except Exception as exc:
            logger.warning("Staging summary query failed: %s", exc)
            return

    if st_df.empty:
        logger.warning("Staging summary is empty!")
        return

    st_df["fecha"] = pd.to_datetime(st_df["fecha"])

    logger.info("--- Staging summary (DRO months) ---")
    dro_st = st_df[st_df["perfil"] == "DRO"]
    for _, row in dro_st.iterrows():
        m_key = f"DRO_{row['fecha'].strftime('%Y-%m')}"
        bk_val = (bk_info.get("summary", {}) or {}).get(m_key, 0)
        logger.info(
            "  DRO %s: staging=$%s  backup=$%s  delta=$%s",
            row["fecha"].strftime("%Y-%m"),
            f"{row['total']:,.0f}",
            f"{bk_val:,.0f}",
            f"{row['total'] - bk_val:,.0f}",
        )

    logger.info("--- Staging all perfiles totals ---")
    for perf, grp in st_df.groupby("perfil", dropna=False):
        logger.info("  %-6s  total=$%s", perf, f"{grp['total'].sum():,.0f}")


# ---------------------------------------------------------------------------
# Step 7 — Confirm and swap
# ---------------------------------------------------------------------------
def confirm_and_swap(engine) -> None:
    from sqlalchemy import text
    logger.info("=== STEP 7: Confirming and swapping ===")

    if not AUTO_CONFIRM:
        print("\n" + "="*60)
        print("REVISAR los valores de staging vs backup arriba.")
        print("¿Continuar con el reemplazo de forecast_fact_2026? (s/N): ", end="", flush=True)
        answer = input().strip().lower()
        if answer not in ("s", "si", "yes", "y"):
            logger.info("Operación cancelada por el usuario.")
            logger.info("BACKUP disponible en: %s", BACKUP_TABLE)
            logger.info("STAGING disponible en: %s (puede eliminarse manualmente)", STAGING_TABLE)
            sys.exit(0)

    logger.info("Proceeding with swap...")

    with engine.begin() as conn:
        # Check if prod table exists
        exists = conn.execute(text(
            "SELECT EXISTS (SELECT 1 FROM information_schema.tables "
            f"WHERE table_name = '{PROD_TABLE}')"
        )).scalar()

        if exists:
            conn.execute(text(f"TRUNCATE TABLE {PROD_TABLE}"))
            conn.execute(text(f"INSERT INTO {PROD_TABLE} SELECT * FROM {STAGING_TABLE}"))
            logger.info("TRUNCATE + INSERT completed.")
        else:
            # First time: create from staging
            conn.execute(text(f"CREATE TABLE {PROD_TABLE} AS SELECT * FROM {STAGING_TABLE}"))
            logger.info("CREATE TABLE from staging completed (first time).")

        prod_rows = conn.execute(text(f"SELECT COUNT(*) FROM {PROD_TABLE}")).scalar()
        logger.info("forecast_fact_2026 now has %d rows.", prod_rows)


# ---------------------------------------------------------------------------
# Step 8 — Recreate indexes
# ---------------------------------------------------------------------------
def recreate_indexes(engine) -> None:
    from sqlalchemy import text
    logger.info("=== STEP 8: Recreating indexes ===")
    indexes = [
        ("idx_fc_fact2026_tipocli",      "forecast_fact_2026", "(tipocli)"),
        ("idx_fc_fact2026_cliente",       "forecast_fact_2026", "(cliente_id)"),
        ("idx_fc_fact2026_fecha",         "forecast_fact_2026", "(fecha)"),
        ("idx_fc_fact2026_perfil_fecha",  "forecast_fact_2026", "(perfil, fecha)"),
        ("ix_fc_fact2026_codigo_fecha",   "forecast_fact_2026", "(codigo_serie, fecha)"),
        ("ix_fc_fact2026_cliente_fecha",  "forecast_fact_2026", "(cliente_id, fecha)"),
    ]
    raw = engine.raw_connection()
    raw.set_isolation_level(0)  # AUTOCOMMIT for CONCURRENTLY
    try:
        with raw.cursor() as cur:
            for idx_name, tbl, expr in indexes:
                try:
                    cur.execute(
                        f"CREATE INDEX CONCURRENTLY IF NOT EXISTS {idx_name} ON {tbl} {expr}"
                    )
                    logger.info("Index created: %s", idx_name)
                except Exception as exc:
                    logger.warning("Index %s: %s", idx_name, exc)
    finally:
        raw.close()


# ---------------------------------------------------------------------------
# Step 9 — Final verification
# ---------------------------------------------------------------------------
def final_verification(engine) -> None:
    from sqlalchemy import text
    logger.info("=== STEP 9: Final verification ===")
    with engine.connect() as conn:
        rows = conn.execute(text(f"SELECT COUNT(*) FROM {PROD_TABLE}")).scalar()
        logger.info("forecast_fact_2026 total rows: %d", rows)

        try:
            dro_apr = conn.execute(text(
                f"SELECT SUM(imp_hist) FROM {PROD_TABLE} "
                f"WHERE fecha >= '2026-04-01' AND fecha < '2026-05-01' "
                f"AND tipocli = 'DRO'"
            )).scalar() or 0
            logger.info("DRO Apr-2026 via tipocli filter: $%s", f"{dro_apr:,.0f}")
        except Exception:
            pass

        try:
            dro_apr_v2 = conn.execute(text(
                f"SELECT SUM(ff.imp_hist) FROM {PROD_TABLE} ff "
                f"WHERE ff.fecha >= '2026-04-01' AND ff.fecha < '2026-05-01' "
                f"AND CAST(ff.cliente_id AS TEXT) IN ("
                f"  SELECT DISTINCT CAST(fv.cliente_id AS TEXT) "
                f"  FROM forecast_valorizado fv WHERE fv.perfil = 'DRO'"
                f")"
            )).scalar() or 0
            logger.info("DRO Apr-2026 via forecast_valorizado join: $%s", f"{dro_apr_v2:,.0f}")
        except Exception as exc:
            logger.warning("join verification query failed: %s", exc)

    logger.info("Backup table kept at: %s", BACKUP_TABLE)
    logger.info("Staging table (can be dropped): %s", STAGING_TABLE)


# ---------------------------------------------------------------------------
# Tables NOT touched — safety confirmation
# ---------------------------------------------------------------------------
PROTECTED_TABLES = [
    "forecast_user_overrides",
    "forecast_manual_clients",
    "forecast_manual_entries",
    "users",
]


def print_safety_summary() -> None:
    logger.info("=== SAFETY SUMMARY ===")
    logger.info("Tables MODIFIED: %s", PROD_TABLE)
    logger.info("Tables BACKED UP: %s", BACKUP_TABLE)
    logger.info("Tables NOT TOUCHED: %s", ", ".join(PROTECTED_TABLES))
    logger.info("No user overrides, manual clients, or manual entries were modified.")


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------
def main() -> None:
    logger.info("load_fact_2026_safe.py starting — %s", dt.datetime.utcnow().isoformat())
    logger.info("AUTO_CONFIRM=%s", AUTO_CONFIRM)

    # 1. Validate CSV
    df = load_and_validate_csv()

    # 2. Enrich
    df = enrich_with_tipocli(df)

    # 3. Connect
    engine = get_pg_engine()

    # 4. Backup
    bk_info = backup_current_table(engine)

    # 5. Load staging
    staging_rows = load_staging(engine, df)

    # 6. Compare
    compare_staging_vs_backup(engine, bk_info)

    # 7. Confirm + swap
    confirm_and_swap(engine)

    # 8. Indexes
    recreate_indexes(engine)

    # 9. Verify
    final_verification(engine)

    # Safety summary
    print_safety_summary()

    logger.info("=== DONE ===")
    logger.info("To rollback: TRUNCATE %s; INSERT INTO %s SELECT * FROM %s;",
                PROD_TABLE, PROD_TABLE, BACKUP_TABLE)


if __name__ == "__main__":
    main()
