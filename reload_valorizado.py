"""
reload_valorizado.py
====================
Repuebla SOLO forecast_valorizado usando el parquet canónico (702K filas, $121.7B).

Diseño streaming / bajo consumo de RAM:
  - Lee el parquet con pyarrow.iter_batches() → nunca carga el archivo completo.
  - Cada batch = 5 000 filas → ~3 MB activos en RAM.
  - Inserta batch a batch y libera memoria entre lotes.
  - No toca ninguna otra tabla (forecast_main, forecast_imp_hist, etc.).

Uso — LOCAL (contra DB externa de Render):
  export DATABASE_URL="postgresql://user:pass@host:port/db"
  python reload_valorizado.py

Uso — Render Shell (DATABASE_URL ya está en el entorno):
  python reload_valorizado.py

Uso — batch size distinto:
  BATCH_SIZE=3000 python reload_valorizado.py

Post-carga verifica automáticamente que SUM(monto_yhat) = $121.742B.
"""

from __future__ import annotations
import gc
import logging
import os
import sys
from pathlib import Path

import pandas as pd
import pyarrow.parquet as pq

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s  %(levelname)-8s %(message)s",
    datefmt="%H:%M:%S",
)
logger = logging.getLogger("reload_valorizado")

# ---------------------------------------------------------------------------
# Paths — funciona tanto local como en el shell de Render (misma estructura)
# ---------------------------------------------------------------------------
BASE_DIR      = Path(__file__).resolve().parent
DATA_DIR      = BASE_DIR / "web_comparativas" / "data" / "forecast_data"
PARQUET_PATH  = DATA_DIR / "fact_forecast_valorizado.parquet"
CLIENTES_FILE = DATA_DIR / "clientes.csv"

# ---------------------------------------------------------------------------
# Config
# ---------------------------------------------------------------------------
DATABASE_URL = os.environ.get("DATABASE_URL", "")
BATCH_SIZE   = int(os.environ.get("BATCH_SIZE", "5000"))

# Valor de referencia para validación final
EXPECTED_TOTAL = 121_742_106_031.0
TOLERANCE_PCT  = 0.5            # ±0.5 % se considera OK

# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _get_engine(url: str):
    """Crea engine SQLAlchemy. Ajusta el scheme si Render usa 'postgres://'."""
    from sqlalchemy import create_engine
    if url.startswith("postgres://"):
        url = url.replace("postgres://", "postgresql://", 1)
    return create_engine(url, pool_pre_ping=True)


def _load_cli_lu() -> tuple[pd.DataFrame | None, set]:
    """Carga lookup clientes → fantasia / nombre_grupo desde CSV local."""
    if not CLIENTES_FILE.exists():
        logger.warning("clientes.csv no encontrado en %s — fantasia/nombre_grupo quedarán vacíos", CLIENTES_FILE)
        return None, set()
    df = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    df["codigo"] = df["codigo"].astype(str).str.strip()
    lu = df[["codigo", "fantasia", "nombre_grupo"]].drop_duplicates("codigo")
    grupo_set = set(df["nombre_grupo"].dropna().unique())
    logger.info("cli_lu cargado: %d clientes", len(lu))
    return lu, grupo_set


def _load_neg_map(engine) -> pd.DataFrame:
    """Obtiene neg/subneg desde forecast_main (ya en PostgreSQL)."""
    from sqlalchemy import text
    try:
        with engine.connect() as conn:
            df = pd.read_sql(
                "SELECT DISTINCT codigo_serie, neg, subneg FROM forecast_main "
                "WHERE neg IS NOT NULL LIMIT 100000",
                conn,
            )
        df = df.drop_duplicates("codigo_serie")
        logger.info("neg_map cargado desde forecast_main: %d series", len(df))
        return df
    except Exception as exc:
        logger.warning("No se pudo cargar neg_map desde forecast_main: %s — se omitirá", exc)
        return pd.DataFrame(columns=["codigo_serie", "neg", "subneg"])


def _enrich_batch(
    df: pd.DataFrame,
    cli_lu: pd.DataFrame | None,
    grupo_set: set,
    neg_map: pd.DataFrame,
) -> pd.DataFrame:
    """Aplica joins de clientes y neg/subneg sobre un batch."""
    df.columns = [c.lower().strip() for c in df.columns]

    # fecha como datetime (el parquet ya la tiene, por si acaso)
    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
    elif "periodo" in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")

    # Join clientes → fantasia / nombre_grupo
    if cli_lu is not None and "cliente_id" in df.columns:
        df["cliente_id"] = df["cliente_id"].astype(str).str.strip()
        df = pd.merge(df, cli_lu, left_on="cliente_id", right_on="codigo", how="left")
        df.drop(columns=["codigo"], inplace=True, errors="ignore")

        mask_nm = df["fantasia"].isna()
        if mask_nm.any():
            is_grp  = df.loc[mask_nm, "cliente_id"].isin(grupo_set)
            idx_grp = mask_nm[mask_nm].index[is_grp.values]
            df.loc[idx_grp, "fantasia"]     = df.loc[idx_grp, "cliente_id"]
            df.loc[idx_grp, "nombre_grupo"] = df.loc[idx_grp, "cliente_id"]
            still = df["fantasia"].isna()
            df.loc[still, "fantasia"]     = df.loc[still, "cliente_id"]
            df.loc[still, "nombre_grupo"] = "SIN GRUPO"

        df["fantasia"]     = df["fantasia"].fillna(df["cliente_id"])
        df["nombre_grupo"] = df["nombre_grupo"].fillna("SIN GRUPO")
    else:
        df.setdefault("fantasia",     pd.NA)
        df.setdefault("nombre_grupo", "SIN GRUPO")

    # Join neg / subneg desde neg_map
    if not neg_map.empty and "codigo_serie" in df.columns:
        df = pd.merge(df, neg_map, on="codigo_serie", how="left", suffixes=("", "_nm"))
        for col in ("neg", "subneg"):
            col_nm = f"{col}_nm"
            if col_nm in df.columns:
                if col not in df.columns:
                    df[col] = df[col_nm]
                else:
                    df[col] = df[col].fillna(df[col_nm])
                df.drop(columns=[col_nm], inplace=True, errors="ignore")
    for col in ("neg", "subneg"):
        df.setdefault(col, pd.NA)

    # descripcion fallback
    if "descripcion" not in df.columns and "codigo_serie" in df.columns:
        df["descripcion"] = df["codigo_serie"].astype(str)

    return df


# ---------------------------------------------------------------------------
# Main
# ---------------------------------------------------------------------------

def run() -> None:
    if not DATABASE_URL:
        logger.error(
            "DATABASE_URL no está configurado.\n"
            "  Local → export DATABASE_URL='postgresql://user:pass@host:port/db'\n"
            "  Render → ya debería estar en el entorno."
        )
        sys.exit(1)

    if not PARQUET_PATH.exists():
        logger.error("Parquet no encontrado: %s", PARQUET_PATH)
        sys.exit(1)

    engine = _get_engine(DATABASE_URL)
    logger.info("Conectado a DB: %s", str(engine.url).split("@")[-1])  # oculta credenciales

    # Lookups (pequeños, caben siempre en RAM)
    cli_lu, grupo_set = _load_cli_lu()
    neg_map           = _load_neg_map(engine)

    # Metadata del parquet
    pf         = pq.ParquetFile(str(PARQUET_PATH))
    total_rows = pf.metadata.num_rows
    n_batches  = -(-total_rows // BATCH_SIZE)   # ceil division
    logger.info(
        "Parquet: %d filas | batch_size=%d | ~%d batches",
        total_rows, BATCH_SIZE, n_batches,
    )

    # ── Streaming insert ──────────────────────────────────────────────────
    from sqlalchemy import text

    inserted   = 0
    first_batch = True

    for batch_arrow in pf.iter_batches(batch_size=BATCH_SIZE):
        df = batch_arrow.to_pandas()
        df = _enrich_batch(df, cli_lu, grupo_set, neg_map)

        # Primera iteración: DROP + CREATE (replace); resto: append
        if_exists = "replace" if first_batch else "append"
        df.to_sql(
            "forecast_valorizado",
            engine,
            if_exists=if_exists,
            index=False,
            chunksize=1000,   # sub-lote para el INSERT de SQLAlchemy
            method="multi",
        )
        inserted  += len(df)
        first_batch = False

        # Log cada ~50 K filas
        if inserted % 50_000 < BATCH_SIZE:
            logger.info(
                "  → %d / %d filas (%.0f%%)",
                inserted, total_rows, inserted / total_rows * 100,
            )

        del df, batch_arrow
        gc.collect()

    logger.info("Carga completa: %d filas insertadas.", inserted)

    # ── Índices ────────────────────────────────────────────────────────────
    logger.info("Creando índices...")
    with engine.begin() as conn:
        for col in ("fecha", "perfil", "codigo_serie", "cliente_id"):
            conn.execute(text(
                f"CREATE INDEX IF NOT EXISTS idx_fc_val_{col} "
                f"ON forecast_valorizado ({col})"
            ))
    logger.info("Índices OK.")

    # ── Validación post-carga ──────────────────────────────────────────────
    logger.info("Validando...")
    with engine.connect() as conn:
        row = pd.read_sql(
            "SELECT COUNT(*) AS cnt, "
            "COALESCE(SUM(monto_yhat), 0) AS total_monto "
            "FROM forecast_valorizado",
            conn,
        ).iloc[0]

    cnt   = int(row["cnt"])
    total = float(row["total_monto"])
    delta = abs(total - EXPECTED_TOTAL) / EXPECTED_TOTAL * 100

    logger.info("━" * 60)
    logger.info("  Filas en forecast_valorizado : %d  (esperadas: ~702 436)", cnt)
    logger.info("  SUM(monto_yhat)              : $%.3fB  (ref: $121.742B)", total / 1e9)
    logger.info("  Desviación vs referencia     : %.3f %%", delta)

    if delta <= TOLERANCE_PCT:
        logger.info("  ✅  KPI 1 CORRECTO — coincide con la referencia buena")
    else:
        logger.warning("  ⚠️  KPI 1 FUERA DE RANGO — revisar datos")

    # KPI rápidos (sin filtros)
    with engine.connect() as conn:
        hist_row = pd.read_sql(
            "SELECT COALESCE(SUM(imp_hist), 0) AS hist_2025 "
            "FROM forecast_imp_hist "
            "WHERE EXTRACT(YEAR FROM fecha) = 2025",
            conn,
        ).iloc[0]
    real_2025 = float(hist_row["hist_2025"])
    if real_2025 > 0:
        INFLATION = ((1 + 2.9 / 100) ** 12 - 1) * 100
        var_nom  = (total / real_2025 - 1) * 100
        var_real = (total / (1 + INFLATION / 100) / real_2025 - 1) * 100
        logger.info("  Total real 2025              : $%.3fB  (ref: $98.029B)", real_2025 / 1e9)
        logger.info("  Variación nominal            : %+.2f %%  (ref: +24.19%%)", var_nom)
        logger.info("  Variación real               : %+.2f %%  (ref: -11.87%%)", var_real)
    logger.info("━" * 60)
    logger.info("Script finalizado correctamente.")


if __name__ == "__main__":
    run()
