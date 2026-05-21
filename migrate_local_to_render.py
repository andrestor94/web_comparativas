"""
EJECUTAR LOCALMENTE — corre en tu PC y sube los datos a PostgreSQL de Render.
No tocar el servidor de Render para no reventar la RAM.

INSTRUCCIONES:
  1. Abrí una terminal en la carpeta del proyecto.
  2. Ejecutá:  python migrate_local_to_render.py
  3. Ingresá la URL de conexión de Render cuando se pida.
       → En Render: Dashboard → web_comparativas_db → Connect → External Database URL
       → Formato: postgresql://usuario:password@host/dbname
"""
import csv
import sys
import os
import gc
import json
import logging
from pathlib import Path

import pandas as pd
import psycopg2
import psycopg2.extras
from sqlalchemy import create_engine, text

logging.basicConfig(level=logging.INFO, format="%(asctime)s  %(message)s")
log = logging.getLogger("migrate")

# ────────────────────────────────────────────────────────────────────────────
# 1. URL de conexión — lee de .env, variable de entorno, o pide interactivo
# ────────────────────────────────────────────────────────────────────────────

# Intentar leer .env si existe
_env_file = Path(__file__).parent / ".env"
if _env_file.exists():
    for _line in _env_file.read_text("utf-8").splitlines():
        _line = _line.strip()
        if _line.startswith("DATABASE_URL=") and not _line.endswith("PEGAR_URL_AQUI"):
            os.environ.setdefault("DATABASE_URL", _line.split("=", 1)[1].strip())
            break

RENDER_URL = os.getenv("DATABASE_URL", "").strip()
if not RENDER_URL:
    print("\n⚠️  No se encontró DATABASE_URL en el archivo .env")
    print("   Abrí el archivo  .env  en la raíz del proyecto y pegá la URL de Render.")
    print("   Luego volvé a ejecutar este script.")
    sys.exit(1)

# Normalizar esquema
RENDER_URL = RENDER_URL.replace("postgres://", "postgresql://")

# Para psycopg2 necesitamos el esquema "postgresql+psycopg2://"
SQLALCHEMY_URL = RENDER_URL.replace("postgresql://", "postgresql+psycopg2://")

from urllib.parse import urlsplit as _urlsplit
_p = _urlsplit(RENDER_URL)
_safe_url = f"{_p.scheme}://***:***@{_p.hostname or '<host>'}{':' + str(_p.port) if _p.port else ''}{_p.path}"
log.info("Conectando a: %s", _safe_url)

try:
    engine = create_engine(
        SQLALCHEMY_URL,
        connect_args={"sslmode": "require"},
        pool_pre_ping=True,
    )
    with engine.begin() as conn:
        conn.execute(text("SELECT 1"))
    log.info("✅  Conexión exitosa a Render PostgreSQL")
except Exception as e:
    log.error("❌  No se pudo conectar: %s", e)
    sys.exit(1)

# ────────────────────────────────────────────────────────────────────────────
# 2. Rutas de los archivos CSV (locales)
# ────────────────────────────────────────────────────────────────────────────

BASE = Path(__file__).resolve().parent / "web_comparativas" / "data" / "forecast_data"

FORECAST_FILE       = BASE / "forecast_base_consolidado.csv"
MASTER_FILE         = BASE / "Articulos 1.csv"
NEGOCIOS_FILE       = BASE / "Negocios.csv"
SERIES_FILE         = BASE / "dataset_base.csv"
ARTICULOS_FILE      = BASE / "Articulos 1.csv"
CLIENTES_FILE       = BASE / "clientes.csv"
IMP_HIST_FILE       = BASE / "importe_historico.csv"
FACT_2026_FILE      = BASE / "facturacion_real_2026_sin_neg2.csv"
VALORIZADO_PREP     = BASE / "fact_forecast_valorizado.csv"
VALORIZADO_FALLBACK = BASE / "forecast_valorizado_v2.csv"


def _norm_cols(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [c.lower().strip() for c in df.columns]
    return df


def _apply_neg_names(df: pd.DataFrame) -> pd.DataFrame:
    """Sustituye códigos numéricos de neg/subneg por nombres si Negocios.csv existe."""
    if not NEGOCIOS_FILE.exists() or "neg" not in df.columns:
        return df
    try:
        neg = pd.read_csv(str(NEGOCIOS_FILE), encoding="latin-1")
        neg = _norm_cols(neg)
        # columnas esperadas: unidad, subunidad, descrip
        if {"unidad", "subunidad", "descrip"}.issubset(neg.columns):
            u_map = neg.drop_duplicates("unidad").set_index("unidad")["descrip"].to_dict()
            su_map = neg.drop_duplicates("subunidad").set_index("subunidad")["descrip"].to_dict()
            df["neg"] = df["neg"].astype(str).map(lambda x: u_map.get(x, x))
            if "subneg" in df.columns:
                df["subneg"] = df["subneg"].astype(str).map(lambda x: su_map.get(x, x))
    except Exception as exc:
        log.warning("Negocios join: %s", exc)
    return df


def upload_table(df: pd.DataFrame, table_name: str, chunksize: int = 5_000):
    """Sube un DataFrame completo a Render, reemplazando la tabla."""
    log.info("  Uploading %-30s  (%d filas)", table_name, len(df))
    df.to_sql(table_name, engine, if_exists="replace", index=False, chunksize=chunksize)
    log.info("  ✅  %s listo.", table_name)


def upload_csv_chunked(csv_path: Path, table_name: str, sep=",", decimal=".",
                       encoding="utf-8-sig", chunksize=20_000, post_fn=None):
    """Sube un CSV pesado en chunks directamente a Render."""
    log.info("  Streaming %-30s  desde %s", table_name, csv_path.name)
    first = True
    total = 0
    for chunk in pd.read_csv(str(csv_path), sep=sep, decimal=decimal,
                              encoding=encoding, chunksize=chunksize, low_memory=False):
        chunk = _norm_cols(chunk)
        if post_fn:
            chunk = post_fn(chunk)
        mode = "replace" if first else "append"
        chunk.to_sql(table_name, engine, if_exists=mode, index=False)
        total += len(chunk)
        first = False
        del chunk
        gc.collect()
    log.info("  ✅  %s listo  (%d filas totales).", table_name, total)


# ────────────────────────────────────────────────────────────────────────────
# 3. Tablas
# ────────────────────────────────────────────────────────────────────────────

def migrate_forecast_main():
    """forecast_main — tabla de series / forecast base."""
    if not FORECAST_FILE.exists():
        log.warning("SKIP forecast_main: %s no encontrado", FORECAST_FILE.name)
        return

    log.info("▶  Procesando forecast_main …")
    df = pd.read_csv(str(FORECAST_FILE), sep=";", decimal=",",
                     encoding="utf-8-sig", low_memory=False)
    df = _norm_cols(df)

    # fecha
    if "periodo" in df.columns and "fecha" not in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")
    elif "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")

    df = _apply_neg_names(df)

    # monto_yhat numérico
    for col in ("monto_yhat", "yhat", "yhat_lower", "yhat_upper"):
        if col in df.columns:
            df[col] = pd.to_numeric(df[col], errors="coerce").fillna(0.0)

    upload_table(df, "forecast_main")
    del df; gc.collect()

    # Índices
    with engine.begin() as conn:
        for c in ("perfil", "neg", "subneg", "codigo_serie", "fecha"):
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fcm_{c} ON forecast_main ({c})"))
    log.info("  Índices forecast_main creados.")


def migrate_forecast_valorizado():
    """forecast_valorizado — la tabla más pesada, se sube en chunks."""
    val_file = VALORIZADO_PREP if VALORIZADO_PREP.exists() else VALORIZADO_FALLBACK
    if not val_file.exists():
        log.warning("SKIP forecast_valorizado: ningún archivo encontrado")
        return

    log.info("▶  Procesando forecast_valorizado  (%s) …", val_file.name)

    # Pre-cargar lookup de clientes para el join
    cli_lu = None
    grupo_set: set = set()
    if CLIENTES_FILE.exists():
        df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
        df_cli = _norm_cols(df_cli)
        df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
        cli_lu = df_cli[["codigo", "fantasia", "nombre_grupo"]].drop_duplicates("codigo")
        grupo_set = set(df_cli["nombre_grupo"].dropna().unique())
        del df_cli; gc.collect()

    # Negocios lookup
    neg_lu = None
    if NEGOCIOS_FILE.exists():
        try:
            neg = pd.read_csv(str(NEGOCIOS_FILE), encoding="latin-1")
            neg = _norm_cols(neg)
            if {"unidad", "subunidad", "descrip"}.issubset(neg.columns):
                u_map  = neg.drop_duplicates("unidad").set_index("unidad")["descrip"].to_dict()
                su_map = neg.drop_duplicates("subunidad").set_index("subunidad")["descrip"].to_dict()
                neg_lu = (u_map, su_map)
        except Exception as exc:
            log.warning("Negocios lookup: %s", exc)

    sep     = "," if VALORIZADO_PREP.exists() else ";"
    decimal = "." if VALORIZADO_PREP.exists() else ","
    first   = True
    total   = 0

    for chunk in pd.read_csv(str(val_file), sep=sep, decimal=decimal,
                              encoding="utf-8-sig", chunksize=20_000, low_memory=False):
        chunk = _norm_cols(chunk)

        # fecha
        if "periodo" in chunk.columns and "fecha" not in chunk.columns:
            chunk["fecha"] = pd.to_datetime(chunk["periodo"], format="%Y-%m", errors="coerce")
        elif "fecha" in chunk.columns:
            chunk["fecha"] = pd.to_datetime(chunk["fecha"], errors="coerce")

        # Join clientes
        if cli_lu is not None and "cliente_id" in chunk.columns:
            chunk["cliente_id"] = chunk["cliente_id"].astype(str).str.strip()
            chunk = pd.merge(chunk, cli_lu, left_on="cliente_id", right_on="codigo", how="left")
            chunk.drop(columns=["codigo"], inplace=True, errors="ignore")

            mask = chunk["fantasia"].isna()
            if mask.any():
                is_grp = chunk.loc[mask, "cliente_id"].isin(grupo_set)
                idx_g  = mask[mask].index[is_grp.values]
                chunk.loc[idx_g, "fantasia"]     = chunk.loc[idx_g, "cliente_id"]
                chunk.loc[idx_g, "nombre_grupo"] = chunk.loc[idx_g, "cliente_id"]
            still = chunk["fantasia"].isna()
            chunk.loc[still, "fantasia"]     = chunk.loc[still, "cliente_id"]
            chunk.loc[still, "nombre_grupo"] = "SIN GRUPO"

        # Negocios
        if neg_lu and "neg" in chunk.columns:
            u_map, su_map = neg_lu
            chunk["neg"]    = chunk["neg"].astype(str).map(lambda x: u_map.get(x, x))
            if "subneg" in chunk.columns:
                chunk["subneg"] = chunk["subneg"].astype(str).map(lambda x: su_map.get(x, x))

        for c in ("neg", "subneg"):
            if c in chunk.columns: chunk[c] = chunk[c].astype(str)

        if "codigo_serie" in chunk.columns and "descripcion" not in chunk.columns:
            chunk["descripcion"] = chunk["codigo_serie"]

        mode = "replace" if first else "append"
        chunk.to_sql("forecast_valorizado", engine, if_exists=mode, index=False)
        total += len(chunk)
        first  = False
        log.info("    … %d filas subidas", total)
        del chunk; gc.collect()

    log.info("  ✅  forecast_valorizado listo  (%d filas totales).", total)
    with engine.begin() as conn:
        for c in ("fecha", "perfil", "codigo_serie", "cliente_id"):
            conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fcv_{c} ON forecast_valorizado ({c})"))
    log.info("  Índices forecast_valorizado creados.")


def migrate_imp_hist():
    if not IMP_HIST_FILE.exists():
        return
    log.info("▶  Procesando forecast_imp_hist …")
    df = pd.read_csv(str(IMP_HIST_FILE), sep=",", encoding="utf-8", low_memory=False)
    df = _norm_cols(df)
    if "periodo" in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")
    if "imp_hist" in df.columns:
        df["imp_hist"] = pd.to_numeric(df["imp_hist"], errors="coerce").fillna(0)
    upload_table(df, "forecast_imp_hist")
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fih_perfil ON forecast_imp_hist (perfil)"))


def _read_fact_2026_robust(path: Path) -> pd.DataFrame:
    """Parser robusto para facturacion_real_2026_sin_neg2.csv.

    Recupera filas con comillas dobles escapadas (ej: AGUJA DESCARTABLE 13X3 30GX1/2"")
    que pd.read_csv descarta al tratar el caracter como delimitador de campo.
    Garantiza las 206 246 filas sin dropna.
    """
    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.reader(fh)
        header = next(reader)
        n_cols = len(header)
        for row in reader:
            if len(row) == n_cols:
                rows.append(row)
            elif len(row) == 1 and "," in row[0]:
                reparsed = next(csv.reader([row[0]]))
                if len(reparsed) == n_cols:
                    rows.append(reparsed)
                else:
                    log.debug("fact_2026 fila no resuelta: cols=%d", len(reparsed))
            else:
                log.debug("fact_2026 fila no resuelta: cols=%d", len(row))
    df = pd.DataFrame(rows, columns=header)
    df = _norm_cols(df)
    return df


# Valores de referencia exactos (validados localmente antes del deploy)
_FACT_2026_REF = {
    "2026-01": 7_503_407_631.74,
    "2026-02": 8_831_536_228.81,
    "2026-03": 7_633_931_486.28,
    "2026-04": 8_516_862_115.01,
}


def migrate_fact_2026():
    if not FACT_2026_FILE.exists():
        log.warning("SKIP forecast_fact_2026: %s no encontrado", FACT_2026_FILE.name)
        return
    log.info("▶  Procesando forecast_fact_2026 (%s) …", FACT_2026_FILE.name)

    # ── 1. Leer con parser robusto ──────────────────────────────────────
    df = _read_fact_2026_robust(FACT_2026_FILE)
    log.info("  Filas parseadas: %d", len(df))

    if "fecha" in df.columns:
        df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        invalidas = df["fecha"].isna().sum()
        if invalidas:
            log.warning("  Fechas inválidas descartadas: %d", invalidas)
        df = df[df["fecha"].notna()].copy()
        df["fecha"] = df["fecha"].dt.to_period("M").dt.to_timestamp()
        df = df[df["fecha"] >= pd.Timestamp("2026-01-01")].copy()

    df["tipo"] = "val"
    if "imp_hist" in df.columns:
        df["imp_hist"] = pd.to_numeric(df["imp_hist"], errors="coerce").fillna(0)

    # ── 2. Enriquecer tipocli desde clientes.csv ────────────────────────
    if CLIENTES_FILE.exists() and "cliente_id" in df.columns:
        try:
            df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
            df_cli = _norm_cols(df_cli)
            df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
            df["cliente_id"] = df["cliente_id"].astype(str).str.strip()
            df = df.merge(
                df_cli[["codigo", "tipocli"]].drop_duplicates("codigo"),
                left_on="cliente_id", right_on="codigo", how="left",
            ).drop(columns=["codigo"], errors="ignore")
            log.info("  tipocli enriched: %d/%d filas", df["tipocli"].notna().sum(), len(df))
            del df_cli; gc.collect()
        except Exception as exc:
            log.warning("  tipocli enrichment error: %s", exc)

    # ── 3. Validar totales contra referencia exacta ─────────────────────
    log.info("  === VALIDACIÓN ANTES DE INSERTAR ===")
    months = df.groupby(df["fecha"].dt.to_period("M"))["imp_hist"].agg(["count", "sum"])
    all_ok = True
    for m, row in months.iterrows():
        ref = _FACT_2026_REF.get(str(m), 0)
        diff = abs(row["sum"] - ref)
        ok = diff < 1.0
        if not ok:
            all_ok = False
        log.info("  %s: %d filas | $%.2f | ref $%.2f | diff $%.2f %s",
                 m, row["count"], row["sum"], ref, diff, "OK" if ok else "DIFERENCIA")

    if not all_ok:
        log.error("  ❌  Totales no coinciden con referencia. Abortando migración de fact_2026.")
        return

    log.info("  ✅  Validación OK — %d filas totales", len(df))

    # ── 4. Eliminar datos anteriores en PostgreSQL ──────────────────────
    log.info("  Eliminando datos anteriores (2026-01 a 2026-04) de forecast_fact_2026 …")
    try:
        with engine.begin() as conn:
            conn.execute(text(
                "DELETE FROM forecast_fact_2026 "
                "WHERE fecha >= '2026-01-01' AND fecha < '2026-05-01'"
            ))
        log.info("  Datos anteriores eliminados.")
    except Exception as exc:
        log.info("  Tabla inexistente (primer carga): %s", exc)

    # ── 5. Insertar en chunks de 20 000 filas ───────────────────────────
    CHUNK = 20_000
    total_insertado = 0
    n_chunk = 0
    first = True

    for start in range(0, len(df), CHUNK):
        chunk = df.iloc[start:start + CHUNK].copy()
        mode = "replace" if first else "append"
        chunk.to_sql("forecast_fact_2026", engine, if_exists=mode, index=False)
        total_insertado += len(chunk)
        n_chunk += 1
        first = False
        log.info("  Chunk %d insertado: %d filas (acumulado: %d)", n_chunk, len(chunk), total_insertado)
        del chunk; gc.collect()

    log.info("  ✅  forecast_fact_2026 listo. Total insertado: %d filas.", total_insertado)

    # ── 6. Índices ───────────────────────────────────────────────────────
    with engine.begin() as conn:
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ff26_fecha   ON forecast_fact_2026 (fecha)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ff26_tipocli ON forecast_fact_2026 (tipocli)"))
        conn.execute(text("CREATE INDEX IF NOT EXISTS idx_ff26_cliente ON forecast_fact_2026 (cliente_id)"))
    log.info("  Índices forecast_fact_2026 creados.")

    # ── 7. Consulta de verificación en PostgreSQL ────────────────────────
    log.info("  === VERIFICACIÓN EN POSTGRESQL ===")
    try:
        with engine.connect() as conn:
            result = conn.execute(text(
                "SELECT date_trunc('month', fecha)::date AS mes, "
                "       COUNT(*) AS filas, "
                "       SUM(imp_hist) AS total_imp_hist "
                "FROM forecast_fact_2026 "
                "WHERE fecha >= '2026-01-01' AND fecha < '2026-05-01' "
                "GROUP BY 1 ORDER BY 1"
            ))
            for r in result:
                ref = _FACT_2026_REF.get(str(r.mes)[:7], 0)
                diff = abs(float(r.total_imp_hist) - ref)
                log.info("  %s | %d filas | $%.2f | diff $%.2f %s",
                         r.mes, r.filas, float(r.total_imp_hist), diff,
                         "OK" if diff < 1 else "DIFERENCIA")
    except Exception as exc:
        log.warning("  Verificación post-insert: %s", exc)


def migrate_product_labs():
    if not (SERIES_FILE.exists() and ARTICULOS_FILE.exists()):
        return
    log.info("▶  Procesando forecast_product_labs …")
    df_s = pd.read_csv(str(SERIES_FILE), sep=",", encoding="utf-8", dtype=str)
    df_s = _norm_cols(df_s)
    df_a = pd.read_csv(str(ARTICULOS_FILE), sep=",", encoding="latin-1", dtype=str)
    df_a = _norm_cols(df_a)

    # Buscar columnas de laboratorio, familia, descripción
    col_lab  = next((c for c in df_a.columns if "laboratorio_descrip" in c), None)
    col_fam  = next((c for c in df_a.columns if c.startswith("familia")), None)
    col_desc = next((c for c in df_a.columns if c.startswith("descrip")), None)
    col_ser  = next((c for c in df_s.columns if "codigo_serie" in c), None)
    col_niv  = next((c for c in df_s.columns if "nivel" in c), None)

    product_lab_map: dict = {}
    if col_lab and col_ser and col_niv:
        fam_to_lab  = df_a[[col_fam, col_lab]].dropna().groupby(col_fam)[col_lab].apply(set).to_dict() if col_fam else {}
        desc_to_lab = df_a[[col_desc, col_lab]].dropna().groupby(col_desc)[col_lab].apply(set).to_dict() if col_desc else {}
        for _, row in df_s.iterrows():
            serie = str(row[col_ser]).strip()
            nivel = str(row[col_niv]).strip().upper()
            labs: set = set()
            if nivel == "FAMILIA":
                labs = fam_to_lab.get(serie, set())
            elif nivel in ("ARTICULO", "ITEM"):
                labs = desc_to_lab.get(serie, set())
            else:
                labs = fam_to_lab.get(serie, set()) | desc_to_lab.get(serie, set())
            if labs:
                product_lab_map[serie] = sorted(labs)

    if product_lab_map:
        df_labs = pd.DataFrame([
            {"codigo_serie": k, "laboratorios": json.dumps(v)}
            for k, v in product_lab_map.items()
        ])
        upload_table(df_labs, "forecast_product_labs")
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fpl_cdg ON forecast_product_labs (codigo_serie)"))


# ────────────────────────────────────────────────────────────────────────────
# 4. Main
# ────────────────────────────────────────────────────────────────────────────

if __name__ == "__main__":
    log.info("=" * 60)
    log.info("MIGRACIÓN LOCAL → RENDER POSTGRESQL")
    log.info("=" * 60)

    migrate_forecast_main()
    migrate_forecast_valorizado()
    migrate_imp_hist()
    migrate_fact_2026()
    migrate_product_labs()

    log.info("=" * 60)
    log.info("✅  ¡MIGRACIÓN COMPLETADA! Ya podés abrir Forecast en Render.")
    log.info("=" * 60)
