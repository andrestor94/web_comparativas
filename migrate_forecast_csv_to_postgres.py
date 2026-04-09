import os
import sys
import logging
from pathlib import Path
import pandas as pd
import json
import gc

sys.path.insert(0, str(Path(__file__).resolve().parent))

from sqlalchemy import engine_from_config
from web_comparativas.models import engine
from sqlalchemy import text
from web_comparativas.forecast_service import (
    _apply_neg_names, _get_col_ci, FORECAST_FILE, VALORIZADO_FILE, IMP_HIST_FILE,
    FACT_2026_FILE, CLIENTES_FILE, NEGOCIOS_FILE, SERIES_FILE, ARTICULOS_FILE,
    _VALORIZADO_PREPARED, _VALORIZADO_PARQUET,
    _build_price_lookup, _apply_prices, _process_dataframe, MASTER_FILE
)

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_forecast")

def run_migration():
    if not engine:
        logger.error("DB Engine is missing.")
        return
        
    logger.info("Executing RAM-optimized ingestion...")

    # 1. df_main
    df_main = None
    logger.info("Loading df_main...")
    if FORECAST_FILE.exists() and MASTER_FILE.exists():
        df_f = pd.read_csv(str(FORECAST_FILE), sep=";", decimal=",", dtype=str)
        df_m = pd.read_csv(str(MASTER_FILE), sep=",", encoding="latin-1", dtype=str)
        df_main = _process_dataframe(df_f, df_m)
        df_main = _apply_neg_names(df_main, NEGOCIOS_FILE)
        price_lookup = _build_price_lookup(ARTICULOS_FILE)
        df_main = _apply_prices(df_main, price_lookup)
        
        df_main.columns = [str(c).lower().strip() for c in df_main.columns]
        logger.info(f"Uploading forecast_main ({len(df_main)} rows)...")
        df_main.to_sql("forecast_main", engine, if_exists="replace", index=False, chunksize=5000)
        
        with engine.begin() as conn:
            for idx_col in ["perfil", "neg", "subneg", "codigo_serie"]:
                if idx_col in df_main.columns:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fc_main_{idx_col} ON forecast_main ({idx_col})"))

    # Mapping references for chunking
    neg_map = None
    if df_main is not None and "codigo_serie" in df_main.columns:
        join_cols = [c for c in ("neg", "subneg", "descripcion") if c in df_main.columns]
        if join_cols:
            neg_map = df_main[["codigo_serie"] + join_cols].drop_duplicates("codigo_serie")
            
    # Clear df_main from memory to survive Render limits!
    del df_main
    gc.collect()

    # 2. df_valorizado
    # Priority: canonical parquet (9MB, 702K rows, $121.7B, includes monto_li/monto_ls)
    # Fallback: legacy CSV (only used if parquet absent — incomplete, do not use in production)
    _use_parquet = _VALORIZADO_PARQUET.exists()
    _val_file = None if _use_parquet else (_VALORIZADO_PREPARED if _VALORIZADO_PREPARED.exists() else VALORIZADO_FILE)
    _val_available = _use_parquet or (_val_file is not None and _val_file.exists())

    if _val_available:
        logger.info("Uploading forecast_valorizado via %s...", "parquet" if _use_parquet else f"CSV {_val_file}")
        cli_lu = None
        grupo_set = set()
        if CLIENTES_FILE.exists():
            df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
            df_cli.columns = [c.lower().strip() for c in df_cli.columns]
            df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
            cli_lu = df_cli[["codigo", "fantasia", "nombre_grupo"]].drop_duplicates("codigo")
            grupo_set = set(df_cli["nombre_grupo"].dropna().unique())
            del df_cli
            gc.collect()

        first_chunk = True

        def _valorizado_chunks():
            """Yield chunks of the valorizado data regardless of source format."""
            if _use_parquet:
                df_full = pd.read_parquet(str(_VALORIZADO_PARQUET))
                chunk_size = 25000
                for start in range(0, len(df_full), chunk_size):
                    yield df_full.iloc[start:start + chunk_size].copy()
                del df_full
                gc.collect()
            else:
                sep = "," if (_val_file == _VALORIZADO_PREPARED) else ";"
                dec = "." if (_val_file == _VALORIZADO_PREPARED) else ","
                yield from pd.read_csv(str(_val_file), sep=sep, decimal=dec,
                                       encoding="utf-8-sig", chunksize=25000, low_memory=False)

        try:
            for chunk in _valorizado_chunks():
                chunk.columns = [c.lower().strip() for c in chunk.columns]
                if "periodo" in chunk.columns and "fecha" not in chunk.columns:
                    chunk["fecha"] = pd.to_datetime(chunk["periodo"], format="%Y-%m", errors="coerce")
                elif "fecha" in chunk.columns:
                    chunk["fecha"] = pd.to_datetime(chunk["fecha"], errors="coerce")

                if cli_lu is not None:
                    chunk["cliente_id"] = chunk["cliente_id"].astype(str).str.strip()
                    chunk = pd.merge(chunk, cli_lu, left_on="cliente_id", right_on="codigo", how="left")
                    chunk.drop(columns=["codigo"], inplace=True, errors="ignore")
                    
                    mask_nm = chunk["fantasia"].isna()
                    if mask_nm.any():
                        is_grp = chunk.loc[mask_nm, "cliente_id"].isin(grupo_set)
                        idx_g = mask_nm[mask_nm].index[is_grp.values]
                        chunk.loc[idx_g, "fantasia"] = chunk.loc[idx_g, "cliente_id"]
                        chunk.loc[idx_g, "nombre_grupo"] = chunk.loc[idx_g, "cliente_id"]
                        still = chunk["fantasia"].isna()
                        chunk.loc[still, "fantasia"] = chunk.loc[still, "cliente_id"]
                        chunk.loc[still, "nombre_grupo"] = "SIN GRUPO"
                        
                    chunk["fantasia"] = chunk["fantasia"].fillna(chunk["cliente_id"])
                    chunk["nombre_grupo"] = chunk["nombre_grupo"].fillna("SIN GRUPO")

                chunk = _apply_neg_names(chunk, NEGOCIOS_FILE)
                for c in ("neg", "subneg"):
                    if c in chunk.columns: chunk[c] = chunk[c].astype(str)
                if "codigo_serie" in chunk.columns and "descripcion" not in chunk.columns:
                    chunk["descripcion"] = chunk["codigo_serie"]
                    
                if neg_map is not None and "codigo_serie" in chunk.columns:
                    chunk = pd.merge(chunk, neg_map, on="codigo_serie", how="left", suffixes=("", "_drop"))
                    for c in ["neg", "subneg", "descripcion"]:
                        if f"{c}_drop" in chunk.columns:
                            chunk[c] = chunk[c].fillna(chunk[f"{c}_drop"])
                            chunk.drop(columns=[f"{c}_drop"], inplace=True, errors="ignore")
                
                mode = "replace" if first_chunk else "append"
                chunk.to_sql("forecast_valorizado", engine, if_exists=mode, index=False)
                first_chunk = False
                
                del chunk
                gc.collect()
        except Exception as csv_err:
            logger.error("Error reading chunk: %s", csv_err)

        logger.info("forecast_valorizado upload complete.")
        with engine.begin() as conn:
            for idx_col in ["fecha", "perfil", "codigo_serie", "cliente_id"]:
                conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fc_val_{idx_col} ON forecast_valorizado ({idx_col})"))
        
    del cli_lu
    del neg_map
    gc.collect()

    # 3. df_imp_hist
    if IMP_HIST_FILE.exists():
        df_imp_hist = pd.read_csv(str(IMP_HIST_FILE), sep=",", encoding="utf-8")
        df_imp_hist.columns = [c.lower().strip() for c in df_imp_hist.columns]
        df_imp_hist["tipo"] = "hist"
        if "periodo" in df_imp_hist.columns:
            df_imp_hist["fecha"] = pd.to_datetime(df_imp_hist["periodo"], format="%Y-%m", errors="coerce")
        if "imp_hist" in df_imp_hist.columns:
            df_imp_hist["imp_hist"] = pd.to_numeric(df_imp_hist["imp_hist"], errors="coerce").fillna(0)
        df_imp_hist.to_sql("forecast_imp_hist", engine, if_exists="replace", index=False)
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fc_hist_perfil ON forecast_imp_hist (perfil)"))
            
        del df_imp_hist
        gc.collect()

    # 4. df_fact_2026
    if FACT_2026_FILE.exists():
        df_fact_2026 = pd.read_csv(str(FACT_2026_FILE), sep=",", encoding="utf-8")
        df_fact_2026.columns = [c.lower().strip() for c in df_fact_2026.columns]
        df_fact_2026["tipo"] = "val"
        if "fecha" in df_fact_2026.columns:
            df_fact_2026["fecha"] = pd.to_datetime(df_fact_2026["fecha"], errors="coerce")
            df_fact_2026["fecha"] = df_fact_2026["fecha"].dt.to_period("M").dt.to_timestamp()
            mask_2026 = df_fact_2026["fecha"] >= "2026-01-01"
            df_fact_2026 = df_fact_2026[mask_2026].copy()
        if "imp_hist" in df_fact_2026.columns:
            df_fact_2026["imp_hist"] = pd.to_numeric(df_fact_2026["imp_hist"], errors="coerce").fillna(0)
        df_fact_2026.to_sql("forecast_fact_2026", engine, if_exists="replace", index=False)
        del df_fact_2026
        gc.collect()

    # 5. product_lab_map
    if SERIES_FILE.exists() and ARTICULOS_FILE.exists():
        df_s = pd.read_csv(str(SERIES_FILE), sep=",", encoding="utf-8", dtype=str)
        df_s.columns = [c.strip() for c in df_s.columns]
        df_a = pd.read_csv(str(ARTICULOS_FILE), sep=",", encoding="latin-1", dtype=str)
        df_a.columns = [c.strip() for c in df_a.columns]
        col_lab = _get_col_ci(df_a, "laboratorio_descrip")
        col_fam_a = _get_col_ci(df_a, "familia")
        col_desc_a = _get_col_ci(df_a, "descrip")
        product_lab_map = {}
        if col_lab:
            fam_to_lab = df_a[[col_fam_a, col_lab]].dropna().groupby(col_fam_a)[col_lab].apply(set).to_dict() if col_fam_a else {}
            desc_to_lab = df_a[[col_desc_a, col_lab]].dropna().groupby(col_desc_a)[col_lab].apply(set).to_dict() if col_desc_a else {}
            col_serie = _get_col_ci(df_s, "codigo_serie")
            col_nivel = _get_col_ci(df_s, "nivel_agregacion")
            if col_serie and col_nivel:
                for _, row in df_s.iterrows():
                    serie = str(row[col_serie]).strip()
                    nivel = str(row[col_nivel]).strip().upper()
                    labs = set()
                    if nivel == "FAMILIA": labs = fam_to_lab.get(serie, set())
                    elif nivel in ("ARTICULO", "ITEM"): labs = desc_to_lab.get(serie, set())
                    else: labs = fam_to_lab.get(serie, set()) | desc_to_lab.get(serie, set())
                    if labs: product_lab_map[serie] = sorted(list(labs))

        if product_lab_map:
            df_labs = pd.DataFrame([{"codigo_serie": str(k), "laboratorios": json.dumps(v)} for k, v in product_lab_map.items()])
            df_labs.to_sql("forecast_product_labs", engine, if_exists="replace", index=False)
            with engine.begin() as conn:
                conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fc_labs_cdg ON forecast_product_labs (codigo_serie)"))
    
    logger.info("¡Migración OOM-Free a PostgreSQL completada con éxito!")

if __name__ == "__main__":
    run_migration()
