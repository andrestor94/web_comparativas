import os
import sys
import logging
from pathlib import Path
import pandas as pd
import json

# Fix import path for web_comparativas
sys.path.insert(0, str(Path(__file__).resolve().parent))

from web_comparativas.models import engine
from web_comparativas.forecast_service import _load_all_data
from sqlalchemy import text

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger("migrate_forecast")

def migrate_all():
    logger.info("Configuring DB Engine: %s", engine.url)
    
    logger.info("Executing heavy local load and join logic to Memory (this consumes RAM temporarily)...")
    data = _load_all_data()
    
    # 1. df_main
    df_main = data.get("df_main")
    if df_main is not None and not df_main.empty:
        logger.info("Uploading forecast_main to DB (%d rows)...", len(df_main))
        # Ensure all columns are clean lowercase
        df_main.columns = [str(c).lower().strip() for c in df_main.columns]
        df_main.to_sql("forecast_main", engine, if_exists="replace", index=False, chunksize=5000)
        logger.info("Creating indexes for forecast_main...")
        with engine.begin() as conn:
            for idx_col in ["perfil", "neg", "subneg", "codigo_serie"]:
                if idx_col in df_main.columns:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fc_main_{idx_col} ON forecast_main ({idx_col})"))

    # 2. df_valorizado
    df_valorizado = data.get("df_valorizado")
    if df_valorizado is not None and not df_valorizado.empty:
        logger.info("Uploading forecast_valorizado to DB (%d rows)...", len(df_valorizado))
        df_valorizado.columns = [str(c).lower().strip() for c in df_valorizado.columns]
        df_valorizado.to_sql("forecast_valorizado", engine, if_exists="replace", index=False, chunksize=5000)
        logger.info("Creating indexes for forecast_valorizado...")
        with engine.begin() as conn:
            for idx_col in ["fecha", "perfil", "codigo_serie", "cliente_id"]:
                if idx_col in df_valorizado.columns:
                    conn.execute(text(f"CREATE INDEX IF NOT EXISTS idx_fc_val_{idx_col} ON forecast_valorizado ({idx_col})"))

    # 3. df_imp_hist
    df_imp_hist = data.get("df_imp_hist")
    if df_imp_hist is not None and not df_imp_hist.empty:
        logger.info("Uploading forecast_imp_hist to DB (%d rows)...", len(df_imp_hist))
        df_imp_hist.to_sql("forecast_imp_hist", engine, if_exists="replace", index=False)
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fc_hist_perfil ON forecast_imp_hist (perfil)"))

    # 4. df_fact_2026
    df_fact_2026 = data.get("df_fact_2026")
    if df_fact_2026 is not None and not df_fact_2026.empty:
        logger.info("Uploading forecast_fact_2026 to DB (%d rows)...", len(df_fact_2026))
        df_fact_2026.to_sql("forecast_fact_2026", engine, if_exists="replace", index=False)

    # 5. product_lab_map
    product_lab_map = data.get("product_lab_map", {})
    if product_lab_map:
        logger.info("Uploading forecast_product_labs to DB...")
        df_labs = pd.DataFrame([{"codigo_serie": str(k), "laboratorios": json.dumps(v)} for k, v in product_lab_map.items()])
        df_labs.to_sql("forecast_product_labs", engine, if_exists="replace", index=False)
        with engine.begin() as conn:
            conn.execute(text("CREATE INDEX IF NOT EXISTS idx_fc_labs_cdg ON forecast_product_labs (codigo_serie)"))

    logger.info("¡Migración a PostgreSQL completada con éxito!")

if __name__ == "__main__":
    migrate_all()
