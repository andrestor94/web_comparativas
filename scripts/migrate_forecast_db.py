"""Migración de datos del Forecast de CSV a PostgreSQL."""

import os
import sys
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
import traceback

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.append(str(BASE_DIR))

from web_comparativas.forecast_models import (
    ForecastCliente, ForecastNegocio, ForecastArticulo,
    ForecastDatasetBase, ForecastBase, ForecastValorizado
)

DATABASE_URL = os.environ.get("DATABASE_URL", "postgresql://usuario:contraseña@localhost/web_comparativas_db")
if "postgres://" in DATABASE_URL:
    DATABASE_URL = DATABASE_URL.replace("postgres://", "postgresql://", 1)

engine = create_engine(DATABASE_URL)
FORECAST_DATA_DIR = Path(os.environ.get("FORECAST_DATA_DIR", str(BASE_DIR / "data" / "forecast")))

FILES_CONFIG = [
    {"file": "clientes.csv", "table": "forecast_cliente", "sep": ",", "encoding": "latin-1"},
    {"file": "Negocios.csv", "table": "forecast_negocio", "sep": ",", "encoding": "utf-8"},
    {"file": "Articulos 1.csv", "table": "forecast_articulo", "sep": ",", "encoding": "iso-8859-1"},
    {"file": "dataset_base.csv", "table": "forecast_dataset_base", "sep": ",", "encoding": "utf-8"},
    {"file": "forecast_base_consolidado.csv", "table": "forecast_base", "sep": ";", "encoding": "utf-8-sig", "decimal": ","},
    {"file": "forecast_valorizado_v2.csv", "table": "forecast_valorizado", "sep": ";", "encoding": "utf-8-sig", "decimal": ","}
]

def clean_columns(df: pd.DataFrame) -> pd.DataFrame:
    df.columns = [str(c).lower().strip().replace(" ", "_").replace("-", "_") for c in df.columns]
    return df

def parse_fechas(df: pd.DataFrame) -> pd.DataFrame:
    if "periodo" in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce").dt.date
    return df

def build_price_lookup() -> pd.DataFrame:
    file_master = FORECAST_DATA_DIR / "Articulos 1.csv"
    if not file_master.exists():
        return pd.DataFrame()
    df = pd.read_csv(str(file_master), sep=",", encoding="iso-8859-1", low_memory=False)
    df.columns = [c.lower().strip() for c in df.columns]
    
    col_art = "codigo"
    col_desc = "descrip"
    col_fam = "familia"
    col_predrog = "predrog"
    col_env = "cantenv"
    
    if col_art in df.columns: df[col_art] = df[col_art].astype(str).str.strip()
    if col_desc in df.columns: df[col_desc] = df[col_desc].astype(str).str.upper()
    if col_fam in df.columns: df[col_fam] = df[col_fam].astype(str).str.strip().str.upper()

    df_price = df[[col_art, col_desc, col_fam, col_predrog, col_env]].dropna(subset=[col_predrog, col_env]).copy()
    df_price["predrog"] = pd.to_numeric(df_price[col_predrog].astype(str).str.replace(",", "."), errors="coerce")
    df_price["cantenv"] = pd.to_numeric(df_price[col_env].astype(str).str.replace(",", "."), errors="coerce")
    df_price = df_price[(df_price["predrog"] > 0) & (df_price["cantenv"] > 0)].copy()
    df_price["precio_unitario"] = df_price["predrog"] / df_price["cantenv"]
    return df_price

def apply_prices(chunk: pd.DataFrame, df_price: pd.DataFrame) -> pd.DataFrame:
    if df_price.empty or "codigo_serie" not in chunk.columns:
        chunk["precio"] = 1500.0
        return chunk
        
    prices_art = df_price.groupby("codigo")["precio_unitario"].mean().to_dict()
    prices_desc = df_price.groupby("descrip")["precio_unitario"].mean().to_dict()
    prices_fam = df_price.groupby("familia")["precio_unitario"].mean().to_dict()
    
    chunk["codigo_serie"] = chunk["codigo_serie"].astype(str).str.strip()
    chunk["_key_desc"] = chunk["codigo_serie"].str.upper()
    chunk["_key_fam"] = chunk["codigo_serie"].str.upper()
    
    chunk["precio"] = chunk["codigo_serie"].map(prices_art)
    
    mask_zero = chunk["precio"].isna() | (chunk["precio"] == 0)
    if mask_zero.any():
        chunk.loc[mask_zero, "precio"] = chunk.loc[mask_zero, "_key_desc"].map(prices_desc)
        
    mask_zero = chunk["precio"].isna() | (chunk["precio"] == 0)
    if mask_zero.any():
        chunk.loc[mask_zero, "precio"] = chunk.loc[mask_zero, "_key_fam"].map(prices_fam)
        
    chunk["precio"] = chunk["precio"].fillna(1500.0)
    chunk.drop(columns=["_key_desc", "_key_fam"], inplace=True)
    return chunk

def migrate_file(config, df_price):
    file_path = FORECAST_DATA_DIR / config["file"]
    if not file_path.exists():
        print(f"[-] Saltando {config['file']}")
        return

    table_name = config["table"]
    print(f"[*] Migrando {config['file']} a {table_name}...")
    chunksize = 20000
    try:
        model_mapping = {
            "forecast_cliente": ForecastCliente,
            "forecast_negocio": ForecastNegocio,
            "forecast_articulo": ForecastArticulo,
            "forecast_dataset_base": ForecastDatasetBase,
            "forecast_base": ForecastBase,
            "forecast_valorizado": ForecastValorizado,
        }
        model = model_mapping.get(table_name)
        if model:
            model.__table__.drop(engine, checkfirst=True)
            model.__table__.create(engine)
            print(f"  -> Tabla {table_name} recreada con esquema SQLAlchemy estricto.")
        else:
            with engine.begin() as conn:
                conn.execute(text(f"DELETE FROM {table_name}"))
            print(f"  -> Tabla {table_name} limpiada para recarga.")

        chunks = pd.read_csv(
            str(file_path), sep=config["sep"], decimal=config.get("decimal", "."),
            encoding=config["encoding"], chunksize=chunksize, low_memory=False
        )
        total = 0
        for chunk in chunks:
            chunk = clean_columns(chunk)
            chunk = parse_fechas(chunk)
            
            if table_name == "forecast_cliente" and "codigo" in chunk.columns:
                chunk["codigo"] = chunk["codigo"].astype(str).str.strip()
            if table_name == "forecast_valorizado" and "cliente_id" in chunk.columns:
                chunk["cliente_id"] = chunk["cliente_id"].astype(str).str.strip()
            if table_name == "forecast_negocio":
                if "unidad" in chunk.columns: chunk["unidad"] = pd.to_numeric(chunk["unidad"], errors="coerce").fillna(0).astype(int)
                if "subunidad" in chunk.columns: chunk["subunidad"] = pd.to_numeric(chunk["subunidad"], errors="coerce").fillna(0).astype(int)
            if table_name == "forecast_articulo":
                for col in ["predrog", "cantenv"]:
                    if col in chunk.columns:
                        chunk[col] = pd.to_numeric(chunk[col].astype(str).str.replace(",", "."), errors="coerce")
            if table_name == "forecast_base":
                chunk = apply_prices(chunk, df_price)

            chunk.to_sql(table_name, engine, if_exists="append", index=False)
            total += len(chunk)
            print(f"  -> Insertados {total} registros", end="\r")
        print(f"\n[+] Migración exitosa! ({total} rows)")
    except Exception as e:
        print(f"\n[!] Error: {e}")
        traceback.print_exc()

def main():
    print("Building price lookup...")
    df_price = build_price_lookup()
    print("Migrating...")
    for conf in FILES_CONFIG:
        migrate_file(conf, df_price)
    print("Done")

if __name__ == "__main__":
    main()
