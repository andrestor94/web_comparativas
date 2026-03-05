"""
forecast_service.py — Native Forecast data loader & processor.
Ported from Streamlit data_loader.py to run within FastAPI.
Loads CSVs, processes metadata, calculates prices, and returns
JSON-serializable structures for the frontend.
"""
import os
import gc
import logging
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np

logger = logging.getLogger("wc.forecast")

# ---------------------------------------------------------------------------
# Configuration: where to find the Forecast data files
# ---------------------------------------------------------------------------
_BASE = Path(__file__).resolve().parent.parent  # web_comparativas_v2/
FORECAST_DATA_DIR = Path(os.environ.get("FORECAST_DATA_DIR", str(_BASE / "data" / "forecast")))

_FORECAST_FILE      = FORECAST_DATA_DIR / "forecast_base_consolidado.csv"
_MASTER_FILE        = FORECAST_DATA_DIR / "Articulos 1.csv"
_NEGOCIOS_FILE      = FORECAST_DATA_DIR / "Negocios.csv"
_CLIENT_FILE        = FORECAST_DATA_DIR / "forecast_por_cliente.csv"
_SERIES_FILE        = FORECAST_DATA_DIR / "dataset_base.csv"
_VALORIZADO_FILE    = FORECAST_DATA_DIR / "forecast_valorizado_v2.csv"
_CLIENTES_FILE      = FORECAST_DATA_DIR / "clientes.csv"

# ---------------------------------------------------------------------------
# Singleton cache (lazy load)
# ---------------------------------------------------------------------------
_cache: Dict[str, Any] = {}


def _downcast_df(df: pd.DataFrame) -> pd.DataFrame:
    """Aggressively reduce DataFrame memory by converting dtypes."""
    for c in df.select_dtypes(include="object").columns:
        df[c] = df[c].astype("category")
    for c in df.select_dtypes(include="float64").columns:
        df[c] = df[c].astype("float32")
    for c in df.select_dtypes(include="int64").columns:
        df[c] = pd.to_numeric(df[c], downcast="integer")
    return df


def _get_col(df: pd.DataFrame, name: str) -> Optional[str]:
    for c in df.columns:
        if c.lower().strip() == name.lower().strip():
            return c
    return None


def _parse_float(x):
    if pd.isna(x):
        return 0.0
    try:
        return float(str(x).replace(",", "."))
    except Exception:
        return 0.0


def _parse_int(x):
    try:
        return int(float(str(x).replace(",", ".")))
    except Exception:
        return 1


def _norm_key(s):
    return " ".join(str(s).split()).upper()


# ---------------------------------------------------------------------------
# Core loaders (mirror data_loader.py logic)
# ---------------------------------------------------------------------------

def _load_master_meta() -> pd.DataFrame:
    if "df_meta" in _cache:
        return _cache["df_meta"]
    if not _MASTER_FILE.exists():
        return pd.DataFrame()
    try:
        logger.info("[Forecast] Reading master metadata: %s", _MASTER_FILE.name)
        df = pd.read_csv(str(_MASTER_FILE), sep=",", encoding="latin-1")
        df.columns = [c.strip() for c in df.columns]
        col_art = _get_col(df, "Articulo1") or _get_col(df, "Articulo") or _get_col(df, "codigo")
        col_fam = _get_col(df, "Familia")
        col_desc = _get_col(df, "Descrip_art") or _get_col(df, "descrip")
        col_lab = _get_col(df, "laboratorio_descrip") or _get_col(df, "Laboratorio_Descrip")
        
        if not col_art:
            return pd.DataFrame()
        df[col_art] = df[col_art].astype(str)
        cols = [col_art]
        if col_fam: cols.append(col_fam)
        if col_desc: cols.append(col_desc)
        if col_lab: cols.append(col_lab)
            
        meta = df[cols].copy()
        
        sort_cols = [col_art]
        sort_asc = [True]
        if col_fam: sort_cols.append(col_fam); sort_asc.append(False)
        if col_desc: sort_cols.append(col_desc); sort_asc.append(False)
        if len(sort_cols) > 1:
            meta = meta.sort_values(by=sort_cols, ascending=sort_asc)
            
        meta = meta.drop_duplicates(subset=[col_art], keep="first")
        meta = _downcast_df(meta)
        _cache["df_meta"] = meta
        return meta
    except Exception as e:
        logger.warning("Master load error: %s", e)
        return pd.DataFrame()


def _process_df(df_input: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    if df_input.empty:
        return df_input
    df_input.columns = [c.lower().strip() for c in df_input.columns]
    if "codigo_serie" in df_input.columns and "articulo" not in df_input.columns:
        df_input["articulo"] = df_input["codigo_serie"].astype(str)
    else:
        df_input["articulo"] = df_input.get("articulo", pd.Series(dtype="str")).astype(str)

    df = df_input.copy()

    # Merge metadata
    if not df_meta.empty:
        col_art_master = df_meta.columns[0]
        col_desc_master = "Descrip_art" if "Descrip_art" in df_meta.columns else ("descrip" if "descrip" in df_meta.columns else None)
        
        # In forecast_valorizado_v2.csv, codigo_serie holds the core description without the " - LAB" suffix
        if "codigo_serie" in df.columns and col_desc_master:
            df_meta_clean = df_meta.copy()
            df_meta_clean["_descrip_clean"] = df_meta_clean[col_desc_master].astype(str).str.split(" - ").str[0]
            # CRITICAL: Deduplicate on merge key to prevent many-to-many join (was creating 68k extra rows!)
            df_meta_clean = df_meta_clean.drop_duplicates(subset=["_descrip_clean"], keep="first")
            df = pd.merge(df, df_meta_clean, left_on="articulo", right_on="_descrip_clean", how="left")
            df.drop(columns=["_descrip_clean"], inplace=True)
        else:
            df = pd.merge(df, df_meta, left_on="articulo", right_on=col_art_master, how="left")
        col_fam = "Familia" if "Familia" in df_meta.columns else None
        col_desc = "Descrip_art" if "Descrip_art" in df_meta.columns else None
        if "codigo_serie" in df.columns:
            df["descripcion"] = df["codigo_serie"]
        elif col_desc:
            df["descripcion"] = df[col_desc]
        else:
            df["descripcion"] = pd.NA
        if col_desc:
            df["descripcion"] = df["descripcion"].replace(["", "nan"], pd.NA)
            df["descripcion"] = df["descripcion"].fillna(df[col_desc])
        df["descripcion"] = df["descripcion"].replace(["", "nan"], pd.NA)
        df["descripcion"] = df["descripcion"].fillna(df["articulo"])
    else:
        df["descripcion"] = df["articulo"]

    # Date
    if "periodo" in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")

    # Business names
    if _NEGOCIOS_FILE.exists():
        try:
            df_names = pd.read_csv(str(_NEGOCIOS_FILE))
            df_names.columns = [c.upper().strip() for c in df_names.columns]
            if all(c in df_names.columns for c in ["UNIDAD", "DESCRIP", "SUBUNIDAD"]):
                df_names["UNIDAD"] = pd.to_numeric(df_names["UNIDAD"], errors="coerce").fillna(0).astype(int)
                df_names["SUBUNIDAD"] = pd.to_numeric(df_names["SUBUNIDAD"], errors="coerce").fillna(0).astype(int)
                # Negocio
                map_neg = df_names[df_names["SUBUNIDAD"] == 0][["UNIDAD", "DESCRIP"]].drop_duplicates(subset=["UNIDAD"])
                map_neg = map_neg.rename(columns={"DESCRIP": "Negocio_Nombre"})
                if "neg" in df.columns:
                    df["neg_id"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
                    df = pd.merge(df, map_neg, left_on="neg_id", right_on="UNIDAD", how="left")
                    df.drop(columns=["UNIDAD"], inplace=True, errors="ignore")
                # Subnegocio
                map_sub = df_names[df_names["SUBUNIDAD"] != 0][["UNIDAD", "SUBUNIDAD", "DESCRIP"]].drop_duplicates(subset=["UNIDAD", "SUBUNIDAD"])
                map_sub = map_sub.rename(columns={"DESCRIP": "Subnegocio_Nombre"})
                if "neg" in df.columns and "subneg" in df.columns:
                    if "neg_id" not in df.columns:
                        df["neg_id"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
                    df["subneg_id"] = pd.to_numeric(df["subneg"], errors="coerce").fillna(0).astype(int)
                    df = pd.merge(df, map_sub, left_on=["neg_id", "subneg_id"], right_on=["UNIDAD", "SUBUNIDAD"], how="left")
                    df.drop(columns=["UNIDAD", "SUBUNIDAD"], inplace=True, errors="ignore")
                if "Negocio_Nombre" in df.columns:
                    df["neg"] = df["Negocio_Nombre"].fillna(df["neg"])
                    df.drop(columns=["Negocio_Nombre", "neg_id"], inplace=True, errors="ignore")
                if "Subnegocio_Nombre" in df.columns:
                    df["subneg"] = df["Subnegocio_Nombre"].fillna(df["subneg"])
                    df.drop(columns=["Subnegocio_Nombre", "subneg_id"], inplace=True, errors="ignore")
        except Exception:
            pass

    for c in ("neg", "subneg"):
        if c in df.columns:
            df[c] = df[c].astype(str)
    return df


def _build_price_lookup() -> Dict[str, Dict[str, float]]:
    price_lookup: Dict[str, Dict[str, float]] = {"ARTICULO": {}, "FAMILIA": {}, "CODIGO": {}}
    if not _MASTER_FILE.exists():
        return price_lookup
    try:
        df = pd.read_csv(str(_MASTER_FILE), sep=",", encoding="latin-1", dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]
        if "descrip" not in df.columns or "predrog" not in df.columns:
            return price_lookup
        df["predrog_val"] = df["predrog"].apply(_parse_float)
        df["cantenv_val"] = df.get("cantenv", pd.Series([1]*len(df))).apply(_parse_int)
        df.loc[df["cantenv_val"] <= 0, "cantenv_val"] = 1
        df["unit_price"] = df["predrog_val"]
        mask_pack = df["cantenv_val"] > 1
        df.loc[mask_pack, "unit_price"] = df.loc[mask_pack, "predrog_val"] / df.loc[mask_pack, "cantenv_val"]
        # Articulo lookup
        df["descrip_norm"] = df["descrip"].apply(_norm_key)
        price_lookup["ARTICULO"] = df.groupby("descrip_norm")["unit_price"].mean().to_dict()
        # Codigo lookup
        if "codigo" in df.columns:
            df["codigo_norm"] = df["codigo"].apply(_norm_key)
            price_lookup["CODIGO"] = df.groupby("codigo_norm")["unit_price"].mean().to_dict()
        # Familia lookup
        if "familia" in df.columns:
            df["familia_norm"] = df["familia"].apply(_norm_key)
            mask_fam = (df["familia_norm"] != "NAN") & (df["familia_norm"] != "")
            price_lookup["FAMILIA"] = df[mask_fam].groupby("familia_norm")["unit_price"].mean().to_dict()
    except Exception as e:
        logger.warning("Price lookup error: %s", e)
    return price_lookup


def _apply_prices(df: pd.DataFrame, pl: Dict) -> pd.DataFrame:
    if df.empty:
        return df
    df["precio"] = 0.0
    df["_key_norm"] = df["articulo"].apply(_norm_key)
    if "CODIGO" in pl:
        df["precio"] = df["_key_norm"].map(pl["CODIGO"]).fillna(df["precio"])
    mask_zero = df["precio"] == 0
    if "ARTICULO" in pl and mask_zero.any():
        col_d = "descripcion" if "descripcion" in df.columns else "articulo"
        df.loc[mask_zero, "_key_desc"] = df.loc[mask_zero, col_d].apply(_norm_key)
        df.loc[mask_zero, "precio"] = df.loc[mask_zero, "_key_desc"].map(pl["ARTICULO"]).fillna(0.0)
    mask_zero = df["precio"] == 0
    if "FAMILIA" in pl and mask_zero.any():
        fam_col = "familia_x" if "familia_x" in df.columns else ("familia" if "familia" in df.columns else None)
        if fam_col:
            df.loc[mask_zero, "_key_fam"] = df.loc[mask_zero, fam_col].apply(_norm_key)
            df.loc[mask_zero, "precio"] = df.loc[mask_zero, "_key_fam"].map(pl["FAMILIA"]).fillna(0.0)
    df.drop(columns=[c for c in ["_key_norm", "_key_desc", "_key_fam"] if c in df.columns], inplace=True)
    return df


def _load_valorizado(pl: Dict) -> pd.DataFrame:
    if not _VALORIZADO_FILE.exists():
        return pd.DataFrame()
    try:
        df = pd.read_csv(str(_VALORIZADO_FILE), sep=";", decimal=",", encoding="utf-8-sig", low_memory=False)
        df.columns = [c.lower().strip() for c in df.columns]
        if "periodo" in df.columns:
            df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")
        # Join clientes
        if _CLIENTES_FILE.exists():
            try:
                df_cli = pd.read_csv(str(_CLIENTES_FILE), encoding="latin-1", low_memory=False)
                df_cli.columns = [c.lower().strip() for c in df_cli.columns]
                df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
                df["cliente_id"] = df["cliente_id"].astype(str).str.strip()
                cli_lookup = df_cli[["codigo", "fantasia", "nombre_grupo", "cliente_grupo"]].drop_duplicates(subset=["codigo"])
                cli_lookup["cliente_grupo"] = cli_lookup["cliente_grupo"].astype(str).str.strip()
                df = pd.merge(df, cli_lookup, left_on="cliente_id", right_on="codigo", how="left")
                df.drop(columns=["codigo"], inplace=True, errors="ignore")
                mask_no = df["fantasia"].isna()
                if mask_no.any():
                    nombre_grupos_set = set(df_cli["nombre_grupo"].dropna().unique())
                    is_grp = df.loc[mask_no, "cliente_id"].isin(nombre_grupos_set)
                    idx_grp = mask_no[mask_no].index[is_grp.values]
                    df.loc[idx_grp, "fantasia"] = df.loc[idx_grp, "cliente_id"]
                    df.loc[idx_grp, "nombre_grupo"] = df.loc[idx_grp, "cliente_id"]
                    still = df["fantasia"].isna()
                    df.loc[still, "fantasia"] = df.loc[still, "cliente_id"]
                    df.loc[still, "nombre_grupo"] = "SIN GRUPO"
                df["fantasia"] = df["fantasia"].fillna(df["cliente_id"])
                df["nombre_grupo"] = df["nombre_grupo"].fillna("SIN GRUPO")
            except Exception as e:
                logger.warning("clientes join error: %s", e)
                df["fantasia"] = df["cliente_id"]
                df["nombre_grupo"] = "SIN GRUPO"
        # Negocio names
        if _NEGOCIOS_FILE.exists() and "neg" in df.columns:
            try:
                dfn = pd.read_csv(str(_NEGOCIOS_FILE))
                dfn.columns = [c.upper().strip() for c in dfn.columns]
                dfn["UNIDAD"] = pd.to_numeric(dfn["UNIDAD"], errors="coerce").fillna(0).astype(int)
                dfn["SUBUNIDAD"] = pd.to_numeric(dfn["SUBUNIDAD"], errors="coerce").fillna(0).astype(int)
                mn = dfn[dfn["SUBUNIDAD"] == 0][["UNIDAD", "DESCRIP"]].drop_duplicates(subset=["UNIDAD"])
                mn = mn.rename(columns={"DESCRIP": "Negocio_Nombre"})
                df["neg_id"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
                df = pd.merge(df, mn, left_on="neg_id", right_on="UNIDAD", how="left")
                df.drop(columns=["UNIDAD"], inplace=True, errors="ignore")
                if "Negocio_Nombre" in df.columns:
                    df["neg"] = df["Negocio_Nombre"].fillna(df["neg"])
                    df.drop(columns=["Negocio_Nombre", "neg_id"], inplace=True, errors="ignore")
            except Exception:
                pass
        for c in ("neg", "subneg"):
            if c in df.columns:
                df[c] = df[c].astype(str)
        if "codigo_serie" in df.columns and "descripcion" not in df.columns:
            df["descripcion"] = df["codigo_serie"]
        return df
    except Exception as e:
        logger.warning("Valorizado load error: %s", e)
        return pd.DataFrame()


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------

def _build_product_lab_map() -> Dict[str, set]:
    """Build product->set(labs) map using dataset_base.csv (ETAPA1) + Articulos 1.csv.
    Replicates original data_loader.py lines 184-248 exactly."""
    product_lab_map: Dict[str, set] = {}
    if not _SERIES_FILE.exists() or not _MASTER_FILE.exists():
        return product_lab_map
    try:
        df_series = pd.read_csv(str(_SERIES_FILE), sep=",", encoding="utf-8", dtype=str)
        df_series.columns = [c.strip() for c in df_series.columns]
        df_articulos = pd.read_csv(str(_MASTER_FILE), sep=",", encoding="latin-1", dtype=str)
        df_articulos.columns = [c.strip() for c in df_articulos.columns]

        col_lab = _get_col(df_articulos, "laboratorio_descrip")
        col_fam_art = _get_col(df_articulos, "familia")
        col_desc_art = _get_col(df_articulos, "descrip")

        if col_lab:
            fam_to_lab = {}
            if col_fam_art:
                temp = df_articulos[[col_fam_art, col_lab]].dropna()
                fam_to_lab = temp.groupby(col_fam_art)[col_lab].apply(set).to_dict()
            desc_to_lab = {}
            if col_desc_art:
                temp = df_articulos[[col_desc_art, col_lab]].dropna()
                desc_to_lab = temp.groupby(col_desc_art)[col_lab].apply(set).to_dict()

            col_serie = _get_col(df_series, "codigo_serie")
            col_nivel = _get_col(df_series, "nivel_agregacion")

            if col_serie and col_nivel:
                for _, row in df_series.iterrows():
                    serie = str(row[col_serie]).strip()
                    nivel = str(row[col_nivel]).strip().upper()
                    labs = set()
                    if nivel == "FAMILIA":
                        labs = fam_to_lab.get(serie, set())
                    elif nivel in ("ARTICULO", "ITEM"):
                        labs = desc_to_lab.get(serie, set())
                    else:
                        labs = fam_to_lab.get(serie, set()).union(desc_to_lab.get(serie, set()))
                    if labs:
                        product_lab_map[serie] = labs
        logger.info("[Forecast] Built product_lab_map with %d entries", len(product_lab_map))
    except Exception as e:
        logger.warning("product_lab_map error: %s", e)
    return product_lab_map


def _ensure_loaded():
    """Load all data once and cache."""
    if "df_main" in _cache:
        return
    logger.info("[Forecast] Loading data...")
    meta = _load_master_meta()
    pl = _build_price_lookup()
    gc.collect()

    # Main forecast
    try:
        df_raw = pd.read_csv(str(_FORECAST_FILE), sep=";", decimal=",",
                             encoding="utf-8-sig", low_memory=False)
        df_main = _process_df(df_raw, meta)
        del df_raw; gc.collect()
        df_main = _apply_prices(df_main, pl)
    except Exception as e:
        logger.warning("Forecast file error: %s", e)
        df_main = pd.DataFrame()

    if not df_main.empty:
        type_map = {"hist": "Historia", "forecast": "Proyección"}
        df_main["Etiqueta_Upper"] = df_main["tipo"].map(type_map).fillna(df_main["tipo"])
        if "precio" not in df_main.columns:
            df_main["precio"] = 1500

    df_val = _load_valorizado(pl)
    gc.collect()
    plm = _build_product_lab_map()
    gc.collect()

    # --- Downcast all cached DataFrames to save memory ---
    if not df_main.empty:
        df_main = _downcast_df(df_main)
    if not df_val.empty:
        df_val = _downcast_df(df_val)

    _cache["df_main"] = df_main
    _cache["df_valorizado"] = df_val
    _cache["price_lookup"] = pl
    _cache["product_lab_map"] = plm
    gc.collect()
    logger.info("[Forecast] Loaded. Main rows=%d (%.1f MB), Val rows=%d (%.1f MB)",
                len(df_main),
                df_main.memory_usage(deep=True).sum() / 1024 / 1024 if not df_main.empty else 0,
                len(df_val),
                df_val.memory_usage(deep=True).sum() / 1024 / 1024 if not df_val.empty else 0)


def is_available() -> bool:
    """Check if forecast data files exist."""
    return _FORECAST_FILE.exists()


def get_filter_options() -> Dict[str, Any]:
    _ensure_loaded()
    df = _cache.get("df_main", pd.DataFrame())
    plm = _cache.get("product_lab_map", {})  # product -> set(labs)
    if df.empty:
        return {"profiles": [], "negocios": [], "subnegocios": [], "laboratorios": [], "products": [], "product_lab_map": {}, "min_date": "", "max_date": ""}
    profiles = sorted(df["perfil"].dropna().unique().tolist()) if "perfil" in df.columns else []
    negs = sorted(df["neg"].dropna().unique().tolist()) if "neg" in df.columns else []
    subnegs = sorted(df["subneg"].dropna().unique().tolist()) if "subneg" in df.columns else []

    # Build products list from forecast df descriptions
    products_dict: Dict[str, str] = {}
    if "descripcion" in df.columns:
        for p in df["descripcion"].dropna().unique().tolist():
            p_name = str(p).strip()
            if p_name and p_name != "nan":
                # Use product_lab_map to find the labs for this product
                labs = plm.get(p_name, set())
                if labs:
                    products_dict[p_name] = ", ".join(sorted(labs))
                else:
                    products_dict[p_name] = "SIN LABORATORIO"

    products_list = [{"id": k, "lab": v} for k, v in products_dict.items()]

    # Sort products by volume (sum of (y+yhat)*precio) desc
    if "descripcion" in df.columns and "precio" in df.columns:
        df["_vol"] = (df.get("y", 0).fillna(0) + df.get("yhat", 0).fillna(0)) * df["precio"].fillna(0)
        vol_rank = df.groupby("descripcion")["_vol"].sum()
        for p in products_list:
            p["vol"] = float(vol_rank.get(p["id"], 0))
        products_list = sorted(products_list, key=lambda x: x.get("vol", 0), reverse=True)
    else:
        products_list = sorted(products_list, key=lambda x: x["id"])

    # Build unique lab list from product_lab_map
    all_labs: set = set()
    for labs_set in plm.values():
        all_labs.update(labs_set)
    laboratorios = ["ALL"] + sorted(list(all_labs))

    # Build serializable product_lab_map (product -> list of lab strings)
    plm_json: Dict[str, List[str]] = {}
    for prod, labs_set in plm.items():
        plm_json[prod] = sorted(labs_set)

    # Build product -> negocio/subnegocio map for auto-filtering
    product_negocio_map: Dict[str, Dict[str, str]] = {}
    if "descripcion" in df.columns and "neg" in df.columns:
        for _, row in df[["descripcion", "neg", "subneg"]].drop_duplicates(subset=["descripcion"]).iterrows():
            desc = str(row["descripcion"]).strip()
            if desc and desc != "nan":
                neg_val = str(row.get("neg", "")) if pd.notna(row.get("neg")) else ""
                subneg_val = str(row.get("subneg", "")) if pd.notna(row.get("subneg")) else ""
                product_negocio_map[desc] = {"neg": neg_val, "subneg": subneg_val}

    min_d = str(df["fecha"].min().date()) if "fecha" in df.columns and not df["fecha"].isna().all() else ""
    max_d = str(df["fecha"].max().date()) if "fecha" in df.columns and not df["fecha"].isna().all() else ""
    # Cap default end
    hist_max = df[df["tipo"] == "hist"]["fecha"].max() if "tipo" in df.columns else None
    if pd.notna(hist_max):
        last_proj_year = hist_max.year + 1
        default_end = min(df["fecha"].max(), pd.Timestamp(year=last_proj_year, month=12, day=31))
        default_end_str = str(default_end.date())
        # The user wants the default start to be Jan 1st of the year before projection (which is hist_max year)
        default_start = pd.Timestamp(year=hist_max.year, month=1, day=1)
        default_start_str = str(default_start.date())
    else:
        default_end_str = max_d
        default_start_str = min_d
    return {
        "profiles": profiles,
        "negocios": negs,
        "subnegocios": subnegs,
        "laboratorios": laboratorios,
        "products": products_list,
        "product_lab_map": plm_json,
        "product_negocio_map": product_negocio_map,
        "min_date": min_d,
        "max_date": max_d,
        "default_start": default_start_str,
        "default_end": default_end_str,
    }



def get_chart_data(
    start_date: str,
    end_date: str,
    profiles: List[str],
    negocios: List[str],
    subnegocios: List[str],
    products: List[str],
    growth_pct: float = 0.0,
    view_money: bool = True,
) -> Dict[str, Any]:
    """Return chart series for the demand evolution chart."""
    _ensure_loaded()
    df = _cache.get("df_main", pd.DataFrame()).copy()
    df_val = _cache.get("df_valorizado", pd.DataFrame()).copy()
    if df.empty:
        return {"history": [], "forecast": [], "forecast_adj": [], "ci_upper": [], "ci_lower": []}

    sd = pd.to_datetime(start_date)
    ed = pd.to_datetime(end_date)

    # Filter main
    mask = (df["fecha"] >= sd) & (df["fecha"] <= ed)
    if profiles:
        mask = mask & df["perfil"].isin(profiles)
    if negocios and "neg" in df.columns:
        mask = mask & df["neg"].isin(negocios)
    if subnegocios and "subneg" in df.columns:
        mask = mask & df["subneg"].isin(subnegocios)
    if products and "descripcion" in df.columns:
        mask = mask & df["descripcion"].isin(products)
    df_f = df[mask].copy()

    # Chart always in $ — convert units to money
    if "precio" in df_f.columns:
        for col in ["y", "yhat", "li", "ls"]:
            if col in df_f.columns:
                df_f[col] = df_f[col] * df_f["precio"]

    # History
    df_hist = df_f[df_f["Etiqueta_Upper"] == "Historia"].groupby("fecha").agg(total=("y", "sum")).reset_index()

    # Forecast from valorizado if available
    if not df_val.empty:
        mask_v = (df_val["fecha"] >= sd) & (df_val["fecha"] <= ed)
        if profiles and "perfil" in df_val.columns:
            mask_v = mask_v & df_val["perfil"].isin(profiles)
        if negocios and "neg" in df_val.columns:
            mask_v = mask_v & df_val["neg"].isin(negocios)
        if subnegocios and "subneg" in df_val.columns:
            mask_v = mask_v & df_val["subneg"].isin(subnegocios)
        if products and "descripcion" in df_val.columns:
            mask_v = mask_v & df_val["descripcion"].isin(products)
        vf = df_val[mask_v]
        df_fcst = vf.groupby("fecha").agg(
            total=("monto_yhat", "sum"),
            li=("monto_li", "sum"),
            ls=("monto_ls", "sum"),
        ).reset_index()
    else:
        df_fc_src = df_f[df_f["Etiqueta_Upper"] == "Proyección"]
        df_fcst = df_fc_src.groupby("fecha").agg(
            total=("yhat", "sum"),
            li=("li", "sum"),
            ls=("ls", "sum"),
        ).reset_index()

    df_fcst = df_fcst[df_fcst["fecha"] <= ed].sort_values("fecha")

    # Growth adjustment
    df_fcst["total_adj"] = df_fcst["total"].copy()
    if growth_pct != 0 and not df_fcst.empty:
        start_proj = df_fcst["fecha"].min()
        months_diff = ((df_fcst["fecha"].dt.year - start_proj.year) * 12 +
                       (df_fcst["fecha"].dt.month - start_proj.month))
        quarters = (months_diff // 3) + 1
        factor = 1.0 + (growth_pct * quarters / 100.0)
        df_fcst["total_adj"] = df_fcst["total"] * factor

    # Separator date
    hist_max_date = str(df_hist["fecha"].max().date()) if not df_hist.empty else None

    def series(dframe, col):
        return [
            {"x": str(r["fecha"].date()), "y": round(float(r[col]))}
            for _, r in dframe.iterrows()
        ]

    return {
        "history": series(df_hist.sort_values("fecha"), "total"),
        "forecast": series(df_fcst, "total"),
        "forecast_adj": series(df_fcst, "total_adj") if growth_pct != 0 else [],
        "ci_upper": series(df_fcst, "ls"),
        "ci_lower": series(df_fcst, "li"),
        "hist_max_date": hist_max_date,
        "growth_pct": growth_pct,
    }


def get_client_table(
    start_date: str,
    end_date: str,
    profiles: List[str],
    negocios: List[str],
    subnegocios: List[str],
    products: List[str],
    growth_pct: float = 0.0,
    view_money: bool = True,
) -> Dict[str, Any]:
    """Return hierarchical client pivot table data grouped by nombre_grupo."""
    _ensure_loaded()
    df_main = _cache.get("df_main", pd.DataFrame())
    df_val = _cache.get("df_valorizado", pd.DataFrame())
    src = df_val if not df_val.empty else pd.DataFrame()
    use_val = not df_val.empty

    if src.empty:
        return {"columns": [], "groups": [], "grand_totals": {}}

    sd = pd.to_datetime(start_date)
    ed = pd.to_datetime(end_date)
    mask = (src["fecha"] >= sd) & (src["fecha"] <= ed)
    if profiles and "perfil" in src.columns:
        mask = mask & src["perfil"].isin(profiles)
    if negocios and "neg" in src.columns:
        mask = mask & src["neg"].isin(negocios)
    if subnegocios and "subneg" in src.columns:
        mask = mask & src["subneg"].isin(subnegocios)
    if products and "descripcion" in src.columns:
        mask = mask & src["descripcion"].isin(products)
    df_c = src[mask].copy()
    if df_c.empty:
        return {"columns": [], "groups": [], "grand_totals": {}}

    val_col = "monto_yhat" if (use_val and view_money) else "yhat_cliente"
    if val_col not in df_c.columns:
        val_col = "yhat_cliente" if "yhat_cliente" in df_c.columns else "monto_yhat"
    if val_col not in df_c.columns:
        return {"columns": [], "groups": [], "grand_totals": {}}

    # Client display name & group
    if use_val and "fantasia" in df_c.columns:
        df_c["_cli"] = df_c["fantasia"]
        if "nombre_grupo" in df_c.columns:
            df_c["_grp"] = df_c["nombre_grupo"].fillna("")
            mask_sg = df_c["_grp"] == "SIN GRUPO"
            mask_sr = df_c["_cli"] == df_c["_grp"]
            df_c.loc[mask_sg | mask_sr, "_grp"] = ""
        else:
            df_c["_grp"] = ""
    else:
        df_c["_cli"] = df_c["cliente_id"]
        df_c["_grp"] = ""

    # Also keep a raw client_id for the detail endpoint
    if "cliente_id" in df_c.columns:
        df_c["_cli_id"] = df_c["cliente_id"].astype(str)
    else:
        df_c["_cli_id"] = df_c["_cli"]

    # Pivot
    piv = df_c.groupby(["_cli", "_grp", "_cli_id", "fecha"])[val_col].sum().reset_index()
    pivot = piv.set_index(["_cli", "_grp", "_cli_id", "fecha"])[val_col].unstack("fecha").fillna(0)
    pivot = pivot.sort_index(axis=1)
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=False)
    pivot.drop(columns=["_total"], inplace=True)

    date_cols = list(pivot.columns)

    # Calculate base grand totals before adjustment
    grand_totals_base = {label: float(pivot[d].sum()) for d, label in zip(date_cols, [d.strftime("%b %Y").title() for d in date_cols])}

    # Growth adjustment on future columns
    if growth_pct != 0 and not df_main.empty and "tipo" in df_main.columns:
        max_hist = df_main[df_main["tipo"] == "hist"]["fecha"].max()
        future_cols = sorted([c for c in date_cols if c > max_hist])
        if future_cols:
            start_proj = future_cols[0]
            for col_date in future_cols:
                md = (col_date.year - start_proj.year) * 12 + (col_date.month - start_proj.month)
                q = (md // 3) + 1
                f = 1.0 + (growth_pct * q / 100.0)
                pivot[col_date] *= f

    col_labels = [d.strftime("%b %Y").title() for d in date_cols]

    plm = _cache.get("product_lab_map", {})
    client_labs = {}
    if "descripcion" in df_c.columns:
        grouped = df_c.groupby("_cli_id")["descripcion"].unique()
        for cid, prods in grouped.items():
            labs = set()
            for p in prods:
                if pd.notna(p):
                    labs.update(plm.get(str(p).strip(), set()))
            client_labs[str(cid)] = list(labs)

    # Build hierarchical structure: groups -> clients
    # Collect all rows first
    flat_rows = []
    for (cli, grp, cli_id), row_data in pivot.iterrows():
        r = {"cliente": cli, "grupo": grp, "cliente_id": cli_id, "labs": client_labs.get(str(cli_id), [])}
        for d, label in zip(date_cols, col_labels):
            r[label] = round(float(row_data[d]))
        flat_rows.append(r)

    # Group them
    from collections import OrderedDict
    group_map = OrderedDict()
    ungrouped = []
    for r in flat_rows:
        grp_name = r["grupo"]
        if grp_name:
            if grp_name not in group_map:
                group_map[grp_name] = []
            group_map[grp_name].append(r)
        else:
            ungrouped.append(r)

    groups_out = []
    # Named groups first (sorted by total desc)
    for grp_name, clients in group_map.items():
        grp_totals = {}
        grp_labs = set()
        for label in col_labels:
            grp_totals[label] = sum(c[label] for c in clients)
        for c in clients:
            grp_labs.update(c.get("labs", []))
        groups_out.append({
            "grupo": grp_name,
            "totals": grp_totals,
            "clients": clients,
            "labs": list(grp_labs),
        })
    # Sort groups by sum of totals desc
    groups_out.sort(key=lambda g: sum(g["totals"].values()), reverse=True)

    # Ungrouped clients as individual groups (1 client each, no expand)
    for r in ungrouped:
        grp_totals = {label: r[label] for label in col_labels}
        groups_out.append({
            "grupo": None,
            "totals": grp_totals,
            "clients": [r],
            "labs": r.get("labs", []),
        })

    # Grand totals
    grand_totals = {}
    for label in col_labels:
        grand_totals[label] = sum(r[label] for r in flat_rows)

    # Min/max for heatmap
    all_vals = [r[label] for r in flat_rows for label in col_labels]
    min_val = min(all_vals) if all_vals else 0
    max_val = max(all_vals) if all_vals else 1

    return {
        "columns": col_labels,
        "groups": groups_out,
        "grand_totals": grand_totals,
        "grand_totals_base": grand_totals_base,
        "growth_pct": growth_pct,
        "min_val": min_val,
        "max_val": max_val,
        "view_money": view_money,
    }


def get_client_detail(
    cliente_display: str,
    start_date: str,
    end_date: str,
    profiles: List[str],
    negocios: List[str],
    subnegocios: List[str],
    products: List[str],
    growth_pct: float = 0.0,
) -> Dict[str, Any]:
    """Return product-level detail for a specific client, grouped by Negocio→Subnegocio."""
    _ensure_loaded()
    df_main = _cache.get("df_main", pd.DataFrame())
    df_val = _cache.get("df_valorizado", pd.DataFrame())
    pl = _cache.get("price_lookup", {})

    src = df_val if not df_val.empty else pd.DataFrame()
    if src.empty:
        return {"client": cliente_display, "negocios": []}

    sd = pd.to_datetime(start_date)
    ed = pd.to_datetime(end_date)

    # Filter
    mask = (src["fecha"] >= sd) & (src["fecha"] <= ed)
    if profiles and "perfil" in src.columns:
        mask = mask & src["perfil"].isin(profiles)
    if negocios and "neg" in src.columns:
        mask = mask & src["neg"].isin(negocios)
    if subnegocios and "subneg" in src.columns:
        mask = mask & src["subneg"].isin(subnegocios)
    if products and "descripcion" in src.columns:
        mask = mask & src["descripcion"].isin(products)
    # Match on fantasia (display name)
    cli_col = "fantasia" if "fantasia" in src.columns else "cliente_id"
    mask = mask & (src[cli_col] == cliente_display)

    df_c = src[mask].copy()
    if df_c.empty:
        return {"client": cliente_display, "negocios": [], "columns": [], "perfil": "", "negocio": ""}

    # Client info
    first = df_c.iloc[0]
    perfil = str(first.get("perfil", "N/A"))
    negocio_top = str(first.get("neg", "N/A"))

    # Ensure descripcion and neg/subneg columns exist
    desc_col = "descripcion" if "descripcion" in df_c.columns else "codigo_serie"
    if desc_col not in df_c.columns:
        return {"client": cliente_display, "negocios": [], "columns": [], "perfil": perfil, "negocio": negocio_top}

    # Enrich with neg/subneg from df_main if missing in valorizado
    if "neg" not in df_c.columns or df_c["neg"].isna().all():
        if not df_main.empty and "neg" in df_main.columns and desc_col in df_main.columns:
            series_neg = df_main[[desc_col, "neg"]].drop_duplicates(subset=[desc_col])
            df_c = pd.merge(df_c, series_neg, on=desc_col, how="left", suffixes=("", "_main"))
            if "neg_main" in df_c.columns:
                df_c["neg"] = df_c["neg_main"].fillna(df_c.get("neg", ""))
                df_c.drop(columns=["neg_main"], inplace=True, errors="ignore")

    if "subneg" not in df_c.columns or df_c["subneg"].isna().all():
        if not df_main.empty and "subneg" in df_main.columns and desc_col in df_main.columns:
            series_sub = df_main[[desc_col, "subneg"]].drop_duplicates(subset=[desc_col])
            df_c = pd.merge(df_c, series_sub, on=desc_col, how="left", suffixes=("", "_main"))
            if "subneg_main" in df_c.columns:
                df_c["subneg"] = df_c["subneg_main"].fillna(df_c.get("subneg", ""))
                df_c.drop(columns=["subneg_main"], inplace=True, errors="ignore")

    # Fill missing neg/subneg
    if "neg" not in df_c.columns:
        df_c["neg"] = negocio_top
    else:
        df_c["neg"] = df_c["neg"].fillna(negocio_top).astype(str)

    if "subneg" not in df_c.columns:
        df_c["subneg"] = "General"
    else:
        df_c["subneg"] = df_c["subneg"].fillna("General").astype(str)

    # Unit of measure
    um_col = "unidad_medida" if "unidad_medida" in df_c.columns else None

    # Pivot values
    val_col_detail = "yhat_cliente" if "yhat_cliente" in df_c.columns else "monto_yhat"

    # Price per product (from precio_base or precio_ajustado in valorizado, or from price_lookup)
    if "precio_base" in df_c.columns:
        price_map = df_c.groupby(desc_col)["precio_base"].first().to_dict()
    elif "precio_ajustado" in df_c.columns:
        price_map = df_c.groupby(desc_col)["precio_ajustado"].first().to_dict()
    else:
        price_map = {}

    # Date range
    sorted_dates = sorted(df_c["fecha"].dropna().unique())
    col_labels = [pd.Timestamp(d).strftime("%b %Y").title() for d in sorted_dates]

    # Growth calc params
    max_hist_date = None
    if not df_main.empty and "tipo" in df_main.columns:
        hist_dates = df_main[df_main["tipo"] == "hist"]["fecha"]
        if not hist_dates.empty:
            max_hist_date = hist_dates.max()

    # Build grouped structure: neg → subneg → products
    neg_groups = {}
    for neg_name in sorted(df_c["neg"].unique()):
        df_neg = df_c[df_c["neg"] == neg_name]
        subneg_groups = {}
        for subneg_name in sorted(df_neg["subneg"].unique()):
            df_sub = df_neg[df_neg["subneg"] == subneg_name]
            # Pivot: product × date
            idx_cols = [desc_col]
            if um_col:
                idx_cols.append(um_col)

            piv = df_sub.groupby(idx_cols + ["fecha"])[val_col_detail].sum().reset_index()
            pivot = piv.set_index(idx_cols + ["fecha"])[val_col_detail].unstack("fecha").fillna(0)
            pivot = pivot.reindex(columns=sorted_dates, fill_value=0)

            product_rows = []
            for idx_val, row_data in pivot.iterrows():
                if isinstance(idx_val, tuple):
                    prod_name = str(idx_val[0])
                    um = str(idx_val[1]) if len(idx_val) > 1 else "Unid."
                else:
                    prod_name = str(idx_val)
                    um = "Unid."

                unit_price = float(price_map.get(prod_name, 0))

                months = []
                for d, label in zip(sorted_dates, col_labels):
                    orig = round(float(row_data.get(d, 0)))
                    nuevo = orig
                    pct = 0.0
                    if growth_pct != 0 and max_hist_date is not None and pd.Timestamp(d) > max_hist_date:
                        t = (pd.Timestamp(d).year - max_hist_date.year) * 12 + (pd.Timestamp(d).month - max_hist_date.month)
                        ra = growth_pct / 100.0
                        rm = (1 + ra) ** (1/12.0) - 1
                        factor = (1 + rm) ** t
                        nuevo = round(orig * factor)
                        pct = round(rm * 100, 1)
                    monto = round(nuevo * unit_price)
                    months.append({
                        "label": label,
                        "orig": orig,
                        "nuevo": nuevo,
                        "pct": pct,
                        "monto": monto,
                    })
                product_rows.append({
                    "producto": prod_name,
                    "um": um,
                    "unit_price": unit_price,
                    "months": months,
                })

            product_rows.sort(key=lambda p: p["producto"])
            subneg_groups[subneg_name] = product_rows

        neg_count = sum(len(prods) for prods in subneg_groups.values())
        neg_groups[neg_name] = {
            "negocio": neg_name,
            "count": neg_count,
            "subnegocios": [
                {"subnegocio": sn, "products": prods}
                for sn, prods in subneg_groups.items()
            ],
        }

    total_products = sum(ng["count"] for ng in neg_groups.values())

    return {
        "client": cliente_display,
        "perfil": perfil,
        "negocio": negocio_top,
        "columns": col_labels,
        "negocios": list(neg_groups.values()),
        "n_products": total_products,
        "growth_pct": growth_pct,
    }

