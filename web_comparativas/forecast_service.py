"""
forecast_service.py — Native Forecast data loader & processor via SQLAlchemy.
Fetches data from PostgreSQL instead of loading massive CSVs into memory, preventing OOM.
"""
import os
import gc
import logging
import datetime as dt
from pathlib import Path
from typing import Dict, List, Any, Optional, Tuple
import pandas as pd
import numpy as np

from web_comparativas.models import db_session
from web_comparativas.forecast_models import (
    ForecastBase, ForecastValorizado, ForecastArticulo, ForecastNegocio, ForecastCliente
)
from sqlalchemy import select, func, and_, desc

logger = logging.getLogger("wc.forecast")

# ---------------------------------------------------------------------------
# Filter Data
# ---------------------------------------------------------------------------

def is_available() -> bool:
    """Check if forecast data exists in the database."""
    session = db_session()
    try:
        count = session.execute(select(func.count(ForecastBase.id))).scalar()
        return (count or 0) > 0
    except Exception as e:
        logger.error(f"Error checking availability: {e}")
        return False
    finally:
        session.close()


def get_filter_options() -> Dict[str, Any]:
    session = db_session()
    try:
        profiles = session.execute(
            select(ForecastBase.perfil).distinct().where(ForecastBase.perfil.isnot(None))
        ).scalars().all()
        profiles = sorted([p for p in profiles if str(p).strip()])

        negs = session.execute(
            select(ForecastBase.neg).distinct().where(ForecastBase.neg.isnot(None))
        ).scalars().all()
        negs = sorted([int(n) for n in negs if n is not None])

        subnegs = session.execute(
            select(ForecastBase.subneg).distinct().where(ForecastBase.subneg.isnot(None))
        ).scalars().all()
        subnegs = sorted([int(s) for s in subnegs if s is not None])

        # Get all articles to map description -> codigos -> labs
        # To avoid massive joins, we can use df to load the small dimension tables
        # or just fetch what we need
        query = select(
            ForecastBase.codigo_serie, 
            ForecastBase.neg, 
            ForecastBase.subneg,
            func.sum( (func.coalesce(ForecastBase.y, 0) + func.coalesce(ForecastBase.yhat, 0)) * func.coalesce(ForecastBase.precio, 0) ).label("vol")
        ).group_by(ForecastBase.codigo_serie, ForecastBase.neg, ForecastBase.subneg)
        
        base_stats = pd.read_sql(query, session.bind)
        
        # Get descriptions and labs from Articulo
        art_query = select(ForecastArticulo.codigo, ForecastArticulo.descrip, ForecastArticulo.laboratorio_descrip)
        art_df = pd.read_sql(art_query, session.bind)
        
        if not base_stats.empty and not art_df.empty:
            # Merge to get description and lab
            merged = pd.merge(base_stats, art_df, left_on="codigo_serie", right_on="codigo", how="left")
            merged["descrip"] = merged["descrip"].fillna(merged["codigo_serie"])
            
            # Map products
            products_dict = {}
            product_negocio_map = {}
            plm_json = {}
            all_labs = set()
            
            # Group by description
            for desc, group in merged.groupby("descrip"):
                desc_str = str(desc).strip()
                if not desc_str or desc_str == "nan": continue
                
                # Get labs
                labs = set(str(l).strip() for l in group["laboratorio_descrip"].dropna().unique() if str(l).strip())
                if labs:
                    products_dict[desc_str] = ", ".join(sorted(labs))
                    plm_json[desc_str] = sorted(labs)
                    all_labs.update(labs)
                else:
                    products_dict[desc_str] = "SIN LABORATORIO"
                    plm_json[desc_str] = []
                    
                # Get neg/subneg (take first)
                row = group.iloc[0]
                neg_val = str(int(row["neg"])) if pd.notna(row["neg"]) else ""
                subneg_val = str(int(row["subneg"])) if pd.notna(row["subneg"]) else ""
                product_negocio_map[desc_str] = {"neg": neg_val, "subneg": subneg_val}

            # Build list and sort by volume
            vol_series = merged.groupby("descrip")["vol"].sum()
            products_list = []
            for k, v in products_dict.items():
                products_list.append({"id": k, "lab": v, "vol": float(vol_series.get(k, 0))})
            products_list = sorted(products_list, key=lambda x: x["vol"], reverse=True)
            
            laboratorios = ["ALL"] + sorted(list(all_labs))
        else:
            products_list = []
            laboratorios = ["ALL"]
            plm_json = {}
            product_negocio_map = {}

        # Dates
        min_d = session.execute(select(func.min(ForecastBase.fecha))).scalar()
        max_d = session.execute(select(func.max(ForecastBase.fecha))).scalar()
        hist_max = session.execute(select(func.max(ForecastBase.fecha)).where(ForecastBase.tipo == "hist")).scalar()
        
        min_date_str = str(min_d.date()) if min_d else ""
        max_date_str = str(max_d.date()) if max_d else ""
        
        default_start_str = ""
        default_end_str = ""
        
        if hist_max:
            last_proj_year = hist_max.year + 1
            default_start = pd.Timestamp(year=hist_max.year, month=1, day=1)
            default_end = min(pd.Timestamp(max_d), pd.Timestamp(year=last_proj_year, month=12, day=31))
            default_start_str = str(default_start.date())
            default_end_str = str(default_end.date())

        return {
            "profiles": profiles,
            "negocios": negs,
            "subnegocios": subnegs,
            "laboratorios": laboratorios,
            "products": products_list,
            "product_lab_map": plm_json,
            "product_negocio_map": product_negocio_map,
            "min_date": min_date_str,
            "max_date": max_date_str,
            "default_start_date": default_start_str,
            "default_end_date": default_end_str
        }

    except Exception as e:
        logger.error(f"Error in get_filter_options: {e}")
        return {"profiles": [], "negocios": [], "subnegocios": [], "laboratorios": [], "products": [], "product_lab_map": {}, "min_date": "", "max_date": ""}
    finally:
        session.close()

# ---------------------------------------------------------------------------
# Queries for Chart & Tables
# ---------------------------------------------------------------------------

def _build_base_filter(model, start_date: str, end_date: str, profiles: List[str], negocios: List[str], subnegocios: List[str]):
    """Build SQLAlchemy filters based on common dimensions."""
    filters = []
    if start_date:
        filters.append(model.fecha >= pd.to_datetime(start_date))
    if end_date:
        filters.append(model.fecha <= pd.to_datetime(end_date))
    if profiles:
        filters.append(model.perfil.in_(profiles))
    if negocios:
        # DB Neg is integer
        negs_int = [int(x) for x in negocios if str(x).isdigit()]
        if negs_int:
            filters.append(model.neg.in_(negs_int))
    if subnegocios:
        subnegs_int = [int(x) for x in subnegocios if str(x).isdigit()]
        if subnegs_int:
            filters.append(model.subneg.in_(subnegs_int))
    return filters

def _build_valorizado_filter(model, start_date: str, end_date: str, profiles: List[str]):
    filters = []
    if start_date:
        filters.append(model.fecha >= pd.to_datetime(start_date))
    if end_date:
        filters.append(model.fecha <= pd.to_datetime(end_date))
    if profiles:
        filters.append(model.perfil.in_(profiles))
    return filters

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
    session = db_session()
    try:
        # Build query
        filters = _build_base_filter(ForecastBase, start_date, end_date, profiles, negocios, subnegocios)
        query = select(ForecastBase).where(and_(*filters))
        
        # Load to pandas for rapid aggregation
        df = pd.read_sql(query, session.bind)
        if df.empty:
            return {"history": [], "forecast": [], "forecast_adj": [], "ci_upper": [], "ci_lower": []}

        # Filter products via description from Articulos if needed
        if products:
            art_query = select(ForecastArticulo.codigo, ForecastArticulo.descrip)
            art_df = pd.read_sql(art_query, session.bind)
            art_df["descrip"] = art_df["descrip"].fillna(art_df["codigo"])
            
            # Sub-filter the codes that match the descriptions
            valid_codes = art_df[art_df["descrip"].isin(products)]["codigo"].tolist()
            # If a product is not found in article table, its code is exactly the description
            valid_codes.extend(products)
            
            df = df[df["codigo_serie"].isin(valid_codes)]
            if df.empty:
                return {"history": [], "forecast": [], "forecast_adj": [], "ci_upper": [], "ci_lower": []}

        # Value mapping
        if view_money:
            df["y_val"] = df["y"].fillna(0) * df["precio"]
            df["yhat_val"] = df["yhat"].fillna(0) * df["precio"]
            df["li_val"] = df["li"].fillna(0) * df["precio"]
            df["ls_val"] = df["ls"].fillna(0) * df["precio"]
        else:
            df["y_val"] = df["y"].fillna(0)
            df["yhat_val"] = df["yhat"].fillna(0)
            df["li_val"] = df["li"].fillna(0)
            df["ls_val"] = df["ls"].fillna(0)

        # Aggregate by date, tipo
        grouped = df.groupby(["fecha", "tipo"])[["y_val", "yhat_val", "li_val", "ls_val"]].sum().reset_index()

        df_hist = grouped[grouped["tipo"] == "hist"].sort_values("fecha")
        df_fore = grouped[grouped["tipo"] == "forecast"].sort_values("fecha")

        # Format arrays for Plotly
        history = [{"x": str(row["fecha"].date()), "y": round(float(row["y_val"]), 2)} for _, row in df_hist.iterrows()]
        
        forecast = [{"x": str(row["fecha"].date()), "y": round(float(row["yhat_val"]), 2)} for _, row in df_fore.iterrows()]
        ci_lower = [{"x": str(row["fecha"].date()), "y": round(float(row["li_val"]), 2)} for _, row in df_fore.iterrows()]
        ci_upper = [{"x": str(row["fecha"].date()), "y": round(float(row["ls_val"]), 2)} for _, row in df_fore.iterrows()]

        forecast_adj = []
        if growth_pct != 0 and forecast:
            factor = 1.0 + (growth_pct / 100.0)
            forecast_adj = [{"x": pt["x"], "y": round(pt["y"] * factor, 2)} for pt in forecast]

        hist_max_date = str(df_hist["fecha"].max().date()) if not df_hist.empty else ""

        return {
            "history": history,
            "forecast": forecast,
            "forecast_adj": forecast_adj,
            "ci_upper": ci_upper,
            "ci_lower": ci_lower,
            "hist_max_date": hist_max_date,
            "growth_pct": growth_pct,
        }
    except Exception as e:
        logger.error(f"Error in get_chart_data: {e}")
        return {"history": [], "forecast": [], "forecast_adj": [], "ci_upper": [], "ci_lower": []}
    finally:
        session.close()

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
    session = db_session()
    try:
        filters = _build_valorizado_filter(ForecastValorizado, start_date, end_date, profiles)
        query = select(ForecastValorizado).where(and_(*filters))
        df_val = pd.read_sql(query, session.bind)
        
        if df_val.empty:
            return {"columns": [], "groups": [], "grand_totals": {}}

        # Also need base data for Neg/Subneg filtering if applicable
        if negocios or subnegocios or products:
            base_filters = _build_base_filter(ForecastBase, start_date, end_date, profiles, negocios, subnegocios)
            base_query = select(ForecastBase.codigo_serie).where(and_(*base_filters)).distinct()
            valid_codes = pd.read_sql(base_query, session.bind)["codigo_serie"].tolist()
            
            if products:
                art_query = select(ForecastArticulo.codigo, ForecastArticulo.descrip)
                art_df = pd.read_sql(art_query, session.bind)
                art_df["descrip"] = art_df["descrip"].fillna(art_df["codigo"])
                prod_codes = art_df[art_df["descrip"].isin(products)]["codigo"].tolist()
                prod_codes.extend(products)
                
                # intersection
                valid_codes = list(set(valid_codes).intersection(set(prod_codes)))
                
            df_val = df_val[df_val["codigo_serie"].isin(valid_codes)]
            if df_val.empty:
                return {"columns": [], "groups": [], "grand_totals": {}}

        # Get client names + groups from Cliente table
        cli_query = select(ForecastCliente.codigo, ForecastCliente.nombre, ForecastCliente.fantasia, ForecastCliente.nombre_grupo)
        df_cli = pd.read_sql(cli_query, session.bind)
        
        # Ensure correct types for join
        df_val["cliente_id"] = df_val["cliente_id"].astype(str).str.strip()
        df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
        df_cli["fantasia"] = df_cli["fantasia"].fillna(df_cli["codigo"])
        df_cli["nombre_grupo"] = df_cli["nombre_grupo"].fillna("SIN GRUPO")
        
        # Merge names into val
        df_c = pd.merge(df_val, df_cli, left_on="cliente_id", right_on="codigo", how="left")
        
        val_col = "monto_yhat" if view_money else "yhat_cliente"
        if val_col not in df_c.columns:
            val_col = "yhat_cliente" if "yhat_cliente" in df_c.columns else "monto_yhat"

        # Groups logic
        df_c["_cli"] = df_c["fantasia"].fillna(df_c["cliente_id"])
        df_c["_grp"] = df_c["nombre_grupo"].fillna("")
        mask_sg = df_c["_grp"] == "SIN GRUPO"
        mask_sr = df_c["_cli"] == df_c["_grp"]
        df_c.loc[mask_sg | mask_sr, "_grp"] = ""
        df_c["_cli_id"] = df_c["cliente_id"].astype(str)

        factor = 1.0 + (growth_pct / 100.0)

        # Pivot
        piv = df_c.groupby(["_cli", "_grp", "_cli_id", "fecha"])[val_col].sum().reset_index()
        if piv.empty:
            return {"columns": [], "groups": [], "grand_totals": {}}

        piv["fecha"] = piv["fecha"].dt.strftime("%Y-%m")
        # Apply factor
        piv[val_col] = piv[val_col] * factor

        pivot = piv.set_index(["_cli", "_grp", "_cli_id", "fecha"])[val_col].unstack("fecha").fillna(0)
        
        col_names = sorted(pivot.columns.tolist())
        pivot["Total"] = pivot.sum(axis=1)

        result_groups = {}
        min_val = float('inf')
        max_val = float('-inf')

        for idx, row in pivot.iterrows():
            cli_name, grp_name, cli_id = idx
            group_key = grp_name if grp_name else cli_name

            if group_key not in result_groups:
                result_groups[group_key] = {
                    "group_name": group_key,
                    "is_group": bool(grp_name),
                    "clients": [],
                    "totals": {c: 0.0 for c in col_names},
                    "group_total": 0.0,
                }

            cli_dict = {"cliente": cli_name, "id": cli_id}
            for c in col_names:
                val = float(row[c])
                cli_dict[c] = val
                result_groups[group_key]["totals"][c] += val
                result_groups[group_key]["group_total"] += val
                if val > max_val: max_val = val
                if val < min_val: min_val = val

            cli_dict["Total"] = float(row["Total"])
            if result_groups[group_key]["is_group"]:
                result_groups[group_key]["clients"].append(cli_dict)

        merged_groups = []
        grand_totals = {c: 0.0 for c in col_names}
        grand_totals["Total"] = 0.0

        for g in result_groups.values():
            for c in col_names:
                grand_totals[c] += g["totals"][c]
            grand_totals["Total"] += g["group_total"]

            if g["is_group"]:
                g["clients"] = sorted(g["clients"], key=lambda x: x["Total"], reverse=True)
            merged_groups.append(g)

        merged_groups = sorted(merged_groups, key=lambda x: x["group_total"], reverse=True)
        grand_totals_base = {k: v / factor for k, v in grand_totals.items()} if factor != 0 else grand_totals.copy()

        return {
            "columns": col_names,
            "groups": merged_groups,
            "grand_totals": grand_totals,
            "grand_totals_base": grand_totals_base,
            "growth_pct": growth_pct,
            "min_val": min_val if min_val != float('inf') else 0,
            "max_val": max_val if max_val != float('-inf') else 0,
            "view_money": view_money,
        }
    except Exception as e:
        logger.error(f"Error in get_client_table: {e}")
        return {"columns": [], "groups": [], "grand_totals": {}}
    finally:
        session.close()


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
    session = db_session()
    try:
        # Same exact filter logic as the table
        filters = _build_valorizado_filter(ForecastValorizado, start_date, end_date, profiles)
        query = select(ForecastValorizado).where(and_(*filters))
        df_val = pd.read_sql(query, session.bind)
        
        if df_val.empty:
            return {"client": cliente_display, "negocios": []}

        df_val["cliente_id"] = df_val["cliente_id"].astype(str).str.strip()
        df_val = df_val[df_val["cliente_id"] == str(cliente_display).strip()]
        
        if df_val.empty:
            return {"client": cliente_display, "negocios": []}

        # Need Neg/Subneg mappings to detail by product
        base_query = select(ForecastBase.codigo_serie, ForecastBase.neg, ForecastBase.subneg).distinct()
        df_base_dim = pd.read_sql(base_query, session.bind)
        
        # Need Neg/Subneg descriptors
        neg_query = select(ForecastNegocio.unidad, ForecastNegocio.subunidad, ForecastNegocio.descrip)
        df_neg = pd.read_sql(neg_query, session.bind)
        
        # Merge dimensions to Val
        df_c = pd.merge(df_val, df_base_dim, on="codigo_serie", how="left")
        
        # Filters
        if negocios:
            negs_int = [int(x) for x in negocios if str(x).isdigit()]
            df_c = df_c[df_c["neg"].isin(negs_int)]
        if subnegocios:
            subnegs_int = [int(x) for x in subnegocios if str(x).isdigit()]
            df_c = df_c[df_c["subneg"].isin(subnegs_int)]

        if products:
            art_query = select(ForecastArticulo.codigo, ForecastArticulo.descrip)
            art_df = pd.read_sql(art_query, session.bind)
            art_df["descrip"] = art_df["descrip"].fillna(art_df["codigo"])
            prod_codes = art_df[art_df["descrip"].isin(products)]["codigo"].tolist()
            prod_codes.extend(products)
            df_c = df_c[df_c["codigo_serie"].isin(prod_codes)]

        if df_c.empty:
            return {"client": cliente_display, "negocios": []}

        # Join product descriptions
        art_query = select(ForecastArticulo.codigo, ForecastArticulo.descrip)
        art_df = pd.read_sql(art_query, session.bind)
        df_c = pd.merge(df_c, art_df, left_on="codigo_serie", right_on="codigo", how="left")
        df_c["producto"] = df_c["descrip"].fillna(df_c["codigo_serie"])

        # Join Neg descriptor
        df_neg_main = df_neg[df_neg["subunidad"] == 0].copy()
        df_c = pd.merge(df_c, df_neg_main, left_on="neg", right_on="unidad", how="left")
        df_c.rename(columns={"descrip_y": "negocio_desc", "descrip_x": "descrip"}, inplace=True)
        df_c["negocio_desc"] = df_c["negocio_desc"].fillna("Sin Negocio")

        # Pivot Negocio -> Producto
        factor = 1.0 + (growth_pct / 100.0)
        df_c["yhat"] = df_c["monto_yhat"] * factor

        idx_cols = ["negocio_desc", "producto"]
        piv = df_c.groupby(idx_cols + ["fecha"])["yhat"].sum().reset_index()
        piv["fecha"] = piv["fecha"].dt.strftime("%Y-%m")

        pivot = piv.set_index(idx_cols + ["fecha"])["yhat"].unstack("fecha").fillna(0)
        col_names = sorted(pivot.columns.tolist())
        pivot["Total"] = pivot.sum(axis=1)
        pivot = pivot.reset_index()

        result_negs = []
        for neg, group in pivot.groupby("negocio_desc"):
            products_arr = []
            neg_totals = {c: 0.0 for c in col_names}
            neg_totals["Total"] = 0.0

            for _, row in group.iterrows():
                p_dict = {"producto": row["producto"]}
                for c in col_names:
                    val = float(row[c])
                    p_dict[c] = val
                    neg_totals[c] += val
                p_total = float(row["Total"])
                p_dict["Total"] = p_total
                neg_totals["Total"] += p_total
                products_arr.append(p_dict)

            products_arr = sorted(products_arr, key=lambda x: x["Total"], reverse=True)
            result_negs.append({
                "negocio": neg,
                "productos": products_arr,
                "totals": neg_totals
            })

        result_negs = sorted(result_negs, key=lambda x: x["totals"]["Total"], reverse=True)
        return {"client": cliente_display, "negocios": result_negs, "columns": col_names}
    except Exception as e:
        logger.error(f"Error in get_client_detail: {e}")
        return {"client": cliente_display, "negocios": [], "columns": []}
    finally:
        session.close()

