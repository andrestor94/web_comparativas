"""
Forecast data service — adapted from "Forecast ultimo/dashboard/data_loader.py"
Pure Python/pandas, zero Streamlit dependencies.
"""
from __future__ import annotations

import json
import logging
import threading
import time
from functools import wraps
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from web_comparativas.models import engine
except ImportError:
    engine = None

try:
    from sqlalchemy import text as _sa_text
except ImportError:
    _sa_text = None  # type: ignore[assignment]

logger = logging.getLogger("wc.forecast")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
# Now using inline data folder packaged for the deployed repository.
FORECAST_DIR = BASE_DIR / "data" / "forecast_data"

# Original Forecast directory
_ORIG_FORECAST_DIR = FORECAST_DIR
# Canonical slim parquet (9MB, 702K rows, monto_yhat+monto_li+monto_ls pre-computed, $121.7B)
# This is the authoritative source for both local CSV mode and the PostgreSQL migration.
_VALORIZADO_PARQUET  = FORECAST_DIR / "fact_forecast_valorizado.parquet"
# Legacy CSV path (kept for backward compat — parquet is preferred)
_VALORIZADO_PREPARED = _ORIG_FORECAST_DIR / "fact_forecast_valorizado.csv"
# Fallback: incomplete copy (110K rows, 1838 series, ~$52B — DO NOT USE for production)
_VALORIZADO_FALLBACK = FORECAST_DIR / "forecast_valorizado_v2.csv"

FORECAST_FILE   = FORECAST_DIR / "forecast_base_consolidado.csv"
MASTER_FILE     = FORECAST_DIR / "Articulos 1.csv"
NEGOCIOS_FILE   = FORECAST_DIR / "Negocios.csv"
SERIES_FILE     = FORECAST_DIR / "dataset_base.csv"
ARTICULOS_FILE  = FORECAST_DIR / "Articulos 1.csv"
VALORIZADO_FILE = FORECAST_DIR / "forecast_valorizado_v2.csv"
CLIENTES_FILE   = FORECAST_DIR / "clientes.csv"
IMP_HIST_FILE   = FORECAST_DIR / "importe_historico.csv"
FACT_2026_FILE  = FORECAST_DIR / "facturacion_real_2026.csv"

_cache_lock = threading.Lock()
_data_cache: dict[str, Any] = {}

# ---------------------------------------------------------------------------
# Client override store (in-memory persistence of modal edits)
# Stores the % adjustment per (client_id, articulo, date_str).
# factor = 1 + pct/100  →  nuevo_yhat = orig_yhat * factor
# Key: client_id → {(articulo, date_str): pct_float}
# ---------------------------------------------------------------------------
_overrides_lock = threading.Lock()
_client_overrides: dict[str, dict[tuple, float]] = {}

# ---------------------------------------------------------------------------
# Service-level TTL response cache
# Caches the serialisable dict/list returned by each public get_* function.
# Key: function name + normalised filter args (lists → sorted for stable keys).
# TTL: 5 min for data, 15 min for filter-options (rarely changes mid-session).
# Cleared on: a) reload_data(), b) save_client_overrides() (overrides alter data).
# ---------------------------------------------------------------------------
_resp_cache: dict[str, tuple[float, Any]] = {}
_resp_cache_lock = threading.Lock()
_RESP_TTL_DATA   = 300   # 5 min — chart, table, treemap, product-list
_RESP_TTL_STATIC = 900   # 15 min — filter-options


def _resp_key(fn_name: str, *args, **kwargs) -> str:
    """Stable JSON key from fn name + args (lists normalised to sorted for consistency)."""
    def _norm(v):
        if isinstance(v, list):
            return sorted(str(x) for x in v if x is not None)
        return v
    return json.dumps(
        [fn_name, [_norm(a) for a in args], {k: _norm(v) for k, v in sorted(kwargs.items())}],
        default=str,
    )


def _resp_get(key: str, ttl: float) -> "Any | None":
    with _resp_cache_lock:
        entry = _resp_cache.get(key)
    if entry is None:
        return None
    ts, value = entry
    return value if (time.monotonic() - ts) < ttl else None


def _resp_set(key: str, value: Any) -> None:
    with _resp_cache_lock:
        _resp_cache[key] = (time.monotonic(), value)


def clear_response_cache() -> None:
    """Flush the service-level response cache (after reload or client-save)."""
    with _resp_cache_lock:
        _resp_cache.clear()
    logger.info("[FORECAST cache] Response cache cleared.")


def _with_resp_cache(ttl: float):
    """Decorator: transparently cache the return value of a public get_* function."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            key = _resp_key(fn.__name__, *args, **kwargs)
            cached = _resp_get(key, ttl)
            if cached is not None:
                logger.debug("[FORECAST cache] HIT  %s", fn.__name__)
                return cached
            logger.debug("[FORECAST cache] MISS %s", fn.__name__)
            result = fn(*args, **kwargs)
            _resp_set(key, result)
            return result
        return wrapper
    return decorator


def save_client_overrides(client_id: str, overrides: list[dict]) -> None:
    """Persist per-product % adjustments for a client.
    Each override: {articulo: str, date: 'YYYY-MM', pct: float}
    """
    with _overrides_lock:
        store = _client_overrides.setdefault(client_id, {})
        for ov in overrides:
            key = (str(ov["articulo"]), str(ov["date"]))
            store[key] = float(ov["pct"])
    # Overrides alter the projected data — flush cached responses so the next
    # request reflects the change rather than serving stale aggregates.
    clear_response_cache()


def clear_client_overrides(client_id: str) -> None:
    """Remove all overrides for a client (full undo to CSV baseline)."""
    with _overrides_lock:
        _client_overrides.pop(client_id, None)
    clear_response_cache()


def _get_client_overrides_snapshot(client_id: str) -> dict:
    """Return the stored pct overrides for a specific client (read-only copy)."""
    with _overrides_lock:
        return dict(_client_overrides.get(client_id, {}))


def _get_patched_df_val(df_source=None) -> "pd.DataFrame":
    """Return df_valorizado with all saved overrides applied (copy — never mutates cache)."""
    if df_source is not None:
        df = df_source
    else:
        data = get_data()
        df = data.get("df_valorizado", None)
    if df is None or df.empty:
        return df if df is not None else __import__("pandas").DataFrame()
    with _overrides_lock:
        has_overrides = bool(_client_overrides)
    if not has_overrides:
        return df  # No copy needed — all callers only read or filter/copy filtered subsets

    df = df.copy()
    cli_col = "fantasia" if "fantasia" in df.columns else "cliente_id"
    if cli_col not in df.columns or "articulo" not in df.columns:
        return df

    df["_ds"] = df["fecha"].dt.strftime("%Y-%m")
    with _overrides_lock:
        overrides_snapshot = {cid: dict(ov) for cid, ov in _client_overrides.items()}

    for client_id, store in overrides_snapshot.items():
        if not store:
            continue
        cli_mask = df[cli_col] == client_id
        for (articulo, date_str), pct in store.items():
            factor = 1.0 + pct / 100.0
            mask = cli_mask & (df["articulo"] == articulo) & (df["_ds"] == date_str)
            if not mask.any():
                continue
            df.loc[mask, "yhat_cliente"] = df.loc[mask, "yhat_cliente"] * factor
            if "monto_yhat" in df.columns:
                df.loc[mask, "monto_yhat"] = df.loc[mask, "monto_yhat"] * factor

    df.drop(columns=["_ds"], inplace=True, errors="ignore")
    return df


# ---------------------------------------------------------------------------
# Helpers
# ---------------------------------------------------------------------------

def _norm_key(s: str) -> str:
    return " ".join(str(s).split()).upper()


def _get_col_ci(df: pd.DataFrame, name: str) -> str | None:
    for c in df.columns:
        if c.lower() == name.lower():
            return c
    return None


def _apply_neg_names(df: pd.DataFrame, negocios_path: Path) -> pd.DataFrame:
    if not negocios_path.exists() or df.empty:
        return df
    try:
        df_neg = pd.read_csv(str(negocios_path))
        df_neg.columns = [c.upper().strip() for c in df_neg.columns]
        if not all(c in df_neg.columns for c in ["UNIDAD", "DESCRIP", "SUBUNIDAD"]):
            return df
        df_neg["UNIDAD"] = pd.to_numeric(df_neg["UNIDAD"], errors="coerce").fillna(0).astype(int)
        df_neg["SUBUNIDAD"] = pd.to_numeric(df_neg["SUBUNIDAD"], errors="coerce").fillna(0).astype(int)

        map_neg = (
            df_neg[df_neg["SUBUNIDAD"] == 0][["UNIDAD", "DESCRIP"]]
            .drop_duplicates("UNIDAD")
            .rename(columns={"DESCRIP": "_neg_nombre"})
        )
        map_sub = (
            df_neg[df_neg["SUBUNIDAD"] != 0][["UNIDAD", "SUBUNIDAD", "DESCRIP"]]
            .drop_duplicates(["UNIDAD", "SUBUNIDAD"])
            .rename(columns={"DESCRIP": "_subneg_nombre"})
        )

        if "neg" in df.columns:
            df["_neg_id"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
            df = pd.merge(df, map_neg, left_on="_neg_id", right_on="UNIDAD", how="left")
            df.drop(columns=["UNIDAD"], inplace=True, errors="ignore")

        if "neg" in df.columns and "subneg" in df.columns:
            if "_neg_id" not in df.columns:
                df["_neg_id"] = pd.to_numeric(df["neg"], errors="coerce").fillna(0).astype(int)
            df["_subneg_id"] = pd.to_numeric(df["subneg"], errors="coerce").fillna(0).astype(int)
            df = pd.merge(df, map_sub, left_on=["_neg_id", "_subneg_id"], right_on=["UNIDAD", "SUBUNIDAD"], how="left")
            df.drop(columns=["UNIDAD", "SUBUNIDAD"], inplace=True, errors="ignore")

        if "_neg_nombre" in df.columns:
            df["neg"] = df["_neg_nombre"].fillna(df["neg"])
            df.drop(columns=["_neg_nombre", "_neg_id"], inplace=True, errors="ignore")
        if "_subneg_nombre" in df.columns:
            df["subneg"] = df["_subneg_nombre"].fillna(df["subneg"])
            df.drop(columns=["_subneg_nombre", "_subneg_id"], inplace=True, errors="ignore")
    except Exception as exc:
        logger.warning("Negocios merge error: %s", exc)
    return df


def _process_dataframe(df_input: pd.DataFrame, df_meta: pd.DataFrame) -> pd.DataFrame:
    if df_input.empty:
        return df_input
    df_input.columns = [c.lower().strip() for c in df_input.columns]

    if "codigo_serie" in df_input.columns and "articulo" not in df_input.columns:
        df_input["articulo"] = df_input["codigo_serie"].astype(str)
    else:
        df_input["articulo"] = df_input.get("articulo", pd.Series(dtype="str")).astype(str)

    df = df_input.copy()

    if not df_meta.empty:
        col_art = df_meta.columns[0]
        col_fam = "Familia" if "Familia" in df_meta.columns else None
        col_desc = "Descrip_art" if "Descrip_art" in df_meta.columns else None

        df = pd.merge(df, df_meta, left_on="articulo", right_on=col_art, how="left")

        if "codigo_serie" in df.columns:
            df["descripcion"] = df["codigo_serie"]
        elif col_fam and col_fam in df.columns:
            df["descripcion"] = df[col_fam]
        else:
            df["descripcion"] = pd.NA

        if col_desc and col_desc in df.columns:
            df["descripcion"] = df["descripcion"].replace(["", "nan"], pd.NA).fillna(df[col_desc])

        df["descripcion"] = df["descripcion"].replace(["", "nan"], pd.NA).fillna(df["articulo"])
    else:
        df["descripcion"] = df["articulo"]

    if "periodo" in df.columns:
        df["fecha"] = pd.to_datetime(df["periodo"], format="%Y-%m", errors="coerce")

    return df


def _build_price_lookup(articulos_file: Path) -> dict:
    price_lookup: dict[str, dict] = {"ARTICULO": {}, "FAMILIA": {}, "CODIGO": {}}
    if not articulos_file.exists():
        return price_lookup
    try:
        df = pd.read_csv(str(articulos_file), sep=",", encoding="latin-1", dtype=str)
        df.columns = [c.strip().lower() for c in df.columns]

        def parse_float(x):
            try:
                return float(str(x).replace(",", "."))
            except Exception:
                return 0.0

        def parse_int(x):
            try:
                return max(1, int(float(str(x).replace(",", "."))))
            except Exception:
                return 1

        if "descrip" in df.columns and "predrog" in df.columns:
            df["_predrog"] = df["predrog"].apply(parse_float)
            df["_cantenv"] = df.get("cantenv", pd.Series("1", index=df.index)).apply(parse_int)
            df.loc[df["_cantenv"] <= 0, "_cantenv"] = 1
            df["unit_price"] = df["_predrog"]
            mask_pack = df["_cantenv"] > 1
            df.loc[mask_pack, "unit_price"] = df.loc[mask_pack, "_predrog"] / df.loc[mask_pack, "_cantenv"]

            df["_descrip_norm"] = df["descrip"].apply(_norm_key)
            price_lookup["ARTICULO"] = df.groupby("_descrip_norm")["unit_price"].mean().to_dict()

            if "codigo" in df.columns:
                df["_cod_norm"] = df["codigo"].apply(_norm_key)
                price_lookup["CODIGO"] = df.groupby("_cod_norm")["unit_price"].mean().to_dict()

            if "familia" in df.columns:
                df["_fam_norm"] = df["familia"].apply(_norm_key)
                mask_fam = (df["_fam_norm"] != "NAN") & (df["_fam_norm"] != "")
                price_lookup["FAMILIA"] = df[mask_fam].groupby("_fam_norm")["unit_price"].mean().to_dict()
    except Exception as exc:
        logger.warning("Price lookup build error: %s", exc)
    return price_lookup


def _apply_prices(df: pd.DataFrame, price_lookup: dict) -> pd.DataFrame:
    if df.empty:
        return df
    df = df.copy()
    df["precio"] = 0.0
    df["_kn"] = df["articulo"].apply(_norm_key)

    if "CODIGO" in price_lookup:
        df["precio"] = df["_kn"].map(price_lookup["CODIGO"]).fillna(0.0)

    mask0 = df["precio"] == 0
    if "ARTICULO" in price_lookup and mask0.any():
        col_d = "descripcion" if "descripcion" in df.columns else "articulo"
        df.loc[mask0, "_kd"] = df.loc[mask0, col_d].apply(_norm_key)
        df.loc[mask0, "precio"] = df.loc[mask0, "_kd"].map(price_lookup["ARTICULO"]).fillna(0.0)

    mask0 = df["precio"] == 0
    if "FAMILIA" in price_lookup and mask0.any():
        fc = next((c for c in ("familia_x", "familia") if c in df.columns), None)
        if fc:
            df.loc[mask0, "_kf"] = df.loc[mask0, fc].apply(_norm_key)
            df.loc[mask0, "precio"] = df.loc[mask0, "_kf"].map(price_lookup["FAMILIA"]).fillna(0.0)

    df.drop(columns=[c for c in ("_kn", "_kd", "_kf") if c in df.columns], inplace=True)
    return df


# ---------------------------------------------------------------------------
# Main loader
# ---------------------------------------------------------------------------

def _load_all_data() -> dict[str, Any]:
    result: dict[str, Any] = {}

    # ── Meta ──────────────────────────────────────────────────────────────
    df_meta = pd.DataFrame()
    try:
        df_m = pd.read_csv(str(MASTER_FILE), sep=",", encoding="latin-1")
        df_m.columns = [c.strip() for c in df_m.columns]
        col_art = _get_col_ci(df_m, "Articulo1") or _get_col_ci(df_m, "Articulo") or _get_col_ci(df_m, "codigo")
        col_fam = _get_col_ci(df_m, "Familia")
        col_desc = _get_col_ci(df_m, "Descrip_art") or _get_col_ci(df_m, "descrip")
        if col_art:
            df_m[col_art] = df_m[col_art].astype(str)
            cols = [col_art] + ([col_fam] if col_fam else []) + ([col_desc] if col_desc else [])
            df_meta = df_m[cols].drop_duplicates(subset=[col_art], keep="first")
    except Exception as exc:
        logger.warning("Master load error: %s", exc)
    result["df_meta"] = df_meta

    # ── Main forecast ─────────────────────────────────────────────────────
    df_main = pd.DataFrame()
    try:
        df_main = pd.read_csv(str(FORECAST_FILE), sep=";", decimal=",", encoding="utf-8-sig")
        df_main = _process_dataframe(df_main, df_meta)
        df_main = _apply_neg_names(df_main, NEGOCIOS_FILE)
        # Ensure string columns
        for c in ("neg", "subneg"):
            if c in df_main.columns:
                df_main[c] = df_main[c].astype(str)
        # Normalise perfil column
        for raw in ("Perfil", "PERFIL"):
            if raw in df_main.columns:
                df_main.rename(columns={raw: "perfil"}, inplace=True)
                break
    except Exception as exc:
        logger.error("Main forecast load error: %s", exc)
    result["df_main"] = df_main

    # ── Prices ────────────────────────────────────────────────────────────
    price_lookup = _build_price_lookup(ARTICULOS_FILE)
    result["price_lookup"] = price_lookup

    if not df_main.empty:
        df_main = _apply_prices(df_main, price_lookup)
        result["df_main"] = df_main

    # ── Valorizado (etapa 5) ──────────────────────────────────────────────
    # Priority 1: canonical parquet (9MB, 702K rows, $121.7B — correct source)
    # Priority 2: legacy prepared CSV (comma-sep, if parquet absent)
    # DO NOT fall back to forecast_valorizado_v2.csv — it has only 110K rows / $52B
    df_val = pd.DataFrame()
    _val_file = None
    _use_parquet = False
    if _VALORIZADO_PARQUET.exists():
        _use_parquet = True
    elif _VALORIZADO_PREPARED.exists():
        _val_file = _VALORIZADO_PREPARED
    # Intentionally skip VALORIZADO_FILE / _VALORIZADO_FALLBACK — incomplete data

    if _use_parquet or _val_file is not None:
        try:
            if _use_parquet:
                df_val = pd.read_parquet(str(_VALORIZADO_PARQUET))
                logger.info("[FORECAST] Loaded valorizado from PARQUET: %d rows, monto_yhat=$%.0fB",
                            len(df_val), df_val["monto_yhat"].sum() / 1e9 if "monto_yhat" in df_val.columns else 0)
            else:
                # Legacy CSV (comma-sep, decimal='.')
                df_val = pd.read_csv(str(_val_file), sep=",", encoding="utf-8-sig", low_memory=False)
                logger.info("[FORECAST] Loaded valorizado from CSV: %d rows", len(df_val))
            df_val.columns = [c.lower().strip() for c in df_val.columns]
            if "periodo" in df_val.columns and "fecha" not in df_val.columns:
                df_val["fecha"] = pd.to_datetime(df_val["periodo"], format="%Y-%m", errors="coerce")
            elif "fecha" in df_val.columns:
                df_val["fecha"] = pd.to_datetime(df_val["fecha"], errors="coerce")

            # Join clientes
            if CLIENTES_FILE.exists():
                try:
                    df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
                    df_cli.columns = [c.lower().strip() for c in df_cli.columns]
                    df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
                    df_val["cliente_id"] = df_val["cliente_id"].astype(str).str.strip()

                    cli_lu = df_cli[["codigo", "fantasia", "nombre_grupo"]].drop_duplicates("codigo")
                    df_val = pd.merge(df_val, cli_lu, left_on="cliente_id", right_on="codigo", how="left")
                    df_val.drop(columns=["codigo"], inplace=True, errors="ignore")

                    mask_nm = df_val["fantasia"].isna()
                    if mask_nm.any():
                        grupo_set = set(df_cli["nombre_grupo"].dropna().unique())
                        is_grp = df_val.loc[mask_nm, "cliente_id"].isin(grupo_set)
                        idx_g = mask_nm[mask_nm].index[is_grp.values]
                        df_val.loc[idx_g, "fantasia"] = df_val.loc[idx_g, "cliente_id"]
                        df_val.loc[idx_g, "nombre_grupo"] = df_val.loc[idx_g, "cliente_id"]
                        still = df_val["fantasia"].isna()
                        df_val.loc[still, "fantasia"] = df_val.loc[still, "cliente_id"]
                        df_val.loc[still, "nombre_grupo"] = "SIN GRUPO"

                    df_val["fantasia"] = df_val["fantasia"].fillna(df_val["cliente_id"])
                    df_val["nombre_grupo"] = df_val["nombre_grupo"].fillna("SIN GRUPO")
                except Exception as exc:
                    logger.warning("Clientes join error: %s", exc)
                    df_val["fantasia"] = df_val.get("cliente_id", "")
                    df_val["nombre_grupo"] = "SIN GRUPO"

            df_val = _apply_neg_names(df_val, NEGOCIOS_FILE)
            for c in ("neg", "subneg"):
                if c in df_val.columns:
                    df_val[c] = df_val[c].astype(str)
            if "codigo_serie" in df_val.columns and "descripcion" not in df_val.columns:
                df_val["descripcion"] = df_val["codigo_serie"]

            # ── Join neg/subneg/descripcion from df_main if missing in df_val ──
            if not df_main.empty and "codigo_serie" in df_val.columns:
                join_cols = [c for c in ("neg", "subneg", "descripcion") if c in df_main.columns and c not in df_val.columns]
                if join_cols and "codigo_serie" in df_main.columns:
                    neg_map = (
                        df_main[["codigo_serie"] + join_cols]
                        .drop_duplicates("codigo_serie")
                    )
                    df_val = pd.merge(df_val, neg_map, on="codigo_serie", how="left")
                    for c in ("neg", "subneg"):
                        if c in df_val.columns:
                            df_val[c] = df_val[c].astype(str)
                    logger.info("[FORECAST] Joined %s from df_main into df_val", join_cols)
        except Exception as exc:
            logger.error("Valorizado load error: %s", exc)
    result["df_valorizado"] = df_val

    # ── Lab mapping ───────────────────────────────────────────────────────
    product_lab_map: dict[str, list] = {}
    if SERIES_FILE.exists() and ARTICULOS_FILE.exists():
        try:
            df_s = pd.read_csv(str(SERIES_FILE), sep=",", encoding="utf-8", dtype=str)
            df_s.columns = [c.strip() for c in df_s.columns]
            df_a = pd.read_csv(str(ARTICULOS_FILE), sep=",", encoding="latin-1", dtype=str)
            df_a.columns = [c.strip() for c in df_a.columns]

            col_lab = _get_col_ci(df_a, "laboratorio_descrip")
            col_fam_a = _get_col_ci(df_a, "familia")
            col_desc_a = _get_col_ci(df_a, "descrip")

            if col_lab:
                fam_to_lab: dict = {}
                if col_fam_a:
                    tmp = df_a[[col_fam_a, col_lab]].dropna()
                    fam_to_lab = tmp.groupby(col_fam_a)[col_lab].apply(set).to_dict()
                desc_to_lab: dict = {}
                if col_desc_a:
                    tmp = df_a[[col_desc_a, col_lab]].dropna()
                    desc_to_lab = tmp.groupby(col_desc_a)[col_lab].apply(set).to_dict()

                col_serie = _get_col_ci(df_s, "codigo_serie")
                col_nivel = _get_col_ci(df_s, "nivel_agregacion")
                if col_serie and col_nivel:
                    for _, row in df_s.iterrows():
                        serie = str(row[col_serie]).strip()
                        nivel = str(row[col_nivel]).strip().upper()
                        labs: set = set()
                        if nivel == "FAMILIA":
                            labs = fam_to_lab.get(serie, set())
                        elif nivel in ("ARTICULO", "ITEM"):
                            labs = desc_to_lab.get(serie, set())
                        else:
                            labs = fam_to_lab.get(serie, set()) | desc_to_lab.get(serie, set())
                        if labs:
                            product_lab_map[serie] = sorted(labs)
        except Exception as exc:
            logger.warning("Lab mapping error: %s", exc)
    result["product_lab_map"] = product_lab_map

    # ── Canonical series set from valorizado (3039 series matching fact_forecast_base) ──
    # The original app cross-filters both history and val data by the series present
    # in fact_forecast_base/fact_forecast_valorizado — this is the source of truth universe.
    _canonical_series: set = set()
    if not result.get("df_valorizado", pd.DataFrame()).empty:
        _v = result["df_valorizado"]
        if "codigo_serie" in _v.columns:
            _canonical_series = set(_v["codigo_serie"].astype(str).unique())
            logger.info("[FORECAST] Canonical series set: %d series from valorizado", len(_canonical_series))

    # ── Importe histórico real 2025 (actual billing amounts) ─────────────
    # Original: cross-filtered to only series present in fact_forecast_base (same 3039 as valorizado)
    # This reduces history from 44861 rows → 38758 rows, $109.1B → $98.0B
    df_imp_hist = pd.DataFrame()
    if IMP_HIST_FILE.exists():
        try:
            df_imp_hist = pd.read_csv(str(IMP_HIST_FILE), sep=",", encoding="utf-8")
            df_imp_hist.columns = [c.lower().strip() for c in df_imp_hist.columns]
            df_imp_hist["tipo"] = "hist"
            if "periodo" in df_imp_hist.columns:
                df_imp_hist["fecha"] = pd.to_datetime(df_imp_hist["periodo"], format="%Y-%m", errors="coerce")
            if "imp_hist" in df_imp_hist.columns:
                df_imp_hist["imp_hist"] = pd.to_numeric(df_imp_hist["imp_hist"], errors="coerce").fillna(0)
            # Cross-filter to canonical series (same logic as original app inner join)
            if _canonical_series and "codigo_serie" in df_imp_hist.columns:
                df_imp_hist["codigo_serie"] = df_imp_hist["codigo_serie"].astype(str)
                before = len(df_imp_hist)
                df_imp_hist = df_imp_hist[df_imp_hist["codigo_serie"].isin(_canonical_series)].copy()
                logger.info("[FORECAST] importe_historico: %d → %d rows after canonical series filter", before, len(df_imp_hist))
        except Exception as exc:
            logger.warning("importe_historico load error: %s", exc)
    result["df_imp_hist"] = df_imp_hist

    # ── Facturación real 2026 (actual billing Jan+Feb+Mar 2026) ──────────
    # Original: uses fact_history.csv val rows which include ALL available months (Jan/Feb/Mar).
    # March IS included in the analytical layer (KPI 7) but hidden from the chart line.
    # Cross-filtered to canonical series (same as original agg_trends['val'] filter).
    df_fact_2026 = pd.DataFrame()
    if FACT_2026_FILE.exists():
        try:
            df_fact_2026 = pd.read_csv(str(FACT_2026_FILE), sep=",", encoding="utf-8")
            df_fact_2026.columns = [c.lower().strip() for c in df_fact_2026.columns]
            df_fact_2026["tipo"] = "val"
            if "fecha" in df_fact_2026.columns:
                df_fact_2026["fecha"] = pd.to_datetime(df_fact_2026["fecha"], errors="coerce")
                # Round daily dates to month start
                df_fact_2026["fecha"] = df_fact_2026["fecha"].dt.to_period("M").dt.to_timestamp()
                # Keep ALL available 2026 months (Jan+Feb+Mar) — March included in KPI 7,
                # only hidden from chart (that filtering happens in get_chart_data)
                mask_2026 = df_fact_2026["fecha"] >= "2026-01-01"
                df_fact_2026 = df_fact_2026[mask_2026].copy()
            if "imp_hist" in df_fact_2026.columns:
                df_fact_2026["imp_hist"] = pd.to_numeric(df_fact_2026["imp_hist"], errors="coerce").fillna(0)
            # Cross-filter to canonical series
            if _canonical_series and "codigo_serie" in df_fact_2026.columns:
                df_fact_2026["codigo_serie"] = df_fact_2026["codigo_serie"].astype(str)
                before = len(df_fact_2026)
                df_fact_2026 = df_fact_2026[df_fact_2026["codigo_serie"].isin(_canonical_series)].copy()
                logger.info("[FORECAST] facturacion_2026: %d → %d rows after canonical series filter", before, len(df_fact_2026))
        except Exception as exc:
            logger.warning("facturacion_real_2026 load error: %s", exc)
    result["df_fact_2026"] = df_fact_2026

    logger.info("[FORECAST] All data loaded. Main rows: %d, Valorizado rows: %d, ImpHist rows: %d, Fact2026 rows: %d",
                len(result.get("df_main", [])), len(result.get("df_valorizado", [])),
                len(result.get("df_imp_hist", [])), len(result.get("df_fact_2026", [])))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _safe_in(col: str, vals: list) -> str:
    if not vals: return ""
    clean_vals = ["'" + str(v).replace("'", "''") + "'" for v in vals]
    return f"{col} IN ({','.join(clean_vals)})"

def _query_db(table: str, start_date=None, end_date=None, profiles=None, neg=None, subneg=None, products=None, extra_where=None) -> "pd.DataFrame":
    import pandas as pd
    try:
        if engine is None or "sqlite" in str(engine.url): return pd.DataFrame()
        query = f"SELECT * FROM {table} WHERE 1=1"
        if start_date: query += f" AND fecha >= '{start_date}'"
        if end_date: query += f" AND fecha <= '{end_date}'"
        if profiles: query += " AND " + _safe_in("perfil", profiles)
        if neg: query += " AND " + _safe_in("neg", neg)
        if subneg: query += " AND " + _safe_in("subneg", subneg)
        if products:
            p_cond = _safe_in("codigo_serie", products)
            desc_cond = _safe_in("descripcion", products)
            query += f" AND ({p_cond} OR {desc_cond})"
        if extra_where: query += f" AND {extra_where}"
        
        with engine.begin() as conn:
            df = pd.read_sql(query, conn)
        if "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        return df
    except Exception as e:
        logger.error(f"SQL DB Query Error ({table}): {e}")
        return pd.DataFrame()

# ---------------------------------------------------------------------------
# PostgreSQL aggregation helpers — avoid loading full tables into RAM
# ---------------------------------------------------------------------------

def _has_overrides() -> bool:
    with _overrides_lock:
        return bool(_client_overrides)


def _build_filter_sql(
    start_date=None,
    end_date=None,
    profiles=None,
    neg=None,
    subneg=None,
    products=None,           # filter by codigo_serie only (descripcion = codigo_serie in DB)
    products_as_codes=None,  # filter by codigo_serie only; [] means "no matches → empty"
    extra=None,
    skip_neg=False,          # True for tables without neg/subneg cols (imp_hist, fact_2026)
) -> str:
    """Build a SQL WHERE clause from dashboard filter params."""
    parts = ["1=1"]
    if start_date:
        parts.append(f"fecha >= '{start_date}'")
    if end_date:
        parts.append(f"fecha <= '{end_date}'")
    if profiles:
        c = _safe_in("perfil", profiles)
        if c:
            parts.append(c)
    if not skip_neg:
        if neg:
            c = _safe_in("neg", neg)
            if c:
                parts.append(c)
        if subneg:
            c = _safe_in("subneg", subneg)
            if c:
                parts.append(c)
    if products_as_codes is not None:
        if len(products_as_codes) == 0:
            parts.append("1=0")  # no matching series → empty result
        else:
            parts.append(_safe_in("codigo_serie", products_as_codes))
    elif products:
        # descripcion = codigo_serie in all tables; filter by codigo_serie only
        parts.append(_safe_in("codigo_serie", products))
    if extra:
        parts.append(extra)
    return " AND ".join(parts)


def _query_agg(sql: str) -> "pd.DataFrame":
    """Execute a read-only SQL query on PostgreSQL; returns empty DataFrame on any error.

    Uses conn.execute(text(sql)).mappings() instead of pd.read_sql(raw_string, conn)
    to avoid the SQLAlchemy 2.x + pandas incompatibility where Row objects backed by
    immutabledict raise "immutabledict is not a sequence" when pandas tries to treat
    each row as a plain sequence/tuple.
    """
    try:
        if engine is None or "sqlite" in str(engine.url):
            return pd.DataFrame()
        stmt = _sa_text(sql) if _sa_text is not None else sql
        with engine.connect() as conn:
            result = conn.execute(stmt)
            df = pd.DataFrame(result.mappings().all())
        if not df.empty and "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        return df
    except Exception as exc:
        logger.error("DB agg query error: %s | SQL (first 300 chars): %.300s", exc, sql)
        return pd.DataFrame()


def _pg_resolve_prod_codes(products: list) -> "list | None":
    """Return codigo_serie list for given product names/codes, or None if no product filter.
    Uses only codigo_serie filter — forecast_main has no descripcion column."""
    if not products:
        return None
    where = _safe_in("codigo_serie", products)
    df = _query_agg(f"SELECT DISTINCT codigo_serie FROM forecast_main WHERE {where}")
    return df["codigo_serie"].tolist() if not df.empty else []


# Cache for schema check — checked once per process lifetime.
_val_has_codigo_serie: "bool | None" = None
_val_schema_lock = threading.Lock()


def _pg_valorizado_has_codigo_serie() -> bool:
    """Return True if forecast_valorizado has a codigo_serie column (cached after first check).

    Older migrations may not have this column.  When absent, product-level filtering
    on forecast_valorizado must be skipped — data degrades gracefully to the broader
    neg/subneg/perfil filter rather than returning an empty result set.
    """
    global _val_has_codigo_serie
    with _val_schema_lock:
        if _val_has_codigo_serie is not None:
            return _val_has_codigo_serie
    # Run outside lock to avoid holding it during the DB round-trip
    df = _query_agg(
        "SELECT column_name FROM information_schema.columns "
        "WHERE table_name='forecast_valorizado' AND column_name='codigo_serie'"
    )
    result = not df.empty
    with _val_schema_lock:
        _val_has_codigo_serie = result
    logger.info("[FORECAST schema] forecast_valorizado.codigo_serie present: %s", result)
    return result


def _val_prod_filter(prod_codes: "list | None") -> "list | None":
    """Return prod_codes only when forecast_valorizado actually has the column.

    If the column is absent, returns None so _build_filter_sql skips the IN-list
    instead of generating a WHERE that raises UndefinedColumn and silently empties
    every query that touches forecast_valorizado.
    """
    if prod_codes is None:
        return None
    if not _pg_valorizado_has_codigo_serie():
        logger.warning(
            "[FORECAST schema] forecast_valorizado lacks codigo_serie — "
            "product filter (%d codes) skipped on valorizado queries; "
            "run migration to restore full product-level accuracy.",
            len(prod_codes),
        )
        return None   # None → no filter (graceful degradation)
    return prod_codes


# ---------------------------------------------------------------------------
# PostgreSQL-optimized implementations (use SQL GROUP BY, no full-table loads)
# ---------------------------------------------------------------------------

def _pg_get_product_list(profiles: list | None, neg: list | None) -> list:
    """Return product list with volume ranking.
    Two-query strategy:
      1. forecast_main  → neg/codigo_serie mapping (TEXT ops only — no numeric aggregation)
      2. forecast_valorizado → monto_yhat volume (FLOAT, reliable)
    Avoids SUM on TEXT columns (y/yhat in forecast_main are TEXT in production).
    """
    import json
    neg_where = _build_filter_sql(profiles=profiles, neg=neg)
    # Query 1: channel mapping — only distinct text columns, no numeric aggregation
    df_neg = _query_agg(
        f"SELECT DISTINCT neg, codigo_serie FROM forecast_main WHERE {neg_where}"
    )
    if df_neg.empty:
        return []

    # Query 2: monetary volume from forecast_valorizado (monto_yhat is FLOAT)
    vol_where = _build_filter_sql(profiles=profiles, neg=neg)
    df_vol = _query_agg(
        f"SELECT codigo_serie, SUM(COALESCE(monto_yhat, 0)) AS vol_venta "
        f"FROM forecast_valorizado WHERE {vol_where} GROUP BY codigo_serie"
    )
    # vol_venta alias is already lowercase — PostgreSQL preserves it as-is ✓

    # Merge: left join so all products appear even if not in forecast_valorizado
    if df_vol.empty:
        df = df_neg.copy()
        df["vol_venta"] = 0.0
    else:
        df = pd.merge(df_neg, df_vol, on="codigo_serie", how="left")
        df["vol_venta"] = df["vol_venta"].fillna(0.0)

    labs_df = _query_agg("SELECT codigo_serie, laboratorios FROM forecast_product_labs")
    lab_map: dict = {}
    for _, row in labs_df.iterrows():
        try:
            lab_map[str(row["codigo_serie"])] = json.loads(row["laboratorios"])
        except Exception:
            pass
    ranking = (
        df.groupby(["neg", "codigo_serie"])["vol_venta"]
        .sum()
        .reset_index()
        .sort_values(["neg", "vol_venta"], ascending=[True, False])
    )
    ranking["descripcion"] = ranking["codigo_serie"]
    ranking["labs"] = ranking["codigo_serie"].apply(
        lambda x: lab_map.get(str(x) if pd.notna(x) else "", [])
    )
    return ranking.to_dict(orient="records")


def _pg_get_chart_data(
    start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct
) -> dict:
    """Memory-safe PostgreSQL chart data: all heavy aggregation runs in SQL."""
    _EMPTY = {"history": [], "forecast": [], "val_2026": [], "kpis": {}}
    _step = "init"
    try:
        return _pg_get_chart_data_inner(
            start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct
        )
    except Exception as exc:
        import traceback
        logger.error(
            "[FORECAST] _pg_get_chart_data FAILED at step=%s: %s\n%s",
            _step, exc, traceback.format_exc(),
        )
        return _EMPTY


def _pg_get_chart_data_inner(
    start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct
) -> dict:
    """Inner implementation — called by _pg_get_chart_data which catches all exceptions."""
    _EMPTY = {"history": [], "forecast": [], "val_2026": [], "kpis": {}}

    logger.info("[FORECAST chart] step=resolve_prod_codes")
    # Resolve product descriptions → codigo_serie (avoids cross-table joins in Python)
    prod_codes = _pg_resolve_prod_codes(products)
    # val_prod: None when forecast_valorizado lacks the column (graceful degradation)
    val_prod   = _val_prod_filter(prod_codes)

    logger.info("[FORECAST chart] step=build_where")
    # WHERE for forecast_main (has neg/subneg, no descripcion — uses codigo_serie only)
    main_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=prod_codes,
        products=None if prod_codes is not None else products,
    )
    logger.info("[FORECAST chart] step=query_meta main_where=%s", main_where[:100])
    # Lightweight metadata: n_products + max history date
    # forecast_main has no descripcion — count by codigo_serie
    df_meta = _query_agg(
        f"SELECT COUNT(DISTINCT codigo_serie) AS n_products, "
        f"MAX(CASE WHEN tipo = 'hist' THEN fecha END) AS max_hist_date "
        f"FROM forecast_main WHERE {main_where}"
    )
    if df_meta.empty:
        logger.warning("[FORECAST chart] df_meta empty — returning _EMPTY")
        return _EMPTY
    logger.info("[FORECAST chart] step=parse_meta cols=%s", list(df_meta.columns))
    n_products = int(df_meta["n_products"].iloc[0] or 0)
    _mhd = df_meta["max_hist_date"].iloc[0] if not df_meta.empty else None
    max_hist = pd.to_datetime(_mhd) if pd.notna(_mhd) else pd.Timestamp("2000-01-01")
    logger.info("[FORECAST chart] n_products=%s max_hist=%s", n_products, max_hist)

    # WHERE for forecast_imp_hist: only has perfil + codigo_serie + fecha (no neg/subneg)
    # hist/fact tables always have codigo_serie — use prod_codes (not val_prod) here.
    hist_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles,
        products_as_codes=prod_codes,
        skip_neg=True,
    )
    # WHERE for forecast_valorizado: use val_prod (None when column absent → no crash)
    val_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
    )
    # WHERE for forecast_fact_2026: only has perfil + codigo_serie + fecha (no neg/subneg)
    fact_where = _build_filter_sql(
        profiles=profiles,
        products_as_codes=prod_codes,
        skip_neg=True,
    )

    logger.info("[FORECAST chart] step=query_hist view_money=%s hist_where=%s", view_money, hist_where[:80])
    # History: from imp_hist (real billing, already in money) or forecast_main y×precio
    # NOTE: PostgreSQL returns column aliases in lowercase regardless of AS casing.
    # All queries use lowercase aliases; rename to Title-Case after each query so the
    # rest of the function keeps its existing column references unchanged.
    if view_money:
        # CANONICAL SERIES FILTER: restrict imp_hist to the 3039 series that exist in
        # forecast_valorizado (same inner-join the original app.py applied at load time).
        # Without this filter forecast_imp_hist returns 44 861 rows / $109.1B (all series).
        # With it: 38 758 rows / $98.0B — the correct real-2025 baseline.
        df_hist = _query_agg(
            f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "
            f"FROM forecast_imp_hist "
            f"WHERE {hist_where} "
            f"AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado) "
            f"GROUP BY fecha ORDER BY fecha"
        )
        # forecast_main fallback intentionally omitted: y/yhat are TEXT in production,
        # SUM(COALESCE(y,0)) raises a type error caught by _query_agg → empty anyway.
    else:
        # Units path — forecast_main.y is TEXT in production so this will be empty;
        # kept for local/SQLite mode where y is numeric.
        df_hist = _query_agg(
            f"SELECT fecha, SUM(COALESCE(y::numeric, 0)) AS total_venta "
            f"FROM forecast_main WHERE {main_where} AND tipo = 'hist' GROUP BY fecha ORDER BY fecha"
        )
    # Normalise alias to Title-Case so downstream code is unchanged
    if not df_hist.empty and "total_venta" in df_hist.columns:
        df_hist.rename(columns={"total_venta": "Total_Venta"}, inplace=True)

    logger.info("[FORECAST chart] step=query_fcst df_hist_rows=%s", len(df_hist))
    # Forecast: from valorizado (monto_yhat=money, yhat_cliente=units; monto_li/monto_ls=band)
    val_col    = "monto_yhat"    if view_money else "yhat_cliente"
    val_col_li = "monto_li"      if view_money else "li_cliente"
    val_col_ls = "monto_ls"      if view_money else "ls_cliente"
    df_fcst = _query_agg(
        f"SELECT fecha, "
        f"SUM(COALESCE({val_col}, 0)) AS total_forecast, "
        f"SUM(COALESCE({val_col_li}, 0)) AS total_li, "
        f"SUM(COALESCE({val_col_ls}, 0)) AS total_ls "
        f"FROM forecast_valorizado WHERE {val_where} GROUP BY fecha ORDER BY fecha"
    )
    # Normalise aliases (PostgreSQL returns lowercase regardless of AS casing)
    if not df_fcst.empty:
        rename_map = {k: v for k, v in {
            "total_forecast": "Total_Forecast",
            "total_li": "Total_Li",
            "total_ls": "Total_Ls",
        }.items() if k in df_fcst.columns}
        if rename_map:
            df_fcst.rename(columns=rename_map, inplace=True)
    if not df_fcst.empty:
        # If li/ls columns absent from table (older migration), fall back to flat band
        if "Total_Li" not in df_fcst.columns:
            df_fcst["Total_Li"] = df_fcst.get("Total_Forecast", 0)
        if "Total_Ls" not in df_fcst.columns:
            df_fcst["Total_Ls"] = df_fcst.get("Total_Forecast", 0)

        # Line 3 — Proyección estándar comercial: modelo × (1 + growth_pct/100)
        df_fcst["Total_Adj"] = df_fcst["Total_Forecast"]
        if growth_pct != 0:
            g = 1.0 + growth_pct / 100.0
            future = df_fcst["fecha"] > max_hist
            df_fcst.loc[future, "Total_Adj"] = df_fcst.loc[future, "Total_Forecast"] * g

        # Line 4 — Proyección comercial ajustada por usuario.
        # Parte de Total_Adj (crecimiento estándar) y aplica un delta por producto/mes
        # donde el usuario editó la tasa: delta = orig × (override_pct − growth_pct) / 100.
        # Rows sin override contribuyen igual a Total_Adj → solo los editados difieren.
        df_fcst["Total_User_Adj"] = df_fcst["Total_Adj"].copy()
        if _has_overrides():
            with _overrides_lock:
                overrides_snapshot = {cid: dict(ov) for cid, ov in _client_overrides.items()}
            if overrides_snapshot:
                cli_col = "fantasia"
                # forecast_valorizado has no 'articulo' column — use codigo_serie.
                # Overrides are stored with art = codigo_serie (set by _pg_get_client_detail).
                art_col = "codigo_serie"
                val_col = "monto_yhat" if view_money else "yhat_cliente"
                conditions = []
                params_map: dict = {}
                for client_id, store in overrides_snapshot.items():
                    for (articulo, date_str), pct in store.items():
                        try:
                            month_ts = pd.Timestamp(date_str + "-01")
                        except Exception:
                            continue
                        safe_cid = str(client_id).replace("'", "''")
                        safe_art = str(articulo).replace("'", "''")
                        safe_dt  = month_ts.strftime("%Y-%m-%d")
                        conditions.append(
                            f"({cli_col} = '{safe_cid}' AND {art_col} = '{safe_art}'"
                            f" AND DATE_TRUNC('month', fecha) = '{safe_dt}')"
                        )
                        params_map[(str(client_id), str(articulo), date_str)] = pct
                if conditions:
                    where_ovr = " OR ".join(conditions)
                    df_orig = _query_agg(
                        f"SELECT fecha, {cli_col}, {art_col}, "
                        f"SUM(COALESCE({val_col}, 0)) AS orig_val "
                        f"FROM forecast_valorizado WHERE {where_ovr} "
                        f"GROUP BY fecha, {cli_col}, {art_col}"
                    )
                    if not df_orig.empty:
                        df_orig["_ds"] = df_orig["fecha"].dt.strftime("%Y-%m")
                        df_orig["_override_pct"] = df_orig.apply(
                            lambda r: params_map.get(
                                (str(r[cli_col]), str(r[art_col]), str(r["_ds"])), None
                            ), axis=1,
                        )
                        df_orig = df_orig[df_orig["_override_pct"].notna()]
                        # stored pct is a MONTHLY rate (e.g. 3.4074 for 50% annual).
                        # Recover annual multiplier: (1 + monthly/100)^12, then compute
                        # delta vs the standard growth multiplier (1 + growth_pct/100).
                        # Example: monthly=3.4074 → annual_eff=1.50; growth_pct=25 → std=1.25
                        #          delta = orig × (1.50 − 1.25) = orig × 0.25  → clearly visible
                        _std_eff = 1.0 + growth_pct / 100.0
                        df_orig["_annual_eff"] = (1.0 + df_orig["_override_pct"] / 100.0) ** 12
                        df_orig["_delta"] = df_orig["orig_val"] * (
                            df_orig["_annual_eff"] - _std_eff
                        )
                        df_delta = df_orig.groupby("fecha")["_delta"].sum().reset_index()
                        df_delta.rename(columns={"_delta": "User_Delta"}, inplace=True)
                        df_fcst = df_fcst.merge(df_delta, on="fecha", how="left")
                        df_fcst["User_Delta"] = df_fcst["User_Delta"].fillna(0)
                        # Apply delta only to forecast months (bridge point stays at hist value)
                        future_mask = df_fcst["fecha"] > max_hist
                        df_fcst.loc[future_mask, "Total_User_Adj"] = (
                            df_fcst.loc[future_mask, "Total_Adj"]
                            + df_fcst.loc[future_mask, "User_Delta"]
                        )
                        df_fcst.drop(columns=["User_Delta"], inplace=True)

    logger.info("[FORECAST chart] step=bridge df_fcst_rows=%s df_hist_rows=%s", len(df_fcst), len(df_hist))
    # Bridge: connect last history point to start of forecast line
    if not df_hist.empty and not df_fcst.empty:
        hist_last = df_hist.sort_values("fecha").iloc[-1]
        bridge = pd.DataFrame([{
            "fecha": hist_last["fecha"],
            "Total_Forecast": float(hist_last["Total_Venta"]),
            "Total_Li": float(hist_last["Total_Venta"]),
            "Total_Ls": float(hist_last["Total_Venta"]),
            "Total_Adj": float(hist_last["Total_Venta"]),
            "Total_User_Adj": float(hist_last["Total_Venta"]),
        }])
        df_fcst = pd.concat([bridge, df_fcst.sort_values("fecha")], ignore_index=True)

    logger.info("[FORECAST chart] step=query_fact2026")
    # Facturación real 2026
    # CANONICAL SERIES FILTER: restrict fact_2026 to the series that exist in
    # forecast_valorizado (same inner-join the original app.py applied at load time).
    # Without this filter, extra rows from series not in the model are included,
    # inflating the total: 17.661B (17.7B) instead of 17.618B (17.6B),
    # and accuracy: 91.2% instead of 90.9%.  Mirrors the identical filter on
    # forecast_imp_hist (AND codigo_serie IN (SELECT DISTINCT ... FROM forecast_valorizado)).
    df_fact_raw = _query_agg(
        f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "
        f"FROM forecast_fact_2026 WHERE {fact_where} AND fecha >= '2026-01-01' "
        f"AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado) "
        f"GROUP BY fecha ORDER BY fecha"
    )
    # Normalise alias
    if not df_fact_raw.empty and "total_venta" in df_fact_raw.columns:
        df_fact_raw.rename(columns={"total_venta": "Total_Venta"}, inplace=True)
    val_2026_records: list = []
    fact_2026_sum = 0.0
    if not df_fact_raw.empty:
        fact_2026_sum = float(df_fact_raw["Total_Venta"].sum())
        df_v2026_chart = df_fact_raw[df_fact_raw["fecha"] < pd.Timestamp("2026-03-01")].copy()
        if not df_hist.empty and not df_v2026_chart.empty:
            hist_last = df_hist.sort_values("fecha").iloc[-1]
            brow = pd.DataFrame([{"fecha": hist_last["fecha"],
                                   "Total_Venta": float(hist_last["Total_Venta"])}])
            df_v2026_chart = pd.concat([brow, df_v2026_chart], ignore_index=True)
        for _, row in df_v2026_chart.sort_values("fecha").iterrows():
            val_2026_records.append({
                "fecha": row["fecha"].strftime("%Y-%m-%d") if pd.notna(row["fecha"]) else None,
                "Total_Venta": round(float(row.get("Total_Venta", 0)), 0),
            })

    logger.info("[FORECAST chart] step=kpis df_fact_raw_rows=%s", len(df_fact_raw))
    # KPIs
    total_hist = float(df_hist["Total_Venta"].sum()) if not df_hist.empty else 0.0
    total_real_2025 = 0.0
    if not df_hist.empty:
        m25 = df_hist["fecha"].dt.year == 2025
        total_real_2025 = float(df_hist.loc[m25, "Total_Venta"].sum()) if m25.any() else 0.0
    total_fcst = total_adj = 0.0
    if not df_fcst.empty:
        m26 = df_fcst["fecha"].dt.year == 2026
        total_fcst = float(df_fcst.loc[m26, "Total_Forecast"].sum()) if m26.any() else 0.0
        total_adj  = float(df_fcst.loc[m26, "Total_Adj"].sum())      if m26.any() else total_fcst

    INFLATION_MO_PCT = 2.9
    inflation_pct = ((1 + INFLATION_MO_PCT / 100) ** 12 - 1) * 100
    var_nominal = ((total_adj / total_real_2025) - 1) * 100 if total_real_2025 > 0 else 0.0
    var_real    = ((total_adj / (1 + inflation_pct / 100) / total_real_2025) - 1) * 100 if total_real_2025 > 0 else 0.0
    meta_completeness = (fact_2026_sum / total_adj * 100) if total_adj > 0 else 0.0

    accuracy_val = 0.0
    if not df_fact_raw.empty and not df_fcst.empty:
        try:
            val_months = sorted(m for m in df_fact_raw["fecha"].dropna().unique()
                                if pd.Timestamp(m).year == 2026)
            closed = val_months[:-1] if len(val_months) > 1 else val_months
            scores = []
            for m in closed:
                actual = float(df_fact_raw[df_fact_raw["fecha"] == m]["Total_Venta"].sum())
                proj   = float(df_fcst[df_fcst["fecha"] == m]["Total_Forecast"].sum()) if not df_fcst.empty else 0.0
                if actual > 0:
                    scores.append(max(0.0, (1 - abs(actual - proj) / actual) * 100))
            accuracy_val = float(np.mean(scores)) if scores else 0.0
        except Exception:
            pass

    def _fmt(dfs: "pd.DataFrame", cols: list) -> list:
        out = []
        for _, row in dfs.sort_values("fecha").iterrows():
            rec = {"fecha": row["fecha"].strftime("%Y-%m-%d") if pd.notna(row["fecha"]) else None}
            for c in cols:
                v = row.get(c, 0)
                rec[c] = round(float(v), 0) if pd.notna(v) else 0
            out.append(rec)
        return out

    logger.info("[FORECAST chart] step=serialize total_adj=%s n_products=%s", round(total_adj, 0), n_products)
    return {
        "history":  _fmt(df_hist, ["Total_Venta"])                                     if not df_hist.empty else [],
        "forecast": _fmt(df_fcst, ["Total_Forecast", "Total_Li", "Total_Ls", "Total_Adj", "Total_User_Adj"]) if not df_fcst.empty else [],
        "val_2026": val_2026_records,
        "has_overrides": _has_overrides(),
        "max_hist_date": max_hist.strftime("%Y-%m-%d") if pd.notna(max_hist) else None,
        "kpis": {
            "total_proyeccion_2026":    round(total_adj, 0),
            "var_nominal_2025":         round(var_nominal, 2),
            "inflation_pct":            round(inflation_pct, 1),
            "inflation_mo_pct":         INFLATION_MO_PCT,
            "var_real_2025":            round(var_real, 2),
            "accuracy_val":             round(accuracy_val, 1),
            "expectation_accuracy_val": 0.0,
            "fact_2026":                round(fact_2026_sum, 0),
            "meta_completeness":        round(meta_completeness, 1),
            "total_historia":           round(total_hist, 0),
            "total_proyeccion":         round(total_fcst, 0),
            "total_proyeccion_adj":     round(total_adj, 0),
            "total_real_2025":          round(total_real_2025, 0),
            "n_products":               n_products,
        },
    }


def _pg_get_client_table(
    start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct, lab_products
) -> dict:
    """Memory-safe PostgreSQL client table: GROUP BY (fantasia, nombre_grupo, fecha).

    Valorization strategy (view_money=True):
      1. Try monto_yhat (pre-computed monetary column from the parquet migration).
      2. If the total is zero (column absent or all-NULL in DB — stale migration),
         fall back to yhat_cliente (units) × avg(precio) from forecast_main.
    This makes the table robust to Render deploys where the migration
    hasn't been re-run yet with the latest parquet file.
    """
    _EMPTY = {"months": [], "rows": [], "totals": {}, "min_val": 0, "max_val": 0, "total_projected": 0}

    prod_codes = _pg_resolve_prod_codes(products)
    val_prod   = _val_prod_filter(prod_codes)
    val_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
    )

    if view_money:
        # Primary: SUM(monto_yhat) per (fantasia, nombre_grupo, fecha).
        # Does NOT select codigo_serie from forecast_valorizado — backward-compatible
        # with older migrations where that column may be absent (avoids UndefinedColumn
        # → empty DataFrame → "No Rows To Show" regression).
        df_agg = _query_agg(
            f"SELECT fantasia, nombre_grupo, fecha, "
            f"SUM(COALESCE(monto_yhat, 0)) AS val "
            f"FROM forecast_valorizado WHERE {val_where} "
            f"GROUP BY fantasia, nombre_grupo, fecha ORDER BY fecha"
        )
        if df_agg.empty:
            return _EMPTY

        if df_agg["val"].sum() == 0:
            # monto_yhat all-zero (stale migration without parquet):
            # attempt per-serie price fallback via subquery so val_where column
            # references are unambiguous (no JOIN column name clash).
            # If forecast_valorizado lacks codigo_serie, _query_agg catches the
            # UndefinedColumn error and returns empty — in that case we keep df_agg
            # (rows present with val=0: visible but unvalorized, better than no rows).
            logger.warning(
                "[FORECAST client-table] monto_yhat is all-zero — "
                "falling back to yhat_cliente × avg(precio). Run the migration to fix permanently."
            )
            df_fallback = _query_agg(
                f"SELECT v.fantasia, v.nombre_grupo, v.fecha, "
                f"SUM(COALESCE(v.yhat_cliente, 0) * COALESCE(m.avg_precio, 0)) AS val "
                f"FROM (SELECT fantasia, nombre_grupo, fecha, codigo_serie, yhat_cliente "
                f"      FROM forecast_valorizado WHERE {val_where}) v "
                f"LEFT JOIN (SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS avg_precio "
                f"           FROM forecast_main GROUP BY codigo_serie) m "
                f"  ON v.codigo_serie = m.codigo_serie "
                f"GROUP BY v.fantasia, v.nombre_grupo, v.fecha ORDER BY v.fecha"
            )
            if not df_fallback.empty and df_fallback["val"].sum() > 0:
                df_agg = df_fallback
            # else: keep df_agg — rows exist (val=0), table renders rather than going blank
    else:
        df_agg = _query_agg(
            f"SELECT fantasia, nombre_grupo, fecha, "
            f"SUM(COALESCE(yhat_cliente, 0)) AS val "
            f"FROM forecast_valorizado WHERE {val_where} "
            f"GROUP BY fantasia, nombre_grupo, fecha ORDER BY fecha"
        )
        if df_agg.empty:
            return _EMPTY

    # Max hist date for growth adjustment
    df_mhd = _query_agg(
        f"SELECT MAX(fecha) AS max_hist_date FROM forecast_main "
        f"WHERE {_build_filter_sql(profiles=profiles, neg=neg, subneg=subneg)} AND tipo = 'hist'"
    )
    max_hist_date = None
    if not df_mhd.empty and pd.notna(df_mhd["max_hist_date"].iloc[0]):
        max_hist_date = pd.to_datetime(df_mhd["max_hist_date"].iloc[0])

    # Growth adjustment on future months
    if growth_pct != 0 and max_hist_date is not None:
        future_mask = df_agg["fecha"] > max_hist_date
        df_agg.loc[future_mask, "val"] = df_agg.loc[future_mask, "val"] * (1.0 + growth_pct / 100.0)

    # Normalise client/group display names
    df_agg["fantasia"]     = df_agg["fantasia"].fillna("").astype(str).str.strip()
    df_agg["nombre_grupo"] = df_agg["nombre_grupo"].fillna("").astype(str).str.strip()
    sin_mask  = df_agg["nombre_grupo"].str.upper().isin({"SIN GRUPO", ""})
    self_mask = df_agg["fantasia"] == df_agg["nombre_grupo"]
    df_agg.loc[sin_mask | self_mask, "nombre_grupo"] = ""

    # Lab highlighting
    clients_with_lab: set = set()
    if lab_products:
        lab_codes = _pg_resolve_prod_codes(lab_products)
        if lab_codes:
            lab_where = _build_filter_sql(
                products_as_codes=lab_codes,
                profiles=profiles, neg=neg, subneg=subneg,
            )
            df_lab = _query_agg(
                f"SELECT DISTINCT fantasia FROM forecast_valorizado WHERE {lab_where}"
            )
            if not df_lab.empty:
                clients_with_lab = set(df_lab["fantasia"].dropna().tolist())

    # Pivot → (fantasia, nombre_grupo) × fecha
    pivot = (
        df_agg.groupby(["fantasia", "nombre_grupo", "fecha"])["val"]
        .sum()
        .reset_index()
        .set_index(["fantasia", "nombre_grupo", "fecha"])["val"]
        .unstack("fecha")
        .fillna(0)
    )
    pivot = pivot.sort_index(axis=1)
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=False)
    pivot.drop(columns=["_total"], inplace=True)

    date_cols = list(pivot.columns)
    col_names = [d.strftime("%b %Y").title() for d in date_cols]
    pivot.columns = col_names
    pivot_reset = pivot.reset_index()
    pivot_reset.rename(columns={"fantasia": "Cliente", "nombre_grupo": "Grupo"}, inplace=True)

    rows = []
    for _, row in pivot_reset.iterrows():
        r = {
            "Cliente": str(row.get("Cliente", "")),
            "Grupo":   str(row.get("Grupo", "")),
            "_lab":    1 if row.get("Cliente") in clients_with_lab else 0,
        }
        for mn in col_names:
            r[mn] = round(float(row.get(mn, 0)), 0)
        rows.append(r)

    totals       = {mn: round(float(pivot_reset[mn].sum()), 0) for mn in col_names}
    vals_flat    = [v for r in rows for k, v in r.items() if k in col_names and isinstance(v, (int, float))]
    min_val      = float(min(vals_flat)) if vals_flat else 0
    max_val      = float(max(vals_flat)) if vals_flat else 0
    total_projected = round(sum(totals.values()), 0)

    return {
        "months": col_names, "rows": rows, "totals": totals,
        "min_val": min_val, "max_val": max_val, "total_projected": total_projected,
    }


def _pg_get_treemap_data(
    start_date, end_date, profiles, neg, subneg, products, view_money, period_date
) -> dict:
    """Memory-safe PostgreSQL treemap: GROUP BY (perfil, nombre_grupo, fantasia, cliente_id)."""
    _EMPTY = {"ids": [], "labels": [], "parents": [], "values": [], "colors": [], "periods": [], "canals": []}

    # All available periods (unfiltered)
    periods_df = _query_agg("SELECT DISTINCT fecha FROM forecast_valorizado ORDER BY fecha")
    periods = [str(r["fecha"])[:10] for _, r in periods_df.iterrows() if pd.notna(r["fecha"])]

    prod_codes = _pg_resolve_prod_codes(products)
    val_prod   = _val_prod_filter(prod_codes)
    val_col    = "monto_yhat" if view_money else "yhat_cliente"

    extra = None
    if period_date:
        target = pd.to_datetime(period_date).replace(day=1).strftime("%Y-%m-%d")
        extra = f"fecha = '{target}'"

    val_where = _build_filter_sql(
        start_date=start_date if not period_date else None,
        end_date=end_date     if not period_date else None,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
        extra=extra,
    )
    df_tree = _query_agg(
        f"SELECT perfil, nombre_grupo, fantasia, cliente_id, "
        f"SUM(COALESCE({val_col}, 0)) AS monto "
        f"FROM forecast_valorizado WHERE {val_where} "
        f"GROUP BY perfil, nombre_grupo, fantasia, cliente_id"
    )
    if df_tree.empty:
        return {**_EMPTY, "periods": periods}

    # Normalise column name — PostgreSQL always returns lowercase aliases
    if "monto" not in df_tree.columns and "Monto" in df_tree.columns:
        df_tree.rename(columns={"Monto": "monto"}, inplace=True)
    if "monto" not in df_tree.columns:
        logger.error("treemap: expected column 'monto' not found. Columns: %s", list(df_tree.columns))
        return {**_EMPTY, "periods": periods}

    # Build display columns (same logic as get_treemap_data)
    df_tree["_canal"] = df_tree["perfil"].astype(str).str.upper().str.strip()
    df_tree["_canal"] = df_tree["_canal"].replace(
        {"NO_ASIGNADO": "POTENCIAL", "NO_ASIGNADA": "POTENCIAL", "SIN ASIGNAR": "POTENCIAL"}
    )
    cli = df_tree["fantasia"].astype(str).str.strip().replace("nan", pd.NA)
    cli = cli.fillna(df_tree["cliente_id"].astype(str).str.strip())
    df_tree["_cliente_display"] = cli.fillna("Sin dato")

    grp_raw  = df_tree["nombre_grupo"].astype(str).str.strip()
    sin_mask = grp_raw.str.upper().isin({"SIN GRUPO", "SIN GRUPO / OTROS", "", "NAN", "NONE"})
    df_tree["_grupo_display"] = grp_raw
    df_tree.loc[sin_mask, "_grupo_display"] = df_tree.loc[sin_mask, "_cliente_display"]

    tree_df = (
        df_tree.groupby(["_canal", "_grupo_display", "_cliente_display"], dropna=False)["monto"]
        .sum().reset_index()
    )
    tree_df.columns = ["Canal", "Grupo", "Cliente", "Monto"]
    tree_df = tree_df[tree_df["Monto"] > 0].copy()
    if tree_df.empty:
        return {**_EMPTY, "periods": periods}

    unique_canals = sorted(tree_df["Canal"].unique().tolist())
    canals = [{"name": c, "color": _get_segment_color(c)} for c in unique_canals]

    ids: list = []; labels: list = []; parents: list = []
    values: list = []; colors: list = []

    def _add(nid, label, parent, value, color):
        ids.append(nid); labels.append(label); parents.append(parent)
        values.append(float(value)); colors.append(color)

    _add("total", "Total", "", float(tree_df["Monto"].sum()), "#EAF0F5")

    group_totals = tree_df.groupby(["Canal", "Grupo"], as_index=False)["Monto"].sum()
    group_totals["rank_grupo"]   = group_totals.groupby("Canal")["Monto"].rank(method="first", ascending=False)
    group_totals["share_canal"]  = group_totals["Monto"] / group_totals.groupby("Canal")["Monto"].transform("sum")
    group_totals["show_direct"]  = (group_totals["rank_grupo"] <= 8) | (group_totals["share_canal"] >= 0.06)

    for canal, canal_df in tree_df.groupby("Canal", sort=False):
        bc       = _get_segment_color(str(canal))
        canal_id = f"canal::{canal}"
        _add(canal_id, str(canal), "total", float(canal_df["Monto"].sum()), _blend_with_white(bc, 0.22))

        cg      = group_totals[group_totals["Canal"] == canal].copy()
        small_g = cg[~cg["show_direct"]]
        otras_g = None
        if not small_g.empty:
            otras_g = f"{canal_id}::otras_grupos"
            _add(otras_g, "Otras", canal_id, float(small_g["Monto"].sum()), _blend_with_white(bc, 0.14))

        for _, gr in cg.iterrows():
            grupo   = gr["Grupo"]
            g_par   = canal_id if gr["show_direct"] else otras_g
            if g_par is None:
                continue
            gid = f"{canal_id}::grupo::{grupo}"
            _add(gid, str(grupo), g_par, float(gr["Monto"]),
                 _blend_with_white(bc, 0.33 if gr["show_direct"] else 0.43))

            grp_cli = canal_df[canal_df["Grupo"] == grupo].copy()
            grp_cli = grp_cli.assign(
                rank_c=grp_cli["Monto"].rank(method="first", ascending=False),
                share_g=grp_cli["Monto"] / grp_cli["Monto"].sum(),
            )
            grp_cli["show_c"] = (grp_cli["rank_c"] <= 6) | (grp_cli["share_g"] >= 0.08)
            small_c = grp_cli[~grp_cli["show_c"]]
            otras_c = None
            if not small_c.empty:
                otras_c = f"{gid}::otras_clientes"
                _add(otras_c, "Otras", gid, float(small_c["Monto"].sum()), _blend_with_white(bc, 0.24))
            for _, cr in grp_cli.iterrows():
                c_par = gid if cr["show_c"] else otras_c
                if c_par is None:
                    continue
                tone = 0.56 - min(float(cr["share_g"]) * 0.35, 0.22)
                _add(f"{gid}::cliente::{cr['Cliente']}", str(cr["Cliente"]),
                     c_par, float(cr["Monto"]), _blend_with_white(bc, max(0.10, tone)))

    return {"ids": ids, "labels": labels, "parents": parents, "values": values,
            "colors": colors, "periods": periods, "canals": canals}


def _pg_get_client_detail(
    client_id, start_date, end_date, profiles, neg, subneg, products, growth_pct
) -> dict:
    """Memory-safe PostgreSQL client detail: loads only single client's rows."""
    _EMPTY = {"client_id": client_id, "perfil": "", "negocios": [], "dates": []}

    # Filter strictly by this client (fantasia or cliente_id)
    safe_cid  = str(client_id).replace("'", "''")
    cli_extra = f"(fantasia = '{safe_cid}' OR cliente_id = '{safe_cid}')"
    prod_codes = _pg_resolve_prod_codes(products)
    val_prod   = _val_prod_filter(prod_codes)
    val_where  = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
        extra=cli_extra,
    )
    # forecast_valorizado has no 'articulo' column — use codigo_serie as fallback.
    # 'descripcion' may or may not exist; use COALESCE. 'unidad_medida' is absent too.
    df_c = _query_agg(
        f"SELECT fecha, codigo_serie, "
        f"COALESCE(descripcion, codigo_serie) AS descripcion, "
        f"neg, subneg, perfil, fantasia, yhat_cliente, monto_yhat, nivel_agregacion "
        f"FROM forecast_valorizado WHERE {val_where}"
    )
    if df_c.empty:
        return _EMPTY

    # max hist date
    df_mhd = _query_agg("SELECT MAX(fecha) AS mhd FROM forecast_main WHERE tipo = 'hist'")
    max_hist_date = None
    if not df_mhd.empty and pd.notna(df_mhd["mhd"].iloc[0]):
        max_hist_date = pd.to_datetime(df_mhd["mhd"].iloc[0])

    # Price map from forecast_main (avg precio per codigo_serie)
    df_price = _query_agg(
        "SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS precio FROM forecast_main GROUP BY codigo_serie"
    )
    price_map: dict = {}
    if not df_price.empty:
        price_map = df_price.set_index("codigo_serie")["precio"].to_dict()

    saved_overrides = _get_client_overrides_snapshot(client_id)

    # Ensure required columns — articulo and unidad_medida absent from forecast_valorizado
    if "articulo" not in df_c.columns:
        df_c["articulo"] = df_c["codigo_serie"].astype(str) if "codigo_serie" in df_c.columns else ""
    if "descripcion" not in df_c.columns:
        df_c["descripcion"] = df_c["articulo"]
    for col, default in [("unidad_medida", "Unid."), ("nivel_agregacion", "ARTICULO"),
                          ("neg", "Sin Negocio"), ("subneg", "General")]:
        if col not in df_c.columns:
            df_c[col] = default
        else:
            df_c[col] = df_c[col].fillna(default)

    first    = df_c.iloc[0]
    perfil   = str(first.get("perfil", ""))
    neg_val  = str(first.get("neg", ""))

    val_col = next((c for c in ("yhat_cliente", "yhat", "monto_yhat") if c in df_c.columns), None)
    if val_col is None:
        return _EMPTY

    grp_keys = [k for k in ["articulo", "descripcion", "unidad_medida",
                              "nivel_agregacion", "neg", "subneg", "fecha"]
                if k in df_c.columns]
    agg       = df_c.groupby(grp_keys)[val_col].sum().reset_index()
    all_dates = sorted(agg["fecha"].unique())
    date_strs = [d.strftime("%Y-%m") for d in all_dates]

    def _get_price(articulo):
        return float(price_map.get(str(articulo), 0) or 0)

    negocios_out = []
    for neg_name, df_neg in agg.groupby("neg"):
        subnegs_out = []
        sub_col = "subneg" if "subneg" in df_neg.columns else None
        for subneg_name, df_sub in (df_neg.groupby("subneg") if sub_col else [("General", df_neg)]):
            products_out = []
            for _, prow in df_sub.groupby(["articulo", "descripcion"]):
                art   = str(prow.iloc[0]["articulo"])
                desc  = str(prow.iloc[0]["descripcion"])
                um    = str(prow.iloc[0].get("unidad_medida", "Unid."))
                nivel = str(prow.iloc[0].get("nivel_agregacion", "ARTICULO"))
                precio = _get_price(art)
                months_data = {}
                for d, ds in zip(all_dates, date_strs):
                    row_d = prow[prow["fecha"] == d]
                    orig  = float(row_d[val_col].sum()) if not row_d.empty else 0.0
                    adj = orig; pct = 0.0
                    saved_pct = saved_overrides.get((art, ds), None)
                    if saved_pct is not None:
                        pct = saved_pct
                        adj = orig * (1.0 + pct / 100.0)
                    elif max_hist_date and d > max_hist_date and growth_pct != 0:
                        t  = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                        rm = (1 + growth_pct / 100.0) ** (1 / 12.0) - 1
                        adj = orig * (1 + rm) ** t
                        # Guardamos tasa MENSUAL — el gráfico la reconvierte a anual vía (1+rm)^12
                        pct = round(rm * 100, 4)
                    months_data[ds] = {
                        "orig": round(orig, 2), "pct": round(pct, 4),
                        "nuevo": round(adj, 2), "money": round(adj * precio, 0),
                    }
                total_nuevo = sum(v["nuevo"] for v in months_data.values())
                total_money = round(total_nuevo * precio, 0)
                if total_nuevo > 0 or any(v["orig"] > 0 for v in months_data.values()):
                    products_out.append({
                        "articulo": art, "descripcion": desc,
                        "unidad_medida": um, "nivel_agregacion": nivel,
                        "precio": round(precio, 2),
                        "total_nuevo": round(total_nuevo, 2), "total_money": total_money,
                        "months": months_data,
                    })
            products_out.sort(key=lambda x: x["total_money"], reverse=True)
            subnegs_out.append({"subneg": str(subneg_name), "products": products_out})
        negocios_out.append({"neg": str(neg_name), "subnegs": subnegs_out})

    return {
        "client_id": client_id, "perfil": perfil, "neg": neg_val,
        "negocios": negocios_out, "dates": date_strs,
        "max_hist_date": max_hist_date.strftime("%Y-%m") if max_hist_date else None,
        "growth_pct": growth_pct,
    }


# ---------------------------------------------------------------------------
# Public cache / data access
# ---------------------------------------------------------------------------

def get_data() -> dict[str, Any]:
    global _data_cache
    if _data_cache:
        return _data_cache
    with _cache_lock:
        if not _data_cache:
            if engine is not None and "postgresql" in str(engine.url):
                # NEVER LOAD GLOBALLY ON RENDER! OOM RISK
                return {}
            else:
                _data_cache = _load_all_data()
    return _data_cache


def reload_data() -> None:
    global _data_cache, _val_has_codigo_serie
    clear_response_cache()   # Always flush response cache on explicit reload
    # Reset schema cache so a just-run migration is detected on next request
    with _val_schema_lock:
        _val_has_codigo_serie = None
    if engine is not None and "postgresql" in str(engine.url):
        # PostgreSQL mode: data lives in DB, not in CSV files.
        # Just clear the in-memory cache (which is {} anyway in this mode).
        with _cache_lock:
            _data_cache = {}
        logger.info("[FORECAST] PostgreSQL mode: cache cleared (no CSV reload needed)")
        return
    with _cache_lock:
        _data_cache = _load_all_data()


def get_forecast_schema_info() -> dict:
    """Return actual column names + dtypes for all forecast tables from information_schema.
    Used for debugging schema mismatches between code and production DB."""
    tables = [
        "forecast_main", "forecast_valorizado",
        "forecast_imp_hist", "forecast_fact_2026", "forecast_product_labs",
    ]
    result: dict = {}
    if engine is None or "postgresql" not in str(engine.url):
        return {"error": "Solo disponible en modo PostgreSQL", "tables": {}}
    try:
        with engine.connect() as conn:
            for tbl in tables:
                df = pd.read_sql(
                    f"SELECT column_name, data_type "
                    f"FROM information_schema.columns "
                    f"WHERE table_name = '{tbl}' ORDER BY ordinal_position",
                    conn,
                )
                if df.empty:
                    result[tbl] = {"exists": False, "columns": []}
                else:
                    result[tbl] = {
                        "exists": True,
                        "columns": [
                            {"name": r["column_name"], "type": r["data_type"]}
                            for _, r in df.iterrows()
                        ],
                    }
                    # Quick row count
                    try:
                        cnt = pd.read_sql(f"SELECT COUNT(*) AS n FROM {tbl}", conn)
                        result[tbl]["row_count"] = int(cnt["n"].iloc[0])
                    except Exception:
                        result[tbl]["row_count"] = -1
    except Exception as exc:
        return {"error": str(exc), "tables": result}
    return {"tables": result}


@_with_resp_cache(ttl=_RESP_TTL_STATIC)
def get_filter_options() -> dict:
    import json  # needed in both branches
    if engine is not None and "postgresql" in str(engine.url):
        # ── Core filters: profiles, neg, subneg, dates ────────────────────
        # Isolated in their own try/except so a labs table absence does NOT
        # wipe out the core filter options (critical for Problem 2).
        perfiles: list = []
        negs: list = []
        subnegs: list = []
        min_date = max_date = None
        try:
            with engine.connect() as conn:
                perfiles = pd.read_sql(
                    "SELECT DISTINCT perfil FROM forecast_main WHERE perfil IS NOT NULL", conn
                )["perfil"].tolist()
                negs = pd.read_sql(
                    "SELECT DISTINCT neg FROM forecast_main WHERE neg IS NOT NULL", conn
                )["neg"].tolist()
                subnegs = pd.read_sql(
                    "SELECT DISTINCT subneg FROM forecast_main WHERE subneg IS NOT NULL", conn
                )["subneg"].tolist()
                valid_dates = pd.read_sql(
                    "SELECT min(fecha) AS min_d, max(fecha) AS max_d FROM forecast_main", conn
                )
                if not valid_dates.empty:
                    if pd.notnull(valid_dates["min_d"].iloc[0]):
                        min_date = valid_dates["min_d"].iloc[0].strftime("%Y-%m-%d")
                    if pd.notnull(valid_dates["max_d"].iloc[0]):
                        max_date = valid_dates["max_d"].iloc[0].strftime("%Y-%m-%d")
        except Exception as exc:
            logger.error("Filter options DB error (core): %s", exc, exc_info=True)
            return {"profiles": [], "neg": [], "subneg": [], "labs": [], "min_date": None, "max_date": None}

        # ── Labs: optional — failure here must NOT affect core filters ────
        all_labs: set = set()
        try:
            with engine.connect() as conn:
                labs_df = pd.read_sql(
                    "SELECT codigo_serie, laboratorios FROM forecast_product_labs", conn
                )
                if not labs_df.empty:
                    for _, row in labs_df.iterrows():
                        try:
                            all_labs.update(json.loads(row["laboratorios"]))
                        except Exception:
                            pass
        except Exception as exc:
            logger.warning("Filter options: forecast_product_labs not available (%s)", exc)

        return {
            "profiles": sorted(perfiles),
            "neg": sorted(negs),
            "subneg": sorted(subnegs),
            "labs": sorted(all_labs),
            "min_date": min_date,
            "max_date": max_date,
        }
    else:
        data = get_data()
        df = data.get("df_main", pd.DataFrame())
        all_labs: set = set()
        for labs in data.get("product_lab_map", {}).values():
            all_labs.update(labs)

        min_date = max_date = None
        if not df.empty and "fecha" in df.columns:
            valid = df["fecha"].dropna()
            if not valid.empty:
                min_date = valid.min().strftime("%Y-%m-%d")
                max_date = valid.max().strftime("%Y-%m-%d")

        return {
            "profiles": sorted(df["perfil"].dropna().unique().tolist()) if "perfil" in df.columns else [],
            "neg": sorted(df["neg"].dropna().unique().tolist()) if "neg" in df.columns else [],
            "subneg": sorted(df["subneg"].dropna().unique().tolist()) if "subneg" in df.columns else [],
            "labs": sorted(all_labs),
            "min_date": min_date,
            "max_date": max_date,
        }


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_product_list(profiles: list | None = None, neg: list | None = None) -> list[dict]:
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_product_list(profiles=profiles, neg=neg)
    else:
        data = get_data()
        df = data.get("df_main", pd.DataFrame())
        lab_map = data.get("product_lab_map", {})
    if df.empty:
        return []

    mask = pd.Series(True, index=df.index)
    if profiles:
        mask &= df["perfil"].isin(profiles) if "perfil" in df.columns else mask
    if neg:
        mask &= df["neg"].isin(neg) if "neg" in df.columns else mask

    df_f = df[mask].copy()
    if "precio" not in df_f.columns:
        df_f["precio"] = 1500

    df_f["vol_venta"] = (
        df_f["y"].fillna(0) + df_f["yhat"].fillna(0)
    ) * df_f["precio"].fillna(0)

    if "neg" not in df_f.columns:
        df_f["neg"] = "Varios"

    ranking = (
        df_f.groupby(["neg", "descripcion"])["vol_venta"]
        .sum()
        .reset_index()
        .sort_values(["neg", "vol_venta"], ascending=[True, False])
    )

    # Add lab info
    # lab_map already acquired via DB overlay
    ranking["labs"] = ranking["codigo_serie" if "codigo_serie" in ranking.columns else "descripcion"].apply(lambda x: lab_map.get(x, []))

    return ranking.to_dict(orient="records")


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_chart_data(
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    growth_pct: float = 0.0,
) -> dict:
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_chart_data(
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money, growth_pct=growth_pct,
        )

    data = get_data()
    df = data.get("df_main", pd.DataFrame())
    _ovr_active = _has_overrides()
    # Use unpatched base for all chart lines — overrides are reflected in Total_User_Adj (Line 4)
    df_val = data.get("df_valorizado", pd.DataFrame())
    df_imp_hist = data.get("df_imp_hist", pd.DataFrame())
    df_fact_2026 = data.get("df_fact_2026", pd.DataFrame())

    if df.empty:
        return {"history": [], "forecast": [], "val_2026": [], "kpis": {}}

    # Date mask
    mask = pd.Series(True, index=df.index)
    if start_date:
        mask &= df["fecha"] >= pd.to_datetime(start_date)
    if end_date:
        mask &= df["fecha"] <= pd.to_datetime(end_date)
    if profiles and "perfil" in df.columns:
        mask &= df["perfil"].isin(profiles)
    if neg and "neg" in df.columns:
        mask &= df["neg"].isin(neg)
    if subneg and "subneg" in df.columns:
        mask &= df["subneg"].isin(subneg)
    if products and "descripcion" in df.columns:
        mask &= df["descripcion"].isin(products)

    df_filt = df[mask].copy()

    # Ensure precio column and apply prices → monetary conversion
    if "precio" not in df_filt.columns:
        df_filt["precio"] = 1500

    if view_money:
        for col in ("y", "yhat", "li", "ls"):
            if col in df_filt.columns:
                df_filt[col] = df_filt[col] * df_filt["precio"].fillna(0)

    type_map = {"hist": "Historia", "forecast": "Proyección"}
    if "tipo" in df_filt.columns:
        df_filt["Etiqueta_Upper"] = df_filt["tipo"].map(type_map).fillna(df_filt["tipo"])

    # ── History: prefer imp_hist real-billing amounts; fallback to y×precio ──
    # Precompute producto→codigo_serie lookup from df_val (has both columns post-enrichment)
    _prod_codes_lookup: set = set()
    if products and "descripcion" in df_val.columns and "codigo_serie" in df_val.columns:
        _prod_codes_lookup = set(
            df_val[df_val["descripcion"].isin(products)]["codigo_serie"].astype(str).unique()
        )

    df_hist = pd.DataFrame()
    if view_money and not df_imp_hist.empty and "imp_hist" in df_imp_hist.columns and "fecha" in df_imp_hist.columns:
        mask_ih = pd.Series(True, index=df_imp_hist.index)
        if start_date:
            mask_ih &= df_imp_hist["fecha"] >= pd.to_datetime(start_date)
        if end_date:
            mask_ih &= df_imp_hist["fecha"] <= pd.to_datetime(end_date)
        if profiles and "perfil" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["perfil"].isin(profiles)
        if neg and "neg" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["neg"].isin(neg)
        if subneg and "subneg" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["subneg"].isin(subneg)
        # Filter by selected products via codigo_serie lookup (df_imp_hist has no descripcion)
        if products and "codigo_serie" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["codigo_serie"].astype(str).isin(_prod_codes_lookup)
        df_hist = (
            df_imp_hist[mask_ih]
            .groupby("fecha")
            .agg(Total_Venta=("imp_hist", "sum"))
            .reset_index()
        )

    if df_hist.empty:
        # Fallback: price×units from forecast_base_consolidado tipo='hist'
        df_hist = df_filt[df_filt.get("Etiqueta_Upper", pd.Series()) == "Historia"].groupby("fecha").agg(
            Total_Venta=("y", "sum")
        ).reset_index()

    # Forecast from valorizado if available
    if not df_val.empty:
        mask_v = pd.Series(True, index=df_val.index)
        if start_date:
            mask_v &= df_val["fecha"] >= pd.to_datetime(start_date)
        if end_date:
            mask_v &= df_val["fecha"] <= pd.to_datetime(end_date)
        if profiles and "perfil" in df_val.columns:
            mask_v &= df_val["perfil"].isin(profiles)
        if neg and "neg" in df_val.columns:
            mask_v &= df_val["neg"].isin(neg)
        if subneg and "subneg" in df_val.columns:
            mask_v &= df_val["subneg"].isin(subneg)
        if products and "descripcion" in df_val.columns:
            mask_v &= df_val["descripcion"].isin(products)

        df_val_f = df_val[mask_v]
        col_y = "monto_yhat" if view_money else "yhat_cliente"
        col_li = "monto_li" if ("monto_li" in df_val_f.columns) else col_y
        col_ls = "monto_ls" if ("monto_ls" in df_val_f.columns) else col_y

        if col_y not in df_val_f.columns:
            col_y = next((c for c in ("yhat", "monto_yhat") if c in df_val_f.columns), None)

        if col_y:
            df_fcst = df_val_f.groupby("fecha").agg(
                Total_Forecast=(col_y, "sum"),
                Total_Li=(col_li, "sum"),
                Total_Ls=(col_ls, "sum"),
            ).reset_index()

            # ── Proyección comercial ajustada por usuario (Línea 4) ──────────
            # Lógica: misma fórmula que la línea "+X%" pero usando la tasa de
            # crecimiento editada por el usuario (override_pct) en vez de la tasa
            # global estándar. Para filas sin override, usa la tasa global.
            # Resultado: SUM(monto_yhat × tasa_efectiva) agrupado por mes.
            _g_base = 1.0 + growth_pct / 100.0
            if _ovr_active:
                _cli_col_v = "fantasia" if "fantasia" in df_val_f.columns else "cliente_id"
                # Support both 'articulo' (CSV) and 'codigo_serie' (PG fallback) columns
                _art_col_v = (
                    "articulo"     if "articulo"     in df_val_f.columns else
                    "codigo_serie" if "codigo_serie" in df_val_f.columns else None
                )
                if _cli_col_v in df_val_f.columns and _art_col_v is not None:
                    with _overrides_lock:
                        _ovr_snap = {cid: dict(ov) for cid, ov in _client_overrides.items()}
                    _ovr_lookup = {
                        (str(cid), str(art), str(ds)): pct
                        for cid, store in _ovr_snap.items()
                        for (art, ds), pct in store.items()
                    }
                    _df_u = df_val_f[[_cli_col_v, _art_col_v, "fecha", col_y]].copy()
                    _df_u["_ds"] = _df_u["fecha"].dt.strftime("%Y-%m")
                    _keys = list(zip(
                        _df_u[_cli_col_v].astype(str),
                        _df_u[_art_col_v].astype(str),
                        _df_u["_ds"].astype(str),
                    ))
                    # Stored pct is a MONTHLY compound rate (e.g. 3.4074 for 50% annual).
                    # To get the correct annual multiplier, convert: (1 + rm)^12.
                    # This gives 1.50 for 3.4074% monthly — clearly different from g_base=1.25.
                    _df_u["_eff"] = [
                        (1.0 + _ovr_lookup[k] / 100.0) ** 12 if k in _ovr_lookup else _g_base
                        for k in _keys
                    ]
                    _df_u["_ua"] = _df_u[col_y] * _df_u["_eff"]
                    _ua_agg = (
                        _df_u.groupby("fecha")["_ua"].sum()
                        .reset_index()
                        .rename(columns={"_ua": "Total_User_Adj"})
                    )
                    df_fcst = df_fcst.merge(_ua_agg, on="fecha", how="left")
                    df_fcst["Total_User_Adj"] = df_fcst["Total_User_Adj"].fillna(
                        df_fcst["Total_Forecast"] * _g_base
                    )
                else:
                    df_fcst["Total_User_Adj"] = df_fcst["Total_Forecast"] * _g_base
            else:
                df_fcst["Total_User_Adj"] = df_fcst["Total_Forecast"] * _g_base
        else:
            df_fcst = pd.DataFrame(columns=["fecha", "Total_Forecast", "Total_Li", "Total_Ls", "Total_User_Adj"])
    else:
        df_f2 = df_filt[df_filt.get("Etiqueta_Upper", pd.Series()) == "Proyección"].groupby("fecha").agg(
            Total_Forecast=("yhat", "sum"),
            Total_Li=("li", "sum"),
            Total_Ls=("ls", "sum"),
        ).reset_index()
        df_fcst = df_f2
        df_fcst["Total_User_Adj"] = df_fcst["Total_Forecast"] * (1.0 + growth_pct / 100.0)

    # Safety fallback — ensures Total_User_Adj is always present
    if "Total_User_Adj" not in df_fcst.columns:
        df_fcst["Total_User_Adj"] = df_fcst["Total_Forecast"] * (1.0 + growth_pct / 100.0)

    # Growth adjustment — flat multiplier matching original app.py
    # Original: Total_Forecast_Adj = Total_Forecast * (1 + growth_pct/100) for all projection months
    def apply_growth(df_src: pd.DataFrame, col: str, max_hist_date: pd.Timestamp) -> pd.DataFrame:
        df_src = df_src.copy()
        df_src["Total_Adj"] = df_src[col]
        if growth_pct == 0:
            return df_src
        growth_factor = 1.0 + (growth_pct / 100.0)
        future = df_src["fecha"] > max_hist_date
        if not future.any():
            return df_src
        df_src.loc[future, "Total_Adj"] = df_src.loc[future, col] * growth_factor
        return df_src

    max_hist = df_hist["fecha"].max() if not df_hist.empty else pd.Timestamp("2000-01-01")
    df_fcst = apply_growth(df_fcst, "Total_Forecast", max_hist)

    # ── Bridge: prepend last history point to forecast so the projection line
    # visually starts where history ends — same as original app.py lines 1588-1608
    if not df_hist.empty and not df_fcst.empty:
        hist_last = df_hist.sort_values("fecha").iloc[-1]
        hist_last_val = float(hist_last["Total_Venta"])
        bridge_fcst = pd.DataFrame([{
            "fecha": hist_last["fecha"],
            "Total_Forecast": hist_last_val,
            "Total_Li": hist_last_val,
            "Total_Ls": hist_last_val,
            "Total_Adj": hist_last_val,
            "Total_User_Adj": hist_last_val,
        }])
        df_fcst = pd.concat([bridge_fcst, df_fcst.sort_values("fecha")], ignore_index=True)

    def to_records_safe(df_src: pd.DataFrame, cols: list) -> list:
        out = []
        for _, row in df_src.iterrows():
            rec = {"fecha": row["fecha"].strftime("%Y-%m-%d") if pd.notna(row["fecha"]) else None}
            for c in cols:
                val = row.get(c, 0)
                rec[c] = round(float(val), 0) if pd.notna(val) else 0
            out.append(rec)
        return out

    history_records = to_records_safe(df_hist.sort_values("fecha"), ["Total_Venta"])
    forecast_records = to_records_safe(
        df_fcst.sort_values("fecha"),
        ["Total_Forecast", "Total_Li", "Total_Ls", "Total_Adj", "Total_User_Adj"],
    )

    total_hist = float(df_hist["Total_Venta"].sum()) if not df_hist.empty else 0
    # KPI totals use only 2026 months — bridge point (Dec 2025) excluded via year filter below
    total_fcst = float(df_fcst.loc[df_fcst["fecha"].dt.year == 2026, "Total_Forecast"].sum()) if not df_fcst.empty else 0
    total_adj  = float(df_fcst.loc[df_fcst["fecha"].dt.year == 2026, "Total_Adj"].sum()) if not df_fcst.empty and "Total_Adj" in df_fcst.columns else total_fcst

    # ── KPI 1-7 (replica exacta del Streamlit original) ──────────────────
    INFLATION_MO_PCT = 2.9  # tasa mensual fija (igual que app.py)
    inflation_pct = ((1 + INFLATION_MO_PCT / 100) ** 12 - 1) * 100  # anualizado ~40.5%

    # Total proyectado anual 2026 (solo meses 2026 del forecast ajustado)
    if not df_fcst.empty and "fecha" in df_fcst.columns and "Total_Adj" in df_fcst.columns:
        mask_2026_fcst = df_fcst["fecha"].dt.year == 2026
        total_proyectado_2026_annual = float(df_fcst.loc[mask_2026_fcst, "Total_Adj"].sum()) if mask_2026_fcst.any() else total_adj
    else:
        total_proyectado_2026_annual = total_adj

    # Total real 2025 (historia, año 2025)
    if not df_hist.empty and "fecha" in df_hist.columns:
        mask_2025 = df_hist["fecha"].dt.year == 2025
        total_real_2025 = float(df_hist.loc[mask_2025, "Total_Venta"].sum()) if mask_2025.any() else 0.0
    else:
        total_real_2025 = 0.0

    # Variación nominal 2026 vs 2025
    var_nominal_2025 = ((total_proyectado_2026_annual / total_real_2025) - 1) * 100 if total_real_2025 > 0 else 0.0

    # Variación real (deflactada)
    total_proyectado_2026_deflated = total_proyectado_2026_annual / (1 + inflation_pct / 100)
    var_real_2025 = ((total_proyectado_2026_deflated / total_real_2025) - 1) * 100 if total_real_2025 > 0 else 0.0

    # ── Facturación 2026 (real billing — analytical layer: Jan+Feb+Mar) ────
    # Original app uses fact_history.csv val rows which include ALL available months.
    # March IS included in KPI calculations (fact_2026_sum, meta, accuracy)
    # but hidden from the chart line (df_v_line filters out March).
    val_2026_records: list = []
    fact_2026_sum = 0.0
    accuracy_val = 0.0
    expectation_accuracy_val = 0.0
    meta_completeness = 0.0

    if not df_fact_2026.empty and "fecha" in df_fact_2026.columns and "imp_hist" in df_fact_2026.columns:
        mask_f2 = pd.Series(True, index=df_fact_2026.index)
        if profiles and "perfil" in df_fact_2026.columns:
            mask_f2 &= df_fact_2026["perfil"].isin(profiles)
        if neg and "neg" in df_fact_2026.columns:
            mask_f2 &= df_fact_2026["neg"].isin(neg)
        if subneg and "subneg" in df_fact_2026.columns:
            mask_f2 &= df_fact_2026["subneg"].isin(subneg)
        # Filter by selected products via codigo_serie lookup (df_fact_2026 has no descripcion)
        if products and "codigo_serie" in df_fact_2026.columns:
            mask_f2 &= df_fact_2026["codigo_serie"].astype(str).isin(_prod_codes_lookup)
        df_f2 = df_fact_2026[mask_f2].copy()

        # All months aggregated (analytical layer: Jan+Feb+Mar)
        df_v2026_all = df_f2.groupby("fecha").agg(Total_Venta=("imp_hist", "sum")).reset_index()

        # fact_2026_sum = ALL available months (Jan+Feb+Mar) — matches original KPI 7
        fact_2026_sum = float(df_f2["imp_hist"].sum())
        meta_completeness = (fact_2026_sum / total_proyectado_2026_annual * 100) if total_proyectado_2026_annual > 0 else 0.0

        # Chart line: Jan+Feb only (March hidden from line, same as original df_v_line filter)
        df_v2026_chart = df_v2026_all[df_v2026_all["fecha"] < pd.Timestamp("2026-03-01")].copy()

        # Bridge: connect chart line to end of history
        if not df_hist.empty and not df_v2026_chart.empty:
            hist_last = df_hist.sort_values("fecha").iloc[-1]
            bridge = pd.DataFrame([{"fecha": hist_last["fecha"], "Total_Venta": hist_last["Total_Venta"]}])
            df_v2026_chart = pd.concat([bridge, df_v2026_chart], ignore_index=True)

        val_2026_records = to_records_safe(df_v2026_chart.sort_values("fecha"), ["Total_Venta"])

        # Accuracy: use ALL 2026 months as the universe (Jan+Feb+Mar).
        # "closed" = all except the last (most recent open) month.
        # Original: val_months_2026[:-1] → with Mar present, closed = [Jan, Feb] → accuracy = mean([Jan,Feb])
        if not df_fcst.empty:
            try:
                val_months_2026 = sorted(
                    m for m in df_v2026_all["fecha"].dropna().unique()
                    if pd.Timestamp(m).year == 2026
                )
                closed = val_months_2026[:-1] if len(val_months_2026) > 1 else val_months_2026
                model_scores, exp_scores = [], []
                for m in closed:
                    actual = float(df_v2026_all[df_v2026_all["fecha"] == m]["Total_Venta"].sum())
                    proj_base = float(df_fcst[df_fcst["fecha"] == m]["Total_Forecast"].sum()) if "Total_Forecast" in df_fcst.columns else 0.0
                    proj_adj  = float(df_fcst[df_fcst["fecha"] == m]["Total_Adj"].sum()) if "Total_Adj" in df_fcst.columns else proj_base
                    if actual > 0:
                        model_scores.append(max(0.0, (1 - abs(actual - proj_base) / actual) * 100))
                        if growth_pct != 0:
                            exp_scores.append(max(0.0, (1 - abs(actual - proj_adj) / actual) * 100))
                if model_scores:
                    accuracy_val = float(np.mean(model_scores))
                if exp_scores:
                    expectation_accuracy_val = float(np.mean(exp_scores))
            except Exception:
                pass

    return {
        "history": history_records,
        "forecast": forecast_records,
        "val_2026": val_2026_records,
        "has_overrides": _ovr_active,
        "max_hist_date": max_hist.strftime("%Y-%m-%d") if pd.notna(max_hist) else None,
        "kpis": {
            # KPI 1 - Monto Total Proyectado Anual 2026
            "total_proyeccion_2026": round(total_proyectado_2026_annual, 0),
            # KPI 2 - Variación Nominal sobre 2025
            "var_nominal_2025": round(var_nominal_2025, 2),
            # KPI 3 - Inflación Esperada (fija)
            "inflation_pct": round(inflation_pct, 1),
            "inflation_mo_pct": INFLATION_MO_PCT,
            # KPI 4 - Variación Real sobre 2025
            "var_real_2025": round(var_real_2025, 2),
            # KPI 5 - Coincidencia modelo
            "accuracy_val": round(accuracy_val, 1),
            # KPI 6 - Coincidencia expectativa
            "expectation_accuracy_val": round(expectation_accuracy_val, 1),
            # KPI 7 - Facturado 2026
            "fact_2026": round(fact_2026_sum, 0),
            "meta_completeness": round(meta_completeness, 1),
            # --- Legacy / extra ---
            "total_historia": round(total_hist, 0),
            "total_proyeccion": round(total_fcst, 0),
            "total_proyeccion_adj": round(total_adj, 0),
            "total_real_2025": round(total_real_2025, 0),
            "n_products": int(df_filt["descripcion"].nunique()) if "descripcion" in df_filt.columns else 0,
        },
    }


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_client_table(
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    growth_pct: float = 0.0,
    lab_products: list | None = None,
) -> dict:
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_client_table(
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money,
            growth_pct=growth_pct, lab_products=lab_products,
        )

    data = get_data()
    df_val = _get_patched_df_val()
    df_main = data.get("df_main", pd.DataFrame())

    if df_val.empty:
        return {"months": [], "rows": [], "totals": [], "min_val": 0, "max_val": 0, "total_projected": 0}

    # Filter
    mask = pd.Series(True, index=df_val.index)
    if start_date:
        mask &= df_val["fecha"] >= pd.to_datetime(start_date)
    if end_date:
        mask &= df_val["fecha"] <= pd.to_datetime(end_date)
    if profiles and "perfil" in df_val.columns:
        mask &= df_val["perfil"].isin(profiles)
    if neg and "neg" in df_val.columns:
        mask &= df_val["neg"].isin(neg)
    if subneg and "subneg" in df_val.columns:
        mask &= df_val["subneg"].isin(subneg)
    if products and "descripcion" in df_val.columns:
        mask &= df_val["descripcion"].isin(products)

    df_c = df_val[mask].copy()
    if df_c.empty:
        return {"months": [], "rows": [], "totals": [], "min_val": 0, "max_val": 0, "total_projected": 0}

    # Value column
    val_col = "monto_yhat" if (view_money and "monto_yhat" in df_c.columns) else "yhat_cliente"
    if val_col not in df_c.columns:
        val_col = next((c for c in ("monto_yhat", "yhat_cliente", "yhat") if c in df_c.columns), None)
    if val_col is None:
        return {"months": [], "rows": [], "totals": [], "min_val": 0, "max_val": 0, "total_projected": 0}

    # Client & group display
    if "fantasia" in df_c.columns:
        df_c["_cliente"] = df_c["fantasia"]
        if "nombre_grupo" in df_c.columns:
            df_c["_grupo"] = df_c["nombre_grupo"].fillna("")
            mask_sin = df_c["_grupo"] == "SIN GRUPO"
            mask_self = df_c["_cliente"] == df_c["_grupo"]
            df_c.loc[mask_sin | mask_self, "_grupo"] = ""
        else:
            df_c["_grupo"] = ""
    else:
        df_c["_cliente"] = df_c.get("cliente_id", "")
        df_c["_grupo"] = ""

    # Pivot
    grp = df_c.groupby(["_cliente", "_grupo", "fecha"])[val_col].sum().reset_index()
    pivot = grp.set_index(["_cliente", "_grupo", "fecha"])[val_col].unstack("fecha").fillna(0)
    pivot = pivot.sort_index(axis=1)

    # Growth adjustment on future columns
    if growth_pct != 0 and not df_main.empty and "tipo" in df_main.columns:
        max_hist_date = df_main[df_main["tipo"] == "hist"]["fecha"].max()
        future_cols = sorted([c for c in pivot.columns if c > max_hist_date])
        if future_cols:
            start_proj = future_cols[0]
            for col_date in future_cols:
                months_diff = (col_date.year - start_proj.year) * 12 + (col_date.month - start_proj.month)
                quarters = (months_diff // 3) + 1
                factor = 1.0 + (growth_pct * quarters / 100.0)
                pivot[col_date] = pivot[col_date] * factor

    # Sort by total desc
    pivot["_total"] = pivot.sum(axis=1)
    pivot = pivot.sort_values("_total", ascending=False)
    pivot.drop(columns=["_total"], inplace=True)

    date_cols = list(pivot.columns)
    new_col_names = [d.strftime("%b %Y").title() for d in date_cols]

    pivot.columns = new_col_names
    pivot_reset = pivot.reset_index()
    pivot_reset.rename(columns={"_cliente": "Cliente", "_grupo": "Grupo"}, inplace=True)

    # Lab highlighting
    lab_set = set(lab_products) if lab_products else set()
    if lab_set and "descripcion" in df_c.columns:
        clients_with_lab = set(df_c[df_c["descripcion"].isin(lab_set)]["_cliente"].unique())
    else:
        clients_with_lab = set()

    rows = []
    for _, row in pivot_reset.iterrows():
        r: dict = {
            "Cliente": str(row.get("Cliente", "")),
            "Grupo": str(row.get("Grupo", "")),
            "_lab": 1 if row.get("Cliente") in clients_with_lab else 0,
        }
        for mn in new_col_names:
            r[mn] = round(float(row.get(mn, 0)), 0)
        rows.append(r)

    # Totals row
    totals = {mn: round(float(pivot_reset[mn].sum()), 0) for mn in new_col_names}

    # Min/max for heatmap
    vals_flat = [v for r in rows for k, v in r.items() if k in new_col_names and isinstance(v, (int, float))]
    min_val = float(min(vals_flat)) if vals_flat else 0
    max_val = float(max(vals_flat)) if vals_flat else 0

    total_projected = round(float(sum(totals.values())), 0)

    return {
        "months": new_col_names,
        "rows": rows,
        "totals": totals,
        "min_val": min_val,
        "max_val": max_val,
        "total_projected": total_projected,
    }


def _get_segment_color(canal_code: str) -> str:
    """Color function identical to the original Streamlit get_segment_color()."""
    c = str(canal_code).upper().strip()
    if "PROYECCIÓN TOTAL" in c:
        return "#DCEAF2"
    if "FAR" in c:
        return "#6FC9E2"
    if "DRO" in c:
        return "#6A6A6A"
    if "IPR" in c or "SAN" in c:
        return "#E291C1"
    if "IPU" in c or "PUB" in c or "LAN" in c or "PER" in c:
        return "#A576FF"
    if "HOS" in c:
        return "#A576FF"
    if "COM" in c or "PRO" in c:
        return "#5770B0"
    if any(x in c for x in ("OSP", "OSU", "OES")):
        return "#00A487"
    if "DPM" in c or "FIN" in c:
        return "#06486F"
    if "POTENCIAL" in c or "SIN" in c:
        return "#C9D6E2"
    return "#C9D6E2"


def _blend_with_white(hex_color: str, weight: float) -> str:
    """Blend a hex color toward white by `weight` (0=original, 1=white)."""
    hex_color = hex_color.lstrip("#")
    rgb = [int(hex_color[i: i + 2], 16) for i in (0, 2, 4)]
    out = [int(c + (255 - c) * weight) for c in rgb]
    return "#{:02x}{:02x}{:02x}".format(*out)


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_treemap_data(
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    period_date: str | None = None,
) -> dict:
    """Return Plotly treemap: Canal (perfil) → Grupo → Cliente hierarchy.

    Matches exactly the original Streamlit build_market_treemap() logic:
    - period_date=None → accumulate all available months (or start/end range)
    - period_date='YYYY-MM-DD' → only that specific month
    Returns ids/labels/parents/values/colors for Plotly, plus periods and canals for UI.
    """
    import calendar as _cal

    _EMPTY = {"ids": [], "labels": [], "parents": [], "values": [], "colors": [], "periods": [], "canals": []}

    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_treemap_data(
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money, period_date=period_date,
        )

    df_val = _get_patched_df_val()
    if df_val.empty:
        return _EMPTY

    # ── Collect available periods (from full df_val, before filtering) ────
    periods: list[str] = []
    if "fecha" in df_val.columns:
        periods = sorted(str(m)[:10] for m in df_val["fecha"].dropna().unique())

    # ── Date / filter mask ─────────────────────────────────────────────────
    mask = pd.Series(True, index=df_val.index)
    if period_date and "fecha" in df_val.columns:
        target_month = pd.to_datetime(period_date).replace(day=1)
        mask &= df_val["fecha"] == target_month
    else:
        if start_date and "fecha" in df_val.columns:
            mask &= df_val["fecha"] >= pd.to_datetime(start_date).replace(day=1)
        if end_date and "fecha" in df_val.columns:
            end_dt = pd.to_datetime(end_date)
            end_month_last = end_dt.replace(day=_cal.monthrange(end_dt.year, end_dt.month)[1])
            mask &= df_val["fecha"] <= end_month_last
    if profiles and "perfil" in df_val.columns:
        mask &= df_val["perfil"].isin(profiles)
    if neg and "neg" in df_val.columns:
        mask &= df_val["neg"].isin(neg)
    if subneg and "subneg" in df_val.columns:
        mask &= df_val["subneg"].isin(subneg)

    df_f = df_val[mask].copy()
    if df_f.empty:
        return {**_EMPTY, "periods": periods}

    # ── Value column ───────────────────────────────────────────────────────
    val_col = "monto_yhat" if (view_money and "monto_yhat" in df_f.columns) else "yhat_cliente"
    if val_col not in df_f.columns:
        val_col = next((c for c in ("monto_yhat", "yhat_cliente", "yhat") if c in df_f.columns), None)
    if val_col is None:
        return {**_EMPTY, "periods": periods}

    # ── Build display columns ──────────────────────────────────────────────
    # _canal = perfil code uppercased
    df_f["_canal"] = (
        df_f["perfil"].astype(str).str.upper().str.strip()
        if "perfil" in df_f.columns
        else pd.Series("SIN DATO", index=df_f.index)
    )
    df_f["_canal"] = df_f["_canal"].replace(
        {"NO_ASIGNADO": "POTENCIAL", "NO_ASIGNADA": "POTENCIAL", "SIN ASIGNAR": "POTENCIAL"}
    )

    # _cliente_display = fantasia → cliente_id fallback
    if "fantasia" in df_f.columns:
        cli_col = df_f["fantasia"].astype(str).str.strip()
        if "cliente_id" in df_f.columns:
            cli_col = cli_col.replace("nan", pd.NA).fillna(df_f["cliente_id"].astype(str).str.strip())
        df_f["_cliente_display"] = cli_col.fillna("Sin dato")
    elif "cliente_id" in df_f.columns:
        df_f["_cliente_display"] = df_f["cliente_id"].astype(str).str.strip()
    else:
        df_f["_cliente_display"] = "Sin dato"

    # _grupo_display = nombre_grupo; collapse "SIN GRUPO" variants → use client name
    if "nombre_grupo" in df_f.columns:
        grp_raw = df_f["nombre_grupo"].astype(str).str.strip()
        sin_grupo_mask = grp_raw.str.upper().isin(
            {"SIN GRUPO", "SIN GRUPO / OTROS", "DIRECTO / OTROS", "", "NAN", "NONE"}
        )
        df_f["_grupo_display"] = grp_raw
        df_f.loc[sin_grupo_mask, "_grupo_display"] = df_f.loc[sin_grupo_mask, "_cliente_display"]
    else:
        df_f["_grupo_display"] = df_f["_cliente_display"]

    # ── Aggregate Canal × Grupo × Cliente ─────────────────────────────────
    tree_df = (
        df_f.groupby(["_canal", "_grupo_display", "_cliente_display"], dropna=False)[val_col]
        .sum()
        .reset_index()
    )
    tree_df.columns = ["Canal", "Grupo", "Cliente", "Monto"]
    tree_df = tree_df[tree_df["Monto"] > 0].copy()

    if tree_df.empty:
        return {**_EMPTY, "periods": periods}

    # ── Legend canals ──────────────────────────────────────────────────────
    unique_canals = sorted(tree_df["Canal"].unique().tolist())
    canals = [{"name": c, "color": _get_segment_color(c)} for c in unique_canals]

    # ── Build treemap nodes (mirrors build_market_treemap exactly) ─────────
    ids: list[str] = []
    labels: list[str] = []
    parents: list[str] = []
    values: list[float] = []
    colors: list[str] = []

    def add_node(nid: str, label: str, parent: str, value: float, color: str) -> None:
        ids.append(nid)
        labels.append(label)
        parents.append(parent)
        values.append(float(value))
        colors.append(color)

    total_value = tree_df["Monto"].sum()
    add_node("total", "Total", "", total_value, "#EAF0F5")

    # Pre-compute group ranks / shares per canal
    group_totals = tree_df.groupby(["Canal", "Grupo"], as_index=False)["Monto"].sum()
    group_totals["rank_grupo"] = group_totals.groupby("Canal")["Monto"].rank(method="first", ascending=False)
    group_totals["share_canal"] = group_totals["Monto"] / group_totals.groupby("Canal")["Monto"].transform("sum")
    group_totals["show_direct"] = (group_totals["rank_grupo"] <= 8) | (group_totals["share_canal"] >= 0.06)

    for canal, canal_df in tree_df.groupby("Canal", sort=False):
        base_color = _get_segment_color(str(canal))
        canal_id = f"canal::{canal}"
        canal_total = float(canal_df["Monto"].sum())
        add_node(canal_id, str(canal), "total", canal_total, _blend_with_white(base_color, 0.22))

        canal_groups = group_totals[group_totals["Canal"] == canal].copy()
        small_groups = canal_groups[~canal_groups["show_direct"]]
        otras_canal_id: str | None = None
        if not small_groups.empty:
            otras_canal_id = f"{canal_id}::otras_grupos"
            add_node(otras_canal_id, "Otras", canal_id, float(small_groups["Monto"].sum()), _blend_with_white(base_color, 0.14))

        for _, grp_row in canal_groups.iterrows():
            grupo = grp_row["Grupo"]
            grupo_total = float(grp_row["Monto"])
            grupo_parent = canal_id if grp_row["show_direct"] else otras_canal_id
            if grupo_parent is None:
                continue
            grupo_id = f"{canal_id}::grupo::{grupo}"
            blend_g = 0.33 if grp_row["show_direct"] else 0.43
            add_node(grupo_id, str(grupo), grupo_parent, grupo_total, _blend_with_white(base_color, blend_g))

            grp_clients = canal_df[canal_df["Grupo"] == grupo].copy()
            grp_clients = grp_clients.assign(
                rank_cliente=grp_clients["Monto"].rank(method="first", ascending=False),
                share_grupo=grp_clients["Monto"] / grp_clients["Monto"].sum(),
            )
            grp_clients["show_direct"] = (grp_clients["rank_cliente"] <= 6) | (grp_clients["share_grupo"] >= 0.08)

            small_clients = grp_clients[~grp_clients["show_direct"]]
            otras_cli_id: str | None = None
            if not small_clients.empty:
                otras_cli_id = f"{grupo_id}::otras_clientes"
                add_node(otras_cli_id, "Otras", grupo_id, float(small_clients["Monto"].sum()), _blend_with_white(base_color, 0.24))

            for _, cli_row in grp_clients.iterrows():
                cliente = cli_row["Cliente"]
                cliente_parent = grupo_id if cli_row["show_direct"] else otras_cli_id
                if cliente_parent is None:
                    continue
                cliente_node_id = f"{grupo_id}::cliente::{cliente}"
                tone = 0.56 - min(float(cli_row["share_grupo"]) * 0.35, 0.22)
                add_node(cliente_node_id, str(cliente), cliente_parent, float(cli_row["Monto"]), _blend_with_white(base_color, max(0.10, tone)))

    return {
        "ids": ids,
        "labels": labels,
        "parents": parents,
        "values": values,
        "colors": colors,
        "periods": periods,
        "canals": canals,
    }


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_client_detail(
    client_id: str,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    growth_pct: float = 0.0,
) -> dict:
    """Return per-product detail for a client, pivoted by month, grouped by neg/subneg.
    Used by the modal edit dialog.
    Always uses the RAW (unpatched) df_valorizado so that 'orig' = CSV baseline.
    Saved % overrides are injected directly into months_data so the modal shows them.
    """
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_client_detail(
            client_id=client_id,
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, growth_pct=growth_pct,
        )

    data = get_data()
    df_val = data.get("df_valorizado", pd.DataFrame()).copy()
    df_main = data.get("df_main", pd.DataFrame())
    price_lookup = data.get("price_lookup", {})
    # Load any previously saved % overrides for this client
    saved_overrides = _get_client_overrides_snapshot(client_id)

    if df_val.empty:
        return {"client_id": client_id, "perfil": "", "negocios": [], "dates": []}

    # Filter by client
    if "fantasia" in df_val.columns:
        mask_cli = df_val["fantasia"] == client_id
    elif "cliente_id" in df_val.columns:
        mask_cli = df_val["cliente_id"] == client_id
    else:
        return {"client_id": client_id, "perfil": "", "negocios": [], "dates": []}

    df_c = df_val[mask_cli].copy()

    # Apply date/filter masks
    if start_date:
        df_c = df_c[df_c["fecha"] >= pd.to_datetime(start_date)]
    if end_date:
        df_c = df_c[df_c["fecha"] <= pd.to_datetime(end_date)]
    if profiles and "perfil" in df_c.columns:
        df_c = df_c[df_c["perfil"].isin(profiles)]
    if neg and "neg" in df_c.columns:
        df_c = df_c[df_c["neg"].isin(neg)]
    if subneg and "subneg" in df_c.columns:
        df_c = df_c[df_c["subneg"].isin(subneg)]
    if products and "descripcion" in df_c.columns:
        df_c = df_c[df_c["descripcion"].isin(products)]

    if df_c.empty:
        return {"client_id": client_id, "perfil": "", "negocios": [], "dates": []}

    # Ensure columns
    if "articulo" not in df_c.columns and "codigo_serie" in df_c.columns:
        df_c["articulo"] = df_c["codigo_serie"].astype(str)
    if "unidad_medida" not in df_c.columns:
        df_c["unidad_medida"] = "Unid."
    if "nivel_agregacion" not in df_c.columns:
        df_c["nivel_agregacion"] = "ARTICULO"
    if "neg" not in df_c.columns:
        df_c["neg"] = "Sin Negocio"
    if "subneg" not in df_c.columns:
        df_c["subneg"] = "General"

    # Ensure neg/subneg from main df if missing
    if df_main is not None and not df_main.empty and "descripcion" in df_c.columns:
        if "neg" in df_main.columns:
            neg_map = df_main[["descripcion", "neg"]].drop_duplicates("descripcion").set_index("descripcion")["neg"].to_dict()
            df_c["neg"] = df_c["descripcion"].map(neg_map).fillna(df_c.get("neg", "Sin Negocio"))
        if "subneg" in df_main.columns:
            sub_map = df_main[["descripcion", "subneg"]].drop_duplicates("descripcion").set_index("descripcion")["subneg"].to_dict()
            df_c["subneg"] = df_c["descripcion"].map(sub_map).fillna(df_c.get("subneg", "General"))

    first = df_c.iloc[0]
    perfil = str(first.get("perfil", ""))
    neg_val = str(first.get("neg", ""))

    # Max hist date for growth adjustment
    max_hist_date = None
    if not df_main.empty and "tipo" in df_main.columns:
        mhd = df_main[df_main["tipo"] == "hist"]["fecha"].max()
        if pd.notna(mhd):
            max_hist_date = mhd

    # Pivot: articulo × fecha → yhat_cliente
    val_col = "yhat_cliente"
    if val_col not in df_c.columns:
        val_col = next((c for c in ("yhat_cliente", "yhat", "monto_yhat") if c in df_c.columns), None)
    if val_col is None:
        return {"client_id": client_id, "perfil": perfil, "negocios": [], "dates": []}

    grp_keys = ["articulo", "descripcion", "unidad_medida", "nivel_agregacion", "neg", "subneg", "fecha"]
    grp_keys = [k for k in grp_keys if k in df_c.columns]
    agg = df_c.groupby(grp_keys)[val_col].sum().reset_index()

    all_dates = sorted(agg["fecha"].unique())
    date_strs = [d.strftime("%Y-%m") for d in all_dates]

    # Build price map for this client
    def get_price(articulo, descripcion, nivel):
        key = _norm_key(articulo)
        p = price_lookup.get("CODIGO", {}).get(key, 0)
        if p == 0:
            kd = _norm_key(descripcion)
            if nivel == "FAMILIA":
                p = price_lookup.get("FAMILIA", {}).get(kd, 0)
            else:
                p = price_lookup.get("ARTICULO", {}).get(kd, 0)
                if p == 0:
                    p = price_lookup.get("FAMILIA", {}).get(kd, 0)
        return float(p)

    # Group by neg → subneg
    negocios_out = []
    for neg_name, df_neg in agg.groupby("neg"):
        subnegs_out = []
        subneg_col = "subneg" if "subneg" in df_neg.columns else None
        for subneg_name, df_sub in (df_neg.groupby("subneg") if subneg_col else [("General", df_neg)]):
            products_out = []
            for _, prow in df_sub.groupby(["articulo", "descripcion"]):
                art = str(prow.iloc[0]["articulo"])
                desc = str(prow.iloc[0]["descripcion"])
                um = str(prow.iloc[0].get("unidad_medida", "Unid."))
                nivel = str(prow.iloc[0].get("nivel_agregacion", "ARTICULO"))
                precio = get_price(art, desc, nivel)

                months_data = {}
                for d, ds in zip(all_dates, date_strs):
                    row_d = prow[prow["fecha"] == d]
                    orig = float(row_d[val_col].sum()) if not row_d.empty else 0.0
                    adj = orig
                    pct = 0.0
                    # Saved override takes priority over global growth_pct
                    saved_pct = saved_overrides.get((art, ds), None)
                    if saved_pct is not None:
                        pct = saved_pct
                        adj = orig * (1.0 + pct / 100.0)
                    elif max_hist_date and d > max_hist_date and growth_pct != 0:
                        months_diff = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                        t = months_diff
                        ra = growth_pct / 100.0
                        rm = (1 + ra) ** (1 / 12.0) - 1 if growth_pct != 0 else 0.0
                        factor = (1 + rm) ** t if growth_pct != 0 else 1.0
                        adj = orig * factor
                        pct = round(rm * 100, 4)
                    months_data[ds] = {
                        "orig": round(orig, 2),
                        "pct": round(pct, 4),
                        "nuevo": round(adj, 2),
                        "money": round(adj * precio, 0),
                    }

                total_nuevo = sum(v["nuevo"] for v in months_data.values())
                total_money = round(total_nuevo * precio, 0)
                if total_nuevo > 0 or any(v["orig"] > 0 for v in months_data.values()):
                    products_out.append({
                        "articulo": art,
                        "descripcion": desc,
                        "unidad_medida": um,
                        "nivel_agregacion": nivel,
                        "precio": round(precio, 2),
                        "total_nuevo": round(total_nuevo, 2),
                        "total_money": total_money,
                        "months": months_data,
                    })

            # Sort by total_money desc
            products_out.sort(key=lambda x: x["total_money"], reverse=True)
            subnegs_out.append({"subneg": str(subneg_name), "products": products_out})

        negocios_out.append({"neg": str(neg_name), "subnegs": subnegs_out})

    return {
        "client_id": client_id,
        "perfil": perfil,
        "neg": neg_val,
        "negocios": negocios_out,
        "dates": date_strs,
        "max_hist_date": max_hist_date.strftime("%Y-%m") if max_hist_date else None,
        "growth_pct": growth_pct,
    }
