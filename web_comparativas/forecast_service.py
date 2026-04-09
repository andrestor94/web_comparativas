"""
Forecast data service — adapted from "Forecast ultimo/dashboard/data_loader.py"
Pure Python/pandas, zero Streamlit dependencies.
"""
from __future__ import annotations

import logging
import threading
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from web_comparativas.models import engine
except ImportError:
    engine = None

logger = logging.getLogger("wc.forecast")

# ---------------------------------------------------------------------------
# Paths
# ---------------------------------------------------------------------------
BASE_DIR = Path(__file__).parent
# Now using inline data folder packaged for the deployed repository.
FORECAST_DIR = BASE_DIR / "data" / "forecast_data"

# Original Forecast directory
_ORIG_FORECAST_DIR = FORECAST_DIR
# Prepared fact_forecast_valorizado.csv (comma-sep, monto_yhat pre-computed, 702K rows)
_VALORIZADO_PREPARED = _ORIG_FORECAST_DIR / "fact_forecast_valorizado.csv"
# Fallback: SIEM copy (semicolon-sep, only 110K rows — incomplete)
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


def save_client_overrides(client_id: str, overrides: list[dict]) -> None:
    """Persist per-product % adjustments for a client.
    Each override: {articulo: str, date: 'YYYY-MM', pct: float}
    """
    with _overrides_lock:
        store = _client_overrides.setdefault(client_id, {})
        for ov in overrides:
            key = (str(ov["articulo"]), str(ov["date"]))
            store[key] = float(ov["pct"])


def clear_client_overrides(client_id: str) -> None:
    """Remove all overrides for a client (full undo to CSV baseline)."""
    with _overrides_lock:
        _client_overrides.pop(client_id, None)


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
    # Priority: original prepared file (702K rows, comma-sep, monto_yhat pre-computed)
    # Fallback: SIEM copy (110K rows — incomplete, use only if original unavailable)
    df_val = pd.DataFrame()
    _use_prepared = _VALORIZADO_PREPARED.exists()
    _val_file = _VALORIZADO_PREPARED if _use_prepared else (VALORIZADO_FILE if VALORIZADO_FILE.exists() else None)
    if _val_file is not None:
        try:
            if _use_prepared:
                # Prepared file is comma-separated, no decimal override needed
                df_val = pd.read_csv(str(_val_file), sep=",", encoding="utf-8-sig", low_memory=False)
                logger.info("[FORECAST] Loaded valorizado from PREPARED file: %d rows", len(df_val))
            else:
                df_val = pd.read_csv(str(_val_file), sep=";", decimal=",", encoding="utf-8-sig", low_memory=False)
                logger.info("[FORECAST] Loaded valorizado from FALLBACK file: %d rows", len(df_val))
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

def get_data() -> dict[str, Any]:
    global _data_cache
    if _data_cache:
        return _data_cache
    with _cache_lock:
        if not _data_cache:
            _data_cache = _load_all_data()
    return _data_cache


def reload_data() -> None:
    global _data_cache
    with _cache_lock:
        _data_cache = _load_all_data()


def get_filter_options() -> dict:
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


def get_product_list(profiles: list | None = None, neg: list | None = None) -> list[dict]:
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        df = _query_db("forecast_main", profiles=profiles, neg=neg)
        import json
        with engine.begin() as conn:
            labs_df = pd.read_sql("SELECT * FROM forecast_product_labs", conn)
        lab_map = {row["codigo_serie"]: json.loads(row["laboratorios"]) for _, row in labs_df.iterrows()} if not labs_df.empty else {}
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
        df = _query_db("forecast_main", profiles=profiles, neg=neg, subneg=subneg, products=products)
        df_val = _query_db("forecast_valorizado", start_date=start_date, end_date=end_date, profiles=profiles, neg=neg, subneg=subneg, products=products)
        df_val = _get_patched_df_val(df_val)
    else:
        data = get_data()
        df = data.get("df_main", pd.DataFrame())
        df_val = _get_patched_df_val()
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
        else:
            df_fcst = pd.DataFrame(columns=["fecha", "Total_Forecast", "Total_Li", "Total_Ls"])
    else:
        df_f2 = df_filt[df_filt.get("Etiqueta_Upper", pd.Series()) == "Proyección"].groupby("fecha").agg(
            Total_Forecast=("yhat", "sum"),
            Total_Li=("li", "sum"),
            Total_Ls=("ls", "sum"),
        ).reset_index()
        df_fcst = df_f2

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
        ["Total_Forecast", "Total_Li", "Total_Ls", "Total_Adj"],
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
        df_val = _query_db("forecast_valorizado", start_date=start_date, end_date=end_date, profiles=profiles, neg=neg, subneg=subneg, products=products)
        df_val = _get_patched_df_val(df_val)
    else:
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
        df_val = _query_db("forecast_valorizado", start_date=start_date, end_date=end_date, profiles=profiles, neg=neg, subneg=subneg, products=products, extra_where=f"cliente_id = '{client_id}'")
        df_main = _query_db("forecast_main", profiles=profiles, neg=neg, subneg=subneg, products=products)
        price_lookup = {}
    else:
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
