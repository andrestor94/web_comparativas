"""
Forecast data service — adapted from "Forecast ultimo/dashboard/data_loader.py"
Pure Python/pandas, zero Streamlit dependencies.
"""
from __future__ import annotations

import copy
import csv
import json
import logging
import os
import re
import threading
import time
import datetime as dt
from collections import OrderedDict
from functools import wraps
from pathlib import Path
from typing import Any

import numpy as np
import pandas as pd

try:
    from web_comparativas.models import engine, SessionLocal, ForecastUserOverride, ForecastManualClient, ForecastManualEntry, ForecastChangeRequest
except ImportError:
    engine = None
    SessionLocal = None
    ForecastUserOverride = None
    ForecastManualClient = None  # type: ignore[assignment,misc]
    ForecastManualEntry = None   # type: ignore[assignment,misc]
    ForecastChangeRequest = None  # type: ignore[assignment,misc]

try:
    from sqlalchemy import text as _sa_text
except ImportError:
    _sa_text = None  # type: ignore[assignment]

logger = logging.getLogger("wc.forecast")
logger.setLevel(logging.INFO)


def _forecast_diag(message: str, *args) -> None:
    """Emit critical Forecast diagnostics in Render even when logger config is quiet."""
    text = message % args if args else message
    logger.info(text)
    print(text, flush=True)


def _forecast_diag_warn(message: str, *args) -> None:
    text = message % args if args else message
    logger.warning(text)
    print(text, flush=True)


def _forecast_payload_bytes(payload: Any) -> int:
    if isinstance(payload, (bytes, bytearray)):
        return len(payload)
    try:
        return len(json.dumps(payload, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        return -1


def _forecast_result_rows(payload: Any) -> int:
    if isinstance(payload, (bytes, bytearray)):
        return -1  # bytes are pre-serialized; row count not available without deserialization
    if isinstance(payload, list):
        return len(payload)
    if not isinstance(payload, dict):
        return -1
    for key in ("rows", "forecast", "ids", "history", "records", "profiles"):
        value = payload.get(key)
        if isinstance(value, list):
            return len(value)
    return -1

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
FACT_2026_FILE  = FORECAST_DIR / "facturacion_real_2026_sin_neg2.csv"

_cache_lock = threading.Lock()
_data_cache: dict[str, Any] = {}
FORECAST_OVERRIDE_SOURCE = "forecast"
FORECAST_OVERRIDE_CONTEXT = "default"
FORECAST_SCOPE_SUBNEG = "subnegocio"
FORECAST_SCOPE_PRODUCT = "producto"
FORECAST_SCOPE_CELL = "celda"
_overrides_lock = threading.Lock()
_client_overrides: dict[str, dict[tuple, float]] = {}

# ---------------------------------------------------------------------------
# Client override store (in-memory persistence of modal edits)
# Stores the % adjustment per (client_id, articulo, date_str).
# factor = 1 + pct/100  →  nuevo_yhat = orig_yhat * factor
# Key: client_id → {(articulo, date_str): pct_float}
# ---------------------------------------------------------------------------

# ---------------------------------------------------------------------------
# Service-level TTL response cache
# Caches the serialisable dict/list returned by each public get_* function.
# Key: function name + normalised filter args (lists → sorted for stable keys).
# TTL: 5 min for data, 15 min for filter-options (rarely changes mid-session).
# Cleared on: a) reload_data(), b) save_client_overrides() (overrides alter data).
# ---------------------------------------------------------------------------
_resp_cache: "OrderedDict[str, tuple[float, Any, int]]" = OrderedDict()
_resp_cache_lock = threading.Lock()
_resp_inflight: dict[str, threading.Event] = {}
_RESP_TTL_DATA   = 600   # 10 min — chart, table, treemap, product-list
_RESP_TTL_STATIC = 1800  # 30 min — filter-options
_RESP_CACHE_MAX_ITEMS = 128
_RESP_CACHE_MAX_ENTRY_BYTES = 5_000_000   # 5 MB — allows client-table (~1.7 MB) and treemap to cache
_RESP_CACHE_MAX_TOTAL_BYTES = 64_000_000  # 64 MB total
_OVERRIDE_PCT_TOL = 5e-4


def _is_all_marker(value: Any) -> bool:
    return str(value or "").strip().lower() in {"", "all", "todos", "todo", "__all__", "none", "null"}


def _norm_filter_list(value: Any) -> list[str]:
    if value is None:
        return []
    if not isinstance(value, (list, tuple, set)):
        value = [value]
    normalized = {
        str(v).strip()
        for v in value
        if v is not None and not _is_all_marker(v)
    }
    return sorted(normalized)


def _resp_key(fn_name: str, *args, **kwargs) -> str:
    """Stable JSON key from fn name + args.

    Empty filters, "Todos" markers and None collapse to the same key so the
    cache is reused after Limpiar, reloads and restored UI state.
    """
    def _norm(v):
        if isinstance(v, (list, tuple, set)):
            return _norm_filter_list(v)
        if isinstance(v, str) and _is_all_marker(v):
            return ""
        return v
    return json.dumps(
        [fn_name, [_norm(a) for a in args], {k: _norm(v) for k, v in sorted(kwargs.items())}],
        default=str,
    )


def _resp_get(key: str, ttl: float) -> "Any | None":
    """Return cached value (deserialized from JSON bytes) or None on miss/expiry."""
    with _resp_cache_lock:
        entry = _resp_cache.get(key)
    if entry is None:
        return None
    ts, body, _size = entry
    if (time.monotonic() - ts) >= ttl:
        with _resp_cache_lock:
            _resp_cache.pop(key, None)
        return None
    with _resp_cache_lock:
        _resp_cache.move_to_end(key)
    try:
        return json.loads(body)
    except Exception:
        return None


def _resp_set(key: str, value: Any) -> None:
    """Serialize value to JSON bytes and store in cache.
    Stores bytes instead of a Python object to avoid deepcopy overhead on reads.
    """
    try:
        body = json.dumps(value, default=str, separators=(",", ":")).encode("utf-8")
    except Exception:
        return
    approx_size = len(body)
    if approx_size > _RESP_CACHE_MAX_ENTRY_BYTES:
        logger.info("[FORECAST cache] SKIP oversized entry bytes=%s key=%.120s", approx_size, key)
        return
    with _resp_cache_lock:
        _resp_cache[key] = (time.monotonic(), body, approx_size)
        _resp_cache.move_to_end(key)
        total = sum(entry[2] for entry in _resp_cache.values())
        while (
            len(_resp_cache) > _RESP_CACHE_MAX_ITEMS
            or total > _RESP_CACHE_MAX_TOTAL_BYTES
        ) and _resp_cache:
            _, (_, _, dropped_size) = _resp_cache.popitem(last=False)
            total -= dropped_size


def clear_response_cache() -> None:
    """Flush the service-level response cache (after reload or client-save)."""
    with _resp_cache_lock:
        _resp_cache.clear()
        _resp_inflight.clear()
    with _PROD_CODE_CACHE_LOCK:
        _PROD_CODE_CACHE.clear()
    with _LAB_CODE_CACHE_LOCK:
        _LAB_CODE_CACHE.clear()
    # Also clear canonical series cache so it refreshes with new data
    global _CANONICAL_SERIES_CACHE, _FACT_2026_DF_CACHE
    with _CANONICAL_SERIES_LOCK:
        _CANONICAL_SERIES_CACHE = None
    with _FACT_2026_DF_LOCK:
        _FACT_2026_DF_CACHE = None
    logger.info("[FORECAST cache] Response cache cleared.")


def clear_user_cache(user_id: int) -> None:
    """Flush only the cache entries that belong to a specific user.

    Uses a regex to match the exact JSON token '"user_id": <N>[,}]' so that
    user_id=2 does NOT accidentally invalidate entries for user_id=12 or user_id=20.
    The cache key is produced by _resp_key() as a json.dumps array (uncompact separators),
    so the integer user_id always appears as: "user_id": <N>, or "user_id": <N>}
    """
    uid_pat = re.compile(rf'"user_id": {int(user_id)}[,}}]')
    with _resp_cache_lock:
        to_delete = [k for k in _resp_cache if uid_pat.search(k)]
        for k in to_delete:
            del _resp_cache[k]
    logger.info("[FORECAST cache] User %s cache cleared (%d keys).", user_id, len(to_delete))


def _clear_cache_for_override_save(user_id: int) -> None:
    """Targeted cache invalidation after an override save.

    Clears two domains that become stale after user_id saves an override:
      1. The saving user's own cached results (keyed by user_id).
      2. Admin/all-users view entries (keyed with "is_admin": true) — admins see
         ALL users' overrides, so their cache must refresh after any user saves.

    Other regular users' cached results are NOT affected (their overrides didn't
    change), so they keep their warm cache and avoid unnecessary recomputation.
    """
    clear_user_cache(user_id)
    admin_key_count = 0
    with _resp_cache_lock:
        admin_keys = [k for k in _resp_cache if '"is_admin": true' in k]
        for k in admin_keys:
            del _resp_cache[k]
        admin_key_count = len(admin_keys)
    logger.info(
        "[FORECAST cache] Override save user=%s: cleared user keys + %d admin entries.",
        user_id, admin_key_count,
    )


# ---------------------------------------------------------------------------
# Module-level short-lived caches for repeated read-only SQL queries
# These queries are identical across users and requests within a session window.
# ---------------------------------------------------------------------------

_MAX_HIST_DATE_CACHE: "tuple[float, pd.Timestamp | None] | None" = None
_MAX_HIST_DATE_TTL = 600  # 10 min
_MAX_HIST_DATE_LOCK = threading.Lock()


def _get_max_hist_date_cached() -> "pd.Timestamp | None":
    """Return MAX(fecha) WHERE tipo='hist' from forecast_main, cached for 10 min."""
    global _MAX_HIST_DATE_CACHE
    with _MAX_HIST_DATE_LOCK:
        if _MAX_HIST_DATE_CACHE is not None:
            ts, val = _MAX_HIST_DATE_CACHE
            if time.monotonic() - ts < _MAX_HIST_DATE_TTL:
                return val
        df = _query_agg("SELECT MAX(fecha) AS mhd FROM forecast_main WHERE tipo = 'hist'")
        result: "pd.Timestamp | None" = None
        if not df.empty and pd.notna(df["mhd"].iloc[0]):
            result = pd.to_datetime(df["mhd"].iloc[0])
        _MAX_HIST_DATE_CACHE = (time.monotonic(), result)
        logger.debug("[FORECAST cache] _get_max_hist_date_cached refreshed → %s", result)
        return result


_PRECIO_MAP_CACHE: "tuple[float, dict] | None" = None
_PRECIO_MAP_TTL = 3600  # 1 hour — precio barely changes during a session
_PRECIO_MAP_LOCK = threading.Lock()


def _get_precio_map_cached() -> dict:
    """Return {codigo_serie: avg_precio} from forecast_main, cached for 1 hour."""
    global _PRECIO_MAP_CACHE
    with _PRECIO_MAP_LOCK:
        if _PRECIO_MAP_CACHE is not None:
            ts, val = _PRECIO_MAP_CACHE
            if time.monotonic() - ts < _PRECIO_MAP_TTL:
                return val
        df = _query_agg(
            "SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS precio "
            "FROM forecast_main GROUP BY codigo_serie"
        )
        result: dict = {}
        if not df.empty:
            result = df.set_index("codigo_serie")["precio"].to_dict()
        _PRECIO_MAP_CACHE = (time.monotonic(), result)
        logger.debug("[FORECAST cache] _get_precio_map_cached refreshed — %d series.", len(result))
        return result


_FORECAST_PERIODS_CACHE: "tuple[float, list] | None" = None
_FORECAST_PERIODS_TTL = 600  # 10 min
_FORECAST_PERIODS_LOCK = threading.Lock()
_PROD_CODE_CACHE: "OrderedDict[str, tuple[float, list]]" = OrderedDict()
_PROD_CODE_CACHE_TTL = 600
_PROD_CODE_CACHE_LOCK = threading.Lock()
_LAB_CODE_CACHE: "OrderedDict[str, tuple[float, list]]" = OrderedDict()
_LAB_CODE_CACHE_TTL = 900
_LAB_CODE_CACHE_LOCK = threading.Lock()

# Cache for canonical series codes from forecast_valorizado.
# Used to replace the expensive IN(SELECT DISTINCT...) subquery in hist queries.
# TTL=1h — series set changes only when data is reloaded.
_CANONICAL_SERIES_CACHE: "tuple[float, list[str]] | None" = None
_CANONICAL_SERIES_TTL = 3600
_CANONICAL_SERIES_LOCK = threading.Lock()

# In-memory cache for the parsed facturacion_real CSV (32 MB, 206K rows).
# Re-reading + re-parsing this CSV adds 600-700ms to every cold chart-data call
# in the local light path. Caching the parsed DataFrame avoids this on 2nd+ call.
# TTL=1h — data changes only with a new CSV file upload / data reload.
_FACT_2026_DF_CACHE: "tuple[float, Any] | None" = None
_FACT_2026_DF_TTL = 3600
_FACT_2026_DF_LOCK = threading.Lock()


def _fact_2026_closed_month_cap(today: "dt.date | None" = None) -> "dt.date":
    """Tope superior DINÁMICO para la facturación real 2026: primer día del mes EN CURSO.

    La serie del gráfico y el KPI fact_2026 muestran SOLO meses CERRADOS, es decir
    `fecha < este_tope`. Regla de negocio: un mes se muestra recién cuando ya cerró
    por calendario (su último día pasó), para no graficar el mes en curso parcial
    (caída falsa).

    Se calcula contra HOY (`date.today()` del servidor), NO contra MAX(fecha) de
    forecast_fact_2026: si se basara en MAX, el mes en curso —que puede estar cargado
    parcialmente en la tabla— se mostraría igual, que es justo lo que se quiere evitar.
    Es dinámico (se recalcula solo cada mes); no hay fechas hardcodeadas. Ej.: hoy
    2026-06-17 → tope 2026-06-01 → muestra hasta mayo; el 2026-07-01 → muestra junio.

    Nota TZ: usa la fecha LOCAL del servidor. Render corre en UTC; en el instante del
    cambio de mes el corte puede adelantarse hasta ~3 h respecto de Argentina (UTC-3).
    Efecto despreciable (solo afecta unas horas alrededor de la medianoche de fin de mes).
    """
    d = today if today is not None else dt.date.today()
    return d.replace(day=1)


def _get_fact_2026_df_cached() -> "pd.DataFrame":
    """Return the parsed facturacion_real_2026 DataFrame, cached in memory for 1 h.

    Only used by the local light path (_local_get_chart_data_light).
    The full _load_all_data() path still reads the CSV independently and stores
    it as _data_cache['df_fact_2026'] — this cache is separate and additive.
    """
    import pandas as _pd
    global _FACT_2026_DF_CACHE
    with _FACT_2026_DF_LOCK:
        if _FACT_2026_DF_CACHE is not None:
            ts, val = _FACT_2026_DF_CACHE
            if time.monotonic() - ts < _FACT_2026_DF_TTL:
                return val
        if not FACT_2026_FILE.exists():
            return _pd.DataFrame()
        try:
            df = _pd.read_csv(
                str(FACT_2026_FILE),
                sep=";",
                encoding="utf-8-sig",
                low_memory=False,
                usecols=lambda c: str(c).strip() in {"fecha", "codigo_serie", "perfil", "imp_hist"},
            )
            df["fecha"] = _pd.to_datetime(df["fecha"], errors="coerce", dayfirst=True)
            df["imp_hist"] = _pd.to_numeric(
                df["imp_hist"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
                errors="coerce",
            ).fillna(0)
            _FACT_2026_DF_CACHE = (time.monotonic(), df)
            logger.info("[FORECAST cache] _get_fact_2026_df_cached loaded %d rows from CSV", len(df))
            return df
        except Exception as exc:
            logger.warning("[FORECAST cache] _get_fact_2026_df_cached error: %s", exc)
            return _pd.DataFrame()


def _get_forecast_periods_cached() -> list:
    """Return sorted list of fecha strings from forecast_valorizado, cached for 10 min."""
    global _FORECAST_PERIODS_CACHE
    with _FORECAST_PERIODS_LOCK:
        if _FORECAST_PERIODS_CACHE is not None:
            ts, val = _FORECAST_PERIODS_CACHE
            if time.monotonic() - ts < _FORECAST_PERIODS_TTL:
                return val
        periods_df = _query_agg(
            "SELECT DISTINCT fecha FROM forecast_valorizado ORDER BY fecha"
        )
        result = [
            str(r["fecha"])[:10]
            for _, r in periods_df.iterrows()
            if pd.notna(r["fecha"])
        ]
        _FORECAST_PERIODS_CACHE = (time.monotonic(), result)
        logger.debug("[FORECAST cache] _get_forecast_periods_cached refreshed — %d periods.", len(result))
        return result


def _get_canonical_series_cached() -> list[str]:
    """Return list of distinct codigo_serie from forecast_valorizado, cached for 1 hour.

    Used to replace the expensive correlated subquery
    ``AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado)``
    with a pre-resolved literal list via _safe_in()/ANY(ARRAY[...]).
    Returns [] on any error (caller falls back to original subquery).
    """
    global _CANONICAL_SERIES_CACHE
    with _CANONICAL_SERIES_LOCK:
        if _CANONICAL_SERIES_CACHE is not None:
            ts, val = _CANONICAL_SERIES_CACHE
            if time.monotonic() - ts < _CANONICAL_SERIES_TTL:
                return val
        df = _query_agg(
            "SELECT DISTINCT codigo_serie FROM forecast_valorizado WHERE codigo_serie IS NOT NULL"
        )
        result: list[str] = []
        if not df.empty and "codigo_serie" in df.columns:
            result = sorted({str(c).strip() for c in df["codigo_serie"].tolist() if c is not None and str(c).strip()})
        _CANONICAL_SERIES_CACHE = (time.monotonic(), result)
        logger.info("[FORECAST cache] canonical_series refreshed — %d series.", len(result))
        return result


def _clear_canonical_series_cache() -> None:
    global _CANONICAL_SERIES_CACHE
    with _CANONICAL_SERIES_LOCK:
        _CANONICAL_SERIES_CACHE = None


def _with_resp_cache(ttl: float):
    """Decorator: transparently cache the return value of a public get_* function."""
    def decorator(fn):
        @wraps(fn)
        def wrapper(*args, **kwargs):
            _cache_t0 = time.perf_counter()
            key = _resp_key(fn.__name__, *args, **kwargs)
            cached = _resp_get(key, ttl)
            if cached is not None:
                _forecast_diag(
                    "[FORECAST cache] HIT fn=%s total_ms=%.1f rows=%s json_bytes=%s",
                    fn.__name__,
                    (time.perf_counter() - _cache_t0) * 1000,
                    _forecast_result_rows(cached),
                    _forecast_payload_bytes(cached),
                )
                return cached
            owner = False
            with _resp_cache_lock:
                event = _resp_inflight.get(key)
                if event is None:
                    event = threading.Event()
                    _resp_inflight[key] = event
                    owner = True
            if not owner:
                _forecast_diag("[FORECAST cache] WAIT fn=%s", fn.__name__)
                event.wait(timeout=55)
                cached = _resp_get(key, ttl)
                if cached is not None:
                    _forecast_diag(
                        "[FORECAST cache] HIT_AFTER_WAIT fn=%s total_ms=%.1f rows=%s json_bytes=%s",
                        fn.__name__,
                        (time.perf_counter() - _cache_t0) * 1000,
                        _forecast_result_rows(cached),
                        _forecast_payload_bytes(cached),
                    )
                    return cached
            _forecast_diag("[FORECAST cache] MISS fn=%s", fn.__name__)
            try:
                result = fn(*args, **kwargs)
                _resp_set(key, result)
                _forecast_diag(
                    "[FORECAST cache] STORE fn=%s total_ms=%.1f rows=%s json_bytes=%s",
                    fn.__name__,
                    (time.perf_counter() - _cache_t0) * 1000,
                    _forecast_result_rows(result),
                    _forecast_payload_bytes(result),
                )
                return result
            finally:
                if owner:
                    with _resp_cache_lock:
                        done_event = _resp_inflight.pop(key, None)
                    if done_event is not None:
                        done_event.set()
        return wrapper
    return decorator


def _monthly_pct_from_annual_growth(growth_pct: float) -> float:
    """Translate an annual growth percentage to the compounded monthly % stored in overrides."""
    growth_pct = float(growth_pct or 0.0)
    if abs(growth_pct) <= 1e-12:
        return 0.0
    base = 1.0 + growth_pct / 100.0
    if base < 0:
        # Fractional power of a negative base is undefined in real numbers; treated as invalid.
        return float("nan")
    if base == 0:
        return -100.0  # -100% anual → proyección cero
    return (base ** (1.0 / 12.0) - 1.0) * 100.0


def _annual_growth_from_monthly_pct(monthly_pct: float) -> float:
    monthly_pct = float(monthly_pct or 0.0)
    if abs(monthly_pct) <= 1e-12:
        return 0.0
    return (((1.0 + monthly_pct / 100.0) ** 12) - 1.0) * 100.0


def get_forecast_effective_month(today: dt.date | None = None, cutoff_day: int = 20) -> str:
    """Return the first month ("YYYY-MM") from which a forecast change takes effect.

    Rule (cutoff = day 20, inclusive):
      - today.day <= cutoff_day  → effective from NEXT month
      - today.day  > cutoff_day  → effective from the month AFTER next

    Examples:
      2026-05-12 → "2026-06"   (day 12 <= 20)
      2026-05-20 → "2026-06"   (day 20 <= 20)
      2026-05-21 → "2026-07"   (day 21 > 20)
      2026-12-20 → "2027-01"
      2026-12-21 → "2027-02"
    """
    if today is None:
        today = dt.date.today()
    offset = 1 if today.day <= cutoff_day else 2
    # Advance by `offset` months (handles year wrap)
    m = today.month + offset
    y = today.year + (m - 1) // 12
    m = ((m - 1) % 12) + 1
    return f"{y}-{m:02d}"


def _is_effective_override_pct(pct: float, growth_pct: float, tol: float = _OVERRIDE_PCT_TOL) -> bool:
    """Return True only when an override differs materially from the current default growth."""
    pct = float(pct)
    default_pct = _monthly_pct_from_annual_growth(growth_pct)
    return bool(np.isfinite(pct)) and abs(pct - default_pct) > tol


def _clean_override_text(value: Any) -> str:
    return str(value or "").strip()


def clean_group_key(value: Any) -> str:
    import re

    text = _clean_override_text(value)
    text = re.sub(r"\(\s*\d+\s*c?\s*\)$", "", text, flags=re.I).strip()
    text = re.sub(r"\s+\d+\s*cuentas?$", "", text, flags=re.I).strip()
    return text


def _normalize_scope(scope: str | None) -> str:
    raw = _clean_override_text(scope).lower()
    aliases = {
        "subneg": FORECAST_SCOPE_SUBNEG,
        "subnegocio": FORECAST_SCOPE_SUBNEG,
        "product": FORECAST_SCOPE_PRODUCT,
        "producto": FORECAST_SCOPE_PRODUCT,
        "cell": FORECAST_SCOPE_CELL,
        "celda": FORECAST_SCOPE_CELL,
    }
    return aliases.get(raw, raw or FORECAST_SCOPE_CELL)


# ---------------------------------------------------------------------------
# SQL override helpers
# ---------------------------------------------------------------------------

def _normalize_month_key(value: str | None) -> str:
    raw = _clean_override_text(value)
    if not raw:
        return ""
    if len(raw) >= 7 and raw[4] == "-":
        return raw[:7]
    try:
        return pd.to_datetime(raw).strftime("%Y-%m")
    except Exception:
        return raw[:7]


def _override_identity(scope: str | None, subneg: str = "", codigo_serie: str = "", forecast_month: str = "") -> tuple[str, str, str, str]:
    return (
        _normalize_scope(scope),
        _clean_override_text(subneg),
        _clean_override_text(codigo_serie),
        _normalize_month_key(forecast_month),
    )


def _fetch_override_records(
    user_id: int | None,
    client_selector: str | None = None,
    client_selectors: list[str] | None = None,
    *,
    all_users: bool = False,
) -> list[Any]:
    if SessionLocal is None or ForecastUserOverride is None:
        return []
    if not all_users and not user_id:
        return []
    with SessionLocal() as session:
        q = (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.source_module == FORECAST_OVERRIDE_SOURCE)
            .filter(ForecastUserOverride.is_active.is_(True))
        )
        if not all_users:
            q = q.filter(ForecastUserOverride.user_id == int(user_id))
        if client_selector is not None:
            q = q.filter(ForecastUserOverride.client_selector == _clean_override_text(client_selector))
        elif client_selectors:
            q = q.filter(
                ForecastUserOverride.client_selector.in_(
                    [_clean_override_text(v) for v in client_selectors if _clean_override_text(v)]
                )
            )
        # When querying all users, sort by updated_at ascending so later saves win on conflict
        if all_users:
            q = q.order_by(ForecastUserOverride.updated_at.asc())
        return list(q.all())


def _override_record_user_ids(records: list[Any], limit: int = 20) -> list[int]:
    ids: list[int] = []
    for rec in records or []:
        try:
            value = int(getattr(rec, "user_id"))
        except Exception:
            continue
        if value not in ids:
            ids.append(value)
        if len(ids) >= limit:
            break
    return ids


def _forecast_adjustment_summary(records: list[dict]) -> dict[str, Any]:
    if not records:
        return {
            "has_adjusted_series": False,
            "adjusted_diff_sum": 0.0,
            "first_total_adj": None,
            "first_total_user_adj": None,
            "last_total_adj": None,
            "last_total_user_adj": None,
        }

    diff_sum = 0.0
    for row in records:
        try:
            diff_sum += abs(float(row.get("Total_User_Adj") or 0) - float(row.get("Total_Adj") or 0))
        except Exception:
            continue

    first = records[0]
    last = records[-1]
    return {
        "has_adjusted_series": diff_sum > 0.5,
        "adjusted_diff_sum": round(diff_sum, 2),
        "first_total_adj": first.get("Total_Adj"),
        "first_total_user_adj": first.get("Total_User_Adj"),
        "last_total_adj": last.get("Total_Adj"),
        "last_total_user_adj": last.get("Total_User_Adj"),
    }


def _build_override_maps(records: list[Any]) -> dict[str, Any]:
    subneg_map: dict[tuple[str, str], dict[str, Any]] = {}
    product_map: dict[tuple[str, str], dict[str, Any]] = {}
    cell_map: dict[tuple[str, str, str], dict[str, Any]] = {}
    subneg_growths: dict[str, dict[str, float]] = {}
    # effective_from_month per subneg: used to apply subneg growth only from the valid month
    subneg_effective_months: dict[str, dict[str, str | None]] = {}

    for rec in records:
        selector = _clean_override_text(getattr(rec, "client_selector", ""))
        if not selector:
            continue
        scope = _normalize_scope(getattr(rec, "override_scope", None))
        subneg = _clean_override_text(getattr(rec, "subneg", ""))
        codigo = _clean_override_text(getattr(rec, "codigo_serie", ""))
        month = _normalize_month_key(getattr(rec, "forecast_month", ""))
        monthly = getattr(rec, "effective_monthly_pct", None)
        annual = getattr(rec, "override_growth_pct", None)
        efm: str | None = getattr(rec, "effective_from_month", None)  # "YYYY-MM" or None
        if monthly is None and annual is not None:
            monthly = _monthly_pct_from_annual_growth(float(annual))
        if annual is None and monthly is not None:
            annual = _annual_growth_from_monthly_pct(float(monthly))
        monthly = float(monthly or 0.0)
        annual = float(annual or 0.0)
        # effective_from_month is carried in the payload so _resolve_override_for_row
        # can check temporal validity before applying the override.
        payload: dict[str, Any] = {"monthly_pct": monthly, "annual_pct": annual, "effective_from_month": efm}

        if scope == FORECAST_SCOPE_SUBNEG:
            subneg_map[(selector, subneg)] = payload
            subneg_growths.setdefault(selector, {})[subneg] = annual
            subneg_effective_months.setdefault(selector, {})[subneg] = efm
        elif scope == FORECAST_SCOPE_PRODUCT:
            product_map[(selector, codigo)] = payload
        elif scope == FORECAST_SCOPE_CELL and month:
            cell_map[(selector, codigo, month)] = payload

    selectors = sorted(
        {selector for selector, _ in subneg_map.keys()}
        | {selector for selector, _ in product_map.keys()}
        | {selector for selector, _, _ in cell_map.keys()}
    )
    return {
        "subneg": subneg_map,
        "product": product_map,
        "cell": cell_map,
        "subneg_growths": subneg_growths,
        "subneg_effective_months": subneg_effective_months,
        "selectors": selectors,
    }


def _consolidate_override_records(records: list[Any], label: str = "override") -> list[Any]:
    """Collapse active override records to the same effective map semantics.

    The existing map builder lets later records win for the same selector/scope key.
    This keeps that behavior while reducing repeated rows before dataframe matching.
    """
    latest: dict[tuple[str, str, str, str, str], Any] = {}
    for rec in records or []:
        selector = _clean_override_text(getattr(rec, "client_selector", ""))
        if not selector:
            continue
        key = (
            selector,
            _normalize_scope(getattr(rec, "override_scope", None)),
            _clean_override_text(getattr(rec, "subneg", "")),
            _clean_override_text(getattr(rec, "codigo_serie", "")),
            _normalize_month_key(getattr(rec, "forecast_month", "")),
        )
        latest[key] = rec
    consolidated = list(latest.values())
    maps = _build_override_maps(consolidated)
    _forecast_diag(
        "[FORECAST overrides] %s original=%s effective=%s selectors=%s reduction=%s",
        label,
        len(records or []),
        len(consolidated),
        len(maps.get("selectors", [])),
        len(records or []) - len(consolidated),
    )
    return consolidated


def _build_overrides_snapshot_locked(
    growth_pct: float | None = None,
    *,
    user_id: int | None = None,
    client_selector: str | None = None,
    is_admin: bool = False,
) -> dict[str, dict[tuple[str, str], float]]:
    records = _fetch_override_records(user_id, client_selector=client_selector, all_users=is_admin)
    maps = _build_override_maps(records)
    snapshot: dict[str, dict[tuple[str, str], float]] = {}
    for (selector, codigo, month), payload in maps["cell"].items():
        snapshot.setdefault(selector, {})[(codigo, month)] = float(payload["monthly_pct"])
    return snapshot


def _resolve_override_for_row(
    selector_candidates: list[str],
    subneg: str,
    codigo_serie: str,
    forecast_month: str,
    maps: dict[str, Any],
    base_growth_pct: float,
) -> dict[str, float | str]:
    month_key = _normalize_month_key(forecast_month)
    code_key = _clean_override_text(codigo_serie)
    subneg_key = _clean_override_text(subneg)

    # Cell-level: highest priority. Respect effective_from_month if set.
    for selector in selector_candidates:
        payload = maps["cell"].get((selector, code_key, month_key))
        if payload is not None:
            efm = payload.get("effective_from_month")
            if efm is None or month_key >= efm:
                return {"scope": FORECAST_SCOPE_CELL, **payload}
    # Product-level (no effective_from_month restriction — kept for legacy compat)
    for selector in selector_candidates:
        payload = maps["product"].get((selector, code_key))
        if payload is not None:
            efm = payload.get("effective_from_month")
            if efm is None or month_key >= efm:
                return {"scope": FORECAST_SCOPE_PRODUCT, **payload}
    # Subneg-level: apply only if forecast_month >= effective_from_month
    for selector in selector_candidates:
        payload = maps["subneg"].get((selector, subneg_key))
        if payload is not None:
            efm = payload.get("effective_from_month")
            if efm is None or month_key >= efm:
                return {"scope": FORECAST_SCOPE_SUBNEG, **payload}
    # Wildcard subneg (subneg=""): group-level override that applies to all subnegs of a client
    if subneg_key:
        for selector in selector_candidates:
            payload = maps["subneg"].get((selector, ""))
            if payload is not None:
                efm = payload.get("effective_from_month")
                if efm is None or month_key >= efm:
                    return {"scope": FORECAST_SCOPE_SUBNEG, **payload}
    return {
        "scope": "base",
        "monthly_pct": _monthly_pct_from_annual_growth(base_growth_pct),
        "annual_pct": float(base_growth_pct or 0.0),
        "effective_from_month": None,
    }


def _selector_candidates_for_df(df: pd.DataFrame) -> list[str]:
    selectors: set[str] = set()
    for col in ("fantasia", "cliente_id", "_cliente", "Cliente"):
        if col in df.columns:
            selectors.update(
                _clean_override_text(v)
                for v in df[col].dropna().astype(str).tolist()
                if _clean_override_text(v)
            )
    return sorted(selectors)


def _apply_override_effects_to_dataframe(
    df: pd.DataFrame,
    user_id: int | None,
    base_growth_pct: float = 0.0,
    max_hist_date: pd.Timestamp | None = None,
    *,
    _records=None,  # pre-fetched override records — avoids extra DB roundtrip
    is_admin: bool = False,
) -> tuple[pd.DataFrame, dict[str, Any]]:
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame(), {"subneg_growths": {}, "selectors": []}

    if _records is not None:
        # Caller pre-fetched records scoped to this client; _resolve_override_for_row
        # handles selector/subneg matching per-row — no extra filter needed here.
        records = list(_records)
    else:
        selectors = _selector_candidates_for_df(df)
        records = _fetch_override_records(
            user_id,
            client_selectors=selectors if selectors else None,
            all_users=is_admin,
        )
    maps = _build_override_maps(records)

    out = df.copy()

    if not records:
        out["_override_scope"] = "base"
        out["_monthly_pct"] = _monthly_pct_from_annual_growth(base_growth_pct)
        out["_annual_eff"] = 1.0 + float(base_growth_pct or 0.0) / 100.0
        out["_has_override"] = False
        return out, maps

    # ── VECTORIZED pre-computation (replaces slow per-row Python loops) ────────
    # month_keys: numpy datetime64[M] cast is ~156ms vs ~2400ms for dt.strftime.
    import numpy as _np
    if "fecha" in out.columns:
        _fecha_arr = out["fecha"].values
        _nat_mask = pd.isna(out["fecha"]).values
        _mk_arr = _fecha_arr.astype("datetime64[M]").astype(str)
        _mk_arr[_nat_mask] = ""
        month_keys: list[str] = _mk_arr.tolist()
    else:
        month_keys = [""] * len(out)

    # Build override selectors and find affected positions BEFORE building value lists.
    # This lets us skip building 702K-row lists for columns we only need for ~1K rows.
    _override_selectors_pre: set[str] = {
        _clean_override_text(getattr(rec, "client_selector", "") or "")
        for rec in records
    }
    _override_selectors_pre.discard("")

    # Vectorized mask over selector columns (much faster than Python list scan).
    if _override_selectors_pre:
        _aff_mask = pd.Series(False, index=out.index)
        for _sc in ("fantasia", "cliente_id", "_cliente", "Cliente"):
            if _sc in out.columns:
                _aff_mask |= out[_sc].astype(str).str.strip().isin(_override_selectors_pre)
        _affected_pos_arr = _np.where(_aff_mask.values)[0]  # positional (0-based)
    else:
        _affected_pos_arr = _np.array([], dtype=int)

    # Build per-row value arrays ONLY for affected rows (tiny subset, typically < 0.2%).
    _n_aff = len(_affected_pos_arr)
    if _n_aff > 0:
        _df_aff = out.iloc[_affected_pos_arr]
        fantasia_vals: list[str] = (
            [_clean_override_text(v) for v in _df_aff["fantasia"]]
            if "fantasia" in _df_aff.columns else [""] * _n_aff
        )
        cliente_id_vals: list[str] = (
            [_clean_override_text(v) for v in _df_aff["cliente_id"]]
            if "cliente_id" in _df_aff.columns else [""] * _n_aff
        )
        _cliente_vals: list[str] = (
            [_clean_override_text(v) for v in _df_aff["_cliente"]]
            if "_cliente" in _df_aff.columns else [""] * _n_aff
        )
        Cliente_vals: list[str] = (
            [_clean_override_text(v) for v in _df_aff["Cliente"]]
            if "Cliente" in _df_aff.columns else [""] * _n_aff
        )
        subneg_vals: list[str] = (
            [_clean_override_text(v) for v in _df_aff["subneg"]]
            if "subneg" in _df_aff.columns else [""] * _n_aff
        )
        _cod_src_aff = (
            _df_aff["codigo_serie"] if "codigo_serie" in _df_aff.columns
            else _df_aff["articulo"] if "articulo" in _df_aff.columns
            else _df_aff["descripcion"] if "descripcion" in _df_aff.columns
            else pd.Series([""] * _n_aff)
        )
        codigo_vals: list[str] = [_clean_override_text(v) for v in _cod_src_aff]
    else:
        # No affected rows — these lists won't be used but need to exist
        fantasia_vals = cliente_id_vals = _cliente_vals = Cliente_vals = subneg_vals = codigo_vals = []

    # ── Partition: resolve overrides only for affected rows ──────────────────
    # _affected_pos_arr and value lists were already built in the pre-computation
    # section above using vectorized pandas operations.
    _base_monthly = _monthly_pct_from_annual_growth(base_growth_pct)
    _n = len(out)

    logger.info(
        "[FORECAST PATCH] applying overrides to %d/%d rows (selectors=%d)",
        _n_aff, _n, len(_override_selectors_pre),
    )

    # Initialize all rows as base (vectorized — no Python loop).
    scopes: list[str] = ["base"] * _n
    monthlies: list[float] = [_base_monthly] * _n
    annual_effects: list[float] = [1.0] * _n
    flags: list[bool] = [False] * _n

    # Resolve overrides for affected rows only.
    # j = local index (0..n_aff-1); global_i = positional index in out.
    if _n_aff > 0:
        _fecha_col = out["fecha"] if "fecha" in out.columns else None
        for j, global_i in enumerate(_affected_pos_arr):
            if _fecha_col is not None and max_hist_date is not None:
                row_date = _fecha_col.iloc[global_i]
                if pd.notna(row_date) and row_date <= max_hist_date:
                    continue  # historical row stays as base

            selector_candidates: list[str] = []
            for val in (fantasia_vals[j], cliente_id_vals[j], _cliente_vals[j], Cliente_vals[j]):
                if val and val not in selector_candidates:
                    selector_candidates.append(val)

            resolved = _resolve_override_for_row(
                selector_candidates=selector_candidates,
                subneg=subneg_vals[j],
                codigo_serie=codigo_vals[j],
                forecast_month=month_keys[global_i],
                maps=maps,
                base_growth_pct=base_growth_pct,
            )
            scopes[global_i] = str(resolved["scope"])
            monthlies[global_i] = float(resolved["monthly_pct"])
            annual_effects[global_i] = 1.0 + float(resolved["annual_pct"]) / 100.0
            flags[global_i] = str(resolved["scope"]) != "base"

    out["_override_scope"] = scopes
    out["_monthly_pct"] = monthlies
    out["_annual_eff"] = annual_effects
    out["_has_override"] = flags
    return out, maps


# ---------------------------------------------------------------------------
# Aprobaciones Forecast — captura de modificaciones (registro de control)
# ---------------------------------------------------------------------------

# ── Resolver de base valorizada por alcance ──────────────────────────────────
# Estima el monto base (monto_yhat) ANUAL — sin filtrar por mes — para cada
# alcance posible de un override. Usa la MISMA fuente real que Forecast:
#   - Producción (PostgreSQL): tabla forecast_valorizado.
#   - Local (SQLite): parquet df_valorizado.
# Precalcula agregados una sola vez (cache TTL) → resolución O(1) por registro,
# robusta a casing/espacios. NO depende de que el override tenga período.

_SCOPE_RESOLVER_CACHE: dict[str, Any] = {"data": None, "ts": 0.0}
_SCOPE_RESOLVER_TTL = 600  # segundos


def _scope_norm(s: Any) -> str:
    return str(s or "").strip().lower()


def build_scope_value_resolver() -> dict:
    """Construye los agregados de monto_yhat por alcance. Best-effort."""
    res: dict[str, Any] = {
        "ok": False, "source": None, "rows": 0,
        "client": {}, "group": {}, "subneg": {}, "codigo": {}, "perfil": {},
        "client_subneg": {}, "client_codigo": {},
    }
    is_pg = engine is not None and "postgresql" in str(engine.url)
    try:
        if is_pg:
            res["source"] = "postgresql:forecast_valorizado"

            def _agg1(col: str) -> dict:
                df = _query_agg(
                    f"SELECT LOWER(TRIM({col})) AS k, SUM(monto_yhat) AS v "
                    f"FROM forecast_valorizado WHERE {col} IS NOT NULL GROUP BY 1"
                )
                if df is None or df.empty:
                    return {}
                return {str(r["k"]): float(r["v"] or 0.0) for _, r in df.iterrows() if r["k"]}

            res["client"] = _agg1("fantasia")
            res["group"] = _agg1("nombre_grupo")
            res["subneg"] = _agg1("subneg")
            res["codigo"] = _agg1("codigo_serie")
            res["perfil"] = _agg1("perfil")

            df_cs = _query_agg(
                "SELECT LOWER(TRIM(fantasia)) AS k1, LOWER(TRIM(subneg)) AS k2, "
                "SUM(monto_yhat) AS v FROM forecast_valorizado "
                "WHERE fantasia IS NOT NULL AND subneg IS NOT NULL GROUP BY 1,2"
            )
            if df_cs is not None and not df_cs.empty:
                res["client_subneg"] = {
                    (str(r["k1"]), str(r["k2"])): float(r["v"] or 0.0)
                    for _, r in df_cs.iterrows() if r["k1"] and r["k2"]
                }
            df_cc = _query_agg(
                "SELECT LOWER(TRIM(fantasia)) AS k1, LOWER(TRIM(codigo_serie)) AS k2, "
                "SUM(monto_yhat) AS v FROM forecast_valorizado "
                "WHERE fantasia IS NOT NULL AND codigo_serie IS NOT NULL GROUP BY 1,2"
            )
            if df_cc is not None and not df_cc.empty:
                res["client_codigo"] = {
                    (str(r["k1"]), str(r["k2"])): float(r["v"] or 0.0)
                    for _, r in df_cc.iterrows() if r["k1"] and r["k2"]
                }
        else:
            res["source"] = "parquet:df_valorizado"
            df = get_data().get("df_valorizado", pd.DataFrame())
            if df is not None and not df.empty and "monto_yhat" in df.columns:
                def _g(col: str) -> dict:
                    if col not in df.columns:
                        return {}
                    key = df[col].astype(str).str.strip().str.lower()
                    t = df.groupby(key)["monto_yhat"].sum()
                    return {k: float(v) for k, v in t.items() if k}

                res["client"] = _g("fantasia")
                res["group"] = _g("nombre_grupo")
                res["subneg"] = _g("subneg")
                res["codigo"] = _g("codigo_serie")
                res["perfil"] = _g("perfil")
                if {"fantasia", "subneg"}.issubset(df.columns):
                    k1 = df["fantasia"].astype(str).str.strip().str.lower()
                    k2 = df["subneg"].astype(str).str.strip().str.lower()
                    t = df.groupby([k1, k2])["monto_yhat"].sum()
                    res["client_subneg"] = {(a, b): float(v) for (a, b), v in t.items()}
                if {"fantasia", "codigo_serie"}.issubset(df.columns):
                    k1 = df["fantasia"].astype(str).str.strip().str.lower()
                    k2 = df["codigo_serie"].astype(str).str.strip().str.lower()
                    t = df.groupby([k1, k2])["monto_yhat"].sum()
                    res["client_codigo"] = {(a, b): float(v) for (a, b), v in t.items()}

        res["rows"] = len(res["client"]) + len(res["subneg"])
        res["ok"] = bool(res["client"] or res["subneg"] or res["codigo"] or res["perfil"])
    except Exception as exc:
        logger.warning("build_scope_value_resolver error: %s", exc)
    return res


def get_scope_value_resolver(force: bool = False) -> dict:
    """Resolver cacheado (TTL). force=True lo reconstruye."""
    now = time.time()
    cached = _SCOPE_RESOLVER_CACHE.get("data")
    if (not force) and cached is not None and (now - _SCOPE_RESOLVER_CACHE.get("ts", 0) < _SCOPE_RESOLVER_TTL):
        return cached
    data = build_scope_value_resolver()
    if data.get("ok"):
        _SCOPE_RESOLVER_CACHE["data"] = data
        _SCOPE_RESOLVER_CACHE["ts"] = now
    return data


def resolve_scope_base(
    resolver: dict | None,
    *,
    perfil: str | None = None,
    subneg: str | None = None,
    codigo_serie: str | None = None,
    client_selector: str | None = None,
) -> float | None:
    """Monto base ANUAL del alcance, con fallback progresivo. None si no hay match."""
    if not resolver or not resolver.get("ok"):
        return None
    cli = _scope_norm(client_selector)
    sub = _scope_norm(subneg)
    cod = _scope_norm(codigo_serie)
    per = _scope_norm(perfil)

    if cli and cod and (cli, cod) in resolver["client_codigo"]:
        return resolver["client_codigo"][(cli, cod)]
    if cli and sub and (cli, sub) in resolver["client_subneg"]:
        return resolver["client_subneg"][(cli, sub)]
    if cli:
        if cli in resolver["client"]:
            return resolver["client"][cli]
        if cli in resolver["group"]:
            return resolver["group"][cli]
    if cod and cod in resolver["codigo"]:
        return resolver["codigo"][cod]
    if sub and sub in resolver["subneg"]:
        return resolver["subneg"][sub]
    if per and per in resolver["perfil"]:
        return resolver["perfil"][per]
    return None


# ---------------------------------------------------------------------------
# Impacto curva-consistente de los overrides ACTIVOS para "Aprobaciones Forecast".
#
# Reutiliza EXACTAMENTE la lógica de la curva visible (/forecast/api/chart-data):
# por cada fila del valorizado afectada por un override,
#     delta_fila = base_val × (_annual_eff − _eff_base),  _eff_base = 1+growth/100
# (igual que _pg_get_chart_data_inner al construir Total_User_Adj − Total_Adj).
# forecast_valorizado es la PROYECCIÓN (todo futuro) → _eff_base aplica a todas
# las filas; por eso la suma reconcilia con Σ(Total_User_Adj − Total_Adj) por
# construcción. SOLO LECTURA: no modifica datos ni el guardado de overrides.
# ---------------------------------------------------------------------------
def compute_approval_curve_impacts(growth_pct: float = 25.0, *, is_admin: bool = True) -> dict:
    """Impacto de cada override ACTIVO sobre la curva, agregado por alcance.

    Devuelve dict keyed por identidad normalizada
    ``(scope, selector, subneg, codigo, month)`` →
    ``{'impact', 'ogp', 'scope', 'selector', 'subneg', 'codigo', 'month'}``,
    donde ``impact`` = contribución del override a (Proyección ajustada −
    Proyección +growth%). El router linkea cada identidad con su
    forecast_change_request vigente para asignarle el status.

    Misma precedencia/last-wins que la curva (vía _apply_override_effects_to_dataframe
    + _build_override_maps); no introduce reglas paralelas.
    """
    recs = [r for r in _fetch_override_records(None, all_users=bool(is_admin))
            if getattr(r, "is_active", False)]
    if not recs:
        return {}
    # MISMA consolidación que la curva en producción (_pg_get_chart_data_inner):
    # un único override por alcance, el más reciente (records ya vienen ordenados
    # por updated_at ASC). Evita doble conteo de duplicados activos.
    recs = _consolidate_override_records(recs, "approvals")
    selectors = _build_override_maps(recs).get("selectors", [])
    if not selectors:
        return {}

    is_pg = engine is not None and "postgresql" in str(engine.url)
    try:
        if is_pg:
            sel_f = _safe_in("fantasia", selectors)
            ov = _query_agg(
                "SELECT fecha, fantasia, cliente_id, subneg, codigo_serie, "
                "SUM(COALESCE(monto_yhat,0)) AS base_val "
                f"FROM forecast_valorizado WHERE ({sel_f}) "
                "GROUP BY fecha, fantasia, cliente_id, subneg, codigo_serie"
            )
        else:
            df = get_data().get("df_valorizado", pd.DataFrame())
            if df is None or df.empty or "monto_yhat" not in df.columns or "fantasia" not in df.columns:
                return {}
            sel_norm = {_clean_override_text(s) for s in selectors}
            fnorm = df["fantasia"].astype(str).map(_clean_override_text)
            sub_df = df[fnorm.isin(sel_norm)].copy()
            if sub_df.empty:
                return {}
            sub_df["base_val"] = sub_df["monto_yhat"]
            gcols = [c for c in ("fecha", "fantasia", "cliente_id", "subneg", "codigo_serie") if c in sub_df.columns]
            ov = sub_df.groupby(gcols, dropna=False)["base_val"].sum().reset_index()
    except Exception as exc:
        logger.warning("compute_approval_curve_impacts load failed: %s", exc)
        return {}
    if ov is None or ov.empty:
        return {}

    # fecha → datetime (necesario para _apply_override_effects_to_dataframe).
    if "fecha" in ov.columns:
        ov["fecha"] = pd.to_datetime(ov["fecha"], errors="coerce")

    # Reusar EXACTAMENTE la resolución de overrides de la curva. max_hist_date=None
    # porque el valorizado es proyección (todo futuro) → todas las filas vigentes.
    ov, _ = _apply_override_effects_to_dataframe(
        ov, user_id=None, base_growth_pct=float(growth_pct),
        max_hist_date=None, _records=recs, is_admin=bool(is_admin),
    )
    if ov is None or ov.empty or "_annual_eff" not in ov.columns:
        return {}

    eff_base = 1.0 + float(growth_pct) / 100.0
    ov = ov[ov.get("_has_override") == True].copy()
    if ov.empty:
        return {}
    ov["_delta"] = ov["base_val"].astype(float) * (ov["_annual_eff"].astype(float) - eff_base)

    # Selector = el client_selector del override que matcheó (mismo campo que la
    # change_request), NO el fantasia crudo de la fila. _apply matchea por varios
    # candidatos (fantasia/cliente_id/...); acá recuperamos cuál es un selector de
    # override activo para que el linkeo impacto↔request del router sea correcto.
    ovr_sel_set = {
        _clean_override_text(getattr(rec, "client_selector", "") or "").lower()
        for rec in recs
    }
    ovr_sel_set.discard("")

    def _matched_selector(row):
        cands = [
            _clean_override_text(row.get("fantasia", "") or "").lower(),
            _clean_override_text(row.get("cliente_id", "") or "").lower(),
        ]
        for c in cands:
            if c in ovr_sel_set:
                return c
        return cands[0]  # fallback (no deberia ocurrir si la fila matcheó un override)

    # Vínculo impacto→override: mapa clave-de-alcance → id del override. `recs` ya
    # está consolidado a UNO por alcance (el efectivo/más reciente), así que la clave
    # es 1:1. Se arma con la MISMA normalización que la clave `key` del loop de abajo
    # (sel/sub/cod lowercased, _normalize_scope/_normalize_month_key), para que el
    # match cierre exacto. Permite al router leer cr.status vía override_id en vez de
    # adivinar por valor. NO altera la consolidación ni el cálculo del impacto (_delta).
    _oid_by_key: dict[tuple, Any] = {}
    for _rec in recs:
        _rid = getattr(_rec, "id", None)
        if _rid is None:
            continue
        _sc = _normalize_scope(getattr(_rec, "override_scope", None))
        _sel = _clean_override_text(getattr(_rec, "client_selector", "") or "").lower()
        if not _sel:
            continue
        _sub = _clean_override_text(getattr(_rec, "subneg", "") or "").lower()
        _cod = _clean_override_text(getattr(_rec, "codigo_serie", "") or "").lower()
        _mon = _normalize_month_key(getattr(_rec, "forecast_month", "") or "")
        if _sc == FORECAST_SCOPE_SUBNEG:
            _oid_by_key[(_sc, _sel, _sub, "", "")] = _rid
        elif _sc == FORECAST_SCOPE_PRODUCT:
            _oid_by_key[(_sc, _sel, "", _cod, "")] = _rid
        elif _sc == FORECAST_SCOPE_CELL:
            _oid_by_key[(_sc, _sel, _sub, _cod, _mon)] = _rid

    out: dict[tuple, dict] = {}
    for _, r in ov.iterrows():
        d = float(r.get("_delta") or 0.0)
        if d == 0.0:
            continue
        scope = _normalize_scope(r.get("_override_scope"))
        sel = _matched_selector(r)
        sub = _clean_override_text(r.get("subneg", "") or "").lower()
        cod = _clean_override_text(r.get("codigo_serie", "") or "").lower()
        ae = float(r.get("_annual_eff", 1.0) or 1.0)
        if scope == FORECAST_SCOPE_SUBNEG:
            key = (scope, sel, sub, "", "")
            ident = {"subneg": sub, "codigo": "", "month": ""}
        elif scope == FORECAST_SCOPE_PRODUCT:
            key = (scope, sel, "", cod, "")
            ident = {"subneg": "", "codigo": cod, "month": ""}
        elif scope == FORECAST_SCOPE_CELL:
            f = r.get("fecha")
            month = _normalize_month_key(str(f)[:7]) if f is not None and pd.notna(f) else ""
            key = (scope, sel, sub, cod, month)
            ident = {"subneg": sub, "codigo": cod, "month": month}
        else:
            continue
        agg = out.get(key)
        if agg is None:
            out[key] = {"impact": d, "ogp": round((ae - 1.0) * 100.0, 2),
                        "scope": scope, "selector": sel,
                        "override_id": _oid_by_key.get(key), **ident}
        else:
            agg["impact"] += d
    for v in out.values():
        v["impact"] = round(v["impact"], 2)
    return out


def estimate_scope_amount(
    *,
    perfil: str | None = None,
    subneg: str | None = None,
    codigo_serie: str | None = None,
    client_selector: str | None = None,
    forecast_month: str | None = None,   # se ignora: el impacto se estima sobre el alcance ANUAL completo
) -> float | None:
    """Monto base (estimado) del alcance del override desde la fuente real
    (PG forecast_valorizado en producción / parquet en local).

    NO depende del período: si el override es anual/global (sin forecast_month),
    igual se estima sobre el alcance completo (cliente / cliente+subneg /
    subnegocio / artículo / perfil), con fallback progresivo. Devuelve ``None``
    solo si ningún identificador del alcance matchea la base valorizada.
    """
    return resolve_scope_base(
        get_scope_value_resolver(),
        perfil=perfil, subneg=subneg, codigo_serie=codigo_serie,
        client_selector=client_selector,
    )


# Sentinelas que representan "sin grupo" (misma convención que el treemap/cliente).
_SIN_GRUPO_SENTINELS = {"SIN GRUPO", "SIN GRUPO / OTROS", "", "NAN", "NONE"}


_CLIENT_DIM_CACHE: dict[str, Any] = {"data": None, "ts": 0.0}
_CLIENT_DIM_TTL = 600  # segundos


def build_client_dim_map() -> dict[str, dict]:
    """Mapa cliente(fantasía normalizada) → {grupo, perfil}.

    MISMA fuente y criterio que la tabla "Proyección más expectativa de
    crecimiento": en producción usa PG forecast_valorizado; en local el parquet
    df_valorizado. Cada cliente tiene 1 grupo y 1 perfil. Best-effort: {} si falla.
    """
    out: dict[str, dict] = {}
    is_pg = engine is not None and "postgresql" in str(engine.url)
    try:
        if is_pg:
            df = _query_agg(
                "SELECT LOWER(TRIM(fantasia)) AS f, "
                "MAX(TRIM(nombre_grupo)) AS grupo, MAX(TRIM(perfil)) AS perfil "
                "FROM forecast_valorizado WHERE fantasia IS NOT NULL GROUP BY 1"
            )
            if df is None or df.empty:
                return out
            for _, r in df.iterrows():
                f = str(r.get("f") or "").strip()
                if not f:
                    continue
                grp = str(r.get("grupo") or "").strip()
                perf = str(r.get("perfil") or "").strip()
                out[f] = {
                    "grupo": None if grp.upper() in _SIN_GRUPO_SENTINELS else grp,
                    "perfil": perf or None,
                }
        else:
            df = get_data().get("df_valorizado", pd.DataFrame())
            if df is None or df.empty or "fantasia" not in df.columns:
                return out
            cols = [c for c in ("fantasia", "nombre_grupo", "perfil") if c in df.columns]
            sub = df[cols].dropna(subset=["fantasia"]).drop_duplicates("fantasia")
            for _, r in sub.iterrows():
                f = str(r.get("fantasia") or "").strip().lower()
                if not f:
                    continue
                grp = str(r.get("nombre_grupo") or "").strip()
                perf = str(r.get("perfil") or "").strip()
                out[f] = {
                    "grupo": None if grp.upper() in _SIN_GRUPO_SENTINELS else grp,
                    "perfil": perf or None,
                }
    except Exception as exc:
        logger.warning("build_client_dim_map error: %s", exc)
        return {}
    return out


def get_client_dim_map(force: bool = False) -> dict[str, dict]:
    """Mapa cliente→{grupo,perfil} cacheado (TTL)."""
    now = time.time()
    cached = _CLIENT_DIM_CACHE.get("data")
    if (not force) and cached is not None and (now - _CLIENT_DIM_CACHE.get("ts", 0) < _CLIENT_DIM_TTL):
        return cached
    data = build_client_dim_map()
    if data:
        _CLIENT_DIM_CACHE["data"] = data
        _CLIENT_DIM_CACHE["ts"] = now
    return data


def get_client_group_map() -> dict[str, str]:
    """Mapa cliente(fantasía normalizada) → nombre_grupo (sin sentinelas)."""
    dim = get_client_dim_map()
    return {k: v["grupo"] for k, v in dim.items() if v.get("grupo")}


def get_client_perfil_map() -> dict[str, str]:
    """Mapa cliente(fantasía normalizada) → perfil."""
    dim = get_client_dim_map()
    return {k: v["perfil"] for k, v in dim.items() if v.get("perfil")}


# ── Mapa subnegocio → negocio (derivación 1:1 confirmada en el maestro) ───────
_SUBNEG_NEG_CACHE: dict[str, Any] = {"data": None, "ts": 0.0}
_SUBNEG_NEG_TTL = 600  # segundos


def build_subneg_neg_map() -> dict[str, str]:
    """Mapa subnegocio(normalizado lower/trim) → negocio.

    MISMA fuente y criterio que build_client_dim_map: en producción usa PG
    forecast_valorizado; en local el parquet df_valorizado. La relación subneg→neg
    es 1:1 en el maestro (verificado). Best-effort: {} si la fuente falla.

    Solo se incluyen subnegocios con un ÚNICO negocio asociado: si un subneg fuese
    ambiguo (>1 neg) se OMITE, y subnegocios sin match (p. ej. 'General') tampoco
    aparecen → el lookup devuelve None y el registro queda con neg NULL sin romper.
    """
    out: dict[str, str] = {}
    is_pg = engine is not None and "postgresql" in str(engine.url)
    try:
        if is_pg:
            df = _query_agg(
                "SELECT LOWER(TRIM(subneg)) AS s, MAX(TRIM(neg)) AS neg "
                "FROM forecast_valorizado "
                "WHERE subneg IS NOT NULL AND TRIM(subneg) <> '' "
                "AND neg IS NOT NULL AND TRIM(neg) <> '' "
                "GROUP BY 1 HAVING COUNT(DISTINCT TRIM(neg)) = 1"
            )
            if df is None or df.empty:
                return out
            for _, r in df.iterrows():
                s = str(r.get("s") or "").strip()
                neg = str(r.get("neg") or "").strip()
                if s and neg:
                    out[s] = neg
        else:
            df = get_data().get("df_valorizado", pd.DataFrame())
            if df is None or df.empty or "subneg" not in df.columns or "neg" not in df.columns:
                return out
            sub = df[["subneg", "neg"]].dropna()
            grp: dict[str, set] = {}
            for _, r in sub.iterrows():
                s = str(r.get("subneg") or "").strip().lower()
                neg = str(r.get("neg") or "").strip()
                if not s or not neg:
                    continue
                grp.setdefault(s, set()).add(neg)
            for s, negs in grp.items():
                if len(negs) == 1:  # solo 1:1, ambiguos omitidos
                    out[s] = next(iter(negs))
    except Exception as exc:
        logger.warning("build_subneg_neg_map error: %s", exc)
        return {}
    return out


def get_subneg_neg_map(force: bool = False) -> dict[str, str]:
    """Mapa subneg→neg cacheado (TTL). Lookup por LOWER(TRIM(subneg))."""
    now = time.time()
    cached = _SUBNEG_NEG_CACHE.get("data")
    if (not force) and cached is not None and (now - _SUBNEG_NEG_CACHE.get("ts", 0) < _SUBNEG_NEG_TTL):
        return cached
    data = build_subneg_neg_map()
    if data:
        _SUBNEG_NEG_CACHE["data"] = data
        _SUBNEG_NEG_CACHE["ts"] = now
    return data


def _classify_change_type(old_pct: float | None, new_pct: float | None) -> str:
    if old_pct is None:
        return "ajuste"  # alta de override
    try:
        d = float(new_pct or 0.0) - float(old_pct or 0.0)
    except (TypeError, ValueError):
        return "ajuste"
    if d > 0:
        return "suba_pct"
    if d < 0:
        return "baja_pct"
    return "ajuste"


def _record_change_request(
    session,
    rec,
    *,
    old_pct: float | None,
    new_pct: float | None,
    source: str,
    user_id: int,
    user_email: str | None,
) -> None:
    """Registra una solicitud de cambio PENDIENTE (módulo Aprobaciones Forecast).

    Registro de control: NO bloquea ni revierte el override. Best-effort —
    nunca propaga excepción para no romper el guardado del cotizador.
    """
    if ForecastChangeRequest is None:
        return
    try:
        old_v = None if old_pct is None else float(old_pct)
        new_v = None if new_pct is None else float(new_pct)
        # No registrar no-ops (re-guardar el mismo valor).
        if old_v is not None and new_v is not None and abs(new_v - old_v) < 1e-9:
            return

        abs_delta = None
        pct_delta = None
        if new_v is not None:
            base = old_v if old_v is not None else 0.0
            abs_delta = round(new_v - base, 4)
            pct_delta = abs_delta  # los valores ya son puntos porcentuales

        amount_base = estimate_scope_amount(
            perfil=rec.perfil,
            subneg=rec.subneg,
            codigo_serie=rec.codigo_serie,
            client_selector=rec.client_selector,
            forecast_month=rec.forecast_month,
        )
        amount_delta = None
        if amount_base is not None and abs_delta is not None:
            amount_delta = round(amount_base * (abs_delta / 100.0), 2)

        # Perfil: si el override no lo trae (típico en alcance subnegocio), se
        # deriva del cliente (cada cliente tiene un único perfil). Hace que el
        # filtro Perfil funcione también para registros nuevos.
        _perfil = (rec.perfil or "").strip() or None
        if not _perfil:
            try:
                _cli = getattr(rec, "client_display", None) or rec.client_selector
                _perfil = get_client_perfil_map().get(str(_cli or "").strip().lower())
            except Exception:
                _perfil = None

        # Flush para asegurar rec.id (trazabilidad e idempotencia del backfill).
        try:
            session.flush()
        except Exception:
            pass

        cr = ForecastChangeRequest(
            override_id=getattr(rec, "id", None),
            source=source,
            created_by_user_id=int(user_id) if user_id else None,
            created_by_username=user_email,
            change_type=_classify_change_type(old_v, new_v),
            scope_type=getattr(rec, "override_scope", None),
            client_selector=rec.client_selector,
            client_name=getattr(rec, "client_display", None) or rec.client_selector,
            perfil=_perfil,
            neg=rec.neg,
            subneg=(rec.subneg or None),
            codigo_serie=(rec.codigo_serie or None),
            descripcion_articulo=None,
            period=(rec.forecast_month or None),
            field_changed="% ajuste anual",
            old_value=old_v,
            new_value=new_v,
            absolute_delta=abs_delta,
            percentage_delta=pct_delta,
            estimated_amount_base=amount_base,
            estimated_amount_delta=amount_delta,
            status="pendiente",
        )
        session.add(cr)
    except Exception as exc:
        logger.warning("change-request record skipped: %s", exc)


def _upsert_override_record(
    session,
    existing_map: dict[tuple[str, str, str, str], Any],
    *,
    user_id: int,
    client_id: str,
    scope: str,
    base_growth_pct: float,
    override_growth_pct: float,
    effective_monthly_pct: float,
    user_email: str | None,
    subneg: str = "",
    codigo_serie: str = "",
    forecast_month: str = "",
    perfil: str | None = None,
    neg: str | None = None,
    effective_from_month: str | None = None,
    source: str = "save-client",
) -> None:
    identity = _override_identity(scope, subneg=subneg, codigo_serie=codigo_serie, forecast_month=forecast_month)
    rec = existing_map.get(identity)
    # Capturar el valor anterior ANTES de mutar (None = alta de override).
    _old_override_pct = None if rec is None else getattr(rec, "override_growth_pct", None)
    if rec is None:
        rec = ForecastUserOverride(
            user_id=int(user_id),
            source_module=FORECAST_OVERRIDE_SOURCE,
            context_key=FORECAST_OVERRIDE_CONTEXT,
            client_selector=_clean_override_text(client_id),
            client_display=_clean_override_text(client_id),
            override_scope=identity[0],
            subneg=identity[1],
            codigo_serie=identity[2],
            forecast_month=identity[3],
            created_by=user_email,
        )
        session.add(rec)
        existing_map[identity] = rec
    rec.perfil = perfil
    rec.neg = neg
    rec.base_growth_pct = float(base_growth_pct or 0.0)
    rec.override_growth_pct = float(override_growth_pct or 0.0)
    rec.effective_monthly_pct = float(effective_monthly_pct or 0.0)
    rec.client_display = _clean_override_text(client_id)
    rec.is_active = True
    rec.updated_by = user_email
    rec.updated_at = dt.datetime.utcnow()
    # Store the effective_from_month so the rule can be enforced on load
    try:
        rec.effective_from_month = effective_from_month
    except AttributeError:
        pass  # Column not yet migrated — safe to skip

    # Aprobaciones Forecast: registrar la modificación como solicitud pendiente.
    # Registro de control aditivo; nunca rompe el guardado (best-effort interno).
    _record_change_request(
        session,
        rec,
        old_pct=_old_override_pct,
        new_pct=rec.override_growth_pct,
        source=source,
        user_id=user_id,
        user_email=user_email,
    )


def _deactivate_override(
    existing_map: dict[tuple[str, str, str, str], Any],
    *,
    scope: str,
    user_email: str | None,
    subneg: str = "",
    codigo_serie: str = "",
    forecast_month: str = "",
) -> None:
    rec = existing_map.get(_override_identity(scope, subneg=subneg, codigo_serie=codigo_serie, forecast_month=forecast_month))
    if rec is None:
        return
    rec.is_active = False
    rec.updated_by = user_email
    rec.updated_at = dt.datetime.utcnow()


def deactivate_override_by_id(
    session,
    override_id: Any,
    *,
    reviewer_email: str | None = None,
) -> int | None:
    """Desactiva (is_active=False) el override vigente identificado por id, DENTRO
    de la sesión recibida (atómico con el cambio de estado del change_request).

    Compuerta de aprobación: al RECHAZAR un change_request, el override vigente de
    ese alcance se revierte → el alcance vuelve a la BASE del modelo. Opera al grano
    del override tal cual (override_scope): un subnegocio revierte todas sus celdas;
    un override de celda más fino, separado, sobrevive.

    Devuelve el user_id DUEÑO del override (para invalidar su caché) o None si no
    hay nada que desactivar: id None/inexistente, o override YA inactivo → no-op sin
    error (idempotente).
    """
    if override_id is None or ForecastUserOverride is None:
        return None
    try:
        rec = session.get(ForecastUserOverride, int(override_id))
    except Exception:
        return None
    if rec is None or not getattr(rec, "is_active", False):
        return None
    rec.is_active = False
    rec.updated_by = reviewer_email
    rec.updated_at = dt.datetime.utcnow()
    return int(rec.user_id) if getattr(rec, "user_id", None) is not None else None


def save_client_overrides(
    *,
    user_id: int,
    client_id: str,
    growth_pct: float = 0.0,
    user_email: str | None = None,
    cell_overrides: list[dict] | None = None,
    subneg_overrides: list[dict] | None = None,
) -> None:
    """Persist Forecast overrides in SQL without mutating forecast_main / forecast_valorizado.

    Temporal rule (cutoff day 20):
      - Changes saved on or before day 20 take effect from the NEXT month.
      - Changes saved after day 20 take effect from the month AFTER next.
    Cell overrides for months before effective_from_month are silently skipped.
    Subneg overrides store effective_from_month so the display/chart logic can
    respect it when applying the rate to individual months.
    """
    if SessionLocal is None or ForecastUserOverride is None:
        raise RuntimeError("Forecast override storage is not available")

    cell_overrides = cell_overrides or []
    subneg_overrides = subneg_overrides or []
    client_key = _clean_override_text(client_id)

    # Forward-fix de dimensiones: derivar perfil (1 por cliente) y neg (1:1 desde
    # subneg) UNA sola vez por guardado, para que tanto forecast_user_overrides
    # como su change-request nazcan poblados. Best-effort: None si no hay match
    # (p. ej. subneg 'General') → el registro queda con neg/perfil NULL, sin romper.
    try:
        _perfil_for_client = get_client_perfil_map().get(str(client_key or "").strip().lower()) or None
    except Exception:
        _perfil_for_client = None
    try:
        _subneg_neg_map = get_subneg_neg_map()
    except Exception:
        _subneg_neg_map = {}

    # Backend is the source of truth for the effective month cutoff
    effective_from_month = get_forecast_effective_month()

    with SessionLocal() as session:
        existing = (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.user_id == int(user_id))
            .filter(ForecastUserOverride.source_module == FORECAST_OVERRIDE_SOURCE)
            .filter(ForecastUserOverride.client_selector == client_key)
            .all()
        )
        existing_map = {
            _override_identity(
                getattr(rec, "override_scope", None),
                subneg=getattr(rec, "subneg", ""),
                codigo_serie=getattr(rec, "codigo_serie", ""),
                forecast_month=getattr(rec, "forecast_month", ""),
            ): rec
            for rec in existing
        }

        subneg_monthly_lookup: dict[str, float] = {}
        for item in subneg_overrides:
            subneg = _clean_override_text(item.get("subneg"))
            if not subneg:
                continue
            annual_growth = float(item.get("growth_pct") or 0.0)
            monthly_growth = _monthly_pct_from_annual_growth(annual_growth)
            if _is_effective_override_pct(monthly_growth, growth_pct):
                _upsert_override_record(
                    session,
                    existing_map,
                    user_id=user_id,
                    client_id=client_key,
                    scope=FORECAST_SCOPE_SUBNEG,
                    subneg=subneg,
                    perfil=_perfil_for_client,
                    neg=_subneg_neg_map.get(subneg.strip().lower()),
                    base_growth_pct=growth_pct,
                    override_growth_pct=annual_growth,
                    effective_monthly_pct=monthly_growth,
                    user_email=user_email,
                    effective_from_month=effective_from_month,
                )
                subneg_monthly_lookup[subneg] = monthly_growth
            else:
                _deactivate_override(
                    existing_map,
                    scope=FORECAST_SCOPE_SUBNEG,
                    subneg=subneg,
                    user_email=user_email,
                )

        current_client_maps = _build_override_maps(
            [rec for rec in existing_map.values() if getattr(rec, "is_active", False)]
        )
        for item in cell_overrides:
            codigo = _clean_override_text(item.get("articulo"))
            subneg = _clean_override_text(item.get("subneg"))
            month = _normalize_month_key(item.get("date"))
            if not codigo or not month:
                continue
            # Backend enforcement: silently reject overrides for months before effective_from_month
            if month < effective_from_month:
                logger.debug(
                    "[FORECAST] save_client_overrides: skipping cell override %s/%s (month %s < effective %s)",
                    codigo, month, month, effective_from_month,
                )
                continue
            monthly_pct = float(item.get("pct") or 0.0)
            reference_maps = {
                "subneg": current_client_maps.get("subneg", {}),
                "product": current_client_maps.get("product", {}),
                "cell": dict(current_client_maps.get("cell", {})),
                "subneg_growths": current_client_maps.get("subneg_growths", {}),
                "subneg_effective_months": current_client_maps.get("subneg_effective_months", {}),
                "selectors": current_client_maps.get("selectors", []),
            }
            reference_maps["cell"].pop((client_key, codigo, month), None)
            resolved = _resolve_override_for_row(
                selector_candidates=[client_key],
                subneg=subneg,
                codigo_serie=codigo,
                forecast_month=month,
                maps=reference_maps,
                base_growth_pct=growth_pct,
            )
            reference_monthly = float(subneg_monthly_lookup.get(subneg, resolved.get("monthly_pct", 0.0)))
            if abs(monthly_pct - reference_monthly) <= _OVERRIDE_PCT_TOL:
                _deactivate_override(
                    existing_map,
                    scope=FORECAST_SCOPE_CELL,
                    subneg=subneg,
                    codigo_serie=codigo,
                    forecast_month=month,
                    user_email=user_email,
                )
                continue

            _upsert_override_record(
                session,
                existing_map,
                user_id=user_id,
                client_id=client_key,
                scope=FORECAST_SCOPE_CELL,
                subneg=subneg,
                perfil=_perfil_for_client,
                neg=_subneg_neg_map.get(subneg.strip().lower()),
                codigo_serie=codigo,
                forecast_month=month,
                base_growth_pct=growth_pct,
                override_growth_pct=_annual_growth_from_monthly_pct(monthly_pct),
                effective_monthly_pct=monthly_pct,
                user_email=user_email,
                effective_from_month=effective_from_month,
            )

        session.commit()

    # Invalidate saving user's cache + admin views (admin sees all users' overrides).
    # Other regular users keep their warm cache — their overrides didn't change.
    _clear_cache_for_override_save(user_id)


def save_group_expectations(
    *,
    user_id: int,
    group_name: str,
    client_ids: list[str],
    growth_pct: float,
    base_growth_pct: float = 0.0,
    user_email: str | None = None,
) -> dict:
    """Save a uniform growth expectation override for all clients in a group.

    growth_pct      — the NEW target rate to save for every subneg of every client.
    base_growth_pct — the current GLOBAL dashboard growth rate (STATE.growthPct).
                      Used as the "baseline" for the effectiveness check so that
                      _is_effective_override_pct(monthly(growth_pct), base_growth_pct)
                      is True whenever growth_pct != base_growth_pct.
    Returns {"saved_clients": N, "skipped_clients": [...]}.
    """
    group_name = clean_group_key(group_name)
    client_ids = [_clean_override_text(c) for c in (client_ids or []) if _clean_override_text(c)]
    effective_from_month = get_forecast_effective_month()
    storage = "postgresql" if (engine is not None and "postgresql" in str(engine.url)) else "sqlite"

    if not client_ids:
        # Frontend sent empty client_ids — try to resolve from group_name in DB
        clean_group = clean_group_key(group_name).replace("'", "''")
        if clean_group:
            if storage == "sqlite":
                _df_val = get_data().get("df_valorizado", pd.DataFrame())
                if _df_val is not None and not _df_val.empty and {"fantasia", "nombre_grupo"}.issubset(_df_val.columns):
                    _mask_group = _df_val["nombre_grupo"].astype(str).map(clean_group_key) == group_name
                    df_group = pd.DataFrame({"fantasia": sorted(_df_val.loc[_mask_group, "fantasia"].dropna().astype(str).str.strip().unique())})
                else:
                    df_group = pd.DataFrame(columns=["fantasia"])
            else:
                df_group = _query_agg(
                    f"SELECT DISTINCT TRIM(fantasia) AS fantasia FROM forecast_valorizado "
                    f"WHERE TRIM(nombre_grupo) = '{clean_group}'"
                )
            if not df_group.empty:
                client_ids = df_group["fantasia"].dropna().tolist()
        if not client_ids:
            return {
                "saved_clients": 0,
                "saved_overrides": 0,
                "skipped_clients": [],
                "storage": storage,
                "effective_from_month": effective_from_month,
                "sample": [],
                "error": f"No se encontraron cuentas hijas para el grupo {group_name}",
            }

    clean_ids = [_clean_override_text(c) for c in client_ids if c]
    if not clean_ids:
        return {"saved_clients": 0, "saved_overrides": 0, "skipped_clients": [],
                "storage": storage, "effective_from_month": effective_from_month, "sample": [],
                "error": "Lista de cuentas inválida tras normalización"}

    # Query distinct (fantasia, subneg) pairs — use TRIM() for reliable matching
    if storage == "sqlite":
        _df_val = get_data().get("df_valorizado", pd.DataFrame())
        if _df_val is not None and not _df_val.empty and {"fantasia", "subneg"}.issubset(_df_val.columns):
            _df_sub_src = _df_val[
                _df_val["fantasia"].astype(str).str.strip().isin(set(clean_ids))
                & _df_val["subneg"].notna()
                & (_df_val["subneg"].astype(str).str.strip() != "")
            ].copy()
            df_sub = (
                _df_sub_src.assign(
                    fantasia=_df_sub_src["fantasia"].astype(str).str.strip(),
                    subneg=_df_sub_src["subneg"].astype(str).str.strip(),
                )[["fantasia", "subneg"]]
                .drop_duplicates()
            )
        else:
            df_sub = pd.DataFrame(columns=["fantasia", "subneg"])
    else:
        where_clause = _safe_in("TRIM(fantasia)", clean_ids)
        df_sub = _query_agg(
            f"SELECT DISTINCT TRIM(fantasia) AS fantasia, TRIM(subneg) AS subneg "
            f"FROM forecast_valorizado "
            f"WHERE {where_clause} AND subneg IS NOT NULL AND TRIM(subneg) <> ''"
        )
    logger.info(
        "[SAVE_GROUP] group=%r clients=%d df_sub rows=%d",
        group_name, len(clean_ids), len(df_sub),
    )

    client_subnegs: dict[str, list[str]] = {}
    if not df_sub.empty:
        for _, row in df_sub.iterrows():
            client = str(row.get("fantasia", "")).strip()
            sub = str(row.get("subneg", "")).strip()
            if client and sub:
                client_subnegs.setdefault(client, [])
                if sub not in client_subnegs[client]:
                    client_subnegs[client].append(sub)

    effective_from_month = get_forecast_effective_month()
    saved = 0
    saved_overrides = 0
    sample: list[dict[str, Any]] = []
    skipped: list[str] = []
    for client_id in client_ids:
        clean_id = _clean_override_text(client_id)
        subnegs = client_subnegs.get(client_id) or client_subnegs.get(clean_id) or []

        # Always clear existing overrides so group save fully replaces individual ones
        clear_client_overrides(user_id=user_id, client_id=client_id, user_email=user_email)

        if subnegs:
            # Same mechanism as individual modal save:
            # growth_pct = base/global rate (for effectiveness check)
            # subneg_overrides[i].growth_pct = target rate per subneg
            subneg_overrides = [{"subneg": s, "growth_pct": growth_pct} for s in subnegs]
            save_client_overrides(
                user_id=user_id,
                client_id=client_id,
                growth_pct=base_growth_pct,   # ← base/global rate, NOT the target
                user_email=user_email,
                subneg_overrides=subneg_overrides,
            )
            saved_overrides += len(subnegs)
            if len(sample) < 5:
                for sub in subnegs[: 5 - len(sample)]:
                    sample.append({
                        "client": clean_id,
                        "subneg": sub,
                        "growth_pct": float(growth_pct),
                        "base_growth_pct": float(base_growth_pct or 0.0),
                    })
            logger.info("[SAVE_GROUP] client=%r subnegs=%d saved with base=%.1f%% target=%.1f%%",
                        clean_id, len(subnegs), base_growth_pct, growth_pct)
        else:
            # Fallback: no subnegs in DB — save wildcard override (subneg="")
            monthly_pct = _monthly_pct_from_annual_growth(growth_pct)
            if SessionLocal is not None and ForecastUserOverride is not None:
                with SessionLocal() as session:
                    existing = (
                        session.query(ForecastUserOverride)
                        .filter(ForecastUserOverride.user_id == int(user_id))
                        .filter(ForecastUserOverride.source_module == FORECAST_OVERRIDE_SOURCE)
                        .filter(ForecastUserOverride.client_selector == clean_id)
                        .all()
                    )
                    existing_map = {
                        _override_identity(
                            getattr(r, "override_scope", None),
                            subneg=getattr(r, "subneg", ""),
                            codigo_serie=getattr(r, "codigo_serie", ""),
                            forecast_month=getattr(r, "forecast_month", ""),
                        ): r
                        for r in existing
                    }
                    _upsert_override_record(
                        session, existing_map,
                        user_id=user_id, client_id=clean_id,
                        scope=FORECAST_SCOPE_SUBNEG, subneg="",
                        base_growth_pct=float(base_growth_pct),
                        override_growth_pct=float(growth_pct),
                        effective_monthly_pct=float(monthly_pct),
                        user_email=user_email,
                        effective_from_month=effective_from_month,
                        source="save-group",
                    )
                    session.commit()
                _clear_cache_for_override_save(user_id)
            logger.info("[SAVE_GROUP] client=%r no subnegs — wildcard override saved base=%.1f%% target=%.1f%%",
                        clean_id, base_growth_pct, growth_pct)

        if not subnegs:
            saved_overrides += 1
            if len(sample) < 5:
                sample.append({
                    "client": clean_id,
                    "subneg": "",
                    "growth_pct": float(growth_pct),
                    "base_growth_pct": float(base_growth_pct or 0.0),
                })

        saved += 1

    logger.info("[SAVE_GROUP] done — saved=%d skipped=%d", saved, len(skipped))
    return {
        "saved_clients": saved,
        "saved_overrides": saved_overrides,
        "skipped_clients": skipped,
        "storage": storage,
        "effective_from_month": effective_from_month,
        "sample": sample,
    }


def clear_client_overrides(*, user_id: int, client_id: str, user_email: str | None = None) -> None:
    if SessionLocal is None or ForecastUserOverride is None:
        return
    client_key = _clean_override_text(client_id)
    with SessionLocal() as session:
        rows = (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.user_id == int(user_id))
            .filter(ForecastUserOverride.source_module == FORECAST_OVERRIDE_SOURCE)
            .filter(ForecastUserOverride.client_selector == client_key)
            .filter(ForecastUserOverride.is_active.is_(True))
            .all()
        )
        for rec in rows:
            rec.is_active = False
            rec.updated_by = user_email
            rec.updated_at = dt.datetime.utcnow()
        session.commit()
    # Invalidate saving user's cache + admin views (admin sees all users' overrides).
    _clear_cache_for_override_save(user_id)


def _get_client_overrides_snapshot(
    *,
    user_id: int | None,
    client_id: str,
    growth_pct: float | None = None,
    is_admin: bool = False,
) -> dict[tuple[str, str], float]:
    records = _fetch_override_records(user_id, client_selector=client_id, all_users=is_admin)
    maps = _build_override_maps(records)
    snapshot: dict[tuple[str, str], float] = {}
    for (selector, codigo, month), payload in maps["cell"].items():
        if selector == _clean_override_text(client_id):
            snapshot[(codigo, month)] = float(payload["monthly_pct"])
    return snapshot


def _get_client_subneg_growths(user_id: int | None, client_id: str, *, is_admin: bool = False) -> dict[str, float]:
    records = _fetch_override_records(user_id, client_selector=client_id, all_users=is_admin)
    maps = _build_override_maps(records)
    return dict(maps["subneg_growths"].get(_clean_override_text(client_id), {}))


def _derive_visible_client_growth_pct(
    negocios: list[dict[str, Any]] | None,
    saved_subneg_growths: dict[str, float] | None,
    base_growth_pct: float,
) -> float:
    """Infer the modal's client-level growth from visible saved subneg rates."""
    state = _derive_visible_client_growth_state(negocios, saved_subneg_growths, base_growth_pct)
    value = state.get("value")
    return float(base_growth_pct or 0.0) if value is None else float(value)


def _derive_visible_client_growth_state(
    negocios: list[dict[str, Any]] | None,
    saved_subneg_growths: dict[str, float] | None,
    base_growth_pct: float,
) -> dict[str, Any]:
    """Return the value/source used to rehydrate the modal's top growth input."""
    base = float(base_growth_pct or 0.0)
    saved = {
        _clean_override_text(k): float(v)
        for k, v in (saved_subneg_growths or {}).items()
        if _clean_override_text(k)
    }
    visible_subnegs: list[str] = []
    seen: set[str] = set()
    for neg in negocios or []:
        for sub in (neg or {}).get("subnegs", []) or []:
            if not ((sub or {}).get("products") or []):
                continue
            key = _clean_override_text((sub or {}).get("subneg", ""))
            if key and key not in seen:
                seen.add(key)
                visible_subnegs.append(key)

    if not visible_subnegs:
        return {"value": base, "source": "base", "mixed": False, "visible_subnegs": []}

    common: float | None = None
    found_saved = False
    missing_saved = False
    for key in visible_subnegs:
        if key not in saved:
            missing_saved = True
            continue
        value = saved[key]
        found_saved = True
        if common is None:
            common = value
        elif abs(common - value) > _OVERRIDE_PCT_TOL:
            return {
                "value": None,
                "source": "mixed",
                "mixed": True,
                "visible_subnegs": visible_subnegs,
            }
    if not found_saved:
        return {
            "value": base,
            "source": "base",
            "mixed": False,
            "visible_subnegs": visible_subnegs,
        }
    if missing_saved:
        return {
            "value": None,
            "source": "mixed",
            "mixed": True,
            "visible_subnegs": visible_subnegs,
        }
    return {
        "value": common if common is not None else base,
        "source": "uniform_subneg" if common is not None else "base",
        "mixed": False,
        "visible_subnegs": visible_subnegs,
    }


def _get_patched_df_val(user_id: int | None = None, df_source=None, *, is_admin: bool = False) -> "pd.DataFrame":
    """Return df_valorizado with SQL overrides applied (copy — never mutates cache)."""
    if df_source is not None:
        df = df_source
    else:
        data = get_data()
        df = data.get("df_valorizado", None)
    if df is None or df.empty:
        return df if df is not None else pd.DataFrame()

    # Pre-fetch override records once here to avoid _selector_candidates_for_df(702K rows)
    # inside _apply_override_effects_to_dataframe, which scans all rows to build SQL IN clause.
    _records = _fetch_override_records(user_id, all_users=is_admin)

    if not _records:
        # Fast path: no overrides — skip expensive df.copy() + row-by-row loop.
        # Just add the 4 sentinel columns and return. Values are unchanged from cache.
        logger.info("[FORECAST PATCH] skipped apply overrides reason=no_overrides uid=%s is_admin=%s", user_id, is_admin)
        out = df.copy()
        out["_override_scope"] = "base"
        out["_monthly_pct"] = _monthly_pct_from_annual_growth(0.0)
        out["_annual_eff"] = 1.0
        out["_has_override"] = False
        out.drop(columns=["_month_key"], inplace=True, errors="ignore")
        return out

    data = get_data()
    df_main = data.get("df_main", pd.DataFrame())
    max_hist_date = None
    if not df_main.empty and "tipo" in df_main.columns and "fecha" in df_main.columns:
        max_hist_date = df_main[df_main["tipo"] == "hist"]["fecha"].max()

    patched, _maps = _apply_override_effects_to_dataframe(
        df,
        user_id=user_id,
        base_growth_pct=0.0,
        max_hist_date=max_hist_date,
        _records=_records,
        is_admin=is_admin,
    )
    if not patched.empty:
        future_mask = patched["_has_override"]
        for col in ("yhat_cliente", "monto_yhat", "li_cliente", "ls_cliente", "monto_li", "monto_ls"):
            if col in patched.columns:
                patched[col] = pd.to_numeric(patched[col], errors="coerce").fillna(0).astype(float)
                patched.loc[future_mask, col] = (
                    patched.loc[future_mask, col].astype(float)
                    * patched.loc[future_mask, "_annual_eff"].astype(float)
                )
    patched.drop(columns=["_month_key"], inplace=True, errors="ignore")
    return patched


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

def _read_fact_2026_csv_robust(path: Path) -> pd.DataFrame:
    """Read facturacion_real_2026 CSV recovering rows with embedded double-quotes.

    Supports both comma-delimited (legacy format) and semicolon-delimited (new
    format: UTF-8 BOM, decimal comma, trailing delimiter) by auto-detecting the
    separator from the header row.  RFC-4180 double-quote escaping is handled
    via csv.reader to recover all rows without dropna.
    """
    # Detect delimiter from first non-empty byte sequence
    with open(path, "r", encoding="utf-8-sig", errors="replace") as _probe:
        first_line = _probe.readline()
    delimiter = ";" if ";" in first_line else ","

    rows: list[list[str]] = []
    with open(path, "r", encoding="utf-8-sig", errors="replace", newline="") as fh:
        reader = csv.reader(fh, delimiter=delimiter)
        header = next(reader)
        # Strip empty trailing columns produced by a trailing delimiter in the header
        header = [h for h in header if h.strip()]
        n_cols = len(header)
        for row in reader:
            # Trim trailing empty fields produced by a trailing delimiter per row
            while row and not row[-1].strip():
                row = row[:-1]
            if len(row) == n_cols:
                rows.append(row)
            elif len(row) == 1 and delimiter == "," and "," in row[0]:
                # Fallback for comma-delimited files with embedded RFC-4180 quotes
                reparsed = next(csv.reader([row[0]]))
                if len(reparsed) == n_cols:
                    rows.append(reparsed)
                else:
                    logger.debug("[FORECAST] fact_2026 skip unresolvable row: cols=%d", len(reparsed))
            else:
                logger.debug("[FORECAST] fact_2026 skip unresolvable row: cols=%d", len(row))
    df = pd.DataFrame(rows, columns=header)
    df.columns = [c.lower().strip() for c in df.columns]
    return df


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

    # ── Facturación real 2026 (actual billing, todos los meses cargados) ──
    # Source: facturacion_real_2026_sin_neg2.csv (todos los meses disponibles, p.ej. Ene–Jun 2026)
    # Robust parser recovers all rows including those with embedded double-quotes.
    # No canonical-series cross-filter here — the full CSV total must be preserved
    # for the "Todos" view; per-filter reduction happens at chart-query time.
    df_fact_2026 = pd.DataFrame()
    if FACT_2026_FILE.exists():
        try:
            df_fact_2026 = _read_fact_2026_csv_robust(FACT_2026_FILE)
            if "fecha" in df_fact_2026.columns:
                df_fact_2026["fecha"] = pd.to_datetime(df_fact_2026["fecha"], dayfirst=True, errors="coerce")
                df_fact_2026 = df_fact_2026[df_fact_2026["fecha"].notna()].copy()
                df_fact_2026["fecha"] = df_fact_2026["fecha"].dt.to_period("M").dt.to_timestamp()
                df_fact_2026 = df_fact_2026[df_fact_2026["fecha"] >= pd.Timestamp("2026-01-01")].copy()
            df_fact_2026["tipo"] = "val"
            if "imp_hist" in df_fact_2026.columns:
                # Handle European decimal comma (e.g. "16320,75") from semicolon-delimited export
                df_fact_2026["imp_hist"] = pd.to_numeric(
                    df_fact_2026["imp_hist"].astype(str).str.replace(",", ".", regex=False),
                    errors="coerce",
                ).fillna(0)
            # Monthly validation log
            _f26_months = df_fact_2026.groupby(df_fact_2026["fecha"].dt.to_period("M"))["imp_hist"].agg(["count", "sum"])
            for _m, _mr in _f26_months.iterrows():
                logger.info("[FORECAST] facturacion_2026 %s: %d rows | imp_hist $%.0f", _m, _mr["count"], _mr["sum"])
            logger.info("[FORECAST] facturacion_real CSV (todos los meses): %d rows loaded", len(df_fact_2026))
        except Exception as _csv_exc:
            logger.warning("facturacion_real_2026 CSV load error: %s", _csv_exc)
    if not df_fact_2026.empty:
        # Enrich with tipocli (real commercial profile) from clientes.csv.
        # forecast_fact_2026.perfil contains internal codes (e.g. "9 - 1"), NOT the
        # commercial profile used by the UI.  The correct value is clientes.tipocli,
        # joined via cliente_id → clientes.codigo.
        if CLIENTES_FILE.exists() and "cliente_id" in df_fact_2026.columns:
            try:
                df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
                df_cli.columns = [c.lower().strip() for c in df_cli.columns]
                df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
                df_fact_2026["cliente_id"] = df_fact_2026["cliente_id"].astype(str).str.strip()
                df_fact_2026 = df_fact_2026.merge(
                    df_cli[["codigo", "tipocli"]].drop_duplicates("codigo"),
                    left_on="cliente_id", right_on="codigo", how="left",
                ).drop(columns=["codigo"], errors="ignore")
                matched = df_fact_2026["tipocli"].notna().sum()
                logger.info("[FORECAST] facturacion_2026: tipocli enriched for %d/%d rows", matched, len(df_fact_2026))
                del df_cli
            except Exception as _cli_exc:
                logger.warning("facturacion_2026 tipocli enrichment error: %s", _cli_exc)
    result["df_fact_2026"] = df_fact_2026

    logger.info("[FORECAST] All data loaded. Main rows: %d, Valorizado rows: %d, ImpHist rows: %d, Fact2026 rows: %d",
                len(result.get("df_main", [])), len(result.get("df_valorizado", [])),
                len(result.get("df_imp_hist", [])), len(result.get("df_fact_2026", [])))
    return result


# ---------------------------------------------------------------------------
# Public API
# ---------------------------------------------------------------------------


def _safe_in(col: str, vals: list) -> str:
    vals = _norm_filter_list(vals)
    if not vals:
        return ""
    clean_vals = ["'" + str(v).replace("'", "''") + "'" for v in vals]
    if len(clean_vals) > 20 and engine is not None and "postgresql" in str(engine.url):
        return f"{col} = ANY(ARRAY[{','.join(clean_vals)}])"
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

def _has_overrides(user_id: int | None = None, growth_pct: float | None = None, *, is_admin: bool = False) -> bool:
    return bool(_fetch_override_records(user_id, all_users=is_admin))


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
    profiles = _norm_filter_list(profiles)
    neg = _norm_filter_list(neg)
    subneg = _norm_filter_list(subneg)
    products = _norm_filter_list(products)
    products_as_codes = _norm_filter_list(products_as_codes) if products_as_codes is not None else None
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


def _query_agg(sql: str, _conn=None) -> "pd.DataFrame":
    """Execute a read-only SQL query on PostgreSQL; returns empty DataFrame on any error.

    Uses conn.execute(text(sql)).mappings() instead of pd.read_sql(raw_string, conn)
    to avoid the SQLAlchemy 2.x + pandas incompatibility where Row objects backed by
    immutabledict raise "immutabledict is not a sequence" when pandas tries to treat
    each row as a plain sequence/tuple.

    Pass _conn to reuse an existing engine connection (avoids pool exhaustion when
    multiple queries are needed in the same request).
    """
    t0 = time.perf_counter()
    try:
        if engine is None or "sqlite" in str(engine.url):
            return pd.DataFrame()
        stmt = _sa_text(sql) if _sa_text is not None else sql
        if _conn is not None:
            result = _conn.execute(stmt)
            df = pd.DataFrame(result.mappings().all())
        else:
            with engine.connect() as conn:
                result = conn.execute(stmt)
                df = pd.DataFrame(result.mappings().all())
        if not df.empty and "fecha" in df.columns:
            df["fecha"] = pd.to_datetime(df["fecha"], errors="coerce")
        elapsed_ms = (time.perf_counter() - t0) * 1000
        approx_mem = int(df.memory_usage(deep=True).sum()) if not df.empty else 0
        logger.info(
            "[FORECAST SQL] %.1f ms rows=%d mem=%d sql=%.220s",
            elapsed_ms,
            len(df),
            approx_mem,
            " ".join(sql.split()),
        )
        return df
    except Exception as exc:
        elapsed_ms = (time.perf_counter() - t0) * 1000
        logger.error("DB agg query error after %.1f ms: %s | SQL (first 300 chars): %.300s", elapsed_ms, exc, sql)
        return pd.DataFrame()


def _query_agg_hi_mem(sql: str, work_mem: str = "16MB") -> "pd.DataFrame":
    """Como _query_agg pero con SET LOCAL work_mem en la MISMA transacción que el
    SELECT, para que el sort del ORDER BY no derrame a disco. SET LOCAL solo afecta
    esta transacción y se revierte al COMMIT — no toca la config global de Render ni
    deja la conexión del pool con un work_mem alterado. Solo PostgreSQL; ante
    cualquier problema reintenta sin work_mem (mismo resultado, más lento)."""
    if engine is None or "postgresql" not in str(engine.url) or _sa_text is None:
        return _query_agg(sql)          # SQLite/otros: camino normal, sin work_mem
    try:
        with engine.begin() as conn:    # BEGIN ... COMMIT explícito (1 transacción)
            conn.execute(_sa_text(f"SET LOCAL work_mem = '{work_mem}'"))
            return _query_agg(sql, _conn=conn)   # reusa logging + parseo de fecha
    except Exception as exc:
        logger.error("[FORECAST] hi_mem agg error: %s | SQL (first 200): %.200s", exc, sql)
        return _query_agg(sql)          # fallback defensivo


def _pg_resolve_prod_codes(products: list, _conn=None) -> "list | None":
    """Return codigo_serie list for given product names/codes, or None if no product filter.
    Queries both forecast_main and forecast_valorizado to ensure all products are resolved."""
    products = _norm_filter_list(products)
    if not products:
        return None
    cache_key = json.dumps(products, ensure_ascii=False)
    if _conn is None:
        with _PROD_CODE_CACHE_LOCK:
            entry = _PROD_CODE_CACHE.get(cache_key)
            if entry and (time.monotonic() - entry[0]) < _PROD_CODE_CACHE_TTL:
                _PROD_CODE_CACHE.move_to_end(cache_key)
                return list(entry[1])
    where = _safe_in("codigo_serie", products)
    df_main = _query_agg(f"SELECT DISTINCT codigo_serie FROM forecast_main WHERE {where}", _conn)
    codes = df_main["codigo_serie"].tolist() if not df_main.empty else []

    if _pg_valorizado_has_codigo_serie():
        df_val = _query_agg(f"SELECT DISTINCT codigo_serie FROM forecast_valorizado WHERE {where}", _conn)
        if not df_val.empty:
            codes = list(set(codes + df_val["codigo_serie"].tolist()))
    codes = sorted({str(c) for c in codes if c is not None and str(c).strip()})
    if _conn is None:
        with _PROD_CODE_CACHE_LOCK:
            _PROD_CODE_CACHE[cache_key] = (time.monotonic(), list(codes))
            _PROD_CODE_CACHE.move_to_end(cache_key)
            while len(_PROD_CODE_CACHE) > 64:
                _PROD_CODE_CACHE.popitem(last=False)
    return codes


def get_lab_product_codes(lab_name: str | None) -> list[str]:
    lab = str(lab_name or "").strip()
    if not lab:
        return []
    cache_key = lab.lower()
    with _LAB_CODE_CACHE_LOCK:
        entry = _LAB_CODE_CACHE.get(cache_key)
        if entry and (time.monotonic() - entry[0]) < _LAB_CODE_CACHE_TTL:
            _LAB_CODE_CACHE.move_to_end(cache_key)
            return list(entry[1])

    codes: list[str] = []
    if engine is not None and "postgresql" in str(engine.url):
        safe_lab = lab.replace("'", "''").replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
        df_labs = _query_agg(
            "SELECT codigo_serie, laboratorios FROM forecast_product_labs "
            f"WHERE laboratorios ILIKE '%\"{safe_lab}\"%' ESCAPE '\\'"
        )
        for _, row in df_labs.iterrows():
            try:
                labs = json.loads(row.get("laboratorios") or "[]")
            except Exception:
                labs = []
            if lab in labs:
                codes.append(str(row.get("codigo_serie") or "").strip())
    else:
        for code, labs in _build_local_product_lab_map_light().items():
            if lab in (labs or []):
                codes.append(str(code).strip())

    codes = sorted({c for c in codes if c})
    with _LAB_CODE_CACHE_LOCK:
        _LAB_CODE_CACHE[cache_key] = (time.monotonic(), list(codes))
        _LAB_CODE_CACHE.move_to_end(cache_key)
        while len(_LAB_CODE_CACHE) > 64:
            _LAB_CODE_CACHE.popitem(last=False)
    return codes


def _build_local_product_lab_map_light() -> dict[str, list]:
    product_lab_map: dict[str, list] = {}
    if not (SERIES_FILE.exists() and ARTICULOS_FILE.exists()):
        return product_lab_map
    try:
        df_s = pd.read_csv(str(SERIES_FILE), sep=",", encoding="utf-8", dtype=str)
        df_s.columns = [c.strip() for c in df_s.columns]
        df_a = pd.read_csv(str(ARTICULOS_FILE), sep=",", encoding="latin-1", dtype=str)
        df_a.columns = [c.strip() for c in df_a.columns]
        col_lab = _get_col_ci(df_a, "laboratorio_descrip")
        col_fam_a = _get_col_ci(df_a, "familia")
        col_desc_a = _get_col_ci(df_a, "descrip")
        col_serie = _get_col_ci(df_s, "codigo_serie")
        col_nivel = _get_col_ci(df_s, "nivel_agregacion")
        if not (col_lab and col_serie and col_nivel):
            return product_lab_map
        df_slim = df_s[[col_serie, col_nivel]].dropna().copy()
        df_slim[col_serie] = df_slim[col_serie].astype(str).str.strip()
        df_slim["_nivel"] = df_slim[col_nivel].astype(str).str.strip().str.upper()
        lab_pairs: list[pd.DataFrame] = []
        if col_fam_a:
            fam_src = df_slim[df_slim["_nivel"].eq("FAMILIA")][[col_serie]].drop_duplicates()
            fam_labs = df_a[[col_fam_a, col_lab]].dropna().drop_duplicates()
            fam_labs[col_fam_a] = fam_labs[col_fam_a].astype(str).str.strip()
            lab_pairs.append(
                fam_src.merge(fam_labs, left_on=col_serie, right_on=col_fam_a, how="left")[[col_serie, col_lab]]
            )
        if col_desc_a:
            art_src = df_slim[df_slim["_nivel"].isin(["ARTICULO", "ITEM"])][[col_serie]].drop_duplicates()
            desc_labs = df_a[[col_desc_a, col_lab]].dropna().drop_duplicates()
            desc_labs[col_desc_a] = desc_labs[col_desc_a].astype(str).str.strip()
            lab_pairs.append(
                art_src.merge(desc_labs, left_on=col_serie, right_on=col_desc_a, how="left")[[col_serie, col_lab]]
            )
        if lab_pairs:
            lab_df = pd.concat(lab_pairs, ignore_index=True).dropna()
            lab_df[col_lab] = lab_df[col_lab].astype(str).str.strip()
            product_lab_map = (
                lab_df[lab_df[col_lab] != ""]
                .groupby(col_serie)[col_lab]
                .apply(lambda s: sorted(set(s)))
                .to_dict()
            )
    except Exception as exc:
        logger.warning("Local product lab light map failed: %s", exc)
    return product_lab_map


def _local_get_product_list_light(profiles: list | None = None, neg: list | None = None) -> list[dict]:
    profiles = _norm_filter_list(profiles)
    neg = _norm_filter_list(neg)
    try:
        main_cols = {"neg", "codigo_serie", "perfil", "descripcion"}
        if not FORECAST_FILE.exists():
            return []
        df_main = pd.read_csv(
            str(FORECAST_FILE),
            sep=";",
            decimal=",",
            encoding="utf-8-sig",
            low_memory=False,
            usecols=lambda c: str(c).strip().lower() in main_cols,
        )
        df_main.rename(columns={"Neg": "neg", "Subneg": "subneg"}, inplace=True)
        df_main = _apply_neg_names(df_main, NEGOCIOS_FILE)
        if profiles and "perfil" in df_main.columns:
            df_main = df_main[df_main["perfil"].isin(profiles)]
        if neg and "neg" in df_main.columns:
            df_main = df_main[df_main["neg"].astype(str).isin(neg)]
        keep_cols = [c for c in ["neg", "codigo_serie", "perfil", "descripcion"] if c in df_main.columns]
        df = df_main[keep_cols].drop_duplicates()

        val_path = _VALORIZADO_PARQUET if _VALORIZADO_PARQUET.exists() else _VALORIZADO_PREPARED
        df_vol = pd.DataFrame(columns=["codigo_serie", "vol_venta"])
        if val_path.exists():
            if val_path.suffix.lower() == ".parquet":
                df_val = pd.read_parquet(str(val_path), columns=["codigo_serie", "perfil", "monto_yhat"])
            else:
                df_val = pd.read_csv(
                    str(val_path),
                    encoding="utf-8-sig",
                    low_memory=False,
                    usecols=lambda c: str(c).strip() in {"codigo_serie", "perfil", "monto_yhat"},
                )
            if profiles and "perfil" in df_val.columns:
                df_val = df_val[df_val["perfil"].isin(profiles)]
            if neg and "codigo_serie" in df.columns:
                allowed_codes = set(df["codigo_serie"].dropna().astype(str))
                df_val = df_val[df_val["codigo_serie"].astype(str).isin(allowed_codes)]
            if not df_val.empty:
                df_vol = (
                    df_val.groupby("codigo_serie", dropna=False)["monto_yhat"]
                    .sum()
                    .reset_index()
                    .rename(columns={"monto_yhat": "vol_venta"})
                )
        if not df_vol.empty:
            df = pd.merge(df, df_vol, on="codigo_serie", how="left")
        else:
            df["vol_venta"] = 0.0
        df["vol_venta"] = df["vol_venta"].fillna(0.0)
        if "descripcion" not in df.columns:
            df["descripcion"] = df["codigo_serie"]
        lab_map = _build_local_product_lab_map_light()
        ranking = (
            df.groupby(["neg", "descripcion"], dropna=False)["vol_venta"]
            .sum()
            .reset_index()
            .sort_values(["neg", "vol_venta"], ascending=[True, False])
        )
        ranking["labs"] = ranking["descripcion"].apply(lambda x: lab_map.get(str(x), []))
        return ranking.to_dict(orient="records")
    except Exception as exc:
        logger.warning("Local product-list light path failed, falling back to full load: %s", exc)
        return []


def _local_allowed_codes_for_filters(profiles=None, neg=None, subneg=None, products=None) -> set[str] | None:
    profiles = _norm_filter_list(profiles)
    neg = _norm_filter_list(neg)
    subneg = _norm_filter_list(subneg)
    products = _norm_filter_list(products)
    if not (profiles or neg or subneg or products):
        return None
    cols = {"codigo_serie", "perfil", "neg", "subneg"}
    df = pd.read_csv(
        str(FORECAST_FILE),
        sep=";",
        decimal=",",
        encoding="utf-8-sig",
        low_memory=False,
        usecols=lambda c: str(c).strip().lower() in cols,
    )
    df.rename(columns={"Neg": "neg", "Subneg": "subneg"}, inplace=True)
    df = _apply_neg_names(df, NEGOCIOS_FILE)
    rows_before = len(df)
    if profiles and "perfil" in df.columns:
        df = df[df["perfil"].isin(profiles)]
    if neg and "neg" in df.columns:
        df = df[df["neg"].astype(str).isin(neg)]
    if subneg and "subneg" in df.columns:
        df = df[df["subneg"].astype(str).isin(subneg)]
    if products:
        df = df[df["codigo_serie"].astype(str).isin(products)]
    allowed = set(df["codigo_serie"].dropna().astype(str))
    print(f"[FORECAST FILTER] profiles={profiles} neg={neg} subneg={subneg} | rows_before={rows_before} rows_after={len(df)} codes={len(allowed)}", flush=True)
    return allowed


def _local_get_chart_data_light(
    user_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    growth_pct: float = 0.0,
) -> dict:
    try:
        profiles = _norm_filter_list(profiles)
        allowed_codes = _local_allowed_codes_for_filters(profiles, neg, subneg, products)
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None

        df_hist = pd.read_csv(
            str(IMP_HIST_FILE),
            encoding="utf-8-sig",
            low_memory=False,
            usecols=lambda c: str(c).strip() in {"periodo", "codigo_serie", "perfil", "imp_hist"},
        )
        df_hist["fecha"] = pd.to_datetime(df_hist["periodo"], errors="coerce")
        df_hist["imp_hist"] = pd.to_numeric(
            df_hist["imp_hist"].astype(str).str.replace(".", "", regex=False).str.replace(",", ".", regex=False),
            errors="coerce",
        ).fillna(0)
        if start_ts is not None and start_ts.year <= 2025:
            df_hist = df_hist[df_hist["fecha"] >= start_ts]
        if end_ts is not None and end_ts.year <= 2025:
            df_hist = df_hist[df_hist["fecha"] <= end_ts]
        if profiles and "perfil" in df_hist.columns:
            df_hist = df_hist[df_hist["perfil"].isin(profiles)]
        if allowed_codes is not None:
            df_hist = df_hist[df_hist["codigo_serie"].astype(str).isin(allowed_codes)]
        hist_agg = df_hist.groupby("fecha", dropna=True)["imp_hist"].sum().reset_index(name="Total_Venta")

        val_cols = ["fecha", "codigo_serie", "perfil", "monto_yhat", "monto_li", "monto_ls", "yhat_cliente", "li_cliente", "ls_cliente"]
        df_val = pd.read_parquet(str(_VALORIZADO_PARQUET), columns=val_cols)
        if start_ts is not None:
            df_val = df_val[df_val["fecha"] >= start_ts]
        if end_ts is not None:
            df_val = df_val[df_val["fecha"] <= end_ts]
        if profiles and "perfil" in df_val.columns:
            df_val = df_val[df_val["perfil"].isin(profiles)]
        if allowed_codes is not None:
            df_val = df_val[df_val["codigo_serie"].astype(str).isin(allowed_codes)]
        val_col = "monto_yhat" if view_money else "yhat_cliente"
        li_col = "monto_li" if view_money else "li_cliente"
        ls_col = "monto_ls" if view_money else "ls_cliente"
        fcst = (
            df_val.groupby("fecha", dropna=True)
            .agg(Total_Forecast=(val_col, "sum"), Total_Li=(li_col, "sum"), Total_Ls=(ls_col, "sum"))
            .reset_index()
            .sort_values("fecha")
        )
        if fcst.empty:
            return {"history": [], "forecast": [], "val_2026": [], "kpis": {}}
        max_hist = hist_agg["fecha"].max() if not hist_agg.empty else pd.Timestamp("2000-01-01")
        fcst["Total_Adj"] = fcst["Total_Forecast"]
        if growth_pct:
            fcst.loc[fcst["fecha"] > max_hist, "Total_Adj"] *= (1.0 + float(growth_pct) / 100.0)
        fcst["Total_User_Adj"] = fcst["Total_Adj"]

        if not hist_agg.empty:
            hist_last = hist_agg.sort_values("fecha").iloc[-1]
            bridge = pd.DataFrame([{
                "fecha": hist_last["fecha"],
                "Total_Forecast": float(hist_last["Total_Venta"]),
                "Total_Li": float(hist_last["Total_Venta"]),
                "Total_Ls": float(hist_last["Total_Venta"]),
                "Total_Adj": float(hist_last["Total_Venta"]),
                "Total_User_Adj": float(hist_last["Total_Venta"]),
            }])
            fcst = pd.concat([bridge, fcst], ignore_index=True)

        fact_records: list[dict] = []
        fact_2026_sum = 0.0
        if FACT_2026_FILE.exists() and view_money:
            # Use in-memory cache: avoids re-reading the 32 MB CSV on every cold request
            df_fact = _get_fact_2026_df_cached().copy()
            if not df_fact.empty:
                # Solo meses CERRADOS: tope dinámico = primer día del mes en curso (_fact_2026_closed_month_cap).
                _cap = _fact_2026_closed_month_cap().isoformat()
                df_fact = df_fact[(df_fact["fecha"] >= "2026-01-01") & (df_fact["fecha"] < _cap)]
                if profiles and "perfil" in df_fact.columns:
                    df_fact = df_fact[df_fact["perfil"].isin(profiles)]
                if allowed_codes is not None:
                    df_fact = df_fact[df_fact["codigo_serie"].astype(str).isin(allowed_codes)]
                fact_agg = df_fact.groupby("fecha", dropna=True)["imp_hist"].sum().reset_index(name="Total_Venta")
                fact_2026_sum = float(fact_agg["Total_Venta"].sum()) if not fact_agg.empty else 0.0
                for _, row in fact_agg.sort_values("fecha").iterrows():
                    fact_records.append({"fecha": row["fecha"].strftime("%Y-%m-%d"), "Total_Venta": round(float(row["Total_Venta"]), 0)})

        def _fmt(dfs: pd.DataFrame, cols: list[str]) -> list[dict]:
            out = []
            for _, row in dfs.sort_values("fecha").iterrows():
                rec = {"fecha": row["fecha"].strftime("%Y-%m-%d") if pd.notna(row["fecha"]) else None}
                for c in cols:
                    rec[c] = round(float(row.get(c, 0) or 0), 0)
                out.append(rec)
            return out

        total_hist = float(hist_agg["Total_Venta"].sum()) if not hist_agg.empty else 0.0
        total_real_2025 = float(hist_agg.loc[hist_agg["fecha"].dt.year == 2025, "Total_Venta"].sum()) if not hist_agg.empty else 0.0
        total_fcst = float(fcst.loc[fcst["fecha"].dt.year == 2026, "Total_Forecast"].sum()) if not fcst.empty else 0.0
        total_adj = float(fcst.loc[fcst["fecha"].dt.year == 2026, "Total_Adj"].sum()) if not fcst.empty else 0.0
        inflation_mo = 2.9
        inflation_pct = ((1 + inflation_mo / 100) ** 12 - 1) * 100
        return {
            "history": _fmt(hist_agg, ["Total_Venta"]) if not hist_agg.empty else [],
            "forecast": _fmt(fcst, ["Total_Forecast", "Total_Li", "Total_Ls", "Total_Adj", "Total_User_Adj"]),
            "val_2026": fact_records,
            "has_overrides": False,
            "max_hist_date": max_hist.strftime("%Y-%m-%d") if pd.notna(max_hist) else None,
            "kpis": {
                "total_proyeccion_2026": round(total_adj, 0),
                "var_nominal_2025": round(((total_adj / total_real_2025) - 1) * 100, 2) if total_real_2025 > 0 else 0.0,
                "inflation_pct": round(inflation_pct, 1),
                "inflation_mo_pct": inflation_mo,
                "var_real_2025": round(((total_adj / (1 + inflation_pct / 100) / total_real_2025) - 1) * 100, 2) if total_real_2025 > 0 else 0.0,
                "accuracy_val": 0.0,
                "expectation_accuracy_val": 0.0,
                "fact_2026": round(fact_2026_sum, 0),
                "meta_completeness": round((fact_2026_sum / total_adj * 100), 1) if total_adj > 0 else 0.0,
                "total_historia": round(total_hist, 0),
                "total_proyeccion": round(total_fcst, 0),
                "total_proyeccion_adj": round(total_adj, 0),
                "total_real_2025": round(total_real_2025, 0),
                "n_products": int(df_val["codigo_serie"].nunique()) if not df_val.empty else 0,
            },
        }
    except Exception as exc:
        logger.warning("Local chart-data light path failed, falling back to full load: %s", exc)
        return {}


def _local_get_treemap_data_light(
    user_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    period_date: str | None = None,
) -> dict:
    _EMPTY = {"ids": [], "labels": [], "parents": [], "values": [], "colors": [], "periods": [], "canals": []}
    try:
        _t0 = time.monotonic()
        profiles = _norm_filter_list(profiles)
        allowed_codes = _local_allowed_codes_for_filters(profiles, neg, subneg, products)
        val_col = "monto_yhat" if view_money else "yhat_cliente"

        # Use _data_cache["df_valorizado"] when available — skips parquet I/O and client CSV join.
        # Fallback: read parquet directly (original cold path).
        _cached_df = _data_cache.get("df_valorizado") if _data_cache else None
        _has_cache = (
            _cached_df is not None
            and not _cached_df.empty
            and val_col in _cached_df.columns
        )

        if _has_cache:
            logger.info("[FORECAST TREEMAP] using _data_cache")
            _needed = [c for c in ("fecha", "codigo_serie", "perfil", "cliente_id", val_col, "fantasia", "nombre_grupo")
                       if c in _cached_df.columns]
            df_val = _cached_df[_needed]  # view — no copy; filters below create new objects
        else:
            logger.info("[FORECAST TREEMAP] fallback parquet path")
            cols = ["fecha", "codigo_serie", "perfil", "cliente_id", val_col]
            df_val = pd.read_parquet(str(_VALORIZADO_PARQUET), columns=cols)

        periods = [str(d)[:10] for d in sorted(df_val["fecha"].dropna().unique())]
        if period_date:
            target = pd.to_datetime(period_date).replace(day=1)
            df_val = df_val[df_val["fecha"] == target]
        else:
            if start_date:
                df_val = df_val[df_val["fecha"] >= pd.to_datetime(start_date)]
            if end_date:
                df_val = df_val[df_val["fecha"] <= pd.to_datetime(end_date)]
        if profiles and "perfil" in df_val.columns:
            df_val = df_val[df_val["perfil"].isin(profiles)]
        if allowed_codes is not None:
            df_val = df_val[df_val["codigo_serie"].astype(str).isin(allowed_codes)]
        if df_val.empty:
            return {**_EMPTY, "periods": periods}

        # Group — when fantasia/nombre_grupo are already in df_val (cache path), include them
        # in the groupby key to avoid a separate client-CSV join.
        if _has_cache and "fantasia" in df_val.columns and "nombre_grupo" in df_val.columns:
            df_tree = (
                df_val.groupby(["perfil", "cliente_id", "fantasia", "nombre_grupo"], dropna=False)[val_col]
                .sum()
                .reset_index()
                .rename(columns={val_col: "Monto"})
            )
        else:
            df_tree = (
                df_val.groupby(["perfil", "cliente_id"], dropna=False)[val_col]
                .sum()
                .reset_index()
                .rename(columns={val_col: "Monto"})
            )
            if CLIENTES_FILE.exists():
                df_cli = pd.read_csv(
                    str(CLIENTES_FILE),
                    encoding="latin-1",
                    dtype=str,
                    usecols=lambda c: str(c).strip() in {"codigo", "fantasia", "nombre_grupo"},
                )
                df_cli["codigo"] = df_cli["codigo"].astype(str).str.strip()
                df_tree["cliente_id"] = df_tree["cliente_id"].astype(str).str.strip()
                df_tree = df_tree.merge(df_cli.drop_duplicates("codigo"), left_on="cliente_id", right_on="codigo", how="left")
        if "fantasia" not in df_tree.columns:
            df_tree["fantasia"] = df_tree["cliente_id"]
        if "nombre_grupo" not in df_tree.columns:
            df_tree["nombre_grupo"] = df_tree["fantasia"]
        df_tree["Canal"] = df_tree["perfil"].astype(str).str.upper().str.strip().replace(
            {"NO_ASIGNADO": "POTENCIAL", "NO_ASIGNADA": "POTENCIAL", "SIN ASIGNAR": "POTENCIAL"}
        )
        df_tree["Cliente"] = df_tree["fantasia"].fillna(df_tree["cliente_id"]).astype(str).str.strip()
        grp = df_tree["nombre_grupo"].fillna("").astype(str).str.strip()
        sin_mask = grp.str.upper().isin({"SIN GRUPO", "SIN GRUPO / OTROS", "", "NAN", "NONE"})
        df_tree["Grupo"] = grp
        df_tree.loc[sin_mask, "Grupo"] = df_tree.loc[sin_mask, "Cliente"]
        tree_df = df_tree.groupby(["Canal", "Grupo", "Cliente"], dropna=False)["Monto"].sum().reset_index()
        tree_df = tree_df[tree_df["Monto"] > 0]
        if tree_df.empty:
            return {**_EMPTY, "periods": periods}

        ids: list[str] = []
        labels: list[str] = []
        parents: list[str] = []
        values: list[float] = []
        colors: list[str] = []

        def add(nid: str, label: str, parent: str, value: float, color: str) -> None:
            ids.append(nid); labels.append(label); parents.append(parent); values.append(float(value)); colors.append(color)

        add("total", "Total", "", float(tree_df["Monto"].sum()), "#EAF0F5")
        canals = [{"name": c, "color": _get_segment_color(c)} for c in sorted(tree_df["Canal"].unique())]
        for canal, canal_df in tree_df.groupby("Canal", sort=False):
            base = _get_segment_color(str(canal))
            cid = f"canal::{canal}"
            add(cid, str(canal), "total", float(canal_df["Monto"].sum()), _blend_with_white(base, 0.22))
            group_totals = canal_df.groupby("Grupo", as_index=False)["Monto"].sum().sort_values("Monto", ascending=False)
            keep_groups = set(group_totals.head(8)["Grupo"])
            for _, grow in group_totals.iterrows():
                group_name = str(grow["Grupo"])
                parent = cid
                if group_name not in keep_groups:
                    parent = f"{cid}::otras_grupos"
                    if parent not in ids:
                        small_total = float(group_totals[~group_totals["Grupo"].isin(keep_groups)]["Monto"].sum())
                        add(parent, "Otras", cid, small_total, _blend_with_white(base, 0.14))
                gid = f"{cid}::grupo::{group_name}"
                add(gid, group_name, parent, float(grow["Monto"]), _blend_with_white(base, 0.33))
                clients = canal_df[canal_df["Grupo"] == grow["Grupo"]].sort_values("Monto", ascending=False).head(6)
                for _, crow in clients.iterrows():
                    add(f"{gid}::cliente::{crow['Cliente']}", str(crow["Cliente"]), gid, float(crow["Monto"]), _blend_with_white(base, 0.50))
        _elapsed_ms = (time.monotonic() - _t0) * 1000
        logger.info("[FORECAST TREEMAP] built in %.0f ms (cache=%s)", _elapsed_ms, _has_cache)
        return {"ids": ids, "labels": labels, "parents": parents, "values": values, "colors": colors, "periods": periods, "canals": canals}
    except Exception as exc:
        logger.warning("Local treemap-data light path failed, falling back to full load: %s", exc)
        return {}


# Cache for schema check — checked once per process lifetime.
_val_has_codigo_serie: "bool | None" = None
_val_schema_lock = threading.Lock()


# ── Gate de summaries agregadas (consumido por endpoints en Rama 2/3) ──────
_SUMMARY_AVAIL_CACHE: dict = {}          # table -> (checked_monotonic, bool)
_SUMMARY_AVAIL_TTL = 60.0                # TTL corto: levanta tablas recién creadas por --summaries-only
_SUMMARY_AVAIL_LOCK = threading.Lock()


def _forecast_summary_available(table: str = "forecast_valorizado_summary") -> bool:
    """True si: motor PostgreSQL + FORECAST_USE_SUMMARY no apagado + la tabla existe.

    Kill-switch: FORECAST_USE_SUMMARY in {0,false,no,off} -> fuerza camino crudo
    sin redeploy. Cache con TTL para no pegarle a information_schema por request.
    Inerte en Rama 1: ningún endpoint lo consume todavía.
    """
    if engine is None or "postgresql" not in str(engine.url):
        return False
    if os.environ.get("FORECAST_USE_SUMMARY", "1").strip().lower() in {"0", "false", "no", "off"}:
        return False
    now = time.monotonic()
    with _SUMMARY_AVAIL_LOCK:
        entry = _SUMMARY_AVAIL_CACHE.get(table)
        if entry and (now - entry[0]) < _SUMMARY_AVAIL_TTL:
            return entry[1]
    df = _query_agg(
        f"SELECT 1 FROM information_schema.tables WHERE table_name = '{table}' LIMIT 1"
    )
    exists = not df.empty
    with _SUMMARY_AVAIL_LOCK:
        _SUMMARY_AVAIL_CACHE[table] = (now, exists)
    if not exists:
        logger.info("[FORECAST summary] tabla %s ausente -> endpoints caen a crudo.", table)
    return exists


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
    _pl_total_t0 = time.perf_counter()
    _pl_mapping_ms = 0.0
    _pl_volume_ms = 0.0
    _pl_labs_ms = 0.0
    _pl_build_ms = 0.0
    neg_where = _build_filter_sql(profiles=profiles, neg=neg)
    # Rama summary (PR-3): la tabla 2 ya trae (neg, codigo_serie, vol_venta), así que UNA
    # lectura reemplaza el mapping UNION+DISTINCT (query 1) y la de volumen (query 2).
    # Gate con NOMBRE EXPLÍCITO: product-list usa forecast_product_summary, no el default.
    # Sin work_mem (product-list no derrama a disco).
    if _forecast_summary_available("forecast_product_summary"):
        _pl_t0 = time.perf_counter()
        df = _query_agg(
            f"SELECT COALESCE(neg, 'Varios') AS neg, codigo_serie, "
            f"SUM(COALESCE(vol_venta, 0)) AS vol_venta "
            f"FROM forecast_product_summary WHERE {neg_where} "
            f"GROUP BY COALESCE(neg, 'Varios'), codigo_serie"
        )
        _pl_mapping_ms = (time.perf_counter() - _pl_t0) * 1000
        if df.empty:
            return []
        df["neg"] = df["neg"].fillna("Varios").replace({"nan": "Varios", "None": "Varios", "none": "Varios"})
    else:
        # Query 1: channel mapping — only distinct text columns, from both main and valorizado
        _pl_t0 = time.perf_counter()
        df_neg = _query_agg(
            f"SELECT DISTINCT COALESCE(neg, 'Varios') AS neg, codigo_serie FROM ("
            f"  SELECT neg, codigo_serie, perfil FROM forecast_main"
            f"  UNION"
            f"  SELECT neg, codigo_serie, perfil FROM forecast_valorizado"
            f") AS combined WHERE {neg_where}"
        )
        _pl_mapping_ms = (time.perf_counter() - _pl_t0) * 1000
        if df_neg.empty:
            return []
        df_neg["neg"] = df_neg["neg"].fillna("Varios").replace({"nan": "Varios", "None": "Varios", "none": "Varios"})

        # Query 2: monetary volume from forecast_valorizado (monto_yhat is FLOAT)
        vol_where = _build_filter_sql(profiles=profiles, neg=neg)
        _pl_t0 = time.perf_counter()
        df_vol = _query_agg(
            f"SELECT codigo_serie, SUM(COALESCE(monto_yhat, 0)) AS vol_venta "
            f"FROM forecast_valorizado WHERE {vol_where} GROUP BY codigo_serie"
        )
        # vol_venta alias is already lowercase — PostgreSQL preserves it as-is ✓

        _pl_volume_ms = (time.perf_counter() - _pl_t0) * 1000
        # Merge: left join so all products appear even if not in forecast_valorizado
        if df_vol.empty:
            df = df_neg.copy()
            df["vol_venta"] = 0.0
        else:
            df = pd.merge(df_neg, df_vol, on="codigo_serie", how="left")
            df["vol_venta"] = df["vol_venta"].fillna(0.0)

    _pl_t0 = time.perf_counter()
    labs_df = _query_agg("SELECT codigo_serie, laboratorios FROM forecast_product_labs")
    _pl_labs_ms = (time.perf_counter() - _pl_t0) * 1000
    lab_map: dict = {}
    _pl_t0 = time.perf_counter()
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
    result = ranking.to_dict(orient="records")
    _pl_build_ms = (time.perf_counter() - _pl_t0) * 1000
    _forecast_diag(
        "[PRODUCT_LIST] phases mapping_ms=%.1f volume_ms=%.1f labs_ms=%.1f build_json_ms=%.1f "
        "total_ms=%.1f rows=%s payload_bytes=%s profiles=%s neg=%s",
        _pl_mapping_ms,
        _pl_volume_ms,
        _pl_labs_ms,
        _pl_build_ms,
        (time.perf_counter() - _pl_total_t0) * 1000,
        len(result),
        _forecast_payload_bytes(result),
        len(_norm_filter_list(profiles)),
        len(_norm_filter_list(neg)),
    )
    return result


def _pg_get_chart_data(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct,
    is_admin=False,
) -> dict:
    """Memory-safe PostgreSQL chart data: all heavy aggregation runs in SQL."""
    _EMPTY = {"history": [], "forecast": [], "val_2026": [], "kpis": {}}
    _step = "init"
    try:
        return _pg_get_chart_data_inner(
            user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct,
            is_admin=is_admin,
        )
    except Exception as exc:
        import traceback
        _tb_str = traceback.format_exc()
        logger.error("[FORECAST] _pg_get_chart_data FAILED at step=%s: %s\n%s", _step, exc, _tb_str)
        return _EMPTY


def _pg_get_chart_data_inner(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct,
    is_admin=False,
) -> dict:
    """Inner implementation — called by _pg_get_chart_data which catches all exceptions."""
    _EMPTY = {"history": [], "forecast": [], "val_2026": [], "kpis": {}}
    # Single override fetch — reused throughout this function to avoid multiple DB roundtrips
    _ch_total_t0 = time.perf_counter()
    _ch_override_fetch_ms = 0.0
    _ch_meta_ms = 0.0
    _ch_hist_ms = 0.0
    _ch_forecast_ms = 0.0
    _ch_override_ms = 0.0
    _ch_fact_ms = 0.0
    _ch_build_ms = 0.0
    global_overrides = bool(is_admin)
    _ch_t0 = time.perf_counter()
    _ovr_records = _fetch_override_records(user_id, all_users=global_overrides)
    _ch_override_fetch_ms = (time.perf_counter() - _ch_t0) * 1000
    _ovr_original_count = len(_ovr_records)
    if _ovr_records:
        _ovr_records = _consolidate_override_records(_ovr_records, "chart-data")
    _ovr_active = bool(_ovr_records)
    _ovr_user_ids = _override_record_user_ids(_ovr_records)

    logger.info(
        "[FORECAST INNER] chart user_id=%s override_scope=%s override_count=%s override_user_ids=%s start=%s end=%s growth_pct=%s profiles=%s neg=%s subneg=%s",
        user_id,
        "global" if global_overrides else "user",
        _ovr_original_count,
        _ovr_user_ids,
        start_date,
        end_date,
        growth_pct,
        profiles,
        neg,
        subneg,
    )
    # Resolve product descriptions → codigo_serie (avoids cross-table joins in Python)
    prod_codes = _pg_resolve_prod_codes(products)
    # val_prod: None when forecast_valorizado lacks the column (graceful degradation)
    val_prod   = _val_prod_filter(prod_codes)

    logger.debug("[FORECAST INNER] prod_codes_count=%s", len(prod_codes) if prod_codes is not None else "None")
    # WHERE for forecast_main (has neg/subneg, no descripcion — uses codigo_serie only)
    main_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=prod_codes,
        products=None if prod_codes is not None else products,
    )
    logger.debug("[FORECAST INNER] main_where_len=%s", len(main_where))
    # Lightweight metadata: only n_products varies by filter; max_hist_date is global
    if _pg_valorizado_has_codigo_serie():
        val_where_meta = _build_filter_sql(
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products_as_codes=val_prod,
            products=None if val_prod is not None else products,
        )
        _ch_t0 = time.perf_counter()
        df_meta = _query_agg(
            f"SELECT COUNT(DISTINCT codigo_serie) AS n_products "
            f"FROM forecast_valorizado WHERE {val_where_meta}"
        )
        _ch_meta_ms = (time.perf_counter() - _ch_t0) * 1000
    else:
        _ch_t0 = time.perf_counter()
        df_meta = _query_agg(
            f"SELECT COUNT(DISTINCT codigo_serie) AS n_products "
            f"FROM forecast_main WHERE {main_where}"
        )
        _ch_meta_ms = (time.perf_counter() - _ch_t0) * 1000
    if df_meta.empty:
        logger.debug("[FORECAST INNER] df_meta empty — returning _EMPTY")
        return _EMPTY
    n_products = int(df_meta["n_products"].iloc[0] or 0)
    _cached_mhd = _get_max_hist_date_cached()
    max_hist = _cached_mhd if _cached_mhd is not None else pd.Timestamp("2000-01-01")
    logger.debug("[FORECAST INNER] meta n_products=%s max_hist=%s", n_products, max_hist)

    # WHERE for forecast_imp_hist: only has perfil + codigo_serie + fecha (no neg/subneg)
    # hist/fact tables always have codigo_serie — use prod_codes (not val_prod) here.
    hist_start = start_date
    if start_date:
        try:
            if pd.to_datetime(start_date).year >= 2026:
                hist_start = None
        except Exception:
            pass
    hist_end = end_date
    if end_date:
        try:
            if pd.to_datetime(end_date).year >= 2026:
                hist_end = None
        except Exception:
            pass

    hist_where = _build_filter_sql(
        start_date=hist_start, end_date=hist_end,
        profiles=profiles,
        products_as_codes=prod_codes,
        skip_neg=True,  # forecast_imp_hist has no neg/subneg cols — use subquery below
    )
    # When neg/subneg filter active, restrict hist series via forecast_main subquery
    _hist_neg_subquery = ""
    if neg or subneg:
        _neg_series_where = _build_filter_sql(neg=neg, subneg=subneg)
        _hist_neg_subquery = (
            f" AND codigo_serie IN "
            f"(SELECT DISTINCT codigo_serie FROM forecast_main WHERE {_neg_series_where})"
        )
    # WHERE for forecast_valorizado: use val_prod (None when column absent → no crash)
    val_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
    )
    # fact_2026 uses two independent filters so each dimension resolves correctly:
    # 1. Profile → tipocli column directly from forecast_fact_2026 (enriched at load time
    #    from clientes.csv). Using JOIN via forecast_valorizado.perfil was incorrect: it
    #    excluded clients present in forecast_fact_2026 (tipocli=X) but absent in
    #    forecast_valorizado, causing $663M undercount for DRO Apr-2026 (73 clients lost).
    # 2. Neg/subneg/products → series subquery via forecast_main.
    _has_series_filter = bool(neg or subneg or (prod_codes is not None) or products)
    fact_series_only_where = _build_filter_sql(
        neg=neg, subneg=subneg,
        products_as_codes=prod_codes,
        products=None if prod_codes is not None else products,
    ) if _has_series_filter else None

    logger.debug("[FORECAST INNER] query_hist view_money=%s", view_money)
    # History: from imp_hist (real billing, already in money) or forecast_main y×precio
    # NOTE: PostgreSQL returns column aliases in lowercase regardless of AS casing.
    # All queries use lowercase aliases; rename to Title-Case after each query so the
    # rest of the function keeps its existing column references unchanged.
    if view_money:
        # CANONICAL SERIES FILTER: restrict imp_hist to the 3039 series that exist in
        # forecast_valorizado (same inner-join the original app.py applied at load time).
        # Without this filter forecast_imp_hist returns 44 861 rows / $109.1B (all series).
        # With it: 38 758 rows / $98.0B — the correct real-2025 baseline.
        # Use cached canonical series list to avoid correlated IN(SELECT DISTINCT...) subquery
        # which is expensive on 702K-row forecast_valorizado.
        _canonical = _get_canonical_series_cached()
        if _canonical:
            _canon_filter = _safe_in("codigo_serie", _canonical)
            _canon_clause = f" AND {_canon_filter}" if _canon_filter else ""
        else:
            # Fallback to original subquery when cache miss / empty result
            _canon_clause = " AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado)"
        _ch_t0 = time.perf_counter()
        df_hist = _query_agg(
            f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "
            f"FROM forecast_imp_hist "
            f"WHERE {hist_where}{_canon_clause}{_hist_neg_subquery} "
            f"GROUP BY fecha ORDER BY fecha"
        )
        _ch_hist_ms = (time.perf_counter() - _ch_t0) * 1000
        # forecast_main fallback intentionally omitted: y/yhat are TEXT in production,
        # SUM(COALESCE(y,0)) raises a type error caught by _query_agg → empty anyway.
    else:
        # Units path — forecast_main.y is TEXT in production so this will be empty;
        # kept for local/SQLite mode where y is numeric.
        _ch_t0 = time.perf_counter()
        df_hist = _query_agg(
            f"SELECT fecha, SUM(COALESCE(y::numeric, 0)) AS total_venta "
            f"FROM forecast_main WHERE {main_where} AND tipo = 'hist' GROUP BY fecha ORDER BY fecha"
        )
        _ch_hist_ms = (time.perf_counter() - _ch_t0) * 1000
    # Normalise alias to Title-Case so downstream code is unchanged
    if not df_hist.empty and "total_venta" in df_hist.columns:
        df_hist.rename(columns={"total_venta": "Total_Venta"}, inplace=True)

    logger.debug("[FORECAST INNER] hist_rows=%s — querying forecast", len(df_hist))
    # Forecast: from valorizado (monto_yhat=money, yhat_cliente=units; monto_li/monto_ls=band)
    val_col    = "monto_yhat"    if view_money else "yhat_cliente"
    val_col_li = "monto_li"      if view_money else "li_cliente"
    val_col_ls = "monto_ls"      if view_money else "ls_cliente"
    _ch_t0 = time.perf_counter()
    df_fcst = _query_agg(
        f"SELECT fecha, "
        f"SUM(COALESCE({val_col}, 0)) AS total_forecast, "
        f"SUM(COALESCE({val_col_li}, 0)) AS total_li, "
        f"SUM(COALESCE({val_col_ls}, 0)) AS total_ls "
        f"FROM forecast_valorizado WHERE {val_where} GROUP BY fecha ORDER BY fecha"
    )
    _ch_forecast_ms = (time.perf_counter() - _ch_t0) * 1000
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
        logger.debug("[FORECAST INNER] df_fcst_rows=%s", len(df_fcst))
        df_fcst["Total_User_Adj"] = df_fcst["Total_Adj"].copy()

    # ── DIAGNOSTIC: log override records at debug level only ─────────────
    if _ovr_active and logger.isEnabledFor(logging.DEBUG):
        _ovr_record_summary = [
            {
                "sel": getattr(_r, "client_selector", "?"),
                "scope": getattr(_r, "override_scope", "?"),
                "subneg": getattr(_r, "subneg", "?"),
                "annual_pct": getattr(_r, "override_growth_pct", "?"),
                "efm": getattr(_r, "effective_from_month", "?"),
                "active": getattr(_r, "is_active", "?"),
            }
            for _r in _ovr_records[:5]
        ]
        logger.debug(
            "[FORECAST INNER] override application fcst_rows=%s hist_rows=%s ovr_records=%s sample=%s",
            len(df_fcst), len(df_hist), len(_ovr_records), _ovr_record_summary,
        )
    if _ovr_active:
        _t_ovr = time.perf_counter()
        _used_delta = False
        _override_rows = pd.DataFrame()
        try:
            _ovr_maps_pre = _build_override_maps(_ovr_records)
            _ovr_selectors = _ovr_maps_pre.get("selectors", [])
            logger.info(
                "[FORECAST INNER] delta_path selectors=%s ovr_records=%s",
                len(_ovr_selectors), len(_ovr_records),
            )
            if _ovr_selectors:
                _sel_f = f"({_safe_in('fantasia', _ovr_selectors)})"
                _override_rows = _query_agg(
                    f"SELECT fecha, fantasia, cliente_id, subneg, codigo_serie, "
                    f"SUM(COALESCE({val_col}, 0)) AS base_val "
                    f"FROM forecast_valorizado WHERE {val_where} AND {_sel_f} "
                    f"GROUP BY fecha, fantasia, cliente_id, subneg, codigo_serie ORDER BY fecha"
                )
                logger.info(
                    "[FORECAST INNER] delta_rows=%s selectors=%s",
                    len(_override_rows), len(_ovr_selectors),
                )
            _used_delta = True
        except Exception as _delta_exc:
            logger.warning(
                "[FORECAST INNER] delta_path FAILED (%s) -- fallback to full table load",
                _delta_exc,
            )
        if not _used_delta:
            _override_rows = _query_agg(
                f"SELECT fecha, fantasia, cliente_id, subneg, codigo_serie, "
                f"SUM(COALESCE({val_col}, 0)) AS base_val "
                f"FROM forecast_valorizado WHERE {val_where} "
                f"GROUP BY fecha, fantasia, cliente_id, subneg, codigo_serie ORDER BY fecha"
            )
            logger.warning("[FORECAST INNER] FALLBACK full_rows=%s", len(_override_rows))
        if not _override_rows.empty:
            _override_rows, _ovr_maps = _apply_override_effects_to_dataframe(
                _override_rows,
                user_id=user_id,
                base_growth_pct=growth_pct,
                max_hist_date=max_hist,
                _records=_ovr_records,
                is_admin=global_overrides,
            )
            _n_overridden = int(_override_rows["_has_override"].sum()) if "_has_override" in _override_rows.columns else -1
            _n_base = len(_override_rows) - _n_overridden if _n_overridden >= 0 else -1
            if "_has_override" in _override_rows.columns:
                _ovr_detail = _override_rows[_override_rows["_has_override"] == True]
                if not _ovr_detail.empty:
                    logger.debug(
                        "[FORECAST INNER] overridden_rows_sample=%s",
                        _ovr_detail[["fantasia", "subneg", "_override_scope", "_annual_eff"]].drop_duplicates().head(5).to_dict("records"),
                    )
                else:
                    _rec_selectors = {_clean_override_text(getattr(r, "client_selector", "")) for r in _ovr_records}
                    _df_selectors = set(_override_rows["fantasia"].dropna().apply(_clean_override_text).unique()[:20].tolist()) if "fantasia" in _override_rows.columns else set()
                    _matching = _rec_selectors & _df_selectors
                    logger.info(
                        "[FORECAST USER ADJ] NO OVERRIDES APPLIED rec_selectors=%s df_selectors_sample=%s matching=%s",
                        _rec_selectors, list(_df_selectors)[:5], _matching,
                    )
            if _used_delta:
                # DELTA: Total_User_Adj = Total_Adj + sum(base_val * (annual_eff - base_eff))
                # base_eff = 1.0 for hist dates, 1+growth_pct/100 for future.
                # Rows without active override get annual_eff == base_eff -> delta 0.
                _eff_base_scalar = 1.0 + growth_pct / 100.0
                _override_rows = _override_rows.copy()
                _override_rows["_eff_base"] = 1.0
                if growth_pct != 0 and max_hist is not None:
                    _fut = _override_rows["fecha"] > max_hist
                    _override_rows.loc[_fut, "_eff_base"] = _eff_base_scalar
                _override_rows["_delta"] = (
                    _override_rows["base_val"]
                    * (_override_rows["_annual_eff"] - _override_rows["_eff_base"])
                )
                _delta_by_fecha = (
                    _override_rows.groupby("fecha")["_delta"].sum()
                    .reset_index()
                    .rename(columns={"_delta": "_delta_sum"})
                )
                df_fcst = df_fcst.merge(_delta_by_fecha, on="fecha", how="left")
                df_fcst["Total_User_Adj"] = (
                    df_fcst["Total_Adj"] + df_fcst["_delta_sum"].fillna(0)
                )
                df_fcst.drop(columns=["_delta_sum"], inplace=True)
            else:
                # FALLBACK: original merge semantics
                _override_rows["_ua_sql"] = _override_rows["base_val"] * _override_rows["_annual_eff"]
                _ua_sql = (
                    _override_rows.groupby("fecha")["_ua_sql"].sum()
                    .reset_index()
                    .rename(columns={"_ua_sql": "Total_User_Adj_SQL"})
                )
                df_fcst = df_fcst.merge(_ua_sql, on="fecha", how="left")
                df_fcst["Total_User_Adj"] = df_fcst["Total_User_Adj_SQL"].fillna(
                    df_fcst["Total_User_Adj"]
                )
                df_fcst.drop(columns=["Total_User_Adj_SQL"], inplace=True)
            _future_mask = df_fcst["fecha"] > max_hist
            _adj_diff = (df_fcst.loc[_future_mask, "Total_User_Adj"] - df_fcst.loc[_future_mask, "Total_Adj"]).abs().sum()
            _t_ovr_ms = (time.perf_counter() - _t_ovr) * 1000
            _ch_override_ms = _t_ovr_ms
            logger.info(
                "[FORECAST INNER] ovr_complete: overridden=%s adj_diff=%.0f elapsed_ms=%.1f delta=%s",
                _n_overridden, _adj_diff, _t_ovr_ms, _used_delta,
            )
            if _adj_diff == 0:
                logger.info(
                    "[FORECAST USER ADJ] zero diff -- ovr_records=%s overridden=%s",
                    len(_ovr_records), _n_overridden,
                )
        else:
            _ch_override_ms = (time.perf_counter() - _t_ovr) * 1000

    # ── Inject manual client entries into PG forecast totals ─────────────
    _manual_df_pg_chart = _get_manual_entries_df(user_id, start_date, end_date, neg, subneg, is_admin=is_admin, profiles_filter=profiles)
    if not _manual_df_pg_chart.empty and not df_fcst.empty:
        _val_col_m_pg = "monto_yhat" if view_money else "yhat_cliente"
        _manual_monthly_pg = (
            _manual_df_pg_chart.groupby("fecha")[_val_col_m_pg]
            .sum()
            .reset_index()
            .rename(columns={_val_col_m_pg: "_manual_amt"})
        )
        print(f"[MANUAL_DASHBOARD] PG chart manual monthly total={_manual_monthly_pg['_manual_amt'].sum():.0f}", flush=True)
        new_pg_rows = []
        for _, mr in _manual_monthly_pg.iterrows():
            mask_m = df_fcst["fecha"] == mr["fecha"]
            if mask_m.any():
                for col in ("Total_Forecast", "Total_User_Adj", "Total_Adj", "Total_Li", "Total_Ls"):
                    if col in df_fcst.columns:
                        df_fcst.loc[mask_m, col] = df_fcst.loc[mask_m, col] + mr["_manual_amt"]
            else:
                new_pg_rows.append({
                    "fecha": mr["fecha"],
                    "Total_Forecast": mr["_manual_amt"],
                    "Total_Li": mr["_manual_amt"],
                    "Total_Ls": mr["_manual_amt"],
                    "Total_Adj": mr["_manual_amt"],
                    "Total_User_Adj": mr["_manual_amt"],
                })
        if new_pg_rows:
            df_fcst = pd.concat([df_fcst, pd.DataFrame(new_pg_rows)], ignore_index=True)
        df_fcst = df_fcst.sort_values("fecha").reset_index(drop=True)
    elif not _manual_df_pg_chart.empty and df_fcst.empty:
        _val_col_m_pg = "monto_yhat" if view_money else "yhat_cliente"
        _manual_monthly_pg = (
            _manual_df_pg_chart.groupby("fecha")[_val_col_m_pg]
            .sum()
            .reset_index()
            .rename(columns={_val_col_m_pg: "_manual_amt"})
        )
        df_fcst = pd.DataFrame([{
            "fecha": row["fecha"],
            "Total_Forecast": row["_manual_amt"],
            "Total_Li": row["_manual_amt"],
            "Total_Ls": row["_manual_amt"],
            "Total_Adj": row["_manual_amt"],
            "Total_User_Adj": row["_manual_amt"],
        } for _, row in _manual_monthly_pg.iterrows()])

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

    logger.debug("[FORECAST INNER] querying fact2026")
    # Facturación real 2026 — fuente única: forecast_fact_2026.
    # Sin filtro de series canónicas: en vista Todos se devuelve el total real completo.
    # El filtro por Perfil/Neg/Subneg/Producto se aplica solo cuando el usuario los activa.
    # La query NO capa el mes superior (trae desde 2026-01-01 en adelante) porque df_fact_raw
    # alimenta también el cálculo de accuracy más abajo (que tiene su propia lógica de "mes
    # abierto"). El recorte a meses CERRADOS (tope dinámico) se aplica al graficar y al KPI.
    _fact_parts = ["fecha >= '2026-01-01'"]
    if profiles:
        # Filter directly by tipocli (commercial profile enriched at load time from clientes.csv).
        # Do NOT use JOIN with forecast_valorizado: that table misses clients that exist in
        # forecast_fact_2026, causing an undercount.  tipocli is the authoritative profile column.
        _fact_tipocli = _safe_in("tipocli", profiles)
        if _fact_tipocli:
            _fact_parts.append(_fact_tipocli)
        print(f"[FORECAST FACT2026] profiles={profiles} → tipocli filter: {_fact_tipocli or 'NONE'}", flush=True)
    if fact_series_only_where:
        _fact_parts.append(
            f"codigo_serie IN ("
            f"  SELECT DISTINCT fm.codigo_serie FROM forecast_main fm WHERE {fact_series_only_where}"
            f")"
        )
    _ch_t0 = time.perf_counter()
    df_fact_raw = _query_agg(
        f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "
        f"FROM forecast_fact_2026 WHERE {' AND '.join(_fact_parts)} "
        f"GROUP BY fecha ORDER BY fecha"
    )
    _ch_fact_ms = (time.perf_counter() - _ch_t0) * 1000
    # Normalise alias
    if not df_fact_raw.empty and "total_venta" in df_fact_raw.columns:
        df_fact_raw.rename(columns={"total_venta": "Total_Venta"}, inplace=True)
    val_2026_records: list = []
    fact_2026_sum = 0.0
    if not df_fact_raw.empty:
        # Solo meses CERRADOS (tope dinámico = primer día del mes en curso; _fact_2026_closed_month_cap).
        # df_fact_raw queda completo (alimenta accuracy abajo, con su propia lógica de mes abierto);
        # acá se capa lo que se grafica (val_2026) y el KPI fact_2026.
        _fact_cap_ts = pd.Timestamp(_fact_2026_closed_month_cap())
        df_fact_closed = df_fact_raw[df_fact_raw["fecha"] < _fact_cap_ts]
        fact_2026_sum = float(df_fact_closed["Total_Venta"].sum())
        df_v2026_chart = df_fact_closed.copy()
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

    logger.debug("[FORECAST INNER] fact2026_rows=%s — computing kpis", len(df_fact_raw))
    _ch_build_t0 = time.perf_counter()
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

    forecast_payload = _fmt(df_fcst, ["Total_Forecast", "Total_Li", "Total_Ls", "Total_Adj", "Total_User_Adj"]) if not df_fcst.empty else []
    _adjustment_summary = _forecast_adjustment_summary(forecast_payload)

    logger.info(
        "[FORECAST] chart-data: user_id=%s override_scope=%s override_count=%s adjusted=%s adjusted_diff_sum=%.2f total_adj=%.0f total_hist=%.0f n_products=%s",
        user_id,
        "global" if global_overrides else "user",
        len(_ovr_records),
        _adjustment_summary["has_adjusted_series"],
        _adjustment_summary["adjusted_diff_sum"],
        total_adj,
        total_hist,
        n_products,
    )
    result = {
        "history":  _fmt(df_hist, ["Total_Venta"])                                     if not df_hist.empty else [],
        "forecast": forecast_payload,
        "val_2026": val_2026_records,
        "has_overrides": _ovr_active,
        "override_debug": {
            "scope": "global" if global_overrides else "user",
            "all_users": global_overrides,
            "requested_user_id": int(user_id) if user_id is not None else None,
            "override_count": len(_ovr_records),
            "override_user_ids": _ovr_user_ids,
            **_adjustment_summary,
        },
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
    _ch_build_ms = (time.perf_counter() - _ch_build_t0) * 1000
    _forecast_diag(
        "[CHART_DATA] phases override_fetch_ms=%.1f meta_ms=%.1f hist_ms=%.1f forecast_ms=%.1f "
        "override_ms=%.1f fact_ms=%.1f build_json_ms=%.1f total_ms=%.1f rows=%s payload_bytes=%s "
        "override_count=%s effective_override_count=%s",
        _ch_override_fetch_ms,
        _ch_meta_ms,
        _ch_hist_ms,
        _ch_forecast_ms,
        _ch_override_ms,
        _ch_fact_ms,
        _ch_build_ms,
        (time.perf_counter() - _ch_total_t0) * 1000,
        len(result.get("forecast", [])),
        _forecast_payload_bytes(result),
        _ovr_original_count,
        len(_ovr_records),
    )
    return result


def _pg_get_client_table(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct, lab_products,
    is_admin=False,
) -> dict:
    """Shell: catches all exceptions so the router never sees a 500 from this path."""
    _EMPTY = {"months": [], "rows": [], "totals": {}, "min_val": 0, "max_val": 0, "total_projected": 0}
    try:
        result = _pg_get_client_table_inner(
            user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct, lab_products,
            is_admin=is_admin,
        )
        return _inject_manual_client_rows_into_table(
            result, user_id=user_id,
            start_date=start_date, end_date=end_date,
            neg_filter=neg, subneg_filter=subneg,
            view_money=view_money, is_admin=is_admin,
            profiles_filter=profiles,
        )
    except Exception as exc:
        import traceback as _tb
        print(
            f"[FORECAST] _pg_get_client_table FAILED: {exc}\n{_tb.format_exc()}",
            flush=True,
        )
        return _EMPTY


def _pg_get_client_table_inner(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, growth_pct, lab_products,
    is_admin=False,
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
    _ct_total_t0 = time.perf_counter()
    _ct_base_ms = 0.0
    _ct_fetch_ovr_ms = 0.0
    _ct_delta_query_ms = 0.0
    _ct_apply_ms = 0.0
    _ct_base_rows = 0
    _ct_rows_loaded = 0
    _used_delta_ct = False

    prod_codes = _pg_resolve_prod_codes(products)
    val_prod   = _val_prod_filter(prod_codes)
    val_where = _build_filter_sql(
        start_date=start_date, end_date=end_date,
        profiles=profiles, neg=neg, subneg=subneg,
        products_as_codes=val_prod,
        products=None if val_prod is not None else products,
    )

    _ct_base_t0 = time.perf_counter()
    # Rama summary (PR-2): sin filtro de producto y con la tabla 1 disponible -> lee el
    # agregado pre-calculado con work_mem alto (el ORDER BY fecha derramaba a disco).
    # Si el summary da vacío o (en plata) monto all-zero, df_agg=None y cae al bloque
    # crudo de abajo, que conserva el fallback yhat×precio IDÉNTICO a hoy.
    _use_summary = (prod_codes is None and _forecast_summary_available())
    df_agg = None
    if _use_summary:
        _ct_col = "monto_yhat" if view_money else "yhat_cliente"
        df_agg = _query_agg_hi_mem(
            f"SELECT fantasia, nombre_grupo, fecha, "
            f"SUM(COALESCE({_ct_col}, 0)) AS val "
            f"FROM forecast_valorizado_summary WHERE {val_where} "
            f"GROUP BY fantasia, nombre_grupo, fecha ORDER BY fecha"
        )
        if df_agg.empty or (view_money and df_agg["val"].sum() == 0):
            df_agg = None   # cae al crudo (con su fallback yhat×precio)
    if df_agg is None:
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

    # Max hist date for growth adjustment — shared across all filter combinations
    _ct_base_ms = (time.perf_counter() - _ct_base_t0) * 1000
    _ct_base_rows = len(df_agg)

    max_hist_date = _get_max_hist_date_cached()

    _ct_fetch_t0 = time.perf_counter()
    _ovr_records_ct = _fetch_override_records(user_id, all_users=is_admin)
    _ct_fetch_ovr_ms = (time.perf_counter() - _ct_fetch_t0) * 1000
    _ct_ovr_original_count = len(_ovr_records_ct)
    if _ovr_records_ct:
        _ovr_records_ct = _consolidate_override_records(_ovr_records_ct, "client-table")
    overrides_active = bool(_ovr_records_ct)
    _forecast_diag(
        "[CLIENT_TABLE] user=%s overrides_active=%s override_count=%s effective_override_count=%s is_admin=%s",
        user_id, overrides_active, _ct_ovr_original_count, len(_ovr_records_ct), is_admin,
    )
    if overrides_active:
        _t_ct = time.perf_counter()
        _used_delta_ct = False
        _ct_rows_loaded = 0
        try:
            _ovr_maps_ct = _build_override_maps(_ovr_records_ct)
            _selectors_ct = _ovr_maps_ct.get("selectors", [])
            _forecast_diag(
                "[CLIENT_TABLE] delta_path selectors=%s ovr_records=%s",
                len(_selectors_ct), len(_ovr_records_ct),
            )
            if _selectors_ct:
                _sel_f_ct = f"({_safe_in('fantasia', _selectors_ct)})"
                _ct_delta_query_t0 = time.perf_counter()
                if view_money:
                    df_ovr_rows = _query_agg(
                        f"SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, "
                        f"SUM(COALESCE(monto_yhat, 0)) AS base_val "
                        f"FROM forecast_valorizado WHERE {val_where} AND {_sel_f_ct} "
                        f"GROUP BY fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie"
                    )
                    if not df_ovr_rows.empty and df_ovr_rows["base_val"].sum() == 0:
                        logger.warning("[CLIENT_TABLE] delta monto_yhat all-zero -- fallback to yhat x precio")
                        df_ovr_rows = _query_agg(
                            f"SELECT v.fantasia, v.nombre_grupo, v.cliente_id, v.fecha, v.subneg, v.codigo_serie, "
                            f"SUM(COALESCE(v.yhat_cliente, 0) * COALESCE(m.avg_precio, 0)) AS base_val "
                            f"FROM (SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, yhat_cliente "
                            f"      FROM forecast_valorizado WHERE {val_where} AND {_sel_f_ct}) v "
                            f"LEFT JOIN (SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS avg_precio "
                            f"           FROM forecast_main GROUP BY codigo_serie) m "
                            f"  ON v.codigo_serie = m.codigo_serie "
                            f"GROUP BY v.fantasia, v.nombre_grupo, v.cliente_id, v.fecha, v.subneg, v.codigo_serie"
                        )
                else:
                    df_ovr_rows = _query_agg(
                        f"SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, "
                        f"SUM(COALESCE(yhat_cliente, 0)) AS base_val "
                        f"FROM forecast_valorizado WHERE {val_where} AND {_sel_f_ct} "
                        f"GROUP BY fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie"
                    )
                _ct_delta_query_ms = (time.perf_counter() - _ct_delta_query_t0) * 1000
                _ct_rows_loaded = len(df_ovr_rows)
                _forecast_diag("[CLIENT_TABLE] delta_rows=%s selectors=%s", _ct_rows_loaded, len(_selectors_ct))
                if not df_ovr_rows.empty:
                    _ct_apply_t0 = time.perf_counter()
                    df_ovr_rows, _override_maps = _apply_override_effects_to_dataframe(
                        df_ovr_rows,
                        user_id=user_id,
                        base_growth_pct=growth_pct,
                        max_hist_date=max_hist_date,
                        _records=_ovr_records_ct,
                        is_admin=is_admin,
                    )
                    df_ovr_rows["val"] = df_ovr_rows["base_val"]
                    if max_hist_date is not None and "fecha" in df_ovr_rows.columns:
                        _fut_ct = df_ovr_rows["fecha"] > max_hist_date
                        df_ovr_rows.loc[_fut_ct, "val"] = (
                            df_ovr_rows.loc[_fut_ct, "base_val"]
                            * df_ovr_rows.loc[_fut_ct, "_annual_eff"]
                        )
                    else:
                        df_ovr_rows["val"] = df_ovr_rows["base_val"] * df_ovr_rows["_annual_eff"]
                    df_ovr_agg = (
                        df_ovr_rows.groupby(["fantasia", "nombre_grupo", "fecha"], dropna=False)["val"]
                        .sum().reset_index()
                    )
                    # fantasias as they appear in the DB; used to split df_agg.
                    _ovr_fantasias = set(df_ovr_agg["fantasia"].str.strip().unique())
                    # Non-override clients: keep from initial df_agg, apply base growth.
                    df_nonovr = df_agg[~df_agg["fantasia"].str.strip().isin(_ovr_fantasias)].copy()
                    if growth_pct != 0:
                        if max_hist_date is not None:
                            _fut_no = df_nonovr["fecha"] > max_hist_date
                            df_nonovr.loc[_fut_no, "val"] = (
                                df_nonovr.loc[_fut_no, "val"] * (1.0 + growth_pct / 100.0)
                            )
                        else:
                            df_nonovr["val"] = df_nonovr["val"] * (1.0 + growth_pct / 100.0)
                    df_agg = pd.concat([df_nonovr, df_ovr_agg], ignore_index=True)
                    _used_delta_ct = True
                    _ct_apply_ms = (time.perf_counter() - _ct_apply_t0) * 1000
                else:
                    # Selector filter returned 0 rows -> cannot apply override -> base growth only.
                    _forecast_diag("[CLIENT_TABLE] delta_rows empty -- applying base growth to all")
                    if growth_pct != 0:
                        if max_hist_date is not None:
                            _fut_ct2 = df_agg["fecha"] > max_hist_date
                            df_agg.loc[_fut_ct2, "val"] = df_agg.loc[_fut_ct2, "val"] * (1.0 + growth_pct / 100.0)
                        else:
                            df_agg["val"] = df_agg["val"] * (1.0 + growth_pct / 100.0)
                    _used_delta_ct = True
            else:
                # No valid selectors -> no effective overrides -> base growth only.
                _forecast_diag("[CLIENT_TABLE] no valid selectors -- applying base growth to all")
                if growth_pct != 0:
                    if max_hist_date is not None:
                        _fut_ct3 = df_agg["fecha"] > max_hist_date
                        df_agg.loc[_fut_ct3, "val"] = df_agg.loc[_fut_ct3, "val"] * (1.0 + growth_pct / 100.0)
                    else:
                        df_agg["val"] = df_agg["val"] * (1.0 + growth_pct / 100.0)
                _used_delta_ct = True
        except Exception as _delta_exc_ct:
            _forecast_diag_warn(
                "[CLIENT_TABLE] delta_path FAILED (%s) -- fallback to full table load",
                _delta_exc_ct,
            )
        if not _used_delta_ct:
            # FALLBACK: original full-table behavior (safe but higher RAM).
            if view_money:
                df_rows = _query_agg(
                    f"SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, "
                    f"SUM(COALESCE(monto_yhat, 0)) AS base_val "
                    f"FROM forecast_valorizado WHERE {val_where} "
                    f"GROUP BY fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie"
                )
                if not df_rows.empty and df_rows["base_val"].sum() == 0:
                    logger.warning("[CLIENT_TABLE] FALLBACK monto_yhat all-zero -- yhat x precio")
                    df_rows = _query_agg(
                        f"SELECT v.fantasia, v.nombre_grupo, v.cliente_id, v.fecha, v.subneg, v.codigo_serie, "
                        f"SUM(COALESCE(v.yhat_cliente, 0) * COALESCE(m.avg_precio, 0)) AS base_val "
                        f"FROM (SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, yhat_cliente "
                        f"      FROM forecast_valorizado WHERE {val_where}) v "
                        f"LEFT JOIN (SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS avg_precio "
                        f"           FROM forecast_main GROUP BY codigo_serie) m "
                        f"  ON v.codigo_serie = m.codigo_serie "
                        f"GROUP BY v.fantasia, v.nombre_grupo, v.cliente_id, v.fecha, v.subneg, v.codigo_serie"
                    )
            else:
                df_rows = _query_agg(
                    f"SELECT fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie, "
                    f"SUM(COALESCE(yhat_cliente, 0)) AS base_val "
                    f"FROM forecast_valorizado WHERE {val_where} "
                    f"GROUP BY fantasia, nombre_grupo, cliente_id, fecha, subneg, codigo_serie"
                )
            _forecast_diag_warn("[CLIENT_TABLE] FALLBACK full_rows=%s", len(df_rows))
            if not df_rows.empty:
                _ct_apply_t0 = time.perf_counter()
                df_rows, _override_maps = _apply_override_effects_to_dataframe(
                    df_rows,
                    user_id=user_id,
                    base_growth_pct=growth_pct,
                    max_hist_date=max_hist_date,
                    _records=_ovr_records_ct,
                    is_admin=is_admin,
                )
                df_rows["val"] = df_rows["base_val"]
                if max_hist_date is not None and "fecha" in df_rows.columns:
                    future_mask = df_rows["fecha"] > max_hist_date
                    df_rows.loc[future_mask, "val"] = (
                        df_rows.loc[future_mask, "base_val"] * df_rows.loc[future_mask, "_annual_eff"]
                    )
                else:
                    df_rows["val"] = df_rows["base_val"] * df_rows["_annual_eff"]
                df_agg = (
                    df_rows.groupby(["fantasia", "nombre_grupo", "fecha"], dropna=False)["val"]
                    .sum()
                    .reset_index()
                    .sort_values("fecha")
                )
                _ct_apply_ms = (time.perf_counter() - _ct_apply_t0) * 1000
        _t_ct_ms = (time.perf_counter() - _t_ct) * 1000
        _forecast_diag(
            "[CLIENT_TABLE] ovr_complete: rows_loaded=%s elapsed_ms=%.1f delta=%s",
            _ct_rows_loaded, _t_ct_ms, _used_delta_ct,
        )
    elif growth_pct != 0 and max_hist_date is not None:
        future_mask = df_agg["fecha"] > max_hist_date
        df_agg.loc[future_mask, "val"] = df_agg.loc[future_mask, "val"] * (1.0 + growth_pct / 100.0)

    _ct_build_t0 = time.perf_counter()

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

    # Vectorized row build — 40-50x faster than iterrows for 6K+ clients
    # Totals from unrounded values (matching original behavior: sum then round)
    totals          = {mn: round(float(pivot_reset[mn].sum()), 0) for mn in col_names}
    total_projected = round(sum(totals.values()), 0)
    # Round cells after computing totals
    pivot_reset["_lab"] = pivot_reset["Cliente"].isin(clients_with_lab).astype(int)
    for mn in col_names:
        pivot_reset[mn] = pivot_reset[mn].round(0)
    min_val = float(pivot_reset[col_names].min().min()) if col_names else 0
    max_val = float(pivot_reset[col_names].max().max()) if col_names else 0
    rows = [
        {**r, "Cliente": str(r["Cliente"]), "Grupo": str(r["Grupo"])}
        for r in pivot_reset[["Cliente", "Grupo", "_lab"] + col_names].to_dict("records")
    ]

    result = {
        "months": col_names, "rows": rows, "totals": totals,
        "min_val": min_val, "max_val": max_val, "total_projected": total_projected,
    }
    _ct_build_ms = (time.perf_counter() - _ct_build_t0) * 1000
    try:
        _ct_payload_bytes = len(json.dumps(result, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        _ct_payload_bytes = -1
    _forecast_diag(
        "[CLIENT_TABLE] phases base_ms=%.1f fetch_overrides_ms=%.1f delta_query_ms=%.1f "
        "apply_ms=%.1f build_json_ms=%.1f total_ms=%.1f base_rows=%s delta_rows=%s "
        "payload_bytes=%s delta=%s",
        _ct_base_ms,
        _ct_fetch_ovr_ms,
        _ct_delta_query_ms,
        _ct_apply_ms,
        _ct_build_ms,
        (time.perf_counter() - _ct_total_t0) * 1000,
        _ct_base_rows,
        _ct_rows_loaded,
        _ct_payload_bytes,
        _used_delta_ct,
    )
    return result


def _pg_get_treemap_data(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, period_date,
    is_admin=False,
) -> dict:
    """Shell: catches all exceptions so the router never sees a 500 from this path."""
    _EMPTY = {"ids": [], "labels": [], "parents": [], "values": [], "colors": [], "periods": [], "canals": []}
    try:
        return _pg_get_treemap_data_inner(
            user_id, start_date, end_date, profiles, neg, subneg, products, view_money, period_date,
            is_admin=is_admin,
        )
    except Exception as exc:
        import traceback as _tb
        print(
            f"[FORECAST] _pg_get_treemap_data FAILED: {exc}\n{_tb.format_exc()}",
            flush=True,
        )
        return _EMPTY


def _pg_get_treemap_data_inner(
    user_id, start_date, end_date, profiles, neg, subneg, products, view_money, period_date,
    is_admin=False,
) -> dict:
    """Memory-safe PostgreSQL treemap: GROUP BY (perfil, nombre_grupo, fantasia, cliente_id)."""
    _EMPTY = {"ids": [], "labels": [], "parents": [], "values": [], "colors": [], "periods": [], "canals": []}

    # All available periods (unfiltered) — cached to avoid repeated full-scan
    periods = _get_forecast_periods_cached()
    _tm_total_t0 = time.perf_counter()
    _tm_base_ms = 0.0
    _tm_fetch_ovr_ms = 0.0
    _tm_delta_query_ms = 0.0
    _tm_apply_ms = 0.0
    _tm_build_ms = 0.0
    _tm_base_rows = 0
    _tm_delta_rows = 0
    _tm_used_delta = False

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
    _tm_base_t0 = time.perf_counter()
    # Rama summary: sin filtro de producto y con la tabla 1 disponible -> lee el
    # agregado pre-calculado (gate default = forecast_valorizado_summary). Treemap
    # no ORDER BY -> HashAggregate en RAM, no necesita work_mem.
    if prod_codes is None and _forecast_summary_available():
        df_tree = _query_agg(
            f"SELECT perfil, nombre_grupo, fantasia, cliente_id, "
            f"SUM(COALESCE({val_col}, 0)) AS monto "
            f"FROM forecast_valorizado_summary WHERE {val_where} "
            f"GROUP BY perfil, nombre_grupo, fantasia, cliente_id"
        )
    else:
        df_tree = _query_agg(
            f"SELECT perfil, nombre_grupo, fantasia, cliente_id, "
            f"SUM(COALESCE({val_col}, 0)) AS monto "
            f"FROM forecast_valorizado WHERE {val_where} "
            f"GROUP BY perfil, nombre_grupo, fantasia, cliente_id"
        )
    if df_tree.empty:
        return {**_EMPTY, "periods": periods}
    _tm_base_ms = (time.perf_counter() - _tm_base_t0) * 1000
    _tm_base_rows = len(df_tree)

    max_hist_date = _get_max_hist_date_cached()

    _tm_fetch_t0 = time.perf_counter()
    _ovr_records_tm = _fetch_override_records(user_id, all_users=is_admin)
    _tm_fetch_ovr_ms = (time.perf_counter() - _tm_fetch_t0) * 1000
    _tm_ovr_original_count = len(_ovr_records_tm)
    if _ovr_records_tm:
        _ovr_records_tm = _consolidate_override_records(_ovr_records_tm, "treemap-data")
    _forecast_diag(
        "[TREEMAP] user=%s overrides_active=%s override_count=%s effective_override_count=%s is_admin=%s",
        user_id, bool(_ovr_records_tm), _tm_ovr_original_count, len(_ovr_records_tm), is_admin,
    )
    if bool(_ovr_records_tm):
        _tm_delta_t0 = time.perf_counter()
        try:
            _ovr_maps_tm = _build_override_maps(_ovr_records_tm)
            _selectors_tm = _ovr_maps_tm.get("selectors", [])
            _forecast_diag(
                "[TREEMAP] delta_path selectors=%s ovr_records=%s",
                len(_selectors_tm), len(_ovr_records_tm),
            )
            if _selectors_tm:
                _sel_f_tm = f"({_safe_in('fantasia', _selectors_tm)})"
                if view_money:
                    df_rows = _query_agg(
                        f"SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, "
                        f"COALESCE(monto_yhat, 0) AS base_val "
                        f"FROM forecast_valorizado WHERE {val_where} AND {_sel_f_tm}"
                    )
                    if not df_rows.empty and df_rows["base_val"].sum() == 0:
                        _forecast_diag_warn("[TREEMAP] delta monto_yhat all-zero -- fallback to yhat x precio")
                        df_rows = _query_agg(
                            f"SELECT v.perfil, v.nombre_grupo, v.fantasia, v.cliente_id, v.fecha, v.subneg, v.codigo_serie, "
                            f"SUM(COALESCE(v.yhat_cliente, 0) * COALESCE(m.avg_precio, 0)) AS base_val "
                            f"FROM (SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, yhat_cliente "
                            f"      FROM forecast_valorizado WHERE {val_where} AND {_sel_f_tm}) v "
                            f"LEFT JOIN (SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS avg_precio "
                            f"           FROM forecast_main GROUP BY codigo_serie) m "
                            f"  ON v.codigo_serie = m.codigo_serie "
                            f"GROUP BY v.perfil, v.nombre_grupo, v.fantasia, v.cliente_id, v.fecha, v.subneg, v.codigo_serie"
                        )
                else:
                    df_rows = _query_agg(
                        f"SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, "
                        f"COALESCE(yhat_cliente, 0) AS base_val "
                        f"FROM forecast_valorizado WHERE {val_where} AND {_sel_f_tm}"
                    )
                _tm_delta_query_ms = (time.perf_counter() - _tm_delta_t0) * 1000
                _tm_delta_rows = len(df_rows)
                _forecast_diag("[TREEMAP] delta_rows=%s selectors=%s", _tm_delta_rows, len(_selectors_tm))
                if not df_rows.empty:
                    _tm_apply_t0 = time.perf_counter()
                    df_rows, _override_maps = _apply_override_effects_to_dataframe(
                        df_rows,
                        user_id=user_id,
                        base_growth_pct=0.0,
                        max_hist_date=max_hist_date,
                        _records=_ovr_records_tm,
                        is_admin=is_admin,
                    )
                    df_rows["monto"] = df_rows["base_val"]
                    if max_hist_date is not None and "fecha" in df_rows.columns:
                        future_mask = df_rows["fecha"] > max_hist_date
                        df_rows.loc[future_mask, "monto"] = (
                            df_rows.loc[future_mask, "base_val"] * df_rows.loc[future_mask, "_annual_eff"]
                        )
                    else:
                        df_rows["monto"] = df_rows["base_val"] * df_rows["_annual_eff"]
                    df_ovr_tree = (
                        df_rows.groupby(["perfil", "nombre_grupo", "fantasia", "cliente_id"], dropna=False)["monto"]
                        .sum()
                        .reset_index()
                    )
                    _ovr_fantasias = set(df_ovr_tree["fantasia"].fillna("").astype(str).str.strip())
                    df_base_no_ovr = df_tree[
                        ~df_tree["fantasia"].fillna("").astype(str).str.strip().isin(_ovr_fantasias)
                    ].copy()
                    df_tree = pd.concat([df_base_no_ovr, df_ovr_tree], ignore_index=True)
                    _tm_apply_ms = (time.perf_counter() - _tm_apply_t0) * 1000
                    _tm_used_delta = True
                else:
                    _forecast_diag("[TREEMAP] delta_rows empty -- keeping base aggregate")
                    _tm_used_delta = True
            else:
                _forecast_diag("[TREEMAP] no valid selectors -- keeping base aggregate")
                _tm_delta_query_ms = (time.perf_counter() - _tm_delta_t0) * 1000
                _tm_used_delta = True
        except Exception as _delta_exc_tm:
            _forecast_diag_warn(
                "[TREEMAP] delta_path FAILED (%s) -- fallback to full table load",
                _delta_exc_tm,
            )
    if bool(_ovr_records_tm) and not _tm_used_delta:
        _tm_delta_t0 = time.perf_counter()
        if view_money:
            df_rows = _query_agg(
                f"SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, "
                f"COALESCE(monto_yhat, 0) AS base_val "
                f"FROM forecast_valorizado WHERE {val_where}"
            )
            if not df_rows.empty and df_rows["base_val"].sum() == 0:
                logger.warning(
                    "[FORECAST treemap] monto_yhat is all-zero while applying overrides; "
                    "falling back to yhat_cliente × avg(precio)."
                )
                df_rows = _query_agg(
                    f"SELECT v.perfil, v.nombre_grupo, v.fantasia, v.cliente_id, v.fecha, v.subneg, v.codigo_serie, "
                    f"SUM(COALESCE(v.yhat_cliente, 0) * COALESCE(m.avg_precio, 0)) AS base_val "
                    f"FROM (SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, yhat_cliente "
                    f"      FROM forecast_valorizado WHERE {val_where}) v "
                    f"LEFT JOIN (SELECT codigo_serie, AVG(COALESCE(precio, 0)) AS avg_precio "
                    f"           FROM forecast_main GROUP BY codigo_serie) m "
                    f"  ON v.codigo_serie = m.codigo_serie "
                    f"GROUP BY v.perfil, v.nombre_grupo, v.fantasia, v.cliente_id, v.fecha, v.subneg, v.codigo_serie"
                )
        else:
            df_rows = _query_agg(
                f"SELECT perfil, nombre_grupo, fantasia, cliente_id, fecha, subneg, codigo_serie, "
                f"COALESCE(yhat_cliente, 0) AS base_val "
                f"FROM forecast_valorizado WHERE {val_where}"
            )

        _tm_delta_query_ms = (time.perf_counter() - _tm_delta_t0) * 1000
        _tm_delta_rows = len(df_rows)
        _forecast_diag_warn("[TREEMAP] FALLBACK full_rows=%s", _tm_delta_rows)

        if not df_rows.empty:
            _tm_apply_t0 = time.perf_counter()
            df_rows, _override_maps = _apply_override_effects_to_dataframe(
                df_rows,
                user_id=user_id,
                base_growth_pct=0.0,
                max_hist_date=max_hist_date,
                _records=_ovr_records_tm,
                is_admin=is_admin,
            )
            df_rows["monto"] = df_rows["base_val"]
            if max_hist_date is not None and "fecha" in df_rows.columns:
                future_mask = df_rows["fecha"] > max_hist_date
                df_rows.loc[future_mask, "monto"] = (
                    df_rows.loc[future_mask, "base_val"] * df_rows.loc[future_mask, "_annual_eff"]
                )
            else:
                df_rows["monto"] = df_rows["base_val"] * df_rows["_annual_eff"]
            df_tree = (
                df_rows.groupby(["perfil", "nombre_grupo", "fantasia", "cliente_id"], dropna=False)["monto"]
                .sum()
                .reset_index()
            )

    # Normalise column name — PostgreSQL always returns lowercase aliases
    if bool(_ovr_records_tm) and not _tm_used_delta and _tm_delta_rows > 0:
        _tm_apply_ms = (time.perf_counter() - _tm_apply_t0) * 1000

    if bool(_ovr_records_tm):
        _forecast_diag(
            "[TREEMAP] ovr_complete: rows_loaded=%s elapsed_ms=%.1f delta=%s",
            _tm_delta_rows,
            _tm_delta_query_ms + _tm_apply_ms,
            _tm_used_delta,
        )

    _tm_build_t0 = time.perf_counter()

    if "monto" not in df_tree.columns and "Monto" in df_tree.columns:
        df_tree.rename(columns={"Monto": "monto"}, inplace=True)
    if "monto" not in df_tree.columns:
        logger.error("treemap: expected column 'monto' not found. Columns: %s", list(df_tree.columns))
        return {**_EMPTY, "periods": periods}

    # ── Inject manual client entries into PG treemap ───────────────────────
    _manual_df_pg_tm = _get_manual_entries_df(user_id, start_date, end_date, neg, subneg, is_admin=is_admin, profiles_filter=profiles)
    if not _manual_df_pg_tm.empty:
        _val_col_tm = "monto_yhat" if view_money else "yhat_cliente"
        if period_date:
            _target_m_pg = pd.to_datetime(period_date).replace(day=1)
            _manual_df_pg_tm = _manual_df_pg_tm[_manual_df_pg_tm["fecha"] == _target_m_pg]
        if not _manual_df_pg_tm.empty:
            print(f"[MANUAL_DASHBOARD] PG treemap manual rows={len(_manual_df_pg_tm)}", flush=True)
            _manual_tree = _manual_df_pg_tm.groupby(
                ["perfil", "fantasia", "nombre_grupo"], dropna=False
            )[_val_col_tm].sum().reset_index()
            _manual_tree.rename(columns={_val_col_tm: "monto"}, inplace=True)
            _manual_tree["cliente_id"] = _manual_tree["fantasia"]
            _manual_tree = _manual_tree.rename(columns={"fantasia": "fantasia", "nombre_grupo": "nombre_grupo"})
            for _c in df_tree.columns:
                if _c not in _manual_tree.columns:
                    _manual_tree[_c] = None
            df_tree = pd.concat([df_tree, _manual_tree[df_tree.columns]], ignore_index=True)

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

        for gr in cg.itertuples(index=False):
            grupo   = gr.Grupo
            g_par   = canal_id if gr.show_direct else otras_g
            if g_par is None:
                continue
            gid = f"{canal_id}::grupo::{grupo}"
            _add(gid, str(grupo), g_par, float(gr.Monto),
                 _blend_with_white(bc, 0.33 if gr.show_direct else 0.43))

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
            for cr in grp_cli.itertuples(index=False):
                c_par = gid if cr.show_c else otras_c
                if c_par is None:
                    continue
                tone = 0.56 - min(float(cr.share_g) * 0.35, 0.22)
                _add(f"{gid}::cliente::{cr.Cliente}", str(cr.Cliente),
                     c_par, float(cr.Monto), _blend_with_white(bc, max(0.10, tone)))

    result = {"ids": ids, "labels": labels, "parents": parents, "values": values,
              "colors": colors, "periods": periods, "canals": canals}
    _tm_build_ms = (time.perf_counter() - _tm_build_t0) * 1000
    try:
        _tm_payload_bytes = len(json.dumps(result, default=str, separators=(",", ":")).encode("utf-8"))
    except Exception:
        _tm_payload_bytes = -1
    _forecast_diag(
        "[TREEMAP] phases base_ms=%.1f fetch_overrides_ms=%.1f delta_query_ms=%.1f "
        "apply_ms=%.1f build_json_ms=%.1f total_ms=%.1f base_rows=%s delta_rows=%s "
        "payload_bytes=%s delta=%s",
        _tm_base_ms,
        _tm_fetch_ovr_ms,
        _tm_delta_query_ms,
        _tm_apply_ms,
        _tm_build_ms,
        (time.perf_counter() - _tm_total_t0) * 1000,
        _tm_base_rows,
        _tm_delta_rows,
        _tm_payload_bytes,
        _tm_used_delta,
    )
    return result


def _pg_get_client_detail(
    user_id, client_id, start_date, end_date, profiles, neg, subneg, products, growth_pct,
    is_admin=False,
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

    # max hist date and price map — both cached to avoid repeated full-table scans
    max_hist_date = _get_max_hist_date_cached()
    price_map: dict = _get_precio_map_cached()

    # Single override fetch — replaces 3 separate DB calls (_get_client_overrides_snapshot
    # + 2× _get_client_subneg_growths, the second of which was a duplicate bug)
    _cd_records = _fetch_override_records(user_id, client_selector=client_id, all_users=is_admin)
    _cd_maps = _build_override_maps(_cd_records)
    _cd_client_key = _clean_override_text(client_id)
    saved_overrides = {
        (codigo, month): float(payload["monthly_pct"])
        for (selector, codigo, month), payload in _cd_maps["cell"].items()
        if selector == _cd_client_key
    }
    # Cell override effective_from_month — keyed by (codigo, month) for fast lookup
    saved_overrides_efm: dict[tuple[str, str], str | None] = {
        (codigo, month): payload.get("effective_from_month")
        for (selector, codigo, month), payload in _cd_maps["cell"].items()
        if selector == _cd_client_key
    }
    saved_subneg_growths = dict(_cd_maps["subneg_growths"].get(_cd_client_key, {}))
    saved_subneg_efm = dict(_cd_maps.get("subneg_effective_months", {}).get(_cd_client_key, {}))
    # Effective month for new changes (used by the frontend to show the banner)
    effective_from_month = get_forecast_effective_month()

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
                    # Cell override: respect its own effective_from_month
                    if saved_pct is not None:
                        cell_efm = saved_overrides_efm.get((art, ds))
                        if cell_efm is None or ds >= cell_efm:
                            pct = saved_pct
                            adj = orig * (1.0 + pct / 100.0)
                        else:
                            saved_pct = None  # blocked month — treat as no override
                    if saved_pct is None and max_hist_date and d > max_hist_date and growth_pct != 0:
                        t  = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                        rm = (1 + growth_pct / 100.0) ** (1 / 12.0) - 1
                        adj = orig * (1 + rm) ** t
                        # Guardamos tasa MENSUAL — el gráfico la reconvierte a anual vía (1+rm)^12
                        pct = round(rm * 100, 4)
                    if (
                        saved_pct is None
                        and max_hist_date
                        and d > max_hist_date
                        and str(subneg_name) in saved_subneg_growths
                    ):
                        # Subneg override: apply only from its effective_from_month onwards
                        subneg_efm = saved_subneg_efm.get(str(subneg_name))
                        if subneg_efm is None or ds >= subneg_efm:
                            scoped_growth_pct = float(saved_subneg_growths.get(str(subneg_name)) or 0.0)
                            if scoped_growth_pct != 0:
                                t  = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                                rm = (1 + scoped_growth_pct / 100.0) ** (1 / 12.0) - 1
                                adj = orig * (1 + rm) ** t
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

    _client_growth_state = _derive_visible_client_growth_state(
        negocios_out, saved_subneg_growths, growth_pct
    )
    logger.debug(
        "[FORECAST client-detail growth] client=%s base=%s client_growth=%s subneg_growths=%s",
        client_id,
        growth_pct,
        _client_growth_state,
        saved_subneg_growths,
    )
    return {
        "client_id": client_id, "perfil": perfil, "neg": neg_val,
        "negocios": negocios_out, "dates": date_strs,
        "max_hist_date": max_hist_date.strftime("%Y-%m") if max_hist_date else None,
        "growth_pct": growth_pct,
        "client_growth_pct": _client_growth_state["value"],
        "client_growth_source": _client_growth_state["source"],
        "client_growth_mixed": _client_growth_state["mixed"],
        "subneg_growths": saved_subneg_growths,
        "effective_from_month": effective_from_month,
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


def preload_valorizado_parquet() -> None:
    """Pre-warm _data_cache in a background thread at server startup.

    Calls get_data() which loads the full valorizado parquet + CSV files into
    _data_cache so the first user request finds data already in memory.

    - On SQLite/local: loads ~10-30s in background; subsequent requests use in-memory data.
    - On PostgreSQL/Render: get_data() returns {} immediately — this is a no-op.
    - Thread-safe: get_data() already uses _cache_lock; only one load runs at a time.
    - Non-blocking: runs as a daemon thread; the server stays responsive.
    - Failure-safe: any exception is logged and the app continues working normally.
    """
    def _load() -> None:
        if engine is not None and "postgresql" in str(engine.url):
            logger.info("[FORECAST PRELOAD] skipped (PostgreSQL mode — data lives in DB)")
            return
        logger.info("[FORECAST PRELOAD] start")
        t0 = time.monotonic()
        try:
            data = get_data()
            elapsed_ms = (time.monotonic() - t0) * 1000
            n_rows = len(data.get("df_valorizado", pd.DataFrame())) if data else 0
            logger.info("[FORECAST PRELOAD] loaded in %.0f ms — df_valorizado %d rows", elapsed_ms, n_rows)
        except Exception as exc:
            logger.error("[FORECAST PRELOAD] failed: %s", exc, exc_info=True)

    threading.Thread(target=_load, name="forecast-preload", daemon=True).start()


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
    _fo_total_t0 = time.perf_counter()
    _fo_profiles_ms = 0.0
    _fo_neg_ms = 0.0
    _fo_subneg_ms = 0.0
    _fo_dates_ms = 0.0
    _fo_labs_ms = 0.0
    _fo_build_ms = 0.0
    if engine is not None and "postgresql" in str(engine.url):
        # ── Core filters: profiles, neg, subneg, dates ────────────────────
        # Isolated in their own try/except so a labs table absence does NOT
        # wipe out the core filter options (critical for Problem 2).
        # OPTIMIZED: replaced 9 separate pd.read_sql() calls with 4 combined
        # UNION queries in a single connection — saves ~6 DB round-trips.
        perfiles_set: set = set()
        negs_set: set = set()
        subnegs_set: set = set()
        min_dates = []
        max_dates = []
        try:
            with engine.connect() as conn:
                # 1. Combined distinct perfil from both tables (1 query vs 2)
                _fo_t0 = time.perf_counter()
                df_perfiles = pd.read_sql(
                    "SELECT DISTINCT perfil AS v FROM forecast_main WHERE perfil IS NOT NULL"
                    " UNION "
                    "SELECT DISTINCT perfil FROM forecast_valorizado WHERE perfil IS NOT NULL",
                    conn,
                )
                _fo_profiles_ms = (time.perf_counter() - _fo_t0) * 1000
                if not df_perfiles.empty:
                    perfiles_set.update(df_perfiles["v"].tolist())

                # 2. Combined distinct neg
                _fo_t0 = time.perf_counter()
                df_negs = pd.read_sql(
                    "SELECT DISTINCT neg AS v FROM forecast_main WHERE neg IS NOT NULL"
                    " UNION "
                    "SELECT DISTINCT neg FROM forecast_valorizado WHERE neg IS NOT NULL",
                    conn,
                )
                _fo_neg_ms = (time.perf_counter() - _fo_t0) * 1000
                if not df_negs.empty:
                    negs_set.update(df_negs["v"].tolist())

                # 3. Combined distinct subneg
                _fo_t0 = time.perf_counter()
                df_subnegs = pd.read_sql(
                    "SELECT DISTINCT subneg AS v FROM forecast_main WHERE subneg IS NOT NULL"
                    " UNION "
                    "SELECT DISTINCT subneg FROM forecast_valorizado WHERE subneg IS NOT NULL",
                    conn,
                )
                _fo_subneg_ms = (time.perf_counter() - _fo_t0) * 1000
                if not df_subnegs.empty:
                    subnegs_set.update(df_subnegs["v"].tolist())

                # 4. Date range from both tables in one query
                _fo_t0 = time.perf_counter()
                df_dates = pd.read_sql(
                    "SELECT MIN(fecha) AS min_d, MAX(fecha) AS max_d FROM ("
                    "  SELECT fecha FROM forecast_main"
                    "  UNION ALL"
                    "  SELECT fecha FROM forecast_valorizado"
                    ") t",
                    conn,
                )
                _fo_dates_ms = (time.perf_counter() - _fo_t0) * 1000
                if not df_dates.empty:
                    if pd.notnull(df_dates["min_d"].iloc[0]):
                        min_dates.append(pd.to_datetime(df_dates["min_d"].iloc[0]))
                    if pd.notnull(df_dates["max_d"].iloc[0]):
                        max_dates.append(pd.to_datetime(df_dates["max_d"].iloc[0]))

                # 5. Labs (optional — must NOT affect core filters if absent)
                all_labs: set = set()
                try:
                    _fo_t0 = time.perf_counter()
                    labs_df = pd.read_sql(
                        "SELECT laboratorios FROM forecast_product_labs", conn
                    )
                    _fo_labs_ms = (time.perf_counter() - _fo_t0) * 1000
                    if not labs_df.empty:
                        for _, row in labs_df.iterrows():
                            try:
                                all_labs.update(json.loads(row["laboratorios"]))
                            except Exception:
                                pass
                except Exception as labs_exc:
                    logger.warning("Filter options: forecast_product_labs not available (%s)", labs_exc)

            _fo_t0 = time.perf_counter()
            def sanitize_list(s):
                return sorted(list({str(x).strip() for x in s if x is not None and pd.notna(x) and str(x).strip() != "" and str(x).lower().strip() != "nan" and str(x).lower().strip() != "none"}))

            perfiles = sanitize_list(perfiles_set)
            negs = sanitize_list(negs_set)
            subnegs = sanitize_list(subnegs_set)

            min_date = min(min_dates).strftime("%Y-%m-%d") if min_dates else None
            max_date = max(max_dates).strftime("%Y-%m-%d") if max_dates else None
            _fo_build_ms = (time.perf_counter() - _fo_t0) * 1000

        except Exception as exc:
            logger.error("Filter options DB error (core): %s", exc, exc_info=True)
            return {"profiles": [], "neg": [], "subneg": [], "labs": [], "min_date": None, "max_date": None}

        result = {
            "profiles": perfiles,
            "neg": negs,
            "subneg": subnegs,
            "labs": sorted(all_labs),
            "min_date": min_date,
            "max_date": max_date,
        }
        _forecast_diag(
            "[FILTER_OPTIONS] phases profiles_ms=%.1f neg_ms=%.1f subneg_ms=%.1f dates_ms=%.1f "
            "labs_ms=%.1f build_json_ms=%.1f total_ms=%.1f rows=%s payload_bytes=%s",
            _fo_profiles_ms,
            _fo_neg_ms,
            _fo_subneg_ms,
            _fo_dates_ms,
            _fo_labs_ms,
            _fo_build_ms,
            (time.perf_counter() - _fo_total_t0) * 1000,
            len(result.get("profiles", [])),
            _forecast_payload_bytes(result),
        )
        return result
    else:
        all_labs: set = set()
        perfiles_set = set()
        negs_set = set()
        subnegs_set = set()
        min_dates = []
        max_dates = []

        def _consume_filter_df(df: pd.DataFrame) -> None:
            if df is None or df.empty:
                return
            if "perfil" in df.columns:
                perfiles_set.update(df["perfil"].dropna().tolist())
            if "neg" in df.columns:
                negs_set.update(df["neg"].dropna().tolist())
            if "subneg" in df.columns:
                subnegs_set.update(df["subneg"].dropna().tolist())
            if "fecha" in df.columns:
                valid = pd.to_datetime(df["fecha"], errors="coerce").dropna()
                if not valid.empty:
                    min_dates.append(valid.min())
                    max_dates.append(valid.max())

        needed = {"perfil", "neg", "subneg", "fecha"}
        try:
            if FORECAST_FILE.exists():
                df_main_opts = pd.read_csv(
                    str(FORECAST_FILE),
                    sep=";",
                    decimal=",",
                    encoding="utf-8-sig",
                    low_memory=False,
                    usecols=lambda c: str(c).strip().lower() in needed,
                )
                df_main_opts.rename(columns={"Neg": "neg", "Subneg": "subneg"}, inplace=True)
                df_main_opts = _apply_neg_names(df_main_opts, NEGOCIOS_FILE)
                _consume_filter_df(df_main_opts)
        except Exception as exc:
            logger.warning("Filter options local forecast_main light read failed: %s", exc)

        try:
            val_path = _VALORIZADO_PARQUET if _VALORIZADO_PARQUET.exists() else _VALORIZADO_PREPARED
            if val_path.exists() and val_path.suffix.lower() == ".parquet":
                import pyarrow.parquet as _pq
                available = set(_pq.read_schema(str(val_path)).names)
                df_val_opts = pd.read_parquet(str(val_path), columns=[c for c in needed if c in available])
            elif val_path.exists():
                df_val_opts = pd.read_csv(
                    str(val_path),
                    encoding="utf-8-sig",
                    low_memory=False,
                    usecols=lambda c: str(c).strip() in needed,
                )
            else:
                df_val_opts = pd.DataFrame()
            _consume_filter_df(df_val_opts)
        except Exception as exc:
            logger.warning("Filter options local valorizado light read failed: %s", exc)

        try:
            if ARTICULOS_FILE.exists():
                df_labs = pd.read_csv(
                    str(ARTICULOS_FILE),
                    sep=",",
                    encoding="latin-1",
                    dtype=str,
                    low_memory=False,
                    usecols=lambda c: str(c).strip().lower() == "laboratorio_descrip",
                )
                if not df_labs.empty:
                    all_labs.update(df_labs.iloc[:, 0].dropna().astype(str).str.strip().tolist())
        except Exception as exc:
            logger.warning("Filter options local labs light read failed: %s", exc)

        def sanitize_list(s):
            return sorted(list({str(x).strip() for x in s if x is not None and pd.notna(x) and str(x).strip() != "" and str(x).lower().strip() != "nan" and str(x).lower().strip() != "none"}))

        min_date = min(min_dates).strftime("%Y-%m-%d") if min_dates else None
        max_date = max(max_dates).strftime("%Y-%m-%d") if max_dates else None

        result = {
            "profiles": sanitize_list(perfiles_set),
            "neg": sanitize_list(negs_set),
            "subneg": sanitize_list(subnegs_set),
            "labs": sorted(all_labs),
            "min_date": min_date,
            "max_date": max_date,
        }
        print(f"[FORECAST FILTER-OPTIONS] neg={result['neg']} subneg_count={len(result['subneg'])}", flush=True)
        return result


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_product_list(profiles: list | None = None, neg: list | None = None) -> list[dict]:
    import pandas as pd
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_product_list(profiles=profiles, neg=neg)
    else:
        if not _data_cache:
            light = _local_get_product_list_light(profiles=profiles, neg=neg)
            if light:
                return light
        data = get_data()
        df_main = data.get("df_main", pd.DataFrame())
        df_val = data.get("df_valorizado", pd.DataFrame())
        lab_map = data.get("product_lab_map", {})

    p_list = []
    if df_main is not None and not df_main.empty:
        cols = [c for c in ["neg", "codigo_serie", "perfil", "descripcion"] if c in df_main.columns]
        p_list.append(df_main[cols])
    if df_val is not None and not df_val.empty:
        cols = [c for c in ["neg", "codigo_serie", "perfil", "descripcion"] if c in df_val.columns]
        p_list.append(df_val[cols])

    if not p_list:
        return []

    df_comb = pd.concat(p_list, ignore_index=True)
    if "codigo_serie" in df_comb.columns and "descripcion" not in df_comb.columns:
        df_comb["descripcion"] = df_comb["codigo_serie"]
    elif "descripcion" in df_comb.columns and "codigo_serie" not in df_comb.columns:
        df_comb["codigo_serie"] = df_comb["descripcion"]
    
    if "codigo_serie" not in df_comb.columns:
        return []

    df_neg = df_comb.drop_duplicates(["neg", "codigo_serie"]).copy()
    df_neg["neg"] = df_neg["neg"].fillna("Varios").replace({"nan": "Varios", "None": "Varios", "none": "Varios"})

    # Filter df_neg by profiles and neg
    mask_neg = pd.Series(True, index=df_neg.index)
    if profiles:
        mask_neg &= df_neg["perfil"].isin(profiles) if "perfil" in df_neg.columns else mask_neg
    if neg:
        mask_neg &= df_neg["neg"].isin(neg) if "neg" in df_neg.columns else mask_neg
    df_neg = df_neg[mask_neg].copy()

    if df_neg.empty:
        return []

    df_vol = pd.DataFrame()
    if df_val is not None and not df_val.empty:
        mask_v = pd.Series(True, index=df_val.index)
        if profiles:
            mask_v &= df_val["perfil"].isin(profiles) if "perfil" in df_val.columns else mask_v
        if neg:
            mask_v &= df_val["neg"].isin(neg) if "neg" in df_val.columns else mask_v
        
        df_val_f = df_val[mask_v]
        if not df_val_f.empty and "codigo_serie" in df_val_f.columns:
            df_vol = df_val_f.groupby("codigo_serie")["monto_yhat"].sum().reset_index().rename(columns={"monto_yhat": "vol_venta"})

    if (df_vol.empty or df_vol["vol_venta"].sum() == 0) and df_main is not None and not df_main.empty:
        mask_m = pd.Series(True, index=df_main.index)
        if profiles:
            mask_m &= df_main["perfil"].isin(profiles) if "perfil" in df_main.columns else mask_m
        if neg:
            mask_m &= df_main["neg"].isin(neg) if "neg" in df_main.columns else mask_m
        
        df_main_f = df_main[mask_m].copy()
        if not df_main_f.empty and "codigo_serie" in df_main_f.columns:
            if "precio" not in df_main_f.columns:
                df_main_f["precio"] = 1500
            df_main_f["vol_venta"] = (
                df_main_f["y"].fillna(0) + df_main_f["yhat"].fillna(0)
            ) * df_main_f["precio"].fillna(0)
            df_vol = df_main_f.groupby("codigo_serie")["vol_venta"].sum().reset_index()

    # Merge df_neg and df_vol
    if df_vol.empty:
        df_f = df_neg.copy()
        df_f["vol_venta"] = 0.0
    else:
        df_f = pd.merge(df_neg, df_vol, on="codigo_serie", how="left")
        df_f["vol_venta"] = df_f["vol_venta"].fillna(0.0)

    if "descripcion" not in df_f.columns:
        df_f["descripcion"] = df_f["codigo_serie"]

    ranking = (
        df_f.groupby(["neg", "descripcion"])["vol_venta"]
        .sum()
        .reset_index()
        .sort_values(["neg", "vol_venta"], ascending=[True, False])
    )

    ranking["labs"] = ranking["descripcion"].apply(lambda x: lab_map.get(str(x), []))
    return ranking.to_dict(orient="records")


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_chart_data(
    user_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    growth_pct: float = 0.0,
    is_admin: bool = False,
) -> dict:
    import pandas as pd
    global_overrides = bool(is_admin)
    _engine_url = str(engine.url) if engine is not None else "NO_ENGINE"
    _is_pg = engine is not None and "postgresql" in _engine_url
    if _is_pg:
        return _pg_get_chart_data(
            user_id=user_id,
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money, growth_pct=growth_pct,
            is_admin=global_overrides,
        )

    _loc_ovr_records_for_debug = _fetch_override_records(user_id, all_users=global_overrides)
    _ovr_active = bool(_loc_ovr_records_for_debug)
    _loc_ovr_user_ids = _override_record_user_ids(_loc_ovr_records_for_debug)
    logger.info(
        "[FORECAST LOCAL] chart user_id=%s override_scope=%s override_count=%s override_user_ids=%s",
        user_id,
        "global" if global_overrides else "user",
        len(_loc_ovr_records_for_debug),
        _loc_ovr_user_ids,
    )
    _has_manual = bool(_query_manual_clients(user_id, is_admin=is_admin))
    if not _data_cache and not _ovr_active and not _has_manual:
        light = _local_get_chart_data_light(
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            growth_pct=growth_pct,
        )
        if light:
            return light

    data = get_data()
    df = data.get("df_main", pd.DataFrame())
    # Use unpatched base for all chart lines — overrides are reflected in Total_User_Adj (Line 4)
    df_val = data.get("df_valorizado", pd.DataFrame())
    df_imp_hist = data.get("df_imp_hist", pd.DataFrame())
    df_fact_2026 = data.get("df_fact_2026", pd.DataFrame())
    df_val_f = pd.DataFrame()

    if df.empty:
        return {"history": [], "forecast": [], "val_2026": [], "kpis": {}}

    # Date mask (separating history and forecast)
    mask = pd.Series(True, index=df.index)
    start_ts = pd.to_datetime(start_date) if start_date else None
    end_ts = pd.to_datetime(end_date) if end_date else None
    if start_ts is not None or end_ts is not None:
        mask_hist = pd.Series(True, index=df.index)
        if start_ts is not None and start_ts.year <= 2025:
            mask_hist &= df["fecha"] >= start_ts
        if end_ts is not None and end_ts.year <= 2025:
            mask_hist &= df["fecha"] <= end_ts
        mask_fcst = pd.Series(True, index=df.index)
        if start_ts is not None:
            mask_fcst &= df["fecha"] >= start_ts
        if end_ts is not None:
            mask_fcst &= df["fecha"] <= end_ts
        is_hist = df.get("tipo", pd.Series()) == "hist"
        mask = (is_hist & mask_hist) | (~is_hist & mask_fcst)
    if profiles and "perfil" in df.columns:
        mask &= df["perfil"].isin(profiles)
    if neg and "neg" in df.columns:
        mask &= df["neg"].isin(neg)
    if subneg and "subneg" in df.columns:
        mask &= df["subneg"].isin(subneg)
    if products and "descripcion" in df.columns:
        mask &= df["descripcion"].isin(products)

    df_filt = df[mask].copy()
    print(f"[FORECAST CHART FULL-PATH] profiles={profiles} neg={neg} | total_rows={len(df)} filtered_rows={len(df_filt)} neg_col_sample={df['neg'].dropna().unique()[:3].tolist() if 'neg' in df.columns else 'NO_NEG_COL'}", flush=True)

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

    # Derive allowed codigo_serie for neg/subneg filter when hist/val lack those columns.
    # df (df_main) has neg/subneg after _apply_neg_names; same pattern as df_fact_2026 below.
    _neg_allowed_codes: "set | None" = None
    if (neg or subneg) and "codigo_serie" in df.columns:
        _neg_mask = pd.Series(True, index=df.index)
        if neg and "neg" in df.columns:
            _neg_mask &= df["neg"].isin(neg)
        if subneg and "subneg" in df.columns:
            _neg_mask &= df["subneg"].isin(subneg)
        _neg_allowed_codes = set(df.loc[_neg_mask, "codigo_serie"].dropna().astype(str).unique())
        print(f"[FORECAST HIST NEG FILTER] neg={neg} subneg={subneg} | neg_allowed_codes_count={len(_neg_allowed_codes)}", flush=True)

    df_hist = pd.DataFrame()
    if view_money and not df_imp_hist.empty and "imp_hist" in df_imp_hist.columns and "fecha" in df_imp_hist.columns:
        mask_ih = pd.Series(True, index=df_imp_hist.index)
        start_ts = pd.to_datetime(start_date) if start_date else None
        end_ts = pd.to_datetime(end_date) if end_date else None
        if start_ts is not None and start_ts.year <= 2025:
            mask_ih &= df_imp_hist["fecha"] >= start_ts
        if end_ts is not None and end_ts.year <= 2025:
            mask_ih &= df_imp_hist["fecha"] <= end_ts
        if profiles and "perfil" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["perfil"].isin(profiles)
        if neg and "neg" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["neg"].isin(neg)
        if subneg and "subneg" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["subneg"].isin(subneg)
        # Fallback: df_imp_hist has no neg/subneg cols — filter via codigo_serie derived from df_main
        if _neg_allowed_codes is not None and "codigo_serie" in df_imp_hist.columns:
            if "neg" not in df_imp_hist.columns and "subneg" not in df_imp_hist.columns:
                mask_ih &= df_imp_hist["codigo_serie"].astype(str).isin(_neg_allowed_codes)
        # Filter by selected products via codigo_serie lookup (df_imp_hist has no descripcion)
        if products and "codigo_serie" in df_imp_hist.columns:
            mask_ih &= df_imp_hist["codigo_serie"].astype(str).isin(_prod_codes_lookup)
        _hist_rows_before = int(mask_ih.sum())
        df_hist = (
            df_imp_hist[mask_ih]
            .groupby("fecha")
            .agg(Total_Venta=("imp_hist", "sum"))
            .reset_index()
        )
        print(f"[FORECAST HIST] rows_matched={_hist_rows_before} months={len(df_hist)} total_venta={df_hist['Total_Venta'].sum():.0f}", flush=True)

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
        # Fallback: df_val (parquet) has no neg/subneg cols — filter via codigo_serie from df_main
        if _neg_allowed_codes is not None and "codigo_serie" in df_val.columns:
            if "neg" not in df_val.columns and "subneg" not in df_val.columns:
                mask_v &= df_val["codigo_serie"].astype(str).isin(_neg_allowed_codes)
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
            df_fcst["Total_User_Adj"] = df_fcst["Total_Forecast"] * _g_base
            # Pass pre-fetched records to avoid large SQL IN clause (SQLite 999-var limit)
            _loc_ovr_records = list(_loc_ovr_records_for_debug) if (user_id or global_overrides) else []
            if _ovr_active and not df_val_f.empty:
                _loc_max_hist = df_hist["fecha"].max() if not df_hist.empty else pd.Timestamp("2000-01-01")
                _df_u_sql, _ovr_maps_sql = _apply_override_effects_to_dataframe(
                    df_val_f,
                    user_id=user_id,
                    base_growth_pct=growth_pct,
                    max_hist_date=_loc_max_hist,
                    _records=_loc_ovr_records,
                    is_admin=global_overrides,
                )
                if not _df_u_sql.empty:
                    _df_u_sql["_ua_sql"] = _df_u_sql[col_y] * _df_u_sql["_annual_eff"]
                    _ua_sql = (
                        _df_u_sql.groupby("fecha")["_ua_sql"].sum()
                        .reset_index()
                        .rename(columns={"_ua_sql": "Total_User_Adj_SQL"})
                    )
                    df_fcst = df_fcst.merge(_ua_sql, on="fecha", how="left")
                    df_fcst["Total_User_Adj"] = df_fcst["Total_User_Adj_SQL"].fillna(
                        df_fcst["Total_User_Adj"]
                    )
                    df_fcst.drop(columns=["Total_User_Adj_SQL"], inplace=True)
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

    # ── Inject manual client entries into forecast totals ─────────────────
    _manual_df_chart = _get_manual_entries_df(user_id, start_date, end_date, neg, subneg, is_admin=is_admin, profiles_filter=profiles)
    if not _manual_df_chart.empty:
        _val_col_m = "monto_yhat" if view_money else "yhat_cliente"
        _manual_monthly = (
            _manual_df_chart.groupby("fecha")[_val_col_m]
            .sum()
            .reset_index()
            .rename(columns={_val_col_m: "_manual_amt"})
        )
        print(f"[MANUAL_DASHBOARD] chart manual monthly total={_manual_monthly['_manual_amt'].sum():.0f}", flush=True)
        new_rows = []
        for _, mr in _manual_monthly.iterrows():
            mask_m = df_fcst["fecha"] == mr["fecha"]
            if mask_m.any():
                df_fcst.loc[mask_m, "Total_Forecast"] = df_fcst.loc[mask_m, "Total_Forecast"] + mr["_manual_amt"]
                df_fcst.loc[mask_m, "Total_User_Adj"] = df_fcst.loc[mask_m, "Total_User_Adj"] + mr["_manual_amt"]
                if "Total_Li" in df_fcst.columns:
                    df_fcst.loc[mask_m, "Total_Li"] = df_fcst.loc[mask_m, "Total_Li"] + mr["_manual_amt"]
                if "Total_Ls" in df_fcst.columns:
                    df_fcst.loc[mask_m, "Total_Ls"] = df_fcst.loc[mask_m, "Total_Ls"] + mr["_manual_amt"]
            else:
                new_rows.append({
                    "fecha": mr["fecha"],
                    "Total_Forecast": mr["_manual_amt"],
                    "Total_Li": mr["_manual_amt"],
                    "Total_Ls": mr["_manual_amt"],
                    "Total_User_Adj": mr["_manual_amt"],
                })
        if new_rows:
            df_fcst = pd.concat([df_fcst, pd.DataFrame(new_rows)], ignore_index=True)
        df_fcst = df_fcst.sort_values("fecha").reset_index(drop=True)

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
    _adjustment_summary = _forecast_adjustment_summary(forecast_records)
    logger.info(
        "[FORECAST LOCAL] chart-data: user_id=%s override_scope=%s override_count=%s adjusted=%s adjusted_diff_sum=%.2f",
        user_id,
        "global" if global_overrides else "user",
        len(_loc_ovr_records_for_debug),
        _adjustment_summary["has_adjusted_series"],
        _adjustment_summary["adjusted_diff_sum"],
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
        # Profile filter: use tipocli (real commercial profile from clientes.csv).
        # forecast_fact_2026.perfil contains internal codes ("9 - 1"), NOT the commercial profile.
        if profiles:
            if "tipocli" in df_fact_2026.columns:
                mask_f2 &= df_fact_2026["tipocli"].isin(profiles)
            elif "codigo_serie" in df_fact_2026.columns and "codigo_serie" in df.columns:
                _pfil_mask = pd.Series(True, index=df.index)
                if "perfil" in df.columns:
                    _pfil_mask &= df["perfil"].isin(profiles)
                _pfil_series = set(df.loc[_pfil_mask, "codigo_serie"].astype(str).unique())
                mask_f2 &= df_fact_2026["codigo_serie"].astype(str).isin(_pfil_series)
        # Neg/subneg filter: product-level via codigo_serie → df (forecast_main).
        if (neg or subneg) and "codigo_serie" in df_fact_2026.columns and "codigo_serie" in df.columns:
            _ns_mask = pd.Series(True, index=df.index)
            if neg and "neg" in df.columns:
                _ns_mask &= df["neg"].isin(neg)
            if subneg and "subneg" in df.columns:
                _ns_mask &= df["subneg"].isin(subneg)
            _ns_series = set(df.loc[_ns_mask, "codigo_serie"].astype(str).unique())
            mask_f2 &= df_fact_2026["codigo_serie"].astype(str).isin(_ns_series)
        # Products filter: direct codigo_serie lookup.
        if products and "codigo_serie" in df_fact_2026.columns:
            mask_f2 &= df_fact_2026["codigo_serie"].astype(str).isin(_prod_codes_lookup)
        df_f2 = df_fact_2026[mask_f2].copy()

        # All months aggregated (analytical layer: Jan+Feb+Mar)
        df_v2026_all = df_f2.groupby("fecha").agg(Total_Venta=("imp_hist", "sum")).reset_index()

        # Solo meses CERRADOS (tope dinámico = primer día del mes en curso; _fact_2026_closed_month_cap).
        # df_v2026_all queda completo para el cálculo de accuracy (usa su propia lógica de mes abierto [:-1]).
        _fact_cap_ts = pd.Timestamp(_fact_2026_closed_month_cap())
        fact_2026_sum = float(df_f2.loc[df_f2["fecha"] < _fact_cap_ts, "imp_hist"].sum())
        meta_completeness = (fact_2026_sum / total_proyectado_2026_annual * 100) if total_proyectado_2026_annual > 0 else 0.0

        # Chart line: solo meses cerrados.
        df_v2026_chart = df_v2026_all[df_v2026_all["fecha"] < _fact_cap_ts].copy()

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
        "override_debug": {
            "scope": "global" if global_overrides else "user",
            "all_users": global_overrides,
            "requested_user_id": int(user_id) if user_id is not None else None,
            "override_count": len(_loc_ovr_records_for_debug),
            "override_user_ids": _loc_ovr_user_ids,
            **_adjustment_summary,
        },
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
            "n_products": int(df_val_f["descripcion"].nunique()) if (not df_val_f.empty and "descripcion" in df_val_f.columns) else (int(df_filt["descripcion"].nunique()) if "descripcion" in df_filt.columns else 0),
        },
    }


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_client_table(
    user_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    growth_pct: float = 0.0,
    lab_products: list | None = None,
    is_admin: bool = False,
) -> dict:
    import pandas as pd
    _db_path = "postgresql" if (engine is not None and "postgresql" in str(engine.url)) else "sqlite"
    print(f"[CLIENT_TABLE] get_client_table called — db={_db_path} user={user_id} growth_pct={growth_pct}", flush=True)
    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_client_table(
            user_id=user_id,
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money,
            growth_pct=growth_pct, lab_products=lab_products,
            is_admin=is_admin,
        )

    data = get_data()
    df_val = _get_patched_df_val(user_id=user_id, is_admin=is_admin)
    print(f"[CLIENT_TABLE] sqlite path — df_val rows={len(df_val)} "
          f"has_override_rows={int(df_val['_has_override'].sum()) if not df_val.empty and '_has_override' in df_val.columns else 'N/A'}",
          flush=True)
    df_main = data.get("df_main", pd.DataFrame())

    def _empty_result_with_manual(months_list=None):
        """Return an empty result that still includes manual client rows."""
        if months_list is None:
            months_list = []
            if start_date and end_date:
                try:
                    months_list = [
                        m.strftime("%b %Y").title()
                        for m in pd.date_range(
                            start=pd.to_datetime(start_date),
                            end=pd.to_datetime(end_date),
                            freq="MS",
                        )
                    ]
                except Exception:
                    pass
        base = {
            "months": months_list,
            "rows": [],
            "totals": {m: 0.0 for m in months_list},
            "min_val": 0,
            "max_val": 0,
            "total_projected": 0,
        }
        return _inject_manual_client_rows_into_table(
            base, user_id=user_id,
            start_date=start_date, end_date=end_date,
            neg_filter=neg, subneg_filter=subneg,
            view_money=view_money, is_admin=is_admin,
            profiles_filter=profiles,
        )

    if df_val.empty:
        return _empty_result_with_manual()

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
        return _empty_result_with_manual()

    if "fantasia" in df_c.columns:
        _match_test = df_c[df_c["fantasia"].astype(str).str.contains("MINISTERIO DE SALUD PCIA DE BS AS", case=False, na=False)].copy()
        if not _match_test.empty:
            if "_has_override" in _match_test.columns and _match_test["_has_override"].fillna(False).any():
                _row = _match_test[_match_test["_has_override"].fillna(False)].sort_values("fecha").iloc[0]
            else:
                _row = _match_test.sort_values("fecha").iloc[0]
            _has_override = bool(_row.get("_has_override", False))
            _annual_eff = float(_row.get("_annual_eff", 1.0) or 1.0)
            _applied_growth_pct = round((_annual_eff - 1.0) * 100.0, 4) if _has_override else float(growth_pct or 0.0)
            _scope = str(_row.get("_override_scope", "base"))
            _efm = ""
            try:
                _records = _fetch_override_records(user_id, client_selector=str(_row.get("fantasia", "")), all_users=is_admin)
                _maps = _build_override_maps(_records)
                _client_key = _clean_override_text(_row.get("fantasia", ""))
                _subneg_key = _clean_override_text(_row.get("subneg", ""))
                _efm = (
                    _maps.get("subneg_effective_months", {})
                    .get(_client_key, {})
                    .get(_subneg_key, "")
                )
            except Exception:
                _efm = ""
            print("[CLIENT_TABLE MATCH TEST] fantasia=", _row.get("fantasia", ""), flush=True)
            print("[CLIENT_TABLE MATCH TEST] grupo=", _row.get("nombre_grupo", ""), flush=True)
            print("[CLIENT_TABLE MATCH TEST] negocio=", _row.get("neg", ""), flush=True)
            print("[CLIENT_TABLE MATCH TEST] subneg=", _row.get("subneg", ""), flush=True)
            print("[CLIENT_TABLE MATCH TEST] has_override=", _has_override, flush=True)
            print("[CLIENT_TABLE MATCH TEST] applied_growth_pct=", _applied_growth_pct, flush=True)
            print("[CLIENT_TABLE MATCH TEST] effective_from_month=", _efm, flush=True)
            print("[CLIENT_TABLE MATCH TEST] scope=", _scope, flush=True)

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

    # Apply growth only to non-overridden future rows BEFORE pivoting.
    # _get_patched_df_val already multiplied overridden rows by their override factor,
    # so applying global growth again to those rows would double-count.
    if growth_pct != 0 and not df_main.empty and "tipo" in df_main.columns and "fecha" in df_main.columns:
        _max_hist = df_main[df_main["tipo"] == "hist"]["fecha"].max()
        if pd.notna(_max_hist):
            _future_dates_pre = sorted(df_c.loc[df_c["fecha"] > _max_hist, "fecha"].unique())
            if _future_dates_pre:
                _start_proj = _future_dates_pre[0]
                _has_ov_col = "_has_override" in df_c.columns
                for _fd in _future_dates_pre:
                    _months_diff = (_fd.year - _start_proj.year) * 12 + (_fd.month - _start_proj.month)
                    _quarters = (_months_diff // 3) + 1
                    _factor = 1.0 + (growth_pct * _quarters / 100.0)
                    _dm = df_c["fecha"] == _fd
                    if _has_ov_col:
                        _dm = _dm & (~df_c["_has_override"].fillna(False))
                    df_c.loc[_dm, val_col] = df_c.loc[_dm, val_col].astype(float) * _factor

    # Pivot
    grp = df_c.groupby(["_cliente", "_grupo", "fecha"])[val_col].sum().reset_index()
    pivot = grp.set_index(["_cliente", "_grupo", "fecha"])[val_col].unstack("fecha").fillna(0)
    pivot = pivot.sort_index(axis=1)

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

    # Vectorized row build — 40-50x faster than iterrows for 6K+ clients
    # Totals from unrounded values (matching original behavior: sum then round)
    totals = {mn: round(float(pivot_reset[mn].sum()), 0) for mn in new_col_names}
    total_projected = round(float(sum(totals.values())), 0)
    # Round cells after computing totals
    pivot_reset["_lab"] = pivot_reset["Cliente"].isin(clients_with_lab).astype(int)
    for mn in new_col_names:
        pivot_reset[mn] = pivot_reset[mn].round(0)
    min_val = float(pivot_reset[new_col_names].min().min()) if new_col_names else 0
    max_val = float(pivot_reset[new_col_names].max().max()) if new_col_names else 0
    rows = [
        {**r, "Cliente": str(r["Cliente"]), "Grupo": str(r["Grupo"])}
        for r in pivot_reset[["Cliente", "Grupo", "_lab"] + new_col_names].to_dict("records")
    ]

    base_result = {
        "months": new_col_names,
        "rows": rows,
        "totals": totals,
        "min_val": min_val,
        "max_val": max_val,
        "total_projected": total_projected,
    }

    # Inject manually-added clients
    return _inject_manual_client_rows_into_table(
        base_result, user_id=user_id,
        start_date=start_date, end_date=end_date,
        neg_filter=neg, subneg_filter=subneg,
        view_money=view_money, is_admin=is_admin,
        profiles_filter=profiles,
    )


def _get_segment_color(canal_code: str) -> str:
    """Color function mapped to a professional SIEM palette."""
    c = str(canal_code).upper().strip()
    if "PROYECCIÓN TOTAL" in c:
        return "#D1E3F0"
    if "FAR" in c:
        return "#3CA9C4"  # Celeste suave
    if "DRO" in c:
        return "#26A69A"  # Verde menta
    if "IPR" in c or "SAN" in c:
        return "#5A738E"  # Azul acero
    if "IPU" in c or "PUB" in c or "LAN" in c or "PER" in c or "HOS" in c:
        return "#7057BE"  # Violeta controlado
    if "COM" in c or "PRO" in c:
        return "#64748B"  # Gris azulado
    if any(x in c for x in ("OSP", "OSU", "OES")):
        return "#14929E"  # Verde teal
    if "DPM" in c or "FIN" in c:
        return "#1E3E62"  # Azul petróleo
    if "POTENCIAL" in c or "SIN" in c:
        return "#90A4AE"  # Gris claro azulado
    return "#90A4AE"


def _blend_with_white(hex_color: str, weight: float) -> str:
    """Blend a hex color toward white by `weight` (0=original, 1=white)."""
    hex_color = hex_color.lstrip("#")
    rgb = [int(hex_color[i: i + 2], 16) for i in (0, 2, 4)]
    out = [int(c + (255 - c) * weight) for c in rgb]
    return "#{:02x}{:02x}{:02x}".format(*out)


@_with_resp_cache(ttl=_RESP_TTL_DATA)
def get_treemap_data(
    user_id: int | None = None,
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    view_money: bool = True,
    period_date: str | None = None,
    is_admin: bool = False,
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
            user_id=user_id,
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, view_money=view_money, period_date=period_date,
            is_admin=is_admin,
        )

    _ovr_active = _has_overrides(user_id, is_admin=is_admin)
    _has_manual_tm = bool(_query_manual_clients(user_id, is_admin=is_admin))
    # Use light path when no overrides/manual clients — _local_get_treemap_data_light
    # now uses _data_cache["df_valorizado"] when available (avoids parquet re-read).
    if not _ovr_active and not _has_manual_tm:
        light = _local_get_treemap_data_light(
            user_id=user_id,
            start_date=start_date,
            end_date=end_date,
            profiles=profiles,
            neg=neg,
            subneg=subneg,
            products=products,
            view_money=view_money,
            period_date=period_date,
        )
        if light:
            return light

    df_val = _get_patched_df_val(user_id=user_id, is_admin=is_admin)
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

    # ── Inject manual client entries ───────────────────────────────────────
    _manual_df_tm = _get_manual_entries_df(user_id, start_date, end_date, neg, subneg, is_admin=is_admin, profiles_filter=profiles)
    if not _manual_df_tm.empty:
        # Filter manual entries by period_date if set
        if period_date:
            _target_m = pd.to_datetime(period_date).replace(day=1)
            _manual_df_tm = _manual_df_tm[_manual_df_tm["fecha"] == _target_m]
        if not _manual_df_tm.empty:
            print(f"[MANUAL_DASHBOARD] treemap manual rows={len(_manual_df_tm)}", flush=True)
            # Add manual rows to periods list
            for _d in _manual_df_tm["fecha"].dropna().unique():
                _ds = str(_d)[:10]
                if _ds not in periods:
                    periods = sorted(periods + [_ds])
            # Align columns: fill any missing cols with None, concat shared cols
            _shared_cols = list(df_f.columns.intersection(_manual_df_tm.columns))
            _extra_manual = _manual_df_tm[[c for c in _manual_df_tm.columns if c not in _shared_cols]]
            for _c in _extra_manual.columns:
                df_f[_c] = None
            for _c in _shared_cols:
                pass  # already in both
            _manual_aligned = pd.DataFrame(index=range(len(_manual_df_tm)))
            for _c in df_f.columns:
                if _c in _manual_df_tm.columns:
                    _manual_aligned[_c] = _manual_df_tm[_c].values
                else:
                    _manual_aligned[_c] = None
            df_f = pd.concat([df_f, _manual_aligned], ignore_index=True)

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

        for grp_row in canal_groups.itertuples(index=False):
            grupo = grp_row.Grupo
            grupo_total = float(grp_row.Monto)
            grupo_parent = canal_id if grp_row.show_direct else otras_canal_id
            if grupo_parent is None:
                continue
            grupo_id = f"{canal_id}::grupo::{grupo}"
            blend_g = 0.33 if grp_row.show_direct else 0.43
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

            for cli_row in grp_clients.itertuples(index=False):
                cliente = cli_row.Cliente
                cliente_parent = grupo_id if cli_row.show_direct else otras_cli_id
                if cliente_parent is None:
                    continue
                cliente_node_id = f"{grupo_id}::cliente::{cliente}"
                tone = 0.56 - min(float(cli_row.share_grupo) * 0.35, 0.22)
                add_node(cliente_node_id, str(cliente), cliente_parent, float(cli_row.Monto), _blend_with_white(base_color, max(0.10, tone)))

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
    user_id: int | None = None,
    client_id: str = "",
    start_date: str | None = None,
    end_date: str | None = None,
    profiles: list | None = None,
    neg: list | None = None,
    subneg: list | None = None,
    products: list | None = None,
    growth_pct: float = 0.0,
    is_admin: bool = False,
) -> dict:
    """Return per-product detail for a client, pivoted by month, grouped by neg/subneg.
    Used by the modal edit dialog.
    Always uses the RAW (unpatched) df_valorizado so that 'orig' = CSV baseline.
    Saved % overrides are injected directly into months_data so the modal shows them.
    """
    import pandas as pd

    # Check if this is a manually-added client first (both SQLite and PG paths)
    _manual = _manual_client_by_name(client_id, user_id)
    _manual_to_merge = None  # Set when a base client has manual additions to merge

    if _manual is not None:
        # PG: manual clients are always purely manual — route to manual detail
        if engine is not None and "postgresql" in str(engine.url):
            return get_manual_client_detail(
                _manual, user_id=user_id,
                start_date=start_date, end_date=end_date,
                neg_filter=neg, subneg_filter=subneg,
                growth_pct=growth_pct, is_admin=is_admin,
            )
        # SQLite: check if client also exists in base data
        _chk = get_data().get("df_valorizado", pd.DataFrame())
        _chk_col = next((c for c in ("fantasia", "cliente_id") if c in _chk.columns), None)
        _in_base = bool(_chk_col and not _chk.empty and (_chk[_chk_col] == client_id).any())
        if not _in_base:
            # Purely manual client → route entirely to manual detail
            return get_manual_client_detail(
                _manual, user_id=user_id,
                start_date=start_date, end_date=end_date,
                neg_filter=neg, subneg_filter=subneg,
                growth_pct=growth_pct, is_admin=is_admin,
            )
        # Base client with manual additions → continue with base flow, merge later
        _manual_to_merge = _manual

    if engine is not None and "postgresql" in str(engine.url):
        return _pg_get_client_detail(
            user_id=user_id,
            client_id=client_id,
            start_date=start_date, end_date=end_date,
            profiles=profiles, neg=neg, subneg=subneg,
            products=products, growth_pct=growth_pct,
            is_admin=is_admin,
        )

    data = get_data()
    df_val = data.get("df_valorizado", pd.DataFrame()).copy()
    df_main = data.get("df_main", pd.DataFrame())
    price_lookup = data.get("price_lookup", {})
    # Load any previously saved % overrides for this client
    saved_overrides = _get_client_overrides_snapshot(user_id=user_id, client_id=client_id, growth_pct=growth_pct, is_admin=is_admin)
    saved_subneg_growths = _get_client_subneg_growths(user_id, client_id, is_admin=is_admin)
    # Build full maps to get effective_from_month per override
    _loc_records = _fetch_override_records(user_id, client_selector=client_id, all_users=is_admin)
    _loc_maps = _build_override_maps(_loc_records)
    _loc_client_key = _clean_override_text(client_id)
    _loc_cell_efm: dict[tuple[str, str], str | None] = {
        (codigo, month): payload.get("effective_from_month")
        for (selector, codigo, month), payload in _loc_maps["cell"].items()
        if selector == _loc_client_key
    }
    _loc_subneg_efm = dict(_loc_maps.get("subneg_effective_months", {}).get(_loc_client_key, {}))
    effective_from_month = get_forecast_effective_month()

    if df_val.empty:
        if _manual_to_merge is not None:
            return get_manual_client_detail(
                _manual_to_merge, user_id=user_id, start_date=start_date, end_date=end_date,
                neg_filter=neg, subneg_filter=subneg, growth_pct=growth_pct, is_admin=is_admin,
            )
        return {"client_id": client_id, "perfil": "", "negocios": [], "dates": []}

    # Filter by client
    if "fantasia" in df_val.columns:
        mask_cli = df_val["fantasia"] == client_id
    elif "cliente_id" in df_val.columns:
        mask_cli = df_val["cliente_id"] == client_id
    else:
        if _manual_to_merge is not None:
            return get_manual_client_detail(
                _manual_to_merge, user_id=user_id, start_date=start_date, end_date=end_date,
                neg_filter=neg, subneg_filter=subneg, growth_pct=growth_pct, is_admin=is_admin,
            )
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
        if _manual_to_merge is not None:
            return get_manual_client_detail(
                _manual_to_merge, user_id=user_id, start_date=start_date, end_date=end_date,
                neg_filter=neg, subneg_filter=subneg, growth_pct=growth_pct, is_admin=is_admin,
            )
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
                    # Saved cell override: respect its own effective_from_month
                    saved_pct = saved_overrides.get((art, ds), None)
                    if saved_pct is not None:
                        cell_efm = _loc_cell_efm.get((art, ds))
                        if cell_efm is None or ds >= cell_efm:
                            pct = saved_pct
                            adj = orig * (1.0 + pct / 100.0)
                        else:
                            saved_pct = None  # blocked — treat as no override
                    if saved_pct is None and max_hist_date and d > max_hist_date and growth_pct != 0:
                        months_diff = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                        t = months_diff
                        ra = growth_pct / 100.0
                        rm = (1 + ra) ** (1 / 12.0) - 1 if growth_pct != 0 else 0.0
                        factor = (1 + rm) ** t if growth_pct != 0 else 1.0
                        adj = orig * factor
                        pct = round(rm * 100, 4)
                    if (
                        saved_pct is None
                        and max_hist_date
                        and d > max_hist_date
                        and str(subneg_name) in saved_subneg_growths
                    ):
                        # Subneg override: apply only from its effective_from_month onwards
                        subneg_efm = _loc_subneg_efm.get(str(subneg_name))
                        if subneg_efm is None or ds >= subneg_efm:
                            scoped_growth_pct = float(saved_subneg_growths.get(str(subneg_name)) or 0.0)
                            if scoped_growth_pct != 0:
                                months_diff = (d.year - max_hist_date.year) * 12 + (d.month - max_hist_date.month)
                                rm = (1 + scoped_growth_pct / 100.0) ** (1 / 12.0) - 1
                                factor = (1 + rm) ** months_diff
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

    # Merge manually-added articles (base client that also has manual additions)
    if _manual_to_merge is not None:
        _man_detail = get_manual_client_detail(
            _manual_to_merge, user_id=user_id,
            start_date=start_date, end_date=end_date,
            neg_filter=neg, subneg_filter=subneg,
            growth_pct=growth_pct, is_admin=is_admin,
        )
        _merge_manual_into_base_negocios(negocios_out, _man_detail.get("negocios", []))

    _client_growth_state = _derive_visible_client_growth_state(
        negocios_out, saved_subneg_growths, growth_pct
    )
    logger.debug(
        "[FORECAST client-detail growth] client=%s base=%s client_growth=%s subneg_growths=%s",
        client_id,
        growth_pct,
        _client_growth_state,
        saved_subneg_growths,
    )
    _result = {
        "client_id": client_id,
        "perfil": perfil,
        "neg": neg_val,
        "negocios": negocios_out,
        "dates": date_strs,
        "max_hist_date": max_hist_date.strftime("%Y-%m") if max_hist_date else None,
        "growth_pct": growth_pct,
        "client_growth_pct": _client_growth_state["value"],
        "client_growth_source": _client_growth_state["source"],
        "client_growth_mixed": _client_growth_state["mixed"],
        "subneg_growths": saved_subneg_growths,
        "effective_from_month": effective_from_month,
    }
    if _manual_to_merge is not None:
        _result["manual_client_id"] = _manual_to_merge.id
        _result["is_manual"] = True
    return _result


# ---------------------------------------------------------------------------
# Manual-client helpers (Forecast > Agregar cliente)
# ---------------------------------------------------------------------------

def _query_manual_clients(user_id, is_admin=False):
    """Return list of active ForecastManualClient rows for a user."""
    if SessionLocal is None or ForecastManualClient is None:
        return []
    session = SessionLocal()
    try:
        q = session.query(ForecastManualClient).filter(ForecastManualClient.is_active == True)
        if not is_admin and user_id is not None:
            q = q.filter(ForecastManualClient.user_id == user_id)
        return q.all()
    except Exception as exc:
        logger.warning("[FORECAST manual] _query_manual_clients error: %s", exc)
        return []
    finally:
        session.close()


def _query_manual_entries(client_ids):
    """Return active ForecastManualEntry rows for a list of client ids."""
    if SessionLocal is None or ForecastManualEntry is None or not client_ids:
        return []
    session = SessionLocal()
    try:
        q = session.query(ForecastManualEntry).filter(
            ForecastManualEntry.client_id.in_(client_ids)
        )
        # Filter active entries if the column exists (migration may not have run yet)
        try:
            q = q.filter(ForecastManualEntry.is_active == True)
        except Exception:
            pass
        return q.all()
    except Exception as exc:
        logger.warning("[FORECAST manual] _query_manual_entries error: %s", exc)
        return []
    finally:
        session.close()


def _manual_client_by_name(client_id_str, user_id):
    """Return ForecastManualClient if client_id_str matches an active manual client."""
    if SessionLocal is None or ForecastManualClient is None:
        return None
    session = SessionLocal()
    try:
        return session.query(ForecastManualClient).filter(
            ForecastManualClient.nombre_cliente == client_id_str,
            ForecastManualClient.user_id == user_id,
            ForecastManualClient.is_active == True,
        ).first()
    except Exception as exc:
        logger.warning("[FORECAST manual] _manual_client_by_name error: %s", exc)
        return None
    finally:
        session.close()


def _get_manual_entries_df(
    user_id,
    start_date=None,
    end_date=None,
    neg_filter=None,
    subneg_filter=None,
    is_admin=False,
    profiles_filter=None,
):
    """Return manual entries as a DataFrame compatible with df_val for dashboard injection."""
    import pandas as pd
    clients = _query_manual_clients(user_id, is_admin=is_admin)
    if not clients:
        return pd.DataFrame()
    client_ids = [c.id for c in clients]
    all_entries = _query_manual_entries(client_ids)
    if not all_entries:
        return pd.DataFrame()
    client_map = {c.id: c for c in clients}
    start_p = pd.to_datetime(start_date).to_period("M") if start_date else None
    end_p   = pd.to_datetime(end_date).to_period("M") if end_date else None
    neg_set     = set(_norm_filter_list(neg_filter))
    subneg_set  = set(_norm_filter_list(subneg_filter))
    profiles_set = set(_norm_filter_list(profiles_filter))
    rows = []
    for e in all_entries:
        if neg_set and str(e.neg or "").strip() not in neg_set:
            continue
        if subneg_set and str(e.subneg or "").strip() not in subneg_set:
            continue
        _ep = str(getattr(e, "perfil", None) or "").strip() or "SIN PERFIL"
        if profiles_set and _ep not in profiles_set:
            continue
        try:
            ep = pd.Period(e.forecast_month, freq="M")
            if start_p and ep < start_p:
                continue
            if end_p and ep > end_p:
                continue
        except Exception:
            continue
        mc = client_map.get(e.client_id)
        if mc is None:
            continue
        _perfil_val = str(getattr(e, "perfil", None) or "").strip() or "SIN PERFIL"
        rows.append({
            "fecha": pd.Timestamp(e.forecast_month + "-01"),
            "fantasia": mc.nombre_cliente,
            "nombre_grupo": mc.grupo or mc.nombre_cliente,
            "neg": str(e.neg or "").strip(),
            "subneg": str(e.subneg or "").strip(),
            "perfil": _perfil_val,
            "origen_manual": True,
            "codigo_serie": str(e.codigo_serie or "").strip(),
            "descripcion": str(e.descripcion or e.codigo_serie or "").strip(),
            "monto_yhat": float(e.monto_total or 0.0),
            "yhat_cliente": float(e.cantidad or 0.0),
        })
    if not rows:
        return pd.DataFrame()
    df = pd.DataFrame(rows)
    print(f"[MANUAL_DASHBOARD] manual entries loaded={len(df)} user_id={user_id}", flush=True)
    return df


def _inject_manual_client_rows_into_table(
    result,
    user_id,
    start_date=None,
    end_date=None,
    neg_filter=None,
    subneg_filter=None,
    view_money=True,
    is_admin=False,
    profiles_filter=None,
):
    """Inject manual-client rows into an existing get_client_table() result dict."""
    import pandas as pd
    from collections import defaultdict
    try:
        months = result.get("months", [])
        if not months:
            print("[MANUAL_CLIENT INJECT] No months in base result — skip", flush=True)
            return result

        clients = _query_manual_clients(user_id, is_admin=is_admin)
        print(f"[MANUAL_CLIENT QUERY] clients_found={len(clients)} user_id={user_id}", flush=True)
        if not clients:
            return result

        client_ids = [c.id for c in clients]
        all_entries = _query_manual_entries(client_ids)
        print(f"[MANUAL_CLIENT QUERY] entries_found={len(all_entries)} client_ids={client_ids}", flush=True)
        if not all_entries:
            return result

        # Build YYYY-MM → display name mapping robustly.
        # Generate display names from YYYY-MM using same strftime as get_client_table
        # and match against the existing months list.
        months_set = set(months)
        _month_name_cache: dict = {}
        # First pass: generate from a wide range of months and check against the existing list
        for yr in range(2024, 2030):
            for mo in range(1, 13):
                ym = f"{yr}-{mo:02d}"
                try:
                    dn = pd.to_datetime(f"{ym}-01").strftime("%b %Y").title()
                    if dn in months_set:
                        _month_name_cache[ym] = dn
                except Exception:
                    pass
        # Second pass: also try parsing existing month display names (fallback)
        for mn in months:
            if any(v == mn for v in _month_name_cache.values()):
                continue
            try:
                d = pd.to_datetime(mn)
                ym = d.strftime("%Y-%m")
                if ym not in _month_name_cache:
                    _month_name_cache[ym] = mn
            except Exception:
                pass

        print(f"[MANUAL_CLIENT INJECT] month_cache={_month_name_cache}", flush=True)

        start_ts = pd.to_datetime(start_date).to_period("M") if start_date else None
        end_ts   = pd.to_datetime(end_date).to_period("M") if end_date else None
        neg_set      = set(_norm_filter_list(neg_filter))
        subneg_set   = set(_norm_filter_list(subneg_filter))
        profiles_set = set(_norm_filter_list(profiles_filter))
        print(f"[MANUAL_CLIENT FILTER] neg_filter={neg_filter} subneg_filter={subneg_filter} profiles_filter={profiles_filter}", flush=True)

        client_map = {c.id: c for c in clients}
        monthly_by_client = defaultdict(lambda: defaultdict(float))
        kept = 0
        discarded = 0

        for entry in all_entries:
            _reason = None
            _entry_perfil = str(getattr(entry, "perfil", None) or "").strip() or "SIN PERFIL"
            if profiles_set and _entry_perfil not in profiles_set:
                _reason = f"perfil={_entry_perfil!r} not in {profiles_set}"
            elif neg_set and str(entry.neg or "").strip() not in neg_set:
                _reason = f"neg={entry.neg!r} not in {neg_set}"
            elif subneg_set and str(entry.subneg or "").strip() not in subneg_set:
                _reason = f"subneg={entry.subneg!r} not in {subneg_set}"
            else:
                try:
                    ep = pd.Period(entry.forecast_month, freq="M")
                    if start_ts and ep < start_ts:
                        _reason = f"month {entry.forecast_month} < start {start_ts}"
                    elif end_ts and ep > end_ts:
                        _reason = f"month {entry.forecast_month} > end {end_ts}"
                except Exception:
                    _reason = f"invalid month {entry.forecast_month!r}"

            if _reason:
                discarded += 1
                print(f"[MANUAL_CLIENT FILTER] DISCARD entry id={entry.id} art={entry.codigo_serie!r} reason={_reason}", flush=True)
                continue

            display_name = _month_name_cache.get(entry.forecast_month)
            if display_name is None:
                try:
                    d = pd.to_datetime(entry.forecast_month + "-01")
                    display_name = d.strftime("%b %Y").title()
                except Exception:
                    print(f"[MANUAL_CLIENT FILTER] DISCARD entry id={entry.id} cannot parse month={entry.forecast_month!r}", flush=True)
                    discarded += 1
                    continue
                if display_name not in months:
                    print(f"[MANUAL_CLIENT FILTER] DISCARD entry id={entry.id} display={display_name!r} not in months", flush=True)
                    discarded += 1
                    continue
                _month_name_cache[entry.forecast_month] = display_name

            value = float(entry.monto_total if view_money else entry.cantidad)
            monthly_by_client[entry.client_id][display_name] += value
            kept += 1

        print(f"[MANUAL_CLIENT FILTER] kept={kept} discarded={discarded}", flush=True)

        if not monthly_by_client:
            print("[MANUAL_CLIENT INJECT] monthly_by_client empty — nothing to inject", flush=True)
            return result

        month_name_set = set(months)
        existing_clients = {r.get("Cliente", "") for r in result.get("rows", [])}
        new_rows = []
        totals = dict(result.get("totals", {}))

        for cid, month_vals in monthly_by_client.items():
            mc = client_map.get(cid)
            if mc is None:
                continue
            if mc.nombre_cliente in existing_clients:
                continue

            row = {
                "Cliente": mc.nombre_cliente,
                "Grupo": mc.grupo or "",
                "_lab": 0,
                "_is_manual": True,
                "_manual_client_id": mc.id,
            }
            for mn in months:
                v = round(float(month_vals.get(mn, 0.0)), 0)
                row[mn] = v
                totals[mn] = round(float(totals.get(mn, 0.0)) + v, 0)
            new_rows.append(row)

        before_rows = len(result.get("rows", []))
        if not new_rows:
            print(f"[MANUAL_CLIENT INJECT] No new_rows to inject (before={before_rows})", flush=True)
            return result

        rows = list(result.get("rows", []))
        rows.extend(new_rows)

        all_vals = [v for r in rows for k, v in r.items() if k in month_name_set and isinstance(v, (int, float))]
        min_val = float(min(all_vals)) if all_vals else 0.0
        max_val = float(max(all_vals)) if all_vals else 0.0
        total_projected = round(float(sum(totals.values())), 0)

        print(f"[MANUAL_CLIENT INJECT] before_rows={before_rows} manual_rows={len(new_rows)} after_rows={len(rows)}", flush=True)
        return {
            **result,
            "rows": rows,
            "totals": totals,
            "min_val": min_val,
            "max_val": max_val,
            "total_projected": total_projected,
        }
    except Exception as exc:
        logger.error("[FORECAST manual] _inject_manual_client_rows_into_table error: %s", exc, exc_info=True)
        print(f"[MANUAL_CLIENT INJECT] Exception: {exc}", flush=True)
        return result


def get_manual_client_detail(
    manual_client,
    user_id,
    start_date=None,
    end_date=None,
    neg_filter=None,
    subneg_filter=None,
    growth_pct=0.0,
    is_admin=False,
):
    """Return a get_client_detail()-compatible response for a manual client."""
    import pandas as pd
    from collections import defaultdict
    effective_from_month = get_forecast_effective_month()

    if SessionLocal is None or ForecastManualEntry is None:
        return {
            "client_id": manual_client.nombre_cliente, "perfil": "", "negocios": [], "dates": [],
            "is_manual": True, "growth_pct": 0, "client_growth_pct": 0,
            "client_growth_source": None, "client_growth_mixed": False,
            "subneg_growths": {}, "effective_from_month": effective_from_month, "max_hist_date": None,
        }

    session = SessionLocal()
    try:
        q = session.query(ForecastManualEntry).filter(
            ForecastManualEntry.client_id == manual_client.id
        )
        try:
            q = q.filter(ForecastManualEntry.is_active == True)
        except Exception:
            pass
        entries = q.all()
    finally:
        session.close()

    saved_overrides = _get_client_overrides_snapshot(
        user_id=user_id, client_id=manual_client.nombre_cliente, growth_pct=0.0, is_admin=is_admin
    )
    saved_subneg_growths = _get_client_subneg_growths(
        user_id, manual_client.nombre_cliente, is_admin=is_admin
    )

    _any_perfil = str(getattr(entries[0], "perfil", None) or "").strip() if entries else ""

    if not entries:
        # Still build dates from range so the modal shows full timeline
        _empty_dates: list = []
        if start_date and end_date:
            try:
                _empty_dates = [
                    str(p)
                    for p in pd.period_range(
                        start=pd.to_datetime(start_date).to_period("M"),
                        end=pd.to_datetime(end_date).to_period("M"),
                        freq="M",
                    )
                ]
            except Exception:
                pass
        return {
            "client_id": manual_client.nombre_cliente,
            "manual_client_id": manual_client.id,
            "perfil": _any_perfil, "neg": "",
            "negocios": [], "dates": _empty_dates, "growth_pct": growth_pct,
            "client_growth_pct": 0, "client_growth_source": None, "client_growth_mixed": False,
            "subneg_growths": saved_subneg_growths,
            "effective_from_month": effective_from_month, "max_hist_date": None, "is_manual": True,
        }

    start_p = pd.to_datetime(start_date).to_period("M") if start_date else None
    end_p   = pd.to_datetime(end_date).to_period("M") if end_date else None
    neg_set    = set(_norm_filter_list(neg_filter))
    subneg_set = set(_norm_filter_list(subneg_filter))

    filtered = []
    for e in entries:
        if neg_set and str(e.neg or "").strip() not in neg_set:
            continue
        if subneg_set and str(e.subneg or "").strip() not in subneg_set:
            continue
        try:
            ep = pd.Period(e.forecast_month, freq="M")
            if start_p and ep < start_p:
                continue
            if end_p and ep > end_p:
                continue
        except Exception:
            continue
        filtered.append(e)

    if not filtered:
        filtered = list(entries)

    # Build date_strs from the full start_date/end_date range so the modal
    # always shows ALL months in the period, not just months with entries.
    date_strs: list = []
    if start_date and end_date:
        try:
            date_strs = [
                str(p)
                for p in pd.period_range(
                    start=pd.to_datetime(start_date).to_period("M"),
                    end=pd.to_datetime(end_date).to_period("M"),
                    freq="M",
                )
            ]
        except Exception:
            pass
    if not date_strs:
        # Fallback: only months that have entries
        date_strs = sorted({e.forecast_month for e in filtered})

    neg_sub_art = defaultdict(lambda: defaultdict(dict))

    for e in filtered:
        neg_name = str(e.neg or "Sin Negocio").strip()
        sub_name = str(e.subneg or "General").strip()
        art_key  = str(e.codigo_serie).strip()
        desc     = str(e.descripcion or e.codigo_serie).strip()
        um       = str(e.unidad_medida or "Unid.").strip()
        costo    = float(e.costo_unitario or 0.0)
        cantidad = float(e.cantidad or 0.0)
        monto    = float(e.monto_total or 0.0)

        if art_key not in neg_sub_art[neg_name][sub_name]:
            neg_sub_art[neg_name][sub_name][art_key] = {
                "articulo": art_key, "descripcion": desc, "unidad_medida": um,
                "nivel_agregacion": "ARTICULO", "precio": costo,
                "months": {},
                "entries": [],  # list of {entry_id, forecast_month} for admin delete
            }
        neg_sub_art[neg_name][sub_name][art_key]["months"][e.forecast_month] = {
            "cantidad": cantidad, "costo_unitario": costo, "monto_total": monto,
            "entry_id": e.id,
        }
        neg_sub_art[neg_name][sub_name][art_key]["entries"].append(
            {"entry_id": e.id, "forecast_month": e.forecast_month, "cantidad": cantidad, "monto_total": monto}
        )

    negocios_out = []
    for neg_name, subs in neg_sub_art.items():
        subnegs_out = []
        for sub_name, arts in subs.items():
            products_out = []
            for art_key, art_data in arts.items():
                months_data = {}
                for ds in date_strs:
                    m = art_data["months"].get(ds, {})
                    orig     = float(m.get("cantidad", 0.0))
                    costo_u  = float(m.get("costo_unitario", float(art_data["precio"])))
                    monto_base = float(m.get("monto_total", 0.0))
                    pct = 0.0
                    adj = orig
                    saved_pct = saved_overrides.get((art_key, ds), None)
                    if saved_pct is not None:
                        pct = saved_pct
                        adj = orig * (1.0 + pct / 100.0)
                    elif str(sub_name) in saved_subneg_growths and saved_subneg_growths[str(sub_name)]:
                        scoped = float(saved_subneg_growths[str(sub_name)] or 0.0)
                        if scoped != 0:
                            rm = (1 + scoped / 100.0) ** (1 / 12.0) - 1
                            adj = orig * (1 + rm)
                            pct = round(rm * 100, 4)
                    money = round(adj * costo_u, 0) if costo_u else round(monto_base * (1 + pct / 100.0), 0)
                    months_data[ds] = {
                        "orig": round(orig, 2), "pct": round(pct, 4),
                        "nuevo": round(adj, 2), "money": money,
                    }
                total_nuevo = sum(v["nuevo"] for v in months_data.values())
                total_money = round(sum(v["money"] for v in months_data.values()), 0)
                products_out.append({
                    "articulo": art_data["articulo"],
                    "descripcion": art_data["descripcion"],
                    "unidad_medida": art_data["unidad_medida"],
                    "nivel_agregacion": art_data["nivel_agregacion"],
                    "precio": round(float(art_data["precio"]), 2),
                    "total_nuevo": round(total_nuevo, 2),
                    "total_money": total_money,
                    "months": months_data,
                    "entries": art_data.get("entries", []),
                })
            products_out.sort(key=lambda x: x["total_money"], reverse=True)
            subnegs_out.append({"subneg": sub_name, "products": products_out})
        negocios_out.append({"neg": neg_name, "subnegs": subnegs_out})

    first_neg = negocios_out[0]["neg"] if negocios_out else ""
    _first_perfil = str(getattr(filtered[0], "perfil", None) or "").strip() if filtered else _any_perfil
    return {
        "client_id": manual_client.nombre_cliente,
        "manual_client_id": manual_client.id,
        "perfil": _first_perfil, "neg": first_neg,
        "negocios": negocios_out, "dates": date_strs,
        "growth_pct": growth_pct, "client_growth_pct": 0,
        "client_growth_source": None, "client_growth_mixed": False,
        "subneg_growths": saved_subneg_growths,
        "effective_from_month": effective_from_month,
        "max_hist_date": None,
        "is_manual": True,
    }


def add_articles_to_manual_client(user_id, manual_client_id, entries):
    """Append new article-month entries to an existing manual client."""
    if SessionLocal is None or ForecastManualClient is None or ForecastManualEntry is None:
        raise RuntimeError("SessionLocal not available")

    session = SessionLocal()
    try:
        mc = session.query(ForecastManualClient).filter(
            ForecastManualClient.id == manual_client_id,
            ForecastManualClient.is_active == True,
        ).first()
        if mc is None:
            raise ValueError(f"Cliente manual id={manual_client_id} no encontrado o inactivo")

        # Use the perfil from the first existing active entry for this client (if not provided)
        existing_entry = session.query(ForecastManualEntry).filter(
            ForecastManualEntry.client_id == manual_client_id,
            ForecastManualEntry.is_active == True,
        ).first()
        fallback_perfil = str(getattr(existing_entry, "perfil", None) or "").strip() if existing_entry else ""

        inserted = 0
        for e in entries:
            cantidad = float(e.get("cantidad", 0) or 0)
            costo_u  = float(e.get("costo_unitario", 0) or 0)
            monto    = float(e.get("monto_total", 0) or 0) or round(cantidad * costo_u, 2)
            perfil_v = str(e.get("perfil") or fallback_perfil or "").strip() or None
            entry = ForecastManualEntry(
                client_id=mc.id,
                perfil=perfil_v,
                neg=str(e.get("neg", "") or "").strip(),
                subneg=str(e.get("subneg", "") or "").strip(),
                codigo_serie=str(e.get("codigo_serie", "") or "").strip(),
                descripcion=str(e.get("descripcion", "") or "").strip(),
                unidad_medida=str(e.get("unidad_medida", "Unid.") or "Unid.").strip(),
                forecast_month=str(e.get("forecast_month", "") or "").strip(),
                cantidad=cantidad,
                costo_unitario=costo_u,
                monto_total=monto,
            )
            session.add(entry)
            inserted += 1

        session.commit()
        print(f"[MANUAL_CLIENT ADD_ARTICLES] client_id={manual_client_id} inserted={inserted}", flush=True)
        logger.info("[FORECAST manual] add_articles client_id=%s inserted=%d", manual_client_id, inserted)
        return {"ok": True, "manual_client_id": manual_client_id, "inserted": inserted}
    except Exception as exc:
        session.rollback()
        logger.error("[FORECAST manual] add_articles_to_manual_client error: %s", exc, exc_info=True)
        raise
    finally:
        session.close()


def add_articles_by_client_name(user_id, created_by, client_name, perfil, entries):
    """Add article entries to a client by name.

    If a manual client with that name already exists for this user, entries are
    appended to it.  Otherwise a new ForecastManualClient is created on the fly.
    This is the unified endpoint for both base-dataset clients and manual clients.
    """
    if SessionLocal is None or ForecastManualClient is None or ForecastManualEntry is None:
        raise RuntimeError("SessionLocal not available")

    session = SessionLocal()
    try:
        # Look for an existing active manual client with this exact name
        mc = session.query(ForecastManualClient).filter(
            ForecastManualClient.user_id == user_id,
            ForecastManualClient.nombre_cliente == client_name.strip(),
            ForecastManualClient.is_active == True,
        ).first()

        if mc is None:
            # Create it on the fly — this happens for base-dataset clients
            mc = ForecastManualClient(
                user_id=user_id,
                nombre_cliente=client_name.strip(),
                grupo=None,
                created_by=created_by,
                is_active=True,
            )
            session.add(mc)
            session.flush()
            print(f"[ADD_ARTICLES_BY_NAME] created new manual client id={mc.id} name={client_name!r}", flush=True)
        else:
            print(f"[ADD_ARTICLES_BY_NAME] found existing manual client id={mc.id} name={client_name!r}", flush=True)

        inserted = 0
        for e in entries:
            cantidad = float(e.get("cantidad", 0) or 0)
            costo_u  = float(e.get("costo_unitario", 0) or 0)
            monto    = float(e.get("monto_total", 0) or 0) or round(cantidad * costo_u, 2)
            perfil_v = str(e.get("perfil") or perfil or "").strip() or None
            entry = ForecastManualEntry(
                client_id=mc.id,
                perfil=perfil_v,
                neg=str(e.get("neg", "") or "").strip(),
                subneg=str(e.get("subneg", "") or "").strip(),
                codigo_serie=str(e.get("codigo_serie", "") or "").strip(),
                descripcion=str(e.get("descripcion", "") or "").strip(),
                unidad_medida=str(e.get("unidad_medida", "Unid.") or "Unid.").strip(),
                forecast_month=str(e.get("forecast_month", "") or "").strip(),
                cantidad=cantidad,
                costo_unitario=costo_u,
                monto_total=monto,
            )
            session.add(entry)
            inserted += 1

        session.commit()
        print(f"[ADD_ARTICLES_BY_NAME] manual_client_id={mc.id} inserted={inserted}", flush=True)
        logger.info("[FORECAST manual] add_articles_by_name client=%r id=%s inserted=%d", client_name, mc.id, inserted)
        return {"ok": True, "manual_client_id": mc.id, "client_name": client_name, "inserted": inserted}
    except Exception as exc:
        session.rollback()
        logger.error("[FORECAST manual] add_articles_by_client_name error: %s", exc, exc_info=True)
        raise
    finally:
        session.close()


def delete_manual_client(user_id, manual_client_id, deleted_by):
    """Logical delete of a manual forecast client (is_active=False)."""
    if SessionLocal is None or ForecastManualClient is None:
        raise RuntimeError("SessionLocal not available")
    import datetime as _dt
    session = SessionLocal()
    try:
        mc = session.query(ForecastManualClient).filter(
            ForecastManualClient.id == manual_client_id,
            ForecastManualClient.is_active == True,
        ).first()
        if mc is None:
            return {"ok": False, "error": "Cliente manual no encontrado o ya eliminado"}
        mc.is_active = False
        mc.deleted_at = _dt.datetime.utcnow()
        mc.deleted_by = deleted_by
        session.commit()
        print(f"[MANUAL_CLIENT DELETE] client_id={manual_client_id} by={deleted_by}", flush=True)
        return {"ok": True, "client_id": manual_client_id}
    except Exception as exc:
        session.rollback()
        logger.error("[FORECAST manual] delete_manual_client error: %s", exc, exc_info=True)
        raise
    finally:
        session.close()


def delete_manual_entry(user_id, manual_entry_id, deleted_by):
    """Logical delete of a single manual forecast entry row."""
    if SessionLocal is None or ForecastManualEntry is None:
        raise RuntimeError("SessionLocal not available")
    import datetime as _dt
    session = SessionLocal()
    try:
        entry = session.query(ForecastManualEntry).filter(
            ForecastManualEntry.id == manual_entry_id,
        ).first()
        if entry is None:
            return {"ok": False, "error": "Entrada no encontrada"}
        entry.is_active = False
        entry.deleted_at = _dt.datetime.utcnow()
        entry.deleted_by = deleted_by
        session.commit()
        print(f"[MANUAL_ENTRY DELETE] entry_id={manual_entry_id} client_id={entry.client_id} by={deleted_by}", flush=True)
        return {"ok": True, "entry_id": manual_entry_id, "client_id": entry.client_id}
    except Exception as exc:
        session.rollback()
        logger.error("[FORECAST manual] delete_manual_entry error: %s", exc, exc_info=True)
        raise
    finally:
        session.close()


def _merge_manual_into_base_negocios(base_negocios: list, manual_negocios: list) -> None:
    """Merge manual-entry products into base negocios list in-place.

    Products from manual_negocios are appended to the matching neg/subneg group in
    base_negocios (or added as a new group if not present).  Articles that already
    exist in a subneg are skipped to avoid duplicates.
    """
    neg_idx: dict[str, int] = {g["neg"]: i for i, g in enumerate(base_negocios)}
    for m_neg in manual_negocios:
        neg_name = m_neg["neg"]
        if neg_name in neg_idx:
            base_neg = base_negocios[neg_idx[neg_name]]
            sub_idx: dict[str, int] = {s["subneg"]: j for j, s in enumerate(base_neg["subnegs"])}
            for m_sub in m_neg.get("subnegs", []):
                sub_name = m_sub["subneg"]
                if sub_name in sub_idx:
                    base_sub = base_neg["subnegs"][sub_idx[sub_name]]
                    existing_arts = {p["articulo"] for p in base_sub["products"]}
                    for m_prod in m_sub.get("products", []):
                        if m_prod["articulo"] not in existing_arts:
                            base_sub["products"].append(m_prod)
                else:
                    base_neg["subnegs"].append(m_sub)
        else:
            base_negocios.append(m_neg)


def get_new_client_catalog(user_id=None):
    """Return catalog for the new-client form: negocios, subnegocios, articulos, grupos."""
    import pandas as pd
    result = {"negocios": [], "subnegocios": [], "articulos": [], "grupos": []}

    try:
        if FORECAST_FILE.exists():
            cols_needed = {"neg", "subneg", "codigo_serie", "descripcion", "unidad_medida"}
            df_base = pd.read_csv(
                str(FORECAST_FILE), sep=";", decimal=",", encoding="utf-8-sig", low_memory=False,
                usecols=lambda c: str(c).strip().lower() in cols_needed,
            )
            df_base.columns = [c.lower().strip() for c in df_base.columns]
            df_base = df_base.dropna(subset=["codigo_serie"])
            # Translate numeric neg/subneg IDs → text names (same as _load_all_data)
            df_base = _apply_neg_names(df_base, NEGOCIOS_FILE)

            if "neg" in df_base.columns:
                result["negocios"] = sorted({str(v).strip() for v in df_base["neg"].dropna() if str(v).strip()})

            if "subneg" in df_base.columns and "neg" in df_base.columns:
                sub_df = df_base[["neg", "subneg"]].drop_duplicates().dropna()
                result["subnegocios"] = [
                    {"neg": str(r["neg"]).strip(), "subneg": str(r["subneg"]).strip()}
                    for _, r in sub_df.iterrows()
                    if str(r["neg"]).strip() and str(r["subneg"]).strip()
                ]

            art_cols_avail = [c for c in ["codigo_serie", "descripcion", "neg", "subneg", "unidad_medida"] if c in df_base.columns]
            art_df = df_base[art_cols_avail].drop_duplicates(subset=["codigo_serie"]).dropna(subset=["codigo_serie"])
            result["articulos"] = [
                {
                    "codigo_serie": str(row.get("codigo_serie", "")).strip(),
                    "descripcion": str(row.get("descripcion", row.get("codigo_serie", ""))).strip(),
                    "neg": str(row.get("neg", "")).strip(),
                    "subneg": str(row.get("subneg", "")).strip(),
                    "unidad_medida": str(row.get("unidad_medida", "Unid.")).strip(),
                }
                for _, row in art_df.iterrows()
                if str(row.get("codigo_serie", "")).strip()
            ]
    except Exception as exc:
        logger.warning("[FORECAST manual] get_new_client_catalog CSV error: %s", exc)

    if not result["articulos"] and _VALORIZADO_PARQUET.exists():
        try:
            df_v = pd.read_parquet(str(_VALORIZADO_PARQUET), columns=["codigo_serie", "descripcion", "neg", "subneg"])
            art_df2 = df_v.drop_duplicates(subset=["codigo_serie"]).dropna(subset=["codigo_serie"])
            result["articulos"] = [
                {
                    "codigo_serie": str(row.get("codigo_serie", "")).strip(),
                    "descripcion": str(row.get("descripcion", "")).strip(),
                    "neg": str(row.get("neg", "")).strip(),
                    "subneg": str(row.get("subneg", "")).strip(),
                    "unidad_medida": "Unid.",
                }
                for _, row in art_df2.iterrows()
                if str(row.get("codigo_serie", "")).strip()
            ]
        except Exception as exc2:
            logger.warning("[FORECAST manual] get_new_client_catalog parquet error: %s", exc2)

    grupos = set()
    try:
        if CLIENTES_FILE.exists():
            df_cli = pd.read_csv(str(CLIENTES_FILE), encoding="latin-1", low_memory=False)
            df_cli.columns = [c.lower().strip() for c in df_cli.columns]
            if "nombre_grupo" in df_cli.columns:
                for g in df_cli["nombre_grupo"].dropna().unique():
                    gs = str(g).strip()
                    if gs and gs.upper() not in ("SIN GRUPO", "NAN", "NONE", ""):
                        grupos.add(gs)
    except Exception as exc:
        logger.warning("[FORECAST manual] get_new_client_catalog grupos CSV error: %s", exc)

    if _data_cache:
        try:
            df_v2 = _data_cache.get("df_valorizado", pd.DataFrame())
            if not df_v2.empty and "nombre_grupo" in df_v2.columns:
                for g in df_v2["nombre_grupo"].dropna().unique():
                    gs = str(g).strip()
                    if gs and gs.upper() not in ("SIN GRUPO", "NAN", "NONE", ""):
                        grupos.add(gs)
        except Exception:
            pass

    result["grupos"] = sorted(grupos)
    return result


_ARTICLE_CACHE: list = []
_ARTICLE_CACHE_TS: float = 0.0
_ARTICLE_CACHE_TTL: float = 300.0  # seconds


def _get_article_list() -> list:
    """Return full article list, refreshing every 5 minutes."""
    import time
    global _ARTICLE_CACHE, _ARTICLE_CACHE_TS
    now = time.monotonic()
    if _ARTICLE_CACHE and (now - _ARTICLE_CACHE_TS) < _ARTICLE_CACHE_TTL:
        return _ARTICLE_CACHE
    catalog = get_new_client_catalog()
    _ARTICLE_CACHE = catalog.get("articulos", [])
    _ARTICLE_CACHE_TS = now
    return _ARTICLE_CACHE


def search_articles(q: str = "", limit: int = 30):
    """Search articles by codigo_serie or descripcion across the full catalog."""
    q = (q or "").strip().lower()
    arts = _get_article_list()

    if not q:
        return arts[:limit]

    matched = []
    for a in arts:
        code = (a.get("codigo_serie") or "").lower()
        desc = (a.get("descripcion") or "").lower()
        if q in code or q in desc:
            matched.append(a)
        if len(matched) >= limit:
            break
    return matched


def create_manual_client(user_id, created_by, nombre_cliente, grupo, entries):
    """Persist a new manual forecast client with its article-month entries."""
    if SessionLocal is None or ForecastManualClient is None or ForecastManualEntry is None:
        raise RuntimeError("SessionLocal not available")

    print(f"[MANUAL_CLIENT CREATE] payload: nombre={nombre_cliente!r} grupo={grupo!r} entries={len(entries)}", flush=True)
    for i, e in enumerate(entries[:5]):
        print(f"[MANUAL_CLIENT CREATE] entry[{i}]: art={e.get('codigo_serie')!r} month={e.get('forecast_month')!r} neg={e.get('neg')!r} sub={e.get('subneg')!r} qty={e.get('cantidad')} cost={e.get('costo_unitario')}", flush=True)

    session = SessionLocal()
    try:
        client = ForecastManualClient(
            user_id=user_id,
            nombre_cliente=nombre_cliente.strip(),
            grupo=grupo.strip() if grupo else None,
            created_by=created_by,
            is_active=True,
        )
        session.add(client)
        session.flush()

        entries_inserted = 0
        for e in entries:
            cantidad    = float(e.get("cantidad", 0) or 0)
            costo_u     = float(e.get("costo_unitario", 0) or 0)
            monto_total = float(e.get("monto_total", 0) or 0) or round(cantidad * costo_u, 2)
            perfil_val = e.get("perfil") or None
            entry = ForecastManualEntry(
                client_id=client.id,
                perfil=str(perfil_val).strip() if perfil_val else None,
                neg=str(e.get("neg", "") or "").strip(),
                subneg=str(e.get("subneg", "") or "").strip(),
                codigo_serie=str(e.get("codigo_serie", "") or "").strip(),
                descripcion=str(e.get("descripcion", "") or "").strip(),
                unidad_medida=str(e.get("unidad_medida", "Unid.") or "Unid.").strip(),
                forecast_month=str(e.get("forecast_month", "") or "").strip(),
                cantidad=cantidad,
                costo_unitario=costo_u,
                monto_total=monto_total,
            )
            session.add(entry)
            entries_inserted += 1

        session.commit()
        client_id = client.id
        print(f"[MANUAL_CLIENT CREATE] client_id={client_id} entries_inserted={entries_inserted}", flush=True)
        logger.info("[FORECAST manual] Created client id=%s name=%r by=%s entries=%d", client_id, nombre_cliente, created_by, entries_inserted)
        return {"ok": True, "client_id": client_id, "nombre_cliente": nombre_cliente.strip()}
    except Exception as exc:
        session.rollback()
        logger.error("[FORECAST manual] create_manual_client error: %s", exc, exc_info=True)
        print(f"[MANUAL_CLIENT CREATE] ERROR: {exc}", flush=True)
        raise
    finally:
        session.close()
