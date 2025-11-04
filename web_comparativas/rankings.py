# web_comparativas/rankings.py
from __future__ import annotations
import re
import math
import unicodedata
import pandas as pd
import numpy as np
from typing import Dict, List, Optional, Tuple

# -----------------------------------------------------------
# Utilidades robustas para nombres de columnas y números AR
# -----------------------------------------------------------

def _slug(s: str) -> str:
    s = unicodedata.normalize("NFKD", str(s)).encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"\s+", " ", s).strip().lower()
    s = s.replace(".", "").replace("/", " ").replace("\\", " ")
    return s

def _first_column(df: pd.DataFrame, candidates: List[str]) -> Optional[str]:
    want = set(candidates)
    for c in df.columns:
        if _slug(c) in want:
            return c
    return None

_NUM_RE = re.compile(r"[-+]?\d+(?:[.,]\d+)?")

def _to_float(x) -> Optional[float]:
    """
    Convierte valores con formato español/argentino o US a float.
    Acepta '$ 1.234,56' / '1,234.56' / '1234,56' / '1234.56'.
    """
    if x is None or (isinstance(x, float) and math.isnan(x)):
        return None
    s = str(x).strip()
    if s == "" or s.lower() in {"nan", "none", "null"}:
        return None

    # Extrae primer número plausible
    m = _NUM_RE.search(s.replace(" ", ""))
    if not m:
        return None
    s = m.group(0)

    # Heurística de separadores:
    #   si hay coma y punto -> asumimos miles/punto + decimales/coma => quitamos puntos, cambiamos coma por punto
    #   si solo hay coma    -> decimales/coma
    #   si solo hay punto   -> decimales/punto
    if "," in s and "." in s:
        s = s.replace(".", "").replace(",", ".")
    elif "," in s and "." not in s:
        s = s.replace(",", ".")
    # si solo punto, ya está bien

    try:
        return float(s)
    except Exception:
        return None

# -----------------------------------------------------------
# Transformación principal
# -----------------------------------------------------------

def build_ranked_positions(
    df: pd.DataFrame,
    max_positions: Optional[int] = None,
    group_by_priority: Tuple[Tuple[str, ...], ...] = (
        # intentamos en este orden: Renglón, luego Código, luego Descripción
        ("renglon", "renglón", "posicion", "posición", "n", "n°", "nº", "item", "ítem"),
        ("codigo", "código"),
        ("descripcion", "descripción"),
    ),
) -> pd.DataFrame:
    """
    Devuelve una tabla tipo 'ranking por renglón' con columnas dinámicas:
      N°, Descripción, 1° Proveedor, 1° Precio, 1° %, 2° Proveedor, 2° Precio, 2° %, ...

    - Ordena por PRECIO ASC.
    - La columna % es la diferencia vs el puesto anterior:
        2°: (Precio2 / Precio1 - 1) * 100
        3°: (Precio3 / Precio2 - 1) * 100
        etc.
    - Si max_positions es None, incluye todos los puestos.
    - Soporta nombres de columnas variados en español.
    """

    # Detectamos columnas clave (robusto a variantes)
    col_proveedor = _first_column(df, [_slug(x) for x in ("Proveedor", "Proveedor oferente")])
    col_desc = _first_column(df, [_slug(x) for x in ("Descripción", "Descripcion", "Articulo", "Artículo", "Producto")])
    col_precio = _first_column(df, [_slug(x) for x in ("Precio unitario", "Precio", "PU", "Precio Unitario")])
    col_total_renglon = _first_column(df, [_slug(x) for x in ("Total por renglón", "Total por renglon", "Total renglón", "Total renglon")])

    if not col_precio and col_total_renglon:
        # fallback si sólo tenemos total por renglón
        col_precio = col_total_renglon

    if not col_proveedor:
        raise ValueError("No se encontró la columna de 'Proveedor' en el normalized.xlsx.")
    if not col_precio:
        raise ValueError("No se encontró la columna de 'Precio unitario' (ni equivalente) en el normalized.xlsx.")

    # Columna por la cual agrupamos los ítems/renglones
    col_group = None
    for candidate_group in group_by_priority:
        col_group = _first_column(df, list(map(_slug, candidate_group)))
        if col_group:
            break

    if not col_group:
        # último recurso: agrupamos por descripción si existe, sino por índice
        col_group = col_desc if col_desc else df.index.name or "__idx__"
        if col_group == "__idx__":
            df = df.reset_index().rename(columns={"index": "__idx__"})

    # Normalizamos numéricos
    work = df.copy()
    work["_precio_"] = work[col_precio].map(_to_float)
    # descartamos filas sin precio válido
    work = work[work["_precio_"].notna()]

    # Si falta descripción, la fabricamos a partir del group
    if not col_desc:
        work["_desc_"] = work[col_group].astype(str)
        col_desc = "_desc_"

    # Ordenamos por grupo y precio
    work.sort_values([col_group, "_precio_", col_proveedor], inplace=True, kind="mergesort")

    # Armamos filas por grupo
    rows: List[Dict[str, object]] = []
    for gval, gdf in work.groupby(col_group, sort=False):
        gdf = gdf.copy()

        # Eliminamos posibles duplicados de proveedor dentro del mismo renglón tomando el menor precio
        gdf = gdf.loc[gdf.groupby(col_proveedor)["_precio_"].idxmin()].sort_values("_precio_")

        # limit opcional
        if max_positions is not None:
            gdf = gdf.head(max_positions)

        # Base de la fila
        base_desc = str(gdf.iloc[0][col_desc]) if col_desc in gdf.columns else ""
        row: Dict[str, object] = {
            "N°": gval,
            "Descripción": base_desc,
        }

        prev_price = None
        for i, (_, r) in enumerate(gdf.iterrows(), start=1):
            prov = str(r[col_proveedor])
            price = float(r["_precio_"]) if r["_precio_"] is not None else None

            row[f"{i}° Proveedor"] = prov
            row[f"{i}° Precio"] = price

            if prev_price is None or price is None or prev_price == 0:
                row[f"{i}° %"] = None  # sin variación para el primero
            else:
                row[f"{i}° %"] = round((price / prev_price - 1.0) * 100.0, 0)

            prev_price = price

        rows.append(row)

    out = pd.DataFrame(rows)

    # Orden natural: por N° si es numérico; en su defecto, por texto
    try:
        out["_ord_"] = pd.to_numeric(out["N°"], errors="coerce")
        out.sort_values(["_ord_", "N°"], inplace=True, kind="mergesort")
        out.drop(columns=["_ord_"], inplace=True)
    except Exception:
        out.sort_values(["N°"], inplace=True, kind="mergesort")

    # Formato amigable opcional: redondeo de precios a 2 decimales (sin tocar los tipos)
    price_cols = [c for c in out.columns if c.endswith(" Precio")]
    for c in price_cols:
        out[c] = out[c].map(lambda v: None if v is None or (isinstance(v, float) and math.isnan(v)) else round(float(v), 2))

    return out
