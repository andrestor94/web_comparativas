# siprosa.py
# Adapter SIPROSA Ranking (multi-archivo)
# - Input recomendado: .zip que contenga 1..N Excels "Ranking" (Medicamentos/Laboratorios/Descartables)
# - También acepta un .xlsx/.xls/.xlsm individual (procesa solo ese archivo)
# - Output: DataFrame con estructura estándar del dashboard + columna Rubro
#
# Firma esperada por tu interfaz/pipeline:
#   def normalize_siprosa_ranking(input_path: Path, metadata: dict, out_dir: Path) -> dict:
#       return {"df": DataFrame, "summary": {...}}

from __future__ import annotations

import re
import zipfile
import unicodedata
from pathlib import Path
from typing import Any, Dict, List

import numpy as np
import pandas as pd


# === ESTRUCTURA NORMALIZADA (idéntica a tu Excel + Rubro) ===
OUTPUT_COLS = [
    "Proveedor",
    "Renglón",
    "Alternativa",
    "Código",
    "Descripción",
    "Cantidad solicitada",
    "Unidad de medida",
    "Precio unitario",
    "Cantidad ofertada",
    "Total por renglón",
    "Especificación técnica",
    "Marca",
    "Posicion",
    "Rubro",
]

# columnas intermedias "tidy"
TIDY_COLS = [
    "id",
    "nombre_producto",
    "cantidad",
    "precio",
    "total",
    "unidad_medida",
    "marca",
    "vencimiento",
    "posicion",
    "proveedor",
    "cuit",
    "observaciones",
]


# ---------- helpers ----------
def _norm(s: str) -> str:
    if s is None:
        return ""
    s = str(s)
    s = unicodedata.normalize("NFKD", s)
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.lower().strip()
    s = re.sub(r"[\s\-\./_]+", " ", s)
    return s


def _looks_ranking(name: str) -> bool:
    return "ranking" in _norm(name)


def _clean_id_str(x) -> str:
    if pd.isna(x):
        return ""
    if isinstance(x, (int, np.integer)):
        return str(int(x))
    if isinstance(x, float) and x.is_integer():
        return str(int(x))
    return str(x).strip()


def _parse_number_series(s: pd.Series) -> pd.Series:
    if s is None:
        return pd.Series(dtype="float64")
    s = s.astype(str).str.strip()
    s = s.str.replace(r"[’' ]", "", regex=True)

    def _to_float(x: str):
        x = x.strip()
        if x == "" or x.lower() in ("nan", "none"):
            return np.nan
        # 1.234,56 ó 1.234.567,89
        if re.search(r"\d\.\d{3}(?:\.\d{3})*,\d{1,2}$", x):
            x = x.replace(".", "").replace(",", ".")
        # 123,45
        elif re.search(r"\d,\d{1,2}$", x) and x.count(".") == 0:
            x = x.replace(",", ".")
        else:
            # quitar miles con punto y comas residuales
            x = re.sub(r"(?<=\d)\.(?=\d{3}(\D|$))", "", x)
            x = x.replace(",", "")
        try:
            return float(x)
        except Exception:
            return np.nan

    return s.map(_to_float)


# ---------- detección y recorte robusto de tabla ----------
def _find_header_row(df_nohdr: pd.DataFrame):
    keys = [
        r"^id$",
        r"nombre.*producto|^producto$|descrip",
        r"cantidad|^cant$",
        r"^precio|importe|punit",
        r"^marca|presentaci",
        r"fechavto|fecha vto|venc",
        r"posicion|posición|ranking|^pos$",
        r"^proveedor|razon social|razón social",
        r"^cuit",
        r"observ",
        r"total",
    ]
    best_i, best_score = None, -1
    scan = min(80, len(df_nohdr))
    for i in range(scan):
        row = df_nohdr.iloc[i].astype(str).map(_norm).tolist()
        score = sum(1 for cell in row if any(re.search(p, cell) for p in keys))
        if score > best_score:
            best_i, best_score = i, score
    return best_i if best_score >= 5 else None


def _cut_contiguous_block(df: pd.DataFrame, key_cols: List[str], max_blank_run: int = 3) -> pd.DataFrame:
    if df.empty:
        return df

    def row_has_keys(r):
        return any(pd.notna(r.get(c, np.nan)) and str(r.get(c)).strip() != "" for c in key_cols)

    start = None
    for i, r in df.iterrows():
        if row_has_keys(r):
            start = i
            break
    if start is None:
        return pd.DataFrame(columns=df.columns)

    blank_run = 0
    end = start
    for i in range(start, len(df)):
        row = df.iloc[i]
        if row.dropna().astype(str).str.strip().eq("").all():
            blank_run += 1
        else:
            blank_run = 0
        if blank_run >= max_blank_run:
            end = i - max_blank_run
            break
        end = i

    block = df.iloc[start : end + 1].copy()

    # Encabezados repetidos
    header_like = []
    for i, r in block.iterrows():
        txts = [_norm(x) for x in r.astype(str).tolist()]
        score = sum(
            1
            for t in txts
            if t
            in [
                "id",
                "nombre producto",
                "producto",
                "descripcion",
                "descripción",
                "cantidad",
                "precio",
                "marca",
                "fechavto",
                "vencimiento",
                "posicion",
                "posición",
                "proveedor",
                "cuit",
                "observaciones",
                "empate",
                "total",
            ]
        )
        if score >= 5:
            header_like.append(i)
    if header_like:
        block = block.drop(index=header_like)

    # Totales / promedio
    mask_total = block.astype(str).apply(
        lambda row: any(
            re.search(r"\btotal\b|\bsub\s*total\b|\bpromedio\b", _norm(v)) if isinstance(v, str) else False
            for v in row
        ),
        axis=1,
    )
    block = block.loc[~mask_total]

    return block.dropna(how="all")


# ---------- rubro robusto (mejora: escanea varias celdas) ----------
def _extract_rubro_value(xls: pd.ExcelFile) -> str:
    """
    Busca una celda que contenga "Rubro" y luego:
    - Escanea hacia la derecha (hasta +12 columnas) en la misma fila
    - Si no encuentra, escanea hacia abajo (hasta +20 filas) en la misma columna
    - Si no encuentra, escanea un rectángulo (derecha + abajo)
    """
    def is_good_value(v) -> bool:
        if pd.isna(v):
            return False
        s = str(v).strip()
        if s == "":
            return False
        if _norm(s).startswith("rubro"):
            return False
        return True

    RIGHT_SCAN = 12
    DOWN_SCAN = 20

    for sh in xls.sheet_names:
        tmp = pd.read_excel(xls, sheet_name=sh, header=None)
        if tmp.empty:
            continue

        nrows, ncols = tmp.shape
        max_r = min(nrows, 1000)
        max_c = min(ncols, 200)

        for r in range(max_r):
            for c in range(max_c):
                val = tmp.iat[r, c]
                if isinstance(val, str) and _norm(val).startswith("rubro"):
                    # 1) buscar a la derecha en la misma fila
                    for k in range(1, RIGHT_SCAN + 1):
                        if c + k < ncols:
                            v = tmp.iat[r, c + k]
                            if is_good_value(v):
                                return str(v).strip()

                    # 2) buscar abajo en la misma columna
                    for k in range(1, DOWN_SCAN + 1):
                        if r + k < nrows:
                            v = tmp.iat[r + k, c]
                            if is_good_value(v):
                                return str(v).strip()

                    # 3) fallback: rectángulo abajo-derecha
                    for dr in range(0, 6):
                        for dc in range(0, RIGHT_SCAN + 1):
                            rr, cc = r + dr, c + dc
                            if rr < nrows and cc < ncols and not (rr == r and cc == c):
                                v = tmp.iat[rr, cc]
                                if is_good_value(v):
                                    return str(v).strip()

    return "Sin dato"


def _load_internal_table(xls: pd.ExcelFile) -> pd.DataFrame:
    for sh in xls.sheet_names:
        raw = pd.read_excel(xls, sheet_name=sh, header=None)
        if raw.empty:
            continue
        hdr = _find_header_row(raw)
        if hdr is None:
            continue
        df = pd.read_excel(xls, sheet_name=sh, header=hdr)
        df = df.dropna(how="all")
        df = _cut_contiguous_block(df, key_cols=["id", "producto", "nombre producto", "precio", "proveedor"])
        if df.shape[1] >= 7 and df.shape[0] >= 1:
            return df
    return pd.DataFrame()


def _column_numeric_ratio(col: pd.Series, sample_n: int = 30) -> float:
    vals = col.head(sample_n).astype(str).str.strip()
    nums = _parse_number_series(vals)
    return float(nums.notna().mean()) if len(vals) else 0.0


def _map_and_reduce(df: pd.DataFrame) -> pd.DataFrame:
    rename_map: Dict[str, str] = {}
    incoming = {c: _norm(c) for c in df.columns}
    total_assigned = False

    for c, n in incoming.items():
        dest = None
        if re.fullmatch(r"id", n):
            dest = "id"
        elif re.search(r"^nombre.*producto|^producto$|descrip", n):
            dest = "nombre_producto"
        elif re.fullmatch(r"cantidad|^cant$", n):
            dest = "cantidad"
        elif re.search(r"^precio|importe|punit", n):
            dest = "precio"
        elif re.search(r"\btotal\b|total por renglon|total rengl", n):
            dest = "total"
            total_assigned = True
        elif re.search(r"unidad|u\.?m\.?|unidad de medida|medida", n):
            dest = "unidad_medida"
        elif re.search(r"^marca|presentaci", n):
            dest = "marca"
        elif re.search(r"^fechavto|fecha vto|venc", n):
            dest = "vencimiento"
        elif re.search(r"^posicion|posición|ranking|^pos$", n):
            dest = "posicion"
        elif re.search(r"^proveedor|razon social|razón social", n):
            dest = "proveedor"
        elif re.search(r"^cuit", n):
            dest = "cuit"
        elif re.search(r"^observ", n):
            dest = "observaciones"
        elif re.fullmatch(r"empate", n):
            dest = None
        elif (not total_assigned) and n.startswith("unnamed"):
            # Heurística: columnas sin nombre -> si mayormente numérica, la tratamos como total
            try:
                ratio = _column_numeric_ratio(df[c])
            except Exception:
                ratio = 0.0
            if ratio >= 0.60:
                dest = "total"
                total_assigned = True

        if dest is not None:
            rename_map[c] = dest

    df2 = df.rename(columns=rename_map)

    keep = [c for c in df2.columns if c in TIDY_COLS]
    df2 = df2[keep]

    for c in TIDY_COLS:
        if c not in df2.columns:
            df2[c] = np.nan

    for c in ["precio", "cantidad", "total"]:
        df2[c] = _parse_number_series(df2[c])

    df2["cantidad"] = df2["cantidad"].round().astype("Int64")
    df2["vencimiento"] = pd.to_datetime(df2["vencimiento"], errors="coerce", dayfirst=True)

    key_cols = ["id", "nombre_producto", "precio", "proveedor"]
    mask_all_empty = df2[key_cols].isna().all(axis=1)
    df2 = df2.loc[~mask_all_empty]

    def row_looks_like_header(r):
        vals = [_norm(x) for x in r.astype(str).tolist()]
        score = sum(
            1
            for t in vals
            if t
            in [
                "id",
                "nombre producto",
                "producto",
                "descripcion",
                "descripción",
                "cantidad",
                "precio",
                "marca",
                "vencimiento",
                "posicion",
                "posición",
                "proveedor",
                "cuit",
                "observaciones",
                "total",
            ]
        )
        return score >= 5

    df2 = df2.loc[~df2.apply(row_looks_like_header, axis=1)]
    return df2.reset_index(drop=True)


def _tidy_to_normalized(tidy: pd.DataFrame, rubro: str) -> pd.DataFrame:
    out = pd.DataFrame(index=tidy.index)

    out["Proveedor"] = tidy["proveedor"].astype(str).replace({"nan": ""})
    out["Código"] = tidy["id"].apply(_clean_id_str)
    out["Descripción"] = tidy["nombre_producto"].astype(str).replace({"nan": ""})

    out["Cantidad solicitada"] = tidy["cantidad"]
    out["Cantidad ofertada"] = tidy["cantidad"]

    if tidy["unidad_medida"].notna().any():
        out["Unidad de medida"] = tidy["unidad_medida"].astype(str).replace({"nan": ""})
        out.loc[out["Unidad de medida"].astype(str).str.strip().eq(""), "Unidad de medida"] = "S/D"
    else:
        out["Unidad de medida"] = "S/D"

    out["Precio unitario"] = tidy["precio"]

    total = tidy["total"].copy()
    m = total.isna() & tidy["precio"].notna() & tidy["cantidad"].notna()
    total.loc[m] = tidy.loc[m, "precio"] * tidy.loc[m, "cantidad"].astype(float)
    out["Total por renglón"] = total

    out["Especificación técnica"] = tidy["observaciones"].astype(str).replace({"nan": ""})
    out.loc[out["Especificación técnica"].astype(str).str.strip().eq(""), "Especificación técnica"] = "S/D"

    out["Marca"] = tidy["marca"].astype(str).replace({"nan": ""})
    out.loc[out["Marca"].astype(str).str.strip().eq(""), "Marca"] = "S/D"

    out["Posicion"] = pd.to_numeric(tidy["posicion"], errors="coerce").round().astype("Int64")

    # No viene en la fuente: fija
    out["Alternativa"] = 1

    # Se asigna después (global por Código)
    out["Renglón"] = pd.NA

    out["Rubro"] = rubro if rubro else "Sin dato"

    return out


def _assign_renglon(df: pd.DataFrame) -> pd.DataFrame:
    # Renglón consecutivo por Código (orden determinístico)
    codes = df["Código"].fillna("").astype(str)
    uniq = [u for u in codes.unique().tolist() if u != ""]

    def key(v: str):
        v = str(v)
        if re.fullmatch(r"\d+", v):
            return (0, int(v))
        return (1, v)

    uniq_sorted = sorted(uniq, key=key)
    mapping = {code: i + 1 for i, code in enumerate(uniq_sorted)}

    df["Renglón"] = codes.map(mapping).astype("Int64")
    return df


def _process_excel(path: Path) -> pd.DataFrame:
    xls = pd.ExcelFile(path)
    rubro = _extract_rubro_value(xls)

    raw_tbl = _load_internal_table(xls)
    if raw_tbl.empty:
        return pd.DataFrame(columns=OUTPUT_COLS)

    tidy = _map_and_reduce(raw_tbl)
    normed = _tidy_to_normalized(tidy, rubro)
    return normed


def normalize_siprosa_ranking(input_path: Path, metadata: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    """
    Handler para el pipeline del sistema.
    - input_path: archivo subido (recomendado .zip con 3 excels ranking)
    - metadata: se mantiene por compatibilidad (puede traer 'platform', etc.)
    - out_dir: carpeta processed/ (el pipeline guardará normalized.xlsx)
    Retorna: {"df": DataFrame, "summary": dict}
    """
    input_path = Path(input_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    excels: List[Path] = []

    # 1) Si es ZIP: extrae y toma todos los excels Ranking
    if input_path.suffix.lower() == ".zip":
        extract_dir = out_dir / f"siprosa_extract_{input_path.stem}"
        extract_dir.mkdir(parents=True, exist_ok=True)

        with zipfile.ZipFile(input_path, "r") as zf:
            zf.extractall(extract_dir)

        for p in extract_dir.rglob("*"):
            if p.is_file() and p.suffix.lower() in {".xlsx", ".xls", ".xlsm"} and _looks_ranking(p.name):
                excels.append(p)

        if not excels:
            raise ValueError("SIPROSA Ranking: el ZIP no contiene Excels 'Ranking' (.xlsx/.xls/.xlsm).")

    # 2) Si es Excel: procesa uno
    elif input_path.suffix.lower() in {".xlsx", ".xls", ".xlsm"}:
        excels = [input_path]

    else:
        raise ValueError("SIPROSA Ranking: se espera .zip o .xlsx/.xls/.xlsm.")

    # Procesar
    frames: List[pd.DataFrame] = []
    for p in excels:
        df = _process_excel(p)
        if not df.empty:
            frames.append(df)

    if not frames:
        raise RuntimeError("SIPROSA Ranking: no se pudo extraer ninguna tabla válida desde los archivos.")

    final_df = pd.concat(frames, ignore_index=True, sort=False)
    final_df = _assign_renglon(final_df)
    final_df = final_df[OUTPUT_COLS]

    # summary mínimo (si el pipeline calcula otros KPIs, no molesta)
    total = float(pd.to_numeric(final_df["Total por renglón"], errors="coerce").fillna(0).sum())
    positions = final_df["Posicion"].value_counts(dropna=True).to_dict()

    summary = {"total_offers": total, "positions": positions, "rows": int(len(final_df))}
    return {"df": final_df, "summary": summary}


# Alias opcional si tu sistema a veces busca `normalize(...)` genérico
def normalize(input_path: Path, metadata: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    return normalize_siprosa_ranking(input_path, metadata, out_dir)
