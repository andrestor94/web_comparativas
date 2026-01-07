# web_comparativas/adapters/la_pampa.py
# Adapter "La Pampa" para integrarse al pipeline
# Input esperado:
#   - .zip con PDFs (comparativas + pliegos) adentro (modo original), O
#   - .pdf suelto (comparativa o pliego): busca el PDF "compañero" en la misma carpeta, O
#   - directorio: busca PDFs dentro
#
# Output: DataFrame en formato "estándar" para dashboard + archivos extra en processed/

from __future__ import annotations

import os
import re
import zipfile
from pathlib import Path
from typing import Any, Dict, List, Optional, Tuple
from decimal import Decimal, ROUND_HALF_UP

import pandas as pd
import numpy as np


# =======================
# Columnas estándar (dashboard)
# =======================
STANDARD_COLUMNS = [
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
    "Archivo Origen",  # extra (no rompe)
]


# =======================
# Helpers
# =======================
FOOTER_NOISE_RE = re.compile(
    r"(Provincia\s+de\s+La\s+Pampa|DEPARTAMENTO\s+COMPRAS|CONTADURIA\s+GENERAL|"
    r"Firma\s+y\s+Sello\s+del\s+Proponente|Domicilio\s+Legal)",
    re.IGNORECASE,
)

NUM_EU_RE = re.compile(r"^\d{1,3}(?:\.\d{3})*(?:,\d+)?$")

_UNIT_CANON = {
    "UN": "UN", "UNI": "UN", "UNI.": "UN", "UN.": "UN", "U.": "UN", "UNIDAD": "UN",
    "ENV": "ENV", "ENV.": "ENV", "ENVASE": "ENV", "ENVASE.": "ENV", "ENVASES": "ENV", "ENVASES.": "ENV",
    "CAJA": "CAJA", "CAJA.": "CAJA",
    "SOBRE": "SOBRE", "SOBRE.": "SOBRE",
    "FRASCO": "FRASCO", "FRASCO.": "FRASCO",
    "BOLSA": "BOLSA", "BOLSA.": "BOLSA",
    "TUBO": "TUBO", "TUBO.": "TUBO",
    "PAR": "PAR", "PARES": "PAR",
    "KIT": "KIT",
    "ROLLO": "ROLLO", "ROLLO.": "ROLLO",
    "PAQUETE": "PAQUETE", "PAQ": "PAQUETE", "PAQ.": "PAQUETE",
    "JUEGO": "JUEGO", "JGO": "JUEGO",
    "JERINGA": "JERINGA",
    "BOTELLA": "BOTELLA",
    "POTE": "POTE",
    "AMPOLLA": "AMPOLLA", "AMP": "AMP", "AMP.": "AMP",
    "BIDON": "BIDON", "BIDÓN": "BIDON",
    "BOBINA": "BOBINA",
    "L": "L", "LITRO": "L", "ML": "ML", "CC": "CC",
    "KG": "KG", "G": "G",
    "M": "M", "METRO": "M",
    "CM": "CM", "MM": "MM",
}

_UNIT_DISPLAY = {
    "UN": "UNIDAD",
    "ENV": "ENVASE",
    "AMP": "AMPOLLA",
    "AMPOLLA": "AMPOLLA",
    "CAJA": "CAJA",
    "SOBRE": "SOBRE",
    "FRASCO": "FRASCO",
    "BOLSA": "BOLSA",
    "TUBO": "TUBO",
    "PAR": "PAR",
    "KIT": "KIT",
    "ROLLO": "ROLLO",
    "PAQUETE": "PAQUETE",
    "JUEGO": "JUEGO",
    "JERINGA": "JERINGA",
    "BOTELLA": "BOTELLA",
    "POTE": "POTE",
    "BIDON": "BIDON",
    "BOBINA": "BOBINA",
    "L": "L",
    "ML": "ML",
    "CC": "CC",
    "KG": "KG",
    "G": "G",
    "M": "M",
    "CM": "CM",
    "MM": "MM",
}


def _normalize_spaces(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    s2 = re.sub(r"\s+", " ", str(s)).strip()
    return s2 if s2 else None


def _is_nan(x) -> bool:
    try:
        return x is None or (isinstance(x, float) and np.isnan(x))
    except Exception:
        return False


def _round_half_up(val: float, decimals: int) -> float:
    q = Decimal(str(val)).quantize(Decimal("1." + "0" * decimals), rounding=ROUND_HALF_UP)
    return float(q)


def _parse_number_eu_from_text(txt: str) -> Optional[float]:
    if txt is None:
        return None
    s = str(txt).strip()
    if s == "":
        return None

    s = (s.replace("ARS", "").replace("$", "").replace("€", "").replace("\u00A0", " ")
         .replace(" ", "").strip())
    s = re.sub(r"[^0-9\.,]", "", s)
    s = s.replace(".", "")
    if s.count(",") > 1:
        last = s.rfind(",")
        s = s[:last].replace(",", "") + s[last:]
    s = s.replace(",", ".")
    if s in {"", "."}:
        return None

    try:
        return float(Decimal(s))
    except Exception:
        m = re.search(r"\d+(?:\.\d+)?", s)
        return float(m.group(0)) if m else None


def _parse_number_cell(val: Any, *, decimals: Optional[int] = None, for_quantity: bool = False) -> Optional[float]:
    if _is_nan(val):
        return None
    try:
        num = float(val)
        if for_quantity and decimals is not None:
            return _round_half_up(num, decimals)
        return num
    except Exception:
        pass

    num = _parse_number_eu_from_text(str(val))
    if num is None:
        return None
    if for_quantity and decimals is not None:
        return _round_half_up(num, decimals)
    return num


def _canonical_unidad(s: Optional[str]) -> Optional[str]:
    if s is None:
        return None
    t = _normalize_spaces(s)
    if not t:
        return None
    up = t.upper()
    for tok in re.split(r"[,:\s/|;·\-*]+", up):
        tok2 = tok.strip(".").strip()
        if not tok2:
            continue
        if tok2 in _UNIT_CANON:
            return _UNIT_CANON[tok2]
    return None


def _unit_display(u: Any) -> Optional[str]:
    if u is None or (isinstance(u, float) and np.isnan(u)):
        return None
    s = str(u).strip().upper()
    return _UNIT_DISPLAY.get(s, s)


def _split_desc_and_unit_from_text(desc_raw: Optional[str]) -> Tuple[Optional[str], Optional[str]]:
    if not desc_raw:
        return (None, None)
    txt = _normalize_spaces(desc_raw) or ""
    if FOOTER_NOISE_RE.search(txt):
        return (None, None)
    t = re.sub(r"\$\s*\$", "$$", txt)
    if "$$" in t:
        left, right = t.split("$$", 1)
        desc = _normalize_spaces(left)
        munit = re.search(r"^([A-Za-zÁÉÍÓÚÑ./()%0-9 ]{1,20})", right.strip())
        unidad = _canonical_unidad(munit.group(1)) if munit else None
        return (desc, unidad)
    return (_normalize_spaces(txt), None)


# =======================
# Extract expediente/tag
# =======================
def _extract_expediente_from_pdf(path_pdf: str) -> str:
    try:
        import pdfplumber
        with pdfplumber.open(path_pdf) as pdf:
            for page in pdf.pages[:3]:
                txt = page.extract_text() or ""
                m = re.search(r"Expediente\s*N[º°]?:?\s*([0-9A-Za-z\/\-.]+)", txt, re.IGNORECASE)
                if m:
                    return m.group(1).strip()
    except Exception:
        pass

    base = os.path.basename(path_pdf)
    m2 = re.search(r"(\d{3,6})[-_/](\d{2,4})", base)
    if m2:
        return f"{m2.group(1)}/{m2.group(2)}"
    return "SIN_EXPEDIENTE"


def _tag_expediente(expediente: str) -> str:
    m = re.search(r"([0-9]{3,6}).*?([0-9]{2,4})", expediente or "")
    if not m:
        solo_dig = re.sub(r"\D+", "", expediente or "")
        return solo_dig[-6:] if solo_dig else "SIN_TAG"
    num = int(m.group(1))
    year = int(m.group(2))
    if year >= 100:
        year = year % 100
    return f"{num}-{year:02d}"


# =======================
# Detectores tipo PDF
# =======================
def _looks_like_comparativa_pdf(path: str) -> bool:
    name_ok = "comparativa" in os.path.basename(path).lower()
    if name_ok:
        return True
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            page = pdf.pages[0]
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
            return any(re.search(r"\bAlt\.?\s*\d+", w["text"], flags=re.IGNORECASE) for w in words)
    except Exception:
        return False


def _looks_like_pliego_pdf(path: str) -> bool:
    name = os.path.basename(path).lower()
    if ("-lp" in name) or ("pliego" in name):
        return True
    try:
        import pdfplumber
        with pdfplumber.open(path) as pdf:
            txt = (pdf.pages[0].extract_text() or "").upper()
            return any(t in txt for t in ["ITEM", "ÍTEM", "RENGL", "DESCRIP", "UNIDAD"])
    except Exception:
        return False


# =======================
# Pairing por TAG
# =======================
def _find_tag_in_filename(path: str) -> Optional[str]:
    base = os.path.basename(path)
    m = re.search(r"(\d{3,6})[-_/](\d{2})", base)
    if m:
        return f"{int(m.group(1))}-{int(m.group(2)):02d}"
    return None


def _pair_pliego_for_tag(tag: str, pliego_paths: List[str]) -> Optional[str]:
    for p in pliego_paths:
        if tag in os.path.basename(p):
            return p
    for p in pliego_paths:
        exp = _extract_expediente_from_pdf(p)
        if exp and _tag_expediente(exp) == tag:
            return p
    return None


# =======================
# Input: ZIP / PDF / DIR
# =======================
def _collect_pdfs(input_path: Path, out_dir: Path) -> Tuple[List[Path], Optional[Path]]:
    """
    Devuelve (pdf_paths, extract_dir)
    - Si es ZIP: extrae a out_dir y devuelve PDFs desde extract_dir.
    - Si es PDF: busca PDFs en la misma carpeta (no recursivo).
    - Si es DIR: busca PDFs recursivo.
    """
    input_path = Path(input_path)

    if input_path.is_dir():
        return (list(input_path.rglob("*.pdf")), None)

    if input_path.suffix.lower() == ".zip":
        extract_dir = out_dir / f"la_pampa_extract_{input_path.stem}"
        extract_dir.mkdir(parents=True, exist_ok=True)
        with zipfile.ZipFile(input_path, "r") as zf:
            zf.extractall(extract_dir)
        return (list(extract_dir.rglob("*.pdf")), extract_dir)

    if input_path.suffix.lower() == ".pdf":
        # MUY IMPORTANTE: tu interfaz suele guardar los dos PDFs en la misma carpeta temporal.
        # Entonces los levantamos desde ahí.
        base_dir = input_path.parent
        return (list(base_dir.glob("*.pdf")), None)

    raise ValueError("La Pampa: se espera un .zip, un .pdf o un directorio con PDFs.")


# =======================
# Lector PLIEGO (tu versión)
# =======================
def _detect_column_anchors(words: List[dict]) -> Tuple[float, float, float, float, float]:
    # Clustering approach: Find vertical columns of numbers
    
    # 1. Identify ITEM column candidate (Leftmost vertical cluster of integers 1..9999)
    # Filter words that are purely numeric and small (items usually are 1, 2, 3...)
    item_cands = [w for w in words if re.fullmatch(r"^\d{1,5}$", w["text"]) and w["x0"] < 300]
    
    x_item = 56.0 # default
    if item_cands:
        # Simple Clustering
        xs = sorted([w["x0"] for w in item_cands])
        clusters = []
        if xs:
            curr_c = [xs[0]]
            for x in xs[1:]:
                if x - curr_c[-1] <= 10: # 10px tolerance
                    curr_c.append(x)
                else:
                    clusters.append(curr_c)
                    curr_c = [x]
            clusters.append(curr_c)
            # Pick the cluster with most elements that is somewhat to the left
            # IMPROVED: Prefer the LEFTMOST cluster that has a significant number of items (e.g. > 20% of max length)
            # This avoids picking CGO (Code) column as ITEM if CGO is denser.
            
            # 1. Filter small clusters (noise)
            max_len = max(len(c) for c in clusters)
            significant_clusters = [c for c in clusters if len(c) >= max(3, max_len * 0.5)]
            
            # 2. Sort by position (Left -> Right)
            significant_clusters.sort(key=lambda c: np.median(c))
            
            if significant_clusters:
                best_c = significant_clusters[0] # Pick Leftmost
                x_item = float(np.median(best_c))
            else:
                 # Fallback
                best_c = max(clusters, key=len)
                x_item = float(np.median(best_c))

    # 2. Identify QTY column candidate (Cluster of numbers with decimals or integers, to the right of item)
    # Quantity usually has 2 decimals or look like big integers
    qty_cands = [w for w in words if (NUM_EU_RE.fullmatch(w["text"]) or re.fullmatch(r"\d+", w["text"])) 
                 and w["x0"] > (x_item + 30) and w["x0"] < 500]
    
    x_qty = x_item + 90.0 # default spacing
    if qty_cands:
         xs = sorted([w["x0"] for w in qty_cands])
         clusters = []
         if xs:
            curr_c = [xs[0]]
            for x in xs[1:]:
                if x - curr_c[-1] <= 10:
                    curr_c.append(x)
                else:
                    clusters.append(curr_c)
                    curr_c = [x]
            clusters.append(curr_c)
            # Usually Qty is the dense column in the middle-ish
            # We pick the one with most items, or maybe specific check?
            # Let's pick the one distinct from Item
            valid_clusters = [c for c in clusters if np.median(c) > (x_item + 40)]
            if valid_clusters:
                best_c = max(valid_clusters, key=len)
                x_qty = float(np.median(best_c))
    
    # 3. Derive other anchors relative to Item and Qty
    # Standard La Pampa: Item | Cgo | Qty | Unit | Desc
    # Or: Item | Qty | ...
    # We try to fit the Cgo in between if space permits, else assume it's close to Item
    
    if x_qty - x_item > 80:
        x_cgo = x_item + (x_qty - x_item) * 0.4 # approx between
    else:
        x_cgo = x_item + 40.0
        x_qty = max(x_qty, x_cgo + 40.0)

    x_unit = x_qty + 40.0
    
    # 4. Describe anchor (starts after Unit)
    # Look for long text words
    desc_cands = [w for w in words if len(w["text"]) > 3 and not re.match(r"^[\d.,]+$", w["text"]) and w["x0"] > x_qty]
    if desc_cands:
         xs_desc = sorted([w["x0"] for w in desc_cands])
         x_desc = float(np.percentile(xs_desc, 10)) # Start of description block
         x_desc = max(x_desc, x_unit + 10)
    else:
         x_desc = x_unit + 60.0

    return (x_item, x_cgo, x_qty, x_unit, x_desc)


def _build_cuts_from_anchors(x_item: float, x_cgo: float, x_qty: float, x_unit: float, x_desc: float) -> List[float]:
    return [(x_item + x_cgo) / 2.0, (x_cgo + x_qty) / 2.0, (x_qty + x_unit) / 2.0, (x_unit + x_desc) / 2.0]


def _assign_band(x: float, cuts: List[float]) -> int:
    if x < cuts[0]: return 0
    if x < cuts[1]: return 1
    if x < cuts[2]: return 2
    if x < cuts[3]: return 3
    return 4


def _pick_qty_from_words(ws: List[dict], x_qty: float, x_unit: float) -> Optional[str]:
    cands = [w for w in ws if NUM_EU_RE.fullmatch(w["text"])
             and (x_qty - 40) <= w["x0"] <= (x_unit + 40)]
    if not cands:
        return None
    mid = (x_qty + x_unit) / 2.0
    w = min(cands, key=lambda t: abs(t["x0"] - mid))
    return w["text"]


def _extract_unit_and_b3desc(b3_words: List[dict]) -> Tuple[Optional[str], Optional[str]]:
    if not b3_words:
        return (None, None)
    ws = sorted(b3_words, key=lambda t: t["x0"])
    unit = None
    unit_idx = None
    for i, w in enumerate(ws):
        cand = _canonical_unidad(w["text"])
        if cand:
            unit = cand
            unit_idx = i
            break
    if unit is not None and unit_idx is not None:
        rest = [w["text"] for w in ws[unit_idx + 1:]]
        extra = _normalize_spaces(" ".join(rest)) if rest else None
        return (unit, extra)
    extra = _normalize_spaces(" ".join(w["text"] for w in ws))
    return (None, extra if extra else None)


def leer_pliego_pdf(path_pdf: str) -> pd.DataFrame:
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError("Falta dependencia: instalar pdfplumber para leer pliegos PDF.") from e

    regs: List[Dict[str, Any]] = []

    with pdfplumber.open(path_pdf) as pdf:
        for page in pdf.pages:
            words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
            if not words:
                continue

            x_item, x_cgo, x_qty, x_unit, x_desc = _detect_column_anchors(words)
            cuts = _build_cuts_from_anchors(x_item, x_cgo, x_qty, x_unit, x_desc)

            y_tol = 1.8
            def ybin(y): return round(y / y_tol) * y_tol
            line_map: Dict[float, List[dict]] = {}
            for w in words:
                line_map.setdefault(ybin(w["top"]), []).append(w)

            last_row: Optional[Dict[str, Any]] = None
            expect_unit_lines = 0

            for y in sorted(line_map.keys()):
                ws = sorted(line_map[y], key=lambda w: w["x0"])
                band_words = {0: [], 1: [], 2: [], 3: [], 4: []}
                for w in ws:
                    band_words[_assign_band(w["x0"], cuts)].append(w)

                def join_text(idx: int) -> Optional[str]:
                    return _normalize_spaces(" ".join(t["text"] for t in band_words[idx]).strip()) if band_words[idx] else None

                cgo_txt  = join_text(1)
                qty_txt0 = join_text(2)
                unit_from_b3, desc_prefix_b3 = _extract_unit_and_b3desc(band_words[3])
                desc_txt_b4 = join_text(4)

                if cgo_txt and FOOTER_NOISE_RE.search(cgo_txt): cgo_txt = None
                if qty_txt0 and FOOTER_NOISE_RE.search(qty_txt0): qty_txt0 = None
                if desc_txt_b4 and FOOTER_NOISE_RE.search(desc_txt_b4): desc_txt_b4 = None

                nums_on_line = [w for w in ws if NUM_EU_RE.fullmatch(w["text"]) or re.fullmatch(r"\d{1,6}", w["text"])]
                nums_on_line.sort(key=lambda t: t["x0"])

                # Relaxed tolerance for scanned docs: 35px
                is_item_line = any(re.fullmatch(r"\d{1,6}", w["text"]) and abs(w["x0"] - x_item) <= 35 for w in nums_on_line)
                if is_item_line:
                    if last_row:
                        regs.append(last_row)
                        last_row = None
                        expect_unit_lines = 0

                    item_candidates = [w for w in nums_on_line if re.fullmatch(r"\d{1,6}", w["text"]) and abs(w["x0"] - x_item) <= 35]
                    if not item_candidates:
                        continue
                    item_val = int(item_candidates[0]["text"])

                    # CGO
                    cgo_val = None
                    if len(nums_on_line) >= 2:
                        cgo_val = nums_on_line[1]["text"]
                    if cgo_val is None:
                        near_cgo = [w for w in nums_on_line if abs(w["x0"] - x_cgo) <= 20]
                        if near_cgo:
                            cgo_val = near_cgo[0]["text"]
                    cgo_val = _normalize_spaces(cgo_val)

                    # CANTIDAD
                    qty_val = None
                    if len(nums_on_line) >= 3:
                        qty_val = _parse_number_cell(nums_on_line[2]["text"], decimals=2, for_quantity=True)
                    if qty_val is None:
                        qty_txt = qty_txt0 if (qty_txt0 and NUM_EU_RE.fullmatch(qty_txt0)) else _pick_qty_from_words(ws, x_qty, x_unit)
                        qty_val = _parse_number_cell(qty_txt, decimals=2, for_quantity=True)

                    # DESCRIPCION
                    desc_line = " ".join([t for t in [desc_prefix_b3, desc_txt_b4] if t])
                    desc_clean, unit_from_desc = _split_desc_and_unit_from_text(desc_line or "")

                    # UNIDAD
                    unidad_val = unit_from_b3 or (_canonical_unidad(unit_from_desc) if unit_from_desc else None)
                    if (desc_line and "$" in desc_line) and not unidad_val:
                        expect_unit_lines = 3

                    last_row = {
                        "ITEM": item_val,
                        "CGO": cgo_val,
                        "CANTIDAD": qty_val,
                        "DESCRIPCION": desc_clean,
                        "UNIDAD": unidad_val,
                    }
                    continue

                # continuación
                if last_row:
                    if (not last_row.get("UNIDAD")) and unit_from_b3:
                        last_row["UNIDAD"] = unit_from_b3

                    if desc_prefix_b3 or desc_txt_b4:
                        prev = last_row.get("DESCRIPCION") or ""
                        joined_desc = _normalize_spaces((" ".join([prev, desc_prefix_b3 or "", desc_txt_b4 or ""])).strip())
                        d2, u2 = _split_desc_and_unit_from_text(joined_desc)
                        last_row["DESCRIPCION"] = d2
                        if (not last_row.get("UNIDAD")) and u2:
                            last_row["UNIDAD"] = _canonical_unidad(u2)
                        if (desc_prefix_b3 and "$" in desc_prefix_b3) or (desc_txt_b4 and "$" in desc_txt_b4):
                            if not last_row.get("UNIDAD"):
                                expect_unit_lines = max(expect_unit_lines, 3)

                    if last_row.get("CANTIDAD") is None:
                        qty_txt = qty_txt0 if (qty_txt0 and NUM_EU_RE.fullmatch(qty_txt0)) else _pick_qty_from_words(ws, x_qty, x_unit)
                        q2 = _parse_number_cell(qty_txt, decimals=2, for_quantity=True)
                        if q2 is not None:
                            last_row["CANTIDAD"] = q2

                    if expect_unit_lines > 0 and not last_row.get("UNIDAD"):
                        line_text_all = _normalize_spaces(" ".join(t["text"] for t in ws))
                        cand2 = _canonical_unidad(line_text_all or "")
                        if cand2:
                            last_row["UNIDAD"] = cand2
                        expect_unit_lines -= 1

            if last_row:
                regs.append(last_row)

    df = pd.DataFrame(regs)
    if df.empty:
        return pd.DataFrame(columns=["ITEM", "CGO", "CANTIDAD", "DESCRIPCION", "UNIDAD"])

    # colapsar por ITEM (manteniendo lo mejor)
    def _modo_str(series: pd.Series) -> Optional[str]:
        s = series.dropna().astype(str).str.strip()
        s = s[s != ""]
        if s.empty:
            return None
        return s.value_counts(dropna=True).index[0]

    def _longest_str(series: pd.Series) -> Optional[str]:
        s = series.dropna().astype(str).str.strip()
        s = s[s != ""]
        if s.empty:
            return None
        return max(s, key=len)

    def _modo_num(series: pd.Series) -> Optional[float]:
        s = series.dropna().astype(float)
        if s.empty:
            return None
        vc = s.round(6).value_counts()
        cands = vc[vc == vc.max()].index.tolist()
        return float(min(cands))

    out = df.groupby("ITEM", as_index=False).agg({
        "CGO": _modo_str,
        "UNIDAD": _modo_str,
        "DESCRIPCION": _longest_str,
        "CANTIDAD": _modo_num,
    })

    out["CANTIDAD"] = out["CANTIDAD"].apply(lambda x: _round_half_up(float(x), 2) if x is not None else None)
    out["CGO"] = out["CGO"].apply(_normalize_spaces)
    out["UNIDAD"] = out["UNIDAD"].apply(_canonical_unidad)
    out["DESCRIPCION"] = out["DESCRIPCION"].apply(_normalize_spaces)

    return out[["ITEM", "CGO", "CANTIDAD", "DESCRIPCION", "UNIDAD"]]


# =======================
# Lector COMPARATIVA (reforzado POR PÁGINA)
# =======================
_CODE_RE = re.compile(r"(\d+)\s*[-–—]\s*\d+\s*[-–—]\s*Alt\.?\s*(\d+)", re.IGNORECASE)
_NUM_SCAN_RE = re.compile(r"\d{1,3}(?:\.\d{3})*(?:,\d+)?")  # EU style


def _parse_comparativa_page_table(page) -> List[Dict[str, Any]]:
    """
    Intenta leer una página como tabla (grilla). Si no puede, devuelve [].
    """
    registros: List[Dict[str, Any]] = []
    tbl = page.extract_table()
    if not tbl:
        return registros

    best_i = None
    best_cnt = 0
    for i, row in enumerate(tbl[:12]):
        if not row:
            continue
        cnt = sum(1 for c in row if c and _CODE_RE.search(str(c).replace("\n", " ")))
        if cnt > best_cnt:
            best_cnt = cnt
            best_i = i

    if best_i is None or best_cnt < 3:
        return registros

    header = tbl[best_i]
    code_cols: List[int] = []
    colcodes: Dict[int, Tuple[int, int, str]] = {}

    for j, cell in enumerate(header):
        if not cell:
            continue
        m = _CODE_RE.search(str(cell).replace("\n", " "))
        if m:
            item = int(m.group(1))
            alt = int(m.group(2))
            canon = f"{item}-0-Alt.{alt}"
            code_cols.append(j)
            colcodes[j] = (item, alt, canon)

    if not code_cols:
        return registros

    last_provider: Optional[str] = None

    for row in tbl[best_i + 1:]:
        if not row:
            continue

        prov_cell = row[0] if len(row) > 0 else None
        prov = _normalize_spaces(str(prov_cell).replace("\n", " ")) if prov_cell else None
        if prov:
            last_provider = prov
        prov = prov or last_provider
        if not prov:
            continue

        up = prov.upper()
        if up.startswith(("TOTAL", "PROMEDIO", "PROVEEDOR")):
            continue

        for j in code_cols:
            if j >= len(row):
                continue
            cell = row[j]
            if not cell:
                continue
            mnum = _NUM_SCAN_RE.search(str(cell).replace("\n", " "))
            if not mnum:
                continue
            pu = _parse_number_cell(mnum.group(0))
            if pu is None:
                continue

            item, alt, canon = colcodes[j]
            registros.append({
                "PROVEEDOR": prov.strip(),
                "ITEM": item,
                "Alt": alt,
                "PU": float(pu),
                "ItemCode": canon,
            })

    return registros


def _parse_comparativa_page_words(words: List[dict]) -> List[Dict[str, Any]]:
    """
    Tu método original (coords/words) aplicado a UNA página.
    """
    registros: List[Dict[str, Any]] = []
    num_pat = re.compile(r"^\d{1,3}(?:\.\d{3})*(?:,\d+)?$")

    def _parse_item_alt_token_strict(txt: str) -> Tuple[Optional[int], Optional[int]]:
        if not txt:
            return (None, None)
        s = str(txt).strip()
        m = re.search(r"(\d+)\s*(?:-\s*\d+)?\s*-\s*Alt\.?\s*(\d+)", s, flags=re.IGNORECASE)
        if m:
            return (int(m.group(1)), int(m.group(2)))
        m2 = re.search(r"(\d+)\s+Alt\.?\s*(\d+)", s, flags=re.IGNORECASE)
        if m2:
            return (int(m2.group(1)), int(m2.group(2)))
        return (None, None)

    code_words = []
    for w in words:
        it, al = _parse_item_alt_token_strict(w["text"])
        if it is not None:
            code_words.append(w)
    if not code_words:
        return registros

    xs = sorted([w["x0"] for w in code_words])
    cols_x = []
    cluster = [xs[0]]
    for x in xs[1:]:
        if abs(x - float(np.mean(cluster))) <= 12:
            cluster.append(x)
        else:
            cols_x.append(float(np.mean(cluster)))
            cluster = [x]
    cols_x.append(float(np.mean(cluster)))
    code_cols = sorted(set(cols_x))

    colcodes: Dict[int, Tuple[int, int, str]] = {}
    for w in code_words:
        idx = int(np.argmin([abs(w["x0"] - cx) for cx in code_cols]))
        it, al = _parse_item_alt_token_strict(w["text"])
        if it is not None and idx not in colcodes:
            colcodes[idx] = (int(it), int(al), w["text"])

    if not colcodes:
        return registros

    min_code_x = min(code_cols)
    x_thresh = min_code_x - 12.0
    x_left_limit = x_thresh + 25.0

    y_tol = 1.8
    def ybin(y): return round(y / y_tol) * y_tol

    line_map: Dict[float, Dict[str, List[dict]]] = {}
    for w in words:
        y = ybin(w["top"])
        obj = line_map.setdefault(y, {"left": [], "nums": [], "all": []})
        obj["all"].append(w)

        if w["x0"] < x_left_limit:
            obj["left"].append(w)

        if num_pat.fullmatch(w["text"]) and (w["x0"] >= (min_code_x - 5)):
            obj["nums"].append(w)

    ys = sorted(line_map.keys())

    def _join_provider(ws: List[dict]) -> Optional[str]:
        if not ws:
            return None
        ws_sorted = sorted(ws, key=lambda t: t["x0"])
        has_letters = any(re.search(r"[A-Za-zÁÉÍÓÚÑ]", t["text"]) for t in ws_sorted)

        keep = []
        for t in ws_sorted:
            tx = str(t["text"]).strip()
            if not tx:
                continue
            if re.search(r"[A-Za-zÁÉÍÓÚÑ]", tx):
                keep.append(tx)
                continue
            if has_letters and re.fullmatch(r"\d{1,3}", tx):
                keep.append(tx)
                continue
        return _normalize_spaces(" ".join(keep))

    def _get_provider(i: int) -> Optional[str]:
        if line_map[ys[i]]["left"]:
            p = _join_provider(line_map[ys[i]]["left"])
            if p:
                return p

        for k in range(1, 6):
            j = i - k
            if j < 0:
                break
            if ys[i] - ys[j] > 60:
                break
            if line_map[ys[j]]["left"]:
                p = _join_provider(line_map[ys[j]]["left"])
                if p:
                    return p

        if i + 1 < len(ys) and (ys[i + 1] - ys[i] <= 24.0) and line_map[ys[i + 1]]["left"]:
            p = _join_provider(line_map[ys[i + 1]]["left"])
            if p:
                return p
        return None

    for ii, y in enumerate(ys):
        row = line_map[y]
        if not row["nums"]:
            continue

        prov = _get_provider(ii)
        if not prov:
            continue

        up = prov.upper()
        if up.startswith("TOTAL") or up.startswith("PROMEDIO") or up.startswith("PROVEEDOR"):
            continue

        for numw in sorted(row["nums"], key=lambda t: t["x0"]):
            idx = int(np.argmin([abs(numw["x0"] - cx) for cx in code_cols]))
            if idx not in colcodes:
                continue

            if abs(numw["x0"] - code_cols[idx]) > 35:
                continue

            item, alt, rawtxt = colcodes[idx]
            pu = _parse_number_cell(numw["text"])
            if pu is None:
                continue

            registros.append({
                "PROVEEDOR": prov.strip(),
                "ITEM": int(item),
                "Alt": int(alt),
                "PU": float(pu),
                "ItemCode": rawtxt or f"{item}-0-Alt.{alt}",
            })

    return registros


def leer_comparativa_pdf(path_pdf: str) -> pd.DataFrame:
    """
    Refuerzo real: por cada página:
      1) intenta tabla
      2) si esa página no produce registros, intenta words/coords
    Luego concatena y deduplica.
    """
    try:
        import pdfplumber
    except Exception as e:
        raise RuntimeError("Falta dependencia: instalar pdfplumber para leer comparativas PDF.") from e

    regs: List[Dict[str, Any]] = []

    with pdfplumber.open(path_pdf) as pdf:
        for page in pdf.pages:
            page_regs = _parse_comparativa_page_table(page)
            if not page_regs:
                words = page.extract_words(use_text_flow=True, keep_blank_chars=False) or []
                if words:
                    page_regs = _parse_comparativa_page_words(words)
            regs.extend(page_regs)

    cols = ["PROVEEDOR", "ITEM", "Alt", "PU", "ItemCode"]
    if not regs:
        return pd.DataFrame(columns=cols)

    df = pd.DataFrame(regs, columns=cols)

    # limpiar / deduplicar
    df["PROVEEDOR"] = df["PROVEEDOR"].astype(str).map(lambda x: _normalize_spaces(x) or x).str.strip()
    df["ITEM"] = pd.to_numeric(df["ITEM"], errors="coerce").astype("Int64")
    df["Alt"] = pd.to_numeric(df["Alt"], errors="coerce").astype("Int64")
    df["PU"] = pd.to_numeric(df["PU"], errors="coerce")

    df = df.dropna(subset=["PROVEEDOR", "ITEM", "Alt", "PU"]).copy()
    df["ITEM"] = df["ITEM"].astype(int)
    df["Alt"] = df["Alt"].astype(int)

    df = df.drop_duplicates(subset=["PROVEEDOR", "ITEM", "Alt", "PU"], keep="first").reset_index(drop=True)
    return df


# =======================
# Construcción estándar
# =======================
def _build_standard_from_master(master_df: pd.DataFrame) -> pd.DataFrame:
    if master_df is None or master_df.empty:
        return pd.DataFrame(columns=STANDARD_COLUMNS)

    out = pd.DataFrame(index=master_df.index)
    out["Proveedor"] = master_df.get("PROVEEDOR")
    out["Renglón"] = master_df.get("ITEM")
    out["Alternativa"] = master_df.get("Alt")
    out["Código"] = master_df.get("CGO")
    out["Descripción"] = master_df.get("DESCRIPCION")
    out["Cantidad solicitada"] = master_df.get("CANTIDAD")

    if "UNIDAD" in master_df.columns:
        out["Unidad de medida"] = master_df["UNIDAD"].apply(_unit_display)
    else:
        out["Unidad de medida"] = None

    out["Precio unitario"] = master_df.get("PU")
    out["Cantidad ofertada"] = pd.Series([None] * len(master_df), index=master_df.index)
    out["Total por renglón"] = master_df.get("TOTAL")
    out["Especificación técnica"] = pd.Series([None] * len(master_df), index=master_df.index)
    out["Marca"] = pd.Series([None] * len(master_df), index=master_df.index)
    out["Posicion"] = master_df.get("Posicion")

    if "ArchivoComparativa" in master_df.columns:
        out["Archivo Origen"] = master_df["ArchivoComparativa"]
    elif "ArchivoPliego" in master_df.columns:
        out["Archivo Origen"] = master_df["ArchivoPliego"]
    else:
        out["Archivo Origen"] = None

    for c in STANDARD_COLUMNS:
        if c not in out.columns:
            out[c] = None

    return out[STANDARD_COLUMNS]


# =======================
# Entrypoint para registry.yml
# =======================
def normalize_la_pampa(input_path: Path, metadata: Dict[str, Any], out_dir: Path) -> Dict[str, Any]:
    """
    Firma pensada para el pipeline.
    Ahora soporta:
      - ZIP con PDFs
      - PDF suelto (busca el compañero en el mismo dir)
      - directorio con PDFs
    """
    input_path = Path(input_path)
    out_dir = Path(out_dir)
    out_dir.mkdir(parents=True, exist_ok=True)

    pdf_paths, extract_dir = _collect_pdfs(input_path, out_dir)
    if not pdf_paths:
        raise ValueError("La Pampa: no se encontraron PDFs para procesar.")

    # 2) Clasificar PDFs
    comparativas = [str(p) for p in pdf_paths if _looks_like_comparativa_pdf(str(p))]
    pliegos = [str(p) for p in pdf_paths if _looks_like_pliego_pdf(str(p))]

    if not comparativas and not pliegos:
        raise ValueError("La Pampa: no se detectaron PDFs de COMPARATIVA ni de PLIEGO.")

    # 3) Pairing
    master_rows: List[pd.DataFrame] = []
    procesados = 0
    omitidos = 0
    forced_pairs = 0
    omitidos_sin_pliego = 0
    omitidos_sin_tag = 0
    omitidos_comp_vacia = 0
    omitidos_pliego_vacio = 0

    # Caso clave para tu interfaz: 1 comparativa + 1 pliego -> pair directo
    if len(comparativas) == 1 and len(pliegos) == 1:
        pairs = [(comparativas[0], pliegos[0], "PAIR_UNICO")]
        forced_pairs = 1
    else:
        pairs = []
        for comp in sorted(comparativas):
            expediente = _extract_expediente_from_pdf(comp)
            tag_name = _find_tag_in_filename(comp) or _tag_expediente(expediente)
            if not tag_name or tag_name == "SIN_TAG":
                omitidos += 1
                omitidos_sin_tag += 1
                continue
            pliego_file = _pair_pliego_for_tag(tag_name, pliegos)
            if not pliego_file:
                omitidos += 1
                omitidos_sin_pliego += 1
                continue
            pairs.append((comp, pliego_file, tag_name))

    if not pairs:
        raise ValueError("La Pampa: no se pudo armar ninguna pareja comparativa+pliego.")

    for comp, pliego_file, tag_name in pairs:
        expediente = _extract_expediente_from_pdf(comp)

        df_pl = leer_pliego_pdf(pliego_file)
        if df_pl.empty:
            omitidos += 1
            omitidos_pliego_vacio += 1
            continue

        df_comp = leer_comparativa_pdf(comp)
        if df_comp.empty:
            omitidos += 1
            omitidos_comp_vacia += 1
            continue

        # 🔒 Refuerzo de tipos para merge
        df_pl = df_pl.copy()
        df_comp = df_comp.copy()
        df_pl["ITEM"] = pd.to_numeric(df_pl["ITEM"], errors="coerce").astype("Int64")
        df_comp["ITEM"] = pd.to_numeric(df_comp["ITEM"], errors="coerce").astype("Int64")

        df_pl = df_pl.dropna(subset=["ITEM"]).copy()
        df_comp = df_comp.dropna(subset=["ITEM"]).copy()

        df_pl["ITEM"] = df_pl["ITEM"].astype(int)
        df_comp["ITEM"] = df_comp["ITEM"].astype(int)

        df = df_comp.merge(
            df_pl[["ITEM", "CGO", "CANTIDAD", "DESCRIPCION", "UNIDAD"]],
            on="ITEM",
            how="left",
        )

        df["EXPEDIENTE"] = expediente
        df["Tag"] = tag_name
        df["ArchivoComparativa"] = os.path.basename(comp)
        df["ArchivoPliego"] = os.path.basename(pliego_file)

        df["TOTAL"] = df.apply(
            lambda r: (r["PU"] * r["CANTIDAD"]) if pd.notna(r.get("PU")) and pd.notna(r.get("CANTIDAD")) else None,
            axis=1,
        )

        if not df.empty:
            ranked = df.groupby(["ITEM", "Alt"])["PU"].rank(method="dense", ascending=True)
            df["Posicion"] = ranked.mask(df["PU"].isna()).astype("Int64")
        else:
            df["Posicion"] = pd.Series(dtype="Int64")

        cols = [
            "Tag", "EXPEDIENTE",
            "ITEM", "Alt", "CGO", "CANTIDAD", "UNIDAD", "DESCRIPCION",
            "PROVEEDOR", "PU", "TOTAL", "Posicion",
            "ItemCode", "ArchivoComparativa", "ArchivoPliego"
        ]
        for c in cols:
            if c not in df.columns:
                df[c] = None

        df = df[cols].sort_values(["Tag", "ITEM", "Alt", "Posicion", "PROVEEDOR"], na_position="last").reset_index(drop=True)

        master_rows.append(df)
        procesados += 1

    if not master_rows:
        raise ValueError("La Pampa: no se pudo construir el master (no hubo parejas válidas comparativa+pliego).")

    master_df = pd.concat(master_rows, ignore_index=True)

    # 4) Convertir a estándar
    std_df = _build_standard_from_master(master_df)

    # 5) Guardar extras
    try:
        master_xlsx = out_dir / "la_pampa_master.xlsx"
        standar_xlsx = out_dir / "la_pampa_estandar.xlsx"
        with pd.ExcelWriter(master_xlsx, engine="openpyxl") as xw:
            master_df.to_excel(xw, sheet_name="Master", index=False)
        with pd.ExcelWriter(standar_xlsx, engine="openpyxl") as xw:
            std_df.to_excel(xw, sheet_name="Estandar", index=False)
    except Exception:
        pass

    # Debug útil: cuántas filas quedaron sin datos del pliego
    miss_pliego = int(master_df["DESCRIPCION"].isna().sum()) if "DESCRIPCION" in master_df.columns else int(len(master_df))
    miss_cant = int(master_df["CANTIDAD"].isna().sum()) if "CANTIDAD" in master_df.columns else int(len(master_df))

    summary = {
        "platform": "LA_PAMPA",
        "files_total": len(pdf_paths),
        "comparativas": len(comparativas),
        "pliegos": len(pliegos),
        "pairs_processed": procesados,
        "pairs_skipped": omitidos,
        "forced_pairs": forced_pairs,
        "skipped_no_tag": omitidos_sin_tag,
        "skipped_no_pliego_for_tag": omitidos_sin_pliego,
        "skipped_pliego_empty": omitidos_pliego_vacio,
        "skipped_comparativa_empty": omitidos_comp_vacia,
        "rows_standard": int(len(std_df)),
        "rows_master": int(len(master_df)),
        "rows_missing_pliego_desc": miss_pliego,
        "rows_missing_pliego_qty": miss_cant,
    }

    return {"df": std_df, "summary": summary}
