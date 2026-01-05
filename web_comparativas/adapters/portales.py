from pathlib import Path
import pandas as pd
from openpyxl import load_workbook
import unicodedata, re

BASE_DIR = Path(__file__).resolve().parents[1]  # .../web_comparativas
MARCAS_PATH = BASE_DIR / "marcas.xlsm"

def _norm(s):
    if s is None:
        return ""
    s = str(s)
    s = "".join(c for c in unicodedata.normalize("NFD", s) if unicodedata.category(c) != "Mn")
    s = re.sub(r"\s+", " ", s).strip().lower()
    return s

def _parse_latam(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return None
    if isinstance(x, (int, float)):
        return float(x)
    s = str(x).strip().replace("\xa0", " ").replace(" ", "")
    s = s.replace(".", "").replace(",", ".")
    try:
        return float(s)
    except Exception:
        return None

def _to_int(x):
    if x is None or (isinstance(x, float) and pd.isna(x)):
        return pd.NA
    try:
        return int(float(str(x).replace(",", ".").replace(" ", "")))
    except Exception:
        m = re.search(r"\d+", str(x))
        return int(m.group()) if m else pd.NA

def _cargar_marcas():
    if MARCAS_PATH.exists():
        try:
            df = pd.read_excel(MARCAS_PATH, header=None, usecols=[0])
            return [str(x).upper() for x in df[0].dropna().tolist()]
        except Exception:
            return []
    return []

MARCAS = _cargar_marcas()

def _detectar_marca(texto):
    if not texto:
        return "S/D"
    t = str(texto).upper()
    for m in MARCAS:
        if m in t:
            return m
    return "S/D"

def _find_header_and_provider_rows(df, token="precio unitario", search_rows=(5, 40)):
    tok = _norm(token)
    for r in range(search_rows[0], min(search_rows[1], df.shape[0])):
        row_vals = [_norm(v) for v in df.iloc[r].tolist()]
        if any(v and tok in v for v in row_vals):
            return r, r - 1
    return None, None

def _first_fixed_cols(row_headers):
    fixed = {}
    for j, v in enumerate(row_headers.tolist()):
        t = _norm(v)
        if not t:
            continue
        if "renglon" in t and "Renglón" not in fixed:
            fixed["Renglón"] = j
        elif ("alternativa" in t or "opcion" in t) and "Alternativa" not in fixed:
            fixed["Alternativa"] = j
        elif "codigo" in t and "Código" not in fixed:
            fixed["Código"] = j
        elif ("descripcion" in t or "detalle" in t or "producto" in t) and "Descripción" not in fixed:
            fixed["Descripción"] = j
        elif ("cantidad" in t or "cant." in t) and ("solic" in t or "pedid" in t or ("ofert" not in t and "adjud" not in t)) and "Cantidad solicitada" not in fixed:
            fixed["Cantidad solicitada"] = j
        elif ("unidad" in t or "u. medida" in t or "unid." in t) and "Unidad de medida" not in fixed:
            fixed["Unidad de medida"] = j
    return fixed

def _provider_blocks(df, provider_row_idx, header_row_idx, fixed_cols):
    last_fixed = max(fixed_cols.values()) if fixed_cols else 0
    providers = []
    for j, v in enumerate(df.iloc[provider_row_idx].tolist()):
        if pd.isna(v):
            continue
        name = str(v).strip()
        if not name or _norm(name) in ("", "nan"):
            continue
        if j > last_fixed:
            providers.append((j, name))
    blocks = []
    for idx, (start, name) in enumerate(providers):
        end = providers[idx + 1][0] if idx + 1 < len(providers) else df.shape[1]
        hdrs_norm = [_norm(x) for x in df.iloc[header_row_idx, start:end].tolist()]
        if any(h and "precio unitario" in h for h in hdrs_norm):
            blocks.append((name, start, end))
    return blocks

def _map_cols_in_block(df, header_row_idx, start, end):
    hdrs = [_norm(v) for v in df.iloc[header_row_idx, start:end].tolist()]
    cols = list(range(start, end))
    mapping = {}
    for h, c in zip(hdrs, cols):
        if not h:
            continue
        # Precio unitario
        if any(x in h for x in ("precio unitario", "p. unitario", "imp. unitario", "importe unitario", "precio")):
            # Evitar confusiones con "precio total" si "unitario" no está, pero "precio" solo es arriesgado.
            # Mejor orden: si tiene "unitario" es unitario.
            if "total" not in h:
                mapping["Precio unitario"] = c
        
        # Cantidad ofertada
        # Ojo: en el bloque proveedor suele llamarse "cantidad" o "cantidad ofertada".
        # En fixed cols está "cantidad solicitada".
        if any(x in h for x in ("cantidad ofertada", "cant. ofertada", "cantidad", "cant.")):
            if "solicitada" not in h:
                mapping["Cantidad ofertada"] = c

        # Total
        if any(x in h for x in ("total por rengl", "importe total", "monto total")) or h.startswith("total"):
            mapping["Total por renglón"] = c
        
        # Specs
        if "especificacion" in h or "observacion" in h or "marca" in h or "detalle" in h:
            mapping["Especificación técnica"] = c
    return mapping

def _normalize_core(path: Path, hoja: str | None = None):
    wb = load_workbook(path, data_only=True)
    ws = wb[hoja] if hoja and hoja in wb.sheetnames else wb.active
    data = [[cell for cell in row] for row in ws.iter_rows(values_only=True)]
    df = pd.DataFrame(data)

    header_row_idx, provider_row_idx = _find_header_and_provider_rows(df)
    if header_row_idx is None or provider_row_idx is None:
        return pd.DataFrame(), {"total_offers": 0.0, "awarded": 0.0, "pct_over_awarded": 0.0}

    row_headers = df.iloc[header_row_idx]
    fixed_cols = _first_fixed_cols(row_headers)
    blocks = _provider_blocks(df, provider_row_idx, header_row_idx, fixed_cols)
    if not fixed_cols or not blocks:
        return pd.DataFrame(), {"total_offers": 0.0, "awarded": 0.0, "pct_over_awarded": 0.0}

    out_rows = []
    for r in range(header_row_idx + 1, df.shape[0]):
        row = df.iloc[r]
        rc = fixed_cols.get("Renglón", None)
        if rc is None or pd.isna(row[rc]):
            continue

        base = {}
        for k, i in fixed_cols.items():
            base[k] = row[i] if i < len(row) else None

        for (prov_name, start, end) in blocks:
            mapping = _map_cols_in_block(df, header_row_idx, start, end)
            rec = base.copy()
            rec["Proveedor"] = str(prov_name).strip()

            pu = _parse_latam(row[mapping["Precio unitario"]]) if "Precio unitario" in mapping else None
            co = _parse_latam(row[mapping["Cantidad ofertada"]]) if "Cantidad ofertada" in mapping else None
            tot = _parse_latam(row[mapping["Total por renglón"]]) if "Total por renglón" in mapping else None
            espec = row[mapping["Especificación técnica"]] if "Especificación técnica" in mapping else None

            rec["Precio unitario"] = pu
            rec["Cantidad ofertada"] = co
            rec["Total por renglón"] = tot
            rec["Especificación técnica"] = espec
            rec["Marca"] = _detectar_marca(espec)

            out_rows.append(rec)

    out = pd.DataFrame(out_rows)

    # Convertir a enteros donde aplique
    for col in ["Renglón", "Alternativa"]:
        if col in out.columns:
            out[col] = out[col].apply(_to_int).astype("Int64")

    # Si no hay total por renglón, lo calculamos como pu*cant
    if "Total por renglón" not in out.columns or out["Total por renglón"].isna().all():
        out["Total por renglón"] = (
            pd.to_numeric(out.get("Precio unitario"), errors="coerce").fillna(0) *
            pd.to_numeric(out.get("Cantidad ofertada"), errors="coerce").fillna(0)
        )

    # Posición por renglón
    if "Renglón" in out.columns and "Precio unitario" in out.columns:
        out["Posicion"] = (
            out.groupby(["Renglón"])["Precio unitario"]
               .rank(method="dense", ascending=True)
               .astype("Int64")
        )

    # KPIs
    total_offers = float(pd.to_numeric(out["Total por renglón"], errors="coerce").fillna(0).sum())
    awarded = 0.0
    if "Posicion" in out.columns:
        winners = out[out["Posicion"] == 1]
        awarded = float(pd.to_numeric(winners["Total por renglón"], errors="coerce").fillna(0).sum())

    pct = round(awarded * 100.0 / total_offers, 2) if total_offers > 0 else 0.0

    summary = {
        "total_offers": round(total_offers, 2),
        "awarded": round(awarded, 2),
        "pct_over_awarded": pct,
        # datos para charts
        "chart_suppliers": (out.groupby("Proveedor")["Total por renglón"]
                            .sum().sort_values(ascending=False).head(10)
                            .pipe(lambda s: {"labels": s.index.tolist(), "values": [float(x) for x in s.values]})),
        "chart_positions": (out["Posicion"].value_counts(dropna=True).sort_index()
                            .pipe(lambda s: {"labels": [str(i) for i in s.index.tolist()], "values": s.values.tolist()}))
    }

    # Orden columnas
    ordered = [
        'Proveedor', 'Renglón', 'Alternativa', 'Código', 'Descripción',
        'Cantidad solicitada', 'Unidad de medida',
        'Precio unitario', 'Cantidad ofertada', 'Total por renglón',
        'Especificación técnica', 'Marca', 'Posicion'
    ]
    cols = [c for c in ordered if c in out.columns] + [c for c in out.columns if c not in ordered]
    out = out[cols]

    return out, summary

# ====== INTERFACES PÚBLICAS ======

def normalize_comprar(input_path: Path, metadata: dict, out_dir: Path):
    df, summary = _normalize_core(input_path, hoja="Cuadro comparativo")
    return {"df": df, "summary": summary}

def normalize_bac(input_path: Path, metadata: dict, out_dir: Path):
    # De momento usamos la misma lógica; si el BAC cambia, clonás y ajustás.
    df, summary = _normalize_core(input_path, hoja=None)
    return {"df": df, "summary": summary}

def normalize_pbac(input_path: Path, metadata: dict, out_dir: Path):
    df, summary = _normalize_core(input_path, hoja=None)
    return {"df": df, "summary": summary}
