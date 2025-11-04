from pathlib import Path
import pandas as pd

def _load_table(path: Path) -> pd.DataFrame:
    ext = path.suffix.lower()
    if ext in [".xlsx", ".xlsm", ".xltx", ".xltm"]:
        return pd.read_excel(path, engine="openpyxl")
    if ext == ".xls":
        return pd.read_excel(path, engine="xlrd")
    if ext == ".xlsb":
        return pd.read_excel(path, engine="pyxlsb")
    if ext == ".csv":
        # ajusta encoding si hace falta
        return pd.read_csv(path, encoding="utf-8", sep=",")
    raise ValueError(f"Extensión no soportada: {ext}")

def normalize(input_path: Path, out_dir: Path) -> dict:
    df = _load_table(input_path)

    out_dir.mkdir(parents=True, exist_ok=True)
    out_xlsx = out_dir / "normalized.xlsx"
    df.to_excel(out_xlsx, index=False)

    total = float(df["Oferta"].sum()) if "Oferta" in df.columns else 0.0
    adj = float(df["Adjudicado"].sum()) if "Adjudicado" in df.columns else 0.0
    by = (df.groupby("Oferentes")["Oferta"].sum().reset_index().to_dict("records")
          if {"Oferentes","Oferta"}.issubset(df.columns) else [])
    pos = df["Posicion"].value_counts().to_dict() if "Posicion" in df.columns else {}

    return {
        "normalized_path": str(out_xlsx),
        "rows": int(len(df)),
        "summary": {"kpis": {"total_ofertas": total, "adjudicado": adj},
                    "by_oferente": by, "positions": pos},
    }
