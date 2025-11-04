import re, yaml
from importlib import import_module
from pathlib import Path

def load_registry(path: str | Path) -> list[dict]:
    p = Path(path)
    if not p.exists():
        raise FileNotFoundError(f"registry.yml no encontrado en {p.resolve()}")
    with p.open("r", encoding="utf-8") as f:
        raw = yaml.safe_load(f) or {}
    return raw.get("sources", [])

def pick_handler(meta: dict, registry: list[dict]):
    """
    meta: {"platform","province","buyer","filename","header_sample"...}
    Devuelve (callable, source_id)
    """
    for src in registry:
        w = src.get("when", {})
        ok = True
        if "platform" in w:
            ok &= (meta.get("platform") in w["platform"])
        if "province" in w:
            ok &= (meta.get("province") in w["province"])
        if "buyer_regex" in w:
            ok &= bool(re.search(w["buyer_regex"], meta.get("buyer","")))
        if "filename_regex" in w:
            ok &= bool(re.search(w["filename_regex"], meta.get("filename","")))
        if "header_contains" in w:
            sample = " ".join(meta.get("header_sample", [])).lower()
            ok &= all(h.lower() in sample for h in w["header_contains"])
        if not ok:
            continue
        mod, func = src["handler"].split(":")
        handler = getattr(import_module(mod), func)
        return handler, src.get("id")
    raise ValueError(f"No se encontró handler para meta={meta}")
