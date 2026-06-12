"""
Consultas y agregaciones para sell out de laboratorios (Informes de Laboratorio).
Adaptado desde 02 - Informes de Laboratorio/backend/services/laboratorios_service.py.
Usa indicadores_db en lugar del db.py local del módulo original.
"""

import logging
import time
from datetime import date
from typing import Iterable, Optional
import unicodedata

from sqlalchemy import bindparam, text

from web_comparativas.indicadores_db import (
    get_etl_db,
    get_fusion_db,
    _corrida_activa,
    _indicadores_summary_available,
)
from web_comparativas.models import engine

logger = logging.getLogger("wc.indicadores.lab")

CADNEG_VALUES = ("2 - 1", "2 - 2", "2 - 3", "2 - 4", "2 - 5")
# Artículos monitoreados que se incorporan por excepción aunque vengan en otro negocio.
MONITORED_EXTRA_ARTICLES = ("8111612", "8142146", "8134261")

# Consolidación de variantes de laboratorio en su laboratorio matriz.
LABORATORIO_GRUPOS = {
    "ABBOT DIAG TRATAMIENTOS": "ABBOTT",
    "ABBOTT": "ABBOTT",
    "ABBOTT USO COMPASIVO": "ABBOTT",
    "ADIUM INSUL": "ADIUM",
    "ADIUM TE": "ADIUM",
    "AMGEN": "AMGEN",
    "AMGEN USO COMPASIVO": "AMGEN",
    "ASPEN": "ASPEN",
    "ASPEN DISPRO": "ASPEN",
    "BAGO ESP": "BAGO",
    "BAGO INSTITUCIONAL": "BAGO",
    "BIOFACTOR TE": "BIOFACTOR",
    "BIOPAS ROFINA": "BIOPAS",
    "BIOPAS SOLS": "BIOPAS",
    "BOEHRINGER ING.": "BOEHRINGER",
    "BRISTOL MYERS": "BRISTOL",
    "ELEA ETICO": "ELEA",
    "ELEA GRILLA": "ELEA",
    "EVEREX": "EVEREX",
    "EVEREX CRYS": "EVEREX",
    "FERRING": "FERRING",
    "FERRING TA": "FERRING",
    "GOBBI NOVAG": "GOBBI",
    "GOBBI NOVAG MEDPRO": "GOBBI",
    "MERCK SERONO": "MERCK SERONO",
    "MERCK SERONO FERT": "MERCK SERONO",
    "PFIZER INDEP.": "PFIZER",
    "PFIZER INSTITUCIONAL*": "PFIZER",
    "PFIZER USO COMPASIVO": "PFIZER",
    "ROCHE DIAG AIR LIQUIDE": "ROCHE",
    "ROCHE DIAG BIC": "ROCHE",
    "ROCHE DIAG.TE": "ROCHE",
    "ROCHE RX": "ROCHE",
    "SANOFI EX GENZYME": "SANOFI",
    "SANOFI GRILLA": "SANOFI",
    "SANOFI TE": "SANOFI",
    "SANOFI USO COMPASIVO": "SANOFI",
    "SIDUS": "SIDUS",
    "SIDUS LIFESCAN": "SIDUS",
    "VARIFARMA": "VARIFARMA",
    "VARIFARMA ULTRAPHARMA": "VARIFARMA",
}

QUERY_SALES = """
DECLARE @FechaInicio DATE = ?;
DECLARE @FechaFin DATE = ?;

WITH ClientesBase AS (
    SELECT
        codigo,
        fantasia,
        cliente_grupo,
        CASE
            WHEN nombre_grupo = 'SIN GRUPO' OR nombre_grupo IS NULL THEN fantasia
            ELSE nombre_grupo
        END AS NombreCliente
    FROM dbo.clientes
),
VentasBase AS (
    SELECT
        rc.ctacte,
        rc.fecha,
        rc.articulo,
        rc.cadneg,
        rc.cant
    FROM dbo.rentabilidad_cliente rc
    WHERE
        rc.fecha IS NOT NULL
        AND CAST(rc.fecha AS DATE) >= @FechaInicio
        AND CAST(rc.fecha AS DATE) < @FechaFin
        AND (
            LTRIM(RTRIM(rc.cadneg)) IN ('2 - 1', '2 - 2', '2 - 3', '2 - 4', '2 - 5')
            OR CAST(rc.articulo AS VARCHAR(20)) IN ('8111612', '8142146', '8134261')
        )
        {cadneg_filter}
)
SELECT
    cb.cliente_grupo AS Grupo_Cliente,
    cb.NombreCliente AS Nombre_Grupo_Cliente,
    CONVERT(CHAR(7), DATEFROMPARTS(YEAR(vb.fecha), MONTH(vb.fecha), 1), 120) AS mes,
    vb.articulo AS Articulo,
    SUM(CAST(vb.cant AS FLOAT)) AS Unidades
FROM VentasBase vb
LEFT JOIN ClientesBase cb ON vb.ctacte = cb.codigo
WHERE 1 = 1
    {cliente_filter}
GROUP BY
    cb.cliente_grupo,
    cb.NombreCliente,
    DATEFROMPARTS(YEAR(vb.fecha), MONTH(vb.fecha), 1),
    vb.articulo
HAVING SUM(CAST(vb.cant AS FLOAT)) <> 0
ORDER BY mes, Articulo;
"""

QUERY_ARTICLES_BASE = """
SELECT
    codigo AS articulo,
    nombre AS marca,
    lab_nombre AS laboratorio,
    familia
FROM dbo.vsl_art_alfabeta_full
WHERE codigo IN ({placeholders});
"""

QUERY_HEALTH_ETL = """
SELECT TOP 1 fecha, articulo, cant
FROM dbo.rentabilidad_cliente
WHERE fecha IS NOT NULL;
"""

QUERY_HEALTH_FUSION = """
SELECT TOP 1 codigo, nombre, lab_nombre
FROM dbo.vsl_art_alfabeta_full;
"""


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _norm_text(value) -> str:
    return str(value or "").strip()


def normalize_text(value) -> str:
    if not value:
        return ""
    normalized = unicodedata.normalize("NFD", str(value))
    return "".join(c for c in normalized if unicodedata.category(c) != "Mn").lower().strip()


LABORATORIO_GRUPOS_NORM = {normalize_text(key): value for key, value in LABORATORIO_GRUPOS.items()}


def group_laboratorio(value) -> str:
    text = _norm_text(value) or "SIN LABORATORIO"
    return LABORATORIO_GRUPOS_NORM.get(normalize_text(text), text)


def _clean_cliente(value) -> str:
    text = _norm_text(value)
    replacements = [
        ("BANCO PROV. NUEVO", "GRUPO BANCO PROVINCIA"),
        ("PLACONA", "UTA - SAN ANTONIO (GRUPO PLACONA)"),
        ("OSTEL - OS DEL PERSONALLEFONICO", "OSTEL - OS DEL PERSONAL TELEFONICO"),
        ("VTA MOSTRADOR FCIA LIBERTADOR SAR201", "VENTA MOSTRADOR"),
        ("GPO OSMATA", "GRUPO OSMATA"),
    ]
    for old, new in replacements:
        text = text.replace(old, new)
    suffixes = [
        " (T.E)", " TRAT ESPEC", " TE", " - ALTO COSTO", " T.E",
        " (TRAT ESP)", " (TRA.ESP)", " TRAT ESP", " (TRAT. ESP.)",
        " (PROFE)", " -.", " (TRAT.ESP.)", " (TRAT. ESP)",
    ]
    for suffix in suffixes:
        text = text.replace(suffix, "")
    return text.strip()


def _chunk(values: list, size: int = 1800) -> Iterable:
    for i in range(0, len(values), size):
        yield values[i:i + size]


def _build_sales_query(cadneg: Optional[str]) -> tuple:
    params = []
    cadneg_filter = ""
    if cadneg:
        cadneg_filter = "AND LTRIM(RTRIM(rc.cadneg)) = ?"
        params.append(cadneg.strip())
    return QUERY_SALES.format(cadneg_filter=cadneg_filter, cliente_filter=""), params


# ---------------------------------------------------------------------------
# RAMA FLAG ON — lectura desde tablas summary PUBLICADAS (SQLite local / Postgres prod).
# Validada A/B contra el vivo (ventana 2025-06..2026-06: total global idéntico, 0 diffs
# en claves comunes; huérfanos esperables porque cliente_grupo/nombre_cliente quedan
# CONGELADOS al extract — comportamiento aceptado). NO toca la rama OFF.
# ---------------------------------------------------------------------------

def _fetch_sales_summary(desde: date, hasta: date, cadneg: Optional[str] = None) -> list:
    """Rama ON de QUERY_SALES: agrega ind_rentabilidad_lineas con la MISMA lógica —
    SUM(cant) por cliente_grupo × nombre_cliente × mes × articulo, mismo universo
    (cadneg 2-x OR artículos monitoreados), mismo HAVING SUM(cant)<>0 y mismo filtro
    opcional de cadneg. cliente_grupo/nombre_cliente salen TAL CUAL del summary
    (congelados al extract): NO se re-joinea dbo.clientes. Mismo shape que el vivo.
    Lee SOLO la corrida approved activa; sin corrida aprobada devuelve vacío limpio."""
    corrida = _corrida_activa()
    if corrida is None:
        return []
    sql = (
        'SELECT cliente_grupo AS "Grupo_Cliente", '
        'nombre_cliente AS "Nombre_Grupo_Cliente", '
        'substr(CAST(fecha AS VARCHAR(32)), 1, 7) AS mes, '
        'articulo AS "Articulo", '
        'SUM(CAST(cant AS FLOAT)) AS "Unidades" '
        "FROM ind_rentabilidad_lineas "
        "WHERE import_run_id = :corrida "
        "AND fecha >= :desde AND fecha < :hasta "
        "AND (cadneg IN :cadnegs OR articulo IN :extras) "
    )
    params = {
        "corrida": corrida,
        "desde": desde,
        "hasta": hasta,
        "cadnegs": list(CADNEG_VALUES),
        "extras": [int(a) for a in MONITORED_EXTRA_ARTICLES],
    }
    if cadneg:
        sql += "AND LTRIM(RTRIM(cadneg)) = :cad "
        params["cad"] = cadneg.strip()
    sql += (
        "GROUP BY cliente_grupo, nombre_cliente, substr(CAST(fecha AS VARCHAR(32)), 1, 7), articulo "
        "HAVING SUM(CAST(cant AS FLOAT)) <> 0 "
        "ORDER BY mes, articulo"
    )
    stmt = text(sql).bindparams(
        bindparam("cadnegs", expanding=True),
        bindparam("extras", expanding=True),
    )
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(stmt, params)]


def _get_article_map_summary(articulos: list) -> dict:
    """Rama ON de get_article_map: marca/laboratorio/familia desde ind_articulos
    (no desde vsl_art_alfabeta_full en vivo). Mismo shape y mismos fallbacks.
    Lee SOLO la corrida approved activa; sin corrida aprobada devuelve {} limpio."""
    corrida = _corrida_activa()
    if corrida is None:
        return {}
    arts = [int(a) for a in (str(x).strip() for x in articulos) if a.lstrip("-").isdigit()]
    if not arts:
        return {}
    article_map = {}
    stmt = text(
        "SELECT articulo, marca, laboratorio, familia FROM ind_articulos "
        "WHERE import_run_id = :corrida AND articulo IN :arts"
    ).bindparams(bindparam("arts", expanding=True))
    try:
        with engine.connect() as conn:
            for items in _chunk(arts):
                for articulo, marca, laboratorio, familia in conn.execute(stmt, {"corrida": corrida, "arts": items}):
                    article_map[str(int(articulo))] = {
                        "marca": _norm_text(marca) or str(int(articulo)),
                        "laboratorio": _norm_text(laboratorio) or "SIN LABORATORIO",
                        "familia": _norm_text(familia) or "SIN FAMILIA",
                    }
    except Exception:
        pass
    return article_map


def get_health() -> dict:
    result = {"status": "ok", "etl": False, "fusion": False, "error": None}
    try:
        with get_etl_db() as conn:
            cursor = conn.cursor()
            cursor.execute(QUERY_HEALTH_ETL)
            result["etl"] = cursor.fetchone() is not None
    except Exception as exc:
        result["error"] = str(exc)
        result["status"] = "error"
    try:
        with get_fusion_db() as conn:
            cursor = conn.cursor()
            cursor.execute(QUERY_HEALTH_FUSION)
            result["fusion"] = cursor.fetchone() is not None
    except Exception as exc:
        if not result["error"]:
            result["error"] = str(exc)
        result["status"] = "error"
    return result


def get_article_map(articulos: list) -> dict:
    if not articulos:
        return {}
    if _indicadores_summary_available("ind_articulos"):
        return _get_article_map_summary(articulos)
    article_map = {}
    try:
        with get_fusion_db() as conn:
            cursor = conn.cursor()
            for items in _chunk(articulos):
                placeholders = ",".join("?" for _ in items)
                cursor.execute(QUERY_ARTICLES_BASE.format(placeholders=placeholders), items)
                for row in _rows_to_dicts(cursor):
                    article_map[str(row["articulo"])] = {
                        "marca": _norm_text(row.get("marca")) or str(row["articulo"]),
                        "laboratorio": _norm_text(row.get("laboratorio")) or "SIN LABORATORIO",
                        "familia": _norm_text(row.get("familia")) or "SIN FAMILIA",
                    }
    except Exception:
        pass
    return article_map


def get_rows(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    familia: Optional[str] = None,
    cliente: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> list:
    t0 = time.monotonic()
    logger.info("lab get_rows: START desde=%s hasta=%s lab=%s cadneg=%s", desde, hasta, laboratorio or "-", cadneg or "-")

    if _indicadores_summary_available("ind_rentabilidad_lineas"):
        sales_rows = _fetch_sales_summary(desde, hasta, cadneg=cadneg)
    else:
        query, extra_params = _build_sales_query(cadneg=cadneg)
        with get_etl_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [desde, hasta, *extra_params])
            sales_rows = _rows_to_dicts(cursor)

    logger.info("lab get_rows: raw=%d (%.1fs)", len(sales_rows), time.monotonic() - t0)

    articulos = sorted({str(row["Articulo"]) for row in sales_rows if row.get("Articulo") is not None})
    article_map = get_article_map(articulos)
    search_norm = normalize_text(search)
    cliente_norm = normalize_text(cliente)
    lab_norm = normalize_text(laboratorio)
    fam_norm = normalize_text(familia)

    rows = []
    for row in sales_rows:
        articulo = str(row.get("Articulo"))
        article = article_map.get(articulo, {"marca": articulo, "laboratorio": "SIN LABORATORIO", "familia": "SIN FAMILIA"})
        lab_raw = article["laboratorio"]
        lab = group_laboratorio(lab_raw)
        fam = article["familia"]
        marca = article["marca"]
        cliente_limpio = _clean_cliente(row.get("Nombre_Grupo_Cliente")) or "SIN CLIENTE"

        if lab_norm and normalize_text(lab) != lab_norm and normalize_text(lab_raw) != lab_norm:
            continue
        if fam_norm and normalize_text(fam) != fam_norm:
            continue
        if cliente_norm and cliente_norm not in normalize_text(cliente_limpio):
            continue
        if search_norm and search_norm not in normalize_text(f"{marca} {articulo}"):
            continue

        rows.append({
            "grupo": row.get("Grupo_Cliente"),
            "cliente": cliente_limpio,
            "mes": row.get("mes"),
            "articulo": articulo,
            "marca": marca,
            "familia": fam,
            "unidades": float(row.get("Unidades") or 0),
            "laboratorio": lab,
        })

    logger.info("lab get_rows: DONE filtered=%d (%.1fs)", len(rows), time.monotonic() - t0)
    return rows


def _add(acc: dict, key: str, value: float):
    if not key:
        return
    acc[key] = acc.get(key, 0) + value


def _rank(acc: dict) -> list:
    return [
        {"name": key, "value": value}
        for key, value in sorted(acc.items(), key=lambda item: item[1], reverse=True)
    ]


def get_summary(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    familia: Optional[str] = None,
    cliente: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> dict:
    rows = get_rows(desde, hasta, laboratorio=laboratorio, familia=familia, cliente=cliente, search=search, cadneg=cadneg)
    by_month: dict = {}
    by_lab: dict = {}
    by_brand: dict = {}
    by_client: dict = {}
    labs, brands, clients = set(), set(), set()
    total = 0.0

    for row in rows:
        units = float(row["unidades"] or 0)
        total += units
        labs.add(row["laboratorio"])
        brands.add(row["marca"])
        clients.add(row["cliente"])
        _add(by_month, row["mes"], units)
        _add(by_lab, row["laboratorio"], units)
        _add(by_brand, row["marca"], units)
        _add(by_client, row["cliente"], units)

    months = [{"mes": key, "unidades": value} for key, value in sorted(by_month.items())]
    prev = months[-2]["unidades"] if len(months) >= 2 else None
    last = months[-1]["unidades"] if months else None
    variation = (last / prev - 1) if prev and last is not None else None

    return {
        "total_unidades": total,
        "promedio_mensual": total / len(months) if months else 0,
        "variacion_mensual": variation,
        "meses": months,
        "laboratorios": _rank(by_lab),
        "marcas": _rank(by_brand),
        "clientes": _rank(by_client),
        "cantidad_laboratorios": len(labs),
        "cantidad_marcas": len(brands),
        "cantidad_clientes": len(clients),
        "total_registros": len(rows),
    }


def get_detail(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    familia: Optional[str] = None,
    cliente: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> list:
    rows = get_rows(desde, hasta, laboratorio=laboratorio, familia=familia, cliente=cliente, search=search, cadneg=cadneg)
    total = sum(float(row["unidades"] or 0) for row in rows)
    grouped: dict = {}

    for row in rows:
        key = (row["laboratorio"], row["familia"], row["marca"], row["cliente"])
        if key not in grouped:
            grouped[key] = {
                "laboratorio": row["laboratorio"],
                "familia": row["familia"],
                "marca": row["marca"],
                "cliente": row["cliente"],
                "unidades": 0.0,
                "meses_set": set(),
                "mensual": {},
            }
        item = grouped[key]
        units = float(row["unidades"] or 0)
        item["unidades"] += units
        item["meses_set"].add(row["mes"])
        item["mensual"][row["mes"]] = item["mensual"].get(row["mes"], 0) + units

    result = []
    for item in grouped.values():
        result.append({
            "laboratorio": item["laboratorio"],
            "familia": item["familia"],
            "marca": item["marca"],
            "cliente": item["cliente"],
            "unidades": item["unidades"],
            "meses": len(item["meses_set"]),
            "mensual": dict(sorted(item["mensual"].items())),
            "participacion": item["unidades"] / total if total else 0,
        })

    return sorted(result, key=lambda item: item["unidades"], reverse=True)


def get_metadata(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    familia: Optional[str] = None,
    cliente: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> dict:
    rows = get_rows(desde, hasta, laboratorio=laboratorio, familia=familia,
                    cliente=cliente, search=search, cadneg=cadneg)
    return {
        "laboratorios": sorted({row["laboratorio"] for row in rows}),
        "familias": sorted({row["familia"] for row in rows}),
        "clientes": sorted({row["cliente"] for row in rows}),
        "marcas": sorted({row["marca"] for row in rows}),
        "meses": sorted({row["mes"] for row in rows if row["mes"]}),
        "total_registros": len(rows),
    }
