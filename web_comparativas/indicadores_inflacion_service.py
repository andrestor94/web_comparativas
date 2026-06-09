"""
Lógica de negocio para el cálculo de inflación PVP mensual a mes cerrado.
Adaptado desde 01 - Inflacion/backend/services/inflacion_service.py.
Usa indicadores_db (bridge PowerShell) en lugar de pyodbc directo.

Metodología:
  - Solo entran al indicador los productos COMPARABLES
    (tienen pvp_inicial válido Y pvp_final válido).
  - Indicador principal: SUM(pvp_final) / SUM(pvp_inicial) - 1  (índice de precios)
  - Los productos nuevos (alta del período) NO impactan el indicador.
  - Los productos sin cambio SÍ entran con variación 0%.
"""

import csv
import io
import logging
import ssl
import time
from datetime import date
from typing import Optional
from urllib.request import Request, urlopen

from web_comparativas.indicadores_db import get_db, get_sales_db

logger = logging.getLogger("wc.indicadores.inf")

# ---------------------------------------------------------------------------
# Queries SQL
# ---------------------------------------------------------------------------

QUERY_PRODUCTOS = """
DECLARE @FechaInicio DATE = ?;
DECLARE @FechaFin    DATE = ?;

WITH ProductosTE AS (
    SELECT
        a.codigo  AS articulo,
        MAX(a.descrip) AS descripcion,
        MAX(alf.lab_nombre) AS laboratorio
    FROM dbo.articulos a
    LEFT JOIN dbo.vsl_art_alfabeta alf ON a.codigo = alf.codigo
    WHERE a.unineg = 2
    GROUP BY a.codigo
),
PrecioInicial AS (
    SELECT
        h.articulo,
        h.fecha      AS fecha_inicial,
        h.prepubact  AS pvp_inicial,
        ROW_NUMBER() OVER (PARTITION BY h.articulo ORDER BY h.fecha DESC) AS rn
    FROM dbo.histopre h
    INNER JOIN ProductosTE p ON h.articulo = p.articulo
    WHERE
        CAST(h.fecha AS DATE) <= @FechaInicio
        AND h.prepubact IS NOT NULL
        AND h.prepubact >= 1
),
PrecioFinal AS (
    SELECT
        h.articulo,
        h.fecha      AS fecha_final,
        h.prepubact  AS pvp_final,
        ROW_NUMBER() OVER (PARTITION BY h.articulo ORDER BY h.fecha DESC) AS rn
    FROM dbo.histopre h
    INNER JOIN ProductosTE p ON h.articulo = p.articulo
    WHERE
        CAST(h.fecha AS DATE) <= @FechaFin
        AND h.prepubact IS NOT NULL
        AND h.prepubact >= 1
),
BaseCalculo AS (
    SELECT
        p.articulo,
        p.descripcion,
        p.laboratorio,
        CONVERT(VARCHAR(10), pi.fecha_inicial, 23) AS fecha_inicial,
        pi.pvp_inicial,
        CONVERT(VARCHAR(10), pf.fecha_final, 23)   AS fecha_final,
        pf.pvp_final,
        CASE
            WHEN pi.pvp_inicial IS NULL AND pf.pvp_final IS NOT NULL
                THEN 'ALTA_PERIODO_SIN_PRECIO_INICIAL'
            WHEN pi.pvp_inicial IS NOT NULL AND pf.pvp_final IS NULL
                THEN 'SIN_PRECIO_FINAL'
            WHEN pi.pvp_inicial IS NULL AND pf.pvp_final IS NULL
                THEN 'SIN_PRECIOS'
            WHEN pi.pvp_inicial IS NOT NULL AND pf.pvp_final IS NOT NULL
                AND DATEDIFF(month, pi.fecha_inicial, @FechaInicio) > 8
                THEN 'ALTA_REINCORPORADO'
            WHEN pi.pvp_inicial = pf.pvp_final
                THEN 'COMPARABLE_SIN_CAMBIO'
            WHEN pf.pvp_final > pi.pvp_inicial
                THEN 'COMPARABLE_CON_AUMENTO'
            WHEN pf.pvp_final < pi.pvp_inicial
                THEN 'COMPARABLE_CON_BAJA'
            ELSE 'REVISAR'
        END AS estado_calculo,
        CASE
            WHEN pi.pvp_inicial IS NULL THEN 0
            WHEN pf.pvp_final   IS NULL THEN 0
            WHEN pi.pvp_inicial < 1     THEN 0
            WHEN DATEDIFF(month, pi.fecha_inicial, @FechaInicio) > 8 THEN 0
            ELSE 1
        END AS es_comparable,
        CASE
            WHEN pi.pvp_inicial IS NULL THEN NULL
            WHEN pf.pvp_final   IS NULL THEN NULL
            WHEN pi.pvp_inicial < 1     THEN NULL
            WHEN DATEDIFF(month, pi.fecha_inicial, @FechaInicio) > 8 THEN NULL
            ELSE (CAST(pf.pvp_final AS FLOAT) / CAST(pi.pvp_inicial AS FLOAT)) - 1
        END AS variacion_pvp
    FROM ProductosTE p
    LEFT JOIN PrecioInicial pi ON pi.articulo = p.articulo AND pi.rn = 1
    LEFT JOIN PrecioFinal   pf ON pf.articulo = p.articulo AND pf.rn = 1
)
SELECT * FROM BaseCalculo
ORDER BY es_comparable DESC, variacion_pvp DESC;
"""

QUERY_FACTURACION = """
SELECT
    LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))) AS articulo,
    MAX(LTRIM(RTRIM(cadneg))) AS cadneg,
    SUM(CAST(cant AS FLOAT)) AS unidades,
    SUM(CAST(importe AS FLOAT)) AS facturacion
FROM dbo.rentabililad_x_cliente
WHERE
    fecha IS NOT NULL
    AND CAST(fecha AS DATE) >= ?
    AND CAST(fecha AS DATE) < ?
    {cadneg_filter}
GROUP BY LTRIM(RTRIM(CAST(articulo AS VARCHAR(50))));
"""

QUERY_FACTURACION_HEALTH = """
SELECT TOP 1 fecha, articulo, cant, importe
FROM dbo.rentabililad_x_cliente
WHERE fecha IS NOT NULL;
"""

QUERY_FACTURACION_MENSUAL = """
SELECT
    LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))) AS articulo,
    CONVERT(CHAR(7), fecha, 120) AS mes,
    SUM(CAST(cant AS FLOAT)) AS unidades,
    SUM(CAST(importe AS FLOAT)) AS facturacion
FROM dbo.rentabililad_x_cliente
WHERE
    fecha IS NOT NULL
    AND CAST(fecha AS DATE) >= ?
    AND CAST(fecha AS DATE) < ?
    AND LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))) IN ({placeholders})
GROUP BY LTRIM(RTRIM(CAST(articulo AS VARCHAR(50)))), CONVERT(CHAR(7), fecha, 120)
ORDER BY articulo, mes;
"""

QUERY_HEALTH_FUSION = """
SELECT TOP 1 codigo, descrip FROM dbo.articulos WHERE unineg = 2;
"""

INDEC_IPC_DIVISIONES_CSV = "https://www.indec.gob.ar/ftp/cuadros/economia/serie_ipc_divisiones.csv"

CADNEG_LABELS = {
    "2 - 1": "Alto Costo",
    "2 - 2": "Uso Compasivo",
    "2 - 3": "Diabetes",
    "2 - 4": "Alimentos",
    "2 - 5": "Capita Fija",
}


def _rows_to_dicts(cursor) -> list:
    columns = [col[0] for col in cursor.description]
    return [dict(zip(columns, row)) for row in cursor.fetchall()]


def _parse_float(value):
    if value is None:
        return None
    text = str(value).strip()
    if not text or text.upper() in {"NA", "N/A", "NULL"}:
        return None
    text = text.replace("%", "").replace(" ", "")
    if "," in text:
        text = text.replace(".", "").replace(",", ".")
    try:
        return float(text)
    except ValueError:
        return None


def _norm(value):
    return (value or "").strip().lower()


def _period_key(value):
    text = str(value or "").strip()
    digits = "".join(ch for ch in text if ch.isdigit())
    return digits or text


def _period_int(value):
    key = _period_key(value)
    digits = "".join(ch for ch in str(key) if ch.isdigit())
    if len(digits) >= 6:
        return int(digits[:6])
    return None


def _date_period(d: date):
    return d.year * 100 + d.month if d else None


def _download_text(url: str) -> str:
    request = Request(url, headers={"User-Agent": "Mozilla/5.0"})
    context = ssl.create_default_context()
    with urlopen(request, timeout=30, context=context) as response:
        raw = response.read()
    for encoding in ("utf-8-sig", "latin-1"):
        try:
            return raw.decode(encoding)
        except UnicodeDecodeError:
            continue
    return raw.decode("utf-8", errors="replace")


def _pick_column(fieldnames, candidates):
    normalized = {_norm(name): name for name in fieldnames or []}
    for candidate in candidates:
        if candidate in normalized:
            return normalized[candidate]
    for name in fieldnames or []:
        n = _norm(name)
        if any(candidate in n for candidate in candidates):
            return name
    return None


# ---------------------------------------------------------------------------
# Health
# ---------------------------------------------------------------------------

def get_health() -> dict:
    result = {"status": "ok", "fusion": False, "etl": False, "error": None}
    try:
        with get_db() as conn:
            cursor = conn.cursor()
            cursor.execute(QUERY_HEALTH_FUSION)
            result["fusion"] = cursor.fetchone() is not None
    except Exception as exc:
        result["error"] = str(exc)
        result["status"] = "error"
    try:
        with get_sales_db() as conn:
            cursor = conn.cursor()
            cursor.execute(QUERY_FACTURACION_HEALTH)
            result["etl"] = cursor.fetchone() is not None
    except Exception as exc:
        if not result["error"]:
            result["error"] = str(exc)
        result["status"] = "error"
    return result


# ---------------------------------------------------------------------------
# INDEC IPC
# ---------------------------------------------------------------------------

def get_indec_ipc(desde: Optional[date] = None, hasta: Optional[date] = None) -> dict:
    text = _download_text(INDEC_IPC_DIVISIONES_CSV)
    sample = text[:4096]
    dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
    rows = list(csv.DictReader(io.StringIO(text), dialect=dialect))
    if not rows:
        raise RuntimeError("La serie de IPC de INDEC no devolvió filas.")

    fieldnames = rows[0].keys()
    desc_col = _pick_column(fieldnames, ["descripcion_divisiones", "descripcion", "division"])
    period_col = _pick_column(fieldnames, ["indice_tiempo", "periodo", "fecha", "mes"])
    vm_col = _pick_column(fieldnames, ["v_m_ipc", "variacion mensual", "variacion_inter mensual"])
    region_col = _pick_column(fieldnames, ["region", "region_nombre", "descripcion_region"])

    if not desc_col or not period_col or not vm_col:
        raise RuntimeError("No se pudieron identificar columnas de IPC en la fuente INDEC.")

    def row_matches(row, label):
        desc = _norm(row.get(desc_col))
        if label == "nivel_general":
            return desc == "nivel general" or "nivel general" in desc
        return desc == "salud" or desc.endswith(" salud") or "salud" in desc

    def region_score(row):
        if not region_col:
            return 0
        region = _norm(row.get(region_col))
        if "total nacional" in region or region == "nacional":
            return 2
        if "nacional" in region:
            return 1
        return 0

    def build_item(row, valor=None, meses=None):
        return {
            "periodo": row.get(period_col) if row else None,
            "valor": _parse_float(row.get(vm_col)) if valor is None and row else valor,
            "descripcion": row.get(desc_col) if row else None,
            "region": row.get(region_col) if row and region_col else None,
            "meses": meses,
        }

    def rows_for(label):
        matches = [row for row in rows if row_matches(row, label) and _parse_float(row.get(vm_col)) is not None]
        if not matches:
            return []
        matches.sort(key=lambda row: (_period_key(row.get(period_col)), region_score(row)), reverse=True)
        best_region = region_score(matches[0])
        return [row for row in matches if region_score(row) == best_region]

    def latest_for(label):
        matches = rows_for(label)
        return build_item(matches[0]) if matches else None

    def accumulated_for(label):
        start_period = _date_period(desde)
        end_period = _date_period(hasta)
        matches = [
            row for row in rows_for(label)
            if _period_int(row.get(period_col)) is not None
            and _period_int(row.get(period_col)) >= start_period
            and _period_int(row.get(period_col)) < end_period
        ]
        matches.sort(key=lambda row: _period_int(row.get(period_col)))
        if not matches:
            return None
        factor = 1.0
        for row in matches:
            factor *= 1 + (_parse_float(row.get(vm_col)) / 100)
        last = matches[-1]
        return build_item(last, valor=(factor - 1) * 100, meses=[row.get(period_col) for row in matches])

    use_accumulated = desde is not None and hasta is not None
    nivel_general = accumulated_for("nivel_general") if use_accumulated else latest_for("nivel_general")
    salud = accumulated_for("salud") if use_accumulated else latest_for("salud")

    return {
        "fuente": "INDEC",
        "url": INDEC_IPC_DIVISIONES_CSV,
        "tipo": "acumulado" if use_accumulated else "mensual",
        "fecha_inicio": str(desde) if desde else None,
        "fecha_fin": str(hasta) if hasta else None,
        "nivel_general": nivel_general,
        "salud": salud,
        "periodo": (nivel_general or salud or {}).get("periodo"),
    }


def get_indec_ipc_evolucion(desde: date, hasta: date) -> list:
    text = _download_text(INDEC_IPC_DIVISIONES_CSV)
    sample = text[:4096]
    dialect = csv.Sniffer().sniff(sample, delimiters=";,|\t")
    rows = list(csv.DictReader(io.StringIO(text), dialect=dialect))
    if not rows:
        raise RuntimeError("La serie de IPC de INDEC no devolvió filas.")

    fieldnames = rows[0].keys()
    desc_col = _pick_column(fieldnames, ["descripcion_divisiones", "descripcion", "division"])
    period_col = _pick_column(fieldnames, ["indice_tiempo", "periodo", "fecha", "mes"])
    vm_col = _pick_column(fieldnames, ["v_m_ipc", "variacion mensual", "variacion_inter mensual"])
    region_col = _pick_column(fieldnames, ["region", "region_nombre", "descripcion_region"])

    if not desc_col or not period_col or not vm_col:
        raise RuntimeError("No se pudieron identificar columnas de IPC en la fuente INDEC.")

    start_period = _date_period(desde)
    end_period = _date_period(hasta)

    def row_label(row):
        desc = _norm(row.get(desc_col))
        if desc == "nivel general" or "nivel general" in desc:
            return "nivel_general"
        if desc == "salud" or desc.endswith(" salud") or "salud" in desc:
            return "salud"
        return None

    def region_score(row):
        if not region_col:
            return 0
        region = _norm(row.get(region_col))
        if "total nacional" in region or region == "nacional":
            return 2
        if "nacional" in region:
            return 1
        return 0

    candidates = {}
    for row in rows:
        label = row_label(row)
        period = _period_int(row.get(period_col))
        value = _parse_float(row.get(vm_col))
        if not label or period is None or value is None:
            continue
        if period < start_period or period >= end_period:
            continue
        key = (period, label)
        score = region_score(row)
        current = candidates.get(key)
        if current is None or score > current["score"]:
            candidates[key] = {"score": score, "valor": value}

    periods = sorted({period for period, _ in candidates.keys()})
    return [
        {
            "mes": f"{str(period)[:4]}-{str(period)[4:6]}",
            "ipc_nivel_general": candidates.get((period, "nivel_general"), {}).get("valor"),
            "ipc_salud": candidates.get((period, "salud"), {}).get("valor"),
        }
        for period in periods
    ]


# ---------------------------------------------------------------------------
# Facturación (ponderación)
# ---------------------------------------------------------------------------

def _get_facturacion_por_articulo(desde: date, hasta: date, cadneg: Optional[str] = None) -> dict:
    params = [str(desde), str(hasta)]
    cadneg_filter = ""
    if cadneg:
        cadneg_filter = "AND LTRIM(RTRIM(cadneg)) = ?"
        params.append(cadneg.strip())

    query = QUERY_FACTURACION.format(cadneg_filter=cadneg_filter)
    try:
        with get_sales_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, params)
            rows = _rows_to_dicts(cursor)
    except RuntimeError:
        rows = []

    return {
        str(r["articulo"]).strip(): {
            "unidades": r.get("unidades") or 0,
            "facturacion": r.get("facturacion") or 0,
            "cadneg": (r.get("cadneg") or "").strip() or None,
            "negocio": CADNEG_LABELS.get((r.get("cadneg") or "").strip()),
        }
        for r in rows
    }


def get_facturacion_mensual(desde: date, hasta: date, articulos: list) -> list:
    articulos = [str(a).strip() for a in articulos if str(a).strip()]
    if not articulos:
        return []
    placeholders = ",".join("?" for _ in articulos)
    query = QUERY_FACTURACION_MENSUAL.format(placeholders=placeholders)
    params = [str(desde), str(hasta), *articulos]
    with get_sales_db() as conn:
        cursor = conn.cursor()
        cursor.execute(query, params)
        return _rows_to_dicts(cursor)


# ---------------------------------------------------------------------------
# Productos (detalle)
# ---------------------------------------------------------------------------

def get_productos(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> list:
    t0 = time.monotonic()
    logger.info("inflacion get_productos: START desde=%s hasta=%s", desde, hasta)

    with get_db() as conn:
        cursor = conn.cursor()
        cursor.execute(QUERY_PRODUCTOS, (str(desde), str(hasta)))
        rows = _rows_to_dicts(cursor)

    logger.info("inflacion get_productos: raw=%d (%.1fs)", len(rows), time.monotonic() - t0)

    facturacion = _get_facturacion_por_articulo(desde, hasta, cadneg=cadneg)
    for row in rows:
        ventas = facturacion.get(str(row.get("articulo")).strip(), {})
        row["unidades"] = ventas.get("unidades", 0)
        row["facturacion"] = ventas.get("facturacion", 0)
        row["cadneg"] = ventas.get("cadneg") or cadneg
        row["negocio"] = ventas.get("negocio") or CADNEG_LABELS.get(cadneg or "")
        row["tiene_facturacion"] = 1 if row["facturacion"] and row["facturacion"] > 0 else 0

    if cadneg:
        rows = [r for r in rows if (r.get("facturacion") or 0) > 0]
    if search:
        q = search.upper()
        rows = [r for r in rows if q in (r.get("descripcion") or "").upper()]
    if laboratorio:
        rows = [r for r in rows if (r.get("laboratorio") or "").upper() == laboratorio.upper()]

    logger.info("inflacion get_productos: DONE filtered=%d (%.1fs)", len(rows), time.monotonic() - t0)
    return rows


# ---------------------------------------------------------------------------
# Resumen / KPIs
# ---------------------------------------------------------------------------

def get_resumen(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> dict:
    productos = get_productos(desde, hasta, laboratorio=laboratorio, search=search, cadneg=cadneg)

    comparables = [p for p in productos if p["es_comparable"] == 1]
    productos_facturados = [p for p in productos if (p.get("facturacion") or 0) > 0]
    comparables_facturados = [
        p for p in comparables
        if (p.get("facturacion") or 0) > 0 and p["variacion_pvp"] is not None
    ]

    sum_ini = sum(p["pvp_inicial"] for p in comparables if p["pvp_inicial"])
    sum_fin = sum(p["pvp_final"] for p in comparables if p["pvp_final"])
    inflacion_indice = (sum_fin / sum_ini - 1) if sum_ini else None

    facturacion_total = sum(p.get("facturacion") or 0 for p in productos_facturados)
    unidades_total = sum(p.get("unidades") or 0 for p in productos_facturados)
    facturacion_comparable = sum(p.get("facturacion") or 0 for p in comparables_facturados)
    unidades_comparables = sum(p.get("unidades") or 0 for p in comparables_facturados)
    inflacion_ponderada = (
        sum(p["variacion_pvp"] * (p.get("facturacion") or 0) for p in comparables_facturados)
        / facturacion_comparable
    ) if facturacion_comparable else None
    cobertura_facturacion = (facturacion_comparable / facturacion_total) if facturacion_total else None

    mayor = max(comparables, key=lambda p: p["variacion_pvp"] or 0, default=None)
    mayor_aumento = {
        "articulo": mayor["articulo"],
        "descripcion": mayor["descripcion"],
        "variacion": mayor["variacion_pvp"],
    } if mayor and mayor.get("variacion_pvp") else None

    def count(estado):
        return sum(1 for p in productos if p["estado_calculo"] == estado)

    return {
        "fecha_inicio": str(desde),
        "fecha_fin": str(hasta),
        "inflacion_pvp_indice": inflacion_indice,
        "inflacion_pvp_ponderada_facturacion": inflacion_ponderada,
        "total_productos": len(productos),
        "productos_comparables": len(comparables),
        "productos_con_facturacion": len(productos_facturados),
        "productos_comparables_con_facturacion": len(comparables_facturados),
        "productos_sin_cambio": count("COMPARABLE_SIN_CAMBIO"),
        "productos_con_aumento": count("COMPARABLE_CON_AUMENTO"),
        "productos_con_baja": count("COMPARABLE_CON_BAJA"),
        "productos_alta_periodo": count("ALTA_PERIODO_SIN_PRECIO_INICIAL") + count("ALTA_REINCORPORADO"),
        "productos_sin_precio_final": count("SIN_PRECIO_FINAL"),
        "productos_sin_precios": count("SIN_PRECIOS"),
        "productos_revisar": count("REVISAR"),
        "facturacion_total": facturacion_total,
        "facturacion_comparable": facturacion_comparable,
        "unidades_total": unidades_total,
        "unidades_comparables": unidades_comparables,
        "cobertura_facturacion": cobertura_facturacion,
        "mayor_aumento": mayor_aumento,
    }


# ---------------------------------------------------------------------------
# Ranking por laboratorio
# ---------------------------------------------------------------------------

def get_laboratorios(desde: date, hasta: date, search: Optional[str] = None, cadneg: Optional[str] = None) -> list:
    productos = get_productos(desde, hasta, search=search, cadneg=cadneg)
    comparables = [p for p in productos if p["es_comparable"] == 1]

    labs_data: dict = {}
    for p in comparables:
        lab = p.get("laboratorio") or "SIN LABORATORIO"
        if lab not in labs_data:
            labs_data[lab] = {"sum_ini": 0, "sum_fin": 0, "facturacion": 0,
                               "unidades": 0, "pond_num": 0, "variaciones": [], "count": 0}
        d = labs_data[lab]
        if p["pvp_inicial"]:
            d["sum_ini"] += p["pvp_inicial"]
        if p["pvp_final"]:
            d["sum_fin"] += p["pvp_final"]
        if p["variacion_pvp"] is not None:
            d["variaciones"].append(p["variacion_pvp"])
            fact = p.get("facturacion") or 0
            d["facturacion"] += fact
            d["unidades"] += p.get("unidades") or 0
            d["pond_num"] += p["variacion_pvp"] * fact
        d["count"] += 1

    resultado = []
    for lab, data in labs_data.items():
        inflacion_indice = (data["sum_fin"] / data["sum_ini"] - 1) if data["sum_ini"] > 0 else None
        promedio = (sum(data["variaciones"]) / len(data["variaciones"])) if data["variaciones"] else None
        ponderada = (data["pond_num"] / data["facturacion"]) if data["facturacion"] > 0 else None
        resultado.append({
            "laboratorio": lab,
            "inflacion_indice": inflacion_indice,
            "inflacion_ponderada_facturacion": ponderada,
            "promedio_variacion": promedio,
            "cantidad_comparables": data["count"],
            "facturacion": data["facturacion"],
            "unidades": data["unidades"],
        })

    resultado.sort(key=lambda x: x["inflacion_indice"] or -999, reverse=True)
    return resultado


# ---------------------------------------------------------------------------
# Evolución mensual
# ---------------------------------------------------------------------------

def get_evolucion(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
) -> list:
    resultados = []
    cursor_mes = date(desde.year, desde.month, 1)

    while cursor_mes < hasta:
        if cursor_mes.month == 12:
            siguiente = date(cursor_mes.year + 1, 1, 1)
        else:
            siguiente = date(cursor_mes.year, cursor_mes.month + 1, 1)

        resumen = get_resumen(cursor_mes, siguiente, laboratorio=laboratorio, search=search, cadneg=cadneg)
        resultados.append({
            "mes": cursor_mes.strftime("%Y-%m"),
            "inflacion_pvp_indice": resumen["inflacion_pvp_indice"],
            "inflacion_pvp_ponderada_facturacion": resumen["inflacion_pvp_ponderada_facturacion"],
            "productos_comparables": resumen["productos_comparables"],
            "productos_comparables_con_facturacion": resumen["productos_comparables_con_facturacion"],
            "facturacion_comparable": resumen["facturacion_comparable"],
            "total_productos": resumen["total_productos"],
        })
        cursor_mes = siguiente

    return resultados
