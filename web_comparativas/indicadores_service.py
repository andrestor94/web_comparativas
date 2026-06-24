"""
Consultas y agregaciones para el módulo Indicadores Comerciales.
Adaptado de 03 - Rentabilidad Negativa/backend/services/rentabilidad_service.py.
Utiliza indicadores_db en lugar del db.py local del módulo original.
"""

from datetime import date, datetime, timezone
from typing import Iterable, Optional
import hashlib
import logging
import time
import unicodedata

from sqlalchemy import bindparam, text

from web_comparativas.indicadores_db import (
    get_etl_db,
    get_fusion_db,
    _corrida_activa,
    _indicadores_summary_available,
)
from web_comparativas.models import engine

logger = logging.getLogger("wc.indicadores.svc")

# ── Caché en memoria para get_rows() ──────────────────────────────────────────
# Evita que /api/resumen y /api/detalle ejecuten la misma query SQL por separado
# cuando se llaman con los mismos parámetros en un intervalo corto.
_ROW_CACHE: dict = {}
_CACHE_TTL = 90  # segundos


def _cache_key(desde, hasta, laboratorio, familia, cliente, search, cadneg, modo) -> str:
    raw = "|".join([
        str(desde), str(hasta),
        str(laboratorio or ""), str(familia or ""),
        str(cliente or ""), str(search or ""),
        str(cadneg or ""), str(modo),
    ])
    return hashlib.md5(raw.encode()).hexdigest()


def _cache_get(key: str) -> Optional[list]:
    entry = _ROW_CACHE.get(key)
    if entry and (time.monotonic() - entry["ts"]) < _CACHE_TTL:
        return entry["data"]
    return None


def _cache_set(key: str, data: list) -> None:
    _ROW_CACHE[key] = {"data": data, "ts": time.monotonic()}
    # Purga entradas expiradas para no crecer sin límite
    now = time.monotonic()
    stale = [k for k, v in _ROW_CACHE.items() if (now - v["ts"]) > _CACHE_TTL * 2]
    for k in stale:
        _ROW_CACHE.pop(k, None)


CADNEG_VALUES = ("2 - 1", "2 - 2", "2 - 3", "2 - 4", "2 - 5")

# ─────────────────────────────────────────────────────────────────────────────
# RAMA OFF (vivo SQL Server) — FUENTE: dbo.rentabililad_x_cliente.
#
# IMPORTANTE: esta rama (flag INDICADORES_USE_SUMMARY apagado) lee de
# `rentabililad_x_cliente`, que expone `renta1` REAL POR COMPROBANTE (validado por
# negocio contra el Excel). La tabla vieja `rentabilidad_cliente` traía `renta1`
# como un TOTAL agregado repetido por línea (no por transacción) y quedó descartada.
# La utilidad Y el importe salen de esta misma fuente.
#
# GRANO DE TRANSACCIÓN = (ctacte + articulo + cadneg + comprob + letra + terminal +
# numero). `renta1` es CONSTANTE dentro de ese grano (verificado: 0 transacciones con
# renta1 múltiple) y VARÍA entre transacciones. cadneg DEBE ir en el grano: un mismo
# comprobante puede tener 2 cadneg con renta1 distinto. Por eso:
#   • utilidad de la transacción = renta1 ÚNICO  →  MIN(renta1) (NO SUM).
#   • unidades/facturación = SUM(cant)/SUM(importe) de las líneas físicas de la transacción.
# Filtro de negatividad a nivel transacción (renta1 < 0).
#
# CTE compartido (TxBase): detalle = 1 fila por transacción; agrupado = SUMA de las
# utilidades por transacción por (mes+cliente+articulo+negocio). Σagrupado == Σdetalle
# por construcción. NOTA: la rama ON/summary (_RENTNEG_*_SUMMARY_SQL) NO se tocó y
# sigue leyendo la fuente vieja agregada — queda incorrecta hasta el rework de ETL.
# ─────────────────────────────────────────────────────────────────────────────
_TX_CTE_OFF = """
ClientesBase AS (
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
TxBase AS (
    SELECT
        rc.ctacte,
        rc.articulo,
        LTRIM(RTRIM(rc.cadneg)) AS cadneg,
        MIN(rc.fecha) AS fecha,
        SUM(CAST(ISNULL(rc.cant, 0) AS FLOAT))    AS Unidades,
        SUM(CAST(ISNULL(rc.importe, 0) AS FLOAT)) AS Facturacion,
        MIN(CAST(rc.renta1 AS FLOAT))             AS Utilidad
    FROM dbo.rentabililad_x_cliente rc
    WHERE
        rc.fecha IS NOT NULL
        AND CAST(rc.fecha AS DATE) >= @FechaInicio
        AND CAST(rc.fecha AS DATE) < @FechaFin
        AND UPPER(LTRIM(RTRIM(ISNULL(rc.comprob, '')))) <> 'NC'
        AND LTRIM(RTRIM(rc.cadneg)) IN ('2 - 1', '2 - 2', '2 - 3', '2 - 4', '2 - 5')
        AND TRY_CONVERT(FLOAT, rc.renta1) < 0
        {cadneg_filter}
    GROUP BY
        rc.ctacte,
        rc.articulo,
        LTRIM(RTRIM(rc.cadneg)),
        rc.comprob,
        rc.letra,
        rc.terminal,
        rc.numero
)
"""


QUERY_NEGATIVE_ROWS = """
DECLARE @FechaInicio DATE = ?;
DECLARE @FechaFin DATE = ?;

WITH """ + _TX_CTE_OFF + """
SELECT
    tx.ctacte AS Cliente,
    cb.cliente_grupo AS Cliente_Grupo,
    cb.NombreCliente AS Nombre_Cliente_Grupo,
    tx.fecha,
    tx.articulo AS Articulo,
    tx.cadneg AS Negocio,
    tx.Unidades AS Unidades,
    tx.Facturacion AS Facturacion,
    tx.Utilidad AS Utilidad,
    CASE
        WHEN tx.Facturacion = 0 THEN NULL
        ELSE tx.Utilidad / NULLIF(tx.Facturacion, 0)
    END AS Rentabilidad
FROM TxBase tx
LEFT JOIN ClientesBase cb ON tx.ctacte = cb.codigo
ORDER BY tx.fecha DESC, Utilidad ASC;
"""


QUERY_GROUPED_ROWS = """
DECLARE @FechaInicio DATE = ?;
DECLARE @FechaFin DATE = ?;

WITH """ + _TX_CTE_OFF + """
SELECT
    tx.ctacte AS Cliente,
    cb.cliente_grupo AS Cliente_Grupo,
    cb.NombreCliente AS Nombre_Cliente_Grupo,
    DATEFROMPARTS(YEAR(tx.fecha), MONTH(tx.fecha), 1) AS fecha,
    tx.articulo AS Articulo,
    tx.cadneg AS Negocio,
    SUM(tx.Unidades) AS Unidades,
    SUM(tx.Facturacion) AS Facturacion,
    SUM(tx.Utilidad) AS Utilidad,
    CASE
        WHEN SUM(tx.Facturacion) = 0 THEN NULL
        ELSE SUM(tx.Utilidad) / NULLIF(SUM(tx.Facturacion), 0)
    END AS Rentabilidad
FROM TxBase tx
LEFT JOIN ClientesBase cb ON tx.ctacte = cb.codigo
GROUP BY
    tx.ctacte,
    cb.cliente_grupo,
    cb.NombreCliente,
    DATEFROMPARTS(YEAR(tx.fecha), MONTH(tx.fecha), 1),
    tx.articulo,
    tx.cadneg
ORDER BY fecha DESC, Utilidad ASC;
"""


QUERY_ARTICLES_BASE = """
SELECT
    a.codigo     AS articulo,
    f.marca      AS marca,
    a.descrip    AS descripcion,
    f.lab_nombre AS laboratorio,
    f.monodroga  AS principio_activo,
    f.familia    AS familia
FROM dbo.articulos a
LEFT JOIN dbo.vsl_art_alfabeta_full f ON f.codigo = a.codigo
WHERE a.codigo IN ({placeholders});
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


NEGOCIO_LABELS = {
    "2 - 1": "Alto Costo",
    "2 - 2": "Uso Compasivo",
    "2 - 3": "Diabetes",
    "2 - 4": "Alimentos",
    "2 - 5": "Capita Fija",
}


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


def _date_text(value) -> str:
    if not value:
        return ""
    if hasattr(value, "isoformat"):
        return value.isoformat()[:10]
    text = str(value)
    if text.startswith("/Date("):
        digits = "".join(c for c in text if c.isdigit() or c == "-")
        if digits:
            ms = int(digits)
            return datetime.fromtimestamp(ms / 1000, tz=timezone.utc).date().isoformat()
    return text[:10]


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


def _build_negative_query(cadneg: Optional[str]) -> tuple:
    params = []
    cadneg_filter = ""
    if cadneg:
        cadneg_filter = "AND LTRIM(RTRIM(rc.cadneg)) = ?"
        params.append(cadneg.strip())
    return QUERY_NEGATIVE_ROWS.format(cadneg_filter=cadneg_filter), params


def _build_grouped_query(cadneg: Optional[str]) -> tuple:
    params = []
    cadneg_filter = ""
    if cadneg:
        cadneg_filter = "AND LTRIM(RTRIM(rc.cadneg)) = ?"
        params.append(cadneg.strip())
    return QUERY_GROUPED_ROWS.format(cadneg_filter=cadneg_filter), params


# ---------------------------------------------------------------------------
# RAMA FLAG ON — lectura desde tablas summary PUBLICADAS (SQLite local / Postgres prod).
# SQL idéntico al validado A/B contra el vivo (ventana 2025-06..2026-06: 0 huérfanos,
# 0 diffs en 2048 claves detalle + 792 agrupado, conjuntos SUM(DISTINCT) idénticos).
# cliente_grupo/nombre_cliente van CONGELADOS del summary (no se re-joinea dbo.clientes).
# Nota de tipo: CAST(... AS FLOAT) == CAST(... AS REAL) en SQLite (misma afinidad, igual
# que el script de validación); en PostgreSQL FLOAT es doble precisión (REAL sería float4).
# NO toca la rama OFF.
# ---------------------------------------------------------------------------

# ─────────────────────────────────────────────────────────────────────────────
# RAMA ON / SUMMARY — lee ind_rentabilidad_lineas YA PRE-DEDUPLICADA al grano de
# transacción por el ETL (camino b: ver indicadores_etl_rentabilidad.py). Cada fila
# de la tabla = 1 transacción con renta1 UNA sola vez (cant/importe sumados).
#
# ⚠️ ACOPLAMIENTO ETL↔LECTURA: estas consultas ASUMEN una tabla pre-deduplicada.
# Solo son correctas leyendo una corrida generada por el ETL nuevo (fuente
# rentabililad_x_cliente). NO volver a cargar ind_rentabilidad_lineas con un ETL que
# escriba a nivel línea: el detalle (sin GROUP BY) y el agrupado (SUM(renta1)) sobre
# datos a nivel línea volverían a sobre-contar. ETL y read-SQL se mueven JUNTOS.
# Espejan exactamente la rama OFF (_TX_CTE_OFF): mismo grano, misma utilidad.
# ─────────────────────────────────────────────────────────────────────────────

# DETALLE: la tabla ya trae 1 fila por transacción -> SELECT directo, SIN GROUP BY
# (agrupar y SUM(renta1) volvería a colapsar/sobre-contar transacciones).
_RENTNEG_DETALLE_SUMMARY_SQL = """
SELECT
    ctacte            AS "Cliente",
    cliente_grupo     AS "Cliente_Grupo",
    nombre_cliente    AS "Nombre_Cliente_Grupo",
    fecha,
    articulo          AS "Articulo",
    cadneg            AS "Negocio",
    CAST(COALESCE(cant, 0) AS FLOAT)    AS "Unidades",
    CAST(COALESCE(importe, 0) AS FLOAT) AS "Facturacion",
    CAST(COALESCE(renta1, 0) AS FLOAT)  AS "Utilidad",
    CASE WHEN CAST(COALESCE(importe, 0) AS FLOAT) = 0 THEN NULL
         ELSE CAST(COALESCE(renta1, 0) AS FLOAT) / NULLIF(CAST(importe AS FLOAT), 0)
    END AS "Rentabilidad"
FROM ind_rentabilidad_lineas
WHERE import_run_id = :corrida
  AND fecha >= :desde AND fecha < :hasta
  AND UPPER(LTRIM(RTRIM(COALESCE(comprob, '')))) <> 'NC'
  AND cadneg IN :cadnegs
  AND CAST(renta1 AS FLOAT) < 0
  {cadneg_filter}
ORDER BY fecha DESC, "Utilidad" ASC
"""

# AGRUPADO: SUMA de las utilidades por transacción (renta1 ya una vez por fila) por
# (mes + cliente + articulo + cadneg). SUM(renta1) directo — el viejo SUM(DISTINCT)
# queda ELIMINADO (sub-contaba). Negocio = cadneg (está en el grano), no MIN.
_RENTNEG_AGRUPADO_SUMMARY_SQL = """
SELECT
    ctacte            AS "Cliente",
    cliente_grupo     AS "Cliente_Grupo",
    nombre_cliente    AS "Nombre_Cliente_Grupo",
    substr(CAST(fecha AS VARCHAR(32)), 1, 7) || '-01' AS fecha,
    articulo          AS "Articulo",
    cadneg            AS "Negocio",
    SUM(CAST(COALESCE(cant, 0) AS FLOAT))    AS "Unidades",
    SUM(CAST(COALESCE(importe, 0) AS FLOAT)) AS "Facturacion",
    SUM(CAST(COALESCE(renta1, 0) AS FLOAT))  AS "Utilidad",
    CASE WHEN SUM(CAST(COALESCE(importe, 0) AS FLOAT)) = 0 THEN NULL
         ELSE SUM(CAST(COALESCE(renta1, 0) AS FLOAT)) / NULLIF(SUM(CAST(COALESCE(importe, 0) AS FLOAT)), 0)
    END AS "Rentabilidad"
FROM ind_rentabilidad_lineas
WHERE import_run_id = :corrida
  AND fecha >= :desde AND fecha < :hasta
  AND UPPER(LTRIM(RTRIM(COALESCE(comprob, '')))) <> 'NC'
  AND cadneg IN :cadnegs
  AND CAST(renta1 AS FLOAT) < 0
  {cadneg_filter}
GROUP BY ctacte, cliente_grupo, nombre_cliente, substr(CAST(fecha AS VARCHAR(32)), 1, 7) || '-01', articulo, cadneg
ORDER BY fecha DESC, "Utilidad" ASC
"""


def _fetch_rentneg_summary(desde: date, hasta: date, cadneg: Optional[str], grouped: bool) -> list:
    """Rama ON de QUERY_NEGATIVE_ROWS / QUERY_GROUPED_ROWS sobre ind_rentabilidad_lineas.
    Mismo shape que el vivo (la columna fecha del modo agrupado sale como 'YYYY-MM-01',
    que _date_text deja idéntico al DATEFROMPARTS del vivo). Respeta cadneg_filter.
    Lee SOLO la corrida approved activa; sin corrida aprobada devuelve vacío limpio."""
    corrida = _corrida_activa()
    if corrida is None:
        return []
    sql_tpl = _RENTNEG_AGRUPADO_SUMMARY_SQL if grouped else _RENTNEG_DETALLE_SUMMARY_SQL
    params = {"corrida": corrida, "desde": desde, "hasta": hasta, "cadnegs": list(CADNEG_VALUES)}
    cadneg_filter = ""
    if cadneg:
        cadneg_filter = "AND LTRIM(RTRIM(cadneg)) = :cad"
        params["cad"] = cadneg.strip()
    stmt = text(sql_tpl.format(cadneg_filter=cadneg_filter)).bindparams(
        bindparam("cadnegs", expanding=True)
    )
    with engine.connect() as conn:
        return [dict(r._mapping) for r in conn.execute(stmt, params)]


def _get_article_map_summary(articulos: list) -> dict:
    """Rama ON de get_article_map: atributos desde ind_articulos (no Fusion en vivo).
    Mismo shape y mismos fallbacks que la rama OFF, incluido principio_activo.
    Lee SOLO la corrida approved activa; sin corrida aprobada devuelve {} limpio."""
    corrida = _corrida_activa()
    if corrida is None:
        return {}
    arts = [int(a) for a in (str(x).strip() for x in articulos) if a.lstrip("-").isdigit()]
    if not arts:
        return {}
    article_map = {}
    stmt = text(
        "SELECT articulo, marca, descripcion, laboratorio, principio_activo, familia "
        "FROM ind_articulos WHERE import_run_id = :corrida AND articulo IN :arts"
    ).bindparams(bindparam("arts", expanding=True))
    try:
        with engine.connect() as conn:
            for items in _chunk(arts):
                for articulo, marca, descripcion, laboratorio, principio_activo, familia in conn.execute(stmt, {"corrida": corrida, "arts": items}):
                    article_map[str(int(articulo))] = {
                        # Nombre de producto: SIEMPRE la descripcion del maestro
                        # (dbo.articulos.descrip, presente en el 100% del universo); la marca
                        # queda como fallback (en ~167 articulos traia el nombre del laboratorio
                        # y tapaba la descripcion real) y el codigo solo como ultimo recurso.
                        "marca": _norm_text(descripcion) or _norm_text(marca) or str(int(articulo)),
                        "laboratorio": _norm_text(laboratorio) or "SIN LABORATORIO",
                        "principio_activo": _norm_text(principio_activo) or "",
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
                        # Misma prelación que la rama ON: descripcion del producto SIEMPRE,
                        # marca como fallback, codigo como último recurso.
                        "marca": _norm_text(row.get("descripcion")) or _norm_text(row.get("marca")) or str(row["articulo"]),
                        "laboratorio": _norm_text(row.get("laboratorio")) or "SIN LABORATORIO",
                        "principio_activo": _norm_text(row.get("principio_activo")) or "",
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
    modo: str = "detalle",
) -> list:
    key = _cache_key(desde, hasta, laboratorio, familia, cliente, search, cadneg, modo)
    cached = _cache_get(key)
    if cached is not None:
        logger.debug("get_rows: cache HIT key=%s rows=%d", key[:8], len(cached))
        return cached

    t0 = time.monotonic()
    grouped_mode = modo == "agrupado"
    query, extra_params = _build_grouped_query(cadneg) if grouped_mode else _build_negative_query(cadneg)

    logger.info(
        "get_rows: SQL START modo=%s desde=%s hasta=%s lab=%s fam=%s cli=%s cadneg=%s",
        modo, desde, hasta,
        laboratorio or "-", familia or "-", cliente or "-", cadneg or "-",
    )

    if _indicadores_summary_available("ind_rentabilidad_lineas"):
        raw_rows = _fetch_rentneg_summary(desde, hasta, cadneg=cadneg, grouped=grouped_mode)
    else:
        with get_etl_db() as conn:
            cursor = conn.cursor()
            cursor.execute(query, [desde, hasta, *extra_params])
            raw_rows = _rows_to_dicts(cursor)

    logger.info("get_rows: ETL raw_rows=%d (%.1fs)", len(raw_rows), time.monotonic() - t0)

    articulos = sorted({str(row["Articulo"]) for row in raw_rows if row.get("Articulo") is not None})
    article_map = get_article_map(articulos)
    search_norm = normalize_text(search)
    cliente_norm = normalize_text(cliente)
    lab_norm = normalize_text(laboratorio)
    fam_norm = normalize_text(familia)

    rows = []
    for row in raw_rows:
        articulo = str(row.get("Articulo"))
        article = article_map.get(
            articulo,
            {"marca": articulo, "laboratorio": "SIN LABORATORIO", "principio_activo": "", "familia": "SIN FAMILIA"},
        )
        cliente_limpio = _clean_cliente(row.get("Nombre_Cliente_Grupo")) or "SIN CLIENTE"
        lab = article["laboratorio"]
        fam = article["familia"]
        marca = article["marca"]

        if lab_norm and normalize_text(lab) != lab_norm:
            continue
        if fam_norm and normalize_text(fam) != fam_norm:
            continue
        if cliente_norm and cliente_norm not in normalize_text(cliente_limpio):
            continue
        if search_norm and search_norm not in normalize_text(f"{marca} {articulo} {article['principio_activo']}"):
            continue

        negocio_codigo = _norm_text(row.get("Negocio"))
        fecha_text = _date_text(row.get("fecha"))
        utilidad = float(row.get("Utilidad") or 0)
        facturacion = float(row.get("Facturacion") or 0)

        rows.append({
            "fecha": fecha_text,
            "mes": fecha_text[:7],
            "cliente_codigo": row.get("Cliente"),
            "grupo": row.get("Cliente_Grupo"),
            "cliente": cliente_limpio,
            "articulo": articulo,
            "marca": marca,
            "laboratorio": lab,
            "principio_activo": article["principio_activo"],
            "familia": fam,
            "negocio": NEGOCIO_LABELS.get(negocio_codigo, negocio_codigo or "SIN NEGOCIO"),
            "negocio_codigo": negocio_codigo,
            "unidades": float(row.get("Unidades") or 0),
            "facturacion": facturacion,
            "utilidad": utilidad,
            "rentabilidad": (
                row.get("Rentabilidad")
                if row.get("Rentabilidad") is not None
                else (utilidad / facturacion if facturacion else None)
            ),
            "modo": "agrupado" if grouped_mode else "detalle",
        })

    logger.info("get_rows: DONE filtered_rows=%d total_time=%.1fs", len(rows), time.monotonic() - t0)
    _cache_set(key, rows)
    return rows


def _rank(acc: dict, limit: Optional[int] = None) -> list:
    items = [
        {"name": key, "value": value}
        for key, value in sorted(acc.items(), key=lambda item: item[1])
    ]
    return items[:limit] if limit else items


def _add(acc: dict, key: str, value: float):
    if key:
        acc[key] = acc.get(key, 0.0) + value


def get_summary(
    desde: date,
    hasta: date,
    laboratorio: Optional[str] = None,
    familia: Optional[str] = None,
    cliente: Optional[str] = None,
    search: Optional[str] = None,
    cadneg: Optional[str] = None,
    modo: str = "detalle",
) -> dict:
    rows = get_rows(desde, hasta, laboratorio=laboratorio, familia=familia,
                    cliente=cliente, search=search, cadneg=cadneg, modo=modo)
    by_month: dict = {}
    by_lab: dict = {}
    by_client: dict = {}
    by_brand: dict = {}
    by_negocio: dict = {}
    facturacion_total = 0.0
    utilidad_total = 0.0

    for row in rows:
        loss = float(row["utilidad"] or 0)
        facturacion_total += float(row["facturacion"] or 0)
        utilidad_total += loss
        _add(by_month, row["mes"], loss)
        _add(by_lab, row["laboratorio"], loss)
        _add(by_client, row["cliente"], loss)
        _add(by_brand, row["marca"], loss)
        _add(by_negocio, row["negocio"], loss)

    months = [{"mes": key, "utilidad": value} for key, value in sorted(by_month.items())]
    prev = months[-2]["utilidad"] if len(months) >= 2 else None
    last = months[-1]["utilidad"] if months else None
    variation = (last / prev - 1) if prev and last is not None else None

    return {
        "total_transacciones": len(rows),
        "facturacion_total": facturacion_total,
        "utilidad_total": utilidad_total,
        "rentabilidad_promedio": utilidad_total / facturacion_total if facturacion_total else None,
        "variacion_mensual": variation,
        "meses": months,
        "laboratorios": _rank(by_lab, 12),
        "clientes": _rank(by_client, 12),
        "marcas": _rank(by_brand, 12),
        "negocios": _rank(by_negocio),
        "cantidad_laboratorios": len({row["laboratorio"] for row in rows}),
        "cantidad_marcas": len({row["marca"] for row in rows}),
        "cantidad_clientes": len({row["cliente"] for row in rows}),
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
    modo: str = "detalle",
) -> list:
    return get_rows(desde, hasta, laboratorio=laboratorio, familia=familia,
                    cliente=cliente, search=search, cadneg=cadneg, modo=modo)


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
        "meses": sorted({row["mes"] for row in rows}),
        "total_registros": len(rows),
    }
