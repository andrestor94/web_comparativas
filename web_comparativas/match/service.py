"""Capa de servicio del módulo Match — SIEMPRE paginada.

Reglas de memoria (críticas):
  - NUNCA se carga el dataset de ~1,5M filas ni se consulta en vivo.
  - Todo sale de tablas compactas (`match_propuestas`) con LIMIT/OFFSET.
  - Nada de SELECT * sin límite sobre tablas grandes.

La "corrida vigente" es la última `match_import_runs` en estado `approved`
(las corridas nuevas nacen `pending_approval` y no pisan la vigente hasta aprobar).
"""
from __future__ import annotations

import datetime as dt
import io
from typing import Any

from sqlalchemy import and_, case, func, or_, select, text
from sqlalchemy.orm import Session

import re
import unicodedata

from web_comparativas.match.models import (
    DECISION_DESCARTADO,
    DECISION_HOMOLOGADO,
    EVENTO_PAPELERA_ELIMINADO,
    MATCH_RUN_APPROVED,
    MatchDemandaDesc,
    MatchHomologacion,
    MatchHomologacionEvento,
    MatchImportRun,
    MatchNegocioMap,
    MatchPropuesta,
)


_PUNCT_RE = re.compile(r"[^a-z0-9]+")


def norm_desc(texto: str | None) -> str:
    """Normalización ÚNICA para descripciones de portal (se usa al construir el resumen
    y al buscar en caliente, así matchean): minúsculas + sin acentos + puntuación→espacio
    + espacios colapsados + trim."""
    if not texto:
        return ""
    s = unicodedata.normalize("NFKD", str(texto))
    s = "".join(c for c in s if not unicodedata.combining(c))
    s = s.lower()
    s = _PUNCT_RE.sub(" ", s)
    return " ".join(s.split())


def ensure_match_demanda_desc(db: Session | None = None) -> dict[str, int]:
    """Crea (si falta) y PUEBLA (si está vacía) `match_demanda_desc`: por cada descripción
    de portal normalizada de `dimensionamiento_records`, renglones=COUNT(*) y
    clientes=COUNT(DISTINCT cliente_visible). Idempotente; pensada para el boot. Lee
    `dimensionamiento_records` (solo lectura), una sola pasada."""
    from web_comparativas.models import SessionLocal, engine

    MatchDemandaDesc.__table__.create(bind=engine, checkfirst=True)
    own = db is None
    if own:
        db = SessionLocal()
    try:
        total = int(db.execute(select(func.count(MatchDemandaDesc.desc_norm))).scalar_one() or 0)
        filled = 0
        if total == 0:
            agg: dict[str, list] = {}  # desc_norm -> [renglones, set(clientes)]
            rows = db.execute(text(
                "SELECT producto_nombre_original, cliente_visible "
                "FROM dimensionamiento_records "
                "WHERE producto_nombre_original IS NOT NULL"
            ))
            for desc, cliente in rows:
                k = norm_desc(desc)
                if not k:
                    continue
                slot = agg.get(k)
                if slot is None:
                    slot = [0, set()]
                    agg[k] = slot
                slot[0] += 1
                if cliente:
                    slot[1].add(cliente)
            batch = [
                {"desc_norm": k, "renglones": v[0], "clientes": len(v[1])}
                for k, v in agg.items()
            ]
            if batch:
                db.bulk_insert_mappings(MatchDemandaDesc, batch)
                db.commit()
                filled = len(batch)
            total = filled
        return {"total": total, "filled": filled}
    finally:
        if own:
            db.close()

# Categoría para los códigos de Match sin entrada en el mapa de negocio.
SIN_CLASIFICAR = "Sin clasificar"


def ensure_negocio_map(db: Session | None = None) -> dict[str, int]:
    """Crea (si falta) y PUEBLA (si está vacía) `match_negocio_map` con un DISTINCT de
    `dimensionamiento_records` (código → negocio/subnegocio). Idempotente; pensada para
    correr al boot. No toca índices ni datos de dimensionamiento_records (solo lectura)."""
    from web_comparativas.models import SessionLocal, engine

    MatchNegocioMap.__table__.create(bind=engine, checkfirst=True)
    own = db is None
    if own:
        db = SessionLocal()
    try:
        total = int(db.execute(select(func.count(MatchNegocioMap.codigo))).scalar_one() or 0)
        filled = 0
        if total == 0:
            rows = db.execute(text(
                "SELECT codigo_articulo, MAX(unidad_negocio), MAX(subunidad_negocio) "
                "FROM dimensionamiento_records "
                "WHERE codigo_articulo IS NOT NULL "
                "GROUP BY codigo_articulo"
            )).all()
            batch = [
                {"codigo": str(c).strip(), "negocio": n, "subnegocio": s}
                for c, n, s in rows if c is not None and str(c).strip()
            ]
            if batch:
                db.bulk_insert_mappings(MatchNegocioMap, batch)
                db.commit()
                filled = len(batch)
            total = filled
        return {"total": total, "filled": filled}
    finally:
        if own:
            db.close()


def match_negocios(db: Session) -> dict[str, Any]:
    """Árbol {negocio: [subnegocios...]} desde match_negocio_map + 'Sin clasificar'."""
    rows = db.execute(
        select(MatchNegocioMap.negocio, MatchNegocioMap.subnegocio).distinct()
    ).all()
    tree: dict[str, set] = {}
    for neg, sub in rows:
        neg = (neg or SIN_CLASIFICAR).strip() or SIN_CLASIFICAR
        sub = (sub or SIN_CLASIFICAR).strip() or SIN_CLASIFICAR
        tree.setdefault(neg, set()).add(sub)
    # 'Sin clasificar' siempre disponible (códigos de Match sin mapear).
    tree.setdefault(SIN_CLASIFICAR, set()).add(SIN_CLASIFICAR)
    out = {neg: sorted(subs) for neg, subs in sorted(tree.items())}
    return {"negocios": out}
from web_comparativas.models import User

# Niveles accionables (A–D). E queda fuera de la cola (sin candidato).
NIVELES = ["A", "B", "C", "D", "E"]

DEFAULT_PAGE_SIZE = 50
MAX_PAGE_SIZE = 200


def latest_approved_run(db: Session) -> MatchImportRun | None:
    """Corrida vigente: última corrida `approved` (no las `pending_approval`)."""
    return db.execute(
        select(MatchImportRun)
        .where(MatchImportRun.status == MATCH_RUN_APPROVED)
        .order_by(MatchImportRun.finished_at.desc(), MatchImportRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


def _resolve_run_id(db: Session, run_id: int | None) -> int | None:
    if run_id is not None:
        return run_id
    latest = latest_approved_run(db)
    return latest.id if latest else None


def _clean_page(page: int) -> int:
    return page if page and page > 0 else 1


def _clean_page_size(page_size: int | None) -> int:
    ps = page_size or DEFAULT_PAGE_SIZE
    return max(1, min(MAX_PAGE_SIZE, ps))


def _like_term(q: str | None) -> str | None:
    """Patrón LIKE case-insensitive y portable (SQLite/PG). None si q vacío."""
    if not q:
        return None
    s = q.strip().lower()
    if not s:
        return None
    # Escapamos comodines del usuario para que '%'/'_' sean literales.
    s = s.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")
    return f"%{s}%"


def listar_articulos(
    db: Session,
    page: int = 1,
    page_size: int = DEFAULT_PAGE_SIZE,
    q: str | None = None,
    nivel: str | None = None,
    run_id: int | None = None,
    negocio: str | None = None,
    subnegocio: str | None = None,
) -> dict[str, Any]:
    """Maestro de artículos Suizo: agrupa `match_propuestas` de la corrida vigente por
    `candidato_codigo`. Devuelve por artículo: candidato_codigo, candidato_descripcion,
    n_descripciones (COUNT), mejor_score (MAX) y mejor_nivel (MIN alfabético: A es mejor).

    SIEMPRE con LIMIT/OFFSET. Soporta búsqueda `q` (sobre descripción o código) y filtro
    por `nivel`.
    """
    page = _clean_page(page)
    page_size = _clean_page_size(page_size)
    rid = _resolve_run_id(db, run_id)

    empty = {
        "run_id": rid,
        "page": page,
        "page_size": page_size,
        "total": 0,
        "total_pages": 0,
        "articulos": [],
    }
    if rid is None:
        return empty

    P = MatchPropuesta
    # Solo filas con candidato real (la cola accionable A–D siempre tiene candidato).
    conds = [
        P.import_run_id == rid,
        P.candidato_codigo.isnot(None),
        func.coalesce(P.candidato_codigo, "") != "",
    ]
    if nivel:
        conds.append(P.nivel_confianza == nivel.strip().upper())

    like = _like_term(q)
    if like is not None:
        conds.append(
            or_(
                func.lower(func.coalesce(P.candidato_descripcion, "")).like(like, escape="\\"),
                func.lower(func.coalesce(P.candidato_codigo, "")).like(like, escape="\\"),
            )
        )

    # Filtro Negocio/Subnegocio vía el MAPA CHICO (match_negocio_map), NO la tabla grande.
    neg = (negocio or "").strip()
    sub = (subnegocio or "").strip()
    M = MatchNegocioMap
    if neg:
        if neg == SIN_CLASIFICAR:
            # Códigos de Match sin entrada en el mapa.
            conds.append(P.candidato_codigo.notin_(select(M.codigo)))
        else:
            mq = select(M.codigo).where(M.negocio == neg)
            if sub and sub != SIN_CLASIFICAR:
                mq = mq.where(M.subnegocio == sub)
            conds.append(P.candidato_codigo.in_(mq))

    # Total de artículos distintos (para la paginación), con los MISMOS filtros.
    total = db.execute(
        select(func.count(func.distinct(P.candidato_codigo))).where(*conds)
    ).scalar_one() or 0

    rows = db.execute(
        select(
            P.candidato_codigo.label("candidato_codigo"),
            func.max(P.candidato_descripcion).label("candidato_descripcion"),
            func.count(P.id).label("n_descripciones"),
            func.max(P.score_mejor).label("mejor_score"),
            func.min(P.nivel_confianza).label("mejor_nivel"),
        )
        .where(*conds)
        .group_by(P.candidato_codigo)
        .order_by(func.max(P.score_mejor).desc(), P.candidato_codigo.asc())
        .limit(page_size)
        .offset((page - 1) * page_size)
    ).all()

    articulos = [
        {
            "candidato_codigo": r.candidato_codigo,
            "candidato_descripcion": r.candidato_descripcion,
            "n_descripciones": int(r.n_descripciones or 0),
            "mejor_score": float(r.mejor_score) if r.mejor_score is not None else None,
            "mejor_nivel": r.mejor_nivel,
            # Fase 2 (vienen del 1,5M): aún no existen → placeholder explícito.
            "plataformas": None,
            "renglones": None,
            "clientes": None,
        }
        for r in rows
    ]

    total = int(total)
    total_pages = (total + page_size - 1) // page_size if total else 0
    return {
        "run_id": rid,
        "page": page,
        "page_size": page_size,
        "total": total,
        "total_pages": total_pages,
        "articulos": articulos,
    }


def detalle_articulo(
    db: Session,
    candidato_codigo: str,
    run_id: int | None = None,
) -> dict[str, Any]:
    """Propuestas (descripciones de portal) de un artículo, ordenadas por score_mejor
    DESC, cada una con su estado de homologación (join por producto_plataforma)."""
    rid = _resolve_run_id(db, run_id)
    base = {
        "run_id": rid,
        "candidato_codigo": candidato_codigo,
        "candidato_descripcion": None,
        "propuestas": [],
    }
    if rid is None or not candidato_codigo:
        return base

    P = MatchPropuesta
    H = MatchHomologacion
    rows = db.execute(
        select(P, H)
        .outerjoin(H, H.producto_plataforma == P.producto_plataforma)
        .where(P.import_run_id == rid, P.candidato_codigo == candidato_codigo)
        .order_by(P.score_mejor.desc(), P.id.asc())
    ).all()

    propuestas = []
    descripcion = None
    for p, h in rows:
        if descripcion is None:
            descripcion = p.candidato_descripcion
        # Las descartadas SALEN del listado (van a la papelera / quedan permanentes).
        # Las homologadas se mantienen marcadas.
        if h is not None and h.decision == DECISION_DESCARTADO:
            continue
        propuestas.append({
            "producto_plataforma": p.producto_plataforma,
            "nivel_confianza": p.nivel_confianza,
            "score_mejor": p.score_mejor,
            "candidato_codigo": p.candidato_codigo,
            "candidato_descripcion": p.candidato_descripcion,
            "score_tfidf": p.score_tfidf,
            "score_fuzzy": p.score_fuzzy,
            "score_pharma": p.score_pharma,
            "homologacion": (
                {
                    "decision": h.decision,
                    "codigo_elegido": h.codigo_elegido,
                    "descripcion_elegida": h.descripcion_elegida,
                    "usuario": h.usuario,
                    "updated_at": h.updated_at.isoformat() if h.updated_at else None,
                }
                if h is not None
                else None
            ),
        })

    # Demanda por DESCRIPCIÓN normalizada: lookup por PK en match_demanda_desc (chica),
    # sin full-scan de dimensionamiento_records. Si no matchea por texto → demanda=None.
    if propuestas:
        norms = {pp["producto_plataforma"]: norm_desc(pp["producto_plataforma"]) for pp in propuestas}
        claves = {v for v in norms.values() if v}
        dmap: dict[str, dict] = {}
        if claves:
            for dn, rg, cl in db.execute(
                select(MatchDemandaDesc.desc_norm, MatchDemandaDesc.renglones, MatchDemandaDesc.clientes)
                .where(MatchDemandaDesc.desc_norm.in_(claves))
            ).all():
                dmap[dn] = {"renglones": int(rg or 0), "clientes": int(cl or 0)}
        for pp in propuestas:
            pp["demanda"] = dmap.get(norms.get(pp["producto_plataforma"], ""))

    base["candidato_descripcion"] = descripcion
    base["propuestas"] = propuestas
    return base


# ── Exportación a Excel (reporte = columnas del Excel de entrada + estado) ────────
# Encabezados EXACTOS del Excel de entrada (hoja "Todos") en su MISMO orden, SOLO las
# columnas que se guardaron en match_propuestas. Las del original NO importadas
# ('aprobado', 'codigo_elegido', 'candidato_1_score') se OMITEN (no inventamos datos).
# Cada par: (encabezado_original_en_el_Excel, atributo en MatchPropuesta).
REPORTE_COLS: list[tuple[str, str]] = [
    ("producto_plataforma", "producto_plataforma"),
    ("nivel_confianza", "nivel_confianza"),
    ("score_mejor", "score_mejor"),
    ("candidato_1_descripcion", "candidato_descripcion"),
    ("candidato_1_codigo", "candidato_codigo"),
    ("candidato_1_score_tfidf", "score_tfidf"),
    ("candidato_1_score_fuzzy", "score_fuzzy"),
    ("candidato_1_score_pharma", "score_pharma"),
]


def exportar_reporte_bytes(
    db: Session, run_id: int | None = None
) -> tuple[io.BytesIO, int | None, int]:
    """Genera EN MEMORIA un .xlsx (una hoja) con TODAS las propuestas de la corrida
    vigente. Las columnas replican el Excel de entrada (mapeo `REPORTE_COLS`) y al final
    suma 2 columnas: 'homologado' y 'descartado' ('Sí'/vacío según la decisión vigente).

    SOLO LECTURA: no escribe en app.db (se puede correr con el server vivo). Una sola
    consulta — LEFT JOIN propuestas↔homologaciones por `producto_plataforma` (único en la
    corrida → no multiplica filas), sin N+1. Writer openpyxl `write_only` (memoria acotada
    para ~64k filas). Devuelve (buffer, run_id, filas_escritas)."""
    from openpyxl import Workbook

    rid = _resolve_run_id(db, run_id)
    bio = io.BytesIO()
    wb = Workbook(write_only=True)
    ws = wb.create_sheet("Reporte")
    ws.append([h for h, _ in REPORTE_COLS] + ["homologado", "descartado"])

    filas = 0
    if rid is not None:
        P = MatchPropuesta
        H = MatchHomologacion
        cols = [getattr(P, attr) for _, attr in REPORTE_COLS]
        stmt = (
            select(*cols, H.decision)
            .outerjoin(H, H.producto_plataforma == P.producto_plataforma)
            .where(P.import_run_id == rid)
            .order_by(P.id.asc())
            .execution_options(yield_per=2000)
        )
        for row in db.execute(stmt):
            decision = row[-1]
            ws.append(
                list(row[:-1])
                + [
                    "Sí" if decision == DECISION_HOMOLOGADO else "",
                    "Sí" if decision == DECISION_DESCARTADO else "",
                ]
            )
            filas += 1

    wb.save(bio)
    bio.seek(0)
    return bio, rid, filas


PAPELERA_VENTANA_HORAS = 24


def match_papelera(db: Session, run_id: int | None = None) -> dict[str, Any]:
    """Papelera de descartados: filas `decision='descartado'` con updated_at en las
    últimas 24h y SIN un evento 'papelera_eliminado' posterior. Calculada (sin esquema
    nuevo). Ordena por más próximo a vencer primero. Incluye horas restantes."""
    rid = _resolve_run_id(db, run_id)
    out: dict[str, Any] = {"run_id": rid, "items": [], "total": 0}
    if rid is None:
        return out

    now = dt.datetime.utcnow()
    corte = now - dt.timedelta(hours=PAPELERA_VENTANA_HORAS)
    H = MatchHomologacion
    P = MatchPropuesta
    EV = MatchHomologacionEvento

    rows = db.execute(
        select(H, P.candidato_descripcion.label("art_desc"), P.nivel_confianza.label("nivel"))
        .outerjoin(P, and_(
            P.import_run_id == rid,
            P.producto_plataforma == H.producto_plataforma,
            P.candidato_codigo == H.codigo_elegido,
        ))
        .where(H.decision == DECISION_DESCARTADO, H.updated_at >= corte)
        .order_by(H.updated_at.asc())  # el más viejo (más cerca de vencer) primero
    ).all()

    items = []
    for h, art_desc, nivel in rows:
        # ¿hay un "eliminar definitivamente" posterior a este descarte? → fuera de papelera.
        elim = db.execute(
            select(func.count(EV.id)).where(
                EV.producto_plataforma == h.producto_plataforma,
                EV.decision == EVENTO_PAPELERA_ELIMINADO,
                EV.created_at >= h.updated_at,
            )
        ).scalar_one() or 0
        if elim:
            continue
        horas_rest = PAPELERA_VENTANA_HORAS - (now - h.updated_at).total_seconds() / 3600.0
        horas_rest = round(max(0.0, horas_rest), 1)
        items.append({
            "producto_plataforma": h.producto_plataforma,
            "candidato_codigo": h.codigo_elegido,
            "candidato_descripcion": art_desc,
            "nivel_confianza": nivel,
            "descartado_at": h.updated_at.isoformat() if h.updated_at else None,
            "horas_restantes": horas_rest,
            "usuario": h.usuario,
        })

    out["items"] = items
    out["total"] = len(items)
    return out


def match_resumen(db: Session, run_id: int | None = None) -> dict[str, Any]:
    """Resumen AGREGADO de la corrida vigente para el encabezado del módulo.

    SIEMPRE agregado (COUNT/GROUP BY), nunca trae filas crudas. Devuelve:
      - run_id
      - total_propuestas  : filas de la corrida (descripciones de portal con candidato).
      - total_articulos   : candidato_codigo distintos de la corrida.
      - total_homologadas : filas en match_homologaciones con decision='homologado'.
      - pct_homologadas   : % de homologadas sobre total_propuestas (0..100, 1 decimal).
      - conteo_por_nivel  : {A,B,C,D,...} contados sobre la corrida vigente.
    """
    rid = _resolve_run_id(db, run_id)
    resumen: dict[str, Any] = {
        "run_id": rid,
        "total_propuestas": 0,
        "total_articulos": 0,
        "total_homologadas": 0,
        "pct_homologadas": 0.0,
        "conteo_por_nivel": {},
    }
    if rid is None:
        return resumen

    P = MatchPropuesta
    cond_real = [
        P.import_run_id == rid,
        P.candidato_codigo.isnot(None),
        func.coalesce(P.candidato_codigo, "") != "",
    ]

    resumen["total_propuestas"] = int(
        db.execute(select(func.count(P.id)).where(*cond_real)).scalar_one() or 0
    )
    resumen["total_articulos"] = int(
        db.execute(
            select(func.count(func.distinct(P.candidato_codigo))).where(*cond_real)
        ).scalar_one() or 0
    )

    nivel_rows = db.execute(
        select(P.nivel_confianza, func.count(P.id))
        .where(*cond_real)
        .group_by(P.nivel_confianza)
    ).all()
    conteo = {
        (n or "").upper(): int(c)
        for n, c in nivel_rows
        if (n or "").upper() in NIVELES
    }
    # Orden estable A..E (solo los presentes).
    resumen["conteo_por_nivel"] = {k: conteo[k] for k in NIVELES if k in conteo}

    H = MatchHomologacion
    resumen["total_homologadas"] = int(
        db.execute(
            select(func.count(H.id)).where(H.decision == DECISION_HOMOLOGADO)
        ).scalar_one() or 0
    )

    tot = resumen["total_propuestas"]
    if tot:
        resumen["pct_homologadas"] = round(resumen["total_homologadas"] * 100.0 / tot, 1)
    return resumen


def _nombre_iniciales(usuario: str, full_name: str | None, name: str | None) -> tuple[str, str]:
    """Devuelve (nombre_corto, iniciales) para mostrar en el ranking.

    Prioriza el nombre real de la tabla users (full_name/name). Si solo hay email
    (ej. admin@suizo.com), deriva un nombre corto e iniciales del local-part.
    """
    fuente = (full_name or name or "").strip()
    if not fuente:
        local = (usuario or "").split("@")[0]
        partes = [p for p in local.replace(".", " ").replace("_", " ").replace("-", " ").split() if p]
        fuente = " ".join(p.capitalize() for p in partes) if partes else (usuario or "—")
    palabras = [p for p in fuente.split() if p]
    if len(palabras) >= 2:
        iniciales = (palabras[0][0] + palabras[1][0]).upper()
    elif palabras:
        iniciales = palabras[0][:2].upper()
    else:
        iniciales = "—"
    return fuente, iniciales


def match_desempeno(
    db: Session,
    run_id: int | None = None,
    usuario: str | None = None,
    limite: int = 8,
) -> dict[str, Any]:
    """Tablero de DESEMPEÑO (agregado puro). Métrica = cantidad de homologaciones
    (decision='homologado') por usuario en la corrida vigente. Sin XP/niveles/rangos.

    Devuelve: desempeno (usuario actual), avance (equipo) y ranking por conteo.
    """
    rid = _resolve_run_id(db, run_id)
    out: dict[str, Any] = {
        "run_id": rid,
        "desempeno": None,
        "avance": {"homologadas": 0, "total": 0, "pct": 0.0},
        "ranking": {"top": [], "yo": None, "total_usuarios": 0},
    }
    cur = (usuario or "").strip().lower()

    # Nombre/iniciales del usuario actual (aunque tenga 0 homologaciones).
    urow = None
    if usuario:
        urow = db.execute(
            select(User.full_name, User.name).where(User.email == usuario)
        ).first()
    nombre_cur, ini_cur = _nombre_iniciales(
        usuario or "", urow.full_name if urow else None, urow.name if urow else None
    )

    hoy = dt.datetime.utcnow().date()
    hoy_str = hoy.isoformat()
    lunes_str = (hoy - dt.timedelta(days=hoy.weekday())).isoformat()

    if rid is None:
        out["desempeno"] = {
            "usuario": usuario, "nombre": nombre_cur, "iniciales": ini_cur,
            "homologadas_total": 0, "hoy": 0, "semana": 0, "posicion": None,
            "total_homologadores": 0, "total_del_anterior": None,
            "faltan_para_anterior": None, "lidera": False,
        }
        return out

    H = MatchHomologacion
    P = MatchPropuesta
    es_hoy = case((func.date(H.updated_at) == hoy_str, 1), else_=0)

    rows = db.execute(
        select(
            H.usuario.label("usuario"),
            func.count(H.id).label("total"),
            func.coalesce(func.sum(es_hoy), 0).label("hoy"),
            func.max(User.full_name).label("full_name"),
            func.max(User.name).label("name"),
        )
        .outerjoin(User, User.email == H.usuario)
        .where(H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO)
        .group_by(H.usuario)
    ).all()

    users = []
    for r in rows:
        nombre, ini = _nombre_iniciales(r.usuario, r.full_name, r.name)
        users.append({
            "usuario": r.usuario, "nombre": nombre, "iniciales": ini,
            "total": int(r.total or 0), "hoy": int(r.hoy or 0),
        })
    users.sort(key=lambda u: (-u["total"], u["nombre"].lower()))

    me = None
    me_idx = None
    for i, u in enumerate(users):
        u["rank"] = i + 1
        u["yo"] = bool(cur) and (u["usuario"] or "").strip().lower() == cur
        if u["yo"]:
            me, me_idx = u, i

    out["ranking"] = {
        "top": users[:max(1, limite)],
        "yo": me,
        "total_usuarios": len(users),
    }

    # Semana ISO actual (lunes→hoy) del usuario actual.
    semana = 0
    if usuario:
        semana = int(db.execute(
            select(func.count(H.id)).where(
                H.usuario == usuario,
                H.decision == DECISION_HOMOLOGADO,
                func.date(H.updated_at) >= lunes_str,
            )
        ).scalar_one() or 0)

    # Bloque de desempeño del usuario actual + competencia con el de arriba.
    homol_total = me["total"] if me else 0
    hoy_cur = me["hoy"] if me else 0
    if me is not None:
        posicion = me["rank"]
        if me_idx == 0:
            lidera = True
            total_anterior = None
            faltan = None
        else:
            lidera = False
            arriba = users[me_idx - 1]
            total_anterior = arriba["total"]
            faltan = total_anterior - homol_total
    else:
        # Sin homologaciones del usuario: queda detrás de todos los que sí tienen.
        posicion = (len(users) + 1) if users else None
        lidera = False
        total_anterior = users[-1]["total"] if users else None
        faltan = total_anterior if total_anterior is not None else None

    out["desempeno"] = {
        "usuario": usuario, "nombre": nombre_cur, "iniciales": ini_cur,
        "homologadas_total": homol_total, "hoy": hoy_cur, "semana": semana,
        "posicion": posicion, "total_homologadores": len(users),
        "total_del_anterior": total_anterior, "faltan_para_anterior": faltan,
        "lidera": lidera,
    }

    # Avance del equipo: homologadas_total / propuestas accionables de la corrida.
    homol_equipo = int(db.execute(
        select(func.count(H.id)).where(H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO)
    ).scalar_one() or 0)
    total_prop = int(db.execute(
        select(func.count(P.id)).where(
            P.import_run_id == rid,
            P.candidato_codigo.isnot(None),
            func.coalesce(P.candidato_codigo, "") != "",
        )
    ).scalar_one() or 0)
    pct = round(homol_equipo * 100.0 / total_prop, 1) if total_prop else 0.0
    out["avance"] = {"homologadas": homol_equipo, "total": total_prop, "pct": pct}

    # Descartadas (equipo y usuario) de la corrida vigente — no afectan el avance.
    descartadas_equipo = int(db.execute(
        select(func.count(H.id)).where(H.import_run_id == rid, H.decision == DECISION_DESCARTADO)
    ).scalar_one() or 0)
    descartadas_usuario = 0
    if usuario:
        descartadas_usuario = int(db.execute(
            select(func.count(H.id)).where(
                H.import_run_id == rid, H.decision == DECISION_DESCARTADO, H.usuario == usuario)
        ).scalar_one() or 0)

    # ── Agregados por nivel (derivados, sin tablas nuevas) ───────────────
    LV = ["A", "B", "C", "D"]

    def _por_nivel(rows):
        d = {k: 0 for k in LV}
        for n, cnt in rows:
            k = (n or "").upper()
            if k in d:
                d[k] = int(cnt or 0)
        return d

    # total por nivel: propuestas accionables de la corrida.
    total_por_nivel = _por_nivel(db.execute(
        select(P.nivel_confianza, func.count(P.id))
        .where(
            P.import_run_id == rid,
            P.candidato_codigo.isnot(None),
            func.coalesce(P.candidato_codigo, "") != "",
        )
        .group_by(P.nivel_confianza)
    ).all())

    # join homologaciones → propuesta homologada (producto + código elegido) para su nivel.
    join_cond = and_(
        P.import_run_id == rid,
        P.producto_plataforma == H.producto_plataforma,
        P.candidato_codigo == H.codigo_elegido,
    )

    homol_equipo_por_nivel = _por_nivel(db.execute(
        select(P.nivel_confianza, func.count(H.id))
        .select_from(H).join(P, join_cond)
        .where(H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO)
        .group_by(P.nivel_confianza)
    ).all())

    tu_por_nivel = {k: 0 for k in LV}
    if usuario:
        tu_por_nivel = _por_nivel(db.execute(
            select(P.nivel_confianza, func.count(H.id))
            .select_from(H).join(P, join_cond)
            .where(H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO, H.usuario == usuario)
            .group_by(P.nivel_confianza)
        ).all())

    pendientes_por_nivel = {k: max(0, total_por_nivel[k] - homol_equipo_por_nivel[k]) for k in LV}
    pendientes_total = max(0, total_prop - homol_equipo)

    # por_nivel estructurado para el tablero: {nivel: {homologadas, total, pendientes}}
    por_nivel = {
        k: {
            "homologadas": homol_equipo_por_nivel[k],
            "total": total_por_nivel[k],
            "pendientes": pendientes_por_nivel[k],
        }
        for k in LV
    }

    # Ritmo del EQUIPO: hoy, semana (lunes→hoy) y últimos 7 días para proyección.
    hace7 = (hoy - dt.timedelta(days=6)).isoformat()
    hoy_equipo = int(db.execute(
        select(func.count(H.id)).where(
            H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO,
            func.date(H.updated_at) == hoy.isoformat())
    ).scalar_one() or 0)
    semana_equipo = int(db.execute(
        select(func.count(H.id)).where(
            H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO,
            func.date(H.updated_at) >= lunes_str)
    ).scalar_one() or 0)
    recientes_7d = int(db.execute(
        select(func.count(H.id)).where(
            H.import_run_id == rid, H.decision == DECISION_HOMOLOGADO,
            func.date(H.updated_at) >= hace7)
    ).scalar_one() or 0)
    ritmo_diario_7d = round(recientes_7d / 7.0, 2)

    # Proyección con guarda de datos bajos (< 5 homologaciones en 7 días → sin número).
    UMBRAL_RITMO = 5
    if recientes_7d < UMBRAL_RITMO or ritmo_diario_7d <= 0 or pendientes_total <= 0:
        proyeccion = None
    else:
        dias = pendientes_total / ritmo_diario_7d
        if dias <= 60:
            proyeccion = f"~{round(dias)} días"
        else:
            meses = dias / 30.0
            proyeccion = f"~{round(meses)} meses" if meses <= 24 else f"~{round(meses / 12.0)} años"

    # Artículos Suizo distintos y homologadores activos.
    articulos = int(db.execute(
        select(func.count(func.distinct(P.candidato_codigo))).where(
            P.import_run_id == rid,
            P.candidato_codigo.isnot(None),
            func.coalesce(P.candidato_codigo, "") != "",
        )
    ).scalar_one() or 0)

    out["desempeno"]["tu_por_nivel"] = tu_por_nivel
    out["desempeno"]["descartadas"] = descartadas_usuario
    out["avance"].update({
        "pendientes": pendientes_total,
        "pendientes_total": pendientes_total,
        "descartadas": descartadas_equipo,
        "por_nivel": por_nivel,
        "total_por_nivel": total_por_nivel,
        "homologadas_equipo_por_nivel": homol_equipo_por_nivel,
        "pendientes_por_nivel": pendientes_por_nivel,
        "hoy": hoy_equipo,
        "semana": semana_equipo,
        "ritmo_diario_7d": ritmo_diario_7d,
        "recientes_7d": recientes_7d,
        "proyeccion": proyeccion,
        "articulos": articulos,
        "homologadores_activos": len(users),
    })
    return out
