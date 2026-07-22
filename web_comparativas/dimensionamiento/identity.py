"""Resolución de identidad de clientes para Dimensionamiento.

Problema: una misma entidad legal entra por plataformas distintas (BIONEXO / PORTADA /
MEDOX) y la homologación es incompleta a nivel FILA. Contar por `cliente_visible` /
`is_client` (derivados fila-por-fila) hace que la misma entidad se cuente dos veces
(una como cliente homologado, otra como no-cliente original). Ver el módulo de query
para el histórico del bug (374 → 347 → resolución por entidad).

Solución: resolución DETERMINISTA anclada en CUIT (order-independent, auditable):

  PASO 1 — Anclas: cada CUIT != 'SIN DATO' es una entidad.
  PASO 2 — Huérfanos: cada nombre original que aparece en filas SIN CUIT se adjunta a
           un ancla por PRIORIDAD ESTRICTA, deteniéndose en el primer nivel que dé
           EXACTAMENTE un match:
              nivel 1: nombre original idéntico (string exacto) de un ancla
              nivel 2: canon(nombre original) de un ancla
              nivel 3: canon(nombre homologado) de un ancla
           Si un nivel devuelve >1 ancla se registra ambigüedad y se continúa; si
           ningún nivel matchea, el nombre es su propia entidad.
  PASO 3 — es_cliente = TRUE si la entidad tiene ≥1 fila con homologado != 'SIN DATO'.

La prioridad nivel 1 > nivel 3 resuelve el caso Sanatorio Argentino (dos CUITs, mismo
nombre canónico) sin guard de CUIT ni dependencia del orden de iteración.

Números de referencia validados contra la base local (run 7, 364.887 filas):
  Flag test OFF: 256 entidades = 158 Sí + 98 No | 186 anclas · 117 adjuntadas · 70 propias · 0 ambiguas
  Flag test ON:  254 = 157 + 97
"""
from __future__ import annotations

import datetime as dt
import json
import logging
import os
import re
import unicodedata
from collections import defaultdict
from dataclasses import dataclass, field
from typing import Any, Iterable

from sqlalchemy import text
from sqlalchemy.orm import Session

from web_comparativas.models import IS_POSTGRES, IS_SQLITE
from .models import DimensionamientoImportRun

logger = logging.getLogger("wc.dimensionamiento.identity")

_SIN_DATO = "SIN DATO"


def latest_success_run_id(session: Session) -> int | None:
    """Único lookup del run activo (corrida success más reciente). Fuente de verdad
    compartida por el endpoint de estado, el POST y el backfill de arranque."""
    return session.execute(
        text("SELECT id FROM dimensionamiento_import_runs WHERE status = 'success' "
             "ORDER BY finished_at DESC, id DESC LIMIT 1")
    ).scalar_one_or_none()


def _set_unlimited_timeout(session: Session) -> None:
    """SET LOCAL statement_timeout = 0 SOLO para esta transacción (Postgres). El backfill
    es mantenimiento administrativo y no debe morir por el timeout global de 55s. SET LOCAL
    se resetea al commitear, por eso se reaplica en cada fase."""
    if IS_POSTGRES:
        session.execute(text("SET LOCAL statement_timeout = 0"))


def _record_identidad_estado(session: Session, run_id: int, *, ok: bool = False, error: str | None = None) -> None:
    """Persiste en import_run.summary el estado de la última resolución de identidad,
    para que un fallo se vea desde el endpoint de estado sin ir a los logs de Render."""
    try:
        run = session.get(DimensionamientoImportRun, run_id)
        if run is None:
            return
        s = dict(run.summary or {})
        now_iso = dt.datetime.utcnow().isoformat()
        s["identidad_ultimo_intento"] = now_iso
        if error is not None:
            s["identidad_ultimo_error"] = str(error)[:2000]
        if ok:
            s["identidad_ultima_resolucion"] = now_iso
            s["identidad_ultimo_error"] = None
        run.summary = s
        session.commit()
    except Exception:
        logger.exception("[DIM][IDENTITY] no se pudo registrar estado de identidad run=%s", run_id)
        session.rollback()

# Datos de prueba (MEDOX). Se excluyen SOLO si DIM_EXCLUDE_TEST_ENTITIES está activo.
# Se comparan por canon del nombre original. NO se borran filas del dataset.
_TEST_ORIGINAL_NAMES = ("OOSS prueba 1", "OOSS prueba 2", "hospital prueba 1")


def canon(value: str | None) -> str:
    """Canonicaliza un nombre: NFKD→ascii, upper, no-alfanumérico→espacio, colapsa, trim.

    Idéntica en SQLite y Postgres porque se resuelve en Python (no depende de unaccent
    ni REGEXP_REPLACE, que difieren entre motores).
    """
    if value is None:
        return ""
    s = unicodedata.normalize("NFKD", str(value))
    s = "".join(ch for ch in s if not unicodedata.combining(ch))
    s = s.encode("ascii", "ignore").decode("ascii")
    s = re.sub(r"[^A-Za-z0-9]+", " ", s.upper())
    return re.sub(r"\s+", " ", s).strip()


def _is_sin_dato(value: Any) -> bool:
    return value is None or str(value).strip() == "" or str(value).strip().upper() == _SIN_DATO


def test_names_enabled() -> bool:
    return os.getenv("DIM_EXCLUDE_TEST_ENTITIES", "0").strip().lower() in ("1", "true", "yes", "on")


@dataclass
class ClientEntity:
    entidad_key: int
    es_cliente: bool
    nombre_visible: str
    provincia: str | None
    cuits: list[str]
    forms: set[str]
    total_registros: int = 0


@dataclass
class ResolveResult:
    entities: list[ClientEntity]
    cuit_to_key: dict[str, int]          # cuit → entidad_key (anclas)
    orphan_name_to_key: dict[str, int]   # nombre original exacto (fila SIN cuit) → entidad_key
    visible_to_key: dict[str, int]       # cliente_visible (crudo) → entidad_key (para el summary)
    ambiguous: list[dict[str, Any]] = field(default_factory=list)
    stats: dict[str, int] = field(default_factory=dict)


def resolve_entities(session: Session, import_run_id: int, *, exclude_test: bool | None = None) -> ResolveResult:
    """Ejecuta la resolución determinista. Solo lectura; no escribe nada."""
    if exclude_test is None:
        exclude_test = test_names_enabled()
    test_canon = {canon(t) for t in _TEST_ORIGINAL_NAMES}

    rows = session.execute(
        text(
            """
            SELECT cuit, cliente_nombre_homologado, cliente_nombre_original,
                   cliente_visible, provincia, COUNT(*) AS n
            FROM dimensionamiento_records
            WHERE import_run_id = :run
            GROUP BY cuit, cliente_nombre_homologado, cliente_nombre_original,
                     cliente_visible, provincia
            """
        ),
        {"run": import_run_id},
    ).all()

    # PASO 1 — anclas por CUIT / recolección de huérfanos (filas sin CUIT)
    anchor_rows: dict[str, list[tuple]] = defaultdict(list)   # cuit → filas
    orphan_rows: dict[str, list[tuple]] = defaultdict(list)   # nombre original exacto → filas SIN cuit
    for cuit, hom, ori, visible, prov, n in rows:
        o = "" if ori is None else str(ori)
        row = (hom, o, visible, prov, int(n or 0))
        if exclude_test and canon(o) in test_canon:
            continue
        if not _is_sin_dato(cuit):
            anchor_rows[str(cuit).strip()].append(row)
        else:
            orphan_rows[o].append(row)

    # Índices de matching de las anclas
    idx_exact: dict[str, set[str]] = defaultdict(set)
    idx_canon_ori: dict[str, set[str]] = defaultdict(set)
    idx_canon_hom: dict[str, set[str]] = defaultdict(set)
    for cu, rs in anchor_rows.items():
        for hom, o, visible, prov, n in rs:
            idx_exact[o].add(cu)
            if canon(o):
                idx_canon_ori[canon(o)].add(cu)
            if not _is_sin_dato(hom):
                idx_canon_hom[canon(hom)].add(cu)

    # Claves estables: anclas por CUIT (ordenadas), luego propias por nombre (ordenadas).
    # entidad_key = índice determinista → estable entre corridas para los mismos datos.
    cuit_to_key: dict[str, int] = {cu: i for i, cu in enumerate(sorted(anchor_rows))}

    # PASO 2 — adjuntar huérfanos por prioridad estricta
    ambiguous: list[dict[str, Any]] = []
    orphan_name_to_cuit: dict[str, str] = {}
    own_names: list[str] = []
    for name in sorted(orphan_rows):
        target_cuit = None
        for level, idx in ((1, idx_exact), (2, idx_canon_ori), (3, idx_canon_hom)):
            key = name if level == 1 else canon(name)
            matches = idx.get(key, set())
            if len(matches) == 1:
                target_cuit = next(iter(matches))
                break
            if len(matches) > 1:
                ambiguous.append({"nombre": name, "nivel": level, "anclas": sorted(matches)})
        if target_cuit is not None:
            orphan_name_to_cuit[name] = target_cuit
        else:
            own_names.append(name)

    # claves de entidades propias, después de las anclas
    next_key = len(cuit_to_key)
    own_name_to_key: dict[str, int] = {}
    for name in own_names:  # ya ordenado
        own_name_to_key[name] = next_key
        next_key += 1

    # Consolidar filas por entidad
    ent_rows: dict[int, list[tuple]] = defaultdict(list)
    for cu, rs in anchor_rows.items():
        ent_rows[cuit_to_key[cu]].extend(rs)
    orphan_name_to_key: dict[str, int] = {}
    for name, rs in orphan_rows.items():
        if name in orphan_name_to_cuit:
            k = cuit_to_key[orphan_name_to_cuit[name]]
        else:
            k = own_name_to_key[name]
        orphan_name_to_key[name] = k
        ent_rows[k].extend(rs)

    # PASO 3 — clasificación + nombre visible + provincia dominante + mapa visible→key
    entities: list[ClientEntity] = []
    visible_to_keys: dict[str, set[int]] = defaultdict(set)
    key_to_cuits: dict[int, set[str]] = defaultdict(set)
    for cu, k in cuit_to_key.items():
        key_to_cuits[k].add(cu)

    for k, rs in ent_rows.items():
        has_hom = any(not _is_sin_dato(h) for h, o, v, p, n in rs)
        rowsum = sum(n for h, o, v, p, n in rs)
        # nombre visible: homologado más largo (desempate por filas) si cliente;
        # si no, la forma original con más filas.
        if has_hom:
            homs: dict[str, int] = defaultdict(int)
            for h, o, v, p, n in rs:
                if not _is_sin_dato(h):
                    homs[h] += n
            nombre_visible = max(homs.items(), key=lambda kv: (len(kv[0]), kv[1]))[0]
        else:
            oris: dict[str, int] = defaultdict(int)
            for h, o, v, p, n in rs:
                oris[o] += n
            nombre_visible = max(oris.items(), key=lambda kv: (kv[1], kv[0]))[0]
        provs: dict[str, int] = defaultdict(int)
        forms: set[str] = set()
        for h, o, v, p, n in rs:
            if p and not _is_sin_dato(p):
                provs[p] += n
            if v:
                forms.add(v)
                visible_to_keys[v].add(k)
        provincia = max(provs.items(), key=lambda kv: kv[1])[0] if provs else None
        entities.append(
            ClientEntity(
                entidad_key=k,
                es_cliente=has_hom,
                nombre_visible=nombre_visible,
                provincia=provincia,
                cuits=sorted(key_to_cuits.get(k, set())),
                forms=forms,
                total_registros=rowsum,
            )
        )
    entities.sort(key=lambda e: e.entidad_key)

    # Mapa visible→key para poblar el summary.
    # ASSERT PERMANENTE del invariante 1:1 (cliente_visible → UNA sola entidad). A (rebuild
    # con GROUP BY) y C (repair por cliente_visible) dependen de que esto sea una función.
    # Si se rompe, el propagado al summary sería ambiguo: cortamos en vez de adivinar.
    visible_to_key: dict[str, int] = {}
    conflicts = {v: sorted(ks) for v, ks in visible_to_keys.items() if len(ks) > 1}
    if conflicts:
        raise ValueError(
            "[DIM][IDENTITY] INVARIANTE 1:1 ROTO en run=%s: %d cliente_visible mapean a >1 entidad. "
            "A/C no son válidos así. Ejemplos: %s"
            % (import_run_id, len(conflicts), dict(list(conflicts.items())[:5]))
        )
    for v, ks in visible_to_keys.items():
        visible_to_key[v] = next(iter(ks))

    _log_defensive_checks(entities, exclude_test)

    stats = {
        "anclas": len(cuit_to_key),
        "adjuntadas": len(orphan_name_to_cuit),
        "propias": len(own_name_to_key),
        "total": len(entities),
        "si": sum(1 for e in entities if e.es_cliente),
        "no": sum(1 for e in entities if not e.es_cliente),
        "ambiguas": len(ambiguous),
        "filas": sum(e.total_registros for e in entities),
    }
    logger.info("[DIM][IDENTITY] resolución run=%s exclude_test=%s stats=%s", import_run_id, exclude_test, stats)
    if ambiguous:
        logger.warning("[DIM][IDENTITY] %d nombres AMBIGUOS (matchearon >1 ancla): %s", len(ambiguous), ambiguous[:20])

    return ResolveResult(
        entities=entities,
        cuit_to_key=cuit_to_key,
        orphan_name_to_key=orphan_name_to_key,
        visible_to_key=visible_to_key,
        ambiguous=ambiguous,
        stats=stats,
    )


def _log_defensive_checks(entities: list[ClientEntity], exclude_test: bool) -> None:
    """Verificaciones defensivas: truncamiento, encoding, sucursales. Solo loguean."""
    for e in entities:
        # Encoding: '#' suele ser una Ñ corrompida en la fuente. No se corrige.
        if "#" in e.nombre_visible:
            logger.warning("[DIM][IDENTITY] posible encoding roto (# ~ Ñ) en nombre_visible=%r (entidad %d)", e.nombre_visible, e.entidad_key)
        # Sucursales / multi-CUIT: una entidad con >1 CUIT (fusionada por ancla).
        if len(e.cuits) > 1:
            logger.info("[DIM][IDENTITY] entidad %d agrupa %d CUITs (sucursales/multi-CUIT): %s (%r)", e.entidad_key, len(e.cuits), e.cuits, e.nombre_visible)
    # Truncamiento: homologados de 30/40 char que sean prefijo de otro. Solo se
    # fusionan por CUIT (no por prefijo); acá solo se advierte.
    visibles = [(e.nombre_visible, e) for e in entities if e.es_cliente]
    for i, (na, ea) in enumerate(visibles):
        if len(na) in (30, 40):
            for nb, eb in visibles:
                if eb.entidad_key != ea.entidad_key and nb.startswith(na) and len(nb) > len(na):
                    same_cuit = bool(set(ea.cuits) & set(eb.cuits))
                    logger.warning(
                        "[DIM][IDENTITY] nombre truncado %r es prefijo de %r (misma_entidad=%s, cuit_compartido=%s). "
                        "No se fusiona por prefijo; se exige CUIT.",
                        na, nb, ea.entidad_key == eb.entidad_key, same_cuit,
                    )


def persist_records_and_registry(
    session: Session, import_run_id: int, *, exclude_test: bool | None = None, commit: bool = True
) -> dict[str, int]:
    """Resuelve y persiste la parte PESADA: registry + records.cliente_entidad_id.

    SETEADO, no por loop. El resolvedor (Python) YA decidió la asignación por prioridad
    (cuit → exacto → canon(original) → canon(homologado)); acá el SQL SOLO la APLICA:
    los mapeos cuit→entidad y nombre_original→entidad se cargan en tablas TEMP y se aplican
    con 2 `UPDATE ... FROM` (una pasada cada uno) en vez de ~373 sentencias. Esto evita el
    statement_timeout de 55s de Postgres que cancelaba el loop.

    Las TEMP son por-conexión y viven dentro de UNA sola transacción (se crean, pueblan y
    aplican sin commit intermedio), así no cruzan un commit ni el cambio de conexión del
    pool, y dos backfills simultáneos no colisionan. Con commit=True hay commits POR FASE
    (registry, records) para que el progreso parcial sobreviva; con commit=False (finalize)
    todo va en la transacción del llamador.

    NO toca el summary (eso lo hace ensure_entidad_columns_populated / capa A).
    """
    result = resolve_entities(session, import_run_id, exclude_test=exclude_test)
    now = dt.datetime.utcnow()

    # ── Fase 1: registry (reemplazo total, INSERT por lotes) ──
    _set_unlimited_timeout(session)
    session.execute(
        text("DELETE FROM dimensionamiento_cliente_entidad WHERE import_run_id = :run"),
        {"run": import_run_id},
    )
    registry_rows = [
        {
            "run": import_run_id, "key": e.entidad_key, "cli": bool(e.es_cliente),
            "vis": e.nombre_visible, "prov": e.provincia, "cuits": json.dumps(e.cuits),
            "nf": len(e.forms), "tot": e.total_registros, "ts": now,
        }
        for e in result.entities
    ]
    if registry_rows:
        session.execute(
            text(
                "INSERT INTO dimensionamiento_cliente_entidad "
                "(import_run_id, entidad_key, es_cliente, nombre_visible, provincia, "
                " cuits, n_formas, total_registros, created_at) "
                "VALUES (:run, :key, :cli, :vis, :prov, :cuits, :nf, :tot, :ts)"
            ),
            registry_rows,  # executemany
        )
    if commit:
        session.commit()

    # ── Fase 2: records.cliente_entidad_id (SETEADO, TEMP confinadas a esta transacción) ──
    _set_unlimited_timeout(session)
    # DROP IF EXISTS por si una corrida previa dejó residuo en una conexión reutilizada.
    session.execute(text("DROP TABLE IF EXISTS _dim_cuit_map"))
    session.execute(text("DROP TABLE IF EXISTS _dim_ori_map"))
    session.execute(text("CREATE TEMP TABLE _dim_cuit_map (cuit TEXT PRIMARY KEY, eid INTEGER)"))
    session.execute(text("CREATE TEMP TABLE _dim_ori_map (name TEXT PRIMARY KEY, eid INTEGER)"))
    cuit_rows = [{"cuit": cu, "eid": k} for cu, k in result.cuit_to_key.items()]
    ori_rows = [{"name": name, "eid": k} for name, k in result.orphan_name_to_key.items()]
    if cuit_rows:
        session.execute(text("INSERT INTO _dim_cuit_map (cuit, eid) VALUES (:cuit, :eid)"), cuit_rows)
    if ori_rows:
        session.execute(text("INSERT INTO _dim_ori_map (name, eid) VALUES (:name, :eid)"), ori_rows)

    # Anclas por CUIT (una sola pasada). Aplica el mapeo ya resuelto por igualdad de cuit.
    session.execute(
        text(
            "UPDATE dimensionamiento_records SET cliente_entidad_id = _dim_cuit_map.eid "
            "FROM _dim_cuit_map "
            "WHERE dimensionamiento_records.import_run_id = :run "
            "AND dimensionamiento_records.cuit = _dim_cuit_map.cuit"
        ),
        {"run": import_run_id},
    )
    # Huérfanos (filas SIN cuit) por nombre original EXACTO. El mapeo orphan_name_to_key ya
    # codifica la prioridad de 3 niveles del resolvedor; acá solo se aplica por igualdad.
    session.execute(
        text(
            "UPDATE dimensionamiento_records SET cliente_entidad_id = _dim_ori_map.eid "
            "FROM _dim_ori_map "
            "WHERE dimensionamiento_records.import_run_id = :run "
            "AND (dimensionamiento_records.cuit IS NULL OR TRIM(dimensionamiento_records.cuit) = '' "
            "     OR UPPER(TRIM(dimensionamiento_records.cuit)) = :sin) "
            "AND COALESCE(dimensionamiento_records.cliente_nombre_original, '') = _dim_ori_map.name"
        ),
        {"run": import_run_id, "sin": _SIN_DATO},
    )
    session.execute(text("DROP TABLE IF EXISTS _dim_cuit_map"))
    session.execute(text("DROP TABLE IF EXISTS _dim_ori_map"))
    if commit:
        session.commit()

    logger.info("[DIM][IDENTITY] records+registry persistidos (seteado) run=%s stats=%s", import_run_id, result.stats)
    return result.stats


def _count_summary_entidad_null(session: Session, import_run_id: int) -> int:
    return int(
        session.execute(
            text(
                "SELECT COUNT(*) FROM dimensionamiento_family_monthly_summary "
                "WHERE import_run_id = :run AND cliente_entidad_id IS NULL"
            ),
            {"run": import_run_id},
        ).scalar_one()
    )


def ensure_entidad_columns_populated(session: Session, import_run_id: int, *, commit: bool = True) -> int:
    """CAPA C — invariante con auto-reparación del summary.

    Chequeo barato: ¿hay filas de summary del run con cliente_entidad_id NULL? Si no, no
    hace nada. Si sí, REPARA propagando desde records (cliente_visible→cliente_entidad_id,
    invariante 1:1) y desde el registry (entidad_key→es_cliente). NO corre el resolvedor
    completo (records ya está resuelto): son dos UPDATE indexados.

    Debe llamarse DESPUÉS de CADA ruta que puebla el summary: rebuild de arranque, finalize
    de ingesta y finalize del push a prod (incluso cuando el rebuild se saltea porque el
    summary subido por el cliente reconcilia — ese es el caso crítico que A no cubre).

    Devuelve cuántas filas estaban NULL (0 = ya estaba consistente).
    """
    missing = _count_summary_entidad_null(session, import_run_id)
    if missing == 0:
        return 0

    # Sin límite de timeout: el UPDATE toca ~300k filas del summary y en Postgres puede
    # cruzar los 55s globales. SET LOCAL aplica solo a esta transacción.
    _set_unlimited_timeout(session)
    S = "dimensionamiento_family_monthly_summary"
    # a) cliente_entidad_id desde records por (import_run_id, cliente_visible)
    session.execute(
        text(
            f"""
            UPDATE {S}
            SET cliente_entidad_id = (
                SELECT r.cliente_entidad_id FROM dimensionamiento_records r
                WHERE r.import_run_id = {S}.import_run_id
                  AND r.cliente_visible = {S}.cliente_visible
                  AND r.cliente_entidad_id IS NOT NULL
                LIMIT 1
            )
            WHERE import_run_id = :run AND cliente_entidad_id IS NULL
            """
        ),
        {"run": import_run_id},
    )
    # b) es_cliente_entidad desde el registry por entidad_key
    session.execute(
        text(
            f"""
            UPDATE {S}
            SET es_cliente_entidad = (
                SELECT ce.es_cliente FROM dimensionamiento_cliente_entidad ce
                WHERE ce.import_run_id = {S}.import_run_id
                  AND ce.entidad_key = {S}.cliente_entidad_id
                LIMIT 1
            )
            WHERE import_run_id = :run AND cliente_entidad_id IS NOT NULL
              AND es_cliente_entidad IS NULL
            """
        ),
        {"run": import_run_id},
    )
    if commit:
        session.commit()
    still_null = _count_summary_entidad_null(session, import_run_id)
    logger.warning(
        "[DIM][IDENTITY] CAPA C auto-reparó summary run=%s: %d filas estaban en NULL, quedan %d. "
        "Si esto se dispara seguido, hay un cuarto camino que puebla el summary sin entidad.",
        import_run_id, missing, still_null,
    )
    return missing


def rebuild_client_entities(session: Session, import_run_id: int, *, exclude_test: bool | None = None, commit: bool = True) -> dict[str, int]:
    """Backfill completo (para script/one-off): records+registry (pesado) + summary (C)."""
    stats = persist_records_and_registry(session, import_run_id, exclude_test=exclude_test, commit=False)
    ensure_entidad_columns_populated(session, import_run_id, commit=False)
    if commit:
        session.commit()
    return stats
