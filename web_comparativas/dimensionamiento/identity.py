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

from web_comparativas.models import IS_SQLITE

logger = logging.getLogger("wc.dimensionamiento.identity")

_SIN_DATO = "SIN DATO"

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

    # Mapa visible→key para poblar el summary. Si un cliente_visible cae en >1 entidad,
    # se registra conflicto (no debería pasar) y se elige la de mayor peso.
    visible_to_key: dict[str, int] = {}
    for v, ks in visible_to_keys.items():
        if len(ks) == 1:
            visible_to_key[v] = next(iter(ks))
        else:
            weights = {k: 0 for k in ks}
            for e in entities:
                if e.entidad_key in weights and v in e.forms:
                    weights[e.entidad_key] = e.total_registros
            chosen = max(weights.items(), key=lambda kv: kv[1])[0]
            visible_to_key[v] = chosen
            logger.warning(
                "[DIM][IDENTITY] cliente_visible %r cae en %d entidades %s; asignado a %d",
                v, len(ks), sorted(ks), chosen,
            )

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


def rebuild_client_entities(session: Session, import_run_id: int, *, exclude_test: bool | None = None, commit: bool = True) -> dict[str, int]:
    """Resuelve y PERSISTE: registry + records.cliente_entidad_id + summary (entidad_id/es_cliente).

    Diseñado para correr como paso independiente del finalize de ingesta: NO depende de
    que el rebuild del summary se haya ejecutado; mapea las filas de summary existentes
    por `cliente_visible`.
    """
    result = resolve_entities(session, import_run_id, exclude_test=exclude_test)

    # 1) Registry: reemplazo total para la corrida
    session.execute(
        text("DELETE FROM dimensionamiento_cliente_entidad WHERE import_run_id = :run"),
        {"run": import_run_id},
    )
    now = dt.datetime.utcnow()
    for e in result.entities:
        session.execute(
            text(
                """
                INSERT INTO dimensionamiento_cliente_entidad
                    (import_run_id, entidad_key, es_cliente, nombre_visible, provincia,
                     cuits, n_formas, total_registros, created_at)
                VALUES (:run, :key, :cli, :vis, :prov, :cuits, :nf, :tot, :ts)
                """
            ),
            {
                "run": import_run_id,
                "key": e.entidad_key,
                "cli": bool(e.es_cliente),
                "vis": e.nombre_visible,
                "prov": e.provincia,
                "cuits": json.dumps(e.cuits),
                "nf": len(e.forms),
                "tot": e.total_registros,
                "ts": now,
            },
        )

    # 2) records.cliente_entidad_id
    #    anclas por CUIT
    for cu, k in result.cuit_to_key.items():
        session.execute(
            text(
                "UPDATE dimensionamiento_records SET cliente_entidad_id = :k "
                "WHERE import_run_id = :run AND cuit = :cu"
            ),
            {"k": k, "run": import_run_id, "cu": cu},
        )
    #    huérfanos (filas sin CUIT) por nombre original exacto
    for name, k in result.orphan_name_to_key.items():
        session.execute(
            text(
                "UPDATE dimensionamiento_records SET cliente_entidad_id = :k "
                "WHERE import_run_id = :run "
                "AND (cuit IS NULL OR TRIM(cuit) = '' OR UPPER(TRIM(cuit)) = :sin) "
                "AND COALESCE(cliente_nombre_original, '') = :name"
            ),
            {"k": k, "run": import_run_id, "sin": _SIN_DATO, "name": name},
        )

    # 3) summary por cliente_visible → entidad_key + es_cliente_entidad
    es_cliente_by_key = {e.entidad_key: e.es_cliente for e in result.entities}
    for v, k in result.visible_to_key.items():
        session.execute(
            text(
                "UPDATE dimensionamiento_family_monthly_summary "
                "SET cliente_entidad_id = :k, es_cliente_entidad = :cli "
                "WHERE import_run_id = :run AND cliente_visible = :v"
            ),
            {"k": k, "cli": bool(es_cliente_by_key.get(k, False)), "run": import_run_id, "v": v},
        )

    if commit:
        session.commit()
    logger.info("[DIM][IDENTITY] persistido run=%s stats=%s", import_run_id, result.stats)
    return result.stats
