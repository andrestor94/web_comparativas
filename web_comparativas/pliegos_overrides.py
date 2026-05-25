"""
pliegos_overrides.py
Capa de resolución de valores vigentes para Lectura de Pliegos.

Patrón: valor_vigente = override_activo si existe
                        si no, valor_original_extraído
                        si no, faltante/vacío
"""
from __future__ import annotations

import datetime as dt
import copy
from typing import Any, Dict, List, Optional, Tuple

from sqlalchemy.orm import Session

from web_comparativas.models import (
    PliegoFieldOverride,
    PliegoEditHistory,
    PliegoSolicitud,
    User,
)

# Estado que se asigna a campos editados manualmente
STATUS_EDITADO = "editado_manualmente"
STATUS_VALIDADO = "validado_manualmente"

# Marcadores que indican ausencia de valor (para calcular completitud)
MISSING_MARKERS = {
    "", "-", "--", "n/a", "na", "nan", "none",
    "no encontrado", "no encontrada", "sin dato", "sin datos",
    "sin información", "sin informacion", "faltante", "no aplica",
}


# ---------------------------------------------------------------------------
# Carga de overrides
# ---------------------------------------------------------------------------

def get_overrides_map(db: Session, solicitud_id: int) -> Dict[str, PliegoFieldOverride]:
    """
    Retorna dict keyed por "{entity_type}:{entity_id or 'global'}:{field_key}"
    Solo overrides activos (is_active=True).
    """
    rows = (
        db.query(PliegoFieldOverride)
        .filter(
            PliegoFieldOverride.solicitud_id == solicitud_id,
            PliegoFieldOverride.is_active == True,
        )
        .all()
    )
    result: Dict[str, PliegoFieldOverride] = {}
    for row in rows:
        key = _make_key(row.entity_type, row.entity_id, row.field_key)
        result[key] = row
    return result


def _make_key(entity_type: str, entity_id: Optional[int], field_key: str) -> str:
    eid = str(entity_id) if entity_id is not None else "global"
    return f"{entity_type}:{eid}:{field_key}"


def get_override(
    db: Session,
    solicitud_id: int,
    entity_type: str,
    entity_id: Optional[int],
    field_key: str,
) -> Optional[PliegoFieldOverride]:
    return (
        db.query(PliegoFieldOverride)
        .filter(
            PliegoFieldOverride.solicitud_id == solicitud_id,
            PliegoFieldOverride.entity_type == entity_type,
            PliegoFieldOverride.entity_id == entity_id,
            PliegoFieldOverride.field_key == field_key,
            PliegoFieldOverride.is_active == True,
        )
        .first()
    )


# ---------------------------------------------------------------------------
# Resolución de valor vigente
# ---------------------------------------------------------------------------

def resolve_value(
    overrides_map: Dict[str, PliegoFieldOverride],
    entity_type: str,
    entity_id: Optional[int],
    field_key: str,
    original_value: Any,
) -> Tuple[Any, bool, Optional[PliegoFieldOverride]]:
    """
    Retorna (valor_vigente, is_overridden, override_obj).
    is_overridden=True cuando hay un override activo.
    """
    key = _make_key(entity_type, entity_id, field_key)
    override = overrides_map.get(key)
    if override is not None:
        return override.edited_value, True, override
    return original_value, False, None


def resolve_str(
    overrides_map: Dict[str, PliegoFieldOverride],
    entity_type: str,
    entity_id: Optional[int],
    field_key: str,
    original_value: Any,
) -> str:
    """Versión simplificada que siempre retorna str."""
    val, _, _ = resolve_value(overrides_map, entity_type, entity_id, field_key, original_value)
    if val is None:
        return ""
    return str(val).strip()


# ---------------------------------------------------------------------------
# Aplicar overrides a proceso.datos (dict)
# ---------------------------------------------------------------------------

def apply_proceso_overrides(
    proceso_datos: dict,
    overrides_map: Dict[str, PliegoFieldOverride],
) -> dict:
    """
    Retorna copia del dict proceso.datos con overrides aplicados.
    No modifica el objeto original.
    """
    if not proceso_datos:
        return {}
    result = copy.deepcopy(proceso_datos)
    for key, override in overrides_map.items():
        entity_type, entity_id_str, field_key = key.split(":", 2)
        if entity_type == "proceso":
            result[field_key] = override.edited_value
    return result


def apply_row_overrides(
    entity_type: str,
    entity_id: int,
    row_dict: dict,
    overrides_map: Dict[str, PliegoFieldOverride],
) -> dict:
    """
    Aplica overrides sobre un dict de fila (renglón, garantía, etc.).
    """
    result = dict(row_dict)
    prefix = f"{entity_type}:{entity_id}:"
    for key, override in overrides_map.items():
        if key.startswith(prefix):
            _, _, field_key = key.split(":", 2)
            result[field_key] = override.edited_value
    return result


# ---------------------------------------------------------------------------
# Guardar override
# ---------------------------------------------------------------------------

def save_override(
    db: Session,
    solicitud_id: int,
    entity_type: str,
    entity_id: Optional[int],
    field_key: str,
    field_label: str,
    new_value: str,
    user: User,
    section_key: str = "",
    reason: str = "",
    original_value: str = "",
    original_status: str = "",
) -> PliegoFieldOverride:
    """
    Guarda un override para un campo. Si ya existe uno activo, lo desactiva.
    Siempre crea un registro en PliegoEditHistory.
    Retorna el nuevo override activo.
    """
    now = dt.datetime.now(dt.timezone.utc)

    # Desactivar override anterior
    prev = get_override(db, solicitud_id, entity_type, entity_id, field_key)
    old_value = original_value
    old_status = original_status
    if prev:
        old_value = prev.edited_value or original_value
        old_status = prev.edited_status or original_status
        prev.is_active = False

    new_status = STATUS_EDITADO if new_value.strip() else original_status

    # Crear nuevo override
    override = PliegoFieldOverride(
        solicitud_id=solicitud_id,
        entity_type=entity_type,
        entity_id=entity_id,
        section_key=section_key,
        field_key=field_key,
        field_label=field_label,
        original_value=original_value,
        edited_value=new_value,
        original_status=original_status,
        edited_status=new_status,
        edited_by_user_id=user.id,
        edited_by_name=user.full_name or user.name or user.email,
        edited_by_role=user.role or "usuario",
        edited_at=now,
        reason=reason,
        is_active=True,
    )
    db.add(override)

    # Registrar en historial
    history = PliegoEditHistory(
        solicitud_id=solicitud_id,
        entity_type=entity_type,
        entity_id=entity_id,
        section_key=section_key,
        field_key=field_key,
        field_label=field_label,
        old_value=old_value,
        new_value=new_value,
        old_status=old_status,
        new_status=new_status,
        edited_by_user_id=user.id,
        edited_by_name=user.full_name or user.name or user.email,
        edited_by_role=user.role or "usuario",
        edited_at=now,
        reason=reason,
    )
    db.add(history)
    db.flush()
    return override


def restore_original(
    db: Session,
    solicitud_id: int,
    entity_type: str,
    entity_id: Optional[int],
    field_key: str,
    user: User,
    reason: str = "Restaurado al valor original",
) -> bool:
    """Desactiva el override activo. Retorna True si había uno activo."""
    prev = get_override(db, solicitud_id, entity_type, entity_id, field_key)
    if not prev:
        return False

    now = dt.datetime.now(dt.timezone.utc)

    history = PliegoEditHistory(
        solicitud_id=solicitud_id,
        entity_type=entity_type,
        entity_id=entity_id,
        section_key=prev.section_key,
        field_key=field_key,
        field_label=prev.field_label,
        old_value=prev.edited_value,
        new_value=prev.original_value,
        old_status=prev.edited_status,
        new_status=prev.original_status,
        edited_by_user_id=user.id,
        edited_by_name=user.full_name or user.name or user.email,
        edited_by_role=user.role or "usuario",
        edited_at=now,
        reason=reason,
    )
    db.add(history)
    prev.is_active = False
    db.flush()
    return True


# ---------------------------------------------------------------------------
# Consultas de historial
# ---------------------------------------------------------------------------

def get_edit_history(db: Session, solicitud_id: int) -> List[PliegoEditHistory]:
    return (
        db.query(PliegoEditHistory)
        .filter(PliegoEditHistory.solicitud_id == solicitud_id)
        .order_by(PliegoEditHistory.edited_at.desc())
        .all()
    )


def history_to_dict(h: PliegoEditHistory) -> dict:
    return {
        "id": h.id,
        "entity_type": h.entity_type,
        "entity_id": h.entity_id,
        "section_key": h.section_key,
        "field_key": h.field_key,
        "field_label": h.field_label or h.field_key,
        "old_value": h.old_value,
        "new_value": h.new_value,
        "old_status": h.old_status,
        "new_status": h.new_status,
        "edited_by_name": h.edited_by_name,
        "edited_by_role": h.edited_by_role,
        "edited_at": h.edited_at.strftime("%d/%m/%Y %H:%M") if h.edited_at else "",
        "edited_at_iso": h.edited_at.isoformat() if h.edited_at else "",
        "reason": h.reason or "",
    }


def override_to_dict(o: PliegoFieldOverride) -> dict:
    return {
        "id": o.id,
        "entity_type": o.entity_type,
        "entity_id": o.entity_id,
        "section_key": o.section_key,
        "field_key": o.field_key,
        "field_label": o.field_label or o.field_key,
        "original_value": o.original_value,
        "edited_value": o.edited_value,
        "original_status": o.original_status,
        "edited_status": o.edited_status,
        "edited_by_name": o.edited_by_name,
        "edited_by_role": o.edited_by_role,
        "edited_at": o.edited_at.strftime("%d/%m/%Y %H:%M") if o.edited_at else "",
        "reason": o.reason or "",
    }


# ---------------------------------------------------------------------------
# Aplicar overrides a fusion_ctx (resultado de calcular_estado_fusion)
# ---------------------------------------------------------------------------

def apply_fusion_ctx_overrides(fusion_ctx: dict, overrides_map: Dict[str, Any]) -> dict:
    """
    Aplica overrides sobre el dict fusion_ctx (in-memory, no toca la BD).
    - entity_type="fusion_campo" → modifica campos_fusion[field_key].valor
    - entity_type="fusion_renglon" → modifica campo en renglones_fusion por entity_id
    Retorna el mismo dict modificado (muta in-place y también retorna).
    """
    if not fusion_ctx or not overrides_map:
        return fusion_ctx

    campos_fusion = fusion_ctx.get("campos_fusion") or {}
    renglones_fusion = fusion_ctx.get("renglones_fusion") or []

    for key, override in overrides_map.items():
        entity_type, entity_id_str, field_key = key.split(":", 2)

        if entity_type == "fusion_campo":
            if field_key not in campos_fusion:
                campos_fusion[field_key] = {}
            fc = campos_fusion[field_key]
            new_val = override.get("edited_value", "") if isinstance(override, dict) else override.edited_value
            fc["valor"] = new_val
            fc["valor_normalizado"] = new_val
            fc["estado"] = STATUS_EDITADO
            if "label" not in fc:
                fc["label"] = (override.get("field_label") or field_key) if isinstance(override, dict) else (override.field_label or field_key)

        elif entity_type == "fusion_renglon":
            try:
                eid = int(entity_id_str)
            except (ValueError, TypeError):
                continue
            new_val = override.get("edited_value", "") if isinstance(override, dict) else override.edited_value
            for renglon in renglones_fusion:
                if renglon.get("_entity_id") == eid:
                    renglon[field_key] = new_val
                    break

    fusion_ctx["campos_fusion"] = campos_fusion
    fusion_ctx["renglones_fusion"] = renglones_fusion
    return fusion_ctx


def get_overridden_field_keys(overrides_map: Dict[str, Any], entity_type: str) -> set:
    """Retorna el set de field_keys overrideados para un entity_type dado."""
    result = set()
    for key in overrides_map:
        parts = key.split(":", 2)
        if len(parts) == 3 and parts[0] == entity_type:
            result.add(parts[2])
    return result


# ---------------------------------------------------------------------------
# Completitud: recalcular con overrides aplicados
# ---------------------------------------------------------------------------

def count_active_overrides(db: Session, solicitud_id: int) -> int:
    return (
        db.query(PliegoFieldOverride)
        .filter(
            PliegoFieldOverride.solicitud_id == solicitud_id,
            PliegoFieldOverride.is_active == True,
        )
        .count()
    )
