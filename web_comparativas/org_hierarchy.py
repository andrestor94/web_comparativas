"""Jerarquía comercial explícita (`reporta_a_id`) — quién reporta a quién.

Extraído de `dimensionamiento/oportunidades_visibilidad.py` (2026-08-25, enganche de
Oportunidades al motor central de cartera) para romper un import circular: antes,
`cartera_visibilidad.py` importaba estas dos funciones DESDE
`oportunidades_visibilidad.py`; al enganchar Oportunidades a
`cartera_visibilidad.clientes_visibles_para`, `oportunidades_visibilidad.py` pasó a
necesitar el import en el sentido contrario. Este módulo no depende de ninguno de los
dos — solo de `models.User` — y ambos importan de acá.

Reconoce los roles canónicos MÁS sus alias históricos (`analyst`, etc.): a diferencia
de `cartera_visibilidad.clientes_visibles_para` (estricto, cae fail-closed ante
cualquier alias), acá el criterio es simplemente "¿quién tiene este `reporta_a_id`?"
— arma el árbol, no decide visibilidad.
"""
from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from web_comparativas.models import User

_ROLES_ANALISTA = {"analista", "analyst"}
_ROLES_SUPERVISOR = {"supervisor"}


def analistas_a_cargo(db: Session, supervisor_id: int) -> list[int]:
    filas = (
        db.query(User.id)
        .filter(User.reporta_a_id == supervisor_id, func.lower(User.role).in_(_ROLES_ANALISTA))
        .all()
    )
    return [r[0] for r in filas]


def supervisores_a_cargo(db: Session, gerente_id: int) -> list[int]:
    filas = (
        db.query(User.id)
        .filter(User.reporta_a_id == gerente_id, func.lower(User.role).in_(_ROLES_SUPERVISOR))
        .all()
    )
    return [r[0] for r in filas]
