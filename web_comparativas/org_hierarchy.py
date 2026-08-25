"""Jerarquía comercial explícita — quién reporta a quién.

Extraído de `dimensionamiento/oportunidades_visibilidad.py` (2026-08-25, enganche de
Oportunidades al motor central de cartera) para romper un import circular: antes,
`cartera_visibilidad.py` importaba estas dos funciones DESDE
`oportunidades_visibilidad.py`; al enganchar Oportunidades a
`cartera_visibilidad.clientes_visibles_para`, `oportunidades_visibilidad.py` pasó a
necesitar el import en el sentido contrario. Este módulo no depende de ninguno de los
dos — solo de `models.User`/`models.UserReporte` — y ambos importan de acá.

Fuente del vínculo (actualizado 2026-08-25): tabla `user_reportes` (M:N) —
reemplaza a la columna `User.reporta_a_id` (1:1, congelada) porque un Analista
puede tener más de un Supervisor/Gerente a cargo, y un Supervisor más de un
Gerente (caso real: Daniela y Yanina comparten varios analistas). Ver
`models.UserReporte` y `routers/sic_router.py` (donde se valida y se escribe).

Reconoce los roles canónicos MÁS sus alias históricos (`analyst`, etc.): a diferencia
de `cartera_visibilidad.clientes_visibles_para` (estricto, cae fail-closed ante
cualquier alias), acá el criterio es simplemente "¿quién tiene este vínculo?" — arma
el árbol, no decide visibilidad. Sin recursión: cada función hace UNA consulta de un
solo salto (join `user_reportes` -> `users` filtrando por rol) — el árbol de 3 niveles
(Analista/Supervisor/Gerente) se recorre con llamadas fijas desde
`cartera_visibilidad.py`/`oportunidades_visibilidad.py`, nunca acá adentro.
"""
from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from web_comparativas.models import User, UserReporte

_ROLES_ANALISTA = {"analista", "analyst"}
_ROLES_SUPERVISOR = {"supervisor"}


def analistas_a_cargo(db: Session, supervisor_id: int) -> list[int]:
    filas = (
        db.query(User.id)
        .join(UserReporte, UserReporte.subordinado_id == User.id)
        .filter(UserReporte.superior_id == supervisor_id, func.lower(User.role).in_(_ROLES_ANALISTA))
        .distinct()
        .all()
    )
    return [r[0] for r in filas]


def supervisores_a_cargo(db: Session, gerente_id: int) -> list[int]:
    filas = (
        db.query(User.id)
        .join(UserReporte, UserReporte.subordinado_id == User.id)
        .filter(UserReporte.superior_id == gerente_id, func.lower(User.role).in_(_ROLES_SUPERVISOR))
        .distinct()
        .all()
    )
    return [r[0] for r in filas]


def superiores_de(db: Session, subordinado_id: int) -> list[int]:
    """Todos los ids de superiores directos de `subordinado_id` (0, 1 o varios),
    sin filtrar por rol — usado para validaciones de "¿le puedo asignar esto?"
    (ver `oportunidades_router.py::_require_oportunidades_asignar`), no para
    armar el árbol de cartera (eso son las dos funciones de arriba)."""
    filas = (
        db.query(UserReporte.superior_id)
        .filter(UserReporte.subordinado_id == subordinado_id)
        .all()
    )
    return [r[0] for r in filas]
