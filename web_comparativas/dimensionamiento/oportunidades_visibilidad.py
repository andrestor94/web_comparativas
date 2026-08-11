"""Cálculo de visibilidad por cartera comercial de Oportunidades (Mercado Privado).

Conectado a `oportunidades_router.py` (endpoint `/list`) detrás del kill-switch
`OPORTUNIDADES_CARTERA_ENABLED` (`oportunidades.py`), default OFF: con el switch
apagado el router ni siquiera llama a `oportunidades_visibles_para` — el listado
sigue devolviendo las mismas filas a todo el mundo, sin excepción.

Fórmula (confirmada con negocio, ago-2026 — los 16 vendedores de Operadores.xlsx son
la fuerza de venta real: hay tanto Analistas como Supervisores entre ellos, cada uno
con cartera propia):

  - Analista:    oportunidades cuya cuenta resuelve al vendedor de Fusión vinculado a
                 ESTE usuario (su identidad) MÁS las asignadas a mano a este analista
                 (`OportunidadAsignacionManual`, pieza 3) — la asignación manual es
                 ADITIVA: no le saca visibilidad a nadie, solo se la suma al analista
                 asignado, aunque la cuenta sea de otro vendedor.
  - Supervisor:  su propia cartera (igual que un Analista, si está vinculado a un
                 vendedor) + la cartera de sus analistas a cargo + el buffer de
                 oportunidades cuya cuenta no resuelve a NINGÚN vendedor todavía
                 vinculado a un usuario (ni matchea Operadores.xlsx, ni el vendedor
                 que matchea está vinculado) — ese buffer es el que el supervisor
                 reparte a mano entre sus analistas.
  - Gerente:     la cartera de sus supervisores a cargo + transitivamente la de los
                 analistas de esos supervisores. Alcance PARCIAL (no ve todo) y NO
                 incluye el buffer: repartirlo es tarea del Supervisor, no del
                 Gerente. Hoy los gerentes no tienen cartera propia (no hay currently
                 ningún dato del proyecto que indique que un gerente sea también
                 vendedor de Fusión) — si eso cambia, sumar su propia cartera acá
                 igual que se hace para Supervisor.
  - Auditor/Admin: todo, sin filtrar.
"""
from __future__ import annotations

from sqlalchemy import func
from sqlalchemy.orm import Session

from web_comparativas.models import User, VendedorFusion
from web_comparativas.dimensionamiento.models import (
    OportunidadAsignacionManual,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.account_resolution import (
    get_master_index,
    normalize_identifier,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id

_ROLES_ANALISTA = {"analista", "analyst"}
_ROLES_SUPERVISOR = {"supervisor"}
_ROLES_GERENTE = {"gerente", "manager"}
_ROLES_FULL_READ = {"admin", "administrator", "administrador", "auditor", "visor", "viewer"}


def _rol(user) -> str:
    return (getattr(user, "role", "") or "").strip().lower()


def codigo_vendedor_de_cuenta(cuenta_interna: str | None) -> str | None:
    """Código de vendedor de Fusión (Operadores.xlsx) de una cuenta, o None si la
    cuenta está vacía o no matchea. Mismo join que ya usa el resolver de cuentas
    (`account_resolution._load_master_index`): normalize_identifier + operators_by_code,
    sin llamar al CRM.
    """
    codigo_cuenta = normalize_identifier(cuenta_interna)
    if not codigo_cuenta:
        return None
    operator = get_master_index().operators_by_code.get(codigo_cuenta)
    return operator["vendedor_codigo"] if operator else None


def vendedores_vinculados(db: Session) -> dict[str, int]:
    """{codigo_vendedor: user_id} de los vendedores de Fusión YA vinculados a un
    usuario de SIEM. Los códigos sin vincular no aparecen (van al buffer)."""
    filas = (
        db.query(VendedorFusion.codigo_vendedor, VendedorFusion.user_id)
        .filter(VendedorFusion.user_id.isnot(None))
        .all()
    )
    return {codigo: user_id for codigo, user_id in filas}


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


def oportunidades_visibles_para(
    db: Session, user: User, oportunidades: list[OportunidadSummary]
) -> list[OportunidadSummary]:
    """Subconjunto de `oportunidades` (típicamente el run activo completo) visible
    para este usuario, según su rol. Función pura respecto del kill-switch: quien la
    llame decide si corresponde (hoy nadie la llama todavía)."""
    rol = _rol(user)
    if rol in _ROLES_FULL_READ:
        return list(oportunidades)

    vendedor_por_oportunidad = {
        o.id: codigo_vendedor_de_cuenta(o.cuenta_interna) for o in oportunidades
    }
    vinculados = vendedores_vinculados(db)

    def cartera_de(user_ids: set[int]) -> set[int]:
        codigos = {c for c, uid in vinculados.items() if uid in user_ids}
        return {oid for oid, cod in vendedor_por_oportunidad.items() if cod in codigos}

    if rol in _ROLES_ANALISTA:
        asignadas_a_mano = {
            r[0] for r in
            db.query(OportunidadAsignacionManual.oportunidad_id)
            .filter(OportunidadAsignacionManual.analista_user_id == user.id)
            .all()
        }
        manual_ids = {
            o.id for o in oportunidades
            if opportunity_stable_id(o.cliente_visible, o.codigo_articulo) in asignadas_a_mano
        }
        visibles_ids = cartera_de({user.id}) | manual_ids

    elif rol in _ROLES_SUPERVISOR:
        equipo_ids = set(analistas_a_cargo(db, user.id))
        buffer_ids = {
            oid for oid, cod in vendedor_por_oportunidad.items() if cod not in vinculados
        }
        visibles_ids = cartera_de({user.id}) | cartera_de(equipo_ids) | buffer_ids

    elif rol in _ROLES_GERENTE:
        supervisores_ids = set(supervisores_a_cargo(db, user.id))
        analistas_ids: set[int] = set()
        for sid in supervisores_ids:
            analistas_ids.update(analistas_a_cargo(db, sid))
        # Sin cartera propia (ver docstring) y sin buffer: eso es turf del Supervisor.
        visibles_ids = cartera_de(supervisores_ids) | cartera_de(analistas_ids)

    else:
        visibles_ids = set()

    return [o for o in oportunidades if o.id in visibles_ids]
