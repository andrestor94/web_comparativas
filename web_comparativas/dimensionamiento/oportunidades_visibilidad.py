"""Cálculo de visibilidad por cartera comercial de Oportunidades (Mercado Privado).

Conectado a `oportunidades_router.py` (endpoint `/list`) detrás del kill-switch
`OPORTUNIDADES_CARTERA_ENABLED` (`oportunidades.py`), default OFF: con el switch
apagado el router ni siquiera llama a `oportunidades_visibles_para` — el listado
sigue devolviendo las mismas filas a todo el mundo, sin excepción.

Motor de visibilidad (desde 2026-08-25): `cartera_visibilidad.clientes_visibles_para`,
el mismo que ya usan Forecast y Dimensionamiento — reemplaza el mecanismo viejo
basado en `VendedorFusion` (16 vendedores de Fusión vinculados 1 a 1 a un usuario).
Motivo del cambio: la medición de cobertura contra prod (ago-2026) mostró que
`VendedorFusion` dejaba a la gran mayoría de la cartera de cada usuario sin ninguna
oportunidad visible — el cruce válido es `oportunidades_summary.cuenta_interna`
contra `cartera_operadores` / `cartera_vendedores.codigo_cliente` (confirmado
46/58 de match literal contra el padrón vigente en prod, run 68).

Fórmula:
  - Analista:    su cartera resuelta por `clientes_visibles_para` (operador propio +
                 vendedor propio) MÁS las asignadas a mano a este analista
                 (`OportunidadAsignacionManual`, pieza 3) — la asignación manual es
                 ADITIVA: no le saca visibilidad a nadie, solo se la suma al analista
                 asignado, aunque la cuenta sea de otro vendedor.
  - Supervisor:  su cartera resuelta por `clientes_visibles_para` (propia + la de sus
                 analistas a cargo, agregada por la jerarquía `reporta_a_id`). SIN
                 buffer: a diferencia del mecanismo viejo, el Supervisor ya no recibe
                 las oportunidades sin dueño — ese rol pasa al Gerente (ver abajo).
  - Gerente:     su cartera resuelta por `clientes_visibles_para` (la de sus
                 supervisores a cargo y, transitivamente, la de los analistas de esos
                 supervisores) MÁS el buffer: toda oportunidad cuya cuenta no está en
                 la cartera de NINGÚN usuario del sistema (huérfana). Decisión de
                 negocio 2026-08-25: "nada se pierde" — antes ese buffer era turf
                 exclusivo del Supervisor; ahora, como la cartera ya no tiene un
                 concepto de "vendedor sin vincular" (es por cuenta, no por persona),
                 el catch-all se le da a Gerente (y a Admin/Auditor, que ya ven todo).
  - Auditor/Admin: todo, sin filtrar — incluye el buffer sin que haga falta calcularlo
                 aparte.
"""
from __future__ import annotations

from sqlalchemy.orm import Session

from web_comparativas.models import User
from web_comparativas.cartera_visibilidad import clientes_visibles_para
from web_comparativas.org_hierarchy import (  # noqa: F401 (re-exportadas: oportunidades_router.py las importa desde acá)
    analistas_a_cargo,
    supervisores_a_cargo,
)
from web_comparativas.dimensionamiento.models import (
    OportunidadAsignacionManual,
    OportunidadSummary,
)
from web_comparativas.dimensionamiento.oportunidades import opportunity_stable_id

_ROLES_ANALISTA = {"analista", "analyst"}
_ROLES_SUPERVISOR = {"supervisor"}
_ROLES_GERENTE = {"gerente", "manager"}
_ROLES_FULL_READ = {"admin", "administrator", "administrador", "auditor", "visor", "viewer"}


def _rol(user) -> str:
    return (getattr(user, "role", "") or "").strip().lower()


def _codigos_cartera_de_todos(db: Session) -> frozenset[str]:
    """Unión de los códigos de cliente cubiertos por LA CARTERA de algún usuario
    (cualquier rol con scope propio vía `clientes_visibles_para`) — el complemento
    es el buffer de Gerente: cuentas que no son de nadie. Se excluyen los usuarios
    `unrestricted` (admin/auditor): ven todo por definición, no aportan códigos
    concretos a la cobertura.

    Recorre TODOS los usuarios — cara, pero solo se llama para armar el buffer de
    Gerente (no en cada request de Analista/Supervisor, que son la mayoría)."""
    codigos: set[str] = set()
    for u in db.query(User).all():
        scope = clientes_visibles_para(db, u)
        if not scope.unrestricted:
            codigos |= scope.codigos_cliente
    return frozenset(codigos)


def oportunidades_visibles_para(
    db: Session, user: User, oportunidades: list[OportunidadSummary]
) -> list[OportunidadSummary]:
    """Subconjunto de `oportunidades` (típicamente el run activo completo) visible
    para este usuario, según su rol. Función pura respecto del kill-switch: quien la
    llame decide si corresponde (hoy `oportunidades_router.py`, detrás de
    `OPORTUNIDADES_CARTERA_ENABLED`)."""
    rol = _rol(user)
    if rol in _ROLES_FULL_READ:
        return list(oportunidades)

    scope = clientes_visibles_para(db, user)
    if scope.unrestricted:
        return list(oportunidades)

    visibles_ids = {o.id for o in oportunidades if scope.permite(o.cuenta_interna)}

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
        visibles_ids |= manual_ids

    elif rol in _ROLES_GERENTE:
        cubiertas = _codigos_cartera_de_todos(db)
        huerfanas_ids = {
            o.id for o in oportunidades
            if (o.cuenta_interna or "").strip() not in cubiertas
        }
        visibles_ids |= huerfanas_ids

    return [o for o in oportunidades if o.id in visibles_ids]
