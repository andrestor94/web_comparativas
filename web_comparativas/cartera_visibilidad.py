"""Resolución de visibilidad por cartera de cuentas — motor único compartido por
Forecast, Dimensionamiento y (más adelante, detrás de OPORTUNIDADES_CARTERA_ENABLED)
Oportunidades. Ver informe de auditoría "Cartera de Cuentas" (2026-08-19) y decisiones
tomadas el mismo día.

Contrato — FAIL-CLOSED (decisión de negocio, no accidente):
    Un usuario sin códigos de cartera asignados ve CERO clientes. Nunca "todos".
    Esto es la semántica INVERSA de `User.module_access` (donde NULL = acceso total)
    — no reutilizar esa convención acá. Ver comentario en `models.py` sobre
    `cartera_operador_codigos` / `cartera_vendedor_codigos` / `cartera_unineg_scope`.

Reusa la jerarquía (`analistas_a_cargo` / `supervisores_a_cargo`, vía `reporta_a_id`)
del motor ya construido para Oportunidades en `oportunidades_visibilidad.py` — un
solo lugar donde vive la lógica de "quién reporta a quién" para cartera comercial.
NO reutiliza su tabla (`VendedorFusion` modela otra relación: identidad 1 a 1 de los
16 vendedores de Fusión/Mercado Privado, con su propio UniqueConstraint). Este motor
resuelve contra los padrones nuevos `cartera_operadores` / `cartera_vendedores`
(22 operadores / 248 vendedores, Mercado Público + Privado).

Identidad del cliente: el código numérico compartido por operadores_comerciales.csv
(columna `codigo`), Cliente_vendedor.csv (columna `cliente`) y `clientes.csv` de
Forecast (columna `codigo`, de ahí sale `forecast_*.cliente_id`). Ver Sección 2 del
informe de auditoría para la cobertura medida de este cruce.
"""
from __future__ import annotations

import os
from dataclasses import dataclass, field
from typing import Iterable

from sqlalchemy.orm import Session

from web_comparativas.models import User, CarteraOperador, CarteraVendedor
from web_comparativas.fusion_name_matching import fusion_codes_for_user
from web_comparativas.dimensionamiento.oportunidades_visibilidad import (
    analistas_a_cargo,
    supervisores_a_cargo,
)

# Este motor nuevo reconoce EXCLUSIVAMENTE los cinco roles canónicos seleccionables
# en S.I.C. Cualquier alias histórico o rol desconocido cae en NONE_ (fail-closed).
# Gerente y Supervisor tienen alcance agregado por jerarquía, no acceso total.
_ROLES_FULL_READ = {"admin", "auditor"}
_ROLES_ANALISTA = {"analista"}
_ROLES_SUPERVISOR = {"supervisor"}
_ROLES_GERENTE = {"gerente"}


@dataclass(frozen=True)
class CarteraBranchScope:
    '''Una rama autorizada, manteniendo juntas cuentas, UN y miembros.'''

    root_user_id: int
    member_user_ids: frozenset[int] = field(default_factory=frozenset)
    codigos_cliente: frozenset[str] = field(default_factory=frozenset)
    # None: Analista sin restriccion por UN. Vacio: configuracion incompleta.
    unidades_negocio: frozenset[str] | None = None


@dataclass(frozen=True)
class CarteraScope:
    """Resultado de la resolución para un usuario.

    `unrestricted=True`  -> sin filtro, ve todo (admin/auditor).
    `unrestricted=False` -> ve exactamente `codigos_cliente` (puede ser el set
                             vacío: fail-closed, "no ve nada").
    """

    unrestricted: bool
    codigos_cliente: frozenset[str] = field(default_factory=frozenset)
    branches: tuple[CarteraBranchScope, ...] = field(default_factory=tuple)

    def permite(self, codigo_cliente: str | int | None) -> bool:
        if self.unrestricted:
            return True
        if codigo_cliente is None:
            return False
        return str(codigo_cliente).strip() in self.codigos_cliente


ALL = CarteraScope(unrestricted=True)
NONE_ = CarteraScope(unrestricted=False, codigos_cliente=frozenset())


def _flag(name: str) -> bool:
    raw = (os.getenv(name) or "false").strip().lower()
    return raw not in {"0", "false", "no", "off", "n"}


def FORECAST_CARTERA_ENABLED() -> bool:
    """Kill-switch por módulo (mismo patrón que OPORTUNIDADES_CARTERA_ENABLED en
    dimensionamiento/oportunidades.py), default OFF. Necesario: apenas se prende,
    todo usuario sin cartera cargada en `users.cartera_*` pasa a ver CERO clientes en
    Forecast (fail-closed) — recién tiene sentido prenderlo después de correr
    push_cartera_data.py Y asignar cartera a cada usuario desde S.I.C."""
    return _flag("FORECAST_CARTERA_ENABLED")


def DIMENSIONAMIENTO_CARTERA_ENABLED() -> bool:
    """Kill-switch para el filtro de cartera en Dimensionamiento — todavía sin
    consumidor (pendiente medir cobertura real contra prod antes de aplicarlo,
    ver informe de auditoría 2026-08-19). Default OFF."""
    return _flag("DIMENSIONAMIENTO_CARTERA_ENABLED")


def _codes(value: Iterable | None) -> set[str]:
    """Normaliza una columna JSONEncodedList (lista|None) a un set[str] sin vacíos.
    None Y [] se tratan IGUAL acá — ambos son "sin códigos" (ver contrato fail-closed
    en el docstring del módulo)."""
    if not value:
        return set()
    return {str(v).strip() for v in value if str(v).strip()}


def _clientes_por_operador(db: Session, codigos_operador: set[str]) -> set[str]:
    if not codigos_operador:
        return set()
    rows = (
        db.query(CarteraOperador.codigo_cliente)
        .filter(CarteraOperador.operador_codigo.in_(codigos_operador))
        .distinct()
        .all()
    )
    return {r[0] for r in rows}


def _clientes_por_vendedor(
    db: Session, codigos_vendedor: set[str], unineg_scope: set[str] | None
) -> set[str]:
    """`unineg_scope=None` -> sin filtro de unidad de negocio (usuario común/analista,
    la BU no forma parte de su llave de autorización). `unineg_scope=set()` (vacío)
    -> filtra a NINGUNA unineg -> fail-closed (Supervisor sin BU asignada explícita
    no hereda acceso "sin restricción" por omisión)."""
    if not codigos_vendedor:
        return set()
    q = db.query(CarteraVendedor.codigo_cliente).filter(
        CarteraVendedor.vendedor_codigo.in_(codigos_vendedor)
    )
    if unineg_scope is not None:
        q = q.filter(CarteraVendedor.unineg.in_(unineg_scope))
    return {r[0] for r in q.distinct().all()}


def _cartera_propia(db: Session, u: User, unineg_scope: set[str] | None = None) -> set[str]:
    """Cuentas propias de un usuario: unión de lo que le toca por su código de
    operador y por su(s) código(s) de vendedor. `unineg_scope` solo se aplica al lado
    vendedor (operadores_comerciales.csv no trae unidad de negocio).

    `unineg_scope` es EXCLUSIVAMENTE el scope del propio `u` — nunca el de un
    superior ni el de un subordinado (regla corregida 2026-08-20: la jerarquía
    manda, la UN de un jefe no filtra lo que hereda de su equipo). Los
    llamadores que arman la cartera de un Supervisor/Gerente deben pasar
    `unineg_scope=None` cuando invocan esto para un ANALISTA/SUPERVISOR a
    cargo — ver `clientes_visibles_para`."""
    fusion_operadores, fusion_vendedores, _match = fusion_codes_for_user(db, u)
    operadores = _codes(u.cartera_operador_codigos) | fusion_operadores
    vendedores = _codes(u.cartera_vendedor_codigos) | fusion_vendedores
    codigos = _clientes_por_operador(db, operadores) | _clientes_por_vendedor(
        db, vendedores, unineg_scope
    )
    if unineg_scope is not None:
        # Supervisor = cartera encontrada ∩ unidades autorizadas. Se aplica sobre
        # el resultado completo, incluido el lado operador.
        if not unineg_scope:
            return set()
        permitidos = {
            row[0]
            for row in db.query(CarteraVendedor.codigo_cliente)
            .filter(CarteraVendedor.unineg.in_(unineg_scope))
            .distinct()
            .all()
        }
        codigos &= permitidos
    return codigos


def clientes_visibles_para(db: Session, user: User) -> CarteraScope:
    """Resuelve la cartera visible para `user`. Función pura: no consulta ningún
    feature flag — quien la llame decide si corresponde aplicarla en ese módulo."""
    rol = (user.role or "").strip().lower()

    if rol in _ROLES_FULL_READ:
        return ALL

    if rol in _ROLES_ANALISTA:
        # Usuario común: sin scope de BU, ve exactamente su propia cartera.
        codigos = _cartera_propia(db, user, unineg_scope=None)
        branch = CarteraBranchScope(
            root_user_id=user.id,
            member_user_ids=frozenset({user.id}),
            codigos_cliente=frozenset(codigos),
            unidades_negocio=None,
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=branch.codigos_cliente,
            branches=(branch,),
        )

    if rol in _ROLES_SUPERVISOR:
        # Regla corregida 2026-08-20: la jerarquía manda. cartera_unineg_scope
        # SOLO acota la cartera PROPIA de este Supervisor (sus propios códigos
        # de vendedor/operador) — set() vacío sigue siendo fail-closed ahí. Lo
        # que hereda de cada Analista a cargo entra COMPLETO, sin aplicar este
        # scope: la UN del jefe no es un filtro sobre las cuentas del equipo.
        # Bug real que esto corrige: Myriam (vendedor 1, unineg 4) reporta a
        # Daniela (scope 5+15) — Daniela no veía nada de Myriam porque su
        # cartera se intersecaba contra un scope que no es el suyo.
        unineg_scope = _codes(user.cartera_unineg_scope)
        member_ids = {user.id}
        codigos = _cartera_propia(db, user, unineg_scope=unineg_scope)
        for analista_id in analistas_a_cargo(db, user.id):
            analista = db.get(User, analista_id)
            if analista is not None:
                member_ids.add(analista.id)
                codigos |= _cartera_propia(db, analista, unineg_scope=None)
        branch = CarteraBranchScope(
            root_user_id=user.id,
            member_user_ids=frozenset(member_ids),
            codigos_cliente=frozenset(codigos),
            unidades_negocio=frozenset(unineg_scope),
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=branch.codigos_cliente,
            branches=(branch,),
        )

    if rol in _ROLES_GERENTE:
        # Sin cartera propia (igual que en oportunidades_visibilidad.py): agrega la
        # cartera de sus supervisores a cargo y, transitivamente, la de los analistas
        # de esos supervisores.
        # Regla corregida 2026-08-20 (mismo criterio que Supervisor arriba): la UN
        # del supervisor acota SOLO la cartera PROPIA de ESE supervisor — nunca lo
        # que sus analistas aportan a la rama. La jerarquía manda de punta a punta.
        branches: list[CarteraBranchScope] = []
        for supervisor_id in supervisores_a_cargo(db, user.id):
            supervisor = db.get(User, supervisor_id)
            if supervisor is None:
                continue
            sup_unineg_scope = _codes(supervisor.cartera_unineg_scope)
            branch_codes = _cartera_propia(db, supervisor, unineg_scope=sup_unineg_scope)
            member_ids = {supervisor.id}
            for analista_id in analistas_a_cargo(db, supervisor_id):
                analista = db.get(User, analista_id)
                if analista is not None:
                    member_ids.add(analista.id)
                    branch_codes |= _cartera_propia(db, analista, unineg_scope=None)
            branches.append(CarteraBranchScope(
                root_user_id=supervisor.id,
                member_user_ids=frozenset(member_ids),
                codigos_cliente=frozenset(branch_codes),
                unidades_negocio=frozenset(sup_unineg_scope),
            ))
        codigos = frozenset(
            codigo for branch in branches for codigo in branch.codigos_cliente
        )
        return CarteraScope(
            unrestricted=False,
            codigos_cliente=codigos,
            branches=tuple(branches),
        )

    # Cualquier otro rol: fail-closed.
    return NONE_
