# tests/test_permissions.py
"""
Suite de pruebas funcionales para el motor de permisos del sistema S.I.C.

Cubre:
  - Predicados de rol
  - can_create_upload
  - can_view_upload
  - can_edit_upload / can_delete_upload
  - can_manage_user
  - can_manage_group / can_add_member_to_group
  - can_access_module
  - visible_user_scope (via visibility_service)
  - validate_user_config
  - resolve_login_redirect

NO requiere base de datos para la mayoría de tests (usa objetos simples).
Los tests de visibilidad de datos (visible_user_scope) usan SQLite en memoria.

Ejecutar:
  cd web_comparativas_v2
  python -m pytest tests/test_permissions.py -v
"""
from __future__ import annotations
import sys
import os
from types import SimpleNamespace
from typing import Set

import pytest

# Asegurar que el paquete esté en el path
sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.policy import (
    is_admin,
    is_auditor,
    is_supervisor,
    is_analyst,
    is_manager,
    has_full_read,
    has_write_access,
    can_create_upload,
    can_view_upload,
    can_edit_upload,
    can_delete_upload,
    can_manage_user,
    can_manage_group,
    can_add_member_to_group,
    can_access_module,
    validate_user_config,
    resolve_login_redirect,
    visible_user_scope,
)


# ──────────────────────────────────────────────────────────────────────────────
# Factories de usuarios de prueba (sin DB)
# ──────────────────────────────────────────────────────────────────────────────

def _user(
    id: int = 1,
    role: str = "analista",
    unit_business: str = "Productos Hospitalarios",
    access_scope: str = "todos",
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        role=role,
        unit_business=unit_business,
        access_scope=access_scope,
    )


def _upload(id: int = 10, user_id: int = 1, proceso_key: str = None) -> SimpleNamespace:
    return SimpleNamespace(id=id, user_id=user_id, proceso_key=proceso_key)


def _group(
    id: int = 1,
    business_unit: str = "Productos Hospitalarios",
    created_by_user_id: int = 99,
) -> SimpleNamespace:
    return SimpleNamespace(
        id=id,
        business_unit=business_unit,
        created_by_user_id=created_by_user_id,
    )


# ──────────────────────────────────────────────────────────────────────────────
# 1. Predicados de rol
# ──────────────────────────────────────────────────────────────────────────────

class TestRolePredicates:

    def test_admin_is_admin(self):
        assert is_admin(_user(role="admin"))

    def test_administrator_alias(self):
        assert is_admin(_user(role="administrator"))

    def test_auditor_is_auditor(self):
        assert is_auditor(_user(role="auditor"))

    def test_visor_is_auditor(self):
        assert is_auditor(_user(role="visor"))

    def test_supervisor_is_supervisor(self):
        assert is_supervisor(_user(role="supervisor"))

    def test_analista_is_analyst(self):
        assert is_analyst(_user(role="analista"))

    def test_analyst_alias(self):
        assert is_analyst(_user(role="analyst"))

    def test_gerente_is_manager(self):
        assert is_manager(_user(role="gerente"))

    def test_manager_alias(self):
        assert is_manager(_user(role="manager"))

    def test_admin_has_full_read(self):
        assert has_full_read(_user(role="admin"))

    def test_auditor_has_full_read(self):
        assert has_full_read(_user(role="auditor"))

    def test_manager_has_full_read(self):
        assert has_full_read(_user(role="gerente"))

    def test_supervisor_not_full_read(self):
        assert not has_full_read(_user(role="supervisor"))

    def test_analyst_not_full_read(self):
        assert not has_full_read(_user(role="analista"))

    def test_auditor_no_write(self):
        assert not has_write_access(_user(role="auditor"))

    def test_visor_no_write(self):
        assert not has_write_access(_user(role="visor"))

    def test_analyst_has_write(self):
        assert has_write_access(_user(role="analista"))

    def test_admin_has_write(self):
        assert has_write_access(_user(role="admin"))

    def test_supervisor_has_write(self):
        assert has_write_access(_user(role="supervisor"))


# ──────────────────────────────────────────────────────────────────────────────
# 2. can_create_upload
# ──────────────────────────────────────────────────────────────────────────────

class TestCanCreateUpload:

    def test_admin_can_upload(self):
        assert can_create_upload(_user(role="admin"))

    def test_supervisor_can_upload(self):
        assert can_create_upload(_user(role="supervisor"))

    def test_analyst_can_upload(self):
        assert can_create_upload(_user(role="analista"))

    def test_auditor_cannot_upload(self):
        """REGLA CRÍTICA: auditor solo lectura, no puede cargar."""
        assert not can_create_upload(_user(role="auditor"))

    def test_visor_cannot_upload(self):
        assert not can_create_upload(_user(role="visor"))

    def test_viewer_cannot_upload(self):
        assert not can_create_upload(_user(role="viewer"))

    def test_none_user_cannot_upload(self):
        assert not can_create_upload(None)

    def test_unknown_role_cannot_upload(self):
        assert not can_create_upload(_user(role="invitado"))


# ──────────────────────────────────────────────────────────────────────────────
# 3. can_view_upload (sin DB — tests de rol y owner)
# ──────────────────────────────────────────────────────────────────────────────

class TestCanViewUploadNoDb:
    """
    Tests que no requieren DB: role-global y owner-first.
    Para tests con DB (grupos, BU), ver TestCanViewUploadWithDb.
    """

    def test_admin_can_view_any(self):
        upload = _upload(user_id=99)
        actor = _user(id=1, role="admin")
        assert can_view_upload(None, actor, upload)

    def test_auditor_can_view_any(self):
        upload = _upload(user_id=99)
        actor = _user(id=1, role="auditor")
        assert can_view_upload(None, actor, upload)

    def test_manager_can_view_any(self):
        upload = _upload(user_id=99)
        actor = _user(id=1, role="gerente")
        assert can_view_upload(None, actor, upload)

    def test_owner_can_view_own(self):
        """El owner SIEMPRE puede ver lo suyo, independiente del rol."""
        upload = _upload(user_id=5)
        actor = _user(id=5, role="analista")
        assert can_view_upload(None, actor, upload)

    def test_none_user_cannot_view(self):
        assert not can_view_upload(None, None, _upload())

    def test_none_upload_cannot_view(self):
        assert not can_view_upload(None, _user(), None)


# ──────────────────────────────────────────────────────────────────────────────
# 4. can_edit_upload / can_delete_upload
# ──────────────────────────────────────────────────────────────────────────────

class TestEditDeleteUpload:

    def test_admin_can_edit_any(self):
        assert can_edit_upload(_user(role="admin"), _upload(user_id=99))

    def test_auditor_cannot_edit(self):
        assert not can_edit_upload(_user(id=1, role="auditor"), _upload(user_id=1))

    def test_auditor_cannot_edit_own(self):
        """Auditor NO puede editar ni siquiera lo suyo."""
        assert not can_edit_upload(_user(id=5, role="auditor"), _upload(user_id=5))

    def test_analyst_can_edit_own(self):
        assert can_edit_upload(_user(id=5, role="analista"), _upload(user_id=5))

    def test_analyst_cannot_edit_others(self):
        assert not can_edit_upload(_user(id=5, role="analista"), _upload(user_id=99))

    def test_supervisor_can_edit_own(self):
        assert can_edit_upload(_user(id=3, role="supervisor"), _upload(user_id=3))

    def test_supervisor_cannot_edit_others(self):
        assert not can_edit_upload(_user(id=3, role="supervisor"), _upload(user_id=99))

    def test_admin_can_delete_any(self):
        assert can_delete_upload(_user(role="admin"), _upload(user_id=99))

    def test_auditor_cannot_delete(self):
        assert not can_delete_upload(_user(id=5, role="auditor"), _upload(user_id=5))

    def test_analyst_can_delete_own(self):
        assert can_delete_upload(_user(id=5, role="analista"), _upload(user_id=5))

    def test_analyst_cannot_delete_others(self):
        assert not can_delete_upload(_user(id=5, role="analista"), _upload(user_id=99))


# ──────────────────────────────────────────────────────────────────────────────
# 5. can_manage_user
# ──────────────────────────────────────────────────────────────────────────────

class TestCanManageUser:

    def test_admin_can_manage_any(self):
        assert can_manage_user(_user(role="admin"))

    def test_admin_can_manage_specific(self):
        assert can_manage_user(_user(role="admin"), _user(id=99, role="analista"))

    def test_auditor_cannot_manage(self):
        assert not can_manage_user(_user(role="auditor"))

    def test_supervisor_cannot_manage(self):
        assert not can_manage_user(_user(role="supervisor"))

    def test_analyst_cannot_manage(self):
        assert not can_manage_user(_user(role="analista"))

    def test_manager_cannot_manage(self):
        """Gerente tiene lectura total pero NO gestión de usuarios."""
        assert not can_manage_user(_user(role="gerente"))

    def test_none_cannot_manage(self):
        assert not can_manage_user(None)


# ──────────────────────────────────────────────────────────────────────────────
# 6. can_manage_group
# ──────────────────────────────────────────────────────────────────────────────

class TestCanManageGroup:

    def test_admin_can_manage_any_group(self):
        g = _group(business_unit="Estética Médica y Reconstructiva")
        assert can_manage_group(_user(role="admin"), g)

    def test_supervisor_can_manage_own_bu_group(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Productos Hospitalarios")
        assert can_manage_group(actor, g)

    def test_supervisor_cannot_manage_other_bu_group(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Tratamientos Especiales")
        assert not can_manage_group(actor, g)

    def test_supervisor_without_bu_cannot_manage(self):
        actor = _user(id=1, role="supervisor", unit_business="")
        g = _group(business_unit="Productos Hospitalarios")
        assert not can_manage_group(actor, g)

    def test_supervisor_can_create_group_no_group_arg(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        assert can_manage_group(actor, None)  # None = "crear nuevo"

    def test_analyst_cannot_manage_group(self):
        assert not can_manage_group(_user(role="analista"), _group())

    def test_auditor_cannot_manage_group(self):
        assert not can_manage_group(_user(role="auditor"), _group())


# ──────────────────────────────────────────────────────────────────────────────
# 7. can_add_member_to_group
# ──────────────────────────────────────────────────────────────────────────────

class TestCanAddMemberToGroup:

    def test_admin_can_add_anyone(self):
        actor = _user(role="admin")
        target = _user(id=99, role="analista", unit_business="Otros")
        g = _group(business_unit="Estética Médica y Reconstructiva")
        assert can_add_member_to_group(actor, target, g)

    def test_supervisor_can_add_same_bu_analyst(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        target = _user(id=2, role="analista", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Productos Hospitalarios")
        assert can_add_member_to_group(actor, target, g)

    def test_supervisor_cannot_add_different_bu_user(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        target = _user(id=2, role="analista", unit_business="Tratamientos Especiales")
        g = _group(business_unit="Productos Hospitalarios")
        assert not can_add_member_to_group(actor, target, g)

    def test_supervisor_cannot_add_to_different_bu_group(self):
        actor = _user(id=1, role="supervisor", unit_business="Productos Hospitalarios")
        target = _user(id=2, role="analista", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Tratamientos Especiales")  # diferente BU
        assert not can_add_member_to_group(actor, target, g)

    def test_supervisor_without_bu_cannot_add(self):
        actor = _user(id=1, role="supervisor", unit_business="")
        target = _user(id=2, role="analista", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Productos Hospitalarios")
        assert not can_add_member_to_group(actor, target, g)

    def test_analyst_cannot_add_member(self):
        actor = _user(role="analista", unit_business="Productos Hospitalarios")
        target = _user(id=2, role="analista", unit_business="Productos Hospitalarios")
        g = _group(business_unit="Productos Hospitalarios")
        assert not can_add_member_to_group(actor, target, g)


# ──────────────────────────────────────────────────────────────────────────────
# 8. can_access_module
# ──────────────────────────────────────────────────────────────────────────────

class TestCanAccessModule:

    def test_admin_access_all_modules(self):
        u = _user(role="admin")
        for mod in ["sic", "dimensionamiento", "mercado_publico", "mercado_privado",
                    "admin_usuarios", "admin_grupos", "reports", "helpdesk"]:
            assert can_access_module(u, mod), f"admin debe acceder a {mod}"

    def test_auditor_cannot_access_admin_modules(self):
        u = _user(role="auditor")
        assert not can_access_module(u, "admin_usuarios")

    def test_auditor_can_access_helpdesk(self):
        assert can_access_module(_user(role="auditor"), "helpdesk")

    def test_auditor_can_access_admin_grupos(self):
        """Auditor NO puede gestionar grupos (eso requiere admin o supervisor)."""
        assert not can_access_module(_user(role="auditor"), "admin_grupos")

    def test_supervisor_cannot_access_admin_usuarios(self):
        assert not can_access_module(_user(role="supervisor"), "admin_usuarios")

    def test_supervisor_can_access_admin_grupos(self):
        assert can_access_module(_user(role="supervisor"), "admin_grupos")

    def test_analyst_can_access_sic(self):
        assert can_access_module(_user(role="analista"), "sic")

    def test_analyst_cannot_access_admin_usuarios(self):
        assert not can_access_module(_user(role="analista"), "admin_usuarios")

    def test_analyst_cannot_access_admin_grupos(self):
        assert not can_access_module(_user(role="analista"), "admin_grupos")

    def test_unknown_module_denied(self):
        assert not can_access_module(_user(role="admin"), "modulo_desconocido")

    def test_none_user_denied(self):
        assert not can_access_module(None, "sic")


# ──────────────────────────────────────────────────────────────────────────────
# 9. validate_user_config
# ──────────────────────────────────────────────────────────────────────────────

class TestValidateUserConfig:

    def test_admin_valid_without_bu(self):
        u = _user(role="admin", unit_business="")
        assert validate_user_config(u) == []

    def test_auditor_valid_without_bu(self):
        u = _user(role="auditor", unit_business="")
        assert validate_user_config(u) == []

    def test_supervisor_without_bu_has_error(self):
        u = _user(role="supervisor", unit_business="")
        errors = validate_user_config(u)
        assert len(errors) > 0
        assert any("Supervisor" in e for e in errors)

    def test_analyst_without_bu_has_error(self):
        u = _user(role="analista", unit_business="")
        errors = validate_user_config(u)
        assert len(errors) > 0
        assert any("Analista" in e for e in errors)

    def test_supervisor_with_valid_bu_is_ok(self):
        u = _user(role="supervisor", unit_business="Productos Hospitalarios")
        assert validate_user_config(u) == []

    def test_analyst_with_valid_bu_is_ok(self):
        u = _user(role="analista", unit_business="Tratamientos Especiales")
        assert validate_user_config(u) == []

    def test_invalid_scope_has_error(self):
        u = _user(role="analista", unit_business="Productos Hospitalarios", access_scope="xyz_invalido")
        errors = validate_user_config(u)
        assert any("access_scope" in e for e in errors)


# ──────────────────────────────────────────────────────────────────────────────
# 10. resolve_login_redirect
# ──────────────────────────────────────────────────────────────────────────────

class TestResolveLoginRedirect:

    def test_admin_goes_to_home(self):
        assert resolve_login_redirect(_user(role="admin")) == "/"

    def test_auditor_goes_to_home(self):
        assert resolve_login_redirect(_user(role="auditor")) == "/"

    def test_manager_goes_to_home(self):
        assert resolve_login_redirect(_user(role="gerente")) == "/"

    def test_analyst_publico_goes_to_mercado_publico(self):
        u = _user(role="analista", access_scope="mercado_publico")
        assert resolve_login_redirect(u) == "/mercado-publico"

    def test_analyst_todos_goes_to_mercado_publico(self):
        u = _user(role="analista", access_scope="todos")
        assert resolve_login_redirect(u) == "/mercado-publico"

    def test_analyst_privado_goes_to_mercado_privado(self):
        u = _user(role="analista", access_scope="privado")
        assert resolve_login_redirect(u) == "/mercado-privado"

    def test_analyst_mercado_privado_scope(self):
        u = _user(role="analista", access_scope="mercado_privado")
        assert resolve_login_redirect(u) == "/mercado-privado"

    def test_supervisor_privado_goes_to_mercado_privado(self):
        u = _user(role="supervisor", access_scope="privado")
        assert resolve_login_redirect(u) == "/mercado-privado"

    def test_supervisor_default_goes_to_mercado_publico(self):
        u = _user(role="supervisor", access_scope="todos")
        assert resolve_login_redirect(u) == "/mercado-publico"

    def test_none_user_goes_to_login(self):
        assert resolve_login_redirect(None) == "/login"


# ──────────────────────────────────────────────────────────────────────────────
# 11. visible_user_scope — tests con DB en memoria
# ──────────────────────────────────────────────────────────────────────────────

@pytest.fixture(scope="module")
def db_session():
    """
    Crea una base de datos SQLite en memoria con datos de prueba.
    """
    from sqlalchemy import create_engine
    from sqlalchemy.orm import sessionmaker
    from web_comparativas.models import Base, User, Group, GroupMember
    import datetime as dt

    engine = create_engine("sqlite:///:memory:", connect_args={"check_same_thread": False})
    Base.metadata.create_all(bind=engine)
    Session = sessionmaker(bind=engine)
    s = Session()

    # Usuarios de prueba
    admin = User(id=1, email="admin@test.com", role="admin", unit_business=None)
    auditor = User(id=2, email="auditor@test.com", role="auditor", unit_business=None)
    supervisor_ph = User(id=3, email="sup_ph@test.com", role="supervisor", unit_business="Productos Hospitalarios")
    analyst_ph_1 = User(id=4, email="ana1_ph@test.com", role="analista", unit_business="Productos Hospitalarios")
    analyst_ph_2 = User(id=5, email="ana2_ph@test.com", role="analista", unit_business="Productos Hospitalarios")
    analyst_te = User(id=6, email="ana_te@test.com", role="analista", unit_business="Tratamientos Especiales")
    supervisor_te = User(id=7, email="sup_te@test.com", role="supervisor", unit_business="Tratamientos Especiales")

    for u in [admin, auditor, supervisor_ph, analyst_ph_1, analyst_ph_2, analyst_te, supervisor_te]:
        u.created_at = dt.datetime.utcnow()
        u.password_hash = "x"
        s.add(u)

    # Grupo: creado por supervisor_ph, BU=Productos Hospitalarios, contiene analyst_ph_1 y analyst_ph_2
    group_ph = Group(
        id=1,
        name="Grupo PH",
        business_unit="Productos Hospitalarios",
        created_by_user_id=3,  # supervisor_ph
        created_at=dt.datetime.utcnow(),
    )
    s.add(group_ph)
    s.flush()

    gm1 = GroupMember(group_id=1, user_id=4, role_in_group="analista", added_by_user_id=3)
    gm2 = GroupMember(group_id=1, user_id=5, role_in_group="analista", added_by_user_id=3)
    s.add(gm1)
    s.add(gm2)

    s.commit()
    yield s
    s.close()


class TestVisibleUserScopeWithDb:

    def test_admin_sees_all_users(self, db_session):
        admin = SimpleNamespace(id=1, role="admin", unit_business=None)
        ids = visible_user_scope(db_session, admin)
        # Debe incluir todos los usuarios creados en el fixture (ids 1-7)
        assert 1 in ids
        assert 2 in ids
        assert 4 in ids
        assert 6 in ids

    def test_auditor_sees_all_users(self, db_session):
        auditor = SimpleNamespace(id=2, role="auditor", unit_business=None)
        ids = visible_user_scope(db_session, auditor)
        assert len(ids) >= 7

    def test_supervisor_ph_sees_analysts_of_same_bu(self, db_session):
        """Supervisor de PH ve a analistas de PH + él mismo."""
        sup = SimpleNamespace(id=3, role="supervisor", unit_business="Productos Hospitalarios")
        ids = visible_user_scope(db_session, sup)
        assert 3 in ids   # él mismo
        assert 4 in ids   # analyst_ph_1
        assert 5 in ids   # analyst_ph_2
        assert 6 not in ids  # analyst de TE — NUNCA visible para sup de PH

    def test_supervisor_does_not_see_other_bu(self, db_session):
        sup = SimpleNamespace(id=3, role="supervisor", unit_business="Productos Hospitalarios")
        ids = visible_user_scope(db_session, sup)
        assert 6 not in ids  # analyst_te (Tratamientos Especiales)
        assert 7 not in ids  # supervisor_te

    def test_analyst_no_group_sees_only_self(self, db_session):
        """Analista sin grupo: solo ve sus propias cargas."""
        # analyst_te (id=6) no está en ningún grupo
        ana = SimpleNamespace(id=6, role="analista", unit_business="Tratamientos Especiales")
        ids = visible_user_scope(db_session, ana)
        assert ids == {6}

    def test_analyst_with_group_sees_group_members(self, db_session):
        """Analista con grupo válido ve a los otros miembros del grupo."""
        # analyst_ph_1 (id=4) está en el grupo PH con analyst_ph_2 (id=5)
        ana = SimpleNamespace(id=4, role="analista", unit_business="Productos Hospitalarios")
        ids = visible_user_scope(db_session, ana)
        assert 4 in ids  # él mismo
        assert 5 in ids  # compañero de grupo

    def test_analyst_never_sees_other_bu(self, db_session):
        """Analista de PH nunca ve datos de TE (frontera BU)."""
        ana = SimpleNamespace(id=4, role="analista", unit_business="Productos Hospitalarios")
        ids = visible_user_scope(db_session, ana)
        assert 6 not in ids  # analyst_te

    def test_analyst_never_sees_supervisor(self, db_session):
        """Analista no debe ver al supervisor dentro de los IDs de grupo."""
        ana = SimpleNamespace(id=4, role="analista", unit_business="Productos Hospitalarios")
        ids = visible_user_scope(db_session, ana)
        # El supervisor (id=3) creó el grupo pero NO debe aparecer en el scope del analista
        assert 3 not in ids

    def test_supervisor_with_no_bu_sees_only_self(self, db_session):
        """Supervisor sin BU: solo se ve a sí mismo (configuración inválida, degrada graciosamente)."""
        sup = SimpleNamespace(id=3, role="supervisor", unit_business="")
        ids = visible_user_scope(db_session, sup)
        assert ids == {3}


# ──────────────────────────────────────────────────────────────────────────────
# 12. Regresión: Analista no ve uploads ajenos en primer render
# ──────────────────────────────────────────────────────────────────────────────

class TestFirstLoginRegression:
    """
    Verifica el bug original: analista nuevo NO debe ver uploads de otros usuarios.
    Este test documenta la regresión para evitar que vuelva.
    """

    def test_new_analyst_scope_does_not_include_strangers(self, db_session):
        """Un analista recién creado solo ve su propio ID."""
        # Analista nuevo sin grupo (id=6, TE), no debe ver a nadie de PH
        new_analyst = SimpleNamespace(id=6, role="analista", unit_business="Tratamientos Especiales")
        ids = visible_user_scope(db_session, new_analyst)
        # No debe incluir IDs de Productos Hospitalarios
        ph_user_ids = {3, 4, 5}  # supervisor_ph, analyst_ph_1, analyst_ph_2
        intersection = ids & ph_user_ids
        assert intersection == set(), (
            f"Analista nuevo NO debe ver usuarios de otra BU: {intersection}"
        )

    def test_analyst_without_group_sees_only_own_id(self, db_session):
        new_analyst = SimpleNamespace(id=6, role="analista", unit_business="Tratamientos Especiales")
        ids = visible_user_scope(db_session, new_analyst)
        assert ids == {6}

    def test_resolve_login_redirect_never_sends_analyst_to_unfiltered_home(self):
        """El redirect de login NUNCA debe enviar a un analista a '/'."""
        for scope in ["todos", "mercado_publico", "mercado_privado", "privado", ""]:
            u = _user(role="analista", access_scope=scope)
            redirect = resolve_login_redirect(u)
            assert redirect != "/", (
                f"Analista con scope='{scope}' fue redirigido a '/' (ruta sin filtrar)"
            )

    def test_resolve_login_redirect_never_sends_supervisor_to_unfiltered_home(self):
        """El redirect de login NUNCA debe enviar a un supervisor a '/'."""
        for scope in ["todos", "mercado_publico", "mercado_privado", "privado", ""]:
            u = _user(role="supervisor", access_scope=scope)
            redirect = resolve_login_redirect(u)
            assert redirect != "/", (
                f"Supervisor con scope='{scope}' fue redirigido a '/' (ruta sin filtrar)"
            )
