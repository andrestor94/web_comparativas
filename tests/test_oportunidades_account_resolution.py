from __future__ import annotations

import os
import sys
from contextlib import contextmanager
from types import SimpleNamespace

import pytest

sys.path.insert(0, os.path.dirname(os.path.dirname(__file__)))

from web_comparativas.dimensionamiento import crm_client
from web_comparativas.dimensionamiento import account_resolution as account_resolution_module
from web_comparativas.dimensionamiento.account_resolution import (
    AccountSelectionError,
    MasterIndex,
    RunIdentityIndex,
    normalize_cuit,
    normalize_identifier,
    relationship_candidates,
    select_account_resolution,
)


def candidate(code: str, operator: str | None = None) -> dict:
    return {
        "cuenta": code,
        "codigo_relacionado": code,
        "cliente_maestro": "SANATORIO DEL OESTE",
        "fantasia": None,
        "operador_codigo": "77" if operator else None,
        "operador_nombre": operator,
        "fuentes": ["test"],
        "existe_en_crm": None,
        "crm_account_id": None,
        "crm_nombre": None,
    }


def relation(*codes: str, original: str = "32059", valid: bool = True) -> dict:
    return {
        "cuit": "30595201027",
        "cuenta_original": original,
        "nombres_relacionados": ["SANATORIO DEL OESTE"],
        "cuentas_candidatas": [candidate(code, "AYELEN PILUSO" if code == "8519" else None) for code in codes],
        "fuente_relacion": "CUIT -> nombre legal canonico -> clientes.codigo",
        "relacion_valida": valid,
        "advertencias": [],
    }


def found(code: str) -> dict:
    return {"exists": True, "crm_account_id": f"crm-{code}", "crm_nombre": f"Cuenta {code}"}


def test_identifier_normalization_preserves_text_identity():
    assert normalize_identifier("00123") == "00123"
    assert normalize_identifier("123.0") == "123"
    assert normalize_identifier("1.23E+4") == "12300"
    assert normalize_identifier(123.0) == "123"
    assert normalize_identifier("SIN DATO") is None
    assert normalize_cuit("30-59520102-7") == "30595201027"
    assert normalize_cuit("123") is None


class FakeResult:
    def __init__(self, rows):
        self.rows = rows

    def all(self):
        return self.rows


class FakeDb:
    def __init__(self, rows):
        self.rows = rows

    def execute(self, _statement):
        return FakeResult(self.rows)


def test_relationship_uses_same_cuit_exact_legal_name_and_deduplicates():
    master = MasterIndex(
        clients_by_name={
            "SANATORIO DEL OESTE": (
                {"codigo": "32059", "nombre": "SANATORIO DEL OESTE", "fantasia": "", "grupo": "1", "cliente_grupo": "", "nombre_grupo": "", "tipocli": ""},
                {"codigo": "8519", "nombre": "SANATORIO DEL OESTE", "fantasia": "SANATORIO", "grupo": "999", "cliente_grupo": "", "nombre_grupo": "NO USAR", "tipocli": ""},
            ),
        },
        operators_by_code={"8519": {"codigo_cuenta": "8519", "fantasia": None, "vendedor_codigo": "77", "nombre": "AYELEN PILUSO"}},
        signature=(),
    )
    db = FakeDb([
        ("30-59520102-7", "Sanatorio del Oeste", "SANATORIO DEL OESTE", "32059.0"),
        ("30-59520102-7", "Sanatorio del Oeste", "SANATORIO DEL OESTE", "32059"),
        ("30-00000000-0", "Otro", "OTRO", "999"),
    ])
    opportunity = SimpleNamespace(cuit="30595201027", cuenta_interna="32059", cliente_visible="Sanatorio del Oeste")
    identity = RunIdentityIndex(
        names_by_cuit={"30595201027": ("SANATORIO DEL OESTE",)},
        cuits_by_name={"SANATORIO DEL OESTE": ("30595201027",)},
        raw_names_by_key={"SANATORIO DEL OESTE": ("SANATORIO DEL OESTE",)},
        accounts_by_cuit={"30595201027": ("32059",)},
    )
    result = relationship_candidates(db, 10, opportunity, master=master, identity=identity)
    assert [row["cuenta"] for row in result["cuentas_candidatas"]] == ["32059", "8519"]
    assert result["cuentas_candidatas"][1]["operador_nombre"] == "AYELEN PILUSO"
    assert result["relacion_valida"] is True
    assert "grupo" not in result["fuente_relacion"].lower()


def test_original_account_wins_and_alternatives_need_not_be_selected():
    result = select_account_resolution(
        relation("32059", "8519"), {"32059": found("32059")}, crm_mode="test",
    )
    assert result["cuenta_seleccionada"]["cuenta"] == "32059"
    assert result["criterio_seleccion"] == "cuenta_original_existente"
    assert result["bloqueado"] is False


def test_single_valid_alternative_is_selected_with_trace_and_operator():
    result = select_account_resolution(
        relation("32059", "8519"), {"32059": {"exists": False}, "8519": found("8519")}, crm_mode="test",
    )
    assert result["cuenta_seleccionada"]["cuenta"] == "8519"
    assert result["criterio_seleccion"] == "unica_alternativa_valida"
    assert result["cuenta_seleccionada"]["operador_nombre"] == "AYELEN PILUSO"
    assert "Cuenta original 32059" in result["trazabilidad_texto"]
    assert "CRM no confirmó identidad fiscal" in result["trazabilidad_texto"]


def test_multiple_alternatives_require_explicit_valid_selection():
    crm = {"32059": {"exists": False}, "8519": found("8519"), "9000": found("9000")}
    unresolved = select_account_resolution(relation("32059", "8519", "9000"), crm, crm_mode="prod")
    assert unresolved["cuenta_seleccionada"] is None
    assert unresolved["requiere_seleccion"] is True
    assert unresolved["estado_confianza"] == "RELACION_AMBIGUA"
    assert unresolved["bloqueado"] is True
    resolved = select_account_resolution(
        relation("32059", "8519", "9000"), crm, crm_mode="prod", requested_account="9000",
    )
    assert resolved["cuenta_seleccionada"]["cuenta"] == "9000"
    assert resolved["criterio_seleccion"] == "seleccion_manual_entre_alternativas"
    assert resolved["trazabilidad_texto"] == resolved["cuenta_seleccionada"]["trazabilidad_seleccion"]


def test_manipulated_or_non_original_selection_is_rejected():
    with pytest.raises(AccountSelectionError, match="no pertenece"):
        select_account_resolution(
            relation("32059", "8519"), {"32059": {"exists": False}, "8519": found("8519")},
            crm_mode="test", requested_account="HACK",
        )
    with pytest.raises(AccountSelectionError, match="original existe"):
        select_account_resolution(
            relation("32059", "8519"), {"32059": found("32059")},
            crm_mode="test", requested_account="8519",
        )


def test_none_partial_invalid_and_simulated_are_safe():
    none = select_account_resolution(
        relation("32059", "8519"), {"32059": {"exists": False}, "8519": {"exists": False}}, crm_mode="test",
    )
    assert none["bloqueado"] is True and none["criterio_seleccion"] == "sin_relacion"
    partial = select_account_resolution(
        relation("32059", "8519"), {"32059": {"exists": False}, "8519": {"exists": None, "error": "timeout"}}, crm_mode="test",
    )
    assert partial["bloqueado"] is True and partial["estado_confianza"] == "ERROR_CONSULTA_CRM"
    invalid = select_account_resolution(relation("32059", valid=False), {}, crm_mode="prod")
    assert invalid["bloqueado"] is True and invalid["estado_confianza"] == "SIN_RELACION"
    simulated = select_account_resolution(relation("32059", "8519"), {}, crm_mode="simulado")
    assert simulated["cuenta_seleccionada"]["cuenta"] == "32059"
    assert simulated["criterio_seleccion"] == "simulado_cuenta_original"


def test_batch_lookup_reuses_one_session_and_stops_when_original_exists(monkeypatch):
    calls = []

    @contextmanager
    def fake_session(_cfg):
        calls.append("session")
        yield object()

    monkeypatch.setattr(crm_client, "crm_config", lambda: {"modo": "test", "base_url": "https://crm.test"})
    monkeypatch.setattr(crm_client, "_nueva_sesion", fake_session)
    monkeypatch.setattr(crm_client, "obtener_token", lambda *_args: "token")
    monkeypatch.setattr(crm_client, "buscar_cuenta", lambda *_args: {"id": "id-original", "name": "Original", "n_cuenta_c": "32059"})
    result = crm_client.consultar_cuentas(["32059", "8519"], detener_si_primera_existe=True)
    assert calls == ["session"]
    assert list(result["results"]) == ["32059"]
    assert result["results"]["32059"]["crm_account_id"] == "id-original"


def test_batch_lookup_distinguishes_absence_from_operational_error(monkeypatch):
    @contextmanager
    def fake_session(_cfg):
        yield object()

    def fake_lookup(_session, _cfg, _token, number):
        if number == "32059":
            raise crm_client.CrmError("missing", kind="cuenta_no_encontrada", paso="cuenta")
        raise crm_client.CrmError("timeout", kind="red", paso="cuenta", reintentable=True)

    monkeypatch.setattr(crm_client, "crm_config", lambda: {"modo": "prod", "base_url": "https://crm.prod"})
    monkeypatch.setattr(crm_client, "_nueva_sesion", fake_session)
    monkeypatch.setattr(crm_client, "obtener_token", lambda *_args: "token")
    monkeypatch.setattr(crm_client, "buscar_cuenta", fake_lookup)
    result = crm_client.consultar_cuentas(["32059", "8519"])
    assert result["crm_modo"] == "prod"
    assert result["results"]["32059"] == {"exists": False}
    assert result["results"]["8519"]["exists"] is None
    assert result["results"]["8519"]["reintentable"] is True


def test_frontend_requires_read_only_resolution_before_posting():
    path = os.path.join(os.path.dirname(os.path.dirname(__file__)), "web_comparativas", "static", "js", "mercado_privado_oportunidades.js")
    source = open(path, encoding="utf-8").read()
    assert "ACCOUNT_API(o.id)" in source
    assert 'query.set("cuenta_seleccionada", account.cuenta)' in source
    assert 'resolution.requiere_seleccion' in source
    assert 'payload.cuenta_original' in source
    assert 'payload.cuenta_utilizada' in source

def test_canon_is_exact_and_does_not_strip_legal_suffixes_or_branches():
    from web_comparativas.dimensionamiento.identity import canon

    assert canon("Clínica  Norte S.A.") == "CLINICA NORTE S A"
    assert canon("CLINICA NORTE SRL") == "CLINICA NORTE SRL"
    assert canon("Clínica Norte - Sucursal Centro") == "CLINICA NORTE SUCURSAL CENTRO"
    assert canon("Clínica Norte S.A.") != canon("Clínica Norte SRL")
    assert canon("Clínica Norte") != canon("Clínica Norte Sucursal Centro")


def test_name_shared_by_multiple_cuits_blocks_every_alternative():
    master = MasterIndex(
        clients_by_name={"CLINICA NORTE": ({"codigo": "900", "nombre": "CLINICA NORTE", "fantasia": "", "grupo": "", "cliente_grupo": "", "nombre_grupo": "", "tipocli": ""},)},
        operators_by_code={}, signature=(),
    )
    identity = RunIdentityIndex(
        names_by_cuit={"30111111119": ("CLINICA NORTE",)},
        cuits_by_name={"CLINICA NORTE": ("30111111119", "30222222228")},
        raw_names_by_key={"CLINICA NORTE": ("CLINICA NORTE",)},
        accounts_by_cuit={"30111111119": ("100",)},
    )
    opportunity = SimpleNamespace(cuit="30111111119", cuenta_interna="100", cliente_visible="Clínica Norte")
    rel = relationship_candidates(FakeDb([]), 1, opportunity, master=master, identity=identity)
    assert rel["relacion_ambigua"] is True
    assert rel["motivos_ambiguedad"] == ["NOMBRE_CANONICO_COMPARTIDO_POR_MULTIPLES_CUIT"]
    assert [row["cuenta"] for row in rel["cuentas_candidatas"]] == ["100"]
    assert rel["cantidad_candidatas_total"] == 2
    result = select_account_resolution(rel, {"100": found("100")}, crm_mode="test")
    assert result["estado_confianza"] == "RELACION_AMBIGUA"
    assert result["cuenta_seleccionada"] is None


def test_cuit_with_incompatible_names_and_generic_key_are_blocked():
    empty = MasterIndex(clients_by_name={}, operators_by_code={}, signature=())
    multiple = RunIdentityIndex(
        names_by_cuit={"30111111119": ("CLINICA NORTE", "SANATORIO SUR")},
        cuits_by_name={"CLINICA NORTE": ("30111111119",), "SANATORIO SUR": ("30111111119",)},
        raw_names_by_key={"CLINICA NORTE": ("CLINICA NORTE",), "SANATORIO SUR": ("SANATORIO SUR",)},
        accounts_by_cuit={"30111111119": ("100",)},
    )
    opportunity = SimpleNamespace(cuit="30111111119", cuenta_interna="100", cliente_visible="Cliente")
    rel = relationship_candidates(FakeDb([]), 1, opportunity, master=empty, identity=multiple)
    assert "CUIT_CON_MULTIPLES_NOMBRES_HOMOLOGADOS" in rel["motivos_ambiguedad"]

    generic = RunIdentityIndex(
        names_by_cuit={"30111111119": ("HOSPITAL PRIVADO CENTRO MEDICO",)},
        cuits_by_name={"HOSPITAL PRIVADO CENTRO MEDICO": ("30111111119",)},
        raw_names_by_key={"HOSPITAL PRIVADO CENTRO MEDICO": ("HOSPITAL PRIVADO CENTRO MEDICO",)},
        accounts_by_cuit={"30111111119": ("100",)},
    )
    rel_generic = relationship_candidates(FakeDb([]), 1, opportunity, master=empty, identity=generic)
    assert "CLAVE_NOMINAL_SIN_TERMINOS_DISTINTIVOS" in rel_generic["motivos_ambiguedad"]
    assert rel_generic["cantidad_candidatas_total"] == 1


def test_more_than_25_candidates_are_not_truncated_and_are_blocked():
    key = "EMPRESA DISTINTIVA DEL NORTE"
    rows = tuple(
        {"codigo": str(index), "nombre": key, "fantasia": "", "grupo": "", "cliente_grupo": "", "nombre_grupo": "", "tipocli": ""}
        for index in range(100, 130)
    )
    master = MasterIndex(clients_by_name={key: rows}, operators_by_code={}, signature=())
    identity = RunIdentityIndex(
        names_by_cuit={"30111111119": (key,)}, cuits_by_name={key: ("30111111119",)},
        raw_names_by_key={key: (key,)}, accounts_by_cuit={"30111111119": ("100",)},
    )
    opportunity = SimpleNamespace(cuit="30111111119", cuenta_interna="100", cliente_visible=key)
    rel = relationship_candidates(FakeDb([]), 1, opportunity, master=master, identity=identity)
    assert rel["cantidad_candidatas_total"] == 30
    assert len(rel["cuentas_candidatas"]) == 30
    assert rel["exceso_candidatos"] is True
    result = select_account_resolution(rel, {}, crm_mode="test")
    assert result["estado_confianza"] == "RELACION_AMBIGUA"
    assert result["cuenta_seleccionada"] is None


def test_name_only_alternative_never_claims_fiscal_confirmation():
    rel = relation("32059", "8519")
    rel["cuentas_candidatas"][1]["relacion_confianza"] = "relacionada_por_nombre_exacto_no_ambiguo"
    result = select_account_resolution(
        rel, {"32059": {"exists": False}, "8519": found("8519")}, crm_mode="test",
    )
    assert result["estado_confianza"] == "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO"
    assert result["confirmacion_fiscal"] is False
    assert "únicamente por igualdad exacta" in result["trazabilidad_texto"]


def test_crm_matching_cuit_confirms_alternative_and_mismatch_blocks():
    rel = relation("32059", "8519")
    rel["cuentas_candidatas"][1]["relacion_confianza"] = "relacionada_por_nombre_exacto_no_ambiguo"
    matching = found("8519") | {"crm_cuit": "30-59520102-7"}
    result = select_account_resolution(
        rel, {"32059": {"exists": False}, "8519": matching}, crm_mode="test",
    )
    assert result["estado_confianza"] == "ALTERNATIVA_CONFIRMADA_POR_CUIT"
    assert result["confirmacion_fiscal"] is True

    mismatch = found("8519") | {"crm_cuit": "30-00000000-0"}
    blocked = select_account_resolution(
        relation("32059", "8519"), {"32059": {"exists": False}, "8519": mismatch}, crm_mode="test",
    )
    assert blocked["estado_confianza"] == "RELACION_AMBIGUA"
    assert "CUIT_CRM_DIVERGENTE" in blocked["motivos_ambiguedad"]
    assert blocked["cuenta_seleccionada"] is None

def test_dataset_same_cuit_alternative_is_not_fiscally_confirmed_without_crm_cuit():
    linked = relation("32059", "8519")
    linked["cuentas_candidatas"][1]["relacion_confianza"] = "confirmada_por_cuit_dataset"
    result = select_account_resolution(
        linked,
        {"32059": {"exists": False}, "8519": found("8519")},
        crm_mode="test",
    )
    assert result["estado_confianza"] == "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO"
    assert result["confirmacion_fiscal"] is False

def test_master_cache_keeps_last_valid_index_when_files_change_during_reload(monkeypatch):
    previous = MasterIndex(clients_by_name={}, operators_by_code={}, signature=("old",))
    loaded = MasterIndex(clients_by_name={}, operators_by_code={}, signature=("new-after",))
    monkeypatch.setattr(account_resolution_module, "_master_cache", previous)
    monkeypatch.setattr(account_resolution_module, "_file_signature", lambda *_paths: ("new-before",))
    monkeypatch.setattr(account_resolution_module, "_load_master_index", lambda *_paths: loaded)

    with pytest.raises(ValueError, match="cambiaron durante la lectura"):
        account_resolution_module.get_master_index(SimpleNamespace(), SimpleNamespace())

    assert account_resolution_module._master_cache is previous
