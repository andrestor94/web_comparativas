"""Match: la cola muestra solo lo pendiente (artículos y candidatas) y la monodroga
viene del mapa chico, sin tocar conteos ni ranking."""
import datetime as dt

import pytest
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.match.models import (
    DECISION_DESCARTADO,
    DECISION_HOMOLOGADO,
    EVENTO_PAPELERA_ELIMINADO,
    MATCH_RUN_APPROVED,
    MatchDemandaDesc,
    MatchHomologacion,
    MatchHomologacionEvento,
    MatchImportRun,
    MatchMonodrogaMap,
    MatchPropuesta,
)
from web_comparativas.match.service import detalle_articulo, listar_articulos, match_desempeno
from web_comparativas.models import Base, User


@pytest.fixture()
def db():
    engine = create_engine(
        "sqlite:///:memory:",
        connect_args={"check_same_thread": False},
        poolclass=StaticPool,
    )
    Base.metadata.create_all(
        engine,
        tables=[
            User.__table__,
            MatchImportRun.__table__,
            MatchPropuesta.__table__,
            MatchHomologacion.__table__,
            MatchHomologacionEvento.__table__,
            MatchDemandaDesc.__table__,
            MatchMonodrogaMap.__table__,
        ],
    )
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


NOW = dt.datetime.utcnow()


def _seed(db):
    run = MatchImportRun(
        source_path="run.csv", status=MATCH_RUN_APPROVED,
        started_at=NOW, finished_at=NOW, approved_at=NOW,
    )
    db.add(run)
    db.flush()
    props = [
        # artículo 1: todo decidido → sale de la cola
        ("P1A", "1", "A", 0.95),
        ("P1B", "1", "B", 0.75),
        # artículo 2: una decidida, una pendiente → queda, con 1 pendiente de 2
        ("P2A", "2", "A", 0.90),
        ("P2B", "2", "C", 0.60),
        # artículo 3: nada decidido
        ("P3A", "3", "D", 0.45),
        # artículo 4: decidido para OTRO código → su par sigue pendiente
        ("P4A", "4", "B", 0.80),
    ]
    for prod, code, lvl, score in props:
        db.add(MatchPropuesta(
            import_run_id=run.id, producto_plataforma=prod, candidato_codigo=code,
            candidato_descripcion=f"Artículo {code}", nivel_confianza=lvl, score_mejor=score,
        ))

    def dec(prod, code, decision, when=NOW):
        db.add(MatchHomologacion(
            producto_plataforma=prod, codigo_elegido=code, decision=decision,
            usuario="ana@example.com", import_run_id=run.id, created_at=when, updated_at=when,
        ))

    dec("P1A", "1", DECISION_HOMOLOGADO)
    dec("P1B", "1", DECISION_DESCARTADO)
    dec("P2A", "2", DECISION_DESCARTADO, NOW - dt.timedelta(hours=30))  # fuera de papelera
    dec("P4A", "99", DECISION_HOMOLOGADO)
    db.add(MatchMonodrogaMap(codigo="2", monodroga="interferón alfa 2b"))
    db.commit()
    return run


def test_lista_solo_articulos_con_pendientes(db):
    _seed(db)
    data = listar_articulos(db)

    assert data["total"] == 3
    assert [a["candidato_codigo"] for a in data["articulos"]] == ["2", "4", "3"]
    art2 = data["articulos"][0]
    # Agregados sobre TODAS las propuestas (el orden no se mueve al decidir).
    assert art2["n_descripciones"] == 2
    assert art2["n_pendientes"] == 1
    assert art2["mejor_score"] == pytest.approx(0.90)
    assert art2["mejor_nivel"] == "A"
    assert art2["monodroga"] == "interferón alfa 2b"
    assert data["articulos"][2]["monodroga"] is None


def test_filtro_nivel_cuenta_pendientes_de_ese_nivel(db):
    _seed(db)
    # Nivel A: el artículo 1 y el 2 tienen su A decidida → ninguno queda.
    assert listar_articulos(db, nivel="A")["total"] == 0
    assert [a["candidato_codigo"] for a in listar_articulos(db, nivel="C")["articulos"]] == ["2"]


def test_detalle_trae_decididas_con_recuperable(db):
    _seed(db)
    det = detalle_articulo(db, "1")
    estados = {p["producto_plataforma"]: p["homologacion"] for p in det["propuestas"]}
    assert estados["P1A"]["decision"] == DECISION_HOMOLOGADO
    assert estados["P1A"]["recuperable"] is None
    assert estados["P1B"]["decision"] == DECISION_DESCARTADO
    assert estados["P1B"]["recuperable"] is True

    det2 = detalle_articulo(db, "2")
    assert det2["monodroga"] == "interferón alfa 2b"
    estados2 = {p["producto_plataforma"]: p["homologacion"] for p in det2["propuestas"]}
    assert estados2["P2A"]["recuperable"] is False   # > 24 h: ya no está en la papelera
    assert estados2["P2B"] is None


def test_descartada_eliminada_de_papelera_no_es_recuperable(db):
    _seed(db)
    db.add(MatchHomologacionEvento(
        producto_plataforma="P1B", decision=EVENTO_PAPELERA_ELIMINADO,
        usuario="ana@example.com", created_at=NOW + dt.timedelta(seconds=1),
    ))
    db.commit()
    estados = {p["producto_plataforma"]: p["homologacion"] for p in detalle_articulo(db, "1")["propuestas"]}
    assert estados["P1B"]["recuperable"] is False


def test_tablero_no_cambia(db):
    _seed(db)
    board = match_desempeno(db, usuario="ana@example.com")
    assert board["avance"]["total"] == 6
    assert board["avance"]["homologadas"] == 1      # P4A es de otro código: no cuenta
    assert board["avance"]["descartadas"] == 2
    assert board["avance"]["articulos"] == 4        # incluye el artículo resuelto


def test_push_monodroga_map_reset_e_idempotente(db):
    from web_comparativas.routers.match_router import match_admin_apply_data_chunk

    db.add(MatchMonodrogaMap(codigo="viejo", monodroga="vieja"))
    db.commit()
    lote1 = {"kind": "monodroga-map", "reset": True,
             "rows": [["1", "paclitaxel"], ["2", "interferón alfa 2b"], ["3", ""], [None, "x"]]}
    r = match_admin_apply_data_chunk(payload=lote1, _token="t", db=db)
    assert r["insertadas"] == 2 and r["total"] == 2          # reset borró 'viejo'; vacíos afuera
    # Reintento del mismo lote sin reset + lote 2: no duplica.
    match_admin_apply_data_chunk(payload={**lote1, "reset": False}, _token="t", db=db)
    r = match_admin_apply_data_chunk(
        payload={"kind": "monodroga-map", "rows": [["4", "sirolimus"]]}, _token="t", db=db)
    assert r["total"] == 3
    assert db.get(MatchMonodrogaMap, "2").monodroga == "interferón alfa 2b"
