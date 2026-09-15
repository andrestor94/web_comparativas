import datetime as dt

import pytest
from sqlalchemy import create_engine, select
from sqlalchemy.orm import sessionmaker
from sqlalchemy.pool import StaticPool

from web_comparativas.match.models import (
    MATCH_RUN_APPROVED,
    MatchDemandaDesc,
    MatchHomologacion,
    MatchImportRun,
    MatchPropuesta,
)
from web_comparativas.match.service import detalle_articulo, match_desempeno, match_resumen
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
            MatchDemandaDesc.__table__,
        ],
    )
    session = sessionmaker(bind=engine)()
    yield session
    session.close()


def _run(db, source, finished):
    run = MatchImportRun(
        source_path=source,
        status=MATCH_RUN_APPROVED,
        started_at=finished,
        finished_at=finished,
        approved_at=finished,
    )
    db.add(run)
    db.flush()
    return run


def _proposal(db, run, product, code, level):
    db.add(
        MatchPropuesta(
            import_run_id=run.id,
            producto_plataforma=product,
            candidato_codigo=code,
            candidato_descripcion=f"Artículo {code}",
            nivel_confianza=level,
        )
    )


def test_new_run_preserves_matching_work_and_excludes_orphans(db):
    old_time = dt.datetime(2026, 6, 25, 12, 0)
    old = _run(db, "old.xlsx", old_time)
    new = _run(db, "new.csv", dt.datetime(2026, 9, 15, 12, 0))

    _proposal(db, old, "PRODUCTO CONSERVADO", "1", "A")
    _proposal(db, old, "PRODUCTO HUERFANO", "2", "B")
    _proposal(db, old, "CANDIDATO CAMBIADO", "3", "C")
    _proposal(db, new, "PRODUCTO CONSERVADO", "1", "A")
    _proposal(db, new, "CANDIDATO CAMBIADO", "4", "C")
    _proposal(db, new, "PROPUESTA NUEVA", "5", "D")

    db.add_all(
        [
            MatchHomologacion(
                producto_plataforma="PRODUCTO CONSERVADO",
                codigo_elegido="1",
                decision="homologado",
                usuario="alice@example.com",
                import_run_id=old.id,
                created_at=old_time,
                updated_at=old_time,
            ),
            MatchHomologacion(
                producto_plataforma="PRODUCTO HUERFANO",
                codigo_elegido="2",
                decision="homologado",
                usuario="bob@example.com",
                import_run_id=old.id,
                created_at=old_time,
                updated_at=old_time,
            ),
            MatchHomologacion(
                producto_plataforma="CANDIDATO CAMBIADO",
                codigo_elegido="3",
                decision="homologado",
                usuario="carol@example.com",
                import_run_id=old.id,
                created_at=old_time,
                updated_at=old_time,
            ),
        ]
    )
    db.commit()

    summary = match_resumen(db)
    board = match_desempeno(db)
    changed = detalle_articulo(db, "4")

    assert summary == {
        "run_id": new.id,
        "total_propuestas": 3,
        "total_articulos": 3,
        "total_homologadas": 1,
        "pct_homologadas": 33.3,
        "conteo_por_nivel": {"A": 1, "C": 1, "D": 1},
    }
    assert board["avance"]["homologadas"] == 1
    assert board["avance"]["total"] == 3
    assert board["avance"]["pct"] == 33.3
    assert board["avance"]["por_nivel"]["D"]["total"] == 1
    assert [(row["usuario"], row["total"]) for row in board["ranking"]["top"]] == [
        ("alice@example.com", 1)
    ]
    assert changed["propuestas"][0]["homologacion"] is None

    rows = db.execute(
        select(
            MatchHomologacion.usuario,
            MatchHomologacion.import_run_id,
            MatchHomologacion.created_at,
            MatchHomologacion.updated_at,
        ).order_by(MatchHomologacion.usuario)
    ).all()
    assert rows == [
        ("alice@example.com", old.id, old_time, old_time),
        ("bob@example.com", old.id, old_time, old_time),
        ("carol@example.com", old.id, old_time, old_time),
    ]
