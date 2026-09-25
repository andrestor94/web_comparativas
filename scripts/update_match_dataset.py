"""Actualiza de forma segura la corrida local de Match desde CSV.

La actualización es aditiva respecto del historial: crea una corrida nueva, importa
las propuestas únicas y clasifica las decisiones vigentes entre conservadas y
huérfanas. No borra ni modifica homologaciones o eventos anteriores.

Uso:
    python -m scripts.update_match_dataset --csv-path "ruta/propuestas.csv" --approve

Arrastre de decisiones (--carry-decisions local|prod): los pares (producto_plataforma,
candidato_codigo) con decisión vigente (homologado o descartado) que el CSV nuevo no
trae se agregan a la corrida nueva con el nivel, scores y descripción que tenían en la
corrida vigente de esa fuente. Así ninguna decisión queda huérfana y el ranking no baja.
`prod` lee las decisiones de producción en una sesión de SOLO LECTURA
(DATABASE_URL_EXTERNAL de web_comparativas/.env); no escribe nada en prod.

    python -m scripts.update_match_dataset --csv-path "..." --approve --carry-decisions prod

Para sumar decisiones tomadas después del import (idempotente, sin CSV):

    python -m scripts.update_match_dataset --carry-into-run 6 --carry-decisions prod
"""
from __future__ import annotations

import argparse
import csv
import datetime as dt
import sys
from collections import Counter
from pathlib import Path

from sqlalchemy import func, select
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.dialects.sqlite import insert as sqlite_insert

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from web_comparativas.match.models import (  # noqa: E402
    MATCH_RUN_APPROVED,
    MATCH_RUN_FAILED,
    MATCH_RUN_PENDING,
    MatchHomologacion,
    MatchImportRun,
    MatchPropuesta,
)
from web_comparativas.models import IS_SQLITE, SessionLocal  # noqa: E402


BATCH_SIZE = 5_000
REQUIRED_COLUMNS = {
    "producto_plataforma",
    "nivel_confianza",
    "score_mejor",
    "candidato_1_codigo",
    "candidato_1_descripcion",
    "candidato_1_score_tfidf",
    "candidato_1_score_fuzzy",
    "candidato_1_score_pharma",
}


def _text(value: str | None) -> str | None:
    value = (value or "").strip()
    return value or None


def _float(value: str | None) -> float | None:
    value = (value or "").strip()
    if not value:
        return None
    try:
        return float(value)
    except ValueError:
        return None


def _insert_batch(db, batch: list[dict]) -> None:
    if not batch:
        return
    if IS_SQLITE:
        stmt = sqlite_insert(MatchPropuesta).on_conflict_do_nothing()
    else:
        stmt = pg_insert(MatchPropuesta).on_conflict_do_nothing(
            constraint="uq_match_propuestas_run_prod_cand"
        )
    db.execute(stmt, batch)
    db.commit()


def _latest_approved_run_id(db) -> int | None:
    return db.execute(
        select(MatchImportRun.id)
        .where(MatchImportRun.status == MATCH_RUN_APPROVED)
        .order_by(MatchImportRun.finished_at.desc(), MatchImportRun.id.desc())
        .limit(1)
    ).scalar_one_or_none()


_PROPUESTA_COLS = (
    "producto_plataforma",
    "candidato_codigo",
    "nivel_confianza",
    "score_mejor",
    "candidato_descripcion",
    "score_tfidf",
    "score_fuzzy",
    "score_pharma",
)


def _decided_pairs_local(db) -> tuple[list[dict], int]:
    """Decisiones vigentes de la base local + su propuesta en la corrida vigente local.
    Devuelve (pares con propuesta, decisiones sin propuesta de origen)."""
    source_run = _latest_approved_run_id(db)
    H, P = MatchHomologacion, MatchPropuesta
    rows = db.execute(
        select(H.decision, *(getattr(P, c) for c in _PROPUESTA_COLS))
        .select_from(H)
        .outerjoin(
            P,
            (P.import_run_id == source_run)
            & (P.producto_plataforma == H.producto_plataforma)
            & (P.candidato_codigo == H.codigo_elegido),
        )
    ).all()
    found = [dict(zip(("decision", *_PROPUESTA_COLS), r)) for r in rows if r[1] is not None]
    return found, len(rows) - len(found)


def _decided_pairs_prod() -> tuple[list[dict], int]:
    """Igual que _decided_pairs_local pero contra PRODUCCIÓN, en sesión de SOLO
    LECTURA (default_transaction_read_only). Solo SELECT.

    Si el par decidido no está en la corrida vigente de prod (p. ej. se decidió sobre
    la corrida anterior mientras corría el push de la nueva), se toman nivel y scores
    de la corrida aprobada más reciente de prod que lo tenga."""
    import os

    import psycopg2
    from dotenv import dotenv_values

    url = os.getenv("DATABASE_URL_EXTERNAL") or dotenv_values(
        PROJECT_ROOT / "web_comparativas" / ".env"
    ).get("DATABASE_URL_EXTERNAL")
    if not url:
        raise SystemExit("[MATCH][UPDATE] Falta DATABASE_URL_EXTERNAL para leer decisiones de prod.")
    con = psycopg2.connect(url, options="-c default_transaction_read_only=on", connect_timeout=30)
    try:
        con.set_session(readonly=True, autocommit=False)
        cur = con.cursor()
        cur.execute(
            "SELECT id FROM match_import_runs WHERE status = %s "
            "ORDER BY finished_at DESC, id DESC",
            (MATCH_RUN_APPROVED,),
        )
        approved_runs = [r[0] for r in cur.fetchall()]  # vigente primero
        cur.execute("SELECT id FROM match_homologaciones")
        pending = {r[0] for r in cur.fetchall()}
        found: list[dict] = []
        for source_run in approved_runs:
            if not pending:
                break
            cur.execute(
                "SELECT h.id, h.decision, " + ", ".join(f"p.{c}" for c in _PROPUESTA_COLS) + " "
                "FROM match_homologaciones h "
                "JOIN match_propuestas p ON p.import_run_id = %s "
                " AND p.producto_plataforma = h.producto_plataforma "
                " AND p.candidato_codigo = h.codigo_elegido",
                (source_run,),
            )
            for hid, *row in cur.fetchall():
                if hid in pending:
                    pending.discard(hid)
                    found.append(dict(zip(("decision", *_PROPUESTA_COLS), row)))
        con.rollback()
    finally:
        con.close()
    return found, len(pending)


def _carry_decided_pairs(db, run_id: int, source: str) -> dict:
    """Agrega a `run_id` los pares decididos que no estén (insert idempotente por el
    UNIQUE de la corrida). No toca match_homologaciones."""
    decided, sin_origen = (
        _decided_pairs_prod() if source == "prod" else _decided_pairs_local(db)
    )
    present = {
        (p, c)
        for p, c in db.execute(
            select(MatchPropuesta.producto_plataforma, MatchPropuesta.candidato_codigo)
            .where(MatchPropuesta.import_run_id == run_id)
        ).all()
    }
    now = dt.datetime.utcnow()
    missing = [
        d for d in decided
        if (d["producto_plataforma"], d["candidato_codigo"]) not in present
    ]
    by_decision = Counter(d["decision"] for d in missing)
    rows = [
        {"import_run_id": run_id, "created_at": now, **{c: d[c] for c in _PROPUESTA_COLS}}
        for d in missing
    ]
    for i in range(0, len(rows), BATCH_SIZE):
        _insert_batch(db, rows[i:i + BATCH_SIZE])
    return {
        "carry_source": source,
        "carry_decisiones_fuente": len(decided) + sin_origen,
        "carry_ya_presentes": len(decided) - len(missing),
        "carry_traidos": len(missing),
        "carry_traidos_por_decision": dict(sorted(by_decision.items())),
        "carry_sin_propuesta_origen": sin_origen,
    }


def _refresh_run_counts(db, run: MatchImportRun) -> dict:
    run_id = run.id
    run.rows_inserted = int(
        db.execute(
            select(func.count(MatchPropuesta.id)).where(MatchPropuesta.import_run_id == run_id)
        ).scalar_one()
        or 0
    )
    run.articulos_distintos = int(
        db.execute(
            select(func.count(func.distinct(MatchPropuesta.candidato_codigo))).where(
                MatchPropuesta.import_run_id == run_id,
                MatchPropuesta.candidato_codigo.isnot(None),
            )
        ).scalar_one()
        or 0
    )
    run.counts_by_nivel = {
        (level or "?"): int(count)
        for level, count in db.execute(
            select(MatchPropuesta.nivel_confianza, func.count(MatchPropuesta.id))
            .where(MatchPropuesta.import_run_id == run_id)
            .group_by(MatchPropuesta.nivel_confianza)
        ).all()
    }
    return {
        "stored_rows": run.rows_inserted,
        "articulos_distintos": run.articulos_distintos,
        "stored_counts_by_level": dict(sorted(run.counts_by_nivel.items())),
    }


def carry_into_run(run_id: int, source: str) -> dict:
    """Modo top-up: arrastra decisiones a una corrida ya existente (sin CSV)."""
    db = SessionLocal()
    try:
        run = db.get(MatchImportRun, run_id)
        if run is None:
            raise SystemExit(f"[MATCH][UPDATE] La corrida {run_id} no existe.")
        carry = _carry_decided_pairs(db, run_id, source)
        counts = _refresh_run_counts(db, run)
        db.commit()
        return {"run_id": run_id, "status": run.status, **counts, **carry}
    finally:
        db.close()


def _classify_current_decisions(db, new_run_id: int) -> dict:
    """Clasifica decisiones vigentes sin modificarlas.

    La identidad de una propuesta es (producto_plataforma, candidato_codigo). Los
    demás campos son atributos recalculables. Las decisiones sin ese par en la corrida
    nueva quedan intactas en su corrida anterior (huérfanas reportables, nunca borradas).
    """
    present = (
        select(MatchPropuesta.id)
        .where(
            MatchPropuesta.import_run_id == new_run_id,
            MatchPropuesta.producto_plataforma
            == MatchHomologacion.producto_plataforma,
            MatchPropuesta.candidato_codigo == MatchHomologacion.codigo_elegido,
        )
        .exists()
    )
    conserved = int(
        db.execute(select(func.count(MatchHomologacion.id)).where(present)).scalar_one()
        or 0
    )
    orphan_rows = db.execute(
        select(
            MatchHomologacion.usuario,
            MatchHomologacion.decision,
            func.count(MatchHomologacion.id),
        )
        .where(~present)
        .group_by(MatchHomologacion.usuario, MatchHomologacion.decision)
        .order_by(MatchHomologacion.usuario, MatchHomologacion.decision)
    ).all()
    return {
        "conserved": conserved,
        "orphans": sum(int(r[2]) for r in orphan_rows),
        "orphans_by_user": [
            {"usuario": r[0], "decision": r[1], "cantidad": int(r[2])}
            for r in orphan_rows
        ],
    }


def update_dataset(csv_path: Path, approve: bool, carry_decisions: str | None = None) -> dict:
    if not csv_path.is_file():
        raise SystemExit(f"[MATCH][UPDATE] Archivo no encontrado: {csv_path}")

    db = SessionLocal()
    previous_run_id = _latest_approved_run_id(db)
    run = MatchImportRun(
        source_path=str(csv_path),
        status=MATCH_RUN_PENDING,
        started_at=dt.datetime.utcnow(),
    )
    db.add(run)
    db.commit()
    db.refresh(run)
    run_id = run.id

    raw_rows = 0
    levels: Counter[str] = Counter()
    try:
        with csv_path.open("r", encoding="utf-8-sig", newline="") as handle:
            reader = csv.DictReader(handle, delimiter=";")
            missing = sorted(REQUIRED_COLUMNS - set(reader.fieldnames or []))
            if missing:
                raise ValueError(f"Faltan columnas requeridas: {missing}")

            batch: list[dict] = []
            now = dt.datetime.utcnow()
            for row in reader:
                product = _text(row.get("producto_plataforma"))
                if not product:
                    continue
                code = _text(row.get("candidato_1_codigo"))
                level = (_text(row.get("nivel_confianza")) or "")[:2].upper() or None
                raw_rows += 1
                if level:
                    levels[level] += 1
                batch.append(
                    {
                        "import_run_id": run_id,
                        "producto_plataforma": product,
                        "nivel_confianza": level,
                        "score_mejor": _float(row.get("score_mejor")),
                        "candidato_codigo": code,
                        "candidato_descripcion": _text(
                            row.get("candidato_1_descripcion")
                        ),
                        "score_tfidf": _float(row.get("candidato_1_score_tfidf")),
                        "score_fuzzy": _float(row.get("candidato_1_score_fuzzy")),
                        "score_pharma": _float(row.get("candidato_1_score_pharma")),
                        "created_at": now,
                    }
                )
                if len(batch) >= BATCH_SIZE:
                    _insert_batch(db, batch)
                    batch.clear()
            _insert_batch(db, batch)

        csv_rows = int(
            db.execute(
                select(func.count(MatchPropuesta.id)).where(
                    MatchPropuesta.import_run_id == run_id
                )
            ).scalar_one()
            or 0
        )
        carried = (
            _carry_decided_pairs(db, run_id, carry_decisions) if carry_decisions else {}
        )
        counts = _refresh_run_counts(db, run)
        run.finished_at = dt.datetime.utcnow()

        carry = {"conserved": 0, "orphans": 0, "orphans_by_user": []}
        if approve:
            carry = _classify_current_decisions(db, run_id)
            run.status = MATCH_RUN_APPROVED
            run.approved_at = dt.datetime.utcnow()
            run.approved_by = "cli:update_match_dataset" + (
                f"+carry:{carry_decisions}" if carry_decisions else ""
            )
        db.commit()
        return {
            "previous_run_id": previous_run_id,
            "run_id": run_id,
            "status": run.status,
            "raw_rows": raw_rows,
            "csv_unique_rows": csv_rows,
            "duplicates_collapsed": raw_rows - csv_rows,
            "raw_counts_by_level": dict(sorted(levels.items())),
            **counts,
            **carried,
            **carry,
        }
    except Exception as exc:
        db.rollback()
        failed = db.get(MatchImportRun, run_id)
        if failed is not None:
            failed.status = MATCH_RUN_FAILED
            failed.error_message = str(exc)[:1000]
            failed.finished_at = dt.datetime.utcnow()
            db.commit()
        raise
    finally:
        db.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="Actualización segura del dataset Match")
    parser.add_argument("--csv-path")
    parser.add_argument("--approve", action="store_true")
    parser.add_argument(
        "--carry-decisions",
        choices=("local", "prod"),
        help="traer a la corrida los pares con decisión vigente que el CSV no trae",
    )
    parser.add_argument(
        "--carry-into-run",
        type=int,
        help="sin CSV: arrastrar decisiones a esta corrida existente (idempotente)",
    )
    args = parser.parse_args()
    if args.carry_into_run is not None:
        if not args.carry_decisions:
            parser.error("--carry-into-run requiere --carry-decisions")
        result = carry_into_run(args.carry_into_run, args.carry_decisions)
    else:
        if not args.csv_path:
            parser.error("--csv-path es obligatorio (salvo con --carry-into-run)")
        result = update_dataset(
            Path(args.csv_path).expanduser().resolve(), args.approve, args.carry_decisions
        )
    print("[MATCH][UPDATE]", result)


if __name__ == "__main__":
    main()
