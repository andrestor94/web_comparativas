"""Migración ONE-OFF: datos del backup (esquema viejo, tablas _staging) al esquema nuevo
versionado por corrida. 100% local SQLite→SQLite, SIN tocar Fusion/ETL_Data ni la red.

Lee las 4 tablas *_staging de app.db.bak_pre_runs (ATTACH en modo solo lectura) y copia
las filas 1:1 a las tablas publicadas nuevas del app.db actual, agregando ÚNICAMENTE la
etiqueta import_run_id de una corrida creada al efecto, que queda en 'approved'
(equivalente local de una carga base ya validada — los datos son los mismos que validamos
A/B contra el vivo). También asegura el watermark de histopre en ind_etl_control.

Uso:
    python -m web_comparativas.migrar_backup_a_runs
"""

from __future__ import annotations

import json
import sqlite3
from datetime import datetime
from pathlib import Path
from urllib.parse import quote

_DIR = Path(__file__).resolve().parent
DB_ACTUAL = _DIR / "app.db"
DB_BACKUP = _DIR / "app.db.bak_pre_runs"

WATERMARK_HISTOPRE = 10247779

# tabla nueva -> (tabla _staging del backup, columnas comunes a copiar 1:1)
TABLAS = {
    "ind_inflacion_pvp_mensual": (
        "ind_inflacion_pvp_mensual_staging",
        "articulo, mes, fecha_snapshot, pvp",
    ),
    "ind_rentabilidad_lineas": (
        "ind_rentabilidad_lineas_staging",
        "ctacte, cliente_grupo, nombre_cliente, articulo, cadneg, fecha, cant, importe, renta1, comprob",
    ),
    "ind_inflacion_facturacion_mensual": (
        "ind_inflacion_facturacion_mensual_staging",
        "articulo, cadneg, mes, unidades, facturacion",
    ),
    "ind_articulos": (
        "ind_articulos_staging",
        "articulo, marca, descripcion, laboratorio, familia, principio_activo, unineg",
    ),
}


def main() -> None:
    # uri=True en la conexión principal habilita el procesamiento de URIs también para
    # el ATTACH (sin esto, 'file:...' se interpreta como nombre de archivo literal).
    # quote(safe="/:"): la ruta lleva espacios (la URI exige %20) pero el ':' del drive
    # de Windows debe quedar sin codificar.
    con = sqlite3.connect(f"file:{quote(DB_ACTUAL.as_posix(), safe='/:')}?mode=rw", uri=True)
    cur = con.cursor()
    # Backup en SOLO LECTURA (URI mode=ro): imposible escribirlo por accidente.
    cur.execute(f"ATTACH DATABASE 'file:{quote(DB_BACKUP.as_posix(), safe='/:')}?mode=ro' AS bak")

    ahora = datetime.utcnow().isoformat(sep=" ")
    cur.execute(
        "INSERT INTO ind_import_run (status, created_at, nota) VALUES ('running', ?, ?)",
        (ahora, "migración 1:1 desde app.db.bak_pre_runs (esquema viejo _staging), sin Fusion"),
    )
    rid = cur.lastrowid
    con.commit()
    print(f"[migra] corrida creada: import_run_id={rid} (status=running)", flush=True)

    try:
        conteos = {}
        for tabla, (staging, cols) in TABLAS.items():
            esperado = cur.execute(f"SELECT COUNT(*) FROM bak.{staging}").fetchone()[0]
            cur.execute(
                f"INSERT INTO {tabla} ({cols}, import_run_id) "
                f"SELECT {cols}, ? FROM bak.{staging}",
                (rid,),
            )
            con.commit()
            copiadas = cur.execute(
                f"SELECT COUNT(*) FROM {tabla} WHERE import_run_id = ?", (rid,)
            ).fetchone()[0]
            conteos[tabla] = copiadas
            print(f"[migra] {staging} -> {tabla}: backup={esperado}  copiadas={copiadas}  "
                  f"{'OK' if copiadas == esperado else 'MISMATCH'}", flush=True)
            if copiadas != esperado:
                raise RuntimeError(f"Mismatch en {tabla}: backup={esperado} copiadas={copiadas}")

        fin = datetime.utcnow().isoformat(sep=" ")
        cur.execute(
            "UPDATE ind_import_run SET status='approved', finalized_at=?, approved_at=?, "
            "approved_by=?, rows_por_tabla=? WHERE id=?",
            (fin, fin, "migración desde backup (sin Fusion)", json.dumps(conteos), rid),
        )

        # Watermark de histopre: asegurar 10247779 (restaurado antes; idempotente).
        row = cur.execute(
            "SELECT watermark_idhisto FROM ind_etl_control WHERE fuente='histopre'"
        ).fetchone()
        if row is None:
            cur.execute(
                "INSERT INTO ind_etl_control (fuente, watermark_idhisto, estado) "
                "VALUES ('histopre', ?, 'staging')",
                (WATERMARK_HISTOPRE,),
            )
            print(f"[migra] watermark histopre INSERTADO: {WATERMARK_HISTOPRE}", flush=True)
        elif row[0] != WATERMARK_HISTOPRE:
            print(f"[migra] OJO: watermark existente {row[0]} != {WATERMARK_HISTOPRE} (no lo toco)", flush=True)
        else:
            print(f"[migra] watermark histopre ya presente: {row[0]}", flush=True)

        con.commit()
        print(f"[migra] corrida {rid} -> approved  rows_por_tabla={json.dumps(conteos)}", flush=True)
    except Exception as exc:
        con.rollback()
        cur.execute(
            "UPDATE ind_import_run SET status='failed', finalized_at=?, nota=? WHERE id=?",
            (datetime.utcnow().isoformat(sep=" "), str(exc)[:2000], rid),
        )
        con.commit()
        print(f"[migra] CORRIDA {rid} FAILED: {exc}", flush=True)
        raise
    finally:
        con.close()


if __name__ == "__main__":
    main()
