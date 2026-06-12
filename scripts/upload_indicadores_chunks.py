#!/usr/bin/env python
"""Cliente de push por chunks del módulo Indicadores Comerciales.

Calca scripts/upload_dimensionamiento_chunks.py adaptado al patrón de corridas
de Indicadores (ind_import_run): start -> chunks de 4 tablas -> finalize, que
deja la corrida remota en PENDING_APPROVAL. NO publica: la aprobación es el
switch humano desde el Home (admin).

Diferencias deliberadas con el cliente de Dimensionamiento:
  - El state file NO guarda el token (no persistir secretos en disco); al
    reanudar se re-pasa por --token o env INDICADORES_IMPORT_TOKEN.
  - post_with_retry reintenta SOLO 5xx / 429 / errores de red. Un 4xx (403
    token, 400 estado, 404) es definitivo: corta mostrando el detail.
  - Sin subcomando cleanup (el router de Indicadores no tiene esa ruta;
    approve/discard son de la UI admin, no del script).
  - --tabla <nombre> (opcional) limita la subida a una sola tabla, para
    pruebas y debug.
"""
from __future__ import annotations

import argparse
import json
import os
import socket
import sys
import time
from pathlib import Path

# Add project root to sys.path
root_dir = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(root_dir))

try:
    import requests
except ImportError:
    print("❌ Error: El paquete 'requests' no está instalado en este entorno.")
    print("   Por favor, instálelo con:")
    print("   venv_webcomparativas\\Scripts\\pip install requests")
    sys.exit(1)

from sqlalchemy import func
from web_comparativas.models import SessionLocal
from web_comparativas.indicadores_summary_models import (
    IndArticulos,
    IndImportRun,
    IndInflacionFacturacionMensual,
    IndInflacionPvpMensual,
    IndRentabilidadLineas,
)

STATE_FILE = root_dir / "scratch" / "upload_indicadores_state.json"

# 4xx definitivos: NO reintentar (token inválido, corrida en estado equivocado,
# run inexistente). 429 queda fuera: es rate-limit, sí se reintenta.
_FATAL_4XX = range(400, 429)


def load_state() -> dict:
    if STATE_FILE.exists():
        try:
            with open(STATE_FILE, "r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            pass
    return {}


def save_state(state: dict):
    STATE_FILE.parent.mkdir(parents=True, exist_ok=True)
    with open(STATE_FILE, "w", encoding="utf-8") as f:
        json.dump(state, f, indent=2, ensure_ascii=False)


def clear_state():
    if STATE_FILE.exists():
        try:
            STATE_FILE.unlink()
        except Exception:
            pass


def post_with_retry(url: str, headers: dict, json_data: dict, description: str, max_retries: int = 5) -> dict:
    """POST con reintentos exponenciales SOLO ante 5xx/429/errores de red.

    Un 4xx (≠429) es definitivo: el servidor entendió y rechazó (token inválido,
    corrida en estado equivocado, run inexistente) — reintentar no lo arregla.
    """
    retry_delay = 1.0
    for attempt in range(1, max_retries + 1):
        try:
            response = requests.post(url, headers=headers, json=json_data, timeout=600)
            if response.status_code == 200:
                return response.json()

            try:
                err_detail = response.json().get("detail", response.text)
            except Exception:
                err_detail = response.text[:200]

            if response.status_code in _FATAL_4XX and response.status_code != 429:
                print(f"\n  ❌ {description}: HTTP {response.status_code} — error DEFINITIVO, no se reintenta.")
                print(f"      Detalle: {err_detail}")
                raise RuntimeError(f"Error definitivo (HTTP {response.status_code}) al enviar {description}: {err_detail}")

            print(f"  ⚠️  Falló {description} (Intento {attempt}/{max_retries}). HTTP Status: {response.status_code}")
            print(f"      Detalle: {err_detail}")
        except requests.RequestException as e:
            print(f"  ⚠️  Error de red al enviar {description} (Intento {attempt}/{max_retries}): {e}")

        if attempt < max_retries:
            print(f"      Reintentando en {retry_delay:.1f} segundos...")
            time.sleep(retry_delay)
            retry_delay *= 2.0

    raise RuntimeError(f"Error persistente al enviar {description} después de {max_retries} intentos.")


def format_progress_bar(iteration, total, prefix='', suffix='', decimals=1, length=40, fill='█'):
    percent = ("{0:." + str(decimals) + "f}").format(100 * (iteration / float(total)))
    filled_length = int(length * iteration // total)
    bar = fill * filled_length + '-' * (length - filled_length)
    return f'\r{prefix} |{bar}| {percent}% {suffix}'


# ── Serialización por tabla ───────────────────────────────────────────────────
# CLAVE: los money (Numeric 19,4) viajan como STRING para no perder precisión en
# el JSON; el router los vuelve Decimal server-side (_to_decimal). None va como
# None (no "None"). unidades es float de origen y viaja como float.

def _money(v):
    return str(v) if v is not None else None


def _ser_rentabilidad(r, seq: int) -> dict:
    return {
        "line_seq": seq,
        "ctacte": r.ctacte,
        "cliente_grupo": r.cliente_grupo,
        "nombre_cliente": r.nombre_cliente,
        "articulo": r.articulo,
        "cadneg": r.cadneg,
        "fecha": r.fecha.isoformat() if r.fecha else None,
        "cant": r.cant,
        "importe": _money(r.importe),
        "renta1": _money(r.renta1),
        "comprob": r.comprob,
    }


def _ser_pvp(r, seq: int) -> dict:
    return {
        "articulo": r.articulo,
        "mes": r.mes,
        "fecha_snapshot": r.fecha_snapshot.isoformat() if r.fecha_snapshot else None,
        "pvp": _money(r.pvp),
    }


def _ser_facturacion(r, seq: int) -> dict:
    return {
        "articulo": r.articulo,
        "cadneg": r.cadneg,
        "mes": r.mes,
        "unidades": r.unidades,
        "facturacion": _money(r.facturacion),
    }


def _ser_articulos(r, seq: int) -> dict:
    return {
        "articulo": r.articulo,
        "marca": r.marca,
        "descripcion": r.descripcion,
        "laboratorio": r.laboratorio,
        "familia": r.familia,
        "principio_activo": r.principio_activo,
        "unineg": r.unineg,
    }


# Orden de subida. line_seq de rentabilidad es DENSO 1..N y continuo entre
# chunks: seq = offset + i + 1 sobre un ORDER BY id (surrogate, estable), así
# un reenvío manda el mismo line_seq y el on_conflict_do_nothing dedupe.
TABLES = [
    {"key": "rentabilidad", "model": IndRentabilidadLineas, "route": "rentabilidad",
     "label": "Rentabilidad ", "serializer": _ser_rentabilidad},
    {"key": "pvp", "model": IndInflacionPvpMensual, "route": "pvp",
     "label": "PVP          ", "serializer": _ser_pvp},
    {"key": "facturacion", "model": IndInflacionFacturacionMensual, "route": "facturacion",
     "label": "Facturación  ", "serializer": _ser_facturacion},
    {"key": "articulos", "model": IndArticulos, "route": "articulos",
     "label": "Artículos    ", "serializer": _ser_articulos},
]


def _resolver_corrida_local(session, run_id):
    """--run-id explícito, o la última corrida 'approved' (la activa)."""
    if run_id:
        local_run = session.query(IndImportRun).filter_by(id=run_id).first()
        if not local_run:
            print(f"❌ Error: No se encontró la corrida con ID {run_id} en la base de datos local.")
            sys.exit(1)
    else:
        local_run = session.query(IndImportRun).filter_by(status="approved").order_by(
            IndImportRun.approved_at.desc(),
            IndImportRun.id.desc()
        ).first()
        if not local_run:
            print("❌ Error: No se encontró ninguna corrida 'approved' en la base de datos local.")
            sys.exit(1)
    return local_run


def handle_upload(args):
    print("🔌 Conectando a la base de datos local SQLite...")
    session = SessionLocal()

    try:
        # 1. Resolver la corrida local fuente
        local_run = _resolver_corrida_local(session, args.run_id)

        print(f"✅ Corrida local seleccionada:")
        print(f"   - Run ID: {local_run.id}")
        print(f"   - Status: {local_run.status}")
        print(f"   - Aprobada: {local_run.approved_at} (por {local_run.approved_by})")
        print(f"   - Nota: {local_run.nota}")

        # 2. Contar filas de las 4 tablas
        totals = {}
        for t in TABLES:
            totals[t["key"]] = session.query(func.count(t["model"].id)).filter_by(
                import_run_id=local_run.id).scalar()
            print(f"   - {t['model'].__tablename__}: {totals[t['key']]:,} filas")

        solo = getattr(args, "tabla", None)
        tablas_a_subir = [t for t in TABLES if not solo or t["key"] == solo]
        if solo:
            print(f"\n⚠️  MODO --tabla {solo}: se subirá ÚNICAMENTE esa tabla (prueba/debug).")

        if sum(totals[t["key"]] for t in tablas_a_subir) == 0:
            print("❌ Error: La corrida seleccionada no tiene filas en las tablas a subir.")
            sys.exit(1)

        if getattr(args, "dry_run", False):
            print("\n🔍 ========================================================")
            print("🔍               MODO DRY-RUN: SIN CONEXIÓN DE RED         ")
            print("🔍 ========================================================")
            print(f"\n📦 Simulación de Chunks (tamaño {args.chunk_size:,}):")
            for t in tablas_a_subir:
                n = totals[t["key"]]
                chunks = (n + args.chunk_size - 1) // args.chunk_size
                print(f"   - {t['model'].__tablename__}: {n:,} filas -> {chunks} lotes")
            print("\n   ✅ Dry-run completado. No se envió nada al servidor.")
            print("🔍 ========================================================\n")
            return

        # 3. Detectar reanudación
        state = load_state()
        resume_upload = False
        if not args.fresh and state:
            if (state.get("local_run_id") == local_run.id and
                state.get("target_url") == args.url and
                state.get("status") == "running"):
                resume_upload = True

        headers = {
            "X-Import-Token": args.token,
            "Content-Type": "application/json"
        }

        # Warmup: despierta el servicio de Render (free tier puede estar dormido)
        # para que la primera inserción no sufra un cold-start que dispare timeouts.
        print("\n⏰ Despertando el servidor destino (warmup)...")
        try:
            warm = requests.get(args.url, timeout=120)
            print(f"   Servidor respondió (HTTP {warm.status_code}). Continuando.")
        except requests.RequestException as e:
            print(f"   Advertencia: warmup no respondió a tiempo ({e}). Continuando igualmente.")

        offsets = {}
        if resume_upload:
            remote_run_id = state["remote_run_id"]
            for t in TABLES:
                offsets[t["key"]] = state.get(f"{t['key']}_offset", 0)
            print(f"\n🔄 Detectado progreso anterior en scratch/upload_indicadores_state.json.")
            print(f"   - Reanudando corrida remota ID: {remote_run_id}")
            for t in TABLES:
                print(f"   - {t['key']}: {offsets[t['key']]:,} / {totals[t['key']]:,} ya subidas")
        else:
            print(f"\n🚀 Iniciando nueva corrida remota en {args.url}...")
            start_payload = {
                "nota": f"push desde {socket.gethostname()} corrida local {local_run.id}"
            }
            start_url = f"{args.url}/api/indicadores/admin/import/start"
            res = post_with_retry(start_url, headers, start_payload, "Inicio de corrida")
            remote_run_id = res["import_run_id"]
            for t in TABLES:
                offsets[t["key"]] = 0

            # Estado inicial (SIN token: no persistir secretos en disco)
            state = {
                "local_run_id": local_run.id,
                "remote_run_id": remote_run_id,
                "target_url": args.url,
                "status": "running",
            }
            for t in TABLES:
                state[f"{t['key']}_offset"] = 0
            save_state(state)
            print(f"✅ Corrida remota creada con ID: {remote_run_id}")

        # 4. Subir cada tabla en chunks
        for t in tablas_a_subir:
            key, model, serializer = t["key"], t["model"], t["serializer"]
            total = totals[key]
            offset = offsets[key]
            chunk_url = f"{args.url}/api/indicadores/admin/import/chunk/{t['route']}"

            if offset >= total:
                print(f"\n⏭️  {model.__tablename__}: ya subida en su totalidad. Saltando.")
                continue

            print(f"\n📦 Subiendo '{model.__tablename__}' en chunks de {args.chunk_size:,}...")
            while offset < total:
                db_chunk = session.query(model).filter_by(
                    import_run_id=local_run.id
                ).order_by(model.id).offset(offset).limit(args.chunk_size).all()

                if not db_chunk:
                    break

                # line_seq denso y continuo entre chunks: seq = offset + i + 1
                lineas = [serializer(r, offset + i + 1) for i, r in enumerate(db_chunk)]
                chunk_payload = {
                    "import_run_id": remote_run_id,
                    "lineas": lineas,
                }

                desc = f"chunk {key} [{offset:,} a {offset + len(db_chunk):,}]"
                post_with_retry(chunk_url, headers, chunk_payload, desc)

                offset += len(db_chunk)
                offsets[key] = offset
                state[f"{key}_offset"] = offset
                save_state(state)

                sys.stdout.write(format_progress_bar(offset, total, prefix=t["label"], suffix='completado'))
                sys.stdout.flush()

            print(f"\n✅ {model.__tablename__}: subida completa.")

        # 5. Finalize -> pending_approval (NO publica, NO aprueba)
        print("\n🏁 Finalizando corrida de importación en el servidor remoto...")
        finalize_payload = {"import_run_id": remote_run_id}
        finalize_url = f"{args.url}/api/indicadores/admin/import/finalize"
        finalize_res = post_with_retry(finalize_url, headers, finalize_payload, "Finalización de importación")

        clear_state()

        print("\n🎉 ========================================================")
        print("🎉 CARGA COMPLETADA — PENDIENTE DE APROBACIÓN")
        print(f"🎉 Corrida remota ID {remote_run_id} quedó en estado PENDING_APPROVAL.")
        print(f"🎉 Filas por tabla (server): {finalize_res.get('rows_por_tabla')}")
        print("🎉 ⚠️  NO está publicada: hay que APROBARLA desde el Home (admin)")
        print("🎉     para que las lecturas la tomen como corrida activa.")
        print("🎉 ========================================================\n")

    finally:
        session.close()


def handle_rollback(args):
    target_run_id = args.remote_run_id
    if not target_run_id:
        state = load_state()
        if state and state.get("remote_run_id") and state.get("target_url") == args.url:
            target_run_id = state["remote_run_id"]
            print(f"💡 ID de corrida remota detectado desde el estado: {target_run_id}")
        else:
            print("❌ Error: Debe especificar --remote-run-id para poder realizar rollback.")
            sys.exit(1)

    print(f"⚠️  Realizando rollback de la corrida remota ID {target_run_id}...")
    headers = {
        "X-Import-Token": args.token,
        "Content-Type": "application/json"
    }
    rollback_payload = {
        "import_run_id": target_run_id,
        "error_message": args.reason or "Cancelado manualmente por el administrador vía script cliente."
    }
    rollback_url = f"{args.url}/api/indicadores/admin/import/rollback"

    try:
        res = post_with_retry(rollback_url, headers, rollback_payload, f"Rollback de corrida {target_run_id}")
        print(f"✅ Rollback exitoso: la corrida {res.get('import_run_id')} quedó en '{res.get('status')}'.")
        clear_state()
    except Exception as e:
        print(f"❌ Error al ejecutar rollback: {e}")
        sys.exit(1)


def main():
    parser = argparse.ArgumentParser(
        description="Cliente de push por chunks para el módulo Indicadores Comerciales (4 tablas summary + patrón de corridas)."
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000",
        help="URL base de la aplicación (ej: http://127.0.0.1:8000 o https://web-comparativas.onrender.com)."
    )
    parser.add_argument(
        "--token",
        default=os.getenv("INDICADORES_IMPORT_TOKEN", "local_dev_token"),
        help="Token de importación secreto (cabecera X-Import-Token). NO se persiste en disco."
    )

    subparsers = parser.add_subparsers(dest="action", help="Acciones disponibles")

    upload_parser = subparsers.add_parser("upload", help="Sube la corrida local por chunks; finalize deja la remota en pending_approval.")
    upload_parser.add_argument(
        "--run-id",
        type=int,
        help="ID de la corrida local SQLite a subir. Si se omite, se sube la última corrida 'approved'."
    )
    upload_parser.add_argument(
        "--chunk-size",
        type=int,
        default=20000,
        help="Cantidad de filas a transmitir por lote HTTP (default: 20000)."
    )
    upload_parser.add_argument(
        "--fresh",
        action="store_true",
        help="Ignora cualquier estado de subida previo en upload_indicadores_state.json e inicia desde cero."
    )
    upload_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Cuenta filas de las 4 tablas y simula los chunks, sin enviar nada al servidor."
    )
    upload_parser.add_argument(
        "--tabla",
        choices=[t["key"] for t in TABLES],
        help="Limita la subida a UNA sola tabla (prueba/debug). Default: las 4."
    )

    rollback_parser = subparsers.add_parser("rollback", help="Cancela una corrida remota poniéndola en 'failed' (no borra filas).")
    rollback_parser.add_argument(
        "--remote-run-id",
        type=int,
        help="ID de la corrida remota a cancelar. Si se omite, se intentará leer del estado guardado localmente."
    )
    rollback_parser.add_argument(
        "--reason",
        help="Razón de la cancelación."
    )

    args = parser.parse_args()

    if not args.action:
        args.action = "upload"
        args.run_id = None
        args.chunk_size = 20000
        args.fresh = False
        args.dry_run = False
        args.tabla = None

    print("======================================================================")
    print("     🚀  WEB COMPARATIVAS - CLIENTE DE CARGA DE INDICADORES           ")
    print("======================================================================")
    print(f"📍 Servidor destino: {args.url}")
    print(f"⚙️  Acción a ejecutar: {args.action.upper()}")
    print("======================================================================\n")

    if args.url.endswith("/"):
        args.url = args.url[:-1]

    if args.action == "upload":
        handle_upload(args)
    elif args.action == "rollback":
        handle_rollback(args)


if __name__ == "__main__":
    main()
