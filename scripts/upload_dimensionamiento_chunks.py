#!/usr/bin/env python
from __future__ import annotations

import argparse
import datetime as dt
import json
import os
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
from web_comparativas.dimensionamiento.models import (
    DimensionamientoImportRun,
    DimensionamientoRecord,
    DimensionamientoFamilyMonthlySummary,
    DimensionamientoDashboardSnapshot
)

STATE_FILE = root_dir / "scratch" / "upload_state.json"

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

def post_with_retry(url: str, headers: dict, json_data: dict, description: str, max_retries: int = 5, ok_if_already_final: bool = False) -> dict:
    """Realiza un POST HTTP con reintentos exponenciales y reporte claro.

    ok_if_already_final: para el finalize. Trata como ÉXITO idempotente el caso en
    que el server responde 400 'Import run is not running (status=success)' (un
    intento previo ya finalizó), y trata el 409 'Finalize already in progress' como
    reintentable (otro finalize del mismo run está corriendo).
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
                err_detail = response.text

            # Finalize idempotente: la corrida ya quedó 'success'.
            if ok_if_already_final and response.status_code == 400 and "status=success" in str(err_detail):
                print(f"  ✅ {description}: la corrida ya estaba finalizada (idempotente).")
                return {"ok": True, "idempotent": True, "message": str(err_detail)}

            # Finalize en progreso en el server (try-lock no adquirido): reintentar.
            if ok_if_already_final and response.status_code == 409:
                print(f"  ⏳ {description}: finalize en progreso en el server (Intento {attempt}/{max_retries}).")
            else:
                print(f"  ⚠️  Falló {description} (Intento {attempt}/{max_retries}). HTTP Status: {response.status_code}")
                print(f"      Detalle: {str(err_detail)[:200]}")
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

def handle_upload(args):
    print("🔌 Conectando a la base de datos local SQLite...")
    session = SessionLocal()
    
    try:
        # 1. Resolver la corrida local
        if args.run_id:
            local_run = session.query(DimensionamientoImportRun).filter_by(id=args.run_id).first()
            if not local_run:
                print(f"❌ Error: No se encontró la corrida con ID {args.run_id} en la base de datos local.")
                sys.exit(1)
        else:
            local_run = session.query(DimensionamientoImportRun).filter_by(status="success").order_by(
                DimensionamientoImportRun.finished_at.desc(),
                DimensionamientoImportRun.id.desc()
            ).first()
            if not local_run:
                print("❌ Error: No se encontró ninguna corrida exitosa ('success') en la base de datos local.")
                sys.exit(1)
                
        print(f"✅ Corrida local seleccionada:")
        print(f"   - Run ID: {local_run.id}")
        print(f"   - Modo de importación: {local_run.mode}")
        print(f"   - Origen: {local_run.source_path}")
        print(f"   - Hash: {local_run.source_hash}")
        print(f"   - Finalizada localmente: {local_run.finished_at}")
        
        # 2. Contar registros y resúmenes locales
        total_records = session.query(func.count(DimensionamientoRecord.id)).filter_by(import_run_id=local_run.id).scalar()
        total_summaries = session.query(func.count(DimensionamientoFamilyMonthlySummary.id)).filter_by(import_run_id=local_run.id).scalar()
        
        print(f"   - Registros locales a subir: {total_records:,}")
        print(f"   - Resúmenes locales a subir: {total_summaries:,}")
        
        if total_records == 0:
            print("❌ Error: La corrida seleccionada no tiene registros asociados en la tabla 'dimensionamiento_records'.")
            sys.exit(1)

        if getattr(args, "dry_run", False):
            print("\n🔍 ========================================================")
            print("🔍               MODO DRY-RUN: INICIANDO VALIDACIONES      ")
            print("🔍 ========================================================")
            
            # Calcular KPIs locales de DimensionamientoRecord
            distinct_clients = session.query(func.count(DimensionamientoRecord.cliente_visible.distinct())).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar()
            
            distinct_families = session.query(func.count(DimensionamientoRecord.familia.distinct())).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar()
            
            distinct_provinces = session.query(func.count(DimensionamientoRecord.provincia.distinct())).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar()
            
            min_date = session.query(func.min(DimensionamientoRecord.fecha)).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar()
            
            max_date = session.query(func.max(DimensionamientoRecord.fecha)).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar()
            
            total_val = session.query(func.sum(DimensionamientoRecord.valorizacion_estimada)).filter(
                DimensionamientoRecord.import_run_id == local_run.id
            ).scalar() or 0.0
            
            # Validar snapshot
            snap = session.query(DimensionamientoDashboardSnapshot).filter_by(
                snapshot_key="default_dashboard_bootstrap",
                import_run_id=local_run.id
            ).first()
            
            has_snapshot = snap is not None
            
            print("\n📋 KPIs Calculados en SQLite local:")
            print(f"   - Renglones: {total_records:,}")
            print(f"   - Clientes Homologados (Sí): {distinct_clients:,}")
            print(f"   - Familias: {distinct_families:,}")
            print(f"   - Provincias: {distinct_provinces:,}")
            print(f"   - Fecha Mínima: {min_date}")
            print(f"   - Fecha Máxima: {max_date}")
            print(f"   - Valorización Total: {total_val:,.2f} (${total_val / 1e9:.2f}B)")
            print(f"   - Resúmenes Mensuales: {total_summaries:,}")
            print(f"   - Snapshot precalculado existente: {'SÍ' if has_snapshot else 'NO'}")
            
            # Simular preparación de chunks
            records_chunks = (total_records + args.chunk_size - 1) // args.chunk_size
            summaries_chunks = (total_summaries + args.chunk_size - 1) // args.chunk_size
            print(f"\n📦 Simulación de Chunks (tamaño {args.chunk_size:,}):")
            print(f"   - Chunks de registros: {records_chunks} lotes")
            print(f"   - Chunks de resúmenes: {summaries_chunks} lotes")
            
            # Validaciones de consistencia interna (no compara contra valores fijos)
            errors = []
            if total_summaries == 0:
                errors.append("No se encontraron resúmenes mensuales asociados a esta corrida.")
            if not has_snapshot:
                errors.append("No se encontró el snapshot precalculado 'default_dashboard_bootstrap' para esta corrida.")

            print("\n🛡️  Resultados de la Validación:")
            if not errors:
                print("   ✅ Datos consistentes. El archivo local SQLite está listo para ser subido a producción.")
                print("   ℹ️  (No se realizó ninguna conexión de red al servidor de producción).")
                print("🔍 ========================================================\n")
                return
            else:
                print("   ❌ SE DETECTARON PROBLEMAS DE CONSISTENCIA:")
                for err in errors:
                    print(f"      - {err}")
                print("🔍 ========================================================\n")
                sys.exit(1)

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
        print("\n⏰ Despertando el servidor de producción (warmup)...")
        try:
            warm = requests.get(args.url, timeout=120)
            print(f"   Servidor respondió (HTTP {warm.status_code}). Continuando.")
        except requests.RequestException as e:
            print(f"   Advertencia: warmup no respondió a tiempo ({e}). Continuando igualmente.")

        if resume_upload:
            remote_run_id = state["remote_run_id"]
            records_offset = state.get("records_offset", 0)
            summaries_offset = state.get("summaries_offset", 0)
            print(f"\n🔄 Detectado progreso anterior en scratch/upload_state.json.")
            print(f"   - Reanudando corrida remota ID: {remote_run_id}")
            print(f"   - Registros ya subidos: {records_offset:,} / {total_records:,}")
            print(f"   - Resúmenes ya subidos: {summaries_offset:,} / {total_summaries:,}")
        else:
            print(f"\n🚀 Iniciando nueva corrida remota en {args.url}...")
            start_payload = {
                "source_path": local_run.source_path,
                "source_hash": local_run.source_hash,
                "source_mtime": local_run.source_mtime.isoformat() if local_run.source_mtime else None,
                "mode": local_run.mode,
                "chunk_size": args.chunk_size
            }
            start_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/start"
            res = post_with_retry(start_url, headers, start_payload, "Inicio de corrida")
            remote_run_id = res["import_run_id"]
            records_offset = 0
            summaries_offset = 0
            
            # Guardar estado inicial
            state = {
                "local_run_id": local_run.id,
                "remote_run_id": remote_run_id,
                "target_url": args.url,
                "token": args.token, # para reanudar con las mismas credenciales si es necesario
                "records_offset": 0,
                "summaries_offset": 0,
                "status": "running"
            }
            save_state(state)
            print(f"✅ Corrida remota creada con ID: {remote_run_id}")

        # 4. Subir registros en chunks
        records_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/chunk/records"
        if records_offset < total_records:
            print(f"\n📦 Subiendo registros ('dimensionamiento_records') en chunks de {args.chunk_size:,}...")
            
            while records_offset < total_records:
                # Consultar chunk de SQLite
                db_chunk = session.query(DimensionamientoRecord).filter_by(
                    import_run_id=local_run.id
                ).order_by(DimensionamientoRecord.id).offset(records_offset).limit(args.chunk_size).all()
                
                if not db_chunk:
                    break
                
                # Serializar records
                records_list = []
                for r in db_chunk:
                    records_list.append({
                        "id_registro_unico": r.id_registro_unico,
                        "fecha": r.fecha.isoformat() if r.fecha else None,
                        "plataforma": r.plataforma,
                        "cliente_nombre_homologado": r.cliente_nombre_homologado,
                        "cliente_nombre_original": r.cliente_nombre_original,
                        "cliente_visible": r.cliente_visible,
                        "cuit": r.cuit,
                        "provincia": r.provincia,
                        "cuenta_interna": r.cuenta_interna,
                        "codigo_articulo": r.codigo_articulo,
                        "descripcion": r.descripcion,
                        "clasificacion_suizo": r.clasificacion_suizo,
                        "descripcion_articulo": r.descripcion_articulo,
                        "familia": r.familia,
                        "unidad_negocio": r.unidad_negocio,
                        "subunidad_negocio": r.subunidad_negocio,
                        "cantidad_demandada": r.cantidad_demandada,
                        "valorizacion_estimada": r.valorizacion_estimada,
                        "resultado_participacion": r.resultado_participacion,
                        "producto_nombre_original": r.producto_nombre_original,
                        "fecha_procesamiento": r.fecha_procesamiento.strftime("%Y-%m-%d %H:%M:%S") if r.fecha_procesamiento else None,
                        "is_identified": bool(r.is_identified),
                        "is_client": bool(r.is_client),
                    })
                
                chunk_payload = {
                    "import_run_id": remote_run_id,
                    "records": records_list
                }
                
                desc = f"chunk registros [{records_offset:,} a {records_offset + len(db_chunk):,}]"
                post_with_retry(records_url, headers, chunk_payload, desc)
                
                records_offset += len(db_chunk)
                state["records_offset"] = records_offset
                save_state(state)
                
                # Mostrar barra de progreso
                sys.stdout.write(format_progress_bar(records_offset, total_records, prefix='Registros', suffix='completado'))
                sys.stdout.flush()
                
            print(f"\n✅ Todos los registros subidos correctamente.")
        else:
            print("\n⏭️  Registros ya subidos en su totalidad. Saltando paso de registros.")

        # 5. Subir resúmenes mensuales en chunks
        summaries_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/chunk/summaries"
        if summaries_offset < total_summaries:
            print(f"\n📊 Subiendo resúmenes ('dimensionamiento_family_monthly_summary') en chunks de {args.chunk_size:,}...")
            
            while summaries_offset < total_summaries:
                # Consultar chunk de SQLite
                db_chunk = session.query(DimensionamientoFamilyMonthlySummary).filter_by(
                    import_run_id=local_run.id
                ).order_by(DimensionamientoFamilyMonthlySummary.id).offset(summaries_offset).limit(args.chunk_size).all()
                
                if not db_chunk:
                    break
                
                # Serializar resúmenes
                summaries_list = []
                for s in db_chunk:
                    summaries_list.append({
                        "month": s.month.isoformat() if s.month else None,
                        "plataforma": s.plataforma,
                        "cliente_nombre_homologado": s.cliente_nombre_homologado,
                        "cliente_visible": s.cliente_visible,
                        "provincia": s.provincia,
                        "familia": s.familia,
                        "unidad_negocio": s.unidad_negocio,
                        "subunidad_negocio": s.subunidad_negocio,
                        "resultado_participacion": s.resultado_participacion,
                        "is_identified": bool(s.is_identified),
                        "is_client": bool(s.is_client),
                        "total_cantidad": s.total_cantidad,
                        "total_valorizacion": s.total_valorizacion,
                        "total_registros": s.total_registros,
                        "clientes_unicos": s.clientes_unicos,
                    })
                
                chunk_payload = {
                    "import_run_id": remote_run_id,
                    "summaries": summaries_list,
                    # Idempotencia: solo el primer chunk hace DELETE run-scoped en
                    # el server para arrancar de cero (evita filas viejas/infladas).
                    "reset": summaries_offset == 0,
                }

                desc = f"chunk resúmenes [{summaries_offset:,} a {summaries_offset + len(db_chunk):,}]"
                post_with_retry(summaries_url, headers, chunk_payload, desc)
                
                summaries_offset += len(db_chunk)
                state["summaries_offset"] = summaries_offset
                save_state(state)
                
                # Mostrar barra de progreso
                sys.stdout.write(format_progress_bar(summaries_offset, total_summaries, prefix='Resúmenes', suffix='completado'))
                sys.stdout.flush()
                
            print(f"\n✅ Todos los resúmenes subidos correctamente.")
        else:
            print("\n⏭️  Resúmenes ya subidos en su totalidad. Saltando paso de resúmenes.")

        # 6. Recuperar y subir snapshot finalizador
        print("\n📝 Cargando snapshot de dashboard precalculado localmente...")
        snap = session.query(DimensionamientoDashboardSnapshot).filter_by(
            snapshot_key="default_dashboard_bootstrap",
            import_run_id=local_run.id
        ).first()
        
        snapshot_payload = None
        if snap:
            snapshot_payload = snap.payload
            print(f"✅ Snapshot encontrado localmente. Tamaño en caracteres: {len(json.dumps(snapshot_payload)):,}")
        else:
            print("⚠️  Advertencia: No se encontró un snapshot de dashboard en local para esta corrida. Se finalizará sin snapshot precalculado.")

        print("\n🏁 Finalizando corrida de importación en el servidor remoto...")
        finalize_payload = {
            "import_run_id": remote_run_id,
            "snapshot": snapshot_payload,
            "summary_metadata": local_run.summary
        }
        
        finalize_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/finalize"
        # Más reintentos que los chunks: si el server cae al rebuild (fallback) y
        # tarda, el 409 'en progreso' se reintenta con backoff hasta que finalice.
        finalize_res = post_with_retry(
            finalize_url, headers, finalize_payload, "Finalización de importación",
            max_retries=8, ok_if_already_final=True,
        )
        
        # Limpieza de estado local
        clear_state()

        # Estado EXPLÍCITO de la resolución de identidad de clientes (el server lo devuelve
        # en el finalize). Un push NO es realmente exitoso si la identidad quedó sin resolver.
        identidad = None
        try:
            identidad = (finalize_res.json() or {}).get("identidad")
        except Exception:
            identidad = None

        print("\n🎉 ========================================================")
        print("🎉 ¡ACTUALIZACIÓN COMPLETADA CON ÉXITO!")
        print(f"🎉 Corrida remota activada: ID {remote_run_id}")
        print(f"🎉 El dashboard en producción ya está operativo con los nuevos datos.")
        print("🎉 ========================================================")
        if identidad is None:
            print("⚠️  IDENTIDAD DE CLIENTES: el server no reportó estado (¿versión vieja?).")
            print("    Verificá con: GET .../admin/estado-identidad")
        elif identidad.get("resuelta"):
            print(f"✅ IDENTIDAD DE CLIENTES: RESUELTA — {identidad.get('entidades')} entidades. "
                  "La card cuenta por identidad.")
        else:
            print("🛑 ========================================================")
            print("🛑 IDENTIDAD DE CLIENTES: NO RESUELTA. La card está en FALLBACK")
            print("🛑 (muestra el número anterior, provisorio). El push NO está completo.")
            print(f"🛑   entidades={identidad.get('entidades')} summary_identidad_null={identidad.get('summary_identidad_null')}")
            print("🛑   Correr el backfill:")
            print(f"🛑   curl -s -X POST \"{args.url}/api/mercado-privado/dimensiones/admin/resolve-entities\" -H \"X-Import-Token: <TOKEN>\"")
            print("🛑   Y confirmar con: GET .../admin/estado-identidad")
            print("🛑 ========================================================")
        print("")
        
    finally:
        session.close()

def handle_rollback(args):
    target_run_id = args.remote_run_id
    if not target_run_id:
        # Intentar leer desde el estado guardado
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
    rollback_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/rollback"
    
    try:
        res = post_with_retry(rollback_url, headers, rollback_payload, f"Rollback de corrida {target_run_id}")
        print(f"✅ Rollback exitoso: {res.get('message')}")
        clear_state()
    except Exception as e:
        print(f"❌ Error al ejecutar rollback: {e}")
        sys.exit(1)

def handle_cleanup(args):
    print(f"🧹 Ejecutando limpieza de corridas viejas en {args.url}...")
    print(f"   (Se mantendrán las últimas {args.keep_runs} corridas exitosas y cualquier corrida en progreso)")
    headers = {
        "X-Import-Token": args.token,
        "Content-Type": "application/json"
    }
    cleanup_payload = {
        "keep_runs": args.keep_runs
    }
    cleanup_url = f"{args.url}/api/mercado-privado/dimensiones/admin/import/cleanup"
    
    try:
        res = post_with_retry(cleanup_url, headers, cleanup_payload, "Limpieza de base de datos")
        print(f"✅ Limpieza exitosa: {res.get('message')}")
        deleted_count = res.get("deleted_runs_count", 0)
        if deleted_count > 0:
            print(f"   IDs eliminados físicamente: {res.get('deleted_run_ids')}")
    except Exception as e:
        print(f"❌ Error al ejecutar la limpieza: {e}")
        sys.exit(1)

def main():
    parser = argparse.ArgumentParser(
        description="Cliente de sincronización por chunks para el módulo Dimensionamiento en Mercado Privado."
    )
    parser.add_argument(
        "--url",
        default="http://127.0.0.1:8000",
        help="URL base de la aplicación (ej: http://127.0.0.1:8000 o https://web-comparativas.onrender.com)."
    )
    parser.add_argument(
        "--token",
        default=os.getenv("DIMENSIONAMIENTO_IMPORT_TOKEN", "local_dev_token"),
        help="Token de importación secreto (cabecera X-Import-Token)."
    )
    
    subparsers = parser.add_subparsers(dest="action", help="Acciones disponibles")
    
    # Subparser para upload
    upload_parser = subparsers.add_parser("upload", help="Sube los datos locales por chunks en staging.")
    upload_parser.add_argument(
        "--run-id",
        type=int,
        help="ID de la corrida local SQLite a subir. Si se omite, se subirá la última corrida con éxito ('success')."
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
        help="Ignora cualquier estado de subida previo en upload_state.json e inicia desde cero."
    )
    upload_parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Realiza validaciones locales sobre los datos sin iniciar la carga ni enviar nada al servidor."
    )
    
    # Subparser para rollback
    rollback_parser = subparsers.add_parser("rollback", help="Cancela una corrida remota poniéndola en 'failed'.")
    rollback_parser.add_argument(
        "--remote-run-id",
        type=int,
        help="ID de la corrida remota a cancelar. Si se omite, se intentará leer del estado guardado localmente."
    )
    rollback_parser.add_argument(
        "--reason",
        help="Razón de la cancelación."
    )
    
    # Subparser para cleanup
    cleanup_parser = subparsers.add_parser("cleanup", help="Elimina registros huérfanos y corridas antiguas en producción.")
    cleanup_parser.add_argument(
        "--keep-runs",
        type=int,
        default=2,
        help="Cantidad de corridas exitosas recientes a conservar (default: 2)."
    )
    
    args = parser.parse_args()
    
    # Si no se provee acción, por defecto hacemos 'upload'
    if not args.action:
        args.action = "upload"
        args.run_id = None
        args.chunk_size = 20000
        args.fresh = False
        args.dry_run = False
        
    print("======================================================================")
    print("     🚀  WEB COMPARATIVAS - CLIENTE DE CARGA DE DIMENSIONAMIENTO      ")
    print("======================================================================")
    print(f"📍 Servidor destino: {args.url}")
    print(f"⚙️  Acción a ejecutar: {args.action.upper()}")
    print("======================================================================\n")
    
    # Normalizar la URL (eliminar barra inclinada final)
    if args.url.endswith("/"):
        args.url = args.url[:-1]
        
    if args.action == "upload":
        handle_upload(args)
    elif args.action == "rollback":
        handle_rollback(args)
    elif args.action == "cleanup":
        handle_cleanup(args)

if __name__ == "__main__":
    main()
