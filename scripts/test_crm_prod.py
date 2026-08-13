"""Prueba en duro del circuito CRM contra PRODUCCIÓN — pasaje a prod de Oportunidades.

Corre EXACTAMENTE los mismos 5 pasos que usa la app (`dimensionamiento/crm_client.py`),
llamados directo, sin pasar por FastAPI ni por la base de datos de SIEM:

    1. login (POST /Api/access_token)
    2. usuarios_rendidores
    3. Cuentas_por_numero_fusion (una cuenta real)
    4. crear UNA oportunidad de prueba (Opportunities)
    5. escribir la bitácora (KNN_BitacoraMsj)

Este script NO toca `web_comparativas/app.db` (ni Postgres de SIEM): no crea fila en
`crm_envios` ni marca nada como "enviado". SÍ crea un registro REAL en el CRM al que
apunte tu configuración (`CRM_BASE_URL`/`CRM_MODO`) — por eso el paso 4 pide
confirmación explícita salvo que pases --yes.

Credenciales: se leen de `web_comparativas/.env` (mismo archivo que usa la app; NO el
`.env` de la raíz), vía CRM_BASE_URL / CRM_CLIENT_ID / CRM_CLIENT_SECRET / CRM_MODO /
CRM_CA_BUNDLE / CRM_CA_PEM / CRM_SSL_VERIFY. Para pegarle a prod, ese .env tiene que
tener CRM_MODO=prod y las credenciales de PRODUCCIÓN.

Uso — prueba end-to-end (pasos 1 a 5):
    python -m scripts.test_crm_prod --cuenta 106920 --assigned-user-id <id_crm> --yes
    python -m scripts.test_crm_prod --cuenta 106920 --usuario-email jacqueline.gallo@suizoargentina.com

Uso — medición de cobertura (item 5 del pase a prod; NO crea nada, solo consulta):
    python -m scripts.test_crm_prod --cobertura
    python -m scripts.test_crm_prod --cobertura --run-id 12
"""
from __future__ import annotations

import argparse
import datetime as dt
import sys
from pathlib import Path

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

try:
    sys.stdout.reconfigure(encoding="utf-8", errors="replace")
    sys.stderr.reconfigure(encoding="utf-8", errors="replace")
except Exception:
    pass

# Mismo .env que carga la app (web_comparativas/.env, NO el de la raíz). Ruta explícita
# como en web_comparativas/models.py: nada de auto-discovery por stack frame.
try:
    from dotenv import load_dotenv
    load_dotenv(PROJECT_ROOT / "web_comparativas" / ".env", override=False)
except ImportError:
    pass

from web_comparativas.dimensionamiento import crm_client  # noqa: E402
from web_comparativas.dimensionamiento.crm_client import CrmError  # noqa: E402


def _ok(msg: str) -> None:
    print(f"✅ {msg}", flush=True)


def _fail(msg: str) -> None:
    print(f"❌ {msg}", flush=True)


def _step(n: int, total: int, titulo: str) -> None:
    print(f"\n[{n}/{total}] {titulo}", flush=True)


def _confirmar(cfg: dict) -> bool:
    print("\n" + "=" * 78)
    print("PRUEBA EN DURO CONTRA EL CRM — va a crear UNA oportunidad y su bitácora")
    print("=" * 78)
    print(f"  CRM_BASE_URL : {cfg['base_url']}")
    print(f"  CRM_MODO     : {cfg['modo']}  {'⚠️  PRODUCCIÓN' if cfg['modo'] == 'prod' else ''}")
    print(f"  verify (TLS) : {cfg['verify']}")
    print("=" * 78)
    resp = input("Escribí 'si' para continuar: ").strip().lower()
    return resp in {"si", "sí", "s", "yes", "y"}


def _resolver_asignado(usuarios: list[dict[str, str]], args) -> dict[str, str]:
    if args.sin_validar_usuario:
        if not args.assigned_user_id:
            raise SystemExit("--sin-validar-usuario requiere --assigned-user-id.")
        print(f"   (--sin-validar-usuario: uso {args.assigned_user_id!r} tal cual, sin validar contra usuarios_rendidores)")
        return {"id": args.assigned_user_id, "usuario": "(sin validar)", "origen": "manual"}
    if args.assigned_user_id:
        for u in usuarios:
            if u["id"] == args.assigned_user_id:
                return {"id": u["id"], "usuario": u["usuario"], "origen": "manual"}
        raise SystemExit(
            f"--assigned-user-id {args.assigned_user_id!r} no está en usuarios_rendidores "
            f"({len(usuarios)} usuarios disponibles)."
        )
    if args.usuario_email:
        match = crm_client.buscar_match_usuario(usuarios, args.usuario_email)
        if not match:
            raise SystemExit(
                f"--usuario-email {args.usuario_email!r} no matchea ningún `usuario` de "
                f"usuarios_rendidores. Pasá --assigned-user-id a mano."
            )
        return match
    fallback = crm_client.crm_config().get("usuario_fallback_id")
    if fallback and any(u["id"] == fallback for u in usuarios):
        print(f"   (sin --assigned-user-id/--usuario-email: uso CRM_USUARIO_FALLBACK_ID={fallback})")
        return {"id": fallback, "usuario": next(u["usuario"] for u in usuarios if u["id"] == fallback), "origen": "manual"}
    raise SystemExit(
        "No hay a quién asignar: pasá --assigned-user-id <id_crm> o --usuario-email <mail>."
    )


def cmd_prueba(args) -> int:
    cfg = crm_client.crm_config()
    TOTAL = 5

    if not args.yes and not _confirmar(cfg):
        _fail("Cancelado por el usuario.")
        return 1

    with crm_client._nueva_sesion(cfg) as sesion:
        _step(1, TOTAL, "login (POST /Api/access_token)")
        try:
            token = crm_client.obtener_token(sesion, cfg)
        except CrmError as exc:
            _fail(f"login: {exc.mensaje}")
            return 2
        _ok(f"login OK (token de {len(token)} caracteres, no se imprime completo). modo={cfg['modo']}")

        _step(2, TOTAL, "usuarios_rendidores")
        try:
            usuarios_raw = crm_client.listar_usuarios(sesion, cfg, token)
            usuarios = crm_client.normalizar_usuarios(usuarios_raw)
        except CrmError as exc:
            _fail(f"usuarios_rendidores: {exc.mensaje}")
            return 2
        _ok(f"usuarios_rendidores OK: {len(usuarios)} usuarios recibidos")
        asignado = _resolver_asignado(usuarios, args)
        print(f"   -> asignado: id={asignado['id']} usuario={asignado['usuario']} origen={asignado['origen']}")

        _step(3, TOTAL, f"Cuentas_por_numero_fusion?n_cuenta_c={args.cuenta}")
        try:
            cuenta = crm_client.buscar_cuenta(sesion, cfg, token, args.cuenta)
        except CrmError as exc:
            _fail(f"Cuentas_por_numero_fusion: {exc.mensaje}")
            return 2
        _ok(f"cuenta encontrada: id={cuenta['id']} name={cuenta.get('name')!r} n_cuenta_c={cuenta.get('n_cuenta_c')}")

        momento = dt.datetime.utcnow()
        nombre = args.nombre or f"PRUEBA SIEM - pase a prod - {momento.strftime('%Y%m%d-%H%M%S')} - BORRAR"
        descripcion = (
            f"Oportunidad de PRUEBA generada por scripts/test_crm_prod.py para validar el "
            f"circuito SIEM -> CRM antes del go-live. No corresponde a una oportunidad real; "
            f"se puede borrar del CRM. Generada {crm_client.formato_momento(momento)}."
        )

        _step(4, TOTAL, f"crear oportunidad de prueba: {nombre!r}")
        try:
            crm_id = crm_client.crear_oportunidad(
                sesion, cfg, token,
                nombre=nombre,
                assigned_user_id=asignado["id"],
                account_id=cuenta["id"],
                amount=args.monto,
                description=descripcion,
                id_sistema_origen=f"TEST-{momento.strftime('%Y%m%d%H%M%S')}",
                date_closed=crm_client.fecha_cierre_tentativa(),
            )
        except CrmError as exc:
            _fail(f"crear oportunidad: {exc.mensaje}")
            return 2
        _ok(f"oportunidad creada: crm_id={crm_id}")
        detail_url = crm_client.crm_detail_url(crm_id)
        if detail_url:
            print(f"   -> {detail_url}")

        _step(5, TOTAL, "escribir bitácora (KNN_BitacoraMsj)")
        bitacora_texto = crm_client.texto_bitacora(
            args.usuario_email or "test_crm_prod.py", asignado, momento,
        )
        try:
            bitacora_id = crm_client.crear_bitacora(
                sesion, cfg, token,
                parent_id=crm_id,
                description=bitacora_texto,
                status="prueba_pase_a_prod",
            )
        except CrmError as exc:
            _fail(f"bitácora: {exc.mensaje} (la oportunidad {crm_id} SÍ quedó creada)")
            return 2
        _ok(f"bitácora creada: bitacora_id={bitacora_id}")

    print("\n" + "=" * 78)
    _ok("CIRCUITO COMPLETO OK — 5/5 pasos. Nada se tocó en la base de SIEM.")
    print(f"   crm_id={crm_id}  bitacora_id={bitacora_id}  cuenta={cuenta['id']}  usuario={asignado['usuario']}")
    print("=" * 78)
    return 0


def cmd_cobertura(args) -> int:
    from sqlalchemy import distinct, select

    from web_comparativas.dimensionamiento.models import OportunidadSummary
    from web_comparativas.dimensionamiento.query_service import _latest_success_import_run
    from web_comparativas.models import SessionLocal

    cfg = crm_client.crm_config()
    print(f"CRM_BASE_URL={cfg['base_url']}  CRM_MODO={cfg['modo']}\n")

    session = SessionLocal()
    try:
        run_id = args.run_id
        if run_id is None:
            latest = _latest_success_import_run(session)
            run_id = latest.id if latest else None
        if run_id is None:
            _fail("No hay run activo de Dimensionamiento en la base LOCAL.")
            return 2

        cuentas = [
            c for (c,) in session.execute(
                select(distinct(OportunidadSummary.cuenta_interna))
                .where(OportunidadSummary.import_run_id == run_id)
                .where(OportunidadSummary.cuenta_interna.isnot(None))
            ).all()
            if str(c or "").strip()
        ]
    finally:
        session.close()

    print(f"Run local: {run_id}  |  cuentas distintas con dato: {len(cuentas)}\n")
    if not cuentas:
        _fail("No hay cuentas de fusión para consultar (todas SIN DATO en este run).")
        return 1

    resultado = crm_client.consultar_cuentas(cuentas)
    results = resultado["results"]
    existen = [n for n, r in results.items() if r.get("exists") is True]
    no_existen = [n for n, r in results.items() if r.get("exists") is False]
    errores = [n for n, r in results.items() if r.get("exists") is None]

    print(f"CRM_MODO consultado: {resultado['crm_modo']}\n")
    print(f"Existen en el CRM     : {len(existen)}/{len(cuentas)}")
    print(f"NO existen en el CRM  : {len(no_existen)}/{len(cuentas)}")
    if errores:
        print(f"Con error de consulta : {len(errores)}/{len(cuentas)}")
        for n in errores:
            print(f"   {n}: {results[n].get('kind')} — {results[n].get('error')}")

    if no_existen:
        print("\nCuentas que NO existen en el CRM (hay que darlas de alta antes de poder enviar):")
        for n in sorted(no_existen):
            print(f"   {n}")

    pct = (len(existen) / len(cuentas) * 100) if cuentas else 0
    print(f"\n>>> COBERTURA: {len(existen)} de {len(cuentas)} ({pct:.1f}%)")
    return 0


def cmd_listar_usuarios(args) -> int:
    cfg = crm_client.crm_config()
    print(f"CRM_BASE_URL={cfg['base_url']}  CRM_MODO={cfg['modo']}\n")

    with crm_client._nueva_sesion(cfg) as sesion:
        print("[1/2] login (POST /Api/access_token)", flush=True)
        try:
            token = crm_client.obtener_token(sesion, cfg)
        except CrmError as exc:
            _fail(f"login: {exc.mensaje}")
            return 2
        _ok("login OK")

        print("[2/2] usuarios_rendidores", flush=True)
        try:
            registros = crm_client.listar_usuarios(sesion, cfg, token)
        except CrmError as exc:
            _fail(f"usuarios_rendidores: {exc.mensaje}")
            return 2
        _ok(f"usuarios_rendidores OK: {len(registros)} usuarios\n")

    _CLAVES_NOMBRE = ("nombre", "name", "nombre_completo", "full_name")
    _CLAVES_LEGAJO = ("legajo_c", "legajo")

    filas = []
    for reg in registros:
        rid = crm_client._valor(reg, crm_client._CLAVES_ID_USUARIO) or "-"
        usuario = crm_client._valor(reg, crm_client._CLAVES_USUARIO) or "-"
        nombre = crm_client._valor(reg, _CLAVES_NOMBRE) or "-"
        legajo = crm_client._valor(reg, _CLAVES_LEGAJO) or "-"
        filas.append((rid, usuario, nombre, legajo))
    filas.sort(key=lambda f: f[1].lower())

    w_id = max([len("id")] + [len(f[0]) for f in filas])
    w_usuario = max([len("usuario")] + [len(f[1]) for f in filas])
    w_nombre = max([len("nombre")] + [len(f[2]) for f in filas])
    w_legajo = max([len("legajo")] + [len(f[3]) for f in filas])

    header = f"{'id':<{w_id}}  {'usuario':<{w_usuario}}  {'nombre':<{w_nombre}}  {'legajo':<{w_legajo}}"
    print(header)
    print("-" * len(header))
    for rid, usuario, nombre, legajo in filas:
        print(f"{rid:<{w_id}}  {usuario:<{w_usuario}}  {nombre:<{w_nombre}}  {legajo:<{w_legajo}}")
    return 0


def main() -> int:
    ap = argparse.ArgumentParser(description=__doc__, formatter_class=argparse.RawDescriptionHelpFormatter)
    ap.add_argument("--cobertura", action="store_true", help="modo cobertura: NO crea nada, mide cuántas cuentas del run existen en el CRM")
    ap.add_argument("--run-id", type=int, default=None, help="[--cobertura] run de Dimensionamiento local a usar (default: run activo)")
    ap.add_argument("--listar-usuarios", action="store_true", help="login + usuarios_rendidores, imprime la lista completa y termina. NO crea nada")
    ap.add_argument("--cuenta", help="[prueba] Nº de cuenta de FUSION real para el paso 3/4")
    ap.add_argument("--nombre", default=None, help="[prueba] nombre de la oportunidad de prueba (default: autogenerado, con BORRAR)")
    ap.add_argument("--monto", type=float, default=1.0, help="[prueba] amount de la oportunidad de prueba (default: 1)")
    ap.add_argument("--assigned-user-id", default=None, help="[prueba] id de usuario del CRM al que asignar")
    ap.add_argument("--usuario-email", default=None, help="[prueba] mail SIEM a matchear contra usuarios_rendidores")
    ap.add_argument("--sin-validar-usuario", action="store_true", help="[prueba] usa --assigned-user-id tal cual, sin validar contra usuarios_rendidores (para ver cómo lo rechaza el CRM)")
    ap.add_argument("--yes", action="store_true", help="[prueba] no pedir confirmación interactiva")
    args = ap.parse_args()

    if args.listar_usuarios:
        return cmd_listar_usuarios(args)

    if args.cobertura:
        return cmd_cobertura(args)

    if not args.cuenta:
        ap.error("--cuenta es obligatorio para la prueba end-to-end (o usá --cobertura / --listar-usuarios).")
    return cmd_prueba(args)


if __name__ == "__main__":
    sys.exit(main())
