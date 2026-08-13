"""Limpieza de registros de envíos a CRM (Oportunidades de Venta).

Mientras la API real está diferida, cada "Enviar a CRM" desde la UI registra una fila
en `crm_envios` (marcada crm_status='SIMULADO' por el modo placeholder) y un evento en
`crm_envio_eventos`. Esas filas BLOQUEAN el reenvío (control de duplicados). Este script
borra esos registros para poder volver a probar la UI.

Uso (con el servidor DETENIDO para evitar 'database is locked' en SQLite):

    # Borrar SOLO los simulados (default, recomendado mientras se prueba):
    python scripts/clear_crm_envios.py

    # Borrar TODO (simulados + reales) — pide confirmación:
    python scripts/clear_crm_envios.py --all

    # Sin pedir confirmación (para automatizar):
    python scripts/clear_crm_envios.py --all --yes

    # Solo mostrar qué borraría, sin tocar nada:
    python scripts/clear_crm_envios.py --dry-run

Funciona en SQLite local y PostgreSQL (usa el engine del proyecto / DATABASE_URL).
"""
from __future__ import annotations

import argparse
import sys

from sqlalchemy import delete, func, select

from web_comparativas.models import SessionLocal
from web_comparativas.dimensionamiento.models import CrmEnvio, CrmEnvioEvento

SIMULADO = "SIMULADO"


def _counts(session) -> tuple[int, int, int]:
    total = session.execute(select(func.count(CrmEnvio.id))).scalar() or 0
    simulados = session.execute(
        select(func.count(CrmEnvio.id)).where(CrmEnvio.crm_status == SIMULADO)
    ).scalar() or 0
    eventos = session.execute(select(func.count(CrmEnvioEvento.id))).scalar() or 0
    return int(total), int(simulados), int(eventos)


def _detalle_por_modo(session) -> str:
    """Desglose por entorno: con bloqueo por (oportunidad_id, crm_modo), saber cuántas
    filas hay de cada modo es lo que permite confirmar que no se toca lo productivo."""
    filas = session.execute(
        select(CrmEnvio.crm_modo, func.count(CrmEnvio.id)).group_by(CrmEnvio.crm_modo)
    ).all()
    if not filas:
        return "  (sin envíos registrados)"
    return "\n".join(f"  crm_modo={m or 'NULL'}: {n}" for m, n in sorted(filas, key=lambda r: str(r[0])))


def main() -> int:
    ap = argparse.ArgumentParser(description="Limpia crm_envios / crm_envio_eventos.")
    ap.add_argument("--all", action="store_true",
                    help="Borra TODOS los envíos (no solo los SIMULADO).")
    ap.add_argument("--yes", action="store_true", help="No pedir confirmación.")
    ap.add_argument("--dry-run", action="store_true",
                    help="Solo informa qué borraría, sin ejecutar.")
    args = ap.parse_args()

    session = SessionLocal()
    try:
        total, simulados, eventos = _counts(session)
        print(f"Estado actual: crm_envios={total} (SIMULADO={simulados}), crm_envio_eventos={eventos}")
        print(_detalle_por_modo(session))

        if args.dry_run:
            objetivo = total if args.all else simulados
            print(f"[DRY-RUN] Borraría {objetivo} fila(s) de crm_envios "
                  f"({'TODAS' if args.all else 'solo SIMULADO'}) + sus eventos. Nada ejecutado.")
            return 0

        if args.all:
            if not args.yes:
                resp = input("Vas a borrar TODOS los envíos (incluidos reales). ¿Continuar? [y/N] ")
                if resp.strip().lower() not in {"y", "yes", "s", "si", "sí"}:
                    print("Cancelado.")
                    return 1
            ev = session.execute(delete(CrmEnvioEvento))
            en = session.execute(delete(CrmEnvio))
        else:
            # Solo SIMULADO. Los eventos se filtran por crm_status, NO por
            # oportunidad_id: desde que el bloqueo es por entorno
            # (oportunidad_id, crm_modo), una misma oportunidad puede tener a la vez
            # una fila simulada y una real. Borrar los eventos "de esos oportunidad_id"
            # se llevaría puesta la bitácora de los envíos REALES, que es justo lo que
            # este modo promete no tocar.
            ev = session.execute(
                delete(CrmEnvioEvento).where(CrmEnvioEvento.crm_status == SIMULADO)
            )
            en = session.execute(
                delete(CrmEnvio).where(CrmEnvio.crm_status == SIMULADO)
            )

        session.commit()
        borrados_en = en.rowcount if en is not None else 0
        borrados_ev = ev.rowcount if ev is not None else 0
        print(f"Listo: borrados {borrados_en} envío(s) y {borrados_ev} evento(s).")
        total, simulados, eventos = _counts(session)
        print(f"Estado final:  crm_envios={total} (SIMULADO={simulados}), crm_envio_eventos={eventos}")
        return 0
    finally:
        session.close()


if __name__ == "__main__":
    sys.exit(main())
