"""
backfill_forecast_change_requests.py
====================================
Carga inicial (día uno) del módulo "Aprobaciones Forecast".

Crea una solicitud de revisión PENDIENTE por cada override activo existente en
`forecast_user_overrides` que todavía no tenga una solicitud asociada, para que
el Admin tenga modificaciones para revisar desde el inicio.

GARANTÍAS DE SEGURIDAD
  - SOLO hace INSERT en la tabla nueva `forecast_change_requests`.
  - NO hace UPDATE / DELETE / TRUNCATE / DROP sobre ninguna tabla.
  - NO modifica `forecast_user_overrides` ni ninguna tabla existente.
  - NO toca `app.db` de forma destructiva (solo agrega filas a la tabla nueva).
  - IDEMPOTENTE: re-ejecutarlo no duplica (omite overrides ya registrados).

Usa la conexión configurada por la app:
  - Local: web_comparativas/app.db (SQLite) por defecto.
  - Producción: PostgreSQL si DATABASE_URL está definida.

Ejecutar:
    python scripts/backfill_forecast_change_requests.py
"""
from __future__ import annotations

import sys
from pathlib import Path

REPO_ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(REPO_ROOT))


def main() -> int:
    from web_comparativas.models import (
        init_db, SessionLocal, ForecastUserOverride, ForecastChangeRequest,
    )
    from web_comparativas import forecast_service as svc

    if SessionLocal is None or ForecastChangeRequest is None or ForecastUserOverride is None:
        print("ERROR: almacenamiento ORM no disponible.")
        return 1

    # Asegura que la tabla nueva exista (create_all aditivo, no destructivo).
    init_db()

    created = 0
    skipped_existing = 0
    scanned = 0

    with SessionLocal() as session:
        # Overrides ya registrados (idempotencia): set de override_id presentes.
        already = {
            row[0]
            for row in session.query(ForecastChangeRequest.override_id)
            .filter(ForecastChangeRequest.override_id.isnot(None))
            .distinct()
            .all()
            if row[0] is not None
        }

        overrides = (
            session.query(ForecastUserOverride)
            .filter(ForecastUserOverride.source_module == svc.FORECAST_OVERRIDE_SOURCE)
            .filter(ForecastUserOverride.is_active.is_(True))
            .all()
        )

        for ov in overrides:
            scanned += 1
            if ov.id in already:
                skipped_existing += 1
                continue

            base_pct = ov.base_growth_pct
            ovr_pct = ov.override_growth_pct
            old_v = float(base_pct) if base_pct is not None else None
            new_v = float(ovr_pct) if ovr_pct is not None else None

            abs_delta = None
            pct_delta = None
            if new_v is not None:
                base = old_v if old_v is not None else 0.0
                abs_delta = round(new_v - base, 4)
                pct_delta = abs_delta

            try:
                amount_base = svc.estimate_scope_amount(
                    perfil=ov.perfil,
                    subneg=ov.subneg,
                    codigo_serie=ov.codigo_serie,
                    client_selector=ov.client_selector,
                    forecast_month=ov.forecast_month,
                )
            except Exception:
                amount_base = None
            amount_delta = None
            if amount_base is not None and abs_delta is not None:
                amount_delta = round(amount_base * (abs_delta / 100.0), 2)

            change_type = svc._classify_change_type(old_v, new_v)

            cr = ForecastChangeRequest(
                created_at=ov.updated_at or ov.created_at,
                override_id=ov.id,
                source="backfill",
                created_by_user_id=ov.user_id,
                created_by_username=ov.created_by,
                change_type=change_type,
                scope_type=ov.override_scope,
                client_selector=ov.client_selector,
                client_name=ov.client_display or ov.client_selector,
                perfil=ov.perfil,
                neg=ov.neg,
                subneg=(ov.subneg or None),
                codigo_serie=(ov.codigo_serie or None),
                descripcion_articulo=None,
                period=(ov.forecast_month or None),
                field_changed="% ajuste anual",
                old_value=old_v,
                new_value=new_v,
                absolute_delta=abs_delta,
                percentage_delta=pct_delta,
                estimated_amount_base=amount_base,
                estimated_amount_delta=amount_delta,
                status="pendiente",
            )
            session.add(cr)
            created += 1

        session.commit()

    print("=" * 60)
    print("Backfill Aprobaciones Forecast — resumen")
    print("=" * 60)
    print(f"  Overrides activos analizados : {scanned}")
    print(f"  Ya registrados (omitidos)    : {skipped_existing}")
    print(f"  Solicitudes nuevas creadas   : {created}")
    print("=" * 60)
    if created == 0 and skipped_existing > 0:
        print("Nada para crear: todos los overrides ya tienen solicitud (idempotente).")
    elif created == 0:
        print("No hay overrides activos para registrar.")
    print("OK — solo se insertaron filas en forecast_change_requests.")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
