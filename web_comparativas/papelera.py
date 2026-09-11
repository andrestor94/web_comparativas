"""Reglas compartidas por las papeleras recuperables de SIEM."""
from __future__ import annotations

import datetime as dt


PAPELERA_VENTANA_HORAS = 24


def papelera_corte(now: dt.datetime | None = None) -> dt.datetime:
    instante = now or dt.datetime.utcnow()
    return instante - dt.timedelta(hours=PAPELERA_VENTANA_HORAS)


def papelera_horas_restantes(
    descartado_at: dt.datetime,
    now: dt.datetime | None = None,
) -> float:
    instante = now or dt.datetime.utcnow()
    restantes = PAPELERA_VENTANA_HORAS - (
        instante - descartado_at
    ).total_seconds() / 3600.0
    return round(max(0.0, restantes), 1)


def papelera_recuperable(
    descartado_at: dt.datetime | None,
    now: dt.datetime | None = None,
) -> bool:
    return bool(descartado_at and descartado_at >= papelera_corte(now))
