#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
FASE 3a - MEDICION de comportamiento real de usuarios (READ-ONLY).

Objetivo: medir QUE hace realmente un usuario de SIEM en S.I.C -> Seguimiento
de Usuarios, ANTES de recalibrar el score de adopcion (_adoption_score).

Esta herramienta:
  - Es READ-ONLY ABSOLUTO: solo ejecuta SELECT sobre usage_events / users.
    No hace INSERT/UPDATE/DELETE/ALTER ni migraciones.
  - Reutiliza las MISMAS funciones de la app (_adoption_score, _sessionize_events,
    _map_section_name, is_admin_role) para que la medicion sea fiel al sistema.
  - Excluye admins del calculo de adopcion (igual que el dashboard real).
  - Corre contra la DB LOCAL (SQLite app.db). Si detecta que el engine no es
    SQLite, ABORTA (no toca Render/produccion).

Uso:
    python scripts/measure_user_behavior.py

No se cablea a la app. Es un diagnostico de un solo uso.
"""
from __future__ import annotations

import datetime as dt
import statistics
import sys
from collections import Counter, defaultdict
from typing import Any, Dict, List, Optional

# --- Importes de la app (fuente unica de verdad) -------------------------------
from web_comparativas.models import SessionLocal, engine, UsageEvent, User
from web_comparativas.usage_service import (
    _adoption_score,
    _sessionize_events,
    _map_section_name,
    is_admin_role,
    is_metric_eligible_role,
)

# ------------------------------------------------------------------------------
# GUARDA DE SEGURIDAD: solo DB local SQLite. Nunca Render/produccion.
# ------------------------------------------------------------------------------
if engine.url.get_backend_name() != "sqlite":
    print(
        "ABORTADO: el engine NO es SQLite (backend = "
        f"{engine.url.get_backend_name()!r}). Este script es solo para la DB "
        "LOCAL. Quita DATABASE_URL del entorno y volve a correr.",
        file=sys.stderr,
    )
    sys.exit(2)

# Acciones que el dashboard NO cuenta como signal (replicamos su filtrado).
HEARTBEAT = "heartbeat"
API_CALL = "api_call"
VIEW_ACTIONS = {"page_view", "module_visit"}


def _is_signal(ev: UsageEvent) -> bool:
    """Evento 'real' (no heartbeat, no api_call) -- como current_non_heartbeat."""
    at = (ev.action_type or "").lower()
    return at not in (HEARTBEAT, API_CALL)


def _is_sessionizable(ev: UsageEvent) -> bool:
    """Eventos que entran a _sessionize_events en el dashboard: todo menos api_call
    (los heartbeat SI cuentan para tiempo activo)."""
    return (ev.action_type or "").lower() != API_CALL


def compute_user_signals(events: List[UsageEvent], now: dt.datetime) -> Dict[str, Any]:
    """Calcula TODAS las senales para un usuario sobre la lista de eventos dada,
    usando exactamente la logica del dashboard."""
    signal_events = [e for e in events if _is_signal(e)]
    sessionizable = [e for e in events if _is_sessionizable(e)]

    active_days = len({e.timestamp.date() for e in signal_events if e.timestamp})

    session_data = _sessionize_events(sessionizable)
    active_minutes = float(session_data["active_minutes"])
    sessions = int(session_data["count"])

    def count(at: str) -> int:
        return sum(1 for e in signal_events if (e.action_type or "").lower() == at)

    searches = count("search")
    exports = count("export")
    uploads = count("file_upload")
    page_views = sum(
        1 for e in signal_events if (e.action_type or "").lower() in VIEW_ACTIONS
    )

    module_names = Counter(
        _map_section_name(e.section or "") for e in signal_events
    )
    module_names.pop("", None)
    modules_used = len(module_names)

    total_signal_events = len(signal_events)

    last_ts = max((e.timestamp for e in signal_events if e.timestamp), default=None)
    recency_days = (now - last_ts).days if last_ts else None

    # Score viejo + desglose por componente (mismos pesos que _adoption_score)
    comp_days = min(active_days / 8.0, 1.0) * 20
    comp_minutes = min(active_minutes / 240.0, 1.0) * 15
    comp_uploads = min(uploads / 5.0, 1.0) * 25
    comp_searches = min(searches / 8.0, 1.0) * 15
    comp_exports = min(exports / 4.0, 1.0) * 10
    comp_modules = min(modules_used / 4.0, 1.0) * 15
    score = _adoption_score(active_days, active_minutes, uploads, searches, exports, modules_used)

    return {
        "active_days": active_days,
        "active_minutes": round(active_minutes, 1),
        "active_hours": round(active_minutes / 60.0, 1),
        "sessions": sessions,
        "searches": searches,
        "exports": exports,
        "uploads": uploads,
        "page_views": page_views,
        "modules_used": modules_used,
        "modules_list": [n for n, _ in module_names.most_common(6)],
        "total_signal_events": total_signal_events,
        "recency_days": recency_days,
        "score": score,
        "comp_days": round(comp_days, 1),
        "comp_minutes": round(comp_minutes, 1),
        "comp_uploads": round(comp_uploads, 1),
        "comp_searches": round(comp_searches, 1),
        "comp_exports": round(comp_exports, 1),
        "comp_modules": round(comp_modules, 1),
        # Puntos PERDIDOS en los componentes "nuevos / de resultado"
        "lost_uploads": round(25 - comp_uploads, 1),
        "lost_searches": round(15 - comp_searches, 1),
        "lost_exports": round(10 - comp_exports, 1),
    }


# ------------------------------------------------------------------------------
# Distribuciones (min / max / mediana / promedio entre usuarios)
# ------------------------------------------------------------------------------
DISTRIB_SIGNALS = [
    ("active_days", "Dias activos"),
    ("active_hours", "Horas activas (sesionizado)"),
    ("searches", "Busquedas (search)"),
    ("exports", "Exportaciones (export)"),
    ("uploads", "Cargas (file_upload)"),
    ("modules_used", "Modulos distintos visitados"),
    ("page_views", "Page views"),
    ("total_signal_events", "Eventos totales (signal)"),
    ("recency_days", "Recencia (dias desde ult. evento)"),
    ("score", "SCORE VIEJO (_adoption_score)"),
]


def distribution_block(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    header = f"  {'Senal':<38}{'min':>7}{'mediana':>10}{'prom':>9}{'max':>7}{'=0 (n)':>9}"
    out.append(header)
    out.append("  " + "-" * (len(header) - 2))
    for key, label in DISTRIB_SIGNALS:
        vals = [r["signals"][key] for r in rows if r["signals"][key] is not None]
        if not vals:
            out.append(f"  {label:<38}{'-':>7}{'-':>10}{'-':>9}{'-':>7}{'-':>9}")
            continue
        zeros = sum(1 for v in vals if v == 0)
        out.append(
            f"  {label:<38}{min(vals):>7.1f}{statistics.median(vals):>10.1f}"
            f"{statistics.mean(vals):>9.1f}{max(vals):>7.1f}{zeros:>6}/{len(vals)}"
        )
    return out


# ------------------------------------------------------------------------------
# Tabla por usuario
# ------------------------------------------------------------------------------
def per_user_table(rows: List[Dict[str, Any]]) -> List[str]:
    out: List[str] = []
    rows = sorted(rows, key=lambda r: r["signals"]["score"], reverse=True)
    header = (
        f"  {'Usuario':<26}{'rol':<11}{'dias':>5}{'hrs':>6}{'busq':>6}{'exp':>5}"
        f"{'carg':>6}{'mods':>6}{'pv':>6}{'evt':>6}{'rec':>5}{'SCORE':>7}"
    )
    out.append(header)
    out.append("  " + "-" * (len(header) - 2))
    for r in rows:
        s = r["signals"]
        rec = "-" if s["recency_days"] is None else str(s["recency_days"])
        out.append(
            f"  {r['name'][:25]:<26}{r['role'][:10]:<11}{s['active_days']:>5}"
            f"{s['active_hours']:>6.1f}{s['searches']:>6}{s['exports']:>5}"
            f"{s['uploads']:>6}{s['modules_used']:>6}{s['page_views']:>6}"
            f"{s['total_signal_events']:>6}{rec:>5}{s['score']:>7}"
        )
    return out


def score_breakdown_table(rows: List[Dict[str, Any]]) -> List[str]:
    """Desglose del score viejo por componente, para ver cuanto pierde cada
    usuario en cargas/busquedas/exportaciones."""
    out: List[str] = []
    rows = sorted(rows, key=lambda r: r["signals"]["score"], reverse=True)
    header = (
        f"  {'Usuario':<26}{'dias/20':>9}{'min/15':>9}{'carg/25':>9}"
        f"{'busq/15':>9}{'exp/10':>9}{'mods/15':>9}{'TOTAL':>7}{'perdido c+b+e':>15}"
    )
    out.append(header)
    out.append("  " + "-" * (len(header) - 2))
    for r in rows:
        s = r["signals"]
        lost = s["lost_uploads"] + s["lost_searches"] + s["lost_exports"]
        out.append(
            f"  {r['name'][:25]:<26}{s['comp_days']:>9.1f}{s['comp_minutes']:>9.1f}"
            f"{s['comp_uploads']:>9.1f}{s['comp_searches']:>9.1f}{s['comp_exports']:>9.1f}"
            f"{s['comp_modules']:>9.1f}{s['score']:>7}{lost:>15.1f}"
        )
    return out


def build_window_section(
    title: str,
    eligible_users: List[User],
    events_by_user: Dict[int, List[UsageEvent]],
    now: dt.datetime,
    window_start: Optional[dt.datetime],
) -> List[str]:
    out: List[str] = []
    out.append("")
    out.append("=" * 100)
    out.append(title)
    out.append("=" * 100)

    rows: List[Dict[str, Any]] = []
    for u in eligible_users:
        evs = events_by_user.get(int(u.id), [])
        if window_start is not None:
            evs = [e for e in evs if e.timestamp and e.timestamp >= window_start]
        sig = compute_user_signals(evs, now)
        rows.append({
            "user_id": int(u.id),
            "name": u.full_name or u.name or (u.email.split("@")[0].title() if u.email else f"user{u.id}"),
            "role": (u.role or "sin rol"),
            "signals": sig,
        })

    active_rows = [r for r in rows if r["signals"]["total_signal_events"] > 0]
    out.append("")
    out.append(f"Usuarios elegibles (no-admin): {len(rows)}  |  con actividad en ventana: {len(active_rows)}")
    out.append("")
    out.append("DISTRIBUCION ENTRE USUARIOS (solo usuarios CON actividad):")
    out.extend(distribution_block(active_rows if active_rows else rows))
    out.append("")
    out.append("TABLA POR USUARIO (todos los elegibles, orden por score desc):")
    out.extend(per_user_table(rows))
    out.append("")
    out.append("DESGLOSE DEL SCORE VIEJO POR COMPONENTE:")
    out.extend(score_breakdown_table(rows))
    return out


def main() -> None:
    s = SessionLocal()
    try:
        now = dt.datetime.utcnow()

        # --- Muestra global ---------------------------------------------------
        total_events = s.query(UsageEvent).count()
        min_ts = s.query(UsageEvent.timestamp).order_by(UsageEvent.timestamp.asc()).first()
        max_ts = s.query(UsageEvent.timestamp).order_by(UsageEvent.timestamp.desc()).first()
        min_ts = min_ts[0] if min_ts else None
        max_ts = max_ts[0] if max_ts else None

        all_users = s.query(User).all()
        eligible_users = [u for u in all_users if is_metric_eligible_role(u.role)]
        admin_users = [u for u in all_users if is_admin_role(u.role)]

        # Distribucion de action_type a nivel global (no por usuario)
        action_rows = (
            s.query(UsageEvent.action_type)
            .all()
        )
        action_counter = Counter((a[0] or "").lower() for a in action_rows)

        # Eventos por usuario elegible (excluye api_call, como el dashboard)
        eligible_ids = [int(u.id) for u in eligible_users]
        events_by_user: Dict[int, List[UsageEvent]] = defaultdict(list)
        if eligible_ids:
            for ev in (
                s.query(UsageEvent)
                .filter(UsageEvent.user_id.in_(eligible_ids))
                .all()
            ):
                events_by_user[int(ev.user_id)].append(ev)

        # --- Cabecera del informe --------------------------------------------
        out: List[str] = []
        out.append("#" * 100)
        out.append("FASE 3a - MEDICION DE COMPORTAMIENTO REAL DE USUARIOS  (READ-ONLY)")
        out.append(f"DB: {engine.url}  |  generado (UTC): {now.isoformat(timespec='seconds')}")
        out.append("#" * 100)
        out.append("")
        out.append("TAMANO DE MUESTRA")
        out.append("-" * 100)
        out.append(f"  Eventos totales en usage_events ........ {total_events:,}")
        out.append(f"  Rango temporal ......................... {min_ts} -> {max_ts}")
        out.append(f"  Usuarios totales ....................... {len(all_users)}")
        out.append(f"  Usuarios ELEGIBLES (no-admin) .......... {len(eligible_users)}")
        out.append(f"  Usuarios admin (excluidos de adopcion) . {len(admin_users)}")
        out.append("")
        out.append("  Distribucion GLOBAL de action_type (todos los eventos):")
        for at, n in action_counter.most_common():
            pct = (n / total_events * 100) if total_events else 0
            out.append(f"     {at or '(vacio)':<22}{n:>10,}  ({pct:5.1f}%)")

        # --- Ventana 1: ultimos 30 dias --------------------------------------
        win_30 = now - dt.timedelta(days=30)
        out.extend(build_window_section(
            "VENTANA A: ULTIMOS 30 DIAS",
            eligible_users, events_by_user, now, win_30,
        ))

        # --- Ventana 2: todo el historial ------------------------------------
        out.extend(build_window_section(
            "VENTANA B: TODO EL HISTORIAL",
            eligible_users, events_by_user, now, None,
        ))

        report = "\n".join(out)
        print(report)
    finally:
        s.close()


if __name__ == "__main__":
    main()
