#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
RE-MEDICION POST-TRAFICO - verificar captura de search/export (READ-ONLY).

Tras ejercitar manualmente busquedas y descargas en el navegador, este script
confirma si la instrumentacion de Fase 2 persistio eventos en usage_events.

READ-ONLY ABSOLUTO: solo SELECT. Aborta si el engine no es SQLite (no toca prod).
"""
from __future__ import annotations

import datetime as dt
import json
import sys
from collections import Counter

from web_comparativas.models import SessionLocal, engine, UsageEvent
from web_comparativas.usage_service import _map_section_name

if engine.url.get_backend_name() != "sqlite":
    print("ABORTADO: engine no es SQLite. Solo DB local.", file=sys.stderr)
    sys.exit(2)


def short(v, n=200):
    try:
        s = json.dumps(v, ensure_ascii=False, default=str)
    except Exception:
        s = str(v)
    return s if len(s) <= n else s[: n - 1] + "…"


def dump(action: str, s, now):
    q = (
        s.query(UsageEvent)
        .filter(UsageEvent.action_type.ilike(action))
        .order_by(UsageEvent.timestamp.desc())
    )
    rows = q.all()
    print("=" * 100)
    print(f"action_type = {action!r}  ->  TOTAL eventos = {len(rows)}")
    print("=" * 100)
    if not rows:
        print("  (sin eventos)\n")
        return rows

    # Distribucion por seccion cruda
    by_section = Counter((r.section or "(null)") for r in rows)
    print("  Por section (cruda -> etiqueta canonica):")
    for sec, n in by_section.most_common():
        label = _map_section_name(sec) if sec != "(null)" else ""
        print(f"     {sec:<32} -> {label or '(vacia)':<28} x{n}")
    print()

    print("  Filas recientes (hasta 40):")
    for r in rows[:40]:
        age_min = (now - r.timestamp).total_seconds() / 60.0 if r.timestamp else None
        age = f"{age_min:6.1f}m" if age_min is not None else "   ?  "
        label = _map_section_name(r.section or "")
        print(
            f"   id={r.id:<7} {str(r.timestamp)[:19]}  (hace {age})  uid={r.user_id} "
            f"role={r.user_role!r}"
        )
        print(
            f"        section={r.section!r} -> {label!r}  resource_id={r.resource_id!r}"
        )
        print(f"        extra_data={short(r.extra_data)}")
    print()
    return rows


def main():
    s = SessionLocal()
    try:
        now = dt.datetime.utcnow()
        print(f"DB: {engine.url}")
        print(f"Generado (UTC): {now.isoformat(timespec='seconds')}\n")

        search_rows = dump("search", s, now)
        export_rows = dump("export", s, now)

        # --- Anti-ruido sobre search: agrupar por (uid, termino, minuto) -------
        print("=" * 100)
        print("ANTI-RUIDO (search): posibles duplicados por paginacion / carga inicial")
        print("=" * 100)
        if search_rows:
            buckets = Counter()
            for r in search_rows:
                ed = r.extra_data or {}
                term = (
                    ed.get("query") or ed.get("search") or ed.get("term")
                    or ed.get("q") or r.resource_id or "(sin termino)"
                )
                minute = str(r.timestamp)[:16] if r.timestamp else "?"
                buckets[(r.user_id, str(term), minute, r.section or "")] += 1
            dups = {k: v for k, v in buckets.items() if v > 1}
            print(f"  Grupos (uid, termino, minuto, section) con >1 evento: {len(dups)}")
            for (uid, term, minute, sec), n in sorted(dups.items(), key=lambda x: -x[1]):
                print(f"     x{n}  uid={uid} min={minute} sec={sec!r} term={term!r}")
            if not dups:
                print("  OK: no se detectan duplicados por minuto/termino.")
            # Cuantos terminos distintos
            terms = Counter()
            for r in search_rows:
                ed = r.extra_data or {}
                term = ed.get("query") or ed.get("search") or ed.get("term") or ed.get("q") or r.resource_id or "(sin termino)"
                terms[str(term)] += 1
            print(f"\n  Terminos de busqueda distintos: {len(terms)}")
            for term, n in terms.most_common(20):
                print(f"     x{n}  {term!r}")
        else:
            print("  (no hay search para analizar)")
        print()

        # --- Cruce export vs CSV client-side esperados -----------------------
        print("=" * 100)
        print("CRUCE EXPORT: ¿aparecen los 2 CSV client-side (Oportunidades + Forecast)?")
        print("=" * 100)
        if export_rows:
            for r in export_rows:
                ed = r.extra_data or {}
                fmt = ed.get("format") or ed.get("export_type") or ed.get("file_type") or ""
                name = ed.get("filename") or ed.get("file_name") or r.resource_id or ""
                label = _map_section_name(r.section or "")
                print(
                    f"   id={r.id} {str(r.timestamp)[:19]} uid={r.user_id} "
                    f"section={r.section!r}->{label!r} fmt={fmt!r} name={name!r}"
                )
        else:
            print("  No hay NINGUN evento export -> los 2 CSV client-side estan AUSENTES.")
        print()

        # --- Para diagnostico: que otras acciones llegaron en la ultima hora --
        print("=" * 100)
        print("CONTEXTO: action_type en la ultima 1 hora (para diagnostico anti-silencio)")
        print("=" * 100)
        cutoff = now - dt.timedelta(hours=1)
        recent = (
            s.query(UsageEvent)
            .filter(UsageEvent.timestamp >= cutoff)
            .order_by(UsageEvent.timestamp.desc())
            .all()
        )
        ac = Counter((r.action_type or "").lower() for r in recent)
        print(f"  Eventos en la ultima hora: {len(recent)}")
        for at, n in ac.most_common():
            print(f"     {at or '(vacio)':<22} x{n}")
        # secciones de los page_view recientes, por si search se guardo como page_view
        print("\n  Secciones (cruda) de eventos recientes NO heartbeat/api (ult. hora):")
        secs = Counter(
            (r.section or "(null)")
            for r in recent
            if (r.action_type or "").lower() not in ("heartbeat", "api_call")
        )
        for sec, n in secs.most_common(25):
            print(f"     {sec:<40} x{n}")
    finally:
        s.close()


if __name__ == "__main__":
    main()
