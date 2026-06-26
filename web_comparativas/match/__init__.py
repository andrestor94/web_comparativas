"""Módulo Match (Mercado Privado) — homologación asistida de descripciones de portal
con el artículo real del catálogo de Suizo.

FASE 1: todo se construye desde un Excel de propuestas ya calculado (hoja "Todos",
niveles A–D). NO se carga el dataset grande de ~1,5M filas: la interfaz se sirve
SIEMPRE paginada desde tablas compactas (match_propuestas / match_homologaciones).

Kill-switch `MATCH_ENABLED` (feature flag). Default OFF: el módulo no aparece en el
sidebar (gateado por el global Jinja `match_enabled()` en base.html) ni responde en su
ruta/API (gateado por `MATCH_ENABLED()`), salvo que se setee MATCH_ENABLED=1 en el .env.
"""
from __future__ import annotations

import os

_TRUE = {"1", "true", "yes", "on", "y", "si", "s"}


def MATCH_ENABLED() -> bool:
    """Feature flag del módulo Match. Default OFF (módulo nuevo, no productivo aún).

    Formato .env: KEY=value (sin ':'). Valores que lo encienden: 1/true/yes/on/si.
    """
    raw = (os.getenv("MATCH_ENABLED") or "false").strip().lower()
    return raw in _TRUE
