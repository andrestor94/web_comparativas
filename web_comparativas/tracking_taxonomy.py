"""
tracking_taxonomy.py
────────────────────
FUENTE ÚNICA DE VERDAD para la taxonomía de "secciones" del seguimiento de uso
(módulo S.I.C → Seguimiento de Usuarios).

Hasta esta fase convivían CUATRO mapas de secciones independientes que no
concordaban:
  (1) middleware Python  `_detect_section`        (path → clave)
  (2) JS `_detectTrackingSection` en base.html     (path → clave)
  (3) JS `trackEvent` en base_sic.html             (path → clave)
  (4) JS `blockForSection` / `_LABEL_BLOCK` (3D)   (etiqueta/clave → módulo)
más el etiquetador `_map_section_name` (clave → etiqueta legible).

Este módulo unifica todo en UN solo lugar:
  - `detect_key(path)`     → clave canónica desde una URL (reemplaza 1/2/3).
  - `label_for(raw)`       → etiqueta legible (reemplaza `_map_section_name`).
  - `module_for(raw)`      → módulo padre (de nav.py) para el 3D (reemplaza 4).
  - `room_for(raw)`        → sala 3D reservada para la Fase 4 (no se usa aún).
  - `to_json()`            → serialización para inyectar la MISMA taxonomía al
                             front (un endpoint genera el JS; no se copia a mano).

PRINCIPIO DE DISEÑO
  - Se aplica en la CAPA DE PRESENTACIÓN: el historial ya guardado en
    `usage_events` se re-mapea al mostrarlo, vía el mapa de ALIAS crudo→canónico.
    NO se toca la DB ni se migra nada.
  - El eje "módulo" se DERIVA de nav.py (única fuente de permisos/menú): cada
    sección referencia una key top-level real de nav.MENU, y al importar se
    valida que exista. Las etiquetas de módulo se toman de nav.MODULES.
    nav.py NO se modifica (su semántica de permisos queda intacta).
"""
from __future__ import annotations

import json
import re
from typing import Dict, List, Optional

from . import nav

# ──────────────────────────────────────────────────────────────────────────────
# Salas 3D (reservadas para Fase 4 — el mapa NO se rediseña en esta fase)
# ──────────────────────────────────────────────────────────────────────────────
ROOM_SIC = "sic"
ROOM_FORECAST = "forecast"
ROOM_INDICADORES = "indicadores"          # nuevo: hoy el 3D no tiene esta sala
ROOM_MERCADO_PUBLICO = "mercado_publico"
ROOM_MERCADO_PRIVADO = "mercado_privado"

# ──────────────────────────────────────────────────────────────────────────────
# TAXONOMÍA CANÓNICA
# Cada sección: key (canónica) | label (legible) | module (key top-level de
# nav.py o None) | room (sala 3D reservada) | aliases (claves crudas VIEJAS que
# producían los 4 detectores; cubren el historial guardado).
# ──────────────────────────────────────────────────────────────────────────────
SECTIONS: List[dict] = [
    # ── Genérico / Inicio ────────────────────────────────────────────────────
    # Los "inicios" genéricos que apuntan al landing post-login se UNIFICAN en
    # "Inicio": home (/), panel principal (/dashboard) y centro de mercados
    # (/markets). Los homes de cada módulo (abajo) mantienen su identidad propia.
    {"key": "home", "label": "Inicio", "module": None, "room": ROOM_SIC,
     "aliases": ["home", "inicio", "inicio_general", "dashboard",
                 "inicio_panel_principal", "markets", "markets_home",
                 "centro_de_mercados", "inicio_mercados", "inicio_siem"]},

    # ── S.I.C ────────────────────────────────────────────────────────────────
    {"key": "sic_general", "label": "S.I.C", "module": "sic", "room": ROOM_SIC,
     "aliases": ["sic", "sic_home", "sic_general", "s_i_c", "s_i_c_general",
                 "panel_s_i_c", "panel_sic"]},
    {"key": "sic_helpdesk", "label": "Mesa de Ayuda", "module": "sic", "room": ROOM_SIC,
     "aliases": ["sic_helpdesk", "sic_helpdesk_tickets", "mesa_de_ayuda"]},
    {"key": "sic_tracking", "label": "Seguimiento de Usuarios", "module": "sic", "room": ROOM_SIC,
     "aliases": ["sic_tracking", "sic_tracking_api", "seguimiento_de_usuarios",
                 "seguimiento", "api_tracking_interno", "panel_live",
                 "live_users_dashboard"]},
    {"key": "sic_users", "label": "Usuarios", "module": "sic", "room": ROOM_SIC,
     "aliases": ["sic_users", "sic_usuarios", "usuarios", "gestion_de_usuarios"]},
    # (1) Acción ADMIN: gestión de reseteos sobre credenciales de OTROS usuarios.
    #     Rutas: /sic/password-resets (cola SIC) y /admin/reset-solicitudes.
    {"key": "sic_password_resets", "label": "Administración de Reseteos", "module": "sic", "room": ROOM_SIC,
     "aliases": ["sic_password_resets", "admin_password_resets",
                 "administracion_de_reseteos", "reseteo_de_contrasenas",
                 "gestion_de_contrasenas",  # plural = panel admin (label viejo)
                 "contrasenas"]},           # nav sidebar "Contraseñas" = panel admin
    {"key": "administracion", "label": "Administración", "module": "sic", "room": ROOM_SIC,
     "aliases": ["administracion"]},
    {"key": "grupos", "label": "Grupos", "module": "sic", "room": ROOM_SIC,
     "aliases": ["grupos"]},
    {"key": "notificaciones", "label": "Notificaciones", "module": "sic", "room": ROOM_SIC,
     "aliases": ["notificaciones"]},

    # ── Forecast ─────────────────────────────────────────────────────────────
    {"key": "forecast", "label": "Forecast", "module": "forecast", "room": ROOM_FORECAST,
     "aliases": ["forecast", "forecast_widget", "proyecciones_y_forecast",
                 "panel_de_forecast", "proyecciones"]},

    # ── Indicadores Comerciales (NUEVO: identidad propia) ─────────────────────
    {"key": "indicadores_comerciales", "label": "Indicadores Comerciales",
     "module": "indicadores_comerciales", "room": ROOM_INDICADORES,
     "aliases": ["indicadores_comerciales", "indicadores",
                 # workflow de importación (admin) registrado en el historial
                 "indicadores_import_pending", "indicadores_import_approve",
                 "indicadores_import_discard"]},
    {"key": "indicadores_home", "label": "Indicadores Comerciales — Inicio",
     "module": "indicadores_comerciales", "room": ROOM_INDICADORES,
     "aliases": ["indicadores_comerciales_home", "indicadores_home"]},
    {"key": "indicadores_rentabilidad_negativa", "label": "Indicadores — Rentabilidad Negativa",
     "module": "indicadores_comerciales", "room": ROOM_INDICADORES,
     "aliases": ["indicadores_rentabilidad_negativa", "rentabilidad_negativa",
                 "indicadores_comerciales_rentabilidad_negativa"]},  # forma larga del historial
    {"key": "indicadores_inflacion", "label": "Indicadores — Inflación",
     "module": "indicadores_comerciales", "room": ROOM_INDICADORES,
     "aliases": ["indicadores_inflacion", "inflacion",
                 "indicadores_comerciales_inflacion"]},  # forma larga del historial
    {"key": "indicadores_informes_laboratorio", "label": "Indicadores — Informes de Laboratorio",
     "module": "indicadores_comerciales", "room": ROOM_INDICADORES,
     "aliases": ["indicadores_informes_laboratorio", "informes_laboratorio",
                 "informes_de_laboratorio",
                 # formas largas + sub-recursos de laboratorio del historial
                 "indicadores_comerciales_informes_laboratorio",
                 "indicadores_comerciales_laboratorios_resumen",
                 "indicadores_comerciales_laboratorios_metadata",
                 "indicadores_comerciales_laboratorios_detalle",
                 "laboratorios_resumen", "laboratorios_metadata", "laboratorios_detalle"]},

    # ── Mercado Público ──────────────────────────────────────────────────────
    {"key": "mercado_publico_home", "label": "Mercado Público — Inicio",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["mercado_publico_home", "mercado_publico"]},
    {"key": "comparativa_mercado", "label": "Comparativa de Mercado",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["comparativa", "comparativa_mercado", "web_comparativas",
                 "comparativas", "web_comparativa_home"]},
    {"key": "tablero_comparativa", "label": "Tablero de Comparativa",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["tablero", "tablero_comparativa", "tablero_ranking"]},
    {"key": "vistas_guardadas", "label": "Vistas Guardadas",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["vistas_guardadas"]},
    {"key": "cargas", "label": "Cargas", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["cargas", "cargas_detalle", "cargas_original"]},
    {"key": "cargas_nueva", "label": "Cargas — Nueva", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["cargas_nueva", "nueva_carga"]},
    {"key": "cargas_historial", "label": "Cargas — Historial", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["cargas_historial"]},
    {"key": "cargas_edicion", "label": "Cargas — Edición", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["cargas_edicion", "edicion_de_carga"]},
    {"key": "fuentes_externas", "label": "Fuentes Externas", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["fuentes_externas", "otras_fuentes_nueva",
                 "otas_fuentes_crear",  # typo real del historial ("otas")
                 "otras_fuentes_externas"]},
    {"key": "lectura_pliegos", "label": "Lectura de Pliegos", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["lectura_pliegos", "pliegos", "pliego_widget", "pliego_detalle"]},
    {"key": "mercado_publico_oportunidades", "label": "Oportunidades",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["mercado_publico_oportunidades", "oportunidades"]},
    {"key": "oportunidades_buscador", "label": "Buscador de Oportunidades",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["oportunidades_buscador"]},
    {"key": "oportunidades_dimensiones", "label": "Dimensiones de Oportunidades",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["oportunidades_dimensiones"]},
    {"key": "mercado_publico_reporte_perfiles", "label": "Reporte de Perfiles — Mercado Público",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["mercado_publico_reporte_perfiles"]},
    {"key": "mercado_publico_helpdesk", "label": "Mesa de Ayuda — Mercado Público",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["mercado_publico_helpdesk"]},

    # ── Mercado Privado ──────────────────────────────────────────────────────
    {"key": "mercado_privado_home", "label": "Mercado Privado — Inicio",
     "module": "mercado_privado", "room": ROOM_MERCADO_PRIVADO,
     "aliases": ["mercado_privado_home", "mercado_privado"]},
    {"key": "dimensionamiento", "label": "Dimensionamiento", "module": "mercado_privado", "room": ROOM_MERCADO_PRIVADO,
     "aliases": ["dimensionamiento", "mercado_privado_dimensiones", "dimensiones_privado"]},
    {"key": "mercado_privado_reporte_perfiles", "label": "Reporte de Perfiles — Mercado Privado",
     "module": "mercado_privado", "room": ROOM_MERCADO_PRIVADO,
     "aliases": ["mercado_privado_reporte_perfiles"]},
    {"key": "mercado_privado_helpdesk", "label": "Mesa de Ayuda — Mercado Privado",
     "module": "mercado_privado", "room": ROOM_MERCADO_PRIVADO,
     "aliases": ["mercado_privado_helpdesk"]},

    # ── Reporte de Perfiles genérico (legacy, sin mercado) ───────────────────
    # Clave cruda vieja AMBIGUA: el evento no dice de qué mercado es. NO se
    # inventa: se asigna módulo mercado_publico como best-effort documentado
    # (el reporte original vivía bajo Mercado Público antes del split por
    # mercado). Comparte etiqueta unificada "Reporte de Perfiles".
    {"key": "reporte_perfiles", "label": "Reporte de Perfiles",
     "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["reporte_perfiles", "perfiles", "perfiles_de_clientes"]},

    # ── Descargas / Informes / Comentarios / Clientes ────────────────────────
    {"key": "descargas", "label": "Descargas", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     # "normalized_download": sección cruda vieja de las descargas normalized.xlsx
     # (la usaban /descargas y /api/descargar-final con action_type download_*).
     # Solo re-mapea la ETIQUETA del historial; el action_type viejo NO cambia.
     "aliases": ["descargas", "normalized_download"]},
    {"key": "informes", "label": "Informes", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["informes"]},
    {"key": "reporte_proceso", "label": "Reporte de Proceso", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["reporte_proceso"]},
    {"key": "comentarios", "label": "Comentarios", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["comentarios"]},
    {"key": "clientes_api", "label": "Consulta de Clientes", "module": "mercado_publico", "room": ROOM_MERCADO_PUBLICO,
     "aliases": ["clientes_api"]},

    # ── Autenticación / Sistema (no son módulos navegables) ──────────────────
    {"key": "auth_login", "label": "Inicio de Sesión", "module": None, "room": None,
     "aliases": ["auth", "auth_login", "inicio_de_sesion"]},
    {"key": "auth_logout", "label": "Cierre de Sesión", "module": None, "room": None,
     "aliases": ["auth_logout", "cierre_de_sesion", "logout"]},
    # (2) Usuario común cambiando SU propia clave. Rutas: /mi/password, /password,
    #     /mercado-privado/mi-password.
    {"key": "auth_password", "label": "Mi Contraseña", "module": None, "room": None,
     "aliases": ["auth_password", "mi_password",
                 "gestion_de_contrasena",  # singular = clave propia (label viejo)
                 "gestion_de_contrasena_propia"]},

    # ── Otros / Sin clasificar (catch-all explícito) ─────────────────────────
    {"key": "otros", "label": "Otros / Sin clasificar", "module": None, "room": None,
     "aliases": ["otro", "otros", "sin_identificar", "sin_clasificar", "unknown",
                 "undefined", "n_a", "no_identificado"]},
]

# ──────────────────────────────────────────────────────────────────────────────
# REGLAS DE DETECCIÓN path → clave canónica (ordenadas; primera que matchea gana)
# op ∈ {"eq" (igualdad), "prefix" (empieza con), "contains" (subcadena)}.
# Portadas 1:1 desde la versión efectiva del middleware `_detect_section`,
# con anclas multi-segmento para desambiguar mercado público/privado y con
# Indicadores Comerciales agregado. Este MISMO listado se serializa al front.
# ──────────────────────────────────────────────────────────────────────────────
DETECT_RULES: List[tuple] = [
    ("eq", "/", "home"),

    # S.I.C
    ("contains", "/sic/api/usage", "sic_tracking"),
    ("contains", "/sic/api/track-event", "sic_tracking"),
    ("contains", "/sic/helpdesk", "sic_helpdesk"),
    ("contains", "/sic/api/tickets", "sic_helpdesk"),
    ("contains", "/sic/password-resets", "sic_password_resets"),
    ("contains", "/sic/api/password-resets", "sic_password_resets"),
    ("contains", "/sic/users", "sic_users"),
    ("contains", "/sic/usuarios", "sic_users"),
    ("contains", "/sic/tracking", "sic_tracking"),
    ("prefix", "/sic", "sic_general"),

    # Mercado Privado (anclado a "mercado-privado/…" → cubre /x y /api/x)
    ("contains", "mercado-privado/reporte-perfiles", "mercado_privado_reporte_perfiles"),
    ("contains", "mercado-privado/perfiles", "mercado_privado_reporte_perfiles"),
    ("contains", "mercado-privado/dimensiones", "dimensionamiento"),
    ("contains", "mercado-privado/oportunidades", "dimensionamiento"),
    ("contains", "mercado-privado/comentarios", "comentarios"),
    ("contains", "mercado-privado/mi-password", "auth_password"),
    ("contains", "mercado-privado/helpdesk", "mercado_privado_helpdesk"),
    ("eq", "/mercado-privado", "mercado_privado_home"),
    ("prefix", "/mercado-privado", "mercado_privado_home"),

    # Mercado Público (anclado a "mercado-publico/…")
    ("contains", "mercado-publico/reporte-perfiles", "mercado_publico_reporte_perfiles"),
    ("contains", "mercado-publico/perfiles", "mercado_publico_reporte_perfiles"),
    ("contains", "mercado-publico/lectura-pliegos", "lectura_pliegos"),
    ("contains", "mercado-publico/pliegos", "lectura_pliegos"),
    ("contains", "mercado-publico/web-comparativas", "comparativa_mercado"),
    ("contains", "mercado-publico/comparativas", "comparativa_mercado"),
    ("contains", "mercado-publico/oportunidades", "mercado_publico_oportunidades"),
    ("contains", "mercado-publico/helpdesk", "mercado_publico_helpdesk"),
    ("eq", "/mercado-publico", "mercado_publico_home"),
    ("prefix", "/mercado-publico", "mercado_publico_home"),

    # Oportunidades (standalone /oportunidades y /api/oportunidades)
    ("contains", "oportunidades/dimensiones", "oportunidades_dimensiones"),
    ("contains", "oportunidades/buscador", "oportunidades_buscador"),
    ("contains", "/api/oportunidades", "mercado_publico_oportunidades"),
    ("prefix", "/oportunidades", "mercado_publico_oportunidades"),

    # Cargas / fuentes
    ("contains", "/cargas/historial", "cargas_historial"),
    ("contains", "/cargas/nueva", "cargas_nueva"),
    ("contains", "/cargas/editar", "cargas_edicion"),
    ("contains", "/api/cargas", "cargas"),
    ("prefix", "/cargas", "cargas"),
    ("prefix", "/otras-fuentes", "fuentes_externas"),

    # Descargas / tablero / vistas / informes
    ("contains", "/api/descargar-final", "descargas"),
    ("prefix", "/descargas", "descargas"),
    ("contains", "/api/tablero", "tablero_comparativa"),
    ("prefix", "/tablero", "tablero_comparativa"),
    ("contains", "/api/views", "vistas_guardadas"),
    ("contains", "/api/presets", "vistas_guardadas"),
    ("prefix", "/reportes/proceso", "reporte_proceso"),
    ("prefix", "/informes", "informes"),

    # Indicadores Comerciales (NUEVO)
    ("contains", "rentabilidad-negativa", "indicadores_rentabilidad_negativa"),
    ("contains", "informes-laboratorio", "indicadores_informes_laboratorio"),
    ("contains", "indicadores-comerciales/inflacion", "indicadores_inflacion"),
    ("contains", "indicadores-comerciales/home", "indicadores_home"),
    ("prefix", "/indicadores-comerciales", "indicadores_comerciales"),
    ("contains", "/api/indicadores-comerciales", "indicadores_comerciales"),

    # Forecast / sistema
    ("prefix", "/forecast", "forecast"),
    ("prefix", "/dashboard", "home"),
    ("prefix", "/markets", "home"),
    ("prefix", "/switch-market", "home"),
    ("prefix", "/admin/reset-solicitudes", "sic_password_resets"),
    ("prefix", "/admin", "administracion"),
    ("prefix", "/grupos", "grupos"),
    ("contains", "/api/notifications", "notificaciones"),
    ("prefix", "/notifications", "notificaciones"),
    ("prefix", "/comentarios", "comentarios"),
    ("contains", "/api/comments", "comentarios"),
    ("contains", "/api/clientes", "clientes_api"),

    # Auth
    ("prefix", "/login", "auth_login"),
    ("prefix", "/logout", "auth_logout"),
    ("prefix", "/mi/password", "auth_password"),
    ("prefix", "/password", "auth_password"),
]

# ──────────────────────────────────────────────────────────────────────────────
# Índices y normalización
# ──────────────────────────────────────────────────────────────────────────────
SECTION_BY_KEY: Dict[str, dict] = {s["key"]: s for s in SECTIONS}


def _norm(raw: Optional[str]) -> str:
    """Normaliza una clave/etiqueta cruda a token comparable: minúsculas, sin
    acentos suaves, separadores (-, /, espacios, puntos, em-dash) → '_'."""
    text = (raw or "").strip().lower()
    if not text:
        return ""
    text = text.split("?", 1)[0].split("#", 1)[0]
    text = text.replace("—", " ").replace("–", " ")
    text = re.sub(r"[\s\-/.]+", "_", text)
    text = re.sub(r"_+", "_", text).strip("_")
    return text


# alias normalizado → clave canónica (incluye: claves propias, aliases
# declarados y la etiqueta normalizada de cada sección para soportar "re-feed"
# de etiquetas ya mapeadas que vuelven a entrar).
ALIAS_TO_KEY: Dict[str, str] = {}
for _s in SECTIONS:
    ALIAS_TO_KEY[_norm(_s["key"])] = _s["key"]
    for _a in _s.get("aliases", []):
        ALIAS_TO_KEY.setdefault(_norm(_a), _s["key"])
for _s in SECTIONS:
    # las etiquetas se agregan al final para no pisar aliases más específicos;
    # etiquetas compartidas (p.ej. ningún caso hoy) conservan el primer dueño.
    ALIAS_TO_KEY.setdefault(_norm(_s["label"]), _s["key"])

# Validación: cada módulo debe ser una key top-level real de nav.py (o None).
_VALID_MODULES = set(nav.TOP_LEVEL_KEYS)
for _s in SECTIONS:
    _m = _s["module"]
    if _m is not None and _m not in _VALID_MODULES:
        raise ValueError(
            f"tracking_taxonomy: módulo '{_m}' de la sección '{_s['key']}' no "
            f"existe en nav.TOP_LEVEL_KEYS {_VALID_MODULES}"
        )

# Etiqueta legible de cada módulo, tomada de nav (no se duplica a mano).
MODULE_LABELS: Dict[str, str] = dict(nav.MODULES)


# ──────────────────────────────────────────────────────────────────────────────
# API pública
# ──────────────────────────────────────────────────────────────────────────────
_ACRONYMS = {"sic": "S.I.C", "siem": "SIEM", "ia": "IA"}


def _humanize(raw: str) -> str:
    """Fallback legible para una clave verdaderamente desconocida (no debería
    ocurrir si la taxonomía está completa)."""
    token = _norm(raw)
    if not token:
        return ""
    words = [w for w in token.split("_") if w and w != "api"]
    if not words:
        return ""
    words = words[-3:]
    return " ".join(_ACRONYMS.get(w, w.capitalize()) for w in words)


def canonical_key(raw: Optional[str]) -> str:
    """Clave cruda/alias/etiqueta → clave canónica. Si no se reconoce, devuelve
    la propia clave normalizada (para que `label_for` la humanice)."""
    token = _norm(raw)
    if not token:
        return "home"
    return ALIAS_TO_KEY.get(token, token)


def label_for(raw: Optional[str]) -> str:
    """Clave cruda/alias/etiqueta → etiqueta legible canónica."""
    token = _norm(raw)
    if not token:
        return "Inicio"
    key = ALIAS_TO_KEY.get(token)
    if key:
        return SECTION_BY_KEY[key]["label"]
    return _humanize(raw)


def module_for(raw: Optional[str]) -> Optional[str]:
    """Clave cruda/alias/etiqueta → key del módulo padre (de nav.py) o None."""
    key = ALIAS_TO_KEY.get(_norm(raw))
    if not key:
        return None
    return SECTION_BY_KEY[key]["module"]


def room_for(raw: Optional[str]) -> Optional[str]:
    """Sala 3D reservada (Fase 4). Hoy NO la consume nadie."""
    key = ALIAS_TO_KEY.get(_norm(raw))
    if not key:
        return None
    return SECTION_BY_KEY[key]["room"]


def _clean_path(path: Optional[str]) -> str:
    clean = (path or "/").split("?", 1)[0].split("#", 1)[0].strip().lower()
    if not clean.startswith("/"):
        clean = "/" + clean
    return clean.rstrip("/") or "/"


def detect_key(path: Optional[str]) -> str:
    """URL → clave canónica, evaluando DETECT_RULES en orden (primera gana)."""
    p = _clean_path(path)
    for op, pattern, key in DETECT_RULES:
        if op == "eq":
            if p == pattern:
                return key
        elif op == "prefix":
            if p.startswith(pattern):
                return key
        else:  # contains
            if pattern in p:
                return key
    # Fallback: clave derivada del path (humanizable). Marca "otros" si vacío.
    token = _norm(p.strip("/"))
    return token or "otros"


def to_dict() -> dict:
    """Serializa la taxonomía para el front (un solo origen, generado)."""
    return {
        "rules": [[op, pattern, key] for (op, pattern, key) in DETECT_RULES],
        "labels": {s["key"]: s["label"] for s in SECTIONS},
        "modules": {s["key"]: s["module"] for s in SECTIONS},
        "rooms": {s["key"]: s["room"] for s in SECTIONS},
        "aliases": dict(ALIAS_TO_KEY),
        "moduleLabels": MODULE_LABELS,
    }


def to_json() -> str:
    return json.dumps(to_dict(), ensure_ascii=False)


# JS generado a partir de la MISMA taxonomía. Se sirve por un endpoint e se
# incluye en base.html / base_sic.html, de modo que los detectores del front
# (_detectTrackingSection, trackEvent, blockForSection del 3D) consumen esta
# única fuente en vez de mantener copias propias. `__DATA__` se reemplaza por
# el JSON de to_dict().
_JS_TEMPLATE = r"""/* GENERADO por tracking_taxonomy.to_js() — NO editar a mano. Fuente única. */
window.SectionTaxonomy = (function () {
  var T = __DATA__;
  function norm(raw) {
    var s = (raw == null ? "" : String(raw)).trim().toLowerCase();
    if (!s) return "";
    s = s.split("?")[0].split("#")[0];
    s = s.replace(/[—–]/g, " ");
    s = s.replace(/[\s\-\/.]+/g, "_").replace(/_+/g, "_").replace(/^_|_$/g, "");
    return s;
  }
  function cleanPath(p) {
    var c = (p || "/").split("?")[0].split("#")[0].trim().toLowerCase();
    if (c.charAt(0) !== "/") c = "/" + c;
    c = c.replace(/\/+$/, "");
    return c || "/";
  }
  function detectKey(path) {
    var p = cleanPath(path);
    for (var i = 0; i < T.rules.length; i++) {
      var op = T.rules[i][0], pat = T.rules[i][1], key = T.rules[i][2];
      if (op === "eq") { if (p === pat) return key; }
      else if (op === "prefix") { if (p.indexOf(pat) === 0) return key; }
      else { if (p.indexOf(pat) !== -1) return key; }
    }
    var token = norm(p.replace(/^\/+|\/+$/g, ""));
    return token || "otros";
  }
  function canonicalKey(raw) {
    var t = norm(raw);
    if (!t) return "home";
    return T.aliases[t] || t;
  }
  function labelFor(raw) {
    var t = norm(raw);
    if (!t) return "Inicio";
    var k = T.aliases[t];
    return k ? T.labels[k] : "";
  }
  function moduleFor(raw) {
    var k = T.aliases[norm(raw)];
    return k ? (T.modules[k] || null) : null;
  }
  return {
    data: T, norm: norm, detectKey: detectKey,
    canonicalKey: canonicalKey, labelFor: labelFor, moduleFor: moduleFor
  };
})();
"""


def to_js() -> str:
    """Módulo JS autocontenido (window.SectionTaxonomy) con data + helpers."""
    return _JS_TEMPLATE.replace("__DATA__", to_json())
