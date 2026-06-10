# web_comparativas/nav.py
"""
Estructura declarativa del menú/navegación de la app (Suite SIEM).

ÚNICA FUENTE DE VERDAD para:
  (a) el formulario de permisos por usuario (árbol de checkboxes),
  (b) el filtrado del sidebar (qué ítems ve cada usuario),
  (c) el bloqueo de rutas (policy.require_perm / can_access).

Cada ítem del MENU es un dict con:
  key      : string jerárquico con puntos. Las HOJAS (items sin hijos) son las
             unidades de permiso que se guardan en User.module_access.
             "forecast" es una hoja (no tiene hijos).
  label    : etiqueta visible exacta (como en el sidebar real).
  url      : ruta destino. None para ramas puramente colapsables sin destino propio.
  module   : key top-level a la que pertenece (sic/forecast/indicadores_comerciales/
             mercado_publico/mercado_privado).
  parent   : key del padre (None para top-level).
  roles    : roles —ADEMÁS de admin y gerente, que son universales (ver policy)—
             que pueden ver/acceder el ítem. Se toman de los condicionales REALES
             del sidebar (base.html / sic/base_sic.html). None = sin restricción
             extra (cualquier rol que llegue al contexto).
  market   : market_context del sidebar donde se renderiza
             ('public' | 'private' | 'forecast' | 'indicadores' | 'sic').

Notas de derivación (valores tomados del código real del sidebar):
  - Ítems sin `{% if r in [...] %}` en el sidebar → roles=None (todos los roles).
  - admin y gerente/manager se tratan como acceso universal en policy.role_allows,
    por eso NO hace falta listarlos en `roles` (el sidebar tampoco lista a gerente,
    y a admin lo lista solo en algunos; la universalidad evita regresiones).
  - forecast e indicadores_comerciales: el sidebar y las tarjetas de la suite los
    muestran sin guard → roles=None (todos).
"""
from __future__ import annotations

# Vocabulario de roles (normalizados a minúscula). admin/gerente son universales.
ADMIN_ROLE_KEYS   = {"admin", "administrator", "administrador"}
MANAGER_ROLE_KEYS = {"gerente", "manager"}
ALL_ROLES = {
    "admin", "administrator", "administrador",
    "gerente", "manager",
    "supervisor",
    "analista", "analyst",
    "auditor", "visor", "viewer",
}

# ──────────────────────────────────────────────────────────────────────────────
# MENU declarativo (orden = orden de aparición en el sidebar)
# ──────────────────────────────────────────────────────────────────────────────
MENU: list[dict] = [
    # ===================== S.I.C =====================
    {"key": "sic", "label": "Soporte de Inteligencia Comercial", "url": "/sic",
     "module": "sic", "parent": None, "market": "sic",
     "roles": {"admin", "auditor", "supervisor", "analista"}, "icon": "bi-shield-check"},
    {"key": "sic.mesa_ayuda", "label": "Mesa de ayuda", "url": "/sic/helpdesk",
     "module": "sic", "parent": "sic", "market": "sic",
     "roles": {"admin", "auditor", "supervisor", "analista"}},
    {"key": "sic.seguimiento", "label": "Seguimiento", "url": "/sic/tracking",
     "module": "sic", "parent": "sic", "market": "sic",
     "roles": {"admin", "auditor", "supervisor", "analista"}},
    {"key": "sic.usuarios", "label": "Usuarios", "url": "/sic/users",
     "module": "sic", "parent": "sic", "market": "sic",
     "roles": {"admin", "auditor", "supervisor", "analista"}},
    {"key": "sic.contrasenas", "label": "Contraseñas", "url": "/sic/password-resets",
     "module": "sic", "parent": "sic", "market": "sic",
     "roles": {"admin"}},

    # ===================== FORECAST (hoja top-level) =====================
    {"key": "forecast", "label": "Forecast", "url": "/forecast/",
     "module": "forecast", "parent": None, "market": "forecast",
     "roles": None, "icon": "bi-graph-up-arrow"},

    # ===================== INDICADORES COMERCIALES =====================
    {"key": "indicadores_comerciales", "label": "Indicadores Comerciales",
     "url": "/indicadores-comerciales/", "module": "indicadores_comerciales",
     "parent": None, "market": "indicadores", "roles": None, "icon": "bi-bar-chart-line"},
    {"key": "indicadores_comerciales.rentabilidad_negativa", "label": "Rentabilidad Negativa",
     "url": "/indicadores-comerciales/rentabilidad-negativa", "module": "indicadores_comerciales",
     "parent": "indicadores_comerciales", "market": "indicadores", "roles": None,
     "icon": "bi-graph-down-arrow"},
    {"key": "indicadores_comerciales.inflacion", "label": "Inflación",
     "url": "/indicadores-comerciales/inflacion", "module": "indicadores_comerciales",
     "parent": "indicadores_comerciales", "market": "indicadores", "roles": None,
     "icon": "bi-arrow-up-right-circle"},
    {"key": "indicadores_comerciales.informes_laboratorio", "label": "Informes de Laboratorio",
     "url": "/indicadores-comerciales/informes-laboratorio", "module": "indicadores_comerciales",
     "parent": "indicadores_comerciales", "market": "indicadores", "roles": None,
     "icon": "bi-capsule"},

    # ===================== MERCADO PÚBLICO =====================
    {"key": "mercado_publico", "label": "Mercado Público", "url": "/mercado-publico/web-comparativas",
     "module": "mercado_publico", "parent": None, "market": "public", "roles": None,
     "icon": "bi-building"},
    {"key": "mercado_publico.home", "label": "Home", "url": "/mercado-publico",
     "module": "mercado_publico", "parent": "mercado_publico", "market": "public",
     "roles": None, "icon": "bi-house-door"},

    # --- Comparativa (rama colapsable #wcMenu) ---
    {"key": "mercado_publico.comparativa", "label": "Comparativa", "url": None,
     "module": "mercado_publico", "parent": "mercado_publico", "market": "public",
     "roles": None, "icon": "bi-intersect", "submenu_id": "wcMenu"},
    {"key": "mercado_publico.comparativa.nueva_carga", "label": "Nueva carga", "url": "/cargas/nueva",
     "module": "mercado_publico", "parent": "mercado_publico.comparativa", "market": "public",
     "roles": {"admin", "analista", "supervisor"}},
    {"key": "mercado_publico.comparativa.nueva_carga_otras", "label": "Nueva carga",
     "url": "/otras-fuentes-externas", "module": "mercado_publico",
     "parent": "mercado_publico.comparativa", "market": "public", "roles": None,
     "section_title": "Otras Fuentes"},
    {"key": "mercado_publico.comparativa.historial", "label": "Historial", "url": "/cargas/historial",
     "module": "mercado_publico", "parent": "mercado_publico.comparativa", "market": "public",
     "roles": {"admin", "analista", "supervisor", "auditor"}, "badge": "cargas_counts"},
    {"key": "mercado_publico.comparativa.grupos", "label": "Grupos", "url": "/grupos",
     "module": "mercado_publico", "parent": "mercado_publico.comparativa", "market": "public",
     "roles": {"admin", "supervisor"}},

    # --- Lectura de Pliegos (hoja) ---
    {"key": "mercado_publico.lectura_pliegos", "label": "Lectura de Pliegos",
     "url": "/mercado-publico/lectura-pliegos", "module": "mercado_publico",
     "parent": "mercado_publico", "market": "public",
     "roles": {"admin", "analista", "supervisor", "auditor", "visor"},
     "icon": "bi-file-earmark-text"},

    # --- Oportunidades (rama colapsable #opMenu) ---
    {"key": "mercado_publico.oportunidades", "label": "Oportunidades", "url": None,
     "module": "mercado_publico", "parent": "mercado_publico", "market": "public",
     "roles": {"admin", "analista", "supervisor", "auditor"}, "icon": "bi-lightbulb",
     "submenu_id": "opMenu"},
    {"key": "mercado_publico.oportunidades.buscador", "label": "Buscador", "url": "/oportunidades/buscador",
     "module": "mercado_publico", "parent": "mercado_publico.oportunidades", "market": "public",
     "roles": {"admin", "analista", "supervisor", "auditor"}},
    {"key": "mercado_publico.oportunidades.dimensiones", "label": "Dimensiones",
     "url": "/oportunidades/dimensiones", "module": "mercado_publico",
     "parent": "mercado_publico.oportunidades", "market": "public",
     "roles": {"admin", "analista", "supervisor", "auditor"}},

    # --- Reporte perfiles (hoja) ---
    {"key": "mercado_publico.reporte_perfiles", "label": "Reporte perfiles",
     "url": "/mercado-publico/reporte-perfiles", "module": "mercado_publico",
     "parent": "mercado_publico", "market": "public",
     "roles": {"admin", "supervisor", "auditor", "analista", "analyst"},
     "icon": "bi-file-earmark-bar-graph", "pill": "Nuevo"},

    # --- Mesa de ayuda (hoja). Admin ve la variante "Mesa de ayuda (SIC)" aparte. ---
    {"key": "mercado_publico.mesa_ayuda", "label": "Mesa de ayuda", "url": "/mercado-publico/helpdesk",
     "module": "mercado_publico", "parent": "mercado_publico", "market": "public",
     "roles": {"analista", "supervisor", "auditor"}, "icon": "bi-headset",
     "hide_for_admin_in_sidebar": True},

    # ===================== MERCADO PRIVADO =====================
    {"key": "mercado_privado", "label": "Mercado Privado", "url": "/mercado-privado",
     "module": "mercado_privado", "parent": None, "market": "private", "roles": None,
     "icon": "bi-briefcase"},
    {"key": "mercado_privado.home", "label": "Home", "url": "/mercado-privado",
     "module": "mercado_privado", "parent": "mercado_privado", "market": "private",
     "roles": None, "icon": "bi-house-door"},
    {"key": "mercado_privado.dimensionamiento", "label": "Dimensionamiento",
     "url": "/mercado-privado/dimensiones", "module": "mercado_privado",
     "parent": "mercado_privado", "market": "private", "roles": None, "icon": "bi-pie-chart"},
    {"key": "mercado_privado.reporte_perfiles", "label": "Reporte perfiles",
     "url": "/mercado-privado/reporte-perfiles", "module": "mercado_privado",
     "parent": "mercado_privado", "market": "private",
     "roles": {"admin", "supervisor", "auditor"}, "icon": "bi-file-earmark-bar-graph",
     "pill": "Nuevo"},
    {"key": "mercado_privado.mesa_ayuda", "label": "Mesa de ayuda", "url": "/mercado-privado/helpdesk",
     "module": "mercado_privado", "parent": "mercado_privado", "market": "private",
     "roles": {"analista", "supervisor", "auditor"}, "icon": "bi-headset"},
]

# ──────────────────────────────────────────────────────────────────────────────
# Índices y helpers estructurales
# ──────────────────────────────────────────────────────────────────────────────
MENU_BY_KEY: dict[str, dict] = {it["key"]: it for it in MENU}

# Top-levels (módulos del catálogo gobernados por permisos)
TOP_LEVEL_KEYS: list[str] = [it["key"] for it in MENU if it["parent"] is None]

# Etiqueta de cada top-level (para el formulario)
MODULES: dict[str, str] = {it["key"]: it["label"] for it in MENU if it["parent"] is None}


def children_of(key: str | None) -> list[dict]:
    """Ítems cuyo parent == key (en orden de definición)."""
    return [it for it in MENU if it["parent"] == key]


def is_leaf(key: str) -> bool:
    """True si la key no tiene hijos (es unidad de permiso). 'forecast' es hoja."""
    if key not in MENU_BY_KEY:
        return False
    return not any(it["parent"] == key for it in MENU)


def leaf_keys() -> list[str]:
    """Todas las keys de hoja del MENU (las que se guardan en module_access)."""
    return [it["key"] for it in MENU if is_leaf(it["key"])]


def descendant_leaves(key: str) -> list[str]:
    """Hojas descendientes de `key` (incluye `key` si ya es hoja)."""
    if is_leaf(key):
        return [key]
    out: list[str] = []
    for child in children_of(key):
        out.extend(descendant_leaves(child["key"]))
    return out


def ancestors_of(key: str) -> list[str]:
    """Cadena de ancestros de `key`, de padre inmediato hacia la raíz."""
    out: list[str] = []
    node = MENU_BY_KEY.get(key)
    while node is not None and node["parent"] is not None:
        out.append(node["parent"])
        node = MENU_BY_KEY.get(node["parent"])
    return out


def menu_tree() -> list[dict]:
    """
    Árbol anidado de TODO el MENU (top-levels → secciones → sub-secciones), con
    `children` e `is_leaf` calculados. Útil para construir el formulario de permisos.
    """
    def build(key: str) -> dict:
        node = dict(MENU_BY_KEY[key])
        kids = [build(c["key"]) for c in children_of(key)]
        node["children"] = kids
        node["is_leaf"] = len(kids) == 0
        return node

    return [build(k) for k in TOP_LEVEL_KEYS]
