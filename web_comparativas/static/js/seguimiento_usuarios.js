/**
 * S.I.C — Seguimiento de Usuarios v2027a
 * Módulo completo de monitoreo enterprise con datos mock y arquitectura
 * preparada para reemplazo por API real sin cambios estructurales.
 */
(function () {
  'use strict';

  /* ═══════════════════════════════════════════════════════
     DICCIONARIO DE SECCIONES (nombres técnicos → legibles)
     Agregar nuevas entradas aquí para mantener consistencia
     en toda la UI. Exportable como módulo si se requiere.
  ═══════════════════════════════════════════════════════ */
  const SECTION_MAP = {
    // S.I.C. Module
    'sic':                          'Panel S.I.C.',
    'sic_home':                     'Panel S.I.C.',
    'sic_tracking':                 'Seguimiento de Usuarios',
    'sic_helpdesk':                 'Mesa de Ayuda',
    'sic_usuarios':                 'Gestión de Usuarios',
    'sic_password_resets':          'Gestión de Contraseñas',

    // Main Dashboard
    'dashboard':                    'Panel Principal',
    'markets':                      'Centro de Mercados',
    'markets_home':                 'Inicio — Mercados',

    // Mercado Público
    'mercado_publico':              'Mercado Público',
    'mercado_publico_home':         'Inicio — Mercado Público',
    'mercado_publico_helpdesk':     'Helpdesk — Mercado Público',
    'mercado_publico_oportunidades':'Oportunidades — Público',
    'mercado_publico_dimensiones':  'Dimensionamiento — Público',
    'mercado_publico_buscador':     'Buscador — Público',

    // Mercado Privado
    'mercado_privado':              'Mercado Privado',
    'mercado_privado_home':         'Inicio — Mercado Privado',
    'mercado_privado_dimensiones':  'Dimensionamiento',
    'mercado_privado_buscador':     'Buscador — Privado',

    // Oportunidades
    'oportunidades':                'Oportunidades',
    'oportunidades_buscador':       'Buscador de Oportunidades',
    'oportunidades_dimensiones':    'Análisis de Dimensiones',

    // Cargas / Historial
    'cargas':                       'Cargas',
    'cargas_historial':             'Cargas — Historial',

    // Pliegos
    'pliegos':                      'Módulo de Pliegos',
    'pliego_widget':                'Visor de Pliegos',
    'pliego_detalle':               'Detalle de Pliego',

    // Forecast
    'forecast':                     'Proyecciones y Forecast',
    'forecast_widget':              'Panel de Forecast',

    // Perfiles
    'reporte_perfiles':                     'Reporte de Perfiles',
    'mercado_publico_reporte_perfiles':     'Reporte de Perfiles — Mercado Público',
    'mercado_privado_reporte_perfiles':     'Reporte de Perfiles — Mercado Privado',
    'perfiles':                             'Perfiles de Clientes',

    // Comparativas
    'comparativa':                  'Comparativa de Mercado',
    'web_comparativas':             'Comparativa de Mercado',
    'comparativa_home':             'Inicio — Comparativa',

    // Auth
    'login':                        'Inicio de Sesión',
    'logout':                       'Cierre de Sesión',
    'auth':                         'Inicio de Sesión',

    // Alias tecnico antiguo; no se muestra como apartado
    'otro':                         '',

    // Inicio / vacío
    'home':                         'Inicio',
    '/':                            'Inicio',
    '':                             'Inicio',
  };

  Object.assign(SECTION_MAP, {
    sic: 'S.I.C General',
    sic_general: 'S.I.C General',
    sic_tracking_api: 'API Tracking Interno',
    sic_users: 'Usuarios',
    sic_usuarios: 'Usuarios',
    admin_password_resets: 'Administracion de Reseteos',
    administracion: 'Administracion',
    grupos: 'Grupos',
    notificaciones: 'Notificaciones',
    mercado_publico: 'Mercado Publico Home',
    mercado_publico_home: 'Mercado Publico Home',
    mercado_publico_helpdesk: 'Mesa de Ayuda Mercado Publico',
    mercado_publico_oportunidades: 'Oportunidades',
    mercado_publico_dimensiones: 'Dimensionamiento Mercado Publico',
    mercado_privado_home: 'Mercado Privado Home',
    mercado_privado_helpdesk: 'Mesa de Ayuda Mercado Privado',
    mercado_privado_dimensiones: 'Dimensionamiento',
    dimensionamiento: 'Dimensionamiento',
    oportunidades_dimensiones: 'Dimensiones de Oportunidades',
    lectura_pliegos: 'Lectura de Pliegos',
    pliegos: 'Lectura de Pliegos',
    comparativa_mercado: 'Comparativa de Mercado',
    mercado_publico_reporte_perfiles: 'Reporte de Perfiles — Mercado Público',
    mercado_privado_reporte_perfiles: 'Reporte de Perfiles — Mercado Privado',
    tablero_comparativa: 'Tablero de Comparativa',
    vistas_guardadas: 'Vistas Guardadas',
    cargas_nueva: 'Nueva Carga',
    cargas_edicion: 'Edicion de Carga',
    fuentes_externas: 'Fuentes Externas',
    descargas: 'Descargas',
    reporte_proceso: 'Reporte de Proceso',
    informes: 'Informes',
    auth_login: 'Inicio de Sesion',
    auth_logout: 'Cierre de Sesion',
    auth_password: 'Gestion de Contrasena',
    comentarios: 'Comentarios',
    clientes_api: 'Consulta de Clientes',
    sic_helpdesk_tickets: 'Mesa de Ayuda / Tickets',
    mercado_publico_analisis_dimensiones: 'Analisis de Dimensiones',
    mercado_publico_fuentes_externas: 'Fuentes Externas',
    otro: '',
    otros: '',
    sin_identificar: '',
    sin_clasificar: '',
    unknown: '',
    undefined: '',
    'n/a': '',
  });

  function normalizeSectionKey(raw) {
    return String(raw || '')
      .toLowerCase()
      .trim()
      .split('?')[0]
      .split('#')[0]
      .replace(/^\/+|\/+$/g, '')
      .replace(/-/g, '_')
      .replace(/\//g, '_')
      .replace(/_+/g, '_');
  }

  function isGenericSectionLabel(key) {
    return ['otro', 'otros', 'sin_identificar', 'sin_clasificar', 'sin identificar',
      'sin clasificar', 'unknown', 'undefined', 'n/a', 'no identificado'].includes(key);
  }

  /* Normaliza etiqueta legible para lookup robusto (sin acentos, sin puntos, em-dash→espacio) */
  function normalizeLabelKey(s) {
    return _noAccent(String(s || '').toLowerCase().trim())
      .replace(/\s*[—–]\s*/g, ' ')
      .replace(/\./g, '')
      .replace(/\s+/g, ' ')
      .trim();
  }

  /* Secciones técnicas (auth, API): se silencian sin warning visual */
  const KNOWN_TECHNICAL_LABELS = new Set([
    'inicio de sesion', 'cierre de sesion',
    'api tracking interno', 'api tracking (interno)',
    'inicio de sesion siem',
  ]);

  /* Etiquetas enviadas por el servidor → label display limpia.
   * Claves: resultado de normalizeLabelKey(label_del_servidor). */
  const LABEL_SECTION_MAP = {
    'mercado publico inicio':     'Mercado Público — Inicio',
    'mercado publico home':       'Mercado Público — Inicio',
    'inicio  mercado publico':    'Mercado Público — Inicio',
    'mercado publico':            'Mercado Público',
    'mercado privado inicio':     'Mercado Privado — Inicio',
    'mercado privado home':       'Mercado Privado — Inicio',
    'mercado privado':            'Mercado Privado',
    'sic':                        'S.I.C.',
    'sic general':                'S.I.C.',
    'panel sic':                  'Panel S.I.C.',
    'panel sic':                  'Panel S.I.C.',
    'inicio':                     'Inicio',
    'tablero':                    'Tablero de Comparativa',
    'tablero de comparativa':     'Tablero de Comparativa',
    'seguimiento de usuarios':    'Seguimiento de Usuarios',
    'inicio de sesion':           'Inicio de Sesión',
    'cierre de sesion':           'Cierre de Sesión',
    'api tracking interno':       'API Tracking Interno',
    'api tracking (interno)':     'API Tracking Interno',
  };

  function humanizeSectionKey(raw) {
    const key = normalizeSectionKey(raw)
      .replace(/\b\d+\b/g, '')
      .replace(/[0-9a-f]{8,}(_[0-9a-f]{4,})*/gi, '');
    const words = key.split('_').filter(w => w && w !== 'api');
    if (!words.length) return '';
    const phrases = {
      analisis_dimensiones: 'Analisis de Dimensiones',
      fuentes_externas: 'Fuentes Externas',
      helpdesk_tickets: 'Mesa de Ayuda / Tickets',
      users: 'Usuarios',
      usuarios: 'Usuarios',
      password_resets: 'Reseteo de Contrasenas',
      reporte_perfiles: 'Reporte de Perfiles',
      cargas_historial: 'Cargas Historial',
      oportunidades_buscador: 'Buscador de Oportunidades',
    };
    const joined = words.join('_');
    if (phrases[joined]) return phrases[joined];
    for (let size = Math.min(3, words.length); size > 0; size -= 1) {
      const suffix = words.slice(-size).join('_');
      if (phrases[suffix]) return phrases[suffix];
    }
    const acronyms = { sic: 'S.I.C', siem: 'SIEM', ia: 'IA' };
    const displayWords = words.length > 2 ? words.slice(-2) : words;
    return displayWords.map(w => acronyms[w] || (w.charAt(0).toUpperCase() + w.slice(1))).join(' ');
  }

  /** Resuelve un nombre técnico o etiqueta de servidor a su etiqueta legible. */
  function sectionLabel(raw) {
    if (!raw) return 'Inicio';
    // 1. Clave técnica normalizada (underscores, minúsculas)
    const key = normalizeSectionKey(raw);
    if (isGenericSectionLabel(key)) return '';
    if (SECTION_MAP[key]) return SECTION_MAP[key];
    // 2. Coincidencia parcial con clave técnica
    for (const [k, v] of Object.entries(SECTION_MAP)) {
      if (k && key.includes(k) && k.length > 3) return v;
    }
    // 3. Etiqueta legible normalizada (sin acentos, sin puntos, em-dash→espacio)
    const lk = normalizeLabelKey(raw);
    if (LABEL_SECTION_MAP[lk] !== undefined) return LABEL_SECTION_MAP[lk];
    // 4. Eventos técnicos conocidos — silenciar sin warning
    if (KNOWN_TECHNICAL_LABELS.has(lk)) return raw;
    const label = humanizeSectionKey(raw);
    if (label && window.console && console.warn) {
      console.warn('[tracking] ruta sin mapping explicito:', raw, '->', label);
    }
    return label;
  }

  /* ═══════════════════════════════════════════════════════
     UTILIDADES
  ═══════════════════════════════════════════════════════ */
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const esc = v => String(v ?? '').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;').replace(/"/g,'&quot;');

  const fmtInt = v => Math.round(Number(v) || 0).toLocaleString('es-AR');
  const fmtDec = (v, d = 1) => (Number(v) || 0).toLocaleString('es-AR', { minimumFractionDigits: d, maximumFractionDigits: d });
  const fmtDate = d => new Date(d).toLocaleDateString('es-AR', { day:'2-digit', month:'2-digit', year:'numeric' });
  const fmtTime = d => new Date(d).toLocaleTimeString('es-AR', { hour:'2-digit', minute:'2-digit' });
  const fmtDT   = d => new Date(d).toLocaleString('es-AR', { day:'2-digit', month:'2-digit', year:'numeric', hour:'2-digit', minute:'2-digit' });

  function fmtRelative(dateStr) {
    const diff = Date.now() - new Date(dateStr).getTime();
    if (diff < 0) return 'recién';
    const min = Math.round(diff / 60000);
    if (min < 1) return '< 1 min';
    if (min < 60) return `${min} min`;
    const h = Math.round(min / 60);
    if (h < 24) return `${h} h`;
    return `${Math.round(h / 24)} d`;
  }

  function fmtHours(h) {
    const n = Number(h) || 0;
    if (n < 1) return `${Math.max(1, Math.round(n * 60))} min`;
    return `${fmtDec(n, 1)} h`;
  }

  function initials(name) {
    if (!name) return '?';
    const parts = String(name).trim().split(/\s+/);
    return (parts[0]?.[0] ?? '') + (parts[1]?.[0] ?? parts[0]?.[1] ?? '');
  }

  function scoreInfo(score) {
    const n = Math.max(0, Math.min(100, Math.round(Number(score) || 0)));
    if (n >= 80) return { color: '#10b981', label: 'Referente',     gradient: 'linear-gradient(90deg,#059669,#10b981)' };
    if (n >= 50) return { color: '#3b82f6', label: 'Consolidado',   gradient: 'linear-gradient(90deg,#2563eb,#3b82f6)' };
    if (n >= 30) return { color: '#f59e0b', label: 'En progreso',   gradient: 'linear-gradient(90deg,#d97706,#f59e0b)' };
    return              { color: '#ef4444', label: 'Baja adopción', gradient: 'linear-gradient(90deg,#dc2626,#ef4444)' };
  }

  function avatarColor(name) {
    const colors = [
      'linear-gradient(135deg,#3b82f6,#8b5cf6)',
      'linear-gradient(135deg,#10b981,#3b82f6)',
      'linear-gradient(135deg,#f59e0b,#ef4444)',
      'linear-gradient(135deg,#8b5cf6,#ec4899)',
      'linear-gradient(135deg,#06b6d4,#3b82f6)',
      'linear-gradient(135deg,#10b981,#059669)',
    ];
    let h = 0;
    for (let i = 0; i < (name || '').length; i++) h = (h * 31 + name.charCodeAt(i)) & 0xffffffff;
    return colors[Math.abs(h) % colors.length];
  }

  /* ═══════════════════════════════════════════════════════
     MOCK DATA — reemplazar con llamadas API sin cambios en UI
  ═══════════════════════════════════════════════════════ */
  const NOW = new Date();
  const dAgo = (days, h = 0, m = 0) => new Date(NOW.getTime() - (days * 86400 + h * 3600 + m * 60) * 1000);
  const mAgo = mins => dAgo(0, 0, mins);

  const MOCK_USERS = [
    {
      id: 1, username: 'María González', email: 'mgonzalez@suizoarg.com',
      role: 'analista', unit: 'Comercial Pampeana', group: 'Equipo A',
      created: '2024-03-10',
      score: 87, sessions: 42, active_days: 18, active_hours: 76.4,
      views: 312, searches: 89, downloads: 34, uploads: 21, exports: 15,
      modules: ['mercado_publico', 'oportunidades', 'pliegos'],
      frequency: 'Diaria', last_access: mAgo(8),
      risk: 'bajo', status: 'active',
      current_section: 'oportunidades_buscador',
      session_start: mAgo(62),
      last_ping: mAgo(8),
      last_action: 'Búsqueda avanzada',
      activity_type: 'buscando',
      nav_trail: ['mercado_publico', 'oportunidades', 'oportunidades_buscador'],
      sessions_detail: [
        { date: dAgo(0,1), duration: '1h 20min', sections: ['dashboard','mercado_publico','oportunidades_buscador','pliegos'] },
        { date: dAgo(1),   duration: '2h 05min', sections: ['dashboard','oportunidades','oportunidades_dimensiones','reporte_perfiles'] },
        { date: dAgo(2),   duration: '45min',    sections: ['mercado_publico','pliego_detalle'] },
        { date: dAgo(3),   duration: '1h 50min', sections: ['dashboard','mercado_privado','oportunidades'] },
      ],
      timeline: [
        { time: mAgo(8),  action: 'view',   section: 'oportunidades_buscador' },
        { time: mAgo(14), action: 'search', section: 'oportunidades_buscador' },
        { time: mAgo(22), action: 'view',   section: 'oportunidades' },
        { time: mAgo(38), action: 'view',   section: 'pliegos' },
        { time: mAgo(48), action: 'upload', section: 'mercado_publico' },
        { time: mAgo(61), action: 'view',   section: 'dashboard' },
      ],
      score_history: [62,65,70,74,78,82,85,87],
    },
    {
      id: 2, username: 'Carlos Méndez', email: 'cmendez@suizoarg.com',
      role: 'supervisor', unit: 'Dirección Regional', group: 'Gerencia',
      created: '2023-11-20',
      score: 72, sessions: 28, active_days: 15, active_hours: 48.2,
      views: 198, searches: 52, downloads: 18, uploads: 9, exports: 22,
      modules: ['mercado_publico', 'mercado_privado', 'forecast'],
      frequency: 'Diaria', last_access: mAgo(3),
      risk: 'bajo', status: 'idle',
      current_section: 'dimensionamiento',
      session_start: dAgo(0, 2, 10),
      last_ping: mAgo(18),
      last_action: 'Exportó reporte',
      activity_type: 'exportando',
      nav_trail: ['dashboard','mercado_privado','dimensionamiento'],
      sessions_detail: [
        { date: dAgo(0,2), duration: '2h 10min', sections: ['dashboard','mercado_privado','dimensionamiento','forecast'] },
        { date: dAgo(1),   duration: '1h 30min', sections: ['mercado_publico','forecast_widget'] },
        { date: dAgo(3),   duration: '1h 05min', sections: ['dashboard','mercado_privado'] },
      ],
      timeline: [
        { time: mAgo(3),  action: 'export', section: 'mercado_privado_dimensiones' },
        { time: mAgo(18), action: 'view',   section: 'mercado_privado_dimensiones' },
        { time: mAgo(45), action: 'view',   section: 'mercado_privado' },
        { time: mAgo(72), action: 'view',   section: 'forecast' },
        { time: mAgo(90), action: 'view',   section: 'dashboard' },
      ],
      score_history: [55,58,60,64,66,70,71,72],
    },
    {
      id: 3, username: 'Laura Ríos', email: 'lrios@suizoarg.com',
      role: 'analista', unit: 'Comercial NOA', group: 'Equipo B',
      created: '2024-06-01',
      score: 45, sessions: 15, active_days: 8, active_hours: 22.5,
      views: 87, searches: 23, downloads: 5, uploads: 4, exports: 3,
      modules: ['mercado_publico', 'oportunidades'],
      frequency: 'Semanal', last_access: mAgo(2),
      risk: 'medio', status: 'active',
      current_section: 'mercado_publico',
      session_start: mAgo(25),
      last_ping: mAgo(2),
      last_action: 'Abrió sección',
      activity_type: 'navegando',
      nav_trail: ['dashboard','mercado_publico'],
      sessions_detail: [
        { date: dAgo(0,0,25), duration: '25min',    sections: ['dashboard','mercado_publico'] },
        { date: dAgo(1),      duration: '1h 10min', sections: ['mercado_publico','oportunidades'] },
        { date: dAgo(4),      duration: '40min',    sections: ['dashboard','oportunidades_buscador'] },
      ],
      timeline: [
        { time: mAgo(2),  action: 'view',   section: 'mercado_publico' },
        { time: mAgo(10), action: 'view',   section: 'dashboard' },
        { time: mAgo(25), action: 'search', section: 'oportunidades_buscador' },
      ],
      score_history: [30,33,36,38,40,42,44,45],
    },
    {
      id: 4, username: 'Diego Paredes', email: 'dparedes@suizoarg.com',
      role: 'analista', unit: 'Comercial Cuyo', group: 'Equipo C',
      created: '2024-01-15',
      score: 21, sessions: 5, active_days: 2, active_hours: 5.8,
      views: 22, searches: 4, downloads: 1, uploads: 0, exports: 0,
      modules: ['mercado_publico'],
      frequency: 'Esporádica', last_access: dAgo(9),
      risk: 'alto', status: 'offline',
      current_section: null,
      session_start: null, last_ping: dAgo(9),
      last_action: 'Visitó Panel Principal',
      activity_type: null,
      nav_trail: [],
      sessions_detail: [
        { date: dAgo(9),  duration: '35min', sections: ['dashboard','mercado_publico'] },
        { date: dAgo(14), duration: '20min', sections: ['dashboard'] },
      ],
      timeline: [
        { time: dAgo(9), action: 'view', section: 'mercado_publico' },
        { time: dAgo(9,0,20), action: 'view', section: 'dashboard' },
      ],
      score_history: [15,17,18,20,20,21,21,21],
    },
    {
      id: 5, username: 'Sofía Herrera', email: 'sherrera@suizoarg.com',
      role: 'auditor', unit: 'Auditoría Interna', group: 'Control',
      created: '2023-08-01',
      score: 58, sessions: 22, active_days: 10, active_hours: 35.0,
      views: 145, searches: 41, downloads: 28, uploads: 2, exports: 19,
      modules: ['sic_tracking', 'sic_users', 'mercado_publico_reporte_perfiles'],
      frequency: 'Semanal', last_access: mAgo(55),
      risk: 'bajo', status: 'active',
      current_section: 'mercado_publico_reporte_perfiles',
      session_start: mAgo(65),
      last_ping: mAgo(55),
      last_action: 'Descargó reporte',
      activity_type: 'exportando',
      nav_trail: ['sic_tracking', 'mercado_publico', 'mercado_publico_reporte_perfiles'],
      sessions_detail: [
        { date: dAgo(0,1,5), duration: '1h 05min', sections: ['sic_tracking','mercado_publico','mercado_publico_reporte_perfiles'] },
        { date: dAgo(2),     duration: '1h 40min', sections: ['sic_tracking','sic_users'] },
        { date: dAgo(5),     duration: '50min',    sections: ['mercado_publico','mercado_publico_reporte_perfiles'] },
      ],
      timeline: [
        { time: mAgo(55), action: 'download', section: 'mercado_publico_reporte_perfiles' },
        { time: mAgo(58), action: 'view',     section: 'mercado_publico_reporte_perfiles' },
        { time: mAgo(62), action: 'view',     section: 'sic_tracking' },
        { time: mAgo(64), action: 'view',     section: 'sic' },
      ],
      score_history: [40,44,47,50,52,55,57,58],
    },
    {
      id: 6, username: 'Martín Villalba', email: 'mvillalba@suizoarg.com',
      role: 'analista', unit: 'Comercial Patagonia', group: 'Equipo D',
      created: '2024-09-12',
      score: 93, sessions: 55, active_days: 22, active_hours: 98.5,
      views: 487, searches: 142, downloads: 62, uploads: 38, exports: 27,
      modules: ['oportunidades', 'mercado_privado', 'forecast', 'pliegos'],
      frequency: 'Diaria intensiva', last_access: mAgo(1),
      risk: 'bajo', status: 'active',
      current_section: 'oportunidades_dimensiones',
      session_start: mAgo(48),
      last_ping: mAgo(1),
      last_action: 'Filtro aplicado',
      activity_type: 'buscando',
      nav_trail: ['mercado_privado','oportunidades','oportunidades_dimensiones'],
      sessions_detail: [
        { date: dAgo(0,0,48), duration: '48min',    sections: ['mercado_privado','oportunidades','oportunidades_dimensiones'] },
        { date: dAgo(1),      duration: '3h 15min', sections: ['dashboard','mercado_privado','oportunidades','pliegos','forecast'] },
        { date: dAgo(2),      duration: '2h 40min', sections: ['oportunidades_dimensiones','reporte_perfiles','mercado_privado'] },
      ],
      timeline: [
        { time: mAgo(1),  action: 'search', section: 'oportunidades_dimensiones' },
        { time: mAgo(5),  action: 'view',   section: 'oportunidades_dimensiones' },
        { time: mAgo(20), action: 'upload', section: 'oportunidades' },
        { time: mAgo(35), action: 'view',   section: 'mercado_privado' },
        { time: mAgo(47), action: 'view',   section: 'dashboard' },
      ],
      score_history: [75,78,82,85,88,90,92,93],
    },
  ];

  /* ── Sustituir por datos reales del servidor cuando están disponibles ── */
  (function mergeRealUsers() {
    const raw = window.TRACKING_USERS_DB;
    if (!Array.isArray(raw) || !raw.length) return;

    function normalizeRealUser(r, idx) {
      /* Mapea la estructura del servidor a la forma esperada por la UI */
      const statusMap = { activo: 'active', active: 'active', inactivo: 'idle', inactive: 'idle' };
      const status = statusMap[(r.status || '').toLowerCase()] || 'offline';
      const score  = Math.max(0, Math.min(100, Math.round(Number(r.score ?? r.adoption_score ?? 50))));
      /* Genera score_history sintético desde el score actual */
      const base   = Math.max(0, score - 14);
      const hist   = Array.from({ length: 8 }, (_, i) => Math.min(100, base + Math.round(i * (score - base) / 7)));

      return {
        id:             r.id || (idx + 1),
        username:       r.username || r.name || (r.email || '').split('@')[0],
        email:          r.email || '',
        role:           r.role || r.role_raw || 'analista',
        unit:           r.unit || r.unit_business || 'Sin unidad',
        group:          r.group || 'Sin grupo',
        created:        (r.created || r.created_at || '').slice(0, 10),
        score,
        sessions:       Number(r.sessions || 0),
        active_days:    Number(r.active_days || 0),
        active_hours:   Number(r.active_hours || 0),
        views:          Number(r.views || 0),
        searches:       Number(r.searches || 0),
        downloads:      Number(r.downloads || 0),
        uploads:        Number(r.uploads || 0),
        exports:        Number(r.exports || 0),
        modules:        Array.isArray(r.modules) ? r.modules : [],
        frequency:      r.frequency || 'Ocasional',
        last_access:    r.last_access || r.last_seen || null,
        risk:           r.risk || r.risk_level || 'bajo',
        status,
        current_section: r.current_section || '',
        session_start:   r.session_start || null,
        last_ping:       r.last_ping || r.last_signal || r.last_access || null,
        last_action:     r.last_action || 'Sin actividad registrada',
        activity_type:   r.activity_type || null,
        nav_trail:       Array.isArray(r.nav_trail) ? r.nav_trail : [],
        sessions_detail: Array.isArray(r.sessions_detail) ? r.sessions_detail : [],
        timeline:        Array.isArray(r.timeline) ? r.timeline : [],
        score_history:   hist,
      };
    }

    MOCK_USERS.length = 0;
    raw.forEach((r, i) => MOCK_USERS.push(normalizeRealUser(r, i)));
  })();

  /* KPI sparkline history (last 7 data points) */
  const MOCK_KPI_HISTORY = {
    live:     [1,2,3,2,4,3,3],
    adoption: [60,63,65,68,70,72,75],
    inactive: [3,3,2,3,2,2,1],
    active:   [4,4,5,4,5,5,5],
    hours:    [42,48,51,55,58,62,68],
    prod:     [1.1,1.2,1.3,1.2,1.4,1.5,1.6],
  };

  /* Section usage mock data for charts */
  const SECTION_USAGE = [
    { key: 'oportunidades_buscador', count: 287, users: 5 },
    { key: 'mercado_publico',        count: 241, users: 4 },
    { key: 'dashboard',              count: 198, users: 6 },
    { key: 'mercado_privado_dimensiones', count: 167, users: 3 },
    { key: 'forecast',               count: 134, users: 3 },
    { key: 'pliegos',                count: 112, users: 4 },
    { key: 'oportunidades_dimensiones', count: 98, users: 3 },
    { key: 'reporte_perfiles',       count: 87,  users: 2 },
    { key: 'sic_tracking',           count: 44,  users: 1 },
  ];

  const WEEKDAY_DATA = [
    { day: 'Lun', events: 142, avg: 98 },
    { day: 'Mar', events: 186, avg: 98 },
    { day: 'Mié', events: 203, avg: 98 },
    { day: 'Jue', events: 178, avg: 98 },
    { day: 'Vie', events: 124, avg: 98 },
    { day: 'Sáb', events: 22,  avg: 98 },
    { day: 'Dom', events: 8,   avg: 98 },
  ];

  const HEATMAP_DATA = (() => {
    const days = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'];
    const data = [];
    days.forEach((d, di) => {
      for (let h = 0; h < 24; h++) {
        let v = 0;
        if (di < 5) {
          if (h >= 8 && h <= 12) v = Math.round(Math.random() * 25 + 10 + (h === 10 ? 20 : 0));
          else if (h >= 13 && h <= 18) v = Math.round(Math.random() * 20 + 8);
          else if (h >= 19 && h <= 22) v = Math.round(Math.random() * 6);
        } else {
          if (h >= 9 && h <= 14) v = Math.round(Math.random() * 6);
        }
        data.push({ day: d, hour: h, value: v });
      }
    });
    return data;
  })();

  const ADOPTION_WEEKLY = [
    { week: 'S-7', score: 54 },
    { week: 'S-6', score: 57 },
    { week: 'S-5', score: 59 },
    { week: 'S-4', score: 61 },
    { week: 'S-3', score: 64 },
    { week: 'S-2', score: 67 },
    { week: 'S-1', score: 70 },
    { week: 'Est',  score: 73 },
  ];

  /* ═══════════════════════════════════════════════════════
     COMPUTED KPIs
  ═══════════════════════════════════════════════════════ */
  const LIVE_USERS = MOCK_USERS.filter(u =>
    u.status === 'active' || u.status === 'idle'
  );
  const ONLINE_COUNT = LIVE_USERS.length;
  const ACTIVE_USERS = MOCK_USERS.filter(u => u.active_days > 0);
  const INACTIVE_USERS = MOCK_USERS.filter(u => {
    const days = (Date.now() - new Date(u.last_access)) / 86400000;
    return days > 7;
  });
  const TOTAL_HOURS = MOCK_USERS.reduce((s, u) => s + u.active_hours, 0);
  const TOTAL_SESSIONS = MOCK_USERS.reduce((s, u) => s + u.sessions, 0);
  const TOTAL_UPLOADS = MOCK_USERS.reduce((s, u) => s + u.uploads, 0);
  const ADOPTION_RATE = Math.round(MOCK_USERS.filter(u => u.score >= 30).length / MOCK_USERS.length * 100);
  const PRODUCTIVITY = TOTAL_SESSIONS > 0 ? TOTAL_UPLOADS / TOTAL_SESSIONS : 0;

  /* ═══════════════════════════════════════════════════════
     MOCK ALERTS
  ═══════════════════════════════════════════════════════ */
  // Alertas se generan desde el backend (get_usage_summary → alerts), no datos ficticios
  const MOCK_ALERTS = [];

  /* ═══════════════════════════════════════════════════════
     STATE
  ═══════════════════════════════════════════════════════ */
  let currentTab   = 'usage-tab-live';
  let liveTimer    = null;
  let liveCounter  = 0;
  let activeAlertFilter = 'all';
  let resolvedAlerts = new Set();
  const trackedUsers    = new Set();
  const userGroupOverrides = new Map();
  let chartsBuilt  = { summary: false };
  let sortState    = { col: 'score', dir: 'desc' };
  let sparkCharts  = {};
  let profScoreChart = null;
  let activeMetric = 'events';
  let latestUsageSummary = null;

  const METRIC_CONFIG = {
    events:   { label: 'Eventos',       short: 'eventos', color: '#3b82f6', key: 'events' },
    hours:    { label: 'Horas activas', short: 'h',       color: '#8b5cf6', key: 'active_hours' },
    sessions: { label: 'Sesiones',      short: 'sesiones', color: '#06b6d4', key: 'sessions' },
    uploads:  { label: 'Cargas',        short: 'cargas',  color: '#10b981', key: 'uploads' },
  };

  const SUMMARY_CHART_IDS = [
    'usage-chart-weekday',
    'usage-chart-roles',
    'usage-chart-heatmap',
    'usage-chart-sections',
    'usage-chart-adoption',
    'usage-chart-donut',
  ];

  function metricCfg(metric = activeMetric) {
    return METRIC_CONFIG[metric] || METRIC_CONFIG.events;
  }

  function metricValue(obj, metric = activeMetric) {
    const cfg = metricCfg(metric);
    if (!obj) return 0;
    if (metric === 'events') return Number(obj.events ?? obj.count ?? obj.value ?? 0) || 0;
    if (metric === 'hours') return Number(obj.active_hours ?? obj.hours ?? 0) || 0;
    return Number(obj[cfg.key] ?? obj[metric] ?? 0) || 0;
  }

  function metricTooltip(v, metric = activeMetric) {
    if (metric === 'hours') return `${fmtDec(v, 1)} h`;
    return `${fmtInt(v)} ${metricCfg(metric).short}`;
  }

  function destroySummaryCharts() {
    SUMMARY_CHART_IDS.forEach(id => {
      const el = document.getElementById(id);
      if (el && el._chart) {
        el._chart.destroy();
        el._chart = null;
      }
    });
    chartsBuilt.summary = false;
  }

  function normalizeSummaryUser(row, existing = {}) {
    const uid = Number(row.user_id ?? row.id ?? existing.id ?? 0);
    const score = Math.max(0, Math.min(100, Math.round(Number(row.adoption_score ?? row.score ?? existing.score ?? 0))));
    const base = Math.max(0, score - 14);
    const hist = Array.from({ length: 8 }, (_, i) => Math.min(100, base + Math.round(i * (score - base) / 7)));
    return {
      ...existing,
      id: uid,
      username: row.name || row.username || existing.username || (row.email || '').split('@')[0],
      email: row.email || existing.email || '',
      role: row.role_raw || row.role || existing.role || 'analista',
      unit: row.unit_business || row.unit || existing.unit || 'Sin unidad',
      group: row.group || existing.group || 'Sin grupo',
      created: String(row.created_at || existing.created || '').slice(0, 10),
      score,
      sessions: Number(row.sessions ?? existing.sessions ?? 0),
      active_days: Number(row.active_days ?? existing.active_days ?? 0),
      active_hours: Number(row.active_hours ?? existing.active_hours ?? 0),
      views: Number(row.views ?? existing.views ?? 0),
      searches: Number(row.searches ?? existing.searches ?? 0),
      downloads: Number(row.downloads ?? row.exports ?? existing.downloads ?? 0),
      uploads: Number(row.uploads ?? existing.uploads ?? 0),
      exports: Number(row.exports ?? existing.exports ?? 0),
      modules: Array.isArray(row.modules_used_list) ? row.modules_used_list : (existing.modules || []),
      frequency: row.frequency || existing.frequency || 'Sin actividad',
      last_access: row.last_seen || existing.last_access || null,
      risk: row.risk_level || existing.risk || 'bajo',
      status: existing.status || 'offline',
      current_section: existing.current_section || '',
      session_start: existing.session_start || null,
      last_ping: existing.last_ping || row.last_signal || row.last_seen || null,
      last_action: existing.last_action || row.last_action || 'Sin actividad registrada',
      activity_type: existing.activity_type || row.activity_type || null,
      nav_trail: existing.nav_trail || [],
      sessions_detail: Array.isArray(row.recent_sessions) ? row.recent_sessions : (existing.sessions_detail || []),
      timeline: existing.timeline || [],
      score_history: existing.score_history?.length ? existing.score_history : hist,
    };
  }

  function syncUsageSummary(summary) {
    if (!summary || typeof summary !== 'object') return;
    latestUsageSummary = summary;

    if (Array.isArray(summary.per_user)) {
      const existingById = new Map(MOCK_USERS.map(u => [Number(u.id), u]));
      const nextUsers = summary.per_user.map(row => {
        const uid = Number(row.user_id ?? row.id ?? 0);
        return normalizeSummaryUser(row, existingById.get(uid) || {});
      }).filter(u => u.id);
      if (nextUsers.length) {
        MOCK_USERS.length = 0;
        nextUsers.forEach(u => MOCK_USERS.push(u));
      }
    }

    const charts = summary.charts || {};
    if (Array.isArray(charts.by_weekday) && charts.by_weekday.length) {
      WEEKDAY_DATA.length = 0;
      charts.by_weekday.forEach(row => WEEKDAY_DATA.push({
        day: row.weekday_label || row.day || String(row.weekday_index ?? ''),
        events: Number(row.events || 0),
        users: Number(row.users || 0),
        active_hours: Number(row.active_hours || 0),
        sessions: Number(row.sessions || 0),
        uploads: Number(row.uploads || 0),
      }));
    }

    if (Array.isArray(charts.heatmap)) {
      HEATMAP_DATA.length = 0;
      const dayLabels = ['Lun', 'Mar', 'Mie', 'Jue', 'Vie', 'Sab', 'Dom'];
      charts.heatmap.forEach(row => HEATMAP_DATA.push({
        day: dayLabels[Number(row.weekday)] || String(row.weekday),
        hour: Number(row.hour || 0),
        value: Number(row.events || 0),
        events: Number(row.events || 0),
        active_hours: Number(row.active_hours || 0),
        sessions: Number(row.sessions || 0),
        uploads: Number(row.uploads || 0),
      }));
    }

    if (Array.isArray(charts.sections)) {
      SECTION_USAGE.length = 0;
      charts.sections.forEach(row => SECTION_USAGE.push({
        key: row.section || row.key || '',
        count: Number(row.events || row.count || 0),
        events: Number(row.events || row.count || 0),
        users: Number(row.users || 0),
        active_hours: Number(row.active_hours || 0),
        sessions: Number(row.sessions || 0),
        uploads: Number(row.uploads || 0),
      }));
    }

    if (Array.isArray(summary.alerts)) {
      MOCK_ALERTS.length = 0;
      summary.alerts.forEach((a, idx) => {
        const severity = (a.severity || 'baja').toLowerCase();
        const icon = severity === 'alta' ? 'bi-exclamation-octagon' : (severity === 'media' ? 'bi-exclamation-triangle' : 'bi-info-circle');
        const uid = Number(a.user_id ?? a.userId ?? 0);
        MOCK_ALERTS.push({
          id: String(a.id || `${uid || 'sys'}-${severity}-${idx}-${a.reason || 'alert'}`),
          userId: uid,
          severity,
          icon,
          title: a.reason || a.title || 'Alerta de uso',
          body: a.message || a.body || 'Situacion detectada por el seguimiento operativo.',
          rec: a.recommendation || a.rec || 'Revisar el caso y definir accion de seguimiento.',
          time: a.timestamp || a.time || new Date().toISOString(),
          status: a.status || 'abierta',
        });
      });
      resolvedAlerts = new Set();
    }
  }

  /* ═══════════════════════════════════════════════════════
     INIT
  ═══════════════════════════════════════════════════════ */
  document.addEventListener('DOMContentLoaded', () => {
    syncUsageSummary(window.TRACKING_USAGE_SUMMARY);
    setDefaultDates();
    initTabs();
    initFiltersForm();
    renderKPIs();
    renderSparklines();
    renderLiveTable();
    startLiveRefresh();
    renderByUserTable();
    renderAlerts();
    initProfilePanel();
    initAlertFilters();
    initSortHeaders();
    initMetricSelector();

    // Refresh button
    const rbtn = $('#usage-refresh-btn');
    if (rbtn) rbtn.addEventListener('click', () => { loadUsageSummaryFromBackend(); fetchLiveUsersFromBackend(); });
    window.sicLiveRefresh = renderLiveTable;
    window.sicRefreshTracking = () => { loadUsageSummaryFromBackend(); };

    // FX layer (non-blocking — all effects degrade gracefully)
    setTimeout(initAllEffects, 0);
  });

  /* ═══════════════════════════════════════════════════════
     DATES
  ═══════════════════════════════════════════════════════ */
  function setDefaultDates() {
    const to   = new Date();
    const from = new Date(to.getTime() - 30 * 86400000);
    const fmt  = d => d.toISOString().slice(0, 10);
    const f = $('#flt-date-from'), t = $('#flt-date-to');
    if (f && !f.value) f.value = fmt(from);
    if (t && !t.value) t.value = fmt(to);
  }

  /* ═══════════════════════════════════════════════════════
     TABS
  ═══════════════════════════════════════════════════════ */
  function initTabs() {
    $$('.trk-tab').forEach(btn => {
      btn.addEventListener('click', () => {
        const targetId = btn.dataset.usageTabTarget?.replace('#', '');
        if (!targetId) return;
        $$('.trk-tab').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        $$('[data-usage-tab-panel]').forEach(p => {
          const isTarget = p.id === targetId;
          p.style.display = isTarget ? 'block' : 'none';
          p.classList.toggle('show',   isTarget);
          p.classList.toggle('active', isTarget);
        });
        currentTab = targetId;
        if (targetId === 'usage-tab-summary') {
          if (!chartsBuilt.summary) {
            setTimeout(buildSummaryCharts, 50);
          } else {
            // Panel was already built but hidden — resize ApexCharts
            setTimeout(() => {
              ['usage-chart-weekday','usage-chart-roles','usage-chart-heatmap',
               'usage-chart-sections','usage-chart-adoption','usage-chart-donut'].forEach(id => {
                const el = document.getElementById(id);
                if (el && el._chart) el._chart.updateOptions({}, false, false);
              });
            }, 50);
          }
        }
      });
    });

    // Init panels visibility — explicit 'block'/'none', never '' (empty clears inline
    // style and lets Bootstrap's .tab-pane { display:none } take over)
    $$('[data-usage-tab-panel]').forEach((p, i) => {
      p.style.display = i === 0 ? 'block' : 'none';
    });
  }

  function initFiltersForm() {
    const form = $('#usage-filters-form');
    if (!form) return;
    form.addEventListener('submit', e => {
      e.preventDefault();
      loadUsageSummaryFromBackend();
    });
  }

  function loadUsageSummaryFromBackend() {
    const form = $('#usage-filters-form');
    const params = new URLSearchParams();
    if (form) {
      const data = new FormData(form);
      ['date_from', 'date_to', 'role', 'team', 'granularity'].forEach(key => {
        const value = data.get(key);
        if (value != null && String(value).trim() !== '') params.set(key, value);
      });
    }

    return fetch(`/sic/api/usage/summary?${params.toString()}`)
      .then(r => r.json())
      .then(resp => {
        if (!resp || resp.ok === false) throw new Error(resp?.detail || 'summary_error');
        syncUsageSummary(resp);
        renderKPIs();
        renderLiveTable();
        renderByUserTable();
        renderAlerts();
        destroySummaryCharts();
        if (currentTab === 'usage-tab-summary') setTimeout(buildSummaryCharts, 30);
        return resp;
      })
      .catch(() => {
        showToast('No se pudo actualizar el resumen de uso. Se mantienen los datos cargados.', 'warn');
      });
  }

  /* ═══════════════════════════════════════════════════════
     KPIs
  ═══════════════════════════════════════════════════════ */
  function setEl(id, html) {
    const el = document.getElementById(id);
    if (el) el.innerHTML = html;
  }

  function renderKPIs() {
    const liveCount = MOCK_USERS.filter(u => u.status === 'active' || u.status === 'idle').length;
    const activeUsers = MOCK_USERS.filter(u => Number(u.active_days || 0) > 0);
    const inactiveUsers = MOCK_USERS.filter(u => {
      if (!u.last_access) return true;
      return ((Date.now() - new Date(u.last_access).getTime()) / 86400000) > 7;
    });
    const totalHours = MOCK_USERS.reduce((s, u) => s + Number(u.active_hours || 0), 0);
    const totalSessions = MOCK_USERS.reduce((s, u) => s + Number(u.sessions || 0), 0);
    const totalUploads = MOCK_USERS.reduce((s, u) => s + Number(u.uploads || 0), 0);
    const adoptionRate = latestUsageSummary?.kpis?.adoption_rate != null
      ? Number(latestUsageSummary.kpis.adoption_rate)
      : (MOCK_USERS.length ? Math.round(MOCK_USERS.filter(u => Number(u.score || 0) >= 30).length / MOCK_USERS.length * 100) : 0);
    const productivity = latestUsageSummary?.kpis?.avg_productivity_index != null
      ? Number(latestUsageSummary.kpis.avg_productivity_index)
      : (totalSessions > 0 ? totalUploads / totalSessions : 0);

    animateNumber('card-live-users', latestUsageSummary?.kpis?.connected_now ?? liveCount);
    animateNumber('card-adoption-val', adoptionRate, {
      suffix: '<span style="font-size:16px;font-weight:500;">%</span>',
    });
    animateNumber('card-inactive-users', latestUsageSummary?.kpis?.inactive_7d_count ?? inactiveUsers.length);
    animateNumber('card-active-users', latestUsageSummary?.kpis?.active_users ?? activeUsers.length);
    animateNumber('card-active-hours', latestUsageSummary?.kpis?.active_hours ?? totalHours, { decimals: 0 });
    animateNumber('card-prod-index', productivity, { decimals: 1, commaDecimal: true });
    animateNumber('live-count-badge', latestUsageSummary?.kpis?.connected_now ?? liveCount);

    // KPI state text
    setEl('usage-kpi-state', `
      <i class="bi bi-check-circle me-1" style="color:#10b981;"></i>
      Indicadores actualizados · ${new Date().toLocaleTimeString('es-AR', {hour:'2-digit',minute:'2-digit'})}
    `);

    // Trends
    const trendEl = (id, val, suffix = '') => {
      const el = document.getElementById(id);
      if (!el) return;
      el.style.display = '';
      el.className = `kpi-trend ${val >= 0 ? 'up' : 'down'}`;
      el.textContent = `${val >= 0 ? '↑' : '↓'} ${Math.abs(val)}${suffix}`;
    };
    trendEl('kpi-adopt-trend',    5, '%');
    trendEl('kpi-active-trend',   1, '');
    trendEl('kpi-hours-trend',    8.2, ' h');
    trendEl('kpi-prod-trend',     0.1, '');
    const inact = document.getElementById('kpi-inactive-trend');
    if (inact) { inact.style.display=''; inact.className='kpi-trend up'; inact.textContent='↓ 1'; }
  }

  /* ═══════════════════════════════════════════════════════
     SPARKLINES (Chart.js mini)
  ═══════════════════════════════════════════════════════ */
  function buildSparkline(canvasId, data, color) {
    const canvas = document.getElementById(canvasId);
    if (!canvas) return;
    if (sparkCharts[canvasId]) { sparkCharts[canvasId].destroy(); }
    sparkCharts[canvasId] = new Chart(canvas, {
      type: 'line',
      data: {
        labels: data.map((_, i) => i),
        datasets: [{
          data,
          borderColor: color,
          borderWidth: 1.5,
          fill: true,
          backgroundColor: color.replace(')', ', 0.15)').replace('rgb', 'rgba'),
          pointRadius: 0,
          tension: .4,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: { enabled: false } },
        scales: { x: { display: false }, y: { display: false } },
        animation: { duration: 600 },
      },
    });
  }

  function renderSparklines() {
    if (typeof Chart === 'undefined') return;
    buildSparkline('spark-live',     MOCK_KPI_HISTORY.live,     'rgb(16,185,129)');
    buildSparkline('spark-adopt',    MOCK_KPI_HISTORY.adoption, 'rgb(59,130,246)');
    buildSparkline('spark-inactive', MOCK_KPI_HISTORY.inactive, 'rgb(245,158,11)');
    buildSparkline('spark-active',   MOCK_KPI_HISTORY.active,   'rgb(59,130,246)');
    buildSparkline('spark-hours',    MOCK_KPI_HISTORY.hours,    'rgb(139,92,246)');
    buildSparkline('spark-prod',     MOCK_KPI_HISTORY.prod,     'rgb(6,182,212)');
  }

  /* ═══════════════════════════════════════════════════════
     ACTIVITY CHIP
  ═══════════════════════════════════════════════════════ */
  function activityChip(type) {
    const map = {
      'navegando':   ['chip-blue',   'bi-mouse2',           'Navegando'],
      'buscando':    ['chip-purple', 'bi-search',           'Buscando'],
      'cargando':    ['chip-green',  'bi-cloud-upload',     'Cargando'],
      'exportando':  ['chip-yellow', 'bi-box-arrow-up-right','Exportando'],
      'editando':    ['chip-cyan',   'bi-pencil',           'Editando'],
    };
    const [cls, icon, label] = map[type] || ['chip-gray','bi-activity','Activo'];
    return `<span class="chip ${cls}"><i class="bi ${icon}"></i> ${label}</span>`;
  }

  function statusPill(status) {
    if (status === 'active')  return `<span class="chip chip-green"><span class="sdot sdot-active"></span> Activo</span>`;
    if (status === 'idle')    return `<span class="chip chip-yellow"><span class="sdot sdot-idle"></span> Idle</span>`;
    return `<span class="chip chip-gray"><span class="sdot sdot-offline"></span> Offline</span>`;
  }

  /* ═══════════════════════════════════════════════════════
     LIVE TABLE (Tab 1)
  ═══════════════════════════════════════════════════════ */
  function renderLiveTable() {
    const tbody = document.getElementById('usage-live-tbody');
    if (!tbody) return;

    const liveUsers = MOCK_USERS.filter(u => u.status !== 'offline');

    if (!liveUsers.length) {
      tbody.innerHTML = `<tr><td colspan="12">
        <div class="trk-empty">
          <div class="trk-empty-icon"><i class="bi bi-broadcast"></i></div>
          <div class="trk-empty-title">No hay usuarios conectados en este momento</div>
        </div>
      </td></tr>`;
    } else {
      tbody.innerHTML = liveUsers.map(u => buildLiveRow(u)).join('');
      // Bind expand + profile click
      tbody.querySelectorAll('tr.live-user-row').forEach(row => {
        row.addEventListener('click', (e) => {
          if (e.target.closest('.expand-btn') || e.target.closest('.trk-tab-trigger')) return;
          const uid = Number(row.dataset.uid);
          openProfile(uid);
        });
      });
      tbody.querySelectorAll('.expand-btn').forEach(btn => {
        btn.addEventListener('click', (e) => {
          e.stopPropagation();
          const uid = Number(btn.closest('tr').dataset.uid);
          toggleExpandRow(btn, uid);
        });
      });
    }

    // Update badge + timestamp
    setEl('live-count-badge', liveUsers.length);
    const lud = document.getElementById('live-last-update');
    if (lud) lud.textContent = `Actualizado ${new Date().toLocaleTimeString('es-AR', { hour:'2-digit', minute:'2-digit', second:'2-digit' })}`;
    liveCounter++;
  }

  function buildLiveRow(u) {
    const trail = (u.nav_trail || []).slice(-3);
    const trailHtml = trail.length
      ? trail.map((s, i) => `
          <span class="nav-trail-item${i === trail.length - 1 ? ' current' : ''}">${esc(sectionLabel(s))}</span>
          ${i < trail.length - 1 ? '<span class="nav-trail-sep">›</span>' : ''}
        `).join('')
      : '<span style="color:var(--t-muted);font-size:11px;">Sin historial</span>';

    return `
      <tr class="live-user-row" data-uid="${u.id}">
        <td class="table-action-col">
          <button class="expand-btn" title="Ver detalle de sesión"><i class="bi bi-chevron-right"></i></button>
        </td>
        <td>
          <div style="display:flex;align-items:center;gap:9px;">
            <div style="width:32px;height:32px;border-radius:50%;background:${avatarColor(u.username)};
              display:flex;align-items:center;justify-content:center;font-size:11px;font-weight:700;
              color:#fff;flex-shrink:0;">${esc(initials(u.username))}</div>
            <div>
              <div style="font-weight:600;color:#e2e8f0;font-size:12.5px;">${esc(u.username)}</div>
              <div style="font-size:10.5px;color:var(--t-muted);">${esc(u.email)}</div>
            </div>
          </div>
        </td>
        <td><span class="chip chip-gray" style="text-transform:capitalize;">${esc(u.role)}</span></td>
        <td style="font-size:11.5px;color:var(--t-muted2);max-width:120px;overflow:hidden;text-overflow:ellipsis;">${esc(u.unit)}</td>
        <td style="font-size:11.5px;color:var(--t-muted2);">${esc(u.group)}</td>
        <td>
          <span class="chip chip-blue" title="${esc(u.current_section)}">
            <i class="bi bi-layout-text-window-reverse"></i> ${esc(sectionLabel(u.current_section))}
          </span>
        </td>
        <td>
          <div class="nav-trail">${trailHtml}</div>
        </td>
        <td style="font-size:11.5px;color:var(--t-muted2);">${esc(u.last_action)}</td>
        <td class="text-end" style="font-size:11.5px;color:var(--t-muted);white-space:nowrap;">${fmtRelative(u.last_ping)}</td>
        <td>${statusPill(u.status)}</td>
        <td style="font-size:11.5px;color:var(--t-muted);white-space:nowrap;">${u.session_start ? fmtTime(u.session_start) : '—'}</td>
        <td>${u.activity_type ? activityChip(u.activity_type) : '<span style="color:var(--t-muted);font-size:11px;">—</span>'}</td>
      </tr>
    `;
  }

  function toggleExpandRow(btn, uid) {
    const tr = btn.closest('tr');
    const existing = tr.nextElementSibling;
    if (existing && existing.classList.contains('row-detail-panel-tr')) {
      existing.remove();
      btn.classList.remove('open');
      btn.querySelector('i').className = 'bi bi-chevron-right';
      return;
    }
    btn.classList.add('open');
    btn.querySelector('i').className = 'bi bi-chevron-down';

    const u = MOCK_USERS.find(x => x.id === uid);
    if (!u) return;
    const detail = document.createElement('tr');
    detail.className = 'row-detail-panel-tr';
    const events = (u.timeline || []).slice(0, 6);
    detail.innerHTML = `<td colspan="12" class="row-detail-panel">
      <div style="font-size:11px;font-weight:700;text-transform:uppercase;letter-spacing:.5px;color:var(--t-muted);margin-bottom:8px;">
        <i class="bi bi-clock me-1"></i> Timeline de sesión — ${esc(u.username)}
      </div>
      <div class="session-timeline">
        ${events.map(ev => `
          <div class="session-event">
            <div class="session-event-dot" style="background:${evColor(ev.action)};"></div>
            <div class="session-event-time">${fmtTime(ev.time)}</div>
            <div class="session-event-name">${evLabel(ev.action)} · <strong style="color:#e2e8f0;">${esc(sectionLabel(ev.section))}</strong></div>
          </div>
        `).join('')}
      </div>
    </td>`;
    tr.insertAdjacentElement('afterend', detail);
  }

  function evColor(a) {
    return { view:'#3b82f6', search:'#8b5cf6', upload:'#10b981', download:'#f59e0b', export:'#06b6d4' }[a] || '#64748b';
  }
  function evLabel(a) {
    return { view:'Vista', search:'Búsqueda', upload:'Carga', download:'Descarga', export:'Exportación' }[a] || 'Acción';
  }

  /* ═══════════════════════════════════════════════════════
     AUTO-REFRESH LIVE — sincroniza con datos reales del backend
  ═══════════════════════════════════════════════════════ */
  function fetchLiveUsersFromBackend() {
    fetch('/sic/api/usage/live-users')
      .then(r => r.json())
      .then(resp => {
        if (!resp.ok || !Array.isArray(resp.users)) return;
        resp.users.forEach(live => {
          const uid = Number(live.id);
          const existing = MOCK_USERS.find(u => u.id === uid);
          if (!existing) return;
          // Actualizar campos de presencia en vivo
          if (live.current_section != null) existing.current_section = live.current_section;
          if (live.last_action)             existing.last_action     = live.last_action;
          if (live.activity_type)           existing.activity_type   = live.activity_type;
          if (live.last_signal)             existing.last_ping       = live.last_signal;
          if (live.session_start)           existing.session_start   = live.session_start;
          // Actualizar estado según señal
          const rawStatus = (live.status || '').toLowerCase();
          const statusMap = { activo: 'active', active: 'active', inactivo: 'idle', inactive: 'idle', ausente: 'idle' };
          existing.status = statusMap[rawStatus] || 'offline';
        });
        // Marcar como offline a quienes no aparecen en live
        const liveIds = new Set(resp.users.map(u => Number(u.id)));
        MOCK_USERS.forEach(u => { if (!liveIds.has(u.id)) u.status = 'offline'; });
      })
      .catch(() => { /* red no disponible, mantener datos actuales */ });
  }

  function startLiveRefresh() {
    if (liveTimer) clearInterval(liveTimer);
    // Primer ciclo: sincronizar datos reales inmediatamente
    fetchLiveUsersFromBackend();
    liveTimer = setInterval(() => {
      fetchLiveUsersFromBackend();
      if (currentTab === 'usage-tab-live') renderLiveTable();
    }, 30000);
  }

  /* ═══════════════════════════════════════════════════════
     SUMMARY CHARTS (Tab 2) — ApexCharts
  ═══════════════════════════════════════════════════════ */
  const CHART_DEFAULTS = {
    chart: { background: 'transparent', fontFamily: 'Outfit, sans-serif' },
    theme: { mode: 'dark' },
    grid: { borderColor: 'rgba(255,255,255,.06)', strokeDashArray: 3 },
    tooltip: { theme: 'dark' },
  };

  function buildSummaryCharts() {
    if (typeof ApexCharts === 'undefined') return;
    chartsBuilt.summary = true;
    const cfg = metricCfg();

    // 1. Actividad por día
    const wdEl = document.getElementById('usage-chart-weekday');
    if (wdEl && !wdEl._chart) {
      const weekdayValues = WEEKDAY_DATA.map(d => metricValue(d));
      const weekdayAvg = weekdayValues.length
        ? Math.round((weekdayValues.reduce((s, v) => s + v, 0) / weekdayValues.length) * 10) / 10
        : 0;
      const c = new ApexCharts(wdEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 310, toolbar: { show: false } },
        series: [
          { name: cfg.label, data: weekdayValues },
          { name: 'Promedio', type: 'line', data: WEEKDAY_DATA.map(() => weekdayAvg) },
        ],
        xaxis: { categories: WEEKDAY_DATA.map(d => d.day), labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        colors: [cfg.color, '#f59e0b'],
        stroke: { width: [0, 2.5], curve: 'smooth' },
        fill: { opacity: [.85, 1], type: ['solid','solid'] },
        markers: { size: [0, 4] },
        dataLabels: { enabledOnSeries: [0] },
        plotOptions: { bar: { borderRadius: 5, columnWidth: '55%' } },
        legend: { labels: { colors: '#94a3b8' }, fontSize: '12px' },
        tooltip: {
          theme: 'dark',
          shared: true,
          intersect: false,
          y: { formatter: v => metricTooltip(v) },
        },
      });
      c.render(); wdEl._chart = c;
    }

    // 2. Top usuarios (barras horizontales)
    const rolesEl = document.getElementById('usage-chart-roles');
    if (rolesEl && !rolesEl._chart) {
      const sorted = [...MOCK_USERS].sort((a, b) => metricValue(b) - metricValue(a)).slice(0, 15);
      const c = new ApexCharts(rolesEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 310, toolbar: { show: false } },
        plotOptions: { bar: { horizontal: true, borderRadius: 4, dataLabels: { position: 'top' } } },
        series: [
          { name: cfg.label, data: sorted.map(u => ({ x: u.username.split(' ')[0], y: metricValue(u) })) },
          { name: 'Score',         data: sorted.map(u => ({ x: u.username.split(' ')[0], y: u.score })) },
        ],
        colors: [cfg.color, '#10b981'],
        xaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#94a3b8', fontSize: '11px' }, maxWidth: 100 } },
        legend: { labels: { colors: '#94a3b8' }, fontSize: '12px' },
        tooltip: { theme: 'dark', shared: false, y: { formatter: (v, opts) => opts.seriesIndex === 0 ? metricTooltip(v) : `${fmtInt(v)} pts` } },
      });
      c.render(); rolesEl._chart = c;
    }

    // 3. Heatmap
    const hmEl = document.getElementById('usage-chart-heatmap');
    if (hmEl && !hmEl._chart) {
      const days = ['Lun','Mar','Mié','Jue','Vie','Sáb','Dom'];
      const series = days.map(d => ({
        name: d,
        data: Array.from({ length: 24 }, (_, h) => {
          const dKey = d.normalize('NFD').replace(/[\u0300-\u036f]/g, '');
          const entry = HEATMAP_DATA.find(x => x.hour === h && (x.day === d || x.day === dKey));
          return { x: `${h}:00`, y: entry ? metricValue(entry) : 0 };
        }),
      }));
      const c = new ApexCharts(hmEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'heatmap', height: 310, toolbar: { show: false } },
        series,
        dataLabels: { enabled: false },
        colors: [cfg.color],
        xaxis: {
          labels: {
            show: true, rotate: 0,
            formatter: v => ['0','3','6','9','12','15','18','21','23'].includes(v.split(':')[0]) ? v : '',
            style: { colors: '#64748b', fontSize: '9px' },
          },
        },
        yaxis: { labels: { style: { colors: '#94a3b8', fontSize: '10px' } } },
        tooltip: {
          theme: 'dark',
          custom: ({ seriesIndex, dataPointIndex, w }) => {
            const val = w.config.series[seriesIndex].data[dataPointIndex].y;
            const day = w.config.series[seriesIndex].name;
            const hour = w.config.series[seriesIndex].data[dataPointIndex].x;
            return `<div style="padding:8px 12px;font-size:12px;background:#0f1729;border:1px solid #1e2d4a;border-radius:8px;">
              <strong>${day} ${hour}</strong><br>
              <span style="color:#93c5fd;">${metricTooltip(val)}</span>
            </div>`;
          },
        },
      });
      c.render(); hmEl._chart = c;
    }

    // 4. Secciones más usadas
    const secEl = document.getElementById('usage-chart-sections');
    if (secEl && !secEl._chart) {
      const total = SECTION_USAGE.reduce((s, x) => s + metricValue(x), 0);
      const c = new ApexCharts(secEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 310, toolbar: { show: false } },
        plotOptions: { bar: { horizontal: true, borderRadius: 4, dataLabels: { position: 'top' } } },
        series: [{ name: cfg.label, data: SECTION_USAGE.map(s => ({ x: sectionLabel(s.key), y: metricValue(s) })) }],
        colors: [cfg.color],
        xaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#94a3b8', fontSize: '11px' }, maxWidth: 160 } },
        tooltip: {
          theme: 'dark',
          y: { formatter: (v, { dataPointIndex }) => {
            const pct = total > 0 ? ((metricValue(SECTION_USAGE[dataPointIndex]) / total) * 100).toFixed(1) : '0.0';
            return `${metricTooltip(v)} (${pct}%)`;
          }},
        },
      });
      c.render(); secEl._chart = c;
    }

    // 5. Evolución adopción (línea)
    const adoptEl = document.getElementById('usage-chart-adoption');
    if (adoptEl && !adoptEl._chart) {
      const c = new ApexCharts(adoptEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'area', height: 310, toolbar: { show: false } },
        series: [{ name: 'Score promedio', data: ADOPTION_WEEKLY.map(d => d.score) }],
        xaxis: { categories: ADOPTION_WEEKLY.map(d => d.week), labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { min: 40, max: 100, labels: { style: { colors: '#64748b', fontSize: '11px' }, formatter: v => v + ' pts' } },
        colors: ['#10b981'],
        stroke: { curve: 'smooth', width: 2.5 },
        fill: { type: 'gradient', gradient: { shadeIntensity: 1, opacityFrom: .35, opacityTo: .02, stops: [0,100] } },
        markers: { size: 4, strokeWidth: 0 },
        annotations: {
          yaxis: [{ y: 60, borderColor: '#f59e0b', borderWidth: 1, strokeDashArray: 4,
            label: { text: 'Umbral 60%', style: { background: 'transparent', color: '#f59e0b', fontSize: '10px' }, position: 'left' } }],
        },
        tooltip: { theme: 'dark', y: { formatter: v => v + ' pts' } },
      });
      c.render(); adoptEl._chart = c;
    }

    // 6. Donut — distribución por módulo
    const donutEl = document.getElementById('usage-chart-donut');
    if (donutEl && !donutEl._chart) {
      const topModules = SECTION_USAGE.slice(0, 6);
      const c = new ApexCharts(donutEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'donut', height: 310, toolbar: { show: false } },
        series: topModules.map(s => metricValue(s)),
        labels: topModules.map(s => sectionLabel(s.key)),
        colors: ['#3b82f6','#10b981','#6366f1','#f59e0b','#ef4444','#06b6d4'],
        stroke: { width: 2, colors: ['#0f1729'] },
        plotOptions: { pie: { donut: { size: '62%', labels: {
          show: true,
          name: {
            show: true,
            fontSize: '11px',
            fontFamily: '"Outfit",system-ui,sans-serif',
            color: '#94a3b8',
            offsetY: -4,
            formatter: val => val.length > 20 ? val.slice(0, 18) + '…' : val,
          },
          value: {
            show: true,
            fontSize: '20px',
            fontFamily: '"Outfit",system-ui,sans-serif',
            fontWeight: 700,
            color: '#e2e8f0',
            offsetY: 4,
            formatter: val => activeMetric === 'hours' ? fmtDec(val, 1) : fmtInt(val),
          },
          total: {
            show: true,
            label: 'Total',
            fontSize: '11px',
            fontFamily: '"Outfit",system-ui,sans-serif',
            color: '#94a3b8',
            formatter: () => metricTooltip(SECTION_USAGE.reduce((s,x)=>s + metricValue(x),0)),
          },
        }}}},
        legend: { position: 'bottom', labels: { colors: '#94a3b8' }, fontSize: '11px' },
        tooltip: { theme: 'dark', y: { formatter: v => metricTooltip(v) } },
      });
      c.render(); donutEl._chart = c;
    }
  }

  /* ═══════════════════════════════════════════════════════
     METRIC SELECTOR
  ═══════════════════════════════════════════════════════ */
  function initMetricSelector() {
    const btns = $$('.metric-btn');
    btns.forEach(btn => {
      btn.addEventListener('click', () => {
        btns.forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeMetric = btn.dataset.metric || 'events';
        destroySummaryCharts();
        buildSummaryCharts();
      });
    });
  }

  /* ═══════════════════════════════════════════════════════
     BY USER TABLE (Tab 3)
  ═══════════════════════════════════════════════════════ */
  function renderByUserTable() {
    const tbody = document.getElementById('su-users-byuser-tbody');
    if (!tbody) return;

    const users = sortUsers([...MOCK_USERS]);
    tbody.innerHTML = users.map(u => buildUserRow(u)).join('');

    tbody.querySelectorAll('tr').forEach(row => {
      row.addEventListener('click', () => {
        const uid = Number(row.dataset.uid);
        if (uid) openProfile(uid);
      });
    });
  }

  function sortUsers(arr) {
    const { col, dir } = sortState;
    const colMap = {
      score: 'score', sessions: 'sessions', days: 'active_days',
      hours: 'active_hours', views: 'views', searches: 'searches',
      downloads: 'downloads', uploads: 'uploads', last_access: 'last_access',
    };
    const key = colMap[col] || 'score';
    arr.sort((a, b) => {
      let av = a[key], bv = b[key];
      if (key === 'last_access') { av = new Date(av).getTime(); bv = new Date(bv).getTime(); }
      return dir === 'desc' ? bv - av : av - bv;
    });
    return arr;
  }

  function initSortHeaders() {
    $$('.trk-table thead th.sortable').forEach(th => {
      th.addEventListener('click', () => {
        const col = th.dataset.sort;
        if (sortState.col === col) sortState.dir = sortState.dir === 'desc' ? 'asc' : 'desc';
        else { sortState.col = col; sortState.dir = 'desc'; }
        const ind = document.getElementById('sort-indicator');
        if (ind) ind.textContent = `${th.textContent.trim()} ${sortState.dir === 'desc' ? '↓' : '↑'}`;
        renderByUserTable();
      });
    });
  }

  function buildUserRow(u) {
    const si = scoreInfo(u.score);
    const topModules = (u.modules || []).slice(0, 3);
    const riskClass = { alto: 'chip-red', medio: 'chip-yellow', baixo: 'chip-green', bajo: 'chip-green' }[u.risk] || 'chip-gray';
    const riskLabel = { alto: 'Alto', medio: 'Medio', bajo: 'Bajo' }[u.risk] || '—';

    return `
      <tr data-uid="${u.id}" style="cursor:pointer;">
        <td>
          <div class="score-wrap">
            <div class="score-num" style="color:${si.color};">${u.score}</div>
            <div class="score-bar-bg">
              <div class="score-bar-fill" style="width:${u.score}%;background:${si.gradient};"></div>
            </div>
          </div>
        </td>
        <td>
          <div style="display:flex;align-items:center;gap:8px;">
            <div style="width:30px;height:30px;border-radius:50%;background:${avatarColor(u.username)};
              display:flex;align-items:center;justify-content:center;font-size:10px;font-weight:700;
              color:#fff;flex-shrink:0;">${esc(initials(u.username))}</div>
            <div>
              <div style="font-weight:600;color:#e2e8f0;font-size:12px;">${esc(u.username)}</div>
              <div style="font-size:10px;color:var(--t-muted);">${esc(u.email)}</div>
            </div>
          </div>
        </td>
        <td><span class="chip chip-gray" style="text-transform:capitalize;">${esc(u.role)}</span></td>
        <td style="font-size:11px;color:var(--t-muted2);max-width:100px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;" title="${esc(u.unit)}">${esc(u.unit)}</td>
        <td style="font-size:11px;color:var(--t-muted2);">${esc(u.group)}</td>
        <td>${statusPill(u.status)}</td>
        <td class="text-center"><span class="chip ${riskClass}">${riskLabel}</span></td>
        <td class="text-end" style="font-size:12px;font-weight:600;">${fmtInt(u.sessions)}</td>
        <td class="text-end" style="font-size:12px;">${fmtInt(u.active_days)}</td>
        <td class="text-end" style="font-size:12px;">${fmtDec(u.active_hours, 1)} h</td>
        <td class="text-end" style="font-size:12px;">${fmtInt(u.views)}</td>
        <td class="text-end" style="font-size:12px;">${fmtInt(u.searches)}</td>
        <td class="text-end" style="font-size:12px;">${fmtInt(u.downloads)}</td>
        <td class="text-end" style="font-size:12px;color:var(--t-green);font-weight:600;">${fmtInt(u.uploads)}</td>
        <td>
          <div style="display:flex;gap:4px;flex-wrap:wrap;">
            ${topModules.map(m => `<span class="chip chip-blue" style="font-size:9.5px;">${esc(sectionLabel(m))}</span>`).join('')}
          </div>
        </td>
        <td style="font-size:11px;color:var(--t-muted2);">${esc(u.frequency)}</td>
        <td class="text-end" style="font-size:11px;color:var(--t-muted);white-space:nowrap;">${fmtRelative(u.last_access)}</td>
      </tr>
    `;
  }

  /* ═══════════════════════════════════════════════════════
     ALERTS (Tab 4)
  ═══════════════════════════════════════════════════════ */
  function renderAlerts() {
    const active = MOCK_ALERTS.filter(a => !resolvedAlerts.has(a.id));
    const countsBySev = { alta: 0, media: 0, baja: 0, info: 0 };
    active.forEach(a => { countsBySev[a.severity] = (countsBySev[a.severity] || 0) + 1; });

    // Update counters
    animateNumber('afc-all', active.length);
    animateNumber('afc-alta', countsBySev.alta);
    animateNumber('afc-media', countsBySev.media);
    animateNumber('afc-baja', (countsBySev.baja || 0) + (countsBySev.info || 0));

    const navBadge = document.getElementById('nav-alerts-badge');
    if (navBadge) {
      animateNumber('nav-alerts-badge', active.length);
      navBadge.style.display = active.length ? '' : 'none';
      navBadge.classList.remove('fx-pulse');
      void navBadge.offsetWidth;
      navBadge.classList.add('fx-pulse');
    }
    updateRadarSpeed();

    const filtered = activeAlertFilter === 'all'
      ? active
      : active.filter(a => a.severity === activeAlertFilter || (activeAlertFilter === 'baja' && a.severity === 'info'));

    const container = document.getElementById('alerts-container');
    if (!container) return;

    if (!filtered.length) {
      container.innerHTML = `
        <div class="col-12">
          <div class="trk-empty">
            <div class="trk-empty-icon"><i class="bi bi-check-circle"></i></div>
            <div class="trk-empty-title">Sin alertas en esta categoría</div>
            <div class="trk-empty-sub">No se detectaron situaciones que requieran atención.</div>
          </div>
        </div>`;
      return;
    }

    container.innerHTML = filtered.map(a => {
      const u = MOCK_USERS.find(x => x.id === a.userId);
      const sevLabel = { alta: 'Alta', media: 'Media', baja: 'Baja', info: 'Info' }[a.severity] || a.severity;
      const sevBadgeCls = `sev-${a.severity}`;
      return `
        <div class="col-12 col-md-6 col-xl-4" id="alert-card-${a.id}">
          <div class="alert-card sev-${a.severity}" data-alert-id="${a.id}" data-uid="${u ? u.id : ''}">
            <div class="alert-head">
              <div style="display:flex;gap:10px;align-items:flex-start;min-width:0;">
                <div class="alert-user-avatar" style="background:${u ? avatarColor(u.username) : '#3b82f6'};">
                  ${u ? esc(initials(u.username)) : '?'}
                </div>
                <div style="min-width:0;">
                  <div class="alert-title"><i class="bi ${esc(a.icon)} me-1"></i>${esc(a.title)}</div>
                  <div class="alert-user-label">${u ? esc(u.username) + ' · ' + esc(u.role) : 'Sistema'}</div>
                </div>
              </div>
              <span class="sev-badge ${sevBadgeCls}">${sevLabel}</span>
            </div>
            <div class="alert-body">${esc(a.body)}</div>
            <div class="alert-rec"><i class="bi bi-lightbulb me-1" style="color:var(--t-yellow);flex-shrink:0;"></i>${esc(a.rec)}</div>
            <div class="alert-footer">
              <div class="alert-time"><i class="bi bi-clock me-1"></i>${fmtRelative(a.time)} — ${fmtDT(a.time)}</div>
              <div class="alert-actions">
                ${u ? `<button class="btn-alert-profile" data-uid="${u.id}"><i class="bi bi-person"></i> Perfil</button>` : ''}
                <button class="btn-alert-resolve" data-alert-id="${a.id}"><i class="bi bi-check2"></i> Resolver</button>
              </div>
            </div>
          </div>
        </div>
      `;
    }).join('');

    // Bind resolve (with GSAP collapse + particle burst if available)
    container.querySelectorAll('.btn-alert-resolve').forEach(btn => {
      btn.addEventListener('click', e => {
        e.stopPropagation();
        const id   = btn.dataset.alertId;
        const col  = document.getElementById(`alert-card-${id}`);
        const card = col?.querySelector('.alert-card');
        if (!col || !card) return;
        if (typeof burstParticles === 'function') burstParticles(card);
        if (typeof gsap !== 'undefined') {
          gsap.to(card, {
            scaleY: 0, opacity: 0, duration: .3, ease: 'power2.in',
            onComplete() {
              gsap.to(col, {
                height: 0, padding: 0, margin: 0, duration: .2, ease: 'power2.in',
                onComplete() { resolvedAlerts.add(id); renderAlerts(); },
              });
            },
          });
        } else {
          card.classList.add('resolving');
          setTimeout(() => { resolvedAlerts.add(id); renderAlerts(); }, 350);
        }
      });
    });

    // Bind profile
    container.querySelectorAll('.btn-alert-profile').forEach(btn => {
      btn.addEventListener('click', e => {
        e.stopPropagation();
        openProfile(Number(btn.dataset.uid));
      });
    });

    container.querySelectorAll('.alert-card').forEach(card => {
      card.addEventListener('click', () => {
        const uid = Number(card.dataset.uid);
        if (uid) openProfile(uid);
      });
    });
  }

  function initAlertFilters() {
    const container = document.getElementById('alert-filters');
    if (!container) return;
    container.querySelectorAll('.alert-filter-btn').forEach(btn => {
      btn.addEventListener('click', () => {
        container.querySelectorAll('.alert-filter-btn').forEach(b => b.classList.remove('active'));
        btn.classList.add('active');
        activeAlertFilter = btn.dataset.filter;
        renderAlerts();
      });
    });
  }

  /* ═══════════════════════════════════════════════════════
     PROFILE PANEL (slide-in)
  ═══════════════════════════════════════════════════════ */
  function initProfilePanel() {
    const closeBtn  = document.getElementById('profile-close-btn');
    const overlay   = document.getElementById('profile-overlay');
    const closePanel = () => {
      document.getElementById('userProfilePanel')?.classList.remove('open');
      overlay?.classList.remove('open');
      if (typeof destroyNavGraph === 'function') destroyNavGraph();
    };
    if (closeBtn) closeBtn.addEventListener('click', closePanel);
    if (overlay)  overlay.addEventListener('click', closePanel);

    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') closePanel();
    });
  }

  function openProfile(uid) {
    const uBase = MOCK_USERS.find(x => x.id === uid);
    if (!uBase) return;

    const panel   = document.getElementById('userProfilePanel');
    const overlay = document.getElementById('profile-overlay');
    const loading = document.getElementById('profile-loading');
    const content = document.getElementById('profile-content');

    if (!panel) return;
    loading.style.display = '';
    content.style.display = 'none';
    panel.classList.add('open');
    overlay?.classList.add('open');

    // Muestra datos base inmediatamente y luego enriquece con API
    setTimeout(() => {
      populateProfile(uBase);
      loading.style.display = 'none';
      content.style.display = '';
      buildProfileScoreChart(uBase);
      if (typeof initNavGraph === 'function' && typeof THREE !== 'undefined') {
        setTimeout(() => initNavGraph(uBase), 80);
      }
    }, 180);

    // Fetch real data from API (sesiones, timeline, alertas)
    fetch(`/sic/api/usage/user-profile/${uid}`)
      .then(r => r.json())
      .then(resp => {
        if (!resp.ok || !resp.profile) return;
        const p = resp.profile;

        // Mapear estructura API → estructura UI
        const uEnriched = { ...uBase };

        // Sesiones recientes: API devuelve recent_sessions con start/end/sections
        if (Array.isArray(p.recent_sessions) && p.recent_sessions.length) {
          uEnriched.sessions_detail = p.recent_sessions.map(s => ({
            date:     s.start || s.date,
            duration: s.active_minutes != null ? fmtHours(s.active_minutes / 60) : '—',
            sections: Array.isArray(s.sections) ? s.sections : [],
          }));
        }

        // Timeline / actividad reciente: API devuelve array de eventos
        if (Array.isArray(p.timeline) && p.timeline.length) {
          uEnriched.timeline = p.timeline.map(ev => ({
            time:    ev.timestamp,
            action:  _apiActionToLocal(ev.action_type),
            section: ev.section || '',
          }));
        }

        // Alertas del usuario desde el perfil real
        if (Array.isArray(p.alerts) && p.alerts.length) {
          uEnriched._api_alerts = p.alerts;
        }

        // Stats enriquecidos si el backend los tiene
        if (p.stats) {
          const st = p.stats;
          uEnriched.sessions    = st.sessions    ?? uBase.sessions;
          uEnriched.active_days = st.active_days ?? uBase.active_days;
          uEnriched.active_hours= st.active_hours?? uBase.active_hours;
          uEnriched.views       = st.views       ?? uBase.views;
          uEnriched.searches    = st.searches    ?? uBase.searches;
          uEnriched.uploads     = st.uploads     ?? uBase.uploads;
          uEnriched.exports     = st.exports     ?? uBase.exports;
          uEnriched.downloads   = st.downloads   ?? uBase.downloads;
          uEnriched.modules     = Array.isArray(st.modules_used_list) ? st.modules_used_list : uBase.modules;
        }
        if (p.adoption_score != null) uEnriched.score = p.adoption_score;
        if (p.risk_level)             uEnriched.risk  = p.risk_level;

        // Sección actual desde live info
        if (p.current_status?.current_section) {
          uEnriched.current_section = p.current_status.current_section;
        }

        // Actualizar el panel ya visible con datos enriquecidos
        populateProfile(uEnriched);
        buildProfileScoreChart(uEnriched);
      })
      .catch(() => { /* silencioso: los datos base ya están visibles */ });
  }

  /** Convierte action_type de la API al formato local usado por evLabel/evIcon */
  function _apiActionToLocal(at) {
    const map = {
      'page_view':    'view',
      'module_visit': 'view',
      'file_upload':  'upload',
      'export':       'export',
      'search':       'search',
      'download':     'download',
    };
    return map[(at || '').toLowerCase()] || 'view';
  }

  /* ── Toast notification ── */
  let _toastTimer = null;
  function showToast(msg, type = 'success') {
    const el  = document.getElementById('trk-toast');
    const msgEl = document.getElementById('trk-toast-msg');
    const iconEl = el && el.querySelector('.toast-icon');
    if (!el || !msgEl) return;
    const icons = { success: 'bi-check-circle-fill', info: 'bi-info-circle-fill', warn: 'bi-exclamation-triangle-fill' };
    if (iconEl) { iconEl.className = `toast-icon bi ${icons[type] || icons.success}`; }
    msgEl.textContent = msg;
    el.className = `trk-toast toast-${type} show`;
    clearTimeout(_toastTimer);
    _toastTimer = setTimeout(() => { el.classList.remove('show'); }, 2800);
  }

  /* ── Group assigner popover ── */
  let _activePopover = null;
  function closeActivePopover() {
    if (_activePopover) { _activePopover.remove(); _activePopover = null; }
  }
  function openGroupAssigner(u) {
    closeActivePopover();
    const groups = [...new Set(MOCK_USERS.map(x => x.group).filter(Boolean))];
    const btn    = document.getElementById('btn-assign-group');
    if (!btn) return;
    const rect   = btn.getBoundingClientRect();
    const pop    = document.createElement('div');
    pop.className = 'grp-popover';
    pop.style.cssText = `top:${rect.bottom + window.scrollY + 4}px;left:${rect.left + window.scrollX}px;`;
    const currentGroup = userGroupOverrides.get(u.id) || u.group;
    pop.innerHTML = groups.map(g => `
      <div class="grp-popover-item${g === currentGroup ? ' selected' : ''}" data-group="${esc(g)}">
        <i class="bi bi-people" style="font-size:11px;"></i>${esc(g)}
      </div>
    `).join('');
    pop.querySelectorAll('.grp-popover-item').forEach(item => {
      item.addEventListener('click', () => {
        const chosen = item.dataset.group;
        userGroupOverrides.set(u.id, chosen);
        const mu = MOCK_USERS.find(x => x.id === u.id);
        if (mu) mu.group = chosen;
        closeActivePopover();
        showToast(`Grupo asignado: ${chosen}`, 'success');
        populateProfile(MOCK_USERS.find(x => x.id === u.id) || u);
      });
    });
    document.body.appendChild(pop);
    _activePopover = pop;
    setTimeout(() => document.addEventListener('click', closeActivePopover, { once: true }), 0);
  }

  /* ── Toggle watch ── */
  function toggleWatchUser(u) {
    const btn = document.getElementById('btn-mark-watch');
    if (trackedUsers.has(u.id)) {
      trackedUsers.delete(u.id);
      if (btn) btn.classList.remove('watch-active');
      showToast(`Seguimiento removido: ${u.username}`, 'info');
    } else {
      trackedUsers.add(u.id);
      if (btn) btn.classList.add('watch-active');
      showToast(`Marcado en seguimiento: ${u.username}`, 'warn');
    }
  }

  function populateProfile(u) {
    const si = scoreInfo(u.score);

    // Avatar
    const avatarEl = document.getElementById('prof-avatar');
    if (avatarEl) {
      avatarEl.textContent = initials(u.username);
      avatarEl.style.background = avatarColor(u.username);
      // Online ring
      const ring = document.createElement('div');
      ring.className = 'prof-status-ring';
      ring.style.background = u.status === 'active' ? '#10b981' : u.status === 'idle' ? '#f59e0b' : '#64748b';
      avatarEl.querySelectorAll('.prof-status-ring').forEach(r => r.remove());
      avatarEl.appendChild(ring);
    }

    setEl('prof-name',  esc(u.username));
    setEl('prof-email', esc(u.email));

    // Chips
    const chips = document.getElementById('prof-chips');
    if (chips) chips.innerHTML = `
      <span class="chip chip-blue" style="text-transform:capitalize;">${esc(u.role)}</span>
      <span class="chip chip-gray">${esc(u.unit)}</span>
      <span class="chip chip-purple">${esc(u.group)}</span>
      ${statusPill(u.status)}
    `;

    // Score
    const scoreVal = document.getElementById('prof-score-val');
    if (scoreVal) { scoreVal.textContent = u.score; scoreVal.style.color = si.color; }
    const scoreFill = document.getElementById('prof-score-fill');
    if (scoreFill) { scoreFill.style.width = u.score + '%'; scoreFill.style.background = si.gradient; }
    setEl('prof-score-label', si.label + ' — ' + u.score + '/100');

    // Stats
    const stats = {
      sessions: fmtInt(u.sessions),
      hours:    fmtHours(u.active_hours),
      days:     fmtInt(u.active_days),
      views:    fmtInt(u.views),
      searches: fmtInt(u.searches),
      uploads:  fmtInt(u.uploads),
      exports:  fmtInt(u.exports),
      downloads:fmtInt(u.downloads),
      modules:  (u.modules || []).length,
    };
    Object.entries(stats).forEach(([k, v]) => setEl(`prof-stat-${k}`, v));

    // Counts
    setEl('prof-search-count',   fmtInt(u.searches));
    setEl('prof-upload-count',   fmtInt(u.uploads));
    setEl('prof-download-count', fmtInt(u.downloads));
    setEl('prof-export-count',   fmtInt(u.exports));

    // Nav stepper (most recent session path)
    const stepperEl = document.getElementById('prof-nav-stepper');
    if (stepperEl) {
      const path = (u.nav_trail || []);
      if (!path.length) {
        stepperEl.innerHTML = '<span style="color:var(--t-muted);font-size:11px;">Sin navegación registrada</span>';
      } else {
        stepperEl.innerHTML = path.map((s, i) => `
          <div class="prof-nav-step">
            <span class="prof-nav-step-chip">${esc(sectionLabel(s))}</span>
            ${i < path.length - 1 ? '<span class="prof-nav-sep">→</span>' : '<span style="font-size:10px;color:var(--t-green);">● actual</span>'}
          </div>
        `).join('');
      }
    }

    // Modules
    const modsEl = document.getElementById('prof-modules');
    if (modsEl) {
      modsEl.innerHTML = (u.modules || []).map(m =>
        `<span class="chip chip-blue"><i class="bi bi-grid-1x2 me-1"></i>${esc(sectionLabel(m))}</span>`
      ).join('');
    }

    // Sessions
    const sessEl = document.getElementById('prof-recent-sessions');
    if (sessEl) {
      const sessions = (u.sessions_detail || []).slice(0, 4);
      sessEl.innerHTML = sessions.map(s => `
        <div class="prof-session-item">
          <div class="prof-session-head">
            <span class="prof-session-date"><i class="bi bi-calendar3 me-1" style="color:var(--t-blue);"></i>${fmtDT(s.date)}</span>
            <span class="prof-session-dur"><i class="bi bi-clock me-1"></i>${s.duration}</span>
          </div>
          <div class="prof-session-sections">
            ${(s.sections || []).map(sec => `<span class="prof-session-section-chip">${esc(sectionLabel(sec))}</span>`).join('')}
          </div>
        </div>
      `).join('') || '<div class="trk-empty" style="padding:20px;">Sin sesiones recientes</div>';
    }

    // Timeline
    const tlEl = document.getElementById('prof-timeline');
    if (tlEl) {
      const evts = (u.timeline || []);
      tlEl.innerHTML = evts.map(ev => `
        <div class="prof-evt">
          <div class="prof-evt-icon" style="background:rgba(${evIconBg(ev.action)});border-color:rgba(${evIconBorder(ev.action)});">
            <i class="bi ${evIcon(ev.action)}"></i>
          </div>
          <div class="prof-evt-body">
            <div class="prof-evt-name">${evLabel(ev.action)} — <strong style="color:#e2e8f0;">${esc(sectionLabel(ev.section))}</strong></div>
            <div class="prof-evt-time">${fmtDT(ev.time)} · hace ${fmtRelative(ev.time)}</div>
          </div>
        </div>
      `).join('') || '<div class="trk-empty" style="padding:20px;">Sin actividad</div>';
    }

    // Alerts for this user — usa alertas reales de API si existen, si no las del mock
    const alertsEl = document.getElementById('prof-alerts');
    if (alertsEl) {
      const apiAlerts = u._api_alerts;
      if (apiAlerts && apiAlerts.length) {
        // Alertas reales desde el endpoint de perfil
        const sevColor = { alta: '#ef4444', media: '#f59e0b', baja: '#3b82f6', info: '#3b82f6' };
        alertsEl.innerHTML = apiAlerts.map(a => `
          <div style="padding:9px 12px;border-left:2px solid;border-left-color:${sevColor[a.severity] || '#3b82f6'};
            background:rgba(255,255,255,.02);border-radius:6px;margin-bottom:6px;">
            <div style="font-size:12px;font-weight:600;color:#e2e8f0;margin-bottom:3px;">
              ${esc(a.reason || a.title || 'Alerta')}
            </div>
            <div style="font-size:11px;color:var(--t-muted);">${esc(a.recommendation || a.message || '')}</div>
          </div>
        `).join('');
      } else {
        const userAlerts = MOCK_ALERTS.filter(a => a.userId === u.id && !resolvedAlerts.has(a.id));
        alertsEl.innerHTML = userAlerts.length
          ? userAlerts.map(a => `
            <div style="padding:9px 12px;border-left:2px solid;border-left-color:${a.severity==='alta'?'#ef4444':a.severity==='media'?'#f59e0b':'#3b82f6'};
              background:rgba(255,255,255,.02);border-radius:6px;margin-bottom:6px;">
              <div style="font-size:12px;font-weight:600;color:#e2e8f0;margin-bottom:3px;">
                <i class="bi ${a.icon} me-1"></i>${esc(a.title)}
              </div>
              <div style="font-size:11px;color:var(--t-muted);">${esc(a.rec)}</div>
            </div>
          `).join('')
          : '<div style="font-size:12px;color:var(--t-muted);padding:10px;">Sin alertas activas.</div>';
      }
    }

    // Wire profile action buttons
    const btnMsg   = document.getElementById('btn-send-message');
    const btnGroup = document.getElementById('btn-assign-group');
    const btnWatch = document.getElementById('btn-mark-watch');

    if (btnMsg) {
      const fresh = btnMsg.cloneNode(true);
      btnMsg.parentNode.replaceChild(fresh, btnMsg);
      fresh.addEventListener('click', e => { e.stopPropagation(); openMessageComposer(u); });
    }
    if (btnGroup) {
      const fresh = btnGroup.cloneNode(true);
      btnGroup.parentNode.replaceChild(fresh, btnGroup);
      fresh.addEventListener('click', e => { e.stopPropagation(); openGroupAssigner(u); });
    }
    if (btnWatch) {
      const fresh = btnWatch.cloneNode(true);
      btnWatch.parentNode.replaceChild(fresh, btnWatch);
      fresh.classList.toggle('watch-active', trackedUsers.has(u.id));
      fresh.addEventListener('click', () => toggleWatchUser(u));
    }
  }

  /* ── Compositor de mensaje al usuario ── */
  function openMessageComposer(u) {
    closeActivePopover();
    const btn = document.getElementById('btn-send-message');
    if (!btn) return;
    const rect = btn.getBoundingClientRect();
    const pop  = document.createElement('div');
    pop.className = 'grp-popover';
    pop.style.cssText = `top:${rect.top + window.scrollY - 140}px;left:${rect.left + window.scrollX}px;min-width:260px;padding:12px;`;
    pop.innerHTML = `
      <div style="font-size:11px;font-weight:700;color:#e2e8f0;margin-bottom:8px;">
        <i class="bi bi-envelope me-1" style="color:var(--t-blue);"></i> Mensaje para ${esc(u.username)}
      </div>
      <textarea id="msg-composer-text" placeholder="Escribí el mensaje…"
        style="width:100%;height:72px;resize:none;background:#0f1729;border:1px solid #1e2d4a;
        border-radius:6px;color:#e2e8f0;font-size:12px;padding:8px;outline:none;"></textarea>
      <div style="display:flex;gap:6px;margin-top:8px;justify-content:flex-end;">
        <button id="msg-cancel-btn" class="btn-prof-action btn-prof-ghost" style="font-size:11px;padding:4px 10px;">Cancelar</button>
        <button id="msg-send-btn"   class="btn-prof-action btn-prof-primary" style="font-size:11px;padding:4px 10px;">
          <i class="bi bi-send"></i> Enviar
        </button>
      </div>`;
    document.body.appendChild(pop);
    _activePopover = pop;
    pop.querySelector('#msg-composer-text')?.focus();
    pop.querySelector('#msg-cancel-btn')?.addEventListener('click', closeActivePopover);
    pop.querySelector('#msg-send-btn')?.addEventListener('click', () => {
      const text = (pop.querySelector('#msg-composer-text')?.value || '').trim();
      if (!text) { showToast('Escribí un mensaje antes de enviar.', 'warn'); return; }
      closeActivePopover();
      showToast(`Mensaje enviado a ${u.username}`, 'success');
      // TODO: conectar con endpoint real de mensajería cuando esté disponible
    });
    setTimeout(() => document.addEventListener('click', e => {
      if (!pop.contains(e.target)) closeActivePopover();
    }, { once: true }), 0);
  }

  function evIcon(a) {
    return { view: 'bi-eye', search: 'bi-search', upload: 'bi-cloud-upload', download: 'bi-download', export: 'bi-box-arrow-up-right' }[a] || 'bi-activity';
  }
  function evIconBg(a)     { return { view:'59,130,246,.1', search:'139,92,246,.1', upload:'16,185,129,.1', download:'245,158,11,.1', export:'6,182,212,.1' }[a] || '100,116,139,.1'; }
  function evIconBorder(a) { return { view:'59,130,246,.2', search:'139,92,246,.2', upload:'16,185,129,.2', download:'245,158,11,.2', export:'6,182,212,.2' }[a] || '100,116,139,.2'; }

  function buildProfileScoreChart(u) {
    if (typeof Chart === 'undefined') return;
    const canvas = document.getElementById('prof-score-chart');
    if (!canvas) return;
    if (profScoreChart) { profScoreChart.destroy(); profScoreChart = null; }
    const history = u.score_history || [u.score];
    const si = scoreInfo(u.score);
    profScoreChart = new Chart(canvas, {
      type: 'line',
      data: {
        labels: history.map((_, i) => `S-${history.length - 1 - i}`).reverse().concat(['Actual']).slice(-history.length),
        datasets: [{
          data: history,
          borderColor: si.color,
          borderWidth: 2,
          fill: true,
          backgroundColor: si.color.replace(')', ',0.1)').replace('rgb','rgba'),
          pointRadius: 3,
          pointBackgroundColor: si.color,
          tension: .4,
        }],
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false }, tooltip: {
          callbacks: { label: ctx => ' Score: ' + ctx.parsed.y + ' pts' },
          backgroundColor: '#0f1729', borderColor: '#1e2d4a', borderWidth: 1,
        }},
        scales: {
          x: { grid: { color: 'rgba(255,255,255,.04)' }, ticks: { color: '#64748b', font: { size: 10 } } },
          y: { min: 0, max: 100, grid: { color: 'rgba(255,255,255,.04)' }, ticks: { color: '#64748b', font: { size: 10 }, callback: v => v + ' pts' } },
        },
      },
    });
  }

  /* ═══════════════════════════════════════════════════════
     FX LAYER - 3D / GSAP / VISUAL POLISH
  ═══════════════════════════════════════════════════════ */
  const FX_STATE = {
    ready: false,
    reducedMotion: window.matchMedia?.('(prefers-reduced-motion: reduce)')?.matches || false,
    lowPower: false,
    vanta: null,
    rafs: new Set(),
    globe: null,
    navGraph: null,
    cursor: { x: 0, y: 0, rx: 0, ry: 0 },
  };

  function initAllEffects() {
    FX_STATE.reducedMotion = window.matchMedia?.('(prefers-reduced-motion: reduce)')?.matches || false;
    FX_STATE.lowPower = detectLowPowerDevice();
    FX_STATE.ready = true;
    document.querySelector('.trk-page')?.classList.toggle('fx-low-power', FX_STATE.lowPower || FX_STATE.reducedMotion);

    if (typeof gsap !== 'undefined' && typeof ScrollTrigger !== 'undefined') {
      gsap.registerPlugin(ScrollTrigger);
    }

    initGlitchTitle();
    initCustomCursor();
    initKpiHolograms();
    initTabFxCapture();
    updateRadarSpeed();
    refreshGlobeUsers();
    animateInitialCascade();
    initVantaBackground();
    initIsometricOffice();

    window.addEventListener('beforeunload', destroyAllEffects, { once: true });
  }

  function canAnimate() {
    return !FX_STATE.reducedMotion && typeof gsap !== 'undefined';
  }

  function canUseThree() {
    return !FX_STATE.reducedMotion && !FX_STATE.lowPower && typeof THREE !== 'undefined';
  }

  function detectLowPowerDevice() {
    const cores = navigator.hardwareConcurrency || 4;
    let limitedGpu = false;
    try {
      const canvas = document.createElement('canvas');
      const gl = canvas.getContext('webgl') || canvas.getContext('experimental-webgl');
      if (!gl) return true;
      const info = gl.getExtension('WEBGL_debug_renderer_info');
      const renderer = info ? String(gl.getParameter(info.UNMASKED_RENDERER_WEBGL)).toLowerCase() : '';
      limitedGpu = /swiftshader|llvmpipe|software|microsoft basic|mesa/i.test(renderer);
    } catch (_) {
      limitedGpu = true;
    }
    return cores < 4 || limitedGpu;
  }

  function animateNumber(id, target, options = {}) {
    const el = document.getElementById(id);
    if (!el) return;
    const decimals = options.decimals ?? 0;
    const suffix = options.suffix || '';
    const format = v => {
      const n = Number(v) || 0;
      if (options.commaDecimal) return fmtDec(n, decimals).replace('.', ',');
      if (decimals > 0) return fmtDec(n, decimals);
      return fmtInt(n);
    };
    const write = v => { el.innerHTML = format(v) + suffix; };
    if (!canAnimate()) {
      write(target);
      return;
    }
    if (el._countTween) el._countTween.kill();
    const counter = { value: 0 };
    el._countTween = gsap.to(counter, {
      value: Number(target) || 0,
      duration: 1.2,
      ease: 'power2.out',
      onUpdate() { write(counter.value); },
      onComplete() {
        write(target);
        el._countTween = null;
      },
    });
  }

  function initVantaBackground() {
    const page = document.querySelector('.trk-page');
    const bg = document.getElementById('vanta-bg-el');
    if (!page || !bg || !canUseThree() || typeof VANTA === 'undefined' || !VANTA.NET) {
      if (page) page.style.background = '#0a0f1a';
      return;
    }
    destroyVantaBackground();
    FX_STATE.vanta = VANTA.NET({
      el: bg,
      THREE,
      mouseControls: true,
      touchControls: true,
      gyroControls: false,
      minHeight: 200,
      minWidth: 200,
      scale: 1,
      scaleMobile: 1,
      color: 0x1e3a5f,
      backgroundColor: 0x0a0f1a,
      points: 9,
      maxDistance: 22,
      spacing: 18,
      showDots: true,
    });
  }

  function destroyVantaBackground() {
    if (FX_STATE.vanta?.destroy) FX_STATE.vanta.destroy();
    FX_STATE.vanta = null;
  }

  function initCustomCursor() {
    const dot = document.getElementById('trk-cursor');
    const ring = document.getElementById('trk-cursor-ring');
    if (!dot || !ring || FX_STATE.reducedMotion || !window.matchMedia?.('(hover:hover)')?.matches) return;

    window.addEventListener('mousemove', e => {
      FX_STATE.cursor.x = e.clientX;
      FX_STATE.cursor.y = e.clientY;
    }, { passive: true });

    document.addEventListener('mouseover', e => {
      const hov = !!e.target.closest('button,a,input,select,textarea,[role="button"],.kpi-card,.alert-card,.globe-user-item,tr');
      dot.classList.toggle('hov', hov);
      ring.classList.toggle('hov', hov);
    }, { passive: true });

    const loop = () => {
      FX_STATE.cursor.rx += (FX_STATE.cursor.x - FX_STATE.cursor.rx) * .18;
      FX_STATE.cursor.ry += (FX_STATE.cursor.y - FX_STATE.cursor.ry) * .18;
      dot.style.left = FX_STATE.cursor.x + 'px';
      dot.style.top = FX_STATE.cursor.y + 'px';
      ring.style.left = FX_STATE.cursor.rx + 'px';
      ring.style.top = FX_STATE.cursor.ry + 'px';
      const raf = requestAnimationFrame(loop);
      FX_STATE.rafs.add(raf);
    };
    loop();
  }

  function initKpiHolograms() {
    $$('.kpi-card').forEach(card => {
      if (!card.querySelector('.kpi-shimmer')) {
        const shimmer = document.createElement('div');
        shimmer.className = 'kpi-shimmer';
        card.prepend(shimmer);
      }
      card.addEventListener('mousemove', e => {
        if (FX_STATE.reducedMotion) return;
        const r = card.getBoundingClientRect();
        const px = (e.clientX - r.left) / r.width;
        const py = (e.clientY - r.top) / r.height;
        const rotY = (px - .5) * 24;
        const rotX = (.5 - py) * 24;
        card.style.setProperty('--mx', `${px * 100}%`);
        card.style.setProperty('--my', `${py * 100}%`);
        card.style.transform = `perspective(900px) rotateX(${rotX}deg) rotateY(${rotY}deg) translateY(-3px)`;
      });
      card.addEventListener('mouseleave', () => {
        card.style.transition = 'transform .5s ease';
        card.style.transform = '';
        setTimeout(() => { card.style.transition = ''; }, 520);
      });
    });
    syncKpiGlow();
  }

  function syncKpiGlow() {
    $$('.kpi-card').forEach(card => {
      const trend = card.querySelector('.kpi-trend');
      const color = trend?.classList.contains('down') ? '239,68,68' : trend?.classList.contains('up') ? '16,185,129' : '59,130,246';
      card.style.boxShadow = `0 18px 48px rgba(0,0,0,.22), 0 0 24px rgba(${color},.12)`;
    });
  }

  function initGlitchTitle() {
    const title = document.getElementById('trk-main-title');
    if (!title || FX_STATE.reducedMotion || title.dataset.glitched) return;
    title.dataset.glitched = '1';
    const layer = document.createElement('span');
    layer.className = 'glitch-layer';
    layer.textContent = title.textContent.trim();
    title.appendChild(layer);
    setTimeout(() => layer.remove(), 700);
  }

  function animateInitialCascade() {
    if (!canAnimate()) return;
    gsap.from('.trk-header,.trk-filters,.kpi-card,.trk-tabs,.tab-pane.active > *', {
      opacity: 0,
      y: -18,
      duration: .45,
      stagger: .08,
      ease: 'power2.out',
    });
  }

  function initTabFxCapture() {
    const tabs = document.querySelector('.trk-tabs');
    if (!tabs || tabs.dataset.fxBound) return;
    tabs.dataset.fxBound = '1';
    tabs.style.position = 'relative';
    const indicator = document.createElement('div');
    indicator.id = 'tab-fx-indicator';
    indicator.style.cssText = 'position:absolute;bottom:0;height:2px;background:#3b82f6;border-radius:2px;box-shadow:0 0 14px rgba(59,130,246,.7);pointer-events:none;';
    tabs.appendChild(indicator);
    moveTabIndicator(tabs.querySelector('.trk-tab.active') || tabs.querySelector('.trk-tab'));

    tabs.querySelectorAll('.trk-tab').forEach(btn => {
      btn.addEventListener('click', e => {
        const targetId = btn.dataset.usageTabTarget?.replace('#', '');
        if (!targetId || targetId === currentTab || !canAnimate()) {
          moveTabIndicator(btn);
          return;
        }
        e.preventDefault();
        e.stopImmediatePropagation();
        morphToTab(btn, targetId);
      }, true);
    });
    window.addEventListener('resize', () => moveTabIndicator(document.querySelector('.trk-tab.active')), { passive: true });
  }

  function moveTabIndicator(btn) {
    const indicator = document.getElementById('tab-fx-indicator');
    const tabs = document.querySelector('.trk-tabs');
    if (!indicator || !tabs || !btn) return;
    const tr = tabs.getBoundingClientRect();
    const br = btn.getBoundingClientRect();
    const vars = { left: br.left - tr.left, width: br.width, duration: .35, ease: 'power3.inOut' };
    if (canAnimate()) gsap.to(indicator, vars);
    else {
      indicator.style.left = vars.left + 'px';
      indicator.style.width = vars.width + 'px';
    }
  }

  function morphToTab(btn, targetId) {
    const currentPanel = document.getElementById(currentTab);
    const nextPanel = document.getElementById(targetId);
    if (!currentPanel || !nextPanel) return;
    $$('.trk-tab').forEach(b => b.classList.remove('active'));
    btn.classList.add('active');
    moveTabIndicator(btn);
    gsap.to(currentPanel, {
      opacity: 0,
      y: -20,
      duration: .2,
      ease: 'power2.in',
      onComplete() {
        currentPanel.style.display = 'none';
        currentPanel.classList.remove('show', 'active');
        nextPanel.style.display = 'block';
        nextPanel.classList.add('show', 'active');
        gsap.set(nextPanel, { opacity: 0, y: 20 });
        currentTab = targetId;
        gsap.to(nextPanel, { opacity: 1, y: 0, duration: .3, ease: 'power2.out' });
        gsap.from($$('.trk-card,.alert-card,.globe-section,tbody tr', nextPanel), {
          opacity: 0,
          y: 18,
          duration: .28,
          stagger: .05,
          ease: 'power2.out',
        });
        if (targetId === 'usage-tab-summary') {
          if (!chartsBuilt.summary) setTimeout(buildSummaryCharts, 50);
          else setTimeout(() => {
            ['usage-chart-weekday','usage-chart-roles','usage-chart-heatmap',
             'usage-chart-sections','usage-chart-adoption','usage-chart-donut'].forEach(id => {
              const el = document.getElementById(id);
              if (el && el._chart) el._chart.updateOptions({}, false, false);
            });
          }, 50);
        }
        if (targetId === 'usage-tab-live') setTimeout(initIsometricOffice, 60);
      },
    });
  }

  function navNodeKey(raw) {
    if (!raw) return '';
    return String(raw).toLowerCase().trim().replace(/-/g, '_');
  }

  /* Normaliza cadena quitando acentos/diacríticos para comparación robusta */
  function _noAccent(s) {
    return s.normalize('NFD').replace(/[̀-ͯ]/g, '');
  }

  /*
   * Reverse-label lookup: cubre los casos donde el servidor devuelve la etiqueta
   * legible (_map_section_name) en lugar de la clave técnica.
   * Lookup normalizado sin acentos para tolerancia de variantes.
   */
  const _LABEL_BLOCK = {
    // Mercado Público y sub-secciones
    'mercado publico':                          'mercado_publico',
    'mercado publico  inicio':                  'mercado_publico',   // "Mercado Público — Inicio"
    'inicio  mercado publico':                  'mercado_publico',
    'mercado publico home':                     'mercado_publico',
    'mercado publico inicio':                   'mercado_publico',   // variante espacio simple
    'oportunidades  publico':                   'mercado_publico',
    'oportunidades':                            'mercado_publico',
    'buscador de oportunidades':                'mercado_publico',
    'analisis de dimensiones':                  'mercado_publico',
    'dimensiones de oportunidades':             'mercado_publico',
    'lectura de pliegos':                       'mercado_publico',
    'modulo de pliegos':                        'mercado_publico',
    'visor de pliegos':                         'mercado_publico',
    'detalle de pliego':                        'mercado_publico',
    'pliegos':                                  'mercado_publico',
    'comparativa de mercado':                   'mercado_publico',
    'tablero de comparativa':                   'mercado_publico',
    'vistas guardadas':                         'mercado_publico',
    'descargas':                                'mercado_publico',
    'reporte de proceso':                       'mercado_publico',
    'informes':                                 'mercado_publico',
    'comentarios':                              'mercado_publico',
    'consulta de clientes':                     'mercado_publico',
    'cargas':                                   'mercado_publico',
    'cargas historial':                         'mercado_publico',
    'nueva carga':                              'mercado_publico',
    'edicion de carga':                         'mercado_publico',
    'fuentes externas':                         'mercado_publico',
    'reporte de perfiles  mercado publico':     'mercado_publico',
    'reporte de perfiles  mercado privado':     'mercado_privado',
    'reporte de perfiles':                      'mercado_publico',
    'perfiles de clientes':                     'mercado_publico',
    'perfiles':                                 'mercado_publico',
    // Mercado Privado y sub-secciones
    'mercado privado':                          'mercado_privado',
    'inicio  mercado privado':                  'mercado_privado',
    'mercado privado home':                     'mercado_privado',
    'dimensionamiento':                         'mercado_privado',
    'dimensiones  privado':                     'mercado_privado',
    'helpdesk  mercado publico':                'sic',
    // Forecast
    'proyecciones y forecast':                  'forecast',
    'panel de forecast':                        'forecast',
    'forecast':                                 'forecast',
    // S.I.C. (todos sus sub-módulos apuntan al módulo raíz)
    'seguimiento de usuarios':                  'sic',
    'panel s.i.c.':                             'sic',
    's.i.c. general':                           'sic',
    's.i.c general':                            'sic',    // sin punto final
    's.i.c':                                    'sic',
    'api tracking (interno)':                   'sic',
    'api tracking interno':                     'sic',
    'panel live':                               'sic',
    'notificaciones':                           'sic',
    'gestion de usuarios':                      'sic',
    'usuarios':                                 'sic',
    'mesa de ayuda':                            'sic',
    'mesa de ayuda mercado publico':            'sic',
    'mesa de ayuda mercado privado':            'sic',
    'gestion de contrasenas':                   'sic',
    'reseteo de contrasenas':                   'sic',
    'administracion de reseteos':               'sic',
    'administracion':                           'sic',
    'grupos':                                   'sic',
    // Panel principal / Centro de Mercados → SIC como módulo de control
    'panel principal':                          'sic',
    'inicio  siem':                             'sic',
    'centro de mercados':                       'sic',
    'inicio  mercados':                         'sic',
    'inicio mercados':                          'sic',
    // Auth / Sistema — no son módulos navegables, no se asignan a sala
    'inicio de sesion':                         null,
    'cierre de sesion':                         null,
    'inicio de sesion  siem':                   null,
    // Tablero → sub de Comparativa → Mercado Público
    'tablero':                                  'mercado_publico',
    'tablero de comparativa':                   'mercado_publico',
    // Inicio genérico sin contexto → SIC (hub central)
    'inicio':                                   'sic',
    'inicio general':                           'sic',
  };

  /* ═══════════════════════════════════════════════════════
     CAMPUS — Oficina isométrica jerárquica de 2 niveles
     Nivel 1: 12 módulos del sistema en grilla 6w × 5d
     Nivel 2: sub-secciones del módulo seleccionado
     Proyección iso: gx/gy → canvas x/y
     Algoritmo del pintor: ordenar por gy + sd
  ═══════════════════════════════════════════════════════ */

  /* ── Nivel 1: campus — 4 módulos principales de SIEM ── */
  const CAMPUS_ROOMS = [
    { key: 'mercado_publico', label: 'Mercado Público',  sub: 'Módulo',         gx: 0, gy: 0, sw: 3, sd: 1 },
    { key: 'mercado_privado', label: 'Mercado Privado',  sub: 'Módulo',         gx: 3, gy: 0, sw: 3, sd: 1 },
    { key: 'forecast',        label: 'Forecast',         sub: 'Proyecciones',   gx: 0, gy: 1, sw: 3, sd: 1 },
    { key: 'sic',             label: 'S.I.C.',           sub: 'Control Center', gx: 3, gy: 1, sw: 3, sd: 1, main: true },
  ];

  const CAMPUS_LINKS = [
    ['sic', 'mercado_publico'],
    ['sic', 'mercado_privado'],
    ['sic', 'forecast'],
  ];

  /* ── Nivel 2: apartados internos por módulo ── */
  const LEVEL2_SUBS = {
    'mercado_publico': [
      { key: 'mercado_publico_home',             label: 'Inicio',             sub: 'Público',    gx: 0, gy: 0, sw: 2, sd: 1 },
      { key: 'comparativa_mercado',              label: 'Comparativa',        sub: 'Análisis',   gx: 2, gy: 0, sw: 4, sd: 1, main: true },
      { key: 'lectura_pliegos',                  label: 'Lectura de Pliegos', sub: 'Documentos', gx: 0, gy: 1, sw: 2, sd: 1 },
      { key: 'mercado_publico_oportunidades',    label: 'Oportunidades',      sub: 'Búsqueda',   gx: 2, gy: 1, sw: 2, sd: 1 },
      { key: 'mercado_publico_reporte_perfiles', label: 'Reporte de Perfiles',sub: 'Informes',   gx: 4, gy: 1, sw: 2, sd: 1 },
      { key: 'mercado_publico_helpdesk',         label: 'Mesa de Ayuda',      sub: 'Soporte',    gx: 0, gy: 2, sw: 3, sd: 1 },
      { key: 'auth_password_mp',                 label: 'Mi Contraseña',      sub: 'Acceso',     gx: 3, gy: 2, sw: 3, sd: 1 },
    ],
    'mercado_privado': [
      { key: 'mercado_privado_home',             label: 'Inicio',             sub: 'Privado',    gx: 0, gy: 0, sw: 2, sd: 1 },
      { key: 'dimensionamiento',                 label: 'Dimensionamiento',   sub: 'Análisis',   gx: 2, gy: 0, sw: 4, sd: 1, main: true },
      { key: 'mercado_privado_reporte_perfiles', label: 'Reporte de Perfiles',sub: 'Informes',   gx: 0, gy: 1, sw: 4, sd: 1 },
      { key: 'auth_password',                    label: 'Mi Contraseña',      sub: 'Acceso',     gx: 4, gy: 1, sw: 2, sd: 1 },
    ],
    'forecast': [
      { key: 'forecast',        label: 'Forecast',          sub: 'Panel',    gx: 0, gy: 0, sw: 4, sd: 1, main: true },
      { key: 'forecast_widget', label: 'Panel de Forecast', sub: 'Detalle',  gx: 4, gy: 0, sw: 2, sd: 1 },
    ],
    'sic': [
      { key: 'sic_general',         label: 'Panel S.I.C.',           sub: 'Hub',       gx: 0, gy: 0, sw: 6, sd: 1, main: true },
      { key: 'sic_helpdesk',        label: 'Mesa de Ayuda',          sub: 'Soporte',   gx: 0, gy: 1, sw: 2, sd: 1 },
      { key: 'sic_tracking',        label: 'Seguimiento de Usuarios',sub: 'Monitoreo', gx: 2, gy: 1, sw: 2, sd: 1 },
      { key: 'sic_users',           label: 'Usuarios',               sub: 'Gestión',   gx: 4, gy: 1, sw: 2, sd: 1 },
      { key: 'sic_password_resets', label: 'Contraseñas',            sub: 'Accesos',   gx: 0, gy: 2, sw: 3, sd: 1 },
      { key: 'sic_admin',           label: 'Administración',         sub: 'Sistema',   gx: 3, gy: 2, sw: 3, sd: 1 },
    ],
  };

  /* ── Mapeo sección técnica → apartado (Nivel 2) dentro de su módulo ── */
  const SECTION_TO_L2 = {
    'mercado_publico': {
      'mercado_publico_home':            'mercado_publico_home',
      'comparativa_mercado':             'comparativa_mercado',
      'cargas_nueva':                    'comparativa_mercado',
      'cargas_historial':                'comparativa_mercado',
      'cargas_edicion':                  'comparativa_mercado',
      'fuentes_externas':                'comparativa_mercado',
      'tablero_comparativa':             'comparativa_mercado',
      'vistas_guardadas':                'comparativa_mercado',
      'descargas':                       'comparativa_mercado',
      'informes':                        'comparativa_mercado',
      'reporte_proceso':                 'comparativa_mercado',
      'comentarios':                     'comparativa_mercado',
      'lectura_pliegos':                 'lectura_pliegos',
      'pliego_widget':                   'lectura_pliegos',
      'pliego_detalle':                  'lectura_pliegos',
      'mercado_publico_oportunidades':   'mercado_publico_oportunidades',
      'oportunidades_buscador':          'mercado_publico_oportunidades',
      'oportunidades_dimensiones':       'mercado_publico_oportunidades',
      'oportunidades':                   'mercado_publico_oportunidades',
      'mercado_publico_reporte_perfiles':'mercado_publico_reporte_perfiles',
      'reporte_perfiles':                'mercado_publico_reporte_perfiles',
      'mercado_publico_helpdesk':        'mercado_publico_helpdesk',
    },
    'mercado_privado': {
      'mercado_privado_home':            'mercado_privado_home',
      'dimensionamiento':                'dimensionamiento',
      'mercado_privado_dimensiones':     'dimensionamiento',
      'mercado_privado_reporte_perfiles':'mercado_privado_reporte_perfiles',
      'auth_password':                   'auth_password',
    },
    'forecast': {
      'forecast':                        'forecast',
      'forecast_widget':                 'forecast_widget',
    },
    'sic': {
      'sic_general':                     'sic_general',
      'sic_home':                        'sic_general',
      'sic_tracking':                    'sic_tracking',
      'sic_tracking_api':                'sic_tracking',
      'sic_helpdesk':                    'sic_helpdesk',
      'sic_helpdesk_tickets':            'sic_helpdesk',
      'sic_users':                       'sic_users',
      'sic_usuarios':                    'sic_users',
      'sic_password_resets':             'sic_password_resets',
      'admin_password_resets':           'sic_admin',
      'administracion':                  'sic_admin',
      'grupos':                          'sic_users',
      'notificaciones':                  'sic_general',
      'dashboard':                       'sic_general',
      'markets_home':                    'sic_general',
      'home':                            'sic_general',
    },
  };

  /* ── Etiquetas de subapartado (Nivel 3) para el breadcrumb ── */
  const LEVEL3_LABELS = {
    // Mercado Público › Comparativa
    'comparativa_mercado':             'Vista Principal',
    'cargas_nueva':                    'Nueva Carga',
    'cargas_historial':                'Historial de Cargas',
    'cargas_edicion':                  'Edición de Carga',
    'fuentes_externas':                'Otras Fuentes',
    'tablero_comparativa':             'Tablero',
    'vistas_guardadas':                'Vistas Guardadas',
    // Mercado Público › Oportunidades
    'oportunidades':                   'Panel',
    'oportunidades_buscador':          'Buscador',
    'oportunidades_dimensiones':       'Dimensiones',
    // Mercado Público › Lectura de Pliegos
    'lectura_pliegos':                 'Listado',
    'pliego_widget':                   'Visor de Pliegos',
    'pliego_detalle':                  'Detalle del Pliego',
    // Mercado Privado › Dimensionamiento
    'dimensionamiento':                'Vista General',
    'mercado_privado_dimensiones':     'Vista General',
    // S.I.C. › Seguimiento de Usuarios
    'sic_tracking':                    'Monitoreo en Vivo',
    'sic_tracking_api':                'API Tracking',
    // S.I.C. › Usuarios
    'sic_users':                       'Gestión de Usuarios',
    'sic_usuarios':                    'Gestión de Usuarios',
    'grupos':                          'Grupos',
    // S.I.C. › Contraseñas
    'sic_password_resets':             'Solicitudes de Contraseña',
    'admin_password_resets':           'Administración de Reseteos',
    // S.I.C. › Mesa de Ayuda
    'sic_helpdesk':                    'Soporte',
    'sic_helpdesk_tickets':            'Tickets',
  };

  /* ── Mapeo sección → módulo principal (4 claves de CAMPUS_ROOMS) ── */
  function blockForSection(raw) {
    if (!raw) return null;

    // 1. Lookup por etiqueta legible — doble normalización para cubrir variantes
    //    a) versión con doble espacio (formato histórico del replace)
    const labelKeyDS = _noAccent(String(raw).toLowerCase().trim()).replace(/\s*[—–-]\s*/g, '  ');
    if (_LABEL_BLOCK[labelKeyDS] !== undefined) return _LABEL_BLOCK[labelKeyDS];
    //    b) versión normalizeLabelKey (espacio simple, sin puntos)
    const labelKey = normalizeLabelKey(raw);
    if (_LABEL_BLOCK[labelKey] !== undefined) return _LABEL_BLOCK[labelKey];

    // 2. Secciones técnicas conocidas — no son módulos navegables, devolver null sin warning
    if (KNOWN_TECHNICAL_LABELS.has(labelKey)) return null;

    // 3. Clave técnica: normalizar también em-dash y espacios a guion bajo
    const key = _noAccent(normalizeSectionKey(raw))
      .replace(/\s*[—–]\s*/g, '_')
      .replace(/\s+/g, '_')
      .replace(/\./g, '')
      .replace(/_+/g, '_');

    if (isGenericSectionLabel(key)) return null;

    // ── S.I.C. — todos sus apartados apuntan al módulo raíz ─────────────────
    if (key.startsWith('sic') || key.includes('seguimiento')) return 'sic';
    if (key.includes('tab_live') || key.includes('tab_summary') ||
        key.includes('tab_by_user') || key.includes('tab_alerts') ||
        key === 'sic_config') return 'sic';
    if (key === 'dashboard' || key.startsWith('markets') || key === 'home') return 'sic';
    if (key.includes('administracion') || key.startsWith('admin_') ||
        key === 'grupos' || key.includes('notificacion')) return 'sic';
    if (key.includes('contrasena')) return 'sic';

    // ── Mercado Privado ──────────────────────────────────────────────────────
    if (key === 'mercado_privado_reporte_perfiles') return 'mercado_privado';
    if (key === 'dimensionamiento' || key === 'mercado_privado_dimensiones') return 'mercado_privado';
    if (key === 'auth_password') return 'mercado_privado';
    if (key.startsWith('mercado_privado') || key.includes('mercado_privad')) return 'mercado_privado';

    // ── Mercado Público ──────────────────────────────────────────────────────
    if (key === 'mercado_publico_reporte_perfiles') return 'mercado_publico';
    if (key === 'lectura_pliegos' || key.startsWith('pliego')) return 'mercado_publico';
    if (key.startsWith('oportunidades') || key.startsWith('oportunidad')) return 'mercado_publico';
    if (key === 'cargas' || key === 'cargas_historial' || key === 'cargas_nueva' ||
        key === 'cargas_edicion' || key === 'fuentes_externas' || key.startsWith('carga')) return 'mercado_publico';
    if (key.startsWith('comparativa') || key.startsWith('web_comparativa') ||
        key.includes('tablero') || key.includes('vista_guardada') ||
        key.includes('descarga') || key.includes('informe') ||
        key.includes('reporte_proceso') || key.includes('comentario') ||
        key.includes('cliente')) return 'mercado_publico';
    if (key.startsWith('mercado_publico') || key.includes('mercado_public')) return 'mercado_publico';
    if (key === 'reporte_perfiles' || key === 'perfiles' || key.startsWith('reporte_perfil') ||
        key.startsWith('perfil')) return 'mercado_publico';

    // ── Forecast ────────────────────────────────────────────────────────────
    if (key.startsWith('forecast') || key.includes('proyeccion')) return 'forecast';

    // Solo advertir si la sección es genuinamente desconocida
    if (window.console && console.warn) {
      console.warn('[tracking] Sección sin módulo asignado:', raw, '→', key);
    }
    return null;
  }

  /* Mapeo sección → apartado (Nivel 2) dentro de su módulo */
  function subRoomForSection(moduleKey, raw) {
    if (!raw || !moduleKey) return null;
    const subs = LEVEL2_SUBS[moduleKey];
    if (!subs) return null;
    const key = _noAccent(normalizeSectionKey(raw));
    // 1. Coincidencia directa con la clave de un apartado L2
    if (subs.find(s => s.key === key)) return key;
    // 2. Mapeo explícito en SECTION_TO_L2
    const mapped = (SECTION_TO_L2[moduleKey] || {})[key];
    if (mapped && subs.find(s => s.key === mapped)) return mapped;
    // 3. Coincidencia parcial
    const partial = subs.find(s => key.includes(s.key) || s.key.includes(key));
    if (partial) return partial.key;
    // 4. Sala principal del módulo
    const main = subs.find(s => s.main);
    return main ? main.key : (subs[0]?.key || null);
  }

  /* Breadcrumb completo: Módulo › Apartado › Subapartado */
  function userBreadcrumb(u) {
    if (!u || !u.current_section) return null;
    const sec = _noAccent(normalizeSectionKey(u.current_section));
    const moduleKey = blockForSection(u.current_section);
    if (!moduleKey) return sectionLabel(u.current_section) || null;

    const moduleRoom = CAMPUS_ROOMS.find(r => r.key === moduleKey);
    const modLabel   = moduleRoom ? moduleRoom.label : moduleKey;

    const l2Key  = subRoomForSection(moduleKey, u.current_section);
    const l2Subs = LEVEL2_SUBS[moduleKey] || [];
    const l2Room = l2Subs.find(s => s.key === l2Key);
    const l2Label = l2Room ? l2Room.label : null;

    if (!l2Label) return modLabel;

    const l3Label = LEVEL3_LABELS[sec] || null;
    // Solo mostrar L3 si aporta información distinta al L2
    if (l3Label && l3Label !== l2Room.label) {
      return `${modLabel} › ${l2Label} › ${l3Label}`;
    }
    return `${modLabel} › ${l2Label}`;
  }

  function blockRouteForUser(user) {
    const route = (user.nav_trail || []).map(blockForSection);
    if (user.current_section) route.push(blockForSection(user.current_section));
    return [...new Set(route.filter(Boolean))];
  }

  function userBelongsToInterfaceBlock(user, blockKey) {
    if (!blockKey) return true;
    return blockForSection(user.current_section) === blockKey || blockRouteForUser(user).includes(blockKey);
  }

  function timeInCurrentSection(user) {
    const current = navNodeKey(user.current_section);
    const match = (user.timeline || []).find(ev => navNodeKey(ev.section) === current);
    return match ? fmtRelative(match.time) : fmtRelative(user.last_ping || user.last_access);
  }

  /* blockKey = módulo (Nivel 1) o sub-sala (Nivel 2 cuando l2Module está definido) */
  function refreshGlobeUsers(blockKey = null, selectedUid = null, l2Module = null) {
    const box = document.getElementById('globe-user-items');
    if (!box) return;
    const users = MOCK_USERS.filter(u => u.status !== 'offline');
    let shown;
    if (l2Module && blockKey) {
      shown = users.filter(u =>
        blockForSection(u.current_section) === l2Module &&
        subRoomForSection(l2Module, u.current_section) === blockKey
      );
    } else if (blockKey) {
      shown = users.filter(u => userBelongsToInterfaceBlock(u, blockKey));
    } else {
      shown = users;
    }
    box.innerHTML = shown.map(u => {
      const sdotCls = u.status === 'active' ? 'sdot-active' : u.status === 'idle' ? 'sdot-idle' : 'sdot-offline';
      const statusLabel = u.status === 'active' ? 'Activo' : u.status === 'idle' ? 'Idle' : 'Offline';
      return `
      <div class="globe-user-item${selectedUid === u.id ? ' selected' : ''}" data-uid="${u.id}">
        <div class="globe-u-name"><span class="sdot ${sdotCls}"></span>${esc(u.username)}</div>
        <div class="globe-u-sec">${esc(userBreadcrumb(u) || sectionLabel(u.current_section))}</div>
        <div class="globe-u-meta">
          <span>${esc(u.role)}</span><span>${statusLabel}</span>
          <span>${esc(u.unit)}</span><span>${esc(timeInCurrentSection(u))}</span>
          <span style="grid-column:1 / -1;">${esc(u.last_action || 'Sin acción registrada')}</span>
        </div>
      </div>`;
    }).join('') || `
      <div class="trk-empty" style="padding:18px;">
        <div class="trk-empty-title">Sin usuarios en esta área</div>
        <div class="trk-empty-sub">Hacé clic en una sala de la oficina.</div>
      </div>`;
    box.querySelectorAll('.globe-user-item').forEach(el => {
      el.addEventListener('click', () => selectNavigationUser(Number(el.dataset.uid)));
    });
  }

  /* Construye array de bloques con usuarios/alertas asignados (Nivel 1) */
  function buildCampusData() {
    const blocks = CAMPUS_ROOMS.map(def => ({ ...def, users: [], alerts: 0 }));
    const byKey = new Map(blocks.map(b => [b.key, b]));
    MOCK_USERS.filter(u => u.status !== 'offline').forEach(u => {
      const block = byKey.get(blockForSection(u.current_section));
      if (block) block.users.push(u);
    });
    MOCK_ALERTS.filter(a => !resolvedAlerts.has(a.id)).forEach(a => {
      const u = MOCK_USERS.find(x => x.id === a.userId);
      if (!u?.current_section) return;
      const block = byKey.get(blockForSection(u.current_section));
      if (block) block.alerts += a.severity === 'alta' ? 2 : 1;
    });
    return blocks;
  }

  /* Construye datos de sub-salas para un módulo (Nivel 2) */
  function buildL2Data(moduleKey) {
    const subs = LEVEL2_SUBS[moduleKey] || [];
    const data = {};
    subs.forEach(s => { data[s.key] = { users: [], alerts: 0 }; });
    MOCK_USERS
      .filter(u => u.status !== 'offline' && blockForSection(u.current_section) === moduleKey)
      .forEach(u => {
        const sk = subRoomForSection(moduleKey, u.current_section);
        if (sk && data[sk]) data[sk].users.push(u);
      });
    MOCK_ALERTS.filter(a => !resolvedAlerts.has(a.id)).forEach(a => {
      const u = MOCK_USERS.find(x => x.id === a.userId);
      if (!u?.current_section || blockForSection(u.current_section) !== moduleKey) return;
      const sk = subRoomForSection(moduleKey, u.current_section);
      if (sk && data[sk]) data[sk].alerts += a.severity === 'alta' ? 2 : 1;
    });
    return data;
  }

  function interfaceBlockState(block) {
    if (block.alerts > 0) return { color: '#ef4444', label: 'alerta / fricción' };
    if (block.users.some(u => u.status === 'idle')) return { color: '#f59e0b', label: 'idle' };
    if (block.users.some(u => u.status === 'active')) return { color: '#10b981', label: 'activo' };
    return { color: '#3b82f6', label: 'normal' };
  }

  /* ═══════════════════════════════════════════════════════
     ISOMETRIC OFFICE — 2D Canvas renderer, 2 niveles
     Nivel 1: campus (12 módulos), Nivel 2: detalle módulo
  ═══════════════════════════════════════════════════════ */

  function selectNavigationNode(key) {
    if (FX_STATE.globe) {
      FX_STATE.globe.selectedRoom = key;
      FX_STATE.globe.selectedUser = null;
    }
    refreshGlobeUsers(key, null);
    const hd = document.querySelector('.globe-list-hd');
    const room = CAMPUS_ROOMS.find(r => r.key === key);
    if (hd && room) {
      hd.innerHTML = `<div class="sdot sdot-active"></div> ${esc(room.label)}`;
    }
  }

  function selectNavigationUser(uid) {
    const u = MOCK_USERS.find(x => x.id === uid);
    if (!u) return;
    const key = blockForSection(u.current_section);
    if (FX_STATE.globe) {
      FX_STATE.globe.selectedRoom = key;
      FX_STATE.globe.selectedUser = uid;
    }
    refreshGlobeUsers(key, uid);
    openProfile(uid);
  }

  function initIsometricOffice() {
    const canvas = document.getElementById('globe-canvas');
    const wrap = canvas?.parentElement;
    if (!canvas || !wrap || FX_STATE.globe) return;

    const DPR    = Math.min(window.devicePixelRatio || 1, 2);
    const WALL_H = 22;

    /* ── Estado del nivel (null = campus, string = módulo en detalle) ── */
    let drillDown   = null;
    let transAlpha  = 1;
    let transTarget = 1;

    /* ── Parámetros de proyección (recalculados al cambiar nivel/tamaño) ── */
    let W       = wrap.clientWidth || 640;
    let CANVAS_H, CW, CH, OX, OY;

    canvas.width  = W * DPR;
    canvas.height = 420 * DPR;
    canvas.style.height = '420px';
    const ctx = canvas.getContext('2d');
    ctx.scale(DPR, DPR);

    function setLevelParams() {
      if (drillDown === null) {
        const maxGx = CAMPUS_ROOMS.reduce((m, r) => Math.max(m, r.gx + r.sw), 6);
        const maxGy = CAMPUS_ROOMS.reduce((m, r) => Math.max(m, r.gy + r.sd), 3);
        CANVAS_H = Math.max(260, 80 + (maxGx + maxGy + 1) * 28);
        CW = Math.max(30, Math.min(56, W / (maxGx + 2)));
        OY = 52;
      } else {
        const subs = LEVEL2_SUBS[drillDown] || [];
        const maxGx = subs.reduce((m, s) => Math.max(m, s.gx + s.sw), 6);
        const maxGy = subs.reduce((m, s) => Math.max(m, s.gy + s.sd), 2);
        CANVAS_H = Math.max(260, 80 + (maxGx + maxGy + 1) * 26);
        CW = Math.max(34, Math.min(58, W / (maxGx + 3)));
        OY = 52;
      }
      CH = CW * 0.5;
      OX = W * 0.5;
      canvas.width  = W * DPR;
      canvas.height = CANVAS_H * DPR;
      canvas.style.height = CANVAS_H + 'px';
      ctx.setTransform(DPR, 0, 0, DPR, 0, 0);
    }
    setLevelParams();

    /* ── Datos de actividad ── */
    let campusData = {};
    let l2Data     = {};

    function syncData() {
      buildCampusData().forEach(b => { campusData[b.key] = { users: b.users, alerts: b.alerts }; });
      if (drillDown) l2Data = buildL2Data(drillDown);
    }
    syncData();

    /* ── Proyección isométrica ── */
    function iso(gx, gy) {
      return { x: OX + (gx - gy) * CW, y: OY + (gx + gy) * CH };
    }

    function roomCorners(room, lift) {
      const l = lift || 0, { gx, gy, sw, sd } = room;
      const A = iso(gx,      gy     ), B = iso(gx + sw, gy     );
      const C = iso(gx + sw, gy + sd), D = iso(gx,      gy + sd);
      return {
        A: { x: A.x, y: A.y - l }, B: { x: B.x, y: B.y - l },
        C: { x: C.x, y: C.y - l }, D: { x: D.x, y: D.y - l },
      };
    }

    function paraPath(pts) {
      ctx.beginPath(); ctx.moveTo(pts[0].x, pts[0].y);
      for (let i = 1; i < pts.length; i++) ctx.lineTo(pts[i].x, pts[i].y);
      ctx.closePath();
    }

    function ptInPara(px, py, A, B, C, D) {
      const pts = [A, B, C, D]; let inside = false;
      for (let i = 0, j = 3; i < 4; j = i++) {
        const xi = pts[i].x, yi = pts[i].y, xj = pts[j].x, yj = pts[j].y;
        if (((yi > py) !== (yj > py)) && (px < (xj - xi) * (py - yi) / (yj - yi) + xi))
          inside = !inside;
      }
      return inside;
    }

    /* ── Posición de avatar dentro de la sala ── */
    function dotPos(room, idx, total, lift) {
      const { gx, gy, sw } = room;
      const A = iso(gx, gy), B = iso(gx + sw, gy), D = iso(gx, gy + 1);
      const cols = Math.min(total, Math.max(1, Math.floor(sw * 2)));
      const col  = idx % cols, row = Math.floor(idx / cols), rows = Math.ceil(total / cols);
      const fx = 0.18 + (cols > 1 ? col / (cols - 1) : 0) * 0.64;
      const fy = 0.2  + (rows > 1 ? row  / (rows - 1) : 0) * 0.6;
      return {
        x: A.x + fx * (B.x - A.x) + fy * (D.x - A.x),
        y: A.y + fx * (B.y - A.y) + fy * (D.y - A.y) - 10 - (lift || 0),
      };
    }

    /* ── Paleta de colores por estado ── */
    function roomPalette(d) {
      if (d.alerts > 0)
        return { top: '#2a0808', lft: '#1a0505', rgt: '#130404', glow: '#ef4444', acc: '#f87171' };
      if (d.users.some(u => u.activity_type === 'exportando'))
        return { top: '#271400', lft: '#190d00', rgt: '#130a00', glow: '#f97316', acc: '#fb923c' };
      if (d.users.some(u => u.status === 'idle'))
        return { top: '#201800', lft: '#150f00', rgt: '#100c00', glow: '#f59e0b', acc: '#fbbf24' };
      if (d.users.some(u => u.status === 'active'))
        return { top: '#071b12', lft: '#04120b', rgt: '#030e08', glow: '#10b981', acc: '#34d399' };
      return { top: '#0a1726', lft: '#060f18', rgt: '#040c13', glow: '#1e3a5f', acc: '#3b82f6' };
    }

    function uStatusColor(u) {
      if (u.activity_type === 'exportando') return '#f97316';
      if (u.status === 'active') return '#10b981';
      if (u.status === 'idle')   return '#f59e0b';
      return '#64748b';
    }

    /* ── Escritorios isométricos sobre la cara del techo ── */
    function drawFurniture(room, A, B, D, lift, col) {
      const count = Math.min(Math.max(1, room.sw), 4);
      for (let i = 0; i < count; i++) {
        const fx  = 0.12 + (i / Math.max(1, count - 1)) * 0.76;
        const fy  = 0.32, dw = Math.max(0.09, 0.2 / room.sw), dd = 0.2, elv = 4;
        const fp  = (ffx, ffd) => ({
          x: A.x + ffx * (B.x - A.x) + ffd * (D.x - A.x),
          y: A.y + ffx * (B.y - A.y) + ffd * (D.y - A.y) - elv,
        });
        const p0 = fp(fx,      fy     ), p1 = fp(fx + dw, fy     );
        const p2 = fp(fx + dw, fy + dd), p3 = fp(fx,      fy + dd);
        ctx.save(); ctx.globalAlpha = .38;
        ctx.beginPath();
        ctx.moveTo(p0.x, p0.y); ctx.lineTo(p1.x, p1.y);
        ctx.lineTo(p2.x, p2.y); ctx.lineTo(p3.x, p3.y);
        ctx.closePath();
        ctx.fillStyle = 'rgba(18,38,76,.8)'; ctx.fill();
        ctx.strokeStyle = col.acc + '28'; ctx.lineWidth = .5; ctx.stroke();
        /* pantalla */
        const scx = (p0.x + p1.x) / 2, scy = (p0.y + p1.y) / 2 - 5;
        ctx.globalAlpha = .22;
        ctx.shadowColor = col.glow; ctx.shadowBlur = 5;
        ctx.beginPath();
        ctx.moveTo(scx - 2.5, scy - 6); ctx.lineTo(scx + 2.5, scy - 6);
        ctx.lineTo(scx + 2.5, scy - 1); ctx.lineTo(scx - 2.5, scy - 1);
        ctx.closePath();
        ctx.fillStyle = col.glow; ctx.fill();
        ctx.restore();
      }
    }

    /* ── Dibuja una sala isométrica ── */
    function drawRoom(room, d, t, sel) {
      const act  = d.users.length > 0 || d.alerts > 0;
      const base = room.main ? 6 : 3;
      const lift = (sel ? 10 : base) + (act ? Math.sin(t * 1.4) * 2 : 0);
      const { A, B, C, D } = roomCorners(room, lift);
      const col = roomPalette(d);

      const Dw = { x: D.x, y: D.y + WALL_H }, Cw = { x: C.x, y: C.y + WALL_H };
      const Bw = { x: B.x, y: B.y + WALL_H };

      /* Pared SW */
      paraPath([D, C, Cw, Dw]);
      ctx.fillStyle = col.lft; ctx.fill();
      ctx.strokeStyle = 'rgba(30,58,95,.5)'; ctx.lineWidth = .6; ctx.stroke();

      /* Pared SE */
      paraPath([C, B, Bw, Cw]);
      ctx.fillStyle = col.rgt; ctx.fill();
      ctx.strokeStyle = 'rgba(30,58,95,.5)'; ctx.lineWidth = .6; ctx.stroke();

      /* Glow del techo */
      if (act || sel) {
        ctx.save();
        ctx.shadowColor = col.glow;
        ctx.shadowBlur  = sel ? 38 : (act ? 24 : 8);
        paraPath([A, B, C, D]); ctx.fillStyle = 'rgba(0,0,0,0)'; ctx.fill();
        ctx.restore();
      }

      /* Cara superior */
      paraPath([A, B, C, D]);
      if (sel) {
        const g = ctx.createLinearGradient(A.x, A.y, C.x, C.y);
        g.addColorStop(0, col.acc + '45'); g.addColorStop(1, col.top);
        ctx.fillStyle = g;
      } else {
        ctx.fillStyle = col.top;
      }
      ctx.fill();

      /* Borde del techo */
      paraPath([A, B, C, D]);
      ctx.strokeStyle = sel ? col.acc + 'cc' : (act ? col.glow + '88' : 'rgba(30,58,95,.75)');
      ctx.lineWidth = sel ? 1.8 : .85;
      ctx.stroke();

      /* Cuadrícula de suelo */
      if (room.sw > 1) {
        ctx.save(); ctx.globalAlpha = .06; ctx.strokeStyle = '#60a5fa'; ctx.lineWidth = .5;
        for (let c = 1; c < room.sw; c++) {
          const f = c / room.sw;
          const p1 = { x: A.x + f * (B.x - A.x), y: A.y + f * (B.y - A.y) };
          const p2 = { x: D.x + f * (C.x - D.x), y: D.y + f * (C.y - D.y) };
          ctx.beginPath(); ctx.moveTo(p1.x, p1.y); ctx.lineTo(p2.x, p2.y); ctx.stroke();
        }
        ctx.restore();
      }

      /* Mobiliario */
      drawFurniture(room, A, B, D, lift, col);

      /* Etiqueta */
      const cx = (A.x + B.x + C.x + D.x) / 4;
      const cy = (A.y + B.y + C.y + D.y) / 4 - lift;
      ctx.save(); ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
      ctx.font = `700 ${room.main ? 10 : 8.5}px "Outfit",system-ui,sans-serif`;
      ctx.fillStyle = 'rgba(226,232,240,.95)';
      ctx.fillText(room.label, cx, cy - 4);
      ctx.font = '500 7px "Outfit",system-ui,sans-serif';
      ctx.fillStyle = 'rgba(148,163,184,.72)';
      ctx.fillText(room.sub, cx, cy + 5);
      ctx.restore();

      /* Badge usuarios */
      if (d.users.length) {
        const bx = B.x, by = B.y - lift - 2;
        ctx.beginPath(); ctx.arc(bx, by, 7, 0, Math.PI * 2);
        ctx.fillStyle = col.glow; ctx.fill();
        ctx.font = '700 8px system-ui'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillStyle = '#fff'; ctx.fillText(d.users.length, bx, by);
      }

      /* Badge alerta */
      if (d.alerts > 0) {
        const bx = A.x, by = A.y - lift - 6;
        ctx.beginPath(); ctx.arc(bx, by, 6, 0, Math.PI * 2);
        ctx.fillStyle = '#ef4444'; ctx.fill();
        ctx.font = '700 8px system-ui'; ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillStyle = '#fff'; ctx.fillText('!', bx, by);
      }

      /* Indicador de drill-down en Nivel 1 */
      if (drillDown === null && LEVEL2_SUBS[room.key]) {
        const ix = C.x, iy = C.y + WALL_H + 4;
        ctx.save(); ctx.globalAlpha = .55;
        ctx.font = '600 7px system-ui'; ctx.textAlign = 'center'; ctx.textBaseline = 'top';
        ctx.fillStyle = col.acc;
        ctx.fillText('▼', ix, iy);
        ctx.restore();
      }
    }

    /* ── Avatares de usuarios ── */
    function drawUsers(room, d, t, sel) {
      if (!d.users.length) return;
      const base = room.main ? 6 : 3;
      const lift = (sel ? 10 : base) + Math.sin(t * 1.4) * 2;

      d.users.forEach((u, i) => {
        const pos  = dotPos(room, i, d.users.length, lift);
        const usel = FX_STATE.globe?.selectedUser === u.id;
        const sc   = uStatusColor(u);
        const r    = usel ? 11 : 9;
        const cs   = avatarColor(u.username).match(/#[0-9a-fA-F]{6}/g) || ['#3b82f6', '#8b5cf6'];

        if (usel) {
          const p = (Math.sin(t * 3.2) + 1) * .5;
          ctx.beginPath(); ctx.arc(pos.x, pos.y, r + 4 + p * 4, 0, Math.PI * 2);
          ctx.strokeStyle = `rgba(59,130,246,${.5 + p * .4})`; ctx.lineWidth = 1.2; ctx.stroke();
        }
        ctx.save();
        ctx.shadowColor = sc;
        ctx.shadowBlur  = usel ? 18 : (u.status === 'active' ? 10 : 4);
        ctx.beginPath(); ctx.arc(pos.x, pos.y, r, 0, Math.PI * 2);
        ctx.fillStyle = 'rgba(0,0,0,0)'; ctx.fill();
        ctx.restore();

        const ag = ctx.createRadialGradient(pos.x - r * .3, pos.y - r * .3, 0, pos.x, pos.y, r);
        ag.addColorStop(0, cs[0]); ag.addColorStop(1, cs[1] || cs[0]);
        ctx.beginPath(); ctx.arc(pos.x, pos.y, r, 0, Math.PI * 2);
        ctx.fillStyle = ag; ctx.fill();

        ctx.beginPath(); ctx.arc(pos.x, pos.y, r + 1.5, 0, Math.PI * 2);
        ctx.strokeStyle = sc; ctx.lineWidth = 1.5; ctx.stroke();

        ctx.font = `700 ${r < 10 ? 6 : 7}px system-ui,sans-serif`;
        ctx.textAlign = 'center'; ctx.textBaseline = 'middle';
        ctx.fillStyle = '#fff';
        ctx.fillText(initials(u.username), pos.x, pos.y + .5);

        if (u.activity_type) {
          ctx.beginPath(); ctx.arc(pos.x + r * .72, pos.y + r * .72, 2.5, 0, Math.PI * 2);
          ctx.fillStyle = sc; ctx.fill();
        }
      });
    }

    /* ── Líneas de conexión (Nivel 1) ── */
    function drawLinks(rooms, links, data, t) {
      const p = (Math.sin(t * .7) + 1) * .5;
      links.forEach(([ak, bk]) => {
        const rA = rooms.find(r => r.key === ak), rB = rooms.find(r => r.key === bk);
        if (!rA || !rB) return;
        const cA = roomCorners(rA), cB = roomCorners(rB);
        const x1 = (cA.A.x + cA.B.x + cA.C.x + cA.D.x) / 4;
        const y1 = (cA.A.y + cA.B.y + cA.C.y + cA.D.y) / 4;
        const x2 = (cB.A.x + cB.B.x + cB.C.x + cB.D.x) / 4;
        const y2 = (cB.A.y + cB.B.y + cB.C.y + cB.D.y) / 4;
        const flow = (data[ak] || { users: [] }).users.length > 0;
        ctx.beginPath(); ctx.moveTo(x1, y1); ctx.lineTo(x2, y2);
        ctx.strokeStyle = flow
          ? `rgba(16,185,129,${.07 + p * .08})`
          : `rgba(59,130,246,${.02 + p * .03})`;
        ctx.lineWidth = flow ? .9 : .4; ctx.stroke();
      });
    }

    /* ── Botón "← Campus" para Nivel 2 ── */
    function drawBackButton() {
      const bw = 78, bh = 22, by = 14;
      const bx = W - bw - 14;
      ctx.save(); ctx.globalAlpha = .88;
      ctx.beginPath();
      ctx.moveTo(bx + 5, by); ctx.lineTo(bx + bw - 5, by);
      ctx.quadraticCurveTo(bx + bw, by, bx + bw, by + 5);
      ctx.lineTo(bx + bw, by + bh - 5);
      ctx.quadraticCurveTo(bx + bw, by + bh, bx + bw - 5, by + bh);
      ctx.lineTo(bx + 5, by + bh);
      ctx.quadraticCurveTo(bx, by + bh, bx, by + bh - 5);
      ctx.lineTo(bx, by + 5); ctx.quadraticCurveTo(bx, by, bx + 5, by);
      ctx.closePath();
      ctx.fillStyle = '#0f172a'; ctx.fill();
      ctx.strokeStyle = '#334155'; ctx.lineWidth = 1; ctx.stroke();
      ctx.font = '600 10px "Outfit",system-ui,sans-serif';
      ctx.fillStyle = '#94a3b8';
      ctx.textAlign = 'left'; ctx.textBaseline = 'middle';
      ctx.fillText('← Campus', bx + 10, by + bh / 2);
      ctx.restore();
    }

    /* ── Título Nivel 2 ── */
    function drawL2Title(moduleKey) {
      const room = CAMPUS_ROOMS.find(r => r.key === moduleKey);
      if (!room) return;
      ctx.save();
      ctx.font = '700 11px "Outfit",system-ui,sans-serif';
      ctx.fillStyle = 'rgba(226,232,240,.75)';
      ctx.textAlign = 'center'; ctx.textBaseline = 'top';
      ctx.fillText(room.label + '  —  detalle de secciones', W / 2, 14);
      ctx.restore();
    }

    /* ── Frame completo ── */
    function drawFrame(t) {
      const W2 = canvas.width / DPR, H2 = canvas.height / DPR;
      ctx.clearRect(0, 0, W2, H2);

      /* Transición suave entre niveles */
      if (Math.abs(transAlpha - transTarget) > .008)
        transAlpha += (transTarget - transAlpha) * .18;
      else
        transAlpha = transTarget;

      /* Fondo */
      const bg = ctx.createLinearGradient(0, 0, W2, H2);
      bg.addColorStop(0, '#050c18'); bg.addColorStop(1, '#081426');
      ctx.fillStyle = bg; ctx.fillRect(0, 0, W2, H2);

      ctx.globalAlpha = transAlpha;

      const paintOrder = rooms => [...rooms].sort((a, b) => {
        const fa = a.gy + a.sd, fb = b.gy + b.sd;
        return fa !== fb ? fa - fb : a.gx - b.gx;
      });

      if (drillDown === null) {
        /* ── NIVEL 1: campus ── */
        const l1MaxGx = CAMPUS_ROOMS.reduce((m, r) => Math.max(m, r.gx + r.sw), 6);
        const l1MaxGy = CAMPUS_ROOMS.reduce((m, r) => Math.max(m, r.gy + r.sd), 3);
        ctx.globalAlpha = transAlpha * .05;
        for (let gx = 0; gx <= l1MaxGx; gx++) for (let gy = 0; gy <= l1MaxGy; gy++) {
          const p = iso(gx, gy);
          ctx.beginPath(); ctx.arc(p.x, p.y, 1.1, 0, Math.PI * 2);
          ctx.fillStyle = '#3b82f6'; ctx.fill();
        }
        ctx.globalAlpha = transAlpha;

        drawLinks(CAMPUS_ROOMS, CAMPUS_LINKS, campusData, t);

        const order = paintOrder(CAMPUS_ROOMS);
        order.forEach(r => drawRoom(r, campusData[r.key] || { users: [], alerts: 0 }, t,
          FX_STATE.globe?.selectedRoom === r.key));
        order.forEach(r => drawUsers(r, campusData[r.key] || { users: [], alerts: 0 }, t,
          FX_STATE.globe?.selectedRoom === r.key));

      } else {
        /* ── NIVEL 2: detalle del módulo ── */
        const subs = LEVEL2_SUBS[drillDown] || [];
        const maxGx = subs.reduce((m, s) => Math.max(m, s.gx + s.sw), 1);
        const maxGy = subs.reduce((m, s) => Math.max(m, s.gy + s.sd), 1);
        ctx.globalAlpha = transAlpha * .05;
        for (let gx = 0; gx <= maxGx; gx++) for (let gy = 0; gy <= maxGy; gy++) {
          const p = iso(gx, gy);
          ctx.beginPath(); ctx.arc(p.x, p.y, 1.1, 0, Math.PI * 2);
          ctx.fillStyle = '#3b82f6'; ctx.fill();
        }
        ctx.globalAlpha = transAlpha;

        drawL2Title(drillDown);

        const order = paintOrder(subs);
        order.forEach(r => drawRoom(r, l2Data[r.key] || { users: [], alerts: 0 }, t,
          FX_STATE.globe?.selectedRoom === r.key));
        order.forEach(r => drawUsers(r, l2Data[r.key] || { users: [], alerts: 0 }, t,
          FX_STATE.globe?.selectedRoom === r.key));

        drawBackButton();
      }

      ctx.globalAlpha = 1;
    }

    /* ── Hit testing ── */
    function hitTest(mx, my) {
      if (drillDown !== null) {
        if (mx >= W - 92 && mx <= W - 14 && my >= 14 && my <= 36) return { type: 'back' };
      }
      const rooms = drillDown !== null ? (LEVEL2_SUBS[drillDown] || []) : CAMPUS_ROOMS;
      const data  = drillDown !== null ? l2Data : campusData;
      const rev   = [...rooms].sort((a, b) => {
        const fa = a.gy + a.sd, fb = b.gy + b.sd;
        return fb !== fa ? fb - fa : b.gx - a.gx;
      });
      for (const room of rev) {
        const { A, B, C, D } = roomCorners(room);
        const d    = data[room.key] || { users: [] };
        const lift = FX_STATE.globe?.selectedRoom === room.key ? 10 : (room.main ? 6 : 3);
        for (let i = 0; i < d.users.length; i++) {
          const pos = dotPos(room, i, d.users.length, lift);
          if (Math.hypot(mx - pos.x, my - pos.y) <= 13)
            return { type: 'user', user: d.users[i], room };
        }
        if (ptInPara(mx, my, A, B, C, D)) return { type: 'room', room };
      }
      return null;
    }

    /* ── Tooltip y eventos ── */
    const ttEl = document.getElementById('globe-tooltip');

    canvas.addEventListener('pointermove', e => {
      const rc  = canvas.getBoundingClientRect();
      const mx  = (e.clientX - rc.left) / rc.width  * W;
      const my  = (e.clientY - rc.top)  / rc.height * CANVAS_H;
      const hit = hitTest(mx, my);
      if (hit && ttEl) {
        const wrc = wrap.getBoundingClientRect();
        if (hit.type === 'back') {
          ttEl.innerHTML = '<strong>← Volver al campus</strong>';
        } else if (hit.type === 'user') {
          const u = hit.user;
          ttEl.innerHTML =
            `<strong>${esc(u.username)}</strong><br>` +
            `${esc(u.role)} · ${esc(u.unit)}<br>` +
            `<span style="color:#94a3b8">📍 ${esc(userBreadcrumb(u) || sectionLabel(u.current_section))}</span><br>` +
            `<span style="color:#94a3b8">⚡ ${esc(u.last_action)}</span><br>` +
            `<span style="color:#94a3b8">⏱ ${esc(timeInCurrentSection(u))}</span>`;
        } else {
          const data = drillDown !== null ? l2Data : campusData;
          const d    = data[hit.room.key] || { users: [] };
          const last = d.users.length
            ? d.users.reduce((m, x) => new Date(x.last_ping) > new Date(m.last_ping) ? x : m, d.users[0])
            : null;
          const hint = drillDown === null && LEVEL2_SUBS[hit.room.key]
            ? ' · Clic para ver detalle' : '';
          ttEl.innerHTML =
            `<strong>${esc(hit.room.label)}</strong><br>` +
            `${d.users.length} usuario${d.users.length !== 1 ? 's' : ''} en esta área${hint}<br>` +
            (d.users.length
              ? `<span style="color:#94a3b8">${esc(d.users.map(u => u.username).join(', '))}</span><br>`
              : '') +
            (last
              ? `<span style="color:#94a3b8">Última actividad: ${esc(fmtRelative(last.last_ping))}</span>`
              : `<span style="color:#94a3b8">Área sin actividad</span>`);
        }
        ttEl.style.left    = (e.clientX - wrc.left + 14) + 'px';
        ttEl.style.top     = (e.clientY - wrc.top  + 14) + 'px';
        ttEl.style.display = 'block';
        canvas.style.cursor = 'pointer';
      } else {
        if (ttEl) ttEl.style.display = 'none';
        canvas.style.cursor = 'default';
      }
    }, { passive: true });

    canvas.addEventListener('click', e => {
      const rc  = canvas.getBoundingClientRect();
      const mx  = (e.clientX - rc.left) / rc.width  * W;
      const my  = (e.clientY - rc.top)  / rc.height * CANVAS_H;
      const hit = hitTest(mx, my);
      if (!hit) return;

      if (hit.type === 'back') {
        drillDown = null;
        transAlpha = 0; transTarget = 1;
        setLevelParams(); syncData();
        if (FX_STATE.globe) { FX_STATE.globe.selectedRoom = null; FX_STATE.globe.selectedUser = null; }
        refreshGlobeUsers(null, null);
        const hd = document.querySelector('.globe-list-hd');
        if (hd) hd.innerHTML = `<div class="sdot sdot-active"></div> Todos los módulos`;
        return;
      }

      if (hit.type === 'user') { selectNavigationUser(hit.user.id); return; }

      const key = hit.room.key;
      if (drillDown === null && LEVEL2_SUBS[key]) {
        /* Entrar a Nivel 2 */
        drillDown = key;
        transAlpha = 0; transTarget = 1;
        setLevelParams();
        l2Data = buildL2Data(key);
        if (FX_STATE.globe) { FX_STATE.globe.selectedRoom = null; FX_STATE.globe.selectedUser = null; }
        selectNavigationNode(key);
      } else {
        /* Seleccionar sala en Nivel 2 */
        if (FX_STATE.globe) FX_STATE.globe.selectedRoom = key;
        refreshGlobeUsers(key, null, drillDown);
        const hd = document.querySelector('.globe-list-hd');
        const allRooms = drillDown ? (LEVEL2_SUBS[drillDown] || []) : CAMPUS_ROOMS;
        const room = allRooms.find(r => r.key === key);
        if (hd && room) hd.innerHTML = `<div class="sdot sdot-active"></div> ${esc(room.label)}`;
      }
    });

    canvas.addEventListener('pointerleave', () => {
      if (ttEl) ttEl.style.display = 'none';
    }, { passive: true });

    /* ── Loop de animación (30fps) ── */
    let lastFrame = 0;
    const loop = t => {
      if (!FX_STATE.globe) return;
      const raf = requestAnimationFrame(loop);
      FX_STATE.rafs.add(raf);
      if (t - lastFrame < 1000 / 30) return;
      lastFrame = t;
      drawFrame(t * .001);
    };

    /* ── Resize ── */
    const resize = () => {
      W = wrap.clientWidth || 640;
      setLevelParams();
    };
    window.addEventListener('resize', resize, { passive: true });

    FX_STATE.globe = { canvas, resize, syncData, selectedRoom: null, selectedUser: null };
    loop(0);
  }

  function destroyIsometricOffice() {
    FX_STATE.globe = null;
  }

  function moduleNodeColor(key, events) {
    if (events.some(e => e.action === 'upload')) return 0x10b981;
    if (events.some(e => e.action === 'export' || e.action === 'download')) return 0xf59e0b;
    return 0x3b82f6;
  }

  function makeTextSprite(text) {
    const cnv = document.createElement('canvas');
    cnv.width = 512;
    cnv.height = 96;
    const ctx = cnv.getContext('2d');
    ctx.font = '600 32px Arial';
    ctx.fillStyle = 'rgba(226,232,240,.92)';
    ctx.textAlign = 'center';
    ctx.fillText(text, 256, 56);
    const tex = new THREE.CanvasTexture(cnv);
    const sprite = new THREE.Sprite(new THREE.SpriteMaterial({ map: tex, transparent: true }));
    sprite.scale.set(1.55, .3, 1);
    return sprite;
  }

  function initNavGraph(u) {
    destroyNavGraph();
    const canvas = document.getElementById('nav-graph-canvas');
    const wrap = document.getElementById('nav-graph-wrap');
    if (!canvas || !wrap || !u || !canUseThree()) return;

    const width = wrap.clientWidth || 420;
    const height = 280;
    const scene = new THREE.Scene();
    const camera = new THREE.PerspectiveCamera(50, width / height, .1, 100);
    camera.position.z = 8;
    const renderer = new THREE.WebGLRenderer({ canvas, alpha: true, antialias: true });
    renderer.setPixelRatio(Math.min(window.devicePixelRatio || 1, 2));
    renderer.setSize(width, height, false);
    scene.add(new THREE.AmbientLight(0x7dd3fc, .45));
    const light = new THREE.PointLight(0x3b82f6, 1.4, 30);
    light.position.set(-3, 4, 6);
    scene.add(light);

    const group = new THREE.Group();
    scene.add(group);
    const events = u.timeline || [];
    const modules = [...new Set(events.map(e => e.section).concat(u.modules || []))].filter(Boolean).slice(0, 8);
    const counts = {};
    events.forEach(e => { counts[e.section] = (counts[e.section] || 0) + 1; });
    const maxCount = Math.max(1, ...Object.values(counts));
    const nodes = [];
    modules.forEach((m, i) => {
      const angle = (Math.PI * 2 * i) / modules.length;
      const z = Math.sin(i * 1.7) * .8;
      const radius = .22 + ((counts[m] || 1) / maxCount) * .22;
      const related = events.filter(e => e.section === m);
      const node = new THREE.Mesh(
        new THREE.SphereGeometry(radius, 24, 24),
        new THREE.MeshStandardMaterial({ color: moduleNodeColor(m, related), emissive: moduleNodeColor(m, related), emissiveIntensity: .35, roughness: .28 })
      );
      node.position.set(Math.cos(angle) * 2.5, Math.sin(angle) * 1.25, z);
      node.userData = { module: m, visits: counts[m] || 1, last: related[0]?.time || u.last_access };
      group.add(node);
      const label = makeTextSprite(sectionLabel(m));
      label.position.copy(node.position).add(new THREE.Vector3(0, radius + .35, 0));
      group.add(label);
      nodes.push(node);
    });
    for (let i = 0; i < nodes.length - 1; i++) {
      const line = new THREE.Line(
        new THREE.BufferGeometry().setFromPoints([nodes[i].position, nodes[i + 1].position]),
        new THREE.LineBasicMaterial({ color: 0x1e3a5f, transparent: true, opacity: .8 })
      );
      group.add(line);
    }

    const raycaster = new THREE.Raycaster();
    const mouse = new THREE.Vector2();
    const tooltip = document.getElementById('nav-graph-tooltip');
    let dragging = false;
    let lastX = 0;
    let lastY = 0;
    let lastFrame = 0;
    const frameMs = 1000 / (FX_STATE.lowPower ? 30 : 60);
    const setMouse = e => {
      const r = canvas.getBoundingClientRect();
      mouse.x = ((e.clientX - r.left) / r.width) * 2 - 1;
      mouse.y = -((e.clientY - r.top) / r.height) * 2 + 1;
    };
    canvas.addEventListener('pointerdown', e => { dragging = true; lastX = e.clientX; lastY = e.clientY; canvas.setPointerCapture?.(e.pointerId); });
    canvas.addEventListener('pointermove', e => {
      setMouse(e);
      if (dragging) {
        group.rotation.y += (e.clientX - lastX) * .01;
        group.rotation.x += (e.clientY - lastY) * .01;
        lastX = e.clientX;
        lastY = e.clientY;
      }
      raycaster.setFromCamera(mouse, camera);
      const hit = raycaster.intersectObjects(nodes)[0];
      nodes.forEach(n => n.scale.setScalar(1));
      if (hit && tooltip) {
        const d = hit.object.userData;
        hit.object.scale.setScalar(1.18);
        tooltip.innerHTML = `<strong>${esc(sectionLabel(d.module))}</strong><br>${d.visits} visitas · ${fmtRelative(d.last)}`;
        tooltip.style.left = (e.clientX - wrap.getBoundingClientRect().left + 10) + 'px';
        tooltip.style.top = (e.clientY - wrap.getBoundingClientRect().top + 10) + 'px';
        tooltip.style.display = 'block';
      } else if (tooltip) tooltip.style.display = 'none';
    });
    canvas.addEventListener('pointerup', e => { dragging = false; canvas.releasePointerCapture?.(e.pointerId); });
    canvas.addEventListener('pointerleave', () => { dragging = false; if (tooltip) tooltip.style.display = 'none'; });
    canvas.addEventListener('wheel', e => {
      e.preventDefault();
      camera.position.z = Math.max(4, Math.min(12, camera.position.z + Math.sign(e.deltaY) * .45));
    }, { passive: false });

    const loop = time => {
      if (!FX_STATE.navGraph) return;
      const raf = requestAnimationFrame(loop);
      FX_STATE.rafs.add(raf);
      if (time - lastFrame < frameMs) return;
      lastFrame = time;
      if (!dragging) group.rotation.y += .004;
      renderer.render(scene, camera);
    };
    FX_STATE.navGraph = { renderer, scene };
    loop(0);
  }

  function destroyNavGraph() {
    const graph = FX_STATE.navGraph;
    if (!graph) return;
    graph.scene.traverse(obj => {
      obj.geometry?.dispose?.();
      if (obj.material?.map) obj.material.map.dispose?.();
      if (obj.material) {
        if (Array.isArray(obj.material)) obj.material.forEach(m => m.dispose?.());
        else obj.material.dispose?.();
      }
    });
    graph.renderer.dispose();
    FX_STATE.navGraph = null;
  }

  function updateRadarSpeed() {
    const radar = document.getElementById('alert-radar-svg');
    if (!radar) return;
    const active = MOCK_ALERTS.filter(a => !resolvedAlerts.has(a.id));
    const hasHigh = active.some(a => a.severity === 'alta');
    radar.style.setProperty('--radar-speed', hasHigh ? '1.15s' : '2.4s');
  }

  function burstParticles(origin) {
    if (FX_STATE.reducedMotion || !origin) return;
    const r = origin.getBoundingClientRect();
    const cx = r.left + r.width / 2;
    const cy = r.top + r.height / 2;
    for (let i = 0; i < 18; i++) {
      const p = document.createElement('div');
      p.className = 'burst-particle';
      p.style.left = cx + 'px';
      p.style.top = cy + 'px';
      document.body.appendChild(p);
      const angle = (Math.PI * 2 * i) / 18;
      const dist = 40 + Math.random() * 70;
      if (canAnimate()) {
        gsap.to(p, {
          x: Math.cos(angle) * dist,
          y: Math.sin(angle) * dist,
          opacity: 0,
          scale: .2,
          duration: .65,
          ease: 'power2.out',
          onComplete: () => p.remove(),
        });
      } else {
        setTimeout(() => p.remove(), 500);
      }
    }
  }

  function destroyAllEffects() {
    FX_STATE.rafs.forEach(id => cancelAnimationFrame(id));
    FX_STATE.rafs.clear();
    destroyIsometricOffice();
    destroyNavGraph();
    destroyVantaBackground();
  }

})();
