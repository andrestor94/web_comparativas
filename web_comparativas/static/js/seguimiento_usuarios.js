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
    'mercado_publico_dimensiones':  'Dimensiones — Público',
    'mercado_publico_buscador':     'Buscador — Público',

    // Mercado Privado
    'mercado_privado':              'Mercado Privado',
    'mercado_privado_home':         'Inicio — Mercado Privado',
    'mercado_privado_dimensiones':  'Dimensiones — Privado',
    'mercado_privado_buscador':     'Buscador — Privado',

    // Oportunidades
    'oportunidades':                'Oportunidades',
    'oportunidades_buscador':       'Buscador de Oportunidades',
    'oportunidades_dimensiones':    'Análisis de Dimensiones',

    // Pliegos
    'pliegos':                      'Módulo de Pliegos',
    'pliego_widget':                'Visor de Pliegos',
    'pliego_detalle':               'Detalle de Pliego',

    // Forecast
    'forecast':                     'Proyecciones y Forecast',
    'forecast_widget':              'Panel de Forecast',

    // Perfiles
    'reporte_perfiles':             'Reporte de Perfiles',
    'perfiles':                     'Perfiles de Clientes',

    // Comparativas
    'comparativa':                  'Comparativa de Mercado',
    'web_comparativas':             'Comparativa de Mercado',
    'comparativa_home':             'Inicio — Comparativa',

    // Auth
    'login':                        'Inicio de Sesión',
    'logout':                       'Cierre de Sesión',

    // Fallback
    '/':                            'Inicio',
    '':                             'Inicio',
  };

  /** Resuelve un nombre técnico a su etiqueta legible. */
  function sectionLabel(raw) {
    if (!raw) return 'Inicio';
    const key = String(raw).toLowerCase().trim().replace(/-/g, '_');
    if (SECTION_MAP[key]) return SECTION_MAP[key];
    // Try partial match
    for (const [k, v] of Object.entries(SECTION_MAP)) {
      if (k && key.includes(k) && k.length > 3) return v;
    }
    // Title-case fallback
    return key.replace(/_/g, ' ').replace(/\b\w/g, c => c.toUpperCase());
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
      current_section: 'mercado_privado_dimensiones',
      session_start: dAgo(0, 2, 10),
      last_ping: mAgo(18),
      last_action: 'Exportó reporte',
      activity_type: 'exportando',
      nav_trail: ['dashboard','mercado_privado','mercado_privado_dimensiones'],
      sessions_detail: [
        { date: dAgo(0,2), duration: '2h 10min', sections: ['dashboard','mercado_privado','mercado_privado_dimensiones','forecast'] },
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
      modules: ['sic_tracking', 'sic_usuarios', 'reporte_perfiles'],
      frequency: 'Semanal', last_access: mAgo(55),
      risk: 'bajo', status: 'active',
      current_section: 'reporte_perfiles',
      session_start: mAgo(65),
      last_ping: mAgo(55),
      last_action: 'Descargó reporte',
      activity_type: 'exportando',
      nav_trail: ['sic', 'sic_tracking', 'reporte_perfiles'],
      sessions_detail: [
        { date: dAgo(0,1,5), duration: '1h 05min', sections: ['sic','sic_tracking','reporte_perfiles'] },
        { date: dAgo(2),     duration: '1h 40min', sections: ['sic','sic_usuarios','sic_tracking'] },
        { date: dAgo(5),     duration: '50min',    sections: ['sic','reporte_perfiles'] },
      ],
      timeline: [
        { time: mAgo(55), action: 'download', section: 'reporte_perfiles' },
        { time: mAgo(58), action: 'view',     section: 'reporte_perfiles' },
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
  const MOCK_ALERTS = [
    {
      id: 'a1', severity: 'alta', userId: 4,
      icon: 'bi-exclamation-octagon',
      title: 'Usuario inactivo prolongado',
      body: 'Diego Paredes no registra actividad desde hace 9 días. Riesgo elevado de abandono del sistema.',
      rec: 'Contactar al usuario para verificar necesidades de capacitación o acceso.',
      time: dAgo(9),
    },
    {
      id: 'a2', severity: 'alta', userId: 4,
      icon: 'bi-graph-down-arrow',
      title: 'Score de adopción crítico',
      body: 'Diego Paredes mantiene un score de 21/100 desde el inicio. Sin actividad de carga ni exportación.',
      rec: 'Asignar sesión de onboarding personalizada. Revisar acceso a módulos relevantes.',
      time: dAgo(7),
    },
    {
      id: 'a3', severity: 'media', userId: 3,
      icon: 'bi-arrow-down-circle',
      title: 'Actividad por debajo del umbral',
      body: 'Laura Ríos presenta frecuencia semanal con score estancado en 45/100. Poca diversidad de módulos.',
      rec: 'Programar demo de módulos avanzados: Dimensiones y Forecast.',
      time: dAgo(3),
    },
    {
      id: 'a4', severity: 'media', userId: 2,
      icon: 'bi-clock-history',
      title: 'Actividad inusual en horario tardío',
      body: 'Carlos Méndez registró 3 sesiones entre las 22:00 y las 01:00 en los últimos 5 días.',
      rec: 'Verificar si el acceso fuera de horario es esperado para este rol (Supervisor).',
      time: dAgo(2),
    },
    {
      id: 'a5', severity: 'baja', userId: 5,
      icon: 'bi-shield-check',
      title: 'Acceso a módulo de auditoría frecuente',
      body: 'Sofía Herrera accedió a Seguimiento de Usuarios 18 veces en el período. Actividad dentro del rol.',
      rec: 'Sin acción requerida. Actividad coherente con perfil de Auditor.',
      time: dAgo(1),
    },
    {
      id: 'a6', severity: 'baja', userId: 1,
      icon: 'bi-star',
      title: 'Primer acceso registrado — módulo Forecast',
      body: 'María González accedió por primera vez al módulo Forecast. Señal positiva de exploración.',
      rec: 'Enviar guía rápida del módulo Forecast para potenciar la adopción.',
      time: dAgo(0, 6),
    },
  ];

  /* ═══════════════════════════════════════════════════════
     STATE
  ═══════════════════════════════════════════════════════ */
  let currentTab   = 'usage-tab-live';
  let liveTimer    = null;
  let liveCounter  = 0;
  let activeAlertFilter = 'all';
  let resolvedAlerts = new Set();
  let chartsBuilt  = { summary: false };
  let sortState    = { col: 'score', dir: 'desc' };
  let sparkCharts  = {};
  let profScoreChart = null;

  /* ═══════════════════════════════════════════════════════
     INIT
  ═══════════════════════════════════════════════════════ */
  document.addEventListener('DOMContentLoaded', () => {
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
    if (rbtn) rbtn.addEventListener('click', () => { renderLiveTable(); renderKPIs(); });
    window.sicLiveRefresh = renderLiveTable;
    window.sicRefreshTracking = () => { renderLiveTable(); renderKPIs(); };
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
      renderLiveTable();
      renderKPIs();
      renderByUserTable();
      renderAlerts();
      if (currentTab === 'usage-tab-summary') {
        chartsBuilt.summary = false;
        buildSummaryCharts();
      }
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
    setEl('card-live-users',    ONLINE_COUNT);
    setEl('card-adoption-val',  `${ADOPTION_RATE}<span style="font-size:16px;font-weight:500;">%</span>`);
    setEl('card-inactive-users', INACTIVE_USERS.length);
    setEl('card-active-users',  ACTIVE_USERS.length);
    setEl('card-active-hours',  fmtDec(TOTAL_HOURS, 0));
    setEl('card-prod-index',    fmtDec(PRODUCTIVITY, 1).replace('.', ','));
    setEl('live-count-badge',   ONLINE_COUNT);

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
     AUTO-REFRESH LIVE
  ═══════════════════════════════════════════════════════ */
  function startLiveRefresh() {
    if (liveTimer) clearInterval(liveTimer);
    liveTimer = setInterval(() => {
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

    // 1. Actividad por día
    const wdEl = document.getElementById('usage-chart-weekday');
    if (wdEl && !wdEl._chart) {
      const c = new ApexCharts(wdEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 280, toolbar: { show: false } },
        series: [
          { name: 'Eventos', data: WEEKDAY_DATA.map(d => d.events) },
          { name: 'Promedio', type: 'line', data: WEEKDAY_DATA.map(d => d.avg) },
        ],
        xaxis: { categories: WEEKDAY_DATA.map(d => d.day), labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        colors: ['#3b82f6', '#f59e0b'],
        stroke: { width: [0, 2.5], curve: 'smooth' },
        fill: { opacity: [.85, 1], type: ['solid','solid'] },
        markers: { size: [0, 4] },
        plotOptions: { bar: { borderRadius: 5, columnWidth: '55%' } },
        legend: { labels: { colors: '#94a3b8' }, fontSize: '12px' },
        tooltip: {
          theme: 'dark',
          shared: true,
          intersect: false,
          y: { formatter: v => fmtInt(v) + ' eventos' },
        },
      });
      c.render(); wdEl._chart = c;
    }

    // 2. Top usuarios (barras horizontales)
    const rolesEl = document.getElementById('usage-chart-roles');
    if (rolesEl && !rolesEl._chart) {
      const sorted = [...MOCK_USERS].sort((a, b) => b.active_hours - a.active_hours);
      const c = new ApexCharts(rolesEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 340, toolbar: { show: false } },
        plotOptions: { bar: { horizontal: true, borderRadius: 4, dataLabels: { position: 'top' } } },
        series: [
          { name: 'Horas activas', data: sorted.map(u => ({ x: u.username.split(' ')[0], y: Math.round(u.active_hours) })) },
          { name: 'Score',         data: sorted.map(u => ({ x: u.username.split(' ')[0], y: u.score })) },
        ],
        colors: ['#3b82f6', '#10b981'],
        xaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#94a3b8', fontSize: '11px' }, maxWidth: 100 } },
        legend: { labels: { colors: '#94a3b8' }, fontSize: '12px' },
        tooltip: { theme: 'dark', shared: false },
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
          const entry = HEATMAP_DATA.find(x => x.day === d && x.hour === h);
          return { x: `${h}:00`, y: entry ? entry.value : 0 };
        }),
      }));
      const c = new ApexCharts(hmEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'heatmap', height: 280, toolbar: { show: false } },
        series,
        dataLabels: { enabled: false },
        colors: ['#3b82f6'],
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
              <span style="color:#93c5fd;">${val} eventos</span>
            </div>`;
          },
        },
      });
      c.render(); hmEl._chart = c;
    }

    // 4. Secciones más usadas
    const secEl = document.getElementById('usage-chart-sections');
    if (secEl && !secEl._chart) {
      const total = SECTION_USAGE.reduce((s, x) => s + x.count, 0);
      const c = new ApexCharts(secEl, {
        ...CHART_DEFAULTS,
        chart: { ...CHART_DEFAULTS.chart, type: 'bar', height: 280, toolbar: { show: false } },
        plotOptions: { bar: { horizontal: true, borderRadius: 4, dataLabels: { position: 'top' } } },
        series: [{ name: 'Eventos', data: SECTION_USAGE.map(s => ({ x: sectionLabel(s.key), y: s.count })) }],
        colors: ['#6366f1'],
        xaxis: { labels: { style: { colors: '#64748b', fontSize: '11px' } } },
        yaxis: { labels: { style: { colors: '#94a3b8', fontSize: '11px' }, maxWidth: 160 } },
        tooltip: {
          theme: 'dark',
          y: { formatter: (v, { dataPointIndex }) => {
            const pct = ((SECTION_USAGE[dataPointIndex].count / total) * 100).toFixed(1);
            return `${fmtInt(v)} eventos (${pct}%)`;
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
        chart: { ...CHART_DEFAULTS.chart, type: 'area', height: 280, toolbar: { show: false } },
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
        chart: { ...CHART_DEFAULTS.chart, type: 'donut', height: 290, toolbar: { show: false } },
        series: topModules.map(s => s.count),
        labels: topModules.map(s => sectionLabel(s.key)),
        colors: ['#3b82f6','#10b981','#6366f1','#f59e0b','#ef4444','#06b6d4'],
        stroke: { width: 2, colors: ['#0f1729'] },
        plotOptions: { pie: { donut: { size: '62%', labels: {
          show: true,
          total: { show: true, label: 'Total', color: '#94a3b8', formatter: () => fmtInt(SECTION_USAGE.reduce((s,x)=>s+x.count,0)) + ' ev.' },
        }}}},
        legend: { position: 'bottom', labels: { colors: '#94a3b8' }, fontSize: '11px' },
        tooltip: { theme: 'dark', y: { formatter: v => fmtInt(v) + ' eventos' } },
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
        // If charts are built, we'd update them — placeholder for API integration
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
    setEl('afc-all',   active.length);
    setEl('afc-alta',  countsBySev.alta);
    setEl('afc-media', countsBySev.media);
    setEl('afc-baja',  (countsBySev.baja || 0) + (countsBySev.info || 0));

    const navBadge = document.getElementById('nav-alerts-badge');
    if (navBadge) {
      navBadge.textContent = active.length;
      navBadge.style.display = active.length ? '' : 'none';
    }

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

    // Bind resolve
    container.querySelectorAll('.btn-alert-resolve').forEach(btn => {
      btn.addEventListener('click', e => {
        e.stopPropagation();
        const id = btn.dataset.alertId;
        const card = document.getElementById(`alert-card-${id}`);
        if (card) {
          card.querySelector('.alert-card').classList.add('resolving');
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
    };
    if (closeBtn) closeBtn.addEventListener('click', closePanel);
    if (overlay)  overlay.addEventListener('click', closePanel);

    document.addEventListener('keydown', e => {
      if (e.key === 'Escape') closePanel();
    });
  }

  function openProfile(uid) {
    const u = MOCK_USERS.find(x => x.id === uid);
    if (!u) return;

    const panel   = document.getElementById('userProfilePanel');
    const overlay = document.getElementById('profile-overlay');
    const loading = document.getElementById('profile-loading');
    const content = document.getElementById('profile-content');

    if (!panel) return;
    loading.style.display = '';
    content.style.display = 'none';
    panel.classList.add('open');
    overlay?.classList.add('open');

    setTimeout(() => {
      populateProfile(u);
      loading.style.display = 'none';
      content.style.display = '';
      buildProfileScoreChart(u);
    }, 280);
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

    // Alerts for this user
    const alertsEl = document.getElementById('prof-alerts');
    if (alertsEl) {
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

})();
