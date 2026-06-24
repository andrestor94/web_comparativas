/**
 * IC_CHART_THEME — tema compartido de Chart.js para Indicadores Comerciales.
 * Fuente de verdad de la presentación de gráficos del módulo: lee los tokens
 * --ic-* del wrapper .ic-dash (así un cambio de paleta en CSS se refleja acá
 * sin tocar JS) y aplica defaults globales de Chart.js (tipografía, tooltips).
 *
 * Cargar DESPUÉS de chart.umd.js y ANTES del JS de cada vista.
 * Solo presentación: no toca datos, fetch ni callbacks de negocio.
 */
window.IC_CHART_THEME = (function () {
  'use strict';

  var host = document.querySelector('.ic-dash');
  var cs = host ? getComputedStyle(host) : null;
  var tok = function (name, fallback) {
    var v = cs ? cs.getPropertyValue(name).trim() : '';
    return v || fallback;
  };

  var colors = {
    navy:  tok('--ic-navy',  '#06486f'),
    blue:  tok('--ic-blue',  '#5770b0'),
    amber: tok('--ic-amber', '#d97706'),
    red:   tok('--ic-red',   '#dc2626'),
    green: tok('--ic-green', '#00A487'),
    ink:   tok('--ic-text-900', '#0f172a'),
    text:  tok('--ic-text-500', '#64748b'),
    grid:  'rgba(15, 23, 42, 0.06)'
  };

  // Tipografía del wrapper (Inter / Space Grotesk según dirección)
  var fontFamily = host
    ? getComputedStyle(host).fontFamily
    : "'Inter', -apple-system, 'Segoe UI', Roboto, sans-serif";

  // '#rgb' / '#rrggbb' / 'rgb(...)' → 'rgba(r,g,b,a)'
  function alpha(color, a) {
    var c = String(color).trim();
    var hex = c.match(/^#([0-9a-f]{3}|[0-9a-f]{6})$/i);
    if (hex) {
      var h = hex[1];
      if (h.length === 3) h = h.split('').map(function (x) { return x + x; }).join('');
      var n = parseInt(h, 16);
      return 'rgba(' + ((n >> 16) & 255) + ', ' + ((n >> 8) & 255) + ', ' + (n & 255) + ', ' + a + ')';
    }
    var rgb = c.match(/^rgba?\(([^)]+)\)/);
    if (rgb) {
      var parts = rgb[1].split(',').slice(0, 3).join(',');
      return 'rgba(' + parts + ', ' + a + ')';
    }
    return c;
  }

  // Rampa de opacidad para rankings: n tonos del mismo color, de 1 a ~0.35
  function ramp(color, n, from, to) {
    from = from == null ? 1 : from;
    to = to == null ? 0.35 : to;
    var out = [];
    for (var i = 0; i < n; i++) {
      var a = n <= 1 ? from : from + (to - from) * (i / (n - 1));
      out.push(alpha(color, a));
    }
    return out;
  }

  // Fragmentos de escala: gridlines mínimas, sin línea de borde
  function gridY(tickFormat) {
    var ticks = { maxTicksLimit: 6, padding: 8 };
    if (tickFormat) ticks.callback = tickFormat;
    return { border: { display: false }, grid: { color: colors.grid }, ticks: ticks };
  }
  function gridX() {
    return { border: { display: false }, grid: { display: false }, ticks: { padding: 6 } };
  }

  // Defaults globales: fuente, color y tooltips estilados
  function applyDefaults(Chart) {
    if (!Chart) return;
    Chart.defaults.font.family = fontFamily;
    Chart.defaults.font.size = 11.5;
    Chart.defaults.color = colors.text;
    var tt = Chart.defaults.plugins.tooltip;
    tt.backgroundColor = alpha(colors.ink, 0.92);
    tt.padding = 10;
    tt.cornerRadius = 8;
    tt.titleFont = { weight: '600' };
    tt.displayColors = false;
    tt.caretSize = 5;
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.boxWidth = 6;
  }

  if (window.Chart) applyDefaults(window.Chart);

  // Sombra de scroll para tablas con columna sticky: agrega/quita la clase
  // is-scrolled-x en los contenedores .table-responsive del módulo.
  // Los listeners van sobre los contenedores (estáticos); el JS de cada
  // vista solo re-renderiza los tbody, así que no hace falta re-bindear.
  function initScrollShadows(root) {
    var scope = root || document;
    var containers = scope.querySelectorAll('.ic-dash .table-responsive');
    Array.prototype.forEach.call(containers, function (el) {
      var update = function () {
        el.classList.toggle('is-scrolled-x', el.scrollLeft > 2);
      };
      el.addEventListener('scroll', update, { passive: true });
      update();
    });
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', function () { initScrollShadows(); });
  } else {
    initScrollShadows();
  }

  return {
    colors: colors,
    fontFamily: fontFamily,
    alpha: alpha,
    ramp: ramp,
    gridY: gridY,
    gridX: gridX,
    applyDefaults: applyDefaults,
    initScrollShadows: initScrollShadows
  };
})();
