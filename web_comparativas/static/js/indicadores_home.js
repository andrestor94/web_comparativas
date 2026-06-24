/**
 * indicadores_home.js — count-up + sparklines del Home (editorial) de Indicadores.
 *
 * - Count-up de los números grandes de cada fila (ease-out cúbico, es-AR).
 * - Sparklines server-side (rentab + labs + inflación) con dibujo progresivo (SVG).
 *   Inflación: serie PRECALCULADA (ind_inflacion_evolucion_mensual) → instantánea;
 *   sin datos → fallback discreto "sin datos aún" (naranja tenue), no "cargando…".
 * - Respeta prefers-reduced-motion: muestra valores finales sin animar.
 *
 * Datos inyectados en <script id="ic-home-data">. SIN fetch.
 * Cargar DESPUÉS de chart.umd.js y de indicadores_chart_theme.js.
 */
(function () {
  'use strict';

  var T = window.IC_CHART_THEME;
  var REDUCE = window.matchMedia && window.matchMedia('(prefers-reduced-motion: reduce)').matches;

  // ── Datos inyectados server-side ────────────────────────────────
  var DATA = {};
  try {
    var el = document.getElementById('ic-home-data');
    if (el) DATA = JSON.parse(el.textContent || '{}');
  } catch (e) { DATA = {}; }

  // ── Formatos es-AR (espejo de los macros Jinja num/money_m/pct) ──
  function fNum(v) { return Math.round(v).toLocaleString('es-AR'); }
  function fMoneyM(v) { return '$ ' + Math.round(v / 1e6).toLocaleString('es-AR') + ' M'; }
  function fPct(v) { return (v * 100).toFixed(1).replace('.', ',') + '%'; }   // v = fracción 0–1
  function fPct0(v) { return Math.round(v) + '%'; }                            // v = ya en 0–100
  var FMT = { num: fNum, money_m: fMoneyM, pct: fPct, pct0: fPct0 };

  // ── Count-up (ease-out cúbico, ~1.4s) ───────────────────────────
  function runCountUps() {
    var nodes = document.querySelectorAll('.ich-num[data-cv]');
    Array.prototype.forEach.call(nodes, function (node) {
      var target = parseFloat(node.getAttribute('data-cv'));
      var fmt = FMT[node.getAttribute('data-fmt')] || fNum;
      if (!isFinite(target)) return;
      if (REDUCE) { node.textContent = fmt(target); return; }
      var dur = 1400, start = null;
      function step(ts) {
        if (start === null) start = ts;
        var t = Math.min(1, (ts - start) / dur);
        var eased = 1 - Math.pow(1 - t, 3);
        node.textContent = fmt(target * eased);
        if (t < 1) requestAnimationFrame(step);
        else node.textContent = fmt(target);
      }
      requestAnimationFrame(step);
    });
  }

  // ── Sparklines ──────────────────────────────────────────────────
  function sortByMes(s) {
    return (s || []).slice().sort(function (a, b) {
      return String(a.mes).localeCompare(String(b.mes));
    });
  }

  // SVG sparkline encapsulado: viewBox 230x46 dentro del recuadro (.ich-spark__plot, 230x44),
  // preserveAspectRatio="none", línea 2px (non-scaling-stroke) + fill gradiente.
  function sparkline(boxId, msgId, serie, valueKey, color, gradId, emptyText) {
    var box = document.getElementById(boxId);
    var plot = box ? box.querySelector('.ich-spark__plot') : null;
    if (!plot) return;

    var pts = sortByMes(serie);
    var msg = document.getElementById(msgId);
    if (!pts.length) {
      // Sin serie (tabla aún sin poblar / corrida sin datos): fallback discreto visible,
      // nunca un "cargando…" infinito. El recuadro queda igual de visible.
      if (msg) { msg.hidden = false; msg.textContent = emptyText || 'Sin datos'; }
      return;
    }
    if (msg) msg.hidden = true;

    var W = 230, H = 46, PAD = 6;
    var vals = pts.map(function (p) { return p[valueKey]; });
    var n = vals.length;
    var min = Math.min.apply(null, vals), max = Math.max.apply(null, vals);
    var span = (max - min) || 1;
    var X = function (i) { return n <= 1 ? 0 : (i / (n - 1)) * W; };
    var Y = function (v) { return H - PAD - ((v - min) / span) * (H - 2 * PAD); };

    var d = '';
    for (var i = 0; i < n; i++) { d += (i === 0 ? 'M' : 'L') + X(i).toFixed(1) + ',' + Y(vals[i]).toFixed(1); }
    var area = d + 'L' + W + ',' + H + 'L0,' + H + 'Z';

    var svg =
      '<svg class="ich-spark__svg" viewBox="0 0 ' + W + ' ' + H + '" preserveAspectRatio="none" aria-hidden="true">' +
        '<defs><linearGradient id="' + gradId + '" x1="0" y1="0" x2="0" y2="1">' +
          '<stop offset="0%" stop-color="' + color + '" stop-opacity="0.22"/>' +
          '<stop offset="100%" stop-color="' + color + '" stop-opacity="0"/>' +
        '</linearGradient></defs>' +
        '<path class="ich-spark__fill" d="' + area + '" fill="url(#' + gradId + ')"/>' +
        '<path class="ich-spark__line" d="' + d + '" fill="none" stroke="' + color + '" stroke-width="2" ' +
          'pathLength="1" vector-effect="non-scaling-stroke"/>' +
      '</svg>';
    plot.insertAdjacentHTML('afterbegin', svg);

    var line = plot.querySelector('.ich-spark__line');
    var fill = plot.querySelector('.ich-spark__fill');
    if (REDUCE || !line) return;

    // Dibujo izq→der vía dashoffset (pathLength normalizado a 1); fill después.
    line.style.strokeDasharray = '1';
    line.style.strokeDashoffset = '1';
    fill.style.opacity = '0';
    requestAnimationFrame(function () {
      requestAnimationFrame(function () {
        line.style.transition = 'stroke-dashoffset 1.1s ease';
        line.style.strokeDashoffset = '0';
        fill.style.transition = 'opacity 0.5s ease 0.55s';
        fill.style.opacity = '1';
      });
    });
  }

  // ── Init ────────────────────────────────────────────────────────
  function init() {
    runCountUps();
    var red = T ? T.colors.red : '#dc2626';
    var navy = T ? T.colors.navy : '#06486f';
    var amber = T ? T.colors.amber : '#d97706';
    sparkline('ich-spark-rentab', 'ich-spark-rentab-msg', DATA.rentabMeses, 'utilidad', red, 'ich-grad-rentab');
    sparkline('ich-spark-labs', 'ich-spark-labs-msg', DATA.labsMeses, 'unidades', navy, 'ich-grad-labs');
    // Inflación: serie PRECALCULADA server-side (índice mensual), mismo SVG que rentab/labs.
    // Si viene vacía → fallback discreto "sin datos aún" (naranja tenue), no "cargando…".
    sparkline('ich-spark-inflacion', 'ich-spark-inflacion-msg', DATA.inflacionMeses,
              'inflacion_pvp_indice', amber, 'ich-grad-inflacion', 'sin datos aún');
  }

  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }
})();
