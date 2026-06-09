/**
 * Indicadores Comerciales — Inflación PVP
 * Variación mensual de precios públicos con comparativa IPC INDEC.
 */

const INF = (() => {
  'use strict';

  let _cfg = {};
  let _resumen = null;
  let _productos = [];
  let _productosFiltro = [];
  let _filtroActivo = 'all';
  let _charts = {};
  let _loading = false;
  let _sortState = { col: 'facturacion', dir: 'desc' };

  // ── Formatters ───────────────────────────────────────────────────────────────
  const _fmtPct = (v, digits = 1) => {
    if (v == null || isNaN(v)) return '—';
    const sign = v >= 0 ? '+' : '';
    return sign + (v * 100).toFixed(digits) + ' %';
  };
  const _fmtPctRaw = (v) => {
    if (v == null || isNaN(v)) return '—';
    const sign = v >= 0 ? '+' : '';
    return sign + parseFloat(v).toFixed(1) + ' %';
  };
  const _fmtARS = (v) => {
    if (v == null || isNaN(v)) return '—';
    const abs = Math.abs(v);
    if (abs >= 1_000_000) return '$ ' + (v/1_000_000).toFixed(1) + ' M';
    return '$ ' + Math.round(v).toLocaleString('es-AR');
  };
  // Moneda argentina completa: "$ 1.234.567,89" (solo display; no altera el valor que se ordena/calcula)
  const _fmtPesos = (v) => {
    if (v == null || isNaN(v)) return '—';
    return '$ ' + Number(v).toLocaleString('es-AR', { minimumFractionDigits: 2, maximumFractionDigits: 2 });
  };
  const _fmtNum = (v) => {
    if (v == null || isNaN(v)) return '—';
    return Math.round(parseFloat(v)).toLocaleString('es-AR');
  };
  const _fmtMonth = (s) => {
    if (!s) return '—';
    const parts = String(s).split('-');
    if (parts.length < 2) return s;
    const ms = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
    return `${ms[parseInt(parts[1])-1]}-${parts[0].slice(2)}`;
  };
  const $ = (id) => document.getElementById(id);
  const setText = (id, v) => { const el = $(id); if (el) el.textContent = v; };
  const escHtml = (s) => String(s||'').replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  // Estado badge por estado_calculo
  const _estadoBadge = {
    'COMPARABLE_CON_AUMENTO': 'ind-badge--rojo',
    'COMPARABLE_CON_BAJA': 'ind-badge--verde',
    'COMPARABLE_SIN_CAMBIO': 'ind-badge--gris',
    'ALTA_PERIODO_SIN_PRECIO_INICIAL': 'ind-badge--ambar',
    'ALTA_REINCORPORADO': 'ind-badge--ambar',
    'SIN_PRECIO_FINAL': 'ind-badge--gris',
    'SIN_PRECIOS': 'ind-badge--gris',
    'REVISAR': 'ind-badge--ambar',
  };
  const _estadoLabel = {
    'COMPARABLE_CON_AUMENTO': 'Con aumento',
    'COMPARABLE_CON_BAJA': 'Con baja',
    'COMPARABLE_SIN_CAMBIO': 'Sin cambio',
    'ALTA_PERIODO_SIN_PRECIO_INICIAL': 'Alta período',
    'ALTA_REINCORPORADO': 'Reincorporado',
    'SIN_PRECIO_FINAL': 'Sin PF',
    'SIN_PRECIOS': 'Sin precios',
    'REVISAR': 'Revisar',
  };

  // ── Init ────────────────────────────────────────────────────────────────────
  function init(cfg) {
    _cfg = cfg;
    _setupDates();
    _bindEvents();
    _checkHealth();
  }

  // Mes en formato YYYY-MM a partir de componentes locales (sin desfase de zona horaria)
  const _ym = (dt) => `${dt.getFullYear()}-${String(dt.getMonth() + 1).padStart(2, '0')}`;

  function _setupDates() {
    // Default: mes anterior completo (mismo mes en ambos selectores)
    const now = new Date();
    const desde = $('inf-desde');
    const hasta = $('inf-hasta');
    const prevMonth = _ym(new Date(now.getFullYear(), now.getMonth() - 1, 1));
    // Los <input type="month"> esperan value YYYY-MM; los defaults/cfg vienen YYYY-MM-DD → recortar.
    if (desde && !desde.value) desde.value = prevMonth;
    if (hasta && !hasta.value) hasta.value = prevMonth;
    if (desde && _cfg.desde) desde.value = String(_cfg.desde).slice(0, 7);
    if (hasta && _cfg.hasta) hasta.value = String(_cfg.hasta).slice(0, 7);
  }

  function _bindEvents() {
    $('inf-btn-apply')?.addEventListener('click', applyFilters);
    $('inf-btn-clear')?.addEventListener('click', clearFilters);
    $('inf-btn-clear2')?.addEventListener('click', clearFilters);
    $('inf-btn-retry')?.addEventListener('click', applyFilters);
    $('inf-btn-export')?.addEventListener('click', exportCSV);

    // Pills filtro de tabla
    document.querySelectorAll('#inf-filter-pills .ind-pill').forEach(btn => {
      btn.addEventListener('click', () => {
        document.querySelectorAll('#inf-filter-pills .ind-pill').forEach(b => b.classList.remove('ind-pill--active'));
        btn.classList.add('ind-pill--active');
        _filtroActivo = btn.dataset.filter;
        _applyTableFilter();
      });
    });

    // Sort tabla
    document.querySelectorAll('#inf-tabla-productos th[data-sort]').forEach(th => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => _sortProductos(th.dataset.sort));
    });

    // Enter en search
    $('inf-search')?.addEventListener('keydown', e => { if (e.key === 'Enter') applyFilters(); });
  }

  // ── Health ──────────────────────────────────────────────────────────────────
  async function _checkHealth() {
    const banner = $('inf-sql-banner');
    const icon = $('inf-sql-icon');
    const text = $('inf-sql-text');
    if (!banner) return;
    banner.style.display = 'flex';
    try {
      const r = await fetch(_cfg.apiBase + '/health', { signal: AbortSignal.timeout(8000) });
      const d = await r.json();
      if (d.fusion && d.etl) {
        banner.className = 'ind-sql-banner ind-sql-banner--ok';
        if (icon) icon.className = 'bi bi-check-circle-fill';
        if (text) text.textContent = 'SQL Server disponible — seleccioná un período y consultá';
        setTimeout(() => { banner.style.display = 'none'; }, 4000);
      } else {
        banner.className = 'ind-sql-banner ind-sql-banner--error';
        if (icon) icon.className = 'bi bi-exclamation-triangle-fill';
        if (text) text.textContent = 'SQL Server no disponible. Verificá la VPN.';
      }
    } catch {
      banner.className = 'ind-sql-banner ind-sql-banner--error';
      if (icon) icon.className = 'bi bi-x-circle-fill';
      if (text) text.textContent = 'No se pudo verificar SQL Server.';
    }
    // Cargar INDEC independientemente del SQL
    _fetchIndec();
  }

  // ── INDEC IPC ───────────────────────────────────────────────────────────────
  async function _fetchIndec(desde, hasta) {
    try {
      let url = `${_cfg.indecBase}/ipc`;
      if (desde && hasta) url += `?desde=${desde}&hasta=${hasta}`;
      const r = await fetch(url, { signal: AbortSignal.timeout(15000) });
      if (!r.ok) return;
      const d = await r.json();
      const gen = d.nivel_general;
      const sal = d.salud;
      setText('inf-kv-indec-general', gen?.valor != null ? _fmtPctRaw(gen.valor) + '/mes' : '—');
      setText('inf-kv-indec-salud', sal?.valor != null ? _fmtPctRaw(sal.valor) + '/mes' : '—');
      setText('inf-kv-indec-periodo', d.periodo ? `Período: ${d.periodo}` : 'Último dato publicado');
    } catch (err) {
      console.warn('INF: INDEC error', err);
      setText('inf-kv-indec-general', 'N/D');
      setText('inf-kv-indec-salud', 'N/D');
    }
  }

  // ── Apply / Clear ────────────────────────────────────────────────────────────
  function applyFilters() {
    if (_loading) return;
    _loadData();
  }

  function clearFilters() {
    ['inf-negocio'].forEach(id => { const el = $(id); if (el) el.value = ''; });
    ['inf-search'].forEach(id => { const el = $(id); if (el) el.value = ''; });
    _clearResults();
  }

  function _clearResults() {
    _resumen = null;
    _productos = [];
    _productosFiltro = [];
    _setKPIsDash();
    $('inf-tbody-productos').innerHTML = `<tr><td colspan="10" class="text-center text-muted py-5">
      <i class="bi bi-funnel fs-3 d-block mb-2 opacity-25"></i>
      Seleccioná un período y hacé clic en <strong>Consultar</strong></td></tr>`;
    $('inf-tbody-labs').innerHTML = '<tr><td colspan="3" class="text-center text-muted py-3">—</td></tr>';
    $('inf-empty').style.display = 'none';
    $('inf-table-footer').style.display = 'none';
    if (_charts.evolucion) { _charts.evolucion.destroy(); _charts.evolucion = null; }
    $('inf-chart-empty').style.display = 'flex';
  }

  // Convierte la selección de mes (YYYY-MM) al rango de fechas que espera el backend.
  // hasta = primer día del mes SIGUIENTE al "mes fin": límite superior exclusivo, así
  // facturación (`fecha < hasta`) cubre el mes fin COMPLETO sin perder el último día,
  // y get_evolucion sigue iterando correctamente. El backend no se toca.
  function _mesARango(mesDesde, mesFin) {
    const desde = `${mesDesde}-01`;
    const [y, m] = mesFin.split('-').map(Number);       // m es 1-based
    const next = new Date(y, m, 1);                       // Date usa mes 0-based → mes siguiente
    const hasta = `${next.getFullYear()}-${String(next.getMonth() + 1).padStart(2, '0')}-01`;
    return { desde, hasta };
  }

  // ── Load ─────────────────────────────────────────────────────────────────────
  async function _loadData() {
    _loading = true;
    _setLoading(true);
    _hideError();

    const mesDesde = $('inf-desde')?.value;
    const mesHasta = $('inf-hasta')?.value;
    if (!mesDesde || !mesHasta) {
      _showError('Seleccioná mes inicio y mes fin.');
      _loading = false;
      _setLoading(false);
      return;
    }
    // Convertir meses (YYYY-MM) al rango de fechas (YYYY-MM-DD) que espera el backend
    const { desde, hasta } = _mesARango(mesDesde, mesHasta);

    const p = new URLSearchParams({ desde, hasta });
    const neg = $('inf-negocio')?.value;
    const srch = $('inf-search')?.value?.trim();
    if (neg) p.set('cadneg', neg);
    if (srch) p.set('search', srch);
    const qs = p.toString();

    try {
      const [resRes, prodRes, evolRes, labsRes] = await Promise.all([
        fetch(`${_cfg.apiBase}/resumen?${qs}`, { signal: AbortSignal.timeout(120000) }),
        fetch(`${_cfg.apiBase}/productos?${qs}`, { signal: AbortSignal.timeout(120000) }),
        fetch(`${_cfg.apiBase}/evolucion?${qs}`, { signal: AbortSignal.timeout(120000) }),
        fetch(`${_cfg.apiBase}/laboratorios?${qs}`, { signal: AbortSignal.timeout(120000) }),
      ]);

      if (!resRes.ok) throw new Error(`Resumen: HTTP ${resRes.status}`);

      _resumen = await resRes.json();
      _productos = prodRes.ok ? await prodRes.json() : [];
      const evolucion = evolRes.ok ? await evolRes.json() : [];
      const labs = labsRes.ok ? await labsRes.json() : [];

      _renderKPIs();
      _renderEvolucion(evolucion);
      _renderLabsTable(labs);
      _applyTableFilter();
      _fetchIndec(desde, hasta);

      $('inf-btn-export').disabled = false;
      $('inf-empty').style.display = _productosFiltro.length === 0 ? 'flex' : 'none';
    } catch (err) {
      _showError(err.name === 'AbortError'
        ? 'La consulta tardó demasiado. Verificá la VPN o reducí el rango de fechas.'
        : (err.message || 'Error al consultar el servidor.'));
    } finally {
      _loading = false;
      _setLoading(false);
    }
  }

  // ── KPIs ─────────────────────────────────────────────────────────────────────
  function _setKPIsDash() {
    const ids = ['inf-kv-ponderada','inf-kv-indice','inf-kv-mayor-pct','inf-kv-mayor-desc',
                 'inf-kv-total','inf-kv-comparables','inf-kv-con-aumento','inf-kv-sin-pf',
                 'inf-kv-facturacion','inf-kv-sin-cambio','inf-kv-altas','inf-kv-cobertura'];
    ids.forEach(id => setText(id, '—'));
  }

  function _renderKPIs() {
    if (!_resumen) return;
    const r = _resumen;
    setText('inf-kv-ponderada', _fmtPct(r.inflacion_pvp_ponderada_facturacion));
    setText('inf-kv-indice', _fmtPct(r.inflacion_pvp_indice));
    setText('inf-kv-total', _fmtNum(r.total_productos));
    setText('inf-kv-comparables', _fmtNum(r.productos_comparables));
    setText('inf-kv-con-aumento', _fmtNum(r.productos_con_aumento));
    setText('inf-kv-sin-pf', _fmtNum(r.productos_sin_precio_final));
    setText('inf-kv-facturacion', _fmtPesos(r.facturacion_total));
    setText('inf-kv-sin-cambio', _fmtNum(r.productos_sin_cambio));
    setText('inf-kv-altas', _fmtNum(r.productos_alta_periodo));
    setText('inf-kv-cobertura', r.cobertura_facturacion != null ? (r.cobertura_facturacion*100).toFixed(1)+'%' : '—');
    if (r.mayor_aumento) {
      setText('inf-kv-mayor-pct', _fmtPct(r.mayor_aumento.variacion));
      setText('inf-kv-mayor-desc', r.mayor_aumento.descripcion || String(r.mayor_aumento.articulo));
    }
  }

  // ── Gráfico evolución ────────────────────────────────────────────────────────
  function _renderEvolucion(evolucion) {
    const canvas = $('inf-chart-evolucion');
    const empty = $('inf-chart-empty');
    if (!canvas) return;
    if (!evolucion?.length) {
      if (empty) empty.style.display = 'flex';
      return;
    }
    if (empty) empty.style.display = 'none';
    if (_charts.evolucion) _charts.evolucion.destroy();

    const labels = evolucion.map(e => _fmtMonth(e.mes));
    const pvpData = evolucion.map(e => e.inflacion_pvp_indice != null ? (e.inflacion_pvp_indice * 100) : null);

    _charts.evolucion = new Chart(canvas, {
      type: 'line',
      data: {
        labels,
        datasets: [{
          label: 'Inflación PVP Índice',
          data: pvpData,
          borderColor: 'rgb(239,68,68)',
          backgroundColor: 'rgba(239,68,68,0.1)',
          borderWidth: 2,
          pointRadius: 4,
          tension: 0.3,
          fill: true,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: {
          legend: { position: 'bottom' },
          tooltip: { callbacks: { label: ctx => (ctx.raw != null ? ctx.raw.toFixed(2) + '%' : '—') } }
        },
        scales: {
          y: {
            ticks: { callback: v => v.toFixed(1) + '%' },
            grid: { color: 'rgba(0,0,0,0.05)' }
          },
          x: { grid: { display: false } }
        }
      }
    });
  }

  // ── Tabla laboratorios ───────────────────────────────────────────────────────
  function _renderLabsTable(labs) {
    const tbody = $('inf-tbody-labs');
    if (!tbody) return;
    if (!labs?.length) {
      tbody.innerHTML = '<tr><td colspan="3" class="text-center text-muted py-3">Sin datos</td></tr>';
      return;
    }
    tbody.innerHTML = labs.slice(0, 20).map(l => {
      const pct = _fmtPct(l.inflacion_indice);
      const cls = (l.inflacion_indice || 0) > 0 ? 'text-danger' : (l.inflacion_indice || 0) < 0 ? 'text-success' : '';
      return `<tr>
        <td>${escHtml(l.laboratorio)}</td>
        <td class="text-end ${cls} fw-semibold">${pct}</td>
        <td class="text-end">${_fmtARS(l.facturacion)}</td>
      </tr>`;
    }).join('');
  }

  // ── Tabla productos ──────────────────────────────────────────────────────────
  function _applyTableFilter() {
    let rows = _productos;
    switch (_filtroActivo) {
      case 'comp': rows = rows.filter(r => r.es_comparable === 1); break;
      case 'alta': rows = rows.filter(r =>
        r.estado_calculo === 'ALTA_PERIODO_SIN_PRECIO_INICIAL' || r.estado_calculo === 'ALTA_REINCORPORADO'); break;
      case 'sin_pf': rows = rows.filter(r => r.estado_calculo === 'SIN_PRECIO_FINAL'); break;
    }
    // Aplicar sort
    const col = _sortState.col;
    const mul = _sortState.dir === 'asc' ? 1 : -1;
    rows = [...rows].sort((a, b) => {
      const av = a[col] ?? -Infinity;
      const bv = b[col] ?? -Infinity;
      if (typeof av === 'number') return (av - bv) * mul;
      return String(av).localeCompare(String(bv)) * mul;
    });
    _productosFiltro = rows;
    _renderProductosTable(rows);
  }

  function _renderProductosTable(rows) {
    const tbody = $('inf-tbody-productos');
    const footer = $('inf-table-footer');
    if (!tbody) return;

    setText('inf-table-count', rows.length + ' productos');
    if (footer) footer.style.display = rows.length ? '' : 'none';
    setText('inf-footer-count', rows.length + ' productos');

    if (!rows.length) {
      tbody.innerHTML = '<tr><td colspan="10" class="text-center text-muted py-4">Sin productos para este filtro</td></tr>';
      return;
    }

    tbody.innerHTML = rows.slice(0, 2000).map(r => {
      const estado = r.estado_calculo || '';
      const badge = _estadoBadge[estado] || 'ind-badge--gris';
      const label = _estadoLabel[estado] || estado;
      const varPct = r.variacion_pvp != null ? _fmtPct(r.variacion_pvp) : '—';
      const varCls = (r.variacion_pvp || 0) > 0 ? 'text-danger fw-semibold' : (r.variacion_pvp || 0) < 0 ? 'text-success fw-semibold' : '';
      return `<tr>
        <td class="text-muted small">${escHtml(r.articulo||'')}</td>
        <td>${escHtml(r.descripcion||'')}</td>
        <td>${escHtml(r.laboratorio||'')}</td>
        <td><span class="ind-badge ${badge}">${escHtml(label)}</span></td>
        <td class="text-end">${_fmtPesos(r.pvp_inicial)}</td>
        <td class="text-muted small">${escHtml(r.fecha_inicial||'')}</td>
        <td class="text-end">${_fmtPesos(r.pvp_final)}</td>
        <td class="text-muted small">${escHtml(r.fecha_final||'')}</td>
        <td class="text-end">${_fmtPesos(r.facturacion)}</td>
        <td class="text-end ${varCls}">${varPct}</td>
      </tr>`;
    }).join('');
  }

  function _sortProductos(col) {
    if (_sortState.col === col) {
      _sortState.dir = _sortState.dir === 'asc' ? 'desc' : 'asc';
    } else {
      _sortState = { col, dir: 'desc' };
    }
    _applyTableFilter();
  }

  // ── Export CSV ───────────────────────────────────────────────────────────────
  function exportCSV() {
    if (!_productosFiltro.length) return;
    const rows = [['Artículo','Descripción','Laboratorio','Estado','PVP Inicial','Fecha PI','PVP Final','Fecha PF','Facturación','Variación']];
    _productosFiltro.forEach(r => {
      rows.push([
        r.articulo||'', r.descripcion||'', r.laboratorio||'', r.estado_calculo||'',
        r.pvp_inicial||'', r.fecha_inicial||'', r.pvp_final||'', r.fecha_final||'',
        r.facturacion||0, r.variacion_pvp != null ? (r.variacion_pvp*100).toFixed(2)+'%' : ''
      ]);
    });
    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿'+csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `inflacion_pvp_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
  }

  // ── UI helpers ───────────────────────────────────────────────────────────────
  function _setLoading(on) {
    const el = $('inf-loading');
    if (el) el.style.display = on ? 'flex' : 'none';
  }
  function _showError(msg) {
    const el = $('inf-error');
    if (!el) return;
    el.style.display = 'flex';
    setText('inf-error-msg', msg);
  }
  function _hideError() {
    const el = $('inf-error');
    if (el) el.style.display = 'none';
  }

  // ── Canvas → imagen temporal ─────────────────────────────────────────────────
  function _snapshotCanvases() {
    const snaps = [];
    document.querySelectorAll('canvas').forEach(canvas => {
      try {
        if (!canvas.width || !canvas.height) return;
        const img = document.createElement('img');
        img.src = canvas.toDataURL('image/png');
        img.className = 'ind-print-canvas-img';
        img.style.display = 'none';
        canvas.parentNode.insertBefore(img, canvas.nextSibling);
        snaps.push({ canvas, img });
      } catch { /* canvas tainted */ }
    });
    return snaps;
  }

  function _restoreCanvases(snaps) {
    snaps.forEach(({ img }) => img.remove());
  }

  // ── Imprimir informe ─────────────────────────────────────────────────────────
  async function printReport() {
    if (!_resumen) {
      alert('Consultá los datos primero antes de imprimir.');
      return;
    }

    const btn = $('inf-btn-print-report');
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="ind-spinner" style="width:13px;height:13px;border-width:2px;display:inline-block;margin-right:6px"></span>Preparando…';
    }

    const desde = $('inf-desde')?.value || '';
    const hasta = $('inf-hasta')?.value || '';
    const neg   = $('inf-negocio')?.value;
    const srch  = $('inf-search')?.value?.trim();

    const filtrosActivos = [
      neg  && `Negocio: ${neg}`,
      srch && `Búsqueda: ${srch}`,
    ].filter(Boolean).join('  ·  ');

    // Los selectores son de mes (YYYY-MM) → mostrar el período como MM/AAAA
    const _fmtMesAnio = (s) => {
      if (!s) return '';
      const [y, m] = String(s).split('-');
      return m ? `${m}/${y}` : String(s);
    };

    const _setTxt = (id, v) => { const el = $(id); if (el) el.textContent = v; };
    _setTxt('inf-ph-rango',   `Período: ${_fmtMesAnio(desde)} — ${_fmtMesAnio(hasta)}`);
    _setTxt('inf-ph-filtros', filtrosActivos ? `Filtros: ${filtrosActivos}` : 'Sin filtros adicionales');
    _setTxt('inf-ph-emitido', `Emitido: ${new Date().toLocaleString('es-AR')}`);

    const ph = $('inf-print-header');
    if (ph) ph.style.display = '';

    const snaps = _snapshotCanvases();

    await new Promise(r => requestAnimationFrame(r));
    await new Promise(r => requestAnimationFrame(r));
    await new Promise(r => setTimeout(r, 400));

    window.print();

    setTimeout(() => {
      _restoreCanvases(snaps);
      if (ph) ph.style.display = 'none';
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-printer me-1"></i>Imprimir';
      }
    }, 600);
  }

  return { init, applyFilters, clearFilters, exportCSV, printReport };
})();
