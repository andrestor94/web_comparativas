/**
 * Indicadores Comerciales — Informes de Laboratorio
 * Sell out mensual de unidades por laboratorio, marca y cliente.
 */

const LAB = (() => {
  'use strict';

  let _cfg = {};
  let _summary = null;
  let _detail = [];
  let _months = [];
  let _visibleYears = new Set();
  let _charts = {};
  let _loading = false;

  // ── Formatters ───────────────────────────────────────────────────────────────
  const _fmtNum = (v) => {
    if (v == null || isNaN(v)) return '—';
    return Math.round(parseFloat(v)).toLocaleString('es-AR');
  };
  const _fmtPct = (v) => {
    if (v == null || isNaN(v)) return '—';
    const sign = v >= 0 ? '+' : '';
    return sign + (v * 100).toFixed(1) + ' %';
  };
  const _fmtMonth = (s) => {
    if (!s) return '—';
    const [y, m] = s.split('-');
    const ms = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
    return `${ms[parseInt(m)-1]}-${y.slice(2)}`;
  };
  const $ = (id) => document.getElementById(id);
  const setText = (id, v) => { const el = $(id); if (el) el.textContent = v; };
  const escHtml = (s) => String(s).replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  // ── Init ────────────────────────────────────────────────────────────────────
  function init(cfg) {
    _cfg = cfg;
    _setupDates();
    _bindEvents();
    // Semáforo de Fusion reemplazado por el indicador de frescura de datos
    // (server-side, partial _ind_data_freshness.html). Ya no se chequea /health
    // en el arranque. _checkHealth() queda definida abajo pero sin invocar.
    // _checkHealth();
  }

  function _setupDates() {
    const d = $('lab-desde');
    const h = $('lab-hasta');
    if (d && !d.value) d.value = _cfg.desde || new Date().getFullYear() + '-01-01';
    if (h && !h.value) h.value = _cfg.hasta || new Date().toISOString().slice(0,10);
  }

  function _bindEvents() {
    const apply = $('lab-btn-apply');
    const clear = $('lab-btn-clear');
    const clear2 = $('lab-btn-clear2');
    const retry = $('lab-btn-retry');
    const expBtn = $('lab-btn-export');

    if (apply) apply.addEventListener('click', applyFilters);
    if (clear) clear.addEventListener('click', clearFilters);
    if (clear2) clear2.addEventListener('click', clearFilters);
    if (retry) retry.addEventListener('click', applyFilters);
    if (expBtn) expBtn.addEventListener('click', exportCSV);

    // Sort en tabla detalle
    document.querySelectorAll('#lab-tabla-detalle th[data-sort]').forEach(th => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => _sortDetail(th.dataset.sort));
    });

    // Year controls
    const yc = $('lab-year-controls');
    if (yc) yc.addEventListener('click', (e) => {
      const btn = e.target.closest('button[data-year]');
      if (btn) _toggleYear(btn.dataset.year);
    });
  }

  // ── Health check ────────────────────────────────────────────────────────────
  async function _checkHealth() {
    const banner = $('lab-sql-banner');
    const icon = $('lab-sql-icon');
    const text = $('lab-sql-text');
    if (!banner) return;
    banner.style.display = 'flex';
    try {
      const r = await fetch(_cfg.apiBase + '/health', { signal: AbortSignal.timeout(8000) });
      const d = await r.json();
      if (d.etl && d.fusion) {
        banner.className = 'ind-sql-banner ind-sql-banner--ok';
        if (icon) icon.className = 'bi bi-check-circle-fill';
        if (text) text.textContent = 'SQL Server disponible — datos listos para consultar';
        setTimeout(() => { banner.style.display = 'none'; }, 4000);
        _loadMetadata();
      } else {
        _setBannerError(banner, icon, text, 'SQL Server no disponible. Verificá la VPN.');
      }
    } catch {
      _setBannerError(banner, icon, text, 'No se pudo verificar SQL Server. Revisá la red o VPN.');
    }
  }

  function _setBannerError(banner, icon, text, msg) {
    banner.className = 'ind-sql-banner ind-sql-banner--error';
    if (icon) icon.className = 'bi bi-exclamation-triangle-fill';
    if (text) text.textContent = msg;
  }

  // ── Metadata (fill dropdowns) ────────────────────────────────────────────────
  async function _loadMetadata() {
    const params = _buildParams();
    try {
      const r = await fetch(`${_cfg.apiBase}/metadata?${params}`, { signal: AbortSignal.timeout(90000) });
      if (!r.ok) return;
      const meta = await r.json();
      _fillSelect('lab-laboratorio', meta.laboratorios || [], 'Todos los laboratorios');
      _fillSelect('lab-familia', meta.familias || [], 'Todas las familias');
      _fillDatalist('lab-clientes-list', meta.clientes || []);
    } catch (err) {
      console.warn('LAB: metadata error', err);
    }
  }

  function _fillSelect(id, items, placeholder) {
    const el = $(id);
    if (!el) return;
    const current = el.value;
    el.innerHTML = `<option value="">${escHtml(placeholder)}</option>` +
      items.map(i => `<option value="${escHtml(i)}"${i === current ? ' selected' : ''}>${escHtml(i)}</option>`).join('');
  }

  function _fillDatalist(id, items) {
    const dl = $(id);
    if (!dl) return;
    dl.innerHTML = items.map(i => `<option value="${escHtml(i)}">`).join('');
  }

  // ── Filters ──────────────────────────────────────────────────────────────────
  function _buildParams() {
    const desde = $('lab-desde')?.value || _cfg.desde;
    const hasta = $('lab-hasta')?.value || _cfg.hasta;
    const hastaExcl = _addDay(hasta);
    const p = new URLSearchParams();
    p.set('desde', desde);
    p.set('hasta', hastaExcl);
    const lab = $('lab-laboratorio')?.value;
    const fam = $('lab-familia')?.value;
    const cli = $('lab-cliente')?.value?.trim();
    const neg = $('lab-negocio')?.value;
    const srch = $('lab-search')?.value?.trim();
    if (lab) p.set('laboratorio', lab);
    if (fam) p.set('familia', fam);
    if (cli) p.set('cliente', cli);
    if (neg) p.set('cadneg', neg);
    if (srch) p.set('search', srch);
    return p.toString();
  }

  function _addDay(dateStr) {
    const d = new Date(dateStr + 'T00:00:00');
    d.setDate(d.getDate() + 1);
    return d.toISOString().slice(0, 10);
  }

  function applyFilters() {
    if (_loading) return;
    _loadData();
  }

  function clearFilters() {
    const ids = ['lab-laboratorio','lab-familia','lab-negocio'];
    ids.forEach(id => { const el = $(id); if (el) el.value = ''; });
    ['lab-cliente','lab-search'].forEach(id => { const el = $(id); if (el) el.value = ''; });
    _clearResults();
  }

  function _clearResults() {
    _summary = null;
    _detail = [];
    $('lab-empty').style.display = 'none';
    $('lab-period-info').style.display = 'none';
    $('lab-table-footer').style.display = 'none';
    const placeholder = '<tr><td colspan="20" class="text-center text-muted py-4">Aplicá filtros y hacé clic en Consultar</td></tr>';
    ['lab-prod-tbody', 'lab-tbody', 'lab-cli-tbody'].forEach(id => { const el = $(id); if (el) el.innerHTML = placeholder; });
  }

  // ── Load data ────────────────────────────────────────────────────────────────
  async function _loadData() {
    _loading = true;
    _setLoading(true);
    _hideError();

    const params = _buildParams();
    try {
      const [sumRes, detRes] = await Promise.all([
        fetch(`${_cfg.apiBase}/resumen?${params}`, { signal: AbortSignal.timeout(120000) }),
        fetch(`${_cfg.apiBase}/detalle?${params}`, { signal: AbortSignal.timeout(120000) }),
      ]);

      if (!sumRes.ok) throw new Error(`Resumen: HTTP ${sumRes.status}`);
      if (!detRes.ok) throw new Error(`Detalle: HTTP ${detRes.status}`);

      _summary = await sumRes.json();
      _detail = await detRes.json();
      _months = (_summary.meses || []).map(m => m.mes);
      _visibleYears = new Set(_months.map(m => m.slice(0,4)));

      _renderKPIs();
      _renderEvolucion();
      _renderLabsChart();
      _renderMatrices();
      _renderPeriodInfo();
      _setExportEnabled(true);

      if (_detail.length === 0) {
        $('lab-empty').style.display = 'flex';
      } else {
        $('lab-empty').style.display = 'none';
      }
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
  function _renderKPIs() {
    if (!_summary) return;
    const s = _summary;
    const topLab = (s.laboratorios || [])[0];
    const topMarca = (s.marcas || [])[0];
    const total = s.total_unidades || 0;
    const meses = (s.meses || []).length;

    setText('lab-kv-unidades', _fmtNum(total));
    setText('lab-kv-periodo', meses ? `${_fmtMonth(_months[0])} — ${_fmtMonth(_months[_months.length-1])}` : '—');
    setText('lab-kv-promedio', _fmtNum(s.promedio_mensual));
    setText('lab-kv-variacion', _fmtPct(s.variacion_mensual));
    setText('lab-kv-top-lab', topLab?.name || '—');
    setText('lab-kv-top-lab-sub', topLab ? `${_fmtNum(topLab.value)} unidades` : '—');
    setText('lab-kv-labs', _fmtNum(s.cantidad_laboratorios));
    setText('lab-kv-marcas', _fmtNum(s.cantidad_marcas));
    setText('lab-kv-clientes', _fmtNum(s.cantidad_clientes));
    setText('lab-kv-top-marca', topMarca?.name || '—');
    setText('lab-kv-top-marca-sub', topMarca ? `${_fmtNum(topMarca.value)} unidades` : '—');
  }

  // ── Gráfico evolución ────────────────────────────────────────────────────────
  function _renderEvolucion() {
    const canvas = $('lab-chart-evolucion');
    const empty = $('lab-chart-empty');
    if (!canvas) return;
    const meses = _summary?.meses || [];
    if (!meses.length) {
      if (empty) empty.style.display = 'flex';
      return;
    }
    if (empty) empty.style.display = 'none';
    if (_charts.evolucion) { _charts.evolucion.destroy(); }

    const labels = meses.map(m => _fmtMonth(m.mes));
    const data = meses.map(m => m.unidades);

    // Presentación vía IC_CHART_THEME (paleta de marca); fallback al estilo previo
    const T = window.IC_CHART_THEME;

    _charts.evolucion = new Chart(canvas, {
      type: 'bar',
      data: {
        labels,
        datasets: [{
          label: 'Unidades',
          data,
          backgroundColor: T ? T.alpha(T.colors.blue, 0.82) : 'rgba(59,130,246,0.7)',
          hoverBackgroundColor: T ? T.colors.blue : 'rgba(59,130,246,1)',
          borderWidth: 0,
          borderRadius: 5,
          maxBarThickness: 36,
        }]
      },
      options: {
        responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false },
          tooltip: { callbacks: { label: ctx => _fmtNum(ctx.raw) + ' unidades' } } },
        scales: {
          x: T ? T.gridX() : { grid: { display: false } },
          y: T ? T.gridY(v => _fmtNum(v)) : { ticks: { callback: v => _fmtNum(v) } }
        }
      }
    });
    if ($('lab-chart-badge')) $('lab-chart-badge').textContent =
      `${labels[0]} — ${labels[labels.length-1]}`;
  }

  // ── Gráfico labs ─────────────────────────────────────────────────────────────
  function _renderLabsChart() {
    const canvas = $('lab-chart-labs');
    if (!canvas) return;
    const labs = (_summary?.laboratorios || []).slice(0, 12);
    if (_charts.labs) { _charts.labs.destroy(); }

    // Ranking: rampa de opacidad sobre el navy de marca (IC_CHART_THEME)
    const T = window.IC_CHART_THEME;

    _charts.labs = new Chart(canvas, {
      type: 'bar',
      data: {
        labels: labs.map(l => l.name),
        datasets: [{
          data: labs.map(l => l.value),
          backgroundColor: T ? T.ramp(T.colors.navy, labs.length) : labs.map((_, i) => `hsl(${210 + i*12},70%,55%)`),
          borderRadius: 4,
          maxBarThickness: 18,
        }]
      },
      options: {
        indexAxis: 'y', responsive: true, maintainAspectRatio: false,
        plugins: { legend: { display: false },
          tooltip: { callbacks: { label: ctx => _fmtNum(ctx.raw) + ' unidades' } } },
        scales: {
          x: T ? T.gridY(v => _fmtNum(v)) : { ticks: { callback: v => _fmtNum(v) } },
          y: T ? T.gridX() : { grid: { display: false } }
        }
      }
    });
  }

  // ── Matrices (3 vistas del mismo detalle) ─────────────────────────────────────
  // Las tres tablas se construyen a partir del MISMO _detail (endpoint /detalle):
  //   1) Sell Out por Producto             → agregado por marca (colapsa clientes)
  //   2) Sell Out por Marca Comercial y Cliente → fila marca + cliente (existente)
  //   3) Sell Out por Cliente y Marca Comercial → fila cliente + marca
  function _renderMatrices() {
    _renderYearControls();
    const ctx = _visibleMonths();

    // 1) Sell Out por Producto
    _renderMatrixTable({
      headYearsId: 'lab-prod-head-years',
      headMonthsId: 'lab-prod-head-months',
      tbodyId: 'lab-prod-tbody',
      countId: 'lab-prod-count',
      fixedLabel: 'Producto',
      rows: _buildProductDetail(),
      fixedCell: (row) => escHtml(row.marca || '—'),
    }, ctx);

    // 2) Sell Out por Marca Comercial y Cliente (comportamiento existente)
    _renderMatrixTable({
      headYearsId: 'lab-head-years',
      headMonthsId: 'lab-head-months',
      tbodyId: 'lab-tbody',
      countId: 'lab-table-count',
      fixedLabel: 'Marca / Cliente',
      rows: _detail,
      fixedCell: (row) =>
        `${escHtml(row.marca || '—')}<br><small class="text-muted">${escHtml(row.cliente || '—')}</small>`,
    }, ctx);
    setText('lab-footer-count', `${_detail.length} combinaciones`);
    if ($('lab-table-footer')) $('lab-table-footer').style.display = _detail.length ? '' : 'none';

    // 3) Sell Out por Cliente y Marca Comercial
    _renderMatrixTable({
      headYearsId: 'lab-cli-head-years',
      headMonthsId: 'lab-cli-head-months',
      tbodyId: 'lab-cli-tbody',
      countId: 'lab-cli-count',
      fixedLabel: 'Cliente / Marca',
      rows: _sortRows(_detail, 'cliente', 'marca'),
      fixedCell: (row) =>
        `${escHtml(row.cliente || '—')}<br><small class="text-muted">${escHtml(row.marca || '—')}</small>`,
    }, ctx);
  }

  function _visibleMonths() {
    const yearsArr = [..._visibleYears].sort();
    const visMonths = _months.filter(m => yearsArr.includes(m.slice(0, 4)));
    const yearGroups = {};
    visMonths.forEach(m => { const y = m.slice(0, 4); yearGroups[y] = (yearGroups[y] || 0) + 1; });
    return { visMonths, yearsArr, yearGroups };
  }

  function _renderYearControls() {
    const yc = $('lab-year-controls');
    if (!yc) return;
    yc.innerHTML = [...new Set(_months.map(m => m.slice(0, 4)))].sort().map(y =>
      `<button class="ind-year-btn ${_visibleYears.has(y) ? 'active' : ''}" data-year="${y}">${y}</button>`
    ).join('');
  }

  function _renderMatrixTable(cfg, ctx) {
    const headYears = $(cfg.headYearsId);
    const headMonths = $(cfg.headMonthsId);
    const tbody = $(cfg.tbodyId);
    if (!headYears || !headMonths || !tbody) return;

    headYears.innerHTML = `<th rowspan="2" class="lab-col-fixed">${escHtml(cfg.fixedLabel)}</th>` +
      ctx.yearsArr.filter(y => ctx.yearGroups[y]).map(y =>
        `<th colspan="${ctx.yearGroups[y]}" class="text-center lab-col-year">${y}</th>`
      ).join('') +
      '<th rowspan="2" class="text-end lab-col-total">Total</th>';

    headMonths.innerHTML = ctx.visMonths.map(m =>
      `<th class="text-end lab-col-month">${_fmtMonth(m)}</th>`
    ).join('');

    if (cfg.countId) setText(cfg.countId, `${cfg.rows.length} combinaciones`);

    if (!cfg.rows.length) {
      tbody.innerHTML = '<tr><td colspan="50" class="text-center text-muted py-4">Sin datos para los filtros seleccionados</td></tr>';
      return;
    }

    tbody.innerHTML = cfg.rows.slice(0, 1500).map(row => {
      const cells = ctx.visMonths.map(m => {
        const v = row.mensual?.[m];
        return `<td class="text-end">${(v == null || isNaN(v) || v === 0) ? '' : _fmtNum(v)}</td>`;
      }).join('');
      return `<tr>
        <td class="lab-col-fixed">${cfg.fixedCell(row)}</td>
        ${cells}
        <td class="text-end fw-semibold">${_fmtNum(row.unidades)}</td>
      </tr>`;
    }).join('');
  }

  // Agrega el detalle por marca (producto), sumando unidades y meses entre clientes.
  function _buildProductDetail() {
    const grouped = new Map();
    _detail.forEach(row => {
      const key = row.marca || 'SIN PRODUCTO';
      if (!grouped.has(key)) grouped.set(key, { marca: key, unidades: 0, mensual: {} });
      const item = grouped.get(key);
      item.unidades += Number(row.unidades || 0);
      Object.entries(row.mensual || {}).forEach(([mes, value]) => {
        item.mensual[mes] = (item.mensual[mes] || 0) + Number(value || 0);
      });
    });
    return [...grouped.values()].sort((a, b) => b.unidades - a.unidades);
  }

  function _sortRows(rows, primary, secondary) {
    return [...rows].sort((a, b) => {
      const pc = String(a[primary] || '').localeCompare(String(b[primary] || ''), 'es');
      if (pc) return pc;
      const sc = String(a[secondary] || '').localeCompare(String(b[secondary] || ''), 'es');
      if (sc) return sc;
      return Number(b.unidades || 0) - Number(a.unidades || 0);
    });
  }

  function _toggleYear(year) {
    if (_visibleYears.has(year)) {
      if (_visibleYears.size > 1) _visibleYears.delete(year);
    } else {
      _visibleYears.add(year);
    }
    _renderMatrices();
  }

  // ── Sort detalle ─────────────────────────────────────────────────────────────
  let _sortState = { col: 'unidades', dir: 'desc' };
  function _sortDetail(col) {
    if (_sortState.col === col) {
      _sortState.dir = _sortState.dir === 'asc' ? 'desc' : 'asc';
    } else {
      _sortState = { col, dir: 'desc' };
    }
    const multiplier = _sortState.dir === 'asc' ? 1 : -1;
    _detail = [..._detail].sort((a, b) => {
      const av = a[col] ?? '';
      const bv = b[col] ?? '';
      if (typeof av === 'number') return (av - bv) * multiplier;
      return String(av).localeCompare(String(bv)) * multiplier;
    });
    _renderMatrices();
  }

  // ── Period info ──────────────────────────────────────────────────────────────
  function _renderPeriodInfo() {
    const pi = $('lab-period-info');
    if (!pi) return;
    pi.style.display = 'flex';
    setText('lab-period-text', `${$('lab-desde')?.value} — ${$('lab-hasta')?.value}`);
    setText('lab-results-count', `${_detail.length} combinaciones`);
  }

  // ── Export CSV ───────────────────────────────────────────────────────────────
  function exportCSV() {
    if (!_detail.length) return;
    const rows = [['Laboratorio','Familia','Marca','Cliente','Unidades','Meses']];
    _detail.forEach(r => {
      rows.push([r.laboratorio||'', r.familia||'', r.marca||'', r.cliente||'',
        Math.round(r.unidades||0), r.meses||0]);
    });
    const csv = rows.map(r => r.map(c => `"${String(c).replace(/"/g,'""')}"`).join(',')).join('\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const a = document.createElement('a');
    a.href = URL.createObjectURL(blob);
    a.download = `laboratorios_${new Date().toISOString().slice(0,10)}.csv`;
    a.click();
  }

  // ── UI helpers ───────────────────────────────────────────────────────────────
  function _setLoading(on) {
    const el = $('lab-loading');
    if (el) el.style.display = on ? 'flex' : 'none';
  }

  function _showError(msg) {
    const el = $('lab-error');
    if (!el) return;
    el.style.display = 'flex';
    setText('lab-error-msg', msg);
  }

  function _hideError() {
    const el = $('lab-error');
    if (el) el.style.display = 'none';
  }

  function _setExportEnabled(enabled) {
    const btn = $('lab-btn-export');
    if (btn) btn.disabled = !enabled;
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
    if (!_summary) {
      alert('Consultá los datos primero antes de imprimir.');
      return;
    }

    const btn = $('lab-btn-print-report');
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="ind-spinner" style="width:13px;height:13px;border-width:2px;display:inline-block;margin-right:6px"></span>Preparando…';
    }

    const desde = $('lab-desde')?.value || '';
    const hasta = $('lab-hasta')?.value || '';
    const lab   = $('lab-laboratorio')?.value;
    const fam   = $('lab-familia')?.value;
    const cli   = $('lab-cliente')?.value?.trim();
    const neg   = $('lab-negocio')?.value;
    const srch  = $('lab-search')?.value?.trim();

    const filtrosActivos = [
      lab  && `Laboratorio: ${lab}`,
      fam  && `Familia: ${fam}`,
      cli  && `Cliente: ${cli}`,
      neg  && `Negocio: ${neg}`,
      srch && `Búsqueda: ${srch}`,
    ].filter(Boolean).join('  ·  ');

    const _fmtDMY = (s) => {
      if (!s) return '';
      const [y, m, d] = s.split('-');
      return `${d}/${m}/${y}`;
    };

    const _setTxt = (id, v) => { const el = $(id); if (el) el.textContent = v; };
    _setTxt('lab-ph-rango',   `Período: ${_fmtDMY(desde)} — ${_fmtDMY(hasta)}`);
    _setTxt('lab-ph-filtros', filtrosActivos ? `Filtros: ${filtrosActivos}` : 'Sin filtros adicionales');
    _setTxt('lab-ph-emitido', `Emitido: ${new Date().toLocaleString('es-AR')}`);

    const ph = $('lab-print-header');
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
