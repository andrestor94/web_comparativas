/**
 * Indicadores Comerciales — Rentabilidad Negativa
 * Vista única continua: KPIs → gráficos → labs → clientes → detalle (lazy).
 */

const IND = (() => {
  'use strict';

  // ── Estado ─────────────────────────────────────────────────────────────────
  let _cfg = {};
  let _summary = null;
  let _detail = [];
  let _detailLoaded = false;
  let _charts = {};
  let _sortState = { col: 'utilidad', dir: 'asc' };
  let _loading = false;

  // ── Formatters ──────────────────────────────────────────────────────────────
  const _fmtARS = (v) => {
    if (v == null || isNaN(v)) return '—';
    const abs = Math.abs(v);
    const fmt = abs >= 1_000_000
      ? (v / 1_000_000).toLocaleString('es-AR', { minimumFractionDigits: 1, maximumFractionDigits: 1 }) + ' M'
      : Math.round(v).toLocaleString('es-AR');
    return '$ ' + fmt;
  };

  const _fmtARSFull = (v) => {
    if (v == null || isNaN(v)) return '—';
    return '$ ' + Math.round(v).toLocaleString('es-AR');
  };

  const _fmtPct = (v) => {
    if (v == null || isNaN(v)) return '—';
    return (v * 100).toFixed(1) + ' %';
  };

  const _fmtDate = (s) => {
    if (!s) return '—';
    if (s.length === 7) {
      const [y, m] = s.split('-');
      const meses = ['ene','feb','mar','abr','may','jun','jul','ago','sep','oct','nov','dic'];
      return `${meses[parseInt(m) - 1]}-${y.slice(2)}`;
    }
    const [y, m, d] = s.split('-');
    return `${d}/${m}/${y.slice(2)}`;
  };

  const _fmtNum = (v) => {
    if (v == null || isNaN(v)) return '—';
    return parseFloat(v).toLocaleString('es-AR', { maximumFractionDigits: 0 });
  };

  // ── Helpers DOM ─────────────────────────────────────────────────────────────
  const $ = (id) => document.getElementById(id);
  const setText = (id, v) => { const el = $(id); if (el) el.textContent = v; };

  // ── Init ────────────────────────────────────────────────────────────────────
  function init(cfg) {
    _cfg = cfg;

    // Sort en tabla detalle
    document.querySelectorAll('#table-detalle th[data-sort]').forEach(th => {
      th.style.cursor = 'pointer';
      th.addEventListener('click', () => _sortDetail(th.dataset.sort));
    });

    _checkHealth();
  }

  // ── Health check SQL ────────────────────────────────────────────────────────
  async function _checkHealth() {
    const banner = $('sql-status-banner');
    const icon   = $('sql-status-icon');
    const text   = $('sql-status-text');
    if (!banner) return;

    banner.style.display = 'flex';
    banner.className = 'ind-sql-banner ind-sql-banner--checking';
    if (icon) icon.className = 'bi bi-circle-fill';
    if (text) text.textContent = 'Verificando conexión SQL Server…';

    try {
      const r = await fetch(`${_cfg.apiBase}/health`);
      const d = await r.json();
      if (d.status === 'ok' && d.etl && d.fusion) {
        banner.className = 'ind-sql-banner ind-sql-banner--ok';
        if (icon) icon.className = 'bi bi-check-circle-fill';
        if (text) text.textContent = 'SQL Server disponible — datos listos para consultar';
        setTimeout(() => { banner.style.display = 'none'; }, 4000);
      } else {
        banner.className = 'ind-sql-banner ind-sql-banner--error';
        if (icon) icon.className = 'bi bi-exclamation-triangle-fill';
        const missing = [!d.etl && 'ETL_Data', !d.fusion && 'Fusion'].filter(Boolean).join(', ');
        if (text) text.textContent = `SQL Server no disponible: ${missing}. Verificá la conexión o VPN.`;
      }
    } catch {
      banner.className = 'ind-sql-banner ind-sql-banner--error';
      if (icon) icon.className = 'bi bi-x-circle-fill';
      if (text) text.textContent = 'No se pudo verificar SQL Server. Revisá la red o VPN.';
    }
  }

  // ── Parámetros de filtros ───────────────────────────────────────────────────
  function _buildParams() {
    const desde = $('f-desde')?.value || _cfg.desde;
    const hasta = $('f-hasta')?.value || _cfg.hasta;
    const hastaDate = new Date(hasta + 'T00:00:00');
    hastaDate.setDate(hastaDate.getDate() + 1);
    const hastaExcl = hastaDate.toISOString().slice(0, 10);

    const p = new URLSearchParams();
    p.set('desde', desde);
    p.set('hasta', hastaExcl);

    const lab  = $('f-laboratorio')?.value;
    const fam  = $('f-familia')?.value;
    const cli  = $('f-cliente')?.value?.trim();
    const neg  = $('f-negocio')?.value;
    const srch = $('f-search')?.value?.trim();
    const modo = $('f-modo')?.value || 'detalle';

    if (lab)  p.set('laboratorio', lab);
    if (fam)  p.set('familia', fam);
    if (cli)  p.set('cliente', cli);
    if (neg)  p.set('cadneg', neg);
    if (srch) p.set('search', srch);
    p.set('modo', modo);

    return p.toString();
  }

  // ── Fetch con timeout ───────────────────────────────────────────────────────
  async function _fetchWithTimeout(url, timeoutMs = 70000) {
    const controller = new AbortController();
    const timer = setTimeout(() => controller.abort(), timeoutMs);
    try {
      const res = await fetch(url, { signal: controller.signal });
      clearTimeout(timer);
      return res;
    } catch (err) {
      clearTimeout(timer);
      if (err.name === 'AbortError') {
        throw new Error(
          'La consulta tardó demasiado. ' +
          'Verificá la conexión de red, VPN o disponibilidad de SQL Server y volvé a intentar.'
        );
      }
      throw err;
    }
  }

  // ── Aplicar filtros ─────────────────────────────────────────────────────────
  async function applyFilters() {
    if (_loading) return;
    _loading = true;
    _detailLoaded = false;
    _detail = [];
    _showLoading(true);
    _hideError();
    _hideEmptyState();
    _resetDetailSection();

    try {
      const params = _buildParams();

      // Resumen y metadata en paralelo — comparten cache del servidor (90 s)
      const [resRes, metaRes] = await Promise.all([
        _fetchWithTimeout(`${_cfg.apiBase}/resumen?${params}`),
        _fetchWithTimeout(`${_cfg.apiBase}/metadata?${params}`, 90000),
      ]);

      if (!resRes.ok) {
        const body = await resRes.text();
        throw new Error(_extractUserMessage(body, resRes.status));
      }

      _summary = await resRes.json();
      _renderAll();
      _updatePeriodInfo();
      _showDetailLoadButton();

      // Poblar filtros desde metadata completa (no limitada a 12)
      if (metaRes.ok) {
        const meta = await metaRes.json();
        _populateFiltersFromMeta(meta);
      } else {
        _populateFiltersFromSummary(); // fallback: usa los 12 del resumen
      }

    } catch (err) {
      _showError(err.message || 'Error al consultar el servidor.');
    } finally {
      _loading = false;
      _showLoading(false);
    }
  }

  // ── Cargar detalle (lazy, por botón) ────────────────────────────────────────
  async function loadDetail() {
    if (_detailLoaded) return;

    const btn = $('btn-load-detail');
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="ind-spinner" style="width:13px;height:13px;border-width:2px;display:inline-block;margin-right:6px"></span>Cargando…';
    }

    const placeholder = $('detalle-placeholder');
    if (placeholder) placeholder.style.display = 'none';

    const wrapper = $('detalle-table-wrapper');
    if (wrapper) {
      wrapper.style.display = '';
      const tbody = $('tbody-detalle');
      if (tbody) {
        tbody.innerHTML = `<tr><td colspan="9" class="text-center py-4">
          <span class="ind-spinner" style="width:20px;height:20px;border-width:2px;display:inline-block"></span>
          <span class="text-muted ms-2 small">Cargando transacciones…</span>
        </td></tr>`;
      }
    }

    try {
      const params = _buildParams();
      const res = await _fetchWithTimeout(`${_cfg.apiBase}/detalle?${params}`);
      if (!res.ok) {
        const body = await res.text();
        throw new Error(_extractUserMessage(body, res.status));
      }
      _detail = await res.json();
      _detailLoaded = true;
      _renderDetail();
      if (btn) btn.style.display = 'none';
      const csvBtn = $('btn-export-csv');
      const printBtn = $('btn-print');
      if (csvBtn) csvBtn.style.display = '';
      if (printBtn) printBtn.style.display = '';
    } catch (err) {
      const tbody = $('tbody-detalle');
      if (tbody) {
        tbody.innerHTML = `<tr><td colspan="9" class="text-center py-4 text-danger small">
          <i class="bi bi-exclamation-triangle me-1"></i>${_escHtml(err.message)}
          <br><button class="btn btn-sm btn-outline-secondary mt-2" onclick="IND._loadDetailRetry()">
            <i class="bi bi-arrow-clockwise me-1"></i>Reintentar
          </button>
        </td></tr>`;
      }
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-download me-1"></i>Cargar detalle';
      }
    }
  }

  function _loadDetailRetry() {
    _detailLoaded = false;
    loadDetail();
  }

  // ── Limpiar filtros ─────────────────────────────────────────────────────────
  function clearFilters() {
    const today     = new Date().toISOString().slice(0, 10);
    const yearStart = new Date(new Date().getFullYear(), 0, 1).toISOString().slice(0, 10);

    [
      ['f-desde', yearStart], ['f-hasta', today],
      ['f-laboratorio', ''], ['f-familia', ''],
      ['f-cliente', ''], ['f-negocio', ''],
      ['f-search', ''], ['f-modo', 'detalle'],
    ].forEach(([id, val]) => { const el = $(id); if (el) el.value = val; });

    _summary = null;
    _detail = [];
    _detailLoaded = false;
    _hideError();
    _hideEmptyState();
    _resetDetailSection();
    _updatePeriodInfo();
  }

  // ── Mensaje amigable de error ───────────────────────────────────────────────
  function _extractUserMessage(body, status) {
    if (status === 500 || status === 503) {
      return 'No se pudo conectar con la fuente de datos de Indicadores Comerciales. ' +
             'Verificá la conexión de red, VPN o disponibilidad de SQL Server y volvé a intentar.';
    }
    if (status === 401) return 'Tu sesión expiró. Recargá la página e ingresá nuevamente.';
    if (status === 403) return 'No tenés permisos para ver este contenido.';
    return `Error ${status} al consultar el servidor.`;
  }

  // ── Render principal ────────────────────────────────────────────────────────
  function _renderAll() {
    if (!_summary) return;

    if (_summary.total_registros === 0) {
      _showEmptyState();
      return;
    }

    _hideEmptyState();
    _renderKPIs();
    _renderNegocios();
    _renderChartEvolucion();
    _renderChartLabs();
    _renderTableLabs();
    _renderChartLabs2();      // siempre visible en vista única
    _renderTableClientes();
    _renderChartClientes();   // siempre visible en vista única

    const modoEl = $('badge-modo');
    if (modoEl) {
      modoEl.textContent = ($('f-modo')?.value === 'agrupado') ? 'Agrupado' : 'Detalle';
    }
  }

  // ── KPIs ────────────────────────────────────────────────────────────────────
  function _renderKPIs() {
    const s = _summary;
    setText('kv-perdida',       _fmtARS(s.utilidad_total));
    setText('kv-transacciones', _fmtNum(s.total_transacciones));
    setText('kv-facturacion',   _fmtARS(s.facturacion_total));
    setText('kv-renta-prom',    _fmtPct(s.rentabilidad_promedio));
    setText('kv-labs',          _fmtNum(s.cantidad_laboratorios));
    setText('kv-marcas',        _fmtNum(s.cantidad_marcas));
    setText('kv-clientes',      _fmtNum(s.cantidad_clientes));

    if (s.clientes?.length > 0) {
      const top = s.clientes[0];
      setText('kv-mayor-perdida', top.name);
      setText('ks-mayor-perdida-monto', _fmtARSFull(top.value));
    }

    const varEl = $('ks-variacion');
    if (varEl && s.variacion_mensual != null) {
      const pct = (s.variacion_mensual * 100).toFixed(1);
      const up  = s.variacion_mensual > 0;
      varEl.textContent = `${up ? '▲' : '▼'} ${Math.abs(pct)}% vs. mes anterior`;
      varEl.style.color = up ? '#d93025' : '#166534';
    } else if (varEl) {
      varEl.textContent = 'vs. mes anterior';
      varEl.style.color = '';
    }
  }

  // ── Negocios chips ──────────────────────────────────────────────────────────
  function _renderNegocios() {
    const container = $('negocios-chips');
    if (!container || !_summary?.negocios) return;
    container.innerHTML = '';
    _summary.negocios.forEach(n => {
      const chip = document.createElement('div');
      chip.className = 'ind-negocio-chip';
      chip.innerHTML = `
        <div>
          <div class="ind-negocio-chip__label">${_escHtml(n.name)}</div>
          <div class="ind-negocio-chip__value">${_fmtARSFull(n.value)}</div>
        </div>`;
      container.appendChild(chip);
    });
    if (!_summary.negocios.length) {
      container.innerHTML = '<p class="text-muted small">Sin datos de negocios para el período.</p>';
    }
  }

  // ── Gráfico evolución mensual ───────────────────────────────────────────────
  function _renderChartEvolucion() {
    const canvas  = $('chart-evolucion');
    const emptyEl = $('chart-evolucion-empty');
    if (!canvas) return;

    const meses = (_summary?.meses || []).sort((a, b) => a.mes.localeCompare(b.mes));
    if (!meses.length) {
      canvas.style.display = 'none';
      if (emptyEl) emptyEl.style.display = 'flex';
      return;
    }
    canvas.style.display = '';
    if (emptyEl) emptyEl.style.display = 'none';
    if (_charts.evolucion) _charts.evolucion.destroy();

    _charts.evolucion = new Chart(canvas, {
      type: 'line',
      data: {
        labels: meses.map(m => _fmtDate(m.mes)),
        datasets: [{
          label: 'Utilidad (ARS)',
          data: meses.map(m => m.utilidad),
          fill: true,
          borderColor: '#d93025',
          backgroundColor: 'rgba(217,48,37,0.07)',
          borderWidth: 2.5,
          pointBackgroundColor: '#d93025',
          pointRadius: 4,
          tension: 0.35,
        }],
      },
      options: {
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ' ' + _fmtARSFull(ctx.raw) } },
        },
        scales: {
          x: { grid: { display: false }, ticks: { font: { size: 11 } } },
          y: { ticks: { font: { size: 11 }, callback: v => _fmtARS(v) }, grid: { color: 'rgba(0,0,0,0.05)' } },
        },
      },
    });
  }

  // ── Gráfico top 10 labs (panel general) ────────────────────────────────────
  function _renderChartLabs() {
    const canvas  = $('chart-labs');
    const emptyEl = $('chart-labs-empty');
    if (!canvas) return;
    const labs = (_summary?.laboratorios || []).slice(0, 10);
    if (!labs.length) {
      canvas.style.display = 'none';
      if (emptyEl) emptyEl.style.display = 'flex';
      return;
    }
    canvas.style.display = '';
    if (emptyEl) emptyEl.style.display = 'none';
    if (_charts.labs) _charts.labs.destroy();
    _charts.labs = _buildHBarChart(canvas, labs, '#06486f');
  }

  // ── Gráfico top 12 labs (sección laboratorios) ─────────────────────────────
  function _renderChartLabs2() {
    const canvas = $('chart-labs-2');
    if (!canvas) return;
    const labs = (_summary?.laboratorios || []).slice(0, 12);
    if (_charts.labs2) _charts.labs2.destroy();
    _charts.labs2 = _buildHBarChart(canvas, labs, '#d93025');
  }

  // ── Gráfico clientes ────────────────────────────────────────────────────────
  function _renderChartClientes() {
    const canvas = $('chart-clientes');
    if (!canvas) return;
    const cli = (_summary?.clientes || []).slice(0, 12);
    if (_charts.clientes) _charts.clientes.destroy();
    _charts.clientes = _buildHBarChart(canvas, cli, '#5770b0');
  }

  function _buildHBarChart(canvas, items, color) {
    return new Chart(canvas, {
      type: 'bar',
      data: {
        labels: items.map(i => i.name.length > 30 ? i.name.slice(0, 29) + '…' : i.name),
        datasets: [{ data: items.map(i => i.value), backgroundColor: color + 'cc', borderColor: color, borderWidth: 1, borderRadius: 4 }],
      },
      options: {
        indexAxis: 'y',
        responsive: true,
        maintainAspectRatio: false,
        plugins: {
          legend: { display: false },
          tooltip: { callbacks: { label: ctx => ' ' + _fmtARSFull(ctx.raw) } },
        },
        scales: {
          x: { ticks: { font: { size: 10 }, callback: v => _fmtARS(v) }, grid: { color: 'rgba(0,0,0,0.04)' } },
          y: { ticks: { font: { size: 11 } }, grid: { display: false } },
        },
      },
    });
  }

  // ── Tabla laboratorios ──────────────────────────────────────────────────────
  function _renderTableLabs() {
    const tbody = $('tbody-labs');
    if (!tbody) return;
    const labs = _summary?.laboratorios || [];
    if (!labs.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">Sin datos</td></tr>';
      return;
    }
    const total  = Math.abs(_summary.utilidad_total) || 1;
    const maxVal = Math.abs(labs[0]?.value || 1);
    tbody.innerHTML = labs.map((l, i) => {
      const pct  = ((Math.abs(l.value) / total) * 100).toFixed(1);
      const barW = Math.round((Math.abs(l.value) / maxVal) * 100);
      return `<tr>
        <td class="text-muted small">${i + 1}</td>
        <td><strong>${_escHtml(l.name)}</strong></td>
        <td class="text-end ind-util-neg">${_fmtARSFull(l.value)}</td>
        <td class="text-end text-muted small">${pct} %</td>
        <td class="ind-rank-bar-cell"><div class="ind-rank-bar" style="width:${barW}%"></div></td>
      </tr>`;
    }).join('');
  }

  // ── Tabla clientes ──────────────────────────────────────────────────────────
  function _renderTableClientes() {
    const tbody = $('tbody-clientes');
    if (!tbody) return;
    const clientes = _summary?.clientes || [];
    if (!clientes.length) {
      tbody.innerHTML = '<tr><td colspan="5" class="text-center text-muted py-4">Sin datos</td></tr>';
      return;
    }
    const total  = Math.abs(_summary.utilidad_total) || 1;
    const maxVal = Math.abs(clientes[0]?.value || 1);
    tbody.innerHTML = clientes.map((c, i) => {
      const pct  = ((Math.abs(c.value) / total) * 100).toFixed(1);
      const barW = Math.round((Math.abs(c.value) / maxVal) * 100);
      return `<tr>
        <td class="text-muted small">${i + 1}</td>
        <td><strong>${_escHtml(c.name)}</strong></td>
        <td class="text-end ind-util-neg">${_fmtARSFull(c.value)}</td>
        <td class="text-end text-muted small">${pct} %</td>
        <td class="ind-rank-bar-cell"><div class="ind-rank-bar" style="background:linear-gradient(90deg,#5770b0,rgba(87,112,176,0.3));width:${barW}%"></div></td>
      </tr>`;
    }).join('');
  }

  // ── Tabla detalle ───────────────────────────────────────────────────────────
  function _renderDetail() {
    const tbody  = $('tbody-detalle');
    const footer = $('detalle-footer');
    const countEl = $('detalle-count');
    if (!tbody) return;

    const rows = _sortedDetail();
    if (!rows.length) {
      tbody.innerHTML = `<tr><td colspan="9" class="text-center text-muted py-5">
        <i class="bi bi-search fs-3 d-block mb-2 opacity-25"></i>Sin resultados
      </td></tr>`;
      if (footer) footer.style.display = 'none';
      return;
    }

    tbody.innerHTML = rows.map(r => {
      const isMes        = r.modo === 'agrupado';
      const fechaDisplay = isMes ? _fmtDate(r.mes) : _fmtDate(r.fecha);
      return `<tr>
        <td class="text-muted small" style="white-space:nowrap">${fechaDisplay}</td>
        <td style="max-width:180px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_escHtml(r.cliente)}">${_escHtml(r.cliente)}</td>
        <td style="max-width:160px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_escHtml(r.marca)}">${_escHtml(r.marca)}</td>
        <td style="max-width:140px;overflow:hidden;text-overflow:ellipsis;white-space:nowrap" title="${_escHtml(r.laboratorio)}">${_escHtml(r.laboratorio)}</td>
        <td><span class="badge bg-light text-secondary" style="font-size:0.68rem">${_escHtml(r.negocio)}</span></td>
        <td class="text-end small">${_fmtNum(r.unidades)}</td>
        <td class="text-end small">${_fmtARSFull(r.facturacion)}</td>
        <td class="text-end ind-util-neg">${_fmtARSFull(r.utilidad)}</td>
        <td class="text-end ind-rent-neg">${_fmtPct(r.rentabilidad)}</td>
      </tr>`;
    }).join('');

    if (footer) footer.style.display = '';
    if (countEl) {
      const note = _detail.length >= 1200 ? ' (máx. 1.200 filas)' : '';
      countEl.textContent = `${rows.length} registros${note}`;
    }
  }

  // ── Sort tabla detalle ──────────────────────────────────────────────────────
  function _sortDetail(col) {
    if (_sortState.col === col) {
      _sortState.dir = _sortState.dir === 'asc' ? 'desc' : 'asc';
    } else {
      _sortState.col = col;
      _sortState.dir = col === 'fecha' ? 'desc' : 'asc';
    }
    document.querySelectorAll('#table-detalle th[data-sort] .sort-icon').forEach(s => s.textContent = '');
    const th = document.querySelector(`#table-detalle th[data-sort="${col}"] .sort-icon`);
    if (th) th.textContent = _sortState.dir === 'asc' ? ' ▲' : ' ▼';
    _renderDetail();
  }

  function _sortedDetail() {
    if (!_detail?.length) return [];
    const col = _sortState.col;
    const dir = _sortState.dir === 'asc' ? 1 : -1;
    return [..._detail].sort((a, b) => {
      const av = a[col] ?? '', bv = b[col] ?? '';
      if (typeof av === 'number' && typeof bv === 'number') return (av - bv) * dir;
      return String(av).localeCompare(String(bv)) * dir;
    });
  }

  // ── Dropdowns desde metadata completa ──────────────────────────────────────
  // get_metadata() devuelve el universo completo sin límite de 12.
  // Los mismos parámetros de filtro comparten la cache del servidor con /resumen.
  function _populateFiltersFromMeta(meta) {
    _populateSelect('f-laboratorio', meta.laboratorios || [], 'Todos los laboratorios');
    _populateSelect('f-familia',     meta.familias     || [], 'Todas las familias');
    _populateDatalist('dl-clientes', meta.clientes     || []);
  }

  // ── Fallback: dropdowns desde resumen (solo top 12, sin familias) ───────────
  function _populateFiltersFromSummary() {
    if (!_summary) return;
    const labs = (_summary.laboratorios || []).map(l => l.name).sort();
    const clis = (_summary.clientes     || []).map(c => c.name).sort();
    _populateSelect('f-laboratorio', labs, 'Todos los laboratorios');
    _populateDatalist('dl-clientes', clis);
    // Nota: familias no está en el summary; se necesita el endpoint /metadata
  }

  function _populateSelect(id, items, placeholder) {
    const el = $(id);
    if (!el) return;
    const current = el.value;
    el.innerHTML = `<option value="">${placeholder}</option>`;
    items.forEach(item => {
      const opt = document.createElement('option');
      opt.value = item;
      opt.textContent = item;
      el.appendChild(opt);
    });
    if (items.includes(current)) el.value = current;
  }

  function _populateDatalist(id, items) {
    const dl = $(id);
    if (!dl) return;
    dl.innerHTML = '';
    items.slice(0, 500).forEach(item => {
      const opt = document.createElement('option');
      opt.value = item;
      dl.appendChild(opt);
    });
  }

  // ── Sección detalle: estados ────────────────────────────────────────────────
  function _showDetailLoadButton() {
    const btn = $('btn-load-detail');
    const placeholder = $('detalle-placeholder');
    if (btn) {
      btn.style.display = '';
      btn.disabled = false;
      btn.innerHTML = '<i class="bi bi-download me-1"></i>Cargar detalle';
    }
    if (placeholder) {
      placeholder.style.display = '';
      placeholder.innerHTML = `
        <i class="bi bi-table fs-3 d-block mb-2 opacity-25"></i>
        <p class="mb-2">Hacé clic en <strong>Cargar detalle</strong> para ver las transacciones individuales.</p>
        <p class="text-muted small mb-0">Hasta 1.200 filas · Los filtros aplicados se usan para esta consulta también.</p>`;
    }
    const wrapper = $('detalle-table-wrapper');
    if (wrapper) wrapper.style.display = 'none';
    const footer = $('detalle-footer');
    if (footer) footer.style.display = 'none';
    const csvBtn = $('btn-export-csv');
    const printBtn = $('btn-print');
    if (csvBtn) csvBtn.style.display = 'none';
    if (printBtn) printBtn.style.display = 'none';
  }

  function _resetDetailSection() {
    const btn = $('btn-load-detail');
    if (btn) btn.style.display = 'none';
    const placeholder = $('detalle-placeholder');
    if (placeholder) {
      placeholder.style.display = '';
      placeholder.innerHTML = `
        <i class="bi bi-funnel fs-3 d-block mb-2 opacity-25"></i>
        Aplicá filtros y hacé clic en <strong>Aplicar</strong> para habilitar el detalle de transacciones.`;
    }
    const wrapper = $('detalle-table-wrapper');
    if (wrapper) wrapper.style.display = 'none';
    const footer = $('detalle-footer');
    if (footer) footer.style.display = 'none';
    const csvBtn = $('btn-export-csv');
    const printBtn = $('btn-print');
    if (csvBtn) csvBtn.style.display = 'none';
    if (printBtn) printBtn.style.display = 'none';
  }

  // ── Info período ────────────────────────────────────────────────────────────
  function _updatePeriodInfo() {
    const infoEl  = $('period-info');
    const textEl  = $('period-text');
    const countEl = $('results-count');
    if (!infoEl || !textEl) return;

    const desde = $('f-desde')?.value || _cfg.desde;
    const hasta = $('f-hasta')?.value || _cfg.hasta;
    const [dy, dm, dd] = desde.split('-');
    const [hy, hm, hd] = hasta.split('-');
    textEl.textContent = `${dd}/${dm}/${dy} — ${hd}/${hm}/${hy}`;
    if (countEl && _summary) {
      countEl.textContent = `· ${_fmtNum(_summary.total_registros)} registros`;
    } else if (countEl) {
      countEl.textContent = '';
    }
    infoEl.style.display = _summary ? '' : 'none';
  }

  // ── Export CSV ──────────────────────────────────────────────────────────────
  function exportCSV() {
    if (!_detail?.length) {
      alert('Cargá el detalle primero usando el botón "Cargar detalle".');
      return;
    }
    const headers = ['Fecha','Cliente','Marca Comercial','Laboratorio','Principio Activo','Negocio','Unidades','Facturación','Utilidad','Rentabilidad'];
    const rows = _detail.map(r => [
      r.modo === 'agrupado' ? r.mes : r.fecha,
      r.cliente, r.marca, r.laboratorio,
      r.principio_activo || '',
      r.negocio, r.unidades, r.facturacion, r.utilidad,
      r.rentabilidad != null ? (r.rentabilidad * 100).toFixed(2) + '%' : '',
    ]);
    const csv = [headers, ...rows]
      .map(row => row.map(v => `"${String(v).replace(/"/g, '""')}"`).join(';'))
      .join('\r\n');
    const blob = new Blob(['﻿' + csv], { type: 'text/csv;charset=utf-8;' });
    const url  = URL.createObjectURL(blob);
    const a    = document.createElement('a');
    const desde = $('f-desde')?.value || _cfg.desde;
    const hasta  = $('f-hasta')?.value  || _cfg.hasta;
    const modo   = $('f-modo')?.value   || 'detalle';
    a.href = url;
    a.download = `indicadores_${modo}_${desde}_${hasta}.csv`;
    a.click();
    URL.revokeObjectURL(url);
  }

  // ── UI helpers ──────────────────────────────────────────────────────────────
  function _showLoading(show) {
    const el  = $('global-loading');
    if (el) el.style.display = show ? 'flex' : 'none';
    const btn = $('btn-apply');
    if (btn) {
      btn.disabled = show;
      btn.innerHTML = show
        ? '<span class="ind-spinner" style="width:14px;height:14px;border-width:2px;margin-right:6px;display:inline-block"></span>Consultando…'
        : '<i class="bi bi-funnel-fill"></i> Aplicar';
    }
  }

  function _showError(msg) {
    const el    = $('global-error');
    const msgEl = $('global-error-msg');
    if (!el) return;
    if (msgEl) msgEl.textContent = msg;
    el.style.display = 'flex';
  }

  function _hideError() {
    const el = $('global-error');
    if (el) el.style.display = 'none';
  }

  function _showEmptyState() {
    const el = $('empty-state');
    if (el) el.style.display = 'flex';
    const sections = ['section-panel','section-laboratorios','section-clientes','section-detalle'];
    sections.forEach(id => { const s = $(id); if (s) s.style.display = 'none'; });
  }

  function _hideEmptyState() {
    const el = $('empty-state');
    if (el) el.style.display = 'none';
    const sections = ['section-panel','section-laboratorios','section-clientes','section-detalle'];
    sections.forEach(id => { const s = $(id); if (s) s.style.display = ''; });
  }

  // ── Escape HTML ─────────────────────────────────────────────────────────────
  function _escHtml(s) {
    if (!s) return '';
    return String(s)
      .replace(/&/g, '&amp;').replace(/</g, '&lt;')
      .replace(/>/g, '&gt;').replace(/"/g, '&quot;');
  }

  // ── Canvas → imagen temporal (para print correcto) ─────────────────────────
  function _snapshotCanvases() {
    const snaps = [];
    document.querySelectorAll('canvas').forEach(canvas => {
      try {
        if (!canvas.width || !canvas.height) return;
        const img = document.createElement('img');
        img.src = canvas.toDataURL('image/png');
        img.className = 'ind-print-canvas-img';
        img.style.display = 'none'; // se muestra solo via @media print
        canvas.parentNode.insertBefore(img, canvas.nextSibling);
        snaps.push({ canvas, img });
      } catch { /* canvas tainted — ignorar */ }
    });
    return snaps;
  }

  function _restoreCanvases(snaps) {
    snaps.forEach(({ img }) => img.remove());
  }

  // ── Imprimir informe completo ────────────────────────────────────────────────
  async function printReport() {
    if (!_summary) {
      alert('Aplicá los filtros primero para generar el informe antes de imprimir.');
      return;
    }

    const btn = $('btn-print-report');
    if (btn) {
      btn.disabled = true;
      btn.innerHTML = '<span class="ind-spinner" style="width:13px;height:13px;border-width:2px;display:inline-block;margin-right:6px"></span>Preparando…';
    }

    // Cargar detalle automáticamente si no está cargado
    if (!_detailLoaded) {
      try { await loadDetail(); } catch { /* imprimir igualmente */ }
    }

    // Poblar cabecera de impresión
    const desde = $('f-desde')?.value || '';
    const hasta = $('f-hasta')?.value || '';
    const lab   = $('f-laboratorio')?.value;
    const fam   = $('f-familia')?.value;
    const cli   = $('f-cliente')?.value?.trim();
    const neg   = $('f-negocio')?.value;
    const srch  = $('f-search')?.value?.trim();
    const modo  = $('f-modo')?.value || 'detalle';

    const filtrosActivos = [
      lab  && `Laboratorio: ${lab}`,
      fam  && `Familia: ${fam}`,
      cli  && `Cliente: ${cli}`,
      neg  && `Negocio: ${neg}`,
      srch && `Búsqueda: ${srch}`,
      `Modo: ${modo === 'agrupado' ? 'Agrupado' : 'Detalle'}`,
    ].filter(Boolean).join('  ·  ');

    const _fmtDMY = (s) => {
      if (!s) return '';
      const [y, m, d] = s.split('-');
      return `${d}/${m}/${y}`;
    };

    setText('ph-rango',   `Período: ${_fmtDMY(desde)} — ${_fmtDMY(hasta)}`);
    setText('ph-filtros', filtrosActivos ? `Filtros: ${filtrosActivos}` : 'Sin filtros adicionales');
    setText('ph-emitido', `Emitido: ${new Date().toLocaleString('es-AR')}`);

    // Mostrar cabecera print
    const ph = $('print-header');
    if (ph) ph.style.display = '';

    // Convertir canvas a imágenes antes de imprimir
    const snaps = _snapshotCanvases();

    // Esperar dos frames + 400ms para que el navegador recalcule layout completo
    await new Promise(r => requestAnimationFrame(r));
    await new Promise(r => requestAnimationFrame(r));
    await new Promise(r => setTimeout(r, 400));

    window.print();

    // Restaurar después de imprimir
    setTimeout(() => {
      _restoreCanvases(snaps);
      if (ph) ph.style.display = 'none';
      if (btn) {
        btn.disabled = false;
        btn.innerHTML = '<i class="bi bi-printer me-1"></i>Imprimir';
      }
    }, 600);
  }

  // ── API pública ─────────────────────────────────────────────────────────────
  return {
    init,
    applyFilters,
    clearFilters,
    exportCSV,
    loadDetail,
    printReport,
    _loadDetailRetry,
  };
})();
