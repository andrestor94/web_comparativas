/* reporte_perfiles_privado.js - Reporte Especial Mercado Privado */
"use strict";

const PV = {
  activeTab: "articulo",
  fechaDesde: "",
  fechaHasta: "",
  charts: {},
  filterOptions: { familias: [], clientes: [], plataformas: [], date_range: {} },
  // Selecciones activas
  artFamilia: null,
  artCliente: null,
  cliCliente: null,
  cliFamilia: null,
  dropdowns: {},
};

const BASE = "/api/mercado-privado/perfiles";

const COLORS = {
  brand900: "#06486f",
  brand700: "#1e5c8a",
  brand500: "#5770b0",
  cyan: "#38bdf8",
  success: "#10b981",
  warning: "#f59e0b",
  neutral: "#94a3b8",
  palette: [
    "#06486f", "#1e5c8a", "#5770b0", "#38bdf8",
    "#10b981", "#f59e0b", "#ef4444", "#8b5cf6",
    "#f97316", "#06b6d4", "#84cc16", "#ec4899",
  ],
};

const EMPTY_DASH = "\u2014";
const NUMBER_FULL = new Intl.NumberFormat("es-AR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 2,
});
const NUMBER_INT = new Intl.NumberFormat("es-AR", {
  minimumFractionDigits: 0,
  maximumFractionDigits: 0,
});

const mj = (...codes) => String.fromCharCode(...codes);

const MOJIBAKE_REPLACEMENTS = [
  [mj(0x00c3, 0x00a1), "\u00e1"],
  [mj(0x00c3, 0x00a9), "\u00e9"],
  [mj(0x00c3, 0x00ad), "\u00ed"],
  [mj(0x00c3, 0x00b3), "\u00f3"],
  [mj(0x00c3, 0x00ba), "\u00fa"],
  [mj(0x00c3, 0x00b1), "\u00f1"],
  [mj(0x00c3, 0x0081), "\u00c1"],
  [mj(0x00c3, 0x0089), "\u00c9"],
  [mj(0x00c3, 0x008d), "\u00cd"],
  [mj(0x00c3, 0x0093), "\u00d3"],
  [mj(0x00c3, 0x009a), "\u00da"],
  [mj(0x00c3, 0x0091), "\u00d1"],
  [mj(0x00c2), ""],
  [mj(0x00e2, 0x20ac, 0x0153), "\u201c"],
  [mj(0x00e2, 0x20ac, 0x009d), "\u201d"],
  [mj(0x00e2, 0x20ac, 0x2122), "\u2019"],
  [mj(0x00e2, 0x20ac, 0x02dc), "\u2018"],
  [mj(0x00e2, 0x20ac, 0x00a6), "\u2026"],
  [`Valorizaci${mj(0x00c3, 0x00b3)}n`, "Valorizaci\u00f3n"],
  [`valorizaci${mj(0x00c3, 0x00b3)}n`, "valorizaci\u00f3n"],
  [`Ubicaci${mj(0x00c3, 0x00b3)}n`, "Ubicaci\u00f3n"],
  [`ubicaci${mj(0x00c3, 0x00b3)}n`, "ubicaci\u00f3n"],
  [`Selecci${mj(0x00c3, 0x00b3)}n`, "Selecci\u00f3n"],
  [`selecci${mj(0x00c3, 0x00b3)}n`, "selecci\u00f3n"],
  [`Art${mj(0x00c3, 0x00ad)}culo`, "Art\u00edculo"],
  [`art${mj(0x00c3, 0x00ad)}culo`, "art\u00edculo"],
  [`Per${mj(0x00c3, 0x00ad)}odo`, "Per\u00edodo"],
  [`per${mj(0x00c3, 0x00ad)}odo`, "per\u00edodo"],
  [`An${mj(0x00c3, 0x00a1)}lisis`, "An\u00e1lisis"],
  [`an${mj(0x00c3, 0x00a1)}lisis`, "an\u00e1lisis"],
  [`Evoluci${mj(0x00c3, 0x00b3)}n`, "Evoluci\u00f3n"],
  [`evoluci${mj(0x00c3, 0x00b3)}n`, "evoluci\u00f3n"],
  [`num${mj(0x00c3, 0x00a9)}rico`, "num\u00e9rico"],
  [mj(0x00e2, 0x20ac, 0x201d), EMPTY_DASH],
  [mj(0x00e2, 0x20ac, 0x201c), "\u2013"],
  ["Valorizaci?n", "Valorizaci\u00f3n"],
  ["valorizaci?n", "valorizaci\u00f3n"],
  ["Ubicaci?n", "Ubicaci\u00f3n"],
  ["ubicaci?n", "ubicaci\u00f3n"],
  ["Selecci?n", "Selecci\u00f3n"],
  ["selecci?n", "selecci\u00f3n"],
  ["Art?culo", "Art\u00edculo"],
  ["art?culo", "art\u00edculo"],
  ["Per?odo", "Per\u00edodo"],
  ["per?odo", "per\u00edodo"],
  ["An?lisis", "An\u00e1lisis"],
  ["an?lisis", "an\u00e1lisis"],
  ["Evoluci?n", "Evoluci\u00f3n"],
  ["evoluci?n", "evoluci\u00f3n"],
  ["num?rico", "num\u00e9rico"],
];

function cleanMojibakeText(value) {
  if (value == null) return "";
  let text = String(value);
  MOJIBAKE_REPLACEMENTS.forEach(([bad, good]) => {
    text = text.split(bad).join(good);
  });
  return text.trim();
}

function toFiniteNumber(value) {
  if (value == null || value === "") return null;
  if (typeof value === "number") return Number.isFinite(value) ? value : null;
  const cleaned = cleanMojibakeText(value)
    .replace(/[^\d,.\-]/g, "")
    .trim();
  if (!cleaned || cleaned === "-" || cleaned === EMPTY_DASH) return null;

  let normalized = cleaned;
  const hasComma = normalized.includes(",");
  const hasDot = normalized.includes(".");
  if (hasComma && hasDot) {
    normalized = normalized.replace(/\./g, "").replace(",", ".");
  } else if (hasComma) {
    normalized = normalized.replace(",", ".");
  } else if (hasDot) {
    const parts = normalized.split(".");
    if (parts.length > 2 || parts[parts.length - 1].length === 3) {
      normalized = parts.join("");
    }
  }
  const number = Number(normalized);
  return Number.isFinite(number) ? number : null;
}

function formatNumberFull(value, decimals = 2) {
  const number = toFiniteNumber(value);
  if (number == null) return EMPTY_DASH;
  const hasDecimals = Math.abs(number % 1) > 0.000001;
  if (!hasDecimals || decimals === 0) return NUMBER_INT.format(Math.round(number));
  return NUMBER_FULL.format(Number(number.toFixed(decimals)));
}

function formatNumberShort(value, decimals = 2) {
  const number = toFiniteNumber(value);
  if (number == null) return EMPTY_DASH;
  const abs = Math.abs(number);
  const format = (n, d = decimals) => Number(n.toFixed(d)).toLocaleString("es-AR", {
    minimumFractionDigits: 0,
    maximumFractionDigits: d,
  });
  if (abs >= 1e9) return `${format(number / 1e9)} mil mill.`;
  if (abs >= 1e6) return `${format(number / 1e6)} mill.`;
  if (abs >= 1e3) return `${format(number / 1e3, 0)} mil`;
  return format(number, abs < 10 && abs % 1 ? 2 : 0);
}

function formatMoneyFull(value, decimals = 2) {
  const formatted = formatNumberFull(value, decimals);
  return formatted === EMPTY_DASH ? EMPTY_DASH : `$ ${formatted}`;
}

function formatMoneyShort(value, decimals = 2) {
  const formatted = formatNumberShort(value, decimals);
  return formatted === EMPTY_DASH ? EMPTY_DASH : `$ ${formatted}`;
}

function formatPrice(value) {
  return formatMoneyFull(value);
}

function formatCellValue(value) {
  const number = toFiniteNumber(value);
  if (number == null) return EMPTY_DASH;
  return formatNumberFull(number, 0);
}

// Formateo numerico
function pvFmt(n, decimals = 2) {
  return formatNumberShort(n, decimals);
}

function pvFmtMoney(n, decimals = 2) {
  return formatMoneyShort(n, decimals);
}

function pvFmtInt(n) {
  return formatNumberFull(n, 0);
}

function pvFmtLabel(n) {
  return formatNumberShort(n);
}

function pvFmtMoneyLabel(n) {
  return formatMoneyShort(n);
}

function pvFmtMonthLabel(isoMonth) {
  if (!isoMonth) return "";
  try {
    const [year, month] = isoMonth.split("-");
    const meses = ["ene", "feb", "mar", "abr", "may", "jun",
                   "jul", "ago", "sep", "oct", "nov", "dic"];
    return `${meses[parseInt(month, 10) - 1]} ${year.slice(2)}`;
  } catch (_) {
    return isoMonth;
  }
}

// API helper
async function pvPost(endpoint, body = {}) {
  const res = await fetch(`${BASE}${endpoint}`, {
    method: "POST",
    headers: { "Content-Type": "application/json" },
    body: JSON.stringify(body),
  });
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  return res.json();
}

function pvCurrentPayload(extra = {}) {
  const base = {};
  if (PV.fechaDesde) base.fecha_desde = PV.fechaDesde;
  if (PV.fechaHasta) base.fecha_hasta = PV.fechaHasta;
  return { ...base, ...extra };
}

// Tabs
function pvSwitchTab(tab, btn) {
  document.querySelectorAll(".pf-tab").forEach(b => b.classList.remove("active"));
  document.querySelectorAll(".pf-tab-content").forEach(c => c.classList.remove("active"));
  btn.classList.add("active");
  document.getElementById(`tab-${tab}`).classList.add("active");
  PV.activeTab = tab;
}

// Toolbar principal
function pvApplyFilters() {
  PV.fechaDesde = document.getElementById("pvFechaDesde").value || "";
  PV.fechaHasta = document.getElementById("pvFechaHasta").value || "";

  if (PV.activeTab === "articulo") {
    pvCommitDropdownSelection("artFamiliaMount");
    pvCommitDropdownSelection("artClienteMount");
  }
  if (PV.activeTab === "cliente") {
    pvCommitDropdownSelection("cliClienteMount");
    pvCommitDropdownSelection("cliFamiliaMount");
  }

  if (PV.activeTab === "articulo") pvLoadArticulo();
  if (PV.activeTab === "cliente") pvLoadCliente();
}

function pvApplyDates() {
  pvApplyFilters();
}

function pvClearAll() {
  document.getElementById("pvFechaDesde").value = "";
  document.getElementById("pvFechaHasta").value = "";
  PV.fechaDesde = "";
  PV.fechaHasta = "";

  if (PV.activeTab === "articulo") {
    PV.artFamilia = null;
    PV.artCliente = null;
    pvClearDropdownSelection("artFamiliaMount");
    pvClearDropdownSelection("artClienteMount");
    pvShowArticuloEmpty();
  }

  if (PV.activeTab === "cliente") {
    PV.cliCliente = null;
    PV.cliFamilia = null;
    pvClearDropdownSelection("cliClienteMount");
    pvClearDropdownSelection("cliFamiliaMount");
    pvShowClienteEmpty();
  }
}

// Dropdowns

function pvBuildDropdown(mountId, label, options, currentVal, onChangeFn, required = false) {
  const mount = document.getElementById(mountId);
  if (!mount) return;

  const allLabel = required ? `Selecciona ${label}` : `Sin filtro de ${label.toLowerCase()}`;
  const allMeta = required ? "Filtro principal para habilitar el reporte" : "Filtro complementario opcional";
  const searchPlaceholder = `Buscar ${label.toLowerCase()}...`;
  const safeOptions = Array.isArray(options) ? options.map(cleanMojibakeText).filter(Boolean) : [];

  PV.dropdowns[mountId] = {
    label,
    options: safeOptions,
    selected: currentVal || "",
    draft: currentVal || "",
    query: "",
    onChange: onChangeFn,
    required,
    allLabel,
    allMeta,
  };

  mount.innerHTML = `
    <div class="pf-multi pf-multi--single" data-pv-filter-id="${mountId}">
      <label class="pf-multi__label" for="${mountId}Trigger">
        ${pvEsc(label)}${required ? '<span class="pf-multi__label-badge">Obligatorio</span>' : ''}
      </label>
      <button type="button" class="pf-multi__trigger" id="${mountId}Trigger" onclick="pvToggleFilterPanel('${mountId}')">
        <span class="pf-multi__summary">
          <span class="pf-multi__value${currentVal ? '' : ' is-placeholder'}" id="${mountId}Value">${pvEsc(currentVal || allLabel)}</span>
          <span class="pf-multi__meta" id="${mountId}Meta">${pvEsc(currentVal ? 'Seleccionado' : allMeta)}</span>
        </span>
        <i class="bi bi-chevron-down"></i>
      </button>
      <div class="pf-multi__panel" id="${mountId}Panel">
        <div class="pf-multi__panel-head">
          <input type="text" class="pf-multi__search" id="${mountId}Search" placeholder="${pvEscAttr(searchPlaceholder)}" autocomplete="off" oninput="pvFilterDropdownOptions('${mountId}', this.value)">
        </div>
        <div class="pf-multi__panel-body">
          <div class="pf-multi__list" id="${mountId}Options"></div>
        </div>
        <div class="pf-multi__panel-actions">
          <button type="button" class="pf-btn-clear" onclick="pvClearDropdownDraft('${mountId}')">
            <i class="bi bi-arrow-counterclockwise"></i>
            Limpiar
          </button>
          <button type="button" class="pf-btn-apply" onclick="pvApplyDropdownSelection('${mountId}')">
            <i class="bi bi-check2"></i>
            Aplicar
          </button>
        </div>
      </div>
    </div>`;
  pvRenderDropdownOptions(mountId);
}

function pvResetDropdown(mountId, label) {
  const mount = document.getElementById(mountId);
  if (!mount) return;
  delete PV.dropdowns[mountId];
  mount.innerHTML = `
    <div class="pf-multi pf-multi--single">
      <label class="pf-multi__label">${pvEsc(label)}</label>
      <button type="button" class="pf-multi__trigger is-locked" disabled>
        <span class="pf-multi__summary">
          <span class="pf-multi__value is-placeholder">Cargando...</span>
          <span class="pf-multi__meta">Preparando opciones</span>
        </span>
        <i class="bi bi-chevron-down"></i>
      </button>
    </div>`;
}

function pvToggleFilterPanel(mountId) {
  const panel = document.getElementById(`${mountId}Panel`);
  const trigger = document.getElementById(`${mountId}Trigger`);
  if (!panel || !trigger) return;
  document.querySelectorAll('.pf-page--private .pf-multi__panel.is-open').forEach(openPanel => {
    if (openPanel !== panel) openPanel.classList.remove('is-open');
  });
  document.querySelectorAll('.pf-page--private .pf-multi__trigger.is-open').forEach(openTrigger => {
    if (openTrigger !== trigger) openTrigger.classList.remove('is-open');
  });
  const willOpen = !panel.classList.contains('is-open');
  panel.classList.toggle('is-open', willOpen);
  trigger.classList.toggle('is-open', willOpen);
  if (willOpen) {
    const state = PV.dropdowns[mountId];
    if (state) state.draft = state.selected || '';
    pvRenderDropdownOptions(mountId);
    setTimeout(() => document.getElementById(`${mountId}Search`)?.focus(), 0);
  }
}

function pvFilterDropdownOptions(mountId, query) {
  const state = PV.dropdowns[mountId];
  if (!state) return;
  state.query = query || '';
  pvRenderDropdownOptions(mountId);
}

function pvRenderDropdownOptions(mountId) {
  const state = PV.dropdowns[mountId];
  const list = document.getElementById(`${mountId}Options`);
  if (!state || !list) return;
  const q = state.query.trim().toLowerCase();
  const visible = state.options.filter(opt => !q || opt.toLowerCase().includes(q)).slice(0, 250);
  if (!visible.length) {
    list.innerHTML = '<div class="pf-multi__empty">No hay opciones disponibles para esta busqueda.</div>';
    return;
  }
  const emptyOption = state.required ? '' : `
    <label class="pf-multi__option">
      <input type="radio" name="${mountId}Option" ${!state.draft ? 'checked' : ''} onchange="pvSetDropdownDraft('${mountId}', '')">
      <span class="pf-multi__option-text">${pvEsc(state.allLabel)}</span>
    </label>`;
  list.innerHTML = emptyOption + visible.map((opt, idx) => `
    <label class="pf-multi__option">
      <input type="radio" name="${mountId}Option" ${state.draft === opt ? 'checked' : ''} onchange="pvSetDropdownDraftByVisibleIndex('${mountId}', ${idx})">
      <span class="pf-multi__option-text" title="${pvEscAttr(opt)}">${pvEsc(opt)}</span>
    </label>`).join('');
  state.visible = visible;
}

function pvSetDropdownDraft(mountId, value) {
  const state = PV.dropdowns[mountId];
  if (state) state.draft = value || '';
}

function pvSetDropdownDraftByVisibleIndex(mountId, index) {
  const state = PV.dropdowns[mountId];
  if (!state) return;
  state.draft = state.visible?.[index] || '';
}

function pvClearDropdownDraft(mountId) {
  pvClearDropdownSelection(mountId);
  pvRefreshActiveDashboard();
}

function pvApplyDropdownSelection(mountId) {
  pvCommitDropdownSelection(mountId);
  pvUpdateDropdownTrigger(mountId);
  pvCloseDropdownPanel(mountId);
  pvRefreshActiveDashboard();
}

function pvCommitDropdownSelection(mountId) {
  const state = PV.dropdowns[mountId];
  if (!state) return false;
  state.selected = state.draft || '';
  if (typeof state.onChange === 'function') state.onChange(state.selected);
  return true;
}

function pvClearDropdownSelection(mountId) {
  const state = PV.dropdowns[mountId];
  if (!state) return;
  state.selected = '';
  state.draft = '';
  state.query = '';
  if (typeof state.onChange === 'function') state.onChange('');
  const search = document.getElementById(`${mountId}Search`);
  if (search) search.value = '';
  pvRenderDropdownOptions(mountId);
  pvUpdateDropdownTrigger(mountId);
  pvCloseDropdownPanel(mountId);
}

function pvRefreshActiveDashboard() {
  if (PV.activeTab === "articulo") pvLoadArticulo();
  if (PV.activeTab === "cliente") pvLoadCliente();
}

function pvUpdateDropdownTrigger(mountId) {
  const state = PV.dropdowns[mountId];
  const value = document.getElementById(`${mountId}Value`);
  const meta = document.getElementById(`${mountId}Meta`);
  if (!state || !value || !meta) return;
  const hasValue = Boolean(state.selected);
  value.textContent = hasValue ? state.selected : state.allLabel;
  value.classList.toggle('is-placeholder', !hasValue);
  meta.textContent = hasValue ? 'Seleccionado' : state.allMeta;
}

function pvCloseDropdownPanel(mountId) {
  document.getElementById(`${mountId}Panel`)?.classList.remove('is-open');
  document.getElementById(`${mountId}Trigger`)?.classList.remove('is-open');
}

document.addEventListener('click', event => {
  if (event.target.closest('.pf-page--private .pf-multi')) return;
  document.querySelectorAll('.pf-page--private .pf-multi__panel.is-open').forEach(panel => panel.classList.remove('is-open'));
  document.querySelectorAll('.pf-page--private .pf-multi__trigger.is-open').forEach(trigger => trigger.classList.remove('is-open'));
});
function pvEsc(str) {
  return cleanMojibakeText(str).replace(/&/g, "&amp;").replace(/</g, "&lt;").replace(/>/g, "&gt;").replace(/"/g, "&quot;");
}

function pvEscAttr(str) {
  return cleanMojibakeText(str)
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

// Inicializacion
async function pvInit() {
  pvResetDropdown("artFamiliaMount", "Familia");
  pvResetDropdown("artClienteMount", "Cliente");
  pvResetDropdown("cliClienteMount", "Cliente");
  pvResetDropdown("cliFamiliaMount", "Familia");

  try {
    const res = await pvPost("/filters", {});
    if (res.ok && res.data) {
      PV.filterOptions = res.data;
      pvUpdateHeroStats(res.data);
      pvRenderFilters();
    }
  } catch (e) {
    console.warn("[PV] Error cargando filtros:", e);
  }
}

function pvUpdateHeroStats(data) {
  const famCount = (data.familias || []).length;
  const cliCount = (data.clientes || []).length;
  document.getElementById("pvFamiliasLabel").textContent = famCount > 0 ? pvFmtInt(famCount) : "-";
  document.getElementById("pvClientesLabel").textContent = cliCount > 0 ? pvFmtInt(cliCount) : "-";

  const dr = data.date_range || {};
  if (dr.min && dr.max) {
    document.getElementById("pvPeriodoLabel").textContent =
      `${pvFmtMonthLabel(dr.min)} \u2013 ${pvFmtMonthLabel(dr.max)}`;
  }
}

function pvRenderFilters() {
  const { familias = [], clientes = [] } = PV.filterOptions;

  pvBuildDropdown(
    "artFamiliaMount", "Familia", familias, PV.artFamilia,
    function(val) { PV.artFamilia = val || null; },
    true
  );
  pvBuildDropdown(
    "artClienteMount", "Cliente", clientes, PV.artCliente,
    function(val) { PV.artCliente = val || null; }
  );
  pvBuildDropdown(
    "cliClienteMount", "Cliente", clientes, PV.cliCliente,
    function(val) { PV.cliCliente = val || null; },
    true
  );
  pvBuildDropdown(
    "cliFamiliaMount", "Familia", familias, PV.cliFamilia,
    function(val) { PV.cliFamilia = val || null; }
  );
}

// Empty states
function pvShowArticuloEmpty() {
  document.getElementById("artEmptyState").style.display = "";
  document.getElementById("artDashboard").hidden = true;
}

function pvShowArticuloDashboard() {
  document.getElementById("artEmptyState").style.display = "none";
  document.getElementById("artDashboard").hidden = false;
}

function pvShowClienteEmpty() {
  document.getElementById("cliEmptyState").style.display = "";
  document.getElementById("cliDashboard").hidden = true;
}

function pvShowClienteDashboard() {
  document.getElementById("cliEmptyState").style.display = "none";
  document.getElementById("cliDashboard").hidden = false;
}

// KPI rendering
function pvRenderKpiRow(containerId, kpis) {
  const row = document.getElementById(containerId);
  if (!row) return;
  row.innerHTML = kpis.map(k => `
    <div class="pf-kpi">
      <div class="pf-kpi__header">
        <div class="pf-kpi__label">${pvEsc(k.label)}</div>
        <div class="pf-kpi__icon"><i class="bi ${pvKpiIcon(k.label)}"></i></div>
      </div>
      <div class="pf-kpi__value">${pvEsc(k.value)}</div>
      ${k.sub ? `<div class="pf-kpi__sub">${pvEsc(k.sub)}</div>` : ""}
    </div>
  `).join("");
}

function pvKpiIcon(label) {
  const text = String(label || "").toLowerCase();
  if (text.includes("precio")) return "bi-currency-dollar";
  if (text.includes("cantidad")) return "bi-box-seam";
  if (text.includes("familia")) return "bi-tags";
  if (text.includes("plataforma")) return "bi-columns-gap";
  if (text.includes("provincia")) return "bi-geo-alt";
  return "bi-graph-up-arrow";
}

// API helper
function pvDestroyChart(id) {
  if (PV.charts[id]) {
    try { PV.charts[id].destroy(); } catch (_) {}
    delete PV.charts[id];
  }
}

function pvChartDefaults() {
  return {
    chart: { fontFamily: "inherit", toolbar: { show: false }, animations: { enabled: true, speed: 400 } },
    grid: { borderColor: "rgba(87,112,176,0.1)", strokeDashArray: 3 },
    tooltip: { theme: "light", y: { formatter: (v) => formatMoneyFull(v) } },
  };
}

function pvRenderLineChart(elId, months, series, yFormatter) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const opts = {
    ...pvChartDefaults(),
    chart: { ...pvChartDefaults().chart, type: "line", height: 220 },
    series: (series || []).map(s => ({
      ...s,
      name: cleanMojibakeText(s.name),
      data: s.data || [],
    })),
    xaxis: {
      categories: months.map(pvFmtMonthLabel),
      labels: { style: { fontSize: "11px" }, rotate: -30 },
    },
    yaxis: {
      labels: {
        formatter: yFormatter || ((v) => formatMoneyShort(v)),
      },
    },
    stroke: { width: 2.5, curve: "smooth" },
    colors: COLORS.palette,
    markers: { size: 3 },
    legend: { position: "top", fontSize: "12px" },
  };
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}


function pvRenderAreaChart(elId, months, series, yFormatter) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const opts = {
    chart: {
      type: "area",
      height: 240,
      fontFamily: "inherit",
      toolbar: { show: false },
      animations: { enabled: true, speed: 400 },
    },
    series: (series || []).map(s => ({
      name: cleanMojibakeText(s.name || ""),
      data: (s.data || []).map(v => (v == null ? 0 : v)),
    })),
    xaxis: {
      categories: months.map(pvFmtMonthLabel),
      labels: { style: { fontSize: "11px" }, rotate: -30 },
    },
    yaxis: {
      labels: {
        formatter: yFormatter || ((v) => formatMoneyShort(v)),
      },
    },
    stroke: { curve: "smooth", width: 2 },
    fill: {
      type: "gradient",
      gradient: {
        opacityFrom: 0.55,
        opacityTo: 0.18,
        stops: [0, 90, 100],
      },
    },
    colors: COLORS.palette,
    markers: { size: 0, hover: { size: 4 } },
    legend: { position: "top", fontSize: "12px" },
    dataLabels: { enabled: false },
    grid: { borderColor: "rgba(87,112,176,0.1)", strokeDashArray: 3 },
    tooltip: {
      theme: "light",
      y: { formatter: (v) => formatMoneyFull(v) },
    },
  };
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}



function pvRenderBarChart(elId, months, values, yFormatter) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const opts = {
    ...pvChartDefaults(),
    chart: { ...pvChartDefaults().chart, type: "bar", height: 220 },
    series: [{ name: "Valorizaci\u00f3n", data: values }],
    xaxis: {
      categories: months.map(pvFmtMonthLabel),
      labels: { style: { fontSize: "11px" }, rotate: -30 },
    },
    yaxis: { labels: { formatter: yFormatter || ((v) => formatMoneyShort(v)) } },
    colors: [COLORS.brand700],
    dataLabels: { enabled: false },
    plotOptions: { bar: { borderRadius: 4, dataLabels: { enabled: false } } },
    tooltip: { y: { formatter: (v) => formatMoneyFull(v) } },
    legend: { show: false },
  };
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}

function pvRenderDonutChart(elId, labels, values) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const opts = {
    ...pvChartDefaults(),
    chart: { ...pvChartDefaults().chart, type: "donut", height: 220 },
    series: values,
    labels: (labels || []).map(cleanMojibakeText),
    colors: COLORS.palette,
    plotOptions: { pie: { donut: { size: "55%" } } },
    legend: { position: "bottom", fontSize: "12px" },
    dataLabels: {
      formatter: (val) => val.toFixed(1).replace(".", ",") + "%",
    },
    tooltip: {
      y: { formatter: (v) => formatMoneyFull(v) },
    },
  };
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}

function pvRenderTreemapChart(elId, data) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const points = (data || []).map(d => ({
    x: cleanMojibakeText(d.cliente || d.familia || d.subnegocio || EMPTY_DASH),
    y: Math.round(d.total_valorizado || 0),
  }));
  const total = points.reduce((acc, point) => acc + Math.max(point.y || 0, 0), 0);
  const visibleLabelIndexes = new Set(
    points
      .map((point, index) => ({ index, pct: total > 0 ? point.y / total : 0 }))
      .filter((point, index) => point.pct >= 0.08 || (index < 5 && point.pct >= 0.03))
      .map(point => point.index)
  );
  const opts = {
    ...pvChartDefaults(),
    chart: { ...pvChartDefaults().chart, type: "treemap", height: 220 },
    series: [{
      data: points,
    }],
    colors: COLORS.palette,
    tooltip: {
      custom: ({ seriesIndex, dataPointIndex, w }) => {
        const point = w.config.series[seriesIndex]?.data?.[dataPointIndex] || {};
        return `
          <div class="pv-treemap-tooltip">
            <div class="pv-treemap-tooltip__title">${pvEsc(point.x || EMPTY_DASH)}</div>
            <div class="pv-treemap-tooltip__value">${formatMoneyFull(point.y)}</div>
          </div>`;
      },
    },
    plotOptions: { treemap: { distributed: true, enableShades: false } },
    legend: { show: false },
    dataLabels: {
      enabled: true,
      formatter: (text, opts) => visibleLabelIndexes.has(opts.dataPointIndex) ? cleanMojibakeText(text) : "",
      style: { fontSize: "11px", fontWeight: 700 },
    },
  };
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}

function pvRenderHBarChart(elId, categories, values, options = {}) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (!el) return;
  const visibleHeight = options.visibleHeight || 260;
  const rowHeight = options.rowHeight || 26;
  const height = Math.max(visibleHeight, (categories || []).length * rowHeight);
  const opts = {
    ...pvChartDefaults(),
    chart: { ...pvChartDefaults().chart, type: "bar", height },
    series: [{ name: "Valorizaci\u00f3n", data: values }],
    xaxis: {
      categories: (categories || []).map(cleanMojibakeText),
      labels: { style: { fontSize: "11px" }, formatter: (v) => formatMoneyShort(v) },
    },
    yaxis: { labels: { style: { fontSize: "11px" }, maxWidth: 160 } },
    plotOptions: { bar: { horizontal: true, borderRadius: 4 } },
    colors: [COLORS.brand500],
    dataLabels: {
      enabled: true,
      formatter: (v) => formatMoneyShort(v),
      offsetX: 6,
      style: { fontSize: "11px", fontWeight: 700 },
    },
    tooltip: { y: { formatter: (v) => formatMoneyFull(v) } },
    legend: { show: false },
  };
  el.style.height = `${height}px`;
  el.style.minHeight = `${height}px`;
  PV.charts[elId] = new ApexCharts(el, opts);
  PV.charts[elId].render();
}

// Tabla consumo mensual
function pvRenderConsumoTable(tbodyId, rows) {
  const tbody = document.getElementById(tbodyId);
  if (!tbody) return;
  if (!rows || rows.length === 0) {
    tbody.innerHTML = `<tr><td colspan="14"><div class="pf-empty-state"><i class="bi bi-search"></i><p>Sin datos de consumo para la selecci\u00f3n.</p></div></td></tr>`;
    return;
  }
  tbody.innerHTML = rows.map(r => {
    const meses = r.meses || {};
    const cells = [1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12].map(m => {
      const v = meses[m] ?? meses[String(m)];
      return `<td class="num">${formatCellValue(v)}</td>`;
    }).join("");
    return `<tr>
      <td class="pf-cell-wrap">${pvEsc(r.familia || EMPTY_DASH)}</td>
      ${cells}
      <td class="num pf-total-col"><strong>${formatCellValue(r.total)}</strong></td>
    </tr>`;
  }).join("");
}

// CARGA ARTICULO
async function pvLoadArticulo() {
  if (!PV.artFamilia) {
    pvShowArticuloEmpty();
    return;
  }

  pvShowArticuloDashboard();
  pvRenderKpiSkeletons("artKpiRow", 3);

  const payload = pvCurrentPayload({
    familias: [PV.artFamilia],
    ...(PV.artCliente ? { clientes: [PV.artCliente] } : {}),
  });

  // Lanzar todas las queries en paralelo
  const [kpisRes, precioRes, montoRes, plataformaRes, clientesRes, consumoRes] = await Promise.allSettled([
    pvPost("/articulo/kpis", payload),
    pvPost("/articulo/precio-evolucion", payload),
    pvPost("/articulo/monto-evolucion", payload),
    pvPost("/articulo/plataforma", payload),
    pvPost("/articulo/clientes", payload),
    pvPost("/articulo/consumo-mensual", payload),
  ]);

  // KPIs
  if (kpisRes.status === "fulfilled" && kpisRes.value.ok) {
    const d = kpisRes.value.data;
    pvRenderKpiRow("artKpiRow", [
      {
        label: "Total valorizado",
        value: pvFmtMoney(d.total_valorizado),
        sub: "Suma de valorizaci\u00f3n estimada",
      },
      {
        label: "Mediana precio unitario",
        value: d.mediana_precio_unitario != null ? formatPrice(d.mediana_precio_unitario) : EMPTY_DASH,
        sub: "Mediana de precio por unidad",
      },
      {
        label: "Cantidad final",
        value: pvFmt(d.total_cantidad, 0),
        sub: "Suma de cantidad demandada",
      },
    ]);
  } else {
    pvRenderKpiRow("artKpiRow", [
      { label: "Total valorizado", value: EMPTY_DASH },
      { label: "Mediana precio unitario", value: EMPTY_DASH },
      { label: "Cantidad final", value: EMPTY_DASH },
    ]);
  }

// Grfico
  if (precioRes.status === "fulfilled" && precioRes.value.ok) {
    const d = precioRes.value.data;
    pvRenderLineChart(
      "artPrecioChart",
      d.months || [],
      [{ name: "Mediana precio unitario", data: d.values || [] }],
      (v) => pvFmtMoneyLabel(v)
    );
  } else {
    pvRenderChartError("artPrecioChart");
  }

// Grfico
  if (montoRes.status === "fulfilled" && montoRes.value.ok) {
    const d = montoRes.value.data;
    pvRenderBarChart("artMontoChart", d.months || [], d.values || []);
  } else {
    pvRenderChartError("artMontoChart");
  }

// Grfico
  if (plataformaRes.status === "fulfilled" && plataformaRes.value.ok) {
    const items = plataformaRes.value.data || [];
    pvRenderDonutChart(
      "artPlataformaChart",
      items.map(i => cleanMojibakeText(i.plataforma || "SIN DATO")),
      items.map(i => Math.round(i.total_valorizado))
    );
  } else {
    pvRenderChartError("artPlataformaChart");
  }

// Grfico
  if (clientesRes.status === "fulfilled" && clientesRes.value.ok) {
    pvRenderTreemapChart("artClientesChart", clientesRes.value.data || []);
  } else {
    pvRenderChartError("artClientesChart");
  }

  // Tabla consumo mensual
  if (consumoRes.status === "fulfilled" && consumoRes.value.ok) {
    pvRenderConsumoTable("artConsumoTbody", consumoRes.value.data?.rows || []);
  } else {
    pvRenderConsumoTable("artConsumoTbody", []);
  }
}

// CARGA ARTICULO
async function pvLoadCliente() {
  if (!PV.cliCliente) {
    pvShowClienteEmpty();
    return;
  }

  pvShowClienteDashboard();
  pvRenderKpiSkeletons("cliKpiRow", 4);

  const payload = pvCurrentPayload({
    clientes: [PV.cliCliente],
    ...(PV.cliFamilia ? { familias: [PV.cliFamilia] } : {}),
  });

  const [kpisRes, negocioRes, totalRes, rankingRes, subnegRes, consumoRes] = await Promise.allSettled([
    pvPost("/cliente/kpis", payload),
    pvPost("/cliente/negocio-evolucion", payload),
    pvPost("/cliente/total-evolucion", payload),
    pvPost("/cliente/producto-ranking", payload),
    pvPost("/cliente/subnegocio", payload),
    pvPost("/cliente/consumo-mensual", payload),
  ]);

  // KPIs
  if (kpisRes.status === "fulfilled" && kpisRes.value.ok) {
    const d = kpisRes.value.data;
    pvRenderKpiRow("cliKpiRow", [
      {
        label: "Total valorizado",
        value: pvFmtMoney(d.total_valorizado),
        sub: "Suma de valorizaci\u00f3n estimada",
      },
      {
        label: "Familias",
        value: pvFmtInt(d.familias),
        sub: "Familias distintas",
      },
      {
        label: "Plataforma",
        value: cleanMojibakeText(d.plataforma || "SIN DATO"),
        sub: "Plataforma principal",
      },
      {
        label: "Provincia",
        value: cleanMojibakeText(d.provincia || "SIN DATO"),
        sub: "Ubicaci\u00f3n del cliente",
      },
    ]);
  } else {
    pvRenderKpiRow("cliKpiRow", [
      { label: "Total valorizado", value: EMPTY_DASH },
      { label: "Familias", value: EMPTY_DASH },
      { label: "Plataforma", value: "SIN DATO" },
      { label: "Provincia", value: "SIN DATO" },
    ]);
  }

// Gráfico de área: Valorizado en el tiempo por negocio
  if (negocioRes.status === "fulfilled" && negocioRes.value.ok) {
    const d = negocioRes.value.data;
    const series = (d.datasets || []).map(ds => ({
      name: cleanMojibakeText(ds.label),
      data: ds.values || [],
    }));
    pvRenderAreaChart("cliNegocioChart", d.months || [], series);
  } else {
    pvRenderChartError("cliNegocioChart");
  }

// Grfico
  if (totalRes.status === "fulfilled" && totalRes.value.ok) {
    const d = totalRes.value.data;
    pvRenderBarChart("cliTotalChart", d.months || [], d.values || []);
  } else {
    pvRenderChartError("cliTotalChart");
  }

// Grfico
  if (rankingRes.status === "fulfilled" && rankingRes.value.ok) {
    const items = rankingRes.value.data || [];
    pvRenderHBarChart(
      "cliRankingChart",
      items.map(i => cleanMojibakeText(i.familia || EMPTY_DASH)),
      items.map(i => Math.round(i.total_valorizado)),
      { visibleHeight: 260, rowHeight: 26 }
    );
  } else {
    pvRenderChartError("cliRankingChart");
  }

// Grfico
  if (subnegRes.status === "fulfilled" && subnegRes.value.ok) {
    pvRenderTreemapChart("cliSubnegocioChart", subnegRes.value.data || []);
  } else {
    pvRenderChartError("cliSubnegocioChart");
  }

  // Tabla consumo mensual
  if (consumoRes.status === "fulfilled" && consumoRes.value.ok) {
    pvRenderConsumoTable("cliConsumoTbody", consumoRes.value.data?.rows || []);
  } else {
    pvRenderConsumoTable("cliConsumoTbody", []);
  }
}

// API helper
function pvRenderKpiSkeletons(containerId, count) {
  const row = document.getElementById(containerId);
  if (!row) return;
  row.innerHTML = Array.from({ length: count }, () =>
    `<div class="pf-kpi pf-skeleton pf-loading-kpi"></div>`
  ).join("");
}

function pvRenderChartError(elId) {
  pvDestroyChart(elId);
  const el = document.getElementById(elId);
  if (el) {
    el.innerHTML = `
      <div class="pf-empty-state" style="padding:1.5rem;text-align:center;">
        <i class="bi bi-exclamation-circle" style="font-size:1.4rem;color:var(--pf-text-muted)"></i>
        <p style="margin-top:.4rem;font-size:.8rem;color:var(--pf-text-muted)">Sin datos disponibles</p>
      </div>`;
  }
}

// Boot
document.addEventListener("DOMContentLoaded", () => {
  pvShowArticuloEmpty();
  pvShowClienteEmpty();
  pvInit();
});
