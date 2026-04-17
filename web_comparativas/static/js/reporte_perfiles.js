/* reporte_perfiles.js - Dashboard de Reporte de Perfiles (Mercado Publico) */
"use strict";

const PF = {
  activeTab: "articulos",
  globalFechaDesde: "",
  globalFechaHasta: "",
  globalPlataforma: "SIPROSA",
  charts: {},
  filterOptions: {},
  multiFilters: {},
};

const BASE = "/api/mercado-publico/perfiles";

const COLORS = {
  brand900: "#06486f",
  brand700: "#1e5c8a",
  brand500: "#5770b0",
  cyan: "#38bdf8",
  success: "#10b981",
  warning: "#f59e0b",
  neutral: "#94a3b8",
  slate: "#64748b",
  palette: ["#06486f", "#1e5c8a", "#5770b0", "#38bdf8", "#10b981", "#f59e0b", "#94a3b8", "#cbd5e1"],
};

const FILTER_CONFIG = {
  artDesc: {
    mountId: "artDescMount",
    tab: "articulos",
    label: "Descripcion",
    param: "descripcion",
    source: "remote",
    field: "descripcion",
    allLabel: "Todos los articulos",
    allMeta: "Filtro principal del reporte",
    searchPlaceholder: "Buscar articulo",
    required: true,
    defaultAll: false,
    emptyLabel: "Selecciona articulos",
    emptyMeta: "Elegi uno o varios para habilitar el reporte",
  },
  artMarca: {
    mountId: "artMarcaMount",
    tab: "articulos",
    label: "Marca",
    param: "marca",
    source: "remote",
    field: "marca",
    allLabel: "Todas las marcas",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar marca",
  },
  artProv: {
    mountId: "artProvMount",
    tab: "articulos",
    label: "Proveedor",
    param: "proveedor",
    source: "remote",
    field: "proveedor",
    allLabel: "Todos los proveedores",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar proveedor",
  },
  artRubro: {
    mountId: "artRubroMount",
    tab: "articulos",
    label: "Rubro",
    param: "rubro",
    source: "static",
    optionsKey: "rubros",
    allLabel: "Todos los rubros",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar rubro",
  },
  compProv: {
    mountId: "compProvMount",
    tab: "competidor",
    label: "Proveedor / Competidor",
    param: "proveedor",
    source: "remote",
    field: "proveedor",
    allLabel: "Todos los proveedores",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar proveedor",
  },
  compRubro: {
    mountId: "compRubroMount",
    tab: "competidor",
    label: "Rubro",
    param: "rubro",
    source: "static",
    optionsKey: "rubros",
    allLabel: "Todos los rubros",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar rubro",
  },
  compDesc: {
    mountId: "compDescMount",
    tab: "competidor",
    label: "Articulo",
    param: "descripcion",
    source: "remote",
    field: "descripcion",
    allLabel: "Todos los articulos",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar articulo",
  },
  cliComp: {
    mountId: "cliCompMount",
    tab: "cliente",
    label: "Comprador / Organismo",
    param: "comprador",
    source: "remote",
    field: "comprador",
    allLabel: "Todos los organismos",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar organismo",
  },
  cliPlat: {
    mountId: "cliPlatMount",
    tab: "cliente",
    label: "Plataforma",
    param: "plataforma",
    source: "static",
    optionsKey: "plataformas",
    allLabel: "Todas las plataformas",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar plataforma",
    lockedByGlobal: true,
  },
  cliProv: {
    mountId: "cliProvMount",
    tab: "cliente",
    label: "Provincia",
    param: "provincia",
    source: "static",
    optionsKey: "provincias",
    allLabel: "Todas las provincias",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar provincia",
  },
};

async function pfFetch(url) {
  const res = await fetch(url);
  if (!res.ok) throw new Error(`HTTP ${res.status}`);
  const json = await res.json();
  if (!json.ok) throw new Error(json.error || "Error del servidor");
  return json.data;
}

function pfFmt(n, decimals = 0) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  return num.toLocaleString("es-AR", {
    minimumFractionDigits: decimals,
    maximumFractionDigits: decimals,
  });
}

function pfPeso(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  return "$ " + num.toLocaleString("es-AR", {
    minimumFractionDigits: 2,
    maximumFractionDigits: 2,
  });
}

function pfFmtShort(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  const abs = Math.abs(num);
  if (abs >= 1e9) return "$ " + (num / 1e9).toFixed(1).replace(/\.0$/, "") + " B";
  if (abs >= 1e6) return "$ " + (num / 1e6).toFixed(1).replace(/\.0$/, "") + " M";
  if (abs >= 1e3) return "$ " + (num / 1e3).toFixed(1).replace(/\.0$/, "") + " K";
  return "$ " + pfFmt(num, 0);
}

function pfFmtNumShort(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  const abs = Math.abs(num);
  if (abs >= 1e9) return (num / 1e9).toFixed(1).replace(/\.0$/, "") + " B";
  if (abs >= 1e6) return (num / 1e6).toFixed(1).replace(/\.0$/, "") + " M";
  if (abs >= 1e3) return (num / 1e3).toFixed(1).replace(/\.0$/, "") + " K";
  return pfFmt(num, 0);
}

function pfKpiPeso(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  const abs = Math.abs(num);
  if (abs >= 1e9) return "$ " + (num / 1e9).toFixed(1).replace(/\.0$/, "") + " B";
  if (abs >= 1e6) return "$ " + (num / 1e6).toFixed(1).replace(/\.0$/, "") + " M";
  if (abs >= 1e3) return "$ " + (num / 1e3).toFixed(1).replace(/\.0$/, "") + " K";
  return "$ " + pfFmt(num, 2);
}

function pfKpiNum(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  const abs = Math.abs(num);
  if (abs >= 1e6) return (num / 1e6).toFixed(1).replace(/\.0$/, "") + " M";
  if (abs >= 1e3) return (num / 1e3).toFixed(1).replace(/\.0$/, "") + " K";
  return pfFmt(num, 0);
}

function pfTrunc(str, max = 32) {
  if (!str) return "-";
  return str.length > max ? `${str.substring(0, max)}...` : str;
}

function pfEsc(s) {
  return (s || "")
    .replace(/&/g, "&amp;")
    .replace(/</g, "&lt;")
    .replace(/>/g, "&gt;");
}

function pfEscAttr(s) {
  return (s || "")
    .replace(/&/g, "&amp;")
    .replace(/"/g, "&quot;")
    .replace(/'/g, "&#39;");
}

function pfCellText(value, max = 50) {
  const raw = value || "-";
  return `<span class="pf-cell-text" title="${pfEscAttr(raw)}">${pfEsc(pfTrunc(raw, max))}</span>`;
}

function pfTableEmpty(icon, message, colspan) {
  return `<tr><td colspan="${colspan}"><div class="pf-empty-state"><i class="bi ${icon}"></i><p>${message}</p></div></td></tr>`;
}

function pfUnique(values) {
  const seen = new Set();
  const out = [];
  values.forEach((value) => {
    const normalized = (value || "").trim();
    if (!normalized || seen.has(normalized)) return;
    seen.add(normalized);
    out.push(normalized);
  });
  return out;
}

function pfFilterMatches(value, query) {
  if (!query) return true;
  return String(value || "").toLowerCase().includes(query.toLowerCase());
}

function pfCreateFilterState(filterId, config) {
  const defaultAll = config.defaultAll !== false;
  PF.multiFilters[filterId] = {
    id: filterId,
    config,
    applied: [],
    appliedAll: defaultAll,
    draft: [],
    draftAll: defaultAll,
    options: [],
    allOptions: [],
    loading: false,
    loaded: false,
    query: "",
    cache: new Map(),
    requestSeq: 0,
    searchTimer: null,
  };
}

function pfRenderFilterControls() {
  Object.entries(FILTER_CONFIG).forEach(([filterId, config]) => {
    pfCreateFilterState(filterId, config);
    const mount = document.getElementById(config.mountId);
    if (!mount) return;

    const requiredBadge = config.required
      ? '<span class="pf-multi__label-badge">Obligatorio</span>'
      : "";

    mount.innerHTML = `
      <div class="pf-multi" data-filter-id="${filterId}">
        <label class="pf-multi__label" for="${filterId}Trigger">${pfEsc(config.label)} ${requiredBadge}</label>
        <button type="button" class="pf-multi__trigger" id="${filterId}Trigger" onclick="pfToggleFilterPanel('${filterId}')">
          <span class="pf-multi__summary">
            <span class="pf-multi__value is-placeholder" id="${filterId}Value">${pfEsc(config.allLabel)}</span>
            <span class="pf-multi__meta" id="${filterId}Meta">${pfEsc(config.allMeta)}</span>
          </span>
          <i class="bi bi-chevron-down"></i>
        </button>
        <div class="pf-multi__panel" id="${filterId}Panel">
          <div class="pf-multi__panel-head">
            <input
              type="text"
              class="pf-multi__search"
              id="${filterId}Search"
              data-filter-id="${filterId}"
              placeholder="${pfEscAttr(config.searchPlaceholder)}"
              autocomplete="off">
          </div>
          <div class="pf-multi__panel-body">
            <label class="pf-multi__all">
              <input type="checkbox" class="pf-multi__all-check" data-filter-id="${filterId}">
              <span>Todos</span>
            </label>
            <div class="pf-multi__list" id="${filterId}Options"></div>
          </div>
          <div class="pf-multi__panel-actions">
            <button type="button" class="pf-btn-clear" onclick="pfClearFilterDraft('${filterId}')">
              <i class="bi bi-arrow-counterclockwise"></i>
              Limpiar
            </button>
            <button type="button" class="pf-btn-apply" onclick="pfApplyFilterSelection('${filterId}')">
              <i class="bi bi-check2"></i>
              Aplicar
            </button>
          </div>
        </div>
      </div>`;

    pfUpdateFilterTrigger(filterId);
  });
}

function pfIsFilterLocked(filterId) {
  const state = PF.multiFilters[filterId];
  return Boolean(state?.config.lockedByGlobal && PF.globalPlataforma);
}

function pfGetFilterAppliedValues(filterId) {
  if (pfIsFilterLocked(filterId)) {
    return PF.globalPlataforma ? [PF.globalPlataforma] : [];
  }
  const state = PF.multiFilters[filterId];
  if (!state || state.appliedAll) return [];
  return state.applied || [];
}

function pfGetFilterUniverseValues(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return [];
  if (state.config.source === "static") {
    return PF.filterOptions[state.config.optionsKey] || [];
  }
  return state.allOptions.length ? state.allOptions : state.options;
}

function pfNormalizeFilterSelection(filterId, scope = "draft") {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  const valuesKey = scope === "draft" ? "draft" : "applied";
  const allKey = scope === "draft" ? "draftAll" : "appliedAll";

  if (state[allKey]) {
    state[valuesKey] = [];
    return;
  }

  state[valuesKey] = pfUnique(state[valuesKey] || []);
  const universe = pfGetFilterUniverseValues(filterId);
  if (!universe.length) return;

  const allSelected = universe.every((value) => state[valuesKey].includes(value));
  if (allSelected) {
    state[allKey] = true;
    state[valuesKey] = [];
  }
}

function pfUpdateFilterTrigger(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  const trigger = document.getElementById(`${filterId}Trigger`);
  const valueEl = document.getElementById(`${filterId}Value`);
  const metaEl = document.getElementById(`${filterId}Meta`);
  if (!trigger || !valueEl || !metaEl) return;

  const locked = pfIsFilterLocked(filterId);
  const values = pfGetFilterAppliedValues(filterId);
  const allSelected = !locked && state.appliedAll;
  let valueText = state.config.emptyLabel || state.config.allLabel;
  let metaText = state.config.emptyMeta || state.config.allMeta;
  let isPlaceholder = true;

  if (locked && values.length) {
    valueText = values[0];
    metaText = "Fijada por el alcance actual";
    isPlaceholder = false;
  } else if (allSelected) {
    valueText = state.config.allLabel;
    metaText = "Se aplican todas las opciones";
    isPlaceholder = false;
  } else if (values.length === 1) {
    valueText = values[0];
    metaText = "1 seleccionado";
    isPlaceholder = false;
  } else if (values.length > 1) {
    valueText = `${pfTrunc(values[0], 34)} +${values.length - 1}`;
    metaText = `${values.length} seleccionados`;
    isPlaceholder = false;
  }

  valueEl.textContent = valueText;
  metaEl.textContent = metaText;
  valueEl.classList.toggle("is-placeholder", isPlaceholder);
  trigger.disabled = locked;
  trigger.classList.toggle("is-locked", locked);
}

async function pfToggleFilterPanel(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state || pfIsFilterLocked(filterId)) return;

  const panel = document.getElementById(`${filterId}Panel`);
  const trigger = document.getElementById(`${filterId}Trigger`);
  const isOpen = panel?.classList.contains("is-open");

  pfCloseAllFilterPanels(filterId);
  if (isOpen) {
    pfCloseFilterPanel(filterId);
    return;
  }

  state.draft = [...state.applied];
  state.draftAll = state.appliedAll;
  state.query = "";

  if (panel) panel.classList.add("is-open");
  if (trigger) trigger.classList.add("is-open");

  const search = document.getElementById(`${filterId}Search`);
  if (search) search.value = "";

  await pfEnsureFilterOptions(filterId, "");
  if (search) search.focus();
}

function pfCloseFilterPanel(filterId) {
  const panel = document.getElementById(`${filterId}Panel`);
  const trigger = document.getElementById(`${filterId}Trigger`);
  if (panel) panel.classList.remove("is-open");
  if (trigger) trigger.classList.remove("is-open");
}

function pfCloseAllFilterPanels(exceptId = "") {
  Object.keys(PF.multiFilters).forEach((filterId) => {
    if (filterId === exceptId) return;
    pfCloseFilterPanel(filterId);
  });
}

async function pfEnsureFilterOptions(filterId, query = "") {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  state.query = query;
  state.loading = true;
  pfRenderFilterOptions(filterId);

  if (state.config.source === "static") {
    const source = PF.filterOptions[state.config.optionsKey] || [];
    state.allOptions = source;
    state.options = source.filter((value) => pfFilterMatches(value, query));
    state.loading = false;
    state.loaded = true;
    pfRenderFilterOptions(filterId);
    return;
  }

  const cacheKey = query.trim().toLowerCase();
  if (state.cache.has(cacheKey)) {
    state.options = state.cache.get(cacheKey) || [];
    state.loading = false;
    state.loaded = true;
    pfRenderFilterOptions(filterId);
    return;
  }

  const requestSeq = ++state.requestSeq;
  try {
    const params = new URLSearchParams({
      campo: state.config.field,
      q: query.trim(),
      limit: query.trim() ? "250" : "5000",
    });
    const data = await pfFetch(`${BASE}/filtros/search?${params}`);
    if (requestSeq !== state.requestSeq) return;
    state.cache.set(cacheKey, data);
    state.options = data;
    if (!query.trim()) state.allOptions = data;
    state.loaded = true;
  } catch (err) {
    if (requestSeq !== state.requestSeq) return;
    state.options = [];
    state.loaded = true;
  } finally {
    if (requestSeq === state.requestSeq) {
      state.loading = false;
      pfRenderFilterOptions(filterId);
    }
  }
}

function pfRenderFilterOptions(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  const allCheck = document.querySelector(`.pf-multi__all-check[data-filter-id="${filterId}"]`);
  const list = document.getElementById(`${filterId}Options`);
  if (!list) return;

  const values = pfUnique([...state.draft, ...state.options]);
  const visibleSelected = state.draftAll
    ? values.length
    : values.filter((value) => state.draft.includes(value)).length;
  if (allCheck) {
    allCheck.checked = state.draftAll;
    allCheck.indeterminate = !state.draftAll && visibleSelected > 0;
  }

  if (state.loading) {
    list.innerHTML = '<div class="pf-multi__empty">Buscando opciones...</div>';
    return;
  }

  if (!values.length) {
    list.innerHTML = '<div class="pf-multi__empty">No hay opciones disponibles para esta busqueda.</div>';
    return;
  }

  list.innerHTML = values.map((value) => {
    const checked = state.draftAll || state.draft.includes(value) ? "checked" : "";
    return `
      <label class="pf-multi__option">
        <input
          type="checkbox"
          class="pf-multi__option-check"
          data-filter-id="${filterId}"
          value="${pfEscAttr(value)}"
          ${checked}>
        <span class="pf-multi__option-text" title="${pfEscAttr(value)}">${pfEsc(value)}</span>
      </label>`;
  }).join("");
}

function pfHandleFilterSearchInput(filterId, query) {
  const state = PF.multiFilters[filterId];
  if (!state || pfIsFilterLocked(filterId)) return;

  clearTimeout(state.searchTimer);
  state.searchTimer = setTimeout(() => {
    pfEnsureFilterOptions(filterId, query);
  }, 220);
}

function pfHandleFilterOptionChange(filterId, value, checked) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  if (checked) {
    if (!state.draftAll) {
      state.draft = pfUnique([...state.draft, value]);
    }
  } else {
    if (state.draftAll) {
      const universe = pfGetFilterUniverseValues(filterId);
      state.draftAll = false;
      state.draft = universe.filter((item) => item !== value);
    } else {
      state.draft = state.draft.filter((item) => item !== value);
    }
  }
  pfNormalizeFilterSelection(filterId, "draft");
  pfRenderFilterOptions(filterId);
}

function pfClearFilterDraft(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state || pfIsFilterLocked(filterId)) return;

  state.draft = [];
  state.draftAll = state.config.defaultAll !== false;
  state.query = "";
  const search = document.getElementById(`${filterId}Search`);
  if (search) search.value = "";
  pfEnsureFilterOptions(filterId, "");
}

function pfApplyFilterSelection(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  if (!pfIsFilterLocked(filterId)) {
    state.applied = [...state.draft];
    state.appliedAll = state.draftAll || (!state.draft.length && state.config.defaultAll !== false);
    pfNormalizeFilterSelection(filterId, "applied");
  }

  pfCloseFilterPanel(filterId);
  pfUpdateFilterTrigger(filterId);

  if (state.config.tab === PF.activeTab) {
    pfLoadCurrentTab();
  } else if (state.config.tab === "articulos") {
    pfSetArtSelectionGate();
  }
}

document.addEventListener("input", (event) => {
  const search = event.target.closest(".pf-multi__search");
  if (!search) return;
  pfHandleFilterSearchInput(search.dataset.filterId || "", search.value || "");
});

document.addEventListener("change", (event) => {
  const option = event.target.closest(".pf-multi__option-check");
  if (option) {
    pfHandleFilterOptionChange(option.dataset.filterId || "", option.value || "", option.checked);
    return;
  }

  const allCheck = event.target.closest(".pf-multi__all-check");
  if (allCheck) {
    const filterId = allCheck.dataset.filterId || "";
    const state = PF.multiFilters[filterId];
    if (!state) return;
    state.draftAll = allCheck.checked;
    state.draft = [];
    pfRenderFilterOptions(filterId);
  }
});

document.addEventListener("click", (event) => {
  if (event.target.closest(".pf-multi")) return;
  pfCloseAllFilterPanels();
});

function pfGlobalParams() {
  const params = new URLSearchParams();
  if (PF.globalFechaDesde) params.set("fecha_desde", PF.globalFechaDesde);
  if (PF.globalFechaHasta) params.set("fecha_hasta", PF.globalFechaHasta);
  if (PF.globalPlataforma) params.set("plataforma", PF.globalPlataforma);
  return params;
}

function pfSetMultiParam(params, filterId, paramName = FILTER_CONFIG[filterId]?.param) {
  if (!paramName) return;
  const state = PF.multiFilters[filterId];
  if (state?.appliedAll) return;
  const values = pfGetFilterAppliedValues(filterId);
  if (values.length) params.set(paramName, values.join(","));
}

function pfDestroyChart(id) {
  if (!PF.charts[id]) return;
  try {
    PF.charts[id].destroy();
  } catch (err) {
    console.warn("[PF] No se pudo destruir el chart", id, err);
  }
  delete PF.charts[id];
}

function pfMergeAxis(base, override) {
  if (!override) return { ...base };
  return {
    ...base,
    ...override,
    labels: { ...(base.labels || {}), ...(override.labels || {}) },
  };
}

function pfAxisFmtMoney(val) {
  if (typeof val === "number") return pfFmtShort(val);
  const n = parseFloat(val);
  return isNaN(n) ? val : pfFmtShort(n);
}

function pfAxisFmtNum(val) {
  if (typeof val === "number") return pfFmtNumShort(val);
  const n = parseFloat(val);
  return isNaN(n) ? val : pfFmtNumShort(n);
}

function pfChartBase(overrides = {}) {
  const base = {
    chart: {
      background: "transparent",
      fontFamily: "Outfit, system-ui, sans-serif",
      toolbar: { show: false },
      animations: { speed: 350 },
      parentHeightOffset: 0,
    },
    colors: COLORS.palette,
    dataLabels: { enabled: false },
    grid: {
      borderColor: "rgba(148, 163, 184, 0.18)",
      strokeDashArray: 3,
      padding: { top: 2, right: 4, bottom: 0, left: 2 },
    },
    legend: {
      position: "bottom",
      fontSize: "11px",
      labels: { colors: COLORS.slate },
      itemMargin: { horizontal: 10, vertical: 4 },
    },
    stroke: { lineCap: "round" },
    tooltip: {
      theme: "dark",
      x: { show: true },
      style: { fontSize: "12px", fontFamily: "Outfit, system-ui, sans-serif" },
    },
    xaxis: {
      axisBorder: { show: false },
      axisTicks: { show: false },
      labels: {
        style: {
          colors: COLORS.slate,
          fontSize: "10px",
          fontWeight: 600,
        },
      },
    },
    yaxis: {
      labels: {
        style: {
          colors: COLORS.slate,
          fontSize: "10px",
          fontWeight: 600,
        },
      },
    },
    noData: {
      text: "Sin datos para el periodo seleccionado",
      style: { color: COLORS.slate, fontSize: "13px", fontFamily: "Outfit, system-ui, sans-serif" },
    },
  };

  return {
    ...base,
    ...overrides,
    chart: { ...base.chart, ...(overrides.chart || {}) },
    dataLabels: { ...base.dataLabels, ...(overrides.dataLabels || {}) },
    grid: { ...base.grid, ...(overrides.grid || {}) },
    legend: { ...base.legend, ...(overrides.legend || {}) },
    stroke: { ...base.stroke, ...(overrides.stroke || {}) },
    tooltip: { ...base.tooltip, ...(overrides.tooltip || {}) },
    xaxis: pfMergeAxis(base.xaxis, overrides.xaxis),
    yaxis: pfMergeAxis(base.yaxis, overrides.yaxis),
    noData: { ...base.noData, ...(overrides.noData || {}) },
  };
}

function pfRenderEmpty(elId, msg = "Sin datos", icon = "bi-bar-chart-fill") {
  const el = document.getElementById(elId);
  if (!el) return;
  el.innerHTML = `<div class="pf-empty-state"><i class="bi ${icon}"></i><p>${msg}</p></div>`;
}

function pfSetSyncBadge(mode, text) {
  const badge = document.getElementById("syncStatusBadge");
  const badgeText = document.getElementById("syncStatusText");
  if (!badge || !badgeText) return;

  badge.classList.remove("is-warning", "is-ready", "is-idle");
  badge.classList.add(mode);
  badgeText.textContent = text;
}

function pfUpdateRowsLabel(rows) {
  const el = document.getElementById("pfRowsLabel");
  if (!el) return;
  if (rows == null) {
    el.textContent = "Sincronizando";
    return;
  }
  el.textContent = pfFmt(rows);
}

function pfShortDate(value) {
  if (!value) return "";
  const parts = value.split("-");
  if (parts.length !== 3) return value;
  return `${parts[2]}/${parts[1]}/${parts[0].slice(2)}`;
}

function pfUpdateDateSummary() {
  const label = document.getElementById("pfDateSummaryLabel");
  if (!label) return;

  if (PF.globalFechaDesde && PF.globalFechaHasta) {
    label.textContent = `${pfShortDate(PF.globalFechaDesde)} a ${pfShortDate(PF.globalFechaHasta)}`;
    return;
  }
  if (PF.globalFechaDesde) {
    label.textContent = `Desde ${pfShortDate(PF.globalFechaDesde)}`;
    return;
  }
  if (PF.globalFechaHasta) {
    label.textContent = `Hasta ${pfShortDate(PF.globalFechaHasta)}`;
    return;
  }
  label.textContent = "Todas las fechas";
}

function pfKpiCard({ tone = "neutral", label, value, rawValue = null, sub = "", icon = "bi-graph-up" }) {
  const titleAttr = rawValue != null
    ? pfEscAttr(String(rawValue))
    : pfEscAttr(String(value).replace(/<[^>]*>/g, ""));
  const subHtml = sub
    ? `<p class="pf-kpi__sub">${pfEsc(sub)}</p>`
    : `<p class="pf-kpi__sub pf-kpi__sub--empty">&nbsp;</p>`;
  return `
    <div class="pf-kpi pf-kpi--${tone}">
      <div class="pf-kpi__header">
        <span class="pf-kpi__label">${pfEsc(label)}</span>
        <span class="pf-kpi__icon"><i class="bi ${icon}"></i></span>
      </div>
      <div class="pf-kpi__value" title="${titleAttr}">${value}</div>
      ${subHtml}
    </div>`;
}

function pfKpiError(message) {
  return pfKpiCard({
    tone: "neutral",
    label: "Error",
    value: "-",
    sub: message,
    icon: "bi-exclamation-circle",
  });
}

function pfHasArticleSelection() {
  const state = PF.multiFilters.artDesc;
  if (!state) return false;
  return state.appliedAll || state.applied.length > 0;
}

function pfResetArtOutputs() {
  ["artPrecioChart", "artCantidadChart", "artMarcaChart", "artProvChart"].forEach(pfDestroyChart);
  const row = document.getElementById("artKpiRow");
  const tbody = document.getElementById("artProvTbody");
  if (row) row.innerHTML = "";
  if (tbody) {
    tbody.innerHTML = pfTableEmpty(
      "bi-info-circle",
      "Selecciona primero un articulo para habilitar el detalle del reporte.",
      7
    );
  }
}

function pfSetArtSelectionGate() {
  const empty = document.getElementById("artEmptySelection");
  const dashboard = document.getElementById("artDashboard");
  const hasArticle = pfHasArticleSelection();

  if (empty) empty.hidden = hasArticle;
  if (dashboard) dashboard.hidden = !hasArticle;
  if (!hasArticle) pfResetArtOutputs();
}

function pfSyncLockedPlatformUI() {
  const state = PF.multiFilters.cliPlat;
  if (!state) return;

  if (PF.globalPlataforma) {
    state.applied = [PF.globalPlataforma];
    state.appliedAll = false;
    state.draft = [PF.globalPlataforma];
    state.draftAll = false;
  }
  pfUpdateFilterTrigger("cliPlat");
}

function pfLoadCurrentTab() {
  if (PF.activeTab === "articulos") return pfLoadArticulos();
  if (PF.activeTab === "competidor") return pfLoadCompetidor();
  return pfLoadCliente();
}

function pfSwitchTab(name, el) {
  PF.activeTab = name;
  document.querySelectorAll(".pf-tab").forEach((tab) => tab.classList.remove("active"));
  document.querySelectorAll(".pf-tab-content").forEach((content) => content.classList.remove("active"));
  if (el) el.classList.add("active");
  const content = document.getElementById(`tab-${name}`);
  if (content) content.classList.add("active");
  pfLoadCurrentTab();
}

function pfApplyGlobalFilters() {
  PF.globalFechaDesde = document.getElementById("gFechaDesde")?.value || "";
  PF.globalFechaHasta = document.getElementById("gFechaHasta")?.value || "";
  pfUpdateDateSummary();
  pfLoadCurrentTab();
}

function pfClearAll() {
  const globalFrom = document.getElementById("gFechaDesde");
  const globalTo = document.getElementById("gFechaHasta");
  if (globalFrom) globalFrom.value = "";
  if (globalTo) globalTo.value = "";

  PF.globalFechaDesde = "";
  PF.globalFechaHasta = "";

  Object.keys(PF.multiFilters).forEach((filterId) => {
    const state = PF.multiFilters[filterId];
    if (!state) return;
    if (!pfIsFilterLocked(filterId)) {
      state.applied = [];
      state.appliedAll = state.config.defaultAll !== false;
      state.draft = [];
      state.draftAll = state.config.defaultAll !== false;
    }
    state.query = "";
    const search = document.getElementById(`${filterId}Search`);
    if (search) search.value = "";
    pfUpdateFilterTrigger(filterId);
  });

  const proceso = document.getElementById("cliProcesoInput");
  if (proceso) proceso.value = "";

  pfSyncLockedPlatformUI();
  pfCloseAllFilterPanels();
  pfUpdateDateSummary();
  pfSetArtSelectionGate();
  pfLoadCurrentTab();
}

async function pfCheckSync() {
  const warn = document.getElementById("syncWarnBanner");
  try {
    const data = await pfFetch(`${BASE}/sync/status`);
    const pending = data.pending_uploads || 0;
    const totalRows = data.total_rows || 0;

    pfUpdateRowsLabel(totalRows);

    if (pending > 0) {
      pfSetSyncBadge("is-warning", `${pending} sin sincronizar`);
      if (warn) warn.style.display = "flex";
      return;
    }

    pfSetSyncBadge("is-ready", `${pfFmt(totalRows)} filas listas`);
    if (warn) warn.style.display = "none";
  } catch (err) {
    pfSetSyncBadge("is-idle", "Sin datos aun");
    pfUpdateRowsLabel(null);
  }
}

async function pfTriggerSync(event) {
  event.preventDefault();
  pfSetSyncBadge("is-idle", "Sincronizando");
  try {
    await fetch(`${BASE}/sync`, { method: "POST" });
    pfSetSyncBadge("is-idle", "Sync iniciado");
    setTimeout(pfCheckSync, 5000);
  } catch (err) {
    pfSetSyncBadge("is-warning", "Error al sincronizar");
  }
}

async function pfLoadFilterOptions() {
  try {
    const data = await pfFetch(`${BASE}/filtros`);
    PF.filterOptions = data;
    Object.keys(PF.multiFilters).forEach((filterId) => {
      const state = PF.multiFilters[filterId];
      if (!state || state.config.source !== "static") return;
      if (document.getElementById(`${filterId}Panel`)?.classList.contains("is-open")) {
        pfEnsureFilterOptions(filterId, state.query || "");
      }
      pfUpdateFilterTrigger(filterId);
    });
    pfSyncLockedPlatformUI();
  } catch (err) {
    console.warn("[PF] No se pudieron cargar opciones de filtros:", err.message);
  }
}

function pfGetArtParams() {
  const params = pfGlobalParams();
  pfSetMultiParam(params, "artDesc");
  pfSetMultiParam(params, "artMarca");
  pfSetMultiParam(params, "artProv");
  pfSetMultiParam(params, "artRubro");
  return params;
}

async function pfLoadArticulos() {
  pfSetArtSelectionGate();
  if (!pfHasArticleSelection()) return;

  await Promise.all([
    pfLoadArtKpis(pfGetArtParams()),
    pfLoadArtEvolucion(pfGetArtParams()),
    pfLoadArtPorMarca(pfGetArtParams()),
    pfLoadArtPorProveedor(pfGetArtParams()),
  ]);
}

async function pfLoadArtKpis(params) {
  const row = document.getElementById("artKpiRow");
  if (!row) return;

  try {
    const data = await pfFetch(`${BASE}/articulos/kpis?${params}`);
    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto ofertado",
        value: pfKpiPeso(data.total_ofertado),
        rawValue: pfPeso(data.total_ofertado),
        sub: "Total del periodo",
        icon: "bi-cash-stack",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Mediana precio",
        value: pfKpiPeso(data.mediana_precio),
        rawValue: pfPeso(data.mediana_precio),
        sub: "Precio de referencia",
        icon: "bi-coin",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Proveedores",
        value: pfKpiNum(data.proveedores_unicos),
        rawValue: pfFmt(data.proveedores_unicos),
        sub: "Con presencia en la muestra",
        icon: "bi-buildings",
      }),
      pfKpiCard({
        tone: "success",
        label: "Cant. solicitada",
        value: pfKpiNum(data.cantidad_solicitada),
        rawValue: pfFmt(data.cantidad_solicitada),
        sub: "Demanda del periodo",
        icon: "bi-box-seam",
      }),
      pfKpiCard({
        tone: "warning",
        label: "Marcas distintas",
        value: pfFmt(data.marcas_distintas),
        sub: `${pfFmt(data.procesos)} procesos analizados`,
        icon: "bi-tags",
      }),
    ].join("");
  } catch (err) {
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadArtEvolucion(params) {
  pfDestroyChart("artPrecioChart");
  pfDestroyChart("artCantidadChart");

  try {
    const data = await pfFetch(`${BASE}/articulos/evolucion?${params}`);
    if (!data.length) {
      pfRenderEmpty("artPrecioChart", "Sin datos para el periodo seleccionado.", "bi-graph-up");
      pfRenderEmpty("artCantidadChart", "Sin datos para el periodo seleccionado.", "bi-bar-chart");
      return;
    }

    const labels = data.map((d) => d.month_label);

    PF.charts.artPrecioChart = new ApexCharts(
      document.getElementById("artPrecioChart"),
      pfChartBase({
        chart: { type: "line", height: 256 },
        series: [{ name: "Marca", data: data.map((d) => d.avg_precio) }],
        xaxis: { categories: labels, labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        stroke: { width: 3, curve: "smooth" },
        markers: {
          size: 4,
          strokeWidth: 0,
          hover: { sizeOffset: 2 },
        },
        colors: [COLORS.brand700],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.artPrecioChart.render();

    PF.charts.artCantidadChart = new ApexCharts(
      document.getElementById("artCantidadChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [
          { name: "Cant. solicitada", data: data.map((d) => d.cantidad_solicitada) },
        ],
        xaxis: { categories: labels, labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtNumShort(value) } },
        plotOptions: {
          bar: {
            borderRadius: 6,
            columnWidth: "52%",
          },
        },
        colors: [COLORS.brand500],
      })
    );
    PF.charts.artCantidadChart.render();
  } catch (err) {
    pfRenderEmpty("artPrecioChart", "Error al cargar la evolucion.", "bi-graph-up");
    pfRenderEmpty("artCantidadChart", "Error al cargar las cantidades.", "bi-bar-chart");
  }
}

async function pfLoadArtPorMarca(params) {
  pfDestroyChart("artMarcaChart");

  try {
    const data = await pfFetch(`${BASE}/articulos/por-marca?${params}`);
    if (!data.length) {
      pfRenderEmpty("artMarcaChart", "Sin datos de marca para el filtro actual.", "bi-tags");
      return;
    }

    const labelsRaw = Array.from(new Set(data.map((d) => d.fecha))).sort();
    const categories = labelsRaw.map(l => pfShortDate(l));
    const marcasUnicas = Array.from(new Set(data.map((d) => d.marca)));

    const seriesData = marcasUnicas.map(marca => ({
      name: pfTrunc(marca, 20),
      data: labelsRaw.map(l => {
        const match = data.find(d => d.fecha === l && d.marca === marca);
        return match ? match.precio_ganador : null;
      })
    }));

    PF.charts.artMarcaChart = new ApexCharts(
      document.getElementById("artMarcaChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: seriesData,
        xaxis: { categories: categories },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        plotOptions: {
          bar: {
            borderRadius: 4,
            columnWidth: "55%",
          },
        },
        legend: { show: false },
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.artMarcaChart.render();
  } catch (err) {
    pfRenderEmpty("artMarcaChart", "Error al cargar el benchmark por marca.", "bi-tags");
  }
}

async function pfLoadArtPorProveedor(params) {
  pfDestroyChart("artProvChart");
  const tbody = document.getElementById("artProvTbody");

  try {
    const data = await pfFetch(`${BASE}/articulos/por-proveedor?${params}`);
    if (!data.length) {
      pfRenderEmpty("artProvChart", "Sin proveedores para este articulo.", "bi-buildings");
      if (tbody) tbody.innerHTML = pfTableEmpty("bi-search", "Sin proveedores para este articulo.", 8);
      return;
    }

    const top = data.slice(0, 15);
    const treemapData = top.map((d) => ({ x: pfTrunc(d.proveedor, 28), y: d.total_adjudicado }));
    PF.charts.artProvChart = new ApexCharts(
      document.getElementById("artProvChart"),
      pfChartBase({
        chart: { type: "treemap", height: 310 },
        series: [{ name: "Monto adjudicado (pos. 1)", data: treemapData }],
        plotOptions: {
          treemap: {
            enableShades: true,
            shadeIntensity: 0.65,
            distributed: false,
          },
        },
        colors: [COLORS.brand700],
        dataLabels: {
          enabled: true,
          style: { fontSize: "12px", fontWeight: 600 },
          formatter: (text) => text,
        },
        tooltip: {
          custom({ dataPointIndex }) {
            const d = top[dataPointIndex];
            if (!d) return "";
            return (
              `<div class="apexcharts-tooltip-box" style="padding:10px 14px;line-height:1.6">` +
              `<strong>${d.proveedor}</strong><br>` +
              `Monto adjudicado: <strong>${pfPeso(d.total_adjudicado)}</strong><br>` +
              `<span style="font-size:11px;opacity:.75">Ganador (posición 1)</span>` +
              `</div>`
            );
          },
        },
      })
    );
    PF.charts.artProvChart.render();

    if (tbody) {
      tbody.innerHTML = data.map((d) => {
        const posClass = d.mejor_posicion === 1 ? "pos1" : d.mejor_posicion === 2 ? "pos2" : "posn";
        const provId = encodeURIComponent(d.proveedor);
        const hasMediana = d.precio_mediana_12m > 0;
        return `
          <tr class="pf-prov-row" data-proveedor="${provId}">
            <td>${pfCellText(d.proveedor, 48)}</td>
            <td class="num">${pfPeso(d.avg_precio)}</td>
            <td class="num">${pfFmt(d.veces_ganado)}</td>
            <td class="num"><span class="pf-badge ${posClass}">#${d.mejor_posicion ?? "-"}</span></td>
            <td class="num">${pfPeso(d.total_adjudicado)}</td>
            <td class="num">${pfFmt(d.procesos)}</td>
            <td class="num">${pfFmt(d.count)}</td>
            <td class="num pf-hist-cell">
              ${hasMediana ? `<div class="pf-hist-mediana"><span>${pfPeso(d.precio_mediana_12m)}</span><small>mediana</small></div>` : `<span class="pf-text-muted-sm">—</span>`}
              <button class="pf-hist-btn" title="Ver historial del último año"
                onclick="pfToggleHistorico(this, decodeURIComponent('${provId}'))">
                <i class="bi bi-chevron-down"></i>
              </button>
            </td>
          </tr>`;
      }).join("");
    }
  } catch (err) {
    pfRenderEmpty("artProvChart", "Error al cargar proveedores.", "bi-buildings");
    if (tbody) tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar el detalle de proveedores.", 8);
  }
}

async function pfToggleHistorico(btn, proveedor) {
  const row = btn.closest("tr");
  const histId = "pf-hist-" + btoa(encodeURIComponent(proveedor)).replace(/[^a-zA-Z0-9]/g, "_");
  const existing = document.getElementById(histId);

  if (existing) {
    existing.remove();
    btn.classList.remove("open");
    btn.innerHTML = `<i class="bi bi-chevron-down"></i>`;
    return;
  }

  btn.classList.add("open");
  btn.innerHTML = `<i class="bi bi-arrow-clockwise pf-spin"></i>`;

  try {
    const params = pfGetArtParams();
    params.set("proveedor", proveedor);
    const data = await pfFetch(`${BASE}/articulos/proveedor-historico?${params}`);

    const bodyRows = data.length
      ? data.map((r) => {
          const posC = r.posicion === 1 ? "pos1" : r.posicion === 2 ? "pos2" : "posn";
          return `<tr>
            <td>${r.fecha ?? "-"}</td>
            <td class="num">${pfPeso(r.precio)}</td>
            <td>${r.marca}</td>
            <td class="num"><span class="pf-badge ${posC}">#${r.posicion ?? "-"}</span></td>
          </tr>`;
        }).join("")
      : `<tr><td colspan="4" style="text-align:center;padding:.6rem;color:var(--pf-text-muted);font-size:.77rem">Sin registros en el último año para los filtros actuales</td></tr>`;

    const detailRow = document.createElement("tr");
    detailRow.id = histId;
    detailRow.className = "pf-hist-row";
    detailRow.innerHTML = `
      <td colspan="8">
        <div class="pf-hist-detail">
          <div class="pf-hist-header"><i class="bi bi-clock-history"></i> Historial último año — ${pfTrunc(proveedor, 60)}</div>
          <table class="pf-hist-table">
            <thead><tr>
              <th>Fecha</th>
              <th class="num">Precio unit.</th>
              <th>Marca</th>
              <th class="num">Pos.</th>
            </tr></thead>
            <tbody>${bodyRows}</tbody>
          </table>
        </div>
      </td>`;

    row.insertAdjacentElement("afterend", detailRow);
    btn.innerHTML = `<i class="bi bi-chevron-up"></i>`;
  } catch {
    btn.classList.remove("open");
    btn.innerHTML = `<i class="bi bi-chevron-down"></i>`;
  }
}

function pfGetCompParams() {
  const params = pfGlobalParams();
  pfSetMultiParam(params, "compProv");
  pfSetMultiParam(params, "compRubro");
  pfSetMultiParam(params, "compDesc");
  return params;
}

async function pfLoadCompetidor() {
  const params = pfGetCompParams();
  await Promise.all([
    pfLoadCompKpis(params),
    pfLoadCompEvolucion(params),
    pfLoadCompRubros(params),
    pfLoadCompTopArt(params),
    pfLoadCompTopMarcas(params),
    pfLoadCompPosiciones(params),
  ]);
}

async function pfLoadCompKpis(params) {
  const row = document.getElementById("compKpiRow");
  if (!row) return;

  try {
    const data = await pfFetch(`${BASE}/competidor/kpis?${params}`);
    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto ofertado",
        value: pfKpiPeso(data.total_ofertado),
        rawValue: pfPeso(data.total_ofertado),
        sub: "Peso economico del competidor",
        icon: "bi-cash-coin",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Procesos",
        value: pfKpiNum(data.procesos),
        rawValue: pfFmt(data.procesos),
        sub: "Con participacion registrada",
        icon: "bi-diagram-3",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Articulos cotizados",
        value: pfKpiNum(data.descripciones_cotizadas),
        rawValue: pfFmt(data.descripciones_cotizadas),
        sub: "Variedad de articulos",
        icon: "bi-box-seam",
      }),
      pfKpiCard({
        tone: "success",
        label: "Rubros cubiertos",
        value: pfFmt(data.rubros_cubiertos),
        sub: "Cobertura del mix",
        icon: "bi-grid-1x2",
      }),
      pfKpiCard({
        tone: "success",
        label: "Marcas utilizadas",
        value: pfFmt(data.marcas_utilizadas),
        sub: "En sus ofertas",
        icon: "bi-tags",
      }),
      pfKpiCard({
        tone: "warning",
        label: "Posicion promedio",
        value: data.posicion_promedio != null ? pfFmt(data.posicion_promedio, 1) : "-",
        sub: "Lugar medio en competencia",
        icon: "bi-speedometer2",
      }),
      pfKpiCard({
        tone: "neutral",
        label: "Mejor posicion",
        value: data.mejor_posicion != null ? String(data.mejor_posicion) : "-",
        sub: "Mejor puesto alcanzado",
        icon: "bi-trophy",
      }),
    ].join("");
  } catch (err) {
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadCompEvolucion(params) {
  pfDestroyChart("compEvolChart");

  try {
    const data = await pfFetch(`${BASE}/competidor/evolucion?${params}`);
    if (!data.length) {
      pfRenderEmpty("compEvolChart", "Sin datos para el competidor seleccionado.", "bi-graph-up-arrow");
      return;
    }

    PF.charts.compEvolChart = new ApexCharts(
      document.getElementById("compEvolChart"),
      pfChartBase({
        chart: { type: "area", height: 256 },
        series: [{ name: "Monto ofertado", data: data.map((d) => d.monto_total) }],
        xaxis: { categories: data.map((d) => d.month_label), labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        fill: {
          type: "gradient",
          gradient: {
            shadeIntensity: 1,
            opacityFrom: 0.32,
            opacityTo: 0.04,
            stops: [0, 95, 100],
          },
        },
        stroke: { width: 3, curve: "smooth" },
        colors: [COLORS.brand500],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.compEvolChart.render();
  } catch (err) {
    pfRenderEmpty("compEvolChart", "Error al cargar la evolucion del competidor.", "bi-graph-up-arrow");
  }
}

async function pfLoadCompRubros(params) {
  pfDestroyChart("compRubroChart");

  try {
    const data = await pfFetch(`${BASE}/competidor/rubros?${params}`);
    if (!data.length) {
      pfRenderEmpty("compRubroChart", "Sin datos de rubros.", "bi-pie-chart");
      return;
    }

    PF.charts.compRubroChart = new ApexCharts(
      document.getElementById("compRubroChart"),
      pfChartBase({
        chart: { type: "donut", height: 256 },
        series: data.map((d) => d.monto_total),
        labels: data.map((d) => pfTrunc(d.rubro, 24)),
        colors: [COLORS.brand900, COLORS.brand500, COLORS.cyan, COLORS.success, COLORS.warning, COLORS.neutral],
        plotOptions: {
          pie: {
            donut: {
              size: "68%",
              labels: { show: false },
            },
          },
        },
        legend: {
          position: "bottom",
          horizontalAlign: "center",
        },
        dataLabels: { enabled: true, formatter: (value) => `${value.toFixed(1)}%` },
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.compRubroChart.render();
  } catch (err) {
    pfRenderEmpty("compRubroChart", "Error al cargar los rubros.", "bi-pie-chart");
  }
}

async function pfLoadCompTopArt(params) {
  pfDestroyChart("compArtChart");

  try {
    const data = await pfFetch(`${BASE}/competidor/top-articulos?${params}`);
    if (!data.length) {
      pfRenderEmpty("compArtChart", "Sin articulos para este filtro.", "bi-bar-chart");
      return;
    }

    const top = data.slice(0, 10);
    PF.charts.compArtChart = new ApexCharts(
      document.getElementById("compArtChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Monto ofertado", data: top.map((d) => d.monto_total) }],
        xaxis: {
          categories: top.map((d) => pfTrunc(d.descripcion, 24)),
          labels: { formatter: pfAxisFmtMoney },
        },
        yaxis: { labels: { formatter: pfAxisFmtMoney } },
        plotOptions: {
          bar: {
            horizontal: true,
            borderRadius: 7,
            barHeight: "70%",
          },
        },
        colors: [COLORS.success],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.compArtChart.render();
  } catch (err) {
    pfRenderEmpty("compArtChart", "Error al cargar articulos.", "bi-bar-chart");
  }
}

async function pfLoadCompTopMarcas(params) {
  pfDestroyChart("compMarcaChart");

  try {
    const data = await pfFetch(`${BASE}/competidor/top-marcas?${params}`);
    if (!data.length) {
      pfRenderEmpty("compMarcaChart", "Sin marcas para este filtro.", "bi-tags");
      return;
    }

    const top = data.slice(0, 10);
    PF.charts.compMarcaChart = new ApexCharts(
      document.getElementById("compMarcaChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Monto ofertado", data: top.map((d) => d.monto_total) }],
        xaxis: { categories: top.map((d) => pfTrunc(d.marca, 22)) },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        plotOptions: {
          bar: {
            borderRadius: 6,
            columnWidth: "48%",
          },
        },
        colors: [COLORS.warning],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.compMarcaChart.render();
  } catch (err) {
    pfRenderEmpty("compMarcaChart", "Error al cargar marcas.", "bi-tags");
  }
}

async function pfLoadCompPosiciones(params) {
  const tbody = document.getElementById("compPosTbody");
  if (!tbody) return;

  try {
    const data = await pfFetch(`${BASE}/competidor/posiciones?${params}`);
    if (!data.length) {
      tbody.innerHTML = pfTableEmpty("bi-building", "Sin articulos para este proveedor.", 5);
      return;
    }

    tbody.innerHTML = data.map((d) => {
      const posClass = d.mejor_posicion === 1 ? "pos1" : d.mejor_posicion === 2 ? "pos2" : "posn";
      return `
        <tr>
          <td>${pfCellText(d.descripcion, 56)}</td>
          <td class="num">${pfFmt(d.posicion_promedio, 1)}</td>
          <td class="num"><span class="pf-badge ${posClass}">#${d.mejor_posicion ?? "-"}</span></td>
          <td class="num">${pfPeso(d.monto_total)}</td>
          <td class="num">${pfFmt(d.count)}</td>
        </tr>`;
    }).join("");
  } catch (err) {
    tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar posiciones.", 5);
  }
}

function pfGetCliParams() {
  const params = pfGlobalParams();
  pfSetMultiParam(params, "cliComp");
  if (!PF.globalPlataforma) pfSetMultiParam(params, "cliPlat");
  pfSetMultiParam(params, "cliProv");

  const proceso = document.getElementById("cliProcesoInput")?.value.trim() || "";
  if (proceso) params.set("nro_proceso", proceso);
  return params;
}

async function pfLoadCliente() {
  const params = pfGetCliParams();
  await Promise.all([
    pfLoadCliKpis(params),
    pfLoadCliEvolucion(params),
    pfLoadCliRubros(params),
    pfLoadCliProveedores(params),
    pfLoadCliArticulos(params),
  ]);
}

async function pfLoadCliKpis(params) {
  const row = document.getElementById("cliKpiRow");
  if (!row) return;

  try {
    const data = await pfFetch(`${BASE}/cliente/kpis?${params}`);
    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto cotizado",
        value: pfKpiPeso(data.monto_total_cotizado),
        rawValue: pfPeso(data.monto_total_cotizado),
        sub: "Total ofertado al organismo",
        icon: "bi-cash-stack",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Procesos",
        value: pfKpiNum(data.procesos_analizados),
        rawValue: pfFmt(data.procesos_analizados),
        sub: "Incluidos en la lectura",
        icon: "bi-diagram-3",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Proveedores",
        value: pfFmt(data.proveedores_unicos),
        sub: "Diversidad de oferentes",
        icon: "bi-buildings",
      }),
      pfKpiCard({
        tone: "success",
        label: "Articulos distintos",
        value: pfKpiNum(data.descripciones_unicas),
        rawValue: pfFmt(data.descripciones_unicas),
        sub: "Variedad de demanda",
        icon: "bi-box-seam",
      }),
      pfKpiCard({
        tone: "success",
        label: "Rubros distintos",
        value: pfFmt(data.rubros_distintos),
        sub: "Mix de compra",
        icon: "bi-grid-1x2",
      }),
      pfKpiCard({
        tone: "warning",
        label: "Ticket promedio",
        value: pfKpiPeso(data.ticket_promedio),
        rawValue: pfPeso(data.ticket_promedio),
        sub: "Por proceso",
        icon: "bi-graph-up-arrow",
      }),
    ].join("");
  } catch (err) {
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadCliEvolucion(params) {
  pfDestroyChart("cliEvolChart");

  try {
    const data = await pfFetch(`${BASE}/cliente/evolucion?${params}`);
    if (!data.length) {
      pfRenderEmpty("cliEvolChart", "Sin datos para este perfil de cliente.", "bi-graph-up-arrow");
      return;
    }

    PF.charts.cliEvolChart = new ApexCharts(
      document.getElementById("cliEvolChart"),
      pfChartBase({
        chart: { type: "area", height: 256 },
        series: [{ name: "Monto cotizado", data: data.map((d) => d.monto_total) }],
        xaxis: { categories: data.map((d) => d.month_label), labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        fill: {
          type: "gradient",
          gradient: {
            shadeIntensity: 1,
            opacityFrom: 0.3,
            opacityTo: 0.05,
            stops: [0, 95, 100],
          },
        },
        stroke: { width: 3, curve: "smooth" },
        colors: [COLORS.success],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.cliEvolChart.render();
  } catch (err) {
    pfRenderEmpty("cliEvolChart", "Error al cargar la evolucion del cliente.", "bi-graph-up-arrow");
  }
}

async function pfLoadCliRubros(params) {
  pfDestroyChart("cliRubroChart");

  try {
    const data = await pfFetch(`${BASE}/cliente/rubros?${params}`);
    if (!data.length) {
      pfRenderEmpty("cliRubroChart", "Sin datos de rubros.", "bi-pie-chart-fill");
      return;
    }

    PF.charts.cliRubroChart = new ApexCharts(
      document.getElementById("cliRubroChart"),
      pfChartBase({
        chart: { type: "donut", height: 256 },
        series: data.map((d) => d.monto_total),
        labels: data.map((d) => pfTrunc(d.rubro, 24)),
        colors: [COLORS.brand700, COLORS.cyan, COLORS.success, COLORS.warning, COLORS.neutral, COLORS.brand500],
        plotOptions: {
          pie: {
            donut: {
              size: "68%",
              labels: { show: false },
            },
          },
        },
        dataLabels: { enabled: true, formatter: (value) => `${value.toFixed(1)}%` },
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.cliRubroChart.render();
  } catch (err) {
    pfRenderEmpty("cliRubroChart", "Error al cargar los rubros.", "bi-pie-chart-fill");
  }
}

async function pfLoadCliProveedores(params) {
  pfDestroyChart("cliProvChart");

  try {
    const data = await pfFetch(`${BASE}/cliente/proveedores?${params}`);
    if (!data.length) {
      pfRenderEmpty("cliProvChart", "Sin proveedores para el perfil filtrado.", "bi-buildings");
      return;
    }

    const top = data.slice(0, 12);
    PF.charts.cliProvChart = new ApexCharts(
      document.getElementById("cliProvChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Monto ofertado", data: top.map((d) => d.monto_total) }],
        xaxis: {
          categories: top.map((d) => pfTrunc(d.proveedor, 24)),
          labels: { formatter: pfAxisFmtMoney },
        },
        yaxis: { labels: { formatter: pfAxisFmtMoney } },
        plotOptions: {
          bar: {
            horizontal: true,
            borderRadius: 7,
            barHeight: "68%",
          },
        },
        colors: [COLORS.brand900],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.cliProvChart.render();
  } catch (err) {
    pfRenderEmpty("cliProvChart", "Error al cargar proveedores.", "bi-buildings");
  }
}

async function pfLoadCliArticulos(params) {
  pfDestroyChart("cliArtChart");
  const tbody = document.getElementById("cliArtTbody");

  try {
    const data = await pfFetch(`${BASE}/cliente/articulos?${params}`);

    if (data.length) {
      const top = data.slice(0, 10);
      PF.charts.cliArtChart = new ApexCharts(
        document.getElementById("cliArtChart"),
        pfChartBase({
          chart: { type: "bar", height: 256 },
          series: [{ name: "Cant. solicitada total", data: top.map((d) => d.cant_total) }],
          xaxis: {
            categories: top.map((d) => pfTrunc(d.descripcion, 24)),
            labels: { formatter: pfAxisFmtNum },
          },
          yaxis: { labels: { formatter: pfAxisFmtNum } },
          plotOptions: {
            bar: {
              horizontal: true,
              borderRadius: 7,
              barHeight: "68%",
            },
          },
          colors: [COLORS.warning],
        })
      );
      PF.charts.cliArtChart.render();
    } else {
      pfRenderEmpty("cliArtChart", "Sin articulos para este perfil de cliente.", "bi-list-stars");
    }

    if (tbody) {
      if (!data.length) {
        tbody.innerHTML = pfTableEmpty("bi-hospital", "Sin articulos para este perfil de cliente.", 5);
        return;
      }

      tbody.innerHTML = data.map((d) => `
        <tr>
          <td>${pfCellText(d.descripcion, 58)}</td>
          <td class="num">${pfFmt(d.cant_total, 1)}</td>
          <td class="num">${pfFmt(d.frecuencia)}</td>
          <td class="num">${pfPeso(d.monto_total)}</td>
          <td class="num">${pfPeso(d.avg_precio)}</td>
        </tr>`).join("");
    }
  } catch (err) {
    pfRenderEmpty("cliArtChart", "Error al cargar articulos.", "bi-list-stars");
    if (tbody) tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar el detalle del cliente.", 5);
  }
}

document.addEventListener("DOMContentLoaded", async () => {
  pfRenderFilterControls();
  pfUpdateDateSummary();
  await pfLoadFilterOptions();
  pfSyncLockedPlatformUI();
  pfSetArtSelectionGate();
  await pfCheckSync();
});
