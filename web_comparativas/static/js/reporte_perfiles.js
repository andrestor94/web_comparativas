/* reporte_perfiles.js - Dashboard de Reporte de Perfiles (Mercado Publico) */
"use strict";

const PF = {
  activeTab: "articulos",
  globalFechaDesde: "",
  globalFechaHasta: "",
  globalPlataforma: "SIPROSA",
  dateUserSet: false,
  charts: {},
  requestGuards: {},
  filterOptions: {},
  multiFilters: {},
  cliProvData: [],
  cliProvMetric: "monto_total",
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
    allLabel: "Articulo",
    allMeta: "Filtro principal del reporte",
    searchPlaceholder: "Buscar articulo",
    required: true,
    selectionMode: "single",
    showSelectAll: false,
    autoApply: true,
    defaultAll: false,
    emptyLabel: "Seleccione un articulo",
    emptyMeta: "Elegi un articulo para habilitar el reporte",
  },
  artMarca: {
    mountId: "artMarcaMount",
    tab: "articulos",
    label: "Marca",
    param: "marca",
    source: "remote",
    field: "marca",
    allLabel: "Sin filtro de marca",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar marca",
    emptyLabel: "Seleccionar marca",
    emptyMeta: "Opcional",
  },
  artProv: {
    mountId: "artProvMount",
    tab: "articulos",
    label: "Proveedor",
    param: "proveedor",
    source: "remote",
    field: "proveedor",
    allLabel: "Sin filtro de proveedor",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar proveedor",
    emptyLabel: "Seleccionar proveedor",
    emptyMeta: "Opcional",
  },
  compProv: {
    mountId: "compProvMount",
    tab: "competidor",
    label: "Proveedor / Competidor",
    param: "proveedor",
    source: "remote",
    field: "proveedor",
    allLabel: "Sin filtro de proveedor",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar proveedor",
    required: true,
    selectionMode: "single",
    showSelectAll: false,
    autoApply: true,
    defaultAll: false,
    emptyLabel: "Seleccione un competidor",
    emptyMeta: "Filtro principal para habilitar el reporte",
  },
  compRubro: {
    mountId: "compRubroMount",
    tab: "competidor",
    label: "Rubro",
    param: "rubro",
    source: "static",
    optionsKey: "rubros",
    allLabel: "Sin filtro de rubro",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar rubro",
    emptyLabel: "Seleccionar rubro",
    emptyMeta: "Opcional",
  },
  compDesc: {
    mountId: "compDescMount",
    tab: "competidor",
    label: "Articulo",
    param: "descripcion",
    source: "remote",
    field: "descripcion",
    allLabel: "Sin filtro de articulo",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar articulo",
    emptyLabel: "Seleccionar articulo",
    emptyMeta: "Opcional",
  },
  cliComp: {
    mountId: "cliCompMount",
    tab: "cliente",
    label: "Comprador / Organismo",
    param: "comprador",
    source: "remote",
    field: "comprador",
    allLabel: "Sin filtro de organismo",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar organismo",
    required: true,
    selectionMode: "single",
    showSelectAll: false,
    autoApply: true,
    defaultAll: false,
    emptyLabel: "Seleccione un organismo",
    emptyMeta: "Filtro principal para habilitar el reporte",
  },
  cliPlat: {
    mountId: "cliPlatMount",
    tab: "cliente",
    label: "Plataforma",
    param: "plataforma",
    source: "static",
    optionsKey: "plataformas",
    allLabel: "Sin filtro de plataforma",
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
    allLabel: "Sin filtro de provincia",
    allMeta: "Sin restriccion",
    searchPlaceholder: "Buscar provincia",
    emptyLabel: "Seleccionar provincia",
    emptyMeta: "Opcional",
  },
};

const ARTICLE_FILTER_IDS = ["artDesc", "artMarca", "artProv"];
const ARTICLE_DEPENDENT_FILTER_IDS = ["artMarca", "artProv"];

function pfDefaultFilterAllState(config) {
  return config.selectionMode === "single" ? false : config.defaultAll === true;
}

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

function pfKpiMedianaPeso(n) {
  if (n == null) return "-";
  const num = parseFloat(n);
  if (Number.isNaN(num)) return "-";
  if (Math.abs(num) < 100000) return "$ " + pfFmt(num, 0);
  return pfKpiPeso(num);
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
  const defaultAll = pfDefaultFilterAllState(config);
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
    lastLoadFailed: false,
  };
}

function pfIsSingleSelectFilter(filterId) {
  return PF.multiFilters[filterId]?.config.selectionMode === "single";
}

function pfShouldShowFilterAllOption(_config) {
  return false;
}

function pfRenderFilterControls() {
  Object.entries(FILTER_CONFIG).forEach(([filterId, config]) => {
    pfCreateFilterState(filterId, config);
    const mount = document.getElementById(config.mountId);
    if (!mount) return;
    const requiredBadgeHtml = config.required
      ? '<span class="pf-multi__label-badge">Obligatorio</span>'
      : "";

    const allOptionHtml = pfShouldShowFilterAllOption(config)
      ? `
            <label class="pf-multi__all">
              <input type="checkbox" class="pf-multi__all-check" data-filter-id="${filterId}">
              <span>Todos</span>
            </label>`
      : "";

    mount.innerHTML = `
      <div class="pf-multi" data-filter-id="${filterId}">
        <label class="pf-multi__label" for="${filterId}Trigger">${pfEsc(config.label)}${requiredBadgeHtml}</label>
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
            ${allOptionHtml}
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

  state[valuesKey] = pfUnique(state[valuesKey] || []);

  if (state.config.selectionMode === "single") {
    state[allKey] = false;
    state[valuesKey] = state[valuesKey].length ? [state[valuesKey][state[valuesKey].length - 1]] : [];
    return;
  }

  if (state[allKey]) {
    state[valuesKey] = [];
    return;
  }

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
  const allSelected = !locked && !pfIsSingleSelectFilter(filterId) && state.appliedAll;
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
    metaText = state.config.selectionMode === "single" ? "Seleccion unica" : "1 seleccionado";
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

function pfBuildFilterContextParams(filterId, mode = "facet") {
  const state = PF.multiFilters[filterId];
  const params = pfGlobalParams();
  if (!state || state.config.tab !== "articulos") return params;

  const includeIds = mode === "article-base"
    ? ["artDesc"]
    : ARTICLE_FILTER_IDS.filter((candidateId) => candidateId !== filterId);

  includeIds.forEach((candidateId) => {
    pfSetMultiParam(params, candidateId);
  });
  return params;
}

function pfClampFilterSelectionToUniverse(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state || state.lastLoadFailed) return false;

  const universe = pfGetFilterUniverseValues(filterId);
  const validValues = new Set(universe);
  const nextApplied = (state.applied || []).filter((value) => validValues.has(value));
  const nextDraft = (state.draft || []).filter((value) => validValues.has(value));
  const nextAppliedAll = pfIsSingleSelectFilter(filterId)
    ? false
    : (nextApplied.length ? false : pfDefaultFilterAllState(state.config));
  const nextDraftAll = pfIsSingleSelectFilter(filterId)
    ? false
    : (nextDraft.length ? false : pfDefaultFilterAllState(state.config));

  const changed = nextApplied.length !== (state.applied || []).length
    || nextDraft.length !== (state.draft || []).length
    || nextAppliedAll !== state.appliedAll
    || nextDraftAll !== state.draftAll;

  state.applied = nextApplied;
  state.draft = nextDraft;
  state.appliedAll = nextAppliedAll;
  state.draftAll = nextDraftAll;
  pfNormalizeFilterSelection(filterId, "applied");
  pfNormalizeFilterSelection(filterId, "draft");
  pfUpdateFilterTrigger(filterId);

  if (document.getElementById(`${filterId}Panel`)?.classList.contains("is-open")) {
    pfRenderFilterOptions(filterId);
  }

  return changed;
}

async function pfEnsureFilterOptions(filterId, query = "", options = {}) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  const contextMode = options.contextMode || "facet";
  const showLoading = options.showLoading !== false;
  state.query = query;
  state.loading = showLoading;
  if (showLoading) pfRenderFilterOptions(filterId);

  if (state.config.source === "static") {
    const source = PF.filterOptions[state.config.optionsKey] || [];
    state.allOptions = source;
    state.options = source.filter((value) => pfFilterMatches(value, query));
    state.loading = false;
    state.loaded = true;
    state.lastLoadFailed = false;
    pfRenderFilterOptions(filterId);
    return state.options;
  }

  const params = new URLSearchParams({
    campo: state.config.field,
    q: query.trim(),
    limit: query.trim() ? "250" : "5000",
  });
  const contextParams = pfBuildFilterContextParams(filterId, contextMode);
  contextParams.forEach((value, key) => params.set(key, value));

  const cacheKey = params.toString();
  if (state.cache.has(cacheKey)) {
    state.options = state.cache.get(cacheKey) || [];
    if (!query.trim()) state.allOptions = [...state.options];
    state.loading = false;
    state.loaded = true;
    state.lastLoadFailed = false;
    pfRenderFilterOptions(filterId);
    return state.options;
  }

  const requestSeq = ++state.requestSeq;
  try {
    const data = await pfFetch(`${BASE}/filtros/search?${params}`);
    if (requestSeq !== state.requestSeq) return;
    state.cache.set(cacheKey, data);
    state.options = data;
    if (!query.trim()) state.allOptions = [...data];
    state.loaded = true;
    state.lastLoadFailed = false;
  } catch (err) {
    if (requestSeq !== state.requestSeq) return;
    state.options = [];
    if (!query.trim()) state.allOptions = [];
    state.loaded = true;
    state.lastLoadFailed = true;
  } finally {
    if (requestSeq === state.requestSeq) {
      state.loading = false;
      pfRenderFilterOptions(filterId);
    }
  }

  return state.options;
}

function pfRenderFilterOptions(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  const allCheck = document.querySelector(`.pf-multi__all-check[data-filter-id="${filterId}"]`);
  const list = document.getElementById(`${filterId}Options`);
  const inputType = state.config.selectionMode === "single" ? "radio" : "checkbox";
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
          type="${inputType}"
          class="pf-multi__option-check"
          data-filter-id="${filterId}"
          name="${filterId}Choice"
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

  if (state.config.selectionMode === "single") {
    state.draftAll = false;
    state.draft = checked ? [value] : [];
    pfNormalizeFilterSelection(filterId, "draft");
    pfRenderFilterOptions(filterId);
    if (state.config.autoApply && checked) {
      pfApplyFilterSelection(filterId);
    }
    return;
  }

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
  state.draftAll = pfDefaultFilterAllState(state.config);
  state.query = "";
  const search = document.getElementById(`${filterId}Search`);
  if (search) search.value = "";
  pfEnsureFilterOptions(filterId, "");
}

async function pfApplyFilterSelection(filterId) {
  const state = PF.multiFilters[filterId];
  if (!state) return;

  if (!pfIsFilterLocked(filterId)) {
    state.applied = [...state.draft];
    state.appliedAll = state.draftAll || (!state.draft.length && pfDefaultFilterAllState(state.config));
    pfNormalizeFilterSelection(filterId, "applied");
  }

  pfCloseFilterPanel(filterId);
  pfUpdateFilterTrigger(filterId);

  if (state.config.tab === "articulos") {
    const contextMode = filterId === "artDesc" ? "article-base" : "facet";
    const targets = filterId === "artDesc"
      ? ARTICLE_DEPENDENT_FILTER_IDS
      : ARTICLE_DEPENDENT_FILTER_IDS.filter((candidateId) => candidateId !== filterId);
    // Limpiar cache de opciones de los filtros dependientes para forzar recarga con el nuevo contexto
    targets.forEach((targetId) => {
      const targetState = PF.multiFilters[targetId];
      if (targetState) targetState.cache.clear();
    });
    const changes = await Promise.all(targets.map(async (targetId) => {
      await pfEnsureFilterOptions(targetId, "", { contextMode, showLoading: false });
      return pfClampFilterSelectionToUniverse(targetId);
    }));
    if (changes.some(Boolean)) {
      pfSetArtSelectionGate();
    }
  }

  if (state.config.tab === PF.activeTab) {
    await pfLoadCurrentTab();
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

// ── Detección dinámica de rango de fechas real ──────────────────────────────

function pfGetGlobalRangeParams() {
  const p = new URLSearchParams();
  if (PF.globalPlataforma) p.set("plataforma", PF.globalPlataforma);
  return p;
}

function pfGetArtRangeParams() {
  const p = new URLSearchParams();
  if (PF.globalPlataforma) p.set("plataforma", PF.globalPlataforma);
  pfSetMultiParam(p, "artDesc");
  pfSetMultiParam(p, "artMarca");
  pfSetMultiParam(p, "artProv");
  return p;
}

function pfGetCompRangeParams() {
  const p = new URLSearchParams();
  if (PF.globalPlataforma) p.set("plataforma", PF.globalPlataforma);
  pfSetMultiParam(p, "compProv");
  pfSetMultiParam(p, "compRubro");
  pfSetMultiParam(p, "compDesc");
  return p;
}

function pfGetCliRangeParams() {
  const p = new URLSearchParams();
  if (PF.globalPlataforma) p.set("plataforma", PF.globalPlataforma);
  pfSetMultiParam(p, "cliComp");
  pfSetMultiParam(p, "cliProv");
  return p;
}

async function pfLoadDateRange(rangeParams) {
  const elFrom = document.getElementById("gFechaDesde");
  const elTo   = document.getElementById("gFechaHasta");
  if (!elFrom || !elTo) return;

  try {
    const data = await pfFetch(`${BASE}/filtros/rango-fechas?${rangeParams}`);
    const { fecha_min, fecha_max } = data;
    if (!fecha_min || !fecha_max) return;

    // Siempre restringir el datepicker al rango real disponible
    elFrom.min = fecha_min;
    elFrom.max = fecha_max;
    elTo.min   = fecha_min;
    elTo.max   = fecha_max;

    // Solo auto-poblar si el usuario no fijó un rango personalizado
    if (!PF.dateUserSet) {
      if (elFrom.value && (elFrom.value < fecha_min || elFrom.value > fecha_max)) {
        elFrom.value = "";
      }
      if (elTo.value && (elTo.value < fecha_min || elTo.value > fecha_max)) {
        elTo.value = "";
      }
      PF.globalFechaDesde = elFrom.value || "";
      PF.globalFechaHasta = elTo.value || "";
      pfUpdateDateSummary();
    }
  } catch {
    // no-op: la detección de rango es no-crítica
  }
}

function pfSetMultiParam(params, filterId, paramName = FILTER_CONFIG[filterId]?.param) {
  if (!paramName) return;
  const state = PF.multiFilters[filterId];
  if (state?.appliedAll) return;
  const values = pfGetFilterAppliedValues(filterId);
  // Use "||" as separator to avoid splitting on commas within values (e.g. Spanish decimal "0,6 ml")
  if (values.length) params.set(paramName, values.join("||"));
}

function pfBeginRequestGuard(key, cleanup) {
  const nextToken = (PF.requestGuards[key] || 0) + 1;
  PF.requestGuards[key] = nextToken;
  if (typeof cleanup === "function") cleanup();
  return nextToken;
}

function pfIsRequestCurrent(key, token) {
  return PF.requestGuards[key] === token;
}

function pfDestroyChart(id) {
  if (PF.charts[id]) {
    try {
      PF.charts[id].destroy();
    } catch (err) {
      console.warn("[PF] No se pudo destruir el chart", id, err);
    }
    delete PF.charts[id];
  }
  const el = document.getElementById(id);
  if (el) el.innerHTML = "";
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

function pfFilterSeriesByValidPoints(series = [], minPoints = 1) {
  return (series || []).filter((serie) => {
    const validPoints = (serie?.data || []).filter((value) => (
      value !== null
      && value !== undefined
      && Number.isFinite(Number(value))
      && Number(value) > 0
    ));
    return validPoints.length >= minPoints;
  });
}

function pfIsValidTooltipValue(value) {
  if (value === null || value === undefined || value === "" || value === "-" || value === "—") return false;
  const n = Number(value);
  return !Number.isNaN(n) && n > 0;
}

// Returns a custom ApexCharts tooltip function for shared multi-series money charts.
// Filters out series with no valid value at the hovered data point.
function pfMakeSharedMoneyTooltip() {
  return function({ series, dataPointIndex, w }) {
    const label = (w.globals.categoryLabels && w.globals.categoryLabels[dataPointIndex])
      || (w.globals.labels && w.globals.labels[dataPointIndex])
      || "";

    const items = series.reduce((acc, serieData, i) => {
      const val = serieData[dataPointIndex];
      if (!pfIsValidTooltipValue(val)) return acc;
      const name = (w.globals.seriesNames && w.globals.seriesNames[i]) || "";
      const color = (w.globals.colors && w.globals.colors[i]) || "#06486f";
      acc.push(
        `<div style="display:flex;align-items:center;padding:2px 10px;font-size:11px">` +
        `<span style="width:8px;height:8px;border-radius:50%;background:${color};` +
        `display:inline-block;margin-right:6px;flex-shrink:0"></span>` +
        `<span style="opacity:.75;margin-right:4px">${pfEsc(name)}:</span>` +
        `<strong>${pfPeso(val)}</strong>` +
        `</div>`
      );
      return acc;
    }, []);

    if (!items.length) return "";

    return (
      `<div style="padding:5px 0;min-width:140px">` +
      (label ? `<div style="padding:4px 10px 3px;font-size:11px;font-weight:600;border-bottom:1px solid rgba(148,163,184,.2)">${pfEsc(label)}</div>` : "") +
      items.join("") +
      `</div>`
    );
  };
}

function pfChartBase(overrides = {}) {
  const base = {
    chart: {
      background: "transparent",
      fontFamily: "Outfit, system-ui, sans-serif",
      toolbar: { show: false },
      animations: { enabled: false },
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
  if (PF.charts[elId]) pfDestroyChart(elId);
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

function pfResetArtOutputs() {
  ["artPrecioChart", "artCantidadChart", "artMarcaChart", "artProvChart"].forEach(pfDestroyChart);
  const row = document.getElementById("artKpiRow");
  const tbody = document.getElementById("artProvTbody");
  const chip = document.getElementById("artRubroChip");
  if (row) row.innerHTML = "";
  if (tbody) {
    tbody.innerHTML = pfTableEmpty(
      "bi-info-circle",
      "Selecciona primero un articulo para habilitar el detalle del reporte.",
      7
    );
  }
  if (chip) { chip.hidden = true; chip.innerHTML = ""; }
}

function pfResetCompOutputs() {
  ["compEvolChart", "compRubroChart", "compArtChart", "compMarcaChart"].forEach(pfDestroyChart);
  const row = document.getElementById("compKpiRow");
  const tbody = document.getElementById("compPosTbody");
  if (row) row.innerHTML = "";
  if (tbody) {
    tbody.innerHTML = pfTableEmpty(
      "bi-building",
      "Selecciona primero un competidor para habilitar su posicionamiento por articulo.",
      7
    );
  }
}

function pfResetCliOutputs() {
  ["cliEvolChart", "cliRubroChart", "cliProvChart", "cliArtChart"].forEach(pfDestroyChart);
  const row = document.getElementById("cliKpiRow");
  const tbody = document.getElementById("cliArtTbody");
  if (row) row.innerHTML = "";
  if (tbody) {
    tbody.innerHTML = pfTableEmpty(
      "bi-hospital",
      "Selecciona primero un organismo para habilitar el detalle del perfil de compra.",
      6
    );
  }
}

const TAB_SELECTION_CONFIG = {
  articulos: {
    requiredFilters: ["artDesc"],
    emptyId: "artEmptySelection",
    dashboardId: "artDashboard",
    resetOutputs: pfResetArtOutputs,
  },
  competidor: {
    requiredFilters: ["compProv"],
    emptyId: "compEmptySelection",
    dashboardId: "compDashboard",
    resetOutputs: pfResetCompOutputs,
  },
  cliente: {
    requiredFilters: ["cliComp"],
    emptyId: "cliEmptySelection",
    dashboardId: "cliDashboard",
    resetOutputs: pfResetCliOutputs,
  },
};

function pfTabHasRequiredSelection(tab) {
  const config = TAB_SELECTION_CONFIG[tab];
  if (!config) return true;
  return config.requiredFilters.every((filterId) => pfGetFilterAppliedValues(filterId).length > 0);
}

function pfSetTabSelectionGate(tab) {
  const config = TAB_SELECTION_CONFIG[tab];
  if (!config) return true;

  const hasSelection = pfTabHasRequiredSelection(tab);
  const empty = document.getElementById(config.emptyId);
  const dashboard = document.getElementById(config.dashboardId);

  if (empty) empty.hidden = hasSelection;
  if (dashboard) dashboard.hidden = !hasSelection;
  if (!hasSelection && typeof config.resetOutputs === "function") {
    config.resetOutputs();
  }

  return hasSelection;
}

function pfSetAllSelectionGates() {
  Object.keys(TAB_SELECTION_CONFIG).forEach((tab) => pfSetTabSelectionGate(tab));
}

function pfSetArtSelectionGate() {
  return pfSetTabSelectionGate("articulos");
}

function pfSyncArticulosCopy() {
  const emptyEyebrow = document.querySelector("#artEmptySelection .pf-required-state__eyebrow");
  if (emptyEyebrow) emptyEyebrow.textContent = "Esperando contexto";

  const emptyTitle = document.querySelector("#artEmptySelection h3");
  if (emptyTitle) emptyTitle.textContent = "Selecciona un articulo para visualizar el reporte.";

  const emptyText = document.querySelector("#artEmptySelection p");
  if (emptyText) {
    emptyText.textContent = "Una vez definido el articulo, vas a poder combinar marcas, proveedores y fechas para profundizar el analisis.";
  }

  const tipItems = document.querySelectorAll("#artEmptySelection .pf-required-state__tips span");
  if (tipItems[0]) tipItems[0].innerHTML = '<i class="bi bi-check2-circle"></i> Usa el filtro principal con seleccion unica';
  if (tipItems[1]) tipItems[1].innerHTML = '<i class="bi bi-check2-circle"></i> La cascada actualiza marca y proveedor automaticamente';
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
  // El usuario fijó un rango explícito; no lo pisar con auto-detección
  PF.dateUserSet = !!(PF.globalFechaDesde || PF.globalFechaHasta);
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
  PF.dateUserSet = false;

  Object.keys(PF.multiFilters).forEach((filterId) => {
    const state = PF.multiFilters[filterId];
    if (!state) return;
    if (!pfIsFilterLocked(filterId)) {
      state.applied = [];
      state.appliedAll = pfDefaultFilterAllState(state.config);
      state.draft = [];
      state.draftAll = pfDefaultFilterAllState(state.config);
    }
    state.query = "";
    const search = document.getElementById(`${filterId}Search`);
    if (search) search.value = "";
    pfUpdateFilterTrigger(filterId);
  });

  pfSyncLockedPlatformUI();
  pfCloseAllFilterPanels();
  pfUpdateDateSummary();
  pfSetAllSelectionGates();
  // Restaurar rango global real y luego recargar
  pfLoadDateRange(pfGetGlobalRangeParams()).then(() => pfLoadCurrentTab());
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
  return params;
}

async function pfLoadArticulos() {
  if (!pfSetTabSelectionGate("articulos")) return;

  // Detectar rango real antes de renderizar (si el usuario no fijó fechas)
  if (!PF.dateUserSet) await pfLoadDateRange(pfGetArtRangeParams());

  const params = pfGetArtParams();
  await Promise.all([
    pfLoadArtKpis(params),
    pfLoadArtEvolucion(params),
    pfLoadArtPorMarca(params),
    pfLoadArtPorProveedor(params),
  ]);
}

function pfUpdateArtRubroChip(rubroPrincipal, data) {
  const chip = document.getElementById("artRubroChip");
  if (!chip) return;
  if (!rubroPrincipal) {
    chip.hidden = true;
    chip.innerHTML = "";
    return;
  }
  const tooltip = (data.rubros_lista || []).join(" | ") || rubroPrincipal;
  const multiLabel = data.rubros_multiples
    ? ` <span class="pf-rubro-chip__multi">(${pfFmt(data.rubros_detectados)} rubros)</span>`
    : "";
  chip.hidden = false;
  chip.innerHTML = `<span class="pf-rubro-chip" title="${pfEscAttr(tooltip)}"><i class="bi bi-diagram-3"></i> ${pfEsc(pfTrunc(rubroPrincipal, 40))}${multiLabel}</span>`;
}

async function pfLoadArtKpis(params) {
  const row = document.getElementById("artKpiRow");
  if (!row) return;
  const requestToken = pfBeginRequestGuard("art:kpis");

  try {
    const data = await pfFetch(`${BASE}/articulos/kpis?${params}`);
    if (!pfIsRequestCurrent("art:kpis", requestToken)) return;
    pfUpdateArtRubroChip(data.rubro_principal || "", data);

    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto adjudicado",
        value: pfKpiPeso(data.total_adjudicado),
        rawValue: pfPeso(data.total_adjudicado),
        sub: "Total adjudicado (pos. 1)",
        icon: "bi-cash-stack",
      }),
      pfKpiCard({
        tone: "primary",
        label: "Mediana precio",
        value: pfKpiMedianaPeso(data.mediana_precio),
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
        label: "Marcas",
        value: pfFmt(data.marcas_distintas),
        sub: `${pfFmt(data.procesos)} procesos analizados`,
        icon: "bi-tags",
      }),
    ].join("");
  } catch (err) {
    if (!pfIsRequestCurrent("art:kpis", requestToken)) return;
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadArtEvolucion(params) {
  const requestToken = pfBeginRequestGuard("art:evol", () => {
    pfDestroyChart("artPrecioChart");
    pfDestroyChart("artCantidadChart");
  });

  try {
    const [data, dataMarca] = await Promise.all([
      pfFetch(`${BASE}/articulos/evolucion?${params}`),
      pfFetch(`${BASE}/articulos/evolucion-marca?${params}`),
    ]);
    if (!pfIsRequestCurrent("art:evol", requestToken)) return;

    if (!data.length) {
      pfRenderEmpty("artPrecioChart", "Sin datos para el periodo seleccionado.", "bi-graph-up");
      pfRenderEmpty("artCantidadChart", "Sin datos para el periodo seleccionado.", "bi-bar-chart");
      return;
    }

    // artPrecioChart: una línea por marca consolidada (mediana de precio, sin duplicados por proveedor)
    if (dataMarca.length) {
      // Labels ordenados cronológicamente usando metadatos year+month del backend
      const labelMeta = new Map();
      dataMarca.forEach((d) => {
        if (!labelMeta.has(d.month_label)) {
          labelMeta.set(d.month_label, d.year * 100 + d.month);
        }
      });
      const labelsOrdered = Array.from(labelMeta.keys())
        .sort((a, b) => labelMeta.get(a) - labelMeta.get(b));

      // Una serie por marca (ya consolidada en backend con mediana).
      const marcasUnicas = [];
      const seenMarcas = new Set();
      dataMarca.forEach((d) => {
        if (!seenMarcas.has(d.marca)) {
          seenMarcas.add(d.marca);
          marcasUnicas.push(d.marca);
        }
      });
      marcasUnicas.sort((a, b) => a.localeCompare(b));

      // Índice por (month_label, marca) para lookup O(1)
      const idx = new Map();
      dataMarca.forEach((d) => idx.set(`${d.month_label}|||${d.marca}`, d.mediana_precio));

      const seriesPrecio = marcasUnicas.map((marca) => ({
        name: pfTrunc(marca, 20),
        data: labelsOrdered.map((label) => idx.get(`${label}|||${marca}`) ?? null),
      }));

      // Solo incluir series que tengan al menos un punto válido (> 0 y no nulo)
      const seriesFiltradas = pfFilterSeriesByValidPoints(seriesPrecio, 2);

      if (!pfIsRequestCurrent("art:evol", requestToken)) return;
      if (!seriesFiltradas.length) {
        pfRenderEmpty("artPrecioChart", "Sin marcas con precio valido para el periodo seleccionado.", "bi-graph-up");
      } else {
        pfDestroyChart("artPrecioChart");
        PF.charts.artPrecioChart = new ApexCharts(
          document.getElementById("artPrecioChart"),
          pfChartBase({
            chart: { type: "line", height: 256 },
            series: seriesFiltradas,
            xaxis: { categories: labelsOrdered, labels: { rotate: -28 } },
            yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
            stroke: { width: 2, curve: "smooth" },
            markers: { size: 3, strokeWidth: 0, hover: { sizeOffset: 2 } },
            legend: { show: seriesFiltradas.length > 1 },
            tooltip: {
              shared: true,
              intersect: false,
              custom: pfMakeSharedMoneyTooltip(),
            },
          })
        );
        PF.charts.artPrecioChart.render();
      }
    } else {
      pfRenderEmpty("artPrecioChart", "Sin datos para el periodo seleccionado.", "bi-graph-up");
    }

    // artCantidadChart: sin cambios
    const labels = data.map((d) => d.month_label);
    if (!pfIsRequestCurrent("art:evol", requestToken)) return;
    pfDestroyChart("artCantidadChart");
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
    if (!pfIsRequestCurrent("art:evol", requestToken)) return;
    pfRenderEmpty("artPrecioChart", "Error al cargar la evolucion.", "bi-graph-up");
    pfRenderEmpty("artCantidadChart", "Error al cargar las cantidades.", "bi-bar-chart");
  }
}

async function pfLoadArtPorMarca(params) {
  const requestToken = pfBeginRequestGuard("art:marca", () => pfDestroyChart("artMarcaChart"));

  try {
    const data = await pfFetch(`${BASE}/articulos/por-marca?${params}`);
    if (!pfIsRequestCurrent("art:marca", requestToken)) return;
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
    const seriesFiltradas = pfFilterSeriesByValidPoints(seriesData, 1);

    if (!pfIsRequestCurrent("art:marca", requestToken)) return;
    if (!seriesFiltradas.length) {
      pfRenderEmpty("artMarcaChart", "Sin marcas con precio ganador valido para el filtro actual.", "bi-tags");
      return;
    }
    pfDestroyChart("artMarcaChart");
    PF.charts.artMarcaChart = new ApexCharts(
      document.getElementById("artMarcaChart"),
      pfChartBase({
        chart: { type: "bar", height: 256, stacked: true },
        series: seriesFiltradas,
        xaxis: { categories: categories },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        plotOptions: {
          bar: { columnWidth: "60%" },
        },
        legend: { show: seriesFiltradas.length > 1 },
        tooltip: {
          shared: true,
          intersect: false,
          custom: pfMakeSharedMoneyTooltip(),
        },
      })
    );
    PF.charts.artMarcaChart.render();
  } catch (err) {
    if (!pfIsRequestCurrent("art:marca", requestToken)) return;
    pfRenderEmpty("artMarcaChart", "Error al cargar el benchmark por marca.", "bi-tags");
  }
}

async function pfLoadArtPorProveedor(params) {
  const requestToken = pfBeginRequestGuard("art:proveedor", () => pfDestroyChart("artProvChart"));
  const tbody = document.getElementById("artProvTbody");

  try {
    const data = await pfFetch(`${BASE}/articulos/por-proveedor?${params}`);
    if (!pfIsRequestCurrent("art:proveedor", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("artProvChart", "Sin proveedores para este articulo.", "bi-buildings");
      if (tbody) tbody.innerHTML = pfTableEmpty("bi-search", "Sin proveedores para este articulo.", 7);
      return;
    }

    const top = data.slice(0, 15);
    const treemapData = top.map((d) => ({ x: pfTrunc(d.proveedor, 28), y: d.total_adjudicado }));
    if (!pfIsRequestCurrent("art:proveedor", requestToken)) return;
    pfDestroyChart("artProvChart");
    PF.charts.artProvChart = new ApexCharts(
      document.getElementById("artProvChart"),
      pfChartBase({
        chart: { type: "treemap", height: 310 },
        series: [{ name: "Monto adjudicado", data: treemapData }],
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
              `<strong>${pfEsc(d.proveedor)}</strong><br>` +
              `Monto adjudicado: <strong>${pfPeso(d.total_adjudicado)}</strong><br>` +
              `<span style="font-size:11px;opacity:.75">` +
              `${pfFmt(d.veces_ganado)} adjudicacion${d.veces_ganado !== 1 ? "es" : ""} · ${pfFmt(d.procesos)} proceso${d.procesos !== 1 ? "s" : ""}` +
              `</span>` +
              `</div>`
            );
          },
        },
      })
    );
    PF.charts.artProvChart.render();

    if (tbody) {
      tbody.innerHTML = data.map((d) => {
        const provId = encodeURIComponent(d.proveedor);
        const efct = d.count > 0 ? pfFmt(d.efectividad, 1) + "%" : "-";
        const hasUltimo = d.ultimo_precio > 0;
        return `
          <tr class="pf-prov-row" data-proveedor="${provId}">
            <td>${pfCellText(d.proveedor, 48)}</td>
            <td class="num">${d.mediana_precio > 0 ? pfPeso(d.mediana_precio) : "-"}</td>
            <td class="num">${pfFmt(d.veces_ganado)}</td>
            <td class="num">${efct}</td>
            <td class="num">${pfFmt(d.count)}</td>
            <td class="num">${pfPeso(d.total_adjudicado)}</td>
            <td class="num pf-hist-cell">
              ${hasUltimo ? `<span>${pfPeso(d.ultimo_precio)}</span>` : `<span class="pf-text-muted-sm">—</span>`}
              <button class="pf-hist-btn" title="Ver historial del período"
                onclick="pfToggleHistorico(this, decodeURIComponent('${provId}'))">
                <i class="bi bi-chevron-down"></i>
              </button>
            </td>
          </tr>`;
      }).join("");
    }
  } catch (err) {
    if (!pfIsRequestCurrent("art:proveedor", requestToken)) return;
    pfRenderEmpty("artProvChart", "Error al cargar proveedores.", "bi-buildings");
    if (tbody) tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar el detalle de proveedores.", 7);
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
          const posC = r.posicion == null ? "posn" : r.posicion === 1 ? "pos1" : r.posicion === 2 ? "pos2" : "posn";
          const posLabel = r.posicion != null ? `#${r.posicion}` : "-";
          const ref = [r.proceso, r.renglon && r.renglon !== "-" ? `R.${r.renglon}` : ""].filter(Boolean).join(" - ");
          return `<tr>
            <td>${r.fecha ?? "-"}</td>
            <td>${pfCellText(ref || "-", 42)}</td>
            <td class="num">${pfPeso(r.precio)}</td>
            <td>${r.marca}</td>
            <td class="num"><span class="pf-badge ${posC}">${posLabel}</span></td>
          </tr>`;
        }).join("")
      : `<tr><td colspan="5" style="text-align:center;padding:.6rem;color:var(--pf-text-muted);font-size:.77rem">Sin registros en el último año para los filtros actuales</td></tr>`;

    const detailRow = document.createElement("tr");
    detailRow.id = histId;
    detailRow.className = "pf-hist-row";
    detailRow.innerHTML = `
      <td colspan="7">
        <div class="pf-hist-detail">
          <div class="pf-hist-header"><i class="bi bi-clock-history"></i> Historial del período — ${pfTrunc(proveedor, 60)}</div>
          <table class="pf-hist-table">
            <thead><tr>
              <th>Fecha</th>
              <th>Proceso / renglon</th>
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
  if (!pfSetTabSelectionGate("competidor")) return;
  if (!PF.dateUserSet) await pfLoadDateRange(pfGetCompRangeParams());

  const params = pfGetCompParams();
  await Promise.all([
    pfLoadCompKpis(params),
    pfLoadCompEvolucion(params),
    pfLoadCompRubros(params),
    pfLoadCompTopArt(params),
    pfLoadCompProductosCompetitivos(params),
    pfLoadCompPosiciones(params),
  ]);
}

async function pfLoadCompKpis(params) {
  const row = document.getElementById("compKpiRow");
  if (!row) return;
  const requestToken = pfBeginRequestGuard("comp:kpis");

  try {
    const data = await pfFetch(`${BASE}/competidor/kpis?${params}`);
    if (!pfIsRequestCurrent("comp:kpis", requestToken)) return;
    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto adjudicado",
        value: pfKpiPeso(data.total_adjudicado),
        rawValue: pfPeso(data.total_adjudicado),
        sub: "Adjudicado al competidor (pos. 1)",
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
        label: "Articulos",
        value: pfKpiNum(data.descripciones_cotizadas),
        rawValue: pfFmt(data.descripciones_cotizadas),
        sub: "Variedad de articulos",
        icon: "bi-box-seam",
      }),
      pfKpiCard({
        tone: "success",
        label: "Rubros",
        value: pfFmt(data.rubros_cubiertos),
        sub: "Cobertura del mix",
        icon: "bi-grid-1x2",
      }),
      pfKpiCard({
        tone: "success",
        label: "Marcas",
        value: pfFmt(data.marcas_utilizadas),
        sub: "Con presencia en la muestra",
        icon: "bi-tags",
      }),
    ].join("");
  } catch (err) {
    if (!pfIsRequestCurrent("comp:kpis", requestToken)) return;
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadCompEvolucion(params) {
  const requestToken = pfBeginRequestGuard("comp:evol", () => pfDestroyChart("compEvolChart"));

  try {
    const data = await pfFetch(`${BASE}/competidor/evolucion?${params}`);
    if (!pfIsRequestCurrent("comp:evol", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("compEvolChart", "Sin datos para el competidor seleccionado.", "bi-graph-up-arrow");
      return;
    }

    if (!pfIsRequestCurrent("comp:evol", requestToken)) return;
    pfDestroyChart("compEvolChart");
    PF.charts.compEvolChart = new ApexCharts(
      document.getElementById("compEvolChart"),
      pfChartBase({
        chart: { type: "line", height: 256 },
        series: [{ name: "Monto adjudicado", data: data.map((d) => d.monto_total) }],
        xaxis: { categories: data.map((d) => d.month_label), labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        stroke: { width: 2, curve: "smooth" },
        markers: { size: 3, strokeWidth: 0, hover: { sizeOffset: 2 } },
        legend: { show: false },
        colors: [COLORS.brand500],
        tooltip: {
          shared: true,
          intersect: false,
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.compEvolChart.render();
  } catch (err) {
    if (!pfIsRequestCurrent("comp:evol", requestToken)) return;
    pfRenderEmpty("compEvolChart", "Error al cargar la evolucion del competidor.", "bi-graph-up-arrow");
  }
}

async function pfLoadCompRubros(params) {
  const requestToken = pfBeginRequestGuard("comp:rubros", () => pfDestroyChart("compRubroChart"));

  try {
    const data = await pfFetch(`${BASE}/competidor/rubros?${params}`);
    if (!pfIsRequestCurrent("comp:rubros", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("compRubroChart", "Sin datos de rubros.", "bi-pie-chart");
      return;
    }

    if (!pfIsRequestCurrent("comp:rubros", requestToken)) return;
    pfDestroyChart("compRubroChart");
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
    if (!pfIsRequestCurrent("comp:rubros", requestToken)) return;
    pfRenderEmpty("compRubroChart", "Error al cargar los rubros.", "bi-pie-chart");
  }
}

async function pfLoadCompTopArt(params) {
  const requestToken = pfBeginRequestGuard("comp:top-art", () => pfDestroyChart("compArtChart"));

  try {
    const data = await pfFetch(`${BASE}/competidor/top-articulos?${params}`);
    if (!pfIsRequestCurrent("comp:top-art", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("compArtChart", "Sin articulos para este filtro.", "bi-bar-chart");
      return;
    }

    const top = data.slice(0, 10);
    if (!pfIsRequestCurrent("comp:top-art", requestToken)) return;
    pfDestroyChart("compArtChart");
    PF.charts.compArtChart = new ApexCharts(
      document.getElementById("compArtChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Monto adjudicado", data: top.map((d) => d.monto_total) }],
        xaxis: {
          categories: top.map((d) => pfTrunc(d.descripcion, 36)),
          labels: { formatter: pfAxisFmtMoney },
        },
        yaxis: { labels: { formatter: pfAxisFmtMoney, maxWidth: 160 } },
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
    if (!pfIsRequestCurrent("comp:top-art", requestToken)) return;
    pfRenderEmpty("compArtChart", "Error al cargar articulos.", "bi-bar-chart");
  }
}

async function pfLoadCompProductosCompetitivos(params) {
  const requestToken = pfBeginRequestGuard("comp:productos", () => pfDestroyChart("compMarcaChart"));

  try {
    const data = await pfFetch(`${BASE}/competidor/productos-competitivos?${params}`);
    if (!pfIsRequestCurrent("comp:productos", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("compMarcaChart", "Sin adjudicaciones para este filtro.", "bi-trophy");
      return;
    }

    const top = data.slice(0, 12);
    if (!pfIsRequestCurrent("comp:productos", requestToken)) return;
    pfDestroyChart("compMarcaChart");
    PF.charts.compMarcaChart = new ApexCharts(
      document.getElementById("compMarcaChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Veces adjudicado", data: top.map((d) => d.veces_adjudicado) }],
        xaxis: {
          categories: top.map((d) => pfTrunc(d.descripcion, 36)),
          labels: { formatter: pfAxisFmtNum },
        },
        yaxis: { labels: { formatter: pfAxisFmtNum, maxWidth: 160 } },
        plotOptions: {
          bar: {
            horizontal: true,
            borderRadius: 6,
            barHeight: "68%",
          },
        },
        colors: [COLORS.warning],
        dataLabels: {
          enabled: true,
          formatter: (val) => val > 0 ? pfFmt(val) : "",
          style: { fontSize: "11px" },
        },
        tooltip: {
          custom({ dataPointIndex }) {
            const d = top[dataPointIndex];
            if (!d) return "";
            return (
              `<div class="apexcharts-tooltip-box" style="padding:10px 14px;line-height:1.6">` +
              `<strong>${pfEsc(pfTrunc(d.descripcion, 48))}</strong><br>` +
              `Adjudicaciones: <strong>${pfFmt(d.veces_adjudicado)}</strong><br>` +
              `Monto adjudicado: <strong>${pfPeso(d.monto_adjudicado)}</strong><br>` +
              `<span style="font-size:11px;opacity:.75">Sobre ${pfFmt(d.participaciones)} participaciones</span>` +
              `</div>`
            );
          },
        },
      })
    );
    PF.charts.compMarcaChart.render();
  } catch (err) {
    if (!pfIsRequestCurrent("comp:productos", requestToken)) return;
    pfRenderEmpty("compMarcaChart", "Error al cargar productos competitivos.", "bi-trophy");
  }
}

async function pfLoadCompPosiciones(params) {
  const tbody = document.getElementById("compPosTbody");
  if (!tbody) return;
  const requestToken = pfBeginRequestGuard("comp:posiciones");

  try {
    const data = await pfFetch(`${BASE}/competidor/posiciones?${params}`);
    if (!pfIsRequestCurrent("comp:posiciones", requestToken)) return;
    if (!data.length) {
      tbody.innerHTML = pfTableEmpty("bi-building", "Sin articulos para este proveedor.", 7);
      return;
    }

    tbody.innerHTML = data.map((d) => {
      const ef = d.efectividad ?? 0;
      const efClass = ef >= 50 ? "pf-efectividad--high" : ef >= 20 ? "pf-efectividad--mid" : "pf-efectividad--low";
      const descId = encodeURIComponent(d.descripcion);
      const hasMediana = d.precio_mediana > 0;
      const hasUltimo = d.ultimo_precio > 0;
      return `
        <tr>
          <td>${pfCellText(d.descripcion, 56)}</td>
          <td class="num">${hasMediana ? pfPeso(d.precio_mediana) : `<span class="pf-text-muted-sm">—</span>`}</td>
          <td class="num">${pfFmt(d.participaciones ?? d.count ?? 0)}</td>
          <td class="num">${pfFmt(d.veces_ganado ?? 0)}</td>
          <td class="num"><span class="pf-efectividad ${efClass}">${ef}%</span></td>
          <td class="num">${pfPeso(d.monto_total)}</td>
          <td class="num pf-hist-cell">
            ${hasUltimo ? `<span>${pfPeso(d.ultimo_precio)}</span>` : `<span class="pf-text-muted-sm">—</span>`}
            <button class="pf-hist-btn" title="Ver historial de precios"
              onclick="pfToggleCompPosHistorico(this, decodeURIComponent('${descId}'))">
              <i class="bi bi-chevron-down"></i>
            </button>
          </td>
        </tr>`;
    }).join("");
  } catch (err) {
    if (!pfIsRequestCurrent("comp:posiciones", requestToken)) return;
    tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar posiciones.", 7);
  }
}

async function pfToggleCompPosHistorico(btn, descripcion) {
  const row = btn.closest("tr");
  const histId = "pf-cpos-hist-" + btoa(encodeURIComponent(descripcion)).replace(/[^a-zA-Z0-9]/g, "_");
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
    const params = pfGetCompParams();
    params.set("descripcion", descripcion);
    const data = await pfFetch(`${BASE}/competidor/articulo-detalle?${params}`);

    const bodyRows = data.length
      ? data.map((r) => {
          const posC = r.posicion == null ? "posn" : r.posicion === 1 ? "pos1" : r.posicion === 2 ? "pos2" : "posn";
          const posLabel = r.posicion != null ? `#${r.posicion}` : "-";
          const ref = [r.proceso, r.renglon && r.renglon !== "-" ? `R.${r.renglon}` : ""].filter(Boolean).join(" - ");
          return `<tr>
            <td>${r.fecha ?? "-"}</td>
            <td>${pfCellText(ref || "-", 42)}</td>
            <td class="num">${pfPeso(r.precio)}</td>
            <td>${r.marca}</td>
            <td class="num"><span class="pf-badge ${posC}">${posLabel}</span></td>
          </tr>`;
        }).join("")
      : `<tr><td colspan="5" style="text-align:center;padding:.6rem;color:var(--pf-text-muted);font-size:.77rem">Sin registros en el período para los filtros actuales</td></tr>`;

    const detailRow = document.createElement("tr");
    detailRow.id = histId;
    detailRow.className = "pf-hist-row";
    detailRow.innerHTML = `
      <td colspan="7">
        <div class="pf-hist-detail">
          <div class="pf-hist-header"><i class="bi bi-clock-history"></i> Historial — ${pfEsc(pfTrunc(descripcion, 60))}</div>
          <table class="pf-hist-table">
            <thead><tr>
              <th>Fecha</th>
              <th>Proceso / renglón</th>
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

function pfGetCliParams() {
  const params = pfGlobalParams();
  pfSetMultiParam(params, "cliComp");
  if (!PF.globalPlataforma) pfSetMultiParam(params, "cliPlat");
  pfSetMultiParam(params, "cliProv");
  return params;
}

async function pfLoadCliente() {
  if (!pfSetTabSelectionGate("cliente")) return;
  if (!PF.dateUserSet) await pfLoadDateRange(pfGetCliRangeParams());

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
  const requestToken = pfBeginRequestGuard("cli:kpis");

  try {
    const data = await pfFetch(`${BASE}/cliente/kpis?${params}`);
    if (!pfIsRequestCurrent("cli:kpis", requestToken)) return;
    row.innerHTML = [
      pfKpiCard({
        tone: "primary",
        label: "Monto adjudicado",
        value: pfKpiPeso(data.monto_total_cotizado),
        rawValue: pfPeso(data.monto_total_cotizado),
        sub: "Total adjudicado al organismo (pos. 1)",
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
        label: "Articulos",
        value: pfKpiNum(data.descripciones_unicas),
        rawValue: pfFmt(data.descripciones_unicas),
        sub: "Variedad de demanda",
        icon: "bi-box-seam",
      }),
      pfKpiCard({
        tone: "success",
        label: "Rubros",
        value: pfFmt(data.rubros_distintos),
        sub: "Mix de compra",
        icon: "bi-grid-1x2",
      }),
    ].join("");
  } catch (err) {
    if (!pfIsRequestCurrent("cli:kpis", requestToken)) return;
    row.innerHTML = pfKpiError(err.message);
  }
}

async function pfLoadCliEvolucion(params) {
  const requestToken = pfBeginRequestGuard("cli:evol", () => pfDestroyChart("cliEvolChart"));

  try {
    const data = await pfFetch(`${BASE}/cliente/evolucion?${params}`);
    if (!pfIsRequestCurrent("cli:evol", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("cliEvolChart", "Sin datos para este perfil de cliente.", "bi-graph-up-arrow");
      return;
    }

    if (!pfIsRequestCurrent("cli:evol", requestToken)) return;
    pfDestroyChart("cliEvolChart");
    PF.charts.cliEvolChart = new ApexCharts(
      document.getElementById("cliEvolChart"),
      pfChartBase({
        chart: { type: "line", height: 256 },
        series: [{ name: "Monto adjudicado", data: data.map((d) => d.monto_total) }],
        xaxis: { categories: data.map((d) => d.month_label), labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        stroke: { width: 2, curve: "smooth" },
        markers: { size: 3, strokeWidth: 0, hover: { sizeOffset: 2 } },
        legend: { show: false },
        colors: [COLORS.success],
        tooltip: {
          shared: true,
          intersect: false,
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.cliEvolChart.render();
  } catch (err) {
    if (!pfIsRequestCurrent("cli:evol", requestToken)) return;
    pfRenderEmpty("cliEvolChart", "Error al cargar la evolucion del cliente.", "bi-graph-up-arrow");
  }
}

async function pfLoadCliRubros(params) {
  const requestToken = pfBeginRequestGuard("cli:rubros", () => pfDestroyChart("cliRubroChart"));

  try {
    const data = await pfFetch(`${BASE}/cliente/rubros?${params}`);
    if (!pfIsRequestCurrent("cli:rubros", requestToken)) return;
    if (!data.length) {
      pfRenderEmpty("cliRubroChart", "Sin datos de rubros.", "bi-pie-chart-fill");
      return;
    }

    if (!pfIsRequestCurrent("cli:rubros", requestToken)) return;
    pfDestroyChart("cliRubroChart");
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
    if (!pfIsRequestCurrent("cli:rubros", requestToken)) return;
    pfRenderEmpty("cliRubroChart", "Error al cargar los rubros.", "bi-pie-chart-fill");
  }
}

PF._cliProvData = [];
PF._cliArtMetric = "monto";
PF._cliArtData = [];

function pfRenderCliProvChart() {
  const renderToken = pfBeginRequestGuard("cli:prov-chart", () => pfDestroyChart("cliProvChart"));
  const data = PF._cliProvData;
  if (!data.length) {
    pfRenderEmpty("cliProvChart", "Sin proveedores para el perfil filtrado.", "bi-buildings");
    return;
  }
  const top = data.slice(0, 12);
  const seriesData = top.map((d) => d.monto_total);
  const seriesName  = "Monto adjudicado";
  const fmtAxis     = pfAxisFmtMoney;
  const fmtTooltip  = (v) => pfPeso(v);

  if (!pfIsRequestCurrent("cli:prov-chart", renderToken)) return;
  pfDestroyChart("cliProvChart");
  PF.charts.cliProvChart = new ApexCharts(
    document.getElementById("cliProvChart"),
    pfChartBase({
      chart: { type: "bar", height: 256 },
      series: [{ name: seriesName, data: seriesData }],
      xaxis: {
        categories: top.map((d) => pfTrunc(d.proveedor, 36)),
        labels: { formatter: fmtAxis },
      },
      yaxis: { labels: { formatter: fmtAxis, maxWidth: 160 } },
      plotOptions: {
        bar: {
          horizontal: true,
          borderRadius: 7,
          barHeight: "68%",
        },
      },
      colors: [COLORS.brand900],
      tooltip: {
        y: { formatter: fmtTooltip },
      },
    })
  );
  PF.charts.cliProvChart.render();
}

async function pfLoadCliProveedores(params) {
  const requestToken = pfBeginRequestGuard("cli:proveedores", () => pfDestroyChart("cliProvChart"));

  try {
    const data = await pfFetch(`${BASE}/cliente/proveedores?${params}`);
    if (!pfIsRequestCurrent("cli:proveedores", requestToken)) return;
    PF._cliProvData = data;
    if (!data.length) {
      pfRenderEmpty("cliProvChart", "Sin proveedores para el perfil filtrado.", "bi-buildings");
      return;
    }
    pfRenderCliProvChart();
  } catch (err) {
    if (!pfIsRequestCurrent("cli:proveedores", requestToken)) return;
    pfRenderEmpty("cliProvChart", "Error al cargar proveedores.", "bi-buildings");
  }
}

function pfToggleCliArtMetric(metric) {
  PF._cliArtMetric = metric;
  document.querySelectorAll("#cliArtToggle .pf-toggle-btn").forEach((btn) => {
    btn.classList.toggle("active", btn.dataset.metric === metric);
  });
  pfRenderCliArtChart();
}

function pfRenderCliArtChart() {
  const renderToken = pfBeginRequestGuard("cli:art-chart", () => pfDestroyChart("cliArtChart"));
  const data = PF._cliArtData;
  if (!data.length) {
    pfRenderEmpty("cliArtChart", "Sin articulos para este perfil de cliente.", "bi-list-stars");
    return;
  }

  const useMonto = PF._cliArtMetric !== "cant";
  const metricKey = useMonto ? "monto_total" : "cant_adjudicada";
  const sorted = [...data].sort((a, b) => {
    const diff = (parseFloat(b[metricKey]) || 0) - (parseFloat(a[metricKey]) || 0);
    if (diff !== 0) return diff;
    return String(a.descripcion || "").localeCompare(String(b.descripcion || ""), "es");
  });
  const top = sorted.slice(0, 10);
  const chartRows = top;
  const seriesName = useMonto ? "Monto adjudicado" : "Cantidad adjudicada";
  const fmtAxis = useMonto ? pfAxisFmtMoney : pfAxisFmtNum;
  const fmtTooltip = useMonto ? (value) => pfPeso(value) : (value) => pfFmt(value, 2);

  if (!pfIsRequestCurrent("cli:art-chart", renderToken)) return;
  pfDestroyChart("cliArtChart");
  PF.charts.cliArtChart = new ApexCharts(
    document.getElementById("cliArtChart"),
    pfChartBase({
      chart: { type: "bar", height: 256 },
      series: [{ name: seriesName, data: chartRows.map((d) => d[metricKey]) }],
      xaxis: {
        categories: chartRows.map((d) => pfTrunc(d.descripcion, 36)),
        labels: { formatter: fmtAxis },
      },
      yaxis: { labels: { formatter: fmtAxis, maxWidth: 160 } },
      plotOptions: {
        bar: {
          horizontal: true,
          borderRadius: 7,
          barHeight: "68%",
        },
      },
      colors: [useMonto ? COLORS.warning : COLORS.cyan],
      tooltip: {
        y: { formatter: fmtTooltip },
      },
    })
  );
  PF.charts.cliArtChart.render();
}

async function pfLoadCliArticulos(params) {
  const requestToken = pfBeginRequestGuard("cli:articulos", () => pfDestroyChart("cliArtChart"));
  const tbody = document.getElementById("cliArtTbody");
  // Guardar params activos para el desplegable
  PF._cliArtParams = params;

  try {
    const data = await pfFetch(`${BASE}/cliente/articulos?${params}`);
    if (!pfIsRequestCurrent("cli:articulos", requestToken)) return;
    PF._cliArtData = data;

    if (data.length) {
      pfRenderCliArtChart();
    } else {
      pfRenderEmpty("cliArtChart", "Sin articulos para este perfil de cliente.", "bi-list-stars");
    }

    if (tbody) {
      if (!data.length) {
        tbody.innerHTML = pfTableEmpty("bi-hospital", "Sin articulos para este perfil de cliente.", 6);
        return;
      }

      tbody.innerHTML = data.map((d) => {
        const descId = encodeURIComponent(d.descripcion);
        return `
        <tr class="pf-prov-row" data-desc="${descId}">
          <td>${pfCellText(d.descripcion, 56)}</td>
          <td class="num">${pfFmt(d.cant_adjudicada, 2)}</td>
          <td class="num">${pfFmt(d.frecuencia)}</td>
          <td class="num">${pfPeso(d.monto_total)}</td>
          <td class="num">${d.precio_mediana > 0 ? pfPeso(d.precio_mediana) : `<span class="pf-text-muted-sm">—</span>`}</td>
          <td class="num pf-hist-cell">
            <button class="pf-hist-btn" title="Ver detalle del articulo"
              onclick="pfToggleArtDetalle(this, decodeURIComponent('${descId}'))">
              <i class="bi bi-chevron-down"></i>
            </button>
          </td>
        </tr>`;
      }).join("");
    }
  } catch (err) {
    if (!pfIsRequestCurrent("cli:articulos", requestToken)) return;
    pfRenderEmpty("cliArtChart", "Error al cargar articulos.", "bi-list-stars");
    if (tbody) tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar el detalle del cliente.", 6);
  }
}

async function pfToggleArtDetalle(btn, descripcion) {
  const row = btn.closest("tr");
  const detId = "pf-artdet-" + btoa(encodeURIComponent(descripcion)).replace(/[^a-zA-Z0-9]/g, "_");
  const existing = document.getElementById(detId);

  if (existing) {
    existing.remove();
    btn.classList.remove("open");
    btn.innerHTML = `<i class="bi bi-chevron-down"></i>`;
    return;
  }

  btn.classList.add("open");
  btn.innerHTML = `<i class="bi bi-arrow-clockwise pf-spin"></i>`;

  try {
    const params = PF._cliArtParams ? new URLSearchParams(PF._cliArtParams.toString()) : new URLSearchParams();
    params.set("descripcion", descripcion);
    const data = await pfFetch(`${BASE}/cliente/articulo-detalle?${params}`);
    const bodyRows = data.length
      ? data.map((r) => {
          return `<tr>
            <td>${r.fecha ?? "-"}</td>
            <td>${pfEsc(r.marca)}</td>
            <td class="num">${pfPeso(r.precio)}</td>
            <td>${pfEsc(pfTrunc(r.proveedor, 42))}</td>
          </tr>`;
        }).join("")
      : `<tr><td colspan="4" style="text-align:center;padding:.6rem;color:var(--pf-text-muted);font-size:.77rem">Sin registros para los filtros actuales</td></tr>`;

    const detailRow = document.createElement("tr");
    detailRow.id = detId;
    detailRow.className = "pf-hist-row";
    detailRow.innerHTML = `
      <td colspan="6">
        <div class="pf-hist-detail">
          <div class="pf-hist-header"><i class="bi bi-list-ul"></i> Detalle — ${pfTrunc(descripcion, 60)}</div>
          <table class="pf-hist-table">
            <thead><tr>
              <th>Fecha</th>
              <th>Marca</th>
              <th class="num">Precio unit.</th>
              <th>Proveedor</th>
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

document.addEventListener("DOMContentLoaded", async () => {
  pfSyncArticulosCopy();
  pfRenderFilterControls();
  pfUpdateDateSummary();
  await pfLoadFilterOptions();
  pfSyncLockedPlatformUI();
  pfSetAllSelectionGates();
  await pfCheckSync();
  // Poblar inputs con el rango real de la data disponible al iniciar
  await pfLoadDateRange(pfGetGlobalRangeParams());
});
