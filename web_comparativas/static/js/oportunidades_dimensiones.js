// static/js/oportunidades_dimensiones.js
(function () {
  console.log("[Dimensiones] JS cargado v-colores-7-mapa-top");

  // ------------------------------------------------------------------
  // Helpers generales
  // ------------------------------------------------------------------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const pickId = (...ids) => {
    for (const id of ids) {
      if (!id) continue;
      const el = document.getElementById(id);
      if (el) return el;
    }
    return null;
  };

  // ============================================================
  //  PERSISTENCIA DECISIONES (localStorage) - Copied from Buscador
  // ============================================================
  const STORAGE_KEY =
    "wc_opp_decisions_v1_" + (window.location && window.location.pathname ? window.location.pathname.replace("/dimensiones", "/buscador") : "default"); // Hack: share same key as buscador (assuming pathname structure) or just use fixed key if suitable.
  // Better: Use exact same key logic as buscador.js
  // Buscador uses: "wc_opp_decisions_v1_" + pathname.
  // If we are at /oportunidades/dimensiones, we want to read /oportunidades/buscador decisions?
  // User said: "en el apartado de 'Buscador' hay una columna... lo que quiero es que ese mismo filtro este en 'dimensiones'"
  // So we must share the key. Let's assume the key "wc_opp_decisions_v1_/oportunidades/buscador" is what we want if we are in ./dimensiones
  // Robust approach: try to match common key.

  // Update: Buscador path might vary. Let's use a adaptable key or standard one. 
  // Buscador JS: "wc_opp_decisions_v1_" + window.location.pathname
  // If user is at /oportunidades/buscador -> key ends in .../buscador
  // We are at /oportunidades/dimensiones. We need to replce "dimensiones" with "buscador" to access same data.

  function getDecisionStorageKey() {
    const path = window.location.pathname || "";
    // Si estamos en /mercado-privado/dimensiones -> /mercado-privado/buscador ?? (Check routes)
    // Routes: /oportunidades/buscador vs /oportunidades/dimensiones.
    // Replacement seems safe.
    const targetPath = path.replace("dimensiones", "buscador");
    return "wc_opp_decisions_v1_" + targetPath;
  }

  function loadDecisions() {
    try {
      const key = getDecisionStorageKey();
      const raw = window.localStorage.getItem(key);
      if (!raw) return {};
      const parsed = JSON.parse(raw);
      if (parsed && typeof parsed === "object") return parsed;
    } catch (e) {
      // ignorar
    }
    return {};
  }

  let decisionMap = loadDecisions();


  // Debounce helper
  function debounce(func, wait) {
    let timeout;
    return function (...args) {
      const context = this;
      clearTimeout(timeout);
      timeout = setTimeout(() => func.apply(context, args), wait);
    };
  }

  const normalize = (s) =>
    (s || "")
      .toString()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .trim();

  // Normalización especial para nombres de provincia (datos vs GeoJSON)
  function normalizeProvinceName(name) {
    let s = normalize(name);
    if (!s) return "";

    // Sacamos palabras como "provincia de", "prov.", "pcia", etc.
    s = s
      .replace(/\bprovincia de\b/g, "")
      .replace(/\bprovincia\b/g, "")
      .replace(/\bprov\./g, "")
      .replace(/\bprov\b/g, "")
      .replace(/\bpcia\b/g, "")
      .replace(/\bde la\b/g, " ")
      .replace(/\bde los\b/g, " ")
      .replace(/\bde las\b/g, " ")
      .replace(/\bde\b/g, " ");

    // Casos especiales
    if (
      s.includes("ciudad autonoma") ||
      s.includes("capital federal") ||
      s.includes("caba")
    ) {
      s = "caba";
    }

    if (s.includes("tierra del fuego")) {
      s = "tierra del fuego";
    }

    // Compactar espacios
    s = s.replace(/\s+/g, " ").trim();
    return s;
  }

  function isPAMIName(rep) {
    const r = normalize(rep);
    if (!r) return false;
    if (r.includes("pami")) return true;
    const base =
      "instituto nacional de servicios sociales para jubilados y pensionados";
    return r.includes(base);
  }

  function getChipGroupValue(groupEl) {
    if (!groupEl) return "todos";
    const on = groupEl.querySelector(".chip.is-on");
    return on ? on.dataset.val || "todos" : "todos";
  }

  function setChipGroupValue(groupEl, value) {
    if (!groupEl) return;
    $$(".chip", groupEl).forEach((btn) => {
      const isOn = btn.dataset.val === value;
      btn.classList.toggle("is-on", isOn);
      btn.setAttribute("aria-pressed", isOn ? "true" : "false");
    });
  }

  function bindChipGroup(groupEl, onChange) {
    if (!groupEl) return;
    groupEl.addEventListener("click", (e) => {
      const btn = e.target.closest(".chip");
      if (!btn || btn.disabled) return;
      const val = btn.dataset.val || "todos";
      setChipGroupValue(groupEl, val);
      if (typeof onChange === "function") onChange();
    });
  }

  // ------------------------------------------------------------------
  // Nodo raíz y upload_id
  // ------------------------------------------------------------------
  const rootDim = document.getElementById("dimensiones-root");
  const uploadId = rootDim ? rootDim.dataset.uploadId || "" : "";

  if (!rootDim) return; // no estamos en Dimensiones

  // ------------------------------------------------------------------
  // Referencias de UI
  // ------------------------------------------------------------------
  const dateFromEl = pickId("dimDateFrom", "fDateFrom");
  const dateToEl = pickId("dimDateTo", "fDateTo");

  const selPlataforma = pickId("dimPlataforma", "fPlataforma");
  const selCuenta = pickId("dimCuenta", "fCuenta");
  const selReparticion = pickId("dimReparticion", "fReparticion");

  // Slider interno de rango de fechas
  const rangeMinEl = document.getElementById("dimRangeMin");
  const rangeMaxEl = document.getElementById("dimRangeMax");
  const rangeFillEl = document.getElementById("dimDateFill");

  // Dominio total de fechas disponibles (todas las fechas Apertura)
  let DATE_DOMAIN = []; // array de strings YYYY-MM-DD

  // KPI
  const kProcesos = pickId("dimKpiProcesos", "kProcesos");

  // Chips
  const swPAMI = document.getElementById("swPAMI");
  const swEstado = document.getElementById("swEstado");
  const swEvaluacion = document.getElementById("swEvaluacion"); // NEW

  // Chart canvases
  function getCtx(canvasId, containerId) {
    let canvas = document.getElementById(canvasId);
    if (!canvas && containerId) {
      const container = document.getElementById(containerId);
      if (container) {
        canvas = document.createElement("canvas");
        canvas.id = canvasId;
        container.innerHTML = "";
        container.appendChild(canvas);
      }
    }
    return canvas ? canvas.getContext("2d") : null;
  }

  const ctxTimeline = getCtx("dimChartTimeline", "dimPanelTimeline");
  const ctxTipo = getCtx("dimChartTipo", "dimPanelTipo");
  const ctxRepEstado = getCtx("dimChartRepEstado", "dimPanelRepEstado");
  const ctxEstadoPie = getCtx("dimChartEstadoPie", "dimPanelEstadoPie");

  if (typeof Chart === "undefined") {
    console.warn(
      "[Dimensiones] Chart.js no está disponible. Verificá que se cargue el script de Chart.js en el template."
    );
  }

  // ------------------------------------------------------------------
  // Paleta de colores Dimensiones
  // ------------------------------------------------------------------
  const COLORS = {
    emergency: "#064066", // Azul Suizo (oscuro)
    emergencySoft: "#064066cc", // + transparencia
    regular: "#6CC4E0", // Celeste Suizo
    regularSoft: "#6CC4E0cc", // + transparencia
    treemapDark1: "#064066",
    treemapDark2: "#185c8a",
    treemapDark3: "#327bb3",
    treemapBase: "#6CC4E0",
    textMain: "#1e293b",
    textSoft: "#64748b",
    grid: "rgba(148, 163, 184, 0.15)",
  };

  // ------------------------------------------------------------------
  // Configuración Global Chart.js (Modern Look)
  // ------------------------------------------------------------------
  if (typeof Chart !== "undefined") {
    Chart.defaults.font.family = "'Inter', system-ui, -apple-system, sans-serif";
    Chart.defaults.color = COLORS.textMain;

    // Tooltips modernos
    Chart.defaults.plugins.tooltip.backgroundColor = "rgba(255, 255, 255, 0.95)";
    Chart.defaults.plugins.tooltip.titleColor = COLORS.textMain;
    Chart.defaults.plugins.tooltip.bodyColor = COLORS.textSoft;
    Chart.defaults.plugins.tooltip.borderColor = "#e2e8f0";
    Chart.defaults.plugins.tooltip.borderWidth = 1;
    Chart.defaults.plugins.tooltip.padding = 10;
    Chart.defaults.plugins.tooltip.cornerRadius = 8;
    Chart.defaults.plugins.tooltip.displayColors = true;
    Chart.defaults.plugins.tooltip.boxPadding = 4;

    // Leyenda
    Chart.defaults.plugins.legend.labels.usePointStyle = true;
    Chart.defaults.plugins.legend.labels.boxWidth = 8;
    Chart.defaults.plugins.legend.labels.padding = 20;
  }

  // ------------------------------------------------------------------
  // Plugin global de colores
  // ------------------------------------------------------------------
  if (typeof window !== "undefined" && window.Chart) {
    window.Chart.register({
      id: "dimensionesColors",
      afterUpdate(chart) {
        const canvas = chart.canvas || {};
        const id = canvas.id || "";
        if (!/^dimChart/.test(id)) return;
        if (chart.config.type === "pie") return;

        chart.data.datasets.forEach((ds) => {
          const label = (ds.label || "").toString().toUpperCase();
          const isHorizontal = chart.options.indexAxis === "y";

          if (label.includes("EMERGENCIA")) {
            ds.backgroundColor = isHorizontal
              ? COLORS.emergency
              : COLORS.emergencySoft;
            ds.borderColor = COLORS.emergency;
          } else if (label.includes("REGULAR")) {
            ds.backgroundColor = isHorizontal
              ? COLORS.regular
              : COLORS.regularSoft;
            ds.borderColor = COLORS.regular;
          }
        });
      },
    });

    console.log("[Dimensiones] Plugin de colores registrado", COLORS);
  }

  // ------------------------------------------------------------------
  // Estado global
  // ------------------------------------------------------------------
  // CHANGE: We now use a POST endpoint for filtering
  const API_FILTER_URL = "/api/oportunidades/dimensiones/filter";
  const API_BASE_URL = "/api/oportunidades/dimensiones"; // Keep for initial domain fetch (GET) if needed
  const GEOJSON_PROVINCIAS_URL = "/static/data/provincias_argentina.geojson";

  let RAW = null; // datos filtrados actuales
  let RAW_DOMAIN = null; // respuesta sin filtro de fecha (solo para dominio)

  let LAST_FILTERED = null;

  // Variables de filtrado interactivo (click en gráficos)
  let FILTER_TYPE = "";
  let FILTER_PROV = ""; // Nombre normalizado de la provincia seleccionada (mapa)

  function kickFilter(key, value) {
    console.log("[Dimensiones] kickFilter", key, value);
    let changed = false;

    if (key === "buyer") {
      // Intentamos sincronizar con el select si existe
      if (selReparticion) {
        // Si el valor ya estaba seleccionado, lo deseleccionamos (toggle)
        if (selReparticion.value === value) selReparticion.value = "";
        else selReparticion.value = value;
        changed = true;
      }
    } else if (key === "platform") {
      if (selPlataforma) {
        if (selPlataforma.value === value) selPlataforma.value = "";
        else selPlataforma.value = value;
        changed = true;
      }
    } else if (key === "process_type") {
      // Toggle
      if (FILTER_TYPE === value) FILTER_TYPE = "";
      else FILTER_TYPE = value;
      changed = true;
    } else if (key === "province") {
      // Toggle
      if (FILTER_PROV === value) FILTER_PROV = "";
      else FILTER_PROV = value;
      changed = true;
    }

    if (changed) {
      if (key === "province") redrawProvinciaChoropleth(); // highlight selection immediately setup
      fetchDataFiltered();
    }
  }

  const charts = {
    timeline: null,
    tipo: null,
    repEstado: null,
    estadoPie: null,
  };

  // Mapa Leaflet
  let provMap = null;
  let provGeoLayer = null;
  let PROV_GEOJSON = null;

  // New Global Filter for Evaluation
  let FILTER_EVALUATION = "todos"; // todos | aceptado | rechazado | sin-marcar

  // ------------------------------------------------------------------
  // Selects de Plataforma / Cuenta / Repartición
  // ------------------------------------------------------------------
  function refreshSelectOptions() {
    if (!RAW || !RAW.dimensions) return;

    const dims = RAW.dimensions;

    function listLabels(dimList) {
      if (!Array.isArray(dimList)) return [];
      const seen = new Set();
      const out = [];
      for (const item of dimList) {
        const label =
          item && item.label != null ? String(item.label).trim() : "";
        if (!label) continue;
        if (seen.has(label)) continue;
        seen.add(label);
        out.push(label);
      }
      return out;
    }

    const plataformas = listLabels(dims.plataforma);
    const cuentas = listLabels(dims.cuenta);
    const reps = listLabels(dims.comprador);

    function fillSelect(sel, values) {
      if (!sel) return;
      const current = sel.value;
      sel.innerHTML = "";

      const optAll = document.createElement("option");
      optAll.value = "";
      optAll.textContent = "Todas";
      sel.appendChild(optAll);

      values.forEach((v) => {
        const opt = document.createElement("option");
        opt.value = v;
        opt.textContent = v;
        sel.appendChild(opt);
      });

      if (current && values.includes(current)) {
        sel.value = current;
      }
    }

    fillSelect(selPlataforma, plataformas);
    fillSelect(selCuenta, cuentas);
    fillSelect(selReparticion, reps);
  }

  // ------------------------------------------------------------------
  // Query string para fetch filtrado
  // ------------------------------------------------------------------
  function buildQueryString() {
    const params = new URLSearchParams();

    if (uploadId) params.set("upload_id", uploadId);

    if (dateFromEl && dateFromEl.value) {
      params.set("date_from", dateFromEl.value);
    }
    if (dateToEl && dateToEl.value) {
      params.set("date_to", dateToEl.value);
    }

    if (selPlataforma && selPlataforma.value) {
      params.set("platform", selPlataforma.value);
    }
    if (selReparticion && selReparticion.value) {
      params.set("buyer", selReparticion.value);
    }

    if (selCuenta && selCuenta.value) {
      params.set("cuenta", selCuenta.value);
    }

    // Filtros interactivos extra
    if (FILTER_TYPE) params.set("process_type", FILTER_TYPE);
    if (FILTER_PROV) params.set("province", FILTER_PROV);

    // Evaluation filter is passed in body now, but we can keep param helper if needed.
    // We will build a body object instead.
    return params;
  }

  // Helper to build Body object
  function buildFilterBody() {
    const body = {
      decisions: decisionMap || {},
      evaluation: getChipGroupValue(swEvaluacion),
      q: "", // Not used in dimensions UI usually, but good to have
      buyer: selReparticion ? selReparticion.value : "",
      platform: selPlataforma ? selPlataforma.value : "",
      province: FILTER_PROV,
      date_from: dateFromEl ? dateFromEl.value : "",
      date_to: dateToEl ? dateToEl.value : "",
      process_type: FILTER_TYPE,
      cuenta: selCuenta ? selCuenta.value : "",
    };

    if (uploadId) body.upload_id = uploadId;
    return body;
  }

  // ------------------------------------------------------------------
  // Fetch de datos FILTRADOS (usa POST)
  // ------------------------------------------------------------------
  async function fetchDataFiltered() {
    // const qs = buildQueryString(); // Legacy
    const bodyData = buildFilterBody();

    try {
      const res = await fetch(API_FILTER_URL, {
        method: "POST",
        headers: {
          "Accept": "application/json",
          "Content-Type": "application/json"
        },
        body: JSON.stringify(bodyData)
      });

      if (!res.ok) {
        console.error("[Dimensiones] Error HTTP (filtrado)", res.status);
        return;
      }
      const data = await res.json();
      RAW = data;
      refreshSelectOptions();
      updateUI();
    } catch (err) {
      console.error("[Dimensiones] Error de red (filtrado)", err);
    }
  }

  // ------------------------------------------------------------------
  // Fetch inicial: solo para obtener DOMINIO de fechas
  // ------------------------------------------------------------------
  async function fetchDomainAndInit() {
    const params = new URLSearchParams();
    if (uploadId) params.set("upload_id", uploadId);
    const url = `${API_BASE_URL}?${params.toString()}`;

    try {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
      if (!res.ok) {
        console.error("[Dimensiones] Error HTTP (dominio)", res.status);
        return;
      }
      const data = await res.json();
      RAW_DOMAIN = data;
      extractDateDomainFromRaw(RAW_DOMAIN);

      // Si no hay fechas, igual intentamos dibujar algo
      if (!DATE_DOMAIN.length) {
        await fetchDataFiltered();
        return;
      }

      // Solo ponemos el default si los inputs están vacíos
      setDefaultDateRangeIfEmpty();
      syncSliderWithInputsFromDomain();

      // Ahora sí: pedimos los datos filtrados
      await fetchDataFiltered();
    } catch (err) {
      console.error("[Dimensiones] Error de red (dominio)", err);
    }
  }

  // ------------------------------------------------------------------
  // Dominio de fechas a partir de RAW_DOMAIN
  // ------------------------------------------------------------------
  function extractDateDomainFromRaw(raw) {
    DATE_DOMAIN = [];
    if (!raw || !raw.dimensions) return;
    const arr = raw.dimensions.fecha_apertura;
    if (!Array.isArray(arr)) return;

    const set = new Set();
    arr.forEach((d) => {
      if (!d) return;
      const v = d.date || d.fecha || d.label || null;
      if (!v) return;
      const s = String(v).slice(0, 10); // YYYY-MM-DD
      set.add(s);
    });

    DATE_DOMAIN = Array.from(set).sort();
  }

  // ------------------------------------------------------------------
  // Utilidades de fechas + slider
  // ------------------------------------------------------------------
  function addBusinessDays(date, days) {
    const d = new Date(date.getFullYear(), date.getMonth(), date.getDate());
    let remaining = days;
    while (remaining > 0) {
      d.setDate(d.getDate() + 1);
      const day = d.getDay(); // 0 domingo, 6 sábado
      if (day !== 0 && day !== 6) remaining -= 1;
    }
    return d;
  }

  function toISODate(d) {
    const y = d.getFullYear();
    const m = String(d.getMonth() + 1).padStart(2, "0");
    const dd = String(d.getDate()).padStart(2, "0");
    return `${y}-${m}-${dd}`;
  }

  function findDomainDateOnOrAfter(targetDate) {
    if (!DATE_DOMAIN.length) return null;
    const targetTime = targetDate.getTime();
    for (const s of DATE_DOMAIN) {
      const t = new Date(s).getTime();
      if (!Number.isNaN(t) && t >= targetTime) return s;
    }
    // si no hay ninguna mayor, usamos la última
    return DATE_DOMAIN[DATE_DOMAIN.length - 1];
  }

  function findIndexForDateStr(dateStr, fallbackIndex) {
    if (!DATE_DOMAIN.length) return 0;
    const idx = DATE_DOMAIN.indexOf(dateStr);
    if (idx >= 0) return idx;

    const targetTime = new Date(dateStr).getTime();
    if (Number.isNaN(targetTime)) return fallbackIndex;

    let bestIdx = 0;
    let bestDiff = Infinity;
    DATE_DOMAIN.forEach((s, i) => {
      const t = new Date(s).getTime();
      if (Number.isNaN(t)) return;
      const diff = Math.abs(t - targetTime);
      if (diff < bestDiff) {
        bestDiff = diff;
        bestIdx = i;
      }
    });
    return bestIdx;
  }

  // Primer rango por defecto: desde hoy + 3 días hábiles hasta última fecha del dominio
  function setDefaultDateRangeIfEmpty() {
    if (!dateFromEl || !dateToEl) return;
    if (!DATE_DOMAIN.length) return;

    if (dateFromEl.value || dateToEl.value) return; // ya estaba seteado

    const today = new Date();
    const candidateFrom = addBusinessDays(today, 3);
    const fromDomainStr = findDomainDateOnOrAfter(candidateFrom);

    // Si el dominio es todo anterior a candidateFrom, fromDomainStr será la última (queda bien)
    const toStr = DATE_DOMAIN[DATE_DOMAIN.length - 1];

    dateFromEl.value = fromDomainStr || DATE_DOMAIN[0];
    dateToEl.value = toStr;
  }

  function updateDateRangeFill() {
    if (!rangeMinEl || !rangeMaxEl || !rangeFillEl) return;

    const min = parseInt(rangeMinEl.min || "0", 10);
    const max = parseInt(rangeMinEl.max || "0", 10);
    if (max <= min) {
      rangeFillEl.style.left = "0%";
      rangeFillEl.style.width = "0%";
      return;
    }

    let vMin = parseInt(rangeMinEl.value || "0", 10);
    let vMax = parseInt(rangeMaxEl.value || "0", 10);
    if (vMin > vMax) {
      const tmp = vMin;
      vMin = vMax;
      vMax = tmp;
    }

    const total = max - min;
    const startPct = ((vMin - min) / total) * 100;
    const endPct = ((vMax - min) / total) * 100;

    rangeFillEl.style.left = `${startPct}%`;
    rangeFillEl.style.width = `${Math.max(endPct - startPct, 0)}%`;
  }

  // Configura min/max del slider y lo alinea con los inputs actuales
  function syncSliderWithInputsFromDomain() {
    if (!rangeMinEl || !rangeMaxEl || !DATE_DOMAIN.length) {
      updateDateRangeFill();
      return;
    }

    const maxIndex = DATE_DOMAIN.length - 1;
    rangeMinEl.min = "0";
    rangeMinEl.max = String(maxIndex);
    rangeMaxEl.min = "0";
    rangeMaxEl.max = String(maxIndex);

    const defaultFromStr = DATE_DOMAIN[0];
    const defaultToStr = DATE_DOMAIN[maxIndex];

    const fromStr =
      dateFromEl && dateFromEl.value ? dateFromEl.value : defaultFromStr;
    const toStr =
      dateToEl && dateToEl.value ? dateToEl.value : defaultToStr;

    let idxFrom = findIndexForDateStr(fromStr, 0);
    let idxTo = findIndexForDateStr(toStr, maxIndex);
    if (idxFrom > idxTo) {
      const tmp = idxFrom;
      idxFrom = idxTo;
      idxTo = tmp;
    }

    rangeMinEl.value = String(idxFrom);
    rangeMaxEl.value = String(idxTo);
    updateDateRangeFill();
  }

  // Cuando se mueve el slider → actualizar inputs y recargar datos filtrados
  function handleDateRangeSliderChange() {
    if (!rangeMinEl || !rangeMaxEl || !DATE_DOMAIN.length) return;

    let iFrom = parseInt(rangeMinEl.value, 10);
    let iTo = parseInt(rangeMaxEl.value, 10);
    if (Number.isNaN(iFrom)) iFrom = 0;
    if (Number.isNaN(iTo)) iTo = 0;

    const maxIdx = DATE_DOMAIN.length - 1;
    if (iFrom < 0) iFrom = 0;
    if (iTo > maxIdx) iTo = maxIdx;

    if (iFrom > iTo) {
      const tmp = iFrom;
      iFrom = iTo;
      iTo = tmp;
    }

    const fromDate = DATE_DOMAIN[iFrom];
    const toDate = DATE_DOMAIN[iTo];

    if (dateFromEl && fromDate) dateFromEl.value = fromDate;
    if (dateToEl && toDate) dateToEl.value = toDate;

    updateDateRangeFill();
    updateDateRangeFill();
    // Reemplazamos la llamada directa por la versión debounced (más abajo)
    debouncedFetch();
  }

  // Creamos la función debounced fuera del handler para que mantenga su timer
  const debouncedFetch = debounce(() => {
    fetchDataFiltered();
  }, 300);

  // ------------------------------------------------------------------
  // Transformación de datos según filtros PAMI / Estado
  // ------------------------------------------------------------------
  function computeFilteredData() {
    if (!RAW || !RAW.dimensions) return null;

    const dims = RAW.dimensions;

    let dimFecha = Array.isArray(dims.fecha_apertura)
      ? dims.fecha_apertura
      : [];
    let dimProv = Array.isArray(dims.provincia) ? dims.provincia : [];
    let dimTipo = Array.isArray(dims.tipo_proceso) ? dims.tipo_proceso : [];
    let dimRepEstado = Array.isArray(dims.reparticion_estado)
      ? dims.reparticion_estado
      : [];
    let dimEstado = Array.isArray(dims.estado) ? dims.estado : [];

    const pamiVal = getChipGroupValue(swPAMI);
    const estVal = getChipGroupValue(swEstado);

    // Filtro PAMI solo sobre repartición+estado
    if (pamiVal !== "todos") {
      dimRepEstado = dimRepEstado.filter((row) => {
        const isPami = isPAMIName(row.label);
        if (pamiVal === "pami") return isPami;
        if (pamiVal === "otras") return !isPami;
        return true;
      });
    }

    // Filtro EMERGENCIA / REGULAR
    if (estVal !== "todos") {
      const wanted =
        estVal === "emergencia"
          ? "EMERGENCIA"
          : estVal === "regular"
            ? "REGULAR"
            : null;

      if (wanted) {
        const filterDimListByEstado = (list) => {
          if (!Array.isArray(list)) return [];
          return list
            .map((row) => {
              if (!row) return row;
              const emRaw =
                typeof row.emergencia === "number"
                  ? row.emergencia
                  : typeof row.EMERGENCIA === "number"
                    ? row.EMERGENCIA
                    : null;
              const rgRaw =
                typeof row.regular === "number"
                  ? row.regular
                  : typeof row.REGULAR === "number"
                    ? row.REGULAR
                    : null;

              const hasSplit = emRaw !== null || rgRaw !== null;
              if (!hasSplit) return row;

              const em = emRaw || 0;
              const rg = rgRaw || 0;
              let em2 = em;
              let rg2 = rg;

              if (wanted === "EMERGENCIA") rg2 = 0;
              else if (wanted === "REGULAR") em2 = 0;

              const count = em2 + rg2;

              return Object.assign({}, row, {
                emergencia: em2,
                regular: rg2,
                count,
                total:
                  typeof row.total === "number" ? row.total : count,
              });
            })
            .filter((row) => {
              if (!row) return false;
              const em = row.emergencia || 0;
              const rg = row.regular || 0;
              const c =
                typeof row.count === "number" ? row.count : em + rg;
              return c > 0;
            });
        };

        // a) Repartición + estado
        dimRepEstado = dimRepEstado
          .map((row) => {
            const em = row.emergencia || 0;
            const rg = row.regular || 0;
            let em2 = em;
            let rg2 = rg;

            if (wanted === "EMERGENCIA") rg2 = 0;
            else if (wanted === "REGULAR") em2 = 0;

            return {
              label: row.label,
              emergencia: em2,
              regular: rg2,
              total: em2 + rg2,
            };
          })
          .filter((r) => r.total > 0);

        // b) Torta
        dimEstado = dimEstado.filter(
          (e) => (e.estado || "").toString().toUpperCase() === wanted
        );

        // c) Serie temporal
        dimFecha = dimFecha.map((row) => {
          const em = row.emergencia || 0;
          const rg = row.regular || 0;
          let em2 = em;
          let rg2 = rg;

          if (wanted === "EMERGENCIA") rg2 = 0;
          else if (wanted === "REGULAR") em2 = 0;

          return Object.assign({}, row, {
            emergencia: em2,
            regular: rg2,
            count: (em2 || 0) + (rg2 || 0),
          });
        });

        // d) Tipo y provincia
        dimTipo = filterDimListByEstado(dimTipo);
        dimProv = filterDimListByEstado(dimProv);
      }
    }

    // KPI procesos
    let procesosCount = 0;
    if (dimRepEstado.length && (pamiVal !== "todos" || estVal !== "todos")) {
      procesosCount = dimRepEstado.reduce(
        (acc, r) => acc + (r.emergencia || 0) + (r.regular || 0),
        0
      );
    } else if (RAW.kpis && typeof RAW.kpis.total_rows === "number") {
      procesosCount = RAW.kpis.total_rows;
    } else if (Array.isArray(dimFecha) && dimFecha.length) {
      procesosCount = dimFecha.reduce((acc, d) => acc + (d.count || 0), 0);
    }

    return {
      dimFecha,
      dimProv,
      dimTipo,
      dimRepEstado,
      dimEstado,
      procesosCount,
    };
  }

  // ------------------------------------------------------------------
  // Listener swEvaluacion
  // ------------------------------------------------------------------
  if (swEvaluacion) {
    bindChipGroup(swEvaluacion, () => {
      // debouncedFetch? Or direct? Direct is fine for click.
      fetchDataFiltered();
    });
  }

  // ------------------------------------------------------------------
  // Mapa Leaflet
  // ------------------------------------------------------------------
  function initProvinciaMap() {
    const mapEl = document.getElementById("dimProvinciaMap");
    if (!mapEl) return;
    if (typeof L === "undefined") {
      console.warn("[Dimensiones] Leaflet no está disponible.");
      return;
    }

    provMap = L.map(mapEl, {
      zoomControl: false,
      attributionControl: false,
    }).setView([-38.4, -64.8], 3.8);

    // CartoDB Positron (Moderno, limpio)
    L.tileLayer("https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png", {
      maxZoom: 7,
      minZoom: 3,
      attribution: '&copy; OpenStreetMap &copy; CARTO'
    }).addTo(provMap);

    fetch(GEOJSON_PROVINCIAS_URL, {
      headers: { Accept: "application/json" },
    })
      .then((res) => {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then((geo) => {
        if (geo && Array.isArray(geo.features)) {
          PROV_GEOJSON = geo;
          redrawProvinciaChoropleth();
        } else {
          console.warn(
            "[Dimensiones] provincias_argentina.geojson no tiene el formato esperado."
          );
        }
      })
      .catch((err) => {
        console.error(
          "[Dimensiones] Error cargando provincias_argentina.geojson",
          err
        );
      });
  }

  function redrawProvinciaChoropleth() {
    if (!provMap || !PROV_GEOJSON || !LAST_FILTERED) return;

    const dimProv = LAST_FILTERED.dimProv || [];

    // 1) Sumamos procesos por provincia, siendo flexibles con los campos
    const countsByName = new Map();

    dimProv.forEach((d) => {
      if (!d) return;

      const rawName =
        d.label ||
        d.provincia ||
        d.nombre ||
        d.NOMBRE ||
        d.name ||
        "";
      const key = normalizeProvinceName(rawName);
      if (!key) return;

      const em =
        typeof d.emergencia === "number"
          ? d.emergencia
          : typeof d.EMERGENCIA === "number"
            ? d.EMERGENCIA
            : 0;
      const rg =
        typeof d.regular === "number"
          ? d.regular
          : typeof d.REGULAR === "number"
            ? d.REGULAR
            : 0;

      let count = 0;
      if (typeof d.count === "number") count = d.count;
      else if (typeof d.total === "number") count = d.total;
      else count = em + rg;

      const prev = countsByName.get(key) || 0;
      countsByName.set(key, prev + (count || 0));
    });

    // Si ya había una capa dibujada, la removemos
    if (provGeoLayer) {
      provGeoLayer.remove();
      provGeoLayer = null;
    }

    // 2) Detectamos la provincia con mayor cantidad de procesos
    const entries = Array.from(countsByName.entries());
    let maxKey = null;
    let maxCount = 0;
    for (const [k, v] of entries) {
      if (v > maxCount) {
        maxCount = v;
        maxKey = k;
      }
    }

    console.log(
      "[Dimensiones][Mapa] countsByName",
      Object.fromEntries(countsByName),
      "maxKey:",
      maxKey,
      "maxCount:",
      maxCount
    );

    const COLOR_EMPTY = "#E5EEF5";              // sin procesos
    const COLOR_TOP_FILL = COLORS.emergency;    // azul oscuro
    const COLOR_TOP_BORDER = COLORS.emergency;  // borde azul oscuro
    const COLOR_OTHER_FILL = COLORS.regularSoft; // azul claro
    const COLOR_OTHER_BORDER = COLORS.regular;   // borde celeste

    // Bounds SOLO de las provincias con datos
    let dataBounds = null;

    provGeoLayer = L.geoJSON(PROV_GEOJSON, {
      style: (feature) => {
        const props = feature.properties || {};
        const rawName =
          props.provincia ||
          props.nombre ||
          props.NOMBRE ||
          props.name ||
          "";
        const key = normalizeProvinceName(rawName);
        const value = countsByName.get(key) || 0;
        const hasData = value > 0;

        const isTop =
          hasData &&
          maxCount > 0 &&
          (value === maxCount || (maxKey && key === maxKey));

        if (!hasData) {
          // Provincias sin procesos: muy claras
          return {
            color: "#d1d5db",
            weight: 1,
            fill: true,
            fillColor: COLOR_EMPTY,
            fillOpacity: 0.25,
          };
        }

        if (isTop) {
          // PROVINCIA CON MÁS PROCESOS → TODO AZUL OSCURO
          return {
            color: COLOR_TOP_BORDER,
            weight: 3,
            fill: true,
            fillColor: COLOR_TOP_FILL,
            fillOpacity: 1,
          };
        }

        // Resto de provincias con procesos → azul claro
        return {
          color: COLOR_OTHER_BORDER,
          weight: 1.5,
          fill: true,
          fillColor: COLOR_OTHER_FILL,
          fillOpacity: 0.85,
        };
      },
      onEachFeature(feature, layer) {
        const props = feature.properties || {};
        const rawName =
          props.provincia ||
          props.nombre ||
          props.NOMBRE ||
          props.name ||
          "Sin nombre";
        const key = normalizeProvinceName(rawName);
        const value = countsByName.get(key) || 0;

        const isTop =
          value > 0 &&
          maxCount > 0 &&
          (value === maxCount || (maxKey && key === maxKey));

        // Tooltip
        layer.bindTooltip(
          `${rawName}: ${value.toLocaleString("es-AR")} procesos`,
          {
            direction: "top",
            sticky: true,
          }
        );

        // Acumulamos bounds SOLO de provincias con datos
        if (value > 0) {
          const b = layer.getBounds && layer.getBounds();
          if (b && b.isValid && b.isValid()) {
            if (!dataBounds) dataBounds = b;
            else dataBounds.extend(b);
          }
        }

        if (isTop && layer.bringToFront) {
          layer.bringToFront();
        }

        // CLICK: Filtrar por provincia
        layer.on("click", () => {
          // Usamos rawName si es consistente, o key si preferimos normalizado.
          // kickFilter normalizara si hace falta? 
          // Mejor enviamos el normalizado Key para matching, aunque kickFilter usa exact string match si es var normalizada?
          // El backend usa 'contains'. 'key' es procesado. rawName es mejor para visual search. 
          // Pero kickFilter("province", val) sets FILTER_PROV.
          // Y la condicion es if (FILTER_PROV) params.set("province", FILTER_PROV).
          // Backend search: if province in ... (contains).
          // Si el rawName es "Buenos Aires", backend busca "Buenos Aires". 
          // Si pasamos "buenos aires", anda igual (case sensitive? No, backend usa lower()).
          // Vamos con rawName.
          if (rawName) kickFilter("province", rawName);
        });
      },
    }).addTo(provMap);

    // 3) Zoom a donde hay procesos (si existe), si no al mapa completo
    if (dataBounds && dataBounds.isValid && dataBounds.isValid()) {
      provMap.fitBounds(dataBounds, {
        padding: [15, 15],
        maxZoom: 6,
      });
    } else {
      const bounds = provGeoLayer.getBounds();
      if (bounds && bounds.isValid()) {
        provMap.fitBounds(bounds, { padding: [10, 10] });
      }
    }
  }

  // ------------------------------------------------------------------
  // Creación / actualización de gráficos
  // ------------------------------------------------------------------
  function createOrUpdateBar(chartRef, ctx, labels, datasets, options) {
    if (!ctx || typeof Chart === "undefined") return null;
    if (chartRef && typeof chartRef.destroy === "function") chartRef.destroy();
    return new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: Object.assign(
        {
          layout: { padding: 10 },
          elements: {
            bar: {
              borderRadius: 6, // Bordes redondeados
              borderSkipped: false,
            }
          },
          scales: {
            x: {
              grid: { display: false }, // Sin grid vertical
              ticks: { color: COLORS.textSoft }
            },
            y: {
              grid: { color: COLORS.grid, borderDash: [4, 4] },
              border: { display: false },
              ticks: { color: COLORS.textSoft }
            }
          }
        },
        options || {}
      ),
    });
  }

  function createOrUpdatePie(chartRef, ctx, labels, data, options) {
    if (!ctx || typeof Chart === "undefined") return null;

    const backgroundColor = labels.map((lbl) => {
      const up = (lbl || "").toString().toUpperCase();
      if (up.includes("EMERGENCIA")) return COLORS.emergency;
      if (up.includes("REGULAR")) return COLORS.regular;
      return "#d1d5db";
    });

    if (chartRef && typeof chartRef.destroy === "function") chartRef.destroy();

    return new Chart(ctx, {
      type: "pie",
      data: {
        labels,
        datasets: [
          {
            data,
            backgroundColor,
            borderColor: "#ffffff",
            borderWidth: 2,
            hoverOffset: 15, // Efecto hover
          },
        ],
      },
      options: Object.assign(
        {
          layout: { padding: 20 },
          plugins: {
            legend: { position: "right" } // Leyenda al costado para pie
          }
        },
        options || {}
      ),
    });
  }

  function createOrUpdateTreemap(chartRef, ctx, tree, options) {
    if (!ctx || typeof Chart === "undefined") return null;

    const hasTreemap =
      Chart.registry &&
      typeof Chart.registry.getController === "function" &&
      !!Chart.registry.getController("treemap");

    if (!hasTreemap) {
      console.warn(
        "[Dimensiones] Plugin treemap no está disponible. Verificá el script chartjs-chart-treemap."
      );
      return chartRef;
    }

    const rankByLabel = new Map(tree.map((d, idx) => [d.label, idx]));

    const makeBgFn = () =>
      function backgroundColor(ctx) {
        const label = ctx.raw && ctx.raw.g ? ctx.raw.g : "";
        const rank = rankByLabel.get(label);
        if (rank === 0) return COLORS.treemapDark1;
        if (rank === 1) return COLORS.treemapDark2;
        if (rank === 2) return COLORS.treemapDark3;
        return COLORS.treemapBase;
      };

    if (chartRef && typeof chartRef.destroy === "function") chartRef.destroy();

    return new Chart(ctx, {
      type: "treemap",
      data: {
        datasets: [
          {
            label: "Procesos por tipo",
            tree,
            key: "value",
            groups: ["label"],
            borderColor: "#ffffff",
            borderWidth: 2,
            spacing: 2,
            backgroundColor: makeBgFn(),
            borderRadius: 6,
            labels: {
              display: true,
              color: "white",
              font: { weight: "600", size: 12 },
              formatter(ctx) {
                const label = ctx.raw.g || "";
                const element = ctx.element;

                // 1. Si el nombre es muy extenso (> 35 caracteres), no aparece
                if (label.length > 35) return "";

                // 2. Si el recuadro es muy pequeño (umbral de seguridad), no aparece
                if (!element || element.width < 70 || element.height < 40) {
                  return "";
                }

                // 3. Verificamos si el texto cabe horizontalmente (aprox 7px por char para size 12)
                if (label.length * 7 > element.width) {
                  return "";
                }

                return label;
              },
            },
            // Clickable
            clickable: true,
          },
        ],
      },
      options: Object.assign(
        {
          responsive: true,
          maintainAspectRatio: false,
          plugins: {
            legend: { display: false },
            tooltip: {
              callbacks: {
                title(items) { return items[0].raw.g || ""; },
                label(item) {
                  const val = item.raw.v ?? item.raw.value ?? item.raw.count ?? 0;
                  return `Procesos: ${val}`;
                },
              },
            },
          },
          onClick(evt, elements, chart) {
            if (!elements.length) return;
            // Elemento treemap
            const el = elements[0].element;
            // Chart.js treemap guarda data en el objeto raw
            // Pero 'elements' access difiere segun version.
            // Probamos acceder via data index
            const dataIdx = elements[0].index;
            const dsIdx = elements[0].datasetIndex;
            // En treemap, la data plana esta en chart.data.datasets[dsIdx].data[dataIdx]? No.
            // El plugin procesa 'tree' y genera rects. 
            // Accedemos a raw object via element options o similar?
            // Lo mas facil: 
            const item = elements[0].element.$context.raw;
            if (item && item.g) {
              kickFilter("process_type", item.g);
            }
          }
        },
        options || {}
      ),
    });
  }

  // ------------------------------------------------------------------
  // Actualizar UI completa
  // ------------------------------------------------------------------
  function updateUI() {
    if (!RAW || !RAW.ok || !RAW.has_file) {
      if (kProcesos) kProcesos.textContent = "0";
      return;
    }

    const F = computeFilteredData();
    if (!F) return;
    LAST_FILTERED = F;

    if (kProcesos) {
      kProcesos.textContent = String(F.procesosCount || 0);
    }

    // Timeline
    if (ctxTimeline) {
      const labels = F.dimFecha.map((d) => d.date || "");
      const emData = F.dimFecha.map((d) => d.emergencia || 0);
      const rgData = F.dimFecha.map((d) => d.regular || 0);

      charts.timeline = createOrUpdateBar(
        charts.timeline,
        ctxTimeline,
        labels,
        [
          {
            label: "EMERGENCIA",
            data: emData,
            backgroundColor: COLORS.emergencySoft,
            borderColor: COLORS.emergency,
            borderWidth: 1,
          },
          {
            label: "REGULAR",
            data: rgData,
            backgroundColor: COLORS.regularSoft,
            borderColor: COLORS.regular,
            borderWidth: 1,
          },
        ],
        {
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: {
              stacked: true,
              ticks: { autoSkip: true, maxTicksLimit: 15 },
              grid: { display: false },
            },
            y: {
              stacked: true,
              beginAtZero: true,
              grid: { color: "rgba(148, 163, 184, 0.3)" },
            },
          },
          plugins: {
            legend: { display: true, position: "top", align: "end" },
          },
          onClick: function (evt, elements, chart) {
            if (!swEstado) return;
            if (!elements.length) return;
            const el = elements[0];
            const dsIndex = el.datasetIndex;
            let target = "todos";
            if (dsIndex === 0) target = "emergencia";
            else if (dsIndex === 1) target = "regular";
            const current = getChipGroupValue(swEstado);
            if (current === target) target = "todos";
            setChipGroupValue(swEstado, target);
            updateUI();
          },
        }
      );
    }

    // Mapa
    redrawProvinciaChoropleth();

    // Treemap tipo
    if (ctxTipo) {
      const src = [...F.dimTipo]
        .map((d) => ({
          label: d.label || "Sin tipo",
          count: d.count || 0,
        }))
        .sort((a, b) => b.count - a.count)
        .slice(0, 40);

      const tree = src.map((d) => ({
        label: d.label,
        value: d.count,
      }));

      charts.tipo = createOrUpdateTreemap(charts.tipo, ctxTipo, tree);
    }

    // Barras repartición/estado
    if (ctxRepEstado) {
      const src = [...F.dimRepEstado]
        .map((r) => ({
          ...r,
          total: (r.emergencia || 0) + (r.regular || 0),
        }))
        .sort((a, b) => (b.total || 0) - (a.total || 0))
        .slice(0, 10);

      const labels = src.map((d) => d.label || "");
      const emData = src.map((d) => d.emergencia || 0);
      const rgData = src.map((d) => d.regular || 0);

      charts.repEstado = createOrUpdateBar(
        charts.repEstado,
        ctxRepEstado,
        labels,
        [
          {
            label: "EMERGENCIA",
            data: emData,
            backgroundColor: COLORS.emergency,
          },
          { label: "REGULAR", data: rgData, backgroundColor: COLORS.regular },
        ],
        {
          indexAxis: "y",
          responsive: true,
          maintainAspectRatio: false,
          scales: {
            x: {
              stacked: true,
              beginAtZero: true,
              grid: { color: "rgba(148, 163, 184, 0.25)" },
            },
            y: {
              stacked: true,
              grid: { display: false },
              ticks: {
                autoSkip: false,
                font: { size: 10 },
                callback: function (value) {
                  let lbl = this.getLabelForValue(value) || "";
                  if (lbl.length > 35) {
                    return lbl.substring(0, 35) + "...";
                  }
                  return lbl;
                },
              },
            },
          },
          plugins: {
            legend: { position: "top", labels: { font: { size: 11 } } },
          },
          onClick: function (evt, elements, chart) {
            if (!elements.length) return;
            const el = elements[0];
            const idx = el.index; // index axis y -> index is row index
            const label = chart.data.labels[idx];

            // Si es un gráfico de Repartición (que lo es, charts.repEstado), label es el comprador.
            if (label) kickFilter("buyer", label);
          },
        }
      );
    }

    // Torta estado
    if (ctxEstadoPie) {
      const labels = F.dimEstado.map((e) => e.estado || "");
      const data = F.dimEstado.map((e) => e.count || 0);

      charts.estadoPie = createOrUpdatePie(
        charts.estadoPie,
        ctxEstadoPie,
        labels,
        data,
        {
          responsive: true,
          maintainAspectRatio: false,
          plugins: { legend: { position: "bottom" } },
          onClick: function (evt, elements, chart) {
            if (!swEstado) return;
            if (!elements.length) {
              setChipGroupValue(swEstado, "todos");
              updateUI();
              return;
            }
            const el = elements[0];
            const labelRaw = chart.data.labels[el.index] || "";
            const label = labelRaw.toString().toUpperCase();
            let target = "todos";
            if (label.includes("EMERGENCIA")) target = "emergencia";
            else if (label.includes("REGULAR")) target = "regular";
            const current = getChipGroupValue(swEstado);
            if (current === target) target = "todos";
            setChipGroupValue(swEstado, target);
            updateUI();
          },
        }
      );
    }
  }

  // ------------------------------------------------------------------
  // Eventos de filtros
  // ------------------------------------------------------------------
  function handleDateInputChange() {
    // cuando cambian los inputs manualmente, actualizamos slider y recargamos
    syncSliderWithInputsFromDomain();
    fetchDataFiltered();
  }

  function bindFilters() {
    if (dateFromEl) {
      dateFromEl.addEventListener("change", handleDateInputChange);
    }
    if (dateToEl) {
      dateToEl.addEventListener("change", handleDateInputChange);
    }
    if (selPlataforma) {
      selPlataforma.addEventListener("change", fetchDataFiltered);
    }
    if (selReparticion) {
      selReparticion.addEventListener("change", fetchDataFiltered);
    }
    if (selCuenta) {
      selCuenta.addEventListener("change", fetchDataFiltered);
    }

    if (rangeMinEl) {
      rangeMinEl.addEventListener("input", handleDateRangeSliderChange);
    }
    if (rangeMaxEl) {
      rangeMaxEl.addEventListener("input", handleDateRangeSliderChange);
    }

    bindChipGroup(swPAMI, updateUI);
    bindChipGroup(swEstado, updateUI);

    // NEW: Bind Evaluacion filter
    if (swEvaluacion) {
      bindChipGroup(swEvaluacion, () => {
        fetchDataFiltered();
      });
    }
  }

  // ------------------------------------------------------------------
  // Inicialización
  // ------------------------------------------------------------------
  document.addEventListener("DOMContentLoaded", function () {
    bindFilters();
    initProvinciaMap();
    fetchDomainAndInit(); // primero dominio, luego vista inicial filtrada
  });
})();
