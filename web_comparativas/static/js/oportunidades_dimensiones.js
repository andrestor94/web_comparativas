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
    emergency: "#064066",
    emergencySoft: "#0B527F",
    regular: "#6CC4E0",
    regularSoft: "#BFE9F6",
    treemapDark1: "#064066",
    treemapDark2: "#1E5A8A",
    treemapDark3: "#3F7FB0",
    treemapBase: "#8CC5EA",
  };

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
  const API_BASE_URL = "/api/oportunidades/dimensiones";
  const GEOJSON_PROVINCIAS_URL = "/static/data/provincias_argentina.geojson";

  let RAW = null; // datos filtrados actuales
  let RAW_DOMAIN = null; // respuesta sin filtro de fecha (solo para dominio)

  let LAST_FILTERED = null;

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

    // Cuenta NO se envía por ahora
    return params.toString();
  }

  // ------------------------------------------------------------------
  // Fetch de datos FILTRADOS (usa date_from/date_to)
  // ------------------------------------------------------------------
  async function fetchDataFiltered() {
    const qs = buildQueryString();
    const url = qs ? `${API_BASE_URL}?${qs}` : API_BASE_URL;

    try {
      const res = await fetch(url, { headers: { Accept: "application/json" } });
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
    fetchDataFiltered();
  }

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

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 7,
      minZoom: 3,
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

      // La provincia top arriba de todo
      if (isTop && layer.bringToFront) {
        layer.bringToFront();
      }
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
      options: options || {},
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
          },
        ],
      },
      options: options || {},
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
            borderWidth: 1,
            spacing: 0.5,
            backgroundColor: makeBgFn(),
            labels: {
              display: true,
              formatter(ctx) {
                const name = ctx.raw.g || "";
                const short =
                  name.length > 24 ? name.slice(0, 23).trimEnd() + "…" : name;
                return short;
              },
            },
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
                title(items) {
                  return items[0].raw.g || "";
                },
                label(item) {
                  const val =
                    item.raw.v ?? item.raw.value ?? item.raw.count ?? 0;
                  return `Procesos: ${val}`;
                },
              },
            },
          },
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
            legend: { display: true, position: "top" },
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
              ticks: { autoSkip: false, font: { size: 10 } },
            },
          },
          plugins: {
            legend: { position: "top", labels: { font: { size: 11 } } },
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
