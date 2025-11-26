// static/js/oportunidades_dimensiones.js
(function () {
  console.log("[Dimensiones] JS cargado v-colores-4");

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
  // Nodo raíz y upload_id (si el template lo pasó)
  // ------------------------------------------------------------------
  const rootDim = document.getElementById("dimensiones-root");
  const uploadId = rootDim ? rootDim.dataset.uploadId || "" : "";

  if (!rootDim) {
    // No estamos en la vista de Dimensiones
    return;
  }

  // ------------------------------------------------------------------
  // Referencias de UI
  // ------------------------------------------------------------------
  const dateFromEl = pickId("dimDateFrom", "fDateFrom");
  const dateToEl = pickId("dimDateTo", "fDateTo");

  const selPlataforma = pickId("dimPlataforma", "fPlataforma");
  const selCuenta = pickId("dimCuenta", "fCuenta");
  const selReparticion = pickId("dimReparticion", "fReparticion");

  // KPI de procesos
  const kProcesos = pickId("dimKpiProcesos", "kProcesos");

  // Grupos de chips
  const swPAMI = document.getElementById("swPAMI");
  const swEstado = document.getElementById("swEstado");

  // Contenedores / canvas de gráficos (solo charts, el mapa va con Leaflet)
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
    // EMERGENCIA: azul corporativo
    emergency: "#064066",
    emergencySoft: "#0B527F",

    // REGULAR: azul agua marina suave
    regular: "#6CC4E0",
    regularSoft: "#BFE9F6",

    // Treemap: degradé por tamaño
    treemapDark1: "#064066", // más grande
    treemapDark2: "#1E5A8A", // segundo
    treemapDark3: "#3F7FB0", // tercero
    treemapBase: "#8CC5EA",  // resto
  };

  // ------------------------------------------------------------------
  // Plugin global que fuerza la paleta SOLO en los charts de Dimensiones
  // ------------------------------------------------------------------
  if (typeof window !== "undefined" && window.Chart) {
    window.Chart.register({
      id: "dimensionesColors",
      afterUpdate(chart) {
        const canvas = chart.canvas || {};
        const id = canvas.id || "";

        // Solo tocamos los gráficos de esta pantalla
        if (!/^dimChart/.test(id)) return;

        // Para la torta dejamos que la configure createOrUpdatePie
        if (chart.config.type === "pie") {
          return;
        }

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
  // Estado global (datos crudos + gráficos + mapa)
  // ------------------------------------------------------------------
  const API_BASE_URL = "/api/oportunidades/dimensiones";
  const GEOJSON_PROVINCIAS_URL = "/static/data/provincias_argentina.geojson";

  let RAW = null;
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
  // Llenar selects de Plataforma / Cuenta / Repartición desde RAW.dimensions
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
  // Construir query de filtros y pedir datos
  // ------------------------------------------------------------------
  function buildQueryString() {
    const params = new URLSearchParams();

    if (uploadId) {
      params.set("upload_id", uploadId);
    }

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

    // Cuenta NO se envía por ahora (no está en el endpoint)
    return params.toString();
  }

  async function fetchData() {
    const qs = buildQueryString();
    const url = qs ? `${API_BASE_URL}?${qs}` : API_BASE_URL;

    try {
      const res = await fetch(url, {
        headers: { Accept: "application/json" },
      });
      if (!res.ok) {
        console.error("[Dimensiones] Error HTTP", res.status);
        return;
      }
      const data = await res.json();
      RAW = data;
      refreshSelectOptions();
      updateUI();
    } catch (err) {
      console.error("[Dimensiones] Error de red", err);
    }
  }

  // ------------------------------------------------------------------
  // Transformar datos crudos según filtros PAMI / Estado
  // ------------------------------------------------------------------
  function computeFilteredData() {
    if (!RAW || !RAW.dimensions) return null;

    const dims = RAW.dimensions;

    // Copia base de todas las dimensiones
    let dimFecha = Array.isArray(dims.fecha_apertura)
      ? dims.fecha_apertura
      : [];
    let dimProv = Array.isArray(dims.provincia) ? dims.provincia : [];
    let dimTipo = Array.isArray(dims.tipo_proceso)
      ? dims.tipo_proceso
      : [];
    let dimRepEstado = Array.isArray(dims.reparticion_estado)
      ? dims.reparticion_estado
      : [];
    let dimEstado = Array.isArray(dims.estado) ? dims.estado : [];

    const pamiVal = getChipGroupValue(swPAMI); // todos | pami | otras
    const estVal = getChipGroupValue(swEstado); // todos | emergencia | regular

    // 1) Filtro PAMI solo en repartición+estado
    if (pamiVal !== "todos") {
      dimRepEstado = dimRepEstado.filter((row) => {
        const isPami = isPAMIName(row.label);
        if (pamiVal === "pami") return isPami;
        if (pamiVal === "otras") return !isPami;
        return true;
      });
    }

    // 2) Filtro de Estado (EMERGENCIA / REGULAR)
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

            if (wanted === "EMERGENCIA") {
              rg2 = 0;
            } else if (wanted === "REGULAR") {
              em2 = 0;
            }

            return {
              label: row.label,
              emergencia: em2,
              regular: rg2,
              total: em2 + rg2,
            };
          })
          .filter((r) => r.total > 0);

        // b) Torta global
        dimEstado = dimEstado.filter(
          (e) =>
            (e.estado || "").toString().toUpperCase() === wanted
        );

        // c) Serie temporal
        dimFecha = dimFecha.map((row) => {
          const em = row.emergencia || 0;
          const rg = row.regular || 0;
          let em2 = em;
          let rg2 = rg;

          if (wanted === "EMERGENCIA") {
            rg2 = 0;
          } else if (wanted === "REGULAR") {
            em2 = 0;
          }

          return Object.assign({}, row, {
            emergencia: em2,
            regular: rg2,
            count: (em2 || 0) + (rg2 || 0),
          });
        });

        // d) Tipo de proceso y Provincia
        dimTipo = filterDimListByEstado(dimTipo);
        dimProv = filterDimListByEstado(dimProv);
      }
    }

    // 3) KPI de Procesos
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
  // Creación / actualización de gráficos con Chart.js
  // ------------------------------------------------------------------
  function createOrUpdateBar(chartRef, ctx, labels, datasets, options) {
    if (!ctx || typeof Chart === "undefined") return null;
    if (chartRef) {
      chartRef.data.labels = labels;
      chartRef.data.datasets = datasets;
      chartRef.options = Object.assign(chartRef.options || {}, options || {});
      chartRef.update();
      return chartRef;
    }
    return new Chart(ctx, {
      type: "bar",
      data: { labels, datasets },
      options: options || {},
    });
  }

  // Torta "Proceso por estado" con colores según la etiqueta
  function createOrUpdatePie(chartRef, ctx, labels, data, options) {
    if (!ctx || typeof Chart === "undefined") return null;

    const backgroundColor = labels.map((lbl) => {
      const up = (lbl || "").toString().toUpperCase();
      if (up.includes("EMERGENCIA")) return COLORS.emergency;
      if (up.includes("REGULAR")) return COLORS.regular;
      return "#d1d5db";
    });

    if (chartRef) {
      chartRef.data.labels = labels;
      chartRef.data.datasets[0].data = data;
      chartRef.data.datasets[0].backgroundColor = backgroundColor;
      chartRef.data.datasets[0].borderColor = "#ffffff";
      chartRef.data.datasets[0].borderWidth = 2;
      chartRef.options = Object.assign(chartRef.options || {}, options || {});
      chartRef.update();
      return chartRef;
    }

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

  // Treemap para "Procesos por tipo" con degradé según tamaño
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

    // Mapear label -> rank (posición por tamaño)
    const rankByLabel = new Map(
      tree.map((d, idx) => [d.label, idx])
    );

    const makeBgFn = () =>
      function backgroundColor(ctx) {
        const label = ctx.raw && ctx.raw.g ? ctx.raw.g : "";
        const rank = rankByLabel.get(label);
        if (rank === 0) return COLORS.treemapDark1;
        if (rank === 1) return COLORS.treemapDark2;
        if (rank === 2) return COLORS.treemapDark3;
        return COLORS.treemapBase;
      };

    if (chartRef) {
      chartRef.data.datasets[0].tree = tree;
      chartRef.data.datasets[0].backgroundColor = makeBgFn();
      chartRef.options = Object.assign(chartRef.options || {}, options || {});
      chartRef.update();
      return chartRef;
    }

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
  // Mapa de "Procesos por provincia" con Leaflet
  // ------------------------------------------------------------------
  function initProvinciaMap() {
    const mapEl = document.getElementById("dimProvinciaMap");
    if (!mapEl) return;
    if (typeof L === "undefined") {
      console.warn(
        "[Dimensiones] Leaflet (L) no está disponible. El mapa no se mostrará."
      );
      return;
    }

    provMap = L.map(mapEl, {
      zoomControl: false,
      attributionControl: false,
    }).setView([-38.4, -64.8], 3.8); // centro aproximado de Argentina

    L.tileLayer("https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png", {
      maxZoom: 7,
      minZoom: 3,
    }).addTo(provMap);

    // Cargamos el GeoJSON local de provincias
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
          console.log(
            "[Dimensiones] GeoJSON de provincias cargado:",
            geo.features.length,
            "features"
          );
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

    // Conteo por nombre normalizado de provincia
    const countsByName = new Map();
    dimProv.forEach((d) => {
      const name = normalize(d.label);
      const prev = countsByName.get(name) || 0;
      countsByName.set(name, prev + (d.count || 0));
    });

    // Eliminamos capa anterior si existe
    if (provGeoLayer) {
      provGeoLayer.remove();
      provGeoLayer = null;
    }

    const entries = Array.from(countsByName.entries());
    let maxKey = null;
    let maxCount = 0;
    for (const [k, v] of entries) {
      if (v > maxCount) {
        maxCount = v;
        maxKey = k;
      }
    }

    // Misma escala suave que ya venías usando para el resto
    function colorForValue(v) {
      if (maxCount <= 0 || v <= 0) return "#E5EEF5";
      const t = v / maxCount;
      if (t > 0.7) return COLORS.regular;
      if (t > 0.4) return COLORS.treemapBase;
      return COLORS.regularSoft;
    }

    provGeoLayer = L.geoJSON(PROV_GEOJSON, {
      style: (feature) => {
        const props = feature.properties || {};
        const rawName =
          props.provincia ||
          props.nombre ||
          props.NOMBRE ||
          props.name ||
          "";
        const key = normalize(rawName);
        const value = countsByName.get(key) || 0;

        // Solo la provincia con más procesos va en azul oscuro corporativo
        const isTop = maxKey && key === maxKey && value > 0;

        return {
          color: "#ffffff",
          weight: 1,
          fillColor: isTop ? COLORS.emergency : colorForValue(value),
          fillOpacity: value > 0 ? 0.9 : 0.4,
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
        const key = normalize(rawName);
        const value = countsByName.get(key) || 0;
        layer.bindTooltip(
          `${rawName}: ${value.toLocaleString("es-AR")} procesos`,
          {
            direction: "top",
            sticky: true,
          }
        );
      },
    }).addTo(provMap);

    const bounds = provGeoLayer.getBounds();
    if (bounds.isValid()) {
      provMap.fitBounds(bounds, { padding: [10, 10] });
    }
  }

  // ------------------------------------------------------------------
  // Actualizar toda la UI (KPI + gráficos + mapa)
  // ------------------------------------------------------------------
  function updateUI() {
    if (!RAW || !RAW.ok || !RAW.has_file) {
      if (kProcesos) kProcesos.textContent = "0";
      return;
    }

    const F = computeFilteredData();
    if (!F) return;
    LAST_FILTERED = F;

    // KPI Procesos
    if (kProcesos) {
      kProcesos.textContent = String(F.procesosCount || 0);
    }

    // 1) Apertura de procesos en el tiempo
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
            legend: {
              display: true,
              position: "top",
            },
          },
        }
      );
    }

    // 2) Procesos por provincia (MAPA Leaflet)
    redrawProvinciaChoropleth();

    // 3) Procesos por tipo (TREEMAP con degradé)
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

    // 4) Proceso por repartición y estado (barras apiladas HORIZONTALES)
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
          {
            label: "REGULAR",
            data: rgData,
            backgroundColor: COLORS.regular,
          },
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
              },
            },
          },
          plugins: {
            legend: {
              position: "top",
              labels: {
                font: { size: 11 },
              },
            },
          },
        }
      );
    }

    // 5) Proceso por estado (torta)
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
          plugins: {
            legend: {
              position: "bottom",
            },
          },
          // clic en la torta = cambiar chips de Estado
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
            if (current === target) {
              target = "todos";
            }

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
  function bindFilters() {
    if (dateFromEl) {
      dateFromEl.addEventListener("change", fetchData);
    }
    if (dateToEl) {
      dateToEl.addEventListener("change", fetchData);
    }
    if (selPlataforma) {
      selPlataforma.addEventListener("change", fetchData);
    }
    if (selReparticion) {
      selReparticion.addEventListener("change", fetchData);
    }
    if (selCuenta) {
      selCuenta.addEventListener("change", fetchData);
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
    fetchData();
  });
})();
