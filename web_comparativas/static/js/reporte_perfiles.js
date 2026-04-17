/* reporte_perfiles.js - Dashboard de Reporte de Perfiles (Mercado Publico) */
"use strict";

const PF = {
  activeTab: "articulos",
  globalFechaDesde: "",
  globalFechaHasta: "",
  globalPlataforma: "SIPROSA",
  charts: {},
  typeaheadTimers: {},
  filterOptions: {},
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

/* ── FORMATTERS ABREVIADOS ────────────────────────────────────
   Regla unificada para todo el modulo:
   - pfFmtShort / pfFmtNumShort  → ejes y labels de graficos
   - pfKpiPeso / pfKpiNum        → valores dentro de KPI cards
   Sufijo con espacio (ej: "$ 5 M") para mejor legibilidad.
   Los tooltips usan pfPeso() para mostrar el valor completo.
──────────────────────────────────────────────────────────── */
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

/* Formatter monetario para KPI cards: abrevia para evitar saltos de linea */
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

/* Formatter numerico para KPI cards: abrevia cantidades grandes */
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

function pfGlobalParams() {
  const params = new URLSearchParams();
  if (PF.globalFechaDesde) params.set("fecha_desde", PF.globalFechaDesde);
  if (PF.globalFechaHasta) params.set("fecha_hasta", PF.globalFechaHasta);
  if (PF.globalPlataforma) params.set("plataforma", PF.globalPlataforma);
  return params;
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

/*
  Merge 2 niveles para eje: preserva labels.style + labels.formatter juntos.
  El spread shallow de pfChartBase perdería uno al pasar el otro.
*/
function pfMergeAxis(base, override) {
  if (!override) return { ...base };
  return {
    ...base,
    ...override,
    labels: { ...(base.labels || {}), ...(override.labels || {}) },
  };
}

/*
  Formatter seguro para ejes que pueden recibir tanto strings (categorias)
  como numeros (escala de valores). Lo usan los charts de barras horizontales
  donde ApexCharts puede llamar al mismo formatter para ambos tipos.
*/
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

/* rawValue: valor completo para title/hover cuando value es abreviado */
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

function pfSyncLockedPlatformUI() {
  const platSel = document.getElementById("cliPlatSel");
  if (!platSel) return;

  if (!PF.globalPlataforma) {
    platSel.disabled = false;
    platSel.title = "";
    return;
  }

  const options = Array.from(platSel.options);
  const hasLocked = options.some((option) => option.value === PF.globalPlataforma);
  if (!hasLocked) {
    const opt = document.createElement("option");
    opt.value = PF.globalPlataforma;
    opt.textContent = PF.globalPlataforma;
    platSel.insertBefore(opt, platSel.firstChild);
  }
  platSel.value = PF.globalPlataforma;
  platSel.disabled = true;
  platSel.title = "La vista mantiene bloqueado el alcance actual en SIPROSA.";
}

function pfSwitchTab(name, el) {
  PF.activeTab = name;
  document.querySelectorAll(".pf-tab").forEach((tab) => tab.classList.remove("active"));
  document.querySelectorAll(".pf-tab-content").forEach((content) => content.classList.remove("active"));
  if (el) el.classList.add("active");
  const content = document.getElementById(`tab-${name}`);
  if (content) content.classList.add("active");

  if (name === "articulos") pfLoadArticulos();
  else if (name === "competidor") pfLoadCompetidor();
  else if (name === "cliente") pfLoadCliente();
}

function pfApplyGlobalFilters() {
  PF.globalFechaDesde = document.getElementById("gFechaDesde")?.value || "";
  PF.globalFechaHasta = document.getElementById("gFechaHasta")?.value || "";
  pfUpdateDateSummary();

  if (PF.activeTab === "articulos") pfLoadArticulos();
  else if (PF.activeTab === "competidor") pfLoadCompetidor();
  else if (PF.activeTab === "cliente") pfLoadCliente();
}

function pfClearAll() {
  const globalFrom = document.getElementById("gFechaDesde");
  const globalTo = document.getElementById("gFechaHasta");
  if (globalFrom) globalFrom.value = "";
  if (globalTo) globalTo.value = "";

  PF.globalFechaDesde = "";
  PF.globalFechaHasta = "";

  [
    "artDescInput",
    "artMarcaInput",
    "artProvInput",
    "artRubroSel",
    "compProvInput",
    "compRubroSel",
    "compDescInput",
    "cliCompInput",
    "cliPlatSel",
    "cliProvSel",
    "cliProcesoInput",
  ].forEach((id) => {
    const el = document.getElementById(id);
    if (!el) return;
    el.value = "";
  });

  pfSyncLockedPlatformUI();
  pfUpdateDateSummary();

  if (PF.activeTab === "articulos") pfLoadArticulos();
  else if (PF.activeTab === "competidor") pfLoadCompetidor();
  else if (PF.activeTab === "cliente") pfLoadCliente();
}

function pfTypeahead(inputEl, campo, dropId) {
  const q = inputEl.value.trim();
  clearTimeout(PF.typeaheadTimers[dropId]);

  const drop = document.getElementById(dropId);
  if (!drop) return;

  if (q.length < 2) {
    drop.style.display = "none";
    return;
  }

  PF.typeaheadTimers[dropId] = setTimeout(async () => {
    try {
      const params = new URLSearchParams({ campo, q });
      const data = await pfFetch(`${BASE}/filtros/search?${params}`);
      const list = drop.querySelector("ul");
      if (!list) return;

      list.innerHTML = data.slice(0, 40).map((value) => {
        return `<li onclick="pfSelectTypeahead('${inputEl.id}','${dropId}',this)" data-val="${pfEscAttr(value)}">${pfEsc(value)}</li>`;
      }).join("");

      drop.style.display = data.length ? "block" : "none";
    } catch (err) {
      drop.style.display = "none";
    }
  }, 240);
}

function pfSelectTypeahead(inputId, dropId, li) {
  const input = document.getElementById(inputId);
  const drop = document.getElementById(dropId);
  if (input) input.value = li.getAttribute("data-val") || "";
  if (drop) drop.style.display = "none";
}

document.addEventListener("click", (event) => {
  if (event.target.closest(".pf-typeahead")) return;
  document.querySelectorAll(".pf-dropdown").forEach((dropdown) => {
    dropdown.style.display = "none";
  });
});

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

    ["artRubroSel", "compRubroSel"].forEach((id) => {
      const sel = document.getElementById(id);
      if (!sel) return;
      const values = data.rubros || [];
      sel.innerHTML = ['<option value="">Todos</option>']
        .concat(values.map((value) => `<option value="${pfEscAttr(value)}">${pfEsc(value)}</option>`))
        .join("");
    });

    const platSel = document.getElementById("cliPlatSel");
    if (platSel) {
      const values = data.plataformas || [];
      platSel.innerHTML = ['<option value="">Todas</option>']
        .concat(values.map((value) => `<option value="${pfEscAttr(value)}">${pfEsc(value)}</option>`))
        .join("");
    }

    const provSel = document.getElementById("cliProvSel");
    if (provSel) {
      const values = data.provincias || [];
      provSel.innerHTML = ['<option value="">Todas</option>']
        .concat(values.map((value) => `<option value="${pfEscAttr(value)}">${pfEsc(value)}</option>`))
        .join("");
    }

    pfSyncLockedPlatformUI();
  } catch (err) {
    console.warn("[PF] No se pudieron cargar opciones de filtros:", err.message);
  }
}

function pfGetArtParams() {
  const params = pfGlobalParams();
  const desc = document.getElementById("artDescInput")?.value.trim() || "";
  const marca = document.getElementById("artMarcaInput")?.value.trim() || "";
  const proveedor = document.getElementById("artProvInput")?.value.trim() || "";
  const rubro = document.getElementById("artRubroSel")?.value || "";

  if (desc) params.set("descripcion", desc);
  if (marca) params.set("marca", marca);
  if (proveedor) params.set("proveedor", proveedor);
  if (rubro) params.set("rubro", rubro);
  return params;
}

async function pfLoadArticulos() {
  const params = pfGetArtParams();
  await Promise.all([
    pfLoadArtKpis(params),
    pfLoadArtEvolucion(params),
    pfLoadArtPorMarca(params),
    pfLoadArtPorProveedor(params),
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
        tone: "success",
        label: "Cant. ofertada",
        value: pfKpiNum(data.cantidad_ofertada),
        rawValue: pfFmt(data.cantidad_ofertada),
        sub: "Cotizado en el periodo",
        icon: "bi-box2-heart",
      }),
      pfKpiCard({
        tone: "warning",
        label: "Marcas distintas",
        value: pfFmt(data.marcas_distintas),
        sub: "Diversidad de marca",
        icon: "bi-tags",
      }),
      pfKpiCard({
        tone: "neutral",
        label: "Mejor posicion",
        value: data.mejor_posicion != null ? String(data.mejor_posicion) : "-",
        sub: `${pfFmt(data.procesos)} procesos`,
        icon: "bi-trophy",
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
        series: [{ name: "Precio prom. unitario", data: data.map((d) => d.avg_precio) }],
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
          { name: "Cant. ofertada", data: data.map((d) => d.cantidad_ofertada) },
        ],
        xaxis: { categories: labels, labels: { rotate: -28 } },
        yaxis: { labels: { formatter: (value) => pfFmtNumShort(value) } },
        plotOptions: {
          bar: {
            borderRadius: 6,
            columnWidth: "52%",
          },
        },
        colors: [COLORS.brand500, COLORS.success],
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

    PF.charts.artMarcaChart = new ApexCharts(
      document.getElementById("artMarcaChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Precio prom. unitario", data: data.map((d) => d.avg_precio) }],
        xaxis: { categories: data.map((d) => pfTrunc(d.marca, 20)) },
        yaxis: { labels: { formatter: (value) => pfFmtShort(value) } },
        plotOptions: {
          bar: {
            borderRadius: 6,
            columnWidth: "48%",
          },
        },
        colors: [COLORS.cyan],
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
      if (tbody) tbody.innerHTML = pfTableEmpty("bi-search", "Sin proveedores para este articulo.", 7);
      return;
    }

    const top = data.slice(0, 10);
    PF.charts.artProvChart = new ApexCharts(
      document.getElementById("artProvChart"),
      pfChartBase({
        chart: { type: "bar", height: 256 },
        series: [{ name: "Monto ofertado total", data: top.map((d) => d.total_ofertado) }],
        xaxis: {
          categories: top.map((d) => pfTrunc(d.proveedor, 22)),
          labels: { maxHeight: 72, formatter: pfAxisFmtMoney },
        },
        yaxis: { labels: { formatter: pfAxisFmtMoney } },
        plotOptions: {
          bar: {
            horizontal: true,
            borderRadius: 7,
            barHeight: "70%",
          },
        },
        colors: [COLORS.brand500],
        tooltip: {
          y: { formatter: (value) => pfPeso(value) },
        },
      })
    );
    PF.charts.artProvChart.render();

    if (tbody) {
      tbody.innerHTML = data.map((d) => {
        const posClass = d.mejor_posicion === 1 ? "pos1" : d.mejor_posicion === 2 ? "pos2" : "posn";
        return `
          <tr>
            <td>${pfCellText(d.proveedor, 48)}</td>
            <td class="num">${pfPeso(d.avg_precio)}</td>
            <td class="num">${pfFmt(d.posicion_promedio, 1)}</td>
            <td class="num"><span class="pf-badge ${posClass}">#${d.mejor_posicion ?? "-"}</span></td>
            <td class="num">${pfPeso(d.total_ofertado)}</td>
            <td class="num">${pfFmt(d.procesos)}</td>
            <td class="num">${pfFmt(d.count)}</td>
          </tr>`;
      }).join("");
    }
  } catch (err) {
    pfRenderEmpty("artProvChart", "Error al cargar proveedores.", "bi-buildings");
    if (tbody) tbody.innerHTML = pfTableEmpty("bi-exclamation-circle", "Error al cargar el detalle de proveedores.", 7);
  }
}

function pfGetCompParams() {
  const params = pfGlobalParams();
  const proveedor = document.getElementById("compProvInput")?.value.trim() || "";
  const rubro = document.getElementById("compRubroSel")?.value || "";
  const descripcion = document.getElementById("compDescInput")?.value.trim() || "";

  if (proveedor) params.set("proveedor", proveedor);
  if (rubro) params.set("rubro", rubro);
  if (descripcion) params.set("descripcion", descripcion);
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
  const comprador = document.getElementById("cliCompInput")?.value.trim() || "";
  const plataforma = document.getElementById("cliPlatSel")?.value || "";
  const provincia = document.getElementById("cliProvSel")?.value || "";
  const proceso = document.getElementById("cliProcesoInput")?.value.trim() || "";

  if (comprador) params.set("comprador", comprador);
  if (plataforma && !PF.globalPlataforma) params.set("plataforma", plataforma);
  if (provincia) params.set("provincia", provincia);
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
  pfUpdateDateSummary();
  await pfLoadFilterOptions();
  await pfCheckSync();
  pfLoadArticulos();
});
