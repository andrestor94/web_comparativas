(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));
  const EMPTY = "--";

  const numberOrZero = (v) => {
    const n = Number(v);
    return Number.isFinite(n) ? n : 0;
  };
  const isFilled = (v) => v !== undefined && v !== null && `${v}`.trim() !== "";
  const pick = (obj, keys, fallback = "") => {
    for (const key of keys) {
      if (obj && isFilled(obj[key])) return obj[key];
    }
    return fallback;
  };
  const pickFrom = (sources, keys, fallback = "") => {
    for (const source of sources) {
      const value = pick(source, keys, undefined);
      if (isFilled(value)) return value;
    }
    return fallback;
  };
  const arrayify = (value) => {
    if (Array.isArray(value)) return value;
    if (!isFilled(value)) return [];
    if (typeof value === "string") {
      return value.split(",").map((x) => x.trim()).filter(Boolean);
    }
    return [value];
  };
  const unique = (items) => [...new Set(items.filter(Boolean))];
  const esc = (value) => `${value ?? ""}`
    .replaceAll("&", "&amp;")
    .replaceAll("<", "&lt;")
    .replaceAll(">", "&gt;")
    .replaceAll('"', "&quot;")
    .replaceAll("'", "&#39;");

  const fmtInt = (v) => numberOrZero(v).toLocaleString("es-AR");
  const fmtDecimal = (v, d = 1) => numberOrZero(v).toLocaleString("es-AR", { minimumFractionDigits: d, maximumFractionDigits: d });
  const fmtPercentMaybe = (v, d = 1) => fmtDecimal(numberOrZero(v) <= 1 ? numberOrZero(v) * 100 : numberOrZero(v), d);
  const parseDate = (value) => {
    if (!isFilled(value)) return null;
    const d = value instanceof Date ? value : new Date(value);
    return Number.isNaN(d.getTime()) ? null : d;
  };
  const fmtDateTime = (value) => {
    const d = parseDate(value);
    return d ? d.toLocaleString("es-AR", { day: "2-digit", month: "2-digit", year: "numeric", hour: "2-digit", minute: "2-digit" }) : EMPTY;
  };
  const fmtDate = (value) => {
    const d = parseDate(value);
    return d ? d.toLocaleDateString("es-AR") : EMPTY;
  };
  const fmtRelative = (value) => {
    const d = parseDate(value);
    if (!d) return EMPTY;
    const diffMs = Date.now() - d.getTime();
    if (diffMs < 0) return "recien";
    const mins = Math.round(diffMs / 60000);
    if (mins < 1) return "< 1 min";
    if (mins < 60) return `${mins} min`;
    const hours = Math.round(mins / 60);
    if (hours < 24) return `${hours} h`;
    const days = Math.round(hours / 24);
    return `${days} d`;
  };
  const fmtHours = (value) => {
    const hours = numberOrZero(value);
    if (!hours) return "0 h";
    if (hours < 1) return `${Math.max(1, Math.round(hours * 60))} min`;
    return `${fmtDecimal(hours, 1)} h`;
  };
  const minutesToHours = (value) => numberOrZero(value) / 60;
  const boolish = (value) => value === true || value === 1 || value === "1" || `${value}`.toLowerCase() === "true";

  const scoreInfo = (score) => {
    const n = Math.max(0, Math.min(100, Math.round(numberOrZero(score))));
    if (n >= 80) return { color: "#22c55e", tone: "text-success", label: "Referente" };
    if (n >= 50) return { color: "#3b82f6", tone: "text-info", label: "Consolidado" };
    if (n >= 30) return { color: "#f59e0b", tone: "text-warning", label: "En progreso" };
    return { color: "#ef4444", tone: "text-danger", label: "Baja adopcion" };
  };
  const normalizeSeverity = (raw) => {
    const value = `${raw || "info"}`.toLowerCase();
    if (value.includes("alta") || value.includes("high") || value.includes("critical") || value.includes("danger")) return { key: "alta", className: "sev-alta" };
    if (value.includes("media") || value.includes("medium") || value.includes("warn")) return { key: "media", className: "sev-media" };
    if (value.includes("baja") || value.includes("low")) return { key: "baja", className: "sev-baja" };
    return { key: "info", className: "sev-info" };
  };
  const normalizeRisk = (raw, inactiveFlag = false) => {
    const value = `${raw || ""}`.toLowerCase();
    if (inactiveFlag || value.includes("alto") || value.includes("high") || value.includes("crit")) return { label: "Alto", className: "risk-alto status-pill is-risk" };
    if (value.includes("medio") || value.includes("med")) return { label: "Medio", className: "risk-medio status-pill is-idle" };
    return { label: "Bajo", className: "risk-bajo status-pill is-live" };
  };
  const normalizeStatus = (raw, lastActivity) => {
    const value = `${raw || ""}`.toLowerCase();
    const rel = fmtRelative(lastActivity);
    if (value.includes("ausente")) return { label: "Ausente", className: "status-pill is-offline", dot: "offline" };
    if (value.includes("sin se") || value.includes("offline") || value.includes("descon") || value.includes("fuera")) return { label: "Fuera de monitoreo", className: "status-pill is-offline", dot: "offline" };
    if (value.includes("off") || value.includes("descon") || value.includes("fuera")) return { label: "Offline", className: "status-pill is-offline", dot: "offline" };
    if (value.includes("idle") || value.includes("ocioso") || value.includes("inactivo")) return { label: "Inactivo", className: "status-pill is-idle", dot: "inactivo" };
    if (rel !== EMPTY) {
      const d = parseDate(lastActivity);
      if (d && Date.now() - d.getTime() > 120000) return { label: "Inactivo", className: "status-pill is-idle", dot: "inactivo" };
    }
    return { label: raw || "Activo", className: "status-pill is-live", dot: "activo" };
  };
  const normalizeActivity = (raw) => {
    const value = `${raw || "activo"}`.toLowerCase();
    if (value.includes("search") || value.includes("busq")) return { label: "buscando", className: "ab-buscando" };
    if (value.includes("upload") || value.includes("carga")) return { label: "cargando", className: "ab-cargando" };
    if (value.includes("export") || value.includes("download") || value.includes("descarga")) return { label: "exportando", className: "ab-exportando" };
    if (value.includes("edit")) return { label: "editando", className: "ab-editando" };
    if (value.includes("nav") || value.includes("view") || value.includes("modulo")) return { label: "navegando", className: "ab-navegando" };
    return { label: value || "activo", className: "ab-activo" };
  };
  const normalizeTrend = (rawValue, rawLabel) => {
    const n = numberOrZero(rawValue);
    const label = isFilled(rawLabel) ? `${rawLabel}` : `${n > 0 ? "+" : ""}${fmtDecimal(n, 1)}%`;
    if (n > 1) return { className: "trend-up", icon: "bi-arrow-up-right", label };
    if (n < -1) return { className: "trend-down", icon: "bi-arrow-down-right", label };
    return { className: "trend-stable", icon: "bi-dash", label: isFilled(rawLabel) ? `${rawLabel}` : "Estable" };
  };

  const tabButtons = $$("[data-usage-tab-target]");
  const tabPanels = $$("[data-usage-tab-panel]");
  const activateTab = (targetSelector) => {
    tabButtons.forEach((btn) => btn.classList.toggle("active", btn.dataset.usageTabTarget === targetSelector));
    tabPanels.forEach((panel) => {
      const active = `#${panel.id}` === targetSelector;
      panel.classList.toggle("show", active);
      panel.classList.toggle("active", active);
    });
  };
  tabButtons.forEach((btn) => btn.addEventListener("click", (evt) => {
    evt.preventDefault();
    if (btn.dataset.usageTabTarget) activateTab(btn.dataset.usageTabTarget);
  }));
  if (tabButtons.length) activateTab(tabButtons[0].dataset.usageTabTarget);

  const filtersForm = $("#usage-filters-form");
  const applyBtn = $("#usage-apply-filters");
  const roleFilter = $("#flt-role");
  const teamFilter = $("#flt-team");
  const liveTbody = $("#usage-live-tbody");
  const liveSpinner = $("#live-spinner");
  const liveCountBadge = $("#live-count-badge");
  const liveLastUpdate = $("#live-last-update");
  const alertsContainer = $("#alerts-container");
  const navAlertsBadge = $("#nav-alerts-badge");
  const byUserTbody = $("#su-users-byuser-tbody");
  const kpiState = $("#usage-kpi-state");

  const cardRefs = {
    activeUsers: $("#card-active-users"),
    activeHours: $("#card-active-hours"),
    liveUsers: $("#card-live-users"),
    adoption: $("#card-adoption-val"),
    inactive: $("#card-inactive-users"),
    productivity: $("#card-prod-index"),
  };

  let chartWeekday = null;
  let chartRoles = null;
  let chartSections = null;
  let chartHeatmap = null;
  let byUserRowsCache = [];
  let liveRowsCache = [];
  const weekdayLabels = ["Lun", "Mar", "Mie", "Jue", "Vie", "Sab", "Dom"];
  const chartDestroyers = {
    "usage-chart-weekday": () => { if (chartWeekday) { chartWeekday.destroy(); chartWeekday = null; } },
    "usage-chart-roles": () => { if (chartRoles) { chartRoles.destroy(); chartRoles = null; } },
    "usage-chart-sections": () => { if (chartSections) { chartSections.destroy(); chartSections = null; } },
    "usage-chart-heatmap": () => { if (chartHeatmap) { chartHeatmap.destroy(); chartHeatmap = null; } },
  };
  const stateMessage = (block, state) => {
    const blockName = block === "alerts" ? "estas alertas" : (block === "by_user" ? "esta vista" : (block === "kpis" ? "estos indicadores" : "esta seccion"));
    if (state === "loading") return block === "alerts" ? "Calculando alertas del periodo..." : (block === "by_user" ? "Cargando vista por usuario..." : (block === "kpis" ? "Actualizando indicadores del periodo..." : "Cargando datos del periodo..."));
    if (state === "filter_empty") return "No hay usuarios que coincidan con los filtros aplicados.";
    if (state === "no_activity") return "No hubo actividad de usuarios en el periodo seleccionado.";
    if (state === "empty") return block === "alerts" ? "No se detectaron alertas en este periodo." : "No hay datos para mostrar en el periodo seleccionado.";
    if (state === "error") return `No pudimos cargar ${blockName}. Reintentá o revisá logs del backend.`;
    return "";
  };
  const setTableState = (tbody, colspan, message) => {
    if (!tbody) return;
    tbody.innerHTML = `<tr><td colspan="${colspan}" class="usage-empty">${esc(message)}</td></tr>`;
  };
  const setKpiNote = (message, tone = "muted") => {
    if (!kpiState) return;
    kpiState.textContent = message;
    kpiState.className = `small mb-4 ${tone === "error" ? "text-danger" : "text-white-50"}`;
  };
  const resetMetricCards = (placeholder = "0") => {
    if (cardRefs.activeUsers) cardRefs.activeUsers.textContent = placeholder;
    if (cardRefs.activeHours) cardRefs.activeHours.textContent = placeholder === "--" ? "--" : "0 h";
    if (cardRefs.adoption) cardRefs.adoption.textContent = placeholder;
    if (cardRefs.inactive) cardRefs.inactive.textContent = placeholder;
    if (cardRefs.productivity) cardRefs.productivity.textContent = placeholder === "--" ? "--" : "0,00";
  };
  const setChartMessage = (elementId, message) => {
    chartDestroyers[elementId]?.();
    const el = document.getElementById(elementId);
    if (el) el.innerHTML = `<div class="usage-empty">${esc(message)}</div>`;
  };
  const renderChartStates = (state) => {
    const message = stateMessage("charts", state);
    ["usage-chart-weekday", "usage-chart-roles", "usage-chart-heatmap", "usage-chart-sections"].forEach((id) => setChartMessage(id, message));
  };
  const renderChartSafely = (elementId, fn) => {
    try {
      fn();
    } catch (error) {
      console.error(`[SeguimientoUsuarios] chart ${elementId}`, error);
      setChartMessage(elementId, "No pudimos renderizar este grafico. Reintentá o revisá logs del frontend.");
    }
  };
  const weekdayLabelFor = (value) => {
    const idx = Number(value);
    if (Number.isInteger(idx) && idx >= 0 && idx < weekdayLabels.length) return weekdayLabels[idx];
    return isFilled(value) ? `${value}` : "Sin dato";
  };
  const populateTeamOptions = (groups) => {
    if (!teamFilter) return;
    const selected = teamFilter.value || "";
    const rows = Array.isArray(groups) ? groups : [];
    const options = ['<option value="">Todos los equipos</option>'];
    const seen = new Set();
    rows.forEach((group) => {
      const value = pick(group, ["value", "name", "label"], "");
      if (!value || seen.has(value)) return;
      seen.add(value);
      const users = numberOrZero(pick(group, ["users", "count"], 0));
      const label = users > 0 ? `${value} (${users})` : value;
      options.push(`<option value="${esc(value)}">${esc(label)}</option>`);
    });
    if (selected && !seen.has(selected)) {
      options.push(`<option value="${esc(selected)}">${esc(selected)}</option>`);
    }
    teamFilter.innerHTML = options.join("");
    teamFilter.value = selected;
  };
  const populateRoleOptions = (roles) => {
    if (!roleFilter) return;
    const selected = roleFilter.value || "";
    const rows = Array.isArray(roles) ? roles : [];
    const options = ['<option value="">Todos los roles</option>'];
    const seen = new Set();
    rows.forEach((role) => {
      const value = pick(role, ["value", "role"], "");
      if (!value || seen.has(value)) return;
      seen.add(value);
      const users = numberOrZero(pick(role, ["users", "count"], 0));
      const label = pick(role, ["label"], value);
      options.push(`<option value="${esc(value)}">${esc(users > 0 ? `${label} (${users})` : label)}</option>`);
    });
    if (selected && !seen.has(selected)) {
      options.push(`<option value="${esc(selected)}">${esc(selected)}</option>`);
    }
    roleFilter.innerHTML = options.join("");
    roleFilter.value = selected;
  };
  const ensureApex = (el) => {
    if (!el) return false;
    if (window.ApexCharts) return true;
    el.innerHTML = '<div class="usage-empty">No se pudieron cargar los graficos.</div>';
    return false;
  };
  const chartBase = {
    toolbar: { show: false },
    fontFamily: "Outfit, -apple-system, BlinkMacSystemFont, 'Segoe UI', sans-serif",
    animations: { enabled: true, easing: "easeinout", speed: 500 },
  };
  const buildWeekdayChart = (items) => {
    const el = $("#usage-chart-weekday");
    if (!ensureApex(el)) return;
    if (!items || !items.length) {
      setChartMessage("usage-chart-weekday", "No hubo actividad suficiente para graficar por dia.");
      return;
    }
    const labels = items.map((it) => weekdayLabelFor(pick(it, ["weekday_label", "label", "weekday", "day"], "")));
    const values = items.map((it) => numberOrZero(pick(it, ["events", "count", "sessions"], 0)));
    const options = { chart: { type: "bar", height: 280, ...chartBase }, colors: ["#3b82f6"], series: [{ name: "Eventos", data: values }], xaxis: { categories: labels, labels: { style: { colors: "#94a3b8" } } }, yaxis: { labels: { style: { colors: "#94a3b8" } } }, plotOptions: { bar: { borderRadius: 4, columnWidth: "52%" } }, dataLabels: { enabled: false }, grid: { strokeDashArray: 4, borderColor: "rgba(148, 163, 184, 0.1)" }, tooltip: { theme: "dark" } };
    if (chartWeekday) chartWeekday.updateOptions({ series: options.series, xaxis: options.xaxis }); else { chartWeekday = new ApexCharts(el, options); chartWeekday.render(); }
  };
  const buildRolesChart = (items) => {
    const el = $("#usage-chart-roles");
    if (!ensureApex(el)) return;
    const rows = items || [];
    if (!rows.length) {
      setChartMessage("usage-chart-roles", "No hay usuarios con actividad suficiente para comparar en este periodo.");
      return;
    }
    const options = { chart: { type: "bar", height: 280, ...chartBase }, colors: ["#6366f1", "#60a5fa"], series: [{ name: "Eventos", data: rows.map((it) => numberOrZero(pick(it, ["events", "count"], 0))) }, { name: "Horas activas", data: rows.map((it) => numberOrZero(pick(it, ["active_hours", "hours"], 0))) }], xaxis: { categories: rows.map((it) => pick(it, ["user_label", "name", "label"], "")), labels: { style: { colors: "#94a3b8", fontSize: "11px" }, rotate: -45 } }, yaxis: { labels: { style: { colors: "#94a3b8" } } }, plotOptions: { bar: { borderRadius: 4, columnWidth: "58%" } }, dataLabels: { enabled: false }, grid: { strokeDashArray: 4, borderColor: "rgba(148, 163, 184, 0.1)" }, legend: { labels: { colors: "#cbd5e1" } }, tooltip: { theme: "dark", shared: true, intersect: false } };
    if (chartRoles) chartRoles.updateOptions({ series: options.series, xaxis: options.xaxis }); else { chartRoles = new ApexCharts(el, options); chartRoles.render(); }
  };
  const buildSectionsChart = (items) => {
    const el = $("#usage-chart-sections");
    if (!ensureApex(el)) return;
    const rows = items || [];
    if (!rows.length) {
      setChartMessage("usage-chart-sections", "No se registraron secciones usadas en el periodo seleccionado.");
      return;
    }
    const options = { chart: { type: "bar", height: 280, ...chartBase }, colors: ["#818cf8"], series: [{ name: "Eventos", data: rows.map((it) => numberOrZero(pick(it, ["events", "count"], 0))) }], xaxis: { categories: rows.map((it) => pick(it, ["section", "label", "name"], "")), labels: { style: { colors: "#94a3b8" } } }, yaxis: { labels: { style: { colors: "#94a3b8" } } }, plotOptions: { bar: { horizontal: true, borderRadius: 4, barHeight: "60%" } }, dataLabels: { enabled: false }, grid: { strokeDashArray: 4, borderColor: "rgba(148, 163, 184, 0.1)" }, tooltip: { theme: "dark" } };
    if (chartSections) chartSections.updateOptions({ series: options.series, xaxis: options.xaxis }); else { chartSections = new ApexCharts(el, options); chartSections.render(); }
  };
  const buildHeatmapChart = (items) => {
    const el = $("#usage-chart-heatmap");
    if (!ensureApex(el)) return;
    if (!items || !items.length) { setChartMessage("usage-chart-heatmap", "Aun no hay datos suficientes para construir el mapa de calor."); return; }
    const grouped = {};
    items.forEach((it) => {
      const day = weekdayLabelFor(pick(it, ["day_label", "weekday_label", "day", "weekday", "label"], "Otros"));
      const hour = pick(it, ["hour_label", "hour", "bucket"], 0);
      if (!grouped[day]) grouped[day] = {};
      grouped[day][hour] = numberOrZero(pick(it, ["value", "count", "events"], 0));
    });
    const hours = unique(Object.values(grouped).flatMap((obj) => Object.keys(obj))).sort((a, b) => numberOrZero(a) - numberOrZero(b));
    const series = Object.keys(grouped).map((day) => ({ name: day, data: hours.map((hour) => ({ x: `${hour}:00`, y: numberOrZero(grouped[day][hour]) })) }));
    const options = { chart: { type: "heatmap", height: 280, ...chartBase }, series, dataLabels: { enabled: false }, xaxis: { labels: { style: { colors: "#94a3b8" } } }, yaxis: { labels: { style: { colors: "#94a3b8" } } }, plotOptions: { heatmap: { radius: 4, shadeIntensity: .45, colorScale: { ranges: [{ from: 0, to: 0, color: "rgba(148, 163, 184, 0.1)" }, { from: 1, to: 10, color: "#1e40af" }, { from: 11, to: 50, color: "#3b82f6" }, { from: 51, to: 10000, color: "#60a5fa" }] } } }, tooltip: { theme: "dark" } };
    if (chartHeatmap) chartHeatmap.updateOptions({ series: options.series }); else { chartHeatmap = new ApexCharts(el, options); chartHeatmap.render(); }
  };

  const buildFrequencyLabel = (row) => {
    const label = pick(row, ["frequency_label", "frequency", "usage_frequency"], "");
    if (label) return label;
    const sessions = numberOrZero(pick(row, ["sessions"], 0));
    const days = Math.max(1, numberOrZero(pick(row, ["active_days", "days_active"], 0)));
    return `${fmtDecimal(sessions / days, 1)} ses/dia`;
  };
  const modulesFrom = (row) => {
    const direct = pick(row, ["modules_used_list", "visited_modules", "sections"], undefined);
    if (Array.isArray(direct)) return unique(direct.map((item) => typeof item === "string" ? item : pick(item, ["section", "name", "module"], "")).filter(Boolean)).slice(0, 3);
    const modules = row?.modules;
    if (Array.isArray(modules)) return unique(modules.map((item) => typeof item === "string" ? item : pick(item, ["section", "name", "module"], "")).filter(Boolean)).slice(0, 3);
    const numeric = numberOrZero(pick(row, ["modules_used"], 0));
    return numeric > 0 ? [`${numeric} modulos`] : [];
  };
  const normalizeLiveUser = (row) => {
    const lastActivity = pick(row, ["last_action_ts", "last_activity_at", "last_activity", "last_seen", "last_event_at", "last_action_at", "updated_at", "last_signal", "last_ping", "last_heartbeat"], "");
    return {
      userId: pick(row, ["user_id", "id"], ""),
      name: pick(row, ["name", "user", "username"], "Sin nombre"),
      email: pick(row, ["email"], ""),
      role: pick(row, ["role"], EMPTY),
      unit: pick(row, ["unit_business", "business_unit", "unit"], EMPTY),
      group: pick(row, ["group", "team", "squad"], EMPTY),
      section: pick(row, ["current_section", "section", "current_module", "route"], EMPTY),
      lastAction: pick(row, ["last_action", "action", "last_event", "last_event_name"], EMPTY),
      lastActivity,
      sessionStart: pick(row, ["session_start", "current_session_start", "login_at", "signed_in_at", "started_at"], ""),
      lastPing: pick(row, ["last_signal", "last_ping", "last_heartbeat", "heartbeat_at", "ping_at", "updated_at"], ""),
      status: normalizeStatus(pick(row, ["status", "current_status"], "Activo"), lastActivity),
      activity: normalizeActivity(pick(row, ["activity_type", "activity", "last_action_type", "event_type"], "activo")),
    };
  };
  const normalizeUserRow = (row) => {
    const downloads = numberOrZero(pick(row, ["downloads", "download_count", "exports"], 0));
    const activeHours = isFilled(row.active_hours) ? numberOrZero(row.active_hours) : minutesToHours(pick(row, ["active_minutes"], 0));
    return {
      userId: pick(row, ["user_id", "id"], ""),
      name: pick(row, ["name", "user", "username"], "Sin nombre"),
      role: pick(row, ["role"], EMPTY),
      unit: pick(row, ["unit_business", "business_unit", "unit"], EMPTY),
      group: pick(row, ["group", "team", "squad"], EMPTY),
      status: normalizeStatus(pick(row, ["current_status", "status"], "Activo"), pick(row, ["last_seen", "last_access", "last_activity"], "")),
      risk: normalizeRisk(pick(row, ["risk", "risk_level"], ""), boolish(pick(row, ["is_inactive_7d", "inactive_7d"], false))),
      score: numberOrZero(pick(row, ["adoption_score", "score", "health_score"], 0)),
      sessions: numberOrZero(pick(row, ["sessions"], 0)),
      activeDays: numberOrZero(pick(row, ["active_days", "days_active"], 0)),
      activeHours,
      views: numberOrZero(pick(row, ["module_views", "views", "page_views"], 0)),
      searches: numberOrZero(pick(row, ["searches", "search_count"], 0)),
      downloads,
      uploads: numberOrZero(pick(row, ["uploads", "upload_count", "files_uploaded"], 0)),
      modules: modulesFrom(row),
      frequency: buildFrequencyLabel(row),
      lastAccess: pick(row, ["last_seen", "last_access", "last_activity", "last_login"], ""),
      trend: normalizeTrend(pick(row, ["trend_delta_pct", "trend_pct", "trend_value", "trend_delta", "trend_vs_previous"], 0), pick(row, ["trend_label", "trend_text"], "")),
    };
  };

  const renderAlerts = (alerts, state = "ok") => {
    const rows = Array.isArray(alerts) ? alerts : [];
    if (navAlertsBadge) {
      navAlertsBadge.textContent = rows.length;
      navAlertsBadge.style.display = rows.length ? "inline-block" : "none";
    }
    if (!alertsContainer) return;
    if (state !== "ok") {
      alertsContainer.innerHTML = `<div class="col-12"><div class="usage-empty">${esc(stateMessage("alerts", state))}</div></div>`;
      return;
    }
    if (!rows.length) {
      alertsContainer.innerHTML = `<div class="col-12"><div class="usage-empty pe-none">${esc(stateMessage("alerts", "empty"))}</div></div>`;
      return;
    }
    alertsContainer.innerHTML = rows.map((raw) => {
      const sev = normalizeSeverity(pick(raw, ["severity", "level", "severidad", "type"], "info"));
      const user = pick(raw, ["user", "user_name", "name", "usuario"], "General");
      const reason = pick(raw, ["reason", "motivo", "title", "message"], "Sin detalle");
      const recommendation = pick(raw, ["recommendation", "recomendacion", "action", "suggestion"], "Revisar el caso y validar acompanamiento.");
      return `<div class="col-12 col-md-6 col-lg-4"><div class="alert-card ${sev.className}"><div class="alert-card-head"><div><div class="fw-semibold text-white">${esc(reason)}</div><div class="alert-card-user">Usuario: ${esc(user)}</div></div><span class="alert-severity-badge ${sev.className}">${esc(sev.key)}</span></div><div class="small text-white-50">Motivo detectado: ${esc(reason)}</div><div class="alert-rec">Recomendacion: ${esc(recommendation)}</div></div></div>`;
    }).join("");
  };

  const attachOpenProfile = (root, cache) => {
    $$("tr[data-user-index]", root).forEach((tr) => {
      tr.addEventListener("click", () => {
        const row = cache[numberOrZero(tr.dataset.userIndex)];
        if (row && row.userId) openUserProfile(row.userId);
      });
    });
  };

  const renderLiveUsers = (rowsRaw, state = "ok") => {
    if (!liveTbody) return;
    liveRowsCache = (rowsRaw || []).map(normalizeLiveUser);
    if (liveCountBadge) liveCountBadge.textContent = liveRowsCache.length;
    if (cardRefs.liveUsers) cardRefs.liveUsers.textContent = liveRowsCache.length;
    if (state === "error") {
      setTableState(liveTbody, 11, "No pudimos cargar el monitoreo en vivo. Reintentá o revisá logs del backend.");
      return;
    }
    if (!liveRowsCache.length) {
      setTableState(liveTbody, 11, "No hay usuarios conectados en este momento.");
      return;
    }
    liveTbody.innerHTML = liveRowsCache.map((row, index) => `
      <tr data-user-index="${index}" ${row.userId ? 'style="cursor:pointer;"' : ""}>
        <td><div class="table-meta"><strong>${esc(row.name)}</strong>${row.email ? `<small>${esc(row.email)}</small>` : ""}</div></td>
        <td>${esc(row.role)}</td>
        <td>${esc(row.unit)}</td>
        <td>${esc(row.group)}</td>
        <td>${esc(row.section)}</td>
        <td>${esc(row.lastAction)}</td>
        <td class="text-end">${esc(fmtRelative(row.lastActivity))}</td>
        <td><span class="${row.status.className}"><span class="status-dot ${row.status.dot}"></span>${esc(row.status.label)}</span></td>
        <td>${esc(fmtDateTime(row.sessionStart))}</td>
        <td>${esc(fmtDateTime(row.lastPing))}</td>
        <td><span class="activity-badge ${row.activity.className}">${esc(row.activity.label)}</span></td>
      </tr>`).join("");
    attachOpenProfile(liveTbody, liveRowsCache);
  };

  const renderByUserTable = (rowsRaw, state = "ok") => {
    if (!byUserTbody) return;
    byUserRowsCache = (rowsRaw || []).map(normalizeUserRow);
    if (state !== "ok") {
      setTableState(byUserTbody, 18, stateMessage("by_user", state));
      return;
    }
    if (!byUserRowsCache.length) {
      setTableState(byUserTbody, 18, "No hay usuarios elegibles para mostrar en este periodo.");
      return;
    }
    byUserTbody.innerHTML = byUserRowsCache.map((row, index) => {
      const score = scoreInfo(row.score);
      const modules = row.modules.length ? row.modules.map((m) => `<span class="metric-pill">${esc(m)}</span>`).join("") : '<span class="text-white-50 small">Sin modulos</span>';
      return `
        <tr data-user-index="${index}" style="cursor:pointer;" class="align-middle">
          <td><div class="${score.tone} fw-bold">${fmtInt(row.score)}</div><div class="score-bar-wrap"><div class="score-bar-fill" style="width:${Math.min(100, row.score)}%;background:${score.color};"></div></div></td>
          <td><div class="table-meta"><strong>${esc(row.name)}</strong></div></td>
          <td>${esc(row.role)}</td>
          <td>${esc(row.unit)}</td>
          <td>${esc(row.group)}</td>
          <td><span class="${row.status.className}"><span class="status-dot ${row.status.dot}"></span>${esc(row.status.label)}</span></td>
          <td class="text-center"><span class="${row.risk.className}">${esc(row.risk.label)}</span></td>
          <td class="text-end">${fmtInt(row.sessions)}</td>
          <td class="text-end">${fmtInt(row.activeDays)}</td>
          <td class="text-end">${esc(fmtHours(row.activeHours))}</td>
          <td class="text-end">${fmtInt(row.views)}</td>
          <td class="text-end">${fmtInt(row.searches)}</td>
          <td class="text-end">${fmtInt(row.downloads)}</td>
          <td class="text-end">${fmtInt(row.uploads)}</td>
          <td><div class="table-chip-list">${modules}</div></td>
          <td>${esc(row.frequency)}</td>
          <td class="text-end">${esc(fmtDateTime(row.lastAccess))}</td>
          <td class="text-center"><span class="${row.trend.className}"><i class="bi ${row.trend.icon} me-1"></i>${esc(row.trend.label)}</span></td>
        </tr>`;
    }).join("");
    attachOpenProfile(byUserTbody, byUserRowsCache);
  };
  const offcanvasEl = document.getElementById("userProfileOffcanvas");
  const profileOffcanvas = window.bootstrap && offcanvasEl ? new bootstrap.Offcanvas(offcanvasEl) : null;
  const profileRefs = {
    loading: $("#profile-loading"),
    content: $("#profile-content"),
    name: $("#prof-name"), email: $("#prof-email"), role: $("#prof-role"), bu: $("#prof-bu"), group: $("#prof-group"),
    scoreBadge: $("#prof-score-badge"), statusBadge: $("#prof-current-status-badge"), riskBadge: $("#prof-risk-badge"),
    onlineBadge: $("#prof-online-badge"), onlineSection: $("#prof-online-section"),
    statSessions: $("#prof-stat-sessions"), statHours: $("#prof-stat-hours"), statDays: $("#prof-stat-days"), statViews: $("#prof-stat-views"),
    statSearches: $("#prof-stat-searches"), statUploads: $("#prof-stat-uploads"), statExports: $("#prof-stat-exports"), statDownloads: $("#prof-stat-downloads"), statModules: $("#prof-stat-modules"),
    currentStatus: $("#prof-current-status"), created: $("#prof-created"), lastAccess: $("#prof-last-access"), lastAccessPill: $("#prof-last-access-pill"),
    frequency: $("#prof-frequency"), evolution: $("#prof-usage-evolution"), recentSummary: $("#prof-recent-sessions-summary"),
    modules: $("#prof-modules"), searchCount: $("#prof-search-count"), uploadCount: $("#prof-upload-count"), downloadCount: $("#prof-download-count"), exportCount: $("#prof-export-count"), activeTime: $("#prof-active-time"),
    sessions: $("#prof-recent-sessions"), sessionsNote: $("#prof-sessions-note"), alerts: $("#prof-alerts"), timeline: $("#prof-timeline"), timelineBadge: $("#prof-timeline-badge"),
  };

  const profileValue = (profile, keys, fallback = "") => pickFrom([profile || {}, profile?.summary || {}, profile?.metrics || {}, profile?.stats || {}, profile?.kpis || {}], keys, fallback);
  const profileArray = (profile, keys) => {
    for (const source of [profile || {}, profile?.summary || {}, profile?.metrics || {}, profile?.stats || {}]) {
      for (const key of keys) {
        const value = source?.[key];
        if (Array.isArray(value)) return value;
      }
    }
    return [];
  };
  const timelineIcon = (raw) => {
    const value = `${raw || ""}`.toLowerCase();
    if (value.includes("search") || value.includes("busq")) return "bi-search text-info";
    if (value.includes("upload") || value.includes("carga")) return "bi-cloud-upload text-success";
    if (value.includes("download") || value.includes("export")) return "bi-box-arrow-up-right text-warning";
    if (value.includes("view") || value.includes("nav") || value.includes("section")) return "bi-eye text-primary";
    return "bi-cursor text-secondary";
  };
  const normalizeProfile = (profile) => {
    const currentState = profile?.current_status || {};
    const stats = profile?.stats || {};
    const modulesRaw = Array.isArray(profile?.modules) ? profile.modules : (Array.isArray(stats?.modules_used_list) ? stats.modules_used_list : []);
    const modules = unique(modulesRaw.map((item) => typeof item === "string" ? item : pick(item, ["section", "name", "module"], "")).filter(Boolean));
    const recentSessions = profileArray(profile, ["recent_sessions", "sessions_recent", "last_sessions", "sessions"]);
    const timeline = profileArray(profile, ["timeline", "recent_activity", "events", "activity_timeline"]);
    const alerts = profileArray(profile, ["alerts", "associated_alerts", "risk_alerts"]);
    const activeHours = isFilled(profileValue(profile, ["active_hours"], undefined)) ? numberOrZero(profileValue(profile, ["active_hours"], 0)) : minutesToHours(profileValue(profile, ["active_minutes"], 0));
    return {
      userId: profileValue(profile, ["user_id", "id"], ""),
      name: profileValue(profile, ["name", "user", "username"], "Sin nombre"),
      email: profileValue(profile, ["email"], EMPTY),
      role: profileValue(profile, ["role"], EMPTY),
      unit: profileValue(profile, ["unit_business", "business_unit", "unit"], EMPTY),
      group: profileValue(profile, ["group", "team", "squad"], ""),
      score: numberOrZero(profileValue(profile, ["adoption_score", "score", "health_score"], 0)),
      status: normalizeStatus(pick(currentState, ["status"], profileValue(profile, ["current_status", "status"], "Activo")), pick(currentState, ["last_signal", "last_action_ts"], profileValue(profile, ["last_access", "last_seen", "last_activity"], ""))),
      risk: normalizeRisk(profileValue(profile, ["risk", "risk_level"], ""), boolish(profileValue(profile, ["is_inactive_7d"], false))),
      createdAt: profileValue(profile, ["created_at", "signup_date", "created"], ""),
      lastAccess: pick(currentState, ["last_signal", "last_action_ts"], profileValue(profile, ["last_access", "last_seen", "last_activity", "last_login"], "")),
      currentSection: pick(currentState, ["current_section"], profileValue(profile, ["current_section", "section", "current_module"], "")),
      sessions: numberOrZero(profileValue(profile, ["sessions"], 0)),
      activeDays: numberOrZero(profileValue(profile, ["active_days", "days_active"], 0)),
      activeHours,
      views: numberOrZero(profileValue(profile, ["module_views", "views", "page_views"], 0)),
      searches: numberOrZero(profileValue(profile, ["searches", "search_count"], 0)),
      uploads: numberOrZero(profileValue(profile, ["uploads", "upload_count", "files_uploaded"], 0)),
      downloads: numberOrZero(profileValue(profile, ["downloads", "download_count", "exports"], 0)),
      exports: numberOrZero(profileValue(profile, ["exports", "export_count"], 0)),
      frequency: buildFrequencyLabel(profile),
      evolution: normalizeTrend(profileValue(profile, ["trend_delta_pct", "trend_pct", "trend_delta", "trend_vs_previous"], 0), profileValue(profile, ["trend_label", "trend_text"], "")),
      modules, recentSessions, timeline, alerts,
    };
  };
  const renderProfileList = (target, rows, emptyText, mapper) => {
    if (!target) return;
    if (!rows.length) { target.innerHTML = `<div class="usage-empty py-3">${esc(emptyText)}</div>`; return; }
    target.innerHTML = rows.map(mapper).join("");
  };
  const renderProfile = (rawProfile) => {
    const profile = normalizeProfile(rawProfile || {});
    const score = scoreInfo(profile.score);
    if (profileRefs.name) profileRefs.name.textContent = profile.name;
    if (profileRefs.email) profileRefs.email.textContent = profile.email || EMPTY;
    if (profileRefs.role) profileRefs.role.textContent = profile.role || EMPTY;
    if (profileRefs.bu) profileRefs.bu.textContent = profile.unit || EMPTY;
    if (profileRefs.group) { profileRefs.group.textContent = profile.group || ""; profileRefs.group.style.display = profile.group ? "inline-flex" : "none"; }
    if (profileRefs.scoreBadge) { profileRefs.scoreBadge.textContent = `Score ${fmtInt(profile.score)} - ${score.label}`; profileRefs.scoreBadge.style.background = `${score.color}22`; profileRefs.scoreBadge.style.color = score.color; }
    if (profileRefs.statusBadge) profileRefs.statusBadge.textContent = profile.status.label;
    if (profileRefs.riskBadge) { profileRefs.riskBadge.textContent = `Riesgo ${profile.risk.label}`; profileRefs.riskBadge.style.display = "inline-flex"; profileRefs.riskBadge.className = `sic-badge border-0 ${profile.risk.className}`; }
    if (profileRefs.onlineBadge) profileRefs.onlineBadge.style.display = profile.status.dot === "activo" ? "block" : "none";
    if (profileRefs.onlineSection) profileRefs.onlineSection.textContent = profile.currentSection ? `En ${profile.currentSection}` : "";
    if (profileRefs.statSessions) profileRefs.statSessions.textContent = fmtInt(profile.sessions);
    if (profileRefs.statHours) profileRefs.statHours.textContent = fmtHours(profile.activeHours);
    if (profileRefs.statDays) profileRefs.statDays.textContent = fmtInt(profile.activeDays);
    if (profileRefs.statViews) profileRefs.statViews.textContent = fmtInt(profile.views);
    if (profileRefs.statSearches) profileRefs.statSearches.textContent = fmtInt(profile.searches);
    if (profileRefs.statUploads) profileRefs.statUploads.textContent = fmtInt(profile.uploads);
    if (profileRefs.statExports) profileRefs.statExports.textContent = fmtInt(profile.exports);
    if (profileRefs.statDownloads) profileRefs.statDownloads.textContent = fmtInt(profile.downloads);
    if (profileRefs.statModules) profileRefs.statModules.textContent = fmtInt(profile.modules.length);
    if (profileRefs.currentStatus) profileRefs.currentStatus.textContent = profile.status.label;
    if (profileRefs.created) profileRefs.created.textContent = fmtDate(profile.createdAt);
    if (profileRefs.lastAccess) profileRefs.lastAccess.textContent = fmtDateTime(profile.lastAccess);
    if (profileRefs.lastAccessPill) profileRefs.lastAccessPill.textContent = `Ultimo acceso: ${fmtDateTime(profile.lastAccess)}`;
    if (profileRefs.frequency) profileRefs.frequency.textContent = profile.frequency;
    if (profileRefs.evolution) profileRefs.evolution.innerHTML = `<span class="${profile.evolution.className}"><i class="bi ${profile.evolution.icon} me-1"></i>${esc(profile.evolution.label)}</span>`;
    if (profileRefs.recentSummary) profileRefs.recentSummary.textContent = `${profile.recentSessions.length} registradas`;
    if (profileRefs.modules) profileRefs.modules.innerHTML = profile.modules.length ? profile.modules.map((m) => `<span class="profile-chip">${esc(m)}</span>`).join("") : '<span class="profile-note">Sin modulos visitados en el periodo.</span>';
    if (profileRefs.searchCount) profileRefs.searchCount.textContent = fmtInt(profile.searches);
    if (profileRefs.uploadCount) profileRefs.uploadCount.textContent = fmtInt(profile.uploads);
    if (profileRefs.downloadCount) profileRefs.downloadCount.textContent = fmtInt(profile.downloads);
    if (profileRefs.exportCount) profileRefs.exportCount.textContent = fmtInt(profile.exports);
    if (profileRefs.activeTime) profileRefs.activeTime.textContent = fmtHours(profile.activeHours);
    if (profileRefs.sessionsNote) profileRefs.sessionsNote.textContent = `${profile.recentSessions.length} sesiones recientes`;

    renderProfileList(profileRefs.sessions, profile.recentSessions, "Sin sesiones recientes.", (session) => {
      const start = pick(session, ["start", "started_at", "session_start", "login_at"], "");
      const end = pick(session, ["end", "ended_at", "session_end", "logout_at"], "");
      const duration = isFilled(session.duration_hours) ? fmtHours(session.duration_hours) : (isFilled(session.duration_minutes) ? fmtHours(minutesToHours(session.duration_minutes)) : (isFilled(session.active_minutes) ? fmtHours(minutesToHours(session.active_minutes)) : EMPTY));
      const section = pick(session, ["section", "module", "current_section", "primary_section"], "Actividad general");
      return `<div class="profile-list-item"><div><strong>${esc(section)}</strong><small>${esc(fmtDateTime(start))} - ${esc(fmtDateTime(end))}</small></div><span>${esc(duration)}</span></div>`;
    });
    renderProfileList(profileRefs.alerts, profile.alerts, "Sin alertas para este usuario.", (raw) => {
      const sev = normalizeSeverity(pick(raw, ["severity", "level", "severidad", "type"], "info"));
      const reason = pick(raw, ["reason", "motivo", "title", "message"], "Sin detalle");
      const recommendation = pick(raw, ["recommendation", "recomendacion", "action", "suggestion"], "Revisar seguimiento.");
      return `<div class="profile-list-item"><div><strong>${esc(reason)}</strong><small>Severidad ${esc(sev.key)}</small></div><span>${esc(recommendation)}</span></div>`;
    });
    renderProfileList(profileRefs.timeline, profile.timeline, "Sin actividad en el historial.", (event) => {
      const action = pick(event, ["action", "action_type", "title", "event"], "Actividad");
      const section = pick(event, ["section", "module", "current_section"], "General");
      const detail = pick(event, ["detail", "description", "message"], "");
      const icon = timelineIcon(action);
      return `<div class="timeline-event"><div class="timeline-icon"><i class="bi ${icon}"></i></div><div class="flex-grow-1"><div class="small fw-semibold text-white">${esc(action)}</div><div class="small text-white-50">${esc(section)} · ${esc(fmtDateTime(pick(event, ["timestamp", "created_at", "at"], "")))}</div>${detail ? `<div class="small text-white-50 mt-1">${esc(detail)}</div>` : ""}</div></div>`;
    });
    if (profileRefs.timelineBadge) profileRefs.timelineBadge.textContent = `Ultimas ${profile.timeline.length || 0}`;
  };

  async function openUserProfile(userId) {
    if (!userId) return;
    if (profileOffcanvas) profileOffcanvas.show();
    if (profileRefs.loading) profileRefs.loading.style.display = "block";
    if (profileRefs.content) profileRefs.content.style.display = "none";
    try {
      const resp = await fetch(`/sic/api/usage/user-profile/${encodeURIComponent(userId)}`, { headers: { Accept: "application/json" } });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (data?.ok === false || data?.profile?.error) throw new Error(data?.error || data?.profile?.error || "profile_error");
      renderProfile(data.profile || data.data || data);
      if (profileRefs.loading) profileRefs.loading.style.display = "none";
      if (profileRefs.content) profileRefs.content.style.display = "block";
    } catch (error) {
      console.error("[SeguimientoUsuarios] profile", error);
      if (profileRefs.loading) profileRefs.loading.style.display = "none";
      if (profileRefs.content) profileRefs.content.style.display = "block";
      if (profileRefs.timeline) profileRefs.timeline.innerHTML = '<div class="usage-empty py-3">No se pudo cargar el perfil.</div>';
    }
  }
  const updateCards = (cards, state = "ok", meta = {}) => {
    if (state === "error") {
      resetMetricCards("--");
      setKpiNote(stateMessage("kpis", "error"), "error");
      return;
    }
    if (state === "loading") {
      resetMetricCards("0");
      setKpiNote(stateMessage("kpis", "loading"));
      return;
    }
    if (cardRefs.activeUsers) cardRefs.activeUsers.textContent = fmtInt(pick(cards, ["active_users"], 0));
    if (cardRefs.activeHours) cardRefs.activeHours.textContent = fmtHours(pick(cards, ["active_hours"], 0));
    if (cardRefs.adoption) cardRefs.adoption.textContent = fmtPercentMaybe(pick(cards, ["adoption_rate"], 0), 1);
    if (cardRefs.inactive) cardRefs.inactive.textContent = fmtInt(pick(cards, ["inactive_7d_count"], 0));
    if (cardRefs.productivity) cardRefs.productivity.textContent = fmtDecimal(pick(cards, ["avg_productivity_index", "productivity_index"], 0), 2);
    const adoptionBase = numberOrZero(pick(cards, ["adoption_eligible_users"], 0));
    const excluded = boolish(meta?.admins_excluded_by_default) ? ` Adopcion y productividad se calculan sobre ${fmtInt(adoptionBase)} usuario(s) no admin.` : "";
    if (state === "filter_empty") {
      setKpiNote(`${stateMessage("kpis", state)}${excluded}`);
      return;
    }
    if (state === "no_activity") {
      setKpiNote(`${stateMessage("kpis", state)}${excluded}`);
      return;
    }
    setKpiNote(`Indicadores calculados sobre ${fmtInt(pick(cards, ["eligible_users"], 0))} usuario(s) visibles.${excluded}`);
  };

  async function fetchLiveUsers() {
    if (liveSpinner) liveSpinner.style.visibility = "visible";
    try {
      const resp = await fetch("/sic/api/usage/live-users", { headers: { Accept: "application/json" } });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (data?.ok === false) throw new Error(data?.error || "live_error");
      renderLiveUsers(data.users || data.data || [], "ok");
      if (liveLastUpdate) liveLastUpdate.textContent = `Actualizado ${fmtDateTime(new Date())}`;
    } catch (error) {
      console.error("[SeguimientoUsuarios] live", error);
      renderLiveUsers([], "error");
    } finally {
      if (liveSpinner) liveSpinner.style.visibility = "hidden";
    }
  }

  async function loadSummary() {
    updateCards({}, "loading");
    renderAlerts([], "loading");
    renderByUserTable([], "loading");
    renderChartStates("loading");
    try {
      const params = new URLSearchParams();
      [["date_from", "#flt-date-from"], ["date_to", "#flt-date-to"], ["role", "#flt-role"], ["team", "#flt-team"], ["granularity", "#flt-granularity"]].forEach(([key, sel]) => {
        const value = $(sel)?.value || "";
        if (value) params.set(key, value);
      });
      const resp = await fetch(`/sic/api/usage/summary${params.toString() ? `?${params}` : ""}`, { headers: { Accept: "application/json" } });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const data = await resp.json();
      if (data?.ok === false) throw new Error(data?.error || "summary_error");
      const states = data?.meta?.states || {};
      populateRoleOptions(data?.meta?.available_roles || []);
      populateTeamOptions(data?.meta?.available_groups || []);
      updateCards(data.kpis || {}, states.kpis || "ok", data.meta || {});
      renderAlerts(data.alerts || [], states.alerts || "ok");
      if ((states.charts || "ok") !== "ok") {
        renderChartStates(states.charts || "empty");
      } else {
        renderChartSafely("usage-chart-weekday", () => buildWeekdayChart(data.charts?.by_weekday || []));
        renderChartSafely("usage-chart-roles", () => buildRolesChart(data.charts?.users_vs_users || data.charts?.analysts_vs_supervisors || []));
        renderChartSafely("usage-chart-sections", () => buildSectionsChart(data.charts?.sections || []));
        renderChartSafely("usage-chart-heatmap", () => buildHeatmapChart(data.charts?.heatmap || []));
      }
      renderByUserTable(data.per_user || [], states.by_user || "ok");
    } catch (error) {
      console.error("[SeguimientoUsuarios] summary", error);
      updateCards({}, "error");
      renderAlerts([], "error");
      renderByUserTable([], "error");
      renderChartStates("error");
    }
  }

  if (filtersForm) filtersForm.addEventListener("submit", (evt) => { evt.preventDefault(); loadSummary(); });
  if (applyBtn) applyBtn.addEventListener("click", (evt) => { evt.preventDefault(); loadSummary(); });

  const fDateFrom = $("#flt-date-from");
  const fDateTo = $("#flt-date-to");
  if (fDateFrom && !fDateFrom.value) { const d = new Date(); d.setMonth(d.getMonth() - 1); fDateFrom.value = d.toISOString().split("T")[0]; }
  if (fDateTo && !fDateTo.value) fDateTo.value = new Date().toISOString().split("T")[0];

  loadSummary();
  fetchLiveUsers();
  setInterval(loadSummary, 300000);
  setInterval(fetchLiveUsers, 60000);
  window.sicRefreshTracking = () => { loadSummary(); fetchLiveUsers(); };
})();
