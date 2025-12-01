// static/js/seguimiento_usuarios.js
(function () {
  console.log("[Seguimiento usuarios] JS cargado");

  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  // ---------------------------------------------------------------------------
  // Elementos
  // ---------------------------------------------------------------------------
  const $dateFrom = $("#su-date-from");
  const $dateTo = $("#su-date-to");
  const $role = $("#su-role");
  const $team = $("#su-team");
  const $view = $("#su-view");
  const $apply = $("#su-apply");

  const $kpiActiveUsers = $("#su-kpi-active-users");
  const $kpiFiles = $("#su-kpi-files");
  const $kpiHours = $("#su-kpi-hours");
  const $kpiProductivity = $("#su-kpi-productivity");

  const $kpiActiveUsersCap = $("#su-kpi-active-users-caption");
  const $kpiFilesCap = $("#su-kpi-files-caption");
  const $kpiHoursCap = $("#su-kpi-hours-caption");
  const $kpiProductivityCap = $("#su-kpi-productivity-caption");

  const $tableBody = $("#su-table-users-body");
  const $error = $("#su-error");

  const $weekdayWrapper = $("#su-chart-weekday-wrapper");
  const $weekdayEmpty = $("#su-chart-weekday-empty");
  const $rolesWrapper = $("#su-chart-roles-wrapper");
  const $rolesEmpty = $("#su-chart-roles-empty");
  const $heatmapWrapper = $("#su-chart-heatmap-wrapper");
  const $heatmapEmpty = $("#su-chart-heatmap-empty");
  const $sectionsWrapper = $("#su-chart-sections-wrapper");
  const $sectionsEmpty = $("#su-chart-sections-empty");

  // ---------------------------------------------------------------------------
  // Estado de charts (para reusar instancias)
  // ---------------------------------------------------------------------------
  let weekdayChart = null;
  let rolesChart = null;
  let heatmapChart = null;
  let sectionsChart = null;

  // ---------------------------------------------------------------------------
  // Helpers
  // ---------------------------------------------------------------------------
  function safeNumber(value, decimals = 0) {
    const n = Number(value);
    if (Number.isNaN(n)) return 0;
    return Number(n.toFixed(decimals));
  }

  function setError(msg) {
    if (!msg) {
      $error.classList.add("d-none");
      $error.textContent = "";
    } else {
      $error.classList.remove("d-none");
      $error.textContent = msg;
    }
  }

  function toggleEmpty(wrapper, emptyEl, hasData) {
    if (!wrapper || !emptyEl) return;
    if (hasData) {
      emptyEl.classList.add("d-none");
    } else {
      emptyEl.classList.remove("d-none");
    }
  }

  // Intenta ser compatible con varios nombres de campos posibles del backend
  function getKpis(data) {
    return (
      data.kpis ||
      data.summary ||
      {
        active_users: 0,
        uploaded_files: 0,
        active_hours: 0,
        productivity_index: 0,
      }
    );
  }

  function getCharts(data) {
    return data.charts || data.series || {};
  }

  function getPerUserRows(data) {
    if (data.per_user && Array.isArray(data.per_user.rows)) {
      return data.per_user.rows;
    }
    if (Array.isArray(data.per_user)) return data.per_user;
    if (Array.isArray(data.users)) return data.users;
    if (data.users && Array.isArray(data.users.rows)) return data.users.rows;
    return [];
  }

  // ---------------------------------------------------------------------------
  // Construcción de URL a /api/usage/summary
  // ---------------------------------------------------------------------------
  function buildSummaryUrl() {
    const params = new URLSearchParams();

    const from = $dateFrom.value;
    const to = $dateTo.value;
    const role = $role.value;
    const team = $team.value;
    const view = $view.value || "day";

    if (from) {
      // Dos nombres para aumentar chances de compatibilidad con el backend
      params.set("date_from", from);
      params.set("start_date", from);
    }
    if (to) {
      params.set("date_to", to);
      params.set("end_date", to);
    }
    if (role) params.set("role", role);
    if (team) {
      params.set("team_id", team);
      params.set("group_id", team);
    }
    params.set("granularity", view);
    params.set("view", view);

    return "/api/usage/summary?" + params.toString();
  }

  // ---------------------------------------------------------------------------
  // Render KPIs
  // ---------------------------------------------------------------------------
  function renderKpis(data) {
    const kpis = getKpis(data);

    const activeUsers =
      kpis.active_users ??
      kpis.users_active ??
      kpis.users ??
      kpis.total_users ??
      0;
    const uploadedFiles =
      kpis.uploaded_files ??
      kpis.files ??
      kpis.cargas ??
      kpis.files_uploaded ??
      0;
    const activeHours =
      kpis.active_hours ??
      kpis.hours ??
      kpis.hours_active ??
      kpis.total_hours ??
      0;
    const productivity =
      kpis.productivity_index ??
      kpis.productivity ??
      kpis.productivity_avg ??
      0;

    $kpiActiveUsers.textContent = safeNumber(activeUsers).toString();
    $kpiFiles.textContent = safeNumber(uploadedFiles).toString();
    $kpiHours.textContent = safeNumber(activeHours, 1).toString().replace(".", ",");
    $kpiProductivity.textContent = safeNumber(productivity, 2)
      .toString()
      .replace(".", ",");

    // Mensajes de caption flexibles, según haya o no datos
    if (activeUsers > 0) {
      $kpiActiveUsersCap.textContent =
        "Cantidad de usuarios con al menos una sesión en el rango.";
    } else {
      $kpiActiveUsersCap.textContent =
        "Sin actividad registrada en el rango seleccionado.";
    }

    if (uploadedFiles > 0) {
      $kpiFilesCap.textContent =
        "Archivos cargados por Analistas y Supervisores en el rango.";
    } else {
      $kpiFilesCap.textContent =
        "Todavía no se registraron cargas en el rango seleccionado.";
    }

    if (activeHours > 0) {
      $kpiHoursCap.textContent =
        "Horas activas estimadas a partir de los eventos de uso.";
    } else {
      $kpiHoursCap.textContent =
        "Sin tiempo activo registrado en el rango seleccionado.";
    }

    if (productivity > 0) {
      $kpiProductivityCap.textContent =
        "Índice de productividad promedio basado en sesiones, horas y cargas.";
    } else {
      $kpiProductivityCap.textContent =
        "Cuando haya más actividad, vas a ver acá un índice promedio.";
    }
  }

  // ---------------------------------------------------------------------------
  // Render gráficos
  // ---------------------------------------------------------------------------

  function renderWeekdayChart(charts) {
    const raw =
      charts.weekday_activity ||
      charts.activity_by_weekday ||
      charts.activity_by_day ||
      [];

    if (!raw || raw.length === 0) {
      toggleEmpty($weekdayWrapper, $weekdayEmpty, false);
      if (weekdayChart) weekdayChart.updateSeries([]);
      return;
    }

    toggleEmpty($weekdayWrapper, $weekdayEmpty, true);

    const categories = raw.map(
      (r) =>
        r.label ||
        r.weekday_label ||
        r.weekday ||
        r.day_name ||
        r.day ||
        ""
    );
    const values = raw.map(
      (r) =>
        r.value ??
        r.events ??
        r.count ??
        r.sessions ??
        0
    );

    const options = {
      chart: {
        type: "bar",
        height: 260,
        toolbar: { show: false },
      },
      series: [
        {
          name: "Eventos",
          data: values,
        },
      ],
      xaxis: {
        categories,
      },
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 3 },
    };

    if (weekdayChart) {
      weekdayChart.updateOptions(options);
    } else if (window.ApexCharts) {
      weekdayChart = new ApexCharts(
        document.querySelector("#su-chart-weekday"),
        options
      );
      weekdayChart.render();
    }
  }

  function renderRolesChart(charts) {
    const raw =
      charts.roles_comparison ||
      charts.roles ||
      charts.by_role ||
      [];

    if (!raw || raw.length === 0) {
      toggleEmpty($rolesWrapper, $rolesEmpty, false);
      if (rolesChart) rolesChart.updateSeries([]);
      return;
    }

    toggleEmpty($rolesWrapper, $rolesEmpty, true);

    const categories = raw.map((r) => r.role_label || r.role || r.label || "");
    const sessions = raw.map(
      (r) => r.sessions ?? r.events ?? r.count ?? 0
    );
    const hours = raw.map(
      (r) => r.hours ?? r.active_hours ?? 0
    );
    const files = raw.map(
      (r) => r.files ?? r.uploaded_files ?? r.cargas ?? 0
    );

    const options = {
      chart: {
        type: "bar",
        height: 200,
        stacked: true,
        toolbar: { show: false },
      },
      series: [
        { name: "Sesiones", data: sessions },
        { name: "Horas activas", data: hours },
        { name: "Archivos", data: files },
      ],
      xaxis: {
        categories,
      },
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 3 },
      legend: { position: "top" },
    };

    if (rolesChart) {
      rolesChart.updateOptions(options);
    } else if (window.ApexCharts) {
      rolesChart = new ApexCharts(
        document.querySelector("#su-chart-roles"),
        options
      );
      rolesChart.render();
    }
  }

  function renderHeatmapChart(charts) {
    const raw =
      charts.heatmap_day_hour ||
      charts.day_hour_heatmap ||
      charts.heatmap ||
      [];

    if (!raw || raw.length === 0) {
      toggleEmpty($heatmapWrapper, $heatmapEmpty, false);
      if (heatmapChart) heatmapChart.updateSeries([]);
      return;
    }

    toggleEmpty($heatmapWrapper, $heatmapEmpty, true);

    // Esperamos algo tipo [{day: 'Lun', hour: '09-10', value: 3}, ...]
    const byDay = {};
    for (const item of raw) {
      const day =
        item.day_label ||
        item.day_name ||
        item.day ||
        item.weekday ||
        "—";
      const hour = item.hour_label || item.hour_range || item.hour || "";
      const value = safeNumber(
        item.value ?? item.events ?? item.count ?? 0
      );
      if (!byDay[day]) byDay[day] = [];
      byDay[day].push({ x: hour, y: value });
    }

    const series = Object.entries(byDay).map(([day, data]) => ({
      name: day,
      data,
    }));

    const options = {
      chart: {
        type: "heatmap",
        height: 260,
        toolbar: { show: false },
      },
      series,
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 3 },
    };

    if (heatmapChart) {
      heatmapChart.updateOptions(options);
    } else if (window.ApexCharts) {
      heatmapChart = new ApexCharts(
        document.querySelector("#su-chart-heatmap"),
        options
      );
      heatmapChart.render();
    }
  }

  function renderSectionsChart(charts) {
    const raw =
      charts.sections ||
      charts.sections_usage ||
      charts.most_used_sections ||
      [];

    if (!raw || raw.length === 0) {
      toggleEmpty($sectionsWrapper, $sectionsEmpty, false);
      if (sectionsChart) sectionsChart.updateSeries([]);
      return;
    }

    toggleEmpty($sectionsWrapper, $sectionsEmpty, true);

    // Tomamos top 8
    const top = raw.slice(0, 8);
    const categories = top.map(
      (r) => r.section_label || r.section || r.label || ""
    );
    const values = top.map(
      (r) => r.count ?? r.events ?? r.views ?? 0
    );

    const options = {
      chart: {
        type: "bar",
        height: 200,
        toolbar: { show: false },
      },
      series: [{ name: "Eventos", data: values }],
      xaxis: { categories },
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 3 },
      plotOptions: {
        bar: {
          horizontal: true,
        },
      },
    };

    if (sectionsChart) {
      sectionsChart.updateOptions(options);
    } else if (window.ApexCharts) {
      sectionsChart = new ApexCharts(
        document.querySelector("#su-chart-sections"),
        options
      );
      sectionsChart.render();
    }
  }

  function renderCharts(data) {
    const charts = getCharts(data);
    renderWeekdayChart(charts);
    renderRolesChart(charts);
    renderHeatmapChart(charts);
    renderSectionsChart(charts);
  }

  // ---------------------------------------------------------------------------
  // Render tabla por usuario
  // ---------------------------------------------------------------------------
  function renderTable(data) {
    const rows = getPerUserRows(data);

    // Limpia tbody
    $tableBody.innerHTML = "";

    if (!rows || rows.length === 0) {
      const tr = document.createElement("tr");
      tr.className = "su-empty-row";
      const td = document.createElement("td");
      td.colSpan = 8;
      td.style.textAlign = "left";
      td.style.color = "#9ca3af";
      td.textContent =
        "Aún no hay registros de actividad para mostrar en la tabla.";
      tr.appendChild(td);
      $tableBody.appendChild(tr);
      return;
    }

    for (const r of rows) {
      const tr = document.createElement("tr");

      const tdUser = document.createElement("td");
      tdUser.textContent =
        r.user_display ||
        r.user_name ||
        r.username ||
        r.email ||
        "—";
      tr.appendChild(tdUser);

      const tdRole = document.createElement("td");
      const spanRole = document.createElement("span");
      spanRole.className = "su-badge-role";
      spanRole.textContent = (r.role_label || r.role || "").toString();
      tdRole.appendChild(spanRole);
      tr.appendChild(tdRole);

      const tdSessions = document.createElement("td");
      tdSessions.textContent = (r.sessions ?? r.login_count ?? r.events ?? 0).toString();
      tr.appendChild(tdSessions);

      const tdActiveDays = document.createElement("td");
      tdActiveDays.textContent = (r.active_days ?? r.days ?? 0).toString();
      tr.appendChild(tdActiveDays);

      const tdHours = document.createElement("td");
      tdHours.textContent = safeNumber(
        r.active_hours ?? r.hours ?? 0,
        1
      )
        .toString()
        .replace(".", ",");
      tr.appendChild(tdHours);

      const tdFiles = document.createElement("td");
      tdFiles.textContent = (r.files ?? r.uploaded_files ?? r.cargas ?? 0).toString();
      tr.appendChild(tdFiles);

      const tdProd = document.createElement("td");
      tdProd.textContent = safeNumber(
        r.productivity_index ?? r.productivity ?? 0,
        2
      )
        .toString()
        .replace(".", ",");
      tr.appendChild(tdProd);

      const tdLast = document.createElement("td");
      tdLast.textContent = r.last_access_display || r.last_access || "—";
      tr.appendChild(tdLast);

      $tableBody.appendChild(tr);
    }
  }

  // ---------------------------------------------------------------------------
  // Tabs UI
  // ---------------------------------------------------------------------------
  function initTabs() {
    const tabs = $$(".su-tab");
    const panels = $$(".su-tab-panel");

    tabs.forEach((tab) => {
      tab.addEventListener("click", () => {
        const name = tab.dataset.tab;
        tabs.forEach((t) => t.classList.remove("active"));
        tab.classList.add("active");

        panels.forEach((p) => {
          if (p.id === "su-tab-" + name) {
            p.classList.remove("d-none");
          } else {
            p.classList.add("d-none");
          }
        });
      });
    });
  }

  // ---------------------------------------------------------------------------
  // Fetch inicial
  // ---------------------------------------------------------------------------
  async function fetchSummary() {
    setError(null);

    const url = buildSummaryUrl();
    console.log("[Seguimiento usuarios] Fetch:", url);

    let res;
    try {
      res = await fetch(url, {
        headers: {
          Accept: "application/json",
        },
      });
    } catch (err) {
      console.error(err);
      setError("No se pudo conectar con el servidor de seguimiento.");
      return;
    }

    if (!res.ok) {
      setError("Error al obtener los datos de uso (" + res.status + ").");
      return;
    }

    let data;
    try {
      data = await res.json();
    } catch (err) {
      console.error(err);
      setError("La respuesta del servidor no es un JSON válido.");
      return;
    }

    try {
      renderKpis(data);
      renderCharts(data);
      renderTable(data);
    } catch (err) {
      console.error(err);
      setError("Ocurrió un error al renderizar el tablero de uso.");
    }
  }

  function initFiltersDefaults() {
    // Por defecto: últimos 30 días
    const today = new Date();
    const toISO = (d) => d.toISOString().slice(0, 10);

    const to = toISO(today);
    const fromDate = new Date(today);
    fromDate.setDate(fromDate.getDate() - 29);
    const from = toISO(fromDate);

    if (!$dateFrom.value) $dateFrom.value = from;
    if (!$dateTo.value) $dateTo.value = to;
  }

  function init() {
    initFiltersDefaults();
    initTabs();

    $apply.addEventListener("click", () => {
      fetchSummary();
    });

    // Primera carga
    fetchSummary();
  }

  if (document.readyState === "loading") {
    document.addEventListener("DOMContentLoaded", init);
  } else {
    init();
  }
})();
