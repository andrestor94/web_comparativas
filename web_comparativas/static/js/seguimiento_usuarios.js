// static/js/seguimiento_usuarios.js
(function () {
  console.log("[SeguimientoUsuarios] JS cargado v2");

  // Helpers --------------------------------------------------------------
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const numberOrZero = (v) => (v == null || isNaN(Number(v)) ? 0 : Number(v));

  const fmtInt = (v) => numberOrZero(v).toString();

  const fmtDecimal = (v, decimals = 1) => {
    const n = numberOrZero(v);
    return n.toLocaleString("es-AR", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  };

  const fmtPercent = (v) => {
    const n = numberOrZero(v) * (v > 1 ? 1 : 100); // si backend ya manda 0-1
    return n.toLocaleString("es-AR", {
      minimumFractionDigits: 2,
      maximumFractionDigits: 2,
    });
  };

  const fmtDateTime = (s) => {
    if (!s) return "—";
    const d = new Date(s);
    if (isNaN(d.getTime())) return s;
    return d.toLocaleString("es-AR", {
      day: "2-digit",
      month: "2-digit",
      year: "numeric",
      hour: "2-digit",
      minute: "2-digit",
    });
  };

  // Tabs manuales (no dependemos de JS de Bootstrap) ---------------------
  const tabButtons = $$("[data-usage-tab-target]");
  const tabPanels = $$("[data-usage-tab-panel]");

  function activateTab(targetSelector) {
    tabButtons.forEach((btn) => {
      const isActive = btn.dataset.usageTabTarget === targetSelector;
      btn.classList.toggle("active", isActive);
    });

    tabPanels.forEach((panel) => {
      const selector = "#" + panel.id;
      const isActive = selector === targetSelector;
      panel.classList.toggle("show", isActive);
      panel.classList.toggle("active", isActive);
    });
  }

  tabButtons.forEach((btn) => {
    btn.addEventListener("click", (evt) => {
      evt.preventDefault();
      const target = btn.dataset.usageTabTarget;
      if (!target) return;
      activateTab(target);
    });
  });

  if (tabButtons.length) {
    activateTab(tabButtons[0].dataset.usageTabTarget);
  }

  // Referencias a elementos ----------------------------------------------
  const filtersForm = $("#usage-filters-form");
  const applyBtn = $("#usage-apply-filters");

  // KPIs
  const cardActiveUsers = $("#card-active-users");
  const cardFiles = $("#card-files");
  const cardActiveHours = $("#card-active-hours");
  const cardProductivity = $("#card-productivity");

  const cardActiveUsersSub = $("#card-active-users-sub");
  const cardFilesSub = $("#card-files-sub");
  const cardActiveHoursSub = $("#card-active-hours-sub");
  const cardProductivitySub = $("#card-productivity-sub");

  // Pestaña "Por usuario": mini-cards + tabla
  const byUserTbody = $("#su-users-byuser-tbody");

  const suDetailName = $("#su-user-detail-name");
  const suDetailRoleTeam = $("#su-user-detail-role-team");
  const suDetailSessions = $("#su-user-detail-sessions");
  const suDetailDays = $("#su-user-detail-days");
  const suDetailHours = $("#su-user-detail-hours");
  const suDetailProductivity = $("#su-user-detail-productivity");
  const suDetailLastAccess = $("#su-user-detail-last-access");

  let byUserRowsCache = []; // para poder seleccionar usuario al hacer click

  // Charts (ApexCharts) ---------------------------------------------------
  let chartWeekday = null;
  let chartRoles = null;
  let chartSections = null;
  let chartHeatmap = null;

  function ensureApex(el) {
    if (!el) return false;
    if (window.ApexCharts) return true;
    el.innerHTML =
      '<div class="usage-empty">No se pudieron cargar los gráficos (ApexCharts no disponible).</div>';
    return false;
  }

  function buildWeekdayChart(items) {
    const el = $("#usage-chart-weekday");
    if (!ensureApex(el)) return;

    const labels =
      items && items.length
        ? items.map((it) => it.label || it.weekday || it.day || "")
        : ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

    const values =
      items && items.length
        ? items.map((it) =>
            numberOrZero(it.sessions || it.count || it.events || 0)
          )
        : labels.map(() => 0);

    const options = {
      chart: {
        type: "bar",
        height: 260,
        toolbar: { show: false },
      },
      series: [
        {
          name: "Sesiones",
          data: values,
        },
      ],
      xaxis: {
        categories: labels,
      },
      dataLabels: { enabled: false },
      stroke: { width: 2 },
      grid: { strokeDashArray: 4 },
      tooltip: {
        y: {
          formatter: (v) => `${v} sesión${v === 1 ? "" : "es"}`,
        },
      },
    };

    if (chartWeekday) {
      chartWeekday.updateOptions({
        xaxis: options.xaxis,
        series: options.series,
      });
    } else {
      chartWeekday = new ApexCharts(el, options);
      chartWeekday.render();
    }
  }

  function buildRolesChart(items) {
    const el = $("#usage-chart-roles");
    if (!ensureApex(el)) return;

    const roles =
      items && items.length
        ? items.map((it) => it.role || it.label || "")
        : ["Analistas", "Supervisores"];

    const sessions =
      items && items.length
        ? items.map((it) => numberOrZero(it.sessions || it.count || 0))
        : roles.map(() => 0);

    const hours =
      items && items.length
        ? items.map((it) => numberOrZero(it.active_hours || it.hours || 0))
        : roles.map(() => 0);

    const files =
      items && items.length
        ? items.map((it) => numberOrZero(it.files_uploaded || it.files || 0))
        : roles.map(() => 0);

    const options = {
      chart: {
        type: "bar",
        stacked: true,
        height: 260,
        toolbar: { show: false },
      },
      series: [
        { name: "Sesiones", data: sessions },
        { name: "Horas activas", data: hours },
        { name: "Archivos", data: files },
      ],
      xaxis: { categories: roles },
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 4 },
      tooltip: {
        shared: true,
        intersect: false,
      },
    };

    if (chartRoles) {
      chartRoles.updateOptions({
        xaxis: options.xaxis,
        series: options.series,
      });
    } else {
      chartRoles = new ApexCharts(el, options);
      chartRoles.render();
    }
  }

  function buildSectionsChart(items) {
    const el = $("#usage-chart-sections");
    if (!ensureApex(el)) return;

    const labels =
      items && items.length
        ? items.map((it) => it.section || it.label || it.name || "")
        : ["Home", "Web Comparativa", "Oportunidades", "Reporte perfiles"];

    const values =
      items && items.length
        ? items.map((it) => numberOrZero(it.events || it.count || 0))
        : labels.map(() => 0);

    const options = {
      chart: {
        type: "bar",
        height: 260,
        toolbar: { show: false },
      },
      plotOptions: {
        bar: {
          horizontal: true,
        },
      },
      series: [{ name: "Eventos", data: values }],
      xaxis: { categories: labels },
      dataLabels: { enabled: false },
      grid: { strokeDashArray: 4 },
      tooltip: {
        y: {
          formatter: (v) => `${v} evento${v === 1 ? "" : "s"}`,
        },
      },
    };

    if (chartSections) {
      chartSections.updateOptions({
        xaxis: options.xaxis,
        series: options.series,
      });
    } else {
      chartSections = new ApexCharts(el, options);
      chartSections.render();
    }
  }

  function buildHeatmapChart(items) {
    const el = $("#usage-chart-heatmap");
    if (!ensureApex(el)) return;

    if (!items || !items.length) {
      const html =
        '<div class="usage-empty">Aún no hay datos suficientes para el heatmap día / hora.</div>';
      el.innerHTML = html;
      if (chartHeatmap) {
        chartHeatmap.destroy();
        chartHeatmap = null;
      }
      return;
    }

    const daysOrder = ["Lun", "Mar", "Mié", "Jue", "Vie", "Sáb", "Dom"];

    const grouped = {};
    for (const raw of items) {
      const day =
        raw.day_label ||
        raw.day ||
        raw.weekday ||
        raw.label ||
        "Otros";
      const hour =
        raw.hour_label != null
          ? raw.hour_label
          : raw.hour != null
          ? raw.hour
          : raw.bucket || 0;
      const value = numberOrZero(raw.value || raw.count || raw.events || 0);
      if (!grouped[day]) grouped[day] = {};
      grouped[day][hour] = value;
    }

    const sortedDays = Object.keys(grouped).sort((a, b) => {
      const ia = daysOrder.indexOf(a);
      const ib = daysOrder.indexOf(b);
      if (ia === -1 && ib === -1) return a.localeCompare(b);
      if (ia === -1) return 1;
      if (ib === -1) return -1;
      return ia - ib;
    });

    const hoursSet = new Set();
    sortedDays.forEach((d) => {
      Object.keys(grouped[d]).forEach((h) => hoursSet.add(h));
    });
    const hours = Array.from(hoursSet).sort(
      (a, b) => Number(a) - Number(b)
    );

    const series = sortedDays.map((day) => ({
      name: day,
      data: hours.map((h) => {
        const v = grouped[day][h];
        return {
          x: `${h}:00`,
          y: numberOrZero(v),
        };
      }),
    }));

    const options = {
      chart: {
        type: "heatmap",
        height: 260,
        toolbar: { show: false },
      },
      dataLabels: { enabled: false },
      series,
      xaxis: { type: "category" },
      tooltip: {
        y: {
          formatter: (v) => `${v} evento${v === 1 ? "" : "s"}`,
        },
      },
    };

    if (chartHeatmap) {
      chartHeatmap.updateOptions({
        series: options.series,
        xaxis: options.xaxis,
      });
    } else {
      chartHeatmap = new ApexCharts(el, options);
      chartHeatmap.render();
    }
  }

  // Tarjetas KPIs ---------------------------------------------------------
  function updateCards(cardsRaw) {
    const cards = cardsRaw || {};
    const activeUsers = numberOrZero(
      cards.active_users || cards.users || cards.total_users || 0
    );
    const files = numberOrZero(
      cards.files_uploaded || cards.files || cards.uploads || 0
    );
    const hours = numberOrZero(
      cards.active_hours || cards.hours || cards.time_active || 0
    );
    const productivity =
      cards.productivity_index != null
        ? Number(cards.productivity_index)
        : cards.productivity != null
        ? Number(cards.productivity)
        : 0;

    if (cardActiveUsers)
      cardActiveUsers.textContent = fmtInt(activeUsers);
    if (cardFiles) cardFiles.textContent = fmtInt(files);
    if (cardActiveHours)
      cardActiveHours.textContent = fmtDecimal(hours, 1);
    if (cardProductivity)
      cardProductivity.textContent = fmtPercent(productivity);

    if (cardActiveUsersSub) {
      cardActiveUsersSub.textContent =
        activeUsers > 0
          ? "Usuarios con al menos una sesión en el rango seleccionado."
          : "Sin actividad registrada en el rango seleccionado.";
    }
    if (cardFilesSub) {
      cardFilesSub.textContent =
        files > 0
          ? "Archivos subidos por Analistas y Supervisores en el período."
          : "Todavía no se registraron cargas en el rango seleccionado.";
    }
    if (cardActiveHoursSub) {
      cardActiveHoursSub.textContent =
        hours > 0
          ? "Tiempo total aproximado con actividad en la interfaz."
          : "Sin tiempo activo registrado en el rango seleccionado.";
    }
    if (cardProductivitySub) {
      cardProductivitySub.textContent =
        productivity > 0
          ? "Índice promedio basado en sesiones, horas y cargas."
          : "Cuando haya más actividad, vas a ver acá un índice promedio.";
    }
  }

  // Pestaña Por usuario ---------------------------------------------------

  function resetUserDetail() {
    if (suDetailName)
      suDetailName.textContent = "Seleccioná un usuario de la tabla";
    if (suDetailRoleTeam) suDetailRoleTeam.textContent = "\u00a0";
    if (suDetailSessions) suDetailSessions.textContent = "—";
    if (suDetailDays) suDetailDays.textContent = "—";
    if (suDetailHours) suDetailHours.textContent = "—";
    if (suDetailProductivity) suDetailProductivity.textContent = "—";
    if (suDetailLastAccess) suDetailLastAccess.textContent = "Último acceso: —";
  }

  function updateUserDetailFromRow(r) {
    if (!r) {
      resetUserDetail();
      return;
    }

    const name = r.name || r.username || r.user || "—";
    const role =
      r.role_label || r.role || r.rol || "";
    const team =
      r.team_label || r.team || r.equipo || "";

    const sessions = fmtInt(r.sessions || r.sesiones || 0);
    const activeDays = fmtInt(r.active_days || r.dias_activos || 0);
    const hours = fmtDecimal(r.active_hours || r.hours || 0, 1);
    const prod = fmtPercent(
      r.productivity_index || r.productivity || 0
    );
    const lastAccess = fmtDateTime(
      r.last_access || r.ultimo_acceso
    );

    if (suDetailName) suDetailName.textContent = name;

    if (suDetailRoleTeam) {
      if (role && team) {
        suDetailRoleTeam.textContent = `${role} · ${team}`;
      } else if (role) {
        suDetailRoleTeam.textContent = role;
      } else if (team) {
        suDetailRoleTeam.textContent = team;
      } else {
        suDetailRoleTeam.textContent = "\u00a0";
      }
    }

    if (suDetailSessions) suDetailSessions.textContent = sessions;
    if (suDetailDays) suDetailDays.textContent = activeDays;
    if (suDetailHours) suDetailHours.textContent = hours;
    if (suDetailProductivity) suDetailProductivity.textContent = prod;
    if (suDetailLastAccess)
      suDetailLastAccess.textContent = `Último acceso: ${lastAccess}`;
  }

  function attachByUserRowHandlers() {
    if (!byUserTbody) return;

    const trs = $$("tr[data-user-index]", byUserTbody);
    trs.forEach((tr) => {
      tr.addEventListener("click", () => {
        const idx = Number(tr.dataset.userIndex);
        const row = byUserRowsCache[idx];
        updateUserDetailFromRow(row);

        // resaltamos fila seleccionada
        trs.forEach((t) => t.classList.remove("table-active"));
        tr.classList.add("table-active");
      });
    });

    // auto-seleccionar el primero si hay datos
    if (trs.length > 0 && byUserRowsCache.length > 0) {
      trs[0].click();
    } else {
      resetUserDetail();
    }
  }

  function renderByUserTable(rowsRaw) {
    if (!byUserTbody) return;

    const rows = Array.isArray(rowsRaw) ? rowsRaw : [];
    byUserRowsCache = rows;

    if (!rows.length) {
      byUserTbody.innerHTML =
        '<tr><td colspan="8" class="usage-empty">Aún no hay registros de actividad para mostrar en la tabla.</td></tr>';
      resetUserDetail();
      return;
    }

    const html = rows
      .map((r, idx) => {
        const name = r.name || r.username || r.user || "—";
        const role =
          r.role_label || r.role || r.rol || "—";
        const sessions = fmtInt(r.sessions || r.sesiones || 0);
        const activeDays = fmtInt(
          r.active_days || r.dias_activos || 0
        );
        const hours = fmtDecimal(
          r.active_hours || r.hours || 0,
          1
        );
        const files = fmtInt(
          r.files_uploaded || r.files || r.cargas || 0
        );
        const prod = fmtPercent(
          r.productivity_index || r.productivity || 0
        );
        const lastAccess = fmtDateTime(
          r.last_access || r.ultimo_acceso
        );

        return `
          <tr data-user-index="${idx}" style="cursor:pointer">
            <td>${name}</td>
            <td>${role}</td>
            <td class="text-end">${sessions}</td>
            <td class="text-end">${activeDays}</td>
            <td class="text-end">${hours}</td>
            <td class="text-end">${files}</td>
            <td class="text-end">${prod}</td>
            <td>${lastAccess}</td>
          </tr>
        `;
      })
      .join("");

    byUserTbody.innerHTML = html;
    attachByUserRowHandlers();
  }

  // Carga de datos desde /api/usage/summary -------------------------------
  async function loadSummary() {
    try {
      const params = new URLSearchParams();

      const dateFrom = $("#flt-date-from")?.value || "";
      const dateTo = $("#flt-date-to")?.value || "";
      const role = $("#flt-role")?.value || "";
      const team = $("#flt-team")?.value || "";
      const gran = $("#flt-granularity")?.value || "";

      if (dateFrom) params.set("date_from", dateFrom);
      if (dateTo) params.set("date_to", dateTo);
      if (role) params.set("role", role);
      if (team) params.set("team", team);
      if (gran) params.set("granularity", gran);

      const qs = params.toString();
      const url = "/api/usage/summary" + (qs ? "?" + qs : "");

      console.log("[SeguimientoUsuarios] Fetch", url);

      const resp = await fetch(url, {
        headers: { Accept: "application/json" },
      });

      if (!resp.ok) {
        throw new Error(
          "HTTP " + resp.status + " " + resp.statusText
        );
      }

      const data = await resp.json();
      console.log("[SeguimientoUsuarios] summary payload", data);

      // KPIs
      updateCards(data.cards || data.summary || {});

      // Gráficos (Resumen general)
      buildWeekdayChart(
        data.activity_by_weekday ||
          data.activity_weekday ||
          data.activity ||
          []
      );
      buildRolesChart(
        data.sessions_by_role ||
          data.roles ||
          data.by_role ||
          []
      );
      buildSectionsChart(
        data.top_sections ||
          data.sections ||
          data.sections_top ||
          []
      );
      buildHeatmapChart(
        data.heatmap ||
          data.heatmap_matrix ||
          data.heatmap_data ||
          []
      );

      // Pestaña Por usuario
      renderByUserTable(
        data.users ||
          data.by_user ||
          data.users_summary ||
          []
      );
    } catch (err) {
      console.error(
        "[SeguimientoUsuarios] Error al cargar summary",
        err
      );
    }
  }

  // Eventos de filtros ----------------------------------------------------
  if (filtersForm) {
    filtersForm.addEventListener("submit", (evt) => {
      evt.preventDefault();
      loadSummary();
    });
  }
  if (applyBtn) {
    applyBtn.addEventListener("click", (evt) => {
      evt.preventDefault();
      loadSummary();
    });
  }

  // Primera carga
  loadSummary();
})();
