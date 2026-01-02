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

  // Suizo Argentina Color Palette & Theme
  // Suizo Argentina Color Palette & Theme (S.I.C Dark Mode)
  const SA_COLORS = ["#3b82f6", "#6366f1", "#818cf8", "#60a5fa", "#94a3b8"];
  const CHART_FONT = "Outfit, -apple-system, BlinkMacSystemFont, 'Segoe UI', Roboto, sans-serif";

  const COMMON_CHART_OPTS = {
    fontFamily: CHART_FONT,
    toolbar: { show: false },
    animations: {
      enabled: true,
      easing: 'easeinout',
      speed: 800,
      animateGradually: { enabled: true, delay: 150 },
      animateGradually: { enabled: true, delay: 150 },
      dynamicAnimation: { enabled: true, speed: 350 }
    },
    legend: {
      labels: {
        colors: "#cbd5e1" // Text color for legend
      }
    }
  };

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
        height: 280,
        toolbar: COMMON_CHART_OPTS.toolbar,
        fontFamily: COMMON_CHART_OPTS.fontFamily,
        animations: COMMON_CHART_OPTS.animations
      },
      colors: [SA_COLORS[0]],
      series: [
        {
          name: "Eventos",
          data: values,
        },
      ],
      xaxis: {
        categories: labels,
        axisBorder: { show: false },
        axisTicks: { show: false },
        labels: {
          style: { colors: "#94a3b8", fontSize: "12px" }
        }
      },
      yaxis: {
        labels: {
          style: { colors: "#94a3b8", fontSize: "12px" }
        }
      },
      dataLabels: { enabled: false },
      plotOptions: {
        bar: {
          borderRadius: 4,
          columnWidth: "50%",
        }
      },
      grid: {
        strokeDashArray: 4,
        borderColor: "rgba(148, 163, 184, 0.1)",
        padding: { top: 0, right: 0, bottom: 0, left: 10 }
      },
      tooltip: {
        theme: "dark",
        y: {
          formatter: (v) => `${v} evento${v === 1 ? "" : "s"}`,
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
    // Nota: Aunque la función se llama "buildRolesChart" por compatibilidad,
    // ahora renderiza "Usuario vs Usuario" (Top 15).
    const el = $("#usage-chart-roles");
    if (!ensureApex(el)) return;

    const labels =
      items && items.length
        ? items.map((it) => it.user_label || it.name || "")
        : [];

    const events =
      items && items.length
        ? items.map((it) => numberOrZero(it.events || 0))
        : [];

    const hours =
      items && items.length
        ? items.map((it) => numberOrZero(it.active_hours || 0))
        : [];

    // Si horas es muy bajo comparado con eventos, tal vez convenga doble eje,
    // pero por simplicidad usaremos barras agrupadas.

    const options = {
      chart: {
        type: "bar",
        height: 280,
        toolbar: COMMON_CHART_OPTS.toolbar,
        fontFamily: COMMON_CHART_OPTS.fontFamily,
        animations: COMMON_CHART_OPTS.animations
      },
      colors: [SA_COLORS[2], SA_COLORS[3]],
      series: [
        { name: "Eventos", data: events },
        { name: "Horas activas", data: hours },
      ],
      xaxis: {
        categories: labels,
        axisBorder: { show: false },
        axisTicks: { show: false },
        labels: {
          style: { colors: "#94a3b8", fontSize: "11px" },
          rotate: -45,
          hideOverlappingLabels: false
        }
      },
      yaxis: {
        labels: { style: { colors: "#94a3b8" } }
      },
      dataLabels: { enabled: false },
      plotOptions: {
        bar: {
          borderRadius: 4,
          columnWidth: "60%",
        }
      },
      grid: {
        strokeDashArray: 4,
        borderColor: "rgba(148, 163, 184, 0.1)"
      },
      tooltip: {
        theme: "dark",
        shared: true,
        intersect: false,
      },
      legend: {
        position: 'top',
        horizontalAlign: 'right',
        offsetY: -20,
        labels: {
          colors: "#cbd5e1"
        }
      }
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
        height: 280,
        toolbar: COMMON_CHART_OPTS.toolbar,
        fontFamily: COMMON_CHART_OPTS.fontFamily,
        animations: COMMON_CHART_OPTS.animations
      },
      colors: [SA_COLORS[1]], // Secondary blue for sections
      plotOptions: {
        bar: {
          horizontal: true,
          borderRadius: 4,
          barHeight: "60%",
        },
      },
      series: [{ name: "Eventos", data: values }],
      xaxis: {
        categories: labels,
        axisBorder: { show: false },
        axisTicks: { show: false },
        labels: { style: { colors: "#94a3b8" } }
      },
      yaxis: {
        labels: { style: { colors: "#94a3b8" } }
      },
      dataLabels: { enabled: false },
      grid: {
        strokeDashArray: 4,
        borderColor: "rgba(148, 163, 184, 0.1)",
        padding: { left: 10, right: 0 }
      },
      tooltip: {
        theme: "dark",
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
        height: 280,
        toolbar: COMMON_CHART_OPTS.toolbar,
        fontFamily: COMMON_CHART_OPTS.fontFamily,
        animations: COMMON_CHART_OPTS.animations
      },
      plotOptions: {
        heatmap: {
          shadeIntensity: 0.5,
          radius: 4,
          useFillColorAsStroke: false,
          colorScale: {
            ranges: [
              { from: 0, to: 0, color: 'rgba(148, 163, 184, 0.1)', name: 'Sin actividad' },
              { from: 1, to: 10, color: '#1e40af', name: 'Baja' },
              { from: 11, to: 50, color: '#3b82f6', name: 'Media' },
              { from: 51, to: 10000, color: '#60a5fa', name: 'Alta' }
            ]
          }
        }
      },
      dataLabels: { enabled: false },
      series,
      xaxis: {
        type: "category",
        labels: { style: { colors: "#94a3b8" } },
        axisBorder: { show: false },
        axisTicks: { show: false }
      },
      yaxis: {
        labels: { style: { colors: "#94a3b8" } }
      },
      grid: { borderColor: "transparent" },
      tooltip: {
        theme: "dark",
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

  // updateCards movido a despues de loadSummary para usar formatDuration
  // (Placeholder vacio para mantener estructura si es necesaria, o eliminar bloque)


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

  // Helper de duración amigable
  const formatDuration = (hoursRaw) => {
    const h = numberOrZero(hoursRaw);
    if (h < 1 && h > 0) {
      // Menos de 1 hora -> mostrar minutos
      const m = Math.round(h * 60);
      return `${m} min`;
    }
    // 1 hora o más -> mostrar con 1 decimal
    return fmtDecimal(h, 1); // + " hs" opcional, pero el diseño tiene el icono
  };

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
      const url = "/sic/api/usage/summary" + (qs ? "?" + qs : "");

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
      updateCards(data.kpis || data.cards || {});

      // Gráficos (Resumen general)
      const chats = data.charts || {};
      buildWeekdayChart(chats.by_weekday || []);
      // "analysts_vs_supervisors" ahora viene como "users_vs_users" desde backend
      buildRolesChart(chats.users_vs_users || chats.analysts_vs_supervisors || []);
      buildSectionsChart(chats.sections || []);
      buildHeatmapChart(chats.heatmap || []);

      // Pestaña Por usuario
      renderByUserTable(data.per_user || []);
    } catch (err) {
      console.error(
        "[SeguimientoUsuarios] Error al cargar summary",
        err
      );
    }
  }

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

    if (cardActiveUsers)
      cardActiveUsers.textContent = fmtInt(activeUsers);
    if (cardFiles) cardFiles.textContent = fmtInt(files);

    // Usamos el formatDuration nuevo
    if (cardActiveHours)
      cardActiveHours.textContent = formatDuration(hours);

    if (cardActiveUsersSub) {
      cardActiveUsersSub.textContent =
        activeUsers > 0
          ? "Usuarios con actividad en el rango."
          : "Sin actividad registrada en el rango seleccionado.";
    }
    if (cardFilesSub) {
      cardFilesSub.textContent =
        files > 0
          ? "Archivos subidos por Analistas y Supervisores."
          : "Todavía no se registraron cargas en el rango seleccionado.";
    }
    if (cardActiveHoursSub) {
      cardActiveHoursSub.textContent =
        hours > 0
          ? "Tiempo total aproximado de actividad."
          : "Sin tiempo activo registrado en el rango seleccionado.";
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

  // Inicialización de Fechas (Histórico: 2024-01-01 -> Hoy)
  const fDateFrom = $("#flt-date-from");
  const fDateTo = $("#flt-date-to");
  if (fDateFrom && !fDateFrom.value) {
    fDateFrom.value = "2024-01-01";
  }
  if (fDateTo && !fDateTo.value) {
    fDateTo.value = new Date().toISOString().split("T")[0];
  }

  // Primera carga
  loadSummary();

  // Auto-refresh cada 5 minutos
  setInterval(() => {
    console.log("[SeguimientoUsuarios] Auto-refresh...");
    loadSummary();
  }, 300000);

  // Exponer refresh manual
  window.sicRefreshTracking = loadSummary;
})();
