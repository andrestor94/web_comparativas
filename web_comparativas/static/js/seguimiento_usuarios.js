// static/js/seguimiento_usuarios.js
(function () {
  console.log("[SeguimientoUsuarios] JS cargado v1");

  // Detectamos si estamos en la pantalla de Seguimiento de usuarios
  const applyBtn = document.getElementById("usage-apply-filters");
  if (!applyBtn) {
    // No estamos en esta vista, no hacemos nada
    return;
  }

  // --- Referencias a filtros ---
  const inputFrom = document.getElementById("usage-date-from");
  const inputTo = document.getElementById("usage-date-to");
  const selectRole = document.getElementById("usage-role");
  const selectView = document.getElementById("usage-view");
  const selectTeam = document.getElementById("usage-team"); // por ahora no lo usamos pero lo dejamos listo

  // --- Referencias a KPIs ---
  const kpiActiveUsers = document.getElementById("usage-kpi-active-users");
  const kpiUploads = document.getElementById("usage-kpi-uploads");
  const kpiHours = document.getElementById("usage-kpi-hours");
  const kpiProductivity = document.getElementById("usage-kpi-productivity");

  const kpiActiveUsersHint = document.getElementById("usage-kpi-active-users-hint");
  const kpiUploadsHint = document.getElementById("usage-kpi-uploads-hint");
  const kpiHoursHint = document.getElementById("usage-kpi-hours-hint");
  const kpiProductivityHint = document.getElementById("usage-kpi-productivity-hint");

  function safeSetText(el, value) {
    if (!el) return;
    el.textContent = value;
  }

  function formatNumber(n, decimals) {
    if (typeof n !== "number" || isNaN(n)) return "0";
    return n.toLocaleString("es-AR", {
      minimumFractionDigits: decimals,
      maximumFractionDigits: decimals,
    });
  }

  function getFilters() {
    const df = (inputFrom && inputFrom.value.trim()) || "";
    const dt = (inputTo && inputTo.value.trim()) || "";
    const role = (selectRole && selectRole.value) || "analistas_y_supervisores";
    const view = (selectView && selectView.value) || "day";
    const team = (selectTeam && selectTeam.value) || "all";

    return { date_from: df, date_to: dt, role, view, team };
  }

  function setLoading(isLoading) {
    if (!applyBtn) return;
    if (isLoading) {
      applyBtn.dataset.originalText = applyBtn.dataset.originalText || applyBtn.textContent;
      applyBtn.textContent = "Cargando…";
      applyBtn.disabled = true;
    } else {
      if (applyBtn.dataset.originalText) {
        applyBtn.textContent = applyBtn.dataset.originalText;
      }
      applyBtn.disabled = false;
    }
  }

  async function loadSummary(options) {
    const { silent } = options || {};
    const filters = getFilters();

    const params = new URLSearchParams();
    if (filters.date_from) params.set("date_from", filters.date_from);
    if (filters.date_to) params.set("date_to", filters.date_to);
    if (filters.role) params.set("role", filters.role);
    if (filters.view) params.set("view", filters.view);
    // team por ahora no se envía al backend (lo dejamos para una versión futura)

    const url = `/api/usage/summary?${params.toString()}`;

    if (!silent) setLoading(true);

    try {
      const resp = await fetch(url, {
        headers: { accept: "application/json" },
      });

      if (!resp.ok) {
        throw new Error("Error HTTP " + resp.status);
      }

      const data = await resp.json();
      if (!data || data.ok === false) {
        throw new Error("Respuesta inválida");
      }

      renderSummary(data);
    } catch (err) {
      console.error("[SeguimientoUsuarios] Error cargando summary:", err);
      // Si tenés Notify, mostramos un toast. Si no, un alert simple.
      if (window.Notify && Notify.toastError) {
        Notify.toastError("No se pudo cargar el resumen", "Revisá tu conexión o intentá de nuevo.");
      } else if (!silent) {
        alert("No se pudo cargar el resumen de uso. Intentá de nuevo.");
      }
    } finally {
      if (!silent) setLoading(false);
    }
  }

  function renderSummary(data) {
    if (!data) return;
    const k = data.kpis || {};
    const filters = data.filters || {};

    const activeUsers = Number(k.active_users || 0);
    const uploads = Number(k.uploads || 0);
    const hours = Number(k.active_hours || 0);
    const prod = Number(k.avg_productivity_index || 0);

    // KPIs principales
    safeSetText(kpiActiveUsers, formatNumber(activeUsers, 0));
    safeSetText(kpiUploads, formatNumber(uploads, 0));
    safeSetText(kpiHours, formatNumber(hours, 1));
    safeSetText(kpiProductivity, formatNumber(prod, 2));

    // Subtextos / hints
    const rangeText = filters.date_from && filters.date_to
      ? `entre el ${formatDateLabel(filters.date_from)} y el ${formatDateLabel(filters.date_to)}`
      : "en los últimos días";

    safeSetText(
      kpiActiveUsersHint,
      activeUsers > 0
        ? `Usuarios con al menos una sesión ${rangeText}.`
        : `Sin actividad registrada ${rangeText}.`
    );

    safeSetText(
      kpiUploadsHint,
      uploads > 0
        ? `Total de cargas de archivos realizadas por analistas y supervisores ${rangeText}.`
        : `Todavía no se registraron cargas de archivos ${rangeText}.`
    );

    safeSetText(
      kpiHoursHint,
      hours > 0
        ? `Horas estimadas de interacción activa en la interfaz ${rangeText}.`
        : `Sin tiempo activo registrado ${rangeText}.`
    );

    safeSetText(
      kpiProductivityHint,
      prod > 0
        ? `Promedio de (archivos cargados / sesiones) entre los usuarios activos.`
        : `Cuando haya más actividad, vas a ver aquí un índice de productividad promedio.`
    );

    // Más adelante acá vamos a agregar:
    // - renderCharts(data.charts)
    // - renderPerUserTable(data.per_user)
  }

  function formatDateLabel(isoOrDmy) {
    if (!isoOrDmy) return "";
    const txt = String(isoOrDmy);
    // si ya viene dd/mm/YYYY lo devolvemos tal cual
    if (txt.includes("/")) return txt;
    // si viene YYYY-MM-DD lo convertimos
    const parts = txt.split("-");
    if (parts.length === 3) {
      const [y, m, d] = parts;
      return `${d.padStart(2, "0")}/${m.padStart(2, "0")}/${y}`;
    }
    return txt;
  }

  // --- Eventos ---

  applyBtn.addEventListener("click", function (e) {
    e.preventDefault();
    loadSummary({ silent: false });
  });

  // Enter en los campos de fecha también aplica filtros
  [inputFrom, inputTo].forEach(function (inp) {
    if (!inp) return;
    inp.addEventListener("keydown", function (e) {
      if (e.key === "Enter") {
        e.preventDefault();
        loadSummary({ silent: false });
      }
    });
  });

  if (selectRole) {
    selectRole.addEventListener("change", function () {
      // recargamos pero en modo "silent" (sin bloquear el botón)
      loadSummary({ silent: true });
    });
  }

  // Carga inicial al abrir la pantalla
  loadSummary({ silent: false });
})();
