/* Detalle de una oportunidad ya enviada al CRM. Vista de solo lectura. */
(function () {
  "use strict";

  const LIST_API = "/api/mercado-privado/oportunidades/enviadas";
  const POINT_API = "/api/mercado-privado/oportunidades/enviadas/detalle";
  const CONFIG = window.OPP_ENVIO_DETAIL || {};
  const MISSING = "No registrado en este env\u00edo";
  const $ = (id) => document.getElementById(id);

  const modeLabel = (mode) => ({
    prod: "Producci\u00f3n",
    test: "Test",
    simulado: "Simulado",
  }[mode] || mode || MISSING);

  const statusLabel = (row) => {
    if (row.crm_modo === "simulado") return "Env\u00edo simulado";
    if ((row.crm_status || "").startsWith("ENVIADO_")) return "Enviada correctamente";
    return row.crm_status ? "Registrada en el CRM" : MISSING;
  };

  const assignmentLabel = (origin) => ({
    manual: "Selecci\u00f3n manual",
    match: "Asignaci\u00f3n autom\u00e1tica",
  }[origin] || MISSING);

  const fmtNumber = (value) => value == null ? MISSING : new Intl.NumberFormat("es-AR", {
    maximumFractionDigits: 1,
  }).format(value);

  const fmtMoney = (value) => value == null ? MISSING : new Intl.NumberFormat("es-AR", {
    style: "currency", currency: "ARS", maximumFractionDigits: 0,
  }).format(value);

  const fmtPercent = (value) => value == null ? MISSING : new Intl.NumberFormat("es-AR", {
    style: "percent", maximumFractionDigits: 0,
  }).format(value);

  const fmtDate = (iso) => {
    if (!iso) return MISSING;
    const value = String(iso);
    const date = value.slice(0, 10).split("-");
    if (date.length !== 3) return value;
    const time = value.slice(11, 16);
    return `${date[2]}/${date[1]}/${date[0]}${time ? ` a las ${time}` : ""}`;
  };

  const fmtDateOnly = (iso) => {
    if (!iso) return MISSING;
    const parts = String(iso).slice(0, 10).split("-");
    return parts.length === 3 ? `${parts[2]}/${parts[1]}/${parts[0]}` : String(iso);
  };

  function setText(id, value) {
    const element = $(id);
    if (element) element.textContent = value == null || value === "" ? MISSING : value;
  }

  function normalized(value) {
    return String(value || "").trim().toLocaleLowerCase("es");
  }

  function typeLabel(value) {
    const raw = String(value || "").trim().toLocaleLowerCase("es");
    return raw ? raw.charAt(0).toLocaleUpperCase("es") + raw.slice(1) : "Oportunidad enviada";
  }

  function render(row) {
    const displayName = row.cliente_visible
      || (row.cliente_display && row.cliente_display.texto)
      || MISSING;
    const product = row.producto || MISSING;
    const sentAt = fmtDate(row.enviado_at);
    const hasCurrentData = row.datos_oportunidad_disponibles !== false;
    const opportunityType = String(row.tipo_oportunidad || "PUNTUAL").toUpperCase();
    const safeType = ["ESTABLE", "RECURRENTE", "INTERMITENTE", "PUNTUAL"].includes(opportunityType)
      ? opportunityType : "PUNTUAL";

    const identity = $("envIdentity");
    identity.className = `od-identity od-rail-${safeType}`;
    const chip = $("envIdentityChip");
    chip.className = `od-chip od-chip-${safeType}`;
    const activity = row.estado_actividad
      ? ` \u00b7 ${String(row.estado_actividad).toLocaleLowerCase("es")}`
      : "";
    chip.textContent = `${typeLabel(row.tipo_oportunidad)}${activity}`;

    setText("envDetailClient", displayName);
    setText("envDetailProduct", product);

    setText("envClientCuit", row.cuit || row.crm_cuit_informado);
    setText("envClientAccount", row.cuenta_utilizada);
    setText("envClientOperator", row.operador_nombre);
    setText("envClientProvince", row.provincia);

    setText("envProductCode", row.codigo_articulo);
    setText("envBusinessUnit", row.unidad_negocio);
    setText("envProductFamily", row.familia);
    setText("envProductPlatform", row.plataforma);

    const crmName = row.crm_razon_social_informada;
    const showCrmName = crmName && normalized(crmName) !== normalized(displayName);
    $("envCrmNameRow").style.display = showCrmName ? "flex" : "none";
    if (showCrmName) setText("envCrmName", crmName);

    setText("envDemandTypical", row.consumo_tipico_mensual == null
      ? null : `${fmtNumber(row.consumo_tipico_mensual)} unidades`);
    const hasRange = row.consumo_min_mensual != null || row.consumo_max_mensual != null;
    setText("envDemandRange", hasRange
      ? `${fmtNumber(row.consumo_min_mensual)} a ${fmtNumber(row.consumo_max_mensual)} unidades`
      : null);
    setText("envDemandMonths", row.meses_demanda_cliente_12m == null
      ? null : `${fmtNumber(row.meses_demanda_cliente_12m)} de ${fmtNumber(row.ventana_meses)} meses`);
    setText("envDemandAbsent", row.meses_no_participo_12m == null
      ? null : `${fmtNumber(row.meses_no_participo_12m)} meses`);
    const lastDemand = row.ultima_demanda
      ? `${fmtDateOnly(row.ultima_demanda)}${row.meses_desde_ultima_demanda == null
        ? "" : ` (hace ${fmtNumber(row.meses_desde_ultima_demanda)} meses)`}`
      : null;
    setText("envDemandLast", lastDemand);

    const effectiveness = $("envPerformanceEffectiveness");
    setText("envPerformanceEffectiveness", fmtPercent(row.efectividad));
    effectiveness.className = "od-value od-ef";
    if (row.efectividad != null) {
      effectiveness.classList.add(row.efectividad < 0.3 ? "ef-lo" : row.efectividad < 0.6 ? "ef-mid" : "ef-hi");
    }
    setText("envPerformanceWins", row.ganados);
    setText("envPerformanceClients", row.clientes_distintos);

    setText("envEstimatedPrice", fmtMoney(row.precio_unitario_estimado));
    setText("envMonthlyAmount", fmtMoney(row.monto_oportunidad));

    setText("envAssignedTo", row.asignado_a);
    setText("envAssignmentType", assignmentLabel(row.asignado_origen));
    setText("envSentBy", row.enviado_por);
    setText("envSentAt", sentAt);
    setText("envSentMode", modeLabel(row.crm_modo));
    setText("envSentStatus", statusLabel(row));

    $("envDemandSection").style.display = hasCurrentData ? "block" : "none";
    $("envPerformanceSection").style.display = hasCurrentData ? "block" : "none";
    const historicalSection = $("envHistoricalSummarySection");
    historicalSection.style.display = !hasCurrentData && row.descripcion ? "block" : "none";
    if (!hasCurrentData && row.descripcion) setText("envHistoricalSummary", row.descripcion);

    const crmLink = $("envDetailCrmLink");
    if (row.crm_url) {
      crmLink.href = row.crm_url;
      crmLink.style.display = "inline-flex";
    } else {
      crmLink.removeAttribute("href");
      crmLink.style.display = "none";
    }

    $("envDetailLoading").style.display = "none";
    $("envDetailError").style.display = "none";
    $("envDetailContent").style.display = "block";
  }

  function showError(message) {
    $("envDetailLoading").style.display = "none";
    $("envDetailContent").style.display = "none";
    $("envDetailErrorText").textContent = message;
    $("envDetailError").style.display = "block";
  }

  async function fetchRow() {
    const opportunityId = String(CONFIG.oportunidadId || "").trim();
    if (!opportunityId) throw new Error("No se indic\u00f3 qu\u00e9 oportunidad quer\u00e9s consultar.");

    const requestedMode = new URLSearchParams(window.location.search).get("crm_modo");
    if (requestedMode) {
      const response = await fetch(LIST_API, { headers: { Accept: "application/json" } });
      if (!response.ok) throw new Error("No se pudo obtener la oportunidad enviada.");
      const json = await response.json();
      const rows = ((json && json.data) || {}).rows || [];
      return rows.find((row) =>
        row.oportunidad_id === opportunityId && row.crm_modo === requestedMode
      ) || null;
    }

    const url = `${POINT_API}?oportunidad_id=${encodeURIComponent(opportunityId)}`;
    const response = await fetch(url, { headers: { Accept: "application/json" } });
    if (!response.ok) throw new Error("No se pudo obtener la oportunidad enviada.");
    const json = await response.json();
    const data = (json && json.data) || {};
    return data.found ? data.row : null;
  }

  async function init() {
    try {
      const row = await fetchRow();
      if (!row) {
        showError("No encontramos esta oportunidad entre los env\u00edos disponibles.");
        return;
      }
      render(row);
    } catch (error) {
      showError(error.message || "No se pudo cargar el detalle de la oportunidad.");
    }
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
