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

  const fmtMoney = (value) => value == null ? MISSING : new Intl.NumberFormat("es-AR", {
    style: "currency", currency: "ARS", maximumFractionDigits: 0,
  }).format(value);

  const fmtDate = (iso) => {
    if (!iso) return MISSING;
    const value = String(iso);
    const date = value.slice(0, 10).split("-");
    if (date.length !== 3) return value;
    const time = value.slice(11, 16);
    return `${date[2]}/${date[1]}/${date[0]}${time ? ` a las ${time}` : ""}`;
  };

  function setText(id, value) {
    const element = $(id);
    if (element) element.textContent = value == null || value === "" ? MISSING : value;
  }

  function modeBadge(mode) {
    const safeMode = ["prod", "test", "simulado"].includes(mode) ? mode : "simulado";
    return `<span class="ed-badge ed-badge-${safeMode}">${modeLabel(mode)}</span>`;
  }

  function render(row) {
    const displayName = (row.cliente_display && row.cliente_display.texto)
      || row.cliente_visible || MISSING;
    const product = row.producto || MISSING;
    const amount = fmtMoney(row.monto_oportunidad);
    const sentAt = fmtDate(row.enviado_at);

    setText("envDetailTitle", product);
    setText("envDetailClient", row.cliente_visible || displayName);
    setText("envDetailAmount", amount);
    setText("envDetailDate", sentAt);
    $("envDetailMode").innerHTML = modeBadge(row.crm_modo);

    setText("envClientName", displayName);
    setText("envClientCuit", row.cuit || row.crm_cuit_informado);
    setText("envClientAccount", row.cuenta_utilizada);
    setText("envClientOperator", row.operador_nombre);

    setText("envProductName", product);
    setText("envProductCode", row.codigo_articulo);
    setText("envBusinessUnit", row.unidad_negocio);

    setText("envCommercialAmount", amount);
    setText("envCommercialDescription", row.descripcion);

    setText("envAssignedTo", row.asignado_a);
    setText("envAssignmentType", assignmentLabel(row.asignado_origen));

    setText("envSentBy", row.enviado_por);
    setText("envSentAt", sentAt);
    $("envSentMode").innerHTML = modeBadge(row.crm_modo);
    setText("envSentStatus", statusLabel(row));

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
