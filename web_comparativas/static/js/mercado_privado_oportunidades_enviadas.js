/* Repositorio de oportunidades ENVIADAS al CRM.
   Lee /api/mercado-privado/oportunidades/enviadas (todos los entornos, sin filtrar por
   CRM_MODO: acá el entorno es una columna, no un filtro implícito).
   Orden y búsqueda son del lado del cliente: el volumen es el de lo efectivamente
   enviado (decenas, no miles), así que no justifica paginar en el servidor. */
(function () {
  "use strict";

  const API = "/api/mercado-privado/oportunidades/enviadas";
  let ALL = [];
  let SORT = { campo: "enviado_at", desc: true };

  const $ = (id) => document.getElementById(id);
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const fmtMoney = (v) => (v == null ? "—" :
    new Intl.NumberFormat("es-AR", { style: "currency", currency: "ARS", maximumFractionDigits: 0 }).format(v));
  // Fecha ISO (UTC, sin tz) -> dd/mm/yyyy hh:mm, sin corrimiento por zona horaria.
  const fmtFecha = (iso) => {
    if (!iso) return "—";
    const s = String(iso), p = s.slice(0, 10).split("-");
    if (p.length !== 3) return s;
    return `${p[2]}/${p[1]}/${p[0]}<span class="env-sub">${s.slice(11, 16)}</span>`;
  };

  function filtradas() {
    const q = ($("envSearch").value || "").trim().toLowerCase();
    const modo = $("envModo").value;
    const origen = $("envOrigen").value;
    let rows = ALL.filter((r) => {
      if (modo && (r.crm_modo || "") !== modo) return false;
      if (origen && (r.asignado_origen || "") !== origen) return false;
      if (q) {
        const hay = `${r.cliente_visible || ""} ${r.producto || ""} ${r.codigo_articulo || ""} ` +
          `${r.enviado_por || ""} ${r.asignado_a || ""} ${r.crm_id || ""}`.toLowerCase();
        if (!hay.includes(q)) return false;
      }
      return true;
    });
    const c = SORT.campo, signo = SORT.desc ? -1 : 1;
    rows.sort((a, b) => {
      let va = a[c], vb = b[c];
      if (va == null) return 1;          // los vacíos siempre al final, ordene como ordene
      if (vb == null) return -1;
      if (typeof va === "number" && typeof vb === "number") return (va - vb) * signo;
      return String(va).localeCompare(String(vb), "es") * signo;
    });
    return rows;
  }

  function render() {
    const rows = filtradas(), body = $("envBody"), vacio = $("envEmpty");
    $("envShownLabel").textContent = `${rows.length} de ${ALL.length}`;
    body.innerHTML = "";
    if (!rows.length) {
      vacio.style.display = "block";
      vacio.textContent = ALL.length
        ? "Ningún envío coincide con la búsqueda."
        : "Todavía no hay oportunidades enviadas al CRM.";
      return;
    }
    vacio.style.display = "none";
    body.innerHTML = rows.map((r) => {
      const modo = r.crm_modo || "—";
      const manual = r.asignado_origen === "manual"
        ? `<span class="env-manual" title="La eligió a mano quien envió">manual</span>` : "";
      // Sin crm_id no hay a dónde ir: pasa con los envíos simulados.
      const link = r.crm_url
        ? `<a class="env-crm-link" href="${esc(r.crm_url)}" target="_blank" rel="noopener">` +
          `<i class="bi bi-box-arrow-up-right"></i>Ver en CRM</a>`
        : `<span class="env-sin-link">${modo === "simulado" ? "simulado" : "sin id"}</span>`;
      const fueraDeRun = r.en_run_activo ? "" :
        `<span class="env-sub" title="Ya no califica en la corrida activa">fuera de la corrida actual</span>`;
      return `<tr>` +
        `<td class="env-cliente"><span class="env-trunc" title="${esc(r.cliente_visible)}">${esc(r.cliente_visible || "—")}</span>${fueraDeRun}</td>` +
        `<td class="env-prod"><span class="env-trunc" title="${esc(r.producto)}">${esc(r.producto || "—")}</span>` +
          `<span class="env-sub">${esc(r.codigo_articulo || "")}</span></td>` +
        `<td class="env-monto">${fmtMoney(r.monto_oportunidad)}</td>` +
        `<td><span class="env-trunc" title="${esc(r.enviado_por)}">${esc(r.enviado_por || "—")}</span></td>` +
        `<td><span class="env-trunc" title="${esc(r.asignado_a)}">${esc(r.asignado_a || "—")}</span>${manual}</td>` +
        `<td>${fmtFecha(r.enviado_at)}</td>` +
        `<td><span class="env-modo env-modo-${esc(modo)}">${esc(modo)}</span></td>` +
        `<td>${link}</td>` +
        `</tr>`;
    }).join("");
  }

  function marcarOrden() {
    document.querySelectorAll("#oppEnviadasApp thead th[data-sort]").forEach((th) => {
      const activo = th.dataset.sort === SORT.campo;
      th.classList.toggle("env-sorted", activo);
      const flecha = th.querySelector(".env-arrow");
      if (flecha) flecha.textContent = activo ? (SORT.desc ? "↓" : "↑") : "↕";
    });
  }

  async function load() {
    $("envTotalLabel").textContent = "Cargando…";
    try {
      const resp = await fetch(API, { headers: { Accept: "application/json" } });
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      ALL = ((json && json.data) || {}).rows || [];
      $("envTotalLabel").textContent = `${ALL.length} envío${ALL.length === 1 ? "" : "s"}`;
      marcarOrden();
      render();
    } catch (e) {
      $("envTotalLabel").textContent = "Error al cargar";
      $("envBody").innerHTML = "";
      $("envEmpty").style.display = "block";
      $("envEmpty").textContent = `No se pudieron cargar los envíos (${e.message}).`;
    }
  }

  function init() {
    $("envSearch").addEventListener("input", render);
    $("envModo").addEventListener("change", render);
    $("envOrigen").addEventListener("change", render);
    $("envReloadBtn").addEventListener("click", load);
    document.querySelectorAll("#oppEnviadasApp thead th[data-sort]").forEach((th) => {
      th.addEventListener("click", () => {
        const campo = th.dataset.sort;
        // Reclick sobre la misma columna invierte; columna nueva arranca descendente
        // (que es lo útil en fecha y monto, las dos que más se miran).
        if (SORT.campo === campo) SORT.desc = !SORT.desc;
        else SORT = { campo, desc: true };
        marcarOrden();
        render();
      });
    });
    load();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
