/* Oportunidades de Venta (Mercado Privado) — Fase 1B (UI).
   Lee la tabla precalculada vía /api/mercado-privado/oportunidades/list.
   Tabla compacta (10 col) + KPIs (recalculan con filtros) + panel de detalle.
   El envío al CRM lo hace el backend (POST /enviar/{id}); acá solo se muestra el
   payload, se confirma el envío y, una vez que el CRM devuelve un id, el botón
   "Enviar a CRM" pasa a ser "Ver en CRM" apuntando al DetailView. */
(function () {
  "use strict";

  const API = "/api/mercado-privado/oportunidades/list";
  const SEND_API = (id) => `/api/mercado-privado/oportunidades/enviar/${id}`;
  const ACCOUNT_API = (id) => `/api/mercado-privado/oportunidades/cuentas/${id}`;
  let ALL = [];
  let WINDOW = {};
  let CRM_MODO = null;   // 'simulado' | 'test' | 'prod' — entorno al que se enviaría ahora
  let CRM_ASIGNACION = { match: null, usuarios: [], sugerido_id: null, error: null,
                         bitacora_por_usuario: {} };

  // Cómo se nombra cada entorno en el modal. El bloqueo de duplicados es por entorno,
  // así que el usuario tiene que ver SIEMPRE contra cuál está operando.
  // Completa la frase "…se crea la oportunidad ___" del encabezado del modal.
  const MODO_TXT = {
    simulado: "en modo SIMULADO (no se envía nada al CRM)",
    test: "en el CRM de TEST",
    prod: "en el CRM de PRODUCCIÓN",
  };

  const fmtMoney = (v) =>
    new Intl.NumberFormat("es-AR", { style: "currency", currency: "ARS", maximumFractionDigits: 0 }).format(v || 0);
  const fmtMoneyC = (v) => { // compacto para KPI
    const n = Math.abs(v || 0);
    if (n >= 1e9) return "$" + new Intl.NumberFormat("es-AR", { maximumFractionDigits: 2 }).format((v || 0) / 1e9) + "MM";
    if (n >= 1e6) return "$" + new Intl.NumberFormat("es-AR", { maximumFractionDigits: 1 }).format((v || 0) / 1e6) + "M";
    if (n >= 1e3) return "$" + new Intl.NumberFormat("es-AR", { maximumFractionDigits: 0 }).format((v || 0) / 1e3) + "K";
    return fmtMoney(v);
  };
  const fmtNum = (v) =>
    new Intl.NumberFormat("es-AR", { maximumFractionDigits: (v % 1 === 0 ? 0 : 1) }).format(v || 0);
  const fmtPct = (v) => `${Math.round((v || 0) * 100)}%`;
  const esc = (s) => String(s == null ? "" : s).replace(/[&<>"]/g, (c) =>
    ({ "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;" }[c]));
  const $ = (id) => document.getElementById(id);
  // Fecha ISO (UTC, sin tz) -> dd/mm/yyyy, evitando corrimiento por zona horaria.
  const fmtDate = (iso) => {
    if (!iso) return "fecha desconocida";
    const p = String(iso).slice(0, 10).split("-");
    return p.length === 3 ? `${p[2]}/${p[1]}/${p[0]}` : iso;
  };

  // Mini-medidor de efectividad: número coloreado por umbral + barra de progreso.
  const efMeter = (v) => {
    const pct = Math.round((v || 0) * 100);
    const cls = pct < 30 ? "ef-lo" : pct <= 45 ? "ef-mid" : "ef-hi";
    const w = Math.min(100, Math.max(0, pct));
    return `<div class="opp-c-efect">` +
      `<span class="opp-ef-num ${cls}">${pct}%</span>` +
      `<span class="opp-ef-track"><span class="opp-ef-fill ${cls}" style="width:${w}%"></span></span>` +
      `</div>`;
  };

  // ── Filtros / orden ──
  function getFilters() {
    return {
      tipo: $("fTipo").value, actividad: $("fActividad").value,
      familia: $("fFamilia").value, unidad: $("fUnidad").value,
      search: ($("fSearch").value || "").trim().toLowerCase(),
      sort: $("fSort").value || "score",
    };
  }
  function filteredRows() {
    const f = getFilters();
    let rows = ALL.filter((o) => {
      if (f.tipo && o.tipo_oportunidad !== f.tipo) return false;
      if (f.actividad && o.estado_actividad !== f.actividad) return false;
      if (f.familia && (o.familia || "") !== f.familia) return false;
      if (f.unidad && (o.unidad_negocio || "") !== f.unidad) return false;
      if (f.search) {
        const hay = `${o.cliente_visible || ""} ${o.producto_nombre || ""} ${o.codigo_articulo || ""}`.toLowerCase();
        if (!hay.includes(f.search)) return false;
      }
      return true;
    });
    rows.sort((a, b) => (b[f.sort] || 0) - (a[f.sort] || 0));
    return rows;
  }
  function applyFilters() {
    const rows = filteredRows();
    render(rows);
    updateKpis(rows);
  }

  // ── KPIs (sobre el conjunto filtrado) ──
  function updateKpis(rows) {
    const n = rows.length;
    const monto = rows.reduce((s, o) => s + (o.monto_oportunidad || 0), 0);
    const repetida = rows.filter((o) => o.tipo_oportunidad === "ESTABLE" || o.tipo_oportunidad === "RECURRENTE").length;
    const activas = rows.filter((o) => o.estado_actividad === "ACTIVA").length;
    const efProm = n ? rows.reduce((s, o) => s + (o.efectividad || 0), 0) / n : 0;
    $("kpiMonto").textContent = fmtMoneyC(monto);
    $("kpiMonto").title = fmtMoney(monto);
    $("kpiCount").textContent = new Intl.NumberFormat("es-AR").format(n);
    $("kpiRepetida").textContent = new Intl.NumberFormat("es-AR").format(repetida);
    $("kpiActivas").textContent = new Intl.NumberFormat("es-AR").format(activas);
    $("kpiEfect").textContent = fmtPct(efProm);
  }

  // Botón de CRM en la fila. El estado sale de `o.envio`, que el backend ya resuelve
  // POR ENTORNO vigente (igual que el badge ENVIADA): una oportunidad enviada a TEST
  // aparece como pendiente si se está operando contra PROD, que es lo correcto.
  //   - sin enviar        -> abre el modal de payload (mismo flujo de siempre)
  //   - enviada con id    -> lleva directo al registro del CRM, sin pasar por el modal
  //   - enviada sin id    -> no hay a dónde ir (envío simulado): botón inerte
  function accionCrmHtml(o) {
    const env = o.envio || {};
    if (env.enviado && env.crm_url) {
      return `<button class="opp-detail-btn opp-crm-btn opp-crm-ok" type="button" ` +
        `data-crm-url="${esc(env.crm_url)}" title="Ver esta oportunidad en el CRM" ` +
        `aria-label="Ver en CRM"><i class="bi bi-box-arrow-up-right"></i></button>`;
    }
    if (env.enviado) {
      return `<button class="opp-detail-btn opp-crm-btn opp-crm-ok" type="button" disabled ` +
        `title="Enviada en modo simulado: no hay registro en el CRM" ` +
        `aria-label="Enviada sin registro en el CRM"><i class="bi bi-check2-circle"></i></button>`;
    }
    return `<button class="opp-detail-btn opp-crm-btn" type="button" ` +
      `title="Enviar a CRM" aria-label="Enviar a CRM"><i class="bi bi-send"></i></button>`;
  }

  // ── Render lista de registros (CSS grid, no tabla de celdas) ──
  function render(rows) {
    const body = $("oppBody"), empty = $("oppEmpty");
    body.innerHTML = "";
    $("oppShownLabel").textContent = `${rows.length} de ${ALL.length}`;
    if (!rows.length) { empty.style.display = "block"; return; }
    empty.style.display = "none";
    const frag = document.createDocumentFragment();
    rows.forEach((o) => {
      const tipo = o.tipo_oportunidad || "PUNTUAL";
      const cliente = o.cliente_visible || "—";
      const producto = o.producto_nombre || "—";
      const negocio = o.unidad_negocio || "—";
      const metaTitle = `${tipo} · ${producto} · ${negocio}`;
      const row = document.createElement("div");
      row.className = `opp-row opp-rail-${esc(tipo)}`;
      row.setAttribute("role", "row");
      row.tabIndex = 0;
      const sent = o.envio && o.envio.enviado;
      const sentBadge = sent
        ? `<span class="opp-sent-badge" title="Enviada al CRM por ${esc((o.envio && o.envio.enviado_por) || "")}"><i class="bi bi-send-check"></i>Enviada</span>`
        : "";
      row.innerHTML =
        `<div class="opp-primary">` +
          `<div class="opp-client" title="${esc(cliente)}">${esc(cliente)}${sentBadge}</div>` +
          `<div class="opp-meta" title="${esc(metaTitle)}">` +
            `<span class="opp-tl opp-tl-${esc(tipo)}">${esc(tipo)}</span>` +
            `<span class="opp-sep">·</span>${esc(producto)}` +
            `<span class="opp-sep">·</span>${esc(negocio)}` +
          `</div>` +
        `</div>` +
        `<div class="opp-c-umes">${fmtNum(o.consumo_tipico_mensual)}</div>` +
        efMeter(o.efectividad) +
        `<div class="opp-c-monto">` +
          `<div class="opp-monto-v">${fmtMoney(o.monto_oportunidad)}</div>` +
          `<div class="opp-precio-sub">${fmtMoney(o.precio_unitario_estimado)} c/u</div>` +
        `</div>` +
        `<div class="opp-c-action">` +
          accionCrmHtml(o) +
          `<button class="opp-detail-btn" type="button" aria-label="Ver detalle de la oportunidad" title="Ver detalle"><i class="bi bi-eye"></i></button>` +
        `</div>`;
      // El click en la fila abre el detalle, pero el botón de CRM tiene lo suyo: se
      // frena la propagación para que no dispare las dos cosas a la vez.
      const btnCrm = row.querySelector(".opp-crm-btn");
      if (btnCrm) {
        btnCrm.addEventListener("click", (e) => {
          e.stopPropagation();
          const url = btnCrm.dataset.crmUrl;
          if (url) window.open(url, "_blank", "noopener");   // ya enviada -> directo al CRM
          else showCrm(o);                                   // sin enviar -> modal de payload
        });
      }
      row.addEventListener("click", () => showDetail(o));
      row.addEventListener("keydown", (e) => {
        if (e.key === "Enter" || e.key === " ") { e.preventDefault(); showDetail(o); }
      });
      frag.appendChild(row);
    });
    body.appendChild(frag);
  }

  // ── Panel de detalle (listado agrupado por secciones) ──
  let detailModal = null, crmModal = null;
  const row = (k, v, opts) =>
    `<div class="od-row${opts && opts.hero ? " od-hero" : ""}"><span class="od-label">${esc(k)}</span><span class="od-value">${v}</span></div>`;
  const section = (title, rowsHtml) =>
    `<div class="od-section"><div class="od-section-title">${esc(title)}</div>${rowsHtml}</div>`;
  function showDetail(o) {
    const tipo = o.tipo_oportunidad || "PUNTUAL";
    const act = o.estado_actividad === "ACTIVA" ? "Activa" : "Dormida";
    const rango = `${fmtNum(o.consumo_min_mensual)} – ${fmtNum(o.consumo_max_mensual)} u`;
    const ultima = o.ultima_demanda
      ? `${esc(o.ultima_demanda)} <span class="od-sub">(hace ${o.meses_desde_ultima_demanda} m)</span>` : "s/d";
    const efPct = Math.round((o.efectividad || 0) * 100);
    const efCls = efPct < 30 ? "ef-lo" : efPct <= 45 ? "ef-mid" : "ef-hi";
    const efVal = `<span class="od-ef ${efCls}">${efPct}%</span>`;
    $("detailBody").innerHTML =
      `<div class="od-identity od-rail-${esc(tipo)}">` +
        `<div class="od-chip od-chip-${esc(tipo)}">${esc(tipo)} · ${esc(act)}</div>` +
        `<div class="od-client" title="${esc(o.cliente_visible || "—")}">${esc(o.cliente_visible || "—")}</div>` +
        `<div class="od-prod"><i class="bi bi-capsule"></i>${esc(o.producto_nombre || "—")}</div>` +
      `</div>` +
      section("Producto",
        row("Negocio", esc(o.unidad_negocio || "—")) +
        row("Familia", esc(o.familia || "—")) +
        row("Código de artículo", esc(o.codigo_articulo || "—")) +
        row("Provincia", esc(o.provincia || "—"))
      ) +
      section("Demanda",
        row("Consumo típico", `${fmtNum(o.consumo_tipico_mensual)} u/mes`) +
        row("Rango mensual", rango) +
        row("Aparece en", `${o.meses_demanda_cliente_12m} de ${o.ventana_meses} meses`) +
        row("No participado", `${o.meses_no_participo_12m} de ${o.ventana_meses} meses`) +
        row("Última demanda", ultima)
      ) +
      section("Desempeño comercial",
        row("Efectividad", efVal) +
        row("Adjudicaciones ganadas", `${o.ganados}`) +
        row("Clientes distintos", `${o.clientes_distintos}`)
      ) +
      section("Valorización",
        row("Precio unitario", fmtMoney(o.precio_unitario_estimado)) +
        row("Monto recuperable / mes", fmtMoney(o.monto_oportunidad), { hero: true }) +
        row("Score", fmtMoney(o.score))
      );
    // Ya enviada y con id de CRM -> el botón lleva directo al registro del CRM.
    const crmUrl = (o.envio && o.envio.crm_url) || null;
    const detailBtn = $("detailCrmBtn");
    if (crmUrl) {
      detailBtn.innerHTML = `<i class="bi bi-box-arrow-up-right me-1"></i>Ver en CRM`;
      detailBtn.setAttribute("aria-label", "Ver la oportunidad en el CRM");
      detailBtn.onclick = () => window.open(crmUrl, "_blank", "noopener");
    } else {
      detailBtn.innerHTML = `<i class="bi bi-send me-1"></i>Enviar a CRM`;
      detailBtn.setAttribute("aria-label", "Enviar a CRM");
      detailBtn.onclick = () => {
        if (detailModal) detailModal.hide();
        showCrm(o);
      };
    }
    if (!detailModal) detailModal = new bootstrap.Modal($("detailModal"));
    detailModal.show();
  }

  // ── Modal payload CRM ──
  // El modal es solo presentación: los campos técnicos (NAME, CURRENCY_ID, AMOUNT,
  // ASSIGNED_USER, CUENTA_*, CRM_*, OPERADOR_*, el JSON completo, etc.) ya NO se
  // muestran — el usuario ve el bloque de cuenta (reflectCuenta) y el selector de
  // asignación (reflectAsignacion), nada más. Nada de esto cambia lo que se guarda:
  // `payload` sigue armándose igual y viajando igual al backend, que arma su PROPIO
  // payload server-side (_build_crm_payload) y lo persiste en payload_snapshot /
  // crm_envio_eventos sin depender de esta función en absoluto. Se deja como no-op
  // (en vez de borrar las 3 llamadas que la invocan) para no tocar más superficie de
  // la que hace falta.
  function renderCrmFields() { return; }

  // Banner de estado del envío (warning=duplicado, success=ok, danger=error).
  function setCrmStatus(kind, html) {
    const el = $("crmStatusMsg");
    if (!html) { el.style.display = "none"; el.innerHTML = ""; return; }
    el.className = `alert py-2 small mb-3 alert-${kind}`;
    el.innerHTML = html;
    el.style.display = "block";
  }

  // Muestra u oculta el enlace "Ver en CRM" del footer según haya id del CRM.
  function reflectCrmLink(crmUrl) {
    const link = $("crmViewBtn");
    if (!link) return;
    if (crmUrl) {
      link.href = crmUrl;
      link.style.display = "";
    } else {
      link.removeAttribute("href");
      link.style.display = "none";
    }
  }

  function selectedAccount(o) {
    const resolution = o._cuentaResolucion || {};
    return resolution.cuenta_seleccionada || null;
  }

  function renderResolvedPayload(o) {
    const crmInfo = o.crm || {};
    const payload = Object.assign({}, crmInfo.payload || {});
    const resolution = o._cuentaResolucion || {};
    const account = selectedAccount(o);
    if (account) {
      payload.n_cuenta = account.cuenta;
      payload.cuenta_original = resolution.cuenta_original || null;
      payload.cuenta_utilizada = account.cuenta;
      payload.cuenta_criterio = resolution.criterio_seleccion || null;
      payload.cuenta_estado_confianza = resolution.estado_confianza || null;
      payload.cuenta_confianza_label = resolution.confianza_label || null;
      payload.cuenta_confirmacion_fiscal = !!resolution.confirmacion_fiscal;
      payload.cuenta_seleccion_origen = resolution.seleccion_origen || null;
      payload.crm_cuit_informado = account.crm_cuit || null;
      payload.crm_razon_social_informada = account.crm_razon_social || null;
      payload.operador_codigo = account.operador_codigo || null;
      payload.operador_nombre = account.operador_nombre || null;
      payload.fuente_relacion_cuenta = resolution.fuente_relacion || null;
      payload.cuentas_evaluadas = resolution.cantidad_candidatas_total ?? (resolution.cuentas_candidatas || []).length;
      const trace = resolution.trazabilidad_texto || account.trazabilidad_seleccion;
      if (trace) payload.update_text = `${payload.update_text || ""} ${trace}`.trim();
    }
    const users = CRM_ASIGNACION.usuarios || [];
    const assignedSelect = $("crmAsignadoSel");
    const assigned = assignedSelect ? users.find((u) => u.id === assignedSelect.value) : null;
    const match = CRM_ASIGNACION.match || null;
    if (assigned) {
      payload.assigned_user = match && assigned.id === match.id
        ? `asignado a vos (${assigned.usuario})`
        : assigned.es_sistema
          ? `asignado a ${assigned.usuario} (usuario de sistema)`
          : `asignado a ${assigned.usuario} (selección manual)`;
    }
    const logs = CRM_ASIGNACION.bitacora_por_usuario || {};
    if (assigned && logs[assigned.id]) {
      const trace = account && (resolution.trazabilidad_texto || account.trazabilidad_seleccion);
      payload.update_text = `${logs[assigned.id]}${trace ? ` ${trace}` : ""}`;
    }
    renderCrmFields(payload, crmInfo.pendientes_crm, crmInfo.faltantes_dataset);
  }

  function syncCrmSendButton(o) {
    const btn = $("crmSendBtn");
    if (!btn || (o.envio && o.envio.enviado)) return;
    const staticBlocks = ((o.crm || {}).bloqueos || []).length > 0;
    const assignedBox = $("crmAsignacionBox"), assignedSelect = $("crmAsignadoSel");
    const missingAssigned = assignedBox && assignedBox.style.display !== "none" && !assignedSelect.value;
    const state = o._cuentaResolucionEstado;
    const resolution = o._cuentaResolucion || {};
    if (staticBlocks) {
      btn.disabled = true;
      btn.innerHTML = `<i class="bi bi-slash-circle me-1"></i>No se puede enviar`;
    } else if (state === "loading") {
      btn.disabled = true;
      btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Validando cuenta...`;
    } else if (state === "error" || !selectedAccount(o)) {
      btn.disabled = true;
      btn.innerHTML = resolution.requiere_seleccion
        ? `<i class="bi bi-diagram-3 me-1"></i>Elegí una cuenta`
        : `<i class="bi bi-slash-circle me-1"></i>Cuenta no disponible`;
    } else if (missingAssigned) {
      btn.disabled = true;
      btn.innerHTML = `<i class="bi bi-person-exclamation me-1"></i>Elegí un usuario`;
    } else {
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
    }
  }

  function reflectCuenta(o) {
    const box = $("crmCuentaBox"), selectWrap = $("crmCuentaSelectorWrap");
    const select = $("crmCuentaSel"), details = $("crmCuentaDetails");
    if (!box || !select || !details) return;
    if (o._cuentaResolucionEstado === "loading") {
      box.className = "alert py-2 small mb-3 alert-secondary";
      details.innerHTML = `<span class="spinner-border spinner-border-sm me-2"></span>Validando cuentas relacionadas en ${esc(MODO_TXT[CRM_MODO] || CRM_MODO || "CRM")}...`;
      selectWrap.style.display = "none";
      box.style.display = "block";
      syncCrmSendButton(o);
      return;
    }
    if (o._cuentaResolucionEstado === "error") {
      box.className = "alert py-2 small mb-3 alert-danger";
      details.innerHTML = `<strong>No se pudo validar la cuenta.</strong> ${esc(o._cuentaResolucionError || "Intentá nuevamente.")}`;
      selectWrap.style.display = "none";
      box.style.display = "block";
      syncCrmSendButton(o);
      return;
    }
    const resolution = o._cuentaResolucion || {};
    const account = selectedAccount(o);
    const found = resolution.cuentas_encontradas_en_crm || [];
    const confidence = resolution.estado_confianza || "SIN_RELACION";
    let kind = "success";
    if (confidence === "ALTERNATIVA_CONFIRMADA_POR_CUIT") kind = "info";
    if (confidence === "ALTERNATIVA_RELACIONADA_POR_NOMBRE_EXACTO_NO_AMBIGUO" || resolution.requiere_seleccion) kind = "warning";
    if (["RELACION_AMBIGUA", "SIN_RELACION", "ERROR_CONSULTA_CRM"].includes(confidence)) kind = "danger";
    box.className = `alert py-2 small mb-3 alert-${kind}`;
    const operator = account
      ? [account.operador_codigo, account.operador_nombre].filter(Boolean).join(" - ") || "sin operador"
      : "pendiente de selección";
    // Nombre del cliente: el del dataset y el que informó el CRM. La nota aclaratoria
    // solo aparece cuando son distintos; si son el mismo nombre, explicar la diferencia
    // confundiría en vez de ayudar. Lo arma el backend (`cliente_display`).
    const display = resolution.cliente_display;
    const clienteHtml = display
      ? `<div><strong>Cliente:</strong> ${esc(display.texto)}</div>` +
        (display.dos_nombres
          ? `<div class="text-muted" style="font-size:.85em;">SIEM usa la razón social y el CRM el nombre de fantasía; es el mismo cliente.</div>`
          : "")
      : "";
    // Solo presentación: 4 datos, en este orden. Todo lo demás (cuenta original,
    // estado de confianza, criterio, cantidad evaluada, CUIT del CRM) se sigue
    // calculando arriba para el color/lógica del bloque, pero no se dibuja acá.
    details.innerHTML =
      clienteHtml +
      `<div><strong>CUIT:</strong> ${esc(resolution.cuit || "sin dato")}</div>` +
      `<div><strong>Cuenta:</strong> ${esc(account ? account.cuenta : "pendiente")}</div>` +
      `<div><strong>Operador:</strong> ${esc(operator)}</div>`;
    if (resolution.requiere_seleccion) {
      select.innerHTML = `<option value="">-- Elegí una cuenta --</option>` + found.map((row) => {
        const op = [row.operador_codigo, row.operador_nombre].filter(Boolean).join(" - ");
        return `<option value="${esc(row.cuenta)}">${esc(row.cuenta)} - ${esc(row.crm_nombre || row.nombre_cliente || "cuenta CRM")}${op ? ` - ${esc(op)}` : ""}</option>`;
      }).join("");
      select.value = account ? account.cuenta : "";
      select.onchange = () => {
        const chosen = found.find((row) => row.cuenta === select.value) || null;
        resolution.cuenta_seleccionada = chosen;
        resolution.criterio_seleccion = chosen ? "seleccion_manual_entre_alternativas" : "multiples_alternativas";
        resolution.estado_confianza = chosen ? chosen.estado_confianza : "SIN_RELACION";
        resolution.confianza_label = chosen ? chosen.confianza_label : "Seleccione una cuenta";
        resolution.confirmacion_fiscal = !!(chosen && chosen.confirmacion_fiscal);
        resolution.seleccion_origen = chosen ? "manual" : "ninguna";
        resolution.trazabilidad_texto = chosen ? chosen.trazabilidad_seleccion : null;
        resolution.bloqueado = !chosen;
        // Cada candidata trae su texto ya armado por el backend.
        resolution.cliente_display = chosen ? chosen.cliente_display : null;
        renderResolvedPayload(o);
        reflectCuenta(o);
        // La cuenta elegida a mano puede traer OTRO operador (u ninguno): recalcula
        // el selector de asignación para reflejar el bloqueo correcto.
        if (!(o.envio && o.envio.enviado)) reflectAsignacion(o);
      };
      selectWrap.style.display = "block";
    } else {
      selectWrap.style.display = "none";
    }
    box.style.display = "block";
    syncCrmSendButton(o);
  }

  async function loadAccountResolution(o) {
    o._cuentaResolucionEstado = "loading";
    o._cuentaResolucion = null;
    reflectCuenta(o);
    try {
      const response = await fetch(ACCOUNT_API(o.id), { headers: { Accept: "application/json" } });
      const json = await response.json().catch(() => ({}));
      if (!response.ok) throw new Error(mensajeDeError(response.status, json));
      o._cuentaResolucion = (json && json.data) || {};
      o._cuentaResolucionEstado = "ready";
      renderResolvedPayload(o);
      reflectCuenta(o);
      // El operador (y su usuario del CRM, si tiene) recién se conoce acá: recalcula
      // el selector de asignación para aplicar el bloqueo si corresponde.
      if (!(o.envio && o.envio.enviado)) reflectAsignacion(o);
    } catch (error) {
      o._cuentaResolucionEstado = "error";
      o._cuentaResolucionError = error.message;
      reflectCuenta(o);
    }
  }
  // Selector de asignación. Devuelve true si quedó visible. Se dibuja AL ABRIR el
  // modal, no después de un envío rechazado: quien envía tiene que ver de entrada a
  // quién se va a asignar y poder cambiarlo. El 422 del backend queda solo como
  // defensa; el usuario no debería llegar nunca a verlo.
  //
  // Tres estados posibles (ago-2026):
  //   1. La cuenta tiene un operador de Fusión con usuario propio en el CRM: se
  //      asigna SIEMPRE a esa persona (es la dueña real de la cuenta), sin selector.
  //      Se recalcula cuando termina de resolver la cuenta (`loadAccountResolution`
  //      llama de nuevo a esta función), así que hasta que eso pase puede mostrarse
  //      brevemente el estado 2 y corregirse solo.
  //   2. Sin ese operador: selector acotado a la estructura de quien envía (Supervisor
  //      = su equipo, Gerente = su estructura, Admin/Auditor = todos) — ya viene
  //      acotado desde el backend en CRM_ASIGNACION.usuarios.
  //   3. Analista: no elige nunca, no ve selector (sin cambios).
  function reflectAsignacion(o) {
    const box = $("crmAsignacionBox"), sel = $("crmAsignadoSel");
    const crmInfo = o.crm || {};
    if (!box || !sel) return false;
    if (crmInfo.puede_elegir === false) {
      box.style.display = "none";
      return false;
    }

    const account = selectedAccount(o);
    const operadorMatch = account ? account.operador_asignado_crm : null;

    if (operadorMatch) {
      sel.innerHTML = `<option value="${esc(operadorMatch.id)}">${esc(operadorMatch.usuario)}</option>`;
      sel.value = operadorMatch.id;
      sel.disabled = true;
      box.className = "alert py-2 small mb-3 alert-info";
      $("crmAsignacionMsg").innerHTML =
        `<i class="bi bi-person-check me-1"></i>Esta cuenta es de <strong>${esc(operadorMatch.usuario)}</strong>. ` +
        `Se va a asignar a esa persona en el CRM.`;
      $("crmAsignacionNota").textContent = "Vinculado por el operador de Fusión de la cuenta.";
      box.style.display = "block";
      const syncLocked = () => { renderResolvedPayload(o); syncCrmSendButton(o); };
      sel.onchange = syncLocked;
      syncLocked();
      return true;
    }

    sel.disabled = false;
    const usuarios = CRM_ASIGNACION.usuarios || [];
    // Sin usuarios no hay nada que ofrecer: de eso se ocupa el bloqueo del botón.
    if (!usuarios.length) {
      box.style.display = "none";
      return false;
    }

    const match = CRM_ASIGNACION.match || null;
    // Con match propio viene preseleccionado pero editable (supervisor o superior puede
    // reasignar). Sin match arranca vacío a propósito: obliga a un acto deliberado en vez
    // de dejar al primero de la lista listo para un envío distraído.
    sel.innerHTML = `<option value="">— Elegí un usuario —</option>` +
      usuarios.map((u) => `<option value="${esc(u.id)}">${esc(u.usuario)}</option>`).join("");
    sel.value = match ? match.id : "";

    box.className = `alert py-2 small mb-3 alert-${match ? "info" : "warning"}`;
    $("crmAsignacionMsg").innerHTML = match
      ? `<i class="bi bi-person-check me-1"></i>Se va a asignar a <strong>vos</strong> ` +
        `(${esc(match.usuario)}). Podés cambiarlo si corresponde.`
      : `<i class="bi bi-person-exclamation me-1"></i>Tu usuario de SIEM no coincide con ` +
        `ningún usuario del CRM. <strong>Elegí a quién asignar</strong> esta oportunidad.`;
    $("crmAsignacionNota").textContent = match
      ? "Si elegís a otra persona, queda registrado que la asignaste vos."
      : "Queda registrado que la asignaste vos.";
    box.style.display = "block";

    const sync = () => {
      renderResolvedPayload(o);
      syncCrmSendButton(o);
    };
    sel.onchange = sync;
    sync();
    return true;
  }

  // Bloqueos: faltan datos sin los cuales el envío no puede salir. Deshabilita el botón
  // y explica por qué. Tiene prioridad sobre cualquier otro estado del modal: es mejor
  // no poder apretar el botón que apretarlo y que el CRM rechace (o peor, que entre mal).
  // Devuelve true si quedó bloqueado.
  function reflectBloqueos(o) {
    const bloqueos = ((o.crm || {}).bloqueos) || [];
    if (!bloqueos.length) return false;
    const btn = $("crmSendBtn");
    setCrmStatus("danger",
      `<i class="bi bi-slash-circle me-1"></i><strong>No se puede enviar esta oportunidad.</strong>` +
      `<ul class="mb-0 mt-1 ps-3">${bloqueos.map((b) => `<li>${esc(b)}</li>`).join("")}</ul>`);
    btn.disabled = true;
    btn.innerHTML = `<i class="bi bi-slash-circle me-1"></i>No se puede enviar`;
    reflectCrmLink(null);
    return true;
  }

  // Refleja el estado "ya enviada" en el modal (banner + botón deshabilitado).
  function reflectSent(o) {
    const env = o.envio || {};
    const btn = $("crmSendBtn");
    if (env.enviado) {
      const detalle = env.crm_id
        ? ` Quedó registrada en el CRM con el id <strong>${esc(env.crm_id)}</strong>.`
        : "";
      setCrmStatus("warning",
        `<i class="bi bi-exclamation-triangle me-1"></i>Esta oportunidad ya fue enviada al CRM por ` +
        `<strong>${esc(env.enviado_por || "—")}</strong> el <strong>${esc(fmtDate(env.enviado_at))}</strong>.` +
        detalle);
      btn.disabled = true;
      btn.innerHTML = `<i class="bi bi-check2-circle me-1"></i>Ya enviada`;
    } else {
      setCrmStatus(null, null);
      btn.disabled = false;
      btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
    }
    reflectCrmLink(env.crm_url);
  }

  // Traduce una respuesta de error a algo que le sirva a quien está mirando la pantalla.
  // NUNCA se muestra un código HTTP: "HTTP 422" no le dice nada a un comercial y encima
  // suena a que se rompió el sistema, cuando casi siempre falta un dato o el CRM no está.
  // El `detail` del backend ya viene redactado para leerse; el mapa es solo la red por si
  // alguna respuesta llega sin él.
  function mensajeDeError(status, json) {
    // OJO: en rutas /api/ la app tiene un handler global (main.py) que reempaqueta las
    // HTTPException como {"error": <detail>, "status": <code>} — NO como {"detail": ...}.
    // Leer solo `detail` hacía que TODOS los mensajes del backend se perdieran y siempre
    // se mostrara el genérico de abajo, que además desorientaba ("faltan datos" cuando en
    // realidad el CRM no había respondido). Se leen las dos formas.
    const delBackend = json && (json.detail || json.error);
    if (delBackend) return delBackend;
    if (status === 401) return "Tu sesión expiró. Volvé a iniciar sesión e intentá de nuevo.";
    if (status === 403) return "No tenés permiso para enviar esta oportunidad.";
    if (status === 404) return "La oportunidad ya no está disponible. Actualizá la lista.";
    if (status === 422) return "Faltan datos para poder enviar esta oportunidad.";
    if (status >= 500) return "El CRM no está disponible en este momento. Probá de nuevo en unos minutos.";
    return "No se pudo completar el envío. Probá de nuevo.";
  }

  // POST /enviar/{id}: sella el envío server-side y maneja duplicado/éxito/error.
  function removeFromPending(o) {
    ALL = ALL.filter((row) => row.oportunidad_id !== o.oportunidad_id);
  }

  async function sendCrm(o) {
    const btn = $("crmSendBtn");
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Enviando…`;
    try {
      // Si hubo selección manual, viaja el id elegido. El backend igual lo valida
      // contra la lista real del CRM: acá solo se transmite la decisión.
      // Se manda la elección siempre que el selector esté en juego (el supervisor lo ve
      // aunque tenga match, porque puede reasignar). El backend la revalida igual.
      const sel = $("crmAsignadoSel");
      const puedeElegir = o.crm && o.crm.puede_elegir !== false;
      const elegido = (puedeElegir && sel) ? sel.value : "";
      const query = new URLSearchParams();
      if (elegido) query.set("assigned_user_id", elegido);
      const account = selectedAccount(o);
      if (account) query.set("cuenta_seleccionada", account.cuenta);
      if (account && (o._cuentaResolucion || {}).seleccion_origen === "manual") {
        query.set("cuenta_seleccion_manual", "true");
      }
      const url = SEND_API(o.id) + (query.toString() ? `?${query}` : "");
      const resp = await fetch(url, { method: "POST", headers: { Accept: "application/json" } });
      const json = await resp.json().catch(() => ({}));
      if (resp.status === 403) {
        setCrmStatus("danger",
          `<i class="bi bi-shield-lock me-1"></i>${esc(mensajeDeError(403, json))}`);
        btn.disabled = false; btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
        return;
      }
      if (!resp.ok) throw new Error(mensajeDeError(resp.status, json));
      // Bloqueo de duplicado: el backend devuelve ok:false + quién/cuándo.
      if (json.ok === false && json.status === "duplicado") {
        o.envio = {
          enviado: true, enviado_por: json.enviado_por, enviado_at: json.enviado_at,
          crm_id: json.crm_id, crm_url: json.crm_url,
        };
        reflectSent(o);
        removeFromPending(o);
        applyFilters();
        return;
      }
      // Éxito: sella estado local, refresca el payload mostrado (incluye enviado_por/at).
      o.envio = {
        enviado: true, enviado_por: json.enviado_por, enviado_at: json.enviado_at,
        crm_status: json.crm_status, crm_id: json.crm_id, crm_url: json.crm_url,
        crm_modo: json.crm_modo,
      };
      if (json.payload) renderCrmFields(json.payload, json.pendientes_crm, json.faltantes_dataset);
      // El detalle del cierre depende de si fue simulacro, envío real, o real con la
      // bitácora del CRM fallada (la oportunidad igual quedó creada).
      let cierre;
      if (json.crm_id) {
        cierre = `<span class="text-muted">Creada en el CRM (${esc(json.crm_modo || "")}) con el id ` +
          `<strong>${esc(json.crm_id)}</strong>.</span>`;
        if (json.bitacora_error) {
          cierre += `<br><i class="bi bi-exclamation-triangle me-1"></i>` +
            `<span class="text-muted">La oportunidad se creó, pero no se pudo dejar la bitácora: ` +
            `${esc(json.bitacora_error)}</span>`;
        }
      } else {
        cierre = `<span class="text-muted">Envío simulado (CRM_ENVIO_PLACEHOLDER activo): ` +
          `no se envió nada al CRM real.</span>`;
      }
      // Quién envió y a quién quedó asignada son dos datos distintos: se muestran los dos.
      const asignada = json.assigned_user
        ? ` Asignada a <strong>${esc(json.assigned_user)}</strong>` +
          (json.usuario_origen === "manual" ? " (selección manual)." : ".")
        : "";
      $("crmAsignacionBox").style.display = "none";
      setCrmStatus("success",
        `<i class="bi bi-check2-circle me-1"></i>Envío registrado por <strong>${esc(json.enviado_por)}</strong>.` +
        asignada + " " + cierre);
      btn.disabled = true; btn.innerHTML = `<i class="bi bi-check2-circle me-1"></i>Enviada`;
      reflectCrmLink(json.crm_url);
      removeFromPending(o);
      applyFilters(); // refresca el badge "Enviada" en la lista
    } catch (e) {
      setCrmStatus("danger", `<i class="bi bi-x-circle me-1"></i>No se pudo enviar. ${esc(e.message)}`);
      btn.disabled = false; btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
    }
  }

  function showCrm(o) {
    const crm = o.crm || {}, payload = crm.payload || {};
    const modoEl = $("crmModoLabel");
    if (modoEl) modoEl.textContent = CRM_MODO ? (MODO_TXT[CRM_MODO] || CRM_MODO) : "";
    o._cuentaResolucionEstado = "loading";
    o._cuentaResolucion = null;
    renderCrmFields(payload, crm.pendientes_crm, crm.faltantes_dataset);
    reflectSent(o);
    if (!(o.envio && o.envio.enviado)) reflectAsignacion(o);
    else $("crmAsignacionBox").style.display = "none";
    reflectBloqueos(o);
    reflectCuenta(o);
    $("crmSendBtn").onclick = () => sendCrm(o);
    if (!crmModal) crmModal = new bootstrap.Modal($("crmModal"));
    crmModal.show();
    if (!(o.envio && o.envio.enviado)) loadAccountResolution(o);
  }
  function fillSelect(id, values) {
    const sel = $(id), cur = sel.value;
    sel.innerHTML = `<option value="">Todas</option>` +
      values.map((v) => `<option value="${esc(v)}">${esc(v)}</option>`).join("");
    sel.value = cur;
  }

  async function load() {
    $("oppWindowLabel").textContent = "Cargando…";
    try {
      const resp = await fetch(API, { headers: { Accept: "application/json" } });
      if (!resp.ok) throw new Error(mensajeDeError(resp.status, null));
      const json = await resp.json();
      const data = (json && json.data) || {};
      ALL = data.rows || [];
      WINDOW = data.window || {};
      CRM_MODO = data.crm_modo || null;
      CRM_ASIGNACION = data.crm_asignacion ||
        { match: null, usuarios: [], sugerido_id: null, error: null, bitacora_por_usuario: {} };
      $("oppWindowLabel").textContent = WINDOW.label ? `Demanda analizada: ${WINDOW.label}` : "Período no disponible";
      fillSelect("fFamilia", [...new Set(ALL.map((o) => o.familia).filter(Boolean))].sort());
      fillSelect("fUnidad", [...new Set(ALL.map((o) => o.unidad_negocio).filter(Boolean))].sort());
      applyFilters();
      abrirDesdeDeepLink();
    } catch (e) {
      $("oppWindowLabel").textContent = "Error al cargar";
      $("oppBody").innerHTML =
        `<div class="opp-empty">No se pudieron cargar las oportunidades. ${esc(e.message)}</div>`;
    }
  }

  // ── Deep-link "Ver en SIEM" (lo usa el botón del lado del CRM) ──
  // URL: /mercado-privado/oportunidades?oportunidad_id=<id_sistema_origen_c>
  // Ese id es el MISMO que viaja al CRM en `id_sistema_origen_c` (sha1 estable de
  // cliente+codigo), así que el CRM puede armar el link concatenando y sin guardar nada
  // más. Se resuelve del lado del cliente contra las filas ya cargadas: no hace falta
  // endpoint nuevo ni una segunda consulta.
  function abrirDesdeDeepLink() {
    let pedido;
    try {
      pedido = new URLSearchParams(window.location.search).get("oportunidad_id");
    } catch (e) { return; }
    if (!pedido) return;
    pedido = pedido.trim().toLowerCase();
    const o = ALL.find((x) => String(x.oportunidad_id || "").toLowerCase() === pedido);
    if (o) { showDetail(o); return; }
    // No está en la corrida activa: puede haber dejado de calificar en el último
    // recálculo. Se avisa explícitamente en vez de abrir la lista como si nada.
    const empty = $("oppEmpty");
    $("oppBody").innerHTML = "";
    if (empty) {
      empty.style.display = "block";
      empty.innerHTML =
        `<i class="bi bi-search me-1"></i>La oportunidad <code>${esc(pedido)}</code> no está en la ` +
        `corrida activa (puede haber dejado de calificar en el último recálculo). ` +
        `<a href="/mercado-privado/oportunidades">Ver todas las oportunidades</a>.`;
    }
  }

  function init() {
    ["fTipo", "fActividad", "fFamilia", "fUnidad", "fSort"].forEach((id) => $(id).addEventListener("change", applyFilters));
    $("fSearch").addEventListener("input", applyFilters);
    $("fReset").addEventListener("click", () => {
      ["fTipo", "fActividad", "fFamilia", "fUnidad"].forEach((id) => ($(id).value = ""));
      $("fSearch").value = ""; $("fSort").value = "score"; applyFilters();
    });
    $("oppReloadBtn").addEventListener("click", load);
    load();
  }

  if (document.readyState === "loading") document.addEventListener("DOMContentLoaded", init);
  else init();
})();
