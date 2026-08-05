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
  let ALL = [];
  let WINDOW = {};
  let CRM_MODO = null;   // 'simulado' | 'test' | 'prod' — entorno al que se enviaría ahora
  let CRM_ASIGNACION = { match: null, usuarios: [], sugerido_id: null, error: null };

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
  // Render de los campos del payload (reutilizable: preview y payload sellado post-envío).
  function renderCrmFields(payload, pendientesArr, faltantesArr) {
    const pendientes = new Set(pendientesArr || []);
    const faltantes = new Set(faltantesArr || []);
    const fieldsHtml = Object.keys(payload).map((k) => {
      let val = payload[k], badge = "";
      if (pendientes.has(k)) badge += ` <span class="crm-pendiente">PENDIENTE CRM</span>`;
      // El badge "FALTA EN DATASET" solo para lo que efectivamente falta en el dataset.
      // Un campo vacío por otro motivo (p.ej. assigned_user que no se pudo resolver
      // contra el CRM) se muestra "—" a secas: el banner de bloqueo explica el porqué,
      // y etiquetarlo como problema del dataset mandaría a buscar donde no es.
      if (faltantes.has(k)) badge += ` <span class="crm-faltante">FALTA EN DATASET</span>`;
      if (val === null || val === "") val = "—";
      if (typeof val === "number") val = k === "amount" ? `${fmtMoney(val)} (${val})` : val;
      return `<div class="crm-field"><div class="crm-key">${esc(k)}${badge}</div>` +
        `<div class="crm-val">${esc(val)}</div></div>`;
    }).join("");
    $("crmFields").innerHTML = fieldsHtml;
    $("crmJson").textContent = JSON.stringify(payload, null, 2);
    $("crmCopyBtn").onclick = () => navigator.clipboard && navigator.clipboard.writeText(JSON.stringify(payload, null, 2));
  }

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

  // Selector de asignación. Solo se muestra cuando NO hubo match automático: en ese caso
  // el sistema no elige por el usuario, se lo pregunta. Devuelve true si quedó visible.
  function reflectAsignacion(o) {
    const box = $("crmAsignacionBox"), sel = $("crmAsignadoSel");
    const crmInfo = o.crm || {};
    // El Analista nunca elige: se asigna a sí mismo o no envía. El selector no se
    // dibuja para él (y el backend igual rechaza cualquier otro assigned_user_id).
    const requiere = !!crmInfo.requiere_asignacion && crmInfo.puede_elegir !== false;
    if (!box || !sel) return false;
    if (!requiere) { box.style.display = "none"; return false; }

    const usuarios = CRM_ASIGNACION.usuarios || [];
    // Opción vacía primero: obliga a un acto deliberado. Sin esto, el primero de la
    // lista quedaría preseleccionado y un envío distraído se lo asignaría a esa persona.
    sel.innerHTML = `<option value="">— Elegí un usuario —</option>` +
      usuarios.map((u) => `<option value="${esc(u.id)}">${esc(u.usuario)}</option>`).join("");
    sel.value = "";
    box.style.display = "block";

    const btn = $("crmSendBtn");
    const sync = () => {
      const ok = !!sel.value;
      btn.disabled = !ok;
      btn.innerHTML = ok
        ? `<i class="bi bi-send-check me-1"></i>Confirmar envío`
        : `<i class="bi bi-person-exclamation me-1"></i>Elegí un usuario`;
      // El payload mostrado refleja la elección, para que lo que se ve sea lo que se manda.
      const elegido = usuarios.find((u) => u.id === sel.value);
      const crm = o.crm || {}, payload = Object.assign({}, crm.payload || {});
      payload.assigned_user = elegido ? `asignado a ${elegido.usuario} (selección manual)` : null;
      renderCrmFields(payload, crm.pendientes_crm, crm.faltantes_dataset);
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

  // POST /enviar/{id}: sella el envío server-side y maneja duplicado/éxito/error.
  async function sendCrm(o) {
    const btn = $("crmSendBtn");
    btn.disabled = true;
    btn.innerHTML = `<span class="spinner-border spinner-border-sm me-1"></span>Enviando…`;
    try {
      // Si hubo selección manual, viaja el id elegido. El backend igual lo valida
      // contra la lista real del CRM: acá solo se transmite la decisión.
      const sel = $("crmAsignadoSel");
      const puedeElegir = o.crm && o.crm.requiere_asignacion && o.crm.puede_elegir !== false;
      const elegido = (puedeElegir && sel) ? sel.value : "";
      const url = SEND_API(o.id) + (elegido ? `?assigned_user_id=${encodeURIComponent(elegido)}` : "");
      const resp = await fetch(url, { method: "POST", headers: { Accept: "application/json" } });
      const json = await resp.json().catch(() => ({}));
      if (resp.status === 403) {
        setCrmStatus("danger", `<i class="bi bi-shield-lock me-1"></i>${esc(json.detail || "No autorizado para reenviar.")}`);
        btn.disabled = false; btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
        return;
      }
      if (!resp.ok) throw new Error(json.detail || `HTTP ${resp.status}`);
      // Bloqueo de duplicado: el backend devuelve ok:false + quién/cuándo.
      if (json.ok === false && json.status === "duplicado") {
        o.envio = {
          enviado: true, enviado_por: json.enviado_por, enviado_at: json.enviado_at,
          crm_id: json.crm_id, crm_url: json.crm_url,
        };
        reflectSent(o);
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
      applyFilters(); // refresca el badge "Enviada" en la lista
    } catch (e) {
      setCrmStatus("danger", `<i class="bi bi-x-circle me-1"></i>No se pudo enviar. ${esc(e.message)}`);
      btn.disabled = false; btn.innerHTML = `<i class="bi bi-send-check me-1"></i>Confirmar envío`;
    }
  }

  function showCrm(o) {
    const crm = o.crm || {}, payload = crm.payload || {};
    // Entorno destino en el encabezado: con bloqueo por entorno, confirmar sin saber
    // contra cuál se está operando es justo lo que no queremos en la demo.
    const modoEl = $("crmModoLabel");
    if (modoEl) modoEl.textContent = CRM_MODO ? (MODO_TXT[CRM_MODO] || CRM_MODO) : "";
    renderCrmFields(payload, crm.pendientes_crm, crm.faltantes_dataset);
    // Orden: estado normal -> selector (si falta elegir) -> bloqueo (pisa a todo).
    reflectSent(o);
    if (!(o.envio && o.envio.enviado)) reflectAsignacion(o);
    else $("crmAsignacionBox").style.display = "none";
    reflectBloqueos(o);
    $("crmSendBtn").onclick = () => sendCrm(o);
    if (!crmModal) crmModal = new bootstrap.Modal($("crmModal"));
    crmModal.show();
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
      if (!resp.ok) throw new Error(`HTTP ${resp.status}`);
      const json = await resp.json();
      const data = (json && json.data) || {};
      ALL = data.rows || [];
      WINDOW = data.window || {};
      CRM_MODO = data.crm_modo || null;
      CRM_ASIGNACION = data.crm_asignacion || { match: null, usuarios: [], sugerido_id: null, error: null };
      $("oppWindowLabel").textContent = WINDOW.label ? `Demanda analizada: ${WINDOW.label}` : "Período no disponible";
      fillSelect("fFamilia", [...new Set(ALL.map((o) => o.familia).filter(Boolean))].sort());
      fillSelect("fUnidad", [...new Set(ALL.map((o) => o.unidad_negocio).filter(Boolean))].sort());
      applyFilters();
      abrirDesdeDeepLink();
    } catch (e) {
      $("oppWindowLabel").textContent = "Error al cargar";
      $("oppBody").innerHTML =
        `<div class="opp-empty">No se pudieron cargar las oportunidades (${esc(e.message)}).</div>`;
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
