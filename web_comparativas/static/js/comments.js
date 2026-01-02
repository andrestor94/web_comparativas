/* web_comparativas/static/js/comments.js */
(function () {
  // =============== Helpers ===============
  const $ = (sel, root = document) => root.querySelector(sel);

  // === Contexto recomendado: <div id="comments-context" data-upload-id=".." data-process-code="..">
  function getCommentsContextFallback() {
    const ctx = document.querySelector("#comments-context");
    let uploadId = null, processCode = null;

    if (ctx) {
      uploadId = (ctx.getAttribute("data-upload-id") || "").trim() || null;
      processCode = (ctx.getAttribute("data-process-code") || "").trim() || null;
    }

    // Fallback adicional
    if (!processCode) {
      const header = document.querySelector('#processHeader, .kpi-proceso, .proceso-title, .wc-proceso, .card h5');
      const txt = header && header.textContent ? header.textContent.trim() : "";
      const m = txt.match(/proceso\s*[:\-]?\s*([A-Z0-9.\-_/]+)/i);
      processCode = m ? m[1] : null;
    }
    return { uploadId, processCode };
  }

  // =============== Bootstrap ===============
  const panel = document.getElementById("wc-comments-panel");
  if (!panel) return;

  let uploadId = String(panel.dataset.uploadId || "");
  let processCode = String(panel.dataset.processCode || "");

  // Fallbacks
  {
    const fb = getCommentsContextFallback();
    if (!uploadId && fb.uploadId) uploadId = fb.uploadId;
    if (!processCode && fb.processCode) processCode = fb.processCode;
  }

  const listEl = $("#wc-comments-list", panel);
  const formEl = $("#wc-comments-form", panel);
  const taEl = formEl?.querySelector("textarea[name=body]") || formEl?.querySelector("textarea");

  const btnToggle = document.getElementById("wc-comments-toggle");
  const btnClose = document.getElementById("wc-comments-close");

  // Customize UI for Help Desk Mode
  if (listEl) {
    listEl.innerHTML = `<div class="p-3 text-center text-muted small">
        <p>Utilice este formulario para enviar una consulta a la Mesa de Ayuda sobre este proceso.</p>
        <p>Se creará una nueva Consulta automáticamente.</p>
      </div>`;
  }
  const btnSubmit = formEl?.querySelector("button[type=submit]");
  if (btnSubmit) btnSubmit.textContent = "Enviar Consulta";

  // =============== Open/Close ===============
  function openPanel() {
    panel.setAttribute("aria-hidden", "false");
    btnToggle?.setAttribute("aria-expanded", "true");
    panel.classList.add("wc-open");
    setTimeout(() => taEl?.focus(), 60);
  }
  function closePanel() {
    panel.setAttribute("aria-hidden", "true");
    btnToggle?.setAttribute("aria-expanded", "false");
    panel.classList.remove("wc-open");
  }

  btnToggle?.addEventListener("click", (e) => {
    e.preventDefault();
    panel.classList.contains("wc-open") ? closePanel() : openPanel();
  });
  btnClose?.addEventListener("click", (e) => { e.preventDefault(); closePanel(); });
  window.addEventListener("keydown", (e) => { if (e.key === "Escape" && panel.classList.contains("wc-open")) closePanel(); });

  // =============== Submit -> Ticket ===============
  formEl?.addEventListener("submit", async (e) => {
    e.preventDefault();
    const body = (taEl?.value || "").trim();
    if (!body) return;

    // Disable button
    if (btnSubmit) { btnSubmit.disabled = true; btnSubmit.textContent = "Creando..."; }

    try {
      const payload = {
        title: "Consulta desde Dashboard",
        message: body,
        category: "consulta",
        priority: "media",
        upload_id: uploadId || null,
        process_code: processCode || null
      };

      const r = await fetch("/sic/api/tickets/create", {
        method: "POST",
        headers: { "Content-Type": "application/json" },
        body: JSON.stringify(payload)
      });

      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const res = await r.json();

      if (res.ok) {
        if (taEl) taEl.value = "";
        // Show success and link
        listEl.innerHTML = `
            <div class="alert alert-success m-3">
                <strong>¡Consulta Enviada!</strong><br>
                Su consulta ha sido enviada a la Mesa de Ayuda.<br>
                <a href="${res.redirect_url}" class="btn btn-sm btn-outline-success mt-2" target="_blank">Ver Consulta #${res.ticket_id}</a>
            </div>
          `;
      } else {
        throw new Error(res.error || "Error desconocido");
      }

    } catch (err) {
      alert("No se pudo enviar la consulta: " + err.message);
      console.error(err);
    } finally {
      if (btnSubmit) { btnSubmit.disabled = false; btnSubmit.textContent = "Enviar Consulta"; }
    }
  });

  // Enter = enviar
  taEl?.addEventListener("keydown", (e) => {
    if (e.key === "Enter" && !e.shiftKey) {
      e.preventDefault();
      formEl?.requestSubmit?.() || formEl?.submit?.();
    }
  });

  console.info("[wc-comments] Help Desk Mode activated.");
})();
