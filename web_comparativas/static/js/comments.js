/* web_comparativas/static/js/comments.js */
(function () {
  // =============== Helpers ===============
  const $  = (sel, root=document) => root.querySelector(sel);
  const $$ = (sel, root=document) => Array.from(root.querySelectorAll(sel));
  const esc = (s)=> String(s == null ? "" : s)
    .replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");
  const safeJSON = (s)=> { try { return typeof s === "string" ? JSON.parse(s) : s; } catch { return null; } };
  const fmtDate = (iso)=>{
    if (!iso) return "";
    try { return new Date(iso).toLocaleString("es-AR",{dateStyle:"short",timeStyle:"short"}); }
    catch { return iso; }
  };
  const toISO = (v)=>{ try{ const d=new Date(v); return isNaN(d)? "": d.toISOString(); }catch{ return ""; } };

  // === Contexto recomendado: <div id="comments-context" data-upload-id=".." data-process-code="..">
  function getCommentsContextFallback() {
    const ctx = document.querySelector("#comments-context");
    let uploadId = null, processCode = null;

    if (ctx) {
      uploadId = (ctx.getAttribute("data-upload-id") || "").trim() || null;
      processCode = (ctx.getAttribute("data-process-code") || "").trim() || null;
    }

    // Fallback adicional: intentar leer del encabezado visible "PROCESO 434-1356-LPU25"
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
  if (!panel) return; // No hay panel en esta página

  // Tomamos de data-* y luego completamos con fallback SIN re-declarar
  let uploadId    = String(panel.dataset.uploadId || "");
  const userDisplay = String(panel.dataset.userDisplay || "Yo");
  const userRole    = String(panel.dataset.userRole || "").toLowerCase();
  let processCode   = String(panel.dataset.processCode || "");

  // Fallbacks si faltara alguno
  {
    const fb = getCommentsContextFallback();
    if (!uploadId && fb.uploadId) uploadId = fb.uploadId;
    if (!processCode && fb.processCode) processCode = fb.processCode;
  }

  const listEl        = $("#wc-comments-list", panel);
  const formEl        = $("#wc-comments-form", panel);
  const taEl          = formEl?.querySelector("textarea[name=body]") || formEl?.querySelector("textarea");
  const hiddenParent  = formEl?.querySelector("input[name=parent_id]");
  const replyingBadge = $("#wc-replying", formEl);

  const btnToggle  = document.getElementById("wc-comments-toggle");
  const btnClose   = document.getElementById("wc-comments-close");
  const badgeCount = document.getElementById("wc-comments-count"); // opcional

  let comments = [];
  let eventSource = null;
  let openedOnce = false;

  // =============== API endpoints ===============
  const API = {
    list:    (u) => `/api/comments?upload_id=${encodeURIComponent(u)}`,
    create:  `/api/comments`,
    stream:  (u) => `/api/comments/stream?upload_id=${encodeURIComponent(u)}`,
    resolve: (id)=> `/api/comments/${id}/resolve`,  // POST {resolved:bool}
    patch:   (id)=> `/api/comments/${id}`,          // PATCH {body?, is_resolved?}
  };

  // =============== UI: abrir/cerrar ===============
  function openPanel() {
    panel.setAttribute("aria-hidden","false");
    btnToggle?.setAttribute("aria-expanded","true");
    panel.classList.add("wc-open");
    if (!openedOnce) {
      openedOnce = true;
      loadComments(true);
      connectStream();
    }
    // Foco al textarea si existe
    setTimeout(()=> taEl?.focus(), 60);
  }
  function closePanel() {
    panel.setAttribute("aria-hidden","true");
    btnToggle?.setAttribute("aria-expanded","false");
    panel.classList.remove("wc-open");
  }
  btnToggle?.addEventListener("click",(e)=>{
    e.preventDefault();
    panel.classList.contains("wc-open") ? closePanel() : openPanel();
  });
  btnClose?.addEventListener("click",(e)=>{ e.preventDefault(); closePanel(); });
  // ESC para cerrar
  window.addEventListener("keydown",(e)=>{ if(e.key==="Escape" && panel.classList.contains("wc-open")) closePanel(); });

  // =============== Fetch ===============
  async function loadComments(updateCountOnly=false){
    try{
      const r = await fetch(API.list(uploadId), { headers:{ accept:"application/json" }});
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      const data = await r.json();
      if (!Array.isArray(data)) throw new Error("Formato inesperado");
      comments = data;
      updateCount();
      if (!updateCountOnly) renderList();
    }catch(err){
      console.error("[comments] list error:", err);
      if (!updateCountOnly) listEl.innerHTML = `<div class="wc-empty" style="color:#b91c1c">No se pudo cargar el hilo.</div>`;
    }
  }

  async function createComment(body, parent_id){
    const payload = { upload_id: uploadId, body: (body||"").trim() };
    if (parent_id) payload.parent_id = Number(parent_id);
    if (processCode) payload.process_code = processCode;       // se guarda con el comentario

    const headers = { "Content-Type":"application/json", accept:"application/json" };
    if (processCode) headers["X-Process-Code"] = processCode;  // por si el backend lo usa como header

    const r = await fetch(API.create, { method:"POST", headers, body: JSON.stringify(payload) });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
    const item = await r.json();
    upsert(item);
    return item;
  }

  async function setResolved(id, value){
    const r = await fetch(API.resolve(id), {
      method:"POST",
      headers:{ "Content-Type":"application/json", accept:"application/json" },
      body: JSON.stringify({ resolved: !!value })
    });
    if (!r.ok) throw new Error(`HTTP ${r.status}`);
  }

  // =============== Árbol + visibilidad ===============
  function buildTree(items){
    const byId = new Map();
    const roots = [];
    items.forEach(c => byId.set(c.id, { ...c, children: [] }));
    byId.forEach(node => {
      if (node.deleted_at) return;
      if (node.parent_id && byId.has(node.parent_id)) byId.get(node.parent_id).children.push(node);
      else roots.push(node);
    });
    const sortNodes = (arr)=>{
      arr.sort((a,b)=>{
        const A = toISO(a.created_at), B = toISO(b.created_at);
        if (A===B) return (a.id||0)-(b.id||0);
        return A.localeCompare(B);
      });
      arr.forEach(n=> sortNodes(n.children));
    };
    sortNodes(roots);
    return roots;
  }

  function hasSelf(node){
    if ((node.author||"") === userDisplay) return true;
    return (node.children||[]).some(hasSelf);
  }

  // Admin/Auditor ven todo; otros solo hilos donde participan
  function filterRootsForViewer(roots){
    if (userRole === "admin" || userRole === "auditor") return roots;
    return roots.filter(r => r.author === userDisplay || hasSelf(r));
  }

  function flattenTree(arr, out=[]){
    for (const n of arr){ out.push(n); if (n.children?.length) flattenTree(n.children, out); }
    return out;
  }

  // =============== Render ===============
  function updateCount(){
    if (!badgeCount) return;
    const visible = comments.filter(c=> !c.deleted_at);
    const roots = buildTree(visible);
    const vRoots = filterRootsForViewer(roots);
    const vItems = flattenTree(vRoots, []);
    badgeCount.textContent = String(vItems.length);
  }

  function nodeHTML(n, depth){
    const isResolved = !!n.resolved_at;
    const resolvedCls = isResolved ? "wc-item-resolved" : "";
    const parentTag = n.parent_id ? `<span class="wc-parent">↪ #${n.parent_id}</span>` : "";
    const pad = Math.min(24 + depth*16, 120);

    const adminActions = (userRole === "admin" || userRole === "auditor")
      ? `
        <button class="wc-link wc-act" data-act="ack" data-id="${n.id}">Acusar recibo</button>
        <button class="wc-link wc-act" data-act="ack_resolve" data-id="${n.id}">${isResolved ? "Reabrir" : "Acusar + resolver"}</button>
      `
      : "";

    return `
      <article class="wc-item ${resolvedCls}" data-id="${n.id}" style="padding-left:${pad}px">
        <header class="wc-item-hd">
          <div class="wc-item-author">${esc(n.author || userDisplay)}</div>
          <div class="wc-item-meta">
            ${parentTag}
            <time datetime="${esc(n.created_at||"")}">${esc(fmtDate(n.created_at))}</time>
            ${isResolved ? `<span class="wc-chip">Resuelto</span>` : ``}
          </div>
        </header>
        <div class="wc-item-body">${esc(n.body)}</div>
        <footer class="wc-item-ft">
          ${adminActions}
          <button class="wc-link wc-act" data-act="reply" data-id="${n.id}">Responder</button>
        </footer>
      </article>
      ${n.children.map(ch => nodeHTML(ch, depth+1)).join("")}
    `;
  }

  function renderList(){
    const visible = comments.filter(c=> !c.deleted_at);
    if (!visible.length){
      listEl.innerHTML = `<div class="wc-empty">No hay comentarios aún.</div>`;
      return;
    }
    const tree = buildTree(visible);
    const vRoots = filterRootsForViewer(tree);
    if (!vRoots.length){
      listEl.innerHTML = `<div class="wc-empty">No hay comentarios para mostrar.</div>`;
      return;
    }
    listEl.innerHTML = vRoots.map(n => nodeHTML(n,0)).join("");
    listEl.scrollTop = listEl.scrollHeight;
  }

  // =============== Acciones ===============
  listEl.addEventListener("click", async (e)=>{
    const btn = e.target.closest(".wc-act");
    if (!btn) return;

    const idRaw = btn.dataset.id ?? btn.closest("[data-id]")?.dataset.id;
    const id = Number(idRaw);
    if (!Number.isFinite(id)) return;

    const act = btn.dataset.act;
    try{
      if (act === "reply"){
        hiddenParent && (hiddenParent.value = String(id));
        replyingBadge?.classList.remove("wc-hide");
        taEl?.focus();
        return;
      }
      if (act === "ack"){
        await createComment("Recibido.", id);
        return;
      }
      if (act === "ack_resolve"){
        if (document.activeElement) document.activeElement.blur();
        await createComment(isFinite(id) ? "Recibido." : "Recibido.", id);
        await setResolved(id, true);
        return;
      }
    }catch(err){
      alert("Operación no permitida o fallida.");
      console.error(err);
    }
  });

  $("#wc-reply-cancel", formEl)?.addEventListener("click",(e)=>{
    e.preventDefault();
    if (hiddenParent) hiddenParent.value = "";
    replyingBadge?.classList.add("wc-hide");
  });

  // Enter = enviar; Shift+Enter = salto de línea
  taEl?.addEventListener("keydown",(e)=>{
    if (e.key === "Enter" && !e.shiftKey){
      e.preventDefault();
      formEl?.requestSubmit?.() || formEl?.submit?.();
    }
  });

  formEl?.addEventListener("submit", async (e)=>{
    e.preventDefault();
    const body = (taEl?.value || "").trim();
    if (!body) return;
    const parent_id = hiddenParent?.value ? Number(hiddenParent.value) : undefined;
    try{
      await createComment(body, parent_id);
      if (taEl) taEl.value = "";
      if (hiddenParent) hiddenParent.value = "";
      replyingBadge?.classList.add("wc-hide");
      // Mantener foco para seguir escribiendo
      taEl?.focus();
    }catch(err){
      alert("No se pudo enviar el comentario.");
      console.error(err);
    }
  });

  // =============== SSE (stream) ===============
  function connectStream(){
    if (!("EventSource" in window)) {
      console.warn("[comments] EventSource no soportado");
      return;
    }
    try{ eventSource?.close?.(); }catch{}
    const es = new EventSource(API.stream(uploadId));

    es.onmessage = (ev)=>{
      const payload = safeJSON(ev.data) || {};
      const { type, item, id } = payload;
      if (type === "created" && item) upsert(item);
      if (type === "updated" && item) upsert(item);
      if (type === "deleted" && (id != null)) markDeleted(Number(id));
    };
    es.onerror = ()=>{ try{ es.close(); }catch{}; setTimeout(connectStream, 3000); };
    eventSource = es;
  }

  // =============== Mutadores ===============
  function upsert(c){
    const i = comments.findIndex(x=> x.id === c.id);
    if (i >= 0) comments[i] = c; else comments.push(c);
    updateCount();
    renderList();
  }
  function markDeleted(id){
    const i = comments.findIndex(x=> x.id === id);
    if (i >= 0){
      comments[i].deleted_at = new Date().toISOString();
      updateCount(); renderList();
    } else {
      loadComments();
    }
  }

  // Precarga contador (si existiera) y datos
  loadComments(true);

  // Mensaje de arranque útil para debug
  console.info("[wc-comments] listo.", { uploadId, processCode, userRole, userDisplay });
})();
