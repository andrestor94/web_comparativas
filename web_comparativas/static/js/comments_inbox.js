/* static/js/comments_inbox.js */
(function () {
  // ================= Helpers =================
  const $  = (s, r=document) => r.querySelector(s);
  const $$ = (s, r=document) => Array.from(r.querySelectorAll(s));
  const esc = (s)=> String(s ?? '')
    .replace(/&/g,'&amp;').replace(/</g,'&lt;').replace(/>/g,'&gt;');

  const toISO = (x)=> {
    try { const d = new Date(x); return isNaN(d.getTime()) ? '' : d.toISOString(); }
    catch { return ''; }
  };
  const fmt = (iso)=>{
    try{ const d = new Date(iso); return isNaN(d) ? '' :
      d.toLocaleString('es-AR',{dateStyle:'short',timeStyle:'short'}); }
    catch{ return iso||''; }
  };
  const trunc = (s, n=140)=> String(s||'').length>n ? String(s||'').slice(0,n-1)+'…' : String(s||'');

  async function getJSON(url){
    const r = await fetch(url, {headers:{accept:'application/json'}});
    if (!r.ok) throw new Error(`HTTP ${r.status} ${url}`);
    return r.json();
  }

  // ================= User / Role =================
  const ROLE = (document.body?.dataset?.userRole || $('.meta .badge')?.textContent || '').toLowerCase().trim();
  const USER = (document.body?.dataset?.userDisplay || $('.meta .me-2')?.textContent || 'Yo').trim();
  const isPrivileged = (ROLE==='admin' || ROLE==='auditor');

  // ================= UI refs =================
  const fStatus  = $('#fStatus');
  const fUpload  = $('#fUpload');
  const fAuthor  = $('#fAuthor');
  const fText    = $('#fText');
  const fFrom    = $('#fFrom');
  const fTo      = $('#fTo');
  const fProc    = $('#fProc') || $('#fProcCode'); // soporte para ambos ids si existiera
  const btnSearch= $('#btnSearch');

  const tbody    = $('#cmTBody'), counters=$('#cmCounters');
  const pageInfo = $('#cmPageInfo'), prev=$('#cmPrev'), next=$('#cmNext'), pageSizeSel=$('#cmPageSize');

  // Offcanvas (chat)
  const threadEl = $('#cmThreadCanvas');
  const threadList = $('#cmThreadList');
  const replyForm = $('#cmReplyForm');
  const replyBody = $('#cmReplyBody');
  const offcanvas = threadEl ? new bootstrap.Offcanvas(threadEl) : null;

  // Estado
  let page=1, pageSize=Number(pageSizeSel?.value||50), sort='-created_at', total=0;

  // Cache de códigos de proceso por upload_id
  const procCache = new Map(); // id -> "434-1356-LPU25"

  // ================= Counters =================
  async function loadCounters(){
    try{
      const d = await getJSON(`/api/comments/admin?page_size=1`);
      if (d && d.counters){
        counters.textContent = `Abiertos: ${d.counters.open} · Resueltos: ${d.counters.resolved} · Borrados: ${d.counters.deleted}`;
      }
    }catch{/* ignore */}
  }

  // ================= Query params =================
  function params(){
    const p = new URLSearchParams();
    p.set('status', fStatus?.value || 'open');
    if (fUpload?.value?.trim()) p.set('upload_id', fUpload.value.trim());
    if (fAuthor?.value?.trim()) p.set('author', fAuthor.value.trim());
    if (fText?.value?.trim())   p.set('q', fText.value.trim());
    if (fFrom?.value) p.set('dt_from', fFrom.value);
    if (fTo?.value)   p.set('dt_to', fTo.value);
    if (fProc?.value?.trim())   p.set('process_code', fProc.value.trim()); // NUEVO filtro si backend lo soporta
    p.set('page', String(page));
    p.set('page_size', String(pageSize));
    p.set('sort', sort);
    return p;
  }

  // ============ Adaptadores de datos ============
  function adaptInboxItems(items){
    return (items||[]).map(it => ({
      key: `${it.upload_id}@@${String(it.author||'—')}`,
      upload_id: it.upload_id,
      author: String(it.author || '—'),
      last_ts: it.last_at,
      last_body: it.last_body || '',
      any_open: String(it.status || 'open') !== 'resolved',
      preferred_root_id: (it.root_id != null) ? Number(it.root_id) : null,
      process_code: it.process_code || it.code || it.codigo || null, // si el backend lo envía
    }));
  }

  function groupThreadsFromFlat(items){
    const map = new Map();
    for(const c of (items||[])){
      if (c.deleted_at) continue;
      const author = String(c.author || '—');
      const key = `${c.upload_id}@@${author}`;
      if(!map.has(key)){
        map.set(key, {
          key, upload_id:c.upload_id, author,
          roots: [],
          last_ts: c.created_at,
          last_body: c.body || '',
          any_open: !c.resolved_at,
          preferred_root_id: null,
          process_code: c.process_code || null,
        });
      }
      const g = map.get(key);
      if (toISO(c.created_at) > toISO(g.last_ts)) {
        g.last_ts   = c.created_at;
        g.last_body = c.body || '';
      }
      if (!c.resolved_at) g.any_open = true;
      if (!c.parent_id && String(c.author||'')===author){
        g.roots.push({ id:Number(c.id), created_at:c.created_at, resolved_at:c.resolved_at });
      }
      if (!g.process_code && c.process_code) g.process_code = c.process_code;
    }
    map.forEach(g=>{
      const sorted = g.roots.slice().sort((a,b)=> toISO(b.created_at).localeCompare(toISO(a.created_at)));
      const open = sorted.find(r=>!r.resolved_at);
      g.preferred_root_id = (open?.id) ?? (sorted[0]?.id ?? null);
    });
    let arr = Array.from(map.values());
    if (!isPrivileged) {
      const uname = (USER || '').toLowerCase();
      arr = arr.filter(g => g.author.toLowerCase() === uname);
    }
    arr.sort((a,b)=> toISO(b.last_ts).localeCompare(toISO(a.last_ts)));
    return arr;
  }

  // ================= Fetch + Render list =================
  async function loadList(){
    tbody.innerHTML = `<tr><td colspan="6" class="text-center text-muted py-4">Cargando…</td></tr>`;
    await loadCounters();
    try{
      const d = await getJSON(`/api/comments/inbox?${params().toString()}`);
      const items = d.items || [];

      const groups = (items.length && (("last_at" in items[0]) || ("status" in items[0])))
        ? adaptInboxItems(items)
        : groupThreadsFromFlat(items);

      total = groups.length;
      renderRows(groups);

      const maxPage = Math.max(1, Math.ceil(total/(pageSize||50)));
      page = Math.min(page, maxPage);
      pageInfo.textContent = `Página ${page} / ${maxPage} (${total} hilos)`;
      prev.disabled = page <= 1;
      next.disabled = page >= maxPage;

      // Hidratar códigos de proceso para las filas visibles
      const start = (page-1)*(pageSize||50);
      const slice = groups.slice(start, start+(pageSize||50));
      await hydrateProcessCodes(slice);
    }catch(err){
      console.error(err);
      tbody.innerHTML = `<tr><td colspan="6" class="text-center text-danger py-4">Error cargando</td></tr>`;
    }
  }

  function rowHTML(g){
    const badge = g.any_open
      ? `<span class="badge-sa-abierto">Abierto</span>`
      : `<span class="badge-sa-resuelto">Resuelto</span>`;

    const resolveBtn = isPrivileged && g.preferred_root_id
      ? `<button class="cm-resolve btn btn-sm ${g.any_open ? 'btn-success' : 'btn-outline-sa'}"
                  data-root="${esc(g.preferred_root_id)}"
                  data-upload="${esc(g.upload_id)}"
                  data-action="${g.any_open ? 'resolve' : 'reopen'}">
            ${g.any_open ? 'Resuelto' : 'Reabrir'}
         </button>` : '';

    const actions = `
      <div class="wc-action-group cm-actions">
        <a class="btn btn-link" href="/tablero/${esc(g.upload_id)}">Abrir tablero</a>
        <button class="cm-open btn btn-outline-primary btn-sm"
                data-upload="${esc(g.upload_id)}"
                data-author="${esc(g.author)}"
                ${g.preferred_root_id ? `data-root="${g.preferred_root_id}"` : ''}>
          Responder
        </button>
        ${resolveBtn}
      </div>`;

    const dateTxt = fmt(g.last_ts) || '';
    const cachedCode = g.process_code || procCache.get(g.upload_id) || '';

    // Celda con placeholder para hidratar luego
    const procCell = `
      <div class="cm-proc" data-upload="${esc(g.upload_id)}">
        <a class="link-primary" href="/tablero/${esc(g.upload_id)}">#${esc(g.upload_id)}</a>
        <span class="cm-proc-code text-muted ms-1">${cachedCode ? '· '+esc(cachedCode) : ''}</span>
      </div>`;

    return `<tr data-upload="${esc(g.upload_id)}" data-author="${esc(g.author)}">
      <td class="text-nowrap" style="width:140px">${esc(dateTxt || '')}</td>
      <td style="width:220px">${procCell}</td>
      <td style="width:160px">${esc(g.author)}</td>
      <td><span class="text-muted">Último:</span> ${esc(trunc(g.last_body))}</td>
      <td class="text-center" style="width:110px">${badge}</td>
      <td style="width:360px">${actions}</td>
    </tr>`;
  }

  function renderRows(groups){
    if (!groups.length){
      tbody.innerHTML = `<tr><td colspan="6" class="text-center text-muted py-4">Sin resultados</td></tr>`;
      return;
    }
    const start = (page-1)*(pageSize||50);
    const slice = groups.slice(start, start+(pageSize||50));
    tbody.innerHTML = slice.map(rowHTML).join('');
  }

  // ======== Hidratar códigos de proceso (#id → código) ========
  async function resolveProcessCode(uploadId){
    // 1) cache
    if (procCache.has(uploadId)) return procCache.get(uploadId);

    // 2) intentar /api/uploads/{id}
    try{
      const u = await getJSON(`/api/uploads/${encodeURIComponent(uploadId)}`);
      const code = u?.process_code || u?.process?.code || u?.codigo || u?.proceso || null;
      if (code){ procCache.set(uploadId, code); return code; }
    }catch{/* ignora y sigue */}

    // 3) intentar leer del último comentario (si guarda process_code)
    try{
      const list = await getJSON(`/api/comments?upload_id=${encodeURIComponent(uploadId)}&page_size=1&sort=-created_at`);
      const c = Array.isArray(list) ? list[0] : null;
      const code = c?.process_code || c?.meta?.process_code || null;
      if (code){ procCache.set(uploadId, code); return code; }
    }catch{/* ignora */}

    // 4) sin datos
    procCache.set(uploadId, '');
    return '';
  }

  async function hydrateProcessCodes(groups){
    // Solo para lo visible, con pequeña concurrencia
    const visibles = groups.map(g => g.upload_id);
    const tasks = visibles.map(id => resolveProcessCode(id).then(code => ({id, code})));

    // limitar concurrencia (4)
    const pool = [];
    const out = [];
    for (const t of tasks){
      const p = t.then(v => {
        // actualizar DOM si encontramos código
        if (v.code){
          const cell = $(`.cm-proc[data-upload="${CSS.escape(String(v.id))}"]`);
          const span = cell?.querySelector('.cm-proc-code');
          if (span) span.textContent = '· '+v.code;
        }
        return v;
      });
      pool.push(p);
      if (pool.length >= 4){
        out.push(await Promise.race(pool));
        // limpiar resueltos
        for (let i=pool.length-1;i>=0;i--) if (pool[i].fulfilled) pool.splice(i,1);
      }
    }
    await Promise.all(pool);
    return out;
  }

  // ================= Abrir hilo (por grupo) =================
  function renderThreadItem(c, depth){
    const meta = `${esc(c.author||'—')} · ${fmt(c.created_at)}`;
    const cls  = depth>0 ? 'ms-3 border-start ps-3' : '';
    return `<div class="mb-2 ${cls}">
      <div class="cm-meta">${meta}</div>
      <div>${esc(c.body||'')}</div>
    </div>`;
  }

  function expandDescendants(all, rootIds){
    const allowed = new Set(rootIds);
    let changed = true;
    while(changed){
      changed = false;
      for(const c of all){
        if (c.parent_id && allowed.has(Number(c.parent_id)) && !allowed.has(Number(c.id))){
          allowed.add(Number(c.id));
          changed = true;
        }
      }
    }
    return allowed;
  }

  async function openThreadByGroup(uploadId, author, preferredRootId){
    try{
      const all = await getJSON(`/api/comments?upload_id=${encodeURIComponent(uploadId)}`);
      const roots = (all||[]).filter(x => !x.parent_id && String(x.author||'')===String(author));
      const rootIds = roots.length ? roots.map(x=>Number(x.id)) : (all||[]).filter(x=>!x.parent_id).map(x=>Number(x.id));
      const allowed = expandDescendants(all||[], rootIds);
      const subset = (all||[]).filter(x => allowed.has(Number(x.id)))
                              .sort((a,b)=> {
                                const A = toISO(a.created_at), B = toISO(b.created_at);
                                if (A===B) return (a.id||0) - (b.id||0);
                                return A.localeCompare(B);
                              });

      threadList.innerHTML = subset.map(c=>{
        const depth = c.parent_id ? 1 : 0;
        return renderThreadItem(c, depth);
      }).join('') || `<div class="text-muted">Sin mensajes aún.</div>`;

      let rootId = preferredRootId || null;
      if (!rootId) {
        const sorted = roots.slice().sort((a,b)=> toISO(b.created_at).localeCompare(toISO(a.created_at)));
        rootId = (sorted.find(x=>!x.resolved_at)?.id) ?? (sorted[0]?.id) ?? null;
      }

      replyForm.dataset.uploadId = String(uploadId);
      replyForm.dataset.parentId = rootId ? String(rootId) : '';
      replyBody.value = '';
      offcanvas?.show();
      replyBody.focus();
    }catch(err){
      console.error(err);
      alert('No se pudo cargar el hilo.');
    }
  }

  // ================= Eventos tabla =================
  tbody.addEventListener('click', async (e)=>{
    const openBtn = e.target.closest('.cm-open');
    if (openBtn) {
      const upload = openBtn.dataset.upload;
      const author = openBtn.dataset.author || '';
      const pref   = openBtn.dataset.root ? Number(openBtn.dataset.root) : null;
      return void openThreadByGroup(upload, author, pref);
    }

    const resBtn = e.target.closest('.cm-resolve');
    if (resBtn) {
      const rootId = Number(resBtn.dataset.root || 0);
      const upload = resBtn.dataset.upload;
      const action = (resBtn.dataset.action === 'resolve');
      if (!rootId) return alert('No se encontró la raíz del hilo.');

      try{
        const r = await fetch(`/api/comments/${rootId}/resolve`, {
          method: 'POST',
          headers: {'Content-Type':'application/json', accept:'application/json'},
          body: JSON.stringify({ resolved: action })
        });
        if (!r.ok) {
          const t = await r.text().catch(()=> '');
          throw new Error(`HTTP ${r.status} ${t}`);
        }
        await loadList();
      }catch(err){
        console.error(err);
        alert('No se pudo cambiar el estado del hilo.');
      }
    }
  });

  // ================= Envío (Enter) =================
  replyBody?.addEventListener('keydown', (e)=>{
    if (e.key === 'Enter' && !e.shiftKey) {
      e.preventDefault();
      replyForm.requestSubmit?.();
    }
  });

  replyForm?.addEventListener('submit', async (e)=>{
    e.preventDefault();
    const body = replyBody.value.trim();
    if (!body) return;

    const uploadId = replyForm.dataset.uploadId;
    const parentId = replyForm.dataset.parentId ? Number(replyForm.dataset.parentId) : undefined;

    try{
      const r = await fetch(`/api/comments`, {
        method: 'POST',
        headers: {'Content-Type':'application/json', accept:'application/json'},
        body: JSON.stringify({ upload_id: uploadId, body, ...(parentId ? {parent_id: parentId} : {}) })
      });
      if (!r.ok) throw new Error(`HTTP ${r.status}`);
      replyBody.value = '';
      await openThreadByGroup(uploadId, ($(`[data-upload="${CSS.escape(uploadId)}"]`)?.dataset.author)||USER, parentId);
      await loadList();
    }catch(err){
      console.error(err);
      alert('No se pudo enviar la respuesta.');
    }
  });

  // ================= Paginación y filtros =================
  btnSearch?.addEventListener('click', ()=>{ page=1; loadList(); });
  pageSizeSel?.addEventListener('change', ()=>{ pageSize = Number(pageSizeSel.value||50); page=1; loadList(); });
  prev?.addEventListener('click', ()=>{ if(page>1){ page--; loadList(); }});
  next?.addEventListener('click', ()=>{ page++; loadList(); });

  // ================= Init =================
  loadList();
})();
