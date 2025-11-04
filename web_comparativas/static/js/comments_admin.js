/* web_comparativas/static/js/comments_admin.js */
(function () {
  const $ = (s, r=document)=>r.querySelector(s);
  const $$ = (s, r=document)=>Array.from(r.querySelectorAll(s));
  const esc=(s)=>String(s??"").replace(/&/g,"&amp;").replace(/</g,"&lt;").replace(/>/g,"&gt;");

  const fmt = (iso)=>{
    if(!iso) return "";
    try{ return new Date(iso).toLocaleString("es-AR",{dateStyle:"short", timeStyle:"short"}); }
    catch{ return String(iso); }
  };

  // UI refs
  const form = document.getElementById("cmFilters");
  const table = document.getElementById("cmTable").querySelector("tbody");
  const countersEl = document.getElementById("cmCounters");
  const pageInfo = document.getElementById("cmPageInfo");
  const prevBtn = document.getElementById("cmPrev");
  const nextBtn = document.getElementById("cmNext");
  const pageSizeEl = document.getElementById("cmPageSize");

  let PAGE = 1;

  function paramsFromForm(){
    const fd = new FormData(form);
    const o = Object.fromEntries(fd.entries());
    const out = new URLSearchParams();
    if(o.status) out.set("status", o.status);
    if(o.upload_id) out.set("upload_id", o.upload_id.trim());
    if(o.author) out.set("author", o.author.trim());
    if(o.q) out.set("q", o.q.trim());
    if(o.date_from) out.set("date_from", new Date(o.date_from).toISOString());
    if(o.date_to) out.set("date_to", new Date(o.date_to).toISOString());
    out.set("page", String(PAGE));
    out.set("page_size", String(Math.max(10, Math.min(200, parseInt(pageSizeEl.value||"50",10)))));
    out.set("sort", "-created_at");
    return out.toString();
  }

  async function load(){
    const qs = paramsFromForm();
    const r = await fetch(`/api/comments/admin?${qs}`, {headers:{accept:"application/json"}});
    if(!r.ok){ table.innerHTML='<tr><td colspan="6" class="text-danger text-center py-4">Error cargando</td></tr>'; return; }
    const data = await r.json();
    render(data);
  }

  function render(data){
    const items = Array.isArray(data?.items) ? data.items : [];
    if(!items.length){
      table.innerHTML = '<tr><td colspan="6" class="text-center text-muted py-4">Sin resultados.</td></tr>';
    }else{
      table.innerHTML = items.map(c=>{
        const estado = c.deleted_at ? "Borrado" : (c.resolved_at ? "Resuelto" : "Abierto");
        const idAttr = `data-id="${c.id}" data-upload="${esc(c.upload_id)}"`;
        return `<tr ${idAttr}>
          <td>${esc(fmt(c.created_at))}</td>
          <td>${esc(c.upload_id)}</td>
          <td>${esc(c.author || "")}</td>
          <td>${esc(c.body)}</td>
          <td>${estado}</td>
          <td>
            <button class="btn btn-sm btn-outline-primary cm-act" data-act="open">Abrir tablero</button>
            ${!c.deleted_at ? (
              c.resolved_at
                ? '<button class="btn btn-sm btn-outline-secondary cm-act" data-act="unresolve">Reabrir</button>'
                : '<button class="btn btn-sm btn-success cm-act" data-act="resolve">Resolver</button>'
            ) : ''
            }
            ${!c.deleted_at ? '<button class="btn btn-sm btn-outline-danger cm-act" data-act="delete">Borrar</button>' : ''}
          </td>
        </tr>`;
      }).join("");
    }

    // counters
    const ct = data?.counters || {};
    countersEl.textContent = `Abiertos: ${ct.open||0} · Resueltos: ${ct.resolved||0} · Borrados: ${ct.deleted||0}`;

    // pagination
    const size = Math.max(10, Math.min(200, parseInt(pageSizeEl.value||"50",10)));
    const total = parseInt(data?.total||0,10);
    const pages = Math.max(1, Math.ceil(total/size));
    if(PAGE > pages) PAGE = pages;
    pageInfo.dataset.page = String(PAGE);
    pageInfo.textContent = `Página ${PAGE} / ${pages} (${total} total)`;
    prevBtn.disabled = PAGE<=1;
    nextBtn.disabled = PAGE>=pages;
  }

  // Eventos
  form.addEventListener("submit", (e)=>{ e.preventDefault(); PAGE=1; load(); });
  pageSizeEl.addEventListener("change", ()=>{ PAGE=1; load(); });
  prevBtn.addEventListener("click", ()=>{ PAGE = Math.max(1, (PAGE-1)); load(); });
  nextBtn.addEventListener("click", ()=>{ PAGE = (parseInt(pageInfo.textContent.split('/')[1]||'1',10)>PAGE)?PAGE+1:PAGE; load(); });

  // Acciones fila
  table.addEventListener("click", async (e)=>{
    const btn = e.target.closest(".cm-act"); if(!btn) return;
    const tr = btn.closest("tr"); const id = parseInt(tr?.dataset.id||"",10);
    const up = tr?.dataset.upload;
    if(!Number.isFinite(id) || !up) return;

    const act = btn.dataset.act;
    try{
      if(act==="open"){
        // Abre el tablero con el panel de comentarios visible
        window.open(`/tablero/${encodeURIComponent(up)}?open_comments=1`, "_blank");
      }else if(act==="resolve"){
        await fetch(`/api/comments/${id}`, {method:"PATCH", headers:{"content-type":"application/json"}, body:JSON.stringify({is_resolved:true})});
        load();
      }else if(act==="unresolve"){
        await fetch(`/api/comments/${id}`, {method:"PATCH", headers:{"content-type":"application/json"}, body:JSON.stringify({is_resolved:false})});
        load();
      }else if(act==="delete"){
        if(!confirm("¿Borrar este comentario?")) return;
        await fetch(`/api/comments/${id}`, {method:"DELETE"});
        load();
      }
    }catch(err){ alert("Acción fallida."); console.error(err); }
  });

  load(); // inicial
})();
