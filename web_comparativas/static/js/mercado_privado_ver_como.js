/* "Ver como usuario" (Mercado Privado, admin-only, sep-2026).
   Selector de fichas (buscador + chips con "x" + contador + "Vaciar selección"),
   mismo patrón visual que el selector de sic/users_form.html pero reimplementado
   en tema claro para Mercado Privado (esas páginas extienden base.html, no
   sic/base_sic.html — las variables --sic-* no existen ahí).

   Responsabilidad de este módulo:
   - Cargar el padrón de usuarios (admin-only, GET .../dimensiones/ver-como/usuarios).
   - Mantener la selección actual y PERSISTIRLA en localStorage bajo una key fija,
     así viaja sola al navegar entre Dimensionamiento/Oportunidades/Home (server-
     rendered, sin SPA) sin que cada página tenga que pasársela a la siguiente.
   - Exponer getSelectedIds()/onChange() para que cada página arme sus propios
     fetch con la selección — ESTE módulo no sabe nada de dimensionamiento ni de
     oportunidades, así queda reusable en las tres pantallas.

   Seguridad: esto es SOLO conveniencia de UI. La aplicación real de "Ver como
   usuario" (y el chequeo de que quien pide es admin) vive enteramente en el
   backend (cartera_visibilidad.resolve_effective_scope) — un usuario no-admin
   que edite localStorage a mano no gana nada: el backend ignora el parámetro. */
(function (global) {
  "use strict";

  var STORAGE_KEY = "mp_ver_como_usuarios_v1";
  var USUARIOS_API = "/api/mercado-privado/dimensiones/ver-como/usuarios";
  var STYLE_ID = "mpvc-styles";
  var CHIP_CAP = 6;

  function loadSelected() {
    try {
      var raw = global.localStorage.getItem(STORAGE_KEY);
      if (!raw) return [];
      var parsed = JSON.parse(raw);
      if (!Array.isArray(parsed)) return [];
      return parsed.filter(function (v) { return Number.isInteger(v); });
    } catch (e) {
      return [];
    }
  }

  function saveSelected(ids) {
    try {
      global.localStorage.setItem(STORAGE_KEY, JSON.stringify(ids));
    } catch (e) {
      // localStorage no disponible (modo privado, storage lleno, etc.) — la
      // selección no persiste entre navegaciones, pero la página actual sigue
      // funcionando con lo que haya en memoria.
    }
  }

  var selectedIds = loadSelected();
  var listeners = [];
  var usuariosCache = null;
  var usuariosPromise = null;

  function notify() {
    saveSelected(selectedIds);
    listeners.forEach(function (cb) {
      try { cb(selectedIds.slice()); } catch (e) { console.error("[MPVerComo] listener error:", e); }
    });
  }

  function getSelectedIds() {
    return selectedIds.slice();
  }

  function setSelectedIds(ids) {
    var clean = Array.from(new Set((ids || []).filter(function (v) { return Number.isInteger(v); })));
    selectedIds = clean;
    notify();
  }

  function onChange(cb) {
    if (typeof cb === "function") listeners.push(cb);
  }

  function fetchUsuarios() {
    if (usuariosCache) return Promise.resolve(usuariosCache);
    if (usuariosPromise) return usuariosPromise;
    usuariosPromise = fetch(USUARIOS_API, {
      credentials: "same-origin",
      headers: { Accept: "application/json" },
    })
      .then(function (res) {
        if (!res.ok) throw new Error("HTTP " + res.status);
        return res.json();
      })
      .then(function (json) {
        usuariosCache = (json && json.data) || [];
        // Poda selección de usuarios que ya no existen (usuario borrado desde
        // otra pestaña/sesión) para no mandar ids fantasma al backend.
        var validIds = new Set(usuariosCache.map(function (u) { return u.id; }));
        var pruned = selectedIds.filter(function (id) { return validIds.has(id); });
        if (pruned.length !== selectedIds.length) {
          selectedIds = pruned;
          notify();
        }
        return usuariosCache;
      })
      .catch(function (err) {
        usuariosPromise = null; // permite reintentar en el próximo init/fetch
        throw err;
      });
    return usuariosPromise;
  }

  function escapeHtml(s) {
    return String(s).replace(/[&<>"']/g, function (c) {
      return { "&": "&amp;", "<": "&lt;", ">": "&gt;", '"': "&quot;", "'": "&#39;" }[c];
    });
  }

  function highlight(text, q) {
    var safe = escapeHtml(text);
    if (!q) return safe;
    var idx = text.toLowerCase().indexOf(q.toLowerCase());
    if (idx === -1) return safe;
    return escapeHtml(text.slice(0, idx)) + "<mark>" + escapeHtml(text.slice(idx, idx + q.length)) +
      "</mark>" + escapeHtml(text.slice(idx + q.length));
  }

  function ensureStyles() {
    if (document.getElementById(STYLE_ID)) return;
    var style = document.createElement("style");
    style.id = STYLE_ID;
    style.textContent = [
      ".mpvc-wrap{display:flex;flex-direction:column;gap:.22rem;position:relative;min-width:240px;}",
      ".mpvc-label{font-size:.6rem;font-weight:700;text-transform:uppercase;letter-spacing:.06em;color:#94a3b8;margin:0;display:flex;align-items:center;gap:.3rem;}",
      ".mpvc-label .bi{font-size:.85rem;}",
      ".mpvc-trigger{height:34px;border-radius:999px;border:1px solid rgba(6,72,111,.15);background:#fff;font-size:.8rem;font-weight:600;color:#06486f;padding:.25rem .9rem;display:flex;align-items:center;justify-content:space-between;gap:.5rem;width:100%;min-width:220px;cursor:pointer;}",
      ".mpvc-trigger:hover{border-color:rgba(6,72,111,.3);}",
      ".mpvc-trigger.mpvc-active{border-color:#1e5c8a;background:rgba(30,92,138,.06);}",
      ".mpvc-trigger-label{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}",
      ".mpvc-badge{display:inline-grid;place-items:center;min-width:18px;height:18px;padding:0 5px;border-radius:9px;background:#1e5c8a;color:#fff;font-size:.66rem;font-weight:700;flex:0 0 auto;}",
      ".mpvc-panel{position:absolute;top:calc(100% + 4px);left:0;z-index:1200;width:320px;max-width:90vw;background:#fff;border:1px solid rgba(6,72,111,.15);border-radius:12px;box-shadow:0 10px 30px rgba(6,32,54,.16);padding:.6rem;}",
      ".mpvc-search{width:100%;border:1px solid rgba(6,72,111,.15);border-radius:8px;padding:.4rem .6rem;font-size:.8rem;margin-bottom:.5rem;}",
      ".mpvc-search:focus{outline:none;border-color:#1e5c8a;box-shadow:0 0 0 3px rgba(30,92,138,.15);}",
      ".mpvc-tray{display:flex;flex-wrap:wrap;gap:.3rem;margin-bottom:.4rem;min-height:20px;}",
      ".mpvc-tray .mpvc-empty{color:#94a3b8;font-size:.72rem;font-style:italic;}",
      ".mpvc-chip{display:inline-flex;align-items:center;gap:.3rem;background:rgba(30,92,138,.1);border:1px solid rgba(30,92,138,.28);color:#06486f;font-size:.7rem;font-weight:600;padding:.18rem .3rem .18rem .55rem;border-radius:999px;max-width:180px;}",
      ".mpvc-chip span{overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}",
      ".mpvc-chip button{all:unset;display:flex;align-items:center;justify-content:center;width:14px;height:14px;border-radius:50%;color:#1e5c8a;cursor:pointer;font-size:.65rem;}",
      ".mpvc-chip button:hover{background:rgba(30,92,138,.18);}",
      ".mpvc-list{max-height:220px;overflow-y:auto;border:1px solid rgba(6,72,111,.1);border-radius:8px;}",
      ".mpvc-row{display:flex;align-items:center;gap:.5rem;padding:.4rem .55rem;cursor:pointer;border-bottom:1px solid rgba(6,72,111,.06);}",
      ".mpvc-row:last-child{border-bottom:none;}",
      ".mpvc-row:hover{background:#f7fafc;}",
      ".mpvc-row.mpvc-checked{background:rgba(30,92,138,.07);}",
      ".mpvc-row input{margin:0;flex:0 0 auto;}",
      ".mpvc-row .mpvc-main{flex:1;min-width:0;font-size:.78rem;color:#0f2a3d;overflow:hidden;text-overflow:ellipsis;white-space:nowrap;}",
      ".mpvc-row .mpvc-tag{font-size:.6rem;text-transform:uppercase;letter-spacing:.04em;color:#64748b;background:rgba(100,116,139,.12);border-radius:999px;padding:1px 6px;flex:0 0 auto;}",
      ".mpvc-row mark{background:rgba(21,128,61,.25);border-radius:2px;}",
      ".mpvc-empty-row{padding:.8rem;text-align:center;color:#94a3b8;font-size:.75rem;}",
      ".mpvc-footer{display:flex;justify-content:space-between;align-items:center;margin-top:.45rem;font-size:.7rem;color:#64748b;}",
      ".mpvc-footer button{all:unset;cursor:pointer;font-size:.7rem;font-weight:600;color:#1e5c8a;}",
      ".mpvc-footer button:hover{text-decoration:underline;}",
      ".mpvc-footer button[disabled]{opacity:.4;cursor:not-allowed;text-decoration:none;}",
    ].join("\n");
    document.head.appendChild(style);
  }

  function buildLabel(u) {
    return (u.nombre ? (u.nombre + " · " + u.email) : u.email);
  }

  function init(mountEl, options) {
    options = options || {};
    if (!mountEl) return;
    ensureStyles();
    mountEl.innerHTML = "";

    var wrap = document.createElement("div");
    wrap.className = "mpvc-wrap";

    var label = document.createElement("label");
    label.className = "mpvc-label";
    label.innerHTML = '<i class="bi bi-eye"></i> Ver como usuario';
    wrap.appendChild(label);

    var trigger = document.createElement("button");
    trigger.type = "button";
    trigger.className = "mpvc-trigger";
    trigger.innerHTML = '<span class="mpvc-trigger-label">Todos (sin acotar)</span>';
    wrap.appendChild(trigger);

    var panel = document.createElement("div");
    panel.className = "mpvc-panel";
    panel.style.display = "none";

    var search = document.createElement("input");
    search.type = "text";
    search.className = "mpvc-search";
    search.placeholder = "Buscar por nombre o email…";
    search.setAttribute("autocomplete", "off");
    panel.appendChild(search);

    var tray = document.createElement("div");
    tray.className = "mpvc-tray";
    panel.appendChild(tray);

    var list = document.createElement("div");
    list.className = "mpvc-list";
    panel.appendChild(list);

    var footer = document.createElement("div");
    footer.className = "mpvc-footer";
    var countEl = document.createElement("span");
    var clearBtn = document.createElement("button");
    clearBtn.type = "button";
    clearBtn.textContent = "Vaciar selección";
    footer.appendChild(countEl);
    footer.appendChild(clearBtn);
    panel.appendChild(footer);

    wrap.appendChild(panel);
    mountEl.appendChild(wrap);

    var rows = [];
    var q = "";

    function updateTriggerLabel() {
      var n = selectedIds.length;
      trigger.classList.toggle("mpvc-active", n > 0);
      var textSpan = trigger.querySelector(".mpvc-trigger-label");
      if (n === 0) {
        textSpan.textContent = "Todos (sin acotar)";
        return;
      }
      var nombres = rows
        .filter(function (r) { return selectedIds.indexOf(r.id) !== -1; })
        .map(function (r) { return r.nombre || r.email; });
      textSpan.textContent = n === 1 ? nombres[0] : n + " usuarios seleccionados";
    }

    function renderTray() {
      var sel = rows.filter(function (r) { return selectedIds.indexOf(r.id) !== -1; });
      if (!sel.length) {
        tray.innerHTML = '<span class="mpvc-empty">ninguno seleccionado</span>';
        return;
      }
      tray.innerHTML = sel.map(function (r) {
        return '<span class="mpvc-chip"><span>' + escapeHtml(r.nombre || r.email) + '</span>' +
          '<button type="button" data-id="' + r.id + '" aria-label="Quitar ' + escapeHtml(r.nombre || r.email) + '">✕</button></span>';
      }).join("");
    }

    function renderList() {
      var qq = q.trim().toLowerCase();
      var visible = rows.filter(function (r) {
        if (!qq) return true;
        var hay = (r.nombre + " " + r.email).toLowerCase();
        return hay.indexOf(qq) !== -1;
      });
      if (!visible.length) {
        list.innerHTML = '<div class="mpvc-empty-row">Sin resultados' + (qq ? ' para "' + escapeHtml(search.value) + '"' : "") + '.</div>';
        return;
      }
      list.innerHTML = visible.map(function (r) {
        var checked = selectedIds.indexOf(r.id) !== -1;
        return '<label class="mpvc-row' + (checked ? " mpvc-checked" : "") + '" data-id="' + r.id + '">' +
          '<input type="checkbox"' + (checked ? " checked" : "") + '>' +
          '<span class="mpvc-main">' + highlight(buildLabel(r), qq) + '</span>' +
          '<span class="mpvc-tag">' + escapeHtml(r.rol || "") + '</span></label>';
      }).join("");
    }

    function renderFooter() {
      countEl.innerHTML = "<b>" + selectedIds.length + "</b> de " + rows.length;
      clearBtn.disabled = selectedIds.length === 0;
    }

    function renderAll() {
      renderTray();
      renderList();
      renderFooter();
      updateTriggerLabel();
    }

    function toggle(id) {
      var idx = selectedIds.indexOf(id);
      if (idx === -1) selectedIds.push(id); else selectedIds.splice(idx, 1);
      notify();
      renderAll();
    }

    trigger.addEventListener("click", function () {
      var show = panel.style.display === "none";
      panel.style.display = show ? "block" : "none";
      if (show) search.focus();
    });
    document.addEventListener("click", function (e) {
      if (!wrap.contains(e.target)) panel.style.display = "none";
    });
    list.addEventListener("click", function (e) {
      var row = e.target.closest(".mpvc-row");
      if (!row) return;
      e.preventDefault();
      toggle(parseInt(row.getAttribute("data-id"), 10));
    });
    tray.addEventListener("click", function (e) {
      var btn = e.target.closest("[data-id]");
      if (!btn) return;
      toggle(parseInt(btn.getAttribute("data-id"), 10));
    });
    clearBtn.addEventListener("click", function () {
      if (!selectedIds.length) return;
      selectedIds = [];
      notify();
      renderAll();
    });
    search.addEventListener("input", function () {
      q = search.value;
      renderList();
    });

    fetchUsuarios().then(function (usuarios) {
      rows = (usuarios || []).slice().sort(function (a, b) {
        return (a.nombre || a.email).localeCompare(b.nombre || b.email, "es", { sensitivity: "base" });
      });
      renderAll();
    }).catch(function (err) {
      list.innerHTML = '<div class="mpvc-empty-row">No se pudo cargar el padrón de usuarios.</div>';
      console.error("[MPVerComo] fetchUsuarios:", err);
    });

    renderAll();
    if (typeof options.onReady === "function") options.onReady();
  }

  global.MPVerComo = {
    getSelectedIds: getSelectedIds,
    setSelectedIds: setSelectedIds,
    onChange: onChange,
    init: init,
  };
})(window);
