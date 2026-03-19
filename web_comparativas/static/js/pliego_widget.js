/**
 * pliego_widget.js
 * Widget flotante de comentarios rápidos para el módulo Lectura de Pliegos.
 *
 * Genera / alimenta tickets en Mesa de Ayuda bajo la categoría "lectura_pliegos".
 * No depende de frameworks externos. Solo Bootstrap 5 + Vanilla JS.
 *
 * Uso: incluir el script en cualquier template de pliegos que defina
 * un elemento <div id="pliego-widget-root"> con los data attributes:
 *   data-pliego-id         – ID numérico de PliegoSolicitud (requerido)
 *   data-numero-proceso    – N° de proceso (opcional, texto)
 *   data-nombre-licitacion – Nombre de la licitación (opcional)
 *   data-organismo         – Organismo (opcional)
 *   data-titulo-caso       – Título del caso (opcional)
 *   data-seccion           – "lista" | "detalle" (opcional, default "detalle")
 */
(function () {
  'use strict';

  // ─── Leer contexto desde el root element ──────────────────────────────────
  const ROOT_ID = 'pliego-widget-root';

  function init() {
    console.log('[PliegoWidget] init() ejecutado');
    const root = document.getElementById(ROOT_ID);
    if (!root) {
      console.log('[PliegoWidget] root #pliego-widget-root NO encontrado — widget no carga');
      return;
    }
    console.log('[PliegoWidget] root encontrado:', root);

    const ctx = {
      pliegoId:          parseInt(root.dataset.pliegoId, 10),
      numeroProceso:     root.dataset.numeroProceso    || '',
      nombreLicitacion:  root.dataset.nombreLicitacion || '',
      organismo:         root.dataset.organismo        || '',
      tituloCaso:        root.dataset.tituloCaso       || '',
      seccion:           root.dataset.seccion          || 'detalle',
    };

    if (!ctx.pliegoId) {
      console.log('[PliegoWidget] pliegoId inválido o 0 — widget no carga. data-pliego-id=', root.dataset.pliegoId);
      return;
    }
    console.log('[PliegoWidget] pliegoId:', ctx.pliegoId, '— creando widget...');

    const DRAFT_KEY = `pliego_widget_draft_${ctx.pliegoId}`;

    // ─── Inyectar CSS ────────────────────────────────────────────────────────
    injectStyles();

    // ─── Crear estructura del widget ─────────────────────────────────────────
    const wrapper = document.createElement('div');
    wrapper.id = 'pw-wrapper';
    wrapper.innerHTML = buildHTML(ctx);
    // Aplicar posicionamiento crítico como inline style para evitar conflictos CSS
    wrapper.setAttribute('style',
      'position:fixed!important;bottom:1.5rem!important;right:1.5rem!important;' +
      'z-index:9999!important;');
    document.body.appendChild(wrapper);
    console.log('[PliegoWidget] wrapper añadido al body. style=', wrapper.getAttribute('style'));

    // ─── Referencias DOM ─────────────────────────────────────────────────────
    const fab       = document.getElementById('pw-fab');
    const panel     = document.getElementById('pw-panel');
    const closeBtn  = document.getElementById('pw-close');
    const minBtn    = document.getElementById('pw-minimize');
    const textarea  = document.getElementById('pw-textarea');
    const sendBtn   = document.getElementById('pw-send');
    const histArea  = document.getElementById('pw-history');
    const badge     = document.getElementById('pw-badge');
    const feedback  = document.getElementById('pw-feedback');
    const tabWrite  = document.getElementById('pw-tab-write');
    const tabHist   = document.getElementById('pw-tab-hist');
    const paneWrite = document.getElementById('pw-pane-write');
    const paneHist  = document.getElementById('pw-pane-hist');

    let isOpen      = false;
    let isMinimized = false;

    // ─── Cargar draft guardado ────────────────────────────────────────────────
    const savedDraft = sessionStorage.getItem(DRAFT_KEY);
    if (savedDraft) textarea.value = savedDraft;

    // ─── Cargar resumen inicial (badge + historial) ───────────────────────────
    loadSummary();

    // ─── FAB: abrir/cerrar panel ──────────────────────────────────────────────
    fab.addEventListener('click', () => {
      if (!isOpen) {
        openPanel();
      } else if (isMinimized) {
        restorePanel();
      } else {
        closePanel();
      }
    });

    closeBtn.addEventListener('click', closePanel);

    minBtn.addEventListener('click', () => {
      if (isMinimized) {
        restorePanel();
      } else {
        minimizePanel();
      }
    });

    // ─── Tabs: Escribir / Historial ───────────────────────────────────────────
    tabWrite.addEventListener('click', () => switchTab('write'));
    tabHist.addEventListener('click', () => {
      switchTab('hist');
      loadSummary();
    });

    // ─── Guardar draft en sesión mientras escribe ─────────────────────────────
    textarea.addEventListener('input', () => {
      sessionStorage.setItem(DRAFT_KEY, textarea.value);
      sendBtn.disabled = textarea.value.trim().length === 0;
    });

    // ─── Enter para enviar (Shift+Enter = nueva línea) ────────────────────────
    textarea.addEventListener('keydown', (e) => {
      if (e.key === 'Enter' && !e.shiftKey) {
        e.preventDefault();
        if (!sendBtn.disabled) submitComment();
      }
    });

    sendBtn.addEventListener('click', submitComment);

    // ─── Funciones de UI ─────────────────────────────────────────────────────

    function openPanel() {
      isOpen      = true;
      isMinimized = false;
      panel.style.display     = 'flex';
      panel.classList.add('pw-open');
      panel.classList.remove('pw-minimized');
      fab.classList.add('pw-fab-active');
      fab.title = 'Cerrar notas';
      textarea.focus();
    }

    function closePanel() {
      isOpen = false;
      panel.classList.remove('pw-open');
      panel.style.display = 'none';
      fab.classList.remove('pw-fab-active');
      fab.title = 'Notas y consultas del proceso';
    }

    function minimizePanel() {
      isMinimized = true;
      panel.classList.add('pw-minimized');
      minBtn.title = 'Restaurar';
      minBtn.innerHTML = '<i class="bi bi-chevron-up"></i>';
    }

    function restorePanel() {
      isMinimized = false;
      panel.classList.remove('pw-minimized');
      minBtn.title = 'Minimizar';
      minBtn.innerHTML = '<i class="bi bi-dash-lg"></i>';
    }

    function switchTab(name) {
      if (name === 'write') {
        tabWrite.classList.add('pw-tab-active');
        tabHist.classList.remove('pw-tab-active');
        paneWrite.style.display = 'flex';
        paneHist.style.display  = 'none';
      } else {
        tabHist.classList.add('pw-tab-active');
        tabWrite.classList.remove('pw-tab-active');
        paneHist.style.display  = 'flex';
        paneWrite.style.display = 'none';
      }
    }

    function showFeedback(msg, type) {
      feedback.textContent    = msg;
      feedback.className      = `pw-feedback pw-feedback-${type}`;
      feedback.style.display  = 'block';
      setTimeout(() => { feedback.style.display = 'none'; }, 3500);
    }

    // ─── Enviar comentario ────────────────────────────────────────────────────

    function submitComment() {
      const text = textarea.value.trim();
      if (!text) return;

      sendBtn.disabled = true;
      sendBtn.innerHTML = '<span class="pw-spinner"></span>';

      const payload = {
        pliego_id:         ctx.pliegoId,
        message:           text,
        numero_proceso:    ctx.numeroProceso,
        nombre_licitacion: ctx.nombreLicitacion,
        organismo:         ctx.organismo,
        titulo_caso:       ctx.tituloCaso,
        seccion:           ctx.seccion,
      };

      fetch('/sic/api/tickets/pliego-comment', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
      })
        .then((r) => r.json())
        .then((data) => {
          if (data.ok) {
            textarea.value = '';
            sessionStorage.removeItem(DRAFT_KEY);
            sendBtn.disabled = true;
            showFeedback('Nota enviada correctamente.', 'ok');
            loadSummary();
          } else {
            showFeedback('Error al enviar: ' + (data.error || 'desconocido'), 'err');
          }
        })
        .catch(() => showFeedback('Error de red al enviar.', 'err'))
        .finally(() => {
          sendBtn.disabled = textarea.value.trim().length === 0;
          sendBtn.innerHTML = '<i class="bi bi-send-fill"></i> Enviar';
        });
    }

    // ─── Cargar resumen e historial ───────────────────────────────────────────

    function loadSummary() {
      fetch(`/sic/api/tickets/pliego/${ctx.pliegoId}/summary`)
        .then((r) => r.json())
        .then((data) => {
          if (!data.ok) return;

          // Badge en FAB
          const count = data.open_count || 0;
          badge.textContent   = count > 9 ? '9+' : String(count);
          badge.style.display = count > 0 ? 'flex' : 'none';

          // Historial
          renderHistory(data.recent_messages || [], data.active_ticket_id, data.active_ticket_status);
        })
        .catch(() => {});
    }

    function renderHistory(messages, ticketId, ticketStatus) {
      if (!messages.length) {
        histArea.innerHTML = '<p class="pw-empty-hist">Todavía no hay notas para este proceso.</p>';
        return;
      }

      const statusLabel = {
        abierto:   '🟢 Abierto',
        pendiente: '🟡 Pendiente',
        resuelto:  '🔵 Resuelto',
        cerrado:   '⚫ Cerrado',
      }[ticketStatus] || ticketStatus;

      let html = `<div class="pw-hist-meta">
        <span>Consulta #${ticketId}</span>
        <span class="pw-hist-status">${statusLabel}</span>
      </div>`;

      messages.forEach((m) => {
        const side = m.is_me ? 'pw-msg-me' : 'pw-msg-them';
        const label = m.is_admin ? `${m.sender} <span class="pw-admin-tag">ADMIN</span>` : m.sender;
        html += `
          <div class="pw-msg-row ${side}">
            <div class="pw-msg-bubble">
              <div class="pw-msg-text">${escapeHtml(m.message)}</div>
              <div class="pw-msg-meta">${label} · ${m.created_at}</div>
            </div>
          </div>`;
      });

      if (ticketId) {
        html += `<div class="pw-hist-link">
          <a href="/sic/helpdesk/${ticketId}" target="_blank" rel="noopener">
            <i class="bi bi-box-arrow-up-right"></i> Ver consulta completa en Mesa de Ayuda
          </a>
        </div>`;
      }

      histArea.innerHTML = html;
      histArea.scrollTop = histArea.scrollHeight;
    }

    function escapeHtml(str) {
      return String(str)
        .replace(/&/g, '&amp;')
        .replace(/</g, '&lt;')
        .replace(/>/g, '&gt;')
        .replace(/"/g, '&quot;');
    }
  }

  // ─── HTML del widget ──────────────────────────────────────────────────────

  function buildHTML(ctx) {
    const procStr = ctx.numeroProceso
      ? `<span class="pw-ctx-tag"><i class="bi bi-hash"></i>${escCtx(ctx.numeroProceso)}</span>`
      : '';
    const licitStr = ctx.nombreLicitacion
      ? `<span class="pw-ctx-tag"><i class="bi bi-file-text"></i>${escCtx(ctx.nombreLicitacion.substring(0, 40))}${ctx.nombreLicitacion.length > 40 ? '…' : ''}</span>`
      : '';

    return `
      <!-- FAB flotante -->
      <button id="pw-fab" class="pw-fab"
        style="position:relative;width:52px;height:52px;border-radius:50%;border:none;background:linear-gradient(135deg,#5770b0,#06486f);color:#fff;font-size:1.25rem;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 18px rgba(6,72,111,.45);"
        title="Notas y consultas del proceso" aria-label="Abrir notas del proceso">
        <i class="bi bi-chat-text-fill pw-fab-icon"></i>
        <span id="pw-badge" class="pw-badge" style="display:none;position:absolute;top:-4px;right:-4px;min-width:18px;height:18px;border-radius:9px;background:#ef4444;color:#fff;font-size:.65rem;font-weight:700;align-items:center;justify-content:center;padding:0 4px;border:2px solid #fff;">0</span>
      </button>

      <!-- Panel flotante -->
      <div id="pw-panel" class="pw-panel"
        style="display:none;position:fixed;bottom:72px;right:1.5rem;width:360px;max-height:540px;border-radius:16px;background:rgba(10,22,38,0.97);border:1px solid rgba(87,112,176,.35);box-shadow:0 16px 48px rgba(0,0,0,.55);backdrop-filter:blur(18px);flex-direction:column;overflow:hidden;color:#e2e8f0;font-family:inherit;font-size:.88rem;z-index:9998;"
        role="dialog" aria-label="Notas del proceso">

        <!-- Header -->
        <div class="pw-header">
          <div class="pw-header-title">
            <i class="bi bi-chat-text me-1"></i>
            <span>Notas del proceso</span>
          </div>
          <div class="pw-header-actions">
            <button id="pw-minimize" class="pw-icon-btn" title="Minimizar"><i class="bi bi-dash-lg"></i></button>
            <button id="pw-close"    class="pw-icon-btn" title="Cerrar"><i class="bi bi-x-lg"></i></button>
          </div>
        </div>

        <!-- Contexto del proceso -->
        <div class="pw-ctx-bar">
          ${procStr}${licitStr}
          ${!procStr && !licitStr ? `<span class="pw-ctx-tag"><i class="bi bi-folder2"></i>Pliego #${ctx.pliegoId}</span>` : ''}
        </div>

        <!-- Tabs -->
        <div class="pw-tabs">
          <button id="pw-tab-write" class="pw-tab pw-tab-active">
            <i class="bi bi-pencil"></i> Escribir
          </button>
          <button id="pw-tab-hist" class="pw-tab">
            <i class="bi bi-clock-history"></i> Historial
          </button>
        </div>

        <!-- Pane: Escribir -->
        <div id="pw-pane-write" class="pw-pane" style="display:flex; flex-direction:column; flex:1; gap:.6rem">
          <textarea
            id="pw-textarea"
            class="pw-textarea"
            rows="4"
            placeholder="Escribí tu observación, duda o nota sobre este proceso…&#10;(Enter para enviar · Shift+Enter para nueva línea)"
            maxlength="2000"
          ></textarea>
          <div id="pw-feedback" class="pw-feedback" style="display:none"></div>
          <button id="pw-send" class="pw-send-btn" disabled>
            <i class="bi bi-send-fill"></i> Enviar
          </button>
          <p class="pw-hint">
            <i class="bi bi-info-circle"></i>
            Tu nota quedará visible en <strong>Mesa de Ayuda</strong> para que el equipo pueda responderla.
          </p>
        </div>

        <!-- Pane: Historial -->
        <div id="pw-pane-hist" class="pw-pane" style="display:none; flex-direction:column; flex:1">
          <div id="pw-history" class="pw-history">
            <p class="pw-empty-hist">Cargando historial…</p>
          </div>
        </div>

      </div>
    `;
  }

  function escCtx(str) {
    return String(str).replace(/</g, '&lt;').replace(/>/g, '&gt;');
  }

  // ─── CSS del widget (inyectado una sola vez) ──────────────────────────────

  function injectStyles() {
    if (document.getElementById('pw-styles')) return;
    const style = document.createElement('style');
    style.id = 'pw-styles';
    style.textContent = `
      /* ── FAB ────────────────────────────────────────────── */
      #pw-wrapper { position: fixed !important; bottom: 1.5rem !important; right: 1.5rem !important; z-index: 9999 !important; }

      .pw-fab {
        position: relative;
        width: 52px; height: 52px;
        border-radius: 50%;
        border: none;
        background: linear-gradient(135deg, #5770b0, #06486f);
        color: #fff;
        font-size: 1.25rem;
        cursor: pointer;
        display: flex; align-items: center; justify-content: center;
        box-shadow: 0 4px 18px rgba(6,72,111,.45);
        transition: transform .2s, box-shadow .2s;
      }
      .pw-fab:hover          { transform: scale(1.08); box-shadow: 0 6px 22px rgba(6,72,111,.6); }
      .pw-fab.pw-fab-active  { background: linear-gradient(135deg, #06486f, #5770b0); }

      .pw-badge {
        position: absolute; top: -4px; right: -4px;
        min-width: 18px; height: 18px; border-radius: 9px;
        background: #ef4444; color: #fff;
        font-size: .65rem; font-weight: 700;
        display: flex; align-items: center; justify-content: center;
        padding: 0 4px; border: 2px solid #fff;
        box-shadow: 0 2px 6px rgba(239,68,68,.4);
      }

      /* ── Panel ──────────────────────────────────────────── */
      .pw-panel {
        position: fixed !important; bottom: 72px !important; right: 1.5rem !important;
        width: 360px;
        max-height: 540px;
        border-radius: 16px;
        background: rgba(10, 22, 38, 0.97);
        border: 1px solid rgba(87,112,176,.35);
        box-shadow: 0 16px 48px rgba(0,0,0,.55);
        backdrop-filter: blur(18px);
        display: flex; flex-direction: column;
        overflow: hidden;
        animation: pw-slide-in .22s cubic-bezier(.16,1,.3,1);
        color: #e2e8f0;
        font-family: inherit;
        font-size: .88rem;
      }
      .pw-panel.pw-minimized {
        max-height: 52px;
        overflow: hidden;
      }

      @keyframes pw-slide-in {
        from { opacity:0; transform: translateY(12px) scale(.97); }
        to   { opacity:1; transform: translateY(0)    scale(1); }
      }

      /* ── Header ─────────────────────────────────────────── */
      .pw-header {
        display: flex; align-items: center; justify-content: space-between;
        padding: .75rem 1rem;
        background: rgba(87,112,176,.14);
        border-bottom: 1px solid rgba(87,112,176,.2);
        flex-shrink: 0;
      }
      .pw-header-title { display: flex; align-items: center; gap:.4rem; font-weight: 600; font-size:.88rem; color:#cbd5e1; }
      .pw-header-actions { display: flex; gap:.25rem; }
      .pw-icon-btn {
        background: none; border: none; color: #94a3b8;
        width: 28px; height: 28px; border-radius: 6px;
        display: flex; align-items: center; justify-content: center;
        cursor: pointer; font-size: .85rem; transition: background .15s, color .15s;
      }
      .pw-icon-btn:hover { background: rgba(255,255,255,.08); color: #fff; }

      /* ── Contexto ───────────────────────────────────────── */
      .pw-ctx-bar {
        display: flex; flex-wrap: wrap; gap:.35rem;
        padding: .5rem 1rem;
        border-bottom: 1px solid rgba(87,112,176,.12);
        flex-shrink: 0;
      }
      .pw-ctx-tag {
        display: inline-flex; align-items: center; gap:.3rem;
        font-size: .72rem; font-weight: 500;
        color: #8ba4d8;
        background: rgba(87,112,176,.12);
        border: 1px solid rgba(87,112,176,.2);
        padding: .2em .55em; border-radius: 20px;
      }
      .pw-ctx-tag i { opacity:.75; }

      /* ── Tabs ───────────────────────────────────────────── */
      .pw-tabs {
        display: flex;
        border-bottom: 1px solid rgba(87,112,176,.18);
        flex-shrink: 0;
      }
      .pw-tab {
        flex: 1; background: none; border: none; color: #94a3b8;
        padding: .5rem .75rem; font-size: .78rem; font-weight: 500;
        cursor: pointer; transition: color .15s, border-bottom .15s;
        border-bottom: 2px solid transparent;
        display: flex; align-items: center; justify-content: center; gap:.35rem;
      }
      .pw-tab:hover { color: #cbd5e1; }
      .pw-tab.pw-tab-active { color: #8ba4d8; border-bottom-color: #5770b0; }

      /* ── Panes ──────────────────────────────────────────── */
      .pw-pane { padding: .75rem 1rem; overflow-y: auto; }

      /* ── Textarea ───────────────────────────────────────── */
      .pw-textarea {
        width: 100%; resize: none;
        background: rgba(255,255,255,.05);
        border: 1px solid rgba(87,112,176,.25);
        border-radius: 10px; color: #e2e8f0;
        padding: .65rem .85rem; font-size: .85rem;
        font-family: inherit; line-height: 1.5;
        transition: border-color .15s;
        outline: none;
      }
      .pw-textarea:focus { border-color: rgba(87,112,176,.55); background: rgba(255,255,255,.07); }
      .pw-textarea::placeholder { color: #475569; }

      /* ── Botón enviar ────────────────────────────────────── */
      .pw-send-btn {
        width: 100%; padding: .55rem 1rem;
        background: linear-gradient(135deg, #5770b0, #06486f);
        border: none; border-radius: 10px;
        color: #fff; font-size: .85rem; font-weight: 600;
        cursor: pointer; display: flex; align-items: center; justify-content: center; gap:.4rem;
        transition: opacity .15s, transform .1s;
      }
      .pw-send-btn:disabled { opacity: .4; cursor: not-allowed; transform: none; }
      .pw-send-btn:not(:disabled):hover { opacity: .88; transform: translateY(-1px); }

      /* ── Feedback ───────────────────────────────────────── */
      .pw-feedback { padding: .4rem .75rem; border-radius: 8px; font-size: .8rem; font-weight: 500; }
      .pw-feedback-ok  { background: rgba(0,164,135,.15); color: #2dd4bf; border: 1px solid rgba(0,164,135,.25); }
      .pw-feedback-err { background: rgba(239,68,68,.12);  color: #f87171; border: 1px solid rgba(239,68,68,.2); }

      /* ── Hint ───────────────────────────────────────────── */
      .pw-hint { font-size: .72rem; color: #475569; margin: 0; line-height: 1.4; }
      .pw-hint strong { color: #64748b; }

      /* ── Spinner ─────────────────────────────────────────── */
      .pw-spinner {
        display: inline-block; width: 13px; height: 13px;
        border: 2px solid rgba(255,255,255,.3);
        border-top-color: #fff; border-radius: 50%;
        animation: pw-spin .7s linear infinite;
      }
      @keyframes pw-spin { to { transform: rotate(360deg); } }

      /* ── Historial ──────────────────────────────────────── */
      .pw-history { display: flex; flex-direction: column; gap: .6rem; flex:1; overflow-y:auto; padding-bottom:.5rem; }
      .pw-empty-hist { color: #475569; font-size: .8rem; text-align: center; margin: 1rem 0; }

      .pw-hist-meta {
        display: flex; justify-content: space-between; align-items: center;
        font-size: .72rem; color: #64748b;
        padding: .35rem .5rem;
        background: rgba(87,112,176,.08); border-radius: 8px;
      }
      .pw-hist-status { font-weight: 600; }

      .pw-msg-row { display: flex; }
      .pw-msg-me   { justify-content: flex-end; }
      .pw-msg-them { justify-content: flex-start; }

      .pw-msg-bubble {
        max-width: 82%;
        padding: .5rem .75rem;
        border-radius: 12px;
        line-height: 1.45;
      }
      .pw-msg-me   .pw-msg-bubble { background: linear-gradient(135deg,#5770b0,#06486f); color:#fff; border-bottom-right-radius:3px; }
      .pw-msg-them .pw-msg-bubble { background: rgba(30,41,59,.9); border:1px solid rgba(87,112,176,.2); color:#e2e8f0; border-bottom-left-radius:3px; }

      .pw-msg-text { font-size: .82rem; white-space: pre-wrap; word-break: break-word; }
      .pw-msg-meta { font-size: .65rem; opacity: .65; margin-top: .25rem; }
      .pw-admin-tag {
        display: inline-block; font-size: .58rem; font-weight: 700;
        background: rgba(87,112,176,.3); border: 1px solid rgba(87,112,176,.4);
        color: #8ba4d8; padding: 0 .3em; border-radius: 3px; vertical-align: middle;
      }

      .pw-hist-link {
        text-align: center; font-size: .75rem; padding-top: .25rem;
      }
      .pw-hist-link a { color: #8ba4d8; text-decoration: none; }
      .pw-hist-link a:hover { text-decoration: underline; }

      /* ── Responsive ─────────────────────────────────────── */
      @media (max-width: 480px) {
        .pw-panel { width: calc(100vw - 2rem); right: 1rem; bottom: 68px; }
      }
    `;
    document.head.appendChild(style);
  }

  // ─── Inicializar cuando el DOM esté listo ─────────────────────────────────
  if (document.readyState === 'loading') {
    document.addEventListener('DOMContentLoaded', init);
  } else {
    init();
  }

})();
