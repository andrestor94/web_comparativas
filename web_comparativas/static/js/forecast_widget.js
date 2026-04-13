/**
 * forecast_widget.js
 * Widget flotante de comentarios rápidos para el módulo Forecast.
 *
 * Genera / alimenta tickets en Mesa de Ayuda bajo la categoría "forecast".
 * Reutiliza exactamente el mismo patrón visual e interactivo de pliego_widget.js.
 * No depende de frameworks externos. Solo Bootstrap 5 + Vanilla JS.
 *
 * Uso: incluir el script en el template de Forecast que defina
 * un elemento <div id="forecast-widget-root"> con los data attributes:
 *   data-empresa  – Nombre de la empresa / unidad de negocio (opcional, texto)
 *   data-unidad   – Nombre de la unidad / mercado (opcional, texto)
 */
(function () {
  'use strict';

  // ─── Leer contexto desde el root element ──────────────────────────────────
  const ROOT_ID = 'forecast-widget-root';

  function init() {
    const root = document.getElementById(ROOT_ID);
    if (!root) return;

    const ctx = {
      empresa: root.dataset.empresa || '',
      unidad:  root.dataset.unidad  || '',
    };

    const DRAFT_KEY = 'forecast_widget_draft';

    // ─── Inyectar CSS ────────────────────────────────────────────────────────
    injectStyles();

    // ─── Crear estructura del widget ─────────────────────────────────────────
    const wrapper = document.createElement('div');
    wrapper.id = 'fw-wrapper';
    wrapper.innerHTML = buildHTML(ctx);
    wrapper.setAttribute('style',
      'position:fixed!important;bottom:1.5rem!important;right:1.5rem!important;' +
      'z-index:9999!important;');
    document.body.appendChild(wrapper);

    // ─── Referencias DOM ─────────────────────────────────────────────────────
    const fab       = document.getElementById('fw-fab');
    const panel     = document.getElementById('fw-panel');
    const closeBtn  = document.getElementById('fw-close');
    const minBtn    = document.getElementById('fw-minimize');
    const textarea  = document.getElementById('fw-textarea');
    const sendBtn   = document.getElementById('fw-send');
    const histArea  = document.getElementById('fw-history');
    const badge     = document.getElementById('fw-badge');
    const feedback  = document.getElementById('fw-feedback');
    const tabWrite  = document.getElementById('fw-tab-write');
    const tabHist   = document.getElementById('fw-tab-hist');
    const paneWrite = document.getElementById('fw-pane-write');
    const paneHist  = document.getElementById('fw-pane-hist');

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
      panel.classList.add('fw-open');
      panel.classList.remove('fw-minimized');
      fab.classList.add('fw-fab-active');
      fab.title = 'Cerrar notas';
      textarea.focus();
    }

    function closePanel() {
      isOpen = false;
      panel.classList.remove('fw-open');
      panel.style.display = 'none';
      fab.classList.remove('fw-fab-active');
      fab.title = 'Notas y consultas de Forecast';
    }

    function minimizePanel() {
      isMinimized = true;
      panel.classList.add('fw-minimized');
      minBtn.title = 'Restaurar';
      minBtn.innerHTML = '<i class="bi bi-chevron-up"></i>';
    }

    function restorePanel() {
      isMinimized = false;
      panel.classList.remove('fw-minimized');
      minBtn.title = 'Minimizar';
      minBtn.innerHTML = '<i class="bi bi-dash-lg"></i>';
    }

    function switchTab(name) {
      if (name === 'write') {
        tabWrite.classList.add('fw-tab-active');
        tabHist.classList.remove('fw-tab-active');
        paneWrite.style.display = 'flex';
        paneHist.style.display  = 'none';
      } else {
        tabHist.classList.add('fw-tab-active');
        tabWrite.classList.remove('fw-tab-active');
        paneHist.style.display  = 'flex';
        paneWrite.style.display = 'none';
      }
    }

    function showFeedback(msg, type) {
      feedback.textContent    = msg;
      feedback.className      = `fw-feedback fw-feedback-${type}`;
      feedback.style.display  = 'block';
      setTimeout(() => { feedback.style.display = 'none'; }, 3500);
    }

    // ─── Enviar comentario ────────────────────────────────────────────────────

    function submitComment() {
      const text = textarea.value.trim();
      if (!text) return;

      sendBtn.disabled = true;
      sendBtn.innerHTML = '<span class="fw-spinner"></span>';

      const payload = {
        message: text,
        empresa: ctx.empresa,
        unidad:  ctx.unidad,
      };

      fetch('/forecast/api/comments', {
        method:  'POST',
        headers: { 'Content-Type': 'application/json' },
        body:    JSON.stringify(payload),
      })
        .then((r) => {
          const ct = r.headers.get('content-type') || '';
          if (!ct.includes('application/json')) throw new Error('unexpected_html');
          return r.json();
        })
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
      fetch('/forecast/api/comments/summary')
        .then((r) => {
          // If the server returns HTML (redirect to login or error page),
          // content-type will not be application/json — skip silently.
          const ct = r.headers.get('content-type') || '';
          if (!ct.includes('application/json')) return null;
          return r.json();
        })
        .then((data) => {
          if (!data || !data.ok) return;

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
        histArea.innerHTML = '<p class="fw-empty-hist">Todavía no hay notas en Forecast.</p>';
        return;
      }

      const statusLabel = {
        abierto:   '🟢 Abierto',
        pendiente: '🟡 Pendiente',
        resuelto:  '🔵 Resuelto',
        cerrado:   '⚫ Cerrado',
      }[ticketStatus] || ticketStatus;

      let html = `<div class="fw-hist-meta">
        <span>Consulta #${ticketId}</span>
        <span class="fw-hist-status">${statusLabel}</span>
      </div>`;

      messages.forEach((m) => {
        const side = m.is_me ? 'fw-msg-me' : 'fw-msg-them';
        const label = m.is_admin ? `${m.sender} <span class="fw-admin-tag">ADMIN</span>` : m.sender;
        html += `
          <div class="fw-msg-row ${side}">
            <div class="fw-msg-bubble">
              <div class="fw-msg-text">${escapeHtml(m.message)}</div>
              <div class="fw-msg-meta">${label} · ${m.created_at}</div>
            </div>
          </div>`;
      });

      if (ticketId) {
        html += `<div class="fw-hist-link">
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
    const empresaStr = ctx.empresa
      ? `<span class="fw-ctx-tag"><i class="bi bi-building"></i>${escCtx(ctx.empresa)}</span>`
      : '';
    const unidadStr = ctx.unidad
      ? `<span class="fw-ctx-tag"><i class="bi bi-graph-up"></i>${escCtx(ctx.unidad.substring(0, 40))}${ctx.unidad.length > 40 ? '…' : ''}</span>`
      : '';

    return `
      <!-- FAB flotante -->
      <button id="fw-fab" class="fw-fab"
        style="position:relative;width:52px;height:52px;border-radius:50%;border:none;background:linear-gradient(135deg,#5770b0,#06486f);color:#fff;font-size:1.25rem;cursor:pointer;display:flex;align-items:center;justify-content:center;box-shadow:0 4px 18px rgba(6,72,111,.45);"
        title="Notas y consultas de Forecast" aria-label="Abrir notas de Forecast">
        <i class="bi bi-chat-text-fill fw-fab-icon"></i>
        <span id="fw-badge" class="fw-badge" style="display:none;position:absolute;top:-4px;right:-4px;min-width:18px;height:18px;border-radius:9px;background:#ef4444;color:#fff;font-size:.65rem;font-weight:700;align-items:center;justify-content:center;padding:0 4px;border:2px solid #fff;">0</span>
      </button>

      <!-- Panel flotante -->
      <div id="fw-panel" class="fw-panel"
        style="display:none;position:fixed;bottom:72px;right:1.5rem;width:360px;max-height:540px;border-radius:16px;background:rgba(10,22,38,0.97);border:1px solid rgba(87,112,176,.35);box-shadow:0 16px 48px rgba(0,0,0,.55);backdrop-filter:blur(18px);flex-direction:column;overflow:hidden;color:#e2e8f0;font-family:inherit;font-size:.88rem;z-index:9998;"
        role="dialog" aria-label="Notas de Forecast">

        <!-- Header -->
        <div class="fw-header">
          <div class="fw-header-title">
            <i class="bi bi-chat-text me-1"></i>
            <span>Notas de Forecast</span>
          </div>
          <div class="fw-header-actions">
            <button id="fw-minimize" class="fw-icon-btn" title="Minimizar"><i class="bi bi-dash-lg"></i></button>
            <button id="fw-close"    class="fw-icon-btn" title="Cerrar"><i class="bi bi-x-lg"></i></button>
          </div>
        </div>

        <!-- Contexto del módulo -->
        <div class="fw-ctx-bar">
          ${empresaStr}${unidadStr}
          ${!empresaStr && !unidadStr ? '<span class="fw-ctx-tag"><i class="bi bi-graph-up"></i>Proyecciones de venta</span>' : ''}
        </div>

        <!-- Tabs -->
        <div class="fw-tabs">
          <button id="fw-tab-write" class="fw-tab fw-tab-active">
            <i class="bi bi-pencil"></i> Escribir
          </button>
          <button id="fw-tab-hist" class="fw-tab">
            <i class="bi bi-clock-history"></i> Historial
          </button>
        </div>

        <!-- Pane: Escribir -->
        <div id="fw-pane-write" class="fw-pane" style="display:flex; flex-direction:column; flex:1; gap:.6rem">
          <textarea
            id="fw-textarea"
            class="fw-textarea"
            rows="4"
            placeholder="Escribí tu observación, duda o nota sobre las proyecciones…&#10;(Enter para enviar · Shift+Enter para nueva línea)"
            maxlength="2000"
          ></textarea>
          <div id="fw-feedback" class="fw-feedback" style="display:none"></div>
          <button id="fw-send" class="fw-send-btn" disabled>
            <i class="bi bi-send-fill"></i> Enviar
          </button>
          <p class="fw-hint">
            <i class="bi bi-info-circle"></i>
            Tu nota quedará visible en <strong>Mesa de Ayuda</strong> para que el equipo pueda responderla.
          </p>
        </div>

        <!-- Pane: Historial -->
        <div id="fw-pane-hist" class="fw-pane" style="display:none; flex-direction:column; flex:1">
          <div id="fw-history" class="fw-history">
            <p class="fw-empty-hist">Cargando historial…</p>
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
    if (document.getElementById('fw-styles')) return;
    const style = document.createElement('style');
    style.id = 'fw-styles';
    style.textContent = `
      /* ── FAB ────────────────────────────────────────────── */
      #fw-wrapper { position: fixed !important; bottom: 1.5rem !important; right: 1.5rem !important; z-index: 9999 !important; }

      .fw-fab {
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
      .fw-fab:hover          { transform: scale(1.08); box-shadow: 0 6px 22px rgba(6,72,111,.6); }
      .fw-fab.fw-fab-active  { background: linear-gradient(135deg, #06486f, #5770b0); }

      .fw-badge {
        position: absolute; top: -4px; right: -4px;
        min-width: 18px; height: 18px; border-radius: 9px;
        background: #ef4444; color: #fff;
        font-size: .65rem; font-weight: 700;
        display: flex; align-items: center; justify-content: center;
        padding: 0 4px; border: 2px solid #fff;
        box-shadow: 0 2px 6px rgba(239,68,68,.4);
      }

      /* ── Panel ──────────────────────────────────────────── */
      .fw-panel {
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
        animation: fw-slide-in .22s cubic-bezier(.16,1,.3,1);
        color: #e2e8f0;
        font-family: inherit;
        font-size: .88rem;
      }
      .fw-panel.fw-minimized {
        max-height: 52px;
        overflow: hidden;
      }

      @keyframes fw-slide-in {
        from { opacity:0; transform: translateY(12px) scale(.97); }
        to   { opacity:1; transform: translateY(0)    scale(1); }
      }

      /* ── Header ─────────────────────────────────────────── */
      .fw-header {
        display: flex; align-items: center; justify-content: space-between;
        padding: .75rem 1rem;
        background: rgba(87,112,176,.14);
        border-bottom: 1px solid rgba(87,112,176,.2);
        flex-shrink: 0;
      }
      .fw-header-title { display: flex; align-items: center; gap:.4rem; font-weight: 600; font-size:.88rem; color:#cbd5e1; }
      .fw-header-actions { display: flex; gap:.25rem; }
      .fw-icon-btn {
        background: none; border: none; color: #94a3b8;
        width: 28px; height: 28px; border-radius: 6px;
        display: flex; align-items: center; justify-content: center;
        cursor: pointer; font-size: .85rem; transition: background .15s, color .15s;
      }
      .fw-icon-btn:hover { background: rgba(255,255,255,.08); color: #fff; }

      /* ── Contexto ───────────────────────────────────────── */
      .fw-ctx-bar {
        display: flex; flex-wrap: wrap; gap:.35rem;
        padding: .5rem 1rem;
        border-bottom: 1px solid rgba(87,112,176,.12);
        flex-shrink: 0;
      }
      .fw-ctx-tag {
        display: inline-flex; align-items: center; gap:.3rem;
        font-size: .72rem; font-weight: 500;
        color: #8ba4d8;
        background: rgba(87,112,176,.12);
        border: 1px solid rgba(87,112,176,.2);
        padding: .2em .55em; border-radius: 20px;
      }
      .fw-ctx-tag i { opacity:.75; }

      /* ── Tabs ───────────────────────────────────────────── */
      .fw-tabs {
        display: flex;
        border-bottom: 1px solid rgba(87,112,176,.18);
        flex-shrink: 0;
      }
      .fw-tab {
        flex: 1; background: none; border: none; color: #94a3b8;
        padding: .5rem .75rem; font-size: .78rem; font-weight: 500;
        cursor: pointer; transition: color .15s, border-bottom .15s;
        border-bottom: 2px solid transparent;
        display: flex; align-items: center; justify-content: center; gap:.35rem;
      }
      .fw-tab:hover { color: #cbd5e1; }
      .fw-tab.fw-tab-active { color: #8ba4d8; border-bottom-color: #5770b0; }

      /* ── Panes ──────────────────────────────────────────── */
      .fw-pane { padding: .75rem 1rem; overflow-y: auto; }

      /* ── Textarea ───────────────────────────────────────── */
      .fw-textarea {
        width: 100%; resize: none;
        background: rgba(255,255,255,.05);
        border: 1px solid rgba(87,112,176,.25);
        border-radius: 10px; color: #e2e8f0;
        padding: .65rem .85rem; font-size: .85rem;
        font-family: inherit; line-height: 1.5;
        transition: border-color .15s;
        outline: none;
      }
      .fw-textarea:focus { border-color: rgba(87,112,176,.55); background: rgba(255,255,255,.07); }
      .fw-textarea::placeholder { color: #475569; }

      /* ── Botón enviar ────────────────────────────────────── */
      .fw-send-btn {
        width: 100%; padding: .55rem 1rem;
        background: linear-gradient(135deg, #5770b0, #06486f);
        border: none; border-radius: 10px;
        color: #fff; font-size: .85rem; font-weight: 600;
        cursor: pointer; display: flex; align-items: center; justify-content: center; gap:.4rem;
        transition: opacity .15s, transform .1s;
      }
      .fw-send-btn:disabled { opacity: .4; cursor: not-allowed; transform: none; }
      .fw-send-btn:not(:disabled):hover { opacity: .88; transform: translateY(-1px); }

      /* ── Feedback ───────────────────────────────────────── */
      .fw-feedback { padding: .4rem .75rem; border-radius: 8px; font-size: .8rem; font-weight: 500; }
      .fw-feedback-ok  { background: rgba(0,164,135,.15); color: #2dd4bf; border: 1px solid rgba(0,164,135,.25); }
      .fw-feedback-err { background: rgba(239,68,68,.12);  color: #f87171; border: 1px solid rgba(239,68,68,.2); }

      /* ── Hint ───────────────────────────────────────────── */
      .fw-hint { font-size: .72rem; color: #475569; margin: 0; line-height: 1.4; }
      .fw-hint strong { color: #64748b; }

      /* ── Spinner ─────────────────────────────────────────── */
      .fw-spinner {
        display: inline-block; width: 13px; height: 13px;
        border: 2px solid rgba(255,255,255,.3);
        border-top-color: #fff; border-radius: 50%;
        animation: fw-spin .7s linear infinite;
      }
      @keyframes fw-spin { to { transform: rotate(360deg); } }

      /* ── Historial ──────────────────────────────────────── */
      .fw-history { display: flex; flex-direction: column; gap: .6rem; flex:1; overflow-y:auto; padding-bottom:.5rem; }
      .fw-empty-hist { color: #475569; font-size: .8rem; text-align: center; margin: 1rem 0; }

      .fw-hist-meta {
        display: flex; justify-content: space-between; align-items: center;
        font-size: .72rem; color: #64748b;
        padding: .35rem .5rem;
        background: rgba(87,112,176,.08); border-radius: 8px;
      }
      .fw-hist-status { font-weight: 600; }

      .fw-msg-row { display: flex; }
      .fw-msg-me   { justify-content: flex-end; }
      .fw-msg-them { justify-content: flex-start; }

      .fw-msg-bubble {
        max-width: 82%;
        padding: .5rem .75rem;
        border-radius: 12px;
        line-height: 1.45;
      }
      .fw-msg-me   .fw-msg-bubble { background: linear-gradient(135deg,#5770b0,#06486f); color:#fff; border-bottom-right-radius:3px; }
      .fw-msg-them .fw-msg-bubble { background: rgba(30,41,59,.9); border:1px solid rgba(87,112,176,.2); color:#e2e8f0; border-bottom-left-radius:3px; }

      .fw-msg-text { font-size: .82rem; white-space: pre-wrap; word-break: break-word; }
      .fw-msg-meta { font-size: .65rem; opacity: .65; margin-top: .25rem; }
      .fw-admin-tag {
        display: inline-block; font-size: .58rem; font-weight: 700;
        background: rgba(87,112,176,.3); border: 1px solid rgba(87,112,176,.4);
        color: #8ba4d8; padding: 0 .3em; border-radius: 3px; vertical-align: middle;
      }

      .fw-hist-link {
        text-align: center; font-size: .75rem; padding-top: .25rem;
      }
      .fw-hist-link a { color: #8ba4d8; text-decoration: none; }
      .fw-hist-link a:hover { text-decoration: underline; }

      /* ── Responsive ─────────────────────────────────────── */
      @media (max-width: 480px) {
        .fw-panel { width: calc(100vw - 2rem); right: 1rem; bottom: 68px; }
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
