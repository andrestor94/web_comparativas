/* static/js/notify.js */
(function () {
  const TOAST_ROOT_ID = "toast-root";
  const BANNER_ROOT_ID = "banner-root";

  const TYPES = {
    info:   { bar: "#5274ce" },
    success:{ bar: "#16a34a" },
    warning:{ bar: "#d97706" },
    error:  { bar: "#dc2626" }
  };

  function $(sel, root = document) { return root.querySelector(sel); }
  function el(tag, attrs = {}, children = []) {
    const n = document.createElement(tag);
    Object.entries(attrs).forEach(([k, v]) => {
      if (k === "class") n.className = v;
      else if (k === "style") Object.assign(n.style, v);
      else if (k.startsWith("on") && typeof v === "function") n.addEventListener(k.slice(2), v);
      else n.setAttribute(k, v);
    });
    (Array.isArray(children) ? children : [children]).forEach(ch => {
      if (ch == null) return;
      if (typeof ch === "string") n.appendChild(document.createTextNode(ch));
      else n.appendChild(ch);
    });
    return n;
  }

  function ensureRoots() {
    if (!$( `#${TOAST_ROOT_ID}` )) document.body.appendChild(el("div", { id: TOAST_ROOT_ID }));
    if (!$( `#${BANNER_ROOT_ID}` )) document.body.appendChild(el("div", { id: BANNER_ROOT_ID }));
  }

  function removeNode(node) {
    if (!node) return;
    node.classList.add("sa-toast--out");
    setTimeout(() => node.remove(), 180);
  }

  function toast({ title = "Aviso", message = "", type = "info", timeout = 5000, actionText = null, onAction = null } = {}) {
    ensureRoots();
    const root = $(`#${TOAST_ROOT_ID}`);
    const tcfg = TYPES[type] || TYPES.info;

    const btnClose = el("button", { class: "sa-toast__close", "aria-label": "Cerrar", onclick: () => removeNode(card) }, "×");
    const actionBtn = actionText ? el("button", { class: "sa-toast__action", onclick: () => { if (onAction) onAction(); removeNode(card);} }, actionText) : null;

    const head = el("div", { class: "sa-toast__head" }, [
      el("div", { class: "sa-toast__title" }, title),
      btnClose
    ]);

    const body = el("div", { class: "sa-toast__body" }, message ? el("p", {}, message) : null);
    const footer = actionBtn ? el("div", { class: "sa-toast__footer" }, actionBtn) : null;
    const bar = el("div", { class: "sa-toast__bar", style: { background: tcfg.bar } });

    const card = el("div", { class: `sa-toast sa-toast--${type}`, role: "status", "aria-live": "polite" }, [head, body, footer, bar]);
    root.appendChild(card);

    if (timeout > 0) {
      const start = Date.now();
      const tick = () => {
        const pct = Math.max(0, 1 - (Date.now() - start) / timeout);
        bar.style.transform = `scaleX(${pct})`;
        if (pct <= 0) removeNode(card);
        else requestAnimationFrame(tick);
      };
      requestAnimationFrame(tick);
    }
    return card;
  }

  function banner({ message = "", type = "info", sticky = false } = {}) {
    ensureRoots();
    const root = $(`#${BANNER_ROOT_ID}`);
    const tcfg = TYPES[type] || TYPES.info;

    const close = el("button", { class: "sa-banner__close", "aria-label": "Cerrar", onclick: () => removeNode(node) }, "×");
    const node = el("div", { class: `sa-banner sa-banner--${type}`, role: "status", "aria-live": "polite" }, [
      el("span", { class: "sa-banner__dot", style: { background: tcfg.bar } }),
      el("div", { class: "sa-banner__msg" }, message),
      close
    ]);
    root.appendChild(node);
    if (!sticky) setTimeout(() => removeNode(node), 6000);
    return node;
  }

  window.Notify = { toast, banner };
})();
