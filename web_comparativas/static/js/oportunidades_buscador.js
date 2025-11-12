// static/js/oportunidades_buscador.js
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const tbody = $("#oppTBody");
  if (!tbody) return;

  // UI refs
  const selPlataforma = $("#fPlataforma");
  const selOperador = $("#fOperador");
  const selCuenta = $("#fCuenta");
  const selReparticion = $("#fReparticion");
  const inpBuscar = $("#fBuscar");
  const btnClear = $("#fClear");
  const btnAplicar = $("#btnAplicar");
  const btnLimpiar = $("#btnLimpiar");

  const dateFrom = $("#fDateFrom");
  const dateTo = $("#fDateTo");
  const rMin = $("#fRangeMin");
  const rMax = $("#fRangeMax");
  const fill = $("#dateFill");
  const lblFrom = $("#lblFrom");
  const lblTo = $("#lblTo");

  const showFrom = $("#showFrom");
  const showTo = $("#showTo");
  const totalFound = $("#totalFound");
  const prevBtn = $("#prevPage");
  const nextBtn = $("#nextPage");
  const curPage = $("#curPage");
  const maxPage = $("#maxPage");

  const PAGE_SIZE = (window.OPP_UI && window.OPP_UI.pageSize) || 20;

  // ---------------------- Helpers ----------------------
  const normalize = (s) =>
    (s || "")
      .toString()
      .normalize("NFD")
      .replace(/[\u0300-\u036f]/g, "")
      .toLowerCase()
      .trim();

  const pad = (n) => (n < 10 ? "0" + n : "" + n);

  const toISODate = (d) =>
    d ? `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}` : "";

  const toDDMMYYYY = (d) =>
    d ? `${pad(d.getDate())}/${pad(d.getMonth() + 1)}/${d.getFullYear()}` : "—";

  // Parsea fechas en formatos: "DD/MM/YYYY HH:MM", "YYYY-MM-DD HH:MM", "DD-MM-YYYY", etc.
  function parseApertura(str) {
    if (!str) return null;
    const s = String(str).trim();

    // Intenta ISO directo
    const iso = Date.parse(s);
    if (!Number.isNaN(iso)) return new Date(iso);

    // Separa fecha y hora
    const [fecha, hora] = s.split(/\s+/);
    if (!fecha) return null;

    const sep = fecha.includes("/") ? "/" : fecha.includes("-") ? "-" : null;
    if (!sep) return null;

    const parts = fecha.split(sep).map((x) => x.trim());
    if (parts.length !== 3) return null;

    let dd, mm, yyyy;
    // Detecta si arranca con año o con día
    if (parts[0].length === 4) {
      // YYYY-MM-DD
      yyyy = +parts[0];
      mm = +parts[1];
      dd = +parts[2];
    } else {
      // DD/MM/YYYY
      dd = +parts[0];
      mm = +parts[1];
      yyyy = +parts[2];
    }
    if (!yyyy || !mm || !dd) return null;

    let HH = 0,
      MM = 0;
    if (hora) {
      const hparts = hora.split(":");
      if (hparts.length >= 2) {
        HH = +hparts[0] || 0;
        MM = +hparts[1] || 0;
      }
    }
    const d = new Date(yyyy, mm - 1, dd, HH, MM, 0, 0);
    return Number.isNaN(d.getTime()) ? null : d;
  }

  function uniqSorted(list, numeric = false) {
    const set = new Set();
    for (const v of list) {
      if (v === null || v === undefined) continue;
      const s = String(v).trim();
      if (s !== "") set.add(s);
    }
    const arr = Array.from(set);
    if (numeric) {
      arr.sort((a, b) => Number(a) - Number(b));
    } else {
      arr.sort((a, b) => normalize(a).localeCompare(normalize(b)));
    }
    return arr;
  }

  // ---------------------- Dataset desde DOM ----------------------
  const originalRows = $$("#oppTBody tr");
  const DATA = originalRows.map((tr) => {
    const linkEl = tr.querySelector("td:nth-child(6) a");
    const aperturaTxt = tr.dataset.apertura || "";
    const aperturaDate = parseApertura(aperturaTxt);

    return {
      numero: tr.dataset.numero || "",
      reparticion: tr.dataset.reparticion || "",
      objeto: tr.dataset.objeto || "",
      aperturaTxt,
      apertura: aperturaDate ? aperturaDate.getTime() : null, // ms epoch
      tipo: tr.dataset.tipo || "",
      plataforma: tr.dataset.plataforma || "",
      operador: tr.dataset.operador || "",
      cuenta: tr.dataset.cuenta || "",
      enlace: linkEl ? linkEl.getAttribute("href") : "",
    };
  });

  // Dominio de fechas
  const dates = DATA.map((d) => d.apertura).filter((v) => v !== null);
  const DOMAIN_MIN = dates.length ? Math.min(...dates) : null;
  const DOMAIN_MAX = dates.length ? Math.max(...dates) : null;

  // ---------------------- Inicializar selects ----------------------
  function initSelect(selectEl, values, placeholder) {
    if (!selectEl) return;
    // Mantiene primer option ("Todas/Todos")
    const first = selectEl.firstElementChild;
    selectEl.innerHTML = "";
    if (first) selectEl.appendChild(first);

    const frag = document.createDocumentFragment();
    for (const val of values) {
      const opt = document.createElement("option");
      opt.value = val;
      opt.textContent = val;
      frag.appendChild(opt);
    }
    selectEl.appendChild(frag);
  }

  initSelect(
    selPlataforma,
    uniqSorted(DATA.map((d) => d.plataforma)),
    "Todas"
  );
  initSelect(selOperador, uniqSorted(DATA.map((d) => d.operador)), "Todos");
  initSelect(
    selCuenta,
    uniqSorted(DATA.map((d) => d.cuenta), true /* numeric-ish */),
    "Todas"
  );
  initSelect(
    selReparticion,
    uniqSorted(DATA.map((d) => d.reparticion)),
    "Todas"
  );

  // ---------------------- Slider & fechas ----------------------
  function percentFromEpoch(ms) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) return 0;
    if (DOMAIN_MAX === DOMAIN_MIN) return 100;
    return ((ms - DOMAIN_MIN) / (DOMAIN_MAX - DOMAIN_MIN)) * 100;
  }
  function epochFromPercent(p) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) return null;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(DOMAIN_MIN + (DOMAIN_MAX - DOMAIN_MIN) * (clamped / 100));
  }

  // Valores iniciales
  const initFrom =
    (window.OPP_UI && window.OPP_UI.initialDateFrom && parseApertura(window.OPP_UI.initialDateFrom)) ||
    (DOMAIN_MIN ? new Date(DOMAIN_MIN) : null);
  const initTo =
    (window.OPP_UI && window.OPP_UI.initialDateTo && parseApertura(window.OPP_UI.initialDateTo)) ||
    (DOMAIN_MAX ? new Date(DOMAIN_MAX) : null);

  function setSliderFromDates(d1, d2) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) {
      rMin.value = 0;
      rMax.value = 100;
      updateFill();
      return;
    }
    const p1 = percentFromEpoch(d1 ? d1.getTime() : DOMAIN_MIN);
    const p2 = percentFromEpoch(d2 ? d2.getTime() : DOMAIN_MAX);
    rMin.value = Math.max(0, Math.min(100, Math.floor(Math.min(p1, p2))));
    rMax.value = Math.max(0, Math.min(100, Math.floor(Math.max(p1, p2))));
    updateFill();
  }

  function setDatesFromSlider() {
    const v1 = Number(rMin.value);
    const v2 = Number(rMax.value);
    const e1 = epochFromPercent(Math.min(v1, v2));
    const e2 = epochFromPercent(Math.max(v1, v2));
    const d1 = e1 ? new Date(e1) : null;
    const d2 = e2 ? new Date(e2) : null;
    if (dateFrom) dateFrom.value = d1 ? toISODate(d1) : "";
    if (dateTo) dateTo.value = d2 ? toISODate(d2) : "";
    if (lblFrom) lblFrom.textContent = d1 ? toDDMMYYYY(d1) : "—";
    if (lblTo) lblTo.textContent = d2 ? toDDMMYYYY(d2) : "—";
  }

  function updateFill() {
    const a = Number(rMin.value);
    const b = Number(rMax.value);
    const left = Math.min(a, b);
    const right = Math.max(a, b);
    if (fill) {
      fill.style.left = left + "%";
      fill.style.width = Math.max(0, right - left) + "%";
    }
    setDatesFromSlider();
  }

  // Inicializar rango y fechas visibles
  setSliderFromDates(initFrom, initTo);

  // Si hay inputs date, sincronizar con slider
  if (dateFrom && dateTo) {
    if (initFrom) dateFrom.value = toISODate(initFrom);
    if (initTo) dateTo.value = toISODate(initTo);
  }
  setDatesFromSlider();

  rMin.addEventListener("input", updateFill);
  rMax.addEventListener("input", updateFill);

  function syncSliderWithDates() {
    const d1 = dateFrom && dateFrom.value ? new Date(dateFrom.value) : initFrom;
    const d2 = dateTo && dateTo.value ? new Date(dateTo.value) : initTo;
    setSliderFromDates(d1, d2);
    setDatesFromSlider();
  }
  if (dateFrom) dateFrom.addEventListener("change", syncSliderWithDates);
  if (dateTo) dateTo.addEventListener("change", syncSliderWithDates);

  // ---------------------- Filtrado + render ----------------------
  let CUR_PAGE = 1;
  let FILTERED = DATA;

  function applyFilters() {
    const q = normalize(inpBuscar && inpBuscar.value);

    const vPlat = selPlataforma && selPlataforma.value ? selPlataforma.value : "";
    const vOper = selOperador && selOperador.value ? selOperador.value : "";
    const vCta = selCuenta && selCuenta.value ? selCuenta.value : "";
    const vRep = selReparticion && selReparticion.value ? selReparticion.value : "";

    // Rango por fechas
    let dFrom = null,
      dTo = null;
    if (dateFrom && dateFrom.value) dFrom = new Date(dateFrom.value);
    if (dateTo && dateTo.value) dTo = new Date(dateTo.value);

    const fromMs = dFrom ? new Date(dFrom.getFullYear(), dFrom.getMonth(), dFrom.getDate(), 0, 0, 0, 0).getTime() : null;
    const toMs = dTo ? new Date(dTo.getFullYear(), dTo.getMonth(), dTo.getDate(), 23, 59, 59, 999).getTime() : null;

    FILTERED = DATA.filter((r) => {
      // Texto en Objeto (accent-insensitive)
      if (q) {
        if (!normalize(r.objeto).includes(q)) return false;
      }
      // Selects exactos (si hay valor)
      if (vPlat && r.plataforma !== vPlat) return false;
      if (vOper && r.operador !== vOper) return false;
      if (vCta && r.cuenta !== vCta) return false;
      if (vRep && r.reparticion !== vRep) return false;

      // Rango de fechas (si hay una o ambas puntas)
      if (fromMs !== null || toMs !== null) {
        if (r.apertura === null) return false; // si no tiene fecha, lo excluimos cuando se filtra por fecha
        if (fromMs !== null && r.apertura < fromMs) return false;
        if (toMs !== null && r.apertura > toMs) return false;
      }
      return true;
    });

    CUR_PAGE = 1;
    render();
  }

  function render() {
    const total = FILTERED.length;
    const pages = Math.max(1, Math.ceil(total / PAGE_SIZE));
    if (CUR_PAGE > pages) CUR_PAGE = pages;

    const start = (CUR_PAGE - 1) * PAGE_SIZE;
    const end = Math.min(total, start + PAGE_SIZE);
    const slice = FILTERED.slice(start, end);

    const rowsHtml = slice
      .map((r) => {
        const linkHtml = r.enlace
          ? `<a class="link-ico" href="${r.enlace}" target="_blank" rel="noopener">🔗</a>`
          : `<span class="muted">—</span>`;
        return `
          <tr
            data-numero="${escapeHtml(r.numero)}"
            data-reparticion="${escapeHtml(r.reparticion)}"
            data-objeto="${escapeHtml(r.objeto)}"
            data-apertura="${escapeHtml(r.aperturaTxt)}"
            data-tipo="${escapeHtml(r.tipo)}"
            data-plataforma="${escapeHtml(r.plataforma)}"
            data-operador="${escapeHtml(r.operador)}"
            data-cuenta="${escapeHtml(r.cuenta)}"
          >
            <td>${safeText(r.numero)}</td>
            <td>${safeText(r.reparticion)}</td>
            <td>${safeText(r.objeto)}</td>
            <td>${safeText(r.aperturaTxt)}</td>
            <td>${safeText(r.tipo)}</td>
            <td>${linkHtml}</td>
          </tr>
        `;
      })
      .join("");

    tbody.innerHTML = rowsHtml || `<tr><td class="muted" colspan="6">Sin resultados para los filtros actuales.</td></tr>`;

    // Estado/paginación
    if (totalFound) totalFound.textContent = String(total);
    if (showFrom) showFrom.textContent = String(total ? start + 1 : 0);
    if (showTo) showTo.textContent = String(end);
    if (curPage) curPage.textContent = String(CUR_PAGE);
    if (maxPage) maxPage.textContent = String(pages);

    prevBtn.disabled = CUR_PAGE <= 1;
    nextBtn.disabled = CUR_PAGE >= pages;
  }

  // Escapes simples para HTML
  function escapeHtml(s) {
    return String(s || "")
      .replace(/&/g, "&amp;")
      .replace(/</g, "&lt;")
      .replace(/>/g, "&gt;")
      .replace(/"/g, "&quot;");
  }
  function safeText(s) {
    return escapeHtml(s).replace(/\n/g, "<br>");
  }

  // ---------------------- Eventos ----------------------
  btnAplicar && btnAplicar.addEventListener("click", applyFilters);
  btnLimpiar &&
    btnLimpiar.addEventListener("click", () => {
      if (selPlataforma) selPlataforma.value = "";
      if (selOperador) selOperador.value = "";
      if (selCuenta) selCuenta.value = "";
      if (selReparticion) selReparticion.value = "";
      if (inpBuscar) inpBuscar.value = "";
      if (dateFrom) dateFrom.value = initFrom ? toISODate(initFrom) : "";
      if (dateTo) dateTo.value = initTo ? toISODate(initTo) : "";
      setSliderFromDates(initFrom, initTo);
      setDatesFromSlider();
      applyFilters();
    });

  btnClear &&
    btnClear.addEventListener("click", (e) => {
      e.preventDefault();
      if (inpBuscar) inpBuscar.value = "";
    });

  // Enter en el buscador aplica
  inpBuscar &&
    inpBuscar.addEventListener("keydown", (e) => {
      if (e.key === "Enter") {
        e.preventDefault();
        applyFilters();
      }
    });

  prevBtn &&
    prevBtn.addEventListener("click", () => {
      if (CUR_PAGE > 1) {
        CUR_PAGE--;
        render();
      }
    });
  nextBtn &&
    nextBtn.addEventListener("click", () => {
      const pages = Math.max(1, Math.ceil(FILTERED.length / PAGE_SIZE));
      if (CUR_PAGE < pages) {
        CUR_PAGE++;
        render();
      }
    });

  // Render inicial
  applyFilters();
})();
