// static/js/oportunidades_buscador.js
(function () {
  const $ = (sel, root = document) => root.querySelector(sel);
  const $$ = (sel, root = document) => Array.from(root.querySelectorAll(sel));

  const tbody = $("#oppTBody");
  if (!tbody) return;

  // ---- UI refs
  const selPlataforma = $("#fPlataforma");
  const selOperador = $("#fOperador");
  const selCuenta = $("#fCuenta");
  const selReparticion = $("#fReparticion");
  const inpBuscar = $("#fBuscar");
  const btnClear = $("#fClear");
  const btnAplicar = $("#btnAplicar");
  const btnLimpiar = $("#btnLimpiar");
  const btnExport = $("#btnExport");
  const btnDownloadCurrent = $("#btnDownloadCurrent");

  const dateFrom = $("#fDateFrom");
  const dateTo = $("#fDateTo");
  const rMin = $("#fRangeMin");
  const rMax = $("#fRangeMax");
  const fill = $("#dateFill");
  const lblFrom = $("#lblFrom");
  const lblTo = $("#lblTo");

  const swPAMI = $("#swPAMI");
  const swEstado = $("#swEstado");

  const kProcesos = $("#kProcesos");

  const showFrom = $("#showFrom");
  const showTo = $("#showTo");
  const totalFound = $("#totalFound");
  const prevBtn = $("#prevPage");
  const nextBtn = $("#nextPage");
  const curPage = $("#curPage");
  const maxPage = $("#maxPage");

  // Tamaño de página dinámico (se puede actualizar)
  let PAGE_SIZE = (window.OPP_UI && window.OPP_UI.pageSize) || 20;

  // Upload UI (spinner, drag & drop)
  const formUpload = (function () {
    const f = $("#oppFile") ? $("#oppFile").closest("form") : null;
    return f || null;
  })();
  const drop = $("#oppDrop");
  const fileInput = $("#oppFile");
  const btnUpload = $("#btnUpload");
  const upLoading = $("#upLoading");

  // ---- Helpers
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

  function parseApertura(str) {
    if (!str) return null;
    const s = String(str).trim();
    const iso = Date.parse(s);
    if (!Number.isNaN(iso)) return new Date(iso);

    const [fecha, hora] = s.split(/\s+/);
    if (!fecha) return null;
    const sep = fecha.includes("/") ? "/" : fecha.includes("-") ? "-" : null;
    if (!sep) return null;
    const p = fecha.split(sep).map((x) => x.trim());
    if (p.length !== 3) return null;

    let dd, mm, yyyy;
    if (p[0].length === 4) {
      yyyy = +p[0];
      mm = +p[1];
      dd = +p[2];
    } else {
      dd = +p[0];
      mm = +p[1];
      yyyy = +p[2];
    }
    let HH = 0,
      MM = 0;
    if (hora) {
      const h = hora.split(":");
      if (h.length >= 2) {
        HH = +h[0] || 0;
        MM = +h[1] || 0;
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
    if (numeric) arr.sort((a, b) => Number(a) - Number(b));
    else arr.sort((a, b) => normalize(a).localeCompare(normalize(b)));
    return arr;
  }

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

  function escapeRegex(str) {
    return str.replace(/[.*+?^${}()|[\]\\]/g, "\\$&");
  }

  function highlightPlain(text, term) {
    if (!term) return safeText(text);
    const escText = escapeHtml(text || "").replace(/\n/g, "<br>");
    const pat = escapeRegex(term.trim());
    if (!pat) return escText;
    const re = new RegExp(pat, "ig");
    return escText.replace(re, (m) => `<mark class="hl">${m}</mark>`);
  }

  // ============================================================
  //  DATASET PRINCIPAL
  // ============================================================
  const originalRows = $$("#oppTBody tr[data-numero]");
  const backendData =
    window.OPP_DATA && Array.isArray(window.OPP_DATA) && window.OPP_DATA.length
      ? window.OPP_DATA
      : null;

  const DATA = backendData
    ? backendData.map((r) => {
        const aperturaTxt =
          r.apertura_txt || r.apertura || r.Apertura || r.aperturaTxt || "";
        const aperturaDate = parseApertura(aperturaTxt);
        const estadoRaw = r.estado || r.Estado || "";
        const estNorm = normalize(estadoRaw);
        const estadoNorm = estNorm.includes("emerg") ? "emergencia" : "regular";

        return {
          numero: r.numero || r["Número"] || "",
          reparticion: r.reparticion || r["Repartición"] || "",
          objeto: r.objeto || r["Objeto"] || "",
          aperturaTxt,
          apertura: aperturaDate ? aperturaDate.getTime() : null,
          tipo: r.tipo || r["Tipo"] || "",
          plataforma: r.plataforma || r["Plataforma"] || "",
          operador: r.operador || r["Operador"] || "",
          // Filtro Cuenta: usamos la columna "Número"
          cuenta: r.cuenta || r["Número"] || "",
          // Procesos: usamos la columna "N° UAPE"
          uape:
            r.uape ||
            r["N° UAPE"] ||
            r["Unidad Compra"] ||
            r["Cod. UAPE"] ||
            "",
          estado: estadoNorm,
          enlace: r.enlace || r["Enlace de pliego"] || "",
        };
      })
    : originalRows.map((tr) => {
        const tds = tr.children;
        const td = (i) => (tds[i] ? tds[i].textContent.trim() : "");

        const numero = tr.dataset.numero || td(0);
        const reparticion = tr.dataset.reparticion || td(1);
        const objeto = tr.dataset.objeto || td(2);
        const aperturaTxt = tr.dataset.apertura || td(3);
        const tipo = tr.dataset.tipo || td(4);
        const plataforma = tr.dataset.plataforma || "";
        const operador = tr.dataset.operador || "";
        const cuenta = tr.dataset.cuenta || "";
        const uape = tr.dataset.uape || "";
        const estadoRaw = tr.dataset.estado || "";
        const enlaceEl = tr.querySelector("td:nth-child(6) a");
        const enlace = enlaceEl ? enlaceEl.getAttribute("href") : "";

        const aperturaDate = parseApertura(aperturaTxt);
        const estNorm = normalize(estadoRaw);
        const estadoNorm = estNorm.includes("emerg") ? "emergencia" : "regular";

        return {
          numero,
          reparticion,
          objeto,
          aperturaTxt,
          apertura: aperturaDate ? aperturaDate.getTime() : null,
          tipo,
          plataforma,
          operador,
          cuenta,
          uape,
          estado: estadoNorm,
          enlace,
        };
      });

  // Dominio de fechas
  const dates = DATA.map((d) => d.apertura).filter((v) => v !== null);
  const DOMAIN_MIN = dates.length ? Math.min(...dates) : null;
  const DOMAIN_MAX = dates.length ? Math.max(...dates) : null;

  // ---- Selects
  function initSelect(selectEl, values) {
    if (!selectEl) return;
    const first = selectEl.firstElementChild;
    selectEl.innerHTML = "";
    if (first) selectEl.appendChild(first.cloneNode(true));
    const frag = document.createDocumentFragment();
    for (const val of values) {
      const opt = document.createElement("option");
      opt.value = val;
      opt.textContent = val;
      frag.appendChild(opt);
    }
    selectEl.appendChild(frag);
  }

  initSelect(selPlataforma, uniqSorted(DATA.map((d) => d.plataforma)));
  initSelect(selOperador, uniqSorted(DATA.map((d) => d.operador)));
  initSelect(selCuenta, uniqSorted(DATA.map((d) => d.cuenta)));
  initSelect(selReparticion, uniqSorted(DATA.map((d) => d.reparticion)));

  // ---- Slider & fechas
  function percentFromEpoch(ms) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) return 0;
    if (DOMAIN_MAX === DOMAIN_MIN) return 100;
    return ((ms - DOMAIN_MIN) / (DOMAIN_MAX - DOMAIN_MIN)) * 100;
  }
  function epochFromPercent(p) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) return null;
    const clamped = Math.max(0, Math.min(100, p));
    return Math.round(
      DOMAIN_MIN + (DOMAIN_MAX - DOMAIN_MIN) * (clamped / 100)
    );
  }

  const initFrom =
    (window.OPP_UI &&
      window.OPP_UI.initialDateFrom &&
      parseApertura(window.OPP_UI.initialDateFrom)) ||
    (DOMAIN_MIN ? new Date(DOMAIN_MIN) : null);
  const initTo =
    (window.OPP_UI &&
      window.OPP_UI.initialDateTo &&
      parseApertura(window.OPP_UI.initialDateTo)) ||
    (DOMAIN_MAX ? new Date(DOMAIN_MAX) : null);

  function setSliderFromDates(d1, d2) {
    if (DOMAIN_MIN === null || DOMAIN_MAX === null) {
      if (rMin) rMin.value = 0;
      if (rMax) rMax.value = 100;
      updateFill();
      return;
    }
    const p1 = percentFromEpoch(d1 ? d1.getTime() : DOMAIN_MIN);
    const p2 = percentFromEpoch(d2 ? d2.getTime() : DOMAIN_MAX);
    if (rMin) rMin.value = Math.floor(Math.min(p1, p2));
    if (rMax) rMax.value = Math.floor(Math.max(p1, p2));
    updateFill();
  }
  function setDatesFromSlider() {
    if (!rMin || !rMax) return;
    const v1 = Number(rMin.value),
      v2 = Number(rMax.value);
    const e1 = epochFromPercent(Math.min(v1, v2));
    const e2 = epochFromPercent(Math.max(v1, v2));
    const d1 = e1 ? new Date(e1) : null,
      d2 = e2 ? new Date(e2) : null;
    if (dateFrom) dateFrom.value = d1 ? toISODate(d1) : "";
    if (dateTo) dateTo.value = d2 ? toISODate(d2) : "";
    if (lblFrom) lblFrom.textContent = d1 ? toDDMMYYYY(d1) : "—";
    if (lblTo) lblTo.textContent = d2 ? toDDMMYYYY(d2) : "—";
  }
  function updateFill() {
    if (!rMin || !rMax || !fill) {
      setDatesFromSlider();
      return;
    }
    const a = Number(rMin.value),
      b = Number(rMax.value);
    const left = Math.min(a, b),
      right = Math.max(a, b);
    fill.style.left = left + "%";
    fill.style.width = Math.max(0, right - left) + "%";
    setDatesFromSlider();
  }

  setSliderFromDates(initFrom, initTo);
  if (dateFrom && initFrom) dateFrom.value = toISODate(initFrom);
  if (dateTo && initTo) dateTo.value = toISODate(initTo);
  setDatesFromSlider();
  if (rMin) rMin.addEventListener("input", updateFill);
  if (rMax) rMax.addEventListener("input", updateFill);
  function syncSliderWithDates() {
    const d1 = dateFrom && dateFrom.value ? new Date(dateFrom.value) : initFrom;
    const d2 = dateTo && dateTo.value ? new Date(dateTo.value) : initTo;
    setSliderFromDates(d1, d2);
    setDatesFromSlider();
  }
  if (dateFrom) dateFrom.addEventListener("change", syncSliderWithDates);
  if (dateTo) dateTo.addEventListener("change", syncSliderWithDates);

  // ---- Switches (PAMI/Otras y Estado)
  function setChipGroup(groupEl, value) {
    if (!groupEl) return;
    $$(".chip", groupEl).forEach((b) =>
      b.classList.toggle("is-on", b.dataset.val === value)
    );
  }
  function getChipGroup(groupEl) {
    const on = $$(".chip.is-on", groupEl)[0];
    return on ? on.dataset.val : "todos";
  }
  if (swPAMI) {
    swPAMI.addEventListener("click", (e) => {
      const btn = e.target.closest(".chip");
      if (!btn || btn.disabled) return;
      setChipGroup(swPAMI, btn.dataset.val);
      applyFilters();
    });
  }
  if (swEstado) {
    swEstado.addEventListener("click", (e) => {
      const btn = e.target.closest(".chip");
      if (!btn || btn.disabled) return;
      setChipGroup(swEstado, btn.dataset.val);
      applyFilters();
    });
  }

  // ---- Filtrado + render
  let CUR_PAGE = 1;
  let FILTERED = DATA;
  let LAST_QUERY = "";

  // Detectar si una repartición es PAMI
  function isPAMIName(rep) {
    const r = normalize(rep);
    if (!r) return false;

    // 1) Si literalmente aparece "pami"
    if (r.includes("pami")) return true;

    // 2) Nombre largo oficial (sin acentos, en minúscula)
    const base =
      "instituto nacional de servicios sociales para jubilados y pensionados";
    if (r.includes(base)) return true;

    return false;
  }

  function applyFilters() {
    const rawQ = inpBuscar && inpBuscar.value ? inpBuscar.value.trim() : "";
    const q = normalize(rawQ);
    LAST_QUERY = rawQ;

    const vPlat = selPlataforma && selPlataforma.value ? selPlataforma.value : "";
    const vOper = selOperador && selOperador.value ? selOperador.value : "";
    const vCta = selCuenta && selCuenta.value ? selCuenta.value : "";
    const vRep =
      selReparticion && selReparticion.value ? selReparticion.value : "";

    const vGrp = getChipGroup(swPAMI); // todos | pami | otras
    const vEst = getChipGroup(swEstado); // todos | emergencia | regular

    // Fechas
    let dFrom = null,
      dTo = null;
    if (dateFrom && dateFrom.value) dFrom = new Date(dateFrom.value);
    if (dateTo && dateTo.value) dTo = new Date(dateTo.value);
    const fromMs = dFrom
      ? new Date(
          dFrom.getFullYear(),
          dFrom.getMonth(),
          dFrom.getDate(),
          0,
          0,
          0,
          0
        ).getTime()
      : null;
    const toMs = dTo
      ? new Date(
          dTo.getFullYear(),
          dTo.getMonth(),
          dTo.getDate(),
          23,
          59,
          59,
          999
        ).getTime()
      : null;

    FILTERED = DATA.filter((r) => {
      if (q && !normalize(r.objeto).includes(q)) return false;
      if (vPlat && r.plataforma !== vPlat) return false;
      if (vOper && r.operador !== vOper) return false;
      if (vCta && r.cuenta !== vCta) return false;
      if (vRep && r.reparticion !== vRep) return false;

      // Switch PAMI/Otras
      if (vGrp === "pami" && !isPAMIName(r.reparticion)) return false;
      if (vGrp === "otras" && isPAMIName(r.reparticion)) return false;

      // Switch Estado
      if (vEst !== "todos" && r.estado !== vEst) return false;

      // Rango fechas
      if (fromMs !== null || toMs !== null) {
        if (r.apertura === null) return false;
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
          ? `<a class="link-ico" href="${escapeHtml(
              r.enlace
            )}" target="_blank" rel="noopener" title="Abrir pliego en nueva pestaña">🔗</a>`
          : `<span class="muted">—</span>`;

        // IMPORTANTE: agregamos title en cada <td> para la burbuja
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
          data-uape="${escapeHtml(r.uape || "")}"
          data-estado="${escapeHtml(r.estado)}"
        >
          <td title="${escapeHtml(r.numero)}">${safeText(r.numero)}</td>
          <td title="${escapeHtml(r.reparticion)}">${safeText(
          r.reparticion
        )}</td>
          <td title="${escapeHtml(r.objeto)}">${highlightPlain(
          r.objeto,
          LAST_QUERY
        )}</td>
          <td title="${escapeHtml(r.aperturaTxt)}">${safeText(
          r.aperturaTxt
        )}</td>
          <td title="${escapeHtml(r.tipo)}">${safeText(r.tipo)}</td>
          <td>${linkHtml}</td>
        </tr>`;
      })
      .join("");

    tbody.innerHTML =
      rowsHtml ||
      `<tr><td class="muted" colspan="6">Sin resultados para los filtros actuales.</td></tr>`;

    // Paginación
    if (totalFound) totalFound.textContent = String(total);
    if (showFrom) showFrom.textContent = String(total ? start + 1 : 0);
    if (showTo) showTo.textContent = String(end);
    if (curPage) curPage.textContent = String(CUR_PAGE);
    if (maxPage) maxPage.textContent = String(pages);
    if (prevBtn) prevBtn.disabled = CUR_PAGE <= 1 || !total;
    if (nextBtn) nextBtn.disabled = CUR_PAGE >= pages || !total;

    // KPI: Procesos (N° UAPE únicos del conjunto filtrado)
    if (kProcesos) {
      const uniq = new Set(
        FILTERED.map((r) => r.uape || r.cuenta).filter(Boolean)
      );
      kProcesos.textContent = String(uniq.size);
    }
  }

  // === Manejo dinámico del tamaño de página ====================
  // API para que la use el HTML (window.OPP.refreshPageSize)
  window.OPP = window.OPP || {};
  window.OPP.refreshPageSize = function (newSize) {
    const v = parseInt(newSize, 10) || 20;
    PAGE_SIZE = v;
    CUR_PAGE = 1;
    render();
  };

  // Fallback: escuchar el evento personalizado si el HTML lo usa
  document.addEventListener("opp:pageSizeChanged", function (ev) {
    if (ev && ev.detail && ev.detail.pageSize) {
      window.OPP.refreshPageSize(ev.detail.pageSize);
    }
  });
  // =============================================================

  // ---- Export CSV
  function exportCSV() {
    if (!FILTERED || !FILTERED.length) {
      return;
    }
    const headers = [
      "Número",
      "Repartición",
      "Objeto",
      "Apertura",
      "Tipo",
      "Plataforma",
      "Operador",
      "N° UAPE",
      "Estado",
      "Enlace de pliego",
    ];
    const lines = [];
    lines.push(headers.join(";"));

    for (const r of FILTERED) {
      const row = [
        r.numero,
        r.reparticion,
        r.objeto,
        r.aperturaTxt,
        r.tipo,
        r.plataforma,
        r.operador,
        r.uape || r.cuenta, // N° UAPE
        r.estado,
        r.enlace,
      ].map((v) => {
        const s = String(v == null ? "" : v);
        return `"${s.replace(/"/g, '""')}"`;
      });
      lines.push(row.join(";"));
    }

    const blob = new Blob([lines.join("\n")], {
      type: "text/csv;charset=utf-8;",
    });
    const url = URL.createObjectURL(blob);
    const a = document.createElement("a");
    a.href = url;
    const today = new Date().toISOString().slice(0, 10);
    a.download = `oportunidades_${today}.csv`;
    document.body.appendChild(a);
    a.click();
    document.body.removeChild(a);
    URL.revokeObjectURL(url);
  }

  // ---- Eventos
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
      setChipGroup(swPAMI, "todos");
      setChipGroup(swEstado, "todos");
      setSliderFromDates(initFrom, initTo);
      setDatesFromSlider();
      applyFilters();
    });

  btnClear &&
    btnClear.addEventListener("click", (e) => {
      e.preventDefault();
      if (inpBuscar) {
        inpBuscar.value = "";
        applyFilters();
      }
    });

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

  btnExport && btnExport.addEventListener("click", exportCSV);

  // Descargar maestro actual
  btnDownloadCurrent &&
    btnDownloadCurrent.addEventListener("click", () => {
      window.location.href = "/oportunidades/buscador/download";
    });

  // Upload: mostrar spinner al enviar
  if (formUpload && upLoading && btnUpload) {
    formUpload.addEventListener("submit", () => {
      btnUpload.disabled = true;
      upLoading.classList.add("show");
    });
  }

  // Drag & drop sobre la dropzone
  if (drop && fileInput) {
    ["dragenter", "dragover"].forEach((ev) => {
      drop.addEventListener(ev, (e) => {
        e.preventDefault();
        e.stopPropagation();
        drop.classList.add("is-drag");
      });
    });
    ["dragleave", "dragend"].forEach((ev) => {
      drop.addEventListener(ev, (e) => {
        e.preventDefault();
        e.stopPropagation();
        drop.classList.remove("is-drag");
      });
    });
    drop.addEventListener("drop", (e) => {
      e.preventDefault();
      e.stopPropagation();
      drop.classList.remove("is-drag");
      const dt = e.dataTransfer;
      if (dt && dt.files && dt.files.length) {
        try {
          fileInput.files = dt.files;
        } catch (err) {
          // fallback, el usuario puede elegir manualmente
        }
      }
    });
  }

  // Render inicial
  applyFilters();
})();
