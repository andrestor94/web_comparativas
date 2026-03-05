/**
 * forecast.js — Phase 4: Hierarchical Table + Grouped Modal + Lab Highlight
 */
document.addEventListener('DOMContentLoaded', () => {
    // --- Elements: Sidebar ---
    const paramInicio = document.getElementById('fc-start-date');
    const paramFin = document.getElementById('fc-end-date');
    const labSelect = document.getElementById('fc-laboratorio');
    const filterByLabChk = document.getElementById('fc-filter-by-lab');
    const productSearch = document.getElementById('fc-product-search');
    const productSelectAll = document.getElementById('fc-product-select-all');
    const productList = document.getElementById('fc-product-list');

    // --- Elements: Top Filters ---
    const profileSelect = document.getElementById('fc-profile');
    const negocioSelect = document.getElementById('fc-negocio');
    const subnegocioSelect = document.getElementById('fc-subnegocio');

    // --- Elements: Chart & Toggles ---
    const viewMoneyToggle = document.getElementById('fc-view-money');
    const growthInput = document.getElementById('fc-growth-pct');
    const chartDiv = document.getElementById('fc-chart');
    const chartLoader = document.getElementById('fc-chart-loader');

    // --- Elements: Table ---
    const tableWrapper = document.getElementById('fc-table-wrapper');
    const tableHead = document.getElementById('fc-table-head');
    const tableBody = document.getElementById('fc-table-body');
    const tableFoot = document.getElementById('fc-table-foot');
    const tableLoader = document.getElementById('fc-table-loader');
    const btnExport = document.getElementById('fc-btn-export');

    // --- Elements: Modal ---
    const modalOverlay = document.getElementById('fc-modal-overlay');
    const modalClose = document.getElementById('fc-modal-close');
    const modalClientName = document.getElementById('fc-modal-client-name');
    const modalPerfil = document.getElementById('fc-modal-perfil');
    const modalNegocio = document.getElementById('fc-modal-negocio');
    const modalCount = document.getElementById('fc-modal-count');
    const modalFilter = document.getElementById('fc-modal-filter');
    const modalLoader = document.getElementById('fc-modal-loader');
    const modalBody = document.getElementById('fc-modal-body');
    const modalDownloadBtn = document.getElementById('fc-modal-download');
    const modalSaveBtn = document.getElementById('fc-modal-save');
    const modalCancelBtn = document.getElementById('fc-modal-cancel');

    // --- State ---
    let allProducts = [];
    let abortController = null;
    let currentTableData = null;
    let productNegocioMap = {};  // product -> {neg, subneg}

    // ==========================================
    // 1. Initialization
    // ==========================================
    async function init() {
        try {
            const r = await fetch('/api/forecast/filters');
            if (!r.ok) throw new Error("Filters unavailable");
            const data = await r.json();

            if (data.min_date) { paramInicio.min = data.min_date; paramFin.min = data.min_date; }
            if (data.max_date) { paramInicio.max = data.max_date; paramFin.max = data.max_date; }
            // Strict: use actual data range boundaries
            paramInicio.value = data.default_start || data.min_date || '';
            paramFin.value = data.default_end || data.max_date || '';

            populateSelect(profileSelect, data.profiles);
            populateSelect(negocioSelect, data.negocios);
            populateSelect(subnegocioSelect, data.subnegocios);
            populateSelect(labSelect, data.laboratorios);

            allProducts = data.products || [];
            // Store the product_lab_map for highlighting
            window._productLabMap = data.product_lab_map || {};
            // Store product -> negocio/subnegocio map for auto-filtering
            productNegocioMap = data.product_negocio_map || {};
            renderProductList();

            [paramInicio, paramFin, profileSelect, negocioSelect, subnegocioSelect, viewMoneyToggle, growthInput].forEach(el => {
                el.addEventListener('change', updateAll);
            });
            growthInput.addEventListener('input', debounce(updateAll, 500));

            labSelect.addEventListener('change', () => { filterProductList(); highlightLabProducts(); });
            filterByLabChk.addEventListener('change', () => { filterProductList(); highlightLabProducts(); });
            productSearch.addEventListener('input', filterProductList);
            productSelectAll.addEventListener('change', (e) => {
                const isChecked = e.target.checked;
                productList.querySelectorAll('.fc-product-checkbox').forEach(chk => {
                    const row = chk.closest('.fc-product-item');
                    if (row.style.display !== 'none') chk.checked = isChecked;
                });
                syncNegocioFromProduct();
                updateAll();
            });

            // Modal events
            modalClose.addEventListener('click', closeModal);
            modalCancelBtn.addEventListener('click', closeModal);
            modalOverlay.addEventListener('click', (e) => { if (e.target === modalOverlay) closeModal(); });
            if (modalFilter) modalFilter.addEventListener('input', filterModalProducts);
            modalDownloadBtn.addEventListener('click', downloadModalCSV);
            modalSaveBtn.addEventListener('click', saveModalChanges);

            updateAll();
        } catch (e) {
            console.error("Init Error", e);
            chartLoader.textContent = "Error al cargar datos.";
        }
    }

    // ==========================================
    // 2. Sidebar Logic
    // ==========================================
    function populateSelect(selectEl, items) {
        items.forEach(i => {
            const opt = document.createElement('option');
            opt.value = i; opt.textContent = i;
            selectEl.appendChild(opt);
        });
    }

    function renderProductList() {
        const lab = labSelect.value;
        const enforceLab = filterByLabChk.checked;
        const term = productSearch.value.toLowerCase().trim();

        // Filter logic
        let filtered = allProducts;
        const plm = window._productLabMap || {};
        if (enforceLab && lab !== 'ALL') {
            filtered = filtered.filter(p => {
                const labs = plm[p.id];
                return labs && labs.includes(lab);
            });
        }
        if (term) {
            filtered = filtered.filter(p => p.id.toLowerCase().includes(term));
        }

        // Limit rendering to 1000 items for performance
        const limit = 1000;
        const toRender = filtered.slice(0, limit);

        productList.innerHTML = "";
        const fragment = document.createDocumentFragment();

        toRender.forEach((p, idx) => {
            const row = document.createElement('div');
            row.className = 'fc-product-item';
            row.dataset.lab = p.lab || "SIN LABORATORIO";
            row.dataset.name = p.id.toLowerCase();
            const chkId = `fc-prod-${idx}`;
            row.innerHTML = `<input type="checkbox" class="fc-checkbox fc-product-checkbox" id="${chkId}" value="${p.id}"><label for="${chkId}">${p.id}</label>`;
            fragment.appendChild(row);
            row.querySelector('input').addEventListener('change', () => { updateProductSelectAllState(); syncNegocioFromProduct(); updateAll(); });
        });

        productList.appendChild(fragment);

        if (filtered.length > limit) {
            const msg = document.createElement('div');
            msg.className = 'fc-table-info';
            msg.style.padding = '5px';
            msg.textContent = `Mostrando primeros ${limit} de ${filtered.length} productos...`;
            productList.appendChild(msg);
        }

        highlightLabProducts();
        updateProductSelectAllState();
    }

    function filterProductList() {
        // Now renderProductList handles filtering and rendering together for optimization
        renderProductList();
    }

    function highlightLabProducts() {
        const lab = labSelect.value;
        const enforceLab = filterByLabChk.checked;
        const plm = window._productLabMap || {};

        // 1. Highlight Products in Sidebar
        productList.querySelectorAll('.fc-product-item').forEach(row => {
            row.classList.remove('fc-lab-highlight');
            if (lab && lab !== 'ALL' && !enforceLab) {
                const prodId = row.querySelector('input')?.value;
                if (prodId && plm[prodId] && plm[prodId].includes(lab)) {
                    row.classList.add('fc-lab-highlight');
                }
            }
        });

        // 2. Highlight Clients in Table
        tableBody.querySelectorAll('tr').forEach(tr => {
            tr.classList.remove('fc-lab-highlight');
            if (lab && lab !== 'ALL' && !enforceLab) {
                if (tr.dataset.labs) {
                    try {
                        const labs = JSON.parse(tr.dataset.labs);
                        if (labs.includes(lab)) {
                            tr.classList.add('fc-lab-highlight');
                        }
                    } catch (e) { }
                }
            }
        });

        // 3. Highlight Products in Modal (if open)
        highlightModalProducts();
    }

    function highlightModalProducts() {
        const lab = labSelect.value;
        const enforceLab = filterByLabChk.checked;
        const plm = window._productLabMap || {};

        document.querySelectorAll('.fc-modal-product-row').forEach(tr => {
            tr.classList.remove('fc-lab-highlight');
            if (lab && lab !== 'ALL' && !enforceLab) {
                const prodName = tr.dataset.name;
                const keys = Object.keys(plm);
                const matchingKey = keys.find(k => k.toLowerCase() === prodName);
                if (matchingKey && plm[matchingKey].includes(lab)) {
                    tr.classList.add('fc-lab-highlight');
                }
            }
        });
    }

    function updateProductSelectAllState() {
        const visible = Array.from(productList.querySelectorAll('.fc-product-item')).filter(r => r.style.display !== 'none');
        if (!visible.length) { productSelectAll.checked = false; productSelectAll.indeterminate = false; return; }
        const checked = visible.filter(r => r.querySelector('input').checked).length;
        productSelectAll.checked = checked === visible.length;
        productSelectAll.indeterminate = checked > 0 && checked < visible.length;
    }

    function getSelectedProducts() {
        return Array.from(productList.querySelectorAll('.fc-product-checkbox:checked')).map(c => c.value);
    }

    // ====== Auto-filter Negocio / Subnegocio based on selected product ======
    function syncNegocioFromProduct() {
        var selected = getSelectedProducts();
        if (selected.length === 1) {
            // Single product selected — look up its negocio/subnegocio
            var info = productNegocioMap[selected[0]];
            if (info) {
                if (info.neg) {
                    negocioSelect.value = info.neg;
                    negocioSelect.disabled = true;
                }
                if (info.subneg) {
                    subnegocioSelect.value = info.subneg;
                    subnegocioSelect.disabled = true;
                }
            }
        } else {
            // Zero or multiple products — unlock and reset
            negocioSelect.disabled = false;
            subnegocioSelect.disabled = false;
            if (selected.length === 0) {
                negocioSelect.value = 'ALL';
                subnegocioSelect.value = 'ALL';
            }
        }
    }

    function getPayload() {
        return {
            start_date: paramInicio.value,
            end_date: paramFin.value,
            profiles: profileSelect.value !== 'ALL' ? [profileSelect.value] : [],
            negocios: negocioSelect.value !== 'ALL' ? [negocioSelect.value] : [],
            subnegocios: subnegocioSelect.value !== 'ALL' ? [subnegocioSelect.value] : [],
            products: getSelectedProducts(),
            growth_pct: parseFloat(growthInput.value) || 0,
            view_money: viewMoneyToggle.checked
        };
    }

    // ==========================================
    // 3. API & Updates
    // ==========================================
    async function updateAll() {
        if (abortController) abortController.abort();
        abortController = new AbortController();
        const signal = abortController.signal;

        chartDiv.style.display = 'none'; chartLoader.style.display = 'block';
        tableWrapper.style.display = 'none'; tableLoader.style.display = 'block';

        const payload = getPayload();

        try {
            const [cRes, tRes] = await Promise.all([
                fetch('/api/forecast/chart', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), signal }),
                fetch('/api/forecast/clients', { method: 'POST', headers: { 'Content-Type': 'application/json' }, body: JSON.stringify(payload), signal })
            ]);
            if (!cRes.ok || !tRes.ok) throw new Error("API HTTP Error");
            const cData = await cRes.json();
            const tData = await tRes.json();

            renderChart(cData, payload.view_money);
            renderTable(tData, payload.view_money);
        } catch (e) {
            if (e.name !== 'AbortError') { console.error(e); chartLoader.textContent = "Error"; tableLoader.textContent = "Error"; }
        }
    }

    // ==========================================
    // 4. Plotly Chart — European format
    // ==========================================
    function renderChart(data, viewMoney) {
        chartLoader.style.display = 'none';
        chartDiv.style.display = 'block';
        const traces = [];

        // Helper: format number like the original (European: 1.234.567)
        function fmtNum(v) {
            return Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, '.');
        }

        // --- CONFIDENCE INTERVAL (background layer, no hover) ---
        if (data.ci_upper && data.ci_upper.length) {
            traces.push({
                x: data.ci_upper.map(d => d.x),
                y: data.ci_upper.map(d => d.y),
                name: 'Upper Bound',
                type: 'scatter',
                mode: 'lines',
                line: { width: 0 },
                showlegend: false,
                hoverinfo: 'skip'
            });
            traces.push({
                x: data.ci_lower.map(d => d.x),
                y: data.ci_lower.map(d => d.y),
                name: 'Lower Bound',
                type: 'scatter',
                mode: 'lines',
                fill: 'tonexty',
                fillcolor: 'rgba(99, 102, 241, 0.08)',
                line: { width: 0 },
                showlegend: false,
                hoverinfo: 'skip'
            });
        }

        // --- FORECAST (Proyección Base) ---
        if (data.forecast && data.forecast.length) {
            const fcstY = data.forecast.map(d => d.y);
            const fcstLi = data.ci_lower ? data.ci_lower.map(d => d.y) : fcstY.map(() => 0);
            const fcstLs = data.ci_upper ? data.ci_upper.map(d => d.y) : fcstY.map(() => 0);
            const customdata = fcstY.map((v, i) => [
                fmtNum(v), fmtNum(fcstLi[i] || 0), fmtNum(fcstLs[i] || 0)
            ]);
            traces.push({
                x: data.forecast.map(d => d.x),
                y: fcstY,
                name: 'Proyección (Base)',
                type: 'scatter',
                mode: 'lines',
                line: { color: '#6366F1', width: 2.5, dash: 'dash', shape: 'spline', smoothing: 1.3 },
                customdata: customdata,
                hovertemplate: '<b>Proyección (Base)</b><br>%{x|%b %Y}<br>Pronóstico: $%{customdata[0]}<br>Lm. Inf: $%{customdata[1]}<br>Lm. Sup: $%{customdata[2]}<extra></extra>'
            });
        }

        // --- ADJUSTED FORECAST ---
        if (data.forecast_adj && data.forecast_adj.length) {
            const adjY = data.forecast_adj.map(d => d.y);
            const customdata = adjY.map(v => [fmtNum(v)]);
            const pct = data.growth_pct || 0;
            traces.push({
                x: data.forecast_adj.map(d => d.x),
                y: adjY,
                name: `Proyección (+${pct > 0 ? '+' : ''}${pct}%)`,
                type: 'scatter',
                mode: 'lines',
                line: { color: '#10B981', width: 2.5, dash: 'dot', shape: 'spline', smoothing: 1.3 },
                customdata: customdata,
                hovertemplate: `<b>Proyección (+${pct}%)</b><br>%{x|%b %Y}<br>Ajustado: $%{customdata[0]}<extra></extra>`
            });
        }

        // --- HISTORY (topmost, most prominent) ---
        if (data.history && data.history.length) {
            const histY = data.history.map(d => d.y);
            const customdata = histY.map(v => [fmtNum(v)]);
            traces.push({
                x: data.history.map(d => d.x),
                y: histY,
                name: 'Historia',
                type: 'scatter',
                mode: 'lines',
                line: { color: '#0F172A', width: 3, shape: 'spline', smoothing: 1.3 },
                customdata: customdata,
                hovertemplate: '<b>Historia</b><br>%{x|%b %Y}<br>$%{customdata[0]}<extra></extra>'
            });
        }

        const layout = {
            height: 420,
            margin: { t: 40, l: 0, r: 20, b: 40 },
            font: { family: 'Inter, sans-serif', color: '#475569', size: 12 },
            legend: {
                orientation: 'h', yanchor: 'bottom', y: 1.05,
                xanchor: 'left', x: 0,
                bgcolor: 'rgba(0,0,0,0)',
                font: { size: 12, color: '#0F172A' }
            },
            hovermode: 'x unified',
            paper_bgcolor: 'rgba(0,0,0,0)',
            plot_bgcolor: 'rgba(0,0,0,0)',
            separators: '.,',
            xaxis: {
                showgrid: false, zeroline: false,
                showline: true, linecolor: '#E2E8F0',
                title: '', tickformat: '%b %Y',
                tickfont: { color: '#94A3B8' }
            },
            yaxis: {
                showgrid: true, gridcolor: '#F1F5F9', gridwidth: 1,
                zeroline: false, showline: false,
                title: '', tickfont: { color: '#94A3B8' },
                tickformat: ',d'
            }
        };

        // --- VERTICAL SEPARATOR ---
        if (data.hist_max_date) {
            layout.shapes = [{
                type: 'line',
                x0: data.hist_max_date, x1: data.hist_max_date,
                y0: 0, y1: 1, yref: 'paper',
                line: { color: '#CBD5E1', width: 1, dash: 'dot' }
            }];
            layout.annotations = [
                {
                    x: data.hist_max_date, y: 1, yref: 'paper',
                    text: 'Actual', showarrow: false,
                    font: { size: 10, color: '#64748B' },
                    xanchor: 'right', xshift: -5
                },
                {
                    x: data.hist_max_date, y: 1, yref: 'paper',
                    text: 'Forecast', showarrow: false,
                    font: { size: 10, color: '#6366F1' },
                    xanchor: 'left', xshift: 5
                }
            ];
        }

        // Clamp chart x-axis range to eliminate empty whitespace at the end
        let allDates = [];
        if (data.history) allDates = allDates.concat(data.history.map(d => d.x));
        if (data.forecast) allDates = allDates.concat(data.forecast.map(d => d.x));
        if (data.forecast_adj) allDates = allDates.concat(data.forecast_adj.map(d => d.x));
        if (allDates.length > 0) {
            allDates.sort(); // String sorting works for YYYY-MM-DD
            layout.xaxis.range = [allDates[0], allDates[allDates.length - 1]];
        }

        Plotly.newPlot(chartDiv, traces, layout, {
            responsive: true,
            displayModeBar: false,
            locale: 'es'
        });
    }

    // ==========================================
    // 5. Hierarchical Table (Group → Client)
    // ==========================================
    function renderTable(data, viewMoney) {
        tableLoader.style.display = 'none';
        currentTableData = data;
        if (!data.columns || !data.columns.length) { tableWrapper.style.display = 'none'; return; }
        tableWrapper.style.display = 'block';

        const cols = data.columns;
        const fmt = val => {
            if (val === 0) return '-';
            return viewMoney
                ? '$ ' + Math.round(val).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".")
                : Math.round(val).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
        };
        const heatBg = (val) => {
            if (!val || val === 0 || data.min_val === data.max_val) return 'transparent';
            const n = (val - data.min_val) / (data.max_val - data.min_val);
            return `rgba(16,185,129,${(0.05 + n * 0.55).toFixed(2)})`;
        };

        // Header
        tableHead.innerHTML = `<th class="col-sticky col-edit"></th><th class="col-sticky col-client">CLIENTE</th>`;
        cols.forEach(c => { tableHead.innerHTML += `<th>${c}</th>`; });

        // Body
        tableBody.innerHTML = '';
        let groupIdx = 0;
        data.groups.forEach(g => {
            const gid = `fc-grp-${groupIdx++}`;
            const hasMultipleClients = g.clients.length > 1;
            const isExpandable = g.grupo && hasMultipleClients;

            // Group row
            const grpTr = document.createElement('tr');
            grpTr.className = 'fc-row-group';
            grpTr.dataset.gid = gid;
            if (g.labs && g.labs.length) grpTr.dataset.labs = JSON.stringify(g.labs);

            const tdEdit = document.createElement('td');
            tdEdit.className = 'col-sticky col-edit';
            grpTr.appendChild(tdEdit);

            const tdName = document.createElement('td');
            tdName.className = 'col-sticky col-client';
            if (isExpandable) {
                tdName.innerHTML = `<span class="fc-chevron" data-gid="${gid}">▶</span> ${g.grupo}`;
                tdName.style.cursor = 'pointer';
                tdName.addEventListener('click', () => toggleGroup(gid));
            } else {
                const cli = g.clients[0];
                tdName.innerHTML = `<span class="fc-pencil" title="Ver detalle">✎</span> ${cli.cliente}`;
                tdName.querySelector('.fc-pencil').addEventListener('click', (e) => {
                    e.stopPropagation();
                    openModal(cli.cliente);
                });
            }
            grpTr.appendChild(tdName);

            cols.forEach(c => {
                const td = document.createElement('td');
                td.className = 'heatmap-cell';
                const val = g.totals[c] || 0;
                td.style.backgroundColor = heatBg(val);
                td.textContent = fmt(val);
                grpTr.appendChild(td);
            });
            tableBody.appendChild(grpTr);

            if (isExpandable) {
                g.clients.forEach(cli => {
                    const cliTr = document.createElement('tr');
                    cliTr.className = `fc-row-client fc-child-${gid}`;
                    cliTr.style.display = 'none';
                    if (cli.labs && cli.labs.length) cliTr.dataset.labs = JSON.stringify(cli.labs);

                    const tdPencil = document.createElement('td');
                    tdPencil.className = 'col-sticky col-edit';
                    tdPencil.innerHTML = `<span class="fc-pencil" title="Ver detalle">✎</span>`;
                    tdPencil.querySelector('.fc-pencil').addEventListener('click', () => openModal(cli.cliente));
                    cliTr.appendChild(tdPencil);

                    const tdCli = document.createElement('td');
                    tdCli.className = 'col-sticky col-client fc-indent';
                    tdCli.textContent = cli.cliente;
                    cliTr.appendChild(tdCli);

                    cols.forEach(c => {
                        const td = document.createElement('td');
                        td.className = 'heatmap-cell';
                        const val = cli[c] || 0;
                        td.style.backgroundColor = heatBg(val);
                        td.textContent = fmt(val);
                        cliTr.appendChild(td);
                    });
                    tableBody.appendChild(cliTr);
                });
            }
        });

        // Footer (Grand Totals)
        tableFoot.innerHTML = '';
        const hasGrowth = data.growth_pct !== 0;

        let finalSum = 0;
        if (hasGrowth) {
            // Row 1: Base
            const trBase = document.createElement('tr');
            trBase.innerHTML = '<td class="col-sticky col-edit"></td><td class="col-sticky col-client text-muted">Total (Base)</td>';
            cols.forEach(c => {
                const td = document.createElement('td');
                td.textContent = fmt(data.grand_totals_base[c] || 0);
                td.style.color = '#64748b';
                trBase.appendChild(td);
            });
            tableFoot.appendChild(trBase);

            // Row 2: Adjusted
            const trAdj = document.createElement('tr');
            trAdj.innerHTML = `<td class="col-sticky col-edit"></td><td class="col-sticky col-client" style="color:#059669; font-weight:bold;">Total (Aj. +${data.growth_pct}%)</td>`;
            cols.forEach(c => {
                const td = document.createElement('td');
                const val = data.grand_totals[c] || 0;
                finalSum += val;
                td.textContent = fmt(val);
                td.style.color = '#059669';
                td.style.fontWeight = 'bold';
                trAdj.appendChild(td);
            });
            tableFoot.appendChild(trAdj);
        } else {
            const footTr = document.createElement('tr');
            footTr.innerHTML = '<td class="col-sticky col-edit"></td><td class="col-sticky col-client">Total</td>';
            cols.forEach(c => {
                const td = document.createElement('td');
                const val = data.grand_totals[c] || 0;
                finalSum += val;
                td.textContent = fmt(val);
                footTr.appendChild(td);
            });
            tableFoot.appendChild(footTr);
        }

        // Total proyectado en vista
        const totalLabel = document.getElementById('fc-total-proyectado');
        if (totalLabel) {
            totalLabel.textContent = `Total Proyectado en vista: $ ${Math.round(finalSum).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".")}`;
        }
        // Highlight any clients based on selected lab initially
        highlightLabProducts();
    }

    function toggleGroup(gid) {
        const children = document.querySelectorAll(`.fc-child-${gid}`);
        const chevron = document.querySelector(`.fc-chevron[data-gid="${gid}"]`);
        const isOpen = children[0] && children[0].style.display !== 'none';
        children.forEach(c => c.style.display = isOpen ? 'none' : 'table-row');
        if (chevron) chevron.textContent = isOpen ? '▶' : '▼';
    }

    // Global state for edits
    window.subneg_overrides = window.subneg_overrides || {};
    window.product_month_overrides = window.product_month_overrides || {};
    window.currentClientDetail = null;

    async function openModal(clienteName) {
        modalOverlay.style.display = 'flex';
        document.body.style.overflow = 'hidden';
        modalLoader.style.display = 'block';
        // Clear previous content but keep loader
        const existingContent = modalBody.querySelector('.fc-modal-negocios-wrapper');
        if (existingContent) existingContent.remove();
        modalClientName.textContent = `Cliente: ${clienteName}`;
        if (modalFilter) modalFilter.value = '';

        if (!window.subneg_overrides[clienteName]) window.subneg_overrides[clienteName] = {};
        if (!window.product_month_overrides[clienteName]) window.product_month_overrides[clienteName] = {};

        const payload = getPayload();
        payload.cliente = clienteName;
        // In modal, we want to see all products for this client, even those unselected in sidebar
        payload.products = [];
        payload.laboratorio = labSelect.value;

        try {
            const res = await fetch('/api/forecast/client-detail', {
                method: 'POST',
                headers: { 'Content-Type': 'application/json' },
                body: JSON.stringify(payload)
            });
            if (!res.ok) throw new Error("Detail fetch failed");
            const detail = await res.json();

            window.currentClientDetail = detail;

            modalPerfil.textContent = `Perfil: ${detail.perfil || 'N/A'}`;
            modalNegocio.textContent = `Negocio: ${detail.negocio || 'N/A'}`;
            modalCount.textContent = `${detail.n_products || 0} artículos`;

            renderModalGrouped(detail, clienteName);
            highlightModalProducts(); // Ensure modal is highlighted when opened
        } catch (e) {
            console.error(e);
            modalLoader.textContent = "Error al cargar detalle.";
        }
    }

    function renderModalGrouped(detail, clientName) {
        modalLoader.style.display = 'none';
        const existing = modalBody.querySelector('.fc-modal-negocios-wrapper');
        if (existing) existing.remove();

        if (!detail.negocios || !detail.negocios.length) {
            modalLoader.style.display = 'block';
            modalLoader.textContent = 'No se encontraron productos para este cliente.';
            return;
        }

        const cols = detail.columns || [];
        const wrapper = document.createElement('div');
        wrapper.className = 'fc-modal-negocios-wrapper';

        // Global Growth info
        const globalGrowth = detail.growth_pct || 0;
        const growthInfo = document.createElement('div');
        growthInfo.className = 'fc-modal-growth-info mb-3';
        growthInfo.innerHTML = `<span class="badge bg-secondary">📈 Crecimiento Global: ${globalGrowth}%</span>`;
        wrapper.appendChild(growthInfo);

        detail.negocios.forEach((neg, negIdx) => {
            // Expandable section header
            const section = document.createElement('div');
            section.className = 'fc-modal-neg-section border rounded mb-3';

            const header = document.createElement('div');
            header.className = 'fc-modal-neg-header bg-light p-2 cursor-pointer border-bottom d-flex align-items-center';
            header.innerHTML = `<span class="fc-modal-neg-chevron fw-bold me-2" style="width:15px;display:inline-block;text-align:center;">▶</span> <strong style="color:#334155;">${neg.negocio}</strong> <span class="text-muted ms-2" style="font-size:12px;">(${neg.count} artículos)</span>`;
            header.addEventListener('click', () => {
                const body = section.querySelector('.fc-modal-neg-body');
                const chev = header.querySelector('.fc-modal-neg-chevron');
                if (body.style.display === 'none') {
                    body.style.display = 'block';
                    chev.textContent = '▼';
                } else {
                    body.style.display = 'none';
                    chev.textContent = '▶';
                }
            });
            section.appendChild(header);

            const body = document.createElement('div');
            body.className = 'fc-modal-neg-body p-2';
            body.style.display = 'none'; // Collapsed by default

            // Subnegocio tabs
            if (neg.subnegocios && neg.subnegocios.length > 0) {
                const tabBar = document.createElement('div');
                tabBar.className = 'fc-modal-tab-bar d-flex gap-2 border-bottom pb-2 mb-3';

                // Tab Header Generation
                neg.subnegocios.forEach((sn, snIdx) => {
                    const tabBtn = document.createElement('button');
                    tabBtn.className = 'fc-modal-tab btn btn-sm ' + (snIdx === 0 ? 'btn-dark' : 'btn-outline-secondary');
                    tabBtn.textContent = sn.subnegocio;
                    tabBtn.addEventListener('click', () => {
                        tabBar.querySelectorAll('.fc-modal-tab').forEach(t => { t.classList.remove('btn-dark'); t.classList.add('btn-outline-secondary'); });
                        tabBtn.classList.add('btn-dark');
                        tabBtn.classList.remove('btn-outline-secondary');
                        body.querySelectorAll('.fc-modal-tab-content').forEach(tc => tc.style.display = 'none');
                        body.querySelector(`[data-tab="${negIdx}-${snIdx}"]`).style.display = 'block';
                    });
                    tabBar.appendChild(tabBtn);
                });
                body.appendChild(tabBar);

                // Tab Content Generation
                neg.subnegocios.forEach((sn, snIdx) => {
                    const tabContent = document.createElement('div');
                    tabContent.className = 'fc-modal-tab-content';
                    tabContent.dataset.tab = `${negIdx}-${snIdx}`;
                    tabContent.style.display = snIdx === 0 ? 'block' : 'none';

                    // Subnegocio Override Controller
                    const overrideVal = window.subneg_overrides[clientName][sn.subnegocio] !== undefined ? window.subneg_overrides[clientName][sn.subnegocio] : globalGrowth;
                    const isOverridden = window.subneg_overrides[clientName][sn.subnegocio] !== undefined && window.subneg_overrides[clientName][sn.subnegocio] !== globalGrowth;

                    const subnegCtrl = document.createElement('div');
                    subnegCtrl.className = 'd-flex align-items-center mb-2 justify-content-end bg-light p-2 rounded';
                    subnegCtrl.innerHTML = `
                        <label class="me-2 text-muted fw-bold" style="font-size:12px;">📈 Crec. (${sn.subnegocio}):</label>
                        <input type="number" class="form-control form-control-sm fc-subneg-override-input" value="${overrideVal}" step="0.5" style="width:70px;">
                        <span class="ms-2 badge ${isOverridden ? 'bg-danger' : 'bg-secondary'}">${isOverridden ? '(Override)' : '(Global)'}</span>
                    `;

                    const overrideInput = subnegCtrl.querySelector('input');
                    overrideInput.addEventListener('change', (e) => {
                        const newVal = parseFloat(e.target.value);
                        if (newVal === globalGrowth) {
                            delete window.subneg_overrides[clientName][sn.subnegocio];
                        } else {
                            window.subneg_overrides[clientName][sn.subnegocio] = newVal;
                        }
                        // Re-render whole modal to recalculate
                        renderModalGrouped(window.currentClientDetail, clientName);
                    });

                    tabContent.appendChild(subnegCtrl);
                    tabContent.appendChild(buildProductTable(sn.products, cols, sn.subnegocio, clientName, overrideVal));
                    body.appendChild(tabContent);
                });
            }

            section.appendChild(body);
            wrapper.appendChild(section);
        });

        modalBody.appendChild(wrapper);
    }

    function buildProductTable(products, cols, subnegName, clientName, activeGrowth) {
        const tableDiv = document.createElement('div');
        tableDiv.className = 'table-responsive';

        const table = document.createElement('table');
        table.className = 'fc-datagrid fc-modal-table w-100';

        // Header: 2 rows (month group + sub-columns)
        const thead = document.createElement('thead');
        let headRow1 = '<tr><th class="col-sticky" rowspan="2" style="min-width:250px;">Producto</th><th rowspan="2" style="min-width:50px;">U.M.</th>';
        cols.forEach(c => { headRow1 += `<th colspan="4" class="fc-modal-month-header text-center border-start">${c}</th>`; });
        headRow1 += '</tr>';

        let headRow2 = '<tr>';
        cols.forEach(() => {
            headRow2 += '<th class="fc-modal-sub-header fc-sub-orig border-start">Orig</th>';
            headRow2 += '<th class="fc-modal-sub-header fc-sub-pct text-primary" style="background-color:#eff6ff !important;" title="Editable">%</th>';
            headRow2 += '<th class="fc-modal-sub-header fc-sub-nuevo">Nuevo</th>';
            headRow2 += '<th class="fc-modal-sub-header fc-sub-monto">$</th>';
        });
        headRow2 += '</tr>';
        thead.innerHTML = headRow1 + headRow2;
        table.appendChild(thead);

        // Body
        const tbody = document.createElement('tbody');
        const fmtN = v => v === 0 ? '0' : Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");
        const fmtM = v => v === 0 ? '$ 0' : '$ ' + Math.round(v).toString().replace(/\B(?=(\d{3})+(?!\d))/g, ".");

        products.forEach(p => {
            const tr = document.createElement('tr');
            tr.className = 'fc-modal-product-row';
            tr.dataset.name = p.producto.toLowerCase();

            const tdProd = document.createElement('td');
            tdProd.className = 'col-sticky';
            tdProd.style.textAlign = 'left';
            tdProd.style.fontWeight = '500';
            tdProd.textContent = p.producto;
            tr.appendChild(tdProd);

            const tdUm = document.createElement('td');
            tdUm.textContent = p.um;
            tdUm.style.textAlign = 'center';
            tdUm.style.color = '#64748b';
            tr.appendChild(tdUm);

            p.months.forEach((m, mIdx) => {
                // Determine base pct
                let calcPct = activeGrowth;
                let ra = activeGrowth / 100.0;
                let rm = Math.pow(1 + ra, 1 / 12.0) - 1;
                let factor = Math.pow(1 + rm, mIdx + 1); // Approximation of months_diff

                // Allow product-level overrides
                const pKey = p.producto + "_" + m.label;
                let isProductOverridden = window.product_month_overrides[clientName][pKey] !== undefined;
                let finalPct = isProductOverridden ? window.product_month_overrides[clientName][pKey] : parseFloat((rm * 100).toFixed(1));

                const tdOrig = document.createElement('td');
                tdOrig.className = 'fc-modal-cell-orig text-end border-start';
                tdOrig.textContent = fmtN(m.orig);
                tr.appendChild(tdOrig);

                const tdPct = document.createElement('td');
                tdPct.className = 'fc-modal-cell-pct text-end';
                tdPct.style.backgroundColor = '#eff6ff'; // Light blue to indicate editability
                // Editable Input
                const pctInput = document.createElement('input');
                pctInput.type = 'number';
                pctInput.className = 'form-control form-control-sm text-end fc-pct-edit hide-arrows';
                pctInput.style.padding = '0 2px';
                pctInput.style.width = '48px';
                pctInput.style.display = 'inline-block';
                pctInput.style.border = '1px solid #bfdbfe';
                pctInput.style.color = isProductOverridden ? '#b91c1c' : '#1d4ed8';
                pctInput.style.fontWeight = isProductOverridden ? 'bold' : 'normal';
                pctInput.step = '0.1';
                pctInput.value = finalPct;

                const pctSymbol = document.createElement('span');
                pctSymbol.textContent = '%';
                pctSymbol.style.fontSize = '11px';
                pctSymbol.style.marginLeft = '2px';

                const flexContainer = document.createElement('div');
                flexContainer.className = 'd-flex align-items-center justify-content-end';
                flexContainer.appendChild(pctInput);
                flexContainer.appendChild(pctSymbol);
                tdPct.appendChild(flexContainer);
                tr.appendChild(tdPct);

                const tdNuevo = document.createElement('td');
                tdNuevo.className = 'fc-modal-cell-nuevo text-end font-monospace';

                const tdMonto = document.createElement('td');
                tdMonto.className = 'fc-modal-cell-monto text-end font-monospace text-muted';

                // Update function logic
                const updateRowCalculations = () => {
                    let enteredPct = parseFloat(pctInput.value) || 0;

                    // Update state
                    if (Math.abs(enteredPct - parseFloat((Math.pow(1 + activeGrowth / 100, 1 / 12) - 1) * 100).toFixed(1)) > 0.05) {
                        window.product_month_overrides[clientName][pKey] = enteredPct;
                        pctInput.style.color = '#b91c1c';
                        pctInput.style.fontWeight = 'bold';
                    } else {
                        delete window.product_month_overrides[clientName][pKey];
                        pctInput.style.color = '#1d4ed8';
                        pctInput.style.fontWeight = 'normal';
                    }

                    // Compute Nuevo and Monto dynamically
                    let nuevoVal = Math.round(m.orig * (1 + (enteredPct / 100.0)));
                    let montoVal = Math.round(nuevoVal * p.unit_price);

                    tdNuevo.textContent = fmtN(nuevoVal);
                    if (nuevoVal !== m.orig && nuevoVal > 0) tdNuevo.style.color = '#059669';
                    else tdNuevo.style.color = '';

                    tdMonto.textContent = fmtM(montoVal);
                };

                // Initial calculation
                updateRowCalculations();

                // Listeners
                pctInput.addEventListener('change', updateRowCalculations);
                pctInput.addEventListener('keyup', (e) => { if (e.key === 'Enter') updateRowCalculations(); });

                tr.appendChild(tdNuevo);
                tr.appendChild(tdMonto);
            });
            tbody.appendChild(tr);
        });

        table.appendChild(tbody);
        tableDiv.appendChild(table);
        return tableDiv;
    }

    function filterModalProducts() {
        const term = modalFilter.value.toLowerCase().trim();
        modalBody.querySelectorAll('.fc-modal-product-row').forEach(tr => {
            tr.style.display = !term || tr.dataset.name.includes(term) ? '' : 'none';
        });
    }

    function closeModal() {
        modalOverlay.style.display = 'none';
        document.body.style.overflow = '';
        const wrapper = modalBody.querySelector('.fc-modal-negocios-wrapper');
        if (wrapper) wrapper.remove();
    }

    // ====== Save changes (persist growth overrides and re-render dashboard) ======
    function saveModalChanges() {
        // The subneg_overrides and product_month_overrides are already stored globally.
        // Close modal and trigger a full dashboard refresh so changes reflect.
        closeModal();
        updateAll();
    }

    // ====== Download CSV from modal detail ======
    function downloadModalCSV() {
        var detail = window.currentClientDetail;
        if (!detail || !detail.negocios || !detail.negocios.length) return;

        var clientName = detail.client || 'cliente';
        var perfil = detail.perfil || '';
        var rows = [];

        // Header
        rows.push(['articulo', 'descripcion', 'unidad_medida', 'nivel_agregacion', 'periodo_str', 'yhat_nuevo', 'cliente_id', 'perfil', 'neg'].join(';'));

        var csvVal = function (v) {
            var s = String(v == null ? '' : v);
            if (s.indexOf(';') >= 0 || s.indexOf('"') >= 0) return '"' + s.replace(/"/g, '""') + '"';
            return s;
        };

        detail.negocios.forEach(function (neg) {
            var negName = neg.negocio || '';
            if (!neg.subnegocios) return;
            neg.subnegocios.forEach(function (sn) {
                // Determine active growth % for this subnegocio
                var activeGrowth = detail.growth_pct || 0;
                if (window.subneg_overrides && window.subneg_overrides[clientName] && window.subneg_overrides[clientName][sn.subnegocio] !== undefined) {
                    activeGrowth = window.subneg_overrides[clientName][sn.subnegocio];
                }
                // Pre-compute monthly rate from annual growth (same formula as modal table)
                var ra = activeGrowth / 100.0;
                var rm = Math.pow(1 + ra, 1 / 12.0) - 1;

                if (!sn.products) return;
                sn.products.forEach(function (p) {
                    if (!p.months || !p.months.length) return;
                    p.months.forEach(function (m, mIdx) {
                        // Check for per-product-month override first (key uses "_" like the modal)
                        var pKey = p.producto + '_' + m.label;
                        var finalPct;
                        if (window.product_month_overrides && window.product_month_overrides[clientName] && window.product_month_overrides[clientName][pKey] !== undefined) {
                            finalPct = window.product_month_overrides[clientName][pKey];
                        } else {
                            finalPct = parseFloat((rm * 100).toFixed(1));
                        }
                        // Recalculate nuevo from orig using the active % (same as modal line 908)
                        var yhatNuevo = Math.round(m.orig * (1 + (finalPct / 100.0)));

                        rows.push([
                            csvVal(p.producto),
                            csvVal(p.producto),
                            csvVal(p.um || 'Unid.'),
                            csvVal(''),
                            csvVal(m.label),
                            yhatNuevo,
                            csvVal(clientName),
                            csvVal(perfil),
                            csvVal(negName)
                        ].join(';'));
                    });
                });
            });
        });

        var csvContent = rows.join('\n');
        var blob = new Blob(["\uFEFF" + csvContent], { type: 'text/csv;charset=utf-8;' });
        var url = URL.createObjectURL(blob);
        var a = document.createElement('a');
        var safeName = clientName.replace(/[^a-zA-Z0-9_.-]/g, '_').substring(0, 60);
        a.href = url;
        a.download = 'proyeccion_' + safeName + '.csv';
        a.style.display = 'none';
        document.body.appendChild(a);
        a.click();
        document.body.removeChild(a);
        URL.revokeObjectURL(url);
    }

    // ==========================================
    // 7. Export
    // ==========================================
    btnExport.addEventListener('click', () => {
        const trs = document.querySelectorAll('#fc-table tr');
        let csv = [];
        trs.forEach(tr => {
            if (tr.style.display === 'none') return;
            let row = [];
            tr.querySelectorAll('th, td').forEach(td => {
                let text = td.textContent.replace(/\$/g, '').replace(/\./g, '').trim();
                row.push(`"${text}"`);
            });
            csv.push(row.join(','));
        });
        const blob = new Blob([csv.join('\n')], { type: 'text/csv' });
        const url = window.URL.createObjectURL(blob);
        const a = document.createElement('a');
        a.setAttribute('hidden', ''); a.setAttribute('href', url); a.setAttribute('download', 'forecast_proyeccion.csv');
        document.body.appendChild(a); a.click(); document.body.removeChild(a);
    });

    function debounce(func, wait) {
        let timeout;
        return function (...args) { clearTimeout(timeout); timeout = setTimeout(() => func(...args), wait); };
    }

    init();
});
