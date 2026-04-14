document.addEventListener('DOMContentLoaded', () => {
    Chart.defaults.font.family = "'Outfit', sans-serif";
    Chart.defaults.color = '#64748b';
    Chart.defaults.scale.grid.color = 'rgba(226, 232, 240, 0.4)';
    Chart.defaults.plugins.tooltip.backgroundColor = '#0f172a';
    Chart.defaults.plugins.tooltip.titleFont = { size: 12, weight: 700, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.bodyFont = { size: 11, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.cornerRadius = 8;

    // ─────────────────────────────────────────────────────────────────────────
    // Estado global del dashboard
    // ─────────────────────────────────────────────────────────────────────────
    const state = {
        filtersLoaded: false,
        dashboardReady: false,
        areaChart: null,
        pieChart: null,
        barClientChart: null,
        mapInstance: null,
        mapMarkers: [],
        currentDateRange: { min: null, max: null },
        bootstrapCache: new Map(),
        negocioLabels: { unidades: {}, subunidades: {} },
        pivotPage: 1,
        pivotPageSize: 50,
        pivotTotal: 0,
    };

    // AbortController activo para cancelar request /bootstrap en vuelo
    let _loadAbortController = null;

    // ─────────────────────────────────────────────────────────────────────────
    // Factory: MultiSelect avanzado con búsqueda, checkboxes y Aplicar/Todos/Limpiar
    // ─────────────────────────────────────────────────────────────────────────
    function createMultiSelect(containerId, onApply) {
        const container = document.getElementById(containerId);
        if (!container) return null;

        const trigger  = container.querySelector('.dim-ms-trigger');
        const panel    = container.querySelector('.dim-ms-panel');
        const search   = container.querySelector('.dim-ms-search');
        const list     = container.querySelector('.dim-ms-list');
        const todosBtn = container.querySelector('.dim-ms-todos');
        const limpiarBtn = container.querySelector('.dim-ms-limpiar');
        const aplicarBtn = container.querySelector('.dim-ms-aplicar');
        const labelEl  = container.querySelector('.dim-ms-label');

        let allOptions = [];   // [{value, label}]
        let pending    = new Set(); // estado visual temporal (no aplicado)
        let applied    = new Set(); // estado real (enviado al backend)
        let isOpen     = false;

        function open() {
            // Cierra cualquier otro panel abierto
            document.querySelectorAll('.dim-ms-panel[data-open="true"]').forEach(p => {
                if (p !== panel) {
                    p.style.display = 'none';
                    p.removeAttribute('data-open');
                }
            });
            pending = new Set(applied);
            renderList(search ? search.value : '');
            panel.style.display = 'block';
            panel.setAttribute('data-open', 'true');
            isOpen = true;
        }

        function close() {
            panel.style.display = 'none';
            panel.removeAttribute('data-open');
            isOpen = false;
        }

        function renderList(filter) {
            const q = (filter || '').toLowerCase().trim();
            list.innerHTML = '';
            const visible = allOptions.filter(opt =>
                !q ||
                opt.label.toLowerCase().includes(q) ||
                String(opt.value).toLowerCase().includes(q)
            );
            if (visible.length === 0) {
                list.innerHTML = '<div class="dim-ms-empty">Sin resultados</div>';
                return;
            }
            const frag = document.createDocumentFragment();
            visible.forEach(opt => {
                const div = document.createElement('div');
                div.className = 'dim-ms-item';
                const uid = `ms-${containerId}-${String(opt.value).replace(/\W/g, '_')}`;
                const checked = pending.has(String(opt.value)) ? 'checked' : '';
                div.innerHTML = `
                    <input class="form-check-input dim-ms-cb" type="checkbox" id="${uid}" value="${opt.value}" ${checked}>
                    <label class="form-check-label" for="${uid}" title="${opt.label}">${opt.label}</label>
                `;
                div.querySelector('input').addEventListener('change', e => {
                    const val = e.target.value;
                    if (e.target.checked) pending.add(val);
                    else pending.delete(val);
                });
                frag.appendChild(div);
            });
            list.appendChild(frag);
        }

        function updateTriggerLabel() {
            if (applied.size === 0) {
                labelEl.textContent = 'Todos';
            } else if (applied.size === 1) {
                const val = [...applied][0];
                const opt = allOptions.find(o => String(o.value) === String(val));
                labelEl.textContent = opt ? opt.label : val;
            } else {
                labelEl.textContent = `${applied.size} seleccionados`;
            }
        }

        // ── Eventos ────────────────────────────────────────────────────────
        trigger.addEventListener('click', e => {
            e.stopPropagation();
            isOpen ? close() : open();
        });

        if (search) {
            search.addEventListener('input', () => renderList(search.value));
        }

        if (todosBtn) {
            todosBtn.addEventListener('click', () => {
                pending = new Set(allOptions.map(o => String(o.value)));
                renderList(search ? search.value : '');
            });
        }

        if (limpiarBtn) {
            limpiarBtn.addEventListener('click', () => {
                pending = new Set();
                renderList(search ? search.value : '');
            });
        }

        if (aplicarBtn) {
            aplicarBtn.addEventListener('click', () => {
                applied = new Set(pending);
                updateTriggerLabel();
                close();
                if (typeof onApply === 'function') onApply();
            });
        }

        // Click fuera → cerrar
        document.addEventListener('click', e => {
            if (isOpen && !container.contains(e.target)) close();
        });

        // ── API pública ─────────────────────────────────────────────────────
        return {
            setOptions(opts) {
                // opts: string[] | {value, label}[]
                allOptions = opts.map(o =>
                    typeof o === 'object' ? { value: String(o.value), label: o.label || String(o.value) }
                                         : { value: String(o), label: String(o) }
                );
                updateTriggerLabel();
            },
            getApplied() {
                return Array.from(applied);
            },
            clearApplied() {
                applied = new Set();
                pending = new Set();
                updateTriggerLabel();
            },
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Controlador del rango de fechas (slider dual + inputs manuales)
    // ─────────────────────────────────────────────────────────────────────────
    function createDateRangeCtrl(onApply) {
        const wrap       = document.getElementById('msDateRangeWrap');
        const trigger    = document.getElementById('dateRangeTrigger');
        const panel      = document.getElementById('dateRangePanel');
        const labelEl    = document.getElementById('dateRangeLabel');
        const fillEl     = document.getElementById('dateSliderFill');
        const sliderMin  = document.getElementById('dateRangeMinSlider');
        const sliderMax  = document.getElementById('dateRangeMaxSlider');
        const startDisp  = document.getElementById('dateRangeStartDisp');
        const endDisp    = document.getElementById('dateRangeEndDisp');
        const dateStart  = document.getElementById('dateStart');
        const dateEnd    = document.getElementById('dateEnd');
        const todosBtn   = document.getElementById('dateRangeTodosBtn');
        const limpiarBtn = document.getElementById('dateRangeLimpiarBtn');
        const aplicarBtn = document.getElementById('dateRangeAplicarBtn');

        if (!wrap) return { setMonths() {}, getAppliedMin() { return null; }, getAppliedMax() { return null; } };

        let months = [];       // ['2024-01', '2024-02', ...]
        let appliedMin = null; // null = sin filtro (usar todo el rango)
        let appliedMax = null;
        let isOpen = false;

        function open() {
            document.querySelectorAll('.dim-ms-panel[data-open="true"]').forEach(p => {
                if (p !== panel) { p.style.display = 'none'; p.removeAttribute('data-open'); }
            });
            panel.style.display = 'block';
            panel.setAttribute('data-open', 'true');
            isOpen = true;
        }

        function close() {
            panel.style.display = 'none';
            panel.removeAttribute('data-open');
            isOpen = false;
        }

        function sliderPct(idx) {
            if (months.length <= 1) return idx === 0 ? 0 : 100;
            return (idx / (months.length - 1)) * 100;
        }

        function syncFill() {
            const minIdx = parseInt(sliderMin.value);
            const maxIdx = parseInt(sliderMax.value);
            const pMin = sliderPct(minIdx);
            const pMax = sliderPct(maxIdx);
            fillEl.style.left  = pMin + '%';
            fillEl.style.width = (pMax - pMin) + '%';
            startDisp.textContent = months[minIdx] ? formatMonthLabel(months[minIdx]) : '-';
            endDisp.textContent   = months[maxIdx] ? formatMonthLabel(months[maxIdx]) : '-';
        }

        function sliderToDate(idx, isEnd) {
            const m = months[idx];
            if (!m) return '';
            if (isEnd) {
                // último día del mes
                const [y, mo] = m.split('-').map(Number);
                const last = new Date(y, mo, 0).getDate();
                return `${m}-${String(last).padStart(2, '0')}`;
            }
            return `${m}-01`;
        }

        function dateToSliderIdx(dateStr, isEnd) {
            // dateStr: YYYY-MM-DD → encontrar índice del mes en array
            if (!dateStr) return isEnd ? months.length - 1 : 0;
            const month = dateStr.slice(0, 7); // YYYY-MM
            const idx = months.indexOf(month);
            return idx >= 0 ? idx : (isEnd ? months.length - 1 : 0);
        }

        function updateTriggerLabel() {
            if (appliedMin === null && appliedMax === null) {
                labelEl.textContent = 'Todas las fechas';
            } else {
                const startM = appliedMin ? appliedMin.slice(0, 7) : months[0];
                const endM   = appliedMax ? appliedMax.slice(0, 7) : months[months.length - 1];
                labelEl.textContent = `${formatMonthLabel(startM)} — ${formatMonthLabel(endM)}`;
            }
        }

        // Slider min: no puede superar max
        sliderMin.addEventListener('input', () => {
            if (parseInt(sliderMin.value) > parseInt(sliderMax.value)) {
                sliderMin.value = sliderMax.value;
            }
            syncFill();
            dateStart.value = sliderToDate(parseInt(sliderMin.value), false);
        });

        // Slider max: no puede ser menor que min
        sliderMax.addEventListener('input', () => {
            if (parseInt(sliderMax.value) < parseInt(sliderMin.value)) {
                sliderMax.value = sliderMin.value;
            }
            syncFill();
            dateEnd.value = sliderToDate(parseInt(sliderMax.value), true);
        });

        // Input manual inicio → sincronizar slider
        dateStart.addEventListener('change', () => {
            const idx = dateToSliderIdx(dateStart.value, false);
            sliderMin.value = idx;
            if (parseInt(sliderMin.value) > parseInt(sliderMax.value)) {
                sliderMax.value = sliderMin.value;
                dateEnd.value = sliderToDate(parseInt(sliderMax.value), true);
            }
            syncFill();
        });

        // Input manual fin → sincronizar slider
        dateEnd.addEventListener('change', () => {
            const idx = dateToSliderIdx(dateEnd.value, true);
            sliderMax.value = idx;
            if (parseInt(sliderMax.value) < parseInt(sliderMin.value)) {
                sliderMin.value = sliderMax.value;
                dateStart.value = sliderToDate(parseInt(sliderMin.value), false);
            }
            syncFill();
        });

        trigger.addEventListener('click', e => {
            e.stopPropagation();
            isOpen ? close() : open();
        });

        todosBtn.addEventListener('click', () => {
            sliderMin.value = 0;
            sliderMax.value = months.length - 1;
            dateStart.value = sliderToDate(0, false);
            dateEnd.value   = sliderToDate(months.length - 1, true);
            syncFill();
        });

        limpiarBtn.addEventListener('click', () => {
            sliderMin.value = 0;
            sliderMax.value = months.length - 1;
            dateStart.value = sliderToDate(0, false);
            dateEnd.value   = sliderToDate(months.length - 1, true);
            syncFill();
        });

        aplicarBtn.addEventListener('click', () => {
            const minIdx = parseInt(sliderMin.value);
            const maxIdx = parseInt(sliderMax.value);
            // Si cubre todo el rango → sin filtro (null)
            if (minIdx === 0 && maxIdx === months.length - 1) {
                appliedMin = null;
                appliedMax = null;
            } else {
                appliedMin = sliderToDate(minIdx, false);
                appliedMax = sliderToDate(maxIdx, true);
            }
            updateTriggerLabel();
            close();
            if (typeof onApply === 'function') onApply();
        });

        document.addEventListener('click', e => {
            if (isOpen && !wrap.contains(e.target)) close();
        });

        return {
            setMonths(monthsList) {
                months = monthsList;
                const max = Math.max(0, months.length - 1);
                sliderMin.min = 0; sliderMin.max = max; sliderMin.value = 0;
                sliderMax.min = 0; sliderMax.max = max; sliderMax.value = max;
                dateStart.value = sliderToDate(0, false);
                dateEnd.value   = sliderToDate(max, true);
                syncFill();
                updateTriggerLabel();
            },
            getAppliedMin() { return appliedMin; },
            getAppliedMax() { return appliedMax; },
            resetApplied() {
                appliedMin = null;
                appliedMax = null;
                if (months.length) {
                    const max = months.length - 1;
                    sliderMin.value = 0;
                    sliderMax.value = max;
                    dateStart.value = sliderToDate(0, false);
                    dateEnd.value   = sliderToDate(max, true);
                    syncFill();
                }
                updateTriggerLabel();
            },
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Instancias de los controles de filtro
    // (se crean antes de bindEvents para que estén disponibles en buildQueryParams)
    // ─────────────────────────────────────────────────────────────────────────
    const msClient  = createMultiSelect('msClientWrap',  triggerLoad);
    const msProvince = createMultiSelect('msProvinceWrap', triggerLoad);
    const msFamily  = createMultiSelect('msFamilyWrap',  triggerLoad);
    const msUnit    = createMultiSelect('msUnitWrap',    triggerLoad);
    const msSubunit = createMultiSelect('msSubunitWrap', triggerLoad);
    const dateRangeCtrl = createDateRangeCtrl(triggerLoad);

    // triggerLoad: dispara el refresh del dashboard con debounce
    let _filterDebounceTimer = null;
    function triggerLoad() {
        clearTimeout(_filterDebounceTimer);
        _filterDebounceTimer = setTimeout(loadDashboardData, 350);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Referencias DOM (solo elementos simples que no son multi-select)
    // ─────────────────────────────────────────────────────────────────────────
    const elements = {
        loadingOverlay:    document.getElementById('loadingOverlay'),
        loadingText:       document.getElementById('loadingText'),
        emptyState:        document.getElementById('emptyState'),
        dashboardContent:  document.getElementById('dashboardContent'),
        reloadBtn:         document.getElementById('reloadDashboardBtn'),
        datasetStatusPill: document.getElementById('datasetStatusPill'),
        datasetUpdatedAt:  document.getElementById('datasetUpdatedAt'),
        lastUpdateBadge:   document.getElementById('lastUpdateBadge'),
        filterIsClient:    document.getElementById('filterIsClient'),
        platformCheckboxes: Array.from(document.querySelectorAll('.platform-checkbox')),
        applyPlatformsBtn:  document.getElementById('applyPlatformsBtn'),
        platformsTodosBtn:  document.getElementById('platformsTodosBtn'),
        platformsLimpiarBtn: document.getElementById('platformsLimpiarBtn'),
        platformsLabel:     document.getElementById('platformsLabel'),
        kpiClients:  document.getElementById('kpiClients'),
        kpiRecords:  document.getElementById('kpiRecords'),
        kpiFamilies: document.getElementById('kpiFamilies'),
        familyListContainer:  document.getElementById('familyListContainer'),
        pivotHeader:          document.getElementById('pivotHeader'),
        pivotBody:            document.getElementById('pivotBody'),
        pivotPagination:      document.getElementById('pivotPagination'),
        pivotTotalLabel:      document.getElementById('pivotTotalLabel'),
    };

    const reloadBtnDefaultHtml = elements.reloadBtn ? elements.reloadBtn.innerHTML : '';

    // ─────────────────────────────────────────────────────────────────────────
    // Coordenadas provinciales
    // ─────────────────────────────────────────────────────────────────────────
    const provinceCoords = {
        'Buenos Aires': [-36.6769, -60.5588], 'CABA': [-34.6037, -58.3816],
        'Catamarca': [-28.4696, -65.7852],    'Chaco': [-26.3366, -60.7663],
        'Chubut': [-43.7886, -68.8892],       'Cordoba': [-32.1429, -63.8017],
        'Corrientes': [-28.7743, -57.7568],   'Entre Rios': [-32.0588, -59.2014],
        'Formosa': [-24.8949, -59.5679],      'Jujuy': [-23.3200, -65.7643],
        'La Pampa': [-37.1315, -65.4466],     'La Rioja': [-29.6857, -67.1817],
        'Mendoza': [-34.3667, -68.9167],      'Misiones': [-26.8753, -54.6518],
        'Neuquen': [-38.9525, -68.9126],      'Rio Negro': [-40.0388, -65.5525],
        'Salta': [-24.2991, -64.8144],        'San Juan': [-30.8653, -68.8892],
        'San Luis': [-33.7577, -66.0281],     'Santa Cruz': [-48.8154, -69.2542],
        'Santa Fe': [-30.7069, -60.9498],     'Santiago Del Estero': [-27.7824, -63.2523],
        'Tierra Del Fuego': [-53.4862, -68.3039], 'Tucuman': [-26.8241, -65.2226],
    };

    const seriesPalette  = ['#064066','#1e5c8a','#5274ce','#38bdf8','#10b981','#64748b'];
    const resultPalette  = ['#064066','#1e5c8a','#5274ce','#38bdf8','#10b981','#94a3b8'];

    bindEvents();
    initDashboard();

    // ─────────────────────────────────────────────────────────────────────────
    // bindEvents
    // ─────────────────────────────────────────────────────────────────────────
    function bindEvents() {
        // Filtro ¿Cliente?
        if (elements.filterIsClient) {
            elements.filterIsClient.addEventListener('change', triggerLoad);
        }

        // ── Plataformas ────────────────────────────────────────────────────
        // Todos = desmarcar todo (0 checked = todas las plataformas)
        if (elements.platformsTodosBtn) {
            elements.platformsTodosBtn.addEventListener('click', () => {
                elements.platformCheckboxes.forEach(cb => { cb.checked = false; });
                updatePlatformLabel();
            });
        }
        // Limpiar = igual que Todos
        if (elements.platformsLimpiarBtn) {
            elements.platformsLimpiarBtn.addEventListener('click', () => {
                elements.platformCheckboxes.forEach(cb => { cb.checked = false; });
                updatePlatformLabel();
            });
        }
        // Aplicar plataformas
        if (elements.applyPlatformsBtn) {
            elements.applyPlatformsBtn.addEventListener('click', () => {
                updatePlatformLabel();
                const dropdownEl = document.getElementById('platformsDropdownBtn');
                const bsDropdown = window.bootstrap && window.bootstrap.Dropdown
                    ? window.bootstrap.Dropdown.getInstance(dropdownEl) : null;
                if (bsDropdown) bsDropdown.hide();
                clearTimeout(_filterDebounceTimer);
                loadDashboardData();
            });
        }
        // Actualizar label al hacer tick en cualquier checkbox de plataforma
        elements.platformCheckboxes.forEach(cb => {
            cb.addEventListener('change', updatePlatformLabel);
        });

        // Botón reload principal
        if (elements.reloadBtn) {
            elements.reloadBtn.addEventListener('click', () => {
                state.bootstrapCache.clear();
                if (state.dashboardReady) {
                    loadDashboardData({ blocking: true, bypassSnapshot: true, force: true });
                } else {
                    initDashboard(true);
                }
            });
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Lógica de plataformas
    // 0 seleccionadas = Todas (sin filtro enviado al backend)
    // ─────────────────────────────────────────────────────────────────────────
    function updatePlatformLabel() {
        const selected = elements.platformCheckboxes.filter(cb => cb.checked);
        if (selected.length === 0 || selected.length === elements.platformCheckboxes.length) {
            elements.platformsLabel.textContent = 'Todas';
        } else {
            elements.platformsLabel.textContent = selected
                .map(cb => cb.value.charAt(0) + cb.value.slice(1).toLowerCase())
                .join(', ');
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // initDashboard
    // ─────────────────────────────────────────────────────────────────────────
    async function initDashboard(forceLive = false) {
        setLoading(true, 'Leyendo snapshot persistido...');
        try {
            const [bootstrapResponse, labelsResponse] = await Promise.all([
                apiGet('/bootstrap', forceLive ? { bypass_snapshot: true } : {}),
                apiGet('/negocio-labels').catch(() => ({ data: { unidades: {}, subunidades: {} } })),
            ]);
            const bootstrap = bootstrapResponse.data || {};
            const status = bootstrap.status || {};
            renderStatus(status);

            if (labelsResponse && labelsResponse.data) {
                state.negocioLabels = labelsResponse.data;
            }

            if (!status.has_data) {
                elements.emptyState.style.display = 'block';
                elements.dashboardContent.style.display = 'none';
                return;
            }

            elements.emptyState.style.display = 'none';
            elements.dashboardContent.style.display = 'contents';
            renderBootstrapPayload(bootstrap);
            cacheBootstrapPayload(buildCacheKey(buildQueryParams()), bootstrap);
            state.dashboardReady = true;

            if (bootstrap.meta?.stale) {
                window.setTimeout(() => loadDashboardData({ bypassSnapshot: true, force: true }), 0);
            }
        } catch (error) {
            console.error(error);
            elements.datasetStatusPill.textContent = 'Error';
            elements.datasetStatusPill.className = 'badge text-bg-danger';
            elements.emptyState.style.display = 'block';
            elements.dashboardContent.style.display = 'none';
            elements.emptyState.querySelector('p').textContent =
                `No se pudo cargar Dimensionamiento: ${error.message}`;
        } finally {
            setLoading(false);
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // renderBootstrapPayload
    // ─────────────────────────────────────────────────────────────────────────
    function renderBootstrapPayload(bootstrap) {
        const filterData = bootstrap.filters || {
            clientes: [], provincias: [], familias: [],
            unidades_negocio: [], subunidades_negocio: [],
            resultados: [], date_range: { min: null, max: null },
        };
        state.currentDateRange = filterData.date_range || { min: null, max: null };
        applyFilterOptions(filterData);

        if (!state.filtersLoaded && filterData.date_range) {
            const months = generateMonthsArray(
                filterData.date_range.min,
                filterData.date_range.max
            );
            dateRangeCtrl.setMonths(months);
            state.filtersLoaded = true;
        }

        renderKpis(bootstrap.kpis || {});
        renderAreaChart(bootstrap.series || { months: [], datasets: [] });
        renderPieChart(bootstrap.results || []);
        renderFamilyList(bootstrap.top_families || []);
        renderMapChart(bootstrap.geo || []);
        renderBarClientChart(bootstrap.clients_by_result || []);
        renderPivotTable(bootstrap.family_consumption || { months: [], rows: [], total: 0 }, true);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // applyFilterOptions — poblamos los multi-select con opciones del backend
    // ─────────────────────────────────────────────────────────────────────────
    function applyFilterOptions(data) {
        if (msClient) msClient.setOptions(data.clientes || []);
        if (msProvince) msProvince.setOptions(data.provincias || []);
        if (msFamily) msFamily.setOptions(data.familias || []);

        // Unidad de negocio: value = código, label = descripción
        const unidadOpts = (data.unidades_negocio || []).map(code => ({
            value: code,
            label: resolveUnitLabel(code),
        }));
        if (msUnit) msUnit.setOptions(unidadOpts);

        // Subunidad: ídem
        const currentUnits = msUnit ? msUnit.getApplied() : [];
        const currentUnit  = currentUnits.length === 1 ? currentUnits[0] : null;
        const subunidadOpts = (data.subunidades_negocio || []).map(code => ({
            value: code,
            label: resolveSubunitLabel(code, currentUnit),
        }));
        if (msSubunit) msSubunit.setOptions(subunidadOpts);
    }

    // ─────────────────────────────────────────────────────────────────────────
    // buildQueryParams — lee el estado aplicado de todos los filtros
    // ─────────────────────────────────────────────────────────────────────────
    function buildQueryParams() {
        // Plataformas: 0 checked = sin filtro (todas); ≥1 = filtrar por esas
        const plataformas = elements.platformCheckboxes
            .filter(cb => cb.checked)
            .map(cb => cb.value);

        // Fechas: el controlador devuelve null cuando cubre el rango completo
        const rawFechaDesde = dateRangeCtrl.getAppliedMin();
        const rawFechaHasta = dateRangeCtrl.getAppliedMax();
        const fechaDesde = rawFechaDesde && rawFechaDesde !== state.currentDateRange.min
            ? rawFechaDesde : null;
        const fechaHasta = rawFechaHasta && rawFechaHasta !== state.currentDateRange.max
            ? rawFechaHasta : null;

        return {
            cliente:           msClient   ? msClient.getApplied()   : [],
            provincia:         msProvince ? msProvince.getApplied()  : [],
            familia:           msFamily   ? msFamily.getApplied()    : [],
            unidad_negocio:    msUnit     ? msUnit.getApplied()      : [],
            subunidad_negocio: msSubunit  ? msSubunit.getApplied()   : [],
            plataforma:        plataformas, // vacío = todas
            fecha_desde:       fechaDesde,
            fecha_hasta:       fechaHasta,
            is_client:   elements.filterIsClient ? (elements.filterIsClient.value || null) : null,
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // loadDashboardData — carga principal con AbortController + cache
    // ─────────────────────────────────────────────────────────────────────────
    async function loadDashboardData(options = {}) {
        const { blocking = false, bypassSnapshot = false, force = false } = options;

        if (_loadAbortController) _loadAbortController.abort();
        _loadAbortController = new AbortController();
        const signal = _loadAbortController.signal;

        const query = buildQueryParams();
        const cacheKey = buildCacheKey(query);
        const cachedPayload = !force ? state.bootstrapCache.get(cacheKey) : null;

        if (blocking) {
            setLoading(true, 'Consultando metricas agregadas...');
        } else {
            setLoading(false);
            setRefreshing(true);
        }

        if (cachedPayload) renderBootstrapPayload(cachedPayload);

        try {
            const response = await apiGet('/bootstrap', {
                ...query,
                include_status: false,
                bypass_snapshot: bypassSnapshot,
            }, signal);

            if (signal.aborted) return;

            const bootstrap = (response && response.data) || {};
            renderBootstrapPayload(bootstrap);
            cacheBootstrapPayload(cacheKey, bootstrap);
        } catch (err) {
            if (err.name === 'AbortError') return;
            console.error('[DIM] loadDashboardData error:', err);
            if (!cachedPayload) {
                renderKpisError();
                showCanvasError('areaChart', 'areaChart', 'Error al cargar');
                showCanvasError('pieChart', 'pieChart', '');
                showContainerError(elements.familyListContainer, '');
            }
        } finally {
            if (!signal.aborted) {
                if (blocking) setLoading(false);
                setRefreshing(false);
            }
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // renderStatus — solo actualiza badge y pill (sin "Contexto de datos")
    // ─────────────────────────────────────────────────────────────────────────
    function renderStatus(status) {
        elements.datasetStatusPill.textContent = status.has_data ? 'Datos disponibles' : 'Sin datos';
        elements.datasetStatusPill.className = status.has_data
            ? 'badge text-bg-success' : 'badge text-bg-secondary';

        const ts = status.last_import?.finished_at;
        if (ts && elements.datasetUpdatedAt) {
            elements.datasetUpdatedAt.textContent =
                new Date(ts).toLocaleString('es-AR', { dateStyle: 'short', timeStyle: 'short' });
            if (elements.lastUpdateBadge) elements.lastUpdateBadge.style.display = 'inline-flex';
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Helpers de cache, loading, error
    // ─────────────────────────────────────────────────────────────────────────
    function buildCacheKey(params) {
        const normalized = {};
        Object.keys(params).sort().forEach(key => {
            const value = params[key];
            normalized[key] = Array.isArray(value) ? [...value].sort() : value;
        });
        return JSON.stringify(normalized);
    }

    function cacheBootstrapPayload(key, payload) {
        if (!key) return;
        if (state.bootstrapCache.has(key)) state.bootstrapCache.delete(key);
        state.bootstrapCache.set(key, payload);
        while (state.bootstrapCache.size > 12) {
            state.bootstrapCache.delete(state.bootstrapCache.keys().next().value);
        }
    }

    function setRefreshing(active) {
        if (!elements.reloadBtn) return;
        elements.reloadBtn.disabled = active;
        elements.reloadBtn.innerHTML = active
            ? '<span class="spinner-border spinner-border-sm me-1" aria-hidden="true"></span> Actualizando'
            : reloadBtnDefaultHtml;
    }

    function setLoading(show, text = 'Cargando...') {
        elements.loadingOverlay.style.display = show ? 'flex' : 'none';
        elements.loadingText.textContent = text;
    }

    function renderKpisError() {
        [elements.kpiClients, elements.kpiRecords, elements.kpiFamilies]
            .forEach(el => { if (el) el.textContent = '--'; });
    }

    function showCanvasError(canvasId, chartStateKey, msg) {
        if (state[chartStateKey]) { state[chartStateKey].destroy(); state[chartStateKey] = null; }
        const canvas = document.getElementById(canvasId);
        if (!canvas) return;
        const ctx = canvas.getContext('2d');
        ctx.clearRect(0, 0, canvas.width, canvas.height);
        ctx.save();
        ctx.font = '13px sans-serif';
        ctx.fillStyle = '#94a3b8';
        ctx.textAlign = 'center';
        ctx.textBaseline = 'middle';
        ctx.fillText(msg || 'No disponible temporalmente', canvas.width / 2, canvas.height / 2);
        ctx.restore();
    }

    function showContainerError(container, msg) {
        if (!container) return;
        container.innerHTML = `<div class="text-center text-muted py-4 small">${msg || 'No disponible temporalmente'}</div>`;
    }

    function showPivotError(msg) {
        if (elements.pivotHeader) elements.pivotHeader.innerHTML = '';
        if (elements.pivotBody) {
            elements.pivotBody.innerHTML = `<tr><td colspan="13" class="text-center text-muted py-3 small">${msg || 'No disponible temporalmente'}</td></tr>`;
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // apiGet
    // ─────────────────────────────────────────────────────────────────────────
    async function apiGet(path, params = {}, signal = undefined) {
        const query = new URLSearchParams();
        Object.entries(params).forEach(([key, value]) => {
            if (value === undefined || value === null || value === '') return;
            if (Array.isArray(value)) {
                value.forEach(item => {
                    if (item !== undefined && item !== null && item !== '') query.append(key, item);
                });
                return;
            }
            query.append(key, value);
        });

        const url = `/api/mercado-privado/dimensiones${path}${query.toString() ? `?${query}` : ''}`;
        const fetchOptions = { headers: { Accept: 'application/json' } };
        if (signal) fetchOptions.signal = signal;
        const response = await fetch(url, fetchOptions);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok) throw new Error(payload.detail || payload.message || `Error HTTP ${response.status}`);
        return payload;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Resolvers de nombres de negocio
    // ─────────────────────────────────────────────────────────────────────────
    function resolveUnitLabel(code) {
        if (!code && code !== 0) return String(code);
        const key = _normalizeNegocioCode(code);
        return state.negocioLabels.unidades[key] || String(code);
    }

    function resolveSubunitLabel(code, unitCode) {
        if (!code && code !== 0) return String(code);
        const sKey = _normalizeNegocioCode(code);
        if (unitCode) {
            const uKey = _normalizeNegocioCode(unitCode);
            if (state.negocioLabels.subunidades[`${uKey}|${sKey}`])
                return state.negocioLabels.subunidades[`${uKey}|${sKey}`];
        }
        const prefix = `|${sKey}`;
        const hit = Object.entries(state.negocioLabels.subunidades).find(([k]) => k.endsWith(prefix));
        return hit ? hit[1] : String(code);
    }

    function _normalizeNegocioCode(code) {
        const s = String(code).trim();
        const n = parseFloat(s);
        return Number.isFinite(n) ? String(Math.round(n)) : s;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Generador de array de meses entre dos fechas
    // ─────────────────────────────────────────────────────────────────────────
    function generateMonthsArray(minDate, maxDate) {
        if (!minDate || !maxDate) return [];
        try {
            const toMonth = d => (d.length > 7 ? d.slice(0, 7) : d);
            const [sy, sm] = toMonth(minDate).split('-').map(Number);
            const [ey, em] = toMonth(maxDate).split('-').map(Number);
            const months = [];
            let y = sy, m = sm;
            while (y < ey || (y === ey && m <= em)) {
                months.push(`${y}-${String(m).padStart(2, '0')}`);
                m++;
                if (m > 12) { m = 1; y++; }
                if (months.length > 240) break; // sanity cap (20 años)
            }
            return months;
        } catch {
            return [];
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Renderers de KPIs y gráficos (sin cambios respecto a versión anterior)
    // ─────────────────────────────────────────────────────────────────────────
    function renderKpis(kpis) {
        elements.kpiClients.textContent  = formatInteger(kpis.clientes || 0);
        elements.kpiRecords.textContent  = formatInteger(kpis.renglones || 0);
        elements.kpiFamilies.textContent = formatInteger(kpis.familias || 0);
        try {
            localStorage.setItem('mp_dimensiones_kpis', JSON.stringify({
                clients: kpis.clientes || 0,
                processes: kpis.renglones || 0,
                families: kpis.familias || 0,
                lastUpdated: new Date().toISOString(),
            }));
        } catch {}
    }

    function renderAreaChart(series) {
        const ctx = document.getElementById('areaChart').getContext('2d');
        if (state.areaChart) state.areaChart.destroy();
        state.areaChart = new Chart(ctx, {
            type: 'line',
            data: {
                labels: series.months || [],
                datasets: (series.datasets || []).map((dataset, index) => ({
                    label: resolveUnitLabel(dataset.label),
                    data: dataset.values,
                    backgroundColor: `${seriesPalette[index % seriesPalette.length]}33`,
                    borderColor: seriesPalette[index % seriesPalette.length],
                    borderWidth: 2,
                    fill: true,
                    tension: 0.35,
                    pointRadius: 2,
                    pointHoverRadius: 5,
                })),
            },
            options: buildLineChartOptions(),
        });
    }

    function renderPieChart(results) {
        const ctx = document.getElementById('pieChart').getContext('2d');
        if (state.pieChart) state.pieChart.destroy();
        state.pieChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: results.map(item => item.resultado),
                datasets: [{
                    data: results.map(item => item.renglones),
                    backgroundColor: results.map((_, i) => resultPalette[i % resultPalette.length]),
                    borderWidth: 0,
                    hoverOffset: 8,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '68%',
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } } },
                    tooltip: {
                        callbacks: { label: ctx => `${ctx.label}: ${formatInteger(ctx.parsed)} renglones` },
                    },
                },
            },
        });
    }

    function renderFamilyList(families) {
        const table = document.createElement('table');
        table.className = 'tech-table w-100 dim-family-table';
        table.innerHTML = `
            <colgroup>
                <col class="dim-family-col-name">
                <col class="dim-family-col-count">
                <col class="dim-family-col-qty">
            </colgroup>
            <thead>
                <tr>
                    <th class="dim-family-head dim-family-head-name">Familia</th>
                    <th class="dim-family-head dim-family-head-count text-end">Renglones</th>
                    <th class="dim-family-head dim-family-head-qty text-end">Cantidad</th>
                </tr>
            </thead>
        `;
        const tbody = document.createElement('tbody');
        families.forEach(item => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="dim-family-name-cell" title="${item.familia}">
                    <div class="dim-family-name-text">${item.familia}</div>
                </td>
                <td class="text-end small text-muted dim-family-number">${formatInteger(item.renglones)}</td>
                <td class="text-end fw-bold dim-family-number" style="color:var(--tech-blue-mid);">${formatDecimal(item.cantidad)}</td>
            `;
            tbody.appendChild(tr);
        });
        table.appendChild(tbody);
        elements.familyListContainer.innerHTML = '';
        elements.familyListContainer.appendChild(table);
    }

    function renderBarClientChart(rows) {
        const ctx = document.getElementById('barClientChart').getContext('2d');
        if (state.barClientChart) state.barClientChart.destroy();
        const dynamicH = Math.max(200, rows.length * 32 + 28);
        const container = ctx.canvas.closest('.chart-container');
        if (container) container.style.height = dynamicH + 'px';
        const labels = rows.map(row => row.cliente);
        const resultKeys = Array.from(new Set(rows.flatMap(row => Object.keys(row.resultados || {}))));
        state.barClientChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: resultKeys.map((key, index) => ({
                    label: key,
                    data: rows.map(row => row.resultados?.[key] || 0),
                    backgroundColor: resultPalette[index % resultPalette.length],
                    borderRadius: 4,
                    barPercentage: 0.7,
                    stack: 'resultados',
                })),
            },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: { label: ctx => `${ctx.dataset.label}: ${formatInteger(ctx.parsed.x)} renglones` },
                    },
                },
                scales: {
                    x: { stacked: true, grid: { display: false }, ticks: { display: false } },
                    y: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            font: { size: 9 },
                            callback(value) {
                                const label = this.getLabelForValue(value);
                                return label.length > 16 ? `${label.slice(0, 16)}..` : label;
                            },
                        },
                    },
                },
            },
        });
    }

    function renderPivotTable(data, resetPage = false) {
        const backendPageSize = Number(data.page_size) > 0 ? Number(data.page_size) : state.pivotPageSize;
        state.pivotPageSize = backendPageSize;

        if (resetPage) {
            state.pivotPage = Number(data.page) > 0 ? Number(data.page) : 1;
        } else if (Number(data.page) > 0) {
            state.pivotPage = Number(data.page);
        }
        const total = (data.total != null) ? data.total : (data.rows || []).length;
        state.pivotTotal = total;

        // Actualizar label de total
        if (elements.pivotTotalLabel) {
            elements.pivotTotalLabel.textContent = `${formatInteger(total)} familias`;
            elements.pivotTotalLabel.style.display = total > 0 ? 'inline-block' : 'none';
        }

        elements.pivotHeader.innerHTML = '';
        elements.pivotBody.innerHTML = '';

        const headerRow = document.createElement('tr');
        const familyHeader = document.createElement('th');
        familyHeader.textContent = 'Familia';
        headerRow.appendChild(familyHeader);
        (data.months || []).forEach(month => {
            const th = document.createElement('th');
            th.className = 'text-end';
            th.textContent = formatMonthLabel(month);
            headerRow.appendChild(th);
        });
        elements.pivotHeader.appendChild(headerRow);

        (data.rows || []).forEach(row => {
            const tr = document.createElement('tr');
            const familyCell = document.createElement('td');
            familyCell.className = 'fw-bold text-secondary';
            familyCell.style.fontSize = '0.75rem';
            familyCell.textContent = row.familia;
            tr.appendChild(familyCell);
            row.values.forEach(value => {
                const td = document.createElement('td');
                td.className = 'text-end text-muted';
                td.textContent = value > 0 ? formatDecimal(value) : '-';
                tr.appendChild(td);
            });
            elements.pivotBody.appendChild(tr);
        });

        renderPivotPagination(total, state.pivotPage, state.pivotPageSize);
    }

    function renderPivotPagination(total, page, pageSize) {
        if (!elements.pivotPagination) return;
        const totalPages = Math.max(1, Math.ceil(total / pageSize));
        if (totalPages <= 1) {
            elements.pivotPagination.style.display = 'none';
            return;
        }
        elements.pivotPagination.style.display = 'flex';
        elements.pivotPagination.innerHTML = '';

        const nav = document.createElement('div');
        nav.className = 'dim-pivot-nav';

        const prevBtn = document.createElement('button');
        prevBtn.className = 'btn btn-sm btn-outline-secondary dim-pivot-page-btn';
        prevBtn.textContent = '← Anterior';
        prevBtn.disabled = page <= 1;
        prevBtn.addEventListener('click', () => loadPivotPage(page - 1));

        const pageInfo = document.createElement('span');
        pageInfo.className = 'dim-pivot-page-info small text-muted';
        const startRow = (page - 1) * pageSize + 1;
        const endRow = Math.min(page * pageSize, total);
        pageInfo.textContent = `${formatInteger(startRow)}–${formatInteger(endRow)} de ${formatInteger(total)} familias`;

        const nextBtn = document.createElement('button');
        nextBtn.className = 'btn btn-sm btn-outline-secondary dim-pivot-page-btn';
        nextBtn.textContent = 'Siguiente →';
        nextBtn.disabled = page >= totalPages;
        nextBtn.addEventListener('click', () => loadPivotPage(page + 1));

        nav.appendChild(prevBtn);
        nav.appendChild(pageInfo);
        nav.appendChild(nextBtn);
        elements.pivotPagination.appendChild(nav);
    }

    async function loadPivotPage(page) {
        if (page < 1) return;
        state.pivotPage = page;
        if (elements.pivotBody) {
            elements.pivotBody.innerHTML = '<tr><td colspan="13" class="text-center text-muted py-3 small"><span class="spinner-border spinner-border-sm me-1"></span>Cargando...</td></tr>';
        }
        try {
            const query = buildQueryParams();
            const response = await apiGet('/family-consumption', {
                ...query,
                page: page,
                page_size: state.pivotPageSize,
            });
            const data = (response && response.data) || { months: [], rows: [], total: 0 };
            renderPivotTable(data, false);
        } catch (err) {
            console.error('[DIM] loadPivotPage error:', err);
            showPivotError('Error al cargar la página');
        }
    }

    function renderMapChart(rows) {
        const container = document.getElementById('mapContainer');
        if (!state.mapInstance) {
            state.mapInstance = L.map(container).setView([-38.4161, -63.6167], 3);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
                subdomains: 'abcd', maxZoom: 19,
            }).addTo(state.mapInstance);
        }
        state.mapMarkers.forEach(m => state.mapInstance.removeLayer(m));
        state.mapMarkers = [];
        rows.forEach(item => {
            const key = Object.keys(provinceCoords).find(p => p.toLowerCase() === String(item.provincia).toLowerCase());
            if (!key) return;
            const marker = L.circle(provinceCoords[key], {
                color: '#5274ce', fillColor: '#5274ce', fillOpacity: 0.45,
                radius: Math.min(Math.max(Math.log((item.renglones || 0) + 1) * 22000, 30000), 300000),
                weight: 1,
            }).addTo(state.mapInstance);
            marker.bindTooltip(`<b>${item.provincia}</b><br>Renglones: ${formatInteger(item.renglones)}`);
            state.mapMarkers.push(marker);
        });
        window.setTimeout(() => state.mapInstance.invalidateSize(), 150);
    }

    function buildLineChartOptions() {
        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: { position: 'top', align: 'end', labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } } },
                tooltip: { callbacks: { label: ctx => `${ctx.dataset.label}: ${formatInteger(ctx.parsed.y)} renglones` } },
            },
            scales: {
                y: { beginAtZero: true, grid: { borderDash: [4, 4], color: '#e2e8f0' }, ticks: { callback: v => formatCompactInteger(v) } },
                x: { grid: { display: false } },
            },
        };
    }

    // ─────────────────────────────────────────────────────────────────────────
    // Formatters
    // ─────────────────────────────────────────────────────────────────────────
    function formatInteger(value) {
        return new Intl.NumberFormat('es-AR', { maximumFractionDigits: 0 }).format(value || 0);
    }
    function formatDecimal(value) {
        return new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 }).format(value || 0);
    }
    function formatCompactInteger(value) {
        return new Intl.NumberFormat('es-AR', { notation: 'compact', maximumFractionDigits: 0 }).format(value || 0);
    }
    function formatMonthLabel(monthIso) {
        if (!monthIso) return '-';
        if (/^\d{2}$/.test(String(monthIso))) {
            return new Intl.DateTimeFormat('es-AR', { month: 'short' }).format(
                new Date(2024, Number(monthIso) - 1, 1)
            );
        }
        const date = new Date(`${monthIso}-01T00:00:00`);
        if (Number.isNaN(date.getTime())) return monthIso;
        return new Intl.DateTimeFormat('es-AR', { month: 'short', year: '2-digit' }).format(date);
    }
    function formatDatasetName(sourcePath) {
        return String(sourcePath).split(/[/\\]/).pop() || sourcePath;
    }

    // ─────────────────────────────────────────────────────────────────────────
    // _widgetRender (helper para renders individuales, sin cambios)
    // ─────────────────────────────────────────────────────────────────────────
    function _widgetRender(result, successFn, errorFn) {
        if (result.status === 'fulfilled' && result.value.ok !== false) {
            try { successFn(result.value.data); } catch (e) {
                console.error('[DIM] Error al renderizar widget:', e);
                errorFn('Error al dibujar el widget.');
            }
            return;
        }
        const errMsg = result.status === 'rejected'
            ? (result.reason?.message || 'Error de red')
            : (result.value?.message || 'No disponible temporalmente');
        console.warn('[DIM] Widget no disponible:', errMsg);
        errorFn(errMsg);
    }
});
