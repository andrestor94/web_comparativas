document.addEventListener('DOMContentLoaded', () => {
    Chart.defaults.font.family = "'Outfit', sans-serif";
    Chart.defaults.color = '#64748b';
    Chart.defaults.scale.grid.color = 'rgba(226, 232, 240, 0.4)';
    Chart.defaults.plugins.tooltip.backgroundColor = '#0f172a';
    Chart.defaults.plugins.tooltip.titleFont = { size: 12, weight: 700, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.bodyFont = { size: 11, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.cornerRadius = 8;
    const DEFAULT_DATE_FILTER = Object.freeze({ min: '2025-01-01', max: '2026-12-31' });

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
        mapGeoJsonLayer: null,
        mapLegendControl: null,
        mapGeoJsonData: null,
        mapGeoJsonPromise: null,
        mapRows: [],
        currentDateRange: { min: null, max: null },
        bootstrapCache: new Map(),
        bootstrapCacheTs: new Map(),
        negocioLabels: { unidades: {}, subunidades: {} },
        familyListData: [],
        familyListRenderRaf: null,
        familyListLastRenderKey: '',
        familyListBody: null,
        pivotData: { months: [], rows: [], total: 0 },
        pivotFilteredRows: [],
        pivotSearchTerm: '',
        pivotRenderRaf: null,
        pivotLastRenderKey: '',
        // Estado global de series excluidas: códigos de unidad_negocio que el usuario
        // desactivó desde la leyenda del gráfico. Se propagan como filtro al backend
        // para KPIs/tablas/donut/mapa, pero NO para el propio gráfico de series
        // (que siempre recibe todos los datos para que se puedan reactivar desde la leyenda).
        hiddenSeriesCodes: new Set(),
        // Códigos originales (sin resolver a label) de los datasets actuales del gráfico.
        chartSeriesCodes: [],
        // Estado global de resultado activo: cuando tiene un valor, filtra TODO el dashboard
        // al resultado seleccionado desde el donut "Resultado por participación".
        activeResultados: new Set(),
        // 'renglones' | 'valorizacion'
        activeMetric: 'renglones',
        familySortKey: 'renglones',
        familySortDirection: 'desc',
        clientSortKey: 'total',
        clientSortDirection: 'desc',
        barClientRows: [],
        familyModal: { rows: [], search: '', sortKey: 'renglones', sortDirection: 'desc', page: 1, initial: null },
        clientModal: { rows: [], resultKeys: [], search: '', condition: '', result: '', metric: 'renglones', sortKey: 'total', sortDirection: 'desc', page: 1, initial: null },
        lastBootstrap: null,
    };

    const FAMILY_LIST_ROW_HEIGHT = 42;
    const FAMILY_LIST_OVERSCAN = 8;
    const FAMILY_CARD_LIMIT = 10;
    const CLIENT_CARD_LIMIT = 10;
    const MODAL_PAGE_SIZE = 25;
    const PIVOT_ROW_HEIGHT = 42;
    const PIVOT_OVERSCAN = 10;
    const NO_FILTER_TOKENS = new Set(['__ALL__', '__all__', 'ALL', 'all', 'Todos', 'TODOS', 'todos', '__TODOS__', '__todos__', 'Todas', 'TODAS', 'todas', '*']);
    const QUERY_WARN_LENGTH = 7000;
    const POST_URL_LENGTH_THRESHOLD = 1800;
    const POST_ARRAY_LENGTH_THRESHOLD = 50;
    // 'cliente_entidad_id' reemplaza a 'cliente': el desplegable manda ids de entidad
    // resuelta (no strings), para filtrar por cliente_entidad_id en el backend.
    const MULTI_FILTER_QUERY_KEYS = new Set(['cliente_entidad_id', 'provincia', 'familia', 'unidad_negocio', 'subunidad_negocio', 'plataforma']);

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

        let allOptions = [];   // [{value, label}] — opciones visibles (puede venir estrechado por otros filtros)
        let universe   = new Set(); // universo COMPLETO de valores vistos (monotónico, nunca se achica).
                                    // Se usa para detectar "todos seleccionados" sin que el estrechamiento
                                    // dinámico de un filtro haga colapsar a otro filtro como si fuera "Todos".
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
                // "Todos" = sin filtro en esta dimensión (selección vacía).
                // Coincide con la convención del filtro Plataformas (0 marcados = Todas) y
                // evita el desfase label/consulta: con selección vacía el trigger muestra
                // "Todos" y el backend no recibe filtro para esta dimensión.
                pending = new Set();
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
                // El universo crece de forma monotónica: aunque el backend devuelva una lista
                // estrechada (porque hay otro filtro activo), seguimos recordando todos los
                // valores conocidos. Así "todos seleccionados" se mide contra el set real.
                allOptions.forEach(o => universe.add(String(o.value)));
                updateTriggerLabel();
            },
            getApplied() {
                return Array.from(applied);
            },
            getOptionValues() {
                return allOptions.map(o => String(o.value));
            },
            getUniverseValues() {
                return Array.from(universe);
            },
            getLabelForValue(value) {
                const option = allOptions.find(o => String(o.value) === String(value));
                return option ? option.label : String(value || '');
            },
            setApplied(values) {
                const vals = Array.isArray(values) ? values : [values];
                applied = new Set(vals.map(String));
                pending = new Set(applied);
                updateTriggerLabel();
                if (list) {
                    renderList(search ? search.value : '');
                }
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
        let appliedMin = DEFAULT_DATE_FILTER.min;
        let appliedMax = DEFAULT_DATE_FILTER.max;
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
            const startM = appliedMin ? appliedMin.slice(0, 7) : months[0];
            const endM   = appliedMax ? appliedMax.slice(0, 7) : months[months.length - 1];
            labelEl.textContent = `${formatMonthLabel(startM)} — ${formatMonthLabel(endM)}`;
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
            appliedMin = sliderToDate(minIdx, false);
            appliedMax = sliderToDate(maxIdx, true);
            updateTriggerLabel();
            close();
            if (typeof onApply === 'function') onApply();
        });

        document.addEventListener('click', e => {
            if (isOpen && !wrap.contains(e.target)) close();
        });

        updateTriggerLabel();

        return {
            setMonths(monthsList) {
                months = monthsList;
                const max = Math.max(0, months.length - 1);
                const minIdx = dateToSliderIdx(appliedMin || DEFAULT_DATE_FILTER.min, false);
                const maxIdx = dateToSliderIdx(appliedMax || DEFAULT_DATE_FILTER.max, true);
                sliderMin.min = 0; sliderMin.max = max; sliderMin.value = minIdx;
                sliderMax.min = 0; sliderMax.max = max; sliderMax.value = Math.max(minIdx, maxIdx);
                dateStart.value = sliderToDate(parseInt(sliderMin.value), false);
                dateEnd.value   = sliderToDate(parseInt(sliderMax.value), true);
                appliedMin = dateStart.value || appliedMin;
                appliedMax = dateEnd.value || appliedMax;
                syncFill();
                updateTriggerLabel();
            },
            getAppliedMin() { return appliedMin; },
            getAppliedMax() { return appliedMax; },
            setExactMonth(monthIso) {
                if (!monthIso) return;
                // Normalizar a YYYY-MM (por si llega YYYY-MM-DD desde chart labels)
                const monthStr = String(monthIso).slice(0, 7);
                appliedMin = monthStr + "-01";
                // max is last day of the month
                const [y, m] = monthStr.split('-');
                const maxD = new Date(y, m, 0).getDate();
                appliedMax = `${monthStr}-${String(maxD).padStart(2, '0')}`;

                if (months.length) {
                    const minIdx = dateToSliderIdx(appliedMin, false);
                    const maxIdx = dateToSliderIdx(appliedMax, true);
                    sliderMin.value = minIdx;
                    sliderMax.value = Math.max(minIdx, maxIdx);
                    dateStart.value = sliderToDate(parseInt(sliderMin.value), false);
                    dateEnd.value   = sliderToDate(parseInt(sliderMax.value), true);
                    syncFill();
                }
                updateTriggerLabel();
            },
            resetApplied() {
                appliedMin = DEFAULT_DATE_FILTER.min;
                appliedMax = DEFAULT_DATE_FILTER.max;
                if (months.length) {
                    const minIdx = dateToSliderIdx(appliedMin, false);
                    const maxIdx = dateToSliderIdx(appliedMax, true);
                    sliderMin.value = minIdx;
                    sliderMax.value = Math.max(minIdx, maxIdx);
                    dateStart.value = sliderToDate(parseInt(sliderMin.value), false);
                    dateEnd.value   = sliderToDate(parseInt(sliderMax.value), true);
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
    const msClient  = createMultiSelect('msClientWrap',  onClientFilterApply);
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

    function onClientFilterApply() {
        updateActiveClientSelection();
        triggerLoad();
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
        kpiClientsBreak: document.getElementById('kpiClientsBreak'),
        identidadFallbackPill: document.getElementById('identidadFallbackPill'),
        kpiRecords:  document.getElementById('kpiRecords'),
        kpiFamilies: document.getElementById('kpiFamilies'),
        kpiProvinces: document.getElementById('kpiProvinces'),
        kpiValorizacion: document.getElementById('kpiValorizacion'),
        kpiValorizacionCard: document.querySelector('.dim-kpi-valorizacion'),
        swMetric: document.getElementById('swMetric'),
        familySortSwitch: document.getElementById('familySortSwitch'),
        clearClientSelection: document.getElementById('clearClientSelection'),
        activeClientSelectionLabel: document.getElementById('activeClientSelectionLabel'),
        clientResultLegend: document.getElementById('clientResultLegend'),
        clientSortHeaders: document.getElementById('clientSortHeaders'),
        clientTotalHeaderLabel: document.getElementById('clientTotalHeaderLabel'),
        openAllFamiliesBtn: document.getElementById('openAllFamiliesBtn'),
        openAllClientsBtn: document.getElementById('openAllClientsBtn'),
        familyFullModal: document.getElementById('familyFullModal'),
        familyModalContext: document.getElementById('familyModalContext'),
        familyModalSearch: document.getElementById('familyModalSearch'),
        familyModalClear: document.getElementById('familyModalClear'),
        familyModalBody: document.getElementById('familyModalBody'),
        familyModalCounter: document.getElementById('familyModalCounter'),
        familyModalPrev: document.getElementById('familyModalPrev'),
        familyModalNext: document.getElementById('familyModalNext'),
        familyModalPage: document.getElementById('familyModalPage'),
        clientFullModal: document.getElementById('clientFullModal'),
        clientModalContext: document.getElementById('clientModalContext'),
        clientModalSearch: document.getElementById('clientModalSearch'),
        clientModalCondition: document.getElementById('clientModalCondition'),
        clientModalResult: document.getElementById('clientModalResult'),
        clientModalMetric: document.getElementById('clientModalMetric'),
        clientModalClear: document.getElementById('clientModalClear'),
        clientModalHead: document.getElementById('clientModalHead'),
        clientModalBody: document.getElementById('clientModalBody'),
        clientModalCounter: document.getElementById('clientModalCounter'),
        clientModalPrev: document.getElementById('clientModalPrev'),
        clientModalNext: document.getElementById('clientModalNext'),
        clientModalPage: document.getElementById('clientModalPage'),
        familyListContainer:  document.getElementById('familyListContainer'),
        pivotTableWrap:       document.getElementById('pivotTableWrap'),
        pivotHeader:          document.getElementById('pivotHeader'),
        pivotBody:            document.getElementById('pivotBody'),
        pivotTotalLabel:      document.getElementById('pivotTotalLabel'),
        pivotFamilySearch:    document.getElementById('pivotFamilySearch'),
        pivotSearchCount:     document.getElementById('pivotSearchCount'),
    };

    const reloadBtnDefaultHtml = elements.reloadBtn ? elements.reloadBtn.innerHTML : '';

    // ─────────────────────────────────────────────────────────────────────────
    const MAP_POSITIVE_COLORS = ['#dbeafe', '#93c5fd', '#60a5fa', '#2563eb', '#123f66'];
    const MAP_ZERO_COLOR = '#eff6ff';
    const MAP_NO_DATA_COLOR = '#e5e7eb';
    const RESULT_FALLBACK_PALETTE = ['#2563eb', '#7c3aed', '#c2410c', '#0891b2', '#b45309', '#be185d'];
    const seriesPalette  = ['#064066','#1e5c8a','#5274ce','#38bdf8','#10b981','#64748b'];

    function normalizeKey(value) {
        return String(value || '')
            .normalize('NFD').replace(/[\u0300-\u036f]/g, '')
            .toUpperCase().replace(/[^A-Z0-9]+/g, '_').replace(/^_+|_+$/g, '');
    }

    function resultColor(result) {
        const key = normalizeKey(result);
        if (key === 'GANADO' || key === 'GANADA') return '#169b62';
        if (key === 'NO_COTIZADO' || key === 'NO_COTIZADA') return '#6b7280';
        if (key === 'NO_PARTICIPO') return '#064066';
        let hash = 0;
        for (let i = 0; i < key.length; i += 1) hash = ((hash << 5) - hash + key.charCodeAt(i)) | 0;
        return RESULT_FALLBACK_PALETTE[Math.abs(hash) % RESULT_FALLBACK_PALETTE.length];
    }

    bindEvents();
    initDashboard();

    // ─────────────────────────────────────────────────────────────────────────
    // bindEvents
    // ─────────────────────────────────────────────────────────────────────────
    function bindEvents() {
        // Switch Renglones / Valorización
        if (elements.swMetric) {
            elements.swMetric.addEventListener('click', e => {
                const btn = e.target.closest('.dim-metric-btn');
                if (!btn) return;
                const metric = btn.dataset.metric;
                if (metric === state.activeMetric) return;
                state.activeMetric = metric;
                elements.swMetric.querySelectorAll('.dim-metric-btn').forEach(b => {
                    b.classList.toggle('active', b.dataset.metric === metric);
                });
                // No re-fetches — just re-render with cached data
                _reRenderWithCurrentMetric();
            });
        }

        // Filtro ¿Cliente?
        if (elements.filterIsClient) {
            elements.filterIsClient.addEventListener('change', triggerLoad);
        }

        if (elements.familySortSwitch) {
            elements.familySortSwitch.addEventListener('click', event => {
                const button = event.target.closest('[data-family-sort]');
                if (!button) return;
                setFamilyCardSort(button.dataset.familySort);
            });
        }
        if (elements.familyListContainer) {
            elements.familyListContainer.addEventListener('click', event => {
                const button = event.target.closest('[data-family-card-sort]');
                if (button) setFamilyCardSort(button.dataset.familyCardSort);
            });
        }
        if (elements.clientSortHeaders) {
            elements.clientSortHeaders.addEventListener('click', event => {
                const button = event.target.closest('[data-client-sort]');
                if (button) setClientCardSort(button.dataset.clientSort);
            });
        }
        if (elements.openAllFamiliesBtn) elements.openAllFamiliesBtn.addEventListener('click', openAllFamiliesModal);
        if (elements.openAllClientsBtn) elements.openAllClientsBtn.addEventListener('click', openAllClientsModal);
        bindFullModalEvents();

        if (elements.clearClientSelection) {
            elements.clearClientSelection.addEventListener('click', () => {
                msClient.clearApplied();
                updateActiveClientSelection();
                triggerLoad();
            });
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
                // Limpiar todos los filtros interactivos derivados de gráficos
                state.hiddenSeriesCodes.clear();
                state.chartSeriesCodes = [];
                state.activeResultados.clear();
                if (state.dashboardReady) {
                    loadDashboardData({ blocking: true, bypassSnapshot: true, force: true });
                } else {
                    initDashboard(true);
                }
            });
        }

        if (elements.familyListContainer) {
            elements.familyListContainer.addEventListener('scroll', () => {
                scheduleFamilyListRender();
            }, { passive: true });
        }

        if (elements.pivotFamilySearch) {
            elements.pivotFamilySearch.addEventListener('input', event => {
                applyPivotSearch(event.target.value, { resetScroll: true });
            });
        }

        if (elements.pivotTableWrap) {
            elements.pivotTableWrap.addEventListener('scroll', () => {
                schedulePivotBodyRender();
            }, { passive: true });
        }

        window.addEventListener('resize', () => {
            scheduleFamilyListRender(true);
            schedulePivotBodyRender(true);
        });
    }

    // ─────────────────────────────────────────────────────────────────────────
    function updateActiveClientSelection() {
        if (!elements.clearClientSelection || !elements.activeClientSelectionLabel || !msClient) return;
        const selected = msClient.getApplied();
        if (!selected.length) {
            elements.clearClientSelection.classList.remove('visible');
            elements.activeClientSelectionLabel.textContent = '';
            return;
        }
        const label = selected.length === 1
            ? msClient.getLabelForValue(selected[0])
            : `${selected.length} clientes`;
        elements.activeClientSelectionLabel.textContent = `Cliente: ${label}`;
        elements.clearClientSelection.classList.add('visible');
        elements.clearClientSelection.title = `Quitar filtro Cliente: ${label}`;
        elements.clearClientSelection.setAttribute('aria-label', `Quitar filtro Cliente: ${label}`);
    }

    function selectClientFromRanking(row) {
        if (!row || row.cliente_entidad_id === null || row.cliente_entidad_id === undefined) return;
        msClient.setApplied([String(row.cliente_entidad_id)]);
        updateActiveClientSelection();
        clearTimeout(_filterDebounceTimer);
        loadDashboardData();
    }

    function modalInstance(element) {
        return element && window.bootstrap?.Modal
            ? window.bootstrap.Modal.getOrCreateInstance(element)
            : null;
    }

    function setModalLoading(body, colspan, message = 'Cargando el conjunto completo...') {
        if (!body) return;
        body.innerHTML = '';
        const tr = document.createElement('tr');
        const td = document.createElement('td');
        td.colSpan = colspan;
        td.className = 'text-center text-muted py-4';
        const spinner = document.createElement('span');
        spinner.className = 'spinner-border spinner-border-sm me-2';
        spinner.setAttribute('aria-hidden', 'true');
        td.append(spinner, document.createTextNode(message));
        tr.appendChild(td);
        body.appendChild(tr);
    }

    function appendContextChip(container, label, value) {
        if (!container || !value) return;
        const chip = document.createElement('span');
        chip.className = 'dim-modal-context-chip';
        chip.textContent = `${label}: ${value}`;
        container.appendChild(chip);
    }

    function summarizeValues(values, control = null, max = 3) {
        const list = (Array.isArray(values) ? values : []).map(value => control ? control.getLabelForValue(value) : String(value));
        if (!list.length) return '';
        return list.length > max ? `${list.slice(0, max).join(', ')} +${list.length - max}` : list.join(', ');
    }

    function renderModalGlobalContext(container) {
        if (!container) return;
        container.replaceChildren();
        const label = document.createElement('span');
        label.className = 'dim-modal-context-label';
        label.textContent = 'Contexto global heredado:';
        container.appendChild(label);
        appendContextChip(container, 'Alcance', 'cartera y jerarquía del usuario');
        const params = buildQueryParams();
        appendContextChip(container, 'Cliente', summarizeValues(params.cliente_entidad_id, msClient));
        appendContextChip(container, 'Provincia', summarizeValues(params.provincia, msProvince));
        appendContextChip(container, 'Familia', summarizeValues(params.familia, msFamily));
        appendContextChip(container, 'Unidad', summarizeValues(params.unidad_negocio, msUnit));
        appendContextChip(container, 'Subunidad', summarizeValues(params.subunidad_negocio, msSubunit));
        appendContextChip(container, 'Plataforma', summarizeValues(params.plataforma));
        appendContextChip(container, 'Resultado', summarizeValues(params.resultado));
        appendContextChip(container, 'Negocios excluidos', summarizeValues(params.unidad_negocio_excluir, msUnit));
        if (params.fecha_desde || params.fecha_hasta) appendContextChip(container, 'Fechas', `${params.fecha_desde || 'inicio'} – ${params.fecha_hasta || 'fin'}`);
        if (params.is_client === 'true' || params.is_client === true) appendContextChip(container, '¿Cliente?', 'Cliente');
        if (params.is_client === 'false' || params.is_client === false) appendContextChip(container, '¿Cliente?', 'No cliente');
        if (container.children.length === 2) appendContextChip(container, 'Filtros', 'sin filtros adicionales');
    }

    function bindFullModalEvents() {
        if (elements.familyModalSearch) elements.familyModalSearch.addEventListener('input', () => {
            state.familyModal.search = elements.familyModalSearch.value;
            state.familyModal.page = 1;
            renderFamilyModal();
        });
        document.querySelectorAll('[data-family-modal-sort]').forEach(button => button.addEventListener('click', () => {
            const key = button.dataset.familyModalSort;
            state.familyModal.sortDirection = state.familyModal.sortKey === key
                ? (state.familyModal.sortDirection === 'asc' ? 'desc' : 'asc')
                : (key === 'familia' ? 'asc' : 'desc');
            state.familyModal.sortKey = key;
            state.familyModal.page = 1;
            renderFamilyModal();
        }));
        if (elements.familyModalClear) elements.familyModalClear.addEventListener('click', resetFamilyModalFilters);
        if (elements.familyModalPrev) elements.familyModalPrev.addEventListener('click', () => {
            state.familyModal.page = Math.max(1, state.familyModal.page - 1);
            renderFamilyModal();
        });
        if (elements.familyModalNext) elements.familyModalNext.addEventListener('click', () => {
            state.familyModal.page += 1;
            renderFamilyModal();
        });

        if (elements.clientModalSearch) elements.clientModalSearch.addEventListener('input', () => {
            state.clientModal.search = elements.clientModalSearch.value;
            state.clientModal.page = 1;
            renderClientModal();
        });
        [elements.clientModalCondition, elements.clientModalResult, elements.clientModalMetric].forEach(control => {
            if (!control) return;
            control.addEventListener('change', () => {
                state.clientModal.condition = elements.clientModalCondition.value;
                state.clientModal.result = elements.clientModalResult.value;
                state.clientModal.metric = elements.clientModalMetric.value === 'valorizacion' ? 'valorizacion' : 'renglones';
                state.clientModal.page = 1;
                renderClientModal();
            });
        });
        if (elements.clientModalClear) elements.clientModalClear.addEventListener('click', resetClientModalFilters);
        if (elements.clientModalHead) elements.clientModalHead.addEventListener('click', event => {
            const button = event.target.closest('[data-client-modal-sort]');
            if (!button) return;
            const key = button.dataset.clientModalSort;
            state.clientModal.sortDirection = state.clientModal.sortKey === key
                ? (state.clientModal.sortDirection === 'asc' ? 'desc' : 'asc')
                : (key === 'cliente' || key === 'condition' ? 'asc' : 'desc');
            state.clientModal.sortKey = key;
            state.clientModal.page = 1;
            renderClientModal();
        });
        if (elements.clientModalBody) elements.clientModalBody.addEventListener('click', event => {
            const button = event.target.closest('[data-select-client-id]');
            if (!button) return;
            const row = state.clientModal.rows.find(item => String(item.cliente_entidad_id) === button.dataset.selectClientId);
            if (!row) return;
            modalInstance(elements.clientFullModal)?.hide();
            selectClientFromRanking(row);
        });
        if (elements.clientModalPrev) elements.clientModalPrev.addEventListener('click', () => {
            state.clientModal.page = Math.max(1, state.clientModal.page - 1);
            renderClientModal();
        });
        if (elements.clientModalNext) elements.clientModalNext.addEventListener('click', () => {
            state.clientModal.page += 1;
            renderClientModal();
        });
    }

    async function openAllFamiliesModal() {
        state.familyModal.initial = { sortKey: state.familySortKey, sortDirection: state.familySortDirection };
        state.familyModal.rows = [];
        resetFamilyModalFilters();
        renderModalGlobalContext(elements.familyModalContext);
        setModalLoading(elements.familyModalBody, 3);
        modalInstance(elements.familyFullModal)?.show();
        try {
            const response = await apiGet('/families-complete', buildQueryParams());
            state.familyModal.rows = Array.isArray(response.data) ? response.data : [];
            renderFamilyModal();
        } catch (error) {
            showModalError(elements.familyModalBody, 3, error.message);
        }
    }

    function resetFamilyModalFilters() {
        const initial = state.familyModal.initial || { sortKey: state.familySortKey, sortDirection: state.familySortDirection };
        state.familyModal.search = '';
        state.familyModal.sortKey = initial.sortKey;
        state.familyModal.sortDirection = initial.sortDirection;
        state.familyModal.page = 1;
        if (elements.familyModalSearch) elements.familyModalSearch.value = '';
        renderFamilyModal();
    }

    function renderFamilyModal() {
        if (!elements.familyModalBody) return;
        const term = normalizePivotSearch(state.familyModal.search);
        const filtered = state.familyModal.rows.filter(row => !term || normalizePivotSearch(row.familia).includes(term));
        const sorted = sortFamilyRows(filtered, state.familyModal.sortKey, state.familyModal.sortDirection);
        const totalPages = Math.max(1, Math.ceil(sorted.length / MODAL_PAGE_SIZE));
        state.familyModal.page = Math.min(Math.max(1, state.familyModal.page), totalPages);
        const start = (state.familyModal.page - 1) * MODAL_PAGE_SIZE;
        const pageRows = sorted.slice(start, start + MODAL_PAGE_SIZE);
        elements.familyModalBody.replaceChildren();
        if (!pageRows.length) appendEmptyModalRow(elements.familyModalBody, 3);
        pageRows.forEach(row => {
            const tr = document.createElement('tr');
            tr.append(
                modalTextCell(row.familia || 'Sin familia', 'dim-modal-family-name'),
                modalTextCell(formatInteger(row.renglones), 'text-end'),
                modalTextCell(formatDecimal(row.cantidad), 'text-end fw-bold')
            );
            elements.familyModalBody.appendChild(tr);
        });
        updateModalSortArrows('[data-family-modal-sort]', state.familyModal.sortKey, state.familyModal.sortDirection);
        updatePagination(elements.familyModalCounter, elements.familyModalPage, elements.familyModalPrev, elements.familyModalNext, start, pageRows.length, sorted.length, state.familyModal.page, totalPages);
    }

    async function openAllClientsModal() {
        state.clientModal.initial = {
            search: '', condition: '', result: '', metric: state.activeMetric,
            sortKey: state.clientSortKey, sortDirection: state.clientSortDirection,
        };
        state.clientModal.rows = [];
        state.clientModal.resultKeys = [];
        resetClientModalFilters();
        renderModalGlobalContext(elements.clientModalContext);
        setModalLoading(elements.clientModalBody, 5);
        modalInstance(elements.clientFullModal)?.show();
        try {
            const response = await apiGet('/clients-complete', buildQueryParams());
            state.clientModal.rows = Array.isArray(response.data) ? response.data : [];
            state.clientModal.resultKeys = Array.from(new Set(state.clientModal.rows.flatMap(row => [
                ...Object.keys(row.resultados || {}), ...Object.keys(row.resultados_val || {}),
            ]))).sort((left, right) => normalizeKey(left).localeCompare(normalizeKey(right)));
            populateClientModalResults();
            renderClientModal();
        } catch (error) {
            showModalError(elements.clientModalBody, 5, error.message);
        }
    }

    function populateClientModalResults() {
        if (!elements.clientModalResult) return;
        const current = state.clientModal.result;
        elements.clientModalResult.innerHTML = '<option value="">Todos los resultados</option>';
        state.clientModal.resultKeys.forEach(result => {
            const option = document.createElement('option');
            option.value = result;
            option.textContent = result;
            elements.clientModalResult.appendChild(option);
        });
        elements.clientModalResult.value = state.clientModal.resultKeys.includes(current) ? current : '';
        state.clientModal.result = elements.clientModalResult.value;
    }

    function resetClientModalFilters() {
        const initial = state.clientModal.initial || {
            search: '', condition: '', result: '', metric: state.activeMetric,
            sortKey: state.clientSortKey, sortDirection: state.clientSortDirection,
        };
        Object.assign(state.clientModal, initial, { page: 1 });
        if (elements.clientModalSearch) elements.clientModalSearch.value = state.clientModal.search;
        if (elements.clientModalCondition) elements.clientModalCondition.value = state.clientModal.condition;
        if (elements.clientModalMetric) elements.clientModalMetric.value = state.clientModal.metric;
        if (elements.clientModalResult) elements.clientModalResult.value = state.clientModal.result;
        renderClientModal();
    }

    function clientHasResult(row, result) {
        if (!result) return true;
        return Object.prototype.hasOwnProperty.call(row.resultados || {}, result)
            || Object.prototype.hasOwnProperty.call(row.resultados_val || {}, result);
    }

    function renderClientModal() {
        if (!elements.clientModalBody || !elements.clientModalHead) return;
        const term = normalizePivotSearch(state.clientModal.search);
        const filtered = state.clientModal.rows.filter(row => {
            if (term && !normalizePivotSearch(row.cliente).includes(term)) return false;
            if (state.clientModal.condition === 'true' && !row.is_client) return false;
            if (state.clientModal.condition === 'false' && row.is_client) return false;
            return clientHasResult(row, state.clientModal.result);
        });
        const sorted = sortClientRows(filtered, state.clientModal.sortKey, state.clientModal.sortDirection, state.clientModal.metric);
        const totalPages = Math.max(1, Math.ceil(sorted.length / MODAL_PAGE_SIZE));
        state.clientModal.page = Math.min(Math.max(1, state.clientModal.page), totalPages);
        const start = (state.clientModal.page - 1) * MODAL_PAGE_SIZE;
        const pageRows = sorted.slice(start, start + MODAL_PAGE_SIZE);
        renderClientModalHeader();
        elements.clientModalBody.replaceChildren();
        if (!pageRows.length) appendEmptyModalRow(elements.clientModalBody, state.clientModal.resultKeys.length + 4);
        pageRows.forEach(row => {
            const tr = document.createElement('tr');
            tr.appendChild(modalTextCell(row.cliente || 'Sin cliente', 'dim-modal-client-name'));
            const condition = modalTextCell('', 'text-center');
            const badge = document.createElement('span');
            badge.className = 'dim-condition-badge';
            badge.textContent = row.is_client ? 'Cliente' : 'No cliente';
            condition.appendChild(badge);
            tr.appendChild(condition);
            state.clientModal.resultKeys.forEach(result => {
                tr.appendChild(modalTextCell(formatClientMetric(clientMetricTotal(row, state.clientModal.metric, result), state.clientModal.metric), 'text-end'));
            });
            tr.appendChild(modalTextCell(formatClientMetric(clientMetricTotal(row, state.clientModal.metric), state.clientModal.metric), 'text-end fw-bold'));
            const action = modalTextCell('', 'text-center');
            const select = document.createElement('button');
            select.type = 'button';
            select.className = 'dim-select-client-btn';
            select.dataset.selectClientId = row.cliente_entidad_id;
            select.textContent = 'Seleccionar';
            select.title = `Aplicar ${row.cliente || 'cliente'} al filtro global Cliente`;
            if (row.cliente_entidad_id === null || row.cliente_entidad_id === undefined) select.disabled = true;
            action.appendChild(select);
            tr.appendChild(action);
            elements.clientModalBody.appendChild(tr);
        });
        updatePagination(elements.clientModalCounter, elements.clientModalPage, elements.clientModalPrev, elements.clientModalNext, start, pageRows.length, sorted.length, state.clientModal.page, totalPages);
    }

    function renderClientModalHeader() {
        elements.clientModalHead.replaceChildren();
        elements.clientModalHead.appendChild(modalSortHeader('Cliente', 'cliente'));
        elements.clientModalHead.appendChild(modalSortHeader('Condición', 'condition', 'text-center'));
        state.clientModal.resultKeys.forEach(result => {
            const th = modalSortHeader(result, `result:${result}`, 'text-end dim-result-head');
            th.style.setProperty('--result-color', resultColor(result));
            elements.clientModalHead.appendChild(th);
        });
        elements.clientModalHead.appendChild(modalSortHeader(state.clientModal.metric === 'valorizacion' ? 'Total valorización' : 'Total renglones', 'total', 'text-end'));
        const action = document.createElement('th');
        action.className = 'text-center';
        action.textContent = 'Acción';
        elements.clientModalHead.appendChild(action);
    }

    function modalSortHeader(label, key, className = '') {
        const th = document.createElement('th');
        th.className = className;
        const button = document.createElement('button');
        button.type = 'button';
        button.className = 'dim-sort-button';
        button.dataset.clientModalSort = key;
        button.append(document.createTextNode(`${label} `));
        const arrow = document.createElement('span');
        arrow.className = 'dim-sort-arrow';
        arrow.textContent = sortArrow(key, state.clientModal.sortKey, state.clientModal.sortDirection);
        button.appendChild(arrow);
        th.appendChild(button);
        return th;
    }

    function formatClientMetric(value, metric) {
        return metric === 'valorizacion' ? `$ ${formatDecimal(value)}` : formatInteger(value);
    }

    function modalTextCell(value, className = '') {
        const td = document.createElement('td');
        td.className = className;
        td.textContent = value;
        return td;
    }

    function appendEmptyModalRow(body, colspan) {
        const tr = document.createElement('tr');
        const td = modalTextCell('No hay registros que coincidan con la búsqueda y los filtros.', 'text-center text-muted py-4');
        td.colSpan = colspan;
        tr.appendChild(td);
        body.appendChild(tr);
    }

    function showModalError(body, colspan, message) {
        if (!body) return;
        body.replaceChildren();
        const tr = document.createElement('tr');
        const td = modalTextCell(`No se pudo cargar el conjunto completo: ${message || 'error desconocido'}`, 'text-center text-danger py-4');
        td.colSpan = colspan;
        tr.appendChild(td);
        body.appendChild(tr);
    }

    function updateModalSortArrows(selector, activeKey, direction) {
        document.querySelectorAll(selector).forEach(button => {
            const active = button.dataset.familyModalSort === activeKey;
            const arrow = button.querySelector('.dim-sort-arrow');
            if (arrow) arrow.textContent = active ? sortArrow(activeKey, activeKey, direction) : '';
        });
    }

    function updatePagination(counter, pageLabel, prev, next, start, pageLength, total, page, totalPages) {
        if (counter) counter.textContent = `Mostrando ${total ? start + 1 : 0}–${start + pageLength} de ${total}`;
        if (pageLabel) pageLabel.textContent = `Página ${page} de ${totalPages}`;
        if (prev) prev.disabled = page <= 1;
        if (next) next.disabled = page >= totalPages;
    }
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
        // Mostrar la estructura del dashboard inmediatamente — sin pantalla bloqueante.
        // El botón "Actualizar vista" muestra el spinner mientras los datos llegan.
        elements.emptyState.style.display = 'none';
        elements.dashboardContent.style.display = 'contents';
        setRefreshing(true);
        try {
            const initialQuery = buildQueryParams();
            const [bootstrapResponse, labelsResponse] = await Promise.all([
                apiGet('/bootstrap', forceLive ? { ...initialQuery, bypass_snapshot: true } : initialQuery),
                apiGet('/negocio-labels').catch(() => ({ data: { unidades: {}, subunidades: {} } })),
            ]);
            const bootstrap = bootstrapResponse.data || {};
            const status = bootstrap.status || {};
            renderStatus(status);

            if (labelsResponse && labelsResponse.data) {
                state.negocioLabels = labelsResponse.data;
            }

            if (!status.has_data) {
                // Cartera de cuentas (ago-2026): distingue "sin cartera asignada" del
                // genérico "no hay datos cargados" — nunca pantalla en blanco sin
                // explicación (ver informe de auditoría 2026-08-19).
                const emptyTitle = elements.emptyState.querySelector('h3');
                const emptyBody = elements.emptyState.querySelector('p');
                if (status.cartera_blocked) {
                    if (emptyTitle) emptyTitle.textContent = 'No tenés cuentas asignadas';
                    if (emptyBody) emptyBody.textContent =
                        'No tenés cuentas asignadas. Contactá al administrador.';
                } else if (emptyTitle && emptyBody) {
                    emptyTitle.textContent = 'No hay datos cargados';
                    emptyBody.textContent =
                        'Los datos de Dimensionamiento aún no han sido cargados en la base de datos. ' +
                        'El sistema los cargará automáticamente en el próximo inicio si el dataset está configurado.';
                }
                elements.emptyState.style.display = 'block';
                elements.dashboardContent.style.display = 'none';
                return;
            }

            assertBootstrapSupportsValorizacion(bootstrap);

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
            setRefreshing(false);
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

        state.lastBootstrap = bootstrap;
        renderKpis(bootstrap.kpis || {});
        renderAreaChart(bootstrap.series || { months: [], datasets: [] });
        renderPieChart(bootstrap.results || []);
        renderFamilyList(bootstrap.top_families || []);
        renderMapChart(bootstrap.geo || []);
        renderBarClientChart(bootstrap.clients_by_result || []);
        renderPivotTable(bootstrap.family_consumption || { months: [], rows: [], total: 0 });
    }

    function _reRenderWithCurrentMetric() {
        const b = state.lastBootstrap;
        if (!b) return;
        renderKpis(b.kpis || {});
        renderAreaChart(b.series || { months: [], datasets: [] });
        renderPieChart(b.results || []);
        renderFamilyList(b.top_families || []);
        renderMapChart(b.geo || []);
        renderBarClientChart(b.clients_by_result || []);
        renderPivotTable(b.family_consumption || { months: [], rows: [], total: 0 });
    }

    // ─────────────────────────────────────────────────────────────────────────
    // applyFilterOptions — poblamos los multi-select con opciones del backend
    // ─────────────────────────────────────────────────────────────────────────
    function applyFilterOptions(data) {
        if (msClient) msClient.setOptions(data.clientes || []);
        updateActiveClientSelection();
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

        const rawFechaDesde = dateRangeCtrl.getAppliedMin();
        const rawFechaHasta = dateRangeCtrl.getAppliedMax();

        return {
            cliente_entidad_id:     normalizeMultiSelectParam('cliente_entidad_id', msClient),
            provincia:              normalizeMultiSelectParam('provincia', msProvince),
            familia:                normalizeMultiSelectParam('familia', msFamily),
            unidad_negocio:         normalizeMultiSelectParam('unidad_negocio', msUnit),
            unidad_negocio_excluir: state.hiddenSeriesCodes.size > 0 ? [...state.hiddenSeriesCodes] : [],
            resultado:              state.activeResultados.size  > 0 ? [...state.activeResultados]  : [],
            subunidad_negocio:      normalizeMultiSelectParam('subunidad_negocio', msSubunit),
            plataforma:             normalizeMultiFilterForQuery(plataformas, elements.platformCheckboxes.map(cb => cb.value), 'plataforma'),
            fecha_desde:            rawFechaDesde,
            fecha_hasta:            rawFechaHasta,
            is_client:  elements.filterIsClient ? (elements.filterIsClient.value || null) : null,
        };
    }

    function normalizeMultiSelectParam(name, control) {
        if (!control) return [];
        // Importante: medimos "todos seleccionados" contra el UNIVERSO completo, no contra
        // las opciones visibles (que el backend puede estrechar cuando hay otro filtro activo).
        // Si usáramos las opciones estrechadas, un filtro aplicado podría verse como "Todos"
        // y descartarse silenciosamente al aplicar otro filtro.
        return normalizeMultiFilterForQuery(control.getApplied(), control.getUniverseValues(), name);
    }

    function normalizeMultiFilterForQuery(values, allOptions = [], name = '') {
        const arr = Array.isArray(values) ? values : [];
        const clean = arr
            .map(value => String(value || '').trim())
            .filter(Boolean)
            .filter(value => !isNoFilterToken(value));

        const cleaned = [];
        const seen = new Set();
        clean.forEach(value => {
            const key = value.toLowerCase();
            if (seen.has(key)) return;
            seen.add(key);
            cleaned.push(value);
        });

        if (cleaned.length === 0) return [];

        const all = Array.isArray(allOptions)
            ? allOptions
                .map(value => String(value ?? '').trim())
                .filter(Boolean)
            : [];
        const allUnique = [];
        const allSeen = new Set();
        all.forEach(value => {
            const key = value.toLowerCase();
            if (allSeen.has(key)) return;
            allSeen.add(key);
            allUnique.push(value);
        });

        const selectedAllAvailable = allUnique.length > 0 && cleaned.length >= allUnique.length;

        if (selectedAllAvailable) {
            logQueryNormalization(name, cleaned.length, allUnique.length);
            return [];
        }

        return cleaned;
    }

    function isNoFilterToken(value) {
        const text = String(value || '').trim();
        return NO_FILTER_TOKENS.has(text) || NO_FILTER_TOKENS.has(text.toLowerCase());
    }

    function logQueryNormalization(name, selectedCount, totalCount) {
        if (isDimQueryDebugEnabled()) {
            console.info('[DIM] filtro omitido por representar Todos', {
                filtro: name,
                seleccionados: selectedCount,
                disponibles: totalCount,
            });
        }
    }

    function isDimQueryDebugEnabled() {
        try {
            return window.localStorage && window.localStorage.getItem('dimQueryDebug') === '1';
        } catch (_) {
            return false;
        }
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
        const cacheAgeMs = !force ? (Date.now() - (state.bootstrapCacheTs.get(cacheKey) || 0)) : Infinity;
        // Si el cache tiene menos de 55 segundos no hace falta ir al backend: el
        // TTL del cache del servidor es 120s, así que los datos siguen siendo frescos.
        const CACHE_FRESH_TTL_MS = 55_000;

        if (blocking) {
            setLoading(true, 'Consultando metricas agregadas...');
        } else {
            setLoading(false);
            setRefreshing(true);
        }

        if (cachedPayload) {
            try {
                assertBootstrapSupportsValorizacion(cachedPayload);
                renderBootstrapPayload(cachedPayload);
            } catch (cacheError) {
                state.bootstrapCache.delete(cacheKey);
                state.bootstrapCacheTs.delete(cacheKey);
                console.warn('[DIM] bootstrap cache descartado:', cacheError);
            }
        }

        if (!force && cachedPayload && cacheAgeMs < CACHE_FRESH_TTL_MS) {
            if (blocking) setLoading(false);
            setRefreshing(false);
            return;
        }

        try {
            const response = await apiGet('/bootstrap', {
                ...query,
                include_status: false,
                bypass_snapshot: bypassSnapshot,
            }, signal);

            if (signal.aborted) return;

            const bootstrap = (response && response.data) || {};
            assertBootstrapSupportsValorizacion(bootstrap);
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
        if (state.bootstrapCache.has(key)) {
            state.bootstrapCache.delete(key);
            state.bootstrapCacheTs.delete(key);
        }
        state.bootstrapCache.set(key, payload);
        state.bootstrapCacheTs.set(key, Date.now());
        while (state.bootstrapCache.size > 12) {
            const oldKey = state.bootstrapCache.keys().next().value;
            state.bootstrapCache.delete(oldKey);
            state.bootstrapCacheTs.delete(oldKey);
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
        [elements.kpiClients, elements.kpiRecords, elements.kpiFamilies, elements.kpiProvinces, elements.kpiValorizacion]
            .forEach(el => { if (el) el.textContent = '--'; });
        if (elements.kpiClientsBreak) elements.kpiClientsBreak.textContent = '';
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
        state.pivotLastRenderKey = '';
        const colspan = Math.max((state.pivotData?.months?.length || 0) + 1, 1);
        if (elements.pivotBody) {
            elements.pivotBody.innerHTML = `<tr><td colspan="${colspan}" class="text-center text-muted py-3 small">${msg || 'No disponible temporalmente'}</td></tr>`;
        }
        if (elements.pivotSearchCount) {
            elements.pivotSearchCount.style.display = 'none';
            elements.pivotSearchCount.textContent = '';
        }
    }

    // ─────────────────────────────────────────────────────────────────────────
    // apiGet
    // ─────────────────────────────────────────────────────────────────────────
    async function apiGet(path, params = {}, signal = undefined) {
        const safeParams = normalizeParamsForQuery(params);
        const { url, queryString } = buildRequestUrl(path, safeParams);

        if (shouldUsePostForFilters(safeParams, url)) {
            logRequestMethod('POST', path, safeParams, queryString, url.length);
            return apiPost(path, safeParams, signal);
        }

        logRequestMethod('GET', path, safeParams, queryString, url.length);
        const fetchOptions = { headers: { Accept: 'application/json' } };
        if (signal) fetchOptions.signal = signal;
        const response = await fetch(url, fetchOptions);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.ok === false) throw new Error(payload.detail || payload.message || `Error HTTP ${response.status}`);
        return payload;
    }

    async function apiPost(path, params = {}, signal = undefined) {
        const url = `/api/mercado-privado/dimensiones${path}`;
        const fetchOptions = {
            method: 'POST',
            headers: {
                Accept: 'application/json',
                'Content-Type': 'application/json',
            },
            credentials: 'same-origin',
            body: JSON.stringify(params || {}),
        };
        if (signal) fetchOptions.signal = signal;
        const response = await fetch(url, fetchOptions);
        const payload = await response.json().catch(() => ({}));
        if (!response.ok || payload.ok === false) throw new Error(payload.detail || payload.message || `Error HTTP ${response.status}`);
        return payload;
    }

    function buildRequestUrl(path, params = {}) {
        const query = new URLSearchParams();
        Object.entries(params || {}).forEach(([key, value]) => {
            if (value === undefined || value === null || value === '') return;
            if (Array.isArray(value)) {
                value.forEach(item => {
                    if (item !== undefined && item !== null && item !== '') query.append(key, item);
                });
                return;
            }
            query.append(key, value);
        });
        const queryString = query.toString();
        const url = `/api/mercado-privado/dimensiones${path}${queryString ? `?${queryString}` : ''}`;
        return { url, queryString };
    }

    function shouldUsePostForFilters(params, candidateUrl) {
        if (candidateUrl && candidateUrl.length > POST_URL_LENGTH_THRESHOLD) return true;
        return Array.from(MULTI_FILTER_QUERY_KEYS).some(key =>
            Array.isArray(params[key]) && params[key].length > POST_ARRAY_LENGTH_THRESHOLD
        );
    }

    function normalizeParamsForQuery(params = {}) {
        const normalized = {};
        Object.entries(params || {}).forEach(([key, value]) => {
            if (MULTI_FILTER_QUERY_KEYS.has(key)) {
                normalized[key] = normalizeMultiFilterForQuery(value, getAllOptionsForQueryKey(key), key);
                return;
            }
            normalized[key] = value;
        });
        return normalized;
    }

    function getAllOptionsForQueryKey(key) {
        // Universo completo (monotónico), no las opciones estrechadas: misma razón que en
        // normalizeMultiSelectParam — evita el colapso accidental de un filtro a "Todos".
        if (key === 'familia' && msFamily) return msFamily.getUniverseValues();
        if (key === 'cliente_entidad_id' && msClient) return msClient.getUniverseValues();
        if (key === 'provincia' && msProvince) return msProvince.getUniverseValues();
        if (key === 'unidad_negocio' && msUnit) return msUnit.getUniverseValues();
        if (key === 'subunidad_negocio' && msSubunit) return msSubunit.getUniverseValues();
        if (key === 'plataforma') return elements.platformCheckboxes.map(cb => cb.value);

        const filters = state.lastBootstrap && state.lastBootstrap.filters ? state.lastBootstrap.filters : {};
        if (key === 'familia') return filters.familias || [];
        if (key === 'cliente_entidad_id') return (filters.clientes || []).map(o => (o && typeof o === 'object') ? o.value : o);
        if (key === 'provincia') return filters.provincias || [];
        if (key === 'unidad_negocio') return filters.unidades_negocio || [];
        if (key === 'subunidad_negocio') return filters.subunidades_negocio || [];
        if (key === 'plataforma') return filters.plataformas || [];
        return [];
    }

    function logRequestMethod(method, path, params, queryString, urlLength) {
        const payload = {
            endpoint: path,
            method,
            urlLength,
            queryLength: queryString.length,
            familia: Array.isArray(params.familia) ? params.familia.length : 0,
            cliente_entidad_id: Array.isArray(params.cliente_entidad_id) ? params.cliente_entidad_id.length : 0,
            provincia: Array.isArray(params.provincia) ? params.provincia.length : 0,
            unidad_negocio: Array.isArray(params.unidad_negocio) ? params.unidad_negocio.length : 0,
            subunidad_negocio: Array.isArray(params.subunidad_negocio) ? params.subunidad_negocio.length : 0,
            plataforma: Array.isArray(params.plataforma) ? params.plataforma.length : 0,
        };

        if (method === 'POST') {
            console.info('[DIM] request grande: usando POST', payload);
            return;
        }

        if (urlLength > QUERY_WARN_LENGTH) {
            console.warn('[DIM] query extensa antes del request GET', payload);
            return;
        }

        if (isDimQueryDebugEnabled()) {
            console.debug('[DIM] query normalizada', payload);
        }
    }

    function assertBootstrapSupportsValorizacion(bootstrap) {
        if (!bootstrap || typeof bootstrap !== 'object') {
            throw new Error('El bootstrap de Dimensionamiento es invalido.');
        }

        const kpis = bootstrap.kpis || {};
        const seriesDatasets = (bootstrap.series && bootstrap.series.datasets) || [];
        const results = bootstrap.results || [];
        const topFamilies = bootstrap.top_families || [];
        const geo = bootstrap.geo || [];
        const clients = bootstrap.clients_by_result || [];
        const familyConsumptionRows = ((bootstrap.family_consumption || {}).rows) || [];

        const supportsValorizacion =
            Object.prototype.hasOwnProperty.call(kpis, 'valorizacion') &&
            (seriesDatasets.length === 0 || Object.prototype.hasOwnProperty.call(seriesDatasets[0], 'valorizacion')) &&
            (results.length === 0 || Object.prototype.hasOwnProperty.call(results[0], 'valorizacion')) &&
            (topFamilies.length === 0 || Object.prototype.hasOwnProperty.call(topFamilies[0], 'valorizacion')) &&
            (geo.length === 0 || Object.prototype.hasOwnProperty.call(geo[0], 'valorizacion')) &&
            (clients.length === 0 || Object.prototype.hasOwnProperty.call(clients[0], 'resultados_val')) &&
            (familyConsumptionRows.length === 0 || Object.prototype.hasOwnProperty.call(familyConsumptionRows[0], 'valorizacion'));

        if (!supportsValorizacion) {
            throw new Error('El bootstrap recibido no incluye soporte consistente para valorizacion.');
        }
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
        // Aviso de FALLBACK (identidad no resuelta) — el elemento solo existe para Admin.
        if (elements.identidadFallbackPill) {
            const fallback = kpis.entities_resolved === false;
            elements.identidadFallbackPill.style.display = fallback ? '' : 'none';
        }
        if (elements.kpiClientsBreak) {
            // Desglose del universo de entidades: clientes (Sí) · no clientes (No).
            // Respeta los filtros activos igual que el número grande.
            const si = kpis.clientes_si;
            const no = kpis.clientes_no;
            if (typeof si === 'number' && typeof no === 'number') {
                elements.kpiClientsBreak.textContent = `${formatInteger(si)} clientes · ${formatInteger(no)} no clientes`;
            } else {
                elements.kpiClientsBreak.textContent = '';
            }
        }
        elements.kpiRecords.textContent  = formatInteger(kpis.renglones || 0);
        elements.kpiFamilies.textContent = formatInteger(kpis.familias || 0);
        if (elements.kpiProvinces) {
            elements.kpiProvinces.textContent = formatInteger(kpis.provincias || 0);
        }
        if (elements.kpiValorizacion) {
            elements.kpiValorizacion.textContent = formatAbbreviated(kpis.valorizacion || 0);
        }
        if (elements.kpiValorizacionCard) {
            elements.kpiValorizacionCard.style.display = state.activeMetric === 'valorizacion' ? '' : 'none';
        }
    }

    function renderAreaChart(series) {
        // Guardar los códigos originales (pre-resolución) de cada dataset.
        state.chartSeriesCodes = (series.datasets || []).map(d => String(d.label));

        const useVal = state.activeMetric === 'valorizacion';
        const datasets = (series.datasets || []).map((dataset, index) => ({
            label: resolveUnitLabel(dataset.label),
            data: useVal ? (dataset.valorizacion || dataset.values || []) : (dataset.values || []),
            backgroundColor: seriesPalette[index % seriesPalette.length],
            borderColor: seriesPalette[index % seriesPalette.length],
            borderWidth: 0,
            borderRadius: 2,
            stack: 'negocio',
            hidden: state.hiddenSeriesCodes.has(String(dataset.label)),
        }));

        if (state.areaChart) {
            // Chart.js no permite cambiar el type via update(); si ya existe
            // y es de tipo 'line' (instancia previa), destruir y recrear.
            if (state.areaChart.config.type !== 'bar') {
                state.areaChart.destroy();
                state.areaChart = null;
            } else {
                state.areaChart.data.labels = series.months || [];
                state.areaChart.data.datasets = datasets;
                state.areaChart.update('none');
                return;
            }
        }

        const ctx = document.getElementById('areaChart').getContext('2d');
        state.areaChart = new Chart(ctx, {
            type: 'bar',
            data: { labels: series.months || [], datasets },
            options: buildStackedBarChartOptions(),
        });
    }


    function renderPieChart(results) {
        const hasFilter = state.activeResultados.size > 0;
        const useVal = state.activeMetric === 'valorizacion';
        const labels = results.map(item => item.resultado);
        const data = results.map(item => useVal ? (item.valorizacion || 0) : (item.renglones || 0));
        const backgroundColor = results.map((item, i) => {
            const base = resultColor(item.resultado);
            return (hasFilter && !state.activeResultados.has(item.resultado)) ? base + '38' : base;
        });
        const offset = results.map(item =>
            hasFilter && state.activeResultados.has(item.resultado) ? 8 : 0
        );

        if (state.pieChart) {
            state.pieChart.data.labels = labels;
            state.pieChart.data.datasets[0].data = data;
            state.pieChart.data.datasets[0].backgroundColor = backgroundColor;
            state.pieChart.data.datasets[0].offset = offset;
            state.pieChart.update('none');
            return;
        }

        const ctx = document.getElementById('pieChart').getContext('2d');
        state.pieChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels,
                datasets: [{ data, backgroundColor, borderWidth: 0, hoverOffset: 8, offset }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '68%',
                plugins: {
                    legend: { position: 'bottom', labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } } },
                    tooltip: {
                        callbacks: {
                            label: ctx => state.activeMetric === 'valorizacion'
                                ? `${ctx.label}: ${formatAbbreviated(ctx.parsed)}`
                                : `${ctx.label}: ${formatInteger(ctx.parsed)} renglones`,
                        },
                    },
                },
                onClick(event, elements) {
                    if (!elements.length) {
                        if (state.activeResultados.size > 0) {
                            state.activeResultados.clear();
                            triggerLoad();
                        }
                        return;
                    }
                    const index = elements[0].index;
                    // Leemos el resultado desde chart.data.labels para no depender
                    // del closure sobre `results` (que queda stale tras la primera creación).
                    const resultado = state.pieChart.data.labels[index];
                    if (!resultado) return;
                    if (state.activeResultados.has(resultado)) {
                        state.activeResultados.delete(resultado);
                    } else {
                        state.activeResultados.clear();
                        state.activeResultados.add(resultado);
                    }
                    triggerLoad();
                },
            },
        });
    }

    function sortArrow(key, activeKey, direction) {
        return key === activeKey ? (direction === 'asc' ? '↑' : '↓') : '';
    }

    function compareText(left, right) {
        return String(left || '').localeCompare(String(right || ''), 'es', { sensitivity: 'base' });
    }

    function sortFamilyRows(rows, key, direction) {
        const sortKey = ['familia', 'cantidad', 'renglones'].includes(key) ? key : 'renglones';
        const factor = direction === 'asc' ? 1 : -1;
        return (Array.isArray(rows) ? rows : []).slice().sort((left, right) => {
            const diff = sortKey === 'familia'
                ? compareText(left.familia, right.familia)
                : Number(left[sortKey] || 0) - Number(right[sortKey] || 0);
            return (diff * factor) || compareText(left.familia, right.familia);
        });
    }

    function updateFamilySortSwitch() {
        if (!elements.familySortSwitch) return;
        elements.familySortSwitch.querySelectorAll('[data-family-sort]').forEach(item => {
            const active = item.dataset.familySort === state.familySortKey;
            item.classList.toggle('active', active);
            item.setAttribute('aria-pressed', String(active));
            const label = item.dataset.familySort === 'cantidad' ? 'Cantidades' : 'Renglones';
            item.textContent = active ? `${label} ${sortArrow(item.dataset.familySort, state.familySortKey, state.familySortDirection)}` : label;
        });
    }

    function setFamilyCardSort(key) {
        const nextKey = ['familia', 'cantidad', 'renglones'].includes(key) ? key : 'renglones';
        state.familySortDirection = state.familySortKey === nextKey
            ? (state.familySortDirection === 'asc' ? 'desc' : 'asc')
            : (nextKey === 'familia' ? 'asc' : 'desc');
        state.familySortKey = nextKey;
        updateFamilySortSwitch();
        renderFamilyList((state.lastBootstrap || {}).top_families || []);
    }

    function renderFamilyList(families) {
        const fullySorted = sortFamilyRows(families, state.familySortKey, state.familySortDirection);
        state.familyListData = fullySorted.slice(0, FAMILY_CARD_LIMIT);
        state.familyListLastRenderKey = '';
        updateFamilySortSwitch();

        if (!elements.familyListContainer) return;
        elements.familyListContainer.scrollTop = 0;
        if (!state.familyListData.length) {
            state.familyListBody = null;
            elements.familyListContainer.innerHTML = '<div class="text-center text-muted py-4 small">No hay datos para mostrar.</div>';
            return;
        }

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
                    <th class="dim-family-head dim-family-head-name ${state.familySortKey === 'familia' ? 'dim-family-sort-active' : ''}"><button type="button" class="dim-sort-button" data-family-card-sort="familia">Familia <span class="dim-sort-arrow">${sortArrow('familia', state.familySortKey, state.familySortDirection)}</span></button></th>
                    <th class="dim-family-head dim-family-head-count text-end ${state.familySortKey === 'renglones' ? 'dim-family-sort-active' : ''}"><button type="button" class="dim-sort-button" data-family-card-sort="renglones">Renglones <span class="dim-sort-arrow">${sortArrow('renglones', state.familySortKey, state.familySortDirection)}</span></button></th>
                    <th class="dim-family-head dim-family-head-qty text-end ${state.familySortKey === 'cantidad' ? 'dim-family-sort-active' : ''}"><button type="button" class="dim-sort-button" data-family-card-sort="cantidad">Cantidad <span class="dim-sort-arrow">${sortArrow('cantidad', state.familySortKey, state.familySortDirection)}</span></button></th>
                </tr>
            </thead>
        `;
        const tbody = document.createElement('tbody');
        table.appendChild(tbody);
        state.familyListBody = tbody;
        elements.familyListContainer.innerHTML = '';
        elements.familyListContainer.appendChild(table);
        scheduleFamilyListRender(true);
    }
    function scheduleFamilyListRender(force = false) {
        if (!state.familyListBody) return;

        if (force) {
            if (state.familyListRenderRaf) {
                window.cancelAnimationFrame(state.familyListRenderRaf);
                state.familyListRenderRaf = null;
            }
            renderVisibleFamilyRows();
            return;
        }

        if (state.familyListRenderRaf) return;
        state.familyListRenderRaf = window.requestAnimationFrame(() => {
            state.familyListRenderRaf = null;
            renderVisibleFamilyRows();
        });
    }

    function renderVisibleFamilyRows() {
        if (!state.familyListBody || !elements.familyListContainer) return;

        const rows = state.familyListData || [];
        if (!rows.length) {
            state.familyListLastRenderKey = 'empty';
            state.familyListBody.innerHTML = '';
            return;
        }

        const viewportHeight = Math.max(elements.familyListContainer.clientHeight || 0, FAMILY_LIST_ROW_HEIGHT);
        const visibleCount = Math.max(1, Math.ceil(viewportHeight / FAMILY_LIST_ROW_HEIGHT));
        const scrollTop = elements.familyListContainer.scrollTop || 0;
        const start = Math.max(0, Math.floor(scrollTop / FAMILY_LIST_ROW_HEIGHT) - FAMILY_LIST_OVERSCAN);
        const end = Math.min(rows.length, start + visibleCount + (FAMILY_LIST_OVERSCAN * 2));
        const renderKey = `${start}:${end}:${rows.length}:${state.familySortKey}:${state.familySortDirection}`;

        if (renderKey === state.familyListLastRenderKey) return;
        state.familyListLastRenderKey = renderKey;

        const fragment = document.createDocumentFragment();
        const topSpacerHeight = start * FAMILY_LIST_ROW_HEIGHT;
        const bottomSpacerHeight = Math.max(0, (rows.length - end) * FAMILY_LIST_ROW_HEIGHT);

        if (topSpacerHeight > 0) {
            fragment.appendChild(createFamilyListSpacerRow(topSpacerHeight));
        }

        rows.slice(start, end).forEach(item => {
            const tr = document.createElement('tr');
            tr.className = 'dim-family-row';
            const qtyDisplay = formatDecimal(item.cantidad || 0);
            tr.innerHTML = `
                <td class="dim-family-name-cell" title="${item.familia || ''}">
                    <div class="dim-family-name-text">${item.familia || 'Sin familia'}</div>
                </td>
                <td class="text-end small text-muted dim-family-number">${formatInteger(item.renglones)}</td>
                <td class="text-end fw-bold dim-family-number dim-family-number-qty">${qtyDisplay}</td>
            `;
            fragment.appendChild(tr);
        });

        if (bottomSpacerHeight > 0) {
            fragment.appendChild(createFamilyListSpacerRow(bottomSpacerHeight));
        }

        state.familyListBody.replaceChildren(fragment);
    }

    function createFamilyListSpacerRow(height) {
        const tr = document.createElement('tr');
        tr.className = 'dim-family-spacer-row';
        const td = document.createElement('td');
        td.className = 'dim-family-spacer-cell';
        td.colSpan = 3;
        td.style.height = `${height}px`;
        tr.appendChild(td);
        return tr;
    }

    function renderClientResultLegend(resultKeys) {
        if (!elements.clientResultLegend) return;
        elements.clientResultLegend.replaceChildren();
        resultKeys.forEach(result => {
            const item = document.createElement('span');
            item.className = 'dim-result-legend-item';
            const swatch = document.createElement('span');
            swatch.className = 'dim-result-swatch';
            swatch.style.backgroundColor = resultColor(result);
            swatch.setAttribute('aria-hidden', 'true');
            const label = document.createElement('span');
            label.textContent = result;
            item.append(swatch, label);
            elements.clientResultLegend.appendChild(item);
        });
    }

    function clientMetricTotal(row, metric, resultKey = '') {
        const useVal = metric === 'valorizacion';
        const source = row?.[useVal ? 'resultados_val' : 'resultados'] || {};
        if (resultKey) return Number(source[resultKey] || 0);
        const backendTotal = row?.[useVal ? 'total_valorizacion' : 'total_renglones'];
        return Number(backendTotal ?? Object.values(source).reduce((sum, value) => sum + Number(value || 0), 0));
    }

    function sortClientRows(rows, key, direction, metric) {
        const factor = direction === 'asc' ? 1 : -1;
        return (Array.isArray(rows) ? rows : []).slice().sort((left, right) => {
            let diff;
            if (key === 'cliente') diff = compareText(left.cliente, right.cliente);
            else if (key === 'condition') diff = compareText(left.is_client ? 'Cliente' : 'No cliente', right.is_client ? 'Cliente' : 'No cliente');
            else if (String(key).startsWith('result:')) diff = clientMetricTotal(left, metric, String(key).slice(7)) - clientMetricTotal(right, metric, String(key).slice(7));
            else diff = clientMetricTotal(left, metric) - clientMetricTotal(right, metric);
            return (diff * factor) || compareText(left.cliente, right.cliente);
        });
    }

    function updateClientSortHeaders() {
        if (!elements.clientSortHeaders) return;
        if (elements.clientTotalHeaderLabel) {
            elements.clientTotalHeaderLabel.textContent = state.activeMetric === 'valorizacion' ? 'Total valorización' : 'Total renglones';
        }
        elements.clientSortHeaders.querySelectorAll('[data-client-sort]').forEach(button => {
            const active = button.dataset.clientSort === state.clientSortKey;
            button.classList.toggle('active', active);
            const arrow = button.querySelector('.dim-sort-arrow');
            if (arrow) arrow.textContent = active ? sortArrow(button.dataset.clientSort, state.clientSortKey, state.clientSortDirection) : '';
        });
    }

    function setClientCardSort(key) {
        const nextKey = key === 'cliente' ? 'cliente' : 'total';
        state.clientSortDirection = state.clientSortKey === nextKey
            ? (state.clientSortDirection === 'asc' ? 'desc' : 'asc')
            : (nextKey === 'cliente' ? 'asc' : 'desc');
        state.clientSortKey = nextKey;
        renderBarClientChart((state.lastBootstrap || {}).clients_by_result || []);
    }

    function renderBarClientChart(rows) {
        const useVal = state.activeMetric === 'valorizacion';
        const sourceKey = useVal ? 'resultados_val' : 'resultados';
        const fullySorted = sortClientRows(rows, state.clientSortKey, state.clientSortDirection, state.activeMetric);
        const rankedRows = fullySorted.slice(0, CLIENT_CARD_LIMIT).map(row => ({
            ...row,
            _rankingTotal: clientMetricTotal(row, state.activeMetric),
        }));

        updateClientSortHeaders();
        state.barClientRows = rankedRows;
        const labels = rankedRows.map(row => `${row.is_client ? '[Cliente]' : '[No cliente]'} ${row.cliente || 'Sin cliente'}`);
        const resultKeys = Array.from(new Set(
            rankedRows.flatMap(row => Object.keys(row[sourceKey] || {}))
        )).sort((left, right) => normalizeKey(left).localeCompare(normalizeKey(right)));
        const datasets = resultKeys.map(key => ({
            label: key,
            data: rankedRows.map(row => Number((row[sourceKey] || {})[key] || 0)),
            backgroundColor: resultColor(key),
            borderRadius: 4,
            barPercentage: 0.7,
            stack: 'resultados',
        }));
        renderClientResultLegend(resultKeys);

        const dynamicH = Math.max(200, rankedRows.length * 36 + 46);
        const axisTitle = useVal ? 'Valorización' : 'Renglones';
        const tickFormatter = value => useVal ? formatAbbreviated(value) : formatCompactInteger(value);

        if (state.barClientChart) {
            const container = state.barClientChart.canvas.closest('.chart-container');
            if (container) container.style.height = dynamicH + 'px';
            state.barClientChart.data.labels = labels;
            state.barClientChart.data.datasets = datasets;
            state.barClientChart.options.scales.x.title.text = axisTitle;
            state.barClientChart.options.scales.x.ticks.callback = tickFormatter;
            state.barClientChart.update('none');
            return;
        }

        const ctx = document.getElementById('barClientChart').getContext('2d');
        const container = ctx.canvas.closest('.chart-container');
        if (container) container.style.height = dynamicH + 'px';
        state.barClientChart = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                onHover(event, activeElements) {
                    if (event.native?.target) event.native.target.style.cursor = activeElements.length ? 'pointer' : 'default';
                },
                onClick(_event, activeElements) {
                    if (!activeElements.length) return;
                    selectClientFromRanking(state.barClientRows[activeElements[0].index]);
                },
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            title: items => {
                                const row = state.barClientRows[items[0]?.dataIndex];
                                return row ? row.cliente : '';
                            },
                            beforeLabel: ctx => {
                                const row = state.barClientRows[ctx.dataIndex];
                                return `Condición: ${row?.is_client ? 'Cliente' : 'No cliente'}`;
                            },
                            label: ctx => state.activeMetric === 'valorizacion'
                                ? `${ctx.dataset.label}: $ ${formatAbbreviated(ctx.parsed.x)}`
                                : `${ctx.dataset.label}: ${formatInteger(ctx.parsed.x)} renglones`,
                            afterBody: items => {
                                const row = state.barClientRows[items[0]?.dataIndex];
                                if (!row) return '';
                                return state.activeMetric === 'valorizacion'
                                    ? `Total: $ ${formatAbbreviated(row._rankingTotal)}`
                                    : `Total: ${formatInteger(row._rankingTotal)} renglones`;
                            },
                        },
                    },
                },
                scales: {
                    x: {
                        stacked: true,
                        beginAtZero: true,
                        grid: { display: false },
                        title: { display: true, text: axisTitle, color: '#64748b', font: { size: 10, weight: '600' } },
                        ticks: { display: true, callback: tickFormatter, font: { size: 9 } },
                    },
                    y: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            font: { size: 9 },
                            callback(value) {
                                const label = this.getLabelForValue(value);
                                return label.length > 24 ? `${label.slice(0, 24)}…` : label;
                            },
                        },
                    },
                },
            },
        });
    }

    function renderPivotTable(data) {
        const rows = Array.isArray(data?.rows) ? data.rows : [];
        const months = Array.isArray(data?.months) ? data.months : [];
        state.pivotData = {
            months,
            rows,
            total: Number.isFinite(Number(data?.total)) ? Number(data.total) : rows.length,
        };
        state.pivotLastRenderKey = '';

        if (elements.pivotTotalLabel) {
            elements.pivotTotalLabel.textContent = `${formatInteger(state.pivotData.total)} familias`;
            elements.pivotTotalLabel.style.display = state.pivotData.total > 0 ? 'inline-block' : 'none';
        }

        renderPivotHeader(months);
        applyPivotSearch(elements.pivotFamilySearch ? elements.pivotFamilySearch.value : '', { resetScroll: true, force: true });
    }

    function renderPivotHeader(months) {
        if (!elements.pivotHeader) return;
        elements.pivotHeader.innerHTML = '';

        const headerRow = document.createElement('tr');
        const familyHeader = document.createElement('th');
        familyHeader.textContent = 'Familia';
        headerRow.appendChild(familyHeader);
        months.forEach(month => {
            const th = document.createElement('th');
            th.className = 'text-end';
            th.textContent = formatMonthLabel(month);
            headerRow.appendChild(th);
        });
        elements.pivotHeader.appendChild(headerRow);
    }

    function applyPivotSearch(rawTerm = '', options = {}) {
        const { resetScroll = false, force = false } = options;
        const normalizedTerm = normalizePivotSearch(rawTerm);
        const rows = state.pivotData.rows || [];

        state.pivotSearchTerm = normalizedTerm;
        state.pivotFilteredRows = normalizedTerm
            ? rows.filter(row => normalizePivotSearch(row.familia).includes(normalizedTerm))
            : rows.slice();

        if (elements.pivotTableWrap && resetScroll) {
            elements.pivotTableWrap.scrollTop = 0;
        }

        updatePivotSearchMeta();
        schedulePivotBodyRender(force);
    }

    function updatePivotSearchMeta() {
        if (!elements.pivotSearchCount) return;

        const filteredTotal = state.pivotFilteredRows.length;
        const fullTotal = state.pivotData.total || 0;
        if (!state.pivotSearchTerm) {
            elements.pivotSearchCount.style.display = filteredTotal > 0 ? 'inline-flex' : 'none';
            elements.pivotSearchCount.textContent = filteredTotal > 0
                ? `${formatInteger(filteredTotal)} familias visibles`
                : '';
            return;
        }

        elements.pivotSearchCount.style.display = 'inline-flex';
        elements.pivotSearchCount.textContent = `${formatInteger(filteredTotal)} de ${formatInteger(fullTotal)} familias`;
    }

    function schedulePivotBodyRender(force = false) {
        if (force) {
            if (state.pivotRenderRaf) {
                window.cancelAnimationFrame(state.pivotRenderRaf);
                state.pivotRenderRaf = null;
            }
            renderVisiblePivotRows();
            return;
        }

        if (state.pivotRenderRaf) return;
        state.pivotRenderRaf = window.requestAnimationFrame(() => {
            state.pivotRenderRaf = null;
            renderVisiblePivotRows();
        });
    }

    function renderVisiblePivotRows() {
        if (!elements.pivotBody) return;

        const rows = state.pivotFilteredRows || [];
        const months = state.pivotData.months || [];
        const totalColumns = months.length + 1;

        if (rows.length === 0) {
            state.pivotLastRenderKey = `empty:${state.pivotSearchTerm}`;
            elements.pivotBody.innerHTML = `
                <tr>
                    <td colspan="${Math.max(totalColumns, 1)}" class="text-center text-muted py-3 small">
                        ${state.pivotSearchTerm ? 'No hay familias que coincidan con la búsqueda.' : 'No hay datos para mostrar.'}
                    </td>
                </tr>
            `;
            return;
        }

        const viewportHeight = Math.max(elements.pivotTableWrap?.clientHeight || 0, PIVOT_ROW_HEIGHT);
        const visibleCount = Math.max(1, Math.ceil(viewportHeight / PIVOT_ROW_HEIGHT));
        const scrollTop = elements.pivotTableWrap?.scrollTop || 0;
        const start = Math.max(0, Math.floor(scrollTop / PIVOT_ROW_HEIGHT) - PIVOT_OVERSCAN);
        const end = Math.min(rows.length, start + visibleCount + (PIVOT_OVERSCAN * 2));
        const renderKey = `${start}:${end}:${rows.length}:${months.join('|')}:${state.pivotSearchTerm}`;

        if (renderKey === state.pivotLastRenderKey) return;
        state.pivotLastRenderKey = renderKey;

        const fragment = document.createDocumentFragment();
        const topSpacerHeight = start * PIVOT_ROW_HEIGHT;
        const bottomSpacerHeight = Math.max(0, (rows.length - end) * PIVOT_ROW_HEIGHT);

        if (topSpacerHeight > 0) {
            fragment.appendChild(createPivotSpacerRow(topSpacerHeight, totalColumns));
        }

        rows.slice(start, end).forEach(row => {
            const tr = document.createElement('tr');
            tr.className = 'dim-pivot-row';

            const familyCell = document.createElement('td');
            familyCell.className = 'dim-pivot-family-cell';
            familyCell.title = row.familia || '';
            const familyText = document.createElement('span');
            familyText.className = 'dim-pivot-family-text';
            familyText.textContent = row.familia || 'Sin familia';
            familyCell.appendChild(familyText);
            tr.appendChild(familyCell);

            const useVal = state.activeMetric === 'valorizacion';
            const valArr = useVal ? (row.valorizacion || row.values || []) : (row.values || []);
            valArr.forEach(value => {
                const td = document.createElement('td');
                td.className = 'text-end text-muted dim-pivot-value-cell';
                td.textContent = value > 0 ? (useVal ? formatAbbreviated(value) : formatDecimal(value)) : '-';
                tr.appendChild(td);
            });

            fragment.appendChild(tr);
        });

        if (bottomSpacerHeight > 0) {
            fragment.appendChild(createPivotSpacerRow(bottomSpacerHeight, totalColumns));
        }

        elements.pivotBody.replaceChildren(fragment);
    }

    function createPivotSpacerRow(height, colspan) {
        const tr = document.createElement('tr');
        tr.className = 'dim-pivot-spacer-row';
        const td = document.createElement('td');
        td.className = 'dim-pivot-spacer-cell';
        td.colSpan = colspan;
        td.style.height = `${height}px`;
        tr.appendChild(td);
        return tr;
    }

    function canonicalProvinceName(value) {
        let key = normalizeKey(value)
            .replace(/^PROVINCIA_DE_/, '')
            .replace(/^PROVINCIA_DEL_/, '');
        const aliases = {
            CAPITAL_FEDERAL: 'CABA',
            CIUDAD_AUTONOMA_DE_BUENOS_AIRES: 'CABA',
            TIERRA_DEL_FUEGO_ANTARTIDA_E_ISLAS_DEL_ATLANTICO_SUR: 'TIERRA_DEL_FUEGO',
        };
        return aliases[key] || key;
    }

    function mapIntensityColor(value, maxPositive) {
        if (value === 0) return MAP_ZERO_COLOR;
        if (value < 0 || maxPositive <= 0) return MAP_NO_DATA_COLOR;
        const ratio = Math.log1p(value) / Math.log1p(maxPositive);
        const index = Math.min(MAP_POSITIVE_COLORS.length - 1, Math.max(0, Math.ceil(ratio * MAP_POSITIVE_COLORS.length) - 1));
        return MAP_POSITIVE_COLORS[index];
    }

    function updateMapLegend(maxPositive, useVal) {
        if (state.mapLegendControl && state.mapInstance) {
            state.mapInstance.removeControl(state.mapLegendControl);
        }
        const control = L.control({ position: 'bottomright' });
        control.onAdd = () => {
            const div = L.DomUtil.create('div', 'dim-map-legend');
            const metricLabel = useVal ? 'Valorización' : 'Renglones';
            const maxLabel = useVal ? `$ ${formatAbbreviated(maxPositive)}` : formatInteger(maxPositive);
            div.innerHTML = `
                <span class="dim-map-legend-title">${metricLabel}</span>
                <div class="dim-map-legend-row"><span class="dim-map-legend-swatch dim-map-legend-nodata"></span>Sin datos</div>
                <div class="dim-map-legend-row"><span class="dim-map-legend-swatch" style="background:${MAP_ZERO_COLOR}"></span>Valor cero</div>
                <div class="dim-map-legend-row"><span class="dim-map-legend-swatch dim-map-legend-gradient" style="background:linear-gradient(90deg,${MAP_POSITIVE_COLORS.join(',')})"></span>Menor → mayor</div>
                <div class="dim-map-legend-row">Máximo: ${maxLabel}</div>
            `;
            L.DomEvent.disableClickPropagation(div);
            return div;
        };
        control.addTo(state.mapInstance);
        state.mapLegendControl = control;
    }

    function renderMapChart(rows) {
        const container = document.getElementById('mapContainer');
        if (!container) return;
        state.mapRows = Array.isArray(rows) ? rows : [];

        if (!state.mapInstance) {
            state.mapInstance = L.map(container, {
                attributionControl: false,
                zoomControl: false,
                minZoom: 2,
                maxZoom: 8,
            });
            L.control.zoom({ position: 'topright' }).addTo(state.mapInstance);
            state.mapInstance.fitBounds([[-55.2, -73.8], [-21.5, -53.5]], { padding: [4, 4] });
        }

        if (!state.mapGeoJsonData) {
            if (!state.mapGeoJsonPromise) {
                const geoJsonUrl = container.dataset.geojsonUrl;
                state.mapGeoJsonPromise = fetch(geoJsonUrl, { credentials: 'same-origin' })
                    .then(response => {
                        if (!response.ok) throw new Error(`GeoJSON no disponible (${response.status})`);
                        return response.json();
                    })
                    .then(data => { state.mapGeoJsonData = data; return data; });
            }
            state.mapGeoJsonPromise
                .then(() => renderMapChart(state.mapRows))
                .catch(error => {
                    console.error('[DIM] No se pudo cargar el mapa provincial:', error);
                    container.innerHTML = '<div class="text-center text-muted small p-3">Mapa provincial no disponible.</div>';
                });
            return;
        }

        if (state.mapGeoJsonLayer) {
            state.mapInstance.removeLayer(state.mapGeoJsonLayer);
            state.mapGeoJsonLayer = null;
        }

        const useVal = state.activeMetric === 'valorizacion';
        const dataByProvince = new Map();
        state.mapRows.forEach(item => {
            const key = canonicalProvinceName(item.provincia);
            if (!key || key === 'SIN_PROVINCIA') return;
            const value = Number(useVal ? item.valorizacion : item.renglones) || 0;
            const previous = dataByProvince.get(key);
            dataByProvince.set(key, {
                name: item.provincia,
                value: (previous?.value || 0) + value,
            });
        });
        const positives = Array.from(dataByProvince.values()).map(item => item.value).filter(value => value > 0);
        const maxPositive = positives.length ? Math.max(...positives) : 0;

        state.mapGeoJsonLayer = L.geoJSON(state.mapGeoJsonData, {
            style(feature) {
                const key = canonicalProvinceName(feature?.properties?.nombre);
                const datum = dataByProvince.get(key);
                const hasData = dataByProvince.has(key);
                return {
                    color: hasData ? '#ffffff' : '#94a3b8',
                    dashArray: hasData ? null : '3 3',
                    fillColor: hasData ? mapIntensityColor(datum.value, maxPositive) : MAP_NO_DATA_COLOR,
                    fillOpacity: hasData ? 0.9 : 0.58,
                    weight: hasData ? 1.2 : 1,
                };
            },
            onEachFeature(feature, layer) {
                const provinceName = feature?.properties?.nombre || 'Provincia';
                const datum = dataByProvince.get(canonicalProvinceName(provinceName));
                const valueLine = !datum
                    ? 'Sin datos para los filtros activos'
                    : useVal
                        ? `Valorización: $ ${formatAbbreviated(datum.value)}`
                        : `Renglones: ${formatInteger(datum.value)}`;
                layer.bindTooltip(`<b>${provinceName}</b><br>${valueLine}`, { sticky: true });
                layer.on({
                    mouseover(event) { event.target.setStyle({ weight: 2.5, color: '#0f3f60' }); },
                    mouseout(event) { state.mapGeoJsonLayer.resetStyle(event.target); },
                });
            },
        }).addTo(state.mapInstance);

        container.setAttribute('aria-label', `Mapa de Argentina por provincia según ${useVal ? 'Valorización' : 'Renglones'}`);
        updateMapLegend(maxPositive, useVal);
        window.setTimeout(() => state.mapInstance.invalidateSize(), 150);
    }

    function buildStackedBarChartOptions() {
        return {
            responsive: true,
            maintainAspectRatio: false,
            interaction: { mode: 'index', intersect: false },
            plugins: {
                legend: {
                    position: 'top',
                    align: 'end',
                    labels: { boxWidth: 10, usePointStyle: false, font: { size: 10 } },
                    onClick(e, legendItem, legend) {
                        const idx = legendItem.datasetIndex;
                        const code = state.chartSeriesCodes[idx];
                        if (code === undefined) return;

                        // Actualizar estado global de exclusión (fuente de verdad compartida)
                        if (state.hiddenSeriesCodes.has(code)) {
                            state.hiddenSeriesCodes.delete(code);
                        } else {
                            state.hiddenSeriesCodes.add(code);
                        }

                        // Visibilidad inmediata en el chart (sin esperar el debounce)
                        const chart = legend.chart;
                        const meta = chart.getDatasetMeta(idx);
                        meta.hidden = state.hiddenSeriesCodes.has(code);
                        chart.update();

                        // Propagar como filtro global: KPIs, donut, mapa, tablas se recalculan
                        triggerLoad();
                    },
                },
                tooltip: {
                    callbacks: {
                        label: ctx => state.activeMetric === 'valorizacion'
                            ? `${ctx.dataset.label}: ${formatAbbreviated(ctx.parsed.y)}`
                            : `${ctx.dataset.label}: ${formatInteger(ctx.parsed.y)} renglones`,
                        footer: items => {
                            const total = items.reduce((sum, it) => sum + (it.parsed.y || 0), 0);
                            return state.activeMetric === 'valorizacion'
                                ? `Total: ${formatAbbreviated(total)}`
                                : `Total: ${formatInteger(total)} renglones`;
                        },
                    },
                },
            },
            onClick(event, elements, chart) {
                if (!elements || !elements.length) return;

                const index = elements[0].index;
                const datasetIndex = elements[0].datasetIndex;
                const code = state.chartSeriesCodes[datasetIndex];
                const monthInfo = chart.data.labels[index];

                let filtersChanged = false;

                // 1. Filtrar por Unidad de Negocio (Negocio) seleccionado
                if (code !== undefined && msUnit) {
                    msUnit.setApplied([code]);
                    filtersChanged = true;
                }

                // 2. Filtrar por el mes específico clickeado
                if (monthInfo && dateRangeCtrl) {
                    dateRangeCtrl.setExactMonth(monthInfo);
                    filtersChanged = true;
                }

                if (filtersChanged) {
                    triggerLoad();
                    // Ocultar tooltip para evitar artefactos visuales
                    if (chart.tooltip) {
                        chart.tooltip.setActiveElements([], {x: 0, y: 0});
                    }
                }
            },
            scales: {
                x: {
                    stacked: true,
                    grid: { display: false },
                    ticks: { font: { size: 10 }, maxRotation: 45, minRotation: 0 },
                },
                y: {
                    stacked: true,
                    beginAtZero: true,
                    grid: { borderDash: [4, 4], color: '#e2e8f0' },
                    ticks: { callback: v => state.activeMetric === 'valorizacion' ? formatAbbreviated(v) : formatCompactInteger(v) },
                },
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
    function formatAbbreviated(value) {
        const n = Number(value) || 0;
        if (Math.abs(n) >= 1e9) return `${new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 }).format(n / 1e9)}MM`;
        if (Math.abs(n) >= 1e6) return `${new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 }).format(n / 1e6)}M`;
        if (Math.abs(n) >= 1e3) return `${new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 }).format(n / 1e3)}K`;
        return new Intl.NumberFormat('es-AR', { maximumFractionDigits: 0 }).format(n);
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
    function normalizePivotSearch(value) {
        return String(value || '').trim().toLocaleLowerCase('es-AR');
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
