document.addEventListener('DOMContentLoaded', () => {
    Chart.defaults.font.family = "'Outfit', sans-serif";
    Chart.defaults.color = '#64748b';
    Chart.defaults.scale.grid.color = 'rgba(226, 232, 240, 0.4)';
    Chart.defaults.plugins.tooltip.backgroundColor = '#0f172a';
    Chart.defaults.plugins.tooltip.titleFont = { size: 12, weight: 700, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.bodyFont = { size: 11, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.cornerRadius = 8;

    const state = {
        filtersLoaded: false,
        areaChart: null,
        pieChart: null,
        barClientChart: null,
        mapInstance: null,
        mapMarkers: [],
        // Mapeo de códigos numéricos de negocio a nombres descriptivos (Negocios.xlsx)
        negocioLabels: { unidades: {}, subunidades: {} },
    };

    const elements = {
        loadingOverlay: document.getElementById('loadingOverlay'),
        loadingText: document.getElementById('loadingText'),
        emptyState: document.getElementById('emptyState'),
        dashboardContent: document.getElementById('dashboardContent'),
        reloadBtn: document.getElementById('reloadDashboardBtn'),
        datasetStatusPill: document.getElementById('datasetStatusPill'),
        datasetSource: document.getElementById('datasetSource'),
        datasetUpdatedAt: document.getElementById('datasetUpdatedAt'),
        datasetTotalRows: document.getElementById('datasetTotalRows'),
        filterClient: document.getElementById('filterClient'),
        filterProvince: document.getElementById('filterProvince'),
        filterFamily: document.getElementById('filterFamily'),
        filterUnit: document.getElementById('filterUnit'),
        filterSubunit: document.getElementById('filterSubunit'),
        dateStart: document.getElementById('dateStart'),
        dateEnd: document.getElementById('dateEnd'),
        filterIdentified: document.getElementById('filterIdentified'),
        filterIsClient: document.getElementById('filterIsClient'),
        searchGlobal: document.getElementById('searchGlobal'),
        platformCheckboxes: Array.from(document.querySelectorAll('.platform-checkbox')),
        applyPlatformsBtn: document.getElementById('applyPlatformsBtn'),
        platformsLabel: document.getElementById('platformsLabel'),
        kpiClients: document.getElementById('kpiClients'),
        kpiRecords: document.getElementById('kpiRecords'),
        kpiFamilies: document.getElementById('kpiFamilies'),
        kpiQuantity: document.getElementById('kpiQuantity'),
        familyListContainer: document.getElementById('familyListContainer'),
        pivotHeader: document.getElementById('pivotHeader'),
        pivotBody: document.getElementById('pivotBody'),
    };

    const provinceCoords = {
        'Buenos Aires': [-36.6769, -60.5588],
        'CABA': [-34.6037, -58.3816],
        'Catamarca': [-28.4696, -65.7852],
        'Chaco': [-26.3366, -60.7663],
        'Chubut': [-43.7886, -68.8892],
        'Cordoba': [-32.1429, -63.8017],
        'Corrientes': [-28.7743, -57.7568],
        'Entre Rios': [-32.0588, -59.2014],
        'Formosa': [-24.8949, -59.5679],
        'Jujuy': [-23.3200, -65.7643],
        'La Pampa': [-37.1315, -65.4466],
        'La Rioja': [-29.6857, -67.1817],
        'Mendoza': [-34.3667, -68.9167],
        'Misiones': [-26.8753, -54.6518],
        'Neuquen': [-38.9525, -68.9126],
        'Rio Negro': [-40.0388, -65.5525],
        'Salta': [-24.2991, -64.8144],
        'San Juan': [-30.8653, -68.8892],
        'San Luis': [-33.7577, -66.0281],
        'Santa Cruz': [-48.8154, -69.2542],
        'Santa Fe': [-30.7069, -60.9498],
        'Santiago Del Estero': [-27.7824, -63.2523],
        'Tierra Del Fuego': [-53.4862, -68.3039],
        'Tucuman': [-26.8241, -65.2226]
    };

    const seriesPalette = ['#064066', '#1e5c8a', '#5274ce', '#38bdf8', '#10b981', '#64748b'];
    const resultPalette = ['#064066', '#1e5c8a', '#5274ce', '#38bdf8', '#10b981', '#94a3b8'];

    bindEvents();
    initDashboard();

    function bindEvents() {
        // ── Debounce centralizado para todos los filtros ────────────────────
        // Evita disparar 8 requests por cada cambio rápido de selector.
        // El refresh real ocurre 350 ms después del ÚLTIMO evento de cambio.
        let _filterDebounceTimer = null;
        const FILTER_DEBOUNCE_MS = 350;

        function _scheduleRefresh() {
            clearTimeout(_filterDebounceTimer);
            _filterDebounceTimer = setTimeout(loadDashboardData, FILTER_DEBOUNCE_MS);
        }

        const changeTargets = [
            elements.filterClient,
            elements.filterProvince,
            elements.filterFamily,
            elements.filterUnit,
            elements.filterSubunit,
            elements.dateStart,
            elements.dateEnd,
            elements.filterIdentified,
            elements.filterIsClient,
        ];
        changeTargets.forEach((target) => target.addEventListener('change', _scheduleRefresh));

        // Plataformas: no recargar en cada click individual, solo al presionar "Aplicar"
        elements.applyPlatformsBtn.addEventListener('click', () => {
            const selected = elements.platformCheckboxes.filter((cb) => cb.checked);
            if (selected.length === 0) {
                // Garantizar al menos una selección
                elements.platformCheckboxes[0].checked = true;
            }
            updatePlatformLabel();
            // Usar window.bootstrap para referenciar la librería Bootstrap (no la función local)
            const dropdownEl = document.getElementById('platformsDropdownBtn');
            const bsDropdown = window.bootstrap && window.bootstrap.Dropdown
                ? window.bootstrap.Dropdown.getInstance(dropdownEl)
                : null;
            if (bsDropdown) bsDropdown.hide();
            clearTimeout(_filterDebounceTimer);
            loadDashboardData();
        });

        elements.reloadBtn.addEventListener('click', initDashboard);

        let searchTimeout;
        elements.searchGlobal.addEventListener('input', () => {
            window.clearTimeout(searchTimeout);
            searchTimeout = window.setTimeout(loadDashboardData, 400);
        });
    }

    function updatePlatformLabel() {
        const all = elements.platformCheckboxes;
        const selected = all.filter((cb) => cb.checked);
        if (selected.length === 0 || selected.length === all.length) {
            elements.platformsLabel.textContent = 'Todas';
        } else {
            elements.platformsLabel.textContent = selected
                .map((cb) => cb.value.charAt(0) + cb.value.slice(1).toLowerCase())
                .join(', ');
        }
    }

    async function initDashboard() {
        setLoading(true, 'Leyendo snapshot persistido...');
        try {
            const [bootstrapResponse, labelsResponse] = await Promise.all([
                apiGet('/bootstrap'),
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

            if (bootstrap.meta?.stale) {
                window.setTimeout(loadDashboardData, 0);
            }
        } catch (error) {
            console.error(error);
            elements.datasetStatusPill.textContent = 'Error';
            elements.datasetStatusPill.className = 'badge text-bg-danger';
            elements.emptyState.style.display = 'block';
            elements.dashboardContent.style.display = 'none';
            elements.emptyState.querySelector('p').textContent = `No se pudo cargar Dimensionamiento: ${error.message}`;
        } finally {
            setLoading(false);
        }
    }

    // Carga inicial sin filtros activos: puebla los selects con todos los valores posibles
    // y establece el rango de fechas por defecto.
    // Nunca lanza: un fallo en /filters no debe abortar la carga del dashboard.
    async function loadFilterOptions() {
        try {
            const response = await apiGet('/filters');
            const data = response.data;
            applyFilterOptions(data);
            if (!state.filtersLoaded && data.date_range) {
                elements.dateStart.value = data.date_range.min || '';
                elements.dateEnd.value = data.date_range.max || '';
                state.filtersLoaded = true;
            }
        } catch (e) {
            console.warn('[DIM] loadFilterOptions failed, continuing without filters:', e);
        }
    }

    function renderBootstrapPayload(bootstrap) {
        const filterData = bootstrap.filters || {
            clientes: [],
            provincias: [],
            familias: [],
            unidades_negocio: [],
            subunidades_negocio: [],
            resultados: [],
            date_range: { min: null, max: null },
        };
        applyFilterOptions(filterData);
        if (!state.filtersLoaded && filterData.date_range) {
            elements.dateStart.value = filterData.date_range.min || '';
            elements.dateEnd.value = filterData.date_range.max || '';
            state.filtersLoaded = true;
        }

        renderKpis(bootstrap.kpis || {});
        renderAreaChart(bootstrap.series || { months: [], datasets: [] });
        renderPieChart(bootstrap.results || []);
        renderFamilyList(bootstrap.top_families || []);
        renderMapChart(bootstrap.geo || []);
        renderBarClientChart(bootstrap.clients_by_result || []);
        renderPivotTable(bootstrap.family_consumption || { months: [], rows: [] });
    }

    // Actualiza las opciones de los selects manteniendo los valores ya seleccionados
    // si siguen siendo válidos dentro del nuevo subconjunto.
    function applyFilterOptions(data) {
        populateSelect(elements.filterClient, data.clientes, 'Todos');
        populateSelect(elements.filterProvince, data.provincias, 'Todas');
        populateSelect(elements.filterFamily, data.familias, 'Todas');

        // Unidad de negocio: value = código original, label = descripción de Negocios.xlsx
        const unidadOpts = (data.unidades_negocio || []).map((code) => ({
            value: code,
            label: resolveUnitLabel(code),
        }));
        populateSelect(elements.filterUnit, unidadOpts, 'Todas');

        // Subunidad: value = código original, label = descripción de Negocios.xlsx
        // Para resolver el nombre usamos la unidad actualmente seleccionada como contexto
        const currentUnit = elements.filterUnit.value || null;
        const subunidadOpts = (data.subunidades_negocio || []).map((code) => ({
            value: code,
            label: resolveSubunitLabel(code, currentUnit),
        }));
        populateSelect(elements.filterSubunit, subunidadOpts, 'Todas');
    }

    /**
     * Resuelve el nombre descriptivo de una unidad de negocio.
     * Normaliza el código (trim, to-int si es numérico) y busca en el mapeo.
     * Fallback: retorna el código original si no hay match.
     */
    function resolveUnitLabel(code) {
        if (!code && code !== 0) return String(code);
        const key = _normalizeNegocioCode(code);
        return state.negocioLabels.unidades[key] || String(code);
    }

    /**
     * Resuelve el nombre descriptivo de una subunidad de negocio.
     * Cuando se conoce la unidad actual del filtro, usa clave compuesta "unidad|subunidad".
     * Si no hay unidad seleccionada, busca la primera coincidencia de subunidad en cualquier unidad.
     * Fallback: retorna el código original.
     */
    function resolveSubunitLabel(code, unitCode) {
        if (!code && code !== 0) return String(code);
        const sKey = _normalizeNegocioCode(code);
        if (unitCode) {
            const uKey = _normalizeNegocioCode(unitCode);
            const compound = `${uKey}|${sKey}`;
            if (state.negocioLabels.subunidades[compound]) {
                return state.negocioLabels.subunidades[compound];
            }
        }
        // Sin unidad de contexto: buscar primera coincidencia en cualquier unidad
        const prefix = `|${sKey}`;
        const hit = Object.entries(state.negocioLabels.subunidades)
            .find(([k]) => k.endsWith(prefix));
        return hit ? hit[1] : String(code);
    }

    /** Normaliza un código de negocio a string entero para buscar en el mapeo. */
    function _normalizeNegocioCode(code) {
        const s = String(code).trim();
        // Si parece numérico, convertir a entero (elimina ".0" de floats)
        const n = parseFloat(s);
        return Number.isFinite(n) ? String(Math.round(n)) : s;
    }

    async function loadDashboardData() {
        setLoading(true, 'Consultando métricas agregadas...');
        const query = buildQueryParams();

        // ── Lote 1 (crítico): widgets principales ───────────────────────────
        // NO incluye /filters para no bloquear la ruta crítica con una query
        // de ~30s. Los filtros se actualizan en el lote secundario.
        const [
            kpisResult,
            seriesResult,
            resultsResult,
            familiesResult,
        ] = await Promise.allSettled([
            apiGet('/kpis', query),
            apiGet('/series', query),
            apiGet('/results', query),
            apiGet('/top-families', { ...query, limit: 10 }),
        ]);

        // ── Lote 2 (secundario): geo, clientes, consumo + filtros ───────────
        // Se disparan en paralelo mientras se renderizan los widgets críticos.
        // /filters queda aquí para no bloquear la visualización principal.
        const secondaryPromise = Promise.allSettled([
            apiGet('/geo', query),
            apiGet('/clients-by-result', { ...query, limit: 10 }),
            apiGet('/family-consumption', { ...query, limit: 20 }),
            apiGet('/filters', query),
        ]);

        // Renderizar widgets críticos (el overlay desaparece aquí)
        _widgetRender(kpisResult,
            (data) => renderKpis(data),
            () => renderKpisError()
        );
        _widgetRender(seriesResult,
            (data) => renderAreaChart(data),
            (msg) => showCanvasError('areaChart', 'areaChart', msg)
        );
        _widgetRender(resultsResult,
            (data) => renderPieChart(data),
            (msg) => showCanvasError('pieChart', 'pieChart', msg)
        );
        _widgetRender(familiesResult,
            (data) => renderFamilyList(data),
            (msg) => showContainerError(elements.familyListContainer, msg)
        );

        // El overlay se quita en cuanto los widgets críticos están listos
        setLoading(false);

        // Renderizar widgets secundarios cuando lleguen (no bloqueantes)
        const [geoResult, clientsResult, consumptionResult, filtersResult] = await secondaryPromise;
        _widgetRender(geoResult,
            (data) => renderMapChart(data),
            () => { /* el mapa muestra lo último que tenía; no se fuerza error visual */ }
        );
        _widgetRender(clientsResult,
            (data) => renderBarClientChart(data),
            (msg) => showCanvasError('barClientChart', 'barClientChart', msg)
        );
        _widgetRender(consumptionResult,
            (data) => renderPivotTable(data),
            (msg) => showPivotError(msg)
        );
        // Actualizar opciones de filtros cuando estén disponibles (cascading filters)
        if (filtersResult.status === 'fulfilled' && filtersResult.value.ok !== false) {
            applyFilterOptions(filtersResult.value.data);
        }
    }

    /**
     * Ejecuta successFn con los datos si el resultado es exitoso (fulfilled + ok !== false).
     * Si falló (rejected o ok === false), llama a errorFn con un mensaje de error breve.
     */
    function _widgetRender(result, successFn, errorFn) {
        if (result.status === 'fulfilled' && result.value.ok !== false) {
            try {
                successFn(result.value.data);
            } catch (renderError) {
                console.error('[DIM] Error al renderizar widget:', renderError);
                errorFn('Error al dibujar el widget.');
            }
            return;
        }
        const errMsg = (result.status === 'rejected')
            ? (result.reason?.message || 'Error de red')
            : (result.value?.message || 'No disponible temporalmente');
        const errCode = (result.status === 'fulfilled') ? (result.value?.error_code || '') : 'network';
        console.warn('[DIM] Widget no disponible. error_code=' + errCode + ' msg=' + errMsg);
        errorFn(errMsg);
    }

    /** Muestra '--' en los KPIs cuando la query falló. */
    function renderKpisError() {
        [elements.kpiClients, elements.kpiRecords, elements.kpiFamilies, elements.kpiQuantity]
            .forEach((el) => { if (el) el.textContent = '--'; });
    }

    /** Muestra un mensaje de error en un canvas: destruye el gráfico previo y pinta texto. */
    function showCanvasError(canvasId, chartStateKey, msg) {
        if (state[chartStateKey]) {
            state[chartStateKey].destroy();
            state[chartStateKey] = null;
        }
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

    /** Muestra un mensaje de error dentro de un contenedor genérico (div, tabla, etc.). */
    function showContainerError(container, msg) {
        if (!container) return;
        container.innerHTML = `<div class="text-center text-muted py-4 small">${msg || 'No disponible temporalmente'}</div>`;
    }

    /** Muestra un mensaje de error en la tabla pivot (cabecera + cuerpo). */
    function showPivotError(msg) {
        if (elements.pivotHeader) elements.pivotHeader.innerHTML = '';
        if (elements.pivotBody) {
            elements.pivotBody.innerHTML = `<tr><td colspan="13" class="text-center text-muted py-3 small">${msg || 'No disponible temporalmente'}</td></tr>`;
        }
    }

    async function apiGet(path, params = {}) {
        const query = new URLSearchParams();
        Object.entries(params).forEach(([key, value]) => {
            if (value === undefined || value === null || value === '') return;
            if (Array.isArray(value)) {
                value.forEach((item) => {
                    if (item !== undefined && item !== null && item !== '') {
                        query.append(key, item);
                    }
                });
                return;
            }
            query.append(key, value);
        });

        const url = `/api/mercado-privado/dimensiones${path}${query.toString() ? `?${query}` : ''}`;
        const response = await fetch(url, { headers: { 'Accept': 'application/json' } });
        const payload = await response.json().catch(() => ({}));

        // Error HTTP real (4xx, 5xx): lanzar excepción para que el caller sepa
        if (!response.ok) {
            throw new Error(payload.detail || payload.message || `Error HTTP ${response.status}`);
        }
        // ok: false del backend (timeout, backend_error): retornar payload para que
        // cada widget lo maneje de forma independiente con _widgetRender.
        return payload;
    }

    function buildQueryParams() {
        const plataformas = elements.platformCheckboxes
            .filter((checkbox) => checkbox.checked)
            .map((checkbox) => checkbox.value);

        return {
            cliente: elements.filterClient.value ? [elements.filterClient.value] : [],
            provincia: elements.filterProvince.value ? [elements.filterProvince.value] : [],
            familia: elements.filterFamily.value ? [elements.filterFamily.value] : [],
            unidad_negocio: elements.filterUnit.value ? [elements.filterUnit.value] : [],
            subunidad_negocio: elements.filterSubunit.value ? [elements.filterSubunit.value] : [],
            plataforma: plataformas,
            fecha_desde: elements.dateStart.value || null,
            fecha_hasta: elements.dateEnd.value || null,
            identified: elements.filterIdentified.value || null,
            is_client: elements.filterIsClient.value || null,
            search: elements.searchGlobal.value.trim() || null,
        };
    }

    function setLoading(show, text = 'Cargando...') {
        elements.loadingOverlay.style.display = show ? 'flex' : 'none';
        elements.loadingText.textContent = text;
    }

    function renderStatus(status) {
        elements.datasetStatusPill.textContent = status.has_data ? 'Datos disponibles' : 'Sin datos';
        elements.datasetStatusPill.className = status.has_data ? 'badge text-bg-success' : 'badge text-bg-secondary';
        const sourcePath = status.last_import?.source_path || '';
        elements.datasetSource.textContent = sourcePath ? formatDatasetName(sourcePath) : 'Sin fuente cargada';
        elements.datasetSource.title = sourcePath || '';
        elements.datasetUpdatedAt.textContent = status.last_import?.finished_at
            ? new Date(status.last_import.finished_at).toLocaleString('es-AR')
            : '-';
        elements.datasetTotalRows.textContent = new Intl.NumberFormat('es-AR').format(status.total_rows || 0);
    }

    /**
     * Puebla un <select> con opciones, manteniendo la selección actual si el value sigue siendo válido.
     * Soporta dos formatos de opciones:
     *   - Array de strings: value y label son iguales al string
     *   - Array de {value, label}: separa el valor real del texto visible
     */
    function populateSelect(select, options, defaultLabel) {
        const currentValue = select.value;
        select.innerHTML = `<option value="">${defaultLabel}</option>`;
        options.forEach((option) => {
            const el = document.createElement('option');
            if (typeof option === 'object' && option !== null) {
                el.value = option.value;
                el.textContent = option.label || option.value;
            } else {
                el.value = option;
                el.textContent = option;
            }
            select.appendChild(el);
        });
        // Restaurar selección previa si el value sigue existiendo
        const validValues = Array.from(select.options).map((o) => o.value);
        if (currentValue && validValues.includes(currentValue)) {
            select.value = currentValue;
        }
    }

    function renderKpis(kpis) {
        elements.kpiClients.textContent = formatInteger(kpis.clientes || 0);
        elements.kpiRecords.textContent = formatInteger(kpis.renglones || 0);
        elements.kpiFamilies.textContent = formatInteger(kpis.familias || 0);
        elements.kpiQuantity.textContent = formatDecimal(kpis.cantidad_demandada || 0);
        try {
            localStorage.setItem('mp_dimensiones_kpis', JSON.stringify({
                clients: kpis.clientes || 0,
                processes: kpis.renglones || 0,
                families: kpis.familias || 0,
                lastUpdated: new Date().toISOString(),
            }));
        } catch (error) {
            console.warn('No se pudieron guardar KPIs de Dimensionamiento en localStorage', error);
        }
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
                labels: results.map((item) => item.resultado),
                datasets: [{
                    data: results.map((item) => item.renglones),
                    backgroundColor: results.map((_, index) => resultPalette[index % resultPalette.length]),
                    borderWidth: 0,
                    hoverOffset: 8,
                }],
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '68%',
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } },
                    },
                    tooltip: {
                        callbacks: {
                            label: (context) => `${context.label}: ${formatInteger(context.parsed)} renglones`,
                        },
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

        families.forEach((item) => {
            const tr = document.createElement('tr');
            tr.innerHTML = `
                <td class="dim-family-name-cell" title="${item.familia}">
                    <div class="dim-family-name-text">${item.familia}</div>
                </td>
                <td class="text-end small text-muted dim-family-number dim-family-number-count">${formatInteger(item.renglones)}</td>
                <td class="text-end fw-bold dim-family-number dim-family-number-qty" style="color: var(--tech-blue-mid);">
                    ${formatDecimal(item.cantidad)}
                </td>
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

        // Altura dinámica: 32px por cliente + margen para eje X
        const dynamicH = Math.max(200, rows.length * 32 + 28);
        const container = ctx.canvas.closest('.chart-container');
        if (container) container.style.height = dynamicH + 'px';

        const labels = rows.map((row) => row.cliente);
        const resultKeys = Array.from(new Set(rows.flatMap((row) => Object.keys(row.resultados || {}))));

        state.barClientChart = new Chart(ctx, {
            type: 'bar',
            data: {
                labels,
                datasets: resultKeys.map((key, index) => ({
                    label: key,
                    data: rows.map((row) => row.resultados?.[key] || 0),
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
                        callbacks: {
                            label: (context) => `${context.dataset.label}: ${formatInteger(context.parsed.x)} renglones`,
                        },
                    },
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { display: false },
                        ticks: { display: false },
                    },
                    y: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            font: { size: 9 },
                            callback: function (value) {
                                const label = this.getLabelForValue(value);
                                return label.length > 16 ? `${label.slice(0, 16)}..` : label;
                            },
                        },
                    },
                },
            },
        });
    }

    function renderPivotTable(data) {
        elements.pivotHeader.innerHTML = '';
        elements.pivotBody.innerHTML = '';

        const headerRow = document.createElement('tr');
        const familyHeader = document.createElement('th');
        familyHeader.textContent = 'Familia';
        headerRow.appendChild(familyHeader);

        (data.months || []).forEach((month) => {
            const th = document.createElement('th');
            th.className = 'text-end';
            th.textContent = formatMonthLabel(month);
            headerRow.appendChild(th);
        });
        elements.pivotHeader.appendChild(headerRow);

        (data.rows || []).forEach((row) => {
            const tr = document.createElement('tr');
            const familyCell = document.createElement('td');
            familyCell.className = 'fw-bold text-secondary';
            familyCell.style.fontSize = '0.75rem';
            familyCell.textContent = row.familia;
            tr.appendChild(familyCell);

            row.values.forEach((value) => {
                const td = document.createElement('td');
                td.className = 'text-end text-muted';
                td.textContent = value > 0 ? formatDecimal(value) : '-';
                tr.appendChild(td);
            });
            elements.pivotBody.appendChild(tr);
        });
    }

    function renderMapChart(rows) {
        const container = document.getElementById('mapContainer');
        if (!state.mapInstance) {
            state.mapInstance = L.map(container).setView([-38.4161, -63.6167], 3);
            L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; OpenStreetMap contributors &copy; CARTO',
                subdomains: 'abcd',
                maxZoom: 19,
            }).addTo(state.mapInstance);
        }

        state.mapMarkers.forEach((marker) => state.mapInstance.removeLayer(marker));
        state.mapMarkers = [];

        rows.forEach((item) => {
            const key = Object.keys(provinceCoords).find((province) => province.toLowerCase() === String(item.provincia).toLowerCase());
            if (!key) return;

            const marker = L.circle(provinceCoords[key], {
                color: '#5274ce',
                fillColor: '#5274ce',
                fillOpacity: 0.45,
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
                legend: {
                    position: 'top',
                    align: 'end',
                    labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } },
                },
                tooltip: {
                    callbacks: {
                        label: (context) => `${context.dataset.label}: ${formatInteger(context.parsed.y)} renglones`,
                    },
                },
            },
            scales: {
                y: {
                    beginAtZero: true,
                    grid: { borderDash: [4, 4], color: '#e2e8f0' },
                    ticks: {
                        callback: (value) => formatCompactInteger(value),
                    },
                },
                x: { grid: { display: false } },
            },
        };
    }

    function formatInteger(value) {
        return new Intl.NumberFormat('es-AR', { maximumFractionDigits: 0 }).format(value || 0);
    }

    function formatDecimal(value) {
        return new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 }).format(value || 0);
    }

    function formatCompact(value) {
        return new Intl.NumberFormat('es-AR', { notation: 'compact', maximumFractionDigits: 1 }).format(value || 0);
    }

    function formatCompactInteger(value) {
        return new Intl.NumberFormat('es-AR', { notation: 'compact', maximumFractionDigits: 0 }).format(value || 0);
    }

    function formatMonthLabel(monthIso) {
        if (/^\d{2}$/.test(String(monthIso))) {
            const date = new Date(2024, Number(monthIso) - 1, 1);
            return new Intl.DateTimeFormat('es-AR', { month: 'short' }).format(date);
        }
        const date = new Date(`${monthIso}T00:00:00`);
        if (Number.isNaN(date.getTime())) return monthIso;
        return new Intl.DateTimeFormat('es-AR', { month: 'short', year: '2-digit' }).format(date);
    }

    function formatDatasetName(sourcePath) {
        return String(sourcePath).split(/[/\\]/).pop() || sourcePath;
    }
});
