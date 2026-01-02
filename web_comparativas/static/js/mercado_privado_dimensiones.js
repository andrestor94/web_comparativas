/* Mercado Privado - Dimensiones Controller (Backend Integrated) */

document.addEventListener('DOMContentLoaded', () => {
    // --- Global Configuration ---
    Chart.defaults.font.family = "'Outfit', sans-serif";
    Chart.defaults.color = '#64748b';
    Chart.defaults.scale.grid.color = 'rgba(226, 232, 240, 0.4)';
    Chart.defaults.plugins.tooltip.backgroundColor = '#0f172a';
    Chart.defaults.plugins.tooltip.titleFont = { size: 12, weight: 700, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.bodyFont = { size: 11, family: "'Outfit', sans-serif" };
    Chart.defaults.plugins.tooltip.cornerRadius = 8;

    // --- State ---
    let rawData = []; // The clean list from backend
    let jsonFile = null;
    let excelFile = null;
    let globalFamilyPrices = {}; // Calculated once from rawData

    // --- UI Elements ---
    const jsonInput = document.getElementById('jsonFileInput');
    const excelInput = document.getElementById('excelFileInput');
    const processBtn = document.getElementById('processBtn');

    const uploadContainer = document.getElementById('uploadContainer');
    const dashboardContent = document.getElementById('dashboardContent');
    const loadingOverlay = document.getElementById('loadingOverlay');
    const loadingText = document.getElementById('loadingText');
    const resetBtn = document.getElementById('resetBtn');

    // Controls
    const filters = {
        client: document.getElementById('filterClient'),
        province: document.getElementById('filterProvince'),
        category: document.getElementById('filterCategory'),
        start: document.getElementById('dateStart'),
        end: document.getElementById('dateEnd'),
        search: document.getElementById('searchGlobal'),
        identified: document.getElementById('filterIdentified'),
        isClient: document.getElementById('filterIsClient'),
        isVolume: document.getElementById('toggleVolume') // New Toggle
    };

    // Chart Instances
    let areaChart, pieChart, barClientChart;

    // --- Event Listeners ---
    jsonInput.addEventListener('change', (e) => {
        jsonFile = e.target.files[0];
        checkFiles();
    });

    excelInput.addEventListener('change', (e) => {
        excelFile = e.target.files[0];
        checkFiles();
    });

    processBtn.addEventListener('click', handleProcess);

    // Filter Events
    Object.values(filters).forEach(el => {
        if (el) el.addEventListener('change', updateDashboard);
    });

    let searchTimeout;
    filters.search.addEventListener('input', () => {
        clearTimeout(searchTimeout);
        searchTimeout = setTimeout(updateDashboard, 400);
    });

    if (resetBtn) {
        resetBtn.addEventListener('click', () => {
            location.reload(); // Simplest reset
        });
    }

    function checkFiles() {
        if (jsonFile && excelFile) {
            processBtn.disabled = false;
        } else {
            processBtn.disabled = true;
        }
    }

    async function handleProcess() {
        if (!jsonFile || !excelFile) return;

        setLoading(true, "Procesando y unificando datos...");

        try {
            const formData = new FormData();
            formData.append("json_file", jsonFile);
            formData.append("excel_file", excelFile);

            const response = await fetch("/api/mercado-privado/dimensiones/process", {
                method: "POST",
                body: formData
            });

            if (!response.ok) {
                let errorMsg = "Error al procesar archivos";
                try {
                    const errorText = await response.text();
                    try {
                        const errJson = JSON.parse(errorText);
                        errorMsg = errJson.detail || errorMsg;
                    } catch (e) {
                        errorMsg = "Error del Servidor: " + errorText.substring(0, 100); // Show raw text preview
                    }
                } catch (e) {
                    errorMsg = "Error de red o servidor no disponible.";
                }
                throw new Error(errorMsg);
            }

            const result = await response.json();
            rawData = result.data; // List of merged objects

            // Initialize Filters Options
            populateInitialFilters();

            // Calculate Global Prices (Pass 0)
            globalFamilyPrices = calculateGlobalPrices(rawData);

            // Show Dashboard
            uploadContainer.style.display = 'none';
            document.getElementById('dashboardControls').style.display = 'block';
            dashboardContent.style.display = 'contents';

            updateDashboard();

        } catch (error) {
            alert("Error: " + error.message);
        } finally {
            setLoading(false);
        }
    }

    function setLoading(show, text = "Cargando...") {
        loadingOverlay.style.display = show ? 'flex' : 'none';
        loadingText.textContent = text;
    }

    // --- Aggregation Logic (Client Side) ---

    function calculateGlobalPrices(allData) {
        const familyPricesStats = {};

        allData.forEach(row => {
            const fam = row.family;
            if (!familyPricesStats[fam]) familyPricesStats[fam] = { prices: [] };
            if (row.excel_price > 0 && row.excel_date) {
                familyPricesStats[fam].prices.push({ p: row.excel_price, d: row.excel_date });
            }
        });

        const familyPriceMap = {};
        const now = new Date();
        const sixtyDaysAgo = new Date();
        sixtyDaysAgo.setDate(now.getDate() - 60);

        Object.entries(familyPricesStats).forEach(([fam, meta]) => {
            let finalPrice = 0;
            if (meta.prices.length > 0) {
                const recent = meta.prices.filter(x => {
                    const d = new Date(x.d);
                    return !isNaN(d) && d >= sixtyDaysAgo;
                });
                if (recent.length > 0) {
                    const vals = recent.map(x => x.p);
                    finalPrice = calculateMedian(vals);
                } else {
                    meta.prices.sort((a, b) => new Date(b.d) - new Date(a.d));
                    finalPrice = meta.prices[0].p;
                }
            }
            familyPriceMap[fam] = finalPrice;
        });
        return familyPriceMap;
    }

    function populateInitialFilters() {
        const clients = new Set();
        const provinces = new Set();
        const categories = new Set();

        let minTs = Infinity;
        let maxTs = -Infinity;

        rawData.forEach(row => {
            if (row.client) clients.add(row.client);
            if (row.province) provinces.add(row.province);
            if (row.category) categories.add(row.category);

            if (row.date) {
                const d = new Date(row.date);
                const ts = d.getTime();
                if (!isNaN(ts)) {
                    if (ts < minTs) minTs = ts;
                    if (ts > maxTs) maxTs = ts;
                }
            }
        });

        populateSelect(filters.client, Array.from(clients).sort());
        populateSelect(filters.province, Array.from(provinces).sort());
        populateSelect(filters.category, Array.from(categories).sort());

        // Set Date Range
        if (minTs !== Infinity && maxTs !== -Infinity) {
            const minDate = new Date(minTs);
            const maxDate = new Date(maxTs);

            try {
                filters.start.value = minDate.toISOString().split('T')[0];
                filters.end.value = maxDate.toISOString().split('T')[0];
            } catch (e) {
                console.warn("Could not auto-populate date range", e);
            }
        }
    }

    function updateDashboard() {
        if (!rawData || rawData.length === 0) return;

        // 1. Filter Data
        const fClient = filters.client.value;
        const fProv = filters.province.value;
        const fCat = filters.category.value;
        const fStart = filters.start.value ? new Date(filters.start.value) : null;
        const fEnd = filters.end.value ? new Date(filters.end.value) : null;
        if (fStart) fStart.setHours(0, 0, 0, 0);
        if (fEnd) fEnd.setHours(23, 59, 59, 999);

        const fSearch = filters.search.value.toLowerCase().trim();
        const fIdentified = filters.identified.value;
        const fIsClient = filters.isClient.value;

        const filtered = rawData.filter(row => {
            // Relation Filter (Client)
            const isClient = row.is_client_bool;
            if (fIsClient === 'yes' && !isClient) return false;
            if (fIsClient === 'no' && isClient) return false;

            // Dropdowns
            if (fClient !== 'all' && row.client !== fClient) return false;
            if (fProv !== 'all' && row.province !== fProv) return false;
            if (fCat !== 'all' && row.category !== fCat) return false;

            // Date
            const d = new Date(row.date);
            if (fStart && d < fStart) return false;
            if (fEnd && d > fEnd) return false;

            // Search
            if (fSearch) {
                const searchStr = `${row.client} ${row.family} ${row.process_id}`.toLowerCase();
                if (!searchStr.includes(fSearch)) return false;
            }

            // Identified Filter
            if (fIdentified === 'yes' && !row.identified) return false;
            if (fIdentified === 'no' && row.identified) return false;

            return true;
        });

        // 2. Aggregate Data
        const stats = aggregateStats(filtered);

        // 3. Render
        renderDashboard(stats);
    }

    // --- Map Logic (Leaflet) ---
    let mapInstance = null;
    let mapMarkers = [];

    // Approximate coordinates for Argentina Provinces
    const provinceCoords = {
        'Buenos Aires': [-36.6769, -60.5588],
        'CABA': [-34.6037, -58.3816],
        'Catamarca': [-28.4696, -65.7852],
        'Chaco': [-26.3366, -60.7663],
        'Chubut': [-43.7886, -68.8892],
        'Córdoba': [-32.1429, -63.8017],
        'Corrientes': [-28.7743, -57.7568],
        'Entre Ríos': [-32.0588, -59.2014],
        'Formosa': [-24.8949, -59.5679],
        'Jujuy': [-23.3200, -65.7643],
        'La Pampa': [-37.1315, -65.4466],
        'La Rioja': [-29.6857, -67.1817],
        'Mendoza': [-34.3667, -68.9167],
        'Misiones': [-26.8753, -54.6518],
        'Neuquén': [-38.9525, -68.9126],
        'Río Negro': [-40.0388, -65.5525],
        'Salta': [-24.2991, -64.8144],
        'San Juan': [-30.8653, -68.8892],
        'San Luis': [-33.7577, -66.0281],
        'Santa Cruz': [-48.8154, -69.2542],
        'Santa Fe': [-30.7069, -60.9498],
        'Santiago del Estero': [-27.7824, -63.2523],
        'Tierra del Fuego': [-53.4862, -68.3039],
        'Tucumán': [-26.8241, -65.2226]
    };

    function renderMapChart(provinceData, isVolume) {
        const container = document.getElementById('mapContainer');
        if (!container) return;

        if (!mapInstance) {
            mapInstance = L.map('mapContainer').setView([-38.4161, -63.6167], 3); // Center of Argentina
            L.tileLayer('https://{s}.basemaps.cartocdn.com/light_all/{z}/{x}/{y}{r}.png', {
                attribution: '&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors &copy; <a href="https://carto.com/attributions">CARTO</a>',
                subdomains: 'abcd',
                maxZoom: 19
            }).addTo(mapInstance);
        }

        // Clear existing markers
        mapMarkers.forEach(m => mapInstance.removeLayer(m));
        mapMarkers = [];

        const fmt = isVolume
            ? new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0, notation: 'compact' })
            : new Intl.NumberFormat('es-AR');
        const labelText = isVolume ? 'Volumen' : 'Registros';

        // Add bubbles (Circles)
        Object.entries(provinceData).forEach(([provName, count]) => {
            const key = Object.keys(provinceCoords).find(k => k.toLowerCase() === provName.toLowerCase());

            if (key) {
                const coords = provinceCoords[key];
                // Scale radius 
                const radius = Math.min(Math.max(Math.log(count + 1) * 20000, 30000), 300000);

                const circle = L.circle(coords, {
                    color: '#5274ce',
                    fillColor: '#5274ce',
                    fillOpacity: 0.5,
                    radius: radius,
                    weight: 1
                }).addTo(mapInstance);

                // Tooltip
                circle.bindTooltip(`<b>${provName}</b><br>${labelText}: ${fmt.format(count)}`, {
                    direction: 'top',
                    offset: [0, -10]
                });

                mapMarkers.push(circle);
            }
        });

        // Invalidate size
        setTimeout(() => mapInstance.invalidateSize(), 200);
    }

    function aggregateStats(data) {
        const fIsVolume = filters.isVolume.checked;

        // --- AGGREGATION ---
        // Uses globalFamilyPrices calculated from full dataset

        const clientsSet = new Set();
        const processesSet = new Set();
        const familiesSet = new Set();

        const areaMap = {};
        const resultMap = {};
        const clientResultMap = {};
        const provinceMap = {};

        const familyYearMonthStats = {};

        // Family Local Stats (qty in selection)
        const familyLocalStats = {}; // fam -> qty

        const allMonths = new Set();
        let totalVolume = 0;

        data.forEach(row => {
            clientsSet.add(row.client);
            processesSet.add(row.process_id);
            familiesSet.add(row.family);

            const d = new Date(row.date);
            if (isNaN(d.getTime())) return;

            const m = d.getMonth() + 1;
            const y = d.getFullYear();
            const monthKey = `${y}-${String(m).padStart(2, '0')}`;
            allMonths.add(monthKey);

            // Family Stats (Local Qty)
            const fam = row.family;
            if (!familyLocalStats[fam]) familyLocalStats[fam] = 0;
            familyLocalStats[fam] += row.quantity;

            const famPrice = globalFamilyPrices[row.family] || 0;

            // Value Determination
            // If Volume: Qty * GlobalPrice
            // If Qty: Count(1) for Charts, Qty for Pivot
            const rowValueForCharts = fIsVolume ? (row.quantity * famPrice) : 1;
            const rowValueForPivot = fIsVolume ? (row.quantity * famPrice) : row.quantity;

            totalVolume += (row.quantity * famPrice);

            // Area Chart
            const cat = row.category;
            if (!areaMap[monthKey]) areaMap[monthKey] = {};
            if (!areaMap[monthKey][cat]) areaMap[monthKey][cat] = 0;
            areaMap[monthKey][cat] += rowValueForCharts;

            // Pie Chart
            const res = row.result;
            resultMap[res] = (resultMap[res] || 0) + rowValueForCharts;

            // Client Bar
            if (!clientResultMap[row.client]) clientResultMap[row.client] = {};
            clientResultMap[row.client][res] = (clientResultMap[row.client][res] || 0) + rowValueForCharts;

            // Map
            const prov = row.province || 'Desconocido';
            provinceMap[prov] = (provinceMap[prov] || 0) + rowValueForCharts;

            // Pivot (YearMonth sum)
            const ymKey = `${y}-${String(m).padStart(2, '0')}`;
            if (!familyYearMonthStats[fam]) familyYearMonthStats[fam] = {};
            familyYearMonthStats[fam][ymKey] = (familyYearMonthStats[fam][ymKey] || 0) + rowValueForPivot;
        });

        // Finalize Aggregations
        const sortedMonths = Array.from(allMonths).sort();

        // Area Data
        const catTotals = {};
        Object.values(areaMap).forEach(dayMap => {
            Object.entries(dayMap).forEach(([c, val]) => catTotals[c] = (catTotals[c] || 0) + val);
        });
        const topCats = Object.entries(catTotals).sort((a, b) => b[1] - a[1]).slice(0, 5).map(x => x[0]);

        // Pie Data
        const pieData = Object.entries(resultMap).map(([k, v]) => ({ label: k, value: v }));

        // Family Pivot Sorting
        // Sort by Volume if Volume Mode, else by Qty
        const familyFinalStats = {};
        Object.keys(familyLocalStats).forEach(fam => {
            const qty = familyLocalStats[fam];
            const price = globalFamilyPrices[fam] || 0;
            familyFinalStats[fam] = fIsVolume ? (qty * price) : qty;
        });

        const topFamiliesPivot = Object.entries(familyFinalStats)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 20)
            .map(x => x[0]);

        const monthsIndices = Array.from({ length: 12 }, (_, i) => i + 1);

        const pivotData = topFamiliesPivot.map(fam => {
            const row = { family: fam };
            const famStats = familyYearMonthStats[fam] || {};

            monthsIndices.forEach(mIdx => {
                const mStr = String(mIdx).padStart(2, '0');
                const yearValues = [];
                Object.keys(famStats).forEach(key => {
                    if (key.endsWith(`-${mStr}`)) {
                        yearValues.push(famStats[key]);
                    }
                });

                if (yearValues.length > 0) {
                    const sum = yearValues.reduce((a, b) => a + b, 0);
                    row[mIdx] = sum / yearValues.length;
                } else {
                    row[mIdx] = 0;
                }
            });
            return row;
        });


        // Family List (Top 50)
        const familyList = Object.entries(familyFinalStats)
            .sort((a, b) => b[1] - a[1])
            .slice(0, 50)
            .map(([k, val]) => {
                const qty = familyLocalStats[k];
                const price = globalFamilyPrices[k] || 0;
                return {
                    name: k,
                    count: qty,
                    price: price,
                    volume: qty * price,
                    outdated: false
                };
            });

        // Client Bar
        const topClients = Object.entries(clientResultMap)
            .map(([name, counts]) => ({
                name,
                total: Object.values(counts).reduce((a, b) => a + b, 0),
                counts
            }))
            .sort((a, b) => b.total - a.total)
            .slice(0, 10);

        return {
            isVolume: fIsVolume,
            kpis: {
                clients: clientsSet.size,
                processes: processesSet.size,
                families: familiesSet.size,
                volume: totalVolume
            },
            charts: {
                months: sortedMonths,
                topCategories: topCats,
                areaMap,
                pie: pieData,
                familyList,
                clientBar: topClients,
                pivot: {
                    months: monthsIndices, // Fixed months 1-12
                    data: pivotData
                },
                provinceMap: provinceMap
            }
        };
    }

    function calculateMedian(values) {
        if (!values || values.length === 0) return 0;
        values.sort((a, b) => a - b);
        const half = Math.floor(values.length / 2);
        if (values.length % 2) return values[half];
        return (values[half - 1] + values[half]) / 2.0;
    }

    // --- Custom Legend Logic (Isolation Mode) ---
    function handleLegendClick(e, legendItem, legend) {
        // Resolve correct index: Line/Bar use datasetIndex, Pie uses index.
        const index = typeof legendItem.datasetIndex !== 'undefined' ? legendItem.datasetIndex : legendItem.index;
        const chart = legend.chart;
        const type = chart.config.type;

        const isVisible = (i) => {
            if (type === 'doughnut' || type === 'pie') return chart.getDataVisibility(i);
            return chart.isDatasetVisible(i);
        };
        const setVisible = (i, val) => {
            if (type === 'doughnut' || type === 'pie') {
                if (chart.getDataVisibility(i) !== val) {
                    chart.toggleDataVisibility(i);
                }
            } else {
                chart.setDatasetVisibility(i, val);
            }
        };

        const count = (type === 'doughnut' || type === 'pie')
            ? chart.data.labels.length
            : chart.data.datasets.length;

        let allVisible = true;
        for (let i = 0; i < count; i++) {
            if (!isVisible(i)) {
                allVisible = false;
                break;
            }
        }

        if (allVisible) {
            for (let i = 0; i < count; i++) {
                if (i !== index) setVisible(i, false);
            }
            setVisible(index, true);
        } else {
            if (!isVisible(index)) {
                setVisible(index, true);
            } else {
                let visibleCount = 0;
                for (let i = 0; i < count; i++) {
                    if (isVisible(i)) visibleCount++;
                }

                if (visibleCount === 1) {
                    for (let i = 0; i < count; i++) setVisible(i, true);
                } else {
                    setVisible(index, false);
                }
            }
        }
        chart.update();
    }

    // --- Rendering ---

    function populateSelect(el, list) {
        const current = el.value;
        el.innerHTML = '<option value="all">Todas</option>';
        list.forEach(item => {
            const opt = document.createElement('option');
            opt.value = item;
            opt.textContent = item;
            el.appendChild(opt);
        });
        if (list.includes(current)) el.value = current;
    }

    function renderDashboard(data) {
        const isVolume = data.isVolume;

        // KPIs
        animateNumber('kpiClients', data.kpis.clients);
        animateNumber('kpiProcesses', data.kpis.processes);
        animateNumber('kpiFamilies', data.kpis.families);
        animateNumber('kpiVolume', data.kpis.volume, true);

        // --- Save to LocalStorage for Home Page ---
        try {
            const kpiSummary = {
                clients: data.kpis.clients,
                processes: data.kpis.processes,
                families: data.kpis.families,
                lastUpdated: new Date().toISOString()
            };
            localStorage.setItem('mp_dimensiones_kpis', JSON.stringify(kpiSummary));
        } catch (e) {
            console.warn("Could not save KPIs to localStorage", e);
        }

        // Charts
        renderAreaChart(data.charts, isVolume);
        renderPieChart(data.charts.pie, isVolume);
        renderFamilyList(data.charts.familyList, isVolume);
        renderBarClientChart(data.charts.clientBar, isVolume);
        renderPivotTable(data.charts.pivot, isVolume);
        renderMapChart(data.charts.provinceMap, isVolume);
    }

    function animateNumber(id, val, isCurrency = false) {
        const el = document.getElementById(id);
        const options = isCurrency
            ? { style: 'currency', currency: 'ARS', maximumFractionDigits: 0, notation: val > 1000000 ? 'compact' : 'standard' }
            : {};
        const fmt = new Intl.NumberFormat('es-AR', options);
        el.textContent = fmt.format(val);
    }

    // --- Chart Implementations ---

    function renderAreaChart(chartData, isVolume) {
        const ctx = document.getElementById('areaChart').getContext('2d');
        if (areaChart) areaChart.destroy();

        const labels = chartData.months;
        const palette = ['#064066', '#1e5c8a', '#5274ce', '#38bdf8', '#10b981', '#6366f1', '#64748b'];

        const datasets = chartData.topCategories.map((cat, i) => {
            const color = palette[i % palette.length];
            const data = labels.map(m => (chartData.areaMap[m] && chartData.areaMap[m][cat]) || 0);
            return {
                label: cat,
                data: data,
                backgroundColor: color + '33', // 20% opacity
                borderColor: color,
                borderWidth: 2,
                fill: true,
                tension: 0.4,
                pointRadius: 2,
                pointHoverRadius: 5,
                pointBackgroundColor: color,
                pointBorderColor: '#fff',
                pointBorderWidth: 1
            };
        });

        areaChart = new Chart(ctx, {
            type: 'line',
            data: { labels, datasets },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                interaction: {
                    mode: 'index',
                    intersect: false,
                },
                plugins: {
                    legend: {
                        position: 'top',
                        align: 'end',
                        labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } },
                        onClick: handleLegendClick
                    },
                    tooltip: {
                        backgroundColor: 'rgba(255, 255, 255, 0.95)',
                        titleColor: '#064066',
                        bodyColor: '#64748b',
                        borderColor: '#e2e8f0',
                        borderWidth: 1,
                        padding: 10,
                        usePointStyle: true,
                        callbacks: {
                            label: function (context) {
                                let val = context.parsed.y;
                                if (isVolume) return context.dataset.label + ': ' + new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 }).format(val);
                                return context.dataset.label + ': ' + val;
                            }
                        }
                    }
                },
                scales: {
                    y: {
                        beginAtZero: true,
                        grid: { borderDash: [4, 4], color: '#e2e8f0' },
                        ticks: {
                            callback: function (val) {
                                if (isVolume) return new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', notation: 'compact' }).format(val);
                                return val;
                            }
                        }
                    },
                    x: { grid: { display: false } }
                }
            }
        });
    }

    function renderPieChart(pieData, isVolume) {
        const ctx = document.getElementById('pieChart').getContext('2d');
        if (pieChart) pieChart.destroy();

        const colorMap = {
            'Confirmado': '#064066', 'Ganado': '#064066',
            'No participó': '#94a3b8', 'No Participó': '#94a3b8',
            'Comprado de Otra Empresa': '#1e5c8a', 'Perdido': '#ef4444',
            'Contrato': '#10b981'
        };
        const defaultColors = ['#064066', '#1e5c8a', '#5274ce', '#38bdf8', '#10b981'];

        pieChart = new Chart(ctx, {
            type: 'doughnut',
            data: {
                labels: pieData.map(d => d.label),
                datasets: [{
                    data: pieData.map(d => d.value),
                    backgroundColor: pieData.map((d, i) => colorMap[d.label] || defaultColors[i % defaultColors.length]),
                    borderWidth: 0,
                    hoverOffset: 8
                }]
            },
            options: {
                responsive: true,
                maintainAspectRatio: false,
                cutout: '70%',
                plugins: {
                    legend: {
                        position: 'bottom',
                        labels: { boxWidth: 8, usePointStyle: true, font: { size: 10 } },
                        onClick: handleLegendClick
                    },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                let val = context.parsed;
                                if (isVolume) return context.label + ': ' + new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 }).format(val);
                                return context.label + ': ' + val;
                            }
                        }
                    }
                }
            }
        });
    }

    function renderFamilyList(list, isVolume) {
        const container = document.getElementById('familyListContainer');
        container.innerHTML = '';
        const tpl = document.createElement('table');
        tpl.className = 'tech-table w-100';

        const thead = document.createElement('thead');
        const headerLabel = isVolume ? 'Volumen ($)' : 'Cant.';
        thead.innerHTML = `<tr><th style="background:transparent; pl-0;">Familia</th><th class="text-end" style="background:transparent;">Precio</th><th class="text-end pe-3" style="background:transparent;">${headerLabel}</th></tr>`;
        tpl.appendChild(thead);

        const tbody = document.createElement('tbody');
        const fmtPrice = new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 });

        const fmtValue = isVolume
            ? new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', notation: 'compact', maximumFractionDigits: 1 })
            : new Intl.NumberFormat('es-AR');

        list.forEach(item => {
            const tr = document.createElement('tr');
            const displayVal = isVolume ? item.volume : item.count;
            tr.innerHTML = `
                <td style="font-weight: 500; font-size: 0.75rem; padding: 0.35rem 0;">
                    <div class="text-truncate" style="max-width: 110px;" title="${item.name}">${item.name}</div>
                </td>
                <td class="text-end small" style="padding: 0.35rem 0;">
                    <span style="${item.outdated ? 'background-color: #fee2e2; color: #991b1b; padding: 2px 6px; border-radius: 4px; font-weight: 500;' : 'color: #64748b;'}">
                        ${item.price > 0 ? fmtPrice.format(item.price) : '-'}
                    </span>
                </td>
                <td class="text-end fw-bold pe-3" style="color: var(--tech-blue-mid); padding: 0.35rem 0;">
                    ${fmtValue.format(displayVal)}
                </td>
            `;
            tbody.appendChild(tr);
        });
        tpl.appendChild(tbody);
        container.appendChild(tpl);
    }

    function renderBarClientChart(clients, isVolume) {
        const ctx = document.getElementById('barClientChart').getContext('2d');
        if (barClientChart) barClientChart.destroy();

        const labels = clients.map(c => c.name);
        const allKeys = new Set();
        clients.forEach(c => Object.keys(c.counts).forEach(k => allKeys.add(k)));
        const keys = Array.from(allKeys).sort();

        const colorMap = {
            'Confirmado': '#064066', 'Ganado': '#064066',
            'No Participó': '#94a3b8', 'No participó': '#94a3b8',
            'Comprado de Otra Empresa': '#1e5c8a'
        };
        const defaultColors = ['#064066', '#1e5c8a', '#5274ce', '#10b981'];

        const datasets = keys.map((key, i) => ({
            label: key,
            data: clients.map(c => c.counts[key] || 0),
            backgroundColor: colorMap[key] || defaultColors[i % defaultColors.length],
            borderRadius: 4,
            barPercentage: 0.6,
            stack: 'stack1'
        }));

        barClientChart = new Chart(ctx, {
            type: 'bar',
            data: { labels, datasets },
            options: {
                indexAxis: 'y',
                responsive: true,
                maintainAspectRatio: false,
                plugins: {
                    legend: { display: false },
                    tooltip: {
                        callbacks: {
                            label: function (context) {
                                let val = context.parsed.x;
                                if (isVolume) return context.dataset.label + ': ' + new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', maximumFractionDigits: 0 }).format(val);
                                return context.dataset.label + ': ' + val;
                            }
                        }
                    }
                },
                scales: {
                    x: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            display: false,
                        }
                    },
                    y: {
                        stacked: true,
                        grid: { display: false },
                        ticks: {
                            font: { size: 9 }, callback: function (val) {
                                const l = this.getLabelForValue(val);
                                return l.length > 12 ? l.substr(0, 12) + '..' : l;
                            }
                        }
                    }
                }
            }
        });
    }

    function renderPivotTable(pivot, isVolume) {
        const thead = document.getElementById('pivotHeader');
        const tbody = document.getElementById('pivotBody');
        thead.innerHTML = '';
        tbody.innerHTML = '';

        const rowHeader = document.createElement('th');
        rowHeader.textContent = 'Familia';
        thead.appendChild(rowHeader);

        const monthFmt = new Intl.DateTimeFormat('es-AR', { month: 'short' });

        pivot.months.forEach(mIdx => { // 1..12
            const th = document.createElement('th');
            const date = new Date(2024, mIdx - 1, 15);
            let name = monthFmt.format(date);
            name = name.charAt(0).toUpperCase() + name.slice(1);
            th.textContent = name;
            th.className = 'text-end text-capitalize';
            thead.appendChild(th);
        });

        const fmt = isVolume
            ? new Intl.NumberFormat('es-AR', { style: 'currency', currency: 'ARS', notation: 'compact', maximumFractionDigits: 1 })
            : new Intl.NumberFormat('es-AR', { maximumFractionDigits: 1 });

        pivot.data.forEach(row => {
            const tr = document.createElement('tr');
            const tdName = document.createElement('td');
            tdName.className = 'fw-bold text-secondary';
            tdName.style.fontSize = '0.75rem';
            tdName.textContent = row.family;
            tr.appendChild(tdName);

            pivot.months.forEach(m => {
                const td = document.createElement('td');
                td.className = 'text-end text-muted';
                const val = row[m];
                td.textContent = val > 0 ? fmt.format(val) : '-';
                tr.appendChild(td);
            });
            tbody.appendChild(tr);
        });
    }
});
