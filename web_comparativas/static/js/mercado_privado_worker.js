/* Worker for Mercado Privado Dashboard Processing v4 (Strict Business Logic) */

self.onmessage = async function (e) {
    const { file, filters } = e.data;

    if (!file) {
        self.postMessage({ error: 'No file provided' });
        return;
    }

    try {
        self.postMessage({ status: 'reading', progress: 0 });
        const text = await readFileAsync(file);

        self.postMessage({ status: 'parsing', progress: 30 });
        let data = JSON.parse(text);

        if (!Array.isArray(data)) {
            if (data.data && Array.isArray(data.data)) data = data.data;
            else if (data.rows && Array.isArray(data.rows)) data = data.rows;
            else if (data.results && Array.isArray(data.results)) data = data.results;
            else if (data.items && Array.isArray(data.items)) data = data.items;
            else {
                throw new Error('El JSON no contiene una lista de datos válida.');
            }
        }

        self.postMessage({ status: 'processing', progress: 50 });
        const results = processData(data, filters);

        self.postMessage({ status: 'done', results });
    } catch (err) {
        self.postMessage({ error: err.message });
    }
};

function readFileAsync(file) {
    return new Promise((resolve, reject) => {
        const reader = new FileReader();
        reader.onload = () => resolve(reader.result);
        reader.onerror = () => reject(reader.error);
        reader.readAsText(file);
    });
}

function processData(rawData, filters) {
    // 1. Setup Aggregators
    const clients = new Set();
    const categories = new Set();
    const uniqueProcesses = new Set();

    // Aggregates for Charts
    const areaMap = {}; // Evolution: Month -> Category -> RowCount
    const resultMap = {}; // Share: Result -> RowCount
    const familyStats = {}; // Top Families: Family -> { count, revenue, qty }
    const clientResultMap = {}; // Clients: Client -> Result -> RowCount
    const pivotMap = {}; // Matrix: Family -> Month -> [Quantities] (for Median)
    const allMonths = new Set();

    // 2. Filter Setup
    const fClient = filters.client && filters.client !== 'all' ? filters.client.toLowerCase() : null;
    const fProvince = filters.province && filters.province !== 'all' ? filters.province.toLowerCase() : null;
    const fCategory = filters.category && filters.category !== 'all' ? filters.category.toLowerCase() : null;
    const fIsClient = filters.isClient; // 'yes' (Cliente), 'no' (No Cliente), 'all'

    const start = filters.dateStart ? new Date(filters.dateStart) : null;
    const end = filters.dateEnd ? new Date(filters.dateEnd) : null;
    if (start) start.setHours(0, 0, 0, 0);
    if (end) end.setHours(23, 59, 59, 999);

    const optionsClients = new Set();
    const optionsProvinces = new Set();
    const optionsCategories = new Set();

    // 3. Iterate
    rawData.forEach((item, index) => {
        const entry = normalizeEntry(item);

        // --- IDENTIFY SOURCE ---
        let source = 'other';
        if (entry.data_abertura || entry.hosp_razao_social || item.hosp_razao_social) source = 'bionexo';
        else if (entry.fecha || entry.cotizado) source = 'portada';
        else if (entry.apertura && entry.hospital) source = 'unified'; // legacy/fallback

        // --- DATA EXTRACTION ---
        let dateVal, client, product, category, quantity, price, province, processId, cuit;
        let result = 'Desconocido';

        // Source-specific Extraction
        if (source === 'bionexo') {
            dateVal = parseDate(entry.data_abertura);
            client = entry.hosp_razao_social || 'Desconocido';
            product = entry.producto || entry.familia || 'Sin Producto';
            category = entry.categoria || 'Sin Categoría'; // Use 'categoria' column
            quantity = parseNumber(entry.cantidad);
            price = parseNumber(entry.precio_unitario || entry.valor_unitario);
            province = entry.provincia || 'Desconocido';
            processId = entry.id_pedido || entry.cd_pedido || entry.cd_cotacao || entry.id_proceso;
            cuit = entry.cuit || entry.nro_identificacion_tributaria || null;

            // Result Logic: "Usa directo la columna resultado"
            result = entry.resultado || 'Sin Resultado';

        } else if (source === 'portada') {
            dateVal = parseDate(entry.fecha);
            client = entry.cliente || entry.hospital || 'Desconocido';
            product = entry.producto || entry.familia || 'Sin Producto';
            category = 'Sin Categoría'; // Portada doesn't usually have category, distinct from product?
            quantity = parseNumber(entry.cantidad);
            price = parseNumber(entry.precio || entry.precio_unitario);
            province = entry.provincia || 'Desconocido';
            processId = entry.id_proceso || entry.nro_proceso || entry.id;
            cuit = entry.cuit || null;

            // Result Logic (Critical):
            const cotizado = (entry.cotizado || '').toLowerCase();
            const ganado = (entry.ganado || '').toLowerCase();

            if (!cotizado && !ganado) {
                result = 'No participó';
            } else if (cotizado === 'si' && ganado !== 'si') {
                result = 'Comprado de Otra Empresa';
            } else if (cotizado === 'si' && ganado === 'si') {
                result = 'Confirmado';
            } else {
                result = 'No Participó'; // Fallback
            }

        } else {
            // Unified/Fallback
            dateVal = parseDate(entry.apertura || entry.fecha);
            client = entry.hospital || entry.cliente || 'Desconocido';
            product = entry.producto || 'Sin Producto';
            category = entry.categoria || 'Sin Categoría';
            quantity = parseNumber(entry.cantidad);
            price = parseNumber(entry.precio || entry.unit_price);
            province = entry.provincia || 'Desconocido';
            processId = entry.process_id || entry.id || entry.trazabilidad;
            cuit = entry.cuit || null;
            result = entry.resultado || 'Desconocido';
        }

        if (!dateVal) return;

        // --- RELATION LOGIC (CLIENT FILTER) ---
        // "Si tiene CUIT = Cliente, sino No Cliente"
        const relation = cuit ? 'Cliente' : 'No Cliente';

        // --- ROBUST PROCESS ID ---
        if (!processId || processId === 'unknown') {
            processId = `${client}_${dateVal.getTime()}_${index}`;
        }

        // --- POPULATE OPTIONS ---
        optionsClients.add(client);
        optionsProvinces.add(province);
        optionsCategories.add(category); // "Evolución por Categoría" -> Use Category for filter? Or Product? 
        // User asked to Group Evolution by Category. Filters usually filter the whole dataset. 
        // Let's assume Filter Category dropdown matches the Evolution Category grouping.

        // --- FILTERING ---
        if (start && dateVal < start) return;
        if (end && dateVal > end) return;
        if (fClient && client.toLowerCase() !== fClient) return;
        if (fProvince && province.toLowerCase() !== fProvince) return;
        // if (fCategory && product.toLowerCase() !== fCategory) return; // Wait, filter is by Category or Product? Default 'filterCategory' label says 'Categoría'.
        if (fCategory && category.toLowerCase() !== fCategory) return;

        // Client Filter (Switch Logic)
        if (fIsClient === 'yes') {
            if (relation !== 'Cliente') return;
        } else if (fIsClient === 'no') {
            if (relation !== 'No Cliente') return;
        }
        // if 'all', ignore

        // --- AGGREGATION ---

        clients.add(client);
        categories.add(category); // For KPI metrics
        uniqueProcesses.add(processId);

        const m = dateVal.getMonth() + 1;
        const y = dateVal.getFullYear();
        const monthKey = `${y}-${String(m).padStart(2, '0')}`;
        allMonths.add(monthKey);

        // 3. Evolution: Group by CATEGORY, Count ROWS
        if (!areaMap[monthKey]) areaMap[monthKey] = {};
        if (!areaMap[monthKey][category]) areaMap[monthKey][category] = 0;
        areaMap[monthKey][category] += 1;

        // 4. Share: Group by RESULT, Count ROWS
        if (!resultMap[result]) resultMap[result] = 0;
        resultMap[result]++;

        // 7. Top Families (Product/Family): Add Price
        // Sticking to 'product' for Families list as it's more granular? User said "Top Familias".
        // Usually Family != Category in Bionexo.
        // Let's use 'product' (which maps to family/producto) for this list.
        if (!familyStats[product]) familyStats[product] = { count: 0, revenue: 0, qty: 0 };
        familyStats[product].count += 1;
        familyStats[product].qty += quantity;
        familyStats[product].revenue += (quantity * price);

        // 5. Client Bar: Group by RESULT per Client, Count ROWS
        if (!clientResultMap[client]) clientResultMap[client] = {};
        if (!clientResultMap[client][result]) clientResultMap[client][result] = 0;
        clientResultMap[client][result]++;

        // 6. Matrix (Pivot): MEDIAN of QUANTITY per Month
        // Group by 'product' (Family) or 'category'? Title says "Familia". 
        // I will use 'product' (Family) to match Top Families list.
        if (!pivotMap[product]) pivotMap[product] = {};
        if (!pivotMap[product][monthKey]) pivotMap[product][monthKey] = [];
        pivotMap[product][monthKey].push(quantity);
    });

    // --- AGGREGATION FINALIZATION ---

    const sortedMonths = Array.from(allMonths).sort();

    // Area: Top 5 Categories by total rows
    const catTotals = {};
    Object.keys(areaMap).forEach(m => {
        Object.keys(areaMap[m]).forEach(cat => {
            catTotals[cat] = (catTotals[cat] || 0) + areaMap[m][cat];
        });
    });
    const topCategories = Object.entries(catTotals).sort((a, b) => b[1] - a[1]).slice(0, 5).map(x => x[0]);

    // Pie Data
    const pieData = Object.entries(resultMap).map(([k, v]) => ({ label: k, value: v }));

    // Family List (Top 50 by Qty, show Price)
    const familyList = Object.entries(familyStats)
        .sort((a, b) => b[1].qty - a[1].qty)
        .slice(0, 50)
        .map(([k, meta]) => {
            const avgPrice = meta.qty > 0 ? (meta.revenue / meta.qty) : 0;
            return {
                name: k,
                count: meta.qty,
                price: avgPrice
            };
        });

    // Client Bar: Top 10 by total rows
    // Need to handle dynamic Result keys in the Stacked Bar
    const topClients = Object.entries(clientResultMap)
        .map(([name, resMap]) => ({
            name,
            total: Object.values(resMap).reduce((a, b) => a + b, 0),
            counts: resMap
        }))
        .sort((a, b) => b.total - a.total)
        .slice(0, 10);

    // Pivot: Median Calculation
    // Using top families from familyStats (by Qty) for the matrix rows
    const topFamiliesForPivot = Object.entries(familyStats)
        .sort((a, b) => b[1].qty - a[1].qty)
        .slice(0, 20)
        .map(x => x[0]);

    const pivotData = topFamiliesForPivot.map(fam => {
        const row = { family: fam };
        sortedMonths.forEach(m => {
            const values = (pivotMap[fam] && pivotMap[fam][m]) || [];
            if (values.length === 0) {
                row[m] = 0;
            } else {
                row[m] = calculateMedian(values);
            }
        });
        return row;
    });

    return {
        kpis: {
            clients: clients.size,
            processes: uniqueProcesses.size, // Distinct Count
            families: Object.keys(familyStats).length
        },
        filters: {
            clients: Array.from(optionsClients).sort(),
            provinces: Array.from(optionsProvinces).sort(),
            categories: Array.from(optionsCategories).sort()
        },
        charts: {
            months: sortedMonths,
            topCategories,
            areaMap,
            pie: pieData,
            familyList,
            clientBar: topClients,
            pivot: {
                months: sortedMonths,
                data: pivotData
            }
        }
    };
}

function calculateMedian(values) {
    if (values.length === 0) return 0;
    values.sort((a, b) => a - b);
    const half = Math.floor(values.length / 2);
    if (values.length % 2) return values[half];
    return (values[half - 1] + values[half]) / 2.0;
}

function normalizeEntry(item) {
    const out = {};
    Object.keys(item).forEach(k => {
        out[k.toLowerCase().trim()] = item[k];
    });
    return out;
}

function parseDate(dateStr) {
    if (!dateStr) return null;
    if (dateStr instanceof Date) return dateStr;
    if (typeof dateStr === 'number' && dateStr > 20000) {
        return new Date(Math.round((dateStr - 25569) * 86400 * 1000));
    }
    const s = String(dateStr).trim();
    let d = new Date(s);
    if (!isNaN(d.getTime())) return d;
    const match = s.match(/^(\d{1,2})[\/\-](\d{1,2})[\/\-](\d{4})/);
    if (match) return new Date(+match[3], +match[2] - 1, +match[1]);
    return null;
}

function parseNumber(val) {
    if (typeof val === 'number') return val;
    if (!val) return 0;
    let clean = String(val).trim();
    clean = clean.replace(/[^\d,\.-]/g, '');
    if (clean.includes(',') && clean.includes('.')) {
        if (clean.lastIndexOf(',') > clean.lastIndexOf('.')) clean = clean.replace(/\./g, '').replace(',', '.');
        else clean = clean.replace(/,/g, '');
    } else if (clean.includes(',')) clean = clean.replace(',', '.');
    else if (clean.includes('.')) {
        if (/^\d{1,3}(\.\d{3})+$/.test(clean)) clean = clean.replace(/\./g, '');
    }
    const n = parseFloat(clean);
    return isNaN(n) ? 0 : n;
}
