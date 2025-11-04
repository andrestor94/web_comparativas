// static/js/chart-ux.js
(function () {
  if (!window.Chart) {
    console.warn('[ChartUX] Chart.js no está disponible todavía.');
    return;
  }

  // Registrar plugins si existen (defensivo: no falla si ya están)
  try {
    if (window['chartjs-plugin-zoom']) {
      Chart.register(window['chartjs-plugin-zoom']);
    }
  } catch (e) { console.warn('[ChartUX] zoom no registrado:', e); }

  try {
    if (window.ChartDataLabels) {
      Chart.register(window.ChartDataLabels);
    }
  } catch (e) { console.warn('[ChartUX] datalabels no registrado:', e); }

  // ---- Utilidades de formato ----
  const fmtPeso = (v) => {
    try {
      return new Intl.NumberFormat('es-AR', {
        style: 'currency', currency: 'ARS', maximumFractionDigits: 0
      }).format(v ?? 0);
    } catch {
      return '$ ' + (v ?? 0).toLocaleString('es-AR');
    }
  };

  // ---- Event Bus simple para enlazar gráficos entre sí ----
  const bus = new EventTarget();

  // ---- Zoom / Pan con reset por doble clic ----
  function attachZoom(chart, { mode = 'xy' } = {}) {
    chart.options.plugins = chart.options.plugins || {};
    chart.options.plugins.zoom = {
      pan: { enabled: true, mode },
      zoom: {
        wheel: { enabled: true },
        pinch: { enabled: true },
        drag: { enabled: true, modifierKey: 'ctrl' },
        mode
      },
      limits: {
        x: { min: 'original', max: 'original' },
        y: { min: 'original', max: 'original' }
      }
    };
    chart.update('none');
    chart.canvas.addEventListener('dblclick', () => {
      if (typeof chart.resetZoom === 'function') chart.resetZoom();
    });
  }

  // ---- Hover enlazado (resalta el mismo proveedor en otros gráficos) ----
  function wireLinkedHover(chart, getLabel) {
    const emit = (type, detail) => bus.dispatchEvent(new CustomEvent(type, { detail }));
    chart.options.onHover = (_, elements) => {
      if (elements && elements.length) {
        const el = elements[0];
        const lbl = getLabel(el);
        if (lbl) emit('provider:hover', { provider: lbl });
      } else {
        emit('provider:hover', { provider: null });
      }
    };

    bus.addEventListener('provider:hover', (e) => {
      const provider = e.detail.provider;
      highlightByProvider(chart, getLabel, provider);
    });
  }

  // ---- Click = filtrar (ej.: llenar tu input de búsqueda) ----
  function wireClickToFilter(chart, getLabel, { targetInputSelector } = {}) {
    const emit = (type, detail) => bus.dispatchEvent(new CustomEvent(type, { detail }));
    chart.options.onClick = (_, elements) => {
      if (!elements || !elements.length) return;
      const lbl = getLabel(elements[0]);
      if (!lbl) return;

      emit('provider:filter', { provider: lbl });

      if (targetInputSelector) {
        const input = document.querySelector(targetInputSelector);
        if (input) {
          input.value = lbl;
          // dispara filtrado en tus scripts
          input.dispatchEvent(new Event('input', { bubbles: true }));
          input.dispatchEvent(new Event('change', { bubbles: true }));
        }
      }
    };
  }

  // ---- Resaltado por proveedor (atenúa lo que no coincide) ----
  function highlightByProvider(chart, getLabel, provider) {
    const dsArr = chart.data?.datasets || [];
    const isPie = chart.config.type === 'pie' || chart.config.type === 'doughnut';

    dsArr.forEach((ds, di) => {
      // guardar colores base una vez
      if (!ds._baseColors) {
        const base = Array.isArray(ds.backgroundColor)
          ? ds.backgroundColor.slice()
          : Array.from({ length: (ds.data || []).length }, () => ds.backgroundColor || '#94a3b8');
        ds._baseColors = base;
      }

      if (isPie) {
        ds.backgroundColor = ds._baseColors.map((base, i) => {
          const label = getLabel({ datasetIndex: di, index: i });
          if (!provider || (label && label === provider)) return base;
          return 'rgba(148,163,184,0.35)'; // atenuado
        });
      } else {
        // para barras/lineas no tocamos colores por defecto, solo podemos ajustar opacidad si hace falta
      }
    });

    chart.update('none');
  }

  // Exponer API
  window.ChartUX = {
    ok: true,
    bus,
    fmtPeso,
    attachZoom,
    wireLinkedHover,
    wireClickToFilter,
    highlightByProvider
  };

  console.info('[ChartUX] listo y cargado');
})();
