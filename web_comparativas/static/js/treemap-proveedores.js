/* Treemap de Proveedores – Paleta Suizo
   Requiere:
   - chart.umd.min.js
   - chartjs-chart-treemap.min.js
   - window.CHART_PROV = [{label, value}, ...] inyectado desde Jinja
*/

(() => {
  // ---- Paleta corporativa ----
  const SA_AZUL      = '#064066';   // azul corporativo
  const SA_AZUL_SEC  = '#638AE5';   // azul secundario
  const SA_GRIS_FON  = '#EBEBEB';   // fondo contenedor
  const SA_BLANCO    = '#ffffff';   // bordes/etiquetas sobre azul

  // ---- Helpers de colores / formato ----
  const hex2rgb = (hex) => {
    const [, r, g, b] = /^#?([a-f\d]{2})([a-f\d]{2})([a-f\d]{2})$/i.exec(hex) || [];
    return r ? [parseInt(r, 16), parseInt(g, 16), parseInt(b, 16)] : [0, 0, 0];
  };
  const rgb2hex = (r, g, b) =>
    '#' + [r, g, b].map(v => v.toString(16).padStart(2, '0')).join('');

  const lerp = (a, b, t) => a + (b - a) * t;
  const blend = (hexA, hexB, t) => {
    const A = hex2rgb(hexA), B = hex2rgb(hexB);
    return rgb2hex(
      Math.round(lerp(A[0], B[0], t)),
      Math.round(lerp(A[1], B[1], t)),
      Math.round(lerp(A[2], B[2], t))
    );
  };

  const nfARS = (v) => new Intl.NumberFormat('es-AR', {
    style: 'currency', currency: 'ARS', maximumFractionDigits: 0
  }).format(+v || 0);

  // ---- Datos (inyectados desde el template) ----
  const data = Array.isArray(window.CHART_PROV) ? window.CHART_PROV : [];
  const values = data.map(d => +d.value || 0);
  const minV = Math.min(...values, 0);
  const maxV = Math.max(...values, 1);
  const span = Math.max(1, maxV - minV);

  // escala 0..1 para colorear; más valor => más oscuro
  const colorFor = (v) => {
    const t = Math.min(1, Math.max(0, (v - minV) / span));    // 0..1
    // invertimos para arrancar en azul secundario (claro) y terminar en azul corporativo (oscuro)
    return blend(SA_AZUL_SEC, SA_AZUL, t);
  };

  // ---- Plugin para colorear el área del gráfico (gris Suizo) ----
  const ChartAreaBackground = {
    id: 'chartAreaBackground',
    beforeDraw(chart, _args, opts) {
      const {ctx, chartArea} = chart;
      if (!chartArea) return;
      ctx.save();
      ctx.fillStyle = opts?.color || SA_GRIS_FON;
      ctx.fillRect(chartArea.left, chartArea.top, chartArea.right - chartArea.left, chartArea.bottom - chartArea.top);
      ctx.restore();
    }
  };

  // ---- Render ----
  const el = document.getElementById('treemapProveedores');
  if (!el) return;

  // tamaño amigable: más alto si hay muchos proveedores
  const dynamicH = Math.max(220, 26 * Math.max(8, data.length) + 24);
  el.height = dynamicH;
  el.style.height = dynamicH + 'px';

  // Chart.js defaults (coinciden con el resto del dashboard)
  Chart.defaults.maintainAspectRatio = false;
  Chart.defaults.animation = false;
  Chart.defaults.color = '#1e3a4a';
  Chart.defaults.plugins.legend.display = false;
  Chart.defaults.plugins.tooltip.backgroundColor = SA_BLANCO;
  Chart.defaults.plugins.tooltip.borderColor = SA_GRIS_FON;
  Chart.defaults.plugins.tooltip.borderWidth = 1;
  Chart.defaults.plugins.tooltip.titleColor = '#0f172a';
  Chart.defaults.plugins.tooltip.bodyColor = '#0f172a';

  new Chart(el.getContext('2d'), {
    type: 'treemap',
    data: {
      datasets: [{
        label: 'Participación de oferentes (treemap)',
        tree: data.map(d => ({ g: d.label, v: +d.value || 0 })),
        key: 'v',
        groups: ['g'],
        spacing: 2,
        borderWidth: 2,
        borderColor: SA_BLANCO,
        backgroundColor(ctx) {
          const v = ctx.raw && typeof ctx.raw.v === 'number' ? ctx.raw.v : 0;
          return colorFor(v);
        },
        hoverBackgroundColor(ctx) {
          const v = ctx.raw && typeof ctx.raw.v === 'number' ? ctx.raw.v : 0;
          // sombreado levemente más oscuro al pasar el mouse
          return blend(colorFor(v), SA_AZUL, 0.2);
        },
        hoverBorderColor: SA_AZUL,
        labels: {
          display: true,
          // color del texto: blanco para contraste sobre azules
          color: SA_BLANCO,
          formatter(ctx) {
            // ctx.raw.g = grupo (proveedor), ctx.raw.v = valor
            const name = String(ctx.raw.g || '');
            const val = nfARS(ctx.raw.v || 0);
            // nombre en 1a línea, monto en 2a línea
            return name + '\n' + val;
          },
          // fuente un poco más fuerte para legibilidad
          font: { weight: '600', size: 11 },
          // mostrar siempre dentro del rectángulo si hay espacio
          align: 'center',
        },
      }]
    },
    options: {
      layout: { padding: 4 },
      plugins: {
        chartAreaBackground: { color: SA_GRIS_FON },
        tooltip: {
          callbacks: {
            title(items) {
              const it = items?.[0];
              return it && it.raw ? String(it.raw.g || '') : '';
            },
            label(it) {
              const v = it?.raw?.v || 0;
              return 'Monto: ' + nfARS(v);
            }
          }
        }
      }
    },
    plugins: [ChartAreaBackground]
  });
})();
