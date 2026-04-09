"""
Patch para KPI 5 (Coincidencia 90.9%) y KPI 7 (Facturado 2026 17.6B).
Agrega el mismo filtro canónico de series a forecast_fact_2026 que ya se
usa en forecast_imp_hist. Esto restringe a las 3039 series del modelo.
"""
import re
from pathlib import Path

SVC = Path("web_comparativas/forecast_service.py")
content = SVC.read_text(encoding="utf-8")

# Localizar el bloque a reemplazar
OLD = (
    '    logger.info("[FORECAST chart] step=query_fact2026")\r\n'
    '    # Facturación real 2026\r\n'
    '    df_fact_raw = _query_agg(\r\n'
    '        f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "\r\n'
    '        f"FROM forecast_fact_2026 WHERE {fact_where} AND fecha >= \'2026-01-01\' "\r\n'
    '        f"GROUP BY fecha ORDER BY fecha"\r\n'
    '    )\r\n'
)

NEW = (
    '    logger.info("[FORECAST chart] step=query_fact2026")\r\n'
    '    # Facturación real 2026\r\n'
    '    # CANONICAL SERIES FILTER: restrict fact_2026 to the series that exist in\r\n'
    '    # forecast_valorizado (same inner-join the original app.py applied at load time).\r\n'
    '    # Without this filter, extra rows from series not in the model are included,\r\n'
    '    # inflating the total: 17.661B (17.7B) instead of 17.618B (17.6B),\r\n'
    '    # and accuracy: 91.2% instead of 90.9%.  Mirrors the identical filter on\r\n'
    '    # forecast_imp_hist (AND codigo_serie IN (SELECT DISTINCT ... FROM forecast_valorizado)).\r\n'
    '    df_fact_raw = _query_agg(\r\n'
    '        f"SELECT fecha, SUM(COALESCE(imp_hist, 0)) AS total_venta "\r\n'
    '        f"FROM forecast_fact_2026 WHERE {fact_where} AND fecha >= \'2026-01-01\' "\r\n'
    '        f"AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado) "\r\n'
    '        f"GROUP BY fecha ORDER BY fecha"\r\n'
    '    )\r\n'
)

if OLD in content:
    new_content = content.replace(OLD, NEW, 1)
    SVC.write_text(new_content, encoding="utf-8")
    print("✅ PATCH APLICADO CORRECTAMENTE")
    print("   KPI7 (Facturado 2026) pasará de 17.7B → 17.6B")
    print("   KPI5 (Coincidencia)   pasará de 91.2% → 90.9%")
else:
    # Intentar con LF puro
    OLD_LF = OLD.replace('\r\n', '\n')
    NEW_LF = NEW.replace('\r\n', '\n')
    if OLD_LF in content:
        new_content = content.replace(OLD_LF, NEW_LF, 1)
        SVC.write_text(new_content, encoding="utf-8")
        print("✅ PATCH APLICADO (LF puro) CORRECTAMENTE")
    else:
        print("❌ TARGET NO ENCONTRADO. Mostrando contexto:")
        idx = content.find("step=query_fact2026")
        if idx >= 0:
            print(repr(content[idx:idx+500]))
        else:
            print("No se encontró 'step=query_fact2026' en el archivo")
