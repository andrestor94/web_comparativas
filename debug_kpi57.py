"""
Script de diagnóstico para depurar KPI 5 (Coincidencia) y KPI 7 (Facturado 2026)
"""
import pandas as pd
import numpy as np

FACT_2026 = r'web_comparativas\data\forecast_data\facturacion_real_2026.csv'
VAL_PARQUET = r'web_comparativas\data\forecast_data\fact_forecast_valorizado.parquet'

# Cargar fact_2026 (simulando la migración a DB)
df = pd.read_csv(FACT_2026, sep=',', encoding='utf-8')
df.columns = [c.lower().strip() for c in df.columns]
df['fecha'] = pd.to_datetime(df['fecha'], errors='coerce')
df['fecha'] = df['fecha'].dt.to_period('M').dt.to_timestamp()
df = df[df['fecha'] >= '2026-01-01'].copy()
df['imp_hist'] = pd.to_numeric(df['imp_hist'], errors='coerce').fillna(0)

print('=== fact_2026 CSV LOCAL ===')
print('Columnas:', list(df.columns))
print('Filas 2026:', len(df))
if 'codigo_serie' in df.columns:
    print('Codigos_serie distintos:', df['codigo_serie'].nunique())
if 'perfil' in df.columns:
    print('Perfiles:', df['perfil'].unique())

total_sin_filtro = df['imp_hist'].sum()
print(f'\nTotal SIN filtro: {total_sin_filtro/1e9:.4f}B = {round(total_sin_filtro/1e9, 1)}B')

# Cargar valorizado (para ver qué series tiene)
print('\n=== CARGANDO VALORIZADO ===')
df_val = pd.read_parquet(VAL_PARQUET)
df_val.columns = [c.lower().strip() for c in df_val.columns]
if 'periodo' in df_val.columns and 'fecha' not in df_val.columns:
    df_val['fecha'] = pd.to_datetime(df_val['periodo'], format='%Y-%m', errors='coerce')
elif 'fecha' in df_val.columns:
    df_val['fecha'] = pd.to_datetime(df_val['fecha'], errors='coerce')

series_en_valorizado = set(df_val['codigo_serie'].astype(str).unique())
print(f'Series distintas en valorizado: {len(series_en_valorizado)}')

# Filtrar como hace imp_hist (la subquery AND codigo_serie IN (SELECT DISTINCT codigo_serie FROM forecast_valorizado))
if 'codigo_serie' in df.columns:
    df_cs = df['codigo_serie'].astype(str)
    mascara_en_val = df_cs.isin(series_en_valorizado)
    df_filtrado = df[mascara_en_val].copy()
    
    total_con_filtro = df_filtrado['imp_hist'].sum()
    filas_en_val = mascara_en_val.sum()
    print(f'\nFilas de fact_2026 con codigo_serie en valorizado: {filas_en_val} / {len(df)}')
    print(f'Total CON filtro (solo series en valorizado): {total_con_filtro/1e9:.4f}B = {round(total_con_filtro/1e9, 1)}B')
    
    print('\nPor mes SIN filtro:')
    por_mes_sin = df.groupby('fecha')['imp_hist'].sum()
    for idx, val in por_mes_sin.items():
        print(f'  {idx.strftime("%Y-%m")}: {val/1e9:.4f}B')
    
    print('\nPor mes CON filtro (solo series en valorizado):')
    por_mes_con = df_filtrado.groupby('fecha')['imp_hist'].sum()
    for idx, val in por_mes_con.items():
        print(f'  {idx.strftime("%Y-%m")}: {val/1e9:.4f}B')

# Ahora calcular accuracy con valorizado
print('\n=== CALCULAR KPI5 (accuracy) ===')
df_fcst = df_val.groupby('fecha').agg(Total_Forecast=('monto_yhat', 'sum')).reset_index()
df_fcst['Total_Adj'] = df_fcst['Total_Forecast']
mask_2026_f = df_fcst['fecha'].dt.year == 2026
total_adj = float(df_fcst.loc[mask_2026_f, 'Total_Adj'].sum())
print(f'total_adj (forecast 2026): {total_adj/1e9:.4f}B')

# Calcular como hace la PG (sin filtro en fact_2026)
df_fact_grouped = df.groupby('fecha').agg(Total_Venta=('imp_hist', 'sum')).reset_index()
val_months = sorted(m for m in df_fact_grouped['fecha'].dropna().unique() if pd.Timestamp(m).year == 2026)
closed = val_months[:-1] if len(val_months) > 1 else val_months

scores_pg = []
for m in closed:
    actual = float(df_fact_grouped[df_fact_grouped['fecha'] == m]['Total_Venta'].sum())
    proj = float(df_fcst[df_fcst['fecha'] == m]['Total_Forecast'].sum()) if not df_fcst.empty else 0.0
    score = max(0.0, (1 - abs(actual - proj) / actual) * 100) if actual > 0 else 0.0
    scores_pg.append(score)
    print(f'  SIN FILTRO {m.strftime("%Y-%m")}: actual={actual/1e9:.4f}B proj={proj/1e9:.4f}B score={score:.4f}%')

acc_pg = float(np.mean(scores_pg)) if scores_pg else 0.0
print(f'KPI5 sin filtro (como hace prod actual): {acc_pg:.4f}% => {round(acc_pg, 1)}%')

# Calcular CON filtro por codigo_serie en valorizado (como debería ser para igualar referencia)
if 'codigo_serie' in df.columns:
    df_fact_filt_grouped = df_filtrado.groupby('fecha').agg(Total_Venta=('imp_hist', 'sum')).reset_index()
    val_months_f = sorted(m for m in df_fact_filt_grouped['fecha'].dropna().unique() if pd.Timestamp(m).year == 2026)
    closed_f = val_months_f[:-1] if len(val_months_f) > 1 else val_months_f
    
    scores_f = []
    for m in closed_f:
        actual = float(df_fact_filt_grouped[df_fact_filt_grouped['fecha'] == m]['Total_Venta'].sum())
        proj = float(df_fcst[df_fcst['fecha'] == m]['Total_Forecast'].sum()) if not df_fcst.empty else 0.0
        score = max(0.0, (1 - abs(actual - proj) / actual) * 100) if actual > 0 else 0.0
        scores_f.append(score)
        print(f'  CON FILTRO  {m.strftime("%Y-%m")}: actual={actual/1e9:.4f}B proj={proj/1e9:.4f}B score={score:.4f}%')
    
    acc_f = float(np.mean(scores_f)) if scores_f else 0.0
    print(f'KPI5 con filtro (solo series en valorizado): {acc_f:.4f}% => {round(acc_f, 1)}%')
    print(f'KPI7 con filtro: {df_filtrado["imp_hist"].sum()/1e9:.4f}B => {round(df_filtrado["imp_hist"].sum()/1e9, 1)}B')

print('\n=== RESUMEN ===')
print(f'KPI7 sin filtro:        {round(total_sin_filtro/1e9, 1)}B  (prod actual: 17.7B)')
if 'codigo_serie' in df.columns:
    filt_sum = df_filtrado['imp_hist'].sum()
    print(f'KPI7 con filtro val:    {round(filt_sum/1e9, 1)}B')
print(f'KPI5 sin filtro:        {round(acc_pg, 1)}%   (prod actual: 91.2%)')
if 'codigo_serie' in df.columns:
    print(f'KPI5 con filtro val:    {round(acc_f, 1)}%')
print(f'Objetivo KPI7:          17.6B')  
print(f'Objetivo KPI5:          90.9%')
