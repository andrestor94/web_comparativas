"""
PROPUESTA (Fase 3) — Modelos SQLAlchemy READ-ONLY para las tablas `forecast_*`
de datos base que hoy viven en PostgreSQL producción y NO están en el ORM.

⚠️  ESTE MÓDULO ES INERTE A PROPÓSITO:
    - Usa su PROPIO declarative_base() (`ProposedBase`), NO el `Base` de la app.
      Por eso NO se registra en `web_comparativas.models.Base.metadata` y NO lo
      toca `create_all`. Importarlo no crea ni altera ninguna tabla.
    - NADIE lo importa todavía. Es material de revisión.
    - Los tipos/columnas son INFERIDOS desde los scripts de ingesta
      (migrate_forecast_csv_to_postgres.py, reload_valorizado.py,
      migrate_local_to_render.py) y desde las queries de forecast_service.py.
      DEBEN validarse contra el esquema real de PostgreSQL (ver
      docs/forecast_tables_orm_proposal.md → "Incógnitas").

Cuando se decida integrarlas de verdad, la estrategia propuesta es:
    - Definirlas con el `Base` real PERO marcarlas como read-only a nivel de
      aplicación (nunca INSERT/UPDATE/DELETE desde la app; la carga sigue por los
      scripts de ingesta existentes).
    - Excluirlas de `create_all` en SQLite local (o aceptar que queden vacías),
      ya que en local los datos base son CSV/parquet.
    - Registrar su esquema en Alembic vía `stamp` del baseline (sin recrearlas).
"""
from __future__ import annotations

from sqlalchemy import Column, Float, Integer, String, Text, DateTime, Index
from sqlalchemy.orm import declarative_base

# Base PROPIO e independiente → no contamina el metadata real de la app.
ProposedBase = declarative_base()


class ForecastMain(ProposedBase):
    """forecast_main — series base del forecast (origen: forecast_base_consolidado.csv).

    Carga: migrate_forecast_csv_to_postgres.py:44 (to_sql if_exists="replace").
    Índices reales: idx_fc_main_{perfil,neg,subneg,codigo_serie}.
    """
    __tablename__ = "forecast_main"

    # No hay PK real en la tabla productiva (to_sql index=False). Se declara una
    # PK sintética sobre rowid lógico SOLO para que el ORM pueda mapear; NO crear.
    _rowid = Column("rowid", Integer, primary_key=True)  # placeholder ORM; revisar

    perfil = Column(String, index=True)
    neg = Column(String, index=True)
    subneg = Column(String, index=True)
    codigo_serie = Column(String, index=True)
    fecha = Column(DateTime)
    tipo = Column(String)          # 'hist' / forecast
    yhat = Column(Float)
    yhat_lower = Column(Float)
    yhat_upper = Column(Float)
    monto_yhat = Column(Float)
    y = Column(Float)
    precio = Column(Float)
    descripcion = Column(Text)
    familia = Column(Text)


class ForecastValorizado(ProposedBase):
    """forecast_valorizado — ~702k filas (origen: fact_forecast_valorizado.parquet).

    Carga: migrate_forecast_csv_to_postgres.py:139 / reload_valorizado.py.
    Validación de negocio: SUM(monto_yhat) ≈ referencia exacta.
    Índices reales: idx_fc_val_{fecha,perfil,codigo_serie,cliente_id}.
    """
    __tablename__ = "forecast_valorizado"

    _rowid = Column("rowid", Integer, primary_key=True)  # placeholder ORM; revisar

    codigo_serie = Column(String, index=True)
    fecha = Column(DateTime, index=True)
    monto_yhat = Column(Float)
    monto_li = Column(Float)
    monto_ls = Column(Float)
    perfil = Column(String, index=True)
    cliente_id = Column(String, index=True)
    fantasia = Column(String)
    nombre_grupo = Column(String)
    neg = Column(String)
    subneg = Column(String)
    descripcion = Column(Text)


class ForecastImpHist(ProposedBase):
    """forecast_imp_hist — histórico real (origen: importe_historico.csv).

    Carga: migrate_forecast_csv_to_postgres.py:165. Índice: idx_fc_hist_perfil.
    """
    __tablename__ = "forecast_imp_hist"

    _rowid = Column("rowid", Integer, primary_key=True)  # placeholder ORM; revisar

    perfil = Column(String, index=True)
    codigo_serie = Column(String)
    fecha = Column(DateTime)
    imp_hist = Column(Float)
    tipo = Column(String)


class ForecastFact2026(ProposedBase):
    """forecast_fact_2026 — facturación real Ene-Abr 2026.

    Carga: migrate_forecast_csv_to_postgres.py:198 / migrate_local_to_render.py.
    OJO: 'perfil' es interno; 'tipocli' es el perfil comercial verdadero.
    Índices reales: idx_fc_fact2026_{tipocli,cliente,fecha}.
    """
    __tablename__ = "forecast_fact_2026"

    _rowid = Column("rowid", Integer, primary_key=True)  # placeholder ORM; revisar

    fecha = Column(DateTime, index=True)
    cliente_id = Column(String, index=True)
    codigo_serie = Column(String)
    imp_hist = Column(Float)
    perfil = Column(String)       # interno (ej. "9 - 1")
    tipocli = Column(String, index=True)  # perfil comercial real
    tipo = Column(String)


class ForecastProductLabs(ProposedBase):
    """forecast_product_labs — mapping codigo_serie -> laboratorios (JSON como texto).

    Carga: migrate_forecast_csv_to_postgres.py:232. Índice: idx_fc_labs_cdg.
    """
    __tablename__ = "forecast_product_labs"

    _rowid = Column("rowid", Integer, primary_key=True)  # placeholder ORM; revisar

    codigo_serie = Column(String, index=True)
    laboratorios = Column(Text)   # JSON serializado como string


# Índices declarados arriba inline; este bloque queda como recordatorio de los
# índices REALES creados por los scripts de ingesta (para no duplicarlos).
_REAL_INDEXES_REFERENCE = {
    "forecast_main": ["perfil", "neg", "subneg", "codigo_serie"],
    "forecast_valorizado": ["fecha", "perfil", "codigo_serie", "cliente_id"],
    "forecast_imp_hist": ["perfil"],
    "forecast_fact_2026": ["tipocli", "cliente_id", "fecha"],
    "forecast_product_labs": ["codigo_serie"],
}
