"""Modelos ORM de las tablas *summary* publicadas del módulo Indicadores Comerciales.

Mismo mecanismo que el módulo Dimensionamiento: clases SQLAlchemy declarativas sobre
el `Base` compartido de web_comparativas.models. La materialización ocurre vía
`Base.metadata.create_all(bind=engine)` en el arranque (web_comparativas/main.py),
exactamente igual que dimensionamiento/models.py.

Tipos por dialecto: se usan tipos genéricos de SQLAlchemy, que el dialecto resuelve
solo (Integer/BigInteger/SmallInteger, Date, DateTime, String, Text, Float). Las
columnas `money` del origen SQL Server se modelan como `Numeric(19, 4)`, que SQLAlchemy
emite como NUMERIC(19, 4) en PostgreSQL y como NUMERIC (afinidad numérica) en SQLite.

Cada tabla "publicada" tiene una gemela `_staging` de esquema idéntico: el esquema de
columnas se define una sola vez en un mixin y se hereda en ambas clases, de modo que
publicada y staging no puedan divergir. Los índices de columna (index=True) toman el
nombre `ix_<tabla>_<columna>`, distinto por tabla, así no colisionan entre gemelas.

FASE 1: solo esquema. Ningún endpoint lee todavía de estas tablas (gate inerte en
indicadores_db._indicadores_summary_available, kill-switch INDICADORES_USE_SUMMARY).
"""

from __future__ import annotations

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
)

from web_comparativas.models import Base


# ──────────────────────────────────────────────────────────────────────────────
# (a) Control de watermark/estado del ETL incremental — tabla única, SIN staging.
# ──────────────────────────────────────────────────────────────────────────────
class IndEtlControl(Base):
    __tablename__ = "ind_etl_control"

    fuente = Column(String(60), primary_key=True)
    watermark_idhisto = Column(BigInteger, nullable=True)
    ventana_desde = Column(Date, nullable=True)
    ventana_hasta = Column(Date, nullable=True)
    ultima_corrida = Column(DateTime, nullable=True)
    ultima_aprobacion = Column(DateTime, nullable=True)
    estado = Column(String(20), nullable=False, default="idle")  # idle/staging/aprobado/error
    filas_staging = Column(Integer, nullable=True)
    filas_publicado = Column(Integer, nullable=True)
    nota = Column(Text, nullable=True)


# ──────────────────────────────────────────────────────────────────────────────
# (b) Rentabilidad nivel línea (origen: ETL_Data.dbo.rentabilidad_cliente + clientes)
# ──────────────────────────────────────────────────────────────────────────────
class _RentabilidadLineasCols:
    # Surrogate PK: la grilla de líneas no tiene clave natural única (igual que
    # dimensionamiento_records.id). El watermark vive en ind_etl_control, no acá.
    id = Column(Integer, primary_key=True)
    ctacte = Column(Integer, nullable=True, index=True)            # clientes.codigo / rentabilidad_cliente.ctacte (int)
    cliente_grupo = Column(Integer, nullable=True)                 # dbo.clientes.cliente_grupo (int, null)
    nombre_cliente = Column(Text, nullable=True)
    articulo = Column(Integer, nullable=False, index=True)         # rentabilidad_cliente.articulo (int, not null)
    cadneg = Column(String(16), nullable=True)                     # rentabilidad_cliente.cadneg (varchar 6)
    fecha = Column(DateTime, nullable=True, index=True)            # datetime del origen — NO se baja a date
    cant = Column(Integer, nullable=True)                          # rentabilidad_cliente.cant (int)
    importe = Column(Numeric(19, 4), nullable=True)               # money -> NUMERIC(19,4)
    renta1 = Column(Numeric(19, 4), nullable=True)               # money -> NUMERIC(19,4)
    comprob = Column(String(8), nullable=True)                    # rentabilidad_cliente.comprob (varchar 3)


class IndRentabilidadLineas(_RentabilidadLineasCols, Base):
    __tablename__ = "ind_rentabilidad_lineas"


class IndRentabilidadLineasStaging(_RentabilidadLineasCols, Base):
    __tablename__ = "ind_rentabilidad_lineas_staging"


# ──────────────────────────────────────────────────────────────────────────────
# (c) Inflación PVP agregada articulo×mes (origen: Fusion.dbo.histopre.prepubact)
# ──────────────────────────────────────────────────────────────────────────────
class _InflacionPvpMensualCols:
    articulo = Column(Integer, primary_key=True)                  # histopre.articulo (int)
    mes = Column(String(7), primary_key=True)                     # 'YYYY-MM'
    fecha_snapshot = Column(Date, nullable=True)
    pvp = Column(Numeric(19, 4), nullable=True)                  # money -> NUMERIC(19,4)


class IndInflacionPvpMensual(_InflacionPvpMensualCols, Base):
    __tablename__ = "ind_inflacion_pvp_mensual"


class IndInflacionPvpMensualStaging(_InflacionPvpMensualCols, Base):
    __tablename__ = "ind_inflacion_pvp_mensual_staging"


# ──────────────────────────────────────────────────────────────────────────────
# (d) Inflación facturación agregada articulo×cadneg×mes
#     (origen: ETL_Data.dbo.rentabililad_x_cliente)
# ──────────────────────────────────────────────────────────────────────────────
class _InflacionFacturacionMensualCols:
    articulo = Column(Integer, primary_key=True)
    cadneg = Column(String(16), primary_key=True)
    mes = Column(String(7), primary_key=True, index=True)        # 'YYYY-MM' + índice (mes)
    unidades = Column(Float, nullable=True)                       # SUM(CAST(cant AS FLOAT))
    facturacion = Column(Numeric(19, 4), nullable=True)         # money -> NUMERIC(19,4)


class IndInflacionFacturacionMensual(_InflacionFacturacionMensualCols, Base):
    __tablename__ = "ind_inflacion_facturacion_mensual"


class IndInflacionFacturacionMensualStaging(_InflacionFacturacionMensualCols, Base):
    __tablename__ = "ind_inflacion_facturacion_mensual_staging"


# ──────────────────────────────────────────────────────────────────────────────
# (e) Dimensión de artículos (origen: Fusion.dbo.vsl_art_alfabeta_full / articulos)
# ──────────────────────────────────────────────────────────────────────────────
class _ArticulosCols:
    # Clave natural (código de artículo del origen), no surrogate: autoincrement=False
    # evita que PostgreSQL la compile como SERIAL.
    articulo = Column(Integer, primary_key=True, autoincrement=False)
    marca = Column(Text, nullable=True)
    descripcion = Column(Text, nullable=True)
    laboratorio = Column(Text, nullable=True)
    familia = Column(Text, nullable=True)
    principio_activo = Column(Text, nullable=True)
    unineg = Column(SmallInteger, nullable=True)                  # articulos.unineg (smallint)


class IndArticulos(_ArticulosCols, Base):
    __tablename__ = "ind_articulos"


class IndArticulosStaging(_ArticulosCols, Base):
    __tablename__ = "ind_articulos_staging"
