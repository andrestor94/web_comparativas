"""Modelos ORM de las tablas *summary* publicadas del módulo Indicadores Comerciales.

Patrón de publicación: VERSIONADO POR CORRIDA (import_run_id), igual que el módulo
Dimensionamiento. Ya NO hay tablas gemelas *_staging: cada carga inserta sus filas
etiquetadas con su corrida (ind_import_run) SIN tocar las filas de la corrida aprobada;
las lecturas filtran por la última corrida con status='approved', la aprobación es un
switch atómico de estado y el descarte borra solo las filas de esa corrida.

Las claves naturales originales pasan a UNIQUE compuesto con import_run_id y la PK es
un id surrogate: así dos corridas conviven sin colisión de PK y los reintentos de carga
pueden ser idempotentes (ON CONFLICT DO NOTHING sobre el UNIQUE), como en
dimensiones_router. ind_rentabilidad_lineas no tiene clave natural de contenido (se
preservan líneas repetidas a propósito para el SUM(DISTINCT) de Rentabilidad Negativa),
por lo que su UNIQUE es POSICIONAL: (import_run_id, line_seq), NULL-safe para que las
filas migradas del backup (line_seq NULL) convivan.

ind_etl_control SE MANTIENE tal cual: guarda el watermark idhisto del incremental de
histopre y el estado de ventana por fuente; sigue siendo necesario.

Mismo mecanismo de materialización que Dimensionamiento: clases declarativas sobre el
`Base` compartido, `Base.metadata.create_all(bind=engine)` en el arranque (main.py).

Tipos por dialecto: tipos genéricos de SQLAlchemy (Integer/BigInteger/SmallInteger,
Date, DateTime, String, Text, Float). Las columnas `money` del origen SQL Server se
modelan como `Numeric(19, 4)` (NUMERIC(19,4) en PostgreSQL; afinidad numérica en SQLite).
"""

from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    BigInteger,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Integer,
    Numeric,
    SmallInteger,
    String,
    Text,
    UniqueConstraint,
)

from web_comparativas.models import Base


# ──────────────────────────────────────────────────────────────────────────────
# (a) Control de watermark/estado del ETL incremental — tabla única.
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
# (b) Corridas de importación — versionado + aprobación humana.
# ──────────────────────────────────────────────────────────────────────────────
class IndImportRun(Base):
    """Corrida de importación de las tablas summary ind_*.

    Estados válidos (status):
      running          -> carga en curso (chunks entrando).
      pending_approval -> carga finalizada, esperando aprobación humana.
      approved         -> corrida ACTIVA: la única que sirven las lecturas.
      discarded        -> descartada por el aprobador (sus filas se borran).
      failed           -> carga abortada por error.
    """
    __tablename__ = "ind_import_run"

    id = Column(Integer, primary_key=True, autoincrement=True)
    status = Column(String(20), nullable=False, default="running")
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)
    finalized_at = Column(DateTime, nullable=True)
    approved_at = Column(DateTime, nullable=True)
    approved_by = Column(Text, nullable=True)
    rows_por_tabla = Column(Text, nullable=True)  # JSON serializado: {"tabla": filas, ...}
    nota = Column(Text, nullable=True)


# ──────────────────────────────────────────────────────────────────────────────
# (c) Rentabilidad nivel línea (origen: ETL_Data.dbo.rentabilidad_cliente + clientes)
# ──────────────────────────────────────────────────────────────────────────────
class IndRentabilidadLineas(Base):
    __tablename__ = "ind_rentabilidad_lineas"
    __table_args__ = (
        UniqueConstraint("import_run_id", "line_seq", name="uq_ind_rentab_run_seq"),
    )

    # Surrogate PK: la grilla de líneas no tiene clave natural única. El watermark
    # vive en ind_etl_control, no acá. UNIQUE posicional (import_run_id, line_seq),
    # NULL-safe: las filas migradas del backup (corrida 2) quedan con line_seq NULL
    # y conviven por NULLS DISTINCT; las corridas por chunk traen line_seq NOT NULL
    # y así el on_conflict_do_nothing dedupe reintentos.
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
    import_run_id = Column(Integer, ForeignKey("ind_import_run.id"), nullable=True, index=True)
    line_seq = Column(Integer, nullable=True)                     # ordinal de la fila dentro de la corrida (lo provee el push); backup queda NULL


# ──────────────────────────────────────────────────────────────────────────────
# (d) Inflación PVP agregada articulo×mes (origen: Fusion.dbo.histopre.prepubact)
# ──────────────────────────────────────────────────────────────────────────────
class IndInflacionPvpMensual(Base):
    __tablename__ = "ind_inflacion_pvp_mensual"
    __table_args__ = (
        UniqueConstraint("articulo", "mes", "import_run_id", name="uq_ind_pvp_articulo_mes_run"),
    )

    id = Column(Integer, primary_key=True)
    articulo = Column(Integer, nullable=False)                    # histopre.articulo (int) — ex PK natural
    mes = Column(String(7), nullable=False)                       # 'YYYY-MM' — ex PK natural
    fecha_snapshot = Column(Date, nullable=True)
    pvp = Column(Numeric(19, 4), nullable=True)                  # money -> NUMERIC(19,4)
    import_run_id = Column(Integer, ForeignKey("ind_import_run.id"), nullable=True, index=True)


# ──────────────────────────────────────────────────────────────────────────────
# (e) Inflación facturación agregada articulo×cadneg×mes
#     (origen: ETL_Data.dbo.rentabililad_x_cliente)
# ──────────────────────────────────────────────────────────────────────────────
class IndInflacionFacturacionMensual(Base):
    __tablename__ = "ind_inflacion_facturacion_mensual"
    __table_args__ = (
        UniqueConstraint("articulo", "cadneg", "mes", "import_run_id",
                         name="uq_ind_fact_articulo_cadneg_mes_run"),
    )

    id = Column(Integer, primary_key=True)
    articulo = Column(Integer, nullable=False)                    # ex PK natural
    cadneg = Column(String(16), nullable=False)                   # ex PK natural
    mes = Column(String(7), nullable=False, index=True)           # 'YYYY-MM' — ex PK natural + índice (mes)
    unidades = Column(Float, nullable=True)                       # SUM(CAST(cant AS FLOAT))
    facturacion = Column(Numeric(19, 4), nullable=True)         # money -> NUMERIC(19,4)
    import_run_id = Column(Integer, ForeignKey("ind_import_run.id"), nullable=True, index=True)


# ──────────────────────────────────────────────────────────────────────────────
# (f) Dimensión de artículos (origen: Fusion.dbo.vsl_art_alfabeta_full / articulos)
# ──────────────────────────────────────────────────────────────────────────────
class IndArticulos(Base):
    __tablename__ = "ind_articulos"
    __table_args__ = (
        UniqueConstraint("articulo", "import_run_id", name="uq_ind_articulos_articulo_run"),
    )

    id = Column(Integer, primary_key=True)
    articulo = Column(Integer, nullable=False)                    # código de artículo del origen — ex PK natural
    marca = Column(Text, nullable=True)
    descripcion = Column(Text, nullable=True)
    laboratorio = Column(Text, nullable=True)
    familia = Column(Text, nullable=True)
    principio_activo = Column(Text, nullable=True)
    unineg = Column(SmallInteger, nullable=True)                  # articulos.unineg (smallint)
    import_run_id = Column(Integer, ForeignKey("ind_import_run.id"), nullable=True, index=True)
