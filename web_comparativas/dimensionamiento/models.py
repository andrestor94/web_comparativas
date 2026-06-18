from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    JSON,
    Boolean,
    Column,
    Date,
    DateTime,
    Float,
    ForeignKey,
    Index,
    Integer,
    String,
    Text,
    UniqueConstraint,
)
from sqlalchemy.orm import relationship

from web_comparativas.models import Base


class DimensionamientoImportRun(Base):
    __tablename__ = "dimensionamiento_import_runs"

    id = Column(Integer, primary_key=True)
    source_path = Column(String(500), nullable=False)
    source_hash = Column(String(64), nullable=True, index=True)
    source_mtime = Column(DateTime, nullable=True)
    mode = Column(String(20), nullable=False, default="replace", index=True)
    status = Column(String(20), nullable=False, default="running", index=True)
    chunk_size = Column(Integer, nullable=False, default=10000)
    started_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow, index=True)
    finished_at = Column(DateTime, nullable=True, index=True)

    rows_processed = Column(Integer, nullable=False, default=0)
    rows_inserted = Column(Integer, nullable=False, default=0)
    rows_updated = Column(Integer, nullable=False, default=0)
    rows_rejected = Column(Integer, nullable=False, default=0)

    expected_columns = Column(JSON, nullable=True)
    observed_columns = Column(JSON, nullable=True)
    summary = Column(JSON, nullable=True)
    error_message = Column(Text, nullable=True)

    records = relationship("DimensionamientoRecord", back_populates="import_run")
    summaries = relationship("DimensionamientoFamilyMonthlySummary", back_populates="import_run")
    snapshots = relationship("DimensionamientoDashboardSnapshot", back_populates="import_run")
    errors = relationship(
        "DimensionamientoImportError",
        back_populates="import_run",
        cascade="all, delete-orphan",
    )


class DimensionamientoRecord(Base):
    __tablename__ = "dimensionamiento_records"

    id = Column(Integer, primary_key=True)
    id_registro_unico = Column(String(255), nullable=False, index=True)

    fecha = Column(Date, nullable=False, index=True)
    plataforma = Column(String(40), nullable=False, index=True)

    cliente_nombre_homologado = Column(Text, nullable=True, index=True)
    cliente_nombre_original = Column(Text, nullable=True)
    cliente_visible = Column(Text, nullable=True, index=True)
    cuit = Column(String(32), nullable=True)
    provincia = Column(String(120), nullable=True, index=True)
    cuenta_interna = Column(String(120), nullable=True)
    codigo_articulo = Column(String(120), nullable=True, index=True)
    descripcion = Column(Text, nullable=True)
    clasificacion_suizo = Column(Text, nullable=True)
    descripcion_articulo = Column(Text, nullable=True)
    familia = Column(Text, nullable=True, index=True)
    unidad_negocio = Column(Text, nullable=True, index=True)
    subunidad_negocio = Column(Text, nullable=True, index=True)
    cantidad_demandada = Column(Float, nullable=False, default=0)
    valorizacion_estimada = Column(Float, nullable=True, default=0)
    resultado_participacion = Column(String(120), nullable=True, index=True)
    producto_nombre_original = Column(Text, nullable=True)
    fecha_procesamiento = Column(DateTime, nullable=True, index=True)

    is_identified = Column(Boolean, nullable=False, default=False, index=True)
    is_client = Column(Boolean, nullable=False, default=False, index=True)

    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )

    import_run = relationship("DimensionamientoImportRun", back_populates="records")

    __table_args__ = (
        UniqueConstraint("id_registro_unico", "import_run_id", name="uq_dim_records_id_run"),
        Index("ix_dim_records_platform_date", "plataforma", "fecha"),
        Index("ix_dim_records_client_date", "cliente_nombre_homologado", "fecha"),
        Index("ix_dim_records_visible_date", "cliente_visible", "fecha"),
        Index("ix_dim_records_family_date", "familia", "fecha"),
        Index("ix_dim_records_province_date", "provincia", "fecha"),
        Index("ix_dim_records_result_date", "resultado_participacion", "fecha"),
        Index(
            "ix_dim_records_unit_subunit_date",
            "unidad_negocio",
            "subunidad_negocio",
            "fecha",
        ),
    )


class DimensionamientoFamilyMonthlySummary(Base):
    __tablename__ = "dimensionamiento_family_monthly_summary"

    id = Column(Integer, primary_key=True)
    month = Column(Date, nullable=False, index=True)
    plataforma = Column(String(40), nullable=False, index=True)
    cliente_nombre_homologado = Column(Text, nullable=True, index=True)
    cliente_visible = Column(Text, nullable=True, index=True)
    provincia = Column(String(120), nullable=True, index=True)
    familia = Column(Text, nullable=True, index=True)
    unidad_negocio = Column(Text, nullable=True, index=True)
    subunidad_negocio = Column(Text, nullable=True, index=True)
    resultado_participacion = Column(String(120), nullable=True, index=True)
    is_identified = Column(Boolean, nullable=False, default=False, index=True)
    is_client = Column(Boolean, nullable=False, default=False, index=True)
    total_cantidad = Column(Float, nullable=False, default=0)
    total_valorizacion = Column(Float, nullable=False, default=0)
    total_registros = Column(Integer, nullable=False, default=0)
    clientes_unicos = Column(Integer, nullable=False, default=0)
    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    import_run = relationship("DimensionamientoImportRun", back_populates="summaries")

    __table_args__ = (
        UniqueConstraint(
            "month",
            "plataforma",
            "cliente_nombre_homologado",
            "cliente_visible",
            "provincia",
            "familia",
            "unidad_negocio",
            "subunidad_negocio",
            "resultado_participacion",
            "is_identified",
            "is_client",
            "import_run_id",
            name="uq_dim_family_monthly_summary",
        ),
        Index("ix_dim_summary_platform_month", "plataforma", "month"),
        Index("ix_dim_summary_family_month", "familia", "month"),
        Index("ix_dim_summary_client_month", "cliente_nombre_homologado", "month"),
        Index("ix_dim_summary_visible_month", "cliente_visible", "month"),
        # Composite indexes for filtered queries: is_client + dimension + month
        # Helps when the dashboard filters by is_client=True/False alongside other dims.
        Index("ix_dim_summary_isclient_family_month", "is_client", "familia", "month"),
        Index("ix_dim_summary_isclient_province_month", "is_client", "provincia", "month"),
        Index("ix_dim_summary_isclient_result_month", "is_client", "resultado_participacion", "month"),
        Index("ix_dim_summary_isclient_unit_month", "is_client", "unidad_negocio", "month"),
    )


class OportunidadSummary(Base):
    """Tabla precalculada de oportunidades de venta (ventas perdidas recuperables).

    Grano: un par (cliente_visible + codigo_articulo) que califica como oportunidad
    según el motor en `oportunidades.py`. Run-scoped (una fila por par y corrida),
    reconstruida desde dimensionamiento_records del run activo.
    """

    __tablename__ = "oportunidades_summary"

    id = Column(Integer, primary_key=True)
    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )

    # Identidad de la oportunidad
    codigo_articulo = Column(String(120), nullable=False, index=True)
    cliente_visible = Column(Text, nullable=True, index=True)
    cuit = Column(String(32), nullable=True)
    provincia = Column(String(120), nullable=True)
    producto_nombre = Column(Text, nullable=True)
    familia = Column(Text, nullable=True)
    unidad_negocio = Column(Text, nullable=True)
    plataforma = Column(String(40), nullable=True)

    # Clasificación
    tipo_oportunidad = Column(String(20), nullable=True, index=True)
    estado_actividad = Column(String(20), nullable=True, index=True)

    # Demanda (ventana últimos 12 meses)
    # meses_demanda_cliente_12m: meses con demanda del cliente (TODOS los estados) -> clasifica el tipo.
    # meses_no_participo_12m:     meses con demanda NO_PARTICIPO (define el monto recuperable).
    meses_demanda_cliente_12m = Column(Integer, nullable=False, default=0)
    meses_no_participo_12m = Column(Integer, nullable=False, default=0)
    ventana_meses = Column(Integer, nullable=False, default=12)
    consumo_tipico_mensual = Column(Float, nullable=False, default=0)
    consumo_min_mensual = Column(Float, nullable=False, default=0)
    consumo_max_mensual = Column(Float, nullable=False, default=0)
    ultima_demanda = Column(Date, nullable=True)
    meses_desde_ultima_demanda = Column(Integer, nullable=True)

    # Precio y monto
    precio_unitario_estimado = Column(Float, nullable=False, default=0)
    monto_oportunidad = Column(Float, nullable=False, default=0)

    # Efectividad (histórico completo por codigo_articulo)
    efectividad = Column(Float, nullable=False, default=0)
    ganados = Column(Integer, nullable=False, default=0)
    comprado_otra = Column(Integer, nullable=False, default=0)
    en_espera = Column(Integer, nullable=False, default=0)
    clientes_distintos = Column(Integer, nullable=False, default=0)

    # Multiplicadores y score
    tipo_multiplicador = Column(Float, nullable=False, default=0)
    multiplicador_actividad = Column(Float, nullable=False, default=0)
    score = Column(Float, nullable=False, default=0, index=True)

    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (
        Index("ix_oportunidades_run_score", "import_run_id", "score"),
        Index("ix_oportunidades_run_codigo", "import_run_id", "codigo_articulo"),
    )


class CrmEnvio(Base):
    """Registro CANÓNICO de oportunidades enviadas al CRM (idempotencia + trazabilidad).

    Una fila por oportunidad efectivamente enviada. La identidad estable
    `oportunidad_id` = sha1(cliente_visible + "|" + codigo_articulo) — ver
    `oportunidades.opportunity_stable_id`. Coincide con el GRANO del motor y NO
    depende de montos/efectividad/atributos del último renglón (cuit, unidad), de
    modo que sobrevive a los recálculos mensuales.

    UNIQUE(oportunidad_id) ⇒ bloqueo PERMANENTE de reenvío (default del proyecto).
    Esta fila guarda al PRIMER emisor (quién/cuándo) y NO se sobrescribe; los
    reenvíos por override de Admin/Gerente se anotan en `crm_envio_eventos`.

    Compatible SQLite/Postgres: solo TEXT/INTEGER/TIMESTAMP. El UNIQUE lo crea
    `create_all` en tablas nuevas y `_ensure_crm_envios_table` lo alinea de forma
    idempotente en bases ya existentes (mismo patrón que el resto de _ensure_*).

    ── Modo alternativo "por período" (NO activo por defecto) ──────────────────
    Para permitir reenviar en un mes nuevo, el bloqueo debería ser por
    (oportunidad_id, periodo_yyyymm) en vez de solo oportunidad_id:
      1. Quitar unique=True de `oportunidad_id`.
      2. Agregar UniqueConstraint("oportunidad_id", "periodo_yyyymm",
         name="uq_crm_envio_oport_periodo") a __table_args__.
      3. En _ensure_crm_envios_table, crear el índice único compuesto.
    El campo `periodo_yyyymm` ya se persiste para tener todo listo ese día.
    """

    __tablename__ = "crm_envios"

    id = Column(Integer, primary_key=True)
    # Identidad estable de la oportunidad (hash corto, 16 hex). UNIQUE = bloqueo permanente.
    oportunidad_id = Column(String(40), nullable=False, unique=True, index=True)
    # Período YYYYMM del envío. Hoy informativo; clave del modo "por período".
    periodo_yyyymm = Column(String(6), nullable=True, index=True)

    # Campos descriptivos (NO forman parte de la identidad: pueden driftear).
    cliente_visible = Column(Text, nullable=True)
    cuit = Column(String(32), nullable=True)
    codigo_articulo = Column(String(120), nullable=True)
    unidad_negocio = Column(Text, nullable=True)

    # Sello del usuario que envía (server-side; el email es el campo de control).
    enviado_por = Column(String(255), nullable=False)
    enviado_por_id = Column(Integer, nullable=True)
    enviado_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    crm_status = Column(String(40), nullable=False, default="PENDIENTE_ENVIO_REAL")
    payload_snapshot = Column(Text, nullable=True)  # JSON serializado del payload sellado

    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)


class CrmEnvioEvento(Base):
    """Bitácora (append-only) de TODOS los eventos de envío al CRM.

    A diferencia de `crm_envios` (1 fila canónica por oportunidad, con UNIQUE),
    esta tabla NO tiene unique: registra el primer ENVIO y cada REENVIO_OVERRIDE
    de Admin/Gerente, sin romper el bloqueo permanente. Permite auditar quién
    reenvió y cuándo aunque el bloqueo siga vigente.
    """

    __tablename__ = "crm_envio_eventos"

    id = Column(Integer, primary_key=True)
    oportunidad_id = Column(String(40), nullable=False, index=True)
    evento = Column(String(30), nullable=False, default="ENVIO")  # ENVIO | REENVIO_OVERRIDE
    periodo_yyyymm = Column(String(6), nullable=True)
    enviado_por = Column(String(255), nullable=False)
    enviado_por_id = Column(Integer, nullable=True)
    enviado_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)
    crm_status = Column(String(40), nullable=True)
    payload_snapshot = Column(Text, nullable=True)
    nota = Column(Text, nullable=True)


class DimensionamientoImportError(Base):
    __tablename__ = "dimensionamiento_import_errors"

    id = Column(Integer, primary_key=True)
    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    row_number = Column(Integer, nullable=False)
    error_message = Column(Text, nullable=False)
    raw_payload = Column(JSON, nullable=True)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow, index=True)

    import_run = relationship("DimensionamientoImportRun", back_populates="errors")


class DimensionamientoDashboardSnapshot(Base):
    __tablename__ = "dimensionamiento_dashboard_snapshots"

    id = Column(Integer, primary_key=True)
    snapshot_key = Column(String(100), nullable=False, index=True)
    version = Column(String(20), nullable=False, default="v1", index=True)
    payload = Column(JSON, nullable=False)
    generated_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow, index=True)
    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="SET NULL"),
        nullable=True,
        index=True,
    )

    import_run = relationship("DimensionamientoImportRun", back_populates="snapshots")

    __table_args__ = (
        UniqueConstraint("snapshot_key", "import_run_id", name="uq_dim_dashboard_snapshots_key_run"),
    )
