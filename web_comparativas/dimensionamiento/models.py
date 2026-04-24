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
    id_registro_unico = Column(String(255), nullable=False, unique=True)

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
            name="uq_dim_family_monthly_summary",
        ),
        Index("ix_dim_summary_platform_month", "plataforma", "month"),
        Index("ix_dim_summary_family_month", "familia", "month"),
        Index("ix_dim_summary_client_month", "cliente_nombre_homologado", "month"),
        Index("ix_dim_summary_visible_month", "cliente_visible", "month"),
    )


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
    snapshot_key = Column(String(100), nullable=False, unique=True, index=True)
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
