"""Modelos del módulo Match (Mercado Privado) — homologación asistida.

Compatibles SQLite (local) / PostgreSQL (prod): solo TEXT/INTEGER/REAL/TIMESTAMP.
Run-scoped igual que Dimensionamiento: cada corrida del importer crea una fila en
`match_import_runs` (estado `pending_approval`) y NO pisa la corrida vigente hasta
aprobar. La capa de servicio lee SIEMPRE la última corrida `approved`.
"""
from __future__ import annotations

import datetime as dt

from sqlalchemy import (
    JSON,
    Column,
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

# Estados de una corrida de import del Match (mismo patrón run-scoped de SIEM).
MATCH_RUN_PENDING = "pending_approval"
MATCH_RUN_APPROVED = "approved"
MATCH_RUN_FAILED = "failed"

# Decisiones de homologación.
DECISION_HOMOLOGADO = "homologado"
DECISION_DESCARTADO = "descartado"
# Evento de reversión (solo bitácora; la fila vigente se ELIMINA al revertir).
DECISION_REVERTIDO = "revertido"
# Evento de "eliminar definitivamente" desde la papelera (solo bitácora; la fila
# 'descartado' QUEDA, sale de la papelera al instante). No cambia esquema.
EVENTO_PAPELERA_ELIMINADO = "papelera_eliminado"


class MatchImportRun(Base):
    """Corrida de importación de propuestas. Nace en `pending_approval` y solo se
    convierte en la corrida vigente cuando pasa a `approved` (no pisa la anterior)."""

    __tablename__ = "match_import_runs"

    id = Column(Integer, primary_key=True)
    source_path = Column(String(500), nullable=False)
    status = Column(String(20), nullable=False, default=MATCH_RUN_PENDING, index=True)

    rows_inserted = Column(Integer, nullable=False, default=0)
    articulos_distintos = Column(Integer, nullable=False, default=0)
    counts_by_nivel = Column(JSON, nullable=True)

    started_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow, index=True)
    finished_at = Column(DateTime, nullable=True, index=True)
    approved_at = Column(DateTime, nullable=True)
    approved_by = Column(String(255), nullable=True)
    error_message = Column(Text, nullable=True)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    propuestas = relationship(
        "MatchPropuesta",
        back_populates="import_run",
        cascade="all, delete-orphan",
    )


class MatchPropuesta(Base):
    """Una propuesta de match: descripción de portal + el candidato 1 (artículo Suizo
    sugerido) con sus scores. Run-scoped. La cola accionable son los niveles A–D."""

    __tablename__ = "match_propuestas"

    id = Column(Integer, primary_key=True)
    import_run_id = Column(
        Integer,
        ForeignKey("match_import_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    producto_plataforma = Column(Text, nullable=False)  # descripción del portal
    nivel_confianza = Column(String(2), nullable=True)  # A..E
    score_mejor = Column(Float, nullable=True)          # 0..1
    candidato_codigo = Column(String(120), nullable=True)  # código Suizo (TEXT)
    candidato_descripcion = Column(Text, nullable=True)
    score_tfidf = Column(Float, nullable=True)
    score_fuzzy = Column(Float, nullable=True)
    score_pharma = Column(Float, nullable=True)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    import_run = relationship("MatchImportRun", back_populates="propuestas")

    __table_args__ = (
        UniqueConstraint(
            "import_run_id",
            "producto_plataforma",
            "candidato_codigo",
            name="uq_match_propuestas_run_prod_cand",
        ),
        Index("ix_match_propuestas_run_codigo", "import_run_id", "candidato_codigo"),
        Index("ix_match_propuestas_run_nivel", "import_run_id", "nivel_confianza"),
    )


class MatchHomologacion(Base):
    """Decisión VIGENTE por descripción de portal (upsert last-wins).

    `usuario` se SELLA server-side desde la sesión, nunca del body. Una sola fila por
    `producto_plataforma` (UNIQUE): la última decisión gana."""

    __tablename__ = "match_homologaciones"

    id = Column(Integer, primary_key=True)
    producto_plataforma = Column(Text, nullable=False, unique=True)
    codigo_elegido = Column(String(120), nullable=True)   # normalmente = candidato_codigo
    descripcion_elegida = Column(Text, nullable=True)
    decision = Column(String(20), nullable=False)         # homologado | descartado
    usuario = Column(String(255), nullable=False)         # sellado server-side
    import_run_id = Column(Integer, nullable=True, index=True)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)
    updated_at = Column(
        DateTime,
        nullable=False,
        default=dt.datetime.utcnow,
        onupdate=dt.datetime.utcnow,
    )


class MatchHomologacionEvento(Base):
    """Bitácora append-only (auditoría) de TODAS las decisiones de homologación.

    A diferencia de `match_homologaciones` (1 fila vigente por descripción), acá se
    registra cada decisión sin sobrescribir: permite auditar quién decidió y cuándo."""

    __tablename__ = "match_homologacion_eventos"

    id = Column(Integer, primary_key=True)
    producto_plataforma = Column(Text, nullable=False, index=True)
    decision = Column(String(20), nullable=False)
    codigo_elegido = Column(String(120), nullable=True)
    usuario = Column(String(255), nullable=False)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow, index=True)


class MatchNegocioMap(Base):
    """Mapa compacto código de artículo → (negocio, subnegocio), precalculado UNA vez
    desde `dimensionamiento_records` (~3.238 códigos). Permite filtrar el listado de
    Match por negocio/subnegocio con un lookup chico, SIN joinear la tabla grande en
    caliente. Regenerable con scripts/rebuild_match_negocio_map.py."""

    __tablename__ = "match_negocio_map"

    codigo = Column(String(120), primary_key=True)
    negocio = Column(Text, nullable=True, index=True)
    subnegocio = Column(Text, nullable=True, index=True)


class MatchDemandaDesc(Base):
    """Resumen compacto por DESCRIPCIÓN de portal normalizada → demanda agregada,
    precalculado UNA vez desde `dimensionamiento_records`. `desc_norm` es la misma
    normalización con la que se busca en caliente (lookup por PK, sin full-scan de la
    tabla grande). Regenerable con scripts/rebuild_match_demanda_desc.py."""

    __tablename__ = "match_demanda_desc"

    desc_norm = Column(Text, primary_key=True)
    renglones = Column(Integer, nullable=False, default=0)
    clientes = Column(Integer, nullable=False, default=0)
