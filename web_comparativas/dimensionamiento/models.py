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

    # Ancla de Oportunidades (ago-2026, optimización de /list): "último mes completo"
    # del run. `_window_meta` (oportunidades_router.py) la lee de acá en vez de
    # recalcularla agregando sobre TODO `dimensionamiento_records` del run en cada
    # carga de /list (medido: ~20s, 82% del tiempo total del endpoint).
    #
    # CORRECCIÓN (ago-2026, 2da vuelta): originalmente esta ancla se calculaba
    # SOLO dentro de `rebuild_oportunidades_for_run` (oportunidades.py) — pero esa
    # función NO está enganchada a ningún lado automático del pipeline (su único
    # caller es `scripts/rebuild_oportunidades.py`, a mano, y los tests). Sin
    # arreglar esto, CADA import nuevo dejaba `/list` de vuelta en ~20s hasta que
    # alguien corriera ese script. Ahora se recalcula sola, siempre, desde
    # `refresh_default_dashboard_snapshot` (query_service.py) — que SÍ corre
    # automáticamente después de cada import (`ingest_dimensionamiento_csv` la
    # llama). Esto es liviano (una sola agregación) y no toca `oportunidades_summary`
    # — el rebuild COMPLETO de las oportunidades en sí sigue detrás de
    # OPORTUNIDADES_AUTO_REBUILD_ENABLED (default OFF, ver oportunidades.py),
    # hasta medir cuánto tarda y decidir engancharlo también.
    # NULL para runs de antes de este cambio — `_window_meta` cae al cálculo on-the-fly
    # para esos (sin backfill forzado, ver comentario ahí).
    oportunidades_ref_month = Column(Date, nullable=True)

    # Anclas del DASHBOARD de Dimensionamiento (ago-2026, misma auditoría de
    # rendimiento que ref_month arriba): valores que no cambian entre una carga
    # del dashboard y la siguiente, porque dimensionamiento_records/summary de
    # un run ya escrito no cambia — solo cambian cuando corre el PRÓXIMO import.
    # Ningún índice los arregla: el planner de Postgres descarta cualquier
    # índice sobre import_run_id para agregar sobre este run por selectividad
    # (~26% de la tabla coincide con el run activo — ver auditoría de índices,
    # ago-2026), y un DISTINCT sobre columnas fuera de un covering index sigue
    # necesitando tocar el heap igual.
    #   platform_values    : lista de plataformas distintas del run (antes:
    #                        _default_platform_values en query_service.py,
    #                        medido ~8.2s escaneando 152k filas para 3 valores).
    #   cuenta_entidad_map : {cuenta_interna: [cliente_entidad_id, ...]} de TODA
    #                        la corrida (antes: _cuenta_to_entidad_map, medido
    #                        ~18.7s escaneando 364.887 filas para 371 pares).
    # Calculados UNA VEZ en refresh_default_dashboard_snapshot (query_service.py),
    # en la MISMA transacción que reescribe el snapshot del dashboard — ESE es
    # el paso que corre solo, automático, después de CADA import
    # (ingest_dimensionamiento_csv lo llama). oportunidades_ref_month, en
    # cambio, se puebla desde rebuild_oportunidades_for_run, que HOY es un paso
    # MANUAL (scripts/rebuild_oportunidades.py) — no está enganchado al import
    # automático (confirmado: no hay ningún call site fuera de ese script y de
    # los tests). Por eso estas dos anclas se calculan en un lugar distinto A
    # PROPÓSITO, para que de verdad se generen solas en cada corrida nueva y no
    # dependan de que alguien corra un script aparte.
    # NULL para runs de antes de este cambio -> _default_platform_values /
    # cuenta_to_entidad_ids (query_service.py) caen al cálculo on-the-fly de
    # siempre para esos, igual que con ref_month arriba.
    platform_values = Column(JSON, nullable=True)
    cuenta_entidad_map = Column(JSON, nullable=True)

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

    # Resolución de identidad (identity.py): id de entidad canónica dentro de la corrida.
    # Poblado en el finalize / backfill; NULL en filas aún no resueltas.
    cliente_entidad_id = Column(Integer, nullable=True, index=True)

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
    # Resolución de identidad (identity.py) denormalizada en el summary (fast-path del
    # dashboard, que no tiene cuit/original para resolver por sí solo).
    cliente_entidad_id = Column(Integer, nullable=True, index=True)
    es_cliente_entidad = Column(Boolean, nullable=True, index=True)
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
    # Nº de cuenta de FUSION del cliente (dataset: columna `cuenta_interna`). Es la clave
    # con la que el CRM resuelve la cuenta (`Cuentas_por_numero_fusion?n_cuenta_c=`), NO
    # el cuit. Se arrastra desde el renglón más reciente del par, igual que cuit/provincia.
    # Puede venir 'SIN DATO' en el dataset → se normaliza a NULL en el motor.
    cuenta_interna = Column(String(120), nullable=True)
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

    UNIQUE(oportunidad_id, crm_modo) ⇒ bloqueo de reenvío POR ENTORNO (ago-2026).
    Antes el UNIQUE era solo `oportunidad_id`, lo que hacía que una oportunidad
    probada contra el CRM de TEST quedara bloqueada para PROD. Al incluir `crm_modo`
    ('simulado' | 'test' | 'prod'), cada entorno lleva su propio bloqueo: se puede
    ensayar en TEST toda la semana sin comprometer el envío productivo, y sigue siendo
    imposible mandar dos veces la misma oportunidad al MISMO entorno.

    Cada fila guarda al PRIMER emisor (quién/cuándo) de SU entorno y NO se sobrescribe;
    los reenvíos por override de Admin se anotan en `crm_envio_eventos`.

    ⚠️ `crm_modo` es parte de la clave: si quedara NULL, el UNIQUE no agruparía (en
    SQLite y en Postgres dos NULL se consideran distintos) y el bloqueo se apagaría EN
    SILENCIO. Por eso el código lo setea siempre y la migración rellena las filas viejas.

    Compatible SQLite/Postgres: solo TEXT/INTEGER/TIMESTAMP. El UNIQUE lo crea
    `create_all` en tablas nuevas y `_ensure_crm_envios_table` lo alinea de forma
    idempotente en bases ya existentes (mismo patrón que el resto de _ensure_*).

    ── Modo alternativo "por período" (NO activo por defecto) ──────────────────
    Para permitir además reenviar en un mes nuevo, agregar `periodo_yyyymm` a la
    UniqueConstraint de abajo y al índice que crea `_ensure_crm_envios_table`. El campo
    ya se persiste para tener todo listo ese día.
    """

    __tablename__ = "crm_envios"

    id = Column(Integer, primary_key=True)
    # Identidad estable de la oportunidad (hash corto, 16 hex). El bloqueo NO es por este
    # campo solo: es por (oportunidad_id, crm_modo) — ver __table_args__.
    oportunidad_id = Column(String(40), nullable=False, index=True)
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

    # ── Resultado del envío REAL al CRM (SuiteCRM V8) ──
    # crm_id: id del registro Opportunities creado en el CRM. Es lo que convierte el
    # botón "Enviar a CRM" en "Ver en CRM" (se arma el DetailView con este id).
    crm_id = Column(String(64), nullable=True, index=True)
    # Cuenta del CRM resuelta por número de fusión (paso 3) — se guarda para auditoría.
    crm_account_id = Column(String(64), nullable=True)
    # A quién quedó asignada la oportunidad DEL LADO DEL CRM, y cómo se decidió:
    #   'match'  -> coincidió automáticamente con quien envía (mail sin dominio).
    #   'manual' -> quien envía la eligió a mano en el selector del modal.
    # NO se guarda ningún origen 'fallback': la asignación automática a un tercero se
    # eliminó a propósito. `enviado_por` sigue siendo quien disparó el envío, que puede
    # ser distinto del asignado; son dos preguntas distintas y se auditan por separado.
    crm_assigned_user_id = Column(String(64), nullable=True)
    crm_assigned_usuario = Column(String(255), nullable=True)
    crm_assigned_origen = Column(String(16), nullable=True)
    # Entorno del CRM al que se envió realmente ('simulado' | 'test' | 'prod').
    # FORMA PARTE DE LA CLAVE ÚNICA junto con oportunidad_id: es lo que separa el
    # bloqueo de TEST del de PROD. Nullable solo por compatibilidad con el ALTER TABLE
    # de bases ya existentes; el código lo setea SIEMPRE.
    crm_modo = Column(String(16), nullable=True)

    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("oportunidad_id", "crm_modo", name="uq_crm_envios_oport_modo"),
    )


class CrmEnvioEvento(Base):
    """Bitácora (append-only) de TODOS los eventos de envío al CRM.

    A diferencia de `crm_envios` (1 fila canónica por oportunidad y entorno, con
    UNIQUE), esta tabla NO tiene unique: registra el primer ENVIO y cada
    REENVIO_OVERRIDE de Admin, sin romper el bloqueo. Permite auditar quién
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
    crm_id = Column(String(64), nullable=True)  # id del registro creado en el CRM (si hubo)
    payload_snapshot = Column(Text, nullable=True)
    nota = Column(Text, nullable=True)


class OportunidadAsignacionManual(Base):
    """Asignación manual de una oportunidad a un Analista, hecha por su Supervisor.

    Visibilidad por cartera (Oportunidades, Mercado Privado, ago-2026): la cartera
    de un Analista sale de su identidad en Fusión (ver `VendedorFusion` en
    `web_comparativas/models.py`), pero un Supervisor puede pisar ese default y
    asignarle a mano una oportunidad puntual a uno de SUS analistas — el analista la
    ve aunque la cuenta sea de otro vendedor. Esto es ADITIVO respecto de la cartera
    (no le saca visibilidad a nadie más), ver `oportunidades_visibilidad.py`.

    Identidad estable `oportunidad_id` = `oportunidades.opportunity_stable_id(...)`,
    igual que `CrmEnvio` — NO por `import_run_id`, para que la asignación sobreviva a
    que se recalcule el run. UNIQUE en `oportunidad_id`: una oportunidad tiene a lo
    sumo UN analista asignado a mano a la vez (reasignar pisa la fila anterior, no
    acumula historial — no se pidió auditoría de reasignaciones, solo quién/cuándo
    de la asignación vigente).
    """

    __tablename__ = "oportunidad_asignaciones_manuales"

    id = Column(Integer, primary_key=True)
    oportunidad_id = Column(String(40), nullable=False, unique=True, index=True)

    analista_user_id = Column(Integer, ForeignKey("users.id", ondelete="CASCADE"), nullable=False, index=True)
    asignado_por_user_id = Column(Integer, ForeignKey("users.id", ondelete="SET NULL"), nullable=True)
    asignado_en = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    def __repr__(self) -> str:
        return f"<OportunidadAsignacionManual oport={self.oportunidad_id!r} analista={self.analista_user_id}>"


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


class DimensionamientoClienteEntidad(Base):
    """Registro de entidades-cliente resueltas (1 fila por entidad por corrida).

    Fuente única de verdad para: card de entidades, desglose Sí/No, filtro ¿Cliente?,
    desplegable "Cliente" y matcheo del WHERE (vía entidad_key). Ver identity.py.
    """
    __tablename__ = "dimensionamiento_cliente_entidad"

    id = Column(Integer, primary_key=True)
    import_run_id = Column(
        Integer,
        ForeignKey("dimensionamiento_import_runs.id", ondelete="CASCADE"),
        nullable=False,
        index=True,
    )
    entidad_key = Column(Integer, nullable=False)        # id estable dentro de la corrida
    es_cliente = Column(Boolean, nullable=False, default=False, index=True)
    nombre_visible = Column(Text, nullable=False)
    provincia = Column(String(120), nullable=True)       # provincia dominante (desambiguación)
    cuits = Column(Text, nullable=True)                  # JSON: lista de CUITs del componente
    n_formas = Column(Integer, nullable=False, default=0)
    total_registros = Column(Integer, nullable=False, default=0)
    created_at = Column(DateTime, nullable=False, default=dt.datetime.utcnow)

    __table_args__ = (
        UniqueConstraint("import_run_id", "entidad_key", name="uq_dim_cliente_entidad_run_key"),
        Index("ix_dim_cliente_entidad_run_cli", "import_run_id", "es_cliente"),
    )
