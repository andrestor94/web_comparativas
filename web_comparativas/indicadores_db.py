"""
Conexión a SQL Server para el módulo Indicadores Comerciales.
Apunta al .env y al bridge PowerShell de 03 - Rentabilidad Negativa.
No modifica app.db ni depende de pyodbc (usa el puente sqlclient).
"""

from contextlib import contextmanager
from datetime import date, datetime
import json
import os
from pathlib import Path
import subprocess
import threading
import time
import uuid

_MODULE_DIR = Path(__file__).resolve().parent
# Fuente canónica: Indicadores Comerciales/03 - Rentabilidad Negativa (la carpeta raíz fue movida)
# Fallback a la ubicación original si la nueva no existe
_RENT_DIR_NEW = _MODULE_DIR.parent / "Indicadores Comerciales" / "03 - Rentabilidad Negativa" / "backend"
_RENT_DIR_OLD = _MODULE_DIR.parent / "03 - Rentabilidad Negativa" / "backend"
_RENT_DIR = _RENT_DIR_NEW if _RENT_DIR_NEW.exists() else _RENT_DIR_OLD
_ENV_FILE = _RENT_DIR / ".env"
_BRIDGE_SCRIPT = _RENT_DIR / "sqlclient_bridge.ps1"
_TMP_DIR = _RENT_DIR / ".sqlclient_tmp"


def _load_env(path: Path) -> dict:
    result: dict = {}
    try:
        for line in path.read_text(encoding="utf-8").splitlines():
            line = line.strip()
            if line and not line.startswith("#") and "=" in line:
                key, _, value = line.partition("=")
                result[key.strip()] = value.strip()
    except Exception:
        pass
    return result


_env = _load_env(_ENV_FILE)


def _cfg(key: str, default: str = "") -> str:
    import os
    return os.environ.get(key, _env.get(key, default))


def _cfg_bool(key: str, default: bool = False) -> bool:
    val = _cfg(key, "true" if default else "false").lower()
    return val in ("1", "true", "yes")


class _Settings:
    db_server: str = _cfg("DB_SERVER")
    db_database: str = _cfg("DB_DATABASE")
    db_user: str = _cfg("DB_USER")
    db_password: str = _cfg("DB_PASSWORD")

    etl_server: str = _cfg("ETL_SERVER", "10.10.10.203")
    etl_database: str = _cfg("ETL_DATABASE", "ETL_Data")
    etl_user: str = _cfg("ETL_USER")
    etl_password: str = _cfg("ETL_PASSWORD")
    etl_trusted_connection: bool = _cfg_bool("ETL_TRUSTED_CONNECTION", False)

    fusion_server: str = _cfg("FUSION_SERVER")
    fusion_database: str = _cfg("FUSION_DATABASE")
    fusion_user: str = _cfg("FUSION_USER")
    fusion_password: str = _cfg("FUSION_PASSWORD")
    fusion_trusted_connection: bool = _cfg_bool("FUSION_TRUSTED_CONNECTION", False)

    sql_provider: str = _cfg("SQL_PROVIDER", "sqlclient")


settings = _Settings()


def _json_value(value):
    if isinstance(value, (date, datetime)):
        return value.isoformat()
    return value


def _parameterize_query(query: str, count: int) -> str:
    for index in range(count):
        query = query.replace("?", f"@p{index}", 1)
    if "?" in query:
        raise RuntimeError("La consulta SQL tiene más parámetros '?' que valores recibidos.")
    return query


# ── Clasificación de errores del bridge ────────────────────────────────────────
# El .ps1 separa la apertura de conexión (Open) de la query (Fill) y lo señala
# con exit codes dedicados. Solo la APERTURA es reintentable: si el Open falló,
# la query nunca llegó a ejecutarse, así que relanzar el proceso no duplica nada.
_BRIDGE_EXIT_CONNECTION = 10   # CONNECTION_ERROR: handshake/login/red
_BRIDGE_EXIT_QUERY = 20        # QUERY_ERROR: conexión abierta, falló la query
_BRIDGE_MAX_ATTEMPTS = 3
_BRIDGE_BACKOFF_S = (2, 4)     # espera antes del 2do y 3er intento
_BRIDGE_TIMEOUT_S = 65         # 65s: el JS AbortController corta al cliente a los 70s
_BRIDGE_TIMEOUT_WEB_S = 8      # path web (INDICADORES_USE_SUMMARY on): 1 intento corto


def _summary_mode_on() -> bool:
    """True si INDICADORES_USE_SUMMARY está encendido (mismo criterio que el gate de
    summary). En ese modo el proceso es el WEB: TODO se lee del summary y el bridge en
    vivo NO debería usarse. Si por un fallback se llega igual (flag on pero tabla aún
    no publicada, o red del ETL caída), debe fallar RÁPIDO — 1 intento, timeout corto,
    sin backoff — para no bloquear el request del usuario colgado esperando al ETL_Data.

    El ETL de la VM corre con USE_SUMMARY=0 (extracción real por VPN) y conserva el
    retry/backoff/timeout completos: esta rama NO lo toca."""
    return os.environ.get("INDICADORES_USE_SUMMARY", "0").strip().lower() not in {"0", "false", "no", "off"}


class BridgeConnectionError(RuntimeError):
    """Fallo de APERTURA de conexión (handshake/login/red) tras agotar reintentos."""

    def __init__(self, message: str, attempts: int):
        super().__init__(message)
        self.attempts = attempts


class BridgeQueryError(RuntimeError):
    """Fallo de la QUERY con la conexión ya abierta. No reintentable."""


class SqlClientCursor:
    def __init__(self, connection_config: dict):
        self.connection_config = connection_config
        self.description: list = []
        self._rows: list = []

    def _run_bridge_with_retry(self, connection_path, query_path, params_path):
        """Lanza el proceso bridge; reintenta SOLO fallos de apertura de conexión.

        Reintentable: exit 10 (el Open() falló antes del Fill(): la query nunca
        corrió) y TimeoutExpired (cuelgue de red en el handshake o el transporte).
        Exit 20 (error de query/datos) u otros códigos propagan de inmediato.
        """
        # Path web (INDICADORES_USE_SUMMARY on): 1 intento + timeout corto, sin backoff,
        # para no colgar el request si por fallback se llega al bridge. ETL de la VM
        # (USE_SUMMARY=0): retry/backoff/timeout completos, sin cambios.
        web_fast = _summary_mode_on()
        max_attempts = 1 if web_fast else _BRIDGE_MAX_ATTEMPTS
        timeout_s = _BRIDGE_TIMEOUT_WEB_S if web_fast else _BRIDGE_TIMEOUT_S

        last_message = ""
        for attempt in range(1, max_attempts + 1):
            try:
                completed = subprocess.run(
                    [
                        "powershell",
                        "-NoProfile",
                        "-ExecutionPolicy",
                        "Bypass",
                        "-File",
                        str(_BRIDGE_SCRIPT),
                        "-ConnectionJson",
                        str(connection_path),
                        "-QueryFile",
                        str(query_path),
                        "-ParamsFile",
                        str(params_path),
                    ],
                    capture_output=True,
                    text=True,
                    encoding="utf-8",
                    errors="replace",
                    timeout=timeout_s,
                    check=False,
                )
            except subprocess.TimeoutExpired:
                last_message = f"timeout de {timeout_s}s esperando al proceso bridge"
            except FileNotFoundError as exc:
                # En Render (Linux) no hay 'powershell' ni el .ps1: falla de inmediato,
                # sin reintentar. Es un fallo de apertura → BridgeConnectionError.
                raise BridgeConnectionError(
                    f"bridge no disponible en este entorno: {exc}", attempts=attempt
                )
            else:
                if completed.returncode == 0:
                    return completed
                message = (completed.stderr or completed.stdout or "").strip()
                if completed.returncode == _BRIDGE_EXIT_QUERY:
                    raise BridgeQueryError(message)
                if completed.returncode != _BRIDGE_EXIT_CONNECTION:
                    raise RuntimeError(message)
                last_message = message
            if attempt < max_attempts:
                delay = _BRIDGE_BACKOFF_S[attempt - 1]
                print(
                    f"[BRIDGE retry] intento {attempt}/{max_attempts}: "
                    f"conexión falló, reintentando en {delay}s",
                    flush=True,
                )
                time.sleep(delay)
        raise BridgeConnectionError(last_message, attempts=max_attempts)

    def execute(self, query: str, params=None):
        params = list(params or [])
        query = _parameterize_query(query, len(params))
        payload_id = uuid.uuid4().hex
        _TMP_DIR.mkdir(exist_ok=True)
        connection_path = _TMP_DIR / f"{payload_id}.connection.json"
        query_path = _TMP_DIR / f"{payload_id}.sql"
        params_path = _TMP_DIR / f"{payload_id}.params.json"

        try:
            connection_path.write_text(json.dumps(self.connection_config), encoding="utf-8")
            query_path.write_text(query, encoding="utf-8")
            params_path.write_text(json.dumps([_json_value(v) for v in params]), encoding="utf-8")
            completed = self._run_bridge_with_retry(connection_path, query_path, params_path)

            result = json.loads(completed.stdout or "{}")
            columns = result.get("columns") or []
            rows = result.get("rows") or []
            self.description = [(col,) for col in columns]
            self._rows = [tuple(row.get(col) for col in columns) for row in rows]
            return self
        finally:
            for path in (connection_path, query_path, params_path):
                try:
                    path.unlink(missing_ok=True)
                except OSError:
                    pass

    def fetchall(self):
        rows = self._rows
        self._rows = []
        return rows

    def fetchone(self):
        if not self._rows:
            return None
        return self._rows.pop(0)


class SqlClientConnection:
    def __init__(self, server: str, database: str, user: str, password: str, trusted_connection: bool):
        self.connection_config = {
            "server": server,
            "database": database,
            "user": user,
            "password": password,
            "trusted_connection": trusted_connection,
        }

    def cursor(self):
        return SqlClientCursor(self.connection_config)

    def close(self):
        pass


def _use_sqlclient() -> bool:
    return settings.sql_provider.lower() == "sqlclient"


def _pyodbc_connect(conn_str: str):
    import pyodbc  # optional; only used when sql_provider != sqlclient
    return pyodbc.connect(conn_str)


def _build_conn_str(server: str, database: str, user: str, password: str, trusted: bool) -> str:
    parts = [
        "DRIVER={ODBC Driver 18 for SQL Server};",
        f"SERVER={server};",
        f"DATABASE={database};",
    ]
    if trusted:
        parts.append("Trusted_Connection=yes;")
    else:
        parts += [f"UID={user};", f"PWD={password};"]
    parts.append("TrustServerCertificate=yes;")
    return "".join(parts)


@contextmanager
def get_etl_db():
    # Construcción/apertura (en sqlclient no hay I/O acá; en pyodbc sí conecta).
    try:
        if _use_sqlclient():
            conn = SqlClientConnection(
                server=settings.etl_server,
                database=settings.etl_database,
                user=settings.etl_user or settings.db_user,
                password=settings.etl_password or settings.db_password,
                trusted_connection=settings.etl_trusted_connection,
            )
        else:
            conn_str = _build_conn_str(
                settings.etl_server, settings.etl_database,
                settings.etl_user or settings.db_user,
                settings.etl_password or settings.db_password,
                settings.etl_trusted_connection,
            )
            conn = _pyodbc_connect(conn_str)
    except Exception as exc:
        raise RuntimeError(f"Error de conexión a ETL_Data: {exc}") from exc
    # Errores del caller: etiqueta según su clase — solo el fallo de apertura
    # (BridgeConnectionError, tras agotar reintentos) es "de conexión"; el resto
    # (query, datos, parseo) NO debe disfrazarse de error de conexión.
    try:
        yield conn
    except BridgeConnectionError as exc:
        raise RuntimeError(
            f"Error de conexión a ETL_Data tras {exc.attempts} intentos: {exc}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Error al consultar ETL_Data: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


@contextmanager
def get_fusion_db():
    # Construcción/apertura (en sqlclient no hay I/O acá; en pyodbc sí conecta).
    try:
        if _use_sqlclient():
            conn = SqlClientConnection(
                server=settings.fusion_server or settings.db_server,
                database=settings.fusion_database or settings.db_database,
                user=settings.fusion_user or settings.db_user,
                password=settings.fusion_password or settings.db_password,
                trusted_connection=settings.fusion_trusted_connection,
            )
        else:
            conn_str = _build_conn_str(
                settings.fusion_server or settings.db_server,
                settings.fusion_database or settings.db_database,
                settings.fusion_user or settings.db_user,
                settings.fusion_password or settings.db_password,
                settings.fusion_trusted_connection,
            )
            conn = _pyodbc_connect(conn_str)
    except Exception as exc:
        raise RuntimeError(f"Error de conexión a Fusion: {exc}") from exc
    # Errores del caller: etiqueta según su clase — solo el fallo de apertura
    # (BridgeConnectionError, tras agotar reintentos) es "de conexión"; el resto
    # (query, datos, parseo) NO debe disfrazarse de error de conexión.
    try:
        yield conn
    except BridgeConnectionError as exc:
        raise RuntimeError(
            f"Error de conexión a Fusion tras {exc.attempts} intentos: {exc}"
        ) from exc
    except Exception as exc:
        raise RuntimeError(f"Error al consultar Fusion: {exc}") from exc
    finally:
        try:
            conn.close()
        except Exception:
            pass


def is_available() -> bool:
    """Devuelve True si el bridge script y el .env existen en el sistema."""
    return _BRIDGE_SCRIPT.exists() and _ENV_FILE.exists()


# Aliases para el servicio de Inflación, que usa nombres distintos
get_db = get_fusion_db
get_sales_db = get_etl_db


# ── Gate de tablas summary publicadas (kill-switch INDICADORES_USE_SUMMARY) ────
# Replica EXACTAMENTE el cableado de forecast_service._forecast_summary_available:
# motor PostgreSQL + flag de entorno no apagado + la tabla existe, con cache TTL
# corto bajo lock para no pegarle a information_schema en cada request.
#
# Diferencia deliberada respecto de FORECAST_USE_SUMMARY: el default acá es OFF
# (FASE 1, esquema recién creado, ETL todavía sin poblar). FORECAST_USE_SUMMARY
# arranca en "1" (on); INDICADORES_USE_SUMMARY arranca en "0" (False) por consigna.
#
# Inerte en FASE 1: ningún endpoint del módulo lo consume todavía.
_IND_SUMMARY_AVAIL_CACHE: dict = {}        # table -> (checked_monotonic, bool)
_IND_SUMMARY_AVAIL_TTL = 60.0              # TTL corto: levanta tablas recién publicadas
_IND_SUMMARY_AVAIL_LOCK = threading.Lock()


def _indicadores_summary_available(table: str = "ind_rentabilidad_lineas") -> bool:
    """True si: INDICADORES_USE_SUMMARY encendido + la tabla publicada existe.

    Kill-switch: INDICADORES_USE_SUMMARY in {0,false,no,off} (default) -> fuerza el
    camino crudo (lectura directa de SQL Server vía bridge). Con el flag encendido,
    routea al summary tanto en PostgreSQL (prod) como en SQLite (local) — la existencia
    de la tabla se chequea de forma agnóstica al dialecto (inspect.has_table), con cache
    TTL corto. La rama OFF (flag apagado) queda idéntica en cualquier motor.
    """
    try:
        from web_comparativas.models import engine
    except Exception:
        return False
    if engine is None:
        return False
    if os.environ.get("INDICADORES_USE_SUMMARY", "0").strip().lower() in {"0", "false", "no", "off"}:
        return False
    now = time.monotonic()
    with _IND_SUMMARY_AVAIL_LOCK:
        entry = _IND_SUMMARY_AVAIL_CACHE.get(table)
        if entry and (now - entry[0]) < _IND_SUMMARY_AVAIL_TTL:
            return entry[1]
    try:
        from sqlalchemy import inspect as _sa_inspect
        exists = _sa_inspect(engine).has_table(table)
    except Exception:
        exists = False
    with _IND_SUMMARY_AVAIL_LOCK:
        _IND_SUMMARY_AVAIL_CACHE[table] = (now, exists)
    return exists


def _corrida_activa() -> "int | None":
    """Id de la corrida 'approved' ACTIVA en ind_import_run: la más reciente por
    approved_at (desempate por id). Es la única corrida que deben servir las lecturas
    summary (rama ON) — las filas de corridas running/pending/discarded/failed conviven
    en las mismas tablas y se excluyen filtrando por este id.

    Devuelve None si no hay ninguna corrida aprobada (o si la tabla todavía no existe /
    la consulta falla): el caller debe responder VACÍO de forma limpia, nunca romper.

    Sin cache a propósito: es un SELECT con LIMIT 1 sobre una tabla de pocas filas, y
    así una corrida recién aprobada (o descartada) se refleja en la lectura siguiente.
    """
    try:
        from sqlalchemy import text as _sa_text
        from web_comparativas.models import engine
    except Exception:
        return None
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(_sa_text(
                "SELECT id FROM ind_import_run WHERE status = 'approved' "
                "ORDER BY approved_at DESC, id DESC LIMIT 1"
            )).fetchone()
        return int(row[0]) if row else None
    except Exception:
        return None


# Nombres de mes en español (es-AR) para el indicador de frescura de datos.
# Local a propósito: no dependemos del locale del sistema operativo del server.
_MESES_ES = (
    "enero", "febrero", "marzo", "abril", "mayo", "junio",
    "julio", "agosto", "septiembre", "octubre", "noviembre", "diciembre",
)


def _coerce_fecha(value):
    """Normaliza approved_at/ventana_hasta a date/datetime sea cual sea el motor.

    PostgreSQL (prod) devuelve date/datetime nativos; SQLite (local, el .bat de
    prueba) los devuelve como str ('YYYY-MM-DD' o 'YYYY-MM-DD HH:MM:SS[.ffffff]').
    Devuelve None si el valor es vacío o no parsea — el partial maneja el None.
    """
    if value is None:
        return None
    if isinstance(value, (datetime, date)):
        return value
    if isinstance(value, str):
        s = value.strip().replace("T", " ")
        for fmt in ("%Y-%m-%d %H:%M:%S.%f", "%Y-%m-%d %H:%M:%S", "%Y-%m-%d"):
            try:
                return datetime.strptime(s, fmt)
            except ValueError:
                continue
    return None


def corrida_activa_meta() -> "dict | None":
    """Metadata de la corrida 'approved' ACTIVA para el indicador de frescura de datos.

    Selecciona la MISMA corrida que _corrida_activa() (la más reciente por approved_at,
    desempate por id) pero en vez del solo id devuelve un dict listo para el template:
      - approved_at   : datetime de aprobación (crudo, normalizado)
      - ventana_hasta : MAX(ventana_hasta) entre las fuentes de ind_etl_control (el mes
                        más reciente cubierto); None si no se puede resolver
      - fecha_label   : approved_at como 'dd/mm/aaaa' (sin hora)
      - mes_label     : ventana_hasta como 'mayo 2026' (es-AR); None si no hay ventana

    Devuelve None si no hay corrida aprobada o si la consulta falla (mismo contrato
    defensivo que _corrida_activa(): el caller/partial nunca rompe).
    """
    try:
        from sqlalchemy import text as _sa_text
        from web_comparativas.models import engine
    except Exception:
        return None
    if engine is None:
        return None
    try:
        with engine.connect() as conn:
            row = conn.execute(_sa_text(
                "SELECT id, approved_at FROM ind_import_run WHERE status = 'approved' "
                "ORDER BY approved_at DESC, id DESC LIMIT 1"
            )).fetchone()
            if not row:
                return None
            run_id = int(row[0])
            approved_at = _coerce_fecha(row[1])
            # ventana_hasta: el mes más reciente cubierto entre TODAS las fuentes.
            # Aislado en su propio try: si la tabla/columna falta o está vacía,
            # degradamos a None sin perder la fecha de aprobación.
            ventana_hasta = None
            try:
                vh = conn.execute(_sa_text(
                    "SELECT MAX(ventana_hasta) FROM ind_etl_control"
                )).fetchone()
                ventana_hasta = _coerce_fecha(vh[0]) if vh else None
            except Exception:
                ventana_hasta = None

        fecha_label = approved_at.strftime("%d/%m/%Y") if approved_at else None
        mes_label = None
        if ventana_hasta is not None:
            mes_label = f"{_MESES_ES[ventana_hasta.month - 1]} {ventana_hasta.year}"

        return {
            "id": run_id,
            "approved_at": approved_at,
            "ventana_hasta": ventana_hasta,
            "fecha_label": fecha_label,
            "mes_label": mes_label,
        }
    except Exception:
        return None
