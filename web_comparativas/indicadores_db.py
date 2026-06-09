"""
Conexión a SQL Server para el módulo Indicadores Comerciales.
Apunta al .env y al bridge PowerShell de 03 - Rentabilidad Negativa.
No modifica app.db ni depende de pyodbc (usa el puente sqlclient).
"""

from contextlib import contextmanager
from datetime import date, datetime
import json
from pathlib import Path
import subprocess
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


class SqlClientCursor:
    def __init__(self, connection_config: dict):
        self.connection_config = connection_config
        self.description: list = []
        self._rows: list = []

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
                timeout=65,   # 65s: el JS AbortController corta al cliente a los 70s
                check=False,
            )
            if completed.returncode != 0:
                message = (completed.stderr or completed.stdout or "").strip()
                raise RuntimeError(message)

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
    conn = None
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
        yield conn
    except Exception as exc:
        raise RuntimeError(f"Error de conexión a ETL_Data: {exc}") from exc
    finally:
        if conn:
            try:
                conn.close()
            except Exception:
                pass


@contextmanager
def get_fusion_db():
    conn = None
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
        yield conn
    except Exception as exc:
        raise RuntimeError(f"Error de conexión a Fusion: {exc}") from exc
    finally:
        if conn:
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
