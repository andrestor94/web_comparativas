# web_comparativas/api_comments.py
from fastapi import APIRouter, Request, HTTPException, Body
from fastapi.responses import HTMLResponse
from fastapi.templating import Jinja2Templates
from pydantic import BaseModel
from typing import Optional, List, Dict, Set
from sse_starlette.sse import EventSourceResponse
from pathlib import Path
import sqlite3, os, asyncio, datetime, json

# ============================================================================
# Routers
# ============================================================================
router = APIRouter(prefix="/api/comments", tags=["comments"])
ui_router = APIRouter(tags=["ui:comments"])

# ============================================================================
# SQLite (archivo local)
# ============================================================================
DB_PATH = os.path.join(os.path.dirname(__file__), "comments.sqlite3")
_db_lock = asyncio.Lock()

def _db():
    con = sqlite3.connect(DB_PATH)
    con.row_factory = sqlite3.Row
    return con

def _init_db():
    os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)
    con = _db()
    con.executescript(
        """
        PRAGMA journal_mode=WAL;
        CREATE TABLE IF NOT EXISTS comments (
          id INTEGER PRIMARY KEY AUTOINCREMENT,
          upload_id TEXT NOT NULL,
          body TEXT NOT NULL,
          parent_id INTEGER,
          author TEXT,
          author_key TEXT,              -- identidad estable para ACL
          created_at TEXT NOT NULL,
          resolved_at TEXT,
          deleted_at TEXT,
          process_code TEXT             -- NUEVO: código visible del proceso (ej. 434-1356-LPU25)
        );
        """
    )
    # Migraciones defensivas
    cols = {r["name"] for r in con.execute("PRAGMA table_info(comments)").fetchall()}
    if "author_key" not in cols:
        con.execute("ALTER TABLE comments ADD COLUMN author_key TEXT")
        con.execute("UPDATE comments SET author_key = author WHERE author_key IS NULL")
        cols.add("author_key")
    if "process_code" not in cols:
        con.execute("ALTER TABLE comments ADD COLUMN process_code TEXT")

    # Índice parcial para roots activos por (upload_id, author_key)
    con.execute(
        """
        CREATE INDEX IF NOT EXISTS idx_comments_root_active
        ON comments(upload_id, author_key)
        WHERE parent_id IS NULL AND deleted_at IS NULL
        """
    )

    con.commit()
    con.close()

_init_db()

# ============================================================================
# Modelos
# ============================================================================
class CommentIn(BaseModel):
    upload_id: str
    body: str
    parent_id: Optional[int] = None
    process_code: Optional[str] = None  # NUEVO

class CommentOut(BaseModel):
    id: int
    upload_id: str
    body: str
    parent_id: Optional[int]
    author: Optional[str]
    created_at: str
    resolved_at: Optional[str] = None
    deleted_at: Optional[str] = None
    process_code: Optional[str] = None  # NUEVO

class ResolveIn(BaseModel):
    resolved: bool

class CommentPatch(BaseModel):
    body: Optional[str] = None
    is_resolved: Optional[bool] = None

# ============================================================================
# Pub/Sub en memoria (SSE)
# ============================================================================
_subscribers: Dict[str, Set[asyncio.Queue]] = {}

def _publish(upload_id: str, payload: dict):
    """Publica un evento para todos los suscriptores de ese upload_id."""
    for q in list(_subscribers.get(str(upload_id), set())):
        try:
            q.put_nowait(payload)
        except Exception:
            pass

# ============================================================================
# Helpers
# ============================================================================
PRIV_ROLES = {"admin", "auditor"}

def _is_priv(role: str) -> bool:
    return (role or "").lower() in PRIV_ROLES

def _now_iso():
    return datetime.datetime.utcnow().isoformat(timespec="seconds") + "Z"

def _safe_like(s: str) -> str:
    return f"%{s}%" if s else s

def _user_identity(request: Request):
    """
    Obtiene identidad del usuario para ACL.
    Retorna dict {name, role, key}. 'key' debe ser estable (id o email).
    """
    # 1) Preferimos request.state.user si existe
    u = getattr(getattr(request, "state", None), "user", None)
    if isinstance(u, dict):
        name = u.get("name") or u.get("full_name") or u.get("nombre") or u.get("email") or "Anónimo"
        role = (u.get("role") or u.get("rol") or "analista").lower()
        key = str(u.get("id") or u.get("email") or name)
        return {"name": name, "role": role, "key": key}

    # 2) Sesión
    try:
        sess = request.session
    except Exception:
        sess = {}
    if isinstance(sess, dict):
        if isinstance(sess.get("user"), dict):
            u2 = sess["user"]
            name = u2.get("name") or u2.get("full_name") or u2.get("nombre") or u2.get("email") or "Anónimo"
            role = (u2.get("role") or u2.get("rol") or "analista").lower()
            key = str(u2.get("id") or u2.get("email") or name)
            return {"name": name, "role": role, "key": key}
        name = sess.get("name") or sess.get("full_name") or sess.get("nombre") or sess.get("email")
        role = (sess.get("role") or sess.get("rol") or "analista").lower()
        uid = sess.get("user_id") or sess.get("id") or sess.get("email") or name or "anon"
        if name:
            return {"name": name, "role": role, "key": str(uid)}

    # 3) Encabezado
    x = request.headers.get("X-User")
    if x:
        return {"name": x, "role": "analista", "key": x}

    # Fallback
    return {"name": "Anónimo", "role": "analista", "key": "anon"}

def _find_root(con: sqlite3.Connection, row: sqlite3.Row) -> sqlite3.Row:
    """Camina parent_id hasta hallar el root del hilo."""
    current = row
    while current and current["parent_id"]:
        current = con.execute("SELECT * FROM comments WHERE id=?", (current["parent_id"],)).fetchone()
    return current

def _is_owner_of_thread(con: sqlite3.Connection, uid_key: str, row: sqlite3.Row) -> bool:
    """True si uid_key es dueño del hilo (autor del root)."""
    root = _find_root(con, row)
    return bool(root and (root["author_key"] or root["author"]) and (str(root["author_key"] or root["author"]) == str(uid_key)))

def _visible_for_user(con: sqlite3.Connection, uid_role: str, uid_key: str, row: sqlite3.Row) -> bool:
    """Regla de visibilidad: admin/auditor ven todo; no-priv sólo hilos propios."""
    if _is_priv(uid_role):
        return True
    return _is_owner_of_thread(con, uid_key, row)

def _parse_dt_for_where(s: str) -> str:
    if not s:
        return ""
    try:
        txt = s.strip().replace(" ", "T")
        if len(txt) == 10:
            txt += "T00:00:00"
        return datetime.datetime.fromisoformat(txt).isoformat(timespec="seconds") + "Z"
    except Exception:
        return s

# ============================================================================
# Endpoints REST
# ============================================================================

# --- Lista por upload_id (panel lateral del tablero) ---
@router.get("", response_model=List[CommentOut])
def list_comments(upload_id: str, request: Request):
    ident = _user_identity(request)
    con = _db()
    cur = con.execute(
        "SELECT * FROM comments WHERE upload_id=? AND deleted_at IS NULL ORDER BY id ASC",
        (str(upload_id),),
    )
    rows = cur.fetchall()
    # ACL
    if not _is_priv(ident["role"]):
        rows = [r for r in rows if _visible_for_user(con, ident["role"], ident["key"], r)]
    out = [dict(r) for r in rows]
    con.close()
    return out

# --- Crear comentario ---
@router.post("", response_model=CommentOut, status_code=201)
async def create_comment(req: Request, c: CommentIn):
    """
    Si viene sin parent_id, reusa/crea un hilo raíz por (upload_id + author_key).
    Si el hilo existía y estaba resuelto, se reabre (root + respuestas).
    Siempre guarda el 'process_code' del tablero (body.process_code o header X-Process-Code).
    En respuestas, hereda el 'process_code' del root.
    """
    body = (c.body or "").strip()
    if not body:
        raise HTTPException(status_code=422, detail="El cuerpo no puede estar vacío")

    ident = _user_identity(req)
    author = ident["name"] or "Anónimo"
    author_key = ident["key"]

    # process_code puede venir por body o header
    incoming_proc = (c.process_code or "").strip() or (req.headers.get("X-Process-Code") or "").strip() or None

    async with _db_lock:
        con = _db()

        # Caso: respuesta a un comentario existente
        if c.parent_id:
            parent = con.execute("SELECT * FROM comments WHERE id=?", (c.parent_id,)).fetchone()
            if not parent:
                con.close()
                raise HTTPException(status_code=404, detail="Comentario padre no existe")
            if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, author_key, parent)):
                con.close()
                raise HTTPException(status_code=403, detail="No autorizado para responder en este hilo")

            root = _find_root(con, parent)
            proc_to_use = root["process_code"]  # heredar del root

            now = _now_iso()
            cur = con.execute(
                "INSERT INTO comments (upload_id, body, parent_id, author, author_key, created_at, process_code) VALUES (?,?,?,?,?,?,?)",
                (str(c.upload_id), body, int(c.parent_id), author, author_key, now, proc_to_use),
            )
            con.commit()
            new_id = cur.lastrowid
            row = con.execute("SELECT * FROM comments WHERE id=?", (new_id,)).fetchone()
            con.close()

            out = dict(row)
            _publish(str(c.upload_id), {"type": "created", "item": out})
            return out

        # Caso: comentario "nuevo" -> reusar/crear hilo raíz
        now = _now_iso()
        root = con.execute(
            """
            SELECT * FROM comments
            WHERE upload_id = ?
              AND parent_id IS NULL
              AND deleted_at IS NULL
              AND (author_key = ? OR (author_key IS NULL AND author = ?))
            ORDER BY id DESC
            LIMIT 1
            """,
            (str(c.upload_id), author_key, author),
        ).fetchone()

        if root:
            # Reabrir hilo si estaba resuelto
            if root["resolved_at"] is not None:
                con.execute("UPDATE comments SET resolved_at = NULL WHERE id=? OR parent_id=?", (root["id"], root["id"]))

            # Insertar como respuesta dentro del hilo existente, heredando process_code del root
            cur = con.execute(
                "INSERT INTO comments (upload_id, body, parent_id, author, author_key, created_at, process_code) VALUES (?,?,?,?,?,?,?)",
                (str(c.upload_id), body, int(root["id"]), author, author_key, now, root["process_code"]),
            )
            con.commit()
            new_id = cur.lastrowid
            row = con.execute("SELECT * FROM comments WHERE id=?", (new_id,)).fetchone()
            root_after = con.execute("SELECT * FROM comments WHERE id=?", (root["id"],)).fetchone()
            con.close()

            item = dict(row)
            _publish(str(item["upload_id"]), {"type": "created", "item": item})
            if root_after:
                _publish(str(root_after["upload_id"]), {"type": "updated", "item": dict(root_after)})
            return item

        # No había root: crear uno nuevo (parent_id NULL) con el process_code recibido
        cur = con.execute(
            "INSERT INTO comments (upload_id, body, parent_id, author, author_key, created_at, process_code) VALUES (?,?,?,?,?,?,?)",
            (str(c.upload_id), body, None, author, author_key, now, incoming_proc),
        )
        con.commit()
        new_id = cur.lastrowid
        row = con.execute("SELECT * FROM comments WHERE id=?", (new_id,)).fetchone()
        con.close()

    out = dict(row)
    _publish(str(c.upload_id), {"type": "created", "item": out})
    return out

# --- Actualizar (body / is_resolved) vía PATCH ---
@router.patch("/{comment_id}", response_model=CommentOut)
async def patch_comment(comment_id: int, request: Request, patch: CommentPatch):
    ident = _user_identity(request)
    async with _db_lock:
        con = _db()
        row = con.execute("SELECT * FROM comments WHERE id=?", (comment_id,)).fetchone()
        if not row:
            con.close()
            raise HTTPException(status_code=404, detail="Comentario no encontrado")

        # Permiso: admin/auditor o dueño del hilo
        if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, ident["key"], row)):
            con.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        body = patch.body
        is_resolved = patch.is_resolved
        sets, params = [], []

        if body is not None:
            body = body.strip()
            if not body:
                con.close()
                raise HTTPException(status_code=422, detail="El cuerpo no puede estar vacío")
            sets.append("body=?")
            params.append(body)

        if is_resolved is not None:
            ts = _now_iso() if is_resolved else None
            sets.append("resolved_at=?")
            params.append(ts)

        if not sets:
            out = dict(row)
            con.close()
            return out

        params.append(comment_id)
        con.execute(f"UPDATE comments SET {', '.join(sets)} WHERE id=?", params)
        con.commit()
        row2 = con.execute("SELECT * FROM comments WHERE id=?", (comment_id,)).fetchone()
        con.close()

    item = dict(row2)
    _publish(str(item["upload_id"]), {"type": "updated", "item": item})
    return item

# --- Borrar (soft delete) ---
@router.delete("/{comment_id}", status_code=204)
async def delete_comment(comment_id: int, request: Request):
    ident = _user_identity(request)
    async with _db_lock:
        con = _db()
        row = con.execute("SELECT * FROM comments WHERE id=?", (comment_id,)).fetchone()
        if not row:
            con.close()
            raise HTTPException(status_code=404, detail="Comentario no encontrado")

        # Permiso: admin/auditor o autor del propio comentario o dueño del hilo
        own = str(row["author_key"] or row["author"]) == ident["key"]
        if (not _is_priv(ident["role"])) and not (own or _is_owner_of_thread(con, ident["key"], row)):
            con.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        now = _now_iso()
        con.execute("UPDATE comments SET deleted_at=? WHERE id=?", (now, comment_id))
        con.commit()
        upid = row["upload_id"]
        con.close()
    _publish(str(upid), {"type": "deleted", "id": comment_id})
    return

# --- Resolver / Reabrir explícito ---
@router.post("/{comment_id}/resolve")
async def resolve_comment(comment_id: int, request: Request, body: ResolveIn):
    ident = _user_identity(request)
    ts = _now_iso() if body.resolved else None

    async with _db_lock:
        con = _db()

        row = con.execute(
            "SELECT * FROM comments WHERE id=?",
            (int(comment_id),)
        ).fetchone()
        if not row:
            con.close()
            raise HTTPException(status_code=404, detail="Comentario no encontrado")

        # Permiso: admin/auditor o dueño del hilo
        if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, ident["key"], row)):
            con.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        # ✅ marcar todo el hilo (root + respuestas)
        root = _find_root(con, row)
        con.execute(
            "UPDATE comments SET resolved_at=? WHERE id=? OR parent_id=?",
            (ts, root["id"], root["id"])
        )
        con.commit()

        root_after = con.execute(
            "SELECT * FROM comments WHERE id=?",
            (root["id"],)
        ).fetchone()

        con.close()

    if root_after:
        _publish(str(root_after["upload_id"]), {"type": "updated", "item": dict(root_after)})
    return {"ok": True}

# --- Resolver / Reabrir TODO el hilo (root + respuestas) ---
@router.post("/thread/resolve")
async def resolve_thread(
    request: Request,
    upload_id: str = Body(...),
    root_id: int = Body(...),
    resolved: bool = Body(...),
):
    ident = _user_identity(request)
    ts = _now_iso() if resolved else None

    async with _db_lock:
        con = _db()
        root = con.execute(
            "SELECT * FROM comments WHERE id=? AND upload_id=? AND deleted_at IS NULL",
            (int(root_id), str(upload_id)),
        ).fetchone()
        if not root:
            con.close()
            raise HTTPException(status_code=404, detail="Hilo no encontrado")

        # Permisos: admin/auditor o dueño del hilo
        if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, ident["key"], root)):
            con.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        # Resolver o reabrir TODOS los mensajes de ese hilo
        con.execute(
            "UPDATE comments SET resolved_at=? WHERE upload_id=? AND (id=? OR parent_id=?)",
            (ts, str(upload_id), int(root_id), int(root_id)),
        )
        con.commit()

        # Re-leemos el root para emitir un update por SSE
        row = con.execute("SELECT * FROM comments WHERE id=?", (int(root_id),)).fetchone()
        con.close()

    if row:
        _publish(str(upload_id), {"type": "updated", "item": dict(row)})
    return {"ok": True}

# --- Acuse de recibo (y opcional resolver) ---
@router.post("/{comment_id}/ack", response_model=CommentOut)
async def comments_ack(comment_id: int,
                       request: Request,
                       resolve: bool = Body(False),
                       note: str = Body("")):
    ident = _user_identity(request)
    async with _db_lock:
        con = _db()
        c = con.execute("SELECT * FROM comments WHERE id=?", (comment_id,)).fetchone()
        if not c:
            con.close()
            raise HTTPException(status_code=404, detail="Comentario no encontrado")

        # ACL: admin/auditor siempre; no-priv sólo en su propio hilo
        if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, ident["key"], c)):
            con.close()
            raise HTTPException(status_code=403, detail="No autorizado")

        body_txt = (note or "").strip() or "Recibido ✅"
        now = _now_iso()
        root = _find_root(con, c)

        cur = con.execute(
            "INSERT INTO comments (upload_id, body, parent_id, author, author_key, created_at, process_code) VALUES (?,?,?,?,?,?,?)",
            (str(c["upload_id"]), body_txt, comment_id, ident["name"] or "admin", ident["key"], now, root["process_code"]),
        )

        # Si es admin/auditor y pide resolver, resolvemos TODO el hilo (root + respuestas)
        if _is_priv(ident["role"]) and resolve:
            con.execute(
                "UPDATE comments SET resolved_at=? WHERE id=? OR parent_id=?",
                (now, root["id"], root["id"])
            )

        con.commit()
        new_id = cur.lastrowid
        ack_row = con.execute("SELECT * FROM comments WHERE id=?", (new_id,)).fetchone()
        root_row = _find_root(con, c) if (_is_priv(ident["role"]) and resolve) else None
        con.close()

    item = dict(ack_row)
    _publish(str(item["upload_id"]), {"type": "created", "item": item})
    if root_row is not None:
        _publish(str(root_row["upload_id"]), {"type": "updated", "item": dict(root_row)})
    return item

# --- Resumen para el badge del menú ---
@router.get("/admin")
def admin_summary(request: Request, page_size: int = 10):
    """Counters + últimos N comentarios (no borrados). Admin/Auditor = global; otros = propio."""
    page_size = max(1, min(100, int(page_size or 10)))
    ident = _user_identity(request)
    con = _db()

    if not _is_priv(ident["role"]):
        # Contar sólo hilos visibles para el usuario
        all_rows = con.execute("SELECT * FROM comments WHERE deleted_at IS NULL").fetchall()
        visible = [r for r in all_rows if _visible_for_user(con, ident["role"], ident["key"], r)]
        total_open = sum(1 for r in visible if r["resolved_at"] is None)
        total_resolved = sum(1 for r in visible if r["resolved_at"] is not None)
        total_deleted = con.execute("SELECT COUNT(*) FROM comments WHERE deleted_at IS NOT NULL").fetchone()[0]
        visible.sort(key=lambda r: (r["created_at"], r["id"]), reverse=True)
        items = [dict(r) for r in visible[:page_size]]
        con.close()
        return {
            "counters": {"open": int(total_open), "resolved": int(total_resolved), "deleted": int(total_deleted)},
            "items": items,
        }

    # Priv: global
    total_open = con.execute(
        "SELECT COUNT(*) FROM comments WHERE deleted_at IS NULL AND resolved_at IS NULL"
    ).fetchone()[0]
    total_resolved = con.execute(
        "SELECT COUNT(*) FROM comments WHERE deleted_at IS NULL AND resolved_at IS NOT NULL"
    ).fetchone()[0]
    total_deleted = con.execute(
        "SELECT COUNT(*) FROM comments WHERE deleted_at IS NOT NULL"
    ).fetchone()[0]
    cur = con.execute(
        """
        SELECT * FROM comments
        WHERE deleted_at IS NULL
        ORDER BY created_at DESC, id DESC
        LIMIT ?
        """,
        (page_size,),
    )
    items = [dict(r) for r in cur.fetchall()]
    con.close()
    return {
        "counters": {"open": int(total_open), "resolved": int(total_resolved), "deleted": int(total_deleted)},
        "items": items,
    }

# --- Bandeja general AGRUPADA por hilo (root_id) ---
@router.get("/inbox")
def inbox(
    request: Request,
    status: str = "all",          # all|open|resolved (a nivel HILO)
    q: str = "",
    author: str = "",
    upload_id: str = "",          # "30" ó "30,31"
    page: int = 1,
    page_size: int = 50,
    sort: str = "-created_at",    # usa last_at del hilo
    dt_from: str = "",
    dt_to: str = "",
):
    ident = _user_identity(request)
    dt_from_iso, dt_to_iso = _parse_dt_for_where(dt_from), _parse_dt_for_where(dt_to)

    # 1) Filtro base a nivel MENSAJE
    where, params = ["deleted_at IS NULL"], []
    if q:        where.append("body LIKE ?");   params.append(_safe_like(q))
    if author:   where.append("author LIKE ?"); params.append(_safe_like(author))
    if upload_id:
        ids = [x.strip() for x in upload_id.split(",") if x.strip()]
        if len(ids) == 1:
            where.append("upload_id = ?"); params.append(ids[0])
        elif len(ids) > 1:
            where.append(f"upload_id IN ({','.join('?' for _ in ids)})"); params.extend(ids)
    if dt_from_iso: where.append("created_at >= ?"); params.append(dt_from_iso)
    if dt_to_iso:   where.append("created_at <= ?"); params.append(dt_to_iso)

    sql_where = " AND ".join(where)
    con = _db()
    rows = con.execute(
        f"SELECT * FROM comments WHERE {sql_where} ORDER BY created_at ASC, id ASC",
        params,
    ).fetchall()

    # 2) ACL por mensaje
    if not _is_priv(ident["role"]):
        rows = [r for r in rows if _visible_for_user(con, ident["role"], ident["key"], r)]

    # 3) Agrupar por hilo (USAR root_id como clave para no mezclar hilos)
    threads: Dict[int, dict] = {}
    for r in rows:
        root = _find_root(con, r)
        if not root:
            continue
        rid = int(root["id"])

        if rid not in threads:
            threads[rid] = {
                "upload_id": str(root["upload_id"]),
                "author": root["author"],
                "author_key": root["author_key"] or root["author"],
                "root_id": rid,
                "last_id": int(r["id"]),
                "last_body": r["body"],
                "last_at": r["created_at"],
                "total": 0,
                "process_code": root["process_code"],  # NUEVO
                # Estado del hilo por ROOT
                "status": "open" if (root["resolved_at"] is None) else "resolved",
            }

        t = threads[rid]
        # actualizar “último”
        if str(r["created_at"]) >= str(t["last_at"]):
            t["last_id"] = int(r["id"])
            t["last_body"] = r["body"]
            t["last_at"] = r["created_at"]

        t["total"] += 1

    # 4) Derivar bandera 'unresolved' y construir la lista base
    thread_list = []
    for t in threads.values():
        t["unresolved"] = 1 if t["status"] == "open" else 0
        thread_list.append(t)

    # 5) Filtro por estado a nivel HILO
    if status == "open":
        filtered = [t for t in thread_list if t["status"] == "open"]
    elif status == "resolved":
        filtered = [t for t in thread_list if t["status"] == "resolved"]
    else:
        filtered = thread_list

    # 6) Orden por last_at (desc)
    desc = str(sort or "").startswith("-")
    filtered.sort(key=lambda x: (x["last_at"], x["root_id"]), reverse=desc)

    # 7) Contadores (según filtros base + ACL; sin filtrar por status)
    counters = {
        "open": sum(1 for t in threads.values() if t["status"] == "open"),
        "resolved": sum(1 for t in threads.values() if t["status"] == "resolved"),
        "deleted": 0,
    }

    # 8) Paginación a nivel HILO
    limit = max(1, min(500, int(page_size or 50)))
    page = max(1, int(page or 1))
    total_threads = len(filtered)
    start, end = (page - 1) * limit, (page - 1) * limit + limit
    page_items = filtered[start:end]

    # 9) Shape de salida (compat: id = last_id)
    items = [
        {
            "upload_id": it["upload_id"],
            "author": it["author"],
            "root_id": it["root_id"],
            "last_id": it["last_id"],    # compat con la UI actual
            "last_body": it["last_body"],
            "last_at": it["last_at"],
            "total": it["total"],
            "status": it["status"],
            "unresolved": it["unresolved"],
            "process_code": it["process_code"],  # NUEVO
            "id": it["last_id"],
        }
        for it in page_items
    ]

    con.close()
    return {
        "page": page,
        "page_size": limit,
        "total": int(total_threads),
        "items": items,
        "counters": counters,
    }

# --- Hilo seguro (root + respuestas) ---
@router.get("/thread")
def comments_thread(
    request: Request,
    upload_id: str,
    root_id: int,
):
    ident = _user_identity(request)
    con = _db()

    root = con.execute(
        "SELECT * FROM comments WHERE id=? AND upload_id=? AND deleted_at IS NULL",
        (int(root_id), str(upload_id)),
    ).fetchone()
    if not root:
        con.close()
        raise HTTPException(status_code=404, detail="Hilo no encontrado")

    if (not _is_priv(ident["role"])) and (not _is_owner_of_thread(con, ident["key"], root)):
        con.close()
        raise HTTPException(status_code=403, detail="Sin permiso para ver este hilo")

    rows = con.execute(
        """
        SELECT * FROM comments
        WHERE deleted_at IS NULL
          AND upload_id = ?
          AND (id = ? OR parent_id = ?)
        ORDER BY created_at ASC, id ASC
        """,
        (str(upload_id), int(root_id), int(root_id)),
    ).fetchall()
    con.close()

    return {"items": [dict(r) for r in rows]}

# --- SSE stream (GET) ---
@router.get("/stream")
async def stream(upload_id: str, request: Request):
    key = str(upload_id)
    ident = _user_identity(request)

    q: asyncio.Queue = asyncio.Queue()
    _subscribers.setdefault(key, set()).add(q)

    async def gen():
        con = _db()
        try:
            while True:
                payload = await q.get()
                # Filtrado por usuario NO PRIV
                if not _is_priv(ident["role"]):
                    typ = payload.get("type")
                    if typ in ("created", "updated"):
                        item = payload.get("item")
                        if item is None:
                            continue
                        row = con.execute("SELECT * FROM comments WHERE id=?", (item.get("id"),)).fetchone()
                        if not row or not _visible_for_user(con, ident["role"], ident["key"], row):
                            continue
                    elif typ == "deleted":
                        cid = payload.get("id")
                        row = con.execute("SELECT * FROM comments WHERE id=?", (cid,)).fetchone()
                        if not row or not _visible_for_user(con, ident["role"], ident["key"], row):
                            continue
                yield {"event": "message", "data": json.dumps(payload, ensure_ascii=False)}
        except asyncio.CancelledError:
            pass
        finally:
            _subscribers.get(key, set()).discard(q)
            con.close()

    return EventSourceResponse(gen())

# ============================================================================
# UI (bandeja)  ->  /comentarios
# ============================================================================
templates = Jinja2Templates(directory=str(Path(__file__).resolve().parent / "templates"))

def _user_for_template(request: Request):
    """
    Intenta recuperar el usuario logueado de la misma forma que el resto de tu app.
    Devuelve None si no hay sesión.
    """
    u = getattr(getattr(request, "state", None), "user", None)
    if u:
        return u
    try:
        sess = request.session
    except Exception:
        sess = {}
    if isinstance(sess, dict):
        if sess.get("user"):
            return sess["user"]
        if sess.get("user_id"):
            return {
                "id": sess.get("user_id"),
                "name": sess.get("name") or sess.get("full_name") or sess.get("nombre") or sess.get("email"),
                "role": sess.get("role") or sess.get("rol") or "analista",
                "email": sess.get("email"),
            }
    x = request.headers.get("X-User")
    if x:
        return {"name": x, "role": "analista"}
    return None

@router.get("/ui", response_class=HTMLResponse)
def comments_inbox_page(request: Request):
    user = _user_for_template(request)
    return templates.TemplateResponse(
        "comments_inbox.html",
        {
            "request": request,
            "user": user,  # pasar el user al template base.html
        },
    )
