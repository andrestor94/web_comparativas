from fastapi import Depends, HTTPException, Request
from typing import Optional
from .models import User, db_session
from passlib.context import CryptContext
import re

# ======================================================================
# NOMBRE A MOSTRAR DEL USUARIO
# ======================================================================
def user_display(u: Optional[User]) -> str:
    """
    Intenta construir un nombre legible a partir de los campos disponibles.
    """
    if not u:
        return ""

    for attr in ("name", "full_name", "nombre"):
        v = getattr(u, attr, None)
        if v and str(v).strip():
            return str(v).strip()

    email = getattr(u, "email", "") or ""
    alias = email.split("@")[0] if "@" in email else email
    alias = re.sub(r"[._-]+", " ", alias).strip().title()
    return alias or ""

# ======================================================================
# AUTENTICACIÓN Y ROLES helpers
# ======================================================================
def _reset_session():
    try:
        if hasattr(db_session, "remove"):
            db_session.remove()
        else:
            db_session.close()
    except Exception:
        pass

def get_current_user(request: Request) -> Optional[User]:
    """
    Obtiene el usuario logueado a partir de la sesión.
    Intenta recuperar la sesión si falló la primera vez.
    """
    uid = request.session.get("uid")
    if not uid:
        return None

    try:
        return db_session.get(User, uid)
    except Exception:
        _reset_session()
        try:
            return db_session.get(User, uid)
        except Exception:
            return None

def require_roles(*roles: str):
    """
    Dependencia que exige uno de los roles indicados.
    """
    roles_norm = {r.lower() for r in roles} if roles else set()

    def _dep(request: Request) -> User:
        user = get_current_user(request)
        if user is None:
            raise HTTPException(status_code=401, detail="Inicie sesión")

        if roles_norm and (user.role or "").lower() not in roles_norm:
            raise HTTPException(status_code=403, detail="No autorizado")

        return user

    return _dep

# ======================================================================
# CRYPTO HELPERS
# ======================================================================
# ======================================================================
# CRYPTO HELPERS
# ======================================================================
# Changed default to pbkdf2_sha256 to avoid bcrypt 72-byte limit and passlib issues
pwd_context = CryptContext(
    schemes=["pbkdf2_sha256", "bcrypt"],
    deprecated="auto",
)

def hash_password(p: str) -> str:
    return pwd_context.hash(p)

def verify_password(p: str, h) -> bool:
    """
    Verifica una contraseña. Soporta hashes vigentes y fallbacks.
    """
    if not h:
        return False
    hs = h.decode() if isinstance(h, (bytes, bytearray)) else str(h)
    
    try:
        # Try passlib first
        return pwd_context.verify(p, hs)
    except Exception:
        # Passlib failed (likely bcrypt version mismatch). Fallback manually.
        # Check if it looks like bcrypt ($2a$, $2b$, $2y$)
        if hs.startswith(("$2a$", "$2b$", "$2y$")):
            try:
                import bcrypt
                # bcrypt requires bytes
                return bcrypt.checkpw(p.encode('utf-8'), hs.encode('utf-8'))
            except Exception:
                pass
                
        # Fallback: Plain text (legacy)
        return hs == p

