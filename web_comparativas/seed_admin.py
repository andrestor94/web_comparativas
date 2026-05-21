# seed_admin.py (colócalo dentro de web_comparativas\web_comparativas)
import os
from sqlalchemy import func
from passlib.context import CryptContext
from models import init_db, db_session, User  # <- importante: import local al paquete

ctx = CryptContext(schemes=["bcrypt", "pbkdf2_sha256"], deprecated="auto")

def hash_password(p): return ctx.hash(p)

def verify_password(p, h):
    if not h: return False
    try:
        if str(h).startswith("$"):
            return ctx.verify(p, h)
        return False  # nunca aceptar texto plano
    except Exception:
        return False

EMAIL = os.getenv("ADMIN_SEED_EMAIL", "admin@suizo.com").strip()
ROLE = "admin"
NAME = "Admin"

def main():
    # Leer contraseña de forma segura — nunca hardcodeada
    password = os.getenv("ADMIN_SEED_PASSWORD", "").strip()
    if not password:
        from getpass import getpass as _getpass
        password = _getpass(f"Contraseña para {EMAIL} (mín. 12 chars): ").strip()
    if len(password) < 12:
        print("ERROR: La contraseña debe tener al menos 12 caracteres.")
        raise SystemExit(1)

    init_db()
    email_norm = EMAIL.strip().lower()

    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email_norm)
         .first())

    if u is None:
        attrs = {"email": email_norm}
        if hasattr(User, "password_hash"):
            attrs["password_hash"] = hash_password(password)
        elif hasattr(User, "password"):
            attrs["password"] = hash_password(password)
        if hasattr(User, "role"):
            attrs["role"] = ROLE
        if hasattr(User, "name"):
            attrs["name"] = NAME
        u = User(**attrs)
        db_session.add(u)
        db_session.commit()
        print("✅ Usuario creado:", u.id, u.email)
    else:
        if hasattr(u, "password_hash"):
            u.password_hash = hash_password(password)
        elif hasattr(u, "password"):
            u.password = hash_password(password)
        if hasattr(u, "role"):
            u.role = ROLE
        if hasattr(u, "name"):
            u.name = NAME
        db_session.commit()
        print("✅ Usuario actualizado:", u.id, u.email)

    # Verificación
    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email_norm)
         .first())
    stored = getattr(u, "password_hash", None) or getattr(u, "password", None)
    print("Prueba de verificación:", verify_password(password, stored))
    print("✅ Listo. Guardá las credenciales en un gestor de contraseñas.")

if __name__ == "__main__":
    main()
