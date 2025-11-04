# seed_admin.py (colócalo dentro de web_comparativas\web_comparativas)
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
        return h == p
    except Exception:
        return h == p

EMAIL = "admin@suizo.com"
PASSWORD = "TuClaveFuerte123"
ROLE = "admin"
NAME = "Admin"

def main():
    init_db()
    email_norm = EMAIL.strip().lower()

    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email_norm)
         .first())

    if u is None:
        attrs = {"email": email_norm}
        if hasattr(User, "password_hash"):
            attrs["password_hash"] = hash_password(PASSWORD)
        elif hasattr(User, "password"):
            attrs["password"] = hash_password(PASSWORD)  # guardamos hash también
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
            u.password_hash = hash_password(PASSWORD)
        elif hasattr(u, "password"):
            u.password = hash_password(PASSWORD)
        if hasattr(u, "role"):
            u.role = ROLE
        if hasattr(u, "name"):
            u.name = NAME
        db_session.commit()
        print("🔁 Usuario actualizado:", u.id, u.email)

    # Verificación
    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email_norm)
         .first())
    stored = getattr(u, "password_hash", None) or getattr(u, "password", None)
    print("Prueba de verificación:", verify_password(PASSWORD, stored))

if __name__ == "__main__":
    main()
