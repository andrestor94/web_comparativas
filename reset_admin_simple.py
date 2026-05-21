# reset_admin_simple.py
import os
from getpass import getpass
from web_comparativas.models import db_session, User
from passlib.context import CryptContext

# Use only pbkdf2_sha256 to avoid bcrypt issues
ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

EMAIL = os.getenv("ADMIN_EMAIL", "admin@local").strip()

# Leer contraseña de forma segura — nunca hardcodeada
PASSWORD = os.getenv("ADMIN_INITIAL_PASSWORD", "").strip()
if not PASSWORD:
    PASSWORD = getpass(f"Nueva contraseña para {EMAIL} (mín. 12 chars): ").strip()

if len(PASSWORD) < 12:
    print("ERROR: La contraseña debe tener al menos 12 caracteres.")
    db_session.close()
    raise SystemExit(1)

print(f"Buscando usuario {EMAIL}...")

u = db_session.query(User).filter_by(email=EMAIL).first()

if u:
    u.password_hash = ctx.hash(PASSWORD)
    u.role = "admin"
    db_session.commit()
    print(f"✅ Contraseña actualizada para {EMAIL}")
else:
    u = User(
        email=EMAIL,
        full_name="Admin",
        name="Admin",
        password_hash=ctx.hash(PASSWORD),
        role="admin"
    )
    db_session.add(u)
    db_session.commit()
    print(f"✅ Usuario creado: {EMAIL}")

print("✅ Listo. Guardá las credenciales en un gestor de contraseñas.")
db_session.close()
