# seed_admin.py
import os
from getpass import getpass
from web_comparativas.models import db_session, User
from web_comparativas.main import hash_password

EMAIL = os.getenv("ADMIN_SEED_EMAIL", "admin@local").strip()

# Leer contraseña de forma segura — nunca hardcodeada
PWD = os.getenv("ADMIN_SEED_PASSWORD", "").strip()
if not PWD:
    PWD = getpass(f"Contraseña para {EMAIL} (mín. 12 chars): ").strip()

if len(PWD) < 12:
    print("ERROR: La contraseña debe tener al menos 12 caracteres.")
    db_session.close()
    raise SystemExit(1)

u = db_session.query(User).filter_by(email=EMAIL).first()
if u:
    u.password_hash = hash_password(PWD)
    u.role = "admin"
    print(f"Contraseña actualizada para {EMAIL}")
else:
    u = User(email=EMAIL, full_name="Admin", password_hash=hash_password(PWD), role="admin")
    db_session.add(u)
    print(f"Usuario creado: {EMAIL}")

db_session.commit()
print("✅ Listo. Guardá las credenciales en un gestor de contraseñas.")
