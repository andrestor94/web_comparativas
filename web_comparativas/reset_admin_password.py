"""
Script para resetear la contraseña del usuario admin usando pbkdf2.
Uso: python reset_admin_password.py
     o: ADMIN_INITIAL_PASSWORD=<contraseña> python reset_admin_password.py
"""
import os
from getpass import getpass
from sqlalchemy import func
from passlib.context import CryptContext
from models import init_db, db_session, User

# Usar solo pbkdf2_sha256 para evitar problemas con bcrypt
ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(p):
    return ctx.hash(p)

def main():
    init_db()

    email = os.getenv("ADMIN_EMAIL", "admin@suizo.com").strip()

    # Leer contraseña de forma segura — nunca hardcodeada
    nueva_password = os.getenv("ADMIN_INITIAL_PASSWORD", "").strip()
    if not nueva_password:
        nueva_password = getpass(f"Nueva contraseña para {email} (mín. 12 chars): ").strip()

    if len(nueva_password) < 12:
        print("ERROR: La contraseña debe tener al menos 12 caracteres.")
        raise SystemExit(1)

    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email.lower())
         .first())

    if not u:
        print(f"❌ No se encontró el usuario {email}")
        return

    try:
        u.password_hash = hash_password(nueva_password)
        db_session.commit()
        print(f"✅ Contraseña actualizada para {email}")
        print("✅ Guardá las credenciales en un gestor de contraseñas.")
    except Exception as e:
        db_session.rollback()
        print(f"❌ Error al actualizar contraseña.")

if __name__ == "__main__":
    main()
