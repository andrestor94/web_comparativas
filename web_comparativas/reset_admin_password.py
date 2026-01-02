"""
Script para resetear la contraseña del usuario admin usando pbkdf2.
"""
from sqlalchemy import func
from passlib.context import CryptContext
from models import init_db, db_session, User

# Usar solo pbkdf2_sha256 para evitar problemas con bcrypt
ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

def hash_password(p):
    return ctx.hash(p)

def main():
    init_db()
    
    # Buscar el usuario admin
    email = "admin@suizo.com"
    u = (db_session.query(User)
         .filter(func.lower(func.trim(User.email)) == email.lower())
         .first())
    
    if not u:
        print(f"❌ No se encontró el usuario {email}")
        return
    
    # Nueva contraseña simple
    nueva_password = "admin123"
    
    try:
        u.password_hash = hash_password(nueva_password)
        db_session.commit()
        print(f"✅ Contraseña actualizada para {email}")
        print(f"")
        print(f"   📧 Email: {email}")
        print(f"   🔑 Nueva contraseña: {nueva_password}")
        print(f"")
        print(f"Ahora puedes iniciar sesión con estas credenciales.")
    except Exception as e:
        db_session.rollback()
        print(f"❌ Error: {e}")

if __name__ == "__main__":
    main()
