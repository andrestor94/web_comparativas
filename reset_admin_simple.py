# reset_admin_simple.py
from web_comparativas.models import db_session, User
from passlib.context import CryptContext

# Use only pbkdf2_sha256 to avoid bcrypt issues
ctx = CryptContext(schemes=["pbkdf2_sha256"], deprecated="auto")

EMAIL = "admin@local"
PASSWORD = "admin123"

print(f"Buscando usuario {EMAIL}...")

# Buscar el usuario
u = db_session.query(User).filter_by(email=EMAIL).first()

if u:
    # Actualizar contraseña
    u.password_hash = ctx.hash(PASSWORD)
    u.role = "admin"
    db_session.commit()
    print(f"✅ Contraseña actualizada para {EMAIL}")
    print(f"\n   📧 Usuario: {EMAIL}")
    print(f"   🔑 Contraseña: {PASSWORD}")
else:
    # Crear nuevo usuario
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
    print(f"\n   📧 Usuario: {EMAIL}")
    print(f"   🔑 Contraseña: {PASSWORD}")

db_session.close()
