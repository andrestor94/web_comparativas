import sys
import os
from getpass import getpass
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'web_comparativas'))

from models import User, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from passlib.context import CryptContext

# Configurar DB (solo SQLite local — no usar contra producción)
engine = create_engine('sqlite:///web_comparativas/app.db')
Session = sessionmaker(bind=engine)
session = Session()

# Configurar hash
pwd_context = CryptContext(schemes=["bcrypt", "pbkdf2_sha256"], deprecated="auto")

# Buscar todos los usuarios
users = session.query(User).all()
print(f"\n=== Usuarios en la base de datos ({len(users)}) ===")
for u in users:
    print(f"  • Email: {u.email} | Role: {u.role} | Name: {getattr(u, 'name', 'N/A')}")

# Leer contraseña de forma segura — nunca hardcodeada
NEW_PASSWORD = os.getenv("ADMIN_INITIAL_PASSWORD", "").strip()
if not NEW_PASSWORD:
    NEW_PASSWORD = getpass("\nIngresá la nueva contraseña del administrador (mín. 12 chars): ").strip()

if len(NEW_PASSWORD) < 12:
    print("ERROR: La contraseña debe tener al menos 12 caracteres.")
    session.close()
    sys.exit(1)

# Buscar admin
admin = session.query(User).filter(User.email.like('%admin%')).first()

if admin:
    print(f"\n✓ Usuario admin encontrado: {admin.email}")
    admin.password_hash = pwd_context.hash(NEW_PASSWORD)
    session.commit()
    print("✓ Contraseña reseteada exitosamente")
else:
    ADMIN_EMAIL = os.getenv("ADMIN_EMAIL", "admin@suizo.com").strip()
    print(f"\n✗ No se encontró usuario admin. Creando uno nuevo ({ADMIN_EMAIL})...")
    new_admin = User(
        email=ADMIN_EMAIL,
        name='Admin',
        role='admin',
        password_hash=pwd_context.hash(NEW_PASSWORD)
    )
    session.add(new_admin)
    session.commit()
    print("✓ Usuario admin creado exitosamente")

print("\n✅ Operación completada. Guardá las credenciales en un gestor de contraseñas.\n")
session.close()
