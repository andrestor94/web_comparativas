import sys
import os
sys.path.insert(0, os.path.join(os.path.dirname(__file__), 'web_comparativas'))

from models import User, Base
from sqlalchemy import create_engine
from sqlalchemy.orm import sessionmaker
from passlib.context import CryptContext

# Configurar DB
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

# Buscar admin
admin = session.query(User).filter(User.email.like('%admin%')).first()

if admin:
    print(f"\n✓ Usuario admin encontrado: {admin.email}")
    # Resetear contraseña
    admin.password_hash = pwd_context.hash('admin123')
    session.commit()
    print("✓ Contraseña reseteada exitosamente")
else:
    print("\n✗ No se encontró usuario admin. Creando uno nuevo...")
    new_admin = User(
        email='admin@suizo.com',
        name='Admin',
        role='admin',
        password_hash=pwd_context.hash('admin123')
    )
    session.add(new_admin)
    session.commit()
    print("✓ Usuario admin creado exitosamente")

print("\n=== CREDENCIALES ===")
print("📧 Email: admin@suizo.com")
print("🔑 Contraseña: admin123")
print("====================\n")

session.close()
