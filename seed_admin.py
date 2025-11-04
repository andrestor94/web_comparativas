# seed_admin.py
from web_comparativas.models import db_session, User
from web_comparativas.main import hash_password

EMAIL = "admin@local"
PWD   = "admin123"

u = db_session.query(User).filter_by(email=EMAIL).first()
if u:
    u.password_hash = hash_password(PWD)
    u.role = "admin"
    print(f"Contraseña actualizada para {EMAIL}")
else:
    u = User(email=EMAIL, full_name="Admin", password_hash=hash_password(PWD), role="admin")
    db_session.add(u)
    print(f"Usuario creado: {EMAIL} / {PWD}")

db_session.commit()
