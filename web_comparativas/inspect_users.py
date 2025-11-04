# inspect_users.py  (UBICACIÓN: web_comparativas\web_comparativas)
from sqlalchemy import func
from models import init_db, db_session, User  # <<< import local, igual que seed_admin.py

def field(u, name):
    return getattr(u, name) if hasattr(u, name) else None

def main():
    init_db()
    try:
        print("DB URL ->", db_session.bind.url)
    except Exception:
        pass

    print("== Conectado a la base ==")
    try:
        cols = [c.name for c in User.__table__.columns]
        print("Columnas User:", cols)
    except Exception as e:
        print("No pude inspeccionar columnas:", e)

    total = db_session.query(User).count()
    print("Total usuarios:", total)

    for u in db_session.query(User).all():
        print({
            "id": u.id,
            "email": u.email,
            "role": field(u, "role"),
            "tiene_password_hash": bool(field(u, "password_hash")),
            "tiene_password_legacy": bool(field(u, "password")),
        })

if __name__ == "__main__":
    main()
