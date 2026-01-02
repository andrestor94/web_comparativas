from web_comparativas.models import init_db, db_session, User

def list_and_restore():
    print("Listing users...")
    users = db_session.query(User).all()
    for u in users:
        print(f"ID: {u.id} | Name: {u.name} | Email: {u.email} | Role: {u.role}")
        if u.role == 'analista':
            print(f"-> Restaurando rol de Admin para {u.name} ({u.email})...")
            u.role = 'admin'
            db_session.commit()
            print("   [OK] Rol actualizado.")

if __name__ == "__main__":
    list_and_restore()
