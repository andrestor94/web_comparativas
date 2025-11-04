import os, sqlite3

# Intentamos en las dos rutas que vi en tus capturas
DBS = ("web_comparativas/app.db", "app.db")

for path in DBS:
    if not os.path.exists(path):
        continue
    print(f"\n>>> Trabajando sobre: {path}")
    con = sqlite3.connect(path)
    cur = con.cursor()

    # 1) Agregar columna name si no existe
    try:
        cur.execute("ALTER TABLE users ADD COLUMN name TEXT")
        print("   - Columna 'name' agregada")
    except Exception as e:
        print(f"   - Saltando ADD COLUMN (quizás ya existe): {e}")

    # 2) Backfill: completar name con el alias del email si está vacío
    cur.execute("""
        UPDATE users
        SET name = substr(email, 1, instr(email, '@') - 1)
        WHERE (name IS NULL OR name = '')
          AND email LIKE '%@%';
    """)
    con.commit()
    con.close()
    print("   - Backfill OK")

print("\nListo. Reinicia la app y probá editar/crear un usuario con Nombre real.")
