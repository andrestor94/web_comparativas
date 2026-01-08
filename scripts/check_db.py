
import sqlite3
import os

db_path = r"c:\Users\ANDRES.TORRES\Desktop\web_comparativas_v2- ok\web_comparativas_v2- ok\web_comparativas_v2\web_comparativas\app.db"

print(f"Checking DB: {db_path}")
if not os.path.exists(db_path):
    print("DB DOES NOT EXIST FILE!")
    exit(1)

conn = sqlite3.connect(db_path)
cursor = conn.cursor()

# List tables
cursor.execute("SELECT name FROM sqlite_master WHERE type='table';")
tables = cursor.fetchall()
print("TABLES:", tables)

# Check uploads
if ('uploads',) in tables:
    cursor.execute("SELECT count(*) FROM uploads;")
    count = cursor.fetchone()[0]
    print(f"UPLOADS COUNT: {count}")
    
    if count > 0:
        cursor.execute("SELECT id, proceso_nro, created_at, base_dir FROM uploads WHERE base_dir LIKE '%f781%';")
        rows = cursor.fetchall()
        print("MATCHING UPLOADS (f781):")
        for r in rows:
            print(r)
        
        cursor.execute("SELECT id, proceso_nro, created_at FROM uploads ORDER BY id DESC LIMIT 5;")
else:
    print("uploads TABLE MISSING")
    
if ('users',) in tables:
    cursor.execute("SELECT count(*) FROM users;")
    print(f"USERS COUNT: {cursor.fetchone()[0]}")

conn.close()
