import traceback
from database import get_db_cursor

try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW TABLES")
        tables = [list(r.values())[0] for r in cursor.fetchall()]
        user_tables = [t for t in tables if 'user' in t.lower() or 'usuario' in t.lower()]
        
        print("Tablas de usuarios encontradas:")
        for t in user_tables:
            print(f"- {t}")
except Exception as e:
    traceback.print_exc()
