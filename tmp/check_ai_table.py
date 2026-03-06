import sys, os
sys.path.insert(0, os.getcwd())
from database import get_db_cursor

with get_db_cursor() as cursor:
    cursor.execute("SHOW TABLES LIKE 'sys_ai_feedback'")
    r = cursor.fetchone()
    print("Tabla sys_ai_feedback:", "EXISTE ✅" if r else "NO encontrada ❌")
    if r:
        cursor.execute("SELECT COUNT(*) FROM sys_ai_feedback")
        c = cursor.fetchone()
        print("Registros:", c[0])
        cursor.execute("DESCRIBE sys_ai_feedback")
        cols = cursor.fetchall()
        print("Columnas:", [col[0] for col in cols])
