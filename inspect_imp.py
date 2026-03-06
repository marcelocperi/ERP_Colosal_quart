import os, sys
sys.path.append(os.getcwd())
try:
    from database import get_db_cursor
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SHOW CREATE TABLE imp_despachos")
        row = cursor.fetchone()
        with open('imp_create.txt', 'w', encoding='utf-8') as f:
            f.write(row['Create Table'])
except Exception as e:
    with open('imp_create.txt', 'w', encoding='utf-8') as f:
        f.write(str(e))
