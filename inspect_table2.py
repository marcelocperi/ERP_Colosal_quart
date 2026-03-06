import os, sys
sys.path.append(os.getcwd())
from database import get_db_cursor
try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("DESCRIBE cmp_ordenes_compra")
        cols1 = [r['Field'] for r in cursor.fetchall()]
        cursor.execute("DESCRIBE cmp_detalles_orden")
        cols2 = [r['Field'] for r in cursor.fetchall()]
        
        with open("table_cols.txt", "w") as f:
            f.write("cmp_ordenes_compra:\n" + ", ".join(cols1) + "\n\n")
            f.write("cmp_detalles_orden:\n" + ", ".join(cols2) + "\n")
except Exception as e:
    with open("table_cols.txt", "w") as f:
        f.write(str(e))
