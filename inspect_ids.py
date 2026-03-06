import os, sys
sys.path.append(os.getcwd())
from database import get_db_cursor
try:
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id, nombre FROM erp_terceros LIMIT 5")
        for row in cursor.fetchall():
            print(f"PROVEEDOR: {row['id']} - {row['nombre']}")
        
        cursor.execute("SELECT id, nombre FROM stk_articulos LIMIT 5")
        for row in cursor.fetchall():
            print(f"ARTICULO: {row['id']} - {row['nombre']}")
except Exception as e:
    print(e)
