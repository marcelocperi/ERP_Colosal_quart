import os
import sys

# Ensure we can import from the current directory
sys.path.append(os.path.dirname(os.path.abspath(__file__)))

from database import get_db_cursor

def find_efectivo_id():
    with get_db_cursor(dictionary=True) as cursor:
        cursor.execute("SELECT id FROM fin_condiciones_pago WHERE enterprise_id = 0 AND nombre LIKE '%Efectivo%' LIMIT 1")
        res = cursor.fetchone()
        if res:
            print(f"ID_FOUND:{res['id']}")
        return res['id'] if res else None

if __name__ == "__main__":
    efectivo_id = find_efectivo_id()
    if efectivo_id:
        print(f"ID seleccionado: {efectivo_id}")
    else:
        print("No se encontró la condición 'Efectivo' para empresa 0")
