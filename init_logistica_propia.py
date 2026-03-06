"""
Script para crear el registro 'Consumidor Final / Propia' en stk_logisticas
si no existe, con enterprise_id = 0 (global para todas las empresas).
"""
import sys
sys.path.insert(0, 'multiMCP')
from database import get_db_cursor

with get_db_cursor(dictionary=True) as cursor:
    # Ver estructura de la tabla
    cursor.execute("DESCRIBE stk_logisticas")
    cols = cursor.fetchall()
    print("Columnas de stk_logisticas:")
    for c in cols:
        print(" ", c)

    # Buscar si ya existe
    cursor.execute(
        "SELECT id, nombre, enterprise_id FROM stk_logisticas WHERE nombre LIKE %s",
        ('%Consumidor%',)
    )
    rows = cursor.fetchall()
    print("\nRegistros existentes tipo 'Consumidor':", rows)

    if not rows:
        # Insertar con enterprise_id = 0 (global)
        cursor.execute(
            "INSERT INTO stk_logisticas (enterprise_id, nombre, cuit, activo) VALUES (0, %s, %s, 1)",
            ('Consumidor Final / Propia', '')
        )
        new_id = cursor.lastrowid
        print(f"\nCreado registro 'Consumidor Final / Propia' con id={new_id}")
    else:
        print("\nRegistro ya existe, no se crea uno nuevo.")

print("\nDone.")
