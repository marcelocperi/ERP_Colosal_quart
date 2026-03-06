import os
import sys

project_root = r"c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP"
if project_root not in sys.path:
    sys.path.append(project_root)

from database import get_db_cursor

def dump_for_mapping():
    with get_db_cursor(dictionary=True) as cursor:
        with open("tmp_mapping_dump.txt", "w") as f:
            f.write("=== CONDICIONES DE PAGO ===\n")
            cursor.execute("SELECT id, nombre FROM fin_condiciones_pago WHERE activo = 1")
            for r in cursor.fetchall():
                f.write(f"ID: {r['id']} | Name: {r['nombre']}\n")
            
            f.write("\n=== MEDIOS DE PAGO ===\n")
            cursor.execute("SELECT id, nombre, tipo FROM fin_medios_pago WHERE activo = 1")
            for r in cursor.fetchall():
                f.write(f"ID: {r['id']} | Name: {r['nombre']} | Type: {r['tipo']}\n")
            
            f.write("\n=== MIXED CONDITIONS DETAILS ===\n")
            cursor.execute("""
                SELECT m.nombre as mixta_nombre, d.* 
                FROM fin_condiciones_pago_mixtas_detalle d
                JOIN fin_condiciones_pago_mixtas m ON d.mixta_id = m.id
            """)
            for r in cursor.fetchall():
                f.write(str(r) + "\n")

if __name__ == "__main__":
    dump_for_mapping()
