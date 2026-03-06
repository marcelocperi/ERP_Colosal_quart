import sys
import os
import sqlite3
# Adjust path to import database module
sys.path.append(os.path.join(os.path.dirname(__file__)))
from database import get_db_cursor

def list_publishers():
    with get_db_cursor(dictionary=True) as cursor:
        print("Obteniendo editoriales de LegacyLibros...")
        cursor.execute("SELECT DISTINCT editorial FROM legacy_libros WHERE editorial IS NOT NULL AND editorial != ''")
        publishers = [row['editorial'].strip() for row in cursor.fetchall()]
        
        # Also check StkArticulos brands just in case, though less likely to be book publishers only
        # cursor.execute("SELECT DISTINCT marca FROM stk_articulos WHERE marca IS NOT NULL AND marca != ''")
        # brands = [row['marca'].strip() for row in cursor.fetchall()]
        
        print("Verificando cuáles ya son proveedores...")
        cursor.execute("SELECT nombre FROM erp_terceros WHERE es_proveedor = 1")
        existing_names = set()
        for row in cursor.fetchall():
            if row['nombre']:
                existing_names.add(row['nombre'].strip().lower())

        publishers_counts = {}
        cursor.execute("SELECT editorial, COUNT(*) as cnt FROM legacy_libros WHERE editorial IS NOT NULL AND editorial != '' GROUP BY editorial ORDER BY cnt DESC")
        for row in cursor.fetchall():
            pub_name = row['editorial'].strip()
            count = row['cnt']
            
            # Check if likely already a provider (fuzzy match or exact match)
            is_provider = False
            if pub_name.lower() in existing_names:
                is_provider = True
            
            if not is_provider:
                publishers_counts[pub_name] = count

        sorted_publishers = sorted(publishers_counts.items(), key=lambda x: x[1], reverse=True)
        print(f"Encontradas {len(sorted_publishers)} editoriales únicas que NO son proveedores.")
        
        # Save to JSON
        import json
        with open('publishers_to_process.json', 'w', encoding='utf-8') as f:
            json.dump([{'name': p[0], 'count': p[1]} for p in sorted_publishers], f, ensure_ascii=False, indent=2)
            
        print("Listado guardado en 'publishers_to_process.json'.")
        
        # Return top 20 for immediate display
        return [p[0] for p in sorted_publishers[:20]]

if __name__ == "__main__":
    missing = list_publishers()
    for pub in missing:
        print(f"- {pub}")
