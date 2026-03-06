
import sys
import os
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES")
    all_tables = [list(r.values())[0] for r in cursor.fetchall()]
    print("Buscando 'sugerencia' o 'pendiente' en todas las tablas...")
    matches = [t for t in all_tables if 'sugerencia' in t.lower() or 'pendiente' in t.lower()]
    print(f"Matches: {matches}")
    
    if not matches:
        # Quizás se llama 'roadmap' o 'desarrollo'
        matches2 = [t for t in all_tables if 'roadmap' in t.lower() or 'desarrollo' in t.lower()]
        print(f"Matches alternativos: {matches2}")
