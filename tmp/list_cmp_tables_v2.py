
import sys
import os
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES LIKE 'cmp_%'")
    tables = [list(r.values())[0] for r in cursor.fetchall()]
    print(f"Tablas encontradas: {tables}")
    
    # También buscar si existe algo de sugerencias en sys_ o erp_
    cursor.execute("SHOW TABLES LIKE '%sugerencia%'")
    tables_sug = [list(r.values())[0] for r in cursor.fetchall()]
    print(f"Tablas de sugerencias: {tables_sug}")
