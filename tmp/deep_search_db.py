
import sys
import os
sys.path.append(r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')
from database import get_db_cursor
with get_db_cursor(dictionary=True) as cursor:
    cursor.execute("SHOW TABLES")
    all_tables = [list(r.values())[0] for r in cursor.fetchall()]
    print(f"Total tablas: {len(all_tables)}")
    # Print systemic tables
    sys_tables = [t for t in all_tables if t.startswith('sys_')]
    print(f"Sys Tables: {sys_tables}")
    
    # Search for anything related to roadmap or backlog
    backlog_tables = [t for t in all_tables if any(x in t.lower() for x in ['backlog', 'roadmap', 'task', 'todo', 'issue', 'desarrollo'])]
    print(f"Backlog matches: {backlog_tables}")
    
    # If not found, maybe look for content in sys_transaction_logs or similar?
    # No, user says 'tabla de sugerencias pendientes'. 
    # Let's try to find 'sugerencia' in the database meta
    cursor.execute("SELECT TABLE_NAME FROM information_schema.COLUMNS WHERE COLUMN_NAME LIKE '%sugerencia%'")
    col_matches = cursor.fetchall()
    print(f"Tablas con columnas 'sugerencia': {col_matches}")
