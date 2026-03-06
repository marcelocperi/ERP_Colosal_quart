import sys, os, re
sys.path.insert(0, os.getcwd())
from database import get_db_cursor
import mariadb
from database import DB_CONFIG

# Usar conexión directa para DDL (CREATE TABLE / CREATE VIEW)
conn = mariadb.connect(**DB_CONFIG)
conn.autocommit = True
cursor = conn.cursor()

sql = open("migrations/create_ai_feedback_table.sql", "r", encoding="utf-8").read()

# Quitar comentarios de línea completa, separar por ;
lines = [l for l in sql.splitlines() if not l.strip().startswith("--")]
clean_sql = "\n".join(lines)
stmts = [s.strip() for s in re.split(r";", clean_sql) if s.strip()]

for stmt in stmts:
    try:
        cursor.execute(stmt)
        print("OK:", stmt[:80].replace("\n", " "))
    except Exception as e:
        print("SKIP:", str(e)[:100])

cursor.close()
conn.close()
print("\nMigración completada.")
