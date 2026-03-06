
import sqlite3
conn = sqlite3.connect('multi_mcp.db')
c = conn.cursor()
c.execute("SELECT name FROM sqlite_master WHERE type='table'")
tables = [t[0] for t in c.fetchall()]
print(f"Total tables: {len(tables)}")
print(tables[:20]) # Show some
for t in tables:
    c.execute(f"PRAGMA table_info({t})")
    cols = [col[1] for col in c.fetchall()]
    if 'user_id' not in cols:
        print(f"MISSING Audit in: {t} -> {cols}")
conn.close()
