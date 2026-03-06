import mariadb
from database import DB_CONFIG

conn = mariadb.connect(**DB_CONFIG)
cur = conn.cursor()

print("--- Testing MariaDB JSON behavior ---")
q1 = "SELECT JSON_UNQUOTE(JSON_EXTRACT('{\"archivo_local\": true}', '$.archivo_local'))"
cur.execute(q1)
res1 = cur.fetchone()[0]
print(f"JSON boolean true -> UNQUOTE -> '{res1}' (Type: {type(res1)})")
print(f"Compare with 'true': {res1 == 'true'}")

q2 = "SELECT JSON_UNQUOTE(JSON_EXTRACT('{\"archivo_local\": \"true\"}', '$.archivo_local'))"
cur.execute(q2)
res2 = cur.fetchone()[0]
print(f"JSON string 'true' -> UNQUOTE -> '{res2}' (Type: {type(res2)})")
print(f"Compare with 'true': {res2 == 'true'}")

conn.close()
