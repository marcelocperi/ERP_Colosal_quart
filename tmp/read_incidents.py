import sys
import json
sys.path.insert(0, r'c:\Users\marce\Documents\GitHub\bibliotecaweb\multiMCP')

from database import get_db_cursor

with get_db_cursor(dictionary=True) as cur:
    cur.execute(
        'SELECT id, error_message, clob_data FROM sys_transaction_logs WHERE id IN (651,655,656,657,658) ORDER BY id DESC'
    )
    rows = cur.fetchall()
    for r in rows:
        print('=== ID', r['id'], '===')
        print('Error:', r['error_message'])
        clob = r.get('clob_data', '') or ''
        if clob:
            try:
                d = json.loads(clob)
                for k, v in d.items():
                    val = str(v)[:400]
                    print(f'  {k}: {val}')
            except Exception:
                print(str(clob)[:600])
        print()
